/**
 * Copyright 2018 The Lovefield Project Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {assert} from '../base/assert';
import {ErrorCode} from '../base/enum';
import {ComparisonFunction, EvalRegistry, EvalType, KeyOfIndexFunction} from '../base/eval';
import {Exception} from '../base/exception';
import {Row} from '../base/row';
import {Cache} from '../cache/cache';
import {Key} from '../index/key_range';
import {RuntimeIndex} from '../index/runtime_index';
import {Relation} from '../proc/relation';
import {RelationEntry} from '../proc/relation_entry';
import {BaseColumn} from '../schema/base_column';
import {BaseTable} from '../schema/base_table';
import {MapSet} from '../structs/map_set';
import {PredicateNode} from './predicate_node';

export interface IndexJoinInfo {
  indexedColumn: BaseColumn;
  nonIndexedColumn: BaseColumn;
  index: RuntimeIndex;
}

export class JoinPredicate extends PredicateNode {
  // Exponent of block size, so the block size is 2^(BLOCK_SIZE_EXPONENT).
  private static BLOCK_SIZE_EXPONENT = 8;

  private nullPayload: object|null;
  private evaluatorFn: ComparisonFunction;
  private keyOfIndexFn: KeyOfIndexFunction;

  constructor(
      public leftColumn: BaseColumn, public rightColumn: BaseColumn,
      public evaluatorType: EvalType) {
    super();
    this.nullPayload = null;

    const registry = EvalRegistry.get();
    this.evaluatorFn =
        registry.getEvaluator(this.leftColumn.getType(), this.evaluatorType);
    this.keyOfIndexFn =
        registry.getKeyOfIndexEvaluator(this.leftColumn.getType());
  }

  public copy(): JoinPredicate {
    const clone = new JoinPredicate(
        this.leftColumn, this.rightColumn, this.evaluatorType);
    clone.setId(this.getId());
    return clone;
  }

  public getColumns(results?: BaseColumn[]): BaseColumn[] {
    if (results !== undefined && results !== null) {
      results.push(this.leftColumn);
      results.push(this.rightColumn);
      return results;
    }
    return [this.leftColumn, this.rightColumn];
  }

  public getTables(results?: Set<BaseTable>): Set<BaseTable> {
    const tables = (results !== undefined && results !== null) ?
        results :
        new Set<BaseTable>();
    tables.add(this.leftColumn.getTable());
    tables.add(this.rightColumn.getTable());
    return tables;
  }

  // Creates a new predicate with the  left and right columns swapped and
  // operator changed (if necessary).
  public reverse(): JoinPredicate {
    let evaluatorType = this.evaluatorType;
    switch (this.evaluatorType) {
      case EvalType.GT:
        evaluatorType = EvalType.LT;
        break;
      case EvalType.LT:
        evaluatorType = EvalType.GT;
        break;
      case EvalType.GTE:
        evaluatorType = EvalType.LTE;
        break;
      case EvalType.LTE:
        evaluatorType = EvalType.GTE;
        break;
      default:
        break;
    }
    const newPredicate =
        new JoinPredicate(this.rightColumn, this.leftColumn, evaluatorType);
    return newPredicate;
  }

  public eval(relation: Relation): Relation {
    const entries = relation.entries.filter((entry) => {
      const leftValue = entry.getField(this.leftColumn);
      const rightValue = entry.getField(this.rightColumn);
      return this.evaluatorFn(leftValue, rightValue);
    }, this);

    return new Relation(entries, relation.getTables());
  }

  public toString(): string {
    return 'join_pred(' + this.leftColumn.getNormalizedName() + ' ' +
        this.evaluatorType + ' ' + this.rightColumn.getNormalizedName() + ')';
  }

  // Calculates the join between the input relations using a Nested-Loop-Join
  // algorithm.
  // Nulls cannot be matched. Hence Inner join does not return null matches
  // at all and Outer join retains each null entry of the left table.
  public evalRelationsNestedLoopJoin(
      leftRelation: Relation, rightRelation: Relation,
      isOuterJoin: boolean): Relation {
    let leftRightRelations = [leftRelation, rightRelation];
    // For outer join, left and right are not interchangeable.
    if (!isOuterJoin) {
      leftRightRelations = this.detectLeftRight(leftRelation, rightRelation);
    }
    leftRelation = leftRightRelations[0];
    rightRelation = leftRightRelations[1];

    const combinedEntries = [];
    const leftRelationTables = leftRelation.getTables();
    const rightRelationTables = rightRelation.getTables();
    const leftEntriesLength = leftRelation.entries.length;
    const rightEntriesLength = rightRelation.entries.length;

    // Since block size is a power of two, we can use bitwise operators.
    const blockNumBits = JoinPredicate.BLOCK_SIZE_EXPONENT;
    // This is equivalent to Math.ceil(rightEntriesLength/blockSize).
    const blockCount =
        (rightEntriesLength + (1 << blockNumBits) - 1) >> blockNumBits;
    let currentBlock = 0;
    // The inner loop is executed in blocks. Blocking helps in prefetching
    // the next contents by CPU and also reduces cache misses as long as a block
    // is close to the size of cache.
    while (currentBlock < blockCount) {
      for (let i = 0; i < leftEntriesLength; i++) {
        let matchFound = false;
        const leftValue = leftRelation.entries[i].getField(this.leftColumn);
        if (leftValue !== null) {
          const rightLimit =
              Math.min((currentBlock + 1) << blockNumBits, rightEntriesLength);
          for (let j = currentBlock << blockNumBits; j < rightLimit; j++) {
            // Evaluating before combining the rows, since combining is fairly
            // expensive.
            const predicateResult = this.evaluatorFn(
                leftValue, rightRelation.entries[j].getField(this.rightColumn));

            if (predicateResult) {
              matchFound = true;
              const combinedEntry = RelationEntry.combineEntries(
                  leftRelation.entries[i], leftRelationTables,
                  rightRelation.entries[j], rightRelationTables);
              combinedEntries.push(combinedEntry);
            }
          }
        }
        if (isOuterJoin && !matchFound) {
          combinedEntries.push(this.createCombinedEntryForUnmatched(
              leftRelation.entries[i], leftRelationTables));
        }
      }
      currentBlock++;
    }
    const srcTables =
        leftRelation.getTables().concat(rightRelation.getTables());
    return new Relation(combinedEntries, srcTables);
  }

  // Calculates the join between the input relations using a Hash-Join
  // algorithm. Such a join implementation can only be used if the join
  // conditions is the "equals" operator.
  // Nulls cannot be matched. Hence Inner join does not return null matches
  // at all and Outer join retains each null entry of the left table.
  public evalRelationsHashJoin(
      leftRelation: Relation, rightRelation: Relation,
      isOuterJoin: boolean): Relation {
    let leftRightRelations = [leftRelation, rightRelation];
    // For outer join, left and right are not interchangeable.
    if (!isOuterJoin) {
      leftRightRelations = this.detectLeftRight(leftRelation, rightRelation);
    }
    leftRelation = leftRightRelations[0];
    rightRelation = leftRightRelations[1];

    // If it is an outerjoin, then swap to make sure that the right table is
    // used for the "build" phase of the hash-join algorithm. If it is inner
    // join, choose the smaller of the two relations to be used for the "build"
    // phase.
    let minRelation = leftRelation;
    let maxRelation = rightRelation;
    let minColumn = this.leftColumn;
    let maxColumn = this.rightColumn;
    if (isOuterJoin) {
      minRelation = rightRelation;
      maxRelation = leftRelation;
      minColumn = this.rightColumn;
      maxColumn = this.leftColumn;
    }

    const map = new MapSet<string, RelationEntry>();
    const combinedEntries: RelationEntry[] = [];

    minRelation.entries.forEach((entry) => {
      const key = String(entry.getField(minColumn));
      map.set(key, entry);
    });

    const minRelationTableNames = minRelation.getTables();
    const maxRelationTableNames = maxRelation.getTables();

    maxRelation.entries.forEach((entry) => {
      const value = entry.getField(maxColumn);
      const key = String(value);
      if (value !== null && map.has(key)) {
        (map.get(key) as any as RelationEntry[]).forEach((innerEntry) => {
          const combinedEntry = RelationEntry.combineEntries(
              entry, maxRelationTableNames, innerEntry, minRelationTableNames);
          combinedEntries.push(combinedEntry);
        });
      } else {
        if (isOuterJoin) {
          combinedEntries.push(this.createCombinedEntryForUnmatched(
              entry, maxRelationTableNames));
        }
      }
    }, this);

    const srcTables =
        leftRelation.getTables().concat(rightRelation.getTables());
    return new Relation(combinedEntries, srcTables);
  }

  public evalRelationsIndexNestedLoopJoin(
      leftRelation: Relation, rightRelation: Relation,
      indexJoinInfo: IndexJoinInfo, cache: Cache): Relation {
    assert(
        this.evaluatorType === EvalType.EQ,
        'For now, index nested loop join can only be leveraged for EQ.');

    // Detecting which relation should be used as outer (non-indexed) and which
    // as inner (indexed).
    const indexedTable = indexJoinInfo.indexedColumn.getTable();
    let outerRelation = leftRelation;
    let innerRelation = rightRelation;
    if (leftRelation.getTables().indexOf(indexedTable.getEffectiveName()) !==
        -1) {
      outerRelation = rightRelation;
      innerRelation = leftRelation;
    }

    const combinedEntries: RelationEntry[] = [];
    const innerRelationTables = innerRelation.getTables();
    const outerRelationTables = outerRelation.getTables();

    // Generates and pushes a new combined entry to the results.
    // |row| is The row corresponding to the inner entry.
    function pushCombinedEntry(outerEntry: RelationEntry, row: Row): void {
      const innerEntry = new RelationEntry(row, innerRelationTables.length > 1);
      const combinedEntry = RelationEntry.combineEntries(
          outerEntry, outerRelationTables, innerEntry, innerRelationTables);
      combinedEntries.push(combinedEntry);
    }

    outerRelation.entries.forEach((entry) => {
      const keyOfIndex =
          this.keyOfIndexFn(entry.getField(indexJoinInfo.nonIndexedColumn));
      const matchingRowIds = indexJoinInfo.index.get(keyOfIndex as Key);
      if (matchingRowIds.length === 0) {
        return;
      }
      if (indexJoinInfo.index.isUniqueKey()) {
        // Since the index has only unique keys, expecting only one rowId.
        // Using Cache#get, instead of Cache#getMany, since it has better
        // performance (no unnecessary array allocations).
        pushCombinedEntry(entry, cache.get(matchingRowIds[0]) as Row);
      } else {
        const rows = cache.getMany(matchingRowIds);
        rows.forEach(pushCombinedEntry.bind(null, entry));
      }
    }, this);

    const srcTables =
        outerRelation.getTables().concat(innerRelation.getTables());
    return new Relation(combinedEntries, srcTables);
  }

  public setComplement(isComplement: boolean): void {
    throw new Exception(
        ErrorCode.ASSERTION, 'Join predicate has no complement');
  }

  // Swaps left and right columns and changes operator (if necessary).
  private reverseSelf(): void {
    const temp = this.leftColumn;
    this.leftColumn = this.rightColumn;
    this.rightColumn = temp;

    let evaluatorType = this.evaluatorType;
    switch (this.evaluatorType) {
      case EvalType.GT:
        evaluatorType = EvalType.LT;
        break;
      case EvalType.LT:
        evaluatorType = EvalType.GT;
        break;
      case EvalType.GTE:
        evaluatorType = EvalType.LTE;
        break;
      case EvalType.LTE:
        evaluatorType = EvalType.GTE;
        break;
      default:
        return;
    }
    this.evaluatorType = evaluatorType;
    this.evaluatorFn = EvalRegistry.get().getEvaluator(
        this.leftColumn.getType(), this.evaluatorType);
  }

  // Returns whether the given relation can be used as the "left" parameter of
  // this predicate.
  private appliesToLeft(relation: Relation): boolean {
    return relation.getTables().indexOf(
               this.leftColumn.getTable().getEffectiveName()) !== -1;
  }

  // Returns whether the given relation can be used as the "right" parameter of
  // this predicate.
  private appliesToRight(relation: Relation): boolean {
    return relation.getTables().indexOf(
               this.rightColumn.getTable().getEffectiveName()) !== -1;
  }

  // Asserts that the given relations are applicable to this join predicate.
  // Example of non-applicable relations:
  //   - join predicate: photoTable.albumId == albumTable.id
  //   leftRelation.getTables() does not include photoTable, or
  //   rightRelation.getTables() does not include albumTable.
  private assertRelationsApply(left: Relation, right: Relation): void {
    assert(
        this.appliesToLeft(left),
        'Mismatch between join predicate left operand and right relation.');
    assert(
        this.appliesToRight(right),
        'Mismatch between join predicate right operand and right relation.');
  }

  // Detects which input relation should be used as left/right. If predicate
  // order does not match with the left and right relations, left and right are
  // reversed. If the right table has larger size, then the left, right and
  // evaluation type are reversed (This is done to make it more cache
  // efficient).
  // Returns an array holding the two input relationsin the order of
  // [left, right].
  private detectLeftRight(relation1: Relation, relation2: Relation):
      [Relation, Relation] {
    let left: Relation = null as any as Relation;
    let right: Relation = null as any as Relation;

    if (this.appliesToLeft(relation1)) {
      this.assertRelationsApply(relation1, relation2);
      left = relation1;
      right = relation2;
    } else {
      this.assertRelationsApply(relation2, relation1);
      left = relation2;
      right = relation1;
    }
    if (left.entries.length > right.entries.length) {
      this.reverseSelf();
      this.assertRelationsApply(right, left);
      return [right, left];
    }
    return [left, right];
  }

  // Creates a row with null columns with column names obtained from the table.
  private createNullPayload(table: BaseTable): object {
    const payload = {};
    table.getColumns().forEach((column) => payload[column.getName()] = null);
    return payload;
  }

  // Creates a combined entry with an unmatched left entry from outer join
  // algorithm and a null entry.
  private createCombinedEntryForUnmatched(
      entry: RelationEntry, leftRelationTables: string[]): RelationEntry {
    if (this.nullPayload === null) {
      this.nullPayload = this.createNullPayload(this.rightColumn.getTable());
    }
    // The right relation is guaranteed to never be the result
    // of a previous join.
    const nullEntry =
        new RelationEntry(new Row(Row.DUMMY_ID, this.nullPayload), false);
    const combinedEntry = RelationEntry.combineEntries(
        entry, leftRelationTables, nullEntry,
        [this.rightColumn.getTable().getEffectiveName()]);
    return combinedEntry;
  }
}
