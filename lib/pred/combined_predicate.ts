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
import {Operator} from '../base/private_enum';
import {SingleKeyRangeSet} from '../index/single_key_range_set';
import {Relation} from '../proc/relation';
import {Column} from '../schema/column';
import {Table} from '../schema/table';
import {TreeHelper} from '../structs/tree_helper';
import {PredicateNode} from './predicate_node';
import {ValuePredicate} from './value_predicate';

export class CombinedPredicate extends PredicateNode {
  private isComplement: boolean;

  constructor(public operator: Operator) {
    super();

    // Whether this predicate has been reversed. This is necessary only for
    // handling the case where setComplement() is called twice with the same
    // value.
    this.isComplement = false;
  }

  public eval(relation: Relation): Relation {
    const results = this.getChildren().map(
        (condition) => (condition as PredicateNode).eval(relation));
    return this.combineResults(results);
  }

  public setComplement(isComplement: boolean): void {
    if (this.isComplement === isComplement) {
      // Nothing to do.
      return;
    }

    this.isComplement = isComplement;

    // NOT(AND(c1, c2)) becomes OR(NOT(c1), NOT(c2)).
    // NOT(OR(c1, c2)) becomes AND(NOT(c1), NOT(c2)).

    // Toggling AND/OR.
    this.operator = this.operator === Operator.AND ? Operator.OR : Operator.AND;

    // Toggling children conditions.
    this.getChildren().forEach(
        (condition) =>
            (condition as PredicateNode).setComplement(isComplement));
  }

  public copy(): CombinedPredicate {
    const copy = TreeHelper.map(this, (node) => {
      if (node instanceof CombinedPredicate) {
        const tempCopy = new CombinedPredicate(node.operator);
        tempCopy.isComplement = node.isComplement;
        tempCopy.setId(node.getId());
        return tempCopy;
      } else {
        return (node as any as PredicateNode).copy() as PredicateNode;
      }
    }) as CombinedPredicate;
    return copy;
  }

  public getColumns(results?: Column[]): Column[] {
    const columns = results || [];
    this.traverse((child) => {
      if (child === this) {
        return;
      }
      (child as PredicateNode).getColumns(columns);
    });

    const columnSet = new Set<Column>(columns);
    return Array.from(columnSet.values());
  }

  public getTables(results?: Set<Table>): Set<Table> {
    const tables = results ? results : new Set<Table>();
    this.traverse((child) => {
      if (child === this) {
        return;
      }
      (child as PredicateNode).getTables(tables);
    });
    return tables;
  }

  public toString(): string {
    return `combined_pred_${this.operator.toString()}`;
  }

  // Converts this predicate to a key range.
  // NOTE: Not all predicates can be converted to a key range, callers must call
  // isKeyRangeCompatible() before calling this method.
  public toKeyRange(): SingleKeyRangeSet {
    assert(
        this.isKeyRangeCompatible(),
        'Could not convert combined predicate to key range.');

    if (this.operator === Operator.OR) {
      const keyRangeSet = new SingleKeyRangeSet();
      this.getChildren().forEach((child) => {
        const childKeyRanges =
            (child as ValuePredicate).toKeyRange().getValues();
        keyRangeSet.add(childKeyRanges);
      });
      return keyRangeSet;
    } else {  // this.operator.lf.pred.Operator.OR
      // Unreachable code, because the assertion above should have already
      // thrown an error if this predicate is of type AND.
      assert(false, 'toKeyRange() called for an AND predicate.');
      return new SingleKeyRangeSet();
    }
  }

  // Returns whether this predicate can be converted to a set of key ranges.
  public isKeyRangeCompatible(): boolean {
    if (this.operator === Operator.OR) {
      return this.isKeyRangeCompatibleOr();
    }

    // AND predicates are broken down to individual predicates by the optimizer,
    // and therefore there is no need to convert an AND predicate to a key
    // range, because such predicates do not exist in the tree during query
    // execution.
    return false;
  }

  // Combines the results of all the children predicates.
  private combineResults(results: Relation[]): Relation {
    if (this.operator === Operator.AND) {
      return Relation.intersect(results);
    } else {
      // Must be the case where this.operator === Operator.OR.
      return Relation.union(results);
    }
  }

  // Checks if this OR predicate can be converted to a set of key ranges.
  // Currently only OR predicates that satisfy all of the following criteria can
  // be converted.
  //  1) Every child is a ValuePredicate
  //  2) All children refer to the same table and column.
  //  3) All children are key range compatible.
  private isKeyRangeCompatibleOr(): boolean {
    let predicateColumn: Column|null = null;
    return this.getChildren().every((child) => {
      const isCandidate =
          child instanceof ValuePredicate && child.isKeyRangeCompatible();
      if (!isCandidate) {
        return false;
      }
      if (predicateColumn === null) {
        predicateColumn = (child as ValuePredicate).column;
      }
      return predicateColumn.getNormalizedName() ===
          (child as ValuePredicate).column.getNormalizedName();
    });
  }
}
