/**
 * Copyright 2016 The Lovefield Project Authors. All Rights Reserved.
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
import {Binder} from '../base/bind';
import {EvalRegistry, EvalType} from '../base/eval';
import {ErrorCode, Exception} from '../base/exception';
import {Predicate} from '../base/predicate';
import {Relation} from '../proc/relation';
import {Column} from '../schema/column';
import {Table} from '../schema/table';

import {PredicateNode} from './predicate_node';

export class ValuePredicate extends PredicateNode {
  private column: Column;
  private value: any;
  private evaluatorType: EvalType;
  private evaluatorFn: (l: any, r: any) => boolean;
  private isComplement: boolean;
  private binder: Binder|any;

  constructor(column: Column, value: any, evaluatorType: EvalType) {
    super();
    this.column = column;
    this.value = value;
    this.evaluatorType = evaluatorType;
    this.evaluatorFn = EvalRegistry.get().getEvaluator(
        this.column.getType(), this.evaluatorType);
    this.isComplement = false;
    this.binder = value;
  }

  public eval(relation: Relation): Relation {
    this.checkBinding();

    // Ignoring this.evaluatorFn_() for the case of the IN, in favor of a faster
    // evaluation implementation.
    if (this.evaluatorType === EvalType.IN) {
      return this.evalAsIn(relation);
    }

    const entries = relation.entries.filter((entry) => {
      return this.evaluatorFn(entry.getField(this.column), this.value) !==
          this.isComplement;
    });
    return new Relation(entries, relation.getTables());
  }

  public setComplement(isComplement: boolean): void {
    this.isComplement = isComplement;
  }

  public copy(): Predicate {
    const clone =
        new ValuePredicate(this.column, this.value, this.evaluatorType);
    clone.binder = this.binder;
    clone.isComplement = this.isComplement;
    clone.setId(this.getId());
    return clone;
  }

  public getColumns(results?: Column[]): Column[] {
    if (results) {
      results.push(this.column);
      return results;
    }
    return [this.column];
  }

  public getTables(results?: Set<Table>): Set<Table> {
    const tables = results ? results : new Set<Table>();
    tables.add(this.column.getTable());
    return tables;
  }

  public setBinder(binder: Binder|any): void {
    this.binder = binder;
  }

  public bind(values: any[]): void {
    const checkIndexWithinRange = (index: number) => {
      if (values.length <= index) {
        // 510: Cannot bind to given array: out of range.
        throw new Exception(ErrorCode.BIND_ARRAY_OUT_OF_RANGE);
      }
    };

    if (this.binder instanceof Binder) {
      const index = this.binder.index;
      checkIndexWithinRange(index);
      this.value = values[index];
    } else if (Array.isArray(this.binder)) {
      const array = this.binder as Array<Binder|any>;
      this.value = array.map((val) => {
        if (val instanceof Binder) {
          checkIndexWithinRange(val.index);
          return values[val.index];
        } else {
          return val;
        }
      });
    }
  }

  public toString(): string {
    return 'value_pred(' + this.column.getNormalizedName() + ' ' +
        this.evaluatorType + (this.isComplement ? '(complement)' : '') + ' ' +
        this.value + ')';
  }

  // Whether this predicate can be converted to a KeyRange instance.
  public isKeyRangeCompatible(): boolean {
    this.checkBinding();
    return this.value !== null &&
        (this.evaluatorType === EvalType.BETWEEN ||
         this.evaluatorType === EvalType.IN ||
         this.evaluatorType === EvalType.EQ ||
         this.evaluatorType === EvalType.GT ||
         this.evaluatorType === EvalType.GTE ||
         this.evaluatorType === EvalType.LT ||
         this.evaluatorType === EvalType.LTE);
  }

  // Converts this predicate to a key range.
  // NOTE: Not all predicates can be converted to a key range, callers must call
  // isKeyRangeCompatible() before calling this method.
  public toKeyRange(): void {  // SingleKeyRangeSet {
    // TODO(arthurhsu): implement
  }

  private checkBinding(): void {
    let bound = false;
    if (!(this.value instanceof Binder)) {
      if (Array.isArray(this.value)) {
        const array = this.value as Array<Binder|any>;
        bound = !array.some((val) => {
          return val instanceof Binder;
        });
      } else {
        bound = true;
      }
    }
    if (!bound) {
      // 501: Value is not bounded.
      throw new Exception(ErrorCode.UNBOUND_VALUE);
    }
  }

  private evalAsIn(relation: Relation): Relation {
    assert(
        this.evaluatorType === EvalType.IN,
        'ValuePredicate#evalAsIn_() called for wrong predicate type.');

    const valueSet = new Set<any>(this.value);
    const evaluatorFn = (rowValue: any) => {
      return (rowValue === null) ?
          false :
          (valueSet.has(rowValue) !== this.isComplement);
    };
    const entries = relation.entries.filter((entry) => {
      return evaluatorFn(entry.getField(this.column));
    });
    return new Relation(entries, relation.getTables());
  }

  // Converts value in this predicate to index key.
  // private getValueAsKey(value: T): void { // SingleKey {
  // TODO(arthurhsu): implement
  // }
}
