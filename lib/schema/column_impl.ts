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

import {Binder} from '../base/bind';
import {Type} from '../base/enum';
import {EvalType} from '../base/eval';
import {OperandType, ValueOperandType} from '../pred/operand_type';
import {createPredicate} from '../pred/pred';
import {Predicate} from '../pred/predicate';

import {BaseColumn} from './base_column';
import {BaseTable} from './base_table';
import {Column} from './column';
import {IndexImpl} from './index_impl';

export class ColumnImpl implements BaseColumn {
  readonly alias: string;
  [key: string]: unknown;

  private indices: IndexImpl[];
  private index: IndexImpl;

  constructor(
    readonly table: BaseTable,
    readonly name: string,
    readonly unique: boolean,
    readonly nullable: boolean,
    readonly type: Type,
    alias?: string
  ) {
    this.alias = alias || (null as unknown as string);
    this.indices = [];
    this.index = undefined as unknown as IndexImpl;
  }

  getName(): string {
    return this.name;
  }

  getNormalizedName(): string {
    return `${this.table.getEffectiveName()}.${this.name}`;
  }

  toString(): string {
    return this.getNormalizedName();
  }

  getTable(): BaseTable {
    return this.table;
  }

  getType(): Type {
    return this.type;
  }

  getAlias(): string {
    return this.alias;
  }

  isNullable(): boolean {
    return this.nullable;
  }

  isUnique(): boolean {
    return this.unique;
  }

  getIndices(): IndexImpl[] {
    (this.table.getIndices() as IndexImpl[]).forEach(index => {
      const colNames = index.columns.map(col => col.schema.getName());
      if (colNames.indexOf(this.name) !== -1) {
        this.indices.push(index);
      }
    });
    return this.indices;
  }

  getIndex(): IndexImpl {
    // Check of undefined is used purposefully here, such that this logic is
    // skipped if this.index has been set to null by a previous execution of
    // getIndex().
    if (this.index === undefined) {
      const indices = this.getIndices().filter(indexSchema => {
        if (indexSchema.columns.length !== 1) {
          return false;
        }
        return indexSchema.columns[0].schema.getName() === this.name;
      });

      // Normally there should be only one dedicated index for this column,
      // but if there are more, just grab the first one.
      this.index =
        indices.length > 0 ? indices[0] : (null as unknown as IndexImpl);
    }
    return this.index;
  }

  eq(operand: OperandType): Predicate {
    return createPredicate(this, operand, EvalType.EQ);
  }

  neq(operand: OperandType): Predicate {
    return createPredicate(this, operand, EvalType.NEQ);
  }

  lt(operand: OperandType): Predicate {
    return createPredicate(this, operand, EvalType.LT);
  }

  lte(operand: OperandType): Predicate {
    return createPredicate(this, operand, EvalType.LTE);
  }

  gt(operand: OperandType): Predicate {
    return createPredicate(this, operand, EvalType.GT);
  }

  gte(operand: OperandType): Predicate {
    return createPredicate(this, operand, EvalType.GTE);
  }

  match(operand: Binder | RegExp): Predicate {
    return createPredicate(this, operand, EvalType.MATCH);
  }

  between(from: ValueOperandType, to: ValueOperandType): Predicate {
    return createPredicate(this, [from, to], EvalType.BETWEEN);
  }

  in(values: Binder | ValueOperandType[]): Predicate {
    return createPredicate(this, values, EvalType.IN);
  }

  isNull(): Predicate {
    return this.eq(null as unknown as OperandType);
  }

  isNotNull(): Predicate {
    return this.neq(null as unknown as OperandType);
  }

  as(name: string): Column {
    return new ColumnImpl(
      this.table,
      this.name,
      this.unique,
      this.nullable,
      this.type,
      name
    );
  }
}
