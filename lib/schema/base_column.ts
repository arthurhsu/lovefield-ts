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
import {createPredicate} from '../pred/pred';
import {Predicate} from '../pred/predicate';
import {OperandType, ValueOperandType} from '../pred/predicate_provider';
import {Column} from '../schema/column';
import {IndexImpl} from '../schema/index_impl';
import {Table} from '../schema/table';

export class BaseColumn implements Column {
  public readonly alias: string;

  private indices: IndexImpl[];
  private index: IndexImpl;

  constructor(
      readonly table: Table, readonly name: string, readonly unique: boolean,
      readonly nullable: boolean, readonly type: Type, alias?: string) {
    this.alias = alias || null as any as string;
    this.indices = [];
    this.index = undefined as any as IndexImpl;
  }

  public getName(): string {
    return this.name;
  }

  public getNormalizedName(): string {
    return `${this.table.getEffectiveName()}.${this.name}`;
  }

  public toString(): string {
    return this.getNormalizedName();
  }

  public getTable(): Table {
    return this.table;
  }

  public getType(): Type {
    return this.type;
  }

  public getAlias(): string {
    return this.alias;
  }

  public isNullable(): boolean {
    return this.nullable;
  }

  public isUnique(): boolean {
    return this.unique;
  }

  public getIndices(): IndexImpl[] {
    this.table.getIndices().forEach((index) => {
      const colNames = index.columns.map((col) => col.schema.getName());
      if (colNames.indexOf(this.name) !== -1) {
        this.indices.push(index);
      }
    });
    return this.indices;
  }

  public getIndex(): IndexImpl {
    // Check of undefined is used purposefully here, such that this logic is
    // skipped if this.index has been set to null by a previous execution of
    // getIndex().
    if (this.index === undefined) {
      const indices = this.getIndices().filter((indexSchema) => {
        if (indexSchema.columns.length !== 1) {
          return false;
        }
        return indexSchema.columns[0].schema.getName() === this.name;
      });

      // Normally there should be only one dedicated index for this column,
      // but if there are more, just grab the first one.
      this.index = (indices.length > 0) ? indices[0] : null as any as IndexImpl;
    }
    return this.index;
  }

  public eq(operand: OperandType): Predicate {
    return createPredicate(this, operand, EvalType.EQ);
  }

  public neq(operand: OperandType): Predicate {
    return createPredicate(this, operand, EvalType.NEQ);
  }

  public lt(operand: OperandType): Predicate {
    return createPredicate(this, operand, EvalType.LT);
  }

  public lte(operand: OperandType): Predicate {
    return createPredicate(this, operand, EvalType.LTE);
  }

  public gt(operand: OperandType): Predicate {
    return createPredicate(this, operand, EvalType.GT);
  }

  public gte(operand: OperandType): Predicate {
    return createPredicate(this, operand, EvalType.GTE);
  }

  public match(operand: Binder|RegExp): Predicate {
    return createPredicate(this, operand, EvalType.MATCH);
  }

  public between(from: ValueOperandType, to: ValueOperandType): Predicate {
    return createPredicate(this, [from, to], EvalType.BETWEEN);
  }

  public in(values: Binder|ValueOperandType[]): Predicate {
    return createPredicate(this, values, EvalType.IN);
  }

  public isNull(): Predicate {
    return this.eq(null as any as OperandType);
  }

  public isNotNull(): Predicate {
    return this.neq(null as any as OperandType);
  }

  public as(name: string): Column {
    return new BaseColumn(
        this.table, this.name, this.unique, this.nullable, this.type, name);
  }
}
