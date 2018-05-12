/**
 * @license
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

import {RawRow, Row} from '../base/row';
import {Column} from './column';
import {Constraint} from './constraint';
import {ForeignKeySpec} from './foreign_key_spec';
import {Index} from './index';

export abstract class Table {
  // Row id index is named <tableName>.#, which is an invalid name for JS vars
  // and therefore user-defined indices can never collide with it.
  public static ROW_ID_INDEX_PATTERN = '#';

  private alias: string;
  // Foreign key constraints where this table acts as the parent. Populated
  // later (within lf.schema.Builder#finalize), only if such constraints exist.
  private referencingForeignKeys: ForeignKeySpec[];

  constructor(
      readonly name: string, readonly columns: Column[],
      readonly indices: Index[], readonly persistentIndex: boolean) {
    this.alias = null as any as string;
    this.referencingForeignKeys = null as any as ForeignKeySpec[];
  }

  public getName(): string {
    return this.name;
  }
  public getAlias(): string {
    return this.alias;
  }
  public getEffectiveName(): string {
    return this.alias || this.name;
  }

  public getIndices(): Index[] {
    return this.indices;
  }
  public getColumns(): Column[] {
    return this.columns;
  }
  public getRowIdIndexName(): string {
    return `${this.name}.${Table.ROW_ID_INDEX_PATTERN}`;
  }

  public abstract createRow(): Row;
  public abstract deserializeRow(dbRecord: RawRow): Row;
  public abstract getConstraint(): Constraint;
  public abstract as(alias: string): Table;
}
