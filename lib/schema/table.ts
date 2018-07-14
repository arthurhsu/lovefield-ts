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

import {RawRow, Row} from '../base/row';
import {BaseTable} from './base_table';
import {Column} from './column';
import {Constraint} from './constraint';
import {Index} from './index';

export abstract class Table implements BaseTable {
  // Row id index is named <tableName>.#, which is an invalid name for JS vars
  // and therefore user-defined indices can never collide with it.
  public static ROW_ID_INDEX_PATTERN = '#';

  protected _alias: string;

  constructor(
      readonly _name: string, protected _columns: Column[],
      protected _indices: Index[], readonly _usePersistentIndex: boolean) {
    this._alias = null as any as string;
  }

  public getName(): string {
    return this._name;
  }
  public getAlias(): string {
    return this._alias;
  }
  public getEffectiveName(): string {
    return this._alias || this._name;
  }

  public getIndices(): Index[] {
    return this._indices;
  }
  public getColumns(): Column[] {
    return this._columns;
  }
  public getRowIdIndexName(): string {
    return `${this._name}.${Table.ROW_ID_INDEX_PATTERN}`;
  }

  public persistentIndex(): boolean {
    return this._usePersistentIndex;
  }

  public abstract createRow(value?: object): Row;
  public abstract deserializeRow(dbRecord: RawRow): Row;
  public abstract getConstraint(): Constraint;
  public abstract as(alias: string): Table;
}
