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

import {ErrorCode} from '../base/enum';
import {Exception} from '../base/exception';
import {RawRow, Row} from '../base/row';
import {BaseColumn} from '../schema/base_column';
import {BaseTable} from '../schema/base_table';
import {Constraint} from '../schema/constraint';
import {Index} from '../schema/index';

// Pseudo table used for initializing pseudo columns.
export class UnknownTable implements BaseTable {
  private _alias: string;

  constructor() {
    this._alias = null as any as string;
  }

  public getName(): string {
    return '#UnknownTable';
  }

  public getColumns(): BaseColumn[] {
    return [];
  }

  public getIndices(): Index[] {
    return [];
  }

  public persistentIndex(): boolean {
    return false;
  }

  public getAlias(): string {
    return this._alias;
  }

  public getEffectiveName(): string {
    return this._alias || this.getName();
  }

  public getRowIdIndexName(): string {
    return '#UnknownTable.#';
  }

  public createRow(value?: object): Row {
    throw new Exception(ErrorCode.NOT_SUPPORTED);
  }
  public deserializeRow(dbRecord: RawRow): Row {
    throw new Exception(ErrorCode.NOT_SUPPORTED);
  }

  public getConstraint(): Constraint {
    return null as any as Constraint;
  }

  public as(alias: string): BaseTable {
    this._alias = alias;
    return this;
  }
}
