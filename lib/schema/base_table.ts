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
import {BaseColumn} from './base_column';
import {Constraint} from './constraint';
import {Index} from './index';

export interface BaseTable {
  getName(): string;
  getColumns(): BaseColumn[];
  getIndices(): Index[];
  persistentIndex(): boolean;
  getAlias(): string;
  getConstraint(): Constraint;
  getEffectiveName(): string;
  as(alias: string): BaseTable;
  getRowIdIndexName(): string;
  createRow(value?: object): Row;
  deserializeRow(dbRecord: RawRow): Row;
}
