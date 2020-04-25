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

import {Type} from '../base/enum';
import {BaseColumn} from '../schema/base_column';
import {BaseTable} from '../schema/base_table';
import {Index} from '../schema/index';

import {NonPredicateProvider} from './non_predicate_provider';
import {UnknownTable} from './unknown_table';

//  A dummy Column implementation to be used as a substitute for '*',
// for example in COUNT(*).
export class StarColumn extends NonPredicateProvider implements BaseColumn {
  // Make TypeScript happy.
  [key: string]: unknown;

  private alias: string;
  private table: UnknownTable;

  constructor(alias?: string) {
    super();
    this.alias = alias || ((null as unknown) as string);
    this.table = new UnknownTable();
  }

  getName(): string {
    return '*';
  }

  getNormalizedName(): string {
    return this.getName();
  }

  toString(): string {
    return this.getNormalizedName();
  }

  getTable(): BaseTable {
    // NOTE: The table here does not have a useful meaning, since the StarColumn
    // represents all columns that are available, which could be the result of a
    // join, therefore a dummy Table instance is used.
    return this.table;
  }

  getType(): Type {
    // NOTE: The type here does not have a useful meaning, since the notion of a
    // type does not apply to a collection of all columns (which is what this
    // class represents).
    return Type.NUMBER;
  }

  getAlias(): string {
    return this.alias;
  }

  getIndices(): Index[] {
    return [];
  }

  getIndex(): Index | null {
    return null;
  }

  isNullable(): boolean {
    return false;
  }

  isUnique(): boolean {
    return false;
  }

  as(alias: string): StarColumn {
    const clone = new StarColumn(alias);
    clone.table = this.table;
    return clone;
  }
}
