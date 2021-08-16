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
import {FnType} from '../base/private_enum';
import {BaseColumn} from '../schema/base_column';
import {BaseTable} from '../schema/base_table';
import {Column} from '../schema/column';
import {Index} from '../schema/index';
import {NonPredicateProvider} from './non_predicate_provider';

export class AggregatedColumn
  extends NonPredicateProvider
  implements BaseColumn
{
  alias: string | null;

  // Make TypeScript happy.
  [key: string]: unknown;

  constructor(readonly child: Column, readonly aggregatorType: FnType) {
    super();
    this.alias = null;
  }

  getName(): string {
    return `${this.aggregatorType}(${this.child.getName()})`;
  }

  getNormalizedName(): string {
    return `${this.aggregatorType}(${this.child.getNormalizedName()})`;
  }

  getTable(): BaseTable {
    return this.child.getTable() as BaseTable;
  }

  toString(): string {
    return this.getNormalizedName();
  }

  getType(): Type {
    return this.child.getType();
  }

  getAlias(): string {
    return this.alias as string;
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

  as(name: string): AggregatedColumn {
    this.alias = name;
    return this;
  }

  // Returns The chain of columns that starts from this column. All columns
  // are of type AggregatedColumn except for the last column.
  getColumnChain(): Column[] {
    const columnChain: Column[] = [this];
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    let currentColumn: Column = this;
    while (currentColumn instanceof AggregatedColumn) {
      columnChain.push(currentColumn.child);
      currentColumn = currentColumn.child;
    }
    return columnChain;
  }
}
