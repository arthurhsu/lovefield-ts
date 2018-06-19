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
import {Column} from '../schema/column';
import {Index} from '../schema/index';
import {Table} from '../schema/table';

export class AggregatedColumn implements Column {
  public child: Column;
  public aggregatorType: FnType;
  public alias: string|null;

  constructor(col: Column, aggregatorType: FnType) {
    this.child = col;
    this.aggregatorType = aggregatorType;
    this.alias = null;
  }

  public getName(): string {
    return `${this.aggregatorType}(${this.child.getName()})`;
  }

  public getNormalizedName(): string {
    return `${this.aggregatorType}(${this.child.getNormalizedName()})`;
  }

  public getTable(): Table {
    return this.child.getTable();
  }

  public toString(): string {
    return this.getNormalizedName();
  }

  public getType(): Type {
    return this.child.getType();
  }

  public getAlias(): string {
    return this.alias as string;
  }

  public getIndices(): Index[] {
    return [];
  }

  public getIndex(): Index|null {
    return null;
  }

  public isNullable(): boolean {
    return false;
  }

  public isUnique(): boolean {
    return false;
  }

  public as(name: string): AggregatedColumn {
    this.alias = name;
    return this;
  }

  // Returns The chain of columns that starts from this column. All columns
  // are of type AggregatedColumn except for the last column.
  public getColumnChain(): Column[] {
    const columnChain: Column[] = [this];
    let currentColumn: Column = this;
    while (currentColumn instanceof AggregatedColumn) {
      columnChain.push(currentColumn.child);
      currentColumn = currentColumn.child;
    }
    return columnChain;
  }
}
