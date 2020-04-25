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

import {assert} from '../base/assert';
import {Row} from '../base/row';
import {DatabaseSchema} from '../schema/database_schema';
import {Cache} from './cache';

export class DefaultCache implements Cache {
  private map: Map<number, Row>;
  private tableRows: Map<string, Set<number>>;

  constructor(dbSchema: DatabaseSchema) {
    this.map = new Map<number, Row>();
    this.tableRows = new Map<string, Set<number>>();

    dbSchema.tables().forEach(table => {
      this.tableRows.set(table.getName(), new Set<number>());
    }, this);
  }

  set(tableName: string, row: Row): void {
    this.map.set(row.id(), row);
    this.getTableRowSet(tableName).add(row.id());
  }

  setMany(tableName: string, rows: Row[]): void {
    const tableSet = this.getTableRowSet(tableName);
    rows.forEach(row => {
      this.map.set(row.id(), row);
      tableSet.add(row.id());
    }, this);
  }

  get(id: number): Row | null {
    return this.map.get(id) || null;
  }

  getMany(ids: number[]): Array<Row | null> {
    return ids.map(id => this.get(id));
  }

  getRange(tableName: string, fromId: number, toId: number): Row[] {
    const data: Row[] = [];
    const min = Math.min(fromId, toId);
    const max = Math.max(fromId, toId);
    const tableSet = this.getTableRowSet(tableName);

    // Ensure the least number of keys are iterated.
    if (tableSet.size < max - min) {
      tableSet.forEach(key => {
        if (key >= min && key <= max) {
          const value = this.map.get(key);
          assert(value !== null && value !== undefined, 'Inconsistent cache 1');
          data.push((value as unknown) as Row);
        }
      }, this);
    } else {
      for (let i = min; i <= max; ++i) {
        if (!tableSet.has(i)) {
          continue;
        }
        const value = this.map.get(i);
        assert(value !== null && value !== undefined, 'Inconsistent cache 2');
        data.push((value as unknown) as Row);
      }
    }
    return data;
  }

  remove(tableName: string, id: number): void {
    this.map.delete(id);
    this.getTableRowSet(tableName).delete(id);
  }

  removeMany(tableName: string, ids: number[]): void {
    const tableSet = this.getTableRowSet(tableName);
    ids.forEach(id => {
      this.map.delete(id);
      tableSet.delete(id);
    }, this);
  }

  getCount(tableName?: string): number {
    return tableName ? this.getTableRowSet(tableName).size : this.map.size;
  }

  clear(): void {
    this.map.clear();
    this.tableRows.clear();
  }

  private getTableRowSet(tableName: string): Set<number> {
    const ret = this.tableRows.get(tableName);
    return (ret as unknown) as Set<number>;
  }
}
