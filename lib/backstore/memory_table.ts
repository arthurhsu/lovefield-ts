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

import {Row} from '../base/row';
import {RuntimeTable} from '../base/runtime_table';

export class MemoryTable implements RuntimeTable {
  private data: Map<number, Row>;

  constructor() {
    this.data = new Map();
  }

  getSync(ids: number[]): Row[] {
    // Empty array is treated as "return all rows".
    if (ids.length === 0) {
      return Array.from(this.data.values());
    }

    const results: Row[] = [];
    ids.forEach(id => {
      const row = this.data.get(id) || null;
      if (row !== null) {
        results.push(row);
      }
    }, this);

    return results;
  }

  getData(): Map<number, Row> {
    return this.data;
  }

  get(ids: number[]): Promise<Row[]> {
    return Promise.resolve(this.getSync(ids));
  }

  putSync(rows: Row[]): void {
    rows.forEach(row => this.data.set(row.id(), row));
  }

  put(rows: Row[]): Promise<void> {
    this.putSync(rows);
    return Promise.resolve();
  }

  removeSync(ids: number[]): void {
    if (ids.length === 0 || ids.length === this.data.size) {
      // Remove all.
      this.data.clear();
    } else {
      ids.forEach(id => this.data.delete(id));
    }
  }

  remove(ids: number[]): Promise<void> {
    this.removeSync(ids);
    return Promise.resolve();
  }

  getMaxRowId(): number {
    if (this.data.size === 0) {
      return 0;
    }
    return Array.from(this.data.keys()).reduce((prev, cur) => {
      return prev > cur ? prev : cur;
    }, 0);
  }
}
