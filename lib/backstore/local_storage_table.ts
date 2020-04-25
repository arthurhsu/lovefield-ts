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

import { PayloadType, Row } from '../base/row';
import { RuntimeTable } from '../base/runtime_table';
import { TableDiff } from '../cache/table_diff';

// Tables are stored in LocalStorage as a stringified data object in the format
// of {id1: row1, id2: row2, ..., idN: rowN}.
export class LocalStorageTable implements RuntimeTable {
  private key: string;
  private data: PayloadType;

  constructor(tableKey: string) {
    this.key = tableKey;
    this.data = {};

    const rawData = window.localStorage.getItem(tableKey);
    if (rawData) {
      this.data = JSON.parse(rawData);
    }
  }

  get(ids: number[]): Promise<Row[]> {
    let results: Row[];

    if (ids.length === 0) {
      results = Object.keys(this.data).map(key => {
        const id = Number(key);
        return new Row(id, this.data[key] as PayloadType);
      }, this);
    } else {
      results = [];
      ids.forEach(id => {
        if (Object.prototype.hasOwnProperty.call(this.data, id.toString())) {
          results.push(new Row(id, this.data[id.toString()] as PayloadType));
        }
      }, this);
    }

    return Promise.resolve(results);
  }

  put(rows: Row[]): Promise<void> {
    rows.forEach(row => {
      this.data[row.id().toString()] = row.payload();
    }, this);

    return Promise.resolve();
  }

  remove(
    ids: number[],
    disableClearTableOptimization?: boolean
  ): Promise<void> {
    if (ids.length === 0 || ids.length === Object.keys(this.data).length) {
      // Remove all.
      this.data = {};
    } else {
      ids.forEach(id => delete this.data[id]);
    }

    return Promise.resolve();
  }

  // Flushes contents to Local Storage.
  commit(): void {
    window.localStorage.setItem(this.key, JSON.stringify(this.data));
  }

  // Generates table diff from new data.
  diff(newData: PayloadType): TableDiff {
    const oldIds = Object.keys(this.data);
    const newIds = Object.keys(newData);

    const diff = new TableDiff(this.key);
    newIds.forEach(id => {
      const rowId = Number(id);
      if (Object.prototype.hasOwnProperty.call(this.data, id)) {
        // A maybe update: the event simply pass back all values of table.
        if (JSON.stringify(this.data[id]) !== JSON.stringify(newData[id])) {
          diff.modify([
            new Row(rowId, this.data[id] as PayloadType),
            new Row(rowId, newData[id] as PayloadType),
          ]);
        }
      } else {
        // Add
        diff.add(new Row(rowId, newData[id] as PayloadType));
      }
    }, this);
    oldIds
      .filter(id => Object.prototype.hasOwnProperty.call(newData, id))
      .forEach(id => {
        diff.delete(new Row(Number(id), this.data[id] as PayloadType));
      }, this);
    return diff;
  }
}
