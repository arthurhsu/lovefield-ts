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

import {ErrorCode} from '../../lib/base/enum';
import {Exception} from '../../lib/base/exception';
import {Row} from '../../lib/base/row';
import {RuntimeTable} from '../../lib/base/runtime_table';
import {TableDiff} from '../../lib/cache/table_diff';

export class TrackedTable implements RuntimeTable {
  // The changes that have been applied on this table since the start of the
  // owning transaction.
  private tableDiff: TableDiff;

  // A list of all async operations that have been spawned for this table.
  private requests: Array<Promise<unknown>>;
  private acceptingRequests: boolean;

  constructor(private table: RuntimeTable, tableName: string) {
    this.tableDiff = new TableDiff(tableName);
    this.requests = [];
    this.acceptingRequests = true;
  }

  whenRequestsDone(): Promise<unknown> {
    this.acceptingRequests = false;
    return Promise.all(this.requests);
  }

  getDiff(): TableDiff {
    return this.tableDiff;
  }

  get(ids: number[]): Promise<Row[]> {
    try {
      this.checkAcceptingRequests();
    } catch (e) {
      return Promise.reject(e);
    }

    const promise = this.table.get(ids);
    this.requests.push(promise);
    return promise;
  }

  put(rows: Row[]): Promise<void> {
    try {
      this.checkAcceptingRequests();
    } catch (e) {
      return Promise.reject(e);
    }

    const rowMap = new Map<number, Row>();
    rows.forEach(row => rowMap.set(row.id(), row));

    const promise = this.get(Array.from(rowMap.keys())).then(existingRows => {
      // First update the diff with the existing rows that are modified.
      existingRows.forEach(existingRow => {
        this.tableDiff.modify([
          existingRow,
          rowMap.get(existingRow.id()) as Row,
        ]);
        rowMap.delete(existingRow.id());
      }, this);

      // Then update the diff with the remaining items in the map, all of
      // which correspond to new rows.
      rowMap.forEach(row => this.tableDiff.add(row), this);
      return this.table.put(rows);
    });

    this.requests.push(promise);
    return promise;
  }

  remove(
    ids: number[],
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    disableClearTableOptimization?: boolean
  ): Promise<void> {
    try {
      this.checkAcceptingRequests();
    } catch (e) {
      return Promise.reject(e);
    }

    const promise = this.table.get(ids).then(rows => {
      rows.forEach(row => this.tableDiff.delete(row));
      return this.table.remove(ids);
    });

    this.requests.push(promise);
    return promise;
  }

  private checkAcceptingRequests(): void {
    if (!this.acceptingRequests) {
      // 107: Invalid transaction state transition: {0} -> {1}.
      throw new Exception(ErrorCode.INVALID_TX_STATE, '7', '0');
    }
  }
}
