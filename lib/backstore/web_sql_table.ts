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
import {RuntimeTable} from '../base/runtime_table';

import {WebSqlTx} from './web_sql_tx';

// Table stream based on a given WebSQL table.
export class WebSqlTable implements RuntimeTable {
  private name: string;
  constructor(
      private tx: WebSqlTx, name: string,
      private deserializeFn: (raw: RawRow) => Row) {
    // Escape name string by default.
    this.name = `"${name}"`;
  }

  public get(ids: number[]): Promise<Row[]> {
    const where = (ids.length === 0) ? '' : `WHERE id IN (${ids.join(',')})`;

    const sql = `SELECT id, value FROM ${this.name} ${where}`;
    const deserializeFn = this.deserializeFn;

    const transformer = (res: object): Row[] => {
      const results = res as SQLResultSet;
      const length = results.rows.length;
      const rows: Row[] = new Array(length);
      for (let i = 0; i < length; ++i) {
        rows[i] = deserializeFn({
          id: results.rows.item(i)['id'],
          value: JSON.parse(results.rows.item(i)['value']),
        });
      }
      return rows;
    };

    return this.tx.queue(sql, [], transformer);
  }

  public put(rows: Row[]): Promise<void> {
    if (rows.length === 0) {
      return Promise.resolve();
    }

    const sql = `INSERT OR REPLACE INTO ${this.name} (id, value) VALUES (?, ?)`;
    rows.forEach((row) => {
      this.tx.queue(sql, [row.id(), JSON.stringify(row.payload())]);
    }, this);

    return Promise.resolve();
  }

  public remove(ids: number[], disableClearTableOptimization?: boolean):
      Promise<void> {
    const where = (ids.length === 0) ? '' : `WHERE id IN (${ids.join(',')})`;
    const sql = `DELETE FROM ${this.name} ${where}`;
    this.tx.queue(sql, []);
    return Promise.resolve();
  }
}
