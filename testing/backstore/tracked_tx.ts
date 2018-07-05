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

import {BackStore} from '../../lib/backstore/back_store';
import {BaseTx} from '../../lib/backstore/base_tx';
import {TransactionType} from '../../lib/base/enum';
import {TableType} from '../../lib/base/private_enum';
import {RawRow, Row} from '../../lib/base/row';
import {RuntimeTable} from '../../lib/base/runtime_table';
import {Journal} from '../../lib/cache/journal';
import {TableDiff} from '../../lib/cache/table_diff';

import {TrackedTable} from './tracked_table';

// Pseudo transaction object that tracks all changes made for a single flush.
export class TrackedTx extends BaseTx {
  // A directory of all the table connections that have been created within this
  // transaction.
  private tables: Map<string, TrackedTable>;

  constructor(
      private store: BackStore, type: TransactionType, journal?: Journal) {
    super(type, journal);
    this.tables = new Map<string, TrackedTable>();
    if (type === TransactionType.READ_ONLY) {
      this.resolver.resolve();
    }
  }

  public getTable(
      tableName: string, deserializeFn: (value: RawRow) => Row,
      tableType?: TableType): RuntimeTable {
    let table = this.tables.get(tableName) || null;
    if (table === null) {
      table =
          new TrackedTable(this.store.getTableInternal(tableName), tableName);
      this.tables.set(tableName, table);
    }
    return table;
  }

  public abort(): void {
    this.resolver.reject();
  }

  public commitInternal(): Promise<any> {
    const requests: Array<Promise<any>> = [];
    const tableDiffs: TableDiff[] = [];
    this.tables.forEach((table, tableName) => {
      requests.push(table.whenRequestsDone());
      tableDiffs.push(table.getDiff());
    });

    // Waiting for all asynchronous operations to finish.
    return Promise.all(requests).then(() => {
      // Notifying observers.
      this.store.notify(tableDiffs);
      this.resolver.resolve();
      return this.resolver.promise;
    });
  }
}
