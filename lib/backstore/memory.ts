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

import {ErrorCode, TransactionType} from '../base/enum';
import {Exception} from '../base/exception';
import {RuntimeTable} from '../base/runtime_table';
import {Journal} from '../cache/journal';
import {TableDiff} from '../cache/table_diff';
import {BaseTable} from '../schema/base_table';
import {DatabaseSchema} from '../schema/database_schema';

import {BackStore} from './back_store';
import {MemoryTable} from './memory_table';
import {MemoryTx} from './memory_tx';
import {RawBackStore} from './raw_back_store';
import {Tx} from './tx';

export class Memory implements BackStore {
  private tables: Map<string, MemoryTable>;

  constructor(private schema: DatabaseSchema) {
    this.tables = new Map<string, MemoryTable>();
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  init(onUpgrade?: (db: RawBackStore) => Promise<void>): Promise<void> {
    // Memory does not uses raw back store, just ignore the onUpgrade function.
    (this.schema.tables() as BaseTable[]).forEach(
      table => this.initTable(table),
      this
    );
    return Promise.resolve();
  }

  getTableInternal(tableName: string): RuntimeTable {
    const table = this.tables.get(tableName) || null;
    if (table === null) {
      // 101: Table {0} not found.
      throw new Exception(ErrorCode.TABLE_NOT_FOUND, tableName);
    }
    return table;
  }

  createTx(type: TransactionType, scope: BaseTable[], journal?: Journal): Tx {
    return new MemoryTx(this, type, journal);
  }

  close(): void {
    // No op.
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  subscribe(handler: (diffs: TableDiff[]) => void): void {
    // Not supported.
  }

  // Unsubscribe current change handler.
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  unsubscribe(handler: (diffs: TableDiff[]) => void): void {
    // Not supported.
  }

  // Notifies registered observers with table diffs.
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  notify(changes: TableDiff[]): void {
    // Not supported.
  }

  supportsImport(): boolean {
    return true;
  }

  peek(): Map<string, MemoryTable> {
    return this.tables;
  }

  // Creates a new empty table in the database. It is a no-op if a table with
  // the given name already exists.
  // NOTE: the return value is not ported because it's not used.
  private createTable(tableName: string): void {
    if (!this.tables.has(tableName)) {
      this.tables.set(tableName, new MemoryTable());
    }
  }

  private initTable(tableSchema: BaseTable): void {
    this.createTable(tableSchema.getName());

    if (tableSchema.persistentIndex()) {
      tableSchema.getIndices().forEach(indexSchema => {
        this.createTable(indexSchema.getNormalizedName());
      }, this);

      // Creating RowId index table.
      this.createTable(tableSchema.getRowIdIndexName());
    }
  }
}
