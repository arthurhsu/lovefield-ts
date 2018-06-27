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

import {TransactionType} from '../base/enum';
import {ErrorCode, Exception} from '../base/exception';
import {RuntimeTable} from '../base/runtime_table';
import {Journal} from '../cache/journal';
import {TableDiff} from '../cache/table_diff';
import {Database} from '../schema/database';
import {Table} from '../schema/table';
import {BackStore} from './back_store';
import {MemoryTable} from './memory_table';
import {MemoryTx} from './memory_tx';
import {Tx} from './tx';

export class Memory implements BackStore {
  private tables: Map<string, MemoryTable>;

  constructor(private schema: Database) {
    this.schema = schema;
    this.tables = new Map<string, MemoryTable>();
  }

  public init(onUpgrade?: (db: object) => Promise<void>): Promise<void> {
    // Memory does not uses raw back store, just ignore the onUpgrade function.
    this.schema.tables().forEach((table) => this.initTable(table), this);
    return Promise.resolve();
  }

  public getTableInternal(tableName: string): RuntimeTable {
    const table = this.tables.get(tableName) || null;
    if (table === null) {
      // 101: Table {0} not found.
      throw new Exception(ErrorCode.TABLE_NOT_FOUND, tableName);
    }
    return table;
  }

  public createTx(type: TransactionType, scope: Table[], journal?: Journal):
      Tx {
    return new MemoryTx(this, type, journal);
  }

  public close(): void {
    // No op.
  }

  public subscribe(handler: (diffs: TableDiff[]) => void): void {
    // Not supported.
  }

  // Unsubscribe current change handler.
  public unsubscribe(handler: (diffs: TableDiff[]) => void): void {
    // Not supported.
  }

  // Notifies registered observers with table diffs.
  public notify(changes: TableDiff[]): void {
    // Not supported.
  }

  public supportsImport(): boolean {
    return true;
  }

  // Creates a new empty table in the database. It is a no-op if a table with
  // thegiven name already exists.
  // NOTE: the return value is not ported because it's not used.
  private createTable(tableName: string): void {
    if (!this.tables.has(tableName)) {
      this.tables.set(tableName, new MemoryTable());
    }
  }

  private initTable(tableSchema: Table): void {
    this.createTable(tableSchema.getName());

    if (tableSchema.persistentIndex()) {
      tableSchema.getIndices().forEach((indexSchema) => {
        this.createTable(indexSchema.getNormalizedName());
      }, this);

      // Creating RowId index table.
      this.createTable(tableSchema.getRowIdIndexName());
    }
  }
}
