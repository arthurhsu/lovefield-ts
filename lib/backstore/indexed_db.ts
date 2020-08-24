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
import {Global} from '../base/global';
import {Row} from '../base/row';
import {RuntimeTable} from '../base/runtime_table';
import {Journal} from '../cache/journal';
import {TableDiff} from '../cache/table_diff';
import {BaseTable} from '../schema/base_table';
import {DatabaseSchema} from '../schema/database_schema';

import {BackStore} from './back_store';
import {IndexedDBRawBackStore} from './indexed_db_raw_back_store';
import {IndexedDBTx} from './indexed_db_tx';
import {Page} from './page';
import {RawBackStore} from './raw_back_store';
import {Tx} from './tx';

// IndexedDB-backed back store.
//
// The backstore supports "Bundle Mode", which will bundle 2^BUNDLE_EXPONENT
// logical rows into a physical row (called bundled page) and store it in DB.
// The reason behind this is to workaround IndexedDB spec design flaw in loading
// large tables. Say one wanted to load all rows from table, the implementation
// based on current spec is
//
// var req = objectStore.openCursor();
// req.onsuccess = function() {
//   if (cursor) {
//     // get one row by using cursor.value
//     cursor.continue();
//   } else {
//     // finished
//   }
// };
//
// Which involves N calls of cursor.continue and N eventing of onsuccess. This
// is very expensive when N is big. WebKit needs 57us for firing an event on an
// HP Z620, and the wall clock time for loading 100K rows will be 5.7s just for
// firing N onsuccess events.
//
// As a result, the bundle mode is created to bundle many rows into a physical
// row to workaround overhead caused by number of rows.
//
// And yes, after 4 years when this comment was originally written (2014->2018),
// not much has changed and the statement above is still true.

export class IndexedDB implements BackStore {
  private db!: IDBDatabase;
  private bundledMode: boolean;

  constructor(private global: Global, private schema: DatabaseSchema) {
    this.bundledMode = this.schema.pragma().enableBundledMode || false;
  }

  init(upgrade?: (db: RawBackStore) => Promise<void>): Promise<IDBDatabase> {
    const indexedDB = window.indexedDB;
    if (indexedDB === undefined || indexedDB === null) {
      // 352: IndexedDB is not supported by platform.
      throw new Exception(ErrorCode.IDB_NOT_PROVIDED);
    }

    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const onUpgrade = upgrade || ((rawDb: RawBackStore) => Promise.resolve());

    return new Promise((resolve, reject) => {
      let request: IDBOpenDBRequest;
      try {
        request = indexedDB.open(this.schema.name(), this.schema.version());
      } catch (e) {
        reject(e);
        return;
      }

      // Event sequence for IndexedDB database upgrade:
      // indexedDB.open found version mismatch
      //   --> onblocked (maybe, see http://www.w3.org/TR/IndexedDB 3.3.7)
      //   --> onupgradeneeded (when IndexedDB is ready to handle the
      //   connection)
      //   --> onsuccess
      // As a result the onblocked event is not handled deliberately.
      request.onerror = e => {
        const error: DOMException = (e.target as IDBRequest)
          .error as DOMException;
        // 361: Unable to open IndexedDB database.
        reject(
          new Exception(ErrorCode.CANT_OPEN_IDB, error.name, error.message)
        );
      };
      request.onupgradeneeded = ev => {
        this.onUpgradeNeeded(onUpgrade, ev).then(() => {
          return;
        }, reject);
      };
      request.onsuccess = ev => {
        this.db = (ev.target as IDBRequest).result as IDBDatabase;
        this.scanRowId().then(rowId => {
          Row.setNextIdIfGreater(rowId + 1);
          resolve(this.db);
        });
      };
    });
  }

  createTx(type: TransactionType, scope: BaseTable[], journal?: Journal): Tx {
    const nativeTx = this.db.transaction(
      this.getIndexedDBScope(scope),
      type === TransactionType.READ_ONLY ? 'readonly' : 'readwrite'
    );
    return new IndexedDBTx(
      this.global,
      nativeTx,
      type,
      this.bundledMode,
      journal
    );
  }

  close(): void {
    this.db.close();
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  getTableInternal(tableName: string): RuntimeTable {
    // 511: IndexedDB tables needs to be acquired from transactions.
    throw new Exception(ErrorCode.CANT_GET_IDB_TABLE);
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  subscribe(handler: (diffs: TableDiff[]) => void): void {
    // Not supported yet.
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  unsubscribe(handler: (diffs: TableDiff[]) => void): void {
    // Not supported yet.
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  notify(changes: TableDiff[]): void {
    // Not supported yet.
  }

  supportsImport(): boolean {
    return true;
  }

  peek(): IDBDatabase {
    return this.db;
  }

  private onUpgradeNeeded(
    onUpgrade: (raw: RawBackStore) => Promise<void>,
    ev: IDBVersionChangeEvent
  ): Promise<void> {
    const db = (ev.target as IDBRequest).result as IDBDatabase;
    const tx = (ev.target as IDBRequest).transaction as IDBTransaction;
    const rawDb = new IndexedDBRawBackStore(
      ev.oldVersion,
      db,
      tx,
      this.bundledMode
    );
    this.removeIndexTables(db, tx);
    this.createTables(db);
    return onUpgrade(rawDb);
  }

  // Removes Lovefield-created index tables.
  private removeIndexTables(db: IDBDatabase, tx: IDBTransaction): void {
    const storeNames = [] as string[];
    for (let i = 0; i < db.objectStoreNames.length; ++i) {
      const name = db.objectStoreNames.item(i) as string;
      // Remove all persisted indices.
      if (name.indexOf('.') !== -1) {
        storeNames.push(name);
      }
    }
    storeNames.forEach(store => {
      try {
        db.deleteObjectStore(store);
      } catch (e) {
        // Ignore the error.
      }
    });
  }

  // Creates tables if they had not existed in the database.
  private createTables(db: IDBDatabase): void {
    (this.schema.tables() as BaseTable[]).forEach(table => {
      this.createObjectStoresForTable(db, table);
    }, this);
  }

  private createObjectStoresForTable(
    db: IDBDatabase,
    tableSchema: BaseTable
  ): void {
    if (!db.objectStoreNames.contains(tableSchema.getName())) {
      db.createObjectStore(tableSchema.getName(), {keyPath: 'id'});
    }

    if (tableSchema.persistentIndex()) {
      const tableIndices = tableSchema.getIndices();
      tableIndices.forEach(indexSchema => {
        this.createIndexTable(db, indexSchema.getNormalizedName());
      }, this);

      // Creating RowId index table.
      this.createIndexTable(db, tableSchema.getRowIdIndexName());
    }
  }

  // Creates a backing store corresponding to a persisted index.
  private createIndexTable(db: IDBDatabase, indexName: string): void {
    if (!db.objectStoreNames.contains(indexName)) {
      db.createObjectStore(indexName, {keyPath: 'id'});
    }
  }

  private getIndexedDBScope(scope: BaseTable[]): string[] {
    const indexedDBScope = new Set<string>();

    scope.forEach(tableSchema => {
      // Adding user-defined table to the scope.
      indexedDBScope.add(tableSchema.getName());

      // If the table has persisted indices, adding the corresponding backing
      // store tables to the scope too.
      if (tableSchema.persistentIndex()) {
        const tableIndices = tableSchema.getIndices();
        tableIndices.forEach(indexSchema =>
          indexedDBScope.add(indexSchema.getNormalizedName())
        );

        // Adding RowId backing store name to the scope.
        indexedDBScope.add(tableSchema.getRowIdIndexName());
      }
    });

    return Array.from(indexedDBScope.values());
  }

  private scanRowId(txIn?: IDBTransaction): Promise<number> {
    const tableNames = this.schema.tables().map(table => table.getName());

    const db = this.db;
    let maxRowId = 0;

    const extractRowId = (cursor: IDBCursorWithValue) => {
      if (this.bundledMode) {
        const page = Page.deserialize(cursor.value);
        return Object.keys(page.getPayload()).reduce((prev, cur) => {
          return Math.max(prev, Number(cur));
        }, 0);
      }

      return cursor.key as number;
    };

    const scanTableRowId = (tableName: string) => {
      return new Promise<number>((resolve, reject) => {
        let req: IDBRequest;
        try {
          const tx = txIn || db.transaction([tableName]);
          req = tx.objectStore(tableName).openCursor(undefined, 'prev');
        } catch (e) {
          reject(e);
          return;
        }
        req.onsuccess = ev => {
          const cursor = (ev.target as IDBRequest).result as IDBCursorWithValue;
          if (cursor) {
            // Since the cursor is traversed in the reverse direction, only the
            // first record needs to be examined to determine the max row ID.
            maxRowId = Math.max(maxRowId, extractRowId(cursor));
          }
          resolve(maxRowId);
        };
        req.onerror = () => resolve(maxRowId);
      });
    };

    const execSequentially = (): Promise<void> => {
      if (tableNames.length === 0) {
        return Promise.resolve();
      }

      const tableName = tableNames.shift() as string;
      return scanTableRowId(tableName).then(execSequentially);
    };

    return new Promise(resolve => {
      execSequentially().then(() => resolve(maxRowId));
    });
  }
}
