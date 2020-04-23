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

import { PayloadType, RawRow, Row } from '../base/row';

import { Page } from './page';
import { RawBackStore } from './raw_back_store';

export class IndexedDBRawBackStore implements RawBackStore {
  static convert(value: unknown): string | number | boolean | null {
    let ret: string | number | boolean | null = null;
    if (value instanceof ArrayBuffer) {
      ret = Row.binToHex(value);
    } else if (value instanceof Date) {
      ret = value.getTime();
    } else {
      ret = value as string | number | boolean;
    }
    return ret;
  }

  constructor(
    private version: number,
    private db: IDBDatabase,
    private tx: IDBTransaction,
    private bundleMode: boolean
  ) {}

  getRawDBInstance(): IDBDatabase {
    return this.db;
  }

  getRawTransaction(): IDBTransaction {
    return this.tx;
  }

  dropTable(tableName: string): Promise<void> {
    return new Promise((resolve, reject) => {
      try {
        this.db.deleteObjectStore(tableName);
      } catch (e) {
        reject(e);
        return;
      }
      resolve();
    });
  }

  addTableColumn(
    tableName: string,
    columnName: string,
    defaultValue: string | number | boolean | Date | ArrayBuffer | null
  ): Promise<void> {
    const value = IndexedDBRawBackStore.convert(defaultValue);
    return this.transformRows(tableName, (row: Row) => {
      row.payload()[columnName] = value;
    });
  }

  dropTableColumn(tableName: string, columnName: string): Promise<void> {
    return this.transformRows(tableName, (row: Row) => {
      delete row.payload()[columnName];
    });
  }

  renameTableColumn(
    tableName: string,
    oldColumnName: string,
    newColumnName: string
  ): Promise<void> {
    return this.transformRows(tableName, (row: Row) => {
      row.payload()[newColumnName] = row.payload()[oldColumnName];
      delete row.payload()[oldColumnName];
    });
  }

  createRow(payload: PayloadType): Row {
    const data: PayloadType = {};
    Object.keys(payload).forEach(key => {
      data[key] = IndexedDBRawBackStore.convert(payload[key] as unknown);
    });
    return Row.create(data);
  }

  getVersion(): number {
    return this.version;
  }

  dump(): Promise<object> {
    const tables = this.db.objectStoreNames;
    const promises: Array<Promise<object>> = [];
    for (let i = 0; i < tables.length; ++i) {
      const tableName = tables.item(i) as string;
      promises.push(this.dumpTable(tableName));
    }

    return Promise.all(promises).then(tableDumps => {
      const results: PayloadType = {};
      tableDumps.forEach((tableDump, index) => {
        results[tables.item(index) as string] = tableDump;
      });
      return results;
    });
  }

  private openCursorForWrite(
    tableName: string,
    loopFunc: (cursor: IDBCursorWithValue) => void,
    endFunc: (cursor: IDBObjectStore) => void
  ): Promise<void> {
    return new Promise((resolve, reject) => {
      let req: IDBRequest;
      let store: IDBObjectStore;
      try {
        store = this.tx.objectStore(tableName);
        req = store.openCursor();
      } catch (e) {
        reject(e);
        return;
      }
      req.onsuccess = ev => {
        const cursor = req.result as IDBCursorWithValue;
        if (cursor) {
          loopFunc(cursor);
          cursor.continue();
        } else {
          endFunc(store);
          resolve();
        }
      };
      req.onerror = reject;
    });
  }

  private transformRows(
    tableName: string,
    rowFn: (row: Row) => void
  ): Promise<void> {
    const loopFunc = (cursor: IDBCursorWithValue) => {
      const row = Row.deserialize(cursor.value);
      rowFn(row);
      cursor.update(row.serialize());
    };

    const loopFuncBundle = (cursor: IDBCursorWithValue) => {
      const page = Page.deserialize(cursor.value);
      const data = page.getPayload();
      Object.keys(data).forEach(rowId => {
        const row = Row.deserialize(data[rowId] as RawRow);
        rowFn(row);
        data[rowId] = row.serialize();
      });
      cursor.update(page.serialize());
    };

    const endFunc = () => {
      return;
    };
    return this.openCursorForWrite(
      tableName,
      this.bundleMode ? loopFuncBundle : loopFunc,
      endFunc
    );
  }

  private getTableRows(tableName: string): Promise<RawRow[]> {
    const results: RawRow[] = [];
    return new Promise((resolve, reject) => {
      let req: IDBRequest;
      try {
        req = this.tx.objectStore(tableName).openCursor();
      } catch (e) {
        reject(e);
        return;
      }
      req.onsuccess = (ev: Event) => {
        const cursor: IDBCursorWithValue = req.result;
        if (cursor) {
          if (this.bundleMode) {
            const page = Page.deserialize(cursor.value);
            const data = page.getPayload();
            Object.keys(data).forEach(rowId => {
              results.push(data[rowId] as RawRow);
            });
          } else {
            results.push(cursor.value);
          }
          cursor.continue();
        } else {
          resolve(results);
        }
      };
      req.onerror = reject;
    });
  }

  private dumpTable(tableName: string): Promise<object[]> {
    return this.getTableRows(tableName).then(rawRows =>
      rawRows.map(rawRow => rawRow.value as object)
    );
  }
}
