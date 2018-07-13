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

import {BackStore} from '../backstore/back_store';
import {TransactionType} from '../base/enum';
import {ErrorCode, Exception} from '../base/exception';
import {RuntimeTable} from '../base/runtime_table';
import {Journal} from '../cache/journal';
import {TableDiff} from '../cache/table_diff';
import {Database} from '../schema/database';
import {Table} from '../schema/table';

import {LocalStorageTable} from './local_storage_table';
import {LocalStorageTx} from './local_storage_tx';
import {RawBackStore} from './raw_back_store';
import {Tx} from './tx';

type StorageEventHandler = (ev: StorageEvent) => any;

// A backing store implementation using LocalStorage. It can hold at most 10MB
// of data, depending on browser. This backing store is experimental.
//
// Format of LocalStorage:
//
// namespace.version# Version of this database
// namespace.tableName Serialized object of the table
export class LocalStorage implements BackStore {
  private schema: Database;
  private tables: Map<string, LocalStorageTable>;
  private changeHandler: null|((changes: TableDiff[]) => void);
  private listener: null|StorageEventHandler;

  constructor(schema: Database) {
    this.schema = schema;
    this.tables = new Map<string, LocalStorageTable>();
    this.changeHandler = null;
    this.listener = null;
  }

  public init(onUpgrade?: (db: RawBackStore) => Promise<void>): Promise<any> {
    return new Promise<any>((resolve, reject) => {
      this.initSync();
      resolve();
    });
  }

  public createTx(type: TransactionType, scope: Table[], journal?: Journal):
      Tx {
    return new LocalStorageTx(this, type, journal);
  }

  public close(): void {
    // Do nothing.
  }

  public getTableInternal(tableName: string): RuntimeTable {
    if (!this.tables.has(tableName)) {
      // 101: Table {0} not found.
      throw new Exception(ErrorCode.TABLE_NOT_FOUND, tableName);
    }

    return this.tables.get(tableName) as RuntimeTable;
  }

  public subscribe(handler: (diffs: TableDiff[]) => void): void {
    this.changeHandler = handler;
    if (this.listener) {
      return;
    }

    this.listener = this.onStorageEvent.bind(this) as StorageEventHandler;
    window.addEventListener('storage', this.listener, false);
  }

  public unsubscribe(handler: (diffs: TableDiff[]) => void): void {
    if (this.listener) {
      window.removeEventListener('storage', this.listener, false);
      this.listener = null;
      this.changeHandler = null;
    }
  }

  public notify(changes: TableDiff[]): void {
    if (this.changeHandler) {
      this.changeHandler(changes);
    }
  }

  public supportsImport(): boolean {
    return false;
  }

  // Flushes changes to local storage.
  public commit(): void {
    this.tables.forEach((table) => table.commit());
  }

  // Synchronous version of init()
  private initSync(): void {
    if (!window.localStorage) {
      // 359: LocalStorage is not supported by platform
      throw new Exception(ErrorCode.LOCAL_STORAGE_NOT_PROVIDED);
    }

    const versionKey = `${this.schema.name()}.version#`;
    const version = window.localStorage.getItem(versionKey);
    if (version !== undefined && version !== null) {
      if (version !== this.schema.version().toString()) {
        // TODO(arthurhsu): implement upgrade logic
        // 360: Not implemented yet.
        throw new Exception(ErrorCode.NOT_IMPLEMENTED);
      }
      this.loadTables();
    } else {
      this.loadTables();
      window.localStorage.setItem(versionKey, this.schema.version().toString());
      this.commit();
    }
  }

  private loadTables(): void {
    const prefix = this.schema.name() + '.';
    this.schema.tables().forEach((table) => {
      const tableName = table.getName();
      this.tables.set(tableName, new LocalStorageTable(prefix + tableName));
      if (table.persistentIndex()) {
        const indices = table.getIndices();
        indices.forEach((index) => {
          const indexName = index.getNormalizedName();
          this.tables.set(indexName, new LocalStorageTable(prefix + indexName));
        }, this);
      }
    }, this);
  }

  private onStorageEvent(ev: StorageEvent): void {
    const key = ev.key as string;
    if (ev.storageArea !== window.localStorage ||
        key.indexOf(this.schema.name() + '.') !== 0) {
      return;
    }

    const newValue = window.localStorage.getItem(key);
    let newData = {};
    if (newValue !== null) {
      try {
        // Retrieves the new value from local storage. IE does not offer that in
        // the StorageEvent.
        newData = JSON.parse(newValue);
      } catch (e) {
        return;
      }
    }

    const tableName = key.slice(this.schema.name().length + 1);
    const table = this.tables.get(tableName);
    if (table && this.changeHandler) {
      this.changeHandler([table.diff(newData)]);
    }
  }
}
