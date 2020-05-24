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

import {ErrorCode} from '../base/enum';
import {Exception} from '../base/exception';
import {Global} from '../base/global';
import {RawRow, Row} from '../base/row';
import {RuntimeTable} from '../base/runtime_table';

export class ObjectStore implements RuntimeTable {
  constructor(
    private store: IDBObjectStore,
    private deserializeFn: (raw: RawRow) => Row
  ) {}

  get(ids: number[]): Promise<Row[]> {
    if (ids.length === 0) {
      const options = Global.get().getOptions();
      return options.useGetAll ? this.getAllBulk() : this.getAllWithCursor();
    }

    // Chrome IndexedDB is slower when using a cursor to iterate through a big
    // table. A faster way is to just get everything individually within a
    // transaction.
    const promises = ids.map((id, index) => {
      return new Promise((resolve, reject) => {
        let request: IDBRequest;
        try {
          request = this.store.get(id);
        } catch (e) {
          reject(e);
          return;
        }
        request.onerror = reject;
        request.onsuccess = ev => {
          resolve(this.deserializeFn((ev.target as IDBRequest).result));
        };
      });
    }, this);
    return Promise.all(promises) as Promise<Row[]>;
  }

  put(rows: Row[]): Promise<void> {
    if (rows.length === 0) {
      return Promise.resolve();
    }

    const promises = rows.map(row => {
      return this.performWriteOp(() => {
        // TODO(dpapad): Surround this with try catch, otherwise some errors
        // don't surface to the console.
        return this.store.put(row.serialize());
      });
    }, this);

    return Promise.all(promises).then(() => {
      return;
    });
  }

  remove(
    ids: number[],
    disableClearTableOptimization?: boolean
  ): Promise<void> {
    const deleteByIdsFn = () => {
      const promises = ids.map(id =>
        this.performWriteOp(() => this.store.delete(id))
      );

      return Promise.all(promises).then(() => {
        return;
      });
    };

    if (disableClearTableOptimization) {
      return deleteByIdsFn();
    }

    return new Promise((resolve, reject) => {
      const request = this.store.count();
      request.onsuccess = ev => {
        if (
          ids.length === 0 ||
          (ev.target as IDBRequest).result === ids.length
        ) {
          // Remove all
          this.performWriteOp(() => this.store.clear()).then(resolve, reject);
          return;
        }

        deleteByIdsFn().then(resolve, reject);
      };

      request.onerror = reject;
    });
  }

  // Reads everything from data store, using a cursor.
  private getAllWithCursor(): Promise<Row[]> {
    return new Promise((resolve, reject) => {
      const rows: Row[] = [];
      let request: IDBRequest;
      try {
        request = this.store.openCursor();
      } catch (e) {
        reject(e);
        return;
      }

      request.onerror = reject;
      request.onsuccess = () => {
        const cursor: IDBCursorWithValue = request.result;
        if (cursor) {
          rows.push(this.deserializeFn(cursor.value));
          cursor.continue();
        } else {
          resolve(rows);
        }
      };
    });
  }

  // Reads everything from data store, using IDBObjectStore#getAll.
  private getAllBulk(): Promise<Row[]> {
    return new Promise((resolve, reject) => {
      let request: IDBRequest;
      try {
        // TODO(dpapad): getAll is still experimental (and hidden behind a flag)
        // on both Chrome and Firefox. Add it to the externs once a flag is no
        // longer required.
        request = this.store.getAll();
      } catch (e) {
        reject(new Exception(ErrorCode.CANT_LOAD_IDB, e.name, e.message));
        return;
      }

      request.onerror = reject;
      request.onsuccess = () => {
        try {
          const rows = request.result.map((rawRow: RawRow) =>
            this.deserializeFn(rawRow)
          );
          resolve(rows);
        } catch (e) {
          reject(new Exception(ErrorCode.CANT_READ_IDB, e.name, e.message));
        }
      };
    });
  }

  private performWriteOp(reqFactory: () => IDBRequest): Promise<void> {
    return new Promise((resolve, reject) => {
      let request: IDBRequest;
      try {
        request = reqFactory();
      } catch (e) {
        reject(e);
        return;
      }
      request.onsuccess = ev => resolve();
      request.onerror = reject;
    });
  }
}
