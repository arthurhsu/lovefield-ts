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

import {assert} from '../base/assert';
import {Global} from '../base/global';
import {TableType} from '../base/private_enum';
import {Resolver} from '../base/resolver';
import {RawRow, Row} from '../base/row';
import {RuntimeTable} from '../base/runtime_table';
import {Service} from '../base/service';

import {Page} from './page';

// Table stream based on a given IndexedDB Object Store.
export class BundledObjectStore implements RuntimeTable {
  static forTableType(
    global: Global,
    store: IDBObjectStore,
    deserializeFn: (raw: RawRow) => Row,
    tableType: TableType
  ): RuntimeTable {
    const retrievePageFn =
      tableType === TableType.DATA
        ? BundledObjectStore.getDataTablePage.bind(null, global)
        : BundledObjectStore.getIndexTablePage;

    return new BundledObjectStore(store, deserializeFn, retrievePageFn);
  }

  // Retrieves a page for the case of a DATA table. It uses the Cache to
  // retrieve the rows that belong to the requested page.
  private static getDataTablePage(
    global: Global,
    tableName: string,
    pageId: number
  ): Page {
    const cache = global.getService(Service.CACHE);
    const range = Page.getPageRange(pageId);
    const rows = cache.getRange(tableName, range[0], range[1]);
    const page = new Page(pageId);
    page.setRows(rows);
    return page;
  }

  // Retrieves a page for the case of an INDEX table. It is basically a no-op
  // since the full index contents are rewritten every time.
  private static getIndexTablePage(tableName: string, pageId: number): Page {
    return new Page(pageId);
  }

  constructor(
    private store: IDBObjectStore,
    private deserializeFn: (raw: RawRow) => Row,
    private retrievePageFn: (name: string, page: number) => Page
  ) {}

  get(ids: number[]): Promise<Row[]> {
    if (ids.length === 0) {
      return this.getAll();
    }
    return this.getPagesByRowIds(ids).then(pages => {
      return ids.map(id => {
        const page = pages.get(Page.toPageId(id)) as Page;
        assert(page !== undefined, 'Containing page is empty');
        return this.deserializeFn(page.getPayload()[id] as RawRow);
      });
    });
  }

  put(rows: Row[]): Promise<void> {
    if (rows.length === 0) {
      return Promise.resolve();
    }

    const pages = new Map<number, Page>();
    rows.forEach(row => {
      const pageId = Page.toPageId(row.id());
      let page = pages.get(pageId) || null;
      if (page === null) {
        page = this.retrievePageFn(this.store.name, pageId);
      }
      page.setRows([row]);
      pages.set(pageId, page);
    }, this);

    const promises = Array.from(pages.values()).map(page => {
      return this.performWriteOp(() => {
        return this.store.put(page.serialize());
      });
    }, this);

    return Promise.all(promises).then(() => {
      return;
    });
  }

  remove(
    ids: number[],
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    disableClearTableOptimization?: boolean
  ): Promise<void> {
    if (ids.length === 0) {
      // Remove all
      return this.performWriteOp(() => this.store.clear());
    }

    const pages = new Map<number, Page>();
    ids.forEach(id => {
      const pageId = Page.toPageId(id);
      let page = pages.get(pageId) || null;
      if (page === null) {
        page = this.retrievePageFn(this.store.name, pageId);
      }
      page.removeRows([id]);
      pages.set(pageId, page);
    }, this);

    const promises = Array.from(pages.values()).map(page => {
      return this.performWriteOp(() => {
        return Object.keys(page.getPayload()).length === 0
          ? this.store.delete(page.getId())
          : this.store.put(page.serialize());
      });
    }, this);

    return Promise.all(promises).then(() => {
      return;
    });
  }

  private getPagesByRowIds(rowIds: number[]): Promise<Map<number, Page>> {
    const results = new Map<number, Page>();
    const resolver = new Resolver<Map<number, Page>>();
    const pageIds = Page.toPageIds(rowIds);

    // Chrome IndexedDB is slower when using a cursor to iterate through a big
    // table. A faster way is to just get everything individually within a
    // transaction.
    const promises = pageIds.map(id => {
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
          const page = Page.deserialize((ev.target as IDBRequest).result);
          results.set(page.getId(), page);
          resolve();
        };
      });
    }, this);

    Promise.all(promises).then(() => resolver.resolve(results));
    return resolver.promise;
  }

  // Reads everything from the data store
  private getAll(): Promise<Row[]> {
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
          const page = Page.deserialize(cursor.value);
          const data = page.getPayload();
          Object.keys(data).forEach(key =>
            rows.push(this.deserializeFn(data[key] as RawRow))
          );
          cursor.continue();
        } else {
          resolve(rows);
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
      request.onsuccess = () => resolve();
      request.onerror = reject;
    });
  }
}
