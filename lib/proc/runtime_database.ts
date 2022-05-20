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
import {ExternalChangeObserver} from '../backstore/external_change_observer';
import {IndexedDB} from '../backstore/indexed_db';
import {Memory} from '../backstore/memory';
import {ObservableStore} from '../backstore/observable_store';
import {WebSql} from '../backstore/web_sql';
import {Capability} from '../base/capability';
import {DatabaseConnection} from '../base/database_connection';
import {DataStoreType, ErrorCode, TransactionType} from '../base/enum';
import {Exception} from '../base/exception';
import {Global} from '../base/global';
import {Inspector} from '../base/inspect';
import {ObserverRegistry} from '../base/observer_registry';
import {ObserverCallback} from '../base/observer_registry_entry';
import {PayloadType, Row} from '../base/row';
import {Service} from '../base/service';
import {Transaction} from '../base/transaction';
import {DefaultCache} from '../cache/default_cache';
import {Prefetcher} from '../cache/prefetcher';
import {MemoryIndexStore} from '../index/memory_index_store';
import {DeleteBuilder} from '../query/delete_builder';
import {InsertBuilder} from '../query/insert_builder';
import {SelectBuilder} from '../query/select_builder';
import {SelectQuery} from '../query/select_query';
import {UpdateBuilder} from '../query/update_builder';
import {Column} from '../schema/column';
import {ConnectOptions} from '../schema/connect_options';
import {DatabaseSchema} from '../schema/database_schema';
import {Table} from '../schema/table';

import {DefaultQueryEngine} from './default_query_engine';
import {ExportTask} from './export_task';
import {ImportTask} from './import_task';
import {Runner} from './runner';
import {RuntimeTransaction} from './runtime_transaction';

declare global {
  interface Window {
    '#lfInspect': Function;
    '#lfRowId': Function;
  }
}

export class RuntimeDatabase implements DatabaseConnection {
  private schema: DatabaseSchema;
  private isActive: boolean;
  private runner!: Runner;
  private observeExternalChanges: boolean;

  constructor(private global: Global) {
    this.schema = global.getService(Service.SCHEMA);

    // Whether this connection to the database is active.
    this.isActive = false;

    // Observe external changes, set for non-local persistence storage.
    // This was for Firebase but the TypeScript version does not support it.
    // Kept to allow future integration with other cloud backend.
    this.observeExternalChanges = false;
  }

  init(options?: ConnectOptions): Promise<RuntimeDatabase> {
    // The SCHEMA might have been removed from this.global in the case where
    // Database#close() was called, therefore it needs to be re-added.
    this.global.registerService(Service.SCHEMA, this.schema);
    this.global.registerService(Service.CACHE, new DefaultCache(this.schema));
    const backStore = this.createBackStore(this.schema, options);
    this.global.registerService(Service.BACK_STORE, backStore);
    const indexStore = new MemoryIndexStore();
    this.global.registerService(Service.INDEX_STORE, indexStore);
    const onUpgrade = options ? options.onUpgrade : undefined;
    return backStore
      .init(onUpgrade)
      .then(() => {
        this.global.registerService(
          Service.QUERY_ENGINE,
          new DefaultQueryEngine(this.global)
        );
        this.runner = new Runner();
        this.global.registerService(Service.RUNNER, this.runner);
        this.global.registerService(
          Service.OBSERVER_REGISTRY,
          new ObserverRegistry()
        );
        return indexStore.init(this.schema);
      })
      .then(() => {
        if (this.observeExternalChanges) {
          const externalChangeObserver = new ExternalChangeObserver(
            this.global
          );
          externalChangeObserver.startObserving();
        }
        if (options && options['enableInspector'] && window) {
          // Exposes a global '#lfExport' method, that can be used by the
          // Lovefield Inspector Devtools Chrome extension.
          window.top['#lfInspect'] = Inspector.inspect;

          // TypeScript port specific: this is needed for perf benchmark.
          window.top['#lfRowId'] = Row.getNextId;
        }
        const prefetcher = new Prefetcher(this.global);
        return prefetcher.init(this.schema);
      })
      .then(() => {
        this.isActive = true;
        return this;
      });
  }

  getGlobal(): Global {
    return this.global;
  }

  getSchema(): DatabaseSchema {
    return this.schema;
  }

  select(...columns: Column[]): SelectQuery {
    this.checkActive();
    return new SelectBuilder(this.global, columns);
  }

  insert(): InsertBuilder {
    this.checkActive();
    return new InsertBuilder(this.global);
  }

  insertOrReplace(): InsertBuilder {
    this.checkActive();
    return new InsertBuilder(this.global, /* allowReplace */ true);
  }

  update(table: Table): UpdateBuilder {
    this.checkActive();
    return new UpdateBuilder(this.global, table);
  }

  delete(): DeleteBuilder {
    this.checkActive();
    return new DeleteBuilder(this.global);
  }

  observe(builder: SelectQuery, callback: ObserverCallback): void {
    this.checkActive();
    const observerRegistry = this.global.getService(Service.OBSERVER_REGISTRY);
    observerRegistry.addObserver(builder, callback);
  }

  unobserve(builder: SelectQuery, callback: ObserverCallback): void {
    this.checkActive();
    const observerRegistry = this.global.getService(Service.OBSERVER_REGISTRY);
    observerRegistry.removeObserver(builder, callback);
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  createTransaction(type?: TransactionType): Transaction {
    this.checkActive();
    return new RuntimeTransaction(this.global);
  }

  close(): void {
    try {
      const backStore = this.global.getService(Service.BACK_STORE);
      backStore.close();
    } catch (e) {
      // Swallow the exception if DB is not initialized yet.
    }
    this.global.clear();
    this.isActive = false;
  }

  export(): Promise<object> {
    this.checkActive();
    const task = new ExportTask(this.global);
    return this.runner.scheduleTask(task).then(results => {
      return results[0].getPayloads()[0];
    });
  }

  import(d: object): Promise<object[]> {
    const data = d as PayloadType;
    this.checkActive();
    const task = new ImportTask(this.global, data);
    return this.runner.scheduleTask(task);
  }

  isOpen(): boolean {
    return this.isActive;
  }

  private checkActive(): void {
    if (!this.isActive) {
      throw new Exception(ErrorCode.CONNECTION_CLOSED);
    }
  }

  private createBackStore(
    schema: DatabaseSchema,
    options?: ConnectOptions
  ): BackStore {
    let backStore: BackStore;

    if (Global.get().getOptions().memoryOnly) {
      backStore = new Memory(schema);
      return backStore;
    }

    let dataStoreType: DataStoreType;
    if (options === undefined || options.storeType === undefined) {
      const capability = Capability.get();
      dataStoreType = capability.indexedDb
        ? DataStoreType.INDEXED_DB
        : capability.webSql
        ? DataStoreType.WEB_SQL
        : DataStoreType.MEMORY;
    } else {
      dataStoreType = options.storeType;
    }

    switch (dataStoreType) {
      case DataStoreType.INDEXED_DB:
        backStore = new IndexedDB(this.global, schema);
        break;

      case DataStoreType.MEMORY:
        backStore = new Memory(schema);
        break;

      case DataStoreType.OBSERVABLE_STORE:
        backStore = new ObservableStore(schema);
        break;

      case DataStoreType.WEB_SQL:
        backStore = new WebSql(
          this.global,
          schema,
          options ? options.websqlDbSize : undefined
        );
        break;

      default:
        // We no longer support FIREBASE.
        // 300: Not supported.
        throw new Exception(ErrorCode.NOT_SUPPORTED);
    }

    return backStore;
  }
}
