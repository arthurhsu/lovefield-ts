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
import {DataStoreType, TransactionType} from '../base/enum';
import {ErrorCode, Exception} from '../base/exception';
import {Global} from '../base/global';
import {Inspector} from '../base/inspect';
import {ObserverRegistry} from '../base/observer_registry';
import {ObserverCallback} from '../base/observer_registry_entry';
import {Service} from '../base/service';
import {Transaction} from '../base/transaction';
import {DefaultCache} from '../cache/default_cache';
import {Prefetcher} from '../cache/prefetcher';
import {Flags} from '../gen/flags';
import {MemoryIndexStore} from '../index/memory_index_store';
import {DeleteBuilder} from '../query/delete_builder';
import {InsertBuilder} from '../query/insert_builder';
import {SelectBuilder} from '../query/select_builder';
import {SelectQuery} from '../query/select_query';
import {UpdateBuilder} from '../query/update_builder';
import {Column} from '../schema/column';
import {ConnectOptions} from '../schema/connect_options';
import {Database} from '../schema/database';
import {Table} from '../schema/table';

import {DefaultQueryEngine} from './default_query_engine';
import {ExportTask} from './export_task';
import {ImportTask} from './import_task';
import {Runner} from './runner';
import {RuntimeTransaction} from './runtime_transaction';

export class RuntimeDatabase implements DatabaseConnection {
  private schema: Database;
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

  public init(options?: ConnectOptions): Promise<RuntimeDatabase> {
    // The SCHEMA might have been removed from this.global in the case where
    // Database#close() was called, therefore it needs to be re-added.
    this.global.registerService(Service.SCHEMA, this.schema);
    this.global.registerService(Service.CACHE, new DefaultCache(this.schema));
    const backStore = this.createBackStore(this.schema, options);
    this.global.registerService(Service.BACK_STORE, backStore);
    const indexStore = new MemoryIndexStore();
    this.global.registerService(Service.INDEX_STORE, indexStore);
    const onUpgrade = options ? options.onUpgrade : undefined;
    return backStore.init(onUpgrade)
        .then(() => {
          this.global.registerService(
              Service.QUERY_ENGINE, new DefaultQueryEngine(this.global));
          this.runner = new Runner();
          this.global.registerService(Service.RUNNER, this.runner);
          this.global.registerService(
              Service.OBSERVER_REGISTRY, new ObserverRegistry());
          return indexStore.init(this.schema);
        })
        .then(() => {
          if (this.observeExternalChanges) {
            const externalChangeObserver =
                new ExternalChangeObserver(this.global);
            externalChangeObserver.startObserving();
          }
          if (options && options['enableInspector'] && window) {
            // Exposes a global '#lfExport' method, that can be used by the
            // Lovefield Inspector Devtools Chrome extension.
            window.top['#lfInspect'] = Inspector.inspect;
          }
          const prefetcher = new Prefetcher(this.global);
          return prefetcher.init(this.schema);
        })
        .then(() => {
          this.isActive = true;
          return this;
        });
  }

  public getGlobal(): Global {
    return this.global;
  }

  public getSchema(): Database {
    return this.schema;
  }

  public select(...columns: Column[]): SelectQuery {
    this.checkActive();
    return new SelectBuilder(this.global, columns);
  }

  public insert(): InsertBuilder {
    this.checkActive();
    return new InsertBuilder(this.global);
  }

  public insertOrReplace(): InsertBuilder {
    this.checkActive();
    return new InsertBuilder(this.global, /* allowReplace */ true);
  }

  public update(table: Table): UpdateBuilder {
    this.checkActive();
    return new UpdateBuilder(this.global, table);
  }

  public delete(): DeleteBuilder {
    this.checkActive();
    return new DeleteBuilder(this.global);
  }

  public observe(builder: SelectQuery, callback: ObserverCallback): void {
    this.checkActive();
    const observerRegistry = this.global.getService(Service.OBSERVER_REGISTRY);
    observerRegistry.addObserver(builder, callback);
  }

  public unobserve(builder: SelectQuery, callback: ObserverCallback): void {
    this.checkActive();
    const observerRegistry = this.global.getService(Service.OBSERVER_REGISTRY);
    observerRegistry.removeObserver(builder, callback);
  }

  public createTransaction(type?: TransactionType): Transaction {
    this.checkActive();
    return new RuntimeTransaction(this.global);
  }

  public close(): void {
    try {
      const backStore = this.global.getService(Service.BACK_STORE);
      backStore.close();
    } catch (e) {
      // Swallow the exception if DB is not initialized yet.
    }
    this.global.clear();
    this.isActive = false;
  }

  public export(): Promise<object> {
    this.checkActive();
    const task = new ExportTask(this.global);
    return this.runner.scheduleTask(task).then((results) => {
      return results[0].getPayloads()[0];
    });
  }

  // TODO(arthurhsu): clang-format is unable to properly format this file after
  // this import function.
  // clang-format off
  public import(data: object): Promise<any> {
    this.checkActive();
    const task = new ImportTask(this.global, data);
    return this.runner.scheduleTask(task);
  }

  public isOpen(): boolean {
    return this.isActive;
  }

  private checkActive(): void {
    if (!this.isActive) {
      throw new Exception(ErrorCode.CONNECTION_CLOSED);
    }
  }

  private createBackStore(
      schema: Database, options?: ConnectOptions): BackStore {
    let backStore: BackStore;

    if (Flags.MEMORY_ONLY) {
      backStore = new Memory(schema);
      return backStore;
    }

    let dataStoreType: DataStoreType;
    if (options === undefined || options.storeType === undefined) {
    const capability = Capability.get();
    dataStoreType = capability.indexedDb ?
        DataStoreType.INDEXED_DB :
        (capability.webSql ? DataStoreType.WEB_SQL : DataStoreType.MEMORY);
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
            this.global, schema, options ? options.websqlDbSize : undefined);
        break;

      default:
        // We no longer support FIREBASE.
        // 300: Not supported.
        throw new Exception(ErrorCode.NOT_SUPPORTED);
    }

    return backStore;
  }
  // clang-format on
}
