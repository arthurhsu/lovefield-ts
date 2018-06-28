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

import {Memory} from '../backstore/memory';
import {DatabaseConnection} from '../base/database_connection';
import {TransactionType} from '../base/enum';
import {ErrorCode, Exception} from '../base/exception';
import {Global} from '../base/global';
import {ObserverRegistry} from '../base/observer_registry';
import {ObserverCallback} from '../base/observer_registry_entry';
import {Service} from '../base/service';
import {Transaction} from '../base/transaction';
import {DefaultCache} from '../cache/default_cache';
import {MemoryIndexStore} from '../index/memory_index_store';
import {DeleteBuilder} from '../query/delete_builder';
import {InsertBuilder} from '../query/insert_builder';
import {SelectQuery} from '../query/select_query';
import {UpdateBuilder} from '../query/update_builder';
import {Column} from '../schema/column';
import {ConnectOptions} from '../schema/connect_options';
import {Database} from '../schema/database';
import {Table} from '../schema/table';
import {QueryEngine} from './query_engine';
import {Runner} from './runner';

export class RuntimeDatabase implements DatabaseConnection {
  private schema: Database;
  private isActive: boolean;
  private runner!: Runner;

  constructor(private global: Global) {
    this.schema = global.getService(Service.SCHEMA);

    // Whether this connection to the database is active.
    this.isActive = false;
  }

  public init(options?: ConnectOptions): Promise<RuntimeDatabase> {
    // The SCHEMA might have been removed from this.global_ in the case where
    // Database#close() was called, therefore it needs to be re-added.
    this.global.registerService(Service.SCHEMA, this.schema);
    this.global.registerService(Service.CACHE, new DefaultCache(this.schema));

    // TODO(arthurhsu): implement
    /*
    return lf.base.init(this.global_, opt_options).then(function() {
          this.isActive_ = true;
          this.runner_ = this.global_.getService(lf.service.RUNNER);
          return this;
        }.bind(this));
    */
    this.isActive = true;
    this.runner = new Runner();
    this.global.registerService(Service.QUERY_ENGINE, {} as any as QueryEngine);
    this.global.registerService(Service.RUNNER, this.runner);
    this.global.registerService(Service.BACK_STORE, new Memory(this.schema));
    this.global.registerService(Service.INDEX_STORE, new MemoryIndexStore());
    this.global.registerService(
        Service.OBSERVER_REGISTRY, new ObserverRegistry());
    return this.global.getService(Service.INDEX_STORE)
        .init(this.schema)
        .then(() => this);
  }

  public getGlobal(): Global {
    return this.global;
  }

  public getSchema(): Database {
    return this.schema;
  }

  public select(...columns: Column[]): SelectQuery {
    /*
    this.checkActive();
    return new SelectBuilder(this.global_, columns);
    */
    throw new Exception(ErrorCode.NOT_IMPLEMENTED);
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

  public observe(query: SelectQuery, callback: ObserverCallback): void {
    this.checkActive();
    const observerRegistry = this.global.getService(Service.OBSERVER_REGISTRY);
    observerRegistry.addObserver(query, callback);
  }

  public unobserve(query: SelectQuery, callback: ObserverCallback): void {
    this.checkActive();
    const observerRegistry = this.global.getService(Service.OBSERVER_REGISTRY);
    observerRegistry.removeObserver(query, callback);
  }

  public createTransaction(type?: TransactionType): Transaction {
    /*
    this.checkActive();
    return new RuntimeTransaction(this.global);
    */
    throw new Exception(ErrorCode.NOT_IMPLEMENTED);
  }

  public close(): void {
    // TODO(arthurhsu): implement
    /*
    lf.base.closeDatabase(this.global_);
    */
    this.global.clear();
    this.isActive = false;
  }

  public export(): Promise<object> {
    /*
    this.checkActive_();
    var task = new lf.proc.ExportTask(this.global_);
    return this.runner_.scheduleTask(task).then(function(results) {
      return results[0].getPayloads()[0];
    });
    */
    throw new Exception(ErrorCode.NOT_IMPLEMENTED);
  }

  public import(data: object): Promise<void> {
    /*
    this.checkActive_();
    var task = new lf.proc.ImportTask(this.global_, data);
    return this.runner_.scheduleTask(task).then(function() {
      return null;
    });
    */
    throw new Exception(ErrorCode.NOT_IMPLEMENTED);
  }

  public isOpen(): boolean {
    return this.isActive;
  }

  private checkActive(): void {
    // clang-format off
    if (!this.isActive) {
      // 2: The database connection is not active.
      throw new Exception(ErrorCode.CONNECTION_CLOSED);
    }
    // clang-format on
  }
}
