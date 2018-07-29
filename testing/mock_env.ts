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

import {BackStore} from '../lib/backstore/back_store';
import {DataStoreType} from '../lib/base/enum';
import {Global} from '../lib/base/global';
import {ObserverRegistry} from '../lib/base/observer_registry';
import {Row} from '../lib/base/row';
import {Service} from '../lib/base/service';
import {Cache} from '../lib/cache/cache';
import {IndexStore} from '../lib/index/index_store';
import {QueryEngine} from '../lib/proc/query_engine';
import {Runner} from '../lib/proc/runner';
import {RuntimeDatabase} from '../lib/proc/runtime_database';
import {DatabaseSchema} from '../lib/schema/database_schema';

export class MockEnv {
  public queryEngine!: QueryEngine;
  public runner!: Runner;
  public store!: BackStore;
  public cache!: Cache;
  public indexStore!: IndexStore;
  public observerRegistry!: ObserverRegistry;
  public db!: RuntimeDatabase;
  public global!: Global;

  constructor(public schema: DatabaseSchema) {}

  public init(): Promise<void> {
    const global = Global.get();
    this.global = global;
    global.registerService(Service.SCHEMA, this.schema);

    this.db = new RuntimeDatabase(global);
    return this.db.init({storeType: DataStoreType.MEMORY}).then(() => {
      this.cache = global.getService(Service.CACHE);
      this.store = global.getService(Service.BACK_STORE);
      this.queryEngine = global.getService(Service.QUERY_ENGINE);
      this.runner = global.getService(Service.RUNNER);
      this.indexStore = global.getService(Service.INDEX_STORE);
      this.observerRegistry = global.getService(Service.OBSERVER_REGISTRY);
    });
  }

  public addSampleData(): Promise<void> {
    const table = this.schema.tables()[0];
    const sampleDataCount = 9;
    const rows: Row[] = new Array(sampleDataCount);
    for (let i = 0; i < sampleDataCount; i++) {
      rows[i] = table.createRow({
        id: i.toString(),
        name: `dummyName${i}`,
      });
      rows[i].assignRowId(i);
    }
    return this.db.insert().into(table).values(rows).exec();
  }
}
