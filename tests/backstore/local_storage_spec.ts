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

import * as chai from 'chai';

import { LocalStorage } from '../../lib/backstore/local_storage';
import { Capability } from '../../lib/base/capability';
import { Global } from '../../lib/base/global';
import { Service } from '../../lib/base/service';
import { Cache } from '../../lib/cache/cache';
import { DefaultCache } from '../../lib/cache/default_cache';
import { MemoryIndexStore } from '../../lib/index/memory_index_store';
import { DatabaseSchema } from '../../lib/schema/database_schema';
import { ScudTester } from '../../testing/backstore/scud_tester';
import { getMockSchemaBuilder } from '../../testing/mock_schema_builder';

const assert = chai.assert;

describe('LocalStorage', () => {
  let db: LocalStorage;
  let cache: Cache;
  let schema: DatabaseSchema;

  if (!Capability.get().localStorage) {
    return; // skip this test.
  }

  beforeEach(() => {
    const indexStore = new MemoryIndexStore();
    schema = getMockSchemaBuilder().getSchema();
    cache = new DefaultCache(schema);

    const global = Global.get();
    global.registerService(Service.CACHE, cache);
    global.registerService(Service.INDEX_STORE, indexStore);
    global.registerService(Service.SCHEMA, schema);

    window.localStorage.clear();
    db = new LocalStorage(schema);

    return db.init();
  });

  // Tests that the backstore.Memory is instantiated according to the schema
  // instance that is passed into its constructor.
  it('construct', () => {
    assert.isTrue(schema.tables().length > 0);
    schema.tables().forEach(table => {
      assert.isNotNull(db.getTableInternal(table.getName()));
    });
  });

  it('getTable_NonExisting', () => {
    assert.throws(() => db.getTableInternal('nonExistingTableName'));
  });

  it('SCUD', () => {
    const scudTester = new ScudTester(db, Global.get(), () => {
      const newDb = new LocalStorage(schema);
      newDb.init();
      return newDb;
    });

    return scudTester.run();
  });
});
