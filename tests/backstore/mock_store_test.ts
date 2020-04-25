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

import {ObservableStore} from '../../lib/backstore/observable_store';
import {TransactionType} from '../../lib/base/enum';
import {Global} from '../../lib/base/global';
import {TableType} from '../../lib/base/private_enum';
import {Resolver} from '../../lib/base/resolver';
import {Row} from '../../lib/base/row';
import {Service} from '../../lib/base/service';
import {Cache} from '../../lib/cache/cache';
import {DefaultCache} from '../../lib/cache/default_cache';
import {Journal} from '../../lib/cache/journal';
import {MemoryIndexStore} from '../../lib/index/memory_index_store';
import {BaseTable} from '../../lib/schema/base_table';
import {DatabaseSchema} from '../../lib/schema/database_schema';
import {Table} from '../../lib/schema/table';
import {MockStore} from '../../testing/backstore/mock_store';
import {ScudTester} from '../../testing/backstore/scud_tester';
import {getMockSchemaBuilder} from '../../testing/mock_schema_builder';

const assert = chai.assert;

describe('MockStore', () => {
  let actualStore: ObservableStore;
  let mockStore: MockStore;
  let cache: Cache;
  let schema: DatabaseSchema;

  beforeEach(() => {
    const indexStore = new MemoryIndexStore();
    schema = getMockSchemaBuilder().getSchema();
    cache = new DefaultCache(schema);

    const global = Global.get();
    global.registerService(Service.CACHE, cache);
    global.registerService(Service.INDEX_STORE, indexStore);
    global.registerService(Service.SCHEMA, schema);

    actualStore = new ObservableStore(schema);
    mockStore = new MockStore(actualStore);

    return mockStore.init();
  });

  afterEach(() => Global.get().clear());

  // Tests that the testing.backstore.MockStore is instantiated according to the
  // schema instance that is passed into its constructor.
  it('construction', () => {
    assert.isTrue(schema.tables().length > 0);

    schema.tables().forEach(table => {
      assert.isNotNull(mockStore.getTableInternal(table.getName()));
    });
  });

  it('getTable_NonExisting', () => {
    assert.throws(() => mockStore.getTableInternal('nonExistingTableName'));
  });

  it('SCUD', () => {
    const scudTester = new ScudTester(mockStore, Global.get());
    return scudTester.run();
  });

  // Tests that when a backstore change is submitted via the MockStore
  // interface, observers of the actual backstore (the one registered in
  // Global) are notified.
  it('simulateExternalChange', async () => {
    const resolver = new Resolver();

    const tableSchema = schema.table('tableA');
    const rows: Row[] = new Array(10);
    for (let i = 0; i < rows.length; i++) {
      rows[i] = tableSchema.createRow({
        id: `id${i}`,
        name: `name${i}`,
      });
    }

    // Adding an observer in the actual backstore (the one that is registered in
    // Global).
    actualStore.subscribe(tableDiffs => {
      assert.equal(1, tableDiffs.length);
      assert.equal(tableSchema.getName(), tableDiffs[0].getName());
      assert.equal(5, tableDiffs[0].getAdded().size);
      assert.equal(0, tableDiffs[0].getModified().size);
      assert.equal(0, tableDiffs[0].getDeleted().size);

      resolver.resolve();
    });

    // Using the MockStore to simulate an external backstore change. Changes
    // that are triggered via the MockStore should result in events firing on
    // the actual backing store observers.
    const tx = mockStore.createTx(
      TransactionType.READ_WRITE,
      [tableSchema],
      new Journal(
        Global.get(),
        new Set<Table>([tableSchema])
      )
    );
    const table = tx.getTable(
      tableSchema.getName(),
      (tableSchema as BaseTable).deserializeRow.bind(tableSchema),
      TableType.DATA
    );

    // Insert 10 rows.
    try {
      await table.put(rows);

      // Delete the last 5 rows.
      const rowIds = rows.slice(rows.length / 2).map(row => row.id());
      await table.remove(rowIds);
      await tx.commit();
    } catch (e) {
      resolver.reject(e);
    }

    await resolver.promise;
  });
});
