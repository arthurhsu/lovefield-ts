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

import { IndexedDB } from '../../lib/backstore/indexed_db';
import { Memory } from '../../lib/backstore/memory';
import { Capability } from '../../lib/base/capability';
import { Service } from '../../lib/base/service';
import { DefaultCache } from '../../lib/cache/default_cache';
import { BaseTable } from '../../lib/schema/base_table';
import { Builder } from '../../lib/schema/builder';
import { DatabaseSchema } from '../../lib/schema/database_schema';
import { getHrDbSchemaBuilder } from '../../testing/hr_schema/hr_schema_builder';

const assert = chai.assert;

describe('BackStoreInit', () => {
  if (!Capability.get().indexedDb) {
    return;
  }

  it('init_IndexedDB_NonBundled', () => {
    const builder = getHrDbSchemaBuilder();
    return checkInit_IndexedDB(builder);
  });

  it('init_IndexedDB_Bundled', () => {
    const builder = getHrDbSchemaBuilder();
    builder.setPragma({ enableBundledMode: true });
    const cache = new DefaultCache(builder.getSchema());
    builder.getGlobal().registerService(Service.CACHE, cache);

    return checkInit_IndexedDB(builder);
  });

  // Initializes a DB with the given schema and performs assertions on the
  // tables that were created.
  async function checkInit_IndexedDB(builder: Builder): Promise<void> {
    const schema = builder.getSchema();
    assert.isTrue((schema.table('Holiday') as BaseTable).persistentIndex());

    const indexedDb = new IndexedDB(builder.getGlobal(), schema);
    await indexedDb.init();
    const createdTableNames = new Set<string>();
    const names = indexedDb.peek().objectStoreNames as DOMStringList;
    for (let i = 0; i < names.length; ++i) {
      createdTableNames.add(names.item(i) as string);
    }
    assertUserTables(schema, createdTableNames);
    assertIndexTables(schema, createdTableNames);
  }

  it('init_Memory', async () => {
    const schema = getHrDbSchemaBuilder().getSchema();
    assert.isTrue((schema.table('Holiday') as BaseTable).persistentIndex());

    const memoryDb = new Memory(schema);
    await memoryDb.init();
    const createdTableNames = new Set<string>(memoryDb.peek().keys());
    assertUserTables(schema, createdTableNames);
    assertIndexTables(schema, createdTableNames);
  });

  // Asserts that an object store was created for each user-defined table.
  function assertUserTables(
    schema: DatabaseSchema,
    tableNames: Set<string>
  ): void {
    schema.tables().forEach(tableSchema => {
      assert.isTrue(tableNames.has(tableSchema.getName()));
    });
  }

  // Asserts that an object store was created for each Index instance
  // that belongs to a user-defined table that has "persistentIndex" enabled.
  function assertIndexTables(
    schema: DatabaseSchema,
    tableNames: Set<string>
  ): void {
    schema.tables().forEach(tableSchema => {
      const tbl = tableSchema as BaseTable;
      tbl.getIndices().forEach(indexSchema => {
        assert.equal(
          tbl.persistentIndex(),
          tableNames.has(indexSchema.getNormalizedName())
        );
      });

      // Checking whether backing store for RowId index was created.
      assert.equal(
        tbl.persistentIndex(),
        tableNames.has(tbl.getRowIdIndexName())
      );
    });
  }
});
