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
import * as sinon from 'sinon';

import {Row} from '../../lib/base/row';
import {Prefetcher} from '../../lib/cache/prefetcher';
import {BTree} from '../../lib/index/btree';
import {Key} from '../../lib/index/key_range';
import {MemoryIndexStore} from '../../lib/index/memory_index_store';
import {NullableIndex} from '../../lib/index/nullable_index';
import {RowId} from '../../lib/index/row_id';
import {RuntimeIndex} from '../../lib/index/runtime_index';
import {Table} from '../../lib/schema/table';
import {MockEnv} from '../../testing/mock_env';
import {getMockSchemaBuilder} from '../../testing/mock_schema_builder';

const assert = chai.assert;

describe('Prefetcher', () => {
  let env: MockEnv;
  let sandbox: sinon.SinonSandbox;

  beforeEach(() => {
    sandbox = sinon.createSandbox();
    env = new MockEnv(getMockSchemaBuilder().getSchema());
    sandbox.stub(env.schema.table('tableA'), 'persistentIndex')
        .callsFake(() => true);
    sandbox.stub(env.schema.table('tableF'), 'persistentIndex')
        .callsFake(() => true);
    return env.init();
  });

  afterEach(() => {
    sandbox.restore();
  });

  it('prefetcher', async () => {
    // Setup some data first.
    const tableSchema = env.schema.table('tableB');
    const rows = getSampleRows(tableSchema, 19, 0);

    const indices = env.indexStore.getTableIndices(tableSchema.getName());
    const rowIdIndex = indices[0];
    const pkIndex = indices[1];
    const nameIndex = indices[2];
    const table = env.store.getTableInternal(tableSchema.getName());

    await table.put(rows);
    assert.equal(0, env.cache.getCount());
    assert.sameDeepOrderedMembers([], rowIdIndex.get(1001));
    assert.equal(0, rowIdIndex.getRange().length);
    assert.equal(0, pkIndex.getRange().length);
    assert.equal(0, nameIndex.getRange().length);

    const prefetcher = new Prefetcher(env.global);
    await prefetcher.init(env.schema);

    assert.equal(rows.length, env.cache.getCount());
    assert.equal(rows[1], env.cache.get(pkIndex.get(1001)[0]));

    // Checking that indices have the right size after initialization.
    assert.equal(rows.length, rowIdIndex.getRange().length);
    assert.equal(rows.length, pkIndex.getRange().length);
    assert.equal(rows.length, nameIndex.getRange().length);
  });

  // Tests that Prefetcher is reconstructing persisted indices from the backing
  // store.
  it('init_PersistentIndices', async () => {
    const tableSchema = env.schema.table('tableA');
    const rows = getSampleRows(tableSchema, 10, 0);

    await simulatePersistedIndices(tableSchema, rows);
    const prefetcher = new Prefetcher(env.global);
    await prefetcher.init(env.schema);

    const rowIdIndex =
        env.indexStore.get(tableSchema.getRowIdIndexName()) as RuntimeIndex;
    assert.isTrue(rowIdIndex instanceof RowId);
    assert.equal(rows.length, rowIdIndex.getRange().length);

    // Check that remaining indices have been properly reconstructed.
    const indices =
        env.indexStore.getTableIndices(tableSchema.getName()).slice(1);
    indices.forEach((index) => {
      assert.isTrue(index instanceof BTree);
      assert.equal(rows.length, index.getRange().length);
    });
  });

  // Tests that Prefetcher is correctly reconstructing persisted indices from
  // the backing store for the case where indices with nullable columns exist.
  it('init_PersistentIndices_NullableIndex', async () => {
    const tableSchema = env.schema.table('tableF');
    const nonNullKeyRows = 4;
    const nullKeyRows = 5;
    const rows = getSampleRows(tableSchema, nonNullKeyRows, nullKeyRows);

    await simulatePersistedIndices(tableSchema, rows);
    const prefetcher = new Prefetcher(env.global);
    await prefetcher.init(env.schema);
    // Check that RowId index has been properly reconstructed.
    const rowIdIndex =
        env.indexStore.get(tableSchema.getRowIdIndexName()) as RowId;
    assert.isTrue(rowIdIndex instanceof RowId);
    assert.equal(rows.length, rowIdIndex.getRange().length);

    // Check that remaining indices have been properly reconstructed.
    const indices =
        env.indexStore.getTableIndices(tableSchema.getName()).slice(1);
    indices.forEach((index) => {
      assert.isTrue(index instanceof NullableIndex);
      assert.equal(rows.length, index.getRange().length);
      assert.equal(nullKeyRows, index.get(null as any as Key).length);
    });
  });

  function getSampleRows(
      tableSchema: Table, rowCount: number, nullNameRowCount: number): Row[] {
    const rows = [];

    const rowCountFirstHalf = Math.floor(rowCount / 2);
    for (let i = 0; i < rowCountFirstHalf; i++) {
      const row = tableSchema.createRow({
        id: 1000 + i,
        name: 'name' + i,
      });
      row.assignRowId(i + 2);
      rows.push(row);
    }

    // Generating a few rows with non-unique values for the "name" field. This
    // allows tests in this file to trigger the case where an index
    // corresponding to a non-unique field is initialized.
    for (let i = rowCountFirstHalf; i < rowCount; i++) {
      const row = tableSchema.createRow({
        id: 1000 + i,
        name: 'nonUniqueName',
      });
      row.assignRowId(i + 2);
      rows.push(row);
    }

    for (let i = rowCount; i < rowCount + nullNameRowCount; i++) {
      const row = tableSchema.createRow({
        id: 1000 + i,
        name: null,
      });
      row.assignRowId(i + 2);
      rows.push(row);
    }

    return rows;
  }

  // Populates the backstore tables that correspond to indices for the given
  // table with dummy data. Used for testing prefetcher#init.
  // Resolves when index contents have been persisted in the backing store.
  function simulatePersistedIndices(
      tableSchema: Table, tableRows: Row[]): Promise<void> {
    const tempIndexStore = new MemoryIndexStore();
    return tempIndexStore.init(env.schema).then(() => {
      const indices = tempIndexStore.getTableIndices(tableSchema.getName());
      tableRows.forEach((row) => {
        indices.forEach((index) => {
          const key = row.keyOfIndex(index.getName());
          index.add(key, row.id());
        });
      });

      const serializedIndices = indices.map((index) => {
        return index.serialize();
      });
      const whenIndexTablesPopulated = indices.map((index, i) => {
        const indexTable = env.store.getTableInternal(index.getName());
        return indexTable.put(serializedIndices[i]);
      });

      return Promise.all(whenIndexTablesPopulated).then(() => {
        return;
      });
    });
  }
});
