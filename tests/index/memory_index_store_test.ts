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

import {BTree} from '../../lib/index/btree';
import {ComparatorFactory} from '../../lib/index/comparator_factory';
import {IndexStore} from '../../lib/index/index_store';
import {MemoryIndexStore} from '../../lib/index/memory_index_store';
import {RowId} from '../../lib/index/row_id';
import {RuntimeIndex} from '../../lib/index/runtime_index';
import {BaseTable} from '../../lib/schema/base_table';
import {DatabaseSchema} from '../../lib/schema/database_schema';
import {IndexImpl} from '../../lib/schema/index_impl';
import {getMockSchemaBuilder} from '../../testing/mock_schema_builder';

const assert = chai.assert;

describe('MemoryIndexStore', () => {
  let indexStore: IndexStore;
  let schema: DatabaseSchema;
  let sandbox: sinon.SinonSandbox;

  before(() => {
    sandbox = sinon.createSandbox();
  });

  beforeEach(() => {
    indexStore = new MemoryIndexStore();
    schema = getMockSchemaBuilder().getSchema();
  });

  afterEach(() => {
    sandbox.restore();
  });

  // Asserts that the indices corresponding to the given index names are of a
  // specific type.
  function assertIndicesType(indexNames: string[], type: string): void {
    indexNames.forEach(indexName => {
      const index = indexStore.get(indexName) as RuntimeIndex;
      assert.equal(type, index.constructor.name);
    });
  }

  it('memoryIndexStore', async () => {
    const tableA = schema.table('tableA') as BaseTable;
    const tableB = schema.table('tableB') as BaseTable;
    sandbox.stub(tableB, 'persistentIndex').callsFake(() => true);
    const tableF = schema.table('tableF') as BaseTable;

    assert.isFalse(tableA.persistentIndex());
    assert.isTrue(tableB.persistentIndex());
    assert.isFalse(tableF.persistentIndex());

    await indexStore.init(schema);

    // Table A index names.
    const tableAPkIndex = 'tableA.pkTableA';
    const tableANameIndex = 'tableA.idxName';
    const tableARowIdIndex = 'tableA.#';

    // Table B index names.
    const tableBPkIndex = 'tableB.pkTableB';
    const tableBNameIndex = 'tableB.idxName';
    const tableBRowIdIndex = 'tableB.#';

    // Table F index names.
    const tableFNameIndex = 'tableF.idxName';
    const tableFRowIdIndex = 'tableF.#';

    // Table G index names.
    const tableGFkIndex = 'tableG.fk_Id';
    const tableGFkIndex2 = 'tableG.idx_Id';

    // Table J index names.
    const tableJIdIndex = 'tableJ.idxId';

    assertIndicesType(
      [tableARowIdIndex, tableBRowIdIndex, tableFRowIdIndex],
      'RowId'
    );
    assertIndicesType([tableAPkIndex], 'BTree');
    assertIndicesType([tableGFkIndex], 'BTree');
    assertIndicesType([tableGFkIndex2], 'BTree');
    assertIndicesType([tableANameIndex], 'BTree');
    assertIndicesType([tableBPkIndex, tableBNameIndex], 'BTree');
    // Single-column nullable index is typed NullableIndex.
    assertIndicesType([tableFNameIndex], 'NullableIndex');
    // Cross-column nullable index is typed BTree.
    assertIndicesType([tableJIdIndex], 'BTree');
  });

  // Tests the case of calling getTableIndices() for a table that has no
  // indices.
  it('getTableIndices_NoIndices', async () => {
    await indexStore.init(schema);
    const tableWithNoIndexName = schema.table('tableC') as BaseTable;
    // There should be at least one row id index.
    assert.equal(
      1,
      indexStore.getTableIndices(tableWithNoIndexName.getName()).length
    );
    assert.isNotNull(indexStore.get(tableWithNoIndexName.getRowIdIndexName()));
  });

  // Tests that when searching for a table's indices, the table name is used as
  // a prefix only.
  it('getTableIndices_Prefix', () => {
    const index1 = new RowId('MovieActor.#');
    const index2 = new RowId('Actor.#');
    const index3 = new RowId('ActorMovie.#');

    indexStore.set('MovieActor', index1);
    indexStore.set('Actor', index2);
    indexStore.set('ActorMovie', index3);

    const tableIndices = indexStore.getTableIndices('Actor');
    assert.equal(1, tableIndices.length);
    assert.equal(index2.getName(), tableIndices[0].getName());
  });

  // Tests that set() is correctly replacing any existing indices.
  it('set', async () => {
    const tableSchema = schema.table('tableA') as BaseTable;
    const indexSchema = tableSchema.getIndices()[0] as IndexImpl;

    await indexStore.init(schema);
    const indexBefore = indexStore.get(indexSchema.getNormalizedName());
    const comparator = ComparatorFactory.create(indexSchema);
    const newIndex = new BTree(
      indexSchema.getNormalizedName(),
      comparator,
      indexSchema.isUnique
    );
    indexStore.set(tableSchema.getName(), newIndex);

    const indexAfter = indexStore.get(indexSchema.getNormalizedName());
    assert.isTrue(indexBefore !== indexAfter);
    assert.equal(newIndex, indexAfter);
  });
});
