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

import { BackStore } from '../../lib/backstore/back_store';
import { DataStoreType, TransactionType } from '../../lib/base/enum';
import { TableType } from '../../lib/base/private_enum';
import { Row } from '../../lib/base/row';
import { Service } from '../../lib/base/service';
import { op } from '../../lib/fn/op';
import { BTree, BTreeNode } from '../../lib/index/btree';
import { ComparatorFactory } from '../../lib/index/comparator_factory';
import { Key } from '../../lib/index/key_range';
import { RowId } from '../../lib/index/row_id';
import { RuntimeDatabase } from '../../lib/proc/runtime_database';
import { Table } from '../../lib/schema/table';
import { BaseTable } from '../../lib/schema/base_table';
import { IndexImpl } from '../../lib/schema/index_impl';
import { getHrDbSchemaBuilder } from '../../testing/hr_schema/hr_schema_builder';

const assert = chai.assert;

describe('PersistentIndex', () => {
  let db: RuntimeDatabase;
  let table: Table;
  let table2: Table;
  let backStore: BackStore;
  let sampleRows: Row[];
  let sampleRows2: Row[];

  // This is a hack to access private variables and change it.
  // tslint:disable:no-any
  const maxCount = (BTreeNode as any).MAX_COUNT;
  const maxKeyLen = (BTreeNode as any).MAX_KEY_LEN;
  const minKeyLen = (BTreeNode as any).MIN_KEY_LEN;

  function stubBTreeParam(): void {
    (BTreeNode as any).MAX_COUNT = 5;
    (BTreeNode as any).MAX_KEY_LEN = 5 - 1;
    (BTreeNode as any).MIN_KEY_LEN = 5 >> 1;
    (Row as any).nextId = 0;
  }

  function resetBTreeParam(): void {
    (BTreeNode as any).MAX_COUNT = maxCount;
    (BTreeNode as any).MAX_KEY_LEN = maxKeyLen;
    (BTreeNode as any).MIN_KEY_LEN = minKeyLen;
  }
  // tslint:enable:no-any

  before(() => {
    stubBTreeParam();
  });

  after(() => {
    resetBTreeParam();
  });

  beforeEach(async () => {
    db = (await getHrDbSchemaBuilder().connect({
      storeType: DataStoreType.MEMORY,
    })) as RuntimeDatabase;
    backStore = db.getGlobal().getService(Service.BACK_STORE);
    table = db.getSchema().table('Holiday');
    table2 = db.getSchema().table('CrossColumnTable');
    sampleRows = generateSampleRows();
    sampleRows2 = generateSampleRows2();
  });

  afterEach(() => db.close());

  // Performs insert, update, replace, delete operations and verifies that
  // persisted indices are being updated appropriately on disk.
  it('PersistedIndices', async () => {
    // Inserts 5 records to the database.
    await db
      .insert()
      .into(table)
      .values(generateSampleRows())
      .exec();
    await assertAllIndicesPopulated(table, sampleRows);

    // Updates the 'name' field of rows 1 and 3.
    const updatedDate = new Date(0);
    sampleRows[1].payload()['begin'] = updatedDate;
    sampleRows[3].payload()['begin'] = updatedDate;

    await db
      .update(table)
      .where(
        table
          .col('name')
          .in([
            sampleRows[1].payload()['name'],
            sampleRows[3].payload()['name'],
          ] as string[])
      )
      .set(table.col('begin'), updatedDate)
      .exec();
    await assertAllIndicesPopulated(table, sampleRows);

    // Replaces rows 1 and 3.
    const sampleRow1 = sampleRows[1];
    sampleRow1.payload()['begin'] = new Date(2015, 0, 20);
    sampleRow1.payload()['end'] = new Date(2015, 0, 21);

    const sampleRow3 = sampleRows[1];
    sampleRow3.payload()['begin'] = new Date(2015, 6, 3);
    sampleRow3.payload()['end'] = new Date(2015, 6, 5);

    const replacedRow1 = table.createRow({
      begin: sampleRow1.payload()['begin'],
      end: sampleRow1.payload()['end'],
      name: sampleRow1.payload()['name'],
    });
    const replacedRow3 = table.createRow({
      begin: sampleRow3.payload()['begin'],
      end: sampleRow3.payload()['end'],
      name: sampleRow3.payload()['name'],
    });

    await db
      .insertOrReplace()
      .into(table)
      .values([replacedRow1, replacedRow3])
      .exec();
    await assertAllIndicesPopulated(table, sampleRows);

    // Deletes rows 2 and 3.
    const removedRows = sampleRows.splice(2, 2);

    await db
      .delete()
      .from(table)
      .where(
        table
          .col('name')
          .in([
            removedRows[0].payload()['name'],
            removedRows[1].payload()['name'],
          ] as string[])
      )
      .exec();
    await assertAllIndicesPopulated(table, sampleRows);

    sampleRows = [];

    await db
      .delete()
      .from(table)
      .exec();
    return assertAllIndicesPopulated(table, sampleRows);
  });

  // Generates sample records to be used for testing.
  function generateSampleRows(): Row[] {
    return [
      table.createRow({
        begin: new Date(2014, 0, 20),
        end: new Date(2014, 0, 21),
        name: 'Holiday0',
      }),
      table.createRow({
        begin: new Date(2014, 6, 3),
        end: new Date(2014, 6, 5),
        name: 'Holiday1',
      }),
      table.createRow({
        begin: new Date(2014, 11, 25),
        end: new Date(2014, 11, 26),
        name: 'Holiday2',
      }),
      table.createRow({
        begin: new Date(2014, 0, 1),
        end: new Date(2014, 0, 2),
        name: 'Holiday3',
      }),
      table.createRow({
        begin: new Date(2014, 10, 26),
        end: new Date(2014, 10, 28),
        name: 'Holiday4',
      }),
    ];
  }

  // Asserts that all indices are populated with the given rows.
  // |rows| are the only rows that should be present in the persistent index
  // tables.
  function assertAllIndicesPopulated(t: Table, rows: Row[]): Promise<void> {
    const targetTable = t as BaseTable;
    const tx = backStore.createTx(TransactionType.READ_ONLY, [
      table as BaseTable,
    ]);

    const tableIndices = targetTable.getIndices();
    const promises = tableIndices.map(indexSchema => {
      const indexName = indexSchema.getNormalizedName();
      return tx.getTable(indexName, Row.deserialize, TableType.INDEX).get([]);
    });
    promises.push(
      tx
        .getTable(
          targetTable.getRowIdIndexName(),
          Row.deserialize,
          TableType.INDEX
        )
        .get([])
    );

    return Promise.all(promises).then(results => {
      const rowIdIndexResults = results.splice(results.length - 1, 1)[0];
      assertRowIdIndex(targetTable, rowIdIndexResults, rows.length);

      results.forEach((indexResults, i) => {
        const indexSchema = tableIndices[i] as IndexImpl;
        assertIndexContents(indexSchema, indexResults, rows);
      });
    });
  }

  // Asserts that the contents of the given persistent index appear as expected
  // in the backing store. |serializedRows| are the serialized version of the
  // index. |dataRows| are the rows that hold the actual data (not index data).
  function assertIndexContents(
    indexSchema: IndexImpl,
    serializedRows: Row[],
    dataRows: Row[]
  ): void {
    // Expecting at least one row for each index.
    assert.isTrue(serializedRows.length >= 1);

    // Reconstructing the index and ensuring it contains all expected keys.
    const comparator = ComparatorFactory.create(indexSchema);
    const btreeIndex = BTree.deserialize(
      comparator,
      indexSchema.getNormalizedName(),
      indexSchema.isUnique,
      serializedRows
    );
    assert.equal(dataRows.length, btreeIndex.getRange().length);

    dataRows.forEach(row => {
      const expectedKey = row.keyOfIndex(
        indexSchema.getNormalizedName()
      ) as Key;
      assert.isTrue(btreeIndex.containsKey(expectedKey));
    });
  }

  // Asserts that the contents of the RowId index appear as expected in the
  // backing store.
  function assertRowIdIndex(
    targetTable: BaseTable,
    serializedRows: Row[],
    expectedSize: number
  ): void {
    assert.equal(1, serializedRows.length);
    const rowIdIndex = RowId.deserialize(
      targetTable.getRowIdIndexName(),
      serializedRows
    );
    assert.equal(expectedSize, rowIdIndex.getRange().length);
  }

  function generateSampleRows2(): Row[] {
    return [
      table2.createRow({
        integer1: 1,
        integer2: 2,
        string1: 'A',
        string2: 'B',
      }),
      table2.createRow({
        integer1: 2,
        integer2: 3,
        string1: 'A',
        string2: null,
      }),
      table2.createRow({
        integer1: 3,
        integer2: 4,
        string1: null,
        string2: 'B',
      }),
      table2.createRow({
        integer1: 4,
        integer2: 5,
        string1: null,
        string2: null,
      }),
      table2.createRow({
        integer1: 5,
        integer2: 6,
        string1: 'C',
        string2: 'D',
      }),
    ];
  }

  // Performs insert, update, replace, delete operations and verifies that
  // persisted indices are being updated appropriately on disk.
  it('PersistedIndices_CrossColumn', async () => {
    await db
      .insert()
      .into(table2)
      .values(generateSampleRows2())
      .exec();
    await assertAllIndicesPopulated(table2, sampleRows2);

    sampleRows2[2].payload()['integer2'] = 33;
    sampleRows2[2].payload()['string2'] = 'RR';
    sampleRows2[4].payload()['integer1'] = 99;
    sampleRows2[4].payload()['string2'] = 'KK';

    let q1 = db
      .update(table2)
      .where(table2.col('integer1').eq(3))
      .set(table2.col('integer2'), 33)
      .set(table2.col('string2'), 'RR')
      .exec();
    let q2 = db
      .update(table2)
      .where(table2.col('string1').eq('C'))
      .set(table2.col('integer1'), 99)
      .set(table2.col('string2'), 'KK')
      .exec();
    await Promise.all([q1, q2]);
    await assertAllIndicesPopulated(table2, sampleRows2);

    // Deletes rows 2 and 3.
    sampleRows2.splice(3, 1);
    sampleRows2.splice(2, 1);

    q1 = db
      .delete()
      .from(table2)
      .where(
        op.and(table2.col('integer1').eq(3), table2.col('integer2').eq(33))
      )
      .exec();
    q2 = db
      .delete()
      .from(table2)
      .where(
        op.and(table2.col('string1').isNull(), table2.col('string2').isNull())
      )
      .exec();
    await Promise.all([q1, q2]);
    await assertAllIndicesPopulated(table2, sampleRows2);

    // Deletes all remaining rows.
    sampleRows2 = [];

    await db
      .delete()
      .from(table2)
      .exec();
    await assertAllIndicesPopulated(table2, sampleRows2);
  });
});
