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

import { ExternalChangeObserver } from '../../lib/backstore/external_change_observer';
import { ObservableStore } from '../../lib/backstore/observable_store';
import { DataStoreType, ErrorCode, TransactionType } from '../../lib/base/enum';
import { TableType } from '../../lib/base/private_enum';
import { Resolver } from '../../lib/base/resolver';
import { PayloadType, Row } from '../../lib/base/row';
import { Service } from '../../lib/base/service';
import { Journal } from '../../lib/cache/journal';
import { RuntimeDatabase } from '../../lib/proc/runtime_database';
import { BaseTable } from '../../lib/schema/base_table';
import { Table } from '../../lib/schema/table';
import { MockStore } from '../../testing/backstore/mock_store';
import { getHrDbSchemaBuilder } from '../../testing/hr_schema/hr_schema_builder';
import { MockDataGenerator } from '../../testing/hr_schema/mock_data_generator';
import { TestUtil } from '../../testing/test_util';

const assert = chai.assert;

describe('ExternalChangeObserver', () => {
  let db: RuntimeDatabase;
  let j: Table;
  let sampleJobs: Row[];
  let mockStore: MockStore;

  beforeEach(async () => {
    db = (await getHrDbSchemaBuilder().connect({
      storeType: DataStoreType.OBSERVABLE_STORE,
    })) as RuntimeDatabase;
    j = db.getSchema().table('Job');

    const dataGenerator = new MockDataGenerator();
    dataGenerator.generate(
      /* jobCount */ 10,
      /* employeeCount */ 10,
      /* departmentCount */ 0
    );
    sampleJobs = dataGenerator.sampleJobs;

    const backStore = db
      .getGlobal()
      .getService(Service.BACK_STORE) as ObservableStore;
    mockStore = new MockStore(backStore);

    const externalChangeObserver = new ExternalChangeObserver(db.getGlobal());
    externalChangeObserver.startObserving();
  });

  afterEach(() => db.close());

  it('externalChangesApplied', async () => {
    const initialRows = sampleJobs;
    const notDeletedRows = sampleJobs.slice(0, sampleJobs.length / 2);
    const deletedRows = sampleJobs.slice(sampleJobs.length / 2);

    const modifiedRow = j.createRow();
    modifiedRow.payload()['id'] = 'DummyJobId';
    modifiedRow.assignRowId(sampleJobs[0].id());

    const extractResultsPk = (res: PayloadType[]) => {
      return res.map(obj => obj[j.col('id').getName()]);
    };
    const extractRowsPk = (rows: Row[]) => rows.map(row => row.payload()['id']);

    // Simulate an external insertion of rows.
    await simulateInsertionModification(j, sampleJobs);
    let results: PayloadType[] = (await db
      .select()
      .from(j)
      .orderBy(j.col('id'))
      .exec()) as PayloadType[];
    // Ensure that external insertion change is detected and applied properly.
    assert.sameDeepOrderedMembers(
      extractRowsPk(initialRows),
      extractResultsPk(results)
    );

    // Simulate an external deletion of rows.
    await simulateDeletion(j, deletedRows);
    results = (await db
      .select()
      .from(j)
      .orderBy(j.col('id'))
      .exec()) as PayloadType[];
    // Ensure that external deletion change is detected and applied properly.
    assert.sameDeepOrderedMembers(
      extractRowsPk(notDeletedRows),
      extractResultsPk(results)
    );

    // Simulate an external modification of rows.
    await simulateInsertionModification(j, [modifiedRow]);
    results = (await db
      .select()
      .from(j)
      .where(j.col('id').eq(modifiedRow.payload()['id'] as string))
      .exec()) as PayloadType[];
    // Ensure that external modification change is detected and applied.
    assert.equal(1, results.length);
    assert.equal(
      modifiedRow.payload()['id'],
      results[0][j.col('id').getName()]
    );

    // Attempt to insert a row with an existing primary key.
    // Expecting a constraint error. This ensures that indices are updated
    // as a result of external changes.
    await TestUtil.assertPromiseReject(
      ErrorCode.DUPLICATE_KEYS, // 201: Duplicate keys are not allowed.
      db
        .insert()
        .into(j)
        .values([modifiedRow])
        .exec()
    );
  });

  // Tests that Lovefield observers are firing as a result of an external
  // backstore change.
  it('dbObserversFired', () => {
    const resolver = new Resolver<void>();

    const query = db.select().from(j);
    db.observe(query, changes => {
      assert.equal(sampleJobs.length, changes.length);
      changes.forEach(changeEvent => {
        assert.equal(1, changeEvent.addedCount);
      });
      resolver.resolve();
    });

    simulateInsertionModification(j, sampleJobs);
    return resolver.promise;
  });

  // Ensures that even in the case of an external change, Lovefield observers
  // are fired after every READ_WRITE transaction.
  it('order_Observer_ExternalChange', () => {
    const resolver = new Resolver<void>();

    const sampleJobs1 = sampleJobs.slice(0, sampleJobs.length / 2 - 1);
    const sampleJobs2 = sampleJobs.slice(sampleJobs.length / 2 - 1);

    const query = db.select().from(j);
    let counter = 0;
    db.observe(query, changes => {
      counter++;
      // Expecting the observer to be called twice, once when the db.insert()
      // query is completed, and once when the external change has been merged.
      if (counter === 1) {
        assert.equal(sampleJobs1.length, changes.length);
      } else if (counter === 2) {
        assert.equal(sampleJobs2.length, changes.length);
        resolver.resolve();
      }
    });

    Promise.all([
      db
        .insert()
        .into(j)
        .values(sampleJobs1)
        .exec(),
      simulateInsertionModification(j, sampleJobs2),
    ]);
    return resolver.promise;
  });

  // Simulates an external insertion/modification change.
  function simulateInsertionModification(
    tableSchema: Table,
    rows: Row[]
  ): Promise<unknown> {
    const tx = mockStore.createTx(
      TransactionType.READ_WRITE,
      [tableSchema],
      new Journal(
        db.getGlobal(),
        new Set<Table>([tableSchema])
      )
    );
    const table = tx.getTable(
      tableSchema.getName(),
      (tableSchema as BaseTable).deserializeRow.bind(tableSchema),
      TableType.DATA
    );
    table.put(rows);
    return tx.commit();
  }

  // Simulates an external deletion change.
  function simulateDeletion(tableSchema: Table, rows: Row[]): Promise<unknown> {
    const tx = mockStore.createTx(
      TransactionType.READ_WRITE,
      [tableSchema],
      new Journal(
        db.getGlobal(),
        new Set<Table>([tableSchema])
      )
    );
    const table = tx.getTable(
      tableSchema.getName(),
      (tableSchema as BaseTable).deserializeRow.bind(tableSchema),
      TableType.DATA
    );

    const rowIds = rows.map(row => row.id());
    table.remove(rowIds);
    return tx.commit();
  }
});
