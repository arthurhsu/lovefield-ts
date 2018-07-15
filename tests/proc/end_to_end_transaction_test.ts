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

import {TransactionStats} from '../../lib/backstore/transaction_stats';
import {DataStoreType, ErrorCode} from '../../lib/base/enum';
import {Global} from '../../lib/base/global';
import {ObserverCallback} from '../../lib/base/observer_registry_entry';
import {Resolver} from '../../lib/base/resolver';
import {Row} from '../../lib/base/row';
import {Transaction} from '../../lib/base/transaction';
import {fn} from '../../lib/fn/fn';
import {RuntimeDatabase} from '../../lib/proc/runtime_database';
import {Table} from '../../lib/schema/table';
import {getHrDbSchemaBuilder} from '../../testing/hr_schema/hr_schema_builder';
import {JobDataGenerator} from '../../testing/hr_schema/job_data_generator';
import {MockDataGenerator} from '../../testing/hr_schema/mock_data_generator';
import {TestUtil} from '../../testing/test_util';

const assert = chai.assert;

describe('EndToEndTransaction', () => {
  let db: RuntimeDatabase;
  let e: Table;
  let j: Table;
  let d: Table;
  let sampleJobs: Row[];
  let sampleEmployees: Row[];
  let sampleDepartments: Row[];
  let dataGenerator: MockDataGenerator;
  let global: Global;
  let tx: Transaction;

  beforeEach(async () => {
    db = await getHrDbSchemaBuilder().connect(
             {storeType: DataStoreType.MEMORY}) as RuntimeDatabase;
    j = db.getSchema().table('Job');
    e = db.getSchema().table('Employee');
    d = db.getSchema().table('Department');
    global = db.getGlobal();
    dataGenerator = new MockDataGenerator();
    tx = db.createTransaction();
    await addSampleData();
  });

  afterEach(() => db.close());

  // Populates the database with sample data.
  function addSampleData(): Promise<any> {
    dataGenerator.generate(
        /* jobCount */ 50,
        /* employeeCount */ 300,
        /* departmentCount */ 10);
    sampleJobs = dataGenerator.sampleJobs;
    sampleEmployees = dataGenerator.sampleEmployees;
    sampleDepartments = dataGenerator.sampleDepartments;

    const c = db.getSchema().table('Country');
    const l = db.getSchema().table('Location');
    const r = db.getSchema().table('Region');

    return db.createTransaction().exec([
      db.insert().into(r).values(dataGenerator.sampleRegions),
      db.insert().into(c).values(dataGenerator.sampleCountries),
      db.insert().into(l).values(dataGenerator.sampleLocations),
      db.insert().into(d).values(sampleDepartments),
      db.insert().into(j).values(sampleJobs),
      db.insert().into(e).values(sampleEmployees),
    ]);
  }

  function commitFn(): Promise<any> {
    return tx.commit();
  }
  function rollbackFn(): Promise<void> {
    return tx.rollback();
  }
  function attachFn(): Promise<any> {
    return tx.attach(db.select().from(j));
  }
  function beginFn(): Promise<void> {
    return tx.begin([j, e]);
  }
  function execFn(): Promise<any> {
    return tx.exec([db.select().from(e)]);
  }
  function statsFn(): TransactionStats|null {
    return tx.stats();
  }

  // Tests that an Exception.TRANSACTION is thrown when the following
  // operations are requested before the transaction has started initializing.
  //  - Attaching a query.
  //  - Attempting to commit the transaction.
  //  - Attempting to rollback the transaction.
  it('throws_StateCreated', () => {
    // 107: Invalid transaction state transition: {0} -> {1}.
    TestUtil.assertThrowsError(ErrorCode.INVALID_TX_STATE, attachFn);
    TestUtil.assertThrowsError(ErrorCode.INVALID_TX_STATE, commitFn);
    TestUtil.assertThrowsError(ErrorCode.INVALID_TX_STATE, rollbackFn);
  });

  // Tests that an Exception.TRANSACTION is thrown when any operation is
  // attempted while a query is executing.
  it('throws_StateExecutingQuery', async () => {
    await tx.begin([j, e]);
    tx.attach(db.select().from(e));

    // 107: Invalid transaction state transition: {0} -> {1}.
    TestUtil.assertThrowsError(ErrorCode.INVALID_TX_STATE, attachFn);
    TestUtil.assertThrowsError(ErrorCode.INVALID_TX_STATE, beginFn);
    TestUtil.assertThrowsError(ErrorCode.INVALID_TX_STATE, commitFn);
    TestUtil.assertThrowsError(ErrorCode.INVALID_TX_STATE, rollbackFn);
    TestUtil.assertThrowsError(ErrorCode.INVALID_TX_STATE, execFn);
  });

  // Tests that an Exception.TRANSACTION is thrown when any operation is
  // attempted after transaction has been finalized.
  it('throws_StateFinalized', async () => {
    await tx.begin([e]);
    await tx.commit();

    // 107: Invalid transaction state transition: {0} -> {1}.
    TestUtil.assertThrowsError(ErrorCode.INVALID_TX_STATE, beginFn);
    TestUtil.assertThrowsError(ErrorCode.INVALID_TX_STATE, attachFn);
    TestUtil.assertThrowsError(ErrorCode.INVALID_TX_STATE, commitFn);
    TestUtil.assertThrowsError(ErrorCode.INVALID_TX_STATE, rollbackFn);
    TestUtil.assertThrowsError(ErrorCode.INVALID_TX_STATE, execFn);
  });

  // Tests that an Exception.TRANSACTION is thrown when any operation is
  // attempted while a transaction is still initializing.
  it('throws_StateAcquiringScope', () => {
    const whenDone = tx.begin([e]);

    // 107: Invalid transaction state transition: {0} -> {1}.
    TestUtil.assertThrowsError(ErrorCode.INVALID_TX_STATE, beginFn);
    TestUtil.assertThrowsError(ErrorCode.INVALID_TX_STATE, attachFn);
    TestUtil.assertThrowsError(ErrorCode.INVALID_TX_STATE, commitFn);
    TestUtil.assertThrowsError(ErrorCode.INVALID_TX_STATE, rollbackFn);
    TestUtil.assertThrowsError(ErrorCode.INVALID_TX_STATE, execFn);
    // 105: Attempt to access in-flight transaction states.
    TestUtil.assertThrowsError(ErrorCode.INVALID_TX_ACCESS, statsFn);

    return whenDone;
  });

  // Tests that an Exception.TRANSACTION is thrown when a set of queries that
  // will be automatically committed (no need to call commit), is in progress.
  it('throws_StateExecutingAndCommitting', () => {
    const whenDone = tx.exec([db.select().from(e)]);
    // 107: Invalid transaction state transition: {0} -> {1}.
    TestUtil.assertThrowsError(ErrorCode.INVALID_TX_STATE, beginFn);
    TestUtil.assertThrowsError(ErrorCode.INVALID_TX_STATE, attachFn);
    TestUtil.assertThrowsError(ErrorCode.INVALID_TX_STATE, commitFn);
    TestUtil.assertThrowsError(ErrorCode.INVALID_TX_STATE, rollbackFn);
    TestUtil.assertThrowsError(ErrorCode.INVALID_TX_STATE, execFn);

    return whenDone;
  });

  it('exec', async () => {
    tx = db.createTransaction();
    const q1 = db.select(fn.count(j['id']).as('jid')).from(j);
    const q2 = db.select(fn.count(d['id']).as('did')).from(d);
    const q3 = db.delete().from(e);
    const q4 = db.delete().from(j);
    const results = await tx.exec([q1, q2, q3, q4, q1]);
    assert.equal(5, results.length);
    assert.equal(sampleJobs.length, results[0][0]['jid']);
    assert.equal(sampleDepartments.length, results[1][0]['did']);
    assert.equal(0, results[4][0]['jid']);

    const stats = tx.stats() as TransactionStats;
    assert.isTrue(stats.success());
    assert.equal(350, stats.deletedRowCount());
    assert.equal(0, stats.updatedRowCount());
    assert.equal(0, stats.insertedRowCount());
    assert.equal(2, stats.changedTableCount());
  });

  it('attach_Success', async () => {
    const scope = [j, e];
    await tx.begin(scope);
    const q1 = db.select().from(j);
    let results: object[] = await tx.attach(q1);
    assert.equal(sampleJobs.length, results.length);
    const q2 = db.select().from(e);
    results = await tx.attach(q2);
    assert.equal(sampleEmployees.length, results.length);

    const hireDate = dataGenerator.employeeGroundTruth.maxHireDate;
    const q3 = db.delete().from(e).where(e['hireDate'].eq(hireDate));
    await tx.attach(q3);
    const q4 = db.select().from(e);
    results = await tx.attach(q4);
    assert.isTrue(results.length < sampleEmployees.length);

    const q5 = db.delete().from(e);
    await tx.attach(q5);
    // Deleting all rows in the Job table.
    const q6 = db.delete().from(j);
    await tx.attach(q6);
    const q7 = db.select().from(j);
    results = await tx.attach(q7);
    // Expecting all rows to have been deleted within tx context.
    assert.equal(0, results.length);

    // Expecting all job rows to *not* have been deleted from disk yet, since
    // the transaction has not been committed.
    results = await TestUtil.selectAll(global, j);
    assert.equal(sampleJobs.length, results.length);

    await tx.commit();

    const stats = tx.stats() as TransactionStats;
    assert.isTrue(stats.success());
    assert.equal(2, stats.changedTableCount());
    assert.equal(350, stats.deletedRowCount());  // 50 jobs + 300 employees.
    assert.equal(0, stats.insertedRowCount());
    assert.equal(0, stats.updatedRowCount());

    // Expecting all job rows to have been deleted from disk, now that the
    // transaction was committed.
    results = await TestUtil.selectAll(global, j);
    assert.equal(0, results.length);

    // Expecting all locks to have been released by previous transaction, which
    // should allow the following query to complete.
    results = await db.select().from(e).exec();
    assert.isTrue(results.length < sampleEmployees.length);
  });

  // Tests that if an attached query fails, the entire transaction is rolled
  // back.
  it('attach_Error_Rollback', async () => {
    const scope = [j, e];
    const newJobId = 'SomeUniqueId';

    await tx.begin(scope);
    const q0 = db.select().from(j);
    let results: object[] = await tx.attach(q0);
    assert.equal(sampleJobs.length, results.length);

    // Adding a new job row.
    const newJob = j.createRow();
    newJob.payload()['id'] = newJobId;
    const q1 = db.insert().into(j).values([newJob]);
    await tx.attach(q1);
    const q2 = db.select().from(j).where(j['id'].eq(newJobId));
    results = await tx.attach(q2);
    assert.equal(1, results.length);

    const q3 = db.select().from(j);
    results = await tx.attach(q3);
    assert.equal(sampleJobs.length + 1, results.length);

    // Attempting to add an employee row that already exists.
    const q4 = db.insert().into(e).values([sampleEmployees[0]]);
    let isThrown = false;
    try {
      await tx.attach(q4);
    } catch (ex) {
      isThrown = true;

      // 201: Duplicate keys are not allowed.
      assert.equal(ErrorCode.DUPLICATE_KEYS, ex.code);

      // Checking that the transaction has been finalized.
      // 107: Invalid transaction state transition: {0} -> {1}.
      TestUtil.assertThrowsError(ErrorCode.INVALID_TX_STATE, attachFn);
      TestUtil.assertThrowsError(ErrorCode.INVALID_TX_STATE, commitFn);
      TestUtil.assertThrowsError(ErrorCode.INVALID_TX_STATE, rollbackFn);
      TestUtil.assertThrowsError(ErrorCode.INVALID_TX_STATE, beginFn);
    }

    // Ensure previous catch block is effective.
    assert.isTrue(isThrown);
    results = await TestUtil.selectAll(global, j);
    // Checking that the entire transaction was rolled back, and therefore that
    // Job row that had been added does not appear on disk.
    assert.equal(sampleJobs.length, results.length);

    // Checking that all locks have been released, which will allow other
    // transactions referring to the same scope to execute successfully.
    results = await db.select().from(j).exec();
    assert.equal(sampleJobs.length, results.length);
  });

  // Tests that when a transaction is explicitly rolled back, all changes that
  // were made as part of this transaction are discarded.
  it('rollback', async () => {
    const scope = [j, e];
    const newJobId = 'SomeUniqueId';

    await tx.begin(scope);
    // Adding a new job row.
    const newJob = j.createRow();
    newJob.payload()['id'] = newJobId;
    const q1 = db.insert().into(j).values([newJob]);
    await tx.attach(q1);
    const q2 = db.select().from(j).where(j['id'].eq(newJobId));
    let results: object[] = await tx.attach(q2);
    assert.equal(1, results.length);

    const q3 = db.select().from(j);
    results = await tx.attach(q3);
    assert.equal(sampleJobs.length + 1, results.length);

    await tx.rollback();

    // Checking that the transaction has been finalized.
    // 107: Invalid transaction state transition: {0} -> {1}.
    TestUtil.assertThrowsError(ErrorCode.INVALID_TX_STATE, attachFn);
    TestUtil.assertThrowsError(ErrorCode.INVALID_TX_STATE, commitFn);
    TestUtil.assertThrowsError(ErrorCode.INVALID_TX_STATE, rollbackFn);
    TestUtil.assertThrowsError(ErrorCode.INVALID_TX_STATE, beginFn);

    results = await TestUtil.selectAll(global, j);
    // Checking that the entire transaction was rolled back, and therefore that
    // Job row that had been added does not appear on disk.
    assert.equal(sampleJobs.length, results.length);

    // Expecting all locks to have been released by previous transaction, which
    // should allow the following query to complete.
    await db.select().from(j).exec();
    const stats = tx.stats() as TransactionStats;
    assert.isFalse(stats.success());
    assert.equal(0, stats.insertedRowCount());
    assert.equal(0, stats.updatedRowCount());
    assert.equal(0, stats.deletedRowCount());
    assert.equal(0, stats.changedTableCount());
  });

  // Tests the case where an attached query modifies the results of an observed
  // query and ensures that observers are triggered.
  it('attach_WithObservers', async () => {
    const promiseResolver = new Resolver<void>();
    const scope = [j];

    const initialJobCount = sampleJobs.length;
    const additionalJobCount = 2;

    const jobDataGenerator = new JobDataGenerator();
    const newJobs = jobDataGenerator.generate(additionalJobCount);
    newJobs.forEach((job, index) => {
      job.payload()['id'] = `SomeUniqueId${index}`;
    });

    const observeCallback: ObserverCallback = (changeEvents) => {
      assert.equal(initialJobCount + additionalJobCount, changeEvents.length);
      assert.equal(
          initialJobCount + additionalJobCount,
          changeEvents[0]['object'].length);
      promiseResolver.resolve();
    };

    const q = db.select().from(j);
    db.observe(q, observeCallback);

    try {
      await tx.begin(scope);
      // Adding a new job row.
      const q1 = db.insert().into(j).values([newJobs[0]]);
      await tx.attach(q1);
      const q2 = db.insert().into(j).values([newJobs[1]]);
      await tx.attach(q2);
      await tx.commit();
    } catch (ex) {
      promiseResolver.reject(ex);
    }
    await promiseResolver.promise;
  });

  // Tests executing an invalid query will fail the transaction.
  it('exec_FailsOnInvalidQuery', () => {
    const q = db.insert().into(j);  // an invalid query

    // 518: Invalid usage of insert().
    return TestUtil.assertPromiseReject(ErrorCode.INVALID_INSERT, tx.exec([q]));
  });

  // Tests attaching an invalid query will fail the transaction.
  it('attach_FailsOnInvalidQuery', () => {
    const q = db.insert().into(j);  // an invalid query
    const scope = [j];
    const promise = tx.begin(scope).then(() => tx.attach(q));

    // 518: Invalid usage of insert().
    return TestUtil.assertPromiseReject(518, promise);
  });
});
