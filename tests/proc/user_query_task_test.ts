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

import {bind} from '../../lib/base/bind';
import {ChangeRecord} from '../../lib/base/change_record';
import {DataStoreType} from '../../lib/base/enum';
import {ErrorCode} from '../../lib/base/exception';
import {Global} from '../../lib/base/global';
import {Resolver} from '../../lib/base/resolver';
import {Row} from '../../lib/base/row';
import {Service} from '../../lib/base/service';
import {Cache} from '../../lib/cache/cache';
import {RuntimeDatabase} from '../../lib/proc/runtime_database';
import {TaskItem} from '../../lib/proc/task_item';
import {UserQueryTask} from '../../lib/proc/user_query_task';
import {Table} from '../../lib/schema/table';
import {getHrDbSchemaBuilder} from '../../testing/hr_schema/hr_schema_builder';
import {HRSchemaSampleData} from '../../testing/hr_schema/hr_schema_sample_data';
import {TestUtil} from '../../testing/test_util';

const assert = chai.assert;

describe('UserQueryTask', () => {
  let db: RuntimeDatabase;
  let global: Global;
  let cache: Cache;
  let rows: Row[];
  let j: Table;
  const ROW_COUNT = 5;

  beforeEach(async () => {
    db = await getHrDbSchemaBuilder().connect(
             {storeType: DataStoreType.MEMORY}) as RuntimeDatabase;
    global = db.getGlobal();
    cache = db.getGlobal().getService(Service.CACHE);
    j = db.getSchema().table('Job');
  });

  afterEach(async () => {
    await db.delete().from(j).exec();
    db.close();
  });

  // Creates a physical plan that inserts ROW_COUNT rows into the Job table.
  function getSampleQuery(): TaskItem {
    rows = [];
    for (let i = 0; i < ROW_COUNT; ++i) {
      const job = HRSchemaSampleData.generateSampleJobData();
      job.payload()['id'] = `jobId${i}`;
      rows.push(job);
    }
    return db.insert().into(j).values(rows).getTaskItem();
  }

  // Tests that an INSERT query does indeed add a new record in the database,
  // and update the cache.
  it('singlePlan_Insert', async () => {
    assert.equal(0, cache.getCount());

    const queryTask = new UserQueryTask(global, [getSampleQuery()]);
    await queryTask.exec();
    const results = await TestUtil.selectAll(global, j);
    assert.equal(ROW_COUNT, results.length);
    assert.equal(ROW_COUNT, cache.getCount());
    for (let i = 0; i < ROW_COUNT; ++i) {
      assert.equal(rows[i].id(), results[i].id());
      assert.deepEqual(rows[i].payload(), results[i].payload());
    }
  });

  // Tests that an UPDATE query does change the record in the database and
  // cache.
  it('singlePlan_Update', async () => {
    assert.equal(0, cache.getCount());

    const newTitle = 'Quantum Physicist';
    const queryTask = new UserQueryTask(global, [getSampleQuery()]);
    await queryTask.exec();
    assert.equal(ROW_COUNT, cache.getCount());
    const query = db.update(j).set(j['title'], newTitle).getTaskItem();
    const updateQueryTask = new UserQueryTask(global, [query]);
    await updateQueryTask.exec();
    const results = await TestUtil.selectAll(global, j);
    assert.equal(ROW_COUNT, results.length);
    assert.equal(ROW_COUNT, cache.getCount());
    for (let i = 0; i < ROW_COUNT; ++i) {
      assert.equal(newTitle, results[i].payload()[j['title'].getName()]);
      const id = rows[i].id() as number;
      assert.equal(
          newTitle, (cache.get(id) as Row).payload()[j['title'].getName()]);
    }
  });

  // Tests that an DELETE query does delete the record in the database and
  // cache.
  it('singlePlan_Delete', async () => {
    assert.equal(0, cache.getCount());

    const queryTask = new UserQueryTask(global, [getSampleQuery()]);
    await queryTask.exec();
    assert.equal(ROW_COUNT, cache.getCount());
    const query = db.delete().from(j).getTaskItem();
    const deleteQueryTask = new UserQueryTask(global, [query]);
    await deleteQueryTask.exec();
    const results = await TestUtil.selectAll(global, j);
    assert.equal(0, results.length);
    assert.equal(0, cache.getCount());
  });

  // Tests that a UserQueryTask that includes multiple queries updates both
  // database and cache.
  it('multiPlan', async () => {
    assert.equal(0, cache.getCount());

    rows = [];
    for (let i = 0; i < ROW_COUNT; ++i) {
      const job = HRSchemaSampleData.generateSampleJobData();
      job.payload()['id'] = `jobId${i}`;
      rows.push(job);
    }

    const newTitle = 'Quantum Physicist';
    const deletedId = 'jobId2';

    const insertQuery = db.insert().into(j).values(rows).getTaskItem();
    const updateQuery = db.update(j).set(j['title'], newTitle).getTaskItem();
    const removeQuery =
        db.delete().from(j).where(j['id'].eq(deletedId)).getTaskItem();

    const queryTask =
        new UserQueryTask(global, [insertQuery, updateQuery, removeQuery]);

    await queryTask.exec();
    assert.equal(ROW_COUNT - 1, cache.getCount());

    const results = await TestUtil.selectAll(global, j);
    assert.equal(ROW_COUNT - 1, results.length);
    results.forEach((row) => {
      assert.equal(newTitle, row.payload()[j['title'].getName()]);
      assert.notEqual(deletedId, row.payload()[j['id'].getName()]);
    });
  });

  // Testing that when a UserQueryTask fails to execute a rejected promise is
  // returned and that indices, cache and backstore are in the state prior to
  // the task have been started.
  it('multiPlan_Rollback', async () => {
    assert.equal(0, cache.getCount());

    const job = HRSchemaSampleData.generateSampleJobData();
    // Creating two queries to be executed within the same transaction. The
    // second query will fail because of a primary key constraint violation.
    // Expecting the entire transaction to be rolled back as if it never
    // happened.
    const insertQuery = db.insert().into(j).values([job]).getTaskItem();
    const insertAgainQuery = db.insert().into(j).values([job]).getTaskItem();

    const queryTask =
        new UserQueryTask(global, [insertQuery, insertAgainQuery]);

    // 201: Duplicate keys are not allowed.
    await TestUtil.assertPromiseReject(
        ErrorCode.DUPLICATE_KEYS, queryTask.exec());
    assert.equal(0, cache.getCount());
    const results = await TestUtil.selectAll(global, j);
    assert.equal(0, results.length);
  });

  // Tests that when a parametrized query finishes execution, observers are
  // notified.
  it('singlePlan_ParametrizedQuery', () => {
    const promiseResolver = new Resolver();

    const selectQueryBuilder =
        db.select().from(j).where(j['id'].between(bind(0), bind(1)));

    const observerCallback = (changes: ChangeRecord[]): void => {
      // Expecting one "change" record for each of rows with IDs jobId2, jobId3,
      // jobId4.
      assert.equal(3, changes.length);
      changes.forEach((change) => {
        assert.equal(1, change['addedCount']);
      });

      db.unobserve(selectQueryBuilder, observerCallback);
      promiseResolver.resolve();
    };

    const insertQueryTask = new UserQueryTask(global, [getSampleQuery()]);

    insertQueryTask.exec().then(
        () => {
          // Start observing.
          db.observe(selectQueryBuilder, observerCallback);

          // Bind parameters to some values.
          selectQueryBuilder.bind(['jobId2', 'jobId4']);

          const selectQueryTask =
              new UserQueryTask(global, [selectQueryBuilder.getTaskItem()]);
          return selectQueryTask.exec();
        },
        (e) => {
          promiseResolver.reject(e);
        });
    return promiseResolver.promise;
  });
});
