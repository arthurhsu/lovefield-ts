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
import {Capability} from '../../lib/base/capability';
import {ChangeRecord} from '../../lib/base/change_record';
import {DataStoreType, ErrorCode, Order} from '../../lib/base/enum';
import {Resolver} from '../../lib/base/resolver';
import {Row} from '../../lib/base/row';
import {RuntimeDatabase} from '../../lib/proc/runtime_database';
import {InsertQuery} from '../../lib/query/insert_query';
import {BaseTable} from '../../lib/schema/base_table';
import {Builder} from '../../lib/schema/builder';
import {getHrDbSchemaBuilder} from '../../testing/hr_schema/hr_schema_builder';
import {JobDataGenerator} from '../../testing/hr_schema/job_data_generator';
import {MockDataGenerator} from '../../testing/hr_schema/mock_data_generator';
import {TestUtil} from '../../testing/test_util';

const assert = chai.assert;

type Connector = () => RuntimeDatabase;

describe('EndToEndTest', () => {
  let connector: (builder: Builder) => Connector;
  let db: RuntimeDatabase;
  let j: BaseTable;
  let e: BaseTable;
  let dataGenerator: MockDataGenerator;
  let sampleJobs: Row[];

  before(() => {
    dataGenerator = new MockDataGenerator();
    dataGenerator.generate(
        /* jobCount */ 50,
        /* employeeCount */ 50,
        /* departmentCount */ 1);
    sampleJobs = dataGenerator.sampleJobs;
  });

  beforeEach(async () => {
    db = await connector(getHrDbSchemaBuilder())();
    j = db.getSchema().table('Job');
    e = db.getSchema().table('Employee');
  });

  afterEach(() => db.close());

  // Populates the database with sample data.
  function addSampleData(): Promise<any> {
    const d = db.getSchema().table('Department');
    const l = db.getSchema().table('Location');
    const c = db.getSchema().table('Country');
    const r = db.getSchema().table('Region');

    const tx = db.createTransaction();
    return tx.exec([
      db.insert().into(r).values(dataGenerator.sampleRegions),
      db.insert().into(c).values(dataGenerator.sampleCountries),
      db.insert().into(l).values(dataGenerator.sampleLocations),
      db.insert().into(d).values(dataGenerator.sampleDepartments),
      db.insert().into(j).values(sampleJobs),
    ]);
  }

  async function checkAutoIncrement(builderFn: () => InsertQuery):
      Promise<void> {
    const r = db.getSchema().table('Region');
    const regionRow = r.createRow({id: 'regionId', name: 'dummyRegionName'});

    const c = db.getSchema().table('Country');

    const firstBatch: Row[] = new Array(3);
    for (let i = 0; i < firstBatch.length; i++) {
      firstBatch[i] = c.createRow();
      firstBatch[i].payload()['regionId'] = 'regionId';
      // Default value of the primary key column is set to 0 within createRow
      // (since only integer keys are allowed to be marked as
      // auto-incrementing), which will trigger an automatically assigned
      // primary key.
    }

    const secondBatch: Row[] = new Array(4);
    for (let i = 0; i < secondBatch.length; i++) {
      secondBatch[i] = c.createRow({
        name: `holiday${i}`,
        regionId: 'regionId',
      });
      // 'id' is not specified in the 2nd batch, which should also trigger
      // automatically assigned primary keys.
    }

    const thirdBatch: Row[] = new Array(5);
    for (let i = 0; i < thirdBatch.length; i++) {
      thirdBatch[i] = c.createRow({
        id: null,
        name: `holiday${i}`,
        regionId: 'regionId',
      });
      // 'id' is set to null in the 3rd batch, which should also trigger
      // automatically assigned primary keys.
    }

    // Adding a row with a manually assigned primary key. This ID should not be
    // replaced by an automatically assigned ID.
    const manuallyAssignedId =
        firstBatch.length + secondBatch.length + thirdBatch.length + 1000;
    const manualRow = c.createRow();
    manualRow.payload()['id'] = manuallyAssignedId;
    manualRow.payload()['regionId'] = 'regionId';
    const global = db.getGlobal();

    await db.insert().into(r).values([regionRow]).exec();
    let results: Row[] = await builderFn().into(c).values(firstBatch).exec();
    assert.equal(firstBatch.length, results.length);
    results = await builderFn().into(c).values(secondBatch).exec();
    assert.equal(secondBatch.length, results.length);
    results = await builderFn().into(c).values(thirdBatch).exec();
    assert.equal(thirdBatch.length, results.length);
    results = await builderFn().into(c).values([manualRow]).exec();
    assert.equal(1, results.length);
    results = await TestUtil.selectAll(global, c);
    // Sorting by primary key.
    results.sort((leftRow, rightRow) => {
      return leftRow.payload()['id'] - rightRow.payload()['id'];
    });

    // Checking that all primary keys starting from 1 were automatically
    // assigned.
    results.forEach((row, index) => {
      if (index < results.length - 1) {
        assert.equal(index + 1, row.payload()['id']);
      } else {
        assert.equal(manuallyAssignedId, row.payload()['id']);
      }
    });

    // Testing that previously assigned primary keys that have now been
    // freed are not re-used.
    // Removing the row with the max primary key encountered so far.
    await db.delete().from(c).where(c['id'].eq(manuallyAssignedId)).exec();

    // Adding a new row. Expecting that the automatically assigned primary
    // key will be greater than the max primary key ever encountered in this
    // table (even if that key has now been freed).
    const manualRow2 = c.createRow();
    manualRow2.payload()['id'] = null;
    manualRow2.payload()['regionId'] = 'regionId';
    results = await builderFn().into(c).values([manualRow2]).exec();
    assert.equal(1, results.length);
    assert.equal(manuallyAssignedId + 1, results[0][c['id'].getName()]);
  }

  function getDynamicSchemaConnector(builder: Builder): Connector {
    return builder.connect.bind(builder, {storeType: DataStoreType.MEMORY});
  }

  function getDynamicSchemaBundledConnector(builder: Builder): Connector {
    builder.setPragma({enableBundledMode: true});
    return builder.connect.bind(builder, undefined);
  }

  const scenarios = new Map<string, (builder: Builder) => Connector>();
  scenarios.set('dynamic', getDynamicSchemaConnector);
  if (Capability.get().indexedDb) {
    scenarios.set('dynamic_bundled', getDynamicSchemaBundledConnector);
  }

  scenarios.forEach((conn, key) => {
    connector = conn;

    // Tests that an INSERT query does indeed add a new record in the database.
    it(`${key}_Insert`, async () => {
      const row = j.createRow();
      row.payload()['id'] = 'dummyJobId';

      const queryBuilder = db.insert().into(j).values([row]);
      let results: Row[] = await queryBuilder.exec();
      assert.equal(1, results.length);
      assert.equal(row.payload()['id'], results[0]['id']);

      results = await TestUtil.selectAll(db.getGlobal(), j);
      assert.equal(1, results.length);
    });

    // Tests that insertion succeeds for tables where no primary key is
    // specified.
    it(`${key}_Insert_NoPrimaryKey`, async () => {
      await addSampleData();

      const jobHistory = db.getSchema().table('JobHistory');
      assert.isNull(jobHistory.getConstraint().getPrimaryKey());
      const row = jobHistory.createRow();
      row.payload()['employeeId'] =
          dataGenerator.sampleEmployees[0].payload()['id'];
      row.payload()['departmentId'] =
          dataGenerator.sampleDepartments[0].payload()['id'];

      const queryBuilder = db.insert().into(jobHistory).values([row]);
      const tx = db.createTransaction();
      // Adding necessary rows to avoid triggering foreign key constraints.
      await tx.exec([
        db.insert().into(e).values(dataGenerator.sampleEmployees.slice(0, 1)),
      ]);
      let results: Row[] = await queryBuilder.exec();
      assert.equal(1, results.length);
      results = await TestUtil.selectAll(db.getGlobal(), jobHistory);
      assert.equal(1, results.length);
    });

    it(`${key}_Insert_CrossColumnPrimaryKey`, async () => {
      const table = db.getSchema().table('DummyTable');

      const q1 = db.insert().into(table).values([table.createRow()]);
      const q2 = db.insert().into(table).values([table.createRow()]);

      const results: Row[] = await q1.exec();
      assert.equal(1, results.length);
      let failed = true;
      try {
        await q2.exec();
      } catch (ex) {
        // 201: Duplicate keys are not allowed.
        assert.equal(ErrorCode.DUPLICATE_KEYS, ex.code);
        failed = false;
      }

      assert.isFalse(failed);
    });

    it(`${key}_Insert_CrossColumnUniqueKey`, async () => {
      const table = db.getSchema().table('DummyTable');

      // Creating two rows where 'uq_constraint' is violated.
      const row1 = table.createRow({
        boolean: false,
        integer: 100,
        number: 1,
        string: 'string_1',
        string2: 'string2',
      });
      const row2 = table.createRow({
        boolean: false,
        integer: 100,
        number: 2,
        string: 'string_2',
        string2: 'string2',
      });

      const q1 = db.insert().into(table).values([row1]);
      const q2 = db.insert().into(table).values([row2]);

      const results: Row[] = await q1.exec();
      assert.equal(1, results.length);
      let failed = true;
      try {
        await q2.exec();
      } catch (ex) {
        assert.equal(ErrorCode.DUPLICATE_KEYS, ex.code);
        failed = false;
      }
      assert.isFalse(failed);
    });

    it(`${key}_Insert_CrossColumnNullableIndex`, async () => {
      const table = db.getSchema().table('CrossColumnTable');

      // Creating two rows where 'uq_constraint' is violated.
      const row1 = table.createRow({
        integer1: 1,
        integer2: 2,
        string1: 'A',
        string2: 'Z',
      });
      const row2 = table.createRow({
        integer1: 2,
        integer2: 3,
        string1: 'B',
        string2: null,
      });
      const row3 = table.createRow({
        integer1: 3,
        integer2: 4,
        string1: null,
        string2: 'Y',
      });
      const row4 = table.createRow({
        integer1: 5,
        integer2: 6,
        string1: null,
        string2: null,
      });

      const rows = [row1, row2, row3, row4];
      let results: Row[] = await db.insert().into(table).values(rows).exec();
      assert.equal(4, results.length);
      results = await db.select().from(table).orderBy(table['integer1']).exec();
      const expected = rows.map((row) => row.payload());
      assert.sameDeepOrderedMembers(expected, results);
    });

    // Tests that an INSERT query on a table that uses 'autoIncrement' primary
    // key does indeed automatically assign incrementing primary keys to rows
    // being inserted.
    it(`${key}_Insert_AutoIncrement`, async () => {
      await checkAutoIncrement(db.insert.bind(db));
    });

    // Tests that an INSERT query will fail if the row that is inserted is
    // referring to non existing foreign keys.
    it(`${key}_Insert_FkViolation`, async () => {
      await TestUtil.assertPromiseReject(
          ErrorCode.FK_VIOLATION,
          db.insert()
              .into(e)
              .values(dataGenerator.sampleEmployees.slice(0, 1))
              .exec());
    });

    // Tests that an INSERT OR REPLACE query on a table that uses
    // 'autoIncrement' primary key does indeed automatically assign incrementing
    // primary keys to rows being inserted.
    it(`${key}_InsertOrReplace_AutoIncrement`, async () => {
      await checkAutoIncrement(db.insertOrReplace.bind(db));
    });

    // Tests that an INSERT_OR_REPLACE query will fail if the row that is
    // inserted is referring to non existing foreign keys.
    it(`${key}_InsertOrReplace_FkViolation1`, async () => {
      await TestUtil.assertPromiseReject(
          ErrorCode.FK_VIOLATION,
          db.insertOrReplace()
              .into(e)
              .values(dataGenerator.sampleEmployees.slice(0, 1))
              .exec());
    });

    // Tests that an INSERT_OR_REPLACE query will fail if the row that is
    // replaced is referring to non existing foreign keys as a result of the
    // update.
    it(`${key}_InsertOrReplace_FkViolation2`, async () => {
      await addSampleData();
      const d = db.getSchema().table('Department');

      const rowBefore = dataGenerator.sampleDepartments[0];
      const rowAfter = d.createRow({
        id: rowBefore.payload()['id'],
        // Updating locationId to point to a non-existing foreign key.
        locationId: 'otherLocationId',
        managerId: rowBefore.payload()['managerId'],
        name: rowBefore.payload()['name'],
      });

      return TestUtil.assertPromiseReject(
          ErrorCode.FK_VIOLATION,
          db.insertOrReplace().into(d).values([rowAfter]).exec());
    });

    // Tests INSERT OR REPLACE query accepts value binding.
    it(`${key}_InsertOrReplace_Bind`, async () => {
      const region = db.getSchema().table('Region');
      const rows = [
        region.createRow({id: 'd1', name: 'dummyName'}),
        region.createRow({id: 'd2', name: 'dummyName'}),
      ];

      const queryBuilder =
          db.insertOrReplace().into(region).values([bind(0), bind(1)]);

      await queryBuilder.bind(rows).exec();
      const results = await TestUtil.selectAll(db.getGlobal(), region);
      assert.equal(2, results.length);
    });

    // Tests INSERT OR REPLACE query accepts value binding.
    it(`${key}_InsertOrReplace_BindArray`, async () => {
      const region = db.getSchema().table('Region');
      const rows = [
        region.createRow({id: 'd1', name: 'dummyName'}),
        region.createRow({id: 'd2', name: 'dummyName'}),
      ];

      const queryBuilder = db.insertOrReplace().into(region).values(bind(0));

      await queryBuilder.bind([rows]).exec();
      const results = await TestUtil.selectAll(db.getGlobal(), region);
      assert.equal(2, results.length);
    });

    // Tests that an UPDATE query does indeed update records in the database.
    it(`${key}_Update_All`, async () => {
      await addSampleData();
      const minSalary = 0;
      const maxSalary = 1000;
      const queryBuilder = db.update(j)
                               .set(j['minSalary'], minSalary)
                               .set(j['maxSalary'], maxSalary);

      const minSalaryName = j['minSalary'].getName();
      const maxSalaryName = j['maxSalary'].getName();

      await queryBuilder.exec();
      const results = await TestUtil.selectAll(db.getGlobal(), j);
      results.forEach((row) => {
        assert.equal(minSalary, row.payload()[minSalaryName]);
        assert.equal(maxSalary, row.payload()[maxSalaryName]);
      });
    });

    // Tests that an UPDATE query will fail if a parent column is updated while
    // other rows are pointing to the updated parent row.
    it(`${key}_Update_FkViolation1`, async () => {
      await addSampleData();

      const l = db.getSchema().table('Location');
      const referringRow = dataGenerator.sampleDepartments[0];

      await TestUtil.assertPromiseReject(
          ErrorCode.FK_VIOLATION,
          db.update(l)
              .set(l['id'], 'otherLocationId')
              .where(l['id'].eq(referringRow.payload()['locationId']))
              .exec());
    });

    // Tests that an UPDATE query will fail if a child column is updated such
    // that the updated row will now point to a non-existing foreign key.
    it(`${key}_Update_FkViolation2`, async () => {
      await addSampleData();

      const d = db.getSchema().table('Department');
      const referredRow = dataGenerator.sampleLocations[0];
      await TestUtil.assertPromiseReject(
          ErrorCode.FK_VIOLATION,
          db.update(d)
              .set(d['locationId'], 'otherLocationId')
              .where(d['locationId'].eq(referredRow.payload()['id']))
              .exec());
    });

    // Tests that an UPDATE query with a predicate does updates the
    // corresponding records in the database.
    it(`${key}_Update_Predicate`, async () => {
      await addSampleData();

      const jobId = sampleJobs[0].payload()['id'];

      const queryBuilder = db.update(j)
                               .where(j['id'].eq(jobId))
                               .set(j['minSalary'], 10000)
                               .set(j['maxSalary'], 20000);

      await queryBuilder.exec();
      const results = await TestUtil.selectAll(db.getGlobal(), j);
      let verified = false;
      results.some((row) => {
        if (row.payload()['id'] === jobId) {
          assert.equal(10000, row.payload()['minSalary']);
          assert.equal(20000, row.payload()['maxSalary']);
          verified = true;
          return true;
        }
        return false;
      });
      assert.isTrue(verified);
    });

    // Tests a template UPDATE query where multiple queries are executed in
    // parallel. It ensures that each query respects the values it was bounded
    // too.
    it(`${key}_Update_BindMultiple`, async () => {
      await addSampleData();

      const jobId1 = sampleJobs[0].payload()['id'];
      const jobId2 = sampleJobs[1].payload()['id'];
      const minSalary1After = 0;
      const maxSalary1After = 105;
      const minSalary2After = 100;
      const maxSalary2After = 305;
      const bindValues = [
        [minSalary1After, maxSalary1After, jobId1],
        [minSalary2After, maxSalary2After, jobId2],
      ];
      const query = db.update(j)
                        .set(j['minSalary'], bind(0))
                        .set(j['maxSalary'], bind(1))
                        .where(j['id'].eq(bind(2)));

      const promises = bindValues.map((values) => query.bind(values).exec());
      await Promise.all(promises);
      const results = await db.select()
                          .from(j)
                          .where(j['id'].in([jobId1, jobId2]))
                          .orderBy(j['id'], Order.ASC)
                          .exec();

      assert.equal(minSalary1After, results[0].minSalary);
      assert.equal(maxSalary1After, results[0].maxSalary);
      assert.equal(minSalary2After, results[1].minSalary);
      assert.equal(maxSalary2After, results[1].maxSalary);
    });

    it(`${key}_Update_UnboundPredicate`, async () => {
      await addSampleData();

      const queryBuilder = db.update(j)
                               .set(j['minSalary'], bind(1))
                               .set(j['maxSalary'], 20000)
                               .where(j['id'].eq(bind(0)));

      const jobId = sampleJobs[0].payload()['id'];
      const minSalaryName = j['minSalary'].getName();
      const maxSalaryName = j['maxSalary'].getName();

      await queryBuilder.bind([jobId, 10000]).exec();

      // TODO(arthurhsu): first result was not used in original code, bug?
      let results = await TestUtil.selectAll(db.getGlobal(), j);
      results = await db.select().from(j).where(j['id'].eq(jobId)).exec();
      assert.equal(10000, results[0][minSalaryName]);
      assert.equal(20000, results[0][maxSalaryName]);
      await queryBuilder.bind([jobId, 15000]).exec();
      results = await db.select().from(j).where(j['id'].eq(jobId)).exec();
      assert.equal(15000, results[0][minSalaryName]);
      assert.equal(20000, results[0][maxSalaryName]);
    });

    // Tests that a DELETE query will fail if the row that is deleted is being
    // referred by other columns.
    it(`${key}_Delete_FkViolation`, async () => {
      await addSampleData();

      const l = db.getSchema().table('Location');
      const referringRow = dataGenerator.sampleDepartments[0];
      await TestUtil.assertPromiseReject(
          ErrorCode.FK_VIOLATION,
          db.delete()
              .from(l)
              .where(l['id'].eq(referringRow.payload()['locationId']))
              .exec());
    });

    // Tests that a DELETE query with a specified predicate deletes only the
    // records that satisfy the predicate.
    it(`${key}_Delete_Predicate`, async () => {
      await addSampleData();

      const jobId = 'jobId' + Math.floor(sampleJobs.length / 2).toString();
      const queryBuilder = db.delete().from(j).where(j['id'].eq(jobId));

      await queryBuilder.exec();
      const results = await TestUtil.selectAll(db.getGlobal(), j);
      assert.equal(sampleJobs.length - 1, results.length);
    });

    it(`${key}_Delete_UnboundPredicate`, async () => {
      await addSampleData();

      const jobId = 'jobId' + Math.floor(sampleJobs.length / 2).toString();
      const queryBuilder = db.delete().from(j).where(j['id'].eq(bind(1)));

      await queryBuilder.bind(['', jobId]).exec();
      const results = await TestUtil.selectAll(db.getGlobal(), j);
      assert.equal(sampleJobs.length - 1, results.length);
    });

    it(`${key}_Delete_UnboundPredicateReject`, async () => {
      await addSampleData();

      const queryBuilder = db.delete().from(j).where(j['id'].eq(bind(1)));

      let failed = true;
      try {
        await queryBuilder.exec();
      } catch (ex) {
        // 501: Value is not bounded.
        assert.equal(ErrorCode.UNBOUND_VALUE, ex.code);
        failed = false;
      }
      assert.isFalse(failed);
    });

    // Tests that a DELETE query without a specified predicate deletes the
    // entire table.
    it(`${key}_Delete_All`, async () => {
      await addSampleData();

      await db.delete().from(j).exec();
      const results = await TestUtil.selectAll(db.getGlobal(), j);
      assert.equal(0, results.length);
    });

    // Tests the case where multiple observers are registered for the same query
    // semantically (but not the same query object instance). Each observer
    // should receive different "change" records, depending on the time it was
    // registered.
    it(`${key}_MultipleObservers`, async () => {
      const jobGenerator = new JobDataGenerator();

      // |id| is a suffix to apply to the ID (to avoid triggering constraint
      // violations).
      const createNewRow = (id: number) => {
        const sampleJob = jobGenerator.generate(1)[0];
        sampleJob.payload()['id'] = `someJobId${id}`;
        return sampleJob;
      };
      const getQuery = () => db.select().from(j);

      const callback1Params: number[] = [];
      const callback2Params: number[] = [];
      const callback3Params: number[] = [];

      const resolver = new Resolver();
      const doAssertions = () => {
        try {
          // Expecting callback1 to have been called 3 times.
          assert.sameDeepOrderedMembers([1, 1, 1], callback1Params);
          // Expecting callback2 to have been called 2 times.
          assert.sameDeepOrderedMembers([2, 1], callback2Params);
          // Expecting callback3 to have been called 1 time.
          assert.sameDeepOrderedMembers([3], callback3Params);
        } catch (e) {
          resolver.reject(e);
        }
        resolver.resolve();
      };

      const callback1 = (changes: ChangeRecord[]) =>
          callback1Params.push(changes.length);
      const callback2 = (changes: ChangeRecord[]) =>
          callback2Params.push(changes.length);
      const callback3 = (changes: ChangeRecord[]) => {
        callback3Params.push(changes.length);
        doAssertions();
      };

      db.observe(getQuery(), callback1);
      await db.insert().into(j).values([createNewRow(1)]).exec();
      db.observe(getQuery(), callback2);
      await db.insert().into(j).values([createNewRow(2)]).exec();
      db.observe(getQuery(), callback3);
      await db.insert().into(j).values([createNewRow(3)]).exec();

      await resolver.promise;
    });
  });
});
