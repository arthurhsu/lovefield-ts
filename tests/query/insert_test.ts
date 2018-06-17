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

// import * as chai from 'chai';
import {bind} from '../../lib/base/bind';
import {ErrorCode} from '../../lib/base/exception';
import {Global} from '../../lib/base/global';
import {Service} from '../../lib/base/service';
import {QueryEngine} from '../../lib/proc/query_engine';
import {Runner} from '../../lib/proc/runner';
import {InsertBuilder} from '../../lib/query/insert_builder';
import {getHrDbSchemaBuilder} from '../../testing/hr_schema/hr_schema_builder';
import {HRSchemaSampleData} from '../../testing/hr_schema/hr_schema_sample_data';
import {TestUtil} from '../../testing/test_util';

// const assert = chai.assert;

describe('InsertTest', () => {
  // TODO(arthurhsu): currently uses hacked way to create environment, should
  // use an in-memory database once all pieces are in-place.

  // let db: RuntimeDatabase;
  let global: Global;
  before(() => {
    // hr.db.connect({storeType: DatabaseType.MEMORY}).then(dbi => db = dbi);
    global = new Global();
    global.registerService(Service.SCHEMA, getHrDbSchemaBuilder().getSchema());
    global.registerService(Service.RUNNER, new Runner());
    global.registerService(Service.QUERY_ENGINE, {} as any as QueryEngine);
  });

  // Tests that Insert#exec() fails if into() has not been called first.
  it('exec_ThrowsMissingInto', () => {
    const query = new InsertBuilder(global);
    const job = HRSchemaSampleData.generateSampleJobData();
    query.values([job]);
    // 518: Invalid usage of insert().
    return TestUtil.assertPromiseReject(ErrorCode.INVALID_INSERT, query.exec());
  });

  // Tests that Insert#exec() fails if values() has not been called first.
  it('exec_ThrowsMissingValues', () => {
    const query = new InsertBuilder(global);
    query.into(getHrDbSchemaBuilder().getSchema().table('Job'));
    // 518: Invalid usage of insert().
    return TestUtil.assertPromiseReject(ErrorCode.INVALID_INSERT, query.exec());
  });

  // Tests that Insert#exec() fails if allowReplace is true, for a table that
  // has no primary key.
  it('exec_ThrowsNoPrimaryKey', () => {
    const jobHistoryRow = HRSchemaSampleData.generateSampleJobHistoryData();
    const query = new InsertBuilder(global, /* allowReplace */ true);

    query.into(getHrDbSchemaBuilder().getSchema().table('JobHistory')).values([
      jobHistoryRow,
    ]);
    // 519: Attempted to insert or replace in a table with no primary key.
    return TestUtil.assertPromiseReject(
        ErrorCode.INVALID_INSERT_OR_REPLACE, query.exec());
  });

  // Tests that Insert#values() fails if values() has already been called.
  it('values_ThrowsAlreadyCalled', () => {
    const query = new InsertBuilder(global);

    const job = HRSchemaSampleData.generateSampleJobData();
    const buildQuery = () => query.values([job]).values([job]);

    // 521: values() has already been called.
    TestUtil.assertThrowsError(ErrorCode.DUPLICATE_VALUES, buildQuery);
  });

  // Tests that Insert#into() fails if into() has already been called.
  it('into_ThrowsAlreadyCalled', () => {
    const query = new InsertBuilder(global);

    const buildQuery = () => {
      const jobTable = getHrDbSchemaBuilder().getSchema().table('Job');
      query.into(jobTable).into(jobTable);
    };

    // 520: into() has already been called.
    TestUtil.assertThrowsError(ErrorCode.DUPLICATE_INTO, buildQuery);
  });

  it('values_ThrowMissingBinding', () => {
    const query = new InsertBuilder(global);
    const jobTable = getHrDbSchemaBuilder().getSchema().table('Job');
    query.into(jobTable).values(bind(0));
    // 518: Invalid usage of insert().
    return TestUtil.assertPromiseReject(ErrorCode.INVALID_INSERT, query.exec());
  });

  // TODO(arthurhsu): enable this test when memory DB is ready.
  /*
  it('context_Clone', () => {
    const j = db.getSchema().table('Job');
    const query = db.insert().into(j).values(bind(0)) as InsertBuilder;
    const context = query.getQuery();
    const context2 = context.clone();
    assert.deepEqual(context.into, context2.into);
    assert.sameDeepOrderedMembers(context.values, context2.values);
    assert.isTrue(context2.clonedFrom === context);
    assert.notEqual(context.getUniqueNumber(), context2.getUniqueNumber());

    const query2 =
        db.insertOrReplace().into(j).values(bind(0)) as InsertBuilder;
    const context3 = query2.getQuery();
    const context4 = context3.clone();
    assert.deepEqual(context3.into, context4.into);
    assert.sameDeepOrderedMembers(context3.values, context4.values);
    assert.isTrue(context4.clonedFrom === context3);
    assert.notEqual(context3.getUniqueNumber(), context4.getUniqueNumber());
  });
  */
});
