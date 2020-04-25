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
import {DatabaseConnection} from '../../lib/base/database_connection';
import {DataStoreType, ErrorCode} from '../../lib/base/enum';
import {Global} from '../../lib/base/global';
import {RuntimeDatabase} from '../../lib/proc/runtime_database';
import {InsertBuilder} from '../../lib/query/insert_builder';
import {getHrDbSchemaBuilder} from '../../testing/hr_schema/hr_schema_builder';
import {HRSchemaSampleData} from '../../testing/hr_schema/hr_schema_sample_data';
import {TestUtil} from '../../testing/test_util';

const assert = chai.assert;

describe('InsertTest', () => {
  let db: DatabaseConnection;
  let global: Global;
  before(() => {
    return getHrDbSchemaBuilder()
      .connect({storeType: DataStoreType.MEMORY})
      .then(conn => {
        db = conn;
        global = (db as RuntimeDatabase).getGlobal();
      });
  });

  after(() => {
    db.close();
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

    query
      .into(getHrDbSchemaBuilder().getSchema().table('JobHistory'))
      .values([jobHistoryRow]);
    // 519: Attempted to insert or replace in a table with no primary key.
    return TestUtil.assertPromiseReject(
      ErrorCode.INVALID_INSERT_OR_REPLACE,
      query.exec()
    );
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

  it('context_Clone', () => {
    const j = db.getSchema().table('Job');
    const query = db.insert().into(j).values(bind(0)) as InsertBuilder;
    const context = query.getQuery();
    const context2 = context.clone();
    assert.deepEqual(context.into, context2.into);
    assert.isUndefined(context.values);
    assert.isUndefined(context2.values);
    assert.isTrue(context2.clonedFrom === context);
    assert.notEqual(context.getUniqueNumber(), context2.getUniqueNumber());

    const query2 = db
      .insertOrReplace()
      .into(j)
      .values(bind(0)) as InsertBuilder;
    const context3 = query2.getQuery();
    const context4 = context3.clone();
    assert.deepEqual(context3.into, context4.into);
    assert.isUndefined(context3.values);
    assert.isUndefined(context4.values);
    assert.isTrue(context4.clonedFrom === context3);
    assert.notEqual(context3.getUniqueNumber(), context4.getUniqueNumber());
  });
});
