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
import {DeleteBuilder} from '../../lib/query/delete_builder';
import {getHrDbSchemaBuilder} from '../../testing/hr_schema/hr_schema_builder';
import {TestUtil} from '../../testing/test_util';

const assert = chai.assert;

describe('DeleteTest', () => {
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

  // Tests that Delete#exec() fails if from() has not been called first.
  it('exec_ThrowsMissingFrom', () => {
    const query = new DeleteBuilder(global);
    // 517: Invalid usage of delete().
    return TestUtil.assertPromiseReject(ErrorCode.INVALID_DELETE, query.exec());
  });

  // Tests that Delete#from() fails if from() has already been called.
  it('from_ThrowsAlreadyCalled', () => {
    const query = new DeleteBuilder(global);

    const buildQuery = () => {
      const e = db.getSchema().table('Employee');
      query.from(e).from(e);
    };

    // 515: from() has already been called.
    TestUtil.assertThrowsError(ErrorCode.DUPLICATE_FROM, buildQuery);
  });

  // Tests that Delete#where() fails if where() has already been called.
  it('where_ThrowsAlreadyCalled', () => {
    const query = new DeleteBuilder(global);

    const buildQuery = () => {
      const e = db.getSchema().table('Employee');
      const predicate = e.col('jobId').eq('dummyJobId');
      query.from(e).where(predicate).where(predicate);
    };

    // 516: where() has already been called.
    TestUtil.assertThrowsError(ErrorCode.DUPLICATE_WHERE, buildQuery);
  });

  it('where_ThrowsCalledBeforeFrom', () => {
    const query = new DeleteBuilder(global);

    const buildQuery = () => {
      const e = db.getSchema().table('Employee');
      const predicate = e.col('jobId').eq('dummyJobId');
      query.where(predicate).from(e);
    };

    // 548: from() has to be called before where().
    TestUtil.assertThrowsError(548, buildQuery);
  });

  it('context_Clone', () => {
    const j = db.getSchema().table('Job');
    const query = db
      .delete()
      .from(j)
      .where(j.col('id').eq(bind(0))) as DeleteBuilder;
    const context = query.getQuery();
    const context2 = context.clone();
    assert.deepEqual(context.where, context2.where);
    assert.deepEqual(context.from, context2.from);
    assert.equal(context2.clonedFrom, context);
    assert.notEqual(context.getUniqueNumber(), context2.getUniqueNumber());
  });
});
