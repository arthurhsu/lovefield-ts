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
import {DataStoreType} from '../../lib/base/enum';
import {ErrorCode} from '../../lib/base/exception';
import {Global} from '../../lib/base/global';
import {RuntimeDatabase} from '../../lib/proc/runtime_database';
import {UpdateBuilder} from '../../lib/query/update_builder';
import {getHrDbSchemaBuilder} from '../../testing/hr_schema/hr_schema_builder';
import {TestUtil} from '../../testing/test_util';

const assert = chai.assert;

describe('UpdateTest', () => {
  let db: DatabaseConnection;
  let global: Global;
  before(() => {
    getHrDbSchemaBuilder()
        .connect({storeType: DataStoreType.MEMORY})
        .then((conn) => {
          db = conn;
          global = (db as RuntimeDatabase).getGlobal();
        });
  });

  after(() => {
    db.close();
  });

  // Tests that Update#exec() fails if set() has not been called first.
  it('exec_ThrowsMissingSet', () => {
    const employeeTable = db.getSchema().table('Employee');
    const query = new UpdateBuilder(global, employeeTable);
    query.where(employeeTable['jobId'].eq('dummyJobId'));
    // 532: Invalid usage of update().
    return TestUtil.assertPromiseReject(ErrorCode.INVALID_UPDATE, query.exec());
  });

  // Tests that Update#where() fails if where() has already been called.
  it('where_ThrowsAlreadyCalled', () => {
    const employeeTable = db.getSchema().table('Employee');
    const query = new UpdateBuilder(global, employeeTable);

    const buildQuery = () => {
      const predicate = employeeTable['jobId'].eq('dummyJobId');
      query.where(predicate).where(predicate);
    };

    // 516: where() has already been called.
    TestUtil.assertThrowsError(ErrorCode.DUPLICATE_WHERE, buildQuery);
  });

  it('wet_ThrowsMissingBinding', () => {
    const employeeTable = db.getSchema().table('Employee');
    const query = new UpdateBuilder(global, employeeTable);
    query.set(employeeTable['minSalary'], bind(0));
    query.set(employeeTable['maxSalary'], 20000);
    query.where(employeeTable['jobId'].eq('dummyJobId'));
    // 501: Value is not bounded.
    return TestUtil.assertPromiseReject(ErrorCode.UNBOUND_VALUE, query.exec());
  });

  it('context_Clone', () => {
    const j = db.getSchema().table('Job');
    const query =
        db.update(j).set(j['minSalary'], bind(1)).where(j['id'].eq(bind(0)));
    const context = (query as UpdateBuilder).getQuery();
    const context2 = context.clone();
    assert.deepEqual(context.table, context2.table);
    assert.sameDeepOrderedMembers(context.set, context2.set);
    assert.deepEqual(context.where, context2.where);
    assert.notEqual(context.set[0], context2.set[0]);
    assert.equal(context2.clonedFrom, context);
    assert.notEqual(context.getUniqueId(), context2.getUniqueId());
    // TODO(arthurhsu): implement
    // assert.notEqual((context.where as Predicate).getUniqueId(),
    //    (context2.where as Predicate).getUniqueId());
  });
});
