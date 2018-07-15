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

import {DataStoreType, ErrorCode} from '../../lib/base/enum';
import {Exception} from '../../lib/base/exception';
import {RuntimeDatabase} from '../../lib/proc/runtime_database';
import {Table} from '../../lib/schema/table';
import {getHrDbSchemaBuilder} from '../../testing/hr_schema/hr_schema_builder';
import {TestUtil} from '../../testing/test_util';

const assert = chai.assert;

describe('SimulateError', () => {
  let sandbox: sinon.SinonSandbox;
  let db: RuntimeDatabase;
  let employee: Table;

  beforeEach(async () => {
    sandbox = sinon.createSandbox();
    db = await getHrDbSchemaBuilder().connect(
             {storeType: DataStoreType.MEMORY}) as RuntimeDatabase;
    employee = db.getSchema().table('Employee');
  });

  afterEach(() => {
    db.close();
  });

  function rejecter(): Promise<any> {
    return Promise.reject(new Exception(ErrorCode.SIMULATED_ERROR));
  }

  // Tests that when simulateErrors has been called BaseBuilder#exec() rejects.
  it('builderExec', async () => {
    const selectBuilder = db.select().from(employee);
    const selectFn = () => selectBuilder.exec();
    sandbox.stub(selectBuilder, 'exec').callsFake(rejecter);

    await TestUtil.assertPromiseReject(ErrorCode.SIMULATED_ERROR, selectFn());
    sandbox.restore();
    const results = await selectFn();
    assert.equal(0, results.length);
  });

  // Tests that when simulateErrors has been called Transaction#exec() rejects.
  it('transactionExec', async () => {
    const tx = db.createTransaction();
    sandbox.stub(tx, 'exec').callsFake(rejecter);
    const selectFn = () => {
      return tx.exec([db.select().from(employee)]);
    };

    await TestUtil.assertPromiseReject(ErrorCode.SIMULATED_ERROR, selectFn());
    sandbox.restore();
    const results = await selectFn();
    assert.equal(1, results.length);
    assert.equal(0, results[0].length);
  });

  // Tests that when simulateErrors has been called Transaction#attach()
  // rejects.
  it('transactionAttach', async () => {
    const tx = db.createTransaction();
    sandbox.stub(tx, 'attach').callsFake(rejecter);
    const selectFn = () => tx.attach(db.select().from(employee));

    await tx.begin([employee]);
    await TestUtil.assertPromiseReject(ErrorCode.SIMULATED_ERROR, selectFn());
    sandbox.restore();
    const results = await selectFn();
    assert.equal(0, results.length);
  });
});
