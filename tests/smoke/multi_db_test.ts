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

import {Capability} from '../../lib/base/capability';
import {DatabaseConnection} from '../../lib/base/database_connection';
import {DataStoreType, Type} from '../../lib/base/enum';
import {Row} from '../../lib/base/row';
import {schema} from '../../lib/schema/schema';
import {getHrDbSchemaBuilder} from '../../testing/hr_schema/hr_schema_builder';
import {SmokeTester} from '../../testing/smoke_tester';

const assert = chai.assert;

describe('MultiDB', () => {
  let capability: Capability;
  let hrTester: SmokeTester;
  let orderTester: SmokeTester;
  let dbHr: DatabaseConnection;
  let dbOrder: DatabaseConnection;

  before(() => {
    capability = Capability.get();
  });

  beforeEach(async () => {
    // Setup two databases and connect.
    const hr = getHrDbSchemaBuilder();
    const order = schema.create(`order${Date.now}`, 1);
    order
      .createTable('Region')
      .addColumn('id', Type.STRING)
      .addColumn('name', Type.STRING)
      .addPrimaryKey(['id']);
    const options = {
      storeType: !capability.indexedDb
        ? DataStoreType.MEMORY
        : DataStoreType.INDEXED_DB,
    };
    dbHr = await hr.connect(options);
    dbOrder = await order.connect(options);
    hrTester = new SmokeTester(hr.getGlobal(), dbHr);
    orderTester = new SmokeTester(order.getGlobal(), dbOrder);
  });

  afterEach(async () => {
    await hrTester.clearDb();
    await orderTester.clearDb();
    dbHr.close();
    dbOrder.close();
  });

  it('CRUD', () => {
    // Running both tests in parallel on purpose, since this simulates closer a
    // real-world scenario.
    return Promise.all([hrTester.testCRUD(), orderTester.testCRUD()]);
  });

  // Tests that connecting to a 2nd database does not cause Row.nextId to be
  // overwritten with a smaller value (which guarantees that row IDs will remain
  // unique).
  it('RowIdsUnique', async () => {
    if (!capability.indexedDb) {
      return Promise.resolve();
    }

    Row.setNextId(0);

    const schemaBuilder1 = schema.create('db1', 1);
    schemaBuilder1.createTable('TableA').addColumn('name', Type.STRING);

    const schemaBuilder2 = schema.create('db2', 1);
    schemaBuilder2.createTable('TableB').addColumn('name', Type.STRING);

    const createNewTableARows = () => {
      const rows: Row[] = [];
      const tableA = schemaBuilder1.getSchema().table('TableA');
      for (let i = 0; i < 3; i++) {
        rows.push(tableA.createRow({name: `name_${i}`}));
      }
      return rows;
    };

    const options = {storeType: DataStoreType.INDEXED_DB};
    await schemaBuilder1.connect(options);
    let sampleRows = createNewTableARows();
    assert.sameOrderedMembers(
      [1, 2, 3],
      sampleRows.map(r => r.id())
    );
    await schemaBuilder2.connect(options);
    sampleRows = createNewTableARows();
    assert.sameOrderedMembers(
      [4, 5, 6],
      sampleRows.map(r => r.id())
    );
  });
});
