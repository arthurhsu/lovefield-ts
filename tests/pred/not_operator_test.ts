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
import {DataStoreType} from '../../lib/base/enum';
import {PayloadType, Row} from '../../lib/base/row';
import {op} from '../../lib/fn/op';
import {RuntimeDatabase} from '../../lib/proc/runtime_database';
import {getHrDbSchemaBuilder} from '../../testing/hr_schema/hr_schema_builder';

const assert = chai.assert;

describe('NotOperator', () => {
  let db: RuntimeDatabase;
  const rowCount = 8;

  beforeEach(async () => {
    db = (await getHrDbSchemaBuilder().connect({
      storeType: DataStoreType.MEMORY,
    })) as RuntimeDatabase;
    // Delete any left-overs from previous tests.
    await clearDb();
    await populateDatabase();
  });

  afterEach(() => db.close());

  // Deletes the contents of all tables.
  async function clearDb(): Promise<void> {
    const tables = db.getSchema().tables();
    const deletePromises = tables.map(table => db.delete().from(table).exec());

    return Promise.all(deletePromises).then(() => {
      return;
    });
  }

  function generateSampleData(rc: number): Row[] {
    const rows = new Array(rc);
    const tableSchema = db.getSchema().table('DummyTable');
    for (let i = 0; i < rc; i++) {
      rows[i] = tableSchema.createRow({
        boolean: false,
        integer: 100 * i,
        number: 100 * i,
        string: `string${i}`,
        string2: `string2${i}`,
      });
    }
    return rows;
  }

  // Inserts sample records in the database.
  async function populateDatabase(): Promise<unknown> {
    const dummy = db.getSchema().table('DummyTable');
    return db.insert().into(dummy).values(generateSampleData(rowCount)).exec();
  }

  it('not_In', async () => {
    const tableSchema = db.getSchema().table('DummyTable');
    const excludeIds = ['string3', 'string5', 'string1'];
    const expectedIds = ['string0', 'string2', 'string4', 'string6', 'string7'];

    // Select records from the database.
    const selectFn = () => {
      return db
        .select()
        .from(tableSchema)
        .where(op.not(tableSchema.col('string').in(excludeIds)))
        .exec() as Promise<PayloadType[]>;
    };

    const results = await selectFn();
    const actualIds = results.map(result => result['string']);
    assert.sameMembers(expectedIds, actualIds);
  });

  // Tests the case where not() is used on an indexed property. This exercises
  // the code path where an IndexRangeScanStep is used during query execution as
  // opposed to a SelectStep.
  it('not_Eq', async () => {
    const tableSchema = db.getSchema().table('DummyTable');
    const excludedId = 'string1';

    // Select records from the database.
    const selectFn = () => {
      return db
        .select()
        .from(tableSchema)
        .where(op.not(tableSchema.col('string').eq(excludedId)))
        .exec() as Promise<PayloadType[]>;
    };

    const results = await selectFn();
    assert.equal(rowCount - 1, results.length);
    assert.isFalse(results.some(result => result['string'] === excludedId));
  });

  it('and_Not', async () => {
    const tableSchema = db.getSchema().table('DummyTable');
    const excludedId = 'string1';

    // Select records from the database.
    const selectFn = () => {
      return db
        .select()
        .from(tableSchema)
        .where(
          op.and(
            op.not(tableSchema.col('string').eq(excludedId)),
            tableSchema.col('string').in([excludedId, 'string2', 'string3'])
          )
        )
        .exec() as Promise<PayloadType[]>;
    };

    const results = await selectFn();
    assert.equal(2, results.length);

    const actualIds = results.map(result => result['string']);
    assert.sameMembers(actualIds, ['string2', 'string3']);
  });

  // Tests the case where a combined AND predicate is used with the NOT
  // operator.
  it('not_And', async () => {
    const tableSchema = db.getSchema().table('DummyTable');

    // Select records from the database.
    const selectFn = () => {
      return db
        .select()
        .from(tableSchema)
        .where(
          op.not(
            op.and(
              tableSchema.col('integer').gte(200),
              tableSchema.col('integer').lte(600)
            )
          )
        )
        .exec() as Promise<PayloadType[]>;
    };

    const results = await selectFn();
    const actualValues = results.map(result => result['integer']);
    const expectedValues = [0, 100, 700];
    assert.sameMembers(expectedValues, actualValues);
  });

  // Tests the case where a combined OR predicate is used with the NOT operator.
  it('not_Or', async () => {
    const tableSchema = db.getSchema().table('DummyTable');

    // Select records from the database.
    const selectFn = () => {
      return db
        .select()
        .from(tableSchema)
        .where(
          op.not(
            op.or(
              tableSchema.col('integer').lte(200),
              tableSchema.col('integer').gte(600)
            )
          )
        )
        .exec() as Promise<PayloadType[]>;
    };

    const results = await selectFn();
    const actualValues = results.map(result => result['integer']);
    const expectedValues = [500, 400, 300];
    assert.sameMembers(expectedValues, actualValues);
  });
});
