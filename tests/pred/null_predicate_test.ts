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

import { DataStoreType } from '../../lib/base/enum';
import { PayloadType, Row } from '../../lib/base/row';
import { RuntimeDatabase } from '../../lib/proc/runtime_database';
import { getHrDbSchemaBuilder } from '../../testing/hr_schema/hr_schema_builder';
import { BaseColumn } from '../../lib/schema/base_column';

const assert = chai.assert;

describe('NullPredicate', () => {
  let db: RuntimeDatabase;

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
    const deletePromises = tables.map(table =>
      db
        .delete()
        .from(table)
        .exec()
    );

    return Promise.all(deletePromises).then(() => {
      return;
    });
  }

  //  Creates two sample rows. One with a specified 'datetime' and one
  // with a null 'datetime'.
  function generateSampleDummyData(): Row[] {
    const tableSchema = db.getSchema().table('DummyTable');

    const row1 = tableSchema.createRow({
      boolean: false,
      datetime: new Date(),
      integer: 100,
      number: 100,
      string: 'string1',
      string2: 'string21',
    });

    const row2 = tableSchema.createRow({
      boolean: false,
      datetime: null,
      integer: 200,
      number: 200,
      string: 'string2',
      string2: 'string22',
    });

    return [row1, row2];
  }

  function generateSampleRegionData(): Row[] {
    const tableSchema = db.getSchema().table('Region');
    const rows = new Array(3);
    for (let i = 0; i < rows.length; i++) {
      rows[i] = tableSchema.createRow({
        id: `regionId${i}`,
        name: `regionName${i}`,
      });
    }
    return rows;
  }

  // Inserts sample records in the database.
  async function populateDatabase(): Promise<unknown> {
    const dummy = db.getSchema().table('DummyTable');
    const region = db.getSchema().table('Region');
    const tx = db.createTransaction();
    return tx.exec([
      db
        .insert()
        .into(dummy)
        .values(generateSampleDummyData()),
      db
        .insert()
        .into(region)
        .values(generateSampleRegionData()),
    ]);
  }

  // Tests the case where an isNull() predicate is used on a non-indexed field.
  it('isNull_NonIndexed', async () => {
    const tableSchema = db.getSchema().table('DummyTable');
    // Ensure that the 'datetime' field is not indexed.
    assert.equal(
      0,
      (tableSchema['datetime'] as BaseColumn).getIndices().length
    );
    const sampleData = generateSampleDummyData();

    // Select records to the database.
    const selectFn = () => {
      return db
        .select()
        .from(tableSchema)
        .where(tableSchema['datetime'].isNull())
        .exec();
    };

    const result = await selectFn();
    assert.equal(1, result.length);

    // Expecting the second sample row to have been retrieved.
    const retrievedEmployee = result[0] as PayloadType;
    assert.equal(
      sampleData[1].payload()['string'],
      retrievedEmployee['string']
    );
  });

  // Tests the case where an isNotNull() predicate is used on a non-indexed
  // field.
  it('isNotNull_NonIndexed', async () => {
    const tableSchema = db.getSchema().table('DummyTable');
    // Ensure that the 'datetime' field is not indexed.
    assert.equal(
      0,
      (tableSchema['datetime'] as BaseColumn).getIndices().length
    );
    const sampleData = generateSampleDummyData();

    // Select records to the database.
    const selectFn = () => {
      return db
        .select()
        .from(tableSchema)
        .where(tableSchema['datetime'].isNotNull())
        .exec();
    };

    const result = await selectFn();
    assert.equal(1, result.length);

    // Expecting the first sample row to have been retrieved.
    const retrievedEmployee = result[0] as PayloadType;
    assert.equal(
      sampleData[0].payload()['string'],
      retrievedEmployee['string']
    );
  });

  // Tests the case where an isNull() predicate is used on an indexed field.
  it('isNull_Indexed', async () => {
    const tableSchema = db.getSchema().table('Region');
    // Ensure that the 'id' field is indexed.
    assert.isTrue(
      (tableSchema['id'] as BaseColumn).getIndices().length >= 1
    );

    // Select records to the database.
    const selectFn = () => {
      return db
        .select()
        .from(tableSchema)
        .where(tableSchema['id'].isNull())
        .exec();
    };

    const result = await selectFn();
    assert.equal(0, result.length);
  });

  // Tests the case where an isNotNull() predicate is used on a indexed field.
  it('isNotNull_Indexed', async () => {
    const tableSchema = db.getSchema().table('Region');
    // Ensure that the 'id' field is indexed.
    assert.isTrue(
      (tableSchema['id'] as BaseColumn).getIndices().length >= 1
    );
    const sampleData = generateSampleRegionData();

    // Select records to the database.
    const selectFn = () => {
      return db
        .select()
        .from(tableSchema)
        .where(tableSchema['id'].isNotNull())
        .exec();
    };

    const result = await selectFn();
    assert.equal(sampleData.length, result.length);
  });
});
