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

import {DataStoreType, TransactionType} from '../../lib/base/enum';
import {TableType} from '../../lib/base/private_enum';
import {Row} from '../../lib/base/row';
import {Service} from '../../lib/base/service';
import {RuntimeDatabase} from '../../lib/proc/runtime_database';
import {BaseTable} from '../../lib/schema/base_table';
import {getHrDbSchemaBuilder} from '../../testing/hr_schema/hr_schema_builder';

const assert = chai.assert;

describe('Conversion', () => {
  let db: RuntimeDatabase;

  beforeEach(async () => {
    db = (await getHrDbSchemaBuilder().connect({
      storeType: DataStoreType.MEMORY,
    })) as RuntimeDatabase;
  });

  it('conversions', async () => {
    const tableSchema = db.getSchema().table('DummyTable');
    const row = tableSchema.createRow({
      arrayBuffer: new ArrayBuffer(0),
      boolean: false,
      datetime: new Date(),
      integer: 3,
      number: Math.PI,
      string: 'dummy-string',
      string2: 'dummy-string2',
    });

    await db.insert().into(tableSchema).values([row]).exec();

    // Selects the sample record from the database, skipping the cache, to
    // ensure that deserialization is working when reading a record from the
    // backstore.
    const backStore = db.getGlobal().getService(Service.BACK_STORE);
    const tx = backStore.createTx(TransactionType.READ_ONLY, [tableSchema]);
    const store = tx.getTable(
      tableSchema.getName(),
      (tableSchema as BaseTable).deserializeRow,
      TableType.DATA
    );
    const result = await store.get([]);
    assert.equal(1, result.length);
    assert.isTrue(result[0] instanceof Row);

    const insertedRow = row.payload();
    const retrievedRow = result[0].payload();
    assert.equal(insertedRow['boolean'], retrievedRow['boolean']);
    assert.equal(insertedRow['string'], retrievedRow['string']);
    assert.equal(insertedRow['number'], retrievedRow['number']);
    assert.equal(insertedRow['integer'], retrievedRow['integer']);
    assert.equal(
      (insertedRow['datetime'] as Date).getTime(),
      (retrievedRow['datetime'] as Date).getTime()
    );
    assert.equal(
      Row.binToHex(insertedRow['arraybuffer'] as ArrayBuffer),
      Row.binToHex(retrievedRow['arraybuffer'] as ArrayBuffer)
    );
  });
});
