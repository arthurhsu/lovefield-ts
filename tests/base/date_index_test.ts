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

import {DataStoreType, Order} from '../../lib/base/enum';
import {Row} from '../../lib/base/row';
import {RuntimeDatabase} from '../../lib/proc/runtime_database';
import {Table} from '../../lib/schema/table';
import {getHrDbSchemaBuilder} from '../../testing/hr_schema/hr_schema_builder';

const assert = chai.assert;

describe('DateIndex', () => {
  let db: RuntimeDatabase;
  let holiday: Table;

  beforeEach(async () => {
    db = await getHrDbSchemaBuilder().connect(
             {storeType: DataStoreType.MEMORY}) as RuntimeDatabase;
    holiday = db.getSchema().table('Holiday');
  });

  // Generates sample records to be used for testing.
  function generateSampleRows(): Row[] {
    return [
      holiday.createRow({
        begin: new Date(2013, 11, 31),
        end: new Date(2014, 0, 1, 23, 59, 59),
        name: '2014 New Year\'s Day',
      }),
      holiday.createRow({
        begin: new Date(2014, 0, 20),
        end: new Date(2014, 0, 20, 23, 59, 59),
        name: '2014 MLK Day',
      }),
      holiday.createRow({
        begin: new Date(2014, 1, 17),
        end: new Date(2014, 1, 17, 23, 59, 59),
        name: '2014 President\'s Day',
      }),
      holiday.createRow({
        begin: new Date(2014, 4, 26),
        end: new Date(2014, 4, 26, 23, 59, 59),
        name: '2014 Memorial Day',
      }),
      holiday.createRow({
        begin: new Date(2014, 6, 3),
        end: new Date(2014, 6, 4, 23, 59, 59),
        name: '2014 Independence Day',
      }),
      holiday.createRow({
        begin: new Date(2014, 8, 1),
        end: new Date(2014, 8, 1, 23, 59, 59),
        name: '2014 Labor Day',
      }),
      holiday.createRow({
        begin: new Date(2014, 10, 27),
        end: new Date(2014, 10, 28, 23, 59, 59),
        name: '2014 Thanksgiving',
      }),
      holiday.createRow({
        begin: new Date(2014, 11, 24),
        end: new Date(2014, 11, 26, 23, 59, 59),
        name: '2014 Christmas',
      }),
    ];
  }

  it('dateIndex', async () => {
    const rows = generateSampleRows();
    const expected =
        rows.map((row) => row.payload()['name']).slice(1).reverse();

    await db.insert().into(holiday).values(rows).exec();
    const query = db.select()
                      .from(holiday)
                      .where(holiday['begin'].gt(new Date(2014, 0, 1)))
                      .orderBy(holiday['begin'], Order.DESC);
    assert.notEqual(-1, query.explain().indexOf('index_range_scan'));
    const results: object[] = await query.exec();
    assert.sameDeepOrderedMembers(expected, results.map((row) => row['name']));
  });
});
