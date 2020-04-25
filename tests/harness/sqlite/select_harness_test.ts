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

import {DatabaseConnection} from '../../../lib/base/database_connection';
import {DataStoreType, Type} from '../../../lib/base/enum';
import {PayloadType} from '../../../lib/base/row';
import {Builder} from '../../../lib/schema/builder';
import {Table} from '../../../lib/schema/table';

const assert = chai.assert;

describe('DeleteHarness', () => {
  let db: DatabaseConnection;
  let t1: Table;
  let t2: Table;

  before(async () => {
    const builder = new Builder('delete', 1);
    builder
      .createTable('t1')
      .addColumn('f1', Type.INTEGER)
      .addColumn('f2', Type.INTEGER);
    builder
      .createTable('t2')
      .addColumn('r1', Type.NUMBER)
      .addColumn('r2', Type.NUMBER);
    db = await builder.connect({storeType: DataStoreType.MEMORY});
    t1 = db.getSchema().table('t1');
    t2 = db.getSchema().table('t2');
  });

  after(() => db.close());

  function checkFlatten(
    expected: string,
    rows: PayloadType[],
    fields: string[],
    tablePrefix?: string
  ): void {
    const actual = rows
      .map(obj => {
        const objToCheck = tablePrefix
          ? (obj[tablePrefix] as PayloadType)
          : obj;
        assert.equal(fields.length, Object.keys(objToCheck).length);
        return fields
          .map(name => (objToCheck[name] as object).toString())
          .join(' ');
      })
      .join(' ');
    assert.equal(expected, actual);
  }

  it('Select1_1', async () => {
    const table1Row = t1.createRow({f1: 11, f2: 22});
    const table2Row = t2.createRow({r1: 1.1, r2: 2.2});

    await db
      .createTransaction()
      .exec([
        db.insert().into(t1).values([table1Row]),
        db.insert().into(t2).values([table2Row]),
      ]);
    // 1-1.1 not applicable
    // 1-1.2 not applicable
    // 1-1.3 not applicable

    // 1-1.4
    let results: PayloadType[] = (await db
      .select(t1.col('f1'))
      .from(t1)
      .exec()) as PayloadType[];
    checkFlatten('11', results, ['f1']);

    // 1-1.5
    results = (await db.select(t1.col('f2')).from(t1).exec()) as PayloadType[];
    checkFlatten('22', results, ['f2']);

    // 1-1.6, 1-1.7
    results = (await db
      .select(t1.col('f1'), t1.col('f2'))
      .from(t1)
      .exec()) as PayloadType[];
    checkFlatten('11 22', results, ['f1', 'f2']);

    // 1-1.8
    results = (await db.select().from(t1).exec()) as PayloadType[];
    checkFlatten('11 22', results, ['f1', 'f2']);

    // 1-1.8.1 not applicable
    // 1-1.8.2 not applicable
    // 1-1.8.3 not applicable

    // 1-1.9
    results = (await db.select().from(t1, t2).exec()) as PayloadType[];
    checkFlatten('11 22', results, ['f1', 'f2'], 't1');
    checkFlatten('1.1 2.2', results, ['r1', 'r2'], 't2');

    // 1-1.9.1 not applicable
    // 1-1.9.2 not applicable

    // 1-1.10
    results = (await db
      .select(t1.col('f1'), t2.col('r1'))
      .from(t1, t2)
      .exec()) as PayloadType[];
    checkFlatten('11', results, ['f1'], 't1');
    checkFlatten('1.1', results, ['r1'], 't2');

    // 1-1.11
    results = (await db
      .select(t1.col('f1'), t2.col('r1'))
      .from(t2, t1)
      .exec()) as PayloadType[];
    checkFlatten('11', results, ['f1'], 't1');
    checkFlatten('1.1', results, ['r1'], 't2');

    // 1-1.11.1
    results = (await db.select().from(t2, t1).exec()) as PayloadType[];
    checkFlatten('11 22', results, ['f1', 'f2'], 't1');
    checkFlatten('1.1 2.2', results, ['r1', 'r2'], 't2');

    // 1-1.11.2
    results = (await db
      .select()
      .from(t1.as('a'), t1.as('b'))
      .exec()) as PayloadType[];
    checkFlatten('11 22', results, ['f1', 'f2'], 'a');
    checkFlatten('11 22', results, ['f1', 'f2'], 'b');

    // 1-1.12 not applicable
    // 1-1.13 not applicable
  });
});
