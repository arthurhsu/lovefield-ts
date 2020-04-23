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

import { DatabaseConnection } from '../../../lib/base/database_connection';
import { DataStoreType, Type } from '../../../lib/base/enum';
import { PayloadType, Row } from '../../../lib/base/row';
import { fn } from '../../../lib/fn/fn';
import { Builder } from '../../../lib/schema/builder';
import { Table } from '../../../lib/schema/table';

const assert = chai.assert;

describe('DeleteHarness', () => {
  let db: DatabaseConnection;
  let t1: Table;

  before(async () => {
    const builder = new Builder('delete', 1);
    builder
      .createTable('t1')
      .addColumn('f1', Type.INTEGER)
      .addColumn('f2', Type.INTEGER);
    db = await builder.connect({ storeType: DataStoreType.MEMORY });
    t1 = db.getSchema().table('t1');
  });

  after(() => db.close());

  function checkFlatten(
    expected: string,
    rows: PayloadType[],
    fields: string[]
  ): void {
    const actual = rows
      .map(obj => {
        assert.equal(fields.length, Object.keys(obj).length);
        return fields.map(name => (obj[name] as object).toString()).join(' ');
      })
      .join(' ');
    assert.equal(expected, actual);
  }

  it('Delete3', async () => {
    // 3.1
    const rows = [];
    for (let i = 1; i <= 4; ++i) {
      rows.push(t1.createRow({ f1: i, f2: Math.pow(2, i) }));
    }

    // 3.1.1
    await db
      .insert()
      .into(t1)
      .values(rows)
      .exec();
    let results: PayloadType[] = (await db
      .select()
      .from(t1)
      .orderBy(t1.col('f1'))
      .exec()) as PayloadType[];
    checkFlatten('1 2 2 4 3 8 4 16', results, ['f1', 'f2']);

    // 3.1.2
    await db
      .delete()
      .from(t1)
      .where(t1.col('f1').eq(3))
      .exec();

    // 3.1.3
    results = (await db
      .select()
      .from(t1)
      .orderBy(t1.col('f1'))
      .exec()) as PayloadType[];
    checkFlatten('1 2 2 4 4 16', results, ['f1', 'f2']);

    // 3.1.4 - not exactly the same
    await db
      .delete()
      .from(t1)
      .where(t1.col('f1').eq(3))
      .exec();

    // 3.1.5
    results = (await db
      .select()
      .from(t1)
      .orderBy(t1.col('f1'))
      .exec()) as PayloadType[];
    checkFlatten('1 2 2 4 4 16', results, ['f1', 'f2']);

    // 3.1.6
    await db
      .delete()
      .from(t1)
      .where(t1.col('f1').eq(2))
      .exec();

    // 3.1.7
    results = (await db
      .select()
      .from(t1)
      .orderBy(t1.col('f1'))
      .exec()) as PayloadType[];
    checkFlatten('1 2 4 16', results, ['f1', 'f2']);
  });

  it('Delete5', async () => {
    // 5.1.1
    await db
      .delete()
      .from(t1)
      .exec();

    // 5.1.2
    let results: PayloadType[] = (await db
      .select(fn.count())
      .from(t1)
      .exec()) as PayloadType[];
    assert.equal(0, results[0]['COUNT(*)']);

    // 5.2.1
    const rows: Row[] = [];
    for (let i = 1; i <= 200; i++) {
      rows.push(t1.createRow({ f1: i, f2: i * i }));
    }
    await db
      .insert()
      .into(t1)
      .values(rows)
      .exec();
    results = (await db
      .select(fn.count())
      .from(t1)
      .exec()) as PayloadType[];
    assert.equal(200, results[0]['COUNT(*)']);

    // 5.2.2
    await db
      .delete()
      .from(t1)
      .exec();
    results = (await db
      .select(fn.count())
      .from(t1)
      .exec()) as PayloadType[];
    assert.equal(0, results[0]['COUNT(*)']);

    // 5.2.3
    await db
      .insert()
      .into(t1)
      .values(rows)
      .exec();
    results = (await db
      .select(fn.count())
      .from(t1)
      .exec()) as PayloadType[];
    assert.equal(200, results[0]['COUNT(*)']);

    // 5.2.4
    await db
      .delete()
      .from(t1)
      .exec();

    // 5.2.5
    results = (await db
      .select(fn.count())
      .from(t1)
      .exec()) as PayloadType[];
    assert.equal(0, results[0]['COUNT(*)']);

    // 5.2.6
    await db
      .insert()
      .into(t1)
      .values(rows)
      .exec();
    results = (await db
      .select(fn.count())
      .from(t1)
      .exec()) as PayloadType[];
    assert.equal(200, results[0]['COUNT(*)']);

    // 5.3
    let promises: Array<Promise<unknown>> = [];
    for (let i = 1; i <= 200; i += 4) {
      promises.push(
        db
          .delete()
          .from(t1)
          .where(t1.col('f1').eq(i))
          .exec()
      );
    }
    await Promise.all(promises);
    results = (await db
      .select(fn.count())
      .from(t1)
      .exec()) as PayloadType[];
    assert.equal(150, results[0]['COUNT(*)']);

    // 5.4.1
    await db
      .delete()
      .from(t1)
      .where(t1.col('f1').gt(50))
      .exec();

    // 5.4.2
    results = (await db
      .select(fn.count())
      .from(t1)
      .exec()) as PayloadType[];
    assert.equal(37, results[0]['COUNT(*)']);

    // 5.5
    promises = [];
    for (let i = 1; i <= 70; i += 3) {
      promises.push(
        db
          .delete()
          .from(t1)
          .where(t1.col('f1').eq(i))
          .exec()
      );
    }
    await Promise.all(promises);
    results = (await db
      .select(t1.col('f1'))
      .from(t1)
      .orderBy(t1.col('f1'))
      .exec()) as PayloadType[];
    checkFlatten(
      '2 3 6 8 11 12 14 15 18 20 23 24 26 27 30 ' +
        '32 35 36 38 39 42 44 47 48 50',
      results,
      ['f1']
    );

    // 5.6
    promises = [];
    for (let i = 1; i < 40; ++i) {
      promises.push(
        db
          .delete()
          .from(t1)
          .where(t1.col('f1').eq(i))
          .exec()
      );
    }
    await Promise.all(promises);
    results = (await db
      .select(t1.col('f1'))
      .from(t1)
      .orderBy(t1.col('f1'))
      .exec()) as PayloadType[];
    checkFlatten('42 44 47 48 50', results, ['f1']);

    // 5.7
    await db
      .delete()
      .from(t1)
      .where(t1.col('f1').neq(48))
      .exec();
    results = (await db
      .select(t1.col('f1'))
      .from(t1)
      .orderBy(t1.col('f1'))
      .exec()) as PayloadType[];
    checkFlatten('48', results, ['f1']);
  });
});
