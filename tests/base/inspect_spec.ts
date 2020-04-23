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

import { DataStoreType, Type } from '../../lib/base/enum';
import { Inspector } from '../../lib/base/inspect';
import { RuntimeDatabase } from '../../lib/proc/runtime_database';
import { Builder } from '../../lib/schema/builder';

const assert = chai.assert;

describe('Inspect', () => {
  let expectedDate: Date;

  function createSchemaBuilders(): Builder[] {
    const dsHr = new Builder('hr', 2);
    dsHr
      .createTable('Region')
      .addColumn('id', Type.STRING)
      .addColumn('name', Type.STRING)
      .addPrimaryKey(['id']);
    dsHr
      .createTable('Foo')
      .addColumn('id', Type.STRING)
      .addColumn('bar', Type.INTEGER);

    const dsOrder = new Builder('order', 7);
    dsOrder
      .createTable('Region')
      .addColumn('id', Type.INTEGER)
      .addColumn('date', Type.DATE_TIME)
      .addPrimaryKey(['id']);

    return [dsHr, dsOrder];
  }

  async function addSample1(db: RuntimeDatabase): Promise<unknown> {
    const table = db.getSchema().table('Region');
    const rows = [];
    for (let i = 0; i < 100; ++i) {
      rows.push(
        table.createRow({
          id: `${i}`,
          name: `n${i}`,
        })
      );
    }
    return db
      .insert()
      .into(table)
      .values(rows)
      .exec();
  }

  async function addSample2(db: RuntimeDatabase): Promise<unknown> {
    expectedDate = new Date();
    const table = db.getSchema().table('Region');
    const rows = [];
    for (let i = 0; i < 100; ++i) {
      rows.push(
        table.createRow({
          date: expectedDate,
          id: i,
        })
      );
    }
    return db
      .insert()
      .into(table)
      .values(rows)
      .exec();
  }

  it('inspect', async () => {
    const builders = createSchemaBuilders();
    const options = {
      enableInspector: true,
      storeType: DataStoreType.MEMORY,
    };

    const db1 = (await builders[0].connect(options)) as RuntimeDatabase;
    const db2 = (await builders[1].connect(options)) as RuntimeDatabase;
    await addSample1(db1);
    await addSample2(db2);
    assert.equal('{"hr":2,"order":7}', Inspector.inspect(null, null));
    assert.equal('{"Region":100,"Foo":0}', Inspector.inspect('hr', null));
    assert.equal('{"Region":100}', Inspector.inspect('order', null));
    assert.equal(
      '[{"id":"88","name":"n88"},{"id":"89","name":"n89"}]',
      Inspector.inspect('hr', 'Region', 2, 88)
    );
    assert.equal('[]', Inspector.inspect('hr', 'Region', undefined, 100));
    assert.equal(
      `[{"date":${JSON.stringify(expectedDate)},"id":77}]`,
      Inspector.inspect('order', 'Region', 1, 77)
    );
    db1.close();
    db2.close();
  });
});
