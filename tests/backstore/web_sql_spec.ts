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

import {WebSql} from '../../lib/backstore/web_sql';
import {Capability} from '../../lib/base/capability';
import {DataStoreType, Type} from '../../lib/base/enum';
import {Global} from '../../lib/base/global';
import {PayloadType, Row} from '../../lib/base/row';
import {Service} from '../../lib/base/service';
import {DefaultCache} from '../../lib/cache/default_cache';
import {MemoryIndexStore} from '../../lib/index/memory_index_store';
import {Builder} from '../../lib/schema/builder';
import {DatabaseSchema} from '../../lib/schema/database_schema';
import {ScudTester} from '../../testing/backstore/scud_tester';
import {getMockSchemaBuilder} from '../../testing/mock_schema_builder';

const assert = chai.assert;

describe('WebSql', () => {
  let capability: Capability;
  let schemaName: string;
  let schema: DatabaseSchema;

  before(() => {
    capability = Capability.get();
    schemaName = `wsql${Date.now()}`;
  });

  beforeEach(() => {
    if (!capability.webSql) {
      return;
    }

    schema = getMockSchemaBuilder().getSchema();
    const global = Global.get();
    global.clear();

    Row.setNextId(0);
    global.registerService(Service.CACHE, new DefaultCache(schema));
    global.registerService(Service.INDEX_STORE, new MemoryIndexStore());
    global.registerService(Service.SCHEMA, schema);
  });

  it('SCUD', () => {
    if (!capability.webSql) {
      return;
    }

    // The schema name is on purpose padded with a timestamp to workaround the
    // issue that Chrome can't open the same WebSQL instance again if this test
    // has been run twice.
    const db = new WebSql(Global.get(), schema);
    const scudTester = new ScudTester(db, Global.get());

    return scudTester.run();
  });

  // Tests scanRowId() for the case where all tables are empty.
  it('rowId_Empty', async () => {
    if (!capability.webSql) {
      return Promise.resolve();
    }

    await new Builder(`foo${Date.now()}`, 1).connect({
      storeType: DataStoreType.WEB_SQL,
    });
    assert.equal(1, Row.getNextId());
  });

  function getSchemaBuilder(): Builder {
    const builder = new Builder(schemaName, 1);
    builder.createTable('foo').addColumn('id', Type.INTEGER);
    return builder;
  }

  // The following two tests test scanRowId() for non-empty database.
  // They must run in the given order.
  it('addRow', async () => {
    if (!capability.webSql) {
      return Promise.resolve();
    }

    const db = await getSchemaBuilder().connect({
      storeType: DataStoreType.WEB_SQL,
    });
    const t = db.getSchema().table('foo');
    const row = t.createRow({id: 1});

    await db.insert().into(t).values([row]).exec();
  });

  it('scanRowId', async () => {
    if (!capability.webSql) {
      return;
    }

    await getSchemaBuilder().connect({storeType: DataStoreType.WEB_SQL});
    assert.equal(2, Row.getNextId());
  });

  it('persistentIndex', () => {
    if (!capability.webSql) {
      return;
    }

    const builder = getMockSchemaBuilder(`foo${Date.now()}`, true);
    return builder.connect({storeType: DataStoreType.WEB_SQL});
  });

  it('reservedWordAsTableName', async () => {
    if (!capability.webSql) {
      return;
    }

    const builder = new Builder(`foo${Date.now()}`, 1);
    builder.createTable('Group').addColumn('id', Type.INTEGER);
    const db = await builder.connect({storeType: DataStoreType.WEB_SQL});
    const g = db.getSchema().table('Group');
    await db
      .insert()
      .into(g)
      .values([g.createRow({id: 1})])
      .exec();
    const results = (await db.select().from(g).exec()) as PayloadType[];
    assert.equal(1, results.length);
    assert.equal(1, results[0]['id']);
    await db.delete().from(g).exec();
  });
});
