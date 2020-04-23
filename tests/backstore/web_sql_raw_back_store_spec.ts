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

import { RawBackStore } from '../../lib/backstore/raw_back_store';
import { WebSql } from '../../lib/backstore/web_sql';
import { Capability } from '../../lib/base/capability';
import { DataStoreType, Type } from '../../lib/base/enum';
import { Global } from '../../lib/base/global';
import { Resolver } from '../../lib/base/resolver';
import { PayloadType } from '../../lib/base/row';
import { Service } from '../../lib/base/service';
import { ServiceId } from '../../lib/base/service_id';
import { DefaultCache } from '../../lib/cache/default_cache';
import { MemoryIndexStore } from '../../lib/index/memory_index_store';
import { Builder } from '../../lib/schema/builder';
import { DatabaseSchema } from '../../lib/schema/database_schema';
import { NestedPayloadType } from '../../testing/test_util';

const assert = chai.assert;

describe('WebSqlRawBackStore', () => {
  let capability: Capability;
  let sandbox: sinon.SinonSandbox;
  let upgradeDb: Database;
  let upgradeDbName: string;
  let upgradeGlobal: Global;

  before(() => {
    capability = Capability.get();
    if (!capability.webSql) {
      return;
    }

    sandbox = sinon.createSandbox();
    upgradeDbName = `upgrade${Date.now()}`;
    upgradeDb = window.openDatabase(
      upgradeDbName,
      '',
      'upgrade',
      2 * 1024 * 1024
    );
  });

  beforeEach(() => {
    if (!capability.webSql) {
      return;
    }

    const global = Global.get();
    global.registerService(Service.INDEX_STORE, new MemoryIndexStore());
    upgradeGlobal = new Global();
    global.registerService(new ServiceId('ns_' + upgradeDbName), upgradeGlobal);
    upgradeGlobal.registerService(Service.INDEX_STORE, new MemoryIndexStore());
  });

  afterEach(() => {
    if (!capability.webSql) {
      return;
    }

    Global.get().clear();
    sandbox.restore();
  });

  function getOldSchema(name?: string): DatabaseSchema {
    const builder = new Builder(name || `test${Date.now()}`, 1);
    builder
      .createTable('A')
      .addColumn('id', Type.STRING)
      .addColumn('name', Type.STRING);
    return builder.getSchema();
  }

  // Tests that onUpgrade function is still called with version 0 for a new DB
  // instance.
  it('newDBInstance', () => {
    if (!capability.webSql) {
      return;
    }

    const schema = getOldSchema();
    Global.get().registerService(Service.SCHEMA, schema);
    Global.get().registerService(Service.CACHE, new DefaultCache(schema));

    const onUpgrade = sandbox.spy((rawDb: RawBackStore) => {
      assert.equal(0, rawDb.getVersion());
      return Promise.resolve();
    });

    const db = new WebSql(Global.get(), getOldSchema());
    return db.init(onUpgrade).then(() => {
      assert.isTrue(onUpgrade.calledOnce);
    });
  });

  async function populateOldData(): Promise<void> {
    const CONTENTS = { id: 'hello', name: 'world' };
    const CONTENTS2 = { id: 'hello2', name: 'world2' };

    const builder = new Builder(upgradeDbName, 1);
    builder
      .createTable('A')
      .addColumn('id', Type.STRING)
      .addColumn('name', Type.STRING);

    const db = await builder.connect({ storeType: DataStoreType.WEB_SQL });
    const tableA = db.getSchema().tables()[0];
    const rows = [tableA.createRow(CONTENTS), tableA.createRow(CONTENTS2)];
    await db
      .insert()
      .into(tableA)
      .values(rows)
      .exec();
    const results = (await db
      .select()
      .from(tableA)
      .exec()) as unknown[];
    assert.equal(2, results.length);
  }

  function openDatabaseStub(
    name: DOMString,
    version: DOMString,
    desc: DOMString,
    size: number,
    callback?: (db: Database) => void
  ): Database {
    if (callback) {
      callback(upgradeDb);
    }
    return upgradeDb;
  }

  async function runTest(
    builder: Builder,
    onUpgrade: (raw: RawBackStore) => Promise<unknown>,
    checker: (data: PayloadType[]) => void,
    genOldData = false
  ): Promise<unknown> {
    sandbox.stub(window, 'openDatabase').callsFake(openDatabaseStub);

    const promise = genOldData ? populateOldData() : Promise.resolve();
    await promise;

    // Re-register schema
    upgradeGlobal.registerService(Service.SCHEMA, builder.getSchema());
    upgradeGlobal.registerService(
      Service.CACHE,
      new DefaultCache(builder.getSchema())
    );
    const db = await builder.connect({
      onUpgrade,
      storeType: DataStoreType.WEB_SQL,
    });
    const tableA = db.getSchema().tables()[0];
    const results = (await db
      .select()
      .from(tableA)
      .exec()) as PayloadType[];
    checker(results);
    return results;
  }

  // Due to restrictions of WebSQL, the states of test cases will
  // interfere each other. As a result, the tests are organized in the following
  // way:
  // addTableColumn will add a new column to table A
  // dropTableColumn will remove a column from table A
  // renameTableColumn will rename a column from table A
  // dump will dump data in table A
  // dropTable will delete table A
  //
  // mocha runs all test cases sequentially, therefore do not change the order
  // of these tests.

  it('addTableColumn', async () => {
    if (!capability.webSql) {
      return;
    }

    const builder = new Builder(upgradeDbName, 2);
    builder
      .createTable('A')
      .addColumn('id', Type.STRING)
      .addColumn('name', Type.STRING)
      .addColumn('something', Type.STRING);

    const onUpgrade = (store: RawBackStore) =>
      store.addTableColumn('A', 'something', 'nothing');

    await runTest(
      builder,
      onUpgrade,
      results => {
        assert.equal(2, results.length);
        assert.equal('nothing', results[0]['something']);
      },
      true
    );
  });

  it('dropTableColumn', async () => {
    if (!capability.webSql) {
      return;
    }

    const builder = new Builder(upgradeDbName, 3);
    builder
      .createTable('A')
      .addColumn('id', Type.STRING)
      .addColumn('name', Type.STRING);

    const onUpgrade = (store: RawBackStore) =>
      store.dropTableColumn('A', 'something');

    await runTest(builder, onUpgrade, results => {
      assert.equal(2, results.length);
      const payload = results[0];
      assert.isTrue(payload.hasOwnProperty('id'));
      assert.isTrue(payload.hasOwnProperty('name'));
      assert.isFalse(payload.hasOwnProperty('something'));
    });
  });

  it('renameTableColumn', async () => {
    if (!capability.webSql) {
      return;
    }

    const builder = new Builder(upgradeDbName, 4);
    builder
      .createTable('A')
      .addColumn('id', Type.STRING)
      .addColumn('lastName', Type.STRING);

    const onUpgrade = (store: RawBackStore) =>
      store.renameTableColumn('A', 'name', 'lastName');

    await runTest(builder, onUpgrade, results => {
      assert.equal(2, results.length);
      assert.equal('world', results[0]['lastName']);
    });
  });

  it('dump', async () => {
    if (!capability.webSql) {
      return;
    }

    const builder = new Builder(upgradeDbName, 5);
    builder
      .createTable('A')
      .addColumn('id', Type.STRING)
      .addColumn('lastName', Type.STRING);

    let dumpResult: PayloadType;

    const onUpgrade = (store: RawBackStore) => {
      return store
        .dump()
        .then(results => (dumpResult = results as PayloadType));
    };

    await runTest(builder, onUpgrade, results => {
      const rowsA = dumpResult['A'] as NestedPayloadType[];
      assert.equal(2, rowsA.length);
      assert.deepEqual('world', rowsA[0]['value']['lastName']);
    });
  });

  it('dropTable', async () => {
    if (!capability.webSql) {
      return;
    }

    const builder = new Builder(upgradeDbName, 6);
    builder
      .createTable('B')
      .addColumn('id', Type.STRING)
      .addColumn('lastName', Type.STRING);

    const onUpgrade = (store: RawBackStore) => {
      return store.dropTable('A');
    };

    await runTest(builder, onUpgrade, results => {
      const resolver = new Resolver();
      const reject = (tx: SQLTransaction, e: SQLError) => {
        resolver.reject(e);
        return false;
      };
      upgradeDb.readTransaction(tx => {
        tx.executeSql(
          'SELECT tbl_name FROM sqlite_master WHERE type="table"',
          [],
          (transaction, rowsSet) => {
            const res: string[] = [];
            for (let i = 0; i < rowsSet.rows.length; ++i) {
              res.push(rowsSet.rows.item(i)['tbl_name']);
            }
            assert.isTrue(res.indexOf('B') !== -1);
            assert.equal(-1, res.indexOf('A'));
            resolver.resolve();
          },
          reject
        );
      });
      return resolver.promise;
    });
  });
});
