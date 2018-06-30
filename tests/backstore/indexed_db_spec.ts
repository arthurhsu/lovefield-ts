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

import {IndexedDB} from '../../lib/backstore/indexed_db';
import {ObjectStore} from '../../lib/backstore/object_store';
import {Tx} from '../../lib/backstore/tx';
import {Capability} from '../../lib/base/capability';
import {TransactionType} from '../../lib/base/enum';
import {Global} from '../../lib/base/global';
import {TableType} from '../../lib/base/private_enum';
import {Row} from '../../lib/base/row';
import {Service} from '../../lib/base/service';
import {Cache} from '../../lib/cache/cache';
import {DefaultCache} from '../../lib/cache/default_cache';
import {Journal} from '../../lib/cache/journal';
import {IndexStore} from '../../lib/index/index_store';
import {MemoryIndexStore} from '../../lib/index/memory_index_store';
import {Table} from '../../lib/schema/table';
import {MockSchema} from '../../testing/backstore/mock_schema';
import {ScudTester} from '../../testing/backstore/scud_tester';
import {TestUtil} from '../../testing/test_util';

const assert = chai.assert;
const skip = !(Capability.get().indexedDb);
const test = skip ? describe.skip : describe;

test('IndexedDB', () => {
  let db: IndexedDB;
  let schema: MockSchema;
  let cache: Cache;
  let indexStore: IndexStore;
  let sandbox: sinon.SinonSandbox;

  function setUpEnv(): void {
    schema = new MockSchema();
    cache = new DefaultCache(schema);
    indexStore = new MemoryIndexStore();
    const global = Global.get();
    global.registerService(Service.CACHE, cache);
    global.registerService(Service.INDEX_STORE, indexStore);
    global.registerService(Service.SCHEMA, schema);
    Row.setNextId(0);
  }

  beforeEach(() => {
    sandbox = sinon.createSandbox();
    setUpEnv();
  });

  afterEach(() => {
    sandbox.restore();
    if (db) {
      // Clearing all tables.
      const promises = schema.tables().map((table) => {
        const tx = db.createTx(
            TransactionType.READ_WRITE, [table], createJournal([table]));
        const store = tx.getTable(
            table.getName(), table.deserializeRow.bind(table), TableType.DATA);

        store.remove([]);
        return tx.commit();
      });

      return Promise.all(promises);
    }
    return Promise.resolve();
  });

  function createJournal(tables: Table[]): Journal {
    return new Journal(Global.get(), new Set<Table>(tables));
  }

  it('SCUD', () => {
    db = new IndexedDB(Global.get(), schema);
    const scudTester = new ScudTester(db, Global.get());

    return scudTester.run();
  });

  it('SCUD_Bundled', () => {
    schema.setName(schema.name() + '_bundled');
    schema.setBundledMode(true);
    db = new IndexedDB(Global.get(), schema);
    const scudTester = new ScudTester(db, Global.get());

    return scudTester.run();
  });

  it('twoTableInserts_Bundled', async () => {
    const global = Global.get();
    schema.setName(schema.name() + '_b2');
    schema.setBundledMode(true);
    db = new IndexedDB(global, schema);
    global.registerService(Service.BACK_STORE, db);

    const CONTENTS = {id: 'hello', name: 'world'};
    const CONTENTS2 = {id: 'hello2', name: 'world2'};

    const tableA = schema.table('tableA');
    const tableB = schema.table('tableB');
    const row = Row.create(CONTENTS);
    const row2 = Row.create(CONTENTS);
    const row3 = Row.create(CONTENTS2);
    const row4 = new Row(row.id(), CONTENTS2);

    const getTableA = (tx1: Tx) => {
      return tx1.getTable(
                 tableA.getName(), tableA.deserializeRow.bind(tableA),
                 TableType.DATA) as ObjectStore;
    };

    const getTableB = (tx2: Tx) => {
      return tx2.getTable(
                 tableB.getName(), tableB.deserializeRow.bind(tableB),
                 TableType.DATA) as ObjectStore;
    };

    await db.init();
    let tx = db.createTx(
        TransactionType.READ_WRITE, [tableA], createJournal([tableA]));
    let store = getTableA(tx);

    // insert row1 into table A
    store.put([row]);
    await tx.commit();
    tx = db.createTx(
        TransactionType.READ_WRITE, [tableB], createJournal([tableB]));
    store = getTableB(tx);

    // insert row2 into table B
    store.put([row2]);
    await tx.commit();

    tx = db.createTx(TransactionType.READ_ONLY, [tableB]);
    store = getTableB(tx);

    // get row2 from table B
    let results = await store.get([row2.id()]);

    assert.equal(1, results.length);
    assert.equal(row2.id(), results[0].id());
    assert.deepEqual(CONTENTS, results[0].payload());

    tx = db.createTx(
        TransactionType.READ_WRITE, [tableA], createJournal([tableA]));
    store = getTableA(tx);

    // update row1, insert row3 into table A
    store.put([row4, row3]);
    await tx.commit();
    results = await TestUtil.selectAll(global, tableA);
    assert.equal(2, results.length);
    assert.equal(row4.id(), results[0].id());
    assert.deepEqual(CONTENTS2, results[0].payload());
    assert.equal(row3.id(), results[1].id());
    assert.deepEqual(CONTENTS2, results[1].payload());

    // Update cache, otherwise the bundled operation will fail.
    cache.setMany(tableA.getName(), [row4, row3]);

    tx = db.createTx(
        TransactionType.READ_WRITE, [tableA], createJournal([tableA]));
    store = getTableA(tx);

    // remove row1
    store.remove([row3.id()]);
    await tx.commit();
    results = await TestUtil.selectAll(global, tableA);
    assert.equal(1, results.length);
    assert.equal(row4.id(), results[0].id());
    assert.deepEqual(CONTENTS2, results[0].payload());
  });

  it('scanRowId', async () => {
    // Generates a set of rows where they are on purpose not sorted with respect
    // to the row ID and with the larger rowID in position 0.
    const generateRows = (): Row[] => {
      const rowIds = [200, 9, 1, 3, 2, 20, 100];
      const CONTENTS = {scan: 'rowid'};
      return rowIds.map((rowId) => {
        return new Row(rowId, CONTENTS);
      });
    };

    const insertIntoTable = (values: Row[]) => {
      const table = schema.table('tableA');
      const tx = db.createTx(
          TransactionType.READ_WRITE, [table], createJournal([table]));
      const store = tx.getTable(
          table.getName(), table.deserializeRow.bind(table), TableType.DATA);
      store.put(values);
      return tx.commit();
    };

    db = new IndexedDB(Global.get(), schema);
    const rows = generateRows();
    await db.init();
    assert.equal(Row.getNextId(), 1);
    Row.setNextId(1);
    await insertIntoTable(rows);
    db.close();
    await db.init();
    assert.equal(Row.getNextId(), rows[0].id() + 1);
  });

  // Tests scanRowId() for the case where all tables are empty.
  it('scanRowId_Empty', async () => {
    db = new IndexedDB(Global.get(), schema);
    await db.init();
    assert.equal(1, Row.getNextId());
  });

  it('scanRowId_BundledDB', async () => {
    const insertIntoTable = () => {
      const CONTENTS = {scan: 'rowid'};
      const rows = [];
      for (let i = 0; i <= 2048; i += 256) {
        rows.push(new Row(i, CONTENTS));
      }

      const table = schema.table('tableA');
      const tx = db.createTx(
          TransactionType.READ_WRITE, [table], createJournal([table]));
      const store = tx.getTable(
          table.getName(), table.deserializeRow.bind(table), TableType.DATA);

      store.put(rows);
      return tx.commit();
    };

    schema.setBundledMode(true);
    db = new IndexedDB(Global.get(), schema);
    await db.init();
    await insertIntoTable();
    db.close();
    db = new IndexedDB(Global.get(), schema);
    await db.init();
    assert.equal(2048 + 1, Row.getNextId());
  });

  function filterTableA(): string[] {
    const list = ((db as any).db as IDBDatabase).objectStoreNames;
    const results: string[] = [];
    for (let i = 0; i < list.length; ++i) {
      const name = list.item(i) as string;
      if (name.indexOf('tableA') !== -1) {
        results.push(list.item(i) as string);
      }
    }
    return results;
  }

  it('upgrade', async () => {
    // Randomize schema name to ensure upgrade test works.
    const name = schema.name() + Date.now();
    schema.setName(name);

    // Modifying tableA to use persisted indices.
    sandbox.stub(schema.table('tableA'), 'persistentIndex')
        .callsFake(() => true);

    db = new IndexedDB(Global.get(), schema);
    await db.init();

    // Verify that index tables are created.
    let tables = filterTableA();
    assert.isTrue(tables.length > 1);
    db.close();
    sandbox.restore();

    setUpEnv();  // reset the environment
    schema.setVersion(2);
    schema.setName(name);
    db = new IndexedDB(Global.get(), schema);
    await db.init();

    tables = filterTableA();
    assert.equal(1, tables.length);
    const table = schema.tables().slice(-1)[0];
    assert.equal('tablePlusOne', table.getName());
    const tx = db.createTx(
        TransactionType.READ_WRITE, [table], createJournal([table]));
    const store = tx.getTable(
        table.getName(), table.deserializeRow.bind(table), TableType.DATA);
    assert.isNotNull(store);
  });
});
