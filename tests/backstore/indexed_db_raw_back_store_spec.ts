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

import {assert} from 'chai';
import * as sinon from 'sinon';

import {IndexedDB} from '../../lib/backstore/indexed_db';
import {IndexedDBRawBackStore} from '../../lib/backstore/indexed_db_raw_back_store';
import {Page} from '../../lib/backstore/page';
import {RawBackStore} from '../../lib/backstore/raw_back_store';
import {Capability} from '../../lib/base/capability';
import {Type} from '../../lib/base/enum';
import {RawRow, Row, PayloadType} from '../../lib/base/row';
import {Builder} from '../../lib/schema/builder';
import {schema} from '../../lib/schema/schema';
import {TestUtil} from '../../testing/test_util';

const skip = !Capability.get().indexedDb;
const test = skip ? describe.skip : describe;

test('IndexedDBRawBackStore', () => {
  const CONTENTS = {id: 'hello', name: 'world'};
  const CONTENTS2 = {id: 'hello2', name: 'world2'};
  const MAGIC = Math.pow(2, Page.BUNDLE_EXPONENT);
  let builder1: Builder;
  let builder2: Builder;

  beforeEach(() => {
    const dbName = `schema${Date.now()}`;
    builder1 = schema.create(dbName, 1);
    builder1
      .createTable('tableA_')
      .addColumn('id', Type.STRING)
      .addColumn('name', Type.STRING);
    builder1
      .createTable('tableB_')
      .addColumn('id', Type.STRING)
      .addColumn('name', Type.STRING);
    builder2 = schema.create(dbName, 2);
    builder2
      .createTable('tableA_')
      .addColumn('id', Type.STRING)
      .addColumn('name', Type.STRING);
  });

  it('convert', () => {
    const date = new Date();
    const buffer = new ArrayBuffer(8);
    const view = new Uint8Array(buffer);
    for (let i = 0; i < 8; ++i) {
      view[i] = i;
    }

    const convert = IndexedDBRawBackStore.convert;
    assert.equal(date.getTime(), convert(date));
    assert.equal('0001020304050607', convert(buffer));
    assert.equal(2, convert(2));
    assert.equal(3.3, convert(3.3));
    assert.isFalse(convert(false));
    assert.isTrue(convert(true));
    assert.equal('quick fox', convert('quick fox'));
  });

  // Tests that onUpgrade function is still called with version 0 for a new DB
  // instance.
  it('newDBInstance', async () => {
    const onUpgrade = sinon.spy((rawDb: RawBackStore) => {
      assert.equal(0, rawDb.getVersion());
      return Promise.resolve();
    });

    const db = new IndexedDB(builder1.getGlobal(), builder1.getSchema());
    await db.init(onUpgrade);
    assert.equal(1, onUpgrade.callCount);
  });

  function dumpDB<T>(
    db: IDBDatabase,
    tableName: string,
    fn: (raw: RawRow) => T
  ): Promise<T[]> {
    return new Promise<T[]>((resolve, reject) => {
      const results: T[] = [];
      const tx = db.transaction([tableName], 'readonly');
      const req = tx.objectStore(tableName).openCursor();
      req.onsuccess = () => {
        const cursor = req.result as IDBCursorWithValue;
        if (cursor) {
          results.push(fn(cursor.value));
          cursor.continue();
        } else {
          resolve(results);
        }
      };
      req.onerror = reject;
    });
  }

  function dumpTable(db: IDBDatabase, tableName: string): Promise<Row[]> {
    return dumpDB(db, tableName, Row.deserialize);
  }

  function dumpTableBundled(
    db: IDBDatabase,
    tableName: string
  ): Promise<Page[]> {
    return dumpDB(db, tableName, Page.deserialize);
  }

  function upgradeAddTableColumn(
    date: Date,
    dbInterface: RawBackStore
  ): Promise<void> {
    const db = dbInterface as IndexedDBRawBackStore;
    assert.equal(1, db.getVersion());
    return db.addTableColumn('tableA_', 'dob', date);
  }

  function prepareTxForTableA(db: IDBDatabase): Promise<void> {
    const row = Row.create(CONTENTS);
    const row2 = Row.create(CONTENTS2);
    return new Promise((resolve, reject) => {
      const tx = db.transaction(['tableA_'], 'readwrite');
      const store = tx.objectStore('tableA_');
      store.put(row.serialize());
      store.put(row2.serialize());
      tx.oncomplete = () => {
        resolve();
      };
      tx.onabort = reject;
    });
  }

  function prepareBundledTxForTableA(db: IDBDatabase): Promise<void> {
    const row = new Row(0, CONTENTS);
    const row2 = new Row(MAGIC, CONTENTS2);
    const page = new Page(0);
    const page2 = new Page(1);
    page.setRows([row]);
    page2.setRows([row2]);

    return new Promise((resolve, reject) => {
      const tx = db.transaction(['tableA_'], 'readwrite');
      const store = tx.objectStore('tableA_');
      store.put(page.serialize());
      store.put(page2.serialize());
      tx.oncomplete = () => {
        resolve();
      };
      tx.onabort = reject;
    });
  }

  it('addTableColumn', async () => {
    let db: IndexedDB | null = new IndexedDB(
      builder1.getGlobal(),
      builder1.getSchema()
    );
    const date = new Date();

    const rawDb = await db.init();
    await prepareTxForTableA(rawDb);
    (db as IndexedDB).close();
    db = null;
    db = new IndexedDB(builder2.getGlobal(), builder2.getSchema());
    const newDb = await db.init((raw: RawBackStore) =>
      upgradeAddTableColumn(date, raw)
    );
    const results = await dumpTable(newDb, 'tableA_');
    assert.equal(2, results.length);
    assert.equal(date.getTime(), results[0].payload()['dob']);
    assert.equal(date.getTime(), results[1].payload()['dob']);
  });

  it('addTableColumn_Bundled', async () => {
    builder1.setPragma({enableBundledMode: true});
    builder2.setPragma({enableBundledMode: true});
    let db: IndexedDB | null = new IndexedDB(
      builder1.getGlobal(),
      builder1.getSchema()
    );
    const date = new Date();

    const rawDb = await db.init();
    await prepareBundledTxForTableA(rawDb);
    (db as IndexedDB).close();
    db = null;
    db = new IndexedDB(builder2.getGlobal(), builder2.getSchema());
    const newDb = await db.init((raw: RawBackStore) =>
      upgradeAddTableColumn(date, raw)
    );
    const results = await dumpTableBundled(newDb, 'tableA_');
    assert.equal(2, results.length);
    const newRow = Row.deserialize(results[0].getPayload()[0] as RawRow);
    const newRow2 = Row.deserialize(results[1].getPayload()[MAGIC] as RawRow);
    assert.equal(date.getTime(), newRow.payload()['dob']);
    assert.equal(date.getTime(), newRow2.payload()['dob']);
  });

  function upgradeDropTableColumn(dbInterface: RawBackStore): Promise<void> {
    const db = dbInterface as IndexedDBRawBackStore;
    assert.equal(1, db.getVersion());
    return db.dropTableColumn('tableA_', 'name');
  }

  it('dropTableColumn', async () => {
    let db: IndexedDB | null = new IndexedDB(
      builder1.getGlobal(),
      builder1.getSchema()
    );
    const rawDb = await db.init();
    await prepareTxForTableA(rawDb);
    (db as IndexedDB).close();
    db = null;
    db = new IndexedDB(builder2.getGlobal(), builder2.getSchema());
    const newDb = await db.init(upgradeDropTableColumn);
    const results = await dumpTable(newDb, 'tableA_');
    assert.equal(2, results.length);
    assert.isFalse(TestUtil.hasProperty(results[0].payload(), 'name'));
    assert.isFalse(TestUtil.hasProperty(results[1].payload(), 'name'));
  });

  it('dropTableColumn_Bundled', async () => {
    builder1.setPragma({enableBundledMode: true});
    builder2.setPragma({enableBundledMode: true});
    let db: IndexedDB | null = new IndexedDB(
      builder1.getGlobal(),
      builder1.getSchema()
    );
    const rawDb = await db.init();
    await prepareBundledTxForTableA(rawDb);
    (db as IndexedDB).close();
    db = null;
    db = new IndexedDB(builder2.getGlobal(), builder2.getSchema());
    const newDb = await db.init(upgradeDropTableColumn);
    const results = await dumpTableBundled(newDb, 'tableA_');
    assert.equal(2, results.length);
    const newRow = Row.deserialize(results[0].getPayload()[0] as RawRow);
    const newRow2 = Row.deserialize(results[1].getPayload()[MAGIC] as RawRow);
    assert.isFalse(TestUtil.hasProperty(newRow, 'name'));
    assert.isFalse(TestUtil.hasProperty(newRow2, 'name'));
  });

  function upgradeRenameTableColumn(dbInterface: RawBackStore): Promise<void> {
    const db = dbInterface as IndexedDBRawBackStore;
    assert.equal(1, db.getVersion());
    return db.renameTableColumn('tableA_', 'name', 'username');
  }

  it('renameTableColumn', async () => {
    let db: IndexedDB | null = new IndexedDB(
      builder1.getGlobal(),
      builder1.getSchema()
    );
    const rawDb = await db.init();
    await prepareTxForTableA(rawDb);
    (db as IndexedDB).close();
    db = null;
    db = new IndexedDB(builder2.getGlobal(), builder2.getSchema());
    const newDb = await db.init(upgradeRenameTableColumn);
    const results = await dumpTable(newDb, 'tableA_');
    assert.equal(2, results.length);
    assert.isFalse(TestUtil.hasProperty(results[0].payload(), 'name'));
    assert.isFalse(TestUtil.hasProperty(results[1].payload(), 'name'));
    assert.equal('world', results[0].payload()['username']);
    assert.equal('world2', results[1].payload()['username']);
  });

  it('renameTableColumn_Bundled', async () => {
    builder1.setPragma({enableBundledMode: true});
    builder2.setPragma({enableBundledMode: true});
    let db: IndexedDB | null = new IndexedDB(
      builder1.getGlobal(),
      builder1.getSchema()
    );
    const rawDb = await db.init();
    await prepareBundledTxForTableA(rawDb);
    (db as IndexedDB).close();
    db = null;
    db = new IndexedDB(builder2.getGlobal(), builder2.getSchema());
    const newDb = await db.init(upgradeRenameTableColumn);
    const results = await dumpTableBundled(newDb, 'tableA_');
    assert.equal(2, results.length);
    const newRow = Row.deserialize(results[0].getPayload()[0] as RawRow);
    const newRow2 = Row.deserialize(results[1].getPayload()[MAGIC] as RawRow);
    assert.isFalse(TestUtil.hasProperty(newRow, 'name'));
    assert.isFalse(TestUtil.hasProperty(newRow2, 'name'));
    assert.equal('world', newRow.payload()['username']);
    assert.equal('world2', newRow2.payload()['username']);
  });

  function upgradeDropTable(dbInterface: RawBackStore): Promise<void> {
    const db = dbInterface as IndexedDBRawBackStore;
    assert.equal(1, db.getVersion());
    return db.dropTable('tableB_');
  }

  it('dropTable', async () => {
    let db: IndexedDB | null = new IndexedDB(
      builder1.getGlobal(),
      builder1.getSchema()
    );
    const rawDb = await db.init();
    assert.equal(2, rawDb.objectStoreNames.length);
    (db as IndexedDB).close();
    db = null;
    db = new IndexedDB(builder2.getGlobal(), builder2.getSchema());
    const rawDb2 = await db.init(upgradeDropTable);
    assert.equal(1, rawDb2.objectStoreNames.length);
  });

  async function upgradeDumping(dbInterface: RawBackStore): Promise<void> {
    const db = dbInterface as IndexedDBRawBackStore;
    assert.equal(1, db.getVersion());
    const res = await db.dump();
    const results = res as PayloadType;
    assert.sameDeepMembers(
      [CONTENTS, CONTENTS2],
      results['tableA_'] as unknown[]
    );
    assert.sameDeepMembers([], results['tableB_'] as unknown[]);
  }

  it('dump', async () => {
    let db: IndexedDB | null = new IndexedDB(
      builder1.getGlobal(),
      builder1.getSchema()
    );
    const rawDb = await db.init();
    await prepareTxForTableA(rawDb);
    (db as IndexedDB).close();
    db = null;
    db = new IndexedDB(builder2.getGlobal(), builder2.getSchema());
    await db.init(upgradeDumping);
  });

  it('dump_Bundled', async () => {
    builder1.setPragma({enableBundledMode: true});
    builder2.setPragma({enableBundledMode: true});
    let db: IndexedDB | null = new IndexedDB(
      builder1.getGlobal(),
      builder1.getSchema()
    );
    const rawDb = await db.init();
    await prepareBundledTxForTableA(rawDb);
    (db as IndexedDB).close();
    db = null;
    db = new IndexedDB(builder2.getGlobal(), builder2.getSchema());
    await db.init(upgradeDumping);
  });
});
