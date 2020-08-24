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
import {IndexedDBRawBackStore} from '../../lib/backstore/indexed_db_raw_back_store';
import {Page} from '../../lib/backstore/page';
import {RawBackStore} from '../../lib/backstore/raw_back_store';
import {Capability} from '../../lib/base/capability';
import {Type} from '../../lib/base/enum';
import {RawRow, Row, PayloadType} from '../../lib/base/row';
import {Builder} from '../../lib/schema/builder';
import {schema} from '../../lib/schema/schema';
import {TestUtil} from '../../testing/test_util';

const assert = chai.assert;
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
  it('newDBInstance', () => {
    const onUpgrade = sinon.spy((rawDb: RawBackStore) => {
      assert.equal(0, rawDb.getVersion());
      return Promise.resolve();
    });

    const db = new IndexedDB(builder1.getGlobal(), builder1.getSchema());
    return db.init(onUpgrade).then(() => {
      assert.equal(1, onUpgrade.callCount);
    });
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
      req.onsuccess = ev => {
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
      tx.oncomplete = ev => {
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
      tx.oncomplete = ev => {
        resolve();
      };
      tx.onabort = reject;
    });
  }

  it('addTableColumn', () => {
    let db: IndexedDB | null = new IndexedDB(
      builder1.getGlobal(),
      builder1.getSchema()
    );
    const date = new Date();

    return db
      .init()
      .then(rawDb => {
        return prepareTxForTableA(rawDb);
      })
      .then(() => {
        (db as IndexedDB).close();
        db = null;
        db = new IndexedDB(builder2.getGlobal(), builder2.getSchema());
        return db.init((raw: RawBackStore) => upgradeAddTableColumn(date, raw));
      })
      .then(newDb => {
        return dumpTable(newDb, 'tableA_');
      })
      .then(results => {
        assert.equal(2, results.length);
        assert.equal(date.getTime(), results[0].payload()['dob']);
        assert.equal(date.getTime(), results[1].payload()['dob']);
      });
  });

  it('addTableColumn_Bundled', () => {
    builder1.setPragma({enableBundledMode: true});
    builder2.setPragma({enableBundledMode: true});
    let db: IndexedDB | null = new IndexedDB(
      builder1.getGlobal(),
      builder1.getSchema()
    );
    const date = new Date();

    return db
      .init()
      .then(rawDb => {
        return prepareBundledTxForTableA(rawDb);
      })
      .then(() => {
        (db as IndexedDB).close();
        db = null;
        db = new IndexedDB(builder2.getGlobal(), builder2.getSchema());
        return db.init((raw: RawBackStore) => upgradeAddTableColumn(date, raw));
      })
      .then(newDb => {
        return dumpTableBundled(newDb, 'tableA_');
      })
      .then(results => {
        assert.equal(2, results.length);
        const newRow = Row.deserialize(results[0].getPayload()[0] as RawRow);
        const newRow2 = Row.deserialize(
          results[1].getPayload()[MAGIC] as RawRow
        );
        assert.equal(date.getTime(), newRow.payload()['dob']);
        assert.equal(date.getTime(), newRow2.payload()['dob']);
      });
  });

  function upgradeDropTableColumn(dbInterface: RawBackStore): Promise<void> {
    const db = dbInterface as IndexedDBRawBackStore;
    assert.equal(1, db.getVersion());
    return db.dropTableColumn('tableA_', 'name');
  }

  it('dropTableColumn', () => {
    let db: IndexedDB | null = new IndexedDB(
      builder1.getGlobal(),
      builder1.getSchema()
    );
    return db
      .init()
      .then(rawDb => {
        return prepareTxForTableA(rawDb);
      })
      .then(() => {
        (db as IndexedDB).close();
        db = null;
        db = new IndexedDB(builder2.getGlobal(), builder2.getSchema());
        return db.init(upgradeDropTableColumn);
      })
      .then(newDb => {
        return dumpTable(newDb, 'tableA_');
      })
      .then(results => {
        assert.equal(2, results.length);
        assert.isFalse(TestUtil.hasProperty(results[0].payload(), 'name'));
        assert.isFalse(TestUtil.hasProperty(results[1].payload(), 'name'));
      });
  });

  it('dropTableColumn_Bundled', () => {
    builder1.setPragma({enableBundledMode: true});
    builder2.setPragma({enableBundledMode: true});
    let db: IndexedDB | null = new IndexedDB(
      builder1.getGlobal(),
      builder1.getSchema()
    );
    return db
      .init()
      .then(rawDb => {
        return prepareBundledTxForTableA(rawDb);
      })
      .then(() => {
        (db as IndexedDB).close();
        db = null;
        db = new IndexedDB(builder2.getGlobal(), builder2.getSchema());
        return db.init(upgradeDropTableColumn);
      })
      .then(newDb => {
        return dumpTableBundled(newDb, 'tableA_');
      })
      .then(results => {
        assert.equal(2, results.length);
        const newRow = Row.deserialize(results[0].getPayload()[0] as RawRow);
        const newRow2 = Row.deserialize(
          results[1].getPayload()[MAGIC] as RawRow
        );
        assert.isFalse(TestUtil.hasProperty(newRow, 'name'));
        assert.isFalse(TestUtil.hasProperty(newRow2, 'name'));
      });
  });

  function upgradeRenameTableColumn(dbInterface: RawBackStore): Promise<void> {
    const db = dbInterface as IndexedDBRawBackStore;
    assert.equal(1, db.getVersion());
    return db.renameTableColumn('tableA_', 'name', 'username');
  }

  it('renameTableColumn', () => {
    let db: IndexedDB | null = new IndexedDB(
      builder1.getGlobal(),
      builder1.getSchema()
    );
    return db
      .init()
      .then(rawDb => {
        return prepareTxForTableA(rawDb);
      })
      .then(() => {
        (db as IndexedDB).close();
        db = null;
        db = new IndexedDB(builder2.getGlobal(), builder2.getSchema());
        return db.init(upgradeRenameTableColumn);
      })
      .then(newDb => {
        return dumpTable(newDb, 'tableA_');
      })
      .then(results => {
        assert.equal(2, results.length);
        assert.isFalse(TestUtil.hasProperty(results[0].payload(), 'name'));
        assert.isFalse(TestUtil.hasProperty(results[1].payload(), 'name'));
        assert.equal('world', results[0].payload()['username']);
        assert.equal('world2', results[1].payload()['username']);
      });
  });

  it('renameTableColumn_Bundled', () => {
    builder1.setPragma({enableBundledMode: true});
    builder2.setPragma({enableBundledMode: true});
    let db: IndexedDB | null = new IndexedDB(
      builder1.getGlobal(),
      builder1.getSchema()
    );
    return db
      .init()
      .then(rawDb => {
        return prepareBundledTxForTableA(rawDb);
      })
      .then(() => {
        (db as IndexedDB).close();
        db = null;
        db = new IndexedDB(builder2.getGlobal(), builder2.getSchema());
        return db.init(upgradeRenameTableColumn);
      })
      .then(newDb => {
        return dumpTableBundled(newDb, 'tableA_');
      })
      .then(results => {
        assert.equal(2, results.length);
        const newRow = Row.deserialize(results[0].getPayload()[0] as RawRow);
        const newRow2 = Row.deserialize(
          results[1].getPayload()[MAGIC] as RawRow
        );
        assert.isFalse(TestUtil.hasProperty(newRow, 'name'));
        assert.isFalse(TestUtil.hasProperty(newRow2, 'name'));
        assert.equal('world', newRow.payload()['username']);
        assert.equal('world2', newRow2.payload()['username']);
      });
  });

  function upgradeDropTable(dbInterface: RawBackStore): Promise<void> {
    const db = dbInterface as IndexedDBRawBackStore;
    assert.equal(1, db.getVersion());
    return db.dropTable('tableB_');
  }

  it('dropTable', () => {
    let db: IndexedDB | null = new IndexedDB(
      builder1.getGlobal(),
      builder1.getSchema()
    );
    return db
      .init()
      .then(rawDb => {
        assert.equal(2, rawDb.objectStoreNames.length);
        (db as IndexedDB).close();
        db = null;
        db = new IndexedDB(builder2.getGlobal(), builder2.getSchema());
        return db.init(upgradeDropTable);
      })
      .then(rawDb => {
        assert.equal(1, rawDb.objectStoreNames.length);
      });
  });

  function upgradeDumping(dbInterface: RawBackStore): Promise<void> {
    const db = dbInterface as IndexedDBRawBackStore;
    assert.equal(1, db.getVersion());
    return db.dump().then(res => {
      const results = res as PayloadType;
      assert.sameDeepMembers(
        [CONTENTS, CONTENTS2],
        results['tableA_'] as unknown[]
      );
      assert.sameDeepMembers([], results['tableB_'] as unknown[]);
    });
  }

  it('dump', () => {
    let db: IndexedDB | null = new IndexedDB(
      builder1.getGlobal(),
      builder1.getSchema()
    );
    return db
      .init()
      .then(rawDb => {
        return prepareTxForTableA(rawDb);
      })
      .then(() => {
        (db as IndexedDB).close();
        db = null;
        db = new IndexedDB(builder2.getGlobal(), builder2.getSchema());
        return db.init(upgradeDumping);
      });
  });

  it('dump_Bundled', () => {
    builder1.setPragma({enableBundledMode: true});
    builder2.setPragma({enableBundledMode: true});
    let db: IndexedDB | null = new IndexedDB(
      builder1.getGlobal(),
      builder1.getSchema()
    );
    return db
      .init()
      .then(rawDb => {
        return prepareBundledTxForTableA(rawDb);
      })
      .then(() => {
        (db as IndexedDB).close();
        db = null;
        db = new IndexedDB(builder2.getGlobal(), builder2.getSchema());
        return db.init(upgradeDumping);
      });
  });
});
