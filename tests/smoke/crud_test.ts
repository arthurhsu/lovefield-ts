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

import {Page} from '../../lib/backstore/page';
import {Capability} from '../../lib/base/capability';
import {DatabaseConnection} from '../../lib/base/database_connection';
import {DataStoreType, TransactionType} from '../../lib/base/enum';
import {Row} from '../../lib/base/row';
import {RuntimeDatabase} from '../../lib/proc/runtime_database';
import {Builder} from '../../lib/schema/builder';
import {getHrDbSchemaBuilder} from '../../testing/hr_schema/hr_schema_builder';
import {SmokeTester} from '../../testing/smoke_tester';
import {TestUtil} from '../../testing/test_util';

const assert = chai.assert;

type Connector = () => Promise<DatabaseConnection>;

describe('CRUD', () => {
  let connector: (builder: Builder) => Connector;
  let tester: SmokeTester;
  let db: DatabaseConnection;

  const getGlobal = (conn: DatabaseConnection) => {
    return (conn as RuntimeDatabase).getGlobal();
  };

  beforeEach(async () => {
    db = await connector(getHrDbSchemaBuilder())();
    tester = new SmokeTester(getGlobal(db), db);
  });

  afterEach(async () => {
    await tester.clearDb();
    db.close();
  });

  function getDynamicSchemaConnector(builder: Builder): Connector {
    const options = Capability.get().indexedDb ?
        undefined :
        {storeType: DataStoreType.MEMORY};
    return builder.connect.bind(builder, options);
  }

  function getDynamicSchemaBundledConnector(builder: Builder): Connector {
    builder.setPragma({enableBundledMode: true});
    return builder.connect.bind(builder, undefined);
  }

  const scenarios = new Map<string, (builder: Builder) => Connector>();
  scenarios.set('dynamic', getDynamicSchemaConnector);
  if (Capability.get().indexedDb) {
    scenarios.set('dynamic_bundled', getDynamicSchemaBundledConnector);
  }

  scenarios.forEach((conn, key) => {
    connector = conn;

    it(`${key}_CRUD`, () => {
      return tester.testCRUD();
    });

    it(`${key}_OverlappingScope_MultipleInserts`, () => {
      return tester.testOverlappingScope_MultipleInserts();
    });

    it(`${key}_Transaction`, () => {
      return tester.testTransaction();
    });

    it(`${key}_Serialization`, () => {
      const dummy = db.getSchema().table('DummyTable');
      const row = dummy.createRow({
        arraybuffer: null,
        boolean: false,
        integer: 1,
        number: 2,
        string: 'A',
        string2: 'B',
      });

      const expected = {
        arraybuffer: null,
        boolean: false,
        datetime: null,
        integer: 1,
        number: 2,
        proto: null,
        string: 'A',
        string2: 'B',
      };
      assert.deepEqual(expected, row.toDbPayload());
      assert.deepEqual(
          expected, dummy.deserializeRow(row.serialize()).payload());

      if (db.getSchema().pragma().enableBundledMode) {
        const page = new Page(1);
        page.setRows([row]);
        const page2 = Page.deserialize(page.serialize()).getPayload();
        assert.deepEqual(expected, page2[row.id()]['value']);
      }
    });

    it(`${key}_ReplaceRow`, async () => {
      // This is a regression test for a bug that lovefield had with IndexedDb
      // backstore. The bug manifested when a single transaction had a delete
      // for an entire table, along with an insert to the same table. The bug
      // was that the insert would not stick, because the remove/put calls were
      // racing and the remove would result in a clear call for the entire
      // object store after the put happened.

      // The test is only relevant for indexedDb.
      if (!Capability.get().indexedDb) {
        return;
      }

      const table = db.getSchema().table('Region');
      const originalRow = table.createRow({id: '1', name: 'North America'});
      const replacementRow =
          table.createRow({id: '2', name: 'Central America'});

      // First insert a single record into a table.
      let tx = db.createTransaction(TransactionType.READ_WRITE);
      const insert = db.insert().into(table).values([originalRow]);
      await tx.exec([insert]);
      // Read the entire table directly from IndexedDb and verify that the
      // original record is in place.
      let results: Row[] = await TestUtil.selectAll(getGlobal(db), table);
      assert.equal(1, results.length);
      assert.deepEqual(originalRow.payload(), results[0].payload());

      // Now execute a transaction that removes the single row and inserts a
      // replacement.
      tx = db.createTransaction(TransactionType.READ_WRITE);
      await tx.exec([
        db.delete().from(table),
        db.insert().into(table).values([replacementRow]),
      ]);

      // Read the entire table, verify that we have a single row present (not
      // zero rows), and that it is the replacement, not the original.
      results = await TestUtil.selectAll(getGlobal(db), table);

      assert.equal(1, results.length);
      assert.deepEqual(replacementRow.payload(), results[0].payload());
    });
  });
});
