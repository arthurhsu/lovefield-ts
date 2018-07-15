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

import {BackStore} from '../lib/backstore/back_store';
import {TransactionStats} from '../lib/backstore/transaction_stats';
import {DatabaseConnection} from '../lib/base/database_connection';
import {ErrorCode, TransactionType} from '../lib/base/enum';
import {Global} from '../lib/base/global';
import {TableType} from '../lib/base/private_enum';
import {Row} from '../lib/base/row';
import {Service} from '../lib/base/service';
import {Table} from '../lib/schema/table';

const assert = chai.assert;

// Smoke test for the most basic DB operations, Create, Read, Update, Delete.
export class SmokeTester {
  private db: DatabaseConnection;
  private backStore: BackStore;
  private r: Table;

  constructor(global: Global, db: DatabaseConnection) {
    this.db = db;
    this.backStore = global.getService(Service.BACK_STORE);
    this.r = db.getSchema().table('Region');
  }

  public clearDb(): Promise<any> {
    const tables = this.db.getSchema().tables();
    const deletePromises = tables.map((table) => {
      return this.db.delete().from(table).exec();
    }, this);

    return Promise.all(deletePromises);
  }

  // Smoke test for the most basic DB operations, Create, Read, Update, Delete.
  public async testCRUD(): Promise<void> {
    const regionRows = this.generateSampleRows();
    const db = this.db;
    const r = this.r;

    // Inserts 5 records to the database.
    const insertFn = () => {
      return db.insert().into(r).values(regionRows).exec();
    };

    // Selects all records from the database.
    const selectAllFn = () => {
      return db.select().from(r).exec();
    };

    // Selects some records from the database.
    const selectFn = (ids: string[]) => {
      return db.select().from(r).where(r['id'].in(ids)).exec();
    };

    // Updates the 'name' field of two specific rows.
    const updateFn = () => {
      return db.update(r)
          .where(r['id'].in(['1', '2']))
          .set(r['name'], 'Mars')
          .exec();
    };

    // Updates two specific records by replacing the entire row.
    const replaceFn = () => {
      const regionRow0 = r.createRow({id: '1', name: 'Venus'});
      const regionRow1 = r.createRow({id: '2', name: 'Zeus'});

      return db.insertOrReplace()
          .into(r)
          .values([regionRow0, regionRow1])
          .exec();
    };

    // Deletes two specific records from the database.
    const deleteFn = () => {
      return db.delete().from(r).where(r['id'].in(['4', '5'])).exec();
    };

    await insertFn();
    let results: object[] = await selectFn(['1', '5']);
    assert.equal(2, results.length);
    assert.deepEqual({id: '1', name: 'North America'}, results[0]);
    assert.deepEqual({id: '5', name: 'Southern Europe'}, results[1]);

    results = await selectAllFn();
    assert.equal(regionRows.length, results.length);

    await updateFn();
    results = await selectFn(['1', '2']);
    assert.deepEqual({id: '1', name: 'Mars'}, results[0]);
    assert.deepEqual({id: '2', name: 'Mars'}, results[1]);

    await replaceFn();
    results = await selectFn(['1', '2']);
    assert.deepEqual({id: '1', name: 'Venus'}, results[0]);
    assert.deepEqual({id: '2', name: 'Zeus'}, results[1]);

    await deleteFn();
    results = await selectAllFn();
    assert.equal(regionRows.length - 2, results.length);
  }

  // Tests that queries that have overlapping scope are processed in a
  // serialized manner.
  public async testOverlappingScope_MultipleInserts(): Promise<void> {
    // TODO(arthurhsu): add a new test case to test failure case.
    const rowCount = 3;
    const rows = this.generateSampleRowsWithSamePrimaryKey(3);
    const db = this.db;
    const r = this.r;

    // Issuing multiple queries back to back (no waiting on the previous query
    // to finish). All rows to be inserted have the same primary key.
    const promises = rows.map((row) => {
      return db.insertOrReplace().into(r).values([row]).exec();
    });

    await Promise.all(promises);
    // The fact that this success callback executes is already a signal that
    // no exception was thrown during update of primary key index, which
    // proves that all insertOrReplace queries where not executed
    // simultaneously, instead the first query inserted the row, and
    // subsequent queries updated it.
    const results = await this.selectAll();

    // Assert that only one record exists in the DB.
    assert.equal(1, results.length);

    const retrievedRow = results[0] as Row;
    // Assert the retrieved row matches the value ordered by the last query.
    assert.equal(
        'Region' + String(rowCount - 1), retrievedRow.payload()['name']);
  }

  // Smoke test for transactions.
  public async testTransaction(): Promise<void> {
    const rows = this.generateSampleRows();
    const r = this.r;
    const db = this.db;
    const tx = db.createTransaction(TransactionType.READ_WRITE);
    const insert1 = db.insert().into(r).values(rows.slice(1));
    const insert2 = db.insert().into(r).values([rows[0]]);

    await tx.exec([insert1, insert2]);

    const results = await this.selectAll();
    assert.equal(5, results.length);
    const stats = tx.stats() as TransactionStats;
    assert.equal(true, stats.success());
    assert.equal(1, stats.changedTableCount());
    assert.equal(5, stats.insertedRowCount());
    assert.equal(0, stats.updatedRowCount());
    assert.equal(0, stats.deletedRowCount());

    // Transaction shall not be able to be executed again after committed.
    const select = db.select().from(r);
    let thrown = false;
    try {
      tx.exec([select]);
    } catch (e) {
      thrown = true;
      // 107: Invalid transaction state transition.
      assert.equal(ErrorCode.INVALID_TX_STATE, e.code);
    }
    assert.isTrue(thrown);

    // Invalid query shall be caught in transaction, too.
    const select2 = db.select();
    const tx2 = db.createTransaction(TransactionType.READ_ONLY);
    thrown = false;
    try {
      await tx2.exec([select2]);
    } catch (e) {
      thrown = true;
      assert.equal(ErrorCode.INVALID_SELECT, e.code);
    }
    assert.isTrue(thrown);
  }

  // Generates sample records to be used for testing.
  private generateSampleRows(): Row[] {
    const r = this.r;
    return [
      r.createRow({id: '1', name: 'North America'}),
      r.createRow({id: '2', name: 'Central America'}),
      r.createRow({id: '3', name: 'South America'}),
      r.createRow({id: '4', name: 'Western Europe'}),
      r.createRow({id: '5', name: 'Southern Europe'}),
    ];
  }

  // Generates sample records such that all generated rows have the same primary
  // key.
  private generateSampleRowsWithSamePrimaryKey(count: number): Row[] {
    const r = this.r;
    const sampleRows = new Array(count);

    for (let i = 0; i < count; i++) {
      sampleRows[i] = r.createRow({id: 1, name: 'Region' + i.toString()});
    }

    return sampleRows;
  }

  // Selects all entries from the database (skips the cache).
  private selectAll(): Promise<Row[]> {
    const r = this.r;
    const tx = this.backStore.createTx(TransactionType.READ_ONLY, [r]);
    return tx.getTable(r.getName(), r.deserializeRow.bind(r), TableType.DATA)
        .get([]);
  }
}
