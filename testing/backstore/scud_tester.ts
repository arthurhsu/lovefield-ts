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

import {BackStore} from '../../lib/backstore/back_store';
import {TableType, TransactionType} from '../../lib/base/enum';
import {Global} from '../../lib/base/global';
import {Row} from '../../lib/base/row';
import {Service} from '../../lib/base/service';
import {Cache} from '../../lib/cache/cache';
import {Journal} from '../../lib/cache/journal';
import {Database} from '../../lib/schema/database';
import {Table} from '../../lib/schema/table';

export class ScudTester {
  private db: BackStore;
  private global: Global;
  private schema: Database;
  private tableSchema: Table;
  private cache: Cache;
  private reload: null|(() => BackStore);

  constructor(db: BackStore, global: Global, reload?: () => BackStore) {
    this.db = db;
    this.global = global;
    this.schema = global.getService(Service.SCHEMA);
    this.tableSchema = this.schema.tables()[0];
    this.cache = global.getService(Service.CACHE);
    this.reload = reload || null;
  }

  public run(): Promise<void> {
    const CONTENTS = {id: 'hello', name: 'world'};
    const CONTENTS2 = {id: 'hello2', name: 'world2'};

    const row1 = Row.create(CONTENTS);
    const row2 = Row.create(CONTENTS);
    const row3 = new Row(row1.id(), CONTENTS2);

    return this.db.init()
        .then(() => this.insert([row1]))
        .then(() => this.assertOnlyRows([row1]))
        .then((results) => {
          // Insert row2, update row1.
          return this.insert([row2, row3]);
        })
        .then(() => this.assertOnlyRows([row3, row2]))
        .then((results) => {
          // Update cache, otherwise the bundled operation will fail.
          this.cache.setMany(this.tableSchema.getName(), [row2, row3]);

          // Remove row1.
          return this.remove([row1.id()]);
        })
        .then(() => this.assertOnlyRows([row2]))
        .then((results) => {
          // Remove all.
          return this.removeAll();
        })
        .then(() => this.assertOnlyRows([]));
  }

  private insert(rows: Row[]): Promise<void> {
    const tx = this.db.createTx(
        TransactionType.READ_WRITE, [this.tableSchema],
        new Journal(this.global, new Set<Table>([this.tableSchema])));
    const store = tx.getTable(
        this.tableSchema.getName(),
        this.tableSchema.deserializeRow.bind(this.tableSchema), TableType.DATA);

    store.put(rows);
    return tx.commit();
  }

  private remove(rowIds: number[]): Promise<void> {
    const tx = this.db.createTx(
        TransactionType.READ_WRITE, [this.tableSchema],
        new Journal(this.global, new Set<Table>([this.tableSchema])));
    const store = tx.getTable(
        this.tableSchema.getName(),
        this.tableSchema.deserializeRow.bind(this.tableSchema), TableType.DATA);

    store.remove(rowIds);
    return tx.commit();
  }

  private removeAll(): Promise<void> {
    return this.remove([]);
  }

  private select(rowIds: number[]): Promise<Row[]> {
    const tx = this.db.createTx(TransactionType.READ_ONLY, [this.tableSchema]);
    const store = tx.getTable(
        this.tableSchema.getName(),
        this.tableSchema.deserializeRow.bind(this.tableSchema), TableType.DATA);

    const promise = store.get(rowIds);
    tx.commit();
    return promise;
  }

  private selectAll(): Promise<Row[]> {
    return this.select([]);
  }

  // Asserts that only the given rows exists in the database.
  private assertOnlyRows(rows: Row[]): Promise<void> {
    if (this.reload !== null) {
      this.db = this.reload();
    }
    return this.selectAll().then((results) => {
      chai.assert.equal(rows.length, results.length);
      rows.forEach((row, index) => {
        const retrievedRow = results[index];
        chai.assert.equal(row.id(), retrievedRow.id());
        chai.assert.deepEqual(row.payload(), retrievedRow.payload());
      });
    });
  }
}
