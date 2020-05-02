/**
 * Copyright 2020 The Lovefield Project Authors. All Rights Reserved.
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

import * as lf from './node_modules/lovefield-ts/dist/es6/lf.js';
import {TestCase} from './test_case.js';

export class ScenarioBenchmark {
  constructor() {
    this.REPETITIONS = 1000;
  }

  async init() {
    const schemaBuilder = lf.schema.create('scenario0', 1);
    schemaBuilder.createTable('Brand').
        addColumn('rowId', lf.Type.INTEGER).
        addColumn('brandId', lf.Type.INTEGER).
        addPrimaryKey(['rowId']).
        addIndex('idxBrandId', ['brandId']);
    this.db = await schemaBuilder.connect({
      storeType: lf.DataStoreType.MEMORY,
    });
    this.brand = this.db.getSchema().table('Brand');
  }

  async tearDown() {
    return this.db.delete().from(this.brand).exec();
  }

  createRows() {
    const rows = [];
    for (let i = 0; i < this.REPETITIONS; ++i) {
      rows.push(this.brand.createRow({
        rowId: i,
        brandId: i,
      }));
    }
    return rows;
  }

  async insertTxAttach() {
    const tx = this.db.createTransaction();
    const rows = this.createRows();
    const fn = () => {
      if (rows.length == 0) {
        return Promise.resolve();
      }
      const row = rows.shift();
      return tx.attach(
          this.db.insert().into(this.brand).values([row])).then(fn);
    };

    return tx.begin([this.brand]).then(() => {
      return fn();
    }).then(() => {
      return tx.commit();
    });
  }

  async select() {
    const queries = [];
    for (let i = 0; i < this.REPETITIONS; ++i) {
      queries.push(this.db
          .select(this.brand['rowId'])
          .from(this.brand)
          .where(this.brand['brandId'].eq(i))
          .exec(),
      );
    }
    return Promise.all(queries);
  }

  async selectBinding() {
    const q = this.db
        .select(this.brand['rowId'])
        .from(this.brand)
        .where(this.brand['brandId'].eq(lf.bind(0)));
    const queries = [];
    for (let i = 0; i < this.REPETITIONS_; ++i) {
      queries.push(q.bind([i]).exec());
    }
    return Promise.all(queries);
  }

  getTestCases() {
    return [
      new TestCase(
          'Insert via Tx Attach', this.insertTxAttach.bind(this)),
      new TestCase('Select',
          this.select.bind(this)),
      new TestCase(
          'Select Binding', this.selectBinding.bind(this)),
      new TestCase(
          'Teardown', this.tearDown.bind(this), undefined, true),
    ];
  }
}
