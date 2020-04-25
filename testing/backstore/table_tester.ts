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
import {Resolver} from '../../lib/base/resolver';
import {Row} from '../../lib/base/row';
import {RuntimeTable} from '../../lib/base/runtime_table';

const assert = chai.assert;

export class TableTester {
  constructor(readonly creator: () => RuntimeTable) {}

  run(): Promise<void> {
    return Promise.all([
      this.testGet_NonExisting(),
      this.testGet_AllValues(),
      this.testPut(),
      this.testRemove(),
    ]).then(() => Promise.resolve());
  }

  private testGet_NonExisting(): Promise<void> {
    const resolver = new Resolver<void>();
    const table = this.creator();
    const nonExistingRowId = 10;
    table.get([nonExistingRowId]).then(
      results => {
        assert.sameMembers([], results);
        resolver.resolve();
      },
      () => {
        assert.isTrue(false, 'Unknown error testGet_NonExisting');
        resolver.reject();
      }
    );
    return resolver.promise;
  }

  private testGet_AllValues(): Promise<void> {
    const rowCount = 10;
    const rows = [];
    for (let i = 0; i < rowCount; i++) {
      rows.push(Row.create());
    }

    const resolver = new Resolver<void>();
    const table = this.creator();
    table
      .put(rows)
      .then(() => table.get([]))
      .then(
        results => {
          assert.equal(rowCount, results.length);
          resolver.resolve();
        },
        () => {
          assert.isTrue(false, 'Unknown error testGet_AllValues');
          resolver.reject();
        }
      );

    return resolver.promise;
  }

  private testPut(): Promise<void> {
    const rows: Row[] = [];
    const rowIds: number[] = [];

    for (let i = 0; i < 10; i++) {
      const row = Row.create();
      rows.push(row);
      rowIds.push(row.id());
    }

    const resolver = new Resolver<void>();
    const table = this.creator();
    table
      .put(rows)
      .then(() => table.get(rowIds))
      .then(
        results => {
          const resultRowIds = results.map(row => row.id());
          assert.sameOrderedMembers(rowIds, resultRowIds);
          resolver.resolve();
        },
        () => {
          assert.isTrue(false, 'Unknown error testPut');
          resolver.reject();
        }
      );

    return resolver.promise;
  }

  private testRemove(): Promise<void> {
    const rows: Row[] = [];
    const rowIdsToDelete: number[] = [];

    for (let i = 0; i < 10; i++) {
      const row = Row.create();
      rows.push(row);
      rowIdsToDelete.push(row.id());
    }

    for (let j = 0; j < 5; j++) {
      const row = Row.create();
      rows.push(row);
    }

    const resolver = new Resolver<void>();
    const table = this.creator();
    table
      .put(rows)
      .then(() => table.get([]))
      .then(results => {
        assert.equal(rows.length, results.length);
        return table.remove(rowIdsToDelete);
      })
      .then(() => table.get([]))
      .then(
        results => {
          assert.equal(rows.length - rowIdsToDelete.length, results.length);
          results.forEach(row => {
            assert.isTrue(rowIdsToDelete.indexOf(row.id()) === -1);
          });
          resolver.resolve();
        },
        () => {
          assert.isTrue(false, 'Unknown error testRemove');
          resolver.reject();
        }
      );

    return resolver.promise;
  }
}
