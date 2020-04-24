/**
 * Copyright 2016 The Lovefield Project Authors. All Rights Reserved.
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

import { DiffCalculator } from '../../lib/base/diff_calculator';
import { Type } from '../../lib/base/enum';
import { Global } from '../../lib/base/global';
import { Resolver } from '../../lib/base/resolver';
import { Row } from '../../lib/base/row';
import { Relation } from '../../lib/proc/relation';
import { SelectBuilder } from '../../lib/query/select_builder';
import { SelectContext } from '../../lib/query/select_context';
import { Builder } from '../../lib/schema/builder';
import { DatabaseSchema } from '../../lib/schema/database_schema';
import { MockEnv } from '../../testing/mock_env';
import { getMockSchemaBuilder } from '../../testing/mock_schema_builder';
import { ChangeRecord } from '../../lib/base/change_record';

const assert = chai.assert;

describe('DiffCalculator', () => {
  let schema: DatabaseSchema;

  beforeEach(() => {
    schema = getMockSchemaBuilder().getSchema();
    const env = new MockEnv(schema);
    return env.init();
  });

  function generateSampleRows(): Row[] {
    const table = schema.table('tableA');
    const rowCount = 10;
    const rows = new Array(rowCount);
    for (let i = 0; i < rowCount; i++) {
      rows[i] = table.createRow({
        id: `dummyId${i}`,
        name: 'dummyName',
      });
    }

    return rows;
  }

  // Tests the case where the observed query explicitly names the columns to be
  // projected.
  it('diffCalculation_ExplicitColumns', () => {
    const table = schema.table('tableA');
    const builder = new SelectBuilder(Global.get(), [
      table.col('id'),
      table.col('name'),
    ]);
    builder.from(table);
    const query = builder.getQuery();
    checkDiffCalculation(query, 'ExplicitColumns');
  });

  // Tests the case where the observed query implicitly projects all columns.
  it('DiffCalculation_ImplicitColumns', () => {
    const table = schema.table('tableA');
    const builder = new SelectBuilder(Global.get(), []);
    builder.from(table);
    const query = builder.getQuery();
    checkDiffCalculation(query, 'ImplicitColumns');
  });

  // Checks that change notifications are sent as expected when observed results
  // are mutated in continuous ways.
  function checkDiffCalculation(
    query: SelectContext,
    description: string
  ): void {
    const promiseResolver = new Resolver<void>();

    const callback = (currentVersion: number, changes: ChangeRecord[]) => {
      switch (currentVersion) {
        case 0:
          assert.equal(1, changes.length);
          assert.equal(1, changes[0].object.length);
          assert.equal(1, changes[0]['addedCount']);
          assert.equal(0, changes[0].index);
          break;
        case 1:
          assert.equal(2, changes.length);
          assert.equal(3, changes[0].object.length);
          assert.equal(1, changes[0]['addedCount']);
          assert.equal(0, changes[0].index);
          assert.equal(1, changes[1]['addedCount']);
          assert.equal(2, changes[1].index);
          break;
        case 2:
          assert.equal(4, changes.length);
          assert.equal(7, changes[0].object.length);

          changes.forEach(change => assert.equal(1, change['addedCount']));

          assert.equal(0, changes[0].index); // row1
          assert.equal(3, changes[1].index); // row4
          assert.equal(5, changes[2].index); // row6
          assert.equal(6, changes[3].index); // row7
          break;
        case 3:
          assert.equal(1, changes.length);
          assert.equal(6, changes[0].object.length);
          assert.equal(0, changes[0]['addedCount']);
          assert.equal(1, changes[0].removed.length);
          break;
        case 4:
          assert.equal(2, changes.length);
          assert.equal(6, changes[0].object.length);

          // Checking row5 removed.
          assert.equal(0, changes[0]['addedCount']);
          assert.equal(1, changes[0].removed.length);
          assert.equal(3, changes[0].index);

          // Checking row8 added.
          assert.equal(1, changes[1]['addedCount']);
          assert.equal(0, changes[1].removed.length);
          assert.equal(5, changes[1].index);
          break;
        case 5:
          assert.equal(4, changes[0].object.length);

          // Checking row6 removed.
          assert.equal(0, changes[0]['addedCount']);
          assert.equal(1, changes[0].removed.length);
          assert.equal(3, changes[0].index);

          // Checking row8 removed.
          assert.equal(0, changes[1]['addedCount']);
          assert.equal(1, changes[1].removed.length);
          assert.equal(5, changes[1].index);
          break;
        case 6:
          // Checking all removed.
          assert.equal(0, changes[0].object.length);

          changes.forEach((change, i) => {
            assert.equal(0, change['addedCount']);
            assert.equal(1, change.removed.length);
            assert.equal(i, change.index);
          });

          promiseResolver.resolve();
          break;

        default:
          promiseResolver.reject();
          break;
      }
      return promiseResolver.promise;
    };

    const rows = generateSampleRows();
    const rowsPerVersion = [
      // Version0: row3 added.
      [rows[3]],
      // Version1: row2, row5 added.
      [rows[2], rows[3], rows[5]],
      // Version2: row1, row4, row6, row7 added.
      [rows[1], rows[2], rows[3], rows[4], rows[5], rows[6], rows[7]],
      // Version3: row2 removed.
      [rows[1], rows[3], rows[4], rows[5], rows[6], rows[7]],
      // Version4: row5 removed, row8 added.
      [rows[1], rows[3], rows[4], rows[6], rows[7], rows[8]],
      // Version5: row6, row8 removed.
      [rows[1], rows[3], rows[4], rows[7]],
      // Version6: row1, row3, row4, row7 removed.
      [],
    ];

    performMutations(rowsPerVersion, query, callback);
  }

  // Performs a series of mutations.
  // |rowsPerVersion| is the query results for each version to be simulated.
  // |callback| is the function to be called every time a change is applied.
  function performMutations(
    rowsPerVersion: Row[][],
    query: SelectContext,
    callback: (version: number, change: ChangeRecord[]) => void
  ): void {
    let currentVersion = -1;
    const observable: object[] = [];
    let oldResults = Relation.createEmpty();
    const diffCalculator = new DiffCalculator(query, observable);

    // Updates the observed results to the next version, which should trigger
    // the observer callback.
    const updateResultsToNextVersion = () => {
      currentVersion++;
      const table = schema.table('tableA');
      const newResults = Relation.fromRows(rowsPerVersion[currentVersion], [
        table.getName(),
      ]);
      const changeRecords = diffCalculator.applyDiff(oldResults, newResults);
      oldResults = newResults;

      callback(currentVersion, changeRecords);
      if (currentVersion < rowsPerVersion.length - 1) {
        updateResultsToNextVersion();
      }
    };

    updateResultsToNextVersion();
  }

  // Tests the case where the observed table has an Type.OBJECT column.
  it('DiffCalculation_ObjectColumn', () => {
    const schemaBuilder = new Builder('object_diff', 1);
    schemaBuilder
      .createTable('myTable')
      .addColumn('id', Type.STRING)
      .addColumn('obj', Type.OBJECT)
      .addColumn('arraybuffer', Type.ARRAY_BUFFER)
      .addPrimaryKey(['id']);

    const schema2 = schemaBuilder.getSchema();
    const myTable = schema2.table('myTable');

    const builder = new SelectBuilder(Global.get(), []);
    builder.from(myTable);
    const diffCalculator = new DiffCalculator(builder.getQuery(), []);

    // Simulate insertion.
    const rowBefore = myTable.createRow({
      arraybuffer: null,
      id: 'dummyId',
      obj: { hello: 'world' },
    });

    let oldResults = Relation.createEmpty();
    let newResults = Relation.fromRows([rowBefore], [myTable.getName()]);

    let changes = diffCalculator.applyDiff(oldResults, newResults);
    assert.equal(1, changes.length);
    assert.equal(1, changes[0].object.length);
    assert.equal(1, changes[0]['addedCount']);
    assert.equal(0, changes[0].index);

    // Simulate a change in the 'obj' field.
    const rowAfter = myTable.createRow({
      arraybuffer: null,
      id: 'dummyId',
      obj: { hello: 'amigo' },
    });
    rowAfter.assignRowId(rowBefore.id());

    oldResults = newResults;
    newResults = Relation.fromRows([rowAfter], [myTable.getName()]);

    changes = diffCalculator.applyDiff(oldResults, newResults);
    assert.equal(2, changes.length);

    assert.equal(1, changes[0].object.length);
    assert.equal(0, changes[0]['addedCount']);
    assert.equal(1, changes[0].removed.length);
    assert.equal(0, changes[0].index);

    assert.equal(1, changes[1].object.length);
    assert.equal(1, changes[1]['addedCount']);
    assert.equal(0, changes[1].removed.length);
    assert.equal(0, changes[1].index);
  });
});
