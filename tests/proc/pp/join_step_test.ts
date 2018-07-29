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

import {Type} from '../../../lib/base/enum';
import {Row} from '../../../lib/base/row';
import {JoinPredicate} from '../../../lib/pred/join_predicate';
import {JoinStep} from '../../../lib/proc/pp/join_step';
import {NoOpStep} from '../../../lib/proc/pp/no_op_step';
import {Relation} from '../../../lib/proc/relation';
import {BaseTable} from '../../../lib/schema/base_table';
import {Builder} from '../../../lib/schema/builder';
import {DatabaseSchema} from '../../../lib/schema/database_schema';
import {MockEnv} from '../../../testing/mock_env';

const assert = chai.assert;

describe('JoinStep', () => {
  let env: MockEnv;
  let ta: BaseTable;
  let tb: BaseTable;
  let tableARows: Row[];
  let tableBRows: Row[];

  beforeEach(async () => {
    env = new MockEnv(getSchema());
    await env.init();
    ta = env.schema.table('TableA');
    tb = env.schema.table('TableB');
    return insertSampleData();
  });

  // Returns the schema to be used for tests in this file.
  function getSchema(): DatabaseSchema {
    const schemaBuilder = new Builder('testschema', 1);
    schemaBuilder.createTable('TableA')
        .addColumn('id', Type.NUMBER)
        .addColumn('name', Type.STRING)
        .addIndex('idx_id', ['id']);

    schemaBuilder.createTable('TableB')
        .addColumn('id', Type.NUMBER)
        .addColumn('name', Type.STRING)
        .addIndex('idx_id', ['id']);
    return schemaBuilder.getSchema();
  }

  // Inserts 3 sample rows to the database, for each table.
  function insertSampleData(): Promise<any> {
    const generateRowsForTable = (table: BaseTable): Row[] => {
      const sampleDataCount = 3;
      const rows = new Array(sampleDataCount);
      for (let i = 0; i < sampleDataCount; i++) {
        rows[i] = table.createRow({
          id: i,
          name: `dummyName${i}`,
        });
      }
      return rows;
    };

    tableARows = generateRowsForTable(ta);
    tableBRows = generateRowsForTable(tb);

    const tx = env.db.createTransaction();
    return tx.exec([
      env.db.insert().into(ta).values(tableARows),
      env.db.insert().into(tb).values(tableBRows),
    ]);
  }

  // Performs index join on the given relation and asserts that the results are
  // correct.
  // |joinPredicate| is the join predicate to used for joining. The left column
  // of the join predicate will be used for performing the join. Callers are
  // responsible to ensure that the relation corresponding to the indexed column
  // (could be either of tableARelation or tableBRelation) includes ALL rows of
  // this table, otherwise index join is invalid and this test would fail.
  function checkIndexJoin(
      tableARelation: Relation, tableBRelation: Relation,
      joinPredicate: JoinPredicate): Promise<void> {
    const noOpStepA = new NoOpStep([tableARelation]);
    const noOpStepB = new NoOpStep([tableBRelation]);
    const joinStep = new JoinStep(env.global, joinPredicate, false);
    joinStep.addChild(noOpStepA);
    joinStep.addChild(noOpStepB);

    // Detecting the expected IDs that should appear in the result.
    const tableAIds = new Set<number>(tableARelation.entries.map(
        (entry) => entry.getField(ta['id']) as number));
    const tableBIds = new Set<number>(tableARelation.entries.map(
        (entry) => entry.getField(tb['id']) as number));
    const expectedIds =
        Array.from(setIntersection(tableAIds, tableBIds).values());

    // Choosing the left predicate column as the indexed column.
    joinStep.markAsIndexJoin(joinPredicate.leftColumn);
    assert.notEqual(-1, joinStep.toString().indexOf('index_nested_loop'));
    return joinStep.exec().then((relations) => {
      assert.equal(1, relations.length);
      assertTableATableBJoin(relations[0], expectedIds);
    });
  }

  function setIntersection(set1: Set<number>, set2: Set<number>): Set<number> {
    const intersection = new Set<number>();
    set1.forEach((value) => {
      if (set2.has(value)) {
        intersection.add(value);
      }
    });
    return intersection;
  }

  // Tests index join for the case where the entire tableA and tabelB contents
  // are joined.
  it('indexJoin_EntireTables', async () => {
    const tableARelation = Relation.fromRows(tableARows, [ta.getName()]);
    const tableBRelation = Relation.fromRows(tableBRows, [tb.getName()]);

    // First calculate index join using the index of TableA's index.
    await checkIndexJoin(tableARelation, tableBRelation, ta['id'].eq(tb['id']));
    // Then calculate index join using the index of TableB's index.
    await checkIndexJoin(tableARelation, tableBRelation, tb['id'].eq(ta['id']));
  });

  // Tests index join for the case where a subset of TableA is joined with the
  // entire TableB (using TableB's index for the join).
  it('indexJoin_PartialTable', async () => {
    const tableARelation =
        Relation.fromRows(tableARows.slice(2), [ta.getName()]);
    const tableBRelation = Relation.fromRows(tableBRows, [tb.getName()]);
    await checkIndexJoin(tableARelation, tableBRelation, tb['id'].eq(ta['id']));
  });

  // Tests index join for the case where an empty relation is joined with the
  // entire TableB (using TableB's index for the join).
  it('indexJoin_EmptyTable', async () => {
    const tableARelation = Relation.fromRows([], [ta.getName()]);
    const tableBRelation = Relation.fromRows(tableBRows, [tb.getName()]);
    await checkIndexJoin(tableARelation, tableBRelation, tb['id'].eq(ta['id']));
  });

  // Asserts that the results of joining rows belonging to TableA and TableB is
  // as expected.
  function assertTableATableBJoin(
      relation: Relation, expectedIds: number[]): void {
    assert.equal(expectedIds.length, relation.entries.length);
    relation.entries.forEach((entry, i) => {
      assert.equal(2, Object.keys(entry.row.payload()).length);
      const expectedId = expectedIds[i];
      assert.equal(expectedId, entry.getField(ta['id']));
      assert.equal('dummyName' + expectedId, entry.getField(ta['name']));
      assert.equal(expectedId, entry.getField(tb['id']));
      assert.equal('dummyName' + expectedId, entry.getField(tb['name']));
    });
  }
});
