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

import {DatabaseConnection} from '../../../lib/base/database_connection';
import {DataStoreType, Type} from '../../../lib/base/enum';
import {Global} from '../../../lib/base/global';
import {op} from '../../../lib/fn/op';
import {IndexJoinPass} from '../../../lib/proc/pp/index_join_pass';
import {JoinStep} from '../../../lib/proc/pp/join_step';
import {ProjectStep} from '../../../lib/proc/pp/project_step';
import {SelectStep} from '../../../lib/proc/pp/select_step';
import {TableAccessFullStep} from '../../../lib/proc/pp/table_access_full_step';
import {RuntimeDatabase} from '../../../lib/proc/runtime_database';
import {SelectContext} from '../../../lib/query/select_context';
import {Builder} from '../../../lib/schema/builder';
import {Column} from '../../../lib/schema/column';
import {Database} from '../../../lib/schema/database';
import {Table} from '../../../lib/schema/table';
import {TestTree, TreeTestHelper} from '../../../testing/tree_test_helper';

describe('IndexJoinPass', () => {
  let conn: DatabaseConnection;
  let schema: Database;
  let global: Global;
  let pass: IndexJoinPass;

  function getSchemaBuilder(): Builder {
    const schemaBuilder = new Builder('testschema', 1);
    schemaBuilder.createTable('TableA')
        .addColumn('id', Type.NUMBER)
        .addIndex('idx_id', ['id']);
    schemaBuilder.createTable('TableB')
        .addColumn('id', Type.NUMBER)
        .addIndex('idx_id', ['id']);
    schemaBuilder.createTable('TableC').addColumn('id', Type.NUMBER);
    return schemaBuilder;
  }

  beforeEach(() => {
    return getSchemaBuilder()
        .connect({storeType: DataStoreType.MEMORY})
        .then((db) => {
          conn = db;
          schema = db.getSchema();
          global = (db as RuntimeDatabase).getGlobal();
          pass = new IndexJoinPass();
        });
  });

  afterEach(() => {
    conn.close();
  });

  // Tests a simple tree, where
  //  - Only one join predicate exists.
  //  - Both columns of the join predicate have an index.
  //  - The columns in the join predicate appear in the same order as the tables
  //    of the join, left TableA, right TableB.
  it('BothColumnsIndexed', () => {
    const treeBefore = [
      'project()',
      '-join(type: inner, impl: hash, join_pred(TableA.id eq TableB.id))',
      '--table_access(TableA)',
      '--table_access(TableB)',
      '',
    ].join('\n');

    const treeAfter = [
      'project()',
      '-join(type: inner, impl: index_nested_loop, ' +
          'join_pred(TableA.id eq TableB.id))',
      '--table_access(TableA)',
      '--no_op_step(TableB)',
      '',
    ].join('\n');

    TreeTestHelper.assertTreeTransformation(
        constructTree1(schema.table('TableA'), schema.table('TableB'), false),
        treeBefore, treeAfter, pass);
  });

  // Tests a simple tree, where
  //  - Only one join predicate exists.
  //  - Both columns of the join predicate have an index.
  //  - The columns in the join predicate appear in the reverse order compared
  //    to the order of the tables.
  //    Predicate has TableB on the left, TableA on the right, where as the join
  //    has TableA on the left and TableB on the right.
  it('BothColumnsIndexed_ReversePredicate', () => {
    const treeBefore = [
      'project()',
      '-join(type: inner, impl: hash, join_pred(TableB.id eq TableA.id))',
      '--table_access(TableA)',
      '--table_access(TableB)',
      '',
    ].join('\n');

    const treeAfter = [
      'project()',
      '-join(type: inner, impl: index_nested_loop, ' +
          'join_pred(TableB.id eq TableA.id))',
      '--table_access(TableA)',
      '--no_op_step(TableB)',
      '',
    ].join('\n');

    TreeTestHelper.assertTreeTransformation(
        constructTree1(schema.table('TableA'), schema.table('TableB'), true),
        treeBefore, treeAfter, pass);
  });

  // Tests a simple tree, where
  //  - Only one join predicate exists.
  //  - Only the column of the left table has an index.
  // Ensures that index join is chosen for the left table.
  it('LeftTableColumnIndexed', () => {
    const treeBefore = [
      'project()',
      '-join(type: inner, impl: hash, join_pred(TableA.id eq TableC.id))',
      '--table_access(TableA)',
      '--table_access(TableC)',
      '',
    ].join('\n');

    const treeAfter = [
      'project()',
      '-join(type: inner, impl: index_nested_loop, ' +
          'join_pred(TableA.id eq TableC.id))',
      '--no_op_step(TableA)',
      '--table_access(TableC)',
      '',
    ].join('\n');

    TreeTestHelper.assertTreeTransformation(
        constructTree2(false), treeBefore, treeAfter, pass);
  });

  // Tests a simple tree, where
  //  - Only one join predicate exists.
  //  - Only the column of the right table has an index.
  // Ensures that index join is chosen for the right table.
  it('RightTableColumnIndexed', () => {
    const treeBefore = [
      'project()',
      '-join(type: inner, impl: hash, join_pred(TableA.id eq TableC.id))',
      '--table_access(TableC)',
      '--table_access(TableA)',
      '',
    ].join('\n');

    const treeAfter = [
      'project()',
      '-join(type: inner, impl: index_nested_loop, ' +
          'join_pred(TableA.id eq TableC.id))',
      '--table_access(TableC)',
      '--no_op_step(TableA)',
      '',
    ].join('\n');

    TreeTestHelper.assertTreeTransformation(
        constructTree2(true), treeBefore, treeAfter, pass);
  });

  // Tests a simple tree, where
  //  - Only one join predicate exists, which is a self-join.
  //  - Both columns of the join predicate have an index.
  // Ensures that index join is chosen for the right table, and that a NoOpStep
  // is inserted in the tree for the chosen table.
  it('SelfJoinTree', () => {
    const treeBefore = [
      'project()',
      '-join(type: inner, impl: hash, join_pred(t1.id eq t2.id))',
      '--table_access(TableA as t1)',
      '--table_access(TableA as t2)',
      '',
    ].join('\n');

    const treeAfter = [
      'project()',
      '-join(type: inner, impl: index_nested_loop, ' +
          'join_pred(t1.id eq t2.id))',
      '--table_access(TableA as t1)',
      '--no_op_step(t2)',
      '',
    ].join('\n');

    TreeTestHelper.assertTreeTransformation(
        constructTree1(
            schema.table('TableA').as('t1'), schema.table('TableA').as('t2'),
            false),
        treeBefore, treeAfter, pass);
  });

  // Tests a simple tree, where
  //  - Only one join predicate exists.
  //  - Only the column of the right table has an index, but there is a
  //  SelectStep
  //    after the table access.
  // Ensures that index join optimization is not applied in this case.
  it('TreeUnaffected', () => {
    const treeBefore = [
      'project()',
      '-join(type: inner, impl: hash, join_pred(TableC.id eq TableB.id))',
      '--table_access(TableC)',
      '--select(value_pred(TableB.id gt 100))',
      '---table_access(TableB)',
      '',
    ].join('\n');

    TreeTestHelper.assertTreeTransformation(
        constructTree3(), treeBefore, treeBefore, pass);
  });

  // |predicateReverseOrder| Whether to construct the predicate in reverse
  // order (table2 refrred on the left side of the predicate, table1 on the
  // right)
  function constructTree1(
      table1: Table, table2: Table, predicateReverseOrder: boolean): TestTree {
    const queryContext = new SelectContext(schema);
    queryContext.from = [table1, table2];
    queryContext.columns = [];
    const joinPredicate = predicateReverseOrder ?
        table2['id'].eq(table1['id']) :
        table1['id'].eq(table2['id']);
    queryContext.where = joinPredicate;

    const tableAccessStep1 =
        new TableAccessFullStep(global, queryContext.from[0]);
    const tableAccessStep2 =
        new TableAccessFullStep(global, queryContext.from[1]);
    const joinStep = new JoinStep(global, joinPredicate, false /* outerJoin*/);
    const projectStep =
        new ProjectStep(queryContext.columns, null as any as Column[]);
    projectStep.addChild(joinStep);
    joinStep.addChild(tableAccessStep1);
    joinStep.addChild(tableAccessStep2);

    return {
      queryContext: queryContext,
      root: projectStep,
    };
  }

  function constructTree2(tableRerevseOrder: boolean): TestTree {
    const tableA = schema.table('TableA');
    const tableC = schema.table('TableC');
    const t1 = tableRerevseOrder ? tableC : tableA;
    const t2 = tableRerevseOrder ? tableA : tableC;

    const queryContext = new SelectContext(schema);
    queryContext.from = [t1, t2];
    queryContext.columns = [];
    const joinPredicate = tableA['id'].eq(tableC['id']);
    queryContext.where = joinPredicate;

    const tableAccessStep1 =
        new TableAccessFullStep(global, queryContext.from[0]);
    const tableAccessStep2 =
        new TableAccessFullStep(global, queryContext.from[1]);
    const joinStep = new JoinStep(global, joinPredicate, false /* outerJoin*/);
    const projectStep =
        new ProjectStep(queryContext.columns, null as any as Column[]);
    projectStep.addChild(joinStep);
    joinStep.addChild(tableAccessStep1);
    joinStep.addChild(tableAccessStep2);

    return {
      queryContext: queryContext,
      root: projectStep,
    };
  }

  function constructTree3(): TestTree {
    const t1 = schema.table('TableC');
    const t2 = schema.table('TableB');

    const queryContext = new SelectContext(schema);
    queryContext.from = [t1, t2];
    const joinPredicate = t1['id'].eq(t2['id']);
    const valuePredicate = t2['id'].gt(100);
    queryContext.where = op.and(valuePredicate, joinPredicate);
    queryContext.columns = [];

    const tableAccessStep1 =
        new TableAccessFullStep(global, queryContext.from[0]);
    const tableAccessStep2 =
        new TableAccessFullStep(global, queryContext.from[1]);
    const selectStep = new SelectStep(valuePredicate.getId());
    const joinStep =
        new JoinStep(global, joinPredicate, false /* isOuterJoin*/);
    const projectStep =
        new ProjectStep(queryContext.columns, null as any as Column[]);

    joinStep.addChild(tableAccessStep1);
    selectStep.addChild(tableAccessStep2);
    joinStep.addChild(selectStep);
    projectStep.addChild(joinStep);

    return {
      queryContext: queryContext,
      root: projectStep,
    };
  }
});
