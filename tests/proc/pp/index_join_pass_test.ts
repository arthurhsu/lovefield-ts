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
import {JoinPredicate} from '../../../lib/pred/join_predicate';
import {IndexJoinPass} from '../../../lib/proc/pp/index_join_pass';
import {JoinStep} from '../../../lib/proc/pp/join_step';
import {ProjectStep} from '../../../lib/proc/pp/project_step';
import {SelectStep} from '../../../lib/proc/pp/select_step';
import {TableAccessFullStep} from '../../../lib/proc/pp/table_access_full_step';
import {RuntimeDatabase} from '../../../lib/proc/runtime_database';
import {SelectContext} from '../../../lib/query/select_context';
import {Builder} from '../../../lib/schema/builder';
import {Column} from '../../../lib/schema/column';
import {DatabaseSchema} from '../../../lib/schema/database_schema';
import {schema} from '../../../lib/schema/schema';
import {Table} from '../../../lib/schema/table';
import {TestTree, TreeTestHelper} from '../../../testing/tree_test_helper';

describe('IndexJoinPass', () => {
  let conn: DatabaseConnection;
  let dbSchema: DatabaseSchema;
  let global: Global;
  let pass: IndexJoinPass;

  function getSchemaBuilder(): Builder {
    const schemaBuilder = schema.create('testSchema', 1);
    schemaBuilder
      .createTable('TableA')
      .addColumn('id', Type.NUMBER)
      .addIndex('idx_id', ['id']);
    schemaBuilder
      .createTable('TableB')
      .addColumn('id', Type.NUMBER)
      .addIndex('idx_id', ['id']);
    schemaBuilder.createTable('TableC').addColumn('id', Type.NUMBER);
    return schemaBuilder;
  }

  beforeEach(() => {
    return getSchemaBuilder()
      .connect({storeType: DataStoreType.MEMORY})
      .then(db => {
        conn = db;
        dbSchema = db.getSchema();
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
      constructTree1(dbSchema.table('TableA'), dbSchema.table('TableB'), false),
      treeBefore,
      treeAfter,
      pass
    );
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
      constructTree1(dbSchema.table('TableA'), dbSchema.table('TableB'), true),
      treeBefore,
      treeAfter,
      pass
    );
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
      constructTree2(false),
      treeBefore,
      treeAfter,
      pass
    );
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
      constructTree2(true),
      treeBefore,
      treeAfter,
      pass
    );
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
        dbSchema.table('TableA').as('t1'),
        dbSchema.table('TableA').as('t2'),
        false
      ),
      treeBefore,
      treeAfter,
      pass
    );
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
      constructTree3(),
      treeBefore,
      treeBefore,
      pass
    );
  });

  // |predicateReverseOrder| Whether to construct the predicate in reverse
  // order (table2 referred on the left side of the predicate, table1 on the
  // right)
  function constructTree1(
    t1: Table,
    t2: Table,
    predicateReverseOrder: boolean
  ): TestTree {
    const table1 = t1;
    const table2 = t2;
    const queryContext = new SelectContext(dbSchema);
    queryContext.from = [table1, table2];
    queryContext.columns = [];
    const joinPredicate = (predicateReverseOrder
      ? table2.col('id').eq(table1.col('id'))
      : table1.col('id').eq(table2.col('id'))) as JoinPredicate;
    queryContext.where = joinPredicate;

    const tableAccessStep1 = new TableAccessFullStep(
      global,
      queryContext.from[0]
    );
    const tableAccessStep2 = new TableAccessFullStep(
      global,
      queryContext.from[1]
    );
    const joinStep = new JoinStep(global, joinPredicate, false /* outerJoin*/);
    const projectStep = new ProjectStep(
      queryContext.columns,
      (null as unknown) as Column[]
    );
    projectStep.addChild(joinStep);
    joinStep.addChild(tableAccessStep1);
    joinStep.addChild(tableAccessStep2);

    return {
      queryContext,
      root: projectStep,
    };
  }

  function constructTree2(tableReverseOrder: boolean): TestTree {
    const tableA = dbSchema.table('TableA');
    const tableC = dbSchema.table('TableC');
    const t1 = tableReverseOrder ? tableC : tableA;
    const t2 = tableReverseOrder ? tableA : tableC;

    const queryContext = new SelectContext(dbSchema);
    queryContext.from = [t1, t2];
    queryContext.columns = [];
    const joinPredicate = tableA
      .col('id')
      .eq(tableC.col('id')) as JoinPredicate;
    queryContext.where = joinPredicate;

    const tableAccessStep1 = new TableAccessFullStep(
      global,
      queryContext.from[0]
    );
    const tableAccessStep2 = new TableAccessFullStep(
      global,
      queryContext.from[1]
    );
    const joinStep = new JoinStep(global, joinPredicate, false /* outerJoin*/);
    const projectStep = new ProjectStep(
      queryContext.columns,
      (null as unknown) as Column[]
    );
    projectStep.addChild(joinStep);
    joinStep.addChild(tableAccessStep1);
    joinStep.addChild(tableAccessStep2);

    return {
      queryContext,
      root: projectStep,
    };
  }

  function constructTree3(): TestTree {
    const t1 = dbSchema.table('TableC');
    const t2 = dbSchema.table('TableB');

    const queryContext = new SelectContext(dbSchema);
    queryContext.from = [t1, t2];
    const joinPredicate = t1.col('id').eq(t2.col('id')) as JoinPredicate;
    const valuePredicate = t2.col('id').gt(100);
    queryContext.where = op.and(valuePredicate, joinPredicate);
    queryContext.columns = [];

    const tableAccessStep1 = new TableAccessFullStep(
      global,
      queryContext.from[0]
    );
    const tableAccessStep2 = new TableAccessFullStep(
      global,
      queryContext.from[1]
    );
    const selectStep = new SelectStep(valuePredicate.getId());
    const joinStep = new JoinStep(
      global,
      joinPredicate,
      false /* isOuterJoin*/
    );
    const projectStep = new ProjectStep(
      queryContext.columns,
      (null as unknown) as Column[]
    );

    joinStep.addChild(tableAccessStep1);
    selectStep.addChild(tableAccessStep2);
    joinStep.addChild(selectStep);
    projectStep.addChild(joinStep);

    return {
      queryContext,
      root: projectStep,
    };
  }
});
