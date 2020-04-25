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

import * as sinon from 'sinon';

import {DatabaseConnection} from '../../../lib/base/database_connection';
import {DataStoreType, Order} from '../../../lib/base/enum';
import {Global} from '../../../lib/base/global';
import {Service} from '../../../lib/base/service';
import {op} from '../../../lib/fn/op';
import {IndexStats} from '../../../lib/index/index_stats';
import {IndexStore} from '../../../lib/index/index_store';
import {JoinPredicate} from '../../../lib/pred/join_predicate';
import {Predicate} from '../../../lib/pred/predicate';
import {PredicateNode} from '../../../lib/pred/predicate_node';
import {CrossProductStep} from '../../../lib/proc/pp/cross_product_step';
import {IndexRangeScanPass} from '../../../lib/proc/pp/index_range_scan_pass';
import {JoinStep} from '../../../lib/proc/pp/join_step';
import {LimitStep} from '../../../lib/proc/pp/limit_step';
import {OrderByStep} from '../../../lib/proc/pp/order_by_step';
import {ProjectStep} from '../../../lib/proc/pp/project_step';
import {SelectStep} from '../../../lib/proc/pp/select_step';
import {TableAccessFullStep} from '../../../lib/proc/pp/table_access_full_step';
import {RuntimeDatabase} from '../../../lib/proc/runtime_database';
import {SelectContext} from '../../../lib/query/select_context';
import {BaseColumn} from '../../../lib/schema/base_column';
import {BaseTable} from '../../../lib/schema/base_table';
import {Column} from '../../../lib/schema/column';
import {DatabaseSchema} from '../../../lib/schema/database_schema';
import {Table} from '../../../lib/schema/table';
import {getHrDbSchemaBuilder} from '../../../testing/hr_schema/hr_schema_builder';
import {TestUtil} from '../../../testing/test_util';
import {TestTree, TreeTestHelper} from '../../../testing/tree_test_helper';

describe('IndexRangeScanPass', () => {
  let db: DatabaseConnection;
  let schema: DatabaseSchema;
  let global: Global;
  let e: Table;
  let j: Table;
  let d: Table;
  let cct: Table;
  let dt: Table;
  let indexStore: IndexStore;
  let pass: IndexRangeScanPass;
  let sandbox: sinon.SinonSandbox;

  beforeEach(() => {
    sandbox = sinon.createSandbox();
    const option = {storeType: DataStoreType.MEMORY};
    return getHrDbSchemaBuilder()
      .connect(option)
      .then(conn => {
        db = conn;
        schema = db.getSchema();
        e = schema.table('Employee');
        j = schema.table('Job');
        d = schema.table('Department');
        cct = schema.table('CrossColumnTable');
        dt = schema.table('DummyTable');
        global = (db as RuntimeDatabase).getGlobal();
        indexStore = global.getService(Service.INDEX_STORE);
        pass = new IndexRangeScanPass(global);
      });
  });

  afterEach(() => {
    db.close();
    sandbox.restore();
  });

  // Tests a simple tree, where only one value predicate exists.
  it('simpleTree', () => {
    const treeBefore = [
      'limit(20)',
      '-project()',
      '--select(value_pred(Employee.id gt 100))',
      '---table_access(Employee)',
      '',
    ].join('\n');

    const treeAfter = [
      'limit(20)',
      '-project()',
      '--table_access_by_row_id(Employee)',
      '---index_range_scan(Employee.pkEmployee, (100, unbound], natural)',
      '',
    ].join('\n');

    const constructTree = () => {
      const queryContext = new SelectContext(schema);
      queryContext.from = [e];
      queryContext.where = e.col('id').gt('100');
      queryContext.limit = 20;

      const limitNode = new LimitStep();
      const projectNode = new ProjectStep([], (null as unknown) as Column[]);
      limitNode.addChild(projectNode);
      const selectNode = new SelectStep(
        (queryContext.where as Predicate).getId()
      );
      projectNode.addChild(selectNode);
      const tableAccessNode = new TableAccessFullStep(
        global,
        queryContext.from[0]
      );
      selectNode.addChild(tableAccessNode);

      return {
        queryContext,
        root: limitNode,
      };
    };

    TreeTestHelper.assertTreeTransformation(
      constructTree(),
      treeBefore,
      treeAfter,
      pass
    );
  });

  // Test a tree that has an IN predicate on a column that has an index. It
  // ensures that the optimization is applied only if the number of values in
  // the IN predicate is small enough compared to the total number of rows.
  it('tree_WithInPredicate', () => {
    const treeBefore = [
      'project()',
      '-select(value_pred(Employee.id in 1,2,3))',
      '--table_access(Employee)',
      '',
    ].join('\n');

    const treeAfter = [
      'project()',
      '-table_access_by_row_id(Employee)',
      '--index_range_scan(Employee.pkEmployee, [1, 1],[2, 2],[3, 3], natural)',
      '',
    ].join('\n');

    const indexStats = new IndexStats();
    TestUtil.simulateIndexStats(
      sandbox,
      indexStore,
      (e as BaseTable).getRowIdIndexName(),
      indexStats
    );

    // Simulating case where the IN predicate has a low enough number of values
    // with respect to the total number of rows to be eligible for optimization.
    indexStats.totalRows = 200; // limit = 200 * 0.02 = 4
    TreeTestHelper.assertTreeTransformation(
      constructTreeWithInPredicate(3),
      treeBefore,
      treeAfter,
      pass
    );

    // Simulating case where the IN predicate has a high enough number of values
    // with respect to the total number of rows to NOT be eligible for
    // optimization.
    indexStats.totalRows = 100; // limit = 100 * 0.02 = 2
    TreeTestHelper.assertTreeTransformation(
      constructTreeWithInPredicate(3),
      treeBefore,
      treeBefore,
      pass
    );
  });

  it('tree_WithOrPredicate', () => {
    const treeBefore = [
      'project()',
      '-select(combined_pred_or)',
      '--table_access(Employee)',
      '',
    ].join('\n');

    const treeAfter = [
      'project()',
      '-table_access_by_row_id(Employee)',
      '--index_range_scan(Employee.pkEmployee, [1, 1],[2, 2],[3, 3], natural)',
      '',
    ].join('\n');

    const indexStats = new IndexStats();
    TestUtil.simulateIndexStats(
      sandbox,
      indexStore,
      (e as BaseTable).getRowIdIndexName(),
      indexStats
    );

    // Simulating case where the OR predicate has a low enough number of
    // children with respect to the total number of rows to be eligible for
    // optimization.
    indexStats.totalRows = 200; // limit = 200 * 0.02 = 4
    TreeTestHelper.assertTreeTransformation(
      constructTreeWithOrPredicate(3),
      treeBefore,
      treeAfter,
      pass
    );

    // Simulating case where the OR predicate has a high enough number of
    // children with respect to the total number of rows to NOT be eligible for
    // optimization.
    indexStats.totalRows = 100; // limit = 100 * 0.02 = 2
    TreeTestHelper.assertTreeTransformation(
      constructTreeWithOrPredicate(3),
      treeBefore,
      treeBefore,
      pass
    );
  });

  it('tree1', () => {
    const treeBefore = [
      'select(value_pred(Employee.id gt 100))',
      '-select(value_pred(Employee.salary eq 10000))',
      '--table_access(Employee)',
      '',
    ].join('\n');

    const treeAfter = [
      'select(value_pred(Employee.salary eq 10000))',
      '-table_access_by_row_id(Employee)',
      '--index_range_scan(Employee.pkEmployee, (100, unbound], natural)',
      '',
    ].join('\n');

    TestUtil.simulateIndexCost(
      sandbox,
      indexStore,
      (e.col('salary') as BaseColumn).getIndices()[0],
      100
    );
    TestUtil.simulateIndexCost(
      sandbox,
      indexStore,
      (e.col('id') as BaseColumn).getIndices()[0],
      5
    );

    TreeTestHelper.assertTreeTransformation(
      constructTree1(),
      treeBefore,
      treeAfter,
      pass
    );
  });

  it('tree2', () => {
    const treeBefore = [
      'project()',
      '-order_by(Employee.salary ASC)',
      '--join(type: inner, impl: hash, join_pred(Job.id eq Employee.jobId))',
      '---select(value_pred(Employee.id gt 100))',
      '----order_by(Employee.salary ASC)',
      '-----select(value_pred(Employee.salary eq 10000))',
      '------table_access(Employee)',
      '---select(value_pred(Job.id gt 100))',
      '----order_by(Job.title ASC)',
      '-----select(value_pred(Job.maxSalary eq 1000))',
      '------table_access(Job)',
      '',
    ].join('\n');

    const treeAfter = [
      'project()',
      '-order_by(Employee.salary ASC)',
      '--join(type: inner, impl: hash, join_pred(Job.id eq Employee.jobId))',
      '---order_by(Employee.salary ASC)',
      '----select(value_pred(Employee.salary eq 10000))',
      '-----table_access_by_row_id(Employee)',
      '------index_range_scan(Employee.pkEmployee, (100, unbound], natural)',
      '---order_by(Job.title ASC)',
      '----select(value_pred(Job.maxSalary eq 1000))',
      '-----table_access_by_row_id(Job)',
      '------index_range_scan(Job.pkJob, (100, unbound], natural)',
      '',
    ].join('\n');

    TestUtil.simulateIndexCost(
      sandbox,
      indexStore,
      (e.col('salary') as BaseColumn).getIndices()[0],
      100
    );
    TestUtil.simulateIndexCost(
      sandbox,
      indexStore,
      (e.col('id') as BaseColumn).getIndices()[0],
      5
    );
    TestUtil.simulateIndexCost(
      sandbox,
      indexStore,
      (j.col('maxSalary') as BaseColumn).getIndices()[0],
      100
    );
    TestUtil.simulateIndexCost(
      sandbox,
      indexStore,
      (j.col('id') as BaseColumn).getIndices()[0],
      5
    );

    TreeTestHelper.assertTreeTransformation(
      constructTree2(),
      treeBefore,
      treeAfter,
      pass
    );
  });

  // Tests the case where a SelectStep node is paired with a TableAccessFullStep
  // and the two are separated by a CrossProductStep. It ensures that other
  // children of the CrossProductStep are not affected.
  it('tree3', () => {
    const treeBefore = [
      'project()',
      '-select(value_pred(Job.id eq 100))',
      '--cross_product',
      '---table_access(Job)',
      '---table_access(Department)',
      '',
    ].join('\n');

    const treeAfter = [
      'project()',
      '-cross_product',
      '--table_access_by_row_id(Job)',
      '---index_range_scan(Job.pkJob, [100, 100], natural)',
      '--table_access(Department)',
      '',
    ].join('\n');

    const constructTree = () => {
      const queryContext = new SelectContext(schema);
      queryContext.from = [j, d];
      queryContext.where = j.col('id').eq('100');

      const crossProductStep = new CrossProductStep();
      const tableAccessJob = new TableAccessFullStep(
        global,
        queryContext.from[0]
      );
      const tableAccessDepartment = new TableAccessFullStep(
        global,
        queryContext.from[1]
      );
      crossProductStep.addChild(tableAccessJob);
      crossProductStep.addChild(tableAccessDepartment);

      const selectStep = new SelectStep(
        (queryContext.where as Predicate).getId()
      );
      selectStep.addChild(crossProductStep);
      const rootNode = new ProjectStep([], (null as unknown) as Column[]);
      rootNode.addChild(selectStep);

      return {
        queryContext,
        root: rootNode,
      };
    };

    TreeTestHelper.assertTreeTransformation(
      constructTree(),
      treeBefore,
      treeAfter,
      pass
    );
  });

  // Tests a tree where
  //  - 2 predicates for 'salary' column exist.
  //  - 1 predicate for 'id' column exists.
  //  - 2 indices exist, one for each column.
  // This test checks that two separate predicates can be replaced by an
  // IndexRangeScanPass if they refer to the same column in the case where that
  // column's index is chosen to be used for optimization.
  it('tree_MultiplePredicates_SingleColumnIndices', () => {
    const treeBefore = [
      'select(value_pred(Employee.salary lte 200))',
      '-select(value_pred(Employee.id gt 100))',
      '--select(value_pred(Employee.salary gte 100))',
      '---table_access(Employee)',
      '',
    ].join('\n');

    const treeAfter = [
      'select(value_pred(Employee.id gt 100))',
      '-table_access_by_row_id(Employee)',
      '--index_range_scan(Employee.idx_salary, [100, 200], natural)',
      '',
    ].join('\n');

    TestUtil.simulateIndexCost(
      sandbox,
      indexStore,
      (e.col('salary') as BaseColumn).getIndices()[0],
      10
    );
    TestUtil.simulateIndexCost(
      sandbox,
      indexStore,
      (e.col('id') as BaseColumn).getIndices()[0],
      500
    );

    TreeTestHelper.assertTreeTransformation(
      constructTree3(),
      treeBefore,
      treeAfter,
      pass
    );
  });

  // Tests a tree where
  //  - two cross-column indices exist, each index is indexing two columns (one
  //  of
  //    which is a nullable index).
  //  - two predicates exist for the first cross-column index.
  //  - two predicates exist for the second cross-column index.
  //
  //  It ensures that the most selective index is chosen by the optimizer and
  //  that the predicates are correctly replaced by an IndexRangeScanStep in the
  //  tree.
  it('tree_MultipleCrossColumnIndices', () => {
    const treeBefore = [
      'select(value_pred(CrossColumnTable.string1 gt StringValue1))',
      '-select(value_pred(CrossColumnTable.integer2 gt 100))',
      '--select(value_pred(CrossColumnTable.integer1 gte 400))',
      '---select(value_pred(CrossColumnTable.string2 eq StringValue2))',
      '----table_access(CrossColumnTable)',
      '',
    ].join('\n');

    const treeAfter = [
      'select(value_pred(CrossColumnTable.integer2 gt 100))',
      '-select(value_pred(CrossColumnTable.integer1 gte 400))',
      '--table_access_by_row_id(CrossColumnTable)',
      '---index_range_scan(CrossColumnTable.idx_crossNull, ' +
        '(StringValue1, unbound],[StringValue2, StringValue2], natural)',
      '',
    ].join('\n');

    const indices = (cct as BaseTable).getIndices();
    TestUtil.simulateIndexCost(sandbox, indexStore, indices[0], 100);
    TestUtil.simulateIndexCost(sandbox, indexStore, indices[1], 10);

    const constructTree = () => {
      const queryContext = new SelectContext(schema);
      queryContext.from = [cct];
      queryContext.where = op.and(
        cct.col('string1').gt('StringValue1'),
        cct.col('integer2').gt(100),
        cct.col('integer1').gte(400),
        cct.col('string2').eq('StringValue2')
      );

      const selectNode1 = createSelectStep(queryContext, 0);
      const selectNode2 = createSelectStep(queryContext, 1);
      const selectNode3 = createSelectStep(queryContext, 2);
      const selectNode4 = createSelectStep(queryContext, 3);
      const tableAccessNode = new TableAccessFullStep(
        global,
        queryContext.from[0]
      );
      selectNode1.addChild(selectNode2);
      selectNode2.addChild(selectNode3);
      selectNode3.addChild(selectNode4);
      selectNode4.addChild(tableAccessNode);

      return {
        queryContext,
        root: selectNode1,
      };
    };

    TreeTestHelper.assertTreeTransformation(
      constructTree(),
      treeBefore,
      treeAfter,
      pass
    );
  });

  // Tests a tree where
  //  - two cross-column indices exist, each index is indexing two columns.
  //  - a predicate that refers to a subset of the cross column index's columns
  //    exists. The referred columns are enough to leverage the index, since
  //    they do form a prefix. Cross-column indices exist on
  //    ['string', 'number'] and ['integer', 'string2'], and the predicates
  //    refer to 'string', and 'integer', therefore both indices are valid
  //    candidates and the most selective one should be leveraged.
  it('tree_MultipleCrossColumnIndices_PartialMatching', () => {
    const treeBefore = [
      'select(value_pred(DummyTable.string eq StringValue))',
      '-select(value_pred(DummyTable.integer gt 100))',
      '--table_access(DummyTable)',
      '',
    ].join('\n');

    const treeAfter = [
      'select(value_pred(DummyTable.integer gt 100))',
      '-table_access_by_row_id(DummyTable)',
      '--index_range_scan(DummyTable.pkDummyTable, ' +
        '[StringValue, StringValue],[unbound, unbound], natural)',
      '',
    ].join('\n');

    const indices = (dt as BaseTable).getIndices();
    TestUtil.simulateIndexCost(sandbox, indexStore, indices[0], 10);
    TestUtil.simulateIndexCost(sandbox, indexStore, indices[1], 100);

    const constructTree = () => {
      const queryContext = new SelectContext(schema);
      queryContext.from = [dt];
      queryContext.where = op.and(
        dt.col('string').eq('StringValue'),
        dt.col('integer').gt(100)
      );

      const selectNode1 = createSelectStep(queryContext, 0);
      const selectNode2 = createSelectStep(queryContext, 1);
      const tableAccessNode = new TableAccessFullStep(
        global,
        queryContext.from[0]
      );
      selectNode1.addChild(selectNode2);
      selectNode2.addChild(tableAccessNode);

      return {
        queryContext,
        root: selectNode1,
      };
    };

    TreeTestHelper.assertTreeTransformation(
      constructTree(),
      treeBefore,
      treeAfter,
      pass
    );
  });

  // Tests a tree where
  //  - two cross-column indices exist, each index is indexing two columns.
  //  - a predicate that refers to a subset of the cross column index's columns
  //    exists. The referred columns are not enough to leverage the index, since
  //    they don't form a prefix. Cross-column indices exist on
  //    ['string', 'number'] and ['integer', 'string2'], but only 'number' and
  //    'string2' are bound with a predicate.
  //  - a predicate that refers to a non-indexed column ('boolean') exists.
  //
  //  It ensures that the tree remains unaffected since no index can be
  //  leveraged.
  it('tree_Unaffected', () => {
    const treeBefore = [
      'select(value_pred(DummyTable.boolean eq false))',
      '-select(value_pred(DummyTable.number gt 100))',
      '--select(value_pred(DummyTable.string2 eq OtherStringValue))',
      '---table_access(DummyTable)',
      '',
    ].join('\n');

    const constructTree = () => {
      const queryContext = new SelectContext(schema);
      queryContext.from = [dt];
      queryContext.where = op.and(
        dt.col('boolean').eq(false),
        dt.col('number').gt(100),
        dt.col('string2').eq('OtherStringValue')
      );

      const selectNode1 = createSelectStep(queryContext, 0);
      const selectNode2 = createSelectStep(queryContext, 1);
      const selectNode3 = createSelectStep(queryContext, 2);
      const tableAccessNode = new TableAccessFullStep(
        global,
        queryContext.from[0]
      );
      selectNode1.addChild(selectNode2);
      selectNode2.addChild(selectNode3);
      selectNode3.addChild(tableAccessNode);

      return {
        queryContext,
        root: selectNode1,
      };
    };

    TreeTestHelper.assertTreeTransformation(
      constructTree(),
      treeBefore,
      treeBefore,
      pass
    );
  });

  // Constructs a tree where:
  //  - One TableAccessFullStep node exists.
  //  - Multiple SelectStep nodes exist without any nodes in-between them.
  function constructTree1(): TestTree {
    const queryContext = new SelectContext(schema);
    queryContext.from = [e];
    queryContext.where = op.and(
      e.col('id').gt('100'),
      e.col('salary').eq(10000)
    );

    const selectNode1 = createSelectStep(queryContext, 0);
    const selectNode2 = createSelectStep(queryContext, 1);
    selectNode1.addChild(selectNode2);
    const tableAccessNode = new TableAccessFullStep(
      global,
      queryContext.from[0]
    );
    selectNode2.addChild(tableAccessNode);
    return {
      queryContext,
      root: selectNode1,
    };
  }

  // Constructs a tree where:
  //  - Two TableAccessFullStep nodes exist.
  //  - Multiple SelectStep nodes per TableAccessFullStep node exist.
  //  - SelectStep nodes are separated by an OrderByStep node in between them.
  function constructTree2(): TestTree {
    const queryContext = new SelectContext(schema);
    queryContext.from = [e, j];
    queryContext.where = op.and(
      e.col('id').gt('100'),
      e.col('salary').eq(10000),
      j.col('id').gt('100'),
      j.col('maxSalary').eq(1000),
      j.col('id').eq(e.col('jobId'))
    );

    // Constructing left sub-tree.
    const selectNode1 = createSelectStep(queryContext, 0);
    const orderByNode1 = new OrderByStep([
      {column: e.col('salary'), order: Order.ASC},
    ]);
    const selectNode2 = createSelectStep(queryContext, 1);
    const tableAccessNode1 = new TableAccessFullStep(
      global,
      queryContext.from[0]
    );

    selectNode1.addChild(orderByNode1);
    orderByNode1.addChild(selectNode2);
    selectNode2.addChild(tableAccessNode1);

    // Constructing right sub-tree.
    const selectNode3 = createSelectStep(queryContext, 2);
    const orderByNode2 = new OrderByStep([
      {column: j.col('title'), order: Order.ASC},
    ]);
    const selectNode4 = createSelectStep(queryContext, 3);
    const tableAccessNode2 = new TableAccessFullStep(
      global,
      queryContext.from[1]
    );

    selectNode3.addChild(orderByNode2);
    orderByNode2.addChild(selectNode4);
    selectNode4.addChild(tableAccessNode2);

    // Constructing the overall tree.
    const rootNode = new ProjectStep([], (null as unknown) as Column[]);
    const orderByNode3 = new OrderByStep([
      {column: e.col('salary'), order: Order.ASC},
    ]);
    const joinPredicate = (queryContext.where as PredicateNode).getChildAt(
      4
    ) as JoinPredicate;
    const joinNode = new JoinStep(global, joinPredicate, false);

    rootNode.addChild(orderByNode3);
    orderByNode3.addChild(joinNode);
    joinNode.addChild(selectNode1);
    joinNode.addChild(selectNode3);

    return {
      queryContext,
      root: rootNode,
    };
  }

  function constructTree3(): TestTree {
    const queryContext = new SelectContext(schema);
    queryContext.from = [e];
    queryContext.where = op.and(
      e.col('salary').lte(200),
      e.col('id').gt('100'),
      e.col('salary').gte(100)
    );

    const selectNode1 = createSelectStep(queryContext, 0);
    const selectNode2 = createSelectStep(queryContext, 1);
    const selectNode3 = createSelectStep(queryContext, 2);
    const tableAccessNode = new TableAccessFullStep(
      global,
      queryContext.from[0]
    );

    selectNode1.addChild(selectNode2);
    selectNode2.addChild(selectNode3);
    selectNode3.addChild(tableAccessNode);

    return {
      queryContext,
      root: selectNode1,
    };
  }

  // Constructs a tree that has an IN predicate.
  // |valueCount| is the number of values in the IN predicate.
  function constructTreeWithInPredicate(valueCount: number): TestTree {
    const values: string[] = new Array(valueCount);
    for (let i = 0; i < values.length; i++) {
      values[i] = (i + 1).toString();
    }
    return constructTreeWithPredicate(e.col('id').in(values));
  }

  function constructTreeWithOrPredicate(valueCount: number): TestTree {
    const predicates: Predicate[] = new Array(valueCount);
    for (let i = 0; i < predicates.length; i++) {
      predicates[i] = e.col('id').eq((i + 1).toString());
    }
    const orPredicate = op.or.apply(null, predicates);
    return constructTreeWithPredicate(orPredicate);
  }

  function constructTreeWithPredicate(predicate: Predicate): TestTree {
    const queryContext = new SelectContext(schema);
    queryContext.from = [e];
    queryContext.where = predicate;

    const projectNode = new ProjectStep([], (null as unknown) as Column[]);
    const selectNode = new SelectStep(queryContext.where.getId());
    projectNode.addChild(selectNode);
    const tableAccessNode = new TableAccessFullStep(
      global,
      queryContext.from[0]
    );
    selectNode.addChild(tableAccessNode);

    return {
      queryContext,
      root: projectNode,
    };
  }

  function createSelectStep(
    queryContext: SelectContext,
    predicateIndex: number
  ): SelectStep {
    return new SelectStep(
      ((queryContext.where as PredicateNode).getChildAt(
        predicateIndex
      ) as PredicateNode).getId()
    );
  }
});
