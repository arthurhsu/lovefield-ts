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

import {DataStoreType, Order, Type} from '../../../lib/base/enum';
import {Global} from '../../../lib/base/global';
import {op} from '../../../lib/fn/op';
import {SingleKeyRange} from '../../../lib/index/key_range';
import {ValuePredicate} from '../../../lib/pred/value_predicate';
import {IndexRangeScanStep} from '../../../lib/proc/pp/index_range_scan_step';
import {MultiIndexRangeScanStep} from '../../../lib/proc/pp/multi_index_range_scan_step';
import {OrderByIndexPass} from '../../../lib/proc/pp/order_by_index_pass';
import {OrderByStep} from '../../../lib/proc/pp/order_by_step';
import {ProjectStep} from '../../../lib/proc/pp/project_step';
import {SelectStep} from '../../../lib/proc/pp/select_step';
import {TableAccessByRowIdStep} from '../../../lib/proc/pp/table_access_by_row_id_step';
import {TableAccessFullStep} from '../../../lib/proc/pp/table_access_full_step';
import {RuntimeDatabase} from '../../../lib/proc/runtime_database';
import {SelectContext} from '../../../lib/query/select_context';
import {BaseColumn} from '../../../lib/schema/base_column';
import {BaseTable} from '../../../lib/schema/base_table';
import {Builder} from '../../../lib/schema/builder';
import {IndexImpl} from '../../../lib/schema/index_impl';
import {MockKeyRangeCalculator} from '../../../testing/mock_key_range_calculator';
import {TestTree, TreeTestHelper} from '../../../testing/tree_test_helper';

describe('OrderByIndexPass', () => {
  let db: RuntimeDatabase;
  let global: Global;
  let simpleTable: BaseTable;
  let crossColumnTable: BaseTable;
  let pass: OrderByIndexPass;
  const NULL = null as any as BaseColumn[];

  beforeEach(async () => {
    const schemaBuilder = getSchemaBuilder();
    global = schemaBuilder.getGlobal();
    db = await schemaBuilder.connect({storeType: DataStoreType.MEMORY}) as
        RuntimeDatabase;
    simpleTable = db.getSchema().table('SimpleTable');
    crossColumnTable = db.getSchema().table('CrossColumnTable');
    pass = new OrderByIndexPass(global);
  });

  afterEach(() => {
    db.close();
  });

  function getSchemaBuilder(): Builder {
    const schemaBuilder = new Builder('orderbyindexpass', 1);
    schemaBuilder.createTable('SimpleTable')
        .addColumn('id', Type.INTEGER)
        .addColumn('salary', Type.INTEGER)
        .addColumn('age', Type.INTEGER)
        .addIndex('idx_salary', ['salary'], false, Order.DESC)
        .addIndex('idx_age', ['age'], false, Order.DESC);

    schemaBuilder.createTable('CrossColumnTable')
        .addColumn('string', Type.STRING)
        .addColumn('number', Type.NUMBER)
        .addColumn('boolean', Type.BOOLEAN)
        .addIndex('idx_crossColumn', ['string', 'number'])
        .addNullable(['number']);
    return schemaBuilder;
  }

  function getIndexByName(table: BaseTable, indexName: string): IndexImpl {
    return (table.getIndices() as IndexImpl[])
        .filter((index) => index.name === indexName)[0];
  }

  // Tests a tree where the contents of a table are filtered by a value
  // predicate referring to a different column than the one used for sorting.
  it('tree1', () => {
    const treeBefore = [
      'project()',
      '-order_by(SimpleTable.salary DESC)',
      '--select(value_pred(SimpleTable.id gt 100))',
      '---table_access(SimpleTable)',
      '',
    ].join('\n');

    const treeAfter = [
      'project()',
      '-select(value_pred(SimpleTable.id gt 100))',
      '--table_access_by_row_id(SimpleTable)',
      '---index_range_scan(' +
          'SimpleTable.idx_salary, [unbound, unbound], natural)',
      '',
    ].join('\n');

    TreeTestHelper.assertTreeTransformation(
        constructTree1(simpleTable['salary'], Order.DESC), treeBefore,
        treeAfter, pass);
  });

  // Tests a tree where an IndexRangeScanStep for the same column used for
  // sorting already exists.
  it('tree2', () => {
    const treeBefore = [
      'project()',
      '-order_by(SimpleTable.salary DESC)',
      '--table_access_by_row_id(SimpleTable)',
      '---index_range_scan(SimpleTable.idx_salary, [10000, unbound], reverse)',
      '',
    ].join('\n');

    const treeAfter = [
      'project()',
      '-table_access_by_row_id(SimpleTable)',
      '--index_range_scan(SimpleTable.idx_salary, [10000, unbound], natural)',
      '',
    ].join('\n');

    const constructTree = () => {
      const queryContext = new SelectContext(db.getSchema());
      queryContext.from = [simpleTable];
      queryContext.where = simpleTable['salary'].gte(10000);
      queryContext.orderBy = [{
        column: simpleTable['salary'],
        order: Order.DESC,
      }];

      const rootNode = new ProjectStep([], NULL);
      const orderByNode = new OrderByStep(queryContext.orderBy);
      const tableAccessByRowIdNode =
          new TableAccessByRowIdStep(global, queryContext.from[0]);
      const keyRanges =
          (queryContext.where as ValuePredicate).toKeyRange().getValues();
      const indexRangeScanNode = new IndexRangeScanStep(
          global, simpleTable['salary'].getIndex(),
          new MockKeyRangeCalculator(keyRanges), true);
      tableAccessByRowIdNode.addChild(indexRangeScanNode);
      orderByNode.addChild(tableAccessByRowIdNode);
      rootNode.addChild(orderByNode);

      return {
        queryContext: queryContext,
        root: rootNode,
      };
    };

    TreeTestHelper.assertTreeTransformation(
        constructTree(), treeBefore, treeAfter, pass);
  });

  // Tests the case where an OrderByNode exists in the tree, but there is no
  // index for the column that is used for sorting. The tree should remain
  // unaffected.
  it('tree3_Unaffected', () => {
    const treeBefore = [
      'project()',
      '-order_by(SimpleTable.id ASC)',
      '--select(value_pred(SimpleTable.id gt 100))',
      '---table_access(SimpleTable)',
      '',
    ].join('\n');

    // Tree should be unaffected, since no index exists on SimpleTable#age.
    TreeTestHelper.assertTreeTransformation(
        constructTree1(simpleTable['id'], Order.ASC), treeBefore, treeBefore,
        pass);
  });

  // Tests the case where a MultiIndexRangeScanStep exists in the tree,
  // preventing this optimization from being applicable, even though on
  // OrderByStep exists in the tree for a column where a corresponding
  // IndexRangeScanStep also exists.
  it('tree_MultiColumnIndexRangeScan_Unaffected', () => {
    const treeBefore = [
      'project()',
      '-order_by(SimpleTable.salary ASC)',
      '--table_access_by_row_id(SimpleTable)',
      '---multi_index_range_scan()',
      '----index_range_scan(SimpleTable.idx_salary, [100, 100], natural)',
      '----index_range_scan(SimpleTable.idx_age, [40, 40], natural)',
      '',
    ].join('\n');

    const constructTree = () => {
      const queryContext = new SelectContext(db.getSchema());
      queryContext.from = [simpleTable];
      const predicate1 = simpleTable['salary'].eq(100) as ValuePredicate;
      const predicate2 = simpleTable['age'].eq(40) as ValuePredicate;
      queryContext.where = op.or(predicate1, predicate2);
      queryContext.orderBy = [{
        column: simpleTable['salary'],
        order: Order.ASC,
      }];

      const indexRangeScanStep1 = new IndexRangeScanStep(
          global, simpleTable['salary'].getIndex(),
          new MockKeyRangeCalculator(predicate1.toKeyRange().getValues()),
          false);
      const indexRangeScanStep2 = new IndexRangeScanStep(
          global, simpleTable['age'].getIndex(),
          new MockKeyRangeCalculator(predicate2.toKeyRange().getValues()),
          false);
      const multiIndexRangeScanStep = new MultiIndexRangeScanStep();
      const tableAccessByRowIdStep =
          new TableAccessByRowIdStep(global, queryContext.from[0]);
      const orderByStep = new OrderByStep(queryContext.orderBy);
      const projectStep = new ProjectStep([], NULL);
      projectStep.addChild(orderByStep);
      orderByStep.addChild(tableAccessByRowIdStep);
      tableAccessByRowIdStep.addChild(multiIndexRangeScanStep);
      multiIndexRangeScanStep.addChild(indexRangeScanStep1);
      multiIndexRangeScanStep.addChild(indexRangeScanStep2);

      return {
        queryContext: queryContext,
        root: projectStep,
      };
    };

    TreeTestHelper.assertTreeTransformation(
        constructTree(), treeBefore, treeBefore, pass);
  });

  // Tests the case where a cross-column index can be leveraged to perform the
  // ORDER BY, using the index's natural order.
  it('tree_TableAccess_CrossColumnIndex_Natural', () => {
    const treeBefore = [
      'project()',
      '-order_by(CrossColumnTable.string ASC, CrossColumnTable.number ASC)',
      '--select(value_pred(CrossColumnTable.boolean eq false))',
      '---table_access(CrossColumnTable)',
      '',
    ].join('\n');

    const treeAfter = [
      'project()',
      '-select(value_pred(CrossColumnTable.boolean eq false))',
      '--table_access_by_row_id(CrossColumnTable)',
      '---index_range_scan(CrossColumnTable.idx_crossColumn, ' +
          '[unbound, unbound],[unbound, unbound], natural)',
      '',
    ].join('\n');

    TreeTestHelper.assertTreeTransformation(
        constructTree2(Order.ASC, Order.ASC), treeBefore, treeAfter, pass);
  });

  // Tests the case where a cross-column index can be leveraged to perform the
  // ORDER BY, using the index's reverse order.
  it('tree_TableAccess_CrossColumnIndex_Reverse', () => {
    const treeBefore = [
      'project()',
      '-order_by(CrossColumnTable.string DESC, CrossColumnTable.number DESC)',
      '--select(value_pred(CrossColumnTable.boolean eq false))',
      '---table_access(CrossColumnTable)',
      '',
    ].join('\n');

    const treeAfter = [
      'project()',
      '-select(value_pred(CrossColumnTable.boolean eq false))',
      '--table_access_by_row_id(CrossColumnTable)',
      '---index_range_scan(CrossColumnTable.idx_crossColumn, ' +
          '[unbound, unbound],[unbound, unbound], reverse)',
      '',
    ].join('\n');

    TreeTestHelper.assertTreeTransformation(
        constructTree2(Order.DESC, Order.DESC), treeBefore, treeAfter, pass);
  });

  // Tests the case where an existing cross-column index can't be leveraged to
  // perform the ORDER BY, even though it refers to the same columns as the
  // ORDER BY because the requested order does not match the index's natural or
  // reverse order.
  it('tree_TableAccess_CrossColumnIndex_Unaffected', () => {
    const treeBefore = [
      'project()',
      '-order_by(CrossColumnTable.string DESC, CrossColumnTable.number ASC)',
      '--select(value_pred(CrossColumnTable.boolean eq false))',
      '---table_access(CrossColumnTable)',
      '',
    ].join('\n');

    TreeTestHelper.assertTreeTransformation(
        constructTree2(Order.DESC, Order.ASC), treeBefore, treeBefore, pass);
  });

  // Tests the case where an existing IndexRangeScanStep can be leveraged to
  // perform the ORDER BY. The optimization pass simply removes the ORDER BY
  // node and adjusts the IndexRangeScanStep's ordering to match the requested
  // order.
  it('tree_IndexRangeScan_CrossColumnIndex', () => {
    const treeBefore = [
      'project()',
      '-order_by(CrossColumnTable.string DESC, CrossColumnTable.number DESC)',
      '--select(value_pred(CrossColumnTable.boolean eq false))',
      '---table_access_by_row_id(CrossColumnTable)',
      '----index_range_scan(CrossColumnTable.idx_crossColumn, ' +
          '[unbound, unbound],[unbound, 10], natural)',
      '',
    ].join('\n');

    const treeAfter = [
      'project()',
      '-select(value_pred(CrossColumnTable.boolean eq false))',
      '--table_access_by_row_id(CrossColumnTable)',
      '---index_range_scan(CrossColumnTable.idx_crossColumn, ' +
          '[unbound, unbound],[unbound, 10], reverse)',
      '',
    ].join('\n');

    TreeTestHelper.assertTreeTransformation(
        constructTree3(Order.DESC, Order.DESC), treeBefore, treeAfter, pass);
  });

  // Tests the case where an existing IndexRangeScanStep can't be leveraged to
  // perform the ORDER BY, because the requested order does not much neither the
  // reverse nor the natural index's order.
  it('tree_IndexRangeScan_CrossColumnIndex_Unaffected', () => {
    const treeBefore = [
      'project()',
      '-order_by(CrossColumnTable.string ASC, CrossColumnTable.number DESC)',
      '--select(value_pred(CrossColumnTable.boolean eq false))',
      '---table_access_by_row_id(CrossColumnTable)',
      '----index_range_scan(CrossColumnTable.idx_crossColumn, ' +
          '[unbound, unbound],[unbound, 10], natural)',
      '',
    ].join('\n');

    TreeTestHelper.assertTreeTransformation(
        constructTree3(Order.ASC, Order.DESC), treeBefore, treeBefore, pass);
  });

  function constructTree1(sortColumn: BaseColumn, sortOrder: Order): TestTree {
    const queryContext = new SelectContext(db.getSchema());
    queryContext.from = [simpleTable];
    queryContext.orderBy = [{
      column: sortColumn,
      order: sortOrder,
    }];
    queryContext.where = simpleTable['id'].gt(100) as ValuePredicate;

    const rootNode = new ProjectStep([], NULL);
    const orderByNode = new OrderByStep(queryContext.orderBy);
    const selectNode = new SelectStep(queryContext.where.getId());
    const tableAccessNode =
        new TableAccessFullStep(global, queryContext.from[0]);

    selectNode.addChild(tableAccessNode);
    orderByNode.addChild(selectNode);
    rootNode.addChild(orderByNode);

    return {
      queryContext: queryContext,
      root: rootNode,
    };
  }

  function constructTree2(sortOrder1: Order, sortOrder2: Order): TestTree {
    const queryContext = new SelectContext(db.getSchema());
    queryContext.from = [crossColumnTable];
    queryContext.where =
        crossColumnTable['boolean'].eq(false) as ValuePredicate;
    queryContext.orderBy = [
      {
        column: crossColumnTable['string'],
        order: sortOrder1,
      },
      {
        column: crossColumnTable['number'],
        order: sortOrder2,
      },
    ];

    const projectNode = new ProjectStep([], NULL);
    const orderByNode = new OrderByStep(queryContext.orderBy);
    const selectNode = new SelectStep(queryContext.where.getId());
    const tableAccessNode =
        new TableAccessFullStep(global, queryContext.from[0]);

    selectNode.addChild(tableAccessNode);
    orderByNode.addChild(selectNode);
    projectNode.addChild(orderByNode);

    return {
      queryContext: queryContext,
      root: projectNode,
    };
  }

  function constructTree3(sortOrder1: Order, sortOrder2: Order): TestTree {
    const queryContext = new SelectContext(db.getSchema());
    queryContext.from = [crossColumnTable];
    queryContext.where =
        crossColumnTable['boolean'].eq(false) as ValuePredicate;
    queryContext.orderBy = [
      {
        column: crossColumnTable['string'],
        order: sortOrder1,
      },
      {
        column: crossColumnTable['number'],
        order: sortOrder2,
      },
    ];

    const projectNode = new ProjectStep([], NULL);
    const orderByNode = new OrderByStep(queryContext.orderBy);
    const selectNode = new SelectStep(queryContext.where.getId());
    const tableAccessByRowIdNode =
        new TableAccessByRowIdStep(global, queryContext.from[0]);
    const indexRangeScanNode = new IndexRangeScanStep(
        global, getIndexByName(crossColumnTable, 'idx_crossColumn'),
        new MockKeyRangeCalculator([
          SingleKeyRange.all(),
          SingleKeyRange.upperBound(10),
        ]),
        false);

    tableAccessByRowIdNode.addChild(indexRangeScanNode);
    selectNode.addChild(tableAccessByRowIdNode);
    orderByNode.addChild(selectNode);
    projectNode.addChild(orderByNode);

    return {
      queryContext: queryContext,
      root: projectNode,
    };
  }
});
