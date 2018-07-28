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
import {DataStoreType, Order} from '../../../lib/base/enum';
import {Global} from '../../../lib/base/global';
import {fn} from '../../../lib/fn/fn';
import {op} from '../../../lib/fn/op';
import {SingleKeyRange} from '../../../lib/index/key_range';
import {Predicate} from '../../../lib/pred/predicate';
import {PredicateNode} from '../../../lib/pred/predicate_node';
import {ValuePredicate} from '../../../lib/pred/value_predicate';
import {IndexRangeScanStep} from '../../../lib/proc/pp/index_range_scan_step';
import {LimitSkipByIndexPass} from '../../../lib/proc/pp/limit_skip_by_index_pass';
import {LimitStep} from '../../../lib/proc/pp/limit_step';
import {OrderByStep} from '../../../lib/proc/pp/order_by_step';
import {ProjectStep} from '../../../lib/proc/pp/project_step';
import {SelectStep} from '../../../lib/proc/pp/select_step';
import {SkipStep} from '../../../lib/proc/pp/skip_step';
import {TableAccessByRowIdStep} from '../../../lib/proc/pp/table_access_by_row_id_step';
import {RuntimeDatabase} from '../../../lib/proc/runtime_database';
import {SelectContext} from '../../../lib/query/select_context';
import {BaseColumn} from '../../../lib/schema/base_column';
import {BaseTable} from '../../../lib/schema/base_table';
import {Database} from '../../../lib/schema/database';
import {IndexImpl} from '../../../lib/schema/index_impl';
import {getHrDbSchemaBuilder} from '../../../testing/hr_schema/hr_schema_builder';
import {MockKeyRangeCalculator} from '../../../testing/mock_key_range_calculator';
import {TreeTestHelper} from '../../../testing/tree_test_helper';

describe('LimitSkipByIndexPass', () => {
  let db: DatabaseConnection;
  let schema: Database;
  let global: Global;
  let e: BaseTable;
  let pass: LimitSkipByIndexPass;

  beforeEach(() => {
    const builder = getHrDbSchemaBuilder();
    schema = builder.getSchema();
    e = schema.table('Employee');
    pass = new LimitSkipByIndexPass();
    return builder.connect({storeType: DataStoreType.MEMORY}).then((conn) => {
      db = conn;
      global = (db as RuntimeDatabase).getGlobal();
    });
  });

  afterEach(() => {
    db.close();
  });

  function getIndexByName(table: BaseTable, indexName: string): IndexImpl {
    return (table.getIndices() as IndexImpl[])
        .filter((index) => index.name === indexName)[0];
  }

  // Tests a tree where an existing IndexRangeScanStep can be leveraged for
  // limiting and skipping results.
  it('tree1', () => {
    const treeBefore = [
      'limit(100)',
      '-skip(200)',
      '--project()',
      '---table_access_by_row_id(Employee)',
      '----index_range_scan(Employee.idx_salary, ' +
          '[unbound, 1000],[2000, unbound], reverse)',
      '',
    ].join('\n');

    const treeAfter = [
      'project()',
      '-table_access_by_row_id(Employee)',
      '--index_range_scan(Employee.idx_salary, ' +
          '[unbound, 1000],[2000, unbound], reverse, limit:100, skip:200)',
      '',
    ].join('\n');

    const constructTree = () => {
      const queryContext = new SelectContext(schema);
      queryContext.from = [e];
      queryContext.limit = 100;
      queryContext.skip = 200;
      queryContext.where = op.or(e['salary'].lte(1000), e['salary'].gte(2000));

      const limitNode = new LimitStep();
      const skipNode = new SkipStep();
      limitNode.addChild(skipNode);
      const projectNode = new ProjectStep([], null as any as BaseColumn[]);
      skipNode.addChild(projectNode);
      const tableAccessByRowIdNode =
          new TableAccessByRowIdStep(global, queryContext.from[0]);
      projectNode.addChild(tableAccessByRowIdNode);
      const child0 = (queryContext.where as PredicateNode).getChildAt(0);
      const child1 = (queryContext.where as PredicateNode).getChildAt(1);
      const indexRangeScanStep = new IndexRangeScanStep(
          global, getIndexByName(e, 'idx_salary'), new MockKeyRangeCalculator([
            (child0 as ValuePredicate).toKeyRange().getValues()[0],
            (child1 as ValuePredicate).toKeyRange().getValues()[0],
          ]),
          true);
      tableAccessByRowIdNode.addChild(indexRangeScanStep);

      return {queryContext: queryContext, root: limitNode};
    };

    TreeTestHelper.assertTreeTransformation(
        constructTree(), treeBefore, treeAfter, pass);
  });

  // Tests a tree where an existing IndexRangeScanStep exists, but it can't be
  // leveraged for limiting and skipping results, because a SelectStep exists in
  // the tree.
  it('tree_SelectStep_Unaffected', () => {
    const treeBefore = [
      'limit(100)',
      '-skip(200)',
      '--project()',
      '---select(value_pred(Employee.id lt 300))',
      '----table_access_by_row_id(Employee)',
      '-----index_range_scan(Employee.idx_salary, [unbound, unbound], reverse)',
      '',
    ].join('\n');

    const constructTree = () => {
      const queryContext = new SelectContext(schema);
      queryContext.limit = 100;
      queryContext.skip = 200;
      queryContext.from = [e];
      queryContext.where = e['id'].lt('300');

      const limitNode = new LimitStep();
      const skipNode = new SkipStep();
      limitNode.addChild(skipNode);
      const projectNode = new ProjectStep([], null as any as BaseColumn[]);
      skipNode.addChild(projectNode);
      const selectNode =
          new SelectStep((queryContext.where as Predicate).getId());
      projectNode.addChild(selectNode);
      const tableAccessByRowIdNode =
          new TableAccessByRowIdStep(global, queryContext.from[0]);
      selectNode.addChild(tableAccessByRowIdNode);
      const indexRangeScanStep = new IndexRangeScanStep(
          global, getIndexByName(e, 'idx_salary'),
          new MockKeyRangeCalculator([SingleKeyRange.all()]), true);
      tableAccessByRowIdNode.addChild(indexRangeScanStep);

      return {queryContext: queryContext, root: limitNode};
    };

    TreeTestHelper.assertTreeTransformation(
        constructTree(), treeBefore, treeBefore, pass);
  });

  // Tests a tree where an existing IndexRangeScanStep exists, but it can't be
  // leveraged for limiting and skipping results, because a GROUP_BY operation
  // exists.
  it('tree_GroupBy_Unaffected', () => {
    const treeBefore = [
      'limit(100)',
      '-skip(200)',
      '--project(Employee.id, groupBy(Employee.jobId))',
      '---table_access_by_row_id(Employee)',
      '----index_range_scan(Employee.idx_salary, ' +
          '[unbound, unbound], reverse)',
      '',
    ].join('\n');

    const constructTree = () => {
      const queryContext = new SelectContext(schema);
      queryContext.limit = 100;
      queryContext.skip = 200;

      const limitNode = new LimitStep();
      const skipNode = new SkipStep();
      limitNode.addChild(skipNode);
      const projectNode = new ProjectStep([e['id']], [e['jobId']]);
      skipNode.addChild(projectNode);
      const tableAccessByRowIdNode = new TableAccessByRowIdStep(global, e);
      projectNode.addChild(tableAccessByRowIdNode);
      const indexRangeScanStep = new IndexRangeScanStep(
          global, getIndexByName(e, 'idx_salary'),
          new MockKeyRangeCalculator([SingleKeyRange.all()]), true);
      tableAccessByRowIdNode.addChild(indexRangeScanStep);

      return {queryContext: queryContext, root: limitNode};
    };

    TreeTestHelper.assertTreeTransformation(
        constructTree(), treeBefore, treeBefore, pass);
  });

  // Tests a tree where an existing IndexRangeScanStep exists, but it can't be
  // leveraged for limiting and skipping results, because a project step that
  // includes aggregators exists.
  it('tree_Aggregators_Unaffected', () => {
    const treeBefore = [
      'limit(100)',
      '-skip(200)',
      '--project(MAX(Employee.salary),MIN(Employee.salary))',
      '---table_access_by_row_id(Employee)',
      '----index_range_scan(Employee.idx_salary, ' +
          '[unbound, unbound], reverse)',
      '',
    ].join('\n');

    const constructTree = () => {
      const queryContext = new SelectContext(schema);
      queryContext.limit = 100;
      queryContext.skip = 200;

      const limitNode = new LimitStep();
      const skipNode = new SkipStep();
      limitNode.addChild(skipNode);
      const projectNode = new ProjectStep(
          [
            fn.max(e['salary']),
            fn.min(e['salary']),
          ],
          null as any as BaseColumn[]);
      skipNode.addChild(projectNode);
      const tableAccessByRowIdNode = new TableAccessByRowIdStep(global, e);
      projectNode.addChild(tableAccessByRowIdNode);
      const indexRangeScanStep = new IndexRangeScanStep(
          global, getIndexByName(e, 'idx_salary'),
          new MockKeyRangeCalculator([SingleKeyRange.all()]), true);
      tableAccessByRowIdNode.addChild(indexRangeScanStep);

      return {queryContext: queryContext, root: limitNode};
    };

    TreeTestHelper.assertTreeTransformation(
        constructTree(), treeBefore, treeBefore, pass);
  });

  // Tests a tree where an existing IndexRangeScanStep exists, but it can't be
  // leveraged for limiting and skipping results, because an ORDER BY step
  // exists.
  it('tree_OrderBy_Unaffected', () => {
    const treeBefore = [
      'limit(100)',
      '-skip(200)',
      '--project()',
      '---order_by(Employee.salary DESC)',
      '----table_access_by_row_id(Employee)',
      '-----index_range_scan(Employee.idx_salary, [unbound, unbound], reverse)',
      '',
    ].join('\n');

    const constructTree = () => {
      const queryContext = new SelectContext(schema);
      queryContext.limit = 100;
      queryContext.skip = 200;

      const limitNode = new LimitStep();
      const skipNode = new SkipStep();
      limitNode.addChild(skipNode);
      const projectNode = new ProjectStep([], null as any as BaseColumn[]);
      skipNode.addChild(projectNode);
      const orderByNode = new OrderByStep([{
        column: e['salary'],
        order: Order.DESC,
      }]);
      projectNode.addChild(orderByNode);
      const tableAccessByRowIdNode = new TableAccessByRowIdStep(global, e);
      orderByNode.addChild(tableAccessByRowIdNode);
      const indexRangeScanStep = new IndexRangeScanStep(
          global, getIndexByName(e, 'idx_salary'),
          new MockKeyRangeCalculator([SingleKeyRange.all()]), true);
      tableAccessByRowIdNode.addChild(indexRangeScanStep);

      return {queryContext: queryContext, root: limitNode};
    };

    TreeTestHelper.assertTreeTransformation(
        constructTree(), treeBefore, treeBefore, pass);
  });
});
