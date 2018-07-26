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

import {DataStoreType} from '../../../lib/base/enum';
import {Global} from '../../../lib/base/global';
import {op} from '../../../lib/fn/op';
import {Predicate} from '../../../lib/pred/predicate';
import {IndexRangeScanStep} from '../../../lib/proc/pp/index_range_scan_step';
import {JoinStep} from '../../../lib/proc/pp/join_step';
import {MultiColumnOrPass} from '../../../lib/proc/pp/multi_column_or_pass';
import {ProjectStep} from '../../../lib/proc/pp/project_step';
import {SelectStep} from '../../../lib/proc/pp/select_step';
import {TableAccessByRowIdStep} from '../../../lib/proc/pp/table_access_by_row_id_step';
import {TableAccessFullStep} from '../../../lib/proc/pp/table_access_full_step';
import {RuntimeDatabase} from '../../../lib/proc/runtime_database';
import {SelectContext} from '../../../lib/query/select_context';
import {BaseColumn} from '../../../lib/schema/base_column';
import {Database} from '../../../lib/schema/database';
import {Table} from '../../../lib/schema/table';
import {getHrDbSchemaBuilder} from '../../../testing/hr_schema/hr_schema_builder';
import {MockKeyRangeCalculator} from '../../../testing/mock_key_range_calculator';
import {TestTree, TreeTestHelper} from '../../../testing/tree_test_helper';

describe('MultiColumnOrPass', () => {
  let db: RuntimeDatabase;
  let schema: Database;
  let e: Table;
  let j: Table;
  let pass: MultiColumnOrPass;
  let global: Global;

  beforeEach(async () => {
    db = await getHrDbSchemaBuilder().connect(
             {storeType: DataStoreType.MEMORY}) as RuntimeDatabase;
    global = db.getGlobal();
    schema = db.getSchema();
    e = schema.table('Employee');
    j = schema.table('Job');
    pass = new MultiColumnOrPass(global);
  });

  // Tests a simple tree, where only one OR predicate exists and it can leverage
  // indices.
  it('singleOrPredicate', () => {
    const treeBefore = [
      'project()',
      '-select(combined_pred_or)',
      '--table_access(Employee)',
      '',
    ].join('\n');

    const treeAfter = [
      'project()',
      '-table_access_by_row_id(Employee)',
      '--multi_index_range_scan()',
      '---index_range_scan(Employee.pkEmployee, [100, 100], natural)',
      '---index_range_scan(Employee.idx_salary, [200, 200], natural)',
      '',
    ].join('\n');

    const tree = constructTreeWithPredicates(
        [op.or(e['id'].eq(100), e['salary'].eq(200))]);
    TreeTestHelper.assertTreeTransformation(tree, treeBefore, treeAfter, pass);
  });

  // Tests a tree where two separate OR predicates exist for the same table and
  // either of them could potentially be chosen by the optimizer. Currently
  // optimizer chooses the first encountered predicate that is eligible, without
  // comparing the cost two subsequent candidate prediacates.
  it('multipleOrPredicates_AllIndexed', () => {
    const treeBefore = [
      'project()',
      '-select(combined_pred_or)',
      '--select(combined_pred_or)',
      '---table_access(Employee)',
      '',
    ].join('\n');

    const treeAfter = [
      'project()',
      '-select(combined_pred_or)',
      '--table_access_by_row_id(Employee)',
      '---multi_index_range_scan()',
      '----index_range_scan(Employee.pkEmployee, (100, unbound], natural)',
      '----index_range_scan(Employee.idx_salary, (200, unbound], natural)',
      '',
    ].join('\n');

    const tree = constructTreeWithPredicates([
      op.or(e['id'].gt(100), e['salary'].gt(200)),
      op.or(e['jobId'].gt(300), e['departmentId'].gt(400)),
    ]);
    TreeTestHelper.assertTreeTransformation(tree, treeBefore, treeAfter, pass);
  });

  // Tests a tree where two separate OR predicates exist for the same table, but
  // only one of them could potentially be chosen by the optimizer. Ensures that
  // the optimizer finds that predicate and optimizes it.
  it('multipleOrPredicates_SomeIndexed', () => {
    const treeBefore = [
      'project()',
      '-select(combined_pred_or)',
      '--select(combined_pred_or)',
      '---table_access(Employee)',
      '',
    ].join('\n');

    const treeAfter = [
      'project()',
      '-select(combined_pred_or)',
      '--table_access_by_row_id(Employee)',
      '---multi_index_range_scan()',
      '----index_range_scan(Employee.fk_JobId, (300, unbound], natural)',
      '----index_range_scan(' +
          'Employee.fk_DepartmentId, (400, unbound], natural)',
      '',
    ].join('\n');

    const tree = constructTreeWithPredicates([
      op.or(e['id'].gt(100), e['commissionPercent'].gt(200)),
      op.or(e['jobId'].gt(300), e['departmentId'].gt(400)),
    ]);
    TreeTestHelper.assertTreeTransformation(tree, treeBefore, treeAfter, pass);
  });

  // Constructs a tree with multiple predicates.
  function constructTreeWithPredicates(predicates: Predicate[]): TestTree {
    const queryContext = new SelectContext(schema);
    queryContext.from = [e];
    queryContext.where = op.and.apply(null, predicates);

    const tableAccessNode =
        new TableAccessFullStep(global, queryContext.from[0]);
    const selectNodes =
        predicates.map((predicate) => new SelectStep(predicate.getId()));
    const projectNode = new ProjectStep([], null as any as BaseColumn[]);
    let lastSelectNode = selectNodes[0];
    projectNode.addChild(lastSelectNode);
    for (let i = 1; i < selectNodes.length; i++) {
      lastSelectNode.addChild(selectNodes[i]);
      lastSelectNode = selectNodes[i];
    }
    selectNodes[selectNodes.length - 1].addChild(tableAccessNode);

    return {
      queryContext: queryContext,
      root: projectNode,
    };
  }

  // Tests a tree where an OR predicate that refers to multiple tables exists.
  // Ensures that the optimization is not be applied and the tree remains
  // unaffected.
  it('crossTableOrPredicates_Unaffected', () => {
    const treeBefore = [
      'project()',
      '-select(combined_pred_or)',
      '--join(type: inner, impl: hash, join_pred(Job.id eq Employee.jobId))',
      '---table_access(Employee)',
      '---table_access(Job)',
      '',
    ].join('\n');

    const constructTree = () => {
      const queryContext = new SelectContext(schema);
      queryContext.from = [e, j];
      const joinPredicate = j['id'].eq(e['jobId']);
      const orPredicate = op.or(e['salary'].lte(1000), j['maxSalary'].gte(200));
      queryContext.where = op.and(orPredicate, joinPredicate);

      const projectStep = new ProjectStep([], null as any as BaseColumn[]);
      const selectStep = new SelectStep(orPredicate.getId());
      const joinStep = new JoinStep(global, joinPredicate, false);
      const tableAccessStep1 =
          new TableAccessFullStep(global, queryContext.from[0]);
      const tableAccessStep2 =
          new TableAccessFullStep(global, queryContext.from[1]);

      projectStep.addChild(selectStep);
      selectStep.addChild(joinStep);
      joinStep.addChild(tableAccessStep1);
      joinStep.addChild(tableAccessStep2);

      return {
        queryContext: queryContext,
        root: projectStep,
      };
    };

    TreeTestHelper.assertTreeTransformation(
        constructTree(), treeBefore, treeBefore, pass);
  });

  // Test the case where a predicate of the form AND(c1, OR(c2, c3)) exists and
  // c1 has already been optimized by previous optimization passes. This
  // optimization does not apply.
  it('alreadyOptimized_Unaffected', () => {
    const treeBefore = [
      'project()',
      '-select(combined_pred_or)',
      '--table_access_by_row_id(Job)',
      '---index_range_scan(Job.pkJob, [1, 1], natural)',
      '',
    ].join('\n');

    const constructTree = () => {
      const queryContext = new SelectContext(schema);
      queryContext.from = [j];
      const simplePredicate = j['id'].eq('1');
      const orPredicate = op.or(j['id'].eq('2'), j['maxSalary'].eq(100));
      queryContext.where = op.and(simplePredicate, orPredicate);

      const projectStep = new ProjectStep([], null as any as BaseColumn[]);
      const selectStep = new SelectStep(orPredicate.getId());
      const tableAccessByRowIdStep =
          new TableAccessByRowIdStep(global, queryContext.from[0]);
      const indexRangeScanStep = new IndexRangeScanStep(
          global, j['id'].getIndex(),
          new MockKeyRangeCalculator(simplePredicate.toKeyRange()), false);

      projectStep.addChild(selectStep);
      selectStep.addChild(tableAccessByRowIdStep);
      tableAccessByRowIdStep.addChild(indexRangeScanStep);

      return {
        queryContext: queryContext,
        root: projectStep,
      };
    };

    TreeTestHelper.assertTreeTransformation(
        constructTree(), treeBefore, treeBefore, pass);
  });
});
