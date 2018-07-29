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

import {Order} from '../../../lib/base/enum';
import {op} from '../../../lib/fn/op';
import {Predicate} from '../../../lib/pred/predicate';
import {CrossProductNode} from '../../../lib/proc/lp/cross_product_node';
import {OrderByNode} from '../../../lib/proc/lp/orderby_node';
import {PushDownSelectionsPass} from '../../../lib/proc/lp/push_down_selections_pass';
import {SelectNode} from '../../../lib/proc/lp/select_node';
import {TableAccessNode} from '../../../lib/proc/lp/table_access_node';
import {SelectContext} from '../../../lib/query/select_context';
import {BaseTable} from '../../../lib/schema/base_table';
import {DatabaseSchema} from '../../../lib/schema/database_schema';
import {getHrDbSchemaBuilder} from '../../../testing/hr_schema/hr_schema_builder';
import {TreeTestHelper} from '../../../testing/tree_test_helper';

describe('ImplicitJoinsPass', () => {
  let schema: DatabaseSchema;
  let pass: PushDownSelectionsPass;

  before(() => {
    schema = getHrDbSchemaBuilder().getSchema();
    pass = new PushDownSelectionsPass();
  });

  // Tests a simple tree where 3 ValuePredicate selections are pushed below a
  // cross ploduct node. Two of the selections are pushed on one branch and the
  // other selections are pushed on the other.
  it('tree_ValuePredicates1', () => {
    const e = schema.table('Employee');
    const j = schema.table('Job');

    const hireDate = new Date(1422667933572);

    const treeBefore = [
      'order_by(Employee.id ASC)',
      '-select(value_pred(Employee.salary gt 1000))',
      '--select(value_pred(Job.minSalary gt 100))',
      `---select(value_pred(Employee.hireDate lt ${hireDate.toString()}))`,
      '----cross_product',
      '-----table_access(Employee)',
      '-----table_access(Job)',
      '',
    ].join('\n');

    const treeAfter = [
      'order_by(Employee.id ASC)',
      '-cross_product',
      '--select(value_pred(Employee.salary gt 1000))',
      `---select(value_pred(Employee.hireDate lt ${hireDate.toString()}))`,
      '----table_access(Employee)',
      '--select(value_pred(Job.minSalary gt 100))',
      '---table_access(Job)',
      '',
    ].join('\n');

    const constructTree = () => {
      const queryContext = new SelectContext(schema);
      queryContext.from = [e, j];
      const predicate1 = e['salary'].gt(1000);
      const predicate2 = j['minSalary'].gt(100);
      const predicate3 = e['hireDate'].lt(hireDate);
      queryContext.where = op.and(predicate1, predicate2, predicate3);
      queryContext.orderBy = [{column: e['id'], order: Order.ASC}];

      const orderByNode = new OrderByNode(queryContext.orderBy);
      const selectNode1 = new SelectNode(predicate1);
      orderByNode.addChild(selectNode1);
      const selectNode2 = new SelectNode(predicate2);
      selectNode1.addChild(selectNode2);
      const selectNode3 = new SelectNode(predicate3);
      selectNode2.addChild(selectNode3);
      const crossProductNode = new CrossProductNode();
      selectNode3.addChild(crossProductNode);
      queryContext.from.forEach((tableSchema) => {
        crossProductNode.addChild(new TableAccessNode(tableSchema));
      });

      return {queryContext: queryContext, root: orderByNode};
    };

    TreeTestHelper.assertTreeTransformation(
        constructTree(), treeBefore, treeAfter, pass);
  });

  // Testing case where two ValuePredicate select nodes exist, but they can't be
  // pushed further down. Ensuring that no endless recursion occurs (swapping
  // the select nodes with each other indefinitely).
  it('tree_ValuePredicates2_Unaffected', () => {
    const e = schema.table('Employee');

    const treeBefore = [
      'order_by(Employee.id ASC)',
      '-select(value_pred(Employee.salary gt 10))',
      '--select(value_pred(Employee.salary lt 20))',
      '---table_access(Employee)',
      '',
    ].join('\n');

    const constructTree = () => {
      const queryContext = new SelectContext(schema);
      queryContext.from = [e];
      const predicate1 = e['salary'].gt(10);
      const predicate2 = e['salary'].lt(20);
      queryContext.where = op.and(predicate1, predicate2);
      queryContext.orderBy = [{column: e['id'], order: Order.ASC}];

      const orderByNode = new OrderByNode(queryContext.orderBy);
      const selectNode1 = new SelectNode(predicate1);
      orderByNode.addChild(selectNode1);
      const selectNode2 = new SelectNode(predicate2);
      selectNode1.addChild(selectNode2);
      const tableAccess = new TableAccessNode(queryContext.from[0]);
      selectNode2.addChild(tableAccess);

      return {queryContext: queryContext, root: orderByNode};
    };

    TreeTestHelper.assertTreeTransformation(
        constructTree(), treeBefore, treeBefore, pass);
  });

  // Ensuring that the order of cross-product/join children is not changed
  // during re-writing.
  it('tree_ValuePredicates3', () => {
    const e = schema.table('Employee');
    const j = schema.table('Job');

    const treeBefore = [
      'order_by(Employee.id ASC)',
      '-select(value_pred(Employee.salary gt 10))',
      '--cross_product',
      '---table_access(Employee)',
      '---table_access(Job)',
      '',
    ].join('\n');

    const treeAfter = [
      'order_by(Employee.id ASC)',
      '-cross_product',
      '--select(value_pred(Employee.salary gt 10))',
      '---table_access(Employee)',
      '--table_access(Job)',
      '',
    ].join('\n');

    const constructTree = () => {
      const queryContext = new SelectContext(schema);
      queryContext.from = [e, j];
      queryContext.where = e['salary'].gt(10);
      queryContext.orderBy = [{column: e['id'], order: Order.ASC}];

      const orderByNode = new OrderByNode(queryContext.orderBy);
      const selectNode = new SelectNode(queryContext.where as Predicate);
      orderByNode.addChild(selectNode);
      const crossProductNode = new CrossProductNode();
      selectNode.addChild(crossProductNode);
      queryContext.from.forEach(
          (tableSchema) =>
              crossProductNode.addChild(new TableAccessNode(tableSchema)));

      return {queryContext: queryContext, root: orderByNode};
    };

    TreeTestHelper.assertTreeTransformation(
        constructTree(), treeBefore, treeAfter, pass);
  });

  // Tests a tree where two value predicates and two join predicates exist (one
  // outer, one inner).
  // It ensures that
  //  1) The two value predicates are not pushed below the outer join
  //  predicates.
  //  2) The join predicate closer to the root is pushed below the
  //  outer join predicate further from the root..
  it('tree_MixedJoinPredicates_WithWhere', () => {
    const treeBefore = [
      'select(value_pred(Department.id eq null))',
      '-select(value_pred(Job.id eq null))',
      '--select(join_pred(Employee.jobId eq Job.id))',
      '---select(join_pred(Employee.departmentId eq Department.id))',
      '----cross_product',
      '-----cross_product',
      '------table_access(Employee)',
      '------table_access(Job)',
      '-----table_access(Department)',
      '',
    ].join('\n');

    const treeAfter = [
      'select(value_pred(Department.id eq null))',
      '-select(value_pred(Job.id eq null))',
      '--select(join_pred(Employee.departmentId eq Department.id))',
      '---cross_product',
      '----select(join_pred(Employee.jobId eq Job.id))',
      '-----cross_product',
      '------table_access(Employee)',
      '------table_access(Job)',
      '----table_access(Department)',
      '',
    ].join('\n');

    const constructTree = () => {
      const d = schema.table('Department');
      const e = schema.table('Employee');
      const j = schema.table('Job');

      const queryContext = new SelectContext(schema);
      queryContext.from = [e, j, d];
      const innerJoinPredicate = e['jobId'].eq(j['id']);
      const outerJoinPredicate = e['departmentId'].eq(d['id']);
      const valuePredicate1 = d['id'].isNull();
      const valuePredicate2 = j['id'].isNull();
      queryContext.where = op.and(
          valuePredicate1, valuePredicate2, innerJoinPredicate,
          outerJoinPredicate);
      queryContext.outerJoinPredicates = new Set<number>();
      queryContext.outerJoinPredicates.add(outerJoinPredicate.getId());

      const crossProductNode1 = new CrossProductNode();
      crossProductNode1.addChild(new TableAccessNode(e));
      crossProductNode1.addChild(new TableAccessNode(j));
      const crossProductNode2 = new CrossProductNode();
      crossProductNode2.addChild(crossProductNode1);
      crossProductNode2.addChild(new TableAccessNode(d));

      const selectNode1 = new SelectNode(valuePredicate1);
      const selectNode2 = new SelectNode(valuePredicate2);
      const selectNode3 = new SelectNode(innerJoinPredicate);
      const selectNode4 = new SelectNode(outerJoinPredicate);
      selectNode1.addChild(selectNode2);
      selectNode2.addChild(selectNode3);
      selectNode3.addChild(selectNode4);
      selectNode4.addChild(crossProductNode2);

      return {queryContext: queryContext, root: selectNode1};
    };

    TreeTestHelper.assertTreeTransformation(
        constructTree(), treeBefore, treeAfter, pass);
  });

  // Tests a tree that involves a 3 table join. It ensures that the
  // JoinPredicate nodes are pushed down until they become parents of the
  // appropriate cross product node.
  it('tree_JoinPredicates', () => {
    const treeBefore = [
      'select(join_pred(Employee.jobId eq Job.id))',
      '-select(join_pred(Employee.departmentId eq Department.id))',
      '--cross_product',
      '---cross_product',
      '----table_access(Employee)',
      '----table_access(Job)',
      '---table_access(Department)',
      '',
    ].join('\n');

    const treeAfter = [
      'select(join_pred(Employee.departmentId eq Department.id))',
      '-cross_product',
      '--select(join_pred(Employee.jobId eq Job.id))',
      '---cross_product',
      '----table_access(Employee)',
      '----table_access(Job)',
      '--table_access(Department)',
      '',
    ].join('\n');

    const constructTree = () => {
      const d = schema.table('Department');
      const e = schema.table('Employee');
      const j = schema.table('Job');

      const queryContext = new SelectContext(schema);
      queryContext.from = [e, j, d];
      const predicate1 = e['departmentId'].eq(d['id']);
      const predicate2 = e['jobId'].eq(j['id']);
      queryContext.where = op.and(predicate1, predicate2);

      const crossProductNode1 = new CrossProductNode();
      crossProductNode1.addChild(new TableAccessNode(queryContext.from[0]));
      crossProductNode1.addChild(new TableAccessNode(queryContext.from[1]));
      const crossProductNode2 = new CrossProductNode();
      crossProductNode2.addChild(crossProductNode1);
      crossProductNode2.addChild(new TableAccessNode(queryContext.from[2]));

      const selectNode1 = new SelectNode(predicate1);
      selectNode1.addChild(crossProductNode2);
      const selectNode2 = new SelectNode(predicate2);
      selectNode2.addChild(selectNode1);

      return {queryContext: queryContext, root: selectNode2};
    };

    TreeTestHelper.assertTreeTransformation(
        constructTree(), treeBefore, treeAfter, pass);
  });

  // Tests a tree that involves a 5 table join. It ensures that all predicate
  // nodes (both JoinPredicate and ValuePredicate) are pushed as down in the
  // tree as possible.
  it('tree_JoinPredicates2', () => {
    const c = schema.table('Country');
    const d = schema.table('Department');
    const e = schema.table('Employee');
    const jh = schema.table('JobHistory');
    const j = schema.table('Job');

    const treeBefore = [
      'select(join_pred(Country.id eq Department.id))',
      '-select(value_pred(Employee.id eq empId))',
      '--select(join_pred(Employee.departmentId eq Department.id))',
      '---select(join_pred(Employee.jobId eq Job.id))',
      '----select(join_pred(JobHistory.jobId eq Job.id))',
      '-----cross_product',
      '------cross_product',
      '-------cross_product',
      '--------cross_product',
      '---------table_access(Employee)',
      '---------table_access(Job)',
      '--------table_access(Department)',
      '-------table_access(JobHistory)',
      '------table_access(Country)',
      '',
    ].join('\n');

    const treeAfter = [
      'select(join_pred(Country.id eq Department.id))',
      '-cross_product',
      '--select(join_pred(JobHistory.jobId eq Job.id))',
      '---cross_product',
      '----select(join_pred(Employee.departmentId eq Department.id))',
      '-----cross_product',
      '------select(join_pred(Employee.jobId eq Job.id))',
      '-------cross_product',
      '--------select(value_pred(Employee.id eq empId))',
      '---------table_access(Employee)',
      '--------table_access(Job)',
      '------table_access(Department)',
      '----table_access(JobHistory)',
      '--table_access(Country)',
      '',
    ].join('\n');

    const constructTree = () => {
      const queryContext = new SelectContext(schema);
      queryContext.from = [e, j, d, jh, c];
      const predicate1 = jh['jobId'].eq(j['id']);
      const predicate2 = e['jobId'].eq(j['id']);
      const predicate3 = e['departmentId'].eq(d['id']);
      const predicate4 = e['id'].eq('empId');
      const predicate5 = c['id'].eq(d['id']);
      queryContext.where =
          op.and(predicate1, predicate2, predicate3, predicate4, predicate5);

      const crossProductNode1 = new CrossProductNode();
      crossProductNode1.addChild(new TableAccessNode(e));
      crossProductNode1.addChild(new TableAccessNode(j));

      const crossProductNode2 = new CrossProductNode();
      crossProductNode2.addChild(crossProductNode1);
      crossProductNode2.addChild(new TableAccessNode(d));

      const crossProductNode3 = new CrossProductNode();
      crossProductNode3.addChild(crossProductNode2);
      crossProductNode3.addChild(new TableAccessNode(jh));

      const crossProductNode4 = new CrossProductNode();
      crossProductNode4.addChild(crossProductNode3);
      crossProductNode4.addChild(new TableAccessNode(c));

      const selectStep1 = new SelectNode(predicate1);
      selectStep1.addChild(crossProductNode4);
      const selectStep2 = new SelectNode(predicate2);
      selectStep2.addChild(selectStep1);
      const selectStep3 = new SelectNode(predicate3);
      selectStep3.addChild(selectStep2);
      const selectStep4 = new SelectNode(predicate4);
      selectStep4.addChild(selectStep3);
      const selectStep5 = new SelectNode(predicate5);
      selectStep5.addChild(selectStep4);

      return {queryContext: queryContext, root: selectStep5};
    };

    TreeTestHelper.assertTreeTransformation(
        constructTree(), treeBefore, treeAfter, pass);
  });

  // Tests a tree that involves a self-table join and table aliases. The value
  // predicate that refers to only one of the two tables is expected to be
  // pushed below the cross product node, whereas the join predicate refers to
  // both tables and therefore should not be pushed further down.
  it('tree_JoinPredicates3', () => {
    const j1 = schema.table('Job').as('j1');
    const j2 = schema.table('Job').as('j2');

    const treeBefore = [
      'select(join_pred(j1.maxSalary eq j2.minSalary))',
      '-select(value_pred(j1.maxSalary lt 30000))',
      '--cross_product',
      '---table_access(Job as j1)',
      '---table_access(Job as j2)',
      '',
    ].join('\n');

    const treeAfter = [
      'select(join_pred(j1.maxSalary eq j2.minSalary))',
      '-cross_product',
      '--select(value_pred(j1.maxSalary lt 30000))',
      '---table_access(Job as j1)',
      '--table_access(Job as j2)',
      '',
    ].join('\n');

    const constructTree = () => {
      const queryContext = new SelectContext(schema);
      queryContext.from = [j1 as BaseTable, j2 as BaseTable];
      const predicate1 = j1['maxSalary'].lt(30000);
      const predicate2 = j1['maxSalary'].eq(j2['minSalary']);
      queryContext.where = op.and(predicate1, predicate2);

      const crossProductNode = new CrossProductNode();
      crossProductNode.addChild(new TableAccessNode(j1 as BaseTable));
      crossProductNode.addChild(new TableAccessNode(j2 as BaseTable));
      const selectNode1 = new SelectNode(predicate1);
      selectNode1.addChild(crossProductNode);
      const selectNode2 = new SelectNode(predicate2);
      selectNode2.addChild(selectNode1);

      return {queryContext: queryContext, root: selectNode2};
    };

    TreeTestHelper.assertTreeTransformation(
        constructTree(), treeBefore, treeAfter, pass);
  });

  // Tests a tree that involves a left outer join and also has an additional
  // value predicate. It ensures that the value predicate is not pushed below
  // the join predicate, such that the join operation is performed before the
  // value predicate is applied.
  it('tree_OuterJoinPredicate_Unaffected', () => {
    const r = schema.table('Region');
    const c = schema.table('Country');

    const treeBefore = [
      'select(value_pred(Country.id eq 1))',
      '-select(join_pred(Region.id eq Country.regionId))',
      '--cross_product',
      '---table_access(Region)',
      '---table_access(Country)',
      '',
    ].join('\n');

    const constructTree = () => {
      const queryContext = new SelectContext(schema);
      queryContext.from = [r, c];
      const valuePredicate = c['id'].eq(1);
      const joinPredicate = r['id'].eq(c['regionId']);
      queryContext.where = op.and(valuePredicate, joinPredicate);
      queryContext.outerJoinPredicates = new Set<number>();
      queryContext.outerJoinPredicates.add(joinPredicate.getId());

      const selectNode1 = new SelectNode(valuePredicate);
      const selectNode2 = new SelectNode(joinPredicate);
      const crossProductNode = new CrossProductNode();

      selectNode1.addChild(selectNode2);
      selectNode2.addChild(crossProductNode);
      crossProductNode.addChild(new TableAccessNode(r));
      crossProductNode.addChild(new TableAccessNode(c));

      return {queryContext: queryContext, root: selectNode1};
    };

    TreeTestHelper.assertTreeTransformation(
        constructTree(), treeBefore, treeBefore, pass);
  });

  // Tests a tree where to OR predicates exist, each one refers to a single
  // table. They should both be pushed down below the cross-product node, and
  // specifically towards the branch that matches the table each predicate
  // refers to.
  it('tree_CombinedPredicates_Or', () => {
    const e = schema.table('Employee');
    const j = schema.table('Job');

    const treeBefore = [
      'order_by(Employee.id ASC)',
      '-select(combined_pred_or)',
      '--select(combined_pred_or)',
      '---cross_product',
      '----table_access(Employee)',
      '----table_access(Job)',
      '',
    ].join('\n');

    const treeAfter = [
      'order_by(Employee.id ASC)',
      '-cross_product',
      '--select(combined_pred_or)',
      '---table_access(Employee)',
      '--select(combined_pred_or)',
      '---table_access(Job)',
      '',
    ].join('\n');

    const constructTree = () => {
      const queryContext = new SelectContext(schema);
      queryContext.from = [e, j];
      const predicate1 =
          op.or(e['salary'].gt(1000), e['commissionPercent'].gt(10));
      const predicate2 = op.or(j['minSalary'].gt(100), j['maxSalary'].gt(200));
      queryContext.where = op.and(predicate1, predicate2);
      queryContext.orderBy = [{column: e['id'], order: Order.ASC}];

      const orderByNode = new OrderByNode(queryContext.orderBy);
      const selectNode1 = new SelectNode(predicate1);
      const selectNode2 = new SelectNode(predicate2);
      const crossProductNode = new CrossProductNode();
      orderByNode.addChild(selectNode1);
      selectNode1.addChild(selectNode2);
      selectNode2.addChild(crossProductNode);
      queryContext.from.forEach((tableSchema) => {
        crossProductNode.addChild(new TableAccessNode(tableSchema));
      });

      return {queryContext: queryContext, root: orderByNode};
    };

    TreeTestHelper.assertTreeTransformation(
        constructTree(), treeBefore, treeAfter, pass);
  });
});
