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
import {bind} from '../../../lib/base/bind';
import {op} from '../../../lib/fn/op';
import {PredicateNode} from '../../../lib/pred/predicate_node';
import {LogicalPlanFactory} from '../../../lib/proc/lp/logical_plan_factory';
import {DeleteBuilder} from '../../../lib/query/delete_builder';
import {SelectBuilder} from '../../../lib/query/select_builder';
import {UpdateBuilder} from '../../../lib/query/update_builder';
import {TreeHelper} from '../../../lib/structs/tree_helper';
import {MockEnv} from '../../../testing/mock_env';
import {getMockSchemaBuilder} from '../../../testing/mock_schema_builder';

const assert = chai.assert;

describe('LogicalPlanFactory', () => {
  let logicalPlanFactory: LogicalPlanFactory;
  let env: MockEnv;

  beforeEach(() => {
    env = new MockEnv(getMockSchemaBuilder().getSchema());
    return env
      .init()
      .then(() => (logicalPlanFactory = new LogicalPlanFactory()));
  });

  // Tests that the generated logical query plan for a simple DELETE query is as
  // expected and also that the query object itself is not mutated as part of
  // generating a plan. This is essential such that calling
  // DefaultBuilder#explain() does not have any side effects which would prevent
  // a subsequent call to DefaultBuilder#exec() from operating on the same query
  // object.
  it('create_DeletePlan', () => {
    const table = env.schema.table('tableA');
    const queryBuilder = new DeleteBuilder(env.global);
    queryBuilder
      .from(table)
      .where(op.and(table.col('id').eq('id'), table.col('name').eq('name')));

    const query = queryBuilder.getQuery();
    assert.equal(2, (query.where as PredicateNode).getChildCount());

    const expectedTree = [
      'delete(tableA)',
      '-select(value_pred(tableA.id eq id))',
      '--select(value_pred(tableA.name eq name))',
      '---table_access(tableA)',
      '',
    ].join('\n');

    const logicalPlan = logicalPlanFactory.create(query);
    assert.equal(expectedTree, TreeHelper.toString(logicalPlan.getRoot()));
    assert.equal(2, (query.where as PredicateNode).getChildCount());
  });

  // Tests that the generated logical query plan for a simple SELECT query is as
  // expected and also that the query object itself is not mutated as part of
  // generating a plan.
  it('create_SelectPlan', () => {
    const table = env.schema.table('tableA');
    const queryBuilder = new SelectBuilder(env.global, []);
    queryBuilder
      .from(table)
      .where(op.and(table.col('id').eq('id'), table.col('name').eq('name')));

    const query = queryBuilder.getQuery();
    assert.equal(2, (query.where as PredicateNode).getChildCount());

    const expectedTree = [
      'project()',
      '-select(value_pred(tableA.id eq id))',
      '--select(value_pred(tableA.name eq name))',
      '---table_access(tableA)',
      '',
    ].join('\n');

    const logicalPlan = logicalPlanFactory.create(query);
    assert.equal(expectedTree, TreeHelper.toString(logicalPlan.getRoot()));
    assert.equal(2, (query.where as PredicateNode).getChildCount());
  });

  // Tests that the generated logical query plan for a SELECT query with
  // "SKIP 0" does not include a "skip" node, since it has no effect on the
  // query results.
  it('create_SelectPlan_SkipZero', () => {
    const table = env.schema.table('tableA');
    const queryBuilder = new SelectBuilder(env.global, []);
    queryBuilder.from(table).skip(0);

    const query = queryBuilder.getQuery();
    const expectedTree = ['project()', '-table_access(tableA)', ''].join('\n');

    const logicalPlan = logicalPlanFactory.create(query);
    assert.equal(expectedTree, TreeHelper.toString(logicalPlan.getRoot()));
  });

  // Tests that the generated logical query plan for a SELECT query with "SKIP
  // ?" includes a "skip" node.
  it('create_SelectPlan_Skip_WithBind', () => {
    const table = env.schema.table('tableA');
    const queryBuilder = new SelectBuilder(env.global, []);
    queryBuilder.from(table).skip(bind(0));

    const query = (queryBuilder.bind([3]) as SelectBuilder).getQuery();
    const expectedTree = [
      'skip(3)',
      '-project()',
      '--table_access(tableA)',
      '',
    ].join('\n');

    const logicalPlan = logicalPlanFactory.create(query);
    assert.equal(expectedTree, TreeHelper.toString(logicalPlan.getRoot()));
  });

  // Tests that the generated logical query plan for a SELECT query with
  // "SKIP ?" when the value is bound to zero includes a "skip" node. Although
  // skipping 0 has no effect on query results, the bound value may change on
  // subsequent invocations.
  it('create_SelectPlan_SkipZero_WithBind', () => {
    const table = env.schema.table('tableA');
    const queryBuilder = new SelectBuilder(env.global, []);
    queryBuilder.from(table).skip(bind(0));

    const query = (queryBuilder.bind([0]) as SelectBuilder).getQuery();

    const expectedTree = [
      'skip(0)',
      '-project()',
      '--table_access(tableA)',
      '',
    ].join('\n');

    const logicalPlan = logicalPlanFactory.create(query);
    assert.equal(expectedTree, TreeHelper.toString(logicalPlan.getRoot()));
  });

  // Tests that the generated logical query plan for a SELECT query with
  // "LIMIT 0" does in fact include a "limit" node.
  it('create_SelectPlan_LimitZero', () => {
    const table = env.schema.table('tableA');
    const queryBuilder = new SelectBuilder(env.global, []);
    queryBuilder.from(table).limit(0);

    const query = queryBuilder.getQuery();
    const expectedTree = [
      'limit(0)',
      '-project()',
      '--table_access(tableA)',
      '',
    ].join('\n');

    const logicalPlan = logicalPlanFactory.create(query);
    assert.equal(expectedTree, TreeHelper.toString(logicalPlan.getRoot()));
  });

  // Tests that the generated logical query plan for a simple UPDATE query is as
  // expected and also that the query object itself is not mutated as part of
  // generating a plan.
  it('create_UpdatePlan', () => {
    const table = env.schema.table('tableA');

    const queryBuilder = new UpdateBuilder(env.global, table);
    queryBuilder
      .set(table.col('name'), 'NewName')
      .where(op.and(table.col('id').eq('id'), table.col('name').eq('name')));

    const query = queryBuilder.getQuery();
    assert.equal(2, (query.where as PredicateNode).getChildCount());

    const expectedTree = [
      'update(tableA)',
      '-select(combined_pred_and)',
      '--table_access(tableA)',
      '',
    ].join('\n');

    const logicalPlan = logicalPlanFactory.create(query);
    assert.equal(expectedTree, TreeHelper.toString(logicalPlan.getRoot()));

    assert.equal(2, (query.where as PredicateNode).getChildCount());
  });
});
