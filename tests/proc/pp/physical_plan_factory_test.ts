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
import * as sinon from 'sinon';

import { op } from '../../../lib/fn/op';
import { PredicateNode } from '../../../lib/pred/predicate_node';
import { DeleteNode } from '../../../lib/proc/lp/delete_node';
import { LogicalQueryPlan } from '../../../lib/proc/lp/logical_query_plan';
import { SelectNode } from '../../../lib/proc/lp/select_node';
import { TableAccessNode } from '../../../lib/proc/lp/table_access_node';
import { PhysicalPlanFactory } from '../../../lib/proc/pp/physical_plan_factory';
import { PhysicalQueryPlanNode } from '../../../lib/proc/pp/physical_query_plan_node';
import { DeleteContext } from '../../../lib/query/delete_context';
import { BaseColumn } from '../../../lib/schema/base_column';
import { Table } from '../../../lib/schema/table';
import { TreeHelper } from '../../../lib/structs/tree_helper';
import { TreeNode } from '../../../lib/structs/tree_node';
import { MockEnv } from '../../../testing/mock_env';
import { getMockSchemaBuilder } from '../../../testing/mock_schema_builder';
import { TestUtil } from '../../../testing/test_util';

const assert = chai.assert;

describe('PhysicalPlanFactory', () => {
  let sandbox: sinon.SinonSandbox;
  let physicalPlanFactory: PhysicalPlanFactory;
  let env: MockEnv;

  beforeEach(() => {
    sandbox = sinon.createSandbox();
    env = new MockEnv(getMockSchemaBuilder().getSchema());
    return env.init().then(() => {
      physicalPlanFactory = new PhysicalPlanFactory(env.global);
    });
  });

  afterEach(() => {
    sandbox.restore();
  });

  // Tests that the conversion of a DELETE logical query plan to a physical
  // query plan in the case where multiple predicates exist is performed as
  // expected.
  // TODO(dpapad): Add similar test for remaining query types (INSERT,
  // INSERT_OR_REPLACE, UPDATE, SELECT).
  it('create_DeletePlan', () => {
    const logicalTree = [
      'delete(tableA)',
      '-select(value_pred(tableA.id eq id))',
      '--select(value_pred(tableA.name eq name))',
      '---table_access(tableA)',
      '',
    ].join('\n');

    const physicalTree = [
      'delete(tableA)',
      '-select(value_pred(tableA.id eq id))',
      '--table_access_by_row_id(tableA)',
      '---index_range_scan(tableA.idxName, [name, name], natural)',
      '',
    ].join('\n');

    const table = env.schema.table('tableA');
    const queryContext = new DeleteContext(env.schema);
    queryContext.from = table;
    queryContext.where = op.and(
      table.col('id').eq('id'),
      table.col('name').eq('name')
    );

    TestUtil.simulateIndexCost(
      sandbox,
      env.indexStore,
      (table.col('id') as BaseColumn).getIndices()[0],
      100
    );
    TestUtil.simulateIndexCost(
      sandbox,
      env.indexStore,
      (table.col('name') as BaseColumn).getIndices()[0],
      1
    );

    const deleteNode = new DeleteNode(table);
    const selectNode1 = new SelectNode(
      (queryContext.where as PredicateNode).getChildAt(0) as PredicateNode
    );
    deleteNode.addChild(selectNode1);
    const selectNode2 = new SelectNode(
      (queryContext.where as PredicateNode).getChildAt(1) as PredicateNode
    );
    selectNode1.addChild(selectNode2);
    const tableAccessNode = new TableAccessNode(queryContext.from);
    selectNode2.addChild(tableAccessNode);

    assert.equal(logicalTree, TreeHelper.toString(deleteNode));
    const testScope = new Set<Table>();
    testScope.add(table);
    const logicalPlan = new LogicalQueryPlan(deleteNode, testScope);
    const physicalPlan = physicalPlanFactory.create(logicalPlan, queryContext);
    const toStringFn = (node: TreeNode) =>
      `${(node as PhysicalQueryPlanNode).toContextString(queryContext)}\n`;
    assert.equal(
      physicalTree,
      TreeHelper.toString(physicalPlan.getRoot(), toStringFn)
    );
  });
});
