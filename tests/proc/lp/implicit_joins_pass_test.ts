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

import { Predicate } from '../../../lib/pred/predicate';
import { CrossProductNode } from '../../../lib/proc/lp/cross_product_node';
import { ImplicitJoinsPass } from '../../../lib/proc/lp/implicit_joins_pass';
import { SelectNode } from '../../../lib/proc/lp/select_node';
import { TableAccessNode } from '../../../lib/proc/lp/table_access_node';
import { SelectContext } from '../../../lib/query/select_context';
import { DatabaseSchema } from '../../../lib/schema/database_schema';
import { getHrDbSchemaBuilder } from '../../../testing/hr_schema/hr_schema_builder';
import { TreeTestHelper } from '../../../testing/tree_test_helper';

describe('ImplicitJoinsPass', () => {
  let schema: DatabaseSchema;

  before(() => {
    schema = getHrDbSchemaBuilder().getSchema();
  });

  // Tests a simple tree, where only one cross product node exists.
  it('simpleTree', () => {
    const e = schema.table('Employee');
    const j = schema.table('Job');

    const treeBefore = [
      'select(join_pred(Employee.jobId eq Job.id))',
      '-cross_product',
      '--table_access(Employee)',
      '--table_access(Job)',
      '',
    ].join('\n');

    const treeAfter = [
      'join(type: inner, join_pred(Employee.jobId eq Job.id))',
      '-table_access(Employee)',
      '-table_access(Job)',
      '',
    ].join('\n');

    const constructTree = () => {
      const queryContext = new SelectContext(schema);
      queryContext.from = [e, j];
      queryContext.where = e.col('jobId').eq(j.col('id'));

      const selectNode = new SelectNode(queryContext.where as Predicate);
      const crossProductNode = new CrossProductNode();
      selectNode.addChild(crossProductNode);
      queryContext.from.forEach(tableSchema => {
        crossProductNode.addChild(new TableAccessNode(tableSchema));
      });

      return { queryContext, root: selectNode };
    };

    const pass = new ImplicitJoinsPass();
    TreeTestHelper.assertTreeTransformation(
      constructTree(),
      treeBefore,
      treeAfter,
      pass
    );
  });
});
