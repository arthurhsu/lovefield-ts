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

import {op} from '../../../lib/fn/op';
import {CrossProductNode} from '../../../lib/proc/lp/cross_product_node';
import {CrossProductPass} from '../../../lib/proc/lp/cross_product_pass';
import {SelectNode} from '../../../lib/proc/lp/select_node';
import {TableAccessNode} from '../../../lib/proc/lp/table_access_node';
import {SelectContext} from '../../../lib/query/select_context';
import {DatabaseSchema} from '../../../lib/schema/database_schema';
import {getHrDbSchemaBuilder} from '../../../testing/hr_schema/hr_schema_builder';
import {TreeTestHelper} from '../../../testing/tree_test_helper';

describe('CrossProductPass', () => {
  let schema: DatabaseSchema;

  before(() => {
    schema = getHrDbSchemaBuilder().getSchema();
  });

  // Tests a complex tree, where a CrossProductNode with many children exists.
  it('complexTree', () => {
    const d = schema.table('Department');
    const e = schema.table('Employee');
    const j = schema.table('Job');

    const treeBefore = [
      'select(combined_pred_and)',
      '-cross_product',
      '--table_access(Employee)',
      '--table_access(Job)',
      '--table_access(Location)',
      '--table_access(JobHistory)',
      '--table_access(Department)',
      '',
    ].join('\n');

    const treeAfter = [
      'select(combined_pred_and)',
      '-cross_product',
      '--cross_product',
      '---cross_product',
      '----cross_product',
      '-----table_access(Employee)',
      '-----table_access(Job)',
      '----table_access(Location)',
      '---table_access(JobHistory)',
      '--table_access(Department)',
      '',
    ].join('\n');

    const constructTree = () => {
      const queryContext = new SelectContext(schema);
      queryContext.from =
          [e, j, schema.table('Location'), schema.table('JobHistory'), d];
      queryContext.where =
          op.and(e['jobId'].eq(j['id']), e['departmentId'].eq(d['id']));

      const crossProductNode = new CrossProductNode();
      queryContext.from.forEach((tableSchema) => {
        crossProductNode.addChild(new TableAccessNode(tableSchema));
      });

      const selectNode = new SelectNode(queryContext.where);
      selectNode.addChild(crossProductNode);

      return {queryContext: queryContext, root: selectNode};
    };

    const pass = new CrossProductPass();
    TreeTestHelper.assertTreeTransformation(
        constructTree(), treeBefore, treeAfter, pass);
  });
});
