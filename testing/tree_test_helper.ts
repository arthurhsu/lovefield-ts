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

import { LogicalQueryPlanNode } from '../lib/proc/lp/logical_query_plan_node';
import { PhysicalQueryPlanNode } from '../lib/proc/pp/physical_query_plan_node';
import { RewritePass } from '../lib/proc/rewrite_pass';
import { Context } from '../lib/query/context';
import { SelectContext } from '../lib/query/select_context';
import { TreeHelper } from '../lib/structs/tree_helper';
import { TreeNode } from '../lib/structs/tree_node';

const assert = chai.assert;

export interface TestTree {
  queryContext: SelectContext;
  root: PhysicalQueryPlanNode | LogicalQueryPlanNode;
}

export class TreeTestHelper {
  static assertTreeTransformation(
    treeBefore: TestTree,
    treeStringBefore: string,
    treeStringAfter: string,
    pass: RewritePass<LogicalQueryPlanNode>
  ): void {
    const toStringFn = TreeTestHelper.toString.bind(
      null,
      treeBefore.queryContext
    );
    assert.equal(
      treeStringBefore,
      TreeHelper.toString(treeBefore.root, toStringFn)
    );
    const rootNodeAfter = pass.rewrite(
      treeBefore.root,
      treeBefore.queryContext
    );
    assert.equal(
      treeStringAfter,
      TreeHelper.toString(rootNodeAfter, toStringFn)
    );
  }

  static toString(queryContext: Context, node: TreeNode): string {
    return node instanceof PhysicalQueryPlanNode
      ? `${node.toContextString(queryContext)}\n`
      : `${node.toString()}\n`;
  }
}
