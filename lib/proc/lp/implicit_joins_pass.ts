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

import {assert} from '../../base/assert';
import {JoinPredicate} from '../../pred/join_predicate';
import {Context} from '../../query/context';
import {SelectContext} from '../../query/select_context';
import {TreeHelper} from '../../structs/tree_helper';
import {CrossProductNode} from './cross_product_node';
import {JoinNode} from './join_node';
import {LogicalQueryPlanNode} from './logical_query_plan_node';
import {RewritePass} from './rewrite_pass';
import {SelectNode} from './select_node';

export class ImplicitJoinsPass extends RewritePass<LogicalQueryPlanNode> {
  constructor() {
    super();
  }

  public rewrite(rootNode: LogicalQueryPlanNode, context: Context):
      LogicalQueryPlanNode {
    const queryContext = context as SelectContext;
    if (queryContext.from.length < 2) {
      return rootNode;
    }

    this.rootNode = rootNode;
    this.traverse(this.rootNode, queryContext);
    return this.rootNode;
  }

  private traverse(rootNode: LogicalQueryPlanNode, queryContext: SelectContext):
      void {
    if (rootNode instanceof SelectNode &&
        rootNode.predicate instanceof JoinPredicate) {
      assert(
          rootNode.getChildCount() === 1,
          'SelectNode must have exactly one child.');
      const predicateId = rootNode.predicate.getId();

      const child = rootNode.getChildAt(0);
      if (child instanceof CrossProductNode) {
        const isOuterJoin = queryContext.outerJoinPredicates &&
            queryContext.outerJoinPredicates.has(predicateId);
        const joinNode = new JoinNode(rootNode.predicate, isOuterJoin);
        TreeHelper.replaceChainWithNode(rootNode, child, joinNode);
        if (rootNode === this.rootNode) {
          this.rootNode = joinNode;
        }
        rootNode = joinNode;
      }
    }
    rootNode.getChildren().forEach(
        (child) => this.traverse(child, queryContext));
  }
}
