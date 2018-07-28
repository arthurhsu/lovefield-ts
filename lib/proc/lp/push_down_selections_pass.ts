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

import {JoinPredicate} from '../../pred/join_predicate';
import {Context} from '../../query/context';
import {SelectContext} from '../../query/select_context';
import {BaseTable} from '../../schema/base_table';
import {isSubset} from '../../structs/set_util';
import {TreeHelper} from '../../structs/tree_helper';
import {TreeNode} from '../../structs/tree_node';
import {RewritePass} from '../rewrite_pass';

import {CrossProductNode} from './cross_product_node';
import {JoinNode} from './join_node';
import {LogicalQueryPlanNode} from './logical_query_plan_node';
import {SelectNode} from './select_node';
import {TableAccessNode} from './table_access_node';

export class PushDownSelectionsPass extends RewritePass<LogicalQueryPlanNode> {
  // A set of SelectNodes that have already been pushed down. This is necessary
  // to avoid re-visiting the same nodes (endless recursion).
  private alreadyPushedDown: Set<TreeNode>;

  constructor() {
    super();
    this.alreadyPushedDown = new Set<TreeNode>();
  }

  public rewrite(rootNode: LogicalQueryPlanNode, context: Context):
      LogicalQueryPlanNode {
    const queryContext = context as SelectContext;
    if (queryContext.where === undefined || queryContext.where === null) {
      // No predicates exist.
      return rootNode;
    }

    this.clear();
    this.rootNode = rootNode;
    this.traverse(this.rootNode, queryContext);
    this.clear();
    return this.rootNode;
  }

  // Clears any state in this rewrite pass, such that it can be re-used for
  // rewriting multiple trees.
  private clear(): void {
    this.alreadyPushedDown.clear();
  }

  private traverse(rootNode: LogicalQueryPlanNode, queryContext: SelectContext):
      void {
    const processChildren = (node: TreeNode) => {
      node.getChildren().forEach(processNodeRec);
    };

    const processNodeRec = (node: TreeNode) => {
      if (this.alreadyPushedDown.has(node)) {
        return;
      }
      if (!this.isCandidateNode(node)) {
        processChildren(node);
        return;
      }

      const selectNode = node as SelectNode;
      const selectNodeTables = selectNode.predicate.getTables();

      const shouldPushDownFn = (child: LogicalQueryPlanNode) =>
          this.doesReferToTables(child, selectNodeTables);

      const newRoot =
          this.pushDownNodeRec(queryContext, selectNode, shouldPushDownFn);
      this.alreadyPushedDown.add(selectNode);
      if (newRoot !== selectNode) {
        if (newRoot.getParent() === null) {
          this.rootNode = newRoot as LogicalQueryPlanNode;
        }
        processNodeRec(newRoot);
      }
      processChildren(selectNode);
    };

    processNodeRec(rootNode);
  }

  // Recursively pushes down a SelectNode until it can't be pushed any further
  // down. |shouldPushDown| is a function to be called for each child to
  // determine whether the node should be pushed down one level.
  // Returns the new root of the subtree that itself could not be pushed further
  // down.
  private pushDownNodeRec(
      queryContext: SelectContext, node: SelectNode,
      shouldPushDownFn: (n: TreeNode) => boolean): LogicalQueryPlanNode {
    let newRoot: SelectNode = node;

    if (this.shouldSwapWithChild(queryContext, node)) {
      newRoot = TreeHelper.swapNodeWithChild(node) as SelectNode;
      this.pushDownNodeRec(queryContext, node, shouldPushDownFn);
    } else if (this.shouldPushBelowChild(node)) {
      const newNodes: SelectNode[] = [];
      const cloneFn = (n: TreeNode): TreeNode => {
        const newNode = new SelectNode((n as SelectNode).predicate);
        newNodes.push(newNode);
        return newNode;
      };
      newRoot = TreeHelper.pushNodeBelowChild(
                    node, shouldPushDownFn, cloneFn) as SelectNode;

      // Recursively pushing down the nodes that were just added to the tree as
      // a result of pushing down "node", if any.
      newNodes.forEach(
          (newNode) =>
              this.pushDownNodeRec(queryContext, newNode, shouldPushDownFn));
    }

    return newRoot;
  }

  // Whether the subtree that starts at root refers to all tables in the given
  // list.
  private doesReferToTables(root: LogicalQueryPlanNode, tables: Set<BaseTable>):
      boolean {
    // Finding all tables that are involved in the subtree starting at the given
    // root.
    const referredTables = new Set<BaseTable>();
    TreeHelper.getLeafNodes(root).forEach(
        (tableAccessNode) =>
            referredTables.add((tableAccessNode as TableAccessNode).table));

    if (root instanceof TableAccessNode) {
      referredTables.add(root.table);
    }

    return isSubset(referredTables, tables);
  }

  // Whether the given node is a candidate for being pushed down the tree.
  private isCandidateNode(node: TreeNode): boolean {
    return node instanceof SelectNode;
  }

  // Whether an attempt should be made to push the given node below its only
  // child.
  private shouldPushBelowChild(node: TreeNode): boolean {
    const child = node.getChildAt(0);
    return child instanceof CrossProductNode || child instanceof JoinNode;
  }

  // Whether the given node should be swapped with its only child.
  private shouldSwapWithChild(queryContext: SelectContext, node: SelectNode):
      boolean {
    const child = node.getChildAt(0);
    if (!(child instanceof SelectNode)) {
      return false;
    }

    if (queryContext.outerJoinPredicates === undefined ||
        queryContext.outerJoinPredicates === null) {
      return true;
    }
    const nodeIsJoin = node.predicate instanceof JoinPredicate;
    const childIsOuterJoin =
        queryContext.outerJoinPredicates.has(child.predicate.getId());
    // If the node corresponds to a join predicate (outer or inner), allow it to
    // be pushed below any other SelectNode. If the node does not correspond to
    // a join predicate don't allow it to be pushed below an outer join, because
    // it needs to be applied after the outer join is calculated.
    return nodeIsJoin || !childIsOuterJoin;
  }
}
