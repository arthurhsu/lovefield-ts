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
import {Operator} from '../../base/private_enum';
import {CombinedPredicate} from '../../pred/combined_predicate';
import {PredicateNode} from '../../pred/predicate_node';
import {Context} from '../../query/context';
import {ArrayHelper} from '../../structs/array_helper';
import {TreeHelper} from '../../structs/tree_helper';
import {RewritePass} from '../rewrite_pass';

import {LogicalQueryPlanNode} from './logical_query_plan_node';
import {SelectNode} from './select_node';

export class AndPredicatePass extends RewritePass<LogicalQueryPlanNode> {
  constructor() {
    super();
  }

  rewrite(
    rootNode: LogicalQueryPlanNode,
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    context?: Context
  ): LogicalQueryPlanNode {
    this.rootNode = rootNode;
    this.traverse(this.rootNode);
    return this.rootNode;
  }

  // Traverses the subtree that starts at the given node and rewrites it such
  // that all AND predicates are broken down to separate SelectNode instances.
  private traverse(rootNode: LogicalQueryPlanNode): void {
    if (rootNode instanceof SelectNode) {
      assert(
        rootNode.getChildCount() === 1,
        'SelectNode must have exactly one child.'
      );

      const predicates = this.breakAndPredicate(
        rootNode.predicate as PredicateNode
      );
      const newNodes = this.createSelectNodeChain(predicates);
      TreeHelper.replaceNodeWithChain(rootNode, newNodes[0], newNodes[1]);

      if (rootNode === this.rootNode) {
        this.rootNode = newNodes[0];
      }
      rootNode = newNodes[0];
    }

    rootNode.getChildren().forEach(child => this.traverse(child));
  }

  // Recursively breaks down an AND predicate to its components.
  // OR predicates are unaffected, as well as other types of predicates
  // (value/join).
  // Example: (a0 AND (a1 AND a2)) AND (b OR c) becomes
  //           a0 AND a1 AND a2 AND (b OR c) -> [a0, a1, a2, (b OR c)]
  private breakAndPredicate(predicate: PredicateNode): PredicateNode[] {
    if (predicate.getChildCount() === 0) {
      return [predicate];
    }

    const combinedPredicate = predicate as CombinedPredicate;
    if (combinedPredicate.operator !== Operator.AND) {
      return [predicate];
    }

    const predicates = combinedPredicate
      .getChildren()
      .slice()
      .map(childPredicate => {
        combinedPredicate.removeChild(childPredicate);
        return this.breakAndPredicate(childPredicate as PredicateNode);
      });
    return ArrayHelper.flatten(predicates) as PredicateNode[];
  }

  private createSelectNodeChain(
    predicates: PredicateNode[]
  ): LogicalQueryPlanNode[] {
    let parentNode: LogicalQueryPlanNode | null = null;
    let lastNode: LogicalQueryPlanNode | null = null;
    predicates.map((predicate, index) => {
      const node = new SelectNode(predicate);
      if (index === 0) {
        parentNode = node;
      } else {
        (lastNode as PredicateNode).addChild(node);
      }
      lastNode = node;
    }, this);

    return [
      (parentNode as unknown) as LogicalQueryPlanNode,
      (lastNode as unknown) as LogicalQueryPlanNode,
    ];
  }
}
