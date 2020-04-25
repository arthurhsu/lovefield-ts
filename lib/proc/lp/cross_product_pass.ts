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

import {Context} from '../../query/context';
import {SelectContext} from '../../query/select_context';
import {RewritePass} from '../rewrite_pass';

import {CrossProductNode} from './cross_product_node';
import {LogicalQueryPlanNode} from './logical_query_plan_node';

export class CrossProductPass extends RewritePass<LogicalQueryPlanNode> {
  constructor() {
    super();
  }

  rewrite(
    rootNode: LogicalQueryPlanNode,
    queryContext: Context
  ): LogicalQueryPlanNode {
    if ((queryContext as SelectContext).from.length < 3) {
      return rootNode;
    }

    this.rootNode = rootNode;
    this.traverse(this.rootNode);
    return this.rootNode;
  }

  private traverse(rootNode: LogicalQueryPlanNode): void {
    // If rootNode is a CrossProduct and has more than 2 children, break it down.
    // TODO(dpapad): This needs optimization, since the order chosen here
    // affects whether subsequent steps will be able to convert the
    // cross-product to a join.
    if (rootNode instanceof CrossProductNode) {
      while (rootNode.getChildCount() > 2) {
        const crossProduct = new CrossProductNode();
        for (let i = 0; i < 2; i++) {
          const child = rootNode.removeChildAt(0);
          crossProduct.addChild(child as LogicalQueryPlanNode);
        }
        rootNode.addChildAt(crossProduct, 0);
      }
    }

    rootNode.getChildren().forEach(child => this.traverse(child));
  }
}
