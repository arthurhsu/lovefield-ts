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

import {DeleteContext} from '../../query/delete_context';
import {BaseLogicalPlanGenerator} from './base_logical_plan_generator';
import {DeleteNode} from './delete_node';
import {LogicalPlanRewriter} from './logical_plan_rewriter';
import {LogicalQueryPlanNode} from './logical_query_plan_node';
import {RewritePass} from './rewrite_pass';
import {SelectNode} from './select_node';
import {TableAccessNode} from './table_access_node';

export class DeleteLogicalPlanGenerator extends
    BaseLogicalPlanGenerator<DeleteContext> {
  constructor(
      query: DeleteContext,
      private rewritePasses: Array<RewritePass<LogicalQueryPlanNode>>) {
    super(query);
  }

  public generateInternal(): LogicalQueryPlanNode {
    const deleteNode = new DeleteNode(this.query.from);
    const selectNode =
        this.query.where ? new SelectNode(this.query.where.copy()) : null;
    const tableAccessNode = new TableAccessNode(this.query.from);

    if (selectNode === null) {
      deleteNode.addChild(tableAccessNode);
    } else {
      selectNode.addChild(tableAccessNode);
      deleteNode.addChild(selectNode);
    }

    // Optimizing the "naive" logical plan.
    const planRewriter =
        new LogicalPlanRewriter(deleteNode, this.query, this.rewritePasses);
    return planRewriter.generate();
  }
}
