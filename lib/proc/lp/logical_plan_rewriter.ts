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
import {LogicalPlanGenerator} from './logical_plan_generator';
import {LogicalQueryPlanNode} from './logical_query_plan_node';
import {RewritePass} from './rewrite_pass';

// Rewrites the logical query plan such that the resulting logical query plan is
// faster to execute than the original "naive" plan.
export class LogicalPlanRewriter implements LogicalPlanGenerator {
  private rootNode: LogicalQueryPlanNode;
  private queryContext: Context;
  private rewritePasses: Array<RewritePass<LogicalQueryPlanNode>>;

  constructor(
      rootNode: LogicalQueryPlanNode, queryContext: Context,
      rewritePasses: Array<RewritePass<LogicalQueryPlanNode>>) {
    this.rootNode = rootNode;
    // TODO(arthurhsu): weird hack here, refactor to remove abstract class.
    // It should be a clean interface with default implementation.
    this.queryContext = queryContext;
    this.rewritePasses = rewritePasses;
  }

  public generate(): LogicalQueryPlanNode {
    this.rewritePasses.forEach((rewritePass) => {
      this.rootNode = rewritePass.rewrite(this.rootNode, this.queryContext);
    }, this);
    return this.rootNode;
  }
}