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

import {ErrorCode, Exception} from '../../base/exception';
import {Global} from '../../base/global';
import {Context} from '../../query/context';
import {Column} from '../../schema/column';
import {TreeHelper} from '../../structs/tree_helper';
import {AggregationNode} from '../lp/aggregation_node';
import {CrossProductNode} from '../lp/cross_product_node';
import {DeleteNode} from '../lp/delete_node';
import {GroupByNode} from '../lp/group_by_node';
import {InsertNode} from '../lp/insert_node';
import {InsertOrReplaceNode} from '../lp/insert_or_replace_node';
import {JoinNode} from '../lp/join_node';
import {LimitNode} from '../lp/limit_node';
import {LogicalQueryPlan} from '../lp/logical_query_plan';
import {LogicalQueryPlanNode} from '../lp/logical_query_plan_node';
import {OrderByNode} from '../lp/orderby_node';
import {ProjectNode} from '../lp/project_node';
import {SelectNode} from '../lp/select_node';
import {SkipNode} from '../lp/skip_node';
import {TableAccessNode} from '../lp/table_access_node';
import {UpdateNode} from '../lp/update_node';
import {RewritePass} from '../rewrite_pass';

import {AggregationStep} from './aggregation_step';
import {CrossProductStep} from './cross_product_step';
import {DeleteStep} from './delete_step';
import {GetRowCountPass} from './get_row_count_pass';
import {GroupByStep} from './group_by_step';
import {IndexJoinPass} from './index_join_pass';
import {IndexRangeScanPass} from './index_range_scan_pass';
import {InsertOrReplaceStep} from './insert_or_replace_step';
import {InsertStep} from './insert_step';
import {JoinStep} from './join_step';
import {LimitSkipByIndexPass} from './limit_skip_by_index_pass';
import {LimitStep} from './limit_step';
import {OrderByStep} from './order_by_step';
import {PhysicalPlanRewriter} from './physical_plan_rewriter';
import {PhysicalQueryPlan} from './physical_query_plan';
import {PhysicalQueryPlanNode} from './physical_query_plan_node';
import {ProjectStep} from './project_step';
import {SelectStep} from './select_step';
import {SkipStep} from './skip_step';
import {TableAccessFullStep} from './table_access_full_step';
import {UpdateStep} from './update_step';

export class PhysicalPlanFactory {
  private selectOptimizationPasses: Array<RewritePass<PhysicalQueryPlanNode>>;
  private deleteOptimizationPasses: Array<RewritePass<PhysicalQueryPlanNode>>;

  constructor(private global: Global) {
    this.selectOptimizationPasses = [
      new IndexJoinPass(),
      new IndexRangeScanPass(global),
      // TODO(arthurhsu): implement
      // new MultiColumnOrPass(),
      // new OrderByIndexPass(),
      new LimitSkipByIndexPass(),
      new GetRowCountPass(global),
    ];

    this.deleteOptimizationPasses = [
      new IndexRangeScanPass(global),
    ];
  }

  public create(logicalQueryPlan: LogicalQueryPlan, queryContext: Context):
      PhysicalQueryPlan {
    const logicalQueryPlanRoot = logicalQueryPlan.getRoot();
    if ((logicalQueryPlanRoot instanceof InsertOrReplaceNode) ||
        (logicalQueryPlanRoot instanceof InsertNode)) {
      return this.createPlan(logicalQueryPlan, queryContext);
    }

    if (logicalQueryPlanRoot instanceof ProjectNode ||
        logicalQueryPlanRoot instanceof LimitNode ||
        logicalQueryPlanRoot instanceof SkipNode) {
      return this.createPlan(
          logicalQueryPlan, queryContext, this.selectOptimizationPasses);
    }

    if ((logicalQueryPlanRoot instanceof DeleteNode) ||
        (logicalQueryPlanRoot instanceof UpdateNode)) {
      return this.createPlan(
          logicalQueryPlan, queryContext, this.deleteOptimizationPasses);
    }

    // Should never get here since all cases are handled above.
    // 8: Unknown query plan node.
    throw new Exception(ErrorCode.UNKNOWN_PLAN_NODE);
  }

  private createPlan(
      logicalPlan: LogicalQueryPlan, queryContext: Context,
      rewritePasses?: Array<RewritePass<PhysicalQueryPlanNode>>):
      PhysicalQueryPlan {
    let rootStep =
        TreeHelper.map(logicalPlan.getRoot(), this.mapFn.bind(this)) as
        PhysicalQueryPlanNode;

    if (rewritePasses !== undefined && rewritePasses !== null) {
      const planRewriter =
          new PhysicalPlanRewriter(rootStep, queryContext, rewritePasses);
      rootStep = planRewriter.generate();
    }
    return new PhysicalQueryPlan(rootStep, logicalPlan.getScope());
  }

  // Maps each node of a logical execution plan to a corresponding physical
  // execution step.
  private mapFn(node: LogicalQueryPlanNode): PhysicalQueryPlanNode {
    if (node instanceof ProjectNode) {
      return new ProjectStep(node.columns, node.groupByColumns as Column[]);
    } else if (node instanceof GroupByNode) {
      return new GroupByStep(node.columns);
    } else if (node instanceof AggregationNode) {
      return new AggregationStep(node.columns);
    } else if (node instanceof OrderByNode) {
      return new OrderByStep(node.orderBy);
    } else if (node instanceof SkipNode) {
      return new SkipStep();
    } else if (node instanceof LimitNode) {
      return new LimitStep();
    } else if (node instanceof SelectNode) {
      return new SelectStep(node.predicate.getId());
    } else if (node instanceof CrossProductNode) {
      return new CrossProductStep();
    } else if (node instanceof JoinNode) {
      return new JoinStep(this.global, node.predicate, node.isOuterJoin);
    } else if (node instanceof TableAccessNode) {
      return new TableAccessFullStep(this.global, node.table);
    } else if (node instanceof DeleteNode) {
      return new DeleteStep(node.table);
    } else if (node instanceof UpdateNode) {
      return new UpdateStep(node.table);
    } else if (node instanceof InsertOrReplaceNode) {
      return new InsertOrReplaceStep(this.global, node.table);
    } else if (node instanceof InsertNode) {
      return new InsertStep(this.global, node.table);
    }

    // 514: Unknown node type.
    throw new Exception(ErrorCode.UNKNOWN_NODE_TYPE);
  }
}
