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

import {SelectContext} from '../../query/select_context';
import {TreeHelper} from '../../structs/tree_helper';
import {TreeNode} from '../../structs/tree_node';
import {RewritePass} from '../rewrite_pass';

import {IndexRangeScanStep} from './index_range_scan_step';
import {LimitStep} from './limit_step';
import {OrderByStep} from './order_by_step';
import {PhysicalQueryPlanNode} from './physical_query_plan_node';
import {ProjectStep} from './project_step';
import {SelectStep} from './select_step';
import {SkipStep} from './skip_step';

export class LimitSkipByIndexPass extends RewritePass<PhysicalQueryPlanNode> {
  constructor() {
    super();
  }

  rewrite(
    rootNode: PhysicalQueryPlanNode,
    queryContext: SelectContext
  ): PhysicalQueryPlanNode {
    if (queryContext.limit === undefined && queryContext.skip === undefined) {
      // No LIMIT or SKIP exists.
      return rootNode;
    }

    const indexRangeScanStep = this.findIndexRangeScanStep(rootNode);
    if (indexRangeScanStep === null) {
      // No IndexRangeScanStep that can be leveraged was found.
      return rootNode;
    }

    const nodes: Array<SkipStep | LimitStep> = TreeHelper.find(
      rootNode,
      node => node instanceof LimitStep || node instanceof SkipStep
    ) as Array<SkipStep | LimitStep>;

    nodes.forEach(node => {
      this.mergeToIndexRangeScanStep(node, indexRangeScanStep);
    }, this);

    return indexRangeScanStep.getRoot() as PhysicalQueryPlanNode;
  }

  // Merges a LimitStep or SkipStep to the given IndexRangeScanStep.
  private mergeToIndexRangeScanStep(
    node: SkipStep | LimitStep,
    indexRangeScanStep: IndexRangeScanStep
  ): PhysicalQueryPlanNode {
    if (node instanceof LimitStep) {
      indexRangeScanStep.useLimit = true;
    } else {
      indexRangeScanStep.useSkip = true;
    }

    return TreeHelper.removeNode(node).parent as PhysicalQueryPlanNode;
  }

  // Finds any existing IndexRangeScanStep that can be leveraged to limit and
  // skip results.
  private findIndexRangeScanStep(
    rootNode: PhysicalQueryPlanNode
  ): IndexRangeScanStep | null {
    const filterFn = (node: TreeNode) => {
      return node instanceof IndexRangeScanStep;
    };

    // LIMIT and SKIP needs to be executed after
    //  - projections that include either groupBy or aggregators,
    //  - joins/cross-products,
    //  - selections,
    //  - sorting
    // have been calculated. Therefore if such nodes exist this optimization can
    // not be applied.
    const stopFn = (node: TreeNode) => {
      const hasAggregators =
        node instanceof ProjectStep && node.hasAggregators();
      return (
        hasAggregators ||
        node instanceof OrderByStep ||
        node.getChildCount() !== 1 ||
        node instanceof SelectStep
      );
    };

    const indexRangeScanSteps = TreeHelper.find(
      rootNode,
      filterFn,
      stopFn
    ) as IndexRangeScanStep[];
    return indexRangeScanSteps.length > 0 ? indexRangeScanSteps[0] : null;
  }
}
