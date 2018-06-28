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

import {Global} from '../../base/global';
import {Context} from '../../query/context';
import {TreeHelper} from '../../structs/tree_helper';
import {RewritePass} from '../rewrite_pass';

import {IndexCostEstimator} from './index_cost_estimator';
import {IndexRangeCandidate} from './index_range_candidate';
import {IndexRangeScanStep} from './index_range_scan_step';
import {JoinStep} from './join_step';
import {PhysicalQueryPlanNode} from './physical_query_plan_node';
import {SelectStep} from './select_step';
import {TableAccessByRowIdStep} from './table_access_by_row_id_step';
import {TableAccessFullStep} from './table_access_full_step';

//  An optimization pass that detects if there are any indices that can be used
// in order to avoid full table scan.
export class IndexRangeScanPass extends RewritePass<PhysicalQueryPlanNode> {
  constructor(private global: Global) {
    super();
  }

  public rewrite(rootNode: PhysicalQueryPlanNode, queryContext: Context):
      PhysicalQueryPlanNode {
    this.rootNode = rootNode;

    const tableAccessFullSteps =
        TreeHelper.find(
            rootNode, (node) => node instanceof TableAccessFullStep) as
        TableAccessFullStep[];
    tableAccessFullSteps.forEach((tableAccessFullStep) => {
      const selectStepsCandidates = this.findSelectSteps(tableAccessFullStep);
      if (selectStepsCandidates.length === 0) {
        return;
      }

      const costEstimator =
          new IndexCostEstimator(this.global, tableAccessFullStep.table);
      const indexRangeCandidate = costEstimator.chooseIndexFor(
          queryContext,
          selectStepsCandidates.map(
              (c) => queryContext.getPredicate(c.predicateId)));
      if (indexRangeCandidate === null) {
        // No SelectStep could be optimized for this table.
        return;
      }

      // Creating a temporary mapping from Predicate to SelectStep, such that
      // the predicates that can be replaced by an index-range scan can be
      // mapped back to SelectStep nodes.
      const predicateToSelectStepMap = new Map<number, SelectStep>();
      selectStepsCandidates.forEach((selectStep) => {
        predicateToSelectStepMap.set(selectStep.predicateId, selectStep);
      }, this);

      this.rootNode = this.replaceWithIndexRangeScanStep(
          indexRangeCandidate, predicateToSelectStepMap, tableAccessFullStep,
          queryContext);
    }, this);

    return this.rootNode;
  }

  // Finds all the SelectStep instances that exist in the tree above the given
  // node and are eligible for optimization.
  private findSelectSteps(startNode: PhysicalQueryPlanNode): SelectStep[] {
    const selectSteps: SelectStep[] = [];
    let node = startNode.getParent();
    while (node) {
      if (node instanceof SelectStep) {
        selectSteps.push(node);
      } else if (node instanceof JoinStep) {
        // Stop searching if a join node is traversed.
        break;
      }
      node = node.getParent();
    }

    return selectSteps;
  }

  // Replaces all the SelectSteps that can be calculated by using the chosen
  // index with two new steps an IndexRangeScanStep and a
  // TableAccessByRowIdStep.
  private replaceWithIndexRangeScanStep(
      indexRangeCandidate: IndexRangeCandidate,
      predicateToSelectStepMap: Map<number, SelectStep>,
      tableAccessFullStep: TableAccessFullStep,
      queryContext: Context): PhysicalQueryPlanNode {
    const predicateIds = indexRangeCandidate.getPredicateIds();
    const selectSteps = predicateIds.map((predicateId) => {
      return predicateToSelectStepMap.get(predicateId) as SelectStep;
    });
    selectSteps.forEach(TreeHelper.removeNode);

    const indexRangeScanStep = new IndexRangeScanStep(
        this.global, indexRangeCandidate.indexSchema,
        indexRangeCandidate.getKeyRangeCalculator(), false /* reverseOrder */);
    const tableAccessByRowIdStep =
        new TableAccessByRowIdStep(this.global, tableAccessFullStep.table);
    tableAccessByRowIdStep.addChild(indexRangeScanStep);
    TreeHelper.replaceNodeWithChain(
        tableAccessFullStep, tableAccessByRowIdStep, indexRangeScanStep);

    return indexRangeScanStep.getRoot() as PhysicalQueryPlanNode;
  }
}
