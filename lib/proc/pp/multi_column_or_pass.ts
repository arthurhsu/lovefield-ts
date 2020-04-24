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

import { Global } from '../../base/global';
import { Operator } from '../../base/private_enum';
import { CombinedPredicate } from '../../pred/combined_predicate';
import { PredicateNode } from '../../pred/predicate_node';
import { Context } from '../../query/context';
import { TreeHelper } from '../../structs/tree_helper';
import { TreeNode } from '../../structs/tree_node';
import { RewritePass } from '../rewrite_pass';

import { IndexCostEstimator } from './index_cost_estimator';
import { IndexRangeCandidate } from './index_range_candidate';
import { IndexRangeScanStep } from './index_range_scan_step';
import { MultiIndexRangeScanStep } from './multi_index_range_scan_step';
import { PhysicalQueryPlanNode } from './physical_query_plan_node';
import { SelectStep } from './select_step';
import { TableAccessByRowIdStep } from './table_access_by_row_id_step';
import { TableAccessFullStep } from './table_access_full_step';

// An optimization pass that detects if there are any OR predicates that
// 1) Refer to a single table.
// 2) Refer to multiple columns.
// 3) All referred columns  are indexed.
//
// If such predicates are found the tree is transformed to leverage indices.
// OR predicates that refer to a single column are already optimized by the
// previous optimization pass IndexRangeScanPass.
export class MultiColumnOrPass extends RewritePass<PhysicalQueryPlanNode> {
  constructor(private global: Global) {
    super();
  }

  rewrite(
    rootNode: PhysicalQueryPlanNode,
    queryContext: Context
  ): PhysicalQueryPlanNode {
    this.rootNode = rootNode;
    const orSelectSteps = this.findOrPredicates(queryContext);
    if (orSelectSteps.length === 0) {
      // No OR predicates exist, this optimization does not apply.
      return this.rootNode;
    }

    // In the presence of multiple candidate OR predicates currently the first
    // one that can leverage indices is chosen.
    // TODO(dpapad): Compare the index range scan cost for each of the
    // predicates and select the fastest one.
    let indexRangeCandidates: IndexRangeCandidate[] | null = null;
    let orSelectStep: SelectStep | null = null;
    let i = 0;
    do {
      orSelectStep = orSelectSteps[i++];
      indexRangeCandidates = this.findIndexRangeCandidates(
        orSelectStep,
        queryContext
      );
    } while (indexRangeCandidates === null && i < orSelectSteps.length);

    if (indexRangeCandidates === null) {
      return this.rootNode;
    }

    const tableAccessFullStep = this.findTableAccessFullStep(
      indexRangeCandidates[0].indexSchema.tableName
    );
    if (tableAccessFullStep === null) {
      // No TableAccessFullStep exists, an index is leveraged already, this
      // optimization does not apply.
      return this.rootNode;
    }

    this.rootNode = this.replaceWithIndexRangeScan(
      orSelectStep,
      tableAccessFullStep,
      indexRangeCandidates
    );
    return this.rootNode;
  }

  // Find SelectStep instances in the tree corresponding to OR predicates.
  private findOrPredicates(queryContext: Context): SelectStep[] {
    const filterFn = (node: TreeNode) => {
      if (!(node instanceof SelectStep)) {
        return false;
      }

      const predicate = queryContext.getPredicate(node.predicateId);
      return (
        predicate instanceof CombinedPredicate &&
        predicate.operator === Operator.OR
      );
    };

    return TreeHelper.find(this.rootNode, filterFn) as SelectStep[];
  }

  // Find the table access step corresponding to the given table, or null if
  // such a step does not exist.
  private findTableAccessFullStep(
    tableName: string
  ): TableAccessFullStep | null {
    return (
      (TreeHelper.find(
        this.rootNode,
        node =>
          node instanceof TableAccessFullStep &&
          node.table.getName() === tableName
      )[0] as TableAccessFullStep) || null
    );
  }

  // Returns the IndexRangeCandidates corresponding to the given multi-column
  // OR predicate. Null is returned if no indices can be leveraged for the
  // given predicate.
  private findIndexRangeCandidates(
    selectStep: SelectStep,
    queryContext: Context
  ): IndexRangeCandidate[] | null {
    const predicate = queryContext.getPredicate(
      selectStep.predicateId
    ) as PredicateNode;

    const tables = predicate.getTables();
    if (tables.size !== 1) {
      // Predicates which refer to more than one table are not eligible for this
      // optimization.
      return null;
    }

    const tableSchema = Array.from(tables.values())[0];
    const indexCostEstimator = new IndexCostEstimator(this.global, tableSchema);

    let indexRangeCandidates: IndexRangeCandidate[] | null = null;
    const allIndexed = predicate.getChildren().every(childPredicate => {
      const indexRangeCandidate = indexCostEstimator.chooseIndexFor(
        queryContext,
        [childPredicate as PredicateNode]
      );
      if (indexRangeCandidate !== null) {
        indexRangeCandidates === null
          ? (indexRangeCandidates = [indexRangeCandidate])
          : indexRangeCandidates.push(indexRangeCandidate);
      }

      return indexRangeCandidate !== null;
    });

    return allIndexed ? indexRangeCandidates : null;
  }

  // Replaces the given SelectStep with a MultiIndexRangeScanStep
  // (and children).
  private replaceWithIndexRangeScan(
    selectStep: SelectStep,
    tableAccessFullStep: TableAccessFullStep,
    indexRangeCandidates: IndexRangeCandidate[]
  ): PhysicalQueryPlanNode {
    const tableAccessByRowIdStep = new TableAccessByRowIdStep(
      this.global,
      tableAccessFullStep.table
    );
    const multiIndexRangeScanStep = new MultiIndexRangeScanStep();
    tableAccessByRowIdStep.addChild(multiIndexRangeScanStep);

    indexRangeCandidates.forEach(candidate => {
      const indexRangeScanStep = new IndexRangeScanStep(
        this.global,
        candidate.indexSchema,
        candidate.getKeyRangeCalculator(),
        false /* reverseOrder */
      );
      multiIndexRangeScanStep.addChild(indexRangeScanStep);
    }, this);

    TreeHelper.removeNode(selectStep);
    TreeHelper.replaceNodeWithChain(
      tableAccessFullStep,
      tableAccessByRowIdStep,
      multiIndexRangeScanStep
    );

    return multiIndexRangeScanStep.getRoot() as PhysicalQueryPlanNode;
  }
}
