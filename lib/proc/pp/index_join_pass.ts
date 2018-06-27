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

import {EvalType} from '../../base/eval';
import {SelectContext} from '../../query/select_context';
import {Column} from '../../schema/column';
import {Table} from '../../schema/table';
import {TreeHelper} from '../../structs/tree_helper';
import {Relation} from '../relation';
import {RewritePass} from '../rewrite_pass';

import {JoinStep} from './join_step';
import {NoOpStep} from './no_op_step';
import {PhysicalQueryPlanNode} from './physical_query_plan_node';
import {TableAccessFullStep} from './table_access_full_step';

// An optimization pass responsible for identifying JoinSteps that can be
// calculated as index nested loop joins. It transforms the tree by specifying
// the algorithm to use in such JoinSteps and also by eliminating
// TableAccessFullStep corresponding to the side of the join where the index
// will be used.
export class IndexJoinPass extends RewritePass<PhysicalQueryPlanNode> {
  constructor() {
    super();
  }

  public rewrite(rootNode: PhysicalQueryPlanNode, queryContext: SelectContext):
      PhysicalQueryPlanNode {
    this.rootNode = rootNode;

    if (!this.canOptimize(queryContext)) {
      return rootNode;
    }

    const joinSteps =
        TreeHelper.find(rootNode, (node) => node instanceof JoinStep) as
        JoinStep[];
    joinSteps.forEach(this.processJoinStep, this);

    return this.rootNode;
  }

  private canOptimize(queryContext: SelectContext): boolean {
    return queryContext.from.length > 1;
  }

  // Examines the given join step and decides whether it should be executed as
  // an index-join.
  private processJoinStep(joinStep: JoinStep): void {
    // Currently ONLY inner EQ join can be calculated using index join.
    if (joinStep.predicate.evaluatorType !== EvalType.EQ ||
        joinStep.isOuterJoin) {
      return;
    }

    // Finds which of the two joined columns corresponds to the given table.
    const getColumnForTable = (table: Table): Column => {
      return table.getEffectiveName() ===
              joinStep.predicate.rightColumn.getTable().getEffectiveName() ?
          joinStep.predicate.rightColumn :
          joinStep.predicate.leftColumn;
    };

    // Extracts the candidate indexed column for the given execution step node.
    const getCandidate = (executionStep: PhysicalQueryPlanNode): Column => {
      // In order to use and index for implementing a join, the entire relation
      // must be fed to the JoinStep, otherwise the index can't be used.
      if (!(executionStep instanceof TableAccessFullStep)) {
        return null as any as Column;
      }
      const candidateColumn = getColumnForTable(executionStep.table);
      return candidateColumn.getIndex() === null ? null as any as Column :
                                                   candidateColumn;
    };

    const leftCandidate =
        getCandidate(joinStep.getChildAt(0) as PhysicalQueryPlanNode);
    const rightCandidate =
        getCandidate(joinStep.getChildAt(1) as PhysicalQueryPlanNode);

    if (leftCandidate === null && rightCandidate === null) {
      // None of the two involved columns can be used for an index join.
      return;
    }

    // TODO(dpapad): If both columns can be used, currently the right column is
    // preferred. A smarter decision is to use the column corresponding to the
    // bigger incoming relation, such that index accesses are minimized. Use
    // index stats to figure out the size of each relation.
    const chosenColumn =
        rightCandidate !== null ? rightCandidate : leftCandidate;

    joinStep.markAsIndexJoin(chosenColumn);
    const dummyRelation =
        new Relation([], [chosenColumn.getTable().getEffectiveName()]);
    joinStep.replaceChildAt(
        new NoOpStep([dummyRelation]), chosenColumn === leftCandidate ? 0 : 1);
  }
}
