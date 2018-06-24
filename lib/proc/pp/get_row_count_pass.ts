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
import {FnType} from '../../base/private_enum';
import {AggregatedColumn} from '../../fn/aggregated_column';
import {StarColumn} from '../../fn/star_column';
import {SelectContext} from '../../query/select_context';
import {TreeHelper} from '../../structs/tree_helper';
import {RewritePass} from '../lp/rewrite_pass';
import {GetRowCountStep} from './get_row_count_step';
import {PhysicalQueryPlanNode} from './physical_query_plan_node';
import {TableAccessFullStep} from './table_access_full_step';

// An optimization pass responsible for optimizing SELECT COUNT(*) queries,
// where no LIMIT, SKIP, WHERE or GROUP_BY appears.
export class GetRowCountPass extends RewritePass<PhysicalQueryPlanNode> {
  constructor(private global: Global) {
    super();
  }

  public rewrite(rootNode: PhysicalQueryPlanNode, queryContext: SelectContext):
      PhysicalQueryPlanNode {
    this.rootNode = rootNode;
    if (!this.canOptimize(queryContext)) {
      return rootNode;
    }

    const tableAccessFullStep: TableAccessFullStep =
        TreeHelper.find(
            rootNode,
            (node) => node instanceof TableAccessFullStep,
            )[0] as any as TableAccessFullStep;
    const getRowCountStep =
        new GetRowCountStep(this.global, tableAccessFullStep.table);
    TreeHelper.replaceNodeWithChain(
        tableAccessFullStep, getRowCountStep, getRowCountStep);

    return this.rootNode;
  }

  private canOptimize(queryContext: SelectContext): boolean {
    const isDefAndNotNull = (v: any) => (v !== null && v !== undefined);
    const isCandidate = queryContext.columns.length === 1 &&
        queryContext.from.length === 1 &&
        !isDefAndNotNull(queryContext.where) &&
        !isDefAndNotNull(queryContext.limit) &&
        !isDefAndNotNull(queryContext.skip) &&
        !isDefAndNotNull(queryContext.groupBy);

    if (isCandidate) {
      const column = queryContext.columns[0];
      return column instanceof AggregatedColumn &&
          column.aggregatorType === FnType.COUNT &&
          column.child instanceof StarColumn;
    }

    return false;
  }
}
