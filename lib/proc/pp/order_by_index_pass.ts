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

import {Order} from '../../base/enum';
import {Global} from '../../base/global';
import {SelectContext, SelectContextOrderBy} from '../../query/select_context';
import {BaseTable} from '../../schema/base_table';
import {IndexImpl} from '../../schema/index_impl';
import {TreeHelper} from '../../structs/tree_helper';
import {TreeNode} from '../../structs/tree_node';
import {RewritePass} from '../rewrite_pass';

import {IndexRangeScanStep} from './index_range_scan_step';
import {OrderByStep} from './order_by_step';
import {PhysicalQueryPlanNode} from './physical_query_plan_node';
import {TableAccessByRowIdStep} from './table_access_by_row_id_step';
import {TableAccessFullStep} from './table_access_full_step';
import {UnboundedKeyRangeCalculator} from './unbounded_key_range_calculator';

interface OrderByIndexRangeCandidate {
  indexSchema: IndexImpl;
  isReverse: boolean;
}

// The OrderByIndexPass is responsible for modifying a tree that has a
// OrderByStep node to an equivalent tree that leverages indices to perform
// sorting.
export class OrderByIndexPass extends RewritePass<PhysicalQueryPlanNode> {
  constructor(private global: Global) {
    super();
  }

  rewrite(
    rootNode: PhysicalQueryPlanNode,
    queryContext: SelectContext
  ): PhysicalQueryPlanNode {
    const orderByStep = this.findOrderByStep(rootNode, queryContext);
    if (orderByStep === null) {
      // No OrderByStep was found.
      return rootNode;
    }

    let newSubtreeRoot = this.applyTableAccessFullOptimization(orderByStep);
    if (newSubtreeRoot === orderByStep) {
      newSubtreeRoot = this.applyIndexRangeScanStepOptimization(orderByStep);
    }

    return newSubtreeRoot.getRoot() as PhysicalQueryPlanNode;
  }

  // Attempts to replace the OrderByStep with a new IndexRangeScanStep.
  private applyTableAccessFullOptimization(
    orderByStep: OrderByStep
  ): PhysicalQueryPlanNode {
    let rootNode: PhysicalQueryPlanNode = orderByStep;

    const tableAccessFullStep = this.findTableAccessFullStep(
      orderByStep.getChildAt(0) as PhysicalQueryPlanNode
    );
    if (tableAccessFullStep !== null) {
      const indexRangeCandidate = this.findIndexCandidateForOrderBy(
        tableAccessFullStep.table as BaseTable,
        orderByStep.orderBy
      );

      if (indexRangeCandidate === null) {
        // Could not find an index schema that can be leveraged.
        return rootNode;
      }

      const indexRangeScanStep = new IndexRangeScanStep(
        this.global,
        indexRangeCandidate.indexSchema,
        new UnboundedKeyRangeCalculator(indexRangeCandidate.indexSchema),
        indexRangeCandidate.isReverse
      );
      const tableAccessByRowIdStep = new TableAccessByRowIdStep(
        this.global,
        tableAccessFullStep.table
      );
      tableAccessByRowIdStep.addChild(indexRangeScanStep);

      TreeHelper.removeNode(orderByStep);
      rootNode = TreeHelper.replaceNodeWithChain(
        tableAccessFullStep,
        tableAccessByRowIdStep,
        indexRangeScanStep
      ) as PhysicalQueryPlanNode;
    }

    return rootNode;
  }

  // Attempts to replace the OrderByStep with an existing IndexRangeScanStep.
  private applyIndexRangeScanStepOptimization(
    orderByStep: OrderByStep
  ): PhysicalQueryPlanNode {
    let rootNode: PhysicalQueryPlanNode = orderByStep;
    const indexRangeScanStep = this.findIndexRangeScanStep(
      orderByStep.getChildAt(0) as PhysicalQueryPlanNode
    );
    if (indexRangeScanStep !== null) {
      const indexRangeCandidate = this.getIndexCandidateForIndexSchema(
        indexRangeScanStep.index,
        orderByStep.orderBy
      );

      if (indexRangeCandidate === null) {
        return rootNode;
      }

      indexRangeScanStep.reverseOrder = indexRangeCandidate.isReverse;
      rootNode = TreeHelper.removeNode(orderByStep)
        .parent as PhysicalQueryPlanNode;
    }

    return rootNode;
  }

  // Finds any existing IndexRangeScanStep that can potentially be used to
  // produce the requested ordering instead of the OrderByStep.
  private findIndexRangeScanStep(
    rootNode: PhysicalQueryPlanNode
  ): IndexRangeScanStep | null {
    const filterFn = (node: TreeNode) => node instanceof IndexRangeScanStep;

    // CrossProductStep/JoinStep/MultiIndexRangeScanStep nodes have more than
    // one child, and mess up the ordering of results. Therefore if such nodes
    // exist this optimization can not be applied.
    const stopFn = (node: TreeNode) => node.getChildCount() !== 1;

    const indexRangeScanSteps = TreeHelper.find(
      rootNode,
      filterFn,
      stopFn
    ) as IndexRangeScanStep[];
    return indexRangeScanSteps.length > 0 ? indexRangeScanSteps[0] : null;
  }

  // Finds any existing TableAccessFullStep that can potentially be converted to
  // an IndexRangeScanStep instead of using an explicit OrderByStep.
  private findTableAccessFullStep(
    rootNode: PhysicalQueryPlanNode
  ): TableAccessFullStep | null {
    const filterFn = (node: TreeNode) => node instanceof TableAccessFullStep;

    // CrossProductStep and JoinStep nodes have more than one child, and mess up
    // the ordering of results. Therefore if such nodes exist this optimization
    // can not be applied.
    const stopFn = (node: TreeNode) => node.getChildCount() !== 1;

    const tableAccessFullSteps = TreeHelper.find(
      rootNode,
      filterFn,
      stopFn
    ) as TableAccessFullStep[];
    return tableAccessFullSteps.length > 0 ? tableAccessFullSteps[0] : null;
  }

  // Finds the OrderByStep if it exists in the tree.
  private findOrderByStep(
    rootNode: PhysicalQueryPlanNode,
    queryContext: SelectContext
  ): OrderByStep | null {
    if (queryContext.orderBy === undefined) {
      // No ORDER BY exists.
      return null;
    }

    return TreeHelper.find(
      rootNode,
      node => node instanceof OrderByStep
    )[0] as OrderByStep;
  }

  private findIndexCandidateForOrderBy(
    tableSchema: BaseTable,
    orderBy: SelectContextOrderBy[]
  ): OrderByIndexRangeCandidate | null {
    let indexCandidate: OrderByIndexRangeCandidate | null = null;

    const indexSchemas = tableSchema.getIndices() as IndexImpl[];
    for (let i = 0; i < indexSchemas.length && indexCandidate === null; i++) {
      indexCandidate = this.getIndexCandidateForIndexSchema(
        indexSchemas[i],
        orderBy
      );
    }

    return indexCandidate;
  }

  // Determines whether the given index schema can be leveraged for producing
  // the ordering specified by the given orderBy.
  private getIndexCandidateForIndexSchema(
    indexSchema: IndexImpl,
    orderBy: SelectContextOrderBy[]
  ): OrderByIndexRangeCandidate | null {
    // First find an index schema which includes all columns to be sorted in the
    // same order.
    const columnsMatch =
      indexSchema.columns.length === orderBy.length &&
      orderBy.every((singleOrderBy, j) => {
        const indexedColumn = indexSchema.columns[j];
        return (
          singleOrderBy.column.getName() === indexedColumn.schema.getName()
        );
      });

    if (!columnsMatch) {
      return null;
    }

    // If columns match, determine whether the requested ordering within each
    // column matches the index, either in natural or reverse order.
    const isNaturalOrReverse = this.checkOrder(orderBy, indexSchema);

    if (!isNaturalOrReverse[0] && !isNaturalOrReverse[1]) {
      return null;
    }

    return {
      indexSchema,
      isReverse: isNaturalOrReverse[1],
    };
  }

  // Compares the order of each column in the orderBy and the indexSchema and
  // determines whether it is equal to the indexSchema 'natural' or 'reverse'
  // order.
  // Returns An array of 2 elements, where 1st element corresponds to isNatural
  // and 2nd to isReverse.
  private checkOrder(
    orderBy: SelectContextOrderBy[],
    indexSchema: IndexImpl
  ): [boolean, boolean] {
    // Converting orderBy orders to a bitmask.
    const ordersLeftBitmask = orderBy.reduce((soFar, columnOrderBy) => {
      return (soFar << 1) | (columnOrderBy.order === Order.DESC ? 0 : 1);
    }, 0);

    // Converting indexSchema orders to a bitmask.
    const ordersRightBitmask = indexSchema.columns.reduce(
      (soFar, indexedColumn) => {
        return (soFar << 1) | (indexedColumn.order === Order.DESC ? 0 : 1);
      },
      0
    );

    const xorBitmask = ordersLeftBitmask ^ ordersRightBitmask;
    const isNatural = xorBitmask === 0;
    const isReverse =
      xorBitmask ===
      Math.pow(2, Math.max(orderBy.length, indexSchema.columns.length)) - 1;

    return [isNatural, isReverse];
  }
}
