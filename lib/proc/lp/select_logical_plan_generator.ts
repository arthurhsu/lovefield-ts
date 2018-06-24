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

import {AggregatedColumn} from '../../fn/aggregated_column';
import {SelectContext} from '../../query/select_context';
import {AggregationNode} from './aggregation_node';
import {BaseLogicalPlanGenerator} from './base_logical_plan_generator';
import {CrossProductNode} from './cross_product_node';
import {GroupByNode} from './group_by_node';
import {LimitNode} from './limit_node';
import {LogicalPlanRewriter} from './logical_plan_rewriter';
import {LogicalQueryPlanNode} from './logical_query_plan_node';
import {OrderByNode} from './orderby_node';
import {ProjectNode} from './project_node';
import {RewritePass} from './rewrite_pass';
import {SelectNode} from './select_node';
import {SkipNode} from './skip_node';
import {TableAccessNode} from './table_access_node';

export class SelectLogicalPlanGenerator extends
    BaseLogicalPlanGenerator<SelectContext> {
  private rewritePasses: Array<RewritePass<LogicalQueryPlanNode>>;
  private tableAccessNodes: LogicalQueryPlanNode[];
  private crossProductNode: LogicalQueryPlanNode;
  private selectNode: LogicalQueryPlanNode;
  private groupByNode: LogicalQueryPlanNode;
  private aggregationNode: LogicalQueryPlanNode;
  private orderByNode: LogicalQueryPlanNode;
  private skipNode: LogicalQueryPlanNode;
  private limitNode: LogicalQueryPlanNode;
  private projectNode: LogicalQueryPlanNode;

  constructor(
      query: SelectContext,
      rewritePasses: Array<RewritePass<LogicalQueryPlanNode>>) {
    super(query);
    this.rewritePasses = rewritePasses;
    this.tableAccessNodes = null as any as LogicalQueryPlanNode[];
    this.crossProductNode = null as any as LogicalQueryPlanNode;
    this.selectNode = null as any as LogicalQueryPlanNode;
    this.groupByNode = null as any as LogicalQueryPlanNode;
    this.aggregationNode = null as any as LogicalQueryPlanNode;
    this.orderByNode = null as any as LogicalQueryPlanNode;
    this.skipNode = null as any as LogicalQueryPlanNode;
    this.limitNode = null as any as LogicalQueryPlanNode;
    this.projectNode = null as any as LogicalQueryPlanNode;
  }

  public generateInternal(): LogicalQueryPlanNode {
    this.generateNodes();
    const rootNode = this.connectNodes();

    // Optimizing the "naive" logical plan.
    const planRewriter =
        new LogicalPlanRewriter(rootNode, this.query, this.rewritePasses);
    return planRewriter.generate();
  }

  // Generates all the nodes that will make up the logical plan tree. After
  // this function returns all nodes have been created, but they are not yet
  // connected to each other.
  private generateNodes(): void {
    this.generateTableAccessNodes();
    this.generateCrossProductNode();
    this.generateSelectNode();
    this.generateOrderByNode();
    this.generateSkipNode();
    this.generateLimitNode();
    this.generateGroupByNode();
    this.generateAggregationNode();
    this.generateProjectNode();
  }

  // Connects the nodes together such that the logical plan tree is formed.
  private connectNodes(): LogicalQueryPlanNode {
    const parentOrder = [
      this.limitNode,
      this.skipNode,
      this.projectNode,
      this.orderByNode,
      this.aggregationNode,
      this.groupByNode,
      this.selectNode,
      this.crossProductNode,
    ];

    let lastExistingParentIndex = -1;
    let rootNode: LogicalQueryPlanNode = null as any as LogicalQueryPlanNode;
    for (let i = 0; i < parentOrder.length; i++) {
      const node = parentOrder[i];
      if (node !== null) {
        if (rootNode === null) {
          rootNode = node;
        } else {
          parentOrder[lastExistingParentIndex].addChild(node);
        }
        lastExistingParentIndex = i;
      }
    }

    this.tableAccessNodes.forEach((tableAccessNode) => {
      parentOrder[lastExistingParentIndex].addChild(tableAccessNode);
    });

    return rootNode;
  }

  private generateTableAccessNodes(): void {
    this.tableAccessNodes =
        this.query.from.map((table) => new TableAccessNode(table));
  }

  private generateCrossProductNode(): void {
    if (this.query.from.length >= 2) {
      this.crossProductNode = new CrossProductNode();
    }
  }

  private generateSelectNode(): void {
    if (this.query.where) {
      this.selectNode = new SelectNode(this.query.where.copy());
    }
  }

  private generateOrderByNode(): void {
    if (this.query.orderBy) {
      this.orderByNode = new OrderByNode(this.query.orderBy);
    }
  }

  private generateSkipNode(): void {
    if (this.query.skip && this.query.skip > 0) {
      this.skipNode = new SkipNode(this.query.skip);
    }
  }

  private generateLimitNode(): void {
    if (this.query.limit !== undefined && this.query.limit !== null) {
      this.limitNode = new LimitNode(this.query.limit);
    }
  }

  private generateGroupByNode(): void {
    if (this.query.groupBy) {
      this.groupByNode = new GroupByNode(this.query.groupBy);
    }
  }

  private generateAggregationNode(): void {
    const aggregatedColumns = this.query.columns.filter((column) => {
      return column instanceof AggregatedColumn;
    });

    if (this.query.orderBy) {
      this.query.orderBy.forEach((orderBy) => {
        if (orderBy.column instanceof AggregatedColumn) {
          aggregatedColumns.push(orderBy.column);
        }
      });
    }

    if (aggregatedColumns.length > 0) {
      this.aggregationNode =
          new AggregationNode(aggregatedColumns as any as AggregatedColumn[]);
    }
  }

  private generateProjectNode(): void {
    this.projectNode =
        new ProjectNode(this.query.columns || [], this.query.groupBy || null);
  }
}
