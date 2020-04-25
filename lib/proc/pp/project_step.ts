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

import {ExecType} from '../../base/private_enum';
import {Journal} from '../../cache/journal';
import {AggregatedColumn} from '../../fn/aggregated_column';
import {Context} from '../../query/context';
import {Column} from '../../schema/column';
import {Relation} from '../relation';
import {RelationTransformer} from '../relation_transformer';
import {PhysicalQueryPlanNode} from './physical_query_plan_node';

export class ProjectStep extends PhysicalQueryPlanNode {
  constructor(private columns: Column[], private groupByColumns: Column[]) {
    super(PhysicalQueryPlanNode.ANY, ExecType.FIRST_CHILD);
  }

  toString(): string {
    let postfix = '';
    if (this.groupByColumns) {
      const groupBy = this.groupByColumns
        .map(col => col.getNormalizedName())
        .join(', ');
      postfix = `, groupBy(${groupBy})`;
    }
    return `project(${this.columns.toString()}${postfix})`;
  }

  execInternal(
    relations: Relation[],
    journal?: Journal,
    context?: Context
  ): Relation[] {
    if (relations.length === 0) {
      return [Relation.createEmpty()];
    } else if (relations.length === 1) {
      return [this.execNonGroupByProjection(relations[0])];
    } else {
      return [this.execGroupByProjection(relations)];
    }
  }

  // Returns whether any aggregators (either columns or groupBy) have been
  // specified.
  hasAggregators(): boolean {
    const hasAggregators = this.columns.some(column => {
      return column instanceof AggregatedColumn;
    });
    return hasAggregators || this.groupByColumns !== null;
  }

  // Calculates the final relation for the case where GROUP_BY exists.
  private execGroupByProjection(relations: Relation[]): Relation {
    return RelationTransformer.transformMany(relations, this.columns);
  }

  // Calculates the final relation for the case where no GROUP_BY exists.
  private execNonGroupByProjection(relation: Relation): Relation {
    if (this.columns.length === 0) {
      return relation;
    }
    const relationTransformer = new RelationTransformer(relation, this.columns);
    return relationTransformer.getTransformed();
  }
}
