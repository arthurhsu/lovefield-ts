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

import { ExecType } from '../../base/private_enum';
import { Journal } from '../../cache/journal';
import { AggregatedColumn } from '../../fn/aggregated_column';
import { Context } from '../../query/context';
import { Relation } from '../relation';
import { AggregationCalculator } from './aggregation_calculator';
import { PhysicalQueryPlanNode } from './physical_query_plan_node';

export class AggregationStep extends PhysicalQueryPlanNode {
  constructor(readonly aggregatedColumns: AggregatedColumn[]) {
    super(PhysicalQueryPlanNode.ANY, ExecType.FIRST_CHILD);
  }

  toString(): string {
    const columnNames = this.aggregatedColumns.map(column =>
      column.getNormalizedName()
    );

    return `aggregation(${columnNames.toString()})`;
  }

  execInternal(
    relations: Relation[],
    journal?: Journal,
    context?: Context
  ): Relation[] {
    relations.forEach(relation => {
      const calculator = new AggregationCalculator(
        relation,
        this.aggregatedColumns
      );
      calculator.calculate();
    }, this);
    return relations;
  }
}
