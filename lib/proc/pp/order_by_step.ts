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
import {IndexableType} from '../../base/eval';
import {ExecType} from '../../base/private_enum';
import {Journal} from '../../cache/journal';
import {AggregatedColumn} from '../../fn/aggregated_column';
import {fn} from '../../fn/fn';
import {Context} from '../../query/context';
import {SelectContext, SelectContextOrderBy} from '../../query/select_context';
import {Column} from '../../schema/column';

import {Relation} from '../relation';
import {RelationEntry} from '../relation_entry';
import {PhysicalQueryPlanNode} from './physical_query_plan_node';

export class OrderByStep extends PhysicalQueryPlanNode {
  constructor(readonly orderBy: SelectContextOrderBy[]) {
    super(PhysicalQueryPlanNode.ANY, ExecType.FIRST_CHILD);
  }

  toString(): string {
    return `order_by(${SelectContext.orderByToString(this.orderBy)})`;
  }

  execInternal(
    relations: Relation[],
    journal?: Journal,
    context?: Context
  ): Relation[] {
    if (relations.length === 1) {
      const distinctColumn = this.findDistinctColumn(relations[0]);

      // If such a column exists, sort the results of the lf.fn.distinct
      // aggregator instead, since this is what will be used in the returned
      // result.
      const relationToSort =
        distinctColumn === null
          ? relations[0]
          : (relations[0].getAggregationResult(distinctColumn) as Relation);

      relationToSort.entries.sort(this.entryComparatorFn.bind(this));
    } else {
      // if (relations.length > 1) {
      relations.sort(this.relationComparatorFn.bind(this));
    }
    return relations;
  }

  // Determines whether sorting is requested on a column that has been
  // aggregated with lf.fn.distinct (if any).
  private findDistinctColumn(relation: Relation): Column | null {
    let distinctColumn: Column | null = null;

    this.orderBy.every(entry => {
      const tempDistinctColumn = fn.distinct(entry.column);
      if (relation.hasAggregationResult(tempDistinctColumn)) {
        distinctColumn = tempDistinctColumn;
        return false;
      }
      return true;
    }, this);
    return distinctColumn;
  }

  // Returns -1 if a should precede b, 1 if b should precede a, 0 if a and b
  // are determined to be equal.
  private comparator(
    getLeftPayload: (col: Column) => IndexableType,
    getRightPayload: (col: Column) => IndexableType
  ): number {
    let order: Order;
    let leftPayload = null;
    let rightPayload = null;
    let comparisonIndex = -1;

    do {
      comparisonIndex++;
      const column = this.orderBy[comparisonIndex].column;
      order = this.orderBy[comparisonIndex].order;
      leftPayload = getLeftPayload(column);
      rightPayload = getRightPayload(column);
    } while (
      leftPayload === rightPayload &&
      comparisonIndex + 1 < this.orderBy.length
    );

    let result =
      leftPayload < rightPayload ? -1 : leftPayload > rightPayload ? 1 : 0;
    result = order === Order.ASC ? result : -result;
    return result;
  }

  private entryComparatorFn(lhs: RelationEntry, rhs: RelationEntry): number {
    // NOTE: Avoiding on purpose to create a getPayload(operand, column) method
    // here, and binding it once to lhs and once to rhs, because it turns out
    // that Function.bind() is significantly hurting performance (measured on
    // Chrome 40).
    return this.comparator(
      column => lhs.getField(column) as IndexableType,
      column => rhs.getField(column) as IndexableType
    );
  }

  private relationComparatorFn(lhs: Relation, rhs: Relation): number {
    // NOTE: See NOTE in entryComparatorFn_ on why two separate functions are
    // passed in this.comparator_ instead of using one method and binding to lhs
    // and to rhs respectively.
    return this.comparator(
      column => {
        // If relations are sorted based on a non-aggregated column, choose
        // the last entry of each relation as a representative row (same as
        // SQLite).
        return (column instanceof AggregatedColumn
          ? lhs.getAggregationResult(column)
          : lhs.entries[lhs.entries.length - 1].getField(
              column
            )) as IndexableType;
      },
      column => {
        return (column instanceof AggregatedColumn
          ? rhs.getAggregationResult(column)
          : rhs.entries[rhs.entries.length - 1].getField(
              column
            )) as IndexableType;
      }
    );
  }
}
