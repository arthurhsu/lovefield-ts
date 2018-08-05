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

import {FnType} from '../../base/private_enum';
import {AggregatedColumn} from '../../fn/aggregated_column';
import {StarColumn} from '../../fn/star_column';
import {BaseColumn} from '../../schema/base_column';
import {MathHelper} from '../../structs/math_helper';
import {AggregationResult, Relation} from '../relation';
import {RelationEntry} from '../relation_entry';

type AggregatorValueType = number|string|Date|null;

export class AggregationCalculator {
  constructor(private relation: Relation, private columns: AggregatedColumn[]) {
  }

  // Calculates all requested aggregations. Results are stored within
  // this.relation.
  public calculate(): void {
    this.columns.forEach((column) => {
      const reverseColumnChain = column.getColumnChain().reverse();
      for (let i = 1; i < reverseColumnChain.length; i++) {
        const currentColumn = reverseColumnChain[i] as AggregatedColumn;
        const leafColumn = currentColumn.getColumnChain().slice(-1)[0];
        const inputRelation = this.getInputRelationFor(currentColumn);

        // Return early if the aggregation result has already been calculated.
        if (inputRelation.hasAggregationResult(currentColumn)) {
          return;
        }

        const result = this.evalAggregation(
            currentColumn.aggregatorType, inputRelation, leafColumn);
        this.relation.setAggregationResult(currentColumn, result);
      }
    }, this);
  }

  // Returns the relation that should be used as input for calculating the
  // given aggregated column.
  private getInputRelationFor(column: AggregatedColumn): Relation {
    return column.child instanceof AggregatedColumn ?
        this.relation.getAggregationResult(column.child) as Relation :
        this.relation;
  }

  private evalAggregation(
      aggregatorType: FnType, relation: Relation,
      column: BaseColumn): AggregationResult {
    let result: AggregationResult = null as any as AggregationResult;

    switch (aggregatorType) {
      case FnType.MIN:
        result = this.reduce(
                     relation, column,
                     (soFar, value) => (value < soFar ? value : soFar)) as
            AggregationResult;
        break;
      case FnType.MAX:
        result = this.reduce(
                     relation, column,
                     (soFar, value) => (value > soFar ? value : soFar)) as
            AggregationResult;
        break;
      case FnType.DISTINCT:
        result = this.distinct(relation, column);
        break;
      case FnType.COUNT:
        result = this.count(relation, column);
        break;
      case FnType.SUM:
        result = this.sum(relation, column);
        break;
      case FnType.AVG:
        const count = this.count(relation, column);
        if (count > 0) {
          result = this.sum(relation, column) as number / count;
        }
        break;
      case FnType.GEOMEAN:
        result = this.geomean(relation, column) as AggregationResult;
        break;
      default:
        // Must be case of FnType.STDDEV.
        result = this.stddev(relation, column) as AggregationResult;
        break;
    }

    return result;
  }

  // Reduces the input relation to a single value. Null values are ignored.
  private reduce(
      relation: Relation, column: BaseColumn,
      reduceFn: (cur: any, v: any) => AggregatorValueType):
      AggregatorValueType {
    return relation.entries.reduce((soFar: AggregatorValueType, entry) => {
      const value: AggregatorValueType = entry.getField(column);
      if (value === null) {
        return soFar;
      }
      return (soFar === null) ? value : reduceFn(soFar, value);
    }, null);
  }

  // Calculates the count of the given column for the given relation.
  // COUNT(*) returns count of all rows but COUNT(column) ignores nulls
  // in that column.
  private count(relation: Relation, column: BaseColumn): number {
    if (column instanceof StarColumn) {
      return relation.entries.length;
    }
    return relation.entries.reduce((soFar, entry) => {
      return soFar + (entry.getField(column) === null ? 0 : 1);
    }, 0);
  }

  // Calculates the sum of the given column for the given relation.
  // If all rows have only value null for that column, then null is returned.
  // If the table is empty, null is returned.
  private sum(relation: Relation, column: BaseColumn): number|string {
    return this.reduce(relation, column, (soFar, value) => value + soFar) as
        number |
        string;
  }

  // Calculates the standard deviation of the given column for the given
  // relation. If all rows have only value null for that column, then null is
  // returned. If the table is empty, null is returned.
  private stddev(relation: Relation, column: BaseColumn): number|null {
    const values: number[] = [];
    relation.entries.forEach((entry) => {
      const value = entry.getField(column);
      if (value !== null) {
        values.push(value);
      }
    });

    return values.length === 0 ?
        null :
        MathHelper.standardDeviation.apply(null, values);
  }

  // Calculates the geometrical mean of the given column for the given relation.
  // Zero values are ignored. If all values given are zero, or if the input
  // relation is empty, null is returned.
  private geomean(relation: Relation, column: BaseColumn): number|null {
    let nonZeroEntriesCount = 0;

    const reduced = relation.entries.reduce((soFar, entry) => {
      const value = entry.getField(column);
      if (value !== null && value !== 0) {
        nonZeroEntriesCount++;
        return soFar + Math.log(value);
      } else {
        return soFar;
      }
    }, 0);

    return nonZeroEntriesCount === 0 ?
        null :
        Math.pow(Math.E, reduced / nonZeroEntriesCount);
  }

  // Keeps only distinct entries with regards to the given column.
  private distinct(relation: Relation, column: BaseColumn): Relation {
    const distinctMap = new Map<any, RelationEntry>();

    relation.entries.forEach((entry) => {
      const value = entry.getField(column);
      distinctMap.set(value, entry);
    });

    return new Relation(Array.from(distinctMap.values()), relation.getTables());
  }
}
