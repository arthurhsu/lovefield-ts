/**
 * Copyright 2016 The Lovefield Project Authors. All Rights Reserved.
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

import {assert} from '../base/assert';
import {Row} from '../base/row';
import {Column} from '../schema/column';
import {setEquals} from '../structs/set_util';

import {RelationEntry} from './relation_entry';

export type AggregationResult = Relation | string | number;

export class Relation {
  // Creates an empty Relation instance. Since a relation is immutable, a
  // singleton "empty" relation instance is lazily instantiated and returned in
  // all subsequent calls.
  static createEmpty(): Relation {
    if (Relation.emptyRelation === null) {
      Relation.emptyRelation = new Relation([], []);
    }
    return Relation.emptyRelation;
  }

  // Finds the intersection of a given list of relations.
  static intersect(relations: Relation[]): Relation {
    if (relations.length === 0) {
      return Relation.createEmpty();
    }

    const totalCount = relations.reduce((soFar, relation) => {
      Relation.assertCompatible(relations[0], relation);
      return soFar + relation.entries.length;
    }, 0);
    const allEntries = new Array<RelationEntry>(totalCount);
    let entryCounter = 0;

    // Creating a map [entry.id --> entry] for each relation, and at the same
    // time populating the allEntries array.
    const relationMaps = relations.map(relation => {
      const map = new Map<number, RelationEntry>();
      relation.entries.forEach(entry => {
        allEntries[entryCounter++] = entry;
        map.set(entry.id, entry);
      });
      return map;
    });

    const intersection = new Map<number, RelationEntry>();
    allEntries.forEach(entry => {
      const existsInAll = relationMaps.every(relation =>
        relation.has(entry.id)
      );
      if (existsInAll) {
        intersection.set(entry.id, entry);
      }
    });

    return new Relation(
      Array.from(intersection.values()),
      Array.from(relations[0].tables.values())
    );
  }

  // Finds the union of a given list of relations.
  static union(relations: Relation[]): Relation {
    if (relations.length === 0) {
      return Relation.createEmpty();
    }

    const union = new Map<number, RelationEntry>();
    relations.forEach(relation => {
      Relation.assertCompatible(relations[0], relation);
      relation.entries.forEach(entry => union.set(entry.id, entry));
    });

    return new Relation(
      Array.from(union.values()),
      Array.from(relations[0].tables.values())
    );
  }

  // Creates an lf.proc.Relation instance from a set of lf.Row instances.
  static fromRows(rows: Row[], tables: string[]): Relation {
    const isPrefixApplied = tables.length > 1;
    const entries = rows.map(row => new RelationEntry(row, isPrefixApplied));
    return new Relation(entries, tables);
  }

  private static emptyRelation: Relation = (null as unknown) as Relation;

  // Asserts that two relations are compatible with regards to
  // union/intersection operations.
  private static assertCompatible(lhs: Relation, rhs: Relation): void {
    assert(
      lhs.isCompatible(rhs),
      'Intersection/union operations only apply to compatible relations.'
    );
  }

  private tables: Set<string>;

  // A map holding any aggregations that have been calculated for this relation.
  // Null if no aggregations have been calculated. The keys of the map
  // correspond to the normalized name of the aggregated column, for example
  // 'COUNT(*)' or 'MIN(Employee.salary)'.
  private aggregationResults: Map<string, AggregationResult>;

  constructor(readonly entries: RelationEntry[], tables: string[]) {
    this.tables = new Set(tables);
    this.aggregationResults = (null as unknown) as Map<
      string,
      AggregationResult
    >;
  }

  // Whether this is compatible with given relation in terms of calculating
  // union/intersection.
  isCompatible(relation: Relation): boolean {
    return setEquals(this.tables, relation.tables);
  }

  // Returns the names of all source tables of this relation.
  getTables(): string[] {
    return Array.from(this.tables.values());
  }

  // Whether prefixes have been applied to the payloads in this relation.
  isPrefixApplied(): boolean {
    return this.tables.size > 1;
  }

  getPayloads(): object[] {
    return this.entries.map(entry => entry.row.payload());
  }

  getRowIds(): number[] {
    return this.entries.map(entry => entry.row.id());
  }

  // Adds an aggregated result to this relation.
  setAggregationResult(column: Column, result: AggregationResult): void {
    if (this.aggregationResults === null) {
      this.aggregationResults = new Map<string, AggregationResult>();
    }
    this.aggregationResults.set(column.getNormalizedName(), result);
  }

  // Gets an already calculated aggregated result for this relation.
  getAggregationResult(column: Column): AggregationResult {
    assert(
      this.aggregationResults !== null,
      'getAggregationResult called before any results have been calculated.'
    );

    const colName = column.getNormalizedName();
    const result = this.aggregationResults.get(colName);
    assert(result !== undefined, `Could not find result for ${colName}`);
    return result as AggregationResult;
  }

  // Whether an aggregation result for the given aggregated column has been
  // calculated.
  hasAggregationResult(column: Column): boolean {
    return (
      this.aggregationResults !== null &&
      this.aggregationResults.has(column.getNormalizedName())
    );
  }
}
