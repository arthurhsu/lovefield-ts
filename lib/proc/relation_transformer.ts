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

import {FnType} from '../base/private_enum';
import {Row} from '../base/row';
import {AggregatedColumn} from '../fn/aggregated_column';
import {BaseColumn} from '../schema/base_column';
import {Relation} from './relation';
import {RelationEntry} from './relation_entry';

export class RelationTransformer {
  // Transforms a list of relations to a single relation. Each input relation is
  // transformed to a single entry on the final relation.
  // Note: Projection columns must include at least one aggregated column.
  // |relations|: The relations to be transformed.
  // |columns|: The columns to include in the transformed relation.
  public static transformMany(relations: Relation[], columns: BaseColumn[]):
      Relation {
    const entries = relations.map((relation) => {
      const relationTransformer = new RelationTransformer(relation, columns);
      const singleEntryRelation = relationTransformer.getTransformed();
      return singleEntryRelation.entries[0];
    });

    return new Relation(entries, relations[0].getTables());
  }

  constructor(private relation: Relation, private columns: BaseColumn[]) {}

  // Calculates a transformed Relation based on the columns that are requested.
  // The type of the requested columns affect the output (non-aggregate only VS
  // aggregate and non-aggregate mixed up).
  public getTransformed(): Relation {
    // Determine whether any aggregated columns have been requested.
    const aggregatedColumnsExist =
        this.columns.some((column) => column instanceof AggregatedColumn);

    return aggregatedColumnsExist ? this.handleAggregatedColumns() :
                                    this.handleNonAggregatedColumns();
  }

  // Generates the transformed relation for the case where the requested columns
  // include any aggregated columns.
  private handleAggregatedColumns(): Relation {
    // If the only aggregator that was used was DISTINCT, return the relation
    // corresponding to it.
    if (this.columns.length === 1 &&
        (this.columns[0] as AggregatedColumn).aggregatorType ===
            FnType.DISTINCT) {
      const distinctRelation: Relation =
          this.relation.getAggregationResult(this.columns[0]) as Relation;
      const newEntries = distinctRelation.entries.map((e) => {
        const newEntry = new RelationEntry(
            new Row(Row.DUMMY_ID, {}), this.relation.isPrefixApplied());
        newEntry.setField(
            this.columns[0],
            e.getField((this.columns[0] as AggregatedColumn).child));
        return newEntry;
      }, this);

      return new Relation(newEntries, []);
    }

    // Generate a new relation where there is only one entry, and within that
    // entry there is exactly one field per column.
    const entry = new RelationEntry(
        new Row(Row.DUMMY_ID, {}), this.relation.isPrefixApplied());
    this.columns.forEach((column) => {
      const value = column instanceof AggregatedColumn ?
          this.relation.getAggregationResult(column) :
          this.relation.entries[0].getField(column);
      entry.setField(column, value);
    }, this);

    return new Relation([entry], this.relation.getTables());
  }

  // Generates the transformed relation for the case where the requested columns
  // include only non-aggregated columns.
  private handleNonAggregatedColumns(): Relation {
    // Generate a new relation where each entry includes only the specified
    // columns.
    const transformedEntries: RelationEntry[] =
        new Array(this.relation.entries.length);
    const isPrefixApplied = this.relation.isPrefixApplied();

    this.relation.entries.forEach((entry, index) => {
      transformedEntries[index] =
          new RelationEntry(new Row(entry.row.id(), {}), isPrefixApplied);

      this.columns.forEach((column) => {
        transformedEntries[index].setField(column, entry.getField(column));
      }, this);
    }, this);

    return new Relation(transformedEntries, this.relation.getTables());
  }
}
