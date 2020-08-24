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
import {Context} from '../../query/context';
import {Column} from '../../schema/column';
import {MapSet} from '../../structs/map_set';
import {Relation} from '../relation';
import {RelationEntry} from '../relation_entry';

import {PhysicalQueryPlanNode} from './physical_query_plan_node';

export class GroupByStep extends PhysicalQueryPlanNode {
  constructor(private groupByColumns: Column[]) {
    super(1, ExecType.FIRST_CHILD);
  }

  toString(): string {
    const columnNames = this.groupByColumns.map(column =>
      column.getNormalizedName()
    );
    return `groupBy(${columnNames.toString()})`;
  }

  execInternal(
    relations: Relation[],
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    journal?: Journal,
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    ctx?: Context
  ): Relation[] {
    return this.calculateGroupedRelations(relations[0]);
  }

  // Breaks down a single relation to multiple relations by grouping rows based
  // on the specified groupBy columns.
  private calculateGroupedRelations(relation: Relation): Relation[] {
    const groupMap = new MapSet<string, RelationEntry>();

    const getKey = (entry: RelationEntry) => {
      const keys = this.groupByColumns.map(column => entry.getField(column));
      return keys.join(',');
    };

    relation.entries.forEach(entry => groupMap.set(getKey(entry), entry));
    return groupMap.keys().map(key => {
      return new Relation(
        groupMap.get(key) as RelationEntry[],
        relation.getTables()
      );
    });
  }
}
