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
import {Relation} from '../relation';
import {RelationEntry} from '../relation_entry';
import {PhysicalQueryPlanNode} from './physical_query_plan_node';

export class CrossProductStep extends PhysicalQueryPlanNode {
  constructor() {
    super(2, ExecType.ALL);
  }

  toString(): string {
    return 'cross_product';
  }

  execInternal(
    relations: Relation[],
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    journal?: Journal,
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    context?: Context
  ): Relation[] {
    return this.crossProduct(relations[0], relations[1]);
  }

  // Calculates the cross product of two relations.
  private crossProduct(
    leftRelation: Relation,
    rightRelation: Relation
  ): Relation[] {
    const combinedEntries: RelationEntry[] = [];

    const leftRelationTableNames = leftRelation.getTables();
    const rightRelationTableNames = rightRelation.getTables();
    leftRelation.entries.forEach(le => {
      rightRelation.entries.forEach(re => {
        const combinedEntry = RelationEntry.combineEntries(
          le,
          leftRelationTableNames,
          re,
          rightRelationTableNames
        );
        combinedEntries.push(combinedEntry);
      });
    });

    const srcTables = leftRelation
      .getTables()
      .concat(rightRelation.getTables());
    return [new Relation(combinedEntries, srcTables)];
  }
}
