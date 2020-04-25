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

export class MultiIndexRangeScanStep extends PhysicalQueryPlanNode {
  constructor() {
    super(PhysicalQueryPlanNode.ANY, ExecType.ALL);
  }

  toString(): string {
    return 'multi_index_range_scan()';
  }

  execInternal(
    relations: Relation[],
    journal?: Journal,
    ctx?: Context
  ): Relation[] {
    // Calculate a new Relation that includes the union of the entries of all
    // relations. All child relations must be including rows from the same
    // table.
    const entriesUnion = new Map<number, RelationEntry>();
    relations.forEach(relation => {
      relation.entries.forEach(entry => {
        entriesUnion.set(entry.row.id(), entry);
      });
    });
    const entries = Array.from(entriesUnion.values());
    return [new Relation(entries, relations[0].getTables())];
  }
}
