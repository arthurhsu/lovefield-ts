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
import {UpdateContext} from '../../query/update_context';
import {BaseTable} from '../../schema/base_table';

import {Relation} from '../relation';
import {PhysicalQueryPlanNode} from './physical_query_plan_node';

export class UpdateStep extends PhysicalQueryPlanNode {
  constructor(private table: BaseTable) {
    super(1, ExecType.FIRST_CHILD);
  }

  public toString(): string {
    return `update(${this.table.getName()})`;
  }

  public execInternal(
      relations: Relation[], journal?: Journal, context?: Context): Relation[] {
    const rows = relations[0].entries.map((entry) => {
      // Need to clone the row here before modifying it, because it is a
      // direct reference to the cache's contents.
      const clone = this.table.deserializeRow(entry.row.serialize());

      (context as UpdateContext).set.forEach((update) => {
        clone.payload()[update.column.getName()] = update.value;
      }, this);
      return clone;
    }, this);
    (journal as Journal).update(this.table, rows);
    return [Relation.createEmpty()];
  }
}
