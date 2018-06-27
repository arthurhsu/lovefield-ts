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

import {Global} from '../../base/global';
import {ExecType} from '../../base/private_enum';
import {Row} from '../../base/row';
import {Service} from '../../base/service';
import {Cache} from '../../cache/cache';
import {Journal} from '../../cache/journal';
import {Context} from '../../query/context';
import {Table} from '../../schema/table';
import {Relation} from '../relation';

import {PhysicalQueryPlanNode} from './physical_query_plan_node';

export class TableAccessByRowIdStep extends PhysicalQueryPlanNode {
  private cache: Cache;
  constructor(global: Global, private table: Table) {
    super(1, ExecType.FIRST_CHILD);
    this.cache = global.getService(Service.CACHE);
  }

  public toString(): string {
    return `table_access_by_row_id(${this.table.getName()})`;
  }

  public execInternal(relations: Relation[], journal?: Journal, ctx?: Context):
      Relation[] {
    return [Relation.fromRows(
        this.cache.getMany(relations[0].getRowIds()) as Row[],
        [this.table.getEffectiveName()])];
  }
}
