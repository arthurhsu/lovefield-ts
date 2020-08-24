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
import {IndexStore} from '../../index/index_store';
import {RuntimeIndex} from '../../index/runtime_index';
import {Context} from '../../query/context';
import {BaseTable} from '../../schema/base_table';
import {Table} from '../../schema/table';
import {Relation} from '../relation';
import {PhysicalQueryPlanNode} from './physical_query_plan_node';

export class TableAccessFullStep extends PhysicalQueryPlanNode {
  private cache: Cache;
  private indexStore: IndexStore;

  constructor(global: Global, readonly table: Table) {
    super(0, ExecType.NO_CHILD);
    this.cache = global.getService(Service.CACHE);
    this.indexStore = global.getService(Service.INDEX_STORE);
  }

  toString(): string {
    let postfix = '';
    const table = this.table as BaseTable;
    if (table.getAlias()) {
      postfix = ` as ${table.getAlias()}`;
    }
    return `table_access(${this.table.getName()}${postfix})`;
  }

  execInternal(
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    relations: Relation[],
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    journal?: Journal,
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    context?: Context
  ): Relation[] {
    const table = this.table as BaseTable;
    const rowIds = (this.indexStore.get(
      table.getRowIdIndexName()
    ) as RuntimeIndex).getRange();

    return [
      Relation.fromRows(this.cache.getMany(rowIds) as Row[], [
        table.getEffectiveName(),
      ]),
    ];
  }
}
