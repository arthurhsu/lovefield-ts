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

import { Global } from '../../base/global';
import { ExecType } from '../../base/private_enum';
import { Service } from '../../base/service';
import { Journal } from '../../cache/journal';
import { fn } from '../../fn/fn';
import { IndexStore } from '../../index/index_store';
import { RuntimeIndex } from '../../index/runtime_index';
import { Context } from '../../query/context';
import { BaseTable } from '../../schema/base_table';
import { Table } from '../../schema/table';
import { Relation } from '../relation';
import { PhysicalQueryPlanNode } from './physical_query_plan_node';

export class GetRowCountStep extends PhysicalQueryPlanNode {
  private indexStore: IndexStore;

  constructor(global: Global, readonly table: Table) {
    super(0, ExecType.NO_CHILD);
    this.indexStore = global.getService(Service.INDEX_STORE);
  }

  toString(): string {
    return `get_row_count(${this.table.getName()})`;
  }

  execInternal(
    relations: Relation[],
    journal?: Journal,
    context?: Context
  ): Relation[] {
    const rowIdIndex = this.indexStore.get(
      (this.table as BaseTable).getRowIdIndexName()
    ) as RuntimeIndex;
    const relation = new Relation([], [this.table.getName()]);
    relation.setAggregationResult(fn.count(), rowIdIndex.stats().totalRows);
    return [relation];
  }
}
