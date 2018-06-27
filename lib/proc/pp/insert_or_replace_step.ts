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
import {Service} from '../../base/service';
import {Journal} from '../../cache/journal';
import {IndexStore} from '../../index/index_store';
import {Context} from '../../query/context';
import {InsertContext} from '../../query/insert_context';
import {Table} from '../../schema/table';
import {Relation} from '../relation';

import {InsertStep} from './insert_step';
import {PhysicalQueryPlanNode} from './physical_query_plan_node';

export class InsertOrReplaceStep extends PhysicalQueryPlanNode {
  private indexStore: IndexStore;

  constructor(global: Global, private table: Table) {
    super(0, ExecType.NO_CHILD);
    this.indexStore = global.getService(Service.INDEX_STORE);
  }

  public toString(): string {
    return `insert_replace(${this.table.getName()})`;
  }

  public execInternal(relations: Relation[], journal?: Journal, ctx?: Context):
      Relation[] {
    const queryContext = context as any as InsertContext;
    InsertStep.assignAutoIncrementPks(
        this.table, queryContext.values, this.indexStore);
    (journal as Journal).insertOrReplace(this.table, queryContext.values);

    return [Relation.fromRows(queryContext.values, [this.table.getName()])];
  }
}
