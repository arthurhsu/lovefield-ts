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

import {BaseTable} from '../../schema/base_table';
import {Table} from '../../schema/table';
import {LogicalQueryPlanNode} from './logical_query_plan_node';

export class TableAccessNode extends LogicalQueryPlanNode {
  constructor(readonly table: Table) {
    super();
  }

  toString(): string {
    const table = this.table as BaseTable;
    const postfix = table.getAlias() ? ` as ${table.getAlias()}` : '';
    return `table_access(${this.table.getName()}${postfix})`;
  }
}
