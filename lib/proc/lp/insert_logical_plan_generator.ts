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

import { InsertContext } from '../../query/insert_context';
import { BaseLogicalPlanGenerator } from './base_logical_plan_generator';
import { InsertNode } from './insert_node';
import { InsertOrReplaceNode } from './insert_or_replace_node';
import { LogicalQueryPlanNode } from './logical_query_plan_node';

export class InsertLogicalPlanGenerator extends BaseLogicalPlanGenerator<
  InsertContext
> {
  constructor(query: InsertContext) {
    super(query);
  }

  generateInternal(): LogicalQueryPlanNode {
    return this.query.allowReplace
      ? new InsertOrReplaceNode(this.query.into, this.query.values)
      : new InsertNode(this.query.into, this.query.values);
  }
}
