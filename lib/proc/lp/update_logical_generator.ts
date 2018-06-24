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

import {UpdateContext} from '../../query/update_context';
import {BaseLogicalPlanGenerator} from './base_logical_plan_generator';
import {LogicalQueryPlanNode} from './logical_query_plan_node';
import {SelectNode} from './select_node';
import {TableAccessNode} from './table_access_node';
import {UpdateNode} from './update_node';

export class UpdateLogicalPlanGenerator extends
    BaseLogicalPlanGenerator<UpdateContext> {
  constructor(query: UpdateContext) {
    super(query);
  }

  public generateInternal(): LogicalQueryPlanNode {
    const updateNode = new UpdateNode(this.query.table);
    const selectNode = this.query.where !== null ?
        new SelectNode(this.query.where.copy()) :
        null;
    const tableAccessNode = new TableAccessNode(this.query.table);

    if (selectNode === null) {
      updateNode.addChild(tableAccessNode);
    } else {
      selectNode.addChild(tableAccessNode);
      updateNode.addChild(selectNode);
    }

    return updateNode;
  }
}
