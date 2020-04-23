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

import { Table } from '../../schema/table';
import { PhysicalQueryPlanNode } from './physical_query_plan_node';

export class PhysicalQueryPlan {
  // Calculates the combined scope of the given list of physical query plans.
  static getCombinedScope(plans: PhysicalQueryPlan[]): Set<Table> {
    const tableSet = new Set<Table>();
    plans.forEach(plan => {
      plan.getScope().forEach(tableSet.add.bind(tableSet));
    });
    return tableSet;
  }

  constructor(
    private rootNode: PhysicalQueryPlanNode,
    private scope: Set<Table>
  ) {}

  getRoot(): PhysicalQueryPlanNode {
    return this.rootNode;
  }

  // Returns scope of this plan (i.e. tables involved)
  getScope(): Set<Table> {
    return this.scope;
  }
}
