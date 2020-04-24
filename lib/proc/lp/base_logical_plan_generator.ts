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

import { Context } from '../../query/context';
import { LogicalPlanGenerator } from './logical_plan_generator';
import { LogicalQueryPlanNode } from './logical_query_plan_node';

// TODO(arthurhsu): this abstract base class is not necessary. Refactor to
// remove and simplify code structure.
export abstract class BaseLogicalPlanGenerator<T extends Context>
  implements LogicalPlanGenerator {
  private rootNode: LogicalQueryPlanNode;

  constructor(protected query: T) {
    this.rootNode = (null as unknown) as LogicalQueryPlanNode;
  }

  generate(): LogicalQueryPlanNode {
    if (this.rootNode === null) {
      this.rootNode = this.generateInternal();
    }

    return this.rootNode;
  }

  abstract generateInternal(): LogicalQueryPlanNode;
}
