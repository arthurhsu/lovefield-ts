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

import { ErrorCode } from '../../base/enum';
import { Exception } from '../../base/exception';
import { Context } from '../../query/context';
import { DeleteContext } from '../../query/delete_context';
import { InsertContext } from '../../query/insert_context';
import { SelectContext } from '../../query/select_context';
import { UpdateContext } from '../../query/update_context';
import { RewritePass } from '../rewrite_pass';

import { AndPredicatePass } from './and_predicate_pass';
import { CrossProductPass } from './cross_product_pass';
import { DeleteLogicalPlanGenerator } from './delete_logical_plan_generator';
import { ImplicitJoinsPass } from './implicit_joins_pass';
import { InsertLogicalPlanGenerator } from './insert_logical_plan_generator';
import { LogicalPlanGenerator } from './logical_plan_generator';
import { LogicalQueryPlan } from './logical_query_plan';
import { LogicalQueryPlanNode } from './logical_query_plan_node';
import { PushDownSelectionsPass } from './push_down_selections_pass';
import { SelectLogicalPlanGenerator } from './select_logical_plan_generator';
import { UpdateLogicalPlanGenerator } from './update_logical_generator';

// A factory used to create a logical query plan corresponding to a given query.
export class LogicalPlanFactory {
  private selectOptimizationPasses: Array<RewritePass<LogicalQueryPlanNode>>;
  private deleteOptimizationPasses: Array<RewritePass<LogicalQueryPlanNode>>;

  constructor() {
    this.selectOptimizationPasses = [
      new AndPredicatePass(),
      new CrossProductPass(),
      new PushDownSelectionsPass(),
      new ImplicitJoinsPass(),
    ];

    this.deleteOptimizationPasses = [new AndPredicatePass()];
  }

  create(query: Context): LogicalQueryPlan {
    let generator: LogicalPlanGenerator = (null as unknown) as LogicalPlanGenerator;
    if (query instanceof InsertContext) {
      generator = new InsertLogicalPlanGenerator(query);
    } else if (query instanceof DeleteContext) {
      generator = new DeleteLogicalPlanGenerator(
        query,
        this.deleteOptimizationPasses
      );
    } else if (query instanceof SelectContext) {
      generator = new SelectLogicalPlanGenerator(
        query,
        this.selectOptimizationPasses
      );
    } else if (query instanceof UpdateContext) {
      generator = new UpdateLogicalPlanGenerator(query);
    } else {
      // 513: Unknown query context.
      throw new Exception(ErrorCode.UNKNOWN_QUERY_CONTEXT);
    }

    const rootNode = generator.generate();
    return new LogicalQueryPlan(rootNode, query.getScope());
  }
}
