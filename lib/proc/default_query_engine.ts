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

import {Global} from '../base/global';
import {Context} from '../query/context';
import {LogicalPlanFactory} from './lp/logical_plan_factory';
import {PhysicalPlanFactory} from './pp/physical_plan_factory';
import {PhysicalQueryPlan} from './pp/physical_query_plan';
import {QueryEngine} from './query_engine';

export class DefaultQueryEngine implements QueryEngine {
  private logicalPlanFactory: LogicalPlanFactory;
  private physicalPlanFactory: PhysicalPlanFactory;

  constructor(global: Global) {
    this.logicalPlanFactory = new LogicalPlanFactory();
    this.physicalPlanFactory = new PhysicalPlanFactory(global);
  }

  getPlan(query: Context): PhysicalQueryPlan {
    const logicalQueryPlan = this.logicalPlanFactory.create(query);
    return this.physicalPlanFactory.create(logicalQueryPlan, query);
  }
}
