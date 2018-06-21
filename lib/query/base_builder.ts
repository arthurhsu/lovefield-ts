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
import {Service} from '../base/service';
import {PhysicalQueryPlan} from '../proc/pp/physical_query_plan';
import {PhysicalQueryPlanNode} from '../proc/pp/physical_query_plan_node';
import {QueryEngine} from '../proc/query_engine';
import {Runner} from '../proc/runner';
import {TaskItem} from '../proc/task_item';
import {UserQueryTask} from '../proc/user_query_task';
import {TreeHelper} from '../structs/tree_helper';
import {TreeNode} from '../structs/tree_node';
import {Context} from './context';
import {QueryBuilder} from './query_builder';

export class BaseBuilder<CONTEXT extends Context> implements QueryBuilder {
  protected global: Global;
  protected query: CONTEXT;

  private queryEngine: QueryEngine;
  private runner: Runner;
  private plan!: PhysicalQueryPlan;

  constructor(global: Global, context: Context) {
    this.global = global;
    this.queryEngine = global.getService(Service.QUERY_ENGINE);
    this.runner = global.getService(Service.RUNNER);
    this.query = context as CONTEXT;
  }

  public exec(): Promise<any> {
    try {
      this.assertExecPreconditions();
    } catch (e) {
      return Promise.reject(e);
    }

    return new Promise((resolve, reject) => {
      const queryTask = new UserQueryTask(this.global, [this.getTaskItem()]);
      this.runner.scheduleTask(queryTask).then(
          (results) => resolve(results[0].getPayloads()), reject);
    });
  }

  public explain(): string {
    const stringFn = (node: TreeNode) =>
        `${(node as PhysicalQueryPlanNode).toContextString(this.query)}\n`;
    return TreeHelper.toString(this.getPlan().getRoot(), stringFn);
  }

  public bind(values: any[]): QueryBuilder {
    this.query.bind(values);
    return this;
  }

  public toSql(stripValueInfo = false): string {
    // TODO(arthurhsu): implement
    // return QueryHelper.toSql(this, stripValueInfo);
    return 'not implemented';
  }

  // Asserts whether the preconditions for executing this query are met. Should
  // be overridden by subclasses.
  public assertExecPreconditions(): void {
    // No-op default implementation.
  }

  public getQuery(): CONTEXT {
    return this.query.clone() as CONTEXT;
  }

  public getObservableQuery(): CONTEXT {
    return this.query.clone() as CONTEXT;
  }

  public getTaskItem(): TaskItem {
    return {
      context: this.getQuery(),
      plan: this.getPlan(),
    };
  }

  public getObservableTaskItem(): TaskItem {
    return {
      context: this.getObservableQuery(),
      plan: this.getPlan(),
    };
  }

  private getPlan(): PhysicalQueryPlan {
    if (this.plan !== undefined && this.plan !== null) {
      this.plan = this.queryEngine.getPlan(this.query);
    }
    return this.plan;
  }
}
