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
import {ObserverRegistry} from '../base/observer_registry';
import {TaskPriority} from '../base/private_enum';
import {Service} from '../base/service';
import {SelectContext} from '../query/select_context';
import {QueryTask} from './query_task';
import {Relation} from './relation';
import {TaskItem} from './task_item';

export class ObserverQueryTask extends QueryTask {
  private observerRegistry: ObserverRegistry;

  constructor(global: Global, items: TaskItem[]) {
    super(global, items);
    this.observerRegistry = global.getService(Service.OBSERVER_REGISTRY);
  }

  public getPriority(): TaskPriority {
    return TaskPriority.OBSERVER_QUERY_TASK;
  }

  public onSuccess(results: Relation[]): void {
    this.queries.forEach((query, index) => {
      this.observerRegistry.updateResultsForQuery(
          query as SelectContext, results[index]);
    });
  }
}
