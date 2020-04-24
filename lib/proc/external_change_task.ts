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

import { TransactionType } from '../base/enum';
import { Global } from '../base/global';
import { ObserverRegistry } from '../base/observer_registry';
import { TaskPriority } from '../base/private_enum';
import { Resolver } from '../base/resolver';
import { Service } from '../base/service';
import { UniqueId } from '../base/unique_id';
import { InMemoryUpdater } from '../cache/in_memory_updater';
import { TableDiff } from '../cache/table_diff';
import { Table } from '../schema/table';

import { ObserverQueryTask } from './observer_query_task';
import { Relation } from './relation';
import { Runner } from './runner';
import { Task } from './task';

export class ExternalChangeTask extends UniqueId implements Task {
  private observerRegistry: ObserverRegistry;
  private runner: Runner;
  private inMemoryUpdater: InMemoryUpdater;
  private scope: Set<Table>;
  private resolver: Resolver<Relation[]>;

  constructor(private global: Global, private tableDiffs: TableDiff[]) {
    super();
    this.observerRegistry = this.global.getService(Service.OBSERVER_REGISTRY);
    this.runner = this.global.getService(Service.RUNNER);
    this.inMemoryUpdater = new InMemoryUpdater(this.global);

    const dbSchema = this.global.getService(Service.SCHEMA);
    const tableSchemas = this.tableDiffs.map(td =>
      dbSchema.table(td.getName())
    );
    this.scope = new Set<Table>(tableSchemas);
    this.resolver = new Resolver<Relation[]>();
  }

  exec(): Promise<Relation[]> {
    this.inMemoryUpdater.update(this.tableDiffs);
    this.scheduleObserverTask();
    return Promise.resolve([]);
  }

  getType(): TransactionType {
    return TransactionType.READ_WRITE;
  }

  getScope(): Set<Table> {
    return this.scope;
  }

  getResolver(): Resolver<Relation[]> {
    return this.resolver;
  }

  getId(): number {
    return this.getUniqueNumber();
  }

  getPriority(): TaskPriority {
    return TaskPriority.EXTERNAL_CHANGE_TASK;
  }

  // Schedules an ObserverTask for any observed queries that need to be
  // re-executed, if any.
  private scheduleObserverTask(): void {
    const items = this.observerRegistry.getTaskItemsForTables(
      Array.from(this.scope.values())
    );
    if (items.length !== 0) {
      const observerTask = new ObserverQueryTask(this.global, items);
      this.runner.scheduleTask(observerTask);
    }
  }
}
