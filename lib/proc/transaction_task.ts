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

import {BackStore} from '../backstore/back_store';
import {TransactionStats} from '../backstore/transaction_stats';
import {Tx} from '../backstore/tx';
import {TransactionType} from '../base/enum';
import {Global} from '../base/global';
import {ObserverRegistry} from '../base/observer_registry';
import {TaskPriority} from '../base/private_enum';
import {Resolver} from '../base/resolver';
import {Service} from '../base/service';
import {UniqueId} from '../base/unique_id';
import {Journal} from '../cache/journal';
import {QueryBuilder} from '../query/query_builder';
import {Table} from '../schema/table';

import {ObserverQueryTask} from './observer_query_task';
import {Relation} from './relation';
import {Runner} from './runner';
import {Task} from './task';

// A TransactionTask is used when the user explicitly starts a transaction and
// can execute queries within this transaction at will. A TransactionTask is
// posted to the Runner to ensure that all required locks have been acquired
// before any queries are executed. Any queries that are performed as part of a
// TransactionTask will not be visible to lf.proc.Runner at all (no
// corresponding QueryTask will be posted). Once the transaction is finalized,
// it will appear to the lf.proc.Runner that this task finished and all locks
// will be released, exactly as is done for any type of Task.
export class TransactionTask extends UniqueId implements Task {
  private backStore: BackStore;
  private runner: Runner;
  private observerRegistry: ObserverRegistry;
  private scope: Set<Table>;
  private journal: Journal;
  private resolver: Resolver<Relation[]>;
  private execResolver: Resolver<Relation[]>;
  private acquireScopeResolver: Resolver<void>;
  private tx!: Tx;

  constructor(private global: Global, scope: Table[]) {
    super();
    this.backStore = global.getService(Service.BACK_STORE);
    this.runner = global.getService(Service.RUNNER);
    this.observerRegistry = global.getService(Service.OBSERVER_REGISTRY);
    this.scope = new Set<Table>(scope);
    this.journal = new Journal(this.global, this.scope);
    this.resolver = new Resolver<Relation[]>();
    this.execResolver = new Resolver<Relation[]>();
    this.acquireScopeResolver = new Resolver<void>();
  }

  public exec(): Promise<Relation[]> {
    this.acquireScopeResolver.resolve();
    return this.execResolver.promise;
  }

  public getType(): TransactionType {
    return TransactionType.READ_WRITE;
  }

  public getScope(): Set<Table> {
    return this.scope;
  }

  public getResolver(): Resolver<Relation[]> {
    return this.resolver;
  }

  // Returns a unique number for this task.
  public getId(): number {
    return this.getUniqueNumber();
  }

  // Returns the priority of this task.
  public getPriority(): TaskPriority {
    return TaskPriority.TRANSACTION_TASK;
  }

  // Acquires all locks required such that this task can execute queries.
  public acquireScope(): Promise<void> {
    this.runner.scheduleTask(this);
    return this.acquireScopeResolver.promise;
  }

  // Executes the given query without flushing any changes to disk yet.
  public attachQuery(queryBuilder: QueryBuilder): Promise<any> {
    const taskItem = queryBuilder.getTaskItem();
    return taskItem.plan.getRoot()
        .exec(this.journal, taskItem.context)
        .then(
            (relations) => {
              return relations[0].getPayloads();
            },
            (e) => {
              this.journal.rollback();

              // Need to reject execResolver here such that all locks acquired
              // by this transaction task are eventually released. NOTE: Using a
              // CancellationError to prevent the Promise framework to consider
              // this.execResolver_.promise an unhandled rejected promise, which
              // ends up in an unwanted exception showing up in the console.
              this.execResolver.reject(e);
              throw e;
            });
  }

  public commit(): Promise<Relation[]> {
    this.tx = this.backStore.createTx(
        this.getType(), Array.from(this.scope.values()), this.journal);
    this.tx.commit().then(
        () => {
          this.scheduleObserverTask();
          this.execResolver.resolve();
        },
        (e) => {
          this.journal.rollback();
          this.execResolver.reject(e);
        });

    return this.resolver.promise;
  }

  public rollback(): Promise<any> {
    this.journal.rollback();
    this.execResolver.resolve();
    return this.resolver.promise;
  }

  public stats(): TransactionStats|null {
    let results: TransactionStats|null = null;
    if (this.tx) {
      results = this.tx.stats();
    }
    return results === null ? TransactionStats.getDefault() : results;
  }

  // Schedules an ObserverTask for any observed queries that need to be
  // re-executed, if any.
  private scheduleObserverTask(): void {
    const items = this.observerRegistry.getTaskItemsForTables(
        Array.from(this.scope.values()));
    if (items.length !== 0) {
      const observerTask = new ObserverQueryTask(this.global, items);
      this.runner.scheduleTask(observerTask);
    }
  }
}
