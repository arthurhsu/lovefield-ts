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

import { BackStore } from '../backstore/back_store';
import { TransactionStatsImpl } from '../backstore/transaction_stats_impl';
import { Tx } from '../backstore/tx';
import { TransactionType } from '../base/enum';
import { Global } from '../base/global';
import { TaskPriority } from '../base/private_enum';
import { Resolver } from '../base/resolver';
import { Service } from '../base/service';
import { UniqueId } from '../base/unique_id';
import { Journal } from '../cache/journal';
import { Context } from '../query/context';
import { SelectContext } from '../query/select_context';
import { BaseTable } from '../schema/base_table';
import { Table } from '../schema/table';
import { PhysicalQueryPlan } from './pp/physical_query_plan';
import { Relation } from './relation';
import { Task } from './task';
import { TaskItem } from './task_item';

// A QueryTask represents a collection of queries that should be executed as
// part of a single transaction.
export abstract class QueryTask extends UniqueId implements Task {
  protected backStore: BackStore;
  protected queries: Context[];
  private plans: PhysicalQueryPlan[];
  private combinedScope: Set<Table>;
  private txType: TransactionType;
  private resolver: Resolver<Relation[]>;
  private tx!: Tx;

  constructor(protected global: Global, items: TaskItem[]) {
    super();
    this.backStore = global.getService(Service.BACK_STORE);
    this.queries = items.map(item => item.context);
    this.plans = items.map(item => item.plan);
    this.combinedScope = PhysicalQueryPlan.getCombinedScope(this.plans);
    this.txType = this.detectType();
    this.resolver = new Resolver<Relation[]>();
  }

  exec(): Promise<Relation[]> {
    const journal =
      this.txType === TransactionType.READ_ONLY
        ? undefined
        : new Journal(this.global, this.combinedScope);
    const results: Relation[] = [];

    const remainingPlans = this.plans.slice();
    const queries = this.queries;

    const sequentiallyExec = (): Promise<Relation[]> => {
      const plan = remainingPlans.shift();
      if (plan) {
        const queryContext = queries[results.length];
        return plan
          .getRoot()
          .exec(journal, queryContext)
          .then(relations => {
            results.push(relations[0]);
            return sequentiallyExec();
          });
      }
      return Promise.resolve(results);
    };

    return sequentiallyExec()
      .then(() => {
        this.tx = this.backStore.createTx(
          this.txType,
          Array.from((this.combinedScope.values() as unknown) as BaseTable[]),
          journal
        );
        return this.tx.commit();
      })
      .then(
        () => {
          this.onSuccess(results);
          return results;
        },
        e => {
          if (journal) {
            journal.rollback();
          }
          throw e;
        }
      );
  }

  getType(): TransactionType {
    return this.txType;
  }

  getScope(): Set<Table> {
    return this.combinedScope;
  }

  getResolver(): Resolver<Relation[]> {
    return this.resolver;
  }

  getId(): number {
    return this.getUniqueNumber();
  }

  abstract getPriority(): TaskPriority;

  // Returns stats for the task. Used in transaction.exec([queries]).
  stats(): TransactionStatsImpl {
    let results: TransactionStatsImpl | null = null;
    if (this.tx) {
      results = this.tx.stats() as TransactionStatsImpl;
    }
    return results === null ? TransactionStatsImpl.getDefault() : results;
  }

  // Executes after all queries have finished successfully. Default
  // implementation is a no-op. Subclasses should override this method as
  // necessary.
  protected onSuccess(results: Relation[]): void {
    // Default implementation is a no-op.
  }

  private detectType(): TransactionType {
    return this.queries.some(query => !(query instanceof SelectContext))
      ? TransactionType.READ_WRITE
      : TransactionType.READ_ONLY;
  }
}
