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

import {TransactionStats} from '../backstore/transaction_stats';
import {ErrorCode} from '../base/enum';
import {Exception} from '../base/exception';
import {Global} from '../base/global';
import {Service} from '../base/service';
import {Transaction} from '../base/transaction';
import {QueryBuilder} from '../query/query_builder';
import {BaseTable} from '../schema/base_table';

import {Runner} from './runner';
import {TaskItem} from './task_item';
import {StateTransition, TransactionState} from './transaction_state';
import {TransactionTask} from './transaction_task';
import {UserQueryTask} from './user_query_task';

export class RuntimeTransaction implements Transaction {
  private runner: Runner;
  private task: TransactionTask|UserQueryTask|null;
  private state: TransactionState;
  private stateTransition: StateTransition;

  constructor(private global: Global) {
    this.runner = global.getService(Service.RUNNER);
    this.task = null;
    this.state = TransactionState.CREATED;
    this.stateTransition = StateTransition.get();
  }

  public exec(queryBuilders: QueryBuilder[]): Promise<any> {
    this.updateState(TransactionState.EXECUTING_AND_COMMITTING);

    const taskItems: TaskItem[] = [];
    try {
      queryBuilders.forEach((queryBuilder) => {
        queryBuilder.assertExecPreconditions();
        taskItems.push(queryBuilder.getTaskItem());
      });
    } catch (e) {
      this.updateState(TransactionState.FINALIZED);
      return Promise.reject(e);
    }

    this.task = new UserQueryTask(this.global, taskItems);
    return this.runner.scheduleTask(this.task).then(
        (results) => {
          this.updateState(TransactionState.FINALIZED);
          return results.map((relation) => relation.getPayloads());
        },
        (e) => {
          this.updateState(TransactionState.FINALIZED);
          throw e;
        });
  }

  public begin(scope: BaseTable[]): Promise<void> {
    this.updateState(TransactionState.ACQUIRING_SCOPE);

    this.task = new TransactionTask(this.global, scope);
    return this.task.acquireScope().then(
        () => this.updateState(TransactionState.ACQUIRED_SCOPE));
  }

  public attach(query: QueryBuilder): Promise<any> {
    this.updateState(TransactionState.EXECUTING_QUERY);

    try {
      query.assertExecPreconditions();
    } catch (e) {
      this.updateState(TransactionState.FINALIZED);
      return Promise.reject(e);
    }

    return (this.task as TransactionTask)
        .attachQuery(query)
        .then(
            (result) => {
              this.updateState(TransactionState.ACQUIRED_SCOPE);
              return result;
            },
            (e) => {
              this.updateState(TransactionState.FINALIZED);
              throw e;
            });
  }

  public commit(): Promise<any> {
    this.updateState(TransactionState.COMMITTING);
    return (this.task as TransactionTask)
        .commit()
        .then(() => this.updateState(TransactionState.FINALIZED));
  }

  public rollback(): Promise<any> {
    this.updateState(TransactionState.ROLLING_BACK);
    return (this.task as TransactionTask)
        .rollback()
        .then(() => this.updateState(TransactionState.FINALIZED));
  }

  public stats(): TransactionStats|null {
    if (this.state !== TransactionState.FINALIZED) {
      // 105: Attempt to access in-flight transaction states.
      throw new Exception(ErrorCode.INVALID_TX_ACCESS);
    }
    return (this.task as TransactionTask | UserQueryTask).stats();
  }

  // Update this transaction from its current state to the given one.
  private updateState(newState: TransactionState): void {
    const nextStates = this.stateTransition.get(this.state);
    if (!nextStates.has(newState)) {
      // 107: Invalid transaction state transition: {0} -> {1}.
      throw new Exception(ErrorCode.INVALID_TX_STATE, this.state, newState);
    } else {
      this.state = newState;
    }
  }
}
