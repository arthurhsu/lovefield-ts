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

import {TransactionType} from '../base/enum';
import {LockType, TaskPriority} from '../base/private_enum';
import {LockManager} from './lock_manager';
import {Relation} from './relation';
import {Task} from './task';
import {TaskQueue} from './task_queue';

// Query/Transaction runner which actually runs the query in a transaction
// (either implicit or explict) on the back store.
export class Runner {
  private queue: TaskQueue;
  private lockManager: LockManager;

  constructor() {
    this.queue = new TaskQueue();
    this.lockManager = new LockManager();
  }

  // Schedules a task for this runner.
  public scheduleTask(task: Task): Promise<Relation[]> {
    if (task.getPriority() < TaskPriority.USER_QUERY_TASK ||
        task.getPriority() < TaskPriority.TRANSACTION_TASK) {
      // Any priority that is higher than USER_QUERY_TASK or TRANSACTION_TASK is
      // considered a "high" priority task and all held reserved locks should be
      // cleared to allow it to execute.
      this.lockManager.clearReservedLocks(task.getScope());
    }

    this.queue.insert(task);
    this.consumePending();
    return task.getResolver().promise;
  }

  // Examines the queue and executes as many tasks as possible taking into
  // account the scope of each task and the currently occupied scopes.
  private consumePending(): void {
    const queue = this.queue.getValues();

    queue.forEach((task) => {
      // Note: Iterating on a shallow copy of this.queue_, because this.queue_
      // will be modified during iteration and therefore iterating on
      // this.queue_ would not guarantee that every task in the queue will be
      // traversed.
      let acquiredLock = false;
      if (task.getType() === TransactionType.READ_ONLY) {
        acquiredLock = this.requestTwoPhaseLock(
            task, LockType.RESERVED_READ_ONLY, LockType.SHARED);
      } else {
        acquiredLock = this.requestTwoPhaseLock(
            task, LockType.RESERVED_READ_WRITE, LockType.EXCLUSIVE);
      }

      if (acquiredLock) {
        // Removing task from the task queue and executing it.
        this.queue.remove(task);
        this.execTask(task);
      }
    });
  }

  // Performs a two-phase lock acquisition. The 1st lock is requested first. If
  // it is granted, the 2nd lock is requested. Returns false if the 2nd lock was
  // not granted or both 1st and 2nd were not granted.
  private requestTwoPhaseLock(
      task: Task, lockType1: LockType, lockType2: LockType): boolean {
    let acquiredLock = false;
    const acquiredFirstLock =
        this.lockManager.requestLock(task.getId(), task.getScope(), lockType1);

    if (acquiredFirstLock) {
      // Escalating the first lock to the second lock.
      acquiredLock = this.lockManager.requestLock(
          task.getId(), task.getScope(), lockType2);
    }

    return acquiredLock;
  }

  // Executes a QueryTask. Callers of this method should have already acquired a
  // lock according to the task that is about to be executed.
  private execTask(task: Task): void {
    task.exec().then(
        this.onTaskSuccess.bind(this, task), this.onTaskError.bind(this, task));
  }

  // Executes when a task finished successfully.
  private onTaskSuccess(task: Task, results: Relation[]): void {
    this.lockManager.releaseLock(task.getId(), task.getScope());
    task.getResolver().resolve(results);
    this.consumePending();
  }

  // Executes when a task finished with an error.
  private onTaskError(task: Task, error: Error): void {
    this.lockManager.releaseLock(task.getId(), task.getScope());
    task.getResolver().reject(error);
    this.consumePending();
  }
}
