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

import {ArrayHelper} from '../structs/array_helper';
import {Task} from './task';

export class TaskQueue {
  private queue: Task[];

  constructor() {
    this.queue = [];
  }

  // Inserts a task to the queue.
  insert(task: Task): void {
    ArrayHelper.binaryInsert(this.queue, task, (t1: Task, t2: Task): number => {
      const priorityDiff = t1.getPriority() - t2.getPriority();
      return priorityDiff === 0 ? t1.getId() - t2.getId() : priorityDiff;
    });
  }

  // Returns a shallow copy of this queue.
  getValues(): Task[] {
    return this.queue.slice();
  }

  // Removes the given task from the queue. Returns true if the task were
  // removed, false if the task were not found.
  remove(task: Task): boolean {
    const i = this.queue.indexOf(task);
    if (i >= 0) {
      this.queue.splice(i, 1);
    }
    return i >= 0;
  }
}
