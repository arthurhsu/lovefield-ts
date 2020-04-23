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

import * as chai from 'chai';
import { TransactionType } from '../../lib/base/enum';
import { TaskPriority } from '../../lib/base/private_enum';
import { Resolver } from '../../lib/base/resolver';
import { Runner } from '../../lib/proc/runner';
import { Table } from '../../lib/schema/table';
import { getHrDbSchemaBuilder } from '../../testing/hr_schema/hr_schema_builder';
import { MockTask } from '../../testing/mock_task';

const assert = chai.assert;

describe('Runner', () => {
  let runner: Runner;
  let j: Table;

  before(() => {
    j = getHrDbSchemaBuilder()
      .getSchema()
      .table('Job');
  });

  beforeEach(() => {
    runner = new Runner();
  });

  function createScope(): Set<Table> {
    return new Set<Table>([j]);
  }

  // Tests that SELECT queries are executed after overlapping write transaction
  // finishes.
  it('transaction_Read', () => {
    const executionOrder: string[] = [];

    // Creating two tasks referring to the same scope.
    const queryTask1 = new MockTask(
      TransactionType.READ_WRITE,
      createScope(),
      () => {
        executionOrder.push('query1');
      },
      TaskPriority.USER_QUERY_TASK
    );
    const queryTask2 = new MockTask(
      TransactionType.READ_ONLY,
      createScope(),
      () => {
        executionOrder.push('query2');
      },
      TaskPriority.USER_QUERY_TASK
    );

    const promises = [queryTask1, queryTask2].map(queryTask =>
      runner.scheduleTask(queryTask)
    );
    return Promise.all(promises).then(() => {
      // Ensuring that the READ_ONLY task was executed after the READ_WRITE
      // task finished.
      assert.sameOrderedMembers(['query1', 'query2'], executionOrder);
    });
  });

  // Tests that multiple overlapping READ_WRITE transactions are executed in the
  // expected order.
  it('transaction_Write', () => {
    const actualExecutionOrder: string[] = [];
    const expectedExecutionOrder: string[] = [];

    const ids = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    const queryTasks = ids.map(id => {
      const name = `query${id}`;
      const queryTask = new MockTask(
        TransactionType.READ_WRITE,
        createScope(),
        () => {
          actualExecutionOrder.push(name);
        },
        TaskPriority.USER_QUERY_TASK,
        name
      );
      expectedExecutionOrder.push(name);
      return queryTask;
    });

    const promises = queryTasks.map(task => runner.scheduleTask(task));
    return Promise.all(promises).then(() => {
      assert.sameOrderedMembers(expectedExecutionOrder, actualExecutionOrder);
    });
  });

  it('task_Success', () => {
    const expectedResult = 'dummyResult';

    const queryTask = new MockTask(
      TransactionType.READ_WRITE,
      createScope(),
      () => expectedResult,
      TaskPriority.USER_QUERY_TASK
    );

    return runner
      .scheduleTask(queryTask)
      .then(result =>
        assert.equal(expectedResult, (result as unknown) as string)
      );
  });

  it('task_Failure', () => {
    const expectedError = new Error('dummyError');

    const queryTask = new MockTask(
      TransactionType.READ_WRITE,
      createScope(),
      () => {
        throw expectedError;
      },
      TaskPriority.USER_QUERY_TASK
    );

    return runner.scheduleTask(queryTask).then(
      () => assert.fail,
      error => assert.deepEqual(expectedError, error)
    );
  });

  // Tests that prioritized tasks are placed in the front of the queue.
  it('scheduleTask_Prioritize', () => {
    const resolver = new Resolver();
    const executionOrder: string[] = [];

    const task1 = new MockTask(
      TransactionType.READ_WRITE,
      createScope(),
      () => resolver.promise,
      TaskPriority.USER_QUERY_TASK
    );
    const task2 = new MockTask(
      TransactionType.READ_WRITE,
      createScope(),
      () => {
        executionOrder.push('task2');
      },
      TaskPriority.TRANSACTION_TASK
    );
    const task3 = new MockTask(
      TransactionType.READ_WRITE,
      createScope(),
      () => {
        executionOrder.push('task3');
      },
      TaskPriority.EXTERNAL_CHANGE_TASK
    );
    const task4 = new MockTask(
      TransactionType.READ_WRITE,
      createScope(),
      () => {
        executionOrder.push('task4');
      },
      TaskPriority.OBSERVER_QUERY_TASK
    );

    const p1 = runner.scheduleTask(task1);
    const p2 = runner.scheduleTask(task2);
    const p3 = runner.scheduleTask(task3);
    const p4 = runner.scheduleTask(task4);
    resolver.resolve();

    return Promise.all([p1, p2, p3, p4]).then(() => {
      // Ensuring that the prioritized task3 executed before task2.
      assert.sameOrderedMembers(['task4', 'task3', 'task2'], executionOrder);
    });
  });

  // Tests that a READ_WRITE transaction will wait for an already running
  // READ_ONLY transaction with overlapping scope to finish.
  it('transaction_WriteWhileReading', () => {
    const resolver = new Resolver();
    const executionOrder: string[] = [];

    // Creating a READ_ONLY and a READ_WRITE task that refer to the same scope.
    const queryTask1 = new MockTask(
      TransactionType.READ_ONLY,
      createScope(),
      () => {
        executionOrder.push('q1 start');
        return resolver.promise.then(() => {
          executionOrder.push('q1 end');
        });
      },
      TaskPriority.USER_QUERY_TASK
    );
    const queryTask2 = new MockTask(
      TransactionType.READ_WRITE,
      createScope(),
      () => {
        executionOrder.push('q2 start');
        executionOrder.push('q2 end');
      },
      TaskPriority.USER_QUERY_TASK
    );

    const promises = [queryTask1, queryTask2].map(queryTask =>
      runner.scheduleTask(queryTask)
    );

    resolver.resolve();
    return Promise.all(promises).then(() => {
      // Ensuring that the READ_ONLY task completed before the READ_WRITE task
      // started.
      assert.sameOrderedMembers(
        ['q1 start', 'q1 end', 'q2 start', 'q2 end'],
        executionOrder
      );
    });
  });
});
