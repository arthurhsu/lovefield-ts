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

import {TaskItem} from '../proc/task_item';

export interface QueryBuilder {
  // Executes the query, all errors will be passed to the reject function.
  // The resolve function may receive parameters as results of execution, for
  // example, select queries will return results.
  exec(): Promise<any>;

  // Returns string representation of query execution plan. Similar to EXPLAIN
  // in most SQL engines.
  explain(): string;

  // Bind values to parameterized queries. Callers are responsible to make sure
  // the types of values match those specified in the query.
  bind(values: any[]): QueryBuilder;

  // |stripValueInfo| true will remove value info to protect PII, default to
  // false in all implementations.
  toSql(stripValueInfo?: boolean): string;

  getTaskItem(): TaskItem;
  assertExecPreconditions(): void;
}
