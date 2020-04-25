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

import {DeleteQuery} from '../query/delete_query';
import {InsertQuery} from '../query/insert_query';
import {SelectQuery} from '../query/select_query';
import {UpdateQuery} from '../query/update_query';
import {Column} from '../schema/column';
import {DatabaseSchema} from '../schema/database_schema';
import {Table} from '../schema/table';

import {TransactionType} from './enum';
import {ObserverCallback} from './observer_registry_entry';
import {Transaction} from './transaction';

// Defines the interface of a runtime database instance. This models the return
// value of connect().
// @export
export interface DatabaseConnection {
  getSchema(): DatabaseSchema;
  select(...columns: Column[]): SelectQuery;
  insert(): InsertQuery;
  insertOrReplace(): InsertQuery;
  update(table: Table): UpdateQuery;
  delete(): DeleteQuery;

  // Registers an observer for the given query.
  observe(query: SelectQuery, callback: ObserverCallback): void;

  // Un-registers an observer for the given query.
  unobserve(query: SelectQuery, callback: ObserverCallback): void;

  createTransaction(type?: TransactionType): Transaction;

  // Closes database connection. This is a best effort function and the closing
  // can happen in a separate thread.
  // Once a db is closed, all its queries will fail and cannot be reused.
  close(): void;

  // Exports database as a JSON object.
  export(): Promise<object>;

  // Imports from a JSON object into an empty database.
  import(data: object): Promise<object[]>;
}
