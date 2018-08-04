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
import {QueryBuilder} from '../query/query_builder';
import {BaseTable} from '../schema/base_table';

// @export
export interface Transaction {
  // Executes a list of queries and commits the transaction.
  exec(queries: QueryBuilder[]): Promise<any>;

  // Begins an explicit transaction. Returns a promise fulfilled when all
  // required locks have been acquired.
  //
  // |scope| are the tables that this transaction will be allowed to access.
  // An exclusive lock will be obtained on all tables before any queries
  // belonging to this transaction can be served.
  // @return {!IThenable}
  begin(scope: BaseTable[]): Promise<void>;

  // Attaches |query| to an existing transaction and runs it.
  attach(query: QueryBuilder): Promise<any>;

  // Commits this transaction. Any queries that were performed will be flushed
  // to store.
  commit(): Promise<any>;

  // Rolls back all changes that were made within this transaction. Rollback is
  // only allowed if the transaction has not been yet committed.
  rollback(): Promise<any>;

  // Returns transaction statistics. This call will return meaningful value only
  // after a transaction is committed or rolled back. Read-only transactions
  // will have stats with success equals to true and all other counts as 0.
  stats(): TransactionStats|null;
}
