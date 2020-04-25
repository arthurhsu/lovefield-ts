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

import {TableType} from '../base/private_enum';
import {RawRow, Row} from '../base/row';
import {RuntimeTable} from '../base/runtime_table';
import {Journal} from '../cache/journal';
import {TransactionStats} from './transaction_stats';

// Tx objects are wrappers of backstore-provided transactions. The interface
// defines common methods for these wrappers.
export interface Tx {
  getTable(
    tableName: string,
    deserializeFn: (value: RawRow) => Row,
    tableType: TableType
  ): RuntimeTable;

  // Returns the journal associated with this transaction.
  // The journal keeps track of all changes happened within the transaction.
  // Returns null if this is a READ_ONLY transaction.
  getJournal(): Journal | null;

  // Commits transaction by applying all changes in this transaction's journal
  // to the backing store.
  commit(): Promise<unknown>;

  // Aborts transaction. Caller shall listen to rejection of commit() to detect
  // end of transaction.
  abort(): void;

  // Returns transaction stats if transaction is finalized, otherwise null.
  stats(): TransactionStats | null;
}
