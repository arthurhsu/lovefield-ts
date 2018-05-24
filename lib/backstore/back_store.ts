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
import {RuntimeTable} from '../base/runtime_table';
import {Journal} from '../cache/journal';
import {TableDiff} from '../cache/table_diff';
import {Table} from '../schema/table';
import {Tx} from './tx';

// Interface for all backing stores to implement (Indexed DB, filesystem,
// memory etc).
export interface BackStore {
  // Initialize the database and setting up row id.
  // |db| must be instance of RawBackStore<>.
  init(onUpgrade?: (db: object) => Promise<void>): Promise<void>;

  // Creates backstore native transaction that is tied to a given journal.
  createTx(type: TransactionType, scope: Table[], journal?: Journal): Tx;

  // Closes the database. This is just best-effort.
  close(): void;

  // Returns one table based on table name.
  getTableInternal(tableName: string): RuntimeTable;

  // Subscribe to back store changes outside of this connection. Each change
  // event corresponds to one transaction. The events will be fired in the order
  // of reception, which implies the order of transactions happening. Each
  // backstore will allow only one change handler.
  subscribe(handler: (diffs: TableDiff[]) => void): void;

  // Unsubscribe current change handler.
  unsubscribe(handler: (diffs: TableDiff[]) => void): void;

  // Notifies registered observers with table diffs.
  notify(changes: TableDiff[]): void;
}
