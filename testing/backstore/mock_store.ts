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

import {BackStore} from '../../lib/backstore/back_store';
import {ObservableStore} from '../../lib/backstore/observable_store';
import {RawBackStore} from '../../lib/backstore/raw_back_store';
import {Tx} from '../../lib/backstore/tx';
import {TransactionType} from '../../lib/base/enum';
import {ErrorCode, Exception} from '../../lib/base/exception';
import {RuntimeTable} from '../../lib/base/runtime_table';
import {Journal} from '../../lib/cache/journal';
import {TableDiff} from '../../lib/cache/table_diff';
import {Table} from '../../lib/schema/table';

import {TrackedTx} from './tracked_tx';

// An Memory wrapper to be used for simulating external changes.
// An external change is a modification of the backing store that has already
//
// occurred
//  - via a different tab/window for the case of a local DB (like IndexedDB), or
//  - via a different client for the case of a remote DB (like Firebase).
//
//  MockStore is a wrapper around the actual backstore (the one registered in
//  Global). MockStore itself is not registered in Global. Changes
//  submitted through MockStore will result in
//
//  1) Modifying the actual backstore data.
//  2) Notifying observers of the actual backstore, giving them a chance to
//     update Lovefield internal state (in-memory indices/caches) accordingly.
export class MockStore implements BackStore {
  constructor(private store: ObservableStore) {}

  public init(onUpgrade?: (db: RawBackStore) => Promise<void>): Promise<any> {
    return this.store.init(onUpgrade);
  }

  public createTx(type: TransactionType, scope: Table[], journal?: Journal):
      Tx {
    return new TrackedTx(this, type, journal);
  }

  public close(): void {
    // Nothing to do.
  }

  public getTableInternal(tableName: string): RuntimeTable {
    return this.store.getTableInternal(tableName);
  }

  public subscribe(handler: (diffs: TableDiff[]) => void): void {
    throw new Exception(ErrorCode.NOT_SUPPORTED);
  }

  public unsubscribe(handler: (diffs: TableDiff[]) => void): void {
    throw new Exception(ErrorCode.NOT_SUPPORTED);
  }

  public notify(changes: TableDiff[]): void {
    this.store.notify(changes);
  }

  public supportsImport(): boolean {
    return false;
  }
}
