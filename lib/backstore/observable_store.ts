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

import {TableDiff} from '../cache/table_diff';
import {DatabaseSchema} from '../schema/database_schema';

import {Memory} from './memory';

// A backing store implementation that holds all data in-memory, without
// persisting anything to disk, and can be observed. This only makes sense
// during testing where external changes are simulated on a MemoryDB.
export class ObservableStore extends Memory {
  private observer: null | ((changes: TableDiff[]) => void);

  constructor(schema: DatabaseSchema) {
    super(schema);
    this.observer = null;
  }

  subscribe(handler: (diffs: TableDiff[]) => void): void {
    // Currently only one observer is supported.
    if (this.observer === null) {
      this.observer = handler;
    }
  }

  // Unsubscribe current change handler.
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  unsubscribe(handler: (diffs: TableDiff[]) => void): void {
    this.observer = null;
  }

  // Notifies registered observers with table diffs.
  notify(changes: TableDiff[]): void {
    if (this.observer !== null) {
      this.observer(changes);
    }
  }

  supportsImport(): boolean {
    return false;
  }
}
