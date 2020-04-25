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

import {Row} from './row';

// Interface that models a table in runtime. This is different from schema
// table. A runtime table is the collection of rows.
export interface RuntimeTable {
  get(ids: number[]): Promise<Row[]>;
  put(rows: Row[]): Promise<void>;

  // If |disableClearTableOptimization| is true, implementations
  // will avoid an optimization that clears the entire table, as opposed to
  // removing specific rows. The optimization exists for cases where the
  // backstore determines that all rows are being removed. It isn't safe to do
  // this if we are also inserting rows into the same table. It is unsafe
  // because the put call can race with the remove call which is internally
  // doing a count before the remove. It would be much simpler to block on
  // the remove before calling put, but because of transaction life cycles
  // of IndexedDb on firefox, we can't do that. Callers must know to set this
  // parameter to true if they want to do a put on the same table in the same
  // transaction as this remove call.
  remove(ids: number[], disableClearTableOptimization?: boolean): Promise<void>;
}
