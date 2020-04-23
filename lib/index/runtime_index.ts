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

import { Row } from '../base/row';
import { Comparator } from './comparator';
import { IndexStats } from './index_stats';
import { Key, KeyRange, SingleKey, SingleKeyRange } from './key_range';

// Index used in runtime execution, lf.index.Index.
export interface RuntimeIndex {
  // Returns normalized name for this index.
  getName(): string;

  // Inserts data into index. If the key already existed, append value to the
  // value list. If the index does not support duplicate keys, adding duplicate
  // keys will result in throwing CONSTRAINT error.
  add(key: Key | SingleKey, value: number): void;

  // Replaces data in index. All existing data for that key will be purged.
  // If the key is not found, inserts the data.
  set(key: Key | SingleKey, value: number): void;

  // Deletes a row having given key from index. If not found return silently.
  // If |rowId| is given, delete a single row id when the index allows
  // duplicate keys. Ignored for index supporting only unique keys.
  remove(key: Key | SingleKey, rowId?: number): void;

  // Gets values from index. Returns empty array if not found.
  get(key: Key | SingleKey): number[];

  // Gets the cost of retrieving data for given range.
  cost(keyRange?: SingleKeyRange | KeyRange): number;

  // Retrieves all data within the range. Returns empty array if not found.
  // When multiple key ranges are specified, the function will return the
  // union of range query results. If none provided, all rowIds in this index
  // will be returned. Caller must ensure the ranges do not overlap.
  //
  // When |reverseOrder| is set to true, retrieves the results in the reverse
  // ordering of the index's comparator.
  // |limit| sets max number of rows to return, |skip| skips first N rows.
  getRange(
    range?: SingleKeyRange[] | KeyRange[],
    reverseOrder?: boolean,
    limit?: number,
    skip?: number
  ): number[];

  // Removes everything from the tree.
  clear(): void;

  // Special note for NULL: if the given index disallows NULL as key (e.g.
  // B-Tree), the containsKey will return garbage. Caller needs to be aware of
  // the behavior of the given index (this shall not be a problem with indices
  // that are properly wrapped by NullableIndex).
  containsKey(key: Key | SingleKey): boolean;

  // Returns an array of exactly two elements, holding the minimum key at
  // position 0, and all associated values at position 1. If no keys exist in
  // the index null is returned.
  min(): unknown[] | null;

  // Returns an array of exactly two elements, holding the maximum key at
  // position 0, and all associated values at position 1. If no keys exist in
  // the index null is returned.
  max(): unknown[] | null;

  // Serializes this index such that it can be persisted.
  serialize(): Row[];

  // Returns the comparator used by this index.
  comparator(): Comparator;

  // Whether the index accepts unique key only.
  isUniqueKey(): boolean;

  // Returns the stats associated with this index.
  // Note: The returned object represents a snapshot of the index state at the
  // time this call was made. It is not guaranteed to be updated as the index
  // changes. Caller needs to call this method again if interested in latest
  // stats.
  stats(): IndexStats;
}
