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

import {Global} from '../base/global';
import {Favor} from '../base/private_enum';
import {Row} from '../base/row';
import {Service} from '../base/service';
import {IndexStore} from '../index/index_store';
import {RuntimeIndex} from '../index/runtime_index';
import {Table} from '../schema/table';
import {DatabaseSchema} from '../schema/database_schema';

import {Cache} from './cache';
import {Modification} from './modification';
import {TableDiff} from './table_diff';

export class InMemoryUpdater {
  private readonly cache: Cache;
  private readonly indexStore: IndexStore;
  private readonly schema: DatabaseSchema;

  constructor(global: Global) {
    this.cache = global.getService(Service.CACHE);
    this.indexStore = global.getService(Service.INDEX_STORE);
    this.schema = global.getService(Service.SCHEMA);
  }

  // Updates all indices and the cache to reflect the changes that are described
  // in the given diffs.
  update(tableDiffs: TableDiff[]): void {
    tableDiffs.forEach(tableDiff => {
      this.updateIndicesForDiff(tableDiff);
      this.updateCacheForDiff(tableDiff);
    }, this);
  }

  // Updates all indices that are affected as a result of the given
  // modification. In the case where an exception is thrown (constraint
  // violation) all the indices are unaffected.
  updateTableIndicesForRow(table: Table, modification: Modification): void {
    const indices = this.indexStore.getTableIndices(table.getName());
    let updatedIndices = 0;
    indices.forEach(index => {
      try {
        this.updateTableIndexForRow(index, modification);
        updatedIndices++;
      } catch (e) {
        // Rolling back any indices that were successfully updated, since
        // updateTableIndicesForRow must be atomic.
        indices.slice(0, updatedIndices).forEach(idx => {
          this.updateTableIndexForRow(idx, [modification[1], modification[0]]);
        }, this);

        // Forwarding the exception to the caller.
        throw e;
      }
    }, this);
  }

  // Updates the cache based on the given table diff.
  private updateCacheForDiff(diff: TableDiff): void {
    const tableName = diff.getName();
    diff
      .getDeleted()
      .forEach((row, rowId) => this.cache.remove(tableName, rowId));
    diff.getAdded().forEach((row, rowId) => this.cache.set(tableName, row));
    diff
      .getModified()
      .forEach((modification, rowId) =>
        this.cache.set(tableName, modification[1] as Row)
      );
  }

  // Updates index data structures based on the given table diff.
  private updateIndicesForDiff(diff: TableDiff): void {
    const table = this.schema.table(diff.getName());
    const modifications = diff.getAsModifications();
    modifications.forEach(modification => {
      this.updateTableIndicesForRow(table, modification);
    }, this);
  }

  // Updates a given index to reflect a given row modification.
  private updateTableIndexForRow(
    index: RuntimeIndex,
    modification: Modification
  ): void {
    // Using 'undefined' as a special value to indicate insertion/
    // deletion instead of 'null', since 'null' can be a valid index key.
    const keyNow =
      modification[1] === null
        ? undefined
        : (modification[1] as Row).keyOfIndex(index.getName());
    const keyThen =
      modification[0] === null
        ? undefined
        : (modification[0] as Row).keyOfIndex(index.getName());

    if (keyThen === undefined && keyNow !== undefined) {
      // Insertion
      index.add(keyNow, (modification[1] as Row).id());
    } else if (keyThen !== undefined && keyNow !== undefined) {
      // Index comparators may not handle null, so handle it here for them.
      if (keyNow === null || keyThen === null) {
        if (keyNow === keyThen) {
          return;
        }
      } else if (index.comparator().compare(keyThen, keyNow) === Favor.TIE) {
        return;
      }

      // Update
      // NOTE: the order of calling add() and remove() here matters.
      // Index#add() might throw an exception because of a constraint
      // violation, in which case the index remains unaffected as expected.
      index.add(keyNow, (modification[1] as Row).id());
      index.remove(keyThen, (modification[0] as Row).id());
    } else if (keyThen !== undefined && keyNow === undefined) {
      // Deletion
      index.remove(keyThen, (modification[0] as Row).id());
    }
  }
}
