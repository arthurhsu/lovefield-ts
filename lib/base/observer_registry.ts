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

import {Relation} from '../proc/relation';
import {TaskItem} from '../proc/task_item';
import {SelectBuilder} from '../query/select_builder';
import {SelectContext} from '../query/select_context';
import {SelectQuery} from '../query/select_query';
import {Table} from '../schema/table';
import {assert} from './assert';
import {ObserverCallback, ObserverRegistryEntry} from './observer_registry_entry';

// A class responsible for keeping track of all observers as well as all arrays
// that are being observed.

export class ObserverRegistry {
  private entries: Map<string, ObserverRegistryEntry>;

  constructor() {
    this.entries = new Map<string, ObserverRegistryEntry>();
  }

  // Registers an observer for the given query.
  public addObserver(query: SelectQuery, callback: ObserverCallback): void {
    const builder = query as SelectBuilder;
    const queryId = this.getQueryId(builder.getObservableQuery());
    let entry = this.entries.get(queryId) || null;
    if (entry === null) {
      entry = new ObserverRegistryEntry(builder);
      this.entries.set(queryId, entry);
    }
    entry.addObserver(callback);
  }

  // Unregister an observer for the given query.
  public removeObserver(query: SelectQuery, callback: ObserverCallback): void {
    const builder = query as SelectBuilder;
    const queryId = this.getQueryId(builder.getObservableQuery());

    const entry: ObserverRegistryEntry =
        this.entries.get(queryId) as ObserverRegistryEntry;
    assert(
        entry !== undefined && entry !== null,
        'Attempted to unobserve a query that was not observed.');
    const didRemove = (entry as ObserverRegistryEntry).removeObserver(callback);
    assert(didRemove, 'removeObserver: Inconsistent state detected.');

    if (!entry.hasObservers()) {
      this.entries.delete(queryId);
    }
  }

  // Finds all the observed queries that reference at least one of the given
  // tables.
  public getTaskItemsForTables(tables: Table[]): TaskItem[] {
    const tableSet = new Set<string>();
    tables.forEach((table) => tableSet.add(table.getName()));

    const items: TaskItem[] = [];
    this.entries.forEach((entry, key) => {
      const item = entry.getTaskItem();
      const refersToTables =
          (item.context as SelectContext)
              .from.some((table) => tableSet.has(table.getName()));
      if (refersToTables) {
        items.push(item);
      }
    });
    return items;
  }

  // Updates the results of a given query. It is ignored if the query is no
  // longer being observed.
  public updateResultsForQuery(query: SelectContext, results: Relation):
      boolean {
    const queryId = this.getQueryId(
        query.clonedFrom !== undefined && query.clonedFrom !== null ?
            query.clonedFrom as SelectContext :
            query);
    const entry = this.entries.get(queryId) || null;

    if (entry !== null) {
      entry.updateResults(results);
      return true;
    }

    return false;
  }

  // Returns a unique ID of the given query.
  private getQueryId(query: SelectContext): string {
    return query.getUniqueId();
  }
}
