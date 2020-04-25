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
import {assert} from './assert';
import {ChangeRecord} from './change_record';
import {DiffCalculator} from './diff_calculator';

export type ObserverCallback = (changes: ChangeRecord[]) => void;
export class ObserverRegistryEntry {
  private observers: Set<ObserverCallback>;
  private observable: object[];
  private lastResults: Relation | null;
  private diffCalculator: DiffCalculator;

  constructor(private builder: SelectBuilder) {
    this.builder = builder;
    this.observers = new Set<ObserverCallback>();
    this.observable = [];
    this.lastResults = null;
    const context: SelectContext = builder.getObservableQuery();
    this.diffCalculator = new DiffCalculator(context, this.observable);
  }

  addObserver(callback: ObserverCallback): void {
    if (this.observers.has(callback)) {
      assert(false, 'Attempted to register observer twice.');
      return;
    }
    this.observers.add(callback);
  }

  // Returns whether the callback was found and removed.
  removeObserver(callback: ObserverCallback): boolean {
    return this.observers.delete(callback);
  }

  getTaskItem(): TaskItem {
    return this.builder.getObservableTaskItem();
  }

  hasObservers(): boolean {
    return this.observers.size > 0;
  }

  // Updates the results for this query, which causes observes to be notified.
  updateResults(newResults: Relation): void {
    const changeRecords = this.diffCalculator.applyDiff(
      this.lastResults as Relation,
      newResults
    );
    this.lastResults = newResults;

    if (changeRecords.length > 0) {
      this.observers.forEach(observerFn => observerFn(changeRecords));
    }
  }
}
