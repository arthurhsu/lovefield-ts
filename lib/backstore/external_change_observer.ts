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

import { Global } from '../base/global';
import { Service } from '../base/service';
import { TableDiff } from '../cache/table_diff';
import { ExternalChangeTask } from '../proc/external_change_task';
import { Runner } from '../proc/runner';

import { BackStore } from './back_store';

export class ExternalChangeObserver {
  private backStore: BackStore;
  private runner: Runner;

  constructor(private global: Global) {
    this.backStore = global.getService(Service.BACK_STORE);
    this.runner = global.getService(Service.RUNNER);
  }

  // Starts observing the backing store for any external changes.
  startObserving(): void {
    this.backStore.subscribe(this.onChange.bind(this));
  }

  // Stops observing the backing store.
  stopObserving(): void {
    this.backStore.unsubscribe(this.onChange.bind(this));
  }

  // Executes every time a change is reported by the backing store. It is
  // responsible for scheduling a task that will updated the in-memory data
  // layers (indices and cache) accordingly.
  private onChange(tableDiffs: TableDiff[]): void {
    // Note: Current logic does not check for any conflicts between external
    // changes and in-flight READ_WRITE queries. It assumes that no conflicts
    // exist (single writer, multiple readers model).
    const externalChangeTask = new ExternalChangeTask(this.global, tableDiffs);
    this.runner.scheduleTask(externalChangeTask);
  }
}
