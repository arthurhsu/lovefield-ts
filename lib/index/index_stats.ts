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

import {Key} from './key_range';

export class IndexStats {
  public totalRows: number;

  // Useful only for primary key auto-increment indices. Ensures that previously
  // encountered IDs within the same session (application run) are not re-used,
  // even after they have been freed.
  public maxKeyEncountered: Key|null;

  constructor() {
    this.totalRows = 0;
    this.maxKeyEncountered = 0;
  }

  // Signals that a row had been added to index.
  public add(key: Key, rowCount: number): void {
    this.totalRows += rowCount;

    this.maxKeyEncountered = (this.maxKeyEncountered === null) ?
        key :
        key > this.maxKeyEncountered ? key : this.maxKeyEncountered;
  }

  // Signals that row(s) had been removed from index.
  public remove(key: Key, removedCount: number): void {
    this.totalRows -= removedCount;
  }

  // Signals that the index had been cleared.
  public clear(): void {
    this.totalRows = 0;
    // this.maxKeyEncountered shall not be updated.
  }

  // Combines stats given and put the results into current object.
  public updateFromList(statsList: IndexStats[]): void {
    this.clear();
    statsList.forEach((stats) => {
      this.totalRows += stats.totalRows;
    }, this);
  }
}
