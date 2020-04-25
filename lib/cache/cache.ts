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

import {Row} from '../base/row';

export interface Cache {
  // Inserts/Updates contents in cache. This version takes single row.
  set(tableName: string, row: Row): void;

  // Inserts/Updates contents in cache. This version takes multiple rows.
  setMany(tableName: string, rows: Row[]): void;

  // Returns contents from the cache.
  get(id: number): Row | null;

  // Returns contents from the cache.
  getMany(id: number[]): Array<Row | null>;

  // Returns contents from the cache. The range query will return only the rows
  // with row ids matching the range.
  getRange(tableName: string, fromId: number, toId: number): Row[];

  // Removes a single entry from the cache.
  remove(tableName: string, rowId: number): void;

  // Removes entries from the cache.
  removeMany(tableName: string, rowIds: number[]): void;

  // Number of rows in cache for |tableName|. If |tableName| is omitted, count
  // rows for all tables.
  getCount(tableName?: string): number;

  // Removes all contents from the cache.
  clear(): void;
}
