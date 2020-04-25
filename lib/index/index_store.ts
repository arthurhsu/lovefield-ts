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

import {DatabaseSchema} from '../schema/database_schema';
import {RuntimeIndex} from './runtime_index';

export interface IndexStore {
  // Initializes index store. This will create empty index instances.
  init(schema: DatabaseSchema): Promise<void>;

  // Returns the index by full qualified name. Returns null if not found.
  get(name: string): RuntimeIndex | null;

  // Returns the indices for a given table or an empty array if no indices
  // exist.
  getTableIndices(tableName: string): RuntimeIndex[];

  // Sets the given index. If an index with the same name already exists it will
  // be overwritten.
  set(tableName: string, index: RuntimeIndex): void;
}
