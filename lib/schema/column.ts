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

import {Type} from '../base/enum';
import {PredicateProvider} from '../pred/predicate_provider';

import {Table} from './table';

// Public column interface
// @export
export interface Column extends PredicateProvider {
  getName(): string;
  getNormalizedName(): string;

  // Different from original Lovefield, moved from BaseColumn to here.
  // This makes more sense since these getter calls are non-mutable and
  // easier for TypeScript users to determine how to proper cast.
  getTable(): Table;
  getType(): Type;
  isNullable(): boolean;

  // Additional function call, not existent in original Lovefield.
  isUnique(): boolean;

  // Alias the column, used in query.
  as(alias: string): Column;
}
