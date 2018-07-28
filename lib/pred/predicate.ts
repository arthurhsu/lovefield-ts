/**
 * Copyright 2016 The Lovefield Project Authors. All Rights Reserved.
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
import {BaseColumn} from '../schema/base_column';
import {BaseTable} from '../schema/base_table';

export interface Predicate {
  // Returns relation that holds only the entries satisfying given predicate.
  eval(relation: Relation): Relation;

  // Reverses the predicate: predicate evaluates to true where before it was
  // evaluating to false, and vice versa.
  setComplement(isComplement: boolean): void;

  // Returns a clone of the predicate.
  copy(): Predicate;

  // Returns an array of all columns involved in this predicate. The optional
  // results array as the parameter holds previous results, given that this
  // function is called recursively.  If provided any column will be added on
  // that array. If not provided a new array will be allocated.
  getColumns(results?: BaseColumn[]): BaseColumn[];

  // Returns an array of all tables involved in this predicate. The optional
  // results array as the parameter holds previous results, given that this
  // function is called recursively.  If provided any table will be added on
  // that array. If not provided a new array will be allocated.
  getTables(results?: Set<BaseTable>): Set<BaseTable>;

  setId(id: number): void;
  getId(): number;
}
