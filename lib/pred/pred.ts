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

import { EvalType } from '../base/eval';
import { BaseColumn } from '../schema/base_column';
import { Column } from '../schema/column';
import { JoinPredicate } from './join_predicate';
import { Predicate } from './predicate';
import { ValuePredicate } from './value_predicate';

export function createPredicate<T>(
  lhs: Column,
  rhs: Column | T,
  type: EvalType
): Predicate {
  // For the case of .eq(null).
  if (rhs === null) {
    return new ValuePredicate(lhs, rhs, type);
  }

  const r = rhs as BaseColumn;
  if (r.getIndex && r.getIndices) {
    return new JoinPredicate(lhs, r, type);
  }

  // Value predicate, which can be bounded or not.
  return new ValuePredicate(lhs, rhs, type);
}
