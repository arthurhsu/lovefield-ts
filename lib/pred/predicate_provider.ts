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

import { Binder } from '../base/bind';
import { OperandType, ValueOperandType } from './operand_type';
import { Predicate } from './predicate';

// @export
export interface PredicateProvider {
  // Equal to
  eq(operand: OperandType): Predicate;

  // Not equal to
  neq(operand: OperandType): Predicate;

  // Less than
  lt(operand: OperandType): Predicate;

  // Less than or equals to
  lte(operand: OperandType): Predicate;

  // Greater than
  gt(operand: OperandType): Predicate;

  // Greater than or equals to
  gte(operand: OperandType): Predicate;

  // JavaScript regex match
  match(operand: Binder | RegExp): Predicate;

  // Between test: to must be greater or equals to from.
  between(from: ValueOperandType, to: ValueOperandType): Predicate;

  // Array finding
  in(values: Binder | ValueOperandType[]): Predicate;

  // Nullity test
  isNull(): Predicate;

  // Non-nullity test
  isNotNull(): Predicate;
}
