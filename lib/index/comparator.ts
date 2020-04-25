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

import {Favor} from '../base/private_enum';
import {Key, Range} from './key_range';

/**
 * Comparator used to provide necessary information for building an index tree.
 * It offers methods to indicate which operand is "favorable".
 */
export interface Comparator {
  compare(lhs: Key, rhs: Key): Favor;

  /**
   * Returns an array of boolean which represents the relative positioning of
   * a key to a range. The concept is to project both the key and the range onto
   * the 1-D space. The returned array is in the form of [left, right]. If the
   * range projection covers any value left/right of the key (including the key
   * itself), then left/right will be set to true.
   */
  compareRange(key: Key, range: Range): boolean[];

  /**
   * Finds which one of the two operands is the minimum in absolute terms.
   */
  min(lhs: Key, rhs: Key): Favor;

  /**
   * Finds which one of the two operands is the maximum in absolute terms.
   */
  max(lhs: Key, rhs: Key): Favor;

  isInRange(key: Key, range: Range): boolean;

  /**
   * Whether the key's first dimension is in range's first dimension or not.
   * For example, a key pair is [3, 5] and the range is [gt(3), gt(2)]. The
   * B-Tree shall stop looping when the first key is out of range since the tree
   * is sorted by first dimension.
   */
  isFirstKeyInRange(key: Key, range: Range): boolean;

  /**
   * Returns a range that represents all data.
   */
  getAllRange(): Range;

  /**
   * Binds unbound values to given key ranges, and sorts them so that these
   * ranges will be in the order from left to right.
   */
  sortKeyRanges(keyRanges: Range[]): Range[];

  /**
   * Returns true if the given range is open ended on the left-hand-side.
   */
  isLeftOpen(range: Range): boolean;

  /**
   * Converts key range to keys.
   */
  rangeToKeys(range: Range): Key[];

  /**
   * Returns false if any dimension of the key contains null.
   */
  comparable(key: Key): boolean;

  /**
   * Returns number of key dimensions.
   */
  keyDimensions(): number;
}
