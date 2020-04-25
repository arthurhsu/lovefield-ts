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

import {Order} from '../base/enum';
import {Favor} from '../base/private_enum';
import {Comparator} from './comparator';
import {SingleKey, SingleKeyRange} from './key_range';

export class SimpleComparator implements Comparator {
  static compareAscending(lhs: SingleKey, rhs: SingleKey): Favor {
    return lhs > rhs ? Favor.LHS : lhs < rhs ? Favor.RHS : Favor.TIE;
  }

  static compareDescending(lhs: SingleKey, rhs: SingleKey): Favor {
    return SimpleComparator.compareAscending(rhs, lhs);
  }

  static orderRangeAscending(lhs: SingleKeyRange, rhs: SingleKeyRange): Favor {
    return SingleKeyRange.compare(lhs, rhs);
  }

  static orderRangeDescending(lhs: SingleKeyRange, rhs: SingleKeyRange): Favor {
    return SingleKeyRange.compare(rhs, lhs);
  }

  protected compareFn: (lhs: SingleKey, rhs: SingleKey) => Favor;
  private normalizeKeyRange: (
    keyrange?: SingleKeyRange
  ) => SingleKeyRange | null;
  private orderRange: (lhs: SingleKeyRange, rhs: SingleKeyRange) => Favor;

  constructor(order: Order) {
    this.compareFn =
      order === Order.DESC
        ? SimpleComparator.compareDescending
        : SimpleComparator.compareAscending;

    this.normalizeKeyRange =
      order === Order.DESC
        ? (keyRange?: SingleKeyRange) => {
            return keyRange !== undefined && keyRange !== null
              ? keyRange.reverse()
              : null;
          }
        : (keyRange?: SingleKeyRange) => keyRange || null;

    this.orderRange =
      order === Order.DESC
        ? SimpleComparator.orderRangeDescending
        : SimpleComparator.orderRangeAscending;
  }

  // Checks if the range covers "left" or "right" of the key (inclusive).
  // For example:
  //
  // key is 2, comparator ASC
  //
  // |-----|-----X-----|-----|
  // 0     1     2     3     4
  //
  // range [0, 4] and [2, 2] cover both left and right, so return [true, true].
  // range [0, 2) covers only left, return [true, false].
  // range (2, 0] covers only right, return [false, true].
  compareRange(key: SingleKey, naturalRange: SingleKeyRange): boolean[] {
    const LEFT = 0;
    const RIGHT = 1;
    const range = this.normalizeKeyRange(naturalRange) as SingleKeyRange;

    const results = [
      SingleKeyRange.isUnbound(range.from),
      SingleKeyRange.isUnbound(range.to),
    ];
    if (!results[LEFT]) {
      const favor = this.compareFn(key, range.from as SingleKey);
      results[LEFT] = range.excludeLower
        ? favor === Favor.LHS
        : favor !== Favor.RHS;
    }

    if (!results[RIGHT]) {
      const favor = this.compareFn(key, range.to as SingleKey);
      results[RIGHT] = range.excludeUpper
        ? favor === Favor.RHS
        : favor !== Favor.LHS;
    }

    return results;
  }

  compare(lhs: SingleKey, rhs: SingleKey): Favor {
    return this.compareFn(lhs, rhs);
  }

  min(lhs: SingleKey, rhs: SingleKey): Favor {
    return lhs < rhs ? Favor.LHS : lhs === rhs ? Favor.TIE : Favor.RHS;
  }

  max(lhs: SingleKey, rhs: SingleKey): Favor {
    return lhs > rhs ? Favor.LHS : lhs === rhs ? Favor.TIE : Favor.RHS;
  }

  isInRange(key: SingleKey, range: SingleKeyRange): boolean {
    const results = this.compareRange(key, range);
    return results[0] && results[1];
  }

  isFirstKeyInRange(key: SingleKey, range: SingleKeyRange): boolean {
    return this.isInRange(key, range);
  }

  getAllRange(): SingleKeyRange {
    return SingleKeyRange.all();
  }

  orderKeyRange(lhs: SingleKeyRange, rhs: SingleKeyRange): Favor {
    return this.orderRange(lhs, rhs);
  }

  sortKeyRanges(keyRanges: SingleKeyRange[]): SingleKeyRange[] {
    return keyRanges
      .filter(range => range !== null)
      .sort((lhs, rhs) => this.orderKeyRange(lhs, rhs));
  }

  isLeftOpen(range: SingleKeyRange): boolean {
    return SingleKeyRange.isUnbound(
      (this.normalizeKeyRange(range) as SingleKeyRange).from
    );
  }

  rangeToKeys(naturalRange: SingleKeyRange): SingleKey[] {
    const range = this.normalizeKeyRange(naturalRange) as SingleKeyRange;
    return [range.from as SingleKey, range.to as SingleKey];
  }

  comparable(key: SingleKey): boolean {
    return key !== null;
  }

  keyDimensions(): number {
    return 1;
  }

  toString(): string {
    return this.compare === SimpleComparator.compareDescending
      ? 'SimpleComparator_DESC'
      : 'SimpleComparator_ASC';
  }
}
