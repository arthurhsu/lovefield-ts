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

import {Favor} from './comparator';

export type SingleKey = string|number;
export type Key = SingleKey|SingleKey[];

// Unbound is used to denote an unbound key range boundary.
export class UnboundKey {}

// A SingleKeyRange represents a key range of a single column.
export class SingleKeyRange {
  public static UNBOUND_VALUE: UnboundKey = new UnboundKey();

  public static isUnbound(value: SingleKey|UnboundKey): boolean {
    return value === SingleKeyRange.UNBOUND_VALUE;
  }

  public static upperBound(key: SingleKey, shouldExclude = false):
      SingleKeyRange {
    return new SingleKeyRange(
        SingleKeyRange.UNBOUND_VALUE, key, false, shouldExclude);
  }

  public static lowerBound(key: SingleKey, shouldExclude = false):
      SingleKeyRange {
    return new SingleKeyRange(
        key, SingleKeyRange.UNBOUND_VALUE, shouldExclude, false);
  }

  // Creates a range that includes a single key.
  public static only(key: SingleKey): SingleKeyRange {
    return new SingleKeyRange(key, key, false, false);
  }

  // Creates a range that includes all keys.
  public static all(): SingleKeyRange {
    return new SingleKeyRange(
        SingleKeyRange.UNBOUND_VALUE, SingleKeyRange.UNBOUND_VALUE, false,
        false);
  }

  public static xor(a: boolean, b: boolean) {
    return a ? !b : b;
  }

  // Compares two ranges, meant to be used in Array#sort.
  public static compare(lhs: SingleKeyRange, rhs: SingleKeyRange): Favor {
    let result = SingleKeyRange.compareKey(
        lhs.from, rhs.from, true, lhs.excludeLower, rhs.excludeLower);
    if (result === Favor.TIE) {
      result = SingleKeyRange.compareKey(
          lhs.to, rhs.to, false, lhs.excludeUpper, rhs.excludeUpper);
    }
    return result;
  }

  // Returns a new range that is the minimum range that covers both ranges
  // given.
  public static getBoundingRange(r1: SingleKeyRange, r2: SingleKeyRange):
      SingleKeyRange {
    let from = SingleKeyRange.UNBOUND_VALUE;
    let to = SingleKeyRange.UNBOUND_VALUE;
    let excludeLower = false;
    let excludeUpper = false;

    if (!SingleKeyRange.isUnbound(r1.from) &&
        !SingleKeyRange.isUnbound(r2.from)) {
      const favor = SingleKeyRange.compareKey(r1.from, r2.from, true);
      if (favor !== Favor.LHS) {
        from = r1.from;
        excludeLower = (favor !== Favor.TIE) ?
            r1.excludeLower :
            r1.excludeLower && r2.excludeLower;
      } else {
        from = r2.from;
        excludeLower = r2.excludeLower;
      }
    }
    if (!SingleKeyRange.isUnbound(r1.to) && !SingleKeyRange.isUnbound(r2.to)) {
      const favor = SingleKeyRange.compareKey(r1.to, r2.to, false);
      if (favor !== Favor.RHS) {
        to = r1.to;
        excludeUpper = (favor !== Favor.TIE) ?
            r1.excludeUpper :
            r1.excludeUpper && r2.excludeUpper;
      } else {
        to = r2.to;
        excludeUpper = r2.excludeUpper;
      }
    }
    return new SingleKeyRange(from, to, excludeLower, excludeUpper);
  }

  // Intersects two ranges and return their intersection.
  // Returns null if intersection is empty.
  public static and(r1: SingleKeyRange, r2: SingleKeyRange): SingleKeyRange {
    if (!r1.overlaps(r2)) {
      return null as any as SingleKeyRange;
    }

    let favor = SingleKeyRange.compareKey(r1.from, r2.from, true);
    const left = (favor === Favor.TIE) ? (r1.excludeLower ? r1 : r2) :
                                         (favor !== Favor.RHS) ? r1 : r2;

    // right side boundary test is different, null is considered greater.
    let right: SingleKeyRange;
    if (SingleKeyRange.isUnbound(r1.to) || SingleKeyRange.isUnbound(r2.to)) {
      right = (SingleKeyRange.isUnbound(r1.to)) ? r2 : r1;
    } else {
      favor = SingleKeyRange.compareKey(r1.to, r2.to, false);
      right = (favor === Favor.TIE) ? (r1.excludeUpper ? r1 : r2) :
                                      (favor === Favor.RHS) ? r1 : r2;
    }
    return new SingleKeyRange(
        left.from, right.to, left.excludeLower, right.excludeUpper);
  }

  // Calculates the complement key ranges of the input key ranges.
  // NOTE: The key ranges passed in this method must satisfy "isOnly() == true",
  // and none of from/to should be null.
  public static complement(keyRanges: SingleKeyRange[]): SingleKeyRange[] {
    if (keyRanges.length === 0) {
      return SingleKeyRange.EMPTY_RANGE;
    }

    keyRanges.sort(SingleKeyRange.compare);
    const complementKeyRanges = new Array(keyRanges.length + 1);
    for (let i = 0; i < complementKeyRanges.length; i++) {
      if (i === 0) {
        complementKeyRanges[i] =
            SingleKeyRange.upperBound(keyRanges[i].from as SingleKey, true);
      } else if (i === complementKeyRanges.length - 1) {
        complementKeyRanges[i] =
            SingleKeyRange.lowerBound(keyRanges[i - 1].to as SingleKey, true);
      } else {
        complementKeyRanges[i] = new SingleKeyRange(
            keyRanges[i - 1].to, keyRanges[i].from, true, true);
      }
    }
    return complementKeyRanges;
  }

  private static EMPTY_RANGE: SingleKeyRange[] = [];

  private static compareKey(
      l: SingleKey|UnboundKey, r: SingleKey|UnboundKey, isLeftHandSide: boolean,
      excludeL = false, excludeR = false): Favor {
    const flip = (favor: Favor) =>
        isLeftHandSide ? favor : (favor === Favor.LHS) ? Favor.RHS : Favor.LHS;

    // The following logic is implemented for LHS. RHS is achieved using flip().
    const tieLogic = () => !SingleKeyRange.xor(excludeL, excludeR) ?
        Favor.TIE :
        excludeL ? flip(Favor.LHS) : flip(Favor.RHS);

    if (SingleKeyRange.isUnbound(l)) {
      return !SingleKeyRange.isUnbound(r) ? flip(Favor.RHS) : tieLogic();
    }
    return SingleKeyRange.isUnbound(r) ?
        flip(Favor.LHS) :
        (l < r) ? Favor.RHS : (l === r) ? tieLogic() : Favor.LHS;
  }

  public readonly excludeLower: boolean;
  public readonly excludeUpper: boolean;

  constructor(
      readonly from: SingleKey|UnboundKey, readonly to: SingleKey|UnboundKey,
      excludeLower: boolean, excludeUpper: boolean) {
    this.excludeLower =
        !SingleKeyRange.isUnbound(this.from) ? excludeLower : false;
    this.excludeUpper =
        !SingleKeyRange.isUnbound(this.to) ? excludeUpper : false;
  }

  // A text representation of this key range, useful for tests.
  // Example: [a, b] means from a to b, with both a and b included in the range.
  // Example: (a, b] means from a to b, with a excluded, b included.
  // Example: (a, b) means from a to b, with both a and b excluded.
  // Example: [unbound, b) means anything less than b, with b not included.
  // Example: [a, unbound] means anything greater than a, with a included.
  public toString(): string {
    return (this.excludeLower ? '(' : '[') +
        (SingleKeyRange.isUnbound(this.from) ? 'unbound' : this.from) + ', ' +
        (SingleKeyRange.isUnbound(this.to) ? 'unbound' : this.to) +
        (this.excludeUpper ? ')' : ']');
  }

  // Finds the complement key range. Note that in some cases the complement is
  // composed of two disjoint key ranges. For example complementing [10, 20]
  // would result in [unbound, 10) and (20, unbound]. An empty array will be
  // returned in the case where the complement is empty.
  public complement(): SingleKeyRange[] {
    // Complement of SingleKeyRange.all() is empty.
    if (this.isAll()) {
      return SingleKeyRange.EMPTY_RANGE;
    }

    let keyRangeLow: SingleKeyRange = null as any as SingleKeyRange;
    let keyRangeHigh: SingleKeyRange = null as any as SingleKeyRange;

    if (!SingleKeyRange.isUnbound(this.from)) {
      keyRangeLow = new SingleKeyRange(
          SingleKeyRange.UNBOUND_VALUE, this.from, false, !this.excludeLower);
    }

    if (!SingleKeyRange.isUnbound(this.to)) {
      keyRangeHigh = new SingleKeyRange(
          this.to, SingleKeyRange.UNBOUND_VALUE, !this.excludeUpper, false);
    }

    return [keyRangeLow, keyRangeHigh].filter((keyRange) => keyRange !== null);
  }

  // Reverses a keyRange such that "lower" refers to larger values and "upper"
  // refers to smaller values. Note: This is different than what complement()
  // does.
  public reverse(): SingleKeyRange {
    return new SingleKeyRange(
        this.to, this.from, this.excludeUpper, this.excludeLower);
  }

  // Determines if this range overlaps with the given one.
  public overlaps(range: SingleKeyRange): boolean {
    const favor = SingleKeyRange.compareKey(
        this.from, range.from, true, this.excludeLower, range.excludeLower);
    if (favor === Favor.TIE) {
      return true;
    }
    const left = (favor === Favor.RHS) ? this : range;
    const right = (favor === Favor.LHS) ? this : range;

    return SingleKeyRange.isUnbound(left.to) || left.to > right.from ||
        (left.to === right.from && !left.excludeUpper && !right.excludeLower);
  }

  // Returns whether the range is all.
  public isAll(): boolean {
    return SingleKeyRange.isUnbound(this.from) &&
        SingleKeyRange.isUnbound(this.to);
  }

  // Returns if the range is only.
  public isOnly(): boolean {
    return this.from === this.to && !SingleKeyRange.isUnbound(this.from) &&
        !this.excludeLower && !this.excludeUpper;
  }

  public contains(key: SingleKey): boolean {
    const left = SingleKeyRange.isUnbound(this.from) || (key > this.from) ||
        (key === this.from && !this.excludeLower);
    const right = SingleKeyRange.isUnbound(this.to) || (key < this.to) ||
        (key === this.to && !this.excludeUpper);
    return left && right;
  }

  // Bound the range with [min, max] and return the newly bounded range.
  // When the given bound has no intersection with this range, or the
  // range/bound is reversed, return null.
  public getBounded(min: SingleKey, max: SingleKey): SingleKeyRange {
    // Eliminate out of range scenarios.
    if ((SingleKeyRange.isUnbound(this.from) && !this.contains(min)) ||
        (SingleKeyRange.isUnbound(this.to) && !this.contains(max))) {
      return null as any as SingleKeyRange;
    }

    let from = min;
    let to = max;
    let excludeLower = false;
    let excludeUpper = false;
    if (!SingleKeyRange.isUnbound(this.from) && this.from >= min) {
      from = this.from as SingleKey;
      excludeLower = this.excludeLower;
    }
    if (!SingleKeyRange.isUnbound(this.to) && this.to <= max) {
      to = this.to as SingleKey;
      excludeUpper = this.excludeUpper;
    }

    if (from > to || (from === to && (excludeUpper || excludeLower))) {
      return null as any as SingleKeyRange;
    }
    return new SingleKeyRange(from, to, excludeLower, excludeUpper);
  }

  public equals(range: SingleKeyRange): boolean {
    return this.from === range.from &&
        this.excludeLower === range.excludeLower && this.to === range.to &&
        this.excludeUpper === range.excludeUpper;
  }
}

// A KeyRange is more generic than a SingleKeyRange since it can express a key
// range of a cross-column key. The SingleKeyRange instances in the array
// represent a key range for each dimension of the cross-column key range.
// Example for a Index.Key (x, y) a KeyRange of [[0, 100], [50, 70]]
// represents the 2D area where 0 >= x >= 100 AND 50 <= y <=100.
export type KeyRange = SingleKeyRange[];
