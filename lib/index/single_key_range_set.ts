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

import {SingleKey, SingleKeyRange} from './key_range';

export class SingleKeyRangeSet {
  // Intersection of two range sets.
  static intersect(
    s0: SingleKeyRangeSet,
    s1: SingleKeyRangeSet
  ): SingleKeyRangeSet {
    const ranges = s0.getValues().map(r0 => {
      return s1.getValues().map(r1 => SingleKeyRange.and(r0, r1));
    });

    let results: SingleKeyRange[] = [];
    ranges.forEach(dimension => (results = results.concat(dimension)));

    return new SingleKeyRangeSet(results.filter(r => r !== null));
  }

  private ranges: SingleKeyRange[];

  constructor(ranges?: SingleKeyRange[]) {
    this.ranges = [];
    if (ranges) {
      this.add(ranges);
    }
  }

  toString(): string {
    return this.ranges.map(r => r.toString()).join(',');
  }

  containsKey(key: SingleKey): boolean {
    return this.ranges.some(r => r.contains(key));
  }

  getValues(): SingleKeyRange[] {
    return this.ranges;
  }

  add(keyRanges: SingleKeyRange[]): void {
    if (keyRanges.length === 0) {
      return;
    }

    const ranges = this.ranges.concat(keyRanges);
    if (ranges.length === 1) {
      this.ranges = ranges;
      return;
    }

    ranges.sort(SingleKeyRange.compare);
    const results: SingleKeyRange[] = [];
    let start = ranges[0];
    for (let i = 1; i < ranges.length; ++i) {
      if (start.overlaps(ranges[i])) {
        start = SingleKeyRange.getBoundingRange(start, ranges[i]);
      } else {
        results.push(start);
        start = ranges[i];
      }
    }
    results.push(start);
    this.ranges = results;
  }

  equals(set: SingleKeyRangeSet): boolean {
    if (this.ranges.length === set.ranges.length) {
      return (
        this.ranges.length === 0 ||
        this.ranges.every((r, index) => r.equals(set.ranges[index]))
      );
    }

    return false;
  }

  // Returns the boundary of this set, null if range set is empty.
  getBoundingRange(): SingleKeyRange | null {
    if (this.ranges.length <= 1) {
      return this.ranges.length === 0 ? null : this.ranges[0];
    }

    const last = this.ranges.length - 1;
    return SingleKeyRange.getBoundingRange(this.ranges[0], this.ranges[last]);
  }
}
