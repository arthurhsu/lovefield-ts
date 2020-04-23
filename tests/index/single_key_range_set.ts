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

import * as chai from 'chai';
import { SingleKeyRange } from '../../lib/index/key_range';
import { SingleKeyRangeSet } from '../../lib/index/single_key_range_set';

const assert = chai.assert;

describe('SingleKeyRangeSet', () => {
  function generateTestRanges(): { [key: string]: SingleKeyRange } {
    return {
      all: SingleKeyRange.all(),
      atLeast1: SingleKeyRange.lowerBound(1),
      atLeast1Ex: SingleKeyRange.lowerBound(1, true),
      atLeast2: SingleKeyRange.lowerBound(2),
      atLeast2Ex: SingleKeyRange.lowerBound(2, true),
      only1: SingleKeyRange.only(1),
      only2: SingleKeyRange.only(2),
      r1: new SingleKeyRange(5, 10, false, false),
      r2: new SingleKeyRange(5, 10, true, false),
      r3: new SingleKeyRange(5, 10, false, true),
      r4: new SingleKeyRange(5, 10, true, true),
      r5: new SingleKeyRange(10, 11, false, false),
      r6: new SingleKeyRange(1, 5, false, false),
      r7: new SingleKeyRange(-1, 0, false, false),
      upTo1: SingleKeyRange.upperBound(1),
      upTo1Ex: SingleKeyRange.upperBound(1, true),
      upTo2: SingleKeyRange.upperBound(2),
      upTo2Ex: SingleKeyRange.upperBound(2, true),
    };
  }

  it('add', () => {
    function check(expected: SingleKeyRange[], ranges: SingleKeyRange[]): void {
      const set = new SingleKeyRangeSet();
      set.add(ranges);
      assert.sameDeepOrderedMembers(expected, set.getValues());
    }
    const r = generateTestRanges();

    // Empty
    check([], []);

    // Self
    check([r.all], [r.all]);
    check([r.upTo1], [r.upTo1]);
    check([r.atLeast1], [r.atLeast1]);
    check([r.only1], [r.only1]);
    check([r.r1], [r.r1]);
    check([r.r2], [r.r2, r.r2]);
    check([r.r3], [r.r3, r.r3]);
    check([r.r4], [r.r4, r.r4]);

    // Merge to r.all
    check([r.all], [r.all, r.upTo1]);
    check([r.all], [r.all, r.r1, r.r5]);
    check([r.all], [r.only2, r.only1, r.all]);
    check([r.all], [r.r1, r.only2, r.atLeast1Ex, r.all]);

    // Overlapping test cases.
    check([r.upTo1], [r.upTo1, r.upTo1Ex]);
    check([r.upTo2], [r.upTo1, r.upTo2]);
    check([r.upTo1], [r.upTo1, r.only1]);
    check([r.all], [r.upTo1, r.atLeast1]);
    check([SingleKeyRange.upperBound(5)], [r.upTo1, r.r6]);
    check([r.upTo2], [r.upTo1Ex, r.upTo2]);
    check([r.atLeast1], [r.atLeast1, r.only1]);
    check([r.atLeast1], [r.atLeast1, r.only2]);
    check([r.atLeast1], [r.atLeast1, r.r1]);
    check([r.atLeast1], [r.atLeast1, r.r6]);
    check([r.r1], [r.r1, r.r2]);
    check([r.r1], [r.r1, r.r3]);
    check([r.r1], [r.r1, r.r4]);
    check([new SingleKeyRange(5, 11, false, false)], [r.r1, r.r5]);
    check([new SingleKeyRange(1, 10, false, false)], [r.r1, r.r6]);
    check([r.r1], [r.r2, r.r3]);
    check([r.r2], [r.r2, r.r4]);
    check([r.r1], [r.r1, r.r2, r.r3, r.r4]);
    check(
      [new SingleKeyRange(1, 11, false, false)],
      [r.r1, r.r2, r.r3, r.r4, r.r5, r.r6]
    );
    check([r.all], [r.atLeast1, r.r1, r.r5, r.r6, r.upTo1]);

    const excluding = [
      [r.upTo1, r.only2],
      [r.upTo1Ex, r.r6],
      [r.upTo1, r.atLeast1Ex],
      [r.upTo1, r.atLeast2],
      [r.upTo1Ex, r.only1],
      [r.upTo1Ex, r.only2],
      [r.upTo1Ex, r.atLeast1Ex],
      [r.upTo1Ex, r.atLeast2],
      [r.only1, r.atLeast1Ex],
      [r.only1, r.atLeast2],
      [r.r3, r.r5],
      [r.r4, r.r5],
      [r.r6, r.r2],
      [r.r6, r.r4],
    ];
    excluding.forEach(pair => check(pair, pair));
    check([r.r7, r.r6, r.r5], [r.r5, r.r7, r.r7, r.r6]);
    check([r.upTo1Ex, r.only1, r.r5], [r.upTo1Ex, r.only1, r.r5]);
  });

  it('boundingRange', () => {
    function check(expected: SingleKeyRange, ranges: SingleKeyRange[]): void {
      const set = new SingleKeyRangeSet(ranges);
      assert.isTrue(expected.equals(set.getBoundingRange() as SingleKeyRange));
    }
    const r = generateTestRanges();

    check(r.all, [r.only1, r.all]);
    check(r.all, [r.upTo1Ex, r.atLeast1]);
    check(r.all, [r.upTo1Ex, r.atLeast1Ex]);
    check(r.all, [r.upTo1, r.atLeast1]);
    check(new SingleKeyRange(1, 11, false, false), [r.r5, r.r6]);
  });

  it('equals', () => {
    const r = generateTestRanges();

    assert.isTrue(new SingleKeyRangeSet().equals(new SingleKeyRangeSet()));
    assert.isTrue(
      new SingleKeyRangeSet([r.all]).equals(
        new SingleKeyRangeSet([r.upTo1, r.atLeast1])
      )
    );
    assert.isFalse(
      new SingleKeyRangeSet([r.all]).equals(
        new SingleKeyRangeSet([r.upTo1Ex, r.atLeast1])
      )
    );
  });

  it('intersect', () => {
    const r = generateTestRanges();

    /**
     * @param {!Array<!SingleKeyRange>} expected
     * @param {!Array<!SingleKeyRange>} ranges0
     * @param {!Array<!SingleKeyRange>} ranges1
     */
    function check(
      expected: SingleKeyRange[],
      ranges0: SingleKeyRange[],
      ranges1: SingleKeyRange[]
    ): void {
      const s0 = new SingleKeyRangeSet(ranges0);
      const s1 = new SingleKeyRangeSet(ranges1);
      const intersected = SingleKeyRangeSet.intersect(s0, s1);
      assert.isTrue(new SingleKeyRangeSet(expected).equals(intersected));
    }

    // Empty in empty out.
    check([], [], []);

    // No intersections.
    check([], [r.only1], [r.only2]);
    check([], [r.upTo1Ex, r.atLeast1Ex], [r.only1]);
    check([], [r.r5, r.r7], [r.r4]);

    // One overlap.
    check([r.only1], [r.upTo1], [r.atLeast1]);
    check([r.only1], [r.all], [r.only1]);
    check([r.only1], [r.r6], [r.upTo1]);
    check([new SingleKeyRange(1, 5, true, false)], [r.r6], [r.atLeast1Ex]);

    // Two overlaps
    check(
      [r.only1, new SingleKeyRange(1, 5, true, false)],
      [r.r6],
      [r.upTo1, r.atLeast1Ex]
    );
    check([r.upTo1, r.atLeast2], [r.upTo2, r.atLeast1], [r.upTo1, r.atLeast2]);
    check(
      [r.upTo1Ex, r.atLeast2],
      [r.upTo2, r.atLeast1],
      [r.upTo1Ex, r.atLeast2]
    );
    check(
      [r.upTo1, r.atLeast2Ex],
      [r.upTo2Ex, r.atLeast1],
      [r.upTo1, r.atLeast2Ex]
    );
    check([r.only1, r.only2], [r.upTo1, r.atLeast2], [r.only1, r.only2]);

    // Three overlaps
    check(
      [r.upTo1Ex, r.only2, r.r4],
      [r.upTo2Ex, r.only2, r.r1],
      [r.upTo1Ex, r.only2, r.r4]
    );
  });
});
