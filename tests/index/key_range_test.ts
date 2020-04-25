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

import * as chai from 'chai';
import {Favor} from '../../lib/base/private_enum';
import {SingleKeyRange} from '../../lib/index/key_range';

const assert = chai.assert;

describe('KeyRange', () => {
  it('complement_WithBounds', () => {
    // Testing case where both lower and upper bound are included.
    let keyRange = new SingleKeyRange(10, 20, false, false);
    let complementKeyRanges = keyRange.complement();
    assert.equal(2, complementKeyRanges.length);
    assert.equal('[unbound, 10)', complementKeyRanges[0].toString());
    assert.equal('(20, unbound]', complementKeyRanges[1].toString());

    // Testing case where lower bound is excluded.
    keyRange = new SingleKeyRange(10, 20, true, false);
    complementKeyRanges = keyRange.complement();
    assert.equal(2, complementKeyRanges.length);
    assert.equal('[unbound, 10]', complementKeyRanges[0].toString());
    assert.equal('(20, unbound]', complementKeyRanges[1].toString());

    // Testing case where upper bound is excluded.
    keyRange = new SingleKeyRange(10, 20, false, true);
    complementKeyRanges = keyRange.complement();
    assert.equal(2, complementKeyRanges.length);
    assert.equal('[unbound, 10)', complementKeyRanges[0].toString());
    assert.equal('[20, unbound]', complementKeyRanges[1].toString());

    // Testing case where both lower and upper bound are excluded.
    keyRange = new SingleKeyRange(10, 20, true, true);
    complementKeyRanges = keyRange.complement();
    assert.equal(2, complementKeyRanges.length);
    assert.equal('[unbound, 10]', complementKeyRanges[0].toString());
    assert.equal('[20, unbound]', complementKeyRanges[1].toString());
  });

  it('complement_UpperBoundOnly', () => {
    let keyRange = SingleKeyRange.upperBound(20);
    let complementKeyRanges = keyRange.complement();
    assert.equal(1, complementKeyRanges.length);
    assert.equal('(20, unbound]', complementKeyRanges[0].toString());

    keyRange = SingleKeyRange.upperBound(20, true);
    complementKeyRanges = keyRange.complement();
    assert.equal(1, complementKeyRanges.length);
    assert.equal('[20, unbound]', complementKeyRanges[0].toString());
  });

  it('complement_LowerBoundOnly', () => {
    let keyRange = SingleKeyRange.lowerBound(20);
    let complementKeyRanges = keyRange.complement();
    assert.equal(1, complementKeyRanges.length);
    assert.equal('[unbound, 20)', complementKeyRanges[0].toString());

    keyRange = SingleKeyRange.lowerBound(20, true);
    complementKeyRanges = keyRange.complement();
    assert.equal(1, complementKeyRanges.length);
    assert.equal('[unbound, 20]', complementKeyRanges[0].toString());
  });

  it('complement_NoBound', () => {
    const keyRange = SingleKeyRange.all();
    // The complement of unbounded key range is empty key range.
    assert.equal(0, keyRange.complement().length);
  });

  it('complement_OnlyOneValue', () => {
    const keyRange = SingleKeyRange.only(20);
    const complementKeyRanges = keyRange.complement();
    assert.equal(2, complementKeyRanges.length);
    assert.equal('[unbound, 20)', complementKeyRanges[0].toString());
    assert.equal('(20, unbound]', complementKeyRanges[1].toString());
  });

  it('complement_MultiKeyRanges', () => {
    let complementKeyRanges = SingleKeyRange.complement([]);
    assert.equal(0, complementKeyRanges.length);

    const keyRange1 = SingleKeyRange.only(20);
    complementKeyRanges = SingleKeyRange.complement([keyRange1]);
    assert.equal('[unbound, 20),(20, unbound]', complementKeyRanges.join(','));

    const keyRange2 = SingleKeyRange.only(40);
    complementKeyRanges = SingleKeyRange.complement([keyRange1, keyRange2]);
    assert.equal(
      '[unbound, 20),(20, 40),(40, unbound]',
      complementKeyRanges.join(',')
    );

    complementKeyRanges = SingleKeyRange.complement([keyRange2, keyRange1]);
    assert.equal(
      '[unbound, 20),(20, 40),(40, unbound]',
      complementKeyRanges.join(',')
    );
  });

  it('isOnly', () => {
    assert.isFalse(SingleKeyRange.upperBound(20).isOnly());
    assert.isFalse(SingleKeyRange.all().isOnly());
    assert.isTrue(SingleKeyRange.only(20).isOnly());
  });

  it('isAll', () => {
    assert.isFalse(SingleKeyRange.only(20).isAll());
    assert.isFalse(SingleKeyRange.upperBound(20).isAll());
    assert.isTrue(SingleKeyRange.all().isAll());
  });

  it('reverse', () => {
    let keyRange = SingleKeyRange.only(20);
    assert.equal('[20, 20]', keyRange.toString());
    assert.equal('[20, 20]', keyRange.reverse().toString());

    keyRange = SingleKeyRange.upperBound(20);
    assert.equal('[unbound, 20]', keyRange.toString());
    assert.equal('[20, unbound]', keyRange.reverse().toString());

    keyRange = SingleKeyRange.lowerBound(20);
    assert.equal('[20, unbound]', keyRange.toString());
    assert.equal('[unbound, 20]', keyRange.reverse().toString());

    keyRange = SingleKeyRange.all();
    assert.equal('[unbound, unbound]', keyRange.toString());
    assert.equal('[unbound, unbound]', keyRange.reverse().toString());

    keyRange = new SingleKeyRange(20, 50, false, true);
    assert.equal('[20, 50)', keyRange.toString());
    assert.equal('(50, 20]', keyRange.reverse().toString());
  });

  it('contains', () => {
    let range = new SingleKeyRange(0, 10, true, true);
    assert.isFalse(range.contains(-1));
    assert.isFalse(range.contains(0));
    assert.isTrue(range.contains(5));
    assert.isFalse(range.contains(10));
    assert.isFalse(range.contains(11));

    range = new SingleKeyRange('B', 'D', false, false);
    assert.isFalse(range.contains('A'));
    assert.isTrue(range.contains('B'));
    assert.isTrue(range.contains('C'));
    assert.isTrue(range.contains('D'));
    assert.isFalse(range.contains('E'));
  });

  it('getBounded', () => {
    const range = new SingleKeyRange(1, 10, true, true);
    const bound = (min: number, max: number) => {
      const r = range.getBounded(min, max);
      return r === null ? 'null' : r.toString();
    };

    assert.equal('(1, 10)', bound(0, 11));
    assert.equal('(1, 10)', bound(1, 10));
    assert.equal('(1, 2]', bound(0, 2));
    assert.equal('[2, 10)', bound(2, 11));
    assert.equal('[2, 3]', bound(2, 3));
    assert.equal('null', bound(-1, 0));
    assert.equal('null', bound(11, 12));
  });

  it('equals', () => {
    assert.isTrue(SingleKeyRange.all().equals(SingleKeyRange.all()));
    assert.isTrue(SingleKeyRange.only(1).equals(SingleKeyRange.only(1)));
    assert.isTrue(
      new SingleKeyRange(1, 2, true, false).equals(
        new SingleKeyRange(1, 2, true, false)
      )
    );
    assert.isFalse(
      new SingleKeyRange(1, 2, false, false).equals(
        new SingleKeyRange(1, 2, true, false)
      )
    );
  });

  it('xor', () => {
    const xor = SingleKeyRange.xor;
    assert.isFalse(xor(true, true));
    assert.isTrue(xor(true, false));
    assert.isTrue(xor(false, true));
    assert.isFalse(xor(false, false));
  });

  function generateTestRanges(): {[key: string]: SingleKeyRange} {
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

  it('compare', () => {
    const c = SingleKeyRange.compare;

    const r = generateTestRanges();

    const cases = [
      r.all,
      r.upTo1,
      r.upTo1Ex,
      r.atLeast1,
      r.atLeast1Ex,
      r.only1,
      r.r1,
      r.r2,
      r.r3,
      r.r4,
    ];
    cases.forEach(s => assert.equal(Favor.TIE, c(s, s), s.toString()));

    // Test pairs that RHS always wins.
    const pairs = [
      [r.upTo1, r.all],
      [r.all, r.atLeast1],
      [r.all, r.only1],
      [r.atLeast1, r.atLeast2],
      [r.upTo1, r.upTo2],
      [r.atLeast1, r.atLeast1Ex],
      [r.upTo1Ex, r.upTo1],
      [r.r1, r.r2],
      [r.r3, r.r1],
      [r.r1, r.r4],
      [r.r3, r.r2],
      [r.r1, r.r5],
      [r.r6, r.r1],
      [r.only1, r.only2],
    ];

    pairs.forEach(pair => {
      assert.equal(Favor.RHS, c(pair[0], pair[1]), pair[0].toString());
      assert.equal(Favor.LHS, c(pair[1], pair[0]), pair[0].toString());
    });
  });

  it('overlaps', () => {
    const r = generateTestRanges();

    const cases = [
      r.all,
      r.upTo1,
      r.upTo1Ex,
      r.atLeast1,
      r.atLeast1Ex,
      r.only1,
      r.r1,
      r.r2,
      r.r3,
      r.r4,
    ];
    cases.forEach(range => {
      assert.isTrue(range.overlaps(range), range.toString());
      assert.isTrue(range.overlaps(r.all), range.toString());
      assert.isTrue(r.all.overlaps(range), range.toString());
    });

    const overlapping = [
      [r.upTo1, r.upTo1Ex],
      [r.upTo1, r.upTo2],
      [r.upTo1, r.only1],
      [r.upTo1, r.atLeast1],
      [r.upTo1, r.r6],
      [r.upTo1Ex, r.upTo2],
      [r.atLeast1, r.only1],
      [r.atLeast1, r.only2],
      [r.atLeast1, r.r1],
      [r.atLeast1, r.r6],
      [r.r1, r.r2],
      [r.r1, r.r3],
      [r.r1, r.r4],
      [r.r1, r.r5],
      [r.r1, r.r6],
      [r.r2, r.r3],
      [r.r2, r.r4],
    ];
    overlapping.forEach(pair => {
      assert.isTrue(pair[0].overlaps(pair[1]));
      assert.isTrue(pair[1].overlaps(pair[0]));
    });

    const excluding = [
      [r.upTo1, r.only2],
      [r.upTo1Ex, r.r6],
      [r.upTo1, r.atLeast1Ex],
      [r.upTo1, r.atLeast2],
      [r.upTo1Ex, r.atLeast1Ex],
      [r.upTo1Ex, r.only1],
      [r.upTo1Ex, r.only2],
      [r.only1, r.atLeast1Ex],
      [r.only1, r.atLeast2],
      [r.r3, r.r5],
      [r.r4, r.r5],
      [r.r2, r.r6],
      [r.r4, r.r6],
    ];
    excluding.forEach(pair => {
      assert.isFalse(pair[0].overlaps(pair[1]));
      assert.isFalse(pair[1].overlaps(pair[0]));
    });
  });

  it('getBoundingRange', () => {
    const r = generateTestRanges();
    const check = (
      expected: SingleKeyRange,
      r1: SingleKeyRange,
      r2: SingleKeyRange
    ) => {
      assert.isTrue(
        expected.equals(SingleKeyRange.getBoundingRange(r1, r2)),
        expected.toString()
      );
      assert.isTrue(
        expected.equals(SingleKeyRange.getBoundingRange(r2, r1)),
        expected.toString()
      );
    };

    // Self and or with r.all.
    check(r.all, r.all, r.all);
    const cases = [
      r.upTo1,
      r.upTo1Ex,
      r.atLeast1,
      r.atLeast1Ex,
      r.only1,
      r.r1,
      r.r2,
      r.r3,
      r.r4,
    ];
    cases.forEach(range => {
      check(range, range, range);
      check(r.all, range, r.all);
    });

    // Overlapping test cases.
    check(r.upTo1, r.upTo1, r.upTo1Ex);
    check(r.upTo2, r.upTo1, r.upTo2);
    check(r.upTo1, r.upTo1, r.only1);
    check(r.upTo2Ex, r.upTo1, r.upTo2Ex);
    check(r.all, r.upTo1, r.atLeast1);
    check(SingleKeyRange.upperBound(5), r.upTo1, r.r6);
    check(r.upTo2, r.upTo1Ex, r.upTo2);
    check(r.atLeast1, r.atLeast1, r.only1);
    check(r.atLeast1, r.atLeast1, r.only2);
    check(r.atLeast1, r.atLeast1, r.r1);
    check(r.atLeast1, r.atLeast1, r.r6);
    check(r.atLeast1Ex, r.atLeast1Ex, r.atLeast2);
    check(r.r1, r.r1, r.r2);
    check(r.r1, r.r1, r.r3);
    check(r.r1, r.r1, r.r4);
    check(new SingleKeyRange(5, 11, false, false), r.r1, r.r5);
    check(new SingleKeyRange(1, 10, false, false), r.r1, r.r6);
    check(r.r1, r.r2, r.r3);
    check(r.r2, r.r2, r.r4);

    // Non-overlapping test cases.
    check(r.all, r.upTo1Ex, r.atLeast1Ex);
    check(r.upTo1, r.upTo1Ex, r.only1);
    check(r.atLeast1, r.atLeast1Ex, r.only1);
    check(new SingleKeyRange(-1, 10, false, true), r.r7, r.r3);
  });

  it('and', () => {
    const r = generateTestRanges();
    const check = (
      expected: SingleKeyRange,
      r1: SingleKeyRange,
      r2: SingleKeyRange
    ) => {
      assert.isTrue(
        expected.equals(SingleKeyRange.and(r1, r2)),
        expected.toString()
      );
      assert.isTrue(
        expected.equals(SingleKeyRange.and(r2, r1)),
        expected.toString()
      );
    };

    // Self and or with r.all.
    check(r.all, r.all, r.all);
    const cases = [
      r.upTo1,
      r.upTo1Ex,
      r.atLeast1,
      r.atLeast1Ex,
      r.only1,
      r.r1,
      r.r2,
      r.r3,
      r.r4,
    ];
    cases.forEach(range => {
      check(range, range, range);
      check(range, range, r.all);
    });

    // Overlapping test cases.
    check(r.upTo1Ex, r.upTo1, r.upTo1Ex);
    check(r.upTo1, r.upTo1, r.upTo2);
    check(r.only1, r.upTo1, r.only1);
    check(r.upTo1, r.upTo1, r.upTo2Ex);
    check(r.only1, r.upTo1, r.atLeast1);
    check(r.only1, r.upTo1, r.r6);
    check(r.upTo1Ex, r.upTo1Ex, r.upTo2);
    check(r.only1, r.atLeast1, r.only1);
    check(r.r1, r.atLeast1, r.r1);
    check(r.r6, r.atLeast1, r.r6);
    check(r.atLeast2, r.atLeast1Ex, r.atLeast2);
    check(r.r2, r.r1, r.r2);
    check(r.r3, r.r1, r.r3);
    check(r.r4, r.r1, r.r4);
    check(SingleKeyRange.only(10), r.r1, r.r5);
    check(SingleKeyRange.only(5), r.r1, r.r6);
    check(r.r4, r.r2, r.r3);
    check(r.r4, r.r2, r.r4);

    // Excluding shall return null.
    const excluding = [
      [r.upTo1, r.only2],
      [r.upTo1Ex, r.r6],
      [r.upTo1, r.atLeast1Ex],
      [r.upTo1, r.atLeast2],
      [r.upTo1Ex, r.only1],
      [r.upTo1Ex, r.only2],
      [r.only1, r.atLeast1Ex],
      [r.only1, r.atLeast2],
      [r.r3, r.r5],
      [r.r4, r.r5],
      [r.r6, r.r2],
      [r.r6, r.r4],
    ];
    excluding.forEach(pair => {
      assert.isNull(SingleKeyRange.and(pair[0], pair[1]));
      assert.isNull(SingleKeyRange.and(pair[1], pair[0]));
    });
  });
});
