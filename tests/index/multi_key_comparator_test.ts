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
import { Order } from '../../lib/base/enum';
import { Favor } from '../../lib/base/private_enum';
import { SingleKey, SingleKeyRange } from '../../lib/index/key_range';
import { MultiKeyComparator } from '../../lib/index/multi_key_comparator';
import { MultiKeyComparatorWithNull } from '../../lib/index/multi_key_comparator_with_null';

const assert = chai.assert;

describe('MultiKeyComparator', () => {
  function shuffleAndTest(testFn: (c: MultiKeyComparator) => void): void {
    const ORDER = [Order.DESC, Order.ASC];
    ORDER.forEach(o1 => {
      ORDER.forEach(o2 => {
        const c = new MultiKeyComparator([o1, o2]);
        testFn(c);
      });
    });
  }

  function checkMin(c: MultiKeyComparator): void {
    assert.equal(Favor.TIE, c.min([0, 'A'], [0, 'A']));
    assert.equal(Favor.TIE, c.min(['', ''], ['', '']));
    assert.equal(Favor.TIE, c.min([888.88, 888.88], [888.88, 888.88]));
    assert.equal(Favor.TIE, c.min(['ab', 'ab'], ['ab', 'ab']));

    assert.equal(Favor.RHS, c.min([1, 'A'], [0, 'Z']));
    assert.equal(Favor.LHS, c.min([0, 999], [1, 888]));
    assert.equal(Favor.RHS, c.min([1, 1], [1, 0]));
    assert.equal(Favor.LHS, c.min(['A', 'D'], ['A', 'Z']));
    assert.equal(Favor.RHS, c.min([888.88, 999], [888.87, 1]));
    assert.equal(Favor.LHS, c.min([888.87, 999], [888.88, 1]));
    assert.equal(Favor.RHS, c.min([888.88, 999], [888.88, 1]));
    assert.equal(Favor.LHS, c.min([1, 888.87], [1, 888.88]));

    assert.equal(Favor.RHS, c.min(['b', 1], ['a', 999]));
    assert.equal(Favor.LHS, c.min(['a', 999], ['b', 0]));
    assert.equal(Favor.RHS, c.min(['b', 'b'], ['b', 'a']));
    assert.equal(Favor.LHS, c.min(['a', 'a'], ['a', 'b']));
    assert.equal(Favor.RHS, c.min(['bbb', 'bba'], ['bba', 'bbb']));
    assert.equal(Favor.LHS, c.min(['bba', 'bbb'], ['bbb', 'bba']));
    assert.equal(Favor.RHS, c.min(['bbb', 'bbc'], ['bbb', 'bbb']));
    assert.equal(Favor.LHS, c.min(['bba', 'bbb'], ['bba', 'bbc']));
  }

  function checkMax(c: MultiKeyComparator): void {
    assert.equal(Favor.TIE, c.max([0, 'A'], [0, 'A']));
    assert.equal(Favor.TIE, c.max(['', ''], ['', '']));
    assert.equal(Favor.TIE, c.max([888.88, 888.88], [888.88, 888.88]));
    assert.equal(Favor.TIE, c.max(['ab', 'ab'], ['ab', 'ab']));

    assert.equal(Favor.LHS, c.max([1, 'A'], [0, 'Z']));
    assert.equal(Favor.RHS, c.max([0, 999], [1, 888]));
    assert.equal(Favor.LHS, c.max([1, 1], [1, 0]));
    assert.equal(Favor.RHS, c.max(['A', 'D'], ['A', 'Z']));
    assert.equal(Favor.LHS, c.max([888.88, 999], [888.87, 1]));
    assert.equal(Favor.RHS, c.max([888.87, 999], [888.88, 1]));
    assert.equal(Favor.LHS, c.max([888.88, 999], [888.88, 1]));
    assert.equal(Favor.RHS, c.max([1, 888.87], [1, 888.88]));

    assert.equal(Favor.LHS, c.max(['b', 1], ['a', 999]));
    assert.equal(Favor.RHS, c.max(['a', 999], ['b', 0]));
    assert.equal(Favor.LHS, c.max(['b', 'b'], ['b', 'a']));
    assert.equal(Favor.RHS, c.max(['a', 'a'], ['a', 'b']));
    assert.equal(Favor.LHS, c.max(['bbb', 'bba'], ['bba', 'bbb']));
    assert.equal(Favor.RHS, c.max(['bba', 'bbb'], ['bbb', 'bba']));
    assert.equal(Favor.LHS, c.max(['bbb', 'bbc'], ['bbb', 'bbb']));
    assert.equal(Favor.RHS, c.max(['bba', 'bbb'], ['bba', 'bbc']));
  }

  it('min', () => shuffleAndTest(checkMin));
  it('max', () => shuffleAndTest(checkMax));

  it('defaultOrder', () => {
    const orders = MultiKeyComparator.createOrders(2, Order.ASC);
    const comparator = new MultiKeyComparator(orders);
    const c = comparator.compare.bind(comparator);

    assert.equal(Favor.TIE, c([0, 0], [0, 0]));
    assert.equal(Favor.TIE, c(['', ''], ['', '']));
    assert.equal(Favor.TIE, c([99.99, 99.99], [99.99, 99.99]));
    assert.equal(Favor.TIE, c(['ab', 'ab'], ['ab', 'ab']));
    assert.equal(Favor.TIE, c([77, 'ab'], [77, 'ab']));

    assert.equal(Favor.LHS, c([7, 6], [5, 4]));
    assert.equal(Favor.LHS, c([7, 6], [7, 4]));
    assert.equal(Favor.RHS, c([5, 4], [7, 6]));
    assert.equal(Favor.RHS, c([5, 4], [5, 8]));

    assert.equal(Favor.LHS, c([9, 'abc'], [8, 'zzz']));
    assert.equal(Favor.LHS, c(['zzz', 8], ['abc', 12]));
    assert.equal(Favor.LHS, c(['zzz', 2], ['zzz', 1]));
    assert.equal(Favor.LHS, c([2, 'zzz'], [2, 'zza']));
    assert.equal(Favor.RHS, c(['zzz', 1], ['zzz', 2]));
    assert.equal(Favor.RHS, c([2, 'zza'], [2, 'zzz']));
  });

  it('customOrder', () => {
    const comparator = new MultiKeyComparator([Order.DESC, Order.ASC]);
    const c = comparator.compare.bind(comparator);

    assert.equal(Favor.TIE, c([0, 0], [0, 0]));
    assert.equal(Favor.TIE, c(['', ''], ['', '']));
    assert.equal(Favor.TIE, c([99.99, 99.99], [99.99, 99.99]));
    assert.equal(Favor.TIE, c(['ab', 'ab'], ['ab', 'ab']));
    assert.equal(Favor.TIE, c([77, 'ab'], [77, 'ab']));

    assert.equal(Favor.RHS, c([7, 6], [5, 4]));
    assert.equal(Favor.LHS, c([7, 6], [7, 4]));
    assert.equal(Favor.LHS, c([5, 4], [7, 6]));
    assert.equal(Favor.RHS, c([5, 4], [5, 8]));

    assert.equal(Favor.RHS, c([9, 'abc'], [8, 'zzz']));
    assert.equal(Favor.RHS, c(['zzz', 8], ['abc', 12]));
    assert.equal(Favor.LHS, c(['zzz', 2], ['zzz', 1]));
    assert.equal(Favor.LHS, c([2, 'zzz'], [2, 'zza']));
    assert.equal(Favor.RHS, c(['zzz', 1], ['zzz', 2]));
    assert.equal(Favor.RHS, c([2, 'zza'], [2, 'zzz']));
  });

  function checkIsInRange(c: MultiKeyComparator): void {
    const lowerBound = SingleKeyRange.lowerBound(2);
    const lowerBoundExclude = SingleKeyRange.lowerBound(2, true);
    const upperBound = SingleKeyRange.upperBound(2);
    const upperBoundExclude = SingleKeyRange.upperBound(2, true);

    assert.isTrue(
      c.isInRange([2, 2], [SingleKeyRange.all(), SingleKeyRange.all()])
    );
    assert.isTrue(c.isInRange([2, 2], [lowerBound, lowerBound]));
    assert.isFalse(c.isInRange([2, 2], [lowerBoundExclude, lowerBound]));
    assert.isFalse(c.isInRange([2, 2], [lowerBound, lowerBoundExclude]));
    assert.isFalse(c.isInRange([2, 2], [lowerBoundExclude, lowerBoundExclude]));
    assert.isTrue(c.isInRange([2, 2], [upperBound, upperBound]));
    assert.isFalse(c.isInRange([2, 2], [upperBoundExclude, upperBound]));
    assert.isFalse(c.isInRange([2, 2], [upperBound, upperBoundExclude]));
    assert.isFalse(c.isInRange([2, 2], [upperBoundExclude, upperBoundExclude]));
    assert.isTrue(c.isInRange([2, 2], [lowerBound, upperBound]));
    assert.isFalse(c.isInRange([2, 2], [lowerBoundExclude, upperBound]));
    assert.isFalse(c.isInRange([2, 2], [lowerBound, upperBoundExclude]));
    assert.isFalse(c.isInRange([2, 2], [lowerBoundExclude, upperBoundExclude]));
    assert.isTrue(c.isInRange([2, 2], [upperBound, lowerBound]));
    assert.isFalse(c.isInRange([2, 2], [upperBoundExclude, lowerBound]));
    assert.isFalse(c.isInRange([2, 2], [upperBound, lowerBoundExclude]));
    assert.isFalse(c.isInRange([2, 2], [upperBoundExclude, lowerBoundExclude]));
  }

  it('isInRange_MultiKeyComparator', () => {
    // The orders do not really affect the judgement for this test, therefore
    // two random orders are picked to make this test shorter.
    checkIsInRange(new MultiKeyComparator([Order.ASC, Order.DESC]));
  });

  it('isInRange_MultiKeyComparatorWithNull', () => {
    const c = new MultiKeyComparatorWithNull([Order.ASC, Order.DESC]);
    checkIsInRange(c);

    // Null specific tests
    const all = SingleKeyRange.all();
    const lowerBound = SingleKeyRange.lowerBound(2);
    const NULL: SingleKey = (null as unknown) as SingleKey;
    assert.isTrue(c.isInRange([2, NULL], [all, all]));
    assert.isTrue(c.isInRange([NULL, 2], [all, all]));
    assert.isTrue(c.isInRange([2, NULL], [lowerBound, all]));
    assert.isFalse(c.isInRange([2, NULL], [lowerBound, lowerBound]));
    assert.isTrue(c.isInRange([NULL, 2], [all, lowerBound]));

    assert.isTrue(c.isInRange([NULL, NULL], [all, all]));
    assert.isFalse(c.isInRange([NULL, NULL], [lowerBound, all]));
    assert.isFalse(c.isInRange([NULL, NULL], [all, lowerBound]));
    assert.isFalse(c.isInRange([NULL, NULL], [lowerBound, lowerBound]));
  });

  it('sortKeyRanges_ASC', () => {
    const c = new MultiKeyComparator([Order.ASC, Order.ASC]);
    const keyRanges = [
      [
        SingleKeyRange.upperBound(5, true),
        SingleKeyRange.upperBound('D', true),
      ],
      [
        SingleKeyRange.lowerBound(90, true),
        SingleKeyRange.lowerBound('X', true),
      ],
      [SingleKeyRange.only(90), SingleKeyRange.only('X')],
    ];

    const expectations = [
      '[unbound, 5),[unbound, D)',
      '[90, 90],[X, X]',
      '(90, unbound],(X, unbound]',
    ];

    const actual = c.sortKeyRanges(keyRanges).map(range => range.toString());
    assert.sameOrderedMembers(expectations, actual);
  });

  it('sortKeyRanges_MixedOrder', () => {
    const c = new MultiKeyComparator([Order.ASC, Order.DESC]);

    // NOT(BETWEEN([2, 'X'], [24, 'B'])) OR [22, 'D']
    const keyRanges = [
      [
        new SingleKeyRange(SingleKeyRange.UNBOUND_VALUE, 2, false, true),
        new SingleKeyRange(SingleKeyRange.UNBOUND_VALUE, 'B', false, true),
      ],
      [
        new SingleKeyRange(24, SingleKeyRange.UNBOUND_VALUE, true, false),
        new SingleKeyRange('X', SingleKeyRange.UNBOUND_VALUE, true, false),
      ],
      [SingleKeyRange.only(22), SingleKeyRange.only('D')],
    ];

    const expectations = [
      '[unbound, 2),(X, unbound]',
      '[22, 22],[D, D]',
      '(24, unbound],[unbound, B)',
    ];

    const actual = c.sortKeyRanges(keyRanges).map(range => range.toString());
    assert.sameOrderedMembers(expectations, actual);
  });

  it('compareRange', () => {
    const c = new MultiKeyComparator([Order.ASC, Order.DESC]);
    const lowerBound = SingleKeyRange.lowerBound(2);
    const lowerBoundExclude = SingleKeyRange.lowerBound(2, true);
    const upperBound = SingleKeyRange.upperBound(2);
    const upperBoundExclude = SingleKeyRange.upperBound(2, true);
    const all = SingleKeyRange.all();
    const only1 = SingleKeyRange.only(1);
    const only2 = SingleKeyRange.only(2);
    const only3 = SingleKeyRange.only(3);
    const key = [2, 2];

    // Shuffle of valid conditions shall result in covering both ends.
    const ranges = [all, only2, lowerBound, upperBound];
    ranges.forEach(r1 => {
      ranges.forEach(r2 => {
        assert.sameOrderedMembers([true, true], c.compareRange(key, [r1, r2]));
      });
    });

    assert.sameOrderedMembers(
      [true, false],
      c.compareRange(key, [only1, only3])
    );
    assert.sameOrderedMembers(
      [false, true],
      c.compareRange(key, [only3, only1])
    );
    assert.sameOrderedMembers(
      [false, false],
      c.compareRange(key, [only1, only1])
    );
    assert.sameOrderedMembers(
      [false, false],
      c.compareRange(key, [only3, only3])
    );

    assert.sameOrderedMembers(
      [false, true],
      c.compareRange(key, [lowerBoundExclude, lowerBound])
    );
    assert.sameOrderedMembers(
      [false, false],
      c.compareRange(key, [lowerBoundExclude, lowerBoundExclude])
    );
    assert.sameOrderedMembers(
      [false, true],
      c.compareRange(key, [lowerBoundExclude, upperBound])
    );
    assert.sameOrderedMembers(
      [false, true],
      c.compareRange(key, [lowerBoundExclude, upperBoundExclude])
    );

    assert.sameOrderedMembers(
      [true, false],
      c.compareRange(key, [upperBoundExclude, lowerBound])
    );
    assert.sameOrderedMembers(
      [true, false],
      c.compareRange(key, [upperBoundExclude, lowerBoundExclude])
    );
    assert.sameOrderedMembers(
      [true, false],
      c.compareRange(key, [upperBoundExclude, upperBound])
    );
    assert.sameOrderedMembers(
      [false, false],
      c.compareRange(key, [upperBoundExclude, upperBoundExclude])
    );
  });

  it('isFirstKeyInRange', () => {
    const c = new MultiKeyComparatorWithNull([Order.ASC, Order.DESC]);
    const all = SingleKeyRange.all();
    const only1 = SingleKeyRange.only(1);
    const only2 = SingleKeyRange.only(2);
    const NULL: SingleKey = (null as unknown) as SingleKey;

    assert.isTrue(c.isFirstKeyInRange([1, 2], [only1, only1]));
    assert.isTrue(c.isFirstKeyInRange([1, 2], [all, only1]));
    assert.isTrue(
      c.isFirstKeyInRange([1, 2], [only1, (null as unknown) as SingleKeyRange])
    );
    assert.isTrue(c.isFirstKeyInRange([NULL, 2], [all, only1]));
    assert.isFalse(c.isFirstKeyInRange([1, 2], [only2, all]));
    assert.isFalse(c.isFirstKeyInRange([NULL, 2], [only1, all]));
  });
});
