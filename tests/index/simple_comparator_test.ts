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
import {Order} from '../../lib/base/enum';
import {Favor} from '../../lib/index/comparator';
import {SingleKey, SingleKeyRange} from '../../lib/index/key_range';
import {SimpleComparator} from '../../lib/index/simple_comparator';
import {SimpleComparatorWithNull} from '../../lib/index/simple_comparator_with_null';

const assert = chai.assert;

describe('SimpleComparator', () => {
  const NULL: SingleKey = null as any as SingleKey;

  function checkOrderAsc(comparator: SimpleComparator): void {
    const c = comparator.compare.bind(comparator);

    assert.equal(Favor.TIE, c(0, 0));
    assert.equal(Favor.TIE, c('', ''));
    assert.equal(Favor.TIE, c(888.88, 888.88));
    assert.equal(Favor.TIE, c('ab', 'ab'));

    assert.equal(Favor.LHS, c(1, 0));
    assert.equal(Favor.RHS, c(0, 1));
    assert.equal(Favor.LHS, c(888.88, 888.87));
    assert.equal(Favor.RHS, c(888.87, 888.88));

    assert.equal(Favor.LHS, c('b', 'a'));
    assert.equal(Favor.RHS, c('a', 'b'));
    assert.equal(Favor.LHS, c('bbb', 'bba'));
    assert.equal(Favor.RHS, c('bba', 'bbb'));
  }

  it('OrderAsc', () => {
    const c1 = new SimpleComparator(Order.ASC);
    checkOrderAsc(c1);
    const c2 = new SimpleComparatorWithNull(Order.ASC);
    checkOrderAsc(c2);

    // Null-specific tests
    const c = c2.compare.bind(c2);
    assert.equal(Favor.TIE, c(null, null));
    assert.equal(Favor.LHS, c(0, null));
    assert.equal(Favor.RHS, c(null, 0));
  });

  function checkOrderDesc(comparator: SimpleComparator): void {
    const c = comparator.compare.bind(comparator);

    assert.equal(Favor.TIE, c(0, 0));
    assert.equal(Favor.TIE, c('', ''));
    assert.equal(Favor.TIE, c(888.88, 888.88));
    assert.equal(Favor.TIE, c('ab', 'ab'));

    assert.equal(Favor.RHS, c(1, 0));
    assert.equal(Favor.LHS, c(0, 1));
    assert.equal(Favor.RHS, c(888.88, 888.87));
    assert.equal(Favor.LHS, c(888.87, 888.88));

    assert.equal(Favor.RHS, c('b', 'a'));
    assert.equal(Favor.LHS, c('a', 'b'));
    assert.equal(Favor.RHS, c('bbb', 'bba'));
    assert.equal(Favor.LHS, c('bba', 'bbb'));
  }

  it('OrderDesc', () => {
    const c1 = new SimpleComparator(Order.DESC);
    checkOrderDesc(c1);
    const c2 = new SimpleComparatorWithNull(Order.DESC);
    checkOrderDesc(c2);

    // Null-specific tests
    const c = c2.compare.bind(c2);
    assert.equal(Favor.TIE, c(null, null));
    assert.equal(Favor.RHS, c(0, null));
    assert.equal(Favor.LHS, c(null, 0));
  });

  function checkMin(c: SimpleComparator): void {
    assert.equal(Favor.TIE, c.min(0, 0));
    assert.equal(Favor.TIE, c.min('', ''));
    assert.equal(Favor.TIE, c.min(888.88, 888.88));
    assert.equal(Favor.TIE, c.min('ab', 'ab'));

    assert.equal(Favor.RHS, c.min(1, 0));
    assert.equal(Favor.LHS, c.min(0, 1));
    assert.equal(Favor.RHS, c.min(888.88, 888.87));
    assert.equal(Favor.LHS, c.min(888.87, 888.88));

    assert.equal(Favor.RHS, c.min('b', 'a'));
    assert.equal(Favor.LHS, c.min('a', 'b'));
    assert.equal(Favor.RHS, c.min('bbb', 'bba'));
    assert.equal(Favor.LHS, c.min('bba', 'bbb'));
  }

  function checkMinWithNull(c: SimpleComparator): void {
    assert.equal(Favor.RHS, c.min(NULL, 1));
    assert.equal(Favor.LHS, c.min(1, NULL));
    assert.equal(Favor.TIE, c.min(NULL, NULL));
  }

  it('min', () => {
    const c1 = new SimpleComparator(Order.DESC);
    checkMin(c1);

    // Ensuring that Comparator#min() is not be affected by the order.
    const c2 = new SimpleComparator(Order.ASC);
    checkMin(c2);

    const c3 = new SimpleComparatorWithNull(Order.DESC);
    checkMin(c3);
    checkMinWithNull(c3);

    const c4 = new SimpleComparatorWithNull(Order.ASC);
    checkMin(c4);
    checkMinWithNull(c4);
  });

  function checkMax(c: SimpleComparator): void {
    assert.equal(Favor.TIE, c.max(0, 0));
    assert.equal(Favor.TIE, c.max('', ''));
    assert.equal(Favor.TIE, c.max(888.88, 888.88));
    assert.equal(Favor.TIE, c.max('ab', 'ab'));

    assert.equal(Favor.LHS, c.max(1, 0));
    assert.equal(Favor.RHS, c.max(0, 1));
    assert.equal(Favor.LHS, c.max(888.88, 888.87));
    assert.equal(Favor.RHS, c.max(888.87, 888.88));

    assert.equal(Favor.LHS, c.max('b', 'a'));
    assert.equal(Favor.RHS, c.max('a', 'b'));
    assert.equal(Favor.LHS, c.max('bbb', 'bba'));
    assert.equal(Favor.RHS, c.max('bba', 'bbb'));
  }

  function checkMaxWithNull(c: SimpleComparator): void {
    assert.equal(Favor.RHS, c.max(NULL, 1));
    assert.equal(Favor.LHS, c.max(1, NULL));
    assert.equal(Favor.TIE, c.max(NULL, NULL));
  }

  it('max', () => {
    const c1 = new SimpleComparator(Order.DESC);
    checkMax(c1);

    // Ensuring that Comparator#min() is not be affected by the order.
    const c2 = new SimpleComparator(Order.ASC);
    checkMax(c2);

    const c3 = new SimpleComparatorWithNull(Order.DESC);
    checkMax(c3);
    checkMaxWithNull(c3);

    const c4 = new SimpleComparatorWithNull(Order.ASC);
    checkMax(c4);
    checkMaxWithNull(c4);
  });

  it('OrderRange', () => {
    const ranges = [
      new SingleKeyRange(1, 5, false, true),
      new SingleKeyRange(6, 7, false, false),
      SingleKeyRange.only(5),
      new SingleKeyRange(-1, 1, false, true),
    ];

    const expectationAsc = [
      '[-1, 1)',
      '[1, 5)',
      '[5, 5]',
      '[6, 7]',
    ];

    let dupe = ranges.slice();
    let actual = dupe.sort(SimpleComparator.orderRangeAscending)
                     .map((range) => range.toString());
    assert.sameOrderedMembers(expectationAsc, actual);

    const expectationDesc = [
      '[6, 7]',
      '[5, 5]',
      '[1, 5)',
      '[-1, 1)',
    ];

    dupe = ranges.slice();
    actual = dupe.sort(SimpleComparator.orderRangeDescending)
                 .map((range) => range.toString());
    assert.sameOrderedMembers(expectationDesc, actual);
  });

  function checkIsInRange(c: SimpleComparator): void {
    assert.isTrue(c.isInRange(2, SingleKeyRange.all()));
    assert.isTrue(c.isInRange(2, SingleKeyRange.lowerBound(2)));
    assert.isFalse(c.isInRange(2, SingleKeyRange.lowerBound(2, true)));
    assert.isTrue(c.isInRange(2, SingleKeyRange.upperBound(2)));
    assert.isFalse(c.isInRange(2, SingleKeyRange.upperBound(2, true)));
    assert.isTrue(c.isInRange(2, SingleKeyRange.only(2)));
    assert.isFalse(c.isInRange(2, SingleKeyRange.only(3)));
  }

  it('isInRange', () => {
    const c1 = new SimpleComparator(Order.ASC);
    checkIsInRange(c1);

    const c2 = new SimpleComparatorWithNull(Order.DESC);
    checkIsInRange(c2);

    // Null specific
    assert.isTrue(c2.isInRange(NULL, SingleKeyRange.all()));
    const ranges = [
      SingleKeyRange.lowerBound(2),
      SingleKeyRange.lowerBound(2, true),
      SingleKeyRange.upperBound(2),
      SingleKeyRange.upperBound(2, true),
      SingleKeyRange.only(2),
    ];
    ranges.forEach((range) => {
      assert.isFalse(c2.isInRange(NULL, range));
    });
  });

  it('sortKeyRanges', () => {
    const c1 = new SimpleComparator(Order.ASC);
    const c2 = new SimpleComparator(Order.DESC);

    const keyRanges = [
      SingleKeyRange.lowerBound(5, true),
      SingleKeyRange.upperBound(5, true),
      SingleKeyRange.only(5),
    ];

    const expectations1 = [
      '[unbound, 5)',
      '[5, 5]',
      '(5, unbound]',
    ];

    const expectations2 = [
      '(5, unbound]',
      '[5, 5]',
      '[unbound, 5)',
    ];

    const getActual = (c: SimpleComparator, ranges: SingleKeyRange[]) => {
      return c.sortKeyRanges(ranges).map((range) => range.toString());
    };

    assert.sameOrderedMembers(expectations1, getActual(c1, keyRanges));
    assert.sameOrderedMembers(expectations2, getActual(c2, keyRanges));
  });

  it('compareRange', () => {
    const c = new SimpleComparator(Order.ASC);
    assert.sameOrderedMembers(
        [true, true], c.compareRange(2, SingleKeyRange.all()));
    assert.sameOrderedMembers(
        [true, true], c.compareRange(2, SingleKeyRange.only(2)));
    assert.sameOrderedMembers(
        [false, true], c.compareRange(2, SingleKeyRange.only(3)));
    assert.sameOrderedMembers(
        [true, false], c.compareRange(2, SingleKeyRange.only(1)));
    assert.sameOrderedMembers(
        [true, true], c.compareRange(2, SingleKeyRange.lowerBound(2)));
    assert.sameOrderedMembers(
        [false, true], c.compareRange(2, SingleKeyRange.lowerBound(2, true)));
    assert.sameOrderedMembers(
        [true, true], c.compareRange(2, SingleKeyRange.upperBound(2)));
    assert.sameOrderedMembers(
        [true, false], c.compareRange(2, SingleKeyRange.upperBound(2, true)));
  });
});
