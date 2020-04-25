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
import {SingleKey, SingleKeyRange} from '../../lib/index/key_range';
import {RuntimeIndex} from '../../lib/index/runtime_index';
import {TestIndex} from './test_index';

const assert = chai.assert;

export class TestSingleRowNumericalKey extends TestIndex {
  // The key ranges used for testing.
  private keyRanges = [
    // get all.
    undefined,
    SingleKeyRange.all(),
    // get one key
    SingleKeyRange.only(15),
    // lower bound.
    SingleKeyRange.lowerBound(15),
    SingleKeyRange.lowerBound(15, true),
    // upper bound.
    SingleKeyRange.upperBound(15),
    SingleKeyRange.upperBound(15, true),
    // both lower and upper bound.
    new SingleKeyRange(12, 15, false, false),
    new SingleKeyRange(12, 15, true, false),
    new SingleKeyRange(12, 15, false, true),
    new SingleKeyRange(12, 15, true, true),
  ];

  // The corresponding expected results for key ranges.
  private expectations = [
    // get all.
    [20, 21, 22, 23, 24, 25, 26, 27, 28, 29],
    [20, 21, 22, 23, 24, 25, 26, 27, 28, 29],
    // get one key
    [25],
    // lower bound.
    [25, 26, 27, 28, 29],
    [26, 27, 28, 29],
    // upper bound.
    [20, 21, 22, 23, 24, 25],
    [20, 21, 22, 23, 24],
    // both lower and upper bound.
    [22, 23, 24, 25],
    [23, 24, 25],
    [22, 23, 24],
    [23, 24],
  ];

  // Holds the max key and the corresponding values, populated in populateIndex.
  private maxKeyValuePair: [SingleKey, number[]] | null;

  // Holds the min key and the corresponding values, populated in populateIndex.
  private minKeyValuePair: [SingleKey, number[]] | null;

  // |reverse| means range expectations shall be reversed or not.
  constructor(constructorFn: () => RuntimeIndex, readonly reverse = false) {
    super(constructorFn);
    this.maxKeyValuePair = null;
    this.minKeyValuePair = null;
  }

  testAddGet(index: RuntimeIndex): void {
    for (let i = 0; i < 10; ++i) {
      const key = 10 + i;
      const value = 20 + i;
      index.add(key, value);
      const actualValue = index.get(key)[0];
      assert.equal(value, actualValue);
    }
  }

  testGetRangeCost(index: RuntimeIndex): void {
    this.populateIndex(index);
    this.keyRanges.forEach((keyRange, counter) => {
      const expectedResult = this.expectations[counter];
      if (this.reverse) {
        expectedResult.reverse();
      }
      this.assertGetRangeCost(index, keyRange, expectedResult);
    }, this);
  }

  testRemove(index: RuntimeIndex): void {
    this.populateIndex(index);

    index.remove(12, 22);
    assert.sameOrderedMembers([], index.get(12));

    const keyRange = SingleKeyRange.only(12);
    assert.sameOrderedMembers([], index.getRange([keyRange]));
    assert.equal(0, index.cost(keyRange));
  }

  testSet(index: RuntimeIndex): void {
    this.populateIndex(index);
    index.remove(12, 22);
    assert.equal(9, index.getRange().length);

    for (let i = 0; i < 10; ++i) {
      const key = 10 + i;
      const value = 30 + i;
      index.set(key, value);
      const actualValue = index.get(key)[0];
      assert.equal(value, actualValue);
    }

    assert.equal(10, index.getRange().length);
  }

  testMinMax(index: RuntimeIndex): void {
    // First try an empty index.
    assert.isNull(index.min());
    assert.isNull(index.max());

    this.populateIndex(index);
    assert.sameDeepOrderedMembers(
      this.minKeyValuePair as unknown[],
      index.min() as unknown[]
    );
    assert.sameDeepOrderedMembers(
      this.maxKeyValuePair as unknown[],
      index.max() as unknown[]
    );
  }

  testMultiRange(index: RuntimeIndex): void {
    for (let i = 0; i < 20; ++i) {
      index.set(i, i);
    }

    // Simulate NOT(BETWEEN(2, 18))
    const range1 = new SingleKeyRange(
      SingleKeyRange.UNBOUND_VALUE,
      2,
      false,
      true
    );
    const range2 = new SingleKeyRange(
      18,
      SingleKeyRange.UNBOUND_VALUE,
      true,
      false
    );

    const comparator = index.comparator();
    const expected = [0, 1, 19].sort(comparator.compare.bind(comparator));
    const expectedReverse = expected.slice().reverse();

    assert.sameOrderedMembers(expected, index.getRange([range1, range2]));
    assert.sameOrderedMembers(expected, index.getRange([range2, range1]));
    assert.sameOrderedMembers(
      expectedReverse,
      index.getRange([range1, range2], true)
    );
    assert.sameOrderedMembers(
      expectedReverse,
      index.getRange([range2, range1], true)
    );
  }

  private populateIndex(index: RuntimeIndex): void {
    for (let i = 0; i < 10; ++i) {
      const key = 10 + i;
      const value = key + 10;
      index.add(key, value);

      // Detecting min key and corresponding value to be used in assertions.
      if (this.minKeyValuePair === null || key < this.minKeyValuePair[0]) {
        this.minKeyValuePair = [key, [value]];
      }

      // Detecting max key and corresponding value to be used in assertions.
      if (this.maxKeyValuePair === null || key > this.maxKeyValuePair[0]) {
        this.maxKeyValuePair = [key, [value]];
      }
    }
  }
}
