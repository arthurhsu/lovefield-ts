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

export class TestMultiRowNumericalKey extends TestIndex {
  // Values that are added in the index in populateIndex().
  private allValues = [
    20,
    30,
    21,
    31,
    22,
    32,
    23,
    33,
    24,
    34,
    25,
    35,
    26,
    36,
    27,
    37,
    28,
    38,
    29,
    39,
  ];

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

  private expectations = [
    // get all.
    this.allValues,
    this.allValues,
    // get one key
    [25, 35],
    // lower bound.
    [25, 35, 26, 36, 27, 37, 28, 38, 29, 39],
    [26, 36, 27, 37, 28, 38, 29, 39],
    // upper bound.
    [20, 30, 21, 31, 22, 32, 23, 33, 24, 34, 25, 35],
    [20, 30, 21, 31, 22, 32, 23, 33, 24, 34],
    // both lower and upper bound.
    [22, 32, 23, 33, 24, 34, 25, 35],
    [23, 33, 24, 34, 25, 35],
    [22, 32, 23, 33, 24, 34],
    [23, 33, 24, 34],
  ];

  // Holds the max key and the corresponding values, populated in populateIndex.
  private maxKeyValuePair: [SingleKey, number[]] | null;

  // Holds the min key and the corresponding values, populated in populateIndex.
  private minKeyValuePair: [SingleKey, number[]] | null;

  // |reverse| means range expectations shall be reversed or not.
  constructor(constructorFn: () => RuntimeIndex) {
    super(constructorFn);
    this.maxKeyValuePair = null;
    this.minKeyValuePair = null;
  }

  testAddGet(index: RuntimeIndex): void {
    for (let i = 0; i < 10; ++i) {
      const key = 10 + i;
      const value1 = 20 + i;
      const value2 = 30 + i;
      index.add(key, value1);
      index.add(key, value2);
      assert.equal(value1, index.get(key)[0]);
      assert.equal(value2, index.get(key)[1]);
    }
  }

  testGetRangeCost(index: RuntimeIndex): void {
    this.populateIndex(index);

    this.keyRanges.forEach((keyRange, counter) => {
      const expectedResult = this.expectations[counter];
      this.assertGetRangeCost(index, keyRange, expectedResult);
    }, this);
  }

  testRemove(index: RuntimeIndex): void {
    this.populateIndex(index);

    index.remove(11, 21);
    assert.sameDeepOrderedMembers([31], index.get(11));
    index.remove(12, 22);
    index.remove(12, 32);
    assert.sameDeepOrderedMembers([], index.get(12));
    assert.sameDeepOrderedMembers(
      [],
      index.getRange([SingleKeyRange.only(12)])
    );
  }

  testSet(index: RuntimeIndex): void {
    this.populateIndex(index);
    index.remove(12, 22);

    for (let i = 0; i < 10; ++i) {
      const key = 10 + i;
      const value = 40 + i;
      index.set(key, value);
      const actualValues = index.get(key);
      assert.equal(1, actualValues.length);
      assert.equal(value, actualValues[0]);
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
    // TODO(arthurhsu): implement, original code does not have it.
  }

  private populateIndex(index: RuntimeIndex): void {
    for (let i = 0; i < 10; ++i) {
      const key = 10 + i;
      const value1 = 20 + i;
      const value2 = 30 + i;
      index.add(key, value1);
      index.add(key, value2);

      // Detecting min key and corresponding value to be used later in
      // assertions.
      if (this.minKeyValuePair === null || key < this.minKeyValuePair[0]) {
        this.minKeyValuePair = [key, [value1, value2]];
      }

      // Detecting max key and corresponding value to be used later in
      // assertions.
      if (this.maxKeyValuePair === null || key > this.maxKeyValuePair[0]) {
        this.maxKeyValuePair = [key, [value1, value2]];
      }
    }
  }
}
