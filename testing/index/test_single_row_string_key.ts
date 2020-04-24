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
import { SingleKey, SingleKeyRange } from '../../lib/index/key_range';
import { RuntimeIndex } from '../../lib/index/runtime_index';
import { TestIndex } from './test_index';

const assert = chai.assert;

export class TestSingleRowStringKey extends TestIndex {
  // The key ranges used for testing.
  private keyRanges = [
    // get all.
    undefined,
    SingleKeyRange.all(),
    // get one key
    SingleKeyRange.only('key-3'),
    // lower bound.
    SingleKeyRange.lowerBound('key0'),
    SingleKeyRange.lowerBound('key0', true),
    // upper bound.
    SingleKeyRange.upperBound('key0'),
    SingleKeyRange.upperBound('key0', true),
    // both lower and upper bound.
    new SingleKeyRange('key-1', 'key-5', false, false),
    new SingleKeyRange('key-1', 'key-5', true, false),
    new SingleKeyRange('key-1', 'key-5', false, true),
    new SingleKeyRange('key-1', 'key-5', true, true),
  ];

  // The corresponding expected results for key ranges.
  private expectations = [
    // get all.
    [-1, -2, -3, -4, -5, 0, 1, 2, 3, 4],
    [-1, -2, -3, -4, -5, 0, 1, 2, 3, 4],
    // get one key
    [-3],
    // lower bound.
    [0, 1, 2, 3, 4],
    [1, 2, 3, 4],
    // upper bound.
    [-1, -2, -3, -4, -5, 0],
    [-1, -2, -3, -4, -5],
    // both lower and upper bound.
    [-1, -2, -3, -4, -5],
    [-2, -3, -4, -5],
    [-1, -2, -3, -4],
    [-2, -3, -4],
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
    for (let i = -5; i < 5; ++i) {
      const key = 'key' + i.toString();
      const value = i;
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
    const key = 'key-1';
    assert.isTrue(index.get(key).length > 0);

    index.remove(key);
    assert.sameOrderedMembers([], index.get(key));

    const keyRange = SingleKeyRange.only(key);
    assert.sameOrderedMembers([], index.getRange([keyRange]));
    assert.equal(0, index.cost(keyRange));
  }

  testSet(index: RuntimeIndex): void {
    this.populateIndex(index);
    index.remove('key-1');
    assert.equal(9, index.getRange().length);

    for (let i = -5; i < 5; ++i) {
      const key = 'key' + i.toString();
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
    for (let i = 0; i < 10; ++i) {
      index.set('k' + i, i);
    }

    // Simulate NOT(BETWEEN('k2', 'k8'))
    const range1 = new SingleKeyRange(
      SingleKeyRange.UNBOUND_VALUE,
      'k2',
      false,
      true
    );
    const range2 = new SingleKeyRange(
      'k8',
      SingleKeyRange.UNBOUND_VALUE,
      true,
      false
    );

    const comparator = index.comparator();
    const expected = [0, 1, 9].sort(comparator.compare.bind(comparator));
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
    for (let i = -5; i < 5; ++i) {
      const key = 'key' + i.toString();
      const value = i;
      index.add(key, value);

      // Detecting min key and corresponding value to be used later in
      // assertions.
      if (this.minKeyValuePair === null || key < this.minKeyValuePair[0]) {
        this.minKeyValuePair = [key, [value]];
      }

      // Detecting max key and corresponding value to be used later in
      // assertions.
      if (this.maxKeyValuePair === null || key > this.maxKeyValuePair[0]) {
        this.maxKeyValuePair = [key, [value]];
      }
    }
  }
}
