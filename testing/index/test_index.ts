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
import {KeyRange, SingleKeyRange} from '../../lib/index/key_range';
import {RuntimeIndex} from '../../lib/index/runtime_index';

const assert = chai.assert;

export abstract class TestIndex {
  // Asserts that the return values of getRange() and cost() are as expected for
  // the given index, for the given key range.
  public static assertGetRangeCost(
      index: RuntimeIndex, keyRange: KeyRange|SingleKeyRange|undefined,
      expectedResult: number[]): void {
    const actualResult = index.getRange(
        keyRange !== undefined ? [keyRange] as SingleKeyRange[] | KeyRange[] :
                                 undefined);
    assert.sameDeepOrderedMembers(expectedResult, actualResult);
    assert.equal(actualResult.length, index.cost(keyRange));
  }

  protected constructorFn: () => RuntimeIndex;

  constructor(constructorFn: () => RuntimeIndex) {
    this.constructorFn = constructorFn;
  }

  public abstract testAddGet(index: RuntimeIndex): void;
  public abstract testGetRangeCost(index: RuntimeIndex): void;
  public abstract testRemove(index: RuntimeIndex): void;
  public abstract testSet(index: RuntimeIndex): void;
  public abstract testMinMax(index: RuntimeIndex): void;
  public abstract testMultiRange(index: RuntimeIndex): void;

  public run(): void {
    const testCases = [
      this.testAddGet,
      this.testGetRangeCost,
      this.testMinMax,
      this.testRemove,
      this.testSet,
      this.testMultiRange,
    ];

    testCases.forEach((tc) => {
      const index = this.constructorFn();
      tc.call(this, index);
    }, this);
  }
}
