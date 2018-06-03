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
import {KeyRange} from '../../lib/index/key_range';
import {RuntimeIndex} from '../../lib/index/runtime_index';

const assert = chai.assert;

export class TestIndex {
  public static constructorFn: () => RuntimeIndex;

  // Asserts that the return values of getRange() and cost() are as expected for
  // the given index, for the given key range.
  public static assertGetRangeCost(
      index: RuntimeIndex, keyRange: KeyRange|undefined,
      expectedResult: number[]): void {
    const actualResult =
        index.getRange(keyRange !== undefined ? [keyRange] : undefined);
    assert.sameOrderedMembers(expectedResult, actualResult);
    assert.equal(actualResult.length, index.cost(keyRange));
  }
}
