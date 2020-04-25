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
import {isSubset, setEquals} from '../../lib/structs/set_util';

const assert = chai.assert;

describe('SetUtil', () => {
  it('isSubSet', () => {
    function checker<T>(
      set1: Set<T>,
      set2: Set<T>,
      set3: Set<T>,
      set4: Set<T>
    ): void {
      assert.isTrue(isSubset(set1, set2));
      assert.isTrue(isSubset(set1, set1));
      assert.isFalse(isSubset(set2, set1));
      assert.isFalse(isSubset(set1, set3));
      assert.isFalse(isSubset(set3, set2));
      assert.isTrue(isSubset(set1, set4));
      assert.isFalse(isSubset(set4, set1));
    }

    const s1 = new Set<number>([1, 2, 3]);
    const s2 = new Set<number>([2, 3]);
    const s3 = new Set<number>([1, 4]);
    const s4 = new Set<number>();
    checker(s1, s2, s3, s4);

    const s6 = new Set<string>(['A1', 'B2', 'C3']);
    const s7 = new Set<string>(['B2', 'C3']);
    const s8 = new Set<string>(['A1', 'D4']);
    const s9 = new Set<string>();
    checker(s6, s7, s8, s9);
  });

  it('setEquals', () => {
    function checker<T>(
      set1: Set<T>,
      set2: Set<T>,
      set3: Set<T>,
      set4: Set<T>,
      set5: Set<T>
    ): void {
      assert.isTrue(setEquals(set1, set1));
      assert.isTrue(setEquals(set4, set4));
      assert.isFalse(setEquals(set1, set2));
      assert.isFalse(setEquals(set2, set3));
      assert.isFalse(setEquals(set1, set4));
      assert.isTrue(setEquals(set1, set5));
      assert.isTrue(setEquals(set5, set1));
    }

    const s1 = new Set<number>([1, 2, 3]);
    const s2 = new Set<number>([2, 3]);
    const s3 = new Set<number>([1, 4]);
    const s4 = new Set<number>();
    const s5 = new Set<number>([1, 2, 3]);
    checker(s1, s2, s3, s4, s5);

    const s6 = new Set<string>(['A1', 'B2', 'C3']);
    const s7 = new Set<string>(['B2', 'C3']);
    const s8 = new Set<string>(['A1', 'D4']);
    const s9 = new Set<string>();
    const s10 = new Set<string>(['A1', 'B2', 'C3']);
    checker(s6, s7, s8, s9, s10);
  });
});
