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
import {ArrayHelper} from '../../lib/structs/array_helper';

const assert = chai.assert;

describe('ArrayHelper', () => {
  it('binaryInsertRemove_DefaultComparator', () => {
    function makeChecker(
        array: number[], fn: (a: number[], v: number) => boolean):
        ((value: number, expectedRes: boolean, expectedArr: number[]) => void) {
      return (value: number, expectRes: boolean, expectArr: number[]): void => {
        const result = fn(array, value);
        assert.equal(expectRes, result);
        assert.sameOrderedMembers(expectArr, array);
      };
    }

    const a: number[] = [];
    let check = makeChecker(a, ArrayHelper.binaryInsert);
    check(3, true, [3]);
    check(3, false, [3]);
    check(1, true, [1, 3]);
    check(5, true, [1, 3, 5]);
    check(2, true, [1, 2, 3, 5]);
    check(2, false, [1, 2, 3, 5]);

    check = makeChecker(a, ArrayHelper.binaryRemove);
    check(0, false, [1, 2, 3, 5]);
    check(3, true, [1, 2, 5]);
    check(1, true, [2, 5]);
    check(5, true, [2]);
    check(2, true, []);
    check(2, false, []);
  });

  it('binaryInsertRemove_CustomComparator', () => {
    class Class {
      public id: number;
      constructor(id: number) {
        this.id = id;
      }
    }

    function comparator(lhs: Class, rhs: Class): number {
      return lhs.id - rhs.id;
    }

    type comp = (l: Class, r: Class) => number;
    function makeChecker(
        array: Class[], fn: (a: Class[], v: Class, c: comp) => boolean):
        ((value: Class, expectedRes: boolean, expectedArr: Class[]) => void) {
      return (value: Class, expectRes: boolean, expectArr: Class[]): void => {
        const result = fn(array, value, comparator);
        assert.equal(expectRes, result);
        assert.sameOrderedMembers(expectArr, array);
      };
    }

    const a: Class[] = [];
    const obj0 = new Class(0);
    const obj1 = new Class(1);
    const obj2 = new Class(2);
    const obj3 = new Class(3);
    const obj5 = new Class(5);

    let check = makeChecker(a, ArrayHelper.binaryInsert);
    check(obj3, true, [obj3]);
    check(obj3, false, [obj3]);
    check(obj1, true, [obj1, obj3]);
    check(obj5, true, [obj1, obj3, obj5]);
    check(obj2, true, [obj1, obj2, obj3, obj5]);
    check(obj2, false, [obj1, obj2, obj3, obj5]);

    check = makeChecker(a, ArrayHelper.binaryRemove);
    check(obj0, false, [obj1, obj2, obj3, obj5]);
    check(obj3, true, [obj1, obj2, obj5]);
    check(obj1, true, [obj2, obj5]);
    check(obj5, true, [obj2]);
    check(obj2, true, []);
    check(obj2, false, []);
  });

  it('shuffle', () => {
    const testArray = [1, 2, 3, 4, 5];
    const testArrayClone: number[] =
        JSON.parse(JSON.stringify(testArray)) as number[];

    ArrayHelper.shuffle(testArray);
    assert.sameMembers(testArrayClone, testArray);
    assert.throw(() => {
      assert.sameOrderedMembers(testArray, testArrayClone);
    });
  });
});
