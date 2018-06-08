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
import {MathHelper} from '../../lib/structs/math_helper';

const assert = chai.assert;

describe('MathHelper', () => {
  it('longestCommonSequence', () => {
    const func = MathHelper.longestCommonSubsequence;

    assert.sameOrderedMembers([2], func([1, 2], [2, 1]));
    assert.sameOrderedMembers([1, 2], func([1, 2, 5], [2, 1, 2]));
    assert.sameOrderedMembers(
        [1, 2, 3, 4, 5],
        func([1, 0, 2, 3, 8, 4, 9, 5], [8, 1, 2, 4, 3, 6, 4, 5]));
    assert.sameOrderedMembers(
        [1, 1, 1, 1, 1], func([1, 1, 1, 1, 1], [1, 1, 1, 1, 1]));
    assert.sameOrderedMembers([5], func([1, 2, 3, 4, 5], [5, 4, 3, 2, 1]));
    assert.sameOrderedMembers(
        [1, 8, 11], func([1, 6, 8, 11, 13], [1, 3, 5, 8, 9, 11, 12]));
  });

  it('longestCommonSequence_CustomComparator', () => {
    const func = MathHelper.longestCommonSubsequence;

    interface A {
      field: string;
      field2?: string|number;
    }

    const compareFn = (a: A, b: A) => {
      return a.field === b.field;
    };

    const a1: A = {field: 'a1', field2: 'hello'};
    const a2: A = {field: 'a2', field2: 33};
    const a3: A = {field: 'a3'};
    const a4: A = {field: 'a3'};

    assert.sameOrderedMembers(
        [a1, a2], func([a1, a2, a3], [a3, a1, a2], compareFn));
    assert.sameOrderedMembers([a1, a3], func([a1, a3], [a1, a4], compareFn));
    // testing the same arrays without compare function
    assert.sameOrderedMembers([a1], func([a1, a3], [a1, a4]));
  });

  it('longestCommonSequence_CustomCollector', () => {
    const func = MathHelper.longestCommonSubsequence;

    const collectorFn = (a: number, b: number) => b;

    // The expect values are indices from the second array.
    assert.sameOrderedMembers(
        [1, 2, 4, 6, 7],
        func(
            [1, 0, 2, 3, 8, 4, 9, 5], [8, 1, 2, 4, 3, 6, 4, 5], undefined,
            collectorFn));
  });

  it('sum', () => {
    assert.equal(
        0, MathHelper.sum(), 'sum() must return 0 if there are no arguments');
    assert.equal(
        17, MathHelper.sum(17),
        'sum() must return its argument if there is only one');
    assert.equal(
        10, MathHelper.sum(1, 2, 3, 4), 'sum() must handle positive integers');
    assert.equal(
        -2.5, MathHelper.sum(1, -2, 3, -4.5), 'sum() must handle real numbers');
    assert.isTrue(
        isNaN(MathHelper.sum(1, 2, 'foo' as any as number, 3)),
        'sum() must return NaN if one of the arguments isn\'t numeric');
  });

  it('average', () => {
    assert.isTrue(
        isNaN(MathHelper.average()),
        'average() must return NaN if there are no arguments');
    assert.equal(
        17, MathHelper.average(17),
        'average() must return its argument if there is only one');
    assert.equal(
        3, MathHelper.average(1, 2, 3, 4, 5),
        'average() must handle positive integers');
    assert.equal(
        -0.625, MathHelper.average(1, -2, 3, -4.5),
        'average() must handle real numbers');
    assert.isTrue(
        isNaN(MathHelper.average(1, 2, 'foo' as any as number, 3)),
        'average() must return NaN if one of the arguments isn\'t numeric');
  });

  it('standardDeviation', () => {
    assert.equal(
        0, MathHelper.standardDeviation(),
        'standardDeviation() must return 0 if there are no samples');
    assert.equal(
        0, MathHelper.standardDeviation(17),
        'standardDeviation() must return 0 if there is only one sample');
    assert.approximately(
        6.9282, MathHelper.standardDeviation(3, 7, 7, 19), 0.0001,
        'standardDeviation() must handle positive integers');
    assert.approximately(
        3.4660, MathHelper.standardDeviation(1.23, -2.34, 3.14, -4.56), 0.0001,
        'standardDeviation() must handle real numbers');
  });
});
