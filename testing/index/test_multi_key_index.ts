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
import { SingleKeyRange } from '../../lib/index/key_range';
import { RuntimeIndex } from '../../lib/index/runtime_index';
import { TestIndex } from './test_index';

const assert = chai.assert;

export class TestMultiKeyIndex extends TestIndex {
  private keyRanges = [
    // Get all.
    undefined,
    [SingleKeyRange.all(), SingleKeyRange.all()],

    // Get one key.
    [SingleKeyRange.only(8), SingleKeyRange.only('R')],

    // Lower bound.
    [SingleKeyRange.lowerBound(8), SingleKeyRange.upperBound('R')],
    [SingleKeyRange.lowerBound(8, true), SingleKeyRange.upperBound('R')],
    [SingleKeyRange.lowerBound(8), SingleKeyRange.upperBound('R', true)],
    [SingleKeyRange.lowerBound(8, true), SingleKeyRange.upperBound('R', true)],

    // Upper bound.
    [SingleKeyRange.upperBound(2), SingleKeyRange.lowerBound('X')],
    [SingleKeyRange.upperBound(2, true), SingleKeyRange.lowerBound('X')],
    [SingleKeyRange.upperBound(2), SingleKeyRange.lowerBound('X', true)],
    [SingleKeyRange.upperBound(2, true), SingleKeyRange.lowerBound('X', true)],

    // Between.
    [
      new SingleKeyRange(2, 8, false, false),
      new SingleKeyRange('R', 'X', false, false),
    ],
    [
      new SingleKeyRange(2, 8, false, true),
      new SingleKeyRange('R', 'X', false, false),
    ],
    [
      new SingleKeyRange(2, 8, false, false),
      new SingleKeyRange('R', 'X', true, false),
    ],
    [
      new SingleKeyRange(2, 8, false, true),
      new SingleKeyRange('R', 'X', true, false),
    ],
    [
      new SingleKeyRange(2, 8, true, false),
      new SingleKeyRange('R', 'X', false, false),
    ],
    [
      new SingleKeyRange(2, 8, false, false),
      new SingleKeyRange('R', 'X', false, true),
    ],
    [
      new SingleKeyRange(2, 8, true, false),
      new SingleKeyRange('R', 'X', false, true),
    ],
  ];

  private expectations = [
    // Get all.
    [2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009],
    [2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009],

    // Get one key.
    [2008],

    // Lower bound.
    [2008, 2009],
    [2009],
    [2009],
    [2009],

    // Upper bound.
    [2000, 2001, 2002],
    [2000, 2001],
    [2000, 2001],
    [2000, 2001],

    // Between.
    [2002, 2003, 2004, 2005, 2006, 2007, 2008],
    [2002, 2003, 2004, 2005, 2006, 2007],
    [2002, 2003, 2004, 2005, 2006, 2007],
    [2002, 2003, 2004, 2005, 2006, 2007],
    [2003, 2004, 2005, 2006, 2007, 2008],
    [2003, 2004, 2005, 2006, 2007, 2008],
    [2003, 2004, 2005, 2006, 2007, 2008],
  ];

  constructor(constructorFn: () => RuntimeIndex) {
    super(constructorFn);
  }

  testAddGet(index: RuntimeIndex): void {
    for (let i = 0; i < 26; ++i) {
      const key = [i, String.fromCharCode('z'.charCodeAt(0) - i)];
      const value = 2000 + i;
      index.add(key, value);
      const actualValue = index.get(key)[0];
      assert.equal(value, actualValue);
    }
  }

  testGetRangeCost(index: RuntimeIndex): void {
    this.populateIndex(index, 10);

    this.keyRanges.forEach((range, i) => {
      const expected = this.expectations[i];
      this.assertGetRangeCost(index, range, expected);
    }, this);
  }

  testRemove(index: RuntimeIndex): void {
    this.populateIndex(index);

    // Remove non-existent key shall not yield any error.
    index.remove([0, 'K']);

    assert.sameDeepOrderedMembers([2008], index.get([8, 'R']));
    index.remove([8, 'R']);
    assert.sameDeepOrderedMembers([], index.get([8, 'R']));
  }

  testSet(index: RuntimeIndex): void {
    this.populateIndex(index);

    index.remove([8, 'R']);
    assert.equal(25, index.getRange().length);

    for (let i = 0; i < 26; ++i) {
      const key = [i, String.fromCharCode('Z'.charCodeAt(0) - i)];
      const value = 3000 + i;
      index.set(key, value);
      const actualValue = index.get(key)[0];
      assert.equal(value, actualValue);
    }

    assert.equal(26, index.getRange().length);
  }

  testMinMax(index: RuntimeIndex): void {
    // First try an empty index.
    assert.isNull(index.min());
    assert.isNull(index.max());
    this.populateIndex(index);
    assert.sameDeepOrderedMembers([[0, 'Z'], [2000]], index.min() as unknown[]);
    assert.sameDeepOrderedMembers(
      [[25, 'A'], [2025]],
      index.max() as unknown[]
    );
  }

  testMultiRange(index: RuntimeIndex): void {
    this.populateIndex(index);

    // Simulate NOT(BETWEEN([2, 'X'], [24, 'B']))
    // The optimizer already handles the range order.
    const range1 = [
      new SingleKeyRange(SingleKeyRange.UNBOUND_VALUE, 2, false, true),
      new SingleKeyRange(SingleKeyRange.UNBOUND_VALUE, 'B', false, true),
    ];
    const range2 = [
      new SingleKeyRange(24, SingleKeyRange.UNBOUND_VALUE, true, false),
      new SingleKeyRange('X', SingleKeyRange.UNBOUND_VALUE, true, false),
    ];

    const expected = [2000, 2001, 2025];

    assert.sameDeepOrderedMembers(expected, index.getRange([range1, range2]));
    assert.sameDeepOrderedMembers(expected, index.getRange([range2, range1]));
  }

  private populateIndex(index: RuntimeIndex, count = 26): void {
    for (let i = 0; i < count; ++i) {
      const key = [i, String.fromCharCode('Z'.charCodeAt(0) - i)];
      const value = 2000 + i;
      index.add(key, value);
    }
  }
}
