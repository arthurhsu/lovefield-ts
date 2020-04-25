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
import {IndexHelper} from '../../lib/index/index_helper';

const assert = chai.assert;

describe('IndexHelper', () => {
  it('hashCode', () => {
    assert.equal(0, IndexHelper.hashCode(''));

    // Space char's ASCII code is 32.
    // Hash function is hash = hash * 31 + c, so hash code of 32 is 32,
    // and hash code of two 32's are 32 * 32 = 1024
    assert.equal(32, IndexHelper.hashCode(' '));
    assert.equal(1024, IndexHelper.hashCode('  '));
  });

  it('hashArray', () => {
    assert.equal('', IndexHelper.hashArray([]));
    assert.equal('', IndexHelper.hashArray([(null as unknown) as object]));
    assert.equal(
      '0_',
      IndexHelper.hashArray((['', null] as unknown) as object[])
    );
    assert.equal('10', IndexHelper.hashArray([(' ' as unknown) as object]));
    assert.equal(
      '10_10',
      IndexHelper.hashArray(([' ', ' '] as unknown) as object[])
    );
  });

  function checkSlice(reverseOrder: boolean): void {
    const ARRAY = [0, 1, 2, 3, 4];
    const REVERSE_ARRAY = ARRAY.slice().reverse();

    assert.sameOrderedMembers(
      !reverseOrder ? ARRAY : REVERSE_ARRAY,
      IndexHelper.slice(ARRAY.slice(), reverseOrder)
    );

    // Test empty array
    assert.sameOrderedMembers([], IndexHelper.slice([]));
    assert.sameOrderedMembers([], IndexHelper.slice([], reverseOrder, 1));
    assert.sameOrderedMembers(
      [],
      IndexHelper.slice([], reverseOrder, undefined, 1)
    );

    // Test LIMIT
    assert.sameOrderedMembers(
      !reverseOrder ? [ARRAY[0]] : [REVERSE_ARRAY[0]],
      IndexHelper.slice(ARRAY.slice(), reverseOrder, 1)
    );
    assert.sameOrderedMembers(
      !reverseOrder ? ARRAY : REVERSE_ARRAY,
      IndexHelper.slice(ARRAY.slice(), reverseOrder, ARRAY.length)
    );
    assert.sameOrderedMembers(
      !reverseOrder ? ARRAY : REVERSE_ARRAY,
      IndexHelper.slice(ARRAY.slice(), reverseOrder, ARRAY.length + 1)
    );

    // Test SKIP
    assert.sameOrderedMembers(
      !reverseOrder ? ARRAY : REVERSE_ARRAY,
      IndexHelper.slice(ARRAY.slice(), reverseOrder, undefined, 0)
    );
    assert.sameOrderedMembers(
      !reverseOrder ? ARRAY : REVERSE_ARRAY,
      IndexHelper.slice(ARRAY.slice(), reverseOrder, ARRAY.length, 0)
    );

    for (let i = 0; i < ARRAY.length; ++i) {
      assert.sameOrderedMembers(
        !reverseOrder ? [ARRAY[i]] : [REVERSE_ARRAY[i]],
        IndexHelper.slice(ARRAY.slice(), reverseOrder, 1, i)
      );
    }
  }

  it('slice_ASC', () => {
    checkSlice(false);
  });

  it('slice_DESC', () => {
    checkSlice(true);
  });
});
