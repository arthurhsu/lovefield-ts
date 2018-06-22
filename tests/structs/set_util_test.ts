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
import {isSubset, setDiff, setEquals} from '../../lib/structs/set_util';

const assert = chai.assert;

describe('SetUtil', () => {
  it('setDiff_number', () => {
    const set = new Set<number>();
    const set2 = new Set<number>();

    for (let i = 0; i < 10; ++i) {
      set.add(i * 10);
      set2.add(i * 10);
    }
    set.add(1000);
    set.add(null as any as number);
    set.add(undefined as any as number);
    assert.equal(13, set.size);
    const diffSet = setDiff(set, set2);
    assert.equal(3, diffSet.size);
    assert.isTrue(diffSet.has(1000));
    assert.isTrue(diffSet.has(undefined as any as number));
    assert.isTrue(diffSet.has(null as any as number));
  });

  it('setDiff_string', () => {
    const set = new Set<string>();
    const set2 = new Set<string>();

    for (let i = 0; i < 10; ++i) {
      set.add(i * 10 + '-string');
      set2.add(i * 10 + '-string');
    }
    set.add('1000-string');
    set.add('');
    assert.equal(12, set.size);
    const diffSet = setDiff(set, set2);
    assert.equal(2, diffSet.size);
    assert.isTrue(diffSet.has('1000-string'));
    assert.isTrue(diffSet.has(''));
  });

  it('setDiff_Object', () => {
    class MyClass {
      public i: number;
      public name: string;
      constructor(i: number, name: string) {
        this.i = i;
        this.name = name;
      }
    }

    const rows = new Array<MyClass>();
    const set = new Set<MyClass>();

    for (let i = 0; i < 10; ++i) {
      rows[i] = new MyClass(i, i + '-string');
      set.add(rows[i]);
    }
    set.add(null as any as MyClass);
    assert.equal(11, set.size);

    const set2 = new Set<MyClass>(rows);
    assert.equal(10, set2.size);
    const diffSet = setDiff(set, set2);
    assert.equal(1, diffSet.size);
    assert.isTrue(diffSet.has(null as any as MyClass));
  });

  it('isSubSet', () => {
    function checker<T>(
        set1: Set<T>, set2: Set<T>, set3: Set<T>, set4: Set<T>): void {
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
        set1: Set<T>, set2: Set<T>, set3: Set<T>, set4: Set<T>,
        set5: Set<T>): void {
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
