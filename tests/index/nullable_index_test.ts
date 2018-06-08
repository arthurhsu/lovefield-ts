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
import {Order} from '../../lib/base/enum';
import {BTree} from '../../lib/index/btree';
import {Key, SingleKeyRange} from '../../lib/index/key_range';
import {NullableIndex} from '../../lib/index/nullable_index';
import {SimpleComparator} from '../../lib/index/simple_comparator';
import {TestSingleRowNumericalKey} from '../../testing/index/test_single_row_numerical_key';
import {TestSingleRowStringKey} from '../../testing/index/test_single_row_string_key';

const assert = chai.assert;

describe('NullableIndex', () => {
  let index: NullableIndex;
  const NULL = null as any as Key;

  beforeEach(() => {
    index = new NullableIndex(
        new BTree('test', new SimpleComparator(Order.ASC), false));
  });

  it('nullableIndex', () => {
    assert.equal('test', index.getName());

    index.add(1, 2);
    index.add(1, 3);
    index.add(2, 4);
    index.add(3, 5);
    index.add(NULL, 7);
    index.add(NULL, 8);

    assert.sameOrderedMembers([2, 3], index.get(1));
    assert.sameOrderedMembers([7, 8], index.get(NULL));

    assert.equal(1, (index.min() as number[])[0]);
    assert.equal(3, (index.max() as number[])[0]);

    assert.equal(4, index.cost());

    assert.sameOrderedMembers(
        [4, 5], index.getRange([new SingleKeyRange(2, 3, false, false)]));
    assert.sameOrderedMembers([2, 3, 4, 5, 7, 8], index.getRange());

    index.remove(2);
    assert.sameOrderedMembers([], index.get(2));
    index.remove(NULL, 7);
    assert.sameOrderedMembers([8], index.get(NULL));

    index.set(1, 10);
    assert.sameOrderedMembers([10], index.get(1));
    index.set(NULL, 9);
    assert.sameOrderedMembers([9], index.get(NULL));

    assert.isTrue(index.containsKey(1));
    assert.isTrue(index.containsKey(NULL));

    index.remove(NULL);
    assert.isFalse(index.containsKey(NULL));
    assert.sameOrderedMembers([10, 5], index.getRange());

    index.set(NULL, 9);
    assert.sameOrderedMembers([9], index.get(NULL));

    index.clear();

    assert.isFalse(index.containsKey(1));
    assert.isFalse(index.containsKey(NULL));
  });

  it('SingleRowNumericalKey', () => {
    const test = new TestSingleRowNumericalKey(() => {
      return new NullableIndex(
          new BTree('test', new SimpleComparator(Order.ASC), false));
    });
    test.run();
  });

  it('SingleRowStringKey', () => {
    const test = new TestSingleRowStringKey(() => {
      return new NullableIndex(
          new BTree('test', new SimpleComparator(Order.ASC), false));
    });
    test.run();
  });

  it('serialize', () => {
    const deserializeFn = BTree.deserialize.bind(
        undefined, new SimpleComparator(Order.ASC), 'test', false);

    index.add(NULL, 1);
    index.add(NULL, 2);
    index.add(1, 3);
    index.add(1, 4);
    index.add(2, 5);
    const rows = index.serialize();

    const index2 = NullableIndex.deserialize(deserializeFn, rows);
    assert.sameOrderedMembers([3, 4, 5, 1, 2], index2.getRange());
    assert.sameOrderedMembers([1, 2], index2.get(NULL));
    assert.sameOrderedMembers([3, 4], index2.get(1));
    assert.sameOrderedMembers([5], index2.get(2));
  });

  // Tests that a unique nullable index allows multiple nullable keys (this
  // matches the behavior of other SQL engines).
  it('unique', () => {
    index = new NullableIndex(
        new BTree('test', new SimpleComparator(Order.ASC), true));
    index.add(NULL, 1);
    index.add(1, 2);
    index.add(NULL, 3);

    assert.sameOrderedMembers([1, 3], index.get(NULL));
  });

  it('stats', () => {
    index.add(NULL, 1);
    index.add(NULL, 2);
    index.add(NULL, 7);
    index.add(1, 3);
    index.add(1, 4);
    index.add(1, 8);
    index.add(2, 5);
    assert.equal(7, index.stats().totalRows);

    index.remove(NULL, 2);
    assert.equal(6, index.stats().totalRows);
    index.remove(NULL);
    assert.equal(4, index.stats().totalRows);
    index.set(NULL, 22);
    assert.equal(5, index.stats().totalRows);
    index.add(NULL, 33);
    assert.equal(6, index.stats().totalRows);
    index.remove(NULL);
    assert.equal(4, index.stats().totalRows);
    index.remove(1, 3);
    assert.equal(3, index.stats().totalRows);
    index.remove(1);
    assert.equal(1, index.stats().totalRows);
    index.clear();
    assert.equal(0, index.stats().totalRows);
  });
});
