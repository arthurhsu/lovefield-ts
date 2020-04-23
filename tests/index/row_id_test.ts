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
import { RowId } from '../../lib/index/row_id';
import { RuntimeIndex } from '../../lib/index/runtime_index';

const assert = chai.assert;

describe('RowId', () => {
  function getSampleIndex(rowCount: number): RuntimeIndex {
    const index = new RowId('dummyName');
    for (let i = 0; i < rowCount; ++i) {
      index.set(i, i * 100);
    }
    return index;
  }

  function checkGetRange(index: RuntimeIndex): void {
    assert.equal(10, index.getRange().length);

    let result = index.getRange();
    assert.equal(10, result.length);
    result = index.getRange([SingleKeyRange.lowerBound(1)]);
    assert.equal(9, result.length);
    assert.equal(1, result[0]);
    result = index.getRange([new SingleKeyRange(1, 1, false, false)]);
    assert.equal(1, result.length);
    assert.equal(1, result[0]);
    result = index.getRange([new SingleKeyRange(1, 2, false, false)]);
    assert.equal(2, result.length);
    assert.equal(1, result[0]);
    assert.equal(2, result[1]);
  }

  it('constructs', () => {
    checkGetRange(getSampleIndex(10));
  });

  it('remove', () => {
    const index = getSampleIndex(10);
    index.remove(2, 2);
    assert.sameOrderedMembers([], index.get(2));
    assert.sameOrderedMembers([], index.getRange([SingleKeyRange.only(2)]));
    assert.equal(9, index.cost(new SingleKeyRange(1, 3, false, false)));
  });

  // Tests that serializing and deserializing produces the original index.
  it('serialize', () => {
    const index = getSampleIndex(10);

    const serialized = index.serialize();
    assert.equal(1, serialized.length);
    assert.equal(RowId.ROW_ID, serialized[0].id());

    const deserialized = RowId.deserialize('dummyName', serialized);
    checkGetRange(deserialized);
  });

  it('minmax', () => {
    const index1 = new RowId('dummyName');
    assert.isNull(index1.min());
    assert.isNull(index1.max());
    const rowCount = 7;
    const index2 = getSampleIndex(rowCount);
    assert.sameDeepOrderedMembers([0, [0]], index2.min() as unknown[]);
    assert.sameDeepOrderedMembers(
      [rowCount - 1, [rowCount - 1]],
      index2.max() as unknown[]
    );
  });

  it('stats', () => {
    const index = new RowId('dummyName');
    assert.equal(0, index.stats().totalRows);

    index.add(1, 1);
    index.add(2, 2);
    index.add(3, 3);
    assert.equal(3, index.stats().totalRows);

    index.remove(1, 1);
    assert.equal(2, index.stats().totalRows);

    index.remove(4, 4);
    assert.equal(2, index.stats().totalRows);

    index.remove(4, 4);
    assert.equal(2, index.stats().totalRows);

    index.clear();
    assert.equal(0, index.stats().totalRows);
  });
});
