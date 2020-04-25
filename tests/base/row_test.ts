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
import {Row} from '../../lib/base/row';

const assert = chai.assert;

describe('Row', () => {
  it('create', () => {
    const row1 = Row.create();
    const row2 = Row.create();
    assert.isBelow(row1.id(), row2.id());
  });

  it('getId', () => {
    const id = 10;
    const row = new Row(id, {});
    assert.equal(id, row.id());
  });

  it('getPayload', () => {
    const payload = {fieldA: 'valueA'};
    const row = Row.create(payload);

    assert.deepEqual(payload, row.payload());
  });

  it('binHexConversion', () => {
    const buffer = new ArrayBuffer(24);
    const uint8Array = new Uint8Array(buffer);
    for (let i = 0; i < 24; i++) {
      uint8Array[i] = i;
    }

    const expected = '000102030405060708090a0b0c0d0e0f1011121314151617';
    assert.equal(null, Row.hexToBin(''));
    assert.equal(null, Row.hexToBin(null));
    assert.equal(null, Row.binToHex(null));
    assert.equal(expected, Row.binToHex(buffer));
    assert.equal(expected, Row.binToHex(Row.hexToBin(expected)));
  });
});
