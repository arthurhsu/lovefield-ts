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
import {ErrorCode} from '../../lib/base/enum';
import {Exception} from '../../lib/base/exception';
import {Flags} from '../../lib/gen/flags';

const assert = chai.assert;

describe('Exception', () => {
  const BASE_URL =
    'http://google.github.io/lovefield/error_lookup/src/error_lookup.html?c=';

  it('ctorSingleArg', () => {
    const e = new Exception(ErrorCode.SYSTEM_ERROR);
    assert.equal(BASE_URL + '0', e.message);
  });

  it('ctorEncodeString', () => {
    const e = new Exception(ErrorCode.TABLE_NOT_FOUND, 'Album 1');
    assert.equal(BASE_URL + '101&p0=Album%201', e.message);
  });

  it('ctorTwoArgs', () => {
    const e = new Exception(ErrorCode.INVALID_TX_STATE, '2', '8');
    assert.equal(BASE_URL + '107&p0=2&p1=8', e.message);
  });

  it('ctorAtMostFourArgs', () => {
    const e = new Exception(
      ErrorCode.SIMULATED_ERROR,
      'a',
      'b',
      'c',
      'd',
      'e',
      'f',
      'g'
    );
    assert.equal(BASE_URL + '999&p0=a&p1=b&p2=c&p3=d', e.message);
  });

  it('ctorLongString', () => {
    const HEX = '0123456789abcdef';
    let longString = '';
    let expected = '';
    for (let i = 0; i < 10; i++) {
      if (i < 4) {
        expected += HEX;
      }
      longString += HEX;
    }

    const e = new Exception(ErrorCode.SIMULATED_ERROR, longString);
    assert.equal(BASE_URL + '999&p0=' + expected, e.message);
  });

  it('ctorUndefinedArg', () => {
    const e = new Exception(
      ErrorCode.SIMULATED_ERROR,
      '3',
      (undefined as unknown) as string
    );
    assert.equal(BASE_URL + '999&p0=3&p1=undefined', e.message);
  });

  it('baseUrlOverride', () => {
    const origUrl = Flags.EXCEPTION_URL;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (Flags as any).EXCEPTION_URL = '';
    const e = new Exception(
      ErrorCode.SIMULATED_ERROR,
      'a',
      'b',
      'c',
      'd',
      'e',
      'f',
      'g'
    );
    assert.equal('999|a|b|c|d', e.message);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (Flags as any).EXCEPTION_URL = origUrl;
  });

  it('getMessage', () => {
    const debug = Flags.DEBUG;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (Flags as any).DEBUG = true;
    const e = new Exception(ErrorCode.SIMULATED_ERROR);
    assert.equal('Simulated error', e.toString());
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (Flags as any).DEBUG = debug;
  });
});
