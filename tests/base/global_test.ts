/**
 * @license
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
import {ErrorCode} from '../../lib/base/exception';
import {Global} from '../../lib/base/global';
import {ServiceId} from '../../lib/base/service_id';
import {TestUtil} from '../../testing/test_util';

const assert = chai.assert;

describe('Global', () => {
  let global: Global;

  before(() => {
    global = Global.get();
  });

  after(() => {
    global.clear();
  });

  it('returnSameInstance', () => {
    const global2 = Global.get();
    assert.strictEqual(global, global2);
  });

  it('getService', () => {
    const serviceId = new ServiceId<object>('whatever');
    const serviceId2 = new ServiceId<object>('foo');
    const service = {};
    global.registerService(serviceId, service);
    const serviceFromGlobal = global.getService(serviceId);
    assert.strictEqual(service, serviceFromGlobal);
    assert.isTrue(global.isRegistered(serviceId));
    assert.isFalse(global.isRegistered(serviceId2));

    TestUtil.assertThrowsError(ErrorCode.SERVICE_NOT_FOUND, () => {
      global.getService(serviceId2);
    });
  });

  it('clear', () => {
    const serviceId = new ServiceId<object>('whatever');
    const service = {};
    global.registerService(serviceId, service);
    assert.isTrue(global.isRegistered(serviceId));
    global.clear();
    assert.isFalse(global.isRegistered(serviceId));
    assert.deepEqual([], Array.from(global.listServices()));
  });
});
