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
import {Type} from '../../lib/base/enum';
import {Row} from '../../lib/base/row';
import {DefaultCache} from '../../lib/cache/default_cache';
import {Builder} from '../../lib/schema/builder';

const assert = chai.assert;

describe('DefaultCache', () => {
  it('implementsCache', () => {
    const builder = new Builder('test', 1);
    builder.createTable('Foo').addColumn('id', Type.STRING);
    builder.createTable('Bar').addColumn('id', Type.STRING);

    const cache = new DefaultCache(builder.getSchema());

    assert.sameMembers([null, null], cache.getMany([1, 2]));
    const payload = {id: 'something'};
    const row = new Row(1, payload);
    const row2 = new Row(4, payload);
    const row3 = new Row(3, payload);
    cache.setMany('Foo', [row, row2]);
    const result = cache.getMany([0, 1]);
    assert.isNull(result[0]);
    assert.deepEqual(payload, (result[1] as any as Row).payload());
    assert.isNull(cache.get(0));
    assert.deepEqual(payload, (cache.get(1) as any as Row).payload());
    cache.set('Bar', row3);

    assert.equal(3, cache.getCount());
    cache.remove('Foo', 4);
    assert.sameMembers([null, null], cache.getMany([0, 4]));
    assert.equal(2, cache.getCount());

    cache.setMany('Foo', [row2]);
    assert.equal(3, cache.getCount());
    const range = cache.getRange('Foo', 2, 4);
    assert.equal(1, range.length);
    assert.deepEqual(payload, range[0].payload());
    assert.equal(1, cache.getCount('Bar'));
    assert.equal(2, cache.getCount('Foo'));

    cache.clear();
    assert.equal(0, cache.getCount());
  });
});
