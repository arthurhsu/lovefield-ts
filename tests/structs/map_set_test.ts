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
import { MapSet } from '../../lib/structs/map_set';

const assert = chai.assert;

describe('MapSet', () => {
  function getSampleMapSet(): MapSet<number, number> {
    const mapSet = new MapSet<number, number>();
    mapSet.set(10, 11);
    mapSet.set(10, 12);
    mapSet.set(20, 21);
    mapSet.set(20, 25);
    mapSet.set(30, 39);
    return mapSet;
  }

  it('set', () => {
    const mapSet = new MapSet<number, number>();
    assert.isFalse(mapSet.has(10));
    mapSet.set(10, 11);
    assert.equal(1, mapSet.size);
    assert.isTrue(mapSet.has(10));

    // Set the same key/value shall not alter the map.
    mapSet.set(10, 11);
    assert.equal(1, mapSet.size);

    // Set new value of a key shall alter the map.
    mapSet.set(10, 12);
    assert.equal(2, mapSet.size);
  });

  it('delete', () => {
    const mapSet = getSampleMapSet();

    assert.isTrue(mapSet.delete(10, 12));
    assert.equal(4, mapSet.size);

    // Test that removing a non-existing value for an existing key does not
    // modify the map.
    assert.isTrue(mapSet.has(10));
    assert.isFalse(mapSet.delete(10, 13));
    assert.equal(4, mapSet.size);

    // Test that removing a non-existing value for a non-existing key does not
    // modify the map or throw any errors.
    assert.isFalse(mapSet.has(100));
    assert.isFalse(mapSet.delete(100, 2000));
    assert.equal(4, mapSet.size);

    assert.isTrue(mapSet.delete(10, 11));
    assert.isNull(mapSet.get(10));
  });

  it('get', () => {
    const mapSet = getSampleMapSet();

    assert.deepEqual([11, 12], mapSet.get(10));
    assert.deepEqual([21, 25], mapSet.get(20));
    assert.deepEqual([39], mapSet.get(30));
    assert.isNull(mapSet.get(40));
  });

  it('size', () => {
    const emptyMapSet = new MapSet<number, number>();
    assert.equal(0, emptyMapSet.size);

    const mapSet = getSampleMapSet();
    assert.equal(5, mapSet.size);
    mapSet.delete(10, 11);
    assert.equal(4, mapSet.size);
    mapSet.delete(10, 12);
    assert.equal(3, mapSet.size);
    mapSet.delete(20, 21);
    assert.equal(2, mapSet.size);
    mapSet.delete(20, 25);
    assert.equal(1, mapSet.size);
    mapSet.delete(30, 39);
    assert.equal(0, mapSet.size);
  });

  it('clear', () => {
    const mapSet = getSampleMapSet();
    assert.equal(5, mapSet.size);

    mapSet.clear();
    assert.equal(0, mapSet.size);
  });

  it('keys', () => {
    const emptyMapSet = new MapSet<number, number>();
    assert.deepEqual([], emptyMapSet.keys());

    const mapSet = getSampleMapSet();
    assert.deepEqual([10, 20, 30], mapSet.keys());
  });

  it('values', () => {
    const emptyMapSet = new MapSet<number, number>();
    assert.deepEqual([], emptyMapSet.values());

    const mapSet = getSampleMapSet();
    assert.deepEqual([11, 12, 21, 25, 39], mapSet.values());
  });

  it('merge_empty', () => {
    const m1 = new MapSet<number, number>();
    const m2 = new MapSet<number, number>();
    const merged = m1.merge(m2);
    assert.equal(merged, m1);
    assert.equal(0, m1.size);
  });

  it('merge', () => {
    const mapSet1 = getSampleMapSet();
    const origMapSet1Size = mapSet1.size;
    assert.equal(5, origMapSet1Size);
    const mapSet2 = new MapSet<number, number>();
    mapSet2.set(10, 100);
    mapSet2.set(20, 200);
    mapSet2.set(40, 400);

    mapSet1.merge(mapSet2);
    assert.equal(origMapSet1Size + mapSet2.size, mapSet1.size);
    assert.deepEqual([11, 12, 100], mapSet1.get(10));
    assert.deepEqual([21, 25, 200], mapSet1.get(20));
    assert.deepEqual([39], mapSet1.get(30));
    assert.deepEqual([400], mapSet1.get(40));
  });
});
