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

// A MapSet maps a key to a set of values, without allowing duplicate entries
// for a given key.
export class MapSet<K, V> {
  private map: Map<K, Set<V>>;
  private count: number;

  constructor() {
    this.map = new Map<K, Set<V>>();
    this.count = 0;
  }

  get size(): number {
    return this.count;
  }

  has(key: K): boolean {
    return this.map.has(key);
  }

  set(key: K, value: V): MapSet<K, V> {
    const valueSet = this.getSet(key);
    if (!valueSet.has(value)) {
      valueSet.add(value);
      this.count++;
    }
    return this;
  }

  setMany(key: K, values: V[]): MapSet<K, V> {
    const valueSet = this.getSet(key);
    values.forEach(value => {
      if (!valueSet.has(value)) {
        valueSet.add(value);
        this.count++;
      }
    });
    return this;
  }

  merge(mapSet: MapSet<K, V>): MapSet<K, V> {
    mapSet
      .keys()
      .forEach(key => this.setMany(key, Array.from(mapSet.getSet(key))));
    return this;
  }

  /**
   * Removes a value associated with the given key.
   * Returns true if the map was modified.
   */
  delete(key: K, value: V): boolean {
    const valueSet = this.map.get(key) || null;
    if (valueSet === null) {
      return false;
    }

    const didRemove = valueSet.delete(value);
    if (didRemove) {
      this.count--;
      if (valueSet.size === 0) {
        this.map.delete(key);
      }
    }
    return didRemove;
  }

  get(key: K): V[] | null {
    const valueSet = this.map.get(key) || null;
    return valueSet === null ? null : Array.from(valueSet);
  }

  clear(): void {
    this.map.clear();
    this.count = 0;
  }

  keys(): K[] {
    return Array.from(this.map.keys());
  }

  values(): V[] {
    const results: V[] = [];
    this.map.forEach((valueSet, key) =>
      results.push.apply(results, Array.from(valueSet))
    );
    return results;
  }

  // Returns a set for a given key. If the key does not exist in the map,
  // a new Set will be created.
  getSet(key: K): Set<V> {
    let valueSet = this.map.get(key) || null;
    if (valueSet === null) {
      valueSet = new Set<V>();
      this.map.set(key, valueSet);
    }
    return valueSet;
  }
}
