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

import { Order } from '../base/enum';
import { Favor } from '../base/private_enum';
import { Comparator } from './comparator';
import {
  Key,
  KeyRange,
  MultiKey,
  SingleKey,
  SingleKeyRange,
} from './key_range';
import { SimpleComparator } from './simple_comparator';

export class MultiKeyComparator implements Comparator {
  static createOrders(numKeys: number, order: Order): Order[] {
    const orders: Order[] = new Array<Order>(numKeys);
    for (let i = 0; i < numKeys; ++i) {
      orders[i] = order;
    }
    return orders;
  }

  protected comparators: SimpleComparator[];

  constructor(orders: Order[]) {
    this.comparators = orders.map(order => new SimpleComparator(order));
  }

  compare(lk: Key, rk: Key): Favor {
    const lhs = lk as MultiKey;
    const rhs = rk as MultiKey;
    return this.forEach(lhs, rhs, (c, l, r) => {
      return l === SingleKeyRange.UNBOUND_VALUE ||
        r === SingleKeyRange.UNBOUND_VALUE
        ? Favor.TIE
        : c.compare(l, r);
    });
  }

  min(lk: Key, rk: Key): Favor {
    const lhs = lk as MultiKey;
    const rhs = rk as MultiKey;
    return this.forEach(lhs, rhs, (c, l, r) => c.min(l, r));
  }

  max(lk: Key, rk: Key): Favor {
    const lhs = lk as MultiKey;
    const rhs = rk as MultiKey;
    return this.forEach(lhs, rhs, (c, l, r) => c.max(l, r));
  }

  compareRange(k: Key, range: KeyRange): boolean[] {
    const key = k as MultiKey;
    const results = [true, true];
    for (
      let i = 0;
      i < this.comparators.length && (results[0] || results[1]);
      ++i
    ) {
      const dimensionResults = this.comparators[i].compareRange(
        key[i],
        range[i]
      );
      results[0] = results[0] && dimensionResults[0];
      results[1] = results[1] && dimensionResults[1];
    }
    return results;
  }

  isInRange(k: Key, range: KeyRange): boolean {
    const key = k as MultiKey;
    let isInRange = true;
    for (let i = 0; i < this.comparators.length && isInRange; ++i) {
      isInRange = this.comparators[i].isInRange(key[i], range[i]);
    }
    return isInRange;
  }

  isFirstKeyInRange(k: Key, range: KeyRange): boolean {
    const key = k as MultiKey;
    return this.comparators[0].isInRange(key[0], range[0]);
  }

  getAllRange(): KeyRange {
    return this.comparators.map(c => c.getAllRange());
  }

  sortKeyRanges(keyRanges: KeyRange[]): KeyRange[] {
    const outputKeyRanges = keyRanges.filter(range => {
      return range.every(r => r !== undefined && r !== null);
    });

    // Ranges are in the format of
    // [[dim0_range0, dim1_range0, ...], [dim0_range1, dim1_range1, ...], ...]
    // Reorganize the array to
    // [[dim0_range0, dim0_range1, ...], [dim1_range0, dim1_range1, ...], ...]
    const keysPerDimensions: KeyRange[] = new Array<KeyRange>(
      this.comparators.length
    );
    for (let i = 0; i < keysPerDimensions.length; i++) {
      keysPerDimensions[i] = outputKeyRanges.map(range => range[i]);
    }
    // Sort ranges per dimension.
    keysPerDimensions.forEach((keys, i) => {
      keys.sort((lhs: SingleKeyRange, rhs: SingleKeyRange) => {
        return this.comparators[i].orderKeyRange(lhs, rhs);
      });
    }, this);

    // Swapping back to original key range format. This time the new ranges
    // are properly aligned from left to right in each dimension.
    const finalKeyRanges: KeyRange[] = new Array<KeyRange>(
      outputKeyRanges.length
    );
    for (let i = 0; i < finalKeyRanges.length; i++) {
      finalKeyRanges[i] = keysPerDimensions.map(keys => keys[i]);
    }

    // Perform another sorting to properly arrange order of ranges with either
    // excludeLower or excludeUpper.
    return finalKeyRanges.sort((lhs, rhs) => {
      let favor = Favor.TIE;
      for (let i = 0; i < this.comparators.length && favor === Favor.TIE; ++i) {
        favor = this.comparators[i].orderKeyRange(lhs[i], rhs[i]);
      }
      return favor;
    });
  }

  isLeftOpen(range: KeyRange): boolean {
    return this.comparators[0].isLeftOpen(range[0]);
  }

  rangeToKeys(keyRange: KeyRange): Key[] {
    const startKey = keyRange.map(
      (range, i) => this.comparators[i].rangeToKeys(range)[0]
    );
    const endKey = keyRange.map(
      (range, i) => this.comparators[i].rangeToKeys(range)[1]
    );

    return [startKey, endKey];
  }

  comparable(key: Key): boolean {
    return (key as SingleKey[]).every((keyDimension, i) =>
      this.comparators[i].comparable(keyDimension)
    );
  }

  keyDimensions(): number {
    return this.comparators.length;
  }

  private forEach(
    lhs: MultiKey,
    rhs: MultiKey,
    fn: (c: SimpleComparator, l: SingleKey, r: SingleKey) => Favor
  ): Favor {
    let favor = Favor.TIE;
    for (let i = 0; i < this.comparators.length && favor === Favor.TIE; ++i) {
      favor = fn(this.comparators[i], lhs[i], rhs[i]);
    }
    return favor;
  }
}
