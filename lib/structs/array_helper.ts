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

import {assert} from '../base/assert';

export class ArrayHelper {
  // Returns true if the value were inserted, false otherwise.
  public static binaryInsert<T = number>(
      arr: T[], value: T, comparator?: (l: T, r: T) => number): boolean {
    const index = ArrayHelper.binarySearch(arr, value, comparator);
    if (index < 0) {
      arr.splice(-(index + 1), 0, value);
      return true;
    }
    return false;
  }

  // Returns true if the value were inserted, false otherwise.
  public static binaryRemove<T = number>(
      arr: T[], value: T, comparator?: (l: T, r: T) => number): boolean {
    const index = ArrayHelper.binarySearch(arr, value, comparator);
    if (index < 0) {
      return false;
    }

    arr.splice(index, 1);
    return true;
  }

  // Randomly shuffle an array's element.
  public static shuffle<T>(arr: T[]): void {
    for (let i = arr.length - 1; i > 0; i--) {
      // Choose a random array index in [0, i] (inclusive with i).
      const j = Math.floor(Math.random() * (i + 1));
      const tmp = arr[i];
      arr[i] = arr[j];
      arr[j] = tmp;
    }
  }

  // Clone the array.
  public static clone<T>(arr: T[]): T[] {
    const length = arr.length;
    if (length > 0) {
      const rv = new Array(length);
      arr.forEach((v, i) => rv[i] = v);
      return rv;
    }
    return [];
  }

  // Flatten the array.
  public static flatten(...arr: any[]): any[] {
    const CHUNK_SIZE = 8192;

    const result: any[] = [];
    arr.forEach((element) => {
      if (Array.isArray(element)) {
        for (let c = 0; c < element.length; c += CHUNK_SIZE) {
          const chunk = element.slice(c, c + CHUNK_SIZE);
          const recurseResult = ArrayHelper.flatten.apply(null, chunk);
          recurseResult.forEach((r: any) => result.push(r));
        }
      } else {
        result.push(element);
      }
    });
    return result;
  }

  // Cartesian product of zero or more sets.  Gives an iterator that gives every
  // combination of one element chosen from each set.  For example,
  // ([1, 2], [3, 4]) gives ([1, 3], [1, 4], [2, 3], [2, 4]).
  public static product<T>(arrays: T[][]): T[][] {
    const someArrayEmpty = arrays.some((arr) => !arr.length);
    if (someArrayEmpty || arrays.length === 0) {
      return [];
    }

    let indices: number[]|null = new Array<number>(arrays.length);
    indices.fill(0);
    const result = [];
    while (indices !== null) {
      result.push(indices.map(
          (valueIndex, arrayIndex) => arrays[arrayIndex][valueIndex]));

      // Generate the next-largest indices for the next call.
      // Increase the rightmost index. If it goes over, increase the next
      // rightmost (like carry-over addition).
      for (let i = indices.length - 1; i >= 0; i--) {
        // Assertion prevents compiler warning below.
        assert(indices !== null);
        if (indices[i] < arrays[i].length - 1) {
          indices[i]++;
          break;
        }

        // We're at the last indices (the last element of every array), so
        // the iteration is over on the next call.
        if (i === 0) {
          indices = null;
          break;
        }
        // Reset the index in this column and loop back to increment the
        // next one.
        indices[i] = 0;
      }
    }
    return result;
  }

  // Returns an object whose keys are all unique return values of sorter.
  public static bucket<T>(arr: T[], sorter: (v: T) => any): object {
    const bucket = {};

    arr.forEach((v) => {
      const key = sorter(v);
      if (bucket[key] === undefined) {
        bucket[key] = [];
      }
      bucket[key].push(v);
    });
    return bucket;
  }

  // Returns lowest index of the target value if found, otherwise
  // (-(insertion point) - 1). The insertion point is where the value should
  // be inserted into arr to preserve the sorted property.  Return value >= 0
  // iff target is found.
  private static binarySearch<T = number>(
      arr: T[], value: T, comparator?: (l: T, r: T) => number): number {
    let left = 0;
    let right = arr.length;
    const comp: (l: T, r: T) => number = comparator ||
        ArrayHelper.defaultComparator as any as (l: T, r: T) => number;
    while (left < right) {
      const middle = (left + right) >> 1;
      if (comp(arr[middle], value) < 0) {
        left = middle + 1;
      } else {
        right = middle;
      }
    }

    // ~left is a shorthand for -left - 1.
    return left === right && arr[left] === value ? left : ~left;
  }

  // Returns negative value if lhs < rhs, 0 if equal, positive value if
  // lhs > rhs.
  private static defaultComparator(lhs: number, rhs: number): number {
    return lhs - rhs;
  }
}
