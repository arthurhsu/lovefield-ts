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
