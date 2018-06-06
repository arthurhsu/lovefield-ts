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

// Port of goog.math methods used by Lovefield.
export class MathHelper {
  public static longestCommonSubsequence<T>(
      array1: T[], array2: T[], comparator?: (l: T, r: T) => boolean,
      collector?: (idx1: number, idx2: number) => T): T[] {
    const defaultComparator = (a: T, b: T) => (a === b);
    const defaultCollector = (i1: number, i2: number) => array1[i1];
    const compare = comparator || defaultComparator;
    const collect = collector || defaultCollector;
    const length1 = array1.length;
    const length2 = array2.length;

    const arr: number[][] = [];
    let i: number;
    let j: number;
    for (i = 0; i < length1 + 1; ++i) {
      arr[i] = [];
      arr[i][0] = 0;
    }
    for (j = 0; j < length2 + 1; ++j) {
      arr[0][j] = 0;
    }
    for (i = 1; i < length1 + 1; ++i) {
      for (j = 1; j < length2 + 1; ++j) {
        arr[i][j] = compare(array1[i - 1], array2[j - 1]) ?
            arr[i - 1][j - 1] + 1 :
            Math.max(arr[i - 1][j], arr[i][j - 1]);
      }
    }

    // Backtracking
    const result: T[] = [];
    i = length1;
    j = length2;
    while (i > 0 && j > 0) {
      if (compare(array1[i - 1], array2[j - 1])) {
        result.unshift(collect(i - 1, j - 1));
        i--;
        j--;
      } else {
        if (arr[i - 1][j] > arr[i][j - 1]) {
          i--;
        } else {
          j--;
        }
      }
    }

    return result;
  }

  public static sum(...args: number[]): number {
    return args.reduce((sum, value) => sum + value, 0);
  }

  public static average(...args: number[]): number {
    return MathHelper.sum.apply(null, args) / args.length;
  }

  public static standardDeviation(...args: number[]): number {
    if (!args || args.length < 2) {
      return 0;
    }

    const mean = MathHelper.average.apply(null, args);
    const sampleVariance =
        MathHelper.sum.apply(null, args.map((val) => Math.pow(val - mean, 2))) /
        (args.length - 1);
    return Math.sqrt(sampleVariance);
  }
}
