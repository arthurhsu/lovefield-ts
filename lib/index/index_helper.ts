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

// Helper functions for index structures.
export class IndexHelper {
  // Java's String.hashCode method.
  //
  // for each character c in string
  //   hash = hash * 31 + c
  static hashCode(value: string): number {
    let hash = 0;
    for (let i = 0; i < value.length; ++i) {
      hash = (hash << 5) - hash + value.charCodeAt(i);
      hash = hash & hash; // Convert to 32-bit integer.
    }
    return hash;
  }

  // Compute hash key for an array.
  static hashArray(values: object[]): string {
    const keys = values.map(value => {
      return value !== undefined && value !== null
        ? IndexHelper.hashCode(value.toString()).toString(32)
        : '';
    });

    return keys.join('_');
  }

  // Slice result array by limit and skip.
  // Note: For performance reasons the input array might be modified in place.
  static slice(
    rawArray: number[],
    reverseOrder?: boolean,
    limit?: number,
    skip?: number
  ): number[] {
    const array = reverseOrder ? rawArray.reverse() : rawArray;

    // First handling case where no limit and no skip parameters have been
    // specified, such that no copying of the input array is performed. This is
    // an optimization such that unnecessary copying can be avoided for the
    // majority case (no limit/skip params).
    if (
      (limit === undefined || limit === null) &&
      (skip === undefined || skip === null)
    ) {
      return array;
    }

    // Handling case where at least one of limit/skip parameters has been
    // specified. The input array will have to be sliced.
    const actualLimit = Math.min(
      limit !== undefined ? limit : array.length,
      array.length
    );
    if (actualLimit === 0) {
      return [];
    }

    const actualSkip = Math.min(skip || 0, array.length);

    return array.slice(actualSkip, actualSkip + actualLimit);
  }
}
