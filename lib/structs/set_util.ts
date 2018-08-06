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

// Return the difference between two sets. The return value is a new set
// containing all the values (primitive or objects) present in set1 but not in
// set2.
/*
export function setDiff<T>(set1: Set<T>, set2: Set<T>): Set<T> {
  const result = new Set<T>();
  Array.from(set1.values()).forEach((v) => {
    if (!set2.has(v)) {
      result.add(v);
    }
  });
  return result;
}
*/

// Whether set2 is a subset of set1
export function isSubset<T>(set1: Set<T>, set2: Set<T>): boolean {
  if (set2.size > set1.size) {
    return false;
  }

  let result = true;
  set2.forEach((value) => result = result && set1.has(value));
  return result;
}

export function setEquals<T>(set1: Set<T>, set2: Set<T>): boolean {
  return set1.size === set2.size && isSubset(set1, set2);
}
