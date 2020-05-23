/**
 * Copyright 2020 The Lovefield Project Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the 'License');
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an 'AS IS' BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Configuration class that replaces flags.
// This allows Lovefield users to customize the library without compilation.
export interface LovefieldOptions {
  // This controls whether built-in asserts will trigger or not.
  debugMode: boolean;

  // This will force Lovefield to always use in-memory database only.
  memoryOnly: boolean;

  // This controls where to host error message lookup.
  // Default is http://google.github.io/lovefield/error_lookup/src/error_lookup.html
  exceptionUrl: string;

  // Use getAll() optimization.
  // Chrome is not reliable for its IDBObjectStore.getAll() API, see
  // https://bugs.chromium.org/p/chromium/issues/detail?id=868177
  // This option is defaulted to false.
  useGetAll: boolean;

  // This translates error code into meaningful strings.
  // Default is to stringify the code itself.
  // See debug/debug_options.ts for in-program translation.
  errorMessage(code: number): string;
}
