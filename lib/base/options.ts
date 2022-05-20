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

import {Global} from './global';
import {LovefieldOptions} from './lovefield_options';

export class DefaultOptions implements LovefieldOptions {
  readonly debugMode: boolean;
  readonly memoryOnly: boolean;
  readonly exceptionUrl: string;
  readonly useGetAll: boolean;
  static url =
    'http://google.github.io/lovefield/error_lookup/src/error_lookup.html?c=';

  constructor() {
    this.debugMode = false;
    this.memoryOnly = false;
    this.exceptionUrl = DefaultOptions.url;
    this.useGetAll = false;
  }

  errorMessage(code: number): string {
    return code.toString();
  }
}

// @export
export class options {
  static set(opt?: LovefieldOptions): void {
    const options = opt || (new DefaultOptions() as LovefieldOptions);
    if (typeof options.exceptionUrl !== 'string') {
      options.exceptionUrl = DefaultOptions.url;
    }
    if (typeof options.errorMessage !== 'function') {
      options.errorMessage = (code: number) => {
        return code.toString();
      };
    }
    Global.get().setOptions(options);
  }
}
