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

import {ErrorCode} from './enum';
import {Global} from './global';

export class Exception {
  readonly message: string;
  readonly args: string[];

  constructor(readonly code: ErrorCode, ...args: string[]) {
    this.args = args;
    this.message = Global.get().getOptions().exceptionUrl + code.toString();

    if (args.length) {
      // Allow at most 4 parameters, each parameter at most 64 chars.
      for (let i = 0; i < Math.min(4, args.length); ++i) {
        const val = encodeURIComponent(String(args[i]).slice(0, 64));
        if (Global.get().getOptions().exceptionUrl.length) {
          this.message += `&p${i}=${val}`;
        } else {
          this.message += `|${val}`;
        }
      }
    }
  }

  toString(): string {
    const template =
      Global.get()
        .getOptions()
        .errorMessage(this.code as number) || this.code.toString();
    return template.replace(
      /{([^}]+)}/g,
      (match, pattern) => this.args[Number(pattern)]
    );
  }
} // class Exception
