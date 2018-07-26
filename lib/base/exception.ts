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

import {Flags} from '../gen/flags';
import {ErrorCode} from './enum';
import {getErrorMessage} from './error_message';

export class Exception {
  public readonly message: string;
  public readonly args: any[];

  constructor(readonly code: ErrorCode, ...args: any[]) {
    this.args = args;
    this.message = Flags.EXCEPTION_URL + code.toString();

    if (args.length) {
      // Allow at most 4 parameters, each parameter at most 64 chars.
      for (let i = 0; i < Math.min(4, args.length); ++i) {
        const val = encodeURIComponent(String(args[i]).slice(0, 64));
        if (Flags.EXCEPTION_URL.length) {
          this.message += `&p${i}=${val}`;
        } else {
          this.message += `|${val}`;
        }
      }
    }
  }

  public toString(): string {
    const template: string = getErrorMessage(this.code) || this.code.toString();
    return template.replace(
        /{([^}]+)}/g, (match, pattern) => this.args[parseInt(pattern, 10)]);
  }
}  // class Exception
