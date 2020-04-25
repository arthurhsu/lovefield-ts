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

import { Builder } from './builder';

// Keep lower case class name for compatibility with Lovefield API.
/* eslint-disable @typescript-eslint/class-name-casing */
// TODO(arthurhsu): FIXME: Builder should be a public interface, not concrete
// class. Currently Builder has no @export.
// @export
export class schema {
  // Returns a builder.
  // Note that Lovefield builder is a stateful object, and it remembers it has
  // been used for connecting a database instance. Once the connection is closed
  // or dropped, the builder cannot be used to reconnect. Instead, the caller
  // needs to construct a new builder for doing so.
  static create(name: string, version: number): Builder {
    return new Builder(name, version);
  }
}
/* eslint-disable @typescript-eslint/class-name-casing */
