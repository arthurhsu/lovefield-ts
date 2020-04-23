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

import { LocalStorageTable } from '../../lib/backstore/local_storage_table';
import { Capability } from '../../lib/base/capability';
import { TableTester } from '../../testing/backstore/table_tester';

describe('LocalStorageTable', () => {
  if (!Capability.get().localStorage) {
    return;
  }

  it('LocalStorageTable', () => {
    const tester = new TableTester(() => {
      return new LocalStorageTable('foo.bar');
    });
    return tester.run();
  });
});
