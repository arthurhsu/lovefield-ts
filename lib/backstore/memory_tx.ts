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

import {TransactionType} from '../base/enum';
import {TableType} from '../base/private_enum';
import {RawRow, Row} from '../base/row';
import {RuntimeTable} from '../base/runtime_table';
import {Journal} from '../cache/journal';
import {BaseTx} from './base_tx';
import {Memory} from './memory';

export class MemoryTx extends BaseTx {
  constructor(private store: Memory, type: TransactionType, journal?: Journal) {
    super(type, journal);
    if (type === TransactionType.READ_ONLY) {
      this.resolver.resolve();
    }
  }

  getTable(
    tableName: string,
    deserializeFn: (value: RawRow) => Row,
    tableType?: TableType
  ): RuntimeTable {
    return this.store.getTableInternal(tableName);
  }

  abort(): void {
    this.resolver.reject();
  }

  commitInternal(): Promise<unknown> {
    this.resolver.resolve();
    return this.resolver.promise;
  }
}
