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
import {Global} from '../base/global';
import {TableType} from '../base/private_enum';
import {RawRow, Row} from '../base/row';
import {RuntimeTable} from '../base/runtime_table';
import {Journal} from '../cache/journal';

import {BaseTx} from './base_tx';
import {BundledObjectStore} from './bundled_object_store';
import {ObjectStore} from './object_store';

export class IndexedDBTx extends BaseTx {
  constructor(
    private global: Global,
    private tx: IDBTransaction,
    txType: TransactionType,
    private bundleMode: boolean,
    journal?: Journal
  ) {
    super(txType, journal);
    this.tx.oncomplete = () => {
      this.resolver.resolve();
    };
    this.tx.onabort = (ev: Event) => {
      this.resolver.reject(ev);
    };
  }

  getTable(
    tableName: string,
    deserializeFn: (value: RawRow) => Row,
    type?: TableType
  ): RuntimeTable {
    if (this.bundleMode) {
      const tableType =
        type !== undefined && type !== null ? type : TableType.DATA;
      return BundledObjectStore.forTableType(
        this.global,
        this.tx.objectStore(tableName),
        deserializeFn,
        tableType
      );
    } else {
      return new ObjectStore(this.tx.objectStore(tableName), deserializeFn);
    }
  }

  abort(): void {
    this.tx.abort();
  }

  commitInternal(): Promise<unknown> {
    return this.resolver.promise;
  }
}
