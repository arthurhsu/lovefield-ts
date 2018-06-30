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

import * as chai from 'chai';
import * as sinon from 'sinon';

import {TransactionType} from '../lib/base/enum';
import {Exception} from '../lib/base/exception';
import {Global} from '../lib/base/global';
import {TableType} from '../lib/base/private_enum';
import {Row} from '../lib/base/row';
import {Service} from '../lib/base/service';
import {IndexStats} from '../lib/index/index_stats';
import {IndexStore} from '../lib/index/index_store';
import {RuntimeIndex} from '../lib/index/runtime_index';
import {Index} from '../lib/schema/index';
import {Table} from '../lib/schema/table';

export class TestUtil {
  public static assertThrowsError(exceptionCode: number, fn: () => any): void {
    let thrown = false;
    try {
      fn();
    } catch (e) {
      thrown = true;
      chai.assert.isTrue(e instanceof Exception);
      chai.assert.equal(exceptionCode, e.code);
    }
    chai.assert.isTrue(thrown);
  }

  public static assertPromiseReject(
      exceptionCode: number, promise: Promise<any>): Promise<any> {
    return promise.then(chai.assert.fail, (e: any) => {
      chai.assert.equal(exceptionCode, e.code);
    });
  }

  public static simulateIndexCost(
      sandbox: sinon.SinonSandbox, indexStore: IndexStore, indexSchema: Index,
      cost: number): void {
    const index =
        indexStore.get(indexSchema.getNormalizedName()) as RuntimeIndex;
    sandbox.stub(index, 'cost').callsFake(() => cost);
  }

  public static simulateIndexStats(
      sandbox: sinon.SinonSandbox, indexStore: IndexStore, indexName: string,
      indexStats: IndexStats): void {
    const index = indexStore.get(indexName) as RuntimeIndex;
    sandbox.stub(index, 'stats').callsFake(() => indexStats);
  }

  public static selectAll(global: Global, tableSchema: Table): Promise<Row[]> {
    const backStore = global.getService(Service.BACK_STORE);
    const tx = backStore.createTx(TransactionType.READ_ONLY, [tableSchema]);
    const table = tx.getTable(
        tableSchema.getName(), tableSchema.deserializeRow.bind(tableSchema),
        TableType.DATA);
    return table.get([]);
  }
}
