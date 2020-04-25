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

import {Global} from '../../base/global';
import {ExecType} from '../../base/private_enum';
import {Row} from '../../base/row';
import {Service} from '../../base/service';
import {Journal} from '../../cache/journal';
import {IndexHelper} from '../../index/index_helper';
import {IndexStore} from '../../index/index_store';
import {Key, SingleKeyRange} from '../../index/key_range';
import {RuntimeIndex} from '../../index/runtime_index';
import {Context} from '../../query/context';
import {SelectContext} from '../../query/select_context';
import {IndexImpl} from '../../schema/index_impl';
import {Relation} from '../relation';

import {IndexKeyRangeCalculator} from './index_key_range_calculator';
import {PhysicalQueryPlanNode} from './physical_query_plan_node';

export class IndexRangeScanStep extends PhysicalQueryPlanNode {
  useLimit: boolean;
  useSkip: boolean;
  private indexStore: IndexStore;

  // |reverseOrder|: return the results in reverse index order.
  constructor(
    global: Global,
    public index: IndexImpl,
    public keyRangeCalculator: IndexKeyRangeCalculator,
    public reverseOrder: boolean
  ) {
    super(0, ExecType.NO_CHILD);
    this.indexStore = global.getService(Service.INDEX_STORE);
    this.useLimit = false;
    this.useSkip = false;
  }

  toString(): string {
    return (
      `index_range_scan(${this.index.getNormalizedName()}, ?, ` +
      (this.reverseOrder ? 'reverse' : 'natural') +
      (this.useLimit ? ', limit:?' : '') +
      (this.useSkip ? ', skip:?' : '') +
      ')'
    );
  }

  toContextString(context: SelectContext): string {
    let results = this.toString();
    const keyRanges = this.keyRangeCalculator.getKeyRangeCombinations(context);
    results = results.replace('?', keyRanges.toString());

    if (this.useLimit) {
      results = results.replace('?', context.limit.toString());
    }
    if (this.useSkip) {
      results = results.replace('?', context.skip.toString());
    }

    return results;
  }

  execInternal(
    relations: Relation[],
    journal?: Journal,
    ctx?: Context
  ): Relation[] {
    const context = ctx as SelectContext;
    const keyRanges = this.keyRangeCalculator.getKeyRangeCombinations(context);
    const index = this.indexStore.get(
      this.index.getNormalizedName()
    ) as RuntimeIndex;
    let rowIds: number[];
    if (
      keyRanges.length === 1 &&
      keyRanges[0] instanceof SingleKeyRange &&
      (keyRanges[0] as SingleKeyRange).isOnly()
    ) {
      rowIds = IndexHelper.slice(
        index.get((keyRanges[0] as SingleKeyRange).from as Key),
        false, // Single key will never reverse order.
        this.useLimit ? context.limit : undefined,
        this.useSkip ? context.skip : undefined
      );
    } else {
      rowIds = index.getRange(
        keyRanges,
        this.reverseOrder,
        this.useLimit ? context.limit : undefined,
        this.useSkip ? context.skip : undefined
      );
    }

    const rows = rowIds.map(rowId => new Row(rowId, {}));

    return [Relation.fromRows(rows, [this.index.tableName])];
  }
}
