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

import {EvalType} from '../../base/eval';
import {Global} from '../../base/global';
import {ExecType} from '../../base/private_enum';
import {Service} from '../../base/service';
import {Cache} from '../../cache/cache';
import {Journal} from '../../cache/journal';
import {IndexStore} from '../../index/index_store';
import {RuntimeIndex} from '../../index/runtime_index';
import {IndexJoinInfo, JoinPredicate} from '../../pred/join_predicate';
import {Context} from '../../query/context';
import {BaseColumn} from '../../schema/base_column';
import {Column} from '../../schema/column';
import {Index} from '../../schema/index';
import {Relation} from '../relation';

import {PhysicalQueryPlanNode} from './physical_query_plan_node';

/* eslint-disable no-unused-vars */
// The enum is used but eslint will still complain, so just disable it.
enum JoinAlgorithm {
  HASH = 'hash',
  INDEX_NESTED_LOOP = 'index_nested_loop',
  NESTED_LOOP = 'nested_loop',
}
/* eslint-enable */

export class JoinStep extends PhysicalQueryPlanNode {
  private indexStore: IndexStore;
  private cache: Cache;
  private algorithm: JoinAlgorithm;
  private indexJoinInfo: IndexJoinInfo;

  constructor(
    global: Global,
    readonly predicate: JoinPredicate,
    readonly isOuterJoin: boolean
  ) {
    super(2, ExecType.ALL);

    this.indexStore = global.getService(Service.INDEX_STORE);
    this.cache = global.getService(Service.CACHE);
    this.algorithm =
      this.predicate.evaluatorType === EvalType.EQ
        ? JoinAlgorithm.HASH
        : JoinAlgorithm.NESTED_LOOP;
    this.indexJoinInfo = null as unknown as IndexJoinInfo;
  }

  toString(): string {
    return (
      `join(type: ${this.isOuterJoin ? 'outer' : 'inner'}, ` +
      `impl: ${this.algorithm}, ${this.predicate.toString()})`
    );
  }

  execInternal(
    relations: Relation[],
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    journal?: Journal,
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    context?: Context
  ): Relation[] {
    switch (this.algorithm) {
      case JoinAlgorithm.HASH:
        return [
          this.predicate.evalRelationsHashJoin(
            relations[0],
            relations[1],
            this.isOuterJoin
          ),
        ];
      case JoinAlgorithm.INDEX_NESTED_LOOP:
        return [
          this.predicate.evalRelationsIndexNestedLoopJoin(
            relations[0],
            relations[1],
            this.indexJoinInfo,
            this.cache
          ),
        ];
      default:
        // JoinAlgorithm.NESTED_LOOP
        return [
          this.predicate.evalRelationsNestedLoopJoin(
            relations[0],
            relations[1],
            this.isOuterJoin
          ),
        ];
    }
  }

  // Indicates that this JoinStep should be executed as an INDEX_NESTED_LOOP
  // join. |column| is the column whose index should be queried.
  markAsIndexJoin(column: Column): void {
    this.algorithm = JoinAlgorithm.INDEX_NESTED_LOOP;
    const index = this.indexStore.get(
      ((column as BaseColumn).getIndex() as Index).getNormalizedName()
    );
    this.indexJoinInfo = {
      index: index as RuntimeIndex,
      indexedColumn: column,
      nonIndexedColumn:
        column === this.predicate.leftColumn
          ? this.predicate.rightColumn
          : this.predicate.leftColumn,
    };
  }
}
