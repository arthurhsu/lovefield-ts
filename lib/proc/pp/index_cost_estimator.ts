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
import {Service} from '../../base/service';
import {IndexStore} from '../../index/index_store';
import {RuntimeIndex} from '../../index/runtime_index';
import {CombinedPredicate} from '../../pred/combined_predicate';
import {Predicate} from '../../pred/predicate';
import {ValuePredicate} from '../../pred/value_predicate';
import {Context} from '../../query/context';
import {BaseTable} from '../../schema/base_table';
import {Table} from '../../schema/table';
import {IndexImpl} from '../../schema/index_impl';

import {IndexRangeCandidate} from './index_range_candidate';

// The maximum percent of
// 1) values an EvalType.IN predicate can have or
// 2) children an OR CombinedPredicate can have
// to still be considered for leveraging an index, with respect to the total
// number of rows in the table.
// For each one of the values/children an index query will be performed, so the
// trade-off here is that too many index queries can be slower than simply doing
// a full table scan. This constant has been determined by trial and error.
const INDEX_QUERY_THRESHOLD_PERCENT = 0.02;

export class IndexCostEstimator {
  private indexStore: IndexStore;

  constructor(global: Global, private tableSchema: Table) {
    this.indexStore = global.getService(Service.INDEX_STORE);
  }

  chooseIndexFor(
    queryContext: Context,
    predicates: Predicate[]
  ): IndexRangeCandidate | null {
    const candidatePredicates = predicates.filter(this.isCandidate, this);
    if (candidatePredicates.length === 0) {
      return null;
    }

    const indexRangeCandidates = this.generateIndexRangeCandidates(
      candidatePredicates as ValuePredicate[]
    );
    if (indexRangeCandidates.length === 0) {
      return null;
    }

    // If there is only one candidate there is no need to evaluate the cost.
    if (indexRangeCandidates.length === 1) {
      return indexRangeCandidates[0];
    }

    let minCost = Number.MAX_VALUE;
    return indexRangeCandidates.reduce(
      (prev: IndexRangeCandidate | null, curr: IndexRangeCandidate) => {
        const cost = curr.calculateCost(queryContext);
        if (cost < minCost) {
          minCost = cost;
          return curr;
        }
        return prev;
      },
      null
    );
  }

  // Returns the number of Index#getRange queries that can be performed faster
  // than scanning the entire table instead.
  private getIndexQueryThreshold(): number {
    const rowIdIndex = this.indexStore.get(
      (this.tableSchema as BaseTable).getRowIdIndexName()
    ) as RuntimeIndex;
    return Math.floor(
      rowIdIndex.stats().totalRows * INDEX_QUERY_THRESHOLD_PERCENT
    );
  }

  private generateIndexRangeCandidates(
    predicates: ValuePredicate[]
  ): IndexRangeCandidate[] {
    const indexSchemas = (this
      .tableSchema as BaseTable).getIndices() as IndexImpl[];
    return indexSchemas
      .map(indexSchema => {
        const indexRangeCandidate = new IndexRangeCandidate(
          this.indexStore,
          indexSchema
        );
        indexRangeCandidate.consumePredicates(predicates);
        return indexRangeCandidate;
      }, this)
      .filter(indexRangeCandidate => indexRangeCandidate.isUsable());
  }

  private isCandidate(predicate: Predicate): boolean {
    if (predicate instanceof ValuePredicate) {
      return this.isCandidateValuePredicate(predicate);
    } else if (predicate instanceof CombinedPredicate) {
      return this.isCandidateCombinedPredicate(predicate);
    } else {
      return false;
    }
  }

  private isCandidateCombinedPredicate(predicate: CombinedPredicate): boolean {
    if (!predicate.isKeyRangeCompatible()) {
      return false;
    }

    const predicateColumn = (predicate.getChildAt(0) as ValuePredicate).column;
    if (predicateColumn.getTable() !== this.tableSchema) {
      return false;
    }

    return predicate.getChildCount() <= this.getIndexQueryThreshold();
  }

  private isCandidateValuePredicate(predicate: ValuePredicate): boolean {
    if (
      !predicate.isKeyRangeCompatible() ||
      predicate.column.getTable() !== this.tableSchema
    ) {
      return false;
    }

    if (
      predicate.evaluatorType === EvalType.IN &&
      (predicate.peek() as unknown[]).length > this.getIndexQueryThreshold()
    ) {
      return false;
    }

    return true;
  }
}
