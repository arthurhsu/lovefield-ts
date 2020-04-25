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

import {assert} from '../../base/assert';
import {IndexStore} from '../../index/index_store';
import {Range} from '../../index/key_range';
import {RuntimeIndex} from '../../index/runtime_index';
import {ValuePredicate} from '../../pred/value_predicate';
import {Context} from '../../query/context';
import {IndexImpl} from '../../schema/index_impl';
import {MapSet} from '../../structs/map_set';

import {BoundedKeyRangeCalculator} from './bounded_key_range_calculator';
import {IndexKeyRangeCalculator} from './index_key_range_calculator';

export class IndexRangeCandidate {
  // The names of all columns that are indexed by this index schema.
  private indexedColumnNames: Set<string>;

  // A map where a key is the name of an indexed column and the values are
  // predicates IDs that correspond to that column. It is initialized lazily,
  // only if a predicate that matches a column of this index schema is found.
  private predicateMap: MapSet<string, number> | null;

  // The calculator object to be used for generating key ranges based on a given
  // query context. This object will be used by the IndexRangeScanStep during
  // query execution. Initialized lazily.
  private keyRangeCalculator: IndexKeyRangeCalculator | null;

  constructor(private indexStore: IndexStore, readonly indexSchema: IndexImpl) {
    this.indexedColumnNames = new Set<string>(
      this.indexSchema.columns.map(col => col.schema.getName())
    );
    this.predicateMap = null;
    this.keyRangeCalculator = null;
  }

  // The predicates that were consumed by this candidate.
  getPredicateIds(): number[] {
    return this.predicateMap ? this.predicateMap.values() : [];
  }

  getKeyRangeCalculator(): IndexKeyRangeCalculator {
    assert(this.predicateMap !== null);

    if (this.keyRangeCalculator === null) {
      this.keyRangeCalculator = new BoundedKeyRangeCalculator(
        this.indexSchema,
        this.predicateMap as MapSet<string, number>
      );
    }
    return this.keyRangeCalculator as IndexKeyRangeCalculator;
  }

  // Finds which predicates are related to the index schema corresponding to
  // this IndexRangeCandidate.
  consumePredicates(predicates: ValuePredicate[]): void {
    predicates.forEach(predicate => {
      // If predicate is a ValuePredicate there in only one referred column. If
      // predicate is an OR CombinedPredicate, then it must be referring to a
      // single column (enforced by isKeyRangeCompatible()).
      const columnName = predicate.getColumns()[0].getName();
      if (this.indexedColumnNames.has(columnName)) {
        if (this.predicateMap === null) {
          this.predicateMap = new MapSet<string, number>();
        }
        this.predicateMap.set(columnName, predicate.getId());
      }
    }, this);
  }

  // Whether this candidate can actually be used for an IndexRangeScanStep
  // optimization. Sometimes after building the candidate it turns out that it
  // cannot be used. For example consider a cross column index on columns
  // ['A', 'B'] and a query that only binds the key range of the 2nd
  // dimension B.
  isUsable(): boolean {
    if (this.predicateMap === null) {
      // If the map was never initialized, it means that no predicate matched
      // this index schema columns.
      return false;
    }

    let unboundColumnFound = false;
    let isUsable = true;
    this.indexSchema.columns.every(column => {
      const isBound = (this.predicateMap as MapSet<string, number>).has(
        column.schema.getName()
      );
      if (unboundColumnFound && isBound) {
        isUsable = false;
        return false;
      }
      if (!isBound) {
        unboundColumnFound = true;
      }
      return true;
    }, this);

    return isUsable;
  }

  calculateCost(queryContext: Context): number {
    const combinations: Range[] = this.getKeyRangeCalculator().getKeyRangeCombinations(
      queryContext
    );
    const indexData = this.indexStore.get(
      this.indexSchema.getNormalizedName()
    ) as RuntimeIndex;

    return combinations.reduce((costSoFar: number, combination: Range) => {
      return costSoFar + indexData.cost(combination);
    }, 0);
  }
}
