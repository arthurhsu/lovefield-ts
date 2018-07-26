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
import {KeyRange, SingleKeyRange} from '../../index/key_range';
import {SingleKeyRangeSet} from '../../index/single_key_range_set';
import {CombinedPredicate} from '../../pred/combined_predicate';
import {ValuePredicate} from '../../pred/value_predicate';
import {Context} from '../../query/context';
import {IndexImpl} from '../../schema/index_impl';
import {ArrayHelper} from '../../structs/array_helper';
import {MapSet} from '../../structs/map_set';
import {IndexKeyRangeCalculator} from './index_key_range_calculator';

export class BoundedKeyRangeCalculator implements IndexKeyRangeCalculator {
  // The query context that was used for calculating the cached key range
  // combinations.
  private lastQueryContext: Context;

  // Caching the keyRange combinations such that they don't need to be
  // calculated twice, in the case where the same query context is used.
  private combinations: KeyRange[]|SingleKeyRange[];

  // |this.predicateMap| is a map where a key is the name of an indexed column
  // and the values are predicates IDs that correspond to that column. The IDs
  // are used to grab the actual predicates from the given query context, such
  // that this calculator can be re-used with different query contexts.
  constructor(
      private indexSchema: IndexImpl,
      private predicateMap: MapSet<string, number>) {
    this.lastQueryContext = null as any as Context;
    this.combinations = null as any as (KeyRange[] | SingleKeyRange[]);
  }

  public getKeyRangeCombinations(queryContext: Context):
      SingleKeyRange[]|KeyRange[] {
    if (this.lastQueryContext === queryContext) {
      return this.combinations;
    }

    const keyRangeMap = this.calculateKeyRangeMap(queryContext);
    this.fillMissingKeyRanges(keyRangeMap);

    // If this IndexRangeCandidate refers to a single column index there is no
    // need to perform cartesian product, since there is only one dimension.
    this.combinations = this.indexSchema.columns.length === 1 ?
        Array.from(keyRangeMap.values())[0].getValues() :
        this.calculateCartesianProduct(this.getSortedKeyRangeSets(keyRangeMap));
    this.lastQueryContext = queryContext;

    return this.combinations;
  }

  // Builds a map where a key is an indexed column name and the value is
  // the SingleKeyRangeSet, created by considering all provided predicates.
  private calculateKeyRangeMap(queryContext: Context):
      Map<string, SingleKeyRangeSet> {
    const keyRangeMap = new Map<string, SingleKeyRangeSet>();

    Array.from(this.predicateMap.keys()).forEach((columnName) => {
      const predicateIds = this.predicateMap.get(columnName) as number[];
      const predicates = predicateIds.map((predicateId) => {
        return queryContext.getPredicate(predicateId);
      }, this) as Array<CombinedPredicate|ValuePredicate>;
      let keyRangeSetSoFar = new SingleKeyRangeSet([SingleKeyRange.all()]);
      predicates.forEach((predicate) => {
        keyRangeSetSoFar = SingleKeyRangeSet.intersect(
            keyRangeSetSoFar, predicate.toKeyRange());
      });
      keyRangeMap.set(columnName, keyRangeSetSoFar);
    }, this);

    return keyRangeMap;
  }

  // Traverses the indexed columns in reverse order and fills in an "all"
  // SingleKeyRangeSet where possible in the provided map.
  // Example1: Assume that the indexed columns are ['A', 'B', 'C'] and A is
  // already bound, but B and C are unbound. Key ranges for B and C will be
  // filled in with an "all" key range.
  // Example2: Assume that the indexed columns are ['A', 'B', 'C', 'D'] and A, C
  // are already bound, but B and D are unbound. Key ranges only for D will be
  // filled in. In practice such a case will have already been rejected by
  // IndexRangeCandidate#isUsable and should never occur here.
  private fillMissingKeyRanges(keyRangeMap: Map<string, SingleKeyRangeSet>):
      void {
    const getAllKeyRange = () => new SingleKeyRangeSet([SingleKeyRange.all()]);
    for (let i = this.indexSchema.columns.length - 1; i >= 0; i--) {
      const column = this.indexSchema.columns[i];
      const keyRangeSet = keyRangeMap.get(column.schema.getName()) || null;
      if (keyRangeSet !== null) {
        break;
      }
      keyRangeMap.set(column.schema.getName(), getAllKeyRange());
    }
  }

  // Sorts the key range sets corresponding to this index's columns according to
  // the column order of the index schema.
  private getSortedKeyRangeSets(keyRangeMap: Map<string, SingleKeyRangeSet>):
      SingleKeyRangeSet[] {
    const sortHelper = new Map<string, number>();
    let priority = 0;
    this.indexSchema.columns.forEach((column) => {
      sortHelper.set(column.schema.getName(), priority);
      priority++;
    });

    const sortedColumnNames =
        Array.from(keyRangeMap.keys())
            .sort(
                (a, b) => (sortHelper.get(a) || 0) - (sortHelper.get(b) || 0));

    return sortedColumnNames.map(
        (columnName) => keyRangeMap.get(columnName) as SingleKeyRangeSet);
  }

  // Finds the cartesian product of a collection of SingleKeyRangeSets.
  // |keyRangeSets| is a SingleKeyRangeSet at position i in the input array
  // corresponds to all possible values for the ith dimension in the
  // N-dimensional space (where N is the number of columns in the cross-column
  // index).
  // Returns the cross-column key range combinations.
  private calculateCartesianProduct(keyRangeSets: SingleKeyRangeSet[]):
      KeyRange[] {
    assert(
        keyRangeSets.length > 1,
        'Should only be called for cross-column indices.');

    const keyRangeSetsAsArrays =
        keyRangeSets.map((keyRangeSet) => keyRangeSet.getValues());
    return ArrayHelper.product(keyRangeSetsAsArrays);
  }
}
