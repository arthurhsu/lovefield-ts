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

import { KeyRange, SingleKeyRange } from '../../index/key_range';
import { Context } from '../../query/context';
import { IndexImpl } from '../../schema/index_impl';
import { IndexKeyRangeCalculator } from './index_key_range_calculator';

export class UnboundedKeyRangeCalculator implements IndexKeyRangeCalculator {
  constructor(private indexSchema: IndexImpl) {}

  getKeyRangeCombinations(
    queryContext: Context
  ): SingleKeyRange[] | KeyRange[] {
    return this.indexSchema.columns.length === 1
      ? [SingleKeyRange.all()]
      : [this.indexSchema.columns.map(col => SingleKeyRange.all())];
  }
}
