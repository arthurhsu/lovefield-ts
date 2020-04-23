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

import { Order } from '../base/enum';
import { Favor } from '../base/private_enum';
import { SingleKey, SingleKeyRange } from './key_range';
import { SimpleComparator } from './simple_comparator';

// This comparator is not used to replace existing NullableIndex wrapper
// because of its compareAscending function requires extra null key
// checking every time, where the wrapper does it only once. This resulted in
// performance difference and therefore the NullableIndex is kept.
export class SimpleComparatorWithNull extends SimpleComparator {
  static compareAscending(lhs: SingleKey, rhs: SingleKey): Favor {
    if (lhs === null) {
      return rhs === null ? Favor.TIE : Favor.RHS;
    }
    return rhs === null
      ? Favor.LHS
      : SimpleComparator.compareAscending(lhs, rhs);
  }

  static compareDescending(lhs: SingleKey, rhs: SingleKey): Favor {
    return SimpleComparatorWithNull.compareAscending(rhs, lhs);
  }

  constructor(order: Order) {
    super(order);

    this.compareFn =
      order === Order.DESC
        ? SimpleComparatorWithNull.compareDescending
        : SimpleComparatorWithNull.compareAscending;
  }

  isInRange(key: SingleKey, range: SingleKeyRange): boolean {
    return key === null ? range.isAll() : super.isInRange(key, range);
  }

  min(lhs: SingleKey, rhs: SingleKey): Favor {
    const results = this.minMax(lhs, rhs);
    return results === null ? super.min(lhs, rhs) : results;
  }

  max(lhs: SingleKey, rhs: SingleKey): Favor {
    const results = this.minMax(lhs, rhs);
    return results === null ? super.max(lhs, rhs) : results;
  }

  private minMax(lhs: SingleKey, rhs: SingleKey): Favor | null {
    if (lhs === null) {
      return rhs === null ? Favor.TIE : Favor.RHS;
    }
    return rhs === null ? Favor.LHS : null;
  }
}
