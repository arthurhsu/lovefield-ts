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

import {Operator} from '../base/private_enum';
import {CombinedPredicate} from '../pred/combined_predicate';
import {Predicate} from '../pred/predicate';
import {PredicateNode} from '../pred/predicate_node';

// Keep lower case class name for compatibility with Lovefield API.
// TODO(arthurhsu): FIXME: use public interface.
// @export
export class op {
  static and(...predicates: Predicate[]): Predicate {
    return op.createPredicate(Operator.AND, predicates as PredicateNode[]);
  }

  static or(...predicates: Predicate[]): Predicate {
    return op.createPredicate(Operator.OR, predicates as PredicateNode[]);
  }

  static not(operand: Predicate): Predicate {
    operand.setComplement(true);
    return operand;
  }

  private static createPredicate(
    operator: Operator,
    predicates: PredicateNode[]
  ): Predicate {
    const condition = new CombinedPredicate(operator);
    predicates.forEach(predicate => condition.addChild(predicate));
    return condition;
  }
}
