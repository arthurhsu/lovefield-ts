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

import {Binder} from '../base/bind';
import {ErrorCode} from '../base/enum';
import {Exception} from '../base/exception';
import {OperandType, ValueOperandType} from '../pred/operand_type';
import {Predicate} from '../pred/predicate';
import {PredicateProvider} from '../pred/predicate_provider';

// Base class for AggregateColumn and StarColumn which does not support
// PredicateProvider interface.
export class NonPredicateProvider implements PredicateProvider {
  public eq(operand: OperandType): Predicate {
    throw new Exception(ErrorCode.SYNTAX_ERROR);
  }

  public neq(operand: OperandType): Predicate {
    throw new Exception(ErrorCode.SYNTAX_ERROR);
  }

  public lt(operand: OperandType): Predicate {
    throw new Exception(ErrorCode.SYNTAX_ERROR);
  }

  public lte(operand: OperandType): Predicate {
    throw new Exception(ErrorCode.SYNTAX_ERROR);
  }

  public gt(operand: OperandType): Predicate {
    throw new Exception(ErrorCode.SYNTAX_ERROR);
  }

  public gte(operand: OperandType): Predicate {
    throw new Exception(ErrorCode.SYNTAX_ERROR);
  }

  public match(operand: Binder|RegExp): Predicate {
    throw new Exception(ErrorCode.SYNTAX_ERROR);
  }

  public between(from: ValueOperandType, to: ValueOperandType): Predicate {
    throw new Exception(ErrorCode.SYNTAX_ERROR);
  }

  public in(values: Binder|ValueOperandType[]): Predicate {
    throw new Exception(ErrorCode.SYNTAX_ERROR);
  }

  public isNull(): Predicate {
    throw new Exception(ErrorCode.SYNTAX_ERROR);
  }

  public isNotNull(): Predicate {
    throw new Exception(ErrorCode.SYNTAX_ERROR);
  }
}
