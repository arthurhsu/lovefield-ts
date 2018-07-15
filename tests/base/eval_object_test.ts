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
import {ErrorCode, Type} from '../../lib/base/enum';
import {EvalRegistry, EvalType} from '../../lib/base/eval';
import {TestUtil} from '../../testing/test_util';

const assert = chai.assert;

describe('EvalObject', () => {
  let registry: EvalRegistry;

  before(() => {
    registry = new EvalRegistry();
  });

  it('eq', () => {
    const evaluationFn = registry.getEvaluator(Type.OBJECT, EvalType.EQ);

    const obj1 = null;
    const obj2 = {};
    assert.isTrue(evaluationFn(obj1, null));
    assert.isFalse(evaluationFn(obj2, null));

    // 550: where() clause includes an invalid predicate.
    TestUtil.assertThrowsError(
        ErrorCode.INVALID_PREDICATE, () => evaluationFn({}, {}));
  });

  it('neq', () => {
    const evaluationFn = registry.getEvaluator(Type.OBJECT, EvalType.NEQ);

    const obj1 = null;
    const obj2 = {};
    assert.isFalse(evaluationFn(obj1, null));
    assert.isTrue(evaluationFn(obj2, null));

    // 550: where() clause includes an invalid predicate.
    TestUtil.assertThrowsError(
        ErrorCode.INVALID_PREDICATE, () => evaluationFn({}, {}));
  });
});
