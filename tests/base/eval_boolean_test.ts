/**
 * Copyright 2016 The Lovefield Project Authors. All Rights Reserved.
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
import { Type } from '../../lib/base/enum';
import { EvalRegistry, EvalType } from '../../lib/base/eval';

const assert = chai.assert;

describe('Eval_boolean', () => {
  let registry: EvalRegistry;

  before(() => {
    registry = EvalRegistry.get();
  });

  it('eq', () => {
    const evaluationFn = registry.getEvaluator(Type.BOOLEAN, EvalType.EQ);
    const boolean1 = true;
    const boolean2 = true;
    const boolean3 = false;

    assert.isTrue(evaluationFn(boolean1, boolean2));
    assert.isFalse(evaluationFn(boolean1, boolean3));
  });

  it('neq', () => {
    const evaluationFn = registry.getEvaluator(Type.BOOLEAN, EvalType.NEQ);
    const boolean1 = true;
    const boolean2 = false;

    assert.isTrue(evaluationFn(boolean1, boolean2));
    assert.isTrue(evaluationFn(boolean2, boolean1));
    assert.isFalse(evaluationFn(boolean1, boolean1));
  });
});
