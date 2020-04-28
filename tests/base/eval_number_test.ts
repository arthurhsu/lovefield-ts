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
import {Type} from '../../lib/base/enum';
import {EvalRegistry, EvalType} from '../../lib/base/eval';

const assert = chai.assert;

describe('Eval_number', () => {
  let registry: EvalRegistry;

  before(() => {
    registry = EvalRegistry.get();
  });

  it('between', () => {
    const evaluationFn = registry.getEvaluator(Type.NUMBER, EvalType.BETWEEN);

    const number1 = 1;
    const number2 = 50;
    const number3 = 100;
    const number4 = (null as unknown) as number;

    assert.isTrue(evaluationFn(number1, [number1, number3]));
    assert.isTrue(evaluationFn(number2, [number1, number3]));
    assert.isTrue(evaluationFn(number3, [number1, number3]));
    assert.isFalse(evaluationFn(number1, [number2, number3]));
    assert.isFalse(evaluationFn(number3, [number1, number2]));

    // Null tests.
    assert.isFalse(evaluationFn(number1, [number4, number3]));
    assert.isFalse(evaluationFn(number4, [number1, number3]));
    assert.isFalse(evaluationFn(number1, [number1, number4]));
    assert.isFalse(evaluationFn(number1, [number4, number1]));
    assert.isFalse(evaluationFn(number4, [number4, number4]));
    assert.isFalse(evaluationFn(number1, [number4, number4]));
  });

  it('eq', () => {
    const evaluationFn = registry.getEvaluator(Type.NUMBER, EvalType.EQ);

    const number1 = 100;
    const number2 = 100;
    const number3 = 200;

    assert.isTrue(evaluationFn(number1, number2));
    assert.isFalse(evaluationFn(number1, number3));
  });

  it('gte', () => {
    const evaluationFn = registry.getEvaluator(Type.NUMBER, EvalType.GTE);

    const number1 = 100;
    const number2 = 200;
    const number3 = (null as unknown) as number;

    assert.isTrue(evaluationFn(number2, number1));
    assert.isTrue(evaluationFn(number2, number2));
    assert.isFalse(evaluationFn(number1, number2));

    // Null tests.
    assert.isFalse(evaluationFn(number3, number1));
    assert.isFalse(evaluationFn(number3, number2));
    assert.isFalse(evaluationFn(number1, number3));
    assert.isFalse(evaluationFn(number2, number3));
    assert.isFalse(evaluationFn(number3, number3));
  });

  it('gt', () => {
    const evaluationFn = registry.getEvaluator(Type.NUMBER, EvalType.GT);

    const number1 = 100;
    const number2 = 200;
    const number3 = (null as unknown) as number;

    assert.isTrue(evaluationFn(number2, number1));
    assert.isFalse(evaluationFn(number2, number2));
    assert.isFalse(evaluationFn(number1, number2));

    // Null tests.
    assert.isFalse(evaluationFn(number3, number1));
    assert.isFalse(evaluationFn(number3, number2));
    assert.isFalse(evaluationFn(number1, number3));
    assert.isFalse(evaluationFn(number2, number3));
    assert.isFalse(evaluationFn(number3, number3));
  });

  it('in', () => {
    const evaluationFn = registry.getEvaluator(Type.NUMBER, EvalType.IN);

    const number1 = 1;
    const number2 = 10;
    const number3 = 20;
    const number4 = 15;
    const values = [number1, number2, number3];

    assert.isTrue(evaluationFn(number1, values));
    assert.isTrue(evaluationFn(number2, values));
    assert.isTrue(evaluationFn(number3, values));
    assert.isFalse(evaluationFn(number4, values));
  });

  it('lte', () => {
    const evaluationFn = registry.getEvaluator(Type.NUMBER, EvalType.LTE);

    const number1 = 100;
    const number2 = 200;
    const number3 = (null as unknown) as number;

    assert.isTrue(evaluationFn(number1, number2));
    assert.isTrue(evaluationFn(number1, number1));
    assert.isFalse(evaluationFn(number2, number1));

    // Null tests.
    assert.isFalse(evaluationFn(number3, number1));
    assert.isFalse(evaluationFn(number3, number2));
    assert.isFalse(evaluationFn(number1, number3));
    assert.isFalse(evaluationFn(number2, number3));
    assert.isFalse(evaluationFn(number3, number3));
  });

  it('lt', () => {
    const evaluationFn = registry.getEvaluator(Type.NUMBER, EvalType.LT);

    const number1 = 100;
    const number2 = 200;
    const number3 = (null as unknown) as number;

    assert.isTrue(evaluationFn(number1, number2));
    assert.isFalse(evaluationFn(number1, number1));
    assert.isFalse(evaluationFn(number2, number1));

    // Null tests.
    assert.isFalse(evaluationFn(number3, number1));
    assert.isFalse(evaluationFn(number3, number2));
    assert.isFalse(evaluationFn(number1, number3));
    assert.isFalse(evaluationFn(number2, number3));
    assert.isFalse(evaluationFn(number3, number3));
  });

  it('neq', () => {
    const evaluationFn = registry.getEvaluator(Type.NUMBER, EvalType.NEQ);

    const number1 = 100;
    const number2 = 200;

    assert.isTrue(evaluationFn(number1, number2));
    assert.isTrue(evaluationFn(number2, number1));
    assert.isFalse(evaluationFn(number1, number1));
  });
});
