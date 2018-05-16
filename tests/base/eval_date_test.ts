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

describe('Eval_date', () => {
  let registry: EvalRegistry;

  before(() => {
    registry = EvalRegistry.get();
  });

  it('between', () => {
    const evaluationFn =
        registry.getEvaluator(Type.DATE_TIME, EvalType.BETWEEN);

    const date1 = new Date();
    const date2 = new Date(date1.getTime() + 10);
    const date3 = new Date(date1.getTime() + 20);
    const date4: Date = null as any as Date;

    assert.isTrue(evaluationFn(date2, [date1, date3]));
    assert.isFalse(evaluationFn(date1, [date2, date3]));
    assert.isFalse(evaluationFn(date3, [date1, date2]));

    // Null tests.
    assert.isFalse(evaluationFn(date1, [date4, date3]));
    assert.isFalse(evaluationFn(date4, [date1, date3]));
    assert.isFalse(evaluationFn(date1, [date1, date4]));
    assert.isFalse(evaluationFn(date1, [date4, date1]));
    assert.isFalse(evaluationFn(date4, [date4, date4]));
    assert.isFalse(evaluationFn(date1, [date4, date4]));
  });

  it('eq', () => {
    const evaluationFn = registry.getEvaluator(Type.DATE_TIME, EvalType.EQ);

    const date1 = new Date();
    const date2 = new Date(date1.getTime());
    const date3 = new Date(date1.getTime() + 10);

    assert.isTrue(evaluationFn(date1, date2));
    assert.isFalse(evaluationFn(date1, date3));
  });

  it('gte', () => {
    const evaluationFn = registry.getEvaluator(Type.DATE_TIME, EvalType.GTE);

    const date1 = new Date();
    const date2 = new Date(date1.getTime() + 10);
    const date3: Date = null as any as Date;

    assert.isTrue(evaluationFn(date2, date1));
    assert.isTrue(evaluationFn(date2, date2));
    assert.isFalse(evaluationFn(date1, date2));

    // Null tests.
    assert.isFalse(evaluationFn(date3, date1));
    assert.isFalse(evaluationFn(date3, date2));
    assert.isFalse(evaluationFn(date1, date3));
    assert.isFalse(evaluationFn(date2, date3));
    assert.isFalse(evaluationFn(date3, date3));
  });

  it('gt', () => {
    const evaluationFn = registry.getEvaluator(Type.DATE_TIME, EvalType.GT);

    const date1 = new Date();
    const date2 = new Date(date1.getTime() + 10);
    const date3: Date = null as any as Date;

    assert.isTrue(evaluationFn(date2, date1));
    assert.isFalse(evaluationFn(date2, date2));
    assert.isFalse(evaluationFn(date1, date2));

    // Null tests.
    assert.isFalse(evaluationFn(date3, date1));
    assert.isFalse(evaluationFn(date3, date2));
    assert.isFalse(evaluationFn(date1, date3));
    assert.isFalse(evaluationFn(date2, date3));
    assert.isFalse(evaluationFn(date3, date3));
  });

  it('in', () => {
    const evaluationFn = registry.getEvaluator(Type.DATE_TIME, EvalType.IN);

    const date1 = new Date();
    const date2 = new Date(date1.getTime() + 10);
    const date3 = new Date(date1.getTime() + 20);
    const date4 = new Date(date3.getTime());
    const date5 = new Date(date1.getTime() + 15);
    const values = [date1, date2, date3];

    assert.isTrue(evaluationFn(date1, values));
    assert.isTrue(evaluationFn(date2, values));
    assert.isTrue(evaluationFn(date3, values));
    assert.isTrue(evaluationFn(date4, values));
    assert.isFalse(evaluationFn(date5, values));
  });

  it('lte', () => {
    const evaluationFn = registry.getEvaluator(Type.DATE_TIME, EvalType.LTE);

    const date1 = new Date();
    const date2 = new Date(date1.getTime() + 10);
    const date3: Date = null as any as Date;

    assert.isTrue(evaluationFn(date1, date2));
    assert.isTrue(evaluationFn(date1, date1));
    assert.isFalse(evaluationFn(date2, date1));

    // Null tests.
    assert.isFalse(evaluationFn(date3, date1));
    assert.isFalse(evaluationFn(date3, date2));
    assert.isFalse(evaluationFn(date1, date3));
    assert.isFalse(evaluationFn(date2, date3));
    assert.isFalse(evaluationFn(date3, date3));
  });

  it('lt', () => {
    const evaluationFn = registry.getEvaluator(Type.DATE_TIME, EvalType.LT);

    const date1 = new Date();
    const date2 = new Date(date1.getTime() + 10);
    const date3: Date = null as any as Date;

    assert.isTrue(evaluationFn(date1, date2));
    assert.isFalse(evaluationFn(date1, date1));
    assert.isFalse(evaluationFn(date2, date1));

    // Null tests.
    assert.isFalse(evaluationFn(date3, date1));
    assert.isFalse(evaluationFn(date3, date2));
    assert.isFalse(evaluationFn(date1, date3));
    assert.isFalse(evaluationFn(date2, date3));
    assert.isFalse(evaluationFn(date3, date3));
  });

  it('neq', () => {
    const evaluationFn = registry.getEvaluator(Type.DATE_TIME, EvalType.NEQ);

    const date1 = new Date();
    const date2 = new Date(date1.getTime() + 10);

    assert.isTrue(evaluationFn(date1, date2));
    assert.isTrue(evaluationFn(date2, date1));
    assert.isFalse(evaluationFn(date1, date1));
  });
});
