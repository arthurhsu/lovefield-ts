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

describe('Eval_string', () => {
  let registry: EvalRegistry;

  before(() => {
    registry = EvalRegistry.get();
  });

  it('between', () => {
    const evaluationFn = registry.getEvaluator(Type.STRING, EvalType.BETWEEN);

    const string1 = 'a';
    const string2 = 'ab';
    const string3 = 'abc';
    const string4 = null;

    assert.isTrue(evaluationFn(string1, [string1, string3]));
    assert.isTrue(evaluationFn(string2, [string1, string3]));
    assert.isTrue(evaluationFn(string3, [string1, string3]));
    assert.isFalse(evaluationFn(string1, [string2, string3]));
    assert.isFalse(evaluationFn(string3, [string1, string2]));

    // Null tests.
    assert.isFalse(evaluationFn(string1, [string4, string3]));
    assert.isFalse(evaluationFn(string4, [string1, string3]));
    assert.isFalse(evaluationFn(string1, [string1, string4]));
    assert.isFalse(evaluationFn(string1, [string4, string1]));
    assert.isFalse(evaluationFn(string4, [string4, string4]));
    assert.isFalse(evaluationFn(string1, [string4, string4]));
  });

  it('eq', () => {
    const evaluationFn = registry.getEvaluator(Type.STRING, EvalType.EQ);

    const string1 = 'a';
    const string2 = 'a';
    const string3 = 'abc';

    assert.isTrue(evaluationFn(string1, string2));
    assert.isFalse(evaluationFn(string1, string3));
  });

  it('gte', () => {
    const evaluationFn = registry.getEvaluator(Type.STRING, EvalType.GTE);

    const string1 = 'a';
    const string2 = 'ab';
    const string3 = null;

    assert.isTrue(evaluationFn(string2, string1));
    assert.isTrue(evaluationFn(string2, string2));
    assert.isFalse(evaluationFn(string1, string2));

    // Null tests.
    assert.isFalse(evaluationFn(string3, string1));
    assert.isFalse(evaluationFn(string3, string2));
    assert.isFalse(evaluationFn(string1, string3));
    assert.isFalse(evaluationFn(string2, string3));
    assert.isFalse(evaluationFn(string3, string3));
  });

  it('gt', () => {
    const evaluationFn = registry.getEvaluator(Type.STRING, EvalType.GT);

    const string1 = 'a';
    const string2 = 'ab';
    const string3 = null;

    assert.isTrue(evaluationFn(string2, string1));
    assert.isFalse(evaluationFn(string2, string2));
    assert.isFalse(evaluationFn(string1, string2));

    // Null tests.
    assert.isFalse(evaluationFn(string3, string1));
    assert.isFalse(evaluationFn(string3, string2));
    assert.isFalse(evaluationFn(string1, string3));
    assert.isFalse(evaluationFn(string2, string3));
    assert.isFalse(evaluationFn(string3, string3));
  });

  it('in', () => {
    const evaluationFn = registry.getEvaluator(Type.STRING, EvalType.IN);

    const string1 = 'a';
    const string2 = 'ab';
    const string3 = 'abc';
    const string4 = 'abc';
    const string5 = 'abcd';

    const values = [string1, string2, string3];

    assert.isTrue(evaluationFn(string1, values));
    assert.isTrue(evaluationFn(string2, values));
    assert.isTrue(evaluationFn(string3, values));
    assert.isTrue(evaluationFn(string4, values));
    assert.isFalse(evaluationFn(string5, values));
  });

  it('lte', () => {
    const evaluationFn = registry.getEvaluator(Type.STRING, EvalType.LTE);

    const string1 = 'a';
    const string2 = 'ab';
    const string3 = null;

    assert.isTrue(evaluationFn(string1, string2));
    assert.isTrue(evaluationFn(string1, string1));
    assert.isFalse(evaluationFn(string2, string1));

    // Null tests.
    assert.isFalse(evaluationFn(string3, string1));
    assert.isFalse(evaluationFn(string3, string2));
    assert.isFalse(evaluationFn(string1, string3));
    assert.isFalse(evaluationFn(string2, string3));
    assert.isFalse(evaluationFn(string3, string3));
  });

  it('lt', () => {
    const evaluationFn = registry.getEvaluator(Type.STRING, EvalType.LT);

    const string1 = 'a';
    const string2 = 'ab';
    const string3 = null;

    assert.isTrue(evaluationFn(string1, string2));
    assert.isFalse(evaluationFn(string1, string1));
    assert.isFalse(evaluationFn(string2, string1));

    // Null tests.
    assert.isFalse(evaluationFn(string3, string1));
    assert.isFalse(evaluationFn(string3, string2));
    assert.isFalse(evaluationFn(string1, string3));
    assert.isFalse(evaluationFn(string2, string3));
    assert.isFalse(evaluationFn(string3, string3));
  });

  it('neq', () => {
    const evaluationFn = registry.getEvaluator(Type.STRING, EvalType.NEQ);

    const string1 = 'a';
    const string2 = 'ab';

    assert.isTrue(evaluationFn(string1, string2));
    assert.isTrue(evaluationFn(string2, string1));
    assert.isFalse(evaluationFn(string1, string1));
  });

  it('match', () => {
    const evaluationFn = registry.getEvaluator(Type.STRING, EvalType.MATCH);

    const string1 = 'sampleName';
    const string2 = null;

    const pattern1 = /sampleName/;
    const pattern2 = /\bsample[A-Za-z]+\b/;
    const pattern3 = /SAMPLENAME/;
    const pattern4 = null;

    assert.isTrue(evaluationFn(string1, pattern1));
    assert.isTrue(evaluationFn(string1, pattern2));
    assert.isFalse(evaluationFn(string1, pattern3));

    // Null tests.
    assert.isFalse(evaluationFn(string1, pattern4));
    assert.isFalse(evaluationFn(string2, pattern1));
    assert.isFalse(evaluationFn(string2, pattern4));
  });
});
