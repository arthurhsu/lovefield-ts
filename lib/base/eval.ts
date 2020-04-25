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

import {ErrorCode, Type} from './enum';
import {Exception} from './exception';

export enum EvalType {
  BETWEEN = 'between',
  EQ = 'eq',
  GTE = 'gte',
  GT = 'gt',
  IN = 'in',
  LTE = 'lte',
  LT = 'lt',
  MATCH = 'match',
  NEQ = 'neq',
}

type ValueType = boolean | number | string;

// ComparisonFunction is a special type that needs to allow any.
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type ComparisonFunction = (l: any, r: any) => boolean;
export type IndexableType = ValueType | Date;
export type KeyOfIndexFunction = (key: IndexableType) => ValueType;

function identity<T>(value: T): T {
  return value;
}

/**
 * Builds a map associating evaluator types with the evaluator functions, for
 * the case of a column of type 'boolean'.
 * NOTE: lf.eval.Type.BETWEEN, MATCH, GTE, GT, LTE, LT, are not available for
 * boolean objects.
 */
function buildKeyOfIndexConversionMap(): Map<Type, KeyOfIndexFunction> {
  const map = new Map<Type, KeyOfIndexFunction>();
  map.set(Type.BOOLEAN, ((value: boolean) => {
    return value === null ? null : value ? 1 : 0;
  }) as KeyOfIndexFunction);
  map.set(Type.DATE_TIME, ((value: Date) => {
    return value === null ? null : value.getTime();
  }) as KeyOfIndexFunction);

  map.set(Type.INTEGER, identity as KeyOfIndexFunction);
  map.set(Type.NUMBER, identity as KeyOfIndexFunction);
  map.set(Type.STRING, identity as KeyOfIndexFunction);
  return map;
}

type RangeType = ValueType[];
type ListType = ValueType[];

/**
 * Builds a map associating evaluator types with the evaluator functions, for
 * the case of a column of type 'boolean'.
 * NOTE: lf.eval.Type.BETWEEN, MATCH, GTE, GT, LTE, LT, are not available for
 * boolean objects.
 */
function buildBooleanEvaluatorMap(): Map<EvalType, ComparisonFunction> {
  const map = new Map<EvalType, ComparisonFunction>();
  map.set(EvalType.EQ, (a: ValueType, b: ValueType) => a === b);
  map.set(EvalType.NEQ, (a: ValueType, b: ValueType) => a !== b);
  return map;
}

function buildCommonEvaluatorMap(): Map<EvalType, ComparisonFunction> {
  const map = buildBooleanEvaluatorMap();
  map.set(EvalType.BETWEEN, (a: ValueType, range: RangeType) => {
    return a === null || range[0] === null || range[1] === null
      ? false
      : a >= range[0] && a <= range[1];
  });
  map.set(EvalType.GTE, (a: ValueType, b: ValueType) => {
    return a === null || b === null ? false : a >= b;
  });
  map.set(EvalType.GT, (a: ValueType, b: ValueType) => {
    return a === null || b === null ? false : a > b;
  });
  map.set(EvalType.IN, (rowValue: ValueType, values: ListType) => {
    return values.indexOf(rowValue) !== -1;
  });
  map.set(EvalType.LTE, (a: ValueType, b: ValueType) => {
    return a === null || b === null ? false : a <= b;
  });
  map.set(EvalType.LT, (a: ValueType, b: ValueType) => {
    return a === null || b === null ? false : a < b;
  });
  return map;
}

/**
 * Builds a map associating evaluator types with the evaluator functions, for
 * the case of a column of type 'number'.
 * NOTE: lf.eval.Type.MATCH is not available for numbers.
 */
const buildNumberEvaluatorMap = buildCommonEvaluatorMap;

/**
 * Builds a map associating evaluator types with the evaluator functions, for
 * the case of a column of type 'string'.
 */
function buildStringEvaluatorMap(): Map<EvalType, ComparisonFunction> {
  const map = buildCommonEvaluatorMap();
  map.set(EvalType.MATCH, (value, regex) => {
    if (value === null || regex === null) {
      return false;
    }
    const re = new RegExp(regex);
    return re.test(value);
  });
  return map;
}

/**
 * Builds a map associating evaluator types with the evaluator functions, for
 * the case of a column of type 'Object'.
 * NOTE: Only lf.eval.Type.EQ and NEQ are available for objects.
 */
function buildObjectEvaluatorMap(): Map<EvalType, ComparisonFunction> {
  const map = new Map<EvalType, ComparisonFunction>();

  const checkNull = (value: object) => {
    if (value !== null) {
      // 550: where() clause includes an invalid predicate, can't compare
      // lf.Type.OBJECT to anything other than null.
      throw new Exception(ErrorCode.INVALID_PREDICATE);
    }
  };
  map.set(EvalType.EQ, (a: object, b: object) => {
    checkNull(b);
    return a === null;
  });
  map.set(EvalType.NEQ, (a: object, b: object) => {
    checkNull(b);
    return a !== null;
  });
  return map;
}

/**
 * Builds a map associating evaluator types with the evaluator functions, for
 * the case of a column of type 'Date'.
 * NOTE: lf.eval.Type.MATCH is not available for Date objects.
 */
function buildDateEvaluatorMap(): Map<EvalType, ComparisonFunction> {
  const map = new Map<EvalType, ComparisonFunction>();
  map.set(EvalType.BETWEEN, (a: Date, range: Date[]) => {
    return a === null || range[0] === null || range[1] === null
      ? false
      : a.getTime() >= range[0].getTime() && a.getTime() <= range[1].getTime();
  });
  map.set(EvalType.EQ, (a: Date, b: Date) => {
    const aTime = a === null ? -1 : a.getTime();
    const bTime = b === null ? -1 : b.getTime();
    return aTime === bTime;
  });
  map.set(EvalType.GTE, (a: Date, b: Date) => {
    return a === null || b === null ? false : a.getTime() >= b.getTime();
  });
  map.set(EvalType.GT, (a: Date, b: Date) => {
    return a === null || b === null ? false : a.getTime() > b.getTime();
  });
  map.set(EvalType.IN, (targetValue: Date, values: Date[]) => {
    return values.some(value => value.getTime() === targetValue.getTime());
  });
  map.set(EvalType.LTE, (a: Date, b: Date) => {
    return a === null || b === null ? false : a.getTime() <= b.getTime();
  });
  map.set(EvalType.LT, (a: Date, b: Date) => {
    return a === null || b === null ? false : a.getTime() < b.getTime();
  });
  map.set(EvalType.NEQ, (a: Date, b: Date) => {
    const aTime = a === null ? -1 : a.getTime();
    const bTime = b === null ? -1 : b.getTime();
    return aTime !== bTime;
  });
  return map;
}

export class EvalRegistry {
  static get(): EvalRegistry {
    EvalRegistry.instance = EvalRegistry.instance || new EvalRegistry();
    return EvalRegistry.instance;
  }

  private static instance: EvalRegistry;

  // A map holding functions used for converting a value of a given type to the
  // equivalent index key. NOTE: No functions exist in this map for
  // lf.Type.ARRAY_BUFFER and lf.Type.OBJECT, since columns of such types are
  // not indexable.
  private keyOfIndexConversionMap: Map<Type, KeyOfIndexFunction>;

  // A two-level map, associating a column type to the corresponding evaluation
  // functions map.
  // NOTE: No evaluation map exists for lf.Type.ARRAY_BUFFER since predicates
  // involving such a column do not make sense.
  private evalMaps: Map<Type, Map<EvalType, ComparisonFunction>>;

  constructor() {
    this.keyOfIndexConversionMap = buildKeyOfIndexConversionMap();
    this.evalMaps = new Map<Type, Map<EvalType, ComparisonFunction>>();
    const numberOrIntegerEvalMap = buildNumberEvaluatorMap();

    this.evalMaps.set(Type.BOOLEAN, buildBooleanEvaluatorMap());
    this.evalMaps.set(Type.DATE_TIME, buildDateEvaluatorMap());
    this.evalMaps.set(Type.NUMBER, numberOrIntegerEvalMap);
    this.evalMaps.set(Type.INTEGER, numberOrIntegerEvalMap);
    this.evalMaps.set(Type.STRING, buildStringEvaluatorMap());
    this.evalMaps.set(Type.OBJECT, buildObjectEvaluatorMap());
  }

  getEvaluator(columnType: Type, evaluatorType: EvalType): ComparisonFunction {
    const evaluationMap = this.evalMaps.get(columnType) || null;
    if (evaluationMap === null) {
      // 550: where() clause includes an invalid predicate. Could not find
      // evaluation map for the given column type.
      throw new Exception(ErrorCode.INVALID_PREDICATE);
    }

    const evaluatorFn = evaluationMap.get(evaluatorType) || null;
    if (evaluatorFn === null) {
      // 550: where() clause includes an invalid predicate. Could not find
      // evaluation map for the given column type.
      throw new Exception(ErrorCode.INVALID_PREDICATE);
    }
    return evaluatorFn;
  }

  getKeyOfIndexEvaluator(columnType: Type): KeyOfIndexFunction {
    const fn = this.keyOfIndexConversionMap.get(columnType) || null;
    if (fn === null) {
      // 300: Not supported
      throw new Exception(ErrorCode.NOT_SUPPORTED);
    }
    return fn;
  }
}
