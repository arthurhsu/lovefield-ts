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

// The comparison result constant. This must be consistent with the constant
// required by the sort function of Array.prototype.sort.
export enum Favor {
  RHS = -1,  // favors right hand side, i.e. lhs < rhs
  TIE = 0,   // no favorite, i.e. lhs == rhs
  LHS = 1,   // favors left hand side, i.e. lhs > rhs
}

export enum TableType {
  DATA = 0,
  INDEX = 1,
}

export enum ExecType {
  NO_CHILD = -1,    // Will not call any of its children's exec().
  ALL = 0,          // Will invoke all children nodes' exec().
  FIRST_CHILD = 1,  // Will invoke only the first child's exec().
}

export enum LockType {
  EXCLUSIVE = 0,
  RESERVED_READ_ONLY = 1,
  RESERVED_READ_WRITE = 2,
  SHARED = 3,
}

// The priority of each type of task. Lower number means higher priority.
export enum TaskPriority {
  EXPORT_TASK = 0,
  IMPORT_TASK = 0,
  OBSERVER_QUERY_TASK = 0,
  EXTERNAL_CHANGE_TASK = 1,
  USER_QUERY_TASK = 2,
  TRANSACTION_TASK = 2,
}

export enum FnType {
  AVG = 'AVG',
  COUNT = 'COUNT',
  DISTINCT = 'DISTINCT',
  GEOMEAN = 'GEOMEAN',
  MAX = 'MAX',
  MIN = 'MIN',
  STDDEV = 'STDDEV',
  SUM = 'SUM',
}

export enum Operator {
  AND = 'and',
  OR = 'or',
}
