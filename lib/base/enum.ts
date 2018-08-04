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

// @export
export enum ConstraintAction {
  RESTRICT = 0,
  CASCADE = 1,
}

// @export
export enum ConstraintTiming {
  IMMEDIATE = 0,
  DEFERRABLE = 1,
}

// @export
export enum DataStoreType {
  INDEXED_DB = 0,
  MEMORY = 1,
  LOCAL_STORAGE = 2,
  FIREBASE = 3,
  WEB_SQL = 4,
  OBSERVABLE_STORE = 5,
}

// @export
export enum Order {
  DESC = 0,
  ASC = 1,
}

// @export
export enum Type {
  ARRAY_BUFFER = 0,
  BOOLEAN = 1,
  DATE_TIME = 2,
  INTEGER = 3,
  NUMBER = 4,
  STRING = 5,
  OBJECT = 6,
}

export const DEFAULT_VALUES: Map<Type, any> = new Map([
  [Type.ARRAY_BUFFER, null as any],              // nullable
  [Type.BOOLEAN, false],                         // not nullable
  [Type.DATE_TIME, Object.freeze(new Date(0))],  // not nullable
  [Type.INTEGER, 0],                             // not nullable
  [Type.NUMBER, 0],                              // not nullable
  [Type.STRING, ''],                             // not nullable
  [Type.OBJECT, null],                           // nullable
]);

// @export
export enum TransactionType {
  READ_ONLY = 0,
  READ_WRITE = 1,
}

// @export
export enum ErrorCode {
  // System level errors
  SYSTEM_ERROR = 0,
  VERSION_MISMATCH = 1,
  CONNECTION_CLOSED = 2,
  TIMEOUT = 3,
  OPERATION_BLOCKED = 4,
  QUOTA_EXCEEDED = 5,
  TOO_MANY_ROWS = 6,
  SERVICE_NOT_FOUND = 7,
  UNKNOWN_PLAN_NODE = 8,

  // Data errors
  DATA_ERROR = 100,
  TABLE_NOT_FOUND = 101,
  DATA_CORRUPTION = 102,
  INVALID_ROW_ID = 103,
  INVALID_TX_ACCESS = 105,
  OUT_OF_SCOPE = 106,
  INVALID_TX_STATE = 107,
  INCOMPATIBLE_DB = 108,
  ROW_ID_EXISTED = 109,
  IMPORT_TO_NON_EMPTY_DB = 110,
  DB_MISMATCH = 111,
  IMPORT_DATA_NOT_FOUND = 112,
  ALREADY_CONNECTED = 113,

  // Integrity errors
  CONSTRAINT_ERROR = 200,
  DUPLICATE_KEYS = 201,
  NOT_NULLABLE = 202,
  FK_VIOLATION = 203,

  // Unsupported
  NOT_SUPPORTED = 300,
  FB_NO_RAW_TX = 351,
  IDB_NOT_PROVIDED = 352,
  WEBSQL_NOT_PROVIDED = 353,
  CANT_OPEN_WEBSQL_DB = 354,
  NO_CHANGE_NOTIFICATION = 355,
  NO_WEBSQL_TX = 356,
  NO_PRED_IN_TOSQL = 357,
  NOT_IMPL_IN_TOSQL = 358,
  LOCAL_STORAGE_NOT_PROVIDED = 359,
  NOT_IMPLEMENTED = 360,
  CANT_OPEN_IDB = 361,

  // Syntax errors
  SYNTAX_ERROR = 500,
  UNBOUND_VALUE = 501,
  INVALID_NAME = 502,
  NAME_IN_USE = 503,
  INVALID_AUTO_KEY_TYPE = 504,
  INVALID_AUTO_KEY_COLUMN = 505,
  IMMEDIATE_EVAL_ONLY = 506,
  COLUMN_NOT_FOUND = 508,
  COLUMN_NOT_INDEXABLE = 509,
  BIND_ARRAY_OUT_OF_RANGE = 510,
  CANT_GET_IDB_TABLE = 511,
  CANT_GET_WEBSQL_TABLE = 512,
  UNKNOWN_QUERY_CONTEXT = 513,
  UNKNOWN_NODE_TYPE = 514,
  DUPLICATE_FROM = 515,
  DUPLICATE_WHERE = 516,
  INVALID_DELETE = 517,
  INVALID_INSERT = 518,
  INVALID_INSERT_OR_REPLACE = 519,
  DUPLICATE_INTO = 520,
  DUPLICATE_VALUES = 521,
  INVALID_SELECT = 522,
  UNBOUND_LIMIT_SKIP = 523,
  INVALID_DISTINCT = 524,
  INVALID_GROUPBY = 525,
  INVALID_PROJECTION = 526,
  INVALID_AGGREGATION = 527,
  DUPLICATE_LIMIT = 528,
  DUPLICATE_SKIP = 529,
  DUPLICATE_GROUPBY = 530,
  NEGATIVE_LIMIT_SKIP = 531,
  INVALID_UPDATE = 532,
  FK_LOOP = 533,
  FK_COLUMN_IN_USE = 534,
  SCHEMA_FINALIZED = 535,
  INVALID_FK_TABLE = 536,
  INVALID_FK_COLUMN = 537,
  INVALID_FK_COLUMN_TYPE = 538,
  FK_COLUMN_NONUNIQUE = 539,
  INVALID_FK_REF = 540,
  INVALID_OUTER_JOIN = 541,
  MISSING_FROM_BEFORE_JOIN = 542,
  PK_CANT_BE_FK = 543,
  DUPLICATE_PK = 544,
  NULLABLE_PK = 545,
  DUPLICATE_NAME = 546,
  INVALID_WHERE = 547,
  FROM_AFTER_WHERE = 548,
  FROM_AFTER_ORDER_GROUPBY = 549,
  INVALID_PREDICATE = 550,

  // Test errors
  TEST_ERROR = 900,
  ASSERTION = 998,
  SIMULATED_ERROR = 999,
}  // enum ErrorCode
