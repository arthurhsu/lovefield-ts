/**
 * Copyright 2020 The Lovefield Project Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the 'License');
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an 'AS IS' BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {ErrorCode} from '../lib/base/enum';
import {LovefieldOptions} from '../lib/base/lovefield_options';

export class DebugOptions implements LovefieldOptions {
  debugMode: boolean;
  memoryOnly: boolean;
  exceptionUrl: string;
  useGetAll: boolean;
  private messages: Map<ErrorCode, string>;
  private static instance = null as unknown as DebugOptions;

  static get(): DebugOptions {
    if (DebugOptions.instance === null) {
      DebugOptions.instance = new DebugOptions();
    }
    return DebugOptions.instance;
  }

  constructor() {
    if (DebugOptions.instance !== undefined) {
      (this.debugMode = true),
        (this.memoryOnly = false),
        (this.useGetAll = false),
        (this.exceptionUrl =
          'http://google.github.io/lovefield/error_lookup/src/error_lookup.html?c=');
      this.setupMap();
    }
  }

  errorMessage(code: number): string {
    const ec = code as ErrorCode;
    return this.messages.get(ec) || code.toString();
  }

  private setupMap() {
    const m = new Map<ErrorCode, string>();
    const ec = ErrorCode;
    m.set(ec.SYSTEM_ERROR, 'System error');
    m.set(ec.VERSION_MISMATCH, 'Lovefield library version mismatch.');
    m.set(ec.CONNECTION_CLOSED, 'The database connection is not active.');
    m.set(ec.TIMEOUT, 'Operation timeout.');
    m.set(ec.OPERATION_BLOCKED, 'Operation blocked.');
    m.set(ec.QUOTA_EXCEEDED, 'Storage quota exceeded.');
    m.set(
      ec.TOO_MANY_ROWS,
      'Too many rows: B-Tree implementation supports at most {0} rows.'
    );
    m.set(ec.SERVICE_NOT_FOUND, 'Service {0} not registered.');
    m.set(ec.UNKNOWN_PLAN_NODE, 'Unknown query plan node.');

    m.set(ec.DATA_ERROR, 'Data error');
    m.set(ec.TABLE_NOT_FOUND, 'Table {0} not found.');
    m.set(ec.DATA_CORRUPTION, 'Data corruption detected.');
    m.set(ec.INVALID_ROW_ID, 'Row id must be numbers.');
    m.set(
      ec.INVALID_TX_ACCESS,
      'Attempt to access in-flight transaction states.'
    );
    m.set(ec.OUT_OF_SCOPE, 'Attempt to access {0} outside of specified scope.');
    m.set(
      ec.INVALID_TX_STATE,
      'Invalid transaction state transition: {0} -> {1}.'
    );
    m.set(
      ec.INCOMPATIBLE_DB,
      'Attempt to open a newer database with old code.'
    );
    m.set(
      ec.ROW_ID_EXISTED,
      'Attempt to insert a row number that already existed.'
    );
    m.set(
      ec.IMPORT_TO_NON_EMPTY_DB,
      'Attempt to import into a non-empty database.'
    );
    m.set(ec.DB_MISMATCH, 'Database name/version mismatch for import.');
    m.set(ec.IMPORT_DATA_NOT_FOUND, 'Import data not found.');
    m.set(
      ec.ALREADY_CONNECTED,
      'Attempt to connect() to an already connected/connecting database.'
    );

    m.set(ec.CONSTRAINT_ERROR, 'Constraint error');
    m.set(
      ec.DUPLICATE_KEYS,
      'Duplicate keys are not allowed, index: {0}, key: {1}'
    );
    m.set(
      ec.NOT_NULLABLE,
      'Attempted to insert NULL value to non-nullable field {0}.'
    );
    m.set(
      ec.FK_VIOLATION,
      'Foreign key constraint violation on constraint {0}.'
    );

    m.set(ec.NOT_SUPPORTED, 'Not supported');
    m.set(ec.FB_NO_RAW_TX, 'Firebase does not have raw transaction.');
    m.set(ec.IDB_NOT_PROVIDED, 'IndexedDB is not supported by platform.');
    m.set(ec.WEBSQL_NOT_PROVIDED, 'WebSQL is not supported by platform.');
    m.set(
      ec.CANT_OPEN_WEBSQL_DB,
      'Unable to open WebSQL database. {0}. See ' +
        'https://github.com/google/lovefield/blob/master/docs/FAQ.md ' +
        'for possible explanation.'
    );
    m.set(
      ec.NO_CHANGE_NOTIFICATION,
      'WebSQL does not support change notification.'
    );
    m.set(
      ec.NO_WEBSQL_TX,
      'Use WebSQL instance to create transaction instead.'
    );
    m.set(ec.NO_PRED_IN_TOSQL, 'toSql() does not support predicate type: {0}.');
    m.set(ec.NOT_IMPL_IN_TOSQL, 'toSql() is not implemented for {0}.');
    m.set(
      ec.LOCAL_STORAGE_NOT_PROVIDED,
      'LocalStorage is not supported by platform.'
    );
    m.set(ec.NOT_IMPLEMENTED, 'Not implemented yet.');
    m.set(
      ec.CANT_OPEN_IDB,
      'Unable to open IndexedDB database: {0}, {1}. See ' +
        'https://github.com/google/lovefield/blob/master/docs/FAQ.md ' +
        'for possible explanation.'
    );
    m.set(ec.CANT_READ_IDB, 'Error deserializing row from IndexedDB: {0}, {1}');
    m.set(ec.CANT_LOAD_IDB, 'Error reading IndexedDB object store: {0}, {1}');

    m.set(ec.SYNTAX_ERROR, 'Syntax error');
    m.set(ec.UNBOUND_VALUE, 'Value is not bounded.');
    m.set(ec.INVALID_NAME, 'Naming rule violation: {0}.');
    m.set(ec.NAME_IN_USE, 'Name {0} is already defined.');
    m.set(
      ec.INVALID_AUTO_KEY_TYPE,
      'Can not use autoIncrement with a non-integer primary key.'
    );
    m.set(
      ec.INVALID_AUTO_KEY_COLUMN,
      'Can not use autoIncrement with a cross-column primary key.'
    );
    m.set(
      ec.IMMEDIATE_EVAL_ONLY,
      'Lovefield allows only immediate evaluation of cascading constraints.'
    );
    m.set(ec.COLUMN_NOT_FOUND, 'Table {0} does not have column: {1}.');
    m.set(
      ec.COLUMN_NOT_INDEXABLE,
      'Attempt to index table {0} on non-indexable column {1}.'
    );
    m.set(
      ec.BIND_ARRAY_OUT_OF_RANGE,
      'Cannot bind to given array: out of range.'
    );
    m.set(
      ec.CANT_GET_IDB_TABLE,
      'IndexedDB tables needs to be acquired from transactions.'
    );
    m.set(
      ec.CANT_GET_WEBSQL_TABLE,
      'WebSQL tables needs to be acquired from transactions.'
    );
    m.set(ec.UNKNOWN_QUERY_CONTEXT, 'Unknown query context.');
    m.set(ec.UNKNOWN_NODE_TYPE, 'Unknown node type.');
    m.set(ec.DUPLICATE_FROM, 'from() has already been called.');
    m.set(ec.DUPLICATE_WHERE, 'where() has already been called.');
    m.set(ec.INVALID_DELETE, 'Invalid usage of delete().');
    m.set(ec.INVALID_INSERT, 'Invalid usage of insert().');
    m.set(
      ec.INVALID_INSERT_OR_REPLACE,
      'Attempted to insert or replace in a table with no primary key.'
    );
    m.set(ec.DUPLICATE_INTO, 'into() has already been called.');
    m.set(ec.DUPLICATE_VALUES, 'values() has already been called.');
    m.set(ec.INVALID_SELECT, 'Invalid usage of select().');
    m.set(
      ec.UNBOUND_LIMIT_SKIP,
      'Binding parameters of limit/skip without providing values.'
    );
    m.set(ec.INVALID_DISTINCT, 'Invalid usage of lf.fn.distinct().');
    m.set(ec.INVALID_GROUPBY, 'Invalid projection list or groupBy columns.');
    m.set(
      ec.INVALID_PROJECTION,
      'Invalid projection list: mixing aggregated with non-aggregated.'
    );
    m.set(ec.INVALID_AGGREGATION, 'Invalid aggregation detected: {0}');
    m.set(ec.DUPLICATE_LIMIT, 'limit() has already been called.');
    m.set(ec.DUPLICATE_SKIP, 'skip() has already been called.');
    m.set(ec.DUPLICATE_GROUPBY, 'groupBy() has already been called.');
    m.set(
      ec.NEGATIVE_LIMIT_SKIP,
      'Number of rows must not be negative for limit/skip.'
    );
    m.set(ec.INVALID_UPDATE, 'Invalid usage of update().');
    m.set(ec.FK_LOOP, 'Foreign key loop detected.');
    m.set(
      ec.FK_COLUMN_IN_USE,
      'Foreign key {0} refers to source column of another foreign key.'
    );
    m.set(ec.SCHEMA_FINALIZED, 'Schema is already finalized.');
    m.set(ec.INVALID_FK_TABLE, 'Foreign key {0} refers to invalid table.');
    m.set(ec.INVALID_FK_COLUMN, 'Foreign key {0} refers to invalid column.');
    m.set(ec.INVALID_FK_COLUMN_TYPE, 'Foreign key {0} column type mismatch.');
    m.set(
      ec.FK_COLUMN_NONUNIQUE,
      'Foreign key {0} refers to non-unique column.'
    );
    m.set(ec.INVALID_FK_REF, 'Foreign key {0} has invalid reference syntax.');
    m.set(ec.INVALID_OUTER_JOIN, 'Outer join accepts only join predicate.');
    m.set(
      ec.MISSING_FROM_BEFORE_JOIN,
      'from() has to be called before innerJoin() or leftOuterJoin().'
    );
    m.set(
      ec.PK_CANT_BE_FK,
      'Foreign key {0}. A primary key column cannot also be ' +
        'a foreign key child column'
    );
    m.set(ec.DUPLICATE_PK, 'Duplicate primary key index found at {0}');
    m.set(
      ec.NULLABLE_PK,
      'Primary key column {0} cannot be marked as nullable'
    );
    m.set(
      ec.DUPLICATE_NAME,
      'Indices/constraints/columns cannot re-use the table name {0}'
    );
    m.set(
      ec.INVALID_WHERE,
      'where() cannot be called before innerJoin() or leftOuterJoin().'
    );
    m.set(ec.FROM_AFTER_WHERE, 'from() has to be called before where().');
    m.set(
      ec.FROM_AFTER_ORDER_GROUPBY,
      'from() has to be called before orderBy() or groupBy().'
    );
    m.set(ec.INVALID_PREDICATE, 'where() clause includes an invalid predicate');

    m.set(ec.TEST_ERROR, 'Test error');
    m.set(ec.ASSERTION, 'ASSERT: {0}');
    m.set(ec.SIMULATED_ERROR, 'Simulated error');

    this.messages = m;
  }
}
