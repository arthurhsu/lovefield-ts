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
import * as lf from '../../../dist/lf';

const assert = chai.assert;

function assertMethods(obj: any, methodNames: string[], target: string): void {
  methodNames.forEach((methodName) => {
    const hasMethod = typeof obj[methodName] === 'function';
    if (!hasMethod) {
      throw new Error(`Missing ${methodName} from ${target}`);
    }
  });
}

function assertAttributes(
    obj: object, attributeNames: string[], target: string): void {
  attributeNames.forEach((attributeName) => {
    if (!obj.hasOwnProperty(attributeName)) {
      throw new Error(`Missing ${attributeName} from ${target}`);
    }
  });
}

describe('ApiTester', async () => {
  let builder: lf.Builder;
  let tableBuilder: lf.TableBuilder;

  before(() => {
    builder = lf.schema.create('apicheck', 1);
    tableBuilder = builder.createTable('DummyTable')
                       .addColumn('number', lf.Type.NUMBER)
                       .addColumn('dateTime', lf.Type.DATE_TIME)
                       .addColumn('string', lf.Type.STRING)
                       .addColumn('boolean', lf.Type.BOOLEAN)
                       .addColumn('arrayBuffer', lf.Type.ARRAY_BUFFER)
                       .addColumn('object', lf.Type.OBJECT);
  });

  it('hasAttributes', () => {
    assertAttributes(
        lf,
        [
          // enums
          'ConstraintAction',
          'ConstraintTiming',
          'Order',
          'TransactionType',
          'Type',

          // classes
          'fn',
          'op',
          'schema',
        ],
        'lf');

    assertMethods(lf, ['bind'], 'lf');
  });

  it('enumType', () => {
    assertAttributes(
        lf.Type,
        [
          'ARRAY_BUFFER',
          'BOOLEAN',
          'DATE_TIME',
          'INTEGER',
          'NUMBER',
          'STRING',
          'OBJECT',
        ],
        'lf.Type');
  });

  it('enumOrder', () => {
    assertAttributes(
        lf.Order,
        [
          'ASC',
          'DESC',
        ],
        'lf.Order');
  });

  it('enumTransactionType', () => {
    assertAttributes(
        lf.TransactionType,
        [
          'READ_ONLY',
          'READ_WRITE',
        ],
        'lf.TransactionType');
  });

  it('enumDataStoreType', () => {
    assertAttributes(
        lf.DataStoreType,
        [
          'INDEXED_DB',
          'MEMORY',
          'LOCAL_STORAGE',
          'WEB_SQL',
        ],
        'lf.DataStoreType');
  });

  it('enumConstraintAction', () => {
    assertAttributes(
        lf.ConstraintAction,
        [
          'RESTRICT',
          'CASCADE',
        ],
        'lf.ConstraintAction');
  });

  it('enumConstraintTiming', () => {
    assertAttributes(
        lf.ConstraintTiming,
        [
          'IMMEDIATE',
          'DEFERRABLE',
        ],
        'lf.ConstraintTiming');
  });

  it('apiFn', () => {
    assertMethods(
        lf.fn,
        [
          'avg',
          'count',
          'distinct',
          'max',
          'min',
          'stddev',
          'sum',
          'geomean',
        ],
        'lf.fn');
  });

  it('apiOp', () => {
    assertMethods(
        lf.op,
        [
          'and',
          'or',
          'not',
        ],
        'lf.op');
  });

  it('apiSchemaBuilder', () => {
    assertMethods(lf.schema, ['create'], 'lf.schema');
    assertMethods(
        builder,
        [
          'createTable',
          'connect',
          'setPragma',
          'getSchema',
        ],
        'schemaBuilder');

    assertMethods(
        tableBuilder,
        [
          'addColumn',
          'addPrimaryKey',
          'addForeignKey',
          'addUnique',
          'addNullable',
          'addIndex',
          'persistentIndex',
        ],
        'tableBuilder');
  });

  it('Capability', () => {
    const cap = lf.Capability.get();
    assert.isDefined(cap);
    assert.isNotNull(cap);
    assertAttributes(
        cap,
        [
          'supported',
          'indexedDb',
          'localStorage',
          'webSql',
        ],
        'Capability');
  });
});
