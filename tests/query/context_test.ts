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
import {ConstraintAction, ConstraintTiming, Type} from '../../lib/base/enum';
import {DeleteContext} from '../../lib/query/delete_context';
import {InsertContext} from '../../lib/query/insert_context';
import {UpdateContext} from '../../lib/query/update_context';
import {createSchema} from '../../lib/schema/builder';
import {Database} from '../../lib/schema/database';
import {SchemaTestHelper} from '../../testing/schema_test_helper';

const assert = chai.assert;

describe('Context', () => {
  // Returns a schema where no foreign keys exist.
  function getSchemaWithoutForeignKeys(): Database {
    const schemaBuilder = createSchema('contexttest', 1);
    schemaBuilder.createTable('TableA').addColumn('id', Type.STRING);
    schemaBuilder.createTable('TableB').addColumn('id', Type.STRING);
    return schemaBuilder.getSchema();
  }

  it('getScope_Insert', () => {
    const schema =
        SchemaTestHelper.getOneForeignKey(ConstraintTiming.IMMEDIATE);
    const context = new InsertContext(schema);
    const childTable = schema.table('Child');
    const parentTable = schema.table('Parent');
    const row = childTable.createRow();
    context.values = [row];
    context.into = childTable;
    const scope = context.getScope();
    assert.isTrue(scope.has(childTable));
    assert.equal(2, scope.size);
    assert.isTrue(scope.has(parentTable));
  });

  it('getScope_InsertNoExpansion', () => {
    const schema = getSchemaWithoutForeignKeys();
    const context = new InsertContext(schema);
    const tableA = schema.table('TableA');
    const row = tableA.createRow();
    context.values = [row];
    context.into = tableA;
    const scope = context.getScope();
    assert.isTrue(scope.has(tableA));
    assert.equal(1, scope.size);
  });

  it('getScope_InsertOrReplace', () => {
    const schema = SchemaTestHelper.getTableChain(ConstraintAction.RESTRICT);
    const context = new InsertContext(schema);
    context.allowReplace = true;
    const tableA = schema.table('TableA');
    const tableB = schema.table('TableB');
    const tableC = schema.table('TableC');
    const row = tableB.createRow();
    context.values = [row];
    context.into = tableB;
    const scope = context.getScope();
    assert.equal(3, scope.size);
    assert.isTrue(scope.has(tableA));
    assert.isTrue(scope.has(tableB));
    assert.isTrue(scope.has(tableC));
  });

  it('getScope_InsertOrReplaceNoExpansion', () => {
    const schema = getSchemaWithoutForeignKeys();
    const context = new InsertContext(schema);
    context.allowReplace = true;
    const tableA = schema.table('TableA');
    const row = tableA.createRow();
    context.values = [row];
    context.into = tableA;
    const scope = context.getScope();
    assert.equal(1, scope.size);
    assert.isTrue(scope.has(tableA));
  });

  it('getScope_Delete_Restrict', () => {
    const schema = SchemaTestHelper.getTableChain(ConstraintAction.RESTRICT);
    const context = new DeleteContext(schema);
    const tableA = schema.table('TableA');
    const tableB = schema.table('TableB');
    context.from = tableA;
    const scope = context.getScope();
    assert.equal(2, scope.size);
    assert.isTrue(scope.has(tableA));
    assert.isTrue(scope.has(tableB));
  });

  it('getScope_Delete_Cascade', () => {
    const schema = SchemaTestHelper.getTableChain(ConstraintAction.CASCADE);
    const context = new DeleteContext(schema);
    const tableA = schema.table('TableA');
    const tableB = schema.table('TableB');
    const tableC = schema.table('TableC');
    context.from = tableA;
    const scope = context.getScope();
    assert.equal(3, scope.size);
    assert.isTrue(scope.has(tableA));
    assert.isTrue(scope.has(tableB));
    assert.isTrue(scope.has(tableC));
  });

  it('getScope_DeleteNoExpansion', () => {
    const schema = getSchemaWithoutForeignKeys();
    const context = new DeleteContext(schema);
    const tableA = schema.table('TableA');
    context.from = tableA;
    const scope = context.getScope();
    assert.equal(1, scope.size);
    assert.isTrue(scope.has(tableA));
  });

  it('getScope_UpdateOneColumn', () => {
    const schema =
        SchemaTestHelper.getTwoForeignKeys(ConstraintAction.RESTRICT);
    const context = new UpdateContext(schema);
    const tableA = schema.table('TableA');
    const tableB1 = schema.table('TableB1');
    context.table = tableA;
    context.set = [{column: tableA['id1'], value: 'test1'}];
    const scope = context.getScope();
    assert.equal(2, scope.size);
    assert.isTrue(scope.has(tableA));
    assert.isTrue(scope.has(tableB1));
  });

  it('getScope_UpdateTwoColumns', () => {
    const schema =
        SchemaTestHelper.getTwoForeignKeys(ConstraintAction.RESTRICT);
    const context = new UpdateContext(schema);
    const tableA = schema.table('TableA');
    const tableB1 = schema.table('TableB1');
    const tableB2 = schema.table('TableB2');
    context.table = tableA;
    context.set = [
      {column: tableA['id1'], value: 'test1'},
      {column: tableA['id2'], value: 'test2'},
    ];
    const scope = context.getScope();
    assert.equal(3, scope.size);
    assert.isTrue(scope.has(tableA));
    assert.isTrue(scope.has(tableB1));
    assert.isTrue(scope.has(tableB2));
  });

  it('getScope_UpdateReferredColumn', () => {
    const schema = SchemaTestHelper.getTableChain(ConstraintAction.RESTRICT);
    const context = new UpdateContext(schema);
    const tableB = schema.table('TableB');
    const tableC = schema.table('TableC');
    context.table = tableB;
    context.set = [{column: tableB['id'], value: 'test'}];
    const scope = context.getScope();
    assert.equal(2, scope.size);
    assert.isTrue(scope.has(tableB));
    assert.isTrue(scope.has(tableC));
  });

  it('getScope_UpdateReferredAndReferringColumn', () => {
    const schema = SchemaTestHelper.getTableChain(ConstraintAction.RESTRICT);
    const context = new UpdateContext(schema);

    const tableA = schema.table('TableA');
    const tableB = schema.table('TableB');
    const tableC = schema.table('TableC');
    context.table = tableB;
    context.set = [
      {column: tableB['id'], value: 'test'},
      {column: tableB['foreignKey'], value: 'test'},
    ];
    const scope = context.getScope();
    assert.equal(3, scope.size);
    assert.isTrue(scope.has(tableA));
    assert.isTrue(scope.has(tableB));
    assert.isTrue(scope.has(tableC));
  });

  it('getScope_UpdateNoExpansion', () => {
    const schema = getSchemaWithoutForeignKeys();
    const context = new UpdateContext(schema);
    const tableA = schema.table('TableA');
    context.table = tableA;
    context.set = [{column: tableA['id'], value: 'test'}];
    const scope = context.getScope();
    assert.equal(1, scope.size);
    assert.isTrue(scope.has(tableA));
  });
});
