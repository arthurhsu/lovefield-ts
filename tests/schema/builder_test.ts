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
import {ConstraintAction, ConstraintTiming, Order, Type} from '../../lib/base/enum';
import {ErrorCode} from '../../lib/base/exception';
import {createSchema} from '../../lib/schema/builder';
import {ForeignKeySpec} from '../../lib/schema/foreign_key_spec';
import {TestUtil} from '../../testing/test_util';

const assert = chai.assert;

describe('Builder', () => {
  const createBuilder = () => {
    const schemaBuilder = createSchema('hr', 1);
    schemaBuilder.createTable('Job')
        .addColumn('id', Type.STRING)
        .addColumn('title', Type.STRING)
        .addColumn('minSalary', Type.NUMBER)
        .addColumn('maxSalary', Type.NUMBER)
        .addPrimaryKey(['id'])
        .addIndex('idx_maxSalary', ['maxSalary'], false, Order.DESC);

    schemaBuilder.createTable('JobHistory')
        .addColumn('employeeId', Type.INTEGER)
        .addColumn('startDate', Type.DATE_TIME)
        .addColumn('endDate', Type.DATE_TIME)
        .addColumn('jobId', Type.STRING)
        .addColumn('departmentId', Type.INTEGER)
        .addForeignKey('fk_EmployeeId', {
          action: ConstraintAction.CASCADE,
          local: 'employeeId',
          ref: 'Employee.id',
        })
        .addForeignKey('fk_DeptId', {
          action: ConstraintAction.CASCADE,
          local: 'departmentId',
          ref: 'Department.id',
          timing: ConstraintTiming.IMMEDIATE,
        });

    schemaBuilder.createTable('Employee')
        .addColumn('id', Type.INTEGER)
        .addColumn('firstName', Type.STRING)
        .addColumn('lastName', Type.STRING)
        .addColumn('email', Type.STRING)
        .addColumn('phoneNumber', Type.STRING)
        .addColumn('hireDate', Type.DATE_TIME)
        .addColumn('jobId', Type.STRING)
        .addColumn('salary', Type.NUMBER)
        .addColumn('commissionPercent', Type.NUMBER)
        .addColumn('managerId', Type.STRING)
        .addColumn('departmentId', Type.INTEGER)
        .addColumn('photo', Type.ARRAY_BUFFER)
        .addPrimaryKey([{name: 'id', autoIncrement: true}])
        .addUnique('uq_email', ['email'])
        .addIndex('idx_salary', [{name: 'salary', order: Order.DESC}])
        .addForeignKey(
            'fk_JobId',
            {local: 'jobId', ref: 'Job.id', action: ConstraintAction.CASCADE})
        .addNullable(['hireDate']);

    schemaBuilder.createTable('Department')
        .addColumn('id', Type.INTEGER)
        .addColumn('name', Type.STRING)
        .addColumn('managerId', Type.INTEGER)
        .addPrimaryKey([{name: 'id', order: Order.DESC}])
        .addForeignKey(
            'fk_ManagerId', {local: 'managerId', ref: 'Employee.id'});

    schemaBuilder.createTable('DummyTable')
        .addColumn('arraybuffer', Type.ARRAY_BUFFER)
        .addColumn('boolean', Type.BOOLEAN)
        .addColumn('datetime', Type.DATE_TIME)
        .addColumn('integer', Type.INTEGER)
        .addColumn('number', Type.NUMBER)
        .addColumn('object', Type.OBJECT)
        .addColumn('string', Type.STRING)
        .addIndex('idx_string', ['string'], true, Order.ASC)
        .addIndex('idx_number', [{name: 'number'}], true)
        .addNullable(['arraybuffer', 'object']);
    return schemaBuilder;
  };

  it('foreignKeys_SimpleSpec', () => {
    const schema = createBuilder().getSchema();
    assert.equal(0, schema.table('Job').getConstraint().foreignKeys.length);
    assert.equal(
        1, schema.table('Department').getConstraint().foreignKeys.length);

    const specs = new ForeignKeySpec(
        {
          action: ConstraintAction.RESTRICT,
          local: 'managerId',
          ref: 'Employee.id',
          timing: ConstraintTiming.IMMEDIATE,
        },
        'Department', 'fk_ManagerId');
    assert.deepEqual(
        specs, schema.table('Department').getConstraint().foreignKeys[0]);
  });

  it('foreignKeys_TwoSpec', () => {
    const schema = createBuilder().getSchema();
    assert.equal(
        2, schema.table('JobHistory').getConstraint().foreignKeys.length);

    let specs = new ForeignKeySpec(
        {
          action: ConstraintAction.CASCADE,
          local: 'employeeId',
          ref: 'Employee.id',
          timing: ConstraintTiming.IMMEDIATE,
        },
        'JobHistory', 'fk_EmployeeId');
    assert.deepEqual(
        specs, schema.table('JobHistory').getConstraint().foreignKeys[0]);
    specs = new ForeignKeySpec(
        {
          action: ConstraintAction.CASCADE,
          local: 'departmentId',
          ref: 'Department.id',
          timing: ConstraintTiming.IMMEDIATE,
        },
        'JobHistory', 'fk_DeptId');
    assert.deepEqual(
        specs, schema.table('JobHistory').getConstraint().foreignKeys[1]);
  });

  it('getParentForeignKeys', () => {
    const schema = createBuilder().getSchema();
    const parentForeignKeys = schema.info().getReferencingForeignKeys('Job');
    const spec = new ForeignKeySpec(
        {
          action: ConstraintAction.CASCADE,
          local: 'jobId',
          ref: 'Job.id',
          timing: ConstraintTiming.IMMEDIATE,
        },
        'Employee', 'fk_JobId');
    assert.deepEqual([spec], parentForeignKeys);
  });

  it('throws_DuplicateTable', () => {
    const schemaBuilder = createBuilder();
    // 503: Name {0} is already defined.
    TestUtil.assertThrowsError(ErrorCode.NAME_IN_USE, () => {
      schemaBuilder.createTable('DummyTable');
    });
  });

  it('defaultIndexOnForeignKey', () => {
    const schema = createBuilder().getSchema();
    const employee = schema.table('Employee');
    assert.equal(
        'Employee.fk_JobId', employee['jobId'].getIndex().getNormalizedName());
  });

  it('throws_InvalidFKRefTableName', () => {
    const schemaBuilder = createBuilder();
    schemaBuilder.createTable('FkTable2')
        .addColumn('employeeId', Type.STRING)
        .addForeignKey(
            'fkemployeeId', {local: 'employeeId', ref: 'Employee1.id'});
    // 536: Foreign key {0} refers to invalid table.
    TestUtil.assertThrowsError(ErrorCode.INVALID_FK_TABLE, () => {
      schemaBuilder.getSchema();
    });
  });

  it('throws_ColumnTypeMismatch', () => {
    const schemaBuilder = createBuilder();
    schemaBuilder.createTable('FkTable3')
        .addColumn('employeeId', Type.STRING)
        .addForeignKey(
            'fkemployeeId', {local: 'employeeId', ref: 'FkTable4.employeeId'});
    schemaBuilder.createTable('FkTable4').addColumn('employeeId', Type.INTEGER);
    // 538: Foreign key {0} column type mismatch.
    TestUtil.assertThrowsError(ErrorCode.INVALID_FK_COLUMN_TYPE, () => {
      schemaBuilder.getSchema();
    });
  });

  it('throws_InValidFKRefColName', () => {
    const schemaBuilder = createBuilder();
    schemaBuilder.createTable('FkTable5')
        .addColumn('employeeId', Type.STRING)
        .addForeignKey(
            'fkemployeeId', {local: 'employeeId', ref: 'Employee.id1'});
    // 537: Foreign key {0} refers to invalid column.
    TestUtil.assertThrowsError(ErrorCode.INVALID_FK_COLUMN, () => {
      schemaBuilder.getSchema();
    });
  });

  it('throws_InValidFKRefName', () => {
    const schemaBuilder = createBuilder();
    // 540: Foreign key {0} has invalid reference syntax.
    TestUtil.assertThrowsError(ErrorCode.INVALID_FK_REF, () => {
      schemaBuilder.createTable('FkTable5')
          .addColumn('employeeId', Type.STRING)
          .addForeignKey(
              'fkemployeeId', {local: 'employeeId', ref: 'Employeeid'});
    });
  });

  it('checkForeignKeyChainOnSameColumn', () => {
    const schemaBuilder = createSchema('hr', 1);
    schemaBuilder.createTable('FkTable1')
        .addColumn('employeeId', Type.INTEGER)
        .addForeignKey(
            'fk_employeeId', {local: 'employeeId', ref: 'FkTable2.employeeId'});
    schemaBuilder.createTable('FkTable2')
        .addColumn('employeeId', Type.INTEGER)
        .addUnique('uq_employeeId', ['employeeId'])
        .addForeignKey(
            'fk_employeeId', {local: 'employeeId', ref: 'FkTable3.employeeId'});
    schemaBuilder.createTable('FkTable3')
        .addColumn('employeeId', Type.INTEGER)
        .addPrimaryKey(['employeeId']);

    // 534: Foreign key {0} refers to source column of another foreign key.
    TestUtil.assertThrowsError(ErrorCode.FK_COLUMN_IN_USE, () => {
      schemaBuilder.getSchema();
    });
  });

  it('checkForeignKeyLoop', () => {
    const schemaBuilder = createBuilder();
    schemaBuilder.createTable('FkTable8')
        .addColumn('employeeId', Type.INTEGER)
        .addColumn('employeeId2', Type.INTEGER)
        .addPrimaryKey([{name: 'employeeId2', order: Order.DESC}])
        .addForeignKey(
            'fkemployeeId1',
            {local: 'employeeId', ref: 'FkTable10.employeeId'});
    schemaBuilder.createTable('FkTable9')
        .addColumn('employeeId', Type.INTEGER)
        .addForeignKey(
            'fkemployeeId2',
            {local: 'employeeId', ref: 'FkTable10.employeeId'});
    schemaBuilder.createTable('FkTable10')
        .addColumn('employeeId', Type.INTEGER)
        .addColumn('employeeId2', Type.INTEGER)
        .addPrimaryKey([{name: 'employeeId', order: Order.DESC}])
        .addForeignKey(
            'fkemployeeId3',
            {local: 'employeeId2', ref: 'FkTable11.employeeId'});
    schemaBuilder.createTable('FkTable11')
        .addColumn('employeeId', Type.INTEGER)
        .addColumn('employeeId2', Type.INTEGER)
        .addPrimaryKey([{name: 'employeeId', order: Order.DESC}])
        .addForeignKey(
            'fkemployeeId4',
            {local: 'employeeId2', ref: 'FkTable8.employeeId2'});
    // 533: Foreign key loop detected.
    TestUtil.assertThrowsError(ErrorCode.FK_LOOP, () => {
      schemaBuilder.getSchema();
    });
  });

  it('checkForeignKeySelfLoop', () => {
    const schemaBuilder = createBuilder();
    schemaBuilder.createTable('FkTable8')
        .addColumn('employeeId', Type.INTEGER)
        .addColumn('employeeId2', Type.INTEGER)
        .addPrimaryKey([{name: 'employeeId', order: Order.DESC}])
        .addForeignKey(
            'fkemployeeId1',
            {local: 'employeeId2', ref: 'FkTable8.employeeId'});
    schemaBuilder.getSchema();
  });

  it('checkForeignKeySelfLoopOfBiggerGraph', () => {
    const schemaBuilder = createBuilder();
    schemaBuilder.createTable('FkTable8')
        .addColumn('employeeId', Type.INTEGER)
        .addColumn('employeeId2', Type.INTEGER)
        .addPrimaryKey([{name: 'employeeId2', order: Order.DESC}])
        .addForeignKey(
            'fkemployeeId1',
            {local: 'employeeId', ref: 'FkTable9.employeeId2'});
    schemaBuilder.createTable('FkTable9')
        .addColumn('employeeId', Type.INTEGER)
        .addColumn('employeeId2', Type.INTEGER)
        .addPrimaryKey([{name: 'employeeId2', order: Order.DESC}]);
    // Self loop on table11
    schemaBuilder.createTable('FkTable11')
        .addColumn('employeeId', Type.INTEGER)
        .addColumn('employeeId2', Type.INTEGER)
        .addPrimaryKey([{name: 'employeeId', order: Order.DESC}])
        .addForeignKey(
            'fkemployeeId4',
            {local: 'employeeId2', ref: 'FkTable8.employeeId2'})
        .addForeignKey(
            'fkemployeeId2',
            {local: 'employeeId2', ref: 'FkTable11.employeeId'});
    schemaBuilder.getSchema();
  });

  it('throws_FKRefKeyNonUnique', () => {
    const schemaBuilder = createBuilder();
    schemaBuilder.createTable('FkTable5')
        .addColumn('employeeId', Type.STRING)
        .addForeignKey(
            'fkemployeeId', {local: 'employeeId', ref: 'Employee.firstName'});
    // 539: Foreign key {0} refers to non-unique column.
    TestUtil.assertThrowsError(ErrorCode.FK_COLUMN_NONUNIQUE, () => {
      schemaBuilder.getSchema();
    });
  });

  it('throws_ModificationAfterFinalization', () => {
    const schemaBuilder = createBuilder();
    schemaBuilder.getSchema();
    // 535: Schema is already finalized.
    TestUtil.assertThrowsError(ErrorCode.SCHEMA_FINALIZED, () => {
      schemaBuilder.createTable('NewTable');
    });
  });

  it('verifySchemaCorrectness', () => {
    const schema = createBuilder().getSchema();
    assert.equal(5, schema.tables().length);

    const emp = schema.table('Employee');
    assert.equal('Employee', emp.getEffectiveName());
    assert.isTrue(emp['id'].unique);
    assert.isTrue(emp['email'].unique);
    assert.isTrue(emp['hireDate'].nullable);

    const e = emp.as('e');
    assert.equal('e', e.getEffectiveName());
    assert.equal(4, emp.getIndices().length);
    assert.equal(12, emp.getColumns().length);
    assert.equal('Employee.#', e.getRowIdIndexName());

    const dummy = schema.table('DummyTable');
    const row = dummy.createRow({
      arraybuffer: null,
      boolean: true,
      datetime: new Date(1),
      integer: 2,
      number: 3,
      object: null,
      string: 'bar',
    });
    const row2 = dummy.deserializeRow(row.serialize());
    assert.deepEqual(row.payload(), row2.payload());
  });

  it('verifySchemaCorrectness_IndexOrder', () => {
    const schema = createBuilder().getSchema();

    // Test case of DESC index.
    const job = schema.table('Job');
    const maxSalaryIndexSchema = job.getIndices().filter(
        (indexSchema) => indexSchema.name === 'idx_maxSalary')[0];
    assert.equal(Order.DESC, maxSalaryIndexSchema.columns[0].order);

    // Test case of ASC index.
    const dummyTable = schema.table('DummyTable');
    const stringIndexSchema = dummyTable.getIndices().filter(
        (indexSchema) => indexSchema.name === 'idx_string')[0];
    assert.equal(Order.ASC, stringIndexSchema.columns[0].order);
  });

  it('throws_IllegalName', () => {
    // 502: Naming rule violation: {0}.
    TestUtil.assertThrowsError(ErrorCode.INVALID_NAME, () => {
      const schemaBuilder = createSchema('d1', 1);
      schemaBuilder.createTable('#NewTable');
    });

    TestUtil.assertThrowsError(ErrorCode.INVALID_NAME, () => {
      const schemaBuilder = createSchema('d2', 1);
      schemaBuilder.createTable('NameTable')
          .addColumn('22arraybuffer', Type.ARRAY_BUFFER);
    });

    TestUtil.assertThrowsError(ErrorCode.INVALID_NAME, () => {
      const schemaBuilder = createSchema('d3', 1);
      schemaBuilder.createTable('NameTable')
          .addColumn('_obj_#ect', Type.OBJECT);
    });

    TestUtil.assertThrowsError(ErrorCode.INVALID_NAME, () => {
      const schemaBuilder = createSchema('d4', 1);
      schemaBuilder.createTable('NameTable')
          .addColumn('name', Type.STRING)
          .addIndex('idx.name', ['name']);
    });

    TestUtil.assertThrowsError(ErrorCode.INVALID_NAME, () => {
      const schemaBuilder = createSchema('d4', 1);
      schemaBuilder.createTable('NameTable')
          .addColumn('name', Type.STRING)
          .addUnique('unq#name', ['name']);
    });
  });
});
