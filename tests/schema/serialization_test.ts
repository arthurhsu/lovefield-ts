/**
 * Copyright 2026 The Lovefield Project Authors. All Rights Reserved.
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

import {assert} from 'chai';
import {schema} from '../../lib/schema/schema';
import {Type, Order, ConstraintAction, ErrorCode} from '../../lib/base/enum';
import {TestUtil} from '../../testing/test_util';

describe('SchemaSerialization', () => {
  const sortSchema = (s: any) => {
    s.tables.forEach((t: any) => {
      t.columns.sort((a: any, b: any) => a.name.localeCompare(b.name));
      t.indices.sort((a: any, b: any) => a.name.localeCompare(b.name));
      t.indices.forEach((idx: any) => {
        idx.columns.sort((a: any, b: any) => a.name.localeCompare(b.name));
      });
      if (t.constraints.notNullable) {
        t.constraints.notNullable.sort();
      }
      if (t.constraints.foreignKeys) {
        t.constraints.foreignKeys.sort((a: any, b: any) =>
          a.name.localeCompare(b.name)
        );
      }
    });
    s.tables.sort((a: any, b: any) => a.name.localeCompare(b.name));
  };

  it('serialize', () => {
    const builder = schema.create('hr', 1);
    builder
      .createTable('Job')
      .addColumn('id', Type.STRING)
      .addColumn('title', Type.STRING)
      .addColumn('minSalary', Type.NUMBER)
      .addColumn('maxSalary', Type.NUMBER)
      .addPrimaryKey(['id'])
      .addIndex('idx_maxSalary', ['maxSalary'], false, Order.DESC);

    builder
      .createTable('Employee')
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
      .addForeignKey('fk_JobId', {
        local: 'jobId',
        ref: 'Job.id',
        action: ConstraintAction.CASCADE,
      })
      .addNullable(['hireDate']);

    const serialized = builder.serialize() as any;
    assert.equal('hr', serialized.name);
    assert.equal(1, serialized.version);
    assert.equal(2, serialized.tables.length);

    const jobTable = serialized.tables.find((t: any) => t.name === 'Job');
    assert.isDefined(jobTable);
    assert.equal(4, jobTable.columns.length);
    assert.equal(2, jobTable.indices.length);

    const employeeTable = serialized.tables.find(
      (t: any) => t.name === 'Employee'
    );
    assert.isDefined(employeeTable);
    assert.equal(12, employeeTable.columns.length);
    assert.equal(4, employeeTable.indices.length);

    // Check constraints
    assert.isDefined(employeeTable.constraints);
    assert.isDefined(employeeTable.constraints.primaryKey);
    assert.equal('pkEmployee', employeeTable.constraints.primaryKey.name);
    assert.equal(10, employeeTable.constraints.notNullable.length);
    assert.equal(1, employeeTable.constraints.foreignKeys.length);

    // Test deserialization
    const builder2 = schema.createFrom(serialized);
    const serialized2 = builder2.serialize() as any;

    sortSchema(serialized);
    sortSchema(serialized2);
    assert.deepEqual(serialized, serialized2);
  });

  it('throws_createFrom_InvalidData', () => {
    // 100: Data error.
    TestUtil.assertThrowsError(ErrorCode.DATA_ERROR, () => {
      schema.createFrom({name: 1, version: 1} as any);
    });
    TestUtil.assertThrowsError(ErrorCode.DATA_ERROR, () => {
      schema.createFrom({name: 'hr', version: '1'} as any);
    });
    TestUtil.assertThrowsError(ErrorCode.DATA_ERROR, () => {
      schema.createFrom({version: 1} as any);
    });
    TestUtil.assertThrowsError(ErrorCode.DATA_ERROR, () => {
      schema.createFrom({name: 'hr'} as any);
    });
  });
});
