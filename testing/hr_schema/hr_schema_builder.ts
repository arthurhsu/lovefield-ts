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

import {ConstraintAction, Order, Type} from '../../lib/base/enum';
import {Builder} from '../../lib/schema/builder';
import {SchemaBuilder} from '../../lib/schema/schema_builder';

export function getHrDbSchemaBuilder(dbName?: string): Builder {
  const name = dbName || `hr${Date.now()}`;
  const schemaBuilder = new SchemaBuilder(name, 1);

  schemaBuilder
    .createTable('Job')
    .addColumn('id', Type.STRING)
    .addColumn('title', Type.STRING)
    .addColumn('minSalary', Type.NUMBER)
    .addColumn('maxSalary', Type.NUMBER)
    .addPrimaryKey(['id'])
    .addIndex('idx_maxSalary', ['maxSalary'], false, Order.DESC);

  schemaBuilder
    .createTable('JobHistory')
    .addColumn('employeeId', Type.STRING)
    .addColumn('startDate', Type.DATE_TIME)
    .addColumn('endDate', Type.DATE_TIME)
    .addColumn('jobId', Type.STRING)
    .addColumn('departmentId', Type.STRING)
    .addForeignKey('fk_EmployeeId', {
      action: ConstraintAction.RESTRICT,
      local: 'employeeId',
      ref: 'Employee.id',
    })
    .addForeignKey('fk_DepartmentId', {
      action: ConstraintAction.RESTRICT,
      local: 'departmentId',
      ref: 'Department.id',
    });

  schemaBuilder
    .createTable('Employee')
    .addColumn('id', Type.STRING)
    .addColumn('firstName', Type.STRING)
    .addColumn('lastName', Type.STRING)
    .addColumn('email', Type.STRING)
    .addColumn('phoneNumber', Type.STRING)
    .addColumn('hireDate', Type.DATE_TIME)
    .addColumn('jobId', Type.STRING)
    .addColumn('salary', Type.NUMBER)
    .addColumn('commissionPercent', Type.NUMBER)
    .addColumn('managerId', Type.STRING)
    .addColumn('departmentId', Type.STRING)
    .addColumn('photo', Type.ARRAY_BUFFER)
    .addPrimaryKey(['id'])
    .addForeignKey('fk_JobId', {
      action: ConstraintAction.RESTRICT,
      local: 'jobId',
      ref: 'Job.id',
    })
    .addForeignKey('fk_DepartmentId', {
      action: ConstraintAction.RESTRICT,
      local: 'departmentId',
      ref: 'Department.id',
    })
    .addIndex('idx_salary', ['salary'], false, Order.DESC)
    .addNullable(['hireDate']);

  schemaBuilder
    .createTable('Department')
    .addColumn('id', Type.STRING)
    .addColumn('name', Type.STRING)
    .addColumn('managerId', Type.STRING)
    .addColumn('locationId', Type.STRING)
    .addPrimaryKey(['id'])
    .addForeignKey('fk_LocationId', {
      action: ConstraintAction.RESTRICT,
      local: 'locationId',
      ref: 'Location.id',
    });

  schemaBuilder
    .createTable('Location')
    .addColumn('id', Type.STRING)
    .addColumn('streetAddress', Type.STRING)
    .addColumn('postalCode', Type.STRING)
    .addColumn('city', Type.STRING)
    .addColumn('stateProvince', Type.STRING)
    .addColumn('countryId', Type.INTEGER)
    .addPrimaryKey(['id'])
    .addForeignKey('fk_CountryId', {
      action: ConstraintAction.RESTRICT,
      local: 'countryId',
      ref: 'Country.id',
    });

  schemaBuilder
    .createTable('Country')
    .addColumn('id', Type.INTEGER)
    .addColumn('name', Type.STRING)
    .addColumn('regionId', Type.STRING)
    .addPrimaryKey(['id'], true)
    .addForeignKey('fk_RegionId', {
      action: ConstraintAction.RESTRICT,
      local: 'regionId',
      ref: 'Region.id',
    });

  schemaBuilder
    .createTable('Region')
    .addColumn('id', Type.STRING)
    .addColumn('name', Type.STRING)
    .addPrimaryKey(['id']);

  schemaBuilder
    .createTable('Holiday')
    .addColumn('name', Type.STRING)
    .addColumn('begin', Type.DATE_TIME)
    .addColumn('end', Type.DATE_TIME)
    .addIndex('idx_begin', ['begin'], false, Order.ASC)
    .addPrimaryKey(['name'])
    .persistentIndex(true);

  schemaBuilder
    .createTable('DummyTable')
    .addColumn('arraybuffer', Type.ARRAY_BUFFER)
    .addColumn('boolean', Type.BOOLEAN)
    .addColumn('datetime', Type.DATE_TIME)
    .addColumn('integer', Type.INTEGER)
    .addColumn('number', Type.NUMBER)
    .addColumn('string', Type.STRING)
    .addColumn('string2', Type.STRING)
    .addColumn('proto', Type.OBJECT)
    .addPrimaryKey(['string', 'number'])
    .addUnique('uq_constraint', ['integer', 'string2'])
    .addNullable(['datetime']);

  schemaBuilder
    .createTable('CrossColumnTable')
    .addColumn('integer1', Type.INTEGER)
    .addColumn('integer2', Type.INTEGER)
    .addColumn('string1', Type.STRING)
    .addColumn('string2', Type.STRING)
    .addNullable(['string1', 'string2'])
    .addIndex(
      'idx_ascDesc',
      [
        {
          name: 'integer1',
          order: Order.ASC,
        },
        {
          name: 'integer2',
          order: Order.DESC,
        },
      ],
      true
    )
    .addIndex('idx_crossNull', ['string1', 'string2'], true)
    .persistentIndex(true);

  return schemaBuilder;
}
