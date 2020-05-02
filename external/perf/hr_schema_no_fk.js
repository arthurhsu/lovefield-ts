/**
 * Copyright 2020 The Lovefield Project Authors. All Rights Reserved.
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

import * as lf from './node_modules/lovefield-ts/dist/es6/lf.js';

export function getHrDbSchemaBuilder() {
  const builder = lf.schema.create('hr_noFK', 1);
  builder
      .createTable('Job')
      .addColumn('id', lf.Type.STRING)
      .addColumn('title', lf.Type.STRING)
      .addColumn('minSalary', lf.Type.NUMBER)
      .addColumn('maxSalary', lf.Type.NUMBER)
      .addPrimaryKey(['id'])
      .addIndex('idx_maxSalary', ['maxSalary'], false, lf.Order.DESC);

  builder
      .createTable('JobHistory')
      .addColumn('employeeId', lf.Type.STRING)
      .addColumn('startDate', lf.Type.DATE_TIME)
      .addColumn('endDate', lf.Type.DATE_TIME)
      .addColumn('jobId', lf.Type.STRING)
      .addColumn('departmentId', lf.Type.STRING);

  builder
      .createTable('Employee')
      .addColumn('id', lf.Type.STRING)
      .addColumn('firstName', lf.Type.STRING)
      .addColumn('lastName', lf.Type.STRING)
      .addColumn('email', lf.Type.STRING)
      .addColumn('phoneNumber', lf.Type.STRING)
      .addColumn('hireDate', lf.Type.DATE_TIME)
      .addColumn('jobId', lf.Type.STRING)
      .addColumn('salary', lf.Type.NUMBER)
      .addColumn('commissionPercent', lf.Type.NUMBER)
      .addColumn('managerId', lf.Type.STRING)
      .addColumn('departmentId', lf.Type.STRING)
      .addColumn('photo', lf.Type.ARRAY_BUFFER)
      .addPrimaryKey(['id'])
      .addIndex('idx_salary', ['salary'], false, lf.Order.DESC)
      .addNullable(['hireDate']);

  builder
      .createTable('Department')
      .addColumn('id', lf.Type.STRING)
      .addColumn('name', lf.Type.STRING)
      .addColumn('managerId', lf.Type.STRING)
      .addColumn('locationId', lf.Type.STRING)
      .addPrimaryKey(['id']);

  builder
      .createTable('Location')
      .addColumn('id', lf.Type.STRING)
      .addColumn('streetAddress', lf.Type.STRING)
      .addColumn('postalCode', lf.Type.STRING)
      .addColumn('city', lf.Type.STRING)
      .addColumn('stateProvince', lf.Type.STRING)
      .addColumn('countryId', lf.Type.INTEGER)
      .addPrimaryKey(['id']);

  builder
      .createTable('Country')
      .addColumn('id', lf.Type.INTEGER)
      .addColumn('name', lf.Type.STRING)
      .addColumn('regionId', lf.Type.STRING)
      .addPrimaryKey(['id'], true);

  builder
      .createTable('Region')
      .addColumn('id', lf.Type.STRING)
      .addColumn('name', lf.Type.STRING)
      .addPrimaryKey(['id']);

  builder
      .createTable('Holiday')
      .addColumn('name', lf.Type.STRING)
      .addColumn('begin', lf.Type.DATE_TIME)
      .addColumn('end', lf.Type.DATE_TIME)
      .addIndex('idx_begin', ['begin'], false, lf.Order.ASC)
      .addPrimaryKey(['name'])
      .persistentIndex(true);

  builder
      .createTable('CrossColumnTable')
      .addColumn('integer1', lf.Type.INTEGER)
      .addColumn('integer2', lf.Type.INTEGER)
      .addIndex(
          'idx_ascDesc',
          [
            {
              name: 'integer1',
              order: lf.Order.ASC,
            },
            {
              name: 'integer2',
              order: lf.Order.DESC,
            },
          ],
          true,
      );

  return builder;
}
