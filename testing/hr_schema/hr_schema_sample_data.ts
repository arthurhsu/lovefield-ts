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

import { Row } from '../../lib/base/row';
import { getHrDbSchemaBuilder } from './hr_schema_builder';

export class HRSchemaSampleData {
  static generateSampleEmployeeData(): Row {
    const buffer = new ArrayBuffer(8);
    const view = new Uint8Array(buffer);
    for (let i = 0; i < 8; ++i) {
      view[i] = i;
    }

    return getHrDbSchemaBuilder()
      .getSchema()
      .table('Employee')
      .createRow({
        id: 'empId',
        firstName: 'John',
        lastName: 'Doe',
        email: 'john.doe@neverland.com',
        phoneNumber: '123456',
        // 'Fri Feb 01 1985 14:15:00 GMT-0800 (PST)'
        hireDate: new Date(476144100000),
        jobId: 'jobId',
        salary: 100,
        commissionPercent: 0.15,
        managerId: 'managerId',
        departmentId: 'departmentId',
        photo: buffer,
      });
  }

  static generateSampleJobData(): Row {
    return getHrDbSchemaBuilder()
      .getSchema()
      .table('Job')
      .createRow({
        id: 'jobId',
        title: 'Software Engineer',
        minSalary: 100000,
        maxSalary: 500000,
      });
  }

  static generateSampleDepartmentData(): Row {
    return getHrDbSchemaBuilder()
      .getSchema()
      .table('Department')
      .createRow({
        id: 'departmentId',
        name: 'departmentName',
        managerId: 'managerId',
        locationId: 'locationId',
      });
  }

  static generateSampleJobHistoryData(): Row {
    return getHrDbSchemaBuilder()
      .getSchema()
      .table('JobHistory')
      .createRow({
        employeeId: 'employeeId',
        // 'Fri Feb 01 1985 14:15:00 GMT-0800 (PST)'
        startDate: new Date(476144100000),
        // 'Fri Feb 01 1986 14:15:00 GMT-0800 (PST)'
        endDate: new Date(507680100000),
        jobId: 'jobId',
        departmentId: 'departmentId',
      });
  }
}
