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

import {assert} from '../../lib/base/assert';
import {Row} from '../../lib/base/row';
import {getHrDbSchemaBuilder} from './hr_schema_builder';
import {HRSchemaSamples} from './sample';

export class EmployeeDataGenerator {
  // A bag of salary values used to assign unique salaries to all generated
  // employees.
  private assignedSalaries: Set<number>;
  private jobCount: number;
  private departmentCount: number;

  constructor() {
    this.assignedSalaries = new Set<number>();
    this.jobCount = HRSchemaSamples.JOB_TITLES.length;
    this.departmentCount = HRSchemaSamples.DEPARTMENT_NAMES.length;
  }

  // Sets the number of jobs that will be used for all generated employees.
  public setJobCount(count: number): void {
    this.jobCount = Math.min(count, HRSchemaSamples.JOB_TITLES.length);
  }

  // Sets the number of departments that will be used for all generated
  // employees.
  public setDepartmentCount(count: number): void {
    this.departmentCount =
        Math.min(count, HRSchemaSamples.DEPARTMENT_NAMES.length);
  }

  public generate(count: number): Row[] {
    assert(
        count >= this.jobCount,
        'employee count must be greater or equal to job count');
    assert(
        count >= this.departmentCount,
        'employee count must be greater or equal to department count');

    const rawData = this.generateRaw(count);
    const e = getHrDbSchemaBuilder().getSchema().table('Employee');
    return rawData.map((object) => e.createRow(object));
  }

  private generateRaw(count: number): object[] {
    const employees = new Array<object>(count);
    for (let i = 0; i < count; i++) {
      const firstName = this.genFirstName();
      const lastName = this.genLastName();
      const email = firstName.toLowerCase() + '.' + lastName.toLowerCase() +
          '@theweb.com';
      const phoneNumber =
          String(1000000000 + Math.floor(Math.random() * 999999999));
      const commissionPercent = 0.15 + Math.random();

      // tslint:disable
      employees[i] = {
        id: `employeeId${i}`,
        firstName: firstName,
        lastName: lastName,
        email: email,
        phoneNumber: phoneNumber,
        hireDate: this.genHireDate(),
        // Ensuring that each job is assigned at least one employee.
        jobId: i < this.jobCount ? 'jobId' + i : this.genJobId(),
        salary: this.genSalary(),
        commissionPercent: commissionPercent,
        managerId: 'managerId',
        // Ensuring that each department is assigned at least one employee.
        departmentId: i < this.departmentCount ? `departmentId${i}` :
                                                 this.genDepartmentId(),
        photo: this.genPhoto()
      };
      // tslint:enable
    }

    return employees;
  }

  private genFirstName(): string {
    const maxIndex = HRSchemaSamples.FIRST_NAMES.length;
    const index = Math.floor(Math.random() * maxIndex);
    return HRSchemaSamples.FIRST_NAMES[index];
  }

  private genLastName(): string {
    const maxIndex = HRSchemaSamples.LAST_NAMES.length;
    const index = Math.floor(Math.random() * maxIndex);
    return HRSchemaSamples.LAST_NAMES[index];
  }

  private genJobId(): string {
    const index = Math.floor(Math.random() * this.jobCount);
    return `jobId${index}`;
  }

  private genDepartmentId(): string {
    const index = Math.floor(Math.random() * this.departmentCount);
    return `departmentId${index}`;
  }

  private genHireDate(): Date {
    // Tue Jan 01 1980 10:00:00 GMT-0800 (PST)
    const min = new Date(315597600000);
    // Fri Sep 12 2014 13:52:20 GMT-0700 (PDT)
    const max = new Date(1410555147354);

    const diff = Math.random() * (max.getTime() - min.getTime());
    return new Date(min.getTime() + diff);
  }

  private genSalary(): number {
    const getNewSalary = () => (10000 + Math.floor(Math.random() * 200000));
    let salary = null;
    do {
      salary = getNewSalary();
    } while (this.assignedSalaries.has(salary));
    this.assignedSalaries.add(salary);
    return salary;
  }

  private genPhoto(): ArrayBuffer {
    const buffer = new ArrayBuffer(8);
    const view = new Uint8Array(buffer);
    for (let i = 0; i < 8; ++i) {
      view[i] = i;
    }
    return buffer;
  }
}
