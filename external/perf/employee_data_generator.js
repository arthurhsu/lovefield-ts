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
import {Sample} from './sample.js';

export class EmployeeDataGenerator {
  constructor(schema) {
    this.schema = schema;
    this.assignedSalaries = new Set();
    this.jobCount = Sample.JOB_TITLES.length;
    this.departmentCount = Sample.DEPARTMENT_NAMES.length;
  }

  // Sets the number of jobs that will be used for all generated employees.
  setJobCount(count) {
    this.jobCount = Math.min(count, Sample.JOB_TITLES.length);
  }

  // Sets the number of departments that will be used for all generated
  // employees.
  setDepartmentCount(count) {
    this.departmentCount = Math.min(
        count,
        Sample.DEPARTMENT_NAMES.length,
    );
  }

  generate(count) {
    assert(
        count >= this.jobCount,
        'employee count must be greater or equal to job count',
    );
    assert(
        count >= this.departmentCount,
        'employee count must be greater or equal to department count',
    );

    const rawData = this.generateRaw(count);
    const e = this.schema.table('Employee');
    return rawData.map((object) => e.createRow(object));
  }

  generateRaw(count) {
    const employees = new Array(count);
    for (let i = 0; i < count; i++) {
      const firstName = this.genFirstName();
      const lastName = this.genLastName();
      const email =
        firstName.toLowerCase() + '.' + lastName.toLowerCase() + '@theweb.com';
      const phoneNumber = String(
          1000000000 + Math.floor(Math.random() * 999999999),
      );
      const commissionPercent = 0.15 + Math.random();

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
        departmentId:
          i < this.departmentCount ?
            `departmentId${i}` :
            this.genDepartmentId(),
        photo: this.genPhoto(),
      };
    }

    return employees;
  }

  genFirstName() {
    const maxIndex = Sample.FIRST_NAMES.length;
    const index = Math.floor(Math.random() * maxIndex);
    return Sample.FIRST_NAMES[index];
  }

  genLastName() {
    const maxIndex = Sample.LAST_NAMES.length;
    const index = Math.floor(Math.random() * maxIndex);
    return Sample.LAST_NAMES[index];
  }

  genJobId() {
    const index = Math.floor(Math.random() * this.jobCount);
    return `jobId${index}`;
  }

  genDepartmentId() {
    const index = Math.floor(Math.random() * this.departmentCount);
    return `departmentId${index}`;
  }

  genHireDate() {
    // Tue Jan 01 1980 10:00:00 GMT-0800 (PST)
    const min = new Date(315597600000);
    // Fri Sep 12 2014 13:52:20 GMT-0700 (PDT)
    const max = new Date(1410555147354);

    const diff = Math.random() * (max.getTime() - min.getTime());
    return new Date(min.getTime() + diff);
  }

  genSalary() {
    const getNewSalary = () => 10000 + Math.floor(Math.random() * 200000);
    let salary = null;
    do {
      salary = getNewSalary();
    } while (this.assignedSalaries.has(salary));
    this.assignedSalaries.add(salary);
    return salary;
  }

  genPhoto() {
    const buffer = new ArrayBuffer(8);
    const view = new Uint8Array(buffer);
    for (let i = 0; i < 8; ++i) {
      view[i] = i;
    }
    return buffer;
  }
}
