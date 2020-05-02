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

import {DepartmentDataGenerator} from './department_data_generator.js';
import {EmployeeDataGenerator} from './employee_data_generator.js';
import {JobDataGenerator} from './job_data_generator.js';
import {MathHelper} from './math_helper.js';

export class MockDataGenerator {
  constructor(schema) {
    this.schema = schema;
    this.sampleJobs = [];
    this.sampleEmployees = [];
    this.sampleDepartments = [];
    this.sampleLocations = [];
    this.sampleCountries = [];
    this.sampleRegions = [];
  }

  static fromExportData(schema, data) {
    const deserialize = (tableSchema, obj) => {
      return tableSchema.deserializeRow({
        id: window.top['#lfRowId'](),
        value: obj,
      });
    };

    const employeeSchema = schema.table('Employee');
    const employees = data.employees.map((obj) =>
      deserialize(employeeSchema, obj),
    );

    const jobSchema = schema.table('Job');
    const jobs = data.jobs.map((obj) => deserialize(jobSchema, obj));

    const departmentSchema = schema.table('Department');
    const departments = data.departments.map((obj) =>
      deserialize(departmentSchema, obj),
    );

    const generator = new MockDataGenerator();
    generator.sampleJobs = jobs;
    generator.sampleEmployees = employees;
    generator.sampleDepartments = departments;
    generator.jobGroundTruth = generator.extractJobGroundTruth();
    generator.employeeGroundTruth = generator.extractEmployeeGroundTruth();

    return generator;
  }

  generate(jobCount, employeeCount, departmentCount) {
    const employeeGenerator = new EmployeeDataGenerator(this.schema);

    employeeGenerator.setJobCount(jobCount);
    employeeGenerator.setDepartmentCount(departmentCount);
    this.sampleEmployees = employeeGenerator.generate(employeeCount);

    const jobGenerator = new JobDataGenerator(this.schema);
    this.sampleJobs = jobGenerator.generate(jobCount);

    const departmentGenerator = new DepartmentDataGenerator(this.schema);
    this.sampleDepartments = departmentGenerator.generate(departmentCount);

    const location = this.schema.table('Location');
    this.sampleLocations = [
      location.createRow({
        id: 'locationId',
        streetAddress: 'dummyStreetAddress',
        postalCode: 'dummyPostalCode',
        city: 'dummyCity',
        stateProvince: 'dummyStateProvince',
        countryId: 1,
      }),
    ];
    const country = this.schema.table('Country');
    this.sampleCountries = [
      country.createRow({
        id: 1,
        name: 'dummyCountryName',
        regionId: 'regionId',
      }),
      country.createRow({
        id: 2,
        name: 'dummyCountryName',
        regionId: 'regionId',
      }),
    ];
    const region = this.schema.table('Region');
    this.sampleRegions = [
      region.createRow({
        id: 'regionId',
        name: 'dummyRegionName',
      }),
      region.createRow({
        id: 'regionId2',
        name: 'dummyRegionName2',
      }),
      region.createRow({
        id: 'regionId3',
        name: 'dummyRegionName2',
      }),
    ];

    this.jobGroundTruth = this.extractJobGroundTruth();
    this.employeeGroundTruth = this.extractEmployeeGroundTruth();
  }

  exportData() {
    const employeesPayloads = this.sampleEmployees.map((employee) =>
      employee.toDbPayload(),
    );
    const jobsPayloads = this.sampleJobs.map((job) => job.toDbPayload());
    const departmentsPayloads = this.sampleDepartments.map((department) =>
      department.toDbPayload(),
    );

    return {
      departments: departmentsPayloads,
      employees: employeesPayloads,
      jobs: jobsPayloads,
    };
  }

  extractJobGroundTruth() {
    const minSalary = (job) => job.payload()['minSalary'];
    const maxSalary = (job) => job.payload()['maxSalary'];

    return {
      minMinSalary: this.findJobMin(minSalary),
      maxMinSalary: this.findJobMax(minSalary),
      distinctMinSalary: this.findJobDistinct(minSalary),
      sumDistinctMinSalary: MathHelper.sum.apply(
          null,
          this.findJobDistinct(minSalary),
      ),
      countDistinctMinSalary: this.findJobDistinct(minSalary).length,
      avgDistinctMinSalary: MathHelper.average.apply(
          null,
          this.findJobDistinct(minSalary),
      ),
      stddevDistinctMinSalary: MathHelper.standardDeviation.apply(
          null,
          this.findJobDistinct(minSalary),
      ),
      minMaxSalary: this.findJobMin(maxSalary),
      maxMaxSalary: this.findJobMax(maxSalary),
      distinctMaxSalary: this.findJobDistinct(maxSalary),
      sumDistinctMaxSalary: MathHelper.sum.apply(
          null,
          this.findJobDistinct(maxSalary),
      ),
      countDistinctMaxSalary: this.findJobDistinct(maxSalary).length,
      avgDistinctMaxSalary: MathHelper.average.apply(
          null,
          this.findJobDistinct(maxSalary),
      ),
      stddevDistinctMaxSalary: MathHelper.standardDeviation.apply(
          null,
          this.findJobDistinct(maxSalary),
      ),
      geomeanDistinctMaxSalary: this.findGeomean(
          this.findJobDistinct(maxSalary),
      ),
      selfJoinSalary: this.findSelfJoinSalary(),
    };
  }

  extractEmployeeGroundTruth() {
    const salary = (employee) => employee.payload()['salary'];
    const hireDate = (employee) => employee.payload()['hireDate'];
    return {
      employeesPerJob: this.findEmployeesPerJob(),
      minSalary: 0,
      maxSalary: 0,
      avgSalary: MathHelper.average(...this.sampleEmployees.map(salary)),
      stddevSalary: MathHelper.standardDeviation(
          ...this.sampleEmployees.map(salary)),
      countSalary: 0,
      distinctHireDates: this.findDistinct(hireDate, this.sampleEmployees),
      minHireDate: this.findEmployeeMinDate(),
      maxHireDate: this.findEmployeeMaxDate(),
      thetaJoinSalaryIds: this.findThetaJoinSalaryIds(),
    };
  }

  // Finds the MIN of a given attribute in the Job table.
  findJobMin(getterFn) {
    const jobsSorted = this.sampleJobs
        .slice()
        .sort((job1, job2) => getterFn(job1) - getterFn(job2));
    return getterFn(jobsSorted[0]);
  }

  // Finds the MAX of a given attribute in the Job table.
  findJobMax(getterFn) {
    const jobsSorted = this.sampleJobs
        .slice()
        .sort((job1, job2) => getterFn(job2) - getterFn(job1));
    return getterFn(jobsSorted[0]);
  }

  // Finds the DISTINCT of a given attribute in the Job table.
  findDistinct(getterFn, rows) {
    const valueSet = new Set();
    rows.forEach((row) => valueSet.add(getterFn(row)));
    return Array.from(valueSet.values());
  }

  findJobDistinct(getterFn) {
    return this.findDistinct(getterFn, this.sampleJobs);
  }

  // Finds all job pairs where j1.minSalary == j2.maxSalary.
  findSelfJoinSalary() {
    const result = [];

    this.sampleJobs.forEach((job1) => {
      this.sampleJobs.forEach((job2) => {
        if (job1.payload()['minSalary'] === job2.payload()['maxSalary']) {
          result.push([job1, job2]);
        }
      });
    });

    // Sorting results to be in deterministic order such that they can be
    // useful for assertions.
    result.sort((jobPair1, jobPair2) => {
      const jp1id0 = jobPair1[0].payload()['id'];
      const jp2id0 = jobPair2[0].payload()['id'];
      if (jp1id0 < jp2id0) {
        return -1;
      } else if (jp1id0 > jp2id0) {
        return 1;
      } else {
        const jp1id1 = jobPair1[1].payload()['id'];
        const jp2id1 = jobPair2[1].payload()['id'];
        if (jp1id1 < jp2id1) {
          return -1;
        } else if (jp1id1 > jp2id1) {
          return 1;
        }
        return 0;
      }
    });

    return result;
  }

  findGeomean(values) {
    const reduced = values.reduce(
        (soFar, value) => (soFar += Math.log(value)),
        0,
    );
    return Math.pow(Math.E, reduced / values.length);
  }

  // Find the association between Jobs and Employees.
  findEmployeesPerJob() {
    const employeesPerJob = new Map();
    this.sampleEmployees.forEach((employee) => {
      const key = employee.payload()['jobId'];
      const value = employee.payload()['id'];
      if (!employeesPerJob.has(key)) {
        employeesPerJob.set(key, [value]);
      } else {
        employeesPerJob.get(key).push(value);
      }
    });
    return employeesPerJob;
  }

  // Find the MIN hireDate attribute in the Employee table.
  findEmployeeMinDate() {
    const employeesSorted = this.sampleEmployees
        .slice()
        .sort(
            (employee1, employee2) =>
              (employee1.payload()['hireDate']).getTime() -
          (employee2.payload()['hireDate']).getTime(),
        );
    return employeesSorted[0].payload()['hireDate'];
  }

  // Find the MAX hireDate attribute in the Employee table.
  findEmployeeMaxDate() {
    const employeesSorted = this.sampleEmployees
        .slice()
        .sort(
            (employee1, employee2) =>
              (employee2.payload()['hireDate']).getTime() -
          (employee1.payload()['hireDate']).getTime(),
        );
    return employeesSorted[0].payload()['hireDate'];
  }

  // Finds the IDs of all employees whose salary is larger than the MAX salary
  // for their job title.
  // Note: This is possible because generated employee data does not respect
  // corresponding min/max job salary.
  findThetaJoinSalaryIds() {
    const employeeIds = [];

    this.sampleEmployees.forEach((employee) => {
      this.sampleJobs.forEach((job) => {
        if (
          employee.payload()['jobId'] === job.payload()['id'] &&
          (employee.payload()['salary']) >
            (job.payload()['maxSalary'])
        ) {
          employeeIds.push(employee.payload()['id']);
        }
      });
    });

    return employeeIds.sort();
  }
}
