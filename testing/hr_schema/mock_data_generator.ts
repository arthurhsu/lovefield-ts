/**
 * Copyright 2018 The Lovefield Project Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {PayloadType, Row} from '../../lib/base/row';
import {Table} from '../../lib/schema/table';
import {BaseTable} from '../../lib/schema/base_table';
import {MathHelper} from '../../lib/structs/math_helper';
import {DepartmentDataGenerator} from './department_data_generator';
import {EmployeeDataGenerator} from './employee_data_generator';
import {getHrDbSchemaBuilder} from './hr_schema_builder';
import {JobDataGenerator} from './job_data_generator';

interface EmployeeGroundTruth {
  minSalary: number;
  maxSalary: number;
  avgSalary: number;
  stddevSalary: number;
  countSalary: number;
  minHireDate: Date;
  maxHireDate: Date;
  distinctHireDates: Date[];
  employeesPerJob: Map<string, string[]>;
  thetaJoinSalaryIds: string[];
}

interface JobGroundTruth {
  minMinSalary: number;
  maxMinSalary: number;
  distinctMinSalary: number[];
  sumDistinctMinSalary: number;
  countDistinctMinSalary: number;
  avgDistinctMinSalary: number;
  stddevDistinctMinSalary: number;
  minMaxSalary: number;
  maxMaxSalary: number;
  distinctMaxSalary: number[];
  sumDistinctMaxSalary: number;
  countDistinctMaxSalary: number;
  avgDistinctMaxSalary: number;
  stddevDistinctMaxSalary: number;
  geomeanDistinctMaxSalary: number;
  selfJoinSalary: Row[][];
}

interface ExportData {
  departments: PayloadType[];
  employees: PayloadType[];
  jobs: PayloadType[];
}

export class MockDataGenerator {
  static fromExportData(data: ExportData): MockDataGenerator {
    const schema = getHrDbSchemaBuilder().getSchema();
    const deserialize = (tableSchema: Table, obj: PayloadType) => {
      return (tableSchema as BaseTable).deserializeRow({
        id: Row.getNextId(),
        value: obj,
      });
    };

    const employeeSchema = schema.table('Employee');
    const employees = data.employees.map(obj =>
      deserialize(employeeSchema, obj)
    );

    const jobSchema = schema.table('Job');
    const jobs = data.jobs.map(obj => deserialize(jobSchema, obj));

    const departmentSchema = schema.table('Department');
    const departments = data.departments.map(obj =>
      deserialize(departmentSchema, obj)
    );

    const generator = new MockDataGenerator();
    generator.sampleJobs = jobs;
    generator.sampleEmployees = employees;
    generator.sampleDepartments = departments;
    generator.jobGroundTruth = generator.extractJobGroundTruth();
    generator.employeeGroundTruth = generator.extractEmployeeGroundTruth();

    return generator;
  }

  sampleJobs: Row[];
  sampleEmployees: Row[];
  sampleDepartments: Row[];
  sampleLocations: Row[];
  sampleCountries: Row[];
  sampleRegions: Row[];
  employeeGroundTruth!: EmployeeGroundTruth;
  jobGroundTruth!: JobGroundTruth;

  constructor() {
    this.sampleCountries = [];
    this.sampleDepartments = [];
    this.sampleEmployees = [];
    this.sampleJobs = [];
    this.sampleLocations = [];
    this.sampleRegions = [];
  }

  generate(
    jobCount: number,
    employeeCount: number,
    departmentCount: number
  ): void {
    const employeeGenerator = new EmployeeDataGenerator();
    employeeGenerator.setJobCount(jobCount);
    employeeGenerator.setDepartmentCount(departmentCount);
    this.sampleEmployees = employeeGenerator.generate(employeeCount);

    const jobGenerator = new JobDataGenerator();
    this.sampleJobs = jobGenerator.generate(jobCount);

    const departmentGenerator = new DepartmentDataGenerator();
    this.sampleDepartments = departmentGenerator.generate(departmentCount);

    const schema = getHrDbSchemaBuilder().getSchema();
    const location = schema.table('Location');
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
    const country = schema.table('Country');
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
    const region = schema.table('Region');
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
        name: 'dummyRegionName3',
      }),
    ];

    this.jobGroundTruth = this.extractJobGroundTruth();
    this.employeeGroundTruth = this.extractEmployeeGroundTruth();
  }

  exportData(): ExportData {
    const employeesPayloads = this.sampleEmployees.map(employee =>
      employee.toDbPayload()
    );
    const jobsPayloads = this.sampleJobs.map(job => job.toDbPayload());
    const departmentsPayloads = this.sampleDepartments.map(department =>
      department.toDbPayload()
    );

    return {
      departments: departmentsPayloads,
      employees: employeesPayloads,
      jobs: jobsPayloads,
    };
  }

  private extractJobGroundTruth(): JobGroundTruth {
    const minSalary = (job: Row) => job.payload()['minSalary'] as number;
    const maxSalary = (job: Row) => job.payload()['maxSalary'] as number;

    return {
      minMinSalary: this.findJobMin(minSalary),
      maxMinSalary: this.findJobMax(minSalary),
      distinctMinSalary: this.findJobDistinct(minSalary),
      sumDistinctMinSalary: MathHelper.sum.apply(
        null,
        this.findJobDistinct(minSalary)
      ),
      countDistinctMinSalary: this.findJobDistinct(minSalary).length,
      avgDistinctMinSalary: MathHelper.average.apply(
        null,
        this.findJobDistinct(minSalary)
      ),
      stddevDistinctMinSalary: MathHelper.standardDeviation.apply(
        null,
        this.findJobDistinct(minSalary)
      ),
      minMaxSalary: this.findJobMin(maxSalary),
      maxMaxSalary: this.findJobMax(maxSalary),
      distinctMaxSalary: this.findJobDistinct(maxSalary),
      sumDistinctMaxSalary: MathHelper.sum.apply(
        null,
        this.findJobDistinct(maxSalary)
      ),
      countDistinctMaxSalary: this.findJobDistinct(maxSalary).length,
      avgDistinctMaxSalary: MathHelper.average.apply(
        null,
        this.findJobDistinct(maxSalary)
      ),
      stddevDistinctMaxSalary: MathHelper.standardDeviation.apply(
        null,
        this.findJobDistinct(maxSalary)
      ),
      geomeanDistinctMaxSalary: this.findGeomean(
        this.findJobDistinct(maxSalary)
      ),
      selfJoinSalary: this.findSelfJoinSalary(),
    };
  }

  private extractEmployeeGroundTruth(): EmployeeGroundTruth {
    const salary = (employee: Row) => employee.payload()['salary'] as number;
    const hireDate = (employee: Row) => employee.payload()['hireDate'] as Date;
    return {
      employeesPerJob: this.findEmployeesPerJob(),
      minSalary: 0,
      maxSalary: 0,
      avgSalary: MathHelper.average.apply(
        null,
        this.sampleEmployees.map(salary)
      ),
      stddevSalary: MathHelper.standardDeviation.apply(
        null,
        this.sampleEmployees.map(salary)
      ),
      countSalary: 0,
      distinctHireDates: this.findDistinct(hireDate, this.sampleEmployees),
      minHireDate: this.findEmployeeMinDate(),
      maxHireDate: this.findEmployeeMaxDate(),
      thetaJoinSalaryIds: this.findThetaJoinSalaryIds(),
    };
  }

  // Finds the MIN of a given attribute in the Job table.
  private findJobMin(getterFn: (row: Row) => number): number {
    const jobsSorted = this.sampleJobs
      .slice()
      .sort((job1, job2) => getterFn(job1) - getterFn(job2));
    return getterFn(jobsSorted[0]);
  }

  // Finds the MAX of a given attribute in the Job table.
  private findJobMax(getterFn: (row: Row) => number): number {
    const jobsSorted = this.sampleJobs
      .slice()
      .sort((job1, job2) => getterFn(job2) - getterFn(job1));
    return getterFn(jobsSorted[0]);
  }

  // Finds the DISTINCT of a given attribute in the Job table.
  private findDistinct<T>(getterFn: (row: Row) => T, rows: Row[]): T[] {
    const valueSet = new Set<T>();
    rows.forEach(row => valueSet.add(getterFn(row)));
    return Array.from(valueSet.values());
  }

  private findJobDistinct(getterFn: (row: Row) => number): number[] {
    return this.findDistinct(getterFn, this.sampleJobs);
  }

  // Finds all job pairs where j1.minSalary == j2.maxSalary.
  private findSelfJoinSalary(): Row[][] {
    const result: Row[][] = [];

    this.sampleJobs.forEach(job1 => {
      this.sampleJobs.forEach(job2 => {
        if (job1.payload()['minSalary'] === job2.payload()['maxSalary']) {
          result.push([job1, job2]);
        }
      });
    });

    // Sorting results to be in deterministic order such that they can be
    // useful for assertions.
    result.sort((jobPair1, jobPair2) => {
      const jp1id0 = jobPair1[0].payload()['id'] as string;
      const jp2id0 = jobPair2[0].payload()['id'] as string;
      if (jp1id0 < jp2id0) {
        return -1;
      } else if (jp1id0 > jp2id0) {
        return 1;
      } else {
        const jp1id1 = jobPair1[1].payload()['id'] as string;
        const jp2id1 = jobPair2[1].payload()['id'] as string;
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

  private findGeomean(values: number[]): number {
    const reduced = values.reduce(
      (soFar, value) => (soFar += Math.log(value)),
      0
    );
    return Math.pow(Math.E, reduced / values.length);
  }

  // Find the association between Jobs and Employees.
  private findEmployeesPerJob(): Map<string, string[]> {
    const employeesPerJob = new Map<string, string[]>();
    this.sampleEmployees.forEach(employee => {
      const key = employee.payload()['jobId'] as string;
      const value = employee.payload()['id'] as string;
      if (!employeesPerJob.has(key)) {
        employeesPerJob.set(key, [value]);
      } else {
        (employeesPerJob.get(key) as string[]).push(value);
      }
    });
    return employeesPerJob;
  }

  // Find the MIN hireDate attribute in the Employee table.
  private findEmployeeMinDate(): Date {
    const employeesSorted = this.sampleEmployees
      .slice()
      .sort(
        (employee1, employee2) =>
          (employee1.payload()['hireDate'] as Date).getTime() -
          (employee2.payload()['hireDate'] as Date).getTime()
      );
    return employeesSorted[0].payload()['hireDate'] as Date;
  }

  // Find the MAX hireDate attribute in the Employee table.
  private findEmployeeMaxDate(): Date {
    const employeesSorted = this.sampleEmployees
      .slice()
      .sort(
        (employee1, employee2) =>
          (employee2.payload()['hireDate'] as Date).getTime() -
          (employee1.payload()['hireDate'] as Date).getTime()
      );
    return employeesSorted[0].payload()['hireDate'] as Date;
  }

  // Finds the IDs of all employees whose salary is larger than the MAX salary
  // for their job title.
  // Note: This is possible because generated employee data does not respect
  // corresponding min/max job salary.
  private findThetaJoinSalaryIds(): string[] {
    const employeeIds: string[] = [];

    this.sampleEmployees.forEach(employee => {
      this.sampleJobs.forEach(job => {
        if (
          employee.payload()['jobId'] === job.payload()['id'] &&
          (employee.payload()['salary'] as number) >
            (job.payload()['maxSalary'] as number)
        ) {
          employeeIds.push(employee.payload()['id'] as string);
        }
      });
    });

    return employeeIds.sort();
  }
}
