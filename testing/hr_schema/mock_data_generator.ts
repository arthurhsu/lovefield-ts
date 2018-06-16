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

import {Row} from '../../lib/base/row';

interface EmployeeGroundTruth {
  minSalary: number;
  maxSalary: number;
  avgSalary: number;
  stddevSalary: number;
  countSalary: number;
  minHireDate: Date;
  maxHireDate: Date;
  distinctHireDates: Date[];
  employeesPerJob: Map<string, string>;  // TODO(arthurhsu): implement multimap
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

export class MockDataGenerator {
  public sampleJobs: Row[];
  public sampleEmployees: Row[];
  public sampleDepartments: Row[];
  public sampleLocations: Row[];
  public sampleCountries: Row[];
  public sampleRegions: Row[];
  public employeeGroundTruth!: EmployeeGroundTruth;
  public jobGroundTruth!: JobGroundTruth;

  constructor() {
    this.sampleCountries = [];
    this.sampleDepartments = [];
    this.sampleEmployees = [];
    this.sampleJobs = [];
    this.sampleLocations = [];
    this.sampleRegions = [];
  }
}
