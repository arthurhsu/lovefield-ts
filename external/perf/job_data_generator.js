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
import {shuffle} from './shuffle.js';

export class JobDataGenerator {
  constructor(schema) {
    this.schema = schema;
    this.titles = shuffle(Sample.JOB_TITLES.slice());
  }

  static get SALARY_POOL() {
    return [100000, 200000, 300000, 400000, 500000, 600000];
  }

  generateRaw(count) {
    if (count > this.titles.length) {
      throw new Error(`count can be at most ${this.titles.length}`);
    }

    const jobs = new Array(count);
    for (let i = 0; i < count; i++) {
      const salaries = this.genSalaries();
      jobs[i] = {
        id: `jobId${i}`,
        title: this.titles.shift(),
        minSalary: salaries[0],
        maxSalary: salaries[1],
      };
    }

    return jobs;
  }

  genSalaries() {
    const salary1Index = Math.floor(
        Math.random() * JobDataGenerator.SALARY_POOL.length,
    );
    const salary2Index = Math.floor(
        Math.random() * JobDataGenerator.SALARY_POOL.length,
    );
    return [
      JobDataGenerator.SALARY_POOL[salary1Index],
      JobDataGenerator.SALARY_POOL[salary2Index],
    ].sort((a, b) => a - b);
  }

  generate(count) {
    const rawData = this.generateRaw(count);
    return rawData.map((object) => this.schema.table('Job').createRow(object));
  }
}
