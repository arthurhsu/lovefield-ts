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
import {ArrayHelper} from '../../lib/structs/array_helper';
import {getHrDbSchemaBuilder} from './hr_schema_builder';
import {HRSchemaSamples} from './sample';

export class JobDataGenerator {
  private static SALARY_POOL = [
    100000,
    200000,
    300000,
    400000,
    500000,
    600000,
  ];

  private titles: string[];
  constructor() {
    this.titles = HRSchemaSamples.JOB_TITLES.slice();
    ArrayHelper.shuffle(this.titles);
  }

  public generate(count: number): Row[] {
    const j = getHrDbSchemaBuilder().getSchema().table('Job');
    const rawData = this.generateRaw(count);
    return rawData.map((object) => j.createRow(object));
  }

  private generateRaw(count: number): object[] {
    assert(
        count <= this.titles.length,
        `count can be at most ${this.titles.length}`);
    const jobs = new Array<object>(count);
    for (let i = 0; i < count; i++) {
      const salaries = this.genSalaries();
      jobs[i] = {
        id: 'jobId' + i.toString(),
        maxSalary: salaries[1],
        minSalary: salaries[0],
        title: this.titles.shift(),
      };
    }
    return jobs;
  }

  private genSalaries(): number[] {
    const salary1Index =
        Math.floor(Math.random() * JobDataGenerator.SALARY_POOL.length);
    const salary2Index =
        Math.floor(Math.random() * JobDataGenerator.SALARY_POOL.length);
    return [
      JobDataGenerator.SALARY_POOL[salary1Index],
      JobDataGenerator.SALARY_POOL[salary2Index],
    ].sort((a, b) => (a - b));
  }
}
