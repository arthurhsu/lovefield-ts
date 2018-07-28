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

import * as chai from 'chai';

import {GroupByStep} from '../../../lib/proc/pp/group_by_step';
import {NoOpStep} from '../../../lib/proc/pp/no_op_step';
import {Relation} from '../../../lib/proc/relation';
import {BaseTable} from '../../../lib/schema/base_table';
import {getHrDbSchemaBuilder} from '../../../testing/hr_schema/hr_schema_builder';
import {MockDataGenerator} from '../../../testing/hr_schema/mock_data_generator';

const assert = chai.assert;

describe('GroupByStep', () => {
  let e: BaseTable;
  let j: BaseTable;
  let dataGenerator: MockDataGenerator;

  beforeEach(() => {
    const schema = getHrDbSchemaBuilder().getSchema();
    j = schema.table('Job');
    e = schema.table('Employee');
    dataGenerator = new MockDataGenerator();
    dataGenerator.generate(20, 100, 0);
  });

  // Tests GroupByStep#exec() method for the case where grouping is performed on
  // a single column.
  it('exec_SingleColumn', () => {
    const inputRelation =
        Relation.fromRows(dataGenerator.sampleEmployees, [e.getName()]);
    const childStep = new NoOpStep([inputRelation]);
    const groupByStep = new GroupByStep([e['jobId']]);
    groupByStep.addChild(childStep);

    const employeesPerJob = dataGenerator.employeeGroundTruth.employeesPerJob;
    return groupByStep.exec().then((relations) => {
      const jobIds = Array.from(employeesPerJob.keys());
      assert.equal(jobIds.length, relations.length);
      relations.forEach((relation) => {
        const jobId = relation.entries[0].getField(e['jobId']) as string;
        const expectedRows = employeesPerJob.get(jobId) as string[];
        assert.equal(expectedRows.length, relation.entries.length);
        relation.entries.forEach((entry) => {
          assert.equal(jobId, entry.getField(e['jobId']));
        });
      });
    });
  });

  // Tests GroupByStep#exec() method for the case where grouping is performed on
  // multiple columns.
  it('exec_MultiColumn', () => {
    const inputRelation =
        Relation.fromRows(dataGenerator.sampleJobs, [j.getName()]);
    const childStep = new NoOpStep([inputRelation]);
    const groupByStep = new GroupByStep([j['minSalary'], j['maxSalary']]);
    groupByStep.addChild(childStep);

    return groupByStep.exec().then((relations) => {
      let jobCount = 0;
      relations.forEach((relation) => {
        const groupByMinSalary = relation.entries[0].getField(j['minSalary']);
        const groupByMaxSalary = relation.entries[0].getField(j['maxSalary']);
        relation.entries.forEach((entry) => {
          assert.equal(groupByMinSalary, entry.getField(j['minSalary']));
          assert.equal(groupByMaxSalary, entry.getField(j['maxSalary']));
          jobCount++;
        });
      });
      assert.equal(inputRelation.entries.length, jobCount);
    });
  });
});
