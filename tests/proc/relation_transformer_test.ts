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
import {EvalType} from '../../lib/base/eval';
import {Row} from '../../lib/base/row';
import {fn} from '../../lib/fn/fn';
import {JoinPredicate} from '../../lib/pred/join_predicate';
import {Relation} from '../../lib/proc/relation';
import {RelationEntry} from '../../lib/proc/relation_entry';
import {RelationTransformer} from '../../lib/proc/relation_transformer';
import {Column} from '../../lib/schema/column';
import {Table} from '../../lib/schema/table';
import {EmployeeDataGenerator} from '../../testing/hr_schema/employee_data_generator';
import {getHrDbSchemaBuilder} from '../../testing/hr_schema/hr_schema_builder';
import {JobDataGenerator} from '../../testing/hr_schema/job_data_generator';

const assert = chai.assert;

describe('RelationTransformer', () => {
  let j: Table;
  let e: Table;
  let sampleJobs: Row[];
  let sampleEmployees: Row[];

  before(() => {
    const schema = getHrDbSchemaBuilder().getSchema();
    j = schema.table('Job');
    e = schema.table('Employee');
  });

  beforeEach(() => {
    // generateSampleJobData in original test
    const jobGenerator = new JobDataGenerator();
    const jobCount = 10;
    sampleJobs = jobGenerator.generate(jobCount);

    const employeeGenerator = new EmployeeDataGenerator();
    const employeeCount = 2 * jobCount;
    employeeGenerator.setJobCount(jobCount);
    sampleEmployees = employeeGenerator.generate(employeeCount);

    for (let i = 0; i < jobCount; i++) {
      const jobId = sampleJobs[i].payload()['id'];
      // Assigning two employees per job.
      sampleEmployees[2 * i].payload()['jobId'] = jobId;
      sampleEmployees[2 * i + 1].payload()['jobId'] = jobId;
    }
  });

  // Tests the case where no columns are requested.
  it('getTransformed_NoColumns', () => {
    const relation = Relation.fromRows(sampleJobs, [j.getName()]);
    const transformer = new RelationTransformer(relation, []);
    const transformedRelation = transformer.getTransformed();
    transformedRelation.entries.forEach(entry => {
      assert.isEmpty(entry.row.payload());
    });
  });

  // Tests the case where all requested columns are simple (non-aggregated).
  it('getTransformed_SimpleColumnsOnly', () => {
    const columns = [j.col('title'), j.col('minSalary')];
    checkTransformationWithoutJoin(columns, sampleJobs.length);
  });

  // Tests the case where all requested columns are aggregated.
  it('getTransformed_AggregatedColumnsOnly', () => {
    const columns = [fn.min(j.col('maxSalary')), fn.max(j.col('maxSalary'))];
    checkTransformationWithoutJoin(columns, 1);
  });

  // Tests the case where both simple and aggregated columns are requested.
  it('getTransformed_MixedColumns', () => {
    const columns = [
      j.col('title'),
      j.col('maxSalary'),
      fn.avg(j.col('maxSalary')),
    ];
    checkTransformationWithoutJoin(columns, 1);
  });

  // Tests the case where a single DISTINCT column is requested.
  it('getTransformed_DistinctOnly', () => {
    const columns = [fn.distinct(j.col('maxSalary'))];
    checkTransformationWithoutJoin(columns, 2);
  });

  it('getTransformed_SimpleColumnsOnly_Join', () => {
    const columns = [e.col('email'), e.col('hireDate'), j.col('title')];
    checkTransformationWithJoin(columns, sampleEmployees.length);
  });

  it('getTransformed_AggregatedColumnsOnly_Join', () => {
    const columns = [
      fn.min(e.col('hireDate')),
      fn.max(e.col('hireDate')),
      fn.min(j.col('maxSalary')),
      fn.max(j.col('maxSalary')),
    ];
    checkTransformationWithJoin(columns, 1);
  });

  it('getTransformed_MixedColumns_Join', () => {
    const columns = [
      j.col('title'),
      j.col('maxSalary'),
      fn.min(j.col('maxSalary')),
      e.col('email'),
      e.col('hireDate'),
      fn.min(e.col('hireDate')),
    ];
    checkTransformationWithJoin(columns, 1);
  });

  it('getTransformed_DistinctOnly_Join', () => {
    const columns = [fn.distinct(j.col('maxSalary'))];
    checkTransformationWithJoin(columns, 2);
  });

  it('getTransformed_Many', () => {
    // Creating multiple relations where each relation holds two employees that
    // have the same "jobId" field.
    const relations: Relation[] = [];
    for (let i = 0; i < sampleEmployees.length; i += 2) {
      const relation = Relation.fromRows(
        [sampleEmployees[i], sampleEmployees[i + 1]],
        [e.getName()]
      );
      relation.setAggregationResult(fn.avg(e.col('salary')), 50);
      relation.setAggregationResult(fn.max(e.col('salary')), 100);
      relation.setAggregationResult(fn.min(e.col('salary')), 0);

      relations.push(relation);
    }

    const columns = [
      e.col('jobId'),
      fn.min(e.col('salary')),
      fn.max(e.col('salary')),
      fn.avg(e.col('salary')),
    ];

    const transformedRelation = RelationTransformer.transformMany(
      relations,
      columns
    );
    assert.equal(relations.length, transformedRelation.entries.length);
    assertColumnsPopulated(columns, transformedRelation);
  });

  // Checks that performing a transformation on a relationship that is *not* the
  // result of a natural join, results in a relation with fields that are
  // populated as expected.
  function checkTransformationWithoutJoin(
    columns: Column[],
    expectedResultCount: number
  ): Relation {
    const transformer = new RelationTransformer(getRelation(), columns);
    const transformedRelation = transformer.getTransformed();

    assert.equal(expectedResultCount, transformedRelation.entries.length);
    assertColumnsPopulated(columns, transformedRelation);

    return transformedRelation;
  }

  // Checks that performing a transformation on a relationship that is the
  // result of a natural join, results in a relation with fields that are
  // populated as expected.
  function checkTransformationWithJoin(
    columns: Column[],
    expectedResultCount: number
  ): Relation {
    const transformer = new RelationTransformer(getJoinedRelation(), columns);
    const transformedRelation = transformer.getTransformed();

    assert.equal(expectedResultCount, transformedRelation.entries.length);
    assertColumnsPopulated(columns, transformedRelation);

    return transformedRelation;
  }

  // Asserts that all requested columns are populated in the given relation's
  // entries.
  function assertColumnsPopulated(columns: Column[], relation: Relation): void {
    relation.entries.forEach(entry => {
      columns.forEach(column => {
        // Checking that all requested columns are populated.
        const field = entry.getField(column);
        assert.isDefined(field);
        assert.isNotNull(field);
      });
    });
  }

  // Generates a dummy relation, with bogus aggregation results to be used for
  // testing.
  function getRelation(): Relation {
    const relation = Relation.fromRows(sampleJobs, [j.getName()]);

    // Filling in dummy aggregation results. In a normal scenario those have
    // been calculated before ProjectStep executes.
    relation.setAggregationResult(fn.avg(j.col('maxSalary')), 50);
    relation.setAggregationResult(fn.max(j.col('maxSalary')), 100);
    relation.setAggregationResult(fn.min(j.col('maxSalary')), 0);

    const entry1 = new RelationEntry(new Row(1, {maxSalary: 1000}), false);
    const entry2 = new RelationEntry(new Row(1, {maxSalary: 2000}), false);
    const distinctRelation = new Relation([entry1, entry2], [j.getName()]);
    relation.setAggregationResult(
      fn.distinct(j.col('maxSalary')),
      distinctRelation
    );
    return relation;
  }

  // Generates a dummy joined relation, with bogus aggregation results to be
  // used for testing.
  function getJoinedRelation(): Relation {
    const relationLeft = Relation.fromRows(sampleEmployees, [e.getName()]);
    const relationRight = Relation.fromRows(sampleJobs, [j.getName()]);
    const joinPredicate = new JoinPredicate(
      e.col('jobId'),
      j.col('id'),
      EvalType.EQ
    );
    const joinedRelation = joinPredicate.evalRelationsHashJoin(
      relationLeft,
      relationRight,
      false
    );

    joinedRelation.setAggregationResult(fn.avg(j.col('maxSalary')), 50);
    joinedRelation.setAggregationResult(fn.max(j.col('maxSalary')), 100);
    joinedRelation.setAggregationResult(fn.min(j.col('maxSalary')), 0);
    joinedRelation.setAggregationResult(fn.min(e.col('hireDate')), 0);
    joinedRelation.setAggregationResult(fn.max(e.col('hireDate')), 0);

    const entry1 = new RelationEntry(new Row(1, {maxSalary: 1000}), false);
    const entry2 = new RelationEntry(new Row(1, {maxSalary: 2000}), false);
    const distinctRelation = new Relation([entry1, entry2], [j.getName()]);
    joinedRelation.setAggregationResult(
      fn.distinct(j.col('maxSalary')),
      distinctRelation
    );

    return joinedRelation;
  }
});
