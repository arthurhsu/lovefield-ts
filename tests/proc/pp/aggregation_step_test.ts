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

import {EvalType} from '../../../lib/base/eval';
import {Row} from '../../../lib/base/row';
import {AggregatedColumn} from '../../../lib/fn/aggregated_column';
import {fn} from '../../../lib/fn/fn';
import {JoinPredicate} from '../../../lib/pred/join_predicate';
import {AggregationStep} from '../../../lib/proc/pp/aggregation_step';
import {NoOpStep} from '../../../lib/proc/pp/no_op_step';
import {Relation} from '../../../lib/proc/relation';
import {BaseColumn} from '../../../lib/schema/base_column';
import {BaseTable} from '../../../lib/schema/base_table';
import {Database} from '../../../lib/schema/database';
import {getHrDbSchemaBuilder} from '../../../testing/hr_schema/hr_schema_builder';
import {MockDataGenerator} from '../../../testing/hr_schema/mock_data_generator';
import {NullableDataGenerator} from '../../../testing/nullable_data_generator';

const assert = chai.assert;

describe('AggregationStep', () => {
  let e: BaseTable;
  let j: BaseTable;
  let schemaWithNullable: Database;
  let dataGenerator: MockDataGenerator;
  let nullableGenerator: NullableDataGenerator;

  function checkEquals(
      expected: number|string|Date, value: number|string|Date): boolean {
    return expected === value;
  }

  function checkFloatEquals(expected: number, value: number): boolean {
    // The precision to use when comparing floating point numbers.
    const epsilon = Math.pow(10, -9);
    try {
      chai.assert.approximately(expected, value, epsilon);
    } catch (e) {
      return false;
    }
    return true;
  }

  function checkCalculation(
      aggregatedColumn: BaseColumn, expectedValue: number|number[],
      assertFn: (expected: any, actual: any) => boolean): Promise<any> {
    return Promise.all([
      checkCalculationWithoutJoin(aggregatedColumn, expectedValue, assertFn),
      checkCalculationWithJoin(aggregatedColumn, expectedValue, assertFn),
    ]);
  }

  // Checks that performing a transformation on a relationship that is *not* the
  // result of a natural join, results in a relation with fields that are
  // populated as expected.
  function checkCalculationWithoutJoin(
      aggregatedColumn: BaseColumn, expectedValue: number|number[],
      assertFn: (expected: any, actual: any) => boolean): Promise<any> {
    const inputRelation =
        Relation.fromRows(dataGenerator.sampleJobs, [j.getName()]);
    return checkCalculationForRelation(
        inputRelation, aggregatedColumn, expectedValue, assertFn);
  }

  // Checks that performing a transformation on a relationship that is the
  // result of a natural join, results in a relation with fields that are
  // populated as expected.
  function checkCalculationWithJoin(
      aggregatedColumn: BaseColumn, expectedValue: number|number[],
      assertFn: (expected: any, actual: any) => boolean): Promise<any> {
    const relationLeft =
        Relation.fromRows(dataGenerator.sampleEmployees, [e.getName()]);
    const relationRight =
        Relation.fromRows(dataGenerator.sampleJobs, [j.getName()]);
    const joinPredicate = new JoinPredicate(e['jobId'], j['id'], EvalType.EQ);
    const joinedRelation =
        joinPredicate.evalRelationsHashJoin(relationLeft, relationRight, false);
    return checkCalculationForRelation(
        joinedRelation, aggregatedColumn, expectedValue, assertFn);
  }

  function checkCalculationForRelation(
      inputRelation: Relation, aggregatedColumn: BaseColumn, expectedValue: any,
      assertFn: (expected: any, actual: any) => boolean): Promise<any> {
    const childStep = new NoOpStep([inputRelation]);
    const aggregationStep =
        new AggregationStep([aggregatedColumn as AggregatedColumn]);
    aggregationStep.addChild(childStep);

    return aggregationStep.exec().then((relations) => {
      const relation = relations[0];
      if (expectedValue instanceof Array) {
        assert.equal(
            expectedValue.length,
            (relation.getAggregationResult(aggregatedColumn) as Relation)
                .entries.length);
      } else {
        assert.isTrue(assertFn(
            expectedValue, relation.getAggregationResult(aggregatedColumn)));
      }
    });
  }

  // Creates two news rows with null hireDate and adds to the sample employees.
  function getEmployeeDatasetWithNulls(): Row[] {
    const data: Row[] = [];
    const startId = dataGenerator.sampleEmployees.length;
    for (let i = startId; i < startId + 2; i++) {
      const employeeRow = e.createRow();
      employeeRow.payload()['id'] = `employeeId${i.toString()}`;
      employeeRow.payload()['hireDate'] = null;
      data.push(employeeRow);
    }
    return data;
  }

  beforeEach(() => {
    const schema = getHrDbSchemaBuilder().getSchema();
    j = schema.table('Job');
    e = schema.table('Employee');
    dataGenerator = new MockDataGenerator();
    dataGenerator.generate(20, 100, 0);

    // For the tests involving nullable integer columns, a different schema
    // is created. The tables in hr schema do not handle nullable integer
    // column.
    nullableGenerator = new NullableDataGenerator();
    schemaWithNullable = nullableGenerator.schema;
    nullableGenerator.generate();
  });

  it('exec_Min', () => {
    return checkCalculation(
        fn.min(j['maxSalary']), dataGenerator.jobGroundTruth.minMaxSalary,
        checkEquals);
  });

  it('exec_MinNullableColumn', () => {
    const data = getEmployeeDatasetWithNulls();
    const inputRelation = Relation.fromRows(
        dataGenerator.sampleEmployees.concat(data), [e.getName()]);
    return checkCalculationForRelation(
        inputRelation, fn.min(e['hireDate']),
        dataGenerator.employeeGroundTruth.minHireDate, checkEquals);
  });

  it('exec_MinEmptyTable', () => {
    const inputRelation = Relation.fromRows([], [e.getName()]);
    return checkCalculationForRelation(
        inputRelation, fn.min(e['hireDate']), null, checkEquals);
  });

  it('exec_Max', () => {
    return checkCalculation(
        fn.max(j['maxSalary']), dataGenerator.jobGroundTruth.maxMaxSalary,
        checkEquals);
  });

  it('exec_MaxNullableColumn', () => {
    const data = getEmployeeDatasetWithNulls();
    const inputRelation = Relation.fromRows(
        dataGenerator.sampleEmployees.concat(data), [e.getName()]);
    return checkCalculationForRelation(
        inputRelation, fn.max(e['hireDate']),
        dataGenerator.employeeGroundTruth.maxHireDate, checkEquals);
  });

  it('exec_MaxEmptyTable', () => {
    const inputRelation = Relation.fromRows([], [e.getName()]);
    return checkCalculationForRelation(
        inputRelation, fn.max(e['hireDate']), null, checkEquals);
  });

  it('exec_Distinct', () => {
    return checkCalculation(
        fn.distinct(j['maxSalary']),
        dataGenerator.jobGroundTruth.distinctMaxSalary, checkEquals);
  });

  it('exec_DistinctNullableColumn', () => {
    const data = getEmployeeDatasetWithNulls();
    const expectedHireDates =
        dataGenerator.employeeGroundTruth.distinctHireDates;
    expectedHireDates.push(null as any as Date);
    const inputRelation = Relation.fromRows(
        dataGenerator.sampleEmployees.concat(data), [e.getName()]);
    return checkCalculationForRelation(
        inputRelation, fn.distinct(e['hireDate']), expectedHireDates,
        checkEquals);
  });

  // Count on a distinct column ignores nulls returned from distinct.
  it('exec_CountDistinctNullableColumn', () => {
    const data = getEmployeeDatasetWithNulls();
    const inputRelation = Relation.fromRows(
        dataGenerator.sampleEmployees.concat(data), [e.getName()]);
    return checkCalculationForRelation(
        inputRelation, fn.count(fn.distinct(e['hireDate'])),
        dataGenerator.employeeGroundTruth.distinctHireDates.length,
        checkEquals);
  });

  it('exec_Count_Distinct', () => {
    return checkCalculation(
        fn.count(fn.distinct(j['minSalary'])),
        dataGenerator.jobGroundTruth.countDistinctMinSalary, checkEquals);
  });

  it('exec_CountNullableColumn', () => {
    const data = getEmployeeDatasetWithNulls();
    const inputRelation = Relation.fromRows(
        dataGenerator.sampleEmployees.concat(data), [e.getName()]);
    return checkCalculationForRelation(
        inputRelation, fn.count(e['hireDate']),
        dataGenerator.sampleEmployees.length, checkEquals);
  });

  it('exec_CountStar', () => {
    const data = getEmployeeDatasetWithNulls();
    const inputRelation = Relation.fromRows(
        dataGenerator.sampleEmployees.concat(data), [e.getName()]);
    return checkCalculationForRelation(
        inputRelation, fn.count(),
        dataGenerator.sampleEmployees.length + data.length, checkEquals);
  });

  it('exec_Avg_Distinct', () => {
    return checkCalculation(
        fn.avg(fn.distinct(j['minSalary'])),
        dataGenerator.jobGroundTruth.avgDistinctMinSalary, checkFloatEquals);
  });

  // Tests for average distinct on TableA which has a mix of null and
  // non-null values for the column.
  it('exec_AvgDistinctNullableColumn', () => {
    const tableA = schemaWithNullable.table('TableA');
    const inputRelation = Relation.fromRows(
        nullableGenerator.sampleTableARows, [tableA.getName()]);
    return checkCalculationForRelation(
        inputRelation, fn.avg(fn.distinct(tableA['id'])),
        nullableGenerator.tableAGroundTruth.avgDistinctId, checkFloatEquals);
  });

  // Tests for average on TableB which has only null values for the column.
  it('exec_Avg_NullRows', () => {
    const tableB = schemaWithNullable.table('TableB');
    const inputRelation = Relation.fromRows(
        nullableGenerator.sampleTableBRows, [tableB.getName()]);
    return checkCalculationForRelation(
        inputRelation, fn.avg(tableB['id']), null, checkEquals);
  });

  it('exec_Avg_Empty', () => {
    const inputRelation = Relation.createEmpty();
    return checkCalculationForRelation(
        inputRelation, fn.avg(j['maxSalary']), null, checkEquals);
  });

  it('exec_Sum_Distinct', () => {
    return checkCalculation(
        fn.sum(fn.distinct(j['minSalary'])),
        dataGenerator.jobGroundTruth.sumDistinctMinSalary, checkEquals);
  });

  // Tests for sum distinct on TableA which has a mix of null and
  // non-null values for the column.
  it('exec_SumDistinctNullableColumn', () => {
    const tableA = schemaWithNullable.table('TableA');
    const inputRelation = Relation.fromRows(
        nullableGenerator.sampleTableARows, [tableA.getName()]);
    return checkCalculationForRelation(
        inputRelation, fn.sum(fn.distinct(tableA['id'])),
        nullableGenerator.tableAGroundTruth.sumDistinctId, checkEquals);
  });

  // Tests for sum on empty table.
  it('exec_SumEmptyTable', () => {
    const inputRelation = Relation.createEmpty();
    return checkCalculationForRelation(
        inputRelation, fn.sum(j['maxSalary']), null, checkEquals);
  });

  // Tests for sum on TableB which has only null values for the column.
  it('exec_Sum_NullRows', () => {
    const tableB = schemaWithNullable.table('TableB');
    const inputRelation = Relation.fromRows(
        nullableGenerator.sampleTableBRows, [tableB.getName()]);
    return checkCalculationForRelation(
        inputRelation, fn.sum(tableB['id']), null, checkEquals);
  });

  it('exec_Stddev_Distinct', () => {
    return checkCalculation(
        fn.stddev(fn.distinct(j['minSalary'])),
        dataGenerator.jobGroundTruth.stddevDistinctMinSalary, checkFloatEquals);
  });

  // Tests for Stddev distinct on TableA which has a mix of null and
  // non-null values for the column.
  it('exec_StddevDistinctNullableColumn', () => {
    const tableA = schemaWithNullable.table('TableA');
    const inputRelation = Relation.fromRows(
        nullableGenerator.sampleTableARows, [tableA.getName()]);
    return checkCalculationForRelation(
        inputRelation, fn.stddev(fn.distinct(tableA['id'])),
        nullableGenerator.tableAGroundTruth.stddevDistinctId, checkFloatEquals);
  });

  // Tests for Stddev on empty table.
  it('exec_StddevEmptyTable', () => {
    const inputRelation = Relation.createEmpty();
    return checkCalculationForRelation(
        inputRelation, fn.stddev(j['maxSalary']), null, checkEquals);
  });

  // Tests for Stddev on TableB which has only null values for the column.
  it('exec_Stddev_NullRows', () => {
    const tableB = schemaWithNullable.table('TableB');
    const inputRelation = Relation.fromRows(
        nullableGenerator.sampleTableBRows, [tableB.getName()]);
    return checkCalculationForRelation(
        inputRelation, fn.stddev(tableB['id']), null, checkEquals);
  });

  it('exec_Geomean_Distinct', () => {
    return checkCalculation(
        fn.geomean(fn.distinct(j['maxSalary'])),
        dataGenerator.jobGroundTruth.geomeanDistinctMaxSalary,
        checkFloatEquals);
  });

  it('exec_Geomean_Empty', () => {
    return checkCalculationForRelation(
        Relation.createEmpty(), fn.geomean(j['maxSalary']), null, checkEquals);
  });

  // Tests for geomean distinct on TableA which has a mix of null and
  // non-null values for the column.
  it('exec_GeomeanDistinctNullableColumn', () => {
    const tableA = schemaWithNullable.table('TableA');
    const inputRelation = Relation.fromRows(
        nullableGenerator.sampleTableARows, [tableA.getName()]);
    return checkCalculationForRelation(
        inputRelation, fn.geomean(fn.distinct(tableA['id'])),
        nullableGenerator.tableAGroundTruth.geomeanDistinctId,
        checkFloatEquals);
  });

  // Tests for Geomean on TableB which has only null values for the column.
  it('exec_Geomean_NullRows', () => {
    const tableB = schemaWithNullable.table('TableB');
    const inputRelation = Relation.fromRows(
        nullableGenerator.sampleTableBRows, [tableB.getName()]);
    return checkCalculationForRelation(
        inputRelation, fn.geomean(tableB['id']), null, checkEquals);
  });

  // Tests that AggregationStep is using existing aggregation result
  // (pre-calculated by previous steps).
  it('exec_UsesExistingResult', () => {
    const inputRelation = Relation.fromRows([], [j.getName()]);
    const aggregatedColumn = fn.count();
    const aggregationResult = 100;
    inputRelation.setAggregationResult(aggregatedColumn, aggregationResult);
    const childStep = new NoOpStep([inputRelation]);
    const aggregationStep =
        new AggregationStep([aggregatedColumn as AggregatedColumn]);
    aggregationStep.addChild(childStep);

    return aggregationStep.exec().then((relations) => {
      assert.equal(
          aggregationResult,
          relations[0].getAggregationResult(aggregatedColumn));
    });
  });
});
