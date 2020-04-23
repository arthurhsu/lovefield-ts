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

import { EvalType } from '../../lib/base/eval';
import { Row, PayloadType } from '../../lib/base/row';
import { JoinPredicate } from '../../lib/pred/join_predicate';
import { Predicate } from '../../lib/pred/predicate';
import { Relation } from '../../lib/proc/relation';
import { RelationEntry } from '../../lib/proc/relation_entry';
import { Column } from '../../lib/schema/column';
import { DatabaseSchema } from '../../lib/schema/database_schema';
import { BaseTable } from '../../lib/schema/base_table';
import { Table } from '../../lib/schema/table';
import { getHrDbSchemaBuilder } from '../../testing/hr_schema/hr_schema_builder';
import { HRSchemaSampleData } from '../../testing/hr_schema/hr_schema_sample_data';
import { NullableDataGenerator } from '../../testing/nullable_data_generator';

const assert = chai.assert;

interface SampleDataType {
  departments: Row[];
  employees: Row[];
  jobs: Row[];
}

describe('JoinPredicate', () => {
  let db: DatabaseSchema;
  let d: Table;
  let e: BaseTable;
  let j: BaseTable;
  let schemaWithNullable: DatabaseSchema;
  let nullableGenerator: NullableDataGenerator;

  before(() => {
    db = getHrDbSchemaBuilder().getSchema();
    d = db.table('Department');
    e = db.table('Employee') as BaseTable;
    j = db.table('Job') as BaseTable;

    // For the tests involving nullable columns, a different schema
    // is created. The tables in hr schema do not handle nullable column.
    nullableGenerator = new NullableDataGenerator();
    schemaWithNullable = nullableGenerator.schema;
    nullableGenerator.generate();
  });

  it('copy', () => {
    const original = e.col('jobId').eq(j.col('id')) as JoinPredicate;
    const copy = original.copy();

    assert.isTrue(original instanceof JoinPredicate);
    assert.isTrue(copy instanceof JoinPredicate);
    assert.notEqual(original, copy);
    assert.equal(original.leftColumn, copy.leftColumn);
    assert.equal(original.rightColumn, copy.rightColumn);
    assert.equal(original.evaluatorType, copy.evaluatorType);
    assert.equal(original.getId(), copy.getId());
  });

  it('getColumns', () => {
    const p: Predicate = e.col('jobId').eq(j.col('id'));
    assert.sameMembers([e.col('jobId'), j.col('id')], p.getColumns());

    // Test case where optional parameter is provided.
    const columns: Column[] = [];
    assert.equal(columns, p.getColumns(columns));
    assert.sameMembers([e.col('jobId'), j.col('id')], columns);
  });

  it('getTables', () => {
    const p: Predicate = e.col('jobId').eq(j.col('id'));
    assert.sameMembers([e, j], Array.from(p.getTables().values()));

    // Test case where optional parameter is provided.
    const tables = new Set<Table>();
    assert.equal(tables, p.getTables(tables));
    assert.sameMembers([e, j], Array.from(tables.values()));
  });

  it('joinPredicate_reverse', () => {
    const predicates = [
      e.col('jobId').lt(j.col('id')),
      e.col('jobId').gt(j.col('id')),
      e.col('jobId').lte(j.col('id')),
      e.col('jobId').gte(j.col('id')),
      e.col('jobId').eq(j.col('id')),
      e.col('jobId').neq(j.col('id')),
    ] as JoinPredicate[];
    const expectedEvalTypes: EvalType[] = [
      EvalType.GT,
      EvalType.LT,
      EvalType.GTE,
      EvalType.LTE,
      EvalType.EQ,
      EvalType.NEQ,
    ];
    checkJoinPredicate_ExplicitReverse(predicates, expectedEvalTypes);
    checkJoinPredicate_NestedLoop_Reverse(predicates, expectedEvalTypes);
  });

  // Tests that JoinPredicate.reverse() works correctly.
  function checkJoinPredicate_ExplicitReverse(
    predicates: JoinPredicate[],
    expectedEvalTypes: EvalType[]
  ): void {
    for (let i = 0; i < predicates.length; i++) {
      const reversePredicate = predicates[i].reverse();
      assert.equal(predicates[i].leftColumn, reversePredicate.rightColumn);
      assert.equal(predicates[i].rightColumn, reversePredicate.leftColumn);
      assert.equal(expectedEvalTypes[i], reversePredicate.evaluatorType);
    }
  }

  // Tests that Nested Loop Join reverses join order and evaluation type when
  // right table is smaller than the left.
  function checkJoinPredicate_NestedLoop_Reverse(
    predicates: JoinPredicate[],
    expectedEvalTypes: EvalType[]
  ): void {
    const sampleRows = getSampleRows();
    const sampleEmployees = sampleRows.employees;
    const sampleJobs = sampleRows.jobs;

    const employeeRelation = Relation.fromRows(sampleEmployees, [
      e.getEffectiveName(),
    ]);
    const jobRelation = Relation.fromRows(sampleJobs, [j.getEffectiveName()]);

    const expectedLeftColumn = predicates[0].leftColumn;
    const expectedRightColumn = predicates[1].rightColumn;
    for (let i = 0; i < predicates.length; i++) {
      predicates[i].evalRelationsNestedLoopJoin(
        employeeRelation,
        jobRelation,
        false
      );
      assert.equal(expectedRightColumn, predicates[i].leftColumn);
      assert.equal(expectedLeftColumn, predicates[i].rightColumn);
      assert.equal(expectedEvalTypes[i], predicates[i].evaluatorType);
    }
  }

  it('joinPredicate_Eval_True', () => {
    const leftEntry = new RelationEntry(
      HRSchemaSampleData.generateSampleEmployeeData(),
      false
    );
    const rightEntry = new RelationEntry(
      HRSchemaSampleData.generateSampleJobData(),
      false
    );
    const combinedEntry = RelationEntry.combineEntries(
      leftEntry,
      [e.getName()],
      rightEntry,
      [j.getName()]
    );
    const relation = new Relation([combinedEntry], [e.getName(), j.getName()]);

    const joinPredicate1 = e.col('jobId').eq(j.col('id'));
    const resultRelation1 = joinPredicate1.eval(relation);
    assert.equal(1, resultRelation1.entries.length);

    const joinPredicate2 = j.col('id').eq(e.col('jobId'));
    const resultRelation2 = joinPredicate2.eval(relation);
    assert.equal(1, resultRelation2.entries.length);
  });

  it('joinPredicate_Eval_False', () => {
    const leftEntry = new RelationEntry(
      HRSchemaSampleData.generateSampleEmployeeData(),
      false
    );
    const rightEntry = new RelationEntry(
      HRSchemaSampleData.generateSampleJobData(),
      false
    );
    const combinedEntry = RelationEntry.combineEntries(
      leftEntry,
      [e.getName()],
      rightEntry,
      [j.getName()]
    );
    const relation = new Relation([combinedEntry], [e.getName(), j.getName()]);

    const joinPredicate = e.col('firstName').eq(j.col('id'));
    const resultRelation = joinPredicate.eval(relation);
    assert.equal(0, resultRelation.entries.length);
  });

  // Tests that evalRelations() will detect which input relation should be used
  // as "left" and which as "right" independently of the input order.
  // |evalFn| is the join implementation method, should be either
  // evalRelationsNestedLoopJoin or evalRelationsHashJoin.
  function checkJoinPredicate_RelationsInputOrder(
    employeeSchema: BaseTable,
    jobSchema: BaseTable,
    evalFn: (r1: Relation, r2: Relation, outer: boolean) => Relation
  ): void {
    const sampleEmployee = HRSchemaSampleData.generateSampleEmployeeData();
    const sampleJob = HRSchemaSampleData.generateSampleJobData();

    const employeeRelation = Relation.fromRows(
      [sampleEmployee],
      [employeeSchema.getEffectiveName()]
    );
    const jobRelation = Relation.fromRows(
      [sampleJob],
      [jobSchema.getEffectiveName()]
    );

    const joinPredicate = employeeSchema.col('jobId').eq(jobSchema.col('id'));
    const result1 = evalFn.call(
      joinPredicate,
      employeeRelation,
      jobRelation,
      false
    );
    const result2 = evalFn.call(
      joinPredicate,
      jobRelation,
      employeeRelation,
      false
    );

    assert.equal(1, result1.entries.length);
    assert.equal(1, result2.entries.length);
    assert.equal(
      sampleEmployee.payload()['id'],
      result1.entries[0].getField(employeeSchema.col('id'))
    );
    assert.equal(
      sampleEmployee.payload()['id'],
      result2.entries[0].getField(employeeSchema.col('id'))
    );
    assert.equal(
      sampleJob.payload()['id'],
      result1.entries[0].getField(jobSchema.col('id'))
    );
    assert.equal(
      sampleJob.payload()['id'],
      result2.entries[0].getField(jobSchema.col('id'))
    );
  }

  it('joinPredicate_RelationsInputOrder', () => {
    checkJoinPredicate_RelationsInputOrder(
      e,
      j,
      JoinPredicate.prototype.evalRelationsNestedLoopJoin
    );
    checkJoinPredicate_RelationsInputOrder(
      e,
      j,
      JoinPredicate.prototype.evalRelationsHashJoin
    );
  });

  it('joinPredicate_RelationOrder_Alias', () => {
    const eAlias = e.as('employeeAlias') as BaseTable;
    const jAlias = j.as('jobAlias') as BaseTable;
    checkJoinPredicate_RelationsInputOrder(
      eAlias,
      jAlias,
      JoinPredicate.prototype.evalRelationsNestedLoopJoin
    );
    checkJoinPredicate_RelationsInputOrder(
      eAlias,
      jAlias,
      JoinPredicate.prototype.evalRelationsHashJoin
    );
  });

  it('joinPredicate_EvalRelations_HashJoin', () => {
    checkEvalRelations_UniqueKeys(
      JoinPredicate.prototype.evalRelationsHashJoin
    );
    checkEvalRelations_NonUniqueKeys(
      JoinPredicate.prototype.evalRelationsHashJoin
    );
    checkEvalRelations_NullableKeys(
      JoinPredicate.prototype.evalRelationsHashJoin
    );
  });

  it('joinPredicate_EvalRelations_NestedLoopJoin', () => {
    checkEvalRelations_UniqueKeys(
      JoinPredicate.prototype.evalRelationsNestedLoopJoin
    );
    checkEvalRelations_NonUniqueKeys(
      JoinPredicate.prototype.evalRelationsNestedLoopJoin
    );
    checkEvalRelations_NullableKeys(
      JoinPredicate.prototype.evalRelationsNestedLoopJoin
    );
  });

  it('joinPredicate_EvalRelations_OuterJoin_HashJoin', () => {
    checkEvalRelations_OuterJoin_UniqueKeys(
      JoinPredicate.prototype.evalRelationsHashJoin
    );
    checkEvalRelations_OuterJoin_NonUniqueKeys(
      JoinPredicate.prototype.evalRelationsHashJoin
    );
    checkEvalRelations_TwoOuterJoins(
      JoinPredicate.prototype.evalRelationsHashJoin
    );
    checkEvalRelations_OuterInnerJoins(
      JoinPredicate.prototype.evalRelationsHashJoin
    );
    checkEvalRelations_OuterJoin_NullableKeys(
      JoinPredicate.prototype.evalRelationsHashJoin
    );
  });

  it('joinPredicate_EvalRelations_OuterJoin_NestedLoopJoin', () => {
    checkEvalRelations_OuterJoin_UniqueKeys(
      JoinPredicate.prototype.evalRelationsNestedLoopJoin
    );
    checkEvalRelations_OuterJoin_NonUniqueKeys(
      JoinPredicate.prototype.evalRelationsNestedLoopJoin
    );
    checkEvalRelations_TwoOuterJoins(
      JoinPredicate.prototype.evalRelationsNestedLoopJoin
    );
    checkEvalRelations_OuterInnerJoins(
      JoinPredicate.prototype.evalRelationsNestedLoopJoin
    );
    checkEvalRelations_OuterJoin_NullableKeys(
      JoinPredicate.prototype.evalRelationsNestedLoopJoin
    );
  });

  it('joinPredicate_EvalRelations_NestedLoopJoin_MultiJoin', () => {
    checkEvalRelations_MultiJoin(
      JoinPredicate.prototype.evalRelationsNestedLoopJoin
    );
  });

  it('joinPredicate_EvalRelations_HashJoin_MultiJoin', () => {
    checkEvalRelations_MultiJoin(JoinPredicate.prototype.evalRelationsHashJoin);
  });

  // Checks that the given join implementation is correct, for the case where
  // the join predicate refers to unique keys.
  // |evalFn| is the join implementation method, should be either
  // evalRelationsNestedLoopJoin or evalRelationsHashJoin.
  function checkEvalRelations_UniqueKeys(
    evalFn: (r1: Relation, r2: Relation, outer: boolean) => Relation
  ): void {
    const sampleRows = getSampleRows();

    const employeeRelation = Relation.fromRows(sampleRows.employees, [
      e.getName(),
    ]);
    const jobRelation = Relation.fromRows(sampleRows.jobs, [j.getName()]);

    const joinPredicate1 = e.col('jobId').eq(j.col('id'));
    let result: Relation = evalFn.call(
      joinPredicate1,
      employeeRelation,
      jobRelation,
      false
    );
    assert.equal(sampleRows.employees.length, result.entries.length);

    // Expecting only 5 result entries, since there are only 5 employees that
    // have the same ID with a job.
    const joinPredicate2 = e.col('id').eq(j.col('id'));
    result = evalFn.call(joinPredicate2, employeeRelation, jobRelation, false);
    assert.equal(sampleRows.jobs.length, result.entries.length);
  }

  // Checks that the given join implementation is correct, for the case where
  // the join predicate refers to nullable keys.
  // |evalFn| is the join implementation method, should be either
  // evalRelationsNestedLoopJoin or evalRelationsHashJoin.
  function checkEvalRelations_NullableKeys(
    evalFn: (r1: Relation, r2: Relation, outer: boolean) => Relation
  ): void {
    const tableA = schemaWithNullable.table('TableA') as BaseTable;
    const tableB = schemaWithNullable.table('TableB');
    const tableC = schemaWithNullable.table('TableC') as BaseTable;

    const tableARelation = Relation.fromRows(
      nullableGenerator.sampleTableARows,
      [tableA.getName()]
    );
    const tableBRelation = Relation.fromRows(
      nullableGenerator.sampleTableBRows,
      [tableB.getName()]
    );
    const tableCRelation = Relation.fromRows(
      nullableGenerator.sampleTableCRows,
      [tableC.getName()]
    );

    const joinPredicate1 = tableA.col('id').eq(tableC.col('id'));
    let result: Relation = evalFn.call(
      joinPredicate1,
      tableARelation,
      tableCRelation,
      false
    );
    assert.equal(
      nullableGenerator.sampleTableARows.length -
        nullableGenerator.tableAGroundTruth.numNullable,
      result.entries.length
    );
    result.entries.forEach(entry => {
      assert.isTrue(hasNonNullEntry(entry, tableA.getEffectiveName()));
      assert.isFalse(hasNullEntry(entry, tableC.getEffectiveName()));
    });
    // Join with left table containing only nulls.
    const joinPredicate2 = tableB.col('id').eq(tableC.col('id'));
    result = evalFn.call(joinPredicate2, tableBRelation, tableCRelation, false);
    assert.equal(0, result.entries.length);
  }

  // Checks that the given outer join implementation is correct, for the case
  // where the join predicate refers to nullable keys.
  // |evalFn| is the join implementation method, should be either
  // evalRelationsNestedLoopJoin or evalRelationsHashJoin.
  function checkEvalRelations_OuterJoin_NullableKeys(
    evalFn: (r1: Relation, r2: Relation, outer: boolean) => Relation
  ): void {
    const tableA = schemaWithNullable.table('TableA') as BaseTable;
    const tableB = schemaWithNullable.table('TableB') as BaseTable;
    const tableC = schemaWithNullable.table('TableC') as BaseTable;

    const tableARelation = Relation.fromRows(
      nullableGenerator.sampleTableARows,
      [tableA.getName()]
    );
    const tableBRelation = Relation.fromRows(
      nullableGenerator.sampleTableBRows,
      [tableB.getName()]
    );
    const tableCRelation = Relation.fromRows(
      nullableGenerator.sampleTableCRows,
      [tableC.getName()]
    );

    const lengthTableA = nullableGenerator.sampleTableARows.length;
    const numNullableTableA = nullableGenerator.tableAGroundTruth.numNullable;
    const joinPredicate1 = tableA.col('id').eq(tableC.col('id'));
    let result: Relation = evalFn.call(
      joinPredicate1,
      tableARelation,
      tableCRelation,
      true
    );
    assert.equal(lengthTableA, result.entries.length);
    result.entries.slice(0, lengthTableA - numNullableTableA).forEach(entry => {
      assert.isTrue(hasNonNullEntry(entry, tableA.getEffectiveName()));
    });
    result.entries.slice(lengthTableA - numNullableTableA).forEach(entry => {
      assert.isTrue(hasNullEntry(entry, tableA.getEffectiveName()));
    });
    let numNullEntries = result.entries.filter(entry => {
      return hasNullEntry(entry, tableC.getEffectiveName());
    }).length;
    assert.equal(numNullableTableA, numNullEntries);

    // Join with left table containing only nulls.
    const joinPredicate2 = tableB.col('id').eq(tableC.col('id'));
    result = evalFn.call(joinPredicate2, tableBRelation, tableCRelation, true);
    assert.equal(
      nullableGenerator.sampleTableBRows.length,
      result.entries.length
    );
    numNullEntries = result.entries.filter(entry => {
      return (
        hasNullEntry(entry, tableC.getEffectiveName()) &&
        hasNullEntry(entry, tableB.getEffectiveName())
      );
    }).length;
    assert.equal(nullableGenerator.sampleTableBRows.length, numNullEntries);
  }

  // Checks that the given combined entry has a null entry for table
  // 'tableName'.
  function hasNullEntry(entry: RelationEntry, tableName: string): boolean {
    const keys = Object.keys(entry.row.payload()[tableName] as object);
    assert.isTrue(keys.length > 0);
    return Object.keys(entry.row.payload()[tableName] as object).every(key => {
      return (entry.row.payload()[tableName] as PayloadType)[key] === null;
    });
  }

  // Checks that the given combined entry has a non-null entry for table
  // 'tableName'.
  function hasNonNullEntry(entry: RelationEntry, tableName: string): boolean {
    const payload = entry.row.payload()[tableName] as PayloadType;
    const keys = Object.keys(payload);
    assert.isTrue(keys.length > 0);
    return Object.keys(payload).every(key => payload[key] !== null);
  }

  // Checks that the given outer join implementation is correct for the case,
  // where the join predicate refers to unique keys.
  // |evalFn| is the join implementation method, should be either
  // evalRelationsNestedLoopJoin or evalRelationsHashJoin.
  function checkEvalRelations_OuterJoin_UniqueKeys(
    evalFn: (r1: Relation, r2: Relation, outer: boolean) => Relation
  ): void {
    const sampleRows = getSampleRows();
    // Remove the last job row.
    const lessJobs = sampleRows.jobs.slice(0, sampleRows.jobs.length - 1);
    const employeeRelation = Relation.fromRows(sampleRows.employees, [
      e.getName(),
    ]);
    const jobRelation = Relation.fromRows(lessJobs, [j.getName()]);
    // For every Job , there are 10 employees according to getSampleRows().
    const numEmployeesPerJob = 10;

    const joinPredicate1 = e.col('jobId').eq(j.col('id'));
    const result: Relation = evalFn.call(
      joinPredicate1,
      employeeRelation,
      jobRelation,
      true
    );
    assert.equal(sampleRows.employees.length, result.entries.length);
    const numNullEntries = result.entries.filter(entry =>
      hasNullEntry(entry, 'Job')
    ).length;
    assert.equal(numEmployeesPerJob, numNullEntries);
  }

  // Checks that the given outer join implementation is correct for two
  // Outer joins, for the case where the join predicate refers to unique keys.
  // |evalFn| is the join implementation method, should be either
  // evalRelationsNestedLoopJoin or evalRelationsHashJoin.
  function checkEvalRelations_TwoOuterJoins(
    evalFn: (r1: Relation, r2: Relation, outer: boolean) => Relation
  ): void {
    const sampleRows = getSampleRows();
    const numJobsDeleted = 1;
    const numDepartmentsDeleted = 2;
    // Remove the last job row.
    const lessJobs = sampleRows.jobs.slice(
      0,
      sampleRows.jobs.length - numJobsDeleted
    );
    // Remove the last 2 rows in Departments.
    const lessDepartments = sampleRows.departments.slice(
      0,
      sampleRows.departments.length - numDepartmentsDeleted
    );

    const employeeRelation = Relation.fromRows(sampleRows.employees, [
      e.getName(),
    ]);
    const jobRelation = Relation.fromRows(lessJobs, [j.getName()]);
    const departmentRelation = Relation.fromRows(lessDepartments, [
      d.getName(),
    ]);

    let joinPredicate1 = e.col('jobId').eq(j.col('id'));
    let joinPredicate2 = e.col('departmentId').eq(d.col('id'));

    const numEmployeesPerJob = 10;
    const numEmployeesPerDepartment = 20;
    const expectedResults =
      sampleRows.employees.length - numJobsDeleted * numEmployeesPerJob;
    const expectedResults2 =
      sampleRows.employees.length -
      numDepartmentsDeleted * numEmployeesPerDepartment;

    // Tests inner join followed by outer join.
    let result: Relation = evalFn.call(
      joinPredicate1,
      employeeRelation,
      jobRelation,
      false
    );
    assert.equal(expectedResults, result.entries.length);
    let result2: Relation = evalFn.call(
      joinPredicate2,
      result,
      departmentRelation,
      true
    );
    // Join employee and job with department.
    assert.equal(expectedResults, result2.entries.length);
    // joinPredicate1 is reversed in previous join.
    joinPredicate1 = e.col('jobId').eq(j.col('id'));
    // Tests outer join followed by inner join.
    result = evalFn.call(joinPredicate1, employeeRelation, jobRelation, true);
    assert.equal(sampleRows.employees.length, result.entries.length);
    result2 = evalFn.call(joinPredicate2, result, departmentRelation, false);
    // Join employee and job with department
    assert.equal(expectedResults2, result2.entries.length);
    // joinPredicate2 is reversed in previous join.
    joinPredicate2 = e.col('departmentId').eq(d.col('id'));
    // Tests outer join followed by outer join.
    result = evalFn.call(joinPredicate1, employeeRelation, jobRelation, true);
    assert.equal(sampleRows.employees.length, result.entries.length);
    result2 = evalFn.call(joinPredicate2, result, departmentRelation, true);
    assert.equal(sampleRows.employees.length, result2.entries.length);
  }

  // Checks that the given outer join implementation is correct
  // for two Outer joins, for the case where the join predicate
  // refers to unique keys.
  // |evalFn| is the join implementation method, should be either
  // evalRelationsNestedLoopJoin or evalRelationsHashJoin.
  function checkEvalRelations_OuterInnerJoins(
    evalFn: (r1: Relation, r2: Relation, outer: boolean) => Relation
  ): void {
    const sampleRows = getSampleRows();
    const lessJobs = sampleRows.jobs.slice(0, sampleRows.jobs.length - 1);
    const lessDepartments = sampleRows.departments.slice(
      0,
      sampleRows.departments.length - 1
    );
    const numJobsDeleted = 1;
    const numDepartmentsDeleted = 1;
    const numEmployeesPerJob = 10;
    const numEmployeesPerDepartment = 20;

    const employeeRelation = Relation.fromRows(sampleRows.employees, [
      e.getName(),
    ]);
    const jobRelation = Relation.fromRows(lessJobs, [j.getName()]);
    const departmentRelation = Relation.fromRows(lessDepartments, [
      d.getName(),
    ]);

    let joinPredicate1 = e.col('jobId').eq(j.col('id'));
    let result: Relation = evalFn.call(
      joinPredicate1,
      employeeRelation,
      jobRelation,
      false
    );
    const expectedResults =
      sampleRows.employees.length - numJobsDeleted * numEmployeesPerJob;
    const expectedResults2 =
      sampleRows.employees.length -
      numDepartmentsDeleted * numEmployeesPerDepartment;
    assert.equal(expectedResults, result.entries.length);
    // joinPredicate1 is reversed in previous join.
    joinPredicate1 = e.col('jobId').eq(j.col('id'));
    result = evalFn.call(joinPredicate1, employeeRelation, jobRelation, true);
    assert.equal(sampleRows.employees.length, result.entries.length);

    // Join employee and job with department.
    const joinPredicate2 = e.col('departmentId').eq(d.col('id'));
    let result2: Relation = evalFn.call(
      joinPredicate2,
      result,
      departmentRelation,
      true
    );
    assert.equal(sampleRows.employees.length, result2.entries.length);
    result2 = evalFn.call(joinPredicate2, result, departmentRelation, false);
    assert.equal(expectedResults2, result2.entries.length);
  }

  // Checks that the given join implementation is correct, for the case where
  // the join predicate refers to non-unique keys.
  // |evalFn| is the join implementation method, should be either
  // evalRelationsNestedLoopJoin or evalRelationsHashJoin.
  function checkEvalRelations_NonUniqueKeys(
    evalFn: (r1: Relation, r2: Relation, outer: boolean) => Relation
  ): void {
    const sampleRows = getSampleRows();

    const employeeRelation = Relation.fromRows(sampleRows.employees, [
      e.getName(),
    ]);
    const jobRelation = Relation.fromRows(sampleRows.jobs, [j.getName()]);

    const joinPredicate1 = e.col('salary').eq(j.col('minSalary'));
    const result: Relation = evalFn.call(
      joinPredicate1,
      employeeRelation,
      jobRelation,
      false
    );
    assert.equal(
      sampleRows.employees.length * sampleRows.jobs.length,
      result.entries.length
    );
  }

  // Checks that the given join implementation is correct, for the case where
  // the join predicate refers to non-unique keys.
  // |evalFn| is the join implementation method, should be either
  // evalRelationsNestedLoopJoin or evalRelationsHashJoin.
  function checkEvalRelations_OuterJoin_NonUniqueKeys(
    evalFn: (r1: Relation, r2: Relation, outer: boolean) => Relation
  ): void {
    const sampleRows = getSampleRows();
    const lessJobs = sampleRows.jobs.slice(0, sampleRows.jobs.length - 1);
    sampleRows.employees[sampleRows.employees.length - 1].payload()[
      'salary'
    ] = 1;
    const employeeRelation = Relation.fromRows(sampleRows.employees, [
      e.getName(),
    ]);
    const jobRelation = Relation.fromRows(lessJobs, [j.getName()]);

    const numEmployeesChanged = 1;

    const joinPredicate1 = e.col('salary').eq(j.col('minSalary'));
    const result: Relation = evalFn.call(
      joinPredicate1,
      employeeRelation,
      jobRelation,
      true
    );
    assert.equal(
      (sampleRows.employees.length - numEmployeesChanged) * lessJobs.length +
        numEmployeesChanged,
      result.entries.length
    );
    let numNullEntries = 0;
    result.entries.forEach(entry => {
      if (hasNullEntry(entry, 'Job')) {
        numNullEntries++;
      }
    });
    assert.equal(numEmployeesChanged, numNullEntries);
  }

  // Checks that the given join implementation is correct, for the case where
  // a 3 table natural join is performed.
  // |evalFn| is the join implementation method, should be either
  // evalRelationsNestedLoopJoin or evalRelationsHashJoin.
  function checkEvalRelations_MultiJoin(
    evalFn: (r1: Relation, r2: Relation, outer: boolean) => Relation
  ): void {
    const sampleRows = getSampleRows();

    const employeeRelation = Relation.fromRows(sampleRows.employees, [
      e.getName(),
    ]);
    const jobRelation = Relation.fromRows(sampleRows.jobs, [j.getName()]);
    const departmentRelation = Relation.fromRows(sampleRows.departments, [
      d.getName(),
    ]);

    const joinPredicate1 = e.col('jobId').eq(j.col('id'));
    const joinPredicate2 = e.col('departmentId').eq(d.col('id'));

    const resultEmployeeJob: Relation = evalFn.call(
      joinPredicate1,
      employeeRelation,
      jobRelation,
      false
    );
    const resultEmployeeJobDepartment: Relation = evalFn.call(
      joinPredicate2,
      resultEmployeeJob,
      departmentRelation,
      false
    );
    assert.equal(
      sampleRows.employees.length,
      resultEmployeeJobDepartment.entries.length
    );
  }

  // Generates sample data to be used for tests. Specifically it generates
  //  - 60 employees,
  //  - 6 jobs,
  //  - 3 departments
  // Such that each job contains 10 employees and each department contains 20
  // employees.
  function getSampleRows(): SampleDataType {
    const employeeCount = 60;
    const jobCount = employeeCount / 10;
    const departmentCount = employeeCount / 20;
    const salary = 100000;

    const employees = new Array(employeeCount);
    for (let i = 0; i < employeeCount; i++) {
      const employee = HRSchemaSampleData.generateSampleEmployeeData();
      employee.payload()['id'] = i.toString();
      employee.payload()['jobId'] = String(i % jobCount);
      employee.payload()['departmentId'] = `departmentId${String(
        i % departmentCount
      )}`;
      employee.payload()['salary'] = salary;
      employees[i] = employee;
    }

    const jobs = new Array(jobCount);
    for (let i = 0; i < jobCount; i++) {
      const job = HRSchemaSampleData.generateSampleJobData();
      job.payload()['id'] = i.toString();
      job.payload()['minSalary'] = salary;
      jobs[i] = job;
    }

    const departments = new Array(departmentCount);
    for (let i = 0; i < departmentCount; i++) {
      const department = HRSchemaSampleData.generateSampleDepartmentData();
      department.payload()['id'] = `departmentId${String(i)}`;
      departments[i] = department;
    }

    return {
      departments,
      employees,
      jobs,
    };
  }
});
