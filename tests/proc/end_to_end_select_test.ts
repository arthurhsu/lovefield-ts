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

import { bind } from '../../lib/base/bind';
import { DataStoreType, ErrorCode, Order } from '../../lib/base/enum';
import { PayloadType, Row } from '../../lib/base/row';
import { fn } from '../../lib/fn/fn';
import { op } from '../../lib/fn/op';
import { RuntimeDatabase } from '../../lib/proc/runtime_database';
import { SelectQuery } from '../../lib/query/select_query';
import { BaseColumn } from '../../lib/schema/base_column';
import { BaseTable } from '../../lib/schema/base_table';
import { Column } from '../../lib/schema/column';
import { Table } from '../../lib/schema/table';
import { ArrayHelper } from '../../lib/structs/array_helper';
import { getHrDbSchemaBuilder } from '../../testing/hr_schema/hr_schema_builder';
import { MockDataGenerator } from '../../testing/hr_schema/mock_data_generator';
import { NestedPayloadType } from '../../testing/test_util';
import { ValueOperandType } from '../../lib/pred/operand_type';

const assert = chai.assert;

describe('EndToEndSelectTest', () => {
  let db: RuntimeDatabase;
  let j: Table;
  let e: Table;
  let d: Table;
  let c: Table;
  let r: Table;
  let l: Table;
  let cct: Table;
  let dataGenerator: MockDataGenerator;

  beforeEach(async () => {
    db = (await getHrDbSchemaBuilder().connect({
      storeType: DataStoreType.MEMORY,
    })) as RuntimeDatabase;
    dataGenerator = new MockDataGenerator();
    j = db.getSchema().table('Job');
    e = db.getSchema().table('Employee');
    d = db.getSchema().table('Department');
    c = db.getSchema().table('Country');
    r = db.getSchema().table('Region');
    l = db.getSchema().table('Location');
    cct = db.getSchema().table('CrossColumnTable');
    await addSampleData();
  });

  function isDefAndNotNull(v: unknown): boolean {
    return v !== null && v !== undefined;
  }

  function addSampleData(): Promise<unknown> {
    dataGenerator.generate(
      /* jobCount */ 50,
      /* employeeCount */ 300,
      /* departmentCount */ 10
    );

    return db.createTransaction().exec([
      db
        .insert()
        .into(r)
        .values(dataGenerator.sampleRegions),
      db
        .insert()
        .into(c)
        .values(dataGenerator.sampleCountries),
      db
        .insert()
        .into(l)
        .values(dataGenerator.sampleLocations),
      db
        .insert()
        .into(d)
        .values(dataGenerator.sampleDepartments),
      db
        .insert()
        .into(j)
        .values(dataGenerator.sampleJobs),
      db
        .insert()
        .into(e)
        .values(dataGenerator.sampleEmployees),
      db
        .insert()
        .into(cct)
        .values(getSampleCrossColumnTable()),
    ]);
  }

  // Sample rows for the CrossColumnTable, which contains a nullable
  // cross-column index.
  function getSampleCrossColumnTable(): Row[] {
    const sampleRows: Row[] = new Array(20);
    const padZeros = (n: number) => (n < 10 ? `0${n}` : `${n}`);

    for (let i = 0; i < 20; i++) {
      sampleRows[i] = cct.createRow({
        integer1: i,
        integer2: i * 10,
        // Generating a null value for i = [10, 12, 14].
        string1:
          i % 2 === 0 && i >= 10 && i < 15 ? null : `string1_${padZeros(i)}`,
        // Generating a null value for i = 16 and 18.
        string2: i % 2 === 0 && i >= 15 ? null : `string2_${i * 10}`,
      });
    }
    return sampleRows;
  }

  // Tests that a SELECT query without a specified predicate selects the entire
  // table.
  it('All', async () => {
    const queryBuilder = db.select().from(j);

    const results = (await queryBuilder.exec()) as PayloadType[];
    assert.equal(dataGenerator.sampleJobs.length, results.length);
  });

  // Tests that a SELECT query with a specified limit respects that limit.
  it('Limit', () => {
    return checkSelectLimit(Math.floor(dataGenerator.sampleJobs.length / 3));
  });

  // Tests that a SELECT query with a specified limit of zero respects that
  // limit.
  it('LimitZero', () => {
    return checkSelectLimit(0);
  });

  function checkSelectLimit(limit: number): Promise<void> {
    const queryBuilder = db
      .select()
      .from(j)
      .limit(limit);

    return queryBuilder
      .exec()
      .then(results => assert.equal(limit, (results as PayloadType[]).length));
  }

  // Tests that a SELECT query with a specified limit respects that limit.
  it('LimitIndex', () => {
    return checkSelectIndexLimit(
      Math.floor(dataGenerator.sampleJobs.length / 3)
    );
  });

  // Tests that a SELECT query with a specified limit of zero respects that
  // limit.
  it('LimitIndexZero', () => {
    return checkSelectIndexLimit(0);
  });

  function checkSelectIndexLimit(limit: number): Promise<void> {
    const queryBuilder = db
      .select()
      .from(j)
      .where(j.col('maxSalary').gt(0))
      .limit(limit);
    const plan = queryBuilder.explain();
    assert.notEqual(-1, plan.indexOf('index_range_scan'));
    assert.equal(-1, plan.indexOf('skip'));

    return queryBuilder
      .exec()
      .then(results => assert.equal(limit, (results as PayloadType[]).length));
  }

  // Tests that a SELECT query with a specified SKIP actually skips those rows.
  it('Skip', () => {
    return checkSelectSkip(Math.floor(dataGenerator.sampleJobs.length / 3));
  });

  // Tests that a SELECT query with a specified SKIP of zero skips no rows.
  it('SkipZero', () => {
    return checkSelectSkip(0);
  });

  function checkSelectSkip(skip: number): Promise<void> {
    const queryBuilder = db
      .select()
      .from(j)
      .skip(skip);

    return queryBuilder
      .exec()
      .then(results =>
        assert.equal(
          dataGenerator.sampleJobs.length - skip,
          (results as PayloadType[]).length
        )
      );
  }

  // Tests that a SELECT query with a specified SKIP actually skips those rows.
  it('SkipIndex', () => {
    return checkSelectIndexSkip(
      Math.floor(dataGenerator.sampleJobs.length / 3)
    );
  });

  // Tests that a SELECT query with a specified SKIP of zero skips no rows.
  it('SkipIndexZero', () => {
    return checkSelectIndexSkip(0);
  });

  // Tests that a SELECT query that uses a binder for SKIP works correctly.
  it('SkipBinder', async () => {
    const queryBuilder = db
      .select()
      .from(j)
      .skip(bind(0));

    let results = (await queryBuilder.bind([0]).exec()) as PayloadType[];
    assert.equal(dataGenerator.sampleJobs.length, results.length);
    results = (await queryBuilder.bind([2]).exec()) as PayloadType[];
    assert.equal(dataGenerator.sampleJobs.length - 2, results.length);
  });

  function checkSelectIndexSkip(skip: number): Promise<void> {
    const queryBuilder = db
      .select()
      .from(j)
      .where(j.col('maxSalary').gt(0))
      .skip(skip);
    const plan = queryBuilder.explain();
    assert.notEqual(-1, plan.indexOf('index_range_scan'));
    assert.equal(-1, plan.indexOf('limit'));

    return queryBuilder.exec().then(results => {
      assert.equal(
        dataGenerator.sampleJobs.length - skip,
        (results as PayloadType[]).length
      );
    });
  }

  // Tests that a SELECT query with a specified predicate selects only the rows
  // that satisfy the predicate.
  it('Predicate', async () => {
    const targetId = dataGenerator.sampleJobs[3].payload()['id'] as string;

    const queryBuilder = db
      .select()
      .from(j)
      .where(j.col('id').eq(targetId));
    const results = (await queryBuilder.exec()) as PayloadType[];
    assert.equal(1, results.length);
    assert.equal(targetId, (results[0] as PayloadType)['id']);
  });

  it('Predicate_IsNull', async () => {
    const queryBuilder = db
      .select()
      .from(cct)
      .where(cct.col('string1').isNull());

    // TODO(dpapad): Currently isNull() predicates do not leverage indices.
    // Reverse the assertion below once addressed.
    const plan = queryBuilder.explain();
    assert.equal(
      -1,
      plan.indexOf('index_range_scan(CrossColumnTable.idx_crossNull')
    );

    const results = (await queryBuilder.exec()) as PayloadType[];
    assert.sameMembers(
      [10, 12, 14],
      results.map(obj => (obj as PayloadType)['integer1'])
    );
  });

  // Tests the case where a cross-column nullable index is being used, even
  // though the predicates only bind the first indexed column, but not the 2nd
  // indexed column.
  it('CrossColumnNullable_PartialMatch', async () => {
    const targetValue = 'string1_09';
    const queryBuilder = db
      .select()
      .from(cct)
      .where(cct.col('string1').gt(targetValue));

    // Ensure that cross-column nullable index is being used.
    const plan = queryBuilder.explain();
    assert.notEqual(
      -1,
      plan.indexOf('index_range_scan(CrossColumnTable.idx_crossNull')
    );

    const results = (await queryBuilder.exec()) as PayloadType[];
    // Rows with integer1 value of 14, 16 and 18 have string1 value of null,
    // so should not appear in the results.
    assert.sameMembers(
      [11, 13, 15, 16, 17, 18, 19],
      results.map(obj => (obj as PayloadType)['integer1'])
    );
  });

  // Tests the case where a cross-column nullable index is being used and both
  // indexed columns are bound by predicates.
  it('CrossColumnNullable_FullMatch', async () => {
    const targetString1 = 'string1_08';
    const targetString2 = 'string2_80';
    const queryBuilder = db
      .select()
      .from(cct)
      .where(
        op.and(
          cct.col('string1').eq(targetString1),
          cct.col('string2').eq(targetString2)
        )
      );

    // Ensure that cross-column nullable index is being used.
    const plan = queryBuilder.explain();
    assert.notEqual(
      -1,
      plan.indexOf('index_range_scan(CrossColumnTable.idx_crossNull')
    );

    const results = (await queryBuilder.exec()) as PayloadType[];
    assert.sameMembers(
      [8],
      results.map(obj => (obj as PayloadType)['integer1'])
    );
  });

  it('As', async () => {
    const targetId = dataGenerator.sampleJobs[3].payload()['id'] as string;
    const q1 = db
      .select(j.col('id').as('Foo'))
      .from(j)
      .where(j.col('id').eq(targetId));
    const q2 = db
      .select(j.col('id'))
      .from(j)
      .where(j.col('id').eq(targetId));

    let results = (await q1.exec()) as PayloadType[];
    assert.equal(1, results.length);
    assert.equal(targetId, results[0]['Foo']);
    results = (await q2.exec()) as PayloadType[];
    assert.equal(1, results.length);
    assert.equal(targetId, results[0][j.col('id').getName()]);
  });

  // Tests that a SELECT query with column filtering only returns the columns
  // that were requested.
  it('ColumnFiltering', async () => {
    const queryBuilder = db
      .select(j.col('id'), j.col('title').as('Job Title'))
      .from(j);

    const results = (await queryBuilder.exec()) as PayloadType[];
    assert.equal(dataGenerator.sampleJobs.length, results.length);
    results.forEach(result => {
      assert.equal(2, Object.keys(result).length);
      assert.isTrue(isDefAndNotNull(result['id']));
      assert.isTrue(isDefAndNotNull(result['Job Title']));
    });
  });

  // Tests the case of a SELECT query with an implicit join.
  it('ImplicitJoin', async () => {
    const jobId =
      'jobId' + Math.floor(dataGenerator.sampleJobs.length / 2).toString();
    const queryBuilder = db
      .select()
      .from(e, j)
      .where(op.and(e.col('jobId').eq(jobId), e.col('jobId').eq(j.col('id'))));

    const results = (await queryBuilder.exec()) as PayloadType[];
    assertEmployeesForJob(e, jobId, results);
  });

  // Tests the case of a SELECT query with an implicit join and with a join
  // predicate that is in reverse order compared to the ordering of tables in
  // the from() clause.
  it('ImplicitJoin_ReverseOrder', async () => {
    const jobId =
      'jobId' + Math.floor(dataGenerator.sampleJobs.length / 2).toString();

    const queryBuilder = db
      .select()
      .from(j, e)
      .where(op.and(e.col('jobId').eq(jobId), e.col('jobId').eq(j.col('id'))));

    const results = (await queryBuilder.exec()) as PayloadType[];
    assertEmployeesForJob(e, jobId, results);
  });

  // Tests the case of a SELECT query with an implicit join and with the
  // involved tables using aliases.
  it('ImplicitJoin_Alias', async () => {
    const jobId =
      'jobId' + Math.floor(dataGenerator.sampleJobs.length / 2).toString();
    const j1 = j.as('j1');
    const e1 = e.as('e1');

    const queryBuilder = db
      .select()
      .from(j1, e1)
      .where(
        op.and(e1.col('jobId').eq(jobId), e1.col('jobId').eq(j1.col('id')))
      );

    const results = (await queryBuilder.exec()) as PayloadType[];
    assertEmployeesForJob(e1, jobId, results);
  });

  // Tests the case where a SELECT query with a self-table join is being issued.
  it('SelfJoin', async () => {
    const j1 = j.as('j1') as BaseTable;
    const j2 = j.as('j2') as BaseTable;

    const queryBuilder = db
      .select()
      .from(j1, j2)
      .where(j1.col('minSalary').eq(j2.col('maxSalary')))
      .orderBy(j1.col('id'), Order.ASC)
      .orderBy(j2.col('id'), Order.ASC);

    const results = (await queryBuilder.exec()) as NestedPayloadType[];
    const groundTruth = dataGenerator.jobGroundTruth.selfJoinSalary;
    assert.equal(groundTruth.length, results.length);
    for (let i = 0; i < results.length; i++) {
      assert.equal(
        results[i][j1.getAlias()]['id'],
        groundTruth[i][0].payload()['id']
      );
      assert.equal(
        results[i][j2.getAlias()]['id'],
        groundTruth[i][1].payload()['id']
      );
    }
  });

  // Tests the case of a SELECT query with a 3+ table join.
  it('MultiJoin_Implicit', () => {
    const queryBuilder = db
      .select()
      .from(e, j, d)
      .where(
        op.and(
          e.col('jobId').eq(j.col('id')),
          e.col('departmentId').eq(d.col('id'))
        )
      );
    return checkMultiJoin(queryBuilder);
  });

  // Tests the case of a SELECT query with a 3+ table join.
  it('MultiJoin_Explicit', () => {
    const queryBuilder = db
      .select()
      .from(e)
      .innerJoin(j, j.col('id').eq(e.col('jobId')))
      .innerJoin(d, d.col('id').eq(e.col('departmentId')));
    return checkMultiJoin(queryBuilder);
  });

  // Executes and checks the given multi-join query (implicit vs explicit).
  function checkMultiJoin(queryBuilder: SelectQuery): Promise<void> {
    return queryBuilder.exec().then(res => {
      const results = res as unknown[];
      assert.equal(dataGenerator.sampleEmployees.length, results.length);
      results.forEach(res => {
        const obj = res as NestedPayloadType;
        assert.equal(3, Object.keys(obj).length);
        assert.isTrue(isDefAndNotNull(obj[e.getName()]));
        assert.isTrue(isDefAndNotNull(obj[j.getName()]));
        assert.isTrue(isDefAndNotNull(obj[d.getName()]));

        const employeeJobId = obj[e.getName()][e.col('jobId').getName()];
        const employeeDepartmentId =
          obj[e.getName()][e.col('departmentId').getName()];
        const jobId = obj[j.getName()][j.col('id').getName()];
        const departmentId = obj[d.getName()][d.col('id').getName()];
        assert.equal(employeeJobId, jobId);
        assert.equal(employeeDepartmentId, departmentId);
      });
    });
  }

  // Tests the case of a SELECT with an AND condition that has 3 clauses.
  it('Predicate_constArgAnd', async () => {
    const sampleEmployee =
      dataGenerator.sampleEmployees[
        Math.floor(dataGenerator.sampleEmployees.length / 2)
      ];
    const queryBuilder = db
      .select()
      .from(e, j, d)
      .where(
        op.and(
          e.col('jobId').eq(j.col('id')),
          e.col('departmentId').eq(d.col('id')),
          e.col('id').eq(sampleEmployee.payload()['id'] as string)
        )
      );

    const results = (await queryBuilder.exec()) as unknown[];
    assert.equal(1, results.length);
    const obj = results[0] as NestedPayloadType;
    assert.equal(
      sampleEmployee.payload()['id'],
      obj[e.getName()][e.col('id').getName()]
    );
    assert.equal(
      sampleEmployee.payload()['jobId'],
      obj[j.getName()][j.col('id').getName()]
    );
    assert.equal(
      sampleEmployee.payload()['departmentId'],
      obj[d.getName()][d.col('id').getName()]
    );
    assert.equal(
      obj[e.getName()][e.col('jobId').getName()],
      obj[j.getName()][j.col('id').getName()]
    );
    assert.equal(
      obj[e.getName()][e.col('departmentId').getName()],
      obj[d.getName()][d.col('id').getName()]
    );
  });

  // Tests the case of a SELECT with an OR condition that has 3 clauses.
  it('Predicate_constArgOr', async () => {
    const sampleJob =
      dataGenerator.sampleJobs[Math.floor(dataGenerator.sampleJobs.length / 2)];
    const queryBuilder = db
      .select()
      .from(j)
      .where(
        op.or(
          j.col('minSalary').eq(dataGenerator.jobGroundTruth.minMinSalary),
          j.col('maxSalary').eq(dataGenerator.jobGroundTruth.maxMaxSalary),
          j.col('title').eq(sampleJob.payload()['title'] as string)
        )
      );

    const results = (await queryBuilder.exec()) as PayloadType[];
    assert.isTrue(results.length >= 1);
    results.forEach((obj, index) => {
      assert.isTrue(
        obj[j.col('minSalary').getName()] ===
          dataGenerator.jobGroundTruth.minMinSalary ||
          obj[j.col('maxSalary').getName()] ===
            dataGenerator.jobGroundTruth.maxMaxSalary ||
          obj[j.col('id').getName()] === sampleJob.payload()['id']
      );
    });
  });

  // Tests that a SELECT query with an explicit join.
  it('ExplicitJoin', async () => {
    const minSalaryLimit = 59000;
    const queryBuilder = db
      .select()
      .from(e)
      .innerJoin(j, j.col('id').eq(e.col('jobId')))
      .where(j.col('minSalary').gt(minSalaryLimit));

    const results = (await queryBuilder.exec()) as NestedPayloadType[];
    const expectedJobs = dataGenerator.sampleJobs.filter(job => {
      return (job.payload()['minSalary'] as number) > minSalaryLimit;
    });

    const truth = dataGenerator.employeeGroundTruth.employeesPerJob as Map<
      string,
      string[]
    >;
    const expectedEmployeeCount = expectedJobs.reduce((soFar, job) => {
      return (
        soFar + (truth.get(job.payload()['id'] as string) as string[]).length
      );
    }, 0);

    assert.equal(expectedEmployeeCount, results.length);
    results.forEach(result => {
      assert.isTrue(truth.has(result[e.getName()]['jobId'] as string));
      const employeesInTruth = new Set<string>(
        truth.get(result[e.getName()]['jobId'] as string)
      );
      assert.isTrue(employeesInTruth.has(result[e.getName()]['id'] as string));
    });
  });

  function assertOuterJoinResult(
    lTable: Table,
    rTable: Table,
    results: NestedPayloadType[]
  ): void {
    const leftTable = lTable as BaseTable;
    const rightTable = rTable as BaseTable;
    assert.equal(dataGenerator.sampleRegions.length + 1, results.length);
    const expectedMatched = 2;
    const matchedRows = results.slice(0, expectedMatched);
    matchedRows.forEach(resultRow => {
      Object.keys(resultRow[rightTable.getEffectiveName()]).forEach(column => {
        assert.isNotNull(resultRow[rightTable.getEffectiveName()][column]);
      });
    });
    const unMatchedRows = results.slice(expectedMatched);
    unMatchedRows.forEach(resultRow => {
      Object.keys(resultRow[rightTable.getEffectiveName()]).forEach(column => {
        assert.isNull(resultRow[rightTable.getEffectiveName()][column]);
      });
    });
    results.forEach(resultRow => {
      Object.keys(resultRow[leftTable.getEffectiveName()]).forEach(column => {
        assert.isNotNull(resultRow[leftTable.getEffectiveName()][column]);
      });
    });
  }

  function assertOuterInnerJoinResult(
    t1: Table,
    t2: Table,
    t3: Table,
    results: NestedPayloadType[]
  ): void {
    const table1 = t1 as BaseTable;
    const table2 = t2 as BaseTable;
    const table3 = t3 as BaseTable;
    assert.equal(dataGenerator.sampleLocations.length, results.length);
    // All are non-null.
    results.forEach(resultRow => {
      Object.keys(resultRow[table1.getEffectiveName()]).forEach(column => {
        assert.isNotNull(resultRow[table1.getEffectiveName()][column]);
      });
      Object.keys(resultRow[table2.getEffectiveName()]).forEach(column => {
        assert.isNotNull(resultRow[table2.getEffectiveName()][column]);
      });
      Object.keys(resultRow[table3.getEffectiveName()]).forEach(column => {
        assert.isNotNull(resultRow[table3.getEffectiveName()][column]);
      });
    });
  }

  function assertInnerOuterJoinResult(
    t1: Table,
    t2: Table,
    t3: Table,
    results: NestedPayloadType[]
  ): void {
    const table1 = t1 as BaseTable;
    const table2 = t2 as BaseTable;
    const table3 = t3 as BaseTable;
    assert.equal(dataGenerator.sampleCountries.length, results.length);
    const expectedMatched = 1;
    // The matched rows are non-null.
    results.slice(0, expectedMatched).forEach(resultRow => {
      Object.keys(resultRow[table1.getEffectiveName()]).forEach(column => {
        assert.isNotNull(resultRow[table1.getEffectiveName()][column]);
      });
      Object.keys(resultRow[table2.getEffectiveName()]).forEach(column => {
        assert.isNotNull(resultRow[table2.getEffectiveName()][column]);
      });
      Object.keys(resultRow[table3.getEffectiveName()]).forEach(column => {
        assert.isNotNull(resultRow[table3.getEffectiveName()][column]);
      });
    });
    //  The first two tables have non-null entries and third table null.
    results.slice(expectedMatched, results.length).forEach(resultRow => {
      Object.keys(resultRow[table1.getEffectiveName()]).forEach(column => {
        assert.isNotNull(resultRow[table1.getEffectiveName()][column]);
      });
      Object.keys(resultRow[table2.getEffectiveName()]).forEach(column => {
        assert.isNotNull(resultRow[table2.getEffectiveName()][column]);
      });
      Object.keys(resultRow[table3.getEffectiveName()]).forEach(column => {
        assert.isNull(resultRow[table3.getEffectiveName()][column]);
      });
    });
  }

  // Tests a SELECT query with an outer join.
  it('OuterJoin', async () => {
    const queryBuilder = db
      .select()
      .from(r)
      .leftOuterJoin(c, r.col('id').eq(c.col('regionId')))
      .orderBy(r.col('id'), Order.ASC);

    const results = (await queryBuilder.exec()) as NestedPayloadType[];
    assertOuterJoinResult(r, c, results);
  });

  // Tests a SELECT query with an outer join and a where clause. It ensures that
  // the where clause is applied on the result of the join (and not before the
  // join has been calculated).
  it('OuterJoinWithWhere', async () => {
    const countryId = 2;
    const queryBuilder = db
      .select()
      .from(r)
      .leftOuterJoin(c, r.col('id').eq(c.col('regionId')))
      .orderBy(r.col('id'), Order.ASC)
      .where(c.col('id').eq(countryId));

    const results = (await queryBuilder.exec()) as NestedPayloadType[];
    assert.equal(1, results.length);
    assert.equal(countryId, results[0][c.getName()][c.col('id').getName()]);
    assert.isNotNull(results[0][r.getName()]);
    assert.equal(
      results[0][c.getName()][c.col('regionId').getName()],
      results[0][r.getName()][r.col('id').getName()]
    );
  });

  // Tests a query with two outer joins and a composite where clause.
  it('OuterMultiJoinWithWhere', async () => {
    const queryBuilder = db
      .select()
      .from(e)
      .leftOuterJoin(j, e.col('jobId').eq(j.col('id')))
      .leftOuterJoin(d, e.col('departmentId').eq(d.col('id')))
      .where(op.and(j.col('id').isNull(), d.col('id').isNull()));

    const results = (await queryBuilder.exec()) as unknown[];
    // Since every employee corresponds to an existing jobId and
    // departmentId expecting an empty result.
    assert.equal(0, results.length);
  });

  // Tests a SELECT query with an outer join followed by inner join.
  it('OuterInnerJoin', async () => {
    const queryBuilder = db
      .select()
      .from(c)
      .leftOuterJoin(r, r.col('id').eq(c.col('regionId')))
      .innerJoin(l, c.col('id').eq(l.col('countryId')))
      .orderBy(r.col('id'), Order.ASC);

    const results = (await queryBuilder.exec()) as NestedPayloadType[];
    assertOuterInnerJoinResult(r, c, l, results);
  });

  // Tests a SELECT query with an inner join followed by outer join.
  it('InnerOuterJoin', async () => {
    const queryBuilder = db
      .select()
      .from(c)
      .innerJoin(r, c.col('regionId').eq(r.col('id')))
      .leftOuterJoin(l, c.col('id').eq(l.col('countryId')))
      .orderBy(r.col('id'), Order.ASC);

    const results = (await queryBuilder.exec()) as NestedPayloadType[];
    assertInnerOuterJoinResult(c, r, l, results);
  });

  // Tests a SELECT left outer join query with the reversed order of columns
  // in the predicate.
  it('outerJoin_reversePredicate', async () => {
    const queryBuilder = db
      .select()
      .from(r)
      .leftOuterJoin(c, c.col('regionId').eq(r.col('id')))
      .orderBy(r.col('id'), Order.ASC);

    const results = (await queryBuilder.exec()) as NestedPayloadType[];
    assertOuterJoinResult(r, c, results);
  });

  // Tests a SELECT query with an outer join on tables using alias.
  it('OuterJoin_Alias', async () => {
    const c1 = c.as('c1');
    const r1 = r.as('r1');
    const queryBuilder = db
      .select()
      .from(r1)
      .leftOuterJoin(c1, r1.col('id').eq(c1.col('regionId')))
      .orderBy(r1.col('id'), Order.ASC);

    const results = (await queryBuilder.exec()) as NestedPayloadType[];
    assertOuterJoinResult(r1, c1, results);
  });

  // Tests that a SELECT query with an explicit join that also includes a cross
  // product with a third table (a table not involved in the join predicate).
  it('ExplicitJoin_WithCrossProduct', async () => {
    const sampleEmployee =
      dataGenerator.sampleEmployees[
        Math.floor(dataGenerator.sampleEmployees.length / 2)
      ];
    const expectedDepartmentId = sampleEmployee.payload()[
      'departmentId'
    ] as string;
    const queryBuilder = db
      .select()
      .from(e, d)
      .innerJoin(j, j.col('id').eq(e.col('jobId')))
      .where(d.col('id').eq(expectedDepartmentId));

    const results = (await queryBuilder.exec()) as NestedPayloadType[];
    assert.equal(dataGenerator.sampleEmployees.length, results.length);
    results.forEach(obj => {
      assert.equal(3, Object.keys(obj).length);
      assert.isTrue(isDefAndNotNull(obj[e.getName()]));
      assert.isTrue(isDefAndNotNull(obj[j.getName()]));
      assert.isTrue(isDefAndNotNull(obj[d.getName()]));

      const departmentId = obj[d.getName()][d.col('id').getName()];
      assert.equal(expectedDepartmentId, departmentId);

      const employeeJobId = obj[e.getName()][e.col('jobId').getName()];
      const jobId = obj[j.getName()][j.col('id').getName()];
      assert.equal(employeeJobId, jobId);
    });
  });

  it('OrderBy_Ascending', async () => {
    const queryBuilder = db
      .select()
      .from(j)
      .orderBy(j.col('minSalary'), Order.ASC);

    const results = (await queryBuilder.exec()) as PayloadType[];
    assertOrder(results, j.col('minSalary'), Order.ASC);
  });

  it('OrderBy_Descending', async () => {
    const queryBuilder = db
      .select()
      .from(j)
      .orderBy(j.col('maxSalary'), Order.DESC);

    const results = (await queryBuilder.exec()) as PayloadType[];
    assertOrder(results, j.col('maxSalary'), Order.DESC);
  });

  // Tests the case where the results are ordered by more than one columns.
  it('OrderBy_Multiple', async () => {
    const queryBuilder = db
      .select()
      .from(j)
      .orderBy(j.col('maxSalary'), Order.DESC)
      .orderBy(j.col('minSalary'), Order.ASC);

    const results = (await queryBuilder.exec()) as PayloadType[];
    assert.equal(results.length, dataGenerator.sampleJobs.length);
    assertOrder(results, j.col('maxSalary'), Order.DESC);

    // Assert that within entries that have the same maxSalary, the
    // minSalary appears in ASC order.
    const maxSalaryBuckets = ArrayHelper.bucket(
      results,
      result => result['maxSalary'] as number
    ) as PayloadType;
    Object.keys(maxSalaryBuckets).forEach(key => {
      assertOrder(
        maxSalaryBuckets[key] as PayloadType[],
        j.col('minSalary'),
        Order.ASC
      );
    });
  });

  // Tests the case where the results are ordered by an aggregate column (in
  // combination with GROUP_BY).
  it('OrderBy_Aggregate', async () => {
    const aggregatedColumn = fn.min(e.col('salary'));
    const order = Order.ASC;
    const queryBuilder = db
      .select(e.col('jobId'), aggregatedColumn)
      .from(e)
      .orderBy(aggregatedColumn, order)
      .groupBy(e.col('jobId'));

    const results = (await queryBuilder.exec()) as PayloadType[];
    assertOrder(results, aggregatedColumn, order);
  });

  // Tests the case where the ordering is requested on a column that is being
  // projected as a DISTINCT aggregation.
  it('OrderBy_Distinct', async () => {
    const aggregatedColumn = fn.distinct(e.col('jobId'));
    const order = Order.DESC;
    const queryBuilder = db
      .select(aggregatedColumn)
      .from(e)
      .orderBy(e.col('jobId'), order);

    const results = (await queryBuilder.exec()) as PayloadType[];
    assertOrder(results, aggregatedColumn, order);
  });

  // Tests the case where the results are ordered by an aggregate column (in
  // combination with GROUP_BY), but that aggregate column is not present in the
  // projection list.
  it('OrderBy_NonProjectedAggregate', async () => {
    const aggregatedColumn = fn.min(e.col('salary'));
    const order = Order.ASC;
    const queryBuilder1 = db
      .select(e.col('jobId'), aggregatedColumn)
      .from(e)
      .orderBy(aggregatedColumn, order)
      .groupBy(e.col('jobId'));

    const queryBuilder2 = db
      .select(e.col('jobId'))
      .from(e)
      .orderBy(aggregatedColumn, order)
      .groupBy(e.col('jobId'));

    let expectedJobIdOrder: string[];
    // First executing the query with the aggregated column in the projected
    // list, to get the expected jobId ordering.
    let results = (await queryBuilder1.exec()) as PayloadType[];
    assertOrder(results, aggregatedColumn, order);
    expectedJobIdOrder = results.map(
      obj => obj[e.col('jobId').getName()] as string
    );
    // Then executing the same query without the aggregated column in the
    // projected list.
    results = (await queryBuilder2.exec()) as PayloadType[];
    const actualJobIdOrder = results.map(obj => obj[e.col('jobId').getName()]);
    assert.sameDeepOrderedMembers(expectedJobIdOrder, actualJobIdOrder);
  });

  it('GroupBy', async () => {
    const queryBuilder = db
      .select(e.col('jobId'), fn.avg(e.col('salary')), fn.count(e.col('id')))
      .from(e)
      .groupBy(e.col('jobId'));

    const results = (await queryBuilder.exec()) as PayloadType[];
    const expectedResultCount = Array.from(
      dataGenerator.employeeGroundTruth.employeesPerJob.keys()
    ).length;
    assert.equal(expectedResultCount, results.length);
    assertGroupByResults(results, [
      e.col('jobId').getName(),
      fn.avg(e.col('salary')).getName(),
    ]);
  });

  it('GroupByWithLimit', async () => {
    const limit = 2;
    const queryBuilder = db
      .select(e.col('jobId'), fn.avg(e.col('salary')), fn.count(e.col('id')))
      .from(e)
      .limit(limit)
      .groupBy(e.col('jobId'));

    const results = (await queryBuilder.exec()) as PayloadType[];
    assert.equal(limit, results.length);
    assertGroupByResults(results, [
      e.col('jobId').getName(),
      fn.avg(e.col('salary')).getName(),
    ]);
  });

  it('GroupByMixedColumn', async () => {
    const queryBuilder = db
      .select(e.col('jobId'), e.col('salary'), fn.count(e.col('id')))
      .from(e)
      .groupBy(e.col('jobId'));
    const results = (await queryBuilder.exec()) as PayloadType[];
    const expectedResultCount = Array.from(
      dataGenerator.employeeGroundTruth.employeesPerJob.keys()
    ).length;
    assert.equal(expectedResultCount, results.length);
    assertGroupByResults(results, [
      e.col('jobId').getName(),
      e.col('salary').getName(),
    ]);
  });

  it('GroupByComplexJoin', async () => {
    // The query author knows that there's only one country, so abuse it.
    const queryBuilder = db
      .select(
        e.col('jobId').as('jid'),
        c.col('name').as('c'),
        fn.count(e.col('id')).as('idc')
      )
      .from(e, c, d, l)
      .where(
        op.and(
          e.col('departmentId').eq(d.col('id')),
          d.col('locationId').eq(l.col('id')),
          l.col('countryId').eq(c.col('id'))
        )
      )
      .groupBy(e.col('jobId'));
    const results = (await queryBuilder.exec()) as PayloadType[];
    const expectedResultCount = Array.from(
      dataGenerator.employeeGroundTruth.employeesPerJob.keys()
    ).length;
    assert.equal(expectedResultCount, results.length);
    assertGroupByComplex(results);
  });

  // Helper function for performing assertions on the results of
  // testSelect_GroupBy and testSelect_GroupByWithLimit.
  function assertGroupByResults(
    results: PayloadType[],
    columnNames: string[]
  ): void {
    assert.equal(2, columnNames.length);
    results.forEach(obj => {
      assert.equal(3, Object.keys(obj).length);
      assert.isTrue(isDefAndNotNull(obj[columnNames[0]]));
      assert.isTrue(isDefAndNotNull(obj[columnNames[1]]));

      // Verifying that each group has the correct count of employees.
      const employeesPerJobCount = obj[fn.count(e.col('id')).getName()];
      const expectedEmployeesPerJobCount = (dataGenerator.employeeGroundTruth.employeesPerJob.get(
        obj[e.col('jobId').getName()] as string
      ) as string[]).length;
      assert.equal(expectedEmployeesPerJobCount, employeesPerJobCount);
    });
  }

  // Helper function for performing assertions an the results of
  // testSelect_GroupBy and testSelect_GroupByWithLimit.
  function assertGroupByComplex(results: PayloadType[]): void {
    results.forEach(obj => {
      assert.equal(3, Object.keys(obj).length);
      assert.isTrue(isDefAndNotNull(obj['jid']));
      assert.equal('dummyCountryName', obj['c']);

      // Verifying that each group has the correct count of employees.
      const employeesPerJobCount = obj['idc'] as number;
      const expectedEmployeesPerJobCount = (dataGenerator.employeeGroundTruth.employeesPerJob.get(
        obj['jid'] as string
      ) as string[]).length;
      assert.equal(expectedEmployeesPerJobCount, employeesPerJobCount);
    });
  }

  // Tests the case where a MIN,MAX aggregators are used without being mixed up
  // with non-aggregated columns.
  it('AggregatorsOnly', async () => {
    const aggregatedColumn1 = fn.max(j.col('maxSalary'));
    const aggregatedColumn2 = fn
      .min(j.col('maxSalary'))
      .as('minS') as BaseColumn;
    const queryBuilder = db
      .select(aggregatedColumn1, aggregatedColumn2)
      .from(j);

    const results = (await queryBuilder.exec()) as PayloadType[];
    assert.equal(1, results.length);
    assert.equal(2, Object.keys(results[0]).length);
    assert.equal(
      dataGenerator.jobGroundTruth.maxMaxSalary,
      results[0][aggregatedColumn1.getName()]
    );
    assert.equal(
      dataGenerator.jobGroundTruth.minMaxSalary,
      results[0][aggregatedColumn2.getAlias()]
    );
  });

  // Tests the case where a COUNT and DISTINCT aggregators are combined.
  it('Count_Distinct', async () => {
    const aggregatedColumn = fn
      .count(fn.distinct(j.col('maxSalary')))
      .as('NS') as BaseColumn;
    const queryBuilder = db.select(aggregatedColumn).from(j);

    const results = (await queryBuilder.exec()) as PayloadType[];
    assert.equal(1, results.length);
    assert.equal(1, Object.keys(results[0]).length);
    assert.equal(
      dataGenerator.jobGroundTruth.countDistinctMaxSalary,
      results[0][aggregatedColumn.getAlias()]
    );
  });

  // Tests the case where a COUNT aggregator is used on an empty table.
  it('Count_Empty', async () => {
    const h = db.getSchema().table('Holiday');
    const aggregatedColumn = fn.count(h.col('name'));
    const queryBuilder = db.select(aggregatedColumn).from(h);

    const results = (await queryBuilder.exec()) as PayloadType[];
    assert.equal(1, results.length);
    assert.equal(1, Object.keys(results[0]).length);
    assert.equal(0, results[0][aggregatedColumn.getName()]);
  });

  // Tests the case where a COUNT(*) aggregator is used.
  it('Count_Star', async () => {
    const aggregatedColumn = fn.count();
    const queryBuilder = db.select(aggregatedColumn).from(e);

    const results = (await queryBuilder.exec()) as PayloadType[];
    assert.equal(1, results.length);
    assert.equal(1, Object.keys(results[0]).length);
    assert.equal(
      dataGenerator.sampleEmployees.length,
      results[0][aggregatedColumn.getName()]
    );
  });

  // Tests the case where a MIN aggregator is used on an empty table.
  it('Min_EmptyTable', async () => {
    const h = db.getSchema().table('Holiday');
    const aggregatedColumn = fn.min(h.col('begin'));
    const queryBuilder = db.select(aggregatedColumn).from(h);

    const results = (await queryBuilder.exec()) as PayloadType[];
    assert.equal(1, results.length);
    assert.equal(1, Object.keys(results[0]).length);
    assert.isNull(results[0][aggregatedColumn.getName()]);
  });

  // Tests the case where a MAX aggregator is used on an empty table.
  it('Max_EmptyTable', async () => {
    const h = db.getSchema().table('Holiday');
    const aggregatedColumn = fn.max(h.col('begin'));
    const queryBuilder = db.select(aggregatedColumn).from(h);

    const results = (await queryBuilder.exec()) as PayloadType[];
    assert.equal(1, results.length);
    assert.equal(1, Object.keys(results[0]).length);
    assert.isNull(results[0][aggregatedColumn.getName()]);
  });

  // Tests the case where a SUM and DISTINCT aggregators are combined.
  it('Sum_Distinct', async () => {
    const aggregatedColumn = fn.sum(fn.distinct(j.col('maxSalary')));
    const queryBuilder = db.select(aggregatedColumn).from(j);

    const results = (await queryBuilder.exec()) as PayloadType[];
    assert.equal(1, results.length);
    assert.equal(1, Object.keys(results[0]).length);
    assert.equal(
      dataGenerator.jobGroundTruth.sumDistinctMaxSalary,
      results[0][aggregatedColumn.getName()]
    );
  });

  // Tests the case where a AVG and DISTINCT aggregators are combined.
  it('Avg_Distinct', async () => {
    const aggregatedColumn = fn.avg(fn.distinct(j.col('maxSalary')));
    const queryBuilder = db.select(aggregatedColumn).from(j);

    const results = (await queryBuilder.exec()) as PayloadType[];
    assert.equal(1, results.length);
    assert.equal(1, Object.keys(results[0]).length);
    assert.equal(
      dataGenerator.jobGroundTruth.avgDistinctMaxSalary,
      results[0][aggregatedColumn.getName()]
    );
  });

  // Tests the case where a STDDEV and DISTINCT aggregators are combined.
  it('Stddev_Distinct', async () => {
    const aggregatedColumn = fn.stddev(fn.distinct(j.col('maxSalary')));
    const queryBuilder = db.select(aggregatedColumn).from(j);

    const results = (await queryBuilder.exec()) as PayloadType[];
    assert.equal(1, results.length);
    assert.equal(1, Object.keys(results[0]).length);
    assert.equal(
      dataGenerator.jobGroundTruth.stddevDistinctMaxSalary,
      results[0][aggregatedColumn.getName()]
    );
  });

  // Tests the case where a DISTINCT aggregator is used on its own.
  it('Distinct', async () => {
    const aggregatedColumn = fn.distinct(j.col('maxSalary'));
    const queryBuilder = db.select(aggregatedColumn).from(j);

    const results = (await queryBuilder.exec()) as PayloadType[];
    const distinctSalaries = results.map(result => {
      return result[aggregatedColumn.getName()];
    });
    assert.sameMembers(
      dataGenerator.jobGroundTruth.distinctMaxSalary,
      distinctSalaries
    );
  });

  // Asserts the ordering of a given list of results.
  // |results| are the results to be examined.
  // |column| is the column on which the entries are sorted.
  // |order| is the expected ordering of the entries.
  function assertOrder(
    results: PayloadType[],
    column: Column,
    order: Order
  ): void {
    let soFar: ValueOperandType = (null as unknown) as ValueOperandType;
    results.forEach((result, index) => {
      const value = result[column.getName()] as ValueOperandType;
      if (index > 0) {
        assert.isTrue(order === Order.DESC ? value <= soFar : value >= soFar);
      }
      soFar = value;
    });
  }

  function assertEmployeesForJob(
    schema: Table,
    jobId: string,
    actualEmployees: PayloadType[]
  ): void {
    const employeeSchema = schema as BaseTable;
    const expectedEmployeeIds = dataGenerator.employeeGroundTruth.employeesPerJob.get(
      jobId
    ) as string[];
    const actualEmployeeIds = actualEmployees.map(res => {
      const result = res as NestedPayloadType;
      return result[employeeSchema.getEffectiveName()]['id'] as string;
    });
    assert.sameMembers(expectedEmployeeIds, actualEmployeeIds);
  }

  it('InnerJoinOrderBy', async () => {
    const expected = dataGenerator.sampleEmployees
      .map(row => row.payload()['lastName'])
      .sort();

    const results = (await db
      .select(d.col('name'), e.col('lastName').as('elName'), e.col('firstName'))
      .from(d, e)
      .where(e.col('departmentId').eq(d.col('id')))
      .orderBy(e.col('lastName'))
      .exec()) as PayloadType[];
    const actual = results.map(row => row['elName']);
    assert.sameDeepOrderedMembers(expected, actual);
  });

  it('ParamBinding', async () => {
    const targetId = dataGenerator.sampleJobs[3].payload()['id'];
    const queryBuilder = db
      .select()
      .from(j)
      .where(j.col('id').eq(bind(1)));

    let results = (await queryBuilder.bind(['', '']).exec()) as PayloadType[];
    assert.equal(0, results.length);
    results = (await queryBuilder.bind(['', targetId]).exec()) as PayloadType[];
    assert.equal(1, results.length);
    assert.equal(targetId, results[0]['id']);
    results = (await queryBuilder.exec()) as PayloadType[];
    assert.equal(1, results.length);
    assert.equal(targetId, results[0]['id']);
  });

  it('ForgetParamBindingRejects', async () => {
    const q = db
      .select()
      .from(j)
      .where(j.col('id').eq(bind(1)));
    let failed = true;
    try {
      await q.exec();
    } catch (ex) {
      // 501: Value is not bounded.
      assert.equal(ErrorCode.UNBOUND_VALUE, ex.code);
      failed = false;
    }
    assert.isFalse(failed);
  });

  it('InvalidParamBindingThrows', () => {
    return new Promise((resolve, reject) => {
      let q = db
        .select()
        .from(j)
        .where(j.col('id').eq(bind(1)));
      let thrown = false;
      try {
        q.bind([0]);
      } catch (e) {
        thrown = true;
        // 510: Cannot bind to given array: out of range..
        assert.equal(ErrorCode.BIND_ARRAY_OUT_OF_RANGE, e.code);
      }
      assert.isTrue(thrown);

      thrown = false;
      q = db
        .select()
        .from(j)
        .where(j.col('id').between(bind(0), bind(1)));
      try {
        q.bind([0]);
      } catch (e) {
        thrown = true;
        // 510: Cannot bind to given array: out of range.
        assert.equal(ErrorCode.BIND_ARRAY_OUT_OF_RANGE, e.code);
      }
      assert.isTrue(thrown);
      resolve();
    });
  });
});
