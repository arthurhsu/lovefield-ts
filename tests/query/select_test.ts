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
import { DataStoreType, ErrorCode } from '../../lib/base/enum';
import { EvalType } from '../../lib/base/eval';
import { AggregatedColumn } from '../../lib/fn/aggregated_column';
import { fn } from '../../lib/fn/fn';
import { op } from '../../lib/fn/op';
import { ValuePredicate } from '../../lib/pred/value_predicate';
import { RuntimeDatabase } from '../../lib/proc/runtime_database';
import { SelectBuilder } from '../../lib/query/select_builder';
import { Table } from '../../lib/schema/table';
import { getHrDbSchemaBuilder } from '../../testing/hr_schema/hr_schema_builder';
import { TestUtil } from '../../testing/test_util';

const assert = chai.assert;

describe('Select', () => {
  let db: RuntimeDatabase;

  beforeEach(async () => {
    db = (await getHrDbSchemaBuilder().connect({
      storeType: DataStoreType.MEMORY,
    })) as RuntimeDatabase;
  });

  afterEach(() => db.close());

  // Tests that Select#exec() fails if from() has not been called first.
  it('exec_ThrowsMissingFrom', () => {
    const query = new SelectBuilder(db.getGlobal(), []);
    // 522: Invalid usage of select().
    return TestUtil.assertPromiseReject(ErrorCode.INVALID_SELECT, query.exec());
  });

  /**
   * Tests that constructing a query fails if an invalid projection list is
   * requested.
   * @return {!IThenable}
   */
  it('exec_ThrowsInvalidProjectionList', () => {
    const e = db.getSchema().table('Employee');
    const query = new SelectBuilder(db.getGlobal(), [
      e.col('email'),
      fn.avg(e.col('salary')),
    ]);

    // 526: Invalid projection list: mixing aggregated with non-aggregated.
    return TestUtil.assertPromiseReject(
      ErrorCode.INVALID_PROJECTION,
      query.from(e).exec()
    );
  });

  /**
   * Tests that groupBy on non-indexable fields will fail.
   * @return {!IThenable}
   */
  it('Exec_ThrowsGroupByNonIndexableColumn', () => {
    const e = db.getSchema().table('Employee');
    const query = new SelectBuilder(db.getGlobal(), [
      e.col('email'),
      e.col('salary'),
      e.col('photo'),
    ]);
    return TestUtil.assertPromiseReject(
      // 525: Invalid projection list or groupBy columns.
      ErrorCode.INVALID_GROUPBY,
      query
        .from(e)
        .groupBy(e.col('photo'))
        .exec()
    );
  });

  /**
   * Tests that constructing a query succeeds if a valid projection list is
   * requested (and if no other violation occurs).
   * @return {!IThenable}
   */
  it('Exec_ValidProjectionList', () => {
    const e = db.getSchema().table('Employee');

    // Constructing a query where all requested columns are aggregated.
    const query1 = new SelectBuilder(db.getGlobal(), [
      fn.min(e.col('salary')),
      fn.avg(e.col('salary')),
    ]);
    query1.from(e);

    // Constructing a query where all requested columns are non-aggregated.
    const query2 = new SelectBuilder(db.getGlobal(), [
      e.col('salary'),
      e.col('salary'),
    ]);
    query2.from(e);

    return Promise.all([query1.exec(), query2.exec()]);
  });

  /**
   * Tests that constructing queries involving Select#groupBy() succeed if a
   * valid combination of projection and groupBy list is requested. This test
   * checks that columns in groupBy() does not necessarily exist in projection
   * list.
   * @return {!IThenable}
   */
  it('Exec_ValidProjectionList_GroupBy', () => {
    const e = db.getSchema().table('Employee');
    const query = new SelectBuilder(db.getGlobal(), [
      e.col('jobId'),
      fn.avg(e.col('salary')),
    ]);
    return query
      .from(e)
      .groupBy(e.col('jobId'), e.col('departmentId'))
      .exec();
  });

  /**
   * Tests that unbound parameterized search condition will throw.
   * @return {!IThenable}
   */
  it('Exec_UnboundPredicateThrows', () => {
    const emp = db.getSchema().table('Employee');
    const query = new SelectBuilder(db.getGlobal(), [emp.col('jobId')]);
    return TestUtil.assertPromiseReject(
      ErrorCode.UNBOUND_VALUE, // 501: Value is not bounded.
      query
        .from(emp)
        .where(emp.col('jobId').eq(bind(0)))
        .exec()
    );
  });

  /**
   * Tests that Select#from() fails if from() has already been called.
   */
  it('From_ThrowsAlreadyCalled', () => {
    const query = new SelectBuilder(db.getGlobal(), []);

    const buildQuery = () => {
      const jobTable = db.getSchema().table('Job');
      const employeeTable = db.getSchema().table('Employee');
      query.from(jobTable).from(employeeTable);
    };

    // 515: from() has already been called.
    TestUtil.assertThrowsError(ErrorCode.DUPLICATE_FROM, buildQuery);
  });

  /**
   * Tests that Select#leftOuterJoin() fails if the predicate is not
   * join predicate.
   */
  it('OuterJoin_ThrowsOnlyJoinPredicateAllowed', () => {
    const query = new SelectBuilder(db.getGlobal(), []);

    const buildQuery = () => {
      const j = db.getSchema().table('Job');
      const e = db.getSchema().table('Employee');
      query
        .from(e)
        .leftOuterJoin(
          j,
          op.and(j.col('id').eq(e.col('jobId')), j.col('id').eq('jobId1'))
        );
    };

    // 541: Outer join accepts only join predicate.
    TestUtil.assertThrowsError(ErrorCode.INVALID_OUTER_JOIN, buildQuery);
  });

  it('Exec_ThrowsWhereNotAllowedBeforeInnerJoin', () => {
    const query = new SelectBuilder(db.getGlobal(), []);

    const buildQuery = () => {
      const e = db.getSchema().table('Employee');
      const j = db.getSchema().table('Job');
      query
        .from(e)
        .where(j.col('id').eq('1'))
        .innerJoin(j, j.col('id').eq(e.col('jobId')));
    };
    // 547: where() cannot be called before innerJoin() or leftOuterJoin().
    TestUtil.assertThrowsError(ErrorCode.INVALID_WHERE, buildQuery);
  });

  it('Throws_WhereNotAllowedBeforeOuterJoin', () => {
    const query = new SelectBuilder(db.getGlobal(), []);

    const buildQuery = () => {
      const e = db.getSchema().table('Employee');
      const j = db.getSchema().table('Job');
      query
        .from(e)
        .where(j.col('id').eq('1'))
        .leftOuterJoin(j, j.col('id').eq(e.col('jobId')));
    };
    // 547: where() cannot be called before innerJoin() or leftOuterJoin().
    TestUtil.assertThrowsError(ErrorCode.INVALID_WHERE, buildQuery);
  });

  /**
   * Tests that Select#leftOuterJoin() fails if from() is not called before
   * it is called.
   */
  it('OuterJoin_ThrowsFromNotCalled', () => {
    const query = new SelectBuilder(db.getGlobal(), []);

    const buildQuery = () => {
      const j = db.getSchema().table('Job');
      const e = db.getSchema().table('Employee');
      query.leftOuterJoin(j, e.col('jobId').eq(j.col('id'))).from(e);
    };

    // 542: from() has to be called before innerJoin() or leftOuterJoin().
    TestUtil.assertThrowsError(ErrorCode.MISSING_FROM_BEFORE_JOIN, buildQuery);
  });

  /**
   * Tests that Select#innerJoin() fails if from() is not called before
   * it is called.
   */
  it('InnerJoin_ThrowsFromNotCalled', () => {
    const query = new SelectBuilder(db.getGlobal(), []);

    const buildQuery = () => {
      const j = db.getSchema().table('Job');
      const e = db.getSchema().table('Employee');
      query.innerJoin(j, e.col('jobId').eq(j.col('id'))).from(e);
    };

    // 542: from() has to be called before innerJoin() or leftOuterJoin().
    TestUtil.assertThrowsError(ErrorCode.MISSING_FROM_BEFORE_JOIN, buildQuery);
  });

  /**
   * Tests that Select#where() fails if where() has already been called.
   */
  it('Where_ThrowsAlreadyCalled', () => {
    const query = new SelectBuilder(db.getGlobal(), []);

    const buildQuery = () => {
      const employeeTable = db.getSchema().table('Employee');
      const predicate = employeeTable.col('id').eq('testId');
      query
        .from(employeeTable)
        .where(predicate)
        .where(predicate);
    };

    // 516: where() has already been called.
    TestUtil.assertThrowsError(ErrorCode.DUPLICATE_WHERE, buildQuery);
  });

  /**
   * Tests that Select#groupBy() fails if groupBy() has already been called.
   */
  it('GroupBy_ThrowsAlreadyCalled', () => {
    const query = new SelectBuilder(db.getGlobal(), []);

    const buildQuery = () => {
      const employeeTable = db.getSchema().table('Employee');
      query
        .from(employeeTable)
        .groupBy(employeeTable.col('id'))
        .groupBy(employeeTable.col('jobId'));
    };

    // 530: groupBy() has already been called.
    TestUtil.assertThrowsError(ErrorCode.DUPLICATE_GROUPBY, buildQuery);
  });

  /**
   * Tests that Select#limit() fails if limit() has already been called.
   */
  it('Limit_ThrowsAlreadyCalled', () => {
    const query = new SelectBuilder(db.getGlobal(), []);
    const query2 = new SelectBuilder(db.getGlobal(), []);
    const emp = db.getSchema().table('Employee');

    const buildQuery = () => {
      query
        .from(emp)
        .limit(100)
        .limit(100);
    };

    const buildQuery2 = () => {
      query2
        .from(emp)
        .limit(bind(0))
        .limit(bind(1));
    };

    // 528: limit() has already been called.
    TestUtil.assertThrowsError(ErrorCode.DUPLICATE_LIMIT, buildQuery);
    TestUtil.assertThrowsError(ErrorCode.DUPLICATE_LIMIT, buildQuery2);
  });

  /**
   * Tests that Select#limit() fails if a negative value is passed.
   */
  it('Limit_ThrowsInvalidParameter', () => {
    const query = new SelectBuilder(db.getGlobal(), []);
    const employeeTable = db.getSchema().table('Employee');

    const buildQuery = () => {
      query.from(employeeTable).limit(-100);
    };

    // 531: Number of rows must not be negative for limit/skip.
    TestUtil.assertThrowsError(ErrorCode.NEGATIVE_LIMIT_SKIP, buildQuery);
  });

  /**
   * Tests that Select#skip() fails if skip() has already been called.
   */
  it('Skip_ThrowsAlreadyCalled', () => {
    const query = new SelectBuilder(db.getGlobal(), []);
    const query2 = new SelectBuilder(db.getGlobal(), []);
    const emp = db.getSchema().table('Employee');

    const buildQuery = () => {
      query
        .from(emp)
        .skip(100)
        .skip(100);
    };

    const buildQuery2 = () => {
      query2
        .from(emp)
        .skip(bind(0))
        .skip(bind(1));
    };

    // 529: skip() has already been called.
    TestUtil.assertThrowsError(ErrorCode.DUPLICATE_SKIP, buildQuery);
    TestUtil.assertThrowsError(ErrorCode.DUPLICATE_SKIP, buildQuery2);
  });

  /**
   * Tests that Select#skip() fails if a negative value is passed.
   */
  it('Skip_ThrowsInvalidParameter', () => {
    const query = new SelectBuilder(db.getGlobal(), []);
    const employeeTable = db.getSchema().table('Employee');

    const buildQuery = () => {
      query.from(employeeTable).skip(-100);
    };

    // 531: Number of rows must not be negative for limit/skip.
    TestUtil.assertThrowsError(ErrorCode.NEGATIVE_LIMIT_SKIP, buildQuery);
  });

  it('Project_ThrowsInvalidColumns', () => {
    const job = db.getSchema().table('Job');

    const buildQuery1 = () => {
      const query = new SelectBuilder(db.getGlobal(), [
        fn.distinct(job.col('maxSalary')),
        fn.avg(job.col('maxSalary')),
      ]);
      query.from(job);
    };
    // 524: Invalid usage of fn.distinct().
    TestUtil.assertThrowsError(ErrorCode.INVALID_DISTINCT, buildQuery1);

    const buildQuery2 = () => {
      const query = new SelectBuilder(db.getGlobal(), [
        job.col('title'),
        fn.distinct(job.col('maxSalary')),
      ]);
      query.from(job);
    };
    // 524: Invalid usage of fn.distinct().
    TestUtil.assertThrowsError(ErrorCode.INVALID_DISTINCT, buildQuery2);
  });

  it('Project_Aggregator_Avg', () => {
    const table = db.getSchema().table('DummyTable');

    const invalidAggregators = [
      fn.avg(table.col('arraybuffer')),
      fn.avg(table.col('datetime')),
      fn.avg(table.col('string')),
      fn.avg(table.col('boolean')),
    ] as AggregatedColumn[];
    const validAggregators = [
      fn.avg(table.col('number')),
      fn.avg(table.col('integer')),
    ] as AggregatedColumn[];

    checkAggregators(invalidAggregators, validAggregators, table);
  });

  it('Project_Aggregator_Count', () => {
    const table = db.getSchema().table('DummyTable');

    const invalidAggregators = [] as AggregatedColumn[];
    const validAggregators = [
      fn.count(table.col('arraybuffer')),
      fn.count(table.col('datetime')),
      fn.count(table.col('string')),
      fn.count(table.col('boolean')),
      fn.count(table.col('number')),
      fn.count(table.col('integer')),
    ] as AggregatedColumn[];

    checkAggregators(invalidAggregators, validAggregators, table);
  });

  it('Project_Aggregator_Distinct', () => {
    const table = db.getSchema().table('DummyTable');

    const invalidAggregators = [] as AggregatedColumn[];
    const validAggregators = [
      fn.distinct(table.col('arraybuffer')),
      fn.distinct(table.col('datetime')),
      fn.distinct(table.col('string')),
      fn.distinct(table.col('boolean')),
      fn.distinct(table.col('number')),
      fn.distinct(table.col('integer')),
    ] as AggregatedColumn[];

    checkAggregators(invalidAggregators, validAggregators, table);
  });

  it('Project_Aggregator_Max', () => {
    const table = db.getSchema().table('DummyTable');

    const invalidAggregators = [
      fn.max(table.col('arraybuffer')),
      fn.max(table.col('boolean')),
    ] as AggregatedColumn[];
    const validAggregators = [
      fn.max(table.col('datetime')),
      fn.max(table.col('integer')),
      fn.max(table.col('number')),
      fn.max(table.col('string')),
    ] as AggregatedColumn[];

    checkAggregators(invalidAggregators, validAggregators, table);
  });

  it('Project_Aggregator_Min', () => {
    const table = db.getSchema().table('DummyTable');

    const invalidAggregators = [
      fn.min(table.col('arraybuffer')),
      fn.min(table.col('boolean')),
    ] as AggregatedColumn[];
    const validAggregators = [
      fn.min(table.col('datetime')),
      fn.min(table.col('integer')),
      fn.min(table.col('number')),
      fn.min(table.col('string')),
    ] as AggregatedColumn[];

    checkAggregators(invalidAggregators, validAggregators, table);
  });

  it('Project_Aggregator_Stddev', () => {
    const table = db.getSchema().table('DummyTable');

    const invalidAggregators = [
      fn.stddev(table.col('arraybuffer')),
      fn.stddev(table.col('datetime')),
      fn.stddev(table.col('string')),
      fn.stddev(table.col('boolean')),
    ] as AggregatedColumn[];
    const validAggregators = [
      fn.stddev(table.col('number')),
      fn.stddev(table.col('integer')),
    ] as AggregatedColumn[];

    checkAggregators(invalidAggregators, validAggregators, table);
  });

  it('Project_Aggregator_Sum', () => {
    const table = db.getSchema().table('DummyTable');

    const invalidAggregators = [
      fn.sum(table.col('arraybuffer')),
      fn.sum(table.col('datetime')),
      fn.sum(table.col('string')),
      fn.sum(table.col('boolean')),
    ] as AggregatedColumn[];
    const validAggregators = [
      fn.sum(table.col('number')),
      fn.sum(table.col('integer')),
    ] as AggregatedColumn[];

    checkAggregators(invalidAggregators, validAggregators, table);
  });

  function checkAggregators(
    invalidAggregators: AggregatedColumn[],
    validAggregators: AggregatedColumn[],
    table: Table
  ): void {
    invalidAggregators.forEach(aggregator => {
      const buildQuery = () =>
        new SelectBuilder(db.getGlobal(), [aggregator]).from(table);

      // 527: Invalid aggregation detected: {0}.
      TestUtil.assertThrowsError(ErrorCode.INVALID_AGGREGATION, buildQuery);
    });

    validAggregators.forEach(aggregator => {
      const buildQuery = () =>
        new SelectBuilder(db.getGlobal(), [aggregator]).from(table);
      assert.doesNotThrow(buildQuery);
    });
  }

  it('Explain', () => {
    const query = db
      .select()
      .from(db.getSchema().table('Employee'))
      .skip(1);
    const expected = [
      'skip(1)',
      '-project()',
      '--table_access(Employee)',
      '',
    ].join('\n');
    assert.equal(expected, query.explain());
  });

  it('SkipLimitBinding', () => {
    const query = db
      .select()
      .from(db.getSchema().table('Employee'))
      .limit(bind(0))
      .skip(bind(1));

    query.bind([22, 33]);
    const expected = [
      'limit(22)',
      '-skip(33)',
      '--project()',
      '---table_access(Employee)',
      '',
    ].join('\n');

    assert.equal(expected, query.explain());

    query.bind([44, 55]);
    const expected2 = [
      'limit(44)',
      '-skip(55)',
      '--project()',
      '---table_access(Employee)',
      '',
    ].join('\n');

    assert.equal(expected2, query.explain());
  });

  it('InvalidBindingRejects', () => {
    const query = db
      .select()
      .from(db.getSchema().table('Employee'))
      .limit(bind(0))
      .skip(bind(1));

    // 523: Binding parameters of limit/skip without providing values.
    return TestUtil.assertPromiseReject(
      ErrorCode.UNBOUND_LIMIT_SKIP,
      query.exec()
    );
  });

  it('Context_Clone', () => {
    const j = db.getSchema().table('Job');
    const e = db.getSchema().table('Employee');
    const pred1 = e.col('jobId').eq(j.col('id'));
    const query = db
      .select(j.col('title'))
      .from(e)
      .leftOuterJoin(j, pred1)
      .where(
        op.or(j.col('minSalary').lt(bind(0)), j.col('maxSalary').gt(bind(1)))
      )
      .orderBy(j.col('title'))
      .groupBy(j.col('minSalary'))
      .limit(10)
      .skip(2) as SelectBuilder;
    const context = query.getQuery();
    const context2 = context.clone();
    assert.deepEqual(context.from, context2.from);
    assert.deepEqual(context.where, context2.where);
    assert.isTrue(context2.clonedFrom === context);
    assert.deepEqual(context.orderBy, context2.orderBy);
    assert.deepEqual(context.groupBy, context2.groupBy);
    assert.sameDeepOrderedMembers(context.columns, context2.columns);
    assert.equal(context.outerJoinPredicates, context2.outerJoinPredicates);
    assert.equal(context.limit, context2.limit);
    assert.equal(context.skip, context2.skip);
    assert.notEqual(context.getUniqueId(), context2.getUniqueId());
  });

  it('Builder_Clone', () => {
    const emp = db.getSchema().table('Employee');
    const builder = db
      .select()
      .from(emp)
      .limit(bind(0))
      .skip(bind(1))
      .where(emp.col('salary').gt(bind(2))) as SelectBuilder;
    const builder2 = builder.clone();

    assert.isTrue(builder2 !== builder);
    builder.bind([22, 33, 20000]);
    const expected = [
      'project()',
      '-table_access_by_row_id(Employee)',
      '--index_range_scan(Employee.idx_salary, (20000, unbound], natural, ' +
        'limit:22, skip:33)',
      '',
    ].join('\n');

    assert.equal(expected, builder.explain());
    builder2.bind([44, 55, 40000]);
    const expected2 = [
      'project()',
      '-table_access_by_row_id(Employee)',
      '--index_range_scan(Employee.idx_salary, (40000, unbound], natural, ' +
        'limit:44, skip:55)',
      '',
    ].join('\n');

    assert.equal(expected2, builder2.explain());
  });

  it('Builder_ReverseJoinPredicate', () => {
    const j = db.getSchema().table('Job');
    const e = db.getSchema().table('Employee');
    const pred1 = e.col('jobId').lt(j.col('id'));
    const pred2 = j.col('id').gt(e.col('jobId'));
    const builder = db
      .select(j.col('title'))
      .from(e)
      .leftOuterJoin(j, pred1);
    const builder2 = db
      .select(j.col('title'))
      .from(e)
      .leftOuterJoin(j, pred2) as SelectBuilder;
    const expected = [
      'project(Job.title)',
      '-join(type: outer, impl: nested_loop, ' +
        'join_pred(Employee.jobId lt Job.id))',
      '--table_access(Employee)',
      '--table_access(Job)',
      '',
    ].join('\n');

    assert.equal(expected, builder.explain());
    assert.equal(expected, builder2.explain());
    assert.equal(EvalType.LT, (pred1 as ValuePredicate).evaluatorType);
    assert.equal(
      EvalType.LT,
      (builder2.getQuery().where as ValuePredicate).evaluatorType
    );
  });

  it('Where_ThrowsFromNotCalled', () => {
    const query = new SelectBuilder(db.getGlobal(), []);

    const buildQuery = () => {
      const j = db.getSchema().table('Job');
      query.where(j.col('id').eq('1')).from(j);
    };
    // 548: from() has to be called before where().
    TestUtil.assertThrowsError(ErrorCode.FROM_AFTER_WHERE, buildQuery);
  });

  it('OrderBy_ThrowsFromNotCalled', () => {
    const query = new SelectBuilder(db.getGlobal(), []);

    const buildQuery = () => {
      const j = db.getSchema().table('Job');
      query.orderBy(j.col('id')).from(j);
    };

    // 549: from() has to be called before orderBy() or groupBy().
    TestUtil.assertThrowsError(ErrorCode.FROM_AFTER_ORDER_GROUPBY, buildQuery);
  });

  it('GroupBy_ThrowsFromNotCalled', () => {
    const query = new SelectBuilder(db.getGlobal(), []);

    const buildQuery = () => {
      const j = db.getSchema().table('Job');
      query.groupBy(j.col('id')).from(j);
    };

    // 549: from() has to be called before orderBy() or groupBy().
    TestUtil.assertThrowsError(ErrorCode.FROM_AFTER_ORDER_GROUPBY, buildQuery);
  });
});
