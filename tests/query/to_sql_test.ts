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
import { DataStoreType, Order } from '../../lib/base/enum';
import { fn } from '../../lib/fn/fn';
import { op } from '../../lib/fn/op';
import { RuntimeDatabase } from '../../lib/proc/runtime_database';
import { DeleteBuilder } from '../../lib/query/delete_builder';
import { InsertBuilder } from '../../lib/query/insert_builder';
import { Table } from '../../lib/schema/table';
import { getHrDbSchemaBuilder } from '../../testing/hr_schema/hr_schema_builder';
import { HRSchemaSampleData } from '../../testing/hr_schema/hr_schema_sample_data';

const assert = chai.assert;

describe('toSql', () => {
  let db: RuntimeDatabase;
  let j: Table;

  beforeEach(async () => {
    db = (await getHrDbSchemaBuilder().connect({
      storeType: DataStoreType.MEMORY,
    })) as RuntimeDatabase;
    j = db.getSchema().table('Job');
  });

  afterEach(() => db.close());

  it('insertToSql', () => {
    const query = new InsertBuilder(db.getGlobal());
    const job = HRSchemaSampleData.generateSampleJobData();
    query.into(j);
    query.values([job]);
    assert.equal(
      'INSERT INTO Job(id, title, minSalary, maxSalary) VALUES (' +
        "'jobId', 'Software Engineer', 100000, 500000);",
      query.toSql()
    );

    const query2 = new InsertBuilder(db.getGlobal(), true);
    query2.into(db.getSchema().table('Job'));
    query2.values([job]);
    assert.equal(
      'INSERT OR REPLACE INTO Job(id, title, minSalary, maxSalary) VALUES (' +
        "'jobId', 'Software Engineer', 100000, 500000);",
      query2.toSql()
    );
  });

  it('deleteToSql_DeleteAll', () => {
    const query = new DeleteBuilder(db.getGlobal());
    query.from(j);
    assert.equal('DELETE FROM Job;', query.toSql());
  });

  it('deleteToSql_Where', () => {
    let query = db
      .delete()
      .from(j)
      .where(j.col('id').eq('1'));
    assert.equal("DELETE FROM Job WHERE Job.id = '1';", query.toSql());

    query = db
      .delete()
      .from(j)
      .where(j.col('id').eq(bind(0)));
    assert.equal('DELETE FROM Job WHERE Job.id = ?0;', query.toSql());

    query = db
      .delete()
      .from(j)
      .where(j.col('minSalary').lt(10000));
    assert.equal('DELETE FROM Job WHERE Job.minSalary < 10000;', query.toSql());

    query = db
      .delete()
      .from(j)
      .where(j.col('minSalary').lte(10000));
    assert.equal(
      'DELETE FROM Job WHERE Job.minSalary <= 10000;',
      query.toSql()
    );

    query = db
      .delete()
      .from(j)
      .where(j.col('minSalary').gt(10000));
    assert.equal('DELETE FROM Job WHERE Job.minSalary > 10000;', query.toSql());

    query = db
      .delete()
      .from(j)
      .where(j.col('minSalary').gte(10000));
    assert.equal(
      'DELETE FROM Job WHERE Job.minSalary >= 10000;',
      query.toSql()
    );

    query = db
      .delete()
      .from(j)
      .where(j.col('minSalary').in([10000, 20000]));
    assert.equal(
      'DELETE FROM Job WHERE Job.minSalary IN (10000, 20000);',
      query.toSql()
    );

    query = db
      .delete()
      .from(j)
      .where(j.col('minSalary').between(10000, 20000));
    assert.equal(
      'DELETE FROM Job WHERE Job.minSalary BETWEEN 10000 AND 20000;',
      query.toSql()
    );

    // The LIKE conversion is incompatible with SQL, which is known.
    query = db
      .delete()
      .from(j)
      .where(j.col('id').match(/ab+c/));
    assert.equal("DELETE FROM Job WHERE Job.id LIKE '/ab+c/';", query.toSql());

    query = db
      .delete()
      .from(j)
      .where(
        op.and(
          j.col('id').eq('1'),
          j.col('minSalary').gt(10000),
          j.col('maxSalary').lt(30000)
        )
      );
    assert.equal(
      "DELETE FROM Job WHERE (Job.id = '1') AND (Job.minSalary > 10000) " +
        'AND (Job.maxSalary < 30000);',
      query.toSql()
    );

    query = db
      .delete()
      .from(j)
      .where(
        op.or(
          j.col('id').eq('1'),
          op.and(j.col('minSalary').gt(10000), j.col('maxSalary').lt(30000))
        )
      );
    assert.equal(
      "DELETE FROM Job WHERE (Job.id = '1') OR ((Job.minSalary > 10000) " +
        'AND (Job.maxSalary < 30000));',
      query.toSql()
    );
  });

  it('updateToSql', () => {
    let query = db.update(j).set(j.col('minSalary'), 10000);
    assert.equal('UPDATE Job SET Job.minSalary = 10000;', query.toSql());

    query = db
      .update(j)
      .set(j.col('minSalary'), 10000)
      .where(j.col('id').eq('1'));
    assert.equal(
      "UPDATE Job SET Job.minSalary = 10000 WHERE Job.id = '1';",
      query.toSql()
    );

    query = db
      .update(j)
      .set(j.col('minSalary'), bind(1))
      .set(j.col('maxSalary'), bind(2))
      .where(j.col('id').eq(bind(0)));
    assert.equal(
      'UPDATE Job SET Job.minSalary = ?1, Job.maxSalary = ?2 ' +
        'WHERE Job.id = ?0;',
      query.toSql()
    );
  });

  it('selectToSql_Simple', () => {
    let query = db.select().from(j);
    assert.equal('SELECT * FROM Job;', query.toSql());

    query = db
      .select(j.col('title').as('T'), j.col('minSalary'), j.col('maxSalary'))
      .from(j)
      .orderBy(j.col('id'))
      .orderBy(j.col('maxSalary'), Order.DESC)
      .groupBy(j.col('minSalary'), j.col('maxSalary'))
      .limit(20)
      .skip(50);
    assert.equal(
      'SELECT Job.title AS T, Job.minSalary, Job.maxSalary' +
        ' FROM Job' +
        ' ORDER BY Job.id ASC, Job.maxSalary DESC' +
        ' GROUP BY Job.minSalary, Job.maxSalary' +
        ' LIMIT 20' +
        ' SKIP 50;',
      query.toSql()
    );

    query = db
      .select(j.col('title'))
      .from(j)
      .where(j.col('minSalary').gt(10000));
    assert.equal(
      'SELECT Job.title FROM Job WHERE Job.minSalary > 10000;',
      query.toSql()
    );

    query = db.select(fn.avg(j.col('minSalary'))).from(j);
    assert.equal('SELECT AVG(Job.minSalary) FROM Job;', query.toSql());

    query = db.select(fn.count(fn.distinct(j.col('minSalary')))).from(j);
    assert.equal(
      'SELECT COUNT(DISTINCT(Job.minSalary)) FROM Job;',
      query.toSql()
    );

    query = db.select(fn.max(j.col('minSalary'))).from(j);
    assert.equal('SELECT MAX(Job.minSalary) FROM Job;', query.toSql());

    query = db.select(fn.min(j.col('minSalary'))).from(j);
    assert.equal('SELECT MIN(Job.minSalary) FROM Job;', query.toSql());

    query = db.select(fn.stddev(j.col('minSalary'))).from(j);
    assert.equal('SELECT STDDEV(Job.minSalary) FROM Job;', query.toSql());

    query = db.select(fn.sum(j.col('minSalary'))).from(j);
    assert.equal('SELECT SUM(Job.minSalary) FROM Job;', query.toSql());
  });

  it('selectToSql_Join', () => {
    const e = db.getSchema().table('Employee');
    const d = db.getSchema().table('Department');

    let query = db
      .select(e.col('firstName'), e.col('lastName'), j.col('title'))
      .from(e, j)
      .where(e.col('jobId').eq(j.col('id')))
      .orderBy(e.col('id'))
      .limit(20)
      .skip(10);
    assert.equal(
      'SELECT Employee.firstName, Employee.lastName, Job.title' +
        ' FROM Employee, Job' +
        ' WHERE Employee.jobId = Job.id' +
        ' ORDER BY Employee.id ASC' +
        ' LIMIT 20' +
        ' SKIP 10;',
      query.toSql()
    );

    const j1 = j.as('j1');
    const j2 = j.as('j2');

    query = db
      .select()
      .from(j1, j2)
      .where(j1.col('minSalary').eq(j2.col('maxSalary')))
      .orderBy(j1.col('id'), Order.DESC)
      .orderBy(j2.col('id'), Order.DESC);
    assert.equal(
      'SELECT *' +
        ' FROM Job AS j1, Job AS j2' +
        ' WHERE j1.minSalary = j2.maxSalary' +
        ' ORDER BY j1.id DESC, j2.id DESC;',
      query.toSql()
    );

    query = db
      .select(
        e.col('firstName'),
        e.col('lastName'),
        d.col('name'),
        j.col('title')
      )
      .from(e)
      .innerJoin(j, j.col('id').eq(e.col('jobId')))
      .innerJoin(d, d.col('id').eq(e.col('departmentId')));
    assert.equal(
      'SELECT Employee.firstName, Employee.lastName,' +
        ' Department.name, Job.title' +
        ' FROM Employee, Job, Department' +
        ' WHERE (Department.id = Employee.departmentId) AND' +
        ' (Job.id = Employee.jobId);',
      query.toSql()
    );
  });

  it('selectToSql_SingleOuterJoin', () => {
    const e = db.getSchema().table('Employee');
    const pred = j.col('id').eq(e.col('jobId'));
    const query = db
      .select(e.col('firstName'), j.col('title'))
      .from(e)
      .leftOuterJoin(j, pred);
    assert.equal(
      'SELECT Employee.firstName, Job.title' +
        ' FROM Employee LEFT OUTER JOIN Job ON (Employee.jobId = Job.id);',
      query.toSql()
    );
  });

  it('selectToSql_MultipleOuterJoin', () => {
    const e = db.getSchema().table('Employee');
    const d = db.getSchema().table('Department');
    const jh = db.getSchema().table('JobHistory');
    const pred1 = j.col('id').eq(e.col('jobId'));
    const pred2 = d.col('id').eq(e.col('departmentId'));
    const pred3 = j.col('id').eq(jh.col('jobId'));
    const query = db
      .select(
        e.col('firstName'),
        e.col('lastName'),
        d.col('name'),
        j.col('title')
      )
      .from(e)
      .leftOuterJoin(j, pred1)
      .leftOuterJoin(d, pred2)
      .leftOuterJoin(jh, pred3);
    assert.equal(
      'SELECT Employee.firstName, Employee.lastName,' +
        ' Department.name, Job.title' +
        ' FROM Employee LEFT OUTER JOIN Job ON (Employee.jobId = Job.id) ' +
        'LEFT OUTER JOIN Department ON (Employee.departmentId = Department.id) ' +
        'LEFT OUTER JOIN JobHistory ON (Job.id = JobHistory.jobId);',
      query.toSql()
    );
  });

  it('selectToSql_InnerJoinWithOuterJoin', () => {
    const e = db.getSchema().table('Employee');
    const d = db.getSchema().table('Department');
    const jh = db.getSchema().table('JobHistory');
    let pred1 = j.col('id').eq(e.col('jobId'));
    let pred2 = d.col('id').eq(e.col('departmentId'));
    let pred3 = jh.col('jobId').eq(j.col('id'));
    let query = db
      .select(
        e.col('firstName'),
        e.col('lastName'),
        d.col('name'),
        j.col('title')
      )
      .from(e)
      .leftOuterJoin(j, pred1)
      .leftOuterJoin(d, pred2)
      .innerJoin(jh, pred3);
    assert.equal(
      'SELECT Employee.firstName, Employee.lastName,' +
        ' Department.name, Job.title' +
        ' FROM Employee' +
        ' LEFT OUTER JOIN Job ON (Employee.jobId = Job.id)' +
        ' LEFT OUTER JOIN Department ON (Employee.departmentId = Department.id)' +
        ' INNER JOIN JobHistory ON (JobHistory.jobId = Job.id);',
      query.toSql()
    );
    // Change order of inner and outer join compared to previous one.
    pred1 = pred1.copy();
    pred2 = pred2.copy();
    pred3 = pred3.copy();
    query = db
      .select(
        e.col('firstName'),
        e.col('lastName'),
        d.col('name'),
        j.col('title')
      )
      .from(e)
      .leftOuterJoin(j, pred1)
      .innerJoin(d, pred2)
      .leftOuterJoin(jh, pred3);
    assert.equal(
      'SELECT Employee.firstName, Employee.lastName,' +
        ' Department.name, Job.title' +
        ' FROM Employee' +
        ' LEFT OUTER JOIN Job ON (Employee.jobId = Job.id)' +
        ' INNER JOIN Department ON (Department.id = Employee.departmentId)' +
        ' LEFT OUTER JOIN JobHistory ON (Job.id = JobHistory.jobId);',
      query.toSql()
    );
  });

  it('selectToSql_WhereWithOuterJoin', () => {
    const e = db.getSchema().table('Employee');
    const d = db.getSchema().table('Department');
    const jh = db.getSchema().table('JobHistory');
    let pred1 = j.col('id').eq(e.col('jobId'));
    let pred2 = d.col('id').eq(e.col('departmentId'));
    let pred3 = j.col('id').eq(jh.col('jobId'));
    let query = db
      .select(
        e.col('firstName'),
        e.col('lastName'),
        d.col('name'),
        j.col('title')
      )
      .from(e)
      .leftOuterJoin(j, pred1)
      .leftOuterJoin(d, pred2)
      .innerJoin(jh, pred3)
      .where(j.col('id').eq(1));
    assert.equal(
      'SELECT Employee.firstName, Employee.lastName,' +
        ' Department.name, Job.title' +
        ' FROM Employee' +
        ' LEFT OUTER JOIN Job ON (Employee.jobId = Job.id)' +
        ' LEFT OUTER JOIN Department ON (Employee.departmentId = Department.id)' +
        ' INNER JOIN JobHistory ON (Job.id = JobHistory.jobId)' +
        " WHERE Job.id = '1';",
      query.toSql()
    );
    // In the following assert, where has a combined predicate.
    pred1 = pred1.copy();
    pred2 = pred2.copy();
    pred3 = pred3.copy();
    query = db
      .select(
        e.col('firstName'),
        e.col('lastName'),
        d.col('name'),
        j.col('title')
      )
      .from(e)
      .leftOuterJoin(j, pred1)
      .leftOuterJoin(d, pred2)
      .innerJoin(jh, pred3)
      .where(
        op.or(
          op.and(j.col('id').eq(1), d.col('id').eq(1)),
          jh.col('jobId').eq('1')
        )
      );
    assert.equal(
      'SELECT Employee.firstName, Employee.lastName,' +
        ' Department.name, Job.title' +
        ' FROM Employee' +
        ' LEFT OUTER JOIN Job ON (Employee.jobId = Job.id)' +
        ' LEFT OUTER JOIN Department ON (Employee.departmentId = Department.id)' +
        ' INNER JOIN JobHistory ON (Job.id = JobHistory.jobId)' +
        " WHERE ((Job.id = '1') AND" +
        " (Department.id = '1')) OR" +
        " (JobHistory.jobId = '1');",
      query.toSql()
    );
  });

  it('null', () => {
    const e = db.getSchema().table('Employee');
    const row = e.createRow({
      commissionPercent: 0,
      departmentId: 'g',
      email: 'c',
      firstName: 'a',
      hireDate: null,
      id: '1',
      jobId: 'e',
      lastName: 'b',
      managerId: 'f',
      phoneNumber: 'd',
      photo: null,
      salary: 10000,
    });

    const query = db
      .insert()
      .into(e)
      .values([row]);
    assert.equal(
      'INSERT INTO Employee(id, firstName, lastName, email, phoneNumber, ' +
        'hireDate, jobId, salary, commissionPercent, managerId, departmentId, ' +
        "photo) VALUES ('1', 'a', 'b', 'c', 'd', NULL, 'e', 10000, " +
        "0, 'f', 'g', NULL);",
      query.toSql()
    );
  });

  it('stripValueInfo', () => {
    const query = db
      .select(j.col('title'))
      .from(j)
      .where(op.and(j.col('minSalary').gt(bind(0)), j.col('id').eq(1)));
    assert.equal(
      'SELECT Job.title FROM Job WHERE (Job.minSalary > ?0) AND (Job.id = #);',
      query.toSql(true)
    );

    // Simulate wrong usage exposed by toSql().
    query.bind([null]);
    assert.equal(
      'SELECT Job.title FROM Job WHERE ' +
        '(Job.minSalary > NULL) AND (Job.id = #);',
      query.toSql(true)
    );

    const job = HRSchemaSampleData.generateSampleJobData();
    const query2 = db
      .insert()
      .into(j)
      .values([job]);
    assert.equal(
      'INSERT INTO Job(id, title, minSalary, maxSalary) VALUES (#, #, #, #);',
      query2.toSql(true)
    );
  });

  it('nullConversion', () => {
    let query = db
      .select()
      .from(j)
      .where(j.col('id').isNull());
    assert.equal('SELECT * FROM Job WHERE Job.id IS NULL;', query.toSql());
    query = db
      .select()
      .from(j)
      .where(j.col('id').isNotNull());
    assert.equal(
      'SELECT * FROM Job WHERE Job.id IS NOT NULL;',
      query.toSql(true)
    );
  });
});
