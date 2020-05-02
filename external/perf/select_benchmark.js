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
import * as lf from './node_modules/lovefield-ts/dist/es6/lf.js';

import {getHrDbSchemaBuilder} from './hr_schema_no_fk.js';
import {MockDataGenerator} from './mock_data_generator.js';
import {TestCase} from './test_case.js';

export class SelectBenchmark {
  async init(volatile) {
    const options = {
      storeType: volatile ?
          lf.DataStoreType.MEMORY : lf.DataStoreType.INDEXED_DB,
      enableInspector: true,
    };

    this.db = await getHrDbSchemaBuilder().connect(options);
    this.e = this.db.getSchema().table('Employee');
    this.j = this.db.getSchema().table('Job');
    this.cct = this.db.getSchema().table('CrossColumnTable');
    return Promise.resolve(this.db);
  }

  genData(dataGenerator) {
    this.dataGenerator = dataGenerator;

    // The number of expected results for all "range" and "spaced out" queries.
    // This is necessary so that even though the data is randomly generated, the
    // query returns the same amount of results each time, such that timings are
    // comparable across runs.
    this.employeeResultCount = this.dataGenerator.sampleEmployees.length / 10;

    this.queryData = this.generateQueryData();
    return Promise.resolve();
  }

  // The precision to use when comparing floating point numbers.
  static get EPSILON() {
    return Math.pow(10, -9);
  }

  generateQueryData() {
    const queryData = {};
    const sampleEmployees = this.dataGenerator.sampleEmployees;

    const employeeIdIndex =
        Math.floor(Math.random() * sampleEmployees.length);
    const employeeEmailIndex =
        Math.floor(Math.random() * sampleEmployees.length);

    queryData.employeeId = sampleEmployees[employeeIdIndex].payload()['id'];
    queryData.employeeEmail =
        sampleEmployees[employeeEmailIndex].payload()['email'];

    // Randomly select an employee somewhere in the first half.
    const employeeIndex1 =
        Math.floor(Math.random() * (sampleEmployees.length / 2));
    // Randomly select an employee somewhere in the second half.
    const employeeIndex2 = Math.floor(
        (sampleEmployees.length / 2) +
            Math.random() * (sampleEmployees.length / 2));
    queryData.employeeIds = [
      sampleEmployees[employeeIndex1].payload()['id'],
      sampleEmployees[Math.floor((employeeIndex1 + employeeIndex2) / 2)]
          .payload()['id'],
      sampleEmployees[employeeIndex2].payload()['id'],
    ];

    queryData.employeeSalariesSpacedOut = [];
    queryData.employeeHireDatesSpacedOut = [];
    const step = Math.floor(
        this.dataGenerator.sampleEmployees.length /
        this.employeeResultCount);
    for (let i = 0; i < sampleEmployees.length; i += step) {
      queryData.employeeSalariesSpacedOut.push(
          sampleEmployees[i].payload()['salary']);
      queryData.employeeHireDatesSpacedOut.push(
          sampleEmployees[i].payload()['hireDate']);
    }

    // Sorting employees by hireDate.
    sampleEmployees.sort((emp1, emp2) => {
      return emp1.payload()['hireDate'] - emp2.payload()['hireDate'];
    });
    let employee1Index = Math.floor(
        Math.random() * (sampleEmployees.length / 2));
    let employee2Index = employee1Index + this.employeeResultCount - 1;
    // Selecting hireDateStart and hireDateEnd such that the amount of results
    // falling within the range is EMPLOYEE_RESULT_COUNT.
    queryData.employeeHireDateStart =
        sampleEmployees[employee1Index].payload()['hireDate'];
    queryData.employeeHireDateEnd =
        sampleEmployees[employee2Index].payload()['hireDate'];

    // Sorting employees by salary.
    sampleEmployees.sort((emp1, emp2) => {
      return emp1.payload()['salary'] - emp2.payload()['salary'];
    });
    employee1Index = Math.floor(
        Math.random() * (sampleEmployees.length / 2));
    employee2Index = employee1Index + this.employeeResultCount - 1;
    // Selecting employeeSalaryStart and employeeSalaryEnd such that the amount
    // of results falling within the range is EMPLOYEE_RESULT_COUNT.
    queryData.employeeSalaryStart =
        sampleEmployees[employee1Index].payload()['salary'];
    queryData.employeeSalaryEnd =
        sampleEmployees[employee2Index].payload()['salary'];
    queryData.employeeLimit = 5;
    queryData.employeeSkip = 2000;

    return queryData;
  }

  async insertSampleData() {
    const insertEmployees = this.db
        .insert()
        .into(this.e)
        .values(this.dataGenerator.sampleEmployees)
        .exec();

    const insertJobs = this.db
        .insert()
        .into(this.j)
        .values(this.dataGenerator.sampleJobs)
        .exec();

    const crossColumnTableRows = [];
    for (let i = 0; i < 200; i++) {
      for (let j = 0; j < 200; j++) {
        crossColumnTableRows.push(this.cct.createRow({
          integer1: i,
          integer2: j,
        }));
      }
    }
    const insertCrossColumnTable = this.db
        .insert()
        .into(this.cct)
        .values(crossColumnTableRows)
        .exec();

    return Promise.all([insertJobs, insertEmployees, insertCrossColumnTable]);
  }

  async tearDown() {
    const tx = this.db.createTransaction();
    return tx.exec([
      this.db.delete().from(this.e),
      this.db.delete().from(this.j),
    ]).then(() => {
      this.db.close();
    });
  }

  async querySingleRowIndexed() {
    return this.db
        .select()
        .from(this.e)
        .where(this.e['id'].eq(this.queryData.employeeId))
        .exec();
  }

  async verifySingleRowIndexed(results) {
    if (results.length != 1) {
      return Promise.resolve(false);
    }

    return Promise.resolve(results[0]['id'] == this.queryData.employeeId);
  }

  async querySingleRowNonIndexed() {
    return this.db
        .select()
        .from(this.e)
        .where(this.e['email'].eq(this.queryData.employeeEmail))
        .exec();
  }

  async verifySingleRowNonIndexed(results) {
    if (results.length < 1) {
      return Promise.resolve(false);
    }

    const validated =
        results.every((obj) => obj['email'] == this.queryData.employeeEmail);
    return Promise.resolve(validated);
  }

  // Note: The following query uses two ValuePredicates that both operate on an
  // indexed property. This causes the IndexRangeScanPass to select the most
  // selective index for the IndexRangeScan step by calling BTree#cost(), and
  // because of the TODO that exists in that method performance is very poor.
  // Should observe a noticeable change once BTree#cost() is optimized.
  async querySingleRowMultipleIndices() {
    // Selecting a value purposefully high such that all rows satisfy this
    // predicate, whereas the other predicate is satisfied by only one row.
    const salaryThreshold =
        2 * this.dataGenerator.employeeGroundTruth.avgSalary;

    const q = this.db
        .select()
        .from(this.e)
        .orderBy(this.e['id'], lf.Order.ASC)
        .where(lf.op.and(
            this.e['id'].eq(this.queryData.employeeId),
            this.e['salary'].lt(salaryThreshold)));

    return q.exec();
  }

  async verifySingleRowMultipleIndices(results) {
    const salaryThreshold =
        2 * this.dataGenerator.employeeGroundTruth.avgSalary;

    const validated = results.every((obj) => {
      return obj[this.e['id'].getName()] == this.queryData.employeeId &&
          obj[this.e['salary'].getName()] < salaryThreshold;
    });

    return Promise.resolve(validated);
  }

  async queryMultiRowIndexedSpacedOut() {
    return this.db
        .select()
        .from(this.e)
        .where(this.e['salary'].in(this.queryData.employeeSalariesSpacedOut))
        .exec();
  }

  async verifyMultiRowIndexedSpacedOut(results) {
    // Multiple employees can have the same salary.
    if (results.length < this.queryData.employeeSalariesSpacedOut.length) {
      return Promise.resolve(false);
    }

    const salariesSet = new Set(this.queryData.employeeSalariesSpacedOut);
    const errorsExist =
        results.some((obj, index) => !salariesSet.has(obj.salary));

    return Promise.resolve(!errorsExist);
  }

  async queryMultiRowIndexedRange() {
    return this.db
        .select()
        .from(this.e)
        .where(this.e['salary'].between(
            this.queryData.employeeSalaryStart,
            this.queryData.employeeSalaryEnd,
        ))
        .exec();
  }

  async verifyMultiRowIndexedRange(results) {
    const employeeIdSet = new Set();
    this.dataGenerator.sampleEmployees.forEach((employee) => {
      if (employee.payload()['salary'] >= this.queryData.employeeSalaryStart &&
          employee.payload()['salary'] <= this.queryData.employeeSalaryEnd) {
        employeeIdSet.add(employee.payload()['id']);
      }
    });

    if (results.length != employeeIdSet.size) {
      return Promise.resolve(false);
    }

    const validated = results.every((obj) => employeeIdSet.has(obj.id));
    return Promise.resolve(validated);
  }

  async queryMultiRowNonIndexedSpacedOut() {
    return this.db
        .select()
        .from(this.e)
        .where(this.e['hireDate'].in(this.queryData.employeeHireDatesSpacedOut))
        .exec();
  }

  async verifyMultiRowNonIndexedSpacedOut(results) {
    if (results.length < this.queryData.employeeHireDatesSpacedOut.length) {
      return Promise.resolve(false);
    }

    const dateStamps = this.queryData.employeeHireDatesSpacedOut.map((date) => {
      return date.getTime();
    });

    const dateStampsSet = new Set(dateStamps);
    const errorsExist = results.some((obj, index) => {
      return !dateStampsSet.has(obj.hireDate.getTime());
    });

    return Promise.resolve(!errorsExist);
  }

  async queryMultiRowNonIndexedRange() {
    return this.db
        .select()
        .from(this.e)
        .where(this.e['hireDate'].between(
            this.queryData.employeeHireDateStart,
            this.queryData.employeeHireDateEnd,
        ))
        .exec();
  }

  async verifyMultiRowNonIndexedRange(results) {
    const employeeHireDateSet = new Set();
    this.dataGenerator.sampleEmployees.forEach((employee) => {
      if (employee.payload()['hireDate'].getTime() >=
          this.queryData.employeeHireDateStart.getTime() &&
          employee.payload()['hireDate'].getTime() <=
          this.queryData.employeeHireDateEnd.getTime()) {
        employeeHireDateSet.add(employee.payload()['hireDate']);
      }
    });

    if (results.length != employeeHireDateSet.size) {
      return Promise.resolve(false);
    }

    const validated =
       results.every((obj) => employeeHireDateSet.has(obj.hireDate));
    return Promise.resolve(validated);
  }

  async queryIndexedOrPredicate() {
    const predicates = this.queryData.employeeIds.map((employeeId) => {
      return this.e['id'].eq(employeeId);
    });

    return this.db
        .select()
        .from(this.e)
        .where(lf.op.or(...predicates))
        .exec();
  }

  async verifyIndexedOrPredicate(results) {
    if (this.queryData.employeeIds.length != results.length) {
      return Promise.resolve(false);
    }

    const actualIds =
        new Set(results.map((obj) => obj[this.e['id'].getName()]));
    const validated =
        this.queryData.employeeIds.every((id) => actualIds.has(id));
    return Promise.resolve(validated);
  }

  async queryIndexedOrPredicateMultiColumn() {
    const targetSalary1 = this.queryData.employeeSalaryStart;
    const targetSalary2 = this.queryData.employeeSalaryEnd;
    const targetId = this.queryData.employeeId;

    return this.db
        .select()
        .from(this.e)
        .where(lf.op.or(
            this.e['id'].eq(targetId),
            this.e['salary'].in([targetSalary1, targetSalary2]),
        ))
        .exec();
  }

  async verifyIndexedOrPredicateMultiColumn(results) {
    if (results < 2) {
      return Promise.resolve(false);
    }

    const targetSalary1 = this.queryData.employeeSalaryStart;
    const targetSalary2 = this.queryData.employeeSalaryEnd;
    const targetId = this.queryData.employeeId;

    const validated = results.every((obj) => {
      return obj['id'] == targetId ||
          obj['salary'] == targetSalary1 ||
          obj['salary'] == targetSalary2;
    });
    return Promise.resolve(validated);
  }

  async queryIndexedInPredicate() {
    return this.db
        .select()
        .from(this.e)
        .where(this.e['id'].in(this.queryData.employeeIds))
        .exec();
  }

  async queryOrderByIndexed() {
    return this.db
        .select()
        .from(this.e)
        .orderBy(this.e['salary'], lf.Order.DESC)
        .exec();
  }

  async verifyOrderByIndexed(results) {
    if (results.length != this.dataGenerator.sampleEmployees.length) {
      return Promise.resolve(false);
    }

    for (let i = 1; i < results.length; i++) {
      const salary1 = results[i]['salary'];
      const salary0 = results[i - 1]['salary'];
      if (salary1 === undefined || salary1 === null ||
          salary0 === undefined || salary0 === null ||
          salary1 > salary0) {
        return Promise.resolve(false);
      }
    }

    return Promise.resolve(true);
  }

  async queryOrderByIndexedCrossColumn() {
    return this.db
        .select()
        .from(this.cct)
        .orderBy(this.cct['integer1'], lf.Order.ASC)
        .orderBy(this.cct['integer2'], lf.Order.DESC)
        .exec();
  }

  async verifyOrderByIndexedCrossColumn(results) {
    if (results.length != 200 * 200) {
      return Promise.resolve(false);
    }

    let objCounter = 0;
    for (let i = 0; i < 200; i++) {
      for (let j = 199; j >= 0; j--, objCounter++) {
        if (results[objCounter]['integer1'] != i ||
            results[objCounter]['integer2'] != j) {
          return Promise.resolve(false);
        }
      }
    }

    return Promise.resolve(true);
  }

  async queryOrderByNonIndexed() {
    return this.db
        .select()
        .from(this.e)
        .orderBy(this.e['commissionPercent'], lf.Order.DESC)
        .exec();
  }

  async verifyOrderByNonIndexed(results) {
    if (results.length != this.dataGenerator.sampleEmployees.length) {
      return Promise.resolve(false);
    }

    for (let i = 1; i < results.length; i++) {
      const cpt1 = results[i]['commissionPercent'];
      const cpt0 = results[i - 1]['commissionPercent'];
      if (cpt1 === undefined || cpt1 === null ||
          cpt0 === undefined || cpt0 === null ||
          cpt1 > cpt0) {
        return Promise.resolve(false);
      }
    }

    return Promise.resolve(true);
  }

  async queryLimitSkipIndexed() {
    return this.db
        .select()
        .from(this.e)
        .orderBy(this.e['salary'], lf.Order.DESC)
        .limit(this.queryData.employeeLimit)
        .skip(this.queryData.employeeSkip)
        .exec();
  }

  async verifyLimitSkipIndexed(results) {
    if (this.queryData.employeeLimit != results.length) {
      return Promise.resolve(false);
    }

    // Sorting sample employees by descending salary.
    this.dataGenerator.sampleEmployees.sort((emp1, emp2) => {
      return emp2.payload()['salary'] - emp1.payload()['salary'];
    });

    const expectedEmployees = this.dataGenerator.sampleEmployees.slice(
        this.queryData.employeeSkip,
        this.queryData.employeeSkip + this.queryData.employeeLimit,
    );

    const validated = expectedEmployees.every((employee, index) => {
      const obj = results[index];
      return employee.payload()['id'] == obj[this.e['id'].getName()];
    });

    return Promise.resolve(validated);
  }

  async queryProjectNonAggregatedColumns() {
    return this.db
        .select(this.e['email'], this.e['salary'])
        .from(this.e)
        .exec();
  }

  async verifyProjectNonAggregatedColumns(results) {
    if (results.length != this.dataGenerator.sampleEmployees.length) {
      return Promise.resolve(false);
    }

    const validated = results.every((obj) => {
      const keys = Object.keys(obj).sort((a, b) => a < b);
      return keys.length == 2 && keys[0] == 'email' && keys[1] == 'salary';
    });

    return Promise.resolve(validated);
  }

  async queryProjectAggregateIndexed() {
    return this.db
        .select(lf.fn.avg(this.e['salary']), lf.fn.stddev(this.e['salary']))
        .from(this.e)
        .exec();
  }

  async verifyProjectAggregateIndexed(results) {
    if (results.length != 1) {
      return Promise.resolve(false);
    }

    const avgSalaryColumn = lf.fn.avg(this.e['salary']);
    const stddevSalaryColumn = lf.fn.stddev(this.e['salary']);
    const approx = (a, b, tolerance) => {
      return Math.abs(a - b) <= (tolerance || 0.000001);
    };

    const validated =
        approx(results[0][avgSalaryColumn.getName()],
            this.dataGenerator.employeeGroundTruth.avgSalary,
            SelectBenchmark.EPSILON) &&
        approx(results[0][stddevSalaryColumn.getName()],
            this.dataGenerator.employeeGroundTruth.stddevSalary,
            SelectBenchmark.EPSILON);

    return Promise.resolve(validated);
  }

  async queryProjectAggregateNonIndexed() {
    return this.db
        .select(lf.fn.min(this.e['hireDate']), lf.fn.max(this.e['hireDate']))
        .from(this.e)
        .exec();
  }

  async verifyProjectAggregateNonIndexed(results) {
    if (results.length != 1) {
      return Promise.resolve(false);
    }

    const minHireDateColumn = lf.fn.min(this.e['hireDate']);
    const maxHireDateColumn = lf.fn.max(this.e['hireDate']);

    const validated =
        results[0][minHireDateColumn.getName()].getTime() ==
            this.dataGenerator.employeeGroundTruth.minHireDate.getTime() &&
        results[0][maxHireDateColumn.getName()].getTime() ==
            this.dataGenerator.employeeGroundTruth.maxHireDate.getTime();

    return Promise.resolve(validated);
  }

  async queryJoinEqui() {
    return this.db
        .select()
        .from(this.e, this.j)
        .where(this.e['jobId'].eq(this.j['id']))
        .exec();
  }

  async verifyJoinEqui(results) {
    if (this.dataGenerator.sampleEmployees.length != results.length) {
      return Promise.resolve(false);
    }

    const validated = results.every((obj) => {
      if (Object.keys(obj).length != 2) {
        return false;
      }
      const e = obj['Employee'];
      const j = obj['Job'];
      if (e === undefined || e === null ||
          j === undefined || j === null ||
          e['jobId'] !== j['id']) {
        return false;
      }
      return true;
    });

    return Promise.resolve(validated);
  }

  async queryJoinTheta() {
    return this.db
        .select()
        .from(this.j, this.e)
        .orderBy(this.e['id'], lf.Order.ASC)
        .where(lf.op.and(
            this.e['jobId'].eq(this.j['id']),
            this.e['salary'].gt(this.j['maxSalary']),
        ))
        .exec();
  }

  async verifyJoinTheta(results) {
    if (results.length !=
        this.dataGenerator.employeeGroundTruth.thetaJoinSalaryIds.length) {
      return Promise.resolve(false);
    }

    const validated = results.every((obj, i) => {
      return obj['Employee']['id'] ==
          this.dataGenerator.employeeGroundTruth.thetaJoinSalaryIds[i];
    });

    return Promise.resolve(validated);
  }

  async queryCountStar() {
    return this.db
        .select(lf.fn.count())
        .from(this.e)
        .exec();
  }

  async verifyCountStar(results) {
    const aggregatedColumn = lf.fn.count();
    const validated = (1 == results.length &&
        this.dataGenerator.sampleEmployees.length ==
            results[0][aggregatedColumn]);

    return Promise.resolve(validated);
  }

  getTestCases() {
    const testCases = [
      ['SingleRowIndexed',
        this.querySingleRowIndexed, this.verifySingleRowIndexed],
      ['SingleRowNonIndexed',
        this.querySingleRowNonIndexed, this.verifySingleRowNonIndexed],
      ['SingleRowMultipleIndices',
        this.querySingleRowMultipleIndices,
        this.verifySingleRowMultipleIndices],
      ['MultiRowIndexedRange',
        this.queryMultiRowIndexedRange, this.verifyMultiRowIndexedRange],
      ['MultiRowIndexedSpacedOut',
        this.queryMultiRowIndexedSpacedOut,
        this.verifyMultiRowIndexedSpacedOut],
      ['MultiRowNonIndexedRange',
        this.queryMultiRowNonIndexedRange, this.verifyMultiRowNonIndexedRange],
      ['MultiRowNonIndexedSpacedOut',
        this.queryMultiRowNonIndexedSpacedOut,
        this.verifyMultiRowNonIndexedSpacedOut],
      ['IndexedOrPredicate',
        this.queryIndexedOrPredicate,
        this.verifyIndexedOrPredicate],
      ['IndexedOrPredicateMultiColumn',
        this.queryIndexedOrPredicateMultiColumn,
        this.verifyIndexedOrPredicateMultiColumn],
      ['IndexedInPredicate',
        this.queryIndexedInPredicate,
        // Intentionally using the same verification method as with the OR case.
        this.verifyIndexedOrPredicate],
      ['OrderByIndexed', this.queryOrderByIndexed, this.verifyOrderByIndexed],
      ['OrderByNonIndexed',
        this.queryOrderByNonIndexed, this.verifyOrderByNonIndexed],
      ['OrderByIndexedCrossColumn',
        this.queryOrderByIndexedCrossColumn,
        this.verifyOrderByIndexedCrossColumn],
      ['LimitSkipIndexed',
        this.queryLimitSkipIndexed, this.verifyLimitSkipIndexed],
      ['ProjectNonAggregatedColumns',
        this.queryProjectNonAggregatedColumns,
        this.verifyProjectNonAggregatedColumns],
      ['ProjectAggregateIndexed',
        this.queryProjectAggregateIndexed,
        this.verifyProjectAggregateIndexed],
      ['ProjectAggregateNonIndexed',
        this.queryProjectAggregateNonIndexed,
        this.verifyProjectAggregateNonIndexed],
      ['JoinEqui', this.queryJoinEqui, this.verifyJoinEqui],
      ['JoinTheta', this.queryJoinTheta, this.verifyJoinTheta],
      ['CountStar', this.queryCountStar, this.verifyCountStar],
    ];

    return testCases.map((testCase) => {
      const testCaseName = testCase[0];
      const queryFn = testCase[1].bind(this);
      const verifyFn = testCase[2].bind(this);
      return new TestCase(testCaseName, queryFn, verifyFn);
    });
  }

  static async fromJson(jsonFileName, volatile) {
    const sampleData =
        await SelectBenchmark.loadSampleDataFromJson(jsonFileName);
    const selectBenchmark = new SelectBenchmark();
    const db = await selectBenchmark.init(volatile);

    const dataGenerator = MockDataGenerator.fromExportData(
        db.getSchema(), sampleData,
    );
    selectBenchmark.genData(dataGenerator);
    return Promise.resolve(selectBenchmark);
  }

  static async loadSampleDataFromJson(fileName) {
    return new Promise((resolve, reject) => {
      const xhr = new XMLHttpRequest();
      xhr.open('GET', fileName, true);
      xhr.onreadystatechange = () => {
        if (xhr.readyState === 4 && xhr.status === 200) {
          resolve(JSON.parse(xhr.responseText));
        }
      };
      xhr.error = reject;
      xhr.send();
    });
  }
}
