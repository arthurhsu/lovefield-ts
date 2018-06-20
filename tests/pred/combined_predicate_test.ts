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
import {DatabaseConnection} from '../../lib/base/database_connection';
import {DataStoreType} from '../../lib/base/enum';
import {Row} from '../../lib/base/row';
import {op} from '../../lib/fn/op';
import {CombinedPredicate} from '../../lib/pred/combined_predicate';
import {Predicate} from '../../lib/pred/predicate';
import {PredicateNode} from '../../lib/pred/predicate_node';
import {Relation} from '../../lib/proc/relation';
import {Table} from '../../lib/schema/table';
import {TreeHelper} from '../../lib/structs/tree_helper';
import {TreeNode} from '../../lib/structs/tree_node';
import {getHrDbSchemaBuilder} from '../../testing/hr_schema/hr_schema_builder';
import {HRSchemaSampleData} from '../../testing/hr_schema/hr_schema_sample_data';

const assert = chai.assert;

describe('CombinedPredicate', () => {
  let db: DatabaseConnection;
  let e: Table;
  let j: Table;
  let d: Table;

  before(() => {
    return getHrDbSchemaBuilder()
        .connect({storeType: DataStoreType.MEMORY})
        .then((conn) => {
          db = conn;
          d = db.getSchema().table('Department');
          e = db.getSchema().table('Employee');
          j = db.getSchema().table('Job');
        });
  });

  after(() => {
    db.close();
  });

  // Tests that copy() creates an identical tree where each node is a new
  // instance.
  it('copy_Simple', () => {
    const expectedTree =
        'combined_pred_and\n' +
        '-value_pred(Employee.salary gte 200)\n' +
        '-value_pred(Employee.salary lte 600)\n';

    const original =
        op.and(e['salary'].gte(200), e['salary'].lte(600)) as PredicateNode;
    const copy = original.copy() as PredicateNode;
    assertTreesIdentical(expectedTree, original, copy);
    assert.equal(original.getId(), copy.getId());
  });

  // Tests that copy() creates an identical tree where each node is a new
  // instance for the case of a tree with 3+ nodes.
  it('copy_VarArgs', () => {
    const expectedTree =
        'combined_pred_and\n' +
        '-value_pred(Employee.salary gte 200)\n' +
        '-value_pred(Employee.salary lte 600)\n' +
        '-value_pred(Employee.commissionPercent lt 0.15)\n' +
        '-value_pred(Employee.commissionPercent gt 0.1)\n';

    const original =
        op.and(
            e['salary'].gte(200),
            e['salary'].lte(600),
            e['commissionPercent'].lt(0.15),
            e['commissionPercent'].gt(0.1)) as PredicateNode;
    const copy = original.copy() as PredicateNode;
    assertTreesIdentical(expectedTree, original, copy);
    assert.equal(original.getId(), copy.getId());
  });

  // Tests that copy() creates an identical tree where each node is a new
  // instance for the case of a tree with nested CombinedPredicate instances.
  it('copy_Nested', () => {
    const expectedTree =
        'combined_pred_and\n' +
        '-value_pred(Employee.salary gte 200)\n' +
        '-combined_pred_and\n' +
        '--join_pred(Employee.jobId eq Job.id)\n' +
        '--join_pred(Employee.departmentId eq Department.id)\n';

    const original: PredicateNode = op.and(
        e['salary'].gte(200),
        op.and(
            e['jobId'].eq(j['id']),
            e['departmentId'].eq(d['id']))) as PredicateNode;
    const copy = original.copy() as PredicateNode;
    assertTreesIdentical(expectedTree, original, copy);
    assert.equal(original.getId(), copy.getId());
  });

  // Asserts that the given trees have identical structure and that they do not
  // hold any common object references.
  function assertTreesIdentical(
      expectedTree: string, original: TreeNode, copy: TreeNode): void {
    assert.equal(expectedTree, TreeHelper.toString(original));
    assert.equal(expectedTree, TreeHelper.toString(copy));

    // Asserting that the copy tree holds new instances for each node.
    const originalTraversedNodes: TreeNode[] = [];
    original.traverse((node) => {
      originalTraversedNodes.push(node);
      return true;
    });

    copy.traverse((node) => {
      const originalNode = originalTraversedNodes.shift();
      assert.notEqual(originalNode, node);
      return true;
    });
  }

  // Tests that Predicate#getColumns() returns all involved columns for various
  // predicates.
  it('getColumns', () => {
    const p1 =
        op.and(e['salary'].gte(200), e['salary'].lte(600)) as PredicateNode;
    let expectedColumns = [e['salary']];
    assert.sameMembers(expectedColumns, p1.getColumns());

    const p2 =
        op.and(
            e['salary'].gte(200),
            e['salary'].lte(600),
            e['commissionPercent'].lt(0.15),
            e['commissionPercent'].gt(0.1)) as PredicateNode;
    expectedColumns = [e['salary'], e['commissionPercent']];
    assert.sameMembers(expectedColumns, p2.getColumns());

    const p3 = op.and(
        e['salary'].gte(200),
        op.and(
            e['jobId'].eq(j['id']),
            e['departmentId'].eq(['d.id'])));
    expectedColumns = [
      e['salary'], e['jobId'], e['departmentId'],
      j['id'], d['id'],
    ];
    assert.sameMembers(expectedColumns, p3.getColumns());
  });

  it('getTables', () => {
    const p1 =
        op.and(e['salary'].gte(200), e['salary'].lte(600)) as PredicateNode;
    let expectedTables = [e];
    assert.sameMembers(expectedTables, Array.from(p1.getTables().values()));

    const p2 =
        op.and(e['salary'].gte(200), j['maxSalary'].lte(600)) as PredicateNode;
    expectedTables = [e, j];
    assert.sameMembers(expectedTables, Array.from(p2.getTables().values()));

    const p3 = op.and(
        j['maxSalary'].gte(200),
        op.and(
            e['jobId'].eq(j['id']),
            e['departmentId'].eq(d['id'])));
    expectedTables = [e, j, d];
    assert.sameMembers(expectedTables, Array.from(p3.getTables().values()));
  });

  // Tests the setComplement() method for the case of an AND predicate.
  it('setComplement_And', () => {
    const predicate = op.and(e['salary'].gte(200), e['salary'].lte(600));
    const expectedSalariesOriginal = [200, 300, 400, 500, 600];
    const expectedSalariesComplement = [0, 100, 700];

    checkSetComplement(
        predicate, 8, expectedSalariesOriginal, expectedSalariesComplement);
  });

  // Tests the setComplement() method for the case of an OR predicate.
  it('setComplement_Or', () => {
    const predicate = op.or(e['salary'].lte(200), e['salary'].gte(600));
    const expectedSalariesOriginal = [0, 100, 200, 600, 700];
    const expectedSalariesComplement = [300, 400, 500];

    checkSetComplement(
        predicate, 8, expectedSalariesOriginal, expectedSalariesComplement);
  });

  it('isKeyRangeCompatbile_And', () => {
    const predicate =
        op.and(e['salary'].gte(200), e['salary'].lte(600)) as CombinedPredicate;
    assert.isFalse(predicate.isKeyRangeCompatible());
  });

  it('isKeyRangeCompatbile_Or', () => {
    const keyRangeCompatbilePredicates = [
      op.or(e['salary'].eq(200), e['salary'].eq(600)),
      op.or(e['salary'].lte(200), e['salary'].gte(600)),
      op.or(e['salary'].eq(200)),
    ];
    keyRangeCompatbilePredicates.forEach((p) => {
      assert.isTrue((p as CombinedPredicate).isKeyRangeCompatible());
    });

    const notKeyRangeCompatbilePredicates = [
      op.or(e['firstName'].match(/Foo/), e['firstName'].eq('Bar')),
      op.or(e['firstName'].neq('Foo'), e['firstName'].eq('Bar')),
      op.or(e['salary'].eq(100),
      op.or(e['salary'].eq(200), e['salary'].eq(300))),
      op.or(e['salary'].isNull(), e['salary'].eq(600)),
      op.or(e['firstName'].eq('Foo'), e['lastName'].eq('Bar')),
    ];
    notKeyRangeCompatbilePredicates.forEach((p) => {
      assert.isFalse((p as CombinedPredicate).isKeyRangeCompatible());
    });
  });

  it('toKeyRange_Or', () => {
    const testCases = [
      [op.or(e['salary'].eq(200), e['salary'].eq(600)), '[200, 200],[600, 600]'],
      [op.or(e['salary'].lte(200), e['salary'].gte(600)),
      '[unbound, 200],[600, unbound]'],
      [op.or(e['salary'].lt(200), e['salary'].lt(100)), '[unbound, 200)'],
      [op.or(e['salary'].eq(200), e['salary'].eq(200)), '[200, 200]'],
    ];

    testCases.forEach((testCase) => {
      const predicate = testCase[0] as CombinedPredicate;
      const expected = testCase[1];
      assert.equal(expected, predicate.toKeyRange().toString());
    });
  });

  // Performs a series of tests for the setComplement() method.
  function checkSetComplement(
      predicate: Predicate, sampleRowCount: number,
      expectedSalariesOriginal: number[],
      expectedSalariesComplement: number[]): void {
    const extractSalaries = (relation: Relation) => {
      return relation.entries.map((entry) => entry.row.payload()['salary']);
    };

    const inputRelation = Relation.fromRows(
        getSampleRows(sampleRowCount), [e.getName()]);

    const assertOriginal = () => {
      const outputRelation = predicate.eval(inputRelation);
      assert.sameDeepOrderedMembers(
          expectedSalariesOriginal,
          extractSalaries(outputRelation));
    };

    const assertComplement = () => {
      const outputRelation = predicate.eval(inputRelation);
      assert.sameDeepOrderedMembers(
          expectedSalariesComplement,
          extractSalaries(outputRelation));
    };

    // Testing the original predicate.
    assertOriginal();

    // Testing the complement predicate.
    predicate.setComplement(true);
    assertComplement();

    // Testing going from the complement predicate back to the original.
    predicate.setComplement(false);
    assertOriginal();

    // Testing that calling setComplement() twice with the same value leaves the
    // predicate in a consistent state.
    predicate.setComplement(true);
    predicate.setComplement(true);
    assertComplement();
  }

  // Generates sample emolyee data to be used for tests.
  function getSampleRows(rowCount: number): Row[] {
    const employees: Row[] = new Array(rowCount);
    for (let i = 0; i < rowCount; i++) {
      const employee = HRSchemaSampleData.generateSampleEmployeeData();
      employee.payload()['id'] = i.toString();
      employee.payload()['salary'] = 100 * i;
      employees[i] = employee;
    }
    return employees;
  }
});
