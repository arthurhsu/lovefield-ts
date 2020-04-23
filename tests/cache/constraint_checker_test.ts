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

import {
  ConstraintAction,
  ConstraintTiming,
  ErrorCode,
  Type,
} from '../../lib/base/enum';
import { Row } from '../../lib/base/row';
import { ConstraintChecker } from '../../lib/cache/constraint_checker';
import { Modification } from '../../lib/cache/modification';
import { Key } from '../../lib/index/key_range';
import { RuntimeIndex } from '../../lib/index/runtime_index';
import { BaseTable } from '../../lib/schema/base_table';
import { DatabaseSchema } from '../../lib/schema/database_schema';
import { schema } from '../../lib/schema/schema';
import { MockEnv } from '../../testing/mock_env';
import { SchemaTestHelper } from '../../testing/schema_test_helper';
import { TestUtil } from '../../testing/test_util';

const assert = chai.assert;

describe('ConstraintChecker', () => {
  let env: MockEnv;
  let checker: ConstraintChecker;
  const TIMINGS = [ConstraintTiming.IMMEDIATE, ConstraintTiming.DEFERRABLE];

  async function setUpEnvForSchema(schema1: DatabaseSchema): Promise<void> {
    env = new MockEnv(schema1);
    await env.init();
    checker = new ConstraintChecker(env.global);
    return Promise.resolve();
  }

  it('findExistingRowIdInPkIndex', async () => {
    const getSchema = () => {
      const schemaBuilder = schema.create('testSchema', 1);
      schemaBuilder
        .createTable('TableA')
        .addColumn('id', Type.STRING)
        .addPrimaryKey(['id']);
      return schemaBuilder.getSchema();
    };

    await setUpEnvForSchema(getSchema());
    const table = env.schema.table('TableA') as BaseTable;
    const pkIndexSchema = table.getConstraint().getPrimaryKey();
    const pkIndex = env.indexStore.get(
      pkIndexSchema.getNormalizedName()
    ) as RuntimeIndex;

    const row1 = table.createRow({ id: 'pk1', name: 'DummyName' });
    const row2 = table.createRow({ id: 'pk2', name: 'DummyName' });
    const pk1 = row1.payload()['id'] as Key;
    const pk2 = row2.payload()['id'] as Key;
    pkIndex.add(pk1, row1.id());
    pkIndex.add(pk2, row2.id());
    assert.isTrue(pkIndex.containsKey(pk1));
    assert.isTrue(pkIndex.containsKey(pk2));

    const row3 = table.createRow({ id: pk1, name: 'DummyName' });
    const row4 = table.createRow({ id: pk2, name: 'DummyName' });
    const row5 = table.createRow({ id: 'otherPk', name: 'DummyName' });

    assert.equal(row1.id(), checker.findExistingRowIdInPkIndex(table, row3));
    assert.equal(row2.id(), checker.findExistingRowIdInPkIndex(table, row4));
    assert.isNull(checker.findExistingRowIdInPkIndex(table, row5));
  });

  it('checkNotNullable', async () => {
    const getSchema = () => {
      const schemaBuilder = schema.create('testSchema', 1);
      schemaBuilder
        .createTable('TableA')
        .addColumn('id', Type.STRING)
        .addColumn('email', Type.STRING)
        .addPrimaryKey(['id']);
      return schemaBuilder.getSchema();
    };

    await setUpEnvForSchema(getSchema());
    const table = env.schema.table('TableA') as BaseTable;

    // Attempting to insert rows that violate the NOT_NULLABLE constraint.
    const invalidRows = [1, 2, 3].map(primaryKey => {
      return table.createRow({ id: primaryKey.toString(), email: null });
    });

    TestUtil.assertThrowsError(
      // 202: Attempted to insert NULL value to non-nullable field {0}
      ErrorCode.NOT_NULLABLE,
      () => checker.checkNotNullable(table, invalidRows)
    );

    // Attempting to insert rows that don't violate the constraint.
    const validRows = [1, 2, 3].map(primaryKey => {
      return table.createRow({
        id: primaryKey.toString(),
        email: 'emailAddress',
      });
    });
    assert.doesNotThrow(() => checker.checkNotNullable(table, validRows));
  });

  it('checkForeignKeysForInsert_Immediate', () => {
    return checkForeignKeysForInsert(ConstraintTiming.IMMEDIATE);
  });

  it('checkForeignKeysForInsert_Deferrable', () => {
    return checkForeignKeysForInsert(ConstraintTiming.DEFERRABLE);
  });

  // Asserts that ConstraintChecker#checkForeignKeysForInsert() throws an error
  // if the referred keys do not exist, for constraints that are of the given
  // constraint timing.
  async function checkForeignKeysForInsert(
    constraintTiming: ConstraintTiming
  ): Promise<void> {
    const schema1 = SchemaTestHelper.getOneForeignKey(constraintTiming);
    await setUpEnvForSchema(schema1);
    const childTable = env.schema.table('Child') as BaseTable;
    const childRow = childTable.createRow({ id: 'dummyId' });

    const checkFn = (timing: ConstraintTiming) => {
      checker.checkForeignKeysForInsert(childTable, [childRow], timing);
    };
    assertChecks(constraintTiming, checkFn);
  }

  // Tests that ConstraintChecker#checkForeignKeysForDelete() throws an error if
  // referring keys do exist, for constraints that are IMMEDIATE.
  it('checkForeignKeysForDelete_Immediate', () => {
    return checkForeignKeysForDelete(ConstraintTiming.IMMEDIATE);
  });

  // Tests that ConstraintChecker#checkForeignKeysForDelete() throws an error if
  // referring keys do exist, for constraints that are DEFERRABLE.
  it('checkForeignKeysForDelete_Deferrable', () => {
    return checkForeignKeysForDelete(ConstraintTiming.DEFERRABLE);
  });

  async function checkForeignKeysForDelete(
    constraintTiming: ConstraintTiming
  ): Promise<void> {
    const schema1 = SchemaTestHelper.getOneForeignKey(constraintTiming);
    await setUpEnvForSchema(schema1);
    const parentTable = env.schema.table('Parent');
    const childTable = env.schema.table('Child');
    const parentRow = parentTable.createRow({ id: 'dummyId' });
    const childRow = childTable.createRow({ id: 'dummyId' });

    const tx = env.db.createTransaction();
    await tx.exec([
      env.db
        .insert()
        .into(parentTable)
        .values([parentRow]),
      env.db
        .insert()
        .into(childTable)
        .values([childRow]),
    ]);

    const checkFn = (timing: ConstraintTiming) =>
      checker.checkForeignKeysForDelete(
        parentTable as BaseTable,
        [parentRow as Row],
        timing
      );
    assertChecks(constraintTiming, checkFn);
  }

  // Tests that ConstraintChecker#checkForeignKeysForUpdate() throws an error if
  // referring keys do exist for a column that is being updated, for constraints
  // that are IMMEDIATE.
  it('checkForeignKeysForUpdate_Immediate', () => {
    return checkForeignKeysForUpdate(ConstraintTiming.IMMEDIATE);
  });

  // Tests that ConstraintChecker#checkForeignKeysForUpdate() throws an error if
  // invalid referred keys are introduced for a column that is being updated,
  // for constraints that are DEFERRABLE.
  it('checkForeignKeysForUpdate_Deferrable', () => {
    return checkForeignKeysForUpdate(ConstraintTiming.DEFERRABLE);
  });

  async function checkForeignKeysForUpdate(
    constraintTiming: ConstraintTiming
  ): Promise<void> {
    const schema1 = SchemaTestHelper.getOneForeignKey(constraintTiming);
    await setUpEnvForSchema(schema1);
    const parentTable = env.schema.table('Parent');
    const childTable = env.schema.table('Child');
    const parentRow = parentTable.createRow({ id: 'dummyId' });
    const childRow = childTable.createRow({ id: 'dummyId' });

    const tx = env.db.createTransaction();
    await tx.exec([
      env.db
        .insert()
        .into(parentTable)
        .values([parentRow]),
      env.db
        .insert()
        .into(childTable)
        .values([childRow]),
    ]);
    const parentRowAfter = parentTable.createRow({ id: 'otherId' });
    const modification = [parentRow, parentRowAfter] as Modification;

    const checkFn = (timing: ConstraintTiming) =>
      checker.checkForeignKeysForUpdate(
        parentTable as BaseTable,
        [modification],
        timing
      );
    assertChecks(constraintTiming, checkFn);
  }

  // Tests that ConstraintChecker#detectCascadeDeletion() correctly detects
  // referring rows of the rows that are about to be deleted.
  it('detectCascadeDeletion_TableChain', async () => {
    const schema1 = SchemaTestHelper.getTableChain(ConstraintAction.CASCADE);

    await setUpEnvForSchema(schema1);
    const tableA = env.schema.table('TableA') as BaseTable;
    const tableB = env.schema.table('TableB');
    const tableC = env.schema.table('TableC');
    const tableARow = tableA.createRow({ id: 'tableADummyId' });
    const tableBRow = tableB.createRow({
      foreignKey: tableARow.payload()['id'],
      id: 'tableBDummyId',
    });
    const tableCRow = tableC.createRow({
      foreignKey: tableBRow.payload()['id'],
      id: 'tableCDummyId',
    });

    await env.db
      .insert()
      .into(tableA)
      .values([tableARow])
      .exec();

    // Checking the case where no referring rows exist.
    let cascadedDeletion = checker.detectCascadeDeletion(tableA, [tableARow]);
    assert.equal(0, cascadedDeletion.tableOrder.length);
    assert.equal(0, cascadedDeletion.rowIdsPerTable.size);

    // Inserting row referring to TableA's row.
    await env.db
      .insert()
      .into(tableB)
      .values([tableBRow])
      .exec();
    cascadedDeletion = checker.detectCascadeDeletion(tableA, [tableARow]);
    // Ensure that TableB's row has been detected for deletion.
    assert.equal(1, cascadedDeletion.tableOrder.length);
    assert.sameDeepOrderedMembers(
      [tableB.getName()],
      cascadedDeletion.tableOrder
    );
    assert.equal(1, cascadedDeletion.rowIdsPerTable.size);
    assert.sameDeepOrderedMembers(
      [tableBRow.id()],
      cascadedDeletion.rowIdsPerTable.get(tableB.getName()) as number[]
    );

    // Inserting row referring to TableB's row.
    await env.db
      .insert()
      .into(tableC)
      .values([tableCRow])
      .exec();

    cascadedDeletion = checker.detectCascadeDeletion(tableA, [tableARow]);
    // Ensure that both TableC's and TableB's row have been detected for
    // deletion.
    assert.equal(2, cascadedDeletion.tableOrder.length);
    assert.sameDeepOrderedMembers(
      [tableC.getName(), tableB.getName()],
      cascadedDeletion.tableOrder
    );
    assert.equal(2, cascadedDeletion.rowIdsPerTable.size);
    assert.sameDeepOrderedMembers(
      [tableBRow.id()],
      cascadedDeletion.rowIdsPerTable.get(tableB.getName()) as number[]
    );
    assert.sameDeepOrderedMembers(
      [tableCRow.id()],
      cascadedDeletion.rowIdsPerTable.get(tableC.getName()) as number[]
    );
  });

  it('detectCascadeDeletion_TwoForeignKeys', async () => {
    const schema1 = SchemaTestHelper.getTwoForeignKeys(
      ConstraintAction.CASCADE
    );
    await setUpEnvForSchema(schema1);
    const tableA = env.schema.table('TableA') as BaseTable;
    const tableB1 = env.schema.table('TableB1');
    const tableB2 = env.schema.table('TableB2');
    const tableARow = tableA.createRow({
      id1: 'tableADummyId1',
      id2: 'tableADummyId2',
    });
    const tableB1Row = tableB1.createRow({
      foreignKey: tableARow.payload()['id1'],
      id: 'tableB1DummyId',
    });
    const tableB2Row = tableB2.createRow({
      foreignKey: tableARow.payload()['id2'],
      id: 'tableB2DummyId',
    });

    const tx = env.db.createTransaction();
    await tx.exec([
      env.db
        .insert()
        .into(tableA)
        .values([tableARow]),
      env.db
        .insert()
        .into(tableB1)
        .values([tableB1Row]),
      env.db
        .insert()
        .into(tableB2)
        .values([tableB2Row]),
    ]);

    const cascadedDeletion = checker.detectCascadeDeletion(tableA, [tableARow]);
    // Ensure that both TableB1's and TableB2's row have been detected for
    // deletion.
    assert.equal(2, cascadedDeletion.tableOrder.length);
    assert.sameDeepOrderedMembers(
      [tableB1.getName(), tableB2.getName()],
      cascadedDeletion.tableOrder
    );
    assert.equal(2, cascadedDeletion.rowIdsPerTable.size);
    assert.sameDeepOrderedMembers(
      [tableB1Row.id()],
      cascadedDeletion.rowIdsPerTable.get(tableB1.getName()) as number[]
    );
    assert.sameDeepOrderedMembers(
      [tableB2Row.id()],
      cascadedDeletion.rowIdsPerTable.get(tableB2.getName()) as number[]
    );
  });

  // Asserts that the given constraint checking function, throws a constraint
  // violation error for the given constraint timing, and that it throw no error
  // for other constraint timings.
  function assertChecks(
    constraintTiming: ConstraintTiming,
    checkFn: (timing: ConstraintTiming) => void
  ): void {
    TIMINGS.forEach(timing => {
      if (timing === constraintTiming) {
        // Foreign key constraint violation on constraint {0}.
        TestUtil.assertThrowsError(
          ErrorCode.FK_VIOLATION,
          checkFn.bind(null, timing)
        );
      } else {
        assert.doesNotThrow(checkFn.bind(null, timing));
      }
    });
  }
});
