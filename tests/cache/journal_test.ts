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

import {ErrorCode} from '../../lib/base/enum';
import {Row} from '../../lib/base/row';
import {Journal} from '../../lib/cache/journal';
import {Key, SingleKeyRange} from '../../lib/index/key_range';
import {RuntimeIndex} from '../../lib/index/runtime_index';
import {BaseTable} from '../../lib/schema/base_table';
import {IndexImpl} from '../../lib/schema/index_impl';
import {MockEnv} from '../../testing/mock_env';
import {getMockSchemaBuilder} from '../../testing/mock_schema_builder';
import {TestUtil} from '../../testing/test_util';

const assert = chai.assert;

describe('Journal', () => {
  const NULL = (null as unknown) as Key;
  let env: MockEnv;

  beforeEach(() => {
    env = new MockEnv(getMockSchemaBuilder().getSchema());
    return env.init();
  });

  function createJournal(tables: BaseTable[]): Journal {
    return new Journal(env.global, new Set<BaseTable>(tables));
  }

  // Tests the case where a journal that has no write operations recorded is
  // committed.
  it('noWriteOperations', () => {
    const table = env.schema.table('tableC') as BaseTable;
    const journal = createJournal([table]);

    const commitFn = () => journal.commit();
    assert.doesNotThrow(commitFn);
  });

  // Tests the case where a new row is inserted into the journal.
  it('insert_New', () => {
    const table = env.schema.table('tableA') as BaseTable;
    const pkIndexSchema = table.getConstraint().getPrimaryKey();
    const pkIndex = env.indexStore.get(
      pkIndexSchema.getNormalizedName()
    ) as RuntimeIndex;
    const rowIdIndex = env.indexStore.get(
      table.getRowIdIndexName()
    ) as RuntimeIndex;

    const primaryKey = '100';
    const row = table.createRow({id: primaryKey, name: 'DummyName'});

    // First testing case where the row does not already exist.
    assert.equal(0, env.cache.getCount());
    assert.isFalse(pkIndex.containsKey(primaryKey));
    assert.isFalse(rowIdIndex.containsKey(row.id()));

    const journal = createJournal([table]);
    journal.insert(table, [row]);
    journal.commit();

    assert.equal(1, env.cache.getCount());
    assert.isTrue(pkIndex.containsKey(primaryKey));
    assert.isTrue(rowIdIndex.containsKey(row.id()));
  });

  // Tests the case where a row that has been inserted via a previous, already
  // committed journal is inserted.
  it('insert_PrimaryKeyViolation_SingleColumn', () => {
    const table = env.schema.table('tableA') as BaseTable;
    const primaryKey = '100';
    const row = table.createRow({id: primaryKey, name: 'DummyName'});
    const otherRow = table.createRow({
      id: primaryKey,
      name: 'OtherDummyName',
    });
    checkInsert_UniqueConstraintViolation(table, [row], [otherRow]);
  });

  // Tests the case where a row that has been inserted via a previous, already
  // committed journal is inserted, for the case where the primary key is
  // cross-column.
  it('insert_PrimaryKeyViolation_CrossColumn', () => {
    const table = env.schema.table('tableD') as BaseTable;
    const id1 = 'DummyId';
    const id2 = 100;
    const row = table.createRow({
      firstName: 'DummyFirstName',
      id1,
      id2,
      lastName: 'DummyLastName',
    });
    const otherRow = table.createRow({
      firstName: 'OtherDummyFirstName',
      id1,
      id2,
      lastName: 'OtherDummyLastName',
    });
    checkInsert_UniqueConstraintViolation(table, [row], [otherRow]);
  });

  // Checks that a constraint violation occurs when rows are inserted that have
  // declared unique keys that already exist in the database via a previously
  // committed journal.
  function checkInsert_UniqueConstraintViolation(
    table: BaseTable,
    rows1: Row[],
    rows2: Row[]
  ): void {
    // Inserting the row into the journal and committing.
    let journal = createJournal([table]);
    journal.insert(table, rows1);
    journal.commit();

    // Now re-inserting a row with the same primary key that already exists.
    journal = createJournal([table]);

    TestUtil.assertThrowsError(
      ErrorCode.DUPLICATE_KEYS, // Duplicate keys are not allowed.
      journal.insert.bind(journal, table, rows2)
    );
  }

  // Tests the case where a row that has been inserted previously within the
  // same uncommitted journal is inserted.
  it('insert_PrimaryKeyViolation2', () => {
    const table = env.schema.table('tableA') as BaseTable;
    const pkIndexSchema = table.getConstraint().getPrimaryKey();
    const pkIndex = env.indexStore.get(
      pkIndexSchema.getNormalizedName()
    ) as RuntimeIndex;

    const primaryKey = '100';
    const row1 = table.createRow({id: primaryKey, name: 'DummyName'});

    // Inserting row without committing. Indices and cache are updated
    // immediately regardless of committing or not.
    const journal = createJournal([table]);
    journal.insert(table, [row1]);
    assert.equal(1, env.cache.getCount());
    assert.isTrue(pkIndex.containsKey(primaryKey));

    // Inserting a row with the same primary key.
    const row2 = table.createRow({id: primaryKey, name: 'OtherDummyName'});
    TestUtil.assertThrowsError(
      ErrorCode.DUPLICATE_KEYS, // Duplicate keys are not allowed.
      journal.insert.bind(journal, table, [row2])
    );
    journal.rollback();

    // Expecting the entire journal to have been rolled back.
    assert.equal(0, env.cache.getCount());
    assert.isFalse(pkIndex.containsKey(primaryKey));
  });

  // Tests the case where some of the rows being inserted, have the same primary
  // key. An exception should be thrown even though the primary key does not
  // already exist prior this insertion.
  it('insert_PrimaryKeyViolation3', () => {
    const table = env.schema.table('tableA') as BaseTable;
    const pkIndexSchema = table.getConstraint().getPrimaryKey();
    const pkIndex = env.indexStore.get(
      pkIndexSchema.getNormalizedName()
    ) as RuntimeIndex;

    const rows = [];
    for (let i = 0; i < 3; i++) {
      rows.push(table.createRow({id: `pk${i}`, name: 'DummyName'}));
    }
    for (let j = 0; j < 3; j++) {
      rows.push(table.createRow({id: 'samePk', name: 'DummyName'}));
    }

    const journal = createJournal([table]);
    TestUtil.assertThrowsError(
      ErrorCode.DUPLICATE_KEYS, // Duplicate keys are not allowed.
      journal.insert.bind(journal, table, rows)
    );
    journal.rollback();

    assert.equal(0, env.cache.getCount());
    const rowIdIndex = env.indexStore.get(
      table.getRowIdIndexName()
    ) as RuntimeIndex;
    assert.equal(0, pkIndex.getRange().length);
    assert.equal(0, rowIdIndex.getRange().length);
  });

  it('insert_UniqueKeyViolation_IndexOrder', () => {
    const table = env.schema.table('tableE') as BaseTable;
    const pkIndexSchema = table.getIndices()[0] as IndexImpl;
    assert.isTrue(pkIndexSchema.isUnique);
    const emailIndexSchema = table.getIndices()[1] as IndexImpl;
    assert.isTrue(emailIndexSchema.isUnique);

    const pkIndex = env.indexStore.get(
      pkIndexSchema.getNormalizedName()
    ) as RuntimeIndex;
    const emailIndex = env.indexStore.get(
      emailIndexSchema.getNormalizedName()
    ) as RuntimeIndex;
    assert.equal(0, pkIndex.getRange().length);
    assert.equal(0, emailIndex.getRange().length);

    // Test case where the 'id' column uniqueness is violated.
    let journal = createJournal([table]);
    TestUtil.assertThrowsError(
      ErrorCode.DUPLICATE_KEYS, // Duplicate keys are not allowed.
      () => {
        const row1 = table.createRow({id: 'samePk', email: 'email1'});
        const row2 = table.createRow({id: 'samePk', email: 'email2'});
        journal.insert(table, [row1, row2]);
      }
    );

    // Ensure that after rollback both indices are empty.
    journal.rollback();
    assert.equal(0, pkIndex.getRange().length);
    assert.equal(0, emailIndex.getRange().length);

    // Test case where the 'email' column uniqueness is violated.
    journal = createJournal([table]);
    TestUtil.assertThrowsError(
      ErrorCode.DUPLICATE_KEYS, // Duplicate keys are not allowed.
      () => {
        const row1 = table.createRow({id: 'pk1', email: 'sameEmail'});
        const row2 = table.createRow({id: 'pk2', email: 'sameEmail'});
        journal.insert(table, [row1, row2]);
      }
    );

    // Ensure that none of the indices is affected.
    journal.rollback();
    assert.equal(0, pkIndex.getRange().length);
    assert.equal(0, emailIndex.getRange().length);
  });

  // Tests the case where a unique key violation occurs because a row with the
  // same single-column unique key already exists via a previous committed
  // journal.
  it('insert_UniqueKeyViolation_SingleColumn', () => {
    const table = env.schema.table('tableE') as BaseTable;
    const emailIndexSchema = table.getIndices()[1] as IndexImpl;
    assert.isTrue(emailIndexSchema.isUnique);
    const emailIndex = env.indexStore.get(
      emailIndexSchema.getNormalizedName()
    ) as RuntimeIndex;
    const rowIdIndex = env.indexStore.get(
      table.getRowIdIndexName()
    ) as RuntimeIndex;

    let journal = createJournal([table]);
    const row1 = table.createRow({id: 'pk1', email: 'emailAddress1'});
    journal.insert(table, [row1]);
    journal.commit();
    assert.isTrue(emailIndex.containsKey(row1.payload()['email'] as Key));
    assert.isTrue(rowIdIndex.containsKey(row1.id()));
    assert.equal(1, env.cache.getCount());

    journal = createJournal([table]);
    const row2 = table.createRow({id: 'pk2', email: 'emailAddress1'});
    TestUtil.assertThrowsError(
      ErrorCode.DUPLICATE_KEYS, // Duplicate keys are not allowed.
      journal.insert.bind(journal, table, [row2])
    );
    journal.rollback();

    assert.equal(row1.id(), emailIndex.get(row1.payload()['email'] as Key)[0]);
    assert.isTrue(rowIdIndex.containsKey(row1.id()));
    assert.equal(1, env.cache.getCount());
  });

  // Tests the case where a unique key violation occurs because a row with the
  // same cross-column unique key already exists via a previous committed
  // journal.
  it('insert_UniqueKeyViolation_CrossColumn', () => {
    const table = env.schema.table('tableD') as BaseTable;
    const row = table.createRow({
      firstName: 'DummyFirstName',
      id1: 'id_0',
      id2: 'id_1',
      lastName: 'DummyLastName',
    });

    const otherRow = table.createRow({
      firstName: 'DummyFirstName',
      id1: 'id_2',
      id2: 'id_3',
      lastName: 'DummyLastName',
    });

    checkInsert_UniqueConstraintViolation(table, [row], [otherRow]);
  });

  // Tests that not-nullable constraint checks are happening within
  // Journal#insert.
  it('insert_NotNullableKeyViolation', () => {
    const table = env.schema.table('tableE') as BaseTable;
    assert.equal(0, env.cache.getCount());

    const journal = createJournal([table]);
    const row1 = table.createRow({id: 'pk1', email: null});
    TestUtil.assertThrowsError(
      // Attempted to insert NULL value to non-nullable field {0}.
      ErrorCode.NOT_NULLABLE,
      journal.insert.bind(journal, table, [row1])
    );
    journal.rollback();
    assert.equal(0, env.cache.getCount());
  });

  // Tests that not-nullable constraint checks are happening within
  // Journal#insertOrReplace.
  it('insertOrReplace_NotNullableKeyViolation', () => {
    const table = env.schema.table('tableE') as BaseTable;
    assert.equal(0, env.cache.getCount());

    // Attempting to insert a new invalid row.
    let journal = createJournal([table]);
    const row1 = table.createRow({id: 'pk1', email: null});
    TestUtil.assertThrowsError(
      // Attempted to insert NULL value to non-nullable field {0}.
      ErrorCode.NOT_NULLABLE,
      journal.insertOrReplace.bind(journal, table, [row1])
    );
    journal.rollback();
    assert.equal(0, env.cache.getCount());

    // Attempting to insert a new valid row.
    const row2 = table.createRow({id: 'pk2', email: 'emailAddress'});
    journal = createJournal([table]);
    journal.insertOrReplace(table, [row2]);
    assert.equal(1, env.cache.getCount());

    // Attempting to replace existing row with an invalid one.
    const row2Updated = table.createRow({id: 'pk2', email: null});
    journal = createJournal([table]);
    TestUtil.assertThrowsError(
      // Attempted to insert NULL value to non-nullable field {0}.
      ErrorCode.NOT_NULLABLE,
      journal.insertOrReplace.bind(journal, table, [row2Updated])
    );
    journal.rollback();
  });

  // Tests that not-nullable constraint checks are happening within
  // Journal#update.
  it('update_NotNullableKeyViolation', () => {
    const table = env.schema.table('tableE') as BaseTable;
    assert.equal(0, env.cache.getCount());

    const row = table.createRow({id: 'pk1', email: 'emailAddress'});
    let journal = createJournal([table]);
    journal.insert(table, [row]);
    assert.equal(1, env.cache.getCount());

    journal = createJournal([table]);
    const rowUpdatedInvalid = table.createRow({id: 'pk1', email: null});
    rowUpdatedInvalid.assignRowId(row.id());
    TestUtil.assertThrowsError(
      // Attempted to insert NULL value to non-nullable field {0}.
      ErrorCode.NOT_NULLABLE,
      journal.update.bind(journal, table, [rowUpdatedInvalid])
    );
    journal.rollback();

    journal = createJournal([table]);
    const rowUpdatedValid = table.createRow({
      id: 'pk1',
      email: 'otherEmailAddress',
    });
    rowUpdatedValid.assignRowId(row.id());
    assert.doesNotThrow(() => journal.update(table, [rowUpdatedValid]));
  });

  // Tests that update() succeeds if there is no primary/unique key violation.
  it('update_NoPrimaryKeyViolation', () => {
    const table = env.schema.table('tableA') as BaseTable;
    const pkIndexSchema = table.getConstraint().getPrimaryKey();
    const pkIndex = env.indexStore.get(
      pkIndexSchema.getNormalizedName()
    ) as RuntimeIndex;
    const rowIdIndex = env.indexStore.get(
      table.getRowIdIndexName()
    ) as RuntimeIndex;

    const row = table.createRow({id: 'pk1', name: 'DummyName'});
    let journal = createJournal([table]);
    journal.insert(table, [row]);
    journal.commit();
    assert.equal(1, env.cache.getCount());
    assert.isTrue(pkIndex.containsKey(row.payload()['id'] as Key));
    assert.isTrue(rowIdIndex.containsKey(row.id()));

    // Attempting to update a column that is not the primary key.
    const rowUpdated1 = table.createRow({
      id: row.payload()['id'],
      name: 'OtherDummyName',
    });
    rowUpdated1.assignRowId(row.id());

    journal = createJournal([table]);
    journal.update(table, [rowUpdated1]);
    journal.commit();

    // Attempting to update the primary key column.
    const rowUpdated2 = table.createRow({
      id: 'otherPk',
      name: 'OtherDummyName',
    });
    rowUpdated2.assignRowId(row.id());
    journal = createJournal([table]);
    journal.update(table, [rowUpdated2]);
    journal.commit();

    assert.isFalse(pkIndex.containsKey(row.payload()['id'] as Key));
    assert.isTrue(pkIndex.containsKey(rowUpdated2.payload()['id'] as Key));
  });

  // Tests the case where a row is updated to have a primary key that already
  // exists from a previously committed journal.
  it('update_PrimaryKeyViolation1', () => {
    const table = env.schema.table('tableA') as BaseTable;
    const pkIndexSchema = table.getConstraint().getPrimaryKey();
    const pkIndex = env.indexStore.get(
      pkIndexSchema.getNormalizedName()
    ) as RuntimeIndex;

    const row1 = table.createRow({id: 'pk1', name: 'DummyName'});
    const row2 = table.createRow({id: 'pk2', name: 'DummyName'});

    let journal = createJournal([table]);
    journal.insert(table, [row1, row2]);
    journal.commit();
    assert.equal(2, env.cache.getCount());
    assert.isTrue(pkIndex.containsKey(row1.payload()['id'] as Key));
    assert.isTrue(pkIndex.containsKey(row2.payload()['id'] as Key));

    // Attempting to update row2 to have the same primary key as row1.
    const row2Updated = table.createRow({
      id: row1.payload()['id'],
      name: 'OtherDummyName',
    });
    row2Updated.assignRowId(row2.id());

    journal = createJournal([table]);
    TestUtil.assertThrowsError(
      ErrorCode.DUPLICATE_KEYS, // Duplicate keys are not allowed.
      journal.update.bind(journal, table, [row2Updated])
    );
    journal.rollback();
  });

  // Tests the case where a row is updated to have a primary key that already
  // exists from a row that has been added within the same journal.
  it('update_PrimaryKeyViolation2', () => {
    const table = env.schema.table('tableA') as BaseTable;
    const pkIndexSchema = table.getConstraint().getPrimaryKey();
    const pkIndex = env.indexStore.get(
      pkIndexSchema.getNormalizedName()
    ) as RuntimeIndex;

    const row1 = table.createRow({id: 'pk1', name: 'DummyName'});
    const row2 = table.createRow({id: 'pk2', name: 'DummyName'});

    const journal = createJournal([table]);
    journal.insert(table, [row1, row2]);
    assert.equal(2, env.cache.getCount());
    assert.isTrue(pkIndex.containsKey(row1.payload()['id'] as Key));
    assert.isTrue(pkIndex.containsKey(row2.payload()['id'] as Key));

    // Attempting to update row2 to have the same primary key as row1.
    const row2Updated = table.createRow({
      id: row1.payload()['id'],
      name: 'OtherDummyName',
    });
    row2Updated.assignRowId(row2.id());
    TestUtil.assertThrowsError(
      ErrorCode.DUPLICATE_KEYS, // Duplicate keys are not allowed.
      journal.update.bind(journal, table, [row2Updated])
    );
    journal.rollback();
  });

  // Tests the case where multiple rows are updated to have the same primary key
  // and that primary key does not already exist.
  it('update_PrimaryKeyViolation3', () => {
    const table = env.schema.table('tableA') as BaseTable;
    const pkIndexSchema = table.getConstraint().getPrimaryKey();
    const pkIndex = env.indexStore.get(
      pkIndexSchema.getNormalizedName()
    ) as RuntimeIndex;

    const rows: Row[] = [];
    for (let i = 0; i < 3; i++) {
      rows.push(table.createRow({id: `pk${i}`, name: 'DummyName'}));
    }

    let journal = createJournal([table]);
    journal.insert(table, rows);
    journal.commit();
    assert.equal(rows.length, env.cache.getCount());
    rows.forEach(row =>
      assert.isTrue(pkIndex.containsKey(row.payload()['id'] as Key))
    );

    const rowsUpdated = rows.map(row => {
      const updatedRow = table.createRow({id: 'somePk', name: 'DummyName'});
      updatedRow.assignRowId(row.id());
      return updatedRow;
    });

    journal = createJournal([table]);
    TestUtil.assertThrowsError(
      ErrorCode.DUPLICATE_KEYS, // Duplicate keys are not allowed.
      journal.update.bind(journal, table, rowsUpdated)
    );
    journal.rollback();
  });

  // Tests the cases where a nullable column is updated to null from non-null
  // and vice versa.
  it('update_updateNullsCorrectly', () => {
    const table = env.schema.table('tableI') as BaseTable;
    const nameIndex = env.indexStore.get('tableI.idxName') as RuntimeIndex;
    assert.equal(0, nameIndex.getRange().length);

    // Inserting a new row with a null key.
    const row = table.createRow({id: 'pk0', id2: 'id2x', name: null});
    let journal = createJournal([table]);
    journal.insert(table, [row]);
    journal.commit();
    assert.isTrue(nameIndex.containsKey(NULL));
    assert.equal(1, nameIndex.getRange().length);

    // Updating an existing row, and replacing the null key with a non-null.
    let updatedRow = table.createRow({
      id: 'pk0',
      id2: 'id2x',
      name: 'sampleName',
    });
    updatedRow.assignRowId(row.id());
    journal = createJournal([table]);
    journal.update(table, [updatedRow]);
    journal.commit();
    assert.isFalse(nameIndex.containsKey(NULL));
    assert.equal(1, nameIndex.getRange().length);

    // Updating an existing row replacing a non-null key with null.
    updatedRow = table.createRow({id: 'pk0', id2: 'id2x', name: null});
    updatedRow.assignRowId(row.id());
    journal = createJournal([table]);
    journal.update(table, [updatedRow]);
    journal.commit();
    assert.isTrue(nameIndex.containsKey(NULL));
    assert.equal(1, nameIndex.getRange().length);

    // Removing an existing row.
    journal = createJournal([table]);
    journal.remove(table, [updatedRow]);
    assert.equal(0, nameIndex.getRange().length);
  });

  // Tests the case where a row that has been deleted previously within the same
  // uncommitted journal is inserted.
  it('deleteInsert_Uncommitted', () => {
    const table = env.schema.table('tableA') as BaseTable;
    const pkIndexSchema = table.getConstraint().getPrimaryKey();
    const pkIndex = env.indexStore.get(
      pkIndexSchema.getNormalizedName()
    ) as RuntimeIndex;
    const rowIdIndex = env.indexStore.get(
      table.getRowIdIndexName()
    ) as RuntimeIndex;

    const primaryKey = '100';
    const row1 = table.createRow({id: primaryKey, name: 'DummyName'});

    // First adding the row and committing.
    let journal = createJournal([table]);
    journal.insert(table, [row1]);
    journal.commit();

    // Removing the row on a new journal.
    journal = createJournal([table]);
    journal.remove(table, [row1]);

    // Inserting a row that has the primary key that was just removed within the
    // same journal.
    const row2 = table.createRow({id: primaryKey, name: 'DummyName'});
    journal.insert(table, [row2]);
    journal.commit();

    assert.equal(1, env.cache.getCount());
    assert.isTrue(pkIndex.containsKey(primaryKey));
    assert.isTrue(rowIdIndex.containsKey(row2.id()));
  });

  it('insertOrReplace', () => {
    const table = env.schema.table('tableA') as BaseTable;
    const pkIndexSchema = table.getConstraint().getPrimaryKey();
    const pkIndex = env.indexStore.get(
      pkIndexSchema.getNormalizedName()
    ) as RuntimeIndex;
    const rowIdIndex = env.indexStore.get(
      table.getRowIdIndexName()
    ) as RuntimeIndex;

    const primaryKey = '100';
    const row1 = table.createRow({id: primaryKey, name: 'DummyName'});

    // First testing case where the row does not already exist.
    assert.equal(0, env.cache.getCount());
    assert.isFalse(pkIndex.containsKey(primaryKey));
    assert.isFalse(rowIdIndex.containsKey(row1.id()));

    let journal = createJournal([table]);
    journal.insertOrReplace(table, [row1]);
    journal.commit();

    assert.isTrue(pkIndex.containsKey(primaryKey));
    assert.isTrue(rowIdIndex.containsKey(row1.id()));
    assert.equal(1, env.cache.getCount());

    // Now testing case where the row is being replaced. There should be no
    // exception thrown.
    const row2 = table.createRow({id: primaryKey, name: 'OtherDummyName'});
    journal = createJournal([table]);
    journal.insertOrReplace(table, [row2]);
    journal.commit();

    assert.equal(1, env.cache.getCount());
    assert.isTrue(pkIndex.containsKey(primaryKey));
    // Expecting the previous row ID to have been preserved since a row with
    // the same primaryKey was already existing.
    assert.isTrue(rowIdIndex.containsKey(row1.id()));
  });

  it('insertOrReplace_UniqueKeyViolation', () => {
    const table = env.schema.table('tableE') as BaseTable;
    const emailIndexSchema = table.getIndices()[1] as IndexImpl;
    assert.isTrue(emailIndexSchema.isUnique);
    const emailIndex = env.indexStore.get(
      emailIndexSchema.getNormalizedName()
    ) as RuntimeIndex;

    const row1 = table.createRow({id: 'pk1', email: 'emailAddress1'});
    const row2 = table.createRow({id: 'pk2', email: 'emailAddress2'});

    let journal = createJournal([table]);
    journal.insertOrReplace(table, [row1, row2]);
    journal.commit();
    assert.isTrue(emailIndex.containsKey(row1.payload()['email'] as Key));
    assert.isTrue(emailIndex.containsKey(row2.payload()['email'] as Key));

    // Attempting to insert a new row that has the same 'email' field as an
    // existing row.
    const row3 = table.createRow({id: 'pk3', email: row1.payload()['email']});
    journal = createJournal([table]);
    TestUtil.assertThrowsError(
      ErrorCode.DUPLICATE_KEYS, // Duplicate keys are not allowed.
      journal.insertOrReplace.bind(journal, table, [row3])
    );
    journal.rollback();
    assert.equal(row1.id(), emailIndex.get(row3.payload()['email'] as Key)[0]);

    // Attempting to insert two new rows, that have the same 'email' field with
    // each other, and also it is not occupied by any existing row.
    const row4 = table.createRow({id: 'pk4', email: 'otherEmailAddress'});
    const row5 = table.createRow({id: 'pk5', email: 'otherEmailAddress'});
    journal = createJournal([table]);
    TestUtil.assertThrowsError(
      ErrorCode.DUPLICATE_KEYS, // Duplicate keys are not allowed.
      journal.insertOrReplace.bind(journal, table, [row4, row5])
    );
    journal.rollback();
    assert.isFalse(emailIndex.containsKey(row4.payload()['email'] as Key));
    assert.isFalse(emailIndex.containsKey(row5.payload()['email'] as Key));

    // Attempting to update existing row1 to have the same 'email' as existing
    // row2.
    const row1Updated = table.createRow({
      email: row2.payload()['email'],
      id: row1.payload()['id'],
    });
    row1Updated.assignRowId(row1.id());
    journal = createJournal([table]);
    TestUtil.assertThrowsError(
      ErrorCode.DUPLICATE_KEYS, // Duplicate keys are not allowed.
      journal.insertOrReplace.bind(journal, table, [row1Updated])
    );
    journal.rollback();
    assert.equal(row1.id(), emailIndex.get(row1.payload()['email'] as Key)[0]);
    assert.equal(row2.id(), emailIndex.get(row2.payload()['email'] as Key)[0]);

    // Finally attempting to update existing row1 to have a new unused 'email'
    // field.
    const row2Updated = table.createRow({
      email: 'unusedEmailAddress',
      id: row2.payload()['id'],
    });
    row2Updated.assignRowId(row2.id());
    journal = createJournal([table]);
    assert.doesNotThrow(() => journal.insertOrReplace(table, [row2Updated]));
    assert.equal(2, env.cache.getCount());
    assert.equal(row1.id(), emailIndex.get(row1.payload()['email'] as Key)[0]);
    assert.equal(
      row2.id(),
      emailIndex.get(row2Updated.payload()['email'] as Key)[0]
    );
  });

  it('cacheMerge', () => {
    // Selecting a table without any user-defined index (no primary key either).
    const table = env.schema.table('tableC') as BaseTable;
    const rowIdIndex = env.indexStore.get(
      table.getRowIdIndexName()
    ) as RuntimeIndex;
    const payload = {id: 'something', name: 'dummyName'};

    assert.equal(0, env.cache.getCount());
    const row = new Row(1, payload);
    const row2 = new Row(4, payload);
    let journal = createJournal([table]);
    journal.insert(table, [row, row2]);
    assert.equal(2, env.cache.getCount());
    let results: Row[] = env.cache.getMany([0, 1, 4]) as Row[];
    assert.isNull(results[0]);
    assert.deepEqual(payload, results[1].payload());
    assert.deepEqual(payload, results[2].payload());
    assert.isTrue(rowIdIndex.containsKey(row.id()));
    assert.isTrue(rowIdIndex.containsKey(row2.id()));

    journal = createJournal([table]);
    const payload2 = {id: 'nothing', name: 'dummyName'};
    const row3 = new Row(0, payload2);
    const row4 = new Row(4, payload2);
    journal.insert(table, [row3]);
    journal.update(table, [row4]);
    journal.remove(table, [row]);
    journal.commit();

    assert.isTrue(rowIdIndex.containsKey(row3.id()));
    assert.isTrue(rowIdIndex.containsKey(row4.id()));
    assert.isFalse(rowIdIndex.containsKey(row.id()));

    assert.equal(2, env.cache.getCount());
    results = env.cache.getMany([0, 1, 4]) as Row[];
    assert.deepEqual(payload2, results[0].payload());
    assert.isNull(results[1]);
    assert.deepEqual(payload2, results[2].payload());
  });

  // Tests that the indices and the cache are updated accordingly when insert,
  // remove, update, insertOrReplace are called, even before the journal is
  // committed.
  it('indicesCacheUpdated', () => {
    const table = env.schema.table('tableB') as BaseTable;
    const indices = table.getIndices();
    const journal = createJournal([table]);

    const row1 = table.createRow({id: '1', name: '1'});
    row1.assignRowId(1);
    const row2 = table.createRow({id: '2', name: '2'});
    row2.assignRowId(2);
    const row3 = table.createRow({id: '3', name: '2'});
    row3.assignRowId(3);
    const row4 = table.createRow({id: '4', name: '1'});
    row4.assignRowId(1);
    const row5 = table.createRow({id: '4', name: '4'});
    row5.assignRowId(1);

    const pkId = env.indexStore.get(
      indices[0].getNormalizedName()
    ) as RuntimeIndex;
    const idxName = env.indexStore.get(
      indices[1].getNormalizedName()
    ) as RuntimeIndex;
    const rowIdIndex = env.indexStore.get(
      table.getRowIdIndexName()
    ) as RuntimeIndex;

    assert.isFalse(rowIdIndex.containsKey(1));
    assert.isFalse(rowIdIndex.containsKey(2));
    assert.isFalse(rowIdIndex.containsKey(3));
    assert.isFalse(pkId.containsKey('1'));
    assert.isFalse(pkId.containsKey('2'));
    assert.isFalse(pkId.containsKey('3'));
    assert.isFalse(pkId.containsKey('4'));
    assert.isFalse(idxName.containsKey('1'));
    assert.isFalse(idxName.containsKey('2'));

    journal.insert(table, [row1, row2, row3]);
    assert.sameDeepMembers([row1, row2, row3], getTableRows(table));

    journal.remove(table, [row2]);
    assert.sameDeepMembers([row1, row3], getTableRows(table));

    journal.update(table, [row4]);
    assert.sameDeepMembers([row4, row3], getTableRows(table));

    journal.insertOrReplace(table, [row5]);
    assert.sameDeepMembers([row5, row3], getTableRows(table));

    const assertIndicesUpdated = () => {
      assert.isTrue(rowIdIndex.containsKey(1));
      assert.isFalse(rowIdIndex.containsKey(2));
      assert.isTrue(rowIdIndex.containsKey(3));
      assert.isFalse(pkId.containsKey('1'));
      assert.isFalse(pkId.containsKey('2'));
      assert.sameDeepMembers(
        [3, 1],
        pkId.getRange([new SingleKeyRange('3', '4', false, false)])
      );
      assert.sameDeepMembers([], idxName.get('1'));
      assert.sameDeepMembers([3], idxName.get('2'));
      assert.sameDeepMembers([1], idxName.get('4'));
    };

    assertIndicesUpdated();
    journal.commit();
    assertIndicesUpdated();
  });

  it('indicesUpdated', () => {
    const table = env.schema.table('tableA') as BaseTable;
    const journal = createJournal([table]);
    const indexSchema = table.getIndices()[1];
    const keyRange1 = new SingleKeyRange('aaa', 'bbb', false, false);
    const keyRange2 = new SingleKeyRange('ccc', 'eee', false, false);

    const row1 = table.createRow({id: 'dummyId1', name: 'aba'});
    const row2 = table.createRow({id: 'dummyId2', name: 'cdc'});
    const row3 = table.createRow({id: 'dummyId3', name: 'abb'});

    // Adding row3 in the index such that it is within the range [aaa,bbb].
    journal.insert(table, [row3]);
    const index = env.indexStore.get(
      indexSchema.getNormalizedName()
    ) as RuntimeIndex;
    let rowIds = index.getRange([keyRange1]);
    assert.sameDeepMembers([row3.id()], rowIds);

    // Checking that the Journal returns row3 as a match, given that row3 has
    // not been modified within the journal itself yet.
    rowIds = index.getRange([keyRange1, keyRange2]);
    assert.sameDeepMembers([row3.id()], rowIds);

    // Inserting new rows within this journal, where row1 and row2 are within
    // the specified range, and modifying row3 such that it is not within range
    // anymore.
    const row3Updated = table.createRow({id: 'dummyId3', name: 'bbba'});
    journal.insertOrReplace(table, [row1, row2, row3Updated]);
    rowIds = index.getRange([keyRange1, keyRange2]);
    assert.sameDeepMembers([row1.id(), row2.id()], rowIds);
  });

  // Tests rolling back a journal.
  it('rollback', () => {
    const table = env.schema.table('tableA') as BaseTable;

    const rowToInsert = table.createRow({id: 'add', name: 'DummyName'});
    const rowToModifyOld = table.createRow({id: 'modify', name: 'DummyName'});
    const rowToModifyNew = table.createRow({
      id: 'modify',
      name: 'UpdatedDummyName',
    });
    rowToModifyNew.assignRowId(rowToModifyOld.id());
    const rowToRemove = table.createRow({id: 'delete', name: 'DummyName'});

    const pkIndexSchema = table.getConstraint().getPrimaryKey();
    const pkIndex = env.indexStore.get(
      pkIndexSchema.getNormalizedName()
    ) as RuntimeIndex;
    const rowIdIndex = env.indexStore.get(
      table.getRowIdIndexName()
    ) as RuntimeIndex;

    // Asserts the initial state of the cache and indices.
    const assertInitialState = () => {
      assert.equal(2, env.cache.getCount());

      assert.isTrue(pkIndex.containsKey(rowToModifyOld.payload()['id'] as Key));
      assert.isTrue(pkIndex.containsKey(rowToRemove.payload()['id'] as Key));
      assert.isFalse(pkIndex.containsKey(rowToInsert.payload()['id'] as Key));

      assert.isTrue(rowIdIndex.containsKey(rowToModifyOld.id()));
      assert.isTrue(rowIdIndex.containsKey(rowToRemove.id()));
      assert.isFalse(rowIdIndex.containsKey(rowToInsert.id()));

      const row = env.cache.get(rowToModifyOld.id()) as Row;
      assert.equal(rowToModifyOld.payload()['name'], row.payload()['name']);
    };

    // Setting up the cache and indices to be in the initial state.
    let journal = createJournal([table]);
    journal.insert(table, [rowToModifyOld]);
    journal.insert(table, [rowToRemove]);
    journal.commit();

    assertInitialState();

    // Modifying indices and cache.
    journal = createJournal([table]);
    journal.insert(table, [rowToInsert]);
    journal.update(table, [rowToModifyNew]);
    journal.remove(table, [rowToRemove]);

    // Rolling back the journal and asserting that indices and cache are in the
    // initial state again.
    journal.rollback();
    assertInitialState();
  });

  // Returns the rows that exist in the given table according to the indexStore
  // and the cache.
  function getTableRows(table: BaseTable): Row[] {
    const rowIds = (env.indexStore.get(
      table.getRowIdIndexName()
    ) as RuntimeIndex).getRange();
    return env.cache.getMany(rowIds) as Row[];
  }
});
