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
  ConstraintTiming,
  DataStoreType,
  ErrorCode,
  Type,
} from '../../lib/base/enum';
import {Global} from '../../lib/base/global';
import {Row} from '../../lib/base/row';
import {RuntimeDatabase} from '../../lib/proc/runtime_database';
import {Builder} from '../../lib/schema/builder';
import {Table} from '../../lib/schema/table';
import {TestUtil} from '../../testing/test_util';

const assert = chai.assert;

describe('EndToEndForeignKey', () => {
  let db: RuntimeDatabase;
  let global: Global;
  let sampleRows: Row[];
  let parentTable: Table;
  let childTable: Table;

  beforeEach(async () => {
    const builder = getSchemaBuilder();
    db = (await builder.connect({
      storeType: DataStoreType.MEMORY,
    })) as RuntimeDatabase;
    global = builder.getGlobal();
    parentTable = db.getSchema().table('Parent');
    childTable = db.getSchema().table('Child');
    sampleRows = getSampleRows();
  });

  afterEach(() => db.close());

  function getSchemaBuilder(): Builder {
    const schemaBuilder = new Builder('fk_schema', 1);
    schemaBuilder
      .createTable('Parent')
      .addColumn('id', Type.STRING)
      .addColumn('name', Type.STRING)
      .addPrimaryKey(['id']);
    schemaBuilder
      .createTable('Child')
      .addColumn('id', Type.STRING)
      .addColumn('parentId', Type.STRING)
      .addPrimaryKey(['id'])
      .addNullable(['parentId'])
      .addForeignKey('fk_parentId', {
        local: 'parentId',
        ref: 'Parent.id',
        timing: ConstraintTiming.DEFERRABLE,
      });
    return schemaBuilder;
  }

  function getSampleRows(): Row[] {
    const parentRow = parentTable.createRow({
      id: 'parentId',
      name: 'parentName',
    });

    const childRow = childTable.createRow({
      id: 'childId',
      name: 'childName',
      parentId: 'parentId',
    });

    return [parentRow, childRow];
  }

  // Tests that a query that does not violate DEFERRABLE constraints completes
  // successfully when an implicit transaction is used.
  it('deferrable_ImplicitTx_Success', async () => {
    const parentRow = sampleRows[0];

    await db.insert().into(parentTable).values([parentRow]).exec();
    const results = await TestUtil.selectAll(global, parentTable);
    assert.equal(1, results.length);
    assert.equal(parentRow.payload()['id'], results[0].payload()['id']);
  });

  // Tests that a DEFERRABLE constraint violation results in the appropriate
  // error when an implicit transaction is used.
  it('deferrable_ImplicitTx_Error', () => {
    return TestUtil.assertPromiseReject(
      ErrorCode.FK_VIOLATION,
      db.insert().into(childTable).values([sampleRows[1]]).exec()
    );
  });

  // Tests that a child column value of null, does not trigger a foreign key
  // constraint violation, instead it is ignored.
  it('deferrable_ImplicitTx_IgnoreNull', async () => {
    const childRow = childTable.createRow({
      id: 'childId',
      name: 'childName',
      parentId: null,
    });

    await db.insert().into(childTable).values([childRow]).exec();
    const results = await TestUtil.selectAll(global, childTable);
    assert.equal(1, results.length);
    assert.isNull(results[0].payload()['parentId']);
  });

  // Tests that a DEFERRABLE constraint violation during insertion, results in
  // the appropriate error when an explicit transaction is used.
  it('deferrable_ExplicitTx_Insert_Error', async () => {
    const childRow = getSampleRows()[1];

    const tx = db.createTransaction();
    await tx.begin([childTable]);
    const q1 = db.insert().into(childTable).values([childRow]);
    await tx.attach(q1);
    await TestUtil.assertPromiseReject(ErrorCode.FK_VIOLATION, tx.commit());
  });

  // Tests that a DEFERRABLE constraint violation during deletion, results in
  // the appropriate error when an explicit transaction is used.
  it('deferrable_ExplicitTx_Delete_Error', async () => {
    const parentRow = sampleRows[0];
    const childRow = sampleRows[1];

    const tx1 = db.createTransaction();
    await tx1.exec([
      db.insert().into(parentTable).values([parentRow]),
      db.insert().into(childTable).values([childRow]),
    ]);

    const tx2 = db.createTransaction();
    await tx2.begin([parentTable, childTable]);

    // Deleting parent even though the child row refers to it.
    await tx2.attach(db.delete().from(parentTable));
    await TestUtil.assertPromiseReject(ErrorCode.FK_VIOLATION, tx2.commit());
  });

  // Tests that a DEFERRABLE constraint violation during updating, results in
  // the appropriate error when an explicit transaction is used.
  it('deferrable_ExplicitTx_Update_Error', async () => {
    const parentRow = sampleRows[0];
    const childRow = sampleRows[1];

    const tx1 = db.createTransaction();
    await tx1.exec([
      db.insert().into(parentTable).values([parentRow]),
      db.insert().into(childTable).values([childRow]),
    ]);

    const tx2 = db.createTransaction();
    await tx2.begin([parentTable, childTable]);

    // Updating child to point to a non existing parentId.
    const q = db
      .update(childTable)
      .set(childTable.col('parentId'), 'otherParentId');
    await tx2.attach(q);
    await TestUtil.assertPromiseReject(ErrorCode.FK_VIOLATION, tx2.commit());
  });

  // Tests that a DEFERRABLE constraint violation does NOT result in an error,
  // if the constraint is met by the time the transaction is committed.
  it('deferrable_ExplicitTx_Success', async () => {
    const parentRow = sampleRows[0];
    const childRow = sampleRows[1];

    const tx = db.createTransaction();
    await tx.begin([parentTable, childTable]);

    // Inserting child first, even though parent does not exist yet.
    const q1 = db.insert().into(childTable).values([childRow]);
    await tx.attach(q1);

    // Inserting parent after child has been inserted.
    const q2 = db.insert().into(parentTable).values([parentRow]);
    await tx.attach(q2);

    await tx.commit();
    let results = await TestUtil.selectAll(global, parentTable);
    assert.equal(1, results.length);
    assert.equal(parentRow.payload()['id'], results[0].payload()['id']);
    results = await TestUtil.selectAll(global, childTable);
    assert.equal(1, results.length);
    assert.equal(childRow.payload()['id'], results[0].payload()['id']);
  });
});
