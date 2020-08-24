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
import {
  ConstraintAction,
  DataStoreType,
  ErrorCode,
  Type,
} from '../../lib/base/enum';
import {Row} from '../../lib/base/row';
import {Builder} from '../../lib/schema/builder';
import {schema} from '../../lib/schema/schema';
import {Table} from '../../lib/schema/table';

const assert = chai.assert;

interface SampleRows {
  tableA: Row[];
  tableB: Row[];
  tableB1: Row[];
  tableB2: Row[];
}

describe('EndToEndFKDeleteCascade', () => {
  let db: DatabaseConnection;
  let tA: Table;
  let tB: Table;
  let tB1: Table;
  let tB2: Table;
  let sampleRows: SampleRows;

  // Creates a schema that has both CASCADE and RESTRICT constraints as follows.
  //            TableA
  //              | Cascade
  //            TableB
  //    Cascade /    \ Restrict
  //           /      \
  //        TableB1  TableB2

  function getSchemaBuilder(): Builder {
    const schemaBuilder = schema.create('fk_schema', 1);
    schemaBuilder
      .createTable('TableA')
      .addColumn('id', Type.STRING)
      .addPrimaryKey(['id']);
    schemaBuilder
      .createTable('TableB')
      .addColumn('id', Type.STRING)
      .addColumn('foreignId', Type.STRING)
      .addPrimaryKey(['id'])
      .addForeignKey('fk_foreignId', {
        action: ConstraintAction.CASCADE,
        local: 'foreignId',
        ref: 'TableA.id',
      });
    schemaBuilder
      .createTable('TableB1')
      .addColumn('id', Type.STRING)
      .addColumn('foreignId', Type.STRING)
      .addPrimaryKey(['id'])
      .addForeignKey('fk_foreignId', {
        action: ConstraintAction.CASCADE,
        local: 'foreignId',
        ref: 'TableB.id',
      });
    schemaBuilder
      .createTable('TableB2')
      .addColumn('id', Type.STRING)
      .addColumn('foreignId', Type.STRING)
      .addPrimaryKey(['id'])
      .addForeignKey('fk_foreignId', {
        action: ConstraintAction.RESTRICT,
        local: 'foreignId',
        ref: 'TableB.id',
      });
    return schemaBuilder;
  }

  // Generates one row for each table.
  function getSampleRows(): SampleRows {
    return {
      tableA: [tA.createRow({id: 'tableAId'})],
      tableB: [tB.createRow({id: 'tableBId', foreignId: 'tableAId'})],
      tableB1: [tB1.createRow({id: 'tableB1Id', foreignId: 'tableBId'})],
      tableB2: [tB2.createRow({id: 'tableB2Id', foreignId: 'tableBId'})],
    };
  }

  beforeEach(async () => {
    db = await getSchemaBuilder().connect({storeType: DataStoreType.MEMORY});
    const schema = db.getSchema();
    tA = schema.table('TableA');
    tB = schema.table('TableB');
    tB1 = schema.table('TableB1');
    tB2 = schema.table('TableB2');
    sampleRows = getSampleRows();
  });

  afterEach(() => db.close());

  // Tests a simple case where a deletion on TableA cascades to TableB and
  // TableB1.
  it('cascadeOnlySuccess', async () => {
    let tx = db.createTransaction();
    await tx.exec([
      db.insert().into(tA).values(sampleRows.tableA),
      db.insert().into(tB).values(sampleRows.tableB),
      db.insert().into(tB1).values(sampleRows.tableB1),
    ]);

    await db.delete().from(tA).exec();

    tx = db.createTransaction();
    const results = (await tx.exec([
      db.select().from(tA),
      db.select().from(tB),
      db.select().from(tB1),
    ])) as unknown[][];

    assert.equal(0, results[0].length);
    assert.equal(0, results[1].length);
    assert.equal(0, results[2].length);
  });

  // Test the case where a deletion on TableA, cascades to TableB and TableB1,
  // but because TableB2 refers to TableB with a RESTRICT constraint, the entire
  // operation is rejected.
  it('runCascadeRestrictFail', async () => {
    let tx = db.createTransaction();
    await tx.exec([
      db.insert().into(tA).values(sampleRows.tableA),
      db.insert().into(tB).values(sampleRows.tableB),
      db.insert().into(tB1).values(sampleRows.tableB1),
      db.insert().into(tB2).values(sampleRows.tableB2),
    ]);

    let failed = true;
    try {
      await db.delete().from(tA).exec();
    } catch (e) {
      // 203: Foreign key constraint violation on constraint {0}.
      assert.equal(ErrorCode.FK_VIOLATION, e.code);

      tx = db.createTransaction();
      const res = (await tx.exec([
        db.select().from(tA),
        db.select().from(tB),
        db.select().from(tB1),
        db.select().from(tB2),
      ])) as unknown[][];

      assert.equal(1, res[0].length);
      assert.equal(1, res[1].length);
      assert.equal(1, res[2].length);
      assert.equal(1, res[3].length);
      failed = false;
    }
    assert.isFalse(failed);
  });
});
