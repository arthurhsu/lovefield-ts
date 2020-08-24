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
import {ConstraintAction, DataStoreType, Type} from '../../lib/base/enum';
import {Row, PayloadType} from '../../lib/base/row';
import {op} from '../../lib/fn/op';
import {Builder} from '../../lib/schema/builder';
import {schema} from '../../lib/schema/schema';
import {Table} from '../../lib/schema/table';

const assert = chai.assert;

interface SampleRows {
  tableA: Row[];
  tableB: Row[];
}

describe('EndToEndFKUpdateCascade', () => {
  let db: DatabaseConnection;
  const options = {storeType: DataStoreType.MEMORY};

  afterEach(() => db.close());

  it('update1FK', async () => {
    db = await getSchemaBuilder1().connect(options);
    const schema = db.getSchema();
    const tA = schema.table('TableA');
    const tB = schema.table('TableB');
    const sampleRows = getSampleRows1(tA, tB);

    const updatedId = 'newTableAId0';

    let tx = db.createTransaction();
    await tx.exec([
      db.insert().into(tA).values(sampleRows.tableA),
      db.insert().into(tB).values(sampleRows.tableB),
    ]);
    await db
      .update(tA)
      .set(tA.col('id'), updatedId)
      .where(tA.col('id').eq(sampleRows.tableA[0].payload()['id'] as string))
      .exec();

    tx = db.createTransaction();
    const results = (await tx.exec([
      db.select().from(tA).where(tA.col('id').eq(updatedId)),
      db.select().from(tB).where(tB.col('foreignId').eq(updatedId)),
    ])) as unknown[][];

    assert.equal(1, results[0].length);
    assert.equal(2, results[1].length);
  });

  function getSchemaBuilder1(): Builder {
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
    return schemaBuilder;
  }

  // Generates two rows for TableA and four rows for TableB, where there are two
  // rows referring to each row in TableA.
  function getSampleRows1(tA: Table, tB: Table): SampleRows {
    const rows: SampleRows = {tableA: [], tableB: []};

    for (let i = 0; i < 2; i++) {
      rows.tableA.push(tA.createRow({id: `tableAId${i}`}));

      for (let j = 0; j < 2; j++) {
        rows.tableB.push(
          tB.createRow({
            foreignId: rows.tableA[i].payload()['id'],
            id: 'tableBId' + rows.tableB.length,
          })
        );
      }
    }
    return rows;
  }

  it('update2FK', async () => {
    db = await getSchemaBuilder2().connect(options);
    const dbSchema = db.getSchema();
    const tA = dbSchema.table('TableA');
    const tB = dbSchema.table('TableB');
    const sampleRows = getSampleRows2(tA, tB);

    const updatedId1 = 7;
    const updatedId2 = 8;

    let tx = db.createTransaction();
    await tx.exec([
      db.insert().into(tA).values(sampleRows.tableA),
      db.insert().into(tB).values(sampleRows.tableB),
    ]);

    await db
      .update(tA)
      .set(tA.col('id1'), updatedId1)
      .set(tA.col('id2'), updatedId2)
      .where(
        op.and(
          tA.col('id1').eq(sampleRows.tableA[0].payload()['id1'] as string),
          tA.col('id2').eq(sampleRows.tableA[0].payload()['id2'] as string)
        )
      )
      .exec();

    tx = db.createTransaction();
    const results = (await tx.exec([
      db
        .select()
        .from(tA)
        .where(
          op.and(tA.col('id1').eq(updatedId1), tA.col('id2').eq(updatedId2))
        ),
      db.select().from(tB).orderBy(tB.col('id')),
    ])) as unknown[][];

    assert.equal(1, results[0].length);
    assert.equal(3, results[1].length);
    assert.equal(updatedId1, (results[1][0] as PayloadType)['foreignId1']);
    assert.equal(updatedId2, (results[1][0] as PayloadType)['foreignId2']);
    assert.equal(updatedId2, (results[1][1] as PayloadType)['foreignId2']);
  });

  function getSchemaBuilder2(): Builder {
    const schemaBuilder = schema.create('fk_schema2', 1);
    schemaBuilder
      .createTable('TableA')
      .addColumn('id1', Type.INTEGER)
      .addUnique('uq_id1', ['id1'])
      .addColumn('id2', Type.INTEGER)
      .addUnique('uq_id2', ['id2']);
    schemaBuilder
      .createTable('TableB')
      .addColumn('id', Type.INTEGER)
      .addColumn('foreignId1', Type.INTEGER)
      .addColumn('foreignId2', Type.INTEGER)
      .addForeignKey('fk_foreignId1', {
        action: ConstraintAction.CASCADE,
        local: 'foreignId1',
        ref: 'TableA.id1',
      })
      .addForeignKey('fk_foreignId2', {
        action: ConstraintAction.CASCADE,
        local: 'foreignId2',
        ref: 'TableA.id2',
      });
    return schemaBuilder;
  }

  function getSampleRows2(tA: Table, tB: Table): SampleRows {
    return {
      tableA: [
        tA.createRow({id1: 1, id2: 4}),
        tA.createRow({id1: 2, id2: 5}),
        tA.createRow({id1: 3, id2: 6}),
      ],
      tableB: [
        tB.createRow({id: 0, foreignId1: 1, foreignId2: 4}),
        tB.createRow({id: 1, foreignId1: 2, foreignId2: 4}),
        tB.createRow({id: 2, foreignId1: 3, foreignId2: 6}),
      ],
    };
  }
});
