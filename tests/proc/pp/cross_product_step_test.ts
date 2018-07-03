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

import {Row} from '../../../lib/base/row';
import {CrossProductStep} from '../../../lib/proc/pp/cross_product_step';
import {NoOpStep} from '../../../lib/proc/pp/no_op_step';
import {Relation} from '../../../lib/proc/relation';
import {Database} from '../../../lib/schema/database';
import {MockEnv} from '../../../testing/mock_env';
import {getMockSchemaBuilder} from '../../../testing/mock_schema_builder';

const assert = chai.assert;

describe('CrossProductStep', () => {
  let schema: Database;
  let env: MockEnv;

  beforeEach(() => {
    schema = getMockSchemaBuilder().getSchema();
    env = new MockEnv(schema);
    return env.init();
  });

  // Tests that a cross product is calculated correctly.
  it('crossProduct', async () => {
    const leftRowCount = 3;
    const rightRowCount = 4;

    const leftRows: Row[] = new Array(leftRowCount);
    const leftTable = schema.table('tableA');
    for (let i = 0; i < leftRowCount; i++) {
      leftRows[i] = leftTable.createRow({id: `id${i}`, name: `name${i}`});
    }

    const rightRows: Row[] = new Array(rightRowCount);
    const rightTable = schema.table('tableE');
    for (let i = 0; i < rightRowCount; i++) {
      rightRows[i] = rightTable.createRow({id: `id${i}`, email: `email${i}`});
    }

    const leftChild =
        new NoOpStep([Relation.fromRows(leftRows, [leftTable.getName()])]);
    const rightChild =
        new NoOpStep([Relation.fromRows(rightRows, [rightTable.getName()])]);

    const step = new CrossProductStep();
    step.addChild(leftChild);
    step.addChild(rightChild);

    const relations = await step.exec();
    const relation = relations[0];
    const isDefAndNotNull = (a: any) => (a !== undefined && a !== null);
    assert.equal(leftRowCount * rightRowCount, relation.entries.length);
    relation.entries.forEach((entry) => {
      assert.isTrue(isDefAndNotNull(entry.getField(leftTable['id'])));
      assert.isTrue(isDefAndNotNull(entry.getField(leftTable['name'])));
      assert.isTrue(isDefAndNotNull(entry.getField(rightTable['id'])));
      assert.isTrue(isDefAndNotNull(entry.getField(rightTable['email'])));
    });
  });

  it('crossProduct_PreviousJoins', async () => {
    const relation1Count = 3;
    const relation2Count = 4;
    const relation3Count = 5;

    const relation1Rows: Row[] = [];
    const table1 = schema.table('tableA');
    for (let i = 0; i < relation1Count; i++) {
      const row = table1.createRow({id: `id${i}`, name: `name${i}`});
      relation1Rows.push(row);
    }

    const relation2Rows: Row[] = [];
    const table2 = schema.table('tableB');
    for (let i = 0; i < relation2Count; i++) {
      const row = table2.createRow({id: `id${i}`, name: `name${i}`});
      relation2Rows.push(row);
    }

    const relation3Rows: Row[] = [];
    const table3 = schema.table('tableE');
    for (let i = 0; i < relation3Count; i++) {
      const row = table3.createRow({id: `id${i}`, email: `email${i}`});
      relation3Rows.push(row);
    }

    const relation1 = Relation.fromRows(relation1Rows, [table1.getName()]);
    const relation2 = Relation.fromRows(relation2Rows, [table2.getName()]);
    const relation3 = Relation.fromRows(relation3Rows, [table3.getName()]);

    const relation1Step = new NoOpStep([relation1]);
    const relation2Step = new NoOpStep([relation2]);
    const relation3Step = new NoOpStep([relation3]);

    // Creating a tree structure composed of two cross product steps.
    const crossProductStep12 = new CrossProductStep();
    crossProductStep12.addChild(relation1Step);
    crossProductStep12.addChild(relation2Step);

    const crossProductStep123 = new CrossProductStep();
    crossProductStep123.addChild(crossProductStep12);
    crossProductStep123.addChild(relation3Step);

    const results = await crossProductStep123.exec();
    const result = results[0];

    // Expecting the final result to be a cross product of all 3 tables.
    assert.equal(
        relation1Count * relation2Count * relation3Count,
        result.entries.length);
    const isDefAndNotNull = (a: any) => (a !== undefined && a !== null);
    result.entries.forEach((entry) => {
      assert.isTrue(isDefAndNotNull(entry.getField(table1['id'])));
      assert.isTrue(isDefAndNotNull(entry.getField(table1['name'])));
      assert.isTrue(isDefAndNotNull(entry.getField(table2['id'])));
      assert.isTrue(isDefAndNotNull(entry.getField(table2['name'])));
      assert.isTrue(isDefAndNotNull(entry.getField(table3['id'])));
      assert.isTrue(isDefAndNotNull(entry.getField(table3['email'])));
    });
  });
});
