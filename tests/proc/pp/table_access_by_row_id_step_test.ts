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

import {NoOpStep} from '../../../lib/proc/pp/no_op_step';
import {TableAccessByRowIdStep} from '../../../lib/proc/pp/table_access_by_row_id_step';
import {Relation} from '../../../lib/proc/relation';
import {BaseTable} from '../../../lib/schema/base_table';
import {DatabaseSchema} from '../../../lib/schema/database_schema';
import {Table} from '../../../lib/schema/table';
import {MockEnv} from '../../../testing/mock_env';
import {getMockSchemaBuilder} from '../../../testing/mock_schema_builder';

const assert = chai.assert;

describe('TableAccessByRowId', () => {
  let schema: DatabaseSchema;
  let env: MockEnv;

  beforeEach(() => {
    schema = getMockSchemaBuilder().getSchema();
    env = new MockEnv(schema);
    return env.init().then(() => env.addSampleData());
  });

  it('tableAccessByRowId', () => {
    return checkTableAccessByRowId(schema.table('tableA'));
  });

  it('tableAccessByRowId_Alias', () => {
    return checkTableAccessByRowId(schema.table('tableA').as('SomeTableAlias'));
  });

  // Checks that a TableAccessByRowIdStep that refers to the given table
  // produces the expected results.
  function checkTableAccessByRowId(t: Table): Promise<void> {
    const table = t as BaseTable;
    const step = new TableAccessByRowIdStep(env.global, table);

    // Creating a "dummy" child step that will return only two row IDs.
    const rows = [
      table.createRow({id: 1, name: 'a'}),
      table.createRow({id: 2, name: 'b'}),
    ];
    rows[0].assignRowId(0);
    rows[1].assignRowId(1);
    step.addChild(new NoOpStep([Relation.fromRows(rows, [table.getName()])]));

    return step.exec().then((relations) => {
      const relation = relations[0];
      assert.isFalse(relation.isPrefixApplied());
      assert.sameDeepOrderedMembers(
          [table.getEffectiveName()], relation.getTables());

      assert.equal(rows.length, relation.entries.length);
      relation.entries.forEach((entry, index) => {
        const rowId = rows[index].id();
        assert.equal(rowId, entry.row.id());
        assert.equal('dummyName' + rowId, entry.row.payload()['name']);
      });
    });
  }

  it('tableAccessByRowId_Empty', () => {
    const table = schema.table('tableB');
    const step = new TableAccessByRowIdStep(env.global, table);

    // Creating a "dummy" child step that will not return any row IDs.
    step.addChild(new NoOpStep([Relation.createEmpty()]));

    return step.exec().then((relations) => {
      assert.equal(0, relations[0].entries.length);
    });
  });
});
