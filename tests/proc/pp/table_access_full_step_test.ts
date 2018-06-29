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

import {TableAccessFullStep} from '../../../lib/proc/pp/table_access_full_step';
import {Database} from '../../../lib/schema/database';
import {Table} from '../../../lib/schema/table';
import {MockEnv} from '../../../testing/mock_env';
import {getMockSchemaBuilder} from '../../../testing/mock_schema_builder';

const assert = chai.assert;

describe('TableAccessFullStep', () => {
  let schema: Database;
  let env: MockEnv;

  beforeEach(() => {
    schema = getMockSchemaBuilder().getSchema();
    env = new MockEnv(schema);
    return env.init().then(() => env.addSampleData());
  });

  it('tableAccessFullStep', () => {
    return checkTableAccessFullStep(schema.table('tableA'));
  });

  it('tableAccessFullStep_Alias', () => {
    return checkTableAccessFullStep(
        schema.table('tableA').as('SomeTableAlias'));
  });

  // Checks that a TableAccessByRowIdStep that refers to the given table
  // produces the expected results.
  function checkTableAccessFullStep(table: Table): Promise<void> {
    const step = new TableAccessFullStep(env.global, table);

    return step.exec().then((relations) => {
      const relation = relations[0];
      assert.isFalse(relation.isPrefixApplied());
      assert.sameDeepOrderedMembers(
          [table.getEffectiveName()], relation.getTables());
      assert.isTrue(relation.entries.length > 0);
    });
  }
});
