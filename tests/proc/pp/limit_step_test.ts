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

import { Row } from '../../../lib/base/row';
import { LimitStep } from '../../../lib/proc/pp/limit_step';
import { NoOpStep } from '../../../lib/proc/pp/no_op_step';
import { Relation } from '../../../lib/proc/relation';
import { SelectContext } from '../../../lib/query/select_context';
import { DatabaseSchema } from '../../../lib/schema/database_schema';
import { MockEnv } from '../../../testing/mock_env';
import { getMockSchemaBuilder } from '../../../testing/mock_schema_builder';

const assert = chai.assert;

describe('LimitStep', () => {
  let env: MockEnv;
  let schema: DatabaseSchema;

  beforeEach(() => {
    schema = getMockSchemaBuilder().getSchema();
    env = new MockEnv(schema);
    return env.init();
  });

  it('exec_LimitLessThanResults', () => {
    return checkExec(/* sampleDataCount */ 20, /* limit */ 10);
  });

  it('exec_LimitMoreThanResults', () => {
    return checkExec(/* sampleDataCount */ 20, /* limit */ 100);
  });

  it('exec_LimitEqualToResults', () => {
    return checkExec(/* sampleDataCount */ 20, /* limit */ 20);
  });

  it('exec_LimitZero', () => {
    return checkExec(/* sampleDataCount */ 20, /* limit */ 0);
  });

  // Checks that the number of returned results is as expected.
  function checkExec(sampleDataCount: number, limit: number): Promise<void> {
    const rows = generateSampleRows(sampleDataCount);
    const tableName = 'dummyTable';
    const childStep = new NoOpStep([Relation.fromRows(rows, [tableName])]);

    const queryContext = new SelectContext(schema);
    queryContext.limit = limit;

    const step = new LimitStep();
    step.addChild(childStep);

    return step.exec(undefined, queryContext).then(relations => {
      assert.equal(
        Math.min(limit, sampleDataCount),
        relations[0].entries.length
      );
    });
  }

  function generateSampleRows(rowCount: number): Row[] {
    const rows: Row[] = new Array(rowCount);

    for (let i = 0; i < rowCount; i++) {
      rows[i] = Row.create({ id: `id${i}` });
    }

    return rows;
  }
});
