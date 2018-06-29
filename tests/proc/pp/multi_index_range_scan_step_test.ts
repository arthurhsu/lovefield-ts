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
import {SingleKeyRange} from '../../../lib/index/key_range';
import {IndexRangeScanStep} from '../../../lib/proc/pp/index_range_scan_step';
import {MultiIndexRangeScanStep} from '../../../lib/proc/pp/multi_index_range_scan_step';
import {Database} from '../../../lib/schema/database';
import {Index} from '../../../lib/schema/index';
import {MockEnv} from '../../../testing/mock_env';
import {MockKeyRangeCalculator} from '../../../testing/mock_key_range_calculator';
import {getMockSchemaBuilder} from '../../../testing/mock_schema_builder';

const assert = chai.assert;

describe('MultiIndexRangeScanStep', () => {
  let env: MockEnv;
  let schema: Database;

  beforeEach(() => {
    env = new MockEnv(getMockSchemaBuilder().getSchema());
    return env.init().then(() => {
      schema = env.schema;
      return env.addSampleData();
    });
  });

  it('empty', () => {
    const idKeyRange = new SingleKeyRange(20, 21, false, false);
    const nameKeyRange =
        new SingleKeyRange('dummyName' + 200, 'dummyName' + 205, false, false);
    return assertMultiIndexRangeScanResult(idKeyRange, nameKeyRange, []);
  });

  it('scan', () => {
    const idKeyRange = new SingleKeyRange(5, 8, false, false);
    const nameKeyRange =
        new SingleKeyRange('dummyName' + 3, 'dummyName' + 5, false, false);
    // Expecting the idKeyRange to find rows with rowIDs 5, 6, 7, 8.
    // Expecting the nameKeyRange to find rows with rowIDs 3, 4, 5.
    const expectedRowIds = [3, 4, 5, 6, 7, 8];

    return assertMultiIndexRangeScanResult(
        idKeyRange, nameKeyRange, expectedRowIds);
  });

  // Asserts that MultiIndexRangeScanStep#exec() correctly combines the results
  // of its children IndexRangeScanStep instances.
  function assertMultiIndexRangeScanResult(
      idKeyRange: SingleKeyRange, nameKeyRange: SingleKeyRange,
      expectedRowIds: number[]): Promise<void> {
    const table = schema.table('tableA');
    const idIndex: Index = table['id'].getIndex();
    const idRangeScanStep = new IndexRangeScanStep(
        env.global, idIndex, new MockKeyRangeCalculator([idKeyRange]), false);

    const nameIndex: Index = table['name'].getIndex();
    const nameRangeScanStep = new IndexRangeScanStep(
        env.global, nameIndex, new MockKeyRangeCalculator([nameKeyRange]),
        false);

    const step = new MultiIndexRangeScanStep();
    step.addChild(idRangeScanStep);
    step.addChild(nameRangeScanStep);

    return step.exec().then((relations) => {
      assert.equal(1, relations.length);
      const relation = relations[0];
      const actualRowIds = relation.entries.map((entry) => entry.row.id());
      assert.sameMembers(expectedRowIds, actualRowIds);
    });
  }
});
