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
import {Order} from '../../../lib/base/enum';
import {SingleKeyRange} from '../../../lib/index/key_range';
import {IndexRangeScanStep} from '../../../lib/proc/pp/index_range_scan_step';
import {BaseTable} from '../../../lib/schema/base_table';
import {DatabaseSchema} from '../../../lib/schema/database_schema';
import {IndexImpl} from '../../../lib/schema/index_impl';
import {MockEnv} from '../../../testing/mock_env';
import {MockKeyRangeCalculator} from '../../../testing/mock_key_range_calculator';
import {getMockSchemaBuilder} from '../../../testing/mock_schema_builder';

const assert = chai.assert;

describe('IndexRangeScanStep', () => {
  let env: MockEnv;
  let schema: DatabaseSchema;

  beforeEach(() => {
    env = new MockEnv(getMockSchemaBuilder().getSchema());
    return env.init().then(() => {
      schema = env.schema;
      return env.addSampleData();
    });
  });

  it('ascending', () => {
    return checkIndexRangeScan(Order.ASC);
  });

  it('descending', () => {
    return checkIndexRangeScan(Order.DESC);
  });

  // Checks that an IndexRangeScanStep returns results in the expected order.
  function checkIndexRangeScan(order: Order): Promise<void> {
    const table = schema.table('tableA') as BaseTable;
    const index =
      order === Order.ASC ? table.getIndices()[0] : table.getIndices()[1];
    const keyRange =
      order === Order.ASC
        ? new SingleKeyRange(5, 8, false, false)
        : new SingleKeyRange('dummyName' + 5, 'dummyName' + 8, false, false);
    const step = new IndexRangeScanStep(
      env.global,
      index as IndexImpl,
      new MockKeyRangeCalculator([keyRange]),
      false
    );

    return step.exec().then(relations => {
      const relation = relations[0];
      assert.equal(4, relation.entries.length);
      relation.entries.forEach((entry, j) => {
        if (j === 0) {
          return;
        }

        // Row ID is equal to the payload's ID field for the data used in this
        // test.
        const comparator = order === Order.ASC ? 1 : -1;
        assert.isTrue(
          comparator * (entry.row.id() - relation.entries[j - 1].row.id()) > 0
        );
      });
    });
  }
});
