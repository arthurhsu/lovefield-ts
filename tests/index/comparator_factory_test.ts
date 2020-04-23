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
import { ComparatorFactory } from '../../lib/index/comparator_factory';
import { MultiKeyComparator } from '../../lib/index/multi_key_comparator';
import { SimpleComparator } from '../../lib/index/simple_comparator';
import { BaseTable } from '../../lib/schema/base_table';
import { IndexImpl } from '../../lib/schema/index_impl';
import { getHrDbSchemaBuilder } from '../../testing/hr_schema/hr_schema_builder';

const assert = chai.assert;

describe('ComparatorFactory', () => {
  const schema = getHrDbSchemaBuilder().getSchema();

  const maxSalary = (schema.table('Job') as BaseTable)
    .getIndices()
    .filter(index => {
      return index.getNormalizedName() === 'Job.idx_maxSalary';
    })[0] as IndexImpl;
  assert.isTrue(
    ComparatorFactory.create(maxSalary) instanceof SimpleComparator
  );

  const uqConstraint = (schema.table('DummyTable') as BaseTable)
    .getIndices()
    .filter(index => {
      return index.getNormalizedName() === 'DummyTable.uq_constraint';
    })[0] as IndexImpl;
  assert.isTrue(
    ComparatorFactory.create(uqConstraint) instanceof MultiKeyComparator
  );
});
