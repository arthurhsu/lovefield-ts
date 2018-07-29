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
import {op} from '../../../lib/fn/op';
import {AndPredicatePass} from '../../../lib/proc/lp/and_predicate_pass';
import {SelectNode} from '../../../lib/proc/lp/select_node';
import {TableAccessNode} from '../../../lib/proc/lp/table_access_node';
import {DatabaseSchema} from '../../../lib/schema/database_schema';
import {TreeHelper} from '../../../lib/structs/tree_helper';
import {getHrDbSchemaBuilder} from '../../../testing/hr_schema/hr_schema_builder';

const assert = chai.assert;

describe('AndPredicatePass', () => {
  let schema: DatabaseSchema;

  before(() => {
    schema = getHrDbSchemaBuilder().getSchema();
  });

  it('simpleTree', () => {
    const e = schema.table('Employee');
    const treeBefore = [
      'select(combined_pred_and)',
      '-table_access(Employee)',
      '',
    ].join('\n');

    const treeAfter = [
      'select(value_pred(Employee.id gt 100))',
      '-select(value_pred(Employee.salary lt 100))',
      '--table_access(Employee)',
      '',
    ].join('\n');

    // Generating a simple tree that has just one SelectNode corresponding to an
    // AND predicate.
    const leftPredicate = e['id'].gt('100');
    const rightPredicate = e['salary'].lt(100);
    const rootNodeBefore =
        new SelectNode(op.and(leftPredicate, rightPredicate));
    const tableAccessNode = new TableAccessNode(e);
    rootNodeBefore.addChild(tableAccessNode);
    assert.equal(treeBefore, TreeHelper.toString(rootNodeBefore));

    const pass = new AndPredicatePass();
    const rootNodeAfter = pass.rewrite(rootNodeBefore);
    assert.equal(treeAfter, TreeHelper.toString(rootNodeAfter));
  });
});
