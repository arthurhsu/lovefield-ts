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

import {DatabaseConnection} from '../../../lib/base/database_connection';
import {DataStoreType, Type} from '../../../lib/base/enum';
import {Global} from '../../../lib/base/global';
import {AggregatedColumn} from '../../../lib/fn/aggregated_column';
import {fn} from '../../../lib/fn/fn';
import {Predicate} from '../../../lib/pred/predicate';
import {AggregationStep} from '../../../lib/proc/pp/aggregation_step';
import {GetRowCountPass} from '../../../lib/proc/pp/get_row_count_pass';
import {ProjectStep} from '../../../lib/proc/pp/project_step';
import {SelectStep} from '../../../lib/proc/pp/select_step';
import {TableAccessFullStep} from '../../../lib/proc/pp/table_access_full_step';
import {RuntimeDatabase} from '../../../lib/proc/runtime_database';
import {SelectContext} from '../../../lib/query/select_context';
import {BaseColumn} from '../../../lib/schema/base_column';
import {Builder} from '../../../lib/schema/builder';
import {DatabaseSchema} from '../../../lib/schema/database_schema';
import {TreeTestHelper} from '../../../testing/tree_test_helper';

describe('GetRowCountPass', () => {
  let conn: DatabaseConnection;
  let schema: DatabaseSchema;
  let global: Global;
  let pass: GetRowCountPass;

  function getSchemaBuilder(): Builder {
    const schemaBuilder = new Builder('testschema', 1);
    schemaBuilder.createTable('TableFoo')
        .addColumn('id1', Type.STRING)
        .addColumn('id2', Type.STRING);
    return schemaBuilder;
  }

  beforeEach(() => {
    const connectOptions = {storeType: DataStoreType.MEMORY};
    return getSchemaBuilder().connect(connectOptions).then((db) => {
      conn = db;
      schema = db.getSchema();
      global = (db as RuntimeDatabase).getGlobal();
      pass = new GetRowCountPass(global);
    });
  });

  afterEach(() => {
    conn.close();
  });

  // Tests a simple tree, where only one AND predicate exists.
  it('simpleTree', () => {
    const tf = schema.table('TableFoo');

    const treeBefore = [
      'project(COUNT(*))',
      '-aggregation(COUNT(*))',
      '--table_access(TableFoo)',
      '',
    ].join('\n');

    const treeAfter = [
      'project(COUNT(*))',
      '-aggregation(COUNT(*))',
      '--get_row_count(TableFoo)',
      '',
    ].join('\n');

    const constructTree = () => {
      const queryContext = new SelectContext(schema);
      queryContext.from = [tf];
      queryContext.columns = [fn.count()];

      const tableAccessStep =
          new TableAccessFullStep(global, queryContext.from[0]);
      const aggregationStep =
          new AggregationStep(queryContext.columns as AggregatedColumn[]);
      const projectStep =
          new ProjectStep(queryContext.columns, null as any as BaseColumn[]);
      projectStep.addChild(aggregationStep);
      aggregationStep.addChild(tableAccessStep);

      return {
        queryContext: queryContext,
        root: projectStep,
      };
    };

    TreeTestHelper.assertTreeTransformation(
        constructTree(), treeBefore, treeAfter, pass);
  });

  // Test that this optimization does not apply COUNT(column) is used.
  it('treeUnaffected1', () => {
    const tf = schema.table('TableFoo');

    const treeBefore = [
      'project(COUNT(TableFoo.id1))',
      '-aggregation(COUNT(TableFoo.id1))',
      '--table_access(TableFoo)',
      '',
    ].join('\n');

    const constructTree = () => {
      const queryContext = new SelectContext(schema);
      queryContext.from = [tf];
      queryContext.columns = [fn.count(tf['id1'])];

      const tableAccessStep =
          new TableAccessFullStep(global, queryContext.from[0]);
      const aggregationStep =
          new AggregationStep(queryContext.columns as AggregatedColumn[]);
      const projectStep =
          new ProjectStep(queryContext.columns, null as any as BaseColumn[]);
      projectStep.addChild(aggregationStep);
      aggregationStep.addChild(tableAccessStep);

      return {
        queryContext: queryContext,
        root: projectStep,
      };
    };

    TreeTestHelper.assertTreeTransformation(
        constructTree(), treeBefore, treeBefore, pass);
  });

  // Test that this optimization does not apply if a WHERE clause exists.
  it('treeUnaffected2', () => {
    const tf = schema.table('TableFoo');

    const treeBefore = [
      'project(COUNT(*))',
      '-aggregation(COUNT(*))',
      '--select(value_pred(TableFoo.id1 eq someId))',
      '---table_access(TableFoo)',
      '',
    ].join('\n');

    const constructTree = () => {
      const queryContext = new SelectContext(schema);
      queryContext.from = [tf];
      queryContext.columns = [fn.count()];
      queryContext.where = tf['id1'].eq('someId');

      const tableAccessStep =
          new TableAccessFullStep(global, queryContext.from[0]);
      const selectStep =
          new SelectStep((queryContext.where as Predicate).getId());
      const aggregationStep =
          new AggregationStep(queryContext.columns as AggregatedColumn[]);
      const projectStep =
          new ProjectStep(queryContext.columns, null as any as BaseColumn[]);
      projectStep.addChild(aggregationStep);
      aggregationStep.addChild(selectStep);
      selectStep.addChild(tableAccessStep);

      return {
        queryContext: queryContext,
        root: projectStep,
      };
    };

    TreeTestHelper.assertTreeTransformation(
        constructTree(), treeBefore, treeBefore, pass);
  });
});
