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

import {ChangeRecord} from '../../lib/base/change_record';
import {ObserverRegistry} from '../../lib/base/observer_registry';
import {Resolver} from '../../lib/base/resolver';
import {Relation} from '../../lib/proc/relation';
import {SelectBuilder} from '../../lib/query/select_builder';
import {SelectContext} from '../../lib/query/select_context';
import {BaseTable} from '../../lib/schema/base_table';
import {DatabaseSchema} from '../../lib/schema/database_schema';
import {MockEnv} from '../../testing/mock_env';
import {getMockSchemaBuilder} from '../../testing/mock_schema_builder';

const assert = chai.assert;

describe('ObservableRegistry', () => {
  let registry: ObserverRegistry;
  let schema: DatabaseSchema;
  let env: MockEnv;

  beforeEach(async () => {
    schema = getMockSchemaBuilder().getSchema();
    env = new MockEnv(schema);
    await env.init();
    registry = env.observerRegistry;
  });

  // Tests that addObserver() works as expected by checking that observers are
  // notified when the observable results are modified.
  it('addObserver', () => {
    const promiseResolver = new Resolver();
    const table = schema.tables()[0];
    const builder = new SelectBuilder(env.global, []);
    builder.from(table);

    const callback = (changes: ChangeRecord[]) => promiseResolver.resolve();

    registry.addObserver(builder, callback);
    const row1 = table.createRow({id: 'dummyId1', value: 'dummyValue1'});
    const row2 = table.createRow({id: 'dummyId2', value: 'dummyValue2'});

    const firstResults = Relation.fromRows([row1], [table.getName()]);
    assert.isTrue(
        registry.updateResultsForQuery(builder.getQuery(), firstResults));

    const secondResults = Relation.fromRows([row1, row2], [table.getName()]);
    assert.isTrue(
        registry.updateResultsForQuery(builder.getQuery(), secondResults));
    return promiseResolver.promise;
  });

  it('removeObserver', () => {
    const table = schema.tables()[0];
    const builder = new SelectBuilder(env.global, []);
    builder.from(table);

    const callback = () => assert.fail(new Error('Observer not removed'));
    registry.addObserver(builder, callback);
    registry.removeObserver(builder, callback);
    const row = table.createRow({id: 'dummyId', value: 'dummyValue'});
    const newResults = Relation.fromRows([row], [table.getName()]);
    assert.isFalse(
        registry.updateResultsForQuery(builder.getQuery(), newResults));
  });

  it('getTaskItemsForTables', () => {
    const tables = schema.tables();

    const builder1 = new SelectBuilder(env.global, []);
    builder1.from(tables[0]);
    const builder2 = new SelectBuilder(env.global, []);
    builder2.from(tables[0], tables[1]);
    const builder3 = new SelectBuilder(env.global, []);
    builder3.from(tables[1]);

    const callback = () => {
      return;
    };

    registry.addObserver(builder1, callback);
    registry.addObserver(builder2, callback);
    registry.addObserver(builder3, callback);

    const getQueriesForTables =
        (targetSet: Set<BaseTable>): SelectContext[] => {
          return registry.getTaskItemsForTables(Array.from(targetSet.values()))
              .map((item) => item.context as SelectContext);
        };

    const scope1 = new Set<BaseTable>();
    scope1.add(tables[0]);
    let queries = getQueriesForTables(scope1);
    assert.sameDeepOrderedMembers(
        [builder1.getObservableQuery(), builder2.getObservableQuery()],
        queries);

    const scope2 = new Set<BaseTable>();
    scope2.add(tables[1]);
    queries = getQueriesForTables(scope2);
    assert.sameDeepOrderedMembers(
        [builder2.getObservableQuery(), builder3.getObservableQuery()],
        queries);

    const scope3 = new Set<BaseTable>();
    scope3.add(tables[0]);
    scope3.add(tables[1]);
    queries = getQueriesForTables(scope3);
    assert.sameDeepOrderedMembers(
        [
          builder1.getObservableQuery(),
          builder2.getObservableQuery(),
          builder3.getObservableQuery(),
        ],
        queries);
  });
});
