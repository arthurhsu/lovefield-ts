/**
 * Copyright 2016 The Lovefield Project Authors. All Rights Reserved.
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

import {ConstraintTiming, Order, Type} from '../lib/base/enum';
import {Builder} from '../lib/schema/builder';

export function getMockSchemaBuilder(
  name?: string,
  persistentIndex?: boolean
): Builder {
  const schemaBuilder = new Builder(name || `ms${Date.now()}`, 1);

  schemaBuilder
    .createTable('tableA')
    .addColumn('id', Type.STRING)
    .addColumn('name', Type.STRING)
    .addPrimaryKey(['id'])
    .addIndex('idxName', [{name: 'name', order: Order.DESC}])
    .persistentIndex(persistentIndex || false);

  schemaBuilder
    .createTable('tableB')
    .addColumn('id', Type.STRING)
    .addColumn('name', Type.STRING)
    .addPrimaryKey(['id'])
    .addIndex('idxName', [{name: 'name', order: Order.DESC}]);

  schemaBuilder
    .createTable('tableC')
    .addColumn('id', Type.STRING)
    .addColumn('name', Type.STRING);

  schemaBuilder
    .createTable('tableD')
    .addColumn('id1', Type.STRING)
    .addColumn('id2', Type.NUMBER)
    .addColumn('firstName', Type.STRING)
    .addColumn('lastName', Type.STRING)
    .addPrimaryKey(['id1', 'id2'])
    .addUnique('uq_name', ['firstName', 'lastName']);

  schemaBuilder
    .createTable('tableE')
    .addColumn('id', Type.STRING)
    .addColumn('email', Type.STRING)
    .addPrimaryKey(['id'])
    .addUnique('uq_email', ['email']);

  schemaBuilder
    .createTable('tableF')
    .addColumn('id', Type.STRING)
    .addColumn('name', Type.STRING)
    .addNullable(['name'])
    .addIndex('idxName', [{name: 'name', order: Order.ASC}]);

  schemaBuilder
    .createTable('tableG')
    .addColumn('id', Type.STRING)
    .addColumn('id2', Type.STRING)
    .addUnique('uq_id2', ['id2'])
    .addForeignKey('fk_Id', {
      local: 'id',
      ref: 'tableI.id',
    })
    .addIndex('idx_Id', [{name: 'id', order: Order.ASC}]);

  schemaBuilder
    .createTable('tableH')
    .addColumn('id', Type.STRING)
    .addColumn('id2', Type.STRING)
    .addForeignKey('fk_Id', {
      local: 'id',
      ref: 'tableG.id2',
      timing: ConstraintTiming.DEFERRABLE,
    })
    .addForeignKey('fk_Id2', {
      local: 'id2',
      ref: 'tableI.id2',
      timing: ConstraintTiming.DEFERRABLE,
    });

  schemaBuilder
    .createTable('tableI')
    .addColumn('id', Type.STRING)
    .addColumn('id2', Type.STRING)
    .addPrimaryKey(['id'])
    .addUnique('uq_id2', ['id2'])
    .addColumn('name', Type.STRING)
    .addNullable(['name'])
    .addIndex('idxName', [{name: 'name', order: Order.ASC}]);

  schemaBuilder
    .createTable('tableJ')
    .addColumn('id', Type.STRING)
    .addColumn('id2', Type.STRING)
    .addNullable(['id', 'id2'])
    .addIndex('idxId', ['id', 'id2'], true);

  return schemaBuilder;
}
