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

import {ConstraintAction, ConstraintTiming, Type} from '../lib/base/enum';
import {DatabaseSchema} from '../lib/schema/database_schema';
import {schema} from '../lib/schema/schema';

export class SchemaTestHelper {
  // Returns a schema where TableC refers to TableB, and TableB refers to
  // TableA.
  public static getTableChain(constraintAction: ConstraintAction):
      DatabaseSchema {
    const schemaBuilder = schema.create('contexttest', 1);
    schemaBuilder.createTable('TableA')
        .addColumn('id', Type.STRING)
        .addPrimaryKey(['id']);
    schemaBuilder.createTable('TableB')
        .addColumn('id', Type.STRING)
        .addColumn('foreignKey', Type.STRING)
        .addPrimaryKey(['id'])
        .addForeignKey('fk_tableA', {
          action: constraintAction,
          local: 'foreignKey',
          ref: 'TableA.id',
        });
    schemaBuilder.createTable('TableC')
        .addColumn('id', Type.STRING)
        .addColumn('foreignKey', Type.STRING)
        .addForeignKey('fk_tableB', {
          action: constraintAction,
          local: 'foreignKey',
          ref: 'TableB.id',
        });
    return schemaBuilder.getSchema();
  }

  // Generates a schema with two tables, Parent and Child, linked with a
  // RESTRICT constraint of the given constraint timing.
  public static getOneForeignKey(constraintTiming: ConstraintTiming):
      DatabaseSchema {
    const schemaBuilder = schema.create('testschema', 1);
    schemaBuilder.createTable('Child')
        .addColumn('id', Type.STRING)
        .addForeignKey('fk_Id', {
          action: ConstraintAction.RESTRICT,
          local: 'id',
          ref: 'Parent.id',
          timing: constraintTiming,
        });
    schemaBuilder.createTable('Parent')
        .addColumn('id', Type.STRING)
        .addPrimaryKey(['id']);
    return schemaBuilder.getSchema();
  }

  // Returns a schema where TableB1 and TableB2 both refer to TableA.
  public static getTwoForeignKeys(constraintAction: ConstraintAction):
      DatabaseSchema {
    const schemaBuilder = schema.create('contexttest', 1);
    schemaBuilder.createTable('TableA')
        .addColumn('id1', Type.STRING)
        .addColumn('id2', Type.STRING)
        .addUnique('uq_id1', ['id1'])
        .addUnique('uq_id2', ['id2']);
    schemaBuilder.createTable('TableB1')
        .addColumn('id', Type.STRING)
        .addColumn('foreignKey', Type.STRING)
        .addForeignKey('fk_tableA', {
          action: constraintAction,
          local: 'foreignKey',
          ref: 'TableA.id1',
        });
    schemaBuilder.createTable('TableB2')
        .addColumn('id', Type.STRING)
        .addColumn('foreignKey', Type.STRING)
        .addForeignKey('fk_tableA', {
          action: constraintAction,
          local: 'foreignKey',
          ref: 'TableA.id2',
        });
    return schemaBuilder.getSchema();
  }
}
