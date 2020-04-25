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
import {Capability} from '../../lib/base/capability';
import {DatabaseConnection} from '../../lib/base/database_connection';
import {DataStoreType, ErrorCode} from '../../lib/base/enum';
import {getHrDbSchemaBuilder} from '../../testing/hr_schema/hr_schema_builder';
import {TestUtil} from '../../testing/test_util';

const assert = chai.assert;

describe('Close', () => {
  let capability: Capability;
  let db: DatabaseConnection;

  before(() => {
    capability = Capability.get();
  });

  afterEach(() => {
    if (db) {
      db.close();
    }
  });

  it('connectTwiceThrows_SchemaBuilder', () => {
    TestUtil.assertThrowsError(
      //  Connection operation was already in progress.
      ErrorCode.ALREADY_CONNECTED,
      () => {
        const schemaBuilder = getHrDbSchemaBuilder();
        schemaBuilder
          .connect({storeType: DataStoreType.MEMORY})
          .then(conn => (db = conn));
        schemaBuilder.connect({storeType: DataStoreType.MEMORY});
      }
    );
  });

  it('close', async () => {
    if (!capability.indexedDb) {
      return;
    }

    const dbName = `foo${Date.now()}`;
    let database = await getHrDbSchemaBuilder(dbName).connect();
    db = database;
    // Test that all queries after closing are throwing.
    database.close();
    const thrower = () =>
      database.select().from(database.getSchema().table('Employee'));
    assert.throw(thrower);

    // Test that db can be opened again.
    database = await getHrDbSchemaBuilder(dbName).connect();
    assert.notEqual(db, database);
    assert.isNotNull(database);
    database.close();
  });
});
