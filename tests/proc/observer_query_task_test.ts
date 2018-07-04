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
import {DataStoreType} from '../../lib/base/enum';
import {Resolver} from '../../lib/base/resolver';
import {ObserverQueryTask} from '../../lib/proc/observer_query_task';
import {RuntimeDatabase} from '../../lib/proc/runtime_database';
import {SelectBuilder} from '../../lib/query/select_builder';
import {Table} from '../../lib/schema/table';
import {getHrDbSchemaBuilder} from '../../testing/hr_schema/hr_schema_builder';
import {HRSchemaSampleData} from '../../testing/hr_schema/hr_schema_sample_data';

const assert = chai.assert;

describe('ObserverQueryTask', () => {
  let db: RuntimeDatabase;
  let j: Table;
  const ROW_COUNT = 3;

  beforeEach(async () => {
    db = await getHrDbSchemaBuilder().connect(
             {storeType: DataStoreType.MEMORY}) as RuntimeDatabase;
    j = db.getSchema().table('Job');
  });

  afterEach(() => db.close());

  function insertSampleJobs(): Promise<void> {
    const rows = [];
    for (let i = 0; i < ROW_COUNT; ++i) {
      const job = HRSchemaSampleData.generateSampleJobData();
      job.payload()['id'] = `jobId${i}`;
      rows.push(job);
    }
    return db.insert().into(j).values(rows).exec();
  }

  it('exec', () => {
    const promiseResolver = new Resolver<void>();

    const selectQuery = db.select().from(j) as SelectBuilder;

    const observerCallback = (changes: ChangeRecord[]) => {
      // Expecting one "change" record for each insertion.
      assert.equal(ROW_COUNT, changes.length);
      changes.forEach((change) => {
        assert.equal(1, change['addedCount']);
      });

      db.unobserve(selectQuery, observerCallback);
      promiseResolver.resolve();
    };

    insertSampleJobs().then(
        () => {
          // Start observing.
          db.observe(selectQuery, observerCallback);
          const observerTask = new ObserverQueryTask(
              db.getGlobal(), [selectQuery.getObservableTaskItem()]);
          return observerTask.exec();
        },
        (e) => {
          promiseResolver.reject(e);
        });

    return promiseResolver.promise;
  });
});
