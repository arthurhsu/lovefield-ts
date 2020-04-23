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

import { DataStoreType } from '../../lib/base/enum';
import { Row } from '../../lib/base/row';
import { RuntimeDatabase } from '../../lib/proc/runtime_database';
import { Table } from '../../lib/schema/table';
import { getHrDbSchemaBuilder } from '../../testing/hr_schema/hr_schema_builder';
import { HRSchemaSampleData } from '../../testing/hr_schema/hr_schema_sample_data';

const assert = chai.assert;

describe('ExportTask', () => {
  let db: RuntimeDatabase;
  let j: Table;
  const ROW_COUNT = 2;

  beforeEach(async () => {
    db = (await getHrDbSchemaBuilder('hr').connect({
      storeType: DataStoreType.MEMORY,
    })) as RuntimeDatabase;
    j = db.getSchema().table('Job');
  });

  afterEach(() => db.close());

  function insertSampleJobs(): Promise<unknown> {
    const rows: Row[] = [];
    for (let i = 0; i < ROW_COUNT; ++i) {
      const job = HRSchemaSampleData.generateSampleJobData();
      job.payload()['id'] = `jobId${i}`;
      rows.push(job);
    }
    return db
      .insert()
      .into(j)
      .values(rows)
      .exec();
  }

  it('export', async () => {
    const EXPECTED = {
      name: 'hr',
      tables: {
        Country: [],
        CrossColumnTable: [],
        Department: [],
        DummyTable: [],
        Employee: [],
        Holiday: [],
        Job: [
          {
            id: 'jobId0',
            maxSalary: 500000,
            minSalary: 100000,
            title: 'Software Engineer',
          },
          {
            id: 'jobId1',
            maxSalary: 500000,
            minSalary: 100000,
            title: 'Software Engineer',
          },
        ],
        JobHistory: [],
        Location: [],
        Region: [],
      },
      version: 1,
    };

    await insertSampleJobs();
    const results = await db.export();
    assert.deepEqual(EXPECTED, results);
  });
});
