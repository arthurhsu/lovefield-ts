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
import {DataStoreType} from '../../lib/base/enum';
import {ConnectOptions} from '../../lib/schema/connect_options';
import {getHrDbSchemaBuilder} from '../../testing/hr_schema/hr_schema_builder';
import {MockDataGenerator} from '../../testing/hr_schema/mock_data_generator';

const assert = chai.assert;

describe('ImportTask', () => {
  let capability: Capability;
  before(() => {
    capability = Capability.get();
  });

  async function runTestImport(options: ConnectOptions): Promise<void> {
    // Need to guarantee that the two DBs have different names.
    const builder1 = getHrDbSchemaBuilder(`hr1_${new Date().getTime()}`);
    const builder2 = getHrDbSchemaBuilder(`hr2_${new Date().getTime()}`);
    const dataGen = new MockDataGenerator();
    dataGen.generate(
        /* jobCount */ 100, /* employeeCount */ 1000, /* departmentCount */ 10);

    const db = await builder1.connect(options);
    const d = db.getSchema().table('Department');
    const l = db.getSchema().table('Location');
    const c = db.getSchema().table('Country');
    const r = db.getSchema().table('Region');
    const j = db.getSchema().table('Job');
    const e = db.getSchema().table('Employee');

    const tx = db.createTransaction();
    await tx.exec([
      db.insert().into(r).values(dataGen.sampleRegions),
      db.insert().into(c).values(dataGen.sampleCountries),
      db.insert().into(l).values(dataGen.sampleLocations),
      db.insert().into(d).values(dataGen.sampleDepartments),
      db.insert().into(j).values(dataGen.sampleJobs),
      db.insert().into(e).values(dataGen.sampleEmployees),
    ]);

    const data = await db.export();
    db.close();
    const db2 = await builder2.connect(options);
    data['name'] = builder2.getSchema().name();
    await db2.import(data);
    const exportedData = await db2.export();
    assert.equal(builder2.getSchema().name(), exportedData['name']);
    assert.equal(builder2.getSchema().version(), exportedData['version']);
    assert.deepEqual(data, exportedData);
  }

  it('import_MemDB', async () => {
    await runTestImport({storeType: DataStoreType.MEMORY});
  });

  it('import_IndexedDB', async () => {
    if (!capability.indexedDb) {
      return;
    }

    await runTestImport({storeType: DataStoreType.INDEXED_DB});
  });

  it('import_WebSql', async () => {
    if (!capability.webSql) {
      return;
    }

    await runTestImport({storeType: DataStoreType.WEB_SQL});
  });

  it.skip('benchmark', async () => {
    const ROW_COUNT = 62500;
    const LOOP_COUNT = 30;

    const jobs: object[] = new Array(ROW_COUNT);
    for (let i = 0; i < ROW_COUNT; ++i) {
      jobs[i] = {
        id: `jobId${i}`,
        maxSalary: 20000 + i,
        minSalary: 10000 + i,
        title: `Job ${i}`,
      };
    }

    let start: number;
    let results: number[];
    const runImport = async () => {
      const builder = getHrDbSchemaBuilder();
      const data = {
        name: builder.getSchema().name(),
        tables: {Job: jobs},
        version: builder.getSchema().version(),
      };

      const db = await builder.connect({storeType: DataStoreType.MEMORY});
      start = performance.now();
      await db.import(data);
      const end = performance.now();
      results.push(end - start);
    };

    const runInsert = async () => {
      const builder = getHrDbSchemaBuilder();
      const db = await builder.connect({storeType: DataStoreType.MEMORY});
      const j = db.getSchema().table('Job');
      start = performance.now();
      const rows = jobs.map((data) => j.createRow(data));
      await db.insert().into(j).values(rows).exec();
      const end = performance.now();
      results.push(end - start);
    };

    const compute = () => {
      const base = results.sort((a, b) => (a < b) ? -1 : ((a > b) ? 1 : 0));
      const average = base.reduce((p, c) => p + c, 0) / LOOP_COUNT;
      console['log'](average);
    };

    results = [];
    for (let i = 0; i < LOOP_COUNT; ++i) {
      await runImport();
    }
    compute();
    results = [];
    for (let i = 0; i < LOOP_COUNT; ++i) {
      await runInsert();
    }
    compute();
  });
});
