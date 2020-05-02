/**
 * Copyright 2020 The Lovefield Project Authors. All Rights Reserved.
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
import * as lf from './node_modules/lovefield-ts/dist/es6/lf.js';

import {FullTableBenchmark} from './full_table_benchmark.js';
import {LoadingEmptyDBBenchmark} from './loading_empty_db_benchmark.js';
import {LoadingPopulatedDBBenchmark} from './loading_populated_db_benchmark.js';
import {PKTableBenchmark} from './pk_table_benchmark.js';
import {Runner} from './runner.js';
import {ScenarioBenchmark} from './scenario_benchmark.js';
import {SelectBenchmark} from './select_benchmark.js';
import {ForeignKeysBenchmark} from './foreign_keys_benchmark.js';

const overallResults = [];
const REPETITIONS = 5;

async function setUp() {
  return new Promise((resolve, reject) => {
    const request = window.indexedDB.deleteDatabase('hr_noFK');
    request.onsuccess = resolve;
    request.onerror = reject;
    request.onblocked = resolve;
    request.onupgradeneeded = resolve;
  });
}

async function runTest(ms, test) {
  const timeout = new Promise((resolve, reject) => {
    const id = setTimeout(() => {
      clearTimeout(id);
      reject(new Error('Test timed out'));
    }, ms);
  });
  return Promise.race([
    test(),
    timeout,
  ]);
}

async function selectRunner(name, volatile) {
  const selectBenchmark =
      await SelectBenchmark.fromJson('test4_mock_data_30k.json', volatile);
  await selectBenchmark.insertSampleData();
  const runner = new Runner(name);
  runner.schedule(selectBenchmark);
  overallResults.push(await runner.run(REPETITIONS));
  return selectBenchmark.tearDown();
}

async function test1LoadingEmptyDB() {
  const benchmark = new LoadingEmptyDBBenchmark();
  const runner = new Runner(
      'Loading Empty DB',
      setUp,
      benchmark.close.bind(benchmark));
  runner.schedule(benchmark);
  overallResults.push(await runner.run(REPETITIONS));
}

async function test2FullTableOps() {
  const benchmark = new FullTableBenchmark();
  const runner = new Runner(
      'Full table SCUD',
      setUp,
      benchmark.close.bind(benchmark));
  runner.schedule(benchmark);
  overallResults.push(await runner.run(REPETITIONS));
}

async function test2FullTableOpsMem() {
  const benchmark = new FullTableBenchmark(true);
  const runner = new Runner(
      'Full table SCUD mem',
      undefined,
      benchmark.close.bind(benchmark));
  runner.schedule(benchmark);
  overallResults.push(await runner.run(REPETITIONS));
}

async function test3PKTableOps() {
  const benchmark = new PKTableBenchmark();
  const runner = new Runner(
      'PK-based SCUD',
      setUp,
      benchmark.close.bind(benchmark));
  runner.schedule(benchmark);
  overallResults.push(await runner.run(REPETITIONS));
}

async function test3PKTableOpsMem() {
  const benchmark = new PKTableBenchmark(true);
  const runner = new Runner(
      'PK-based SCUD',
      undefined,
      benchmark.close.bind(benchmark));
  runner.schedule(benchmark);
  overallResults.push(await runner.run(REPETITIONS));
}

async function test4Select() {
  await setUp();
  return selectRunner('Select benchmark');
}

async function test4SelectMem() {
  return selectRunner('Select benchmark mem', true);
}

async function test5LoadingPopulatedDB() {
  const rowCount = 20000;
  const benchmark = new LoadingPopulatedDBBenchmark();

  const preRunSetup = async function() {
    await benchmark.init();
    await benchmark.close();
    await benchmark.init();
    await benchmark.loadTestData('default_benchmark_mock_data_50k.json');
    await benchmark.insert(rowCount);
    await benchmark.close(true /* skipDeletion */);
  };

  const runner = new Runner(
      'Loading Populated DB',
      preRunSetup,
      benchmark.close.bind(benchmark));
  runner.schedule(benchmark);
  overallResults.push(await runner.run(REPETITIONS));
}

async function test6ScenarioSimulations() {
  const benchmark = new ScenarioBenchmark();
  const runner = new Runner('Scenario Simulations');

  await benchmark.init();
  runner.schedule(benchmark);
  overallResults.push(await runner.run(REPETITIONS));
}

async function test7ForeignKeys() {
  const benchmarks = [
    await ForeignKeysBenchmark.create(lf.ConstraintTiming.IMMEDIATE),
    await ForeignKeysBenchmark.create(lf.ConstraintTiming.DEFERRABLE),
    await ForeignKeysBenchmark.create(null),
  ];
  const runner = new Runner('Foreign Keys Benchmark');
  benchmarks.forEach((b) => runner.schedule(b));
  overallResults.push(await runner.run(REPETITIONS));
}

async function start(tests, timeout) {
  for (let i = 0; i < tests.length; ++i) {
    await runTest(timeout, tests[i]);
  }
  return Promise.resolve();
}

window.onload = () => {
  const timeout = 30 * 60 * 1000; // 30 minutes

  const tests = [
    test1LoadingEmptyDB,
    test2FullTableOps,
    test2FullTableOpsMem,
    test3PKTableOps,
    test3PKTableOpsMem,
    test4Select,
    test4SelectMem,
    test5LoadingPopulatedDB,
    test6ScenarioSimulations,
    test7ForeignKeys,
  ];

  start(tests, timeout).then(() => {
    console.log(overallResults);
  });
};
