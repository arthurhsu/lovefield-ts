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

import {DefaultBenchmark} from './default_benchmark.js';
import {TestCase} from './test_case.js';

export class FullTableBenchmark extends DefaultBenchmark {
  constructor(volatile) {
    super(volatile);
  }

  getTestCases() {
    const testCases = [
      new TestCase(
          'Init empty DB',
          this.init.bind(this),
          this.validateEmpty.bind(this),
          true),
      new TestCase(
          'Load test data',
          this.loadTestData.bind(
              this, 'default_benchmark_mock_data_50k.json'), undefined, true),
    ];

    for (let i = 10000; i <= 50000; i += 10000) {
      testCases.push(new TestCase(
          'Insert ' + i,
          this.insert.bind(this, i),
          this.validateInsert.bind(this, i)));
      testCases.push(new TestCase(
          'Select ' + i,
          this.select.bind(this)));
      testCases.push(new TestCase(
          'Update ' + i,
          this.updateAll.bind(this, i),
          this.validateUpdateAll.bind(this, i)));
      testCases.push(new TestCase(
          'Delete ' + i,
          this.deleteAll.bind(this),
          this.validateEmpty.bind(this)));
    }

    return testCases;
  }
}
