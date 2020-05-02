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

export class Runner {
  static get LogLevel() {
    return {
      'ERROR': 3,
      'WARNING': 2,
      'INFO': 1,
      'FINE': 0,
    };
  }

  constructor(name, setUp, tearDown) {
    this.name = name;
    this.results = new Map();
    this.tests = [];
    this.setUp = setUp || this.noOp;
    this.tearDown = tearDown || this.noOp;
    this.logLevel = Runner.LogLevel.FINE;
    this.currentRepetition = 0;
  }

  noOp() {
    return Promise.resolve();
  }

  schedule(benchmark) {
    this.tests.push(...benchmark.getTestCases());
  }

  async run(repetitions) {
    const loopCount = repetitions || 1;
    for (let i = 0; i < loopCount; ++i) {
      await this.runTests();
    }

    const data = this.getResults();
    this.info(`RESULT: ${JSON.stringify(data)}`);
    this.fine(`RESULT: ${JSON.stringify(data, null, 2)}`);
    return Promise.resolve(data);
  }

  getResults() {
    const result = {};
    this.tests.forEach((test) => {
      if (test.skipRecording) {
        return {};
      }

      const durations = this.results.get(test.name) || [];
      if (durations.length > 0) {
        const sum = durations.reduce((a, b) => a + b, 0);
        result[test.name] =
          Number(sum / durations.length).toFixed(3).toString();
      } else {
        result[test.name] = 'unavailable';
      }
    }, this);
    return {
      name: this.name,
      data: result,
    };
  }

  log(logLevel, ...args) {
    if (logLevel < this.logLevel) {
      return;
    }

    console.log(...args);
  }

  info(...args) {
    this.log(Runner.LogLevel.INFO, args);
  }

  fine(...args) {
    this.log(Runner.LogLevel.FINE, args);
  }

  async runTests() {
    this.currentRepetition++;
    this.fine(`REPETITION: ${this.currentRepetition}`);
    const functions = [this.setUp];
    this.tests.forEach((test) => {
      functions.push(this.runOneTest.bind(this, test));
    }, this);
    functions.push(this.tearDown);
    for (let i = 0; i < functions.length; ++i) {
      await functions[i]();
    }
    return Promise.resolve();
  }

  async runOneTest(test) {
    this.fine(`\n----------Running ${test.name} ------------`);
    const start = performance.now();
    const result = await test.tester();
    const end = performance.now();
    const duration = end - start;
    if (!test.skipRecording) {
      const timeData = this.results.get(test.name) || [];
      timeData.push(duration);
      this.results.set(test.name, timeData);
    }

    let validated = true;
    if (this.currentRepetition <= 1) {
      validated = await test.validator(result);
    }
    if (validated) {
      this.fine('PASSED', test.name, ':', duration);
      return Promise.resolve();
    } else {
      this.fine('FAILED ', test.name);
      return Promise.reject(new Error(`${test.name} validation failed`));
    }
  }
}
