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

import 'chromedriver';
import * as path from 'path';
import {Builder, WebDriver} from 'selenium-webdriver';
import {Options} from 'selenium-webdriver/chrome';

// A simple Selenium runner that opens the test harness HTML and waits for Mocha results.
async function runTests() {
  const options = new Options();
  options.addArguments('--headless');
  options.addArguments('--disable-gpu');
  options.addArguments('--no-sandbox');
  options.addArguments('--disable-dev-shm-usage');

  const driver: WebDriver = await new Builder()
    .forBrowser('chrome')
    .setChromeOptions(options)
    .build();

  try {
    const harnessPath =
      'file://' + path.resolve(__dirname, 'harness/test_harness.html');
    console.log('Opening test harness:', harnessPath);
    await driver.get(harnessPath);

    console.log('Waiting for tests to finish...');
    // Poll window.mochaResults until finished is true
    const results = await driver.wait(async () => {
      const res = await driver.executeScript('return window.mochaResults;');
      if (res && (res as any).finished) {
        return res;
      }
      return false;
    }, 60000); // 60 seconds timeout

    const {failures, passes} = results as any;
    console.log(`Tests finished: ${passes} passed, ${failures} failed.`);

    if (failures > 0) {
      process.exit(1);
    }
  } catch (error) {
    console.error('An error occurred during test execution:', error);
    process.exit(1);
  } finally {
    await driver.quit();
  }
}

runTests();
