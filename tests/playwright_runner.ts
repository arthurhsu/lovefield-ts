/**
 * Copyright 2026 The Lovefield Project Authors. All Rights Reserved.
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

import {chromium} from 'playwright';
import * as path from 'path';

// A simple Playwright runner that opens the test harness HTML and waits for Mocha results.
async function runTests() {
  const browser = await chromium.launch({
    headless: true,
    args: ['--disable-gpu', '--no-sandbox', '--disable-dev-shm-usage'],
  });
  const context = await browser.newContext();
  const page = await context.newPage();

  try {
    const harnessPath =
      'file://' + path.resolve(__dirname, 'harness/test_harness.html');
    console.log('Opening test harness:', harnessPath);
    await page.goto(harnessPath);

    console.log('Waiting for tests to finish...');

    // Poll window.mochaResults until finished is true
    const results = await page
      .waitForFunction(
        () => {
          const res = (window as any).mochaResults;
          return res && res.finished;
        },
        {timeout: 60000}
      )
      .then(() => page.evaluate(() => (window as any).mochaResults));

    const {failures, passes, tests} = results as any;
    console.log(`Tests finished: ${passes} passed, ${failures} failed.`);

    if (failures > 0) {
      console.log('\nFailing tests:');
      tests.forEach((test: any) => {
        if (test.state === 'failed') {
          console.log(`\n✖ ${test.title}`);
          if (test.err) {
            console.log(`  ${test.err.message}`);
            if (test.err.stack) {
              console.log(`  ${test.err.stack}`);
            }
          }
        }
      });
      process.exit(1);
    }
  } catch (error) {
    console.error('An error occurred during test execution:', error);
    process.exit(1);
  } finally {
    await browser.close();
  }
}

runTests();
