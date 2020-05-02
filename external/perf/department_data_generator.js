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

import {Sample} from './sample.js';
import {shuffle} from './shuffle.js';

export class DepartmentDataGenerator {
  constructor(schema) {
    this.schema = schema;
    this.names = shuffle(Sample.DEPARTMENT_NAMES.slice());
  }

  generate(count) {
    const rawData = this.generateRaw(count);
    const d = this.schema.table('Department');
    return rawData.map((object) => d.createRow(object));
  }

  generateRaw(count) {
    if (count > this.names.length) {
      throw new Error(`count can be at most ${this.names.length}`);
    }

    const departments = new Array(count);
    for (let i = 0; i < count; i++) {
      departments[i] = {
        id: `departmentId${i}`,
        name: this.names.shift(),
        managerId: 'managerId',
        locationId: 'locationId',
      };
    }
    return departments;
  }
}
