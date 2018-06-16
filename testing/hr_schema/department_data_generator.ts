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

import {assert} from '../../lib/base/assert';
import {Row} from '../../lib/base/row';
import {ArrayHelper} from '../../lib/structs/array_helper';
import {getHrDbSchemaBuilder} from './hr_schema_builder';
import {HRSchemaSamples} from './sample';

export class DepartmentDataGenerator {
  private names: string[];

  constructor() {
    this.names = HRSchemaSamples.DEPARTMENT_NAMES.slice();
    ArrayHelper.shuffle(this.names);
  }

  public generate(count: number): Row[] {
    const rawData = this.generateRaw(count);
    const d = getHrDbSchemaBuilder().getSchema().table('Department');
    return rawData.map((object) => d.createRow(object));
  }

  private generateRaw(count: number): object[] {
    assert(count <= this.names.length,
        `count can be at most ${this.names.length}`);

    const departments = new Array(count);
    for (let i = 0; i < count; i++) {
      // tslint:disable
      departments[i] = {
        id: 'departmentId' + i.toString(),
        name: this.names.shift(),
        managerId: 'managerId',
        locationId: 'locationId'
      };
      // tslint:enable
    }
    return departments;
  }
}
