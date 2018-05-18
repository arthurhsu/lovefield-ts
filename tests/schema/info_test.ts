/**
 * Copyright 2016 The Lovefield Project Authors. All Rights Reserved.
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
import {ConstraintAction} from '../../lib/base/enum';
import {Info} from '../../lib/schema/info';
import {Table} from '../../lib/schema/table';
import {getHrDbSchemaBuilder} from '../../testing/hr_schema/hr_schema_builder';

const assert = chai.assert;

// TODO(arthurhsu): add static builders when available.
describe('Info', () => {
  let dynamicInfo: Info;

  before(() => {
    dynamicInfo = new Info(getHrDbSchemaBuilder().getSchema());
  });

  function getRefs(
      info: Info, tableName: string,
      constraintAction?: ConstraintAction): string[] {
    const refs = info.getReferencingForeignKeys(tableName, constraintAction);
    return (refs === null) ? null as any as string[] :
                             refs.map((ref) => ref.name);
  }

  it('getReferencingForeignKeys', () => {
    const info = dynamicInfo;
    assert.isNull(getRefs(info, 'DummyTable'));
    assert.sameMembers(['Country.fk_RegionId'], getRefs(info, 'Region'));
    assert.sameMembers(
        ['Country.fk_RegionId'],
        getRefs(info, 'Region', ConstraintAction.RESTRICT));
    assert.isNull(getRefs(info, 'Region', ConstraintAction.CASCADE));

    assert.sameMembers(['Location.fk_CountryId'], getRefs(info, 'Country'));
    assert.sameMembers(
        ['Location.fk_CountryId'],
        getRefs(info, 'Country', ConstraintAction.RESTRICT));
    assert.isNull(getRefs(info, 'Country', ConstraintAction.CASCADE));
  });

  function invoke(toTest: (arg: any) => Table[], arg: any): string[] {
    return toTest(arg).map((table) => table.getName());
  }

  it('getParentTables', () => {
    const info = dynamicInfo;
    const toTest = info.getParentTables.bind(info);
    assert.equal(0, invoke(toTest, 'Region').length);
    assert.sameMembers(['Region'], invoke(toTest, 'Country'));
    assert.sameMembers(['Country'], invoke(toTest, 'Location'));
  });

  it('getParentTablesByColumns', () => {
    const info = dynamicInfo;
    const toTest = info.getParentTablesByColumns.bind(info);
    assert.equal(0, invoke(toTest, []).length);
    assert.equal(0, invoke(toTest, ['DummyTable.arraybuffer']).length);
    assert.sameMembers(['Job'], invoke(toTest, ['Employee.jobId']));
    assert.sameMembers(
        ['Department', 'Job'],
        invoke(toTest, ['Employee.jobId', 'Employee.departmentId']));
  });

  it('getChildTables_All', () => {
    const info = dynamicInfo;
    const toTest = info.getChildTables.bind(info);
    assert.equal(0, invoke(toTest, 'DummyTable').length);
    assert.sameMembers(['Country'], invoke(toTest, 'Region'));
    assert.sameMembers(['Location'], invoke(toTest, 'Country'));
  });

  it('getChildTables_Restrict', () => {
    const info = dynamicInfo;
    const jobChildren = info.getChildTables('Job', ConstraintAction.RESTRICT);
    assert.equal(1, jobChildren.length);
    assert.equal('Employee', jobChildren[0].getName());
    const employeeChildren =
        info.getChildTables('Employee', ConstraintAction.RESTRICT);
    assert.equal(1, employeeChildren.length);
    assert.equal('JobHistory', employeeChildren[0].getName());
  });

  it('getChildTables_Cascade', () => {
    const info = dynamicInfo;
    const jobChildren = info.getChildTables('Job', ConstraintAction.CASCADE);
    assert.equal(0, jobChildren.length);

    const employeeChildren =
        info.getChildTables('Employee', ConstraintAction.CASCADE);
    assert.equal(0, employeeChildren.length);
  });

  it('getChildTablesByColumns', () => {
    const info = dynamicInfo;
    const toTest = info.getChildTablesByColumns.bind(info);
    assert.equal(0, invoke(toTest, []).length);
    assert.equal(0, invoke(toTest, ['DummyTable.arraybuffer']).length);
    assert.sameMembers(
        ['Employee', 'JobHistory'], invoke(toTest, ['Department.id']));
  });
});
