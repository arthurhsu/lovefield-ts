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

import {BaseTable} from '../../lib/schema/base_table';
import {Database} from '../../lib/schema/database';
import {getHrDbSchemaBuilder} from '../../testing/hr_schema/hr_schema_builder';

const assert = chai.assert;

describe('Table', () => {
  const checkAlias = (schema: Database) => {
    const noAliasTable = schema.table('Job');
    const name = noAliasTable.getName();
    const alias = 'OtherJob';
    const aliasTable = noAliasTable.as(alias) as BaseTable;

    assert.isTrue(noAliasTable !== aliasTable);

    // Assertions about original instance.
    assert.isNull(noAliasTable.getAlias());
    assert.equal(name, noAliasTable.getName());
    assert.equal(name, noAliasTable.getEffectiveName());

    // Assertions about aliased instance.
    assert.equal(alias, aliasTable.getAlias());
    assert.equal(name, aliasTable.getName());
    assert.equal(alias, aliasTable.getEffectiveName());
    assert.equal(noAliasTable.constructor, aliasTable.constructor);
  };

  it('alias_StaticSchema',
     () => {
         // Will not implement in TypeScript port.
     });

  it('alias_DynamicSchema', () => {
    checkAlias(getHrDbSchemaBuilder().getSchema());
  });
});
