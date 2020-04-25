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
import {BaseColumn} from '../../lib/schema/base_column';
import {IndexImpl} from '../../lib/schema/index_impl';
import {DatabaseSchema} from '../../lib/schema/database_schema';
import {getMockSchemaBuilder} from '../../testing/mock_schema_builder';

const assert = chai.assert;

describe('BaseColumn', () => {
  let schema: DatabaseSchema;
  before(() => {
    schema = getMockSchemaBuilder().getSchema();
  });

  it('getIndices', () => {
    const table = schema.table('tableA');

    const idIndices = (table['id'] as BaseColumn).getIndices() as IndexImpl[];
    assert.equal(1, idIndices.length);
    assert.equal(
      (table['id'] as BaseColumn).name,
      idIndices[0].columns[0].schema.getName()
    );

    const nameIndices = (table[
      'name'
    ] as BaseColumn).getIndices() as IndexImpl[];
    assert.equal(1, nameIndices.length);
    assert.equal(
      (table['name'] as BaseColumn).name,
      nameIndices[0].columns[0].schema.getName()
    );
  });

  it('getIndices_NoIndicesExist', () => {
    const tableWithNoIndices = schema.table('tableC');
    assert.equal(
      0,
      (tableWithNoIndices['id'] as BaseColumn).getIndices().length
    );
    assert.equal(
      0,
      (tableWithNoIndices['name'] as BaseColumn).getIndices().length
    );
  });

  it('getIndex', () => {
    const table = schema.table('tableA');
    const idIndex = (table['id'] as BaseColumn).getIndex() as IndexImpl;
    assert.isNotNull(idIndex);
    assert.equal(1, idIndex.columns.length);
    assert.equal(
      (table['id'] as BaseColumn).name,
      idIndex.columns[0].schema.getName()
    );

    const nameIndex = (table['name'] as BaseColumn).getIndex() as IndexImpl;
    assert.isNotNull(nameIndex);
    assert.equal('idxName', nameIndex.name);
    assert.equal(1, nameIndex.columns.length);
    assert.equal(
      (table['name'] as BaseColumn).name,
      nameIndex.columns[0].schema.getName()
    );
  });

  it('getIndex_NoIndexExists', () => {
    const tableWithNoIndices = schema.table('tableC');
    assert.isNull((tableWithNoIndices['id'] as BaseColumn).getIndex());
    assert.isNull((tableWithNoIndices['name'] as BaseColumn).getIndex());
  });

  // Tests getNormalizedName for the case where an alias for the parent table
  // has been specified.
  it('getNormalizedName', () => {
    const tableNoAlias = schema.table('tableA');
    assert.equal(
      'tableA.name',
      (tableNoAlias['name'] as BaseColumn).getNormalizedName()
    );

    const alias = 'OtherJob';
    const tableAlias = tableNoAlias.as(alias);
    assert.equal(
      `${alias}.name`,
      (tableAlias['name'] as BaseColumn).getNormalizedName()
    );
  });
});
