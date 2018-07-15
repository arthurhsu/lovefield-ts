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
import {ErrorCode, Order, Type} from '../../lib/base/enum';
import {Row} from '../../lib/base/row';
import {TableBuilder} from '../../lib/schema/table_builder';
import {TestUtil} from '../../testing/test_util';

const assert = chai.assert;

describe('TableBuilder', () => {
  it('throws_DuplicateColumn', () => {
    // 503: Name {0} is already defined.
    TestUtil.assertThrowsError(ErrorCode.NAME_IN_USE, () => {
      const tableBuilder = new TableBuilder('Table');
      tableBuilder.addColumn('col', Type.STRING).addColumn('col', Type.STRING);
    });
  });

  it('throws_InvalidNullable', () => {
    // Testing single column primary key.
    // 545: Primary key column {0} can't be marked as nullable,
    TestUtil.assertThrowsError(ErrorCode.NULLABLE_PK, () => {
      const tableBuilder = new TableBuilder('Table');
      tableBuilder.addColumn('id', Type.STRING)
          .addPrimaryKey(['id'])
          .addNullable(['id']);
      tableBuilder.getSchema();
    });

    // Testing multi column primary key.
    TestUtil.assertThrowsError(ErrorCode.NULLABLE_PK, () => {
      const tableBuilder = new TableBuilder('Table');
      tableBuilder.addColumn('id1', Type.INTEGER)
          .addColumn('id2', Type.INTEGER)
          .addPrimaryKey(['id1', 'id2'])
          .addNullable(['id2']);
      tableBuilder.getSchema();
    });
  });

  it('throws_NonIndexableColumns', () => {
    // 509: Attempt to index table {0} on non-indexable column {1}.
    TestUtil.assertThrowsError(ErrorCode.COLUMN_NOT_INDEXABLE, () => {
      const tableBuilder = new TableBuilder('Table');
      tableBuilder.addColumn('object', Type.OBJECT).addPrimaryKey(['object']);
    });

    TestUtil.assertThrowsError(ErrorCode.COLUMN_NOT_INDEXABLE, () => {
      const tableBuilder = new TableBuilder('Table');
      tableBuilder.addColumn('arraybuffer', Type.ARRAY_BUFFER)
          .addIndex('idx_arraybuffer', ['arraybuffer']);
    });
  });

  it('throws_CrossColumnPkWithAutoInc', () => {
    // 505: Can not use autoIncrement with a cross-column primary key.
    TestUtil.assertThrowsError(ErrorCode.INVALID_AUTO_KEY_COLUMN, () => {
      const tableBuilder = new TableBuilder('Table');
      tableBuilder.addColumn('id1', Type.INTEGER)
          .addColumn('id2', Type.INTEGER)
          .addPrimaryKey([
            {name: 'id1', autoIncrement: true},
            {name: 'id2', autoIncrement: true},
          ]);
    });
  });

  it('throws_NonIntegerPkWithAutoInc', () => {
    // 504: Can not use autoIncrement with a non-integer primary key.
    TestUtil.assertThrowsError(ErrorCode.INVALID_AUTO_KEY_TYPE, () => {
      const tableBuilder = new TableBuilder('Table');
      tableBuilder.addColumn('id', Type.STRING).addPrimaryKey([
        {
          autoIncrement: true,
          name: 'id',
        },
      ]);
    });
  });

  it('throws_InvalidFKLocalColName', () => {
    // 540: Foreign key {0} has invalid reference syntax.
    TestUtil.assertThrowsError(ErrorCode.INVALID_FK_REF, () => {
      const tableBuilder = new TableBuilder('Table');
      tableBuilder.addColumn('employeeId', Type.STRING)
          .addForeignKey(
              'fkemployeeId', {local: 'employeeId1', ref: 'Employee.id'});
    });
  });

  it('throws_DuplicateIndexName', () => {
    // 503: Name FkTableDupIndex.fkemployeeId is already defined.
    // Calling addForeignKey first, addIndex second.
    TestUtil.assertThrowsError(ErrorCode.NAME_IN_USE, () => {
      const tableBuilder = new TableBuilder('Table');
      tableBuilder.addColumn('employeeId', Type.INTEGER)
          .addForeignKey(
              'fkemployeeId', {local: 'employeeId', ref: 'Employee.id'})
          .addIndex('fkemployeeId', ['employeeId'], true, Order.ASC);
    });

    // 503: Name FkTableDupIndex.fkemployeeId is already defined.
    // Calling addIndex first, addForeignKey second.
    TestUtil.assertThrowsError(ErrorCode.NAME_IN_USE, () => {
      const tableBuilder = new TableBuilder('Table');
      tableBuilder.addColumn('employeeId', Type.INTEGER)
          .addIndex('fkemployeeId', ['employeeId'], true, Order.ASC)
          .addForeignKey(
              'fkemployeeId', {local: 'employeeId', ref: 'Employee.id'});
    });
  });

  it('throws_IndexColumnConstraintNameConflict', () => {
    // 546: Indices/constraints/columns can't re-use the table name {0},
    TestUtil.assertThrowsError(ErrorCode.DUPLICATE_NAME, () => {
      const tableBuilder = new TableBuilder('Table');
      tableBuilder.addColumn('id', Type.INTEGER).addIndex('Table', ['id']);
    });

    TestUtil.assertThrowsError(ErrorCode.DUPLICATE_NAME, () => {
      const tableBuilder = new TableBuilder('Table');
      tableBuilder.addColumn('id', Type.INTEGER).addUnique('Table', ['id']);
    });

    TestUtil.assertThrowsError(ErrorCode.DUPLICATE_NAME, () => {
      const tableBuilder = new TableBuilder('Table');
      tableBuilder.addColumn('Table', Type.INTEGER);
    });
  });

  it('throws_ColumnBothPkAndFk', () => {
    // 543: Foreign key {0}. A primary key column can't also be a foreign key
    // child column.
    TestUtil.assertThrowsError(ErrorCode.PK_CANT_BE_FK, () => {
      const tableBuilder = new TableBuilder('Table');
      tableBuilder.addColumn('id', Type.INTEGER)
          .addPrimaryKey(['id'])
          .addForeignKey('fk_id', {
            local: 'id',
            ref: 'OtherTable.id',
          });
      return tableBuilder.getSchema();
    });
  });

  it('throws_PrimaryKeyDuplicateIndex', () => {
    // 544: Duplicate primary key index found at {0},
    // Testing single column primary key.
    TestUtil.assertThrowsError(ErrorCode.DUPLICATE_PK, () => {
      const tableBuilder = new TableBuilder('Table');
      tableBuilder.addColumn('id', Type.INTEGER)
          .addPrimaryKey(['id'])
          .addIndex('idx_id', ['id'], false, Order.ASC);
      return tableBuilder.getSchema();
    });

    // Testing multi column primary key.
    TestUtil.assertThrowsError(ErrorCode.DUPLICATE_PK, () => {
      const tableBuilder = new TableBuilder('Table');
      tableBuilder.addColumn('id1', Type.INTEGER)
          .addColumn('id2', Type.INTEGER)
          .addPrimaryKey(['id1', 'id2'])
          .addIndex('idx_id', ['id1', 'id2'], false, Order.ASC);
      return tableBuilder.getSchema();
    });
  });

  it('isUnique_CrossColumnPk', () => {
    const tableBuilder = new TableBuilder('Table');
    tableBuilder.addColumn('id1', Type.NUMBER)
        .addColumn('id2', Type.NUMBER)
        .addColumn('email', Type.STRING)
        .addColumn('maxSalary', Type.NUMBER)
        .addPrimaryKey(['id1', 'id2']);
    const schema = tableBuilder.getSchema();
    assert.isFalse(schema['id1'].unique);
    assert.isFalse(schema['id2'].unique);
  });

  it('addDuplicateIndexOnFk', () => {
    const tableBuilder = new TableBuilder('Table');
    tableBuilder.addColumn('employeeId', Type.INTEGER)
        .addForeignKey(
            'fkemployeeId', {local: 'employeeId', ref: 'Employee.id'})
        .addIndex('idx_employeeId', ['employeeId'], false, Order.ASC);
    const indexNames: Set<string> = new Set();
    tableBuilder.getSchema().getIndices().forEach(
        (index) => indexNames.add(index.name));
    assert.isTrue(indexNames.has('fkemployeeId'));
    assert.isTrue(indexNames.has('idx_employeeId'));
  });

  it('fkChildColumnIndex_Unique', () => {
    // Case 1: addUnique called before addForeignKey.
    const tableBuilder1 = new TableBuilder('Table1');
    tableBuilder1.addColumn('employeeId', Type.INTEGER)
        .addUnique('uq_employeeId', ['employeeId'])
        .addForeignKey(
            'fkemployeeId', {local: 'employeeId', ref: 'Employee.id'});
    const fkIndexSchema1 =
        tableBuilder1.getSchema()['employeeId'].getIndices()[1];
    assert.equal('fkemployeeId', fkIndexSchema1.name);
    assert.isTrue(fkIndexSchema1.isUnique);

    // Case 2: addUnique called after addForeignKey.
    const tableBuilder2 = new TableBuilder('Table2');
    tableBuilder2.addColumn('employeeId', Type.INTEGER)
        .addForeignKey(
            'fkemployeeId', {local: 'employeeId', ref: 'Employee.id'})
        .addUnique('uq_employeeId', ['employeeId']);
    const fkIndexSchema2 =
        tableBuilder2.getSchema()['employeeId'].getIndices()[0];
    assert.equal('fkemployeeId', fkIndexSchema2.name);
    assert.isTrue(fkIndexSchema2.isUnique);
  });

  it('fkChildColumnIndex_NonUnique', () => {
    // Case: Foreign key child column is not unique.
    const tableBuilder = new TableBuilder('Table');
    tableBuilder.addColumn('employeeId', Type.INTEGER)
        .addForeignKey(
            'fkemployeeId', {local: 'employeeId', ref: 'Employee.id'})
        .addIndex('idx_employeeId', ['employeeId'], false, Order.ASC);
    const fkIndexSchema = tableBuilder.getSchema().getIndices()[0];
    assert.equal('fkemployeeId', fkIndexSchema.name);
    assert.isFalse(fkIndexSchema.isUnique);
  });

  it('createRow_DefaultValues', () => {
    const getSchema = () => {
      const tableBuilder = new TableBuilder('Table');
      tableBuilder.addColumn('integer', Type.INTEGER)
          .addColumn('number', Type.NUMBER)
          .addColumn('string', Type.STRING)
          .addColumn('boolean', Type.BOOLEAN)
          .addColumn('datetime', Type.DATE_TIME)
          .addColumn('arraybuffer', Type.ARRAY_BUFFER)
          .addColumn('object', Type.OBJECT);
      return tableBuilder.getSchema();
    };

    const tableSchema = getSchema();
    const row = tableSchema.createRow();
    assert.isTrue(row instanceof Row);

    const payload = row.payload();
    assert.isNull(payload['arraybuffer']);
    assert.isFalse(payload['boolean']);
    assert.equal(0, payload['datetime'].getTime());
    assert.equal(0, payload['integer']);
    assert.equal(0, payload['number']);
    assert.isNull(payload['object']);
    assert.equal('', payload['string']);
  });

  it('createRow_DefaultValues_Nullable', () => {
    const getSchema = () => {
      const tableBuilder = new TableBuilder('Table');
      tableBuilder.addColumn('integer', Type.INTEGER)
          .addColumn('number', Type.NUMBER)
          .addColumn('string', Type.STRING)
          .addColumn('boolean', Type.BOOLEAN)
          .addColumn('datetime', Type.DATE_TIME)
          .addColumn('arraybuffer', Type.ARRAY_BUFFER)
          .addColumn('object', Type.OBJECT)
          .addNullable([
            'integer',
            'number',
            'string',
            'boolean',
            'datetime',
            'arraybuffer',
            'object',
          ]);
      return tableBuilder.getSchema();
    };

    const tableSchema = getSchema();
    const row = tableSchema.createRow();
    assert.isTrue(row instanceof Row);

    const expectedPayload = {
      arraybuffer: null,
      boolean: null,
      datetime: null,
      integer: null,
      number: null,
      object: null,
      string: null,
    };
    const payload = row.payload();
    assert.deepEqual(expectedPayload, payload);
  });

  it('keyOfIndex_SingleKey', () => {
    const getSchema = () => {
      const tableBuilder = new TableBuilder('Table');
      tableBuilder.addColumn('integer', Type.INTEGER)
          .addColumn('number', Type.NUMBER)
          .addColumn('string', Type.STRING)
          .addColumn('datetime', Type.DATE_TIME)
          .addIndex('idx_datetime', ['datetime'])
          .addIndex('idx_integer', ['integer'])
          .addIndex('idx_number', ['number'])
          .addIndex('idx_string', ['string']);
      return tableBuilder.getSchema();
    };

    const tableSchema = getSchema();
    const row = tableSchema.createRow({
      datetime: new Date(999),
      integer: 2,
      number: 3,
      string: 'bar',
    });
    assert.equal(999, row.keyOfIndex('Table.idx_datetime'));
    assert.equal(2, row.keyOfIndex('Table.idx_integer'));
    assert.equal(3, row.keyOfIndex('Table.idx_number'));
    assert.equal('bar', row.keyOfIndex('Table.idx_string'));
    assert.equal(row.id(), row.keyOfIndex('Table.#'));
  });

  it('keyOfIndex_NullableField', () => {
    const getSchema = () => {
      const tableBuilder = new TableBuilder('Table');
      tableBuilder.addColumn('integer', Type.INTEGER)
          .addColumn('number', Type.NUMBER)
          .addColumn('string', Type.STRING)
          .addColumn('datetime', Type.DATE_TIME)
          .addColumn('boolean', Type.BOOLEAN)
          .addNullable(['datetime', 'integer', 'number', 'string', 'boolean'])
          .addIndex('idx_datetime', ['datetime'])
          .addIndex('idx_integer', ['integer'])
          .addIndex('idx_number', ['number'])
          .addIndex('idx_string', ['string'])
          .addIndex('idx_boolean', ['boolean']);
      return tableBuilder.getSchema();
    };

    const tableSchema = getSchema();
    const row = tableSchema.createRow();
    tableSchema.getIndices().forEach(
        (indexSchema) =>
            assert.isNull(row.keyOfIndex(indexSchema.getNormalizedName())));
  });

  it('keyOfIndex_CrossColumnKey', () => {
    const getSchema = () => {
      const tableBuilder = new TableBuilder('Table');
      tableBuilder.addColumn('integer', Type.INTEGER)
          .addColumn('number', Type.NUMBER)
          .addColumn('string', Type.STRING)
          .addColumn('datetime', Type.DATE_TIME)
          .addColumn('boolean', Type.BOOLEAN)
          .addPrimaryKey(['string', 'integer'])
          .addIndex('idx_NumberInteger', ['number', 'integer'])
          .addIndex('idx_NumberIntegerString', ['number', 'integer', 'string'])
          .addIndex('idb_DateTimeString', ['datetime', 'string'])
          .addIndex('idb_BooleanString', ['boolean', 'string']);
      return tableBuilder.getSchema();
    };

    const tableSchema = getSchema();
    const row = tableSchema.createRow({
      boolean: true,
      datetime: new Date(999),
      integer: 2,
      number: 3,
      string: 'bar',
    });
    assert.equal(row.id(), row.keyOfIndex(tableSchema.getRowIdIndexName()));

    const indices = tableSchema.getIndices();
    const pkIndexSchema = indices[0];
    assert.sameMembers(
        ['bar', 2], row.keyOfIndex(pkIndexSchema.getNormalizedName()) as any[]);

    const numberStringIndexSchema = indices[1];
    assert.sameMembers(
        [3, 2],
        row.keyOfIndex(numberStringIndexSchema.getNormalizedName()) as any[]);

    const numberIntegerStringIndexSchema = indices[2];
    assert.sameMembers(
        [3, 2, 'bar'],
        row.keyOfIndex(numberIntegerStringIndexSchema.getNormalizedName()) as
            any[]);

    const dateTimeStringIndexSchema = indices[3];
    assert.sameMembers(
        [999, 'bar'],
        row.keyOfIndex(dateTimeStringIndexSchema.getNormalizedName()) as any[]);

    const booleanStringIndexSchema = indices[4];
    assert.sameMembers(
        [1, 'bar'],
        row.keyOfIndex(booleanStringIndexSchema.getNormalizedName()) as any[]);
  });

  it('serialization', () => {
    const getSchema = () => {
      const tableBuilder = new TableBuilder('Table');
      tableBuilder.addColumn('number', Type.NUMBER)
          .addColumn('string', Type.STRING)
          .addColumn('arraybuffer', Type.ARRAY_BUFFER)
          .addColumn('datetime', Type.DATE_TIME)
          .addColumn('object', Type.OBJECT)
          .addNullable(['datetime']);
      return tableBuilder.getSchema();
    };

    const tableSchema = getSchema();
    const row =
        tableSchema.createRow({number: 1, string: 'bar', arraybuffer: null});
    const expected = {
      arraybuffer: null,
      datetime: null,
      number: 1,
      object: null,
      string: 'bar',
    };

    assert.deepEqual(expected, row.toDbPayload());
    assert.deepEqual(
        expected, tableSchema.deserializeRow(row.serialize()).payload());
  });
});
