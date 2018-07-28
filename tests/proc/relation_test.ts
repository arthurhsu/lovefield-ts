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

import * as chai from 'chai';

import {Row} from '../../lib/base/row';
import {Relation} from '../../lib/proc/relation';
import {RelationEntry} from '../../lib/proc/relation_entry';
import {BaseColumn} from '../../lib/schema/base_column';
import {BaseTable} from '../../lib/schema/base_table';
import {Database} from '../../lib/schema/database';
import {Table} from '../../lib/schema/table';
import {getMockSchemaBuilder} from '../../testing/mock_schema_builder';

const assert = chai.assert;

describe('Relation', () => {
  let schema: Database;

  before(() => {
    schema = getMockSchemaBuilder().getSchema();
  });

  it('fromRows', () => {
    const rows: Row[] = [];
    for (let i = 0; i < 10; i++) {
      rows.push(Row.create());
    }

    const table = 'dummyTable';
    const relation = Relation.fromRows(rows, [table]);

    assert.isTrue(relation.getTables().indexOf(table) !== -1);
    relation.entries.forEach((entry, index) => {
      assert.equal(rows[index], entry.row);
    });
  });

  // Asserts that the given column is populated with the given value.
  function assertPopulated(
      entry: RelationEntry, column: BaseColumn, expectedValue: string,
      isPrefixApplied: boolean): void {
    assert.equal(expectedValue, entry.getField(column));

    const tableName = (column.getTable() as BaseTable).getEffectiveName();
    const value = isPrefixApplied ?
        entry.row.payload()[tableName][column.getName()] :
        entry.row.payload()[column.getName()];
    assert.equal(expectedValue, value);
  }

  function checkGetSetValue_MultipleTables(t: Table[]): void {
    const tables = t as BaseTable[];
    const rows: Row[] = new Array(10);
    for (let i = 0; i < rows.length; i++) {
      rows[i] = Row.create();
    }

    const tableNames = tables.map((table) => table.getEffectiveName());
    const relation = Relation.fromRows(rows, tableNames);
    relation.entries.forEach((entry) => {
      assert.isEmpty(entry.row.payload());

      // Tests setting the value when no previous value is specified.
      const field1 = 'Hello';
      entry.setField(tables[0]['name'], field1);
      const field2 = 'World';
      entry.setField(tables[1]['name'], field2);
      assertPopulated(
          entry, tables[0]['name'], field1, /* isPrefixApplied */ true);
      assertPopulated(
          entry, tables[1]['name'], field2, /* isPrefixApplied */ true);

      // Tests setting the value when a previous value is specified.
      const field3 = 'olleH';
      entry.setField(tables[0]['name'], field3);
      const field4 = 'dlroW';
      entry.setField(tables[1]['name'], field4);
      assertPopulated(
          entry, tables[0]['name'], field3, /* isPrefixApplied */ true);
      assertPopulated(
          entry, tables[1]['name'], field4, /* isPrefixApplied */ true);
    });
  }

  it('getSetValue_MultipleTables', () => {
    const tables = schema.tables().slice(0, 2);
    checkGetSetValue_MultipleTables(tables);
  });

  it('getSetValue_MultipleTables_Alias', () => {
    const table0 = schema.table('tableA').as('table0');
    const table1 = schema.table('tableB').as('table1');
    checkGetSetValue_MultipleTables([table0, table1]);
  });

  it('getSetValue_SingleTable', () => {
    const rows: Row[] = new Array(10);
    for (let i = 0; i < rows.length; i++) {
      rows[i] = Row.create();
    }

    const table = schema.table('tableA');
    const relation = Relation.fromRows(rows, [table.getName()]);
    relation.entries.forEach((entry) => {
      assert.isEmpty(entry.row.payload());

      const field1 = 'HelloWorld';
      entry.setField(table['name'], field1);
      assertPopulated(
          entry, table['name'], field1, /* isPrefixApplied */ false);

      const field2 = 'dlroWolleH';
      entry.setField(table['name'], field2);
      assertPopulated(
          entry, table['name'], field2, /* isPrefixApplied */ false);
    });
  });

  it('setField_WithAlias', () => {
    const rows: Row[] = new Array(10);
    for (let i = 0; i < rows.length; i++) {
      rows[i] = Row.create();
    }

    const table = schema.table('tableA');
    const col = table['name'].as('nickName');

    const relation = Relation.fromRows(rows, [table.getName()]);
    relation.entries.forEach((entry) => {
      assert.isEmpty(entry.row.payload());

      const field1 = 'HelloWorld';
      entry.setField(col, field1);
      assert.equal(field1, entry.row.payload()[col.getAlias()]);
      assert.equal(field1, entry.getField(col));
    });
  });

  it('intersection_Empty', () => {
    const relationEntries: RelationEntry[] = [];
    for (let i = 0; i < 5; i++) {
      relationEntries.push(new RelationEntry(Row.create(), false));
    }

    const relation1 = new Relation(relationEntries, ['tableA']);
    const relation2 = new Relation([], ['tableA']);

    const intersection = Relation.intersect([relation1, relation2]);
    assert.equal(0, intersection.entries.length);
  });

  it('intersection', () => {
    const entries: RelationEntry[] = new Array(6);
    for (let i = 0; i < entries.length; i++) {
      entries[i] = new RelationEntry(Row.create(), false);
    }

    // Creating 3 relations that only have entry1 in common.
    const tableName = 'dummyTable';
    const relation1 = new Relation(
        [entries[0], entries[1], entries[2], entries[3]], [tableName]);
    const relation2 =
        new Relation([entries[0], entries[3], entries[5]], [tableName]);
    const relation3 =
        new Relation([entries[0], entries[4], entries[5]], [tableName]);

    const intersection = Relation.intersect([relation1, relation2, relation3]);
    assert.equal(1, intersection.entries.length);
    assert.isTrue(intersection.isCompatible(relation1));
    assert.equal(entries[0].id, intersection.entries[0].id);
  });

  it('union', () => {
    const entries: RelationEntry[] = new Array(6);
    for (let i = 0; i < entries.length; i++) {
      entries[i] = new RelationEntry(Row.create(), false);
    }

    // Creating 3 relations.
    const tableName = 'dummyTable';
    const relation1 = new Relation(
        [entries[0], entries[1], entries[2], entries[3]], [tableName]);
    const relation2 = new Relation([entries[5]], [tableName]);
    const relation3 = new Relation([entries[4], entries[5]], [tableName]);

    const union = Relation.union([relation1, relation2, relation3]);
    assert.equal(6, union.entries.length);
    assert.isTrue(union.isCompatible(relation1));
  });
});
