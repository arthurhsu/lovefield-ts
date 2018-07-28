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

import {bind, Binder} from '../../lib/base/bind';
import {ErrorCode, Type} from '../../lib/base/enum';
import {EvalType} from '../../lib/base/eval';
import {Row} from '../../lib/base/row';
import {JoinPredicate} from '../../lib/pred/join_predicate';
import {ValuePredicate} from '../../lib/pred/value_predicate';
import {Relation} from '../../lib/proc/relation';
import {BaseColumn} from '../../lib/schema/base_column';
import {BaseTable} from '../../lib/schema/base_table';
import {Builder} from '../../lib/schema/builder';
import {Database} from '../../lib/schema/database';
import {TestUtil} from '../../testing/test_util';

const assert = chai.assert;

describe('ValuePredicate', () => {
  let schema: Database;
  let tableA: BaseTable;
  let tableB: BaseTable;

  function getSchema(): Database {
    const schemaBuilder = new Builder('valuepredicate', 1);
    schemaBuilder.createTable('TableA')
        .addColumn('id', Type.STRING)
        .addColumn('name', Type.STRING);
    schemaBuilder.createTable('TableB')
        .addColumn('id', Type.STRING)
        .addColumn('email', Type.STRING);
    schemaBuilder.createTable('TableC').addColumn('hireDate', Type.DATE_TIME);
    return schemaBuilder.getSchema();
  }

  beforeEach(() => {
    schema = getSchema();
    tableA = schema.table('TableA');
    tableB = schema.table('TableB');
  });

  it('copy', () => {
    const original = new ValuePredicate(tableA['id'], 'myId', EvalType.EQ);
    const copy = original.copy();

    assert.isTrue(copy instanceof ValuePredicate);
    assert.isFalse(original === copy);
    assert.isTrue(original.toString().length > 15);
    assert.equal(original.toString(), copy.toString());
    assert.equal(original.getId(), copy.getId());
  });

  it('getColumns', () => {
    const p = new ValuePredicate(tableA['id'], 'myId', EvalType.EQ);
    assert.sameMembers([tableA['id']], p.getColumns());

    // Test case where optional parameter is provided.
    const columns: BaseColumn[] = [];
    assert.equal(columns, p.getColumns(columns));
    assert.sameMembers([tableA['id']], columns);
  });

  it('getTables', () => {
    const p = new ValuePredicate(tableA['id'], 'myId', EvalType.EQ);
    assert.sameMembers([tableA], Array.from(p.getTables().values()));

    // Test case where optional parameter is provided.
    const tables = new Set<BaseTable>();
    assert.equal(tables, p.getTables(tables));
    assert.sameMembers([tableA], Array.from(tables.values()));
  });

  // Generates sample TableA rows
  function getTableARows(rowCount: number): Row[] {
    const sampleRows: Row[] = new Array(rowCount);

    for (let i = 0; i < rowCount; i++) {
      sampleRows[i] = tableA.createRow({
        id: 'sampleId' + i.toString(),
        name: 'sampleName' + i.toString(),
      });
    }
    return sampleRows;
  }

  function checkEval_Eq(table: BaseTable): void {
    const sampleRow = getTableARows(1)[0];
    const relation = Relation.fromRows([sampleRow], [table.getEffectiveName()]);

    const predicate1 = new ValuePredicate(
        table['id'], (sampleRow.payload() as any).id, EvalType.EQ);
    const finalRelation1 = predicate1.eval(relation);
    assert.equal(1, finalRelation1.entries.length);

    const predicate2 = new ValuePredicate(table['id'], 'otherId', EvalType.EQ);
    const finalRelation2 = predicate2.eval(relation);
    assert.equal(0, finalRelation2.entries.length);
  }

  it('eval_eq', () => {
    checkEval_Eq(tableA);
  });

  it('eval_eq_alias', () => {
    checkEval_Eq(tableA.as('SomeTableAlias') as BaseTable);
  });

  // Testing the case where a ValuePredicate is applied on a relation that is
  // the result of a previous join operation.
  // |table1| must be TableA or alias, |table2| must be TableB or alias.
  function checkEval_Eq_PreviousJoin(
      table1: BaseTable, table2: BaseTable): void {
    const leftRow = table1.createRow({
      id: 'dummyId',
      name: 'dummyName',
    });
    const rightRow1 = table2.createRow({
      email: 'dummyEmail1',
      id: 'dummyId',
    });
    const rightRow2 = table2.createRow({
      email: 'dummyEmail2',
      id: 'dummyId',
    });

    const leftRelation =
        Relation.fromRows([leftRow], [table1.getEffectiveName()]);
    const rightRelation =
        Relation.fromRows([rightRow1, rightRow2], [table2.getEffectiveName()]);

    const joinPredicate =
        new JoinPredicate(table1['id'], table2['id'], EvalType.EQ);
    const joinedRelation =
        joinPredicate.evalRelationsHashJoin(leftRelation, rightRelation, false);

    const valuePredicate = new ValuePredicate(
        table2['email'], rightRow2.payload()['email'], EvalType.EQ);
    const finalRelation = valuePredicate.eval(joinedRelation);
    assert.equal(1, finalRelation.entries.length);
    assert.equal(
        rightRow2.payload()['email'],
        finalRelation.entries[0].getField(table2['email']));
  }

  it('eval_Eq_PreviousJoin', () => {
    checkEval_Eq_PreviousJoin(tableA, tableB);
  });

  it('eval_Eq_PreviousJoin_Alias', () => {
    checkEval_Eq_PreviousJoin(
        tableA.as('table1') as BaseTable, tableB.as('table2') as BaseTable);
  });

  // Tests the conversion of a value predicate to a KeyRange for a column of
  // type STRING.
  it('toKeyRange_String', () => {
    const id1 = 'id1';
    const id2 = 'id2';
    let p = new ValuePredicate(tableA['id'], id1, EvalType.EQ);
    assert.isTrue(p.isKeyRangeCompatible());
    assert.equal('[id1, id1]', p.toKeyRange().toString());

    p = new ValuePredicate(tableA['id'], id1, EvalType.GTE);
    assert.isTrue(p.isKeyRangeCompatible());
    assert.equal('[id1, unbound]', p.toKeyRange().toString());

    p = new ValuePredicate(tableA['id'], id1, EvalType.GT);
    assert.isTrue(p.isKeyRangeCompatible());
    assert.equal('(id1, unbound]', p.toKeyRange().toString());

    p = new ValuePredicate(tableA['id'], id1, EvalType.LTE);
    assert.isTrue(p.isKeyRangeCompatible());
    assert.equal('[unbound, id1]', p.toKeyRange().toString());

    p = new ValuePredicate(tableA['id'], id1, EvalType.LT);
    assert.isTrue(p.isKeyRangeCompatible());
    assert.equal('[unbound, id1)', p.toKeyRange().toString());

    p = new ValuePredicate(tableA['id'], [id1, id2], EvalType.BETWEEN);
    assert.isTrue(p.isKeyRangeCompatible());
    assert.equal('[id1, id2]', p.toKeyRange().toString());

    p.setComplement(true);
    assert.equal('[unbound, id1),(id2, unbound]', p.toKeyRange().toString());
  });

  it('toKeyRange_In_String', () => {
    const values = ['id1', 'id2', 'id3'];
    const p1 = new ValuePredicate(tableA['id'], values, EvalType.IN);
    assert.equal(
        '[id1, id1],[id2, id2],[id3, id3]', p1.toKeyRange().toString());
    p1.setComplement(true);
    assert.equal(
        '[unbound, id1),(id1, id2),(id2, id3),(id3, unbound]',
        p1.toKeyRange().toString());

    const p2 = new ValuePredicate(tableA['id'], values.reverse(), EvalType.IN);
    assert.equal(
        '[id1, id1],[id2, id2],[id3, id3]', p2.toKeyRange().toString());
    p2.setComplement(true);
    assert.equal(
        '[unbound, id1),(id1, id2),(id2, id3),(id3, unbound]',
        p2.toKeyRange().toString());
  });

  // Tests the conversion of a value predicate to a KeyRange for a column of
  // type DATE_TIME.
  it('toKeyRange_DateTime', () => {
    const table = schema.table('TableC');
    const d1 = new Date(1443646468270);

    let p = new ValuePredicate(table['hireDate'], d1, EvalType.EQ);
    assert.isTrue(p.isKeyRangeCompatible());
    assert.equal(
        '[' + d1.getTime() + ', ' + d1.getTime() + ']',
        p.toKeyRange().toString());

    p = new ValuePredicate(table['hireDate'], d1, EvalType.GTE);
    assert.isTrue(p.isKeyRangeCompatible());
    assert.equal('[' + d1.getTime() + ', unbound]', p.toKeyRange().toString());

    p = new ValuePredicate(table['hireDate'], d1, EvalType.GT);
    assert.isTrue(p.isKeyRangeCompatible());
    assert.equal('(' + d1.getTime() + ', unbound]', p.toKeyRange().toString());

    p = new ValuePredicate(table['hireDate'], d1, EvalType.LTE);
    assert.isTrue(p.isKeyRangeCompatible());
    assert.equal('[unbound, ' + d1.getTime() + ']', p.toKeyRange().toString());

    p = new ValuePredicate(table['hireDate'], d1, EvalType.LT);
    assert.isTrue(p.isKeyRangeCompatible());
    assert.equal('[unbound, ' + d1.getTime() + ')', p.toKeyRange().toString());

    const d2 = new Date();
    p = new ValuePredicate(table['hireDate'], [d1, d2], EvalType.BETWEEN);
    assert.isTrue(p.isKeyRangeCompatible());
    assert.equal(
        '[' + d1.getTime() + ', ' + d2.getTime() + ']',
        p.toKeyRange().toString());

    p.setComplement(true);
    assert.equal(
        '[unbound, ' + d1.getTime() + '),(' + d2.getTime() + ', unbound]',
        p.toKeyRange().toString());
  });

  // Tests that isKeyRangeCompatible() returns false when the predicate involves
  // 'null' values.
  it('isKeyRangeCompatible_False', () => {
    const p = new ValuePredicate(tableA['id'], null, EvalType.EQ);
    assert.isFalse(p.isKeyRangeCompatible());
  });

  it('unboundPredicate', () => {
    const sampleRow = getTableARows(1)[0];
    const relation = Relation.fromRows([sampleRow], [tableA.getName()]);

    const binder = bind(1);
    const p = new ValuePredicate(tableA['id'], binder, EvalType.EQ);

    // Predicate shall be unbound.
    assert.isTrue(p.peek() instanceof Binder);

    // Tests binding.
    p.bind([9999, (sampleRow.payload() as any).id]);
    assert.equal((sampleRow.payload() as any).id, p.peek());
    assert.isFalse(p.peek() instanceof Binder);
    const result = p.eval(relation);
    assert.equal(1, result.entries.length);

    // Tests binding to an invalid array throws error.
    TestUtil.assertThrowsError(ErrorCode.BIND_ARRAY_OUT_OF_RANGE, () => {
      p.bind([8888]);
    });
  });

  it('unboundPredicate_Array', () => {
    const sampleRows = getTableARows(3);
    const ids = sampleRows.map((row) => (row.payload() as any).id);
    const relation = Relation.fromRows(sampleRows, [tableA.getName()]);

    const binder = [bind(0), bind(1), bind(2)];
    const p = new ValuePredicate(tableA['id'], binder, EvalType.IN);

    // Tests binding.
    p.bind(ids);
    assert.equal(3, p.eval(relation).entries.length);
  });

  it('copy_UnboundPredicate', () => {
    const sampleRow = getTableARows(1)[0];

    const binder = bind(1);
    const p = new ValuePredicate(tableA['id'], binder, EvalType.EQ);
    const p2: ValuePredicate = p.copy() as ValuePredicate;

    // Both predicates shall be unbound.
    assert.isTrue(p.peek() instanceof Binder);
    assert.isTrue(p2.peek() instanceof Binder);

    // Copying a bounded predicate shall still make it bounded.
    p.bind([9999, (sampleRow.payload() as any).id]);
    const p3: ValuePredicate = p.copy() as ValuePredicate;
    assert.equal((sampleRow.payload() as any).id, p3.peek());

    // The clone should also be able to bind to a new array.
    const sampleRow2 = getTableARows(2)[1];
    p3.bind([9999, (sampleRow2.payload() as any).id]);
    assert.equal((sampleRow2.payload() as any).id, p3.peek());
  });

  it('copy_UnboundPredicate_Array', () => {
    const sampleRows = getTableARows(6);
    const ids = sampleRows.map((row) => (row.payload() as any).id);

    const binder = [bind(0), bind(1), bind(2)];
    const p = new ValuePredicate(tableA['id'], binder, EvalType.IN);
    p.bind(ids);
    const p2: ValuePredicate = p.copy() as ValuePredicate;
    assert.sameDeepOrderedMembers(ids.slice(0, 3), p2.peek());

    // Tests binding.
    p2.bind(ids.slice(3));
    assert.sameDeepOrderedMembers(ids.slice(3), p2.peek());
  });
});
