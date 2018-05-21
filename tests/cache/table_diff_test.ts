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
import {Modification} from '../../lib/cache/modification';
import {TableDiff} from '../../lib/cache/table_diff';
import {Table} from '../../lib/schema/table';
import {getMockSchemaBuilder} from '../../testing/mock_schema_builder';

const assert = chai.assert;

describe('TableDiff', () => {
  let table: Table;

  before(() => {
    table = getMockSchemaBuilder().getSchema().table('tableA');
  });

  // Tests the behavior of the TableDiff class under mulitple
  // additions, modifications and deletions.
  it('multipleOperations', () => {
    const diff = new TableDiff(table.getName());

    // Assuming that 1 and 2 are the only row IDs that reside in the table prior
    // to this diff.
    const row1Old = table.createRow({id: 'pk1', name: 'DummyName'});
    row1Old.assignRowId(1);
    const row1New = table.createRow({id: 'pk1', name: 'UpdatedDummyName'});
    row1New.assignRowId(1);

    const row2Old = table.createRow({id: 'pk2', name: 'DummyName'});
    row2Old.assignRowId(2);
    const row2New = table.createRow({id: 'pk2', name: 'UpdatedDummyName'});
    row2New.assignRowId(2);

    const row3 = table.createRow({id: 'pk3', name: 'DummyName'});
    row3.assignRowId(3);
    const row4 = table.createRow({id: 'pk4', name: 'DummyName'});
    row4.assignRowId(4);

    const row5Old = table.createRow({id: 'pk5', name: 'DummyName'});
    row5Old.assignRowId(5);
    const row5New = table.createRow({id: 'pk5', name: 'UpdatedDummyName'});
    row5New.assignRowId(5);

    // No changes have happened yet.
    assert.equal('[], [], []', diff.toString());

    // Adding a new row to the diff.
    diff.add(row3);
    assert.equal('[3], [], []', diff.toString());

    // Deleting the row that was added befdore. Testing that "delete" is taking
    // into account whether the row ID existed originally, and therefore
    // expecting an empty diff.
    diff.delete(row3);
    assert.equal('[], [], []', diff.toString());

    // Adding row3 and row4, modifying existing row1.
    diff.add(row4);
    diff.add(row5Old);
    diff.modify([row1Old, row1New]);
    assert.equal('[4,5], [1], []', diff.toString());

    // Deleting an existing rowID from the table.
    diff.delete(row2Old);
    assert.equal('[4,5], [1], [2]', diff.toString());

    // Adding the row that was deleted previously. Testing that "add" is
    // converted to "modify" if the row ID existed originally.
    diff.add(row2New);
    assert.equal('[4,5], [1,2], []', diff.toString());
    const modification = diff.getModified().get(2) || null;
    assert.isNotNull(modification);
    assert.equal(
        row2Old.payload()['name'],
        ((modification as Modification)[0] as Row).payload()['name']);
    assert.equal(
        row2New.payload()['name'],
        ((modification as Modification)[1] as Row).payload()['name']);

    // Test that "modify" is preserved as "add" if the row ID has been
    // previously added.
    diff.modify([row5Old, row5New]);
    assert.equal('[4,5], [1,2], []', diff.toString());

    // Deleting an existing modified row. Checking that the original value is
    // recorded and not the one after the modification.
    diff.delete(row2New);
    assert.equal('[4,5], [1], [2]', diff.toString());
    const deletedRow = diff.getDeleted().get(row2New.id()) || null;
    assert.isNotNull(deletedRow);
    assert.equal(
        row2Old.payload()['name'], (deletedRow as Row).payload()['name']);
  });

  // Test reversing an empty diff.
  it('getReversed_Empty', () => {
    const original = new TableDiff(table.getName());
    assert.isTrue(original.isEmpty());
    const reverse = original.getReverse();
    assert.isTrue(reverse.isEmpty());
  });

  // Test reversing a diff with only additions.
  it('getReversed_Add', () => {
    const original = new TableDiff(table.getName());
    const row1 = table.createRow({id: 'pk1', name: 'DummyName'});
    row1.assignRowId(1);
    const row2 = table.createRow({id: 'pk2', name: 'DummyName'});
    row2.assignRowId(2);
    original.add(row1);
    original.add(row2);

    assert.equal(2, original.getAdded().size);
    assert.equal(0, original.getModified().size);
    assert.equal(0, original.getDeleted().size);

    const reverse = original.getReverse();
    assert.equal(0, reverse.getAdded().size);
    assert.equal(0, reverse.getModified().size);
    assert.equal(2, reverse.getDeleted().size);

    assert.sameMembers(
        Array.from(original.getAdded().keys()),
        Array.from(reverse.getDeleted().keys()),
    );
  });

  // Test reversing a diff with only deletions.
  it('getReversed_Delete', () => {
    const original = new TableDiff(table.getName());
    const row1 = table.createRow({id: 'pk1', name: 'DummyName'});
    row1.assignRowId(1);
    const row2 = table.createRow({id: 'pk2', name: 'DummyName'});
    row2.assignRowId(2);
    original.delete(row1);
    original.delete(row2);

    assert.equal(0, original.getAdded().size);
    assert.equal(0, original.getModified().size);
    assert.equal(2, original.getDeleted().size);

    const reverse = original.getReverse();
    assert.equal(2, reverse.getAdded().size);
    assert.equal(0, reverse.getModified().size);
    assert.equal(0, reverse.getDeleted().size);

    assert.sameMembers(
        Array.from(original.getDeleted().keys()),
        Array.from(reverse.getAdded().keys()),
    );
  });

  // Test reversing a diff with only modifications.
  it('getReversed_Modify', () => {
    const original = new TableDiff(table.getName());
    const rowOld = table.createRow({id: 'pk1', name: 'DummyName'});
    rowOld.assignRowId(1);
    const rowNew = table.createRow({id: 'pk2', name: 'OtherDummyName'});
    rowNew.assignRowId(1);
    original.modify([rowOld, rowNew]);

    assert.equal(0, original.getAdded().size);
    assert.equal(1, original.getModified().size);
    assert.equal(0, original.getDeleted().size);

    const reverse = original.getReverse();
    assert.equal(0, reverse.getAdded().size);
    assert.equal(1, reverse.getModified().size);
    assert.equal(0, reverse.getDeleted().size);

    assert.sameMembers(
        Array.from(original.getModified().keys()),
        Array.from(reverse.getModified().keys()),
    );

    reverse.getModified().forEach((modification, rowId) => {
      const originalModification = original.getModified().get(rowId) || null;
      assert.isNotNull(originalModification);
      assert.equal(
          ((originalModification as Modification)[0] as Row).payload()['name'],
          ((modification as Modification)[1] as Row).payload()['name']);
    });
  });

  it('getAsModifications', () => {
    const diff = new TableDiff(table.getName());

    const row1 = table.createRow({id: 'pk1', name: 'DummyName'});
    row1.assignRowId(1);
    const row2 = table.createRow({id: 'pk2', name: 'DummyName'});
    row2.assignRowId(2);
    const row3Before = table.createRow({id: 'pk3', name: 'DummyName'});
    row3Before.assignRowId(3);
    const row3After = table.createRow({id: 'pk3', name: 'OtherDummyName'});
    row3After.assignRowId(3);

    diff.add(row1);
    diff.modify([row3Before, row3After]);
    diff.delete(row2);

    const modifications = diff.getAsModifications();
    assert.equal(3, modifications.length);
    assert.sameMembers([null, row1], modifications[0]);
    assert.sameMembers([row3Before, row3After], modifications[1]);
    assert.sameMembers([row2, null], modifications[2]);
  });
});
