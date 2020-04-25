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
import {Row} from '../../lib/base/row';
import {RelationEntry} from '../../lib/proc/relation_entry';

const assert = chai.assert;

describe('RelationEntry', () => {
  it('combineEntries', () => {
    const row1 = new Row(Row.DUMMY_ID, {foo: 'FOO'});
    const row2 = new Row(Row.DUMMY_ID, {bar: 'BAR'});
    const row3 = new Row(Row.DUMMY_ID, {baz: 'BAZ'});

    const entry1 = new RelationEntry(row1, false);
    const entry2 = new RelationEntry(row2, false);
    const entry3 = new RelationEntry(row3, false);

    // First combining two non-prefixed entries.
    const combinedEntry12 = RelationEntry.combineEntries(
      entry1,
      ['Table1'],
      entry2,
      ['Table2']
    );

    assert.equal(2, Object.keys(combinedEntry12.row.payload()).length);
    let table1Payload = combinedEntry12.row.payload()['Table1'];
    assert.deepEqual(row1.payload(), table1Payload);
    let table2Payload = combinedEntry12.row.payload()['Table2'];
    assert.deepEqual(row2.payload(), table2Payload);

    // Now combining an non-prefixed and a prefixed entry.
    const combinedEntry123 = RelationEntry.combineEntries(
      combinedEntry12,
      ['Table1', 'Table2'],
      entry3,
      ['Table3']
    );
    assert.equal(3, Object.keys(combinedEntry123.row.payload()).length);

    table1Payload = combinedEntry123.row.payload()['Table1'];
    assert.deepEqual(row1.payload(), table1Payload);
    table2Payload = combinedEntry123.row.payload()['Table2'];
    assert.deepEqual(row2.payload(), table2Payload);
    const table3Payload = combinedEntry123.row.payload()['Table3'];
    assert.deepEqual(row3.payload(), table3Payload);
  });
});
