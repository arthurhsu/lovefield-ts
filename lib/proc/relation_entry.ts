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

import {assert} from '../base/assert';
import {Row} from '../base/row';
import {BaseColumn} from '../schema/base_column';
import {BaseTable} from '../schema/base_table';

/**
 * Each RelationEntry represents a row that is passed from one execution step
 * to another and does not necessarily correspond to a physical row in a DB
 * table (as it can be the result of a cross-product/join operation).
 */
export class RelationEntry {
  // Combines two entries into a single entry.
  public static combineEntries(
      leftEntry: RelationEntry, leftEntryTables: string[],
      rightEntry: RelationEntry, rightEntryTables: string[]): RelationEntry {
    const result = {};
    const mergeEntry = (entry: RelationEntry, entryTables: string[]) => {
      if (entry.isPrefixApplied) {
        const payload = entry.row.payload();
        Array.from(Object.keys(payload)).forEach((prefix) => {
          result[prefix] = payload[prefix];
        });
      } else {
        assert(
            !result.hasOwnProperty(entryTables[0]),
            'Attempted to join table with itself, without using table alias, ' +
                'or same alias ' + entryTables[0] +
                'is reused for multiple tables.');

        // Since the entry is not prefixed, all attributes come from a single
        // table.
        result[entryTables[0]] = entry.row.payload();
      }
    };

    mergeEntry(leftEntry, leftEntryTables);
    mergeEntry(rightEntry, rightEntryTables);

    const row = new Row(Row.DUMMY_ID, result);
    return new RelationEntry(row, true);
  }

  // The ID to assign to the next entry that will be created.
  private static nextId = 0;

  private static getNextId(): number {
    return RelationEntry.nextId++;
  }

  public id: number;

  // |isPrefixApplied| Whether the payload in this entry is using prefixes for
  // each attribute. This happens when this entry is the result of a relation
  // join.
  constructor(readonly row: Row, private isPrefixApplied: boolean) {
    this.id = RelationEntry.getNextId();
  }

  public getField(column: BaseColumn): any {
    // Attempting to get the field from the aliased location first, since it is
    // not guaranteed that setField() has been called for this instance. If not
    // found then look for it in its normal location.
    const alias = column.getAlias();
    if (alias !== null && this.row.payload().hasOwnProperty(alias)) {
      return this.row.payload()[alias];
    }

    if (this.isPrefixApplied) {
      return this.row.payload()[(column.getTable() as BaseTable)
                                    .getEffectiveName()][column.getName()];
    } else {
      return this.row.payload()[column.getName()];
    }
  }

  public setField(column: BaseColumn, value: any): void {
    const alias = column.getAlias();
    if (alias !== null) {
      this.row.payload()[alias] = value;
      return;
    }

    if (this.isPrefixApplied) {
      const tableName = (column.getTable() as BaseTable).getEffectiveName();
      let containerObj = this.row.payload()[tableName];
      if (!(containerObj !== undefined && containerObj !== null)) {
        containerObj = {};
        this.row.payload()[tableName] = containerObj;
      }
      containerObj[column.getName()] = value;
    } else {
      this.row.payload()[column.getName()] = value;
    }
  }
}
