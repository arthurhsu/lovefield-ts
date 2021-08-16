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
import {Modification} from './modification';

export class TableDiff {
  private added: Map<number, Row>;
  private modified: Map<number, Modification>;
  private deleted: Map<number, Row>;

  constructor(private name: string) {
    this.added = new Map();
    this.modified = new Map();
    this.deleted = new Map();
  }

  getName(): string {
    return this.name;
  }

  getAdded(): Map<number, Row> {
    return this.added;
  }

  getModified(): Map<number, Modification> {
    return this.modified;
  }

  getDeleted(): Map<number, Row> {
    return this.deleted;
  }

  add(row: Row): void {
    if (this.deleted.has(row.id())) {
      const modification: Modification = [
        this.deleted.get(row.id()) as unknown as Row,
        row,
      ];
      this.modified.set(row.id(), modification);
      this.deleted.delete(row.id());
    } else {
      this.added.set(row.id(), row);
    }
  }

  modify(modification: Modification): void {
    const oldValue = modification[0] as unknown as Row;
    const newValue = modification[1] as unknown as Row;
    assert(
      oldValue.id() === newValue.id(),
      'Row ID mismatch between old/new values.'
    );
    const id = oldValue.id();

    if (this.added.has(id)) {
      this.added.set(id, newValue);
    } else if (this.modified.has(id)) {
      const overallModification: Modification = [
        (this.modified.get(id) as unknown as Modification)[0],
        newValue,
      ];
      this.modified.set(id, overallModification);
    } else {
      this.modified.set(id, modification);
    }
  }

  delete(row: Row): void {
    if (this.added.has(row.id())) {
      this.added.delete(row.id());
    } else if (this.modified.has(row.id())) {
      const originalRow = (
        this.modified.get(row.id()) as unknown as Modification
      )[0] as unknown as Row;
      this.modified.delete(row.id());
      this.deleted.set(row.id(), originalRow);
    } else {
      this.deleted.set(row.id(), row);
    }
  }

  // Merge another diff into this one.
  merge(other: TableDiff): void {
    other.added.forEach(row => this.add(row));
    other.modified.forEach(modification => this.modify(modification));
    other.deleted.forEach(row => this.delete(row));
  }

  // Transforms each changes included in this diff (insertion, modification,
  // deletion) as a pair of before and after values.
  // Example addition:     [null, rowValue]
  // Example modification: [oldRowValue, newRowValue]
  // Example deletion      [oldRowValue, null]
  getAsModifications(): Modification[] {
    const modifications: Modification[] = [];

    this.added.forEach(row =>
      modifications.push([/* then */ null, /* now */ row])
    );
    this.modified.forEach(modification => modifications.push(modification));
    this.deleted.forEach(row =>
      modifications.push([/* then */ row, /* now */ null])
    );

    return modifications;
  }

  toString(): string {
    return (
      `[${Array.from(this.added.keys()).toString()}], ` +
      `[${Array.from(this.modified.keys()).toString()}], ` +
      `[${Array.from(this.deleted.keys()).toString()}]`
    );
  }

  // Reverses this set of changes. Useful for reverting changes after they have
  // been applied.
  getReverse(): TableDiff {
    const reverseDiff = new TableDiff(this.name);

    this.added.forEach(row => reverseDiff.delete(row));
    this.deleted.forEach(row => reverseDiff.add(row));
    this.modified.forEach(modification => {
      reverseDiff.modify([modification[1], modification[0]]);
    });

    return reverseDiff;
  }

  isEmpty(): boolean {
    return (
      this.added.size === 0 &&
      this.deleted.size === 0 &&
      this.modified.size === 0
    );
  }
}
