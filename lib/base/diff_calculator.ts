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

import {Relation} from '../proc/relation';
import {RelationEntry} from '../proc/relation_entry';
import {SelectContext} from '../query/select_context';
import {Column} from '../schema/column';
import {MathHelper} from '../structs/math_helper';
import {ChangeRecord} from './change_record';
import {Type} from './enum';
import {EvalRegistry, EvalType} from './eval';

// A DiffCalculator is responsible for detecting and applying the difference
// between old and new results for a given query.
export class DiffCalculator {
  private evalRegistry: EvalRegistry;
  private query: SelectContext;
  private observableResults: any[];
  private columns: Column[];

  constructor(query: SelectContext, observableResults: any[]) {
    this.evalRegistry = EvalRegistry.get();
    this.query = query;
    this.observableResults = observableResults;
    this.columns = this.detectColumns();
  }

  // Detects the diff between old and new results, and applies it to the
  // observed array, which triggers observers to be notified.
  // NOTE: Following logic does not detect modifications. A modification is
  // detected as a deletion and an insertion.
  // Also the implementation below is calculating longestCommonSubsequence
  // twice, with different collectorFn each time, because comparisons are done
  // based on object reference, there might be a cheaper way, such that
  // longestCommonSubsequence is only called once.
  public applyDiff(oldResults: Relation, newResults: Relation): object[] {
    const oldEntries = oldResults === null ? [] : oldResults.entries;

    // Detecting and applying deletions.
    const longestCommonSubsequenceLeft = MathHelper.longestCommonSubsequence(
        oldEntries, newResults.entries, this.comparator.bind(this),
        (indexLeft, indexRight) => oldEntries[indexLeft]);

    const changeRecords: object[] = [];
    let commonIndex = 0;
    for (let i = 0; i < oldEntries.length; i++) {
      const entry = oldEntries[i];
      if (longestCommonSubsequenceLeft[commonIndex] === entry) {
        commonIndex++;
        continue;
      } else {
        const removed = this.observableResults.splice(commonIndex, 1);
        const changeRecord =
            this.createChangeRecord(i, removed, 0, this.observableResults);
        changeRecords.push(changeRecord);
      }
    }

    // Detecting and applying additions.
    const longestCommonSubsequenceRight = MathHelper.longestCommonSubsequence(
        oldEntries, newResults.entries, this.comparator.bind(this),
        (indexLeft, indexRight) => newResults.entries[indexRight]);

    commonIndex = 0;
    for (let i = 0; i < newResults.entries.length; i++) {
      const entry = newResults.entries[i];
      if (longestCommonSubsequenceRight[commonIndex] === entry) {
        commonIndex++;
        continue;
      } else {
        this.observableResults.splice(i, 0, entry.row.payload());
        const changeRecord =
            this.createChangeRecord(i, [], 1, this.observableResults);
        changeRecords.push(changeRecord);
      }
    }

    return changeRecords;
  }

  // Detects the columns present in each result entry.
  private detectColumns(): Column[] {
    if (this.query.columns.length > 0) {
      return this.query.columns;
    } else {
      // Handle the case where all columns are being projected.
      const columns: Column[] = [];
      this.query.from.forEach((table) => {
        table.getColumns().forEach((column) => columns.push(column));
      });
      return columns;
    }
  }

  // The comparator function to use for determining whether two entries are the
  // same. Returns whether the two entries are identical, taking only into
  // account the columns that are being projected.
  private comparator(left: RelationEntry, right: RelationEntry): boolean {
    return this.columns.every((column) => {
      // For OBJECT and ARRAY_BUFFER columns, don't bother detecting changes
      // within the object. Trigger observers only if the object reference
      // changed.
      if (column.getType() === Type.OBJECT ||
          column.getType() === Type.ARRAY_BUFFER) {
        return left.getField(column) === right.getField(column);
      }

      const evalFn =
          this.evalRegistry.getEvaluator(column.getType(), EvalType.EQ);
      return evalFn(left.getField(column), right.getField(column));
    }, this);
  }

  // Creates a new change record object.
  // |index| is the index that was affected.
  // |removed| is an array holding the elements that were removed.
  // |addedCount| is the number of elements added to the observed array.
  // |object| is the array that is being observed.
  private createChangeRecord(
      index: number, removed: any[], addedCount: number,
      object: any[]): ChangeRecord {
    return {
      addedCount: addedCount,
      index: index,
      object: object,
      removed: removed,
      type: 'splice',
    };
  }
}
