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

import {ConstraintAction, ConstraintTiming, ErrorCode} from '../base/enum';
import {Exception} from '../base/exception';
import {Global} from '../base/global';
import {Row} from '../base/row';
import {Service} from '../base/service';
import {IndexStore} from '../index/index_store';
import {Key} from '../index/key_range';
import {RuntimeIndex} from '../index/runtime_index';
import {Column} from '../schema/column';
import {Database} from '../schema/database';
import {ForeignKeySpec} from '../schema/foreign_key_spec';
import {Index} from '../schema/index';
import {Table} from '../schema/table';
import {MapSet} from '../structs/map_set';
import {Cache} from './cache';
import {CascadeDeletion, CascadeUpdate, CascadeUpdateItem} from './cascade';
import {Modification} from './modification';

export class ConstraintChecker {
  private static didColumnValueChange(
      rowBefore: Row, rowAfter: Row, indexName: string): boolean {
    const deletionOrAddition =
        rowBefore === null ? rowAfter !== null : rowAfter === null;
    return deletionOrAddition ||
        (rowBefore.keyOfIndex(indexName) !== rowAfter.keyOfIndex(indexName));
  }

  private indexStore: IndexStore;
  private schema: Database;
  private cache: Cache;

  // A map where the keys are normalized lf.schema.ForeignKeySpec names, and the
  // values are corresponding parent column indices. The map is used such that
  // this association does not have to be detected more than once.
  private foreignKeysParentIndices: Map<string, RuntimeIndex>;

  constructor(global: Global) {
    this.indexStore = global.getService(Service.INDEX_STORE);
    this.schema = global.getService(Service.SCHEMA);
    this.cache = global.getService(Service.CACHE);
    this.foreignKeysParentIndices = new Map();
  }

  // Finds if any row with the same primary key exists in the primary key index.
  // Returns the row ID of an existing row that has the same primary
  // key as the input row, on null if no existing row was found.
  // |table|: the table where the row belongs.
  // |row|: the row whose primary key needs to be checked.
  public findExistingRowIdInPkIndex(table: Table, row: Row): number|null {
    const pkIndexSchema = table.getConstraint().getPrimaryKey();
    if (pkIndexSchema === null) {
      // There is no primary key for the given table.
      return null;
    }
    return this.findExistingRowIdInIndex(pkIndexSchema, row);
  }

  // Checks whether any not-nullable constraint violation occurs as a result of
  // inserting/updating the given set of rows.
  // |table|: the table where the row belongs.
  // |rows|: the rows being inserted
  public checkNotNullable(table: Table, rows: Row[]): void {
    const notNullable = table.getConstraint().getNotNullable();
    rows.forEach((row) => {
      notNullable.forEach((column) => {
        const target = row.payload()[column.getName()];
        if (!(target !== null && target !== undefined)) {
          // 202: Attempted to insert NULL value to non-nullable field {0}.
          throw new Exception(
              ErrorCode.NOT_NULLABLE, column.getNormalizedName());
        }
      }, this);
    }, this);
  }

  // Finds all rows in the database that should be updated as a result of
  // cascading updates, taking into account the given foreign key constraints.
  public detectCascadeUpdates(
      table: Table, modifications: Modification[],
      foreignKeySpecs: ForeignKeySpec[]): CascadeUpdate {
    const cascadedUpdates = new MapSet<number, CascadeUpdateItem>();
    this.loopThroughReferringRows(
        foreignKeySpecs, modifications,
        (foreignKeySpec, childIndex, parentKey, modification) => {
          const childRowIds = childIndex.get(parentKey as Key);
          childRowIds.forEach((rowId) => {
            cascadedUpdates.set(rowId, {
              fkSpec: foreignKeySpec,
              originalUpdatedRow: modification[1] as Row,
            });
          });
        });
    return cascadedUpdates;
  }

  // Performs all necessary foreign key constraint checks for the case where new
  // rows are inserted. Only constraints with |constraintTiming| will be
  // checked.
  public checkForeignKeysForInsert(
      table: Table, rows: Row[], constraintTiming: ConstraintTiming): void {
    if (rows.length === 0) {
      return;
    }

    const modifications = rows.map((row) => {
      return [null /* rowBefore */, row /* rowNow */] as Modification;
    });
    this.checkReferredKeys(table, modifications, constraintTiming);
  }

  // Performs all necessary foreign key constraint checks for the case of
  // existing rows being updated. Only constraints with |constraintTiming| will
  // be checked.
  public checkForeignKeysForUpdate(
      table: Table, modifications: Modification[],
      constraintTiming: ConstraintTiming): void {
    if (modifications.length === 0) {
      return;
    }

    this.checkReferredKeys(table, modifications, constraintTiming);
    this.checkReferringKeys(
        table, modifications, constraintTiming, ConstraintAction.RESTRICT);
  }

  // Performs all necessary foreign key constraint checks for the case of
  // existing rows being deleted. Only constraints with |constraintTiming| will
  // be checked.
  public checkForeignKeysForDelete(
      table: Table, rows: Row[], constraintTiming: ConstraintTiming): void {
    if (rows.length === 0) {
      return;
    }

    const modifications = rows.map((row) => {
      return [row /* rowBefore */, null /* rowNow */] as Modification;
    });
    this.checkReferringKeys(table, modifications, constraintTiming);
  }

  // Finds all rows in the database that should be deleted as a result of
  // cascading deletions.
  // |rows| are the original rows being deleted (before taking cascating into
  // account).
  public detectCascadeDeletion(table: Table, rows: Row[]): CascadeDeletion {
    const result: CascadeDeletion = {
      rowIdsPerTable: new MapSet<string, number>(),
      tableOrder: [],
    };

    let lastRowIdsToDelete = new MapSet<string, number>();
    lastRowIdsToDelete.setMany(table.getName(), rows.map((row) => row.id()));

    do {
      const newRowIdsToDelete = new MapSet<string, number>();
      lastRowIdsToDelete.keys().forEach((tableName) => {
        const tbl = this.schema.table(tableName);
        const rowIds = lastRowIdsToDelete.get(tableName) as number[];
        const modifications = rowIds.map((rowId) => {
          const row = this.cache.get(rowId);
          return [row /* rowBefore */, null /* rowNow */] as Modification;
        }, this);
        const referringRowIds = this.findReferringRowIds(tbl, modifications);
        if (referringRowIds !== null) {
          result.tableOrder.unshift.apply(
              result.tableOrder, referringRowIds.keys());
          newRowIdsToDelete.merge(referringRowIds);
        }
      }, this);
      lastRowIdsToDelete = newRowIdsToDelete;
      result.rowIdsPerTable.merge(lastRowIdsToDelete);
    } while (lastRowIdsToDelete.size > 0);

    return result;
  }

  // Finds if any row with the same index key exists in the given index.
  // Returns the row ID of an existing row that has the same index
  // key as the input row, on null if no existing row was found.
  // |indexSchema|: the index to check.
  // |row|: the row whose index key needs to be checked.
  private findExistingRowIdInIndex(indexSchema: Index, row: Row): number|null {
    const indexName = indexSchema.getNormalizedName();
    const indexKey = row.keyOfIndex(indexName);
    const index = this.indexStore.get(indexName) as RuntimeIndex;

    const rowIds = index.get(indexKey);
    return rowIds.length === 0 ? null : rowIds[0];
  }

  // Checks that all referred keys in the given rows actually exist.
  // Only constraints with matching |constraintTiming| will be checked.
  private checkReferredKeys(
      table: Table, modifications: Modification[],
      constraintTiming: ConstraintTiming): void {
    const foreignKeySpecs = table.getConstraint().getForeignKeys();
    foreignKeySpecs.forEach((foreignKeySpec) => {
      if (foreignKeySpec.timing === constraintTiming) {
        this.checkReferredKey(foreignKeySpec, modifications);
      }
    }, this);
  }

  private checkReferredKey(
      foreignKeySpec: ForeignKeySpec, modifications: Modification[]): void {
    const parentIndex = this.getParentIndex(foreignKeySpec);
    modifications.forEach((modification) => {
      const didColumnValueChange = ConstraintChecker.didColumnValueChange(
          modification[0] as Row, modification[1] as Row, foreignKeySpec.name);

      if (didColumnValueChange) {
        const rowAfter = modification[1] as Row;
        const parentKey = rowAfter.keyOfIndex(foreignKeySpec.name);
        // A null value in the child column implies to ignore it, and not
        // considering it as a constraint violation.
        if (parentKey !== null && !parentIndex.containsKey(parentKey)) {
          // 203: Foreign key constraint violation on constraint {0}.
          throw new Exception(ErrorCode.FK_VIOLATION, foreignKeySpec.name);
        }
      }
    }, this);
  }

  // Finds the index corresponding to the parent column of the given foreign
  // key by querying the schema and the IndexStore.
  // Returns the index corresponding to the parent column of the
  // given foreign key constraint.
  private findParentIndex(foreignKeySpec: ForeignKeySpec): RuntimeIndex {
    const parentTable = this.schema.table(foreignKeySpec.parentTable);
    const parentColumn: Column = parentTable[foreignKeySpec.parentColumn];
    // getIndex() must find an index since the parent of a foreign key
    // constraint must have a dedicated index.
    const parentIndexSchema: Index = parentColumn.getIndex() as Index;
    return this.indexStore.get(parentIndexSchema.getNormalizedName()) as
        RuntimeIndex;
  }

  // Gets the index corresponding to the parent column of the given foreign key.
  // Leverages this.foreignKeysParentIndices map, such that the work for finding
  // the parent index happens only once per foreign key.
  // Returns the index corresponding to the parent column of the
  // given foreign key constraint.
  private getParentIndex(foreignKeySpec: ForeignKeySpec): RuntimeIndex {
    let parentIndex = this.foreignKeysParentIndices.get(foreignKeySpec.name);
    if (parentIndex === undefined) {
      parentIndex = this.findParentIndex(foreignKeySpec);
      this.foreignKeysParentIndices.set(foreignKeySpec.name, parentIndex);
    }

    return parentIndex;
  }

  // Checks that no referring keys exist for the given rows.
  // Only constraints with |constraintTiming| will be checked.
  // Only constraints with |constraintAction| will be checked. If not provided
  // both CASCADE and RESTRICT are checked.
  private checkReferringKeys(
      table: Table, modifications: Modification[],
      constraintTiming: ConstraintTiming,
      constraintAction?: ConstraintAction): void {
    let foreignKeySpecs = this.schema.info().getReferencingForeignKeys(
        table.getName(), constraintAction);
    if (foreignKeySpecs === null) {
      return;
    }

    // TODO(dpapad): Enhance lf.schema.Info#getReferencingForeignKeys to filter
    // based on constraint timing, such that this linear search is avoided.
    foreignKeySpecs = foreignKeySpecs.filter((foreignKeySpec) => {
      return foreignKeySpec.timing === constraintTiming;
    });

    if (foreignKeySpecs.length === 0) {
      return;
    }

    this.loopThroughReferringRows(
        foreignKeySpecs, modifications,
        (foreignKeySpec, childIndex, parentKey) => {
          if (childIndex.containsKey(parentKey as Key)) {
            // 203: Foreign key constraint violation on constraint {0}.
            throw new Exception(ErrorCode.FK_VIOLATION, foreignKeySpec.name);
          }
        });
  }

  // Finds row IDs that refer to the given modified rows and specifically only
  // if the refer to a modified column. Returns referring row IDs per table.
  private findReferringRowIds(table: Table, modifications: Modification[]):
      MapSet<string, number>|null {
    // Finding foreign key constraints referring to the affected table.
    const foreignKeySpecs = this.schema.info().getReferencingForeignKeys(
        table.getName(), ConstraintAction.CASCADE);
    if (foreignKeySpecs === null) {
      return null;
    }

    const referringRowIds = new MapSet<string, number>();
    this.loopThroughReferringRows(
        foreignKeySpecs, modifications,
        (foreignKeySpec, childIndex, parentKey) => {
          const childRowIds = childIndex.get(parentKey as Key);
          if (childRowIds.length > 0) {
            referringRowIds.setMany(foreignKeySpec.childTable, childRowIds);
          }
        });
    return referringRowIds;
  }

  // Loops through the given list of foreign key constraints, for each modified
  // row and invokes the given callback only when a referred column's value has
  // been modified.
  private loopThroughReferringRows(
      foreignKeySpecs: ForeignKeySpec[], modifications: Modification[],
      callbackFn:
          (fkSpec: ForeignKeySpec, index: RuntimeIndex, key: Key|null,
           modification: Modification) => void): void {
    foreignKeySpecs.forEach((foreignKeySpec) => {
      const childIndex =
          this.indexStore.get(foreignKeySpec.name) as RuntimeIndex;
      const parentIndex = this.getParentIndex(foreignKeySpec);
      modifications.forEach((modification) => {
        const didColumnValueChange = ConstraintChecker.didColumnValueChange(
            modification[0] as Row, modification[1] as Row,
            parentIndex.getName());

        if (didColumnValueChange) {
          const rowBefore = modification[0] as Row;
          const parentKey = rowBefore.keyOfIndex(parentIndex.getName());
          callbackFn(foreignKeySpec, childIndex, parentKey, modification);
        }
      }, this);
    }, this);
  }
}
