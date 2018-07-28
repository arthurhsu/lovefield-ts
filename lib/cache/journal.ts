
/**
 * Copyright 2018 The Lovefield Project Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the 'License');
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an 'AS IS' BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {assert} from '../base/assert';
import {ConstraintAction, ConstraintTiming, ErrorCode} from '../base/enum';
import {Exception} from '../base/exception';
import {Global} from '../base/global';
import {Row} from '../base/row';
import {Service} from '../base/service';
import {IndexStore} from '../index/index_store';
import {RuntimeIndex} from '../index/runtime_index';
import {BaseTable} from '../schema/base_table';
import {Database} from '../schema/database';

import {Cache} from './cache';
import {CascadeUpdateItem} from './cascade';
import {ConstraintChecker} from './constraint_checker';
import {InMemoryUpdater} from './in_memory_updater';
import {Modification} from './modification';
import {TableDiff} from './table_diff';

// A transaction journal which is contained within Tx. The journal
// stores rows changed by this transaction so that they can be merged into the
// backing store. Caches and indices are updated as soon as a change is
// recorded in the journal.
export class Journal {
  private scope: Map<string, BaseTable>;
  private schema: Database;
  private cache: Cache;
  private indexStore: IndexStore;
  private constraintChecker: ConstraintChecker;
  private inMemoryUpdater: InMemoryUpdater;

  // A terminated journal can no longer be modified or rolled back. This should
  // be set to true only after the changes in this Journal have been reflected
  // in the backing store, or the journal has been rolled back.
  private terminated: boolean;

  // When a constraint violation happens the journal becomes not writable
  // anymore and the only operation that is allowed is rolling back. Callers
  // of Journal#insert,insertOrReplace,update,remove *must* rollback the journal
  // if any lf.Exception is thrown, otherwise the index data structures will not
  // reflect what is in the database.
  private pendingRollback: boolean;

  // The changes that have been applied since the start of this journal. The
  // keys are table names, and the values are changes that have happened per
  // table.
  private tableDiffs: Map<string, TableDiff>;

  constructor(global: Global, txScope: Set<BaseTable>) {
    this.scope = new Map<string, BaseTable>();
    txScope.forEach(
        (tableSchema) => this.scope.set(tableSchema.getName(), tableSchema));

    this.schema = global.getService(Service.SCHEMA);
    this.cache = global.getService(Service.CACHE);
    this.indexStore = global.getService(Service.INDEX_STORE);
    this.constraintChecker = new ConstraintChecker(global);
    this.inMemoryUpdater = new InMemoryUpdater(global);
    this.terminated = false;
    this.pendingRollback = false;
    this.tableDiffs = new Map<string, TableDiff>();
  }

  public getDiff(): Map<string, TableDiff> {
    return this.tableDiffs;
  }

  // Returns the indices that were modified in this within this journal.
  // TODO(dpapad): Indices currently can't provide a diff, therefore the entire
  // index is flushed into disk every time, even if only one leaf-node changed.
  public getIndexDiff(): RuntimeIndex[] {
    const tableSchemas = Array.from(this.tableDiffs.keys())
                             .map((tableName) => this.scope.get(tableName));

    const indices: RuntimeIndex[] = [];
    tableSchemas.forEach((schema) => {
      const tableSchema = schema as BaseTable;
      if (tableSchema.persistentIndex()) {
        const tableIndices = tableSchema.getIndices();
        tableIndices.forEach((indexSchema) => {
          indices.push(
              this.indexStore.get(indexSchema.getNormalizedName()) as
              RuntimeIndex);
        }, this);
        indices.push(
            this.indexStore.get(tableSchema.getName() + '.#') as RuntimeIndex);
      }
    }, this);
    return indices;
  }

  public getScope(): Map<string, BaseTable> {
    return this.scope;
  }

  public insert(table: BaseTable, rows: Row[]): void {
    this.assertJournalWritable();
    this.checkScope(table);
    this.constraintChecker.checkNotNullable(table, rows);
    this.constraintChecker.checkForeignKeysForInsert(
        table, rows, ConstraintTiming.IMMEDIATE);

    rows.forEach((row) => {
      this.modifyRow(table, [null /* rowBefore */, row /* rowNow */]);
    }, this);
  }

  public update(table: BaseTable, rows: Row[]): void {
    this.assertJournalWritable();
    this.checkScope(table);
    this.constraintChecker.checkNotNullable(table, rows);

    const modifications: Modification[] = rows.map((row) => {
      const rowBefore = this.cache.get(row.id());
      return [rowBefore /* rowBefore */, row /* rowNow */] as Modification;
    }, this);
    this.updateByCascade(table, modifications);

    this.constraintChecker.checkForeignKeysForUpdate(
        table, modifications, ConstraintTiming.IMMEDIATE);
    modifications.forEach(
        (modification) => this.modifyRow(table, modification));
  }

  public insertOrReplace(table: BaseTable, rows: Row[]): void {
    this.assertJournalWritable();
    this.checkScope(table);
    this.constraintChecker.checkNotNullable(table, rows);

    rows.forEach((rowNow) => {
      let rowBefore = null;

      const existingRowId =
          this.constraintChecker.findExistingRowIdInPkIndex(table, rowNow);

      if (existingRowId !== undefined && existingRowId !== null) {
        rowBefore = this.cache.get(existingRowId);
        rowNow.assignRowId(existingRowId);
        const modification = [rowBefore, rowNow] as Modification;
        this.constraintChecker.checkForeignKeysForUpdate(
            table, [modification], ConstraintTiming.IMMEDIATE);
      } else {
        this.constraintChecker.checkForeignKeysForInsert(
            table, [rowNow], ConstraintTiming.IMMEDIATE);
      }

      this.modifyRow(table, [rowBefore, rowNow]);
    }, this);
  }

  public remove(table: BaseTable, rows: Row[]): void {
    this.assertJournalWritable();
    this.checkScope(table);

    this.removeByCascade(table, rows);
    this.constraintChecker.checkForeignKeysForDelete(
        table, rows, ConstraintTiming.IMMEDIATE);

    rows.forEach((row) => {
      this.modifyRow(table, [row /* rowBefore */, null /* rowNow */]);
    }, this);
  }

  public checkDeferredConstraints(): void {
    this.tableDiffs.forEach((tableDiff, tableName) => {
      const table = this.scope.get(tableDiff.getName()) as BaseTable;
      this.constraintChecker.checkForeignKeysForInsert(
          table, Array.from(tableDiff.getAdded().values()),
          ConstraintTiming.DEFERRABLE);
      this.constraintChecker.checkForeignKeysForDelete(
          table, Array.from(tableDiff.getDeleted().values()),
          ConstraintTiming.DEFERRABLE);
      this.constraintChecker.checkForeignKeysForUpdate(
          table, Array.from(tableDiff.getModified().values()),
          ConstraintTiming.DEFERRABLE);
    }, this);
  }

  // Commits journal changes into cache and indices.
  public commit(): void {
    this.assertJournalWritable();
    this.terminated = true;
  }

  // Rolls back all the changes that were made in this journal from the cache
  // and indices.
  public rollback(): void {
    assert(!this.terminated, 'Attempted to rollback a terminated journal.');

    const reverseDiffs = Array.from(this.tableDiffs.values())
                             .map((tableDiff) => tableDiff.getReverse());
    this.inMemoryUpdater.update(reverseDiffs);

    this.terminated = true;
    this.pendingRollback = false;
  }

  // Asserts that this journal can still be used.
  private assertJournalWritable(): void {
    assert(
        !this.pendingRollback,
        'Attemptted to use journal that needs to be rolled back.');
    assert(!this.terminated, 'Attemptted to commit a terminated journal.');
  }

  // Checks that the given table is within the declared scope.
  private checkScope(tableSchema: BaseTable): void {
    if (!this.scope.has(tableSchema.getName())) {
      // 106: Attempt to access {0} outside of specified scope.
      throw new Exception(ErrorCode.OUT_OF_SCOPE, tableSchema.getName());
    }
  }

  // Updates the journal to reflect a modification (insertion, update, deletion)
  // of a single row.
  private modifyRow(table: BaseTable, modification: Modification): void {
    const tableName = table.getName();
    const diff = this.tableDiffs.get(tableName) || new TableDiff(tableName);
    this.tableDiffs.set(tableName, diff);

    try {
      this.inMemoryUpdater.updateTableIndicesForRow(table, modification);
    } catch (e) {
      this.pendingRollback = true;
      throw e;
    }

    const rowBefore = modification[0];
    const rowNow = modification[1];
    if (rowBefore === null && rowNow !== null) {
      // Insertion
      this.cache.set(tableName, rowNow);
      diff.add(rowNow);
    } else if (rowBefore !== null && rowNow !== null) {
      // Update
      this.cache.set(tableName, rowNow);
      diff.modify(modification);
    } else if (rowBefore !== null && rowNow === null) {
      // Deletion
      this.cache.remove(tableName, rowBefore.id());
      diff.delete(rowBefore);
    }
  }

  // Updates rows in the DB as a result of cascading foreign key constraints.
  // |table| refers to the table where the update is initiated.
  // |modifications| means the initial modifications.
  private updateByCascade(table: BaseTable, modifications: Modification[]):
      void {
    const foreignKeySpecs = this.schema.info().getReferencingForeignKeys(
        table.getName(), ConstraintAction.CASCADE);
    if (foreignKeySpecs === null) {
      // The affected table does not appear as the parent in any CASCADE foreign
      // key constraint, therefore no cascading detection is needed.
      return;
    }
    const cascadedUpdates = this.constraintChecker.detectCascadeUpdates(
        table, modifications, foreignKeySpecs);
    cascadedUpdates.keys().forEach((rowId) => {
      const updates = cascadedUpdates.get(rowId) as CascadeUpdateItem[];
      updates.forEach((update) => {
        const tbl = this.schema.table(update.fkSpec.childTable);
        const rowBefore = this.cache.get(rowId) as Row;
        // TODO(dpapad): Explore faster ways to clone an lf.Row.
        const rowAfter = tbl.deserializeRow(rowBefore.serialize());
        rowAfter.payload()[update.fkSpec.childColumn] =
            update.originalUpdatedRow.payload()[update.fkSpec.parentColumn];
        this.modifyRow(tbl, [rowBefore /* rowBefore */, rowAfter /* rowNow */]);
      }, this);
    }, this);
  }

  // Removes rows in the DB as a result of cascading foreign key constraints.
  // |table| refers to the table where the update is initiated.
  // |rows| means the initial rows to be deleted.
  private removeByCascade(table: BaseTable, deletedRows: Row[]): void {
    const foreignKeySpecs = this.schema.info().getReferencingForeignKeys(
        table.getName(), ConstraintAction.CASCADE);
    if (foreignKeySpecs === null) {
      // The affected table does not appear as the parent in any CASCADE foreign
      // key constraint, therefore no cascading detection is needed.
      return;
    }

    const cascadeDeletion =
        this.constraintChecker.detectCascadeDeletion(table, deletedRows);
    const cascadeRowIds = cascadeDeletion.rowIdsPerTable;

    cascadeDeletion.tableOrder.forEach((tableName) => {
      const tbl = this.schema.table(tableName);
      const rows = (cascadeRowIds.get(tableName) as number[]).map((rowId) => {
        return this.cache.get(rowId) as Row;
      }, this);
      this.constraintChecker.checkForeignKeysForDelete(
          tbl, rows, ConstraintTiming.IMMEDIATE);
      rows.forEach((row) => {
        this.modifyRow(tbl, [row /* rowBefore */, null /* rowNow */]);
      }, this);
    }, this);
  }
}
