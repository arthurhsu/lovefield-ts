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

import {TransactionType} from '../base/enum';
import {TableType} from '../base/private_enum';
import {Resolver} from '../base/resolver';
import {RawRow, Row} from '../base/row';
import {RuntimeTable} from '../base/runtime_table';
import {Journal} from '../cache/journal';
import {BaseTable} from '../schema/base_table';
import {TransactionStatsImpl} from './transaction_stats_impl';
import {Tx} from './tx';

// A base class for all native DB transactions wrappers to subclass.
export abstract class BaseTx implements Tx {
  protected resolver: Resolver<unknown>;

  private journal: Journal | null;
  private success: boolean;
  private statsObject: TransactionStatsImpl | null;

  constructor(protected txType: TransactionType, journal?: Journal) {
    this.journal = journal || null;
    this.resolver = new Resolver<unknown>();
    this.success = false;
    this.statsObject = null;
  }

  abstract getTable(
    tableName: string,
    deserializeFn: (value: RawRow) => Row,
    tableType?: TableType
  ): RuntimeTable;
  abstract abort(): void;
  abstract commitInternal(): Promise<unknown>;

  getJournal(): Journal | null {
    return this.journal;
  }

  commit(): Promise<unknown> {
    const promise =
      this.txType === TransactionType.READ_ONLY
        ? this.commitInternal()
        : this.commitReadWrite();
    return promise.then((results: unknown) => {
      this.success = true;
      return results;
    });
  }

  stats(): TransactionStatsImpl | null {
    if (this.statsObject === null) {
      if (!this.success) {
        this.statsObject = TransactionStatsImpl.getDefault();
      } else if (this.txType === TransactionType.READ_ONLY) {
        this.statsObject = new TransactionStatsImpl(true, 0, 0, 0, 0);
      } else {
        const diff = (this.journal as Journal).getDiff();
        let insertedRows = 0;
        let deletedRows = 0;
        let updatedRows = 0;
        let tablesChanged = 0;
        diff.forEach((tableDiff, tableName) => {
          tablesChanged++;
          insertedRows += tableDiff.getAdded().size;
          updatedRows += tableDiff.getModified().size;
          deletedRows += tableDiff.getDeleted().size;
        });
        this.statsObject = new TransactionStatsImpl(
          true,
          insertedRows,
          updatedRows,
          deletedRows,
          tablesChanged
        );
      }
    }
    return this.statsObject;
  }

  private commitReadWrite(): Promise<unknown> {
    try {
      (this.journal as Journal).checkDeferredConstraints();
    } catch (e) {
      return Promise.reject(e);
    }

    return this.mergeIntoBackstore().then(results => {
      (this.journal as Journal).commit();
      return results;
    });
  }

  // Flushes all changes currently in this transaction's journal to the backing
  // store. Returns a promise firing after all changes have been successfully
  // written to the backing store.
  private mergeIntoBackstore(): Promise<unknown> {
    this.mergeTableChanges();
    this.mergeIndexChanges();

    // When all DB operations have finished, this.whenFinished will fire.
    return this.commitInternal();
  }

  // Flushes the changes currently in this transaction's journal that refer to
  // user-defined tables to the backing store.
  private mergeTableChanges(): void {
    const journal = this.journal as Journal;
    const diff = journal.getDiff();
    diff.forEach((tableDiff, tableName) => {
      const tableSchema = journal.getScope().get(tableName) as BaseTable;
      const table = this.getTable(
        tableSchema.getName(),
        tableSchema.deserializeRow.bind(tableSchema),
        TableType.DATA
      );
      const toDeleteRowIds = Array.from(
        tableDiff.getDeleted().values()
      ).map(row => row.id());
      const toPut = Array.from(tableDiff.getModified().values())
        .map(modification => modification[1] as Row)
        .concat(Array.from(tableDiff.getAdded().values()));
      // If we have things to put and delete in the same transaction then we
      // need to disable the clear table optimization the backing store might
      // want to do. Otherwise we have possible races between the put and
      // count/clear.
      const shouldDisableClearTableOptimization = toPut.length > 0;
      if (toDeleteRowIds.length > 0) {
        table.remove(toDeleteRowIds, shouldDisableClearTableOptimization).then(
          () => {},
          (e: Error) => this.resolver.reject(e)
        );
      }
      table.put(toPut).then(
        () => {},
        (e: Error) => this.resolver.reject(e)
      );
    }, this);
  }

  // Flushes the changes currently in this transaction's journal that refer to
  // persisted indices to the backing store.
  private mergeIndexChanges(): void {
    const indices = (this.journal as Journal).getIndexDiff();
    indices.forEach(index => {
      const indexTable = this.getTable(
        index.getName(),
        Row.deserialize,
        TableType.INDEX
      );
      // Since there is no index diff implemented yet, the entire index needs
      // to be overwritten on disk.
      indexTable.remove([]);
      indexTable.put(index.serialize());
    }, this);
  }
}
