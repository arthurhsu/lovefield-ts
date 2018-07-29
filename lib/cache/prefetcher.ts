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

import {BackStore} from '../backstore/back_store';
import {Tx} from '../backstore/tx';
import {TransactionType} from '../base/enum';
import {Global} from '../base/global';
import {TableType} from '../base/private_enum';
import {Row} from '../base/row';
import {Service} from '../base/service';
import {BTree} from '../index/btree';
import {ComparatorFactory} from '../index/comparator_factory';
import {IndexStore} from '../index/index_store';
import {Key} from '../index/key_range';
import {NullableIndex} from '../index/nullable_index';
import {RowId} from '../index/row_id';
import {BaseTable} from '../schema/base_table';
import {DatabaseSchema} from '../schema/database_schema';
import {IndexImpl} from '../schema/index_impl';

import {Cache} from './cache';

// Prefetcher fetches rows from database into cache and build indices.
export class Prefetcher {
  private backStore: BackStore;
  private indexStore: IndexStore;
  private cache: Cache;

  constructor(global: Global) {
    this.backStore = global.getService(Service.BACK_STORE);
    this.indexStore = global.getService(Service.INDEX_STORE);
    this.cache = global.getService(Service.CACHE);
  }

  public init(schema: DatabaseSchema): Promise<void> {
    // Sequentially load tables
    const tables = schema.tables();
    const execSequentially = (): Promise<void> => {
      if (tables.length === 0) {
        return Promise.resolve();
      }

      const table = tables.shift() as BaseTable;
      const whenTableFetched = table.persistentIndex() ?
          this.fetchTableWithPersistentIndices(table) :
          this.fetchTable(table);
      return whenTableFetched.then(execSequentially);
    };

    return execSequentially();
  }

  private fetchTable(table: BaseTable): Promise<void> {
    const tx = this.backStore.createTx(TransactionType.READ_ONLY, [table]);
    const store = tx.getTable(
        table.getName(), table.deserializeRow.bind(table), TableType.DATA);
    const promise = store.get([]).then((results) => {
      this.cache.setMany(table.getName(), results);
      this.reconstructNonPersistentIndices(table, results);
    });
    tx.commit();
    return promise;
  }

  // Reconstructs a table's indices by populating them from scratch.
  private reconstructNonPersistentIndices(
      tableSchema: BaseTable, tableRows: Row[]): void {
    const indices = this.indexStore.getTableIndices(tableSchema.getName());
    tableRows.forEach((row) => {
      indices.forEach((index) => {
        const key = row.keyOfIndex(index.getName()) as Key;
        index.add(key, row.id());
      });
    });
  }

  // Fetches contents of a table with persistent indices into cache, and
  // reconstructs the indices from disk.
  private fetchTableWithPersistentIndices(tableSchema: BaseTable):
      Promise<void> {
    const tx =
        this.backStore.createTx(TransactionType.READ_ONLY, [tableSchema]);

    const store = tx.getTable(
        tableSchema.getName(), tableSchema.deserializeRow, TableType.DATA);
    const whenTableContentsFetched = store.get([]).then((results) => {
      this.cache.setMany(tableSchema.getName(), results);
    });

    const whenIndicesReconstructed =
        (tableSchema.getIndices() as IndexImpl[])
            .map(
                (indexSchema: IndexImpl) =>
                    this.reconstructPersistentIndex(indexSchema, tx))
            .concat(this.reconstructPersistentRowIdIndex(tableSchema, tx));

    tx.commit();
    return Promise
        .all(whenIndicesReconstructed.concat(whenTableContentsFetched))
        .then(() => {
          return;
        });
  }

  // Reconstructs a persistent index by deserializing it from disk.
  private reconstructPersistentIndex(indexSchema: IndexImpl, tx: Tx):
      Promise<void> {
    const indexTable = tx.getTable(
        indexSchema.getNormalizedName(), Row.deserialize, TableType.INDEX);
    const comparator = ComparatorFactory.create(indexSchema);
    return indexTable.get([]).then((serializedRows) => {
      // No need to replace the index if there is no index contents.
      if (serializedRows.length > 0) {
        if (indexSchema.hasNullableColumn()) {
          const deserializeFn = BTree.deserialize.bind(
              undefined, comparator, indexSchema.getNormalizedName(),
              indexSchema.isUnique);
          const nullableIndex =
              NullableIndex.deserialize(deserializeFn, serializedRows);
          this.indexStore.set(indexSchema.tableName, nullableIndex);
        } else {
          const btreeIndex = BTree.deserialize(
              comparator, indexSchema.getNormalizedName(), indexSchema.isUnique,
              serializedRows);
          this.indexStore.set(indexSchema.tableName, btreeIndex);
        }
      }
    });
  }

  // Reconstructs a persistent RowId index by deserializing it from disk.
  private reconstructPersistentRowIdIndex(tableSchema: BaseTable, tx: Tx):
      Promise<void> {
    const indexTable = tx.getTable(
        tableSchema.getRowIdIndexName(), Row.deserialize, TableType.INDEX);
    return indexTable.get([]).then((serializedRows) => {
      // No need to replace the index if there is no index contents.
      if (serializedRows.length > 0) {
        const rowIdIndex =
            RowId.deserialize(tableSchema.getRowIdIndexName(), serializedRows);
        this.indexStore.set(tableSchema.getName(), rowIdIndex);
      }
    });
  }
}
