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
import {ErrorCode, TransactionType} from '../base/enum';
import {Exception} from '../base/exception';
import {Global} from '../base/global';
import {TableType, TaskPriority} from '../base/private_enum';
import {Resolver} from '../base/resolver';
import {Row} from '../base/row';
import {Service} from '../base/service';
import {UniqueId} from '../base/unique_id';
import {Cache} from '../cache/cache';
import {Journal} from '../cache/journal';
import {IndexStore} from '../index/index_store';
import {RuntimeIndex} from '../index/runtime_index';
import {Database} from '../schema/database';
import {Table} from '../schema/table';

import {Relation} from './relation';
import {Task} from './task';

// Imports table/rows from given JavaScript object to an empty database.
export class ImportTask extends UniqueId implements Task {
  private schema: Database;
  private scope: Set<Table>;
  private resolver: Resolver<Relation[]>;
  private backStore: BackStore;
  private cache: Cache;
  private indexStore: IndexStore;

  constructor(private global: Global, private data: object) {
    super();
    this.schema = global.getService(Service.SCHEMA);
    this.scope = new Set<Table>(this.schema.tables());
    this.resolver = new Resolver<Relation[]>();
    this.backStore = global.getService(Service.BACK_STORE);
    this.cache = global.getService(Service.CACHE);
    this.indexStore = global.getService(Service.INDEX_STORE);
  }

  public exec(): Promise<Relation[]> {
    if (!this.backStore.supportsImport()) {
      // Import is supported only on MemoryDB / IndexedDB / WebSql.
      // 300: Not supported.
      throw new Exception(ErrorCode.NOT_SUPPORTED);
    }

    if (!this.isEmptyDB()) {
      // 110: Attempt to import into a non-empty database.
      throw new Exception(ErrorCode.IMPORT_TO_NON_EMPTY_DB);
    }

    if (this.schema.name() !== this.data['name'] ||
        this.schema.version() !== this.data['version']) {
      // 111: Database name/version mismatch for import.
      throw new Exception(ErrorCode.DB_MISMATCH);
    }

    if (this.data['tables'] === undefined || this.data['tables'] === null) {
      // 112: Import data not found.
      throw new Exception(ErrorCode.IMPORT_DATA_NOT_FOUND);
    }

    return this.import();
  }

  public getType(): TransactionType {
    return TransactionType.READ_WRITE;
  }

  public getScope(): Set<Table> {
    return this.scope;
  }

  public getResolver(): Resolver<Relation[]> {
    return this.resolver;
  }

  public getId(): number {
    return this.getUniqueNumber();
  }

  public getPriority(): TaskPriority {
    return TaskPriority.IMPORT_TASK;
  }

  private isEmptyDB(): boolean {
    return this.schema.tables().every((table) => {
      const index =
          this.indexStore.get(table.getRowIdIndexName()) as RuntimeIndex;
      if (index.stats().totalRows > 0) {
        return false;
      }
      return true;
    });
  }

  private import(): Promise<Relation[]> {
    const journal = new Journal(this.global, this.scope);
    const tx = this.backStore.createTx(
        this.getType(), Array.from(this.scope.values()), journal);

    Object.keys(this.data['tables']).forEach((tableName) => {
    const tableSchema = this.schema.table(tableName);
    const payloads = this.data['tables'][tableName];
    const rows =
        payloads.map((value: object) => tableSchema.createRow(value)) as Row[];

    const table =
        tx.getTable(tableName, tableSchema.deserializeRow, TableType.DATA);
    this.cache.setMany(tableName, rows);
    const indices = this.indexStore.getTableIndices(tableName);
    rows.forEach((row) => {
      indices.forEach((index) => {
        const key = row.keyOfIndex(index.getName());
        index.add(key, row.id());
      });
    });
    table.put(rows);
    }, this);

    return tx.commit();
  }
}
