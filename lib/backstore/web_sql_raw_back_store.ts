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

import {ErrorCode, TransactionType} from '../base/enum';
import {Exception} from '../base/exception';
import {Global} from '../base/global';
import {Resolver} from '../base/resolver';
import {PayloadType, RawRow, Row} from '../base/row';
import {Journal} from '../cache/journal';
import {BaseTable} from '../schema/base_table';

import {IndexedDBRawBackStore} from './indexed_db_raw_back_store';
import {RawBackStore} from './raw_back_store';
import {WebSqlTx} from './web_sql_tx';

// WebSQL raw back store. Please note that all altering functions will commit
// immediately due to implementation restrictions. This is different from the
// IndexedDB raw back store.
export class WebSqlRawBackStore implements RawBackStore {
  static queueListTables(tx: WebSqlTx): void {
    const GET_TABLE_NAMES =
      'SELECT tbl_name FROM sqlite_master WHERE type="table"';

    tx.queue(GET_TABLE_NAMES, [], (results: PayloadType) => {
      const rows = results['rows'] as SQLResultSetRowList;
      const tableNames: string[] = new Array(rows.length);
      for (let i = 0; i < tableNames.length; ++i) {
        tableNames[i] = rows.item(i)['tbl_name'];
      }
      return tableNames;
    });
  }

  constructor(
    private global: Global,
    private version: number,
    private db: Database
  ) {}

  getRawDBInstance(): Database {
    return this.db;
  }

  getRawTransaction(): unknown {
    // 356: Use WebSQL instance to create transaction instead.
    throw new Exception(ErrorCode.NO_WEBSQL_TX);
  }

  dropTable(tableName: string): Promise<void> {
    const tx = this.createTx();
    tx.queue(`DROP TABLE ${tableName}`, []);
    return tx.commit() as Promise<void>;
  }

  addTableColumn(
    tableName: string,
    columnName: string,
    defaultValue: string | number | boolean | Date | ArrayBuffer | null
  ): Promise<void> {
    const value = IndexedDBRawBackStore.convert(defaultValue);

    return this.transformColumn(tableName, row => {
      row.value[columnName] = value;
      return row;
    });
  }

  dropTableColumn(tableName: string, columnName: string): Promise<void> {
    return this.transformColumn(tableName, row => {
      delete row.value[columnName];
      return row;
    });
  }

  renameTableColumn(
    tableName: string,
    oldColumnName: string,
    newColumnName: string
  ): Promise<void> {
    return this.transformColumn(tableName, row => {
      row.value[newColumnName] = row.value[oldColumnName];
      delete row.value[oldColumnName];
      return row;
    });
  }

  createRow(payload: PayloadType): Row {
    const data: PayloadType = {};
    Object.keys(payload).forEach(key => {
      data[key] = IndexedDBRawBackStore.convert(payload[key]);
    });

    return Row.create(data);
  }

  getVersion(): number {
    return this.version;
  }

  dump(): Promise<PayloadType> {
    const resolver = new Resolver<PayloadType>();

    const tx = this.createTx();
    WebSqlRawBackStore.queueListTables(tx);

    const ret: PayloadType = {};
    tx.commit().then(res => {
      const results = res as string[][];
      const tables: string[] = results[0].filter((name: string) => {
        return name !== '__lf_ver' && name !== '__WebKitDatabaseInfoTable__';
      });
      const promises = tables.map(tableName => {
        return this.dumpTable(tableName).then(rows => (ret[tableName] = rows));
      }, this);
      Promise.all(promises).then(() => resolver.resolve(ret));
    });

    return resolver.promise;
  }

  private createTx(): WebSqlTx {
    return new WebSqlTx(
      this.db,
      TransactionType.READ_WRITE,
      new Journal(this.global, new Set<BaseTable>())
    );
  }

  private dumpTable(tableName: string): Promise<RawRow[]> {
    const tx = this.createTx();
    tx.queue(`SELECT id, value FROM ${tableName}`, []);
    return tx.commit().then(res => {
      const results = res as SQLResultSet[];
      const length = results[0].rows.length;
      const rows: RawRow[] = new Array(length);
      for (let i = 0; i < length; ++i) {
        rows[i] = {
          id: results[0].rows.item(i)['id'],
          value: JSON.parse(results[0].rows.item(i)['value']),
        };
      }

      return Promise.resolve(rows);
    });
  }

  private transformColumn(
    tableName: string,
    transformer: (raw: RawRow) => RawRow
  ): Promise<void> {
    const tx = this.createTx();
    const sql = `UPDATE ${tableName} SET value=? WHERE id=?`;
    return this.dumpTable(tableName).then(rows => {
      rows.forEach(row => {
        const newRow = transformer(row);
        tx.queue(sql, [JSON.stringify(newRow.value), newRow.id]);
      });
      return tx.commit() as Promise<void>;
    });
  }
}
