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
import {Row} from '../base/row';
import {RuntimeTable} from '../base/runtime_table';
import {Journal} from '../cache/journal';
import {TableDiff} from '../cache/table_diff';
import {BaseTable} from '../schema/base_table';
import {Database as DatabaseLF} from '../schema/database';

import {BackStore} from './back_store';
import {RawBackStore} from './raw_back_store';
import {Tx} from './tx';
import {WebSqlRawBackStore} from './web_sql_raw_back_store';
import {WebSqlTx} from './web_sql_tx';

type UpgradeCallback = (db: RawBackStore) => Promise<any>;

export class WebSql implements BackStore {
  // Escapes table name so that table name can be reserved words.
  private static escape(tableName: string): string {
    return `"${tableName}"`;
  }

  private size: number;
  private db!: Database;

  constructor(
      private global: Global, private schema: DatabaseLF, size?: number) {
    // Estimated size of WebSQL store. It is defaulted to 1 for a reason:
    // http://pouchdb.com/2014/10/26/10-things-i-learned-from-reading-and-writing-the-pouchdb-source.html
    this.size = size || 1;
  }

  public init(upgrade?: UpgradeCallback): Promise<any> {
    if (!(window.openDatabase)) {
      // 353: WebSQL not supported by platform.
      throw new Exception(ErrorCode.WEBSQL_NOT_PROVIDED);
    }

    const defaultUpgrade = (rawDb: RawBackStore) => Promise.resolve();
    const onUpgrade = upgrade || defaultUpgrade;

    return new Promise((resolve, reject) => {
      const db = window.openDatabase(
          this.schema.name(),
          '',  // Just open it with any version
          this.schema.name(), this.size);
      if (db) {
        this.db = db;
        this.checkVersion(onUpgrade).then(
            () => {
              this.scanRowId().then(resolve, reject);
            },
            (e) => {
              if (e instanceof Exception) {
                throw e;
              }
              // 354: Unable to open WebSQL database.
              throw new Exception(ErrorCode.CANT_OPEN_WEBSQL_DB, e.message);
            });
      } else {
        // 354: Unable to open WebSQL database.
        throw new Exception(ErrorCode.CANT_OPEN_WEBSQL_DB);
      }
    });
  }

  public initialized(): boolean {
    return (this.db !== undefined && this.db !== null);
  }

  public createTx(type: TransactionType, scope: BaseTable[], journal?: Journal):
      Tx {
    if (this.db) {
      return new WebSqlTx(this.db, type, journal);
    }
    // 2: The database has not initialized yet.
    throw new Exception(ErrorCode.CONNECTION_CLOSED);
  }

  public close(): void {
    // WebSQL does not support closing a database connection.
  }

  public getTableInternal(tableName: string): RuntimeTable {
    // 512: WebSQL tables needs to be acquired from transactions.
    throw new Exception(ErrorCode.CANT_GET_WEBSQL_TABLE);
  }

  public subscribe(handler: (diffs: TableDiff[]) => void): void {
    this.notSupported();
  }

  public unsubscribe(handler: (diffs: TableDiff[]) => void): void {
    this.notSupported();
  }

  public notify(changes: TableDiff[]): void {
    this.notSupported();
  }

  public supportsImport(): boolean {
    return true;
  }

  private getEmptyJournal(): Journal {
    return new Journal(this.global, new Set<BaseTable>());
  }

  // Workaround Chrome's changeVersion problem.
  // WebSQL changeVersion function is not working on Chrome. As a result,
  // creating a .version table to save database version.
  private checkVersion(onUpgrade: UpgradeCallback): Promise<any> {
    const CREATE_VERSION = 'CREATE TABLE IF NOT EXISTS __lf_ver(' +
        'id INTEGER PRIMARY KEY, v INTEGER)';
    const GET_VERSION = 'SELECT v FROM __lf_ver WHERE id = 0';
    const resolver = new Resolver<any>();

    const tx = new WebSqlTx(
        this.db, TransactionType.READ_WRITE, this.getEmptyJournal());
    tx.queue(CREATE_VERSION, []);
    tx.queue(GET_VERSION, []);
    tx.commit().then((results: SQLResultSet) => {
      let version = 0;
      if (results[1].rows.length) {
        version = results[1].rows.item(0)['v'];
      }
      if (version < this.schema.version()) {
        this.onUpgrade(onUpgrade, version)
            .then(resolver.resolve.bind(resolver));
      } else if (version > this.schema.version()) {
        // 108: Attempt to open a newer database with old code
        resolver.reject(new Exception(ErrorCode.INCOMPATIBLE_DB));
      } else {
        resolver.resolve();
      }
    }, resolver.reject.bind(resolver));

    return resolver.promise;
  }

  private notSupported(): void {
    // 355: WebSQL does not support change notification.
    throw new Exception(ErrorCode.NO_CHANGE_NOTIFICATION);
  }

  private onUpgrade(upgrade: UpgradeCallback, oldVersion: number):
      Promise<void> {
    return this.preUpgrade().then(() => {
      const rawDb = new WebSqlRawBackStore(this.global, oldVersion, this.db);
      return upgrade(rawDb);
    });
  }

  // Deletes persisted indices and creates new tables.
  private preUpgrade(): Promise<any> {
    const tables = this.schema.tables();

    const tx = new WebSqlTx(
        this.db, TransactionType.READ_WRITE, this.getEmptyJournal());
    const tx2 = new WebSqlTx(
        this.db, TransactionType.READ_WRITE, this.getEmptyJournal());

    tx.queue(
        'INSERT OR REPLACE INTO __lf_ver VALUES (0, ?)',
        [this.schema.version()]);
    WebSqlRawBackStore.queueListTables(tx);
    return tx.commit().then((results) => {
      const existingTables: string[] = results[1];
      // Delete all existing persisted indices.
      existingTables.filter((name) => name.indexOf(WebSqlTx.INDEX_MARK) !== -1)
          .forEach(
              (name) => tx2.queue('DROP TABLE ' + WebSql.escape(name), []));

      // Create new tables.
      const newTables: string[] = [];
      const persistentIndices: string[] = [];
      const rowIdIndices: string[] = [];
      tables.map((table) => {
        if (existingTables.indexOf(table.getName()) === -1) {
          newTables.push(table.getName());
        }
        if (table.persistentIndex) {
          table.getIndices().forEach((index) => {
            const idxTableName =
                WebSqlTx.escapeTableName(index.getNormalizedName());
            newTables.push(idxTableName);
            persistentIndices.push(idxTableName);
          });
          const rowIdTableName =
              WebSqlTx.escapeTableName(table.getRowIdIndexName());
          newTables.push(rowIdTableName);
          rowIdIndices.push(rowIdTableName);
        }
      });

      newTables.forEach((name) => {
        tx2.queue(
            `CREATE TABLE ${WebSql.escape(name)}` +
                '(id INTEGER PRIMARY KEY, value TEXT)',
            []);
      });

      return tx2.commit();
    });
  }

  // Scans existing database and find the maximum row id.
  private scanRowId(): Promise<void> {
    let maxRowId = 0;
    const resolver = new Resolver<void>();

    const selectIdFromTable = (tableName: string): Promise<any> => {
      const tx = new WebSqlTx(this.db, TransactionType.READ_ONLY);
      tx.queue(`SELECT MAX(id) FROM ${WebSql.escape(tableName)}`, []);
      return tx.commit().then((results: SQLResultSet) => {
        const id: number = results[0].rows.item(0)['MAX(id)'];
        maxRowId = Math.max(id, maxRowId);
      });
    };

    const promises =
        this.schema.tables().map((table) => selectIdFromTable(table.getName()));

    Promise.all(promises).then(
        () => {
          Row.setNextIdIfGreater(maxRowId + 1);
          resolver.resolve();
        },
        (e) => {
          resolver.reject(e);
        });

    return resolver.promise;
  }
}
