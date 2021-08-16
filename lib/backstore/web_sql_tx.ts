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
import {PayloadType, RawRow, Row} from '../base/row';
import {RuntimeTable} from '../base/runtime_table';
import {Journal} from '../cache/journal';

import {BaseTx} from './base_tx';
import {WebSqlTable} from './web_sql_table';

interface WebSqlTxCommand {
  statement: string;
  params: unknown[];
  transform?: (data: PayloadType) => Row[] | string[];
  resolver: Resolver<unknown>;
}

// Wrapper for Transaction object obtained from WebSQL.
export class WebSqlTx extends BaseTx {
  static INDEX_MARK = '__d__';

  // SQL standards disallow "." and "#" in the name of table. However, Lovefield
  // will name index tables using their canonical name, which contains those
  // illegal characters. As a result, we need to escape the table name.
  static escapeTableName(tableName: string): string {
    return tableName.replace('.', WebSqlTx.INDEX_MARK).replace('#', '__s__');
  }

  private tables: Map<string, WebSqlTable>;
  private commands: WebSqlTxCommand[];

  constructor(
    private db: Database,
    txType: TransactionType,
    journal?: Journal
  ) {
    super(txType, journal);
    this.tables = new Map<string, WebSqlTable>();
    this.commands = [];
  }

  getTable(
    tableName: string,
    deserializeFn: (value: RawRow) => Row,
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    tableType?: TableType
  ): RuntimeTable {
    let table = this.tables.get(tableName) || null;
    if (table === null) {
      table = new WebSqlTable(
        this,
        WebSqlTx.escapeTableName(tableName),
        deserializeFn
      );
      this.tables.set(tableName, table);
    }

    return table;
  }

  // Queues a SQL statement for the transaction.
  queue(
    statement: string,
    params: unknown[],
    transform?: (raw: PayloadType) => Row[] | string[]
  ): Promise<unknown> {
    const resolver = new Resolver<unknown>();
    this.commands.push({
      params,
      resolver,
      statement,
      transform,
    });
    return resolver.promise;
  }

  abort(): void {
    this.commands = [];
  }

  commitInternal(): Promise<unknown> {
    let lastCommand: WebSqlTxCommand | null = null;
    const onTxError = this.resolver.reject.bind(this.resolver);
    const onExecError: SQLStatementErrorCallback = (
      tx: SQLTransaction,
      e: SQLError
    ) => {
      this.resolver.reject(e);
      return false;
    };

    const results: unknown[] = [];
    const callback = (tx: SQLTransaction, res: unknown) => {
      if (lastCommand !== null) {
        let ret = res;
        if (lastCommand.transform && res !== null && res !== undefined) {
          ret = lastCommand.transform(res as PayloadType);
        }
        results.push(ret);
        lastCommand.resolver.resolve(ret);
      }

      if (this.commands.length > 0) {
        const command = this.commands.shift() as WebSqlTxCommand;
        lastCommand = command;
        tx.executeSql(command.statement, command.params, callback, onExecError);
      } else {
        this.resolver.resolve(results);
      }
    };

    if (this.txType === TransactionType.READ_ONLY) {
      this.db.readTransaction(
        callback as unknown as SQLTransactionCallback,
        onTxError
      );
    } else {
      this.db.transaction(
        callback as unknown as SQLTransactionCallback,
        onTxError
      );
    }

    return this.resolver.promise;
  }
}
