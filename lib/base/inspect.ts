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

import {RuntimeIndex} from '../index/runtime_index';
import {BaseTable} from '../schema/base_table';
import {Global} from './global';
import {Row} from './row';
import {Service} from './service';
import {ServiceId} from './service_id';

export class Inspector {
  // Returns requested results as string for inspector to use.
  // |dbName|: Database name, if null then return list of DB.
  // |tableName|: Table name, if null then return list of tables. Honored only
  // if |dbName| is provided.
  // |limit| and |skip| controls rows to return, honored only if both |dbName|
  // and |tableName| are provided.
  static inspect(
    dbName: string | null,
    tableName: string | null,
    limit?: number,
    skip?: number
  ): string {
    if (dbName === null) {
      return Inspector.listDb();
    }
    if (tableName === null) {
      return Inspector.listTables(dbName);
    }
    return Inspector.inspectTable(dbName, tableName, limit, skip);
  }

  // Return stringified object.
  private static toString(data: object | object[]): string {
    let value = '';
    try {
      value = JSON.stringify(data);
    } catch (e) {
      // Ignore all errors.
    }
    return value;
  }

  // Returns global object by database name.
  private static getGlobal(dbName: string): Global | null {
    const global = Global.get();
    const ns = new ServiceId(`ns_${dbName}`);
    return global.isRegistered(ns) ? (global.getService(ns) as Global) : null;
  }

  // Returns a stringified object, whose key is DB name and value is version.
  private static listDb(): string {
    const global = Global.get();
    interface DBListType {
      [key: string]: number;
    }
    const dbList: DBListType = {};
    global.listServices().forEach(service => {
      if (service.substring(0, 3) === 'ns_') {
        const dbName = service.substring(3);
        dbList[dbName] = (Inspector.getGlobal(dbName) as Global)
          .getService(Service.SCHEMA)
          .version();
      }
    });
    return Inspector.toString(dbList);
  }

  // Returns a stringified object, whose key is table name and value is number
  // of rows in that table.
  private static listTables(dbName: string): string {
    const global = Inspector.getGlobal(dbName);
    interface TableListType {
      [key: string]: number;
    }
    const tables: TableListType = {};
    if (global !== undefined && global !== null) {
      const indexStore = global.getService(Service.INDEX_STORE);
      global
        .getService(Service.SCHEMA)
        .tables()
        .forEach(t => {
          const table = t as BaseTable;
          tables[table.getName()] = (indexStore.get(
            table.getRowIdIndexName()
          ) as RuntimeIndex).stats().totalRows;
        });
    }

    return Inspector.toString(tables);
  }

  // Returns a stringified array of rows.
  private static inspectTable(
    dbName: string,
    tableName: string,
    limit?: number,
    skip?: number
  ): string {
    const global = Inspector.getGlobal(dbName);
    let contents: object[] = [];
    if (global !== undefined && global !== null) {
      let table: BaseTable | null = null;
      try {
        table = global.getService(Service.SCHEMA).table(tableName) as BaseTable;
      } catch (e) {
        // Ignore all errors.
      }
      if (table !== undefined && table !== null) {
        const indexStore = global.getService(Service.INDEX_STORE);
        const cache = global.getService(Service.CACHE);
        const rowIds = (indexStore.get(
          table.getRowIdIndexName()
        ) as RuntimeIndex).getRange(undefined, false, limit, skip);
        if (rowIds.length) {
          contents = (cache.getMany(rowIds) as Row[]).map(row => row.payload());
        }
      }
    }

    return Inspector.toString(contents);
  }
}
