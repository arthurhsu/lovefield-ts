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
import {Row} from '../base/row';

// Raw data store interface passed to onUpgrade() function.
// @export
export interface RawBackStore {
  // Returns original database instance that can be used for low-level data
  // manipulations, not really useful for IndexedDB.
  getRawDBInstance(): any;

  // Returns original database upgrade transaction.
  getRawTransaction(): any;

  // Removes a table from data store. Lovefield does not support automatic
  // dropping table. Users must call dropTable manually during upgrade to purge
  // table that is no longer used from database.
  dropTable(tableName: string): Promise<void>;

  // Adds a column to existing table rows. This API does not provide any
  // consistency check. Callers are solely responsible for making sure the
  // values of |columnName| and |defaultValue| are consistent with the new
  // schema.
  addTableColumn(
      tableName: string, columnName: string,
      defaultValue: string|number|boolean|Date|ArrayBuffer|null): Promise<void>;

  dropTableColumn(tableName: string, columnName: string): Promise<void>;

  // Renames a column for all existing table rows.
  renameTableColumn(
      tableName: string, oldColumnName: string,
      newColumnName: string): Promise<void>;

  // Creates a Lovefield row structure that can be stored into raw DB instance
  // via raw transaction.
  createRow(payload: object): Row;

  // Returns version of existing DB.
  getVersion(): number;

  // Offers last resort for data rescue. This function dumps all rows in the
  // database to one single JSON object.
  // The format is a JSON object of
  //     {
  //        "table1": [ <row1>, <row2>, ..., <rowN> ],
  //        "table2": [ ... ],
  //        ...
  //        "tableM": [ ... ]
  //     }
  dump(): Promise<object>;
}
