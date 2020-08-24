/**
 * Copyright 2020 The Lovefield Project Authors. All Rights Reserved.
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

import {DatabaseSchema} from './database_schema';
import {Global} from '../base/global';
import {ConnectOptions} from './connect_options';
import {DatabaseConnection} from '../base/database_connection';
import {TableBuilder} from './table_builder';
import {Pragma} from './pragma';

// @export
export interface Builder {
  // Constructor syntax itself violates the no any rule.
  // new (dbName: string, dbVersion: number): any;

  getSchema(): DatabaseSchema;
  getGlobal(): Global;

  // Instantiates a connection to the database. Note: This method can only be
  // called once per Builder instance. Subsequent calls will throw an error,
  // unless the previous DB connection has been closed first.
  connect(options?: ConnectOptions): Promise<DatabaseConnection>;

  createTable(tableName: string): TableBuilder;
  setPragma(pragma: Pragma): Builder;
}
