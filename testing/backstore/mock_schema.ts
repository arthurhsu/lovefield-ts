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

import { Order, Type } from '../../lib/base/enum';
import { BaseTable } from '../../lib/schema/base_table';
import { DatabaseSchema } from '../../lib/schema/database_schema';
import { Info } from '../../lib/schema/info';
import { Pragma } from '../../lib/schema/pragma';
import { TableBuilder } from '../../lib/schema/table_builder';

// Dummy schema implementation to be used in tests.
export class MockSchema implements DatabaseSchema {
  private tableA: BaseTable;
  private tableB: BaseTable;
  private tablePlusOne: BaseTable;
  private name_: string;
  private version_: number;
  private simulateDropTableA: boolean;
  private pragma_: Pragma;
  private info_!: Info;

  constructor() {
    this.tableA = this.createTable('tableA');
    this.tableB = this.createTable('tableB');
    this.tablePlusOne = this.createTable('tablePlusOne');
    this.name_ = 'mock_schema';
    this.version_ = 1;
    this.simulateDropTableA = false;
    this.pragma_ = { enableBundledMode: false };
  }

  name(): string {
    return this.name_;
  }

  version(): number {
    return this.version_;
  }

  tables(): BaseTable[] {
    const tables = [this.tableB];

    if (!this.simulateDropTableA) {
      tables.unshift(this.tableA);
    }
    if (this.version_ > 1) {
      tables.push(this.tablePlusOne);
    }
    return tables;
  }

  info(): Info {
    if (!this.info_) {
      this.info_ = new Info(this);
    }
    return this.info_;
  }

  table(tableName: string): BaseTable {
    interface TestTableType {
      [key: string]: BaseTable;
    }
    const tables: TestTableType = {
      tableB: this.tableB,
    };
    if (!this.simulateDropTableA) {
      tables['tableA'] = this.tableA;
    }
    if (this.version_ > 1) {
      tables['tablePlusOne'] = this.tablePlusOne;
    }
    return tables[tableName] || null;
  }

  pragma(): Pragma {
    return this.pragma_;
  }

  setName(name: string): void {
    this.name_ = name;
  }

  setVersion(version: number): void {
    this.version_ = version;
  }

  setBundledMode(mode: boolean): void {
    this.pragma_.enableBundledMode = mode;
  }

  setDropTableA(mode: boolean): void {
    this.simulateDropTableA = mode;
  }

  private createTable(tableName: string): BaseTable {
    return new TableBuilder(tableName)
      .addColumn('id', Type.STRING)
      .addColumn('name', Type.STRING)
      .addPrimaryKey(['id'])
      .addIndex('idxName', ['name'], false, Order.DESC)
      .getSchema();
  }
}
