/**
 * Copyright 2016 The Lovefield Project Authors. All Rights Reserved.
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

import {ErrorCode, Exception} from '../base/exception';

import {Database} from './database';
import {Info} from './info';
import {Pragma} from './pragma';
import {Table} from './table';

export class DatabaseSchema implements Database {
  public _pragma: Pragma;
  private _info: Info;
  private tableMap: Map<string, Table>;

  constructor(readonly _name: string, readonly _version: number) {
    this.tableMap = new Map<string, Table>();
    this._pragma = {enableBundledMode: false};
    this._info = undefined as any as Info;
  }

  public name(): string {
    return this._name;
  }

  public version(): number {
    return this._version;
  }

  public info(): Info {
    if (this._info === undefined) {
      this._info = new Info(this);
    }
    return this._info;
  }

  public tables(): Table[] {
    return Array.from(this.tableMap.values());
  }

  public table(tableName: string): Table {
    const ret = this.tableMap.get(tableName);
    if (!ret) {
      // 101: Table {0} not found.
      throw new Exception(ErrorCode.TABLE_NOT_FOUND, tableName);
    }
    return ret;
  }

  public setTable(table: Table) {
    this.tableMap.set(table.getName(), table);
  }

  public pragma(): Pragma {
    return this._pragma;
  }
}
