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

import { Binder } from '../base/bind';
import { ErrorCode } from '../base/enum';
import { Exception } from '../base/exception';
import { Global } from '../base/global';
import { Row } from '../base/row';
import { Service } from '../base/service';
import { Table } from '../schema/table';
import { BaseTable } from '../schema/base_table';
import { BaseBuilder } from './base_builder';
import { InsertContext } from './insert_context';

export class InsertBuilder extends BaseBuilder<InsertContext> {
  constructor(global: Global, allowReplace = false) {
    super(global, new InsertContext(global.getService(Service.SCHEMA)));
    this.query.allowReplace = allowReplace;
  }

  assertExecPreconditions(): void {
    super.assertExecPreconditions();
    const context = this.query;

    if (
      context.into === undefined ||
      context.into === null ||
      context.values === undefined ||
      context.values === null
    ) {
      // 518: Invalid usage of insert().
      throw new Exception(ErrorCode.INVALID_INSERT);
    }

    // "Insert or replace" makes no sense for tables that do not have a primary
    // key.
    if (
      context.allowReplace &&
      (context.into as BaseTable).getConstraint().getPrimaryKey() === null
    ) {
      // 519: Attempted to insert or replace in a table with no primary key.
      throw new Exception(ErrorCode.INVALID_INSERT_OR_REPLACE);
    }
  }

  into(table: Table): InsertBuilder {
    this.assertIntoPreconditions();
    this.query.into = table;
    return this;
  }

  values(rows: Binder | Binder[] | Row[]): InsertBuilder {
    this.assertValuesPreconditions();
    if (
      rows instanceof Binder ||
      (rows as unknown[]).some(r => r instanceof Binder)
    ) {
      this.query.binder = rows;
    } else {
      this.query.values = rows as Row[];
    }
    return this;
  }

  // Asserts whether the preconditions for calling the into() method are met.
  private assertIntoPreconditions(): void {
    if (this.query.into !== undefined && this.query.into !== null) {
      // 520: into() has already been called.
      throw new Exception(ErrorCode.DUPLICATE_INTO);
    }
  }

  // Asserts whether the preconditions for calling the values() method are met.
  private assertValuesPreconditions(): void {
    if (this.query.values !== undefined && this.query.values !== null) {
      // 521: values() has already been called.
      throw new Exception(ErrorCode.DUPLICATE_VALUES);
    }
  }
}
