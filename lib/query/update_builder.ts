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

import {Binder} from '../base/bind';
import {ErrorCode} from '../base/enum';
import {Exception} from '../base/exception';
import {Global} from '../base/global';
import {Service} from '../base/service';
import {Predicate} from '../pred/predicate';
import {Column} from '../schema/column';
import {Table} from '../schema/table';
import {BaseBuilder} from './base_builder';
import {UpdateContext} from './update_context';

export class UpdateBuilder extends BaseBuilder<UpdateContext> {
  constructor(global: Global, table: Table) {
    super(global, new UpdateContext(global.getService(Service.SCHEMA)));
    this.query.table = table;
  }

  set(column: Column, value: unknown): UpdateBuilder {
    const set = {
      binding: value instanceof Binder ? (value as Binder).index : -1,
      column,
      value,
    };

    if (this.query.set) {
      this.query.set.push(set);
    } else {
      this.query.set = [set];
    }
    return this;
  }

  where(predicate: Predicate): UpdateBuilder {
    this.assertWherePreconditions();
    this.query.where = predicate;
    return this;
  }

  assertExecPreconditions(): void {
    super.assertExecPreconditions();
    if (this.query.set === undefined || this.query.set === null) {
      // 532: Invalid usage of update().
      throw new Exception(ErrorCode.INVALID_UPDATE);
    }

    const notBound = this.query.set.some(set => set.value instanceof Binder);
    if (notBound) {
      // 501: Value is not bounded.
      throw new Exception(ErrorCode.UNBOUND_VALUE);
    }
  }

  private assertWherePreconditions(): void {
    if (this.query.where) {
      // 516: where() has already been called.
      throw new Exception(ErrorCode.DUPLICATE_WHERE);
    }
  }
}
