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

import {ErrorCode} from '../base/enum';
import {Exception} from '../base/exception';
import {Global} from '../base/global';
import {Service} from '../base/service';
import {Predicate} from '../pred/predicate';
import {Table} from '../schema/table';
import {BaseBuilder} from './base_builder';
import {DeleteContext} from './delete_context';

export class DeleteBuilder extends BaseBuilder<DeleteContext> {
  constructor(global: Global) {
    super(global, new DeleteContext(global.getService(Service.SCHEMA)));
  }

  public from(table: Table): DeleteBuilder {
    this.assertFromPreconditions();
    this.query.from = table;
    return this;
  }

  public where(predicate: Predicate): DeleteBuilder {
    this.assertWherePreconditions();
    this.query.where = predicate;
    return this;
  }

  public assertExecPreconditions(): void {
    super.assertExecPreconditions();
    if (this.query.from === undefined || this.query.from === null) {
      // 517: Invalid usage of delete().
      throw new Exception(ErrorCode.INVALID_DELETE);
    }
  }

  private assertFromPreconditions(): void {
    if (this.query.from) {
      // 515: from() has already been called.
      throw new Exception(ErrorCode.DUPLICATE_FROM);
    }
  }

  private assertWherePreconditions(): void {
    if (this.query.from === undefined || this.query.from === null) {
      // 548: from() has to be called before where().
      throw new Exception(ErrorCode.FROM_AFTER_WHERE);
    }
    if (this.query.where) {
      // 516: where() has already been called.
      throw new Exception(ErrorCode.DUPLICATE_WHERE);
    }
  }
}
