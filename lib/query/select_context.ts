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
import {Order} from '../base/enum';
import {BaseColumn} from '../schema/base_column';
import {BaseTable} from '../schema/base_table';
import {Database} from '../schema/database';

import {Context} from './context';

export interface SelectContextOrderBy {
  column: BaseColumn;
  order: Order;
}

// Internal representation of SELECT query.
export class SelectContext extends Context {
  public static orderByToString(orderBy: SelectContextOrderBy[]): string {
    let out = '';
    orderBy.forEach((orderByEl, index) => {
      out += orderByEl.column.getNormalizedName() + ' ';
      out += orderByEl.order === Order.ASC ? 'ASC' : 'DESC';
      if (index < orderBy.length - 1) {
        out += ', ';
      }
    });

    return out;
  }

  public columns!: BaseColumn[];
  public from!: BaseTable[];
  public limit!: number;
  public skip!: number;
  public orderBy!: SelectContextOrderBy[];
  public groupBy!: BaseColumn[];
  public limitBinder!: Binder;
  public skipBinder!: Binder;
  public outerJoinPredicates!: Set<number>;

  constructor(schema: Database) {
    super(schema);
  }

  public getScope(): Set<BaseTable> {
    return new Set<BaseTable>(this.from);
  }

  public clone(): SelectContext {
    const context = new SelectContext(this.schema);
    context.cloneBase(this);
    if (this.columns) {
      context.columns = this.columns.slice();
    }
    if (this.from) {
      context.from = this.from.slice();
    }
    context.limit = this.limit;
    context.skip = this.skip;
    if (this.orderBy) {
      context.orderBy = this.orderBy.slice();
    }
    if (this.groupBy) {
      context.groupBy = this.groupBy.slice();
    }
    if (this.limitBinder) {
      context.limitBinder = this.limitBinder;
    }
    if (this.skipBinder) {
      context.skipBinder = this.skipBinder;
    }
    context.outerJoinPredicates = this.outerJoinPredicates;
    return context;
  }

  public bind(values: any[]): SelectContext {
    super.bind(values);

    if (this.limitBinder !== undefined && this.limitBinder !== null) {
      this.limit = values[this.limitBinder.index] as number;
    }
    if (this.skipBinder !== undefined && this.skipBinder !== null) {
      this.skip = values[this.skipBinder.index] as number;
    }
    this.bindValuesInSearchCondition(values);
    return this;
  }
}
