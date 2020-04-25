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

import {Column} from '../schema/column';
import {Table} from '../schema/table';
import {DatabaseSchema} from '../schema/database_schema';
import {Info} from '../schema/info';

import {Context} from './context';

interface UpdateSetContext {
  binding?: number;
  column: Column;
  value: unknown;
}

// Internal representation of UPDATE query.
export class UpdateContext extends Context {
  table!: Table;
  set!: UpdateSetContext[];

  constructor(dbSchema: DatabaseSchema) {
    super(dbSchema);
  }

  getScope(): Set<Table> {
    const scope = new Set<Table>();
    scope.add(this.table);
    const columns = this.set.map(col => col.column.getNormalizedName());
    const info = Info.from(this.schema);
    info.getParentTablesByColumns(columns).forEach(scope.add.bind(scope));
    info.getChildTablesByColumns(columns).forEach(scope.add.bind(scope));
    return scope;
  }

  clone(): UpdateContext {
    const context = new UpdateContext(this.schema);
    context.cloneBase(this);
    context.table = this.table;
    context.set = this.set ? this.cloneSet(this.set) : this.set;
    return context;
  }

  bind(values: unknown[]): UpdateContext {
    super.bind(values);

    this.set.forEach(set => {
      if (set.binding !== undefined && set.binding !== -1) {
        set.value = values[set.binding as number];
      }
    });
    this.bindValuesInSearchCondition(values);
    return this;
  }

  private cloneSet(set: UpdateSetContext[]): UpdateSetContext[] {
    return set.map(src => {
      const clone = {...src};
      return clone;
    });
  }
}
