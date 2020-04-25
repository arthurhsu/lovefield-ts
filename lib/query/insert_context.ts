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
import {Row} from '../base/row';
import {Table} from '../schema/table';
import {DatabaseSchema} from '../schema/database_schema';
import {Info} from '../schema/info';

import {Context} from './context';

// Internal representation of INSERT and INSERT_OR_REPLACE queries.
export class InsertContext extends Context {
  into!: Table;
  binder!: Binder | Binder[] | Row[];
  values!: Row[];
  allowReplace!: boolean;

  constructor(dbSchema: DatabaseSchema) {
    super(dbSchema);
  }

  getScope(): Set<Table> {
    const scope = new Set<Table>();
    scope.add(this.into);
    const info = Info.from(this.schema);
    info.getParentTables(this.into.getName()).forEach(scope.add.bind(scope));
    if (this.allowReplace) {
      info.getChildTables(this.into.getName()).forEach(scope.add.bind(scope));
    }
    return scope;
  }

  clone(): InsertContext {
    const context = new InsertContext(this.schema);
    context.cloneBase(this);
    context.into = this.into;
    if (this.values) {
      context.values =
        this.values instanceof Binder ? this.values : this.values.slice();
    }
    context.allowReplace = this.allowReplace;
    context.binder = this.binder;
    return context;
  }

  bind(values: unknown[]): InsertContext {
    super.bind(values);

    if (this.binder) {
      if (this.binder instanceof Binder) {
        this.values = values[this.binder.index] as Row[];
      } else {
        this.values = (this.binder as unknown[]).map(val => {
          return (val instanceof Binder ? values[val.index] : val) as Row;
        });
      }
    }
    return this;
  }
}
