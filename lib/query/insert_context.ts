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
import {Database} from '../schema/database';
import {Table} from '../schema/table';
import {Context} from './context';

// Internal representation of INSERT and INSERT_OR_REPLACE queries.
export class InsertContext extends Context {
  public into!: Table;
  public binder!: Binder|Binder[]|Row[];
  public values!: Row[];
  public allowReplace!: boolean;

  constructor(schema: Database) {
    super(schema);
  }

  public getScope(): Set<Table> {
    const scope = new Set<Table>();
    scope.add(this.into);
    const info = this.schema.info();
    info.getParentTables(this.into.getName()).forEach(scope.add.bind(scope));
    if (this.allowReplace) {
      info.getChildTables(this.into.getName()).forEach(scope.add.bind(scope));
    }
    return scope;
  }

  public clone(): InsertContext {
    const context = new InsertContext(this.schema);
    context.cloneBase(this);
    context.into = this.into;
    if (this.values) {
      context.values =
          (this.values instanceof Binder) ? this.values : this.values.slice();
    }
    context.allowReplace = this.allowReplace;
    context.binder = this.binder;
    return context;
  }

  public bind(values: any[]): InsertContext {
    super.bind(values);

    if (this.binder) {
      if (this.binder instanceof Binder) {
        this.values = values[this.binder.index];
      } else {
        this.values = (this.binder as any[]).map((val) => {
          return val instanceof Binder ? values[val.index] : val;
        });
      }
    }
    return this;
  }
}
