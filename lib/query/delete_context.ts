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

import {ConstraintAction} from '../base/enum';
import {BaseTable} from '../schema/base_table';
import {Database} from '../schema/database';

import {Context} from './context';

// Internal representation of DELETE query.
export class DeleteContext extends Context {
  public from!: BaseTable;

  constructor(schema: Database) {
    super(schema);
  }

  public getScope(): Set<BaseTable> {
    const scope = new Set<BaseTable>();
    scope.add(this.from);
    this.expandTableScope(this.from.getName(), scope);
    return scope;
  }

  public clone(): DeleteContext {
    const context = new DeleteContext(this.schema);
    context.cloneBase(this);
    context.from = this.from;
    return context;
  }

  public bind(values: any[]): DeleteContext {
    super.bind(values);
    this.bindValuesInSearchCondition(values);
    return this;
  }

  // Expands the scope of the given table recursively. It takes into account
  // CASCADE foreign key constraints.
  private expandTableScope(tableName: string, scopeSoFar: Set<BaseTable>):
      void {
    const cascadeChildTables =
        this.schema.info().getChildTables(tableName, ConstraintAction.CASCADE);
    const childTables = this.schema.info().getChildTables(tableName);
    childTables.forEach(scopeSoFar.add.bind(scopeSoFar));
    cascadeChildTables.forEach((childTable) => {
      this.expandTableScope(childTable.getName(), scopeSoFar);
    }, this);
  }
}
