/**
 * @license
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
import {MapSet} from '../structs/map_set';
import {Database} from './database';
import {ForeignKeySpec} from './foreign_key_spec';
import {Table} from './table';

// Read-only objects that provides information for schema metadata.
export class Info {
  private schema: Database;

  // A mapping from table name to its referencing CASCADE foreign keys.
  private cascadeReferringFk: MapSet<string, ForeignKeySpec>;

  // A mapping from table name to its referencing RESTRICT foreign keys.
  private restrictReferringFk: MapSet<string, ForeignKeySpec>;

  // The map of table name to their parent tables.
  private parents: MapSet<string, Table>;

  // The map of fully qualified column name to its parent table name.
  private colParent: Map<string, string>;

  // The map of table to their child tables.
  private children: MapSet<string, Table>;
  private cascadeChildren: MapSet<string, Table>;
  private restrictChildren: MapSet<string, Table>;

  // The map of full qualified column name to their child table name.
  private colChild: MapSet<string, string>;

  constructor(dbSchema: Database) {
    this.schema = dbSchema;
    this.cascadeReferringFk = new MapSet();
    this.restrictReferringFk = new MapSet();
    this.parents = new MapSet();
    this.colParent = new Map();
    this.children = new MapSet();
    this.cascadeChildren = new MapSet();
    this.restrictChildren = new MapSet();
    this.colChild = new MapSet();

    this.schema.tables().forEach((table) => {
      const tableName = table.getName();
      table.getConstraint().getForeignKeys().forEach((fkSpec) => {
        this.parents.set(tableName, this.schema.table(fkSpec.parentTable));
        this.children.set(fkSpec.parentTable, table);
        if (fkSpec.action === ConstraintAction.RESTRICT) {
          this.restrictReferringFk.set(fkSpec.parentTable, fkSpec);
          this.restrictChildren.set(fkSpec.parentTable, table);
        } else {  // fkspec.action === ConstraintAction.CASCADE
          this.cascadeReferringFk.set(fkSpec.parentTable, fkSpec);
          this.cascadeChildren.set(fkSpec.parentTable, table);
        }

        this.colParent.set(
            table.getName() + '.' + fkSpec.childColumn, fkSpec.parentTable);

        const ref = `${fkSpec.parentTable}.${fkSpec.parentColumn}`;
        this.colChild.set(ref, table.getName());
      }, this);
    }, this);
  }

  // Looks up referencing foreign key for a given table.
  // If no constraint action type were provided, all types are included.
  public getReferencingForeignKeys(
      tableName: string,
      constraintAction?: ConstraintAction): ForeignKeySpec[]|null {
    if (constraintAction !== undefined && constraintAction !== null) {
      return constraintAction === ConstraintAction.CASCADE ?
          this.cascadeReferringFk.get(tableName) :
          this.restrictReferringFk.get(tableName);
    } else {
      const cascadeConstraints = this.cascadeReferringFk.get(tableName);
      const restrictConstraints = this.restrictReferringFk.get(tableName);
      if (cascadeConstraints === null && restrictConstraints === null) {
        return null;
      } else {
        return (cascadeConstraints || []).concat(restrictConstraints || []);
      }
    }
  }

  // Look up parent tables for given tables.
  public getParentTables(tableName: string): Table[] {
    return this.expandScope(tableName, this.parents);
  }

  // Looks up parent tables for a given column set.
  public getParentTablesByColumns(colNames: string[]): Table[] {
    const tableNames = new Set<string>();
    colNames.forEach((col) => {
      const table = this.colParent.get(col);
      if (table) {
        tableNames.add(table);
      }
    }, this);
    const tables = Array.from(tableNames.values());
    return tables.map((tableName) => {
      return this.schema.table(tableName);
    }, this);
  }

  // Looks up child tables for given tables.
  public getChildTables(tableName: string, constraintAction?: ConstraintAction):
      Table[] {
    if (!(constraintAction !== undefined && constraintAction !== null)) {
      return this.expandScope(tableName, this.children);
    } else if (constraintAction === ConstraintAction.RESTRICT) {
      return this.expandScope(tableName, this.restrictChildren);
    } else {
      return this.expandScope(tableName, this.cascadeChildren);
    }
  }

  // Looks up child tables for a given column set.
  public getChildTablesByColumns(colNames: string[]): Table[] {
    const tableNames = new Set<string>();
    colNames.forEach((col) => {
      const children = this.colChild.get(col);
      if (children) {
        children.forEach((child) => {
          tableNames.add(child);
        });
      }
    }, this);
    const tables = Array.from(tableNames.values());
    return tables.map((tableName) => {
      return this.schema.table(tableName);
    }, this);
  }

  private expandScope(tableName: string, map: MapSet<string, Table>): Table[] {
    const values = map.get(tableName);
    return values === null ? [] : values;
  }
}
