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

import {ConstraintAction, ConstraintTiming, ErrorCode, Order, Type} from '../base/enum';
import {Exception} from '../base/exception';
import {ColumnDef} from './column_def';
import {ForeignKeySpec, RawForeignKeySpec} from './foreign_key_spec';
import {IndexImpl} from './index_impl';
import {IndexedColumnSpec} from './indexed_column';
import {Table} from './table';
import {TableImpl} from './table_impl';

// Dynamic Table schema builder
export class TableBuilder {
  private static NULLABLE_TYPES_BY_DEFAULT: Set<Type> = new Set<Type>([
    Type.ARRAY_BUFFER,
    Type.OBJECT,
  ]);

  private static toPascal(name: string): string {
    return name[0].toUpperCase() + name.substring(1);
  }

  private name: string;
  private columns: Map<string, Type>;
  private uniqueColumns: Set<string>;
  private uniqueIndices: Set<string>;
  private nullable: Set<string>;
  private pkName: string;
  private indices: Map<string, IndexedColumnSpec[]>;
  private persistIndex: boolean;
  private fkSpecs: ForeignKeySpec[];

  constructor(tableName: string) {
    this.checkNamingRules(tableName);
    this.name = tableName;
    this.columns = new Map<string, Type>();
    this.uniqueColumns = new Set<string>();
    this.uniqueIndices = new Set<string>();
    this.nullable = new Set<string>();
    this.pkName = null as any as string;
    this.indices = new Map<string, IndexedColumnSpec[]>();
    this.persistIndex = false;
    this.fkSpecs = [];
  }

  public addColumn(name: string, type: Type): TableBuilder {
    this.checkNamingRules(name);
    this.checkNameConflicts(name);
    this.columns.set(name, type);
    if (TableBuilder.NULLABLE_TYPES_BY_DEFAULT.has(type)) {
      this.addNullable([name]);
    }
    return this;
  }

  // Adds a primary key to table.
  // There are two overloads of this function:
  //
  // case 1: (columns: Array<string>, autoInc)
  //   specifies primary key by given only column names with default ascending
  //   orders (Order.ASC). When autoInc is true, there can be only one
  //   column in the columns, its type must be Type.INTEGER, and its order
  //   must be the default Order.ASC.
  //
  // case 2: (columns: Array<IndexedColumnSpec>)
  //   allows different ordering per-column, but more verbose.
  public addPrimaryKey(columns: string[]|IndexedColumnSpec[], autoInc = false):
      TableBuilder {
    this.pkName = 'pk' + TableBuilder.toPascal(this.name);
    this.checkNamingRules(this.pkName);
    this.checkNameConflicts(this.pkName);
    const cols = this.normalizeColumns(columns, true, undefined, autoInc);
    this.checkPrimaryKey(cols);

    if (cols.length === 1) {
      this.uniqueColumns.add(cols[0].name);
    }
    this.uniqueIndices.add(this.pkName);
    this.indices.set(this.pkName, cols);
    return this;
  }

  // Creates a foreign key on a given table column.
  public addForeignKey(name: string, rawSpec: RawForeignKeySpec): TableBuilder {
    this.checkNamingRules(name);
    this.checkNameConflicts(name);
    if (rawSpec.action === undefined) {
      rawSpec.action = ConstraintAction.RESTRICT;
    }
    if (rawSpec.timing === undefined) {
      rawSpec.timing = ConstraintTiming.IMMEDIATE;
    }

    const spec = new ForeignKeySpec(rawSpec, this.name, name);
    if (spec.action === ConstraintAction.CASCADE &&
        spec.timing === ConstraintTiming.DEFERRABLE) {
      // 506: Lovefield allows only immediate evaluation of cascading
      // constraints.
      throw new Exception(ErrorCode.IMMEDIATE_EVAL_ONLY);
    }

    if (!this.columns.has(spec.childColumn)) {
      // 540: Foreign key {0} has invalid reference syntax.
      throw new Exception(ErrorCode.INVALID_FK_REF, `${this.name}.${name}`);
    }

    this.fkSpecs.push(spec);
    this.addIndex(
        name, [spec.childColumn], this.uniqueColumns.has(spec.childColumn));
    return this;
  }

  public addUnique(name: string, columns: string[]): TableBuilder {
    this.checkNamingRules(name);
    this.checkNameConflicts(name);
    const cols = this.normalizeColumns(columns, true);
    if (cols.length === 1) {
      this.uniqueColumns.add(cols[0].name);
      this.markFkIndexForColumnUnique(cols[0].name);
    }
    this.indices.set(name, cols);
    this.uniqueIndices.add(name);
    return this;
  }

  public addNullable(columns: string[]): TableBuilder {
    this.normalizeColumns(columns, false)
        .forEach((col) => this.nullable.add(col.name));
    return this;
  }

  // Mimics SQL CREATE INDEX.
  // There are two overloads of this function:
  // case 1: (name, columns: !Array<string>, unique, order)
  //   adds an index by column names only. All columns have same ordering.
  // case 2: (name, columns: !Array<!TableBuilder.IndexedColumnSpec>, unique)
  //   adds an index, allowing customization of ordering, but more verbose.
  public addIndex(
      name: string, columns: string[]|object[], unique = false,
      order = Order.ASC): TableBuilder {
    this.checkNamingRules(name);
    this.checkNameConflicts(name);
    this.indices.set(name, this.normalizeColumns(columns, true, order));
    if (unique) {
      this.uniqueIndices.add(name);
    }
    return this;
  }

  public persistentIndex(value: boolean): void {
    this.persistIndex = value;
  }

  public getSchema(): Table {
    this.checkPrimaryKeyNotForeignKey();
    this.checkPrimaryKeyDuplicateIndex();
    this.checkPrimaryKeyNotNullable();

    const columns: ColumnDef[] =
        Array.from(this.columns.keys()).map((colName) => {
          return {
            name: colName,
            nullable: this.nullable.has(colName) || false,
            type: this.columns.get(colName) as any as Type,
            unique: this.uniqueColumns.has(colName) || false,
          };
        });

    // Pass null as indices since Columns are not really constructed yet.
    const table = new TableImpl(
        this.name, columns, null as any as IndexImpl[], this.persistIndex);

    // Columns shall be constructed within TableImpl's ctor, now we can
    // instruct it to construct proper index schema.
    table.constructIndices(
        this.pkName, this.indices, this.uniqueIndices, this.nullable,
        this.fkSpecs);
    return table as any as Table;
  }

  public getFkSpecs(): ForeignKeySpec[] {
    return this.fkSpecs;
  }

  private checkNamingRules(name: string): void {
    if (!(/^[A-Za-z_][A-Za-z0-9_]*$/.test(name))) {
      // 502: Naming rule violation: {0}.
      throw new Exception(ErrorCode.INVALID_NAME, name);
    }
  }

  private checkNameConflicts(name: string): void {
    if (name === this.name) {
      // 546: Indices/constraints/columns can't re-use the table name {0}
      throw new Exception(ErrorCode.DUPLICATE_NAME, name);
    }

    if (this.columns.has(name) || this.indices.has(name) ||
        this.uniqueIndices.has(name)) {
      // 503: Name {0} is already defined.
      throw new Exception(ErrorCode.NAME_IN_USE, `${this.name}.${name}`);
    }
  }

  private checkPrimaryKey(columns: IndexedColumnSpec[]): void {
    let hasAutoIncrement = false;
    columns.forEach((column) => {
      const columnType = this.columns.get(column.name);
      hasAutoIncrement =
          hasAutoIncrement || column.autoIncrement as any as boolean;
      if (column.autoIncrement && columnType !== Type.INTEGER) {
        // 504: Can not use autoIncrement with a non-integer primary key.
        throw new Exception(ErrorCode.INVALID_AUTO_KEY_TYPE);
      }
    });

    if (hasAutoIncrement && columns.length > 1) {
      // 505: Can not use autoIncrement with a cross-column primary key.
      throw new Exception(ErrorCode.INVALID_AUTO_KEY_COLUMN);
    }
  }

  // Checks whether any primary key column is also used as a foreign key child
  // column, and throws an exception if such a column is found.
  private checkPrimaryKeyNotForeignKey(): void {
    if (this.pkName === null) {
      return;
    }

    const index = this.indices.get(this.pkName);
    if (index) {
      const pkColumns = index.map((column) => column.name);
      let fkSpecIndex = 0;
      const conflict = this.fkSpecs.some((fkSpec, i) => {
        fkSpecIndex = i;
        return pkColumns.indexOf(fkSpec.childColumn) !== -1;
      });
      if (conflict) {
        // 543: Foreign key {0}. A primary key column can't also be a foreign
        // key child column.
        throw new Exception(
            ErrorCode.PK_CANT_BE_FK, this.fkSpecs[fkSpecIndex].name);
      }
    }  // else nothing to check.
  }

  // Checks whether the primary key index is identical (in terms of indexed
  // columns) with another explicitly added index.
  private checkPrimaryKeyDuplicateIndex(): void {
    if (this.pkName === null) {
      return;
    }

    const index = this.indices.get(this.pkName);
    if (index) {
      const extractName = (column: IndexedColumnSpec) => column.name;
      const pkColumnsJson = JSON.stringify(index.map(extractName));

      this.indices.forEach((indexedColumnSpecs, indexName) => {
        if (indexName === this.pkName) {
          return;
        }

        if (JSON.stringify(indexedColumnSpecs.map(extractName)) ===
            pkColumnsJson) {
          // 544: Duplicate primary key index found at {0}
          throw new Exception(
              ErrorCode.DUPLICATE_PK, `${this.name}.${indexName}`);
        }
      });
    }  // else nothing to check.
  }

  // Checks whether any primary key column has also been marked as nullable.
  private checkPrimaryKeyNotNullable(): void {
    if (this.pkName === null) {
      return;
    }

    const index = this.indices.get(this.pkName);
    if (index) {
      index.forEach((indexedColumnSpec) => {
        if (this.nullable.has(indexedColumnSpec.name)) {
          // 545: Primary key column {0} can't be marked as nullable
          throw new Exception(
              ErrorCode.NULLABLE_PK, `${this.name}.${indexedColumnSpec.name}`);
        }
      });
    }  // else nothing to check.
  }

  // Convert different column representations (column name only or column
  // objects) into column object array. Also performs consistency check to make
  // sure referred columns are actually defined.
  private normalizeColumns(
      columns: string[]|object[], checkIndexable: boolean,
      sortOrder = Order.ASC, autoInc = false): IndexedColumnSpec[] {
    let normalized: IndexedColumnSpec[] = null as any as IndexedColumnSpec[];
    if (typeof (columns[0]) === 'string') {
      const array = columns as string[];
      normalized = array.map((col) => {
        return {
          autoIncrement: autoInc || false,
          name: col,
          order: sortOrder,
        };
      });
    } else {
      normalized = columns as IndexedColumnSpec[];
    }

    normalized.forEach((col) => {
      if (!this.columns.has(col.name)) {
        // 508: Table {0} does not have column: {1}.
        throw new Exception(ErrorCode.COLUMN_NOT_FOUND, this.name, col.name);
      }
      if (checkIndexable) {
        const type = this.columns.get(col.name);
        if (type === Type.ARRAY_BUFFER || type === Type.OBJECT) {
          // 509: Attempt to index table {0} on non-indexable column {1}.
          throw new Exception(
              ErrorCode.COLUMN_NOT_INDEXABLE, this.name, col.name);
        }
      }
    });
    return normalized;
  }

  private markFkIndexForColumnUnique(column: string): void {
    this.fkSpecs.forEach((fkSpec) => {
      if (fkSpec.childColumn === column) {
        this.uniqueIndices.add(fkSpec.name.split('.')[1]);
      }
    });
  }
}
