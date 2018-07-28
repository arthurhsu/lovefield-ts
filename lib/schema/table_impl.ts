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

import {ErrorCode, Order, Type} from '../base/enum';
import {EvalRegistry} from '../base/eval';
import {Exception} from '../base/exception';
import {RawRow, Row} from '../base/row';
import {Key, SingleKey} from '../index/key_range';

import {BaseColumn} from './base_column';
import {BaseTable} from './base_table';
import {ColumnDef} from './column_def';
import {ColumnImpl} from './column_impl';
import {Constraint} from './constraint';
import {ForeignKeySpec} from './foreign_key_spec';
import {IndexImpl} from './index_impl';
import {IndexedColumn, IndexedColumnSpec} from './indexed_column';
import {RowImpl} from './row_impl';

export class TableImpl implements BaseTable {
  public static ROW_ID_INDEX_PATTERN = '#';
  private static EMPTY_INDICES: IndexImpl[] = [];

  private _alias: string;
  private _columns: BaseColumn[];
  private _constraint: Constraint;

  private _referencingFK: ForeignKeySpec[];
  private _functionMap: Map<string, (column: any) => Key>;
  private _evalRegistry: EvalRegistry;

  constructor(
      readonly _name: string, cols: ColumnDef[], private _indices: IndexImpl[],
      readonly _usePersistentIndex: boolean, alias?: string) {
    this._columns = [];
    cols.forEach((col) => {
      const colSchema =
          new ColumnImpl(this, col.name, col.unique, col.nullable, col.type);
      this[col.name] = colSchema;
      this._columns.push(colSchema);
    }, this);
    this._referencingFK = null as any as ForeignKeySpec[];
    this._functionMap = null as any as Map<string, (column: any) => Key>;
    this._constraint = null as any as Constraint;
    this._evalRegistry = EvalRegistry.get();
    this._alias = alias ? alias : null as any as string;
  }

  public getName(): string {
    return this._name;
  }

  public getAlias(): string {
    return this._alias;
  }

  public getEffectiveName(): string {
    return this._alias || this._name;
  }

  public getIndices(): IndexImpl[] {
    return this._indices || TableImpl.EMPTY_INDICES;
  }

  public getColumns(): BaseColumn[] {
    return this._columns;
  }

  public getConstraint(): Constraint {
    return this._constraint;
  }

  public persistentIndex(): boolean {
    return this._usePersistentIndex;
  }

  public as(name: string): BaseTable {
    const colDef = this._columns.map((col) => {
      return {
        name: col.getName(),
        nullable: col.isNullable(),
        type: col.getType(),
        unique: col.isUnique(),
      };
    });
    const clone = new TableImpl(
        this._name, colDef, this._indices, this._usePersistentIndex, name);
    clone._referencingFK = this._referencingFK;
    clone._constraint = this._constraint;
    clone._alias = name;
    return clone;
  }

  public getRowIdIndexName(): string {
    return `${this._name}.${TableImpl.ROW_ID_INDEX_PATTERN}`;
  }

  public createRow(value?: object): Row {
    return new RowImpl(
        this._functionMap, this._columns, this._indices, Row.getNextId(),
        value);
  }

  public deserializeRow(dbRecord: RawRow): Row {
    const obj = {};
    this._columns.forEach((col) => {
      const key = col.getName();
      const type = col.getType();
      let value: any = dbRecord.value[key];
      if (type === Type.ARRAY_BUFFER) {
        value = Row.hexToBin(value as string);
      } else if (type === Type.DATE_TIME) {
        value = (value !== null) ? new Date(value as number) : null;
      }
      obj[key] = value;
    });
    return new RowImpl(
        this._functionMap, this._columns, this._indices, dbRecord.id, obj);
  }

  public constructIndices(
      pkName: string, indices: Map<string, IndexedColumnSpec[]>,
      uniqueIndices: Set<string>, nullable: Set<string>,
      fkSpecs: ForeignKeySpec[]): void {
    if (indices.size === 0) {
      this._constraint = new Constraint(null as any as IndexImpl, [], []);
      return;
    }

    const columnMap = new Map<string, BaseColumn>();
    this._columns.forEach((col) => columnMap.set(col.getName(), col));

    this._indices = Array.from(indices.keys()).map((indexName) => {
      return new IndexImpl(
          this._name, indexName, uniqueIndices.has(indexName),
          this.generateIndexedColumns(indices, columnMap, indexName));
    });

    this._functionMap = new Map<string, (column: any) => Key>();
    this._indices.forEach(
        (index) => this._functionMap.set(
            index.getNormalizedName(), this.getKeyOfIndexFn(columnMap, index)));

    const pk: IndexImpl = (pkName === null) ?
        null as any as IndexImpl :
        new IndexImpl(
            this._name, pkName, true,
            this.generateIndexedColumns(indices, columnMap, pkName));
    const notNullable =
        this._columns.filter((col) => !nullable.has(col.getName()));
    this._constraint = new Constraint(pk, notNullable, fkSpecs);
  }

  private generateIndexedColumns(
      indices: Map<string, IndexedColumnSpec[]>,
      columnMap: Map<string, BaseColumn>, indexName: string): IndexedColumn[] {
    const index = indices.get(indexName);
    if (index) {
      return index.map((indexedColumn) => {
        return {
          autoIncrement: indexedColumn.autoIncrement as any as boolean,
          order: indexedColumn.order as any as Order,
          schema: columnMap.get(indexedColumn.name) as any as BaseColumn,
        };
      });
    }
    throw new Exception(ErrorCode.ASSERTION);
  }

  private getSingleKeyFn(
      columnMap: Map<string, BaseColumn>,
      column: BaseColumn): (column: any) => Key {
    const col = columnMap.get(column.getName());
    if (col) {
      const colType = col.getType();
      const keyOfIndexFn = this._evalRegistry.getKeyOfIndexEvaluator(colType);
      return (payload: any) =>
                 keyOfIndexFn(payload[column.getName()]) as SingleKey;
    }
    throw new Exception(ErrorCode.ASSERTION);
  }

  private getMultiKeyFn(
      columnMap: Map<string, BaseColumn>,
      columns: IndexedColumn[]): (column: any) => Key {
    const getSingleKeyFunctions =
        columns.map((col) => this.getSingleKeyFn(columnMap, col.schema));
    return (payload: any) => getSingleKeyFunctions.map((fn) => fn(payload)) as
        SingleKey[] as Key;
  }

  private getKeyOfIndexFn(columnMap: Map<string, BaseColumn>, index: IndexImpl):
      (column: any) => Key {
    return index.columns.length === 1 ?
        this.getSingleKeyFn(columnMap, index.columns[0].schema) :
        this.getMultiKeyFn(columnMap, index.columns);
  }
}
