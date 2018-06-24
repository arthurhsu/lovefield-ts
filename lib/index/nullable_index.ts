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

import {ErrorCode, Exception} from '../base/exception';
import {Row} from '../base/row';
import {IndexStats} from './index_stats';
import {Key, KeyRange, SingleKeyRange} from './key_range';
import {RuntimeIndex} from './runtime_index';

// Wraps another index which does not support NULL to accept NULL values.
export class NullableIndex implements RuntimeIndex {
  public static deserialize(
      deserializeFn: (rows: Row[]) => RuntimeIndex,
      rows: Row[]): NullableIndex {
    // Ideally, the special row should be the first one, and we can short cut.
    let index = -1;
    for (let i = 0; i < rows.length; ++i) {
      if (rows[i].id() === NullableIndex.NULL_ROW_ID) {
        index = i;
        break;
      }
    }
    if (index === -1) {
      // 102: Data corruption detected.
      throw new Exception(ErrorCode.DATA_CORRUPTION);
    }

    const nulls = rows[index].payload() as number[];
    const newRows = rows.slice(0);
    newRows.splice(index, 1);
    const tree = deserializeFn(newRows);
    const nullableIndex = new NullableIndex(tree);
    nulls.forEach((rowId) => nullableIndex.nulls.add(rowId));
    return nullableIndex;
  }

  private static NULL_ROW_ID = -2;

  private nulls: Set<number>;
  private statsNull: IndexStats;
  private statsObj: IndexStats;

  constructor(private index: RuntimeIndex) {
    this.nulls = new Set<number>();
    this.statsNull = new IndexStats();
    this.statsObj = new IndexStats();
  }

  public getName(): string {
    return this.index.getName();
  }

  public add(key: Key, value: number): void {
    if (key === null) {
      // Note: Nullable index allows multiple nullable keys even if it is marked
      // as unique. This is matching the behavior of other SQL engines.
      this.nulls.add(value);
      this.statsNull.add(key, 1);
    } else {
      this.index.add(key, value);
    }
  }

  public set(key: Key, value: number): void {
    if (key === null) {
      this.nulls.clear();
      this.statsNull.clear();
      this.add(key, value);
    } else {
      this.index.set(key, value);
    }
  }

  public remove(key: Key, rowId?: number): void {
    if (key === null) {
      if (rowId) {
        this.nulls.delete(rowId);
        this.statsNull.remove(key, 1);
      } else {
        this.nulls.clear();
        this.statsNull.clear();
      }
    } else {
      this.index.remove(key, rowId);
    }
  }

  public get(key: Key): number[] {
    if (key === null) {
      return Array.from(this.nulls.values());
    } else {
      return this.index.get(key);
    }
  }

  public min(): any[]|null {
    return this.index.min();
  }

  public max(): any[]|null {
    return this.index.max();
  }

  public cost(keyRange?: SingleKeyRange|KeyRange): number {
    return this.index.cost(keyRange);
  }

  public getRange(
      range?: SingleKeyRange[]|KeyRange[], reverseOrder?: boolean,
      limit?: number, skip?: number): number[] {
    const results = this.index.getRange(range, reverseOrder, limit, skip);
    if (range !== undefined && range !== null) {
      return results;
    }

    return results.concat(Array.from(this.nulls.values()));
  }

  public clear(): void {
    this.nulls.clear();
    this.index.clear();
  }

  public containsKey(key: Key): boolean {
    return key === null ? this.nulls.size !== 0 : this.index.containsKey(key);
  }

  public serialize(): Row[] {
    const rows =
        [new Row(NullableIndex.NULL_ROW_ID, Array.from(this.nulls.values()))];
    return rows.concat(this.index.serialize());
  }

  public comparator(): any {
    return this.index.comparator();
  }

  public isUniqueKey(): boolean {
    return this.index.isUniqueKey();
  }

  public stats(): IndexStats {
    this.statsObj.updateFromList([this.index.stats(), this.statsNull]);
    return this.statsObj;
  }
}
