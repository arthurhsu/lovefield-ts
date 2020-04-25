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

import {ErrorCode, Order} from '../base/enum';
import {Exception} from '../base/exception';
import {Favor} from '../base/private_enum';
import {Row} from '../base/row';

import {Comparator} from './comparator';
import {IndexHelper} from './index_helper';
import {IndexStats} from './index_stats';
import {Key, KeyRange, SingleKey, SingleKeyRange} from './key_range';
import {RuntimeIndex} from './runtime_index';
import {SimpleComparator} from './simple_comparator';

// This is actually the row id set for a given table, but in the form of
// RuntimeIndex.
export class RowId implements RuntimeIndex {
  // The Row ID to use when serializing this index to disk. Currently the entire
  // index is serialized to a single lf.Row instance with rowId set to ROW_ID.
  static ROW_ID = 0;

  static deserialize(name: string, rows: Row[]): RowId {
    const index = new RowId(name);
    const rowIds: number[] = rows[0].payload()['v'] as number[];
    rowIds.forEach(rowId => index.add(rowId, rowId));
    return index;
  }

  private static EMPTY_ARRAY: number[] = [];

  private rows: Set<SingleKey>;
  private comparatorObj: SimpleComparator;

  constructor(private name: string) {
    this.rows = new Set<SingleKey>();
    this.comparatorObj = new SimpleComparator(Order.ASC);
  }

  getName(): string {
    return this.name;
  }

  add(key: Key, value: number): void {
    if (typeof key !== 'number') {
      // 103: Row id must be numbers.
      throw new Exception(ErrorCode.INVALID_ROW_ID);
    }
    this.rows.add(key);
  }

  set(key: Key, value: number): void {
    this.add(key, value);
  }

  remove(key: Key, rowId?: number): void {
    this.rows.delete(key as SingleKey);
  }

  get(key: Key): number[] {
    return this.containsKey(key) ? [key as number] : RowId.EMPTY_ARRAY;
  }

  min(): unknown[] | null {
    return this.minMax(this.comparatorObj.min.bind(this.comparatorObj));
  }

  max(): unknown[] | null {
    return this.minMax(this.comparatorObj.max.bind(this.comparatorObj));
  }

  cost(keyRange?: SingleKeyRange | KeyRange): number {
    // Give the worst case so that this index is not used unless necessary.
    return this.rows.size;
  }

  getRange(
    range?: SingleKeyRange[] | KeyRange[],
    reverseOrder?: boolean,
    limit?: number,
    skip?: number
  ): number[] {
    const keyRanges: SingleKeyRange[] = (range as SingleKeyRange[]) || [
      SingleKeyRange.all(),
    ];
    const values: number[] = Array.from(this.rows.values()).filter(value => {
      return keyRanges.some(r => this.comparatorObj.isInRange(value, r));
    }, this) as number[];
    return IndexHelper.slice(values, reverseOrder, limit, skip);
  }

  clear(): void {
    this.rows.clear();
  }

  containsKey(key: Key): boolean {
    return this.rows.has(key as SingleKey);
  }

  serialize(): Row[] {
    return [new Row(RowId.ROW_ID, {v: Array.from(this.rows.values())})];
  }

  comparator(): Comparator {
    return this.comparatorObj;
  }

  isUniqueKey(): boolean {
    return true;
  }

  stats(): IndexStats {
    const stats = new IndexStats();
    stats.totalRows = this.rows.size;
    return stats;
  }

  private minMax(
    compareFn: (l: SingleKey, r: SingleKey) => Favor
  ): unknown[] | null {
    if (this.rows.size === 0) {
      return null;
    }

    const keys = Array.from(this.rows.values()).reduce((keySoFar, key) => {
      return keySoFar === null || compareFn(key, keySoFar) === Favor.LHS
        ? key
        : keySoFar;
    });

    return [keys, [keys]];
  }
}
