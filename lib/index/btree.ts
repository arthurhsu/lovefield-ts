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

import {Row} from '../base/row';
import {BTreeNode, BTreeNodePayload} from './btree_node';
import {Favor} from './comparator';
import {ComparatorType} from './comparator_type';
import {IndexHelper} from './index_helper';
import {IndexStats} from './index_stats';
import {Key, KeyRange, SingleKey, SingleKeyRange} from './key_range';
import {RuntimeIndex} from './runtime_index';

// Wrapper of the BTree.
export class BTree implements RuntimeIndex {
  public static EMPTY: number[] = [];

  // Creates tree from serialized leaves.
  public static deserialize(
      comparator: ComparatorType, name: string, uniqueKeyOnly: boolean,
      rows: Row[]): BTree {
    const tree = new BTree(name, comparator, uniqueKeyOnly);
    const newRoot = BTreeNode.deserialize(rows, tree);
    tree.root = newRoot;
    return tree;
  }

  private name: string;
  private comparatorObj: ComparatorType;
  private uniqueKeyOnly: boolean;
  private root: BTreeNode;
  private statsObj: IndexStats;

  constructor(
      name: string, comparator: ComparatorType, uniqueKeyOnly: boolean,
      data?: BTreeNodePayload[]) {
    this.name = name;
    this.comparatorObj = comparator;
    this.uniqueKeyOnly = uniqueKeyOnly;
    this.root = undefined as any as BTreeNode;
    this.statsObj = new IndexStats();
    if (data) {
      this.root = BTreeNode.fromData(this, data);
    } else {
      this.clear();
    }
  }

  public getName(): string {
    return this.name;
  }

  public toString(): string {
    return this.root.toString();
  }

  public add(key: Key, value: number): void {
    this.root = this.root.insert(key, value);
  }

  public set(key: Key, value: number): void {
    this.root = this.root.insert(key, value, true);
  }

  public remove(key: Key, rowId?: number): void {
    this.root = this.root.remove(key, rowId);
  }

  public get(key: Key): number[] {
    return this.root.get(key);
  }

  public cost(keyRange?: SingleKeyRange|KeyRange): number {
    if (keyRange === undefined || keyRange === null) {
      return this.stats().totalRows;
    }

    if (keyRange instanceof SingleKeyRange) {
      if (keyRange.isAll()) {
        return this.stats().totalRows;
      }
      if (keyRange.isOnly()) {
        // TODO(arthurhsu): this shall be further optimized
        return this.get(keyRange.from as Key).length;
      }
    }

    // TODO(arthurhsu): implement better cost calculation for ranges.
    return this.getRange([keyRange]).length;
  }

  public stats(): IndexStats {
    return this.statsObj;
  }

  public getRange(
      keyRanges?: SingleKeyRange[]|KeyRange[], reverseOrder?: boolean,
      rawLimit?: number, rawSkip?: number): number[] {
    const leftMostKey = this.root.getLeftMostNode().keys[0];
    if (leftMostKey === undefined || !rawLimit) {
      // Tree is empty or fake fetch to make query plan cached.
      return BTree.EMPTY;
    }

    const reverse = reverseOrder || false;
    const limit = (rawLimit !== undefined && rawLimit !== null) ?
        Math.min(rawLimit, this.stats().totalRows) :
        this.stats().totalRows;
    const skip = rawSkip || 0;
    const maxCount =
        Math.min(Math.max(this.stats().totalRows - skip, 0), limit);
    if (maxCount === 0) {
      return BTree.EMPTY;
    }

    if (keyRanges === undefined ||
        (keyRanges.length === 1 &&
         keyRanges[0] instanceof SingleKeyRange &&
         keyRanges[0].isAll())) {
      return this.getAll(maxCount, reverse, limit, skip);
    }

    const sortedKeyRanges = this.comparator().sortKeyRanges(keyRanges);
    // TODO(arthurhsu): Currently we did not traverse in reverse order so that
    //     the results array needs to be maxCount. Need further optimization.
    const results =
        new Array<number>(reverse ? this.stats().totalRows : maxCount);
    const params = {
      count: 0,
      limit: results.length,
      reverse: reverse,
      skip: skip,
    };

    // For all cross-column indices, use filter to handle non-continous blocks.
    const useFilter = this.comparator().keyDimensions() > 1;
    sortedKeyRanges.forEach((range) => {
      const keys = this.comparator().rangeToKeys(range);
      const key = this.comparator().isLeftOpen(range) ? leftMostKey : keys[0];
      let start = this.root.getContainingLeaf(key);
      // Need to have two strikes to stop.
      // Reason: say the nodes are [12, 15], [16, 18], when look for >15,
      //         first node will return empty, but we shall not stop there.
      let strikeCount = 0;
      while (start !== undefined &&
             start !== null &&
             params.count < params.limit) {
        if (useFilter) {
          start.getRangeWithFilter(range, params, results);
        } else {
          start.getRange(range, params, results);
        }
        if (params.skip === 0 && !start.isFirstKeyInRange(range)) {
          strikeCount++;
        } else {
          strikeCount = 0;
        }
        start = strikeCount === 2 ? null : start.next();
      }
    }, this);

    if (results.length > params.count) {
      // There are extra elements in results, truncate them.
      results.splice(params.count, results.length - params.count);
    }
    return (reverse) ?
        IndexHelper.slice(results, reverse, limit, skip) :
        results;
  }

  public clear(): void {
    this.root = BTreeNode.create(this);
    this.stats().clear();
  }

  public containsKey(key: Key|SingleKey): boolean {
    return this.root.containsKey(key);
  }

  public min(): any[]|null {
    return this.minMax(this.comparatorObj.min.bind(this.comparatorObj));
  }

  public max(): any[]|null {
    return this.minMax(this.comparatorObj.max.bind(this.comparatorObj));
  }

  public isUniqueKey(): boolean {
    return this.uniqueKeyOnly;
  }

  public comparator(): ComparatorType {
    return this.comparatorObj;
  }

  public eq(lhs: Key|SingleKey, rhs: Key|SingleKey): boolean {
    if (lhs !== undefined && lhs !== null) {
      return this.comparator().compare(lhs, rhs) === Favor.TIE;
    }
    return false;
  }

  // Converts the tree leaves into serializable rows that can be written into
  // persistent stores. Each leaf node is one row.
  public serialize(): Row[] {
    return BTreeNode.serialize(this.root.getLeftMostNode());
  }

  // Special optimization for get all values.
  // |maxCount|: max possible number of rows
  // |reverse|: retrieve the results in the reverse ordering of the comparator.
  private getAll(
      maxCount: number, reverse: boolean, limit: number, skip: number):
      number[] {
    const off = reverse ? this.stats.totalRows - maxCount - skip : skip;

    const results = new Array<number>(maxCount);
    const params = {
      count: maxCount,
      offset: off,
      startIndex: 0,
    };
    this.root.fill(params, results);
    return reverse ? results.reverse() : results;
  }

  // If the first dimension of key is null, returns null, otherwise returns the
  // results for min()/max().
  private checkNullKey(node: BTreeNode, index: number): any[]|null {
    if (!this.comparator().comparable(node.keys[index])) {
      if (node.keys[index].length > 1) {
        if (node.keys[index][0] === null) {
          return null;
        }
      } else {
        return null;
      }
    }
    return [
      node.keys[index],
      this.uniqueKeyOnly ? [node.values[index]] : node.values[index],
    ];
  }

  private findLeftMost(): BTreeNode|null {
    let node: BTreeNode|null = this.root.getLeftMostNode();
    let index = 0;
    do {
      if (index >= node.keys.length) {
        node = node.next;
        index = 0;
        continue;
      }

      const results = this.checkNullKey(node, index);
      if (results !== null) {
        return results;
      }

      index++;
    } while (node !== null);
    return null;
  }

  private findRightMost(): BTreeNode|null {
    let node: BTreeNode|null = this.root.getRightMostNode();
    let index = node.keys.length - 1;
    do {
      if (index < 0) {
        node = node.prev;
        index = 0;
        continue;
      }

      const results = this.checkNullKey(node, index);
      if (results !== null) {
        return results;
      }

      index--;
    } while (node !== null);
    return null;
  }

  private minMax(compareFn: (l: Key, r: Key) => Favor): any[]|null {
    const leftMost = this.findLeftMost();
    const rightMost = this.findRightMost();

    if (leftMost === null || rightMost === null) {
      return null;
    }

    return compareFn(leftMost[0], rightMost[0]) === Favor.LHS ?
        leftMost : rightMost;
  }
}
