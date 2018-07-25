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
// tslint:disable:max-classes-per-file
import {ErrorCode} from '../base/enum';
import {Exception} from '../base/exception';
import {Favor} from '../base/private_enum';
import {Row} from '../base/row';
import {ArrayHelper} from '../structs/array_helper';

import {Comparator} from './comparator';
import {IndexHelper} from './index_helper';
import {IndexStats} from './index_stats';
import {Key, KeyRange, SingleKey, SingleKeyRange} from './key_range';
import {RuntimeIndex} from './runtime_index';

// Wrapper of the BTree.
export class BTree implements RuntimeIndex {
  public static EMPTY: number[] = [];

  // Creates tree from serialized leaves.
  public static deserialize(
      comparator: Comparator, name: string, uniqueKeyOnly: boolean,
      rows: Row[]): BTree {
    const tree = new BTree(name, comparator, uniqueKeyOnly);
    const newRoot = BTreeNode.deserialize(rows, tree);
    tree.root = newRoot;
    return tree;
  }

  private root: BTreeNode;
  private statsObj: IndexStats;

  constructor(
      private name: string, private comparatorObj: Comparator,
      private uniqueKeyOnly: boolean, data?: BTreeNodePayload[]) {
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
    return this.getRange([keyRange] as KeyRange).length;
  }

  public stats(): IndexStats {
    return this.statsObj;
  }

  public getRange(
      keyRanges?: SingleKeyRange[]|KeyRange[], reverseOrder?: boolean,
      rawLimit?: number, rawSkip?: number): number[] {
    const leftMostKey = this.root.getLeftMostNode().keys[0];
    if (leftMostKey === undefined || rawLimit === 0) {
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
        (keyRanges.length === 1 && keyRanges[0] instanceof SingleKeyRange &&
         (keyRanges[0] as SingleKeyRange).isAll())) {
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

    // For all cross-column indices, use filter to handle non-continuous blocks.
    const useFilter = this.comparator().keyDimensions() > 1;
    sortedKeyRanges.forEach((range) => {
      const keys = this.comparator().rangeToKeys(range);
      const key = this.comparator().isLeftOpen(range) ? leftMostKey : keys[0];
      let start = this.root.getContainingLeaf(key);
      // Need to have two strikes to stop.
      // Reason: say the nodes are [12, 15], [16, 18], when look for >15,
      //         first node will return empty, but we shall not stop there.
      let strikeCount = 0;
      while (start !== undefined && start !== null &&
             params.count < params.limit) {
        if (useFilter) {
          start.getRangeWithFilter(range, params, results);
        } else {
          start.getRange(range, params, results);
        }
        if (params.skip === 0 &&
            !start.isFirstKeyInRange(range as SingleKeyRange[])) {
          strikeCount++;
        } else {
          strikeCount = 0;
        }
        start = strikeCount === 2 ? null : (start as BTreeNode).next;
      }
    }, this);

    if (results.length > params.count) {
      // There are extra elements in results, truncate them.
      results.splice(params.count, results.length - params.count);
    }
    return (reverse) ? IndexHelper.slice(results, reverse, limit, skip) :
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

  public comparator(): Comparator {
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
      maxCount: number, reverse: boolean, limit: number,
      skip: number): number[] {
    const off = reverse ? this.stats().totalRows - maxCount - skip : skip;

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
  private checkNullKey(node: BTreeNode, index: number): [Key, any]|null {
    if (!this.comparator().comparable(node.keys[index])) {
      if (Array.isArray(node.keys[index])) {
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

  private findLeftMost(): [Key, any]|null {
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

  private findRightMost(): [Key, any]|null {
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

    return compareFn(leftMost[0], rightMost[0]) === Favor.LHS ? leftMost :
                                                                rightMost;
  }
}

interface BTreeNodePayload {
  key: Key;
  value: number;
}

interface BTreeNodeRangeParam {
  count: number;
  limit: number;
  reverse: boolean;
  skip: number;
}

interface BTreeNodeFillParam {
  offset: number;
  count: number;
  startIndex: number;
}

export class BTreeNode {
  public static create(tree: BTree): BTreeNode {
    // TODO(arthurhsu): Should distinguish internal nodes from leaf nodes to
    // avoid unnecessary row id wasting.
    return new BTreeNode(Row.getNextId(), tree);
  }

  public static serialize(start: BTreeNode): Row[] {
    const rows: Row[] = [];
    let node: BTreeNode|null = start;
    while (node) {
      const payload = [node.keys, node.values];
      rows.push(new Row(node.id, payload));
      node = node.next;
    }
    return rows;
  }

  // Returns new root node.
  public static deserialize(rows: Row[], tree: BTree): BTreeNode {
    const stats = tree.stats();
    const leaves = rows.map((row) => {
      const node = new BTreeNode(row.id(), tree);
      node.keys = row.payload()[0];
      node.values = row.payload()[1];
      node.keys.forEach((key, index) => {
        stats.add(
            key,
            tree.isUniqueKey() ? 1 : (node.values[index] as number[]).length);
      });
      return node;
    });
    for (let i = 0; i < leaves.length - 1; ++i) {
      BTreeNode.associate(leaves[i], leaves[i + 1]);
    }
    return (leaves.length > 1) ? BTreeNode.createInternals(leaves[0]) :
                                 leaves[0];
  }

  // Create B-Tree from sorted array of key-value pairs
  public static fromData(tree: BTree, data: BTreeNodePayload[]): BTreeNode {
    let max = BTreeNode.MAX_KEY_LEN;
    max = max * max * max;
    if (data.length >= max) {
      // Tree has more than three levels, need to use a bigger N!
      // 6: Too many rows: B-Tree implementation supports at most {0} rows.
      throw new Exception(ErrorCode.TOO_MANY_ROWS, max);
    }
    let node = BTreeNode.createLeaves(tree, data);
    node = BTreeNode.createInternals(node);
    return node;
  }

  // Maximum number of children a node can have (i.e. order of the B-Tree,
  // denoted as N in the following comments). This number must be greater or
  // equals to 4 for the implemented deletion algorithm to function correctly.
  private static MAX_COUNT = 512;
  private static MAX_KEY_LEN = BTreeNode.MAX_COUNT - 1;
  private static MIN_KEY_LEN = BTreeNode.MAX_COUNT >> 1;

  // Dump the contents of node of the same depth.
  // |node| is the left-most in the level.
  // Returns key and contents string in pair.
  private static dumpLevel(node: BTreeNode): string[] {
    let key = `${node.id}[${node.keys.join('|')}]`;
    const children = node.children.map((n) => n.id).join('|');
    const values = node.values.join('/');
    const getNodeId = (n: BTreeNode|null) => {
      return (n !== null && n !== undefined) ? n.id.toString() : '_';
    };

    let contents = getNodeId(node.prev) + '{';
    contents += (node.isLeaf()) ? values : children;
    contents = contents + '}' + getNodeId(node.parent);
    if (node.next) {
      const next = BTreeNode.dumpLevel(node.next);
      key = key + '  ' + next[0];
      contents = contents + '  ' + next[1];
    }
    return [key, contents];
  }

  private static associate(left: BTreeNode|null, right: BTreeNode|null): void {
    if (right) {
      right.prev = left;
    }
    if (left) {
      left.next = right;
    }
  }

  // Returns appropriate node length for direct construction.
  private static calcNodeLen(remaining: number): number {
    const maxLen = BTreeNode.MAX_KEY_LEN;
    const minLen = BTreeNode.MIN_KEY_LEN + 1;
    return (remaining >= maxLen + minLen) ?
        maxLen :
        ((remaining >= minLen && remaining <= maxLen) ? remaining : minLen);
  }

  // Create leaf nodes from given data.
  private static createLeaves(tree: BTree, data: BTreeNodePayload[]):
      BTreeNode {
    let remaining = data.length;
    let dataIndex = 0;

    let curNode = BTreeNode.create(tree);
    const node = curNode;
    while (remaining > 0) {
      const nodeLen = BTreeNode.calcNodeLen(remaining);
      const target = data.slice(dataIndex, dataIndex + nodeLen);
      curNode.keys = target.map((e) => e.key);
      curNode.values = target.map((e) => e.value);
      dataIndex += nodeLen;
      remaining -= nodeLen;
      if (remaining > 0) {
        const newNode = BTreeNode.create(curNode.tree);
        BTreeNode.associate(curNode, newNode);
        curNode = newNode;
      }
    }

    return node;
  }

  // Create parent node from children nodes.
  private static createParent(nodes: BTreeNode[]): BTreeNode {
    const node = nodes[0];
    const root = BTreeNode.create(node.tree);
    root.height = node.height + 1;
    root.children = nodes;
    nodes.forEach((n, i) => {
      n.parent = root;
      if (i > 0) {
        root.keys.push(n.keys[0]);
      }
    });
    return root;
  }

  // Create BTree from left-most leaf node.
  private static createInternals(node: BTreeNode): BTreeNode {
    let curNode: BTreeNode|null = node;
    const data = [];
    do {
      data.push(curNode);
      curNode = curNode.next;
    } while (curNode);

    let root;
    if (data.length <= BTreeNode.MAX_KEY_LEN + 1) {
      // Create a root node and return.
      root = BTreeNode.createParent(data);
    } else {
      let remaining = data.length;
      let dataIndex = 0;

      root = BTreeNode.create(node.tree);
      root.height = node.height + 2;
      while (remaining > 0) {
        const nodeLen = BTreeNode.calcNodeLen(remaining);
        const target = data.slice(dataIndex, dataIndex + nodeLen);
        const newNode = BTreeNode.createParent(target);
        newNode.parent = root;
        if (root.children.length) {
          root.keys.push(target[0].keys[0]);
          BTreeNode.associate(root.children[root.children.length - 1], newNode);
        }
        root.children.push(newNode);
        dataIndex += nodeLen;
        remaining -= nodeLen;
      }
    }
    return root;
  }

  // Returns left most key of the subtree.
  private static leftMostKey(node: BTreeNode): Key {
    return node.isLeaf() ? node.keys[0] :
                           BTreeNode.leftMostKey(node.children[0]);
  }

  public prev: BTreeNode|null;
  public next: BTreeNode|null;
  public keys: Key[];
  public values: number[]|number[][];
  public getContainingLeaf: (key: Key|SingleKey) => BTreeNode | null;

  private height: number;
  private parent: BTreeNode|null;
  private children: BTreeNode[];

  constructor(private id: number, private tree: BTree) {
    this.height = 0;
    this.parent = null;
    this.prev = null;
    this.next = null;
    this.keys = [];
    this.values = [];
    this.children = [];
    this.getContainingLeaf = tree.comparator().keyDimensions() === 1 ?
        this.getContainingLeafSingleKey :
        this.getContainingLeafMultiKey;
  }

  // Dump the tree as string. For example, if the tree is
  //
  //                     15
  //          /                      \
  //        9|13                   27|31
  //  /      |       \        /      |      \
  // 1|3  9|10|11  13|14    15|16  27|29  31|38|45
  //
  // and the values of the tree are identical to the keys, then the output will
  // be
  //
  // 11[15]
  // {2|12}
  // 2[9|13]  12[27|31]
  // {0|15|1}11  2{17|5|7}11
  // 0[1|3]  15[9|10|11]  1[13|14]  17[15|16]  5[27|29]  7[31|38|45]
  // {1/3}2  0{9/10/11}2  15{13/14}2  1{15/16}12  17{27/29}12  5{31/38/45}12
  //
  // Each tree level contains two lines, the first line is the key line
  // containing keys of each node in the format of
  // <node_id>[<key0>|<key1>|...|<keyN-1>]. The second line is the value line
  // containing values of each node in the format of
  // <left_node_id>[<value0>|<value1>|...|<valueN>]<parent_node_id>. The root
  // node does not have parent so its parent node id is denoted as underscore.
  //
  // Nodes in each level is a doubly-linked list therefore BFS traversal from
  // left-most to right-most is used. As a result, if the right link is
  // broken, the result will be partial.
  public toString(): string {
    let result = '';
    const level = BTreeNode.dumpLevel(this);
    result += level[0] + '\n' + level[1] + '\n';
    if (this.children.length) {
      result += this.children[0].toString();
    }
    return result;
  }

  public getLeftMostNode(): BTreeNode {
    return this.isLeaf() ? this : this.children[0].getLeftMostNode();
  }

  public getRightMostNode(): BTreeNode {
    return this.isLeaf() ?
        this :
        this.children[this.children.length - 1].getRightMostNode();
  }

  public get(key: Key|SingleKey): number[] {
    let pos = this.searchKey(key);
    if (this.isLeaf()) {
      let results = BTree.EMPTY;
      if (this.tree.eq(this.keys[pos], key)) {
        // Use concat here because this.values[pos] can be number or array.
        results = results.concat(this.values[pos]);
      }
      return results;
    } else {
      pos = (this.tree.eq(this.keys[pos], key)) ? pos + 1 : pos;
      return this.children[pos].get(key);
    }
  }

  public containsKey(key: Key|SingleKey): boolean {
    const pos = this.searchKey(key);
    if (this.tree.eq(this.keys[pos], key)) {
      return true;
    }

    return this.isLeaf() ? false : this.children[pos].containsKey(key);
  }

  // Deletes a node and returns (new) root node after deletion.
  public remove(key: Key|SingleKey, value?: number): BTreeNode {
    this.delete(key, -1, value);

    if (this.isRoot()) {
      let root: BTreeNode|null = this;
      if (this.children.length === 1) {
        root = this.children[0];
        root.parent = null;
      }
      return root;
    }

    return this;
  }

  // Insert node into this subtree. Returns new root if any.
  // |replace| means to replace the value if key existed.
  public insert(key: Key|SingleKey, value: number, replace = false): BTreeNode {
    let pos = this.searchKey(key);
    if (this.isLeaf()) {
      if (this.tree.eq(this.keys[pos], key)) {
        if (replace) {
          this.tree.stats().remove(
              key,
              this.tree.isUniqueKey() ? 1 :
                                        (this.values[pos] as number[]).length);
          this.values[pos] = this.tree.isUniqueKey() ? value : [value];
        } else if (this.tree.isUniqueKey()) {
          // 201: Duplicate keys are not allowed.
          throw new Exception(
              ErrorCode.DUPLICATE_KEYS, this.tree.getName(),
              JSON.stringify(key));
        } else {  // Non-unique key that already existed.
          if (!ArrayHelper.binaryInsert(this.values[pos] as number[], value)) {
            // 109: Attempt to insert a row number that already existed.
            throw new Exception(ErrorCode.ROW_ID_EXISTED);
          }
        }
        this.tree.stats().add(key, 1);
        return this;
      }
      this.keys.splice(pos, 0, key);
      (this.values as any[])
          .splice(pos, 0, (this.tree.isUniqueKey() ? value : [value]) as any);
      this.tree.stats().add(key, 1);
      return (this.keys.length === BTreeNode.MAX_COUNT) ? this.splitLeaf() :
                                                          this;
    } else {
      pos = this.tree.eq(this.keys[pos], key) ? pos + 1 : pos;
      const node = this.children[pos].insert(key, value, replace);
      if (!node.isLeaf() && node.keys.length === 1) {
        // Merge the internal to se
        this.keys.splice(pos, 0, node.keys[0]);
        node.children[1].parent = this;
        node.children[0].parent = this;
        this.children.splice(pos, 1, node.children[1]);
        this.children.splice(pos, 0, node.children[0]);
      }
      return (this.keys.length === BTreeNode.MAX_COUNT) ? this.splitInternal() :
                                                          this;
    }
  }

  // The API signature of this function is specially crafted for performance
  // optimization. Perf results showed that creation of empty array erodes the
  // benefit of indexing significantly (in some cases >50%). As a result, it
  // is required to pass in the results array.
  public getRange(
      keyRange: KeyRange|SingleKeyRange, params: BTreeNodeRangeParam,
      results: number[]): void {
    const c = this.tree.comparator();
    let left = 0;
    let right = this.keys.length - 1;

    // Position of range relative to the key.
    const compare = (coverage: boolean[]) => {
      return coverage[0] ? (coverage[1] ? Favor.TIE : Favor.LHS) : Favor.RHS;
    };

    const keys =
        this.keys;  // Used to avoid binding this for recursive functions.
    const favorLeft = compare(c.compareRange(keys[left], keyRange));
    const favorRight = compare(c.compareRange(keys[right], keyRange));

    // Range is on the left of left most key or right of right most key.
    if (favorLeft === Favor.LHS ||
        (favorLeft === Favor.RHS && favorRight === Favor.RHS)) {
      return;
    }

    const getMidPoint = (l: number, r: number): number => {
      const mid = (l + r) >> 1;
      return mid === l ? mid + 1 : mid;
    };

    // Find the first key that is in range. Returns index of the key, -1 if
    // not found. |favorR| is Favor of right.
    const findFirstKey = (l: number, r: number, favorR: Favor): number => {
      if (l >= r) {
        return favorR === Favor.TIE ? r : -1;
      }
      const favorL = compare(c.compareRange(keys[l], keyRange));
      if (favorL === Favor.TIE) {
        return l;
      } else if (favorL === Favor.LHS) {
        return -1;  // Shall not be here.
      }

      const mid = getMidPoint(l, r);
      if (mid === r) {
        return favorR === Favor.TIE ? r : -1;
      }
      const favorM = compare(c.compareRange(keys[mid], keyRange));
      if (favorM === Favor.TIE) {
        return findFirstKey(l, mid, favorM);
      } else if (favorM === Favor.RHS) {
        return findFirstKey(mid + 1, r, favorR);
      } else {
        return findFirstKey(l + 1, mid, favorM);
      }
    };

    // Find the last key that is in range. Returns index of the key, -1 if
    // not found.
    const findLastKey = (l: number, r: number): number => {
      if (l >= r) {
        return l;
      }
      const favorR = compare(c.compareRange(keys[r], keyRange));
      if (favorR === Favor.TIE) {
        return r;
      } else if (favorR === Favor.RHS) {
        return l;
      }

      const mid = getMidPoint(l, r);
      if (mid === r) {
        return l;
      }
      const favorM = compare(c.compareRange(keys[mid], keyRange));
      if (favorM === Favor.TIE) {
        return findLastKey(mid, r);
      } else if (favorM === Favor.LHS) {
        return findLastKey(l, mid - 1);
      } else {
        return -1;  // Shall not be here.
      }
    };

    if (favorLeft !== Favor.TIE) {
      left = findFirstKey(left + 1, right, favorRight);
    }
    if (left !== -1) {
      right = findLastKey(left, right);
      if (right !== -1 && right >= left) {
        this.appendResults(params, results, left, right + 1);
      }
    }
  }

  // Loops through all keys and check if key is in the given range. If so push
  // the values into results. This method is slower than the getRange() by
  // design and should be used only in the case of cross-column nullable
  // indices.
  // TODO(arthurhsu): remove this method when GridFile is implemented.
  //
  // |results| can be an empty array, or an array holding any results from
  // previous calls to getRangeWithFilter().
  public getRangeWithFilter(
      keyRange: KeyRange|SingleKeyRange, params: BTreeNodeRangeParam,
      results: number[]): void {
    const c = this.tree.comparator();
    let start = -1;

    // Find initial pos
    for (let i = 0; i < this.keys.length; ++i) {
      if (c.isInRange(this.keys[i], keyRange)) {
        start = i;
        break;
      }
    }

    if (start === -1) {
      return;
    }

    for (let i = start; i < this.keys.length && params.count < params.limit;
         ++i) {
      if (!c.isInRange(this.keys[i], keyRange)) {
        continue;
      }
      this.appendResultsAt(params, results, i);
    }
  }

  // Special optimization for appending results. For performance reasons, the
  // parameters of this function are passed by reference.
  // |params| offset means number of rows to skip, count means remaining number
  // of rows to fill, and startIndex is the start index of results for filling.
  public fill(params: BTreeNodeFillParam, results: number[]): void {
    if (this.isLeaf()) {
      for (let i = 0; i < this.values.length && params.count > 0; ++i) {
        const val: number[] = this.values[i] as number[];
        if (params.offset > 0) {
          params.offset -= (!this.tree.isUniqueKey() ? val.length : 1);
          if (params.offset < 0) {
            for (let j = val.length + params.offset;
                 j < val.length && params.count > 0; ++j) {
              results[params.startIndex++] = val[j];
              params.count--;
            }
          }
          continue;
        }
        if (this.tree.isUniqueKey()) {
          results[params.startIndex++] = this.values[i] as number;
          params.count--;
        } else {
          for (let j = 0; j < val.length && params.count > 0; ++j) {
            results[params.startIndex++] = this.values[i][j];
            params.count--;
          }
        }
      }
    } else {
      for (let i = 0; i < this.children.length && params.count > 0; ++i) {
        this.children[i].fill(params, results);
      }
    }
  }

  public isFirstKeyInRange(range: KeyRange): boolean {
    return this.tree.comparator().isFirstKeyInRange(this.keys[0], range);
  }

  private isLeaf(): boolean {
    return this.height === 0;
  }

  private isRoot(): boolean {
    return this.parent === null;
  }

  // Reconstructs internal node keys.
  private fix(): void {
    this.keys = [];
    for (let i = 1; i < this.children.length; ++i) {
      this.keys.push(BTreeNode.leftMostKey(this.children[i]));
    }
  }

  // Deletes a key from a given node. If the key length is smaller than
  // required, execute the following operations according to order:
  // 1. Steal a key from right sibling, if there is one with key > N/2
  // 2. Steal a key from left sibling, if there is one with key > N/2
  // 3. Merge to right sibling, if any
  // 4. Merge to left sibling, if any
  //
  // When stealing and merging happens on internal nodes, the key array of that
  // node will be obsolete and need to be reconstructed by fix().
  //
  // @param {!index.Index.Key} key
  // @param {number} parentPos Position of this node in parent's children.
  // @param {number=} value Match the value to delete.
  // @return {boolean} Whether a fix is needed or not.
  // @private
  private delete(key: Key|SingleKey, parentPos: number, value?: number):
      boolean {
    const pos = this.searchKey(key);
    const isLeaf = this.isLeaf();
    if (!isLeaf) {
      const index = this.tree.eq(this.keys[pos], key) ? pos + 1 : pos;
      if (this.children[index].delete(key, index, value)) {
        this.fix();
      } else {
        return false;
      }
    } else if (!this.tree.eq(this.keys[pos], key)) {
      return false;
    }

    if (this.keys.length > pos && this.tree.eq(this.keys[pos], key)) {
      if (value !== undefined && !this.tree.isUniqueKey() && isLeaf) {
        if (ArrayHelper.binaryRemove((this.values[pos] as number[]), value)) {
          this.tree.stats().remove(key, 1);
        }
        const len = (this.values[pos] as number[]).length;
        if (len) {
          return false;  // No need to fix.
        }
      }

      this.keys.splice(pos, 1);
      if (isLeaf) {
        const removedLength =
            this.tree.isUniqueKey() ? 1 : (this.values[pos] as number[]).length;
        this.values.splice(pos, 1);
        this.tree.stats().remove(key, removedLength);
      }
    }

    if (this.keys.length < BTreeNode.MIN_KEY_LEN && !this.isRoot()) {
      if (!this.steal()) {
        this.merge(parentPos);
      }
      return true;
    }

    return true;
  }

  // Steals key from adjacent nodes.
  private steal(): boolean {
    let from: BTreeNode|null = null;
    let fromIndex: number;
    let fromChildIndex: number;
    let toIndex: number;
    if (this.next && this.next.keys.length > BTreeNode.MIN_KEY_LEN) {
      from = this.next;
      fromIndex = 0;
      fromChildIndex = 0;
      toIndex = this.keys.length + 1;
    } else if (this.prev && this.prev.keys.length > BTreeNode.MIN_KEY_LEN) {
      from = this.prev;
      fromIndex = this.prev.keys.length - 1;
      fromChildIndex = this.isLeaf() ? fromIndex : fromIndex + 1;
      toIndex = 0;
    } else {
      return false;
    }

    this.keys.splice(toIndex, 0, from.keys[fromIndex]);
    from.keys.splice(fromIndex, 1);
    const child: any[] = this.isLeaf() ? this.values : this.children;
    let fromChild = null;
    if (this.isLeaf()) {
      fromChild = from.values;
    } else {
      fromChild = from.children;
      fromChild[fromChildIndex].parent = this;
    }
    child.splice(toIndex, 0, fromChild[fromChildIndex]);
    fromChild.splice(fromChildIndex, 1);
    if (!from.isLeaf()) {
      from.fix();
      this.fix();
    }

    return true;
  }

  // Merges with adjacent nodes.
  // |parentPos| indicates this node's position in parent's children.
  private merge(parentPos: number): void {
    let mergeTo: BTreeNode;
    let keyOffset: number;
    let childOffset: number;
    if (this.next && this.next.keys.length < BTreeNode.MAX_KEY_LEN) {
      mergeTo = this.next;
      keyOffset = 0;
      childOffset = 0;
    } else if (this.prev) {
      mergeTo = this.prev;
      keyOffset = mergeTo.keys.length;
      childOffset =
          mergeTo.isLeaf() ? mergeTo.values.length : mergeTo.children.length;
    } else {
      throw new Exception(ErrorCode.ASSERTION);
    }

    let args: any[] = [keyOffset, 0].concat(this.keys as any[]);
    Array.prototype.splice.apply(mergeTo.keys, args);
    let myChildren = null;
    if (this.isLeaf()) {
      myChildren = this.values;
    } else {
      myChildren = this.children;
      myChildren.forEach((node) => node.parent = mergeTo);
    }
    args = [childOffset, 0].concat(myChildren as any[]);
    Array.prototype.splice.apply(
        mergeTo.isLeaf() ? mergeTo.values : mergeTo.children, args);
    BTreeNode.associate(this.prev, this.next);
    if (!mergeTo.isLeaf()) {
      mergeTo.fix();
    }
    if (parentPos !== -1) {
      (this.parent as BTreeNode).keys.splice(parentPos, 1);
      (this.parent as BTreeNode).children.splice(parentPos, 1);
    }
  }

  // Split leaf node into two nodes, returns the split internal node.
  private splitLeaf(): BTreeNode {
    const half = BTreeNode.MIN_KEY_LEN;

    const right = BTreeNode.create(this.tree);
    const root = BTreeNode.create(this.tree);

    root.height = 1;
    root.keys = [this.keys[half]];
    root.children = [this, right];
    root.parent = this.parent;

    this.parent = root;
    right.keys = this.keys.splice(half);
    right.values = this.values.splice(half);
    right.parent = root;
    BTreeNode.associate(right, this.next);
    BTreeNode.associate(this, right);
    return root;
  }

  // Split internal node into two nodes, returns the split internal node.
  private splitInternal(): BTreeNode {
    const half = BTreeNode.MIN_KEY_LEN;
    const root = BTreeNode.create(this.tree);
    const right = BTreeNode.create(this.tree);

    root.parent = this.parent;
    root.height = this.height + 1;
    root.keys = [this.keys[half]];
    root.children = [this, right];

    this.keys.splice(half, 1);
    right.parent = root;
    right.height = this.height;
    right.keys = this.keys.splice(half);
    right.children = this.children.splice(half + 1);
    right.children.forEach((node) => node.parent = right);

    this.parent = root;
    BTreeNode.associate(right, this.next);
    BTreeNode.associate(this, right);
    return root;
  }

  // Returns the position where the key is the closest smaller or equals to.
  private searchKey(key: Key|SingleKey): number {
    // Binary search.
    let left = 0;
    let right = this.keys.length;
    const c = this.tree.comparator();
    while (left < right) {
      const middle = (left + right) >> 1;
      if (c.compare(this.keys[middle], key) === Favor.RHS) {
        left = middle + 1;
      } else {
        right = middle;
      }
    }

    return left;
  }

  private getContainingLeafSingleKey(key: Key): BTreeNode|null {
    if (!this.isLeaf()) {
      let pos = this.searchKey(key);
      if (this.tree.eq(this.keys[pos], key)) {
        pos++;
      }
      return this.children[pos].getContainingLeaf(key);
    }

    return this;
  }

  private getContainingLeafMultiKey(key: SingleKey[]): BTreeNode|null {
    if (!this.isLeaf()) {
      let pos = this.searchKey(key);
      if (this.tree.eq(this.keys[pos], key)) {
        // Note the multi-key comparator will return TIE if compared with an
        // unbounded key. As a result, we need to check if any dimension of the
        // key contains unbound.
        const hasUnbound =
            key.some((dimension) => dimension === SingleKeyRange.UNBOUND_VALUE);
        if (!hasUnbound) {
          pos++;
        }
      }
      return this.children[pos].getContainingLeafMultiKey(key);
    }

    return this;
  }

  // Appends newly found results to an existing bag of results. For performance
  // reasons, parameters are passed by reference.
  // |params| count is number of filled elements in the results array; limit
  // means max number to fill in the results; reverse means the request is
  // for reverse ordering; skip means remaining skip count.
  private appendResultsAt(
      params: BTreeNodeRangeParam, results: number[], i: number): void {
    if (this.tree.isUniqueKey()) {
      if (!params.reverse && params.skip) {
        params.skip--;
        return;
      }
      results[params.count++] = this.values[i] as number;
    } else {
      for (let j = 0; j < (this.values[i] as number[]).length &&
           params.count < results.length;
           ++j) {
        if (!params.reverse && params.skip) {
          params.skip--;
          continue;
        }
        results[params.count++] = this.values[i][j];
      }
    }
  }

  // Appends newly found results to an existing bag of results. For performance
  // reasons, parameters are passed by reference.
  // |params| count is number of filled elements in the results array; limit
  // means max number to fill in the results; reverse means the request is
  // for reverse ordering; skip means remaining skip count.
  private appendResults(
      params: BTreeNodeRangeParam, results: number[], from: number,
      to: number): void {
    for (let i = from; i < to; ++i) {
      if (!params.reverse && params.count >= params.limit) {
        return;
      }
      this.appendResultsAt(params, results, i);
    }
  }
}

// tslint:enable:max-classes-per-file
