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

import {assert} from '../base/assert';

export class TreeNode {
  private static EMPTY_ARRAY: TreeNode[] = [];

  parent: TreeNode | null;
  private children: TreeNode[] | null;

  constructor() {
    this.parent = null;
    this.children = null;
  }

  getParent(): TreeNode {
    return this.parent as TreeNode;
  }

  setParent(parentNode: TreeNode): void {
    this.parent = parentNode;
  }

  getRoot(): TreeNode {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    let root: TreeNode = this;
    while (root.parent !== null) {
      root = root.parent;
    }
    return root;
  }

  getDepth(): number {
    let depth = 0;
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    let node: TreeNode = this;
    while (node.parent !== null) {
      depth++;
      node = node.parent;
    }
    return depth;
  }

  isLeaf(): boolean {
    return this.children === null;
  }

  getChildren(): TreeNode[] {
    return this.children || TreeNode.EMPTY_ARRAY;
  }

  getChildAt(index: number): TreeNode | null {
    return this.children && index >= 0 && index < this.children.length
      ? this.getChildren()[index]
      : null;
  }

  getChildCount(): number {
    return this.getChildren().length;
  }

  addChildAt(child: TreeNode, index: number): void {
    assert(child.parent === null);
    child.parent = this;
    if (this.children === null) {
      // assert(index == 0);
      this.children = [child];
    } else {
      assert(index >= 0 && index <= this.children.length);
      this.children.splice(index, 0, child);
    }
  }

  addChild(child: TreeNode): void {
    assert(child.parent === null);
    child.parent = this;
    if (this.children === null) {
      this.children = [child];
    } else {
      this.children.push(child);
    }
  }

  // Returns removed node at index, if any.
  removeChildAt(index: number): TreeNode | null {
    if (this.children) {
      const child = this.children[index];
      if (child) {
        child.parent = null;
        this.children.splice(index, 1);
        if (this.children.length === 0) {
          this.children = null;
        }
        return child;
      }
    }
    return null;
  }

  // Returns removed node, if any.
  removeChild(child: TreeNode): TreeNode | null {
    return this.children
      ? this.removeChildAt(this.children.indexOf(child))
      : null;
  }

  // Returns original node, if any.
  replaceChildAt(newChild: TreeNode, index: number): TreeNode | null {
    assert(newChild.parent === null);
    if (this.children) {
      const oldChild = this.getChildAt(index);
      if (oldChild) {
        oldChild.parent = null;
        newChild.parent = this;
        this.children[index] = newChild;
        return oldChild;
      }
    }
    return null;
  }

  // Traverses the subtree with the possibility to skip branches. Starts with
  // this node, and visits the descendant nodes depth-first, in preorder.
  traverse(f: (node: TreeNode) => boolean | void): void {
    if (f(this) !== false) {
      this.getChildren().forEach(child => child.traverse(f));
    }
  }
}
