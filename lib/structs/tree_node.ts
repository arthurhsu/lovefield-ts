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

  public parent: TreeNode|null;
  private children: TreeNode[]|null;

  constructor() {
    this.parent = null;
    this.children = null;
  }

  public root(): TreeNode {
    let root: TreeNode = this;
    while (root.parent !== null) {
      root = root.parent;
    }
    return root;
  }

  public depth(): number {
    let depth = 0;
    let node: TreeNode = this;
    while (node.parent !== null) {
      depth++;
      node = node.parent;
    }
    return depth;
  }

  public isLeaf(): boolean {
    return this.children === null;
  }

  public getChildren(): TreeNode[] {
    return this.children || TreeNode.EMPTY_ARRAY;
  }

  public getChildAt(index: number): TreeNode|null {
    return (this.children && index >= 0 && index < this.children.length) ?
        this.getChildren()[index] :
        null;
  }

  public getChildCount(): number {
    return this.getChildren().length;
  }

  public addChildAt(child: TreeNode, index: number): void {
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

  public addChild(child: TreeNode): void {
    assert(child.parent === null);
    child.parent = this;
    if (this.children === null) {
      this.children = [child];
    } else {
      this.children.push(child);
    }
  }

  // Returns removed node at index, if any.
  public removeChildAt(index: number): TreeNode|null {
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
  public removeChild(child: TreeNode): TreeNode|null {
    return this.children ?
        this.removeChildAt(this.children.indexOf(child)) : null;
  }

  // Returns original node, if any.
  public replaceChildAt(newChild: TreeNode, index: number): TreeNode|null {
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
  public traverse(f: (node: TreeNode) => boolean): void {
    if (f(this)) {
      this.getChildren().forEach((child) => child.traverse(f));
    }
  }
}
