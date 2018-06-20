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

import * as chai from 'chai';
import {TreeNode} from '../../lib/structs/tree_node';

const assert = chai.assert;

describe('TreeNode', () => {
  it('ctor', () => {
    const node = new TreeNode();
    assert.isNull(node.parent);
    assert.deepEqual([], node.getChildren());
    assert.isTrue(node.isLeaf());
  });

  it('parent', () => {
    const node1 = new TreeNode();
    const node2 = new TreeNode();
    node1.addChild(node2);
    assert.equal(node1, node2.parent);
    assert.isNull(node1.parent);
  });

  it('isLeaf', () => {
    const node1 = new TreeNode();
    const node2 = new TreeNode();
    node1.addChild(node2);
    assert.isFalse(node1.isLeaf());
    assert.isTrue(node2.isLeaf());
  });

  it('getChildren', () => {
    const node1 = new TreeNode();
    const node2 = new TreeNode();
    node1.addChild(node2);
    assert.deepEqual([node2], node1.getChildren());
    assert.deepEqual([], node2.getChildren());
  });

  it('getChildAt', () => {
    const node1 = new TreeNode();
    const node2 = new TreeNode();
    node1.addChild(node2);
    assert.isNull(node1.getChildAt(-1));
    assert.equal(node2, node1.getChildAt(0));
    assert.isNull(node1.getChildAt(1));
    assert.isNull(node2.getChildAt(0));
  });

  it('getChildCount', () => {
    const node1 = new TreeNode();
    const node2 = new TreeNode();
    node1.addChild(node2);
    assert.equal(1, node1.getChildCount());
    assert.equal(0, node2.getChildCount());
  });

  it('depth', () => {
    const node1 = new TreeNode();
    const node2 = new TreeNode();
    const node3 = new TreeNode();
    node1.addChild(node2);
    node2.addChild(node3);
    assert.equal(0, node1.getDepth());
    assert.equal(1, node2.getDepth());
    assert.equal(2, node3.getDepth());
  });

  it('root', () => {
    const node1 = new TreeNode();
    const node2 = new TreeNode();
    node1.addChild(node2);
    assert.equal(node1, node1.getRoot());
    assert.equal(node1, node2.getRoot());
  });

  it('traverse', () => {
    const node1 = new TreeNode();
    const node2 = new TreeNode();
    const node3 = new TreeNode();
    const node4 = new TreeNode();
    node1.addChild(node2);
    node2.addChild(node3);
    node2.addChild(node4);

    let visitedNodes: TreeNode[] = [];
    node1.traverse((node) => {
      visitedNodes.push(node);
    });
    assert.deepEqual([node1, node2, node3, node4], visitedNodes);

    visitedNodes = [];
    node1.traverse((node) => {
      visitedNodes.push(node);
      return node !== node2;  // Cut off at node2.
    });
    assert.deepEqual([node1, node2], visitedNodes);
  });

  it('addChild', () => {
    const node1 = new TreeNode();
    const node2 = new TreeNode();
    const node3 = new TreeNode();
    assert.deepEqual([], node1.getChildren());
    node1.addChild(node2);
    assert.deepEqual([node2], node1.getChildren());
    assert.equal(node1, node2.parent);
    node1.addChild(node3);
    assert.deepEqual([node2, node3], node1.getChildren());
  });

  it('addChildAt', () => {
    const node1 = new TreeNode();
    const node2 = new TreeNode();
    const node3 = new TreeNode();
    const node4 = new TreeNode();
    const node5 = new TreeNode();
    node1.addChildAt(node2, 0);
    assert.deepEqual([node2], node1.getChildren());
    assert.equal(node1, node2.parent);
    node1.addChildAt(node3, 0);
    assert.deepEqual([node3, node2], node1.getChildren());
    node1.addChildAt(node4, 1);
    assert.deepEqual([node3, node4, node2], node1.getChildren());
    node1.addChildAt(node5, 3);
    assert.deepEqual([node3, node4, node2, node5], node1.getChildren());
  });

  it('replaceChildAt', () => {
    const root = new TreeNode();
    const node1 = new TreeNode();
    root.addChild(node1);

    const node2 = new TreeNode();
    assert.equal(node1, root.replaceChildAt(node2, 0));
    assert.equal(root, node2.parent);
    assert.deepEqual([node2], root.getChildren());
    assert.isNull(node1.parent);
  });

  it('removeChildAt', () => {
    const node1 = new TreeNode();
    const node2 = new TreeNode();
    const node3 = new TreeNode();
    node1.addChild(node2);
    node1.addChild(node3);

    assert.isNull(node1.removeChildAt(-1));
    assert.isNull(node1.removeChildAt(2));
    assert.deepEqual([node2, node3], node1.getChildren());

    assert.equal(node2, node1.removeChildAt(0));
    assert.deepEqual([node3], node1.getChildren());
    assert.isNull(node2.parent);

    assert.equal(node3, node1.removeChildAt(0));
    assert.deepEqual([], node1.getChildren());
    assert.isTrue(node1.isLeaf());
  });

  it('removeChild', () => {
    const node1 = new TreeNode();
    const node2 = new TreeNode();
    const node3 = new TreeNode();
    node1.addChild(node2);
    node1.addChild(node3);

    assert.isNull(node1.removeChild(node1));
    assert.deepEqual([node2, node3], node1.getChildren());

    assert.equal(node3, node1.removeChild(node3));
    assert.deepEqual([node2], node1.getChildren());
  });
});
