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

import * as chai from 'chai';
import { TreeHelper } from '../../lib/structs/tree_helper';
import { TreeNode } from '../../lib/structs/tree_node';

const assert = chai.assert;

class TreeNode2 extends TreeNode {
  constructor(readonly id: number) {
    super();
  }
}

// Creates a tree to be used in various tests.
// Returns an array holding all the nodes in the tree in pre-order traversal
// order.
function createTestTree1(): TreeNode2[] {
  const nodes: TreeNode2[] = new Array(11);
  for (let i = 0; i < nodes.length; i++) {
    nodes[i] = new TreeNode2(i);
  }

  // Creating a tree that has the following structure.
  //            n0
  //          / | \
  //         /  |  \
  //        /   |  n10
  //       /    |
  //      n1    n5
  //    / | \   |
  //  n2 n3 n4  n6
  //            |
  //            n7
  //           /  \
  //         n8   n9

  nodes[1].addChild(nodes[2]);
  nodes[1].addChild(nodes[3]);
  nodes[1].addChild(nodes[4]);

  nodes[5].addChild(nodes[6]);
  nodes[6].addChild(nodes[7]);

  nodes[7].addChild(nodes[8]);
  nodes[7].addChild(nodes[9]);

  nodes[0].addChild(nodes[1]);
  nodes[0].addChild(nodes[5]);
  nodes[0].addChild(nodes[10]);

  return nodes;
}

// Creates a different tree to be used in various tests.
// Returns an array holding all the nodes in the tree in pre-order traversal
// order.
function createTestTree2(): TreeNode2[] {
  const nodes: TreeNode2[] = new Array(7);
  for (let i = 0; i < nodes.length; i++) {
    nodes[i] = new TreeNode2(i);
  }

  // Creating a tree that has the following structure.
  //           n0
  //           |
  //           n1
  //          /  \
  //        n2    n6
  //       /  \
  //      n3  n4
  //            \
  //            n5

  nodes[0].addChild(nodes[1]);

  nodes[1].addChild(nodes[2]);
  nodes[1].addChild(nodes[6]);

  nodes[2].addChild(nodes[3]);
  nodes[2].addChild(nodes[4]);

  nodes[4].addChild(nodes[5]);

  return nodes;
}

describe('TreeHelper', () => {
  function stringFn(node: TreeNode): string {
    return `[${(node as TreeNode2).id}]\n`;
  }

  // Checks that the given tree is producing a tree with an identical structure
  // when cloned.
  function checkMap(rootNode: TreeNode2): void {
    // Attempting to copy the tree.
    const copy = TreeHelper.map(rootNode, (node: TreeNode2) => {
      return new TreeNode2(node.id);
    });

    assert.equal(
      TreeHelper.toString(rootNode, stringFn),
      TreeHelper.toString(copy, stringFn)
    );
  }

  // Tests that TreeHelper.map() is constructing a new tree with the exact same
  // structure as the original tree, for a simple tree.
  it('Map1', () => {
    const nodes: TreeNode2[] = new Array(6);
    for (let i = 0; i < nodes.length; i++) {
      nodes[i] = new TreeNode2(i);
    }

    nodes[2].addChild(nodes[3]);
    nodes[1].addChild(nodes[2]);
    nodes[1].addChild(nodes[4]);
    nodes[0].addChild(nodes[1]);

    const rootNode = nodes[0];
    checkMap(rootNode);
  });

  // Tests that TreeHelper.map() is constructing a new tree with the exact same
  // structure as the original tree, for two more complex trees.
  it('Map2', () => {
    checkMap(createTestTree1()[0]);
    checkMap(createTestTree2()[0]);
  });

  // Testing case where a node that has both parent and children nodes is
  // removed. Ensure that the children of the removed node are re-parented to
  // its parent.
  it('RemoveNode_Intermediate', () => {
    const nodes = createTestTree1();

    const treeAfter =
      '[0]\n' +
      '-[2]\n' +
      '-[3]\n' +
      '-[4]\n' +
      '-[5]\n' +
      '--[6]\n' +
      '---[7]\n' +
      '----[8]\n' +
      '----[9]\n' +
      '-[10]\n';

    // Removing node n1.
    const removeResult = TreeHelper.removeNode(nodes[1]);
    assert.equal(nodes[0], removeResult.parent);
    assert.sameDeepOrderedMembers(
      [nodes[2], nodes[3], nodes[4]],
      removeResult.children
    );
    assert.equal(treeAfter, TreeHelper.toString(nodes[0], stringFn));
  });

  // Testing case where a leaf node is removed.
  it('RemoveNode_Leaf', () => {
    const nodes = createTestTree1();

    const treeAfter =
      '[0]\n' +
      '-[1]\n' +
      '--[3]\n' +
      '--[4]\n' +
      '-[5]\n' +
      '--[6]\n' +
      '---[7]\n' +
      '----[8]\n' +
      '----[9]\n' +
      '-[10]\n';

    // Removing node n2.
    const removeResult = TreeHelper.removeNode(nodes[2]);
    assert.equal(nodes[1], removeResult.parent);
    assert.sameDeepOrderedMembers([], removeResult.children);
    assert.equal(treeAfter, TreeHelper.toString(nodes[0], stringFn));
  });

  // Testing case where the root node is removed.
  it('RemoveNode_Root', () => {
    const nodes = createTestTree1();

    const subTree1After = '[1]\n' + '-[2]\n' + '-[3]\n' + '-[4]\n';

    const subTree2After =
      '[5]\n' + '-[6]\n' + '--[7]\n' + '---[8]\n' + '---[9]\n';

    const subTree3After = '[10]\n';

    // Removing node n0.
    const removeResult = TreeHelper.removeNode(nodes[0]);
    assert.isNull(removeResult.parent);
    assert.sameDeepOrderedMembers(
      [nodes[1], nodes[5], nodes[10]],
      removeResult.children
    );
    assert.equal(
      subTree1After,
      TreeHelper.toString(removeResult.children[0], stringFn)
    );
    assert.equal(
      subTree2After,
      TreeHelper.toString(removeResult.children[1], stringFn)
    );
    assert.equal(
      subTree3After,
      TreeHelper.toString(removeResult.children[2], stringFn)
    );
  });

  it('InsertNodeAt', () => {
    const nodes = createTestTree1();

    const treeAfter =
      '[0]\n' +
      '-[1]\n' +
      '--[2]\n' +
      '--[3]\n' +
      '--[4]\n' +
      '-[5]\n' +
      '--[6]\n' +
      '---[7]\n' +
      '----[11]\n' +
      '-----[8]\n' +
      '-----[9]\n' +
      '-[10]\n';

    const newNode = new TreeNode2(11);
    TreeHelper.insertNodeAt(nodes[7], newNode);
    assert.equal(treeAfter, TreeHelper.toString(nodes[0], stringFn));
  });

  it('ReplaceChainWithChain', () => {
    const nodes = createTestTree1();

    const treeAfter =
      '[0]\n' +
      '-[1]\n' +
      '--[2]\n' +
      '--[3]\n' +
      '--[4]\n' +
      '-[11]\n' +
      '--[12]\n' +
      '---[13]\n' +
      '----[8]\n' +
      '----[9]\n' +
      '-[10]\n';

    const newHead = new TreeNode2(11);
    const intermediate = new TreeNode2(12);
    newHead.addChild(intermediate);
    const newTail = new TreeNode2(13);
    intermediate.addChild(newTail);

    const head = nodes[5];
    const tail = nodes[7];
    TreeHelper.replaceChainWithChain(head, tail, newHead, newTail);
    assert.equal(treeAfter, TreeHelper.toString(nodes[0], stringFn));
  });

  it('ReplaceChainWithNode', () => {
    const nodes = createTestTree1();

    const treeAfter =
      '[0]\n' +
      '-[1]\n' +
      '--[2]\n' +
      '--[3]\n' +
      '--[4]\n' +
      '-[11]\n' +
      '--[8]\n' +
      '--[9]\n' +
      '-[10]\n';

    const newNode = new TreeNode2(11);
    const head = nodes[5];
    const tail = nodes[7];
    TreeHelper.replaceChainWithNode(head, tail, newNode);
    assert.equal(treeAfter, TreeHelper.toString(nodes[0], stringFn));
  });

  it('ReplaceNodeWithChain', () => {
    const nodes = createTestTree1();

    const treeAfter =
      '[0]\n' +
      '-[1]\n' +
      '--[2]\n' +
      '--[3]\n' +
      '--[4]\n' +
      '-[5]\n' +
      '--[6]\n' +
      '---[7]\n' +
      '----[11]\n' +
      '-----[12]\n' +
      '------[13]\n' +
      '----[9]\n' +
      '-[10]\n';

    const head = new TreeNode2(11);
    const other = new TreeNode2(12);
    head.addChild(other);
    const tail = new TreeNode2(13);
    other.addChild(tail);

    TreeHelper.replaceNodeWithChain(nodes[8], head, tail);
    assert.equal(treeAfter, TreeHelper.toString(nodes[0], stringFn));
  });

  it('PushNodeBelowChild', () => {
    const nodes = createTestTree1();

    const treeAfter =
      '[0]\n' +
      '-[1]\n' +
      '--[2]\n' +
      '--[3]\n' +
      '--[4]\n' +
      '-[5]\n' +
      '--[7]\n' +
      '---[6]\n' +
      '----[8]\n' +
      '---[9]\n' +
      '-[10]\n';

    const cloneFn = (node: TreeNode): TreeNode2 => {
      return new TreeNode2((node as TreeNode2).id);
    };

    const shouldPushDownFn = (child: TreeNode): boolean => {
      return (child as TreeNode2).id === 8;
    };

    // Pushing down n6, only to above grandchildren that have key == 8.
    TreeHelper.pushNodeBelowChild(nodes[6], shouldPushDownFn, cloneFn);
    assert.equal(treeAfter, TreeHelper.toString(nodes[0], stringFn));
  });

  it('SwapNodeWithChild', () => {
    const nodes = createTestTree1();

    const treeAfter =
      '[0]\n' +
      '-[1]\n' +
      '--[2]\n' +
      '--[3]\n' +
      '--[4]\n' +
      '-[6]\n' +
      '--[5]\n' +
      '---[7]\n' +
      '----[8]\n' +
      '----[9]\n' +
      '-[10]\n';
    const newSubtreeRoot = TreeHelper.swapNodeWithChild(nodes[5]);
    assert.equal(nodes[6], newSubtreeRoot);
    assert.equal(treeAfter, TreeHelper.toString(nodes[0], stringFn));
  });

  it('GetLeafNodes', () => {
    const nodes = createTestTree1();
    const leafNodes = TreeHelper.getLeafNodes(nodes[0]);
    const leafNodeKeys = leafNodes.map(node => (node as TreeNode2).id);
    assert.sameOrderedMembers([2, 3, 4, 8, 9, 10], leafNodeKeys);
  });

  it('Find', () => {
    const nodes = createTestTree1();
    const minId = 6;
    const retrievedNodes = TreeHelper.find(
      nodes[0],
      node => (node as TreeNode2).id >= minId
    );
    retrievedNodes.forEach(node => {
      assert.isTrue((node as TreeNode2).id >= minId);
    });
  });

  it('Find_Stop', () => {
    const nodes = createTestTree1();
    const minId = 4;
    const retrievedNodes = TreeHelper.find(
      nodes[0],
      node => (node as TreeNode2).id >= minId,
      node => (node as TreeNode2).id === 7
    );
    assert.sameOrderedMembers(
      [4, 5, 6, 7, 10],
      retrievedNodes.map(node => (node as TreeNode2).id)
    );
  });
});
