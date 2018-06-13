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

import {assert} from '../base/assert';
import {TreeNode} from './tree_node';

interface RemoveResult {
  parent: TreeNode;
  children: TreeNode[];
}

type NodeStringFn = (node: TreeNode) => string;

export class TreeHelper {
  // Creates a new tree with the exact same structure, where every node in the
  // tree has been replaced by a new node according to the mapping function.
  // This is equivalent to Array#map, but for a tree data structure.
  // Note: T1 and T2 are expected to be either lf.structs.TreeNode or subtypes
  // but there is no way to currently express that in JS compiler annotations.
  public static map<T1 extends TreeNode, T2 extends TreeNode>(
      origTree: T1, mapFn: (t: T1) => T2): T2 {
    // A stack storing nodes that will be used as parents later in the
    // traversal.
    const copyParentStack: TreeNode[] = [];

    // Removes a node from the parent stack, if that node has already reached
    // its target number of children.
    const cleanUpParentStack = (original: TreeNode, clone: TreeNode) => {
      if (original === null) {
        return;
      }

      const cloneFull = original.getChildCount() === clone.getChildCount();
      if (cloneFull) {
        const cloneIndex = copyParentStack.indexOf(clone);
        if (cloneIndex !== -1) {
          copyParentStack.splice(cloneIndex, 1);
        }
      }
    };

    // The node that should become the parent of the next traversed node.
    let nextParent: TreeNode = null as any as TreeNode;
    let copyRoot: T2 = null as any as T2;

    origTree.traverse((node) => {
      const newNode = mapFn(node as T1);

      if (node.getParent() == null) {
        copyRoot = newNode;
      } else {
        nextParent.addChild(newNode);
      }

      cleanUpParentStack(node.getParent(), nextParent);
      if (node.getChildCount() > 1) {
        copyParentStack.push(newNode);
      }
      nextParent =
          node.isLeaf() ? copyParentStack[copyParentStack.length - 1] : newNode;
      return true;
    });

    return copyRoot;
  }

  // Finds all leafs node existing in the subtree that starts at the given node.
  public static getLeafNodes(node: TreeNode): TreeNode[] {
    return TreeHelper.find(node, (n) => n.isLeaf());
  }

  // Removes a node from a tree. It takes care of re-parenting the children of
  // the removed node with its parent (if any).
  // Returns an object holding the parent of the node prior to removal (if any),
  // and the children of the node prior to removal.
  public static removeNode(node: TreeNode): RemoveResult {
    const parentNode = node.getParent();
    let originalIndex = 0;
    if (parentNode !== null) {
      originalIndex = parentNode.getChildren().indexOf(node);
      parentNode.removeChild(node);
    }

    const children = node.getChildren().slice();
    children.forEach((child, index) => {
      node.removeChild(child);
      if (parentNode !== null) {
        parentNode.addChildAt(child, originalIndex + index);
      }
    });

    return {
      children: children,
      parent: parentNode,
    };
  }

  // Inserts a new node under an existing node. The new node inherits all
  // children of the existing node, and the existing node ends up having only
  // the new node as a child. Example: Calling iusertNodeAt(n2, n6) would result
  // in the following transformation.
  //
  //        n1              n1
  //       /  \            /  \
  //      n2  n5          n2  n5
  //     /  \      =>    /
  //    n3  n4          n6
  //                   /  \
  //                  n3  n4
  public static insertNodeAt(existingNode: TreeNode, newNode: TreeNode): void {
    const children = existingNode.getChildren().slice();
    children.forEach((child) => {
      existingNode.removeChild(child);
      newNode.addChild(child);
    });

    existingNode.addChild(newNode);
  }

  // Swaps a node with its only child. The child also needs to have exactly one
  // child.
  // Example: Calling swapNodeWithChild(n2) would result in the following
  // transformation.
  //
  //        n1              n1
  //       /  \            /  \
  //      n2   n6         n3  n6
  //     /         =>    /
  //    n3              n2
  //   /  \            /  \
  //  n4  n5          n4  n5
  //
  // Returns the new root of the subtree that used to start where "node" was
  // before swapping.
  public static swapNodeWithChild(node: TreeNode): TreeNode {
    assert(node.getChildCount() === 1);
    const child = node.getChildAt(0) as TreeNode;
    assert(child.getChildCount() === 1);

    TreeHelper.removeNode(node);
    TreeHelper.insertNodeAt(child, node);
    return child;
  }

  // Pushes a node below its only child. It takes care of replicating the node
  // only for those branches where it makes sense.
  // Example: Calling
  //   pushNodeBelowChild(
  //       n2,
  //       function(grandChild) {return true;},
  //       function(node) {return node.clone();})
  //  would result in the following transformation.
  //
  //        n1              n1
  //       /  \            /  \
  //      n2   n6         n3  n6
  //     /         =>    /  \
  //    n3             n2'  n2''
  //   /  \            /      \
  //  n4  n5          n4      n5
  //
  //  where n2 has been pushed below n3, on both branches. n2'and n2'' denote
  //  that copies of the original node were made.
  //
  // |shouldPushDownFn| is a function that is called on every grandchild to
  // determine whether the node can be pushed down on that branch.
  // |cloneFn| is a function used to clone the node that is being pushed down.
  //
  // Returns the new parent of the subtree that used to start at "node" or
  // "node" itself if it could not be pushed down at all.
  public static pushNodeBelowChild(
      node: TreeNode, shouldPushDownFn: (node: TreeNode) => boolean,
      cloneFn: (node: TreeNode) => TreeNode) {
    assert(node.getChildCount() === 1);
    const child = node.getChildAt(0) as TreeNode;
    assert(child.getChildCount() > 1);

    const grandChildren = child.getChildren().slice();
    const canPushDown =
        grandChildren.some((grandChild) => shouldPushDownFn(grandChild));

    if (!canPushDown) {
      return node;
    }
    TreeHelper.removeNode(node);

    grandChildren.forEach((grandChild, index) => {
      if (shouldPushDownFn(grandChild)) {
        const newNode = cloneFn(node);
        child.removeChildAt(index);
        newNode.addChild(grandChild);
        child.addChildAt(newNode, index);
      }
    });

    return child;
  }

  // Replaces a chain of nodes with a new chain of nodes.
  // Example: Calling replaceChainWithChain(n2, n3, n7, n8) would result in the
  // following transformation.
  //
  //        n1              n1
  //       /  \            /  \
  //      n2   n6         n7   n6
  //     /         =>    /
  //    n3              n8
  //   /  \            /  \
  //  n4  n5          n4  n5
  //
  // Returns the new root of the subtree that used to start at "oldhead".
  // Effectively the new root is always equal to "newHead".
  public static replaceChainWithChain(
      oldHead: TreeNode, oldTail: TreeNode, newHead: TreeNode,
      newTail: TreeNode): TreeNode {
    const parentNode = oldHead.getParent();
    if (parentNode !== null) {
      const oldHeadIndex = parentNode.getChildren().indexOf(oldHead);
      parentNode.removeChildAt(oldHeadIndex);
      parentNode.addChildAt(newHead, oldHeadIndex);
    }

    oldTail.getChildren().slice().forEach((child) => {
      oldTail.removeChild(child);
      newTail.addChild(child);
    });

    return newHead;
  }

  // Removes a node from the tree, and replaces it with a chain of nodes where
  // each node in the chain (excluding the tail) has exactly one child.
  // Example: Calling replaceNodeWithChain(n6, n10, n12), where the chain
  // consists of n7->n8->n9, would result in the following transformation.
  //
  //        n1               n1
  //       /  \             /  \
  //      n2   n6          n2  n10
  //     /    /  \    =>  /      \
  //    n3   n7  n8      n3      n11
  //   /  \             /  \       \
  //  n4  n5          n4   n5      n12
  //                               /  \
  //                              n7  n8
  //
  // Returns the new root of the subtree that used to start at "node".
  // Effectively the new root is always equal to "head".
  public static replaceNodeWithChain(
      node: TreeNode, head: TreeNode, tail: TreeNode): TreeNode {
    return TreeHelper.replaceChainWithChain(node, node, head, tail);
  }

  // Replaces a chain of nodes with a new node.
  // Example: Calling replaceChainWithNode(n2, n3, n7) would result in the
  // following transformation.
  //
  //        n1              n1
  //       /  \            /  \
  //      n2   n6         n7   n6
  //     /         =>    /  \
  //    n3              n4  n5
  //   /  \
  //  n4  n5
  //
  // Returns the new root of the subtree that used to start at "head".
  // Effectively the new root is always equal to "node".
  public static replaceChainWithNode(
      head: TreeNode, tail: TreeNode, node: TreeNode): TreeNode {
    return TreeHelper.replaceChainWithChain(head, tail, node, node);
  }

  // Finds all nodes in the given tree that satisfy a given condition.
  // |root| is the root of the tree to search.
  // |filterFn| is the filter function. It will be called on every node of
  // the tree.
  // |stopFn| is a function that indicates whether searching should be stopped.
  // It will be called on every visited node on the tree. If false is returned
  // searching will stop for nodes below that node. If such a function were not
  // provided the entire tree is searched.
  public static find(
      root: TreeNode, filterFn: (node: TreeNode) => boolean,
      stopFn?: (node: TreeNode) => boolean): TreeNode[] {
    const results: TreeNode[] = [];

    /** @param {!lf.structs.TreeNode} node */
    const filterRec = (node: TreeNode) => {
      if (filterFn(node)) {
        results.push(node);
      }
      if (stopFn === undefined || stopFn === null || !stopFn(node)) {
        node.getChildren().forEach(filterRec);
      }
    };

    filterRec(root);
    return results;
  }

  // Returns a string representation of a tree. Useful for testing/debugging.
  // |stringFunc| is the function to use for converting a single node to a
  // string. If not provided a default function will be used.
  public static toString(rootNode: TreeNode, stringFunc?: NodeStringFn):
      string {
    const defaultStringFn: NodeStringFn = (node: TreeNode) => {
      return node.toString() + '\n';
    };
    const stringFn: NodeStringFn = stringFunc || defaultStringFn;
    let out = '';
    rootNode.traverse((node) => {
      for (let i = 0; i < node.getDepth(); i++) {
        out += '-';
      }
      out += stringFn(node);
      return true;
    });
    return out;
  }
}
