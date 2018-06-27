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
import {Order} from '../../lib/base/enum';
import {ErrorCode} from '../../lib/base/exception';
import {Row} from '../../lib/base/row';
import {BTree} from '../../lib/index/btree';
import {BTreeNode} from '../../lib/index/btree_node';
import {Comparator} from '../../lib/index/comparator';
import {Key, SingleKeyRange} from '../../lib/index/key_range';
import {MultiKeyComparator} from '../../lib/index/multi_key_comparator';
import {MultiKeyComparatorWithNull} from '../../lib/index/multi_key_comparator_with_null';
import {SimpleComparator} from '../../lib/index/simple_comparator';
import {TestMultiKeyIndex} from '../../testing/index/test_multi_key_index';
import {TestMultiRowNumericalKey} from '../../testing/index/test_multi_row_numerical_key';
import {TestSingleRowNumericalKey} from '../../testing/index/test_single_row_numerical_key';
import {TestSingleRowStringKey} from '../../testing/index/test_single_row_string_key';
import {TestUtil} from '../../testing/test_util';

const assert = chai.assert;

describe('BTree', () => {
  let c: SimpleComparator;
  let c2: SimpleComparator;

  const maxCount = (BTreeNode as any).MAX_COUNT;
  const maxKeyLen = (BTreeNode as any).MAX_KEY_LEN;
  const minKeyLen = (BTreeNode as any).MIN_KEY_LEN;

  function stubBTreeParam(): void {
    (BTreeNode as any).MAX_COUNT = 5;
    (BTreeNode as any).MAX_KEY_LEN = 5 - 1;
    (BTreeNode as any).MIN_KEY_LEN = 5 >> 1;
    (Row as any).nextId = 0;
  }

  function resetBTreeParam(): void {
    (BTreeNode as any).MAX_COUNT = maxCount;
    (BTreeNode as any).MAX_KEY_LEN = maxKeyLen;
    (BTreeNode as any).MIN_KEY_LEN = minKeyLen;
  }

  beforeEach(() => {
    c = new SimpleComparator(Order.ASC);
    c2 = new SimpleComparator(Order.DESC);
    stubBTreeParam();
  });

  afterEach(() => {
    resetBTreeParam();
  });

  // clang-format off
  const SEQUENCE = [
    13, 9, 21, 17,
    5,
    11, 3, 25, 27,
    14, 15, 31, 29, 22, 23, 38, 45, 47,
    49,
    1,
    10, 12, 16];

  const SEQUENCE2: Array<[number, string]> = [
    [13, '13'], [9, '09'], [21, '21'], [17, '17'],
    [5, '05'],
    [11, '11'], [3, '03'], [25, '25'], [27, '27'],
    [14, '14'], [15, '15'], [31, '31'], [29, '29'], [22, '22'],
    [23, '23'], [38, '38'], [45, '45'], [47, '47'],
    [49, '49'],
    [1, '1'],
    [10, '10'], [12, '12'], [16, '16'],
  ];
  // clang-format on

  function insertToTree(index: number, duplicate?: boolean): BTree {
    const unique: boolean = !duplicate;
    const tree = new BTree('test', c, unique);
    let i = 0;
    while (i < index) {
      tree.add(SEQUENCE[i], SEQUENCE[i]);
      if (duplicate) {
        tree.add(SEQUENCE[i], SEQUENCE[i] * 1000);
      }
      i++;
    }
    return tree;
  }

  function insertToTree2(
      index: number, comparator: Comparator, duplicate?: boolean): BTree {
    const unique = !duplicate;
    const tree = new BTree('test', comparator, unique);
    let i = 0;
    while (i < index) {
      tree.add(SEQUENCE2[i], SEQUENCE2[i][0]);
      if (duplicate) {
        tree.add(SEQUENCE[i], SEQUENCE[i][0] * 1000);
      }
      i++;
    }
    return tree;
  }

  function deserializeTree(rows: Row[]): BTree {
    return BTree.deserialize(c, 'test', true, rows);
  }

  it('emptyTree', () => {
    // Creating empty tree shall have no problem.
    const tree = insertToTree(0);
    const expected = '0[]\n_{}_\n';
    assert.equal(expected, tree.toString());

    // Serialize and deserialize should have no problem.
    const rows = tree.serialize();
    assert.equal(1, rows.length);
    const tree2 = deserializeTree(rows);
    assert.equal(expected, tree2.toString());
  });

  it('LeafNodeAsRoot', () => {
    const tree = insertToTree(4);
    const expected = [
      '0[9|13|17|21]',
      '_{9/13/17/21}_',
      '',
    ].join('\n');
    assert.equal(expected, tree.toString());

    // Serialize and deserialize should have no problem.
    const rows = tree.serialize();
    assert.equal(1, rows.length);
    const tree2 = deserializeTree(rows);
    assert.equal(expected, tree2.toString());
  });

  /**
   * Splits the root node to form new root node.
   *
   * 9|13|17|21
   *
   * insert 5
   *
   *     13
   *    /  \
   *  5|9  13|17|21
   */
  it('FirstInternalNode', () => {
    const tree = insertToTree(5);
    const expected = [
      '2[13]',
      '_{0|1}_',
      '0[5|9]  1[13|17|21]',
      '_{5/9}2  0{13/17/21}2',
      '',
    ].join('\n');
    assert.equal(expected, tree.toString());
  });

  /**
   * Split of leaf node.
   *
   *        13
   *     /     \
   * 3|5|9|11  13|17|21|25
   *
   * insert 27
   *
   *          13|21
   *     /      |      \
   * 3|5|9|11  13|17   21|25|27
   */
  it('Split_Case1', () => {
    const tree = insertToTree(9);
    const expected = [
      '2[13|21]',
      '_{0|1|3}_',
      '0[3|5|9|11]  1[13|17]  3[21|25|27]',
      '_{3/5/9/11}2  0{13/17}2  1{21/25/27}2',
      '',
    ].join('\n');
    assert.equal(expected, tree.toString());

    // Serialize and deserialize should have no problem.
    // Note: Tree deserialization will create internal and root nodes on-the-fly
    //       and therefore the node id will be different. Moreover, the internal
    //       nodes can also be different since a rebalancing is done. That's the
    //       reason to use expected2 in this case and following serialization
    //       tests.

    const expected2 = [
      '6[13|21]',
      '_{0|1|3}_',
      '0[3|5|9|11]  1[13|17]  3[21|25|27]',
      '_{3/5/9/11}6  0{13/17}6  1{21/25/27}6',
      '',
    ].join('\n');
    const rows = tree.serialize();
    assert.equal(3, rows.length);
    const tree2 = deserializeTree(rows);
    assert.equal(expected2, tree2.toString());
  });

  /**
   * Split of leaf node inducing split of internal nodes and a new level.
   *
   *                        13|21|27|31
   *     /          /            |         \         \
   * 3|5|9|11  13|14|15|17  21|22|23|25  27|29  31|38|45|47
   *
   * insert 49
   *                              27
   *                 /                            \
   *              13|21                         31|45
   *     /          |            \          /     |      \
   * 3|5|9|11  13|14|15|17  21|22|23|25  27|29  31|38  45|47|49
   */
  it('Split_Case2', () => {
    const tree = insertToTree(19);
    const expected = [
      '11[27]\n',
      '_{2|12}_\n',
      '2[13|21]  12[31|45]\n',
      '_{0|1|3}11  2{5|7|9}11\n',
      '0[3|5|9|11]  1[13|14|15|17]  3[21|22|23|25]',
      '  5[27|29]  7[31|38]  9[45|47|49]\n',
      '_{3/5/9/11}2  0{13/14/15/17}2  1{21/22/23/25}2',
      '  3{27/29}12  5{31/38}12  7{45/47/49}12\n',
    ].join('');
    assert.equal(expected, tree.toString());
  });

  /**
   * Split of leaf node promoting a new key in internal node.
   *
   *                              27
   *                 /                            \
   *              13|21                         31|45
   *     /          |            \          /     |      \
   * 3|5|9|11  13|14|15|17  21|22|23|25  27|29  31|38  45|47|49
   *
   * insert 1
   *                               27
   *               /                              \
   *            5|13|21                         31|45
   *  /      /         \            \          /     |      \
   * 1|3  5|9|11  13|14|15|17  21|22|23|25  27|29  31|38  45|47|49
   */
  it('Split_Case3', () => {
    const tree = insertToTree(20);
    const expected = [
      '11[27]\n',
      '_{2|12}_\n',
      '2[5|13|21]  12[31|45]\n',
      '_{0|13|1|3}11  2{5|7|9}11\n',
      '0[1|3]  13[5|9|11]  1[13|14|15|17]  3[21|22|23|25]',
      '  5[27|29]  7[31|38]  9[45|47|49]\n',
      '_{1/3}2  0{5/9/11}2  13{13/14/15/17}2  1{21/22/23/25}2',
      '  3{27/29}12  5{31/38}12  7{45/47/49}12\n',
    ].join('');
    assert.equal(expected, tree.toString());

    // Serialize and deserialize should have no problem.
    const expected2 = [
      '16[27]\n',
      '_{17|18}_\n',
      '17[5|13|21]  18[31|45]\n',
      '_{0|13|1|3}16  17{5|7|9}16\n',
      '0[1|3]  13[5|9|11]  1[13|14|15|17]  3[21|22|23|25]',
      '  5[27|29]  7[31|38]  9[45|47|49]\n',
      '_{1/3}17  0{5/9/11}17  13{13/14/15/17}17  1{21/22/23/25}17',
      '  3{27/29}18  5{31/38}18  7{45/47/49}18\n',
    ].join('');
    const rows = tree.serialize();
    assert.equal(7, rows.length);
    const tree2 = deserializeTree(rows);
    assert.equal(expected2, tree2.toString());
  });

  /**
   * Split of leaf node causing double promotion.
   *
   *                                 27
   *               /                                        \
   *          5|10|13|21                                  31|45
   *  /      /    |           \            \          /     |      \
   * 1|3  5|9  10|11|12  13|14|15|17  21|22|23|25  27|29  31|38  45|47|49
   *
   * insert 16
   *
   *                              13|27
   *               /                |                           \
   *      5|10                    15|21                       31|45
   *   /   |      \         /       |          \         /      |       \
   * 1|3  5|9  10|11|12  13|14  15|16|17  21|22|23|25  27|29  31|38  45|47|49
   */
  it('Split_Case4', () => {
    const tree = insertToTree(23);
    const expected = [
      '11[13|27]\n',
      '_{2|20|12}_\n',
      '2[5|10]  20[15|21]  12[31|45]\n',
      '_{0|13|15}11  2{1|17|3}11  20{5|7|9}11\n',
      '0[1|3]  13[5|9]  15[10|11|12]  1[13|14]  17[15|16|17]  ',
      '3[21|22|23|25]  5[27|29]  7[31|38]  9[45|47|49]\n',
      '_{1/3}2  0{5/9}2  13{10/11/12}2',
      '  15{13/14}20  1{15/16/17}20  17{21/22/23/25}20',
      '  3{27/29}12  5{31/38}12  7{45/47/49}12\n',
    ].join('');
    assert.equal(expected, tree.toString());
  });

  /**
   * Tests split leaf and internal which have links to the right.
   *
   *                               363
   *             /                                     \
   *          98|100                                 366|369
   *     /      |       \                    /          |         \
   * -995|97  98|99  100|101|102|103  363|364|365  366|367|368 369|370|371
   *
   *  insert 104
   *                               363
   *                /                                       \
   *             98|100|102                              366|369
   *     /      |       \       \                /          |         \
   * -995|97  98|99  100|101  102|103|104  363|364|365  366|367|368 369|370|371
   */
  it('Split_Case5', () => {
    const tree = new BTree('test', c, true);
    // clang-format off
    const keys = [
      -995, 371, 370, 369,
      368,  // New level created here
      367, 366, 365, 364, 363, 97, 98, 99, 100, 101,
      102,  // New level created here
      103, 104,  // Split leaf node with right link
      105, 106, 486,
      107, 108,  // Split internal node with right link
    ];
    // clang-format on
    for (let i = 0; i < keys.length; ++i) {
      tree.add(keys[i], i);
    }

    const expected = [
      '11[102|363]\n',
      '_{2|20|12}_\n',
      '2[98|100]  20[104|106]  12[366|369]\n',
      '_{0|7|9}11  2{13|15|17}11  20{5|3|1}11\n',
      '0[-995|97]  7[98|99]  9[100|101]',
      '  13[102|103]  15[104|105]  17[106|107|108]',
      '  5[363|364|365]  3[366|367|368]  1[369|370|371|486]\n',
      '_{0/10}2  0{11/12}2  7{13/14}2',
      '  9{15/16}20  13{17/18}20  15{19/21/22}20',
      '  17{9/8/7}12  5{6/5/4}12  3{3/2/1/20}12\n',
    ].join('');

    assert.equal(expected, tree.toString());
  });

  it('ContainsKey', () => {
    const tree = insertToTree(23);
    for (let i = 0; i < 23; i++) {
      const key = SEQUENCE[i];
      assert.isTrue(tree.containsKey(key));
    }
    assert.isFalse(tree.containsKey(0));
    assert.isFalse(tree.containsKey(18));
    assert.isFalse(tree.containsKey(50));
  });

  it('Get', () => {
    const tree = insertToTree(23);
    for (let i = 0; i < 23; i++) {
      const key = SEQUENCE[i];
      assert.sameOrderedMembers([key], tree.get(key));
    }
    assert.sameOrderedMembers([], tree.get(0));
    assert.sameOrderedMembers([], tree.get(18));
    assert.sameOrderedMembers([], tree.get(50));
  });

  it('ConstructFromData', () => {
    const key = SEQUENCE.slice(0, 23).sort((a, b) => a - b);
    const data = key.map((i) => ({key: i, value: i}));
    const tree = new BTree('test', c, true, data);
    const expected = [
      '6[21]\n',
      '_{7|8}_\n',
      '7[10|14]  8[27|45]\n',
      '_{0|1|2}6  7{3|4|5}6\n',
      '0[1|3|5|9]  1[10|11|12|13]  2[14|15|16|17]  3[21|22|23|25]',
      '  4[27|29|31|38]  5[45|47|49]\n',
      '_{1/3/5/9}7  0{10/11/12/13}7  1{14/15/16/17}7',
      '  2{21/22/23/25}8  3{27/29/31/38}8  4{45/47/49}8\n',
    ].join('');
    assert.equal(expected, tree.toString());
  });

  /**
   * Deletes the last few keys from root.
   *
   * 9|13|17|21
   *
   * Delete 9, 17, 21, and 13. Also tests deleting an non-existent value shall
   * yield no-op.
   */
  it('Delete_RootSimple', () => {
    const tree = insertToTree(4);
    tree.remove(9);
    tree.remove(17);
    tree.remove(21);
    assert.equal('0[13]\n_{13}_\n', tree.toString());
    tree.remove(22);
    assert.equal('0[13]\n_{13}_\n', tree.toString());
    tree.remove(13);
    assert.equal('0[]\n_{}_\n', tree.toString());
  });

  /**
   *          13|21
   *     /      |        \
   * 3|5|9|11  13|17  21|25|27
   *
   * delete 3 should just change the left most leaf node.
   */
  it('Delete_Simple', () => {
    const tree = insertToTree(9);
    tree.remove(3);
    const expected = [
      '2[13|21]',
      '_{0|1|3}_',
      '0[5|9|11]  1[13|17]  3[21|25|27]',
      '_{5/9/11}2  0{13/17}2  1{21/25/27}2',
      '',
    ].join('\n');
    assert.equal(expected, tree.toString());
  });

  /**
   *          13|21
   *     /      |        \
   * 3|5|9|11  13|17  21|25|27
   *
   * delete 17
   *
   *          13|25
   *     /      |       \
   * 3|5|9|11  13|21  25|27
   */
  it('Delete_LeafStealFromRight', () => {
    const tree = insertToTree(9);
    tree.remove(17);
    const expected = [
      '2[13|25]',
      '_{0|1|3}_',
      '0[3|5|9|11]  1[13|21]  3[25|27]',
      '_{3/5/9/11}2  0{13/21}2  1{25/27}2',
      '',
    ].join('\n');
    assert.equal(expected, tree.toString());
  });

  /**
   *          13|25
   *     /      |       \
   * 3|5|9|11  13|21  25|27
   *
   * delete 21
   *
   *         11|25
   *      /    |     \
   * 3|5|9  11|13  25|27
   */
  it('Delete_LeafStealFromLeft', () => {
    const tree = insertToTree(9);
    tree.remove(17);
    tree.remove(21);
    const expected = [
      '2[11|25]',
      '_{0|1|3}_',
      '0[3|5|9]  1[11|13]  3[25|27]',
      '_{3/5/9}2  0{11/13}2  1{25/27}2',
      '',
    ].join('\n');
    assert.equal(expected, tree.toString());
  });

  /**
   *         11|25
   *      /    |     \
   * 3|5|9  11|13  25|27
   *
   * delete 9, 13
   *
   *      11
   *    /    \
   * 3|5  11|25|27
   */
  it('Delete_LeafMergeRight', () => {
    const tree = insertToTree(9);
    tree.remove(17);
    tree.remove(21);
    tree.remove(9);
    tree.remove(13);
    const expected = [
      '2[11]',
      '_{0|3}_',
      '0[3|5]  3[11|25|27]',
      '_{3/5}2  0{11/25/27}2',
      '',
    ].join('\n');
    assert.equal(expected, tree.toString());
  });

  /**
   *          13|21
   *     /      |        \
   * 3|5|9|11  13|17  21|25|27
   *
   * delete 27, 25
   *
   *         13
   *     /        \
   * 3|5|9|11  13|17|21
   */
  it('Delete_LeafMergeLeft', () => {
    const tree = insertToTree(9);
    tree.remove(27);
    tree.remove(25);
    const expected = [
      '2[13]',
      '_{0|1}_',
      '0[3|5|9|11]  1[13|17|21]',
      '_{3/5/9/11}2  0{13/17/21}2',
      '',
    ].join('\n');
    assert.equal(expected, tree.toString());
  });

  /**
   *     13
   *    /  \
   *  5|9  13|17|21
   *
   *  delete 5
   *
   *      17
   *    /    \
   *  9|13  17|21
   *
   *  delete 13
   *
   *  9|17|21
   */
  it('Delete_MergeRightAndPromoteAsRoot', () => {
    const tree = insertToTree(5);
    tree.remove(5);
    tree.remove(13);
    const expected = [
      '1[9|17|21]',
      '_{9/17/21}_',
      '',
    ].join('\n');
    assert.equal(expected, tree.toString());
  });

  /**
   *     13
   *    /  \
   *  5|9  13|17|21
   *
   *  delete 17
   *
   *      13
   *    /    \
   *  5|9  13|21
   *
   *  delete 21
   *
   *  5|9|13
   */
  it('Delete_MergeLeftAndPromoteAsRoot', () => {
    const tree = insertToTree(5);
    tree.remove(17);
    tree.remove(21);
    const expected = [
      '0[5|9|13]',
      '_{5/9/13}_',
      '',
    ].join('\n');
    assert.equal(expected, tree.toString());
  });

  /**
   *                              27
   *                 /                            \
   *              13|21                         31|45
   *     /          |            \          /     |      \
   * 3|5|9|11  13|14|15|17  21|22|23|25  27|29  31|38  45|47|49
   *
   * delete 45
   *                              27
   *                 /                            \
   *              13|21                         31|47
   *     /          |            \          /     |      \
   * 3|5|9|11  13|14|15|17  21|22|23|25  27|29  31|38  47|49
   */
  it('Delete_InternalNodeKey', () => {
    const tree = insertToTree(19);
    tree.remove(45);
    const expected = [
      '11[27]\n',
      '_{2|12}_\n',
      '2[13|21]  12[31|47]\n',
      '_{0|1|3}11  2{5|7|9}11\n',
      '0[3|5|9|11]  1[13|14|15|17]  3[21|22|23|25]',
      '  5[27|29]  7[31|38]  9[47|49]\n',
      '_{3/5/9/11}2  0{13/14/15/17}2  1{21/22/23/25}2',
      '  3{27/29}12  5{31/38}12  7{47/49}12\n',
    ].join('');
    assert.equal(expected, tree.toString());
  });

  /**
   *                              27
   *                 /                           \
   *              13|21                         31|45
   *     /          |            \          /     |      \
   * 3|5|9|11  13|14|15|17  21|22|23|25  27|29  31|38  45|47|49
   *
   * delete 27
   *
   *                             25
   *                  /                       \
   *               13|21                     31|45
   *      /          |          \       /      |       \
   * 3|5|9|11  13|14|15|17  21|22|23  25|29  31|38  45|47|49
   */
  it('Delete_InternalNodeKey2', () => {
    const tree = insertToTree(19);
    tree.remove(27);
    const expected = [
      '11[25]\n',
      '_{2|12}_\n',
      '2[13|21]  12[31|45]\n',
      '_{0|1|3}11  2{5|7|9}11\n',
      '0[3|5|9|11]  1[13|14|15|17]  3[21|22|23]',
      '  5[25|29]  7[31|38]  9[45|47|49]\n',
      '_{3/5/9/11}2  0{13/14/15/17}2  1{21/22/23}2',
      '  3{25/29}12  5{31/38}12  7{45/47/49}12\n',
    ].join('');
    assert.equal(expected, tree.toString());
  });

  /**
   *                              13|27
   *         /                      |                          \
   *      5|10                    15|21                       31|45
   *   /   |      \         /       |          \         /      |       \
   * 1|3  5|9  10|11|12  13|14  15|16|17  21|22|23|25  27|29  31|38  45|47|49
   *
   * delete 16, 12, 10, 1, 49
   *
   *                               27
   *              /                              \
   *        11|15|21                            31|45
   *    /      |       \         \          /     |      \
   * 3|5|9  11|13|14  15|17  21|22|23|25  27|29  31|38  45|47
   *
   * delete 47
   *                         21
   *             /                         \
   *          11|15                       27|31
   *    /      |       \         /          |       \
   * 3|5|9  11|13|14  15|17  21|22|23|25  27|29  31|38|45
   */
  it('Delete_StealLeft', () => {
    const tree = insertToTree(23);
    tree.remove(16);
    tree.remove(12);
    tree.remove(10);
    tree.remove(1);
    tree.remove(49);
    tree.remove(47);
    const expected = [
      '11[21]\n',
      '_{20|12}_\n',
      '20[11|15]  12[27|31]\n',
      '_{13|1|17}11  20{3|5|7}11\n',
      '13[3|5|9]  1[11|13|14]  17[15|17]',
      '  3[21|22|23|25]  5[27|29]  7[31|38|45]\n',
      '_{3/5/9}20  13{11/13/14}20  1{15/17}20',
      '  17{21/22/23/25}12  3{27/29}12  5{31/38/45}12\n',
    ].join('');
    assert.equal(expected, tree.toString());
  });

  /**
   *                              13|27
   *               /                |                          \
   *      5|10                    15|21                       31|45
   *   /   |      \         /       |          \         /      |       \
   * 1|3  5|9  10|11|12  13|14  15|16|17  21|22|23|25  27|29  31|38  45|47|49
   *
   * delete 25, 23, 22, 21, 17, 12
   *
   *                     13
   *        /                            \
   *      5|10                      15|27|31|45
   *  /    |     \         /      /      |      \      \
   * 1|3  5|9  10|11  13|14  15|16  27|29  31|38  45|47|49
   *
   * delete 5
   *
   *                     15
   *          /                        \
   *        9|13                    27|31|45
   *  /      |       \      /      /      \       \
   * 1|3  9|10|11  13|14  15|16  27|29  31|38  45|47|49
   */
  it('Delete_StealRight', () => {
    const tree = insertToTree(23);
    tree.remove(25);
    tree.remove(23);
    tree.remove(22);
    tree.remove(21);
    tree.remove(17);
    tree.remove(12);
    tree.remove(5);
    const expected = [
      '11[15]\n',
      '_{2|12}_\n',
      '2[9|13]  12[27|31|45]\n',
      '_{0|15|1}11  2{17|5|7|9}11\n',
      '0[1|3]  15[9|10|11]  1[13|14]',
      '  17[15|16]  5[27|29]  7[31|38]  9[45|47|49]\n',
      '_{1/3}2  0{9/10/11}2  15{13/14}2',
      '  1{15/16}12  17{27/29}12  5{31/38}12  7{45/47/49}12\n',
    ].join('');
    assert.equal(expected, tree.toString());
  });

  /**
   *                              27
   *                 /                           \
   *              13|21                         31|47
   *     /          |            \          /     |      \
   * 3|5|9|11  13|14|15|17  21|22|23|25  27|29  31|38  47|49
   *
   * delete 47
   *
   *                        13|21|27|31
   *     /          /            |         \        \
   * 3|5|9|11  13|14|15|17  21|22|23|25  27|29  31|38|49
   */
  it('Delete_MergeLeft', () => {
    const tree = insertToTree(19);
    tree.remove(45);
    tree.remove(47);
    const expected = [
      '2[13|21|27|31]\n',
      '_{0|1|3|5|7}_\n',
      '0[3|5|9|11]  1[13|14|15|17]  3[21|22|23|25]  5[27|29]  7[31|38|49]\n',
      '_{3/5/9/11}2  0{13/14/15/17}2  1{21/22/23/25}2  3{27/29}2',
      '  5{31/38/49}2\n',
    ].join('');
    assert.equal(expected, tree.toString());
  });

  /**
   *                              27
   *                 /                           \
   *              13|21                         31|45
   *     /          |            \          /     |      \
   * 3|5|9|11  13|14|15|17  21|22|23|25  27|29  31|38  45|47|49
   *
   * delete 9, 11, 15, 17, 23, 25
   *
   *                     27
   *            /                 \
   *        13|21                31|45
   *     /    |      \       /     |      \
   *   3|5  13|14  21|22  27|29  31|38  45|47|49
   *
   * delete 13
   *
   *             14|27|31|45
   *   /      /       |      \       \
   * 3|5  14|21|22  27|29  31|38  45|47|49
   */
  it('Delete_MergeRight', () => {
    const tree = insertToTree(19);
    tree.remove(9);
    tree.remove(11);
    tree.remove(15);
    tree.remove(17);
    tree.remove(23);
    tree.remove(25);
    tree.remove(13);
    const expected = [
      '12[14|27|31|45]',
      '_{0|3|5|7|9}_',
      '0[3|5]  3[14|21|22]  5[27|29]  7[31|38]  9[45|47|49]',
      '_{3/5}12  0{14/21/22}12  3{27/29}12  5{31/38}12  7{45/47/49}12',
      '',
    ].join('\n');
    assert.equal(expected, tree.toString());
  });

  /**
   *          13|21
   *     /      |        \
   * 3|5|9|11  13|17  21|25|27
   *
   * delete 3, 5, 9
   *
   *         21
   *      /      \
   * 11|13|17  21|25|27
   */
  it('Delete_MergeRight2', () => {
    const tree = insertToTree(9);
    tree.remove(3);
    tree.remove(5);
    tree.remove(9);
    const expected = [
      '2[21]',
      '_{1|3}_',
      '1[11|13|17]  3[21|25|27]',
      '_{11/13/17}2  1{21/25/27}2',
      '',
    ].join('\n');
    assert.equal(expected, tree.toString());
  });

  it('Delete_All', () => {
    const tree = insertToTree(23);
    for (let i = 0; i < 23; ++i) {
      tree.remove(SEQUENCE[i]);
    }
    assert.equal('17[]\n_{}_\n', tree.toString());
  });

  it('Delete_All2', () => {
    const tree = insertToTree(23);
    for (let i = 22; i >= 0; --i) {
      tree.remove(SEQUENCE[i]);
    }
    assert.equal('13[]\n_{}_\n', tree.toString());
  });

  it('Delete_None', () => {
    const tree = insertToTree(23);
    tree.remove(18);
    const expected = [
      '11[13|27]\n',
      '_{2|20|12}_\n',
      '2[5|10]  20[15|21]  12[31|45]\n',
      '_{0|13|15}11  2{1|17|3}11  20{5|7|9}11\n',
      '0[1|3]  13[5|9]  15[10|11|12]  1[13|14]  17[15|16|17]  3[21|22|23|25]',
      '  5[27|29]  7[31|38]  9[45|47|49]\n',
      '_{1/3}2  0{5/9}2  13{10/11/12}2',
      '  15{13/14}20  1{15/16/17}20  17{21/22/23/25}20',
      '  3{27/29}12  5{31/38}12  7{45/47/49}12\n',
    ].join('');
    assert.equal(expected, tree.toString());
  });

  it('SingleRow_NumericalKey_Asc', () => {
    const test =
        new TestSingleRowNumericalKey(() => new BTree('test', c, true));
    test.run();
  });

  it('SingleRow_NumericalKey_Desc', () => {
    const test =
        new TestSingleRowNumericalKey(() => new BTree('test', c2, true), true);
    test.run();
  });

  it('SingleRow_StringKey_Asc', () => {
    const test = new TestSingleRowStringKey(() => new BTree('test', c, true));
    test.run();
  });

  it('SingleRow_StringKey_Desc', () => {
    const test =
        new TestSingleRowStringKey(() => new BTree('test', c2, true), true);
    test.run();
  });

  it('MultiKeyIndex', () => {
    const test = new TestMultiKeyIndex(() => {
      return new BTree(
          'test', new MultiKeyComparator([Order.ASC, Order.DESC]), true);
    });
    test.run();
  });

  it('MultiRow_NumericalKey', () => {
    const test =
        new TestMultiRowNumericalKey(() => new BTree('test', c, false));
    test.run();
  });

  it('GetRange_Numeric', () => {
    const tree = new BTree('test', c, true);
    for (let i = -10; i <= 10; ++i) {
      tree.set(i, i);
    }

    let results = tree.getRange();
    assert.equal(21, results.length);
    assert.equal(-10, results[0]);
    assert.equal(10, results[20]);
    const results2 = tree.getRange([SingleKeyRange.all()]);
    assert.sameOrderedMembers(results, results2);

    results = tree.getRange([SingleKeyRange.only(0)]);
    assert.equal(1, results.length);
    assert.equal(0, results[0]);

    results = tree.getRange([SingleKeyRange.only(12)]);
    assert.sameOrderedMembers([], results);

    results = tree.getRange([SingleKeyRange.lowerBound(0)]);
    assert.equal(11, results.length);
    assert.equal(0, results[0]);
    assert.equal(10, results[10]);

    results = tree.getRange([SingleKeyRange.upperBound(0)]);
    assert.equal(11, results.length);
    assert.equal(-10, results[0]);
    assert.equal(0, results[10]);

    results = tree.getRange([SingleKeyRange.lowerBound(0, true)]);
    assert.equal(10, results.length);
    assert.equal(1, results[0]);
    assert.equal(10, results[9]);

    results = tree.getRange([SingleKeyRange.upperBound(0, true)]);
    assert.equal(10, results.length);
    assert.equal(-10, results[0]);
    assert.equal(-1, results[9]);

    tree.remove(7);
    results = tree.getRange([SingleKeyRange.only(7)]);
    assert.equal(0, results.length);
  });

  it('GetRange_LimitSkip', () => {
    const tree = new BTree('test', c, true);
    for (let i = -10; i <= 10; ++i) {
      tree.set(i, i);
    }

    let results = tree.getRange();
    let results2 = tree.getRange([SingleKeyRange.all()]);
    assert.equal(21, results.length);
    assert.sameOrderedMembers(results, results2);
    results2 = tree.getRange(undefined, /* reverse */ true);
    assert.sameOrderedMembers(results, results2.reverse());
    results2 = tree.getRange(undefined, false, /* limit */ 5, /* skip */ 10);
    assert.sameOrderedMembers([0, 1, 2, 3, 4], results2);
    results2 = tree.getRange(undefined, true, 3, 5);
    assert.sameOrderedMembers([5, 4, 3], results2);
    results2 = tree.getRange(undefined, false, 3);
    assert.sameOrderedMembers([-10, -9, -8], results2);
    results2 = tree.getRange(undefined, true, 3);
    assert.sameOrderedMembers([10, 9, 8], results2);
    results2 = tree.getRange(undefined, false, undefined, 17);
    assert.sameOrderedMembers([7, 8, 9, 10], results2);
    results2 = tree.getRange(undefined, true, undefined, 18);
    assert.sameOrderedMembers([-8, -9, -10], results2);
    results2 = tree.getRange(undefined, false, undefined, 22);
    assert.sameOrderedMembers([], results2);
    results2 = tree.getRange(undefined, true, undefined, 22);
    assert.sameOrderedMembers([], results2);

    const keyRange = [SingleKeyRange.lowerBound(1)];
    results = tree.getRange(keyRange, true);
    assert.equal(10, results.length);
    assert.equal(10, results[0]);
    assert.equal(1, results[9]);

    results = tree.getRange(keyRange, false, 4, 4);
    assert.sameOrderedMembers([5, 6, 7, 8], results);
    results = tree.getRange(keyRange, false, 4);
    assert.sameOrderedMembers([1, 2, 3, 4], results);
    results = tree.getRange(keyRange, true, 4);
    assert.sameOrderedMembers([10, 9, 8, 7], results);
    results = tree.getRange(keyRange, false, undefined, 18);
    assert.sameOrderedMembers([], results);
    results = tree.getRange(keyRange, false, undefined, 8);
    assert.sameOrderedMembers([9, 10], results);
    results = tree.getRange(keyRange, true, undefined, 8);
    assert.sameOrderedMembers([2, 1], results);
    results = tree.getRange(keyRange, true, 2, 8);
    assert.sameOrderedMembers([2, 1], results);
  });

  it('GetRange_EmptyTree', () => {
    const tree = new BTree(
        'test', new MultiKeyComparator([Order.ASC, Order.DESC]), true);
    assert.sameOrderedMembers([], tree.getRange());
  });

  it('UniqueConstraint', () => {
    const tree = insertToTree(9);
    TestUtil.assertThrowsError(
        ErrorCode.DUPLICATE_KEYS, () => tree.add(13, 13));
  });

  it('RandomNumbers', () => {
    resetBTreeParam();
    const ROW_COUNT = 5000;
    const set = new Set<number>();
    while (set.size < ROW_COUNT) {
      set.add(Math.floor(Math.random() * ROW_COUNT * 100));
    }

    const keys = Array.from(set.values()).sort((a, b) => a - b);
    const tree = new BTree('test', c, true);
    const tree2 = new BTree('test', c2, true);
    for (let i = 0; i < ROW_COUNT; ++i) {
      tree.add(keys[i], keys[i]);
      tree2.add(keys[i], keys[i]);
    }

    assert.sameOrderedMembers(keys, tree.getRange());
    for (let i = 0; i < ROW_COUNT; ++i) {
      tree.remove(keys[i]);
      tree2.remove(keys[i]);
    }

    assert.sameOrderedMembers([], tree.getRange());
    assert.sameOrderedMembers([], tree2.getRange());
  });

  it('DuplicateKeys_LeafNodeAsRoot', () => {
    const tree = insertToTree(4, true);
    const expected = [
      '0[9|13|17|21]',
      '_{9,9000/13,13000/17,17000/21,21000}_',
      '',
    ].join('\n');
    assert.equal(expected, tree.toString());

    // Serialize and deserialize should have no problem.
    const rows = tree.serialize();
    assert.equal(1, rows.length);
    const tree2 = deserializeTree(rows);
    assert.equal(expected, tree2.toString());
  });

  it('DuplicateKeys_DeleteNone', () => {
    const tree = insertToTree(23, true);
    tree.remove(18);
    const expected = [
      '11[13|27]\n',
      '_{2|20|12}_\n',
      '2[5|10]  20[15|21]  12[31|45]\n',
      '_{0|13|15}11  2{1|17|3}11  20{5|7|9}11\n',
      '0[1|3]  13[5|9]  15[10|11|12]  1[13|14]  17[15|16|17]  ',
      '3[21|22|23|25]  5[27|29]  7[31|38]  9[45|47|49]\n',
      '_{1,1000/3,3000}2  0{5,5000/9,9000}2  ',
      '13{10,10000/11,11000/12,12000}2',
      '  15{13,13000/14,14000}20  1{15,15000/16,16000/17,17000}20  ',
      '17{21,21000/22,22000/23,23000/25,25000}20',
      '  3{27,27000/29,29000}12  5{31,31000/38,38000}12  ',
      '7{45,45000/47,47000/49,49000}12\n',
    ].join('');
    assert.equal(expected, tree.toString());
  });

  it('DuplicateKeys_ContainsKey', () => {
    const tree = insertToTree(23, true);
    for (let i = 0; i < 23; i++) {
      const key = SEQUENCE[i];
      assert.isTrue(tree.containsKey(key));
    }
    assert.isFalse(tree.containsKey(0));
    assert.isFalse(tree.containsKey(18));
    assert.isFalse(tree.containsKey(50));
  });

  it('DuplicateKeys_Get', () => {
    const tree = insertToTree(23, true);
    for (let i = 0; i < 23; i++) {
      const key = SEQUENCE[i];
      assert.sameOrderedMembers([key, key * 1000], tree.get(key));
    }
    assert.sameOrderedMembers([], tree.get(0));
    assert.sameOrderedMembers([], tree.get(18));
    assert.sameOrderedMembers([], tree.get(50));
  });

  it('DuplicateKeys_DeleteSimple', () => {
    const tree = insertToTree(9, true);
    tree.remove(13, 13);
    const expected = [
      '2[13|21]\n',
      '_{0|1|3}_\n',
      '0[3|5|9|11]  1[13|17]  3[21|25|27]\n',
      '_{3,3000/5,5000/9,9000/11,11000}2  0{13000/17,17000}2  ',
      '1{21,21000/25,25000/27,27000}2\n',
    ].join('');
    assert.equal(expected, tree.toString());
    assert.sameOrderedMembers([13000], tree.get(13));
    assert.sameOrderedMembers(
        [13000], tree.getRange([SingleKeyRange.only(13)]));
  });

  it('DuplicateKeys_DeleteAll', () => {
    const tree = insertToTree(23, true);
    for (let i = 0; i < 23; ++i) {
      tree.remove(SEQUENCE[i], SEQUENCE[i]);
      tree.remove(SEQUENCE[i], SEQUENCE[i] * 1000);
    }
    assert.equal('17[]\n_{}_\n', tree.toString());
  });

  it('DuplicateKeys_DeleteAll2', () => {
    const tree = insertToTree(23, true);
    for (let i = 22; i >= 0; --i) {
      tree.remove(SEQUENCE[i], SEQUENCE[i]);
      tree.remove(SEQUENCE[i], SEQUENCE[i] * 1000);
    }
    assert.equal('13[]\n_{}_\n', tree.toString());
  });

  it('DuplicateKeys_SmokeTest', () => {
    const tree = insertToTree(23, true);
    SEQUENCE.forEach((s) => {
      assert.equal(2, tree.cost(SingleKeyRange.only(s)));
      assert.sameOrderedMembers([s, s * 1000], tree.get(s));
      assert.sameOrderedMembers(
          [s, s * 1000], tree.getRange([SingleKeyRange.only(s)]));
    });
    assert.equal(2 * SEQUENCE.length, tree.cost(SingleKeyRange.all()));

    SEQUENCE.forEach((s) => {
      tree.remove(s, s);
      assert.equal(1, tree.cost(SingleKeyRange.only(s)));
      assert.sameOrderedMembers([s * 1000], tree.get(s));
      assert.sameOrderedMembers(
          [s * 1000], tree.getRange([SingleKeyRange.only(s)]));
    });
    assert.equal(SEQUENCE.length, tree.cost(SingleKeyRange.all()));

    tree.clear();
    assert.equal(0, tree.cost(SingleKeyRange.all()));
  });

  it('MultiKeyGet', () => {
    const comparator =
        new MultiKeyComparator(MultiKeyComparator.createOrders(2, Order.ASC));
    const tree = insertToTree2(23, comparator);
    for (let i = 0; i < 23; i++) {
      const key = SEQUENCE2[i];
      assert.sameOrderedMembers([key[0]], tree.get(key));
    }
    assert.sameOrderedMembers([], tree.get([0, '00']));
    assert.sameOrderedMembers([], tree.get([18, '18']));
    assert.sameOrderedMembers([], tree.get([50, '50']));
  });

  it('MultiKeyRandomNumbers', () => {
    resetBTreeParam();
    const ROW_COUNT = 5000;
    const set = new Set<number>();
    while (set.size < ROW_COUNT) {
      set.add(Math.floor(Math.random() * ROW_COUNT * 100));
    }

    const numbers = Array.from(set.values()).sort((a, b) => (a - b));
    const keys = numbers.map((n) => [n, -n]);

    const comparator = new MultiKeyComparator([Order.ASC, Order.DESC]);
    const tree = new BTree('test', comparator, true);
    for (let i = 0; i < ROW_COUNT; ++i) {
      tree.add(keys[i], keys[i][0]);
    }

    assert.sameOrderedMembers(numbers, tree.getRange());
    for (let i = 0; i < ROW_COUNT; ++i) {
      tree.remove(keys[i]);
    }

    assert.sameOrderedMembers([], tree.getRange());
  });

  it('MultiKeyGetRangeRegression', () => {
    const comparator =
        new MultiKeyComparator(MultiKeyComparator.createOrders(2, Order.ASC));
    const tree = new BTree('test', comparator, true);
    // clang-format off
    const data = [
      ['F', 'A'], ['F', 'B'], ['F', 'C'], ['F', 'D'],
      ['G', 'B'], ['G', 'G'], ['G', 'X'],
      ['P', 'K'], ['P', 'M'], ['P', 'P'],
      ['S', 'A'], ['S', 'B'], ['S', 'C'], ['S', 'D'],
    ];
    // clang-format on
    for (let i = 0; i < data.length; ++i) {
      tree.add(data[i], i);
    }
    const keyRange = [[
      SingleKeyRange.only('G'),
      SingleKeyRange.only('X'),
    ]];
    assert.sameOrderedMembers([6], tree.getRange(keyRange));

    const keyRange2 = [[
      SingleKeyRange.only('P'),
      SingleKeyRange.only('P'),
    ]];
    assert.sameOrderedMembers([9], tree.getRange(keyRange2));

    const keyRange3 = [[
      SingleKeyRange.lowerBound('P'),
      SingleKeyRange.upperBound('D'),
    ]];
    assert.sameOrderedMembers([10, 11, 12, 13], tree.getRange(keyRange3));
    assert.sameOrderedMembers([11, 12], tree.getRange(keyRange3, false, 2, 1));
    assert.sameOrderedMembers(
        [12, 11, 10], tree.getRange(keyRange3, true, 3, 1));

    const keyRange4 = [[
      SingleKeyRange.lowerBound('S'),
      SingleKeyRange.all(),
    ]];
    assert.sameOrderedMembers([10, 11, 12, 13], tree.getRange(keyRange4));

    assert.sameOrderedMembers(
        [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13], tree.getRange());

    const comparator2 = new MultiKeyComparator([Order.ASC, Order.DESC]);
    const tree2 = new BTree('test2', comparator2, true);
    for (let i = 0; i < data.length; ++i) {
      tree2.add(data[i], i);
    }
    assert.sameOrderedMembers([6], tree2.getRange(keyRange));
    assert.sameOrderedMembers([9], tree2.getRange(keyRange2));
    assert.sameOrderedMembers(
        [3, 2, 1, 0, 6, 5, 4, 9, 8, 7, 13, 12, 11, 10], tree2.getRange());
    assert.sameOrderedMembers(
        [1, 0, 6], tree2.getRange(undefined, false, 3, 2));
    assert.sameOrderedMembers(
        [13, 7, 8, 9], tree2.getRange(undefined, true, 4, 3));
  });

  /**
   * Tests the stats of a unique tree (each key has only one associated value).
   */
  it('Stats_UniqueTree', () => {
    const tree = insertToTree(23, false);
    let expectedMaxKeyEncountered = Math.max.apply(null, SEQUENCE);

    const stats = tree.stats();
    assert.equal(23, stats.totalRows);
    assert.equal(expectedMaxKeyEncountered, stats.maxKeyEncountered);

    // Testing deletions.
    tree.remove(9);
    tree.remove(17);
    tree.remove(21);
    assert.equal(20, stats.totalRows);

    // Testing additions.
    tree.add(9, 9);
    tree.add(21, 21);
    assert.equal(22, stats.totalRows);

    // Testing replacements.
    tree.set(9, 8);
    assert.equal(22, stats.totalRows);

    // Testing stats.maxKeyEncountered.
    tree.set(999, 888);
    assert.equal(23, stats.totalRows);
    expectedMaxKeyEncountered = 999;
    assert.equal(expectedMaxKeyEncountered, stats.maxKeyEncountered);

    // Testing removing everything from the index.
    tree.clear();
    assert.equal(0, stats.totalRows);
    assert.equal(expectedMaxKeyEncountered, stats.maxKeyEncountered);

    for (let i = 0; i < 23; ++i) {
      tree.set(i, i);
    }
    assert.equal(23, tree.stats().totalRows);

    // Test stats after serialization/deserialization.
    const rows = insertToTree(23, false).serialize();
    const deserializedTree = BTree.deserialize(c, 'dummyTree', true, rows);
    assert.equal(23, deserializedTree.stats().totalRows);
  });

  /**
   * Tests the stats of a non-unique tree where each key has two associated
   * values.
   */
  it('Stats_NonUniqueTree', () => {
    const tree = insertToTree(23, true);
    const expectedMaxKeyEncountered = Math.max.apply(null, SEQUENCE);

    const stats = tree.stats();
    assert.equal(46, stats.totalRows);
    assert.equal(expectedMaxKeyEncountered, stats.maxKeyEncountered);
    // Remove all rows for the given key.
    tree.remove(21);
    assert.equal(44, stats.totalRows);
    // Remove only one row for the given key.
    tree.remove(17, 17);
    assert.equal(43, stats.totalRows);
    tree.remove(17, 9999);  // remove non-existing row
    assert.equal(43, stats.totalRows);
    tree.set(17, 7777);
    tree.add(17, 8888);
    tree.add(17, 9999);
    assert.equal(45, stats.totalRows);
    tree.add(9, 889);
    assert.equal(46, stats.totalRows);
    tree.remove(9);
    assert.equal(43, stats.totalRows);
    assert.equal(expectedMaxKeyEncountered, stats.maxKeyEncountered);

    const rows = tree.serialize();
    const deserializedTree = BTree.deserialize(c, 'dummyTree', false, rows);
    assert.equal(43, deserializedTree.stats().totalRows);
    assert.equal(
        expectedMaxKeyEncountered, deserializedTree.stats().maxKeyEncountered);
  });

  it('GetAll', () => {
    const tree = insertToTree(23, false);
    const expected = SEQUENCE.slice(0).sort((a, b) => {
      return (a < b) ? -1 : ((a > b) ? 1 : 0);
    });
    assert.sameOrderedMembers(expected, tree.getRange());
    assert.sameOrderedMembers(
        expected.slice(2, 5), tree.getRange(undefined, false, 3, 2));
    assert.sameOrderedMembers(
        expected.slice(0, expected.length - 1).reverse(),
        tree.getRange(undefined, true, undefined, 1));

    const tree2 = new BTree('t2', c, false);
    for (let i = 1; i < 10; ++i) {
      for (let j = 0; j < 5; ++j) {
        tree2.add(i, i * 10 + j);
      }
    }

    assert.sameOrderedMembers(
        [11, 12, 13], tree2.getRange(undefined, false, 3, 1));
    assert.sameOrderedMembers(
        [14, 20, 21], tree2.getRange(undefined, false, 3, 4));
    assert.sameOrderedMembers([94], tree2.getRange(undefined, false, 10, 44));
    assert.sameOrderedMembers(
        [], tree2.getRange(undefined, false, undefined, 99));
  });

  it('Smoke_MultiNullableKey', () => {
    const comparator = new MultiKeyComparatorWithNull(
        MultiKeyComparator.createOrders(2, Order.ASC));
    const tree = insertToTree2(23, comparator);
    assert.sameOrderedMembers([1, '1'], (tree.min() as any[])[0]);
    assert.sameOrderedMembers([49, '49'], (tree.max() as any[])[0]);

    tree.set([-1, null] as any as Key, 9996);
    tree.set([null, '33'] as any as Key, 9997);
    tree.set([777, null] as any as Key, 9998);
    tree.set([null, null] as any as Key, 9999);
    assert.equal(SEQUENCE2.length + 4, tree.getRange().length);

    assert.sameOrderedMembers([-1, null], (tree.min() as any[])[0]);
    assert.sameOrderedMembers([777, null], (tree.max() as any[])[0]);

    // Serialize and deserialize should have no problem.
    const rows = tree.serialize();
    const tree2 = deserializeTree(rows);
    assert.sameOrderedMembers(tree.getRange(), tree2.getRange());
  });

  it('GetRange_MultiNullableKey', () => {
    const comparator = new MultiKeyComparatorWithNull(
        MultiKeyComparator.createOrders(2, Order.ASC));
    const tree = new BTree('test', comparator, true);
    // clang-format off
    const data = [
      ['F', 'A'], ['F', 'B'], ['F', 'C'], ['F', 'D'],
      ['G', 'B'], ['G', 'G'], ['G', 'X'],
      ['P', 'K'], ['P', 'M'], ['P', 'P'],
      ['S', 'A'], ['S', 'B'], ['S', 'C'], ['S', 'D'],
      [null, 'Z'], ['Z', null], [null, null],
    ];
    // clang-format on
    for (let i = 0; i < data.length; ++i) {
      tree.add(data[i] as any as Key, i);
    }
    const keyRange = [[
      SingleKeyRange.only('G'),
      SingleKeyRange.only('X'),
    ]];
    assert.sameOrderedMembers([6], tree.getRange(keyRange));

    const keyRange2 = [[
      SingleKeyRange.only('P'),
      SingleKeyRange.only('P'),
    ]];
    assert.sameOrderedMembers([9], tree.getRange(keyRange2));

    const keyRange3 = [[
      SingleKeyRange.lowerBound('P'),
      SingleKeyRange.upperBound('D'),
    ]];
    assert.sameOrderedMembers([10, 11, 12, 13], tree.getRange(keyRange3));
    assert.sameOrderedMembers([11, 12], tree.getRange(keyRange3, false, 2, 1));
    assert.sameOrderedMembers(
        [12, 11, 10], tree.getRange(keyRange3, true, 3, 1));

    const keyRange4 = [[
      SingleKeyRange.lowerBound('S'),
      SingleKeyRange.all(),
    ]];
    assert.sameOrderedMembers([10, 11, 12, 13, 15], tree.getRange(keyRange4));

    assert.sameOrderedMembers(
        [16, 14, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 15],
        tree.getRange());
  });

  it('GetRange_MultiKey', () => {
    const comparator =
        new MultiKeyComparator(MultiKeyComparator.createOrders(2, Order.ASC));
    const tree = new BTree('test', comparator, true);
    const tree2 = new BTree('test2', comparator, false);
    for (let i = 1; i <= 10; ++i) {
      tree.add([i, i * 10], i);
      tree2.add([i, i * 10], i);
      tree2.add([i, i * 10], i * 100);
    }
    tree.add([11, 30], 11);
    tree2.add([11, 30], 11);
    tree2.add([11, 30], 1100);

    const keyRange = [[
      SingleKeyRange.lowerBound(2, true),
      SingleKeyRange.only(30),
    ]];
    assert.sameOrderedMembers([3, 11], tree.getRange(keyRange));
    assert.sameOrderedMembers([3, 300, 11, 1100], tree2.getRange(keyRange));

    const keyRange2 = [[
      SingleKeyRange.only(11),
      SingleKeyRange.all(),
    ]];
    assert.sameOrderedMembers([11], tree.getRange(keyRange2));
    assert.sameOrderedMembers([11, 1100], tree2.getRange(keyRange2));
  });

  it('GetRange_MultiUniqueKey', () => {
    const comparator =
        new MultiKeyComparator(MultiKeyComparator.createOrders(2, Order.ASC));
    const tree = new BTree('test', comparator, true);

    for (let i = 1; i <= 3; ++i) {
      for (let j = 1; j <= 5; ++j) {
        tree.add([i, j], i * 100 + j);
      }
    }

    const all = SingleKeyRange.all();
    const only = SingleKeyRange.only(2);
    const lowerBound = SingleKeyRange.lowerBound(2);
    const lowerBoundEx = SingleKeyRange.lowerBound(2, true);
    const upperBound = SingleKeyRange.upperBound(2);
    const upperBoundEx = SingleKeyRange.upperBound(2, true);

    assert.sameOrderedMembers(
        [201, 202, 203, 204, 205], tree.getRange([[only, all]]));

    // This is a corner case: [2, 2] is the root node, and we want to test if
    // it works correctly.
    assert.sameOrderedMembers([202], tree.getRange([[only, only]]));

    assert.sameOrderedMembers(
        [202, 203, 204, 205], tree.getRange([[only, lowerBound]]));
    assert.sameOrderedMembers(
        [203, 204, 205], tree.getRange([[only, lowerBoundEx]]));
    assert.sameOrderedMembers([201, 202], tree.getRange([[only, upperBound]]));
    assert.sameOrderedMembers([201], tree.getRange([[only, upperBoundEx]]));
  });
});
