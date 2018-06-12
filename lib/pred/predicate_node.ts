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

import {Relation} from '../proc/relation';
import {Column} from '../schema/column';
import {Table} from '../schema/table';
import {TreeNode} from '../structs/tree_node';
import {Predicate} from './predicate';

export abstract class PredicateNode extends TreeNode implements Predicate {
  // The ID to assign to the next predicate that will be created. Note that
  // predicates are constructed with unique IDs, but when a predicate is cloned
  //  the ID is also purposefully cloned.
  private static nextId = 0;

  private id: number;

  constructor() {
    super();
    this.id = PredicateNode.nextId++;
  }

  public abstract eval(relation: Relation): Relation;
  public abstract setComplement(isComplement: boolean): void;
  public abstract copy(): Predicate;
  public abstract getColumns(results?: Column[]): Column[];
  public abstract getTables(results?: Set<Table>): Set<Table>;

  public setId(id: number): void {
    this.id = id;
  }

  public getId(): number {
    return this.id;
  }
}
