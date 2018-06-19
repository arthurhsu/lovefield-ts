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
import {ExecType} from '../base/private_enum';
import {Journal} from '../cache/journal';
import {Context} from '../query/context';
import {TreeNode} from '../structs/tree_node';
import {Relation} from './relation';

export abstract class PhysicalQueryPlanNode extends TreeNode {
  public static ANY = -1;

  private numRelations: number;
  private execType: ExecType;

  constructor(numRelations: number, type: ExecType) {
    super();
    this.execType = type;
    this.numRelations = numRelations;
  }

  // Core logic of this node.
  // Length of |relations| is guaranteed to be consistent with
  // |this.numRelations|.
  public abstract execInternal(
      relations: Relation[], journal?: Journal, context?: Context): Relation[];

  public exec(journal?: Journal, context?: Context): Promise<Relation[]> {
    switch (this.execType) {
      case ExecType.FIRST_CHILD:
        return this.execFirstChild(journal, context);

      case ExecType.ALL:
        return this.execAllChildren(journal, context);

      default:  // NO_CHILD
        return this.execNoChild(journal, context);
    }
  }

  public toString(): string {
    return 'dummy_node';
  }

  // Returns a string representation of this node taking into account the given
  // context.
  public toContextString(context: Context): string {
    return this.toString();
  }

  private assertInput(relations: Relation[]): void {
    assert(
        this.numRelations === PhysicalQueryPlanNode.ANY ||
        relations.length === this.numRelations);
  }

  private execNoChild(journal?: Journal, context?: Context):
      Promise<Relation[]> {
    return new Promise<Relation[]>((resolve, reject) => {
      resolve(this.execInternal([], journal, context));
    });
  }

  private execFirstChild(journal?: Journal, context?: Context):
      Promise<Relation[]> {
    return (this.getChildAt(0) as PhysicalQueryPlanNode)
        .exec(journal, context)
        .then((results) => {
          this.assertInput(results);
          return this.execInternal(results, journal, context);
        });
  }

  private execAllChildren(journal?: Journal, context?: Context):
      Promise<Relation[]> {
    const promises = this.getChildren().map((child) => {
      return (child as PhysicalQueryPlanNode).exec(journal, context);
    });
    return Promise.all<Relation[]>(promises).then((results) => {
      const relations: Relation[] = [];
      results.forEach((result) => {
        result.forEach((res) => relations.push(res));
      });
      this.assertInput(relations);
      return this.execInternal(relations, journal, context);
    });
  }
}
