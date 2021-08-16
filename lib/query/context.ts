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
import {UniqueId} from '../base/unique_id';
import {Predicate} from '../pred/predicate';
import {PredicateNode} from '../pred/predicate_node';
import {ValuePredicate} from '../pred/value_predicate';
import {Table} from '../schema/table';
import {DatabaseSchema} from '../schema/database_schema';

// Base context for all query types.
export abstract class Context extends UniqueId {
  // Creates predicateMap such that predicates can be located by ID.
  private static buildPredicateMap(
    rootPredicate: PredicateNode
  ): Map<number, Predicate> {
    const predicateMap = new Map<number, Predicate>();
    rootPredicate.traverse(n => {
      const node = n as PredicateNode as Predicate;
      predicateMap.set(node.getId(), node);
    });
    return predicateMap;
  }

  where: Predicate | null;
  clonedFrom: Context | null;

  // A map used for locating predicates by ID. Instantiated lazily.
  private predicateMap: Map<number, Predicate>;

  constructor(public schema: DatabaseSchema) {
    super();
    this.clonedFrom = null;
    this.where = null;
    this.predicateMap = null as unknown as Map<number, Predicate>;
  }

  getPredicate(id: number): Predicate {
    if (this.predicateMap === null && this.where !== null) {
      this.predicateMap = Context.buildPredicateMap(
        this.where as PredicateNode
      );
    }
    const predicate: Predicate = this.predicateMap.get(id) as Predicate;
    assert(predicate !== undefined);
    return predicate;
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  bind(values: unknown[]): Context {
    assert(this.clonedFrom === null);
    return this;
  }

  bindValuesInSearchCondition(values: unknown[]): void {
    const searchCondition: PredicateNode = this.where as PredicateNode;
    if (searchCondition !== null) {
      searchCondition.traverse(node => {
        if (node instanceof ValuePredicate) {
          node.bind(values);
        }
      });
    }
  }

  abstract getScope(): Set<Table>;
  abstract clone(): Context;

  protected cloneBase(context: Context): void {
    if (context.where) {
      this.where = context.where.copy();
    }
    this.clonedFrom = context;
  }
}
