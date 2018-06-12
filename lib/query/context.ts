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
import {Predicate} from '../pred/predicate';
import {PredicateNode} from '../pred/predicate_node';
import {ValuePredicate} from '../pred/value_predicate';
import {Database} from '../schema/database';
import {Table} from '../schema/table';

// Base context for all query types.
export abstract class Context {
  // Creates predicateMap such that predicates can be located by ID.
  private static buildPredicateMap(rootPredicate: PredicateNode):
      Map<number, Predicate> {
    const predicateMap = new Map<number, Predicate>();
    rootPredicate.traverse((n) => {
      const node = n as PredicateNode as Predicate;
      predicateMap.set(node.getId(), node);
      return true;
    });
    return predicateMap;
  }

  public schema: Database;
  public where: Predicate|null;
  public clonedFrom: Context|null;

  // A map used for locating predicates by ID. Instantiated lazily.
  private predicateMap: Map<number, Predicate>;

  constructor(schema: Database) {
    this.schema = schema;
    this.clonedFrom = null;
    this.where = null;
    this.predicateMap = null as any as Map<number, Predicate>;
  }

  public getPredicate(id: number): Predicate {
    if (this.predicateMap === null && this.where !== null) {
      this.predicateMap =
          Context.buildPredicateMap(this.where as PredicateNode);
    }
    const predicate: Predicate = this.predicateMap.get(id) as Predicate;
    assert(predicate !== undefined);
    return predicate;
  }

  public bind(values: any[]): Context {
    assert(this.clonedFrom === null);
    return this;
  }

  public bindValuesInSearchCondition(values: any[]): void {
    const searchCondition: PredicateNode = this.where as PredicateNode;
    if (searchCondition !== null) {
      searchCondition.traverse((node) => {
        if (node instanceof ValuePredicate) {
          node.bind(values);
        }
        return true;
      });
    }
  }

  public abstract getScope(): Set<Table>;
  public abstract clone(): Context;

  protected cloneBase(context: Context): void {
    if (context.where) {
      this.where = context.where.copy();
    }
    this.clonedFrom = context;
  }
}
