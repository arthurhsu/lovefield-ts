/**
 * @license
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

import {Row} from '../base/row';

/**
 * Each RelationEntry represents a row that is passed from one execution step
 * to another and does not necessarily correspond to a physical row in a DB
 * table (as it can be the result of a cross-product/join operation).
 */
export class RelationEntry {
  // The ID to assign to the next entry that will be created.
  private static nextId = 0;

  private static getNextId() {
    return RelationEntry.nextId++;
  }

  public id: number;
  // private isPrefixApplied: boolean;

  // |isPrefixApplied| Whether the payload in this entry is using prefixes for
  // each attribute. This happens when this entry is the result of a relation
  // join.
  constructor(readonly row: Row, isPrefixApplied: boolean) {
    this.id = RelationEntry.getNextId();
    // this.isPrefixApplied = isPrefixApplied;
  }

  // TODO(arthurhsu): port the rest
}
