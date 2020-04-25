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

import {TransactionType} from '../base/enum';
import {TaskPriority} from '../base/private_enum';
import {Resolver} from '../base/resolver';
import {Table} from '../schema/table';
import {Relation} from './relation';

export interface Task {
  exec(): Promise<Relation[]>;
  getType(): TransactionType;

  // Returns the tables that this task refers to.
  getScope(): Set<Table>;
  getResolver(): Resolver<Relation[]>;

  // Returns a unique number for this task.
  getId(): number;

  // Returns the priority of this task.
  getPriority(): TaskPriority;
}
