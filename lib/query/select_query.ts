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

import {Binder} from '../base/bind';
import {Order} from '../base/enum';
import {Predicate} from '../pred/predicate';
import {BaseColumn} from '../schema/base_column';
import {Table} from '../schema/table';
import {QueryBuilder} from './query_builder';

// Query Builder which constructs a SELECT query. The builder is stateful.
// All member functions, except orderBy(), can only be called once. Otherwise
// an exception will be thrown.
export interface SelectQuery extends QueryBuilder {
  // Specifies the source of the SELECT query.
  from(...tables: Table[]): SelectQuery;

  // Defines search condition of the SELECT query.
  where(predicate: Predicate): SelectQuery;

  // Explicit inner join target table with specified search condition.
  innerJoin(table: Table, predicate: Predicate): SelectQuery;

  // Explicit left outer join target table with specified search condition.
  leftOuterJoin(table: Table, predicate: Predicate): SelectQuery;

  // Limits the number of rows returned in select results. If there are fewer
  // rows than limit, all rows will be returned.
  limit(numberOfRows: number|Binder): SelectQuery;

  // Skips the number of rows returned in select results from the beginning. If
  // there are fewer rows than skip, no row will be returned.
  skip(numberOfRows: number|Binder): SelectQuery;

  // Specify sorting order of returned results.
  orderBy(column: BaseColumn, order?: Order): SelectQuery;

  // Specify grouping of returned results.
  groupBy(...columns: BaseColumn[]): SelectQuery;
}
