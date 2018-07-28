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
import {ErrorCode, Order, Type} from '../base/enum';
import {Exception} from '../base/exception';
import {Global} from '../base/global';
import {FnType} from '../base/private_enum';
import {Service} from '../base/service';
import {AggregatedColumn} from '../fn/aggregated_column';
import {op} from '../fn/op';
import {JoinPredicate} from '../pred/join_predicate';
import {Predicate} from '../pred/predicate';
import {BaseColumn} from '../schema/base_column';
import {BaseTable} from '../schema/base_table';
import {Column} from '../schema/column';

import {BaseBuilder} from './base_builder';
import {SelectContext} from './select_context';

export class SelectBuilder extends BaseBuilder<SelectContext> {
  private fromAlreadyCalled: boolean;
  private whereAlreadyCalled: boolean;

  constructor(global: Global, columns: Column[]) {
    super(global, new SelectContext(global.getService(Service.SCHEMA)));
    this.fromAlreadyCalled = false;
    this.whereAlreadyCalled = false;
    this.query.columns = columns as BaseColumn[];
    this.checkDistinctColumn();
    this.checkAggregations();
  }

  public assertExecPreconditions(): void {
    super.assertExecPreconditions();
    const context = this.query;
    if (context.from === undefined || context.from === null) {
      // 522: Invalid usage of select().
      throw new Exception(ErrorCode.INVALID_SELECT);
    }

    if ((context.limitBinder && context.limit === undefined) ||
        (context.skipBinder && context.skip === undefined)) {
      // 523: Binding parameters of limit/skip without providing values.
      throw new Exception(ErrorCode.UNBOUND_LIMIT_SKIP);
    }

    this.checkProjectionList();
  }

  public from(...tables: BaseTable[]): SelectBuilder {
    if (this.fromAlreadyCalled) {
      // 515: from() has already been called.
      throw new Exception(ErrorCode.DUPLICATE_FROM);
    }
    this.fromAlreadyCalled = true;

    if (this.query.from === undefined || this.query.from === null) {
      this.query.from = [];
    }

    this.query.from.push.apply(this.query.from, tables);
    return this;
  }

  public where(predicate: Predicate): SelectBuilder {
    // 548: from() has to be called before where().
    this.checkFrom(ErrorCode.FROM_AFTER_WHERE);

    if (this.whereAlreadyCalled) {
      // 516: where() has already been called.
      throw new Exception(ErrorCode.DUPLICATE_WHERE);
    }
    this.whereAlreadyCalled = true;

    this.augmentWhereClause(predicate);
    return this;
  }

  public innerJoin(table: BaseTable, predicate: Predicate): SelectBuilder {
    // 542: from() has to be called before innerJoin() or leftOuterJoin().
    this.checkFrom(ErrorCode.MISSING_FROM_BEFORE_JOIN);

    if (this.whereAlreadyCalled) {
      // 547: where() cannot be called before innerJoin() or leftOuterJoin().
      throw new Exception(ErrorCode.INVALID_WHERE);
    }

    this.query.from.push(table);
    this.augmentWhereClause(predicate);

    return this;
  }

  public leftOuterJoin(table: BaseTable, predicate: Predicate): SelectBuilder {
    // 542: from() has to be called before innerJoin() or leftOuterJoin().
    this.checkFrom(ErrorCode.MISSING_FROM_BEFORE_JOIN);

    if (!(predicate instanceof JoinPredicate)) {
      // 541: Outer join accepts only join predicate.
      throw new Exception(ErrorCode.INVALID_OUTER_JOIN);
    }
    if (this.whereAlreadyCalled) {
      // 547: where() cannot be called before innerJoin() or leftOuterJoin().
      throw new Exception(ErrorCode.INVALID_WHERE);
    }
    this.query.from.push(table);
    if (this.query.outerJoinPredicates === null ||
        this.query.outerJoinPredicates === undefined) {
      this.query.outerJoinPredicates = new Set<number>();
    }
    let normalizedPredicate = predicate;
    if (table.getEffectiveName() !==
        predicate.rightColumn.getTable().getEffectiveName()) {
      normalizedPredicate = predicate.reverse();
    }
    this.query.outerJoinPredicates.add(normalizedPredicate.getId());
    this.augmentWhereClause(normalizedPredicate);
    return this;
  }

  public limit(numberOfRows: Binder|number): SelectBuilder {
    if (this.query.limit !== undefined || this.query.limitBinder) {
      // 528: limit() has already been called.
      throw new Exception(ErrorCode.DUPLICATE_LIMIT);
    }
    if (numberOfRows instanceof Binder) {
      this.query.limitBinder = numberOfRows;
    } else {
      if (numberOfRows < 0) {
        // 531: Number of rows must not be negative for limit/skip.
        throw new Exception(ErrorCode.NEGATIVE_LIMIT_SKIP);
      }
      this.query.limit = numberOfRows;
    }
    return this;
  }

  public skip(numberOfRows: Binder|number): SelectBuilder {
    if (this.query.skip !== undefined || this.query.skipBinder) {
      // 529: skip() has already been called.
      throw new Exception(ErrorCode.DUPLICATE_SKIP);
    }
    if (numberOfRows instanceof Binder) {
      this.query.skipBinder = numberOfRows;
    } else {
      if (numberOfRows < 0) {
        // 531: Number of rows must not be negative for limit/skip.
        throw new Exception(ErrorCode.NEGATIVE_LIMIT_SKIP);
      }
      this.query.skip = numberOfRows;
    }
    return this;
  }

  public orderBy(column: BaseColumn, order?: Order): SelectBuilder {
    // 549: from() has to be called before orderBy() or groupBy().
    this.checkFrom(ErrorCode.FROM_AFTER_ORDER_GROUPBY);

    if (this.query.orderBy === undefined) {
      this.query.orderBy = [];
    }

    this.query.orderBy.push({
      column: column,
      order: (order !== undefined && order !== null) ? order : Order.ASC,
    });
    return this;
  }

  public groupBy(...columns: BaseColumn[]): SelectBuilder {
    // 549: from() has to be called before orderBy() or groupBy().
    this.checkFrom(ErrorCode.FROM_AFTER_ORDER_GROUPBY);

    if (this.query.groupBy) {
      // 530: groupBy() has already been called.
      throw new Exception(ErrorCode.DUPLICATE_GROUPBY);
    }
    if (this.query.groupBy === undefined) {
      this.query.groupBy = [];
    }

    this.query.groupBy.push.apply(this.query.groupBy, columns);
    return this;
  }

  // Provides a clone of this select builder. This is useful when the user needs
  // to observe the same query with different parameter bindings.
  public clone(): SelectBuilder {
    const builder = new SelectBuilder(this.global, this.query.columns);
    builder.query = this.query.clone();
    builder.query.clonedFrom = null;  // The two builders are not related.
    return builder;
  }

  // Checks that usage of lf.fn.distinct() is correct. Specifically if an
  // lf.fn.distinct() column is requested, then it can't be combined with any
  // other column.
  private checkDistinctColumn(): void {
    const distinctColumns = this.query.columns.filter(
        (column) => (column instanceof AggregatedColumn) &&
            column.aggregatorType === FnType.DISTINCT);

    const isValidCombination = distinctColumns.length === 0 ||
        (distinctColumns.length === 1 && this.query.columns.length === 1);

    if (!isValidCombination) {
      // 524: Invalid usage of lf.fn.distinct().
      throw new Exception(ErrorCode.INVALID_DISTINCT);
    }
  }

  // Checks that the combination of projection list is valid.
  // Specifically:
  // 1) If GROUP_BY is specified: grouped columns must be indexable.
  // 2) If GROUP_BY is not specified: Aggregate and non-aggregated columns can't
  //    be mixed (result does not make sense).
  private checkProjectionList(): void {
    (this.query.groupBy) ? this.checkGroupByColumns() :
                           this.checkProjectionListNotMixed();
  }

  // Checks that grouped columns are indexable.
  private checkGroupByColumns(): void {
    const isInvalid = this.query.groupBy.some((column) => {
      const type = column.getType();
      return (type === Type.OBJECT || type === Type.ARRAY_BUFFER);
    });

    if (isInvalid) {
      // 525: Invalid projection list or groupBy columns.
      throw new Exception(ErrorCode.INVALID_GROUPBY);
    }
  }

  // Checks that the projection list contains either only non-aggregated
  // columns, or only aggregated columns. See checkProjectionList_ for details.
  private checkProjectionListNotMixed(): void {
    const aggregatedColumnsExist =
        this.query.columns.some((column) => column instanceof AggregatedColumn);
    const nonAggregatedColumnsExist =
        this.query.columns.some(
            (column) => !(column instanceof AggregatedColumn)) ||
        this.query.columns.length === 0;

    if (aggregatedColumnsExist && nonAggregatedColumnsExist) {
      // 526: Invalid projection list: mixing aggregated with non-aggregated
      throw new Exception(ErrorCode.INVALID_PROJECTION);
    }
  }

  // Checks that the specified aggregations are valid, in terms of aggregation
  // type and column type.
  private checkAggregations(): void {
    this.query.columns.forEach((column) => {
      const isValidAggregation = !(column instanceof AggregatedColumn) ||
          this.isAggregationValid(column.aggregatorType, column.getType());

      if (!isValidAggregation) {
        // 527: Invalid aggregation detected: {0}.
        throw new Exception(
            ErrorCode.INVALID_AGGREGATION, column.getNormalizedName());
      }
    }, this);
  }

  // Checks if from() has already called.
  private checkFrom(code: ErrorCode): void {
    if (this.query.from === undefined || this.query.from === null) {
      throw new Exception(code);
    }
  }

  // Augments the where clause by ANDing it with the given predicate.
  private augmentWhereClause(predicate: Predicate): void {
    if (this.query.where) {
      const newPredicate = op.and(predicate, this.query.where);
      this.query.where = newPredicate;
    } else {
      this.query.where = predicate;
    }
  }

  // Checks whether the user specified aggregations are valid.
  private isAggregationValid(aggregatorType: FnType, columnType: Type):
      boolean {
    switch (aggregatorType) {
      case FnType.COUNT:
      case FnType.DISTINCT:
        return true;
      case FnType.AVG:
      case FnType.GEOMEAN:
      case FnType.STDDEV:
      case FnType.SUM:
        return columnType === Type.NUMBER || columnType === Type.INTEGER;
      case FnType.MAX:
      case FnType.MIN:
        return columnType === Type.NUMBER || columnType === Type.INTEGER ||
            columnType === Type.STRING || columnType === Type.DATE_TIME;
      default:
        // NOT REACHED
    }
    return false;
  }
}
