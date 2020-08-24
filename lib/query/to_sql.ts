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
import {EvalType} from '../base/eval';
import {Exception} from '../base/exception';
import {Operator} from '../base/private_enum';
import {Row} from '../base/row';
import {CombinedPredicate} from '../pred/combined_predicate';
import {JoinPredicate} from '../pred/join_predicate';
import {Predicate} from '../pred/predicate';
import {PredicateNode} from '../pred/predicate_node';
import {ValuePredicate} from '../pred/value_predicate';
import {BaseColumn} from '../schema/base_column';
import {BaseTable} from '../schema/base_table';
import {Table} from '../schema/table';
import {TreeHelper} from '../structs/tree_helper';
import {TreeNode} from '../structs/tree_node';

import {BaseBuilder} from './base_builder';
import {Context} from './context';
import {DeleteContext} from './delete_context';
import {InsertContext} from './insert_context';
import {SelectContext} from './select_context';
import {UpdateContext} from './update_context';

type V = boolean | number | string | Date | ArrayBuffer;

export class SqlHelper {
  static toSql(builder: BaseBuilder<Context>, stripValueInfo = false): string {
    const query = builder.getQuery();

    if (query instanceof InsertContext) {
      return SqlHelper.insertToSql(query, stripValueInfo);
    }

    if (query instanceof DeleteContext) {
      return SqlHelper.deleteToSql(query, stripValueInfo);
    }

    if (query instanceof UpdateContext) {
      return SqlHelper.updateToSql(query, stripValueInfo);
    }

    if (query instanceof SelectContext) {
      return SqlHelper.selectToSql(query, stripValueInfo);
    }

    // 358: toSql() is not implemented for {0}.
    throw new Exception(ErrorCode.NOT_IMPL_IN_TOSQL, typeof query);
  }

  private static escapeSqlValue(type: Type, val: unknown): string | number {
    const value = val as V;
    if (value === undefined || value === null) {
      return 'NULL';
    }

    switch (type) {
      case Type.BOOLEAN:
        return value ? 1 : 0;

      case Type.INTEGER:
      case Type.NUMBER:
        return value as number;

      case Type.ARRAY_BUFFER:
        // Note: Oracle format is used here.
        return `'${Row.binToHex(value as ArrayBuffer)}'`;

      default:
        // datetime, string
        return `'${value.toString()}'`;
    }
  }

  private static insertToSql(
    query: InsertContext,
    stripValueInfo: boolean
  ): string {
    let prefix = query.allowReplace ? 'INSERT OR REPLACE' : 'INSERT';
    const columns = (query.into as BaseTable).getColumns();
    prefix += ' INTO ' + query.into.getName() + '(';
    prefix += columns.map(col => col.getName()).join(', ');
    prefix += ') VALUES (';
    const sqls = query.values.map(row => {
      const values = columns.map(col => {
        const rawVal = row.payload()[col.getName()];
        return stripValueInfo
          ? rawVal !== undefined && rawVal !== null
            ? '#'
            : 'NULL'
          : SqlHelper.escapeSqlValue(col.getType(), rawVal);
      });
      return prefix + values.join(', ') + ');';
    });
    return sqls.join('\n');
  }

  private static evaluatorToSql(op: EvalType): string {
    switch (op) {
      case EvalType.BETWEEN:
        return 'BETWEEN';
      case EvalType.EQ:
        return '=';
      case EvalType.GTE:
        return '>=';
      case EvalType.GT:
        return '>';
      case EvalType.IN:
        return 'IN';
      case EvalType.LTE:
        return '<=';
      case EvalType.LT:
        return '<';
      case EvalType.MATCH:
        return 'LIKE';
      case EvalType.NEQ:
        return '<>';
      default:
        return 'UNKNOWN';
    }
  }

  private static valueToSql(
    value: unknown,
    op: EvalType,
    type: Type,
    stripValueInfo: boolean
  ): string {
    if (value instanceof Binder) {
      return '?' + value.getIndex().toString();
    }

    if (stripValueInfo) {
      return value !== undefined && value !== null ? '#' : 'NULL';
    } else if (op === EvalType.MATCH) {
      return `'${(value as V).toString()}'`;
    } else if (op === EvalType.IN) {
      const array = value as V[];
      const vals = array.map(e => SqlHelper.escapeSqlValue(type, e));
      return `(${vals.join(', ')})`;
    } else if (op === EvalType.BETWEEN) {
      return (
        SqlHelper.escapeSqlValue(type, (value as unknown[])[0]) +
        ' AND ' +
        SqlHelper.escapeSqlValue(type, (value as unknown[])[1])
      );
    }

    return SqlHelper.escapeSqlValue(type, value).toString();
  }

  private static valuePredicateToSql(
    pred: ValuePredicate,
    stripValueInfo: boolean
  ): string {
    const column = pred.column.getNormalizedName();
    const op = SqlHelper.evaluatorToSql(pred.evaluatorType);
    const value = SqlHelper.valueToSql(
      pred.peek(),
      pred.evaluatorType,
      pred.column.getType(),
      stripValueInfo
    );
    if (op === '=' && value === 'NULL') {
      return [column, 'IS NULL'].join(' ');
    } else if (op === '<>' && value === 'NULL') {
      return [column, 'IS NOT NULL'].join(' ');
    } else {
      return [column, op, value].join(' ');
    }
  }

  private static combinedPredicateToSql(
    pred: CombinedPredicate,
    stripValueInfo: boolean
  ): string {
    const children = pred.getChildren().map(childNode => {
      return (
        '(' +
        SqlHelper.parseSearchCondition(
          childNode as PredicateNode,
          stripValueInfo
        ) +
        ')'
      );
    });
    const joinToken = pred.operator === Operator.AND ? ' AND ' : ' OR ';
    return children.join(joinToken);
  }

  private static joinPredicateToSql(pred: JoinPredicate): string {
    return [
      pred.leftColumn.getNormalizedName(),
      SqlHelper.evaluatorToSql(pred.evaluatorType),
      pred.rightColumn.getNormalizedName(),
    ].join(' ');
  }

  private static parseSearchCondition(
    pred: Predicate,
    stripValueInfo: boolean
  ): string {
    if (pred instanceof ValuePredicate) {
      return SqlHelper.valuePredicateToSql(pred, stripValueInfo);
    } else if (pred instanceof CombinedPredicate) {
      return SqlHelper.combinedPredicateToSql(pred, stripValueInfo);
    } else if (pred instanceof JoinPredicate) {
      return SqlHelper.joinPredicateToSql(pred);
    }

    // 357: toSql() does not support predicate type: {0}.
    throw new Exception(357, typeof pred);
  }

  private static predicateToSql(
    pred: Predicate,
    stripValueInfo: boolean
  ): string {
    const whereClause = SqlHelper.parseSearchCondition(pred, stripValueInfo);
    if (whereClause) {
      return ' WHERE ' + whereClause;
    }
    return '';
  }

  private static deleteToSql(
    query: DeleteContext,
    stripValueInfo: boolean
  ): string {
    let sql = 'DELETE FROM ' + query.from.getName();
    if (query.where) {
      sql += SqlHelper.predicateToSql(query.where, stripValueInfo);
    }
    sql += ';';
    return sql;
  }

  private static updateToSql(
    query: UpdateContext,
    stripValueInfo: boolean
  ): string {
    let sql = 'UPDATE ' + query.table.getName() + ' SET ';
    sql += query.set
      .map(set => {
        const c = set.column as BaseColumn;
        const setter = c.getNormalizedName() + ' = ';
        if (set.binding !== -1) {
          return setter + '?' + (set.binding as number).toString();
        }
        return (
          setter + SqlHelper.escapeSqlValue(c.getType(), set.value).toString()
        );
      })
      .join(', ');
    if (query.where) {
      sql += SqlHelper.predicateToSql(query.where, stripValueInfo);
    }
    sql += ';';
    return sql;
  }

  private static selectToSql(
    query: SelectContext,
    stripValueInfo: boolean
  ): string {
    let colList = '*';
    if (query.columns.length) {
      colList = query.columns
        .map(c => {
          const col = c as BaseColumn;
          if (col.getAlias()) {
            return col.getNormalizedName() + ' AS ' + col.getAlias();
          } else {
            return col.getNormalizedName();
          }
        })
        .join(', ');
    }

    let sql = 'SELECT ' + colList + ' FROM ';
    if (query.outerJoinPredicates && query.outerJoinPredicates.size !== 0) {
      sql += SqlHelper.getFromListForOuterJoin(query, stripValueInfo);
    } else {
      sql += SqlHelper.getFromListForInnerJoin(query, stripValueInfo);
      if (query.where) {
        sql += SqlHelper.predicateToSql(query.where, stripValueInfo);
      }
    }

    if (query.orderBy) {
      const orderBy = query.orderBy
        .map(order => {
          return (
            order.column.getNormalizedName() +
            (order.order === Order.DESC ? ' DESC' : ' ASC')
          );
        })
        .join(', ');
      sql += ' ORDER BY ' + orderBy;
    }

    if (query.groupBy) {
      const groupBy = query.groupBy
        .map(col => col.getNormalizedName())
        .join(', ');
      sql += ' GROUP BY ' + groupBy;
    }

    if (query.limit) {
      sql += ' LIMIT ' + query.limit.toString();
    }

    if (query.skip) {
      sql += ' SKIP ' + query.skip.toString();
    }

    sql += ';';
    return sql;
  }

  private static getTableNameToSql(t: Table): string {
    const table = t as BaseTable;
    return table.getEffectiveName() !== table.getName()
      ? table.getName() + ' AS ' + table.getEffectiveName()
      : table.getName();
  }

  // Handles Sql queries that have left outer join.
  private static getFromListForOuterJoin(
    query: SelectContext,
    stripValueInfo: boolean
  ): string {
    // Retrieves all JoinPredicates.
    const retrievedNodes = TreeHelper.find(
      query.where as PredicateNode,
      (n: TreeNode) => n instanceof JoinPredicate
    ) as PredicateNode[];
    const predicateString = retrievedNodes.map((n: PredicateNode) =>
      SqlHelper.joinPredicateToSql(n as JoinPredicate)
    );

    let fromList = SqlHelper.getTableNameToSql(query.from[0]);
    for (let i = 1; i < query.from.length; i++) {
      const fromName = SqlHelper.getTableNameToSql(query.from[i]);
      if (
        query.outerJoinPredicates.has(
          retrievedNodes[predicateString.length - i].getId()
        )
      ) {
        fromList += ' LEFT OUTER JOIN ' + fromName;
      } else {
        fromList += ' INNER JOIN ' + fromName;
      }
      fromList += ' ON (' + predicateString[predicateString.length - i] + ')';
    }

    const node = query.where as PredicateNode;
    const leftChild = node.getChildCount() > 0 ? node.getChildAt(0) : node;

    // The following condition checks that where has been called in the query.
    if (!(leftChild instanceof JoinPredicate)) {
      fromList +=
        ' WHERE ' +
        SqlHelper.parseSearchCondition(
          leftChild as PredicateNode,
          stripValueInfo
        );
    }
    return fromList;
  }

  private static getFromListForInnerJoin(
    query: SelectContext,
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    stripValueInfo: boolean
  ): string {
    return query.from.map(SqlHelper.getTableNameToSql).join(', ');
  }
}
