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

import {ErrorCode, Exception} from '../base/exception';

import {Database} from './database';
import {GraphNode} from './graph_node';
import {Info} from './info';
import {Pragma} from './pragma';
import {Table} from './table';
import {TableBuilder} from './table_builder';

export class Builder {
  private schema: DatabaseSchema;
  private tableBuilders: Map<string, TableBuilder>;
  private finalized: boolean;
  // private db: DBInstance;
  // private connectInProgress: boolean;

  constructor(dbName: string, dbVersion: number) {
    this.schema = new DatabaseSchema(dbName, dbVersion);
    this.tableBuilders = new Map<string, TableBuilder>();
    this.finalized = false;
  }

  public getSchema(): Database {
    if (!this.finalized) {
      this.finalize();
    }
    return this.schema;
  }

  // TODO(arthurhsu): implement getGlobal(), connect()

  public createTable(tableName: string): TableBuilder {
    if (this.tableBuilders.has(tableName)) {
      // 503: Name {0} is already defined.
      throw new Exception(ErrorCode.NAME_IN_USE, tableName);
    } else if (this.finalized) {
      // 535: Schema is already finalized.
      throw new Exception(ErrorCode.SCHEMA_FINALIZED);
    }
    this.tableBuilders.set(tableName, new TableBuilder(tableName));
    const ret = this.tableBuilders.get(tableName);
    if (!ret) {
      throw new Exception(ErrorCode.ASSERTION, 'Builder.createTable');
    }
    return ret;
  }

  public setPragma(pragma: Pragma): Builder {
    if (this.finalized) {
      // 535: Schema is already finalized.
      throw new Exception(ErrorCode.SCHEMA_FINALIZED);
    }

    this.schema._pragma = pragma;
    return this;
  }

  // Builds the graph of foreign key relationships and checks for
  // loop in the graph.
  private checkFkCycle() {
    // Builds graph.
    const nodeMap = new Map<string, GraphNode>();
    this.schema.tables().forEach((table) => {
      nodeMap.set(table.getName(), new GraphNode(table.getName()));
    }, this);
    this.tableBuilders.forEach((builder, tableName) => {
      builder.getFkSpecs().forEach((spec) => {
        const parentNode = nodeMap.get(spec.parentTable);
        if (parentNode) {
          parentNode.edges.add(tableName);
        }
      });
    });
    // Checks for cycle.
    Array.from(nodeMap.values()).forEach((graphNode) => {
      this.checkCycleUtil(graphNode, nodeMap);
    }, this);
  }

  // Performs foreign key checks like validity of names of parent and
  // child columns, matching of types and uniqueness of referred column
  // in the parent.
  private checkForeignKeyValidity(builder: TableBuilder) {
    builder.getFkSpecs().forEach((specs) => {
      const parentTableName = specs.parentTable;
      const table = this.tableBuilders.get(parentTableName);
      if (!table) {
        // 536: Foreign key {0} refers to invalid table.
        throw new Exception(ErrorCode.INVALID_FK_TABLE);
      }
      const parentSchema = table.getSchema();
      const parentColName = specs.parentColumn;
      if (!parentSchema.hasOwnProperty(parentColName)) {
        // 537: Foreign key {0} refers to invalid column.
        throw new Exception(ErrorCode.INVALID_FK_COLUMN);
      }

      const localSchema = builder.getSchema();
      const localColName = specs.childColumn;
      if (localSchema[localColName].getType() !==
          parentSchema[parentColName].getType()) {
        // 538: Foreign key {0} column type mismatch.
        throw new Exception(ErrorCode.INVALID_FK_COLUMN_TYPE, specs.name);
      }
      if (!parentSchema[parentColName].isUnique()) {
        // 539: Foreign key {0} refers to non-unique column.
        throw new Exception(ErrorCode.FK_COLUMN_NONUNIQUE, specs.name);
      }
    }, this);
  }

  // Performs checks to avoid chains of foreign keys on same column.
  private checkForiengKeyChain(builder: TableBuilder) {
    const fkSpecArray = builder.getFkSpecs();
    fkSpecArray.forEach((specs) => {
      const parentBuilder = this.tableBuilders.get(specs.parentTable);
      if (parentBuilder) {
        parentBuilder.getFkSpecs().forEach((parentSpecs) => {
          if (parentSpecs.childColumn === specs.parentColumn) {
            // 534: Foreign key {0} refers to source column of another
            // foreign key.
            throw new Exception(ErrorCode.FK_COLUMN_IN_USE, specs.name);
          }
        }, this);
      }
    }, this);
  }

  private finalize() {
    if (!this.finalized) {
      this.tableBuilders.forEach((builder, name) => {
        this.checkForeignKeyValidity(builder);
        this.schema.setTable(builder.getSchema());
      });
      Array.from(this.tableBuilders.values())
          .forEach(this.checkForiengKeyChain, this);
      this.checkFkCycle();
      this.tableBuilders.clear();
      this.finalized = true;
    }
  }

  // Checks for loop in the graph recursively. Ignores self loops.
  // This algorithm is based on Lemma 22.11 in "Introduction To Algorithms
  // 3rd Edition By Cormen et Al". It says that a directed graph G
  // can be acyclic if and only DFS of G yields no back edges.
  // @see http://www.geeksforgeeks.org/detect-cycle-in-a-graph/
  private checkCycleUtil(
      graphNode: GraphNode, nodeMap: Map<string, GraphNode>) {
    if (!graphNode.visited) {
      graphNode.visited = true;
      graphNode.onStack = true;
      graphNode.edges.forEach((edge) => {
        const childNode = nodeMap.get(edge);
        if (childNode) {
          if (!childNode.visited) {
            this.checkCycleUtil(childNode, nodeMap);
          } else if (childNode.onStack) {
            // Checks for self loop, in which case, it does not throw an
            // exception.
            if (graphNode !== childNode) {
              // 533: Foreign key loop detected.
              throw new Exception(ErrorCode.FK_LOOP);
            }
          }
        }
      }, this);
    }
    graphNode.onStack = false;
  }
}

class DatabaseSchema implements Database {
  public _pragma: Pragma;
  private _info: Info;
  private tableMap: Map<string, Table>;

  constructor(readonly _name: string, readonly _version: number) {
    this.tableMap = new Map<string, Table>();
    this._pragma = {enableBundledMode: false};
    this._info = undefined as any as Info;
  }

  public name(): string {
    return this._name;
  }

  public version(): number {
    return this._version;
  }

  public info(): Info {
    if (this._info === undefined) {
      this._info = new Info(this);
    }
    return this._info;
  }

  public tables(): Table[] {
    return Array.from(this.tableMap.values());
  }

  public table(tableName: string): Table {
    const ret = this.tableMap.get(tableName);
    if (!ret) {
      // 101: Table {0} not found.
      throw new Exception(ErrorCode.TABLE_NOT_FOUND, tableName);
    }
    return ret;
  }

  public setTable(table: Table) {
    this.tableMap.set(table.getName(), table);
  }

  public pragma(): Pragma {
    return this._pragma;
  }
}

export function createSchema(name: string, version: number): Builder {
  return new Builder(name, version);
}
