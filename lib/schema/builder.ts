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
    // TODO(arthurhsu): implement
  }

  // Performs foreign key checks like validity of names of parent and
  // child columns, matching of types and uniqueness of referred column
  // in the parent.
  private checkForeignKeyValidity(builder: TableBuilder) {
    // TODO(arthurhsu): implement
  }

  // Performs checks to avoid chains of foreign keys on same column.
  private checkForiengKeyChain(builder: TableBuilder) {
    // TODO(arthurhsu): implement
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
  // private checkCycleUtil(
  //     graphNode: GraphNode, nodeMap: Map<string, GraphNode>) {
  // TODO(arthurhsu): implement
  // }
}

// A class that represents a vertex in the graph of foreign keys relationships.
/*
class GraphNode {
  public visited: boolean;
  public onStack: boolean;
  public edges: Set<GraphNode>;
  readonly name: string;

  constructor(tableName: string) {
    this.name = tableName;
    this.visited = false;
    this.onStack = false;
    this.edges = new Set<GraphNode>();
  }
}
*/

class DatabaseSchema implements Database {
  public readonly _info: Info;
  public _pragma: Pragma;
  private tableMap: Map<string, Table>;

  constructor(readonly _name: string, readonly _version: number) {
    this.tableMap = new Map<string, Table>();
    this._pragma = {enableBundledMode: false};
    this._info = new Info(this);
  }

  public name(): string {
    return this._name;
  }

  public version(): number {
    return this._version;
  }

  public info(): Info {
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
