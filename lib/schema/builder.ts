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

import {DatabaseConnection} from '../base/database_connection';
import {ErrorCode, Exception} from '../base/exception';
import {Global} from '../base/global';
import {Service} from '../base/service';
import {ServiceId} from '../base/service_id';
import {RuntimeDatabase} from '../proc/runtime_database';

import {ConnectOptions} from './connect_options';
import {Database} from './database';
import {DatabaseSchema} from './database_schema';
import {GraphNode} from './graph_node';
import {Pragma} from './pragma';
import {TableBuilder} from './table_builder';

export class Builder {
  private schema: DatabaseSchema;
  private tableBuilders: Map<string, TableBuilder>;
  private finalized: boolean;
  private db: RuntimeDatabase;
  private connectInProgress: boolean;

  constructor(dbName: string, dbVersion: number) {
    this.schema = new DatabaseSchema(dbName, dbVersion);
    this.tableBuilders = new Map<string, TableBuilder>();
    this.finalized = false;
    this.db = null as any as RuntimeDatabase;
    this.connectInProgress = false;
  }

  public getSchema(): Database {
    if (!this.finalized) {
      this.finalize();
    }
    return this.schema;
  }

  public getGlobal(): Global {
    const namespaceGlobalId = new ServiceId<Global>(`ns_${this.schema.name()}`);
    const global = Global.get();
    let namespacedGlobal: Global;
    if (!global.isRegistered(namespaceGlobalId)) {
      namespacedGlobal = new Global();
      global.registerService(namespaceGlobalId, namespacedGlobal);
    } else {
      namespacedGlobal = global.getService(namespaceGlobalId);
    }
    return namespacedGlobal;
  }

  // Instantiates a connection to the database. Note: This method can only be
  // called once per Builder instance. Subsequent calls will throw an error,
  // unless the previous DB connection has been closed first.
  public connect(options: ConnectOptions): Promise<DatabaseConnection> {
    if (this.connectInProgress || (this.db !== null && this.db.isOpen())) {
      // 113: Attempt to connect() to an already connected/connecting database.
      throw new Exception(ErrorCode.ALREADY_CONNECTED);
    }
    this.connectInProgress = true;

    if (this.db === null) {
      const global = this.getGlobal();
      if (!global.isRegistered(Service.SCHEMA)) {
        global.registerService(Service.SCHEMA, this.getSchema());
      }
      this.db = new RuntimeDatabase(global);
    }

    return this.db.init(options).then(
        (db) => {
          this.connectInProgress = false;
          return db;
        },
        (e) => {
          this.connectInProgress = false;
          // TODO(arthurhsu): Add a new test case to verify that failed init
          // call allows the database to be deleted since we close it properly
          // here.
          this.db.close();
          throw e;
        });
  }

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
    Array.from(nodeMap.values())
        .forEach((graphNode) => this.checkCycleUtil(graphNode, nodeMap));
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

export function createSchema(name: string, version: number): Builder {
  return new Builder(name, version);
}
