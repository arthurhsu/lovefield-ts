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
import {ErrorCode} from '../base/enum';
import {Exception} from '../base/exception';
import {Global} from '../base/global';
import {Service} from '../base/service';
import {ServiceId} from '../base/service_id';
import {RuntimeDatabase} from '../proc/runtime_database';

import {BaseColumn} from './base_column';
import {ConnectOptions} from './connect_options';
import {DatabaseSchema} from './database_schema';
import {DatabaseSchemaImpl} from './database_schema_impl';
import {GraphNode} from './graph_node';
import {Pragma} from './pragma';
import {TableBuilder} from './table_builder';
import {Builder} from './builder';
import {BaseTable} from './base_table';
import {IndexImpl} from './index_impl';

export class SchemaBuilder implements Builder {
  private schema: DatabaseSchemaImpl;
  private tableBuilders: Map<string, TableBuilder>;
  private finalized: boolean;
  private db: RuntimeDatabase;
  private connectInProgress: boolean;

  constructor(dbName: string, dbVersion: number) {
    this.schema = new DatabaseSchemaImpl(dbName, dbVersion);
    this.tableBuilders = new Map<string, TableBuilder>();
    this.finalized = false;
    this.db = null as unknown as RuntimeDatabase;
    this.connectInProgress = false;
  }

  serialize(): object {
    const schemaInstance = this.getSchema();
    return {
      name: schemaInstance.name(),
      version: schemaInstance.version(),
      tables: schemaInstance.tables().map((t) => {
        const table = t as BaseTable;
        return {
          columns: table.getColumns().map((col) => {
            return {
              name: col.getName(),
              nullable: col.isNullable(),
              type: col.getType(),
              unique: col.isUnique(),
            };
          }),
          constraints: {
            foreignKeys: table
              .getConstraint()
              .getForeignKeys()
              .map((fk) => {
                return {
                  action: fk.action,
                  local: fk.childColumn,
                  name: fk.name,
                  ref: `${fk.parentTable}.${fk.parentColumn}`,
                  timing: fk.timing,
                };
              }),
            notNullable: table
              .getConstraint()
              .getNotNullable()
              .map((col) => col.getName()),
            primaryKey: table.getConstraint().getPrimaryKey()
              ? {
                  columns: table
                    .getConstraint()
                    .getPrimaryKey()
                    .columns.map((ic) => {
                      return {
                        autoIncrement: ic.autoIncrement,
                        name: ic.schema.getName(),
                        order: ic.order,
                      };
                    }),
                  name: table.getConstraint().getPrimaryKey().name,
                }
              : null,
          },
          indices: table.getIndices().map((index) => {
            const indexImpl = index as IndexImpl;
            return {
              columns: indexImpl.columns.map((ic) => {
                return {
                  autoIncrement: ic.autoIncrement || false,
                  name: ic.schema.getName(),
                  order: ic.order,
                };
              }),
              name: indexImpl.name,
              unique: indexImpl.isUnique,
            };
          }),
          name: table.getName(),
          persistentIndex: table.persistentIndex(),
        };
      }),
    };
  }

  deserialize(data: object): void {
    const schemaData = data as any;
    schemaData.tables.forEach((tableData: any) => {
      const tableBuilder = this.createTable(tableData.name);
      tableData.columns.forEach((col: any) => {
        tableBuilder.addColumn(col.name, col.type);
      });

      if (tableData.constraints) {
        if (tableData.constraints.primaryKey) {
          tableBuilder.addPrimaryKey(
            tableData.constraints.primaryKey.columns,
            tableData.constraints.primaryKey.columns.some(
              (c: any) => c.autoIncrement
            )
          );
        }
        if (tableData.constraints.notNullable) {
          tableBuilder.addNullable(
            tableData.columns
              .filter(
                (c: any) => !tableData.constraints.notNullable.includes(c.name)
              )
              .map((c: any) => c.name)
          );
        }
        // Restore uniqueness for columns that are unique but not part of PK.
        tableData.columns.forEach((col: any) => {
          if (col.unique) {
            // Check if it is already unique via PK.
            const isPk =
              tableData.constraints.primaryKey &&
              tableData.constraints.primaryKey.columns.length === 1 &&
              tableData.constraints.primaryKey.columns[0].name === col.name;
            if (!isPk) {
              // We need to find the unique index name that covers only this
              // column.
              const uniqueIndex = tableData.indices.find(
                (idx: any) =>
                  idx.unique &&
                  idx.columns.length === 1 &&
                  idx.columns[0].name === col.name
              );
              if (uniqueIndex) {
                tableBuilder.addUnique(uniqueIndex.name, [col.name]);
              }
            }
          }
        });

        if (tableData.constraints.foreignKeys) {
          tableData.constraints.foreignKeys.forEach((fk: any) => {
            const name = fk.name.substring(fk.name.lastIndexOf('.') + 1);
            tableBuilder.addForeignKey(name, {
              action: fk.action,
              local: fk.local,
              ref: fk.ref,
              timing: fk.timing,
            });
          });
        }
      }

      if (tableData.indices) {
        tableData.indices.forEach((index: any) => {
          // Primary key is also an index, but it's already added.
          if (
            tableData.constraints &&
            tableData.constraints.primaryKey &&
            index.name === tableData.constraints.primaryKey.name
          ) {
            return;
          }
          // Foreign keys are also indices, but they are already added.
          if (
            tableData.constraints &&
            tableData.constraints.foreignKeys &&
            tableData.constraints.foreignKeys.some((fk: any) =>
              fk.name.endsWith('.' + index.name)
            )
          ) {
            return;
          }
          // Unique constraints are also indices, but they are already added.
          // We only add non-unique indices or multi-column unique indices here
          // if they weren't added via addUnique.
          // Wait, addUnique actually adds the index.
          // If the index is unique and was already added via addUnique, we
          // should skip it.
          if (index.unique && index.columns.length === 1) {
            // This was likely handled by addUnique logic above.
            // Let's check if it was indeed unique and not PK.
            const col = tableData.columns.find(
              (c: any) => c.name === index.columns[0].name
            );
            if (col && col.unique) {
              return;
            }
          }

          tableBuilder.addIndex(index.name, index.columns, index.unique);
        });
      }

      if (tableData.persistentIndex) {
        tableBuilder.persistentIndex(tableData.persistentIndex);
      }
    });
  }

  getSchema(): DatabaseSchema {
    if (!this.finalized) {
      this.finalize();
    }
    return this.schema;
  }

  getGlobal(): Global {
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
  connect(options?: ConnectOptions): Promise<DatabaseConnection> {
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
      }
    );
  }

  createTable(tableName: string): TableBuilder {
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

  setPragma(pragma: Pragma): Builder {
    if (this.finalized) {
      // 535: Schema is already finalized.
      throw new Exception(ErrorCode.SCHEMA_FINALIZED);
    }

    this.schema._pragma = pragma;
    return this;
  }

  // Builds the graph of foreign key relationships and checks for
  // loop in the graph.
  private checkFkCycle(): void {
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
    Array.from(nodeMap.values()).forEach((graphNode) =>
      this.checkCycleUtil(graphNode, nodeMap)
    );
  }

  // Performs foreign key checks like validity of names of parent and
  // child columns, matching of types and uniqueness of referred column
  // in the parent.
  private checkForeignKeyValidity(builder: TableBuilder): void {
    builder.getFkSpecs().forEach((specs) => {
      const parentTableName = specs.parentTable;
      const table = this.tableBuilders.get(parentTableName);
      if (!table) {
        // 536: Foreign key {0} refers to invalid table.
        throw new Exception(ErrorCode.INVALID_FK_TABLE);
      }
      const parentSchema = table.getSchema();
      const parentColName = specs.parentColumn;
      if (!Object.prototype.hasOwnProperty.call(parentSchema, parentColName)) {
        // 537: Foreign key {0} refers to invalid column.
        throw new Exception(ErrorCode.INVALID_FK_COLUMN);
      }

      const localSchema = builder.getSchema();
      const localColName = specs.childColumn;
      if (
        (localSchema[localColName] as BaseColumn).getType() !==
        (parentSchema[parentColName] as BaseColumn).getType()
      ) {
        // 538: Foreign key {0} column type mismatch.
        throw new Exception(ErrorCode.INVALID_FK_COLUMN_TYPE, specs.name);
      }
      if (!(parentSchema[parentColName] as BaseColumn).isUnique()) {
        // 539: Foreign key {0} refers to non-unique column.
        throw new Exception(ErrorCode.FK_COLUMN_NONUNIQUE, specs.name);
      }
    }, this);
  }

  // Performs checks to avoid chains of foreign keys on same column.
  private checkForeignKeyChain(builder: TableBuilder): void {
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

  private finalize(): void {
    if (!this.finalized) {
      this.tableBuilders.forEach((builder) => {
        this.checkForeignKeyValidity(builder);
        this.schema.setTable(builder.getSchema());
      });
      Array.from(this.tableBuilders.values()).forEach(
        this.checkForeignKeyChain,
        this
      );
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
    graphNode: GraphNode,
    nodeMap: Map<string, GraphNode>
  ): void {
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
