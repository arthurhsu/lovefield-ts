/**
 * Copyright 2018 The Lovefield Project Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {Type} from '../lib/base/enum';
import {Row} from '../lib/base/row';
import {Database} from '../lib/schema/database';
import {schema} from '../lib/schema/schema';

// A helper class for generating sample database rows for tables with
// nullable columns and also ground truth data for the generated rows.
export class NullableDataGenerator {
  public sampleTableARows: Row[];
  public sampleTableBRows: Row[];
  public sampleTableCRows: Row[];
  public tableAGroundTruth: TableAGroundTruth;
  public schema: Database;

  constructor() {
    const schemaBuilder = schema.create('NullableSchema', 1);
    schemaBuilder.createTable('TableA')
        .addColumn('id', Type.INTEGER)
        .addNullable(['id']);
    schemaBuilder.createTable('TableB')
        .addColumn('id', Type.INTEGER)
        .addNullable(['id']);
    schemaBuilder.createTable('TableC')
        .addColumn('id', Type.INTEGER)
        .addNullable(['id']);
    this.schema = schemaBuilder.getSchema();
    this.sampleTableARows = [];
    this.sampleTableBRows = [];
    this.sampleTableCRows = [];
    this.tableAGroundTruth = {
      avgDistinctId: 3,
      geomeanDistinctId: 2.6051710846973517,
      numNullable: 2,
      stddevDistinctId: 1.5811388300841898,
      sumDistinctId: 15,
    };
  }

  public generate(): void {
    this.generateTableA();
    this.generateTableB();
    this.generateTableC();
  }

  private generateTableA(): void {
    const tableA = this.schema.table('TableA');
    const nonNullCount = 5;
    for (let i = 0; i < nonNullCount; i++) {
      this.sampleTableARows.push(tableA.createRow({id: i + 1}));
    }
    for (let i = 0; i < this.tableAGroundTruth.numNullable; i++) {
      this.sampleTableARows.push(tableA.createRow({id: null}));
    }
  }

  private generateTableB(): void {
    const tableB = this.schema.table('TableB');
    for (let i = 0; i < 2; i++) {
      this.sampleTableBRows.push(tableB.createRow({id: null}));
    }
  }

  // Generates sample rows for TableC. This has identical id values with
  // TableA.id. Intended to be used for testing joins with TableA.
  private generateTableC(): void {
    const tableC = this.schema.table('TableC');
    const nonNullCount = 5;
    for (let i = 0; i < nonNullCount; i++) {
      this.sampleTableCRows.push(tableC.createRow({id: i + 1}));
    }
    for (let i = 0; i < this.tableAGroundTruth.numNullable; i++) {
      this.sampleTableCRows.push(tableC.createRow({id: null}));
    }
  }
}

interface TableAGroundTruth {
  numNullable: number;
  avgDistinctId: number;
  sumDistinctId: number;
  geomeanDistinctId: number;
  stddevDistinctId: number;
}
