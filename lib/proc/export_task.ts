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

import { TransactionType } from '../base/enum';
import { Global } from '../base/global';
import { TaskPriority } from '../base/private_enum';
import { Resolver } from '../base/resolver';
import { PayloadType, Row } from '../base/row';
import { Service } from '../base/service';
import { UniqueId } from '../base/unique_id';
import { RuntimeIndex } from '../index/runtime_index';
import { BaseTable } from '../schema/base_table';
import { DatabaseSchema } from '../schema/database_schema';
import { Table } from '../schema/table';

import { Relation } from './relation';
import { RelationEntry } from './relation_entry';
import { Task } from './task';

export class ExportTask extends UniqueId implements Task {
  private schema: DatabaseSchema;
  private scope: Set<Table>;
  private resolver: Resolver<Relation[]>;

  constructor(private global: Global) {
    super();
    this.schema = global.getService(Service.SCHEMA);
    this.scope = new Set<Table>(this.schema.tables());
    this.resolver = new Resolver<Relation[]>();
  }

  // Grabs contents from the cache and exports them as a plain object.
  execSync(): PayloadType {
    const indexStore = this.global.getService(Service.INDEX_STORE);
    const cache = this.global.getService(Service.CACHE);

    const tables: PayloadType = {};
    (this.schema.tables() as BaseTable[]).forEach(table => {
      const rowIds = (indexStore.get(
        table.getRowIdIndexName()
      ) as RuntimeIndex).getRange();
      const payloads = cache.getMany(rowIds).map(row => (row as Row).payload());
      tables[table.getName()] = payloads;
    });

    return {
      name: this.schema.name(),
      tables,
      version: this.schema.version(),
    };
  }

  exec(): Promise<Relation[]> {
    const results = this.execSync();
    const entry = new RelationEntry(new Row(Row.DUMMY_ID, results), true);

    return Promise.resolve([new Relation([entry], [])]);
  }

  getType(): TransactionType {
    return TransactionType.READ_ONLY;
  }

  getScope(): Set<Table> {
    return this.scope;
  }

  getResolver(): Resolver<Relation[]> {
    return this.resolver;
  }

  getId(): number {
    return this.getUniqueNumber();
  }

  getPriority(): TaskPriority {
    return TaskPriority.EXPORT_TASK;
  }
}
