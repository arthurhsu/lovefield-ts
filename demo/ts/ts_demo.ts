/**
 * Copyright 2026 The Lovefield Project Authors. All Rights Reserved.
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

// This is a simple demo showing how to use Lovefield in TypeScript.

import * as lf from 'lovefield-ts';

interface RawDataType {
  id: number;
  description: string;
  deadline: number;
  done: boolean;
}

interface DataType {
  id: number;
  description: string;
  deadline: Date;
  done: boolean;
}

class TodoDemo {
  private db!: lf.DatabaseConnection;
  private item!: lf.Table;

  constructor() {}

  private async createDatabase(): Promise<lf.DatabaseConnection> {
    const schemaBuilder = lf.schema.create('todo', 1);

    schemaBuilder
      .createTable('Item')
      .addColumn('id', lf.Type.INTEGER)
      .addColumn('description', lf.Type.STRING)
      .addColumn('deadline', lf.Type.DATE_TIME)
      .addColumn('done', lf.Type.BOOLEAN)
      .addPrimaryKey(['id'])
      .addIndex('idxDeadline', ['deadline'], false, lf.Order.DESC);

    return schemaBuilder.connect();
  }

  private async fetchData(): Promise<RawDataType[]> {
    const response = await fetch('ts_demo.json');
    if (!response.ok) {
      throw new Error(`Failed to fetch data: ${response.statusText}`);
    }
    return (await response.json()) as RawDataType[];
  }

  private async insertData(data: RawDataType[]): Promise<void> {
    this.item = this.db.getSchema().table('Item');
    const rows = data.map((d) =>
      this.item.createRow({
        id: d.id,
        description: d.description,
        deadline: new Date(d.deadline),
        done: d.done,
      })
    );
    await this.db.insertOrReplace().into(this.item).values(rows).exec();
  }

  private async selectTodoItems(): Promise<DataType[]> {
    return (await this.db
      .select()
      .from(this.item)
      .where(this.item.col('done').eq(false))
      .orderBy(this.item.col('deadline'))
      .exec()) as DataType[];
  }

  async serve() {
    try {
      this.db = await this.createDatabase();
      const data = await this.fetchData();
      await this.insertData(data);
      const todoItems = await this.selectTodoItems();

      const dl = document.getElementById('data');
      if (dl) {
        let innerHTML = '';
        todoItems.forEach((t) => {
          innerHTML +=
            `<dt>${t.description}</dt>` +
            `<dd>${t.deadline.toLocaleString()}</dd>\n`;
        });
        dl.innerHTML = innerHTML;
      }
    } catch (e) {
      console.error('An error occurred during TodoDemo execution:', e);
    }
  }
}

window.addEventListener('DOMContentLoaded', () => {
  new TodoDemo().serve();
});
