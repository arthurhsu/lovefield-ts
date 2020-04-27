/**
 * Copyright 2020 The Lovefield Project Authors. All Rights Reserved.
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

// This is a simple demo showing how to use Lovefield in a web page.
// Error handlings are deliberately omitted to make the flow clear.

import * as lf from './node_modules/lovefield-ts/dist/es6/lf.js';

class TodoDemo {
  createDatabase() {
    const schemaBuilder = lf.schema.create('todo', 1);

    schemaBuilder
      .createTable('Item')
      .addColumn('id', lf.Type.INTEGER)
      .addColumn('description', lf.Type.STRING)
      .addColumn('deadline', lf.Type.DATE_TIME)
      .addColumn('done', lf.Type.BOOLEAN)
      .addPrimaryKey(['id'])
      .addIndex('idxDeadline', ['deadline'], false, lf.Order.DESC);

    return schemaBuilder.connect({
      storeType: lf.DataStoreType.MEMORY
    });
  }

  fetchData() {
    const parseXHRResult = xhr => {
      if (xhr.status === 200) {
        // OK
        return JSON.parse(xhr.responseText);
      }
      throw `ERROR: Received: ${xhr.responseText}`;
    };

    return new Promise((resolve, reject) => {
      const xhr = new XMLHttpRequest();
      xhr.open('GET', 'demo_data.json', true);
      xhr.onreadystatechange = () => {
        if (xhr.readyState === 4) {
          resolve(parseXHRResult(xhr));
        }
      };
      xhr.error = reject;
      xhr.send();
    });
  }

  insertData(data) {
    this.item = this.db.getSchema().table('Item');
    const rows = data.map(d =>
      this.item.createRow({
        id: d.id,
        description: d.description,
        deadline: new Date(d.deadline),
        done: d.done
      })
    );
    return this.db.insertOrReplace().into(this.item).values(rows).exec();
  }

  selectTodoItems() {
    return this.db
      .select()
      .from(this.item)
      .where(this.item.col('done').eq(false))
      .orderBy(this.item.col('deadline'))
      .exec();
  }

  serve() {
    this.createDatabase().then(db => {
      this.db = db;
      return this.fetchData();
    }).then(data => {
      return this.insertData(data);
    }).then(() => {
      return this.selectTodoItems();
    }).then(todoItems => {
      const dl = document.getElementById('data');
      let innerHTML = '';
      todoItems.forEach(t => {
        innerHTML +=
          `<dt>${t.description}</dt>` +
          `<dd>${t.deadline.toLocaleString()}</dd>\n`
      });
      dl.innerHTML = innerHTML;
    });
  }
}

window.onload = () => {
  new TodoDemo().serve();
};
