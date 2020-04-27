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

// This is a simple demo showing how to use Lovefield in node.js.
// Error handlings are deliberately omitted to make the flow clear.

const fs = require('fs');
const lf = require('lovefield-ts');

async function createDatabase() {
  const schemaBuilder = lf.schema.create('todo', 1);

  schemaBuilder
    .createTable('Item')
    .addColumn('id', lf.Type.INTEGER)
    .addColumn('description', lf.Type.STRING)
    .addColumn('deadline', lf.Type.DATE_TIME)
    .addColumn('done', lf.Type.BOOLEAN)
    .addPrimaryKey(['id'])
    .addIndex('idxDeadline', ['deadline'], false, lf.Order.DESC);

  // In node.js, the options are defaulted to memory-DB only. No persistence.
  return schemaBuilder.connect();
}

async function insertData(db) {
  const data = JSON.parse(fs.readFileSync('node_demo.json'));
  const item = db.getSchema().table('Item');
  const rows = data.map(d =>
    item.createRow({
      id: d.id,
      description: d.description,
      deadline: new Date(d.deadline),
      done: d.done,
    })
  );
  return db.insertOrReplace().into(item).values(rows).exec();
}

async function selectTodoItems(db) {
  const item = db.getSchema().table('Item');
  return db
    .select()
    .from(item)
    .where(item.done.eq(false))
    .orderBy(item.deadline)
    .exec();
}

async function main() {
  const db = await createDatabase();
  await insertData(db);
  const res = await selectTodoItems(db);
  res.forEach(r => {
    console.log(
      'Finish [',
      r.description,
      '] before',
      r.deadline.toLocaleString()
    );
  });
}

main();
