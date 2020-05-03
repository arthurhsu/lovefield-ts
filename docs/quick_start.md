# Quick start guide for Lovefield-TS

## For JavaScript users

The demo can be found at `demo/js`. Let's start with `index.html`:

```html
<!-- index.html -->
<html>
  <body>
    <dl id="data"></dl>
    <script type="module" src="todo_demo.js"></script>
  </body>
</html>
```

Lovefield-TS assumes ES6 module by default, therefore the script needs to be
loaded using `<script type="module" src="...">`. This demo tries to load the
TODO data from `demo_data.json` on the server, store them in browser-side
database, then render it onto the `data` element.

The `todo_demo.js` is structured into the following blocks:

```javascript
// todo_demo.js
import * as lf from './node_modules/lovefield-ts/dist/es6/lf.js';

class TodoDemo {
  createDatabase() {
    // Defines database schema.
  }

  fetchData() {
    // Retrieve demo_data.json from server using XHR.
  }

  insertData() {
    // Insert retrieved data into browser database.
  }

  selectTodoItems() {
    // Select data from database.
  }

  serve() {
    // Main logic controlling the execution and rendering
  }
}

window.onload = () => {
  // window.onload event hook.
}
```

Lovefield is a relational database. Relational database treats each entry of
data as *row*. Inside the row there are data fields called *columns*. Data
corresponding to a specific column always has the same data type.

When the rows of the same columns are grouped together, they are called *table*.
One can think that this is the sheet in any spreadsheet program. The definition
of how a table "looked like" is called *schema*. A schema defines:

* All columns in a table, i.e. their *name* and *type*
* Constraints of a table
  * Can a column be null (a.k.a nullable)?
  * Will all values in this column be unique (a.k.a. uniqueness)?
* Indices of a table to accelerate search

A *database* is a collection of tables. So, a *database schema* means the
collection of all table schemas belonging to this database.

In this demo, our database is defined as following:

```javascript
// Creates a schema builder for database named 'todo'.
//
// SQL equivalent:
// CREATE DATABASE IF NOT EXISTS todo
const schemaBuilder = lf.schema.create('todo', 1);

// Creates a table and define the table schema.
//
// SQL equivalent:
// CREATE TABLE IF NOT EXISTS Item (
//   id AS INTEGER,
//   description AS INTEGER,
//   deadline as DATE_TIME,
//   done as BOOLEAN,
//   PRIMARY KEY ON ('id')
// );
// ALTER TABLE Item ADD INDEX idxDeadLine(Item.deadline DESC);
schemaBuilder
  .createTable('Item')
  .addColumn('id', lf.Type.INTEGER)
  .addColumn('description', lf.Type.STRING)
  .addColumn('deadline', lf.Type.DATE_TIME)
  .addColumn('done', lf.Type.BOOLEAN)
  .addPrimaryKey(['id'])
  .addIndex('idxDeadline', ['deadline'], false, lf.Order.DESC);
```

The code above defines a database named `todo` and it has one table `Item`.
Inside `Item`, each row will have four fields `id`, `description`, `deadline`,
and `done`. There are two indices for this table:

* Primary key on column `id`.
* Additional index on column `deadline`.

A *primary key* means that this field is *unique* and serves as the main index
of this table. This implies

* Every row will have a unique value for this column.
* Using this column to perform search in a table will be fast.

Once the schema is defined, the database still needs to be *instantiated* to
start storing data and accept *queries*. This is done by invoking the `connect`
method of the builder:

```javascript
// Instantiate the database instance, promise-based.
return schemaBuilder.connect({
  storeType: lf.DataStoreType.MEMORY
});
```

From this point on, the schema cannot be altered. Almost all Lovefield APIs are
asynchronous Promise-based APIs except specific ones. This design is to prevent
Lovefield from blocking main thread since the queries can be long running and
demanding quite some CPU and I/O cycles.

If the database is brand new, Lovefield will create it using the schema. If the
database already exists, Lovefield will attempt to identify the instance using
database name specified in the schema, and *connect* to it, that's why this
method is named `connect()`.

Currently the schema persistence is not implemented yet, therefore you *MUST*
define schema every time before being able to connect to an existing database.
The schema *MUST be exactly the same* as the persisted database.

After getting some data from the server, let's persist it into the database:

```javascript
// Retrieve the table object.
this.item = this.db.getSchema().table('Item');

// Create rows to be inserted into database.
const rows = data.map(d =>
  this.item.createRow({
    id: d.id,
    description: d.description,
    deadline: new Date(d.deadline),
    done: d.done
  })
);

// Insert data into the database, and replace existing rows if they exist.
//
// SQL equivalent (using SQLite dialects):
// INSERT OR REPLACE INTO Item VALUES(?)
// bind the values data.length times
return this.db.insertOrReplace().into(this.item).values(rows).exec();
```

Once data is persisted in the database, it's time to use `select` queries to
shape the data into the desired output:

```javascript
// List all undone TODO items, and sort them by date (expiring ones on top)
//
// SQL equivalent:
// SELECT FROM Item WHERE done = FALSE ORDER BY deadline
this.db
  .select()
  .from(this.item)
  .where(this.item.col('done').eq(false))
  .orderBy(this.item.col('deadline'))
  .exec();
```

In this simple example, the returned *rows* are actually array of JavaScript
objects, within each object there are fields matching the column name. Therefore
you can consume them directly like this:

```javascript
const dl = document.getElementById('data');
let innerHTML = '';
todoItems.forEach(t => {
  /*
    t looks like:
    {
      id: number,           // lf.Type.INTEGER
      description: string,  // lf.Type.STRING
      deadline: Date,       // lf.Type.DATE_TIME
      done: boolean         // lf.Type.BOOLEAN
    }
  */
  innerHTML +=
    `<dt>${t.description}</dt>` +
    `<dd>${t.deadline.toLocaleString()}</dd>\n`
});
dl.innerHTML = innerHTML;
```

Once the browser session ends, the database is automatically closed, therefore
don't worry about it. If you insist, you can call `db.close()` to close it.
However, due to HTML5 spec limitations, the `close()` operation is best efforts,
i.e. no guarantee to run.

> Technical details: spec said `IDBDatabase.close()` will be run if and only if
> all references to that database are gone. BTW, there's no way for you to know
> that in JavaScript. Given Lovefield-TS still uses IndexedDB as a transactional
> store, it is also suffering from that limitation.

## ES5 users

If you are constrained by not able to use ES6 modules, you're not out of luck
yet. Here's a quick hack to still use Lovefield-TS in your projects:

```html
<body>
  <!-- The hack to make CommonJS module system work -->
  <script type="text/javascript">
    var exports = {};
  </script>
  <script type="text/javascript" src="./node_modules/lovefield-ts/dist/es5/lf.js">
  </script>
  <!-- your script continues here -->
</body>
```

## Node.js users

The demo can be found at `demo/node`. Given Node.js does not provide
transactional storage yet, all Lovefield databases are *in-memory only*.

To start using Lovefield-TS, run the `npm` command:

```shell
npm install lovefield-ts
```

To use Lovefield-TS in your node.js code, just do this:

```javascript
const lf = require('lovefield-ts');
```

Note that majority of Lovefield APIs are promise-based, which means they can be
wrapped in `async` functions and use `await` syntax:

```javascript
async function createDatabase() { ... }
async function insertData(db) { ... }
async function selectTodoItems(db) { ... }

async function main() {
  const db = await createDatabase();
  await insertData(db);
  const res = await selectTodoItems(db);
  ...
}
```

And that's it for the quick start. Happy coding and hope Lovefield will ease
your pain of manipulating data (a little bit).
