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

import * as lf from './node_modules/lovefield-ts/dist/es6/lf.js';
import {getHrDbSchemaBuilder} from './hr_schema_no_fk.js';

export class DefaultBenchmark {
  constructor(volatile) {
    this.volatile = volatile || false;
  }

  async init() {
    const options = {
      storeType: this.volatile ?
          lf.DataStoreType.MEMORY : lf.DataStoreType.INDEXED_DB,
      enableInspector: true,
    };

    this.db = await getHrDbSchemaBuilder().connect(options);
    this.e = this.db.getSchema().table('Employee');
    return Promise.resolve();
  }

  async close(skipDeletion) {
    const skip = skipDeletion || false;
    if (skip) {
      this.db.close();
      return Promise.resolve();
    }

    await this.db.delete().from(this.e).exec();
    this.db.close();
    return Promise.resolve();
  }

  async generateTestData() {
    if (this.data !== undefined && this.data !== null) {
      return Promise.resolve();
    }

    const generator = new EmployeeDataGenerator(this.db.getSchema());
    this.data = generator.generate(50000);
    this.data.forEach((row, i) => {
      const id = ('000000' + i.toString()).slice(-6);
      row.setId(id);
    });

    for (let i = 10000; i < 20000; ++i) {
      this.data[i].setSalary(30000 + i);
    }
    return Promise.resolve();
  }

  async loadTestData(filename) {
    const parseXHRResult = (xhr) => {
      if (xhr.status === 200) {
        // OK
        const rawData = JSON.parse(xhr.responseText);
        this.data = rawData.map((obj) => {
          return this.e.deserializeRow({
            'id': window.top['#lfRowId'](),
            'value': obj,
          });
        });
        return;
      }
      throw new Error(`ERROR: Received: ${xhr.responseText}`);
    };

    return new Promise((resolve, reject) => {
      const xhr = new XMLHttpRequest();
      xhr.open('GET', filename, true);
      xhr.onreadystatechange = () => {
        if (xhr.readyState === 4) {
          resolve(parseXHRResult(xhr));
        }
      };
      xhr.error = reject;
      xhr.send();
    });
  }

  async insert(rowCount) {
    const data = this.data.slice(0, rowCount);
    return this.db.insert().into(this.e).values(data).exec();
  }

  async select() {
    return this.db.
        select().
        from(this.e).
        orderBy(this.e.id, lf.Order.ASC).
        exec();
  }

  async updateAll() {
    return this.db.update(this.e).set(this.e.salary, 50000).exec();
  }

  async deleteAll() {
    return this.db.delete().from(this.e).exec();
  }

  getRangeEnd(rowCount) {
    const id = 10000 + rowCount - 1;
    return ('000000' + id.toString()).slice(-6);
  }

  async deletePartial(rowCount) {
    if (rowCount == 1) {
      return this.db.
          delete().from(this.e).where(this.e.id.eq('010000')).exec();
    } else {
      return this.db.
          delete().
          from(this.e).
          where(this.e.id.between('010000', this.getRangeEnd(rowCount))).
          exec();
    }
  }

  async insertPartial(rowCount) {
    const data = this.data.slice(10000, 10000 + rowCount);
    return this.db.insert().into(this.e).values(data).exec();
  }

  async updatePartial(rowCount) {
    if (rowCount == 1) {
      return this.db.
          update(this.e).set(this.e.salary, 50000).where(
              this.e.id.eq('010000')).exec();
    } else {
      return this.db.
          update(this.e).set(this.e.salary, 50000).where(
              this.e.id.between('010000', this.getRangeEnd(rowCount))).exec();
    }
  }

  async selectPartial(rowCount) {
    if (rowCount == 1) {
      return this.db.
          select().from(this.e).where(this.e.id.eq('010000')).exec();
    } else {
      return this.db.
          select().from(this.e).where(
              this.e.id.between('010000', this.getRangeEnd(rowCount))).exec();
    }
  }

  async validateInsert(rowCount) {
    return this.select().then((results) => {
      if (results.length != rowCount) {
        return false;
      }

      const data = this.data.slice(0, rowCount);
      return data.every(function(expected, index) {
        const row = results[index];
        const payload = expected.payload();
        return payload['id'] == row['id'] &&
            payload['firstName'] == row['firstName'] &&
            payload['lastName'] == row['lastName'] &&
            payload['salary'] == row['salary'];
      });
    });
  }

  async validateUpdateAll(rowCount) {
    return this.select().then((results) => {
      if (results.length != rowCount) {
        return false;
      }

      const data = this.data.slice(0, rowCount);
      return data.every(function(expected, index) {
        const row = results[index];
        const payload = expected.payload();
        return payload['id'] == row['id'] &&
            payload['firstName'] == row['firstName'] &&
            payload['lastName'] == row['lastName'] &&
            50000 == row['salary'];
      });
    });
  }

  async validateEmpty() {
    return this.select().then((results) => {
      return results.length == 0;
    });
  }

  async validateDeletePartial(rowCount) {
    return this.selectPartial(rowCount).then((rows) => {
      return rows.length == 0;
    });
  }

  async validateUpdatePartial(rowCount, rows) {
    if (rowCount != rows.length) {
      return Promise.resolve(false);
    }

    const data = this.data.slice(10000, 10000 + rowCount);
    const validated = data.every((expected, index) => {
      const row = rows[index];
      const payload = expected.payload();
      return payload['id'] == row['id'] &&
          payload['firstName'] == row['firstName'] &&
          payload['lastName'] == row['lastName'] &&
          50000 == row['salary'];
    });
    return Promise.resolve(validated);
  }
}
