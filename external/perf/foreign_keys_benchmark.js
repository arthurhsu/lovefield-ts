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
import {TestCase} from './test_case.js';

export class ForeignKeysBenchmark {
  constructor(db, constraintTiming) {
    this.db = db;
    this.constraintTiming = constraintTiming;
    this.parent = this.db.getSchema().table('Parent');
    this.child = this.db.getSchema().table('Child');

    const sampleRows = this.generateSampleData();
    this.parentsWithChildren = sampleRows[0];
    this.parentsWithoutChildren = sampleRows[1];
    this.parents = this.parentsWithChildren.concat(this.parentsWithoutChildren);
    this.updatedParents = this.parents.map((row) => {
      return this.parent.createRow({
        id: row.payload()['id'],
        name: `updatedName${row.payload()['id']}`,
      });
    });
    this.children = sampleRows[2];
  }

  generateSampleData() {
    const parentWithChildrenCount = 10000;
    const parentWithoutChildrenCount = 50000;
    const childRowCount = 60000;

    const parentWithChildrenRows = new Array(parentWithChildrenCount);
    for (let i = 0; i < parentWithChildrenRows.length; i++) {
      parentWithChildrenRows[i] = this.parent.createRow({
        id: i,
        name: `name${i}`,
      });
    }

    const parentWithoutChildrenRows = new Array(parentWithoutChildrenCount);
    for (let i = 0; i < parentWithoutChildrenRows.length; i++) {
      parentWithoutChildrenRows[i] = this.parent.createRow({
        id: parentWithChildrenRows.length + i,
        name: `name${i}`,
      });
    }

    const childRows = new Array(childRowCount);
    for (let i = 0; i < childRows.length; i++) {
      childRows[i] = this.child.createRow({
        id: i,
        parentId: i % parentWithChildrenCount,
        name: `name${i}`,
      });
    }

    return [parentWithChildrenRows, parentWithoutChildrenRows, childRows];
  }

  async insertParent() {
    return this.db
        .insert()
        .into(this.parent)
        .values(this.parents)
        .exec();
  }

  async validateInsertParent() {
    const results = await this.db
        .select()
        .from(this.parent)
        .exec();
    return Promise.resolve(results.length == this.parents.length);
  }

  async insertChild() {
    return this.db
        .insert()
        .into(this.child)
        .values(this.children)
        .exec();
  }

  async validateInsertChild() {
    const results = await this.db
        .select()
        .from(this.child)
        .exec();
    return Promise.resolve(results.length == this.children.length);
  }

  async updateParent() {
    return this.db
        .insertOrReplace()
        .into(this.parent)
        .values(this.updatedParents)
        .exec();
  }

  async validateUpdateParent() {
    const results = await this.db
        .select()
        .from(this.parent)
        .exec();
    const validated = results.length == this.parents.length &&
        results.every((obj) => obj['name'].indexOf('updatedName') != -1);
    return Promise.resolve(validated);
  }

  async updateChild() {
    const targetIndex = Math.floor(this.parentsWithChildren.length / 2);
    const targetParentId =
        this.parentsWithChildren[targetIndex].payload()['id'];
    const targetChildId = Math.floor(this.children.length / 2);

    // Updating all children that have parentId greater than a given id.
    return this.db
        .update(this.child)
        .set(this.child['parentId'], targetParentId)
        .where(this.child['id'].gt(targetChildId))
        .exec();
  }

  async validateUpdateChild() {
    const targetIndex = Math.floor(this.parentsWithChildren.length / 2);
    const targetParentId =
        this.parentsWithChildren[targetIndex].payload()['id'];

    const results = await this.db
        .select()
        .from(this.child)
        .where(this.child['parentId'].gt(targetParentId))
        .exec();

    const validated = results.length > 0 &&
        results.every((obj) => obj['parentId'] > targetParentId);
    return Promise.resolve(validated);
  }

  async deleteParent() {
    // Finding first parent without children row.
    const targetId = this.parentsWithoutChildren[0].payload()['id'];

    return this.db
        .delete()
        .from(this.parent)
        .where(this.parent['id'].gte(targetId))
        .exec();
  }

  async validateDeleteParent() {
    const targetId = this.parentsWithoutChildren[0].payload()['id'];

    const results = await this.db
        .select()
        .from(this.parent)
        .where(this.parent['id'].gte(targetId))
        .exec();
    return Promise.resolve(results.length == 0);
  }

  async deleteChild() {
    return this.db.delete().from(this.child).exec();
  }

  async validateDeleteChild() {
    const results = await this.db.select().from(this.child).exec();
    return Promise.resolve(results.length == 0);
  }

  async tearDown() {
    await this.db.delete().from(this.child).exec();
    return this.db.delete().from(this.parent).exec();
  }

  getTestCases() {
    const suffix = ForeignKeysBenchmark.getSuffix(this.constraintTiming);
    return [
      new TestCase(
          'InsertParent_' + this.parent.length + suffix,
          this.insertParent.bind(this),
          this.validateInsertParent.bind(this)),
      new TestCase(
          'InsertChild_' + this.children.length + suffix,
          this.insertChild.bind(this),
          this.validateInsertChild.bind(this)),
      new TestCase(
          'UpdateParent_' + this.parent.length + suffix,
          this.updateParent.bind(this),
          this.validateUpdateParent.bind(this)),
      new TestCase(
          'UpdateChild_' + this.children.length + suffix,
          this.updateChild.bind(this),
          this.validateUpdateChild.bind(this)),
      new TestCase(
          'DeleteParent_' + this.parentsWithoutChildren.length + suffix,
          this.deleteParent.bind(this),
          this.validateDeleteParent.bind(this)),
      new TestCase(
          'DeleteChild_' + this.children.length + suffix,
          this.deleteChild.bind(this),
          this.validateDeleteChild.bind(this)),
      new TestCase(
          'TearDown' + suffix,
          this.tearDown.bind(this), undefined, true),
    ];
  }

  static getSuffix(constraintTiming) {
    if (constraintTiming == lf.ConstraintTiming.IMMEDIATE) {
      return '_immediate';
    } else if (constraintTiming == lf.ConstraintTiming.DEFERRABLE) {
      return '_deferrable';
    } else {
      return '_noFK';
    }
  }

  static getSchemaBuilder(constraintTiming) {
    const suffix = ForeignKeysBenchmark.getSuffix(constraintTiming);

    const schemaBuilder = lf.schema.create(`fk_bench${suffix}`, 1);
    schemaBuilder
        .createTable('Parent')
        .addColumn('id', lf.Type.INTEGER)
        .addColumn('name', lf.Type.STRING)
        .addPrimaryKey(['id']);

    const childBuilder = schemaBuilder
        .createTable('Child')
        .addColumn('id', lf.Type.INTEGER)
        .addColumn('parentId', lf.Type.INTEGER)
        .addColumn('name', lf.Type.STRING)
        .addPrimaryKey(['id']);

    if (constraintTiming !== null) {
      childBuilder.addForeignKey('fk_parentId', {
        local: 'parentId',
        ref: 'Parent.id',
        timing: constraintTiming,
      });
    }

    return schemaBuilder;
  }

  static async create(constraintTiming) {
    const schemaBuilder =
        ForeignKeysBenchmark.getSchemaBuilder(constraintTiming);
    const db = await(schemaBuilder.connect({
      storeType: lf.DataStoreType.MEMORY,
      enableInspector: true,
    }));
    return Promise.resolve(new ForeignKeysBenchmark(db, constraintTiming));
  }
}
