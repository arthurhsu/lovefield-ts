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

import {PayloadType, RawRow, Row} from '../base/row';

// The base page class for bundled rows. Each page is a physical row in
// IndexedDB, and contains 2^BUNDLE_EXPONENT logical rows.
export class Page {
  // Power factor of bundle size, e.g. 9 means 2^9 = 512.
  static BUNDLE_EXPONENT = 9;

  //  Creates a new Page instance from DB data.
  static deserialize(data: RawRow): Page {
    return new Page(
      data.id,
      (JSON.parse((data.value as unknown) as string) as unknown) as PayloadType
    );
  }

  // Returns distinct page ids containing given row ids.
  static toPageIds(rowIds: number[]): number[] {
    const pageIds = new Set<number>();
    rowIds.forEach(id => pageIds.add(Page.toPageId(id)));
    return Array.from(pageIds.values());
  }

  static toPageId(rowId: number): number {
    return rowId >> Page.BUNDLE_EXPONENT;
  }

  // Returns range of page's row id [from, to].
  static getPageRange(pageId: number): [number, number] {
    return [
      pageId << Page.BUNDLE_EXPONENT,
      ((pageId + 1) << Page.BUNDLE_EXPONENT) - 1,
    ];
  }

  constructor(private id: number, private payload: PayloadType = {}) {}

  getId(): number {
    return this.id;
  }

  getPayload(): PayloadType {
    return this.payload;
  }

  setRows(rows: Row[]): void {
    rows.forEach(row => (this.payload[row.id()] = row.serialize()));
  }

  removeRows(ids: number[]): void {
    ids.forEach(id => delete this.payload[id]);
  }

  serialize(): RawRow {
    return {
      id: this.id,
      value: (JSON.stringify(this.payload) as unknown) as PayloadType,
    };
  }
}
