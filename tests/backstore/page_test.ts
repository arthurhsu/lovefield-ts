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

import * as chai from 'chai';
import { Page } from '../../lib/backstore/page';
import { Row } from '../../lib/base/row';

const assert = chai.assert;

describe('Page', () => {
  const MAGIC = Math.pow(2, Page.BUNDLE_EXPONENT);

  it('toPageIds', () => {
    const rowIds = [0, MAGIC - 1, MAGIC, 2 * MAGIC - 1, 2 * MAGIC];
    const expected = [0, 1, 2];
    assert.sameDeepOrderedMembers(expected, Page.toPageIds(rowIds));
  });

  it('getPageRange', () => {
    const expected0 = [0, MAGIC - 1];
    const expected1 = [MAGIC, 2 * MAGIC - 1];

    assert.sameDeepOrderedMembers(expected0, Page.getPageRange(0));
    assert.sameDeepOrderedMembers(expected1, Page.getPageRange(1));
  });

  function createRows(): Row[] {
    const rows: Row[] = [];
    for (let i = 0; i <= 4 * MAGIC; i += MAGIC / 2) {
      rows.push(new Row(i, { id: i }));
    }
    return rows;
  }

  it('setRemoveRows', () => {
    const rows = createRows();
    const pages = new Map<number, Page>();
    for (let i = 0; i <= 4; ++i) {
      pages.set(i, new Page(i));
    }
    rows.forEach(row => {
      const page = pages.get(Page.toPageId(row.id())) as Page;
      page.setRows([row]);
    });

    for (let i = 0; i < 4; ++i) {
      assert.equal(2, Object.keys((pages.get(i) as Page).getPayload()).length);
    }
    assert.equal(1, Object.keys((pages.get(4) as Page).getPayload()).length);

    (pages.get(4) as Page).removeRows([4 * MAGIC]);
    assert.equal(0, Object.keys((pages.get(4) as Page).getPayload()).length);

    (pages.get(0) as Page).removeRows([MAGIC - 1]);
    assert.equal(2, Object.keys((pages.get(0) as Page).getPayload()).length);
  });
});
