/**
 * @license
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

import {DEFAULT_VALUES, Type} from '../base/enum';
import {Row} from '../base/row';
import {Key} from '../index/key_range';
import {Column} from './column';
import {Index} from './index';

export class RowImpl extends Row {
  private columns: Column[];
  private functionMap: Map<string, (column: any) => Key>;

  constructor(
      functionMap: Map<string, (column: any) => Key>, columns: Column[],
      indices: Index[], id: number, payload?: object) {
    super(id, payload);
    this.columns = columns;
    this.functionMap = functionMap;

    // TypeScript forbids super to be called after this. Therefore we need
    // to duplicate this line from base class ctor because defaultPayload()
    // needs to know column information.
    this.payload_ = payload || this.defaultPayload();
  }

  public defaultPayload(): object {
    if (this.columns === undefined) {
      // Called from base ctor, ignore for now.
      return null as any as object;
    }

    const obj = {};
    this.columns.forEach((col) => {
      obj[col.getName()] =
          col.isNullable() ? null : DEFAULT_VALUES.get(col.getType());
    });
    return obj;
  }

  public toDbPayload(): object {
    const obj = {};
    this.columns.forEach((col) => {
      const key = col.getName();
      const type = col.getType();
      let value = (this.payload() as any)[key];
      if (type === Type.ARRAY_BUFFER) {
        value = value ? Row.binToHex(value) : null;
      } else if (type === Type.DATE_TIME) {
        value = value ? value.getTime() : null;
      } else if (type === Type.OBJECT) {
        value = value || null;
      } else {
        value = value;
      }
      obj[key] = value;
    });
    return obj;
  }

  public keyOfIndex(indexName: string) {
    const key = super.keyOfIndex(indexName);
    if (key === null) {
      const fn = this.functionMap.get(indexName);
      if (fn) {
        return fn(this.payload());
      }
    }
    return key;
  }
}
