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

import {DEFAULT_VALUES, Type} from '../base/enum';
import {PayloadType, Row} from '../base/row';
import {Key} from '../index/key_range';

import {Column} from './column';
import {Index} from './index';

export class RowImpl extends Row {
  constructor(
    private functionMap: Map<string, (payload: PayloadType) => Key>,
    private columns: Column[],
    indices: Index[],
    id: number,
    payload?: PayloadType
  ) {
    super(id, payload);

    // TypeScript forbids super to be called after this. Therefore we need
    // to duplicate this line from base class ctor because defaultPayload()
    // needs to know column information.
    this.payload_ = payload || this.defaultPayload();
  }

  defaultPayload(): PayloadType {
    if (this.columns === undefined) {
      // Called from base ctor, ignore for now.
      return (null as unknown) as PayloadType;
    }

    const obj: PayloadType = {};
    this.columns.forEach(col => {
      obj[col.getName()] = col.isNullable()
        ? null
        : DEFAULT_VALUES.get(col.getType());
    });
    return obj;
  }

  toDbPayload(): PayloadType {
    const obj: PayloadType = {};
    this.columns.forEach(col => {
      const key = col.getName();
      const type = col.getType();
      let value = (this.payload() as PayloadType)[key];
      if (type === Type.ARRAY_BUFFER) {
        value = value ? Row.binToHex(value as ArrayBuffer) : null;
      } else if (type === Type.DATE_TIME) {
        value = value ? (value as Date).getTime() : null;
      } else if (type === Type.OBJECT) {
        value = value || null;
      }
      obj[key] = value;
    });
    return obj;
  }

  keyOfIndex(indexName: string): Key {
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
