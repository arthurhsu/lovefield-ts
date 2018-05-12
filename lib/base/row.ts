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

import {Key} from '../index/key_range';

export class RawRow {
  constructor(readonly id: number, readonly value: string|object) {}
}

// The base row class for all rows.
export class Row {
  // An ID to be used when a row that does not correspond to a DB entry is
  // created (for example the result of joining two rows).
  public static DUMMY_ID = -1;

  // Get the next unique row ID to use for creating a new instance.
  public static getNextId(): number {
    return Row.nextId++;
  }

  // Sets the global row id. This is supposed to be called by BackStore
  // instances during initialization only.
  // NOTE: nextId is currently shared among different databases. It is
  // NOT safe to ever decrease this value, because it will result in re-using
  // row IDs. Currently used only for testing, and for backstores that are based
  // on remote truth.
  // @param nextId The next id should be used. This is typically the max
  //     rowId in database plus 1.
  public static setNextId(nextId: number): void {
    Row.nextId = nextId;
  }

  // Updates global row id. Guarantees that the |nextId_| value will only be
  // increased. This is supposed to be called by BackStore instances during
  // initialization only.
  // @param nextId The next id should be used. This is typically the max
  //     rowId in database plus 1.
  public static setNextIdIfGreater(nextId: number): void {
    Row.nextId = Math.max(Row.nextId, nextId);
  }

  // Creates a new Row instance from DB data.
  public static deserialize(data: RawRow): Row {
    return new Row(data.id, data.value as object);
  }

  // Creates a new Row instance with an automatically assigned ID.
  public static create(payload?: object) {
    return new Row(Row.getNextId(), payload || {});
  }

  // ArrayBuffer to hex string.
  public static binToHex(buffer: ArrayBuffer|null): string|null {
    if (buffer === null) {
      return null;
    }

    const uint8Array = new Uint8Array(buffer);
    let s = '';
    uint8Array.forEach((c) => {
      const chr = c.toString(16);
      s += chr.length < 2 ? '0' + chr : chr;
    });
    return s;
  }

  // Hex string to ArrayBuffer.
  public static hexToBin(hex: string|null): ArrayBuffer|null {
    if (hex === null || hex === '') {
      return null;
    }

    if (hex.length % 2 !== 0) {
      hex = '0' + hex;
    }
    const buffer = new ArrayBuffer(hex.length / 2);
    const uint8Array = new Uint8Array(buffer);
    for (let i = 0, j = 0; i < hex.length; i += 2) {
      uint8Array[j++] = parseInt(hex.substr(i, 2), 16);
    }
    return buffer;
  }

  // The ID to assign to the next row that will be created.
  // Should be initialized to the appropriate value from the BackStore instance
  // that is being used.
  private static nextId: number = Row.DUMMY_ID + 1;

  private id_: number;
  private payload_: object;

  constructor(id: number, payload: object) {
    this.id_ = id;
    this.payload_ = payload || this.defaultPayload();
  }

  public id(): number {
    return this.id_;
  }

  // Set the ID of this row instance.
  public assignRowId(id: number): void {
    this.id_ = id;
  }

  public payload(): object {
    return this.payload_;
  }

  public defaultPayload(): object {
    return {};
  }

  public toDbPayload(): object {
    return this.payload_;
  }

  public serialize(): RawRow {
    return new RawRow(this.id_, this.toDbPayload());
  }

  public keyOfIndex(indexName: string): Key {
    if (indexName.substr(-1) === '#') {
      return this.id_ as Key;
    }

    // Remaining indices keys are implemented by overriding keyOfIndex in
    // subclasses.
    return null as any as Key;
  }
}
