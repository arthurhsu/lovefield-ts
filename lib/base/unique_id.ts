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

export class UniqueId {
  private static nextId = 0;
  uniqueId!: string;
  uniqueNumber!: number;

  getUniqueId(): string {
    if (this.uniqueId === undefined) {
      this.uniqueId = `lf_${this.getUniqueNumber()}`;
    }
    return this.uniqueId;
  }

  getUniqueNumber(): number {
    if (this.uniqueNumber === undefined) {
      this.uniqueNumber = ++UniqueId.nextId;
    }
    return this.uniqueNumber;
  }
}
