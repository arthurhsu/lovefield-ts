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

export class TransactionStats {
  public static getDefault(): TransactionStats {
    return new TransactionStats(false, 0, 0, 0, 0);
  }

  constructor(
      private success_: boolean,
      private insertedRowCount_: number,
      private updatedRowCount_: number,
      private deletedRowCount_: number,
      private changedTableCount_: number,
  ) {}

  public success(): boolean {
    return this.success_;
  }
  public insertedRowCount(): number {
    return this.insertedRowCount_;
  }
  public updatedRowCount(): number {
    return this.updatedRowCount_;
  }
  public deletedRowCount(): number {
    return this.deletedRowCount_;
  }
  public changedTableCount(): number {
    return this.changedTableCount_;
  }
}
