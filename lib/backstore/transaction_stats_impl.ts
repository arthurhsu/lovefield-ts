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

import { TransactionStats } from './transaction_stats';

export class TransactionStatsImpl implements TransactionStats {
  static getDefault(): TransactionStatsImpl {
    return new TransactionStatsImpl(false, 0, 0, 0, 0);
  }

  constructor(
    private success_: boolean,
    private insertedRowCount_: number,
    private updatedRowCount_: number,
    private deletedRowCount_: number,
    private changedTableCount_: number
  ) {}

  success(): boolean {
    return this.success_;
  }
  insertedRowCount(): number {
    return this.insertedRowCount_;
  }
  updatedRowCount(): number {
    return this.updatedRowCount_;
  }
  deletedRowCount(): number {
    return this.deletedRowCount_;
  }
  changedTableCount(): number {
    return this.changedTableCount_;
  }
}
