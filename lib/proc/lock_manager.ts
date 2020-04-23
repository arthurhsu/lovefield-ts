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

import { LockType } from '../base/private_enum';
import { Table } from '../schema/table';
import { LockTableEntry } from './lock_table_entry';

// LockManager is responsible for granting locks to tasks. Each lock corresponds
// to a database table.
//
// Four types of locks exist in order to implement a two-phase locking
// algorithm.
// 1) RESERVED_READ_ONLY: Multiple such locks can be granted. It prevents any
//    RESERVED_READ_WRITE and EXCLUSIVE locks from being granted. It needs to be
//    acquired by any task that wants to eventually escalate it to a SHARED
//    lock.
// 2) SHARED: Multiple shared locks can be granted (meant to be used by
//    READ_ONLY tasks). Such tasks must be already holding a RESERVED_READ_ONLY
//    lock.
// 3) RESERVED_READ_WRITE: Granted to a single READ_WRITE task. It prevents
//    further SHARED, RESERVED_READ_ONLY and RESERVED_READ_WRITE locks to be
//    granted, but the underlying table should not be modified yet, until the
//    lock is escalated to an EXCLUSIVE lock.
// 4) EXCLUSIVE: Granted to a single READ_WRITE task. That task must already be
//    holding a RESERVED_READ_WRITE lock. It prevents further SHARED or
//    EXCLUSIVE locks to be granted. It is OK to modify a table while holding
//    such a lock.
export class LockManager {
  private lockTable: Map<string, LockTableEntry>;

  constructor() {
    this.lockTable = new Map<string, LockTableEntry>();
  }

  // Returns whether the requested lock was acquired.
  requestLock(
    taskId: number,
    dataItems: Set<Table>,
    lockType: LockType
  ): boolean {
    const canAcquireLock = this.canAcquireLock(taskId, dataItems, lockType);
    if (canAcquireLock) {
      this.grantLock(taskId, dataItems, lockType);
    }
    return canAcquireLock;
  }

  releaseLock(taskId: number, dataItems: Set<Table>): void {
    dataItems.forEach(dataItem => this.getEntry(dataItem).releaseLock(taskId));
  }

  // Removes any reserved locks for the given data items. This is needed in
  // order to prioritize a taskId higher than a taskId that already holds a
  // reserved lock.
  clearReservedLocks(dataItems: Set<Table>): void {
    dataItems.forEach(
      dataItem => (this.getEntry(dataItem).reservedReadWriteLock = null)
    );
  }

  private getEntry(dataItem: Table): LockTableEntry {
    let lockTableEntry = this.lockTable.get(dataItem.getName()) || null;
    if (lockTableEntry === null) {
      lockTableEntry = new LockTableEntry();
      this.lockTable.set(dataItem.getName(), lockTableEntry);
    }
    return lockTableEntry;
  }

  private grantLock(
    taskId: number,
    dataItems: Set<Table>,
    lockType: LockType
  ): void {
    dataItems.forEach(dataItem =>
      this.getEntry(dataItem).grantLock(taskId, lockType)
    );
  }

  private canAcquireLock(
    taskId: number,
    dataItems: Set<Table>,
    lockType: LockType
  ): boolean {
    let canAcquireLock = true;
    dataItems.forEach(dataItem => {
      if (canAcquireLock) {
        const lockTableEntry = this.getEntry(dataItem);
        canAcquireLock = lockTableEntry.canAcquireLock(taskId, lockType);
      }
    }, this);
    return canAcquireLock;
  }
}
