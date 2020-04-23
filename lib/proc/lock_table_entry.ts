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

export class LockTableEntry {
  exclusiveLock: number | null;
  reservedReadWriteLock: number | null;
  reservedReadOnlyLocks: Set<number> | null;
  sharedLocks: Set<number> | null;

  constructor() {
    this.exclusiveLock = null;
    this.reservedReadWriteLock = null;
    this.reservedReadOnlyLocks = null;
    this.sharedLocks = null;
  }

  releaseLock(taskId: number): void {
    if (this.exclusiveLock === taskId) {
      this.exclusiveLock = null;
    }
    if (this.reservedReadWriteLock === taskId) {
      this.reservedReadWriteLock = null;
    }
    if (this.reservedReadOnlyLocks) {
      this.reservedReadOnlyLocks.delete(taskId);
    }
    if (this.sharedLocks) {
      this.sharedLocks.delete(taskId);
    }
  }

  canAcquireLock(taskId: number, lockType: LockType): boolean {
    const noReservedReadOnlyLocksExist =
      this.reservedReadOnlyLocks === null ||
      this.reservedReadOnlyLocks.size === 0;

    if (lockType === LockType.EXCLUSIVE) {
      const noSharedLocksExist =
        this.sharedLocks === null || this.sharedLocks.size === 0;
      return (
        noSharedLocksExist &&
        noReservedReadOnlyLocksExist &&
        this.exclusiveLock === null &&
        this.reservedReadWriteLock !== null &&
        this.reservedReadWriteLock === taskId
      );
    } else if (lockType === LockType.SHARED) {
      return (
        this.exclusiveLock === null &&
        this.reservedReadWriteLock === null &&
        this.reservedReadOnlyLocks !== null &&
        this.reservedReadOnlyLocks.has(taskId)
      );
    } else if (lockType === LockType.RESERVED_READ_ONLY) {
      return this.reservedReadWriteLock === null;
    } else {
      // case of lockType == lf.proc.LockType.RESERVED_READ_WRITE
      return (
        noReservedReadOnlyLocksExist &&
        (this.reservedReadWriteLock === null ||
          this.reservedReadWriteLock === taskId)
      );
    }
  }

  grantLock(taskId: number, lockType: LockType): void {
    if (lockType === LockType.EXCLUSIVE) {
      // TODO(dpapad): Assert that reserved lock was held by this taskId.
      this.reservedReadWriteLock = null;
      this.exclusiveLock = taskId;
    } else if (lockType === LockType.SHARED) {
      // TODO(dpapad): Assert that no other locked is held by this taskId and
      // that no reserved/exclusive locks exist.
      if (this.sharedLocks === null) {
        this.sharedLocks = new Set<number>();
      }
      this.sharedLocks.add(taskId);

      if (this.reservedReadOnlyLocks === null) {
        this.reservedReadOnlyLocks = new Set<number>();
      }
      this.reservedReadOnlyLocks.delete(taskId);
    } else if (lockType === LockType.RESERVED_READ_ONLY) {
      if (this.reservedReadOnlyLocks === null) {
        this.reservedReadOnlyLocks = new Set<number>();
      }
      this.reservedReadOnlyLocks.add(taskId);
    } else if (lockType === LockType.RESERVED_READ_WRITE) {
      // TODO(dpapad): Any other assertions here?
      this.reservedReadWriteLock = taskId;
    }
  }
}
