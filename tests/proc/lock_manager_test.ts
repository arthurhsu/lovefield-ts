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
import { LockType } from '../../lib/base/private_enum';
import { LockManager } from '../../lib/proc/lock_manager';
import { Table } from '../../lib/schema/table';
import { getHrDbSchemaBuilder } from '../../testing/hr_schema/hr_schema_builder';

const assert = chai.assert;

describe('LockManager', () => {
  let j: Set<Table>;
  let lockManager: LockManager;

  beforeEach(() => {
    j = new Set<Table>();
    j.add(
      getHrDbSchemaBuilder()
        .getSchema()
        .table('Job')
    );
    lockManager = new LockManager();
  });

  it('requestLock_SharedLocksOnly', () => {
    for (let i = 0; i < 10; i++) {
      const taskId = i;
      // SHARED lock can't be granted if RESERVED_READ_ONLY has not been already
      // granted.
      assert.isFalse(lockManager.requestLock(taskId, j, LockType.SHARED));
      assert.isTrue(
        lockManager.requestLock(taskId, j, LockType.RESERVED_READ_ONLY)
      );
      assert.isTrue(lockManager.requestLock(taskId, j, LockType.SHARED));
    }
  });

  it('requestLock_ReservedReadWriteLocksOnly', () => {
    const dataItems = j;

    // Granting the lock to a single task.
    const reservedLockId = 0;
    assert.isTrue(
      lockManager.requestLock(
        reservedLockId,
        dataItems,
        LockType.RESERVED_READ_WRITE
      )
    );

    // Expecting lock to be denied.
    for (let i = 1; i < 5; i++) {
      const taskId = i;
      assert.isFalse(
        lockManager.requestLock(taskId, dataItems, LockType.RESERVED_READ_WRITE)
      );
    }

    // Releasing the lock.
    lockManager.releaseLock(reservedLockId, dataItems);

    // Expecting the lock to be granted again.
    assert.isTrue(
      lockManager.requestLock(1, dataItems, LockType.RESERVED_READ_WRITE)
    );
  });

  it('requestLock_ExclusiveLocksOnly', () => {
    const dataItems = j;

    // EXCLUSIVE lock can't be granted if RESERVED_READ_WRITE has not been
    // already granted.
    assert.isFalse(lockManager.requestLock(0, dataItems, LockType.EXCLUSIVE));
    assert.isTrue(
      lockManager.requestLock(0, dataItems, LockType.RESERVED_READ_WRITE)
    );
    assert.isTrue(lockManager.requestLock(0, dataItems, LockType.EXCLUSIVE));

    assert.isTrue(
      lockManager.requestLock(1, dataItems, LockType.RESERVED_READ_WRITE)
    );
    // An EXCLUSIVE lock can't be granted, already held by taskId 0.
    assert.isFalse(lockManager.requestLock(1, dataItems, LockType.EXCLUSIVE));
    lockManager.releaseLock(0, dataItems);

    // RESERVED_READ_WRITE lock is already being held. Calling requestLock()
    // should still return true for such cases.
    assert.isTrue(
      lockManager.requestLock(1, dataItems, LockType.RESERVED_READ_WRITE)
    );
    // EXCLUSIVE lock can now be granted, since taskId 0 has released it.
    assert.isTrue(
      lockManager.requestLock(1, dataItems, LockType.RESERVED_READ_WRITE)
    );
  });

  // Checks that for a given resource,
  // 1) RESERVED_READ_ONLY, SHARED locks can't be granted if a
  // RESERVED_READ_WRITE lock has already been granted.
  // 2) RESERVED_READ_ONLY, SHARED locks can be granted after a previously
  // granted RESERVED_READ_WRITE lock is released.
  it('requestLock_SharedReserved', () => {
    const dataItems = j;

    // Granting SHARED locks to a bunch of tasks.
    for (let i = 0; i < 10; i++) {
      const taskId = i;
      assert.isTrue(
        lockManager.requestLock(taskId, j, LockType.RESERVED_READ_ONLY)
      );
      assert.isTrue(lockManager.requestLock(taskId, j, LockType.SHARED));
    }

    // Granting a reserved lock to a single task.
    const reservedLockId = 11;
    assert.isTrue(
      lockManager.requestLock(
        reservedLockId,
        dataItems,
        LockType.RESERVED_READ_WRITE
      )
    );

    // Expecting that no new SHARED locks can be granted.
    for (let i = 20; i < 30; i++) {
      const taskId = i;
      assert.isFalse(
        lockManager.requestLock(taskId, j, LockType.RESERVED_READ_ONLY)
      );
      assert.isFalse(lockManager.requestLock(taskId, j, LockType.SHARED));
    }

    // Releasing RESERVED_READ_WRITE lock.
    lockManager.releaseLock(reservedLockId, dataItems);

    // Expecting new SHARED locks to be successfully granted.
    for (let i = 20; i < 30; i++) {
      const taskId = i;
      assert.isTrue(
        lockManager.requestLock(taskId, j, LockType.RESERVED_READ_ONLY)
      );
      assert.isTrue(lockManager.requestLock(taskId, j, LockType.SHARED));
    }
  });

  // Checks that for a given resource,
  // 1) a RESERVED_READ_WRITE lock can be granted even if an EXCLUSIVE lock has
  //    already been granted.
  // 2) an EXCLUSIVE lock can be granted, after a previous task releases it.
  it('requestLock_ReservedExclusive', () => {
    const dataItems = j;
    let taskId = 0;
    assert.isTrue(
      lockManager.requestLock(taskId, dataItems, LockType.RESERVED_READ_WRITE)
    );
    assert.isTrue(
      lockManager.requestLock(taskId, dataItems, LockType.EXCLUSIVE)
    );

    for (let i = 1; i < 5; i++) {
      taskId = i;
      // Acquiring RESERVED_READ_WRITE lock once.
      assert.isTrue(
        lockManager.requestLock(taskId, dataItems, LockType.RESERVED_READ_WRITE)
      );
      // Ensuring that RESERVED_READ_WRITE lock is re-entrant.
      assert.isTrue(
        lockManager.requestLock(taskId, dataItems, LockType.RESERVED_READ_WRITE)
      );

      // Expecting EXCLUSIVE lock to be denied since it is held by the previous
      // taskId.
      assert.isFalse(
        lockManager.requestLock(taskId, dataItems, LockType.EXCLUSIVE)
      );
      lockManager.releaseLock(taskId - 1, dataItems);

      // Expecting EXCLUSIVE lock to be granted since it was released by the
      // previous taskId.
      assert.isTrue(
        lockManager.requestLock(taskId, dataItems, LockType.EXCLUSIVE)
      );
    }
  });

  // Checks that for a given resource,
  // 1) a SHARED lock can't be granted if an EXCLUSIVE lock has already been
  //    granted.
  // 2) an EXCLUSIVE can't be granted if SHARED locks have already been granted.
  it('requestLock_SharedExclusive', () => {
    const dataItems = j;

    const exclusiveLockId = 0;
    assert.isTrue(
      lockManager.requestLock(
        exclusiveLockId,
        dataItems,
        LockType.RESERVED_READ_WRITE
      )
    );
    assert.isTrue(
      lockManager.requestLock(exclusiveLockId, dataItems, LockType.EXCLUSIVE)
    );

    for (let i = 1; i < 5; i++) {
      const taskId = i;
      assert.isTrue(
        lockManager.requestLock(taskId, dataItems, LockType.RESERVED_READ_ONLY)
      );
      assert.isFalse(
        lockManager.requestLock(taskId, dataItems, LockType.SHARED)
      );
    }
    lockManager.releaseLock(exclusiveLockId, dataItems);

    for (let i = 1; i < 5; i++) {
      const taskId = i;
      // RESERVED_READ_ONLY lock is already being held. Calling requestLock()
      // should still return true for such cases.
      assert.isTrue(
        lockManager.requestLock(taskId, dataItems, LockType.RESERVED_READ_ONLY)
      );
      assert.isTrue(
        lockManager.requestLock(taskId, dataItems, LockType.SHARED)
      );
    }

    // Ensuring that a RESERVED_READ_WRITE lock can be granted, but can't be
    // escalated yet to an EXCLUSIVE.
    assert.isTrue(
      lockManager.requestLock(
        exclusiveLockId,
        dataItems,
        LockType.RESERVED_READ_WRITE
      )
    );
    assert.isFalse(
      lockManager.requestLock(exclusiveLockId, dataItems, LockType.EXCLUSIVE)
    );
  });
});
