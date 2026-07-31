/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.cairo;

import io.questdb.mp.continuation.SuspensionScope;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.StampedLock;

/**
 * Adds logical-execution read reentrancy to an ownerless {@link StampedLock}, so a fiber may
 * release its read hold after carrier migration. One logical execution may hold one engine's
 * role-switch read lock at a time. The write view retains {@link StampedLock}'s non-fair,
 * non-reentrant semantics.
 */
final class RoleSwitchReadWriteLock {
    private final StampedLock delegate = new StampedLock();
    private final Lock readLock = new ReadLock(delegate.asReadLock());
    private final Lock writeLock = delegate.asWriteLock();

    int getReadLockCount() {
        return delegate.getReadLockCount();
    }

    Lock readLock() {
        return readLock;
    }

    Lock writeLock() {
        return writeLock;
    }

    private static final class ReadLock implements Lock {
        private final Lock delegate;

        private ReadLock(Lock delegate) {
            this.delegate = delegate;
        }

        @Override
        public void lock() {
            final SuspensionScope.CarrierScope scope = SuspensionScope.scope();
            if (hasReentered(scope)) {
                return;
            }
            delegate.lock();
            enter(scope);
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
            final SuspensionScope.CarrierScope scope = SuspensionScope.scope();
            if (SuspensionScope.hasRoleSwitchReadLock(scope, this)) {
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }
                SuspensionScope.enterRoleSwitchReadLock(scope, this);
                return;
            }
            checkNoOtherRoleSwitchReadLock(scope);
            delegate.lockInterruptibly();
            enter(scope);
        }

        @Override
        public Condition newCondition() {
            return delegate.newCondition();
        }

        @Override
        public boolean tryLock() {
            final SuspensionScope.CarrierScope scope = SuspensionScope.scope();
            if (hasReentered(scope)) {
                return true;
            }
            if (!delegate.tryLock()) {
                return false;
            }
            enter(scope);
            return true;
        }

        @Override
        public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
            Objects.requireNonNull(unit, "unit");
            final SuspensionScope.CarrierScope scope = SuspensionScope.scope();
            if (SuspensionScope.hasRoleSwitchReadLock(scope, this)) {
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }
                SuspensionScope.enterRoleSwitchReadLock(scope, this);
                return true;
            }
            checkNoOtherRoleSwitchReadLock(scope);
            if (!delegate.tryLock(time, unit)) {
                return false;
            }
            enter(scope);
            return true;
        }

        @Override
        public void unlock() {
            final SuspensionScope.CarrierScope scope = SuspensionScope.scope();
            if (!SuspensionScope.hasRoleSwitchReadLock(scope, this)) {
                throw new IllegalMonitorStateException("role-switch read lock is not held by this execution");
            }
            if (SuspensionScope.getRoleSwitchReadLockDepth(scope, this) == 1) {
                delegate.unlock();
            }
            SuspensionScope.leaveRoleSwitchReadLock(scope, this);
        }

        private void checkNoOtherRoleSwitchReadLock(SuspensionScope.CarrierScope scope) {
            if (SuspensionScope.hasRoleSwitchReadLock(scope)) {
                throw new IllegalStateException("another role-switch read lock is already held");
            }
        }

        private void enter(SuspensionScope.CarrierScope scope) {
            try {
                SuspensionScope.enterRoleSwitchReadLock(scope, this);
            } catch (Throwable th) {
                try {
                    delegate.unlock();
                } catch (Throwable cleanupError) {
                    if (cleanupError != th) {
                        th.addSuppressed(cleanupError);
                    }
                }
                throw th;
            }
        }

        private boolean hasReentered(SuspensionScope.CarrierScope scope) {
            if (SuspensionScope.hasRoleSwitchReadLock(scope, this)) {
                SuspensionScope.enterRoleSwitchReadLock(scope, this);
                return true;
            }
            checkNoOtherRoleSwitchReadLock(scope);
            return false;
        }
    }
}
