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
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.StampedLock;

/**
 * Adds logical-execution read reentrancy to an ownerless {@link StampedLock}, so a fiber may
 * release its read hold after carrier migration. One logical execution may hold one engine's
 * role-switch read lock at a time.
 */
final class RoleSwitchReadWriteLock {
    private final Semaphore admissionGate = new Semaphore(1, true);
    private final StampedLock delegate = new StampedLock();
    private final Lock readLock;
    private final WriteLock writeLock;

    RoleSwitchReadWriteLock() {
        writeLock = new WriteLock(admissionGate, delegate);
        readLock = new ReadLock(admissionGate, delegate.asReadLock(), writeLock);
    }

    int getReadLockCount() {
        return delegate.getReadLockCount();
    }

    Lock readLock() {
        return readLock;
    }

    Lock writeLock() {
        return writeLock;
    }

    private static long remainingNanos(long timeoutNanos, long startNanos) {
        if (timeoutNanos <= 0) {
            return 0;
        }
        final long elapsedNanos = System.nanoTime() - startNanos;
        if (elapsedNanos <= 0) {
            return timeoutNanos;
        }
        return elapsedNanos >= timeoutNanos ? 0 : timeoutNanos - elapsedNanos;
    }

    private static final class ReadLock implements Lock {
        private final Semaphore admissionGate;
        private final Lock delegate;
        private final WriteLock writeLock;

        private ReadLock(Semaphore admissionGate, Lock delegate, WriteLock writeLock) {
            this.admissionGate = admissionGate;
            this.delegate = delegate;
            this.writeLock = writeLock;
        }

        @Override
        public void lock() {
            final SuspensionScope.CarrierScope scope = SuspensionScope.scope();
            if (hasReentered(scope)) {
                return;
            }
            if (writeLock.hasEnteredReadDowngrade()) {
                enterDowngraded(scope);
                return;
            }
            admissionGate.acquireUninterruptibly();
            try {
                delegate.lock();
            } finally {
                admissionGate.release();
            }
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
            if (writeLock.isHeldByCurrentThread()) {
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }
                writeLock.enterReadDowngrade();
                enterDowngraded(scope);
                return;
            }
            admissionGate.acquire();
            try {
                delegate.lockInterruptibly();
            } finally {
                admissionGate.release();
            }
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
            if (writeLock.hasEnteredReadDowngrade()) {
                enterDowngraded(scope);
                return true;
            }
            if (!admissionGate.tryAcquire()) {
                return false;
            }
            final boolean isLocked;
            try {
                isLocked = delegate.tryLock();
            } finally {
                admissionGate.release();
            }
            if (!isLocked) {
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
            if (writeLock.isHeldByCurrentThread()) {
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }
                writeLock.enterReadDowngrade();
                enterDowngraded(scope);
                return true;
            }
            final long timeoutNanos = unit.toNanos(time);
            final long startNanos = System.nanoTime();
            if (!admissionGate.tryAcquire(time, unit)) {
                return false;
            }
            final boolean isLocked;
            try {
                isLocked = delegate.tryLock(
                        remainingNanos(timeoutNanos, startNanos),
                        TimeUnit.NANOSECONDS
                );
            } finally {
                admissionGate.release();
            }
            if (!isLocked) {
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
                if (!writeLock.hasCancelledReadDowngrade()) {
                    delegate.unlock();
                }
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

        private void enterDowngraded(SuspensionScope.CarrierScope scope) {
            try {
                SuspensionScope.enterRoleSwitchReadLock(scope, this);
            } catch (Throwable th) {
                writeLock.hasCancelledReadDowngrade();
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

    private static final class WriteLock implements Lock {
        private final Semaphore admissionGate;
        private final StampedLock delegate;
        private int holdCount;
        private boolean isReadDowngradePending;
        private volatile Thread owner;
        private long stamp;

        private WriteLock(Semaphore admissionGate, StampedLock delegate) {
            this.admissionGate = admissionGate;
            this.delegate = delegate;
        }

        @Override
        public void lock() {
            if (isHeldByCurrentThread()) {
                holdCount++;
                return;
            }
            admissionGate.acquireUninterruptibly();
            boolean isLocked = false;
            try {
                final long newStamp = delegate.writeLock();
                enter(newStamp);
                isLocked = true;
            } finally {
                if (!isLocked) {
                    admissionGate.release();
                }
            }
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
            if (isHeldByCurrentThread()) {
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }
                holdCount++;
                return;
            }
            admissionGate.acquire();
            boolean isLocked = false;
            try {
                final long newStamp = delegate.writeLockInterruptibly();
                enter(newStamp);
                isLocked = true;
            } finally {
                if (!isLocked) {
                    admissionGate.release();
                }
            }
        }

        @Override
        public Condition newCondition() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean tryLock() {
            if (isHeldByCurrentThread()) {
                holdCount++;
                return true;
            }
            if (!admissionGate.tryAcquire()) {
                return false;
            }
            final long newStamp = delegate.tryWriteLock();
            if (newStamp != 0) {
                enter(newStamp);
                return true;
            }
            admissionGate.release();
            return false;
        }

        @Override
        public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
            Objects.requireNonNull(unit, "unit");
            if (isHeldByCurrentThread()) {
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }
                holdCount++;
                return true;
            }
            final long timeoutNanos = unit.toNanos(time);
            final long startNanos = System.nanoTime();
            if (!admissionGate.tryAcquire(time, unit)) {
                return false;
            }
            boolean isLocked = false;
            try {
                final long newStamp = delegate.tryWriteLock(
                        remainingNanos(timeoutNanos, startNanos),
                        TimeUnit.NANOSECONDS
                );
                isLocked = newStamp != 0;
                if (isLocked) {
                    enter(newStamp);
                }
                return isLocked;
            } finally {
                if (!isLocked) {
                    admissionGate.release();
                }
            }
        }

        @Override
        public void unlock() {
            if (!isHeldByCurrentThread()) {
                throw new IllegalMonitorStateException("role-switch write lock is not held by this thread");
            }
            if (--holdCount > 0) {
                return;
            }
            if (isReadDowngradePending) {
                final long readStamp = delegate.tryConvertToReadLock(stamp);
                if (readStamp == 0) {
                    delegate.unlockWrite(stamp);
                    clear();
                    admissionGate.release();
                    throw new IllegalStateException("could not downgrade role-switch write lock");
                }
            } else {
                delegate.unlockWrite(stamp);
            }
            clear();
            admissionGate.release();
        }

        private boolean hasCancelledReadDowngrade() {
            if (isHeldByCurrentThread() && isReadDowngradePending) {
                isReadDowngradePending = false;
                return true;
            }
            return false;
        }

        private void clear() {
            holdCount = 0;
            isReadDowngradePending = false;
            stamp = 0;
            owner = null;
        }

        private void enter(long stamp) {
            this.stamp = stamp;
            holdCount = 1;
            isReadDowngradePending = false;
            owner = Thread.currentThread();
        }

        private void enterReadDowngrade() {
            if (!isHeldByCurrentThread()) {
                throw new IllegalMonitorStateException("role-switch write lock is not held by this thread");
            }
            isReadDowngradePending = true;
        }

        private boolean isHeldByCurrentThread() {
            return owner == Thread.currentThread();
        }

        private boolean hasEnteredReadDowngrade() {
            if (isHeldByCurrentThread()) {
                enterReadDowngrade();
                return true;
            }
            return false;
        }
    }
}
