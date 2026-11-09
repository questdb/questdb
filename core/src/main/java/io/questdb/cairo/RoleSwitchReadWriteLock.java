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

import io.questdb.mp.continuation.Fiber;
import io.questdb.mp.continuation.SuspensionScope;

import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.StampedLock;

/**
 * Adds logical-execution read reentrancy to an ownerless {@link StampedLock}. One logical
 * execution may hold multiple engines' role-switch read locks. A fiber executes in blocking mode
 * while it holds any read lock, so a role-switch writer cannot wait on a parked read holder.
 */
final class RoleSwitchReadWriteLock {
    private final StampedLock delegate = new StampedLock();
    private final AtomicInteger pendingWriterCount = new AtomicInteger();
    private final ReadLock readLock;
    private final WriteLock writeLock;
    private final Semaphore writerGate = new Semaphore(1, true);

    RoleSwitchReadWriteLock() {
        writeLock = new WriteLock(delegate, pendingWriterCount, writerGate);
        readLock = new ReadLock(delegate.asReadLock(), pendingWriterCount, writeLock, writerGate);
        writeLock.setReadLock(readLock);
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
        private final Lock delegate;
        private final AtomicInteger pendingWriterCount;
        private final WriteLock writeLock;
        private final Semaphore writerGate;

        private ReadLock(
                Lock delegate,
                AtomicInteger pendingWriterCount,
                WriteLock writeLock,
                Semaphore writerGate
        ) {
            this.delegate = delegate;
            this.pendingWriterCount = pendingWriterCount;
            this.writeLock = writeLock;
            this.writerGate = writerGate;
        }

        @Override
        public void lock() {
            final SuspensionScope.CarrierScope scope = SuspensionScope.scope();
            if (tryReenter(scope)) {
                return;
            }
            if (writeLock.tryEnterReadDowngrade()) {
                enterDowngraded(scope);
                return;
            }
            while (true) {
                awaitPendingWriters();
                delegate.lock();
                if (pendingWriterCount.get() == 0) {
                    enter(scope);
                    return;
                }
                delegate.unlock();
            }
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
            if (writeLock.isHeldByCurrentThread()) {
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }
                writeLock.enterReadDowngrade();
                enterDowngraded(scope);
                return;
            }
            while (true) {
                awaitPendingWritersInterruptibly();
                delegate.lockInterruptibly();
                if (pendingWriterCount.get() == 0) {
                    enter(scope);
                    return;
                }
                delegate.unlock();
            }
        }

        @Override
        public Condition newCondition() {
            return delegate.newCondition();
        }

        @Override
        public boolean tryLock() {
            final SuspensionScope.CarrierScope scope = SuspensionScope.scope();
            if (tryReenter(scope)) {
                return true;
            }
            if (writeLock.tryEnterReadDowngrade()) {
                enterDowngraded(scope);
                return true;
            }
            if (pendingWriterCount.get() != 0) {
                return false;
            }
            if (!delegate.tryLock()) {
                return false;
            }
            if (pendingWriterCount.get() != 0) {
                delegate.unlock();
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
            while (true) {
                if (!awaitPendingWriters(remainingNanos(timeoutNanos, startNanos))) {
                    return false;
                }
                if (!delegate.tryLock(
                        remainingNanos(timeoutNanos, startNanos),
                        TimeUnit.NANOSECONDS
                )) {
                    return false;
                }
                if (pendingWriterCount.get() == 0) {
                    enter(scope);
                    return true;
                }
                delegate.unlock();
                if (remainingNanos(timeoutNanos, startNanos) == 0) {
                    return false;
                }
            }
        }

        @Override
        public void unlock() {
            final SuspensionScope.CarrierScope scope = SuspensionScope.scope();
            if (!SuspensionScope.hasRoleSwitchReadLock(scope, this)) {
                throw new IllegalMonitorStateException("role-switch read lock is not held by this execution");
            }
            if (SuspensionScope.getRoleSwitchReadLockDepth(scope, this) == 1) {
                if (!writeLock.tryCancelReadDowngrade()) {
                    delegate.unlock();
                }
            }
            SuspensionScope.leaveRoleSwitchReadLock(scope, this);
        }

        private void awaitPendingWriters() {
            while (pendingWriterCount.get() != 0) {
                writerGate.acquireUninterruptibly();
                writerGate.release();
            }
        }

        private boolean awaitPendingWriters(long timeoutNanos) throws InterruptedException {
            if (pendingWriterCount.get() == 0) {
                return true;
            }
            if (!writerGate.tryAcquire(timeoutNanos, TimeUnit.NANOSECONDS)) {
                return false;
            }
            writerGate.release();
            return true;
        }

        private void awaitPendingWritersInterruptibly() throws InterruptedException {
            while (pendingWriterCount.get() != 0) {
                writerGate.acquire();
                writerGate.release();
            }
        }

        private void cancelDowngrade() {
            final SuspensionScope.CarrierScope scope = SuspensionScope.scope();
            while (SuspensionScope.hasRoleSwitchReadLock(scope, this)) {
                SuspensionScope.leaveRoleSwitchReadLock(scope, this);
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
                writeLock.tryCancelReadDowngrade();
                throw th;
            }
        }

        private boolean tryReenter(SuspensionScope.CarrierScope scope) {
            if (SuspensionScope.hasRoleSwitchReadLock(scope, this)) {
                SuspensionScope.enterRoleSwitchReadLock(scope, this);
                return true;
            }
            return false;
        }
    }

    private static final class WriteLock implements Lock {
        private final StampedLock delegate;
        private int holdCount;
        private boolean isReadDowngradePending;
        private volatile Thread owner;
        private final AtomicInteger pendingWriterCount;
        private ReadLock readLock;
        private long stamp;
        private final Semaphore writerGate;

        private WriteLock(
                StampedLock delegate,
                AtomicInteger pendingWriterCount,
                Semaphore writerGate
        ) {
            this.delegate = delegate;
            this.pendingWriterCount = pendingWriterCount;
            this.writerGate = writerGate;
        }

        @Override
        public void lock() {
            rejectFiberOwner();
            if (isHeldByCurrentThread()) {
                reenter();
                return;
            }
            pendingWriterCount.incrementAndGet();
            boolean isGateAcquired = false;
            boolean isLocked = false;
            long newStamp = 0;
            try {
                writerGate.acquireUninterruptibly();
                isGateAcquired = true;
                newStamp = delegate.writeLock();
                enter(newStamp);
                isLocked = true;
            } finally {
                if (!isLocked) {
                    rollbackAcquire(newStamp, isGateAcquired);
                }
            }
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
            rejectFiberOwner();
            if (isHeldByCurrentThread()) {
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }
                reenter();
                return;
            }
            pendingWriterCount.incrementAndGet();
            boolean isGateAcquired = false;
            boolean isLocked = false;
            long newStamp = 0;
            try {
                writerGate.acquire();
                isGateAcquired = true;
                newStamp = delegate.writeLockInterruptibly();
                enter(newStamp);
                isLocked = true;
            } finally {
                if (!isLocked) {
                    rollbackAcquire(newStamp, isGateAcquired);
                }
            }
        }

        @Override
        public Condition newCondition() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean tryLock() {
            rejectFiberOwner();
            if (isHeldByCurrentThread()) {
                reenter();
                return true;
            }
            pendingWriterCount.incrementAndGet();
            boolean isGateAcquired = false;
            boolean isLocked = false;
            long newStamp = 0;
            try {
                if (!writerGate.tryAcquire()) {
                    return false;
                }
                isGateAcquired = true;
                newStamp = delegate.tryWriteLock();
                if (newStamp == 0) {
                    return false;
                }
                enter(newStamp);
                isLocked = true;
                return true;
            } finally {
                if (!isLocked) {
                    rollbackAcquire(newStamp, isGateAcquired);
                }
            }
        }

        @Override
        public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
            Objects.requireNonNull(unit, "unit");
            rejectFiberOwner();
            if (isHeldByCurrentThread()) {
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }
                reenter();
                return true;
            }
            final long timeoutNanos = unit.toNanos(time);
            final long startNanos = System.nanoTime();
            pendingWriterCount.incrementAndGet();
            boolean isGateAcquired = false;
            boolean isLocked = false;
            long newStamp = 0;
            try {
                if (!writerGate.tryAcquire(time, unit)) {
                    return false;
                }
                isGateAcquired = true;
                newStamp = delegate.tryWriteLock(
                        remainingNanos(timeoutNanos, startNanos),
                        TimeUnit.NANOSECONDS
                );
                if (newStamp != 0) {
                    enter(newStamp);
                    isLocked = true;
                }
                return isLocked;
            } finally {
                if (!isLocked) {
                    rollbackAcquire(newStamp, isGateAcquired);
                }
            }
        }

        @Override
        public void unlock() {
            if (!isHeldByCurrentThread()) {
                throw new IllegalMonitorStateException("role-switch write lock is not held by this thread");
            }
            if (holdCount > 1) {
                SuspensionScope.leaveRoleSwitchWriteLock(SuspensionScope.scope());
                holdCount--;
                return;
            }
            if (isReadDowngradePending) {
                final long readStamp = delegate.tryConvertToReadLock(stamp);
                if (readStamp == 0) {
                    delegate.unlockWrite(stamp);
                    try {
                        readLock.cancelDowngrade();
                    } finally {
                        clear();
                        SuspensionScope.leaveRoleSwitchWriteLock(SuspensionScope.scope());
                        releaseWriterGate();
                    }
                    throw new IllegalStateException("could not downgrade role-switch write lock");
                }
            } else {
                delegate.unlockWrite(stamp);
            }
            clear();
            SuspensionScope.leaveRoleSwitchWriteLock(SuspensionScope.scope());
            releaseWriterGate();
        }

        private void clear() {
            holdCount = 0;
            isReadDowngradePending = false;
            stamp = 0;
            owner = null;
        }

        private void enter(long stamp) {
            SuspensionScope.enterRoleSwitchWriteLock(SuspensionScope.scope());
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

        private void reenter() {
            if (holdCount == Integer.MAX_VALUE) {
                throw new IllegalStateException("role-switch write lock depth overflow");
            }
            SuspensionScope.enterRoleSwitchWriteLock(SuspensionScope.scope());
            holdCount++;
        }

        private void rejectFiberOwner() {
            if (Fiber.isMounted()) {
                throw new IllegalStateException("fiber cannot own a role-switch write lock");
            }
        }

        private void releaseWriterGate() {
            pendingWriterCount.decrementAndGet();
            writerGate.release();
        }

        private void rollbackAcquire(long acquiredStamp, boolean isGateAcquired) {
            try {
                if (acquiredStamp != 0) {
                    delegate.unlockWrite(acquiredStamp);
                }
            } finally {
                try {
                    pendingWriterCount.decrementAndGet();
                } finally {
                    if (isGateAcquired) {
                        writerGate.release();
                    }
                }
            }
        }

        private void setReadLock(ReadLock readLock) {
            this.readLock = readLock;
        }

        private boolean tryCancelReadDowngrade() {
            if (isHeldByCurrentThread() && isReadDowngradePending) {
                isReadDowngradePending = false;
                return true;
            }
            return false;
        }

        private boolean tryEnterReadDowngrade() {
            if (isHeldByCurrentThread()) {
                enterReadDowngrade();
                return true;
            }
            return false;
        }
    }
}
