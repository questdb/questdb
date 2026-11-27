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

package io.questdb.mp;

import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;

/**
 * Fair, non-reentrant, non-owner-bound lock: any thread may release it, so a lock taken by one
 * thread (e.g. CHECKPOINT CREATE) can be released by another (e.g. CHECKPOINT RELEASE).
 * <p>
 * Waiters park in a FIFO queue (no spinning). Fairness means a barging {@link #tryLock()} fails
 * while a thread is queued, so {@link #unlock()} effectively hands the lock to the first waiter.
 * Without this, a thread re-taking the lock in a tight loop could starve a waiter indefinitely.
 */
public final class SimpleWaitingLock {
    private final Sync sync = new Sync();

    @TestOnly
    public Object getWaitBlocker() {
        return sync;
    }

    public boolean isLocked() {
        return sync.isLocked();
    }

    /**
     * Acquires the lock, blocking until it is available. Interrupts are ignored.
     */
    public void lock() {
        sync.acquire(1);
    }

    /**
     * Tries to acquire the lock, waiting up to the given timeout. Interrupts do not abort the wait;
     * the interrupt flag is preserved and restored on exit.
     *
     * @param timeout the maximum time to wait for acquiring the lock, in the given TimeUnit
     * @param unit    the time unit of the timeout parameter
     * @return true if the lock was acquired within the timeout period, false otherwise
     */
    public boolean tryLock(long timeout, TimeUnit unit) {
        if (sync.tryAcquire(1)) {
            return true;
        }
        // this might overflow, but that's OK: subtracting the current nanoTime makes it positive again
        final long deadline = System.nanoTime() + unit.toNanos(timeout);
        boolean isInterrupted = false;
        try {
            for (long remainingNanos = deadline - System.nanoTime(); remainingNanos > 0; remainingNanos = deadline - System.nanoTime()) {
                try {
                    return sync.tryAcquireNanos(1, remainingNanos);
                } catch (InterruptedException e) {
                    // The interrupt flag is now clear, so the retry parks rather than spins.
                    isInterrupted = true;
                }
            }
            return false;
        } finally {
            if (isInterrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Acquires the lock only if it is free and no other thread is waiting for it.
     *
     * @return true if the lock was acquired, false otherwise
     */
    public boolean tryLock() {
        return sync.tryAcquire(1);
    }

    /**
     * Releases the lock and wakes the first waiter, if any. The calling thread does not have to
     * be the one that acquired the lock.
     *
     * @throws IllegalStateException if the lock is not held
     */
    public void unlock() {
        sync.release(1);
    }

    private static final class Sync extends AbstractQueuedSynchronizer {
        @Override
        protected boolean tryAcquire(int ignore) {
            return !hasQueuedPredecessors() && compareAndSetState(0, 1);
        }

        @Override
        protected boolean tryRelease(int ignore) {
            if (!compareAndSetState(1, 0)) {
                throw new IllegalStateException("Lock is not owned");
            }
            return true;
        }

        boolean isLocked() {
            return getState() != 0;
        }
    }
}
