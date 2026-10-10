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

package io.questdb.std;

import org.jetbrains.annotations.NotNull;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * This lock is not reentrant.
 * If a thread holds a write lock, and it tries to grab a lock again it will deadlock.
 * If a thread holding a read lock tries to upgrade its lock to a write lock it must first release its read lock or it will deadlock.
 * Threads waiting on a write lock have priority over threads waiting on a read lock.
 * Threads waiting on a write lock are not resumed fairly.
 *
 * @author Patrick Mackinlay
 */
public class SimpleReadWriteLock implements ReadWriteLock {
    private final int MAX_READERS = Integer.MAX_VALUE / 2 - 1;
    private final AtomicBoolean lock = new AtomicBoolean();
    private final AtomicInteger nReaders = new AtomicInteger();

    private final ReadLock readLock = new ReadLock();
    private final WriteLock writeLock = new WriteLock();

    public boolean isWriteLocked() {
        return lock.get();
    }

    @Override
    public @NotNull Lock readLock() {
        return readLock;
    }

    @Override
    public @NotNull Lock writeLock() {
        return writeLock;
    }

    private class ReadLock implements Lock {
        @Override
        public void lock() {
            while (nReaders.incrementAndGet() >= MAX_READERS) {
                nReaders.decrementAndGet();
                Thread.yield();
            }
        }

        @Override
        public void lockInterruptibly() {
            throw new UnsupportedOperationException();
        }

        @Override
        public @NotNull Condition newCondition() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean tryLock(long time, @NotNull TimeUnit unit) {
            final long deadline = System.nanoTime() + unit.toNanos(time);
            for (; ; ) {
                if (tryLock()) {
                    return true;
                }
                // Overflow-safe deadline comparison.
                if (System.nanoTime() - deadline >= 0) {
                    return false;
                }
                Thread.yield();
            }
        }

        @Override
        public boolean tryLock() {
            if (nReaders.incrementAndGet() >= MAX_READERS) {
                // A writer holds the lock or is waiting for readers to drain.
                nReaders.decrementAndGet();
                return false;
            }
            return true;
        }

        @Override
        public void unlock() {
            nReaders.decrementAndGet();
        }
    }

    private class WriteLock implements Lock {
        @Override
        public void lock() {
            while (!lock.compareAndSet(false, true)) {
                Thread.yield();
            }
            int n = nReaders.addAndGet(MAX_READERS);
            while (n != MAX_READERS) {
                n = nReaders.get();
            }
        }

        @Override
        public void lockInterruptibly() {
            throw new UnsupportedOperationException();
        }

        @Override
        public @NotNull Condition newCondition() {
            throw new UnsupportedOperationException();
        }

        /**
         * Bounded write acquire. {@link #lock()} waits for readers to drain in a bare spin with no
         * upper bound, so a caller that must not be pinned indefinitely -- for example one running on
         * a thread borrowed from another runtime -- has no way to give up. {@link #tryLock()} only
         * offers all-or-nothing at the instant of the call. This gives up after {@code time}.
         *
         * <p>On timeout the reader bias and the exclusive flag are both rolled back, so readers that
         * were fenced out while we waited are released.
         */
        @Override
        public boolean tryLock(long time, @NotNull TimeUnit unit) {
            final long deadline = System.nanoTime() + unit.toNanos(time);
            // Phase 1: take the exclusive flag. This also fences out competing writers.
            while (!lock.compareAndSet(false, true)) {
                // Overflow-safe deadline comparison.
                if (System.nanoTime() - deadline >= 0) {
                    return false;
                }
                Thread.yield();
            }
            // Phase 2: bias nReaders so no new reader can enter, then wait for residents to leave.
            if (nReaders.addAndGet(MAX_READERS) == MAX_READERS) {
                return true;
            }
            while (nReaders.get() != MAX_READERS) {
                if (System.nanoTime() - deadline >= 0) {
                    nReaders.addAndGet(-MAX_READERS);
                    lock.set(false);
                    return false;
                }
                Thread.yield();
            }
            return true;
        }

        @Override
        public boolean tryLock() {
            if (!lock.compareAndSet(false, true)) {
                return false;
            }
            int n = nReaders.addAndGet(MAX_READERS);
            if (n == MAX_READERS) {
                return true;
            }
            nReaders.addAndGet(-MAX_READERS);
            lock.set(false);
            return false;
        }

        @Override
        public void unlock() {
            nReaders.addAndGet(-MAX_READERS);
            lock.set(false);
        }
    }
}
