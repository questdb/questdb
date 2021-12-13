/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * Non-reentrant, unfair reader-writer lock based on thread-local atomic reader counters and a writer spinlock.
 * TODO document me.
 */
public class TLBiasedReadWriteLock implements ReadWriteLock {

    private final ThreadLocal<PaddedAtomicLong> tlReaderCounter = new ThreadLocal<>(this::newReaderCounter);
    // guarded by wLock
    private final ArrayList<WeakReference<PaddedAtomicLong>> readerCounters = new ArrayList<>();
    // writer spinlock
    private final AtomicBoolean wLock = new AtomicBoolean();

    private final ReadLock readLock = new ReadLock();
    private final WriteLock writeLock = new WriteLock();

    @Override
    public Lock readLock() {
        return readLock;
    }

    @Override
    public Lock writeLock() {
        return writeLock;
    }

    private PaddedAtomicLong newReaderCounter() {
        PaddedAtomicLong counter = new PaddedAtomicLong();
        writeLock.lock0();
        try {
            readerCounters.add(new WeakReference<>(counter));
        } finally {
            writeLock.unlock();
        }
        return counter;
    }

    private class ReadLock implements Lock {

        @Override
        public void lock() {
            PaddedAtomicLong readerCounter = tlReaderCounter.get();
            for (;;) {
                if (!wLock.get()) {
                    if (readerCounter.incrementAndGet() > 1) {
                        throw new IllegalMonitorStateException("reentrant lock calls are not supported");
                    }
                    if (!wLock.get()) {
                        break;
                    }
                    // attempt failed, go for another spin
                    readerCounter.decrementAndGet();
                }
                LockSupport.parkNanos(10);
            }
        }

        @Override
        public void unlock() {
            PaddedAtomicLong readerCounter = tlReaderCounter.get();
            if (readerCounter.decrementAndGet() < 0) {
                throw new IllegalMonitorStateException("uneven lock-unlock calls");
            }
        }

        @Override
        public void lockInterruptibly() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean tryLock() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean tryLock(long time, @NotNull TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Condition newCondition() {
            throw new UnsupportedOperationException();
        }
    }

    private class WriteLock implements Lock {

        @Override
        public void lock() {
            lock0();

            // fast-path: just wait for readers
            boolean cleanup = false;
            for (WeakReference<PaddedAtomicLong> ref : readerCounters) {
                PaddedAtomicLong counter = ref.get();
                if (counter == null) {
                    // a reader thread stopped: need to clean the reference
                    cleanup = true;
                    break;
                }
                while (counter.get() != 0) {
                    LockSupport.parkNanos(10);
                }
            }

            // slow-path: wait for readers and clear weak references
            if (cleanup) {
                Iterator<WeakReference<PaddedAtomicLong>> iterator = readerCounters.iterator();
                while (iterator.hasNext()) {
                    WeakReference<PaddedAtomicLong> ref = iterator.next();
                    PaddedAtomicLong counter = ref.get();
                    if (counter == null) {
                        iterator.remove();
                        continue;
                    }
                    while (counter.get() != 0) {
                        LockSupport.parkNanos(10);
                    }
                }
            }
        }

        private void lock0() {
            while (!wLock.compareAndSet(false, true)) {
                LockSupport.parkNanos(10);
            }
        }

        @Override
        public void unlock() {
            if (!wLock.compareAndSet(true, false)) {
                throw new IllegalMonitorStateException("uneven lock-unlock calls");
            }
        }

        @Override
        public void lockInterruptibly() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean tryLock() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean tryLock(long time, @NotNull TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Condition newCondition() {
            throw new UnsupportedOperationException();
        }
    }

    private static class PaddedAtomicLong extends AtomicLong {
        @SuppressWarnings("unused")
        private long l1, l2, l3, l4, l5, l6, l7;
    }
}
