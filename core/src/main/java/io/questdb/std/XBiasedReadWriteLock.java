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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * Non-reentrant, unfair reader-writer lock based on sharded reader atomic counters and a writer spinlock.
 * TODO document me.
 */
public class XBiasedReadWriteLock implements ReadWriteLock {

    private static final int CACHE_LINE_BYTES = 128;
    private static final int SLOTS_PER_CACHE_LINE = CACHE_LINE_BYTES / Integer.BYTES;
    private static final int CACHE_LINES_PER_READER = 8;

    private final int rSlotsNum; // number of reader slots; must be a power of two
    private final AtomicIntegerArray rSlots;
    private final AtomicBoolean wLock = new AtomicBoolean();

    private final ReadLock readLock = new ReadLock();
    private final WriteLock writeLock = new WriteLock();

    public XBiasedReadWriteLock() {
        this(Runtime.getRuntime().availableProcessors());
    }

    public XBiasedReadWriteLock(int concurrency) {
        this.rSlotsNum = Numbers.ceilPow2(concurrency) * SLOTS_PER_CACHE_LINE * CACHE_LINES_PER_READER;
        this.rSlots = new AtomicIntegerArray(rSlotsNum);
    }

    @Override
    public Lock readLock() {
        return readLock;
    }

    @Override
    public Lock writeLock() {
        return writeLock;
    }

    private class ReadLock implements Lock {

        @Override
        public void lock() {
            final int index = readerIndex();
            for (;;) {
                if (!wLock.get()) {
                    if (rSlots.addAndGet(index, 1) < 0) {
                        throw new IllegalMonitorStateException("max number of readers reached");
                    }
                    if (!wLock.get()) {
                        break;
                    }
                    // attempt failed, go for another spin
                    rSlots.addAndGet(index, -1);
                }
                LockSupport.parkNanos(10);
            }
        }

        @Override
        public void unlock() {
            final int index = readerIndex();
            if (rSlots.addAndGet(index, -1) < 0) {
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

        private int readerIndex() {
            final long tid = Thread.currentThread().getId();
            return (int) mix64(tid) & (rSlotsNum - 1);
        }
    }

    private class WriteLock implements Lock {

        @Override
        public void lock() {
            while (!wLock.compareAndSet(false, true)) {
                LockSupport.parkNanos(10);
            }
            for (int i = 0; i < rSlotsNum; i++) {
                while (rSlots.get(i) == 1) {
                    LockSupport.parkNanos(10);
                }
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

    private static long mix64(long z) {
        z = (z ^ (z >>> 33)) * 0xff51afd7ed558ccdL;
        z = (z ^ (z >>> 33)) * 0xc4ceb9fe1a85ec53L;
        return z ^ (z >>> 33);
    }
}
