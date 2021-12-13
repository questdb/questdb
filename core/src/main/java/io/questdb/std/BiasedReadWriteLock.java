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
 * Based on the BRAVO (Biased Locking for Reader-Writer Locks) algorithm: https://arxiv.org/pdf/1810.01553.pdf
 * TODO document me.
 */
public class BiasedReadWriteLock implements ReadWriteLock {

    // number of reader slots; must be a power of two
    private static final int READER_SLOTS = 4096;
    // slow-down guard
    private static final int SLOW_DOWN_MULTIPLIER = 9;

    private final AtomicIntegerArray readerSlots = new AtomicIntegerArray(READER_SLOTS);
    private final SimpleReadWriteLock rwLock = new SimpleReadWriteLock();
    private final AtomicBoolean readerBias = new AtomicBoolean();
    private long inhibitUntil;

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

    private class ReadLock implements Lock {

        @Override
        public void lock() {
            if (readerBias.get()) {
                final int index = readerIndex();
                if (readerSlots.compareAndSet(index, 0, 1)) {
                    if (readerBias.get()) {
                        return;
                    }
                    readerSlots.set(index, 0);
                }
            }

            rwLock.readLock().lock();
            final long now = System.nanoTime();
            if (!readerBias.get() && (inhibitUntil - now < 0)) {
                readerBias.set(true);
            }
        }

        @Override
        public void unlock() {
            final int index = readerIndex();
            if (!readerSlots.compareAndSet(index, 1, 0)) {
                rwLock.readLock().unlock();
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
            return (int) mix64(tid) & (READER_SLOTS - 1);
        }
    }

    private class WriteLock implements Lock {

        @Override
        public void lock() {
            rwLock.writeLock().lock();
            if (readerBias.get()) {
                readerBias.set(false);
                final long start = System.nanoTime();
                for (int i = 0; i < READER_SLOTS; i++) {
                    while (readerSlots.get(i) != 0) {
                        LockSupport.parkNanos(10);
                    }
                }
                final long now = System.nanoTime();
                inhibitUntil = now + (now - start) * SLOW_DOWN_MULTIPLIER;
            }
        }

        @Override
        public void unlock() {
            rwLock.writeLock().unlock();
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
