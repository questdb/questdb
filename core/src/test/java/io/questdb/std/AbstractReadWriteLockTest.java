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

import org.junit.Assert;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReadWriteLock;

abstract class AbstractReadWriteLockTest {

    protected static final int WRITER_ACTIVITY_NUM = 100000;

    protected void testHammerLock(ReadWriteLock lock, int readers, int writers, int iterations) throws Exception {
        CyclicBarrier barrier = new CyclicBarrier(readers + writers);
        CountDownLatch latch = new CountDownLatch(readers + writers);
        AtomicInteger activity = new AtomicInteger();

        for (int i = 0; i < readers; i++) {
            Reader reader = new Reader(lock, barrier, latch, activity, iterations);
            reader.start();
        }

        for (int i = 0; i < writers; i++) {
            Writer writer = new Writer(lock, barrier, latch, activity, iterations);
            writer.start();
        }

        latch.await();

        Assert.assertEquals(0, activity.get());
    }

    protected static class Reader extends Thread {

        private final ReadWriteLock lock;
        private final CyclicBarrier barrier;
        private final CountDownLatch latch;
        private final AtomicInteger activity;
        private final int iterations;

        private Reader(ReadWriteLock lock, CyclicBarrier barrier, CountDownLatch latch, AtomicInteger activity, int iterations) {
            this.lock = lock;
            this.barrier = barrier;
            this.latch = latch;
            this.activity = activity;
            this.iterations = iterations;
        }

        @Override
        public void run() {
            try {
                barrier.await();
                for (int i = 0; i < iterations; i++) {
                    lock.readLock().lock();
                    try {
                        int n = activity.incrementAndGet();
                        if (n < 1 || n >= WRITER_ACTIVITY_NUM) {
                            throw new IllegalStateException("reader lock: " + n);
                        }
                        LockSupport.parkNanos(10);
                        activity.decrementAndGet();
                    } finally {
                        lock.readLock().unlock();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                latch.countDown();
            }
        }
    }

    protected static class Writer extends Thread {

        private final ReadWriteLock lock;
        private final CyclicBarrier barrier;
        private final CountDownLatch latch;
        private final AtomicInteger activity;
        private final int iterations;

        private Writer(ReadWriteLock lock, CyclicBarrier barrier, CountDownLatch latch, AtomicInteger activity, int iterations) {
            this.lock = lock;
            this.barrier = barrier;
            this.latch = latch;
            this.activity = activity;
            this.iterations = iterations;
        }

        @Override
        public void run() {
            try {
                barrier.await();
                for (int i = 0; i < iterations; i++) {
                    lock.writeLock().lock();
                    try {
                        int n = activity.addAndGet(WRITER_ACTIVITY_NUM);
                        if (n != WRITER_ACTIVITY_NUM) {
                            throw new IllegalStateException("writer lock: " + n);
                        }
                        LockSupport.parkNanos(10);
                        activity.addAndGet(-WRITER_ACTIVITY_NUM);
                    } finally {
                        lock.writeLock().unlock();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                latch.countDown();
            }
        }
    }
}
