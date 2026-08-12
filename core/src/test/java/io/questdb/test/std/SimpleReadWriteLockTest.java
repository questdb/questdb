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

package io.questdb.test.std;

import io.questdb.std.Os;
import io.questdb.std.SimpleReadWriteLock;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;

public class SimpleReadWriteLockTest {

    protected static final int WRITER_ACTIVITY_NUM = 100_000;

    @Test
    public void testHammerLockMultipleReaderMultipleWriter() throws Exception {
        testHammerLock(4, 4, 1000);
    }

    @Test
    public void testHammerLockMultipleReaderSingleWriter() throws Exception {
        testHammerLock(4, 1, 1000);
    }

    @Test
    public void testHammerLockSingleReaderSingleWriter() throws Exception {
        testHammerLock(1, 1, 1000);
    }

    @Test
    public void testHammerTryLockMultipleReaderMultipleWriter() throws Exception {
        testHammerTryLock(4, 4, 1000);
    }

    @Test
    public void testHammerTryLockMultipleReaderSingleWriter() throws Exception {
        testHammerTryLock(4, 1, 1000);
    }

    @Test
    public void testHammerTryLockSingleReaderSingleWriter() throws Exception {
        testHammerTryLock(1, 1, 1000);
    }

    /**
     * The property that matters: a timed write acquire must GIVE UP while a reader is resident,
     * rather than spinning until the reader leaves. Without a bound, a caller on a borrowed thread
     * is pinned for as long as the reader holds.
     */
    @Test
    public void testTimedWriteTryLockGivesUpWhileReaderResident() throws Exception {
        final SimpleReadWriteLock lock = new SimpleReadWriteLock();
        lock.readLock().lock();
        try {
            final long t0 = System.nanoTime();
            Assert.assertFalse(lock.writeLock().tryLock(200, TimeUnit.MILLISECONDS));
            final long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
            Assert.assertTrue("must wait at least the timeout, waited " + elapsedMs + "ms", elapsedMs >= 150);
            Assert.assertTrue("must not wait unboundedly, waited " + elapsedMs + "ms", elapsedMs < 10_000);

            // Negative control: a failed timed acquire must roll back, leaving the lock usable.
            // A new reader can still enter, which proves the reader bias was undone.
            Assert.assertTrue(lock.readLock().tryLock());
            lock.readLock().unlock();
            Assert.assertFalse("write lock must not be held after a failed timed acquire", lock.isWriteLocked());
        } finally {
            lock.readLock().unlock();
        }
        // With no readers left the same call must now succeed.
        Assert.assertTrue(lock.writeLock().tryLock(200, TimeUnit.MILLISECONDS));
        lock.writeLock().unlock();
    }

    @Test
    public void testTimedWriteTryLockSucceedsWhenReaderLeavesInTime() throws Exception {
        final SimpleReadWriteLock lock = new SimpleReadWriteLock();
        lock.readLock().lock();
        final Thread releaser = new Thread(() -> {
            Os.sleep(100);
            lock.readLock().unlock();
        }, "reader-releaser");
        releaser.start();
        try {
            Assert.assertTrue(lock.writeLock().tryLock(30, TimeUnit.SECONDS));
            lock.writeLock().unlock();
        } finally {
            releaser.join();
        }
    }

    @Test
    public void testReadTryLockFailsWhileWriteHeld() {
        final SimpleReadWriteLock lock = new SimpleReadWriteLock();
        lock.writeLock().lock();
        try {
            Assert.assertFalse(lock.readLock().tryLock());
        } finally {
            lock.writeLock().unlock();
        }
        // Released: the reader must get in now, and the count must not have been corrupted by the
        // failed attempt above.
        Assert.assertTrue(lock.readLock().tryLock());
        lock.readLock().unlock();
        Assert.assertTrue(lock.writeLock().tryLock());
        lock.writeLock().unlock();
    }

    @Test
    public void testSerialReadLock() {
        final SimpleReadWriteLock lock = new SimpleReadWriteLock();
        for (int i = 0; i < 32; i++) {
            lock.readLock().lock();
            lock.readLock().unlock();
        }
    }

    @Test
    public void testSerialWriteLock() {
        final SimpleReadWriteLock lock = new SimpleReadWriteLock();
        for (int i = 0; i < 32; i++) {
            lock.writeLock().lock();
            lock.writeLock().unlock();
        }
    }

    @Test
    public void testSerialWriteTryLock() {
        final SimpleReadWriteLock lock = new SimpleReadWriteLock();
        for (int i = 0; i < 32; i++) {
            Assert.assertTrue(lock.writeLock().tryLock());
            lock.writeLock().unlock();
        }
    }

    private void testHammerLock(int readers, int writers, int iterations) throws Exception {
        final SimpleReadWriteLock lock = new SimpleReadWriteLock();
        final CyclicBarrier barrier = new CyclicBarrier(readers + writers);
        final CountDownLatch latch = new CountDownLatch(readers + writers);
        final AtomicInteger activity = new AtomicInteger();

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

    private void testHammerTryLock(int readers, int writers, int iterations) throws Exception {
        final SimpleReadWriteLock lock = new SimpleReadWriteLock();
        final CyclicBarrier barrier = new CyclicBarrier(readers + writers);
        final CountDownLatch latch = new CountDownLatch(readers + writers);
        final AtomicInteger activity = new AtomicInteger();

        for (int i = 0; i < readers; i++) {
            Reader reader = new Reader(lock, barrier, latch, activity, iterations);
            reader.start();
        }

        for (int i = 0; i < writers; i++) {
            TryWriter writer = new TryWriter(lock, barrier, latch, activity, iterations);
            writer.start();
        }

        latch.await();

        Assert.assertEquals(0, activity.get());
    }

    private static class Reader extends Thread {

        private final AtomicInteger activity;
        private final CyclicBarrier barrier;
        private final int iterations;
        private final CountDownLatch latch;
        private final ReadWriteLock lock;

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
                        Os.pause();
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

    private static class TryWriter extends Thread {

        private final AtomicInteger activity;
        private final CyclicBarrier barrier;
        private final int iterations;
        private final CountDownLatch latch;
        private final ReadWriteLock lock;

        private TryWriter(ReadWriteLock lock, CyclicBarrier barrier, CountDownLatch latch, AtomicInteger activity, int iterations) {
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
                    if (lock.writeLock().tryLock()) {
                        try {
                            int n = activity.addAndGet(WRITER_ACTIVITY_NUM);
                            if (n != WRITER_ACTIVITY_NUM) {
                                throw new IllegalStateException("writer lock: " + n);
                            }
                            Os.pause();
                            activity.addAndGet(-WRITER_ACTIVITY_NUM);
                        } finally {
                            lock.writeLock().unlock();
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                latch.countDown();
            }
        }
    }

    private static class Writer extends Thread {

        private final AtomicInteger activity;
        private final CyclicBarrier barrier;
        private final int iterations;
        private final CountDownLatch latch;
        private final ReadWriteLock lock;

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
                        Os.pause();
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
