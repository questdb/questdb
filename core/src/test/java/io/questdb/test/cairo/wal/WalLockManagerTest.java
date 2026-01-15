/*******************************************************************************
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

package io.questdb.test.cairo.wal;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.wal.WalLockManager;
import io.questdb.cairo.wal.WalLocker;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.std.Rnd;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class WalLockManagerTest {

    private WalLockManager lockManager;
    private DirectUtf8Sink table1;
    private DirectUtf8Sink table2;
    private DirectUtf8Sink table3;

    @Before
    public void setUp() {
        lockManager = new WalLockManager(new WalLocker());
        table1 = new DirectUtf8Sink(16);
        table1.putAscii("table1");
        table2 = new DirectUtf8Sink(16);
        table2.putAscii("table2");
        table3 = new DirectUtf8Sink(16);
        table3.putAscii("table3");
    }

    @After
    public void tearDown() {
        if (lockManager != null) {
            lockManager.close();
        }
        table1.close();
        table2.close();
        table3.close();
    }

    @Test
    public void testConcurrentWriterAndPurgeSameWal() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(null);
        int iterations = 100;
        AtomicReference<Throwable> error = new AtomicReference<>();

        for (int i = 0; i < iterations; i++) {
            CountDownLatch done = new CountDownLatch(2);

            final int minSegmentId = rnd.nextInt(WalUtils.SEG_MAX_ID + 1);

            // Writer thread
            Thread writer = new Thread(() -> {
                try {
                    lockManager.lockWriter(table1, 1, minSegmentId);
                    Thread.sleep(1);
                    lockManager.unlockWriter(table1, 1);
                } catch (Throwable t) {
                    error.compareAndSet(null, t);
                } finally {
                    done.countDown();
                }
            });

            // Purge thread - may run before or after writer
            Thread purge = new Thread(() -> {
                try {
                    Thread.sleep(1); // Small delay to increase chance of interleaving
                    int maxPurgeable = lockManager.lockPurge(table1, 1);
                    Assert.assertTrue(maxPurgeable == WalUtils.SEG_NONE_ID || maxPurgeable == (minSegmentId - 1));
                    Thread.sleep(1);
                    lockManager.unlockPurge(table1, 1);
                } catch (Throwable t) {
                    error.compareAndSet(null, t);
                } finally {
                    done.countDown();
                }
            });

            writer.start();
            purge.start();

            Assert.assertTrue("Iteration " + i + " timed out", done.await(5, TimeUnit.SECONDS));

            if (error.get() != null) {
                error.get().printStackTrace();
                Assert.fail("Iteration " + i + " failed: " + error.get().getMessage());
            }

            // Ensure clean state for next iteration
            Assert.assertFalse(lockManager.isWalLocked(table1, 1));
        }
    }

    @Test
    public void testConcurrentWritersOnDifferentWals() throws Exception {
        int numThreads = 10;
        CyclicBarrier barrier = new CyclicBarrier(numThreads);
        CountDownLatch done = new CountDownLatch(numThreads);
        AtomicReference<Throwable> error = new AtomicReference<>();

        for (int i = 0; i < numThreads; i++) {
            final int walId = i;
            new Thread(() -> {
                try {
                    barrier.await();
                    lockManager.lockWriter(table1, walId, 0);
                    Thread.sleep(10);
                    lockManager.unlockWriter(table1, walId);
                } catch (Throwable t) {
                    error.compareAndSet(null, t);
                } finally {
                    done.countDown();
                }
            }).start();
        }

        Assert.assertTrue(done.await(5, TimeUnit.SECONDS));
        Assert.assertNull(error.get());
    }

    @Test
    public void testDoublePurgeLockThrows() {
        lockManager.lockPurge(table1, 1);

        try {
            lockManager.lockPurge(table1, 1);
            Assert.fail("Expected CairoException");
        } catch (CairoException e) {
            TestUtils.assertContains(e.getMessage(), "cannot acquire purge lock: already exists");
        }

        lockManager.unlockPurge(table1, 1);
    }

    @Test
    public void testDoubleWriterLockThrows() {
        lockManager.lockWriter(table1, 1, 0);

        try {
            lockManager.lockWriter(table1, 1, 5);
            Assert.fail("Expected CairoException");
        } catch (CairoException e) {
            TestUtils.assertContains(e.getMessage(), "cannot acquire writer lock: already exists");
        }

        lockManager.unlockWriter(table1, 1);
    }

    @Test
    public void testMultipleTables() {
        lockManager.lockWriter(table1, 1, 0);
        lockManager.lockWriter(table2, 1, 0);
        lockManager.lockPurge(table3, 1);

        Assert.assertTrue(lockManager.isWalLocked(table1, 1));
        Assert.assertTrue(lockManager.isWalLocked(table2, 1));
        Assert.assertTrue(lockManager.isWalLocked(table3, 1));

        lockManager.unlockWriter(table1, 1);
        Assert.assertFalse(lockManager.isWalLocked(table1, 1));
        Assert.assertTrue(lockManager.isWalLocked(table2, 1));
        Assert.assertTrue(lockManager.isWalLocked(table3, 1));

        lockManager.unlockWriter(table2, 1);
        lockManager.unlockPurge(table3, 1);
    }

    @Test
    public void testMultipleWalsSameTable() {
        lockManager.lockWriter(table1, 1, 0);
        lockManager.lockWriter(table1, 2, 5);
        lockManager.lockPurge(table1, 3);

        Assert.assertTrue(lockManager.isWalLocked(table1, 1));
        Assert.assertTrue(lockManager.isWalLocked(table1, 2));
        Assert.assertTrue(lockManager.isWalLocked(table1, 3));

        lockManager.unlockWriter(table1, 1);
        Assert.assertFalse(lockManager.isWalLocked(table1, 1));
        Assert.assertTrue(lockManager.isWalLocked(table1, 2));

        lockManager.unlockWriter(table1, 2);
        lockManager.unlockPurge(table1, 3);
    }

    @Test
    public void testPurgeLockOnWriterPurgeStateThrows() {
        // Writer locks first
        lockManager.lockWriter(table1, 1, 0);
        // Purge attaches (now in WRITER_PURGE state)
        lockManager.lockPurge(table1, 1);

        try {
            // Another purge tries to attach - should fail
            lockManager.lockPurge(table1, 1);
            Assert.fail("Expected CairoException");
        } catch (CairoException e) {
            TestUtils.assertContains(e.getMessage(), "cannot acquire purge lock: already exists");
        }

        lockManager.unlockPurge(table1, 1);
        lockManager.unlockWriter(table1, 1);
    }

    @Test
    public void testPurgeLockUnlock() {
        int maxPurgeableSegment = lockManager.lockPurge(table1, 1);

        Assert.assertEquals(WalUtils.SEG_NONE_ID, maxPurgeableSegment);
        Assert.assertTrue(lockManager.isWalLocked(table1, 1));

        lockManager.unlockPurge(table1, 1);

        Assert.assertFalse(lockManager.isWalLocked(table1, 1));
    }

    @Test
    public void testPurgeThenWriterBlocksUntilPurgeUnlocks() throws Exception {
        // Purge locks first
        int maxPurgeableSegment = lockManager.lockPurge(table1, 1);
        Assert.assertEquals(WalUtils.SEG_NONE_ID, maxPurgeableSegment);

        AtomicBoolean writerAcquired = new AtomicBoolean(false);
        CountDownLatch writerStarted = new CountDownLatch(1);
        CountDownLatch writerFinished = new CountDownLatch(1);

        // Writer tries to lock in another thread, should block
        Thread writerThread = new Thread(() -> {
            writerStarted.countDown();
            lockManager.lockWriter(table1, 1, 3);
            writerAcquired.set(true);
            writerFinished.countDown();
        });
        writerThread.start();

        // Wait for writer thread to start
        Assert.assertTrue(writerStarted.await(1, TimeUnit.SECONDS));
        Thread.sleep(100); // Give writer time to block

        // Writer should still be blocked
        Assert.assertFalse(writerAcquired.get());

        // Purge unlocks
        lockManager.unlockPurge(table1, 1);

        // Writer should now acquire the lock
        Assert.assertTrue(writerFinished.await(1, TimeUnit.SECONDS));
        Assert.assertTrue(writerAcquired.get());
        Assert.assertTrue(lockManager.isWalLocked(table1, 1));

        // Clean up
        lockManager.unlockWriter(table1, 1);
        writerThread.join(1000);
    }

    @Test
    public void testPurgeThenWriterThenPurgeUnlocksFirst() throws Exception {
        // Purge locks first (exclusive)
        lockManager.lockPurge(table1, 1);

        CountDownLatch writerAcquired = new CountDownLatch(1);
        AtomicInteger writerMinSegment = new AtomicInteger(-1);

        // Writer tries to lock, will block
        Thread writerThread = new Thread(() -> {
            lockManager.lockWriter(table1, 1, 7);
            writerMinSegment.set(7);
            writerAcquired.countDown();
        });
        writerThread.start();
        Thread.sleep(100);

        Assert.assertEquals(-1, writerMinSegment.get());

        // Purge unlocks, writer should acquire with its minSegment
        lockManager.unlockPurge(table1, 1);

        Assert.assertTrue(writerAcquired.await(1, TimeUnit.SECONDS));

        // Verify segment tracking
        Assert.assertTrue(lockManager.isSegmentLocked(table1, 1, 7));
        Assert.assertFalse(lockManager.isSegmentLocked(table1, 1, 6));

        lockManager.unlockWriter(table1, 1);
        writerThread.join(1000);
    }

    @Test
    public void testReset() {
        lockManager.lockWriter(table1, 1, 0);
        lockManager.lockWriter(table2, 1, 0);

        Assert.assertTrue(lockManager.isWalLocked(table1, 1));
        Assert.assertTrue(lockManager.isWalLocked(table2, 1));

        lockManager.reset();

        Assert.assertFalse(lockManager.isWalLocked(table1, 1));
        Assert.assertFalse(lockManager.isWalLocked(table2, 1));
    }

    @Test
    public void testSetWalSegmentMinIdAffectsPurgeLock() {
        // Writer locks with minSegment = 5
        lockManager.lockWriter(table1, 1, 5);

        // Purge attaches, max purgeable = 4
        int maxPurgeable1 = lockManager.lockPurge(table1, 1);
        Assert.assertEquals(4, maxPurgeable1);

        // Writer advances to segment 10
        lockManager.setWalSegmentMinId(table1, 1, 10);

        // Unlock purge and re-lock to see updated value
        lockManager.unlockPurge(table1, 1);
        int maxPurgeable2 = lockManager.lockPurge(table1, 1);
        Assert.assertEquals(9, maxPurgeable2);

        lockManager.unlockPurge(table1, 1);
        lockManager.unlockWriter(table1, 1);
    }

    @Test
    public void testSetWalSegmentMinIdDecreaseThrows() {
        lockManager.lockWriter(table1, 1, 10);

        try {
            lockManager.setWalSegmentMinId(table1, 1, 5);
            Assert.fail("Expected CairoException");
        } catch (CairoException e) {
            Assert.assertTrue(e.getMessage().contains("new minSegmentId must be >= current"));
        }

        lockManager.unlockWriter(table1, 1);
    }

    @Test
    public void testSetWalSegmentMinIdIncrease() {
        lockManager.lockWriter(table1, 1, 0);

        // Initially all segments from 0 are locked
        Assert.assertTrue(lockManager.isSegmentLocked(table1, 1, 0));

        // Advance minSegmentId to 5
        lockManager.setWalSegmentMinId(table1, 1, 5);

        // Now only segments >= 5 are locked
        Assert.assertFalse(lockManager.isSegmentLocked(table1, 1, 4));
        Assert.assertTrue(lockManager.isSegmentLocked(table1, 1, 5));

        lockManager.unlockWriter(table1, 1);
    }

    @Test
    public void testSetWalSegmentMinIdOnNonExistentWal() {
        // Should be a no-op, not throw
        lockManager.setWalSegmentMinId(table1, 1, 10);

        // WAL should not be locked
        Assert.assertFalse(lockManager.isWalLocked(table1, 1));
    }

    @Test
    public void testUnlockPurgeOnActiveWriterOnlyThrows() {
        // Only writer locks (no purge)
        lockManager.lockWriter(table1, 1, 0);

        try {
            // Try to unlock purge when only writer has the lock - should fail
            lockManager.unlockPurge(table1, 1);
            Assert.fail("Expected CairoException");
        } catch (CairoException e) {
            TestUtils.assertContains(e.getMessage(), "cannot release purge lock: invalid state");
        }

        // Clean up
        lockManager.unlockWriter(table1, 1);
    }

    @Test
    public void testUnlockWriterOnPurgeExclusiveThrows() {
        // Only purge locks (no writer)
        lockManager.lockPurge(table1, 1);

        try {
            // Try to unlock writer when only purge has the lock - should fail
            lockManager.unlockWriter(table1, 1);
            Assert.fail("Expected CairoException");
        } catch (CairoException e) {
            TestUtils.assertContains(e.getMessage(), "cannot release writer lock: invalid state");
        }

        // Clean up
        lockManager.unlockPurge(table1, 1);
    }

    @Test
    public void testWriterLockUnlock() {
        lockManager.lockWriter(table1, 1, 0);

        Assert.assertTrue(lockManager.isWalLocked(table1, 1));
        Assert.assertTrue(lockManager.isSegmentLocked(table1, 1, 0));
        Assert.assertTrue(lockManager.isSegmentLocked(table1, 1, 1));
        Assert.assertFalse(lockManager.isSegmentLocked(table1, 1, -1));

        lockManager.unlockWriter(table1, 1);

        Assert.assertFalse(lockManager.isWalLocked(table1, 1));
    }

    @Test
    public void testWriterLockWithHighMinSegment() {
        lockManager.lockWriter(table1, 1, 5);

        Assert.assertTrue(lockManager.isWalLocked(table1, 1));
        Assert.assertFalse(lockManager.isSegmentLocked(table1, 1, 4));
        Assert.assertTrue(lockManager.isSegmentLocked(table1, 1, 5));
        Assert.assertTrue(lockManager.isSegmentLocked(table1, 1, 6));

        lockManager.unlockWriter(table1, 1);
    }

    @Test
    public void testWriterThenPurgeSharedMode() {
        // Writer locks first with minSegment = 5
        lockManager.lockWriter(table1, 1, 5);

        // Purge attaches, should return minSegment - 1 = 4
        int maxPurgeableSegment = lockManager.lockPurge(table1, 1);
        Assert.assertEquals(4, maxPurgeableSegment);

        // Both are sharing the lock
        Assert.assertTrue(lockManager.isWalLocked(table1, 1));

        // Purge unlocks first, writer should still hold the lock
        lockManager.unlockPurge(table1, 1);
        Assert.assertTrue(lockManager.isWalLocked(table1, 1));

        // Writer unlocks, lock should be released
        lockManager.unlockWriter(table1, 1);
        Assert.assertFalse(lockManager.isWalLocked(table1, 1));
    }

    @Test
    public void testWriterThenPurgeWriterUnlocksFirst() {
        // Writer locks first with minSegment = 10
        lockManager.lockWriter(table1, 1, 10);

        // Purge attaches
        int maxPurgeableSegment = lockManager.lockPurge(table1, 1);
        Assert.assertEquals(9, maxPurgeableSegment);

        // Writer unlocks first, purge should still hold the lock
        lockManager.unlockWriter(table1, 1);
        Assert.assertTrue(lockManager.isWalLocked(table1, 1));

        // Purge unlocks, lock should be released
        lockManager.unlockPurge(table1, 1);
        Assert.assertFalse(lockManager.isWalLocked(table1, 1));
    }

    @Test
    public void testWriterWaitsForPurgeThenProceedsWithCorrectSegment() throws Exception {
        // This tests the WRITER_ACQUIRING -> ACTIVE_WRITER transition
        lockManager.lockPurge(table1, 1);

        CountDownLatch writerAcquired = new CountDownLatch(1);
        AtomicBoolean segmentCorrect = new AtomicBoolean(false);

        Thread writerThread = new Thread(() -> {
            lockManager.lockWriter(table1, 1, 42);
            // After acquiring, verify segment is correctly set
            segmentCorrect.set(lockManager.isSegmentLocked(table1, 1, 42));
            writerAcquired.countDown();
        });
        writerThread.start();

        Thread.sleep(50);
        lockManager.unlockPurge(table1, 1);

        Assert.assertTrue(writerAcquired.await(1, TimeUnit.SECONDS));
        Assert.assertTrue(segmentCorrect.get());
        Assert.assertFalse(lockManager.isSegmentLocked(table1, 1, 41));

        lockManager.unlockWriter(table1, 1);
        writerThread.join(1000);
    }
}
