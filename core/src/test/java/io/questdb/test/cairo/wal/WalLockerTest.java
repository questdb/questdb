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
import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.QdbrWalLocker;
import io.questdb.cairo.wal.WalLocker;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.std.Rnd;
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

public class WalLockerTest {

    private WalLocker locker;
    private TableToken table1;
    private TableToken table2;
    private TableToken table3;

    @Before
    public void setUp() {
        locker = new QdbrWalLocker();
        table1 = new TableToken("table1", "table1", null, 1, false, false, false);
        table2 = new TableToken("table2", "table2", null, 2, false, false, false);
        table3 = new TableToken("table3", "table3", null, 3, false, false, false);
    }

    @After
    public void tearDown() {
        if (locker != null) {
            locker.close();
        }
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
                    locker.lockWriter(table1, 1, minSegmentId);
                    Thread.sleep(1);
                    locker.unlockWriter(table1, 1);
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
                    int maxPurgeable = locker.lockPurge(table1, 1);
                    Assert.assertTrue(maxPurgeable == WalUtils.SEG_NONE_ID || maxPurgeable == (minSegmentId - 1));
                    Thread.sleep(1);
                    locker.unlockPurge(table1, 1);
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
            Assert.assertFalse(locker.isWalLocked(table1, 1));
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
                    locker.lockWriter(table1, walId, 0);
                    Thread.sleep(10);
                    locker.unlockWriter(table1, walId);
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
        locker.lockPurge(table1, 1);

        try {
            locker.lockPurge(table1, 1);
            Assert.fail("Expected CairoException");
        } catch (CairoException e) {
            TestUtils.assertContains(e.getMessage(), "cannot acquire purge lock: already exists");
        }

        locker.unlockPurge(table1, 1);
    }

    @Test
    public void testDoubleWriterLockThrows() {
        locker.lockWriter(table1, 1, 0);

        try {
            locker.lockWriter(table1, 1, 5);
            Assert.fail("Expected CairoException");
        } catch (CairoException e) {
            TestUtils.assertContains(e.getMessage(), "cannot acquire writer lock: already exists");
        }

        locker.unlockWriter(table1, 1);
    }

    @Test
    public void testMultipleTables() {
        locker.lockWriter(table1, 1, 0);
        locker.lockWriter(table2, 1, 0);
        locker.lockPurge(table3, 1);

        Assert.assertTrue(locker.isWalLocked(table1, 1));
        Assert.assertTrue(locker.isWalLocked(table2, 1));
        Assert.assertTrue(locker.isWalLocked(table3, 1));

        locker.unlockWriter(table1, 1);
        Assert.assertFalse(locker.isWalLocked(table1, 1));
        Assert.assertTrue(locker.isWalLocked(table2, 1));
        Assert.assertTrue(locker.isWalLocked(table3, 1));

        locker.unlockWriter(table2, 1);
        locker.unlockPurge(table3, 1);
    }

    @Test
    public void testMultipleWalsSameTable() {
        locker.lockWriter(table1, 1, 0);
        locker.lockWriter(table1, 2, 5);
        locker.lockPurge(table1, 3);

        Assert.assertTrue(locker.isWalLocked(table1, 1));
        Assert.assertTrue(locker.isWalLocked(table1, 2));
        Assert.assertTrue(locker.isWalLocked(table1, 3));

        locker.unlockWriter(table1, 1);
        Assert.assertFalse(locker.isWalLocked(table1, 1));
        Assert.assertTrue(locker.isWalLocked(table1, 2));

        locker.unlockWriter(table1, 2);
        locker.unlockPurge(table1, 3);
    }

    @Test
    public void testPurgeLockOnWriterPurgeStateThrows() {
        // Writer locks first
        locker.lockWriter(table1, 1, 0);
        // Purge attaches (now in WRITER_PURGE state)
        locker.lockPurge(table1, 1);

        try {
            // Another purge tries to attach - should fail
            locker.lockPurge(table1, 1);
            Assert.fail("Expected CairoException");
        } catch (CairoException e) {
            TestUtils.assertContains(e.getMessage(), "cannot acquire purge lock: already exists");
        }

        locker.unlockPurge(table1, 1);
        locker.unlockWriter(table1, 1);
    }

    @Test
    public void testPurgeLockUnlock() {
        int maxPurgeableSegment = locker.lockPurge(table1, 1);

        Assert.assertEquals(WalUtils.SEG_NONE_ID, maxPurgeableSegment);
        Assert.assertTrue(locker.isWalLocked(table1, 1));

        locker.unlockPurge(table1, 1);

        Assert.assertFalse(locker.isWalLocked(table1, 1));
    }

    @Test
    public void testPurgeThenWriterBlocksUntilPurgeUnlocks() throws Exception {
        // Purge locks first
        int maxPurgeableSegment = locker.lockPurge(table1, 1);
        Assert.assertEquals(WalUtils.SEG_NONE_ID, maxPurgeableSegment);

        AtomicBoolean writerAcquired = new AtomicBoolean(false);
        CountDownLatch writerStarted = new CountDownLatch(1);
        CountDownLatch writerFinished = new CountDownLatch(1);

        // Writer tries to lock in another thread, should block
        Thread writerThread = new Thread(() -> {
            writerStarted.countDown();
            locker.lockWriter(table1, 1, 3);
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
        locker.unlockPurge(table1, 1);

        // Writer should now acquire the lock
        Assert.assertTrue(writerFinished.await(1, TimeUnit.SECONDS));
        Assert.assertTrue(writerAcquired.get());
        Assert.assertTrue(locker.isWalLocked(table1, 1));

        // Clean up
        locker.unlockWriter(table1, 1);
        writerThread.join(1000);
    }

    @Test
    public void testPurgeThenWriterThenPurgeUnlocksFirst() throws Exception {
        // Purge locks first (exclusive)
        locker.lockPurge(table1, 1);

        CountDownLatch writerAcquired = new CountDownLatch(1);
        AtomicInteger writerMinSegment = new AtomicInteger(-1);

        // Writer tries to lock, will block
        Thread writerThread = new Thread(() -> {
            locker.lockWriter(table1, 1, 7);
            writerMinSegment.set(7);
            writerAcquired.countDown();
        });
        writerThread.start();
        Thread.sleep(100);

        Assert.assertEquals(-1, writerMinSegment.get());

        // Purge unlocks, writer should acquire with its minSegment
        locker.unlockPurge(table1, 1);

        Assert.assertTrue(writerAcquired.await(1, TimeUnit.SECONDS));

        // Verify segment tracking
        Assert.assertTrue(locker.isSegmentLocked(table1, 1, 7));
        Assert.assertFalse(locker.isSegmentLocked(table1, 1, 6));

        locker.unlockWriter(table1, 1);
        writerThread.join(1000);
    }

    @Test
    public void testReset() {
        locker.lockWriter(table1, 1, 0);
        locker.lockWriter(table2, 1, 0);

        Assert.assertTrue(locker.isWalLocked(table1, 1));
        Assert.assertTrue(locker.isWalLocked(table2, 1));

        locker.clear();

        Assert.assertFalse(locker.isWalLocked(table1, 1));
        Assert.assertFalse(locker.isWalLocked(table2, 1));
    }

    @Test
    public void testSetWalSegmentMinIdAffectsPurgeLock() {
        // Writer locks with minSegment = 5
        locker.lockWriter(table1, 1, 5);

        // Purge attaches, max purgeable = 4
        int maxPurgeable1 = locker.lockPurge(table1, 1);
        Assert.assertEquals(4, maxPurgeable1);

        // Writer advances to segment 10
        locker.setWalSegmentMinId(table1, 1, 10);

        // Unlock purge and re-lock to see updated value
        locker.unlockPurge(table1, 1);
        int maxPurgeable2 = locker.lockPurge(table1, 1);
        Assert.assertEquals(9, maxPurgeable2);

        locker.unlockPurge(table1, 1);
        locker.unlockWriter(table1, 1);
    }

    @Test
    public void testSetWalSegmentMinIdDecreaseThrows() {
        locker.lockWriter(table1, 1, 10);

        try {
            locker.setWalSegmentMinId(table1, 1, 5);
            Assert.fail("Expected CairoException");
        } catch (CairoException e) {
            Assert.assertTrue(e.getMessage().contains("new minSegmentId must be >= current"));
        }

        locker.unlockWriter(table1, 1);
    }

    @Test
    public void testSetWalSegmentMinIdIncrease() {
        locker.lockWriter(table1, 1, 0);

        // Initially all segments from 0 are locked
        Assert.assertTrue(locker.isSegmentLocked(table1, 1, 0));

        // Advance minSegmentId to 5
        locker.setWalSegmentMinId(table1, 1, 5);

        // Now only segments >= 5 are locked
        Assert.assertFalse(locker.isSegmentLocked(table1, 1, 4));
        Assert.assertTrue(locker.isSegmentLocked(table1, 1, 5));

        locker.unlockWriter(table1, 1);
    }

    @Test
    public void testSetWalSegmentMinIdOnNonExistentWal() {
        // Should be a no-op, not throw
        locker.setWalSegmentMinId(table1, 1, 10);

        // WAL should not be locked
        Assert.assertFalse(locker.isWalLocked(table1, 1));
    }

    @Test
    public void testUnlockPurgeOnActiveWriterOnlyThrows() {
        // Only writer locks (no purge)
        locker.lockWriter(table1, 1, 0);

        try {
            // Try to unlock purge when only writer has the lock - should fail
            locker.unlockPurge(table1, 1);
            Assert.fail("Expected CairoException");
        } catch (CairoException e) {
            TestUtils.assertContains(e.getMessage(), "cannot release purge lock: invalid state");
        }

        // Clean up
        locker.unlockWriter(table1, 1);
    }

    @Test
    public void testUnlockWriterOnPurgeExclusiveThrows() {
        // Only purge locks (no writer)
        locker.lockPurge(table1, 1);

        try {
            // Try to unlock writer when only purge has the lock - should fail
            locker.unlockWriter(table1, 1);
            Assert.fail("Expected CairoException");
        } catch (CairoException e) {
            TestUtils.assertContains(e.getMessage(), "cannot release writer lock: invalid state");
        }

        // Clean up
        locker.unlockPurge(table1, 1);
    }

    @Test
    public void testWriterLockUnlock() {
        locker.lockWriter(table1, 1, 0);

        Assert.assertTrue(locker.isWalLocked(table1, 1));
        Assert.assertTrue(locker.isSegmentLocked(table1, 1, 0));
        Assert.assertTrue(locker.isSegmentLocked(table1, 1, 1));
        Assert.assertFalse(locker.isSegmentLocked(table1, 1, -1));

        locker.unlockWriter(table1, 1);

        Assert.assertFalse(locker.isWalLocked(table1, 1));
    }

    @Test
    public void testWriterLockWithHighMinSegment() {
        locker.lockWriter(table1, 1, 5);

        Assert.assertTrue(locker.isWalLocked(table1, 1));
        Assert.assertFalse(locker.isSegmentLocked(table1, 1, 4));
        Assert.assertTrue(locker.isSegmentLocked(table1, 1, 5));
        Assert.assertTrue(locker.isSegmentLocked(table1, 1, 6));

        locker.unlockWriter(table1, 1);
    }

    @Test
    public void testWriterThenPurgeSharedMode() {
        // Writer locks first with minSegment = 5
        locker.lockWriter(table1, 1, 5);

        // Purge attaches, should return minSegment - 1 = 4
        int maxPurgeableSegment = locker.lockPurge(table1, 1);
        Assert.assertEquals(4, maxPurgeableSegment);

        // Both are sharing the lock
        Assert.assertTrue(locker.isWalLocked(table1, 1));

        // Purge unlocks first, writer should still hold the lock
        locker.unlockPurge(table1, 1);
        Assert.assertTrue(locker.isWalLocked(table1, 1));

        // Writer unlocks, lock should be released
        locker.unlockWriter(table1, 1);
        Assert.assertFalse(locker.isWalLocked(table1, 1));
    }

    @Test
    public void testWriterThenPurgeWriterUnlocksFirst() {
        // Writer locks first with minSegment = 10
        locker.lockWriter(table1, 1, 10);

        // Purge attaches
        int maxPurgeableSegment = locker.lockPurge(table1, 1);
        Assert.assertEquals(9, maxPurgeableSegment);

        // Writer unlocks first, purge should still hold the lock
        locker.unlockWriter(table1, 1);
        Assert.assertTrue(locker.isWalLocked(table1, 1));

        // Purge unlocks, lock should be released
        locker.unlockPurge(table1, 1);
        Assert.assertFalse(locker.isWalLocked(table1, 1));
    }

    @Test
    public void testWriterWaitsForPurgeThenProceedsWithCorrectSegment() throws Exception {
        // This tests the WRITER_ACQUIRING -> ACTIVE_WRITER transition
        locker.lockPurge(table1, 1);

        CountDownLatch writerAcquired = new CountDownLatch(1);
        AtomicBoolean segmentCorrect = new AtomicBoolean(false);

        Thread writerThread = new Thread(() -> {
            locker.lockWriter(table1, 1, 42);
            // After acquiring, verify segment is correctly set
            segmentCorrect.set(locker.isSegmentLocked(table1, 1, 42));
            writerAcquired.countDown();
        });
        writerThread.start();

        Thread.sleep(50);
        locker.unlockPurge(table1, 1);

        Assert.assertTrue(writerAcquired.await(1, TimeUnit.SECONDS));
        Assert.assertTrue(segmentCorrect.get());
        Assert.assertFalse(locker.isSegmentLocked(table1, 1, 41));

        locker.unlockWriter(table1, 1);
        writerThread.join(1000);
    }
}
