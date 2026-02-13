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
import java.util.concurrent.atomic.AtomicReference;

public class WalLockerFuzzTest {

    private TableToken concurrentTestTable;
    private WalLocker locker;

    @Before
    public void setUp() {
        locker = new QdbrWalLocker();
        concurrentTestTable = createToken("concurrent_test");
    }

    @After
    public void tearDown() {
        locker.close();
    }

    @Test
    public void testFuzz() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(null);
        final int numThreads = 8;
        final int numTables = 3;
        final int numWalsPerTable = 4;
        final int operationsPerThread = 200000;

        // Track state per (table, wal) pair
        // States: 0=unlocked, 1=writer, 2=purge, 3=writer+purge
        final int[][] walStates = new int[numTables][numWalsPerTable];
        final int[][] minSegmentIds = new int[numTables][numWalsPerTable];
        final Object[][] walLocks = new Object[numTables][numWalsPerTable];

        for (int t = 0; t < numTables; t++) {
            for (int w = 0; w < numWalsPerTable; w++) {
                walLocks[t][w] = new Object();
            }
        }

        final TableToken[] tables = new TableToken[numTables];
        for (int t = 0; t < numTables; t++) {
            tables[t] = createToken("fuzz_table_" + t);
        }

        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch doneLatch = new CountDownLatch(numThreads);
        final AtomicReference<Throwable> error = new AtomicReference<>();

        for (int threadIdx = 0; threadIdx < numThreads; threadIdx++) {
            final int threadId = threadIdx;
            new Thread(() -> {
                try {
                    startLatch.await();
                    Rnd threadRnd = new Rnd(rnd.nextLong(), rnd.nextLong());

                    for (int op = 0; op < operationsPerThread && error.get() == null; op++) {
                        int tableIdx = threadRnd.nextInt(numTables);
                        int walId = threadRnd.nextInt(numWalsPerTable);
                        TableToken table = tables[tableIdx];

                        synchronized (walLocks[tableIdx][walId]) {
                            int currentState = walStates[tableIdx][walId];

                            // Choose an operation based on current state
                            switch (currentState) {
                                case 0: // Unlocked - can lock writer or purge
                                    if (threadRnd.nextBoolean()) {
                                        // Lock writer
                                        int minSeg = threadRnd.nextInt(100);
                                        locker.lockWriter(table, walId, minSeg);
                                        walStates[tableIdx][walId] = 1;
                                        minSegmentIds[tableIdx][walId] = minSeg;

                                        // Verify segment locking
                                        Assert.assertTrue("Thread " + threadId + ": Segment " + minSeg + " should be locked",
                                                locker.isSegmentLocked(table, walId, minSeg));
                                        if (minSeg > 0) {
                                            Assert.assertFalse("Thread " + threadId + ": Segment " + (minSeg - 1) + " should not be locked",
                                                    locker.isSegmentLocked(table, walId, minSeg - 1));
                                        }
                                    } else {
                                        // Lock purge
                                        int maxPurgeable = locker.lockPurge(table, walId);
                                        Assert.assertEquals(WalUtils.SEG_NONE_ID, maxPurgeable);
                                        walStates[tableIdx][walId] = 2;
                                    }
                                    Assert.assertTrue(locker.isWalLocked(table, walId));
                                    break;

                                case 1: // Writer locked - can unlock writer, attach purge, or advance segment
                                    int action1 = threadRnd.nextInt(3);
                                    if (action1 == 0) {
                                        // Unlock writer
                                        locker.unlockWriter(table, walId);
                                        walStates[tableIdx][walId] = 0;
                                        Assert.assertFalse(locker.isWalLocked(table, walId));
                                    } else if (action1 == 1) {
                                        // Attach purge
                                        int maxPurgeable = locker.lockPurge(table, walId);
                                        Assert.assertEquals(minSegmentIds[tableIdx][walId] - 1, maxPurgeable);
                                        walStates[tableIdx][walId] = 3;
                                    } else {
                                        // Advance segment
                                        int currentMin = minSegmentIds[tableIdx][walId];
                                        int newMin = currentMin + threadRnd.nextInt(10) + 1;
                                        locker.setWalSegmentMinId(table, walId, newMin);
                                        minSegmentIds[tableIdx][walId] = newMin;

                                        Assert.assertTrue(locker.isSegmentLocked(table, walId, newMin));
                                        Assert.assertFalse(locker.isSegmentLocked(table, walId, currentMin));
                                    }
                                    break;

                                case 2: // Purge locked - can only unlock purge
                                    locker.unlockPurge(table, walId);
                                    walStates[tableIdx][walId] = 0;
                                    Assert.assertFalse(locker.isWalLocked(table, walId));
                                    break;

                                case 3: // Both writer and purge - can unlock either
                                    if (threadRnd.nextBoolean()) {
                                        // Unlock writer first
                                        locker.unlockWriter(table, walId);
                                        walStates[tableIdx][walId] = 2;
                                        Assert.assertTrue(locker.isWalLocked(table, walId));
                                    } else {
                                        // Unlock purge first
                                        locker.unlockPurge(table, walId);
                                        walStates[tableIdx][walId] = 1;
                                        Assert.assertTrue(locker.isWalLocked(table, walId));
                                    }
                                    break;
                            }
                        }

                        // Occasional yield to increase interleaving
                        if (threadRnd.nextInt(10) == 0) {
                            Thread.yield();
                        }
                    }
                } catch (Throwable t) {
                    error.compareAndSet(null, t);
                } finally {
                    doneLatch.countDown();
                }
            }, "FuzzThread-" + threadIdx).start();
        }

        // Start all threads
        startLatch.countDown();

        // Wait for completion
        Assert.assertTrue("Fuzz test timed out", doneLatch.await(30, TimeUnit.SECONDS));

        if (error.get() != null) {
            error.get().printStackTrace();
            Assert.fail("Fuzz test failed: " + error.get().getMessage());
        }

        // Clean up any remaining locks
        for (int t = 0; t < numTables; t++) {
            for (int w = 0; w < numWalsPerTable; w++) {
                synchronized (walLocks[t][w]) {
                    int state = walStates[t][w];
                    if (state == 3) {
                        locker.unlockPurge(tables[t], w);
                        locker.unlockWriter(tables[t], w);
                    } else if (state == 2) {
                        locker.unlockPurge(tables[t], w);
                    } else if (state == 1) {
                        locker.unlockWriter(tables[t], w);
                    }
                }
            }
        }

        // Verify all locks released
        for (int t = 0; t < numTables; t++) {
            for (int w = 0; w < numWalsPerTable; w++) {
                Assert.assertFalse("Lock should be released for " + tables[t] + " wal " + w,
                        locker.isWalLocked(tables[t], w));
            }
        }
    }

    @Test
    public void testFuzzConcurrentWriterPurgeSameWal() throws Exception {
        // This fuzz test specifically targets the writer-purge interaction on the same WAL
        // with one thread acting as a writer and another as purge
        final Rnd rnd = TestUtils.generateRandom(null);
        final int iterations = 50;
        final AtomicReference<Throwable> error = new AtomicReference<>();

        for (int iter = 0; iter < iterations && error.get() == null; iter++) {
            final int minSegment = rnd.nextInt(100);
            final CountDownLatch ready = new CountDownLatch(2);
            final CountDownLatch done = new CountDownLatch(2);
            final CyclicBarrier barrier = new CyclicBarrier(2);

            // Writer thread
            Thread writer = new Thread(() -> {
                try {
                    ready.countDown();
                    barrier.await();

                    locker.lockWriter(concurrentTestTable, 1, minSegment);

                    // Random work
                    for (int i = 0; i < 5; i++) {
                        Thread.yield();
                    }

                    locker.unlockWriter(concurrentTestTable, 1);
                } catch (Throwable t) {
                    error.compareAndSet(null, t);
                } finally {
                    done.countDown();
                }
            });

            // Purge thread
            Thread purge = new Thread(() -> {
                try {
                    ready.countDown();
                    barrier.await();

                    int maxPurgeable = locker.lockPurge(concurrentTestTable, 1);
                    // Should be either SEG_NONE_ID (exclusive) or minSegment-1 (shared with writer)
                    Assert.assertTrue("maxPurgeable should be valid",
                            maxPurgeable == WalUtils.SEG_NONE_ID || maxPurgeable == minSegment - 1);

                    // Random work
                    for (int i = 0; i < 5; i++) {
                        Thread.yield();
                    }

                    locker.unlockPurge(concurrentTestTable, 1);
                } catch (Throwable t) {
                    error.compareAndSet(null, t);
                } finally {
                    done.countDown();
                }
            });

            writer.start();
            purge.start();

            Assert.assertTrue("Iteration " + iter + " threads didn't start", ready.await(5, TimeUnit.SECONDS));
            Assert.assertTrue("Iteration " + iter + " timed out", done.await(5, TimeUnit.SECONDS));

            if (error.get() != null) {
                error.get().printStackTrace();
                Assert.fail("Iteration " + iter + " failed: " + error.get().getMessage());
            }

            Assert.assertFalse("WAL should be unlocked after iteration " + iter,
                    locker.isWalLocked(concurrentTestTable, 1));
        }
    }

    private TableToken createToken(String name) {
        return new TableToken(name, name, null, 1, true, false, true);
    }
}
