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

package io.questdb.test.cairo;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TxnScoreboard;
import io.questdb.cairo.TxnScoreboardPool;
import io.questdb.cairo.TxnScoreboardPoolV2;
import io.questdb.cairo.TxnScoreboardV2;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.Chars;
import io.questdb.std.Numbers;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public class TxnScoreboardTest extends AbstractCairoTest {
    private static volatile long txn;
    private static volatile long writerMin;
    private TxnScoreboardPool scoreboardPoolFactory;
    private TableToken tableToken;


    public TxnScoreboard newTxnScoreboard() {
        return scoreboardPoolFactory.getTxnScoreboard(tableToken);
    }

    @Before
    public void setUp() {
        super.setUp();
        scoreboardPoolFactory = new TxnScoreboardPoolV2(engine.getConfiguration());
        tableToken = createTable(
                new TableModel(engine.getConfiguration(), "x", PartitionBy.DAY)
                        .col("ts", ColumnType.TIMESTAMP)
                        .col("bid", ColumnType.DOUBLE)
                        .timestamp()
        );
    }

    @Test
    public void testBitmapIndexEdgeCase() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            for (int i = 127; i < 130; i++) {
                try (TxnScoreboardV2 scoreboard = new TxnScoreboardV2(i)) {
                    for (int j = 1; j < 88; j++) {
                        scoreboard.acquireTxn(i - j, i * j);
                        Assert.assertFalse(scoreboard.isTxnAvailable(i * j));
                        Assert.assertEquals(1, scoreboard.getActiveReaderCount(i * j));
                        Assert.assertFalse(scoreboard.isRangeAvailable(i * j, i * (j + 1)));
                    }
                    Assert.assertEquals(i, getMin(scoreboard));
                    for (int j = 1; j < 88; j++) {
                        scoreboard.releaseTxn(i - j, i * j);
                        Assert.assertEquals(0, scoreboard.getActiveReaderCount(i * j));
                        Assert.assertTrue(scoreboard.isTxnAvailable(i * j));
                        Assert.assertTrue(scoreboard.isRangeAvailable(i * j, i * (j + 1)));
                    }

                    Assert.assertEquals(-1, getMin(scoreboard));
                }
            }
        });
    }

    @Test
    public void testCannotLockTwice() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TxnScoreboard txnScoreboard = newTxnScoreboard()) {
                Assume.assumeTrue(txnScoreboard.acquireTxn(0, 1));
                Assume.assumeFalse(txnScoreboard.acquireTxn(0, 1));

                Assume.assumeTrue(txnScoreboard.acquireTxn(1, 1));
                txnScoreboard.releaseTxn(0, 1);
                Assume.assumeTrue(txnScoreboard.acquireTxn(0, 1));
            }
            scoreboardPoolFactory.clear();
        });
    }

    @Test
    public void testCheckNoLocksBeforeTxn() {
        final long lastCommittedTxn = 2;
        ff = new TestFilesFacadeImpl();
        try (TxnScoreboard txnScoreboard = newTxnScoreboard()) {
            // Thread A (reader) - grabs read permit
            Assert.assertTrue(txnScoreboard.acquireTxn(0, lastCommittedTxn));
            // Thread B (reader) - lags behind and grabs read permit for the previous transaction. Acquire rejected
            Assert.assertFalse(txnScoreboard.acquireTxn(1, lastCommittedTxn - 1));
            // Thread B (reader) - grabs read permit
            txnScoreboard.acquireTxn(1, lastCommittedTxn);
            // txngetMin(scoreboard) equals to 2 and will stay so
            Assert.assertTrue(txnScoreboard.isTxnAvailable(lastCommittedTxn - 1));
            Assert.assertFalse(txnScoreboard.isTxnAvailable(lastCommittedTxn));

            // Thread C (writer) - checkScoreboardHasReadersBeforeLastCommittedTxn
            txnScoreboard.acquireTxn(2, lastCommittedTxn + 1);
            txnScoreboard.releaseTxn(2, lastCommittedTxn + 1);
            // The assertion fails while there are readers
            Assert.assertEquals(lastCommittedTxn, getMin(txnScoreboard));
        }
    }

    @Test
    public void testCleanOnExclusiveOpen() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TxnScoreboard scoreboard = newTxnScoreboard()) {
                for (int i = 0; i < 1500; i++) {
                    scoreboard.acquireTxn(0, i);
                    scoreboard.releaseTxn(0, i);
                }
                assertScoreboardMinOrNoLocks(scoreboard);
            }
            engine.getTxnScoreboardPool().clear();

            // second open is exclusive, file should be truncated
            try (TxnScoreboard scoreboard2 = newTxnScoreboard()) {
                assertScoreboardMinOrNoLocks(scoreboard2);
            }
            scoreboardPoolFactory.clear();
        });
    }

    @Test
    public void testCleanOnExclusiveOpenLocksFile() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TxnScoreboard scoreboard = newTxnScoreboard()) {
                for (int i = 0; i < 1500; i++) {
                    scoreboard.acquireTxn(0, i);
                    scoreboard.releaseTxn(0, i);
                }
                assertScoreboardMinOrNoLocks(scoreboard);
            }

            // second open is exclusive, file should be truncated
            try (TxnScoreboard scoreboard2 = newTxnScoreboard()) {
                Assert.assertTrue(getMin(scoreboard2) <= 0);
                for (int i = 0; i < 10; i++) {
                    scoreboard2.acquireTxn(0, i);
                    scoreboard2.releaseTxn(0, i);
                }

                // This should not obtain exclusive lock even though file was empty when scoreboard2 put shared lock
                try (TxnScoreboard scoreboard3 = newTxnScoreboard()) {
                    assertScoreboardMinOrNoLocks(scoreboard2);
                    assertScoreboardMinOrNoLocks(scoreboard3);
                }

            }
            scoreboardPoolFactory.clear();
        });
    }

    @Test
    public void testCrawlV1() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TxnScoreboard scoreboard = newTxnScoreboard()) {
                for (int i = 0; i < 1024; i++) {
                    Assert.assertTrue(scoreboard.acquireTxn(0, i));
                    scoreboard.releaseTxn(0, i);
                }
                for (int i = 1024; i < 1500; i++) {
                    Assert.assertTrue(scoreboard.acquireTxn(0, i));
                    scoreboard.releaseTxn(0, i);
                }
                assertScoreboardMinOrNoLocks(scoreboard);

                // increase scoreboard size
                try (TxnScoreboard scoreboard2 = newTxnScoreboard()) {
                    assertScoreboardMinOrNoLocks(scoreboard2);
                    for (int i = 1500; i < 3000; i++) {
                        scoreboard2.acquireTxn(0, i);
                        scoreboard2.releaseTxn(0, i);
                    }

                    assertScoreboardMinOrNoLocks(scoreboard2);
                    assertScoreboardMinOrNoLocks(scoreboard);
                }
                scoreboardPoolFactory.clear();
            }
        });
    }

    @Test
    public void testDoesNotCleanOnNonExclusiveOpen() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TxnScoreboard rootBoard = newTxnScoreboard()) {
                try (final TxnScoreboard scoreboard = newTxnScoreboard()) {
                    for (int i = 0; i < 1500; i++) {
                        scoreboard.acquireTxn(0, i);
                        scoreboard.releaseTxn(0, i);
                    }
                    assertScoreboardMinOrNoLocks(scoreboard);
                }
                engine.getTxnScoreboardPool().clear();

                try (final TxnScoreboard scoreboard2 = newTxnScoreboard()) {
                    assertScoreboardMinOrNoLocks(scoreboard2);
                    for (int i = 1500; i < 3000; i++) {
                        scoreboard2.acquireTxn(0, i);
                        scoreboard2.releaseTxn(0, i);
                    }
                    assertScoreboardMinOrNoLocks(scoreboard2);
                }

                assertScoreboardMinOrNoLocks(rootBoard);
            }
            scoreboardPoolFactory.clear();
        });
    }

    @Test
    public void testGetMinOnEmpty() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (
                    final TxnScoreboard scoreboard = newTxnScoreboard()
            ) {
                assertScoreboardMinOrNoLocks(scoreboard);
                Assert.assertTrue(scoreboard.acquireTxn(0, 2048));
                Assert.assertEquals(2048, getMin(scoreboard));
                scoreboard.releaseTxn(0, 2048);
                assertScoreboardMinOrNoLocks(scoreboard);

                Assert.assertTrue(scoreboard.acquireTxn(0, 10000L));
                scoreboard.releaseTxn(0, 10000L);
                assertScoreboardMinOrNoLocks(scoreboard);


                Assert.assertFalse(scoreboard.acquireTxn(0, 4095));
            }
            scoreboardPoolFactory.clear();
        });
    }

    @Test
    public void testHammer() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        testHammerScoreboard(rnd.nextInt(1000) + 1, 10000);
    }

    @Test
    public void testIncrementTxn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TxnScoreboard scoreboard = newTxnScoreboard()) {
                assertScoreboardMinOrNoLocks(scoreboard);
                Assert.assertTrue(scoreboard.acquireTxn(0, 1));
                Assert.assertEquals(1, getMin(scoreboard));

                // Don't release the older txn.
                Assert.assertTrue(scoreboard.acquireTxn(1, 2));
                scoreboard.releaseTxn(1, 2);
                Assert.assertEquals(1, getMin(scoreboard));

                // acquireTxn() would have failed here, but we can use incrementTxn()
                Assert.assertTrue(scoreboard.incrementTxn(2, 1));
                scoreboard.releaseTxn(2, 1);
                Assert.assertEquals(1, getMin(scoreboard));

                // Now we can release the older txn.
                scoreboard.releaseTxn(0, 1);
                assertScoreboardMinOrNoLocks(scoreboard);
            }
            scoreboardPoolFactory.clear();
        });
    }

    @Test
    public void testMoveToNextPageContention() throws Exception {
        int readers = 8;
        int iterations = 8;
        int entryCount = Math.max(Numbers.ceilPow2(readers) * 8, Numbers.ceilPow2(iterations));
        setProperty(PropertyKey.CAIRO_O3_TXN_SCOREBOARD_ENTRY_COUNT, entryCount);

        try (final TxnScoreboard scoreboard = newTxnScoreboard()) {
            final CyclicBarrier barrier = new CyclicBarrier(readers);
            final CountDownLatch latch = new CountDownLatch(readers);
            final AtomicInteger anomaly = new AtomicInteger();
            for (int i = 0; i < readers; i++) {
                final int txn = i;
                int id = i;
                Thread reader = new Thread(() -> {
                    try {
                        barrier.await();
                        for (int s = 0; s < 100; s++) {
                            try {
                                if (scoreboard.acquireTxn(id, txn + (long) s * entryCount)) {
                                    scoreboard.releaseTxn(id, txn + (long) s * entryCount);
                                    Os.pause();
                                }
                            } catch (CairoException e) {
                                if (Chars.contains(e.getFlyweightMessage(), "max txn-inflight limit reached")) {
                                    LOG.info().$(e.getFlyweightMessage()).$();
                                } else {
                                    throw e;
                                }
                            }
                        }
                    } catch (Throwable e) {
                        LOG.errorW().$(e).$();
                        anomaly.incrementAndGet();
                    } finally {
                        latch.countDown();
                    }
                });
                reader.start();
            }
            latch.await();

            Assert.assertEquals(0, anomaly.get());
        }
    }

    @Test
    public void testStartContention() throws Exception {
        int readers = 8;
        int iterations = 8;
        int entryCount = Math.max(Numbers.ceilPow2(readers) * 8, Numbers.ceilPow2(iterations));
        setProperty(PropertyKey.CAIRO_O3_TXN_SCOREBOARD_ENTRY_COUNT, entryCount);
        try (final TxnScoreboard scoreboard = newTxnScoreboard()) {
            final CyclicBarrier barrier = new CyclicBarrier(readers);
            final CountDownLatch latch = new CountDownLatch(readers);
            final AtomicInteger anomaly = new AtomicInteger();

            for (int i = 0; i < readers; i++) {
                final int txn = i;
                int id = i;
                Thread reader = new Thread(() -> {
                    try {
                        barrier.await();
                        if (scoreboard.acquireTxn(id, txn)) {
                            long activeReaderCount = getActiveReaderCount(scoreboard, txn);
                            if (activeReaderCount > readers || activeReaderCount < 1) {
                                LOG.errorW()
                                        .$("activeReaderCount=")
                                        .$(activeReaderCount)
                                        .$(",txn=").$(txn)
                                        .$(",min=")
                                        .$(getMin(scoreboard))
                                        .$();
                                anomaly.addAndGet(100);
                            }
                            Os.pause();
                            scoreboard.releaseTxn(id, txn);
                            Os.pause();
                        }
                    } catch (Throwable e) {
                        LOG.errorW().$(e).$();
                        anomaly.incrementAndGet();
                    } finally {
                        latch.countDown();
                    }
                });
                reader.start();
            }
            latch.await();

            Assert.assertEquals(0, anomaly.get());
        }
    }

    @Test
    public void testStressOpenParallel() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int parallel = 8;
            int iterations = 500;
            SOCountDownLatch latch = new SOCountDownLatch(parallel);
            AtomicInteger errors = new AtomicInteger();
            for (int i = 0; i < parallel; i++) {
                new Thread(() -> {
                    for (int j = 0; j < iterations; j++) {
                        //noinspection EmptyTryBlock
                        try (TxnScoreboard ignored = newTxnScoreboard()) {
                            // empty body because we need to close this
                        } catch (Exception ex) {
                            ex.printStackTrace();
                            errors.incrementAndGet();
                            break;
                        } finally {
                            Path.clearThreadLocals();
                        }
                    }
                    latch.countDown();
                }).start();
            }

            latch.await();
            Assert.assertEquals(0, errors.get());
            scoreboardPoolFactory.clear();
        });
    }

    @Test
    public void testVanilla() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TxnScoreboard scoreboard2 = newTxnScoreboard()) {
                try (TxnScoreboard scoreboard1 = newTxnScoreboard()) {
                    scoreboard1.acquireTxn(0, 67);
                    scoreboard1.acquireTxn(1, 68);
                    scoreboard1.acquireTxn(2, 68);
                    scoreboard1.acquireTxn(3, 69);
                    scoreboard1.acquireTxn(4, 70);
                    scoreboard1.acquireTxn(5, 71);
                    Assert.assertEquals(67, getMin(scoreboard1));

                    scoreboard1.releaseTxn(1, 68);
                    Assert.assertEquals(67, getMin(scoreboard1));

                    Assert.assertFalse(scoreboard2.isTxnAvailable(68));
                    scoreboard1.releaseTxn(2, 68);
                    Assert.assertEquals(67, getMin(scoreboard1));
                    scoreboard1.releaseTxn(0, 67);
                    Assert.assertEquals(69, getMin(scoreboard1));

                    scoreboard1.releaseTxn(3, 69);
                    Assert.assertEquals(70, getMin(scoreboard1));
                    scoreboard1.releaseTxn(5, 71);
                    Assert.assertTrue(scoreboard1.isTxnAvailable(69));

                    Assert.assertEquals(70, getMin(scoreboard1));
                    scoreboard1.releaseTxn(4, 70);

                    assertScoreboardMinOrNoLocks(scoreboard1);
                    scoreboard1.acquireTxn(0, 72);
                }
                scoreboard2.acquireTxn(0, 72);
                Assert.assertFalse(scoreboard2.isTxnAvailable(72));

                scoreboard2.releaseTxn(0, 72);
                scoreboard2.releaseTxn(0, 72);
                Assert.assertTrue(scoreboard2.isTxnAvailable(71));
            }
            scoreboardPoolFactory.clear();
        });
    }

    @Test
    public void testWideRange() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TxnScoreboard scoreboard = newTxnScoreboard()) {
                scoreboard.acquireTxn(0, 15);
                scoreboard.releaseTxn(0, 15);
                scoreboard.acquireTxn(0, 900992);
                Assert.assertFalse(scoreboard.isTxnAvailable(900992));
            }
            scoreboardPoolFactory.clear();
        });
    }

    // This method is implemented in scoreboards for testing only and
    // is not part of the public API
    private static long getActiveReaderCount(TxnScoreboard scoreboard, long txn) {
        return ((TxnScoreboardV2) scoreboard).getActiveReaderCount(txn);
    }

    // This method is implemented in scoreboards for testing only and
    // is not part of the public API
    private static long getMin(TxnScoreboard scoreboard) {
        return ((TxnScoreboardV2) scoreboard).getMin();
    }

    private void assertScoreboardMinOrNoLocks(TxnScoreboard scoreboard) {
        Assert.assertEquals(-1, getMin(scoreboard));
    }

    @SuppressWarnings("SameParameterValue")
    private void testHammerScoreboard(int readers, int iterations) throws Exception {
        int entryCount = Math.max(Numbers.ceilPow2(readers) * 8, Numbers.ceilPow2(iterations));
        setProperty(PropertyKey.CAIRO_O3_TXN_SCOREBOARD_ENTRY_COUNT, entryCount);
        setProperty(PropertyKey.CAIRO_READER_POOL_MAX_SEGMENTS, (int) Math.ceil((double) readers / configuration.getPoolSegmentSize()));
        try (final TxnScoreboard scoreboard = newTxnScoreboard()) {
            final CyclicBarrier barrier = new CyclicBarrier(readers + 1);
            final CountDownLatch latch = new CountDownLatch(readers + 1);
            txn = 0;
            final AtomicInteger anomaly = new AtomicInteger();

            for (int i = 0; i < readers; i++) {
                // Readers acq/release every txn number and check invariants
                Reader reader = new Reader(i, scoreboard, barrier, latch, anomaly, iterations, readers);
                reader.start();
            }

            // Writer constantly increments txn number
            Writer writer = new Writer(scoreboard, barrier, latch, anomaly, iterations, 2);
            writer.start();

            latch.await();

            Assert.assertEquals(0, anomaly.get());
            for (long i = 0; i < iterations + 1; i++) {
                Assert.assertTrue(scoreboard.isTxnAvailable(i));
            }
        }
    }

    private static class Reader extends Thread {

        private final AtomicInteger anomaly;
        private final CyclicBarrier barrier;
        private final int id;
        private final int iterations;
        private final CountDownLatch latch;
        private final int readers;
        private final TxnScoreboard scoreboard;

        private Reader(int id, TxnScoreboard scoreboard, CyclicBarrier barrier, CountDownLatch latch, AtomicInteger anomaly, int iterations, int readers) {
            this.id = id;
            this.scoreboard = scoreboard;
            this.barrier = barrier;
            this.latch = latch;
            this.anomaly = anomaly;
            this.iterations = iterations;
            this.readers = readers;
        }

        @Override
        public void run() {
            try {
                barrier.await();
                long t;
                while ((t = txn) < iterations) {
                    if (scoreboard.acquireTxn(id, t)) {
                        long writerMinLoc = writerMin;
                        long activeReaderCount = getActiveReaderCount(scoreboard, t);
                        if (activeReaderCount > readers + 1 || activeReaderCount < 1) {
                            LOG.errorW()
                                    .$("activeReaderCount=")
                                    .$(activeReaderCount)
                                    .$(",txn=").$(t)
                                    .$(",min=")
                                    .$(getMin(scoreboard))
                                    .$();
                            anomaly.addAndGet(100);
                        }
                        Os.pause();
                        if (writerMinLoc > t) {
                            LOG.errorW()
                                    .$("writer min too optimistic writerMin=").$(writerMinLoc)
                                    .$(",lockedTxn=").$(t)
                                    .$();
                            anomaly.incrementAndGet();
                        }
                        long rangeTo = txn;
                        if (scoreboard.isRangeAvailable(t, Math.max(rangeTo, t + 1))) {
                            LOG.errorW()
                                    .$("range available from=").$(t)
                                    .$(", to=").$(rangeTo)
                                    .$(",lockedTxn=").$(t)
                                    .$();
                            anomaly.incrementAndGet();
                        }
                        scoreboard.releaseTxn(id, t);
                        LockSupport.parkNanos(10);
                    }
                }
            } catch (Throwable e) {
                LOG.errorW().$(e).$();
                anomaly.incrementAndGet();
            } finally {
                latch.countDown();
            }
        }
    }

    private static class Writer extends Thread {

        private final AtomicInteger anomaly;
        private final CyclicBarrier barrier;
        private final int iterations;
        private final CountDownLatch latch;
        private final TxnScoreboard scoreboard;
        private final int version;

        private Writer(TxnScoreboard scoreboard, CyclicBarrier barrier, CountDownLatch latch, AtomicInteger anomaly, int iterations, int version) {
            this.scoreboard = scoreboard;
            this.barrier = barrier;
            this.latch = latch;
            this.anomaly = anomaly;
            this.iterations = iterations;
            this.version = version;
        }

        @Override
        public void run() {
            try {
                long publishWaitBarrier = scoreboard.getEntryCount() - 2;
                txn = 1;
                scoreboard.hasEarlierTxnLocks(txn);
                writerMin = getMin(scoreboard);

                barrier.await();
                for (int i = 0; i < iterations; i++) {
                    if (version == 1) {
                        for (int sleepCount = 0; sleepCount < 50 && txn - getMin(scoreboard) > publishWaitBarrier; sleepCount++) {
                            // Some readers are slow and haven't released transaction yet. Give them a bit more time
                            LOG.infoW().$("slow reader release, waiting... [txn=")
                                    .$(txn)
                                    .$(", min=").$(getMin(scoreboard))
                                    .$(", size=").$(scoreboard.getEntryCount())
                                    .I$();
                            Os.sleep(100);
                        }

                        if (txn - getMin(scoreboard) > publishWaitBarrier) {
                            // Wait didn't help. Abort the test.
                            anomaly.addAndGet(1000);
                            LOG.errorW().$("slow reader release, abort [txn=")
                                    .$(txn)
                                    .$(", min=").$(getMin(scoreboard))
                                    .$(", size=").$(scoreboard.getEntryCount())
                                    .I$();
                            txn = iterations;
                            return;
                        }
                    }

                    // This is the only writer
                    //noinspection NonAtomicOperationOnVolatileField
                    txn++;
                    long nextTxn = txn;

                    // Simulate TableWriter trying to find if there are readers before the published transaction
                    scoreboard.hasEarlierTxnLocks(nextTxn);
                    writerMin = getMin(scoreboard);

                    if (writerMin > nextTxn) {
                        LOG.errorW()
                                .$("writer min is above max published transaction=").$(writerMin)
                                .$(",publishedTxn=").$(nextTxn)
                                .$();
                        anomaly.addAndGet(10000);
                    }
                }
                LOG.infoW().$("writer done").I$();
            } catch (Throwable e) {
                LOG.errorW().$(e).$();
                anomaly.incrementAndGet();
                txn = iterations;
            } finally {
                latch.countDown();
            }
        }
    }
}
