/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.cairo.TxnScoreboardPoolFactory;
import io.questdb.cairo.TxnScoreboardV1;
import io.questdb.cairo.TxnScoreboardV2;
import io.questdb.cairo.pool.ReaderPool;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacade;
import io.questdb.std.Numbers;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

@RunWith(Parameterized.class)
public class TxnScoreboardTest extends AbstractCairoTest {
    private static volatile long txn;
    private static volatile long writerMin;
    private final int version;
    private TxnScoreboardPool scoreboardPoolFactory;
    private TableToken tableToken;

    public TxnScoreboardTest(int version) {
        this.version = version;
    }

    @Parameterized.Parameters(name = "V{0}")
    public static Collection<Object[]> testParams() {
        return Arrays.asList(new Object[][]{
                {1},
                {2},
        });
    }

    public TxnScoreboard newTxnScoreboard() {
        return scoreboardPoolFactory.getTxnScoreboard(tableToken);
    }

    @Before
    public void setUp() {
        super.setUp();
        setProperty(PropertyKey.CAIRO_TXN_SCOREBOARD_FORMAT, version);
        scoreboardPoolFactory = TxnScoreboardPoolFactory.createPool(engine.getConfiguration());
        tableToken = createTable(
                new TableModel(engine.getConfiguration(), "x", PartitionBy.DAY)
                        .col("ts", ColumnType.TIMESTAMP)
                        .col("bid", ColumnType.DOUBLE)
                        .timestamp()
        );
    }

    @Test
    public void testBitmapIndexEdgeCase() throws Exception {
        Assume.assumeTrue(version == 2);
        TestUtils.assertMemoryLeak(() -> {
            for (int i = 127; i < 130; i++) {
                try (TxnScoreboardV2 scoreboard = new TxnScoreboardV2(i)) {
                    for (int j = 1; j < 88; j++) {
                        scoreboard.acquireTxn(i - j, i * j);
                        Assert.assertFalse(scoreboard.isTxnAvailable(i * j));
                        Assert.assertEquals(1, scoreboard.getActiveReaderCount(i * j));
                        Assert.assertFalse(scoreboard.isRangeAvailable(i * j, i * (j + 1)));
                    }
                    long max = i * 87;
                    Assert.assertEquals(i, getMin(scoreboard));
                    for (int j = 1; j < 88; j++) {
                        scoreboard.releaseTxn(i - j, i * j);
                        Assert.assertEquals(0, scoreboard.getActiveReaderCount(i * j));
                        Assert.assertEquals(i * j < max, scoreboard.isTxnAvailable(i * j));
                        Assert.assertTrue(scoreboard.isRangeAvailable(i * j, i * (j + 1)));
                    }

                    Assert.assertEquals(-1, getMin(scoreboard));
                }
            }
        });
    }

    @Test
    public void testCannotLockTwice() throws Exception {
        Assume.assumeTrue(version == 2);
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
    public void testCleanFailsNoResourceLeakRO() throws Exception {
        Assume.assumeTrue(version == 1);
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openCleanRW(LPSZ name, long size) {
                return -1;
            }
        };

        assertMemoryLeak(ff, () -> {
            try (TxnScoreboard ignored = newTxnScoreboard()) {
                Assert.fail();
            } catch (CairoException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "could not open read-write with clean allocation");
            }
        });
    }

    @Test
    public void testCleanFailsNoResourceLeakRW() throws Exception {
        Assume.assumeTrue(version == 1);
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openCleanRW(LPSZ name, long size) {
                return -1;
            }
        };

        assertMemoryLeak(ff, () -> {
            try (TxnScoreboard ignored = newTxnScoreboard()) {
                Assert.fail();
            } catch (CairoException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "could not open read-write with clean allocation");
            }
        });
    }

    @Test
    public void testCleanOnExclusiveOpen() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TxnScoreboard scoreboard = newTxnScoreboard()) {
                for (int i = 0; i < 1500; i++) {
                    scoreboard.acquireTxn(0, i);
                    scoreboard.releaseTxn(0, i);
                }
                assertScoreboardMinOrNoLocks(scoreboard, 1499);
            }
            engine.getTxnScoreboardPool().clear();

            // second open is exclusive, file should be truncated
            try (TxnScoreboard scoreboard2 = newTxnScoreboard()) {
                assertScoreboardMinOrNoLocks(scoreboard2, 0);
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
                assertScoreboardMinOrNoLocks(scoreboard, 1499);
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
                    assertScoreboardMinOrNoLocks(scoreboard2, 9);
                    assertScoreboardMinOrNoLocks(scoreboard3, 9);
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
                assertScoreboardMinOrNoLocks(scoreboard, 1499);

                // increase scoreboard size
                try (TxnScoreboard scoreboard2 = newTxnScoreboard()) {
                    assertScoreboardMinOrNoLocks(scoreboard2, 1499);
                    for (int i = 1500; i < 3000; i++) {
                        scoreboard2.acquireTxn(0, i);
                        scoreboard2.releaseTxn(0, i);
                    }

                    assertScoreboardMinOrNoLocks(scoreboard2, 2999);
                    assertScoreboardMinOrNoLocks(scoreboard, 2999);
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
                    assertScoreboardMinOrNoLocks(scoreboard, 1499);
                }
                engine.getTxnScoreboardPool().clear();

                try (final TxnScoreboard scoreboard2 = newTxnScoreboard()) {
                    assertScoreboardMinOrNoLocks(scoreboard2, 1499);
                    for (int i = 1500; i < 3000; i++) {
                        scoreboard2.acquireTxn(0, i);
                        scoreboard2.releaseTxn(0, i);
                    }
                    assertScoreboardMinOrNoLocks(scoreboard2, 2999);
                }

                assertScoreboardMinOrNoLocks(rootBoard, 2999);
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
                assertScoreboardMinOrNoLocks(scoreboard, 0);
                Assert.assertTrue(scoreboard.acquireTxn(0, 2048));
                Assert.assertEquals(2048, getMin(scoreboard));
                scoreboard.releaseTxn(0, 2048);
                assertScoreboardMinOrNoLocks(scoreboard, 2048);

                Assert.assertTrue(scoreboard.acquireTxn(0, 10000L));
                scoreboard.releaseTxn(0, 10000L);
                assertScoreboardMinOrNoLocks(scoreboard, 10000L);


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
                assertScoreboardMinOrNoLocks(scoreboard, 0);
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
                assertScoreboardMinOrNoLocks(scoreboard, 2);
            }
            scoreboardPoolFactory.clear();
        });
    }

    @Test
    public void testLimits() throws Exception {
        Assume.assumeTrue(version == 1);
        TestUtils.assertMemoryLeak(() -> {
            int expect = 2048;
            setProperty(PropertyKey.CAIRO_O3_TXN_SCOREBOARD_ENTRY_COUNT, expect);
            try (TxnScoreboard scoreboard2 = newTxnScoreboard()) {
                try (TxnScoreboard scoreboard1 = newTxnScoreboard()) {
                    // we should successfully acquire expected number of entries
                    for (int i = 0; i < expect; i++) {
                        scoreboard1.acquireTxn(0, i + 134);
                    }
                    // scoreboard capacity should be exhausted,
                    // we should be refused to acquire any more slots
                    try {
                        scoreboard1.acquireTxn(0, expect + 134);
                        Assert.fail();
                    } catch (CairoException ignored) {
                    }

                    // now we release middle slot, this does not free any more slots
                    scoreboard1.releaseTxn(0, 11 + 134);
                    Assert.assertEquals(134, getMin(scoreboard1));
                    // we should NOT be able to allocate more slots
                    try {
                        scoreboard1.acquireTxn(0, expect + 134);
                        Assert.fail();
                    } catch (CairoException ignored) {
                    }

                    // now that we release "head" slot we should be able to acquire more
                    scoreboard1.releaseTxn(0, 134);
                    Assert.assertEquals(135, getMin(scoreboard1));
                    // and we should be able to allocate another one
                    scoreboard1.acquireTxn(0, expect + 134);

                    // now check that all counts are intact
                    for (int i = 1; i <= expect; i++) {
                        if (i != 11) {
                            Assert.assertFalse(scoreboard1.isTxnAvailable(i + 134));
                        } else {
                            Assert.assertTrue(scoreboard1.isTxnAvailable(i + 134));
                        }
                    }
                }

                for (int i = 1; i <= expect; i++) {
                    if (i != 11) {
                        Assert.assertFalse(scoreboard2.isTxnAvailable(i + 134));
                    } else {
                        Assert.assertTrue(scoreboard2.isTxnAvailable(i + 134));
                    }
                }
            }
        });
    }

    @Test
    public void testLimitsO3Acquire() throws Exception {
        LOG.debug().$("start testLimitsO3Acquire").$();
        for (int i = 0; i < 1000; i++) {
            testLimits();
        }
    }

    @Test
    public void testMapFailsNoResourceLeakRO() throws Exception {
        Assume.assumeTrue(version == 1);
        TestUtils.assertMemoryLeak(() -> {
            FilesFacade ff = new TestFilesFacadeImpl() {
                @Override
                public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                    if (this.fd == fd) {
                        return -1;
                    }
                    return super.mmap(fd, len, offset, flags, memoryTag);
                }

                @Override
                public long openCleanRW(LPSZ name, long size) {
                    long fd = super.openCleanRW(name, size);
                    this.fd = fd;
                    return fd;
                }
            };

            assertMemoryLeak(ff, () -> {
                try (TxnScoreboard ignored = newTxnScoreboard()) {
                    Assert.fail();
                } catch (CairoException ex) {
                    TestUtils.assertContains(ex.getFlyweightMessage(), "could not mmap");
                }
            });
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
    public void testO3AcquireRejected() throws Exception {
        Assume.assumeTrue(version == 1);
        TestUtils.assertMemoryLeak(() -> {
            int size = 64;
            int start = 134;
            setProperty(PropertyKey.CAIRO_O3_TXN_SCOREBOARD_ENTRY_COUNT, size);

            try (TxnScoreboard scoreboard1 = newTxnScoreboard()) {
                // first 2 go out of order
                scoreboard1.acquireTxn(0, 1 + start);
                Assert.assertFalse(scoreboard1.acquireTxn(0, start));
                Assert.assertTrue(scoreboard1.isTxnAvailable(start));
            }
        });
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
                            long min = getMin(scoreboard);
                            if (!scoreboard.isTxnAvailable(min - 1)) {
                                // V2 scoreboard can add a phantom min entry temporarily and then check
                                // that this is not valid min and remove it, returning lock as unsuccessful (false)
                                // It's only problem with V1 scoreboard
                                if (version == 1) {
                                    // This one also fails, but those could be readers that didn't roll back yet
                                    anomaly.incrementAndGet();
                                }
                            }
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

                    assertScoreboardMinOrNoLocks(scoreboard1, 71);
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
        if (scoreboard instanceof TxnScoreboardV2) {
            return ((TxnScoreboardV2) scoreboard).getActiveReaderCount(txn);
        } else {
            return ((TxnScoreboardV1) scoreboard).getActiveReaderCount(txn);
        }
    }

    // This method is implemented in scoreboards for testing only and
    // is not part of the public API
    private static long getMin(TxnScoreboard scoreboard) {
        if (scoreboard instanceof TxnScoreboardV2) {
            return ((TxnScoreboardV2) scoreboard).getMin();
        } else {
            return ((TxnScoreboardV1) scoreboard).getMin();
        }
    }

    private void assertScoreboardMinOrNoLocks(TxnScoreboard scoreboard, long txn) {
        if (scoreboard instanceof TxnScoreboardV2) {
            Assert.assertEquals(-1, getMin(scoreboard));
        } else {
            Assert.assertEquals(txn, getMin(scoreboard));
        }
    }

    @SuppressWarnings("SameParameterValue")
    private void testHammerScoreboard(int readers, int iterations) throws Exception {
        int entryCount = Math.max(Numbers.ceilPow2(readers) * 8, Numbers.ceilPow2(iterations));
        setProperty(PropertyKey.CAIRO_O3_TXN_SCOREBOARD_ENTRY_COUNT, entryCount);
        setProperty(PropertyKey.CAIRO_READER_POOL_MAX_SEGMENTS, (int) Math.ceil((double) readers / ReaderPool.ENTRY_SIZE));
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
            Writer writer = new Writer(scoreboard, barrier, latch, anomaly, iterations, version);
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
