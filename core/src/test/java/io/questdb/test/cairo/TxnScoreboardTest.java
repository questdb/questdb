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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TxnScoreboard;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacade;
import io.questdb.std.Numbers;
import io.questdb.std.Os;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public class TxnScoreboardTest extends AbstractCairoTest {
    private static volatile long txn;
    private static volatile long writerMin;

    @Test
    public void testCheckNoLocksBeforeTxn() {
        final long lastCommittedTxn = 2;
        ff = new TestFilesFacadeImpl();
        try (
                final Path shmPath = new Path();
                TxnScoreboard txnScoreboard = new TxnScoreboard(ff, 2048).ofRW(shmPath.of(root))
        ) {
            // Thread A (reader) - grabs read permit
            Assert.assertTrue(txnScoreboard.acquireTxn(lastCommittedTxn));
            // Thread B (reader) - lags behind and grabs read permit for the previous transaction. Acquire rejected
            Assert.assertFalse(txnScoreboard.acquireTxn(lastCommittedTxn - 1));
            // Thread A (reader) - grabs read permit
            txnScoreboard.acquireTxn(lastCommittedTxn);
            // txnScoreboard.getMin() equals to 2 and will stay so
            Assert.assertEquals(0, txnScoreboard.getActiveReaderCount(lastCommittedTxn - 1));
            Assert.assertEquals(2, txnScoreboard.getActiveReaderCount(lastCommittedTxn));

            // Thread C (writer) - checkScoreboardHasReadersBeforeLastCommittedTxn
            txnScoreboard.acquireTxn(lastCommittedTxn + 1);
            txnScoreboard.releaseTxn(lastCommittedTxn + 1);
            // The assertion fails while there are readers
            Assert.assertEquals(lastCommittedTxn, txnScoreboard.getMin());
        }
    }

    @Test
    public void testCleanFailsNoResourceLeakRO() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            FilesFacade ff = new TestFilesFacadeImpl() {
                @Override
                public long openCleanRW(LPSZ name, long size) {
                    return -1;
                }
            };

            assertMemoryLeak(() -> {
                try (final Path shmPath = new Path()) {
                    try (TxnScoreboard ignored = new TxnScoreboard(ff, 2048).ofRO(shmPath.of(root))) {
                        Assert.fail();
                    } catch (CairoException ex) {
                        TestUtils.assertContains(ex.getFlyweightMessage(), "could not open read-write with clean allocation");
                    }
                }
            });
        });
    }

    @Test
    public void testCleanFailsNoResourceLeakRW() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            FilesFacade ff = new TestFilesFacadeImpl() {
                @Override
                public long openCleanRW(LPSZ name, long size) {
                    return -1;
                }
            };

            assertMemoryLeak(() -> {
                try (final Path shmPath = new Path()) {
                    try (TxnScoreboard ignored = new TxnScoreboard(ff, 2048).ofRW(shmPath.of(root))) {
                        Assert.fail();
                    } catch (CairoException ex) {
                        TestUtils.assertContains(ex.getFlyweightMessage(), "could not open read-write with clean allocation");
                    }
                }
            });
        });
    }

    @Test
    public void testCleanOnExclusiveOpen() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final Path shmPath = new Path()) {
                try (
                        final TxnScoreboard scoreboard = new TxnScoreboard(TestFilesFacadeImpl.INSTANCE, 1024).ofRW(shmPath.of(root))
                ) {
                    for (int i = 0; i < 1500; i++) {
                        scoreboard.acquireTxn(i);
                        scoreboard.releaseTxn(i);
                    }
                    Assert.assertEquals(1499, scoreboard.getMin());
                }

                // second open is exclusive, file should be truncated
                try (
                        final TxnScoreboard scoreboard2 = new TxnScoreboard(TestFilesFacadeImpl.INSTANCE, 2048).ofRW(shmPath.of(root))
                ) {
                    Assert.assertEquals(0, scoreboard2.getMin());
                }
            }
        });
    }

    @Test
    public void testCleanOnExclusiveOpenLocksFile() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final Path shmPath = new Path()) {
                FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
                try (
                        final TxnScoreboard scoreboard = new TxnScoreboard(ff, 1024).ofRW(shmPath.of(root))
                ) {
                    for (int i = 0; i < 1500; i++) {
                        scoreboard.acquireTxn(i);
                        scoreboard.releaseTxn(i);
                    }
                    Assert.assertEquals(1499, scoreboard.getMin());
                }

                // second open is exclusive, file should be truncated
                try (
                        final TxnScoreboard scoreboard2 = new TxnScoreboard(ff, 2048).ofRW(shmPath.of(root))
                ) {
                    Assert.assertEquals(0, scoreboard2.getMin());
                    for (int i = 0; i < 10; i++) {
                        scoreboard2.acquireTxn(i);
                        scoreboard2.releaseTxn(i);
                    }

                    // This should not obtain exclusive lock even though file was empty when scoreboard2 put shared lock
                    try (
                            final TxnScoreboard scoreboard3 = new TxnScoreboard(ff, 2048).ofRO(shmPath.of(root))
                    ) {
                        Assert.assertEquals(9, scoreboard2.getMin());
                        Assert.assertEquals(9, scoreboard3.getMin());
                    }

                }
            }
        });
    }

    @Test
    public void testCrawl() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final Path shmPath = new Path()) {
                try (
                        final TxnScoreboard scoreboard = new TxnScoreboard(TestFilesFacadeImpl.INSTANCE, 1024).ofRW(shmPath.of(root))
                ) {
                    for (int i = 0; i < 1024; i++) {
                        Assert.assertTrue(scoreboard.acquireTxn(i));
                        scoreboard.releaseTxn(i);
                    }
                    for (int i = 1024; i < 1500; i++) {
                        Assert.assertTrue(scoreboard.acquireTxn(i));
                        scoreboard.releaseTxn(i);
                    }
                    Assert.assertEquals(1499, scoreboard.getMin());

                    // increase scoreboard size
                    try (
                            final TxnScoreboard scoreboard2 = new TxnScoreboard(TestFilesFacadeImpl.INSTANCE, 2048).ofRW(shmPath.of(root))
                    ) {
                        Assert.assertEquals(1499, scoreboard2.getMin());
                        for (int i = 1500; i < 3000; i++) {
                            scoreboard2.acquireTxn(i);
                            scoreboard2.releaseTxn(i);
                        }
                        Assert.assertEquals(2999, scoreboard2.getMin());
                        Assert.assertEquals(2999, scoreboard.getMin());
                    }
                }
            }
        });
    }

    @Test
    public void testDoesNotCleanOnNonExclusiveOpen() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final Path shmPath = new Path()) {
                try (
                        final TxnScoreboard rootBoard = new TxnScoreboard(TestFilesFacadeImpl.INSTANCE, 2048).ofRW(shmPath.of(root))
                ) {
                    try (
                            final TxnScoreboard scoreboard = new TxnScoreboard(TestFilesFacadeImpl.INSTANCE, 1024).ofRW(shmPath.of(root))
                    ) {
                        for (int i = 0; i < 1500; i++) {
                            scoreboard.acquireTxn(i);
                            scoreboard.releaseTxn(i);
                        }
                        Assert.assertEquals(1499, scoreboard.getMin());
                    }

                    try (
                            final TxnScoreboard scoreboard2 = new TxnScoreboard(TestFilesFacadeImpl.INSTANCE, 2048).ofRW(shmPath.of(root))
                    ) {
                        Assert.assertEquals(1499, scoreboard2.getMin());
                        for (int i = 1500; i < 3000; i++) {
                            scoreboard2.acquireTxn(i);
                            scoreboard2.releaseTxn(i);
                        }
                        Assert.assertEquals(2999, scoreboard2.getMin());
                    }

                    Assert.assertEquals(2999, rootBoard.getMin());
                }
            }
        });
    }

    @Test
    public void testGetMinOnEmpty() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (
                    final Path shmPath = new Path();
                    final TxnScoreboard scoreboard = new TxnScoreboard(TestFilesFacadeImpl.INSTANCE, 1024).ofRW(shmPath.of(root))
            ) {
                Assert.assertEquals(0, scoreboard.getMin());
                Assert.assertTrue(scoreboard.acquireTxn(2048));
                Assert.assertEquals(2048, scoreboard.getMin());
                scoreboard.releaseTxn(2048);
                Assert.assertEquals(2048, scoreboard.getMin());

                Assert.assertTrue(scoreboard.acquireTxn(10000L));
                scoreboard.releaseTxn(10000L);
                Assert.assertEquals(10000L, scoreboard.getMin());


                Assert.assertFalse(scoreboard.acquireTxn(4095));
            }
        });
    }

    @Test
    public void testHammer() throws Exception {
        testHammerScoreboard(8, 10000);
    }

    @Test
    public void testLimits() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int expect = 2048;
            try (final Path shmPath = new Path()) {
                try (TxnScoreboard scoreboard2 = new TxnScoreboard(TestFilesFacadeImpl.INSTANCE, expect).ofRW(shmPath.of(root))) {
                    try (TxnScoreboard scoreboard1 = new TxnScoreboard(TestFilesFacadeImpl.INSTANCE, expect).ofRW(shmPath.of(root))) {
                        // we should successfully acquire expected number of entries
                        for (int i = 0; i < expect; i++) {
                            scoreboard1.acquireTxn(i + 134);
                        }
                        // scoreboard capacity should be exhausted,
                        // we should be refused to acquire any more slots
                        try {
                            scoreboard1.acquireTxn(expect + 134);
                            Assert.fail();
                        } catch (CairoException ignored) {
                        }

                        // now we release middle slot, this does not free any more slots
                        scoreboard1.releaseTxn(11 + 134);
                        Assert.assertEquals(134, scoreboard1.getMin());
                        // we should NOT be able to allocate more slots
                        try {
                            scoreboard1.acquireTxn(expect + 134);
                            Assert.fail();
                        } catch (CairoException ignored) {
                        }

                        // now that we release "head" slot we should be able to acquire more
                        scoreboard1.releaseTxn(134);
                        Assert.assertEquals(135, scoreboard1.getMin());
                        // and we should be able to allocate another one
                        scoreboard1.acquireTxn(expect + 134);

                        // now check that all counts are intact
                        for (int i = 1; i <= expect; i++) {
                            if (i != 11) {
                                Assert.assertEquals(1, scoreboard1.getActiveReaderCount(i + 134));
                            } else {
                                Assert.assertEquals(0, scoreboard1.getActiveReaderCount(i + 134));
                            }
                        }
                    }

                    for (int i = 1; i <= expect; i++) {
                        if (i != 11) {
                            Assert.assertEquals(1, scoreboard2.getActiveReaderCount(i + 134));
                        } else {
                            Assert.assertEquals(0, scoreboard2.getActiveReaderCount(i + 134));
                        }
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
        TestUtils.assertMemoryLeak(() -> {
            FilesFacade ff = new TestFilesFacadeImpl() {
                @Override
                public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                    return -1;
                }
            };

            assertMemoryLeak(() -> {
                try (final Path shmPath = new Path()) {
                    try (TxnScoreboard ignored = new TxnScoreboard(ff, 2048).ofRO(shmPath.of(root))) {
                        Assert.fail();
                    } catch (CairoException ex) {
                        TestUtils.assertContains(ex.getFlyweightMessage(), "could not mmap");
                    }
                }
            });
        });
    }

    @Test
    public void testMapFailsNoResourceLeakRW() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            FilesFacade ff = new TestFilesFacadeImpl() {
                @Override
                public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                    return -1;
                }
            };

            assertMemoryLeak(() -> {
                try (final Path shmPath = new Path()) {
                    try (TxnScoreboard ignored = new TxnScoreboard(ff, 2048).ofRW(shmPath.of(root))) {
                        Assert.fail();
                    } catch (CairoException ex) {
                        TestUtils.assertContains(ex.getFlyweightMessage(), "could not mmap");
                    }
                }
            });
        });
    }

    @Test
    public void testMoveToNextPageContention() throws Exception {
        int readers = 8;
        int iterations = 8;
        int entryCount = Math.max(Numbers.ceilPow2(readers) * 8, Numbers.ceilPow2(iterations));
        try (
                final Path shmPath = new Path();
                final TxnScoreboard scoreboard = new TxnScoreboard(TestFilesFacadeImpl.INSTANCE, entryCount).ofRW(shmPath.of(root))
        ) {
            final CyclicBarrier barrier = new CyclicBarrier(readers);
            final CountDownLatch latch = new CountDownLatch(readers);
            final AtomicInteger anomaly = new AtomicInteger();
            for (int i = 0; i < readers; i++) {
                final int txn = i;
                Thread reader = new Thread(() -> {
                    try {
                        barrier.await();
                        for (int s = 0; s < 100; s++) {
                            try {
                                if (scoreboard.acquireTxn(txn + (long) s * entryCount)) {
                                    scoreboard.releaseTxn(txn + (long) s * entryCount);
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
        TestUtils.assertMemoryLeak(() -> {
            int size = 64;
            int start = 134;
            try (final Path shmPath = new Path()) {
                try (TxnScoreboard scoreboard1 = new TxnScoreboard(TestFilesFacadeImpl.INSTANCE, size).ofRW(shmPath.of(root))) {
                    // first 2 go out of order
                    scoreboard1.acquireTxn(1 + start);
                    Assert.assertFalse(scoreboard1.acquireTxn(start));
                    Assert.assertEquals(0, scoreboard1.getActiveReaderCount(start));
                }
            }
        });
    }

    @Test
    public void testStartContention() throws Exception {
        int readers = 8;
        int iterations = 8;
        int entryCount = Math.max(Numbers.ceilPow2(readers) * 8, Numbers.ceilPow2(iterations));
        try (
                final Path shmPath = new Path();
                final TxnScoreboard scoreboard = new TxnScoreboard(TestFilesFacadeImpl.INSTANCE, entryCount).ofRW(shmPath.of(root))
        ) {
            final CyclicBarrier barrier = new CyclicBarrier(readers);
            final CountDownLatch latch = new CountDownLatch(readers);
            final AtomicInteger anomaly = new AtomicInteger();

            for (int i = 0; i < readers; i++) {
                final int txn = i;
                Thread reader = new Thread(() -> {
                    try {
                        barrier.await();
                        if (scoreboard.acquireTxn(txn)) {
                            long activeReaderCount = scoreboard.getActiveReaderCount(txn);
                            if (activeReaderCount > readers || activeReaderCount < 1) {
                                LOG.errorW()
                                        .$("activeReaderCount=")
                                        .$(activeReaderCount)
                                        .$(",txn=").$(txn)
                                        .$(",min=")
                                        .$(scoreboard.getMin())
                                        .$();
                                anomaly.addAndGet(100);
                            }
                            Os.pause();
                            scoreboard.releaseTxn(txn);
                            long min = scoreboard.getMin();
                            long prevCount = scoreboard.getActiveReaderCount(min - 1);
                            if (prevCount > 0) {
                                // This one also fails, but those could be readers that didn't roll back yet
                                anomaly.incrementAndGet();
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
                    try (final Path shmPath = new Path()) {
                        for (int j = 0; j < iterations; j++) {
                            //noinspection EmptyTryBlock
                            try (TxnScoreboard ignored = new TxnScoreboard(TestFilesFacadeImpl.INSTANCE, 1024).ofRW(shmPath.of(root))) {
                                // empty body because we need to close this
                            } catch (Exception ex) {
                                ex.printStackTrace();
                                errors.incrementAndGet();
                                break;
                            }
                        }
                    }
                    latch.countDown();
                }).start();
            }

            latch.await();
            Assert.assertEquals(0, errors.get());
        });
    }

    @Test
    public void testVanilla() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (
                    final Path shmPath = new Path();
                    final TxnScoreboard scoreboard2 = new TxnScoreboard(TestFilesFacadeImpl.INSTANCE, 1024).ofRW(shmPath.of(root))
            ) {
                try (TxnScoreboard scoreboard1 = new TxnScoreboard(TestFilesFacadeImpl.INSTANCE, 1024).ofRW(shmPath.of(root))) {
                    scoreboard1.acquireTxn(67);
                    scoreboard1.acquireTxn(68);
                    scoreboard1.acquireTxn(68);
                    scoreboard1.acquireTxn(69);
                    scoreboard1.acquireTxn(70);
                    scoreboard1.acquireTxn(71);

                    scoreboard1.releaseTxn(68);
                    Assert.assertEquals(67, scoreboard1.getMin());
                    Assert.assertFalse(scoreboard2.isTxnAvailable(68));
                    scoreboard1.releaseTxn(68);
                    Assert.assertEquals(67, scoreboard1.getMin());
                    scoreboard1.releaseTxn(67);
                    Assert.assertEquals(69, scoreboard1.getMin());

                    scoreboard1.releaseTxn(69);
                    Assert.assertEquals(70, scoreboard1.getMin());
                    scoreboard1.releaseTxn(71);
                    Assert.assertTrue(scoreboard1.isTxnAvailable(71));
                    Assert.assertEquals(70, scoreboard1.getMin());
                    scoreboard1.releaseTxn(70);
                    Assert.assertEquals(71, scoreboard1.getMin());

                    scoreboard1.acquireTxn(72);
                }
                scoreboard2.acquireTxn(72);
                Assert.assertEquals(2, scoreboard2.getActiveReaderCount(72));

                scoreboard2.releaseTxn(72);
                scoreboard2.releaseTxn(72);
                Assert.assertEquals(0, scoreboard2.getActiveReaderCount(72));

                Assert.assertTrue(scoreboard2.isTxnAvailable(77));
            }
        });
    }

    @Test
    public void testWideRange() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (
                    final Path shmPath = new Path();
                    final TxnScoreboard scoreboard = new TxnScoreboard(TestFilesFacadeImpl.INSTANCE, 1024).ofRW(shmPath.of(root))
            ) {
                scoreboard.acquireTxn(15);
                scoreboard.releaseTxn(15);
                scoreboard.acquireTxn(900992);
            }
        });
    }

    @SuppressWarnings("SameParameterValue")
    private void testHammerScoreboard(int readers, int iterations) throws Exception {
        int entryCount = Math.max(Numbers.ceilPow2(readers) * 8, Numbers.ceilPow2(iterations));
        try (
                final Path shmPath = new Path();
                final TxnScoreboard scoreboard = new TxnScoreboard(TestFilesFacadeImpl.INSTANCE, entryCount).ofRW(shmPath.of(root))
        ) {
            final CyclicBarrier barrier = new CyclicBarrier(readers + 1);
            final CountDownLatch latch = new CountDownLatch(readers + 1);
            txn = 0;
            final AtomicInteger anomaly = new AtomicInteger();

            for (int i = 0; i < readers; i++) {
                // Readers acq/release every txn number and check invariants
                Reader reader = new Reader(scoreboard, barrier, latch, anomaly, iterations, readers);
                reader.start();
            }

            // Writer constantly increments txn number
            Writer writer = new Writer(scoreboard, barrier, latch, anomaly, iterations);
            writer.start();

            latch.await();

            Assert.assertEquals(0, anomaly.get());
            for (long i = 0; i < iterations + 1; i++) {
                Assert.assertEquals(0, scoreboard.getActiveReaderCount(i));
            }
        }
    }

    private static class Reader extends Thread {

        private final AtomicInteger anomaly;
        private final CyclicBarrier barrier;
        private final int iterations;
        private final CountDownLatch latch;
        private final int readers;
        private final TxnScoreboard scoreboard;

        private Reader(TxnScoreboard scoreboard, CyclicBarrier barrier, CountDownLatch latch, AtomicInteger anomaly, int iterations, int readers) {
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
                    if (scoreboard.acquireTxn(t)) {
                        long writerMinLoc = writerMin;
                        long activeReaderCount = scoreboard.getActiveReaderCount(t);
                        if (activeReaderCount > readers + 1 || activeReaderCount < 1) {
                            LOG.errorW()
                                    .$("activeReaderCount=")
                                    .$(activeReaderCount)
                                    .$(",txn=").$(t)
                                    .$(",min=")
                                    .$(scoreboard.getMin())
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
                        scoreboard.releaseTxn(t);
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

        private Writer(TxnScoreboard scoreboard, CyclicBarrier barrier, CountDownLatch latch, AtomicInteger anomaly, int iterations) {
            this.scoreboard = scoreboard;
            this.barrier = barrier;
            this.latch = latch;
            this.anomaly = anomaly;
            this.iterations = iterations;
        }

        @Override
        public void run() {
            try {
                long publishWaitBarrier = scoreboard.getEntryCount() - 2;
                txn = 1;
                scoreboard.acquireTxn(txn);
                scoreboard.releaseTxn(txn);
                writerMin = scoreboard.getMin();

                barrier.await();
                for (int i = 0; i < iterations; i++) {
                    for (int sleepCount = 0; sleepCount < 50 && txn - scoreboard.getMin() > publishWaitBarrier; sleepCount++) {
                        // Some readers are slow and haven't release transaction yet. Give them a bit more time
                        LOG.infoW().$("slow reader release, waiting... [txn=")
                                .$(txn)
                                .$(", min=").$(scoreboard.getMin())
                                .$(", size=").$(scoreboard.getEntryCount())
                                .I$();
                        Os.sleep(100);
                    }

                    if (txn - scoreboard.getMin() > publishWaitBarrier) {
                        // Wait didn't help. Abort the test.
                        anomaly.addAndGet(1000);
                        LOG.errorW().$("slow reader release, abort [txn=")
                                .$(txn)
                                .$(", min=").$(scoreboard.getMin())
                                .$(", size=").$(scoreboard.getEntryCount())
                                .I$();
                        txn = iterations;
                        return;
                    }

                    // This is the only writer
                    //noinspection NonAtomicOperationOnVolatileField
                    txn++;
                    long nextTxn = txn;

                    // Simulate TableWriter trying to find if there are readers before the published transaction
                    if (scoreboard.acquireTxn(nextTxn)) {
                        scoreboard.releaseTxn(nextTxn);
                    }

                    writerMin = scoreboard.getMin();
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
