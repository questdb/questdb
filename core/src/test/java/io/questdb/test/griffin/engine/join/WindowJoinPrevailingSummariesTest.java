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


package io.questdb.test.griffin.engine.join;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.griffin.engine.join.WindowJoinPrevailingSummaries;
import io.questdb.griffin.engine.join.WindowJoinTimeFrameHelper;
import io.questdb.std.DirectIntIntHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import io.questdb.std.Rows;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The per-block summaries of a window join's prevailing rows, shared by the workers reducing its
 * page frames, against a brute-force oracle over an in-memory slave: every (block, key) answer, the
 * scan that stops at the key it is asked for and resumes there, workers racing for the same blocks,
 * a worker whose scan throws while another waits for the block, and cancellation of both the scan
 * and the wait.
 */
public class WindowJoinPrevailingSummariesTest extends AbstractCairoTest {
    // AsyncWindowJoinFastAtom's slave lookup map keys: slave key + 2, NULL as 1
    private static final int KEY_SHIFT = 2;
    private static final int MASTER_KEY_OFFSET = 1000;
    private static final int NULL_MAP_KEY = 1;

    @Test
    public void testBlocksOfOneFrameMatchOracle() throws Exception {
        assertMemoryLeak(() -> assertSingleThreadMatchesOracle(30, 0));
    }

    @Test
    public void testBlocksOfSeveralFramesMatchOracle() throws Exception {
        // 60k joinable keys most of which never occur: more keys than the entry budget allows one
        // frame per block for, and blocks the scan can never complete early
        assertMemoryLeak(() -> assertSingleThreadMatchesOracle(30, 60_000));
    }

    @Test
    public void testCancelledScanResumesForTheNextReader() throws Exception {
        assertMemoryLeak(() -> {
            final Slave slave = Slave.twoFrames();
            try (DirectIntIntHashMap lookup = slave.newLookupMap(0); WindowJoinPrevailingSummaries summaries = new WindowJoinPrevailingSummaries()) {
                summaries.of(slave.frameCount(), lookup, null);
                final AtomicBooleanCircuitBreaker cancelled = new AtomicBooleanCircuitBreaker(engine);
                cancelled.cancel();
                final SlaveReader reader = slave.newReader();
                final int key = Slave.TWO_FRAMES_RARE_KEY;
                try {
                    summaries.lastRowIdInBlock(1, masterKeyOf(key), reader, reader.record, 0, cancelled);
                    Assert.fail("the scan must observe the cancelled query");
                } catch (CairoException e) {
                    Assert.assertTrue(e.isCancellation());
                }
                Assert.assertEquals(Rows.toRowID(1, 5), summaries.lastRowIdInBlock(1, masterKeyOf(key), reader, reader.record, 0, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER));
            }
        });
    }

    @Test
    public void testConcurrentLookupsMatchOracle() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            for (int round = 0; round < 10; round++) {
                final Slave slave = Slave.create(rnd, 24, 300, 10 + rnd.nextInt(60));
                final int extraKeys = rnd.nextBoolean() ? 0 : 60_000;
                try (DirectIntIntHashMap lookup = slave.newLookupMap(extraKeys); WindowJoinPrevailingSummaries summaries = new WindowJoinPrevailingSummaries()) {
                    summaries.of(slave.frameCount(), lookup, null);
                    final int threadCount = 8;
                    final CyclicBarrier start = new CyclicBarrier(threadCount);
                    final AtomicReference<Throwable> failure = new AtomicReference<>();
                    final Thread[] threads = new Thread[threadCount];
                    final long seed0 = rnd.nextLong();
                    final long seed1 = rnd.nextLong();
                    for (int t = 0; t < threadCount; t++) {
                        final int threadIndex = t;
                        threads[t] = new Thread(() -> {
                            try {
                                final Rnd threadRnd = new Rnd(seed0 + threadIndex, seed1);
                                final SlaveReader reader = slave.newReader();
                                start.await();
                                for (int i = 0; i < 500; i++) {
                                    // most workers ask for the same few blocks, as page frames of one
                                    // hour do
                                    final int block = threadRnd.nextInt(4) == 0
                                            ? threadRnd.nextInt(summaries.getBlockCount())
                                            : summaries.getBlockCount() - 1 - threadRnd.nextInt(2);
                                    final int slaveKey = slave.randomKey(threadRnd);
                                    final long actual = summaries.lastRowIdInBlock(block, masterKeyOf(slaveKey), reader, reader.record, 0, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
                                    final long expected = slave.lastRowIdInBlock(summaries, block, slaveKey);
                                    if (actual != expected) {
                                        throw new AssertionError("block=" + block + ", key=" + slaveKey + ", expected=" + expected + ", actual=" + actual);
                                    }
                                }
                            } catch (Throwable th) {
                                failure.compareAndSet(null, th);
                            }
                        });
                        threads[t].start();
                    }
                    for (Thread thread : threads) {
                        thread.join();
                    }
                    if (failure.get() != null) {
                        throw new AssertionError(failure.get());
                    }
                }
            }
        });
    }

    @Test
    public void testScanStopsAtTheKeyAndResumesThere() throws Exception {
        assertMemoryLeak(() -> {
            // one block of one frame, 1000 rows: key 1 at row 990, key 2 at row 10, key 0 elsewhere
            final int[][] keys = new int[1][1000];
            keys[0][990] = 1;
            keys[0][10] = 2;
            final Slave slave = new Slave(keys, 3);
            try (DirectIntIntHashMap lookup = slave.newLookupMap(100); WindowJoinPrevailingSummaries summaries = new WindowJoinPrevailingSummaries()) {
                summaries.of(1, lookup, null);
                final SlaveReader reader = slave.newReader();
                Assert.assertEquals(Rows.toRowID(0, 990), summaries.lastRowIdInBlock(0, masterKeyOf(1), reader, reader.record, 0, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER));
                // stopped at the key, not at the block's start
                Assert.assertEquals(10, reader.rowsRead);
                Assert.assertEquals(Rows.toRowID(0, 999), summaries.lastRowIdInBlock(0, masterKeyOf(0), reader, reader.record, 0, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER));
                Assert.assertEquals(10, reader.rowsRead);
                Assert.assertEquals(Rows.toRowID(0, 10), summaries.lastRowIdInBlock(0, masterKeyOf(2), reader, reader.record, 0, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER));
                // resumed below row 990, not from the block's end
                Assert.assertEquals(990, reader.rowsRead);
                // a key that can join but has no row: the rest of the block, once
                Assert.assertEquals(Long.MIN_VALUE, summaries.lastRowIdInBlock(0, masterKeyOf(1003), reader, reader.record, 0, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER));
                Assert.assertEquals(1000, reader.rowsRead);
                Assert.assertEquals(Long.MIN_VALUE, summaries.lastRowIdInBlock(0, masterKeyOf(1004), reader, reader.record, 0, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER));
                Assert.assertEquals(1000, reader.rowsRead);
                // a key the slave cannot hold
                Assert.assertEquals(Long.MIN_VALUE, summaries.lastRowIdInBlock(0, masterKeyOf(500), reader, reader.record, 0, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER));
            }
        });
    }

    @Test
    public void testWaitForAnotherWorkersScanIsCancellable() throws Exception {
        assertMemoryLeak(() -> {
            final Slave slave = Slave.twoFrames();
            try (DirectIntIntHashMap lookup = slave.newLookupMap(100); WindowJoinPrevailingSummaries summaries = new WindowJoinPrevailingSummaries()) {
                summaries.of(slave.frameCount(), lookup, null);
                Assert.assertEquals(2, summaries.getBlockCount());
                final CountDownLatch scanning = new CountDownLatch(1);
                final CountDownLatch release = new CountDownLatch(1);
                // the first worker stalls inside the scan of block 1, holding it
                final SlaveReader stalled = slave.newReader();
                stalled.onRow = rows -> {
                    if (rows == 100) {
                        scanning.countDown();
                        awaitQuietly(release);
                    }
                };
                final AtomicReference<Throwable> stalledFailure = new AtomicReference<>();
                final AtomicReference<Long> stalledResult = new AtomicReference<>();
                final Thread stalledThread = new Thread(() -> {
                    try {
                        // a key that can join but has no row: the scan walks the whole block
                        stalledResult.set(summaries.lastRowIdInBlock(1, masterKeyOf(Slave.TWO_FRAMES_KEYS + 1000), stalled, stalled.record, 0, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER));
                    } catch (Throwable th) {
                        stalledFailure.set(th);
                    }
                });
                stalledThread.start();
                Assert.assertTrue(scanning.await(30, TimeUnit.SECONDS));

                // a cancelled worker waiting for the same block gives up instead of waiting it out
                final AtomicBooleanCircuitBreaker cancelled = new AtomicBooleanCircuitBreaker(engine);
                final AtomicReference<Throwable> waiterFailure = new AtomicReference<>();
                final Thread waiter = new Thread(() -> {
                    final SlaveReader reader = slave.newReader();
                    try {
                        summaries.lastRowIdInBlock(1, masterKeyOf(Slave.TWO_FRAMES_RARE_KEY), reader, reader.record, 0, cancelled);
                    } catch (Throwable th) {
                        waiterFailure.set(th);
                    }
                });
                waiter.start();
                cancelled.cancel();
                waiter.join(TimeUnit.SECONDS.toMillis(30));
                Assert.assertFalse("the waiter is still waiting", waiter.isAlive());
                Assert.assertTrue(String.valueOf(waiterFailure.get()), waiterFailure.get() instanceof CairoException && ((CairoException) waiterFailure.get()).isCancellation());

                release.countDown();
                stalledThread.join();
                Assert.assertNull(stalledFailure.get());
                Assert.assertEquals(Long.MIN_VALUE, (long) stalledResult.get());
            }
        });
    }

    @Test
    public void testWorkerWhoseScanFailsHandsTheBlockToTheWaiter() throws Exception {
        assertMemoryLeak(() -> {
            final Slave slave = Slave.twoFrames();
            try (DirectIntIntHashMap lookup = slave.newLookupMap(0); WindowJoinPrevailingSummaries summaries = new WindowJoinPrevailingSummaries()) {
                summaries.of(slave.frameCount(), lookup, null);
                final CountDownLatch scanning = new CountDownLatch(1);
                final CountDownLatch release = new CountDownLatch(1);
                final SlaveReader failing = slave.newReader();
                failing.onRow = rows -> {
                    if (rows == 100) {
                        scanning.countDown();
                        awaitQuietly(release);
                        throw CairoException.nonCritical().put("injected");
                    }
                };
                // its only row is near the start of block 1: the scan walks most of the block
                final int key = Slave.TWO_FRAMES_RARE_KEY;
                final AtomicReference<Throwable> failingFailure = new AtomicReference<>();
                final Thread failingThread = new Thread(() -> {
                    try {
                        summaries.lastRowIdInBlock(1, masterKeyOf(key), failing, failing.record, 0, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
                    } catch (Throwable th) {
                        failingFailure.set(th);
                    }
                });
                failingThread.start();
                Assert.assertTrue(scanning.await(30, TimeUnit.SECONDS));

                final AtomicReference<Throwable> waiterFailure = new AtomicReference<>();
                final AtomicReference<Long> waiterResult = new AtomicReference<>();
                final AtomicInteger waiterRows = new AtomicInteger();
                final Thread waiter = new Thread(() -> {
                    final SlaveReader reader = slave.newReader();
                    try {
                        waiterResult.set(summaries.lastRowIdInBlock(1, masterKeyOf(key), reader, reader.record, 0, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER));
                        waiterRows.set(reader.rowsRead);
                    } catch (Throwable th) {
                        waiterFailure.set(th);
                    }
                });
                waiter.start();
                release.countDown();
                failingThread.join();
                waiter.join();
                TestUtils.assertContains(String.valueOf(failingFailure.get()), "injected");
                Assert.assertNull(waiterFailure.get());
                Assert.assertEquals(slave.lastRowIdInBlock(summaries, 1, key), (long) waiterResult.get());
                // the waiter resumed where the failed scan stopped: the 99 rows it completed are not
                // scanned again
                Assert.assertEquals(slave.rowsAbove(summaries, 1, key) - 99, waiterRows.get());
            }
        });
    }

    private static void assertSingleThreadMatchesOracle(int keyCount, int extraKeys) {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        final Slave slave = Slave.create(rnd, 40, 200, keyCount);
        try (DirectIntIntHashMap lookup = slave.newLookupMap(extraKeys); WindowJoinPrevailingSummaries summaries = new WindowJoinPrevailingSummaries()) {
            summaries.of(slave.frameCount(), lookup, null);
            Assert.assertTrue(summaries.getBlockCount() > 1);
            if (extraKeys > 0) {
                Assert.assertTrue(summaries.getFramesPerBlock() > 1);
            } else {
                Assert.assertEquals(1, summaries.getFramesPerBlock());
            }
            final SlaveReader reader = slave.newReader();
            for (int i = 0; i < 5000; i++) {
                final int block = rnd.nextInt(summaries.getBlockCount());
                final int slaveKey = slave.randomKey(rnd);
                Assert.assertEquals(
                        "block=" + block + ", key=" + slaveKey,
                        slave.lastRowIdInBlock(summaries, block, slaveKey),
                        summaries.lastRowIdInBlock(block, masterKeyOf(slaveKey), reader, reader.record, 0, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER)
                );
            }
        }
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            Assert.assertTrue(latch.await(30, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static int masterKeyOf(int slaveKey) {
        return slaveKey == StaticSymbolTable.VALUE_IS_NULL ? StaticSymbolTable.VALUE_IS_NULL : slaveKey + MASTER_KEY_OFFSET;
    }

    @FunctionalInterface
    private interface RowListener {
        void onRow(int rowsRead);
    }

    // An in-memory slave: per time frame, the symbol key of each row; NULL is VALUE_IS_NULL.
    private static class Slave {
        static final int TWO_FRAMES_KEYS = 20;
        static final int TWO_FRAMES_RARE_KEY = 15;
        private final int keyCount;
        private final int[][] keys;

        Slave(int[][] keys, int keyCount) {
            this.keys = keys;
            this.keyCount = keyCount;
        }

        static Slave create(Rnd rnd, int frameCount, int maxRows, int keyCount) {
            final int[][] keys = new int[frameCount][];
            for (int f = 0; f < frameCount; f++) {
                // some frames are empty
                final int rows = rnd.nextInt(8) == 0 ? 0 : 1 + rnd.nextInt(maxRows);
                keys[f] = new int[rows];
                for (int r = 0; r < rows; r++) {
                    // skewed: high keys are rare; some NULLs; some keys the master does not hold
                    final int k = rnd.nextInt(keyCount) * rnd.nextInt(keyCount) / keyCount;
                    keys[f][r] = rnd.nextInt(50) == 0 ? StaticSymbolTable.VALUE_IS_NULL : (rnd.nextInt(40) == 0 ? keyCount + rnd.nextInt(5) : k);
                }
            }
            return new Slave(keys, keyCount);
        }

        // two frames of 2000 rows, keys 0..9 in turn, and the rare key once, at row 5 of frame 1
        static Slave twoFrames() {
            final int[][] keys = new int[2][2000];
            for (int f = 0; f < 2; f++) {
                for (int r = 0; r < 2000; r++) {
                    keys[f][r] = r % 10;
                }
            }
            keys[1][5] = TWO_FRAMES_RARE_KEY;
            return new Slave(keys, TWO_FRAMES_KEYS);
        }

        int frameCount() {
            return keys.length;
        }

        long lastRowId(int frame, int slaveKey) {
            for (int r = keys[frame].length - 1; r >= 0; r--) {
                if (keys[frame][r] == slaveKey) {
                    return Rows.toRowID(frame, r);
                }
            }
            return Long.MIN_VALUE;
        }

        long lastRowIdInBlock(WindowJoinPrevailingSummaries summaries, int block, int slaveKey) {
            if (slaveKey >= keyCount) {
                // not in the lookup map: cannot join
                return Long.MIN_VALUE;
            }
            final int lo = summaries.getFirstFrameOf(block);
            for (int f = Math.min(lo + summaries.getFramesPerBlock(), keys.length) - 1; f >= lo; f--) {
                final long rowId = lastRowId(f, slaveKey);
                if (rowId != Long.MIN_VALUE) {
                    return rowId;
                }
            }
            return Long.MIN_VALUE;
        }

        // the rows a backward scan of the block reads to reach the key's last row
        int rowsAbove(WindowJoinPrevailingSummaries summaries, int block, int slaveKey) {
            final int lo = summaries.getFirstFrameOf(block);
            int rows = 0;
            for (int f = Math.min(lo + summaries.getFramesPerBlock(), keys.length) - 1; f >= lo; f--) {
                for (int r = keys[f].length - 1; r >= 0; r--) {
                    rows++;
                    if (keys[f][r] == slaveKey) {
                        return rows;
                    }
                }
            }
            return rows;
        }

        // slave key + 2 -> master key for keys 0..keyCount-1 and NULL, plus extra keys no row holds
        DirectIntIntHashMap newLookupMap(int extraKeys) {
            final DirectIntIntHashMap map = new DirectIntIntHashMap(16, 0.7, 0, StaticSymbolTable.VALUE_NOT_FOUND, MemoryTag.NATIVE_UNORDERED_MAP);
            for (int k = 0; k < keyCount; k++) {
                map.put(k + KEY_SHIFT, masterKeyOf(k));
            }
            for (int k = 0; k < extraKeys; k++) {
                final int slaveKey = keyCount + 1000 + k;
                map.put(slaveKey + KEY_SHIFT, masterKeyOf(slaveKey));
            }
            map.put(NULL_MAP_KEY, StaticSymbolTable.VALUE_IS_NULL);
            return map;
        }

        SlaveReader newReader() {
            return new SlaveReader(keys);
        }

        int randomKey(Rnd rnd) {
            // mostly keys that occur, and NULL, and keys the slave does not hold
            final int pick = rnd.nextInt(20);
            if (pick == 0) {
                return StaticSymbolTable.VALUE_IS_NULL;
            }
            if (pick == 1) {
                return keyCount + rnd.nextInt(5);
            }
            return rnd.nextInt(keyCount);
        }
    }

    // A worker's view of the in-memory slave, through the helper's methods the summaries use.
    private static class SlaveReader extends WindowJoinTimeFrameHelper {
        private final int[][] keys;
        private int frame;
        private RowListener onRow;
        private long row;
        private final Record record = new Record() {
            @Override
            public int getInt(int col) {
                return keys[frame][(int) row];
            }
        };
        private int rowsRead;

        SlaveReader(int[][] keys) {
            super(0, 1);
            this.keys = keys;
        }

        @Override
        public long getTimeFrameRowHi() {
            return keys[frame].length;
        }

        @Override
        public long getTimeFrameRowLo() {
            return 0;
        }

        @Override
        public long openFrame(int frameIndex) {
            frame = frameIndex;
            return keys[frameIndex].length;
        }

        @Override
        public void recordAt(int frameIndex, long rowIndex) {
            frame = frameIndex;
            row = rowIndex;
        }

        @Override
        public void recordAtRowIndex(long rowIndex) {
            row = rowIndex;
            rowsRead++;
            if (onRow != null) {
                onRow.onRow(rowsRead);
            }
        }
    }
}
