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

package io.questdb.test.cairo.wal;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.wal.seq.TransactionLogCursor;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Covers {@code cairo.wal.apply.block.size} and {@code cairo.wal.apply.block.memory.percent}: unset, the WAL apply
 * block keeps its old row limit (lag size and squash multiplier x max uncommitted rows); set, the block starts at
 * that size and grows within the memory percentage of the RSS limit.
 */
public class WalApplyBlockSizeTest extends AbstractCairoTest {
    // (ts TIMESTAMP, x LONG): 16 bytes per row, both estimated and measured
    private static final int RECORD_SIZE = 16;
    private static final int ROWS_PER_TXN = 10;
    private static final int TXN_COUNT = 100;

    @Override
    @Before
    public void setUp() {
        super.setUp();
        // Block growth depends on the RSS limit, which Bootstrap-based tests in the same JVM leave set
        Unsafe.setRssMemLimit(0);
    }

    @Test
    public void testBlockDoesNotGrowAtZeroPercent() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_APPLY_BLOCK_SIZE, 10L * ROWS_PER_TXN * RECORD_SIZE);
        setProperty(PropertyKey.CAIRO_WAL_APPLY_BLOCK_MEMORY_PERCENT, 0);
        Unsafe.setRssMemLimit(64 * Numbers.SIZE_1GB);
        try {
            assertCommits(10, 1_000);
        } finally {
            Unsafe.setRssMemLimit(0);
        }
    }

    @Test
    public void testBlockGrowsWithinMemoryPercent() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_APPLY_BLOCK_SIZE, 10L * ROWS_PER_TXN * RECORD_SIZE);
        Unsafe.setRssMemLimit(64 * Numbers.SIZE_1GB);
        try {
            // The limit doubles after every block it cut short: 10 + 20 + 40 txns, then the 30 left
            assertCommits(4, 1_000);
        } finally {
            Unsafe.setRssMemLimit(0);
        }
    }

    @Test
    public void testBudgetLetsBlockGrowWithoutExistingData() throws Exception {
        // Same as testBudgetStopsBlockGrowthOverLargePartition, but the txns land a day before the big partition:
        // they touch no committed data, copy nothing, and the block grows: 10 + 20 + 40 + 30 txns.
        assertCommitsOverLargePartition(4, "2024-01-01", false);
    }

    @Test
    public void testBudgetLetsMergeAppendBlockGrowOverLargePartition() throws Exception {
        // Merge-append merges only the pieces the txns land in, ~19k of the 1M rows per block (~1.9 %). The ratio
        // starts at 15 %: 150k expected copied rows do not fit the budget (~83k), so the block stays at 10 txns.
        // A lower sample moves the ratio a quarter of the way per block: 11.7 %, 9.3 %, then 7.4 % (74k) fits and
        // the block grows: 10 + 10 + 10 + 20 + 40 + the 10 left.
        assertCommitsOverLargePartition(6, "2024-01-05", true);
    }

    @Test
    public void testBudgetStopsBlockGrowthOverLargePartition() throws Exception {
        // Every txn lands in the 1M-row partition, which a classic O3 merge copies whole: 1M x 16 B read and
        // written again is past 1 % of the RSS limit, so the block stays at block.size, 10 txns.
        assertCommitsOverLargePartition(10, "2024-01-05", false);
    }

    @Test
    public void testColdCompositePartitionSingleTxnApply() throws Exception {
        // A writer opened after the partition went composite has no geometry loaded. Estimating the block must not
        // load it: PartitionGeometry reads through the thread-local Path, which is the WAL path of the apply.
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_WAL_APPLY_BLOCK_SIZE, 10L * ROWS_PER_TXN * RECORD_SIZE);
        assertMemoryLeak(() -> {
            final TableToken tableToken = createTable();
            execute("INSERT INTO t SELECT ('2024-01-05'::TIMESTAMP + x * 10_000)::TIMESTAMP, x FROM long_sequence(100_000)");
            execute("INSERT INTO t SELECT ('2024-01-06'::TIMESTAMP + x * 10_000)::TIMESTAMP, x FROM long_sequence(10)");
            drainWalQueue();
            execute("INSERT INTO t SELECT ('2024-01-05'::TIMESTAMP + x * 1_000_000 + 7)::TIMESTAMP, x FROM long_sequence(10)");
            drainWalQueue();
            try (TableWriter writer = getWriter(tableToken)) {
                Assert.assertTrue(writer.getTxWriter().isPartitionComposite(0));
            }
            engine.releaseAllWriters();

            execute("INSERT INTO t SELECT ('2024-01-05'::TIMESTAMP + x * 1_000_000 + 9)::TIMESTAMP, x FROM long_sequence(10)");
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
            assertQuery("SELECT count(), sum(x) FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            count\tsum
                            100030\t5000050165
                            """);
        });
    }

    @Test
    public void testCopyRatioLearnedFromClassicMerge() throws Exception {
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1024);
        setProperty(PropertyKey.CAIRO_WAL_APPLY_BLOCK_SIZE, 10L * ROWS_PER_TXN * RECORD_SIZE);
        assertMemoryLeak(() -> {
            final TableToken tableToken = createTable();
            execute("INSERT INTO t SELECT ('2024-01-05'::TIMESTAMP + x * 1_000_000)::TIMESTAMP, x FROM long_sequence(80_000)");
            drainWalQueue();
            try (TableWriter writer = getWriter(tableToken)) {
                Assert.assertEquals(1.0, writer.getWalBlockCopyRatioRewrite(), 0.0);
            }
            insertTxnsAt("2024-01-05T20:00:00", 10);
            drainWalQueue();
            try (TableWriter writer = getWriter(tableToken)) {
                // The split copies only the 8,000 rows from 20:00 on of the 80,000: a 0.1 sample below the ratio
                // moves it a quarter of the way, 1.0 + 0.25 x (0.1 - 1.0). The merge ratio has no sample.
                Assert.assertEquals(0.775, writer.getWalBlockCopyRatioRewrite(), 1e-9);
                Assert.assertEquals(0.15, writer.getWalBlockCopyRatioMerge(), 0.0);
            }
        });
    }

    @Test
    public void testCopyRatioLearnedFromMergeAppend() throws Exception {
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_WAL_APPLY_BLOCK_SIZE, 10L * ROWS_PER_TXN * RECORD_SIZE);
        assertMemoryLeak(() -> {
            final TableToken tableToken = createTable();
            execute("INSERT INTO t SELECT ('2024-01-05'::TIMESTAMP + x * 10_000)::TIMESTAMP, x FROM long_sequence(100_000)");
            drainWalQueue();
            try (TableWriter writer = getWriter(tableToken)) {
                Assert.assertEquals(0.15, writer.getWalBlockCopyRatioMerge(), 0.0);
            }
            insertTxnsAt("2024-01-05", 10);
            drainWalQueue();
            try (TableWriter writer = getWriter(tableToken)) {
                // The rows land in the first 909 s of the 1,000 s partition, so the merge copies 90.9 % of it: a
                // sample above the ratio replaces it. The rewrite ratio has no sample.
                Assert.assertEquals(0.909, writer.getWalBlockCopyRatioMerge(), 1e-9);
                Assert.assertEquals(1.0, writer.getWalBlockCopyRatioRewrite(), 0.0);
            }
        });
    }

    @Test
    public void testFitsWalBlockBudget() {
        // 100 rows of 16 B, nothing copied: staging 100 x (48 + 16) = 6,400, merge index 100 x 16 = 1,600
        Assert.assertTrue(TableWriter.fitsWalBlockBudget(100, 0, 16, 8_000));
        Assert.assertFalse(TableWriter.fitsWalBlockBudget(100, 0, 16, 7_999));
        // 1,000 rows copied again: merge index 1,100 x 16 = 17,600 (anonymous 24,000), page cache 6,400 + 2 x 16,000
        Assert.assertTrue(TableWriter.fitsWalBlockBudget(100, 1_000, 16, 38_400));
        Assert.assertFalse(TableWriter.fitsWalBlockBudget(100, 1_000, 16, 38_399));
    }

    @Test
    public void testHugeBlockSizeLookaheadReturnsPastDeadline() throws Exception {
        // 40 GiB of 16 B rows is ~2.7 billion rows; with 1-row txns the look-ahead txn estimate overflowed int,
        // nothing loaded and the loop spun forever past the deadline. cairo.wal.apply.block.max.rows caps it.
        setProperty(PropertyKey.CAIRO_WAL_APPLY_BLOCK_SIZE, 40 * Numbers.SIZE_1GB);
        assertLookaheadReturnsPastDeadline(false);
    }

    @Test
    public void testHugeBlockSizeLookaheadReturnsPastDeadlineWithZeroRowTxns() throws Exception {
        // Within the max rows cap, a zero-row UPDATE txn per inserted row doubles the look-ahead txn estimate
        // to ~4 billion, past int range: the estimate itself has to be clamped.
        setProperty(PropertyKey.CAIRO_WAL_APPLY_BLOCK_SIZE, 40 * Numbers.SIZE_1GB);
        setProperty(PropertyKey.CAIRO_WAL_APPLY_BLOCK_MAX_ROWS, 2_000_000_000L);
        assertLookaheadReturnsPastDeadline(true);
    }

    @Test
    public void testLookaheadTxnCountFollowsRowsPerTxn() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_APPLY_BLOCK_SIZE, 300L * RECORD_SIZE);
        assertMemoryLeak(() -> {
            final TableToken tableToken = createTable();
            insertTxn(5);
            insertTxn(15);
            insertTxn(40);
            try (TableWriter writer = loadWalTxnDetails(tableToken)) {
                // 60 rows in 3 txns: 20 rows per txn, a 300-row block is 15 txns
                Assert.assertEquals(15, writer.getWalTnxDetails().getLookaheadTxnCount());
            }

            insertTxn(1);
            insertTxn(1);
            try (TableWriter writer = loadWalTxnDetails(tableToken)) {
                // 62 rows in 5 txns: 12.4 rows per txn, rounded up to 25 txns per block
                Assert.assertEquals(25, writer.getWalTnxDetails().getLookaheadTxnCount());
            }
        });
    }

    @Test
    public void testLookaheadTxnCountIsAtLeastOne() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_APPLY_BLOCK_SIZE, 5L * RECORD_SIZE);
        assertMemoryLeak(() -> {
            final TableToken tableToken = createTable();
            insertTxn(10);
            try (TableWriter writer = loadWalTxnDetails(tableToken)) {
                // A txn bigger than the block: half a txn per block, rounded up to 1
                Assert.assertEquals(1, writer.getWalTnxDetails().getLookaheadTxnCount());
            }
        });
    }

    @Test
    public void testLookaheadTxnCountKeptWithoutRows() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_APPLY_BLOCK_SIZE, 300L * RECORD_SIZE);
        assertMemoryLeak(() -> {
            final TableToken tableToken = createTable();
            execute("ALTER TABLE t ADD COLUMN y INT");
            try (TableWriter writer = loadWalTxnDetails(tableToken)) {
                // Only a 0-row txn loaded: no rows-per-txn stats, the configured count stays
                Assert.assertEquals(configuration.getWalApplyLookAheadTransactionCount(), writer.getWalTnxDetails().getLookaheadTxnCount());
            }
        });
    }

    @Test
    public void testLookaheadTxnCountUnset() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken tableToken = createTable();
            insertTxn(10);
            try (TableWriter writer = loadWalTxnDetails(tableToken)) {
                Assert.assertEquals(configuration.getWalApplyLookAheadTransactionCount(), writer.getWalTnxDetails().getLookaheadTxnCount());
            }
        });
    }

    @Test
    public void testMaxRowsCapsBlock() throws Exception {
        // 1 MiB would take all 100 txns in one block; 50 rows cap it at 5 txns
        setProperty(PropertyKey.CAIRO_WAL_APPLY_BLOCK_SIZE, 1024 * 1024);
        setProperty(PropertyKey.CAIRO_WAL_APPLY_BLOCK_MAX_ROWS, 5 * ROWS_PER_TXN);
        assertCommits(20, 1_000);
    }

    @Test
    public void testMaxRowsCapsBlockGrowth() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_APPLY_BLOCK_SIZE, 10L * ROWS_PER_TXN * RECORD_SIZE);
        setProperty(PropertyKey.CAIRO_WAL_APPLY_BLOCK_MAX_ROWS, 25 * ROWS_PER_TXN);
        Unsafe.setRssMemLimit(64 * Numbers.SIZE_1GB);
        try {
            // The limit doubles 10 -> 20 txns, then stops at the 25-txn cap: 10 + 20 + 25 + 25 + the 20 left
            assertCommits(5, 1_000);
        } finally {
            Unsafe.setRssMemLimit(0);
        }
    }

    @Test
    public void testPressureCapDoesNotGrowBlock() throws Exception {
        // Pressure control caps blocks at 49 rows, below the 100-row block size. Those blocks are not cut by the row
        // limit, so it stays at 100 rows: the look-ahead sizes to 10 txns of 10 rows, not to a doubled limit.
        setProperty(PropertyKey.CAIRO_WAL_APPLY_BLOCK_SIZE, 10L * ROWS_PER_TXN * RECORD_SIZE);
        Unsafe.setRssMemLimit(64 * Numbers.SIZE_1GB);
        try {
            assertMemoryLeak(() -> {
                final TableToken tableToken = createTable();
                engine.getTableSequencerAPI().getTxnTracker(tableToken).getMemPressureControl().setMaxBlockRowCount(50);
                insertTxnsAt("2024-01-01", TXN_COUNT);
                drainWalQueue();
                Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));

                for (int i = 0; i < 10; i++) {
                    insertTxn(ROWS_PER_TXN);
                }
                try (TableWriter writer = loadWalTxnDetails(tableToken)) {
                    Assert.assertEquals(10, writer.getWalTnxDetails().getLookaheadTxnCount());
                }
                assertQuery("SELECT count() FROM t")
                        .noLeakCheck()
                        .expectSize()
                        .noRandomAccess()
                        .returns("count\n" + TXN_COUNT * ROWS_PER_TXN + "\n");
            });
        } finally {
            Unsafe.setRssMemLimit(0);
        }
    }

    @Test
    public void testSetBlockIsNotCutByLookAheadTxnCount() throws Exception {
        // A configured look-ahead txn count of 1 would make the apply job re-read the sequencer only when it
        // runs out of loaded txns. The look-ahead loads 45 txns (1 x 450 rows), the first block takes 30 of
        // them, and with the fixed count the second block would get only the 15 left over:
        // 30 + 15 + 30 + 30 + 15. The count adapts to one block's worth (30 txns of 10 rows), so the job
        // re-reads while fewer than 30 txns are left and every block is full: 4 x 30.
        setProperty(PropertyKey.CAIRO_WAL_APPLY_LOOK_AHEAD_TXN_COUNT, 1);
        setProperty(PropertyKey.CAIRO_WAL_SQUASH_UNCOMMITTED_ROWS_MULTIPLIER, 1);
        setProperty(PropertyKey.CAIRO_WAL_APPLY_BLOCK_SIZE, 30L * ROWS_PER_TXN * RECORD_SIZE);
        assertCommits(4, 450, 120);
    }

    @Test
    public void testSetBlockIsNotCutByTimeQuota() throws Exception {
        // The quota of 0 expires before the look-ahead loads anything past its first read of 1 txn.
        setProperty(PropertyKey.CAIRO_WAL_APPLY_TABLE_TIME_QUOTA, 0);
        setProperty(PropertyKey.CAIRO_WAL_APPLY_LOOK_AHEAD_TXN_COUNT, 1);
        setProperty(PropertyKey.CAIRO_WAL_APPLY_BLOCK_SIZE, 1024 * 1024);
        // One block, plus the commitSeqTxn() of the job run that the expired quota ejects.
        assertCommits(2, 1_000);
    }

    @Test
    public void testSetIgnoresLagSize() throws Exception {
        // 1 byte of lag would mean 1-row blocks, i.e. every txn applied on its own.
        setProperty(PropertyKey.CAIRO_WAL_MAX_LAG_SIZE, 1);
        setProperty(PropertyKey.CAIRO_WAL_APPLY_BLOCK_SIZE, 1024 * 1024);
        assertCommits(1, 1_000);
    }

    @Test
    public void testSetIgnoresSquashMultiplier() throws Exception {
        // Unset, the same table applies in blocks of 2 txns, see testUnsetUsesSquashMultiplier.
        setProperty(PropertyKey.CAIRO_WAL_SQUASH_UNCOMMITTED_ROWS_MULTIPLIER, 1);
        setProperty(PropertyKey.CAIRO_WAL_APPLY_BLOCK_SIZE, 1024 * 1024);
        assertCommits(1, 20);
    }

    @Test
    public void testSetLimitsBlock() throws Exception {
        // Exactly 10 txns fit a block, the 11th would exceed it. Without an RSS limit (tests run with none)
        // the block does not grow.
        setProperty(PropertyKey.CAIRO_WAL_APPLY_BLOCK_SIZE, 10L * ROWS_PER_TXN * RECORD_SIZE);
        assertCommits(10, 1_000);
    }

    @Test
    public void testUnsetUsesLagSize() throws Exception {
        // 10 txns worth of lag size; unset block size falls back to it, like before the setting existed.
        setProperty(PropertyKey.CAIRO_WAL_MAX_LAG_SIZE, 10L * ROWS_PER_TXN * RECORD_SIZE);
        assertCommits(10, 1_000);
    }

    @Test
    public void testUnsetUsesSquashMultiplier() throws Exception {
        // 1 x 20 max uncommitted rows: blocks of 2 txns.
        setProperty(PropertyKey.CAIRO_WAL_SQUASH_UNCOMMITTED_ROWS_MULTIPLIER, 1);
        assertCommits(50, 20);
    }

    private void assertCommits(int expectedCommits, int maxUncommittedRows) throws Exception {
        assertCommits(expectedCommits, maxUncommittedRows, TXN_COUNT);
    }

    private void assertCommits(int expectedCommits, int maxUncommittedRows, int txnCount) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL WITH maxUncommittedRows=" + maxUncommittedRows);
            final TableToken tableToken = engine.verifyTableName("t");
            drainWalQueue();
            final long txnBefore = getTableTxn(tableToken);

            // Every txn spans the whole time range of the others, so no two of them are in order.
            for (int i = 0; i < txnCount; i++) {
                execute(
                        "INSERT INTO t SELECT ('2024-01-01'::TIMESTAMP + (x - 1) * 100_000_000 + " + i
                                + " * 1_000_000)::TIMESTAMP, x FROM long_sequence(" + ROWS_PER_TXN + ")"
                );
            }
            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
            Assert.assertEquals(expectedCommits, getTableTxn(tableToken) - txnBefore);
            assertQuery("SELECT count(), min(ts), max(ts), sum(x) FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\tmin\tmax\tsum\n"
                            + txnCount * ROWS_PER_TXN + "\t2024-01-01T00:00:00.000000Z\t"
                            + Micros.toUSecString(MicrosFormatUtils.parseTimestamp("2024-01-01T00:00:00.000000Z")
                            + (ROWS_PER_TXN - 1) * 100_000_000L + (txnCount - 1) * 1_000_000L) + "\t"
                            + txnCount * (ROWS_PER_TXN * (ROWS_PER_TXN + 1) / 2) + "\n");
            assertQuery("SELECT count() FROM (SELECT ts, lag(ts) OVER () prev FROM t) WHERE ts < prev")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            count
                            0
                            """);
        });
    }

    private void assertCommitsOverLargePartition(int expectedCommits, String txnDay, boolean isMergeAppend) throws Exception {
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, String.valueOf(isMergeAppend));
        setProperty(PropertyKey.CAIRO_WAL_APPLY_BLOCK_SIZE, 10L * ROWS_PER_TXN * RECORD_SIZE);
        setProperty(PropertyKey.CAIRO_WAL_APPLY_BLOCK_MEMORY_PERCENT, 1);
        setProperty(PropertyKey.WAL_APPLY_WORKER_COUNT, 2);
        assertMemoryLeak(() -> {
            final TableToken tableToken = createTable();
            execute("INSERT INTO t SELECT ('2024-01-05'::TIMESTAMP + x * 50_000)::TIMESTAMP, x FROM long_sequence(1_000_000)");
            drainWalQueue();
            // 1 % of 512 MB over 2 WAL apply workers is 2,684,354 B: a block's own staging fits many times over,
            // page cache for ~83k copied rows of 16 B (read and written) does too, the whole 1M-row partition does not
            Unsafe.setRssMemLimit(512 * Numbers.SIZE_1MB);
            try {
                final long txnBefore = getTableTxn(tableToken);
                insertTxnsAt(txnDay, TXN_COUNT);
                drainWalQueue();
                Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
                Assert.assertEquals(expectedCommits, getTableTxn(tableToken) - txnBefore);
            } finally {
                Unsafe.setRssMemLimit(0);
            }
            assertQuery("SELECT count() FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n" + (1_000_000 + TXN_COUNT * ROWS_PER_TXN) + "\n");
        });
    }

    private void assertLookaheadReturnsPastDeadline(boolean hasZeroRowTxns) throws Exception {
        assertMemoryLeak(() -> {
            final TableToken tableToken = createTable();
            final int rowCount = hasZeroRowTxns ? 150 : 300;
            for (int i = 0; i < rowCount; i++) {
                execute("INSERT INTO t VALUES ('2024-01-01'::TIMESTAMP + " + (rowCount - i) + "L, " + i + ")");
                if (hasZeroRowTxns) {
                    execute("UPDATE t SET x = x WHERE x < 0");
                }
            }
            try (TableWriter writer = getWriter(tableToken)) {
                final AtomicBoolean isDone = new AtomicBoolean();
                final AtomicReference<Throwable> error = new AtomicReference<>();
                // Reads on another thread so that a regression fails the test instead of hanging it
                final Thread reader = new Thread(() -> {
                    try (TransactionLogCursor cursor = engine.getTableSequencerAPI().getCursor(tableToken, writer.getAppliedSeqTxn())) {
                        // A deadline in the past, as when the table time quota has run out
                        writer.readWalTxnDetails(cursor, 0);
                        isDone.set(true);
                    } catch (Throwable th) {
                        error.set(th);
                    } finally {
                        Path.clearThreadLocals();
                    }
                });
                reader.setDaemon(true);
                reader.start();
                reader.join(60_000);
                Assert.assertNull(error.get());
                Assert.assertTrue("look-ahead read did not return", isDone.get());
                Assert.assertEquals(writer.getAppliedSeqTxn() + 300, writer.getWalTnxDetails().getLastSeqTxn());
            }
            drainWalQueue();
            assertQuery("SELECT count() FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n" + rowCount + "\n");
        });
    }

    private TableToken createTable() throws Exception {
        execute("CREATE TABLE t (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
        final TableToken tableToken = engine.verifyTableName("t");
        drainWalQueue();
        return tableToken;
    }

    private long getTableTxn(TableToken tableToken) {
        try (TableReader reader = engine.getReader(tableToken)) {
            return reader.getTxn();
        }
    }

    private void insertTxn(int rows) throws Exception {
        execute("INSERT INTO t (ts, x) SELECT ('2024-01-01'::TIMESTAMP + x * 1_000_000)::TIMESTAMP, x FROM long_sequence(" + rows + ")");
    }

    private void insertTxnsAt(String day, int txnCount) throws Exception {
        // Every txn spans the whole time range of the others, so no two of them are in order
        for (int i = 0; i < txnCount; i++) {
            execute(
                    "INSERT INTO t SELECT ('" + day + "'::TIMESTAMP + (x - 1) * 100_000_000 + " + i
                            + " * 1_000_000 + 7)::TIMESTAMP, x FROM long_sequence(" + ROWS_PER_TXN + ")"
            );
        }
    }

    private TableWriter loadWalTxnDetails(TableToken tableToken) {
        final TableWriter writer = getWriter(tableToken);
        try (TransactionLogCursor cursor = engine.getTableSequencerAPI().getCursor(tableToken, writer.getAppliedSeqTxn())) {
            writer.readWalTxnDetails(cursor);
        } catch (Throwable th) {
            writer.close();
            throw th;
        }
        return writer;
    }
}
