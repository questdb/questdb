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

package io.questdb.test.cairo.covering;

import io.questdb.PropertyKey;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.idx.PostingIndexWriter;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.test.AbstractCairoTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * A merge-append table (composite partitions, {@code cairo.o3.partition.merge.append.enabled}) writes
 * every commit at the TAIL of the partition's column files and takes no WAL lag, so a covering POSTING
 * index has to grow the same way: one generation per commit, appended after the rows are on disk and
 * carrying their covered values - never a rebuild of the partition's whole covered sidecar, whose cost
 * grows with the partition. This holds for every shape a commit can take here: the in-place append of
 * the last partition, a backdated batch merged into a piece, a batch landing in an older partition, a
 * dedup merge that supersedes rows, and a replace-range commit that drops them.
 * <p>
 * Each case checks two things: the counters ({@code COVERING_FASTLAG_COMMIT_COUNT} advances, one per
 * covering index per commit, and {@code COVERING_FULL_RESEAL_COUNT} stays at 0), and that a covered
 * read returns exactly what the base column holds, per symbol, including NULLs.
 */
public class CoveringIndexMergeAppendTest extends AbstractCairoTest {
    private static final int SYMBOLS = 5;

    @Before
    public void enableCoveringCounters() {
        PostingIndexWriter.COVERING_COUNTERS_ENABLED = true;
        resetCoveringCounters();
    }

    @After
    public void disableCoveringCounters() {
        PostingIndexWriter.COVERING_COUNTERS_ENABLED = false;
    }

    @Test
    public void testAscendingSingleTxnCommitsAppendOneGenerationEach() throws Exception {
        assertMemoryLeak(() -> {
            createTable("YEAR");
            bootstrapPartition();
            final int commits = 12;
            for (int c = 1; c <= commits; c++) {
                insertAscending(c * 100, 100);
                drainWalQueue();
            }
            assertNotSuspended();
            // One covering index, one partition: exactly one appended generation per commit, and not a
            // single sidecar rebuild.
            Assert.assertEquals(commits, PostingIndexWriter.COVERING_FASTLAG_COMMIT_COUNT.get());
            Assert.assertEquals(0, PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.get());
            assertCoveredMatchesBase();
        });
    }

    @Test
    public void testBackdatedBatchIntoOlderPartitionAppendsOneGeneration() throws Exception {
        assertMemoryLeak(() -> {
            createTable("DAY");
            // Two days, so the backdated batch lands in a partition that is not the active one.
            insertAscending(0, 300);
            drainWalQueue();
            insertAt("2024-01-02T00:00:00Z", 1_000, 200);
            drainWalQueue();
            assertCoveredMatchesBase();
            resetCoveringCounters();

            // Into the middle of day one: a MERGE of the piece it lands in, run on an O3 worker.
            insertAt("2024-01-01T00:01:30Z", 5_000, 80);
            drainWalQueue();
            assertNotSuspended();
            Assert.assertEquals(1, PostingIndexWriter.COVERING_FASTLAG_COMMIT_COUNT.get());
            Assert.assertEquals(0, PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.get());
            assertCoveredMatchesBase();

            // And one that lands before every row day one holds: a new piece ahead of the rest.
            insertAt("2023-12-31T23:59:59.999999Z", 6_000, 1);
            drainWalQueue();
            assertNotSuspended();
            Assert.assertEquals(2, PostingIndexWriter.COVERING_FASTLAG_COMMIT_COUNT.get());
            Assert.assertEquals(0, PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.get());
            assertCoveredMatchesBase();

            // A partition that does not exist yet is not merged into but created, by the O3 path that
            // builds a fresh directory, and its covering sidecar is built once with it - over the batch
            // alone, since that is all the partition holds.
            insertAt("2023-12-30T12:00:00Z", 7_000, 30);
            drainWalQueue();
            assertNotSuspended();
            Assert.assertEquals(1, PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.get());
            assertCoveredMatchesBase();
        });
    }

    @Test
    public void testBackdatedBatchMergesWithoutReseal() throws Exception {
        assertMemoryLeak(() -> {
            createTable("YEAR");
            for (int c = 0; c < 4; c++) {
                insertAscending(c * 100, 100);
                drainWalQueue();
            }
            resetCoveringCounters();

            // Lands inside the rows already there: the piece it hits is merged with it and rewritten at
            // the tail, and the partition becomes composite.
            insertAt("2024-01-01T00:02:30Z", 10_000, 50);
            drainWalQueue();
            assertNotSuspended();
            Assert.assertEquals(1, PostingIndexWriter.COVERING_FASTLAG_COMMIT_COUNT.get());
            Assert.assertEquals(0, PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.get());
            assertCoveredMatchesBase();

            // The last partition is composite now, so an ascending commit is no longer an in-place
            // append but a composite APPEND, published by the O3 worker rather than the seal sweep.
            for (int c = 4; c < 8; c++) {
                insertAscending(c * 100, 100);
                drainWalQueue();
            }
            assertNotSuspended();
            Assert.assertEquals(5, PostingIndexWriter.COVERING_FASTLAG_COMMIT_COUNT.get());
            Assert.assertEquals(0, PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.get());
            assertCoveredMatchesBase();
        });
    }

    @Test
    public void testCoveringIndexOnColumnWithTopInOlderPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, value DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t SELECT dateadd('s', x::INT, '2024-01-01T00:00:00Z'::TIMESTAMP), x::DOUBLE
                    FROM long_sequence(200)
                    """);
            drainWalQueue();
            // sym joins the table with day one already holding rows, so in day one its top sits at the
            // partition's size: the column has no data there. A second day keeps day one out of the
            // active slot.
            execute("ALTER TABLE t ADD COLUMN sym SYMBOL");
            execute("ALTER TABLE t ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (value)");
            execute("""
                    INSERT INTO t SELECT dateadd('s', x::INT, '2024-01-02T00:00:00Z'::TIMESTAMP), (1000 + x)::DOUBLE, 'S' || (x % 5)
                    FROM long_sequence(100)
                    """);
            drainWalQueue();
            assertNotSuspended();
            resetCoveringCounters();

            // The first rows of sym in day one arrive by a backdated batch, above the top and below every
            // row the covered column already has.
            execute("""
                    INSERT INTO t SELECT dateadd('s', (300 + x)::INT, '2024-01-01T00:00:00Z'::TIMESTAMP), (2000 + x)::DOUBLE, 'S' || (x % 5)
                    FROM long_sequence(60)
                    """);
            drainWalQueue();
            assertNotSuspended();
            Assert.assertEquals(1, PostingIndexWriter.COVERING_FASTLAG_COMMIT_COUNT.get());
            Assert.assertEquals(0, PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.get());
            assertCoveredMatchesBase();
        });
    }

    @Test
    public void testDedupMergeSupersedesRowsWithoutReseal() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (value), value DOUBLE)"
                    + " TIMESTAMP(ts) PARTITION BY YEAR WAL DEDUP UPSERT KEYS(ts, sym)");
            insertAscending(0, 200);
            drainWalQueue();
            resetCoveringCounters();

            // Same keys, new values, for a stretch in the middle: the merge writes the surviving rows
            // at the tail and the superseded ones become dead space. Their old index entries stay and
            // must never surface through a covered read.
            execute("""
                    INSERT INTO t SELECT dateadd('s', (50 + x)::INT, '2024-01-01T00:00:00Z'::TIMESTAMP),
                        'S' || ((50 + x) % 5), (100000 + x)::DOUBLE
                    FROM long_sequence(40)
                    """);
            drainWalQueue();
            assertNotSuspended();
            Assert.assertEquals(1, PostingIndexWriter.COVERING_FASTLAG_COMMIT_COUNT.get());
            Assert.assertEquals(0, PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.get());
            assertCoveredMatchesBase();
            assertQuery("SELECT count() c, min(value) lo, max(value) hi FROM t WHERE value >= 100000")
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            c\tlo\thi
                            40\t100001.0\t100040.0
                            """);
        });
    }

    @Test
    public void testIndexAddedThenAppendedAndMerged() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL, value DOUBLE) TIMESTAMP(ts) PARTITION BY YEAR WAL");
            insertAscending(0, 300);
            drainWalQueue();
            // ADD INDEX builds the index through the writer's own live indexer and leaves it holding the
            // covered sidecar files it wrote. The commits below extend those same files through another
            // writer instance; the live one must not trim them back to what it last wrote when it lets
            // go of them.
            execute("ALTER TABLE t ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (value)");
            drainWalQueue();
            assertCoveredMatchesBase();
            resetCoveringCounters();

            // In-place append of the last partition, published by the seal sweep.
            insertAscending(300, 100);
            drainWalQueue();
            assertNotSuspended();
            Assert.assertEquals(1, PostingIndexWriter.COVERING_FASTLAG_COMMIT_COUNT.get());
            Assert.assertEquals(0, PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.get());
            assertCoveredMatchesBase();

            // Backdated batch, merged by the O3 worker: the partition becomes composite.
            insertAt("2024-01-01T00:02:30Z", 10_000, 50);
            drainWalQueue();
            assertNotSuspended();
            Assert.assertEquals(2, PostingIndexWriter.COVERING_FASTLAG_COMMIT_COUNT.get());
            Assert.assertEquals(0, PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.get());
            assertCoveredMatchesBase();
        });
    }

    @Test
    public void testIndexAddedThenBackdatedBatchMerged() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL, value DOUBLE) TIMESTAMP(ts) PARTITION BY YEAR WAL");
            insertAscending(0, 300);
            drainWalQueue();
            execute("ALTER TABLE t ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (value)");
            drainWalQueue();
            assertCoveredMatchesBase();
            resetCoveringCounters();

            // Straight from ADD INDEX to a merge run by the O3 worker, with no in-place append between.
            insertAt("2024-01-01T00:02:30Z", 10_000, 50);
            drainWalQueue();
            assertNotSuspended();
            Assert.assertEquals(1, PostingIndexWriter.COVERING_FASTLAG_COMMIT_COUNT.get());
            Assert.assertEquals(0, PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.get());
            assertCoveredMatchesBase();
        });
    }

    @Test
    public void testMultiTxnDrainsAppendWithoutFastLagOrReseal() throws Exception {
        // Many transactions per drain would form a block, and a block would take the WAL fast append; a
        // merge-append table refuses that and applies the block as one merge-append instead.
        setProperty(PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT, 10_000_000);
        assertMemoryLeak(() -> {
            createTable("YEAR");
            bootstrapPartition();
            final int drains = 6;
            final int txnsPerDrain = 5;
            int row = 100;
            for (int d = 0; d < drains; d++) {
                for (int t = 0; t < txnsPerDrain; t++) {
                    insertAscending(row, 50);
                    row += 50;
                }
                drainWalQueue();
            }
            assertNotSuspended();
            Assert.assertEquals(0, PostingIndexWriter.COVERING_BLOCK_FASTPATH_COUNT.get());
            Assert.assertEquals(drains, PostingIndexWriter.COVERING_FASTLAG_COMMIT_COUNT.get());
            Assert.assertEquals(0, PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.get());
            assertCoveredMatchesBase();
        });
    }

    @Test
    public void testReplaceRangeDropsRowsWithoutReseal() throws Exception {
        assertMemoryLeak(() -> {
            createTable("YEAR");
            for (int c = 0; c < 3; c++) {
                insertAscending(c * 100, 100);
                drainWalQueue();
            }
            resetCoveringCounters();

            // Replace the middle stretch with a handful of rows: the pieces inside the range are dropped
            // or superseded, their rows left behind as dead space with their index entries.
            final TableToken token = engine.verifyTableName("t");
            try (WalWriter ww = engine.getWalWriter(token)) {
                ww.commitWithParams(
                        MicrosTimestampDriver.floor("2024-01-01T00:01:40Z"),
                        MicrosTimestampDriver.floor("2024-01-01T00:03:20Z"),
                        WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE
                );
                for (int i = 0; i < 10; i++) {
                    final TableWriter.Row r = ww.newRow(MicrosTimestampDriver.floor("2024-01-01T00:02:00Z") + i * 1_000_000L);
                    r.putSym(1, "S" + (i % SYMBOLS));
                    r.putDouble(2, 50_000 + i);
                    r.append();
                }
                ww.commit();
            }
            drainWalQueue();
            assertNotSuspended();
            Assert.assertEquals(0, PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.get());
            assertCoveredMatchesBase();
            assertQuery("SELECT count() c FROM t")
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            c
                            210
                            """);
        });
    }

    private static void resetCoveringCounters() {
        PostingIndexWriter.COVERING_FASTLAG_COMMIT_COUNT.set(0);
        PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.set(0);
        PostingIndexWriter.COVERING_AUTOSEAL_COUNT.set(0);
        PostingIndexWriter.COVERING_MAX_GENCOUNT_OBSERVED.set(0);
        PostingIndexWriter.COVERING_MAX_SEGCOUNT_OBSERVED.set(0);
        PostingIndexWriter.COVERING_BLOCK_FASTPATH_COUNT.set(0);
        PostingIndexWriter.COVERING_COW_MIGRATE_COUNT.set(0);
    }

    private void assertCoveredMatchesBase() throws Exception {
        for (int s = 0; s < SYMBOLS; s++) {
            final String sym = "S" + s;
            assertSqlCursors(
                    "SELECT /*+ no_covering */ ts, sym, value FROM t WHERE sym = '" + sym + "' ORDER BY ts, value",
                    "SELECT ts, sym, value FROM t WHERE sym = '" + sym + "' ORDER BY ts, value"
            );
            assertSqlCursors(
                    "SELECT /*+ no_covering */ count(), count(value), sum(value), min(value), max(value) FROM t WHERE sym = '" + sym + "'",
                    "SELECT count(), count(value), sum(value), min(value), max(value) FROM t WHERE sym = '" + sym + "'"
            );
        }
    }

    /**
     * The first commit into an empty table creates its partition, and a partition is created by the O3
     * path that builds a fresh directory, not merged into: that one commit builds the covering sidecar
     * from scratch, over the batch alone. Every commit after it is a merge-append. The counters are
     * reset once the partition exists so a test counts only those.
     */
    private void bootstrapPartition() throws Exception {
        insertAscending(0, 100);
        drainWalQueue();
        resetCoveringCounters();
    }

    private void assertNotSuspended() {
        Assert.assertFalse("table suspended", engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("t")));
    }

    private void createTable(String partitionBy) throws Exception {
        execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (value), value DOUBLE)"
                + " TIMESTAMP(ts) PARTITION BY " + partitionBy + " WAL");
    }

    /**
     * Inserts {@code rows} rows one second apart from {@code startTs}, with a unique value from
     * {@code valueBase} and every seventh value NULL.
     */
    private void insertAt(String startTs, long valueBase, int rows) throws Exception {
        execute("INSERT INTO t SELECT dateadd('s', x::INT, '" + startTs + "'::TIMESTAMP),"
                + " 'S' || ((" + valueBase + " + x) % " + SYMBOLS + "),"
                + " CASE WHEN ((" + valueBase + " + x) % 7) = 0 THEN NULL::DOUBLE ELSE (" + valueBase + " + x)::DOUBLE END"
                + " FROM long_sequence(" + rows + ")");
    }

    /**
     * Inserts {@code rows} ascending rows one second apart, starting {@code firstRow} seconds into
     * 2024-01-01, with value == the row's second, so the whole stream is globally ascending.
     */
    private void insertAscending(int firstRow, int rows) throws Exception {
        execute("INSERT INTO t SELECT dateadd('s', (" + firstRow + " + x)::INT, '2024-01-01T00:00:00Z'::TIMESTAMP),"
                + " 'S' || ((" + firstRow + " + x) % " + SYMBOLS + "),"
                + " CASE WHEN ((" + firstRow + " + x) % 7) = 0 THEN NULL::DOUBLE ELSE (" + firstRow + " + x)::DOUBLE END"
                + " FROM long_sequence(" + rows + ")");
    }
}
