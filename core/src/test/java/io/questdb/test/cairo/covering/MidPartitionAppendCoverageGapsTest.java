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

package io.questdb.test.cairo.covering;

import io.questdb.cairo.idx.PostingIndexWriter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * The paths a level-3 review found the mid-partition covered-append tests never
 * reach. Each was traced as correct by inspection; that is exactly why they are
 * worth a test, because nothing here was verified by execution before.
 * <p>
 * Every test compares the covering table against a control table carrying a
 * plain (non-covering) POSTING index over the identical stream, and additionally
 * compares the index scan against a {@code no_index} full scan of the covering
 * table itself. The first catches a wrong covered value, the second a missing
 * posting - {@code no_covering} catches neither, because it keeps the index scan
 * in place.
 */
public class MidPartitionAppendCoverageGapsTest extends AbstractCairoTest {

    private static final int COMMITS = 8;
    private static final int ROWS = 400;
    private static final int SEED = 3000;

    @Before
    public void enableCounters() {
        PostingIndexWriter.COVERING_COUNTERS_ENABLED = true;
        PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.set(0);
        PostingIndexWriter.COVERING_SEAL_APPEND_COUNT.set(0);
    }

    @After
    public void disableCounters() {
        PostingIndexWriter.COVERING_COUNTERS_ENABLED = false;
        PostingIndexWriter.COVERING_SEAL_APPEND_DISABLED = false;
    }

    /**
     * BYPASS WAL. Every other test on this path is WAL, and a non-WAL commit
     * reaches commit00 -> o3Commit through a different entry path with no WAL
     * re-apply behind it, so a seal failure surfaces as a throwing INSERT rather
     * than a suspended table.
     */
    @Test
    public void testBypassWalMidPartitionAppend() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (value), value DOUBLE)"
                    + " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("CREATE TABLE ctl (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING, value DOUBLE)"
                    + " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");

            // day 1 and day 3 exist, so day 2 is a MID partition.
            insertBoth("2024-01-01", 0, 200);
            insertBoth("2024-01-03", 200, 200);
            insertBoth("2024-01-02", 400, SEED);

            PostingIndexWriter.COVERING_SEAL_APPEND_COUNT.set(0);
            for (int c = 0; c < COMMITS; c++) {
                insertBoth("2024-01-02", 400L + SEED + (long) c * ROWS, ROWS);
            }

            assertIndexAgreesWithColumn();
            assertMatchesControl();
            Assert.assertTrue("the append path must fire on a non-WAL table (appends="
                            + PostingIndexWriter.COVERING_SEAL_APPEND_COUNT.get() + ')',
                    PostingIndexWriter.COVERING_SEAL_APPEND_COUNT.get() > 0);
        });
    }

    /**
     * O3 runs on real worker threads instead of inline on the writer thread.
     * {@code o3CoveringDeferredPartitions} is written by the O3 workers and read
     * by the single-threaded sweep after they join; with the pool never started,
     * that contract was asserted in a comment and exercised by nothing. Several
     * partitions per commit, so multiple workers record deferrals concurrently.
     */
    @Test
    public void testParallelO3WorkersRecordDeferralsSafely() throws Exception {
        final WorkerPool pool = new TestWorkerPool(4, node1.getMetrics());
        assertMemoryLeak(() -> {
            createWalTables();
            // seed four days, so a single commit can touch several mid partitions
            for (int d = 1; d <= 5; d++) {
                insertBoth("2024-01-0" + d, (long) d * 10_000, 800);
            }
            drainWalQueue();

            WorkerPoolUtils.setupWriterJobs(pool, engine);
            WorkerPoolUtils.setupAsyncMunmapJob(pool, engine);
            pool.start(LOG);
            try {
                PostingIndexWriter.COVERING_SEAL_APPEND_COUNT.set(0);
                for (int c = 0; c < COMMITS; c++) {
                    // one txn per day, drained together: the commit spans four
                    // mid partitions at once
                    for (int d = 1; d <= 4; d++) {
                        insertBoth("2024-01-0" + d, (long) d * 10_000 + 800 + (long) c * ROWS, ROWS);
                    }
                    drainWalQueue();
                }
            } finally {
                pool.halt();
            }
            drainWalQueue();

            assertNotSuspended();
            assertIndexAgreesWithColumn();
            assertMatchesControl();
            Assert.assertTrue("the append path must fire under parallel O3 (appends="
                            + PostingIndexWriter.COVERING_SEAL_APPEND_COUNT.get() + ')',
                    PostingIndexWriter.COVERING_SEAL_APPEND_COUNT.get() > 0);
        });
    }

    /**
     * The same parallel-O3 contract on the LAST partition, which is this PR's
     * route. DEDUP disqualifies the WAL fast-lag gate, so these pure appends
     * fall back to O3 and reach the deferral - on the partition every commit
     * touches, with four workers recording deferrals concurrently.
     */
    @Test
    public void testParallelO3LastPartitionDedup() throws Exception {
        final WorkerPool pool = new TestWorkerPool(4, node1.getMetrics());
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (value), value DOUBLE)"
                    + " TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, sym)");
            execute("CREATE TABLE ctl (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING, value DOUBLE)"
                    + " TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, sym)");
            insertBoth("2024-01-01", 0, SEED);
            drainWalQueue();

            WorkerPoolUtils.setupWriterJobs(pool, engine);
            WorkerPoolUtils.setupAsyncMunmapJob(pool, engine);
            pool.start(LOG);
            try {
                PostingIndexWriter.COVERING_SEAL_APPEND_COUNT.set(0);
                for (int c = 0; c < COMMITS; c++) {
                    insertBoth("2024-01-01", (long) SEED + (long) c * ROWS, ROWS);
                    drainWalQueue();
                }
            } finally {
                pool.halt();
            }
            drainWalQueue();

            assertNotSuspended();
            assertIndexAgreesWithColumn();
            assertMatchesControl();
            Assert.assertTrue("the append path must fire on the last partition under parallel O3"
                            + " (appends=" + PostingIndexWriter.COVERING_SEAL_APPEND_COUNT.get() + ')',
                    PostingIndexWriter.COVERING_SEAL_APPEND_COUNT.get() > 0);
        });
    }

    /**
     * A PARQUET partition in the table. The guard reasons that parquet lands in
     * the "O3 already indexed it" case and is declined through getMaxValue();
     * that reasoning had no test. Appends continue into a NATIVE mid partition
     * while an earlier partition is parquet.
     */
    @Test
    public void testAppendWithParquetPartitionPresent() throws Exception {
        assertMemoryLeak(() -> {
            createWalTables();
            insertBoth("2024-01-01", 0, 500);
            insertBoth("2024-01-02", 500, SEED);
            insertBoth("2024-01-03", 5000, 500);
            drainWalQueue();

            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            execute("ALTER TABLE ctl CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();

            PostingIndexWriter.COVERING_SEAL_APPEND_COUNT.set(0);
            for (int c = 0; c < COMMITS; c++) {
                insertBoth("2024-01-02", 500L + SEED + (long) c * ROWS, ROWS);
                drainWalQueue();
            }

            final long parquetCount = selectLong(
                    "SELECT count() FROM table_partitions('t') WHERE isParquet = true");
            Assert.assertEquals("test setup: the partition must actually be parquet", 1, parquetCount);
            Assert.assertTrue("appends into the NATIVE mid partition must still fire alongside a"
                            + " parquet partition (appends="
                            + PostingIndexWriter.COVERING_SEAL_APPEND_COUNT.get() + ')',
                    PostingIndexWriter.COVERING_SEAL_APPEND_COUNT.get() > 0);

            assertNotSuspended();
            assertIndexAgreesWithColumn();
            assertMatchesControl();
        });
    }

    /**
     * TWO indexed covering columns on one table. The deferral set is keyed per
     * PARTITION but consumed per COLUMN, so a partition where one column defers
     * and the other does not is the case where those two granularities disagree.
     * The second index is added later, which is what makes them disagree: on the
     * first commit after ADD INDEX, sym2's column top is not below the pre-append
     * size, so it cannot defer while sym can.
     */
    @Test
    public void testTwoIndexedCoveringColumnsWithStaggeredIndex() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (value),"
                    + " sym2 SYMBOL, value DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE ctl (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING,"
                    + " sym2 SYMBOL, value DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            insertBoth3("2024-01-01", 0, 200);
            insertBoth3("2024-01-03", 200, 200);
            insertBoth3("2024-01-02", 400, SEED);
            drainWalQueue();

            // sym2 becomes a second COVERING posting index only now
            execute("ALTER TABLE t ALTER COLUMN sym2 ADD INDEX TYPE POSTING INCLUDE (value)");
            drainWalQueue();

            PostingIndexWriter.COVERING_SEAL_APPEND_COUNT.set(0);
            for (int c = 0; c < COMMITS; c++) {
                insertBoth3("2024-01-02", 400L + SEED + (long) c * ROWS, ROWS);
                drainWalQueue();
            }

            assertNotSuspended();
            Assert.assertTrue("the append path must fire with two covering indexes (appends="
                            + PostingIndexWriter.COVERING_SEAL_APPEND_COUNT.get() + ')',
                    PostingIndexWriter.COVERING_SEAL_APPEND_COUNT.get() > 0);
            // Pin that BOTH columns are really served as COVERED reads. Without
            // this, a plan that stopped using either index would turn every
            // comparison below into two identical non-covering scans.
            assertQuery("SELECT ts, sym, value FROM t WHERE sym = 'S1' ORDER BY value")
                    .noLeakCheck()
                    .assertsPlanContaining("CoveringIndex on: sym with: ts, value");
            assertQuery("SELECT ts, sym2, value FROM t WHERE sym2 = 'K1' ORDER BY value")
                    .noLeakCheck()
                    .assertsPlanContaining("CoveringIndex on: sym2 with: ts, value");
            // both indexes must agree with a full scan
            for (int s = 0; s < 4; s++) {
                assertSqlCursors(
                        "SELECT /*+ no_index */ ts, sym, value FROM t WHERE sym = 'S" + s + "' ORDER BY ts",
                        "SELECT ts, sym, value FROM t WHERE sym = 'S" + s + "' ORDER BY ts"
                );
                assertSqlCursors(
                        "SELECT /*+ no_index */ ts, sym2, value FROM t WHERE sym2 = 'K" + s + "' ORDER BY ts",
                        "SELECT ts, sym2, value FROM t WHERE sym2 = 'K" + s + "' ORDER BY ts"
                );
            }
            assertSqlCursors(
                    "SELECT ts, sym, sym2, value FROM ctl ORDER BY ts, value",
                    "SELECT ts, sym, sym2, value FROM t ORDER BY ts, value"
            );
        });
    }

    private long selectLong(CharSequence sql) throws Exception {
        try (RecordCursorFactory factory = select(sql);
             RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            Assert.assertTrue("query must return one row [sql=" + sql + ']', cursor.hasNext());
            return cursor.getRecord().getLong(0);
        }
    }

    private void assertIndexAgreesWithColumn() throws Exception {
        // Filtered and per-symbol: an unfiltered keyed group-by compiles to the
        // same plan with and without no_index, so it cannot detect anything.
        for (int s = 0; s < 4; s++) {
            assertSqlCursors(
                    "SELECT /*+ no_index */ ts, sym, value FROM t WHERE sym = 'S" + s + "' ORDER BY ts",
                    "SELECT ts, sym, value FROM t WHERE sym = 'S" + s + "' ORDER BY ts"
            );
        }
    }

    private void assertMatchesControl() throws Exception {
        assertSqlCursors(
                "SELECT ts, sym, value FROM ctl ORDER BY ts, value",
                "SELECT ts, sym, value FROM t ORDER BY ts, value"
        );
        assertSqlCursors(
                "SELECT sym, count(*), sum(value), min(value), max(value) FROM ctl ORDER BY sym",
                "SELECT sym, count(*), sum(value), min(value), max(value) FROM t ORDER BY sym"
        );
    }

    private void assertNotSuspended() {
        Assert.assertFalse("t suspended", engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("t")));
        Assert.assertFalse("ctl suspended", engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("ctl")));
    }

    private void createWalTables() throws Exception {
        execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (value), value DOUBLE)"
                + " TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE TABLE ctl (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING, value DOUBLE)"
                + " TIMESTAMP(ts) PARTITION BY DAY WAL");
    }

    private void insertBoth(String day, long v0, int rows) throws Exception {
        insertBothAt(day, v0, v0, rows);
    }

    /**
     * {@code tsBaseMicros} is the timestamp base and is deliberately INDEPENDENT
     * of {@code v0}, which only feeds sym and value. Deriving the timestamp from
     * v0 pushes the rows past the partition max, turning an intended O3 write
     * into a plain append - which is how the split test first ran green while
     * producing no splits at all.
     */
    private void insertBothAt(String day, long tsBaseMicros, long v0, int rows) throws Exception {
        final String tail = " SELECT dateadd('u', (" + tsBaseMicros + " + x)::INT,"
                + " '" + day + "T00:00:00Z'::TIMESTAMP), 'S' || ((" + v0 + " + x) % 4),"
                + " (" + v0 + " + x)::DOUBLE FROM long_sequence(" + rows + ")";
        execute("INSERT INTO t" + tail);
        execute("INSERT INTO ctl" + tail);
    }

    private void insertBoth3(String day, long v0, int rows) throws Exception {
        final String tail = " SELECT dateadd('u', (" + v0 + " + x)::INT,"
                + " '" + day + "T00:00:00Z'::TIMESTAMP), 'S' || ((" + v0 + " + x) % 4),"
                + " 'K' || ((" + v0 + " + x) % 4), (" + v0 + " + x)::DOUBLE"
                + " FROM long_sequence(" + rows + ")";
        execute("INSERT INTO t" + tail);
        execute("INSERT INTO ctl" + tail);
    }
}
