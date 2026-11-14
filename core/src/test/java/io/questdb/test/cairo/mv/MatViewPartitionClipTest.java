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

package io.questdb.test.cairo.mv;

import io.questdb.PropertyKey;
import io.questdb.TelemetryJob;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.mv.MatViewStateReader;
import io.questdb.cairo.mv.MatViewTimerJob;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.SqlCompiler;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static io.questdb.cairo.wal.WalUtils.*;

/**
 * The partition clip: every non-FULL refresh except the view's first build recomputes only the
 * buckets that overlap an existing base partition, so its REPLACE_RANGE commits never delete view
 * rows in a range the base no longer holds.
 * <p>
 * The tests remove base partitions with TTL, which drops the oldest ones, with replace-range commits
 * written through the WalWriter API, which remove every partition they empty, and with base DROP
 * PARTITION, which leaves the views valid and can leave a gap between the base's partitions. TTL
 * counts from the base's newest timestamp capped at the clock, so the tests that set the clock keep
 * it at or after the base's newest row whenever TTL should drop a partition. The refresh step,
 * which cairo.mat.view.rows.per.query.estimate sizes, spans many partitions unless a test lowers it.
 */
@RunWith(Parameterized.class)
public class MatViewPartitionClipTest extends AbstractCairoTest {
    private static final Log LOG = LogFactory.getLog(MatViewPartitionClipTest.class);
    // The rows of the period view of createPeriodTimerView().
    private static final String PERIOD_VIEW_ROWS = """
            ts\tsym\tprice
            2024-01-01T00:00:00.000000Z\ta\t1.0
            2024-01-01T05:00:00.000000Z\tb\t2.0
            """;
    private static final String PERIOD_VIEW_SQL = "SELECT ts, sym, last(price) AS price FROM base SAMPLE BY 1h";
    // The rows of the period view of createPeriodTimerView() once it recomputed the base UPDATE of
    // createPeriodTimerViewWithUpdatedDay(), which multiplies the prices of 2024-01-01 by 10.
    private static final String UPDATED_PERIOD_VIEW_ROWS = """
            ts\tsym\tprice
            2024-01-01T00:00:00.000000Z\ta\t10.0
            2024-01-01T05:00:00.000000Z\tb\t20.0
            """;
    private final TestTimestampType timestampType;

    public MatViewPartitionClipTest(TestTimestampType timestampType) {
        this.timestampType = timestampType;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> params() {
        return Arrays.asList(new Object[][]{
                {TestTimestampType.MICRO}, {TestTimestampType.NANO}
        });
    }

    @Test
    public void testClipAcrossPartitionByUnits() throws Exception {
        // For every PARTITION BY unit, TTL drops the oldest of three partitions. A late commit then
        // writes a row into the dropped partition, which TTL drops again, and rows at both ends of
        // the next partition. The clip reads each unit's partition ceiling from the timestamp driver:
        // the view keeps its bucket in the dropped partition and follows the base everywhere else,
        // the last bucket of the partition after the gap included.
        assertMemoryLeak(() -> {
            assertUnitClip(
                    "HOUR", "1m",
                    "2024-01-01T00:10:00", "2024-01-01T01:10:00", "2024-01-01T02:10:00",
                    "2024-01-01T00:10:30", "2024-01-01T01:05:00", "2024-01-01T01:59:30",
                    "2024-01-01T00:10:00", "2024-01-01T01:00:00"
            );
            assertUnitClip(
                    "DAY", "1h",
                    "2024-01-01T00:10:00", "2024-01-02T00:10:00", "2024-01-03T00:10:00",
                    "2024-01-01T00:20:00", "2024-01-02T05:00:00", "2024-01-02T23:30:00",
                    "2024-01-01T00:00:00", "2024-01-02T00:00:00"
            );
            // 2024-01-01 is a Monday, so the weekly partitions start on 01-01, 01-08 and 01-15.
            assertUnitClip(
                    "WEEK", "1d",
                    "2024-01-01T06:00:00", "2024-01-08T06:00:00", "2024-01-15T06:00:00",
                    "2024-01-01T12:00:00", "2024-01-09T06:00:00", "2024-01-14T20:00:00",
                    "2024-01-01T00:00:00", "2024-01-08T00:00:00"
            );
            // 2024 is a leap year, so the February partition ends after the 29th.
            assertUnitClip(
                    "MONTH", "1d",
                    "2024-01-10T06:00:00", "2024-02-10T06:00:00", "2024-03-10T06:00:00",
                    "2024-01-10T12:00:00", "2024-02-05T06:00:00", "2024-02-29T12:00:00",
                    "2024-01-10T00:00:00", "2024-02-01T00:00:00"
            );
            assertUnitClip(
                    "YEAR", "1M",
                    "2022-06-10T06:00:00", "2023-06-10T06:00:00", "2024-06-10T06:00:00",
                    "2022-06-20T06:00:00", "2023-02-10T06:00:00", "2023-12-31T12:00:00",
                    "2022-06-01T00:00:00", "2023-01-01T00:00:00"
            );
        });
    }

    @Test
    public void testConsecutiveEmptyPeriodsKeepQueuedUpdateRange() throws Exception {
        // The base holds no partition of 2024-01-02 and 2024-01-03, as over a weekend. The period timer's refresh of
        // each day commits only the period it reached, with one view state event, and keeps the range that a base
        // UPDATE queued before them, which the incremental refresh after them recomputes.
        assertMemoryLeak(() -> {
            createPeriodTimerViewWithUpdatedDay();
            for (String periodHi : new String[]{"2024-01-03T00:00:00", "2024-01-04T00:00:00"}) {
                final long viewTxns = walTxnCount("price_1h");
                runPeriodRefresh(periodHi);
                Assert.assertEquals(viewTxns + 1, walTxnCount("price_1h"));
                assertRefreshPosition(1, periodHi);
                assertCache(2, ts("2024-01-01T00:00:00"), ts("2024-01-02T00:00:00") - 1);
            }
            assertViewRows(PERIOD_VIEW_ROWS);

            runIncrementalRefresh("2024-01-04T00:30:00");
            assertViewRows(UPDATED_PERIOD_VIEW_ROWS);
            assertCache(-1);
            assertViewCaughtUp("price_1h");
            assertViewValid();
        });
    }

    @Test
    public void testEmptyBaseAdvancesPosition() throws Exception {
        // A replace-range commit empties the whole base, which leaves it without partitions. The
        // refresh has nothing left to recompute: the view keeps its rows and its refresh position
        // still moves to the base's last transaction.
        assertMemoryLeak(() -> {
            createBase("DAY", null);
            createView("1h", "DAY");
            execute("""
                    INSERT INTO base VALUES
                    ('a', 1.0, '2024-01-01T00:10:00.000000Z'),
                    ('b', 2.0, '2024-01-02T00:10:00.000000Z')
                    """);
            drainWalAndMatViewQueues();

            try (WalWriter walWriter = engine.getWalWriter(engine.verifyTableName("base"))) {
                walWriter.commitWithParams(ts("2024-01-01T00:00:00"), ts("2024-01-03T00:00:00"), WAL_DEDUP_MODE_REPLACE_RANGE);
            }
            drainWalAndMatViewQueues();

            assertQuery("SELECT count() FROM table_partitions('base')")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n0\n");
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-02T00:00:00.000000Z\tb\t2.0
                    """);
            assertViewCaughtUp("price_1h");
            assertViewValid();
        });
    }

    @Test
    public void testFirstBuildOverGapsWritesNoGapCommits() throws Exception {
        // The first build skips the clip and recomputes the whole [minTs, maxTs] in one range, so a
        // sparse base's gaps add no view WAL transaction over a dense base, and the view's rows match
        // the view's SQL run on the base.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE dense (sym SYMBOL, price DOUBLE, ts " + tsType() + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO dense VALUES
                    ('a', 1.0, '2024-01-01T00:10:00.000000Z'),
                    ('a', 2.0, '2024-01-02T00:10:00.000000Z'),
                    ('a', 3.0, '2024-01-03T00:10:00.000000Z'),
                    ('a', 4.0, '2024-01-04T00:10:00.000000Z'),
                    ('a', 5.0, '2024-01-05T00:10:00.000000Z')
                    """);
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW dense_v AS (SELECT ts, sym, last(price) AS price FROM dense SAMPLE BY 1h) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            final long denseBuildTxns = walTxnCount("dense_v");

            // Two gaps: days 02 and 04 hold no partition.
            execute("CREATE TABLE gappy (sym SYMBOL, price DOUBLE, ts " + tsType() + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO gappy VALUES
                    ('a', 1.0, '2024-01-01T00:10:00.000000Z'),
                    ('a', 3.0, '2024-01-03T00:10:00.000000Z'),
                    ('a', 5.0, '2024-01-05T00:10:00.000000Z')
                    """);
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW gappy_v AS (SELECT ts, sym, last(price) AS price FROM gappy SAMPLE BY 1h) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            final long gappyBuildTxns = walTxnCount("gappy_v");

            Assert.assertEquals(denseBuildTxns, gappyBuildTxns);
            assertSqlEquals(
                    "gappy_v ORDER BY ts, sym",
                    "(SELECT ts, sym, last(price) AS price FROM gappy SAMPLE BY 1h) ORDER BY ts, sym"
            );
        });
    }

    @Test
    public void testIncrementalRefreshAcrossGapsWritesOneCommitPerGap() throws Exception {
        // Performance property: a refresh whose working interval crosses N partition gaps writes
        // exactly N more REPLACE_RANGE commits than the same refresh over a base without gaps, for an
        // incremental refresh and for a user RANGE alike.
        assertMemoryLeak(() -> {
            assertCommitsPerGap(1);
            assertCommitsPerGap(3);
        });
    }

    @Test
    public void testIncrementalRefreshOfIntervalsQueuedBeforeInteriorDropKeepsRows() throws Exception {
        // A step of one partition's buckets at most.
        setProperty(PropertyKey.CAIRO_MAT_VIEW_ROWS_PER_QUERY_ESTIMATE, 1);
        assertMemoryLeak(this::assertIncrementalRefreshOfIntervalsQueuedBeforeInteriorDropKeepsRows);
    }

    @Test
    public void testIncrementalRefreshOfIntervalsQueuedBeforeInteriorDropKeepsRowsWithLargeStep() throws Exception {
        // A step that spans many partitions: a step-group that ran past its interval's end would span the gap.
        setProperty(PropertyKey.CAIRO_MAT_VIEW_ROWS_PER_QUERY_ESTIMATE, 1_000_000);
        assertMemoryLeak(this::assertIncrementalRefreshOfIntervalsQueuedBeforeInteriorDropKeepsRows);
    }

    @Test
    public void testIncrementalRefreshOfIntervalsQueuedBeforeTtlDropKeepsRows() throws Exception {
        // A MANUAL view queues a late row into 2024-01-01; TTL drops that day before the view
        // refreshes. The incremental refresh of the queued interval keeps the view's row there.
        assertMemoryLeak(() -> {
            createBase("DAY", "2 DAYS");
            execute("CREATE MATERIALIZED VIEW price_1h REFRESH MANUAL AS (SELECT ts, sym, last(price) AS price FROM base SAMPLE BY 1h) PARTITION BY DAY");
            execute("""
                    INSERT INTO base VALUES
                    ('a', 1.0, '2024-01-01T00:10:00.000000Z'),
                    ('b', 2.0, '2024-01-02T00:10:00.000000Z')
                    """);
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();

            // A late row, queued while the base still holds 2024-01-01.
            execute("INSERT INTO base VALUES ('a', 11.0, '2024-01-01T00:20:00.000000Z')");
            drainWalQueue();
            // New days move TTL past 2024-01-01, which drops it with both its rows.
            execute("""
                    INSERT INTO base VALUES
                    ('c', 3.0, '2024-01-03T00:10:00.000000Z'),
                    ('d', 4.0, '2024-01-04T00:10:00.000000Z')
                    """);
            drainWalQueue();
            assertBasePartitionsStart("2024-01-02");

            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();
            assertViewRows(allFourDays());
            assertViewCaughtUp("price_1h");
        });
    }

    @Test
    public void testLateRowsRecreatingDroppedPartitionChangeOnlyTheirBuckets() throws Exception {
        // Known limitation: DROP PARTITION gets no cut. A MANUAL view queues a change to the 00:00 bucket of
        // 2024-01-02, the base drops that partition, and a late row recreates it in the 05:00 bucket. The refresh
        // recomputes the buckets of the queued change and of the late row from the rows the base holds now: the
        // 00:00 bucket goes, the 05:00 bucket gets the late row, and the 10:00 bucket keeps its row.
        assertMemoryLeak(() -> {
            createBase("DAY", null);
            createManualView();
            execute("""
                    INSERT INTO base VALUES
                    ('a', 1.0, '2024-01-01T00:10:00.000000Z'),
                    ('b', 2.0, '2024-01-02T00:10:00.000000Z'),
                    ('x', 7.0, '2024-01-02T10:10:00.000000Z'),
                    ('c', 3.0, '2024-01-03T00:10:00.000000Z')
                    """);
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();

            execute("INSERT INTO base VALUES ('b', 22.0, '2024-01-02T00:20:00.000000Z')");
            drainWalQueue();
            execute("ALTER TABLE base DROP PARTITION LIST '2024-01-02'");
            drainWalQueue();
            execute("INSERT INTO base VALUES ('y', 8.0, '2024-01-02T05:10:00.000000Z')");
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();

            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-02T05:00:00.000000Z\ty\t8.0
                    2024-01-02T10:00:00.000000Z\tx\t7.0
                    2024-01-03T00:00:00.000000Z\tc\t3.0
                    """);
            assertViewCaughtUp("price_1h");
            assertViewValid();
        });
    }

    @Test
    public void testLateRowIntoTtlDroppedPeriodKeepsRows() throws Exception {
        // The base TTL drops its oldest day; a late row into that day is applied and dropped again
        // by the same commit, but its WAL interval still reaches the refresh. Without the clip the
        // refresh would recompute the dropped day's bucket as empty and delete the view's row there.
        assertMemoryLeak(() -> {
            createTtlBaseViewWithOldestDropped();

            // The late row also changes 2024-01-03, so the refresh has work to do next to the gap.
            execute("""
                    INSERT INTO base VALUES
                    ('e', 5.0, '2024-01-01T00:20:00.000000Z'),
                    ('c', 33.0, '2024-01-03T00:20:00.000000Z')
                    """);
            drainWalAndMatViewQueues();
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-02T00:00:00.000000Z\tb\t2.0
                    2024-01-03T00:00:00.000000Z\tc\t33.0
                    2024-01-04T00:00:00.000000Z\td\t4.0
                    """);
            assertViewValid();
        });
    }

    @Test
    public void testMinMaxRecomputeAfterMissingWalFilesKeepsRows() throws Exception {
        // When the refresh cannot read the base's WAL transactions, it recomputes the base's whole
        // [minTs, maxTs]. TTL has dropped 2024-01-01 and a replace-range commit has emptied, and so
        // removed, 2024-01-03 in between. The recompute keeps the view's rows in both ranges and
        // follows the base in the others.
        setProperty(PropertyKey.DEBUG_MAT_VIEW_REFRESH_MISSING_WAL_FILES_FATAL, "false");
        assertMemoryLeak(() -> {
            createBase("DAY", "3 DAYS");
            execute("CREATE MATERIALIZED VIEW price_1h REFRESH MANUAL AS (SELECT ts, sym, last(price) AS price FROM base SAMPLE BY 1h) PARTITION BY DAY");
            execute("""
                    INSERT INTO base VALUES
                    ('a', 1.0, '2024-01-01T00:10:00.000000Z'),
                    ('b', 2.0, '2024-01-02T00:10:00.000000Z'),
                    ('c', 3.0, '2024-01-03T00:10:00.000000Z'),
                    ('d', 4.0, '2024-01-04T00:10:00.000000Z')
                    """);
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();

            final TableToken baseToken = engine.verifyTableName("base");
            try (WalWriter walWriter = engine.getWalWriter(baseToken)) {
                walWriter.commitWithParams(ts("2024-01-03T00:00:00"), ts("2024-01-04T00:00:00"), WAL_DEDUP_MODE_REPLACE_RANGE);
            }
            // The new day moves TTL past 2024-01-01 and changes 2024-01-02.
            execute("""
                    INSERT INTO base VALUES
                    ('b', 22.0, '2024-01-02T00:20:00.000000Z'),
                    ('e', 5.0, '2024-01-05T00:10:00.000000Z')
                    """);
            drainWalQueue();
            assertQuery("SELECT name FROM table_partitions('base') ORDER BY name")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            name
                            2024-01-02
                            2024-01-04
                            2024-01-05
                            """);

            removeWalEventFiles(baseToken);
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-02T00:00:00.000000Z\tb\t22.0
                    2024-01-03T00:00:00.000000Z\tc\t3.0
                    2024-01-04T00:00:00.000000Z\td\t4.0
                    2024-01-05T00:00:00.000000Z\te\t5.0
                    """);
            assertViewCaughtUp("price_1h");
        });
    }

    @Test
    public void testMinMaxRecomputeAfterMissingWalFilesKeepsInteriorDroppedRows() throws Exception {
        setProperty(PropertyKey.DEBUG_MAT_VIEW_REFRESH_MISSING_WAL_FILES_FATAL, "false");
        setProperty(PropertyKey.CAIRO_MAT_VIEW_ROWS_PER_QUERY_ESTIMATE, 1);
        assertMemoryLeak(this::assertMinMaxRecomputeAfterMissingWalFilesKeepsInteriorDroppedRows);
    }

    @Test
    public void testMinMaxRecomputeAfterMissingWalFilesKeepsInteriorDroppedRowsWithLargeStep() throws Exception {
        setProperty(PropertyKey.DEBUG_MAT_VIEW_REFRESH_MISSING_WAL_FILES_FATAL, "false");
        setProperty(PropertyKey.CAIRO_MAT_VIEW_ROWS_PER_QUERY_ESTIMATE, 1_000_000);
        assertMemoryLeak(this::assertMinMaxRecomputeAfterMissingWalFilesKeepsInteriorDroppedRows);
    }

    @Test
    public void testParentViewEmptiedPartitionKeepsChildRows() throws Exception {
        // A DEDUP UPSERT overwrite makes the parent view's filter reject the only base row of
        // 2024-01-02. The parent's refresh empties, and so removes, its 2024-01-02 partition. The
        // child cannot tell that from a removed range, so it keeps its row there; a child DROP
        // PARTITION clears it.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, ts " + tsType() + ") TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, sym)");
            execute("CREATE MATERIALIZED VIEW parent AS (SELECT ts, sym, last(price) AS price FROM base WHERE price > 0 SAMPLE BY 1h) PARTITION BY DAY");
            execute("CREATE MATERIALIZED VIEW child AS (SELECT ts, sym, last(price) AS price FROM parent SAMPLE BY 1d) PARTITION BY DAY");
            execute("""
                    INSERT INTO base VALUES
                    ('a', 1.0, '2024-01-01T00:10:00.000000Z'),
                    ('a', 2.0, '2024-01-02T00:10:00.000000Z'),
                    ('a', 3.0, '2024-01-03T00:10:00.000000Z')
                    """);
            drainParentAndChild();
            assertQuery("child ORDER BY ts").timestamp("ts").noLeakCheck().expectSize().returns(replaceTs("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-02T00:00:00.000000Z\ta\t2.0
                    2024-01-03T00:00:00.000000Z\ta\t3.0
                    """));

            execute("INSERT INTO base VALUES ('a', -2.0, '2024-01-02T00:10:00.000000Z')");
            drainParentAndChild();
            assertQuery("SELECT name FROM table_partitions('parent') ORDER BY name")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            name
                            2024-01-01
                            2024-01-03
                            """);
            assertQuery("child ORDER BY ts").timestamp("ts").noLeakCheck().expectSize().returns(replaceTs("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-02T00:00:00.000000Z\ta\t2.0
                    2024-01-03T00:00:00.000000Z\ta\t3.0
                    """));
            assertViewCaughtUp("child");

            execute("ALTER MATERIALIZED VIEW child DROP PARTITION LIST '2024-01-02'");
            drainParentAndChild();
            assertQuery("child ORDER BY ts").timestamp("ts").noLeakCheck().expectSize().returns(replaceTs("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-03T00:00:00.000000Z\ta\t3.0
                    """));
            assertQuery("SELECT view_name, view_status FROM materialized_views() ORDER BY view_name")
                    .noLeakCheck()
                    .returns("""
                            view_name\tview_status
                            child\tvalid
                            parent\tvalid
                            """);
        });
    }

    @Test
    public void testParquetPartitionCountsAsExisting() throws Exception {
        // A base partition converted to Parquet stays in the reader's partition list, so the clip
        // treats it as existing: an incremental refresh whose interval crosses it recomputes it from
        // its Parquet rows, next to a TTL-dropped partition the clip skips.
        assertMemoryLeak(() -> {
            createTtlBaseViewWithOldestDropped();
            execute("ALTER TABLE base CONVERT PARTITION TO PARQUET LIST '2024-01-02'");
            drainWalAndMatViewQueues();
            assertQuery("SELECT name, isParquet FROM table_partitions('base') ORDER BY name")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            name\tisParquet
                            2024-01-02\ttrue
                            2024-01-03\tfalse
                            2024-01-04\tfalse
                            """);

            // One commit across all four days: the dropped 2024-01-01 stays as it was, the Parquet
            // 2024-01-02 bucket gains the late row's price.
            execute("""
                    INSERT INTO base VALUES
                    ('c', 30.0, '2024-01-03T00:20:00.000000Z'),
                    ('e', 5.0, '2024-01-01T00:20:00.000000Z'),
                    ('b', 20.0, '2024-01-02T00:20:00.000000Z')
                    """);
            drainWalAndMatViewQueues();
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-02T00:00:00.000000Z\tb\t20.0
                    2024-01-03T00:00:00.000000Z\tc\t30.0
                    2024-01-04T00:00:00.000000Z\td\t4.0
                    """);
            assertViewValid();
        });
    }

    @Test
    public void testPeriodRefreshOverEmptyPeriodKeepsCachedIntervalInsideIt() throws Exception {
        // A late row into 2024-01-02 is cached, and the base then drops 2024-01-02. The period timer's refresh of the
        // day keeps the cached interval inside it, next to the range of a base UPDATE of 2024-01-01. The incremental
        // refresh after it clips the interval of the dropped day away, recomputes the UPDATE's range and clears the
        // cache.
        assertMemoryLeak(() -> {
            createPeriodTimerView();
            currentMicros = parseFloorPartialTimestamp("2024-01-02T23:00:00.000000Z");
            execute("INSERT INTO base VALUES ('d', 4.0, '2024-01-02T10:10:00.000000Z')");
            drainWalQueue();
            engine.getMatViewStateStore().enqueueUpdateRefreshIntervals(engine.verifyTableName("price_1h"));
            drainWalAndMatViewQueues();
            assertCache(2, ts("2024-01-02T10:10:00"), ts("2024-01-02T10:10:00"));
            execute("ALTER TABLE base DROP PARTITION LIST '2024-01-02'");
            drainWalQueue();
            assertBasePartitions("2024-01-01\n");
            currentMicros = parseFloorPartialTimestamp("2024-01-02T23:45:00.000000Z");
            execute("UPDATE base SET price = price * 10 WHERE ts IN '2024-01-01'");
            drainWalAndMatViewQueues();
            // the range task brings the cache up to the UPDATE, base txn 4, then adds the UPDATE's range
            final long[] cached = {
                    ts("2024-01-01T00:00:00"), ts("2024-01-02T00:00:00") - 1,
                    ts("2024-01-02T10:10:00"), ts("2024-01-02T10:10:00")
            };
            assertCache(4, cached);

            runPeriodRefresh("2024-01-03T00:00:00");
            assertRefreshPosition(1, "2024-01-03T00:00:00");
            assertCache(4, cached);
            assertViewRows(PERIOD_VIEW_ROWS);

            runIncrementalRefresh("2024-01-03T00:30:00");
            assertViewRows(UPDATED_PERIOD_VIEW_ROWS);
            assertCache(-1);
            assertViewCaughtUp("price_1h");
            assertViewValid();
        });
    }

    @Test
    public void testPeriodRefreshOverEmptyPeriodKeepsFinishTimeForRefreshIntervalsUpdate() throws Exception {
        // The period timer's refresh of 2024-01-02, a day the base holds no partition of, finishes at 2024-01-03T00:00
        // in memory as in the view's state file. The refresh intervals update after a base INSERT at 00:10 writes a
        // view state event with the finish time it finds in memory, so materialized_views() goes on to report the
        // period refresh's finish time, and the view as valid rather than as refreshing since 2024-01-03T00:00.
        assertMemoryLeak(() -> {
            createPeriodTimerViewWithUpdatedDay();
            runPeriodRefresh("2024-01-03T00:00:00");
            assertRefreshPosition(1, "2024-01-03T00:00:00");

            currentMicros = parseFloorPartialTimestamp("2024-01-03T00:10:00.000000Z");
            execute("INSERT INTO base VALUES ('c', 3.0, '2024-01-03T00:10:00.000000Z')");
            drainWalQueue();
            engine.getMatViewStateStore().enqueueUpdateRefreshIntervals(engine.verifyTableName("price_1h"));
            drainWalAndMatViewQueues();
            // the update scanned the INSERT, base txn 3, and wrote its view state event
            assertCache(
                    3,
                    ts("2024-01-01T00:00:00"), ts("2024-01-02T00:00:00") - 1,
                    ts("2024-01-03T00:10:00"), ts("2024-01-03T00:10:00")
            );
            Assert.assertEquals(parseFloorPartialTimestamp("2024-01-03T00:00:00.000000Z"), getViewState().getLastRefreshFinishTimestampUs());
            assertQuery("SELECT last_refresh_start_timestamp, last_refresh_finish_timestamp, view_status FROM materialized_views() WHERE view_name = 'price_1h'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            last_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_status
                            2024-01-03T00:00:00.000000Z\t2024-01-03T00:00:00.000000Z\tvalid
                            """);
        });
    }

    @Test
    public void testPeriodRefreshOverEmptyPeriodKeepsQueuedUpdateRange() throws Exception {
        // A TIMER period view has refreshed its periods up to 2024-01-02, and a base UPDATE of 2024-01-01 after the
        // view's last EVERY firing queues the day's range in the view's cache. The period timer's refresh of
        // 2024-01-02, a day the base holds no partition of, recomputes nothing: it commits only the period it
        // reached, in memory and on disk, and leaves the cached range to the next incremental refresh, which
        // recomputes it.
        assertMemoryLeak(() -> {
            createPeriodTimerViewWithUpdatedDay();
            final long viewTxns = walTxnCount("price_1h");
            runPeriodRefresh("2024-01-03T00:00:00");
            Assert.assertEquals(viewTxns + 1, walTxnCount("price_1h"));
            assertRefreshPosition(1, "2024-01-03T00:00:00");
            assertCache(2, ts("2024-01-01T00:00:00"), ts("2024-01-02T00:00:00") - 1);
            assertViewRows(PERIOD_VIEW_ROWS);
            assertViewValid();

            runIncrementalRefresh("2024-01-03T00:30:00");
            assertViewRows(UPDATED_PERIOD_VIEW_ROWS);
            assertSqlEquals("price_1h ORDER BY ts, sym", "(" + PERIOD_VIEW_SQL + ") ORDER BY ts, sym");
            assertCache(-1);
            assertViewCaughtUp("price_1h");
            assertViewValid();
        });
    }

    @Test
    public void testPeriodRefreshOverEmptyPeriodRecordsRangeRefreshTelemetry() throws Exception {
        // The period timer's refresh of 2024-01-02, a day the base holds no partition of, enqueued at the end of the
        // period and run five seconds later, records a refresh success event with the base txn of a range refresh,
        // -1, and the five seconds from the timer's firing to the commit of the period as its latency.
        assertMemoryLeak(() -> {
            try (TelemetryJob telemetryJob = new TelemetryJob(engine)) {
                createPeriodTimerView();
                telemetryJob.runSerially();

                currentMicros = parseFloorPartialTimestamp("2024-01-03T00:00:00.000000Z");
                engine.getMatViewStateStore().enqueueRangeRefresh(engine.verifyTableName("price_1h"), Numbers.LONG_NULL, ts("2024-01-03T00:00:00") - 1);
                currentMicros = parseFloorPartialTimestamp("2024-01-03T00:00:05.000000Z");
                drainWalAndMatViewQueues();
                assertRefreshPosition(1, "2024-01-03T00:00:00");
                // the telemetry job stamps the events it writes with its clock
                telemetryJob.runSerially();

                // 204 is the refresh success event
                assertQuery("SELECT created, event, base_table_txn, invalidation_reason, latency FROM sys.telemetry_mat_view WHERE view_table_id = "
                        + engine.verifyTableName("price_1h").getTableId() + " AND created >= '2024-01-03'")
                        .noLeakCheck()
                        .timestamp("created")
                        .returns("""
                                created\tevent\tbase_table_txn\tinvalidation_reason\tlatency
                                2024-01-03T00:00:05.000000Z\t204\t-1\t\t5000.0
                                """);
            }
        });
    }

    @Test
    public void testPeriodRefreshOverEmptyPeriodWithEmptyCacheCommitsOnlyPeriod() throws Exception {
        // The period timer's refresh of 2024-01-02, a day the base holds no partition of, with nothing cached: one
        // view state event that moves the period, and keeps the refresh position, the empty cache and the rows.
        assertMemoryLeak(() -> {
            createPeriodTimerView();
            final long viewTxns = walTxnCount("price_1h");
            runPeriodRefresh("2024-01-03T00:00:00");
            Assert.assertEquals(viewTxns + 1, walTxnCount("price_1h"));
            assertRefreshPosition(1, "2024-01-03T00:00:00");
            assertCache(-1);
            assertViewRows(PERIOD_VIEW_ROWS);
            assertViewCaughtUp("price_1h");
            assertViewValid();
        });
    }

    @Test
    public void testPeriodViewBehindBaseTtlKeepsRows() throws Exception {
        // A period view's last refreshed period falls behind the base's TTL: the view refreshed its
        // periods up to 2024-01-01, a user RANGE wrote 2024-01-01 and 2024-01-02 ahead of them, and
        // TTL then dropped 2024-01-01. The refresh that catches up the completed periods from
        // 2024-01-01 keeps the view's row there, and so do the period timer's refresh and an
        // incremental refresh of a late row into the dropped day.
        assertMemoryLeak(() -> {
            createBase("DAY", "2 DAYS");
            currentMicros = parseFloorPartialTimestamp("2024-01-01T00:00:00.000000Z");
            execute("INSERT INTO base VALUES ('z', 0.5, '2023-12-31T00:10:00.000000Z')");
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW price_1h REFRESH IMMEDIATE PERIOD (LENGTH 1d) AS (SELECT ts, sym, last(price) AS price FROM base SAMPLE BY 1h) PARTITION BY DAY");
            final MatViewTimerJob timerJob = new MatViewTimerJob(engine);
            drainMatViewTimerQueue(timerJob);
            drainWalAndMatViewQueues();
            assertViewRows("""
                    ts\tsym\tprice
                    2023-12-31T00:00:00.000000Z\tz\t0.5
                    """);

            execute("""
                    INSERT INTO base VALUES
                    ('a', 1.0, '2024-01-01T00:10:00.000000Z'),
                    ('b', 2.0, '2024-01-02T00:10:00.000000Z')
                    """);
            drainWalAndMatViewQueues();
            execute("REFRESH MATERIALIZED VIEW price_1h RANGE FROM '2024-01-01T00:00:00.000000Z' TO '2024-01-03T00:00:00.000000Z'");
            drainWalAndMatViewQueues();
            assertViewRows("""
                    ts\tsym\tprice
                    2023-12-31T00:00:00.000000Z\tz\t0.5
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-02T00:00:00.000000Z\tb\t2.0
                    """);

            // The periods up to 2024-01-03 complete. The new days move TTL past 2023-12-31 and 2024-01-01,
            // and the commit that drops them triggers the refresh that catches up from 2024-01-01.
            currentMicros = parseFloorPartialTimestamp("2024-01-04T00:10:00.000000Z");
            execute("""
                    INSERT INTO base VALUES
                    ('c', 3.0, '2024-01-03T00:10:00.000000Z'),
                    ('d', 4.0, '2024-01-04T00:10:00.000000Z')
                    """);
            drainWalAndMatViewQueues();
            assertBasePartitionsStart("2024-01-02");
            drainMatViewTimerQueue(timerJob);
            drainWalAndMatViewQueues();
            final String expected = """
                    ts\tsym\tprice
                    2023-12-31T00:00:00.000000Z\tz\t0.5
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-02T00:00:00.000000Z\tb\t2.0
                    2024-01-03T00:00:00.000000Z\tc\t3.0
                    """;
            assertViewRows(expected);

            // A late row into the dropped day, which TTL drops again.
            execute("INSERT INTO base VALUES ('a', 11.0, '2024-01-01T00:20:00.000000Z')");
            drainWalAndMatViewQueues();
            assertViewRows(expected);
            assertViewCaughtUp("price_1h");
            assertViewValid();
        });
    }

    @Test
    public void testRefreshIntoDroppedGapAdvancesPositionWithoutRows() throws Exception {
        // A refresh whose whole working interval falls in a TTL-dropped gap recomputes nothing: it
        // leaves the view's rows untouched but still advances the refresh position, so the next
        // refresh does not reprocess the dropped range.
        assertMemoryLeak(() -> {
            createTtlBaseViewWithOldestDropped();

            execute("INSERT INTO base VALUES ('e', 5.0, '2024-01-01T00:20:00.000000Z')");
            drainWalAndMatViewQueues();
            assertViewRows(allFourDays());
            assertViewCaughtUp("price_1h");
            assertViewValid();
        });
    }

    @Test
    public void testRefreshLimitMarginKeepsOldestStraddlingBucket() throws Exception {
        // SAMPLE BY 1d WITH OFFSET '18:00' over PARTITION BY DAY: each bucket straddles two day
        // partitions. TTL 3 DAYS drops 2024-01-01, which leaves the bucket [01-01T18:00, 01-02T18:00)
        // with only its 2024-01-02 rows in the base. A late row lands in that bucket.
        assertMemoryLeak(() -> {
            // A REFRESH LIMIT one bucket shorter than the TTL keeps incremental refreshes out of the
            // bucket, so it keeps the first price of its dropped 2024-01-01 row.
            assertStraddlingBucket("limit_2d", "2 DAYS", "118.0");
            // A REFRESH LIMIT shorter than the TTL by less than one bucket has its boundary inside the
            // bucket. The refresh recomputes the whole bucket from the rows the base still holds.
            assertStraddlingBucket("limit_60h", "60 HOURS", "200.0");
        });
    }

    @Test
    public void testReplaceCommitInOldestPartitionIsFollowed() throws Exception {
        // Inside the base's partitions the view follows the base. A replace-range commit in the
        // oldest existing partition, next to the TTL-dropped 2024-01-01, replaces b with e there, and
        // the view drops b and shows e.
        assertMemoryLeak(() -> {
            createTtlBaseViewWithOldestDropped();

            try (WalWriter walWriter = engine.getWalWriter(engine.verifyTableName("base"))) {
                final TableWriter.Row row = walWriter.newRow(ts("2024-01-02T00:20:00"));
                row.putSym(0, "e");
                row.putDouble(1, 22.0);
                row.append();
                walWriter.commitWithParams(ts("2024-01-02T00:00:00"), ts("2024-01-02T01:00:00"), WAL_DEDUP_MODE_REPLACE_RANGE);
            }
            drainWalAndMatViewQueues();
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-02T00:00:00.000000Z\te\t22.0
                    2024-01-03T00:00:00.000000Z\tc\t3.0
                    2024-01-04T00:00:00.000000Z\td\t4.0
                    """);
            assertViewValid();
        });
    }

    @Test
    public void testSplitPartitionCountsAsOnePartition() throws Exception {
        // An out-of-order write into the middle of the newest partition splits it into two parts that
        // share one logical partition. The clip treats them as one partition range: refreshes into and
        // across the split follow the base, also across the never-populated 2024-01-02 next to it.
        // Merge-append folds a backdated write into the partition instead of splitting it, so the test
        // turns it off to get the split.
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 4 << 10);
        setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 2);
        assertMemoryLeak(() -> {
            createBase("DAY", null);
            createView("1h", "DAY");
            execute("INSERT INTO base VALUES ('a', 1.0, '2024-01-01T00:10:00.000000Z')");
            execute("INSERT INTO base SELECT 'b', x, timestamp_sequence('2024-01-03', 60_000_000L) FROM long_sequence(1_320)");
            drainWalAndMatViewQueues();

            // The out-of-order write splits 2024-01-03 at 20:01.
            execute("INSERT INTO base SELECT 'c', x, timestamp_sequence('2024-01-03T20:01', 1_000_000L) FROM long_sequence(200)");
            drainWalAndMatViewQueues();
            assertQuery("SELECT count() FROM table_partitions('base') WHERE name LIKE '2024-01-03%'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n2\n");
            assertSqlEquals(
                    "price_1h ORDER BY ts, sym",
                    "(SELECT ts, sym, last(price) AS price FROM base SAMPLE BY 1h) ORDER BY ts, sym"
            );

            // One commit across the gap, into both parts of the split.
            execute("""
                    INSERT INTO base VALUES
                    ('a', 11.0, '2024-01-01T00:20:00.000000Z'),
                    ('b', 22.0, '2024-01-03T05:30:00.000000Z'),
                    ('c', 33.0, '2024-01-03T23:30:00.000000Z')
                    """);
            drainWalAndMatViewQueues();
            assertSqlEquals(
                    "price_1h ORDER BY ts, sym",
                    "(SELECT ts, sym, last(price) AS price FROM base SAMPLE BY 1h) ORDER BY ts, sym"
            );
            assertViewCaughtUp("price_1h");
            assertViewValid();
        });
    }

    @Test
    public void testUserRangeAcrossDroppedInteriorPartitionKeepsRows() throws Exception {
        setProperty(PropertyKey.CAIRO_MAT_VIEW_ROWS_PER_QUERY_ESTIMATE, 1);
        assertMemoryLeak(this::assertUserRangeAcrossDroppedInteriorPartitionKeepsRows);
    }

    @Test
    public void testUserRangeAcrossDroppedInteriorPartitionKeepsRowsWithLargeStep() throws Exception {
        setProperty(PropertyKey.CAIRO_MAT_VIEW_ROWS_PER_QUERY_ESTIMATE, 1_000_000);
        assertMemoryLeak(this::assertUserRangeAcrossDroppedInteriorPartitionKeepsRows);
    }

    @Test
    public void testUserRangeAcrossTtlDroppedRangeKeepsRows() throws Exception {
        // A user RANGE recomputes only where the base still holds rows. TTL has dropped 2024-01-01,
        // and a replace-range commit has emptied, and so removed, 2024-01-03, which lies between the
        // base's minimum and maximum timestamps. The view's own 2024-01-04 partition is dropped first,
        // so that the RANGE has work to do: it restores 2024-01-04 and keeps 2024-01-01 and 2024-01-03.
        assertMemoryLeak(() -> {
            // A fixed clock past the base's newest row keeps the view's refresh timestamps, and so its
            // status, deterministic.
            currentMicros = parseFloorPartialTimestamp("2024-01-05T00:00:00.000000Z");
            createTtlBaseViewWithOldestDropped();
            try (WalWriter walWriter = engine.getWalWriter(engine.verifyTableName("base"))) {
                walWriter.commitWithParams(ts("2024-01-03T00:00:00"), ts("2024-01-04T00:00:00"), WAL_DEDUP_MODE_REPLACE_RANGE);
            }
            drainWalAndMatViewQueues();
            execute("ALTER MATERIALIZED VIEW price_1h DROP PARTITION LIST '2024-01-04'");
            drainWalAndMatViewQueues();
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-02T00:00:00.000000Z\tb\t2.0
                    2024-01-03T00:00:00.000000Z\tc\t3.0
                    """);

            execute("REFRESH MATERIALIZED VIEW price_1h RANGE FROM '2023-12-31T00:00:00.000000Z' TO '2024-01-05T00:00:00.000000Z'");
            drainWalAndMatViewQueues();
            assertViewRows(allFourDays());
            assertViewValid();

            // FULL skips the clip: it rebuilds the view from the rows the base holds and drops the
            // history the base no longer has.
            execute("REFRESH MATERIALIZED VIEW price_1h FULL");
            drainWalAndMatViewQueues();
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-02T00:00:00.000000Z\tb\t2.0
                    2024-01-04T00:00:00.000000Z\td\t4.0
                    """);
            assertViewValid();
        });
    }

    @Test
    public void testWeekBucketStraddlingTtlDroppedDayIsRecomputed() throws Exception {
        // SAMPLE BY 1w over PARTITION BY DAY: a weekly bucket straddles seven day partitions. When
        // TTL drops the oldest days of the week, the clip starts the interval at the first existing
        // day, but the iterator rounds down to the week floor, so the straddling bucket is recomputed
        // from the days the base still holds, matching the view's SQL run on the base.
        assertMemoryLeak(() -> {
            // 2024-01-01 is a Monday, so 01..07 is one ISO week.
            execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, ts " + tsType() + ") TIMESTAMP(ts) PARTITION BY DAY TTL 1 WEEK WAL");
            execute("CREATE MATERIALIZED VIEW price_1w AS (SELECT ts, sym, first(price) AS price FROM base SAMPLE BY 1w) PARTITION BY WEEK");
            execute("""
                    INSERT INTO base VALUES
                    ('a', 1.0, '2024-01-01T06:00:00.000000Z'),
                    ('a', 2.0, '2024-01-02T06:00:00.000000Z'),
                    ('a', 3.0, '2024-01-03T06:00:00.000000Z'),
                    ('a', 4.0, '2024-01-04T06:00:00.000000Z'),
                    ('a', 5.0, '2024-01-05T06:00:00.000000Z'),
                    ('a', 6.0, '2024-01-06T06:00:00.000000Z'),
                    ('a', 7.0, '2024-01-07T06:00:00.000000Z')
                    """);
            drainWalAndMatViewQueues();
            // TTL 1 WEEK, counted from 2024-01-11, drops 2024-01-01..03 and keeps 04..07.
            execute("INSERT INTO base VALUES ('a', 9.0, '2024-01-11T06:00:00.000000Z')");
            drainWalAndMatViewQueues();
            assertBasePartitionsStart("2024-01-04");
            assertQuery("price_1w ORDER BY ts").timestamp("ts").noLeakCheck().expectSize().returns(replaceTs("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-08T00:00:00.000000Z\ta\t9.0
                    """));

            // A late row into the straddling week recomputes the week from 04..07.
            execute("INSERT INTO base VALUES ('a', 77.0, '2024-01-07T12:00:00.000000Z')");
            drainWalAndMatViewQueues();
            assertQuery("price_1w ORDER BY ts").timestamp("ts").noLeakCheck().expectSize().returns(replaceTs("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t4.0
                    2024-01-08T00:00:00.000000Z\ta\t9.0
                    """));
            assertSqlEquals(
                    "price_1w ORDER BY ts, sym",
                    "(SELECT ts, sym, first(price) AS price FROM base SAMPLE BY 1w) ORDER BY ts, sym"
            );
        });
    }

    private String allFourDays() {
        return """
                ts\tsym\tprice
                2024-01-01T00:00:00.000000Z\ta\t1.0
                2024-01-02T00:00:00.000000Z\tb\t2.0
                2024-01-03T00:00:00.000000Z\tc\t3.0
                2024-01-04T00:00:00.000000Z\td\t4.0
                """;
    }

    private void assertBasePartitions(String partitions) throws Exception {
        assertQuery("SELECT name FROM table_partitions('base') ORDER BY name")
                .noLeakCheck()
                .expectSize()
                .returns("name\n" + partitions);
    }

    private void assertBasePartitionsStart(String firstPartition) throws Exception {
        assertQuery("SELECT min(name) first FROM table_partitions('base')")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns("first\n" + firstPartition + "\n");
    }

    // Writes, into a dense base (days 1..2N+1) and into a base that lacks every second day (N gaps),
    // the same out-of-order rows across all their days, and runs the same user RANGE over both. Each
    // refresh of the base with gaps writes exactly N more view WAL transactions.
    private void assertCommitsPerGap(int gapCount) throws Exception {
        final int dayCount = 2 * gapCount + 1;
        final String dense = "dense" + gapCount;
        final String gappy = "gappy" + gapCount;
        for (String base : new String[]{dense, gappy}) {
            execute("CREATE TABLE " + base + " (sym SYMBOL, price DOUBLE, ts " + tsType() + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE MATERIALIZED VIEW " + base + "_v AS (SELECT ts, sym, last(price) AS price FROM " + base + " SAMPLE BY 1h) PARTITION BY DAY");
        }
        final StringBuilder denseRows = new StringBuilder();
        final StringBuilder gappyRows = new StringBuilder();
        final StringBuilder denseO3Rows = new StringBuilder();
        final StringBuilder gappyO3Rows = new StringBuilder();
        for (int day = dayCount; day > 0; day--) {
            final String date = "2024-01-0" + day;
            appendRow(denseRows, day, date + "T00:10:00.000000Z");
            appendRow(denseO3Rows, 10 * day, date + "T00:20:00.000000Z");
            if (day % 2 == 1) {
                appendRow(gappyRows, day, date + "T00:10:00.000000Z");
                appendRow(gappyO3Rows, 10 * day, date + "T00:20:00.000000Z");
            }
        }
        execute("INSERT INTO " + dense + " VALUES " + denseRows);
        execute("INSERT INTO " + gappy + " VALUES " + gappyRows);
        drainWalAndMatViewQueues();

        long denseTxns = walTxnCount(dense + "_v");
        long gappyTxns = walTxnCount(gappy + "_v");
        // One out-of-order commit across every day of each base.
        execute("INSERT INTO " + dense + " VALUES " + denseO3Rows);
        execute("INSERT INTO " + gappy + " VALUES " + gappyO3Rows);
        drainWalAndMatViewQueues();
        Assert.assertEquals(
                "incremental refresh across " + gapCount + " gaps",
                walTxnCount(dense + "_v") - denseTxns + gapCount,
                walTxnCount(gappy + "_v") - gappyTxns
        );

        denseTxns = walTxnCount(dense + "_v");
        gappyTxns = walTxnCount(gappy + "_v");
        for (String base : new String[]{dense, gappy}) {
            execute("REFRESH MATERIALIZED VIEW " + base + "_v RANGE FROM '2024-01-01T00:00:00.000000Z' TO '2024-01-0" + (dayCount + 1) + "T00:00:00.000000Z'");
        }
        drainWalAndMatViewQueues();
        Assert.assertEquals(
                "user RANGE across " + gapCount + " gaps",
                walTxnCount(dense + "_v") - denseTxns + gapCount,
                walTxnCount(gappy + "_v") - gappyTxns
        );

        for (String base : new String[]{dense, gappy}) {
            assertSqlEquals(
                    base + "_v ORDER BY ts, sym",
                    "(SELECT ts, sym, last(price) AS price FROM " + base + " SAMPLE BY 1h) ORDER BY ts, sym"
            );
        }
    }

    // A MANUAL view queues changes to 2024-01-02 and 2024-01-03, and the base then drops 2024-01-02, which lies
    // between partitions it keeps. The incremental refresh of the queued intervals keeps the view's row of
    // 2024-01-02, with its value from before the queued change, and follows the change to 2024-01-03.
    private void assertIncrementalRefreshOfIntervalsQueuedBeforeInteriorDropKeepsRows() throws Exception {
        createBase("DAY", null);
        createManualView();
        insertFourDays();
        drainWalQueue();
        execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
        drainWalAndMatViewQueues();

        execute("INSERT INTO base VALUES ('b', 22.0, '2024-01-02T00:20:00.000000Z'), ('c', 33.0, '2024-01-03T00:20:00.000000Z')");
        drainWalQueue();
        execute("ALTER TABLE base DROP PARTITION LIST '2024-01-02'");
        drainWalQueue();
        assertBasePartitions("2024-01-01\n2024-01-03\n2024-01-04\n");

        execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
        drainWalAndMatViewQueues();
        assertViewRows("""
                ts\tsym\tprice
                2024-01-01T00:00:00.000000Z\ta\t1.0
                2024-01-02T00:00:00.000000Z\tb\t2.0
                2024-01-03T00:00:00.000000Z\tc\t33.0
                2024-01-04T00:00:00.000000Z\td\t4.0
                """);
        assertViewCaughtUp("price_1h");
        assertViewValid();
    }

    // The base drops its interior partition 2024-01-02, then the refresh cannot read the base's WAL transactions
    // and recomputes the base's whole [minTs, maxTs]. The recompute keeps the view's row of 2024-01-02 and follows
    // the base in the other partitions.
    private void assertMinMaxRecomputeAfterMissingWalFilesKeepsInteriorDroppedRows() throws Exception {
        createBase("DAY", null);
        createManualView();
        insertFourDays();
        drainWalQueue();
        execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
        drainWalAndMatViewQueues();

        execute("ALTER TABLE base DROP PARTITION LIST '2024-01-02'");
        execute("INSERT INTO base VALUES ('c', 33.0, '2024-01-03T00:20:00.000000Z'), ('e', 5.0, '2024-01-05T00:10:00.000000Z')");
        drainWalQueue();
        assertBasePartitions("2024-01-01\n2024-01-03\n2024-01-04\n2024-01-05\n");

        removeWalEventFiles(engine.verifyTableName("base"));
        execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
        drainWalAndMatViewQueues();
        assertViewRows("""
                ts\tsym\tprice
                2024-01-01T00:00:00.000000Z\ta\t1.0
                2024-01-02T00:00:00.000000Z\tb\t2.0
                2024-01-03T00:00:00.000000Z\tc\t33.0
                2024-01-04T00:00:00.000000Z\td\t4.0
                2024-01-05T00:00:00.000000Z\te\t5.0
                """);
        assertViewCaughtUp("price_1h");
        assertViewValid();
    }

    // Asserts the cached refresh intervals of price_1h, in base table units, and their base txn, in memory and in the
    // view's state file.
    private void assertCache(long baseTxn, long... bounds) {
        final LongList expected = new LongList();
        for (long bound : bounds) {
            expected.add(bound);
        }
        final MatViewState state = getViewState();
        Assert.assertEquals(baseTxn, state.getRefreshIntervalsBaseTxn());
        TestUtils.assertEquals(expected, state.getRefreshIntervals());
        final MatViewStateReader persisted = readPersistedState();
        Assert.assertEquals(baseTxn, persisted.getRefreshIntervalsBaseTxn());
        TestUtils.assertEquals(expected, persisted.getRefreshIntervals());
    }

    // Asserts the refresh position of price_1h and the end of its last refreshed period, in memory and in the view's
    // state file.
    private void assertRefreshPosition(long baseTxn, String periodHi) throws Exception {
        final MatViewState state = getViewState();
        Assert.assertEquals(baseTxn, state.getLastRefreshBaseTxn());
        Assert.assertEquals(ts(periodHi), state.getLastPeriodHi());
        final MatViewStateReader persisted = readPersistedState();
        Assert.assertEquals(baseTxn, persisted.getLastRefreshBaseTxn());
        Assert.assertEquals(ts(periodHi), persisted.getLastPeriodHi());
    }

    private void assertSqlEquals(String actual, String expected) throws Exception {
        TestUtils.assertSqlCursors(engine, sqlExecutionContext, expected, actual, LOG);
    }

    // Builds a view whose daily buckets start at 18:00 over a DAY base with TTL 3 DAYS, sets the given
    // REFRESH LIMIT, moves TTL past 2024-01-01 and writes a late row at 2024-01-02T16:00, inside the
    // bucket [2024-01-01T18:00, 2024-01-02T18:00) that straddles the dropped day. The view's other
    // buckets follow the base either way.
    private void assertStraddlingBucket(String name, String refreshLimit, String expectedFirstPrice) throws Exception {
        final String base = name + "_base";
        execute("CREATE TABLE " + base + " (price DOUBLE, ts " + tsType() + ") TIMESTAMP(ts) PARTITION BY DAY TTL 3 DAYS WAL");
        execute("CREATE MATERIALIZED VIEW " + name + " AS (SELECT ts, first(price) AS first, count() AS c FROM " + base +
                " SAMPLE BY 1d ALIGN TO CALENDAR WITH OFFSET '18:00') PARTITION BY DAY");
        // The price encodes the row's day and hour, so the first price of a bucket shows its first row.
        execute("INSERT INTO " + base + " VALUES " + sixHourlyRows(1, 3));
        drainWalAndMatViewQueues();
        execute("ALTER MATERIALIZED VIEW " + name + " SET REFRESH LIMIT " + refreshLimit);
        drainWalAndMatViewQueues();

        // The newest row, 2024-01-05T03:00, moves TTL past 2024-01-01 and anchors the REFRESH LIMIT.
        currentMicros = parseFloorPartialTimestamp("2024-01-05T03:00:00.000000Z");
        execute("INSERT INTO " + base + " VALUES " + sixHourlyRows(4, 4) + ", (500.0, '2024-01-05T00:00:00.000000Z'), (503.0, '2024-01-05T03:00:00.000000Z')");
        drainWalAndMatViewQueues();
        assertQuery("SELECT min(name) first FROM table_partitions('" + base + "')")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns("first\n2024-01-02\n");

        execute("INSERT INTO " + base + " VALUES (216.0, '2024-01-02T16:00:00.000000Z')");
        drainWalAndMatViewQueues();
        assertQuery(name + " ORDER BY ts").timestamp("ts").noLeakCheck().expectSize().returns(replaceTs(
                "ts\tfirst\tc\n" +
                        "2023-12-31T18:00:00.000000Z\t100.0\t3\n" +
                        "2024-01-01T18:00:00.000000Z\t" + expectedFirstPrice + "\t4\n" +
                        "2024-01-02T18:00:00.000000Z\t218.0\t4\n" +
                        "2024-01-03T18:00:00.000000Z\t318.0\t4\n" +
                        "2024-01-04T18:00:00.000000Z\t418.0\t3\n"
        ));
    }

    // Builds a base partitioned by the given unit with TTL 1 of that unit and a view sampled below the
    // unit, writes a row into each of the partitions p0, p1 and p2, so that TTL drops p0, then writes
    // one commit with a late row into p0's bucket and rows at the start and at the end of p1. The view
    // keeps its p0 bucket at p0Bucket and matches the view's SQL from p1's start on.
    private void assertUnitClip(
            String unit,
            String sampleBy,
            String p0Ts,
            String p1Ts,
            String p2Ts,
            String lateP0Ts,
            String p1StartTs,
            String p1EndTs,
            String p0Bucket,
            String p1Start
    ) throws Exception {
        final String base = "b_" + unit;
        final String view = "v_" + unit;
        final String viewSql = "SELECT ts, sym, last(price) AS price FROM " + base + " SAMPLE BY " + sampleBy;
        execute("CREATE TABLE " + base + " (sym SYMBOL, price DOUBLE, ts " + tsType() + ") TIMESTAMP(ts) PARTITION BY " + unit + " TTL 1 " + unit + " WAL");
        execute("CREATE MATERIALIZED VIEW " + view + " AS (" + viewSql + ") PARTITION BY " + unit);
        execute("INSERT INTO " + base + " VALUES ('a', 1.0, '" + p0Ts + "'), ('a', 2.0, '" + p1Ts + "')");
        drainWalAndMatViewQueues();
        execute("INSERT INTO " + base + " VALUES ('a', 3.0, '" + p2Ts + "')");
        drainWalAndMatViewQueues();
        assertQuery("SELECT count() FROM table_partitions('" + base + "')")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns("count\n2\n");

        execute("INSERT INTO " + base + " VALUES ('a', 10.0, '" + lateP0Ts + "'), ('a', 20.0, '" + p1StartTs + "'), ('a', 30.0, '" + p1EndTs + "')");
        drainWalAndMatViewQueues();
        assertQuery("SELECT ts, sym, price FROM " + view + " WHERE ts < '" + p1Start + "'")
                .timestamp("ts")
                .noLeakCheck()
                .returns(replaceTs("ts\tsym\tprice\n" + p0Bucket + ".000000Z\ta\t1.0\n"));
        assertSqlEquals(
                "SELECT * FROM " + view + " WHERE ts >= '" + p1Start + "' ORDER BY ts, sym",
                "(" + viewSql + ") ORDER BY ts, sym"
        );
        assertQuery("SELECT view_status FROM materialized_views() WHERE view_name = '" + view + "'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("view_status\nvalid\n");
    }

    // The base drops its interior partition 2024-01-02, and the view keeps its row there. The view's own
    // 2024-01-04 partition is dropped, so that a user RANGE across the gap has work to do: it restores 2024-01-04
    // and keeps 2024-01-02.
    private void assertUserRangeAcrossDroppedInteriorPartitionKeepsRows() throws Exception {
        // A fixed clock past the base's newest row keeps the view's refresh timestamps, and so its status,
        // deterministic.
        currentMicros = parseFloorPartialTimestamp("2024-01-05T00:00:00.000000Z");
        createBase("DAY", null);
        createView("1h", "DAY");
        insertFourDays();
        drainWalAndMatViewQueues();
        execute("ALTER TABLE base DROP PARTITION LIST '2024-01-02'");
        drainWalAndMatViewQueues();
        assertBasePartitions("2024-01-01\n2024-01-03\n2024-01-04\n");
        assertViewRows(allFourDays());
        assertViewValid();

        execute("ALTER MATERIALIZED VIEW price_1h DROP PARTITION LIST '2024-01-04'");
        drainWalAndMatViewQueues();
        execute("REFRESH MATERIALIZED VIEW price_1h RANGE FROM '2023-12-31T00:00:00.000000Z' TO '2024-01-05T00:00:00.000000Z'");
        drainWalAndMatViewQueues();
        assertViewRows(allFourDays());
        assertViewCaughtUp("price_1h");
        assertViewValid();
    }

    private void assertViewCaughtUp(String viewName) throws Exception {
        assertQuery("SELECT refresh_base_table_txn = base_table_txn AS caught_up FROM materialized_views() WHERE view_name = '" + viewName + "'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("caught_up\ntrue\n");
    }

    private void assertViewRows(String expected) throws Exception {
        assertQuery("price_1h ORDER BY ts, sym").timestamp("ts").noLeakCheck().expectSize().returns(replaceTs(expected));
    }

    private void assertViewValid() throws Exception {
        assertQuery("SELECT view_status, invalidation_reason FROM materialized_views() WHERE view_name = 'price_1h'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("view_status\tinvalidation_reason\nvalid\t\n");
    }

    private static void appendRow(StringBuilder rows, int price, String ts) {
        if (!rows.isEmpty()) {
            rows.append(", ");
        }
        rows.append("('a', ").append(price).append(".0, '").append(ts).append("')");
    }

    private void createBase(String partitionBy, String ttlClause) throws Exception {
        final String ttl = ttlClause != null ? " TTL " + ttlClause : "";
        execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, ts " + tsType() + ") TIMESTAMP(ts) PARTITION BY " + partitionBy + ttl + " WAL");
    }

    // A child view refreshes after its parent's refresh commit applies, so each drain round reaches one
    // level deeper.
    private static void drainParentAndChild() {
        drainWalAndMatViewQueues();
        drainWalAndMatViewQueues();
    }

    // Builds price_1h over a DAY base with TTL 2 DAYS so the view holds four days (2024-01-01..04)
    // while the base has dropped its oldest day (2024-01-01). The two-phase insert builds the view
    // while 2024-01-01 is still present, then advances the base so TTL drops it.
    private void createManualView() throws Exception {
        execute("CREATE MATERIALIZED VIEW price_1h REFRESH MANUAL AS (SELECT ts, sym, last(price) AS price FROM base SAMPLE BY 1h) PARTITION BY DAY");
    }

    private void createTtlBaseViewWithOldestDropped() throws Exception {
        createBase("DAY", "2 DAYS");
        createView("1h", "DAY");
        execute("""
                INSERT INTO base VALUES
                ('a', 1.0, '2024-01-01T00:10:00.000000Z'),
                ('b', 2.0, '2024-01-02T00:10:00.000000Z')
                """);
        drainWalAndMatViewQueues();
        execute("""
                INSERT INTO base VALUES
                ('c', 3.0, '2024-01-03T00:10:00.000000Z'),
                ('d', 4.0, '2024-01-04T00:10:00.000000Z')
                """);
        drainWalAndMatViewQueues();
        assertBasePartitionsStart("2024-01-02");
        assertViewRows(allFourDays());
    }

    // A TIMER period view over the base's rows of 2024-01-01, built at 2024-01-02T12:00 as an EVERY firing would: it
    // holds the periods up to 2024-01-02, at base txn 1. The base holds no partition of 2024-01-02.
    private void createPeriodTimerView() throws Exception {
        currentMicros = parseFloorPartialTimestamp("2024-01-01T00:00:00.000000Z");
        createBase("DAY", null);
        execute("CREATE MATERIALIZED VIEW price_1h REFRESH EVERY 1h PERIOD (LENGTH 1d) AS (" + PERIOD_VIEW_SQL + ") PARTITION BY DAY");
        execute("INSERT INTO base VALUES ('a', 1.0, '2024-01-01T00:10:00.000000Z'), ('b', 2.0, '2024-01-01T05:10:00.000000Z')");
        drainWalQueue();
        runIncrementalRefresh("2024-01-02T12:00:00");
        assertViewRows(PERIOD_VIEW_ROWS);
        assertRefreshPosition(1, "2024-01-02T00:00:00");
        assertCache(-1);
    }

    // createPeriodTimerView(), then a base UPDATE of 2024-01-01 after the view's last EVERY firing of 2024-01-02. The
    // range task queues the day's range in the view's cache, at the UPDATE's base txn 2.
    private void createPeriodTimerViewWithUpdatedDay() throws Exception {
        createPeriodTimerView();
        currentMicros = parseFloorPartialTimestamp("2024-01-02T23:45:00.000000Z");
        execute("UPDATE base SET price = price * 10 WHERE ts IN '2024-01-01'");
        drainWalAndMatViewQueues();
        assertCache(2, ts("2024-01-01T00:00:00"), ts("2024-01-02T00:00:00") - 1);
        assertViewRows(PERIOD_VIEW_ROWS);
    }

    private void createView(String sampleBy, String viewPartitionBy) throws Exception {
        execute("CREATE MATERIALIZED VIEW price_1h AS (SELECT ts, sym, last(price) AS price FROM base SAMPLE BY " + sampleBy + ") PARTITION BY " + viewPartitionBy);
    }

    // Removes the event files of the base's WAL segments, so that the next refresh cannot read the
    // transactions it has not refreshed yet and recomputes the base's whole [minTs, maxTs] instead.
    private void removeWalEventFiles(TableToken baseToken) {
        engine.releaseInactive();
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        try (Path path = new Path()) {
            for (int walId = 1; walId < 4; walId++) {
                for (int segmentId = 0; segmentId < 4; segmentId++) {
                    path.of(engine.getConfiguration().getDbRoot()).concat(baseToken)
                            .concat(WAL_NAME_BASE).put(walId).slash().put(segmentId).concat(EVENT_FILE_NAME);
                    ff.removeQuiet(path.$());
                }
            }
        }
    }

    private void insertFourDays() throws Exception {
        execute("""
                INSERT INTO base VALUES
                ('a', 1.0, '2024-01-01T00:10:00.000000Z'),
                ('b', 2.0, '2024-01-02T00:10:00.000000Z'),
                ('c', 3.0, '2024-01-03T00:10:00.000000Z'),
                ('d', 4.0, '2024-01-04T00:10:00.000000Z')
                """);
    }

    private static MatViewState getViewState() {
        final MatViewState state = engine.getMatViewStateStore().getViewState(engine.verifyTableName("price_1h"));
        Assert.assertNotNull(state);
        return state;
    }

    private static MatViewStateReader readPersistedState() {
        final TableToken viewToken = engine.verifyTableName("price_1h");
        try (Path path = new Path(); BlockFileReader reader = new BlockFileReader(configuration)) {
            path.of(configuration.getDbRoot()).concat(viewToken).concat(MatViewState.MAT_VIEW_STATE_FILE_NAME);
            reader.of(path.$());
            return new MatViewStateReader().of(reader, viewToken);
        }
    }

    private String replaceTs(String expected) {
        return ColumnType.isTimestampMicro(timestampType.getTimestampType()) ? expected : expected.replace(".000000Z", ".000000000Z");
    }

    // Enqueues the incremental refresh of price_1h that its EVERY timer enqueues, at the given time, and runs it.
    private static void runIncrementalRefresh(String clock) {
        currentMicros = parseFloorPartialTimestamp(clock + ".000000Z");
        engine.getMatViewStateStore().enqueueIncrementalRefresh(engine.verifyTableName("price_1h"));
        drainWalAndMatViewQueues();
    }

    // Enqueues the range refresh of price_1h that the firing of MatViewTimerJob's period timer at the end of the
    // period enqueues, at that time, and runs it.
    private void runPeriodRefresh(String periodHi) throws Exception {
        currentMicros = parseFloorPartialTimestamp(periodHi + ".000000Z");
        engine.getMatViewStateStore().enqueueRangeRefresh(engine.verifyTableName("price_1h"), Numbers.LONG_NULL, ts(periodHi) - 1);
        drainWalAndMatViewQueues();
    }

    // Rows every six hours, at 00:00, 06:00, 12:00 and 18:00 of each day from 2024-01-0<firstDay> to
    // 2024-01-0<lastDay>, priced day * 100 + hour.
    private static String sixHourlyRows(int firstDay, int lastDay) {
        final StringBuilder rows = new StringBuilder();
        for (int day = firstDay; day <= lastDay; day++) {
            for (int hour = 0; hour < 24; hour += 6) {
                if (!rows.isEmpty()) {
                    rows.append(", ");
                }
                rows.append('(').append(day * 100 + hour).append(".0, '2024-01-0").append(day).append('T')
                        .append(hour < 10 ? "0" : "").append(hour).append(":00:00.000000Z')");
            }
        }
        return rows.toString();
    }

    private long ts(String isoSeconds) throws Exception {
        final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType.getTimestampType());
        return driver.parseFloorLiteral(isoSeconds + ".000000Z");
    }

    private String tsType() {
        return ColumnType.isTimestampMicro(timestampType.getTimestampType()) ? "TIMESTAMP" : "TIMESTAMP_NS";
    }

    private long walTxnCount(String viewName) throws Exception {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            try (
                    RecordCursorFactory factory = compiler.compile("SELECT count() FROM wal_transactions('" + viewName + "')", sqlExecutionContext).getRecordCursorFactory();
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                Assert.assertTrue(cursor.hasNext());
                return cursor.getRecord().getLong(0);
            }
        }
    }
}
