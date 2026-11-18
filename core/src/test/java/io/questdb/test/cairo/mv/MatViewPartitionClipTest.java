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
import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.mv.MatViewStateReader;
import io.questdb.cairo.mv.MatViewTimerJob;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
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

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

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
    // The rows of the view of createDstGapView() over the rows of insertDstGapRows() and a row x=100 at
    // 2023-03-26T13:00Z: the row before the transition counts in the bucket before the gap, the other two in the
    // bucket inside it.
    private static final String DST_GAP_VIEW_ROWS = """
            ts\tsym\tc\ts
            2023-03-25T01:30:00.000000Z\ta\t1\t1
            2023-03-26T01:30:00.000000Z\ta\t2\t102
            """;
    // Europe/Berlin moves its clocks forward at 2023-03-26T01:00:00Z, local 02:00 CET (+1h) to 03:00 CEST (+2h), so
    // under this SQL the bucket of 2023-03-26 starts at local 02:30, inside the gap. Its rows start at the transition,
    // and its key is 01:30Z. The rows before the transition, local 01:30-02:00 CET, belong to the bucket of
    // 2023-03-25, keyed 2023-03-25T01:30Z.
    private static final String DST_GAP_VIEW_SQL = dstGapViewSql("base", "Europe/Berlin", "1d", "02:30");
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
    public void testBucketsStraddlingGapEndsCostNoGapCommits() throws Exception {
        // A DAY base holds rows at 06:00 on weekdays and never on weekends. The Monday bucket of a daily view in
        // Europe/Berlin starts on Sunday at 23:00Z, or 22:00Z in summer time, and that of a daily view WITH OFFSET
        // '09:00' on Sunday at 09:00, both inside the weekend gap, and both hold Monday's row. A split at the gap
        // recomputes the Monday bucket as well, so the gap keeps no commit of its own: a user RANGE over four weeks,
        // across the switch to summer time on 2024-03-31, writes one view WAL transaction, as a refresh without the
        // split does, where a split at each of the three weekends would write four.
        assertMemoryLeak(() -> {
            createBase("DAY", null);
            final String berlinSql = "SELECT ts, sym, last(price) AS price FROM base SAMPLE BY 1d ALIGN TO CALENDAR TIME ZONE 'Europe/Berlin'";
            final String offsetSql = "SELECT ts, sym, last(price) AS price FROM base SAMPLE BY 1d ALIGN TO CALENDAR WITH OFFSET '09:00'";
            execute("CREATE MATERIALIZED VIEW berlin AS (" + berlinSql + ") PARTITION BY DAY");
            execute("CREATE MATERIALIZED VIEW offset_9h AS (" + offsetSql + ") PARTITION BY DAY");
            // Monday 2024-03-18 to Friday 2024-04-12
            final StringBuilder rows = new StringBuilder();
            LocalDate date = LocalDate.parse("2024-03-18");
            for (int day = 0; day < 26; day++, date = date.plusDays(1)) {
                if (date.getDayOfWeek() != DayOfWeek.SATURDAY && date.getDayOfWeek() != DayOfWeek.SUNDAY) {
                    appendRow(rows, day, date + "T06:00:00.000000Z");
                }
            }
            execute("INSERT INTO base VALUES " + rows);
            drainWalAndMatViewQueues();
            assertQuery("SELECT count() FROM table_partitions('base')")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n20\n");

            final String[] views = {"berlin", "offset_9h"};
            final String[] sqls = {berlinSql, offsetSql};
            for (int i = 0; i < views.length; i++) {
                final long viewTxns = walTxnCount(views[i]);
                execute("REFRESH MATERIALIZED VIEW " + views[i] + " RANGE FROM '2024-03-18T00:00:00.000000Z' TO '2024-04-13T00:00:00.000000Z'");
                drainWalAndMatViewQueues();
                Assert.assertEquals(views[i], viewTxns + 1, walTxnCount(views[i]));
                assertSqlEquals(views[i] + " ORDER BY ts, sym", "(" + sqls[i] + ") ORDER BY ts, sym");
            }
            assertQuery("SELECT ts, price FROM berlin WHERE ts BETWEEN '2024-03-28' AND '2024-04-02' ORDER BY ts")
                    .timestamp("ts")
                    .noLeakCheck()
                    .returns(replaceTs("""
                            ts\tprice
                            2024-03-28T23:00:00.000000Z\t11.0
                            2024-03-31T22:00:00.000000Z\t14.0
                            2024-04-01T22:00:00.000000Z\t15.0
                            """));
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
    public void testGapCheckReadsCompositeViewPartition() throws Exception {
        // Merge-append leaves the view's month partition composite once refreshes replace ranges inside it: the
        // replaced rows of 2024-01-05 stay in the column files as dead rows, and the new ones, as well as the row of
        // 2024-01-07, which the base writes later, land after the rows of 2024-01-09. The base then drops
        // 2024-01-07. The gap check finds the view's row of 2024-01-07 through the partition's pieces, so a user RANGE
        // keeps a commit for that gap and the row, and joins the days around 2024-01-03, which never held rows.
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        assertMemoryLeak(() -> {
            // A fixed clock past the base's newest row keeps the view's refresh timestamps, and so its status,
            // deterministic.
            currentMicros = parseFloorPartialTimestamp("2024-01-10T00:00:00.000000Z");
            createBase("DAY", null);
            createView("1h", "MONTH");
            final StringBuilder rows = new StringBuilder();
            for (int day = 1; day < 10; day++) {
                if (day != 3 && day != 7) {
                    appendRow(rows, day, "2024-01-0" + day + "T00:10:00.000000Z");
                }
            }
            execute("INSERT INTO base VALUES " + rows);
            drainWalAndMatViewQueues();
            execute("INSERT INTO base VALUES ('a', 50.0, '2024-01-05T00:20:00.000000Z')");
            drainWalAndMatViewQueues();
            execute("INSERT INTO base VALUES ('a', 7.0, '2024-01-07T00:10:00.000000Z')");
            drainWalAndMatViewQueues();
            assertQuery("SELECT name, numRows, pieceCount > 1 composite FROM table_partitions('price_1h')")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            name\tnumRows\tcomposite
                            2024-01\t8\ttrue
                            """);
            execute("ALTER TABLE base DROP PARTITION LIST '2024-01-07'");
            drainWalAndMatViewQueues();

            final long viewTxns = walTxnCount("price_1h");
            execute("REFRESH MATERIALIZED VIEW price_1h RANGE FROM '2024-01-01T00:00:00.000000Z' TO '2024-01-10T00:00:00.000000Z'");
            drainWalAndMatViewQueues();
            Assert.assertEquals(viewTxns + 2, walTxnCount("price_1h"));
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-02T00:00:00.000000Z\ta\t2.0
                    2024-01-04T00:00:00.000000Z\ta\t4.0
                    2024-01-05T00:00:00.000000Z\ta\t50.0
                    2024-01-06T00:00:00.000000Z\ta\t6.0
                    2024-01-07T00:00:00.000000Z\ta\t7.0
                    2024-01-08T00:00:00.000000Z\ta\t8.0
                    2024-01-09T00:00:00.000000Z\ta\t9.0
                    """);
            assertViewValid();
        });
    }

    @Test
    public void testGapsWithoutViewRowsCostNoCommits() throws Exception {
        // Performance property: a refresh whose working interval crosses partition gaps in which the view holds no
        // row, here the gaps of a base that never held rows there, writes as many REPLACE_RANGE commits as the same
        // refresh over a base without gaps, for an incremental refresh, a user RANGE and the min..max recompute alike.
        setProperty(PropertyKey.DEBUG_MAT_VIEW_REFRESH_MISSING_WAL_FILES_FATAL, "false");
        assertMemoryLeak(() -> {
            assertGapsWithoutViewRowsCostNoCommits(1);
            assertGapsWithoutViewRowsCostNoCommits(3);
        });
    }


    @Test
    public void testIncrementalRefreshAfterDstGapAcrossZones() throws Exception {
        // Zones whose gap holds the view's bucket floor: gaps at local midnight, 01:00, 02:00 and 03:00, a 30-minute
        // gap and a month bucket. Rows a minute before the transition and 11 hours after it, then a row 12 hours
        // after it: the incremental refresh of the last row keeps the view valid, and the view holds what its SQL
        // gives on the base.
        assertMemoryLeak(() -> {
            assertIncrementalRefreshAfterDstGap("cairo", "Africa/Cairo", "1d", "00:30", "2023-04-27T22:00:00");
            assertIncrementalRefreshAfterDstGap("new_york", "America/New_York", "1d", "02:30", "2023-03-12T07:00:00");
            assertIncrementalRefreshAfterDstGap("london", "Europe/London", "1d", "01:30", "2023-03-26T01:00:00");
            assertIncrementalRefreshAfterDstGap("helsinki", "Europe/Helsinki", "1d", "03:30", "2023-03-26T01:00:00");
            assertIncrementalRefreshAfterDstGap("lord_howe", "Australia/Lord_Howe", "1d", "02:15", "2023-09-30T15:30:00");
            assertIncrementalRefreshAfterDstGap("sydney", "Australia/Sydney", "1M", "02:30", "2023-09-30T16:00:00");
        });
    }

    @Test
    public void testIncrementalRefreshInBucketAfterDstGapKeepsViewValid() throws Exception {
        // An incremental refresh of a row in the bucket after the gap recomputes that bucket from the transition on,
        // and leaves the row before the transition in the bucket before the gap.
        assertMemoryLeak(() -> {
            createDstGapView("");
            insertDstGapRows();
            drainWalAndMatViewQueues();
            execute("INSERT INTO base VALUES ('a', 100, '2023-03-26T13:00:00.000000Z')");
            drainWalAndMatViewQueues();
            assertDstGapView("v", DST_GAP_VIEW_ROWS);
            assertViewCaughtUp("v");
        });
    }

    @Test
    public void testIncrementalRefreshNextToDstFoldAndGapEdgesKeepsViewValid() throws Exception {
        // A bucket floor inside the fold of 2023-10-29, where local 02:00-03:00 happens twice, and floors at the gap's
        // start and end, which exist: incremental refreshes next to them keep the views valid, and the views hold
        // what their SQL gives on the base.
        assertMemoryLeak(() -> {
            createDstGapBase("fold_base");
            final String foldSql = dstGapViewSql("fold_base", "Europe/Berlin", "1d", "02:30");
            execute("CREATE MATERIALIZED VIEW fold_v AS (" + foldSql + ") PARTITION BY DAY");
            execute("""
                    INSERT INTO fold_base VALUES
                    ('a', 1, '2023-10-28T12:00:00.000000Z'),
                    ('a', 2, '2023-10-29T00:45:00.000000Z'),
                    ('a', 4, '2023-10-29T01:15:00.000000Z'),
                    ('a', 8, '2023-10-29T01:45:00.000000Z')
                    """);
            drainWalAndMatViewQueues();
            assertDstGapViewMatchesSql("fold_v", foldSql);
            execute("INSERT INTO fold_base VALUES ('a', 16, '2023-10-29T05:00:00.000000Z')");
            drainWalAndMatViewQueues();
            assertDstGapViewMatchesSql("fold_v", foldSql);
            // local 02:40 CEST and 02:20 CET, both inside the fold
            execute("INSERT INTO fold_base VALUES ('a', 32, '2023-10-29T00:40:00.000000Z'), ('a', 64, '2023-10-29T01:20:00.000000Z')");
            drainWalAndMatViewQueues();
            assertDstGapViewMatchesSql("fold_v", foldSql);
            assertQuery("fold_v ORDER BY ts").timestamp("ts").noLeakCheck().expectSize().returns(replaceTs("""
                    ts\tsym\tc\ts
                    2023-10-28T00:30:00.000000Z\ta\t3\t69
                    2023-10-29T00:30:00.000000Z\ta\t2\t34
                    2023-10-29T01:30:00.000000Z\ta\t2\t24
                    """));

            for (String offset : new String[]{"02:00", "03:00"}) {
                final String base = "edge_" + offset.charAt(1) + "_base";
                final String view = "edge_" + offset.charAt(1) + "_v";
                final String sql = dstGapViewSql(base, "Europe/Berlin", "1d", offset);
                createDstGapBase(base);
                execute("CREATE MATERIALIZED VIEW " + view + " AS (" + sql + ") PARTITION BY DAY");
                execute("INSERT INTO " + base + " VALUES ('a', 1, '2023-03-26T00:59:00.000000Z'), ('a', 2, '2023-03-26T12:00:00.000000Z')");
                drainWalAndMatViewQueues();
                execute("INSERT INTO " + base + " VALUES ('a', 100, '2023-03-26T13:00:00.000000Z')");
                drainWalAndMatViewQueues();
                assertDstGapViewMatchesSql(view, sql);
                // an out-of-order row a year back makes the refresh snap from its bucket to the one at the gap's edge
                execute("INSERT INTO " + base + " VALUES ('a', 50, '2022-02-19T01:00:00.000000Z')");
                execute("INSERT INTO " + base + " VALUES ('a', 200, '2023-03-26T14:00:00.000000Z')");
                drainWalAndMatViewQueues();
                assertDstGapViewMatchesSql(view, sql);
            }
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
    public void testIncrementalRefreshesAtFirstTimestampsAfterDstGapKeepViewValid() throws Exception {
        // Rows at the first timestamps after the transition. One time unit after it, at local 03:00 plus one unit, the
        // row still lies in the DST shift, so the refresh merges its bucket with the bucket before the gap. Two units
        // after it, the row's bucket floor is the one inside the gap, and the refresh starts the bucket at the
        // transition.
        assertMemoryLeak(() -> {
            createDstGapView("");
            insertDstGapRows();
            drainWalAndMatViewQueues();
            execute("INSERT INTO base VALUES ('a', 10, '" + timestampAfterDstGapTransition(1) + "')");
            drainWalAndMatViewQueues();
            assertDstGapViewMatchesSql("v", DST_GAP_VIEW_SQL);
            execute("INSERT INTO base VALUES ('a', 20, '" + timestampAfterDstGapTransition(2) + "')");
            drainWalAndMatViewQueues();
            assertDstGapViewMatchesSql("v", DST_GAP_VIEW_SQL);
            assertQuery("v ORDER BY ts").timestamp("ts").noLeakCheck().expectSize().returns(replaceTs("""
                    ts\tsym\tc\ts
                    2023-03-25T01:30:00.000000Z\ta\t1\t1
                    2023-03-26T01:30:00.000000Z\ta\t3\t32
                    """));
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
    public void testOnlyGapsWithViewRowsKeepTheirCommits() throws Exception {
        // The mixed base never holds 2024-01-02, 2024-01-03 and 2024-01-08, and drops 2024-01-04 and 2024-01-06 after
        // the view's first build, which keeps its rows there. Its gaps are 2024-01-02..04, with view rows on the last
        // day only, 2024-01-06, with view rows, and 2024-01-08, without. An incremental refresh and a user RANGE
        // across all its days write two more view WAL transactions than the same refreshes of a view over a dense
        // base: one for each gap that holds view rows.
        assertMemoryLeak(() -> {
            for (String base : new String[]{"dense", "mixed"}) {
                execute("CREATE TABLE " + base + " (sym SYMBOL, price DOUBLE, ts " + tsType() + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("CREATE MATERIALIZED VIEW " + base + "_v AS (SELECT ts, sym, last(price) AS price FROM " + base + " SAMPLE BY 1h) PARTITION BY DAY");
            }
            final StringBuilder denseRows = new StringBuilder();
            final StringBuilder mixedRows = new StringBuilder();
            for (int day = 1; day < 10; day++) {
                appendRow(denseRows, day, "2024-01-0" + day + "T00:10:00.000000Z");
                if (day == 1 || day >= 4 && day != 8) {
                    appendRow(mixedRows, day, "2024-01-0" + day + "T00:10:00.000000Z");
                }
            }
            execute("INSERT INTO dense VALUES " + denseRows);
            execute("INSERT INTO mixed VALUES " + mixedRows);
            drainWalAndMatViewQueues();
            execute("ALTER TABLE mixed DROP PARTITION LIST '2024-01-04', '2024-01-06'");
            drainWalAndMatViewQueues();
            assertQuery("SELECT name FROM table_partitions('mixed') ORDER BY name")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            name
                            2024-01-01
                            2024-01-05
                            2024-01-07
                            2024-01-09
                            """);

            // One out-of-order commit into the days both bases hold.
            final String o3Rows = """
                    ('a', 10.0, '2024-01-01T00:20:00.000000Z'),
                    ('a', 50.0, '2024-01-05T00:20:00.000000Z'),
                    ('a', 70.0, '2024-01-07T00:20:00.000000Z'),
                    ('a', 90.0, '2024-01-09T00:20:00.000000Z')
                    """;
            long denseTxns = walTxnCount("dense_v");
            long mixedTxns = walTxnCount("mixed_v");
            execute("INSERT INTO dense VALUES " + o3Rows);
            execute("INSERT INTO mixed VALUES " + o3Rows);
            drainWalAndMatViewQueues();
            Assert.assertEquals("incremental refresh", walTxnCount("dense_v") - denseTxns + 2, walTxnCount("mixed_v") - mixedTxns);

            denseTxns = walTxnCount("dense_v");
            mixedTxns = walTxnCount("mixed_v");
            for (String base : new String[]{"dense", "mixed"}) {
                execute("REFRESH MATERIALIZED VIEW " + base + "_v RANGE FROM '2024-01-01T00:00:00.000000Z' TO '2024-01-10T00:00:00.000000Z'");
            }
            drainWalAndMatViewQueues();
            Assert.assertEquals("user RANGE", walTxnCount("dense_v") - denseTxns + 2, walTxnCount("mixed_v") - mixedTxns);

            assertQuery("mixed_v ORDER BY ts").timestamp("ts").noLeakCheck().expectSize().returns(replaceTs("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t10.0
                    2024-01-04T00:00:00.000000Z\ta\t4.0
                    2024-01-05T00:00:00.000000Z\ta\t50.0
                    2024-01-06T00:00:00.000000Z\ta\t6.0
                    2024-01-07T00:00:00.000000Z\ta\t70.0
                    2024-01-09T00:00:00.000000Z\ta\t90.0
                    """));
            assertSqlEquals("dense_v ORDER BY ts, sym", "(SELECT ts, sym, last(price) AS price FROM dense SAMPLE BY 1h) ORDER BY ts, sym");
        });
    }

    @Test
    public void testOutOfOrderAndPostGapWritesInOneRefreshKeepViewValid() throws Exception {
        // One incremental refresh of two transactions, an out-of-order row a year back and a row in the bucket after
        // the gap: the snap from the old row's bucket to the bucket after the gap lands on the transition.
        assertMemoryLeak(() -> {
            createDstGapView("");
            insertDstGapRows();
            drainWalAndMatViewQueues();
            execute("INSERT INTO base VALUES ('a', 50, '2022-02-19T01:00:00.000000Z')");
            execute("INSERT INTO base VALUES ('a', 100, '2023-03-26T13:00:00.000000Z')");
            drainWalAndMatViewQueues();
            assertDstGapView("v", """
                    ts\tsym\tc\ts
                    2022-02-18T01:30:00.000000Z\ta\t1\t50
                    2023-03-25T01:30:00.000000Z\ta\t1\t1
                    2023-03-26T01:30:00.000000Z\ta\t2\t102
                    """);
            assertViewCaughtUp("v");
        });
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
    public void testParquetViewPartitionKeepsGapCommit() throws Exception {
        // The view keeps its row of 2024-01-02, a day the base drops, in a partition that is in Parquet, as a storage
        // policy can leave it. The gap check reads only native timestamps and counts a Parquet partition it reaches as
        // holding rows, so a user RANGE keeps a commit for that gap, and joins the days around 2024-01-04, which never
        // held rows. Neither commit reaches the Parquet partition, which a replace commit cannot change.
        assertMemoryLeak(() -> {
            currentMicros = parseFloorPartialTimestamp("2024-01-06T00:00:00.000000Z");
            createBase("DAY", null);
            createView("1h", "DAY");
            execute("""
                    INSERT INTO base VALUES
                    ('a', 1.0, '2024-01-01T00:10:00.000000Z'),
                    ('b', 2.0, '2024-01-02T00:10:00.000000Z'),
                    ('c', 3.0, '2024-01-03T00:10:00.000000Z'),
                    ('e', 5.0, '2024-01-05T00:10:00.000000Z')
                    """);
            drainWalAndMatViewQueues();
            execute("ALTER TABLE base DROP PARTITION LIST '2024-01-02'");
            drainWalAndMatViewQueues();
            try (TableWriter writer = getWriter("price_1h")) {
                Assert.assertTrue(writer.convertPartitionNativeToParquet(ts("2024-01-02T00:00:00"), null, 0.01));
            }
            assertQuery("SELECT name, isParquet FROM table_partitions('price_1h') ORDER BY name")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            name\tisParquet
                            2024-01-01\tfalse
                            2024-01-02\ttrue
                            2024-01-03\tfalse
                            2024-01-05\tfalse
                            """);

            final long viewTxns = walTxnCount("price_1h");
            execute("REFRESH MATERIALIZED VIEW price_1h RANGE FROM '2024-01-01T00:00:00.000000Z' TO '2024-01-06T00:00:00.000000Z'");
            drainWalAndMatViewQueues();
            Assert.assertEquals(viewTxns + 2, walTxnCount("price_1h"));
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-02T00:00:00.000000Z\tb\t2.0
                    2024-01-03T00:00:00.000000Z\tc\t3.0
                    2024-01-05T00:00:00.000000Z\te\t5.0
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
    public void testPeriodRefreshTrimmingIncompletePeriodSkipsGapPastWorkingIntervals() throws Exception {
        // A period view has refreshed its periods up to 2024-01-04, over a base without a partition of 2024-01-02. A
        // late row into 2024-01-01 and a row of the incomplete period of 2024-01-04 queue two cached intervals, which the
        // cost model keeps apart. The incremental refresh trims the second one away with the incomplete period, and
        // clips up to the end of the last complete period, past the gap of 2024-01-02, which no working interval spans
        // any more. The gap check leaves that gap alone, and the refresh recomputes only the late row's bucket.
        assertMemoryLeak(() -> {
            createPeriodTimerView();
            currentMicros = parseFloorPartialTimestamp("2024-01-03T01:00:00.000000Z");
            execute("INSERT INTO base VALUES ('c', 3.0, '2024-01-03T00:10:00.000000Z')");
            drainWalQueue();
            runIncrementalRefresh("2024-01-04T12:00:00");
            assertRefreshPosition(2, "2024-01-04T00:00:00");
            assertBasePartitions("2024-01-01\n2024-01-03\n");

            currentMicros = parseFloorPartialTimestamp("2024-01-04T12:10:00.000000Z");
            execute("INSERT INTO base VALUES ('a', 11.0, '2024-01-01T00:20:00.000000Z')");
            execute("INSERT INTO base VALUES ('d', 4.0, '2024-01-04T10:10:00.000000Z')");
            drainWalQueue();
            disableIntervalClustering();
            runIncrementalRefresh("2024-01-04T12:30:00");
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t11.0
                    2024-01-01T05:00:00.000000Z\tb\t2.0
                    2024-01-03T00:00:00.000000Z\tc\t3.0
                    """);
            assertRefreshPosition(4, "2024-01-04T00:00:00");
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
    public void testRefreshAfterDstGapKeepsPreGapBucketWhoseFirstPartitionIsDropped() throws Exception {
        // The bucket before the gap, [2023-03-25T01:30Z, 2023-03-26T01:00Z), straddles the base partitions of
        // 2023-03-25 and 2023-03-26. The base drops 2023-03-25. A refresh of the bucket after the gap starts at the
        // transition, so it leaves the bucket before the gap as it was, with both its rows.
        assertMemoryLeak(() -> {
            createDstGapView("");
            execute("""
                    INSERT INTO base VALUES
                    ('a', 1, '2023-03-25T12:00:00.000000Z'),
                    ('a', 2, '2023-03-26T00:30:00.000000Z'),
                    ('a', 4, '2023-03-26T12:00:00.000000Z')
                    """);
            drainWalAndMatViewQueues();
            execute("ALTER TABLE base DROP PARTITION LIST '2023-03-25'");
            drainWalAndMatViewQueues();
            execute("INSERT INTO base VALUES ('a', 8, '2023-03-26T13:00:00.000000Z')");
            drainWalAndMatViewQueues();
            assertDstGapView("v", """
                    ts\tsym\tc\ts
                    2023-03-25T01:30:00.000000Z\ta\t2\t3
                    2023-03-26T01:30:00.000000Z\ta\t2\t12
                    """);
            assertViewCaughtUp("v");
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
    public void testRefreshLimitBoundaryInBucketAfterDstGapKeepsFrozenBuckets() throws Exception {
        // REFRESH LIMIT 11 HOURS with the base's newest row and the clock at 2023-03-26T13:00Z puts the boundary at
        // 02:00Z, inside the bucket after the gap. The refresh starts that bucket at the transition: the bucket before
        // the gap stays frozen, without the late row at 00:50Z, and a row the user backfilled below the boundary
        // survives.
        assertMemoryLeak(() -> {
            createDstGapView("");
            execute("""
                    INSERT INTO base VALUES
                    ('a', 1, '2023-03-25T12:00:00.000000Z'),
                    ('a', 2, '2023-03-26T00:45:00.000000Z'),
                    ('a', 4, '2023-03-26T12:00:00.000000Z')
                    """);
            drainWalAndMatViewQueues();
            currentMicros = parseFloorPartialTimestamp("2023-03-26T13:00:00.000000Z");
            execute("ALTER MATERIALIZED VIEW v SET REFRESH LIMIT 11 HOURS");
            drainWalAndMatViewQueues();
            execute("INSERT INTO v VALUES ('2023-03-24T01:30:00.000000Z', 'a', 7, 70)");
            drainWalAndMatViewQueues();
            assertDstGapView("v", """
                    ts\tsym\tc\ts
                    2023-03-24T01:30:00.000000Z\ta\t7\t70
                    2023-03-25T01:30:00.000000Z\ta\t2\t3
                    2023-03-26T01:30:00.000000Z\ta\t1\t4
                    """);

            execute("""
                    INSERT INTO base VALUES
                    ('a', 8, '2023-03-26T00:50:00.000000Z'),
                    ('a', 16, '2023-03-26T13:00:00.000000Z')
                    """);
            drainWalAndMatViewQueues();
            assertDstGapView("v", """
                    ts\tsym\tc\ts
                    2023-03-24T01:30:00.000000Z\ta\t7\t70
                    2023-03-25T01:30:00.000000Z\ta\t2\t3
                    2023-03-26T01:30:00.000000Z\ta\t2\t20
                    """);
            assertViewCaughtUp("v");
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
    public void testRefreshLimitViewKeepsGapCommitsForBackfill() throws Exception {
        // A view with REFRESH LIMIT keeps a commit for every gap: a user backfill into its frozen zone can commit after
        // the refresh checked the gaps, where the check cannot see it. Here a backfill into 2024-01-02, a gap of the
        // base that never held rows, commits once a user RANGE across the gap has committed its first batch, a day's
        // bucket. The RANGE's later commits, which apply after the backfill, leave it alone. A RANGE that joined the
        // days around the gap would replace it, from the end of its first batch on, with the base's rows there: none.
        setProperty(PropertyKey.CAIRO_MAT_VIEW_INSERT_AS_SELECT_BATCH_SIZE, 1);
        setProperty(PropertyKey.CAIRO_MAT_VIEW_ROWS_PER_QUERY_ESTIMATE, 1);
        assertMemoryLeak(() -> {
            currentMicros = parseFloorPartialTimestamp("2024-01-06T00:00:00.000000Z");
            createBase("DAY", null);
            createView("1h", "DAY");
            execute("""
                    INSERT INTO base VALUES
                    ('a', 1.0, '2024-01-01T00:10:00.000000Z'),
                    ('c', 3.0, '2024-01-03T00:10:00.000000Z'),
                    ('e', 5.0, '2024-01-05T00:10:00.000000Z')
                    """);
            drainWalAndMatViewQueues();
            execute("ALTER MATERIALIZED VIEW price_1h SET REFRESH LIMIT 2 DAYS");
            drainWalAndMatViewQueues();

            final long viewTxns = walTxnCount("price_1h");
            execute("REFRESH MATERIALIZED VIEW price_1h RANGE FROM '2024-01-01T00:00:00.000000Z' TO '2024-01-06T00:00:00.000000Z'");
            final AtomicBoolean isBackfilled = new AtomicBoolean();
            try (MatViewRefreshJob job = new MatViewRefreshJob(0, engine, 0)) {
                job.setOnBatchCommittedForTesting(() -> {
                    if (isBackfilled.compareAndSet(false, true)) {
                        try {
                            execute("INSERT INTO price_1h VALUES ('2024-01-02T05:00:00.000000Z', 'y', 7.0)");
                        } catch (SqlException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
                drainMatViewQueue(job);
            }
            Assert.assertTrue(isBackfilled.get());
            // the backfill and a commit for each of the base's three days
            Assert.assertEquals(viewTxns + 4, walTxnCount("price_1h"));
            drainWalAndMatViewQueues();
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-02T05:00:00.000000Z\ty\t7.0
                    2024-01-03T00:00:00.000000Z\tc\t3.0
                    2024-01-05T00:00:00.000000Z\te\t5.0
                    """);
            assertViewValid();
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
    public void testResumeCatchesUpInBucketAfterDstGap() throws Exception {
        // RESUME catches up on a row that landed in the bucket after the gap while the view was invalid.
        assertMemoryLeak(() -> {
            createDstGapView("");
            insertDstGapRows();
            drainWalAndMatViewQueues();
            engine.getMatViewStateStore().enqueueInvalidate(engine.verifyTableName("v"), "test invalidation");
            drainWalAndMatViewQueues();
            assertQuery("SELECT view_status, invalidation_reason FROM materialized_views() WHERE view_name = 'v'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("view_status\tinvalidation_reason\ninvalid\ttest invalidation\n");
            execute("INSERT INTO base VALUES ('a', 100, '2023-03-26T13:00:00.000000Z')");
            drainWalAndMatViewQueues();

            execute("REFRESH MATERIALIZED VIEW v RESUME");
            drainWalAndMatViewQueues();
            assertDstGapView("v", DST_GAP_VIEW_ROWS);
            assertViewCaughtUp("v");
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
    public void testStreamingRowsAcrossDstGapKeepViewValid() throws Exception {
        // One row per transaction every 10 minutes from 00:00Z to 02:00Z, across the transition at 01:00Z: every
        // incremental refresh keeps the view valid. The six rows before the transition count in the bucket before the
        // gap, the seven from it on in the bucket inside it.
        assertMemoryLeak(() -> {
            createDstGapView("");
            for (int i = 0; i < 13; i++) {
                execute(String.format("INSERT INTO base VALUES ('a', %d, '2023-03-26T%02d:%02d:00.000000Z')", i + 1, i / 6, 10 * (i % 6)));
                drainWalAndMatViewQueues();
                assertDstGapViewValid("v");
            }
            assertDstGapView("v", """
                    ts\tsym\tc\ts
                    2023-03-25T01:30:00.000000Z\ta\t6\t21
                    2023-03-26T01:30:00.000000Z\ta\t7\t70
                    """);
            assertViewCaughtUp("v");
        });
    }

    @Test
    public void testUnappliedViewRowsKeepGapCommits() throws Exception {
        // A view transaction that the WAL apply has not reached writes a row into 2024-01-02, a gap of the base that
        // never held rows, as an earlier refresh's commit can while the apply lags behind: it replaces the row's
        // bucket, as a refresh commit does. A user RANGE that runs before the apply cannot see the row in the view's
        // applied data, so it keeps a commit for every gap, and the row survives the RANGE's commits, which apply
        // after it. Once the row is applied, a RANGE keeps the commit of its gap only.
        assertMemoryLeak(() -> {
            // A fixed clock past the base's newest row keeps the view's refresh timestamps, and so its status,
            // deterministic.
            currentMicros = parseFloorPartialTimestamp("2024-01-06T00:00:00.000000Z");
            createBase("DAY", null);
            createManualView();
            execute("""
                    INSERT INTO base VALUES
                    ('a', 1.0, '2024-01-01T00:10:00.000000Z'),
                    ('c', 3.0, '2024-01-03T00:10:00.000000Z'),
                    ('e', 5.0, '2024-01-05T00:10:00.000000Z')
                    """);
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();

            try (WalWriter walWriter = engine.getWalWriter(engine.verifyTableName("price_1h"))) {
                final TableWriter.Row row = walWriter.newRow(ts("2024-01-02T00:00:00"));
                row.putSym(1, "z");
                row.putDouble(2, 9.0);
                row.append();
                walWriter.commitWithParams(ts("2024-01-02T00:00:00"), ts("2024-01-02T01:00:00"), WAL_DEDUP_MODE_REPLACE_RANGE);
            }
            final String range = "REFRESH MATERIALIZED VIEW price_1h RANGE FROM '2024-01-01T00:00:00.000000Z' TO '2024-01-06T00:00:00.000000Z'";
            long viewTxns = walTxnCount("price_1h");
            execute(range);
            drainMatViewQueue(engine);
            // a commit for each of the base's three days
            Assert.assertEquals(viewTxns + 3, walTxnCount("price_1h"));
            drainWalQueue();
            final String expectedRows = """
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-02T00:00:00.000000Z\tz\t9.0
                    2024-01-03T00:00:00.000000Z\tc\t3.0
                    2024-01-05T00:00:00.000000Z\te\t5.0
                    """;
            assertViewRows(expectedRows);

            viewTxns = walTxnCount("price_1h");
            execute(range);
            drainWalAndMatViewQueues();
            // 2024-01-02 keeps its commit, 2024-01-04 joins the days around it
            Assert.assertEquals(viewTxns + 2, walTxnCount("price_1h"));
            assertViewRows(expectedRows);
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
    public void testUserRangeInBucketAfterDstGapRefreshesRows() throws Exception {
        // A user RANGE over the bucket after the gap, which the clip turns into a working interval, recomputes the
        // bucket from the transition on and records no refresh error.
        assertMemoryLeak(() -> {
            // A fixed clock keeps the view's refresh timestamps, and so its status, deterministic.
            currentMicros = parseFloorPartialTimestamp("2023-03-27T00:00:00.000000Z");
            createDstGapView("REFRESH MANUAL ");
            insertDstGapRows();
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW v FULL");
            drainWalAndMatViewQueues();
            execute("INSERT INTO base VALUES ('a', 100, '2023-03-26T13:00:00.000000Z')");
            drainWalAndMatViewQueues();
            assertDstGapView("v", """
                    ts\tsym\tc\ts
                    2023-03-25T01:30:00.000000Z\ta\t1\t1
                    2023-03-26T01:30:00.000000Z\ta\t1\t2
                    """);

            execute("REFRESH MATERIALIZED VIEW v RANGE FROM '2023-03-26T12:00:00.000000Z' TO '2023-03-26T14:00:00.000000Z'");
            drainWalAndMatViewQueues();
            assertDstGapView("v", DST_GAP_VIEW_ROWS);
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

    // Writes, into a dense base (days 1..2N+1) and into a base that lacks every second day (N gaps that
    // never held rows), the same out-of-order rows across all their days, and refreshes both views the
    // same three ways: incrementally, with a user RANGE and with the min..max recompute after missing
    // WAL files. The views hold no rows in the gaps, so each refresh of the view over the base with gaps
    // writes as many view WAL transactions as the same refresh over the dense base.
    private void assertGapsWithoutViewRowsCostNoCommits(int gapCount) throws Exception {
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
                walTxnCount(dense + "_v") - denseTxns,
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
                walTxnCount(dense + "_v") - denseTxns,
                walTxnCount(gappy + "_v") - gappyTxns
        );

        // Another out-of-order commit across every day of each base, whose WAL events the refreshes
        // cannot read.
        denseTxns = walTxnCount(dense + "_v");
        gappyTxns = walTxnCount(gappy + "_v");
        execute("INSERT INTO " + dense + " VALUES " + denseO3Rows.toString().replace(":20:", ":30:"));
        execute("INSERT INTO " + gappy + " VALUES " + gappyO3Rows.toString().replace(":20:", ":30:"));
        drainWalQueue();
        removeWalEventFiles(engine.verifyTableName(dense));
        removeWalEventFiles(engine.verifyTableName(gappy));
        drainWalAndMatViewQueues();
        Assert.assertEquals(
                "min..max recompute across " + gapCount + " gaps",
                walTxnCount(dense + "_v") - denseTxns,
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

    private void assertDstGapView(String viewName, String expectedRows) throws Exception {
        assertQuery(viewName + " ORDER BY ts, sym").timestamp("ts").noLeakCheck().expectSize().returns(replaceTs(expectedRows));
        assertDstGapViewValid(viewName);
    }

    private void assertDstGapViewMatchesSql(String viewName, String viewSql) throws Exception {
        assertDstGapViewValid(viewName);
        assertSqlEquals(viewName + " ORDER BY ts, sym", "(" + viewSql + ") ORDER BY ts, sym");
    }

    private void assertDstGapViewValid(String viewName) throws Exception {
        assertQuery("SELECT view_status, invalidation_reason, last_refresh_error FROM materialized_views() WHERE view_name = '" + viewName + "'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("view_status\tinvalidation_reason\tlast_refresh_error\nvalid\t\t\n");
    }

    // A view over its own base in the given zone and bucket: rows a minute before the transition and 11 hours after it
    // build it, and the incremental refresh of a row 12 hours after it, in the bucket whose floor lies in the gap, keeps
    // it valid and equal to its SQL.
    private void assertIncrementalRefreshAfterDstGap(String name, String zone, String sampleBy, String offset, String transition) throws Exception {
        final String base = name + "_base";
        final String view = name + "_v";
        final String viewSql = dstGapViewSql(base, zone, sampleBy, offset);
        createDstGapBase(base);
        execute("CREATE MATERIALIZED VIEW " + view + " AS (" + viewSql + ") PARTITION BY DAY");
        final LocalDateTime transitionTs = LocalDateTime.parse(transition);
        execute("INSERT INTO " + base + " VALUES ('a', 1, '" + isoMicros(transitionTs.minusMinutes(1)) + "'), ('a', 2, '" + isoMicros(transitionTs.plusHours(11)) + "')");
        drainWalAndMatViewQueues();
        execute("INSERT INTO " + base + " VALUES ('a', 4, '" + isoMicros(transitionTs.plusHours(12)) + "')");
        drainWalAndMatViewQueues();
        assertDstGapViewMatchesSql(view, viewSql);
        assertQuery("SELECT sum(c) c, sum(s) s FROM " + view)
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns("c\ts\n3\t7\n");
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

    private static String dstGapViewSql(String base, String zone, String sampleBy, String offset) {
        return "SELECT ts, sym, count() AS c, sum(x) AS s FROM " + base + " SAMPLE BY " + sampleBy
                + " ALIGN TO CALENDAR TIME ZONE '" + zone + "' WITH OFFSET '" + offset + "'";
    }

    // A child view refreshes after its parent's refresh commit applies, so each drain round reaches one
    // level deeper.
    private static void drainParentAndChild() {
        drainWalAndMatViewQueues();
        drainWalAndMatViewQueues();
    }

    private void createDstGapBase(String base) throws Exception {
        execute("CREATE TABLE " + base + " (sym SYMBOL, x LONG, ts " + tsType() + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
    }

    // The view v of DST_GAP_VIEW_SQL over a new base table, with the given refresh clause.
    private void createDstGapView(String refreshClause) throws Exception {
        createDstGapBase("base");
        execute("CREATE MATERIALIZED VIEW v " + refreshClause + "AS (" + DST_GAP_VIEW_SQL + ") PARTITION BY DAY");
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

    // The rows of the view of createDstGapView() before the transition and in the bucket inside the gap: x=1 at
    // 00:59Z, local 01:59 CET, and x=2 at 12:00Z.
    private void insertDstGapRows() throws Exception {
        execute("INSERT INTO base VALUES ('a', 1, '2023-03-26T00:59:00.000000Z'), ('a', 2, '2023-03-26T12:00:00.000000Z')");
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

    private static String isoMicros(LocalDateTime utc) {
        return utc.format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")) + ".000000Z";
    }

    // Drives the commit-gap cost model of price_1h to a zero threshold, so that its next incremental refresh keeps
    // each cached interval as a cluster of its own.
    private static void disableIntervalClustering() {
        final MatViewState state = getViewState();
        Assert.assertTrue(state.tryLock());
        try {
            for (int i = 0; i < 128; i++) {
                state.recordCommitNanos(1);
                state.recordScanMetrics(Long.MAX_VALUE / 8, 1);
            }
            Assert.assertEquals(0, state.getCommitGapThresholdTsUnits());
        } finally {
            state.unlock();
        }
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

    // The timestamp the given number of time units after the transition of DST_GAP_VIEW_SQL, 2023-03-26T01:00Z.
    private String timestampAfterDstGapTransition(int units) {
        return ColumnType.isTimestampMicro(timestampType.getTimestampType())
                ? "2023-03-26T01:00:00.00000" + units + "Z"
                : "2023-03-26T01:00:00.00000000" + units + "Z";
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
