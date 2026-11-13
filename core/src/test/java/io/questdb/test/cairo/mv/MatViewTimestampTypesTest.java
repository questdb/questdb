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

package io.questdb.test.cairo.mv;

import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

/**
 * Views on a TIMESTAMP and on a TIMESTAMP_NS base, built on a table and on another view.
 * <ul>
 *     <li>A child view on a parent materialized view tracks the parent's commits as its backlog while it is
 *     invalid, and RESUME catches it up. The child keeps its rows where the parent dropped a partition (design
 *     section 8), and follows the rows a parent FULL re-inserts.</li>
 *     <li>A parent view DROP COLUMN that the child's SQL reads invalidates the child at its next refresh that
 *     recomputes rows, not when the drop applies (section 2).</li>
 *     <li>View DROP PARTITION LIST and WHERE, with a literal at the type's precision; the clip keeps refreshes
 *     out of the dropped range, and RANGE brings it back.</li>
 *     <li>RESUME SKIP BACKLOG moves a period view's last refreshed period to the current period boundary, in the
 *     base's timestamp units and time zone.</li>
 * </ul>
 * Expected rows follow from the base rows each test inserts; every timestamp is printed with the type's precision.
 */
@RunWith(Parameterized.class)
public class MatViewTimestampTypesTest extends AbstractCairoTest {
    private static final String CHILD_SQL = "SELECT ts, sym, sum(price) AS price FROM parent SAMPLE BY 1d";
    private static final String PARENT_SQL = "SELECT ts, sym, sum(price) AS price FROM base SAMPLE BY 1h";
    private final TestTimestampType timestampType;

    public MatViewTimestampTypesTest(TestTimestampType timestampType) {
        this.timestampType = timestampType;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> params() {
        return Arrays.asList(new Object[][]{
                {TestTimestampType.MICRO}, {TestTimestampType.NANO}
        });
    }

    @Test
    public void testChildBacklogCoversParentFull() throws Exception {
        // A parent FULL truncates the parent and re-inserts every row while the child is invalid. The child's
        // backlog covers the re-inserted range, so RESUME leaves the child equal to its SQL over the parent.
        assertMemoryLeak(() -> {
            createParentAndChild();
            final TableToken childToken = engine.verifyTableName("child");
            invalidate(childToken);
            execute("INSERT INTO base VALUES ('c', 7.0, '2024-01-04T05:00:00.000000Z')");
            drainWalAndMatViewQueues();
            updateBacklog(childToken);
            execute("REFRESH MATERIALIZED VIEW parent FULL");
            drainWalAndMatViewQueues();
            drainWalAndMatViewQueues();
            updateBacklog(childToken);

            execute("REFRESH MATERIALIZED VIEW child RESUME");
            drainWalAndMatViewQueues();
            assertViewStatus("child", "valid", "");
            assertRows("child", """
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-01T00:00:00.000000Z\tb\t2.0
                    2024-01-02T00:00:00.000000Z\ta\t3.0
                    2024-01-04T00:00:00.000000Z\tc\t7.0
                    """);
            assertSqlCursors(CHILD_SQL + " ORDER BY ts, sym", "child ORDER BY ts, sym");
        });
    }

    @Test
    public void testChildBacklogFollowsParentRefreshesAndDropPartition() throws Exception {
        // The parent refreshes twice and drops a partition while the child is invalid. RESUME catches the child up
        // on the parent's commits, and the child keeps its bucket where the parent dropped its partition.
        assertMemoryLeak(() -> {
            createParentAndChild();
            final TableToken childToken = engine.verifyTableName("child");
            invalidate(childToken);

            execute("INSERT INTO base VALUES ('b', 10.0, '2024-01-03T05:00:00.000000Z')");
            drainWalAndMatViewQueues();
            updateBacklog(childToken);
            execute("INSERT INTO base VALUES ('a', 20.0, '2024-01-01T00:20:00.000000Z')");
            drainWalAndMatViewQueues();
            updateBacklog(childToken);
            execute("ALTER MATERIALIZED VIEW parent DROP PARTITION LIST '2024-01-02'");
            drainWalAndMatViewQueues();
            updateBacklog(childToken);
            assertViewStatus("child", "invalid", "test invalidation");
            // the invalid child did not move
            assertRows("child", """
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-01T00:00:00.000000Z\tb\t2.0
                    2024-01-02T00:00:00.000000Z\ta\t3.0
                    """);

            execute("REFRESH MATERIALIZED VIEW child RESUME");
            drainWalAndMatViewQueues();
            assertViewStatus("child", "valid", "");
            assertViewStatus("parent", "valid", "");
            assertRows("parent", """
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t21.0
                    2024-01-01T01:00:00.000000Z\tb\t2.0
                    2024-01-03T05:00:00.000000Z\tb\t10.0
                    """);
            assertRows("child", """
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t21.0
                    2024-01-01T00:00:00.000000Z\tb\t2.0
                    2024-01-02T00:00:00.000000Z\ta\t3.0
                    2024-01-03T00:00:00.000000Z\tb\t10.0
                    """);

            // the caught-up child follows the parent's next commit, which the second drain refreshes it with
            execute("INSERT INTO base VALUES ('b', 1.5, '2024-01-03T06:00:00.000000Z')");
            drainWalAndMatViewQueues();
            drainWalAndMatViewQueues();
            assertRows("child", """
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t21.0
                    2024-01-01T00:00:00.000000Z\tb\t2.0
                    2024-01-02T00:00:00.000000Z\ta\t3.0
                    2024-01-03T00:00:00.000000Z\tb\t11.5
                    """);
        });
    }

    @Test
    public void testBaseSwitchFromTableToViewAndBack() throws Exception {
        // ALTER ... WITH BASE moves a view from a table to a materialized view on that table and back. Each switch
        // leaves the view invalid until RESUME SKIP BACKLOG positions it on the new base, whose commits it then follows.
        assertMemoryLeak(() -> {
            createParentAndChild();
            execute("CREATE MATERIALIZED VIEW daily AS (SELECT ts, sym, sum(price) AS price FROM base SAMPLE BY 1d) PARTITION BY MONTH");
            drainWalAndMatViewQueues();
            final String dailyRows = """
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-01T00:00:00.000000Z\tb\t2.0
                    2024-01-02T00:00:00.000000Z\ta\t3.0
                    """;
            assertRows("daily", dailyRows);

            execute("ALTER MATERIALIZED VIEW daily WITH BASE parent AS (SELECT ts, sym, sum(price) AS price FROM parent SAMPLE BY 1d)");
            drainWalAndMatViewQueues();
            assertViewStatus("daily", "invalid", "base table changed");
            execute("REFRESH MATERIALIZED VIEW daily RESUME SKIP BACKLOG");
            drainWalAndMatViewQueues();
            assertViewStatus("daily", "valid", "");
            assertQuery("SELECT base_table_name FROM materialized_views() WHERE view_name = 'daily'").noLeakCheck().noRandomAccess()
                    .returns("base_table_name\nparent\n");
            // a base row reaches the view through the parent: the first drain refreshes the parent, the second the view
            execute("INSERT INTO base VALUES ('b', 10.0, '2024-01-03T05:00:00.000000Z')");
            drainWalAndMatViewQueues();
            drainWalAndMatViewQueues();
            final String viaParentRows = dailyRows + """
                    2024-01-03T00:00:00.000000Z\tb\t10.0
                    """;
            assertRows("daily", viaParentRows);

            execute("ALTER MATERIALIZED VIEW daily WITH BASE base AS (SELECT ts, sym, sum(price) AS price FROM base SAMPLE BY 1d)");
            drainWalAndMatViewQueues();
            assertViewStatus("daily", "invalid", "base table changed");
            execute("REFRESH MATERIALIZED VIEW daily RESUME SKIP BACKLOG");
            drainWalAndMatViewQueues();
            assertViewStatus("daily", "valid", "");
            // a base row reaches the view straight from the base, in one drain
            execute("INSERT INTO base VALUES ('c', 7.0, '2024-01-04T05:00:00.000000Z')");
            drainWalAndMatViewQueues();
            assertRows("daily", viaParentRows + """
                    2024-01-04T00:00:00.000000Z\tc\t7.0
                    """);
            assertQuery("SELECT base_table_name FROM materialized_views() WHERE view_name = 'daily'").noLeakCheck().noRandomAccess()
                    .returns("base_table_name\nbase\n");
        });
    }

    @Test
    public void testDropPartitionWhereEvaluatesNowWhenStatementRuns() throws Exception {
        // DROP PARTITION WHERE on a view keeps ALTER TABLE's semantics: now() is the time the statement runs, not
        // the time the view's WAL applies it. A plain WAL table running the same statement is the oracle.
        // TIMESTAMP_NS is left out: on a plain TIMESTAMP_NS WAL table, the WAL apply job hands now() the commit
        // timestamp in microseconds as if it were nanoseconds, so DROP PARTITION WHERE ts < now() drops nothing.
        // That predates this feature and needs its own fix (ApplyWal2TableJob, setNowAndFixClock()), see
        // questdb/questdb#8047. Remove the Assume with the fix.
        Assume.assumeTrue(timestampType == TestTimestampType.MICRO);
        assertMemoryLeak(() -> {
            setCurrentMicros(parseFloorPartialTimestamp("2024-01-05T00:00:00.000000Z"));
            createBase();
            insertFourDays();
            drainWalQueue();
            execute("CREATE TABLE plain (sym SYMBOL, price DOUBLE, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO plain SELECT * FROM base");
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW mv AS (" + PARENT_SQL + ") PARTITION BY DAY");
            drainWalAndMatViewQueues();

            execute("ALTER MATERIALIZED VIEW mv DROP PARTITION WHERE ts < dateadd('d', -2, now())");
            execute("ALTER TABLE plain DROP PARTITION WHERE ts < dateadd('d', -2, now())");
            setCurrentMicros(parseFloorPartialTimestamp("2024-01-10T00:00:00.000000Z"));
            drainWalAndMatViewQueues();
            assertRows("plain", """
                    sym\tprice\tts
                    a\t3.0\t2024-01-03T00:10:00.000000Z
                    a\t4.0\t2024-01-04T00:10:00.000000Z
                    """);
            assertRows("mv", """
                    ts\tsym\tprice
                    2024-01-03T00:00:00.000000Z\ta\t3.0
                    2024-01-04T00:00:00.000000Z\ta\t4.0
                    """);
            assertViewStatus("mv", "valid", "");
        });
    }

    @Test
    public void testParentDropColumnReadByChildInvalidatesChildAtNextRefresh() throws Exception {
        // The parent drops a column the child's SQL reads. The drop itself leaves the child valid with its rows;
        // the child's next refresh that recomputes rows fails to compile and invalidates it. ALTER ... AS and RESUME
        // recover it, and the backlog refresh recomputes the bucket the failed refresh did not reach.
        assertMemoryLeak(() -> {
            createParentAndChild();
            final String childRows = """
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-01T00:00:00.000000Z\tb\t2.0
                    2024-01-02T00:00:00.000000Z\ta\t3.0
                    """;
            execute("ALTER MATERIALIZED VIEW parent DROP COLUMN price");
            drainWalAndMatViewQueues();
            assertViewStatus("child", "valid", "");
            assertRows("child", childRows);

            // the first drain refreshes the parent, the second the child
            execute("INSERT INTO base VALUES ('b', 10.0, '2024-01-03T05:00:00.000000Z')");
            drainWalAndMatViewQueues();
            drainWalAndMatViewQueues();
            assertViewStatus("parent", "valid", "");
            assertViewStatus("child", "invalid", "[" + CHILD_SQL.indexOf("price") + "]: Invalid column: price");
            assertRows("child", childRows);

            execute("ALTER MATERIALIZED VIEW child AS (SELECT ts, sym, count() AS price FROM parent SAMPLE BY 1d)");
            // RESUME waits for the view's WAL to apply the definition; no WAL apply job runs in the background here
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW child RESUME");
            drainWalAndMatViewQueues();
            assertViewStatus("child", "valid", "");
            assertRows("child", childRows + """
                    2024-01-03T00:00:00.000000Z\tb\t1.0
                    """);
        });
    }

    @Test
    public void testResumeSkipBacklogMovesPeriodToCurrentBoundary() throws Exception {
        assertResumeSkipBacklogOnPeriodView("", "2024-01-04T00:00:00.000000Z");
    }

    @Test
    public void testResumeSkipBacklogMovesPeriodToCurrentBoundaryInTimeZone() throws Exception {
        // 2024-01-04T12:00Z is 21:00 in Tokyo (UTC+9), so the current Tokyo day began at 2024-01-03T15:00Z.
        assertResumeSkipBacklogOnPeriodView(" TIME ZONE 'Asia/Tokyo'", "2024-01-03T15:00:00.000000Z");
    }

    @Test
    public void testViewDropPartitionListAndWhereAtTypePrecision() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            insertFourDays();
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW mv AS (" + PARENT_SQL + ") PARTITION BY DAY");
            drainWalAndMatViewQueues();

            execute("ALTER MATERIALIZED VIEW mv DROP PARTITION LIST '2024-01-02'");
            drainWalAndMatViewQueues();
            assertRows("mv", """
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-03T00:00:00.000000Z\ta\t3.0
                    2024-01-04T00:00:00.000000Z\ta\t4.0
                    """);
            // One tick past the start of 2024-01-03 drops that day's partition too; a literal cut to a coarser
            // precision would equal the partition start and keep it.
            final String oneTickPast = timestampType == TestTimestampType.NANO ? "2024-01-03T00:00:00.000000001Z" : "2024-01-03T00:00:00.000001Z";
            execute("ALTER MATERIALIZED VIEW mv DROP PARTITION WHERE ts < '" + oneTickPast + "'");
            drainWalAndMatViewQueues();
            assertRows("mv", """
                    ts\tsym\tprice
                    2024-01-04T00:00:00.000000Z\ta\t4.0
                    """);
            // the view stays valid and keeps its refresh position
            assertQuery("SELECT view_status, refresh_base_table_txn FROM materialized_views()").noLeakCheck().noRandomAccess()
                    .returns("view_status\trefresh_base_table_txn\nvalid\t1\n");

            // a base row in a kept partition reaches the view; the dropped range stays empty
            execute("INSERT INTO base VALUES ('b', 5.0, '2024-01-04T05:10:00.000000Z')");
            drainWalAndMatViewQueues();
            assertRows("mv", """
                    ts\tsym\tprice
                    2024-01-04T00:00:00.000000Z\ta\t4.0
                    2024-01-04T05:00:00.000000Z\tb\t5.0
                    """);
            // RANGE recomputes the dropped range where the base still holds rows
            execute("REFRESH MATERIALIZED VIEW mv RANGE FROM '2024-01-01' TO '2024-01-05'");
            drainWalAndMatViewQueues();
            assertRows("mv", """
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-02T00:00:00.000000Z\ta\t2.0
                    2024-01-03T00:00:00.000000Z\ta\t3.0
                    2024-01-04T00:00:00.000000Z\ta\t4.0
                    2024-01-04T05:00:00.000000Z\tb\t5.0
                    """);
        });
    }

    private void assertResumeSkipBacklogOnPeriodView(String timeZone, String expectedPeriodHi) throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(parseFloorPartialTimestamp("2024-01-01T00:00:00.000000Z"));
            createBase();
            execute("CREATE MATERIALIZED VIEW mv REFRESH IMMEDIATE PERIOD (LENGTH 1d" + timeZone + ") AS (" + PARENT_SQL + ") PARTITION BY DAY");
            execute("INSERT INTO base VALUES ('a', 1.0, '2024-01-01T00:10:00.000000Z')");
            setCurrentMicros(parseFloorPartialTimestamp("2024-01-02T20:00:00.000000Z"));
            drainWalAndMatViewQueues();
            assertRows("mv", """
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    """);
            final TableToken viewToken = engine.verifyTableName("mv");
            invalidate(viewToken);
            execute("INSERT INTO base VALUES ('c', 4.0, '2024-01-02T10:10:00.000000Z')");
            drainWalAndMatViewQueues();

            setCurrentMicros(parseFloorPartialTimestamp("2024-01-04T12:00:00.000000Z"));
            execute("REFRESH MATERIALIZED VIEW mv RESUME SKIP BACKLOG");
            final MatViewState viewState = engine.getMatViewStateStore().getViewState(viewToken);
            Assert.assertEquals(timestampType.getDriver().parseFloorLiteral(expectedPeriodHi), viewState.getLastPeriodHi());
            drainWalAndMatViewQueues();
            assertViewStatus("mv", "valid", "");
            // c's row fell into the skipped gap
            assertRows("mv", """
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    """);
        });
    }

    private void assertRows(String tableName, String expectedMicros) throws Exception {
        assertQuery(tableName + " ORDER BY ts" + (tableName.equals("plain") ? "" : ", sym")).timestamp("ts").noLeakCheck().expectSize()
                .returns(timestampType == TestTimestampType.NANO ? expectedMicros.replace(".000000Z", ".000000000Z") : expectedMicros);
    }

    private void assertViewStatus(String viewName, String status, String reason) throws Exception {
        assertQuery("SELECT view_status, invalidation_reason FROM materialized_views() WHERE view_name = '" + viewName + "'")
                .noLeakCheck().noRandomAccess().returns("view_status\tinvalidation_reason\n" + status + '\t' + reason + '\n');
    }

    private void createBase() throws SqlException {
        execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
    }

    // Base rows a 1.0 and b 2.0 on 2024-01-01 and a 3.0 on 2024-01-02; parent buckets of 1h, child buckets of 1d.
    private void createParentAndChild() throws SqlException {
        createBase();
        execute("""
                INSERT INTO base VALUES
                ('a', 1.0, '2024-01-01T00:10:00.000000Z'),
                ('b', 2.0, '2024-01-01T01:10:00.000000Z'),
                ('a', 3.0, '2024-01-02T00:10:00.000000Z')
                """);
        drainWalQueue();
        execute("CREATE MATERIALIZED VIEW parent AS (" + PARENT_SQL + ") PARTITION BY DAY");
        drainWalAndMatViewQueues();
        execute("CREATE MATERIALIZED VIEW child AS (" + CHILD_SQL + ") PARTITION BY MONTH");
        drainWalAndMatViewQueues();
    }

    private static void insertFourDays() throws SqlException {
        execute("""
                INSERT INTO base VALUES
                ('a', 1.0, '2024-01-01T00:10:00.000000Z'),
                ('a', 2.0, '2024-01-02T00:10:00.000000Z'),
                ('a', 3.0, '2024-01-03T00:10:00.000000Z'),
                ('a', 4.0, '2024-01-04T00:10:00.000000Z')
                """);
    }

    private static void invalidate(TableToken viewToken) {
        engine.getMatViewStateStore().enqueueInvalidate(viewToken, "test invalidation");
        drainWalAndMatViewQueues();
    }

    // Runs a backlog cache update, as the refresh intervals update timer enqueues it.
    private static void updateBacklog(TableToken viewToken) {
        engine.getMatViewStateStore().enqueueUpdateRefreshIntervals(viewToken);
        drainWalAndMatViewQueues();
    }
}
