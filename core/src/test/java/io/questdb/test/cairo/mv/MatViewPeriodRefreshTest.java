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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.mv.MatViewTimerJob;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.List;

import static io.questdb.cairo.wal.WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE;

/**
 * A period materialized view refreshes up to the end of the last complete period, but recomputes whole SAMPLE BY
 * buckets, so a bucket that crosses that end takes in rows from the incomplete period. A later change to those rows
 * waits for the refresh that runs once their period completes. These tests check that this refresh starts at the
 * end of the last refreshed period, and not at the earliest base row, which a range replace can move past the
 * bucket.
 */
@RunWith(Parameterized.class)
public class MatViewPeriodRefreshTest extends AbstractCairoTest {
    private final TestTimestampType timestampType;

    public MatViewPeriodRefreshTest(TestTimestampType timestampType) {
        this.timestampType = timestampType;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<TestTimestampType> testParams() {
        return List.of(TestTimestampType.MICRO, TestTimestampType.NANO);
    }

    @Test
    public void testAlteredViewRemovesBucketAfterRangeReplaceDeletesItsRows() throws Exception {
        // The ALTER keeps the refresh state of the view, which has refreshed a base txn but has never recorded
        // the end of a refreshed period, so its next incremental refresh records it.
        testRangeReplaceDeletesRowsOfBucketCrossingPeriodHi(
                "create materialized view mv refresh immediate as " + viewSql("base", "4h"),
                "alter materialized view mv set refresh immediate period (length 7h)"
        );
    }

    @Test
    public void testFirstRefreshRecordsPeriodHiWithoutRowsInCompletePeriods() throws Exception {
        assertMemoryLeak(() -> {
            // The 7h periods end at 05:00 and 12:00 on 2022-01-02, so at 10:00 the last complete period ends at 05:00.
            currentMicros = parseFloorPartialTimestamp("2022-01-02T10:00:00.000000Z");
            // The view refreshes for the first time at creation, while its base table is empty.
            createBaseTable("empty_base");
            execute("create materialized view empty_mv refresh immediate period (length 7h) as " + viewSql("empty_base", "4h"));
            // The deferred view refreshes for the first time after the first base commit, which writes a row in the
            // incomplete period.
            createBaseTable("late_base");
            execute("create materialized view late_mv refresh immediate deferred period (length 7h) as " + viewSql("late_base", "4h"));
            execute("insert into late_base values (1, '2022-01-02T10:30')");
            final MatViewTimerJob timerJob = new MatViewTimerJob(engine);
            drain(timerJob);

            // Neither view has rows to refresh, but both are refreshed up to the end of the last complete period.
            assertPeriodHi("empty_mv", "2022-01-02T05:00:00.000000Z");
            assertPeriodHi("late_mv", "2022-01-02T05:00:00.000000Z");
            assertView("empty_mv", "min_v\tmax_v\tts\n");
            assertView("late_mv", "min_v\tmax_v\tts\n");

            // Another refresh within the same period leaves the recorded end as it is and commits nothing.
            final String walTxnsSql = "select sequencerTxn from wal_transactions('empty_mv')";
            printSql(walTxnsSql);
            final String walTxnsBefore = sink.toString();
            execute("refresh materialized view empty_mv incremental");
            drain(timerJob);
            assertPeriodHi("empty_mv", "2022-01-02T05:00:00.000000Z");
            assertQuery(walTxnsSql)
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns(walTxnsBefore);

            // The refresh of the period that completes at 12:00 starts at the recorded end.
            currentMicros = parseFloorPartialTimestamp("2022-01-02T12:00:00.000000Z");
            drain(timerJob);
            assertPeriodHi("empty_mv", "2022-01-02T12:00:00.000000Z");
            assertPeriodHi("late_mv", "2022-01-02T12:00:00.000000Z");
            assertView("empty_mv", "min_v\tmax_v\tts\n");
            assertView(
                    "late_mv",
                    """
                            min_v\tmax_v\tts
                            1\t1\t2022-01-02T08:00:00.000000Z
                            """
            );
        });
    }

    @Test
    public void testImmediateViewRemovesBucketAfterRangeReplaceDeletesItsRows() throws Exception {
        testRangeReplaceDeletesRowsOfBucketCrossingPeriodHi(
                "create materialized view mv refresh immediate period (length 7h) as " + viewSql("base", "4h")
        );
    }

    @Test
    public void testManualViewRemovesBucketAfterRangeReplaceDeletesItsRows() throws Exception {
        assertMemoryLeak(() -> {
            // The 7h periods end at 05:00, 12:00 and 19:00 on 2022-01-02, and the 4h bucket [04:00, 08:00) crosses 05:00.
            // A manual view has no period timer: its incremental refreshes add the periods that completed since.
            currentMicros = parseFloorPartialTimestamp("2022-01-02T10:00:00.000000Z");
            createBaseTable("base");
            execute("create materialized view mv refresh manual period (length 7h) as " + viewSql("base", "4h"));
            drainWalAndMatViewQueues();

            replace("2022-01-02T00:13", "2022-01-02T17:00", 1, "2022-01-02T07:34", "2022-01-02T09:38");
            refreshIncremental();
            final String withRowsOfFirstReplace = """
                    min_v\tmax_v\tts
                    1\t1\t2022-01-02T04:00:00.000000Z
                    """;
            assertView("mv", withRowsOfFirstReplace);

            replace("2022-01-02T05:24", "2022-01-02T22:12", 2, "2022-01-02T12:45");
            refreshIncremental();
            assertView("mv", withRowsOfFirstReplace);

            // The refresh after the periods that end at 12:00 and 19:00 complete recomputes the 04:00 bucket.
            currentMicros = parseFloorPartialTimestamp("2022-01-02T19:00:00.000000Z");
            refreshIncremental();
            assertView(
                    "mv",
                    """
                            min_v\tmax_v\tts
                            2\t2\t2022-01-02T12:00:00.000000Z
                            """
            );
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, viewSql("base", "4h"), "mv", LOG);
        });
    }

    @Test
    public void testTimeZoneViewRemovesBucketAfterRangeReplaceDeletesItsRows() throws Exception {
        assertMemoryLeak(() -> {
            // Europe/Berlin is at UTC+1 in January, so the 1d periods end at 23:00 UTC, and the 1h delay completes
            // them at 00:00 UTC. At 12:00 UTC the last complete period ends at 2022-01-01T23:00Z, which the 6h bucket
            // [2022-01-01T18:00Z, 2022-01-02T00:00Z) crosses.
            currentMicros = parseFloorPartialTimestamp("2022-01-02T12:00:00.000000Z");
            createBaseTable("base");
            execute(
                    "create materialized view mv refresh immediate period (length 1d time zone 'Europe/Berlin' delay 1h) as " +
                            viewSql("base", "6h")
            );
            final MatViewTimerJob timerJob = new MatViewTimerJob(engine);
            drain(timerJob);

            replace("2022-01-01T19:00", "2022-01-02T06:00", 1, "2022-01-01T23:30");
            drain(timerJob);
            final String withRowOfFirstReplace = """
                    min_v\tmax_v\tts
                    1\t1\t2022-01-01T18:00:00.000000Z
                    """;
            assertView("mv", withRowOfFirstReplace);

            replace("2022-01-01T23:15", "2022-01-02T12:00", 2, "2022-01-02T10:00");
            drain(timerJob);
            assertView("mv", withRowOfFirstReplace);

            currentMicros = parseFloorPartialTimestamp("2022-01-03T00:00:00.000000Z");
            drain(timerJob);
            assertView(
                    "mv",
                    """
                            min_v\tmax_v\tts
                            2\t2\t2022-01-02T06:00:00.000000Z
                            """
            );
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, viewSql("base", "6h"), "mv", LOG);
        });
    }

    private static void drain(MatViewTimerJob timerJob) {
        drainMatViewTimerQueue(timerJob);
        drainWalAndMatViewQueues();
    }

    private static void refreshIncremental() throws SqlException {
        drainWalQueue();
        execute("refresh materialized view mv incremental");
        drainWalAndMatViewQueues();
    }

    private static String viewSql(String baseTableName, String sampleBy) {
        return "select min(v) as min_v, max(v) as max_v, ts from " + baseTableName + " sample by " + sampleBy;
    }

    private void assertPeriodHi(String viewName, String expectedPeriodHi) throws Exception {
        // refresh_period_hi has microsecond precision whatever the base table timestamp type is
        assertQuery("select refresh_period_hi from materialized_views where view_name = '" + viewName + "'")
                .noRandomAccess()
                .noLeakCheck()
                .returns("refresh_period_hi\n" + expectedPeriodHi + "\n");
    }

    private void assertView(String viewName, String expected) throws Exception {
        assertQuery(viewName)
                .timestamp("ts")
                .expectSize()
                .noLeakCheck()
                .returns(ColumnType.isTimestampMicro(timestampType.getTimestampType()) ? expected : expected.replaceAll(".000000Z", ".000000000Z"));
    }

    private void createBaseTable(String tableName) throws SqlException {
        execute("create table " + tableName + " (v int, ts " + timestampType.getTypeName() + ") timestamp(ts) partition by DAY WAL");
    }

    // Commits rows with the value v at the given timestamps, replacing the rows in [lo, hi) of the base table.
    private void replace(String lo, String hi, int v, String... timestamps) {
        final TimestampDriver driver = timestampType.getDriver();
        try (WalWriter walWriter = engine.getWalWriter(engine.verifyTableName("base"))) {
            for (String timestamp : timestamps) {
                final TableWriter.Row row = walWriter.newRow(driver.parseFloorLiteral(timestamp));
                row.putInt(0, v);
                row.append();
            }
            walWriter.commitWithParams(driver.parseFloorLiteral(lo), driver.parseFloorLiteral(hi), WAL_DEDUP_MODE_REPLACE_RANGE);
        }
    }

    // Runs the statements that create the view "mv" over the table "base", then deletes the rows of a bucket that
    // crosses the end of the last complete period before their period completes.
    private void testRangeReplaceDeletesRowsOfBucketCrossingPeriodHi(String... viewStatements) throws Exception {
        assertMemoryLeak(() -> {
            // The 7h periods end at 05:00, 12:00 and 19:00 on 2022-01-02, and the 4h bucket [04:00, 08:00) crosses 05:00.
            currentMicros = parseFloorPartialTimestamp("2022-01-02T10:00:00.000000Z");
            createBaseTable("base");
            final MatViewTimerJob timerJob = new MatViewTimerJob(engine);
            for (String viewStatement : viewStatements) {
                execute(viewStatement);
                drain(timerJob);
            }

            // A range replace that starts in the complete period writes rows in the incomplete one. The refresh stops
            // at 05:00, but the bucket that it recomputes, [04:00, 08:00), takes in the 07:34 row.
            replace("2022-01-02T00:13", "2022-01-02T17:00", 1, "2022-01-02T07:34", "2022-01-02T09:38");
            drain(timerJob);
            final String withRowsOfFirstReplace = """
                    min_v\tmax_v\tts
                    1\t1\t2022-01-02T04:00:00.000000Z
                    """;
            assertView("mv", withRowsOfFirstReplace);
            assertPeriodHi("mv", "2022-01-02T05:00:00.000000Z");

            // A second range replace deletes those rows. It lies in the incomplete period, so the view keeps the
            // bucket until that period completes.
            replace("2022-01-02T05:24", "2022-01-02T22:12", 2, "2022-01-02T12:45");
            drain(timerJob);
            assertView("mv", withRowsOfFirstReplace);

            // The refresh of the period that completes at 12:00 recomputes the bucket, which has no rows left.
            currentMicros = parseFloorPartialTimestamp("2022-01-02T12:00:00.000000Z");
            drain(timerJob);
            assertView("mv", "min_v\tmax_v\tts\n");
            assertPeriodHi("mv", "2022-01-02T12:00:00.000000Z");

            // The period that completes at 19:00 brings the row of the second range replace.
            currentMicros = parseFloorPartialTimestamp("2022-01-02T19:00:00.000000Z");
            drain(timerJob);
            assertView(
                    "mv",
                    """
                            min_v\tmax_v\tts
                            2\t2\t2022-01-02T12:00:00.000000Z
                            """
            );
            assertPeriodHi("mv", "2022-01-02T19:00:00.000000Z");
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, viewSql("base", "4h"), "mv", LOG);
        });
    }
}
