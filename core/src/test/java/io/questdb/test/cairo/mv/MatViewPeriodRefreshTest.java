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

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.mv.MatViewTimerJob;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.SqlException;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.cairo.wal.WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE;

/**
 * A period materialized view refreshes up to the end of the last complete period and records that end, and the
 * refresh that runs once a period completes starts there. These tests check that every base row reaches the view
 * around that recorded end: the rows of a bucket that crosses it, which a range replace can delete before their
 * period completes, rows that an ALTER of the period moves behind it, rows of the periods around a daylight saving
 * time change, and rows that a refresh which cannot read the WAL transactions has to pick up before it.
 */
@RunWith(Parameterized.class)
public class MatViewPeriodRefreshTest extends AbstractCairoTest {
    private final TestTimestampType timestampType;

    public MatViewPeriodRefreshTest(TestTimestampType timestampType) {
        this.timestampType = timestampType;
    }

    // Commits rows with the value v at the given timestamps to the table "base", replacing its rows in [lo, hi).
    public static void replaceRange(CairoEngine engine, TimestampDriver driver, String lo, String hi, int v, String... timestamps) {
        try (WalWriter walWriter = engine.getWalWriter(engine.verifyTableName("base"))) {
            for (String timestamp : timestamps) {
                final TableWriter.Row row = walWriter.newRow(driver.parseFloorLiteral(timestamp));
                row.putInt(0, v);
                row.append();
            }
            walWriter.commitWithParams(driver.parseFloorLiteral(lo), driver.parseFloorLiteral(hi), WAL_DEDUP_MODE_REPLACE_RANGE);
        }
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<TestTimestampType> testParams() {
        return List.of(TestTimestampType.MICRO, TestTimestampType.NANO);
    }

    @Test
    public void testAlterLengthRefreshesRowsBeforeOldPeriodHi() throws Exception {
        // At 10:30, the last complete 6h period ends at 06:00 and the last complete 1d period at 00:00.
        testAlterMovesPeriodHiEarlier(
                null,
                "length 6h",
                "length 1d",
                "2022-01-02T06:00:00.000000Z",
                "2022-01-02T03:00",
                "2022-01-02T00:00:00.000000Z",
                "2022-01-03T00:00:00.000000Z",
                """
                        min_v\tmax_v\tts
                        1\t1\t2022-01-02T03:00:00.000000Z
                        """
        );
    }

    @Test
    public void testAlterTimeZoneRefreshesRowsBeforeOldPeriodHi() throws Exception {
        // At 10:30, the last complete day ends at 00:00 in UTC, and at 15:00 UTC the day before in Asia/Tokyo.
        testAlterMovesPeriodHiEarlier(
                null,
                "length 1d",
                "length 1d time zone 'Asia/Tokyo'",
                "2022-01-02T00:00:00.000000Z",
                "2022-01-01T16:00",
                "2022-01-01T15:00:00.000000Z",
                "2022-01-02T15:00:00.000000Z",
                """
                        min_v\tmax_v\tts
                        1\t1\t2022-01-01T16:00:00.000000Z
                        """
        );
    }

    @Test
    public void testAlterTimeZoneRefreshesRowsBeforeOldPeriodHiWithBaseRows() throws Exception {
        // The base table has a row in a complete period, so the view records the period end the usual way.
        testAlterMovesPeriodHiEarlier(
                "2021-12-31T12:00",
                "length 1d",
                "length 1d time zone 'Asia/Tokyo'",
                "2022-01-02T00:00:00.000000Z",
                "2022-01-01T16:00",
                "2022-01-01T15:00:00.000000Z",
                "2022-01-02T15:00:00.000000Z",
                """
                        min_v\tmax_v\tts
                        7\t7\t2021-12-31T12:00:00.000000Z
                        1\t1\t2022-01-01T16:00:00.000000Z
                        """
        );
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
    public void testDstEndAheadOfUtcRefreshesRowAtPeriodEnd() throws Exception {
        // DST ends in Europe/Berlin on 2022-10-30 at 01:00Z (03:00 CEST -> 02:00 CET), and the clock passes 02:00-03:00
        // local twice. At 00:30Z, the period [00:00Z, 01:00Z) has not ended yet.
        testRowReachesViewAtPeriodEnd(
                "period (length 1h time zone 'Europe/Berlin')",
                "2022-10-29T21:30",
                "2022-10-30T00:30",
                "2022-10-30T00:10",
                "2022-10-30T00:00",
                "2022-10-30T01:00",
                "2022-10-30T01:00"
        );
    }

    @Test
    public void testDstEndBehindUtcRefreshesLateRow() throws Exception {
        testDstEndRefreshesLateRow("period (length 1h time zone 'America/New_York')", viewSql("base", "1h"));
    }

    @Test
    public void testDstEndBehindUtcRefreshesLateRowWithSampleByInterval() throws Exception {
        // PERIOD (SAMPLE BY INTERVAL) takes the time zone of the SAMPLE BY clause.
        testDstEndRefreshesLateRow(
                "period (sample by interval)",
                viewSql("base", "1h align to calendar time zone 'America/New_York'")
        );
    }

    @Test
    public void testDstEndBehindUtcRefreshesRowAtPeriodEnd() throws Exception {
        // DST ends in America/New_York on 2022-11-06 at 06:00Z (02:00 EDT -> 01:00 EST). The period that holds 09:20Z,
        // [04:00, 05:00) local, ends at 10:00Z.
        testRowReachesViewAtPeriodEnd(
                "period (length 1h time zone 'America/New_York')",
                "2022-11-06T03:30",
                "2022-11-06T09:30",
                "2022-11-06T09:20",
                "2022-11-06T09:00",
                "2022-11-06T10:00",
                "2022-11-06T10:00"
        );
    }

    @Test
    public void testDstStartBehindUtcRefreshesRowAfterDelay() throws Exception {
        // DST starts in America/New_York on 2022-03-13 at 07:00Z (02:00 EST -> 03:00 EDT). The period [06:00Z, 07:00Z)
        // ends at the change, and the 30m delay completes it at 07:30Z.
        testRowReachesViewAtPeriodEnd(
                "period (length 1h time zone 'America/New_York' delay 30m)",
                "2022-03-13T04:30",
                "2022-03-13T07:15",
                "2022-03-13T06:30",
                "2022-03-13T06:00",
                "2022-03-13T07:30",
                "2022-03-13T07:00"
        );
    }

    @Test
    public void testDstStartBehindUtcRefreshesRowAtPeriodEnd() throws Exception {
        // DST starts in America/New_York on 2022-03-13 at 07:00Z (02:00 EST -> 03:00 EDT). At 07:30Z, the period
        // [03:00, 04:00) local, [07:00Z, 08:00Z), has not ended yet.
        testRowReachesViewAtPeriodEnd(
                "period (length 1h time zone 'America/New_York')",
                "2022-03-13T04:30",
                "2022-03-13T07:30",
                "2022-03-13T07:10",
                "2022-03-13T07:00",
                "2022-03-13T08:00",
                "2022-03-13T08:00"
        );
    }

    @Test
    public void testDstStartBehindUtcRefreshesRowAtTwoHourPeriodEnd() throws Exception {
        // DST starts in America/New_York on 2022-03-13 at 07:00Z (02:00 EST -> 03:00 EDT). The clock skips the period
        // start at 02:00 local, so that period starts at the change. The period [04:00, 06:00) local, [08:00Z, 10:00Z),
        // has not ended at 09:00Z.
        testRowReachesViewAtPeriodEnd(
                "period (length 2h time zone 'America/New_York')",
                "2022-03-13T04:30",
                "2022-03-13T08:30",
                "2022-03-13T08:20",
                "2022-03-13T08:00",
                "2022-03-13T10:00",
                "2022-03-13T10:00"
        );
    }

    @Test
    public void testFirstRefreshRecordsPeriodHiWithoutRowsInCompletePeriods() throws Exception {
        assertMemoryLeak(() -> {
            // The 7h periods end at 05:00 and 12:00 on 2022-01-02, so at 10:00 the last complete period ends at 05:00.
            currentMicros = parseFloorPartialTimestamp("2022-01-02T10:00:00.000000Z");
            // The views refresh for the first time at creation, while their base table is empty.
            createBaseTable("empty_base");
            execute("create materialized view empty_mv refresh immediate period (length 7h) as " + viewSql("empty_base", "4h"));
            execute("create materialized view manual_mv refresh manual period (length 7h) as " + viewSql("empty_base", "4h"));
            // The deferred view refreshes for the first time after the first base commit, which writes a row in the
            // incomplete period.
            createBaseTable("late_base");
            execute("create materialized view late_mv refresh immediate deferred period (length 7h) as " + viewSql("late_base", "4h"));
            execute("insert into late_base values (1, '2022-01-02T10:30')");
            final MatViewTimerJob timerJob = new MatViewTimerJob(engine);
            drain(timerJob);

            // No view has rows to refresh, but every refresh records the end of the last complete period. A view over
            // the empty base table has no rows to write, so its first refresh commits only the refresh state.
            assertPeriodHi("empty_mv", "2022-01-02T05:00:00.000000Z");
            assertPeriodHi("manual_mv", "2022-01-02T05:00:00.000000Z");
            assertPeriodHi("late_mv", "2022-01-02T05:00:00.000000Z");
            assertView("empty_mv", "min_v\tmax_v\tts\n");
            assertView("manual_mv", "min_v\tmax_v\tts\n");
            assertView("late_mv", "min_v\tmax_v\tts\n");
            assertWalTxnCount("empty_mv", 1);
            assertWalTxnCount("manual_mv", 1);

            // Another refresh within the same period leaves the recorded end as it is and commits nothing.
            execute("refresh materialized view empty_mv incremental");
            drain(timerJob);
            assertPeriodHi("empty_mv", "2022-01-02T05:00:00.000000Z");
            assertWalTxnCount("empty_mv", 1);

            // The refresh of the period that completes at 12:00 starts at the recorded end. The period timer runs it
            // as a range refresh, which commits twice: it replaces the period's range, then stores the new period end.
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
            assertWalTxnCount("empty_mv", 3);

            // The manual view has no period timer, so the period waits for its next incremental refresh. That refresh
            // commits once: the replaced range and the new period end go into the same transaction.
            assertPeriodHi("manual_mv", "2022-01-02T05:00:00.000000Z");
            assertWalTxnCount("manual_mv", 1);
            execute("refresh materialized view manual_mv incremental");
            drain(timerJob);
            assertPeriodHi("manual_mv", "2022-01-02T12:00:00.000000Z");
            assertView("manual_mv", "min_v\tmax_v\tts\n");
            assertWalTxnCount("manual_mv", 2);
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

            final TimestampDriver driver = timestampType.getDriver();
            replaceRange(engine, driver, "2022-01-02T00:13", "2022-01-02T17:00", 1, "2022-01-02T07:34", "2022-01-02T09:38");
            refreshIncremental();
            final String withRowsOfFirstReplace = """
                    min_v\tmax_v\tts
                    1\t1\t2022-01-02T04:00:00.000000Z
                    """;
            assertView("mv", withRowsOfFirstReplace);

            replaceRange(engine, driver, "2022-01-02T05:24", "2022-01-02T22:12", 2, "2022-01-02T12:45");
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

            final TimestampDriver driver = timestampType.getDriver();
            replaceRange(engine, driver, "2022-01-01T19:00", "2022-01-02T06:00", 1, "2022-01-01T23:30");
            drain(timerJob);
            final String withRowOfFirstReplace = """
                    min_v\tmax_v\tts
                    1\t1\t2022-01-01T18:00:00.000000Z
                    """;
            assertView("mv", withRowOfFirstReplace);

            replaceRange(engine, driver, "2022-01-01T23:15", "2022-01-02T12:00", 2, "2022-01-02T10:00");
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

    @Test
    public void testWalReadFailureRefreshesRowsBeforePeriodHi() throws Exception {
        // The base table is empty when the view records the end of the last complete period.
        testWalReadFailure(
                null,
                """
                        min_v\tmax_v\tts
                        1\t1\t2022-01-02T05:00:00.000000Z
                        """
        );
    }

    @Test
    public void testWalReadFailureRefreshesRowsBeforePeriodHiWithBaseRows() throws Exception {
        // The base table has a row in a complete period, so the view records the period end the usual way.
        testWalReadFailure(
                "2022-01-02T08:30",
                """
                        min_v\tmax_v\tts
                        1\t1\t2022-01-02T05:00:00.000000Z
                        7\t7\t2022-01-02T08:00:00.000000Z
                        """
        );
    }

    private static void drain(MatViewTimerJob timerJob) {
        drainMatViewTimerQueue(timerJob);
        drainWalAndMatViewQueues();
    }

    private static long nextHour(long micros) {
        return (micros / Micros.HOUR_MICROS + 1) * Micros.HOUR_MICROS;
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

    private void assertWalTxnCount(String tableName, int expectedCount) throws Exception {
        assertQuery("select count() from wal_transactions('" + tableName + "')")
                .noLeakCheck()
                .expectSize()
                .noRandomAccess()
                .returns("count\n" + expectedCount + "\n");
    }

    private void createBaseTable(String tableName) throws SqlException {
        execute("create table " + tableName + " (v int, ts " + timestampType.getTypeName() + ") timestamp(ts) partition by DAY WAL");
    }

    // At 10:30, creates the view "mv" over the table "base", empty or with a row at baseRowTs, then alters its period
    // so that the last complete period ends earlier than the recorded end. A row written afterwards between the two
    // ends must reach the view once its period completes, at rowPeriodHi.
    private void testAlterMovesPeriodHiEarlier(
            @Nullable String baseRowTs,
            String createPeriod,
            String alterPeriod,
            String recordedPeriodHi,
            String rowTs,
            String rewoundPeriodHi,
            String rowPeriodHi,
            String expectedView
    ) throws Exception {
        assertMemoryLeak(() -> {
            currentMicros = parseFloorPartialTimestamp("2022-01-02T10:30:00.000000Z");
            createBaseTable("base");
            if (baseRowTs != null) {
                execute("insert into base values (7, '" + baseRowTs + "')");
                drainWalQueue();
            }
            final MatViewTimerJob timerJob = new MatViewTimerJob(engine);
            execute(
                    "create materialized view mv refresh immediate period (" + createPeriod + ") as " + viewSql("base", "1h")
            );
            drain(timerJob);
            assertPeriodHi("mv", recordedPeriodHi);

            execute("alter materialized view mv set refresh immediate period (" + alterPeriod + ")");
            drain(timerJob);
            // The refresh of the row trims it at the new period end, and rewinds the recorded end to that period end.
            execute("insert into base values (1, '" + rowTs + "')");
            drain(timerJob);
            assertPeriodHi("mv", rewoundPeriodHi);

            currentMicros = parseFloorPartialTimestamp(rowPeriodHi);
            drain(timerJob);
            assertPeriodHi("mv", rowPeriodHi);
            assertView("mv", expectedView);
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, viewSql("base", "1h"), "mv", LOG);
        });
    }

    // DST ends in America/New_York on 2022-11-06 at 06:00Z (02:00 EDT -> 01:00 EST), and the clock passes 01:00-02:00
    // local twice. Creates the view "mv" over the table "base" with the given period and query, then checks that a row
    // of the hour that ended at 06:00Z, written at 06:30Z, reaches the view.
    private void testDstEndRefreshesLateRow(String period, String query) throws Exception {
        assertMemoryLeak(() -> {
            currentMicros = parseFloorPartialTimestamp("2022-11-06T03:30:00.000000Z");
            createBaseTable("base");
            execute("insert into base values (7, '2022-11-06T01:10')");
            drainWalQueue();
            final MatViewTimerJob timerJob = new MatViewTimerJob(engine);
            execute("create materialized view mv refresh immediate " + period + " as " + query);
            drain(timerJob);
            for (String hour : new String[]{"04", "05", "06"}) {
                currentMicros = parseFloorPartialTimestamp("2022-11-06T" + hour + ":00:00.000000Z");
                drain(timerJob);
            }
            assertPeriodHi("mv", "2022-11-06T06:00:00.000000Z");

            // The hour that ended at 06:00Z is complete, so the refresh of the late row takes it in.
            currentMicros = parseFloorPartialTimestamp("2022-11-06T06:30:00.000000Z");
            execute("insert into base values (1, '2022-11-06T05:30')");
            drain(timerJob);
            final String withLateRow = """
                    min_v\tmax_v\tts
                    7\t7\t2022-11-06T01:00:00.000000Z
                    1\t1\t2022-11-06T05:00:00.000000Z
                    """;
            assertPeriodHi("mv", "2022-11-06T06:00:00.000000Z");
            assertView("mv", withLateRow);

            currentMicros = parseFloorPartialTimestamp("2022-11-06T07:00:00.000000Z");
            drain(timerJob);
            assertPeriodHi("mv", "2022-11-06T07:00:00.000000Z");
            assertView("mv", withLateRow);
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, query, "mv", LOG);
        });
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
            final TimestampDriver driver = timestampType.getDriver();
            replaceRange(engine, driver, "2022-01-02T00:13", "2022-01-02T17:00", 1, "2022-01-02T07:34", "2022-01-02T09:38");
            drain(timerJob);
            final String withRowsOfFirstReplace = """
                    min_v\tmax_v\tts
                    1\t1\t2022-01-02T04:00:00.000000Z
                    """;
            assertView("mv", withRowsOfFirstReplace);
            assertPeriodHi("mv", "2022-01-02T05:00:00.000000Z");

            // A second range replace deletes those rows. It lies in the incomplete period, so the view keeps the
            // bucket until that period completes.
            replaceRange(engine, driver, "2022-01-02T05:24", "2022-01-02T22:12", 2, "2022-01-02T12:45");
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

    // Creates the view "mv" with the given period over the empty table "base" at createAt, and moves the clock on hour
    // by hour, so that the period timer fires. At writeAt, writes a row at rowTs, which lies in the period that starts
    // at periodLo. Checks that the row stays out of the view until that period completes at completeAt, and that the
    // view then takes it in and records periodHi, the end of that period.
    private void testRowReachesViewAtPeriodEnd(
            String period,
            String createAt,
            String writeAt,
            String rowTs,
            String periodLo,
            String completeAt,
            String periodHi
    ) throws Exception {
        assertMemoryLeak(() -> {
            final String query = viewSql("base", "1h");
            final String empty = "min_v\tmax_v\tts\n";
            currentMicros = parseFloorPartialTimestamp(createAt);
            createBaseTable("base");
            final MatViewTimerJob timerJob = new MatViewTimerJob(engine);
            execute("create materialized view mv refresh immediate " + period + " as " + query);
            drain(timerJob);
            final long writeAtUs = parseFloorPartialTimestamp(writeAt);
            for (long hour = nextHour(currentMicros); hour < writeAtUs; hour += Micros.HOUR_MICROS) {
                currentMicros = hour;
                drain(timerJob);
            }

            currentMicros = writeAtUs;
            execute("insert into base values (1, '" + rowTs + "')");
            drain(timerJob);
            assertPeriodHi("mv", periodLo + ":00.000000Z");
            assertView("mv", empty);

            final long completeAtUs = parseFloorPartialTimestamp(completeAt);
            for (long hour = nextHour(writeAtUs); hour < completeAtUs; hour += Micros.HOUR_MICROS) {
                currentMicros = hour;
                drain(timerJob);
                assertView("mv", empty);
            }
            currentMicros = completeAtUs - 1;
            drain(timerJob);
            assertView("mv", empty);

            currentMicros = completeAtUs;
            drain(timerJob);
            assertPeriodHi("mv", periodHi + ":00.000000Z");
            assertView("mv", empty + "1\t1\t" + rowTs.substring(0, 13) + ":00:00.000000Z\n");
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, query, "mv", LOG);
        });
    }

    // At 10:30, creates the manual view "mv" over the table "base", empty or with a row at baseRowTs, then writes a row
    // from before the recorded period end and refreshes it after two more periods complete, while reading the WAL
    // transactions of "base" fails. The refresh falls back to the full range of the base table and must take that
    // row in.
    private void testWalReadFailure(@Nullable String baseRowTs, String expectedView) throws Exception {
        final AtomicReference<String> failingWalTableDirName = new AtomicReference<>();
        final AtomicInteger failedOpenCount = new AtomicInteger();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                return isFailingWalEventFile(name) ? -1 : super.openRO(name);
            }

            @Override
            public long openRONoCache(LPSZ name) {
                return isFailingWalEventFile(name) ? -1 : super.openRONoCache(name);
            }

            private boolean isFailingWalEventFile(LPSZ name) {
                final String dirName = failingWalTableDirName.get();
                if (dirName != null
                        && Utf8s.containsAscii(name, Files.SEPARATOR + dirName + Files.SEPARATOR + "wal")
                        && Utf8s.endsWithAscii(name, Files.SEPARATOR + "_event")) {
                    failedOpenCount.incrementAndGet();
                    return true;
                }
                return false;
            }
        };
        // As the server does by default, the refresh falls back to the full range instead of failing.
        setProperty(PropertyKey.DEBUG_MAT_VIEW_REFRESH_MISSING_WAL_FILES_FATAL, "false");
        assertMemoryLeak(ff, () -> {
            // The 1h periods end on the hour, so at 10:30 the last complete period ends at 10:00.
            currentMicros = parseFloorPartialTimestamp("2022-01-02T10:30:00.000000Z");
            createBaseTable("base");
            if (baseRowTs != null) {
                execute("insert into base values (7, '" + baseRowTs + "')");
                drainWalQueue();
            }
            execute("create materialized view mv refresh manual period (length 1h) as " + viewSql("base", "1h"));
            drainWalAndMatViewQueues();
            assertPeriodHi("mv", "2022-01-02T10:00:00.000000Z");

            execute("insert into base values (1, '2022-01-02T05:30')");
            drainWalQueue();
            currentMicros = parseFloorPartialTimestamp("2022-01-02T12:10:00.000000Z");
            execute("refresh materialized view mv incremental");
            failingWalTableDirName.set(engine.verifyTableName("base").getDirName());
            drainMatViewQueue(engine);
            failingWalTableDirName.set(null);
            drainWalQueue();
            Assert.assertTrue(failedOpenCount.get() > 0);

            assertPeriodHi("mv", "2022-01-02T12:00:00.000000Z");
            assertView("mv", expectedView);
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, viewSql("base", "1h"), "mv", LOG);
        });
    }
}
