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

package io.questdb.test.griffin;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.MonotonicTimestampFunction;
import io.questdb.griffin.engine.functions.columns.TimestampColumn;
import io.questdb.griffin.engine.functions.constants.StrConstant;
import io.questdb.griffin.engine.functions.date.ToTimezoneTimestampFunctionFactory;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.Interval;
import io.questdb.std.LongList;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.griffin.model.TickExprEngineJobTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * g(ts) IN a TICK literal with a date variable, where g is a chain of monotonic functions of the
 * designated timestamp, resolves the literal when the scan opens and maps its intervals back onto
 * the timestamp. Where every function of the chain maps any intervals back exactly, the scan
 * selects exactly the rows that the predicate holds for, and the query keeps no row filter, so it
 * keeps the paths that need none: count() from the size of the intervals, vectorized aggregates
 * and a plain LIMIT. timestamp_floor(), date_trunc(), timestamp_ceil(), dateadd() by a fixed unit,
 * the addition of a constant, to_timezone() and to_utc() with a fixed offset, to_timezone() with a
 * named zone and a cast that widens the timestamp do; a dateadd() by months or with a zone,
 * to_utc() with a named zone, a timestamp_floor() in a named zone and a cast that narrows the
 * timestamp do not.
 * <p>
 * Each test compares the rows with those that in() selects row by row for the same predicate on
 * ts2, a copy of the timestamp that is not designated.
 */
public class MonotonicTimestampTickInversionTest extends AbstractCairoTest {
    // the times around a change of the clocks that a test runs at, in minutes after the change
    private static final long[] DST_CLOCK_OFFSETS_MINUTES = {-12 * 60, -90, -30, 30, 90, 12 * 60, 24 * 60};
    private static final String[] DST_LITERALS = {
            "$today",
            "$yesterday",
            "$tomorrow",
            "$now",
            "$now - 1h..$now",
            "$now - 3h..$now + 1h",
            "$today..$now",
            "$todayT00:30;3h",
            "$todayT01:30;15m",
            "$todayT02:30;15m",
            "$todayT[01:15,01:45];10m",
            "$todayT23:30;1h",
            "[$yesterday..$tomorrow]T01:30;1h",
            "$today#Sun",
            "$today@Europe/London",
            "$today + 90m..$today + 150m"
    };
    // a zone and a UTC instant at which its clocks change
    private static final String[][] DST_TRANSITIONS = {
            // 01:00 to 02:00, and 02:00 back to 01:00
            {"Europe/London", "2024-03-31T01:00:00.000000Z"},
            {"Europe/London", "2024-10-27T01:00:00.000000Z"},
            {"America/New_York", "2024-03-10T07:00:00.000000Z"},
            {"America/New_York", "2024-11-03T06:00:00.000000Z"},
            // the southern hemisphere, 03:00 back to 02:00 on 2024-04-07, and 02:00 to 03:00 on 2024-10-06
            {"Australia/Sydney", "2024-04-06T16:00:00.000000Z"},
            {"Australia/Sydney", "2024-10-05T16:00:00.000000Z"},
            // half an hour, 02:00 back to 01:30, and 02:00 to 02:30
            {"Australia/Lord_Howe", "2024-04-06T15:00:00.000000Z"},
            {"Australia/Lord_Howe", "2024-10-05T15:30:00.000000Z"},
            // at midnight, 00:00 back to 23:00 the day before, and 00:00 to 01:00
            {"America/Santiago", "2024-04-07T03:00:00.000000Z"},
            {"America/Santiago", "2024-09-08T04:00:00.000000Z"},
            // two hours, 01:00 to 03:00, and 03:00 back to 01:00
            {"Antarctica/Troll", "2024-03-31T01:00:00.000000Z"},
            {"Antarctica/Troll", "2024-10-27T01:00:00.000000Z"},
            // a whole day, 2011-12-30, skipped as the zone moves across the date line
            {"Pacific/Apia", "2011-12-30T10:00:00.000000Z"}
    };
    // '{ts}' stands for the timestamp
    private static final String[] EXACT_CHAINS = {
            "timestamp_floor('h', {ts})",
            "timestamp_floor('15m', {ts})",
            "timestamp_floor('d', {ts})",
            "timestamp_floor('15m', {ts}, '2024-01-01T00:05:00.000000Z')",
            "date_trunc('hour', {ts})",
            "timestamp_ceil('h', {ts})",
            "dateadd('h', 1, {ts})",
            "dateadd('m', -90, {ts})",
            "dateadd('d', -1, {ts})",
            "{ts} + 3_600_000_000",
            "{ts} - 600_000_000",
            "to_timezone({ts}, '+02:00')",
            "to_utc({ts}, '-05:30')",
            "to_timezone({ts}, 'UTC')",
            "to_timezone({ts}, 'Europe/London')",
            "to_timezone({ts}, 'America/New_York')",
            "to_timezone({ts}, 'Asia/Kolkata')",
            "timestamp_floor('h', to_timezone({ts}, 'Europe/Berlin'))",
            "to_timezone(dateadd('h', 1, {ts}), 'America/New_York')",
            "dateadd('m', 30, timestamp_floor('h', {ts}))",
            "to_timezone(timestamp_floor('15m', {ts}), 'Australia/Lord_Howe')"
    };
    private static final String[] EXACT_CHAINS_MICRO = {"{ts}::timestamp", "{ts}::timestamp_ns", "cast({ts} AS timestamp_ns)", "to_timezone({ts}, 'Europe/Paris')::timestamp_ns"};
    private static final String[] EXACT_CHAINS_NANO = {"{ts}::timestamp_ns", "cast({ts} AS timestamp_ns)"};
    private static final String[] INEXACT_CHAINS = {
            "dateadd('M', 1, {ts})",
            "dateadd('y', -1, {ts})",
            "dateadd('h', 1, {ts}, 'Europe/London')",
            "dateadd('h', 1, {ts}, '+02:00')",
            "to_utc({ts}, 'Europe/London')",
            "timestamp_floor('h', {ts}, null, '00:00', 'Europe/London')",
            // maps no interval back at all
            "timestamp_floor('M', {ts}, null, '00:00', 'Europe/London')"
    };
    // at 2024-01-02T10:00Z, a Tuesday
    private static final String[] LITERALS = {
            "$today",
            "$yesterday",
            "$tomorrow",
            "$now",
            "$today..$now",
            "$now - 90m..$now",
            "$yesterday..$tomorrow",
            "$todayT09:30;1h",
            // ends before it starts, so it selects nothing
            "$todayT09:30;0h",
            "$todayT[09:00,14:30];30m",
            "[$yesterday, $tomorrow]",
            "[$yesterday..$tomorrow]T10:00;20m",
            "$today#Tue",
            "$today#Mon",
            "$today@Europe/London",
            "$today@+05:30",
            "$todayT10:00@America/New_York;2h",
            "[$today - 2d..$today]#workday"
    };

    @Test
    public void testDstHeldFactoryFollowsClockChange() throws Exception {
        // A factory compiled on Saturday 2024-10-26 selects, on each run, the rows of the day of that
        // run in London: the 24 hours from 23:00Z the day before on the Saturday, the 25 hours from
        // 23:00Z on Sunday 2024-10-27, when the clocks go back from 02:00 BST to 01:00 GMT, and the
        // 24 hours from 00:00Z on Monday. The table holds a row every ten minutes.
        assertMemoryLeak(() -> {
            createTable("TIMESTAMP", "2024-10-25T00:00:00.000000Z", 720, 600_000_000L);
            setCurrentMicros(parseMicros("2024-10-26T12:00:00.000000Z"));
            try (RecordCursorFactory factory = select("SELECT count(), min(ts), max(ts) FROM t WHERE to_timezone(ts, 'Europe/London') IN '$today'")) {
                final String[][] runs = {
                        {"2024-10-26T12:00:00.000000Z", "count\tmin\tmax\n144\t2024-10-25T23:00:00.000000Z\t2024-10-26T22:50:00.000000Z\n"},
                        {"2024-10-27T12:00:00.000000Z", "count\tmin\tmax\n150\t2024-10-26T23:00:00.000000Z\t2024-10-27T23:50:00.000000Z\n"},
                        {"2024-10-28T12:00:00.000000Z", "count\tmin\tmax\n144\t2024-10-28T00:00:00.000000Z\t2024-10-28T23:50:00.000000Z\n"},
                        {"2024-10-27T12:00:00.000000Z", "count\tmin\tmax\n150\t2024-10-26T23:00:00.000000Z\t2024-10-27T23:50:00.000000Z\n"}
                };
                for (String[] run : runs) {
                    setCurrentMicros(parseMicros(run[0]));
                    assertFactory(factory).withContext(sqlExecutionContext).noRandomAccess().expectSize().returns(run[1]);
                }
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testDstIntervalsDroppedBesideSplit() throws Exception {
        // In 2024, London clocks skip from 01:00 to 02:00 on 2024-03-31, and repeat 01:00 to 02:00
        // on 2024-10-27, 210 days later. The quarter of an hour at 01:30 of the first day selects
        // nothing, and that of the second day both of its occurrences. A row every five minutes on
        // the days around them.
        assertMemoryLeak(() -> {
            createTable("TIMESTAMP", "2024-03-30T00:00:00.000000Z", 864, 300_000_000L);
            final String ts = "('2024-10-26T00:00:00.000000Z'::timestamp + (x - 1) * 300_000_000)";
            execute("INSERT INTO t SELECT " + ts + ", " + ts + ", x FROM long_sequence(864)");
            setCurrentMicros(parseMicros("2024-03-31T12:00:00.000000Z"));
            try {
                assertDstIntervals(
                        "[$today, $today + 210d]T01:30;15m",
                        "[(\"2024-10-27T00:30:00.000000Z\",\"2024-10-27T00:44:59.999999Z\"),(\"2024-10-27T01:30:00.000000Z\",\"2024-10-27T01:44:59.999999Z\")]",
                        "count\tmin\tmax\n6\t2024-10-27T00:30:00.000000Z\t2024-10-27T01:40:00.000000Z\n"
                );
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testDstIntervalsOfFallBack() throws Exception {
        // On Sunday 2024-10-27, London clocks go back from 02:00 BST to 01:00 GMT at 01:00Z, so the
        // local times from 01:00 to 02:00 come twice, first at UTC+01, from 00:00Z, then at UTC+00,
        // from 01:00Z. The local day lasts 25 hours, from 23:00Z the day before. A local interval in
        // the repeated hour selects both of its occurrences, and one that holds the whole hour
        // selects the time between them too. A row every five minutes.
        assertMemoryLeak(() -> {
            createTable("TIMESTAMP", "2024-10-26T00:00:00.000000Z", 864, 300_000_000L);
            setCurrentMicros(parseMicros("2024-10-27T12:00:00.000000Z"));
            try {
                assertDstIntervals(
                        "$today",
                        "[(\"2024-10-26T23:00:00.000000Z\",\"2024-10-27T23:59:59.999999Z\")]",
                        "count\tmin\tmax\n300\t2024-10-26T23:00:00.000000Z\t2024-10-27T23:55:00.000000Z\n"
                );
                assertDstIntervals(
                        "$todayT01:30;15m",
                        "[(\"2024-10-27T00:30:00.000000Z\",\"2024-10-27T00:44:59.999999Z\"),(\"2024-10-27T01:30:00.000000Z\",\"2024-10-27T01:44:59.999999Z\")]",
                        "count\tmin\tmax\n6\t2024-10-27T00:30:00.000000Z\t2024-10-27T01:40:00.000000Z\n"
                );
                assertDstIntervals(
                        "$todayT00:30;3h",
                        "[(\"2024-10-26T23:30:00.000000Z\",\"2024-10-27T03:29:59.999999Z\")]",
                        "count\tmin\tmax\n48\t2024-10-26T23:30:00.000000Z\t2024-10-27T03:25:00.000000Z\n"
                );
                // from the second 01:50 to 02:10, and the first 01:50 to the change
                assertDstIntervals(
                        "$todayT01:50;20m",
                        "[(\"2024-10-27T00:50:00.000000Z\",\"2024-10-27T00:59:59.999999Z\"),(\"2024-10-27T01:50:00.000000Z\",\"2024-10-27T02:09:59.999999Z\")]",
                        "count\tmin\tmax\n6\t2024-10-27T00:50:00.000000Z\t2024-10-27T02:05:00.000000Z\n"
                );
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testDstIntervalsOfSkippedDay() throws Exception {
        // Samoa moved across the date line at 10:00Z on 2011-12-30, from UTC-10 to UTC+14, so its
        // clocks went from the end of 2011-12-29 to the start of 2011-12-31 and no instant has a
        // local time on 2011-12-30. A row every ten minutes.
        assertMemoryLeak(() -> {
            createTable("TIMESTAMP", "2011-12-28T00:00:00.000000Z", 576, 600_000_000L);
            setCurrentMicros(parseMicros("2011-12-30T12:00:00.000000Z"));
            try {
                assertQuery("SELECT count(), min(ts), max(ts) FROM t WHERE to_timezone(ts, 'Pacific/Apia') IN '$today'")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .withPlanContaining("filter: null", "intervals: []")
                        .returns("count\tmin\tmax\n0\t\t\n");
                assertDstIntervals(
                        "Pacific/Apia",
                        "$yesterday",
                        "[(\"2011-12-29T10:00:00.000000Z\",\"2011-12-30T09:59:59.999999Z\")]",
                        "count\tmin\tmax\n144\t2011-12-29T10:00:00.000000Z\t2011-12-30T09:50:00.000000Z\n"
                );
                assertDstIntervals(
                        "Pacific/Apia",
                        "$tomorrow",
                        "[(\"2011-12-30T10:00:00.000000Z\",\"2011-12-31T09:59:59.999999Z\")]",
                        "count\tmin\tmax\n144\t2011-12-30T10:00:00.000000Z\t2011-12-31T09:50:00.000000Z\n"
                );
                assertDstIntervals(
                        "Pacific/Apia",
                        "$yesterday..$tomorrow",
                        "[(\"2011-12-29T10:00:00.000000Z\",\"2011-12-31T09:59:59.999999Z\")]",
                        "count\tmin\tmax\n288\t2011-12-29T10:00:00.000000Z\t2011-12-31T09:50:00.000000Z\n"
                );
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testDstIntervalsOfSpringForward() throws Exception {
        // On Sunday 2024-03-31, London clocks go forward from 01:00 GMT to 02:00 BST at 01:00Z, so no
        // instant has a local time from 01:00 to 02:00. The local day lasts 23 hours. A local
        // interval that starts in the gap starts at the change, one that ends in it ends right
        // before the change, and one in it selects nothing. A row every five minutes.
        assertMemoryLeak(() -> {
            createTable("TIMESTAMP", "2024-03-30T00:00:00.000000Z", 864, 300_000_000L);
            setCurrentMicros(parseMicros("2024-03-31T12:00:00.000000Z"));
            try {
                assertDstIntervals(
                        "$today",
                        "[(\"2024-03-31T00:00:00.000000Z\",\"2024-03-31T22:59:59.999999Z\")]",
                        "count\tmin\tmax\n276\t2024-03-31T00:00:00.000000Z\t2024-03-31T22:55:00.000000Z\n"
                );
                assertDstIntervals(
                        "$todayT00:30;1h",
                        "[(\"2024-03-31T00:30:00.000000Z\",\"2024-03-31T00:59:59.999999Z\")]",
                        "count\tmin\tmax\n6\t2024-03-31T00:30:00.000000Z\t2024-03-31T00:55:00.000000Z\n"
                );
                assertDstIntervals(
                        "$todayT01:30;1h",
                        "[(\"2024-03-31T01:00:00.000000Z\",\"2024-03-31T01:29:59.999999Z\")]",
                        "count\tmin\tmax\n6\t2024-03-31T01:00:00.000000Z\t2024-03-31T01:25:00.000000Z\n"
                );
                assertQuery("SELECT count(), min(ts), max(ts) FROM t WHERE to_timezone(ts, 'Europe/London') IN '$todayT01:30;15m'")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .withPlanContaining("filter: null", "intervals: []")
                        .returns("count\tmin\tmax\n0\t\t\n");
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testDstIntervalsOfYearBetweenFallBacks() throws Exception {
        // From 01:30 on 2023-10-29 to 01:30 on 2024-10-27, London local time, both ends come twice, as
        // the clocks go back from 02:00 to 01:00 on both days, at 01:00Z. The interval holds the first
        // 01:30 of its start, but not the local times before 01:30 that follow it, and the second
        // 01:30 of its end, but not the local times after 01:30 that precede it. 364 days are
        // 524,160 minutes. A row every ten minutes on the days around both changes.
        assertMemoryLeak(() -> {
            createTable("TIMESTAMP", "2023-10-28T00:00:00.000000Z", 432, 600_000_000L);
            final String ts = "('2024-10-26T00:00:00.000000Z'::timestamp + (x - 1) * 600_000_000)";
            execute("INSERT INTO t SELECT " + ts + ", " + ts + ", x FROM long_sequence(432)");
            setCurrentMicros(parseMicros("2023-10-29T12:00:00.000000Z"));
            try {
                // 00:30 to 00:50 on the first day, from its 01:30 on to 00:30 on the last, and 01:00 to
                // 01:30 then
                assertDstIntervals(
                        "$today + 90m..$today + 524250m",
                        "[(\"2023-10-29T00:30:00.000000Z\",\"2023-10-29T00:59:59.999999Z\"),(\"2023-10-29T01:30:00.000000Z\",\"2024-10-27T00:30:00.000000Z\"),(\"2024-10-27T01:00:00.000000Z\",\"2024-10-27T01:30:00.000000Z\")]",
                        "count\tmin\tmax\n434\t2023-10-29T00:30:00.000000Z\t2024-10-27T01:30:00.000000Z\n"
                );
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testDstNamedZonesMatchRowFilter() throws Exception {
        assertDstNamedZonesMatchRowFilter("TIMESTAMP", DST_TRANSITIONS);
    }

    @Test
    public void testDstNamedZonesMatchRowFilterNano() throws Exception {
        assertDstNamedZonesMatchRowFilter(
                "TIMESTAMP_NS",
                new String[][]{
                        {"Europe/London", "2024-10-27T01:00:00.000000Z"},
                        {"Australia/Lord_Howe", "2024-10-05T15:30:00.000000Z"},
                        {"America/Santiago", "2024-04-07T03:00:00.000000Z"},
                        {"Pacific/Apia", "2011-12-30T10:00:00.000000Z"}
                }
        );
    }

    @Test
    public void testEngineJobOverflowSelectsNothing() throws Exception {
        // SQL that the engine runs on its own has no caller to report the overflow of
        // testOverflowAtRunFailsQuery to, so the intervals act as a value that does not parse: the
        // query selects nothing
        assertMemoryLeak(() -> {
            createTable("TIMESTAMP_NS", "2024-01-01T00:00:00.000000Z", 432, 600_000_000L);
            setCurrentMicros(parseMicros("1970-01-02T00:00:00.000000Z"));
            try (SqlExecutionContextImpl engineJob = TickExprEngineJobTest.newEngineJobContext()) {
                engineJob.setNowAndFixClock(parseMicros("1970-01-02T00:00:00.000000Z") * 1000, ColumnType.TIMESTAMP_NANO);
                try (RecordCursorFactory factory = select("SELECT count() FROM t WHERE dateadd('w', -15_000, ts) IN '$today'", engineJob)) {
                    engineJob.setNowAndFixClock(parseMicros("2024-01-02T00:00:00.000000Z") * 1000, ColumnType.TIMESTAMP_NANO);
                    assertFactory(factory).withContext(engineJob).noRandomAccess().expectSize().returns("count\n0\n");
                }
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testExactChainsAcrossTableKinds() throws Exception {
        // the scan selects the same rows from a WAL table, from a table without partitions, and from
        // Parquet partitions
        final String[][] kinds = {
                {"PARTITION BY DAY WAL", null},
                {"PARTITION BY NONE", null},
                {"PARTITION BY DAY", "ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-04'"}
        };
        assertMemoryLeak(() -> {
            setCurrentMicros(parseMicros("2024-01-02T10:00:00.000000Z"));
            try {
                for (String[] kind : kinds) {
                    createTable("TIMESTAMP", "2023-12-31T00:00:00.000000Z", 720, 600_000_000L, kind[0]);
                    drainWalQueue();
                    if (kind[1] != null) {
                        execute(kind[1]);
                    }
                    for (String chain : new String[]{"timestamp_floor('h', {ts})", "to_timezone({ts}, 'Europe/London')", "dateadd('m', -90, {ts})", "{ts}::timestamp_ns"}) {
                        for (String literal : new String[]{"$today", "$now - 90m..$now", "[$yesterday..$tomorrow]T10:00;20m"}) {
                            final String predicate = chain + " IN '" + literal + "'";
                            assertNoRowFilter(predicate);
                            assertSameRowsAsRowFilter(predicate);
                        }
                        Assert.assertEquals(kind[0], 144, rowCount(chain + " IN '$today'"));
                    }
                    execute("DROP TABLE t");
                }
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testExactChainsKeepFastPaths() throws Exception {
        // count() counts the rows of the intervals, sum() runs vectorized and LIMIT reads the first
        // rows of the scan, as none of them needs a row filter. A row every ten minutes from
        // 2023-12-31, v counts the rows from one.
        assertMemoryLeak(() -> {
            createTable("TIMESTAMP", "2023-12-31T00:00:00.000000Z", 720, 600_000_000L);
            setCurrentMicros(parseMicros("2024-01-02T10:00:00.000000Z"));
            try {
                assertQuery("SELECT count() FROM t WHERE timestamp_floor('h', ts) IN '$yesterday'")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .withPlan("""
                                Count
                                    PageFrame
                                        Row forward scan
                                        Interval forward scan on: t
                                          intervals: [("2024-01-01T00:00:00.000000Z","2024-01-01T23:59:59.999999Z")]
                                """)
                        .returns("count\n144\n");
                // the rows of 2024-01-02 are the 289th to the 432nd
                assertQuery("SELECT sum(v) FROM t WHERE to_timezone(ts, 'Europe/London') IN '$today'")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .withPlanContaining("vectorized: true", "intervals: [(\"2024-01-02T00:00:00.000000Z\",\"2024-01-02T23:59:59.999999Z\")]")
                        .withPlanNotContaining("filter: to_timezone")
                        .returns("sum\n51912\n");
                // 23:00 the day before comes an hour before the day
                assertQuery("SELECT * FROM t WHERE dateadd('h', 1, ts) IN '$today' LIMIT 3")
                        .noLeakCheck()
                        .timestamp("ts")
                        .withPlan("""
                                Limit value: 3 skip-rows-max: 0 take-rows-max: 3
                                    PageFrame
                                        Row forward scan
                                        Interval forward scan on: t
                                          intervals: [("2024-01-01T23:00:00.000000Z","2024-01-02T22:59:59.999999Z")]
                                """)
                        .returns("""
                                ts\tts2\tv
                                2024-01-01T23:00:00.000000Z\t2024-01-01T23:00:00.000000Z\t283
                                2024-01-01T23:10:00.000000Z\t2024-01-01T23:10:00.000000Z\t284
                                2024-01-01T23:20:00.000000Z\t2024-01-01T23:20:00.000000Z\t285
                                """);
                // the last rows, scanned backwards
                assertQuery("SELECT * FROM t WHERE dateadd('h', 1, ts) IN '$today' LIMIT -2")
                        .noLeakCheck()
                        .timestamp("ts")
                        .expectSize()
                        .withPlanNotContaining("ilter")
                        .returns("""
                                ts\tts2\tv
                                2024-01-02T22:40:00.000000Z\t2024-01-02T22:40:00.000000Z\t425
                                2024-01-02T22:50:00.000000Z\t2024-01-02T22:50:00.000000Z\t426
                                """);
                // a table without rows
                execute("CREATE TABLE e (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
                assertQuery("SELECT count() FROM e WHERE timestamp_floor('h', ts) IN '$today'")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .withPlanNotContaining("ilter")
                        .returns("count\n0\n");
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testExactChainsMatchRowFilter() throws Exception {
        assertExactChainsMatchRowFilter("TIMESTAMP", EXACT_CHAINS_MICRO);
    }

    @Test
    public void testExactChainsMatchRowFilterNano() throws Exception {
        assertExactChainsMatchRowFilter("TIMESTAMP_NS", EXACT_CHAINS_NANO);
    }

    @Test
    public void testExactInversionInEveryContext() throws Exception {
        // the predicate selects the same rows wherever a query holds it, as the same predicate on
        // the copy of the timestamp does
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t AS (
                        SELECT timestamp_sequence('2024-01-01T00:00:00.000000Z', 600_000_000L) ts,
                               timestamp_sequence('2024-01-01T00:00:00.000000Z', 600_000_000L) ts2,
                               (CASE WHEN x % 3 = 0 THEN 'a' WHEN x % 3 = 1 THEN 'b' ELSE 'c' END)::SYMBOL s,
                               x v
                        FROM long_sequence(432)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("CREATE TABLE ti (ts TIMESTAMP, ts2 TIMESTAMP, s SYMBOL INDEX, v LONG) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO ti SELECT ts, ts2, s, v FROM t");
            setCurrentMicros(parseMicros("2024-01-02T10:00:00.000000Z"));
            try {
                final String[] contexts = {
                        "WITH c AS (SELECT * FROM t WHERE ?) SELECT count(), sum(v) FROM c",
                        "SELECT * FROM (SELECT * FROM t WHERE ?) WHERE v % 2 = 0",
                        "SELECT count() FROM (SELECT * FROM t WHERE ?) a JOIN t b ON a.s = b.s",
                        "SELECT count() FROM t a JOIN (SELECT * FROM t WHERE ?) b ON a.s = b.s",
                        "SELECT count() FROM (SELECT * FROM t WHERE ?) a LEFT JOIN t b ON a.v = b.v",
                        "SELECT a.ts, b.ts FROM (SELECT * FROM t WHERE ?) a ASOF JOIN t b",
                        "SELECT a.ts, b.ts FROM t a ASOF JOIN (SELECT * FROM t WHERE ?) b",
                        "SELECT ts FROM t WHERE ? UNION ALL SELECT ts FROM t WHERE v < 3",
                        "SELECT ts FROM t WHERE ? UNION SELECT ts FROM t WHERE v < 3",
                        "SELECT ts FROM t WHERE ? EXCEPT SELECT ts FROM t WHERE v % 2 = 0",
                        "SELECT ts FROM t WHERE ? INTERSECT SELECT ts FROM t WHERE v % 2 = 0",
                        "SELECT ts, count() FROM t WHERE ? SAMPLE BY 1h",
                        "SELECT s, count(), sum(v) FROM t WHERE ? GROUP BY s ORDER BY s",
                        "SELECT s, ts FROM (SELECT * FROM t WHERE ? LATEST ON ts PARTITION BY s) ORDER BY s",
                        "SELECT ts, v, row_number() OVER (ORDER BY ts) FROM t WHERE ?",
                        "SELECT * FROM t WHERE ? ORDER BY ts DESC LIMIT 4",
                        "SELECT * FROM t WHERE ? LIMIT -3",
                        "SELECT * FROM t WHERE ? LIMIT 2, 5",
                        "SELECT * FROM t WHERE s = 'a' AND ?",
                        "SELECT * FROM ti WHERE s = 'a' AND ?",
                        "SELECT * FROM ti WHERE s IN ('a', 'c') AND ? AND v > 200",
                        "SELECT * FROM t WHERE ? AND v > $1",
                        "SELECT * FROM t WHERE ? AND ts2 IN '$today'"
                };
                final String[] predicates = {
                        "timestamp_floor('h', {ts}) IN '$todayT09:00;2h'",
                        "to_timezone({ts}, 'Europe/London') IN '$today'",
                        "dateadd('m', 30, {ts}) IN '[$yesterday, $today]T[09:00,23:00];1h'"
                };
                bindVariableService.clear();
                bindVariableService.setLong(0, 300);
                for (String context : contexts) {
                    for (String predicate : predicates) {
                        assertSqlCursors(
                                context.replace("?", predicate.replace("{ts}", "ts2")),
                                context.replace("?", predicate.replace("{ts}", "ts"))
                        );
                    }
                }
                // the scan of the symbol, and of its index, reads the intervals with no filter of the
                // predicate
                for (String predicate : predicates) {
                    for (String table : new String[]{"t", "ti"}) {
                        final String sql = "SELECT * FROM " + table + " WHERE s = 'a' AND " + predicate.replace("{ts}", "ts");
                        final CharSequence plan = getPlanSink(sql).getSink();
                        Assert.assertTrue(sql + '\n' + plan, Chars.contains(plan, "intervals: [("));
                        Assert.assertFalse(sql + '\n' + plan, Chars.contains(plan, predicate.substring(0, predicate.indexOf('('))));
                    }
                }
            } finally {
                bindVariableService.clear();
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testInexactChainsKeepRowFilter() throws Exception {
        // a chain that maps intervals back to wider ones, or not at all, keeps the predicate as a
        // row filter that selects the rows
        assertMemoryLeak(() -> {
            setCurrentMicros(parseMicros("2024-01-02T10:00:00.000000Z"));
            try {
                for (String type : new String[]{"TIMESTAMP", "TIMESTAMP_NS"}) {
                    createTable(type, "2023-12-31T00:00:00.000000Z", 720, 600_000_000L);
                    for (String chain : INEXACT_CHAINS) {
                        for (String literal : new String[]{"$today", "$now - 90m..$now", "[$yesterday, $tomorrow]"}) {
                            assertRowFilterKept(chain + " IN '" + literal + "'");
                        }
                    }
                    if ("TIMESTAMP_NS".equals(type)) {
                        // a cast to microseconds loses the nanoseconds that the filter compares
                        assertRowFilterKept("{ts}::timestamp IN '$today'");
                    }
                    execute("DROP TABLE t");
                }
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testInvalidLiteralFailsAsBefore() throws Exception {
        // a literal that does not check out as the query compiles fails the query at the literal,
        // with the error of in(), also where the chain maps intervals back exactly
        assertMemoryLeak(() -> {
            createTable("TIMESTAMP", "2024-01-01T00:00:00.000000Z", 432, 600_000_000L);
            setCurrentMicros(parseMicros("2024-01-02T10:00:00.000000Z"));
            try {
                final String[][] cases = {
                        {"$today;1x", "Invalid duration unit"},
                        {"[$today..$today + 2000d]T09:00", "produces too many intervals (max 1024)"},
                        {"$now - 1h..$now;1h#Tue", "Expected number before unit '#'"}
                };
                for (String[] c : cases) {
                    for (String chain : new String[]{"timestamp_floor('h', ts)", "to_timezone(ts, 'Europe/London')", "ts::timestamp_ns"}) {
                        final String sql = "SELECT * FROM t WHERE " + chain + " IN '" + c[0] + "'";
                        assertExceptionNoLeakCheck(sql, sql.indexOf("'" + c[0]), c[1]);
                    }
                }
                // the table keeps answering
                assertQuery("SELECT count() FROM t WHERE timestamp_floor('h', ts) IN '$today'")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("count\n144\n");
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testNamedZoneNearRangeEndKeepsRowFilter() throws Exception {
        // The conversion of a local time within a day of the ends of the range of a nanosecond
        // timestamp, from 1677-09-21 to 2262-04-11, would overflow, so to_timezone() maps no
        // interval back there and the row filter selects the rows
        assertMemoryLeak(() -> {
            createTable("TIMESTAMP_NS", "2024-01-01T00:00:00.000000Z", 432, 600_000_000L);
            try {
                for (String clock : new String[]{"2262-04-11T00:00:00.000000Z", "1677-09-21T12:00:00.000000Z"}) {
                    setCurrentMicros(parseMicros(clock));
                    assertQuery("SELECT count() FROM t WHERE to_timezone(ts, 'Europe/London') IN '$now'")
                            .noLeakCheck()
                            .noRandomAccess()
                            .expectSize()
                            .withPlanContaining("filter: to_timezone(ts) in [")
                            .returns("count\n0\n");
                }
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testOverflowAtCompileKeepsRowFilter() throws Exception {
        // dateadd() by -15,000 weeks, about 287 years, maps 2024 back past the start of the range of
        // a nanosecond timestamp, so the query keeps the row filter, which selects the rows
        assertMemoryLeak(() -> {
            createTable("TIMESTAMP_NS", "2024-01-01T00:00:00.000000Z", 432, 600_000_000L);
            setCurrentMicros(parseMicros("2024-01-02T10:00:00.000000Z"));
            try {
                assertQuery("SELECT count() FROM t WHERE dateadd('w', -15_000, ts) IN '$today'")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .withPlanContaining("filter: dateadd('w',-15000,ts) in [")
                        .returns("count\n0\n");
                // a shift that stays in range maps back exactly: 1,000 weeks are 7,000 days, so the
                // predicate selects the 144 rows of today
                assertQuery("SELECT count() FROM t WHERE dateadd('w', -1_000, ts) IN '$today - 7000d'")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .withPlanNotContaining("ilter")
                        .returns("count\n144\n");
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testOverflowAtRunFailsQuery() throws Exception {
        // A factory compiled on 1970-01-02 maps that day back by 15,000 weeks within the range of a
        // nanosecond timestamp, so it keeps no row filter. When it runs on 2024-01-02, that day maps
        // back past the start of the range, which the scan cannot narrow to, and without a row
        // filter to select the rows, the query fails at the literal.
        assertMemoryLeak(() -> {
            createTable("TIMESTAMP_NS", "2024-01-01T00:00:00.000000Z", 432, 600_000_000L);
            setCurrentMicros(parseMicros("1970-01-02T00:00:00.000000Z"));
            final String sql = "SELECT count() FROM t WHERE dateadd('w', -15_000, ts) IN '$today'";
            try (RecordCursorFactory factory = select(sql)) {
                Assert.assertFalse(Chars.contains(getPlanSink(sql).getSink(), "ilter"));
                assertFactory(factory).withContext(sqlExecutionContext).noRandomAccess().expectSize().returns("count\n0\n");
                setCurrentMicros(parseMicros("2024-01-02T00:00:00.000000Z"));
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    cursor.hasNext();
                    Assert.fail("expected the query to fail");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "TICK interval is out of range of the timestamp function");
                    Assert.assertEquals(sql.indexOf("'$"), e.getPosition());
                }
                // the factory keeps working on a day that maps back within range
                setCurrentMicros(parseMicros("1970-01-03T00:00:00.000000Z"));
                assertFactory(factory).withContext(sqlExecutionContext).noRandomAccess().expectSize().returns("count\n0\n");
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    /**
     * Creates table t with a row every stepMicros from the given time, the timestamp ts, its copy ts2,
     * which is not designated, and v, which counts the rows from one.
     */
    @Test
    public void testToTimezoneInvertsOpenAndExtremeIntervals() throws Exception {
        // to_timezone() with a named zone maps intervals back with the conversion of a TICK timezone,
        // which keeps an open end open, and declines a finite end within a day of the ends of the
        // range, which would overflow the conversion. The intervals before the start index stay.
        assertMemoryLeak(() -> {
            final ObjList<Function> args = new ObjList<>();
            args.add(TimestampColumn.newInstance(0, ColumnType.TIMESTAMP));
            args.add(new StrConstant("Europe/London"));
            final IntList positions = new IntList();
            positions.add(0);
            positions.add(0);
            try (Function function = new ToTimezoneTimestampFunctionFactory().newInstance(0, args, positions, configuration, sqlExecutionContext)) {
                final MonotonicTimestampFunction toTimezone = (MonotonicTimestampFunction) function;
                final Interval scratch = new Interval();
                Assert.assertTrue(toTimezone.isExactForFiniteIntervals(scratch));
                final long hour = 3_600_000_000L;
                assertInverted(toTimezone, scratch, MonotonicTimestampFunction.EXACT, Long.MIN_VALUE, Long.MAX_VALUE, Long.MIN_VALUE, Long.MAX_VALUE);
                // an open start, and an end at 01:30 on 2024-10-27, which comes at 00:30Z and at 01:30Z
                assertInverted(
                        toTimezone,
                        scratch,
                        MonotonicTimestampFunction.EXACT,
                        Long.MIN_VALUE,
                        parseMicros("2024-10-27T01:30:00.000000Z"),
                        Long.MIN_VALUE,
                        parseMicros("2024-10-27T00:30:00.000000Z"),
                        parseMicros("2024-10-27T01:00:00.000000Z"),
                        parseMicros("2024-10-27T01:30:00.000000Z")
                );
                // a start at 01:30 on 2024-03-31, which the clocks skip from 01:00Z, and an open end
                assertInverted(
                        toTimezone,
                        scratch,
                        MonotonicTimestampFunction.EXACT,
                        parseMicros("2024-03-31T01:30:00.000000Z"),
                        Long.MAX_VALUE,
                        parseMicros("2024-03-31T01:00:00.000000Z"),
                        Long.MAX_VALUE
                );
                assertInverted(toTimezone, scratch, MonotonicTimestampFunction.NONE, Long.MIN_VALUE + 1, Long.MIN_VALUE + 2);
                assertInverted(toTimezone, scratch, MonotonicTimestampFunction.NONE, Long.MIN_VALUE, Long.MIN_VALUE + hour);
                assertInverted(toTimezone, scratch, MonotonicTimestampFunction.NONE, Long.MAX_VALUE - hour, Long.MAX_VALUE);
                assertInverted(toTimezone, scratch, MonotonicTimestampFunction.NONE, Long.MAX_VALUE - 2, Long.MAX_VALUE - 1);
                // after an interval that the call leaves as it is, 2024-01-02 local time is the same in UTC
                final LongList intervals = new LongList();
                intervals.add(1L, 2L);
                intervals.add(parseMicros("2024-01-02T00:00:00.000000Z"), parseMicros("2024-01-02T23:59:59.999999Z"));
                Assert.assertEquals(MonotonicTimestampFunction.EXACT, toTimezone.invertTimestampIntervals(intervals, 2, scratch));
                Assert.assertEquals(4, intervals.size());
                Assert.assertEquals(1, intervals.getQuick(0));
                Assert.assertEquals(2, intervals.getQuick(1));
                Assert.assertEquals(parseMicros("2024-01-02T00:00:00.000000Z"), intervals.getQuick(2));
                Assert.assertEquals(parseMicros("2024-01-02T23:59:59.999999Z"), intervals.getQuick(3));
            }
        });
    }

    private static void assertInverted(MonotonicTimestampFunction function, Interval scratch, int expectedGrade, long lo, long hi, long... expected) {
        final LongList intervals = new LongList();
        intervals.add(lo, hi);
        Assert.assertEquals(expectedGrade, function.invertTimestampIntervals(intervals, 0, scratch));
        if (expectedGrade != MonotonicTimestampFunction.NONE) {
            Assert.assertEquals(expected.length, intervals.size());
            for (int i = 0; i < expected.length; i++) {
                Assert.assertEquals("interval end " + i, expected[i], intervals.getQuick(i));
            }
        }
    }

    private static void createTable(String type, String from, int count, long stepMicros) throws SqlException {
        createTable(type, from, count, stepMicros, "PARTITION BY DAY");
    }

    private static void createTable(String type, String from, int count, long stepMicros, String partitioning) throws SqlException {
        execute("CREATE TABLE t (ts " + type + ", ts2 " + type + ", v LONG) TIMESTAMP(ts) " + partitioning);
        final String ts = "('" + from + "'::timestamp + (x - 1) * " + stepMicros + ")::" + type;
        execute("INSERT INTO t SELECT " + ts + ", " + ts + ", x FROM long_sequence(" + count + ")");
    }

    private static long parseMicros(String iso) {
        try {
            return MicrosTimestampDriver.INSTANCE.parseFloorLiteral(iso);
        } catch (NumericException e) {
            throw new AssertionError("bad timestamp literal: " + iso, e);
        }
    }

    private static long rowCount(String predicate) throws SqlException {
        try (
                RecordCursorFactory factory = select("SELECT count() FROM t WHERE " + predicate.replace("{ts}", "ts2"));
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            Assert.assertTrue(cursor.hasNext());
            return cursor.getRecord().getLong(0);
        }
    }

    private void assertDstIntervals(String literal, String intervals, String expected) throws Exception {
        assertDstIntervals("Europe/London", literal, intervals, expected);
    }

    private void assertDstIntervals(String zone, String literal, String intervals, String expected) throws Exception {
        final String predicate = "to_timezone({ts}, '" + zone + "') IN '" + literal + "'";
        assertQuery("SELECT * FROM t WHERE " + predicate.replace("{ts}", "ts"))
                .noLeakCheck()
                .assertsPlanContaining("intervals: " + intervals);
        assertQuery("SELECT count(), min(ts), max(ts) FROM t WHERE " + predicate.replace("{ts}", "ts"))
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .withPlanContaining("filter: null")
                .returns(expected);
        assertSameRowsAsRowFilter(predicate);
    }

    private void assertDstNamedZonesMatchRowFilter(String type, String[][] transitions) throws Exception {
        assertMemoryLeak(() -> {
            try {
                for (String[] transition : transitions) {
                    final long change = parseMicros(transition[1]);
                    // a row every five minutes from three days before the change to three days after
                    final long from = change - 3 * 86_400_000_000L;
                    sink.clear();
                    MicrosTimestampDriver.INSTANCE.append(sink, from - from % 300_000_000L);
                    createTable(type, sink.toString(), 1_728, 300_000_000L);
                    final String zone = transition[0];
                    final String[] chains = {
                            "to_timezone({ts}, '" + zone + "')",
                            "timestamp_floor('h', to_timezone({ts}, '" + zone + "'))",
                            "to_timezone(dateadd('m', 30, {ts}), '" + zone + "')"
                    };
                    int nonEmpty = 0;
                    for (long offset : DST_CLOCK_OFFSETS_MINUTES) {
                        setCurrentMicros(change + offset * 60_000_000L);
                        for (String chain : chains) {
                            for (String literal : DST_LITERALS) {
                                final String predicate = chain + " IN '" + literal + "'";
                                assertNoRowFilter(predicate);
                                assertSameRowsAsRowFilter(predicate);
                                if (rowCount(predicate) > 0) {
                                    nonEmpty++;
                                }
                            }
                        }
                    }
                    // the day before, of and after the change hold rows at every clock
                    Assert.assertTrue("only " + nonEmpty + " predicates selected rows around " + transition[1] + " in " + zone, nonEmpty > DST_CLOCK_OFFSETS_MINUTES.length * chains.length * 3);
                    execute("DROP TABLE t");
                }
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    private void assertExactChainsMatchRowFilter(String type, String[] castChains) throws Exception {
        assertMemoryLeak(() -> {
            // a row every ten minutes for five days from 2023-12-31
            createTable(type, "2023-12-31T00:00:00.000000Z", 720, 600_000_000L);
            setCurrentMicros(parseMicros("2024-01-02T10:00:00.000000Z"));
            try {
                for (String[] chains : new String[][]{EXACT_CHAINS, castChains}) {
                    for (String chain : chains) {
                        for (String literal : LITERALS) {
                            final String predicate = chain + " IN '" + literal + "'";
                            assertNoRowFilter(predicate);
                            assertSameRowsAsRowFilter(predicate);
                        }
                        // the whole days around the clock hold rows for every chain
                        Assert.assertTrue(chain, rowCount(chain + " IN '$today'") > 0);
                        Assert.assertTrue(chain, rowCount(chain + " IN '[$yesterday, $tomorrow]'") > 0);
                        // no day of the literal is a Monday
                        Assert.assertEquals(chain, 0, rowCount(chain + " IN '$today#Mon'"));
                    }
                }
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    private void assertNoRowFilter(String predicate) throws SqlException {
        final String sql = "SELECT * FROM t WHERE " + predicate.replace("{ts}", "ts");
        final CharSequence plan = getPlanSink(sql).getSink();
        if (Chars.contains(plan, "ilter")) {
            Assert.fail("expected no row filter [sql=" + sql + "]\n" + plan);
        }
    }

    private void assertRowFilterKept(String predicate) throws SqlException {
        final String sql = "SELECT * FROM t WHERE " + predicate.replace("{ts}", "ts");
        final CharSequence plan = getPlanSink(sql).getSink();
        if (!Chars.contains(plan, "ilter")) {
            Assert.fail("expected a row filter [sql=" + sql + "]\n" + plan);
        }
        assertSameRowsAsRowFilter(predicate);
    }

    private void assertSameRowsAsRowFilter(String predicate) throws SqlException {
        final String sql = "SELECT * FROM t WHERE " + predicate.replace("{ts}", "ts");
        try {
            assertSqlCursors("SELECT * FROM t WHERE " + predicate.replace("{ts}", "ts2"), sql);
        } catch (Throwable th) {
            throw new AssertionError("rows differ [sql=" + sql + ", now=" + currentMicros + ']', th);
        }
    }
}
