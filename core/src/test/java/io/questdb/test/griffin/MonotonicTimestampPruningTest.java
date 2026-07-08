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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TimestampDriver;
import io.questdb.griffin.SqlException;
import io.questdb.std.Chars;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class MonotonicTimestampPruningTest extends AbstractCairoTest {

    private static final char[] ADD_UNITS = {'s', 'm', 'h', 'd', 'w', 'M', 'y'};
    private static final String[] ANCHORS = {
            "2022-03-27T01:00:00.000000Z", // Europe/London spring-forward gap
            "2022-10-30T01:00:00.000000Z", // Europe/London fall-back overlap
            "2005-03-27T00:00:00.000000Z", // Antarctica/Troll first-ever DST transition
            "2011-12-31T00:00:00.000000Z", // Pacific/Apia date-line skip
            "2021-01-01T00:00:00.000000Z", // year boundary
            "2024-02-29T12:00:00.000000Z", // leap day
            "2008-06-15T08:30:00.000000Z"  // far from any transition
    };
    private static final String[] DATE_TRUNC_UNITS = {"second", "minute", "hour", "day", "week", "month", "quarter", "year"};
    private static final String[] ORIGIN_POOL = {"2020-01-01T00:00:00.000000Z", "2022-01-01T03:00:00.000000Z", "2000-06-15T12:00:00.000000Z"};
    private static final int OUT_LONG = 2;
    private static final int OUT_TS = 0;
    private static final int OUT_YEAR = 1;
    private static final char[] STRIDE_UNITS_ALL = {'U', 'T', 's', 'm', 'h', 'd', 'w', 'M', 'y', 'n'};
    private static final char[] STRIDE_UNITS_TIME = {'U', 'T', 's', 'm', 'h', 'd'};
    private static final String[] TZ_POOL = {
            "Europe/London", "Antarctica/Troll", "Pacific/Apia", "America/New_York",
            "Australia/Lord_Howe", "+00:00", "+02:00", "-05:30", "+05:30"
    };

    @Test
    public void testAddLongConstant() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            // both bounds prune via the interval scan: micros are capped at 9999-12-31, so a one-day
            // shift cannot reach the overflow region and the open-upper inverse stays exact
            assertQuery("SELECT * FROM trades WHERE timestamp + 86_400_000_000 >= '2022-01-03' AND timestamp + 86_400_000_000 < '2022-01-05'")
                    .timestamp("timestamp")
                    .withPlanContaining("Interval forward scan on: trades")
                    .withPlanNotContaining("filter:")
                    .returns("""
                            price\ttimestamp
                            150.0\t2022-01-02T12:00:00.000000Z
                            200.0\t2022-01-03T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testAddLongMicroNearMaxOverflow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tmax (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY YEAR;");
            execute("INSERT INTO tmax VALUES " +
                    "(1, '2022-01-01T00:00:00.000000Z')," +
                    "(2, '9999-12-31T23:59:59.999999Z');");
            // a shift of 9e18 us exceeds the micros gap to Long.MAX, so a stored value near year 9999
            // overflows to a negative value that satisfies the open upper bound; the predicate must keep
            // the filter and must not prune, or the year-9999 row is silently dropped
            assertQuery("SELECT * FROM tmax WHERE ts + 9_000_000_000_000_000_000 < '2022-06-01'")
                    .timestamp("ts")
                    .withPlanContaining("filter:")
                    .withPlanNotContaining("Interval forward scan")
                    .returns("""
                            price\tts
                            2.0\t9999-12-31T23:59:59.999999Z
                            """);
        });
    }

    @Test
    public void testAddLongNanoNearMaxOverflow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tn (price DOUBLE, ts TIMESTAMP_NS) TIMESTAMP(ts) PARTITION BY DAY;");
            execute("INSERT INTO tn VALUES " +
                    "(1, '2024-06-15T12:00:00.000000000Z')," +
                    "(2, '2262-04-11T10:00:00.000000000Z');");
            // ts + 1 day overflows the nano domain for the 2262 row, so its shifted value wraps below
            // the bound; the EXACT inverse must cap the open upper end at MAX - shift, not leave it open
            assertQuery("SELECT * FROM tn WHERE ts + 86_400_000_000_000 >= '2024-01-01'")
                    .timestamp("ts")
                    .withPlanContaining("Interval forward scan on: tn")
                    .withPlanNotContaining("filter:")
                    .returns("""
                            price\tts
                            1.0\t2024-06-15T12:00:00.000000000Z
                            """);
        });
    }

    @Test
    public void testAddLongRuntimeNow() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(1_641_168_000_000_000L); // 2022-01-03T00:00:00Z
            try {
                createTrades();
                assertQuery("SELECT * FROM trades WHERE timestamp + 86_400_000_000 >= now()")
                        .timestamp("timestamp")
                        .withPlanContaining("Interval forward scan on: trades")
                        .returns("""
                                price\ttimestamp
                                150.0\t2022-01-02T12:00:00.000000Z
                                200.0\t2022-01-03T12:00:00.000000Z
                                250.0\t2022-01-04T12:00:00.000000Z
                                """);
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testAddLongRuntimeOverflowBound() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            bindVariableService.clear();
            bindVariableService.setTimestamp(0, -300_000_000_000_000_000L);
            // ts + k >= bound holds for every row; the lower-bound inverse underflows at runtime,
            // so the inverter must impose no interval rather than collapse the result to empty
            assertQuery("SELECT * FROM trades WHERE timestamp + 9_000_000_000_000_000_000 >= $1")
                    .timestamp("timestamp")
                    .withPlan("""
                            Async JIT Filter workers: 1
                              filter: timestamp+9000000000000000000L>=$0::timestamp
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: trades
                                      intervals: [("MIN","MAX")]
                            """)
                    .returns("""
                            price\ttimestamp
                            100.0\t2022-01-01T12:00:00.000000Z
                            150.0\t2022-01-02T12:00:00.000000Z
                            200.0\t2022-01-03T12:00:00.000000Z
                            250.0\t2022-01-04T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testBetweenBothRuntime() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            bindVariableService.clear();
            bindVariableService.setTimestamp(0, 1_641_081_600_000_000L); // 2022-01-02T00:00:00Z
            bindVariableService.setTimestamp(1, 1_641_254_400_000_000L); // 2022-01-04T00:00:00Z
            assertQuery("SELECT * FROM trades WHERE date_trunc('day', timestamp) BETWEEN $1 AND $2")
                    .timestamp("timestamp")
                    .withPlan("""
                            Async Filter workers: 1
                              filter: timestamp_floor('day',timestamp) between $0::timestamp and $1::timestamp
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: trades
                                      intervals: [("2022-01-02T00:00:00.000000Z","2022-01-04T23:59:59.999999Z")]
                            """)
                    .returns("""
                            price\ttimestamp
                            150.0\t2022-01-02T12:00:00.000000Z
                            200.0\t2022-01-03T12:00:00.000000Z
                            250.0\t2022-01-04T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testBetweenConstAndRuntime() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(1_641_297_600_000_000L); // 2022-01-04T12:00:00Z
            try {
                createTrades();
                assertQuery("SELECT * FROM trades WHERE date_trunc('day', timestamp) BETWEEN '2022-01-02' AND now()")
                        .timestamp("timestamp")
                        .withPlanContaining("Interval forward scan on: trades")
                        .returns("""
                                price\ttimestamp
                                150.0\t2022-01-02T12:00:00.000000Z
                                200.0\t2022-01-03T12:00:00.000000Z
                                250.0\t2022-01-04T12:00:00.000000Z
                                """);
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testBetweenReversedConstant() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            // reversed constant bounds are normalized rather than collapsed to empty
            assertQuery("SELECT * FROM trades WHERE date_trunc('day', timestamp) BETWEEN '2022-01-04' AND '2022-01-02'")
                    .timestamp("timestamp")
                    .withPlanContaining("Interval forward scan on: trades")
                    .withPlanNotContaining("filter:")
                    .returns("""
                            price\ttimestamp
                            150.0\t2022-01-02T12:00:00.000000Z
                            200.0\t2022-01-03T12:00:00.000000Z
                            250.0\t2022-01-04T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testBetweenReversedMixed() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            bindVariableService.clear();
            bindVariableService.setTimestamp(0, 1_641_254_400_000_000L); // 2022-01-04T00:00:00Z
            // mixed runtime/constant bounds resolve reversed at scan open: the inverter prunes
            // nothing and the retained row filter returns the normalized range
            assertQuery("SELECT * FROM trades WHERE date_trunc('day', timestamp) BETWEEN $1 AND '2022-01-02'")
                    .timestamp("timestamp")
                    .withPlanContaining("Interval forward scan on: trades")
                    .returns("""
                            price\ttimestamp
                            150.0\t2022-01-02T12:00:00.000000Z
                            200.0\t2022-01-03T12:00:00.000000Z
                            250.0\t2022-01-04T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testBetweenReversedRuntime() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            bindVariableService.clear();
            bindVariableService.setTimestamp(0, 1_641_254_400_000_000L); // 2022-01-04T00:00:00Z
            bindVariableService.setTimestamp(1, 1_641_081_600_000_000L); // 2022-01-02T00:00:00Z
            // both bounds are full runtime values, so the inverter normalizes (swaps) and still prunes
            assertQuery("SELECT * FROM trades WHERE date_trunc('day', timestamp) BETWEEN $1 AND $2")
                    .timestamp("timestamp")
                    .withPlanContaining("Interval forward scan on: trades")
                    .returns("""
                            price\ttimestamp
                            150.0\t2022-01-02T12:00:00.000000Z
                            200.0\t2022-01-03T12:00:00.000000Z
                            250.0\t2022-01-04T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testCastToLongBetween() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            assertQuery("SELECT * FROM trades WHERE timestamp::long BETWEEN 1_641_211_200_000_000 AND 1_641_297_599_999_999")
                    .timestamp("timestamp")
                    .withPlanContaining("Interval forward scan on: trades")
                    .returns("""
                            price\ttimestamp
                            200.0\t2022-01-03T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testCastToLongComparison() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            assertQuery("SELECT * FROM trades WHERE timestamp::long >= 1_641_211_200_000_000")
                    .timestamp("timestamp")
                    .withPlanContaining("Interval forward scan on: trades")
                    .withPlanNotContaining("filter:")
                    .returns("""
                            price\ttimestamp
                            200.0\t2022-01-03T12:00:00.000000Z
                            250.0\t2022-01-04T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testCastToLongDoubleBoundFallsBack() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            // a DOUBLE bound cannot be read via getLong(); the predicate stays a row filter
            // rather than throwing, and the result is the same as a plain long comparison
            assertQuery("SELECT * FROM trades WHERE timestamp::long >= cast(1_641_211_200_000_000 AS double)")
                    .timestamp("timestamp")
                    .withPlanContaining("Frame forward scan on: trades")
                    .returns("""
                            price\ttimestamp
                            200.0\t2022-01-03T12:00:00.000000Z
                            250.0\t2022-01-04T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testCastToLongRuntimeDoubleBoundFallsBack() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            bindVariableService.clear();
            bindVariableService.setDouble(0, 1_641_211_200_000_000.0);
            // a runtime DOUBLE bind cannot be read via getLong() at scan open; it stays a row filter
            assertQuery("SELECT * FROM trades WHERE timestamp::long >= $1")
                    .timestamp("timestamp")
                    .withPlanContaining("Frame forward scan on: trades")
                    .returns("""
                            price\ttimestamp
                            200.0\t2022-01-03T12:00:00.000000Z
                            250.0\t2022-01-04T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testChainOnRightHandSide() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            assertQuery("SELECT * FROM trades WHERE '2022-01-03' <= date_trunc('day', timestamp)")
                    .timestamp("timestamp")
                    .withPlanContaining("Interval forward scan on: trades")
                    .returns("""
                            price\ttimestamp
                            200.0\t2022-01-03T12:00:00.000000Z
                            250.0\t2022-01-04T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testChainedShiftOverflowKeepsFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tmax (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY YEAR;");
            execute("INSERT INTO tmax VALUES " +
                    "(1, '2022-01-01T00:00:00.000000Z')," +
                    "(2, '9999-12-31T23:59:59.999999Z');");
            // each 4.5e18 us shift stays under the single-shift overflow gap, but their sum (9e18)
            // overflows the micros domain for the year-9999 row. Only the innermost shift may use the
            // domain ceiling; the outer shift stays conservative, so the chain keeps its filter and the
            // year-9999 row (whose composed value wraps negative below the bound) is preserved
            assertQuery("SELECT * FROM tmax WHERE dateadd('h', 1250000000, dateadd('h', 1250000000, ts)) < '2022-06-01'")
                    .timestamp("ts")
                    .withPlanContaining("filter:")
                    .withPlanNotContaining("Interval forward scan")
                    .returns("""
                            price\tts
                            2.0\t9999-12-31T23:59:59.999999Z
                            """);
        });
    }

    @Test
    public void testDateAddDayExact() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            assertQuery("SELECT * FROM trades WHERE dateadd('d', 1, timestamp) >= '2022-01-04'")
                    .timestamp("timestamp")
                    .withPlanContaining("Interval forward scan on: trades")
                    .withPlanNotContaining("filter:")
                    .returns("""
                            price\ttimestamp
                            200.0\t2022-01-03T12:00:00.000000Z
                            250.0\t2022-01-04T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testDateAddDayLessThanExact() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            // micros are capped at 9999-12-31, so a one-day shift cannot reach the overflow region:
            // the open-upper comparison inverts exactly and drops the residual filter
            assertQuery("SELECT * FROM trades WHERE dateadd('d', 1, timestamp) < '2022-01-04'")
                    .timestamp("timestamp")
                    .withPlanContaining("Interval forward scan on: trades")
                    .withPlanNotContaining("filter:")
                    .returns("""
                            price\ttimestamp
                            100.0\t2022-01-01T12:00:00.000000Z
                            150.0\t2022-01-02T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testDateAddMonthRuntimeSuperset() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            bindVariableService.clear();
            bindVariableService.setTimestamp(0, 1_643_846_400_000_000L); // 2022-02-03T00:00:00Z
            // SUPERSET + runtime bound: the inverter prunes (widened interval) but the predicate
            // stays a residual filter, so the excluded Jan rows prove the filter is retained
            assertQuery("SELECT * FROM trades WHERE dateadd('M', 1, timestamp) >= $1")
                    .timestamp("timestamp")
                    .withPlanContaining("Interval forward scan on: trades")
                    .returns("""
                            price\ttimestamp
                            200.0\t2022-01-03T12:00:00.000000Z
                            250.0\t2022-01-04T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testDateAddMonthSuperset() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO t VALUES " +
                    "(10, '2022-02-15T00:00:00.000000Z')," +
                    "(20, '2022-02-28T00:00:00.000000Z')," +
                    "(30, '2022-03-01T00:00:00.000000Z')," +
                    "(40, '2022-03-15T00:00:00.000000Z');");
            assertQuery("SELECT * FROM t WHERE dateadd('M', 1, timestamp) >= '2022-03-30'")
                    .timestamp("timestamp")
                    .withPlanContaining("Interval forward scan on: t")
                    .returns("""
                            price\ttimestamp
                            30.0\t2022-03-01T00:00:00.000000Z
                            40.0\t2022-03-15T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testDateAddTimezoneSpringForwardSuperset() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tz (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            // adding 2h in local time across the 2022-03-27T01:00Z spring-forward shifts the rows by
            // different amounts; the constant-shift inverse is invalid, so it must stay SUPERSET
            execute("INSERT INTO tz VALUES " +
                    "(1, '2022-03-27T00:00:00.000000Z')," +
                    "(2, '2022-03-27T03:00:00.000000Z');");
            assertQuery("SELECT * FROM tz WHERE dateadd('h', 2, timestamp, 'Europe/London') >= '2022-03-27T02:00:00.000000Z'")
                    .timestamp("timestamp")
                    .withPlanContaining("filter:")
                    .returns("""
                            price\ttimestamp
                            2.0\t2022-03-27T03:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testDateAddTimezoneSummerExact() throws Exception {
        assertMemoryLeak(() -> {
            createTzSummer();
            // BST is +1h on both ends and July is far from any transition, so dateadd reduces to a
            // constant shift and inverts exactly: the filter is dropped
            assertQuery("SELECT * FROM tz WHERE dateadd('h', 5, timestamp, 'Europe/London') >= '2022-07-15T18:00:00.000000Z'")
                    .timestamp("timestamp")
                    .withPlanContaining("Interval forward scan on: tz")
                    .withPlanNotContaining("filter:")
                    .returns("""
                            price\ttimestamp
                            3.0\t2022-07-15T20:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testDateAddTimezoneWinterExact() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            // January is far from any London transition (offset 0), so the inverse is exact
            assertQuery("SELECT * FROM trades WHERE dateadd('h', 5, timestamp, 'Europe/London') >= '2022-01-03T17:00:00.000000Z'")
                    .timestamp("timestamp")
                    .withPlanContaining("Interval forward scan on: trades")
                    .withPlanNotContaining("filter:")
                    .returns("""
                            price\ttimestamp
                            200.0\t2022-01-03T12:00:00.000000Z
                            250.0\t2022-01-04T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testDateAddYearNegativeStrideNanoOverflow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE yn (price DOUBLE, ts TIMESTAMP_NS) TIMESTAMP(ts) PARTITION BY YEAR;");
            execute("INSERT INTO yn VALUES " +
                    "(1, '2024-06-15T12:00:00.000000000Z')," +
                    "(2, '2260-06-15T12:00:00.000000000Z');");
            // dateadd('y',-5,bound) of '2260' rounds the inverse up past the nano domain max (~2262);
            // the wrapped SUPERSET interval must not prune, so it falls back to a row filter
            assertQuery("SELECT * FROM yn WHERE dateadd('y', -5, ts) <= '2260-01-01'")
                    .timestamp("ts")
                    .withPlanContaining("Frame forward scan on: yn")
                    .returns("""
                            price\tts
                            1.0\t2024-06-15T12:00:00.000000000Z
                            2.0\t2260-06-15T12:00:00.000000000Z
                            """);
        });
    }

    @Test
    public void testDateAddYearSuperset() throws Exception {
        assertMemoryLeak(() -> {
            createYears();
            // calendar add clamps day-of-month, so the year inverse stays SUPERSET (filter kept)
            assertQuery("SELECT * FROM y WHERE dateadd('y', 1, timestamp) >= '2023-01-03'")
                    .timestamp("timestamp")
                    .withPlanContaining("Interval forward scan on: y")
                    .returns("""
                            price\ttimestamp
                            2.0\t2022-06-01T00:00:00.000000Z
                            3.0\t2023-06-01T00:00:00.000000Z
                            4.0\t2024-06-01T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testDateTruncDayBetween() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            assertQuery("SELECT * FROM trades WHERE date_trunc('day', timestamp) BETWEEN '2022-01-02' AND '2022-01-03'")
                    .timestamp("timestamp")
                    .withPlanContaining("Interval forward scan on: trades")
                    .returns("""
                            price\ttimestamp
                            150.0\t2022-01-02T12:00:00.000000Z
                            200.0\t2022-01-03T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testDateTruncDayBoundAtDomainMaxStrict() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tn (price DOUBLE, ts TIMESTAMP_NS) TIMESTAMP(ts) PARTITION BY DAY;");
            execute("INSERT INTO tn VALUES " +
                    "(1, '2024-06-15T12:00:00.000000000Z')," +
                    "(2, '2262-04-10T08:00:00.000000000Z');");
            // a strict bound at the exact nano domain max matches nothing; the strictness +1 must not
            // wrap to the open-lower sentinel and turn the predicate into a full-range scan
            assertQuery("SELECT * FROM tn WHERE date_trunc('day', ts) > 9223372036854775807")
                    .timestamp("ts")
                    .returns("price\tts\n");
        });
    }

    @Test
    public void testDateTruncDayEquals() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            assertQuery("SELECT * FROM trades WHERE date_trunc('day', timestamp) = '2022-01-03'")
                    .timestamp("timestamp")
                    .withPlanContaining("Interval forward scan on: trades")
                    .returns("""
                            price\ttimestamp
                            200.0\t2022-01-03T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testDateTruncDayIn() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            assertQuery("SELECT * FROM trades WHERE date_trunc('day', timestamp) IN '2022-01-03'")
                    .timestamp("timestamp")
                    .withPlanContaining("Interval forward scan on: trades")
                    .returns("""
                            price\ttimestamp
                            200.0\t2022-01-03T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testDateTruncDayInMonth() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            // 'IN <partial date>' is interval membership: a whole month, not a single point
            assertQuery("SELECT * FROM trades WHERE date_trunc('day', timestamp) IN '2022-01'")
                    .timestamp("timestamp")
                    .withPlanContaining("Interval forward scan on: trades")
                    .returns("""
                            price\ttimestamp
                            100.0\t2022-01-01T12:00:00.000000Z
                            150.0\t2022-01-02T12:00:00.000000Z
                            200.0\t2022-01-03T12:00:00.000000Z
                            250.0\t2022-01-04T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testDateTruncDayLower() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            assertQuery("SELECT * FROM trades WHERE date_trunc('day', timestamp) >= '2022-01-03'")
                    .timestamp("timestamp")
                    .withPlanContaining("Interval forward scan on: trades")
                    .returns("""
                            price\ttimestamp
                            200.0\t2022-01-03T12:00:00.000000Z
                            250.0\t2022-01-04T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testDateTruncDayNanoNearMaxOverflow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tn (price DOUBLE, ts TIMESTAMP_NS) TIMESTAMP(ts) PARTITION BY DAY;");
            execute("INSERT INTO tn VALUES " +
                    "(1, '2024-06-15T12:00:00.000000000Z')," +
                    "(2, '2262-04-10T08:00:00.000000000Z');");
            // the round-up of a bound within one day of the nano domain max (~2262-04-11) overflows;
            // both rows satisfy the predicate, so a wrapped interval must not drop them
            assertQuery("SELECT * FROM tn WHERE date_trunc('day', ts) <= '2262-04-11'")
                    .timestamp("ts")
                    .withPlanContaining("Frame forward scan on: tn")
                    .returns("""
                            price\tts
                            1.0\t2024-06-15T12:00:00.000000000Z
                            2.0\t2262-04-10T08:00:00.000000000Z
                            """);
        });
    }

    @Test
    public void testDateTruncDayOverflowLower() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            // a bound a hair below the type maximum overflows the floor inverse; it must fall
            // back to a row filter rather than prune with a wrapped interval
            assertQuery("SELECT * FROM trades WHERE date_trunc('day', timestamp) >= 9223372036854775806")
                    .timestamp("timestamp")
                    .withPlanContaining("Frame forward scan on: trades")
                    .returns("price\ttimestamp\n");
        });
    }

    @Test
    public void testDateTruncDayOverflowUpper() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            // the predicate is true for every row; a wrapped inverse must not silently drop them
            assertQuery("SELECT * FROM trades WHERE date_trunc('day', timestamp) < 9223372036854775806")
                    .timestamp("timestamp")
                    .withPlanContaining("Frame forward scan on: trades")
                    .returns("""
                            price\ttimestamp
                            100.0\t2022-01-01T12:00:00.000000Z
                            150.0\t2022-01-02T12:00:00.000000Z
                            200.0\t2022-01-03T12:00:00.000000Z
                            250.0\t2022-01-04T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testDateTruncMonthOverflowUpper() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            assertQuery("SELECT * FROM trades WHERE date_trunc('month', timestamp) < 9223372036854775806")
                    .timestamp("timestamp")
                    .withPlanContaining("Frame forward scan on: trades")
                    .returns("""
                            price\ttimestamp
                            100.0\t2022-01-01T12:00:00.000000Z
                            150.0\t2022-01-02T12:00:00.000000Z
                            200.0\t2022-01-03T12:00:00.000000Z
                            250.0\t2022-01-04T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testDateTruncDayUpper() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            assertQuery("SELECT * FROM trades WHERE date_trunc('day', timestamp) < '2022-01-03'")
                    .timestamp("timestamp")
                    .withPlanContaining("Interval forward scan on: trades")
                    .returns("""
                            price\ttimestamp
                            100.0\t2022-01-01T12:00:00.000000Z
                            150.0\t2022-01-02T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testDateTruncHourInDay() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            // the IN-string spans a whole day, wider than the hour buckets it floors to
            assertQuery("SELECT * FROM trades WHERE date_trunc('hour', timestamp) IN '2022-01-03'")
                    .timestamp("timestamp")
                    .withPlanContaining("Interval forward scan on: trades")
                    .returns("""
                            price\ttimestamp
                            200.0\t2022-01-03T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testDateTruncHourNonAligned() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            assertQuery("SELECT * FROM trades WHERE date_trunc('hour', timestamp) >= '2022-01-03T12:30:00.000000Z'")
                    .timestamp("timestamp")
                    .withPlanContaining("Interval forward scan on: trades")
                    .returns("""
                            price\ttimestamp
                            250.0\t2022-01-04T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testEqualsNonAlignedEmpty() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            assertQuery("SELECT * FROM trades WHERE date_trunc('day', timestamp) = '2022-01-03T12:00:00.000000Z'")
                    .timestamp("timestamp")
                    .returns("price\ttimestamp\n");
        });
    }

    @Test
    public void testExactInversionDropsFilter() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            // an EXACT inversion captures the predicate fully, so no residual row filter remains
            assertQuery("SELECT * FROM trades WHERE date_trunc('day', timestamp) = '2022-01-03'")
                    .timestamp("timestamp")
                    .withPlanContaining("Interval forward scan on: trades")
                    .withPlanNotContaining("filter:")
                    .returns("""
                            price\ttimestamp
                            200.0\t2022-01-03T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testFloorMicrosecondBoundary() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            // flooring to the column's own resolution is the identity, so the boundary row must match
            assertQuery("SELECT * FROM trades WHERE timestamp_floor('U', timestamp) = '2022-01-03T12:00:00.000000Z'")
                    .timestamp("timestamp")
                    .withPlanContaining("Interval forward scan on: trades")
                    .returns("""
                            price\ttimestamp
                            200.0\t2022-01-03T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testFloorSubMicroStrideFallback() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            // a 500ns stride is finer than the micro column resolution, so the arithmetic
            // inverse would collapse; the inverter must fall back and keep the row filter
            assertQuery("SELECT * FROM trades WHERE timestamp_floor('500n', timestamp) = '2022-01-03T12:00:00.000000Z'")
                    .timestamp("timestamp")
                    .withPlanContaining("Frame forward scan on: trades")
                    .returns("""
                            price\ttimestamp
                            200.0\t2022-01-03T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testFloorTimezoneFixedOffsetExact() throws Exception {
        assertMemoryLeak(() -> {
            createTzSummer();
            // a fixed-offset zone is a constant shift with no DST, so the local-hour floor is exact
            assertQuery("SELECT * FROM tz WHERE timestamp_floor('1h', timestamp, null, '00:00', '+01:00') >= '2022-07-15T13:00:00.000000Z'")
                    .timestamp("timestamp")
                    .withPlanContaining("Interval forward scan on: tz")
                    .withPlanNotContaining("filter:")
                    .returns("""
                            price\ttimestamp
                            2.0\t2022-07-15T12:30:00.000000Z
                            3.0\t2022-07-15T20:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testFloorTimezoneFixedOffsetNanoNearMaxOverflow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tn (price DOUBLE, ts TIMESTAMP_NS) TIMESTAMP(ts) PARTITION BY DAY;");
            execute("INSERT INTO tn VALUES " +
                    "(1, '2024-06-15T12:00:00.000000000Z')," +
                    "(2, '2262-04-11T10:00:00.000000000Z');");
            // rounding the bound up to the next 10h bucket overflows the nano domain max (~2262-04-11);
            // the EXACT floor inverse must detect the wrap and keep a residual filter rather than prune
            // with a wrapped interval that drops both matching rows
            assertQuery("SELECT * FROM tn WHERE timestamp_floor('10h', ts, null, '00:00', '+01:00') <= '2262-04-11T20:00:00.000000000Z'")
                    .timestamp("ts")
                    .withPlanContaining("filter:")
                    .returns("""
                            price\tts
                            1.0\t2024-06-15T12:00:00.000000000Z
                            2.0\t2262-04-11T10:00:00.000000000Z
                            """);
        });
    }

    @Test
    public void testFloorTimezoneSpringForwardSuperset() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tz (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            // the bound's window straddles the 2022-03-27T01:00Z spring-forward, so the local floor
            // is not a constant shift here and the inversion must stay SUPERSET with the filter kept
            execute("INSERT INTO tz VALUES " +
                    "(1, '2022-03-27T00:30:00.000000Z')," +
                    "(2, '2022-03-27T03:30:00.000000Z');");
            assertQuery("SELECT * FROM tz WHERE timestamp_floor('1h', timestamp, null, '00:00', 'Europe/London') >= '2022-03-27T03:00:00.000000Z'")
                    .timestamp("timestamp")
                    .withPlanContaining("filter:")
                    .returns("""
                            price\ttimestamp
                            2.0\t2022-03-27T03:30:00.000000Z
                            """);
        });
    }

    @Test
    public void testFloorTimezoneSummerExact() throws Exception {
        assertMemoryLeak(() -> {
            createTzSummer();
            // BST +1h, far from any transition: the local-hour floor reduces to a constant shift
            assertQuery("SELECT * FROM tz WHERE timestamp_floor('1h', timestamp, null, '00:00', 'Europe/London') >= '2022-07-15T13:00:00.000000Z'")
                    .timestamp("timestamp")
                    .withPlanContaining("Interval forward scan on: tz")
                    .withPlanNotContaining("filter:")
                    .returns("""
                            price\ttimestamp
                            2.0\t2022-07-15T12:30:00.000000Z
                            3.0\t2022-07-15T20:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testFloorTimezoneUtcSuperset() throws Exception {
        assertMemoryLeak(() -> {
            createTzSummer();
            // timestamp_floor_utc (returnUtc) is a different output domain and is never graded EXACT;
            // it must stay SUPERSET with the filter kept even far from any transition
            assertQuery("SELECT * FROM tz WHERE timestamp_floor_utc('1h', timestamp, null, '00:00', 'Europe/London') >= '2022-07-15T12:00:00.000000Z'")
                    .timestamp("timestamp")
                    .withPlanContaining("filter:")
                    .returns("""
                            price\ttimestamp
                            2.0\t2022-07-15T12:30:00.000000Z
                            3.0\t2022-07-15T20:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testFloorTimezoneWinterExact() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            // January is far from any London transition (offset 0), so the inverse is exact
            assertQuery("SELECT * FROM trades WHERE timestamp_floor('1h', timestamp, null, '00:00', 'Europe/London') >= '2022-01-03T13:00:00.000000Z'")
                    .timestamp("timestamp")
                    .withPlanContaining("Interval forward scan on: trades")
                    .withPlanNotContaining("filter:")
                    .returns("""
                            price\ttimestamp
                            250.0\t2022-01-04T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testFloorWithOriginComparison() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            assertQuery("SELECT * FROM trades WHERE timestamp_floor('6h', timestamp, '2022-01-01T03:00:00.000000Z') >= '2022-01-03T10:00:00.000000Z'")
                    .timestamp("timestamp")
                    .withPlanContaining("Interval forward scan on: trades")
                    .returns("""
                            price\ttimestamp
                            250.0\t2022-01-04T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testFloorWithOriginPreOrigin() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            // rows before the origin floor to the origin, so they satisfy a bound at the origin
            // and must not be pruned away
            assertQuery("SELECT * FROM trades WHERE timestamp_floor('6h', timestamp, '2022-01-03T03:00:00.000000Z') >= '2022-01-03T03:00:00.000000Z'")
                    .timestamp("timestamp")
                    .returns("""
                            price\ttimestamp
                            100.0\t2022-01-01T12:00:00.000000Z
                            150.0\t2022-01-02T12:00:00.000000Z
                            200.0\t2022-01-03T12:00:00.000000Z
                            250.0\t2022-01-04T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testFloorWithOffsetOverflow() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            assertQuery("SELECT * FROM trades WHERE timestamp_floor('6h', timestamp, '2020-01-01T03:00:00.000000Z') < 9223372036854775806")
                    .timestamp("timestamp")
                    .withPlanContaining("Frame forward scan on: trades")
                    .returns("""
                            price\ttimestamp
                            100.0\t2022-01-01T12:00:00.000000Z
                            150.0\t2022-01-02T12:00:00.000000Z
                            200.0\t2022-01-03T12:00:00.000000Z
                            250.0\t2022-01-04T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testFloorWithStrideComparison() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            assertQuery("SELECT * FROM trades WHERE timestamp_floor('6h', timestamp) >= '2022-01-03T13:00:00.000000Z'")
                    .timestamp("timestamp")
                    .withPlanContaining("Interval forward scan on: trades")
                    .returns("""
                            price\ttimestamp
                            250.0\t2022-01-04T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testFloorWithStrideOverflow() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            assertQuery("SELECT * FROM trades WHERE timestamp_floor('6h', timestamp) < 9223372036854775806")
                    .timestamp("timestamp")
                    .withPlanContaining("Frame forward scan on: trades")
                    .returns("""
                            price\ttimestamp
                            100.0\t2022-01-01T12:00:00.000000Z
                            150.0\t2022-01-02T12:00:00.000000Z
                            200.0\t2022-01-03T12:00:00.000000Z
                            250.0\t2022-01-04T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testFunctionOnBothSidesFallsBack() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            // a bound that is itself a function of the timestamp cannot be inverted, so the
            // predicate stays a plain row filter (this one is always true)
            assertQuery("SELECT * FROM trades WHERE date_trunc('day', timestamp) <= dateadd('d', 1, timestamp)")
                    .timestamp("timestamp")
                    .withPlanContaining("Frame forward scan on: trades")
                    .returns("""
                            price\ttimestamp
                            100.0\t2022-01-01T12:00:00.000000Z
                            150.0\t2022-01-02T12:00:00.000000Z
                            200.0\t2022-01-03T12:00:00.000000Z
                            250.0\t2022-01-04T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testFuzzDifferentialPushdown() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            final StringSink sink = new StringSink();
            final int tables = 12;
            final int queriesPerTable = 40;
            int sampledPlans = 0;
            int intervalScanHits = 0;
            try {
                for (int t = 0; t < tables; t++) {
                    final boolean nano = rnd.nextBoolean();
                    final TimestampDriver driver = ColumnType.getTimestampDriver(
                            nano ? ColumnType.TIMESTAMP_NANO : ColumnType.TIMESTAMP_MICRO);
                    final long anchorMicros = parseMicros(ANCHORS[rnd.nextInt(ANCHORS.length)]);
                    setCurrentMicros(anchorMicros);
                    final long[] data = createFuzzTable(rnd, nano, anchorMicros, driver, sink);

                    sink.clear();
                    sink.put('\'');
                    driver.append(sink, data[data.length / 2]);
                    sink.put('\'');
                    final String midBound = sink.toString();
                    final String canonPush = "SELECT i, ts FROM x WHERE date_trunc('day', ts) >= " + midBound;
                    assertSqlCursors("SELECT i, ts FROM x WHERE date_trunc('day', ts2) >= " + midBound, canonPush);
                    Assert.assertTrue(
                            "canonical pushdown did not engage the interval scan [nano=" + nano + ']',
                            Chars.contains(getPlanSink(canonPush).getSink(), "Interval forward scan"));

                    for (int q = 0; q < queriesPerTable; q++) {
                        final int wrap = rnd.nextInt(10);
                        final String tsExpr = randomTsExpr(rnd, 2);
                        final String expr;
                        final int outKind;
                        if (wrap == 0) {
                            expr = "year(" + tsExpr + ")";
                            outKind = OUT_YEAR;
                        } else if (wrap == 1) {
                            expr = rnd.nextBoolean() ? "cast(" + tsExpr + " AS long)" : tsExpr + "::long";
                            outKind = OUT_LONG;
                        } else {
                            expr = tsExpr;
                            outKind = OUT_TS;
                        }
                        final String predicate = buildPredicate(rnd, outKind, expr, data, nano, driver, sink);
                        // 'ts' is the designated timestamp (pushed down); 'ts2' an identical copy the optimizer leaves as a row filter
                        final String pushSql = "SELECT i, ts FROM x WHERE " + predicate.replace("@", "ts");
                        final String baseSql = "SELECT i, ts FROM x WHERE " + predicate.replace("@", "ts2");
                        try {
                            assertSqlCursors(baseSql, pushSql);
                        } catch (Throwable th) {
                            throw new AssertionError(
                                    "differential check failed [nano=" + nano + "]\n  push: " + pushSql + "\n  base: " + baseSql, th);
                        }
                        if ((q & 3) == 0) {
                            sampledPlans++;
                            if (Chars.contains(getPlanSink(pushSql).getSink(), "Interval forward scan")) {
                                intervalScanHits++;
                            }
                        }
                    }
                    execute("DROP TABLE x");
                }
            } finally {
                setCurrentMicros(-1);
                bindVariableService.clear();
            }
            LOG.info().$("monotonic pushdown fuzz done [tables=").$(tables)
                    .$(", queriesPerTable=").$(queriesPerTable)
                    .$(", sampledPlans=").$(sampledPlans)
                    .$(", intervalScanHits=").$(intervalScanHits).$(']').$();
            Assert.assertTrue("the fuzz never engaged the timestamp-function pushdown", intervalScanHits > 0);
        });
    }

    @Test
    public void testNanoColumnRuntimeMicroBound() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tn (price DOUBLE, timestamp TIMESTAMP_NS) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO tn VALUES " +
                    "(100, '2022-01-01T12:00:00.000000000Z')," +
                    "(150, '2022-01-02T12:00:00.000000000Z')," +
                    "(200, '2022-01-03T12:00:00.000000000Z')," +
                    "(250, '2022-01-04T12:00:00.000000000Z');");
            bindVariableService.clear();
            bindVariableService.setTimestampWithType(0, ColumnType.TIMESTAMP_MICRO, 1_641_168_000_000_000L); // 2022-01-03T00:00:00Z
            // a micro-typed runtime bound on a nanosecond column is normalized to nanos by the inverter
            assertQuery("SELECT * FROM tn WHERE date_trunc('day', timestamp) >= $1")
                    .timestamp("timestamp")
                    .withPlanContaining("Interval forward scan on: tn")
                    .returns("""
                            price\ttimestamp
                            200.0\t2022-01-03T12:00:00.000000000Z
                            250.0\t2022-01-04T12:00:00.000000000Z
                            """);
        });
    }

    @Test
    public void testNarrowingCastConstant() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades_ns (price DOUBLE, timestamp TIMESTAMP_NS) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades_ns VALUES " +
                    "(100, '2022-01-01T12:00:00.000000Z')," +
                    "(150, '2022-01-02T12:00:00.000000Z')," +
                    "(200, '2022-01-03T12:00:00.000000Z')," +
                    "(250, '2022-01-04T12:00:00.000000Z');");
            assertQuery("SELECT * FROM trades_ns WHERE cast(timestamp AS timestamp) >= '2022-01-03'")
                    .timestamp("timestamp")
                    .withPlanContaining("Interval forward scan on: trades_ns")
                    .returns("""
                            price\ttimestamp
                            200.0\t2022-01-03T12:00:00.000000000Z
                            250.0\t2022-01-04T12:00:00.000000000Z
                            """);
        });
    }

    @Test
    public void testNoopCastBetween() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            assertQuery("SELECT * FROM trades WHERE cast(timestamp AS timestamp) BETWEEN '2022-01-02' AND '2022-01-03T23:59:59.999999Z'")
                    .timestamp("timestamp")
                    .withPlanContaining("Interval forward scan on: trades")
                    .returns("""
                            price\ttimestamp
                            150.0\t2022-01-02T12:00:00.000000Z
                            200.0\t2022-01-03T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testNoopCastComparison() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            assertQuery("SELECT * FROM trades WHERE cast(timestamp AS timestamp) >= '2022-01-02' AND cast(timestamp AS timestamp) < '2022-01-04'")
                    .timestamp("timestamp")
                    .withPlanContaining("Interval forward scan on: trades")
                    .returns("""
                            price\ttimestamp
                            150.0\t2022-01-02T12:00:00.000000Z
                            200.0\t2022-01-03T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testNoopCastEquals() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            assertQuery("SELECT * FROM trades WHERE cast(timestamp AS timestamp) IN '2022-01-03'")
                    .timestamp("timestamp")
                    .withPlanContaining("Interval forward scan on: trades")
                    .returns("""
                            price\ttimestamp
                            200.0\t2022-01-03T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testNoopCastEqualsIsPoint() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            // `=` is a single point for any timestamp expression (the partial-date-as-interval form
            // is IN-only); '2022-01-03' is midnight, which no row sits on -- both the no-op cast and
            // the bare column return nothing
            assertQuery("SELECT * FROM trades WHERE cast(timestamp AS timestamp) = '2022-01-03'")
                    .timestamp("timestamp")
                    .returns("price\ttimestamp\n");
            assertQuery("SELECT * FROM trades WHERE timestamp = '2022-01-03'")
                    .timestamp("timestamp")
                    .returns("price\ttimestamp\n");
            // a full instant matches the point exactly
            assertQuery("SELECT * FROM trades WHERE cast(timestamp AS timestamp) = '2022-01-03T12:00:00.000000Z'")
                    .timestamp("timestamp")
                    .returns("""
                            price\ttimestamp
                            200.0\t2022-01-03T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testNoopCastInToday() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(1_641_211_200_000_000L); // 2022-01-03T12:00:00Z
            try {
                createTrades();
                assertQuery("SELECT * FROM trades WHERE cast(timestamp AS timestamp) IN today()")
                        .timestamp("timestamp")
                        .withPlanContaining("Interval forward scan on: trades")
                        .returns("""
                                price\ttimestamp
                                200.0\t2022-01-03T12:00:00.000000Z
                                """);
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testNoopCastNested() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            assertQuery("SELECT * FROM trades WHERE cast(cast(timestamp AS timestamp) AS timestamp) IN '2022-01-03'")
                    .timestamp("timestamp")
                    .withPlanContaining("Interval forward scan on: trades")
                    .returns("""
                            price\ttimestamp
                            200.0\t2022-01-03T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testNarrowingCastSubMicroPreserved() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades_ns (price DOUBLE, timestamp TIMESTAMP_NS) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades_ns VALUES " +
                    "(100, '2022-01-02T23:59:59.999999500Z')," +
                    "(200, '2022-01-03T00:00:00.000000500Z');");
            // narrowing nano -> micro truncates the sub-microsecond bits, so the inverse is SUPERSET
            // and keeps the filter: the row just below midnight is scanned but filtered out, the one
            // just above is kept
            assertQuery("SELECT * FROM trades_ns WHERE cast(timestamp AS timestamp) >= '2022-01-03'")
                    .timestamp("timestamp")
                    .withPlanContaining("Interval forward scan on: trades_ns")
                    .withPlanContaining("filter:")
                    .returns("""
                            price\ttimestamp
                            200.0\t2022-01-03T00:00:00.000000500Z
                            """);
        });
    }

    @Test
    public void testNotBetweenFallback() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            // a negated BETWEEN is not pushed onto the timestamp axis; it stays a row filter
            assertQuery("SELECT * FROM trades WHERE date_trunc('day', timestamp) NOT BETWEEN '2022-01-02' AND '2022-01-03'")
                    .timestamp("timestamp")
                    .withPlanContaining("Frame forward scan on: trades")
                    .returns("""
                            price\ttimestamp
                            100.0\t2022-01-01T12:00:00.000000Z
                            250.0\t2022-01-04T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testNotEqualsFallback() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            // a negated predicate is not pushed onto the timestamp axis; it stays a row filter
            assertQuery("SELECT * FROM trades WHERE date_trunc('day', timestamp) != '2022-01-03'")
                    .timestamp("timestamp")
                    .withPlanContaining("Frame forward scan on: trades")
                    .returns("""
                            price\ttimestamp
                            100.0\t2022-01-01T12:00:00.000000Z
                            150.0\t2022-01-02T12:00:00.000000Z
                            250.0\t2022-01-04T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testNotInFallback() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            // a negated IN is not pushed onto the timestamp axis; it stays a row filter
            assertQuery("SELECT * FROM trades WHERE date_trunc('day', timestamp) NOT IN '2022-01-03'")
                    .timestamp("timestamp")
                    .withPlanContaining("Frame forward scan on: trades")
                    .returns("""
                            price\ttimestamp
                            100.0\t2022-01-01T12:00:00.000000Z
                            150.0\t2022-01-02T12:00:00.000000Z
                            250.0\t2022-01-04T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testNullBound() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            assertQuery("SELECT * FROM trades WHERE date_trunc('day', timestamp) >= null")
                    .timestamp("timestamp")
                    .returns("price\ttimestamp\n");
        });
    }

    @Test
    public void testNullBoundWithSiblingConjunct() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            // the null bound makes the whole conjunction empty even alongside a matchable sibling
            assertQuery("SELECT * FROM trades WHERE date_trunc('day', timestamp) >= null AND price > 0")
                    .timestamp("timestamp")
                    .returns("price\ttimestamp\n");
        });
    }

    @Test
    public void testPgCastComparison() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            assertQuery("SELECT * FROM trades WHERE timestamp::timestamp >= '2022-01-02' AND timestamp::timestamp < '2022-01-04'")
                    .timestamp("timestamp")
                    .withPlanContaining("Interval forward scan on: trades")
                    .returns("""
                            price\ttimestamp
                            150.0\t2022-01-02T12:00:00.000000Z
                            200.0\t2022-01-03T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testRuntimeBoundWithFalseSibling() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(1_641_168_000_000_000L); // 2022-01-03T00:00:00Z
            try {
                createTrades();
                // the runtime predicate registers a deferred inverter, then the always-false
                // sibling makes the model empty before it is built; the inverter must be freed,
                // not leaked, and the result must be empty
                assertQuery("SELECT * FROM trades WHERE date_trunc('day', timestamp) >= now() AND timestamp::long = null")
                        .timestamp("timestamp")
                        .returns("price\ttimestamp\n");
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testRuntimeNullBoundIsEmpty() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            bindVariableService.clear();
            bindVariableService.setTimestamp(0, Numbers.LONG_NULL);
            assertQuery("SELECT * FROM trades WHERE date_trunc('day', timestamp) >= $1")
                    .timestamp("timestamp")
                    .returns("price\ttimestamp\n");
        });
    }

    @Test
    public void testSubLongConstant() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            assertQuery("SELECT * FROM trades WHERE timestamp - 86_400_000_000 >= '2022-01-03'")
                    .timestamp("timestamp")
                    .withPlanContaining("Interval forward scan on: trades")
                    .returns("""
                            price\ttimestamp
                            250.0\t2022-01-04T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testSubLongRuntimeNow() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(1_641_081_600_000_000L); // 2022-01-02T00:00:00Z
            try {
                createTrades();
                assertQuery("SELECT * FROM trades WHERE timestamp - 86_400_000_000 >= now()")
                        .timestamp("timestamp")
                        .withPlanContaining("Interval forward scan on: trades")
                        .returns("""
                                price\ttimestamp
                                200.0\t2022-01-03T12:00:00.000000Z
                                250.0\t2022-01-04T12:00:00.000000Z
                                """);
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testTimestampCeilDay() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            assertQuery("SELECT * FROM trades WHERE timestamp_ceil('d', timestamp) >= '2022-01-04'")
                    .timestamp("timestamp")
                    .withPlanContaining("Interval forward scan on: trades")
                    .withPlanNotContaining("filter:")
                    .returns("""
                            price\ttimestamp
                            200.0\t2022-01-03T12:00:00.000000Z
                            250.0\t2022-01-04T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testTimestampCeilMonthFallsBack() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            // ceil by month is not epoch-aligned, so the arithmetic inverse cannot apply
            assertQuery("SELECT * FROM trades WHERE timestamp_ceil('M', timestamp) >= '2022-02-01'")
                    .timestamp("timestamp")
                    .withPlanContaining("Frame forward scan on: trades")
                    .returns("""
                            price\ttimestamp
                            100.0\t2022-01-01T12:00:00.000000Z
                            150.0\t2022-01-02T12:00:00.000000Z
                            200.0\t2022-01-03T12:00:00.000000Z
                            250.0\t2022-01-04T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testTimestampCeilHour() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            assertQuery("SELECT * FROM trades WHERE timestamp_ceil('h', timestamp) >= '2022-01-03T13:00:00.000000Z'")
                    .timestamp("timestamp")
                    .withPlanContaining("Interval forward scan on: trades")
                    .withPlanNotContaining("filter:")
                    .returns("""
                            price\ttimestamp
                            200.0\t2022-01-03T12:00:00.000000Z
                            250.0\t2022-01-04T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testTimestampCeilNearMaxBound() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            // ceil rounds the top partial bucket past the domain max, wrapping to a low value that an
            // open lower bound would match, so the predicate cannot prune and stays a row filter
            assertQuery("SELECT * FROM trades WHERE timestamp_ceil('d', timestamp) <= 9223372036854775806")
                    .timestamp("timestamp")
                    .withPlanContaining("Frame forward scan on: trades")
                    .withPlanContaining("filter:")
                    .returns("""
                            price\ttimestamp
                            100.0\t2022-01-01T12:00:00.000000Z
                            150.0\t2022-01-02T12:00:00.000000Z
                            200.0\t2022-01-03T12:00:00.000000Z
                            250.0\t2022-01-04T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testToTimezoneFallBackOverlapSuperset() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tz (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            // London falls back at 2022-10-30T01:00Z: local 01:30 occurs twice (BST then GMT),
            // so both rows map to the same wall-clock value. A wrong EXACT would keep only one;
            // the transition inside the window forces SUPERSET, the filter stays, both rows match
            execute("INSERT INTO tz VALUES " +
                    "(1, '2022-10-30T00:30:00.000000Z')," +
                    "(2, '2022-10-30T01:30:00.000000Z');");
            assertQuery("SELECT * FROM tz WHERE to_timezone(timestamp, 'Europe/London') = '2022-10-30T01:30:00.000000Z'")
                    .timestamp("timestamp")
                    .withPlanContaining("filter:")
                    .returns("""
                            price\ttimestamp
                            1.0\t2022-10-30T00:30:00.000000Z
                            2.0\t2022-10-30T01:30:00.000000Z
                            """);
        });
    }

    @Test
    public void testToTimezoneFirstTransitionSuperset() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tz (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            // Antarctica/Troll's first-ever DST transition is 2005-03-27 (+0 -> +2); the bound's window
            // straddles it, so the offset is not constant and the inversion must stay SUPERSET
            execute("INSERT INTO tz VALUES " +
                    "(1, '2005-03-27T00:30:00.000000Z')," +
                    "(2, '2005-03-27T01:30:00.000000Z');");
            assertQuery("SELECT * FROM tz WHERE to_timezone(timestamp, 'Antarctica/Troll') BETWEEN '2005-03-27T00:00:00.000000Z' AND '2005-03-27T02:00:00.000000Z'")
                    .timestamp("timestamp")
                    .withPlanContaining("filter:")
                    .returns("""
                            price\ttimestamp
                            1.0\t2005-03-27T00:30:00.000000Z
                            """);
        });
    }

    @Test
    public void testToTimezoneFixedOffset() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            assertQuery("SELECT * FROM trades WHERE to_timezone(timestamp, '+02:00') >= '2022-01-03T14:00:00.000000Z'")
                    .timestamp("timestamp")
                    .withPlanContaining("Interval forward scan on: trades")
                    .returns("""
                            price\ttimestamp
                            200.0\t2022-01-03T12:00:00.000000Z
                            250.0\t2022-01-04T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testToTimezoneNamedZoneRuntime() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            bindVariableService.clear();
            bindVariableService.setTimestamp(0, 1_641_211_200_000_000L); // 2022-01-03T12:00:00Z
            // a runtime bound defers inversion to scan open and probes with an open interval, so it
            // stays SUPERSET: the interval prunes while the row filter is kept
            assertQuery("SELECT * FROM trades WHERE to_timezone(timestamp, 'Europe/London') >= $1")
                    .timestamp("timestamp")
                    .withPlanContaining("Interval forward scan on: trades")
                    .withPlanContaining("filter:")
                    .returns("""
                            price\ttimestamp
                            200.0\t2022-01-03T12:00:00.000000Z
                            250.0\t2022-01-04T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testToTimezoneNamedZoneWinterExact() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            // January is far from any London DST transition, so the offset is provably constant
            // (0) across the preimage: the inversion is exact and the row filter is dropped
            assertQuery("SELECT * FROM trades WHERE to_timezone(timestamp, 'Europe/London') >= '2022-01-03T12:00:00.000000Z'")
                    .timestamp("timestamp")
                    .withPlanContaining("Interval forward scan on: trades")
                    .withPlanNotContaining("filter:")
                    .returns("""
                            price\ttimestamp
                            200.0\t2022-01-03T12:00:00.000000Z
                            250.0\t2022-01-04T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testToTimezoneNanoSummerExact() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tzn (price DOUBLE, timestamp TIMESTAMP_NS) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO tzn VALUES " +
                    "(1, '2022-07-15T10:00:00.000000000Z')," +
                    "(2, '2022-07-15T12:30:00.000000000Z')," +
                    "(3, '2022-07-15T20:00:00.000000000Z');");
            // BST is +1h and July is far from any transition, so the inverse is exact
            assertQuery("SELECT * FROM tzn WHERE to_timezone(timestamp, 'Europe/London') >= '2022-07-15T13:30:00.000000000Z'")
                    .timestamp("timestamp")
                    .withPlanContaining("Interval forward scan on: tzn")
                    .withPlanNotContaining("filter:")
                    .returns("""
                            price\ttimestamp
                            2.0\t2022-07-15T12:30:00.000000000Z
                            3.0\t2022-07-15T20:00:00.000000000Z
                            """);
        });
    }

    @Test
    public void testToTimezoneSpringForwardSuperset() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tz (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            // London springs forward at 2022-03-27T01:00Z; the bound's window straddles it, so the
            // offset is not constant and the inversion must stay SUPERSET with the filter retained
            execute("INSERT INTO tz VALUES " +
                    "(1, '2022-03-27T00:30:00.000000Z')," +
                    "(2, '2022-03-27T01:30:00.000000Z');");
            assertQuery("SELECT * FROM tz WHERE to_timezone(timestamp, 'Europe/London') >= '2022-03-27T02:00:00.000000Z'")
                    .timestamp("timestamp")
                    .withPlanContaining("filter:")
                    .returns("""
                            price\ttimestamp
                            2.0\t2022-03-27T01:30:00.000000Z
                            """);
        });
    }

    @Test
    public void testToTimezoneSummerBetweenExact() throws Exception {
        assertMemoryLeak(() -> {
            createTzSummer();
            // BST +1h, far from any transition: two-sided bound inverts exactly, filter dropped
            assertQuery("SELECT * FROM tz WHERE to_timezone(timestamp, 'Europe/London') BETWEEN '2022-07-15T13:00:00.000000Z' AND '2022-07-15T22:00:00.000000Z'")
                    .timestamp("timestamp")
                    .withPlanContaining("Interval forward scan on: tz")
                    .withPlanNotContaining("filter:")
                    .returns("""
                            price\ttimestamp
                            2.0\t2022-07-15T12:30:00.000000Z
                            3.0\t2022-07-15T20:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testToTimezoneSummerEqualsExact() throws Exception {
        assertMemoryLeak(() -> {
            createTzSummer();
            // ts 12:30Z maps to 13:30 BST; the exact inverse pins exactly that row, filter dropped
            assertQuery("SELECT * FROM tz WHERE to_timezone(timestamp, 'Europe/London') = '2022-07-15T13:30:00.000000Z'")
                    .timestamp("timestamp")
                    .withPlanContaining("Interval forward scan on: tz")
                    .withPlanNotContaining("filter:")
                    .returns("""
                            price\ttimestamp
                            2.0\t2022-07-15T12:30:00.000000Z
                            """);
        });
    }

    @Test
    public void testToUtcFixedOffset() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            assertQuery("SELECT * FROM trades WHERE to_utc(timestamp, '+02:00') >= '2022-01-03T10:00:00.000000Z'")
                    .timestamp("timestamp")
                    .withPlanContaining("Interval forward scan on: trades")
                    .returns("""
                            price\ttimestamp
                            200.0\t2022-01-03T12:00:00.000000Z
                            250.0\t2022-01-04T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testToUtcNamedZoneWinterExact() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            // January is far from any London DST transition, so the offset is provably constant
            // (0) across the preimage: the inversion is exact and the row filter is dropped
            assertQuery("SELECT * FROM trades WHERE to_utc(timestamp, 'Europe/London') >= '2022-01-03T12:00:00.000000Z'")
                    .timestamp("timestamp")
                    .withPlanContaining("Interval forward scan on: trades")
                    .withPlanNotContaining("filter:")
                    .returns("""
                            price\ttimestamp
                            200.0\t2022-01-03T12:00:00.000000Z
                            250.0\t2022-01-04T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testToUtcSummerEqualsExact() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tzu (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            // to_utc treats the column as local time; BST 13:30 maps to 12:30Z
            execute("INSERT INTO tzu VALUES " +
                    "(1, '2022-07-15T11:00:00.000000Z')," +
                    "(2, '2022-07-15T13:30:00.000000Z')," +
                    "(3, '2022-07-15T21:00:00.000000Z');");
            assertQuery("SELECT * FROM tzu WHERE to_utc(timestamp, 'Europe/London') = '2022-07-15T12:30:00.000000Z'")
                    .timestamp("timestamp")
                    .withPlanContaining("Interval forward scan on: tzu")
                    .withPlanNotContaining("filter:")
                    .returns("""
                            price\ttimestamp
                            2.0\t2022-07-15T13:30:00.000000Z
                            """);
        });
    }

    @Test
    public void testUnsupportedUnitFallback() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            assertQuery("SELECT * FROM trades WHERE date_trunc('week', timestamp) >= '2022-01-03'")
                    .timestamp("timestamp")
                    .withPlanContaining("Frame forward scan on: trades")
                    .returns("""
                            price\ttimestamp
                            200.0\t2022-01-03T12:00:00.000000Z
                            250.0\t2022-01-04T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testWideningCastConstant() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            assertQuery("SELECT * FROM trades WHERE cast(timestamp AS timestamp_ns) >= '2022-01-03'")
                    .timestamp("timestamp")
                    .withPlanContaining("Interval forward scan on: trades")
                    .returns("""
                            price\ttimestamp
                            200.0\t2022-01-03T12:00:00.000000Z
                            250.0\t2022-01-04T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testWideningCastFiniteUpperExcludesOverflow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tm (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY YEAR;");
            execute("INSERT INTO tm VALUES " +
                    "(1, '2022-06-01T00:00:00.000000Z')," +
                    "(2, '2300-06-01T00:00:00.000000Z');");
            // a finite upper bound below the nano domain stays EXACT: the year-2300 row provably does
            // not satisfy the predicate, so pruning it (and dropping the filter) is correct
            assertQuery("SELECT * FROM tm WHERE cast(timestamp AS timestamp_ns) <= '2261-12-31'")
                    .timestamp("timestamp")
                    .withPlanContaining("Interval forward scan on: tm")
                    .withPlanNotContaining("filter:")
                    .returns("""
                            price\ttimestamp
                            1.0\t2022-06-01T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testWideningCastOpenUpperPreservesOverflowError() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tm (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY YEAR;");
            execute("INSERT INTO tm VALUES " +
                    "(1, '2022-06-01T00:00:00.000000Z')," +
                    "(2, '2300-06-01T00:00:00.000000Z');");
            // an open upper bound stays SUPERSET so the filter is kept: the year-2300 micro value
            // overflows the nano domain when cast, so the query surfaces that error rather than
            // silently pruning the row
            assertExceptionNoLeakCheck(
                    "SELECT * FROM tm WHERE cast(timestamp AS timestamp_ns) >= '2022-01-01'",
                    -1,
                    "inconvertible value"
            );
        });
    }

    @Test
    public void testYearBelowNanoRangeIsEmpty() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE yn (price DOUBLE, timestamp TIMESTAMP_NS) TIMESTAMP(timestamp) PARTITION BY YEAR;");
            execute("INSERT INTO yn VALUES " +
                    "(1, '2021-06-01T00:00:00.000000000Z')," +
                    "(2, '2022-06-01T00:00:00.000000000Z');");
            // a nanosecond column cannot represent any year <= 1676, so the predicate matches
            // nothing -- the bound is below the range, not above it
            assertQuery("SELECT * FROM yn WHERE year(timestamp) <= 1676")
                    .timestamp("timestamp")
                    .returns("price\ttimestamp\n");
        });
    }

    @Test
    public void testYearComparison() throws Exception {
        assertMemoryLeak(() -> {
            createYears();
            assertQuery("SELECT * FROM y WHERE year(timestamp) >= 2023")
                    .timestamp("timestamp")
                    .withPlanContaining("Interval forward scan on: y")
                    .withPlanNotContaining("filter:")
                    .returns("""
                            price\ttimestamp
                            3.0\t2023-06-01T00:00:00.000000Z
                            4.0\t2024-06-01T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testYearDoubleBoundFallsBack() throws Exception {
        assertMemoryLeak(() -> {
            createYears();
            // a DOUBLE bound on year() cannot be read via getLong(); the predicate stays a row
            // filter rather than throwing, and still returns the correct rows
            assertQuery("SELECT * FROM y WHERE year(timestamp) >= cast(2023 AS double)")
                    .timestamp("timestamp")
                    .withPlanContaining("Frame forward scan on: y")
                    .returns("""
                            price\ttimestamp
                            3.0\t2023-06-01T00:00:00.000000Z
                            4.0\t2024-06-01T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testYearEquals() throws Exception {
        assertMemoryLeak(() -> {
            createYears();
            assertQuery("SELECT * FROM y WHERE year(timestamp) = 2023")
                    .timestamp("timestamp")
                    .withPlanContaining("Interval forward scan on: y")
                    .returns("""
                            price\ttimestamp
                            3.0\t2023-06-01T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testYearRuntimeBoundOutOfRange() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(1_641_168_000_000_000L); // 2022-01-03T00:00:00Z
            try {
                createYears();
                assertQuery("SELECT * FROM y WHERE year(timestamp) <= year(now()) + 999999")
                        .timestamp("timestamp")
                        .withPlanContaining("Interval forward scan on: y")
                        .returns("""
                                price\ttimestamp
                                1.0\t2021-06-01T00:00:00.000000Z
                                2.0\t2022-06-01T00:00:00.000000Z
                                3.0\t2023-06-01T00:00:00.000000Z
                                4.0\t2024-06-01T00:00:00.000000Z
                                """);
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    private static String buildPredicate(
            Rnd rnd, int outKind, String expr, long[] data, boolean nano, TimestampDriver driver, StringSink sink
    ) throws SqlException {
        bindVariableService.clear();
        final int[] bindIdx = {0};
        final boolean canIn = outKind == OUT_TS;
        final int roll = rnd.nextInt(100);
        if (canIn && roll >= 80) {
            final int form = rnd.nextInt(10);
            if (form < 6) {
                // a quoted partial date is an interval: g(ts) IN '2022-03' spans the whole month
                return expr + " IN " + randomIntervalString(rnd, data[rnd.nextInt(data.length)], driver, sink);
            }
            if (form < 8) {
                return expr + " IN now()";
            }
            final int i = bindIdx[0]++;
            bindVariableService.setTimestampWithType(
                    i, nano ? ColumnType.TIMESTAMP_NANO : ColumnType.TIMESTAMP_MICRO, data[rnd.nextInt(data.length)]);
            return expr + " IN $" + (i + 1);
        }
        if (roll >= 55) {
            final long b1 = jitterBound(rnd, data[rnd.nextInt(data.length)], nano);
            final long b2 = jitterBound(rnd, data[rnd.nextInt(data.length)], nano);
            final String lo = scalarBound(rnd, outKind, b1, nano, driver, sink, bindIdx);
            final String hi = scalarBound(rnd, outKind, b2, nano, driver, sink, bindIdx);
            // drawn order is kept (not sorted) so reversed bounds exercise normalization
            return expr + " BETWEEN " + lo + " AND " + hi;
        }
        final long b = jitterBound(rnd, data[rnd.nextInt(data.length)], nano);
        final String bound = scalarBound(rnd, outKind, b, nano, driver, sink, bindIdx);
        final String[] ops = {">=", ">", "<=", "<", "="};
        final String[] mirrored = {"<=", "<", ">=", ">", "="};
        final int oi = rnd.nextInt(ops.length);
        return rnd.nextBoolean()
                ? bound + " " + mirrored[oi] + " " + expr
                : expr + " " + ops[oi] + " " + bound;
    }

    private static long[] createFuzzTable(Rnd rnd, boolean nano, long anchorMicros, TimestampDriver driver, StringSink sink) throws Exception {
        final int rows = 40 + rnd.nextInt(40);
        final long anchor = nano ? anchorMicros * 1000 : anchorMicros;
        final long sec = nano ? 1_000_000_000L : 1_000_000L;
        final long[] data = new long[rows];
        for (int i = 0; i < rows; i++) {
            final long off;
            final int r = rnd.nextInt(10);
            if (r < 6) {
                off = (rnd.nextLong(6L * 24 * 3600) - 3L * 24 * 3600) * sec + rnd.nextLong(sec);
            } else if (r < 8) {
                // tightly straddling the anchor transition
                off = (rnd.nextLong(4L * 3600) - 2L * 3600) * sec + rnd.nextLong(sec);
            } else {
                off = (rnd.nextLong(400L * 24 * 3600) - 200L * 24 * 3600) * sec;
            }
            data[i] = anchor + off;
        }
        Arrays.sort(data);
        // designated timestamp must be strictly increasing
        for (int i = 1; i < rows; i++) {
            if (data[i] <= data[i - 1]) {
                data[i] = data[i - 1] + 1;
            }
        }
        final String type = nano ? "TIMESTAMP_NS" : "TIMESTAMP";
        execute("CREATE TABLE x (i INT, ts " + type + ", ts2 " + type + ") TIMESTAMP(ts) PARTITION BY MONTH");
        final StringBuilder insert = new StringBuilder("INSERT INTO x VALUES ");
        for (int i = 0; i < rows; i++) {
            if (i > 0) {
                insert.append(',');
            }
            sink.clear();
            driver.append(sink, data[i]);
            final String iso = sink.toString();
            insert.append('(').append(i).append(",'").append(iso).append("','").append(iso).append("')");
        }
        execute(insert.toString());
        return data;
    }

    private static void createTrades() throws Exception {
        execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
        execute("INSERT INTO trades VALUES " +
                "(100, '2022-01-01T12:00:00.000000Z')," +
                "(150, '2022-01-02T12:00:00.000000Z')," +
                "(200, '2022-01-03T12:00:00.000000Z')," +
                "(250, '2022-01-04T12:00:00.000000Z');");
    }

    private static void createTzSummer() throws Exception {
        execute("CREATE TABLE tz (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
        execute("INSERT INTO tz VALUES " +
                "(1, '2022-07-15T10:00:00.000000Z')," +
                "(2, '2022-07-15T12:30:00.000000Z')," +
                "(3, '2022-07-15T20:00:00.000000Z');");
    }

    private static void createYears() throws Exception {
        execute("CREATE TABLE y (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY YEAR;");
        execute("INSERT INTO y VALUES " +
                "(1, '2021-06-01T00:00:00.000000Z')," +
                "(2, '2022-06-01T00:00:00.000000Z')," +
                "(3, '2023-06-01T00:00:00.000000Z')," +
                "(4, '2024-06-01T00:00:00.000000Z');");
    }

    private static long jitterBound(Rnd rnd, long base, boolean nano) {
        final long sec = nano ? 1_000_000_000L : 1_000_000L;
        return switch (rnd.nextInt(10)) {
            case 0 -> base;
            case 1 -> base + 1;
            case 2 -> base - 1;
            case 3 -> base + sec;
            case 4 -> base + 60 * sec;
            case 5 -> base + 3600 * sec;
            case 6 -> base + 24L * 3600 * sec;
            case 7 -> base + (rnd.nextLong(7L * 24 * 3600) - 3L * 24 * 3600) * sec;
            // within ~1 year of the nano domain max, where the EXACT round-up inverse can wrap past
            // Long.MAX; micro's overflow point lies beyond any literal, so only nano probes the edge
            case 8 -> nano ? Long.MAX_VALUE - rnd.nextLong(366L * 24 * 3600) * sec - rnd.nextLong(sec)
                    : base + (rnd.nextLong(400L * 24 * 3600) - 200L * 24 * 3600) * sec;
            default -> base + (rnd.nextLong(400L * 24 * 3600) - 200L * 24 * 3600) * sec; // far, often empty
        };
    }

    private static long parseMicros(String iso) {
        try {
            return MicrosTimestampDriver.INSTANCE.parseFloorLiteral(iso);
        } catch (NumericException e) {
            throw new AssertionError("bad anchor literal: " + iso, e);
        }
    }

    private static String randomIntervalString(Rnd rnd, long value, TimestampDriver driver, StringSink sink) {
        sink.clear();
        driver.append(sink, value);
        final String full = sink.toString();
        // cut the ISO string to year/month/day/hour/minute/second granularity
        final int[] cuts = {4, 7, 10, 13, 16, 19};
        final int cut = Math.min(cuts[rnd.nextInt(cuts.length)], full.length());
        return "'" + full.substring(0, cut) + "'";
    }

    private static String randomStride(Rnd rnd, char[] units) {
        return (1 + rnd.nextInt(12)) + String.valueOf(units[rnd.nextInt(units.length)]);
    }

    private static String randomTsExpr(Rnd rnd, int depth) {
        if (depth <= 0 || rnd.nextInt(100) < 35) {
            return "@";
        }
        final String inner = randomTsExpr(rnd, depth - 1);
        return switch (rnd.nextInt(10)) {
            case 0 -> "date_trunc('" + DATE_TRUNC_UNITS[rnd.nextInt(DATE_TRUNC_UNITS.length)] + "', " + inner + ")";
            case 1 -> "timestamp_floor('" + randomStride(rnd, STRIDE_UNITS_ALL) + "', " + inner + ")";
            case 2 -> "timestamp_floor('" + randomStride(rnd, STRIDE_UNITS_TIME) + "', " + inner
                    + ", '" + ORIGIN_POOL[rnd.nextInt(ORIGIN_POOL.length)] + "')";
            case 3 -> (rnd.nextInt(4) == 0 ? "timestamp_floor_utc('" : "timestamp_floor('")
                    + randomStride(rnd, STRIDE_UNITS_TIME) + "', " + inner
                    + ", null, '00:00', '" + TZ_POOL[rnd.nextInt(TZ_POOL.length)] + "')";
            // timestamp_ceil takes a bare unit char, not a stride multiplier
            case 4 -> "timestamp_ceil('" + STRIDE_UNITS_ALL[rnd.nextInt(STRIDE_UNITS_ALL.length)] + "', " + inner + ")";
            case 5 ->
                    "dateadd('" + ADD_UNITS[rnd.nextInt(ADD_UNITS.length)] + "', " + (rnd.nextInt(73) - 36) + ", " + inner + ")";
            case 6 -> "dateadd('" + ADD_UNITS[rnd.nextInt(ADD_UNITS.length)] + "', " + (rnd.nextInt(49) - 24)
                    + ", " + inner + ", '" + TZ_POOL[rnd.nextInt(TZ_POOL.length)] + "')";
            case 7 ->
                    (rnd.nextBoolean() ? "to_timezone(" : "to_utc(") + inner + ", '" + TZ_POOL[rnd.nextInt(TZ_POOL.length)] + "')";
            case 8 -> {
                final long shift = (1L + rnd.nextInt(1_000_000)) * 1_000_000L;
                yield "(" + inner + (rnd.nextBoolean() ? " + " : " - ") + shift + ")";
            }
            default -> {
                final String castType = rnd.nextBoolean() ? "timestamp" : "timestamp_ns";
                yield rnd.nextBoolean() ? "cast(" + inner + " AS " + castType + ")" : inner + "::" + castType;
            }
        };
    }

    private static String scalarBound(
            Rnd rnd, int outKind, long value, boolean nano, TimestampDriver driver, StringSink sink, int[] bindIdx
    ) throws SqlException {
        final int kind = rnd.nextInt(100);
        if (outKind == OUT_TS && kind < 8) {
            return "null";
        }
        if (outKind == OUT_TS && kind < 18) {
            return "now()";
        }
        if (kind < 33) {
            final int i = bindIdx[0]++;
            if (outKind == OUT_TS && kind < 22) {
                bindVariableService.setTimestampWithType(
                        i, nano ? ColumnType.TIMESTAMP_NANO : ColumnType.TIMESTAMP_MICRO, Numbers.LONG_NULL);
            } else if (outKind == OUT_TS) {
                bindVariableService.setTimestampWithType(
                        i, nano ? ColumnType.TIMESTAMP_NANO : ColumnType.TIMESTAMP_MICRO, value);
            } else if (outKind == OUT_YEAR) {
                bindVariableService.setLong(i, driver.getYear(value));
            } else {
                bindVariableService.setLong(i, value);
            }
            return "$" + (i + 1);
        }
        if (outKind == OUT_TS) {
            sink.clear();
            sink.put('\'');
            driver.append(sink, value);
            sink.put('\'');
            return sink.toString();
        }
        if (outKind == OUT_YEAR) {
            return Integer.toString(driver.getYear(value));
        }
        return Long.toString(value);
    }
}
