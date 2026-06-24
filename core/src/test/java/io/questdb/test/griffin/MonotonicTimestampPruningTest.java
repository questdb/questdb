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

import io.questdb.std.Numbers;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class MonotonicTimestampPruningTest extends AbstractCairoTest {

    @Test
    public void testAddLongConstant() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
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
    public void testDateAddTimezoneSuperset() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            assertQuery("SELECT * FROM trades WHERE dateadd('h', 5, timestamp, 'Europe/London') >= '2022-01-03T17:00:00.000000Z'")
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
    public void testFloorTimezoneSuperset() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            assertQuery("SELECT * FROM trades WHERE timestamp_floor('1h', timestamp, null, '00:00', 'Europe/London') >= '2022-01-03T13:00:00.000000Z'")
                    .timestamp("timestamp")
                    .withPlanContaining("Interval forward scan on: trades")
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
}
