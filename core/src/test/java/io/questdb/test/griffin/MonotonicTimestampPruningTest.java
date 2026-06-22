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
    public void testNullBound() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            assertQuery("SELECT * FROM trades WHERE date_trunc('day', timestamp) >= null")
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
    public void testToTimezoneNamedZoneSuperset() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            assertQuery("SELECT * FROM trades WHERE to_timezone(timestamp, 'Europe/London') >= '2022-01-03T12:00:00.000000Z'")
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
    public void testToUtcNamedZoneSuperset() throws Exception {
        assertMemoryLeak(() -> {
            createTrades();
            assertQuery("SELECT * FROM trades WHERE to_utc(timestamp, 'Europe/London') >= '2022-01-03T12:00:00.000000Z'")
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
    public void testYearComparison() throws Exception {
        assertMemoryLeak(() -> {
            createYears();
            assertQuery("SELECT * FROM y WHERE year(timestamp) >= 2023")
                    .timestamp("timestamp")
                    .withPlanContaining("Interval forward scan on: y")
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

    private static void createYears() throws Exception {
        execute("CREATE TABLE y (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY YEAR;");
        execute("INSERT INTO y VALUES " +
                "(1, '2021-06-01T00:00:00.000000Z')," +
                "(2, '2022-06-01T00:00:00.000000Z')," +
                "(3, '2023-06-01T00:00:00.000000Z')," +
                "(4, '2024-06-01T00:00:00.000000Z');");
    }
}
