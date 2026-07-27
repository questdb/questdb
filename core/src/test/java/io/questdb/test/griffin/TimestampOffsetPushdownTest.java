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

import io.questdb.jit.JitUtil;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * Tests for timestamp predicate pushdown through virtual models with dateadd offset.
 * These tests verify that:
 * 1. Predicates are correctly pushed down with offset adjustment
 * 2. The correct rows are returned after pushdown
 * 3. The SQL plan shows the expected interval filters
 */
public class TimestampOffsetPushdownTest extends AbstractCairoTest {

    @Test
    public void testDayOffsetPushdown() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            // Row 1: timestamp 2022-01-01 12:00 -> ts (after -1d) = 2021-12-31 12:00 (NOT in 2022)
            execute("INSERT INTO trades VALUES (100, '2022-01-01T12:00:00.000000Z');");
            // Row 2: timestamp 2022-01-02 12:00 -> ts (after -1d) = 2022-01-01 12:00 (in 2022)
            execute("INSERT INTO trades VALUES (150, '2022-01-02T12:00:00.000000Z');");
            // Row 3: timestamp 2023-01-01 12:00 -> ts (after -1d) = 2022-12-31 12:00 (in 2022)
            execute("INSERT INTO trades VALUES (200, '2023-01-01T12:00:00.000000Z');");
            // Row 4: timestamp 2023-01-02 12:00 -> ts (after -1d) = 2023-01-01 12:00 (NOT in 2022)
            execute("INSERT INTO trades VALUES (250, '2023-01-02T12:00:00.000000Z');");

            String query = """
                    SELECT * FROM (
                        SELECT dateadd('d', -1, timestamp) as ts, price FROM trades
                    ) WHERE ts IN '2022'
                    """;

            // Verify correct data: rows 2, 3 (ts values in 2022)
            // Plan shows interval pushdown with +1 day offset applied
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("""
                            VirtualRecord
                              functions: [dateadd('d',-1,timestamp),price]
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: trades
                                      intervals: [("2022-01-02T00:00:00.000000Z","2023-01-01T23:59:59.999999Z")]
                            """)
                    .returns("""
                            ts\tprice
                            2022-01-01T12:00:00.000000Z\t150.0
                            2022-12-31T12:00:00.000000Z\t200.0
                            """);
        });
    }

    @Test
    public void testDynamicBoundOffsetPredicateRemainsAsFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                        ('2024-01-01T01:00:00.000000Z'),
                        ('2024-01-02T01:00:00.000000Z'),
                        ('2024-01-03T01:00:00.000000Z')
                    """);
            bindVariableService.setTimestamp(0, MicrosFormatUtils.parseTimestamp("2024-01-02T00:00:00.000000Z"));

            assertQuery("""
                    SELECT shifted
                    FROM (SELECT dateadd('h', -1, ts) shifted FROM t)
                    WHERE shifted = $1
                    """)
                    .noLeakCheck()
                    .timestamp("shifted")
                    .withPlanContaining("Filter filter: $0::timestamp=shifted")
                    .returns("""
                            shifted
                            2024-01-02T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testEmptyOffsetInterval() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('2024-01-01T01:00:00.000000Z')");

            assertQuery("""
                    SELECT shifted
                    FROM (SELECT dateadd('h', -1, ts) shifted FROM t)
                    WHERE shifted > NULL
                    """)
                    .noLeakCheck()
                    .timestamp("shifted")
                    .returns("shifted\n");
        });
    }

    @Test
    public void testHourOffsetPushdown() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            // Row 1: timestamp 2022-01-01 00:30:00 -> ts (after -1h) = 2021-12-31 23:30:00 (NOT in 2022)
            execute("INSERT INTO trades VALUES (100, '2022-01-01T00:30:00.000000Z');");
            // Row 2: timestamp 2022-01-01 01:30:00 -> ts (after -1h) = 2022-01-01 00:30:00 (in 2022)
            execute("INSERT INTO trades VALUES (150, '2022-01-01T01:30:00.000000Z');");
            // Row 3: timestamp 2022-12-31 23:30:00 -> ts (after -1h) = 2022-12-31 22:30:00 (in 2022)
            execute("INSERT INTO trades VALUES (200, '2022-12-31T23:30:00.000000Z');");
            // Row 4: timestamp 2023-01-01 00:30:00 -> ts (after -1h) = 2022-12-31 23:30:00 (in 2022)
            execute("INSERT INTO trades VALUES (250, '2023-01-01T00:30:00.000000Z');");
            // Row 5: timestamp 2023-01-01 01:30:00 -> ts (after -1h) = 2023-01-01 00:30:00 (NOT in 2022)
            execute("INSERT INTO trades VALUES (300, '2023-01-01T01:30:00.000000Z');");

            String query = """
                    SELECT * FROM (
                        SELECT dateadd('h', -1, timestamp) as ts, price FROM trades
                    ) WHERE ts IN '2022'
                    """;

            // Verify correct data: rows 2, 3, 4 (ts values in 2022)
            // Plan shows interval pushdown with +1h offset applied
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("""
                            VirtualRecord
                              functions: [dateadd('h',-1,timestamp),price]
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: trades
                                      intervals: [("2022-01-01T01:00:00.000000Z","2023-01-01T00:59:59.999999Z")]
                            """)
                    .returns("""
                            ts\tprice
                            2022-01-01T00:30:00.000000Z\t150.0
                            2022-12-31T22:30:00.000000Z\t200.0
                            2022-12-31T23:30:00.000000Z\t250.0
                            """);
        });
    }

    @Test
    public void testIntervalUnionOffsetPushdown() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                        ('2024-01-01T01:00:00.000000Z'),
                        ('2024-01-02T01:00:00.000000Z'),
                        ('2024-01-03T01:00:00.000000Z')
                    """);

            assertQuery("""
                    SELECT shifted
                    FROM (SELECT dateadd('h', -1, ts) shifted FROM t)
                    WHERE shifted IN ('2024-01-01', '2024-01-03')
                    """)
                    .noLeakCheck()
                    .timestamp("shifted")
                    .withPlanContaining("Interval forward scan on: t")
                    .returns("""
                            shifted
                            2024-01-01T00:00:00.000000Z
                            2024-01-03T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testIntervalUnionOffsetPushdownWithDynamicInnerBound() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                        ('2024-01-01T01:00:00.000000Z'),
                        ('2024-01-02T01:00:00.000000Z'),
                        ('2024-01-03T01:00:00.000000Z')
                    """);
            bindVariableService.setTimestamp(0, MicrosFormatUtils.parseTimestamp("2024-01-01T00:00:00.000000Z"));

            assertQuery("""
                    SELECT shifted
                    FROM (
                        SELECT dateadd('h', -1, ts) shifted
                        FROM t
                        WHERE ts >= $1
                    )
                    WHERE shifted IN ('2024-01-01', '2024-01-03')
                    """)
                    .noLeakCheck()
                    .timestamp("shifted")
                    .withPlanContaining("Interval forward scan on: t")
                    .returns("""
                            shifted
                            2024-01-01T00:00:00.000000Z
                            2024-01-03T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testIntervalUnionOffsetPushdownWithDynamicInnerInRemainsAsFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('2024-01-02T01:00:00.000000Z')");
            bindVariableService.setTimestamp(0, MicrosFormatUtils.parseTimestamp("2024-01-02T01:00:00.000000Z"));

            assertQuery("""
                    SELECT shifted
                    FROM (
                        SELECT dateadd('h', -1, ts) shifted
                        FROM t
                        WHERE ts IN ($1, '2024-01-02T01:00:00.000000Z')
                    )
                    WHERE shifted IN ('2024-01-01', '2024-01-03')
                    """)
                    .noLeakCheck()
                    .timestamp("shifted")
                    .withPlanContaining("filter: ts in")
                    .returns("shifted\n");
        });
    }

    @Test
    public void testLargeOffsetNegationOverflowThrowsError() throws Exception {
        // Test for int overflow when negating the stride value.
        // The optimizer stores the INVERSE of the stride for pushdown.
        // If stride = Integer.MIN_VALUE (-2147483648), then inverse = 2147483648 which overflows int.
        assertMemoryLeak(() -> execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;"));

        // Use Integer.MIN_VALUE as stride - negating it causes overflow
        // -(-2147483648) = 2147483648 which exceeds Integer.MAX_VALUE
        // Error position 35 is at the stride argument "-2147483648"
        assertQuery("SELECT * FROM (" +
                "SELECT dateadd('s', -2147483648, timestamp) as ts, price FROM trades" +
                ") WHERE ts > '2022-01-01'")
                .fails(35, "timestamp offset value 2147483648 exceeds maximum allowed range for dateadd");
    }

    @Test
    public void testLargeOffsetThrowsError() throws Exception {
        // When dateadd offset exceeds Integer.MAX_VALUE (2,147,483,647), the optimizer
        // should throw an actionable error message rather than silently failing.
        // This is because dateadd has signature: dateadd(char, int, timestamp)
        // and the optimizer intrinsically understands this function for predicate pushdown.
        //
        // 3,000,000,000 seconds = ~95 years, which exceeds int range.
        assertMemoryLeak(() -> execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;"));

        // Use an offset of 3 billion seconds (exceeds Integer.MAX_VALUE of 2,147,483,647)
        // Error position 35 is at the stride argument "3000000000"
        assertQuery("SELECT * FROM (" +
                "SELECT dateadd('s', 3000000000, timestamp) as ts, price FROM trades" +
                ") WHERE ts > '2100-01-01'")
                .fails(35, "timestamp offset value -3000000000 exceeds maximum allowed range for dateadd");
    }

    @Test
    public void testMinuteOffsetPushdown() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, '2022-01-01T00:15:00.000000Z');");
            execute("INSERT INTO trades VALUES (150, '2022-01-01T00:45:00.000000Z');");

            String query = """
                    SELECT * FROM (
                        SELECT dateadd('m', -30, timestamp) as ts, price FROM trades
                    ) WHERE ts >= '2022-01-01T00:00:00' AND ts < '2022-01-01T00:30:00'
                    """;

            // Row 1: ts = 2021-12-31 23:45 (NOT in range)
            // Row 2: ts = 2022-01-01 00:15 (in range)
            // Plan shows interval pushdown with +30 minute offset
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("""
                            VirtualRecord
                              functions: [dateadd('m',-30,timestamp),price]
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: trades
                                      intervals: [("2022-01-01T00:30:00.000000Z","2022-01-01T00:59:59.999999Z")]
                            """)
                    .returns("""
                            ts\tprice
                            2022-01-01T00:15:00.000000Z\t150.0
                            """);
        });
    }

    @Test
    public void testMixedPredicatesPushdown() throws Exception {
        // Test that non-timestamp predicates don't interfere with pushdown
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, '2022-01-01T01:30:00.000000Z');");
            execute("INSERT INTO trades VALUES (150, '2022-01-01T02:30:00.000000Z');");
            execute("INSERT INTO trades VALUES (200, '2022-01-01T03:30:00.000000Z');");

            String query = """
                    SELECT * FROM (
                        SELECT dateadd('h', -1, timestamp) as ts, price FROM trades
                    ) WHERE ts IN '2022' AND price > 120
                    """;

            // Verify plan shows interval pushdown AND filter for price
            String expectedPlan;
            if (JitUtil.isJitSupported()) {
                expectedPlan = """
                        VirtualRecord
                          functions: [dateadd('h',-1,timestamp),price]
                            Async JIT Filter workers: 1
                              filter: 120<price
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: trades
                                      intervals: [("2022-01-01T01:00:00.000000Z","2023-01-01T00:59:59.999999Z")]
                        """;
            } else {
                expectedPlan = """
                        VirtualRecord
                          functions: [dateadd('h',-1,timestamp),price]
                            Async Filter workers: 1
                              filter: 120<price
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: trades
                                      intervals: [("2022-01-01T01:00:00.000000Z","2023-01-01T00:59:59.999999Z")]
                        """;
            }

            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan(expectedPlan)
                    .returns("""
                            ts\tprice
                            2022-01-01T01:30:00.000000Z\t150.0
                            2022-01-01T02:30:00.000000Z\t200.0
                            """);
        });
    }

    @Test
    public void testMonthOffsetPushdown() throws Exception {
        // Month offset IS pushed down with calendar-aware handling
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY MONTH;");
            // Row 1: timestamp 2022-02-15 -> ts (after -1M) = 2022-01-15 (NOT in Feb 2022)
            execute("INSERT INTO trades VALUES (100, '2022-02-15T12:00:00.000000Z');");
            // Row 2: timestamp 2022-03-15 -> ts (after -1M) = 2022-02-15 (in Feb 2022)
            execute("INSERT INTO trades VALUES (150, '2022-03-15T12:00:00.000000Z');");
            // Row 3: timestamp 2022-04-15 -> ts (after -1M) = 2022-03-15 (NOT in Feb 2022)
            execute("INSERT INTO trades VALUES (200, '2022-04-15T12:00:00.000000Z');");

            String query = """
                    SELECT * FROM (
                        SELECT dateadd('M', -1, timestamp) as ts, price FROM trades
                    ) WHERE ts IN '2022-02'
                    """;

            // Should only return row 2
            // Plan shows interval pushdown with +1 month offset (calendar-aware)
            // 2022-02-01 + 1 month = 2022-03-01
            // 2022-02-28 23:59:59 + 1 month = 2022-03-28 23:59:59
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("""
                            VirtualRecord
                              functions: [dateadd('M',-1,timestamp),price]
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: trades
                                      intervals: [("2022-03-01T00:00:00.000000Z","2022-03-28T23:59:59.999999Z")]
                            """)
                    .returns("""
                            ts\tprice
                            2022-02-15T12:00:00.000000Z\t150.0
                            """);
        });
    }

    @Test
    public void testNegativeIntegerOffsetPushdown() throws Exception {
        // Verify that negative integer offsets (using unary minus) are correctly handled
        // This is a regression test for isConstantIntegerExpression handling of unary minus
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, '2022-01-01T02:00:00.000000Z');");

            // Use explicit negative value with unary minus
            String query = """
                    SELECT * FROM (
                        SELECT dateadd('h', -1, timestamp) as ts, price FROM trades
                    ) WHERE ts IN '2022'
                    """;

            // Verify data correctness
            // Plan should show interval pushdown with +1h offset applied
            // Verifies unary minus is correctly parsed as integer
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("""
                            VirtualRecord
                              functions: [dateadd('h',-1,timestamp),price]
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: trades
                                      intervals: [("2022-01-01T01:00:00.000000Z","2023-01-01T00:59:59.999999Z")]
                            """)
                    .returns("""
                            ts\tprice
                            2022-01-01T01:00:00.000000Z\t100.0
                            """);
        });
    }

    @Test
    public void testNestedOffsetsPushdown() throws Exception {
        // Test that nested dateadd offsets both get applied
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            // With -1h then -1d offset, total is -25h
            // Row at 2022-01-02 02:00 -> after -1h = 2022-01-02 01:00 -> after -1d = 2022-01-01 01:00
            execute("INSERT INTO trades VALUES (100, '2022-01-02T02:00:00.000000Z');");

            String query = """
                    SELECT * FROM (
                        SELECT dateadd('d', -1, ts1) as ts2, price FROM (
                            SELECT dateadd('h', -1, timestamp) as ts1, price FROM trades
                        )
                    ) WHERE ts2 >= '2022-01-01T00:00:00' AND ts2 < '2022-01-01T02:00:00'
                    """;

            // Plan shows combined offset: +1 day +1 hour = 25 hours
            // Range [2022-01-01 00:00, 2022-01-01 02:00) + 1d + 1h = [2022-01-02 01:00, 2022-01-02 03:00)
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts2")
                    .withPlan("""
                            VirtualRecord
                              functions: [dateadd('d',-1,ts1),price]
                                VirtualRecord
                                  functions: [dateadd('h',-1,timestamp),price]
                                    PageFrame
                                        Row forward scan
                                        Interval forward scan on: trades
                                          intervals: [("2022-01-02T01:00:00.000000Z","2022-01-02T02:59:59.999999Z")]
                            """)
                    .returns("""
                            ts2\tprice
                            2022-01-01T01:00:00.000000Z\t100.0
                            """);
        });
    }

    @Test
    public void testNestedOffsetsYearPushdown() throws Exception {
        // Test nested offsets with year-based dateadd
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");

            String query = """
                    SELECT * FROM (
                        SELECT dateadd('d', -1, ts1) as ts2, price FROM (
                            SELECT dateadd('h', -1, timestamp) as ts1, price FROM trades
                        )
                    ) WHERE ts2 IN '2022'
                    """;

            // Plan should show combined offset: +1 day +1 hour
            // 2022-01-01 00:00 + 1 day + 1 hour = 2022-01-02 01:00
            // 2022-12-31 23:59:59 + 1 day + 1 hour = 2023-01-02 00:59:59
            assertQuery(query)
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [dateadd('d',-1,ts1),price]
                                VirtualRecord
                                  functions: [dateadd('h',-1,timestamp),price]
                                    PageFrame
                                        Row forward scan
                                        Interval forward scan on: trades
                                          intervals: [("2022-01-02T01:00:00.000000Z","2023-01-02T00:59:59.999999Z")]
                            """);
        });
    }

    @Test
    public void testNoOffsetNoPushdownMetadata() throws Exception {
        // When there's no dateadd, no ts_offset should appear and literal timestamp wins
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");

            String query = """
                    SELECT price, timestamp FROM trades
                    """;

            // Plan should NOT have ts_offset since there's no dateadd transformation
            assertQuery(query)
                    .noLeakCheck()
                    .assertsPlan("""
                            PageFrame
                                Row forward scan
                                Frame forward scan on: trades
                            """);
        });
    }

    @Test
    public void testNonConstantOffsetNoPushdown() throws Exception {
        // Non-constant offset should not be pushed down
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, offset_val INT, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, 1, '2022-01-01T01:00:00.000000Z');");

            String query = """
                    SELECT * FROM (
                        SELECT dateadd('h', offset_val, timestamp) as ts, price FROM trades
                    ) WHERE ts IN '2022'
                    """;

            // Plan should show filter at virtual level, NOT interval scan (no pushdown)
            assertQuery(query)
                    .noLeakCheck()
                    .assertsPlan("""
                            Filter filter: ts in [1640995200000000,1672531199999999]
                                VirtualRecord
                                  functions: [dateadd('h',offset_val,timestamp),price]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """);
        });
    }

    @Test
    public void testNonLiteralColumnWithTimestampPredicateAndNoPushdown() throws Exception {
        // Test that when a predicate references BOTH the timestamp AND a non-literal column,
        // the non-literal part should NOT be pushed down to the nested model.
        // With AND, the predicates can be split so the timestamp part is pushed down.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, quantity INT, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, 10, '2022-01-01T01:30:00.000000Z');");
            execute("INSERT INTO trades VALUES (150, 20, '2022-01-01T02:30:00.000000Z');");
            execute("INSERT INTO trades VALUES (200, 5, '2022-01-01T03:30:00.000000Z');");

            // Query with computed column (non-literal) and predicate referencing both timestamp AND computed column
            String query = """
                    SELECT * FROM (
                        SELECT dateadd('h', -1, timestamp) as ts, price * quantity as total_value FROM trades
                    ) WHERE ts IN '2022' AND total_value > 1500
                    """;

            // Row 1: ts = 00:30, total_value = 1000 (NOT > 1500)
            // Row 2: ts = 01:30, total_value = 3000 (> 1500) - INCLUDED
            // Row 3: ts = 02:30, total_value = 1000 (NOT > 1500)
            // The timestamp predicate (ts IN '2022') should be pushed down with offset
            // The non-literal predicate (total_value > 1500) should stay at the outer level as a Filter
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("""
                            Filter filter: 1500<total_value
                                VirtualRecord
                                  functions: [dateadd('h',-1,timestamp),price*quantity]
                                    PageFrame
                                        Row forward scan
                                        Interval forward scan on: trades
                                          intervals: [("2022-01-01T01:00:00.000000Z","2023-01-01T00:59:59.999999Z")]
                            """)
                    .returns("""
                            ts\ttotal_value
                            2022-01-01T01:30:00.000000Z\t3000.0
                            """);
        });
    }

    @Test
    public void testNonLiteralColumnWithTimestampPredicateOrNoPushdown() throws Exception {
        // Test that when an OR predicate references BOTH the timestamp AND a non-literal column,
        // the ENTIRE predicate should NOT be pushed down because OR cannot be split.
        // This is a regression test for the case where isTimestampPredicate returns true
        // but the predicate also references other non-literal aliases that can't be resolved.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, quantity INT, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, 10, '2022-01-01T01:30:00.000000Z');");
            execute("INSERT INTO trades VALUES (150, 20, '2022-01-01T02:30:00.000000Z');");
            execute("INSERT INTO trades VALUES (200, 5, '2022-01-02T03:30:00.000000Z');");

            // Query with OR - this cannot be split, so it must stay at outer level
            // ts < '2022-01-01T01:00:00' would return no rows (all ts values are >= 01:00)
            // total_value > 1500 would return row 2 (3000)
            // The OR means: either early timestamp OR high total_value
            String query = """
                    SELECT * FROM (
                        SELECT dateadd('h', -1, timestamp) as ts, price * quantity as total_value FROM trades
                    ) WHERE ts < '2022-01-01T01:00:00' OR total_value > 1500
                    """;

            // Row 1: ts = 00:30 (< 01:00? YES), total_value = 1000 - INCLUDED (ts condition true)
            // Row 2: ts = 01:30 (< 01:00? NO), total_value = 3000 (> 1500? YES) - INCLUDED (total_value condition true)
            // Row 3: ts = 02:30 next day (< 01:00? NO), total_value = 1000 (> 1500? NO) - NOT included
            // The OR predicate cannot be split, so it should stay at outer level as a Filter
            // The timestamp offset pushdown should NOT happen because the predicate
            // references non-literal columns (total_value) that can't be resolved in nested model
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("""
                            Filter filter: (ts<2022-01-01T01:00:00.000000Z or 1500<total_value)
                                VirtualRecord
                                  functions: [dateadd('h',-1,timestamp),price*quantity]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """)
                    .returns("""
                            ts\ttotal_value
                            2022-01-01T00:30:00.000000Z\t1000.0
                            2022-01-01T01:30:00.000000Z\t3000.0
                            """);
        });
    }

    @Test
    public void testNullOffsetThrowsError() throws Exception {
        // Ensure a NULL stride is rejected
        assertMemoryLeak(() -> execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;"));
        assertQuery("SELECT * FROM (SELECT dateadd('h', NULL, timestamp) as ts, price FROM trades) WHERE ts IN '2022'")
                .fails(35, "`null` is not a valid stride");
    }

    @Test
    public void testOriginalTimestampPrecedenceNoOffsetPushdown() throws Exception {
        // When both original timestamp AND dateadd are in projection, filtering on the
        // dateadd column should NOT use and_offset pushdown (original timestamp is designated)
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, '2022-01-01T00:30:00.000000Z');");
            execute("INSERT INTO trades VALUES (150, '2022-01-01T01:30:00.000000Z');");
            execute("INSERT INTO trades VALUES (200, '2022-01-01T02:30:00.000000Z');");

            // Query with both timestamp columns and filter on the dateadd column
            String query = """
                    SELECT * FROM (
                        SELECT dateadd('h', -1, timestamp) as ts, timestamp as original_ts, price FROM trades
                    ) WHERE ts >= '2022-01-01T00:00:00' AND ts < '2022-01-01T01:00:00'
                    """;

            // Row 1: ts = 2021-12-31 23:30 (NOT in range)
            // Row 2: ts = 2022-01-01 00:30 (in range)
            // Row 3: ts = 2022-01-01 01:30 (NOT in range)
            // Plan should NOT show interval pushdown - filter stays at outer level
            // because original timestamp takes precedence (no ts_offset set)
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("original_ts")
                    .withPlan("""
                            Filter filter: (ts>=2022-01-01T00:00:00.000000Z and ts<2022-01-01T01:00:00.000000Z)
                                VirtualRecord
                                  functions: [dateadd('h',-1,timestamp),timestamp,price]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """)
                    .returns("""
                            ts\toriginal_ts\tprice
                            2022-01-01T00:30:00.000000Z\t2022-01-01T01:30:00.000000Z\t150.0
                            """);
        });
    }

    @Test
    public void testOriginalTimestampPrecedenceOverDateadd() throws Exception {
        // Test that when BOTH the original timestamp AND a dateadd column are in the projection,
        // the original timestamp takes precedence as the designated timestamp.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, '2022-01-01T01:00:00.000000Z');");
            execute("INSERT INTO trades VALUES (150, '2022-01-01T02:00:00.000000Z');");

            // Query where dateadd column comes before the literal timestamp column
            String query = """
                    SELECT dateadd('h', -1, timestamp) as ts, timestamp, price FROM trades
                    """;

            // The result should have 'timestamp' as the designated timestamp column (index 1),
            // not 'ts' (index 0), because the original timestamp takes precedence
            // Plan should show VirtualRecord - no ts_offset because original timestamp is present
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("timestamp")
                    .expectSize()
                    .withPlan("""
                            VirtualRecord
                              functions: [dateadd('h',-1,timestamp),timestamp,price]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: trades
                            """)
                    .returns("""
                            ts\ttimestamp\tprice
                            2022-01-01T00:00:00.000000Z\t2022-01-01T01:00:00.000000Z\t100.0
                            2022-01-01T01:00:00.000000Z\t2022-01-01T02:00:00.000000Z\t150.0
                            """);
        });
    }

    @Test
    public void testOriginalTimestampPrecedenceOverDateaddQualifiedAlias() throws Exception {
        // Test that when the original timestamp is referenced through a table alias (t.timestamp),
        // it should still take precedence over dateadd columns.
        // This is a regression test for the case where Chars.equalsIgnoreCase fails to match
        // qualified column names like "t.timestamp" against "timestamp".
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, '2022-01-01T01:00:00.000000Z');");
            execute("INSERT INTO trades VALUES (150, '2022-01-01T02:00:00.000000Z');");

            // Query with table alias and qualified timestamp reference
            // The inner query uses alias 't' and references t.timestamp
            String query = """
                    SELECT * FROM (
                        SELECT dateadd('h', -1, t.timestamp) as ts, t.timestamp as original_ts, price FROM trades t
                    ) WHERE ts >= '2022-01-01T00:00:00' AND ts < '2022-01-01T01:00:00'
                    """;

            // Row 1: ts = 2022-01-01 00:00 (in range), original_ts = 2022-01-01 01:00
            // Row 2: ts = 2022-01-01 01:00 (NOT in range)
            // Plan should NOT show interval pushdown because original timestamp (t.timestamp) is in projection
            // Filter should stay at outer level, not be pushed down with offset
            // Note: qualified column reference results in SelectedRecord wrapper and alias renaming
            assertQuery(query)
                    .noLeakCheck()
                    .withPlan("""
                            Filter filter: (ts>=2022-01-01T00:00:00.000000Z and ts<2022-01-01T01:00:00.000000Z)
                                VirtualRecord
                                  functions: [dateadd('h',-1,timestamp),original_ts,price]
                                    SelectedRecord
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: trades
                            """)
                    .returns("""
                            ts\toriginal_ts\tprice
                            2022-01-01T00:00:00.000000Z\t2022-01-01T01:00:00.000000Z\t100.0
                            """);
        });
    }

    @Test
    public void testPositiveOffsetPushdown() throws Exception {
        // Test positive offset (dateadd adds time, so pushdown subtracts)
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            // Row 1: timestamp 2021-12-31 22:00 -> ts (after +2h) = 2022-01-01 00:00 (in 2022)
            execute("INSERT INTO trades VALUES (100, '2021-12-31T22:00:00.000000Z');");
            // Row 2: timestamp 2021-12-31 21:00 -> ts (after +2h) = 2021-12-31 23:00 (NOT in 2022)
            execute("INSERT INTO trades VALUES (150, '2021-12-31T21:00:00.000000Z');");
            // Row 3: timestamp 2022-12-31 22:00 -> ts (after +2h) = 2023-01-01 00:00 (NOT in 2022)
            execute("INSERT INTO trades VALUES (200, '2022-12-31T22:00:00.000000Z');");
            // Row 4: timestamp 2022-12-31 21:00 -> ts (after +2h) = 2022-12-31 23:00 (in 2022)
            execute("INSERT INTO trades VALUES (250, '2022-12-31T21:00:00.000000Z');");

            String query = """
                    SELECT * FROM (
                        SELECT dateadd('h', 2, timestamp) as ts, price FROM trades
                    ) WHERE ts IN '2022'
                    """;

            // Verify correct data: rows 1, 4
            // Plan shows interval pushdown with -2h offset (inverse of +2)
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("""
                            VirtualRecord
                              functions: [dateadd('h',2,timestamp),price]
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: trades
                                      intervals: [("2021-12-31T22:00:00.000000Z","2022-12-31T21:59:59.999999Z")]
                            """)
                    .returns("""
                            ts\tprice
                            2022-01-01T00:00:00.000000Z\t100.0
                            2022-12-31T23:00:00.000000Z\t250.0
                            """);
        });
    }

    @Test
    public void testQualifiedColumnNameInDateaddExpression() throws Exception {
        // Test that qualified column names like "trades.timestamp" in the dateadd expression
        // are correctly recognized as referencing the designated timestamp
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, '2022-01-01T01:30:00.000000Z');");
            execute("INSERT INTO trades VALUES (150, '2022-01-01T02:30:00.000000Z');");

            // Use qualified column reference in dateadd expression: trades.timestamp
            String query = """
                    SELECT * FROM (
                        SELECT dateadd('h', -1, trades.timestamp) as ts, price FROM trades
                    ) WHERE ts IN '2022'
                    """;

            // Verify correct data and plan shows interval pushdown - qualified trades.timestamp should be detected
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("""
                            VirtualRecord
                              functions: [dateadd('h',-1,timestamp),price]
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: trades
                                      intervals: [("2022-01-01T01:00:00.000000Z","2023-01-01T00:59:59.999999Z")]
                            """)
                    .returns("""
                            ts\tprice
                            2022-01-01T00:30:00.000000Z\t100.0
                            2022-01-01T01:30:00.000000Z\t150.0
                            """);
        });
    }

    @Test
    public void testQualifiedColumnNameInPredicatePushdown() throws Exception {
        // Test that qualified column names like "v.ts" in WHERE clause are correctly matched
        // and rewritten for timestamp predicate pushdown
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, '2022-01-01T01:30:00.000000Z');");
            execute("INSERT INTO trades VALUES (150, '2022-01-01T02:30:00.000000Z');");

            // Use explicit table alias with qualified column reference in WHERE
            String query = """
                    SELECT * FROM (
                        SELECT dateadd('h', -1, timestamp) as ts, price FROM trades
                    ) v WHERE v.ts IN '2022'
                    """;

            // Verify correct data and plan shows interval pushdown even with qualified column name v.ts
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("""
                            VirtualRecord
                              functions: [dateadd('h',-1,timestamp),price]
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: trades
                                      intervals: [("2022-01-01T01:00:00.000000Z","2023-01-01T00:59:59.999999Z")]
                            """)
                    .returns("""
                            ts\tprice
                            2022-01-01T00:30:00.000000Z\t100.0
                            2022-01-01T01:30:00.000000Z\t150.0
                            """);
        });
    }

    @Test
    public void testQualifiedPredicateWithQualifiedSourceTimestamp() throws Exception {
        // Test that when rewriting a qualified predicate (v.ts) to a qualified source (t.timestamp),
        // we don't produce a double-qualified result like "v.t.timestamp".
        // This is a regression test for rewriteColumnToken incorrectly preserving prefix.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, '2022-01-01T01:30:00.000000Z');");
            execute("INSERT INTO trades VALUES (150, '2022-01-01T02:30:00.000000Z');");

            // Query with table alias 't' in dateadd, and outer alias 'v' with qualified predicate v.ts
            // The dateadd references t.timestamp, and the WHERE uses v.ts
            // When rewriting v.ts -> t.timestamp, we should get t.timestamp, not v.t.timestamp
            String query = """
                    SELECT * FROM (
                        SELECT dateadd('h', -1, t.timestamp) as ts, price FROM trades t
                    ) v WHERE v.ts IN '2022'
                    """;

            // Verify correct data and plan shows interval pushdown - the qualified references should be handled correctly
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("""
                            VirtualRecord
                              functions: [dateadd('h',-1,timestamp),price]
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: trades
                                      intervals: [("2022-01-01T01:00:00.000000Z","2023-01-01T00:59:59.999999Z")]
                            """)
                    .returns("""
                            ts\tprice
                            2022-01-01T00:30:00.000000Z\t100.0
                            2022-01-01T01:30:00.000000Z\t150.0
                            """);
        });
    }

    @Test
    public void testQuotedColumnNamePushdown() throws Exception {
        // Test that quoted column names like "ts" are correctly matched for pushdown
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, '2022-01-01T01:30:00.000000Z');");
            execute("INSERT INTO trades VALUES (150, '2022-01-01T02:30:00.000000Z');");

            // Use quoted column reference in WHERE clause
            String query = """
                    SELECT * FROM (
                        SELECT dateadd('h', -1, timestamp) as ts, price FROM trades
                    ) WHERE "ts" IN '2022'
                    """;

            // Verify correct data and plan shows interval pushdown even with quoted column name
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("""
                            VirtualRecord
                              functions: [dateadd('h',-1,timestamp),price]
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: trades
                                      intervals: [("2022-01-01T01:00:00.000000Z","2023-01-01T00:59:59.999999Z")]
                            """)
                    .returns("""
                            ts\tprice
                            2022-01-01T00:30:00.000000Z\t100.0
                            2022-01-01T01:30:00.000000Z\t150.0
                            """);
        });
    }

    @Test
    public void testRejectPredicateAndWithNow() throws Exception {
        // Predicate ts > '2022-01-01' AND ts < now()
        // The constant part can be pushed down, but the now() part stays as filter
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, '2022-01-01T12:00:00.000000Z');");

            String query = """
                    SELECT * FROM (
                        SELECT dateadd('d', -1, timestamp) as ts, price FROM trades
                    ) WHERE ts > '2022-01-01' AND ts < now()
                    """;

            // The constant part is pushed down, now() part stays as filter
            assertQuery(query)
                    .noLeakCheck()
                    .assertsPlan("""
                            Filter filter: ts<now()
                                VirtualRecord
                                  functions: [dateadd('d',-1,timestamp),price]
                                    PageFrame
                                        Row forward scan
                                        Interval forward scan on: trades
                                          intervals: [("2022-01-02T00:00:00.000001Z","MAX")]
                            """);
        });
    }

    @Test
    public void testRejectPredicateBetweenWithNow() throws Exception {
        // Predicate ts BETWEEN ... AND now() should NOT be pushed down
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, '2022-01-01T12:00:00.000000Z');");

            String query = """
                    SELECT * FROM (
                        SELECT dateadd('d', -1, timestamp) as ts, price FROM trades
                    ) WHERE ts BETWEEN dateadd('d', -7, now()) AND now()
                    """;

            // Plan should show Filter (not Interval forward scan)
            assertQuery(query)
                    .noLeakCheck()
                    .assertsPlan("""
                            Filter filter: ts between dateadd('d',-7,now()) and now()
                                VirtualRecord
                                  functions: [dateadd('d',-1,timestamp),price]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """);
        });
    }

    // ==================== Window Function Timestamp Tests ====================
    // Window functions don't support predicate pushdown (they need all rows first),
    // but timestamp detection should still work correctly.

    @Test
    public void testRejectPredicateOrWithNow() throws Exception {
        // Predicate ts > '2025-01-01' OR ts < now() should NOT be pushed down
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, '2022-01-01T12:00:00.000000Z');");

            String query = """
                    SELECT * FROM (
                        SELECT dateadd('d', -1, timestamp) as ts, price FROM trades
                    ) WHERE ts > '2025-01-01' OR ts < now()
                    """;

            // Plan should show Filter (not Interval forward scan)
            assertQuery(query)
                    .noLeakCheck()
                    .assertsPlan("""
                            Filter filter: (2025-01-01T00:00:00.000000Z<ts or ts<now())
                                VirtualRecord
                                  functions: [dateadd('d',-1,timestamp),price]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """);
        });
    }

    @Test
    public void testRejectPredicateWithDateaddNow() throws Exception {
        // Predicate ts > dateadd('d', -7, now()) should NOT be pushed down
        // because dateadd uses now() instead of timestamp
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, '2022-01-01T12:00:00.000000Z');");

            String query = """
                    SELECT * FROM (
                        SELECT dateadd('d', -1, timestamp) as ts, price FROM trades
                    ) WHERE ts > dateadd('d', -7, now())
                    """;

            // Plan should show Filter (not Interval forward scan) because dateadd doesn't use timestamp
            assertQuery(query)
                    .noLeakCheck()
                    .assertsPlan("""
                            Filter filter: dateadd('d',-7,now())<ts
                                VirtualRecord
                                  functions: [dateadd('d',-1,timestamp),price]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """);
        });
    }

    @Test
    public void testRejectPredicateWithDateaddSysdate() throws Exception {
        // Predicate ts > dateadd('h', -1, sysdate()) should NOT be pushed down
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, '2022-01-01T12:00:00.000000Z');");

            String query = """
                    SELECT * FROM (
                        SELECT dateadd('d', -1, timestamp) as ts, price FROM trades
                    ) WHERE ts > dateadd('h', -1, sysdate())
                    """;

            // Plan should show Filter (not Interval forward scan)
            assertQuery(query)
                    .noLeakCheck()
                    .assertsPlan("""
                            Filter filter: dateadd('h',-1,sysdate())<ts
                                VirtualRecord
                                  functions: [dateadd('d',-1,timestamp),price]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """);
        });
    }

    @Test
    public void testRejectPredicateWithNow() throws Exception {
        // Predicate ts > now() should NOT be pushed down
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, '2022-01-01T12:00:00.000000Z');");

            String query = """
                    SELECT * FROM (
                        SELECT dateadd('d', -1, timestamp) as ts, price FROM trades
                    ) WHERE ts > now()
                    """;

            // Plan should show Filter (not Interval forward scan) because now() is rejected
            assertQuery(query)
                    .noLeakCheck()
                    .assertsPlan("""
                            Filter filter: now()<ts
                                VirtualRecord
                                  functions: [dateadd('d',-1,timestamp),price]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """);
        });
    }

    @Test
    public void testRejectPredicateWithSysdate() throws Exception {
        // Predicate ts > sysdate() should NOT be pushed down
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, '2022-01-01T12:00:00.000000Z');");

            String query = """
                    SELECT * FROM (
                        SELECT dateadd('d', -1, timestamp) as ts, price FROM trades
                    ) WHERE ts > sysdate()
                    """;

            // Plan should show Filter (not Interval forward scan) because sysdate() is rejected
            assertQuery(query)
                    .noLeakCheck()
                    .assertsPlan("""
                            Filter filter: sysdate()<ts
                                VirtualRecord
                                  functions: [dateadd('d',-1,timestamp),price]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """);
        });
    }

    // ==================== Tests for rejected predicates ====================
    // These predicates should NOT be pushed down because they contain
    // disallowed functions (now, sysdate, etc.) or dateadd without timestamp reference.

    @Test
    public void testRejectPredicateWithSystimestamp() throws Exception {
        // Predicate ts > systimestamp() should NOT be pushed down
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, '2022-01-01T12:00:00.000000Z');");

            String query = """
                    SELECT * FROM (
                        SELECT dateadd('d', -1, timestamp) as ts, price FROM trades
                    ) WHERE ts > systimestamp()
                    """;

            // Plan should show Filter (not Interval forward scan) because systimestamp() is rejected
            assertQuery(query)
                    .noLeakCheck()
                    .assertsPlan("""
                            Filter filter: systimestamp()<ts
                                VirtualRecord
                                  functions: [dateadd('d',-1,timestamp),price]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """);
        });
    }

    @Test
    public void testSecondOffsetPushdown() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, '2022-01-01T00:00:30.000000Z');");
            execute("INSERT INTO trades VALUES (150, '2022-01-01T00:01:00.000000Z');");

            String query = """
                    SELECT * FROM (
                        SELECT dateadd('s', -30, timestamp) as ts, price FROM trades
                    ) WHERE ts >= '2022-01-01T00:00:00' AND ts < '2022-01-01T00:00:30'
                    """;

            // Row 1: ts = 2022-01-01 00:00:00 (in range)
            // Row 2: ts = 2022-01-01 00:00:30 (NOT in range, >= boundary)
            // Verify plan shows interval pushdown with +30 second offset
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("""
                            VirtualRecord
                              functions: [dateadd('s',-30,timestamp),price]
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: trades
                                      intervals: [("2022-01-01T00:00:30.000000Z","2022-01-01T00:00:59.999999Z")]
                            """)
                    .returns("""
                            ts\tprice
                            2022-01-01T00:00:00.000000Z\t100.0
                            """);
        });
    }

    @Test
    public void testTimestampOverflowThrowsError() throws Exception {
        // When applying the offset to a predicate timestamp would cause overflow,
        // throw an actionable error instead of silently wrapping around.
        //
        // Long.MAX_VALUE microseconds from epoch is approximately year 294247.
        // If we use dateadd('y', -300000, timestamp), the inverse offset is +300000 years.
        // Applying +300000 years to '2022-01-01' would result in year 302022,
        // which exceeds the maximum representable timestamp and causes overflow.
        assertMemoryLeak(() -> execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;"));

        // dateadd('y', -300000, timestamp) means the optimizer stores +300000 as the inverse offset.
        // When pushing down ts > '2022-01-01', it needs to apply +300000 years to 2022-01-01,
        // resulting in year 302022 which overflows the microsecond timestamp range (~year 294247).
        assertQuery("SELECT * FROM (" +
                "SELECT dateadd('y', -300000, timestamp) as ts, price FROM trades" +
                ") WHERE ts > '2022-01-01'")
                .fails(0, "timestamp overflow");
    }

    @Test
    public void testViewPushdown() throws Exception {
        // Test that predicates are pushed down through views with dateadd offset
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, '2022-01-01T12:00:00.000000Z');");
            execute("INSERT INTO trades VALUES (150, '2022-01-02T12:00:00.000000Z');");
            execute("INSERT INTO trades VALUES (200, '2022-01-03T12:00:00.000000Z');");
            execute("INSERT INTO trades VALUES (250, '2022-01-04T12:00:00.000000Z');");

            // Create a view that wraps dateadd on the timestamp
            execute("CREATE VIEW trades_offset AS SELECT dateadd('d', -1, timestamp) as ts, price FROM trades;");

            String query = "SELECT * FROM trades_offset WHERE ts IN '2022-01-01'";

            // Verify correct data: only the row where ts = 2022-01-01 (original timestamp = 2022-01-02)
            // Plan shows interval pushdown with +1 day offset applied through the view
            // ts IN '2022-01-01' means original timestamp must be in '2022-01-02'
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("""
                            VirtualRecord
                              functions: [dateadd('d',-1,timestamp),price]
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: trades
                                      intervals: [("2022-01-02T00:00:00.000000Z","2022-01-02T23:59:59.999999Z")]
                            """)
                    .returns("""
                            ts\tprice
                            2022-01-01T12:00:00.000000Z\t150.0
                            """);
        });
    }

    @Test
    public void testWeekOffsetPushdown() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY MONTH;");
            execute("INSERT INTO trades VALUES (100, '2022-01-08T12:00:00.000000Z');");
            execute("INSERT INTO trades VALUES (150, '2022-01-15T12:00:00.000000Z');");

            String query = """
                    SELECT * FROM (
                        SELECT dateadd('w', -1, timestamp) as ts, price FROM trades
                    ) WHERE ts >= '2022-01-01' AND ts < '2022-01-08'
                    """;

            // Row 1: ts = 2022-01-01 12:00 (in range)
            // Row 2: ts = 2022-01-08 12:00 (NOT in range, >= boundary)
            // Verify plan shows interval pushdown with +1 week offset
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("""
                            VirtualRecord
                              functions: [dateadd('w',-1,timestamp),price]
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: trades
                                      intervals: [("2022-01-08T00:00:00.000000Z","2022-01-14T23:59:59.999999Z")]
                            """)
                    .returns("""
                            ts\tprice
                            2022-01-01T12:00:00.000000Z\t100.0
                            """);
        });
    }

    @Test
    public void testWindowFunctionPreservesTimestamp() throws Exception {
        // Window function should preserve the designated timestamp from the nested model
        // Row ordering is maintained because window functions process rows in order
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, '2022-01-01T00:00:00.000000Z');");
            execute("INSERT INTO trades VALUES (150, '2022-01-01T01:00:00.000000Z');");
            execute("INSERT INTO trades VALUES (200, '2022-01-01T02:00:00.000000Z');");

            // Query with window function - timestamp should be preserved
            String query = "SELECT timestamp, price, row_number() OVER (ORDER BY timestamp) as rn FROM trades";

            // Verify data is correct and ordered
            // Window functions don't support random access but may know size
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("timestamp")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            timestamp\tprice\trn
                            2022-01-01T00:00:00.000000Z\t100.0\t1
                            2022-01-01T01:00:00.000000Z\t150.0\t2
                            2022-01-01T02:00:00.000000Z\t200.0\t3
                            """);
        });
    }

    @Test
    public void testWindowFunctionTimestampConsistency() throws Exception {
        // Verify that shifted and non-shifted timestamps produce consistent results
        // The window function should see the same row ordering in both cases
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, '2022-01-01T00:00:00.000000Z');");
            execute("INSERT INTO trades VALUES (150, '2022-01-01T01:00:00.000000Z');");
            execute("INSERT INTO trades VALUES (200, '2022-01-01T02:00:00.000000Z');");

            // Query 1: Without dateadd
            String queryNoShift = "SELECT timestamp, price, row_number() OVER (ORDER BY timestamp) as rn FROM trades";

            // Query 2: With dateadd (shift by 0 - effectively same timestamps)
            String queryWithShift = """
                    SELECT ts, price, row_number() OVER (ORDER BY ts) as rn
                    FROM (SELECT dateadd('h', 0, timestamp) as ts, price FROM trades)
                    """;

            // Both should produce the same row numbers
            String expectedNoShift = """
                    timestamp\tprice\trn
                    2022-01-01T00:00:00.000000Z\t100.0\t1
                    2022-01-01T01:00:00.000000Z\t150.0\t2
                    2022-01-01T02:00:00.000000Z\t200.0\t3
                    """;

            String expectedWithShift = """
                    ts\tprice\trn
                    2022-01-01T00:00:00.000000Z\t100.0\t1
                    2022-01-01T01:00:00.000000Z\t150.0\t2
                    2022-01-01T02:00:00.000000Z\t200.0\t3
                    """;

            // Window functions don't support random access but may know size
            assertQuery(queryNoShift)
                    .noLeakCheck()
                    .timestamp("timestamp")
                    .noRandomAccess()
                    .expectSize()
                    .returns(expectedNoShift);
            assertQuery(queryWithShift)
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .returns(expectedWithShift);
        });
    }

    @Test
    public void testWindowFunctionWithDateaddTimestamp() throws Exception {
        // Window function with dateadd on timestamp - should still work correctly
        // The dateadd-transformed column should be usable as timestamp
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, '2022-01-01T01:00:00.000000Z');");
            execute("INSERT INTO trades VALUES (150, '2022-01-01T02:00:00.000000Z');");
            execute("INSERT INTO trades VALUES (200, '2022-01-01T03:00:00.000000Z');");

            // Query with dateadd on timestamp and window function
            String query = """
                    SELECT ts, price, row_number() OVER (ORDER BY ts) as rn
                    FROM (SELECT dateadd('h', -1, timestamp) as ts, price FROM trades)
                    """;

            // Verify data is correct - dateadd shifts timestamps by -1 hour
            // Window functions don't support random access but may know size
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            ts\tprice\trn
                            2022-01-01T00:00:00.000000Z\t100.0\t1
                            2022-01-01T01:00:00.000000Z\t150.0\t2
                            2022-01-01T02:00:00.000000Z\t200.0\t3
                            """);
        });
    }

    @Test
    public void testWindowFunctionWithDateaddTimestampAndPredicate() throws Exception {
        // Window function with dateadd timestamp and WHERE clause
        // Predicate should NOT be pushed through window function (window needs all rows first)
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, '2022-01-01T01:00:00.000000Z');");
            execute("INSERT INTO trades VALUES (150, '2022-01-01T02:00:00.000000Z');");
            execute("INSERT INTO trades VALUES (200, '2022-01-01T03:00:00.000000Z');");
            execute("INSERT INTO trades VALUES (250, '2022-01-02T01:00:00.000000Z');");

            // Query: dateadd shifts by -1h, then window function, then filter
            // Row 1: ts = 00:00, rn = 1
            // Row 2: ts = 01:00, rn = 2
            // Row 3: ts = 02:00, rn = 3
            // Row 4: ts = 2022-01-02 00:00, rn = 4 (outside filter)
            String query = """
                    SELECT * FROM (
                        SELECT ts, price, row_number() OVER (ORDER BY ts) as rn
                        FROM (SELECT dateadd('h', -1, timestamp) as ts, price FROM trades)
                    ) WHERE ts IN '2022-01-01'
                    """;

            // Verify window function computes row numbers BEFORE filter is applied
            // All 4 rows get numbered, then we filter to 2022-01-01
            // Window functions don't support random access
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("""
                            ts\tprice\trn
                            2022-01-01T00:00:00.000000Z\t100.0\t1
                            2022-01-01T01:00:00.000000Z\t150.0\t2
                            2022-01-01T02:00:00.000000Z\t200.0\t3
                            """);
        });
    }

    @Test
    public void testYearOffsetPushdown() throws Exception {
        // Year offset IS pushed down with calendar-aware handling
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY YEAR;");
            // Row 1: timestamp 2023-06-15 -> ts (after -1y) = 2022-06-15 (in 2022)
            execute("INSERT INTO trades VALUES (100, '2023-06-15T12:00:00.000000Z');");
            // Row 2: timestamp 2024-06-15 -> ts (after -1y) = 2023-06-15 (NOT in 2022)
            execute("INSERT INTO trades VALUES (150, '2024-06-15T12:00:00.000000Z');");
            // Row 3: timestamp 2022-06-15 -> ts (after -1y) = 2021-06-15 (NOT in 2022)
            execute("INSERT INTO trades VALUES (200, '2022-06-15T12:00:00.000000Z');");

            String query = """
                    SELECT * FROM (
                        SELECT dateadd('y', -1, timestamp) as ts, price FROM trades
                    ) WHERE ts IN '2022'
                    """;

            // Should only return row 1
            // Plan shows interval pushdown with +1 year offset (calendar-aware)
            // 2022-01-01 + 1 year = 2023-01-01
            // 2022-12-31 23:59:59 + 1 year = 2023-12-31 23:59:59
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("""
                            VirtualRecord
                              functions: [dateadd('y',-1,timestamp),price]
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: trades
                                      intervals: [("2023-01-01T00:00:00.000000Z","2023-12-31T23:59:59.999999Z")]
                            """)
                    .returns("""
                            ts\tprice
                            2022-06-15T12:00:00.000000Z\t100.0
                            """);
        });
    }
}
