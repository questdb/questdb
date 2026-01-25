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

package io.questdb.test.griffin;

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

            // Verify plan shows interval pushdown with +1 day offset applied
            assertPlanNoLeakCheck(
                    query,
                    """
                            VirtualRecord
                              functions: [dateadd('d',-1,timestamp),price]
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: trades
                                      intervals: [("2022-01-02T00:00:00.000000Z","2023-01-01T23:59:59.999999Z")]
                            """
            );

            // Verify correct data: rows 2, 3 (ts values in 2022)
            assertQueryNoLeakCheck(
                    """
                            ts\tprice
                            2022-01-01T12:00:00.000000Z\t150.0
                            2022-12-31T12:00:00.000000Z\t200.0
                            """,
                    query,
                    "ts",
                    true,
                    false
            );
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

            // Verify plan shows interval pushdown with +1h offset applied
            assertPlanNoLeakCheck(
                    query,
                    """
                            VirtualRecord
                              functions: [dateadd('h',-1,timestamp),price]
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: trades
                                      intervals: [("2022-01-01T01:00:00.000000Z","2023-01-01T00:59:59.999999Z")]
                            """
            );

            // Verify correct data: rows 2, 3, 4 (ts values in 2022)
            assertQueryNoLeakCheck(
                    """
                            ts\tprice
                            2022-01-01T00:30:00.000000Z\t150.0
                            2022-12-31T22:30:00.000000Z\t200.0
                            2022-12-31T23:30:00.000000Z\t250.0
                            """,
                    query,
                    "ts",
                    true,
                    false
            );
        });
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

            // Verify plan shows interval pushdown with +30 minute offset
            assertPlanNoLeakCheck(
                    query,
                    """
                            VirtualRecord
                              functions: [dateadd('m',-30,timestamp),price]
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: trades
                                      intervals: [("2022-01-01T00:30:00.000000Z","2022-01-01T00:59:59.999999Z")]
                            """
            );

            // Row 1: ts = 2021-12-31 23:45 (NOT in range)
            // Row 2: ts = 2022-01-01 00:15 (in range)
            assertQueryNoLeakCheck(
                    """
                            ts\tprice
                            2022-01-01T00:15:00.000000Z\t150.0
                            """,
                    query,
                    "ts",
                    true,
                    false
            );
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
            assertPlanNoLeakCheck(
                    query,
                    """
                            VirtualRecord
                              functions: [dateadd('h',-1,timestamp),price]
                                Async Filter workers: 1
                                  filter: 120<price
                                    PageFrame
                                        Row forward scan
                                        Interval forward scan on: trades
                                          intervals: [("2022-01-01T01:00:00.000000Z","2023-01-01T00:59:59.999999Z")]
                            """
            );

            assertQueryNoLeakCheck(
                    """
                            ts\tprice
                            2022-01-01T01:30:00.000000Z\t150.0
                            2022-01-01T02:30:00.000000Z\t200.0
                            """,
                    query,
                    "ts",
                    true,
                    false
            );
        });
    }

    @Test
    public void testModelTimestampPrecedenceOverLiteral() throws Exception {
        // Test that when the model detects a dateadd timestamp column (ts), it takes precedence
        // over a literal column that happens to match the base table's timestamp name.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, '2022-01-01T01:00:00.000000Z');");
            execute("INSERT INTO trades VALUES (150, '2022-01-01T02:00:00.000000Z');");

            // Query where dateadd column comes before the literal timestamp column
            String query = """
                    SELECT dateadd('h', -1, timestamp) as ts, timestamp, price FROM trades
                    """;

            // Plan should show ts as the designated timestamp column (first column)
            // The dateadd pattern is detected internally for predicate pushdown
            assertPlanNoLeakCheck(
                    query,
                    """
                            VirtualRecord
                              functions: [dateadd('h',-1,timestamp),timestamp,price]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: trades
                            """
            );

            // The result should have 'ts' as the timestamp column (index 0), not 'timestamp' (index 1)
            assertQueryNoLeakCheck(
                    """
                            ts\ttimestamp\tprice
                            2022-01-01T00:00:00.000000Z\t2022-01-01T01:00:00.000000Z\t100.0
                            2022-01-01T01:00:00.000000Z\t2022-01-01T02:00:00.000000Z\t150.0
                            """,
                    query,
                    "ts",  // ts should be the timestamp, not 'timestamp'
                    true,
                    true
            );
        });
    }

    @Test
    public void testModelTimestampPrecedenceWithFilter() throws Exception {
        // Same as above but with a filter on the transformed timestamp
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, '2022-01-01T00:30:00.000000Z');");
            execute("INSERT INTO trades VALUES (150, '2022-01-01T01:30:00.000000Z');");
            execute("INSERT INTO trades VALUES (200, '2022-01-01T02:30:00.000000Z');");

            // Query with dateadd and filter - ts column should be detected as timestamp
            String query = """
                    SELECT * FROM (
                        SELECT dateadd('h', -1, timestamp) as ts, timestamp as original_ts, price FROM trades
                    ) WHERE ts >= '2022-01-01T00:00:00' AND ts < '2022-01-01T01:00:00'
                    """;

            // Plan should show interval pushdown using the dateadd-transformed timestamp
            assertPlanNoLeakCheck(
                    query,
                    """
                            VirtualRecord
                              functions: [dateadd('h',-1,timestamp),timestamp,price]
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: trades
                                      intervals: [("2022-01-01T01:00:00.000000Z","2022-01-01T01:59:59.999999Z")]
                            """
            );

            // ts (column 0) should be the timestamp, enabling proper interval pushdown
            // Row 1: ts = 2021-12-31 23:30 (NOT in range)
            // Row 2: ts = 2022-01-01 00:30 (in range)
            // Row 3: ts = 2022-01-01 01:30 (NOT in range)
            assertQueryNoLeakCheck(
                    """
                            ts\toriginal_ts\tprice
                            2022-01-01T00:30:00.000000Z\t2022-01-01T01:30:00.000000Z\t150.0
                            """,
                    query,
                    "ts",
                    true,
                    false
            );
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

            // Verify plan shows interval pushdown with +1 month offset (calendar-aware)
            // 2022-02-01 + 1 month = 2022-03-01
            // 2022-02-28 23:59:59 + 1 month = 2022-03-28 23:59:59
            assertPlanNoLeakCheck(
                    query,
                    """
                            VirtualRecord
                              functions: [dateadd('M',-1,timestamp),price]
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: trades
                                      intervals: [("2022-03-01T00:00:00.000000Z","2022-03-28T23:59:59.999999Z")]
                            """
            );

            // Should only return row 2
            assertQueryNoLeakCheck(
                    """
                            ts\tprice
                            2022-02-15T12:00:00.000000Z\t150.0
                            """,
                    query,
                    "ts",
                    true,
                    false
            );
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

            // Plan should show interval pushdown with +1h offset applied
            // Verifies unary minus is correctly parsed as integer
            assertPlanNoLeakCheck(
                    query,
                    """
                            VirtualRecord
                              functions: [dateadd('h',-1,timestamp),price]
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: trades
                                      intervals: [("2022-01-01T01:00:00.000000Z","2023-01-01T00:59:59.999999Z")]
                            """
            );

            // Verify data correctness
            assertQueryNoLeakCheck(
                    """
                            ts\tprice
                            2022-01-01T01:00:00.000000Z\t100.0
                            """,
                    query,
                    "ts",
                    true,
                    false
            );
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

            // Verify plan shows combined offset: +1 day +1 hour = 25 hours
            // Range [2022-01-01 00:00, 2022-01-01 02:00) + 1d + 1h = [2022-01-02 01:00, 2022-01-02 03:00)
            assertPlanNoLeakCheck(
                    query,
                    """
                            VirtualRecord
                              functions: [dateadd('d',-1,ts1),price]
                                VirtualRecord
                                  functions: [dateadd('h',-1,timestamp),price]
                                    PageFrame
                                        Row forward scan
                                        Interval forward scan on: trades
                                          intervals: [("2022-01-02T01:00:00.000000Z","2022-01-02T02:59:59.999999Z")]
                            """
            );

            assertQueryNoLeakCheck(
                    """
                            ts2\tprice
                            2022-01-01T01:00:00.000000Z\t100.0
                            """,
                    query,
                    "ts2",
                    true,
                    false
            );
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
            assertPlanNoLeakCheck(
                    query,
                    """
                            VirtualRecord
                              functions: [dateadd('d',-1,ts1),price]
                                VirtualRecord
                                  functions: [dateadd('h',-1,timestamp),price]
                                    PageFrame
                                        Row forward scan
                                        Interval forward scan on: trades
                                          intervals: [("2022-01-02T01:00:00.000000Z","2023-01-02T00:59:59.999999Z")]
                            """
            );
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
            assertPlanNoLeakCheck(
                    query,
                    """
                            PageFrame
                                Row forward scan
                                Frame forward scan on: trades
                            """
            );
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
            assertPlanNoLeakCheck(
                    query,
                    """
                            Filter filter: ts in [1640995200000000,1672531199999999]
                                VirtualRecord
                                  functions: [dateadd('h',offset_val,timestamp),price]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """
            );
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

            // Verify plan shows interval pushdown with -2h offset (inverse of +2)
            assertPlanNoLeakCheck(
                    query,
                    """
                            VirtualRecord
                              functions: [dateadd('h',2,timestamp),price]
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: trades
                                      intervals: [("2021-12-31T22:00:00.000000Z","2022-12-31T21:59:59.999999Z")]
                            """
            );

            // Verify correct data: rows 1, 4
            assertQueryNoLeakCheck(
                    """
                            ts\tprice
                            2022-01-01T00:00:00.000000Z\t100.0
                            2022-12-31T23:00:00.000000Z\t250.0
                            """,
                    query,
                    "ts",
                    true,
                    false
            );
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

            // Verify plan shows interval pushdown with +30 second offset
            assertPlanNoLeakCheck(
                    query,
                    """
                            VirtualRecord
                              functions: [dateadd('s',-30,timestamp),price]
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: trades
                                      intervals: [("2022-01-01T00:00:30.000000Z","2022-01-01T00:00:59.999999Z")]
                            """
            );

            // Row 1: ts = 2022-01-01 00:00:00 (in range)
            // Row 2: ts = 2022-01-01 00:00:30 (NOT in range, >= boundary)
            assertQueryNoLeakCheck(
                    """
                            ts\tprice
                            2022-01-01T00:00:00.000000Z\t100.0
                            """,
                    query,
                    "ts",
                    true,
                    false
            );
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

            // Verify plan shows interval pushdown with +1 week offset
            assertPlanNoLeakCheck(
                    query,
                    """
                            VirtualRecord
                              functions: [dateadd('w',-1,timestamp),price]
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: trades
                                      intervals: [("2022-01-08T00:00:00.000000Z","2022-01-14T23:59:59.999999Z")]
                            """
            );

            // Row 1: ts = 2022-01-01 12:00 (in range)
            // Row 2: ts = 2022-01-08 12:00 (NOT in range, >= boundary)
            assertQueryNoLeakCheck(
                    """
                            ts\tprice
                            2022-01-01T12:00:00.000000Z\t100.0
                            """,
                    query,
                    "ts",
                    true,
                    false
            );
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

            // Verify plan shows interval pushdown with +1 year offset (calendar-aware)
            // 2022-01-01 + 1 year = 2023-01-01
            // 2022-12-31 23:59:59 + 1 year = 2023-12-31 23:59:59
            assertPlanNoLeakCheck(
                    query,
                    """
                            VirtualRecord
                              functions: [dateadd('y',-1,timestamp),price]
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: trades
                                      intervals: [("2023-01-01T00:00:00.000000Z","2023-12-31T23:59:59.999999Z")]
                            """
            );

            // Should only return row 1
            assertQueryNoLeakCheck(
                    """
                            ts\tprice
                            2022-06-15T12:00:00.000000Z\t100.0
                            """,
                    query,
                    "ts",
                    true,
                    false
            );
        });
    }
}
