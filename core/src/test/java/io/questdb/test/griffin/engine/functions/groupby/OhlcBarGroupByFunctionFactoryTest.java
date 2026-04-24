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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.PropertyKey;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class OhlcBarGroupByFunctionFactoryTest extends AbstractCairoTest {

    // Unicode characters used in rendering:
    // ⠀ = U+2800 Braille Blank (padding)
    // ─ = U+2500 Box Drawings Light Horizontal (wick)
    // █ = U+2588 Full Block (bullish body)
    // ░ = U+2591 Light Shade (bearish body)
    // │ = U+2502 Box Drawings Light Vertical (doji)

    @Test
    public void testAllIdenticalValues() throws Exception {
        // All values the same: open == close == high == low -> doji at center
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (100.0, '2024-01-01T00:00:00.000000Z'),
                    (100.0, '2024-01-01T00:01:00.000000Z'),
                    (100.0, '2024-01-01T00:02:00.000000Z')
                    """);
            // With width=10, all identical -> doji at center (pos 5), blank padding
            assertSql(
                    "ohlc_bar\n" +
                            "\u2800\u2800\u2800\u2800\u2800\u2502\u2800\u2800\u2800\u2800\n",
                    "SELECT ohlc_bar(price, 10) FROM t"
            );
        });
    }

    @Test
    public void testAllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES (NULL, '2024-01-01T00:00:00.000000Z')");
            assertSql(
                    "ohlc_bar\n\n",
                    "SELECT ohlc_bar(price, 10) FROM t"
            );
        });
    }

    @Test
    public void testBearishCandle() throws Exception {
        // Close < Open -> bearish body (░)
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (80.0, '2024-01-01T00:00:00.000000Z'),
                    (100.0, '2024-01-01T00:01:00.000000Z'),
                    (0.0, '2024-01-01T00:02:00.000000Z'),
                    (20.0, '2024-01-01T00:03:00.000000Z')
                    """);
            // Open=80, High=100, Low=0, Close=20
            // Width=10: range=100, positions: open=7, close=1
            // body_start=1, body_end=7 -> bearish ░ from 1 to 7
            // wick at 0 (low), wick at 8-9 (high)
            assertSql(
                    "ohlc_bar\n" +
                            "\u2500\u2591\u2591\u2591\u2591\u2591\u2591\u2591\u2500\u2500\n",
                    "SELECT ohlc_bar(price, 10) FROM t"
            );
        });
    }

    @Test
    public void testBullishCandle() throws Exception {
        // Close > Open -> bullish body (█)
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (20.0, '2024-01-01T00:00:00.000000Z'),
                    (100.0, '2024-01-01T00:01:00.000000Z'),
                    (0.0, '2024-01-01T00:02:00.000000Z'),
                    (80.0, '2024-01-01T00:03:00.000000Z')
                    """);
            // Open=20, High=100, Low=0, Close=80
            // Width=10: range=100, positions: open=1, close=7
            // body_start=1, body_end=7 -> bullish █ from 1 to 7
            // wick at 0 (low), wick at 8-9 (high)
            assertSql(
                    "ohlc_bar\n" +
                            "\u2500\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2500\u2500\n",
                    "SELECT ohlc_bar(price, 10) FROM t"
            );
        });
    }

    @Test
    public void testCustomWidth() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (50.0, '2024-01-01T00:00:00.000000Z'),
                    (100.0, '2024-01-01T00:01:00.000000Z'),
                    (0.0, '2024-01-01T00:02:00.000000Z'),
                    (50.0, '2024-01-01T00:03:00.000000Z')
                    """);
            // Open=50, Close=50 -> doji at center. Width=5.
            // Positions: open=2, close=2 -> doji at 2
            assertSql(
                    "ohlc_bar\n" +
                            "\u2500\u2500\u2502\u2500\u2500\n",
                    "SELECT ohlc_bar(price, 5) FROM t"
            );
        });
    }

    @Test
    public void testDojiCandle() throws Exception {
        // Open == Close -> doji (┼)
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (50.0, '2024-01-01T00:00:00.000000Z'),
                    (100.0, '2024-01-01T00:01:00.000000Z'),
                    (0.0, '2024-01-01T00:02:00.000000Z'),
                    (50.0, '2024-01-01T00:03:00.000000Z')
                    """);
            // Open=50, High=100, Low=0, Close=50 -> doji at center
            // Width=10: open_pos=4, close_pos=4 -> doji at 4
            assertSql(
                    "ohlc_bar\n" +
                            "\u2500\u2500\u2500\u2500\u2502\u2500\u2500\u2500\u2500\u2500\n",
                    "SELECT ohlc_bar(price, 10) FROM t"
            );
        });
    }

    @Test
    public void testEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            assertSql(
                    "ohlc_bar\n\n",
                    "SELECT ohlc_bar(price, 10) FROM t"
            );
        });
    }

    @Test
    public void testExplainPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            assertPlanNoLeakCheck(
                    "SELECT ohlc_bar(price, 10) FROM t",
                    """
                            Async Group By workers: 1
                              vectorized: false
                              values: [ohlc_bar(price,10)]
                              filter: null
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """
            );
        });
    }

    @Test
    public void testFactoryReuse() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (90.0, '2024-01-01T00:01:00.000000Z')
                    """);
            // Run once
            assertSql(
                    "ohlc_bar\n" +
                            "\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\n",
                    "SELECT ohlc_bar(price, 10) FROM t"
            );
            // Truncate and re-insert different data
            execute("TRUNCATE TABLE t");
            execute("""
                    INSERT INTO t VALUES
                    (90.0, '2024-01-01T00:00:00.000000Z'),
                    (10.0, '2024-01-01T00:01:00.000000Z')
                    """);
            // Should produce bearish now
            assertSql(
                    "ohlc_bar\n" +
                            "\u2591\u2591\u2591\u2591\u2591\u2591\u2591\u2591\u2591\u2591\n",
                    "SELECT ohlc_bar(price, 10) FROM t"
            );
        });
    }

    @Test
    public void testFillNone() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:10:00.000000Z'),
                    (20.0, '2024-01-01T00:20:00.000000Z'),
                    (30.0, '2024-01-01T02:10:00.000000Z'),
                    (40.0, '2024-01-01T02:20:00.000000Z')
                    """);
            // FILL(NONE) skips hour 01:00
            String result = queryResultString("SELECT ts, ohlc_bar(price, 5) FROM t SAMPLE BY 1h FILL(NONE)");
            // Should have 2 rows (header + 2 data rows = 3 lines)
            int lineCount = result.split("\n").length;
            Assert.assertEquals("Expected 3 lines (header + 2 rows)", 3, lineCount);
        });
    }

    @Test
    public void testFillNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:10:00.000000Z'),
                    (20.0, '2024-01-01T00:20:00.000000Z'),
                    (30.0, '2024-01-01T02:10:00.000000Z'),
                    (40.0, '2024-01-01T02:20:00.000000Z')
                    """);
            // Global range: 10-40. Width=5.
            // Hour 00: O=10(pos 0), C=20(pos 1), L=10, H=20 -> bullish pos 0-1
            // Hour 01: no data -> NULL
            // Hour 02: O=30(pos 2), C=40(pos 4), L=30, H=40 -> bullish pos 2-4
            assertSql(
                    "ts\tohlc_bar\n" +
                            "2024-01-01T00:00:00.000000Z\t\u2588\u2588\u2800\u2800\u2800\n" +
                            "2024-01-01T01:00:00.000000Z\t\n" +
                            "2024-01-01T02:00:00.000000Z\t\u2800\u2800\u2588\u2588\u2588\n",
                    "SELECT ts, ohlc_bar(price, 5) FROM t SAMPLE BY 1h FILL(NULL)"
            );
        });
    }

    @Test
    public void testFillPrev() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:10:00.000000Z'),
                    (20.0, '2024-01-01T00:20:00.000000Z'),
                    (30.0, '2024-01-01T02:10:00.000000Z'),
                    (40.0, '2024-01-01T02:20:00.000000Z')
                    """);
            // FILL(PREV) copies the rendered candle from the previous bucket.
            // Hour 00 is rendered with global range known at that point (10-20),
            // so it fills the full width. Hour 01 copies that. Hour 02 sees
            // the full global range (10-40) and renders proportionally.
            assertSql(
                    "ts\tohlc_bar\n" +
                            "2024-01-01T00:00:00.000000Z\t\u2588\u2588\u2588\u2588\u2588\n" +
                            "2024-01-01T01:00:00.000000Z\t\u2588\u2588\u2588\u2588\u2588\n" +
                            "2024-01-01T02:00:00.000000Z\t\u2800\u2800\u2588\u2588\u2588\n",
                    "SELECT ts, ohlc_bar(price, 5) FROM t SAMPLE BY 1h FILL(PREV)"
            );
        });
    }

    @Test
    public void testGroupBySymbol() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, symbol SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, 'A', '2024-01-01T00:00:00.000000Z'),
                    (90.0, 'A', '2024-01-01T01:00:00.000000Z'),
                    (90.0, 'B', '2024-01-01T00:00:00.000000Z'),
                    (10.0, 'B', '2024-01-01T01:00:00.000000Z')
                    """);
            // A: open=10, close=90 -> bullish (full bar)
            // B: open=90, close=10 -> bearish (full bar)
            assertSql(
                    "symbol\tohlc_bar\n" +
                            "A\t\u2588\u2588\u2588\u2588\u2588\n" +
                            "B\t\u2591\u2591\u2591\u2591\u2591\n",
                    "SELECT symbol, ohlc_bar(price, 5) FROM t ORDER BY symbol"
            );
        });
    }

    @Test
    public void testIntColumn() throws Exception {
        // Implicit cast from INT to DOUBLE
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (20, '2024-01-01T00:00:00.000000Z'),
                    (100, '2024-01-01T00:01:00.000000Z'),
                    (0, '2024-01-01T00:02:00.000000Z'),
                    (80, '2024-01-01T00:03:00.000000Z')
                    """);
            // Same as testBullishCandle but with INT input
            assertSql(
                    "ohlc_bar\n" +
                            "\u2500\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2500\u2500\n",
                    "SELECT ohlc_bar(price, 10) FROM t"
            );
        });
    }

    @Test
    public void testLabelsBasic() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (20.0, '2024-01-01T00:00:00.000000Z'),
                    (100.0, '2024-01-01T00:01:00.000000Z'),
                    (0.0, '2024-01-01T00:02:00.000000Z'),
                    (80.0, '2024-01-01T00:03:00.000000Z')
                    """);
            // Bullish candle with labels
            assertSql(
                    "ohlc_bar_labels\n" +
                            "\u2500\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2500\u2500 O:20.0 H:100.0 L:0.0 C:80.0\n",
                    "SELECT ohlc_bar_labels(price, 10) FROM t"
            );
        });
    }

    @Test
    public void testLabelsWithCustomWidth() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (50.0, '2024-01-01T00:00:00.000000Z'),
                    (100.0, '2024-01-01T00:01:00.000000Z'),
                    (0.0, '2024-01-01T00:02:00.000000Z'),
                    (50.0, '2024-01-01T00:03:00.000000Z')
                    """);
            // Doji with labels, width=5
            assertSql(
                    "ohlc_bar_labels\n" +
                            "\u2500\u2500\u2502\u2500\u2500 O:50.0 H:100.0 L:0.0 C:50.0\n",
                    "SELECT ohlc_bar_labels(price, 5) FROM t"
            );
        });
    }

    @Test
    public void testMixedNullsSkipped() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (NULL, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T00:01:00.000000Z'),
                    (NULL, '2024-01-01T00:02:00.000000Z'),
                    (80.0, '2024-01-01T00:03:00.000000Z'),
                    (NULL, '2024-01-01T00:04:00.000000Z')
                    """);
            // Nulls skipped: open=20, close=80, min=20, max=80 -> bullish full bar
            assertSql(
                    "ohlc_bar\n" +
                            "\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\n",
                    "SELECT ohlc_bar(price, 10) FROM t"
            );
        });
    }

    @Test
    public void testNegativeWidth() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES (50.0, '2024-01-01T00:00:00.000000Z')");
            assertException(
                    "SELECT ohlc_bar(price, -5) FROM t",
                    23,
                    "width must be a positive integer"
            );
        });
    }

    @Test
    public void testOrderByOhlcBar() throws Exception {
        // Exercises A/B flyweight independence: sort comparator
        // fetches getVarcharA and getVarcharB from the same instance.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, symbol SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, 'A', '2024-01-01T00:00:00.000000Z'),
                    (90.0, 'A', '2024-01-01T01:00:00.000000Z'),
                    (90.0, 'B', '2024-01-01T00:00:00.000000Z'),
                    (10.0, 'B', '2024-01-01T01:00:00.000000Z'),
                    (50.0, 'C', '2024-01-01T00:00:00.000000Z'),
                    (50.0, 'C', '2024-01-01T01:00:00.000000Z')
                    """);
            // Just verify it doesn't crash - ORDER BY on varchar bar output
            String result = queryResultString("SELECT symbol, ohlc_bar(price, 5) bar FROM t ORDER BY bar");
            Assert.assertTrue(result.contains("A"));
            Assert.assertTrue(result.contains("B"));
            Assert.assertTrue(result.contains("C"));
        });
    }

    @Test
    public void testSampleByHourly() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (50.0, '2024-01-01T00:30:00.000000Z'),
                    (80.0, '2024-01-01T01:00:00.000000Z'),
                    (60.0, '2024-01-01T01:30:00.000000Z')
                    """);
            // Global range: 10-80. Width=5.
            // Hour 00: open=10(pos 0), close=50(pos 2), low=10, high=50 -> bullish pos 0-2
            // Hour 01: open=80(pos 4), close=60(pos 2), low=60, high=80 -> bearish pos 2-4
            assertSql(
                    "ts\tohlc_bar\n" +
                            "2024-01-01T00:00:00.000000Z\t\u2588\u2588\u2588\u2800\u2800\n" +
                            "2024-01-01T01:00:00.000000Z\t\u2800\u2800\u2591\u2591\u2591\n",
                    "SELECT ts, ohlc_bar(price, 5) FROM t SAMPLE BY 1h"
            );
        });
    }

    @Test
    public void testSingleValue() throws Exception {
        // Single observation: open == close == high == low -> doji at center
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES (42.0, '2024-01-01T00:00:00.000000Z')");
            // All identical -> doji at center (pos 5 for width=10), blank padding
            assertSql(
                    "ohlc_bar\n" +
                            "\u2800\u2800\u2800\u2800\u2800\u2502\u2800\u2800\u2800\u2800\n",
                    "SELECT ohlc_bar(price, 10) FROM t"
            );
        });
    }

    @Test
    public void testTwoValues() throws Exception {
        // Two values: open is first, close is second
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (0.0, '2024-01-01T00:00:00.000000Z'),
                    (100.0, '2024-01-01T00:01:00.000000Z')
                    """);
            // Open=0 (pos 0), Close=100 (pos 9), bullish full bar
            assertSql(
                    "ohlc_bar\n" +
                            "\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\n",
                    "SELECT ohlc_bar(price, 10) FROM t"
            );
        });
    }

    @Test
    public void testWidthExceedsMaxLimit() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_STR_FUNCTION_BUFFER_MAX_SIZE, 30);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES (50.0, '2024-01-01T00:00:00.000000Z')");
            assertException(
                    "SELECT ohlc_bar(price, 100) FROM t",
                    23,
                    "breached memory limit set for ohlc_bar"
            );
        });
    }

    @Test
    public void testZeroWidth() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES (50.0, '2024-01-01T00:00:00.000000Z')");
            assertException(
                    "SELECT ohlc_bar(price, 0) FROM t",
                    23,
                    "width must be a positive integer"
            );
        });
    }

    @Test
    public void testFillLinearRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:10:00.000000Z'),
                    (20.0, '2024-01-01T01:10:00.000000Z')
                    """);
            assertException(
                    "SELECT ts, ohlc_bar(price, 5) FROM t SAMPLE BY 1h FILL(LINEAR)",
                    11,
                    "support for LINEAR fill is not yet implemented"
            );
        });
    }

    @Test
    public void testLabelsSampleByHourly() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (50.0, '2024-01-01T00:30:00.000000Z'),
                    (80.0, '2024-01-01T01:00:00.000000Z'),
                    (60.0, '2024-01-01T01:30:00.000000Z')
                    """);
            // Labels variant with SAMPLE BY
            assertSql(
                    "ts\tohlc_bar_labels\n" +
                            "2024-01-01T00:00:00.000000Z\t\u2588\u2588\u2588\u2800\u2800 O:10.0 H:50.0 L:10.0 C:50.0\n" +
                            "2024-01-01T01:00:00.000000Z\t\u2800\u2800\u2591\u2591\u2591 O:80.0 H:80.0 L:60.0 C:60.0\n",
                    "SELECT ts, ohlc_bar_labels(price, 5) FROM t SAMPLE BY 1h"
            );
        });
    }

    @Test
    public void testParallelExecutionLargeDataset() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, symbol SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            // Insert enough data to trigger parallel execution
            execute("""
                    INSERT INTO t
                    SELECT
                        x % 100 + rnd_double() * 10,
                        rnd_symbol('A', 'B', 'C', 'D'),
                        dateadd('s', x::INT, '2024-01-01T00:00:00.000000Z')
                    FROM long_sequence(100_000)
                    """);
            // Run twice and verify deterministic output
            String result1 = queryResultString("SELECT symbol, ohlc_bar(price, 20) FROM t ORDER BY symbol");
            String result2 = queryResultString("SELECT symbol, ohlc_bar(price, 20) FROM t ORDER BY symbol");
            Assert.assertEquals(result1, result2);
            // Verify we got 4 groups
            int lineCount = result1.split("\n").length;
            Assert.assertEquals("Expected 5 lines (header + 4 symbols)", 5, lineCount);
        });
    }

    // --- Scalar ohlc_bar(open, high, low, close, min, max [, width]) tests ---

    @Test
    public void testScalarBasicBullish() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "ohlc_bar\n" +
                        "\u2500\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2500\u2500\n",
                "SELECT ohlc_bar(20, 100, 0, 80, 0, 100, 10)"
        ));
    }

    @Test
    public void testScalarBasicBearish() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "ohlc_bar\n" +
                        "\u2500\u2591\u2591\u2591\u2591\u2591\u2591\u2591\u2500\u2500\n",
                "SELECT ohlc_bar(80, 100, 0, 20, 0, 100, 10)"
        ));
    }

    @Test
    public void testScalarDefaultWidth() throws Exception {
        // 6-arg variant uses default width (40)
        assertMemoryLeak(() -> {
            String result = queryResultString("SELECT ohlc_bar(20, 100, 0, 80, 0, 100)");
            // Header + one row. The bar should be 40 chars (120 UTF-8 bytes).
            Assert.assertTrue(result.contains("\u2588"));
        });
    }

    @Test
    public void testScalarDoji() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "ohlc_bar\n" +
                        "\u2500\u2500\u2500\u2500\u2502\u2500\u2500\u2500\u2500\u2500\n",
                "SELECT ohlc_bar(50, 100, 0, 50, 0, 100, 10)"
        ));
    }

    @Test
    public void testScalarInvertedBoundsThrows() throws Exception {
        assertMemoryLeak(() -> assertException(
                "SELECT ohlc_bar(50, 100, 0, 50, 100, 0, 10)",
                0,
                "min must not exceed max"
        ));
    }

    @Test
    public void testScalarLabels() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "ohlc_bar_labels\n" +
                        "\u2500\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2500\u2500 O:20.0 H:100.0 L:0.0 C:80.0\n",
                "SELECT ohlc_bar_labels(20, 100, 0, 80, 0, 100, 10)"
        ));
    }

    @Test
    public void testScalarNullInput() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "ohlc_bar\n\n",
                "SELECT ohlc_bar(NULL, 100, 0, 80, 0, 100, 10)"
        ));
    }

    @Test
    public void testScalarWithSampleBySubquery() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (50.0, '2024-01-01T00:30:00.000000Z'),
                    (80.0, '2024-01-01T01:00:00.000000Z'),
                    (60.0, '2024-01-01T01:30:00.000000Z')
                    """);
            // Scalar with window functions for global scaling.
            // Window functions can't be in the same SELECT as aggregates,
            // so we nest: inner = SAMPLE BY, middle = window, outer = render.
            assertSql(
                    "ts\tohlc_bar\n" +
                            "2024-01-01T00:00:00.000000Z\t\u2588\u2588\u2588\u2800\u2800\n" +
                            "2024-01-01T01:00:00.000000Z\t\u2800\u2800\u2591\u2591\u2591\n",
                    """
                            SELECT ts, ohlc_bar(o, h, l, c, mn, mx, 5) FROM (
                                SELECT ts, o, h, l, c,
                                       min(l) OVER () mn, max(h) OVER () mx
                                FROM (
                                    SELECT ts, first(price) o, max(price) h, min(price) l, last(price) c
                                    FROM t SAMPLE BY 1h
                                )
                            )
                            """
            );
        });
    }

    private String queryResultString(String sql) throws Exception {
        printSql(sql);
        return sink.toString();
    }
}
