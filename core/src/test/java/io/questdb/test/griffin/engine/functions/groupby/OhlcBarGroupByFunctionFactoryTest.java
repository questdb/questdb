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

import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class OhlcBarGroupByFunctionFactoryTest extends AbstractCairoTest {

    // --- Aggregate ohlc_bar(price, min, max [, width]) tests ---

    @Test
    public void testAggregateBearish() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (80.0, '2024-01-01T00:00:00.000000Z'),
                    (100.0, '2024-01-01T00:01:00.000000Z'),
                    (0.0, '2024-01-01T00:02:00.000000Z'),
                    (20.0, '2024-01-01T00:03:00.000000Z')
                    """);
            // O=80, H=100, L=0, C=20, range 0-100, width=10
            assertSql(
                    "ohlc_bar\n" +
                            "\u2500\u2591\u2591\u2591\u2591\u2591\u2591\u2591\u2500\u2500\n",
                    "SELECT ohlc_bar(price, 0, 100, 10) FROM t"
            );
        });
    }

    @Test
    public void testAggregateBullish() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (20.0, '2024-01-01T00:00:00.000000Z'),
                    (100.0, '2024-01-01T00:01:00.000000Z'),
                    (0.0, '2024-01-01T00:02:00.000000Z'),
                    (80.0, '2024-01-01T00:03:00.000000Z')
                    """);
            assertSql(
                    "ohlc_bar\n" +
                            "\u2500\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2500\u2500\n",
                    "SELECT ohlc_bar(price, 0, 100, 10) FROM t"
            );
        });
    }

    @Test
    public void testAggregateDoji() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (50.0, '2024-01-01T00:00:00.000000Z'),
                    (100.0, '2024-01-01T00:01:00.000000Z'),
                    (0.0, '2024-01-01T00:02:00.000000Z'),
                    (50.0, '2024-01-01T00:03:00.000000Z')
                    """);
            assertSql(
                    "ohlc_bar\n" +
                            "\u2500\u2500\u2500\u2500\u2502\u2500\u2500\u2500\u2500\u2500\n",
                    "SELECT ohlc_bar(price, 0, 100, 10) FROM t"
            );
        });
    }

    @Test
    public void testAggregateEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            assertSql(
                    "ohlc_bar\n\n",
                    "SELECT ohlc_bar(price, 0, 100, 10) FROM t"
            );
        });
    }

    @Test
    public void testAggregateFillNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:10:00.000000Z'),
                    (20.0, '2024-01-01T00:20:00.000000Z'),
                    (30.0, '2024-01-01T02:10:00.000000Z'),
                    (40.0, '2024-01-01T02:20:00.000000Z')
                    """);
            // Bounds 0-50, width=5
            // Bounds 0-50, width=5. H00: O=10(pos 0),C=20(pos 1). H02: O=30(pos 2),C=40(pos 3).
            assertSql(
                    "ts\tohlc_bar\n" +
                            "2024-01-01T00:00:00.000000Z\t\u2588\u2588\u2800\u2800\u2800\n" +
                            "2024-01-01T01:00:00.000000Z\t\n" +
                            "2024-01-01T02:00:00.000000Z\t\u2800\u2800\u2588\u2588\u2800\n",
                    "SELECT ts, ohlc_bar(price, 0, 50, 5) FROM t SAMPLE BY 1h FILL(NULL)"
            );
        });
    }

    @Test
    public void testAggregateGroupBySymbol() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, symbol SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, 'A', '2024-01-01T00:00:00.000000Z'),
                    (90.0, 'A', '2024-01-01T01:00:00.000000Z'),
                    (90.0, 'B', '2024-01-01T00:00:00.000000Z'),
                    (10.0, 'B', '2024-01-01T01:00:00.000000Z')
                    """);
            // Bounds 0-100, width=5. A: O=10(pos 0),C=90(pos 3). B: O=90(pos 3),C=10(pos 0).
            assertSql(
                    "symbol\tohlc_bar\n" +
                            "A\t\u2588\u2588\u2588\u2588\u2800\n" +
                            "B\t\u2591\u2591\u2591\u2591\u2800\n",
                    "SELECT symbol, ohlc_bar(price, 0, 100, 5) FROM t ORDER BY symbol"
            );
        });
    }

    @Test
    public void testAggregateLabels() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (20.0, '2024-01-01T00:00:00.000000Z'),
                    (100.0, '2024-01-01T00:01:00.000000Z'),
                    (0.0, '2024-01-01T00:02:00.000000Z'),
                    (80.0, '2024-01-01T00:03:00.000000Z')
                    """);
            assertSql(
                    "ohlc_bar_labels\n" +
                            "\u2500\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2500\u2500 O:20.0 H:100.0 L:0.0 C:80.0\n",
                    "SELECT ohlc_bar_labels(price, 0, 100, 10) FROM t"
            );
        });
    }

    @Test
    public void testAggregateMixedNulls() throws Exception {
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
            assertSql(
                    "ohlc_bar\n" +
                            "\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\n",
                    "SELECT ohlc_bar(price, 20, 80, 10) FROM t"
            );
        });
    }

    @Test
    public void testAggregateNullInput() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES (NULL, '2024-01-01T00:00:00.000000Z')");
            assertSql(
                    "ohlc_bar\n\n",
                    "SELECT ohlc_bar(price, 0, 100, 10) FROM t"
            );
        });
    }

    @Test
    public void testAggregateOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, symbol SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, 'A', '2024-01-01T00:00:00.000000Z'),
                    (90.0, 'A', '2024-01-01T01:00:00.000000Z'),
                    (90.0, 'B', '2024-01-01T00:00:00.000000Z'),
                    (10.0, 'B', '2024-01-01T01:00:00.000000Z')
                    """);
            String result = queryResultString("SELECT symbol, ohlc_bar(price, 0, 100, 5) bar FROM t ORDER BY bar");
            Assert.assertTrue(result.contains("A"));
            Assert.assertTrue(result.contains("B"));
        });
    }

    @Test
    public void testAggregateParallelDeterminism() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, symbol SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t
                    SELECT
                        x % 100 + rnd_double() * 10,
                        rnd_symbol('A', 'B', 'C', 'D'),
                        dateadd('s', x::INT, '2024-01-01T00:00:00.000000Z')
                    FROM long_sequence(100_000)
                    """);
            String result1 = queryResultString("SELECT symbol, ohlc_bar(price, 0, 120, 20) FROM t ORDER BY symbol");
            String result2 = queryResultString("SELECT symbol, ohlc_bar(price, 0, 120, 20) FROM t ORDER BY symbol");
            Assert.assertEquals(result1, result2);
        });
    }

    @Test
    public void testAggregateSampleByHourly() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (50.0, '2024-01-01T00:30:00.000000Z'),
                    (80.0, '2024-01-01T01:00:00.000000Z'),
                    (60.0, '2024-01-01T01:30:00.000000Z')
                    """);
            // Bounds 0-100, width=5
            assertSql(
                    "ts\tohlc_bar\n" +
                            "2024-01-01T00:00:00.000000Z\t\u2588\u2588\u2588\u2800\u2800\n" +
                            "2024-01-01T01:00:00.000000Z\t\u2800\u2800\u2591\u2591\u2800\n",
                    "SELECT ts, ohlc_bar(price, 0, 100, 5) FROM t SAMPLE BY 1h"
            );
        });
    }

    // --- Scalar ohlc_bar(open, high, low, close, min, max [, width]) tests ---

    @Test
    public void testScalarBasicBearish() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "ohlc_bar\n" +
                        "\u2500\u2591\u2591\u2591\u2591\u2591\u2591\u2591\u2500\u2500\n",
                "SELECT ohlc_bar(80, 100, 0, 20, 0, 100, 10)"
        ));
    }

    @Test
    public void testScalarBasicBullish() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "ohlc_bar\n" +
                        "\u2500\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2500\u2500\n",
                "SELECT ohlc_bar(20, 100, 0, 80, 0, 100, 10)"
        ));
    }

    @Test
    public void testScalarDefaultWidth() throws Exception {
        assertMemoryLeak(() -> {
            String result = queryResultString("SELECT ohlc_bar(20, 100, 0, 80, 0, 100)");
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
