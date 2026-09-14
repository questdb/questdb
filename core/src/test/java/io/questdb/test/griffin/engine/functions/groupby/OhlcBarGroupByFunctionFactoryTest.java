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
import io.questdb.mp.WorkerPool;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class OhlcBarGroupByFunctionFactoryTest extends AbstractCairoTest {

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
                            "\u2500\u2500\u2591\u2591\u2591\u2591\u2591\u2591\u2500\u2500\n",
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
                            "\u2500\u2500\u2588\u2588\u2588\u2588\u2588\u2588\u2500\u2500\n",
                    "SELECT ohlc_bar(price, 0, 100, 10) FROM t"
            );
        });
    }

    @Test
    public void testAggregateCTE() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                    (20.0, '2024-01-01T00:00:00.000000Z'),
                    (80.0, '2024-01-01T00:01:00.000000Z')
                    """);
            assertSql(
                    "ohlc_bar\n" +
                            "\u2800\u2800\u2588\u2588\u2588\u2588\u2588\u2588\u2800\u2800\n",
                    """
                            WITH prices AS (
                                SELECT ts, price FROM t
                            )
                            SELECT ohlc_bar(price, 0, 100, 10) FROM prices
                            """
            );
        });
    }

    @Test
    public void testAggregateCrossJoinBounds() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                    (20.0, '2024-01-01T00:00:00.000000Z'),
                    (50.0, '2024-01-01T00:30:00.000000Z'),
                    (80.0, '2024-01-01T01:00:00.000000Z'),
                    (60.0, '2024-01-01T01:30:00.000000Z')
                    """);
            // Cross join bounds come from data: min=20, max=80
            assertSql(
                    "ts\tohlc_bar\n" +
                            "2024-01-01T00:00:00.000000Z\t\u2588\u2588\u2588\u2800\u2800\n" +
                            "2024-01-01T01:00:00.000000Z\t\u2800\u2800\u2800\u2591\u2591\n",
                    """
                            SELECT ts, ohlc_bar(price, lo, hi, 5) FROM t
                            CROSS JOIN (
                                SELECT min(price) AS lo, max(price) AS hi FROM t
                            )
                            SAMPLE BY 1h
                            """
            );
        });
    }

    @Test
    public void testAggregateDeclareVariables() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (20.0, '2024-01-01T00:00:00.000000Z'),
                    (80.0, '2024-01-01T00:01:00.000000Z')
                    """);
            assertSql(
                    "ohlc_bar\n" +
                            "\u2800\u2800\u2588\u2588\u2588\u2588\u2588\u2588\u2800\u2800\n",
                    """
                            DECLARE @lo := 0.0, @hi := 100.0
                            SELECT ohlc_bar(price, @lo, @hi, 10) FROM t
                            """
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
                            "\u2500\u2500\u2500\u2500\u2500\u2502\u2500\u2500\u2500\u2500\n",
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
    public void testAggregateFillLinearRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:10:00.000000Z'),
                    (20.0, '2024-01-01T01:10:00.000000Z')
                    """);
            assertException(
                    "SELECT ts, ohlc_bar(price, 0, 100, 5) FROM t SAMPLE BY 1h FILL(LINEAR)",
                    11,
                    "support for LINEAR fill is not yet implemented"
            );
        });
    }

    @Test
    public void testAggregateFillNone() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:10:00.000000Z'),
                    (20.0, '2024-01-01T00:20:00.000000Z'),
                    (30.0, '2024-01-01T02:10:00.000000Z'),
                    (40.0, '2024-01-01T02:20:00.000000Z')
                    """);
            // FILL(NONE) skips hour 01:00 entirely
            assertSql(
                    "ts\tohlc_bar\n" +
                            "2024-01-01T00:00:00.000000Z\t\u2800\u2588\u2588\u2800\u2800\n" +
                            "2024-01-01T02:00:00.000000Z\t\u2800\u2800\u2588\u2588\u2800\n",
                    "SELECT ts, ohlc_bar(price, 0, 50, 5) FROM t SAMPLE BY 1h FILL(NONE)"
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
            // Bounds 0-50, width=5. H00: O=10,C=20. H02: O=30,C=40.
            assertSql(
                    "ts\tohlc_bar\n" +
                            "2024-01-01T00:00:00.000000Z\t\u2800\u2588\u2588\u2800\u2800\n" +
                            "2024-01-01T01:00:00.000000Z\t\n" +
                            "2024-01-01T02:00:00.000000Z\t\u2800\u2800\u2588\u2588\u2800\n",
                    "SELECT ts, ohlc_bar(price, 0, 50, 5) FROM t SAMPLE BY 1h FILL(NULL)"
            );
        });
    }

    @Test
    public void testAggregateFillPrev() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:10:00.000000Z'),
                    (20.0, '2024-01-01T00:20:00.000000Z'),
                    (30.0, '2024-01-01T02:10:00.000000Z'),
                    (40.0, '2024-01-01T02:20:00.000000Z')
                    """);
            // FILL(PREV) copies hour 00's candle into hour 01
            assertSql(
                    "ts\tohlc_bar\n" +
                            "2024-01-01T00:00:00.000000Z\t\u2800\u2588\u2588\u2800\u2800\n" +
                            "2024-01-01T01:00:00.000000Z\t\u2800\u2588\u2588\u2800\u2800\n" +
                            "2024-01-01T02:00:00.000000Z\t\u2800\u2800\u2588\u2588\u2800\n",
                    "SELECT ts, ohlc_bar(price, 0, 50, 5) FROM t SAMPLE BY 1h FILL(PREV)"
            );
        });
    }

    @Test
    public void testAggregateFloatColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price FLOAT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (20.0, '2024-01-01T00:00:00.000000Z'),
                    (80.0, '2024-01-01T00:01:00.000000Z')
                    """);
            assertSql(
                    "ohlc_bar\n" +
                            "\u2800\u2800\u2588\u2588\u2588\u2588\u2588\u2588\u2800\u2800\n",
                    "SELECT ohlc_bar(price, 0, 100, 10) FROM t"
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
                            "A\t\u2588\u2588\u2588\u2588\u2588\n" +
                            "B\t\u2591\u2591\u2591\u2591\u2591\n",
                    "SELECT symbol, ohlc_bar(price, 0, 100, 5) FROM t ORDER BY symbol"
            );
        });
    }

    @Test
    public void testAggregateIntColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (20, '2024-01-01T00:00:00.000000Z'),
                    (100, '2024-01-01T00:01:00.000000Z'),
                    (0, '2024-01-01T00:02:00.000000Z'),
                    (80, '2024-01-01T00:03:00.000000Z')
                    """);
            assertSql(
                    "ohlc_bar\n" +
                            "\u2500\u2500\u2588\u2588\u2588\u2588\u2588\u2588\u2500\u2500\n",
                    "SELECT ohlc_bar(price, 0, 100, 10) FROM t"
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
                            "\u2500\u2500\u2588\u2588\u2588\u2588\u2588\u2588\u2500\u2500 O:20.0 H:100.0 L:0.0 C:80.0\n",
                    "SELECT ohlc_bar_labels(price, 0, 100, 10) FROM t"
            );
        });
    }

    @Test
    public void testAggregateLateralJoinPerSymbol() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, symbol SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, 'A', '2024-01-01T00:00:00.000000Z'),
                    (90.0, 'A', '2024-01-01T01:00:00.000000Z'),
                    (50.0, 'B', '2024-01-01T00:00:00.000000Z'),
                    (60.0, 'B', '2024-01-01T01:00:00.000000Z')
                    """);
            // A: O=10,C=90, bounds 10-90 -> full bullish
            // B: O=50,C=60, bounds 50-60 -> full bullish
            // Each symbol scaled against its own bounds via lateral join
            assertSql(
                    "symbol\tohlc_bar\n" +
                            "A\t\u2588\u2588\u2588\u2588\u2588\n" +
                            "B\t\u2588\u2588\u2588\u2588\u2588\n",
                    """
                            SELECT symbol, ohlc_bar(price, lo, hi, 5) FROM t
                            JOIN LATERAL (
                                SELECT min(price) AS lo, max(price) AS hi
                                FROM t t2 WHERE t.symbol = t2.symbol
                            )
                            ORDER BY symbol
                            """
            );
        });
    }

    @Test
    public void testAggregateLongColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (20, '2024-01-01T00:00:00.000000Z'),
                    (80, '2024-01-01T00:01:00.000000Z')
                    """);
            assertSql(
                    "ohlc_bar\n" +
                            "\u2800\u2800\u2588\u2588\u2588\u2588\u2588\u2588\u2800\u2800\n",
                    "SELECT ohlc_bar(price, 0, 100, 10) FROM t"
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
    public void testAggregateNullBoundsMaxThrows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES (50.0, '2024-01-01T00:00:00.000000Z')");
            assertException(
                    "SELECT ohlc_bar(price, 0, NULL, 10) FROM t",
                    26,
                    "bounds must not be NULL"
            );
        });
    }

    @Test
    public void testAggregateNullBoundsThrows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES (50.0, '2024-01-01T00:00:00.000000Z')");
            assertException(
                    "SELECT ohlc_bar(price, NULL, 100, 10) FROM t",
                    23,
                    "bounds must not be NULL"
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
        // ORDER BY on ohlc_bar output exercises A/B flyweight independence:
        // the sort comparator reads getVarcharA and getVarcharB from the
        // same function instance for different records.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, symbol SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, 'A', '2024-01-01T00:00:00.000000Z'),
                    (90.0, 'A', '2024-01-01T01:00:00.000000Z'),
                    (90.0, 'B', '2024-01-01T00:00:00.000000Z'),
                    (10.0, 'B', '2024-01-01T01:00:00.000000Z')
                    """);
            // A: bullish (>>>>> in sort order), B: bearish (@@@@@ in sort order).
            // Bearish ░ (U+2591) sorts after bullish █ (U+2588) bytewise,
            // so B comes after A in ascending ORDER BY.
            assertSql(
                    "symbol\tbar\n" +
                            "A\t\u2588\u2588\u2588\u2588\u2588\n" +
                            "B\t\u2591\u2591\u2591\u2591\u2591\n",
                    "SELECT symbol, ohlc_bar(price, 0, 100, 5) bar FROM t ORDER BY bar"
            );
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
    public void testAggregateParallelMergeReconcilesBounds() throws Exception {
        // Per-row bounds (price - 100, price + 100) vary across rows.
        // Run under a 4-worker pool so multiple shards process different
        // rows and store different scaleMin/scaleMax in slots +6/+7.
        // merge() must widen to the widest range. If reconciliation were
        // broken, results would differ across runs.
        //
        // Determinism is the regression guard: non-determinism is the
        // symptom when merge fails to reconcile bounds.
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

            try (WorkerPool pool = new WorkerPool(() -> 4)) {
                TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                    String sql = "SELECT symbol, ohlc_bar(price, price - 100, price + 100, 20) FROM t ORDER BY symbol";

                    // Verify 4-worker Async Group By path
                    sink.clear();
                    TestUtils.printSql(engine, sqlExecutionContext, "EXPLAIN " + sql, sink);
                    String plan = sink.toString();
                    Assert.assertTrue("Expected workers: 4", plan.contains("workers: 4"));

                    // Two runs must produce identical output
                    TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
                }, configuration, LOG);
            }
        });
    }

    @Test
    public void testAggregatePerRowInvertedBoundsThrows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, lo DOUBLE, hi DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (50.0, 100.0, 0.0, '2024-01-01T00:00:00.000000Z'),
                    (60.0, 0.0, 100.0, '2024-01-01T00:01:00.000000Z')
                    """);
            assertException(
                    "SELECT ohlc_bar(price, lo, hi, 10) FROM t",
                    23,
                    "min must not exceed max"
            );
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

    @Test
    public void testAggregateShortColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price SHORT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (20, '2024-01-01T00:00:00.000000Z'),
                    (80, '2024-01-01T00:01:00.000000Z')
                    """);
            assertSql(
                    "ohlc_bar\n" +
                            "\u2800\u2800\u2588\u2588\u2588\u2588\u2588\u2588\u2800\u2800\n",
                    "SELECT ohlc_bar(price, 0, 100, 10) FROM t"
            );
        });
    }

    @Test
    public void testAggregateSubquery() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                    (20.0, '2024-01-01T00:00:00.000000Z'),
                    (80.0, '2024-01-01T00:01:00.000000Z')
                    """);
            assertSql(
                    "ohlc_bar\n" +
                            "\u2800\u2800\u2588\u2588\u2588\u2588\u2588\u2588\u2800\u2800\n",
                    """
                            SELECT ohlc_bar(price, 0, 100, 10) FROM (
                                SELECT ts, price FROM t
                            )
                            """
            );
        });
    }

    @Test
    public void testAggregateWidthExceedsMaxThrows() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_STR_FUNCTION_BUFFER_MAX_SIZE, 30);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES (50.0, '2024-01-01T00:00:00.000000Z')");
            assertException(
                    "SELECT ohlc_bar(price, 0, 100, 100) FROM t",
                    31,
                    "breached memory limit"
            );
        });
    }

    @Test
    public void testAggregateWidthZeroThrows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES (50.0, '2024-01-01T00:00:00.000000Z')");
            assertException(
                    "SELECT ohlc_bar(price, 0, 100, 0) FROM t",
                    31,
                    "width must be a positive integer"
            );
        });
    }

    @Test
    public void testLabelsExceedsMaxBufferSize() throws Exception {
        // Set buffer limit to 50 bytes. Width=1 produces 3 bar bytes.
        // Labels add " O:50.0 H:50.0 L:50.0 C:50.0" = ~32 bytes.
        // Total ~35 bytes fits. But with a very long number like
        // 12345.6789012345, labels exceed the 50-byte limit.
        setProperty(PropertyKey.CAIRO_SQL_STR_FUNCTION_BUFFER_MAX_SIZE, 50);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES (12345.6789012345, '2024-01-01T00:00:00.000000Z')");
            // Width=1 passes effectiveWidth() check (3 bytes < 50).
            // But labels with long numbers push total past 50 bytes.
            assertException(
                    "SELECT ohlc_bar_labels(price, 0, 100000, 1) FROM t",
                    23,
                    "breached memory limit"
            );
        });
    }

    @Test
    public void testScalarBasicBearish() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "ohlc_bar\n" +
                        "\u2500\u2500\u2591\u2591\u2591\u2591\u2591\u2591\u2500\u2500\n",
                "SELECT ohlc_bar(80, 100, 0, 20, 0, 100, 10)"
        ));
    }

    @Test
    public void testScalarBasicBullish() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "ohlc_bar\n" +
                        "\u2500\u2500\u2588\u2588\u2588\u2588\u2588\u2588\u2500\u2500\n",
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
                        "\u2500\u2500\u2500\u2500\u2500\u2502\u2500\u2500\u2500\u2500\n",
                "SELECT ohlc_bar(50, 100, 0, 50, 0, 100, 10)"
        ));
    }

    @Test
    public void testScalarInvertedBoundsThrows() throws Exception {
        // Position 32 points at the "100" (min arg, index 4) in the query
        assertMemoryLeak(() -> assertException(
                "SELECT ohlc_bar(50, 100, 0, 50, 100, 0, 10)",
                32,
                "min must not exceed max"
        ));
    }

    @Test
    public void testScalarInvertedLowHighThrows() throws Exception {
        // low > high is invalid OHLC data, should throw not return null
        assertMemoryLeak(() -> assertException(
                "SELECT ohlc_bar(50, 40, 60, 55, 0, 100, 10)",
                24,
                "low must not exceed high"
        ));
    }

    @Test
    public void testScalarLabels() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "ohlc_bar_labels\n" +
                        "\u2500\u2500\u2588\u2588\u2588\u2588\u2588\u2588\u2500\u2500 O:20.0 H:100.0 L:0.0 C:80.0\n",
                "SELECT ohlc_bar_labels(20, 100, 0, 80, 0, 100, 10)"
        ));
    }

    @Test
    public void testScalarLabelsInvertedLowHighThrows() throws Exception {
        assertMemoryLeak(() -> assertException(
                "SELECT ohlc_bar_labels(50, 40, 60, 55, 0, 100, 10)",
                31,
                "low must not exceed high"
        ));
    }

    @Test
    public void testScalarNullBoundsMaxThrows() throws Exception {
        assertMemoryLeak(() -> assertException(
                "SELECT ohlc_bar(50, 100, 0, 80, 0, NULL, 10)",
                35,
                "bounds must not be NULL"
        ));
    }

    @Test
    public void testScalarNullBoundsThrows() throws Exception {
        assertMemoryLeak(() -> assertException(
                "SELECT ohlc_bar(50, 100, 0, 80, NULL, 100, 10)",
                32,
                "bounds must not be NULL"
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
    public void testScalarWidthExceedsMaxThrows() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_STR_FUNCTION_BUFFER_MAX_SIZE, 30);
        assertMemoryLeak(() -> assertException(
                "SELECT ohlc_bar(50, 100, 0, 80, 0, 100, 100)",
                40,
                "breached memory limit"
        ));
    }

    @Test
    public void testScalarWidthZeroThrows() throws Exception {
        assertMemoryLeak(() -> assertException(
                "SELECT ohlc_bar(50, 100, 0, 80, 0, 100, 0)",
                40,
                "width must be a positive integer"
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
                            "2024-01-01T01:00:00.000000Z\t\u2800\u2800\u2800\u2591\u2591\n",
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
