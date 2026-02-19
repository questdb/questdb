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

import io.questdb.mp.WorkerPool;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class TwapGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testTwapAllNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tbl (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO tbl VALUES
                    (null, '2024-01-01T00:00:00.000000Z'),
                    (null, '2024-01-01T00:00:10.000000Z'),
                    (null, '2024-01-01T00:00:20.000000Z')
                    """);
            assertSql(
                    "twap\nnull\n",
                    "SELECT twap(price, ts) FROM tbl"
            );
        });
    }

    @Test
    public void testTwapAllSameTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tbl (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO tbl VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T00:00:00.000000Z'),
                    (30.0, '2024-01-01T00:00:00.000000Z')
                    """);
            // all same timestamp => simple average: (10 + 20 + 30) / 3 = 20.0
            assertSql(
                    "twap\n20.0\n",
                    "SELECT twap(price, ts) FROM tbl"
            );
        });
    }

    @Test
    public void testTwapBasic() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tbl (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO tbl VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T00:00:10.000000Z'),
                    (30.0, '2024-01-01T00:00:30.000000Z')
                    """);
            // weighted_sum = 10 * 10_000_000 + 20 * 20_000_000 = 500_000_000
            // total_duration = 30_000_000
            // twap = 500_000_000 / 30_000_000 = 16.666666666666668
            assertSql(
                    "twap\n16.666666666666668\n",
                    "SELECT twap(price, ts) FROM tbl"
            );
        });
    }

    @Test
    public void testTwapNoRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tbl (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            assertSql(
                    "twap\nnull\n",
                    "SELECT twap(price, ts) FROM tbl"
            );
        });
    }

    @Test
    public void testTwapNullTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tbl (price DOUBLE, ts TIMESTAMP)");
            execute("""
                    INSERT INTO tbl VALUES
                    (10.0, null),
                    (20.0, '2024-01-01T00:00:10.000000Z'),
                    (30.0, null),
                    (40.0, '2024-01-01T00:00:30.000000Z')
                    """);
            // only rows with ts 10s and 30s are considered
            // weighted_sum = 20 * 20_000_000 = 400_000_000
            // total_duration = 20_000_000
            // twap = 400_000_000 / 20_000_000 = 20.0
            assertSql(
                    "twap\n20.0\n",
                    "SELECT twap(price, ts) FROM tbl"
            );
        });
    }

    @Test
    public void testTwapParallelAllNullPrices() throws Exception {
        assertMemoryLeak((TestUtils.LeakProneCode) () -> {
            execute("""
                    CREATE TABLE tbl AS (
                        SELECT
                            rnd_symbol('A','B','C','D','E') sym,
                            null::DOUBLE price,
                            timestamp_sequence(0, 1000) ts
                        FROM long_sequence(100_000)
                    ) TIMESTAMP(ts)
                    """);
            try (WorkerPool pool = new WorkerPool(() -> 4)) {
                TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                    String sql = "SELECT sym, twap(price, ts) FROM tbl GROUP BY sym ORDER BY sym";
                    TestUtils.assertSql(engine, sqlExecutionContext, sql, sink,
                            """
                                    sym\ttwap
                                    A\tnull
                                    B\tnull
                                    C\tnull
                                    D\tnull
                                    E\tnull
                                    """);
                }, configuration, LOG);
            }
        });
    }

    @Test
    public void testTwapParallelChunky() throws Exception {
        assertMemoryLeak((TestUtils.LeakProneCode) () -> {
            execute("""
                    CREATE TABLE tbl AS (
                        SELECT
                            rnd_symbol('A','B','C','D','E') sym,
                            rnd_double() * 100 price,
                            timestamp_sequence(0, 1000) ts
                        FROM long_sequence(2_000_000)
                    ) TIMESTAMP(ts)
                    """);
            try (WorkerPool pool = new WorkerPool(() -> 4)) {
                TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                    String sql = "SELECT sym, twap(price, ts) FROM tbl GROUP BY sym ORDER BY sym";
                    TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
                }, configuration, LOG);
            }
        });
    }

    @Test
    public void testTwapParallelConstantPrice() throws Exception {
        assertMemoryLeak((TestUtils.LeakProneCode) () -> {
            execute("""
                    CREATE TABLE tbl AS (
                        SELECT
                            rnd_symbol('A','B','C') sym,
                            42.0 price,
                            timestamp_sequence(0, 1000) ts
                        FROM long_sequence(500_000)
                    ) TIMESTAMP(ts)
                    """);
            try (WorkerPool pool = new WorkerPool(() -> 4)) {
                TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                    String sql = "SELECT sym, twap(price, ts) FROM tbl GROUP BY sym ORDER BY sym";
                    // constant price => twap = 42.0 for all groups
                    TestUtils.assertSql(engine, sqlExecutionContext, sql, sink,
                            """
                                    sym\ttwap
                                    A\t42.0
                                    B\t42.0
                                    C\t42.0
                                    """);
                }, configuration, LOG);
            }
        });
    }

    @Test
    public void testTwapParallelMergeNullDestValidSrc() throws Exception {
        assertMemoryLeak((TestUtils.LeakProneCode) () -> {
            // First half has null prices, second half has valid prices.
            // Some chunks will be empty (null dest) that merge with non-empty src.
            execute("""
                    CREATE TABLE tbl AS (
                        SELECT
                            rnd_symbol('A','B','C','D','E') sym,
                            CASE WHEN x <= 1_000_000 THEN null ELSE rnd_double() * 100 END price,
                            timestamp_sequence(0, 1000) ts
                        FROM long_sequence(2_000_000)
                    ) TIMESTAMP(ts)
                    """);
            try (WorkerPool pool = new WorkerPool(() -> 4)) {
                TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                    String sql = "SELECT sym, twap(price, ts) FROM tbl GROUP BY sym ORDER BY sym";
                    TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
                }, configuration, LOG);
            }
        });
    }

    @Test
    public void testTwapParallelMergeValidDestNullSrc() throws Exception {
        assertMemoryLeak((TestUtils.LeakProneCode) () -> {
            // First half has valid prices, second half has null prices.
            // Some chunks will be non-empty (valid dest) merging with empty src.
            execute("""
                    CREATE TABLE tbl AS (
                        SELECT
                            rnd_symbol('A','B','C','D','E') sym,
                            CASE WHEN x > 500_000 THEN null ELSE rnd_double() * 100 END price,
                            timestamp_sequence(0, 1000) ts
                        FROM long_sequence(1_000_000)
                    ) TIMESTAMP(ts)
                    """);
            try (WorkerPool pool = new WorkerPool(() -> 4)) {
                TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                    String sql = "SELECT sym, twap(price, ts) FROM tbl GROUP BY sym ORDER BY sym";
                    TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
                }, configuration, LOG);
            }
        });
    }

    @Test
    public void testTwapParallelNoGroupBy() throws Exception {
        assertMemoryLeak((TestUtils.LeakProneCode) () -> {
            execute("""
                    CREATE TABLE tbl AS (
                        SELECT
                            rnd_double() * 100 price,
                            timestamp_sequence(0, 1000) ts
                        FROM long_sequence(1_000_000)
                    ) TIMESTAMP(ts)
                    """);
            try (WorkerPool pool = new WorkerPool(() -> 4)) {
                TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                    String sql = "SELECT twap(price, ts) FROM tbl";
                    TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
                }, configuration, LOG);
            }
        });
    }

    @Test
    public void testTwapParallelSingleGroup() throws Exception {
        assertMemoryLeak((TestUtils.LeakProneCode) () -> {
            // All rows belong to the same symbol group — exercises merge across chunks for a single key
            execute("""
                    CREATE TABLE tbl AS (
                        SELECT
                            'X' sym,
                            rnd_double() * 100 price,
                            timestamp_sequence(0, 1000) ts
                        FROM long_sequence(1_000_000)
                    ) TIMESTAMP(ts)
                    """);
            try (WorkerPool pool = new WorkerPool(() -> 4)) {
                TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                    String sql = "SELECT sym, twap(price, ts) FROM tbl GROUP BY sym";
                    TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
                }, configuration, LOG);
            }
        });
    }

    @Test
    public void testTwapParallelSmallDataset() throws Exception {
        assertMemoryLeak((TestUtils.LeakProneCode) () -> {
            // Very few rows — some parallel chunks may be completely empty
            execute("""
                    CREATE TABLE tbl AS (
                        SELECT
                            rnd_symbol('A','B') sym,
                            rnd_double() * 100 price,
                            timestamp_sequence(0, 1_000_000) ts
                        FROM long_sequence(10)
                    ) TIMESTAMP(ts)
                    """);
            try (WorkerPool pool = new WorkerPool(() -> 4)) {
                TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                    String sql = "SELECT sym, twap(price, ts) FROM tbl GROUP BY sym ORDER BY sym";
                    TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
                }, configuration, LOG);
            }
        });
    }

    @Test
    public void testTwapParallelSparseNulls() throws Exception {
        assertMemoryLeak((TestUtils.LeakProneCode) () -> {
            // Alternating null/non-null prices — exercises null skipping at chunk boundaries
            execute("""
                    CREATE TABLE tbl AS (
                        SELECT
                            rnd_symbol('A','B','C','D','E') sym,
                            CASE WHEN x % 2 = 0 THEN null ELSE rnd_double() * 100 END price,
                            timestamp_sequence(0, 1000) ts
                        FROM long_sequence(1_000_000)
                    ) TIMESTAMP(ts)
                    """);
            try (WorkerPool pool = new WorkerPool(() -> 4)) {
                TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                    String sql = "SELECT sym, twap(price, ts) FROM tbl GROUP BY sym ORDER BY sym";
                    TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
                }, configuration, LOG);
            }
        });
    }

    @Test
    public void testTwapParallelWithManyGroups() throws Exception {
        assertMemoryLeak((TestUtils.LeakProneCode) () -> {
            // Many distinct keys — stresses the merge map
            execute("""
                    CREATE TABLE tbl AS (
                        SELECT
                            rnd_symbol_zipf(200, 1.5) sym,
                            rnd_double() * 100 price,
                            timestamp_sequence(0, 1000) ts
                        FROM long_sequence(1_000_000)
                    ) TIMESTAMP(ts)
                    """);
            try (WorkerPool pool = new WorkerPool(() -> 4)) {
                TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                    String sql = "SELECT sym, twap(price, ts) FROM tbl GROUP BY sym ORDER BY sym";
                    TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
                }, configuration, LOG);
            }
        });
    }

    @Test
    public void testTwapSingleRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tbl (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO tbl VALUES (42.5, '2024-01-01T00:00:00.000000Z')");
            // single row => total_duration = 0, fallback to price_sum / count = 42.5
            assertSql(
                    "twap\n42.5\n",
                    "SELECT twap(price, ts) FROM tbl"
            );
        });
    }

    @Test
    public void testTwapSomeNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tbl (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO tbl VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (null, '2024-01-01T00:00:10.000000Z'),
                    (30.0, '2024-01-01T00:00:20.000000Z')
                    """);
            // null price at 10s is skipped
            // weighted_sum = 10.0 * 20_000_000 = 200_000_000
            // total_duration = 20_000_000
            // twap = 200_000_000 / 20_000_000 = 10.0
            assertSql(
                    "twap\n10.0\n",
                    "SELECT twap(price, ts) FROM tbl"
            );
        });
    }

    @Test
    public void testTwapWithGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tbl (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO tbl VALUES
                    ('A', 10.0, '2024-01-01T00:00:00.000000Z'),
                    ('B', 100.0, '2024-01-01T00:00:00.000000Z'),
                    ('A', 20.0, '2024-01-01T00:00:10.000000Z'),
                    ('B', 200.0, '2024-01-01T00:00:20.000000Z'),
                    ('A', 30.0, '2024-01-01T00:00:30.000000Z')
                    """);
            // Group A: weighted_sum = 10*10_000_000 + 20*20_000_000 = 500_000_000, dur = 30_000_000, twap = 16.666666666666668
            // Group B: weighted_sum = 100*20_000_000 = 2_000_000_000, dur = 20_000_000, twap = 100.0
            assertSql(
                    """
                            sym\ttwap
                            A\t16.666666666666668
                            B\t100.0
                            """,
                    "SELECT sym, twap(price, ts) FROM tbl ORDER BY sym"
            );
        });
    }

    @Test
    public void testTwapWithSampleBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tbl (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO tbl VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T00:00:10.000000Z'),
                    (30.0, '2024-01-01T00:00:30.000000Z'),
                    (100.0, '2024-01-01T00:01:00.000000Z'),
                    (200.0, '2024-01-01T00:01:20.000000Z')
                    """);
            // Bucket 1 (00:00:00 - 00:01:00): same as testTwapBasic = 16.666666666666668
            // Bucket 2 (00:01:00 - 00:02:00): weighted_sum = 100*20_000_000 = 2_000_000_000, dur = 20_000_000, twap = 100.0
            assertSql(
                    """
                            ts\ttwap
                            2024-01-01T00:00:00.000000Z\t16.666666666666668
                            2024-01-01T00:01:00.000000Z\t100.0
                            """,
                    "SELECT ts, twap(price, ts) FROM tbl SAMPLE BY 1m"
            );
        });
    }

    @Test
    public void testTwapWithSampleByFillPrev() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tbl (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO tbl VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T00:00:10.000000Z'),
                    (30.0, '2024-01-01T00:00:30.000000Z'),
                    (100.0, '2024-01-01T00:02:00.000000Z'),
                    (200.0, '2024-01-01T00:02:20.000000Z')
                    """);
            // Bucket 00:00 has data: twap = 16.666666666666668
            // Bucket 00:01 is empty: FILL(prev) => 16.666666666666668
            // Bucket 00:02 has data: twap = 100.0
            assertSql(
                    """
                            ts\ttwap
                            2024-01-01T00:00:00.000000Z\t16.666666666666668
                            2024-01-01T00:01:00.000000Z\t16.666666666666668
                            2024-01-01T00:02:00.000000Z\t100.0
                            """,
                    "SELECT ts, twap(price, ts) FROM tbl SAMPLE BY 1m FILL(prev)"
            );
        });
    }
}
