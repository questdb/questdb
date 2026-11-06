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

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.test.TestFaultFunctionFactory;
import io.questdb.griffin.engine.functions.test.TestLatchedCounterFunctionFactory;
import io.questdb.griffin.engine.table.AsyncGroupingSetsRecordCursorFactory;
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.model.QueryModelWrapper;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class GroupingSetsTest extends AbstractCairoTest {

    @Test
    public void testAllocationFailureReleasesMemory() throws Exception {
        // Caps native memory just above what is in use, at compile time and after it, and raises
        // the cap step by step, so an allocation fails at every point where the grouping sets
        // factory and cursor allocate. Each failure must be a clean out-of-memory error;
        // assertMemoryLeak then checks that nothing stayed allocated.
        // The filtered query takes the filter over into the parallel factory, and trim() allocates a
        // native sink when it compiles, so a failure that lands in the factory must free it too.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v LONG, s VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t SELECT (x % 50)::INT, (x % 7)::INT, x, ' k' || (x % 3), (x * 60_000_000)::TIMESTAMP FROM long_sequence(5_000)");
            final String cubeQuery = "SELECT a, b, grouping_id(a, b) g, sum(v) FROM t GROUP BY CUBE(a, b)";
            final String filteredQuery = "SELECT trim(s) k, a, sum(v) sv FROM t WHERE v > 10 GROUP BY k, ROLLUP(a)";
            for (int i = 0; i < 2; i++) {
                sqlExecutionContext.setParallelGroupByEnabled(i == 1);
                for (String query : new String[]{cubeQuery, filteredQuery}) {
                    int failures = 0;
                    int successes = 0;
                    for (int isArmedBeforeCompile = 0; isArmedBeforeCompile < 2; isArmedBeforeCompile++) {
                        for (long slack = 0; slack <= 512 * 1024; slack += 4 * 1024) {
                            final long savedLimit = Unsafe.getRssMemLimit();
                            try {
                                if (isArmedBeforeCompile == 1) {
                                    Unsafe.setRssMemLimit(Unsafe.getRssMemUsed() + slack);
                                }
                                try (RecordCursorFactory factory = select(query)) {
                                    if (isArmedBeforeCompile == 0) {
                                        Unsafe.setRssMemLimit(Unsafe.getRssMemUsed() + slack);
                                    }
                                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                                        while (cursor.hasNext()) {
                                            // drain
                                        }
                                    }
                                }
                                successes++;
                            } catch (CairoException e) {
                                Assert.assertTrue("expected out of memory, got: " + e.getFlyweightMessage(), e.isOutOfMemory());
                                failures++;
                            } finally {
                                Unsafe.setRssMemLimit(savedLimit);
                            }
                        }
                    }
                    Assert.assertTrue("the sweep never failed an allocation: " + query, failures > 0);
                    Assert.assertTrue("the sweep never let the query finish: " + query, successes > 0);
                }
                // The engine still answers the queries after the failures.
                assertQuery("SELECT count() n, sum(s) s FROM (" + cubeQuery.replace("sum(v)", "sum(v) s") + ")")
                        .withContext(sqlExecutionContext)
                        .noRandomAccess()
                        .expectSize()
                        .returns("n\ts\n408\t50010000\n");
                assertSqlCursors(
                        "SELECT * FROM (SELECT trim(s) k, a, sum(v) sv FROM t WHERE v > 10 GROUP BY k, a " +
                                "UNION ALL SELECT trim(s) k, NULL::INT a, sum(v) sv FROM t WHERE v > 10 GROUP BY k) ORDER BY k, a",
                        filteredQuery + " ORDER BY k, a"
                );
            }
        });
    }

    @Test
    public void testAsyncGroupingSets() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v DOUBLE)");
            sqlExecutionContext.setParallelGroupByEnabled(true);
            String query = "SELECT a, b, GROUPING_ID(a, b) AS gid, SUM(v) FROM t GROUP BY GROUPING SETS ((a, b), (a), ()) ORDER BY gid, a, b";
            assertQuery(query)
                    .withContext(sqlExecutionContext)
                    .assertsPlanContaining("Async Group By");
            assertQuery(query)
                    .withContext(sqlExecutionContext)
                    .expectSize()
                    .returns("a\tb\tgid\tSUM\nnull\tnull\t3\tnull\n");
            execute("INSERT INTO t VALUES (1, 10, 3), (1, 20, 4), (2, 10, 5)");
            assertQuery(query)
                    .withContext(sqlExecutionContext)
                    .assertsPlanContaining("Async Group By");
            assertQuery(query)
                    .withContext(sqlExecutionContext)
                    .expectSize()
                    .returns("a\tb\tgid\tSUM\n"
                            + "1\t10\t0\t3.0\n"
                            + "1\t20\t0\t4.0\n"
                            + "2\t10\t0\t5.0\n"
                            + "1\tnull\t1\t7.0\n"
                            + "2\tnull\t1\t5.0\n"
                            + "null\tnull\t3\t12.0\n");
        });
    }

    @Test
    public void testAsyncGroupingSetsGroupingOfNonKeyReleasesKeyFunctions() throws Exception {
        // trim(s) is a key function that holds native memory, and the parallel factory clones it
        // per worker. GROUPING(c) names a column that is not a key, so the factory rejects the
        // query while it is being built, and must free the key functions it was handed.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s VARCHAR, b INT, c INT, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES (' x ', 1, 7, 2.0, '2024-01-01'), ('y', 2, 8, 3.0, '2024-01-02')");
            sqlExecutionContext.setParallelGroupByEnabled(true);
            String query = "SELECT trim(s) k, b, GROUPING(c) g, SUM(v) FROM t GROUP BY trim(s), ROLLUP(b)";
            for (int i = 0; i < 3; i++) {
                assertException(query, query.indexOf("GROUPING(c)") + 9, "argument must be a grouping key column");
            }
            String validQuery = "SELECT trim(s) k, b, GROUPING(b) g, SUM(v) s FROM t GROUP BY trim(s), ROLLUP(b) ORDER BY k, g";
            assertQuery(validQuery)
                    .withContext(sqlExecutionContext)
                    .assertsPlanContaining("Async Group By");
            assertQuery(validQuery)
                    .withContext(sqlExecutionContext)
                    .expectSize()
                    .returns("""
                            k\tb\tg\ts
                            x\t1\t0\t2.0
                            x\tnull\t1\t2.0
                            y\t2\t0\t3.0
                            y\tnull\t1\t3.0
                            """);
        });
    }

    @Test
    public void testAsyncGroupingSetsMultipleFrames() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t SELECT (x % 4)::INT, (x % 7)::INT, x, ((x - 1) / 700 * 86_400_000_000L)::TIMESTAMP FROM long_sequence(2_800)");
            sqlExecutionContext.setParallelGroupByEnabled(true);
            String groupedQuery = "SELECT a, b, GROUPING_ID(a, b) AS gid, SUM(x) AS n FROM t GROUP BY GROUPING SETS ((a, b), (a), ())";
            assertQuery(groupedQuery)
                    .withContext(sqlExecutionContext)
                    .assertsPlanContaining("Async Group By");
            assertQuery("SELECT COUNT() AS group_count, SUM(n) AS row_count FROM (" + groupedQuery + ")")
                    .withContext(sqlExecutionContext)
                    .expectSize()
                    .noRandomAccess()
                    .returns("group_count\trow_count\n33\t11764200\n");
            sqlExecutionContext.setParallelGroupByEnabled(false);
            assertQuery("SELECT COUNT() AS group_count, SUM(n) AS row_count FROM (" + groupedQuery + ")")
                    .withContext(sqlExecutionContext)
                    .expectSize()
                    .noRandomAccess()
                    .returns("group_count\trow_count\n33\t11764200\n");
        });
    }

    @Test
    public void testAsyncGroupingSetsWithThreadSafeAggregates() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v DOUBLE)");
            execute("INSERT INTO t VALUES (1, 10, 3), (1, 20, 4), (2, 10, 5)");
            sqlExecutionContext.setParallelGroupByEnabled(true);

            String query = "SELECT a, b, SUM(v) AS s FROM t GROUP BY ROLLUP(a, b)";
            assertQuery(query)
                    .withContext(sqlExecutionContext)
                    .assertsPlanContaining("Async Group By");
            assertQuery(query)
                    .withContext(sqlExecutionContext)
                    .noRandomAccess()
                    .sizeMayVary()
                    .returns("a\tb\ts\n"
                            + "1\t10\t3.0\n"
                            + "1\t20\t4.0\n"
                            + "2\t10\t5.0\n"
                            + "1\tnull\t7.0\n"
                            + "2\tnull\t5.0\n"
                            + "null\tnull\t12.0\n");
        });
    }

    @Test
    public void testAsyncGroupingSetsWithoutGroupingFunction() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v DOUBLE)");
            execute("INSERT INTO t VALUES (1, 10, 3), (1, 20, 4), (2, 10, 5)");
            sqlExecutionContext.setParallelGroupByEnabled(true);
            String query = "SELECT a, b, SUM(v) AS s FROM t GROUP BY GROUPING SETS ((a, b), (a), ()) ORDER BY a, b";
            assertQuery(query)
                    .withContext(sqlExecutionContext)
                    .assertsPlanContaining("Async Group By");
            assertQuery(query)
                    .withContext(sqlExecutionContext)
                    .expectSize()
                    .returns("a\tb\ts\n"
                            + "null\tnull\t12.0\n"
                            + "1\tnull\t7.0\n"
                            + "1\t10\t3.0\n"
                            + "1\t20\t4.0\n"
                            + "2\tnull\t5.0\n"
                            + "2\t10\t5.0\n");
        });
    }

    @Test
    public void testCompositeGroupByCube() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE trades (region SYMBOL, symbol SYMBOL, side SYMBOL, quantity DOUBLE, ts TIMESTAMP)" +
                            " TIMESTAMP(ts) PARTITION BY DAY"
            );
            execute(
                    "INSERT INTO trades VALUES " +
                            "('US', 'BTC', 'buy', 10, '2024-01-01T00:00:00.000000Z')," +
                            "('US', 'BTC', 'sell', 20, '2024-01-01T00:01:00.000000Z')," +
                            "('EU', 'ETH', 'buy', 30, '2024-01-01T00:02:00.000000Z')," +
                            "('EU', 'ETH', 'sell', 40, '2024-01-01T00:03:00.000000Z')"
            );

            // GROUP BY region, CUBE(symbol) -> sets: (region, symbol), (region)
            // same as ROLLUP for single cube column
            assertGroupingSetsQuery(
                    "region\tsymbol\tSUM\n" +
                            "US\tBTC\t30.0\n" +
                            "EU\tETH\t70.0\n" +
                            "US\t\t30.0\n" +
                            "EU\t\t70.0\n",
                    "SELECT region, symbol, SUM(quantity) " +
                            "FROM trades " +
                            "GROUP BY region, CUBE(symbol)"
            );
        });
    }

    @Test
    public void testCompositeGroupByRollup() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE trades (region SYMBOL, symbol SYMBOL, side SYMBOL, quantity DOUBLE, ts TIMESTAMP)" +
                            " TIMESTAMP(ts) PARTITION BY DAY"
            );
            execute(
                    "INSERT INTO trades VALUES " +
                            "('US', 'BTC', 'buy', 10, '2024-01-01T00:00:00.000000Z')," +
                            "('US', 'BTC', 'sell', 20, '2024-01-01T00:01:00.000000Z')," +
                            "('EU', 'ETH', 'buy', 30, '2024-01-01T00:02:00.000000Z')," +
                            "('EU', 'ETH', 'sell', 40, '2024-01-01T00:03:00.000000Z')"
            );

            // GROUP BY region, ROLLUP(symbol) -> sets: (region, symbol), (region)
            assertGroupingSetsQuery(
                    "region\tsymbol\tSUM\n" +
                            "US\tBTC\t30.0\n" +
                            "EU\tETH\t70.0\n" +
                            "US\t\t30.0\n" +
                            "EU\t\t70.0\n",
                    "SELECT region, symbol, SUM(quantity) " +
                            "FROM trades " +
                            "GROUP BY region, ROLLUP(symbol)"
            );
        });
    }

    @Test
    public void testCompositeGroupingSetsSyntaxErrors() throws Exception {
        // A plain key before ROLLUP or CUBE takes its own parse path, with its own checks.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, c1 INT, c2 INT, c3 INT, c4 INT, c5 INT, c6 INT, c7 INT, c8 INT, c9 INT, c10 INT, c11 INT, c12 INT, c13 INT, c14 INT, c15 INT, c16 INT, v INT)");
            String query = "SELECT a, count() FROM t GROUP BY a, ROLLUP()";
            assertException(query, query.indexOf("ROLLUP"), "ROLLUP requires at least one column");
            query = "SELECT a, count() FROM t GROUP BY a, CUBE()";
            assertException(query, query.indexOf("CUBE"), "CUBE requires at least one column");
            query = "SELECT a, count() FROM t GROUP BY a, CUBE(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16)";
            assertException(query, query.indexOf("CUBE"), "CUBE supports at most 15 columns");
            query = "SELECT a, count() FROM t GROUP BY a, (SELECT 1)";
            assertException(query, query.indexOf("SELECT 1"), "literal expected");
            query = "SELECT a, count() FROM t GROUP BY ROLLUP((SELECT 1))";
            assertException(query, query.indexOf("SELECT 1"), "literal expected");
            // Plain keys combine with ROLLUP and CUBE only.
            query = "SELECT a, c1, count() FROM t GROUP BY a, GROUPING SETS ((c1), ())";
            assertException(query, query.indexOf("GROUPING"), "GROUPING SETS must be the only GROUP BY item, list the other keys in each set");
            query = "SELECT a, c1, count() FROM t GROUP BY a, c1, grouping sets ((c2))";
            assertException(query, query.indexOf("grouping"), "GROUPING SETS must be the only GROUP BY item, list the other keys in each set");
        });
    }

    @Test
    public void testCteWithRollup() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE trades (symbol SYMBOL, side SYMBOL, quantity DOUBLE, ts TIMESTAMP)" +
                            " TIMESTAMP(ts) PARTITION BY DAY"
            );
            execute(
                    "INSERT INTO trades VALUES " +
                            "('BTC', 'buy', 10, '2024-01-01T00:00:00.000000Z')," +
                            "('BTC', 'sell', 20, '2024-01-01T00:01:00.000000Z')," +
                            "('ETH', 'buy', 30, '2024-01-01T00:02:00.000000Z')"
            );

            assertGroupingSetsQuery(
                    "symbol\tSUM\n" +
                            "BTC\t30.0\n" +
                            "ETH\t30.0\n" +
                            "\t60.0\n",
                    "WITH base AS (SELECT symbol, quantity FROM trades)" +
                            " SELECT symbol, SUM(quantity) FROM base GROUP BY ROLLUP(symbol)" +
                            " ORDER BY GROUPING(symbol), symbol"
            );
        });
    }

    @Test
    public void testCube12ColumnsAtDefaultLimit() throws Exception {
        // CUBE(12 columns) = 2^12 = 4096 sets, exactly at the default config
        // limit. Tests the boundary where the set count cap is not exceeded.
        // Note: CUBE(15) = 32768 would exceed the default 4096 limit but is
        // within the hard parser cap of 15 columns. CUBE(16+) is rejected by
        // the parser regardless of the set count limit.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE cube12 (" +
                    "c1 INT, c2 INT, c3 INT, c4 INT, c5 INT, " +
                    "c6 INT, c7 INT, c8 INT, c9 INT, c10 INT, " +
                    "c11 INT, c12 INT, v INT)");
            assertGroupingSetsQuery(
                    "c1\tSUM\nnull\tnull\n",
                    "SELECT c1, SUM(v) FROM cube12" +
                            " GROUP BY CUBE(c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12)"
            );
        });
    }

    @Test
    public void testCubeBasic() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE trades (symbol SYMBOL, side SYMBOL, quantity DOUBLE, ts TIMESTAMP)" +
                            " TIMESTAMP(ts) PARTITION BY DAY"
            );
            execute(
                    "INSERT INTO trades VALUES " +
                            "('BTC', 'buy', 10, '2024-01-01T00:00:00.000000Z')," +
                            "('BTC', 'sell', 20, '2024-01-01T00:01:00.000000Z')," +
                            "('ETH', 'buy', 30, '2024-01-01T00:02:00.000000Z')," +
                            "('ETH', 'sell', 40, '2024-01-01T00:03:00.000000Z')"
            );

            assertGroupingSetsQuery(
                    "symbol\tside\tSUM\n" +
                            "BTC\tbuy\t10.0\n" +
                            "BTC\tsell\t20.0\n" +
                            "ETH\tbuy\t30.0\n" +
                            "ETH\tsell\t40.0\n" +
                            "BTC\t\t30.0\n" +
                            "ETH\t\t70.0\n" +
                            "\tbuy\t40.0\n" +
                            "\tsell\t60.0\n" +
                            "\t\t100.0\n",
                    "SELECT symbol, side, SUM(quantity) " +
                            "FROM trades " +
                            "GROUP BY CUBE(symbol, side)"
            );
        });
    }

    @Test
    public void testCubeCaseInsensitive() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT, y DOUBLE)");
            execute("INSERT INTO t VALUES (1, 10), (2, 20)");

            assertGroupingSetsQuery(
                    "x\tSUM\n" +
                            "null\t30.0\n" +
                            "1\t10.0\n" +
                            "2\t20.0\n",
                    "SELECT x, SUM(y) FROM t GROUP BY cube(x) ORDER BY x"
            );
        });
    }

    @Test
    public void testCubeOuterKeyFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v DOUBLE)");
            execute("INSERT INTO t VALUES (1,10,3), (1,20,4), (2,10,5)");
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                assertQuery("SELECT * FROM (SELECT a,b,SUM(v) s FROM t GROUP BY CUBE(a,b)) WHERE a=1 ORDER BY a,b")
                        .withContext(sqlExecutionContext).returns("""
                                a\tb\ts
                                1\tnull\t7.0
                                1\t10\t3.0
                                1\t20\t4.0
                                """);
            }
        });
    }

    @Test
    public void testCubeParserError() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT, y DOUBLE)");
            assertException(
                    "SELECT x, SUM(y) FROM t GROUP BY CUBE()",
                    33,
                    "CUBE requires at least one column"
            );
        });
    }

    @Test
    public void testCubeSixteenColumnsRejected() throws Exception {
        // CUBE takes at most 15 columns, whatever cairo.sql.max.grouping.sets allows.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (c1 INT, c2 INT, c3 INT, c4 INT, c5 INT," +
                    " c6 INT, c7 INT, c8 INT, c9 INT, c10 INT," +
                    " c11 INT, c12 INT, c13 INT, c14 INT, c15 INT, c16 INT, v INT)");
            assertException(
                    "SELECT * FROM t GROUP BY CUBE(c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16)",
                    25,
                    "CUBE supports at most 15 columns"
            );
        });
    }

    @Test
    public void testCubeThreeColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, c INT, v DOUBLE)");
            execute(
                    "INSERT INTO t VALUES " +
                            "(1, 10, 100, 1000), " +
                            "(2, 20, 200, 2000)"
            );

            // CUBE(a, b, c) -> 2^3 = 8 grouping sets, ordered by mask iteration:
            // (a,b,c), (a,b), (a,c), (a), (b,c), (b), (c), ()
            assertGroupingSetsQuery(
                    "a\tb\tc\tSUM\n" +
                            "1\t10\t100\t1000.0\n" +
                            "2\t20\t200\t2000.0\n" +
                            "1\t10\tnull\t1000.0\n" +
                            "2\t20\tnull\t2000.0\n" +
                            "1\tnull\t100\t1000.0\n" +
                            "2\tnull\t200\t2000.0\n" +
                            "1\tnull\tnull\t1000.0\n" +
                            "2\tnull\tnull\t2000.0\n" +
                            "null\t10\t100\t1000.0\n" +
                            "null\t20\t200\t2000.0\n" +
                            "null\t10\tnull\t1000.0\n" +
                            "null\t20\tnull\t2000.0\n" +
                            "null\tnull\t100\t1000.0\n" +
                            "null\tnull\t200\t2000.0\n" +
                            "null\tnull\tnull\t3000.0\n",
                    "SELECT a, b, c, SUM(v) FROM t GROUP BY CUBE(a, b, c)"
            );
        });
    }

    @Test
    public void testCubeTooManySets() throws Exception {
        // CUBE(13 columns) produces 2^13 = 8192 sets, exceeding default 4096
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (c1 INT, c2 INT, c3 INT, c4 INT, c5 INT," +
                    " c6 INT, c7 INT, c8 INT, c9 INT, c10 INT," +
                    " c11 INT, c12 INT, c13 INT, v INT)");
            assertException(
                    "SELECT * FROM t GROUP BY CUBE(c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13)",
                    25,
                    "CUBE produces too many grouping sets"
            );
        });
    }

    @Test
    public void testCursorReuse() throws Exception {
        // Compiling and running the same query twice gives the same rows.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a SYMBOL, v DOUBLE)");
            execute("INSERT INTO t VALUES ('x', 10.0), ('y', 20.0)");
            String expected =
                    "a\tSUM\n" +
                            "\t30.0\n" +
                            "x\t10.0\n" +
                            "y\t20.0\n";
            String query = "SELECT a, SUM(v) FROM t GROUP BY ROLLUP(a) ORDER BY a";
            assertGroupingSetsQuery(expected, query);
            assertGroupingSetsQuery(expected, query);
        });
    }

    @Test
    public void testDataNullsVsRollupNulls() throws Exception {
        // Data NULLs and rollup NULLs both produce NULL in the output,
        // but GROUPING() returns 0 for data NULLs and 1 for rollup NULLs.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a SYMBOL, v DOUBLE)");
            execute("INSERT INTO t VALUES ('x', 10.0), (NULL, 20.0), ('x', 30.0)");
            assertGroupingSetsQuery(
                    "a\tSUM\tGROUPING\n" +
                            "\t20.0\t0\n" +
                            "x\t40.0\t0\n" +
                            "\t60.0\t1\n",
                    "SELECT a, SUM(v), GROUPING(a) FROM t GROUP BY ROLLUP(a) ORDER BY GROUPING(a), a"
            );
        });
    }

    @Test
    public void testDistinctRollupWithDataNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT)");
            execute("INSERT INTO t VALUES (null), (1)");

            assertQuery("SELECT DISTINCT a FROM t GROUP BY ROLLUP(a) ORDER BY a")
                    .expectSize()
                    .returns("a\nnull\n1\n");
        });
    }

    @Test
    public void testDistinctWithDuplicateGroupingSets() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT)");
            execute("INSERT INTO t VALUES (1), (2)");

            assertQuery("SELECT DISTINCT a FROM t GROUP BY GROUPING SETS ((a), (a)) ORDER BY a")
                    .expectSize()
                    .returns("a\n1\n2\n");
        });
    }

    @Test
    public void testDuplicateGroupingSets() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (sym SYMBOL, val DOUBLE)");
            execute("""
                    INSERT INTO t VALUES
                    ('A', 10.0),
                    ('B', 20.0)
                    """);
            // Duplicate sets should produce duplicate output rows per SQL standard
            assertGroupingSetsQuery(
                    "sym\ts\n" +
                            "A\t10.0\n" +
                            "A\t10.0\n" +
                            "B\t20.0\n" +
                            "B\t20.0\n",
                    "SELECT sym, SUM(val) s FROM t GROUP BY GROUPING SETS ((sym), (sym)) ORDER BY sym, s"
            );
        });
    }

    @Test
    public void testEmptyGroupingSetWildcardWithAllSourceColumnsGrouped() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v INT, ts TIMESTAMP)");
            execute("INSERT INTO t VALUES (1, 2, 3, 1000)");
            execute("CREATE TABLE t2 (a INT)");
            execute("INSERT INTO t2 VALUES (1)");
            execute("CREATE TABLE t3 (b INT)");
            execute("INSERT INTO t3 VALUES (1)");

            for (int i = 0; i < 2; i++) {
                sqlExecutionContext.setParallelGroupByEnabled(i == 1);
                assertQuery("SELECT * FROM t GROUP BY ROLLUP(a, b, v, ts) ORDER BY a, b, v, ts")
                        .withContext(sqlExecutionContext)
                        .expectSize()
                        .returns("a\tb\tv\tts\nnull\tnull\tnull\t\n1\tnull\tnull\t\n1\t2\tnull\t\n1\t2\t3\t\n1\t2\t3\t1970-01-01T00:00:00.001000Z\n");
                assertQuery("SELECT * FROM t GROUP BY CUBE(a, b, v, ts) ORDER BY a, b, v, ts")
                        .withContext(sqlExecutionContext)
                        .expectSize()
                        .returns("a\tb\tv\tts\n" +
                                "null\tnull\tnull\t\n" +
                                "null\tnull\tnull\t1970-01-01T00:00:00.001000Z\n" +
                                "null\tnull\t3\t\n" +
                                "null\tnull\t3\t1970-01-01T00:00:00.001000Z\n" +
                                "null\t2\tnull\t\n" +
                                "null\t2\tnull\t1970-01-01T00:00:00.001000Z\n" +
                                "null\t2\t3\t\n" +
                                "null\t2\t3\t1970-01-01T00:00:00.001000Z\n" +
                                "1\tnull\tnull\t\n" +
                                "1\tnull\tnull\t1970-01-01T00:00:00.001000Z\n" +
                                "1\tnull\t3\t\n" +
                                "1\tnull\t3\t1970-01-01T00:00:00.001000Z\n" +
                                "1\t2\tnull\t\n" +
                                "1\t2\tnull\t1970-01-01T00:00:00.001000Z\n" +
                                "1\t2\t3\t\n" +
                                "1\t2\t3\t1970-01-01T00:00:00.001000Z\n");
                assertQuery("SELECT * FROM t GROUP BY GROUPING SETS ((a, b, v, ts), ()) ORDER BY a, b, v, ts")
                        .withContext(sqlExecutionContext)
                        .expectSize()
                        .returns("a\tb\tv\tts\nnull\tnull\tnull\t\n1\t2\t3\t1970-01-01T00:00:00.001000Z\n");
                assertQuery("SELECT * FROM t2 GROUP BY ROLLUP(a) ORDER BY a")
                        .withContext(sqlExecutionContext)
                        .expectSize()
                        .returns("a\nnull\n1\n");
                assertQuery("SELECT * FROM t2 JOIN t3 ON t2.a = t3.b GROUP BY ROLLUP(a, b) ORDER BY a, b")
                        .withContext(sqlExecutionContext)
                        .expectSize()
                        .returns("a\tb\nnull\tnull\n1\tnull\n1\t1\n");

                String insufficientKeysQuery = "SELECT * FROM t GROUP BY ROLLUP(a)";
                assertQuery(insufficientKeysQuery)
                        .withContext(sqlExecutionContext)
                        .fails(insufficientKeysQuery.indexOf('*'), "column must appear in GROUP BY clause or aggregate function");

                String insufficientJoinKeysQuery = "SELECT * FROM t2 JOIN t3 ON t2.a = t3.b GROUP BY ROLLUP(a)";
                assertQuery(insufficientJoinKeysQuery)
                        .withContext(sqlExecutionContext)
                        .fails(insufficientJoinKeysQuery.indexOf('*'), "column must appear in GROUP BY clause or aggregate function");
            }
        });
    }

    @Test
    public void testEmptyGroupingSetWithWindowJoinFailsWithoutCrashing() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP) TIMESTAMP(ts)");
            String query = "SELECT t.ts FROM t WINDOW JOIN t t2 RANGE BETWEEN 1 SECOND PRECEDING " +
                    "AND 1 SECOND FOLLOWING GROUP BY GROUPING SETS (())";
            assertQuery(query)
                    .fails(query.indexOf("GROUP BY"), "GROUP BY cannot be used with WINDOW JOIN");
        });
    }

    @Test
    public void testEmptyGroupingSetsConstantProjection() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, v INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES (1, 10, 1000), (2, 20, 2000)");
            assertQuery("SELECT 1 AS x FROM t GROUP BY GROUPING SETS (())")
                    .expectSize().noRandomAccess()
                    .returns("x\n1\n");
        });
    }

    @Test
    public void testEmptyGroupingSetsDuplicateCountFunctionMultiplicity() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, v INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES (1, 10, 1000), (2, 20, 2000)");

            assertQuery("SELECT COUNT() AS n FROM t GROUP BY GROUPING SETS ((), ())")
                    .expectSize().noRandomAccess()
                    .returns("n\n2\n2\n");
        });
    }

    @Test
    public void testEmptyGroupingSetsDuplicateCountStarMultiplicity() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, v INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES (1, 10, 1000), (2, 20, 2000)");

            assertQuery("SELECT COUNT(*) AS n FROM t GROUP BY GROUPING SETS ((), ())")
                    .expectSize().noRandomAccess()
                    .returns("n\n2\n2\n");
        });
    }

    @Test
    public void testEmptyGroupingSetsDuplicateMaxTimestampMultiplicity() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, v INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES (1, 10, 1000), (2, 20, 2000)");

            assertQuery("SELECT MAX(ts) AS ts FROM t GROUP BY GROUPING SETS ((), ())")
                    .inferTimestamp()
                    .expectSize().noRandomAccess()
                    .returns("ts\n1970-01-01T00:00:00.002000Z\n1970-01-01T00:00:00.002000Z\n");
        });
    }

    @Test
    public void testEmptyGroupingSetsDuplicateSumMultiplicity() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, v INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES (1, 10, 1000), (2, 20, 2000)");

            assertQuery("SELECT SUM(v) AS v FROM t GROUP BY GROUPING SETS ((), ())")
                    .expectSize().noRandomAccess()
                    .returns("v\n30\n30\n");
        });
    }

    @Test
    public void testEmptyGroupingSetsGrandTotal() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, v INT)");
            execute("INSERT INTO t VALUES (1, 10), (1, 20), (2, 30)");

            String singleSetQuery = "SELECT COUNT(*) AS n, SUM(v) AS s " +
                    "FROM t GROUP BY GROUPING SETS (())";
            String duplicateSetsQuery = "SELECT COUNT(*) AS n, SUM(v) AS s " +
                    "FROM t GROUP BY GROUPING SETS ((), ())";
            String populatedSingleSet = "n\ts\n3\t60\n";
            String populatedDuplicateSets = populatedSingleSet + "3\t60\n";

            for (int i = 0; i < 2; i++) {
                sqlExecutionContext.setParallelGroupByEnabled(i == 1);
                assertQuery(singleSetQuery).withContext(sqlExecutionContext).expectSize().noRandomAccess().returns(populatedSingleSet);
                assertQuery(duplicateSetsQuery).withContext(sqlExecutionContext).expectSize().noRandomAccess().returns(populatedDuplicateSets);
            }

            assertQuery("SELECT a, GROUPING_ID(a) AS gid, COUNT(*) AS n, SUM(v) AS s " +
                    "FROM t GROUP BY GROUPING SETS ((a), ()) ORDER BY gid, a")
                    .expectSize()
                    .returns("a\tgid\tn\ts\n1\t0\t2\t30\n2\t0\t1\t30\nnull\t1\t3\t60\n");

            execute("TRUNCATE TABLE t");
            String emptySingleSet = "n\ts\n0\tnull\n";
            String emptyDuplicateSets = emptySingleSet + "0\tnull\n";
            for (int i = 0; i < 2; i++) {
                sqlExecutionContext.setParallelGroupByEnabled(i == 1);
                assertQuery(singleSetQuery).withContext(sqlExecutionContext).expectSize().noRandomAccess().returns(emptySingleSet);
                assertQuery(duplicateSetsQuery).withContext(sqlExecutionContext).expectSize().noRandomAccess().returns(emptyDuplicateSets);
            }

            String invalidGroupingIdQuery = "SELECT GROUPING_ID(a), COUNT(*) FROM t GROUP BY GROUPING SETS (())";
            assertException(invalidGroupingIdQuery, invalidGroupingIdQuery.indexOf("a)"),
                    "argument must be a grouping key column");
        });
    }

    @Test
    public void testEmptyGroupingSetsProjectionValidation() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, v INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES (1, 10, 1000), (2, 20, 2000)");

            String invalidQuery = "SELECT a, SUM(v) FROM t GROUP BY GROUPING SETS (())";
            assertQuery(invalidQuery)
                    .fails(invalidQuery.indexOf("a,"), "column must appear in GROUP BY clause or aggregate function");
        });
    }

    @Test
    public void testEmptyGroupingSetsRejectsStarProjection() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, v INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES (1, 10, 1000), (2, 20, 2000)");

            String[] queries = {
                    "SELECT * FROM t GROUP BY GROUPING SETS (())",
                    "SELECT t.* FROM t GROUP BY GROUPING SETS (())",
                    "SELECT *, SUM(v) FROM t GROUP BY GROUPING SETS (())",
                    "SELECT * FROM t WHERE a > 0 GROUP BY GROUPING SETS (())"
            };
            int unexpectedSuccesses = 0;
            StringBuilder unexpectedQueries = new StringBuilder();
            for (int i = 0; i < 2; i++) {
                sqlExecutionContext.setParallelGroupByEnabled(i == 1);
                for (String query : queries) {
                    try {
                        assertQuery(query)
                                .withContext(sqlExecutionContext)
                                .fails(query.indexOf('*'), "column must appear in GROUP BY clause or aggregate function");
                    } catch (AssertionError e) {
                        unexpectedSuccesses++;
                        unexpectedQueries.append(i == 0 ? "serial: " : "parallel: ").append(query).append('\n');
                    }
                }
            }
            Assert.assertEquals(unexpectedQueries.toString(), 0, unexpectedSuccesses);
        });
    }

    @Test
    public void testEmptyGroupingSetsShorthandSubquery() throws Exception {
        // The shorthand (t ...) selects every column of t, as SELECT * FROM t ... does, so it
        // fails the same way and does not fold into a plain read of t.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, v INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES (1, 10, 1000), (2, 20, 2000)");
            for (int i = 0; i < 2; i++) {
                sqlExecutionContext.setParallelGroupByEnabled(i == 1);
                final String query = "SELECT * FROM (t GROUP BY GROUPING SETS (()))";
                assertQuery(query)
                        .withContext(sqlExecutionContext)
                        .fails(query.indexOf("t GROUP"), "column must appear in GROUP BY clause or aggregate function");
            }
        });
    }

    @Test
    public void testEmptyInputGroupingSetGrandTotal() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v DOUBLE)");
            sqlExecutionContext.setParallelGroupByEnabled(false);
            String query = "SELECT a, b, GROUPING_ID(a, b) AS gid, COUNT(*) AS n, SUM(v) AS s "
                    + "FROM t GROUP BY GROUPING SETS ((a, b), ()) ORDER BY gid, a, b";
            String expected = "a\tb\tgid\tn\ts\nnull\tnull\t3\t0\tnull\n";

            assertQuery(query)
                    .expectSize()
                    .returns(expected);

            sqlExecutionContext.setParallelGroupByEnabled(true);
            assertQuery(query)
                    .withContext(sqlExecutionContext)
                    .assertsPlanContaining("Async Group By");
            assertQuery(query)
                    .withContext(sqlExecutionContext)
                    .expectSize()
                    .returns(expected);
        });
    }

    @Test
    public void testEmptyTableCube() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v DOUBLE)");
            assertGroupingSetsQuery(
                    "a\tb\tSUM\nnull\tnull\tnull\n",
                    "SELECT a, b, SUM(v) FROM t GROUP BY CUBE(a, b)"
            );
        });
    }

    @Test
    public void testEmptyTableRollup() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, v DOUBLE)");
            // Empty table produces the grand-total row for the empty grouping set.
            assertGroupingSetsQuery(
                    "a\tSUM\nnull\tnull\n",
                    "SELECT a, SUM(v) FROM t GROUP BY ROLLUP(a)"
            );
        });
    }

    @Test
    public void testFactoryPropertiesFollowInputs() throws Exception {
        // The query cache, materialized views and plans read these properties, and a grouping
        // sets factory takes them from its input, its expression keys and its aggregates.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL INDEX, a INT, v INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES ('x', 1, 2, '2024-01-01'), ('y', 2, 3, '2024-01-02')");
            // Each grouping sets query must report what the plain GROUP BY over the same input,
            // keys and aggregates reports. The pairs differ in every property, so a factory that
            // ignores an input disagrees on at least one pair.
            String[][] pairs = {
                    {"SELECT a, sum(v) FROM t GROUP BY a", "SELECT a, sum(v) FROM t GROUP BY ROLLUP(a)"},
                    {"SELECT a, sum(rnd_int()) FROM t GROUP BY a", "SELECT a, sum(rnd_int()) FROM t GROUP BY ROLLUP(a)"},
                    {"SELECT rnd_int(0, 1, 0) k, a, sum(v) FROM t GROUP BY k, a", "SELECT rnd_int(0, 1, 0) k, a, sum(v) FROM t GROUP BY k, ROLLUP(a)"},
                    {"SELECT a, sum(v) FROM t WHERE s = 'x' GROUP BY a", "SELECT a, sum(v) FROM t WHERE s = 'x' GROUP BY ROLLUP(a)"},
                    {"SELECT a, sum(v) FROM t WHERE ts > now() GROUP BY a", "SELECT a, sum(v) FROM t WHERE ts > now() GROUP BY ROLLUP(a)"}
            };
            for (int i = 0; i < 2; i++) {
                sqlExecutionContext.setParallelGroupByEnabled(i == 1);
                for (String[] pair : pairs) {
                    try (
                            RecordCursorFactory plainTop = select(pair[0]);
                            RecordCursorFactory rollupTop = select(pair[1])
                    ) {
                        // QueryProgress wraps every top-level factory and keeps the interface defaults.
                        RecordCursorFactory plain = findGroupByFactory(plainTop);
                        RecordCursorFactory rollup = findGroupByFactory(rollupTop);
                        Assert.assertTrue(pair[1], rollup.getClass().getSimpleName().contains("GroupingSets"));
                        Assert.assertEquals(pair[1], plain.isNonDeterministic(), rollup.isNonDeterministic());
                        Assert.assertEquals(pair[1], plain.isStableWithinExecution(), rollup.isStableWithinExecution());
                        Assert.assertEquals(pair[1], plain.usesIndex(), rollup.usesIndex());
                    }
                }
            }
        });
    }

    @Test
    public void testFilterByGroupingViaSubquery() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (sym SYMBOL, val DOUBLE)");
            execute("""
                    INSERT INTO t VALUES
                    ('A', 10.0),
                    ('A', 20.0),
                    ('B', 30.0)
                    """);
            // QuestDB has no HAVING, so filter subtotal/grand total rows via subquery
            assertGroupingSetsQuery(
                    "sym\ts\tgs\n" +
                            "A\t30.0\t0\n" +
                            "B\t30.0\t0\n",
                    "SELECT * FROM (SELECT sym, SUM(val) s, GROUPING(sym) gs FROM t GROUP BY ROLLUP(sym)) WHERE gs = 0 ORDER BY sym"
            );
        });
    }

    @Test
    public void testGroupByExpressionNotAffected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v DOUBLE)");
            execute("INSERT INTO t VALUES (1, 2, 100.0), (1, 2, 200.0), (3, 4, 300.0)");
            // Plain GROUP BY with expressions must still work (no
            // column-reference restriction outside ROLLUP/CUBE/GROUPING SETS).
            assertGroupingSetsQuery(
                    "column\tSUM\n" +
                            "3\t300.0\n" +
                            "7\t300.0\n",
                    "SELECT a + b, SUM(v) FROM t GROUP BY a + b ORDER BY 1"
            );
        });
    }

    @Test
    public void testGroupingAliasShadowsColumnName() throws Exception {
        // GROUP BY names resolve to SELECT aliases first, as plain GROUP BY b, a does, so
        // ROLLUP(b, a) groups b twice when a is the alias of b. GROUPING(a) follows the key.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v INT)");
            execute("INSERT INTO t VALUES (1, 10, 3), (2, 20, 5), (1, 20, 7)");
            execute("CREATE TABLE u (a INT, w INT)");
            execute("INSERT INTO u VALUES (1, 100), (2, 200), (1, 300)");
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                assertGroupingSetsQuery(
                        """
                                a\tg\ts
                                null\t1\t15
                                10\t0\t3
                                10\t0\t3
                                20\t0\t12
                                20\t0\t12
                                """,
                        "SELECT b AS a, GROUPING(a) g, sum(v) s FROM t GROUP BY ROLLUP(b, a) ORDER BY a, g"
                );
                // The alias also shadows a column of a joined table.
                assertGroupingSetsQuery(
                        """
                                w\tg\tn
                                null\t1\t5
                                10\t0\t2
                                10\t0\t2
                                20\t0\t3
                                20\t0\t3
                                """,
                        "SELECT t.b AS w, GROUPING(w) g, count() n FROM t JOIN u ON t.a = u.a GROUP BY ROLLUP(t.b, w) ORDER BY w, g"
                );
            }
        });
    }

    @Test
    public void testGroupingExpressionKeyAlias() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v INT)");
            execute("INSERT INTO t VALUES (1, 10, 3), (2, 20, 5), (1, 20, 7)");
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                // An alias of an expression key present in every set.
                assertGroupingSetsQuery(
                        """
                                k\tb\tgk\tgb\ts
                                2\tnull\t0\t1\t10
                                2\t10\t0\t0\t3
                                2\t20\t0\t0\t7
                                3\tnull\t0\t1\t5
                                3\t20\t0\t0\t5
                                """,
                        "SELECT a + 1 AS k, b, GROUPING(k) gk, GROUPING(b) gb, sum(v) s FROM t GROUP BY a + 1, ROLLUP(b) ORDER BY k, b"
                );
                // An ordinal makes the expression a rolled-up key, which needs a subquery.
                String query = "SELECT a + 1 AS k, GROUPING(k) g, sum(v) s FROM t GROUP BY ROLLUP(1)";
                assertQuery(query).withContext(sqlExecutionContext).fails(
                        query.indexOf("ROLLUP(1)") + "ROLLUP(".length(),
                        "expression keys in ROLLUP, CUBE and GROUPING SETS must be computed in a subquery"
                );
            }
        });
    }

    @Test
    public void testGroupingFillPreservesValue() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE trades (symbol SYMBOL, quantity DOUBLE, ts TIMESTAMP)" +
                            " TIMESTAMP(ts) PARTITION BY DAY"
            );
            execute(
                    "INSERT INTO trades VALUES " +
                            "('BTC', 10, '2024-01-01T00:00:00.000000Z')," +
                            "('BTC', 30, '2024-01-01T02:00:00.000000Z')"
            );

            // GROUPING() in fill rows should show the correct bitmask,
            // not the fill value (NULL or 0).
            assertGroupingSetsQuery(
                    "grp\tts\tsymbol\tSUM\n" +
                            "0\t2024-01-01T00:00:00.000000Z\tBTC\t10.0\n" +
                            "1\t2024-01-01T00:00:00.000000Z\t\t10.0\n" +
                            "0\t2024-01-01T01:00:00.000000Z\tBTC\tnull\n" +
                            "1\t2024-01-01T01:00:00.000000Z\t\tnull\n" +
                            "0\t2024-01-01T02:00:00.000000Z\tBTC\t30.0\n" +
                            "1\t2024-01-01T02:00:00.000000Z\t\t30.0\n",
                    "SELECT GROUPING(symbol) AS grp, ts, symbol, SUM(quantity) " +
                            "FROM trades " +
                            "SAMPLE BY 1h ROLLUP(symbol) FILL(NULL) " +
                            "ORDER BY ts, grp, symbol"
            );
        });
    }

    @Test
    public void testGroupingFunctionMultiArgRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE trades (symbol SYMBOL, side SYMBOL, quantity DOUBLE, ts TIMESTAMP)" +
                            " TIMESTAMP(ts) PARTITION BY DAY"
            );

            // GROUPING() only accepts a single column; use GROUPING_ID() for multiple
            assertException(
                    "SELECT symbol, side, SUM(quantity), GROUPING(symbol, side) AS grp " +
                            "FROM trades " +
                            "GROUP BY ROLLUP(symbol, side)",
                    36,
                    "GROUPING() accepts a single column; use GROUPING_ID() for multiple columns"
            );
        });
    }

    @Test
    public void testGroupingFunctionOutsideGroupingSetsHorizonJoin() throws Exception {
        // HORIZON JOIN aggregates in its own factories, single-join and multi-join, serial and
        // parallel. Each must reject GROUPING() the way plain GROUP BY does.
        assertMemoryLeak(() -> {
            createHorizonJoinTables();
            final String[] queries = {
                    "SELECT t.sym, GROUPING(t.sym) g, sum(p.price) FROM trades t HORIZON JOIN prices p ON (t.sym = p.sym) RANGE FROM -2s TO 2s STEP 1s AS h",
                    "SELECT t.sym, GROUPING_ID(t.sym) g, sum(p.price) FROM trades t HORIZON JOIN prices p ON (t.sym = p.sym) LIST (0, 1s) AS h",
                    "SELECT t.sym, GROUPING(t.sym) g, sum(p.price), sum(m.price) FROM trades t HORIZON JOIN prices p ON (t.sym = p.sym) " +
                            "HORIZON JOIN prices m ON (t.sym = m.sym) LIST (0, 1s) AS h",
            };
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                for (String query : queries) {
                    assertException(query, query.indexOf("GROUPING"), "GROUPING() / GROUPING_ID() can only be used with GROUPING SETS, ROLLUP, or CUBE");
                }
            }
        });
    }

    @Test
    public void testGroupingFunctionOutsideGroupingSetsKeyed() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE trades (symbol SYMBOL, quantity DOUBLE, ts TIMESTAMP)" +
                            " TIMESTAMP(ts) PARTITION BY DAY"
            );

            assertException(
                    "SELECT symbol, SUM(quantity), GROUPING(symbol) " +
                            "FROM trades " +
                            "GROUP BY symbol",
                    30,
                    "GROUPING() / GROUPING_ID() can only be used with GROUPING SETS, ROLLUP, or CUBE"
            );
        });
    }

    @Test
    public void testGroupingFunctionOutsideGroupingSetsNoKey() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE trades (symbol SYMBOL, quantity DOUBLE, ts TIMESTAMP)" +
                            " TIMESTAMP(ts) PARTITION BY DAY"
            );

            assertException(
                    "SELECT SUM(quantity), GROUPING(symbol) " +
                            "FROM trades",
                    22,
                    "GROUPING() / GROUPING_ID() can only be used with GROUPING SETS, ROLLUP, or CUBE"
            );
        });
    }

    @Test
    public void testGroupingFunctionOutsideGroupingSetsSampleBy() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE trades (symbol SYMBOL, quantity DOUBLE, ts TIMESTAMP)" +
                            " TIMESTAMP(ts) PARTITION BY DAY"
            );

            assertException(
                    "SELECT ts, GROUPING(symbol), SUM(quantity) FROM trades SAMPLE BY 1d ALIGN TO FIRST OBSERVATION",
                    11,
                    "GROUPING() / GROUPING_ID() can only be used with GROUPING SETS, ROLLUP, or CUBE"
            );
        });
    }

    @Test
    public void testGroupingFunctionOutsideGroupingSetsWindowJoin() throws Exception {
        assertMemoryLeak(() -> {
            createHorizonJoinTables();
            final String query = "SELECT t.sym, GROUPING(t.sym) g, sum(p.price) FROM trades t WINDOW JOIN prices p ON (t.sym = p.sym) " +
                    "RANGE BETWEEN 1 SECOND PRECEDING AND 1 SECOND FOLLOWING";
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                assertException(query, query.indexOf("GROUPING"), "GROUPING() / GROUPING_ID() can only be used with GROUPING SETS, ROLLUP, or CUBE");
            }
        });
    }

    @Test
    public void testGroupingFunctionRollup() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE trades (symbol SYMBOL, side SYMBOL, quantity DOUBLE, ts TIMESTAMP)" +
                            " TIMESTAMP(ts) PARTITION BY DAY"
            );
            execute(
                    "INSERT INTO trades VALUES " +
                            "('BTC', 'buy', 10, '2024-01-01T00:00:00.000000Z')," +
                            "('BTC', 'sell', 20, '2024-01-01T00:01:00.000000Z')," +
                            "('ETH', 'buy', 30, '2024-01-01T00:02:00.000000Z')"
            );

            // GROUPING(col) returns 0 when column is actively grouped,
            // 1 when the column is rolled up (NULL)
            assertGroupingSetsQuery(
                    "symbol\tside\tSUM\tgs\tgsd\n" +
                            "BTC\tbuy\t10.0\t0\t0\n" +
                            "BTC\tsell\t20.0\t0\t0\n" +
                            "ETH\tbuy\t30.0\t0\t0\n" +
                            "BTC\t\t30.0\t0\t1\n" +
                            "ETH\t\t30.0\t0\t1\n" +
                            "\t\t60.0\t1\t1\n",
                    "SELECT symbol, side, SUM(quantity), GROUPING(symbol) AS gs, GROUPING(side) AS gsd " +
                            "FROM trades " +
                            "GROUP BY ROLLUP(symbol, side)"
            );
        });
    }

    @Test
    public void testGroupingIdExpressionArgumentsInPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v DOUBLE)");
            execute("INSERT INTO t VALUES (1, 10, 3), (1, 20, 4), (2, 10, 5)");
            String expected = "k\tb\tgid\ts\n"
                    + "2\t10\t0\t3.0\n"
                    + "2\t20\t0\t4.0\n"
                    + "3\t10\t0\t5.0\n"
                    + "2\tnull\t1\t7.0\n"
                    + "3\tnull\t1\t5.0\n";
            String aliasQuery = "SELECT a + 1 AS k, b, GROUPING_ID(k, b) AS gid, SUM(v) AS s "
                    + "FROM t GROUP BY k, ROLLUP(b) ORDER BY gid, k, b";
            String expressionQuery = "SELECT a + 1 AS k, b, GROUPING_ID(a + 1, b) AS gid, SUM(v) AS s "
                    + "FROM t GROUP BY a + 1, ROLLUP(b) ORDER BY gid, k, b";
            String groupingExpressionQuery = "SELECT a + 1 AS k, b, GROUPING(a + 1) AS g, SUM(v) AS s "
                    + "FROM t GROUP BY a + 1, ROLLUP(b) ORDER BY g, k, b";
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                assertQuery(aliasQuery)
                        .withContext(sqlExecutionContext)
                        .assertsPlanContaining("grouping(");
                assertQuery(aliasQuery)
                        .withContext(sqlExecutionContext)
                        .expectSize()
                        .returns(expected);
                assertQuery(expressionQuery)
                        .withContext(sqlExecutionContext)
                        .assertsPlanContaining("grouping(");
                assertQuery(expressionQuery)
                        .withContext(sqlExecutionContext)
                        .expectSize()
                        .returns(expected);
                assertQuery(groupingExpressionQuery)
                        .withContext(sqlExecutionContext)
                        .assertsPlanContaining("grouping(");
                assertQuery(groupingExpressionQuery)
                        .withContext(sqlExecutionContext)
                        .expectSize()
                        .returns("k\tb\tg\ts\n"
                                + "2\tnull\t0\t7.0\n"
                                + "2\t10\t0\t3.0\n"
                                + "2\t20\t0\t4.0\n"
                                + "3\tnull\t0\t5.0\n"
                                + "3\t10\t0\t5.0\n");
            }
        });
    }

    @Test
    public void testGroupingIdFunction() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE trades (symbol SYMBOL, side SYMBOL, quantity DOUBLE, ts TIMESTAMP)" +
                            " TIMESTAMP(ts) PARTITION BY DAY"
            );
            execute(
                    "INSERT INTO trades VALUES " +
                            "('BTC', 'buy', 10, '2024-01-01T00:00:00.000000Z')," +
                            "('BTC', 'sell', 20, '2024-01-01T00:01:00.000000Z')," +
                            "('ETH', 'buy', 30, '2024-01-01T00:02:00.000000Z')"
            );

            // GROUPING_ID(symbol, side) is equivalent to multi-arg GROUPING()
            assertGroupingSetsQuery(
                    "symbol\tside\tSUM\tgrp\n" +
                            "BTC\tbuy\t10.0\t0\n" +
                            "BTC\tsell\t20.0\t0\n" +
                            "ETH\tbuy\t30.0\t0\n" +
                            "BTC\t\t30.0\t1\n" +
                            "ETH\t\t30.0\t1\n" +
                            "\t\t60.0\t3\n",
                    "SELECT symbol, side, SUM(quantity), GROUPING_ID(symbol, side) AS grp " +
                            "FROM trades " +
                            "GROUP BY ROLLUP(symbol, side)"
            );
        });
    }

    @Test
    public void testGroupingIdNoArgs() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE trades (symbol SYMBOL, quantity DOUBLE, ts TIMESTAMP)" +
                            " TIMESTAMP(ts) PARTITION BY DAY"
            );

            assertException(
                    "SELECT GROUPING_ID(), SUM(quantity) " +
                            "FROM trades " +
                            "GROUP BY ROLLUP(symbol)",
                    7,
                    "GROUPING_ID() requires at least one argument"
            );
        });
    }

    @Test
    public void testGroupingIdOutsideGroupingSets() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE trades (symbol SYMBOL, quantity DOUBLE, ts TIMESTAMP)" +
                            " TIMESTAMP(ts) PARTITION BY DAY"
            );

            assertException(
                    "SELECT symbol, SUM(quantity), GROUPING_ID(symbol) " +
                            "FROM trades " +
                            "GROUP BY symbol",
                    30,
                    "GROUPING() / GROUPING_ID() can only be used with GROUPING SETS, ROLLUP, or CUBE"
            );
        });
    }

    @Test
    public void testGroupingIdRejectsNonKeyExpression() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v DOUBLE)");
            assertException(
                    "SELECT GROUPING_ID(a + 1, a + 2), SUM(v) FROM t GROUP BY a + 1, ROLLUP(b)",
                    28,
                    "argument must be a column reference"
            );
        });
    }

    @Test
    public void testGroupingIdSingleArg() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE trades (symbol SYMBOL, side SYMBOL, quantity DOUBLE, ts TIMESTAMP)" +
                            " TIMESTAMP(ts) PARTITION BY DAY"
            );
            execute(
                    "INSERT INTO trades VALUES " +
                            "('BTC', 'buy', 10, '2024-01-01T00:00:00.000000Z')," +
                            "('BTC', 'sell', 20, '2024-01-01T00:01:00.000000Z')," +
                            "('ETH', 'buy', 30, '2024-01-01T00:02:00.000000Z')"
            );

            // Single-arg GROUPING_ID works like GROUPING
            assertGroupingSetsQuery(
                    "symbol\tside\tSUM\tgs\n" +
                            "BTC\tbuy\t10.0\t0\n" +
                            "BTC\tsell\t20.0\t0\n" +
                            "ETH\tbuy\t30.0\t0\n" +
                            "BTC\t\t30.0\t0\n" +
                            "ETH\t\t30.0\t0\n" +
                            "\t\t60.0\t1\n",
                    "SELECT symbol, side, SUM(quantity), GROUPING_ID(symbol) AS gs " +
                            "FROM trades " +
                            "GROUP BY ROLLUP(symbol, side)"
            );
        });
    }

    @Test
    public void testGroupingIdSupportsExpressionAlias() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v DOUBLE)");
            execute("INSERT INTO t VALUES (1, 10, 3), (1, 20, 4), (2, 10, 5)");

            assertQuery("SELECT a + 1 AS k, b, GROUPING_ID(k, b) AS gid, SUM(v) AS s "
                    + "FROM t GROUP BY k, ROLLUP(b) ORDER BY gid, k, b")
                    .expectSize()
                    .returns("k\tb\tgid\ts\n"
                            + "2\t10\t0\t3.0\n"
                            + "2\t20\t0\t4.0\n"
                            + "3\t10\t0\t5.0\n"
                            + "2\tnull\t1\t7.0\n"
                            + "3\tnull\t1\t5.0\n");
        });
    }

    @Test
    public void testGroupingIdSupportsQualifiedSelectAlias() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v DOUBLE)");
            execute("INSERT INTO t VALUES (1, 10, 3), (1, 20, 4), (2, 10, 5)");
            String query = "SELECT t.a AS x, b, GROUPING_ID(x, b) AS gid, SUM(v) AS s "
                    + "FROM t GROUP BY ROLLUP(x, b) ORDER BY gid, x, b";
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                assertQuery(query)
                        .withContext(sqlExecutionContext)
                        .expectSize()
                        .returns("x\tb\tgid\ts\n"
                                + "1\t10\t0\t3.0\n"
                                + "1\t20\t0\t4.0\n"
                                + "2\t10\t0\t5.0\n"
                                + "1\tnull\t1\t7.0\n"
                                + "2\tnull\t1\t5.0\n"
                                + "null\tnull\t3\t12.0\n");
            }
        });
    }

    @Test
    public void testGroupingIdSupportsSelectAlias() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v DOUBLE)");
            execute("INSERT INTO t VALUES (1, 10, 3), (1, 20, 4), (2, 10, 5)");

            assertQuery("SELECT a AS x, b, GROUPING_ID(x, b) AS gid, SUM(v) AS s "
                    + "FROM t GROUP BY ROLLUP(x, b) ORDER BY gid, x, b")
                    .expectSize()
                    .returns("x\tb\tgid\ts\n"
                            + "1\t10\t0\t3.0\n"
                            + "1\t20\t0\t4.0\n"
                            + "2\t10\t0\t5.0\n"
                            + "1\tnull\t1\t7.0\n"
                            + "2\tnull\t1\t5.0\n"
                            + "null\tnull\t3\t12.0\n");
        });
    }

    @Test
    public void testGroupingIdThreeColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a SYMBOL, b SYMBOL, c SYMBOL, val DOUBLE)");
            execute("INSERT INTO t VALUES ('x', 'y', 'z', 1.0)");
            // GROUPING_ID(a, b, c) with ROLLUP(a, b, c) produces:
            // (a,b,c) -> 0b000=0, (a,b) -> 0b001=1, (a) -> 0b011=3, () -> 0b111=7
            assertGroupingSetsQuery(
                    "a\tb\tc\ts\tgid\n" +
                            "\t\t\t1.0\t7\n" +
                            "x\t\t\t1.0\t3\n" +
                            "x\ty\t\t1.0\t1\n" +
                            "x\ty\tz\t1.0\t0\n",
                    "SELECT a, b, c, SUM(val) s, GROUPING_ID(a, b, c) gid FROM t GROUP BY ROLLUP(a, b, c) ORDER BY gid DESC"
            );
            assertGroupingSetsQuery(
                    "a\tb\tc\ts\tgid\n" +
                            "\t\t\t1.0\t7\n" +
                            "x\t\t\t1.0\t5\n" +
                            "x\ty\t\t1.0\t4\n" +
                            "x\ty\tz\t1.0\t0\n",
                    "SELECT a, b, c, SUM(val) s, GROUPING_ID(c, a, b) gid FROM t GROUP BY ROLLUP(a, b, c) ORDER BY gid DESC"
            );
        });
    }

    @Test
    public void testGroupingIdTooManyArguments() throws Exception {
        // GROUPING_ID packs one bit per argument into an INT.
        assertMemoryLeak(() -> {
            StringBuilder columns = new StringBuilder("c1");
            StringBuilder ddl = new StringBuilder("CREATE TABLE w (c1 INT");
            for (int i = 2; i <= 32; i++) {
                columns.append(", c").append(i);
                ddl.append(", c").append(i).append(" INT");
            }
            execute(ddl.append(')').toString());
            String query = "SELECT c1, grouping_id(" + columns + "), count() FROM w GROUP BY GROUPING SETS ((" + columns + "))";
            assertException(query, query.indexOf("grouping_id"), "GROUPING_ID() supports at most 31 arguments");
        });
    }

    @Test
    public void testGroupingIdWithCube() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE trades (symbol SYMBOL, side SYMBOL, quantity DOUBLE, ts TIMESTAMP)" +
                            " TIMESTAMP(ts) PARTITION BY DAY"
            );
            execute(
                    "INSERT INTO trades VALUES " +
                            "('BTC', 'buy', 10, '2024-01-01T00:00:00.000000Z')," +
                            "('ETH', 'sell', 20, '2024-01-01T00:01:00.000000Z')"
            );

            // CUBE(symbol, side) produces sets: (symbol,side), (symbol), (side), ()
            // GROUPING_ID bitmasks: 0, 1, 2, 3
            assertGroupingSetsQuery(
                    "symbol\tside\tSUM\tgrp\n" +
                            "BTC\tbuy\t10.0\t0\n" +
                            "ETH\tsell\t20.0\t0\n" +
                            "BTC\t\t10.0\t1\n" +
                            "ETH\t\t20.0\t1\n" +
                            "\tbuy\t10.0\t2\n" +
                            "\tsell\t20.0\t2\n" +
                            "\t\t30.0\t3\n",
                    "SELECT symbol, side, SUM(quantity), GROUPING_ID(symbol, side) AS grp " +
                            "FROM trades " +
                            "GROUP BY CUBE(symbol, side) " +
                            "ORDER BY grp, symbol, side"
            );
        });
    }

    @Test
    public void testGroupingIdWithDuplicatePhysicalKeys() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v DOUBLE)");
            execute("INSERT INTO t VALUES (1, 10, 3), (1, 20, 4), (2, 10, 5)");
            String query = "SELECT a, a2, b, gid, s FROM ("
                    + "SELECT a, a AS a2, b, GROUPING_ID(a, a2, b) AS gid, SUM(v) AS s "
                    + "FROM t GROUP BY ROLLUP(a, a2, b)) ORDER BY gid, a, a2, b";
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                assertQuery(query)
                        .withContext(sqlExecutionContext)
                        .expectSize()
                        .returns("a\ta2\tb\tgid\ts\n"
                                + "1\t1\t10\t0\t3.0\n"
                                + "1\t1\t20\t0\t4.0\n"
                                + "2\t2\t10\t0\t5.0\n"
                                + "1\t1\tnull\t1\t7.0\n"
                                + "1\t1\tnull\t1\t7.0\n"
                                + "2\t2\tnull\t1\t5.0\n"
                                + "2\t2\tnull\t1\t5.0\n"
                                + "null\tnull\tnull\t7\t12.0\n");
            }
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                assertQuery("SELECT a, a2, b, s FROM ("
                        + "SELECT a, a AS a2, b, SUM(v) AS s "
                        + "FROM t GROUP BY ROLLUP(a, a2, b)) ORDER BY a, a2, b")
                        .withContext(sqlExecutionContext)
                        .expectSize()
                        .returns("a\ta2\tb\ts\n"
                                + "null\tnull\tnull\t12.0\n"
                                + "1\t1\tnull\t7.0\n"
                                + "1\t1\tnull\t7.0\n"
                                + "1\t1\t10\t3.0\n"
                                + "1\t1\t20\t4.0\n"
                                + "2\t2\tnull\t5.0\n"
                                + "2\t2\tnull\t5.0\n"
                                + "2\t2\t10\t5.0\n");
            }
            String reversedArgumentsQuery = "SELECT a, a2, b, gid, s FROM ("
                    + "SELECT a, a AS a2, b, GROUPING_ID(a2, a, b) AS gid, SUM(v) AS s "
                    + "FROM t GROUP BY ROLLUP(a, a2, b)) ORDER BY gid, a, a2, b";
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                assertQuery(reversedArgumentsQuery)
                        .withContext(sqlExecutionContext)
                        .expectSize()
                        .returns("a\ta2\tb\tgid\ts\n"
                                + "1\t1\t10\t0\t3.0\n"
                                + "1\t1\t20\t0\t4.0\n"
                                + "2\t2\t10\t0\t5.0\n"
                                + "1\t1\tnull\t1\t7.0\n"
                                + "1\t1\tnull\t1\t7.0\n"
                                + "2\t2\tnull\t1\t5.0\n"
                                + "2\t2\tnull\t1\t5.0\n"
                                + "null\tnull\tnull\t7\t12.0\n");
            }
        });
    }

    @Test
    public void testGroupingIdWithSwappedSelectAliases() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v DOUBLE)");
            execute("INSERT INTO t VALUES (1, 10, 3), (1, 20, 4), (2, 10, 5)");

            assertQuery("SELECT b AS a, a AS b, SUM(v) AS s FROM t GROUP BY ROLLUP(a, b) ORDER BY b, a")
                    .expectSize()
                    .returns("a\tb\ts\n"
                            + "null\tnull\t12.0\n"
                            + "10\tnull\t8.0\n"
                            + "20\tnull\t4.0\n"
                            + "10\t1\t3.0\n"
                            + "20\t1\t4.0\n"
                            + "10\t2\t5.0\n");

            assertQuery("SELECT b AS a, a AS b, GROUPING_ID(a, b) AS gid, SUM(v) AS s "
                    + "FROM t GROUP BY ROLLUP(a, b) ORDER BY gid, b, a")
                    .expectSize()
                    .returns("a\tb\tgid\ts\n"
                            + "10\t1\t0\t3.0\n"
                            + "20\t1\t0\t4.0\n"
                            + "10\t2\t0\t5.0\n"
                            + "10\tnull\t1\t8.0\n"
                            + "20\tnull\t1\t4.0\n"
                            + "null\tnull\t3\t12.0\n");
        });
    }

    @Test
    public void testGroupingInsideExpressions() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v INT)");
            execute("INSERT INTO t VALUES (1, 10, 3), (2, 20, 5), (1, 20, 7)");
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                assertGroupingSetsQuery(
                        """
                                a\tb\tg1\tg2\tg3\ts
                                1\t10\t1\t0\t0\t3
                                1\t20\t1\t0\t0\t7
                                2\t20\t1\t0\t0\t5
                                1\tnull\t1\t0\t2\t10
                                2\tnull\t1\t0\t2\t5
                                null\tnull\t2\t-1\t3\t15
                                """,
                        "SELECT a, b, GROUPING(a) + 1 g1, -abs(GROUPING(a)) g2, GROUPING_ID(b, a) * 1 g3, sum(v) s " +
                                "FROM t GROUP BY ROLLUP(a, b) ORDER BY g3, a, b"
                );
            }
        });
    }

    @Test
    public void testGroupingInsideFunctionArguments() throws Exception {
        // CASE and functions of three or more arguments keep their operands in an argument list.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v INT)");
            execute("INSERT INTO t VALUES (1, 10, 3), (2, 20, 5), (1, 20, 7)");
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                assertGroupingSetsQuery(
                        """
                                a\tk\ts
                                null\ttotal\t15
                                1\trow\t10
                                2\trow\t5
                                """,
                        "SELECT a, CASE WHEN GROUPING(a) = 1 THEN 'total' ELSE 'row' END k, sum(v) s FROM t GROUP BY ROLLUP(a) ORDER BY a"
                );
                // A SELECT alias works as it does in a top-level GROUPING().
                assertGroupingSetsQuery(
                        """
                                x\tk\ts
                                null\ttotal\t15
                                1\trow\t10
                                2\trow\t5
                                """,
                        "SELECT a x, CASE WHEN GROUPING(x) = 1 THEN 'total' ELSE 'row' END k, sum(v) s FROM t GROUP BY ROLLUP(x) ORDER BY x"
                );
                assertGroupingSetsQuery(
                        """
                                x\tb\tg\ts
                                1\t10\t0\t3
                                1\t20\t0\t7
                                2\t20\t0\t5
                                1\tnull\t1\t10
                                2\tnull\t1\t5
                                null\tnull\t3\t15
                                """,
                        "SELECT a x, b, coalesce(GROUPING_ID(x, b), 0, 0) g, sum(v) s FROM t GROUP BY ROLLUP(x, b) ORDER BY g, x, b"
                );
                // x is an alias of a, so ROLLUP(a, x) groups a twice, as in
                // testGroupingIdWithDuplicatePhysicalKeys: (a, x) and (a) give the same rows.
                assertGroupingSetsQuery(
                        """
                                a\tx\tgx\tga\ts
                                1\t1\t0\t0\t10
                                1\t1\t0\t0\t10
                                2\t2\t0\t0\t5
                                2\t2\t0\t0\t5
                                null\tnull\t1\t1\t15
                                """,
                        "SELECT a, a AS x, coalesce(GROUPING(x), 9, 9) gx, coalesce(GROUPING(a), 9, 9) ga, sum(v) s " +
                                "FROM t GROUP BY ROLLUP(a, x) ORDER BY gx, ga, a"
                );
                // Nested and top-level GROUPING_ID() agree with reversed duplicate keys.
                assertGroupingSetsQuery(
                        """
                                a\ta2\tb\tgid\tgid2\ts
                                1\t1\t10\t0\t0\t3
                                1\t1\t20\t0\t0\t7
                                2\t2\t20\t0\t0\t5
                                1\t1\tnull\t1\t1\t10
                                1\t1\tnull\t1\t1\t10
                                2\t2\tnull\t1\t1\t5
                                2\t2\tnull\t1\t1\t5
                                null\tnull\tnull\t7\t7\t15
                                """,
                        "SELECT a, a AS a2, b, coalesce(GROUPING_ID(a2, a, b), 0, 0) gid, GROUPING_ID(a2, a, b) gid2, sum(v) s " +
                                "FROM t GROUP BY ROLLUP(a, a2, b) ORDER BY gid, a, a2, b"
                );
                String query = "SELECT a, CASE WHEN GROUPING(b) = 1 THEN 'all' ELSE 'one' END k FROM t GROUP BY ROLLUP(a)";
                assertQuery(query).withContext(sqlExecutionContext).fails(
                        query.indexOf("GROUPING(b)") + "GROUPING(".length(),
                        "argument must be a grouping key column"
                );
            }
        });
    }

    @Test
    public void testGroupingNoArgs() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE trades (symbol SYMBOL, quantity DOUBLE, ts TIMESTAMP)" +
                            " TIMESTAMP(ts) PARTITION BY DAY"
            );

            assertException(
                    "SELECT GROUPING(), SUM(quantity) " +
                            "FROM trades " +
                            "GROUP BY ROLLUP(symbol)",
                    7,
                    "GROUPING() requires exactly one column argument"
            );
        });
    }

    @Test
    public void testGroupingRejectsNonColumnArgumentsAtArgumentPosition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, v DOUBLE)");
            String expressionQuery = "SELECT GROUPING(a + 2), SUM(v) FROM t GROUP BY ROLLUP(a)";
            assertException(expressionQuery, expressionQuery.indexOf("+"), "argument must be a column reference");

            String constantQuery = "SELECT GROUPING(1), SUM(v) FROM t GROUP BY ROLLUP(a)";
            assertException(constantQuery, constantQuery.indexOf("1)"), "argument must be a column reference");
        });
    }

    @Test
    public void testGroupingSetsAliasEquivalentKeys() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, v DOUBLE)");
            execute("INSERT INTO t VALUES (1,3), (1,4), (2,5)");
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                assertQuery("SELECT a AS x, GROUPING(a) AS g, SUM(v) AS s FROM t GROUP BY GROUPING SETS ((a), (x), ()) ORDER BY g,x")
                        .withContext(sqlExecutionContext).expectSize().returns("""
                                x\tg\ts
                                1\t0\t7.0
                                1\t0\t7.0
                                2\t0\t5.0
                                2\t0\t5.0
                                null\t1\t12.0
                                """);
            }
        });
    }

    @Test
    public void testGroupingSetsExplicit() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE trades (symbol SYMBOL, side SYMBOL, quantity DOUBLE, ts TIMESTAMP)" +
                            " TIMESTAMP(ts) PARTITION BY DAY"
            );
            execute(
                    "INSERT INTO trades VALUES " +
                            "('BTC', 'buy', 10, '2024-01-01T00:00:00.000000Z')," +
                            "('BTC', 'sell', 20, '2024-01-01T00:01:00.000000Z')," +
                            "('ETH', 'buy', 30, '2024-01-01T00:02:00.000000Z')," +
                            "('ETH', 'sell', 40, '2024-01-01T00:03:00.000000Z')"
            );

            assertGroupingSetsQuery(
                    "symbol\tside\tSUM\n" +
                            "BTC\tbuy\t10.0\n" +
                            "BTC\tsell\t20.0\n" +
                            "ETH\tbuy\t30.0\n" +
                            "ETH\tsell\t40.0\n" +
                            "\t\t100.0\n",
                    "SELECT symbol, side, SUM(quantity) " +
                            "FROM trades " +
                            "GROUP BY GROUPING SETS ((symbol, side), ())"
            );
        });
    }

    @Test
    public void testGroupingSetsExpressionRejectedInCube() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v DOUBLE)");
            assertException(
                    "SELECT SUM(v) FROM t GROUP BY CUBE(a + b)",
                    37,
                    "expression keys in ROLLUP, CUBE and GROUPING SETS must be computed in a subquery"
            );
        });
    }

    @Test
    public void testGroupingSetsExpressionRejectedInExplicit() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v DOUBLE)");
            assertException(
                    "SELECT SUM(v) FROM t GROUP BY GROUPING SETS ((a + b))",
                    48,
                    "expression keys in ROLLUP, CUBE and GROUPING SETS must be computed in a subquery"
            );
            // A missing key is not an expression, so it keeps the plain message.
            String query = "SELECT SUM(v) FROM t GROUP BY GROUPING SETS ((a, ))";
            assertException(query, query.indexOf("))"), "column reference expected");
        });
    }

    @Test
    public void testGroupingSetsExpressionRejectedInRollup() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v DOUBLE)");
            assertException(
                    "SELECT SUM(v) FROM t GROUP BY ROLLUP(a + b)",
                    39,
                    "expression keys in ROLLUP, CUBE and GROUPING SETS must be computed in a subquery"
            );
        });
    }

    @Test
    public void testGroupingSetsExpressionRejectedInSampleByRollup() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE t (a INT, b INT, v DOUBLE, ts TIMESTAMP)" +
                            " TIMESTAMP(ts) PARTITION BY DAY"
            );
            assertException(
                    "SELECT SUM(v) FROM t SAMPLE BY 1h ROLLUP(a + b)",
                    43,
                    "expression keys in ROLLUP, CUBE and GROUPING SETS must be computed in a subquery"
            );
        });
    }

    @Test
    public void testGroupingSetsLimitAccepted() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v INT)");
            assertGroupingSetsQuery("a\tSUM\n", "SELECT a,SUM(v) FROM t GROUP BY ROLLUP(a) LIMIT 0");
            assertGroupingSetsQuery("a\tSUM\n", "SELECT a,SUM(v) FROM t GROUP BY CUBE(a) LIMIT 0");
            assertGroupingSetsQuery("a\tb\tSUM\n", "SELECT a,b,SUM(v) FROM t GROUP BY a, ROLLUP(b) LIMIT 0");
            assertGroupingSetsQuery("a\tb\tSUM\n", "SELECT a,b,SUM(v) FROM t GROUP BY a, CUBE(b) LIMIT 0");
            assertGroupingSetsQuery("a\tSUM\n", "SELECT a,SUM(v) FROM t GROUP BY GROUPING SETS ((a)) LIMIT 0");
        });
    }

    @Test
    public void testGroupingSetsLimitFromConfiguration() throws Exception {
        // cairo.sql.max.grouping.sets bounds every way of spelling the sets.
        assertMemoryLeak(() -> {
            // An embedded engine defaults to the server's limit.
            Assert.assertEquals(4096, new DefaultCairoConfiguration(root).getSqlMaxGroupingSets());
            setProperty(PropertyKey.CAIRO_SQL_MAX_GROUPING_SETS, 2);
            execute("CREATE TABLE t (a INT, b INT, v INT)");
            String query = "SELECT a, b, count() FROM t GROUP BY GROUPING SETS ((a), (b), ())";
            assertException(query, query.indexOf("())") + 1, "too many grouping sets (maximum 2)");
            query = "SELECT a, b, count() FROM t GROUP BY ROLLUP(a, b)";
            assertException(query, query.indexOf("ROLLUP"), "ROLLUP produces too many grouping sets (3, maximum 2)");
            query = "SELECT a, b, v, count() FROM t GROUP BY a, ROLLUP(b, v)";
            assertException(query, query.indexOf("ROLLUP"), "ROLLUP produces too many grouping sets (3, maximum 2)");
            query = "SELECT a, b, v, count() FROM t GROUP BY a, CUBE(b, v)";
            assertException(query, query.indexOf("CUBE"), "CUBE produces too many grouping sets (4, maximum 2)");
            // Within the limit the query runs: over no rows, ROLLUP(a) is the grand total alone.
            assertGroupingSetsQuery("a\tcount\nnull\t0\n", "SELECT a, count() FROM t GROUP BY ROLLUP(a)");
        });
    }

    @Test
    public void testGroupingSetsListSyntaxErrors() throws Exception {
        // As in PostgreSQL, a GROUPING SETS list holds at least one set and no trailing comma.
        // Unlike PostgreSQL, every element is parenthesized: (a), not a.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, v INT, ts TIMESTAMP) TIMESTAMP(ts)");
            for (String prefix : new String[]{
                    "SELECT a, sum(v) FROM t GROUP BY ",
                    "SELECT ts, a, sum(v) FROM t SAMPLE BY 1d "
            }) {
                String query = prefix + "GROUPING SETS ()";
                assertException(query, query.length() - 1, "'(' expected");
                query = prefix + "GROUPING SETS ((a), )";
                assertException(query, query.length() - 1, "'(' expected");
                query = prefix + "GROUPING SETS ((a),";
                assertException(query, query.length(), "'(' expected");
                query = prefix + "GROUPING SETS ((a)";
                assertException(query, query.length(), "',' or ')' expected");
                query = prefix + "GROUPING SETS ((a) (a))";
                assertException(query, query.indexOf("(a))"), "',' expected");
                query = prefix + "GROUPING SETS (a, ())";
                assertException(query, query.indexOf("a, ())"), "'(' expected");
            }
        });
    }

    @Test
    public void testGroupingSetsManyKeysWithGroupingFunction() throws Exception {
        // GROUPING() and GROUPING_ID() read only their own arguments' bits, so a query may group
        // by more keys than an INT has bits. The second set rolls up the first and the last key.
        assertMemoryLeak(() -> {
            StringBuilder ddl = new StringBuilder("CREATE TABLE w (c1 INT");
            StringBuilder values = new StringBuilder("1");
            StringBuilder allKeys = new StringBuilder("c1");
            StringBuilder innerKeys = new StringBuilder();
            for (int i = 2; i <= 40; i++) {
                ddl.append(", c").append(i).append(" INT");
                values.append(", ").append(i);
                allKeys.append(", c").append(i);
                if (i < 40) {
                    innerKeys.append(i == 2 ? "" : ", ").append('c').append(i);
                }
            }
            execute(ddl.append(')').toString());
            execute("INSERT INTO w VALUES (" + values + ")");
            String query = "SELECT c1, c2, c40, grouping(c1) g1, grouping(c40) g40, grouping_id(c1, c2, c40) gid, count() n " +
                    "FROM w GROUP BY GROUPING SETS ((" + allKeys + "), (" + innerKeys + ")) ORDER BY gid";
            for (int i = 0; i < 2; i++) {
                sqlExecutionContext.setParallelGroupByEnabled(i == 1);
                assertQuery(query)
                        .withContext(sqlExecutionContext)
                        .withPlanContaining(i == 1 ? "Async Group By" : "GroupBy vectorized: false")
                        .sizeMayVary()
                        .returns("""
                                c1\tc2\tc40\tg1\tg40\tgid\tn
                                1\t2\t40\t0\t0\t0\t1
                                null\t2\tnull\t1\t1\t5\t1
                                """);
            }
        });
    }

    @Test
    public void testGroupingSetsNonOverlapping() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE trades (symbol SYMBOL, side SYMBOL, quantity DOUBLE, ts TIMESTAMP)" +
                            " TIMESTAMP(ts) PARTITION BY DAY"
            );
            execute(
                    "INSERT INTO trades VALUES " +
                            "('BTC', 'buy', 10, '2024-01-01T00:00:00.000000Z')," +
                            "('BTC', 'sell', 20, '2024-01-01T00:01:00.000000Z')," +
                            "('ETH', 'buy', 30, '2024-01-01T00:02:00.000000Z')," +
                            "('ETH', 'sell', 40, '2024-01-01T00:03:00.000000Z')"
            );

            // Non-overlapping sets: group by symbol only, then by side only
            assertGroupingSetsQuery(
                    "symbol\tside\tSUM\n" +
                            "BTC\t\t30.0\n" +
                            "ETH\t\t70.0\n" +
                            "\tbuy\t40.0\n" +
                            "\tsell\t60.0\n",
                    "SELECT symbol, side, SUM(quantity) " +
                            "FROM trades " +
                            "GROUP BY GROUPING SETS ((symbol), (side))"
            );
        });
    }

    @Test
    public void testGroupingSetsOuterAliasedFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v DOUBLE)");
            execute("INSERT INTO t VALUES (1,10,3), (1,20,4), (2,10,5), (2,null,2)");
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                assertQuery("""
                        WITH g AS (SELECT a,b,SUM(v) s FROM t GROUP BY GROUPING SETS ((a,b),(a),()))
                        SELECT * FROM (SELECT a x,b y,s z FROM (SELECT * FROM g))
                        WHERE (y=10 OR y IS NULL) AND (x=2 OR x IS NULL) ORDER BY x,y,z
                        """)
                        .withContext(sqlExecutionContext).returns("""
                                x\ty\tz
                                null\tnull\t14.0
                                2\tnull\t2.0
                                2\tnull\t7.0
                                2\t10\t5.0
                                """);
            }
        });
    }

    @Test
    public void testGroupingSetsParserError() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT, y DOUBLE)");
            assertException(
                    "SELECT x, SUM(y) FROM t GROUP BY GROUPING x",
                    42,
                    "unexpected token [x]"
            );
        });
    }

    @Test
    public void testGroupingSetsTrivialExpressionKeyOverGroupedColumn() throws Exception {
        // a and a - 1 share one base column, the shape the trivial GROUP BY expression rewrite
        // lifts out of the GROUP BY. k is in every set, so the alias is a valid key.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT)");
            execute("INSERT INTO t VALUES (1), (1), (2)");
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                assertQuery("SELECT a, a - 1 AS k, count() c FROM t GROUP BY GROUPING SETS ((a, k), (k)) ORDER BY k, a")
                        .withContext(sqlExecutionContext)
                        .expectSize()
                        .returns("""
                                a\tk\tc
                                null\t0\t2
                                1\t0\t2
                                null\t1\t1
                                2\t1\t1
                                """);
            }
        });
    }

    @Test
    public void testGroupingSetsWithMultipleAggregates() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a SYMBOL, v DOUBLE)");
            execute("INSERT INTO t VALUES ('x', 10.0), ('x', 20.0), ('y', 30.0)");
            assertGroupingSetsQuery(
                    "a\tSUM\tCOUNT\tAVG\n" +
                            "\t60.0\t3\t20.0\n" +
                            "x\t30.0\t2\t15.0\n" +
                            "y\t30.0\t1\t30.0\n",
                    "SELECT a, SUM(v), COUNT(v), AVG(v) FROM t GROUP BY ROLLUP(a) ORDER BY a"
            );
        });
    }

    @Test
    public void testGroupingSetsWithOrdinalGroupByAndRollup() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v DOUBLE)");
            execute("INSERT INTO t VALUES (1, 10, 3), (1, 20, 4), (2, 10, 5)");

            assertQuery("SELECT a, b, SUM(v) AS s FROM t GROUP BY 1, ROLLUP(b) ORDER BY a, b")
                    .expectSize()
                    .returns("a\tb\ts\n"
                            + "1\tnull\t7.0\n"
                            + "1\t10\t3.0\n"
                            + "1\t20\t4.0\n"
                            + "2\tnull\t5.0\n"
                            + "2\t10\t5.0\n");
        });
    }

    @Test
    public void testGroupingSetsWithReversedProjectionOrdinalAndRollup() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v DOUBLE)");
            execute("INSERT INTO t VALUES (1, 10, 3), (1, 20, 4), (2, 10, 5)");

            assertQuery("SELECT b, a, SUM(v) AS s FROM t GROUP BY 2, ROLLUP(b) ORDER BY a, b")
                    .expectSize()
                    .returns("b\ta\ts\n"
                            + "null\t1\t7.0\n"
                            + "10\t1\t3.0\n"
                            + "20\t1\t4.0\n"
                            + "null\t2\t5.0\n"
                            + "10\t2\t5.0\n");
        });
    }

    @Test
    public void testGroupingSupportsSelectAlias() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, v DOUBLE)");
            execute("INSERT INTO t VALUES (1, 3), (2, 5)");

            assertQuery("SELECT a AS x, GROUPING(x) AS g, SUM(v) AS s FROM t GROUP BY ROLLUP(x) ORDER BY g, x")
                    .expectSize()
                    .returns("x\tg\ts\n"
                            + "1\t0\t3.0\n"
                            + "2\t0\t5.0\n"
                            + "null\t1\t8.0\n");
        });
    }

    @Test
    public void testGroupingSyntaxWordsCanBeColumnNames() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (cube INT, rollup INT, grouping INT, v INT)");
            execute("INSERT INTO t VALUES (1, 10, 100, 2), (1, 20, 200, 3)");

            assertQuery("SELECT cube, rollup, grouping, SUM(v) FROM t GROUP BY cube, rollup, grouping ORDER BY cube, rollup, grouping")
                    .expectSize()
                    .returns("cube\trollup\tgrouping\tSUM\n"
                            + "1\t10\t100\t2\n"
                            + "1\t20\t200\t3\n");
            assertQuery("SELECT rollup, cube, SUM(v) FROM t GROUP BY rollup, cube ORDER BY rollup, cube")
                    .expectSize()
                    .returns("rollup\tcube\tSUM\n10\t1\t2\n20\t1\t3\n");
        });
    }

    @Test
    public void testGroupingSyntaxWordsInExpressionsAndCaseVariants() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (cube INT, rollup INT, grouping INT, v INT)");
            execute("INSERT INTO t VALUES (1, 10, 100, 2), (2, 20, 200, 3)");

            assertQuery("SELECT cube + 1 AS k, SUM(v) AS s FROM t GROUP BY cube + 1 ORDER BY k")
                    .expectSize()
                    .returns("k\ts\n2\t2\n3\t3\n");
            assertQuery("SELECT rollup * 2 AS k, SUM(v) AS s FROM t GROUP BY rollup * 2 ORDER BY k")
                    .expectSize()
                    .returns("k\ts\n20\t2\n40\t3\n");
            assertQuery("SELECT cube::string AS k, SUM(v) AS s FROM t GROUP BY cube::string ORDER BY k")
                    .expectSize()
                    .returns("k\ts\n1\t2\n2\t3\n");
            assertQuery("SELECT grouping + 1 AS k, SUM(v) AS s FROM t GROUP BY grouping + 1 ORDER BY k")
                    .expectSize()
                    .returns("k\ts\n101\t2\n201\t3\n");

            assertQuery("SELECT cube, SUM(v) AS s FROM t GROUP BY grouping sets ((cube), ()) ORDER BY cube")
                    .expectSize()
                    .returns("cube\ts\nnull\t5\n1\t2\n2\t3\n");
            assertQuery("SELECT cube, SUM(v) AS s FROM t GROUP BY cube ORDER BY cube")
                    .expectSize()
                    .returns("cube\ts\n1\t2\n2\t3\n");
        });
    }

    @Test
    public void testHorizonJoinWithGroupingSetsRejected() throws Exception {
        // The HORIZON JOIN factories have no per-set maps, so rolled-up rows would be lost.
        // A subquery around the HORIZON JOIN applies the grouping sets instead.
        assertMemoryLeak(() -> {
            createHorizonJoinTables();
            final String horizon = "FROM trades t HORIZON JOIN prices p ON (t.sym = p.sym) LIST (0, 1s) AS h ";
            final String[] queries = {
                    "SELECT t.sym, sum(p.price) " + horizon + "GROUP BY ROLLUP(t.sym)",
                    "SELECT t.sym, sum(p.price) " + horizon + "GROUP BY CUBE(t.sym)",
                    "SELECT t.sym, sum(p.price) " + horizon + "GROUP BY GROUPING SETS ((t.sym), ())",
                    "SELECT t.sym, GROUPING(t.sym) g, sum(p.price) " + horizon + "GROUP BY ROLLUP(t.sym)",
            };
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                for (String query : queries) {
                    assertException(query, query.indexOf("GROUP BY"), "ROLLUP, CUBE and GROUPING SETS are not supported with HORIZON JOIN");
                }
                // Each trade has one price per offset: A 10 at both offsets, B 20 at both.
                assertQuery("SELECT sym, sum(s) s FROM (SELECT t.sym, sum(p.price) s " + horizon + ") GROUP BY ROLLUP(sym) ORDER BY sym")
                        .withContext(sqlExecutionContext)
                        .expectSize()
                        .returns("""
                                sym\ts
                                \t60.0
                                A\t20.0
                                B\t40.0
                                """);
            }
        });
    }

    @Test
    public void testInjectedFaultsKeepFactoryReusable() throws Exception {
        // A failure while the cursor opens or while it scans must release what the cursor
        // holds and leave the compiled factory able to run again.
        assertMemoryLeak(() -> {
            // test_fault() folds to TRUE outside dev mode.
            setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
            execute("CREATE TABLE t (a INT, b INT, v INT)");
            execute("INSERT INTO t VALUES (1, 10, 3), (2, 20, 5), (1, 20, 7)");
            final String expected = """
                    a\tb\ts
                    null\tnull\t15
                    1\tnull\t10
                    1\t10\t3
                    1\t20\t7
                    2\tnull\t5
                    2\t20\t5
                    """;
            final String query = "SELECT a, b, sum(CASE WHEN test_fault() THEN v ELSE 0 END) s FROM t GROUP BY ROLLUP(a, b) ORDER BY a, b";
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                try (RecordCursorFactory factory = select(query)) {
                    for (int stage = 0; stage < 2; stage++) {
                        if (stage == 0) {
                            TestFaultFunctionFactory.armToFailAfterInits(0);
                        } else {
                            TestFaultFunctionFactory.armToFailAfter(1);
                        }
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            TestUtils.assertCursor(expected, cursor, factory.getMetadata(), true, sink);
                            Assert.fail("expected an injected fault, mode=" + mode + ", stage=" + stage);
                        } catch (Throwable e) {
                            TestUtils.assertContains(e.getMessage(), stage == 0 ? "test_fault: injected init failure" : "test_fault: injected failure");
                        } finally {
                            TestFaultFunctionFactory.disarm();
                        }
                        Assert.assertEquals(1, TestFaultFunctionFactory.faultsTriggered());
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            TestUtils.assertCursor(expected, cursor, factory.getMetadata(), true, sink);
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testLateralCorrelatedGroupingSetsRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE u (a INT, w INT, ts TIMESTAMP) TIMESTAMP(ts)");
            final String message = "correlated GROUPING SETS are not supported with LATERAL";
            // GROUP BY grouping sets point at GROUP BY.
            String[] groupByQueries = {
                    "SELECT * FROM t o JOIN LATERAL (SELECT w, count() n FROM u WHERE u.a = o.a GROUP BY ROLLUP(w)) l",
                    "SELECT * FROM t o JOIN LATERAL (SELECT count() n FROM u WHERE u.a = o.a GROUP BY GROUPING SETS (())) l",
                    "SELECT * FROM t o JOIN LATERAL (SELECT w, count() n FROM (SELECT * FROM u WHERE u.a = o.a) GROUP BY ROLLUP(w)) l"
            };
            for (String query : groupByQueries) {
                assertException(query, query.indexOf("GROUP BY"), message);
            }
            // SAMPLE BY grouping sets have no GROUP BY clause: the error points at the first
            // grouping key, or at the stride when the only set is empty.
            String query = "SELECT * FROM t o JOIN LATERAL (SELECT ts, w, count() n FROM u WHERE u.a = o.a SAMPLE BY 1d ROLLUP(w)) l";
            assertException(query, query.indexOf("ROLLUP(w)") + "ROLLUP(".length(), message);
            query = "SELECT * FROM t o JOIN LATERAL (SELECT ts, count() n FROM u WHERE u.a = o.a SAMPLE BY 1d GROUPING SETS (())) l";
            assertException(query, query.indexOf("1d"), message);
        });
    }

    @Test
    public void testLateralOverRollupSubquery() throws Exception {
        // The LATERAL rewrite reads the outer ROLLUP query through a shared, read-only wrapper.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, v INT)");
            execute("INSERT INTO t VALUES (1, 3), (2, 5), (1, 7)");
            execute("CREATE TABLE u (a INT, w INT)");
            execute("INSERT INTO u VALUES (1, 100), (2, 200), (1, 300)");
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                assertGroupingSetsQuery(
                        """
                                a\ts\tn
                                null\t15\t0
                                1\t10\t2
                                2\t5\t1
                                """,
                        "SELECT o.a, o.s, l.n FROM (SELECT a, sum(v) s FROM t GROUP BY ROLLUP(a)) o " +
                                "JOIN LATERAL (SELECT count() n FROM u WHERE u.a = o.a) l ORDER BY o.a"
                );
            }
            QueryModelWrapper wrapper = new QueryModelWrapper().of(QueryModel.FACTORY.newInstance(), 1);
            Assert.assertThrows(UnsupportedOperationException.class, () -> wrapper.setGroupByPosition(1));
        });
    }

    @Test
    public void testLatestOnWithCubeRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE t (sym SYMBOL, v DOUBLE, ts TIMESTAMP)" +
                            " TIMESTAMP(ts) PARTITION BY DAY"
            );
            assertException(
                    "SELECT sym, SUM(v) FROM t LATEST ON ts PARTITION BY sym GROUP BY CUBE(sym)",
                    65,
                    "LATEST ON is not supported with GROUPING SETS, ROLLUP, or CUBE"
            );
        });
    }

    @Test
    public void testLatestOnWithExplicitGroupingSetsRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE t (sym SYMBOL, v DOUBLE, ts TIMESTAMP)" +
                            " TIMESTAMP(ts) PARTITION BY DAY"
            );
            assertException(
                    "SELECT sym, SUM(v) FROM t LATEST ON ts PARTITION BY sym GROUP BY GROUPING SETS ((sym), ())",
                    65,
                    "LATEST ON is not supported with GROUPING SETS, ROLLUP, or CUBE"
            );
        });
    }

    @Test
    public void testLatestOnWithGroupingSetsRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE t (sym SYMBOL, v DOUBLE, ts TIMESTAMP)" +
                            " TIMESTAMP(ts) PARTITION BY DAY"
            );
            assertException(
                    "SELECT sym, SUM(v) FROM t LATEST ON ts PARTITION BY sym GROUP BY ROLLUP(sym)",
                    65,
                    "LATEST ON is not supported with GROUPING SETS, ROLLUP, or CUBE"
            );
        });
    }

    @Test
    public void testMixedQualificationRejectedInCompositeRollup() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v DOUBLE)");
            assertException(
                    "SELECT SUM(v) FROM t GROUP BY a, ROLLUP(t.a)",
                    40,
                    "mixing qualified and unqualified references to the same column"
            );
        });
    }

    @Test
    public void testMixedQualificationRejectedInCube() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v DOUBLE)");
            assertException(
                    "SELECT SUM(v) FROM t GROUP BY CUBE(a, t.a)",
                    38,
                    "mixing qualified and unqualified references to the same column"
            );
        });
    }

    @Test
    public void testMixedQualificationRejectedInExplicitGroupingSets() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v DOUBLE)");
            assertException(
                    "SELECT SUM(v) FROM t GROUP BY GROUPING SETS ((a), (t.a))",
                    51,
                    "mixing qualified and unqualified references to the same column"
            );
        });
    }

    @Test
    public void testMixedQualificationRejectedInRollup() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v DOUBLE)");
            assertException(
                    "SELECT SUM(v) FROM t GROUP BY ROLLUP(a, t.a)",
                    40,
                    "mixing qualified and unqualified references to the same column"
            );
        });
    }

    @Test
    public void testOuterRollupOverSampleByFillHasNoHiddenFillKey() throws Exception {
        // The inner SAMPLE BY owns the fill. The outer ROLLUP fills nothing, so it computes no
        // hidden GROUPING_ID fill key and needs no column-dropping wrapper.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, c INT, v LONG) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('2024-01-01', 1, 10), ('2024-01-03', 2, 20)");
            final String inner = "SELECT ts, c, count() n FROM t SAMPLE BY 1d FILL(0)";
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                final String query = "SELECT c, sum(n) s FROM (" + inner + ") GROUP BY ROLLUP(c) ORDER BY c";
                assertQuery(query).withContext(sqlExecutionContext).assertsPlanNotContaining("grouping(", "SelectedRecord");
                assertSqlCursors(
                        "SELECT * FROM (SELECT c, sum(n) s FROM (" + inner + ") GROUP BY c " +
                                "UNION ALL SELECT NULL::INT c, sum(n) s FROM (" + inner + ")) ORDER BY c",
                        query
                );
            }
        });
    }

    @Test
    public void testPostingIndexDistinctRollupGrandTotal() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (sym SYMBOL INDEX TYPE POSTING)");
            execute("INSERT INTO t VALUES ('A'), ('B')");

            assertQuery("SELECT sym FROM t GROUP BY ROLLUP(sym) ORDER BY sym")
                    .expectSize()
                    .returns("sym\n\nA\nB\n");
        });
    }

    @Test
    public void testPostingIndexDistinctWithDuplicateGroupingSets() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (sym SYMBOL INDEX TYPE POSTING)");
            execute("INSERT INTO t VALUES ('A'), ('B')");

            assertQuery("SELECT sym FROM t GROUP BY GROUPING SETS ((sym), (sym)) ORDER BY sym")
                    .expectSize()
                    .returns("sym\nA\nA\nB\nB\n");
        });
    }

    @Test
    public void testQualifiedReferencesAccepted() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v DOUBLE)");
            execute("INSERT INTO t VALUES (1, 10, 100.0), (1, 20, 200.0), (2, 10, 300.0)");
            // All qualified - no mixed qualification error
            assertGroupingSetsQuery(
                    "a\tSUM\n" +
                            "null\t600.0\n" +
                            "1\t300.0\n" +
                            "2\t300.0\n",
                    "SELECT t.a, SUM(v) FROM t GROUP BY ROLLUP(t.a) ORDER BY t.a"
            );
        });
    }

    @Test
    public void testRollupAliasesQualifiedAndDuplicateProjection() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v DOUBLE)");
            execute("INSERT INTO t VALUES (1, 10, 3), (1, 20, 4), (2, 10, 5)");
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                String expected = """
                        y\tx\tx2\tg\tgid\ts
                        10\t1\t1\t0\t0\t3.0
                        20\t1\t1\t0\t0\t4.0
                        10\t2\t2\t0\t0\t5.0
                        null\t1\t1\t1\t1\t7.0
                        null\t2\t2\t1\t1\t5.0
                        null\tnull\tnull\t1\t3\t12.0
                        """;
                assertQuery("SELECT b AS y, a AS x, a AS x2, GROUPING(b) AS g, GROUPING_ID(a,b) AS gid, SUM(v) AS s FROM t GROUP BY ROLLUP(x,y) ORDER BY gid,x,y")
                        .withContext(sqlExecutionContext).expectSize().returns(expected);
                assertQuery("SELECT t.b AS y, t.a AS x, t.a AS x2, GROUPING(t.b) AS g, GROUPING_ID(t.a,t.b) AS gid, SUM(v) AS s FROM t GROUP BY ROLLUP(t.a,t.b) ORDER BY gid,x,y")
                        .withContext(sqlExecutionContext).expectSize().returns(expected);
            }
        });
    }

    @Test
    public void testRollupArrayKeyWithPlainKey() throws Exception {
        // A rolled-up array key reads as a NULL array.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (k INT, arr DOUBLE[])");
            execute("INSERT INTO t VALUES (1, ARRAY[1.0, 2.0]), (1, ARRAY[1.0, 2.0]), (2, ARRAY[3.0])");
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                assertGroupingSetsQuery(
                        """
                                k\tarr\tn\tg
                                1\t[1.0,2.0]\t2\t0
                                2\t[3.0]\t1\t0
                                1\tnull\t2\t1
                                2\tnull\t1\t1
                                null\tnull\t3\t3
                                """,
                        "SELECT k, arr, count() n, GROUPING_ID(k, arr) g FROM t GROUP BY ROLLUP(k, arr) ORDER BY g, k"
                );
            }
        });
    }

    @Test
    public void testRollupBasic() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE trades (symbol SYMBOL, side SYMBOL, quantity DOUBLE, ts TIMESTAMP)" +
                            " TIMESTAMP(ts) PARTITION BY DAY"
            );
            execute(
                    "INSERT INTO trades VALUES " +
                            "('BTC', 'buy', 10, '2024-01-01T00:00:00.000000Z')," +
                            "('BTC', 'sell', 20, '2024-01-01T00:01:00.000000Z')," +
                            "('ETH', 'buy', 30, '2024-01-01T00:02:00.000000Z')," +
                            "('ETH', 'sell', 40, '2024-01-01T00:03:00.000000Z')"
            );

            assertGroupingSetsQuery(
                    "symbol\tside\tSUM\n" +
                            "BTC\tbuy\t10.0\n" +
                            "BTC\tsell\t20.0\n" +
                            "ETH\tbuy\t30.0\n" +
                            "ETH\tsell\t40.0\n" +
                            "BTC\t\t30.0\n" +
                            "ETH\t\t70.0\n" +
                            "\t\t100.0\n",
                    "SELECT symbol, side, SUM(quantity) " +
                            "FROM trades " +
                            "GROUP BY ROLLUP(symbol, side)"
            );
        });
    }

    @Test
    public void testRollupCaseInsensitive() throws Exception {
        // ROLLUP keyword should be case-insensitive
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT, y DOUBLE)");
            execute("INSERT INTO t VALUES (1, 10), (2, 20)");

            assertGroupingSetsQuery(
                    "x\tSUM\n" +
                            "null\t30.0\n" +
                            "1\t10.0\n" +
                            "2\t20.0\n",
                    "SELECT x, SUM(y) FROM t GROUP BY rollup(x) ORDER BY x"
            );
        });
    }

    @Test
    public void testRollupConstantPrefix() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, v DOUBLE)");
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                assertQuery("SELECT 1 + 0 AS k, a, SUM(v) AS s FROM t GROUP BY 1 + 0, ROLLUP(a) ORDER BY a")
                        .withContext(sqlExecutionContext).expectSize().returns("k\ta\ts\n");
            }
            execute("INSERT INTO t VALUES (1,3), (2,5)");
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                assertQuery("SELECT 1 + 0 AS k, a, SUM(v) AS s FROM t GROUP BY 1 + 0, ROLLUP(a) ORDER BY a")
                        .withContext(sqlExecutionContext).expectSize().returns("""
                                k\ta\ts
                                1\tnull\t8.0
                                1\t1\t3.0
                                1\t2\t5.0
                                """);
            }
        });
    }

    @Test
    public void testRollupCteReferencedTwice() throws Exception {
        // A CTE read twice shares one model between both readers.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v INT)");
            execute("INSERT INTO t VALUES (1, 10, 3), (2, 20, 5), (1, 20, 7)");
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                assertGroupingSetsQuery(
                        """
                                a\tb\ts\tg
                                null\tnull\t15\t3
                                null\tnull\t15\t3
                                1\tnull\t10\t1
                                1\tnull\t10\t1
                                1\t10\t3\t0
                                1\t10\t3\t0
                                1\t20\t7\t0
                                1\t20\t7\t0
                                2\tnull\t5\t1
                                2\tnull\t5\t1
                                2\t20\t5\t0
                                2\t20\t5\t0
                                """,
                        "WITH r AS (SELECT a, b, sum(v) s, GROUPING_ID(a, b) g FROM t GROUP BY ROLLUP(a, b)) " +
                                "SELECT * FROM (SELECT * FROM r UNION ALL SELECT * FROM r) ORDER BY a, b, s"
                );
            }
        });
    }

    @Test
    public void testRollupDuplicatePrefixBeforeRolledKey() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v DOUBLE)");
            execute("INSERT INTO t VALUES (1,10,3), (1,20,4), (2,10,5)");
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                assertQuery("SELECT b, a, GROUPING_ID(a,b) AS gid, SUM(v) AS s FROM t GROUP BY a,a,ROLLUP(b) ORDER BY gid,a,b")
                        .withContext(sqlExecutionContext).expectSize().returns("""
                                b\ta\tgid\ts
                                10\t1\t0\t3.0
                                20\t1\t0\t4.0
                                10\t2\t0\t5.0
                                null\t1\t1\t7.0
                                null\t2\t1\t5.0
                                """);
            }
        });
    }

    @Test
    public void testRollupDuplicatePrefixKey() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, v DOUBLE)");
            execute("INSERT INTO t VALUES (1,3), (1,4), (2,5)");
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                assertQuery("SELECT a, GROUPING(a) AS g, SUM(v) AS s FROM t GROUP BY a, ROLLUP(a) ORDER BY a,s")
                        .withContext(sqlExecutionContext).expectSize().returns("""
                                a\tg\ts
                                1\t0\t7.0
                                1\t0\t7.0
                                2\t0\t5.0
                                2\t0\t5.0
                                """);
            }
        });
    }

    @Test
    public void testRollupEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT, y DOUBLE)");

            // Empty table produces the grand-total row for the empty grouping set.
            assertGroupingSetsQuery(
                    "x\tSUM\nnull\tnull\n",
                    "SELECT x, SUM(y) FROM t GROUP BY ROLLUP(x)"
            );
        });
    }

    @Test
    public void testRollupEquivalentToGroupingSets() throws Exception {
        // ROLLUP(a, b) should produce the same results as
        // GROUPING SETS ((a, b), (a), ())
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v DOUBLE)");
            execute(
                    "INSERT INTO t VALUES " +
                            "(1, 10, 100), (1, 20, 200), " +
                            "(2, 10, 300), (2, 20, 400)"
            );

            String rollupResult = "a\tb\tSUM\n" +
                    "1\t10\t100.0\n" +
                    "1\t20\t200.0\n" +
                    "2\t10\t300.0\n" +
                    "2\t20\t400.0\n" +
                    "1\tnull\t300.0\n" +
                    "2\tnull\t700.0\n" +
                    "null\tnull\t1000.0\n";

            assertGroupingSetsQuery(rollupResult,
                    "SELECT a, b, SUM(v) FROM t GROUP BY ROLLUP(a, b)"
            );

            assertGroupingSetsQuery(rollupResult,
                    "SELECT a, b, SUM(v) FROM t GROUP BY GROUPING SETS ((a, b), (a), ())"
            );
        });
    }

    @Test
    public void testRollupExpressionAliasRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, v DOUBLE)");
            assertException("SELECT a + 1 AS k, SUM(v) FROM t GROUP BY ROLLUP(k)", 49, "expression keys in ROLLUP, CUBE and GROUPING SETS must be computed in a subquery");
            assertException("SELECT a + 1 AS k, SUM(v) FROM t GROUP BY CUBE(k)", 47, "expression keys in ROLLUP, CUBE and GROUPING SETS must be computed in a subquery");
            assertException("SELECT a + 1 AS k, SUM(v) FROM t GROUP BY GROUPING SETS ((k), ())", 58, "expression keys in ROLLUP, CUBE and GROUPING SETS must be computed in a subquery");
            String groupingAliasQuery = "SELECT a + 1 AS k, GROUPING(k), SUM(v) FROM t GROUP BY ROLLUP(k)";
            assertException(groupingAliasQuery, groupingAliasQuery.indexOf("ROLLUP(k)") + 7, "expression keys in ROLLUP, CUBE and GROUPING SETS must be computed in a subquery");
        });
    }

    @Test
    public void testRollupExpressionPrefix() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v DOUBLE)");
            execute("INSERT INTO t VALUES (1, 10, 3), (1, 20, 4), (2, 10, 5)");
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                assertQuery("SELECT a + 1 AS k, b, SUM(v) AS s FROM t GROUP BY a + 1, ROLLUP(b) ORDER BY k, b")
                        .withContext(sqlExecutionContext)
                        .expectSize()
                        .returns("""
                                k\tb\ts
                                2\tnull\t7.0
                                2\t10\t3.0
                                2\t20\t4.0
                                3\tnull\t5.0
                                3\t10\t5.0
                                """);
            }
        });
    }

    @Test
    public void testRollupExpressionPrefixWithoutAggregate() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (b INT)");
            execute("INSERT INTO t VALUES (10), (20)");
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                String query = "SELECT b::STRING AS k, b FROM t GROUP BY b::STRING, ROLLUP(b) ORDER BY k,b";
                assertQuery(query).withContext(sqlExecutionContext)
                        .assertsPlanContaining(mode == 1 ? "Async Group By" : "GroupBy");
                assertQuery(query).withContext(sqlExecutionContext).expectSize().returns("""
                        k\tb
                        10\tnull
                        10\t10
                        20\tnull
                        20\t20
                        """);
            }
        });
    }

    @Test
    public void testRollupExpressionRetainsRolledInput() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (b INT, v DOUBLE)");
            execute("INSERT INTO t VALUES (10, 3), (20, 4), (10, 5), (null, 2)");
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                String query = "SELECT b + 1 AS k, b, GROUPING(b) AS g, SUM(v) AS s FROM t GROUP BY b + 1, ROLLUP(b) ORDER BY k, g";
                assertQuery(query).withContext(sqlExecutionContext)
                        .assertsPlanContaining(mode == 1 ? "Async Group By" : "GroupBy");
                assertQuery(query).withContext(sqlExecutionContext).expectSize().returns("""
                        k\tb\tg\ts
                        null\tnull\t0\t2.0
                        null\tnull\t1\t2.0
                        11\t10\t0\t8.0
                        11\tnull\t1\t8.0
                        21\t20\t0\t4.0
                        21\tnull\t1\t4.0
                        """);
            }
        });
    }

    @Test
    public void testRollupFactoryReuseAcrossInsertAndTruncate() throws Exception {
        // A cached statement runs one compiled factory as the data changes. Each run must read the
        // table as it is then: only the grand total when empty, every level after the INSERT.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            final String empty = """
                    a\tb\tg\tc\ts
                    null\tnull\t3\t0\tnull
                    """;
            final String filled = """
                    a\tb\tg\tc\ts
                    1\t1\t0\t1\t10
                    1\t2\t0\t1\t20
                    2\t1\t0\t1\t30
                    1\tnull\t1\t2\t30
                    2\tnull\t1\t1\t30
                    null\tnull\t3\t3\t60
                    """;
            final String query = "SELECT a, b, grouping_id(a, b) g, count() c, sum(v) s FROM t GROUP BY ROLLUP(a, b) ORDER BY g, a, b";
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                assertQuery(query).withContext(sqlExecutionContext)
                        .assertsPlanContaining(mode == 1 ? "Async Group By" : "GroupBy vectorized: false");
                try (RecordCursorFactory factory = select(query)) {
                    assertFactoryRows(factory, empty);
                    execute("INSERT INTO t VALUES (1, 1, 10, '2024-01-01'), (1, 2, 20, '2024-01-01'), (2, 1, 30, '2024-01-02')");
                    assertFactoryRows(factory, filled);
                    execute("TRUNCATE TABLE t");
                    assertFactoryRows(factory, empty);
                }
            }
        });
    }

    @Test
    public void testRollupFilterBindVariableReusesFactory() throws Exception {
        // A prepared statement runs one compiled factory with new bind values. The parallel
        // grouping sets factory owns the filter, so each run must read the current value.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (sym SYMBOL, a INT, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('x', 1, 10, '2024-01-01'), ('x', 2, 20, '2024-01-01'), ('y', 1, 5, '2024-01-02')");
            final String expectedX = """
                    a\ts
                    null\t30
                    1\t10
                    2\t20
                    """;
            final String expectedY = """
                    a\ts
                    null\t5
                    1\t5
                    """;
            final int callerJitMode = sqlExecutionContext.getJitMode();
            try {
                for (int jitMode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                    sqlExecutionContext.setJitMode(jitMode);
                    sqlExecutionContext.setParallelGroupByEnabled(true);
                    bindVariableService.clear();
                    bindVariableService.setStr(0, "x");
                    final String query = "SELECT a, sum(v) s FROM t WHERE sym = $1 GROUP BY ROLLUP(a) ORDER BY a";
                    assertQuery(query).withContext(sqlExecutionContext)
                            .assertsPlanContaining(jitMode == SqlJitMode.JIT_MODE_ENABLED ? "Async JIT Group By" : "Async Group By", "groupingSets: true", "filter: ");
                    try (RecordCursorFactory factory = select(query)) {
                        for (String sym : new String[]{"x", "y", "x"}) {
                            bindVariableService.setStr(0, sym);
                            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                                sink.clear();
                                TestUtils.assertCursor(sym.equals("x") ? expectedX : expectedY, cursor, factory.getMetadata(), true, sink);
                            }
                        }
                    }
                }
            } finally {
                sqlExecutionContext.setJitMode(callerJitMode);
            }
        });
    }

    @Test
    public void testRollupFilterRunsInParallel() throws Exception {
        // The parallel grouping sets factory takes over the filter of a filtered scan, so a WHERE
        // clause keeps the aggregation on the page-frame workers. The reference is one plain
        // GROUP BY per grouping set.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL, a INT, b INT, v VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t SELECT (x * 3_600_000_000L)::TIMESTAMP, CASE WHEN x % 3 = 0 THEN 'x' ELSE 'y' END, " +
                    "(x % 4)::INT, (x % 5)::INT, x::VARCHAR FROM long_sequence(200)");
            drainWalQueue();
            // The JIT filter falls back to the Java filter on the frames that need it: the
            // parquet partition stores v as VARCHAR and parses it as it reads, and w has a column top.
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts < '1970-01-02'");
            drainWalQueue();
            execute("ALTER TABLE t ALTER COLUMN v TYPE LONG");
            execute("ALTER TABLE t ADD COLUMN w LONG");
            execute("INSERT INTO t SELECT ((200 + x) * 3_600_000_000L)::TIMESTAMP, 'x', (x % 4)::INT, (x % 5)::INT, x, x " +
                    "FROM long_sequence(50)");
            drainWalQueue();
            final String[] filters = {
                    "v > 100",
                    "sym = 'x'",
                    "ts IN '1970-01-03' AND sym = 'x'",
                    "w > 10 OR v < 20",
                    "sym = $1"
            };
            final int callerJitMode = sqlExecutionContext.getJitMode();
            try {
                for (int jitMode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                    sqlExecutionContext.setJitMode(jitMode);
                    for (String filter : filters) {
                        final String query = "SELECT a, b, sum(v) s, count() n FROM t WHERE " + filter +
                                " GROUP BY ROLLUP(a, b) ORDER BY a, b";
                        final String reference = "SELECT * FROM (" +
                                "SELECT a, b, sum(v) s, count() n FROM t WHERE " + filter + " GROUP BY a, b " +
                                "UNION ALL SELECT a, NULL::INT b, sum(v) s, count() n FROM t WHERE " + filter + " GROUP BY a " +
                                "UNION ALL SELECT NULL::INT a, NULL::INT b, sum(v) s, count() n FROM t WHERE " + filter +
                                ") ORDER BY a, b";
                        sqlExecutionContext.setParallelGroupByEnabled(true);
                        // A bind variable filter runs again with a new value on the same query text.
                        for (String sym : new String[]{"x", "y"}) {
                            bindVariableService.clear();
                            bindVariableService.setStr(0, sym);
                            final String groupBy = jitMode == SqlJitMode.JIT_MODE_ENABLED ? "Async JIT Group By" : "Async Group By";
                            assertQuery(query).withContext(sqlExecutionContext)
                                    .assertsPlanContaining(groupBy, "groupingSets: true", "filter: ");
                            assertQuery(query).withContext(sqlExecutionContext)
                                    .assertsPlanNotContaining("Filter workers");
                            assertSqlCursors(reference, query);
                        }
                    }
                }
            } finally {
                sqlExecutionContext.setJitMode(callerJitMode);
            }
        });
    }

    @Test
    public void testRollupKeyOfEveryType() throws Exception {
        // ROLLUP(c) over one row is the detail row plus a total whose key is the type's NULL.
        // The reference spells both out as plain GROUP BY queries, so this checks that a rolled-up
        // key of every groupable type reads back as that type's NULL, serially and in parallel.
        assertMemoryLeak(() -> {
            String[][] columns = {
                    {"c_bool", "BOOLEAN", "rnd_boolean()"},
                    {"c_byte", "BYTE", "rnd_byte()"},
                    {"c_short", "SHORT", "rnd_short()"},
                    {"c_char", "CHAR", "rnd_char()"},
                    {"c_int", "INT", "rnd_int()"},
                    {"c_long", "LONG", "rnd_long()"},
                    {"c_float", "FLOAT", "rnd_float()"},
                    {"c_double", "DOUBLE", "rnd_double()"},
                    {"c_date", "DATE", "rnd_date(1_577_836_800_000L, 1_767_225_600_000L, 0)"},
                    {"c_ts", "TIMESTAMP", "rnd_timestamp(to_timestamp('2024-01-01', 'yyyy-MM-dd'), to_timestamp('2024-04-01', 'yyyy-MM-dd'), 0)"},
                    {"c_str", "STRING", "rnd_str(3, 8, 0)"},
                    {"c_varchar", "VARCHAR", "rnd_varchar(3, 8, 0)"},
                    {"c_sym", "SYMBOL", "rnd_symbol('a', 'b')"},
                    {"c_uuid", "UUID", "rnd_uuid4()"},
                    {"c_ipv4", "IPv4", "rnd_ipv4()"},
                    {"c_long256", "LONG256", "rnd_long256()"},
                    {"c_geo1", "GEOHASH(1c)", "rnd_geohash(5)"},
                    {"c_geo3", "GEOHASH(3c)", "rnd_geohash(15)"},
                    {"c_geo6", "GEOHASH(6c)", "rnd_geohash(30)"},
                    {"c_geo12", "GEOHASH(12c)", "rnd_geohash(60)"},
                    {"c_dec8", "DECIMAL(2, 0)", "rnd_decimal(2, 0, 0)"},
                    {"c_dec16", "DECIMAL(4, 1)", "rnd_decimal(4, 1, 0)"},
                    {"c_dec32", "DECIMAL(9, 2)", "rnd_decimal(9, 2, 0)"},
                    {"c_dec64", "DECIMAL(18, 3)", "rnd_decimal(18, 3, 0)"},
                    {"c_dec128", "DECIMAL(38, 4)", "rnd_decimal(38, 4, 0)"},
                    {"c_dec256", "DECIMAL(76, 5)", "rnd_decimal(76, 5, 0)"},
                    {"c_bin", "BINARY", "rnd_bin(2, 4, 0)"}
            };
            StringBuilder create = new StringBuilder("CREATE TABLE kt (");
            StringBuilder insert = new StringBuilder("INSERT INTO kt SELECT ");
            for (String[] column : columns) {
                create.append(column[0]).append(' ').append(column[1]).append(", ");
                insert.append(column[2]).append(", ");
            }
            execute(create.append("ts TIMESTAMP) TIMESTAMP(ts)").toString());
            execute(insert.append("x::TIMESTAMP FROM long_sequence(1)").toString());
            // An INTERVAL cannot be stored, so its key comes from a subquery.
            String intervalSource = "(SELECT interval(ts, ts) c_iv FROM kt)";
            for (int i = 0; i < 2; i++) {
                sqlExecutionContext.setParallelGroupByEnabled(i == 1);
                for (String[] column : columns) {
                    assertRolledUpKeyMatchesReference(column[0], "CASE WHEN 1 = 0 THEN " + column[0] + " END", "kt");
                }
                // CASE takes no INTERVAL, so the reference builds the NULL interval directly, and the
                // cursor comparison has no INTERVAL case, so the two compare as printed text.
                printSql("SELECT c_iv, count() n, 0 g FROM " + intervalSource + " GROUP BY c_iv"
                        + " UNION ALL SELECT interval(NULL::TIMESTAMP, NULL::TIMESTAMP) c_iv, count() n, 1 g FROM " + intervalSource);
                assertQuery("SELECT c_iv, count() n, grouping(c_iv) g FROM " + intervalSource + " GROUP BY ROLLUP(c_iv) ORDER BY g")
                        .withContext(sqlExecutionContext)
                        .inferRandomAccess()
                        .sizeMayVary()
                        .returns(sink.toString());
            }
        });
    }

    @Test
    public void testRollupMultipleAggregates() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE trades (symbol SYMBOL, price DOUBLE, quantity DOUBLE, ts TIMESTAMP)" +
                            " TIMESTAMP(ts) PARTITION BY DAY"
            );
            execute(
                    "INSERT INTO trades VALUES " +
                            "('BTC', 100, 10, '2024-01-01T00:00:00.000000Z')," +
                            "('BTC', 200, 20, '2024-01-01T00:01:00.000000Z')," +
                            "('ETH', 300, 30, '2024-01-01T00:02:00.000000Z')," +
                            "('ETH', 400, 40, '2024-01-01T00:03:00.000000Z')"
            );

            assertGroupingSetsQuery(
                    "symbol\tSUM\tCOUNT\tMIN\tMAX\n" +
                            "\t100.0\t4\t100.0\t400.0\n" +
                            "BTC\t30.0\t2\t100.0\t200.0\n" +
                            "ETH\t70.0\t2\t300.0\t400.0\n",
                    "SELECT symbol, SUM(quantity), COUNT(), MIN(price), MAX(price) " +
                            "FROM trades " +
                            "GROUP BY ROLLUP(symbol) " +
                            "ORDER BY symbol"
            );
        });
    }

    @Test
    public void testRollupNoOpCastAliasPrefix() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v DOUBLE)");
            execute("INSERT INTO t VALUES (1,10,3), (1,20,4), (2,10,5)");
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                assertQuery("SELECT b, b::INT AS k, b::INT AS k2, GROUPING_ID(a,b) AS gid, SUM(v) AS s FROM t GROUP BY k,ROLLUP(a,b) ORDER BY k,gid,s")
                        .withContext(sqlExecutionContext).expectSize().returns("""
                                b\tk\tk2\tgid\ts
                                10\t10\t10\t0\t3.0
                                10\t10\t10\t0\t5.0
                                null\t10\t10\t1\t3.0
                                null\t10\t10\t1\t5.0
                                null\t10\t10\t3\t8.0
                                20\t20\t20\t0\t4.0
                                null\t20\t20\t1\t4.0
                                null\t20\t20\t3\t4.0
                                """);
            }
        });
    }

    @Test
    public void testRollupNoOpCastAliasRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (b INT, v DOUBLE)");
            assertException("SELECT b::INT AS k, SUM(v) FROM t GROUP BY ROLLUP(k)", 50, "expression keys in ROLLUP, CUBE and GROUPING SETS must be computed in a subquery");
            assertException("SELECT b::INT AS k, SUM(v) FROM t GROUP BY CUBE(k)", 48, "expression keys in ROLLUP, CUBE and GROUPING SETS must be computed in a subquery");
            assertException("SELECT b::INT AS k, SUM(v) FROM t GROUP BY GROUPING SETS ((k), ())", 59, "expression keys in ROLLUP, CUBE and GROUPING SETS must be computed in a subquery");
        });
    }

    @Test
    public void testRollupNoOpCastPrefix() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v DOUBLE)");
            execute("INSERT INTO t VALUES (1,10,3), (1,20,4), (2,10,5)");
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                String query = "SELECT b::INT AS k, b, GROUPING(b) AS g, GROUPING_ID(b) AS gid, SUM(v) AS s FROM t GROUP BY b::INT,ROLLUP(b) ORDER BY k,g";
                assertQuery(query).withContext(sqlExecutionContext)
                        .assertsPlanContaining(mode == 1 ? "Async Group By" : "GroupBy");
                assertQuery(query).withContext(sqlExecutionContext).expectSize().returns("""
                        k\tb\tg\tgid\ts
                        10\t10\t0\t0\t8.0
                        10\tnull\t1\t1\t8.0
                        20\t20\t0\t0\t4.0
                        20\tnull\t1\t1\t4.0
                        """);
            }
        });
    }

    @Test
    public void testRollupNoOpCastPrefixAsync() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v DOUBLE)");
            execute("INSERT INTO t VALUES (1,10,3), (1,20,4), (2,10,5), (2,null,2)");
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                String query = "SELECT b::INT AS k, b, b::STRING AS j, GROUPING(b) AS g, SUM(v) AS s FROM t GROUP BY b::INT,b::STRING,ROLLUP(b) ORDER BY k,g";
                assertQuery(query).withContext(sqlExecutionContext)
                        .assertsPlanContaining(mode == 1 ? "Async Group By" : "GroupBy");
                assertQuery(query).withContext(sqlExecutionContext).expectSize().returns("""
                        k\tb\tj\tg\ts
                        null\tnull\t\t0\t2.0
                        null\tnull\t\t1\t2.0
                        10\t10\t10\t0\t8.0
                        10\tnull\t10\t1\t8.0
                        20\t20\t20\t0\t4.0
                        20\tnull\t20\t1\t4.0
                        """);
            }
        });
    }

    @Test
    public void testRollupOmittedKey() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v DOUBLE)");
            execute("INSERT INTO t VALUES (1, 10, 3), (1, 20, 4), (2, 10, 5)");
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                assertQuery("SELECT b, SUM(v) AS s FROM t GROUP BY ROLLUP(a, b) ORDER BY b, s")
                        .withContext(sqlExecutionContext)
                        .expectSize()
                        .returns("""
                                b\ts
                                null\t5.0
                                null\t7.0
                                null\t12.0
                                10\t3.0
                                10\t5.0
                                20\t4.0
                                """);
            }
        });
    }

    @Test
    public void testRollupOuterConstantFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v DOUBLE)");
            execute("INSERT INTO t VALUES (1,10,3), (1,20,4), (2,10,5)");
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                String grouped = "SELECT a,b,GROUPING_ID(a,b) gid,SUM(v) s FROM t GROUP BY ROLLUP(a,b)";
                assertQuery("SELECT * FROM (" + grouped + ") WHERE false")
                        .withContext(sqlExecutionContext).returns("a\tb\tgid\ts\n");
                assertQuery("SELECT * FROM (" + grouped + ") WHERE 1=0")
                        .withContext(sqlExecutionContext).returns("a\tb\tgid\ts\n");
                assertQuery("SELECT * FROM (" + grouped + ") WHERE rnd_boolean()")
                        .withContext(sqlExecutionContext)
                        .assertsPlanContaining("Filter filter: rnd_boolean()\n    "
                                + (mode == 1 ? "Async Group By" : "GroupBy"));
            }
        });
    }

    @Test
    public void testRollupOuterExpressionKeyFilter() throws Exception {
        // The prefix key is computed below the group by, and a filter on it is safe to push down.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v INT)");
            execute("INSERT INTO t VALUES (1, 10, 3), (2, 20, 5), (1, 20, 7)");
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                assertGroupingSetsQuery(
                        """
                                k\tb\ts
                                2\tnull\t10
                                2\t10\t3
                                2\t20\t7
                                """,
                        "SELECT * FROM (SELECT a + 1 k, b, sum(v) s FROM t GROUP BY k, ROLLUP(b)) WHERE k = 2 ORDER BY b"
                );
            }
        });
    }

    @Test
    public void testRollupOuterFilterOnAliasSpelledLikeKey() throws Exception {
        // The output column a reads the rolled-up b, while k reads the kept key a. A filter on a
        // must stay above the grouping sets, or it drops the rolled-up rows of b. Sets (a, b) and
        // (a) give (b=1, a=1, 10), (b=1, a=2, 20), (b=2, a=1, 30), (NULL, 1, 40) and (NULL, 2, 20).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE pd (a INT, b INT, v DOUBLE)");
            execute("INSERT INTO pd VALUES (1, 1, 10), (2, 1, 20), (1, 2, 30)");
            final String inner = "SELECT b AS a, a AS k, sum(v) s FROM pd GROUP BY k, ROLLUP(a)";
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                final String rolledUpFilter = "SELECT * FROM (" + inner + ") WHERE a = 1 ORDER BY k";
                assertQuery(rolledUpFilter).withContext(sqlExecutionContext).withPlanNotContaining("filter: b=1").returns("""
                        a\tk\ts
                        1\t1\t10.0
                        1\t2\t20.0
                        """);
                // A filter on the kept key still runs below the grouping sets.
                final String keptKeyFilter = "SELECT * FROM (" + inner + ") WHERE k = 2 ORDER BY a";
                assertQuery(keptKeyFilter).withContext(sqlExecutionContext).withPlanContaining("filter: a=2").expectSize().returns("""
                        a\tk\ts
                        null\t2\t20.0
                        1\t2\t20.0
                        """);
            }
        });
    }

    @Test
    public void testRollupOuterInAndBetweenFilters() throws Exception {
        // IN and BETWEEN keep their operands in an argument list. A filter on the rolled-up key
        // b must stay above the group by, or the subtotal rows would survive it.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v INT)");
            execute("INSERT INTO t VALUES (1, 10, 3), (2, 20, 5), (1, 20, 7)");
            final String grouped = "SELECT * FROM (SELECT a, b, sum(v) s FROM t GROUP BY a, ROLLUP(b)) WHERE ";
            final String allRows = """
                    a\tb\ts
                    1\tnull\t10
                    1\t10\t3
                    1\t20\t7
                    2\tnull\t5
                    2\t20\t5
                    """;
            final String detailRows = """
                    a\tb\ts
                    1\t10\t3
                    1\t20\t7
                    2\t20\t5
                    """;
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                assertGroupingSetsQuery(allRows, grouped + "a IN (1, 2, 3) ORDER BY a, b");
                assertGroupingSetsQuery(allRows, grouped + "a BETWEEN 1 AND 2 ORDER BY a, b");
                // rnd_int() cannot be evaluated twice, so the filter stays above the group by.
                assertGroupingSetsQuery(allRows, grouped + "a IN (1, 2, rnd_int()) ORDER BY a, b");
                assertGroupingSetsQuery(detailRows, grouped + "b IN (10, 20, 30) ORDER BY a, b");
                assertGroupingSetsQuery(detailRows, grouped + "b BETWEEN 10 AND 20 ORDER BY a, b");
                // Over a source subquery that computes a key, a filter on the rolled-up a
                // stays above the group by.
                assertGroupingSetsQuery(
                        """
                                a\tb\ts
                                1\tnull\t10
                                1\t11\t3
                                1\t21\t7
                                """,
                        "SELECT * FROM (SELECT a, b, sum(v) s FROM (SELECT a, b + 1 b, v FROM t) GROUP BY ROLLUP(a, b)) WHERE a = 1 ORDER BY b"
                );
            }
        });
    }

    @Test
    public void testRollupOuterKeyFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v DOUBLE)");
            execute("INSERT INTO t VALUES (1,10,3), (1,20,4), (2,10,5)");
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                assertQuery("SELECT * FROM (SELECT a,b,SUM(v) s FROM t GROUP BY ROLLUP(a,b)) WHERE b=10 ORDER BY a,b")
                        .withContext(sqlExecutionContext).returns("a\tb\ts\n1\t10\t3.0\n2\t10\t5.0\n");
            }
        });
    }

    @Test
    public void testRollupOuterKeyFilterAsync() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v DOUBLE)");
            execute("INSERT INTO t VALUES (1,10,3), (1,20,4), (2,10,5)");
            sqlExecutionContext.setParallelGroupByEnabled(true);
            String grouped = "SELECT a,b,GROUPING_ID(a,b) gid,SUM(v) s FROM t GROUP BY ROLLUP(a,b)";
            String query = "SELECT * FROM (" + grouped + ") WHERE b=10 ORDER BY a,b";
            assertQuery(query).withContext(sqlExecutionContext).assertsPlanContaining("Async Group By");
            assertQuery(query).withContext(sqlExecutionContext).returns("a\tb\tgid\ts\n1\t10\t0\t3.0\n2\t10\t0\t5.0\n");
            query = "SELECT * FROM (" + grouped + ") WHERE b IS NULL ORDER BY a,b";
            assertQuery(query).withContext(sqlExecutionContext).assertsPlanContaining("Async Group By");
            assertQuery(query).withContext(sqlExecutionContext).returns("""
                    a\tb\tgid\ts
                    null\tnull\t3\t12.0
                    1\tnull\t1\t7.0
                    2\tnull\t1\t5.0
                    """);
        });
    }

    @Test
    public void testRollupOuterNullFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v DOUBLE)");
            execute("INSERT INTO t VALUES (1,10,3), (1,20,4), (2,10,5)");
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                assertQuery("SELECT * FROM (SELECT a,b,SUM(v) s FROM t GROUP BY ROLLUP(a,b)) WHERE b IS NULL ORDER BY a,b")
                        .withContext(sqlExecutionContext).returns("""
                                a\tb\ts
                                null\tnull\t12.0
                                1\tnull\t7.0
                                2\tnull\t5.0
                                """);
            }
        });
    }

    @Test
    public void testRollupOuterPrefixFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v DOUBLE)");
            execute("INSERT INTO t VALUES (1,10,3), (1,20,4), (2,10,5)");
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                String query = "SELECT * FROM (SELECT a,b,SUM(v) s FROM t GROUP BY a,ROLLUP(b)) WHERE a=1 ORDER BY a,b";
                // The parallel factory takes the pushed-down filter over and runs it per page frame.
                assertQuery(query).withContext(sqlExecutionContext)
                        .assertsPlanContaining(mode == 1
                                ? "Async JIT Group By workers: 1\n      keys: [a,b]\n      values: [sum(v)]\n      filter: a=1"
                                : "values: [sum(v)]\n        Async JIT Filter workers: 1\n          filter: a=1");
                assertQuery(query).withContext(sqlExecutionContext).expectSize().returns("""
                        a\tb\ts
                        1\tnull\t7.0
                        1\t10\t3.0
                        1\t20\t4.0
                        """);
                assertQuery("SELECT * FROM (SELECT a,b,SUM(v) s FROM t GROUP BY a,b) WHERE a=1 ORDER BY a,b")
                        .withContext(sqlExecutionContext).expectSize().returns("a\tb\ts\n1\t10\t3.0\n1\t20\t4.0\n");
                assertQuery("""
                        SELECT * FROM (
                            SELECT t.b AS a,t.a AS b,SUM(v) s FROM t GROUP BY t.a,ROLLUP(t.b)
                        ) WHERE a=10 ORDER BY b
                        """)
                        .withContext(sqlExecutionContext).returns("a\tb\ts\n10\t1\t3.0\n10\t2\t5.0\n");
            }
        });
    }

    @Test
    public void testRollupOuterTimestampPrefixFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, a INT, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('2024-01-01',1,3), ('2024-01-02',1,4), ('2024-01-02',2,5)");
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                String query = "SELECT * FROM (SELECT ts,a,SUM(v) s FROM t GROUP BY ts,ROLLUP(a)) WHERE ts='2024-01-02' ORDER BY ts,a";
                assertQuery(query).withContext(sqlExecutionContext).assertsPlan(mode == 1 ? """
                        Encode sort
                          keys: [ts, a]
                            Async Group By workers: 1
                              keys: [ts,a]
                              values: [sum(v)]
                              vectorized: false
                              groupingSets: true
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: t
                                      intervals: [(\"2024-01-02T00:00:00.000000Z\",\"2024-01-02T00:00:00.000000Z\")]
                        """ : """
                        Encode sort
                          keys: [ts, a]
                            GroupBy vectorized: false
                              groupingSets: true
                              keys: [ts,a]
                              values: [sum(v)]
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: t
                                      intervals: [(\"2024-01-02T00:00:00.000000Z\",\"2024-01-02T00:00:00.000000Z\")]
                        """);
                assertQuery(query).withContext(sqlExecutionContext).expectSize().timestamp("ts").returns("""
                        ts\ta\ts
                        2024-01-02T00:00:00.000000Z\tnull\t9.0
                        2024-01-02T00:00:00.000000Z\t1\t4.0
                        2024-01-02T00:00:00.000000Z\t2\t5.0
                        """);
                query = """
                        SELECT * FROM (
                            SELECT ts,a,SUM(v) s FROM t GROUP BY ts,ROLLUP(a)
                            UNION ALL
                            SELECT ts,a,v FROM t
                        ) WHERE ts='2024-01-02' ORDER BY ts,a,s
                        """;
                assertQuery(query).withContext(sqlExecutionContext).assertsPlan(mode == 1 ? """
                        Encode sort
                          keys: [ts, a, s]
                            Union All
                                Async Group By workers: 1
                                  keys: [ts,a]
                                  values: [sum(v)]
                                  vectorized: false
                                  groupingSets: true
                                    PageFrame
                                        Row forward scan
                                        Interval forward scan on: t
                                          intervals: [(\"2024-01-02T00:00:00.000000Z\",\"2024-01-02T00:00:00.000000Z\")]
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: t
                                      intervals: [(\"2024-01-02T00:00:00.000000Z\",\"2024-01-02T00:00:00.000000Z\")]
                        """ : """
                        Encode sort
                          keys: [ts, a, s]
                            Union All
                                GroupBy vectorized: false
                                  groupingSets: true
                                  keys: [ts,a]
                                  values: [sum(v)]
                                    PageFrame
                                        Row forward scan
                                        Interval forward scan on: t
                                          intervals: [(\"2024-01-02T00:00:00.000000Z\",\"2024-01-02T00:00:00.000000Z\")]
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: t
                                      intervals: [(\"2024-01-02T00:00:00.000000Z\",\"2024-01-02T00:00:00.000000Z\")]
                        """);
                assertQuery(query).withContext(sqlExecutionContext).timestamp("ts").returns("""
                        ts\ta\ts
                        2024-01-02T00:00:00.000000Z\tnull\t9.0
                        2024-01-02T00:00:00.000000Z\t1\t4.0
                        2024-01-02T00:00:00.000000Z\t1\t4.0
                        2024-01-02T00:00:00.000000Z\t2\t5.0
                        2024-01-02T00:00:00.000000Z\t2\t5.0
                        """);
                bindVariableService.setStr(0, "2024-01-02");
                assertQuery(query.replace("ts='2024-01-02'", "ts=$1"))
                        .withContext(sqlExecutionContext).timestamp("ts")
                        .withPlanContaining("groupingSets: true")
                        .returns("""
                                ts\ta\ts
                                2024-01-02T00:00:00.000000Z\tnull\t9.0
                                2024-01-02T00:00:00.000000Z\t1\t4.0
                                2024-01-02T00:00:00.000000Z\t1\t4.0
                                2024-01-02T00:00:00.000000Z\t2\t5.0
                                2024-01-02T00:00:00.000000Z\t2\t5.0
                                """);
            }
        });
    }

    @Test
    public void testRollupOuterUnionAliasedTimestampFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, a INT, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('2024-01-01',1,3), ('2024-01-02',1,4), ('2024-01-02',2,5)");
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                assertQuery("""
                        SELECT * FROM (
                            SELECT ts AS x,a,v s FROM t
                            UNION ALL
                            SELECT ts AS y,a,SUM(v) z FROM t GROUP BY ROLLUP(ts,a)
                        ) WHERE x='2024-01-02' ORDER BY x,a,s
                        """)
                        .withContext(sqlExecutionContext).timestamp("x").returns("""
                                x\ta\ts
                                2024-01-02T00:00:00.000000Z\tnull\t9.0
                                2024-01-02T00:00:00.000000Z\t1\t4.0
                                2024-01-02T00:00:00.000000Z\t1\t4.0
                                2024-01-02T00:00:00.000000Z\t2\t5.0
                                2024-01-02T00:00:00.000000Z\t2\t5.0
                                """);
            }
        });
    }

    @Test
    public void testRollupOuterUnionNonDeterministicFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, a INT, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('2024-01-01',1,3), ('2024-01-02',1,4), ('2024-01-02',2,5)");
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                assertQuery("""
                        SELECT * FROM (
                            SELECT ts,a,SUM(v) s FROM t GROUP BY ROLLUP(ts,a)
                            UNION ALL
                            SELECT ts,a,v FROM t
                        ) WHERE ts='2024-01-02' OR rnd_boolean()
                        """)
                        .withContext(sqlExecutionContext)
                        .assertsPlan(mode == 1 ? """
                                Filter filter: (2024-01-02T00:00:00.000000Z=ts or rnd_boolean())
                                    Union All
                                        Async Group By workers: 1
                                          keys: [ts,a]
                                          values: [sum(v)]
                                          vectorized: false
                                          groupingSets: true
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: t
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t
                                """ : """
                                Filter filter: (2024-01-02T00:00:00.000000Z=ts or rnd_boolean())
                                    Union All
                                        GroupBy vectorized: false
                                          groupingSets: true
                                          keys: [ts,a]
                                          values: [sum(v)]
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: t
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t
                                """);
                String query = """
                        SELECT * FROM (
                            SELECT ts,a,SUM(v) s FROM t GROUP BY ts,ROLLUP(a)
                        ) WHERE ts='2024-01-02' OR rnd_boolean()
                        """;
                assertQuery(query)
                        .withContext(sqlExecutionContext)
                        .assertsPlan(mode == 1 ? """
                                Filter filter: (2024-01-02T00:00:00.000000Z=ts or rnd_boolean())
                                    Async Group By workers: 1
                                      keys: [ts,a]
                                      values: [sum(v)]
                                      vectorized: false
                                      groupingSets: true
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t
                                """ : """
                                Filter filter: (2024-01-02T00:00:00.000000Z=ts or rnd_boolean())
                                    GroupBy vectorized: false
                                      groupingSets: true
                                      keys: [ts,a]
                                      values: [sum(v)]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t
                                """);
            }
        });
    }

    @Test
    public void testRollupOuterUnionTimestampFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, a INT, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('2024-01-01',1,3), ('2024-01-02',1,4), ('2024-01-02',2,5)");
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                assertQuery("""
                        SELECT * FROM (
                            SELECT ts,a,SUM(v) s FROM t GROUP BY ROLLUP(ts,a)
                            UNION ALL
                            SELECT ts,a,v FROM t
                        ) WHERE ts='2024-01-02' ORDER BY ts,a,s
                        """)
                        .withContext(sqlExecutionContext).timestamp("ts").returns("""
                                ts\ta\ts
                                2024-01-02T00:00:00.000000Z\tnull\t9.0
                                2024-01-02T00:00:00.000000Z\t1\t4.0
                                2024-01-02T00:00:00.000000Z\t1\t4.0
                                2024-01-02T00:00:00.000000Z\t2\t5.0
                                2024-01-02T00:00:00.000000Z\t2\t5.0
                                """);
            }
        });
    }

    @Test
    public void testRollupOverSubqueryColumnsReadingSameColumn() throws Exception {
        // The subquery exposes column a under two names, so x and a are distinct grouping keys
        // that read the same table column. Each key must roll up on its own.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT)");
            execute("INSERT INTO t VALUES (1), (2)");
            final String sub = "(SELECT a, a AS x FROM t)";
            final String rollupXA = "SELECT * FROM (" +
                    "SELECT x, a, count() c FROM " + sub + " GROUP BY x, a " +
                    "UNION ALL SELECT x, NULL::INT a, count() c FROM " + sub + " GROUP BY x " +
                    "UNION ALL SELECT NULL::INT x, NULL::INT a, count() c FROM " + sub +
                    ") ORDER BY x, a";
            final String rollupAX = "SELECT * FROM (" +
                    "SELECT x, a, count() c FROM " + sub + " GROUP BY x, a " +
                    "UNION ALL SELECT NULL::INT x, a, count() c FROM " + sub + " GROUP BY a " +
                    "UNION ALL SELECT NULL::INT x, NULL::INT a, count() c FROM " + sub +
                    ") ORDER BY x, a";
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                assertQuery("SELECT x, a, count() c FROM " + sub + " GROUP BY ROLLUP(x, a) ORDER BY x, a")
                        .withContext(sqlExecutionContext)
                        .expectSize()
                        .returns("""
                                x\ta\tc
                                null\tnull\t2
                                1\tnull\t1
                                1\t1\t1
                                2\tnull\t1
                                2\t2\t1
                                """);
                assertSqlCursors(rollupXA, "SELECT x, a, count() c FROM " + sub + " GROUP BY ROLLUP(x, a) ORDER BY x, a");
                assertSqlCursors(rollupXA, "SELECT x, a, c FROM (SELECT a, x, count() c FROM " + sub + " GROUP BY ROLLUP(x, a)) ORDER BY x, a");
                assertSqlCursors(rollupAX, "SELECT x, a, count() c FROM " + sub + " GROUP BY ROLLUP(a, x) ORDER BY x, a");
                assertSqlCursors(rollupAX, "SELECT x, a, c FROM (SELECT a, x, count() c FROM " + sub + " GROUP BY ROLLUP(a, x)) ORDER BY x, a");

                // x is rolled up, so a filter on x stays above the grouping sets, while a filter on
                // the kept key a still runs below them.
                final String inner = "SELECT x, a, count() c FROM " + sub + " GROUP BY a, ROLLUP(x)";
                assertQuery("SELECT * FROM (" + inner + ") WHERE x = 1")
                        .withContext(sqlExecutionContext)
                        .withPlanNotContaining("filter: a=1")
                        .noRandomAccess()
                        .returns("""
                                x\ta\tc
                                1\t1\t1
                                """);
                assertQuery("SELECT * FROM (" + inner + ") WHERE a = 1 ORDER BY x")
                        .withContext(sqlExecutionContext)
                        .withPlanContaining("filter: a=1")
                        .expectSize()
                        .returns("""
                                x\ta\tc
                                null\t1\t1
                                1\t1\t1
                                """);
            }

            // Here the sub-query's a reads the table's b, and x reads the table's a.
            execute("CREATE TABLE u (ts TIMESTAMP, a INT, b INT) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO u VALUES ('2024-01-01', 1, 10), ('2024-01-03', 2, 20)");
            // The same values with x stored as a column of its own.
            execute("CREATE TABLE v AS (SELECT ts, a AS x, b AS a FROM u) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE w AS (SELECT ts, a, a AS x FROM u) TIMESTAMP(ts) PARTITION BY DAY");
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                assertQuery("SELECT x, a, count() c FROM (SELECT b AS a, a AS x FROM u) GROUP BY ROLLUP(x, a) ORDER BY x, a")
                        .withContext(sqlExecutionContext)
                        .expectSize()
                        .returns("""
                                x\ta\tc
                                null\tnull\t2
                                1\tnull\t1
                                1\t10\t1
                                2\tnull\t1
                                2\t20\t1
                                """);
                // The hidden fill key of SAMPLE BY FILL reads the same key-to-column mapping.
                assertSqlCursors(
                        "SELECT ts, x, a, count() c FROM v SAMPLE BY 1d ROLLUP(x, a) FILL(NULL) ORDER BY ts, x, a",
                        "SELECT ts, x, a, count() c FROM (SELECT ts, b AS a, a AS x FROM u) SAMPLE BY 1d ROLLUP(x, a) FILL(NULL) ORDER BY ts, x, a"
                );
                assertSqlCursors(
                        "SELECT ts, x, a, count() c FROM w SAMPLE BY 1d ROLLUP(x, a) FILL(NULL) ORDER BY ts, x, a",
                        "SELECT ts, x, a, count() c FROM (SELECT ts, a, a AS x FROM u) SAMPLE BY 1d ROLLUP(x, a) FILL(NULL) ORDER BY ts, x, a"
                );
            }
        });
    }

    @Test
    public void testRollupParserError() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT, y DOUBLE)");
            assertException(
                    "SELECT x, SUM(y) FROM t GROUP BY ROLLUP()",
                    33,
                    "ROLLUP requires at least one column"
            );
        });
    }

    @Test
    public void testRollupReorderedKeys() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v DOUBLE)");
            execute("INSERT INTO t VALUES (1, 10, 3), (1, 20, 4), (2, 10, 5)");
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                assertQuery("SELECT b, a, GROUPING_ID(a, b) AS gid, SUM(v) AS s FROM t GROUP BY ROLLUP(a, b) ORDER BY gid, a, b")
                        .withContext(sqlExecutionContext)
                        .expectSize()
                        .returns("""
                                b\ta\tgid\ts
                                10\t1\t0\t3.0
                                20\t1\t0\t4.0
                                10\t2\t0\t5.0
                                null\t1\t1\t7.0
                                null\t2\t1\t5.0
                                null\tnull\t3\t12.0
                                """);
            }
        });
    }

    @Test
    public void testRollupReorderedKeysMultipleFrames() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t SELECT (1 + (x % 3) / 2)::INT, (10 + (x % 3) % 2 * 10)::INT, (CASE x % 3 WHEN 0 THEN 3 WHEN 1 THEN 4 ELSE 5 END)::DOUBLE, ((x - 1) / 1_000 * 86_400_000_000L)::TIMESTAMP FROM long_sequence(3_000)");
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                String query = "SELECT b, a, GROUPING_ID(a,b) AS gid, SUM(v) AS s FROM t GROUP BY ROLLUP(a,b) ORDER BY gid,a,b";
                assertQuery(query).withContext(sqlExecutionContext)
                        .assertsPlanContaining(mode == 1 ? "Async Group By" : "GroupBy");
                assertQuery(query).withContext(sqlExecutionContext).expectSize().returns("""
                        b\ta\tgid\ts
                        10\t1\t0\t3000.0
                        20\t1\t0\t4000.0
                        10\t2\t0\t5000.0
                        null\t1\t1\t7000.0
                        null\t2\t1\t5000.0
                        null\tnull\t3\t12000.0
                        """);
            }
        });
    }

    @Test
    public void testRollupSingleColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT, y DOUBLE)");
            execute("INSERT INTO t VALUES (1, 10), (1, 20), (2, 30)");

            assertGroupingSetsQuery(
                    "x\tSUM\n" +
                            "null\t60.0\n" +
                            "1\t30.0\n" +
                            "2\t30.0\n",
                    "SELECT x, SUM(y) FROM t GROUP BY ROLLUP(x) ORDER BY x"
            );
        });
    }

    @Test
    public void testRollupThreeColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, c INT, v DOUBLE)");
            execute(
                    "INSERT INTO t VALUES " +
                            "(1, 10, 100, 1000), " +
                            "(2, 20, 200, 2000)"
            );

            // ROLLUP(a, b, c) -> (a,b,c), (a,b), (a), ()
            assertGroupingSetsQuery(
                    "a\tb\tc\tSUM\n" +
                            "1\t10\t100\t1000.0\n" +
                            "2\t20\t200\t2000.0\n" +
                            "1\t10\tnull\t1000.0\n" +
                            "2\t20\tnull\t2000.0\n" +
                            "1\tnull\tnull\t1000.0\n" +
                            "2\tnull\tnull\t2000.0\n" +
                            "null\tnull\tnull\t3000.0\n",
                    "SELECT a, b, c, SUM(v) FROM t GROUP BY ROLLUP(a, b, c)"
            );
        });
    }

    @Test
    public void testRollupTrivialExpressionAliasRejected() throws Exception {
        // k = a - 1 shares its base column with a, which the trivial GROUP BY expression rewrite
        // would lift out of the GROUP BY. The alias is rolled up, so it must still be rejected.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT)");
            String query = "SELECT a, a - 1 AS k, count() c FROM t GROUP BY ROLLUP(a, k)";
            assertException(query, query.indexOf("ROLLUP(a, k)") + 10, "expression keys in ROLLUP, CUBE and GROUPING SETS must be computed in a subquery");
        });
    }

    @Test
    public void testRollupTrivialExpressionPrefixes() throws Exception {
        // Two expression prefixes over the rolled-up column, the shape the trivial GROUP BY
        // expression rewrite lifts out of the GROUP BY.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT)");
            execute("INSERT INTO t VALUES (1), (1), (2)");
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                assertQuery("SELECT a - 1 AS k1, a - 2 AS k2, a, count() c FROM t GROUP BY a - 1, a - 2, ROLLUP(a) ORDER BY k1, a")
                        .withContext(sqlExecutionContext)
                        .expectSize()
                        .returns("""
                                k1\tk2\ta\tc
                                0\t-1\tnull\t2
                                0\t-1\t1\t2
                                1\t0\tnull\t1
                                1\t0\t2\t1
                                """);
            }
        });
    }

    @Test
    public void testRollupWithAvg() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (category SYMBOL, val DOUBLE)");
            execute(
                    "INSERT INTO t VALUES " +
                            "('A', 10), ('A', 20), ('A', 30), " +
                            "('B', 40), ('B', 50)"
            );

            assertGroupingSetsQuery(
                    "category\tAVG\n" +
                            "\t30.0\n" +
                            "A\t20.0\n" +
                            "B\t45.0\n",
                    "SELECT category, AVG(val) " +
                            "FROM t " +
                            "GROUP BY ROLLUP(category) " +
                            "ORDER BY category"
            );
        });
    }

    @Test
    public void testRollupWithCount() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE trades (symbol SYMBOL, side SYMBOL, quantity DOUBLE, ts TIMESTAMP)" +
                            " TIMESTAMP(ts) PARTITION BY DAY"
            );
            execute(
                    "INSERT INTO trades VALUES " +
                            "('BTC', 'buy', 10, '2024-01-01T00:00:00.000000Z')," +
                            "('BTC', 'sell', 20, '2024-01-01T00:01:00.000000Z')," +
                            "('ETH', 'buy', 30, '2024-01-01T00:02:00.000000Z')," +
                            "('ETH', 'sell', 40, '2024-01-01T00:03:00.000000Z')"
            );

            assertGroupingSetsQuery(
                    "symbol\tCOUNT\tSUM\n" +
                            "\t4\t100.0\n" +
                            "BTC\t2\t30.0\n" +
                            "ETH\t2\t70.0\n",
                    "SELECT symbol, COUNT(), SUM(quantity) " +
                            "FROM trades " +
                            "GROUP BY ROLLUP(symbol) " +
                            "ORDER BY symbol"
            );
        });
    }

    @Test
    public void testRollupWithIntKey() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (grp INT, val DOUBLE)");
            execute(
                    "INSERT INTO t VALUES " +
                            "(1, 10), (1, 20), " +
                            "(2, 30), (2, 40)"
            );

            assertGroupingSetsQuery(
                    "grp\tSUM\n" +
                            "null\t100.0\n" +
                            "1\t30.0\n" +
                            "2\t70.0\n",
                    "SELECT grp, SUM(val) " +
                            "FROM t " +
                            "GROUP BY ROLLUP(grp) " +
                            "ORDER BY grp"
            );
        });
    }

    @Test
    public void testRollupWithJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (product_id INT, qty INT)");
            execute("CREATE TABLE products (id INT, name SYMBOL)");
            execute("INSERT INTO products VALUES (1, 'A'), (2, 'B')");
            execute("INSERT INTO orders VALUES (1, 10), (1, 20), (2, 30)");
            final String expected = "name\tSUM\n" +
                    "\t60\n" +
                    "A\t30\n" +
                    "B\t30\n";
            final String query = "SELECT p.name, SUM(o.qty) FROM orders o" +
                    " JOIN products p ON o.product_id = p.id" +
                    " GROUP BY ROLLUP(p.name) ORDER BY p.name";
            sqlExecutionContext.setParallelGroupByEnabled(true);
            assertGroupingSetsQuery(
                    expected,
                    query
            );
            sqlExecutionContext.setParallelGroupByEnabled(false);
            assertGroupingSetsQuery(expected, query);
            sqlExecutionContext.setParallelGroupByEnabled(true);
        });
    }

    @Test
    public void testRollupWithNullAggregateInputs() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE trades (symbol SYMBOL, quantity DOUBLE, ts TIMESTAMP)" +
                            " TIMESTAMP(ts) PARTITION BY DAY"
            );
            execute(
                    "INSERT INTO trades VALUES " +
                            "('BTC', 10, '2024-01-01T00:00:00.000000Z')," +
                            "('BTC', NaN, '2024-01-01T00:01:00.000000Z')," +
                            "('ETH', 30, '2024-01-01T00:02:00.000000Z')," +
                            "('ETH', NaN, '2024-01-01T00:03:00.000000Z')"
            );

            // SUM should skip NaN values; count should count non-NaN only
            assertGroupingSetsQuery(
                    "symbol\tSUM\tcount\n" +
                            "BTC\t10.0\t1\n" +
                            "ETH\t30.0\t1\n" +
                            "\t40.0\t2\n",
                    "SELECT symbol, SUM(quantity), COUNT(quantity) AS count " +
                            "FROM trades " +
                            "GROUP BY ROLLUP(symbol) " +
                            "ORDER BY GROUPING(symbol), symbol"
            );
        });
    }

    @Test
    public void testRollupWithNullData() throws Exception {
        // Verify behavior when input data contains actual NULLs
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (category SYMBOL, val DOUBLE)");
            execute(
                    "INSERT INTO t VALUES " +
                            "('A', 10), (NULL, 20), ('A', 30)"
            );

            // NULL category rows should aggregate with other NULL category rows
            // The grand total row also has NULL category
            assertGroupingSetsQuery(
                    "category\tSUM\n" +
                            "\t20.0\n" +
                            "A\t40.0\n" +
                            "\t60.0\n",
                    "SELECT category, SUM(val) FROM t GROUP BY ROLLUP(category)"
            );
        });
    }

    @Test
    public void testRollupWithOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE trades (symbol SYMBOL, side SYMBOL, quantity DOUBLE, ts TIMESTAMP)" +
                            " TIMESTAMP(ts) PARTITION BY DAY"
            );
            execute(
                    "INSERT INTO trades VALUES " +
                            "('BTC', 'buy', 10, '2024-01-01T00:00:00.000000Z')," +
                            "('BTC', 'sell', 20, '2024-01-01T00:01:00.000000Z')," +
                            "('ETH', 'buy', 30, '2024-01-01T00:02:00.000000Z')," +
                            "('ETH', 'sell', 40, '2024-01-01T00:03:00.000000Z')"
            );

            assertGroupingSetsQuery(
                    "symbol\tSUM\n" +
                            "\t100.0\n" +
                            "BTC\t30.0\n" +
                            "ETH\t70.0\n",
                    "SELECT symbol, SUM(quantity) " +
                            "FROM trades " +
                            "GROUP BY ROLLUP(symbol) " +
                            "ORDER BY symbol"
            );
        });
    }

    @Test
    public void testRollupWithStringKey() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (name STRING, val DOUBLE)");
            execute(
                    "INSERT INTO t VALUES " +
                            "('Alice', 10), ('Alice', 20), " +
                            "('Bob', 30)"
            );

            assertGroupingSetsQuery(
                    "name\tSUM\n" +
                            "\t60.0\n" +
                            "Alice\t30.0\n" +
                            "Bob\t30.0\n",
                    "SELECT name, SUM(val) " +
                            "FROM t " +
                            "GROUP BY ROLLUP(name) " +
                            "ORDER BY name"
            );
        });
    }

    @Test
    public void testRollupWithVarcharKey() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (label VARCHAR, val DOUBLE)");
            execute(
                    "INSERT INTO t VALUES " +
                            "('foo', 10), ('foo', 20), " +
                            "('bar', 30)"
            );

            assertGroupingSetsQuery(
                    "label\tSUM\n" +
                            "\t60.0\n" +
                            "bar\t30.0\n" +
                            "foo\t30.0\n",
                    "SELECT label, SUM(val) " +
                            "FROM t " +
                            "GROUP BY ROLLUP(label) " +
                            "ORDER BY label"
            );
        });
    }

    @Test
    public void testRollupWithWhereClause() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE trades (symbol SYMBOL, quantity DOUBLE, ts TIMESTAMP)" +
                            " TIMESTAMP(ts) PARTITION BY DAY"
            );
            execute(
                    "INSERT INTO trades VALUES " +
                            "('BTC', 10, '2024-01-01T00:00:00.000000Z')," +
                            "('BTC', 20, '2024-01-01T00:01:00.000000Z')," +
                            "('ETH', 30, '2024-01-01T00:02:00.000000Z')," +
                            "('ETH', 40, '2024-01-01T00:03:00.000000Z')"
            );

            assertGroupingSetsQuery(
                    "symbol\tSUM\n" +
                            "\t90.0\n" +
                            "BTC\t20.0\n" +
                            "ETH\t70.0\n",
                    "SELECT symbol, SUM(quantity) " +
                            "FROM trades " +
                            "WHERE quantity > 10 " +
                            "GROUP BY ROLLUP(symbol) " +
                            "ORDER BY symbol"
            );
        });
    }

    @Test
    public void testSampleByCubeBasic() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE trades (symbol SYMBOL, side SYMBOL, quantity DOUBLE, ts TIMESTAMP)" +
                            " TIMESTAMP(ts) PARTITION BY DAY"
            );
            execute(
                    "INSERT INTO trades VALUES " +
                            "('BTC', 'buy', 10, '2024-01-01T00:00:00.000000Z')," +
                            "('BTC', 'sell', 20, '2024-01-01T00:30:00.000000Z')"
            );

            // SAMPLE BY 1h CUBE(symbol, side): all combinations per hour
            assertGroupingSetsQuery(
                    "ts\tsymbol\tside\tSUM\n" +
                            "2024-01-01T00:00:00.000000Z\tBTC\tbuy\t10.0\n" +
                            "2024-01-01T00:00:00.000000Z\tBTC\tsell\t20.0\n" +
                            "2024-01-01T00:00:00.000000Z\tBTC\t\t30.0\n" +
                            "2024-01-01T00:00:00.000000Z\t\tbuy\t10.0\n" +
                            "2024-01-01T00:00:00.000000Z\t\tsell\t20.0\n" +
                            "2024-01-01T00:00:00.000000Z\t\t\t30.0\n",
                    "SELECT ts, symbol, side, SUM(quantity) " +
                            "FROM trades " +
                            "SAMPLE BY 1h CUBE(symbol, side)"
            );
        });
    }

    @Test
    public void testSampleByFillDuplicateGroupingSets() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES (1, 2, 3, '2024-01-01T00:00:00.000000Z'), (1, 2, 5, '2024-01-03T00:00:00.000000Z')");
            for (int i = 0; i < 2; i++) {
                sqlExecutionContext.setParallelGroupByEnabled(i == 1);
                String duplicateKeyCardinalityQuery = "SELECT ts, a, COUNT(*) FROM t SAMPLE BY 1d GROUPING SETS ((a), (a, a)) FILL(NULL)";
                assertQuery(duplicateKeyCardinalityQuery)
                        .withContext(sqlExecutionContext)
                        .fails(duplicateKeyCardinalityQuery.indexOf("FILL(") + "FILL(".length(), "SAMPLE BY FILL is not supported with duplicate grouping sets");

                String duplicateKeyRollupQuery = "SELECT ts, a, COUNT(*) FROM t SAMPLE BY 1d ROLLUP(a, a) FILL(0)";
                assertQuery(duplicateKeyRollupQuery)
                        .withContext(sqlExecutionContext)
                        .fails(duplicateKeyRollupQuery.indexOf("FILL(") + "FILL(".length(), "SAMPLE BY FILL is not supported with duplicate grouping sets");

                String duplicateKeyCubeQuery = "SELECT ts, a, COUNT(*) FROM t SAMPLE BY 1d CUBE(a, a) FILL(NULL)";
                assertQuery(duplicateKeyCubeQuery)
                        .withContext(sqlExecutionContext)
                        .fails(duplicateKeyCubeQuery.indexOf("FILL(") + "FILL(".length(), "SAMPLE BY FILL is not supported with duplicate grouping sets");

                String unequalSetQuery = "SELECT ts, a, b, v, COUNT(*) FROM t SAMPLE BY 1d GROUPING SETS ((a, a, b), (a, b, v)) FILL(NULL) ORDER BY ts, v";
                assertGroupingSetsQuery(
                        "ts\ta\tb\tv\tCOUNT\n" +
                                "2024-01-01T00:00:00.000000Z\t1\t2\tnull\t1\n" +
                                "2024-01-01T00:00:00.000000Z\t1\t2\t3\t1\n" +
                                "2024-01-01T00:00:00.000000Z\t1\t2\t5\tnull\n" +
                                "2024-01-02T00:00:00.000000Z\t1\t2\tnull\tnull\n" +
                                "2024-01-02T00:00:00.000000Z\t1\t2\t3\tnull\n" +
                                "2024-01-02T00:00:00.000000Z\t1\t2\t5\tnull\n" +
                                "2024-01-03T00:00:00.000000Z\t1\t2\tnull\t1\n" +
                                "2024-01-03T00:00:00.000000Z\t1\t2\t3\tnull\n" +
                                "2024-01-03T00:00:00.000000Z\t1\t2\t5\t1\n",
                        unequalSetQuery
                );

                String fillNoneQuery = "SELECT ts, a, COUNT(*) FROM t SAMPLE BY 1d GROUPING SETS ((a), (a, a)) FILL(NONE) ORDER BY ts, a";
                assertGroupingSetsQuery(
                        "ts\ta\tCOUNT\n" +
                                "2024-01-01T00:00:00.000000Z\t1\t1\n" +
                                "2024-01-01T00:00:00.000000Z\t1\t1\n" +
                                "2024-01-03T00:00:00.000000Z\t1\t1\n" +
                                "2024-01-03T00:00:00.000000Z\t1\t1\n",
                        fillNoneQuery
                );

                String[] duplicateQueries = {
                        "SELECT ts, COUNT(*) FROM t SAMPLE BY 1d GROUPING SETS ((), ()) FILL(NULL)",
                        "SELECT ts, COUNT(*) FROM t SAMPLE BY 1d GROUPING SETS ((), ()) FILL(0)",
                        "SELECT ts, COUNT(*) FROM t SAMPLE BY 1d GROUPING SETS ((), ()) FROM '2024-01-01' TO '2024-01-03' FILL(NULL)",
                        "SELECT ts, a, COUNT(*) FROM t SAMPLE BY 1d GROUPING SETS ((a), (a)) FILL(NULL)",
                        "SELECT ts, a x, COUNT(*) FROM t SAMPLE BY 1d GROUPING SETS ((a), (x)) FILL(NULL)",
                        "SELECT ts, a x, COUNT(*) FROM t SAMPLE BY 1d GROUPING SETS ((x), (a, x)) FILL(0)",
                        // A qualified column behind the alias names the same key as the bare column.
                        "SELECT ts, t.a x, COUNT(*) FROM t SAMPLE BY 1d GROUPING SETS ((x), (a)) FILL(0)",
                        "SELECT ts, t.a x, COUNT(*) FROM t SAMPLE BY 1d GROUPING SETS ((a), (x)) FILL(0)",
                        // No bare "a" among the keys: the qualified forms still name one key.
                        "SELECT ts, t.a x, COUNT(*) FROM t SAMPLE BY 1d GROUPING SETS ((t.a), (x)) FILL(0)",
                        // Two aliases of one qualified column next to the bare column.
                        "SELECT ts, t.a x, t.a y, COUNT(*) FROM t SAMPLE BY 1d GROUPING SETS ((a), (x, y)) FILL(0)",
                        "SELECT ts, a, b, COUNT(*) FROM t SAMPLE BY 1d GROUPING SETS ((a, b), (b, a)) FILL(0)",
                        "SELECT ts, a, COUNT(*) FROM t SAMPLE BY 1d ROLLUP(a, a) FILL(0)",
                        "SELECT ts, a, COUNT(*) FROM t SAMPLE BY 1d CUBE(a, a) FILL(NULL)"
                };
                for (String query : duplicateQueries) {
                    assertQuery(query)
                            .withContext(sqlExecutionContext)
                            .fails(query.indexOf("FILL(") + "FILL(".length(), "SAMPLE BY FILL is not supported with duplicate grouping sets");
                }
                assertGroupingSetsQuery(
                        "ts\ta\tCOUNT\n" +
                                "2024-01-01T00:00:00.000000Z\tnull\t1\n" +
                                "2024-01-01T00:00:00.000000Z\t1\t1\n" +
                                "2024-01-02T00:00:00.000000Z\tnull\tnull\n" +
                                "2024-01-02T00:00:00.000000Z\t1\tnull\n" +
                                "2024-01-03T00:00:00.000000Z\tnull\t1\n" +
                                "2024-01-03T00:00:00.000000Z\t1\t1\n",
                        "SELECT ts, a, COUNT(*) FROM t SAMPLE BY 1d GROUPING SETS ((a), ()) FILL(NULL) ORDER BY ts, a"
                );
            }
        });
    }

    @Test
    public void testSampleByFillDuplicateGroupingSetsAcrossJoin() throws Exception {
        // "a" next to "p.a" and "q.a" is ambiguous. The duplicate check must not report
        // (x, b) and (y, b) as equal sets in place of the ambiguous column.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE p (a INT, b INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE q (a INT, c INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO p VALUES (1, 2, '2024-01-01T00:00:00.000000Z')");
            execute("INSERT INTO q VALUES (1, 5, '2024-01-01T00:00:00.000000Z')");
            for (String fill : new String[]{"NONE", "NULL", "0"}) {
                String query = "SELECT p.ts, p.a x, q.a y, COUNT(*) FROM p JOIN q ON p.a = q.a SAMPLE BY 1d " +
                        "GROUPING SETS ((x, b), (y, b), (a, c)) FILL(" + fill + ")";
                assertQuery(query).fails(query.indexOf("(a, c)") + 1, "Ambiguous column [name=a]");
            }
        });
    }

    @Test
    public void testSampleByFillDuplicateGroupingSetsOfLargeCube() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE w (k1 INT, k2 INT, k3 INT, k4 INT, k5 INT, k6 INT, k7 INT, k8 INT, k9 INT, k10 INT, k11 INT, k12 INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO w VALUES (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, '2024-01-01T00:00:00.000000Z')");
            for (int i = 0; i < 2; i++) {
                sqlExecutionContext.setParallelGroupByEnabled(i == 1);
                // One row in one bucket gives one row per grouping set: 2^12 of them.
                assertQuery(
                        "SELECT count() FROM (SELECT ts, k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11, k12, COUNT(*) FROM w " +
                                "SAMPLE BY 1d CUBE(k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11, k12) FILL(NULL))"
                ).withContext(sqlExecutionContext).noRandomAccess().expectSize().returns("count\n4096\n");

                // x names k1, so every set with k1 and without x equals one with x and without k1.
                String duplicateQuery = "SELECT ts, k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11, k1 x, COUNT(*) FROM w " +
                        "SAMPLE BY 1d CUBE(k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11, x) FILL(NULL)";
                assertQuery(duplicateQuery)
                        .withContext(sqlExecutionContext)
                        .fails(duplicateQuery.indexOf("FILL(") + "FILL(".length(), "SAMPLE BY FILL is not supported with duplicate grouping sets");
            }
        });
    }

    @Test
    public void testSampleByFillDuplicateGroupingSetsWithUnequalDistinctKeys() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES (1, 2, 3, '2024-01-01T00:00:00.000000Z')");
            String query = "SELECT ts, a, b, v, COUNT(*) FROM t SAMPLE BY 1d GROUPING SETS ((a, a, b), (a, b, v)) FILL(NULL) ORDER BY ts, v";
            String expected = "ts\ta\tb\tv\tCOUNT\n" +
                    "2024-01-01T00:00:00.000000Z\t1\t2\tnull\t1\n" +
                    "2024-01-01T00:00:00.000000Z\t1\t2\t3\t1\n";
            for (int i = 0; i < 2; i++) {
                sqlExecutionContext.setParallelGroupByEnabled(i == 1);
                assertQuery(query).withContext(sqlExecutionContext).inferTimestamp().returns(expected);
            }
        });
    }

    @Test
    public void testSampleByFillExpressionAliasKey() throws Exception {
        // The key "a" is the alias of b + 10, not the column a behind x, so (x, a) and (a)
        // are different sets.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES (1, 2, '2024-01-01T00:00:00.000000Z'), (2, 2, '2024-01-03T00:00:00.000000Z')");
            for (int i = 0; i < 2; i++) {
                sqlExecutionContext.setParallelGroupByEnabled(i == 1);
                assertQuery("SELECT ts, a x, b + 10 a, COUNT(*) FROM t SAMPLE BY 1d GROUPING SETS ((x, a), (a)) FILL(NULL) ORDER BY ts, x")
                        .withContext(sqlExecutionContext)
                        .inferTimestamp()
                        .returns("""
                                ts\tx\ta\tCOUNT
                                2024-01-01T00:00:00.000000Z\tnull\t12\t1
                                2024-01-01T00:00:00.000000Z\t1\t12\t1
                                2024-01-01T00:00:00.000000Z\t2\t12\tnull
                                2024-01-02T00:00:00.000000Z\tnull\t12\tnull
                                2024-01-02T00:00:00.000000Z\t1\t12\tnull
                                2024-01-02T00:00:00.000000Z\t2\t12\tnull
                                2024-01-03T00:00:00.000000Z\tnull\t12\t1
                                2024-01-03T00:00:00.000000Z\t1\t12\tnull
                                2024-01-03T00:00:00.000000Z\t2\t12\t1
                                """);

                // An expression key missing from a set fails as an expression key, as it does with
                // FILL(NONE), not as a duplicate of the set (x).
                for (String fill : new String[]{"NONE", "NULL", "0"}) {
                    String query = "SELECT ts, a x, b + 10 a, COUNT(*) FROM t SAMPLE BY 1d GROUPING SETS ((x), (a)) FILL(" + fill + ")";
                    assertQuery(query)
                            .withContext(sqlExecutionContext)
                            .fails(query.indexOf("(a)") + 1, "expression keys in ROLLUP, CUBE and GROUPING SETS must be computed in a subquery");
                }
            }
        });
    }

    @Test
    public void testSampleByFillGroupingSetsWithManyKeys() throws Exception {
        // 31 keys and the time bucket are 32 GROUP BY keys. The hidden fill key needs only one
        // bit per rolled-up key, so FILL works with more keys than an INT has bits. The reference
        // is one plain SAMPLE BY FILL query per grouping set.
        assertMemoryLeak(() -> {
            StringBuilder ddl = new StringBuilder("CREATE TABLE wide (ts TIMESTAMP");
            StringBuilder allKeys = new StringBuilder();
            StringBuilder innerKeys = new StringBuilder();
            StringBuilder values = new StringBuilder();
            for (int i = 0; i < 31; i++) {
                ddl.append(", k").append(i).append(" INT");
                allKeys.append(i == 0 ? "" : ", ").append('k').append(i);
                if (i < 30) {
                    innerKeys.append(i == 0 ? "" : ", ").append('k').append(i);
                }
                values.append(", ").append(i);
            }
            execute(ddl.append(") TIMESTAMP(ts) PARTITION BY DAY").toString());
            execute("INSERT INTO wide VALUES ('2024-01-01T00:00:00'" + values + "), ('2024-01-01T02:00:00'" + values + ")");
            final String query = "SELECT ts, " + allKeys + ", count() n FROM wide SAMPLE BY 1h " +
                    "GROUPING SETS ((" + allKeys + "), (" + innerKeys + ")) FILL(0) ORDER BY ts, k30";
            final String reference = "SELECT * FROM (" +
                    "SELECT ts, " + allKeys + ", count() n FROM wide SAMPLE BY 1h FILL(0) " +
                    "UNION ALL SELECT ts, " + innerKeys + ", NULL::INT k30, n FROM " +
                    "(SELECT ts, " + innerKeys + ", count() n FROM wide SAMPLE BY 1h FILL(0))" +
                    ") ORDER BY ts, k30";
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                assertSqlCursors(reference, query);
            }
        });
    }

    @Test
    public void testSampleByFillNoneWithDuplicateGroupingSets() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES (1, '2024-01-01T00:00:00.000000Z'), (1, '2024-01-03T00:00:00.000000Z')");
            String[] queries = {
                    "SELECT ts, a, COUNT(*) FROM t SAMPLE BY 1d GROUPING SETS ((a), (a, a)) FILL(NONE) ORDER BY ts, a",
                    "SELECT ts, a, COUNT(*) FROM t SAMPLE BY 1d ROLLUP(a, a) FILL(NONE) ORDER BY ts, a",
                    "SELECT ts, a, COUNT(*) FROM t SAMPLE BY 1d CUBE(a, a) FILL(NONE) ORDER BY ts, a"
            };
            String explicitExpected = "ts\ta\tCOUNT\n" +
                    "2024-01-01T00:00:00.000000Z\t1\t1\n" +
                    "2024-01-01T00:00:00.000000Z\t1\t1\n" +
                    "2024-01-03T00:00:00.000000Z\t1\t1\n" +
                    "2024-01-03T00:00:00.000000Z\t1\t1\n";
            String rollupExpected = "ts\ta\tCOUNT\n" +
                    "2024-01-01T00:00:00.000000Z\tnull\t1\n" +
                    "2024-01-01T00:00:00.000000Z\t1\t1\n" +
                    "2024-01-01T00:00:00.000000Z\t1\t1\n" +
                    "2024-01-03T00:00:00.000000Z\tnull\t1\n" +
                    "2024-01-03T00:00:00.000000Z\t1\t1\n" +
                    "2024-01-03T00:00:00.000000Z\t1\t1\n";
            String cubeExpected = "ts\ta\tCOUNT\n" +
                    "2024-01-01T00:00:00.000000Z\tnull\t1\n" +
                    "2024-01-01T00:00:00.000000Z\t1\t1\n" +
                    "2024-01-01T00:00:00.000000Z\t1\t1\n" +
                    "2024-01-01T00:00:00.000000Z\t1\t1\n" +
                    "2024-01-03T00:00:00.000000Z\tnull\t1\n" +
                    "2024-01-03T00:00:00.000000Z\t1\t1\n" +
                    "2024-01-03T00:00:00.000000Z\t1\t1\n" +
                    "2024-01-03T00:00:00.000000Z\t1\t1\n";
            String[] expected = {explicitExpected, rollupExpected, cubeExpected};
            for (int i = 0; i < 2; i++) {
                sqlExecutionContext.setParallelGroupByEnabled(i == 1);
                for (int j = 0; j < queries.length; j++) {
                    assertQuery(queries[j]).withContext(sqlExecutionContext).inferTimestamp().sizeMayVary().returns(expected[j]);
                }
                // FILL(NONE, NONE) gets the same error as it does without grouping sets,
                // not the duplicate grouping sets error
                String[] fillNoneNoneQueries = {
                        "SELECT ts, a, COUNT(*) c, SUM(a) s FROM t SAMPLE BY 1d FILL(NONE, NONE)",
                        "SELECT ts, a, COUNT(*) c, SUM(a) s FROM t SAMPLE BY 1d GROUPING SETS ((a), (a, a)) FILL(NONE, NONE)"
                };
                for (String query : fillNoneNoneQueries) {
                    assertQuery(query)
                            .withContext(sqlExecutionContext)
                            .fails(query.indexOf("FILL(") + "FILL(".length(), "FILL(NONE) cannot be combined with other fill values");
                }
            }
        });
    }

    @Test
    public void testSampleByGroupingSetsSyntaxErrors() throws Exception {
        // ROLLUP, CUBE and GROUPING SETS after SAMPLE BY take their own parse path, with its own checks.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (c1 INT, c2 INT, c3 INT, c4 INT, c5 INT, c6 INT, c7 INT, c8 INT, c9 INT, c10 INT, c11 INT, c12 INT, c13 INT, c14 INT, c15 INT, c16 INT, ts TIMESTAMP) TIMESTAMP(ts)");
            String query = "SELECT ts, count() FROM t SAMPLE BY 1d ROLLUP()";
            assertException(query, query.indexOf("ROLLUP"), "ROLLUP requires at least one column");
            query = "SELECT ts, count() FROM t SAMPLE BY 1d CUBE()";
            assertException(query, query.indexOf("CUBE"), "CUBE requires at least one column");
            query = "SELECT ts, count() FROM t SAMPLE BY 1d CUBE(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16)";
            assertException(query, query.indexOf("CUBE"), "CUBE supports at most 15 columns");
            query = "SELECT ts, count() FROM t SAMPLE BY 1d GROUPING x";
            assertException(query, query.indexOf(" x") + 1, "expected SETS after GROUPING");
        });
    }

    @Test
    public void testSampleByRejectsExplicitGroupByClause() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, v INT, ts TIMESTAMP) TIMESTAMP(ts)");

            String query = "SELECT a, SUM(v) FROM t SAMPLE BY 1d ROLLUP(a) GROUP BY GROUPING SETS ((a))";
            assertException(
                    query,
                    query.indexOf("GROUP BY"),
                    "SELECT query must not contain both GROUP BY and SAMPLE BY"
            );

            query = "SELECT a, SUM(v) FROM t SAMPLE BY 1d ROLLUP(a) GROUP BY GROUPING SETS (())";
            assertException(
                    query,
                    query.indexOf("GROUP BY"),
                    "SELECT query must not contain both GROUP BY and SAMPLE BY"
            );

            query = "SELECT a, SUM(v) FROM t SAMPLE BY 1d GROUP BY ROLLUP(a)";
            assertException(
                    query,
                    query.indexOf("ROLLUP(a)") + "ROLLUP(".length(),
                    "SELECT query must not contain both GROUP BY and SAMPLE BY"
            );

            assertQuery("SELECT ts, a, SUM(v) FROM t SAMPLE BY 1d ROLLUP(a)")
                    .timestamp("ts")
                    .returns("ts\ta\tSUM\n");
        });
    }

    @Test
    public void testSampleByRollupBasic() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE trades (symbol SYMBOL, quantity DOUBLE, ts TIMESTAMP)" +
                            " TIMESTAMP(ts) PARTITION BY DAY"
            );
            execute(
                    "INSERT INTO trades VALUES " +
                            "('BTC', 10, '2024-01-01T00:00:00.000000Z')," +
                            "('BTC', 20, '2024-01-01T00:30:00.000000Z')," +
                            "('ETH', 30, '2024-01-01T00:45:00.000000Z')," +
                            "('BTC', 40, '2024-01-01T01:00:00.000000Z')," +
                            "('ETH', 50, '2024-01-01T01:15:00.000000Z')"
            );

            // SAMPLE BY 1h ROLLUP(symbol): for each hour, compute per-symbol and grand total
            assertGroupingSetsQuery(
                    "ts\tsymbol\tSUM\n" +
                            "2024-01-01T00:00:00.000000Z\t\t60.0\n" +
                            "2024-01-01T00:00:00.000000Z\tBTC\t30.0\n" +
                            "2024-01-01T00:00:00.000000Z\tETH\t30.0\n" +
                            "2024-01-01T01:00:00.000000Z\t\t90.0\n" +
                            "2024-01-01T01:00:00.000000Z\tBTC\t40.0\n" +
                            "2024-01-01T01:00:00.000000Z\tETH\t50.0\n",
                    "SELECT ts, symbol, SUM(quantity) " +
                            "FROM trades " +
                            "SAMPLE BY 1h ROLLUP(symbol) " +
                            "ORDER BY ts, symbol"
            );
        });
    }

    @Test
    public void testSampleByRollupFillConstant() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE trades (symbol SYMBOL, quantity DOUBLE, ts TIMESTAMP)" +
                            " TIMESTAMP(ts) PARTITION BY DAY"
            );
            execute(
                    "INSERT INTO trades VALUES " +
                            "('BTC', 10, '2024-01-01T00:00:00.000000Z')," +
                            "('BTC', 30, '2024-01-01T02:00:00.000000Z')"
            );

            // FILL(0) emits one fill row per key combination per missing
            // timestamp bucket. Key columns retain their actual values,
            // aggregate columns get the fill value.
            assertGroupingSetsQuery(
                    "ts\tsymbol\tSUM\n" +
                            "2024-01-01T00:00:00.000000Z\t\t10.0\n" +
                            "2024-01-01T00:00:00.000000Z\tBTC\t10.0\n" +
                            "2024-01-01T01:00:00.000000Z\t\t0.0\n" +
                            "2024-01-01T01:00:00.000000Z\tBTC\t0.0\n" +
                            "2024-01-01T02:00:00.000000Z\t\t30.0\n" +
                            "2024-01-01T02:00:00.000000Z\tBTC\t30.0\n",
                    "SELECT ts, symbol, SUM(quantity) " +
                            "FROM trades " +
                            "SAMPLE BY 1h ROLLUP(symbol) FILL(0) " +
                            "ORDER BY ts, symbol"
            );
        });
    }

    @Test
    public void testSampleByRollupFillEmptyInput() throws Exception {
        // FILL fills each key combination the input produced, as keyed SAMPLE BY FILL does, so
        // empty input yields no rows even with FROM and TO: there is no combination to fill,
        // not even the grand total of ROLLUP's empty set.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES (1, 10, '2024-01-01T00:00:00.000000Z')");
            for (int i = 0; i < 2; i++) {
                sqlExecutionContext.setParallelGroupByEnabled(i == 1);
                assertQuery("SELECT ts, a, count() c FROM t WHERE v < 0 SAMPLE BY 1d FROM '2024-01-01' TO '2024-01-04' FILL(NULL)")
                        .withContext(sqlExecutionContext)
                        .inferRandomAccess()
                        .inferTimestamp()
                        .sizeMayVary()
                        .returns("ts\ta\tc\n");
                assertQuery("SELECT ts, a, count() c FROM t WHERE v < 0 SAMPLE BY 1d ROLLUP(a) FROM '2024-01-01' TO '2024-01-04' FILL(NULL)")
                        .withContext(sqlExecutionContext)
                        .inferRandomAccess()
                        .inferTimestamp()
                        .sizeMayVary()
                        .returns("ts\ta\tc\n");
                // One matching row is enough to fill both series over the whole range.
                assertQuery("SELECT ts, a, count() c FROM t WHERE v > 0 SAMPLE BY 1d ROLLUP(a) FROM '2024-01-01' TO '2024-01-04' FILL(NULL) ORDER BY ts, a")
                        .withContext(sqlExecutionContext)
                        .inferRandomAccess()
                        .inferTimestamp()
                        .sizeMayVary()
                        .returns("""
                                ts\ta\tc
                                2024-01-01T00:00:00.000000Z\tnull\t1
                                2024-01-01T00:00:00.000000Z\t1\t1
                                2024-01-02T00:00:00.000000Z\tnull\tnull
                                2024-01-02T00:00:00.000000Z\t1\tnull
                                2024-01-03T00:00:00.000000Z\tnull\tnull
                                2024-01-03T00:00:00.000000Z\t1\tnull
                                """);
            }
        });
    }

    @Test
    public void testSampleByRollupFillGroupingBeforeArrayAggregate() throws Exception {
        // first() over an array takes FILL(NULL) but not a constant. With GROUPING() ahead of it,
        // its fill value is still the first one: GROUPING() is a fill key and takes no fill value.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, s VARCHAR, arr DOUBLE[], v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'x', ARRAY[1.0, 2.0], 10), ('2024-01-01T02:00:00', 'y', ARRAY[3.0], 20)");
            final String range = " FROM '2024-01-01' TO '2024-01-01T03:00:00' ";
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                assertSqlCursors(
                        "SELECT * FROM (" +
                                "SELECT ts, s, 0 g, first(arr) fa, sum(v) sv FROM t SAMPLE BY 1h" + range + "FILL(NULL, 0) " +
                                "UNION ALL SELECT ts, NULL::VARCHAR s, 1 g, fa, sv FROM (SELECT ts, first(arr) fa, sum(v) sv FROM t SAMPLE BY 1h" + range + "FILL(NULL, 0))" +
                                ") ORDER BY ts, s",
                        "SELECT ts, s, GROUPING(s) g, first(arr) fa, sum(v) sv FROM t SAMPLE BY 1h ROLLUP(s)" + range + "FILL(NULL, 0) ORDER BY ts, s"
                );
            }
        });
    }

    @Test
    public void testSampleByRollupFillKeyCollisions() throws Exception {
        // A rolled-up key reads as the type's NULL, or as false and 0 for BOOLEAN, BYTE, SHORT and
        // CHAR, so a rolled-up row can show the same key as a real data row of another set. FILL
        // must still fill each set's rows on its own. The reference is one plain SAMPLE BY FILL
        // query per grouping set, which does not use grouping sets at all.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, s VARCHAR, a INT, flag BOOLEAN, v LONG) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00', 'x', 1, true, 10),
                    ('2024-01-01T01:00:00', 'y', 2, false, 20),
                    ('2024-01-02T00:00:00', 'x', 1, false, 30),
                    ('2024-01-02T00:00:00', NULL, NULL, true, 40)
                    """);
            final String range = " FROM '2023-12-31T12:00:00' TO '2024-01-03' ";
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                // A real NULL key collides with the rolled-up NULL.
                assertSqlCursors(
                        "SELECT * FROM (" +
                                "SELECT ts, s, sum(v) sv FROM t SAMPLE BY 12h" + range + "FILL(0) " +
                                "UNION ALL SELECT ts, NULL::VARCHAR s, sv FROM (SELECT ts, sum(v) sv FROM t SAMPLE BY 12h" + range + "FILL(0))" +
                                ") ORDER BY ts, s, sv",
                        "SELECT ts, s, sum(v) sv FROM t SAMPLE BY 12h ROLLUP(s)" + range + "FILL(0) ORDER BY ts, s, sv"
                );
                // A rolled-up BOOLEAN reads as false, so it collides with the real false group.
                assertSqlCursors(
                        "SELECT * FROM (" +
                                "SELECT ts, flag, sum(v) sv FROM t SAMPLE BY 12h" + range + "FILL(NULL) " +
                                "UNION ALL SELECT ts, false flag, sv FROM (SELECT ts, sum(v) sv FROM t SAMPLE BY 12h" + range + "FILL(NULL))" +
                                ") ORDER BY ts, flag, sv",
                        "SELECT ts, flag, sum(v) sv FROM t SAMPLE BY 12h ROLLUP(flag)" + range + "FILL(NULL) ORDER BY ts, flag, sv"
                );
                // GROUPING(s) alone does not tell the (s, a) and (s) sets apart.
                assertSqlCursors(
                        "SELECT * FROM (" +
                                "SELECT ts, s, a, 0 g, sum(v) sv FROM t SAMPLE BY 12h" + range + "FILL(0) " +
                                "UNION ALL SELECT ts, s, NULL::INT a, 0 g, sv FROM (SELECT ts, s, sum(v) sv FROM t SAMPLE BY 12h" + range + "FILL(0)) " +
                                "UNION ALL SELECT ts, NULL::VARCHAR s, NULL::INT a, 1 g, sv FROM (SELECT ts, sum(v) sv FROM t SAMPLE BY 12h" + range + "FILL(0))" +
                                ") ORDER BY ts, s, a, g, sv",
                        "SELECT ts, s, a, GROUPING(s) g, sum(v) sv FROM t SAMPLE BY 12h ROLLUP(s, a)" + range + "FILL(0) ORDER BY ts, s, a, g, sv"
                );
            }
        });
    }

    @Test
    public void testSampleByRollupFillKeyEdgeCases() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, s VARCHAR, v LONG) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'x', 10), ('2024-01-01T12:00:00', NULL, 20)");
            final String range = " FROM '2024-01-01' TO '2024-01-02' ";
            final String rollupReference = "SELECT * FROM (" +
                    "SELECT ts, s, sum(v) sv, count() c FROM t SAMPLE BY 12h" + range + "FILL(0, 1) " +
                    "UNION ALL SELECT ts, NULL::VARCHAR s, sv, c FROM (SELECT ts, sum(v) sv, count() c FROM t SAMPLE BY 12h" + range + "FILL(0, 1))" +
                    ") ORDER BY ts, s, sv, c";
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                // One fill value per aggregate: a GROUPING() column is a fill key and takes none.
                assertSqlCursors(
                        "SELECT * FROM (" +
                                "SELECT ts, s, 0 g, sum(v) sv, count() c FROM t SAMPLE BY 12h" + range + "FILL(0, 1) " +
                                "UNION ALL SELECT ts, NULL::VARCHAR s, 1 g, sv, c FROM (SELECT ts, sum(v) sv, count() c FROM t SAMPLE BY 12h" + range + "FILL(0, 1))" +
                                ") ORDER BY ts, s, g, sv, c",
                        "SELECT ts, s, GROUPING(s) g, sum(v) sv, count() c FROM t SAMPLE BY 12h ROLLUP(s)" + range + "FILL(0, 1) ORDER BY ts, s, g, sv, c"
                );
                // A user column may take the name of the hidden fill key.
                assertSqlCursors(
                        "SELECT * FROM (" +
                                "SELECT ts, s, sum(v) sv, count() grouping_set_fill_key FROM t SAMPLE BY 12h" + range + "FILL(0, 1) " +
                                "UNION ALL SELECT ts, NULL::VARCHAR s, sv, k FROM (SELECT ts, sum(v) sv, count() k FROM t SAMPLE BY 12h" + range + "FILL(0, 1))" +
                                ") ORDER BY ts, s, sv, grouping_set_fill_key",
                        "SELECT ts, s, sum(v) sv, count() grouping_set_fill_key FROM t SAMPLE BY 12h ROLLUP(s)" + range +
                                "FILL(0, 1) ORDER BY ts, s, sv, grouping_set_fill_key"
                );
                // The fill key stays hidden when the query is a subquery.
                assertSqlCursors(
                        "SELECT count() n FROM (" + rollupReference + ")",
                        "SELECT count() n FROM (SELECT ts, s, sum(v) sv, count() c FROM t SAMPLE BY 12h ROLLUP(s)" + range + "FILL(0, 1))"
                );
            }

            final StringBuilder columns = new StringBuilder();
            final StringBuilder keys = new StringBuilder();
            for (int i = 0; i < 32; i++) {
                columns.append(", k").append(i).append(" INT");
                keys.append(i == 0 ? "" : ", ").append('k').append(i);
            }
            execute("CREATE TABLE wide (ts TIMESTAMP" + columns + ") TIMESTAMP(ts) PARTITION BY DAY");
            final String query = "SELECT ts, " + keys + ", count() FROM wide SAMPLE BY 1h ROLLUP(" + keys + ") FILL(0)";
            assertException(query, query.indexOf("k31)"), "SAMPLE BY FILL with grouping sets supports at most 31 rolled-up keys");
        });
    }

    @Test
    public void testSampleByRollupFillLinearNotSupported() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE trades (symbol SYMBOL, quantity DOUBLE, ts TIMESTAMP)" +
                            " TIMESTAMP(ts) PARTITION BY DAY"
            );

            assertException(
                    "SELECT ts, symbol, SUM(quantity) " +
                            "FROM trades " +
                            "SAMPLE BY 1h ROLLUP(symbol) FILL(LINEAR)",
                    78,
                    "FILL(PREV) and FILL(LINEAR) are not supported with ROLLUP, CUBE, or GROUPING SETS"
            );
        });
    }

    @Test
    public void testSampleByRollupFillNone() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE trades (symbol SYMBOL, quantity DOUBLE, ts TIMESTAMP)" +
                            " TIMESTAMP(ts) PARTITION BY DAY"
            );
            execute(
                    "INSERT INTO trades VALUES " +
                            "('BTC', 10, '2024-01-01T00:00:00.000000Z')," +
                            "('ETH', 20, '2024-01-01T00:30:00.000000Z')," +
                            "('BTC', 30, '2024-01-01T01:00:00.000000Z')"
            );

            // SAMPLE BY 1h ROLLUP(symbol) FILL(NONE) should work
            assertGroupingSetsQuery(
                    "ts\tsymbol\tSUM\n" +
                            "2024-01-01T00:00:00.000000Z\t\t30.0\n" +
                            "2024-01-01T00:00:00.000000Z\tBTC\t10.0\n" +
                            "2024-01-01T00:00:00.000000Z\tETH\t20.0\n" +
                            "2024-01-01T01:00:00.000000Z\t\t30.0\n" +
                            "2024-01-01T01:00:00.000000Z\tBTC\t30.0\n",
                    "SELECT ts, symbol, SUM(quantity) " +
                            "FROM trades " +
                            "SAMPLE BY 1h ROLLUP(symbol) FILL(NONE) " +
                            "ORDER BY ts, symbol"
            );
        });
    }

    @Test
    public void testSampleByRollupFillNull() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE trades (symbol SYMBOL, quantity DOUBLE, ts TIMESTAMP)" +
                            " TIMESTAMP(ts) PARTITION BY DAY"
            );
            execute(
                    "INSERT INTO trades VALUES " +
                            "('BTC', 10, '2024-01-01T00:00:00.000000Z')," +
                            "('ETH', 20, '2024-01-01T00:30:00.000000Z')," +
                            "('BTC', 30, '2024-01-01T01:00:00.000000Z')"
            );

            // SAMPLE BY 1h ROLLUP(symbol) FILL(NULL) should work
            assertGroupingSetsQuery(
                    "ts\tsymbol\tSUM\n" +
                            "2024-01-01T00:00:00.000000Z\t\t30.0\n" +
                            "2024-01-01T00:00:00.000000Z\tBTC\t10.0\n" +
                            "2024-01-01T00:00:00.000000Z\tETH\t20.0\n" +
                            "2024-01-01T01:00:00.000000Z\t\t30.0\n" +
                            "2024-01-01T01:00:00.000000Z\tBTC\t30.0\n" +
                            "2024-01-01T01:00:00.000000Z\tETH\tnull\n",
                    "SELECT ts, symbol, SUM(quantity) " +
                            "FROM trades " +
                            "SAMPLE BY 1h ROLLUP(symbol) FILL(NULL) " +
                            "ORDER BY ts, symbol"
            );
        });
    }

    @Test
    public void testSampleByRollupFillNullSortStrategies() throws Exception {
        // The grouping sets cursors have no random access, so the RECORDCHAIN and ENCODED
        // strategies fall back to their FULL forms.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (symbol SYMBOL, quantity DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute(
                    "INSERT INTO trades VALUES " +
                            "('BTC', 10, '2024-01-01T00:00:00.000000Z')," +
                            "('ETH', 20, '2024-01-01T00:30:00.000000Z')," +
                            "('BTC', 30, '2024-01-01T01:00:00.000000Z')"
            );
            for (String strategy : new String[]{"light_encoded", "full_encoded", "light_recordchain", "full_recordchain"}) {
                setProperty(PropertyKey.CAIRO_SQL_SAMPLEBY_FILL_SORT_STRATEGY, strategy);
                for (int mode = 0; mode < 2; mode++) {
                    sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                    assertGroupingSetsQuery(
                            """
                                    ts\tsymbol\tSUM
                                    2024-01-01T00:00:00.000000Z\t\t30.0
                                    2024-01-01T00:00:00.000000Z\tBTC\t10.0
                                    2024-01-01T00:00:00.000000Z\tETH\t20.0
                                    2024-01-01T01:00:00.000000Z\t\t30.0
                                    2024-01-01T01:00:00.000000Z\tBTC\t30.0
                                    2024-01-01T01:00:00.000000Z\tETH\tnull
                                    """,
                            "SELECT ts, symbol, SUM(quantity) FROM trades SAMPLE BY 1h ROLLUP(symbol) FILL(NULL) ORDER BY ts, symbol"
                    );
                }
            }
        });
    }

    @Test
    public void testSampleByRollupFillNullWithGap() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE trades (symbol SYMBOL, quantity DOUBLE, ts TIMESTAMP)" +
                            " TIMESTAMP(ts) PARTITION BY DAY"
            );
            execute(
                    "INSERT INTO trades VALUES " +
                            "('BTC', 10, '2024-01-01T00:00:00.000000Z')," +
                            "('BTC', 30, '2024-01-01T02:00:00.000000Z')"
            );

            // FILL(NULL) emits one fill row per key combination per missing
            // timestamp bucket. Key columns retain their actual values,
            // aggregate columns get NULL.
            assertGroupingSetsQuery(
                    "ts\tsymbol\tSUM\n" +
                            "2024-01-01T00:00:00.000000Z\t\t10.0\n" +
                            "2024-01-01T00:00:00.000000Z\tBTC\t10.0\n" +
                            "2024-01-01T01:00:00.000000Z\t\tnull\n" +
                            "2024-01-01T01:00:00.000000Z\tBTC\tnull\n" +
                            "2024-01-01T02:00:00.000000Z\t\t30.0\n" +
                            "2024-01-01T02:00:00.000000Z\tBTC\t30.0\n",
                    "SELECT ts, symbol, SUM(quantity) " +
                            "FROM trades " +
                            "SAMPLE BY 1h ROLLUP(symbol) FILL(NULL) " +
                            "ORDER BY ts, symbol"
            );
        });
    }

    @Test
    public void testSampleByRollupFillOuterQueryDropsColumns() throws Exception {
        // An outer query that leaves out a column of the SAMPLE BY prunes it from the group-by
        // model. The fill values must still land on the aggregates the user listed them for, and
        // a pruned aggregate named like the hidden fill key must not take its place. The reference
        // is one plain SAMPLE BY FILL query per grouping set.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE k (ts TIMESTAMP, s VARCHAR, v LONG) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO k VALUES ('2024-01-01T00:00:00', 'a', 10), ('2024-01-01T12:00:00', 'b', 20)");
            final String range = " FROM '2024-01-01' TO '2024-01-02' ";
            final String reference = "SELECT * FROM (" +
                    "SELECT ts, s, sum(v) sv, count() c FROM k SAMPLE BY 6h" + range + "FILL(7, 8) " +
                    "UNION ALL SELECT ts, NULL::VARCHAR s, sv, c FROM (SELECT ts, sum(v) sv, count() c FROM k SAMPLE BY 6h" + range + "FILL(7, 8))" +
                    ") ORDER BY ts, s";
            final String inner = "SELECT ts, s, GROUPING(s) g, sum(v) sv, count() c FROM k SAMPLE BY 6h ROLLUP(s)" + range + "FILL(7, 8)";
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                assertSqlCursors(reference, "SELECT ts, s, sv, c FROM (" + inner + ") ORDER BY ts, s");
                assertSqlCursors(reference, "WITH r AS (" + inner + ") SELECT ts, s, sv, c FROM r ORDER BY ts, s");
                assertSqlCursors(
                        reference,
                        "SELECT ts, s, sv, c FROM (" +
                                "SELECT ts, s, GROUPING_ID(s) gid, sum(v) sv, count() c FROM k SAMPLE BY 6h ROLLUP(s)" + range + "FILL(7, 8)" +
                                ") ORDER BY ts, s"
                );
                assertSqlCursors(
                        "SELECT ts, s, sv FROM (" + reference + ")",
                        "SELECT ts, s, sv FROM (" +
                                "SELECT ts, s, sum(v) sv, count() grouping_set_fill_key FROM k SAMPLE BY 6h ROLLUP(s)" + range + "FILL(7, 8)" +
                                ") ORDER BY ts, s"
                );
            }
        });
    }

    @Test
    public void testSampleByRollupFillPrevNotSupported() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE trades (symbol SYMBOL, quantity DOUBLE, ts TIMESTAMP)" +
                            " TIMESTAMP(ts) PARTITION BY DAY"
            );

            assertException(
                    "SELECT ts, symbol, SUM(quantity) " +
                            "FROM trades " +
                            "SAMPLE BY 1h ROLLUP(symbol) FILL(PREV)",
                    78,
                    "FILL(PREV) and FILL(LINEAR) are not supported with ROLLUP, CUBE, or GROUPING SETS"
            );
        });
    }

    @Test
    public void testSampleByRollupFromSubquery() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, v INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES (1, 3, '2024-01-01T00:00:00.000000Z'), (2, 5, '2024-01-01T01:00:00.000000Z'), (1, 7, '2024-01-02T00:00:00.000000Z')");
            String expected = "ts\ta\tsum\n" +
                    "2024-01-01T00:00:00.000000Z\tnull\t8\n" +
                    "2024-01-01T00:00:00.000000Z\t1\t3\n" +
                    "2024-01-01T00:00:00.000000Z\t2\t5\n" +
                    "2024-01-02T00:00:00.000000Z\tnull\t7\n" +
                    "2024-01-02T00:00:00.000000Z\t1\t7\n";
            for (int i = 0; i < 2; i++) {
                sqlExecutionContext.setParallelGroupByEnabled(i == 1);
                assertQuery("SELECT ts, a, sum(v) FROM (SELECT * FROM t) SAMPLE BY 1d ROLLUP(a) ORDER BY ts, a")
                        .withContext(sqlExecutionContext)
                        .inferRandomAccess()
                        .inferTimestamp()
                        .sizeMayVary()
                        .returns(expected);
                assertQuery("SELECT ts, a, sum(v) FROM (SELECT * FROM t WHERE v > 0) SAMPLE BY 1d ROLLUP(a) ORDER BY ts, a")
                        .withContext(sqlExecutionContext)
                        .inferRandomAccess()
                        .inferTimestamp()
                        .sizeMayVary()
                        .returns(expected);
                assertQuery("SELECT ts, a, sum(v) FROM (SELECT * FROM t ORDER BY ts) SAMPLE BY 1d ROLLUP(a) ORDER BY ts, a")
                        .withContext(sqlExecutionContext)
                        .inferRandomAccess()
                        .inferTimestamp()
                        .sizeMayVary()
                        .returns(expected);
                assertQuery("WITH s AS (SELECT ts, a, v FROM t) SELECT ts, a, sum(v) FROM s SAMPLE BY 1d ROLLUP(a) ORDER BY ts, a")
                        .withContext(sqlExecutionContext)
                        .inferRandomAccess()
                        .inferTimestamp()
                        .sizeMayVary()
                        .returns(expected);
                assertQuery("SELECT k, a, sum(v) FROM (SELECT ts k, a, v FROM t) SAMPLE BY 1d ROLLUP(a) ORDER BY k, a")
                        .withContext(sqlExecutionContext)
                        .inferRandomAccess()
                        .inferTimestamp()
                        .sizeMayVary()
                        .returns(expected.replace("ts\t", "k\t"));
                // FILL survives the sub-query: a = 2 has no row on 2024-01-02.
                final String expectedFill = expected + "2024-01-02T00:00:00.000000Z\t2\tnull\n";
                assertQuery("SELECT ts, a, sum(v) FROM (SELECT * FROM t) SAMPLE BY 1d ROLLUP(a) FILL(NULL) ORDER BY ts, a")
                        .withContext(sqlExecutionContext)
                        .withPlanContaining("Sample By Fill")
                        .inferRandomAccess()
                        .inferTimestamp()
                        .sizeMayVary()
                        .returns(expectedFill);
                assertQuery("WITH s AS (SELECT ts, a, v FROM t) SELECT ts, a, sum(v) FROM s SAMPLE BY 1d ROLLUP(a) FILL(NULL) ORDER BY ts, a")
                        .withContext(sqlExecutionContext)
                        .withPlanContaining("Sample By Fill")
                        .inferRandomAccess()
                        .inferTimestamp()
                        .sizeMayVary()
                        .returns(expectedFill);

                // Without a designated timestamp the subquery is rejected, also where it has
                // the timestamp column but loses its order
                String[] noTimestampQueries = {
                        "SELECT a, sum(v) FROM (SELECT a, v FROM t) SAMPLE BY 1d ROLLUP(a)",
                        "SELECT ts, a, sum(v) FROM (SELECT * FROM t ORDER BY a) SAMPLE BY 1d ROLLUP(a)",
                        "SELECT ts, a, sum(v) FROM (SELECT * FROM t ORDER BY ts DESC) SAMPLE BY 1d ROLLUP(a)",
                        "SELECT ts, a, sum(v) FROM (SELECT a, v, ts FROM t UNION ALL SELECT a, v, ts FROM t) SAMPLE BY 1d ROLLUP(a)"
                };
                for (String query : noTimestampQueries) {
                    assertQuery(query)
                            .withContext(sqlExecutionContext)
                            .fails(query.indexOf("(SELECT") + 1, "base query does not provide designated TIMESTAMP column");
                }
            }
        });
    }

    @Test
    public void testSampleByRollupMultipleBuckets() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE trades (symbol SYMBOL, quantity DOUBLE, ts TIMESTAMP)" +
                            " TIMESTAMP(ts) PARTITION BY DAY"
            );
            execute(
                    "INSERT INTO trades VALUES " +
                            "('BTC', 10, '2024-01-01T00:00:00.000000Z')," +
                            "('ETH', 20, '2024-01-01T00:30:00.000000Z')," +
                            "('BTC', 30, '2024-01-01T01:00:00.000000Z')," +
                            "('ETH', 40, '2024-01-01T01:30:00.000000Z')," +
                            "('BTC', 50, '2024-01-01T02:00:00.000000Z')"
            );

            // 3 time buckets, each with per-symbol detail + grand total
            assertGroupingSetsQuery(
                    "ts\tsymbol\tSUM\n" +
                            "2024-01-01T00:00:00.000000Z\t\t30.0\n" +
                            "2024-01-01T00:00:00.000000Z\tBTC\t10.0\n" +
                            "2024-01-01T00:00:00.000000Z\tETH\t20.0\n" +
                            "2024-01-01T01:00:00.000000Z\t\t70.0\n" +
                            "2024-01-01T01:00:00.000000Z\tBTC\t30.0\n" +
                            "2024-01-01T01:00:00.000000Z\tETH\t40.0\n" +
                            "2024-01-01T02:00:00.000000Z\t\t50.0\n" +
                            "2024-01-01T02:00:00.000000Z\tBTC\t50.0\n",
                    "SELECT ts, symbol, SUM(quantity) " +
                            "FROM trades " +
                            "SAMPLE BY 1h ROLLUP(symbol) " +
                            "ORDER BY ts, symbol"
            );
        });
    }

    @Test
    public void testSampleByRollupOuterKeyFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, a INT, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('2024-01-01',1,3), ('2024-01-02',1,4), ('2024-01-02',2,5)");
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                assertQuery("SELECT * FROM (SELECT ts,a,SUM(v) s FROM t SAMPLE BY 1d ROLLUP(a)) WHERE a=1 ORDER BY ts,a")
                        .withContext(sqlExecutionContext).timestamp("ts").returns("""
                                ts\ta\ts
                                2024-01-01T00:00:00.000000Z\t1\t3.0
                                2024-01-02T00:00:00.000000Z\t1\t4.0
                                """);
            }
        });
    }

    @Test
    public void testSampleByRollupOverJoin() throws Exception {
        // Day 1 joins b = 2 with c = 5, day 3 joins b = 3 with c = 6, and day 2 has no rows.
        // FILL fills every grouping set apart: each set's keys get a row on every day.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE p (a INT, b INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE q (a INT, c INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO p VALUES (1, 2, '2024-01-01'), (2, 3, '2024-01-03')");
            execute("INSERT INTO q VALUES (1, 5, '2024-01-01'), (2, 6, '2024-01-01')");
            final String rollupC = "SELECT p.ts, c, count() n FROM p JOIN q ON (a) SAMPLE BY 1d ROLLUP(c) ";
            final String rollupBC = "SELECT p.ts, b, c, count() n FROM p JOIN q ON (a) SAMPLE BY 1d ROLLUP(b, c) ";
            final String rowsC = """
                    ts\tc\tn
                    2024-01-01T00:00:00.000000Z\tnull\t1
                    2024-01-01T00:00:00.000000Z\t5\t1
                    2024-01-03T00:00:00.000000Z\tnull\t1
                    2024-01-03T00:00:00.000000Z\t6\t1
                    """;
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                assertQuery(rollupC + "ORDER BY ts, c").timestamp("ts").sizeMayVary().returns(rowsC);
                assertQuery(rollupC + "FILL(NONE) ORDER BY ts, c").timestamp("ts").sizeMayVary().returns(rowsC);
                assertQuery(rollupC + "FILL(NULL) ORDER BY ts, c").timestamp("ts").sizeMayVary().returns("""
                        ts\tc\tn
                        2024-01-01T00:00:00.000000Z\tnull\t1
                        2024-01-01T00:00:00.000000Z\t5\t1
                        2024-01-01T00:00:00.000000Z\t6\tnull
                        2024-01-02T00:00:00.000000Z\tnull\tnull
                        2024-01-02T00:00:00.000000Z\t5\tnull
                        2024-01-02T00:00:00.000000Z\t6\tnull
                        2024-01-03T00:00:00.000000Z\tnull\t1
                        2024-01-03T00:00:00.000000Z\t5\tnull
                        2024-01-03T00:00:00.000000Z\t6\t1
                        """);
                // The hidden fill key spans keys of both tables.
                assertQuery(rollupBC + "FILL(0) ORDER BY ts, b, c").timestamp("ts").sizeMayVary().returns("""
                        ts\tb\tc\tn
                        2024-01-01T00:00:00.000000Z\tnull\tnull\t1
                        2024-01-01T00:00:00.000000Z\t2\tnull\t1
                        2024-01-01T00:00:00.000000Z\t2\t5\t1
                        2024-01-01T00:00:00.000000Z\t3\tnull\t0
                        2024-01-01T00:00:00.000000Z\t3\t6\t0
                        2024-01-02T00:00:00.000000Z\tnull\tnull\t0
                        2024-01-02T00:00:00.000000Z\t2\tnull\t0
                        2024-01-02T00:00:00.000000Z\t2\t5\t0
                        2024-01-02T00:00:00.000000Z\t3\tnull\t0
                        2024-01-02T00:00:00.000000Z\t3\t6\t0
                        2024-01-03T00:00:00.000000Z\tnull\tnull\t1
                        2024-01-03T00:00:00.000000Z\t2\tnull\t0
                        2024-01-03T00:00:00.000000Z\t2\t5\t0
                        2024-01-03T00:00:00.000000Z\t3\tnull\t1
                        2024-01-03T00:00:00.000000Z\t3\t6\t1
                        """);
            }
        });
    }

    @Test
    public void testSampleByRollupRejectsAdditionalGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v INT, ts TIMESTAMP)");

            String query = "SELECT a, b, SUM(v) FROM t " +
                    "SAMPLE BY 1d ROLLUP(a) GROUP BY b";
            assertException(
                    query,
                    query.indexOf("GROUP BY"),
                    "SELECT query must not contain both GROUP BY and SAMPLE BY"
            );

            query = "SELECT a, b, SUM(v) FROM t " +
                    "SAMPLE BY 1d ROLLUP(a) GROUP BY ROLLUP(b)";
            assertException(
                    query,
                    query.indexOf("GROUP BY"),
                    "SELECT query must not contain both GROUP BY and SAMPLE BY"
            );
        });
    }

    @Test
    public void testSampleByRollupReorderedKeys() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES (1,10,3,'2024-01-01'), (1,20,4,'2024-01-01'), (2,10,5,'2024-01-01')");
            for (int mode = 0; mode < 2; mode++) {
                sqlExecutionContext.setParallelGroupByEnabled(mode == 1);
                assertQuery("SELECT ts, b, a, GROUPING_ID(a,b) AS gid, SUM(v) AS s FROM t SAMPLE BY 1d ROLLUP(a,b) ORDER BY ts,gid,a,b")
                        .withContext(sqlExecutionContext).expectSize().timestamp("ts").returns("""
                                ts\tb\ta\tgid\ts
                                2024-01-01T00:00:00.000000Z\t10\t1\t0\t3.0
                                2024-01-01T00:00:00.000000Z\t20\t1\t0\t4.0
                                2024-01-01T00:00:00.000000Z\t10\t2\t0\t5.0
                                2024-01-01T00:00:00.000000Z\tnull\t1\t1\t7.0
                                2024-01-01T00:00:00.000000Z\tnull\t2\t1\t5.0
                                2024-01-01T00:00:00.000000Z\tnull\tnull\t3\t12.0
                                """);
            }
        });
    }

    @Test
    public void testSampleByRollupTwoColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE trades (symbol SYMBOL, side SYMBOL, quantity DOUBLE, ts TIMESTAMP)" +
                            " TIMESTAMP(ts) PARTITION BY DAY"
            );
            execute(
                    "INSERT INTO trades VALUES " +
                            "('BTC', 'buy', 10, '2024-01-01T00:00:00.000000Z')," +
                            "('BTC', 'sell', 20, '2024-01-01T00:30:00.000000Z')," +
                            "('ETH', 'buy', 30, '2024-01-01T00:45:00.000000Z')"
            );

            // SAMPLE BY 1h ROLLUP(symbol, side): detail, by-symbol subtotals, grand total per hour
            assertGroupingSetsQuery(
                    "ts\tsymbol\tside\tSUM\n" +
                            "2024-01-01T00:00:00.000000Z\tBTC\tbuy\t10.0\n" +
                            "2024-01-01T00:00:00.000000Z\tBTC\tsell\t20.0\n" +
                            "2024-01-01T00:00:00.000000Z\tETH\tbuy\t30.0\n" +
                            "2024-01-01T00:00:00.000000Z\tBTC\t\t30.0\n" +
                            "2024-01-01T00:00:00.000000Z\tETH\t\t30.0\n" +
                            "2024-01-01T00:00:00.000000Z\t\t\t60.0\n",
                    "SELECT ts, symbol, side, SUM(quantity) " +
                            "FROM trades " +
                            "SAMPLE BY 1h ROLLUP(symbol, side)"
            );
        });
    }

    @Test
    public void testSampleByRollupWithBindVariableIntervalNotSupported() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            for (String grouping : new String[]{"ROLLUP(a)", "CUBE(a)", "GROUPING SETS ((a), ())"}) {
                String query = "SELECT ts, a, SUM(v) FROM t SAMPLE BY $1 d " + grouping;
                assertException(query, query.indexOf("$1"), "SAMPLE BY with grouping sets requires a constant interval");
            }
        });
    }

    @Test
    public void testSampleByRollupWithFirstObservationAlignmentNotSupported() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");

            String query = "SELECT ts, a, SUM(v) FROM t " +
                    "SAMPLE BY 1d ROLLUP(a) ALIGN TO FIRST OBSERVATION";
            assertException(
                    query,
                    query.indexOf("1d"),
                    "SAMPLE BY with grouping sets requires calendar alignment"
            );
        });
    }

    @Test
    public void testSampleByRollupWithFrom() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE trades (symbol SYMBOL, quantity DOUBLE, ts TIMESTAMP)" +
                            " TIMESTAMP(ts) PARTITION BY DAY"
            );
            execute(
                    "INSERT INTO trades VALUES " +
                            "('BTC', 10, '2024-01-01T00:00:00.000000Z')," +
                            "('ETH', 20, '2024-01-01T00:30:00.000000Z')," +
                            "('BTC', 30, '2024-01-01T01:00:00.000000Z')"
            );

            // SAMPLE BY 1h ROLLUP(symbol) FROM '...' should work
            assertGroupingSetsQuery(
                    "ts\tsymbol\tSUM\n" +
                            "2024-01-01T00:00:00.000000Z\t\t30.0\n" +
                            "2024-01-01T00:00:00.000000Z\tBTC\t10.0\n" +
                            "2024-01-01T00:00:00.000000Z\tETH\t20.0\n" +
                            "2024-01-01T01:00:00.000000Z\t\t30.0\n" +
                            "2024-01-01T01:00:00.000000Z\tBTC\t30.0\n",
                    "SELECT ts, symbol, SUM(quantity) " +
                            "FROM trades " +
                            "SAMPLE BY 1h ROLLUP(symbol) " +
                            "FROM '2024-01-01' " +
                            "ORDER BY ts, symbol"
            );
        });
    }

    @Test
    public void testSampleByRollupWithNonLiteralFromNotSupported() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");

            String rollupQuery = "SELECT ts, a, SUM(v) FROM t SAMPLE BY 1d ROLLUP(a) FROM now()";
            String cubeQuery = "SELECT ts, a, SUM(v) FROM t SAMPLE BY 1d CUBE(a) FROM now()";
            String groupingSetsQuery = "SELECT ts, a, SUM(v) FROM t SAMPLE BY 1d GROUPING SETS ((a), ()) FROM now()";
            assertException(
                    rollupQuery,
                    rollupQuery.indexOf("now()"),
                    "non-literal FROM bounds are not supported with SAMPLE BY grouping sets"
            );
            assertException(
                    cubeQuery,
                    cubeQuery.indexOf("now()"),
                    "non-literal FROM bounds are not supported with SAMPLE BY grouping sets"
            );
            assertException(
                    groupingSetsQuery,
                    groupingSetsQuery.indexOf("now()"),
                    "non-literal FROM bounds are not supported with SAMPLE BY grouping sets"
            );
        });
    }

    @Test
    public void testSampleByRollupWithoutDesignatedTimestamp() throws Exception {
        // The table has a TIMESTAMP column but no designated timestamp, so SAMPLE BY reports that,
        // as it does without grouping sets, and points at the table.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE nd (a INT, v LONG, ts TIMESTAMP)");
            for (String grouping : new String[]{"ROLLUP(a)", "CUBE(a)", "GROUPING SETS ((a), ())", "ROLLUP(a) FILL(NULL)"}) {
                final String query = "SELECT ts, a, sum(v) FROM nd SAMPLE BY 1h " + grouping;
                assertException(query, query.indexOf("nd"), "base query does not provide designated TIMESTAMP column");
            }
        });
    }

    @Test
    public void testTrailingGroupingItemsAfterCompositeCubeRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, c INT, v INT)");
            assertException(
                    "SELECT a,b,c,SUM(v) FROM t GROUP BY a, CUBE(b), c",
                    46,
                    "grouping items after ROLLUP, CUBE, or GROUPING SETS are not supported"
            );
        });
    }

    @Test
    public void testTrailingGroupingItemsAfterCompositeRollupRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, c INT, v INT)");
            assertException(
                    "SELECT a,b,c,SUM(v) FROM t GROUP BY a, ROLLUP(b), c",
                    48,
                    "grouping items after ROLLUP, CUBE, or GROUPING SETS are not supported"
            );
        });
    }

    @Test
    public void testTrailingGroupingItemsAfterCubeRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v INT)");
            assertException(
                    "SELECT a,b,SUM(v) FROM t GROUP BY CUBE(a), b",
                    41,
                    "grouping items after ROLLUP, CUBE, or GROUPING SETS are not supported"
            );
        });
    }

    @Test
    public void testTrailingGroupingItemsAfterExplicitSetsRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v INT)");
            assertException(
                    "SELECT a,b,SUM(v) FROM t GROUP BY GROUPING SETS ((a)), b",
                    53,
                    "grouping items after ROLLUP, CUBE, or GROUPING SETS are not supported"
            );
        });
    }

    @Test
    public void testTrailingGroupingItemsAfterRollupRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v INT)");
            assertException(
                    "SELECT a,b,SUM(v) FROM t GROUP BY ROLLUP(a), b",
                    43,
                    "grouping items after ROLLUP, CUBE, or GROUPING SETS are not supported"
            );
        });
    }

    @Test
    public void testTrailingGroupingItemsInSubqueryRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v INT)");
            assertException(
                    "SELECT * FROM (SELECT a,b,SUM(v) FROM t GROUP BY ROLLUP(a) /* end */ , b)",
                    69,
                    "grouping items after ROLLUP, CUBE, or GROUPING SETS are not supported"
            );
        });
    }

    @Test
    public void testWorkerFunctionCloseFailuresAreReported() throws Exception {
        // Closing the factory closes the owner and every per-worker copy of the aggregate even
        // when each close fails, and reports one failure with the others suppressed.
        assertMemoryLeak(() -> {
            // test_fault() folds to TRUE outside dev mode.
            setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
            try (WorkerPool pool = new TestWorkerPool(4)) {
                TestUtils.execute(pool, (engine, compiler, context) -> {
                    execute(compiler, "CREATE TABLE t (a INT, v INT)", context);
                    execute(compiler, "INSERT INTO t VALUES (1, 3), (2, 5)", context);
                    context.setParallelGroupByEnabled(true);
                    TestFaultFunctionFactory.armCloseFailures();
                    try {
                        RecordCursorFactory factory = compiler.compile(
                                "SELECT a, sum(CASE WHEN test_fault() THEN v ELSE 0 END) s FROM t GROUP BY ROLLUP(a)",
                                context
                        ).getRecordCursorFactory();
                        Assert.assertTrue(findGroupByFactory(factory) instanceof AsyncGroupingSetsRecordCursorFactory);
                        // the owner's copy and one per worker
                        final int created = TestFaultFunctionFactory.created();
                        Assert.assertTrue("expected copies for several workers, got " + created, created > 2);
                        try {
                            factory.close();
                            Assert.fail("expected the injected close failures");
                        } catch (Throwable failure) {
                            Assert.assertEquals(created, TestFaultFunctionFactory.closeCalls());
                            Assert.assertEquals(created, TestFaultFunctionFactory.closeFailureCount());
                            Set<Throwable> reported = ConcurrentHashMap.newKeySet();
                            reported.add(failure);
                            reported.addAll(Arrays.asList(failure.getSuppressed()));
                            for (int i = 0; i < created; i++) {
                                Assert.assertTrue("close failure " + i + " was not reported", reported.contains(TestFaultFunctionFactory.closeFailure(i)));
                            }
                        }
                    } finally {
                        TestFaultFunctionFactory.disarm();
                    }
                }, configuration, LOG);
            }
        });
    }

    @Test
    public void testWorkerPoolGroupingSetsFilterAndAggregate() throws Exception {
        // Each worker filters its page frames with its own copy of the filter. The first thread to
        // evaluate the filter waits for a second one, so two threads filter and the per-set maps
        // they build must merge. Expected sums: even v in 1..2800 total 1400 * 1401 = 1_961_400,
        // multiples of 4 total 4 * 700 * 701 / 2 = 981_400, and a = 2 takes the rest, 980_000.
        assertMemoryLeak(() -> {
            try (WorkerPool pool = new TestWorkerPool(4)) {
                TestUtils.execute(pool, (engine, compiler, context) -> {
                    execute(compiler,
                            "CREATE TABLE t (a INT, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY",
                            context);
                    execute(compiler,
                            "INSERT INTO t SELECT (x % 4)::INT, x, ((x - 1) / 700 * 86_400_000_000L)::TIMESTAMP FROM long_sequence(2_800)",
                            context);
                    execute(compiler, "ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts < '1970-01-03'", context);
                    context.setParallelGroupByEnabled(true);
                    Set<Thread> filteringThreads = ConcurrentHashMap.newKeySet();
                    CountDownLatch secondThreadArrived = new CountDownLatch(2);
                    TestLatchedCounterFunctionFactory.reset(new TestLatchedCounterFunctionFactory.Callback() {
                        @Override
                        public boolean onGet(Record record, int count) {
                            if (filteringThreads.add(Thread.currentThread())) {
                                secondThreadArrived.countDown();
                                try {
                                    secondThreadArrived.await(10, TimeUnit.SECONDS);
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                            }
                            return true;
                        }
                    });
                    try {
                        assertQuery("SELECT a, sum(v) s FROM t WHERE test_latched_counter() AND v % 2 = 0 GROUP BY ROLLUP(a) ORDER BY a")
                                .withContext(context)
                                .withPlanContaining("Async Group By")
                                .sizeMayVary()
                                .noLeakCheck()
                                .returns("""
                                        a\ts
                                        null\t1961400
                                        0\t981400
                                        2\t980000
                                        """);
                        Assert.assertTrue(
                                "at least two threads must filter, so that their partial maps merge: " + filteringThreads,
                                filteringThreads.size() >= 2
                        );
                    } finally {
                        TestLatchedCounterFunctionFactory.reset(null);
                    }
                }, configuration, LOG);
            }
        });
    }

    @Test
    public void testWorkerPoolGroupingSetsMergeAndReuse() throws Exception {
        assertMemoryLeak(() -> {
            try (WorkerPool pool = new TestWorkerPool(4)) {
                TestUtils.execute(pool, (engine, compiler, context) -> {
                    execute(compiler,
                            "CREATE TABLE t (a INT, b INT, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY",
                            context);
                    execute(compiler,
                            "INSERT INTO t SELECT (x % 4)::INT, (x % 7)::INT, x, ((x - 1) / 700 * 86_400_000_000L)::TIMESTAMP FROM long_sequence(2_800)",
                            context);
                    execute(compiler, "ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts >= 0", context);
                    context.setParallelGroupByEnabled(true);
                    String query = "SELECT a, b, GROUPING_ID(a, b) AS gid, SUM(v) AS n " +
                            "FROM t GROUP BY GROUPING SETS ((a, b), (a), ()) ORDER BY gid, a, b";
                    final Thread ownerThread = Thread.currentThread();
                    String aggregateQuery = query.replace("SUM(v)",
                            "SUM(CASE WHEN test_latched_counter() THEN v ELSE v END)");
                    AtomicBoolean isSingleThreadWorkerObserved = new AtomicBoolean();
                    TestLatchedCounterFunctionFactory.reset(new TestLatchedCounterFunctionFactory.Callback() {
                        @Override
                        public boolean onGet(Record record, int count) {
                            if (Thread.currentThread() != ownerThread) {
                                isSingleThreadWorkerObserved.set(true);
                            }
                            return true;
                        }
                    });
                    context.setParallelGroupByEnabled(false);
                    try {
                        assertQuery("SELECT gid, COUNT() AS group_count, SUM(n) AS row_count FROM (" + aggregateQuery + ") GROUP BY gid ORDER BY gid")
                                .withContext(context)
                                .expectSize()
                                .noLeakCheck()
                                .returns("gid\tgroup_count\trow_count\n0\t28\t3921400\n1\t4\t3921400\n3\t1\t3921400\n");
                        Assert.assertFalse("single-threaded grouping must stay on the owner", isSingleThreadWorkerObserved.get());
                    } finally {
                        TestLatchedCounterFunctionFactory.reset(null);
                    }
                    context.setParallelGroupByEnabled(true);
                    AtomicBoolean isWorkerObserved = new AtomicBoolean();
                    // Every day partition holds every (a, b) key, so when two threads aggregate
                    // rows, both build partial maps with the same keys and the totals below are
                    // right only if those maps merge. The first thread to see a row waits for a
                    // second thread to arrive, so the frames cannot all go to one thread.
                    Set<Thread> aggregatingThreads = ConcurrentHashMap.newKeySet();
                    CountDownLatch secondThreadArrived = new CountDownLatch(2);
                    TestLatchedCounterFunctionFactory.reset(new TestLatchedCounterFunctionFactory.Callback() {
                        @Override
                        public boolean onGet(Record record, int count) {
                            if (Thread.currentThread() != ownerThread) {
                                isWorkerObserved.set(true);
                            }
                            if (aggregatingThreads.add(Thread.currentThread())) {
                                secondThreadArrived.countDown();
                                try {
                                    secondThreadArrived.await(10, TimeUnit.SECONDS);
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                            }
                            return true;
                        }
                    });
                    try {
                        assertQuery("SELECT gid, COUNT() AS group_count, SUM(n) AS row_count FROM (" + aggregateQuery + ") GROUP BY gid ORDER BY gid")
                                .withContext(context)
                                .expectSize()
                                .withPlanContaining("Async Group By")
                                .noLeakCheck()
                                .returns("gid\tgroup_count\trow_count\n0\t28\t3921400\n1\t4\t3921400\n3\t1\t3921400\n");
                        Assert.assertTrue("a non-owner worker must process grouping-set input", isWorkerObserved.get());
                        Assert.assertTrue(
                                "at least two threads must aggregate, so that their partial maps merge: " + aggregatingThreads,
                                aggregatingThreads.size() >= 2
                        );
                    } finally {
                        TestLatchedCounterFunctionFactory.reset(null);
                    }
                }, configuration, LOG);
            }
        });
    }

    @Test
    public void testWorkerPoolGroupingSetsOwnerMergeObservesCancellation() throws Exception {
        // The owner merges every worker map of every set. The breaker cancels at the first
        // check of that merge, buildMap's second check after the one before dispatch, and the
        // query must stop there, release its maps and leave the factory reusable.
        assertMemoryLeak(() -> {
            try (WorkerPool pool = new TestWorkerPool(4)) {
                TestUtils.execute(pool, (engine, compiler, executionContext) -> {
                    execute(compiler,
                            "CREATE TABLE t (a INT, b INT, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY",
                            executionContext);
                    execute(compiler,
                            "INSERT INTO t SELECT (x % 4)::INT, (x % 7)::INT, x, ((x - 1) / 700 * 86_400_000_000L)::TIMESTAMP FROM long_sequence(2_800)",
                            executionContext);

                    SqlExecutionContextImpl context = (SqlExecutionContextImpl) executionContext;
                    SqlExecutionCircuitBreakerConfiguration breakerConfiguration =
                            new DefaultSqlExecutionCircuitBreakerConfiguration();
                    final AtomicInteger buildMapChecks = new AtomicInteger();
                    final AtomicBoolean isCancelling = new AtomicBoolean(true);
                    try (NetworkSqlExecutionCircuitBreaker breaker = new NetworkSqlExecutionCircuitBreaker(engine, breakerConfiguration) {
                        @Override
                        public void statefulThrowExceptionIfTrippedTimeThrottledOrYield() {
                            final boolean isBuildMapCheck = StackWalker.getInstance().walk(frames -> frames.skip(1).findFirst()
                                    .map(f -> f.getClassName().endsWith("AsyncGroupingSetsCursor") && f.getMethodName().equals("buildMap"))
                                    .orElse(false));
                            if (isCancelling.get() && isBuildMapCheck && buildMapChecks.incrementAndGet() == 2) {
                                cancel();
                                statefulThrowExceptionIfTrippedNoThrottle();
                            }
                            super.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
                        }
                    }) {
                        context.with(
                                context.getSecurityContext(),
                                context.getBindVariableService(),
                                context.getRandom(),
                                context.getRequestFd(),
                                breaker
                        );
                        context.setParallelGroupByEnabled(true);
                        context.changePageFrameSizes(1, 1);
                        String query = "SELECT a, b, sum(v) s FROM t GROUP BY GROUPING SETS ((a, b), (a), ())";
                        try (RecordCursorFactory factory = compiler.compile(query, context).getRecordCursorFactory()) {
                            Assert.assertTrue(factory.getBaseFactory() instanceof AsyncGroupingSetsRecordCursorFactory);
                            try (RecordCursor cursor = factory.getCursor(context)) {
                                try {
                                    cursor.hasNext();
                                    Assert.fail("expected cancellation during the owner merge");
                                } catch (CairoException e) {
                                    TestUtils.assertContains(e.getFlyweightMessage(), "cancelled by user");
                                    Assert.assertTrue(e.isCancellation());
                                }
                            }
                            Assert.assertEquals(2, buildMapChecks.get());

                            // 28 (a, b) pairs, since 4 and 7 are coprime, 4 values of a and the total.
                            isCancelling.set(false);
                            breaker.clear();
                            breaker.clearCancelSentinel();
                            try (RecordCursor cursor = factory.getCursor(context)) {
                                int rows = 0;
                                while (cursor.hasNext()) {
                                    rows++;
                                }
                                Assert.assertEquals(33, rows);
                            }
                        }
                    }
                }, configuration, LOG);
            }
        });
    }

    @Test
    public void testWorkerPoolGroupingSetsParallelSetBudget() throws Exception {
        // Every worker and the owner keep one map per set, so the parallel factory runs only while
        // sets * (workers + 1) <= 256. Past that, the query runs serially. With 4 workers the
        // boundary is 51 sets.
        assertMemoryLeak(() -> {
            try (WorkerPool pool = new TestWorkerPool(4)) {
                TestUtils.execute(pool, (engine, compiler, executionContext) -> {
                    execute(compiler, "CREATE TABLE t (a INT, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY", executionContext);
                    execute(compiler, "INSERT INTO t SELECT (x % 3)::INT, x, (x * 1_000_000L)::TIMESTAMP FROM long_sequence(100)", executionContext);
                    executionContext.setParallelGroupByEnabled(true);
                    Assert.assertEquals(4, executionContext.getSharedQueryWorkerCount());
                    for (int setCount : new int[]{51, 52}) {
                        final StringBuilder sets = new StringBuilder();
                        for (int i = 0; i < setCount; i++) {
                            sets.append(i == 0 ? "(a)" : ", (a)");
                        }
                        final String query = "SELECT a, sum(v) s FROM t GROUP BY GROUPING SETS (" + sets + ")";
                        try (RecordCursorFactory factory = compiler.compile(query, executionContext).getRecordCursorFactory()) {
                            Assert.assertEquals(
                                    "sets: " + setCount,
                                    setCount == 51,
                                    factory.getBaseFactory() instanceof AsyncGroupingSetsRecordCursorFactory
                            );
                            // 3 values of a, once per set.
                            try (RecordCursor cursor = factory.getCursor(executionContext)) {
                                int rows = 0;
                                while (cursor.hasNext()) {
                                    rows++;
                                }
                                Assert.assertEquals(3 * setCount, rows);
                            }
                        }
                    }
                }, configuration, LOG);
            }
        });
    }

    @Test
    public void testWorkerPoolGroupingSetsWorkerCancellationReleasesParquetBuffers() throws Exception {
        assertMemoryLeak(() -> {
            try (WorkerPool pool = new TestWorkerPool(4)) {
                TestUtils.execute(pool, (engine, compiler, executionContext) -> {
                    execute(compiler,
                            "CREATE TABLE t (a INT, b INT, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY",
                            executionContext);
                    execute(compiler,
                            "INSERT INTO t SELECT (x % 4)::INT, (x % 7)::INT, x, ((x - 1) / 700 * 86_400_000_000L)::TIMESTAMP FROM long_sequence(2_800)",
                            executionContext);
                    execute(compiler, "ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts >= 0", executionContext);

                    SqlExecutionContextImpl context = (SqlExecutionContextImpl) executionContext;
                    AtomicBoolean isCancellationInjected = new AtomicBoolean();
                    SqlExecutionCircuitBreakerConfiguration breakerConfiguration =
                            new DefaultSqlExecutionCircuitBreakerConfiguration();
                    try (NetworkSqlExecutionCircuitBreaker breaker = new NetworkSqlExecutionCircuitBreaker(engine, breakerConfiguration)) {
                        context.with(
                                context.getSecurityContext(),
                                context.getBindVariableService(),
                                context.getRandom(),
                                context.getRequestFd(),
                                breaker
                        );
                        context.setParallelGroupByEnabled(true);
                        final Thread testThread = Thread.currentThread();
                        TestLatchedCounterFunctionFactory.reset(new TestLatchedCounterFunctionFactory.Callback() {
                            @Override
                            public boolean onGet(Record record, int count) {
                                if (count >= 64 && Thread.currentThread() != testThread) {
                                    isCancellationInjected.set(true);
                                    breaker.cancel();
                                }
                                return true;
                            }
                        });
                        String query = "SELECT a, b, GROUPING_ID(a, b) AS gid, " +
                                "SUM(CASE WHEN test_latched_counter() THEN v ELSE v END) AS n " +
                                "FROM t GROUP BY GROUPING SETS ((a, b), (a), ())";
                        try {
                            try (RecordCursorFactory factory = compiler.compile(query, context).getRecordCursorFactory()) {
                                Assert.assertTrue(
                                        "cancellation must exercise async grouping sets",
                                        factory.getBaseFactory() instanceof AsyncGroupingSetsRecordCursorFactory
                                );
                                context.changePageFrameSizes(1, 1);
                                try (RecordCursor cursor = factory.getCursor(context)) {
                                    try {
                                        while (cursor.hasNext()) {
                                            // Keep consuming until a worker cancels the query from its aggregate expression.
                                        }
                                        Assert.fail("expected worker cancellation during grouping-set dispatch");
                                    } catch (CairoException e) {
                                        TestUtils.assertContains(e.getFlyweightMessage(), "cancelled by user");
                                        Assert.assertTrue(e.isCancellation());
                                        Assert.assertTrue(e.isInterruption());
                                    }
                                }
                            }
                            Assert.assertTrue("the aggregate expression must cancel from a worker", isCancellationInjected.get());
                        } finally {
                            TestLatchedCounterFunctionFactory.reset(null);
                        }
                    }
                }, configuration, LOG);
            }
        });
    }

    // nullOfType is an expression of the key's type whose value is NULL, the reference for the total row.
    private static void assertRolledUpKeyMatchesReference(String name, String nullOfType, String source) throws Exception {
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "SELECT " + name + ", count() n, 0 g FROM " + source + " GROUP BY " + name
                        + " UNION ALL SELECT " + nullOfType + " " + name + ", count() n, 1 g FROM " + source,
                "SELECT " + name + ", count() n, grouping(" + name + ") g FROM " + source + " GROUP BY ROLLUP(" + name + ") ORDER BY g",
                LOG,
                true
        );
    }

    private static RecordCursorFactory findGroupByFactory(RecordCursorFactory factory) {
        RecordCursorFactory current = factory;
        while (!current.getClass().getSimpleName().contains("Group")) {
            current = current.getBaseFactory();
            Assert.assertNotNull("no group by factory under " + factory.getClass().getSimpleName(), current);
        }
        return current;
    }

    private static void createHorizonJoinTables() throws Exception {
        execute("CREATE TABLE trades (sym SYMBOL, qty INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE prices (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO trades VALUES ('A', 1, '2024-01-01T00:00:05'), ('B', 2, '2024-01-01T00:00:06')");
        execute("INSERT INTO prices VALUES ('A', 10, '2024-01-01T00:00:04'), ('B', 20, '2024-01-01T00:00:05')");
    }

    private void assertFactoryRows(RecordCursorFactory factory, String expected) throws Exception {
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            sink.clear();
            TestUtils.assertCursor(expected, cursor, factory.getMetadata(), true, sink);
        }
    }

    private void assertGroupingSetsQuery(String expected, String query) throws Exception {
        assertQuery(query).inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
    }
}
