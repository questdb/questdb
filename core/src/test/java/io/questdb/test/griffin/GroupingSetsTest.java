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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.test.TestLatchedCounterFunctionFactory;
import io.questdb.griffin.engine.table.AsyncGroupingSetsRecordCursorFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

public class GroupingSetsTest extends AbstractCairoTest {

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
    public void testCubeSetCountCapApplied() throws Exception {
        // CUBE(16 columns) would produce 2^16 = 65536 sets, exceeding the default 4096
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (c1 INT, c2 INT, c3 INT, c4 INT, c5 INT," +
                    " c6 INT, c7 INT, c8 INT, c9 INT, c10 INT," +
                    " c11 INT, c12 INT, c13 INT, c14 INT, c15 INT, c16 INT, v INT)");
            // CUBE supports at most 15 columns
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
        // Test that the cursor works correctly on repeated execution
        // (toTop after partial iteration)
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a SYMBOL, v DOUBLE)");
            execute("INSERT INTO t VALUES ('x', 10.0), ('y', 20.0)");
            String expected =
                    "a\tSUM\n" +
                            "\t30.0\n" +
                            "x\t10.0\n" +
                            "y\t20.0\n";
            String query = "SELECT a, SUM(v) FROM t GROUP BY ROLLUP(a) ORDER BY a";
            // Execute twice to trigger toTop/reuse
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
                    "column reference expected"
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
                    "column reference expected"
            );
        });
    }

    @Test
    public void testGroupingSetsExpressionRejectedInRollup() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v DOUBLE)");
            assertException(
                    "SELECT SUM(v) FROM t GROUP BY ROLLUP(a + b)",
                    39,
                    "column reference expected"
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
                    "column reference expected"
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
            assertException("SELECT a + 1 AS k, SUM(v) FROM t GROUP BY ROLLUP(k)", 49, "column reference expected");
            assertException("SELECT a + 1 AS k, SUM(v) FROM t GROUP BY CUBE(k)", 47, "column reference expected");
            assertException("SELECT a + 1 AS k, SUM(v) FROM t GROUP BY GROUPING SETS ((k), ())", 58, "column reference expected");
            String groupingAliasQuery = "SELECT a + 1 AS k, GROUPING(k), SUM(v) FROM t GROUP BY ROLLUP(k)";
            assertException(groupingAliasQuery, groupingAliasQuery.indexOf("ROLLUP(k)") + 7, "column reference expected");
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
            assertException("SELECT b::INT AS k, SUM(v) FROM t GROUP BY ROLLUP(k)", 50, "column reference expected");
            assertException("SELECT b::INT AS k, SUM(v) FROM t GROUP BY CUBE(k)", 48, "column reference expected");
            assertException("SELECT b::INT AS k, SUM(v) FROM t GROUP BY GROUPING SETS ((k), ())", 59, "column reference expected");
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
                assertQuery(query).withContext(sqlExecutionContext)
                        .assertsPlanContaining("values: [sum(v)]\n        Async JIT Filter workers: 1\n          filter: a=1");
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
                    TestLatchedCounterFunctionFactory.reset(new TestLatchedCounterFunctionFactory.Callback() {
                        @Override
                        public boolean onGet(Record record, int count) {
                            if (Thread.currentThread() != ownerThread) {
                                isWorkerObserved.set(true);
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
                    } finally {
                        TestLatchedCounterFunctionFactory.reset(null);
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

    private void assertGroupingSetsQuery(String expected, String query) throws Exception {
        assertQuery(query).inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
    }
}
