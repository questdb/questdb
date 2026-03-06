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

public class GroupingSetsTest extends AbstractCairoTest {

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
            assertSql(
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
            assertSql(
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

            assertSql(
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
            assertSql(
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

            assertSql(
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
            assertSql(
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
    public void testGroupingSetsParserError() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT, y DOUBLE)");
            assertException(
                    "SELECT x, SUM(y) FROM t GROUP BY GROUPING x",
                    33,
                    "expected SETS after GROUPING"
            );
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

            assertSql(
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

            assertSql(
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
    public void testRollupSingleColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT, y DOUBLE)");
            execute("INSERT INTO t VALUES (1, 10), (1, 20), (2, 30)");

            assertSql(
                    "x\tSUM\n" +
                            "1\t30.0\n" +
                            "2\t30.0\n" +
                            "null\t60.0\n",
                    "SELECT x, SUM(y) FROM t GROUP BY ROLLUP(x)"
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
            assertSql(
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

            assertSql(
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

            assertSql(
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

            assertSql(
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

            assertSql(
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

            assertSql(
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

            assertSql(
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

            assertSql(
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

            assertSql(rollupResult,
                    "SELECT a, b, SUM(v) FROM t GROUP BY ROLLUP(a, b)"
            );

            assertSql(rollupResult,
                    "SELECT a, b, SUM(v) FROM t GROUP BY GROUPING SETS ((a, b), (a), ())"
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
            assertSql(
                    "category\tSUM\n" +
                            "\t20.0\n" +
                            "A\t40.0\n" +
                            "\t60.0\n",
                    "SELECT category, SUM(val) FROM t GROUP BY ROLLUP(category)"
            );
        });
    }

    @Test
    public void testRollupCaseInsensitive() throws Exception {
        // ROLLUP keyword should be case-insensitive
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT, y DOUBLE)");
            execute("INSERT INTO t VALUES (1, 10), (2, 20)");

            assertSql(
                    "x\tSUM\n" +
                            "1\t10.0\n" +
                            "2\t20.0\n" +
                            "null\t30.0\n",
                    "SELECT x, SUM(y) FROM t GROUP BY rollup(x)"
            );
        });
    }

    @Test
    public void testCubeCaseInsensitive() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT, y DOUBLE)");
            execute("INSERT INTO t VALUES (1, 10), (2, 20)");

            assertSql(
                    "x\tSUM\n" +
                            "1\t10.0\n" +
                            "2\t20.0\n" +
                            "null\t30.0\n",
                    "SELECT x, SUM(y) FROM t GROUP BY cube(x)"
            );
        });
    }

    @Test
    public void testRollupEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT, y DOUBLE)");

            // Empty table should produce no rows, even for the grand total
            assertSql(
                    "x\tSUM\n",
                    "SELECT x, SUM(y) FROM t GROUP BY ROLLUP(x)"
            );
        });
    }

    // ==================== GROUPING() function tests ====================

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
            assertSql(
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
            assertSql(
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
            assertSql(
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

    // ==================== SAMPLE BY + GROUPING SETS tests ====================

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
            assertSql(
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
            assertSql(
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
            assertSql(
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
            assertSql(
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
            assertSql(
                    "ts\tsymbol\tSUM\n" +
                            "2024-01-01T00:00:00.000000Z\t\t30.0\n" +
                            "2024-01-01T00:00:00.000000Z\tBTC\t10.0\n" +
                            "2024-01-01T00:00:00.000000Z\tETH\t20.0\n" +
                            "2024-01-01T01:00:00.000000Z\t\t30.0\n" +
                            "2024-01-01T01:00:00.000000Z\tBTC\t30.0\n",
                    "SELECT ts, symbol, SUM(quantity) " +
                            "FROM trades " +
                            "SAMPLE BY 1h ROLLUP(symbol) FILL(NULL) " +
                            "ORDER BY ts, symbol"
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
            assertSql(
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
            assertSql(
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
            assertSql(
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
            assertSql(
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

    // ==================== Error / validation tests ====================

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
                    0,
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
                    0,
                    "GROUPING() / GROUPING_ID() can only be used with GROUPING SETS, ROLLUP, or CUBE"
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
                    0,
                    "GROUPING() / GROUPING_ID() can only be used with GROUPING SETS, ROLLUP, or CUBE"
            );
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
            assertSql(
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
    public void testQualifiedReferencesAccepted() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, v DOUBLE)");
            execute("INSERT INTO t VALUES (1, 10, 100.0), (1, 20, 200.0), (2, 10, 300.0)");
            // All qualified - no mixed qualification error
            assertSql(
                    "a\tSUM\n" +
                            "null\t600.0\n" +
                            "1\t300.0\n" +
                            "2\t300.0\n",
                    "SELECT t.a, SUM(v) FROM t GROUP BY ROLLUP(t.a) ORDER BY t.a"
            );
        });
    }
}
