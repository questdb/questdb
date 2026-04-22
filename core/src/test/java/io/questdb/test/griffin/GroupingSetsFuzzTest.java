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

import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

/**
 * Fuzz tests for GROUPING SETS, ROLLUP, and CUBE. Verifies that:
 * <ul>
 *   <li>ROLLUP and CUBE produce identical results to their explicit GROUPING SETS expansions</li>
 *   <li>Each ROLLUP level matches an independent GROUP BY query</li>
 *   <li>SAMPLE BY + ROLLUP matches SAMPLE BY + GROUPING SETS</li>
 *   <li>Random aggregate functions produce consistent results across equivalent forms</li>
 * </ul>
 */
public class GroupingSetsFuzzTest extends AbstractCairoTest {

    private static final String[] AGGREGATES = {"SUM", "COUNT", "AVG", "MIN", "MAX"};
    private static final int ITERATIONS = 100;
    private static final String[] SIDES = {"buy", "sell"};
    private static final String[] SYMBOLS = {"BTC", "ETH", "SOL", "ADA", "DOT"};
    private Rnd rnd;

    @Override
    @Before
    public void setUp() {
        rnd = TestUtils.generateRandom(LOG);
        super.setUp();
    }

    @Test
    public void testCubeEquivalentToGroupingSets() throws Exception {
        // CUBE(symbol, side) must produce the same rows as
        // GROUPING SETS ((symbol, side), (symbol), (side), ())
        assertMemoryLeak(() -> {
            for (int i = 0; i < ITERATIONS; i++) {
                execute("DROP TABLE IF EXISTS t");
                createRandomTable();

                String cubeQuery = "SELECT * FROM (" +
                        "SELECT symbol, side, SUM(quantity) AS s, GROUPING_ID(symbol, side) AS grp " +
                        "FROM t GROUP BY CUBE(symbol, side)" +
                        ") ORDER BY grp, symbol, side";

                String gsQuery = "SELECT * FROM (" +
                        "SELECT symbol, side, SUM(quantity) AS s, GROUPING_ID(symbol, side) AS grp " +
                        "FROM t GROUP BY GROUPING SETS ((symbol, side), (symbol), (side), ())" +
                        ") ORDER BY grp, symbol, side";

                assertEquivalent(cubeQuery, gsQuery, i);
            }
        });
    }

    @Test
    public void testRollupEquivalentToGroupingSets() throws Exception {
        // ROLLUP(symbol, side) must produce the same rows as
        // GROUPING SETS ((symbol, side), (symbol), ())
        assertMemoryLeak(() -> {
            for (int i = 0; i < ITERATIONS; i++) {
                execute("DROP TABLE IF EXISTS t");
                createRandomTable();

                String rollupQuery = "SELECT * FROM (" +
                        "SELECT symbol, side, SUM(quantity) AS s, GROUPING_ID(symbol, side) AS grp " +
                        "FROM t GROUP BY ROLLUP(symbol, side)" +
                        ") ORDER BY grp, symbol, side";

                String gsQuery = "SELECT * FROM (" +
                        "SELECT symbol, side, SUM(quantity) AS s, GROUPING_ID(symbol, side) AS grp " +
                        "FROM t GROUP BY GROUPING SETS ((symbol, side), (symbol), ())" +
                        ") ORDER BY grp, symbol, side";

                assertEquivalent(rollupQuery, gsQuery, i);
            }
        });
    }

    @Test
    public void testRollupGrandTotalMatchesUngrouped() throws Exception {
        // The grand total row from ROLLUP(symbol) must match SELECT AGG(x) FROM t
        assertMemoryLeak(() -> {
            for (int i = 0; i < ITERATIONS; i++) {
                execute("DROP TABLE IF EXISTS t");
                createRandomTable();

                String agg = randomAggregate("quantity");

                // Grand total from ROLLUP: filter to the rolled-up level
                String rollupGrand = "SELECT v FROM (" +
                        "SELECT " + agg + " AS v, GROUPING(symbol) AS g " +
                        "FROM t GROUP BY ROLLUP(symbol)" +
                        ") WHERE g = 1";

                // Standalone grand total: no GROUP BY
                String standalone = "SELECT " + agg + " AS v FROM t";

                assertEquivalent(rollupGrand, standalone, i, agg);
            }
        });
    }

    @Test
    public void testRollupDetailMatchesGroupBy() throws Exception {
        // Detail rows from ROLLUP(symbol) must match GROUP BY symbol
        assertMemoryLeak(() -> {
            for (int i = 0; i < ITERATIONS; i++) {
                execute("DROP TABLE IF EXISTS t");
                createRandomTable();

                String agg = randomAggregate("quantity");

                // Detail rows from ROLLUP: filter to grouped level
                String rollupDetail = "SELECT symbol, v FROM (" +
                        "SELECT symbol, " + agg + " AS v, GROUPING(symbol) AS g " +
                        "FROM t GROUP BY ROLLUP(symbol)" +
                        ") WHERE g = 0 ORDER BY symbol";

                // Standalone GROUP BY
                String groupBy = "SELECT symbol, " + agg + " AS v " +
                        "FROM t GROUP BY symbol ORDER BY symbol";

                assertEquivalent(rollupDetail, groupBy, i, agg);
            }
        });
    }

    @Test
    public void testRollupTwoColumnsMiddleLevelMatchesGroupBy() throws Exception {
        // For ROLLUP(symbol, side), level grp=1 (side rolled up) must match
        // GROUP BY symbol alone
        assertMemoryLeak(() -> {
            for (int i = 0; i < ITERATIONS; i++) {
                execute("DROP TABLE IF EXISTS t");
                createRandomTable();

                String agg = randomAggregate("quantity");

                // Level 1 from ROLLUP(symbol, side): side is rolled up
                String rollupLevel1 = "SELECT symbol, v FROM (" +
                        "SELECT symbol, side, " + agg + " AS v, GROUPING_ID(symbol, side) AS grp " +
                        "FROM t GROUP BY ROLLUP(symbol, side)" +
                        ") WHERE grp = 1 ORDER BY symbol";

                // Standalone GROUP BY symbol
                String groupBy = "SELECT symbol, " + agg + " AS v " +
                        "FROM t GROUP BY symbol ORDER BY symbol";

                assertEquivalent(rollupLevel1, groupBy, i, agg + " level 1");
            }
        });
    }

    @Test
    public void testRollupWithRandomAggregates() throws Exception {
        // Multiple random aggregates: ROLLUP must match GROUPING SETS
        assertMemoryLeak(() -> {
            for (int i = 0; i < ITERATIONS; i++) {
                execute("DROP TABLE IF EXISTS t");
                createRandomTable();

                String agg1 = randomAggregate("quantity");
                String agg2 = randomAggregate("quantity");

                String rollupQuery = "SELECT * FROM (" +
                        "SELECT symbol, " + agg1 + " AS v1, " + agg2 + " AS v2, " +
                        "GROUPING(symbol) AS g " +
                        "FROM t GROUP BY ROLLUP(symbol)" +
                        ") ORDER BY g, symbol";

                String gsQuery = "SELECT * FROM (" +
                        "SELECT symbol, " + agg1 + " AS v1, " + agg2 + " AS v2, " +
                        "GROUPING(symbol) AS g " +
                        "FROM t GROUP BY GROUPING SETS ((symbol), ())" +
                        ") ORDER BY g, symbol";

                assertEquivalent(rollupQuery, gsQuery, i, agg1 + " + " + agg2);
            }
        });
    }

    @Test
    public void testRollupWithWhereClause() throws Exception {
        // ROLLUP with a WHERE filter must produce the same results as
        // GROUPING SETS with the same WHERE filter
        assertMemoryLeak(() -> {
            for (int i = 0; i < ITERATIONS; i++) {
                execute("DROP TABLE IF EXISTS t");
                createRandomTable();

                // Random threshold between 100 and 800
                int threshold = 100 + rnd.nextInt(700);

                String rollupQuery = "SELECT * FROM (" +
                        "SELECT symbol, SUM(quantity) AS s, GROUPING(symbol) AS g " +
                        "FROM t WHERE quantity > " + threshold + " " +
                        "GROUP BY ROLLUP(symbol)" +
                        ") ORDER BY g, symbol";

                String gsQuery = "SELECT * FROM (" +
                        "SELECT symbol, SUM(quantity) AS s, GROUPING(symbol) AS g " +
                        "FROM t WHERE quantity > " + threshold + " " +
                        "GROUP BY GROUPING SETS ((symbol), ())" +
                        ") ORDER BY g, symbol";

                assertEquivalent(rollupQuery, gsQuery, i, "threshold=" + threshold);
            }
        });
    }

    @Test
    public void testSampleByRollupEquivalentToGroupingSets() throws Exception {
        // SAMPLE BY 1h ROLLUP(symbol) must match
        // SAMPLE BY 1h GROUPING SETS ((symbol), ())
        assertMemoryLeak(() -> {
            for (int i = 0; i < ITERATIONS; i++) {
                execute("DROP TABLE IF EXISTS t");
                createRandomTimestampTable();

                String rollupQuery = "SELECT * FROM (" +
                        "SELECT ts, symbol, SUM(quantity) AS s, GROUPING(symbol) AS g " +
                        "FROM t SAMPLE BY 1h ROLLUP(symbol)" +
                        ") ORDER BY ts, g, symbol";

                String gsQuery = "SELECT * FROM (" +
                        "SELECT ts, symbol, SUM(quantity) AS s, GROUPING(symbol) AS g " +
                        "FROM t SAMPLE BY 1h GROUPING SETS ((symbol), ())" +
                        ") ORDER BY ts, g, symbol";

                assertEquivalent(rollupQuery, gsQuery, i);
            }
        });
    }

    @Test
    public void testSampleByCubeEquivalentToGroupingSets() throws Exception {
        // SAMPLE BY 1h CUBE(symbol, side) must match the explicit expansion
        assertMemoryLeak(() -> {
            for (int i = 0; i < ITERATIONS; i++) {
                execute("DROP TABLE IF EXISTS t");
                createRandomTimestampTableTwoKeys();

                String cubeQuery = "SELECT * FROM (" +
                        "SELECT ts, symbol, side, SUM(quantity) AS s, GROUPING_ID(symbol, side) AS grp " +
                        "FROM t SAMPLE BY 1h CUBE(symbol, side)" +
                        ") ORDER BY ts, grp, symbol, side";

                String gsQuery = "SELECT * FROM (" +
                        "SELECT ts, symbol, side, SUM(quantity) AS s, GROUPING_ID(symbol, side) AS grp " +
                        "FROM t SAMPLE BY 1h GROUPING SETS ((symbol, side), (symbol), (side), ())" +
                        ") ORDER BY ts, grp, symbol, side";

                assertEquivalent(cubeQuery, gsQuery, i);
            }
        });
    }

    @Test
    public void testSampleByRollupFillConstantEquivalence() throws Exception {
        // SAMPLE BY ROLLUP with FILL(0) must match GROUPING SETS with FILL(0)
        assertMemoryLeak(() -> {
            for (int i = 0; i < ITERATIONS; i++) {
                execute("DROP TABLE IF EXISTS t");
                createRandomTimestampTable();

                String rollupQuery = "SELECT * FROM (" +
                        "SELECT ts, symbol, SUM(quantity) AS s, GROUPING(symbol) AS g " +
                        "FROM t SAMPLE BY 1h ROLLUP(symbol) FILL(0)" +
                        ") ORDER BY ts, g, symbol";

                String gsQuery = "SELECT * FROM (" +
                        "SELECT ts, symbol, SUM(quantity) AS s, GROUPING(symbol) AS g " +
                        "FROM t SAMPLE BY 1h GROUPING SETS ((symbol), ()) FILL(0)" +
                        ") ORDER BY ts, g, symbol";

                assertEquivalent(rollupQuery, gsQuery, i);
            }
        });
    }

    private void assertEquivalent(String query1, String query2, int iteration) {
        assertEquivalent(query1, query2, iteration, null);
    }

    private void assertEquivalent(String query1, String query2, int iteration, String context) {
        try {
            StringSink result1 = new StringSink();
            printSql(query1, result1);

            StringSink result2 = new StringSink();
            printSql(query2, result2);

            TestUtils.assertEquals(result1, result2);
        } catch (Exception e) {
            String msg = "Iteration " + iteration +
                    (context != null ? " (" + context + ")" : "") +
                    " failed.\nQuery 1: " + query1 +
                    "\nQuery 2: " + query2;
            throw new AssertionError(msg, e);
        }
    }

    private void createRandomTable() throws Exception {
        execute("CREATE TABLE t (symbol SYMBOL, side SYMBOL, quantity DOUBLE)");
        int rows = 5 + rnd.nextInt(46); // 5-50 rows
        StringBuilder sb = new StringBuilder("INSERT INTO t VALUES ");
        for (int r = 0; r < rows; r++) {
            if (r > 0) {
                sb.append(',');
            }
            // ~10% chance of NULL symbol or side to test data NULLs vs rollup NULLs
            boolean nullSymbol = rnd.nextInt(10) == 0;
            boolean nullSide = rnd.nextInt(10) == 0;
            sb.append('(');
            if (nullSymbol) {
                sb.append("NULL");
            } else {
                sb.append('\'').append(SYMBOLS[rnd.nextInt(SYMBOLS.length)]).append('\'');
            }
            sb.append(", ");
            if (nullSide) {
                sb.append("NULL");
            } else {
                sb.append('\'').append(SIDES[rnd.nextInt(SIDES.length)]).append('\'');
            }
            sb.append(", ");
            sb.append(1 + rnd.nextInt(1000));
            sb.append(')');
        }
        execute(sb.toString());
    }

    private void createRandomTimestampTable() throws Exception {
        execute("CREATE TABLE t (symbol SYMBOL, quantity DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        int rows = 10 + rnd.nextInt(41); // 10-50 rows
        StringBuilder sb = new StringBuilder("INSERT INTO t VALUES ");
        long ts = 1_704_067_200_000_000L; // 2024-01-01T00:00:00.000000Z in micros
        for (int r = 0; r < rows; r++) {
            if (r > 0) {
                sb.append(',');
            }
            sb.append("('");
            sb.append(SYMBOLS[rnd.nextInt(3)]); // BTC, ETH, SOL only
            sb.append("', ");
            sb.append(1 + rnd.nextInt(1000));
            sb.append(", ");
            sb.append(ts);
            sb.append(')');
            // Advance 1-3600 seconds to spread across multiple hours
            ts += (1 + rnd.nextInt(3600)) * 1_000_000L;
        }
        execute(sb.toString());
    }

    private void createRandomTimestampTableTwoKeys() throws Exception {
        execute("CREATE TABLE t (symbol SYMBOL, side SYMBOL, quantity DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        int rows = 10 + rnd.nextInt(41); // 10-50 rows
        StringBuilder sb = new StringBuilder("INSERT INTO t VALUES ");
        long ts = 1_704_067_200_000_000L; // 2024-01-01T00:00:00.000000Z in micros
        for (int r = 0; r < rows; r++) {
            if (r > 0) {
                sb.append(',');
            }
            sb.append("('");
            sb.append(SYMBOLS[rnd.nextInt(3)]); // BTC, ETH, SOL
            sb.append("', '");
            sb.append(SIDES[rnd.nextInt(SIDES.length)]);
            sb.append("', ");
            sb.append(1 + rnd.nextInt(1000));
            sb.append(", ");
            sb.append(ts);
            sb.append(')');
            ts += (1 + rnd.nextInt(3600)) * 1_000_000L;
        }
        execute(sb.toString());
    }

    private String randomAggregate(String col) {
        String agg = AGGREGATES[rnd.nextInt(AGGREGATES.length)];
        if ("COUNT".equals(agg)) {
            return "COUNT()";
        }
        return agg + "(" + col + ")";
    }
}
