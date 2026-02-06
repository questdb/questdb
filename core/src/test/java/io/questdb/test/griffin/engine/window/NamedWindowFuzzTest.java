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

package io.questdb.test.griffin.engine.window;

import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

/**
 * Fuzz tests for named window resolution. Verifies that queries using named
 * windows (WINDOW w AS (...)) produce identical results to equivalent queries
 * with inline window specifications.
 */
public class NamedWindowFuzzTest extends AbstractCairoTest {
    private static final String[] AGGREGATE_FUNCTIONS = {
            "sum(x)",
            "avg(x)",
            "count(x)",
            "count(*)",
            "max(x)",
            "min(x)",
            "row_number()",
            "rank()",
            "dense_rank()",
    };
    private static final int ITERATIONS = 200;
    private static final String[] ORDER_COLUMNS = {"ts", "x"};
    private static final String[] PARTITION_COLUMNS = {"category", "x"};
    private Rnd rnd;

    @Override
    @Before
    public void setUp() {
        rnd = TestUtils.generateRandom(LOG);
        super.setUp();
    }

    @Test
    public void testMultipleNamedWindows() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x int, category symbol, ts timestamp) timestamp(ts)");
            execute("insert into t select x, rnd_symbol('A','B','C'), timestamp_sequence(0, 1000000) from long_sequence(20)");

            for (int i = 0; i < ITERATIONS; i++) {
                String spec1 = randomWindowSpec();
                String spec2 = randomWindowSpec();
                String func1 = randomAggregateFunction();
                String func2 = randomAggregateFunction();

                // Use subqueries to avoid ORDER BY ambiguity with inline OVER clauses
                String namedQuery = "SELECT * FROM (" +
                        "SELECT x, " +
                        func1 + " OVER w1 as r1, " +
                        func2 + " OVER w2 as r2 " +
                        "FROM t " +
                        "WINDOW w1 AS (" + spec1 + "), w2 AS (" + spec2 + ")" +
                        ") ORDER BY x";

                String inlineQuery = "SELECT * FROM (" +
                        "SELECT x, " +
                        func1 + " OVER (" + spec1 + ") as r1, " +
                        func2 + " OVER (" + spec2 + ") as r2 " +
                        "FROM t" +
                        ") ORDER BY x";

                assertEquivalent(namedQuery, inlineQuery, i);
            }
        });
    }

    @Test
    public void testNamedWindowEquivalence() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x int, category symbol, ts timestamp) timestamp(ts)");
            execute("insert into t select x, rnd_symbol('A','B','C'), timestamp_sequence(0, 1000000) from long_sequence(20)");

            for (int i = 0; i < ITERATIONS; i++) {
                String spec = randomWindowSpec();
                String func = randomAggregateFunction();

                String namedQuery = "SELECT * FROM (" +
                        "SELECT x, " + func + " OVER w as r FROM t " +
                        "WINDOW w AS (" + spec + ")" +
                        ") ORDER BY x";

                String inlineQuery = "SELECT * FROM (" +
                        "SELECT x, " + func + " OVER (" + spec + ") as r FROM t" +
                        ") ORDER BY x";

                assertEquivalent(namedQuery, inlineQuery, i);
            }
        });
    }

    @Test
    public void testNamedWindowSharedByMultipleFunctions() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x int, category symbol, ts timestamp) timestamp(ts)");
            execute("insert into t select x, rnd_symbol('A','B','C'), timestamp_sequence(0, 1000000) from long_sequence(20)");

            for (int i = 0; i < ITERATIONS; i++) {
                String spec = randomWindowSpec();
                int funcCount = 2 + rnd.nextInt(3); // 2-4 functions sharing the same window
                StringBuilder namedCols = new StringBuilder();
                StringBuilder inlineCols = new StringBuilder();
                for (int f = 0; f < funcCount; f++) {
                    String func = randomAggregateFunction();
                    if (f > 0) {
                        namedCols.append(", ");
                        inlineCols.append(", ");
                    }
                    namedCols.append(func).append(" OVER w as r").append(f);
                    inlineCols.append(func).append(" OVER (").append(spec).append(") as r").append(f);
                }

                String namedQuery = "SELECT * FROM (" +
                        "SELECT x, " + namedCols +
                        " FROM t WINDOW w AS (" + spec + ")" +
                        ") ORDER BY x";

                String inlineQuery = "SELECT * FROM (" +
                        "SELECT x, " + inlineCols +
                        " FROM t" +
                        ") ORDER BY x";

                assertEquivalent(namedQuery, inlineQuery, i);
            }
        });
    }

    private void assertEquivalent(String namedQuery, String inlineQuery, int iteration) throws Exception {
        try {
            StringSink namedResult = new StringSink();
            printSql(namedQuery, namedResult);

            StringSink inlineResult = new StringSink();
            printSql(inlineQuery, inlineResult);

            TestUtils.assertEquals(inlineResult, namedResult);
        } catch (Exception e) {
            throw new AssertionError(
                    "Iteration " + iteration + " failed.\nNamed query: " + namedQuery + "\nInline query: " + inlineQuery,
                    e
            );
        }
    }

    private String randomAggregateFunction() {
        return AGGREGATE_FUNCTIONS[rnd.nextInt(AGGREGATE_FUNCTIONS.length)];
    }

    private String randomWindowSpec() {
        StringBuilder sb = new StringBuilder();

        // PARTITION BY (50% chance)
        if (rnd.nextInt(2) == 0) {
            sb.append("PARTITION BY ");
            sb.append(PARTITION_COLUMNS[rnd.nextInt(PARTITION_COLUMNS.length)]);
        }

        // ORDER BY (always included - required for rank/dense_rank/row_number)
        if (!sb.isEmpty()) {
            sb.append(' ');
        }
        sb.append("ORDER BY ");
        sb.append(ORDER_COLUMNS[rnd.nextInt(ORDER_COLUMNS.length)]);
        if (rnd.nextInt(3) == 0) {
            sb.append(" DESC");
        }

        // ROWS frame (30% chance, only safe variants)
        if (rnd.nextInt(3) == 0) {
            sb.append(" ROWS BETWEEN ");
            if (rnd.nextBoolean()) {
                sb.append("UNBOUNDED PRECEDING AND CURRENT ROW");
            } else {
                sb.append(1 + rnd.nextInt(5)).append(" PRECEDING AND CURRENT ROW");
            }
        }

        return sb.toString();
    }
}
