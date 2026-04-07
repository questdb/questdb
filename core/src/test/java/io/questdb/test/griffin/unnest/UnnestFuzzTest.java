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

package io.questdb.test.griffin.unnest;

import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

/**
 * Fuzz tests for UNNEST. Verifies correctness through oracle comparisons
 * (array_sum vs UNNEST SUM) and row count invariants.
 */
public class UnnestFuzzTest extends AbstractCairoTest {
    private static final int ITERATIONS = 100;
    private Rnd rnd;

    @Override
    @Before
    public void setUp() {
        rnd = TestUtils.generateRandom(LOG);
        super.setUp();
    }

    @Test
    public void testRowCountEquivalence() throws Exception {
        // For any UNNEST query without WHERE, total row count must equal
        // the sum of array lengths across base rows. NULL/empty arrays
        // contribute zero rows.
        assertMemoryLeak(() -> {
            for (int i = 0; i < ITERATIONS; i++) {
                int rows = rnd.nextInt(20) + 1;
                int maxLen = rnd.nextInt(50) + 1;
                execute("DROP TABLE IF EXISTS t");
                execute("CREATE TABLE t AS ("
                        + "SELECT rnd_double_array(1, 0, 0, " + maxLen + ") arr "
                        + "FROM long_sequence(" + rows + ")"
                        + ")");

                printSql("SELECT count() cnt FROM t, UNNEST(t.arr) u(val)");
                String viaUnnest = sink.toString();

                printSql("SELECT sum(array_count(arr)) cnt FROM t");
                String viaArrayCount = sink.toString();

                TestUtils.assertEquals(
                        "row count iteration=" + i + " rows=" + rows + " maxLen=" + maxLen,
                        viaArrayCount,
                        viaUnnest
                );
            }
        });
    }

    @Test
    public void testRowCountMultipleArrays() throws Exception {
        // With multiple arrays, row count = sum of max(len_a, len_b) per base row.
        // We verify this by comparing count() from UNNEST with a manually computed
        // maximum via array_count on each column.
        assertMemoryLeak(() -> {
            for (int i = 0; i < ITERATIONS; i++) {
                int rows = rnd.nextInt(10) + 1;
                int maxLenA = rnd.nextInt(30) + 1;
                int maxLenB = rnd.nextInt(30) + 1;
                execute("DROP TABLE IF EXISTS t");
                execute("CREATE TABLE t AS ("
                        + "SELECT rnd_double_array(1, 0, 0, " + maxLenA + ") a, "
                        + "rnd_double_array(1, 0, 0, " + maxLenB + ") b "
                        + "FROM long_sequence(" + rows + ")"
                        + ")");

                printSql("SELECT count() cnt FROM t, UNNEST(t.a, t.b) u(x, y)");
                String viaUnnest = sink.toString();

                // max(array_count(a), array_count(b)) per row, then sum
                printSql("SELECT sum("
                        + "CASE WHEN array_count(a) > array_count(b) "
                        + "THEN array_count(a) ELSE array_count(b) END"
                        + ") cnt FROM t");
                String viaMax = sink.toString();

                TestUtils.assertEquals(
                        "multi-array row count iteration=" + i,
                        viaMax,
                        viaUnnest
                );
            }
        });
    }

    @Test
    public void testSumEquivalence() throws Exception {
        // SUM(u.val) via UNNEST must equal SUM(array_sum(arr)) for the same data.
        // This is the primary correctness oracle: if UNNEST correctly expands
        // all array elements, their sum must match the array-level sum.
        assertMemoryLeak(() -> {
            for (int i = 0; i < ITERATIONS; i++) {
                int rows = rnd.nextInt(20) + 1;
                int maxLen = rnd.nextInt(50) + 1;
                execute("DROP TABLE IF EXISTS t");
                execute("CREATE TABLE t AS ("
                        + "SELECT rnd_symbol('A','B','C') sym, "
                        + "rnd_double_array(1, 0, 0, " + maxLen + ") arr "
                        + "FROM long_sequence(" + rows + ")"
                        + ")");

                printSql("SELECT sym, round(sum(array_sum(arr)), 4) s "
                        + "FROM t GROUP BY sym ORDER BY sym");
                String viaArraySum = sink.toString();

                printSql("SELECT sym, round(sum(val), 4) s "
                        + "FROM t, UNNEST(t.arr) u(val) GROUP BY sym ORDER BY sym");
                String viaUnnest = sink.toString();

                TestUtils.assertEquals(
                        "sum equivalence iteration=" + i + " rows=" + rows + " maxLen=" + maxLen,
                        viaArraySum,
                        viaUnnest
                );
            }
        });
    }

    @Test
    public void testSumEquivalenceWithOrdinality() throws Exception {
        // Adding WITH ORDINALITY must not change the sum of values.
        assertMemoryLeak(() -> {
            for (int i = 0; i < ITERATIONS; i++) {
                int rows = rnd.nextInt(10) + 1;
                int maxLen = rnd.nextInt(30) + 1;
                execute("DROP TABLE IF EXISTS t");
                execute("CREATE TABLE t AS ("
                        + "SELECT rnd_double_array(1, 0, 0, " + maxLen + ") arr "
                        + "FROM long_sequence(" + rows + ")"
                        + ")");

                printSql("SELECT round(sum(val), 4) s "
                        + "FROM t, UNNEST(t.arr) u(val)");
                String withoutOrd = sink.toString();

                printSql("SELECT round(sum(val), 4) s "
                        + "FROM t, UNNEST(t.arr) WITH ORDINALITY u(val, ord)");
                String withOrd = sink.toString();

                TestUtils.assertEquals(
                        "ordinality sum iteration=" + i,
                        withoutOrd,
                        withOrd
                );
            }
        });
    }

    @Test
    public void testUnnestInCTEMatchesTopLevel() throws Exception {
        // UNNEST inside a CTE must produce the same results as UNNEST at top level.
        assertMemoryLeak(() -> {
            for (int i = 0; i < ITERATIONS; i++) {
                int rows = rnd.nextInt(10) + 1;
                int maxLen = rnd.nextInt(20) + 1;
                execute("DROP TABLE IF EXISTS t");
                execute("CREATE TABLE t AS ("
                        + "SELECT rnd_double_array(1, 0, 0, " + maxLen + ") arr "
                        + "FROM long_sequence(" + rows + ")"
                        + ")");

                printSql("SELECT round(sum(val), 4) s "
                        + "FROM t, UNNEST(t.arr) u(val)");
                String topLevel = sink.toString();

                printSql("WITH cte AS ("
                        + "SELECT u.val FROM t, UNNEST(t.arr) u(val)"
                        + ") SELECT round(sum(val), 4) s FROM cte");
                String viaCte = sink.toString();

                TestUtils.assertEquals(
                        "CTE equivalence iteration=" + i,
                        topLevel,
                        viaCte
                );
            }
        });
    }

    @Test
    public void testUnnestInSubqueryMatchesTopLevel() throws Exception {
        // UNNEST inside a subquery must produce the same results as top-level UNNEST.
        assertMemoryLeak(() -> {
            for (int i = 0; i < ITERATIONS; i++) {
                int rows = rnd.nextInt(10) + 1;
                int maxLen = rnd.nextInt(20) + 1;
                execute("DROP TABLE IF EXISTS t");
                execute("CREATE TABLE t AS ("
                        + "SELECT rnd_double_array(1, 0, 0, " + maxLen + ") arr "
                        + "FROM long_sequence(" + rows + ")"
                        + ")");

                printSql("SELECT round(sum(val), 4) s "
                        + "FROM t, UNNEST(t.arr) u(val)");
                String topLevel = sink.toString();

                printSql("SELECT round(sum(val), 4) s FROM ("
                        + "SELECT u.val FROM t, UNNEST(t.arr) u(val)"
                        + ")");
                String viaSubquery = sink.toString();

                TestUtils.assertEquals(
                        "subquery equivalence iteration=" + i,
                        topLevel,
                        viaSubquery
                );
            }
        });
    }
}
