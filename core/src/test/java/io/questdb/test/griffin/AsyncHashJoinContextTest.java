/*+*****************************************************************************
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

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.test.griffin.ParallelHashJoinTestUtils.*;

/**
 * The parallel hash join in the SQL contexts around it: as a sub-query and a CTE, under ORDER BY,
 * LIMIT and negative LIMIT, under GROUP BY and window functions, inside UNION, EXCEPT and
 * INTERSECT, on either side of another join, and as the master of ASOF, LT and SPLICE joins. Every
 * query compares with the ordinary plan of the same SQL, in order, and runs the operator.
 */
public class AsyncHashJoinContextTest extends AbstractCairoTest {

    @Override
    @Before
    public void setUp() {
        super.setUp();
        sqlExecutionContext.setParallelHashJoinEnabled(true);
    }

    @After
    public void restorePageFrameSizes() {
        sqlExecutionContext.restoreToDefaultPageFrameSizes();
    }

    @Test
    public void testAsOfAndLtJoinsOverTheJoin() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE TABLE q AS (SELECT ('s' || (x % 4))::SYMBOL sym, x qv, timestamp_sequence('2024-01-01', 450_000_000) qts FROM long_sequence(40)) TIMESTAMP(qts) PARTITION BY DAY");
            sqlExecutionContext.changePageFrameSizes(3, 3);
            for (String sql : new String[]{
                    "SELECT t.ts, t.k, d.w, q.qv FROM t LEFT JOIN d ON t.k = d.k ASOF JOIN q ON (sym)",
                    "SELECT t.ts, t.k, d.w, q.qv FROM t LEFT JOIN d ON t.k = d.k ASOF JOIN q ON (sym) TOLERANCE 30m",
                    "SELECT t.ts, t.k, d.w, q.qv FROM t JOIN d ON t.k = d.k LT JOIN q WHERE t.ts IN '2024-01-01T02'",
                    "SELECT t.ts, t.k, d.w, q.qv FROM t LEFT JOIN d ON t.k = d.k AND d.w > t.v % 5 ASOF JOIN q",
                    // The ASOF join's master is a sub-query over the operator.
                    "SELECT j.ts, j.k, j.w, q.qv FROM (SELECT t.ts, t.k, t.sym, d.w FROM t LEFT JOIN d ON t.k = d.k) j ASOF JOIN q ON (sym)",
            }) {
                final String plan = assertParallelHashJoin(sql, sqlExecutionContext);
                Assert.assertTrue(plan, plan.contains("AsOf Join") || plan.contains("Lt Join"));
                // The time-series join on top fixes the join's order, so no swap is possible.
                Assert.assertFalse(plan, plan.contains("inputSwap"));
            }
            // A SPLICE join needs a master with random access, which neither plan's join offers.
            final String splice = "SELECT t.ts, t.k, d.w, q.qv FROM t LEFT JOIN d ON t.k = d.k SPLICE JOIN q";
            assertExceptionNoLeakCheck(splice, splice.indexOf("SPLICE"), "left side of splice join doesn't support random access");
            sqlExecutionContext.setParallelHashJoinEnabled(false);
            assertExceptionNoLeakCheck(splice, splice.indexOf("SPLICE"), "left side of splice join doesn't support random access");
            sqlExecutionContext.setParallelHashJoinEnabled(true);
        });
    }

    @Test
    public void testGroupByOverTheJoin() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            sqlExecutionContext.changePageFrameSizes(3, 3);
            // A single join under a GROUP BY that the fused hash join GROUP BY takes stays fused.
            final String fused = "SELECT t.sym, sum(d.w) w, count() n FROM t JOIN d ON t.k = d.k ORDER BY t.sym";
            try (RecordCursorFactory factory = select(fused)) {
                final String plan = plan(factory, sqlExecutionContext);
                TestUtils.assertContains(plan, "Async Hash Join Group By");
                Assert.assertFalse(plan, plan.contains("Hash Join workers:"));
            }
            // With the fused plan off, or an aggregate that it does not take, the GROUP BY runs over
            // the operator.
            sqlExecutionContext.setParallelHashJoinGroupByEnabled(false);
            try {
                assertParallelHashJoin(fused, sqlExecutionContext);
                assertParallelHashJoin("SELECT t.sym, d.s, sum(d.w) w FROM t LEFT JOIN d ON t.k = d.k ORDER BY t.sym, d.s", sqlExecutionContext);
            } finally {
                sqlExecutionContext.setParallelHashJoinGroupByEnabled(true);
            }
            assertParallelHashJoin("SELECT t.sym, first(d.w) fw, last(d.s) ls FROM t LEFT JOIN d ON t.k = d.k ORDER BY t.sym", sqlExecutionContext);
            // SAMPLE BY over the join keeps the left input's designated timestamp.
            sqlExecutionContext.setParallelHashJoinGroupByEnabled(false);
            try {
                assertParallelHashJoin("SELECT t.ts, count() n, sum(d.w) w FROM t LEFT JOIN d ON t.k = d.k SAMPLE BY 1h", sqlExecutionContext);
            } finally {
                sqlExecutionContext.setParallelHashJoinGroupByEnabled(true);
            }
        });
    }

    @Test
    public void testJoinOnEitherSideOfAnotherJoin() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE TABLE e AS (SELECT (x % 6)::INT k, x ev, ('s' || (x % 3))::SYMBOL es FROM long_sequence(9))");
            sqlExecutionContext.changePageFrameSizes(3, 3);
            for (String sql : new String[]{
                    // The operator as the master of an ordinary hash join, INNER and LEFT.
                    "SELECT t.k, t.v, d.w, e.ev FROM t LEFT JOIN d ON t.k = d.k JOIN e ON t.k = e.k WHERE t.ts IN '2024-01-01'",
                    "SELECT t.k, t.v, d.w, e.ev FROM t LEFT JOIN d ON t.k = d.k LEFT JOIN e ON d.k = e.k",
                    // The operator inside the slave of an ordinary join and of a cross join.
                    "SELECT e.k, e.ev, j.v, j.w FROM e LEFT JOIN (SELECT t.k, t.v, d.w FROM t LEFT JOIN d ON t.k = d.k) j ON e.k = j.k",
                    "SELECT e.k, j.v, j.w FROM e CROSS JOIN (SELECT t.v, d.w FROM t JOIN d ON t.k = d.k WHERE t.ts IN '2024-01-01T01') j",
            }) {
                assertParallelHashJoin(sql, sqlExecutionContext);
            }
        });
    }

    @Test
    public void testOrderByAndLimits() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            sqlExecutionContext.changePageFrameSizes(3, 3);
            final String join = "SELECT t.k, t.v, t.ts, d.w, d.s FROM t LEFT JOIN d ON t.k = d.k";
            for (String tail : new String[]{
                    " ORDER BY d.w, t.v",
                    " ORDER BY d.s DESC, t.ts",
                    " ORDER BY t.ts DESC",
                    " ORDER BY t.ts DESC LIMIT 5",
                    " ORDER BY d.w DESC, t.v LIMIT 4",
                    " ORDER BY d.w, t.v LIMIT -6",
                    " LIMIT 7",
                    " LIMIT -7",
                    " LIMIT 3, 9",
                    " LIMIT -9, -2",
            }) {
                assertParallelHashJoin(join + tail, sqlExecutionContext);
            }
        });
    }

    @Test
    public void testSetOperations() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            sqlExecutionContext.changePageFrameSizes(3, 3);
            final String left = "SELECT t.k, d.w FROM t LEFT JOIN d ON t.k = d.k";
            final String inner = "SELECT t.k, d.w FROM t JOIN d ON t.k = d.k WHERE t.ts IN '2024-01-01T01'";
            for (String sql : new String[]{
                    left + " UNION ALL " + inner,
                    left + " UNION " + inner,
                    left + " EXCEPT " + inner,
                    left + " INTERSECT " + inner,
                    "SELECT k, w FROM d UNION ALL " + left,
                    // An ORDER BY makes UNION's and the set operations' row order independent of the
                    // hash sets they keep.
                    "(" + left + " UNION " + inner + ") ORDER BY k, w",
                    "(" + left + " INTERSECT ALL " + inner + ") ORDER BY k, w",
            }) {
                assertParallelHashJoin(sql, sqlExecutionContext);
            }
        });
    }

    @Test
    public void testSubQueryAndCte() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            sqlExecutionContext.changePageFrameSizes(3, 3);
            for (String sql : new String[]{
                    "SELECT * FROM (SELECT t.k, t.v, d.w FROM t LEFT JOIN d ON t.k = d.k) WHERE w > 3 OR w IS NULL",
                    "SELECT k, v + w s FROM (SELECT t.k, t.v, d.w FROM t JOIN d ON t.k = d.k WHERE t.ts IN '2024-01-01')",
                    "WITH j AS (SELECT t.k, t.v, d.w, d.s FROM t LEFT JOIN d ON t.k = d.k AND d.w > t.v % 4) SELECT k, w, s FROM j WHERE v % 2 = 0",
                    "WITH j AS (SELECT t.k, t.v, d.w FROM t JOIN d ON t.k = d.k) SELECT j.k, j.w FROM j WHERE j.v > 10",
                    // A scalar sub-query over the join, which the outer query compares with.
                    "SELECT t.k, t.v FROM t WHERE t.v > (SELECT d.w FROM t JOIN d ON t.k = d.k WHERE t.ts IN '2024-01-01T00' ORDER BY d.w DESC LIMIT 1)",
                    "SELECT t.k, d.w FROM t LEFT JOIN d ON t.k = d.k WHERE t.v > (SELECT d.w FROM t JOIN d ON t.k = d.k WHERE t.ts IN '2024-01-01T00' ORDER BY d.w DESC LIMIT 1)",
            }) {
                assertParallelHashJoin(sql, sqlExecutionContext);
            }
        });
    }

    @Test
    public void testWindowFunctionsOverTheJoin() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            sqlExecutionContext.changePageFrameSizes(3, 3);
            for (String sql : new String[]{
                    "SELECT t.k, t.ts, d.w, row_number() OVER (PARTITION BY t.k ORDER BY t.ts) rn FROM t LEFT JOIN d ON t.k = d.k",
                    "SELECT t.k, t.ts, d.w, sum(d.w) OVER (ORDER BY t.ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) s FROM t JOIN d ON t.k = d.k WHERE t.ts IN '2024-01-01'",
                    "SELECT t.k, d.w, lag(d.w) OVER (PARTITION BY d.s ORDER BY t.ts) prev FROM t LEFT JOIN d ON t.k = d.k WHERE d.w > t.v % 3 OR d.w IS NULL",
                    // A window over the whole result reads the join's rows twice, with toTop() between.
                    "SELECT t.k, d.w, count(*) OVER () n FROM t LEFT JOIN d ON t.k = d.k ORDER BY t.ts DESC",
                    "SELECT t.k, d.w, avg(d.w) OVER (PARTITION BY t.k) a FROM t LEFT JOIN d ON t.k = d.k",
            }) {
                assertParallelHashJoin(sql, sqlExecutionContext);
            }
        });
    }

    private static void createTables() throws Exception {
        execute("""
                CREATE TABLE t AS (
                    SELECT CASE WHEN x % 11 = 0 THEN NULL ELSE (x % 6)::INT END k, x v, ('s' || (x % 4))::SYMBOL sym,
                        timestamp_sequence('2024-01-01', 600_000_000) ts
                    FROM long_sequence(36)
                ) TIMESTAMP(ts) PARTITION BY HOUR
                """);
        execute("""
                CREATE TABLE d AS (
                    SELECT CASE WHEN x % 7 = 0 THEN NULL ELSE (x % 5)::INT END k, x w, ('d' || (x % 3))::SYMBOL s
                    FROM long_sequence(15)
                )
                """);
    }
}
