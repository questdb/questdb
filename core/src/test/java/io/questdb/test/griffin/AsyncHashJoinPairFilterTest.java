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

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.table.AsyncHashJoinRecordCursorFactory;
import io.questdb.griffin.engine.table.RuntimeConstGateRecordCursorFactory;
import io.questdb.std.Unsafe;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.test.griffin.ParallelHashJoinTestUtils.*;

/**
 * The parallel hash join with filters over its pairs: a LEFT join's ON residual, the part of its ON
 * condition beyond the keys, and the post-join WHERE conjuncts that the join's rows make resolvable,
 * which the planner absorbs into the join. Both run on the workers, for each pair of a probe row and
 * one of its matches, and the rows, their order, types and symbol tables match the ordinary plan of
 * the same SQL, whose filters run in its join and in a filter over the join. A filter that the
 * workers cannot run, and a post-join filter whose value is a runtime constant, keep the ordinary
 * plan's paths.
 */
public class AsyncHashJoinPairFilterTest extends AbstractCairoTest {
    // ON residuals of t LEFT JOIN d ON t.k = d.k, each beyond the key.
    private static final String[] RESIDUALS = {
            // No pair passes, some do, and every pair does.
            "d.w < 0",
            "d.w > t.v",
            "d.w % 3 = 0",
            "d.w > 0",
            // SYMBOL columns of both inputs, whose dictionaries differ, and SYMBOL constants of each.
            "d.s <> t.sym",
            "d.s = 's2'",
            "t.sym IN ('s1', 's3') AND d.w % 2 = 0",
            // NULLs on both sides, and a residual that reads the probe row alone.
            "d.vch IS NULL OR t.str IS NULL",
            "t.v % 4 = 0",
            // A regex, which is not thread safe, so that each worker runs a copy of its own.
            "d.vch ~ 'v[12]'",
    };
    // Post-join filters of t JOIN d ON t.k = d.k, each of which reads both inputs.
    private static final String[] INNER_POST_FILTERS = {
            "d.w < t.v",
            "d.w > t.v",
            "d.w + t.v > 1_000",
            "d.s = 's2' OR t.v < 10",
            "d.s <> t.sym",
            "d.vch ~ 'v[13]' OR t.str ~ 'str[24]'",
    };
    // Post-join filters of t LEFT JOIN d ON t.k = d.k.
    private static final String[] POST_FILTERS = {
            // The anti-join, which keeps the probe rows without a match.
            "d.w IS NULL",
            "d.w IS NOT NULL AND d.w < t.v",
            "coalesce(d.w, 0) + t.v > 40",
            "d.s = 's2' OR t.v < 10",
            "d.s = t.sym OR d.w IS NULL",
            "d.s <> t.sym AND t.v % 3 <> 0",
            "d.vch ~ 'v[13]' OR t.str ~ 'str[24]'",
    };

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
    public void testAntiJoinOverLeftMisses() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // The rows of t whose key d lacks: keys 5 and 6, since d's keys run from 0 to 4. A NULL key
            // of t matches d's NULL keys, as in the ordinary join, so it is no miss.
            final String sql = "SELECT t.k, t.v, d.w FROM t LEFT JOIN d ON t.k = d.k WHERE d.w IS NULL AND t.v < 30";
            assertQuery(sql)
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            k\tv\tw
                            5\t5\tnull
                            6\t6\tnull
                            5\t12\tnull
                            6\t13\tnull
                            5\t19\tnull
                            6\t20\tnull
                            5\t26\tnull
                            """);
            TestUtils.assertContains(assertParallelHashJoin(sql, sqlExecutionContext), "postJoinFilter: d.w=null");
            // A residual that every pair fails turns every row into a miss, which the anti-join keeps.
            assertParallelHashJoin("SELECT t.k, t.v, d.w FROM t LEFT JOIN d ON t.k = d.k AND d.w < 0 WHERE d.w IS NULL", sqlExecutionContext);
        });
    }

    @Test
    public void testHandComputedResiduals() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT x::INT k, x * 10 v, timestamp_sequence('2024-01-01', 60_000_000) ts FROM long_sequence(4)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("CREATE TABLE d (k INT, s STRING, w LONG)");
            execute("INSERT INTO d VALUES (1, 'one', 5), (2, 'two-a', 25), (2, 'two-b', 5), (2, 'two-c', 30), (3, 'three', 1)");
            // Some pairs fail: key 2's matches come newest first, and the one that fails drops out; key
            // 3's only match fails, so its row reads NULLs, as key 4's, which has no match.
            assertQuery("SELECT t.k, d.s, d.w FROM t LEFT JOIN d ON t.k = d.k AND d.w > t.v - 10")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            k\ts\tw
                            1\tone\t5
                            2\ttwo-c\t30
                            2\ttwo-a\t25
                            3\t\tnull
                            4\t\tnull
                            """);
            // Every pair fails: one row with NULLs per probe row.
            assertQuery("SELECT t.k, d.s, d.w FROM t LEFT JOIN d ON t.k = d.k AND d.w > 100")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            k\ts\tw
                            1\t\tnull
                            2\t\tnull
                            3\t\tnull
                            4\t\tnull
                            """);
            // A post-join filter over a residual applies to the rows the residual leaves, NULL rows
            // included.
            assertQuery("SELECT t.k, d.s, d.w FROM t LEFT JOIN d ON t.k = d.k AND d.w > t.v - 10 WHERE d.w IS NULL OR d.w > 26")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            k\ts\tw
                            2\ttwo-c\t30
                            3\t\tnull
                            4\t\tnull
                            """);
            for (String sql : new String[]{
                    "SELECT t.k, d.s, d.w FROM t LEFT JOIN d ON t.k = d.k AND d.w > t.v - 10",
                    "SELECT t.k, d.s, d.w FROM t LEFT JOIN d ON t.k = d.k AND d.w > 100",
                    "SELECT t.k, d.s, d.w FROM t LEFT JOIN d ON t.k = d.k AND d.w > t.v - 10 WHERE d.w IS NULL OR d.w > 26",
            }) {
                assertParallelHashJoin(sql, sqlExecutionContext);
            }
        });
    }

    @Test
    public void testPostFilters() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            for (int frameRows : new int[]{1, 3, 1_000_000}) {
                sqlExecutionContext.changePageFrameSizes(frameRows, frameRows);
                for (String filter : POST_FILTERS) {
                    assertPairFilter("SELECT t.k, t.v, t.sym, t.str, d.w, d.s, d.vch FROM t LEFT JOIN d ON t.k = d.k WHERE " + filter);
                    // A unique build, whose lookups position the payload themselves.
                    assertPairFilter("SELECT t.k, t.v, t.sym, t.str, d.w, d.s, d.vch FROM t LEFT JOIN u d ON t.k = d.k WHERE " + filter);
                }
                // An INNER join of a filtered probe, whose ordinary plan does not swap, with filters that
                // read both inputs: the optimiser pushes a filter of the build alone into the build.
                for (String filter : INNER_POST_FILTERS) {
                    assertPairFilter("SELECT t.k, t.v, t.sym, d.w, d.s FROM t JOIN d ON t.k = d.k WHERE t.v > 2 AND (" + filter + ")");
                    assertPairFilter("SELECT t.k, t.v, t.sym, d.w, d.s FROM t JOIN u d ON t.k = d.k WHERE t.v > 2 AND (" + filter + ")");
                }
                // A post-join filter under the ON residual of the same join.
                assertPairFilter("SELECT t.k, t.v, d.w FROM t LEFT JOIN d ON t.k = d.k AND d.w % 2 = 0 WHERE d.w IS NULL OR t.v > 30");
            }
        });
    }

    @Test
    public void testProjectionsUnderFilters() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            sqlExecutionContext.changePageFrameSizes(3, 3);
            // Projections name the scans' columns in another order, some twice, so the filters' columns
            // map onto other frame columns, on both sides, the SYMBOL ones' symbol tables included.
            for (String sql : new String[]{
                    "SELECT t2.*, d2.* FROM (SELECT v, sym, k, ts, sym sym2 FROM t) t2 LEFT JOIN (SELECT vch, s, w, k FROM d) d2 ON t2.k = d2.k AND d2.s <> t2.sym2",
                    "SELECT t2.*, d2.* FROM (SELECT v, sym, k, ts FROM t WHERE v > 5) t2 LEFT JOIN (SELECT vch, s, w, k FROM d WHERE w > 3) d2 ON t2.k = d2.k WHERE d2.w IS NULL OR d2.s = t2.sym OR d2.vch ~ 'v[12]'",
                    "SELECT t2.*, d2.* FROM (SELECT v, sym, k, ts FROM t WHERE v > 5) t2 JOIN (SELECT vch, s, w, k FROM d) d2 ON t2.k = d2.k WHERE d2.s <> t2.sym AND d2.w < t2.v",
            }) {
                assertPairFilter(sql);
            }
        });
    }

    @Test
    public void testResiduals() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            for (int frameRows : new int[]{1, 3, 1_000_000}) {
                sqlExecutionContext.changePageFrameSizes(frameRows, frameRows);
                for (String residual : RESIDUALS) {
                    final String on = " AND (" + residual + ")";
                    assertPairFilter("SELECT t.k, t.v, t.sym, t.str, d.w, d.s, d.vch FROM t LEFT JOIN d ON t.k = d.k" + on);
                    // A unique build, a SYMBOL key and a staged key.
                    assertPairFilter("SELECT t.k, t.v, t.sym, d.w, d.s, d.vch FROM t LEFT JOIN u d ON t.k = d.k" + on);
                    assertPairFilter("SELECT t.k, t.v, t.sym, d.w, d.s FROM t LEFT JOIN d ON t.sym = d.s" + on);
                    assertPairFilter("SELECT t.k, t.v, t.sym, d.w, d.s FROM t LEFT JOIN d ON t.k = d.k AND t.sym = d.s" + on);
                    // A backward probe scan.
                    assertPairFilter("SELECT t.k, t.v, t.ts, d.w, d.s FROM t LEFT JOIN d ON t.k = d.k" + on + " ORDER BY t.ts DESC");
                }
            }
        });
    }

    @Test
    public void testThrowingResidualAndPostFilter() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // p * 1000 does not fit DECIMAL(4,0) for the one row with p = 10, whose key d holds, so a
            // filter that casts it fails on that row's pairs, in a frame between frames that pass.
            execute("CREATE TABLE tp AS (SELECT (x % 7)::INT k, CASE WHEN x = 46 THEN 10 ELSE x % 10 END p, timestamp_sequence('2024-01-01', 600_000_000) ts FROM long_sequence(60)) TIMESTAMP(ts) PARTITION BY HOUR");
            final String poison = "(tp.p * 1000)::DECIMAL(4,0) > d.w::DECIMAL(4,0)";
            final String[] queries = {
                    "SELECT tp.k, tp.p, d.w FROM tp LEFT JOIN d ON tp.k = d.k AND " + poison,
                    "SELECT tp.k, tp.p, d.w FROM tp LEFT JOIN d ON tp.k = d.k WHERE " + poison,
                    "SELECT tp.k, tp.p, d.w FROM tp JOIN d ON tp.k = d.k WHERE tp.p >= 0 AND " + poison,
                    "SELECT tp.k, tp.p, d.w FROM tp LEFT JOIN u d ON tp.k = d.k AND " + poison,
            };
            sqlExecutionContext.changePageFrameSizes(3, 3);
            for (String sql : queries) {
                final String expected = error(sql, false, sqlExecutionContext);
                TestUtils.assertContains(expected, "inconvertible value: 10000 [LONG -> DECIMAL(4,0)]");
                Assert.assertEquals(sql, expected, error(sql, true, sqlExecutionContext));
            }
            // The tables still work, and so does the join with a filter that passes the row.
            final String safe = "(tp.p * 100)::DECIMAL(4,0) > d.w::DECIMAL(4,0)";
            assertParallelHashJoin("SELECT tp.k, tp.p, d.w FROM tp LEFT JOIN d ON tp.k = d.k AND " + safe, sqlExecutionContext);
            assertParallelHashJoin("SELECT tp.k, tp.p, d.w FROM tp LEFT JOIN d ON tp.k = d.k WHERE " + safe, sqlExecutionContext);
        });
    }

    @Test
    public void testRuntimeConstantAndUnsafeFiltersKeepTheirPaths() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE TABLE flag AS (SELECT true b FROM long_sequence(1))");
            // A whole post-join filter whose value is a runtime constant gates the join's output.
            final String gated = "SELECT t.k, d.w FROM t LEFT JOIN d ON t.k = d.k WHERE (SELECT b FROM flag LIMIT 1)";
            final String gatedPlan = assertParallelHashJoin(gated, sqlExecutionContext);
            Assert.assertFalse(gatedPlan, gatedPlan.contains("postJoinFilter"));
            try (RecordCursorFactory factory = select(gated)) {
                Assert.assertTrue(hasFactory(factory, RuntimeConstGateRecordCursorFactory.class));
            }
            // A post-join filter whose value may change within an execution runs over the join's rows,
            // in a filter of its own.
            final String unstable = "SELECT t.k, d.w FROM t LEFT JOIN d ON t.k = d.k WHERE d.w > 2 OR systimestamp() < t.ts";
            final String unstablePlan = assertParallelHashJoin(unstable, sqlExecutionContext);
            Assert.assertFalse(unstablePlan, unstablePlan.contains("postJoinFilter"));
            TestUtils.assertContains(unstablePlan, "Filter filter: ");
            // An ON residual whose value may change within an execution keeps the ordinary plan.
            assertOrdinaryPlan("SELECT t.k, d.w FROM t LEFT JOIN d ON t.k = d.k AND (d.w > 2 OR systimestamp() < t.ts)", sqlExecutionContext);
            // So do filters that cannot run in parallel, as a cast to SYMBOL, which makes a dictionary
            // of its own as it runs.
            assertOrdinaryPlan("SELECT t.k, d.w FROM t LEFT JOIN d ON t.k = d.k AND d.w::SYMBOL <> '3'", sqlExecutionContext);
            final String serialPlan = assertParallelHashJoin("SELECT t.k, d.w FROM t LEFT JOIN d ON t.k = d.k WHERE d.w::SYMBOL <> '3'", sqlExecutionContext);
            Assert.assertFalse(serialPlan, serialPlan.contains("postJoinFilter"));
            TestUtils.assertContains(serialPlan, "Filter filter: d.w::symbol!='3'");
        });
    }

    @Test
    public void testCompileUnderMemoryLimitLeaksNothing() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // The residual and the post-join filter are regexes, which have worker copies, and the
            // join is constructed twice, the second time with the post-join filter. A native memory
            // limit just above what the process holds fails the compile at each of its allocations in
            // turn; every failure releases what the compile took, as assertMemoryLeak checks, and the
            // first limit that admits the compile plans the operator.
            final String sql = "SELECT t.k, t.v, d.w FROM t LEFT JOIN d ON t.k = d.k AND d.vch ~ 'v[1-3]' WHERE d.w IS NULL OR t.str ~ 'str[12]'";
            final String expected = ordinary(sql, sqlExecutionContext);
            int failures = 0;
            for (long extra = 0; ; extra += 64) {
                RecordCursorFactory factory = null;
                try {
                    Unsafe.setRssMemLimit(Unsafe.getRssMemUsed() + extra);
                    factory = select(sql);
                } catch (CairoException e) {
                    Assert.assertTrue(e.getMessage(), e.isOutOfMemory());
                    failures++;
                    continue;
                } finally {
                    Unsafe.setRssMemLimit(0);
                }
                try {
                    TestUtils.assertContains(plan(factory, sqlExecutionContext), "postJoinFilter: ");
                    Assert.assertEquals(expected, result(factory, sqlExecutionContext));
                } finally {
                    factory.close();
                }
                break;
            }
            Assert.assertTrue(failures > 0);
            assertParallelHashJoin(sql, sqlExecutionContext);
        });
    }

    @Test
    public void testConstructionFailureReleasesFilters() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // The residual and the post-join filter are regexes, which have worker copies. The first
            // construction fails, which leaves the residual to the compile to release, and the second,
            // which leaves it the post-join filter too, along with the spec the first one built.
            final String residual = "SELECT t.k, t.v, d.w FROM t LEFT JOIN d ON t.k = d.k AND d.vch ~ 'v[1-3]'";
            final String postFilter = residual + " WHERE d.w IS NULL OR t.str ~ 'str[12]'";
            for (String sql : new String[]{residual, postFilter}) {
                for (int constructions = 0; constructions < 2; constructions++) {
                    if (sql.equals(residual) && constructions > 0) {
                        // A join without a post-join filter is constructed once.
                        continue;
                    }
                    AsyncHashJoinRecordCursorFactory.setConstructorFailureForTesting(CairoException.nonCritical().put("injected construction failure"), constructions);
                    try {
                        assertExceptionNoLeakCheck(sql, 0, "injected construction failure");
                    } finally {
                        AsyncHashJoinRecordCursorFactory.setConstructorFailureForTesting(null);
                    }
                }
                // The tables still work.
                assertParallelHashJoin(sql, sqlExecutionContext);
            }
        });
    }

    @Test
    public void testOrderByOverPostFilteredJoin() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // The serial filter over a join answers followedOrderByAdvice() with false, without asking
            // the join, and the operator that absorbed it answers so too; a parent that asks then
            // plans its sort as over the serial filter, which keeps the scan direction of the join.
            final String[] orderBys = {"t.ts DESC", "t.ts", "d.w, t.v", "t.v DESC"};
            for (String orderBy : orderBys) {
                for (String sql : new String[]{
                        "SELECT t.k, t.v, t.ts, d.w FROM t LEFT JOIN d ON t.k = d.k WHERE d.w IS NULL OR d.w < t.v ORDER BY " + orderBy,
                        "SELECT t.k, t.v, t.ts, d.w FROM t LEFT JOIN d ON t.k = d.k AND d.w > 3 WHERE d.w IS NULL OR d.w < t.v ORDER BY " + orderBy,
                        "SELECT t.k, t.v, t.ts, d.w FROM t JOIN d ON t.k = d.k WHERE t.v > 3 AND d.w < t.v ORDER BY " + orderBy,
                }) {
                    final String plan = assertParallelHashJoin(sql, sqlExecutionContext);
                    final String ordinaryPlan = ordinaryPlan(sql, sqlExecutionContext);
                    Assert.assertEquals(sql + '\n' + plan + '\n' + ordinaryPlan, ordinaryPlan.contains("Sort"), plan.contains("Sort"));
                }
            }
            // The left input scans backward and follows the advice, which the join without a post-join
            // filter reports, and the join with one does not.
            try (RecordCursorFactory factory = select("SELECT t.k, t.ts, d.w FROM t LEFT JOIN d ON t.k = d.k ORDER BY t.ts DESC")) {
                Assert.assertTrue(find(factory).followedOrderByAdvice());
            }
            try (RecordCursorFactory factory = select("SELECT t.k, t.ts, d.w FROM t LEFT JOIN d ON t.k = d.k WHERE d.w IS NULL ORDER BY t.ts DESC")) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertFalse(join.followedOrderByAdvice());
                Assert.assertEquals(RecordCursorFactory.SCAN_DIRECTION_BACKWARD, join.getScanDirection());
            }
        });
    }

    @Test
    public void testPlanInsideScalarSubQuery() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // The join's plan prints inside an aggregate of the outer GROUP BY, which prints its
            // functions over its base metadata: the join prints its filters against the joined rows
            // and gives the plan sink back its metadata and mode, so the text that follows the
            // sub-query's plan reads as in the ordinary plan.
            sqlExecutionContext.setParallelHashJoinGroupByEnabled(false);
            final String sql = "SELECT t.k, sum(CASE WHEN t.v > (SELECT max(d.w) FROM t t2 LEFT JOIN d ON t2.k = d.k WHERE d.w IS NULL OR d.s <> t2.sym) THEN t.v ELSE 0 END) s FROM t ORDER BY 1";
            final String plan = planText(sql);
            TestUtils.assertContains(plan, "postJoinFilter: (d.w=null or d.s!=t2.sym)");
            final String ordinaryPlan = ordinaryPlan(sql, sqlExecutionContext);
            final String tail = ordinaryPlan.substring(ordinaryPlan.lastIndexOf("Frame forward scan on: d") + "Frame forward scan on: d".length());
            Assert.assertTrue(ordinaryPlan, tail.startsWith(",v,0]))]"));
            Assert.assertTrue(plan, plan.endsWith(tail));
            // So with an ON residual, whose ordinary plan cannot print in this place: its join resolves
            // the residual's columns through a base factory that it does not have.
            final String residualPlan = planText(
                    "SELECT t.k, sum(CASE WHEN t.v > (SELECT max(d.w) FROM t t2 LEFT JOIN d ON t2.k = d.k AND d.w > t2.v WHERE d.w IS NULL OR d.s <> t2.sym) THEN t.v ELSE 0 END) s FROM t ORDER BY 1"
            );
            TestUtils.assertContains(residualPlan, "onFilter: t2.v<d.w");
            TestUtils.assertContains(residualPlan, "postJoinFilter: (d.w=null or d.s!=t2.sym)");
            Assert.assertTrue(residualPlan, residualPlan.endsWith(tail));
        });
    }

    @Test
    public void testPlans() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            // The ON residual and the post-join filter print on the stage's Build node, against the
            // joined rows, as the ordinary plan prints its join's filter and the filter over it.
            assertQuery("SELECT t.k, d.w FROM t LEFT JOIN d ON t.k = d.k AND d.w > t.v WHERE d.w IS NULL OR d.s <> t.sym")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Async Hash Join workers: 1
                                  stages: 1
                                    Probe
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t
                                    Build
                                      joinType: left outer
                                      condition: d.k=t.k
                                      buildPayload: copied when the probe is large enough
                                      onFilter: t.v<d.w
                                      postJoinFilter: (d.w=null or d.s!=t.sym)
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: d
                            """);
            // With the probe's and the build's stolen filters, under an INNER join.
            assertQuery("SELECT t.k, d2.w FROM t JOIN (SELECT * FROM d WHERE w > 3) d2 ON t.k = d2.k WHERE t.v > 2 AND d2.w < t.v")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Async Hash Join workers: 1
                                  stages: 1
                                  filter: 2<v
                                    Probe
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t
                                    Build
                                      joinType: inner
                                      condition: d2.k=t.k
                                      buildPayload: copied when the probe is large enough
                                      postJoinFilter: d2.w<t.v
                                      buildFilter: 3<w
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: d
                            """);
            // The ordinary plan of the same SQL prints the same filters.
            final String ordinaryPlan = ordinaryPlan("SELECT t.k, d.w FROM t LEFT JOIN d ON t.k = d.k AND d.w > t.v WHERE d.w IS NULL OR d.s <> t.sym", sqlExecutionContext);
            TestUtils.assertContains(ordinaryPlan, "filter: (d.w=null or d.s!=t.sym)");
            TestUtils.assertContains(ordinaryPlan, "filter: t.v<d.w");
        });
    }

    @Test
    public void testSharedStepperTemplate() throws Exception {
        assertMemoryLeak(() -> {
            // A JVM that cannot define hidden classes hands every stage the shared stepper template,
            // whose loops over a stage's matches run here, under residuals and post-join filters, with
            // unique and repeated keys, INT, SYMBOL and staged keys, and both scan directions.
            useSteppers(false);
            try {
                createTables();
                sqlExecutionContext.changePageFrameSizes(3, 3);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 1);
                for (String build : new String[]{"d", "u"}) {
                    for (String on : new String[]{"t.k = d.k", "t.sym = d.s", "t.k = d.k AND t.sym = d.s"}) {
                        for (String sql : new String[]{
                                // A stage without filters, whose chains the consumer walks.
                                "SELECT t.k, t.v, d.w FROM t LEFT JOIN " + build + " d ON " + on,
                                "SELECT t.k, t.v, d.w FROM t LEFT JOIN " + build + " d ON " + on + " AND d.w % 3 = 0",
                                "SELECT t.k, t.v, d.w FROM t LEFT JOIN " + build + " d ON " + on + " WHERE d.w < t.v",
                                "SELECT t.k, t.v, d.w FROM t LEFT JOIN " + build + " d ON " + on + " AND d.w % 3 = 0 ORDER BY t.ts DESC",
                                "SELECT t.k, t.v, d.w FROM t LEFT JOIN " + build + " d ON " + on + " WHERE d.w IS NULL OR d.w < t.v",
                                "SELECT t.k, t.v, d.w FROM t JOIN " + build + " d ON " + on + " WHERE t.v > 3 AND d.w < t.v",
                                "SELECT t.k, t.v, d.w FROM t JOIN " + build + " d ON " + on + " WHERE t.v > 3 AND d.w < t.v ORDER BY t.ts DESC",
                        }) {
                            assertParallelHashJoin(sql, sqlExecutionContext);
                            try (RecordCursorFactory factory = select(sql)) {
                                Assert.assertFalse(find(factory).getStepperClass(0).isHidden());
                            }
                        }
                    }
                }
            } finally {
                restoreSteppers();
            }
        });
    }

    @Test
    public void testToTopMidStream() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            sqlExecutionContext.changePageFrameSizes(2, 3);
            for (String sql : new String[]{
                    "SELECT t.k, t.v, d.w, d.s FROM t LEFT JOIN d ON t.k = d.k AND d.vch ~ 'v[1-3]' WHERE d.w IS NULL OR d.w > t.v",
                    "SELECT t.k, t.v, d.w, d.s FROM t JOIN d ON t.k = d.k WHERE t.v > 3 AND d.s <> t.sym",
            }) {
                final String expected = ordinaryRows(sql, sqlExecutionContext);
                try (RecordCursorFactory factory = select(sql)) {
                    Assert.assertNotNull(find(factory));
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        // toTop() before the first read, after a few rows, and after the last row each
                        // read the whole result again, as the filters go back to the top too.
                        for (int stop : new int[]{0, 1, 5, 9, Integer.MAX_VALUE}) {
                            final String rows = printRows(cursor, factory.getMetadata(), stop);
                            Assert.assertTrue(sql + '\n' + rows, expected.startsWith(rows));
                            cursor.toTop();
                        }
                        Assert.assertEquals(expected, printRows(cursor, factory.getMetadata(), Integer.MAX_VALUE));
                    }
                }
            }
        });
    }

    @Test
    public void testRuntimeConstantResidual() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // The ordinary plan runs an ON residual whose value is a runtime constant per pair, as it
            // runs any residual, and so do the workers; a bind variable takes each execution's value.
            final String sql = "SELECT t.k, t.v, d.w FROM t LEFT JOIN d ON t.k = d.k AND (d.w > 10 OR $1)";
            for (boolean value : new boolean[]{false, true}) {
                bindVariableService.clear();
                bindVariableService.setBoolean(0, value);
                TestUtils.assertContains(assertParallelHashJoin(sql, sqlExecutionContext), "onFilter: ");
            }
            bindVariableService.clear();
            bindVariableService.setLong(0, 15);
            final String postFilter = "SELECT t.k, t.v, d.w FROM t LEFT JOIN d ON t.k = d.k WHERE d.w IS NULL OR d.w > $1";
            TestUtils.assertContains(assertParallelHashJoin(postFilter, sqlExecutionContext), "postJoinFilter: ");
            // One factory runs each execution with that execution's values, in its residual and its
            // post-join filter.
            final String both = "SELECT t.k, t.v, d.w FROM t LEFT JOIN d ON t.k = d.k AND d.w > $1 WHERE d.w IS NULL OR d.w < t.v + $2";
            try (RecordCursorFactory factory = select(both)) {
                Assert.assertNotNull(find(factory));
                for (long[] values : new long[][]{{5, 0}, {20, 10}, {-1, 100}}) {
                    bindVariableService.clear();
                    bindVariableService.setLong(0, values[0]);
                    bindVariableService.setLong(1, values[1]);
                    Assert.assertEquals(ordinary(both, sqlExecutionContext), result(factory, sqlExecutionContext));
                }
            }
        });
    }

    private static void assertPairFilter(String sql) throws SqlException {
        for (int jitMode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
            sqlExecutionContext.setJitMode(jitMode);
            final String plan = assertParallelHashJoin(sql, sqlExecutionContext);
            Assert.assertTrue(sql + '\n' + plan, plan.contains("onFilter: ") || plan.contains("postJoinFilter: "));
        }
    }

    private static void createTables() throws Exception {
        execute("""
                CREATE TABLE t AS (
                    SELECT
                        CASE WHEN x % 9 = 0 THEN NULL ELSE (x % 7)::INT END k,
                        x v,
                        ('s' || (x % 4))::SYMBOL sym,
                        CASE WHEN x % 5 = 0 THEN NULL ELSE 'str' || (x % 6) END str,
                        timestamp_sequence('2024-01-01', 600_000_000) ts
                    FROM long_sequence(60)
                ) TIMESTAMP(ts) PARTITION BY HOUR
                """);
        // d repeats its keys and inserts its symbols in another order than t, so equal texts have
        // different keys; u holds one row per key, and per SYMBOL.
        execute("""
                CREATE TABLE d AS (
                    SELECT
                        CASE WHEN x % 6 = 0 THEN NULL ELSE (x % 5)::INT END k,
                        x w,
                        ('s' || (5 - x % 5))::SYMBOL s,
                        CASE WHEN x % 4 = 0 THEN NULL ELSE 'v' || x END::VARCHAR vch,
                        timestamp_sequence('2023-12-31', 600_000_000) dts
                    FROM long_sequence(40)
                ) TIMESTAMP(dts) PARTITION BY HOUR
                """);
        execute("""
                CREATE TABLE u AS (
                    SELECT
                        (x % 6)::INT k,
                        x * 7 w,
                        ('s' || (6 - x))::SYMBOL s,
                        CASE WHEN x % 4 = 0 THEN NULL ELSE 'v' || x END::VARCHAR vch
                    FROM long_sequence(6)
                )
                """);
    }

    private static String planText(String sql) throws SqlException {
        try (RecordCursorFactory factory = select(sql)) {
            return plan(factory, sqlExecutionContext);
        }
    }

    // Up to the given number of the cursor's next rows, as CursorPrinter prints them.
    private static String printRows(RecordCursor cursor, RecordMetadata metadata, int maxRows) {
        final StringSink sink = new StringSink();
        for (int rows = 0; rows < maxRows && cursor.hasNext(); rows++) {
            CursorPrinter.println(cursor.getRecord(), metadata, sink, true);
        }
        return sink.toString();
    }

    private static boolean hasFactory(RecordCursorFactory factory, Class<?> type) {
        for (RecordCursorFactory current = factory; current != null; current = current.getBaseFactory()) {
            if (type.isInstance(current)) {
                return true;
            }
        }
        return false;
    }
}
