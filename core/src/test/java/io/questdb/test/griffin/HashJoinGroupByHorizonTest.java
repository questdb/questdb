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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.QueryAssertion;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.function.UnaryOperator;

import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.context;
import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.plan;

/**
 * The fused hash join GROUP BY next to the operators that compile their own GROUP BY functions:
 * HORIZON JOIN, WINDOW JOIN, GROUP BY and SAMPLE BY. A fused query can be their input, or a scalar
 * sub-query that the code generator compiles while one of them is half-way through its own
 * generation step, so the generator saves and restores that step's scratch around the nested
 * compile. The class also covers join keys that repeat a column with mixed text encodings.
 */
public class HashJoinGroupByHorizonTest extends AbstractCairoTest {
    // LATEST ON leaves its join input without page frames, which the planner finds out only after
    // it compiled the other input, so the sub-query falls back to the ordinary plan mid-way.
    private static final String FALLBACK_SUBQUERY = """
            (SELECT sum(r.w) FROM trades t2 JOIN ((SELECT k, w, ts FROM refs) LATEST ON ts PARTITION BY k) r ON t2.k = r.k)""";
    private static final String FUSED_SAMPLE_BY = """
            ((SELECT t.ts, t.sym, sum(t.qty * r.w) sw, count() n FROM trades t %s refs r ON t.k = r.k SAMPLE BY 1m) TIMESTAMP(ts))""";
    private static final String FUSED_SUBQUERY = """
            (SELECT sum(r.w) FROM trades t2 JOIN refs r ON t2.k = r.k)""";

    @Test
    public void testFallbackScalarSubQueryInHorizonGroupByAndSampleBy() throws Exception {
        // The sub-query passes the fused analysis and compiles one input before the other turns out
        // to have no page frames. HORIZON JOIN, GROUP BY and SAMPLE BY are then assembling their own
        // functions, and must get their scratch back from the abandoned compile as well as from the
        // ordinary one that follows it.
        assertMemoryLeak(() -> {
            createTables();
            for (int workers : new int[]{1, 4}) {
                try (SqlExecutionContextImpl context = context(engine, workers)) {
                    context.changePageFrameSizes(1, 2);
                    for (boolean isParallelHorizonJoin : new boolean[]{false, true}) {
                        context.setParallelHorizonJoinEnabled(isParallelHorizonJoin);
                        assertBothPlans(
                                "SELECT t.sym, h.offset / 1_000_000 sec, sum(CASE WHEN t.qty * 50 > " + FALLBACK_SUBQUERY
                                        + " THEN p.price ELSE 0.0 END) c, count() n FROM trades t HORIZON JOIN prices p ON (t.sym = p.sym)"
                                        + " LIST (0, 60s) AS h ORDER BY 1, 2",
                                context,
                                false,
                                """
                                        sym\tsec\tc\tn
                                        A\t0\t120.0\t2
                                        A\t60\t120.0\t2
                                        B\t0\t410.0\t2
                                        B\t60\t410.0\t2
                                        C\t0\t300.0\t1
                                        C\t60\t310.0\t1
                                        """,
                                QueryAssertion::expectSize
                        );
                    }
                    assertBothPlans(
                            "SELECT sym, t.qty * 50 > " + FALLBACK_SUBQUERY + " big, sum(CASE WHEN qty * 50 > " + FALLBACK_SUBQUERY
                                    + " THEN qty ELSE 0.0 END) q FROM trades t ORDER BY 1, 2",
                            context,
                            false,
                            """
                                    sym\tbig\tq
                                    A\tfalse\t0.0
                                    A\ttrue\t3.0
                                    B\ttrue\t7.0
                                    C\ttrue\t4.0
                                    """,
                            QueryAssertion::expectSize
                    );
                    assertBothPlans(
                            "SELECT ts, sum(CASE WHEN qty * 50 > " + FALLBACK_SUBQUERY + " THEN qty ELSE 0.0 END) q, count() n FROM trades SAMPLE BY 1m",
                            context,
                            false,
                            """
                                    ts\tq\tn
                                    2024-01-01T00:00:00.000000Z\t2.0\t2
                                    2024-01-01T00:01:00.000000Z\t7.0\t2
                                    2024-01-02T00:00:00.000000Z\t5.0\t1
                                    """,
                            assertion -> assertion.expectSize().timestamp("ts")
                    );
                    // The fused sub-query in the same places.
                    assertSubqueryFuses(context);
                    assertResultsOfBothPlans(
                            "SELECT sym, t.qty * 50 > " + FUSED_SUBQUERY + " big, sum(CASE WHEN qty * 50 > " + FUSED_SUBQUERY
                                    + " THEN qty ELSE 0.0 END) q FROM trades t ORDER BY 1, 2",
                            context,
                            """
                                    sym\tbig\tq
                                    A\tfalse\t0.0
                                    A\ttrue\t3.0
                                    B\tfalse\t0.0
                                    B\ttrue\t5.0
                                    C\ttrue\t4.0
                                    """,
                            QueryAssertion::expectSize
                    );
                    assertResultsOfBothPlans(
                            "SELECT ts, sum(CASE WHEN qty * 50 > " + FUSED_SUBQUERY + " THEN qty ELSE 0.0 END) q, count() n FROM trades SAMPLE BY 1m",
                            context,
                            """
                                    ts\tq\tn
                                    2024-01-01T00:00:00.000000Z\t0.0\t2
                                    2024-01-01T00:01:00.000000Z\t7.0\t2
                                    2024-01-02T00:00:00.000000Z\t5.0\t1
                                    """,
                            assertion -> assertion.expectSize().timestamp("ts")
                    );
                }
            }
        });
    }

    @Test
    public void testFusedSampleByAsHorizonMaster() throws Exception {
        // The fused SAMPLE BY keeps its designated timestamp through TIMESTAMP(ts), so it can be the
        // master of a single- and a multi-slave HORIZON JOIN. The LEFT join keeps plant C, whose
        // sum has no build row.
        assertMemoryLeak(() -> {
            createTables();
            for (int workers : new int[]{1, 4}) {
                try (SqlExecutionContextImpl context = context(engine, workers)) {
                    context.changePageFrameSizes(1, 2);
                    assertFusedSampleByAsHorizonMaster(context);
                }
            }
        });
    }

    @Test
    public void testFusedSampleByAsHorizonMasterOnWorkerPool() throws Exception {
        assertMemoryLeak(() -> {
            final TestWorkerPool pool = new TestWorkerPool(4, WorkerPoolMode.LEGACY);
            TestUtils.execute(pool, (db, compiler, ctx) -> {
                createTables(db, ctx);
                try (SqlExecutionContextImpl context = context(db, 4)) {
                    context.changePageFrameSizes(1, 2);
                    assertFusedSampleByAsHorizonMaster(context);
                }
            }, configuration, LOG);
        });
    }

    @Test
    public void testFusedScalarSubQueryInHorizonAggregate() throws Exception {
        // HORIZON JOIN compiles the sub-query while it assembles its GROUP BY functions, in an
        // aggregate argument or in a key, over one and over two slaves.
        assertMemoryLeak(() -> {
            createTables();
            for (int workers : new int[]{1, 4}) {
                try (SqlExecutionContextImpl context = context(engine, workers)) {
                    context.changePageFrameSizes(1, 2);
                    assertSubqueryFuses(context);
                    for (boolean isParallelHorizonJoin : new boolean[]{false, true}) {
                        context.setParallelHorizonJoinEnabled(isParallelHorizonJoin);
                        assertResultsOfBothPlans(
                                "SELECT t.sym, h.offset / 1_000_000 sec, sum(CASE WHEN t.qty * 50 > " + FUSED_SUBQUERY
                                        + " THEN p.price ELSE 0.0 END) c, count() n FROM trades t HORIZON JOIN prices p ON (t.sym = p.sym)"
                                        + " LIST (0, 60s) AS h ORDER BY 1, 2",
                                context,
                                """
                                        sym\tsec\tc\tn
                                        A\t0\t120.0\t2
                                        A\t60\t120.0\t2
                                        B\t0\t210.0\t2
                                        B\t60\t210.0\t2
                                        C\t0\t300.0\t1
                                        C\t60\t310.0\t1
                                        """,
                                QueryAssertion::expectSize
                        );
                        assertResultsOfBothPlans(
                                "SELECT t.qty * 50 > " + FUSED_SUBQUERY + " big, h.offset / 1_000_000 sec, avg(p.price) px, sum(t.qty) q"
                                        + " FROM trades t HORIZON JOIN prices p ON (t.sym = p.sym) LIST (0, 60s) AS h ORDER BY 1, 2",
                                context,
                                """
                                        big\tsec\tpx\tq
                                        false\t0\t150.0\t3.0
                                        false\t60\t160.0\t3.0
                                        true\t0\t210.0\t12.0
                                        true\t60\t213.33333333333334\t12.0
                                        """,
                                QueryAssertion::expectSize
                        );
                        assertResultsOfBothPlans(
                                "SELECT sum(CASE WHEN t.qty * 50 > " + FUSED_SUBQUERY + " THEN p.price ELSE 0.0 END) c, avg(p.price) px"
                                        + " FROM trades t HORIZON JOIN prices p ON (t.sym = p.sym) LIST (0) AS h",
                                context,
                                """
                                        c\tpx
                                        630.0\t186.0
                                        """,
                                assertion -> assertion.expectSize().noRandomAccess()
                        );
                        assertResultsOfBothPlans(
                                "SELECT t.sym, h.offset / 1_000_000 sec, sum(CASE WHEN t.qty * 50 > " + FUSED_SUBQUERY
                                        + " THEN p.price ELSE 0.0 END) c, avg(q.price) qx FROM trades t HORIZON JOIN prices p ON (t.sym = p.sym)"
                                        + " HORIZON JOIN prices q ON (t.v = q.v) LIST (0, 60s) AS h ORDER BY 1, 2",
                                context,
                                """
                                        sym\tsec\tc\tqx
                                        A\t0\t120.0\t105.0
                                        A\t60\t120.0\t165.0
                                        B\t0\t210.0\t205.0
                                        B\t60\t210.0\t160.0
                                        C\t0\t300.0\t300.0
                                        C\t60\t310.0\t300.0
                                        """,
                                QueryAssertion::expectSize
                        );
                    }
                }
            }
        });
    }

    @Test
    public void testFusedScalarSubQueryInWindowJoin() throws Exception {
        // WINDOW JOIN compiles the sub-query while it assembles its aggregates, and its master
        // filter steals the sub-query's function, whose fused plan the outer plan shows.
        assertMemoryLeak(() -> {
            createTables();
            for (int workers : new int[]{1, 4}) {
                try (SqlExecutionContextImpl context = context(engine, workers)) {
                    context.changePageFrameSizes(1, 2);
                    assertSubqueryFuses(context);
                    for (boolean isParallelWindowJoin : new boolean[]{false, true}) {
                        context.setParallelWindowJoinEnabled(isParallelWindowJoin);
                        for (String on : new String[]{"", " ON (t.sym = p.sym)"}) {
                            assertResultsOfBothPlans(
                                    "SELECT t.ts, sum(CASE WHEN t.qty * 50 > " + FUSED_SUBQUERY + " THEN p.price ELSE 0.0 END) c,"
                                            + " avg(p.price) a, count() n FROM trades t WINDOW JOIN prices p" + on
                                            + " RANGE BETWEEN 1 minute PRECEDING AND 1 minute FOLLOWING",
                                    context,
                                    on.isEmpty() ? """
                                            ts\tc\ta\tn
                                            2024-01-01T00:00:10.000000Z\t0.0\t166.0\t5
                                            2024-01-01T00:00:20.000000Z\t0.0\t166.0\t5
                                            2024-01-01T00:01:10.000000Z\t850.0\t170.0\t5
                                            2024-01-01T00:01:40.000000Z\t750.0\t187.5\t4
                                            2024-01-02T00:00:10.000000Z\t710.0\t355.0\t2
                                            """ : """
                                            ts\tc\ta\tn
                                            2024-01-01T00:00:10.000000Z\t0.0\t110.0\t3
                                            2024-01-01T00:00:20.000000Z\t0.0\t200.0\t1
                                            2024-01-01T00:01:10.000000Z\t330.0\t110.0\t3
                                            2024-01-01T00:01:40.000000Z\t610.0\t305.0\t2
                                            2024-01-02T00:00:10.000000Z\t210.0\t210.0\t1
                                            """,
                                    // The parallel WINDOW JOIN does not know its size up front.
                                    assertion -> assertion.expectSize(!isParallelWindowJoin).noRandomAccess().timestamp("ts")
                            );
                            assertBothPlans(
                                    "SELECT t.ts, sum(p.price) s, count() n FROM trades t WINDOW JOIN prices p" + on
                                            + " RANGE BETWEEN 1 minute PRECEDING AND 1 minute FOLLOWING WHERE t.qty * 50 > " + FUSED_SUBQUERY,
                                    context,
                                    true,
                                    on.isEmpty() ? """
                                            ts\ts\tn
                                            2024-01-01T00:01:10.000000Z\t850.0\t5
                                            2024-01-01T00:01:40.000000Z\t750.0\t4
                                            2024-01-02T00:00:10.000000Z\t710.0\t2
                                            """ : """
                                            ts\ts\tn
                                            2024-01-01T00:01:10.000000Z\t330.0\t3
                                            2024-01-01T00:01:40.000000Z\t610.0\t2
                                            2024-01-02T00:00:10.000000Z\t210.0\t1
                                            """,
                                    assertion -> assertion.noRandomAccess().timestamp("ts")
                            );
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testPlanOfFusedScalarSubQueryInAggregate() throws Exception {
        // AsyncHashJoinGroupByRecordCursorFactory.toPlan() sets the plan sink's metadata for its own
        // columns. It must give the caller back its metadata and base-metadata mode, and it must not
        // resolve its own columns through the caller's base metadata. An operator that prints its
        // functions over its own metadata, a HORIZON JOIN, or over its base metadata, a GROUP BY
        // over a hash join, an Async Group By or a SAMPLE BY, otherwise resolves the fused
        // sub-query's columns or the columns that follow it in the wrong metadata. The outer GROUP
        // BY over the hash join doesn't fuse: the fused path rejects a sub-query in an aggregate.
        // Each fragment is the text that follows the sub-query's plan, which matches the text the
        // plan prints with the fused path off.
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContextImpl context = context(engine, 1)) {
                final String[][] cases = {
                        {
                                "SELECT h.offset / 1_000_000 sec, sum(CASE WHEN t.qty * 50 > " + FUSED_SUBQUERY + " THEN p.price ELSE 0.0 END) c,"
                                        + " avg(p.price) px FROM trades t HORIZON JOIN prices p ON (t.sym = p.sym) LIST (0, 60s) AS h ORDER BY 1",
                                "p.price,0.0])),avg(p.price)]"
                        },
                        {
                                "SELECT h.offset / 1_000_000 sec, sum(CASE WHEN t.qty * 50 > " + FUSED_SUBQUERY + " THEN p.price ELSE 0.0 END) c,"
                                        + " avg(q.price) qx FROM trades t HORIZON JOIN prices p ON (t.sym = p.sym) HORIZON JOIN prices q ON (t.v = q.v)"
                                        + " LIST (0, 60s) AS h ORDER BY 1",
                                "p.price,0.0])),avg(q.price)]"
                        },
                        {
                                "SELECT t.sym, sum(CASE WHEN t.qty * 50 > " + FUSED_SUBQUERY + " THEN r.w ELSE 0.0 END) a, sum(r.w) b"
                                        + " FROM trades t JOIN refs r ON t.k = r.k ORDER BY 1",
                                "w,0.0])),sum(w)]"
                        },
                        {
                                "SELECT sym, t.qty * 50 > " + FUSED_SUBQUERY + " big, sum(CASE WHEN qty * 50 > " + FUSED_SUBQUERY
                                        + " THEN qty ELSE 0.0 END) q FROM trades t ORDER BY 1, 2",
                                "qty,0.0]))]"
                        },
                        {
                                "SELECT ts, sum(CASE WHEN qty * 50 > " + FUSED_SUBQUERY + " THEN qty ELSE 0.0 END) q, count() n FROM trades SAMPLE BY 1m",
                                "qty,0.0])),count(*)]"
                        }
                };
                for (String[] c : cases) {
                    assertQuery(c[0])
                            .withContext(context)
                            .noLeakCheck()
                            .assertsPlanContaining("Async Hash Join Group By", c[1]);
                }
            }
        });
    }

    @Ignore("Known bug, fixed by PR #7674: a repeated key column moves into a filter that drops preserved rows")
    @Test
    public void testRepeatedKeyColumnKeepsPreservedRows() throws Exception {
        // SqlOptimiser.mergeContexts() folds a slave column that two keys share into an equality of
        // the two master columns. That equality filters the master before a LEFT or HORIZON JOIN,
        // and after a RIGHT JOIN, so it drops rows that the join must keep with NULLs instead.
        // PR #7674 keeps both key pairs instead, so with it this test also reaches the fused path's
        // shared-key code: unifyRepeatedKeyColumnEncodings() and HashJoinGroupByKeys.hasEncodingConflict().
        assertMemoryLeak(() -> {
            createTables();
            for (boolean isEnabled : new boolean[]{false, true}) {
                sqlExecutionContext.setParallelHashJoinGroupByEnabled(isEnabled);
                // Trade 3 has v = 'B' for sym 'A', and trade 4 has s = 'D' for sym 'C', so no price
                // matches both of their columns.
                assertQuery("SELECT t.qty, p.price FROM trades t LEFT JOIN prices p ON p.sym = t.sym AND p.sym = t.v ORDER BY 1, 2")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                qty\tprice
                                1.0\t100.0
                                1.0\t110.0
                                1.0\t120.0
                                2.0\t200.0
                                2.0\t210.0
                                3.0\tnull
                                4.0\t300.0
                                4.0\t310.0
                                5.0\t200.0
                                5.0\t210.0
                                """);
                assertQuery("SELECT t.sym, count() n, count(p.price) np FROM trades t LEFT JOIN prices p ON p.sym = t.sym AND p.sym = t.v ORDER BY 1")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                sym\tn\tnp
                                A\t4\t3
                                B\t4\t4
                                C\t2\t2
                                """);
                assertQuery("SELECT t.sym, h.offset / 1_000_000 sec, count() n, avg(p.price) px FROM trades t"
                        + " HORIZON JOIN prices p ON (p.sym = t.sym AND p.sym = t.v) LIST (0) AS h ORDER BY 1, 2")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                sym\tsec\tn\tpx
                                A\t0\t2\t100.0
                                B\t0\t2\t205.0
                                C\t0\t1\t300.0
                                """);
                // A slave column that two keys share moves into a filter after the RIGHT join. Trade 4
                // has sym 'C' and s 'D': prices 310 and 400 match its s but not its sym, so the join
                // must NULL-extend them rather than join them to trade 4 and filter them out.
                // A master column that two keys share moves into a filter of the RIGHT join's
                // preserved side: prices 110 and 310 have sym <> v, so no trade matches them.
                assertQuery("SELECT p.sym, count() n, sum(t.qty) q FROM trades t RIGHT JOIN prices p ON t.sym = p.sym AND t.sym = p.v ORDER BY 1")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                sym\tn\tq
                                A\t5\t8.0
                                B\t4\t14.0
                                C\t2\t4.0
                                D\t1\tnull
                                """);
                assertQuery("SELECT p.price, t.qty FROM trades t RIGHT JOIN prices p ON p.s = t.sym AND p.s = t.s ORDER BY 1, 2")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                price\tqty
                                100.0\t1.0
                                100.0\t3.0
                                110.0\t1.0
                                110.0\t3.0
                                120.0\t1.0
                                120.0\t3.0
                                200.0\t2.0
                                200.0\t5.0
                                210.0\t2.0
                                210.0\t5.0
                                300.0\tnull
                                310.0\tnull
                                400.0\tnull
                                """);
            }
        });
    }

    @Ignore("Known bug, fixed by PR #7674: a repeated key column over a projected alias fails to compile")
    @Test
    public void testRepeatedKeyColumnOverProjectedAlias() throws Exception {
        // The master projects sym twice. SqlOptimiser.mergeContexts() folds the shared slave column
        // into t.sym = t.sym2, and the code generator then looks up a key name that the master no
        // longer exposes: the compile throws InvalidColumnException instead of a SqlException.
        assertMemoryLeak(() -> {
            createTables();
            for (boolean isEnabled : new boolean[]{false, true}) {
                sqlExecutionContext.setParallelHashJoinGroupByEnabled(isEnabled);
                assertQuery("SELECT t.qty, p.price FROM (SELECT ts, sym, sym sym2, qty FROM trades) t"
                        + " JOIN prices p ON t.sym = p.sym AND t.sym2 = p.sym ORDER BY 1, 2")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                qty\tprice
                                1.0\t100.0
                                1.0\t110.0
                                1.0\t120.0
                                2.0\t200.0
                                2.0\t210.0
                                3.0\t100.0
                                3.0\t110.0
                                3.0\t120.0
                                4.0\t300.0
                                4.0\t310.0
                                5.0\t200.0
                                5.0\t210.0
                                """);
            }
        });
    }

    @Test
    public void testRepeatedKeyColumnsInHorizon() throws Exception {
        // A master column that two keys share folds into an equality of the slave's two columns,
        // which filters the slave. A column that the master projects twice stays two keys, of
        // different encodings over one base column: SYMBOL against SYMBOL, and SYMBOL against VARCHAR.
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                context.changePageFrameSizes(1, 2);
                for (boolean isParallelHorizonJoin : new boolean[]{false, true}) {
                    context.setParallelHorizonJoinEnabled(isParallelHorizonJoin);
                    assertResultsOfBothPlans(
                            "SELECT t.sym, h.offset / 1_000_000 sec, count() n, avg(p.price) px FROM trades t"
                                    + " HORIZON JOIN prices p ON (t.s = p.sym AND t.s = p.v) LIST (0, 60s) AS h ORDER BY 1, 2",
                            context,
                            """
                                    sym\tsec\tn\tpx
                                    A\t0\t2\t110.0
                                    A\t60\t2\t120.0
                                    B\t0\t2\t205.0
                                    B\t60\t2\t205.0
                                    C\t0\t1\tnull
                                    C\t60\t1\tnull
                                    """,
                            QueryAssertion::expectSize
                    );
                    assertResultsOfBothPlans(
                            "SELECT t.sym, h.offset / 1_000_000 sec, count() n, avg(p.price) px, avg(q.price) qx FROM trades t"
                                    + " HORIZON JOIN prices p ON (t.v = p.s AND t.v = p.sym)"
                                    + " HORIZON JOIN prices q ON (t.s = q.sym AND t.s = q.v) LIST (0, 60s) AS h ORDER BY 1, 2",
                            context,
                            """
                                    sym\tsec\tn\tpx\tqx
                                    A\t0\t2\t150.0\t110.0
                                    A\t60\t2\t165.0\t120.0
                                    B\t0\t2\t205.0\t205.0
                                    B\t60\t2\t205.0\t205.0
                                    C\t0\t1\t300.0\tnull
                                    C\t60\t1\t300.0\tnull
                                    """,
                            QueryAssertion::expectSize
                    );
                    final String projected = " FROM (SELECT ts, sym, sym sym2, s, v, qty FROM trades) t";
                    assertResultsOfBothPlans(
                            "SELECT t.sym, h.offset / 1_000_000 sec, count() n, avg(p.price) px" + projected
                                    + " HORIZON JOIN prices p ON (p.sym = t.sym AND p.v = t.sym2) LIST (0, 60s) AS h ORDER BY 1, 2",
                            context,
                            """
                                    sym\tsec\tn\tpx
                                    A\t0\t2\t110.0
                                    A\t60\t2\t120.0
                                    B\t0\t2\t205.0
                                    B\t60\t2\t205.0
                                    C\t0\t1\t300.0
                                    C\t60\t1\t300.0
                                    """,
                            QueryAssertion::expectSize
                    );
                    assertResultsOfBothPlans(
                            "SELECT t.sym, h.offset / 1_000_000 sec, count() n, avg(p.price) px, avg(q.price) qx" + projected
                                    + " HORIZON JOIN prices p ON (p.sym = t.sym AND p.v = t.sym2)"
                                    + " HORIZON JOIN prices q ON (q.s = t.sym2 AND q.sym = t.sym) LIST (0, 60s) AS h ORDER BY 1, 2",
                            context,
                            """
                                    sym\tsec\tn\tpx\tqx
                                    A\t0\t2\t110.0\t110.0
                                    A\t60\t2\t120.0\t120.0
                                    B\t0\t2\t205.0\t205.0
                                    B\t60\t2\t205.0\t205.0
                                    C\t0\t1\t300.0\t300.0
                                    C\t60\t1\t300.0\t300.0
                                    """,
                            QueryAssertion::expectSize
                    );
                }
            }
        });
    }

    @Test
    public void testRepeatedMasterKeyColumnInHashJoin() throws Exception {
        // t.sym pairs with a SYMBOL and a VARCHAR column of p. The optimiser keeps one pair and
        // filters p on the equality of its two columns, which the fused plan runs on its input:
        // the LEFT join builds p and runs the filter over the build's page frames, VARCHAR column
        // and all, and the filter drops the C price whose VARCHAR reads A. A master that projects
        // sym twice keeps both pairs, a SYMBOL and a VARCHAR one over the same base column, on the
        // ordinary plan.
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                context.changePageFrameSizes(1, 2);
                assertBothPlans(
                        "SELECT t.sym, count() n, sum(p.price) s FROM trades t JOIN prices p ON t.sym = p.sym AND t.sym = p.v ORDER BY 1",
                        context,
                        true,
                        """
                                sym\tn\ts
                                A\t4\t440.0
                                B\t4\t820.0
                                C\t1\t300.0
                                """,
                        QueryAssertion::expectSize
                );
                assertBothPlans(
                        "SELECT t.sym, count() n, count(p.price) np, sum(p.price) s FROM trades t LEFT JOIN prices p ON t.sym = p.sym AND t.sym = p.v ORDER BY 1",
                        context,
                        true,
                        """
                                sym\tn\tnp\ts
                                A\t4\t4\t440.0
                                B\t4\t4\t820.0
                                C\t1\t1\t300.0
                                """,
                        QueryAssertion::expectSize
                );
                for (String join : new String[]{"JOIN", "LEFT JOIN"}) {
                    assertBothPlans(
                            "SELECT t.sym, count() n, count(p.price) np, sum(p.price) s FROM (SELECT ts, sym, sym sym2, qty FROM trades) t "
                                    + join + " prices p ON p.sym = t.sym AND p.v = t.sym2 ORDER BY 1",
                            context,
                            false,
                            """
                                    sym\tn\tnp\ts
                                    A\t4\t4\t440.0
                                    B\t4\t4\t820.0
                                    C\t1\t1\t300.0
                                    """,
                            QueryAssertion::expectSize
                    );
                }
            }
        });
    }

    private void assertBothPlans(
            String sql,
            SqlExecutionContextImpl context,
            boolean isFused,
            String expected,
            UnaryOperator<QueryAssertion> properties
    ) throws Exception {
        for (boolean isEnabled : new boolean[]{true, false}) {
            context.setParallelHashJoinGroupByEnabled(isEnabled);
            try {
                final QueryAssertion assertion = properties.apply(assertQuery(sql)
                        .withEngine(context.getCairoEngine())
                        .withContext(context)
                        .noLeakCheck());
                if (isEnabled && isFused) {
                    assertion.withPlanContaining("Async Hash Join Group By");
                } else {
                    assertion.withPlanNotContaining("Hash Join Group By");
                }
                assertion.returns(expected);
            } finally {
                context.setParallelHashJoinGroupByEnabled(true);
            }
        }
    }

    private void assertFusedSampleByAsHorizonMaster(SqlExecutionContextImpl context) throws Exception {
        for (boolean isParallelHorizonJoin : new boolean[]{false, true}) {
            context.setParallelHorizonJoinEnabled(isParallelHorizonJoin);
            for (String join : new String[]{"JOIN", "LEFT JOIN"}) {
                final String master = FUSED_SAMPLE_BY.formatted(join);
                final boolean isLeft = join.startsWith("LEFT");
                assertBothPlans(
                        "SELECT m.sym, h.offset / 1_000_000 sec, sum(m.sw) sw, sum(m.n) n, avg(p.price) px FROM " + master
                                + " m HORIZON JOIN prices p ON (m.sym = p.sym) RANGE FROM 0s TO 60s STEP 30s AS h ORDER BY 1, 2",
                        context,
                        true,
                        """
                                sym\tsec\tsw\tn\tpx
                                A\t0\t40.0\t2\t110.0
                                A\t30\t40.0\t2\t115.0
                                A\t60\t40.0\t2\t120.0
                                B\t0\t287.0\t4\t205.0
                                B\t30\t287.0\t4\t205.0
                                B\t60\t287.0\t4\t205.0
                                """ + (isLeft ? """
                                C\t0\tnull\t1\t300.0
                                C\t30\tnull\t1\t300.0
                                C\t60\tnull\t1\t310.0
                                """ : ""),
                        QueryAssertion::expectSize
                );
                // The second slave joins on a VARCHAR column, so the master's SYMBOL key writes its text.
                assertBothPlans(
                        "SELECT m.sym, h.offset / 1_000_000 sec, sum(m.sw) sw, avg(p.price) px, avg(q.price) qx FROM " + master
                                + " m HORIZON JOIN prices p ON (m.sym = p.sym) HORIZON JOIN prices q ON (m.sym = q.v)"
                                + " RANGE FROM 0s TO 60s STEP 30s AS h ORDER BY 1, 2",
                        context,
                        true,
                        """
                                sym\tsec\tsw\tpx\tqx
                                A\t0\t40.0\t110.0\t110.0
                                A\t30\t40.0\t115.0\t110.0
                                A\t60\t40.0\t120.0\t215.0
                                B\t0\t287.0\t205.0\t205.0
                                B\t30\t287.0\t205.0\t160.0
                                B\t60\t287.0\t205.0\t160.0
                                """ + (isLeft ? """
                                C\t0\tnull\t300.0\t300.0
                                C\t30\tnull\t300.0\t300.0
                                C\t60\tnull\t310.0\t300.0
                                """ : ""),
                        QueryAssertion::expectSize
                );
            }
        }
    }

    // Asserts results only, with the fused path on and off. testPlanOfFusedScalarSubQueryInAggregate()
    // asserts the plans of a fused scalar sub-query nested in an outer operator.
    private void assertResultsOfBothPlans(
            String sql,
            SqlExecutionContextImpl context,
            String expected,
            UnaryOperator<QueryAssertion> properties
    ) throws Exception {
        for (boolean isEnabled : new boolean[]{true, false}) {
            context.setParallelHashJoinGroupByEnabled(isEnabled);
            try {
                properties.apply(assertQuery(sql)
                                .withEngine(context.getCairoEngine())
                                .withContext(context)
                                .noLeakCheck())
                        .returns(expected);
            } finally {
                context.setParallelHashJoinGroupByEnabled(true);
            }
        }
    }

    // The sub-query on its own fuses under the context, so the outer queries compile it fused.
    private static void assertSubqueryFuses(SqlExecutionContextImpl context) throws Exception {
        try (RecordCursorFactory factory = context.getCairoEngine().select(FUSED_SUBQUERY, context)) {
            Assert.assertTrue(plan(factory, context).contains("Async Hash Join Group By"));
        }
    }

    private void createTables() throws Exception {
        createTables(engine, sqlExecutionContext);
    }

    // Trade 3's VARCHAR and trade 4's STRING differ from their SYMBOL. Prices take their symbols in
    // another order than trades, so equal texts have different symbol keys on the two sides.
    private static void createTables(CairoEngine db, SqlExecutionContext ctx) throws Exception {
        db.execute("CREATE TABLE trades (ts TIMESTAMP, sym SYMBOL, s STRING, v VARCHAR, k INT, qty DOUBLE) TIMESTAMP(ts) PARTITION BY DAY", ctx);
        db.execute("CREATE TABLE refs (k INT, w DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY", ctx);
        db.execute("CREATE TABLE prices (ts TIMESTAMP, sym SYMBOL, s STRING, v VARCHAR, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY", ctx);
        db.execute("""
                INSERT INTO trades VALUES
                    ('2024-01-01T00:00:10.000000Z', 'A', 'A', 'A', 1, 1.0),
                    ('2024-01-01T00:00:20.000000Z', 'B', 'B', 'B', 2, 2.0),
                    ('2024-01-01T00:01:10.000000Z', 'A', 'A', 'B', 1, 3.0),
                    ('2024-01-01T00:01:40.000000Z', 'C', 'D', 'C', 3, 4.0),
                    ('2024-01-02T00:00:10.000000Z', 'B', 'B', 'B', 2, 5.0)
                """, ctx);
        db.execute("""
                INSERT INTO refs VALUES
                    (1, 10.0, '2024-01-01'),
                    (2, 20.0, '2024-01-01'),
                    (2, 21.0, '2024-01-02'),
                    (4, 40.0, '2024-01-02')
                """, ctx);
        db.execute("""
                INSERT INTO prices VALUES
                    ('2024-01-01T00:00:00.000000Z', 'C', 'C', 'C', 300.0),
                    ('2024-01-01T00:00:00.000000Z', 'B', 'B', 'B', 200.0),
                    ('2024-01-01T00:00:00.000000Z', 'A', 'A', 'A', 100.0),
                    ('2024-01-01T00:00:30.000000Z', 'A', 'A', 'B', 110.0),
                    ('2024-01-01T00:01:00.000000Z', 'A', 'A', 'A', 120.0),
                    ('2024-01-01T00:01:30.000000Z', 'B', 'B', 'B', 210.0),
                    ('2024-01-01T00:02:00.000000Z', 'C', 'D', 'A', 310.0),
                    ('2024-01-02T00:00:00.000000Z', 'D', 'D', 'D', 400.0)
                """, ctx);
    }
}
