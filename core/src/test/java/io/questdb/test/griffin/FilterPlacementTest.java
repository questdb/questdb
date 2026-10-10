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

import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.griffin.plan.logical.WindowJoinPlan;
import io.questdb.griffin.plan.logical.WindowJoinStep;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Pins the operators that apply the filter of their input themselves, as operator planning records it on the operator,
 * and the factories the generator builds from that choice.
 */
public class FilterPlacementTest extends AbstractCairoTest {

    @Test
    public void testAsOfJoinStealsSlaveFilter() throws Exception {
        assertMemoryLeak(() -> {
            createJoinTables();
            final String expected = """
                    ts\tk\tv
                    1970-01-01T00:00:01.000000Z\t1\tnull
                    1970-01-01T00:00:02.000000Z\t2\t20
                    1970-01-01T00:00:03.000000Z\t1\t30
                    1970-01-01T00:00:04.000000Z\t2\t30
                    """;
            final String keyed = "SELECT a.ts, a.k, b.v FROM a ASOF JOIN (SELECT * FROM b WHERE v > 10) b";
            assertJoinAlgorithm(JoinInput.Algorithm.TEMPORAL_STOLEN_FILTER, keyed);
            assertQuery(keyed)
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("Filtered AsOf Join Fast")
                    .returns(expected);
            final String projected = "SELECT a.ts, a.k, b.v FROM a ASOF JOIN (SELECT v, ts FROM b WHERE v > 10) b";
            assertJoinAlgorithm(JoinInput.Algorithm.TEMPORAL_STOLEN_FILTER, projected);
            assertQuery(projected)
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("Filtered AsOf Join Fast")
                    .returns(expected);
            final String linear = "SELECT /*+ asof_linear(a b) */ a.ts, a.k, b.v FROM a ASOF JOIN (SELECT * FROM b WHERE v > 10) b";
            assertJoinAlgorithm(JoinInput.Algorithm.TEMPORAL, linear);
            assertQuery(linear)
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected);
            assertJoinAlgorithm(JoinInput.Algorithm.TEMPORAL, "SELECT a.ts, a.k, b.v FROM a LT JOIN (SELECT * FROM b WHERE v > 10) b");
            final String gated = "SELECT a.ts, a.k, b.v FROM a ASOF JOIN (SELECT * FROM b WHERE now() < '2100-01-01') b";
            assertJoinAlgorithm(JoinInput.Algorithm.TEMPORAL_STOLEN_FILTER, gated);
            assertQuery(gated)
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("Filtered AsOf Join Fast")
                    .returns("""
                            ts\tk\tv
                            1970-01-01T00:00:01.000000Z\t1\t10
                            1970-01-01T00:00:02.000000Z\t2\t20
                            1970-01-01T00:00:03.000000Z\t1\t30
                            1970-01-01T00:00:04.000000Z\t2\t5
                            """);
            assertQuery("SELECT a.ts, a.k, b.v FROM a ASOF JOIN (SELECT * FROM b WHERE v > 10) b ON (k)")
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("Filtered AsOf Join Fast")
                    .returns("""
                            ts\tk\tv
                            1970-01-01T00:00:01.000000Z\t1\tnull
                            1970-01-01T00:00:02.000000Z\t2\t20
                            1970-01-01T00:00:03.000000Z\t1\t30
                            1970-01-01T00:00:04.000000Z\t2\t20
                            """);
        });
    }

    @Test
    public void testCoveringResidualNotStolenBySerialFilter() throws Exception {
        assertMemoryLeak(() -> {
            createCoveringTable();
            sqlExecutionContext.setParallelFilterEnabled(false);
            final String sql = "SELECT sym, price FROM c WHERE sym = 'a' AND price > 1 ORDER BY price DESC LIMIT 2";
            assertSortAlgorithm(SortPlan.Algorithm.MATERIALIZED, sql);
            assertQuery(sql)
                    .withPlan("""
                            Limit value: 2 skip-rows-max: 0 take-rows-max: 2
                                Encode sort
                                  keys: [price desc]
                                    Filter filter: 1<price
                                        CoveringIndex on: sym with: price
                                          filter: sym='a'
                            """)
                    .returns("""
                            sym\tprice
                            a\t4.0
                            a\t3.0
                            """);
        });
    }

    @Test
    public void testCoveringResidualStolenByTopK() throws Exception {
        assertMemoryLeak(() -> {
            createCoveringTable();
            final String sql = "SELECT sym, price FROM c WHERE sym = 'a' AND price > 1 ORDER BY price DESC LIMIT 2";
            assertSortAlgorithm(SortPlan.Algorithm.PARALLEL_FILTERED_TOP_K, sql);
            assertQuery(sql)
                    .expectSize()
                    .withPlan("""
                            Async Top K lo: 2 workers: 1
                              filter: 1<price
                              keys: [price desc]
                                CoveringIndex on: sym with: price
                                  filter: sym='a'
                            """)
                    .returns("""
                            sym\tprice
                            a\t4.0
                            a\t3.0
                            """);
        });
    }

    @Test
    public void testGroupByStealsFilter() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String sql = "SELECT x % 2 k, count() c FROM t WHERE x > 1 ORDER BY k";
            assertAggregateAlgorithm(AggregatePlan.Algorithm.PARALLEL_STOLEN_FILTER, sql);
            for (int jitMode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                sqlExecutionContext.setJitMode(jitMode);
                assertQuery(sql)
                        .expectSize()
                        .withPlanContaining(jitMode == SqlJitMode.JIT_MODE_DISABLED ? "Async Group By" : "Async JIT Group By")
                        .returns("""
                                k\tc
                                0\t2
                                1\t1
                                """);
            }
        });
    }

    @Test
    public void testGroupByStealsSerialScanFilter() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            sqlExecutionContext.setParallelFilterEnabled(false);
            final String sql = "SELECT sum(x) s FROM t WHERE s ~ '[13]'";
            assertAggregateAlgorithm(AggregatePlan.Algorithm.PARALLEL_STOLEN_FILTER, sql);
            assertQuery(sql)
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("Async Group By")
                    .returns("""
                            s
                            4
                            """);
        });
    }

    @Test
    public void testGroupByStealsSymbolPatternFilter() throws Exception {
        assertMemoryLeak(() -> {
            createPatternTable();
            final String sql = "SELECT count() c, sum(x) s FROM p WHERE s LIKE '1%'";
            assertAggregateAlgorithm(AggregatePlan.Algorithm.PARALLEL_STOLEN_FILTER, sql);
            assertQuery(sql)
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            c\ts
                            3\t22
                            """);
        });
    }

    @Test
    public void testHorizonJoinStealsMasterFilter() throws Exception {
        assertMemoryLeak(() -> {
            createTradeTables();
            sqlExecutionContext.setParallelHorizonJoinEnabled(true);
            final String sql = """
                    SELECT t.sym, avg(p.px) a
                    FROM trades AS t
                    HORIZON JOIN prices AS p ON (t.sym = p.sym)
                    RANGE FROM 0s TO 0s STEP 1s AS h
                    WHERE t.price > 150
                    ORDER BY t.sym
                    """;
            assertAggregateAlgorithm(AggregatePlan.Algorithm.PARALLEL_STOLEN_FILTER, sql);
            assertQuery(sql)
                    .expectSize()
                    .withPlanContaining("Async JIT Horizon Join")
                    .returns("""
                            sym\ta
                            b\t2.0
                            """);
        });
    }

    @Test
    public void testTopKKeepsGateAsPageFrameSource() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String sql = "SELECT * FROM t WHERE now() < '2100-01-01' ORDER BY x DESC LIMIT 2";
            assertSortAlgorithm(SortPlan.Algorithm.PARALLEL_TOP_K, sql);
            assertQuery(sql)
                    .expectSize()
                    .returns("""
                            ts\tx\ts
                            2024-01-02T00:00:00.000000Z\t4\t4
                            2024-01-01T02:00:00.000000Z\t3\t3
                            """);
        });
    }

    @Test
    public void testTopKSkipsSymbolPatternWithoutParallelFilter() throws Exception {
        assertMemoryLeak(() -> {
            createPatternTable();
            sqlExecutionContext.setParallelFilterEnabled(false);
            sqlExecutionContext.setParallelTopKEnabled(true);
            final String sql = "SELECT * FROM p WHERE s LIKE '1%' ORDER BY x LIMIT 3";
            assertSortAlgorithm(SortPlan.Algorithm.LIMITED, sql);
            assertQuery(sql)
                    .expectSize()
                    .returns("""
                            s\tx\tts
                            1\t1\t1970-01-01T00:00:01.000000Z
                            10\t10\t1970-01-01T00:00:10.000000Z
                            11\t11\t1970-01-01T00:00:11.000000Z
                            """);
        });
    }

    @Test
    public void testTopKStealsFilter() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String sql = "SELECT * FROM t WHERE x > 1 ORDER BY x DESC LIMIT 2";
            assertSortAlgorithm(SortPlan.Algorithm.PARALLEL_FILTERED_TOP_K, sql);
            for (int jitMode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                sqlExecutionContext.setJitMode(jitMode);
                assertQuery(sql)
                        .expectSize()
                        .withPlanContaining(jitMode == SqlJitMode.JIT_MODE_DISABLED ? "Async Top K" : "Async JIT Top K")
                        .returns("""
                                ts\tx\ts
                                2024-01-02T00:00:00.000000Z\t4\t4
                                2024-01-01T02:00:00.000000Z\t3\t3
                                """);
            }
        });
    }

    @Test
    public void testTopKStealsFilterUnderProjection() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String computed = "SELECT x + 1 xp, x FROM t WHERE x > 1 ORDER BY x DESC LIMIT 2";
            assertSortAlgorithm(SortPlan.Algorithm.PARALLEL_FILTERED_TOP_K, computed);
            assertQuery(computed)
                    .expectSize()
                    .withPlanContaining("VirtualRecord")
                    .returns("""
                            xp\tx
                            5\t4
                            4\t3
                            """);
            final String selected = "SELECT x, ts FROM t WHERE x > 1 ORDER BY x LIMIT 2";
            assertSortAlgorithm(SortPlan.Algorithm.PARALLEL_FILTERED_TOP_K, selected);
            assertQuery(selected)
                    .expectSize()
                    .returns("""
                            x\tts
                            2\t2024-01-01T01:00:00.000000Z
                            3\t2024-01-01T02:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testTopKStealsNonThreadSafeFilter() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String sql = "SELECT * FROM t WHERE s ~ '[134]' ORDER BY x DESC LIMIT 2";
            assertSortAlgorithm(SortPlan.Algorithm.PARALLEL_FILTERED_TOP_K, sql);
            try (SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 4)) {
                context.with(sqlExecutionContext.getSecurityContext(), bindVariableService);
                assertQuery(sql)
                        .withContext(context)
                        .expectSize()
                        .returns("""
                                ts\tx\ts
                                2024-01-02T00:00:00.000000Z\t4\t4
                                2024-01-01T02:00:00.000000Z\t3\t3
                                """);
            }
        });
    }

    @Test
    public void testTopKStealsResidualOfIntervalScan() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String sql = "SELECT * FROM t WHERE ts IN '2024-01-01' AND x > 1 ORDER BY x DESC LIMIT 2";
            assertSortAlgorithm(SortPlan.Algorithm.PARALLEL_FILTERED_TOP_K, sql);
            assertQuery(sql)
                    .expectSize()
                    .withPlanContaining("filter: 1<x")
                    .returns("""
                            ts\tx\ts
                            2024-01-01T02:00:00.000000Z\t3\t3
                            2024-01-01T01:00:00.000000Z\t2\t2
                            """);
        });
    }

    @Test
    public void testTopKStealsSymbolPatternFilter() throws Exception {
        assertMemoryLeak(() -> {
            createPatternTable();
            sqlExecutionContext.setParallelTopKEnabled(true);
            final String sql = "SELECT * FROM p WHERE s LIKE '1%' ORDER BY x LIMIT 3";
            assertSortAlgorithm(SortPlan.Algorithm.PARALLEL_FILTERED_TOP_K, sql);
            assertQuery(sql)
                    .expectSize()
                    .returns("""
                            s\tx\tts
                            1\t1\t1970-01-01T00:00:01.000000Z
                            10\t10\t1970-01-01T00:00:10.000000Z
                            11\t11\t1970-01-01T00:00:11.000000Z
                            """);
        });
    }

    @Test
    public void testWindowJoinStealsMasterFilter() throws Exception {
        assertMemoryLeak(() -> {
            createTradeTables();
            final String sql = """
                    SELECT t.ts, t.price, sum(p.px) s
                    FROM (SELECT * FROM trades WHERE price > 150) t
                    WINDOW JOIN prices p ON (t.sym = p.sym)
                    RANGE BETWEEN 1 MINUTE PRECEDING AND 1 MINUTE FOLLOWING EXCLUDE PREVAILING
                    """;
            assertPlanned(sql, WindowJoinPlan.class,
                    join -> Assert.assertEquals(WindowJoinStep.Algorithm.PARALLEL_STOLEN_FILTER, join.getSteps().getQuick(0).getAlgorithm()));
            assertQuery(sql)
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("""
                            ts\tprice\ts
                            2022-01-01T00:01:00.000000Z\t200.0\t2.0
                            """);
        });
    }

    private static void assertAggregateAlgorithm(AggregatePlan.Algorithm expected, String sql) throws Exception {
        assertPlanned(sql, AggregatePlan.class, aggregate -> Assert.assertEquals(expected, aggregate.getAlgorithm()));
    }

    private static void assertJoinAlgorithm(JoinInput.Algorithm expected, String sql) throws Exception {
        assertPlanned(sql, JoinPlan.class, join -> Assert.assertEquals(expected, join.getOrderedInputs().getQuick(1).getAlgorithm()));
    }

    private static <T extends LogicalPlan> void assertPlanned(String sql, Class<T> type, PlanAssertion<T> assertion) throws Exception {
        try (
                SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                RecordCursorFactory ignore = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()
        ) {
            final LogicalPlan found = find(compiler.getPlanForTesting(), type);
            Assert.assertNotNull(found);
            assertion.check(type.cast(found));
        }
    }

    private static void assertSortAlgorithm(SortPlan.Algorithm expected, String sql) throws Exception {
        assertPlanned(sql, SortPlan.class, sort -> Assert.assertEquals(expected, sort.getAlgorithm()));
    }

    private static void createCoveringTable() throws Exception {
        execute("CREATE TABLE c (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
        execute("ALTER TABLE c ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price)");
        execute("""
                INSERT INTO c VALUES
                    ('2024-01-01T00:00:00.000000Z', 'a', 1.0),
                    ('2024-01-01T01:00:00.000000Z', 'b', 5.0),
                    ('2024-01-01T02:00:00.000000Z', 'a', 3.0),
                    ('2024-01-02T00:00:00.000000Z', 'a', 4.0),
                    ('2024-01-02T01:00:00.000000Z', 'a', 2.0)
                """);
    }

    private static void createJoinTables() throws Exception {
        execute("CREATE TABLE a (ts TIMESTAMP, k INT) TIMESTAMP(ts) PARTITION BY DAY");
        execute("""
                INSERT INTO a VALUES
                    ('1970-01-01T00:00:01.000000Z', 1),
                    ('1970-01-01T00:00:02.000000Z', 2),
                    ('1970-01-01T00:00:03.000000Z', 1),
                    ('1970-01-01T00:00:04.000000Z', 2)
                """);
        execute("CREATE TABLE b (ts TIMESTAMP, k INT, v INT) TIMESTAMP(ts) PARTITION BY DAY");
        execute("""
                INSERT INTO b VALUES
                    ('1970-01-01T00:00:00.500000Z', 1, 10),
                    ('1970-01-01T00:00:01.500000Z', 2, 20),
                    ('1970-01-01T00:00:02.500000Z', 1, 30),
                    ('1970-01-01T00:00:03.500000Z', 2, 5)
                """);
    }

    private static void createPatternTable() throws Exception {
        execute("CREATE TABLE p (s SYMBOL INDEX, x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO p SELECT (x % 12)::SYMBOL, x::INT, (x * 1_000_000)::TIMESTAMP FROM long_sequence(12)");
    }

    private static void createTables() throws Exception {
        execute("CREATE TABLE t (ts TIMESTAMP, x LONG, s STRING) TIMESTAMP(ts) PARTITION BY DAY");
        execute("""
                INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 1, '1'),
                    ('2024-01-01T01:00:00.000000Z', 2, '2'),
                    ('2024-01-01T02:00:00.000000Z', 3, '3'),
                    ('2024-01-02T00:00:00.000000Z', 4, '4')
                """);
    }

    private static void createTradeTables() throws Exception {
        execute("CREATE TABLE trades (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE prices (sym SYMBOL, px DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO trades VALUES ('a', 100.0, '2022-01-01T00:00:00.000000Z'), ('b', 200.0, '2022-01-01T00:01:00.000000Z')");
        execute("INSERT INTO prices VALUES ('a', 1.0, '2022-01-01T00:00:00.000000Z'), ('b', 2.0, '2022-01-01T00:01:00.000000Z')");
    }

    private static LogicalPlan find(LogicalPlan plan, Class<? extends LogicalPlan> type) {
        if (type.isInstance(plan)) {
            return plan;
        }
        if (plan instanceof JoinPlan join) {
            for (int i = 0, n = join.getOrderedInputs().size(); i < n; i++) {
                final LogicalPlan input = join.getOrderedInputs().getQuick(i).getInput();
                final LogicalPlan found = input == null ? null : find(input, type);
                if (found != null) {
                    return found;
                }
            }
            return null;
        }
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            final LogicalPlan found = find(plan.inputAt(i), type);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    @FunctionalInterface
    private interface PlanAssertion<T> {
        void check(T plan);
    }
}
