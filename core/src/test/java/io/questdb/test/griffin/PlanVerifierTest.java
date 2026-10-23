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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.optimiser.PlanVerifier;
import io.questdb.griffin.optimiser.DecorrelationCarriers;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.CursorExpression;
import io.questdb.griffin.plan.logical.FillPlan;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.FunctionSourcePlan;
import io.questdb.griffin.plan.logical.HashJoinAggregation;
import io.questdb.griffin.plan.logical.HorizonJoinPlan;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinKind;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LatestByPlan;
import io.questdb.griffin.plan.logical.LimitPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OuterColumnExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.PlanExpressionVisitor;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.SampleByPlan;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.griffin.plan.logical.SetOperationKind;
import io.questdb.griffin.plan.logical.SetOperationPlan;
import io.questdb.griffin.plan.logical.SortDirection;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.griffin.plan.logical.WindowJoinPlan;
import io.questdb.griffin.plan.logical.WindowJoinStep;
import io.questdb.griffin.plan.logical.WindowPlan;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class PlanVerifierTest extends AbstractCairoTest {
    private static final String PASS = "TestPass.rewrite";
    private final DecorrelationCarriers carriers = new DecorrelationCarriers();
    private final PlanVerifier verifier = PlanVerifier.newStandalone();

    @Test
    public void testAccessPathUnplanned() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE unplanned (a INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            try (
                    SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                    RecordCursorFactory ignored = compiler.compile("SELECT a FROM unplanned WHERE ts > '2024-01-01'", sqlExecutionContext).getRecordCursorFactory()
            ) {
                final LogicalPlan plan = compiler.getPlanForTesting();
                LogicalPlan node = plan;
                while (!(node instanceof ScanPlan)) {
                    node = node.inputAt(0);
                }
                verifier.verifyAccessPaths(plan);
                ((ScanPlan) node).clearAccessPath();
                verifier.verify(plan, PASS);
                try {
                    verifier.verifyAccessPaths(plan);
                    Assert.fail("expected " + PlanVerifier.ACCESS_PATH_UNPLANNED);
                } catch (AssertionError e) {
                    TestUtils.assertContains(e.getMessage(), PlanVerifier.ACCESS_PATH_UNPLANNED);
                    TestUtils.assertContains(e.getMessage(), " at ScanPlan position ");
                }
            }
        });
    }

    @Test
    public void testAggregateHashJoinContradictsItsInputs() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE ha (k INT, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE hb (k INT, w DOUBLE)");
            // The fused GROUP BY steals the parallel filter of ha, whose factory then serves no page frames whole.
            assertAlgorithmContradicts("SELECT count(*), sum(hb.w) FROM ha JOIN hb ON ha.k = hb.k WHERE ha.v > 0", AggregatePlan.class, aggregate -> {
                final HashJoinAggregation hashJoin = aggregate.getHashJoin();
                Assert.assertNotNull(hashJoin);
                Assert.assertEquals(HashJoinAggregation.InputRead.STOLEN_FILTER, hashJoin.getProbeRead());
                hashJoin.of(hashJoin.getAggregate(), hashJoin.getJoinType(), hashJoin.getBuildSide(),
                        hashJoin.getProbeInput(), HashJoinAggregation.InputRead.PAGE_FRAMES, hashJoin.getProbeAlias(),
                        hashJoin.getBuildInput(), hashJoin.getBuildRead(), hashJoin.getBuildAlias());
            }, PlanVerifier.AGGREGATE_ALGORITHM);
            // A LEFT join builds the input it NULL-extends, its slave.
            assertAlgorithmContradicts("SELECT count(*), sum(hb.w) FROM ha LEFT JOIN hb ON ha.k = hb.k", AggregatePlan.class, aggregate -> {
                final HashJoinAggregation hashJoin = aggregate.getHashJoin();
                Assert.assertNotNull(hashJoin);
                Assert.assertEquals(HashJoinAggregation.BuildSide.SLAVE, hashJoin.getBuildSide());
                hashJoin.of(hashJoin.getAggregate(), hashJoin.getJoinType(), HashJoinAggregation.BuildSide.MASTER,
                        hashJoin.getProbeInput(), hashJoin.getProbeRead(), hashJoin.getProbeAlias(),
                        hashJoin.getBuildInput(), hashJoin.getBuildRead(), hashJoin.getBuildAlias());
            }, PlanVerifier.AGGREGATE_ALGORITHM);
            // The other build is recorded only when an interval scan can change which input holds fewer rows.
            assertAlgorithmContradicts("SELECT count(*), sum(hb.w) FROM ha JOIN hb ON ha.k = hb.k", AggregatePlan.class, aggregate -> {
                final HashJoinAggregation hashJoin = aggregate.getHashJoin();
                Assert.assertNotNull(hashJoin);
                Assert.assertNull(hashJoin.getAlternate());
                hashJoin.setAlternate(hashJoin);
            }, PlanVerifier.AGGREGATE_ALGORITHM);
        });
    }

    @Test
    public void testAggregateParallelWithoutParallelFunctions() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE grouped (s STRING, x INT)");
            assertAlgorithmContradicts("SELECT string_agg(s, ',') FROM grouped", AggregatePlan.class, aggregate -> {
                Assert.assertEquals(AggregatePlan.Algorithm.SERIAL, aggregate.getAlgorithm());
                aggregate.setAlgorithm(AggregatePlan.Algorithm.PARALLEL);
            }, PlanVerifier.AGGREGATE_ALGORITHM);
        });
    }

    @Test
    public void testAggregateParallelWithoutPageFrames() throws Exception {
        assertMemoryLeak(() -> assertAlgorithmContradicts("SELECT x, sum(x) FROM long_sequence(3)", AggregatePlan.class, aggregate -> {
            Assert.assertEquals(AggregatePlan.Algorithm.SERIAL, aggregate.getAlgorithm());
            aggregate.setAlgorithm(AggregatePlan.Algorithm.PARALLEL);
        }, PlanVerifier.AGGREGATE_ALGORITHM));
    }

    @Test
    public void testAggregateShape() {
        final FunctionSourcePlan source = source();
        final AggregatePlan aggregate = AggregatePlan.FACTORY.newInstance().of(source, 7);
        aggregate.getGroupingExpressions().add(column(1, ColumnType.INT));
        assertFails(aggregate, PlanVerifier.AGGREGATE_SHAPE, "AggregatePlan");
    }

    @Test
    public void testAggregateVectorisedWithoutVectorShape() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE grouped (s STRING, sym SYMBOL, x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            assertAlgorithmContradicts("SELECT s, max(x) FROM grouped", AggregatePlan.class,
                    aggregate -> aggregate.setAlgorithm(AggregatePlan.Algorithm.VECTORISED), PlanVerifier.AGGREGATE_ALGORITHM);
        });
    }

    @Test
    public void testAlgorithmContradictsNode() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE sampled (sym SYMBOL INDEX, x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            assertAlgorithmContradicts("SELECT ts, x, sum(x) OVER (ORDER BY x) s FROM sampled", WindowPlan.class,
                    window -> window.setAlgorithm(WindowPlan.Algorithm.STREAMING), PlanVerifier.WINDOW_ALGORITHM);
            assertAlgorithmContradicts("SELECT ts, sym, first(x) f FROM sampled SAMPLE BY 1h ALIGN TO FIRST OBSERVATION",
                    SampleByPlan.class, sample -> sample.setAlgorithm(SampleByPlan.Algorithm.INTERPOLATE), PlanVerifier.SAMPLE_BY_ALGORITHM);
            assertAlgorithmContradicts("SELECT ts, sym, first(x) f FROM sampled SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR", FillPlan.class,
                    fill -> fill.setAlgorithm(FillPlan.Algorithm.SAMPLE_BY_ROWS), PlanVerifier.FILL_ALGORITHM);
        });
    }

    @Test
    public void testAlgorithmUnplanned() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE sampled (sym SYMBOL INDEX, x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            assertAlgorithmUnplanned("SELECT ts, x, sum(x) OVER (ORDER BY x) s FROM sampled", WindowPlan.class,
                    window -> window.setAlgorithm(null));
            assertAlgorithmUnplanned("SELECT ts, sym, first(x) f FROM sampled WHERE sym = 'S' SAMPLE BY 1h ALIGN TO FIRST OBSERVATION",
                    SampleByPlan.class, sample -> sample.setAlgorithm(null));
            assertAlgorithmUnplanned("SELECT ts, sym, first(x) f FROM sampled SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR", FillPlan.class,
                    fill -> fill.setAlgorithm(null));
        });
    }

    @Test
    public void testBoundedSortTopKWithLimitHi() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE grouped (s STRING, sym SYMBOL, x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            try (
                    SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                    RecordCursorFactory ignored = compiler.compile("SELECT x FROM grouped ORDER BY x LIMIT 5, 10", sqlExecutionContext).getRecordCursorFactory()
            ) {
                final LogicalPlan plan = compiler.getPlanForTesting();
                final SortPlan sort = find(plan, SortPlan.class);
                Assert.assertEquals(LimitPlan.Application.SORT, find(plan, LimitPlan.class).getApplication());
                Assert.assertEquals(SortPlan.Algorithm.LIMITED, sort.getAlgorithm());
                verifier.verifyAccessPaths(plan);
                sort.setAlgorithm(SortPlan.Algorithm.LONG_TOP_K);
                assertFails(plan, PlanVerifier.LIMIT_APPLICATION, "LimitPlan");
            }
        });
    }

    @Test
    public void testBroadSqlSamplePasses() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (sym SYMBOL, price DOUBLE, qty LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE quotes (sym SYMBOL, bid DOUBLE, ask DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE tags (sym SYMBOL, tag STRING, ids DOUBLE[])");
            execute("""
                    INSERT INTO trades VALUES ('a', 1.0, 10, '2024-01-01T00:00:00.000000Z'), ('b', 2.0, 20, '2024-01-01T00:00:01.000000Z'),
                    ('a', 3.0, 30, '2024-01-01T00:00:02.000000Z')
                    """);
            execute("INSERT INTO quotes VALUES ('a', 0.9, 1.1, '2024-01-01T00:00:00.500000Z'), ('b', 1.9, 2.1, '2024-01-01T00:00:01.500000Z')");
            execute("INSERT INTO tags VALUES ('a', 'x', ARRAY[1.0, 2.0]), ('b', 'y', ARRAY[3.0])");
            final String[] queries = {
                    "SELECT * FROM trades",
                    "SELECT sym, price * qty AS notional FROM trades WHERE price > 1 ORDER BY notional DESC LIMIT 2",
                    "SELECT sym, sum(price), count() FROM trades GROUP BY sym ORDER BY sym",
                    "SELECT sym, avg(price), last(ts) FROM trades SAMPLE BY 1s FILL(PREV)",
                    "SELECT sym, max(ts) FROM trades",
                    "SELECT DISTINCT sym FROM trades ORDER BY sym",
                    "SELECT sym, row_number() OVER (PARTITION BY sym ORDER BY ts) rn, sum(qty) OVER (ORDER BY ts) running FROM trades",
                    "SELECT t.sym, t.price, q.bid FROM trades t ASOF JOIN quotes q ON sym",
                    "SELECT t.sym, q.ask FROM trades t JOIN quotes q ON t.sym = q.sym WHERE q.ask > 1 AND t.qty > 5",
                    "SELECT t.sym, q.ask FROM trades t LEFT JOIN quotes q ON t.sym = q.sym AND q.bid > 1",
                    "SELECT t.sym, g.tag FROM trades t CROSS JOIN tags g WHERE t.sym = g.sym",
                    "SELECT sym FROM trades UNION SELECT sym FROM quotes ORDER BY 1",
                    "SELECT sym, price FROM trades UNION ALL SELECT sym, bid FROM quotes",
                    "SELECT sym FROM trades WHERE sym IN (SELECT sym FROM quotes WHERE bid > 1)",
                    "SELECT * FROM trades LATEST ON ts PARTITION BY sym",
                    "SELECT sym, price FROM trades ORDER BY ts DESC LIMIT -2",
                    "SELECT t.sym, l.total FROM trades t JOIN LATERAL (SELECT sum(bid) total FROM quotes q WHERE q.sym = t.sym) l ON true",
                    "SELECT t.sym, l.n FROM trades t LEFT JOIN LATERAL (SELECT count() n FROM quotes q WHERE q.sym = t.sym AND q.ask > t.price) l ON true",
                    "SELECT g.sym, u.v FROM tags g, UNNEST(g.ids) AS u(v)",
                    "SELECT * FROM (SELECT sym, sum(price) s FROM trades) WHERE s > 1",
                    "SELECT sym, s + 1 FROM (SELECT sym, sum(qty * 2) s FROM trades GROUP BY sym) ORDER BY sym",
                    "SELECT * FROM trades PIVOT (sum(price) FOR sym IN ('a', 'b'))",
                    "SELECT t.sym, count(q.bid) FROM trades t WINDOW JOIN quotes q RANGE BETWEEN 1 SECOND PRECEDING AND CURRENT ROW",
                    "SELECT sym, first(price), last(price) FROM trades SAMPLE BY 1s ALIGN TO CALENDAR WITH OFFSET '00:30'",
                    "SELECT ts, price FROM trades WHERE ts IN '2024-01-01' AND sym = 'a' ORDER BY ts",
                    "SELECT * FROM (SELECT sym, max(price) - min(price) spread, max(price) top FROM trades WHERE qty > 5 GROUP BY sym) WHERE top > 1",
            };
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                for (String query : queries) {
                    try (RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                        Assert.assertNotNull(query, factory);
                        verifier.verify(compiler.getPlanForTesting(), PASS);
                    }
                }
            }
        });
    }

    @Test
    public void testChoiceUnplanned() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE chosen (a INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            try (
                    SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                    RecordCursorFactory ignored = compiler.compile("SELECT a FROM chosen WHERE a > 1 ORDER BY a", sqlExecutionContext).getRecordCursorFactory()
            ) {
                final LogicalPlan plan = compiler.getPlanForTesting();
                final SortPlan sort = (SortPlan) find(plan, SortPlan.class);
                final ScanPlan scan = (ScanPlan) find(plan, ScanPlan.class);
                verifier.verifyAccessPaths(plan);
                final SortPlan.Algorithm algorithm = sort.getAlgorithm();
                sort.setAlgorithm(null);
                verifier.verify(plan, PASS);
                assertAccessPathsFail(plan, PlanVerifier.CHOICE_UNPLANNED, "SortPlan");
                sort.setAlgorithm(algorithm);
                final FilterPlan.Algorithm residualAlgorithm = scan.getResidualAlgorithm();
                Assert.assertNotNull(residualAlgorithm);
                scan.setResidualAlgorithm(null);
                assertAccessPathsFail(plan, PlanVerifier.CHOICE_UNPLANNED, "ScanPlan");
                scan.setResidualAlgorithm(residualAlgorithm);
                verifier.verifyAccessPaths(plan);
            }
        });
    }

    @Test
    public void testColumnType() {
        final FunctionSourcePlan source = source();
        assertFails(filter(source, column(1, ColumnType.BOOLEAN)), PlanVerifier.COLUMN_TYPE, "FilterPlan");
    }

    @Test
    public void testDecorrelationCarrier() {
        final JoinPlan inner = join(source(), source(5, 6));
        final JoinInput right = inner.getInputs().getQuick(1);
        right.setJoinType(JoinKind.RIGHT_OUTER);
        carriers.add(right, 5);
        final JoinPlan outer = join(source(10, 11), inner);
        final JoinInput step = outer.getInputs().getQuick(1);
        carriers.add(step, 5);
        verifier.recordBarrierJoins(outer);
        verifier.verifyDecorrelation(outer, PASS, carriers);
        carriers.add(step, 1);
        verifier.recordBarrierJoins(outer);
        try {
            verifier.verifyDecorrelation(outer, PASS, carriers);
            Assert.fail("expected " + PlanVerifier.DECORRELATION_CARRIER);
        } catch (AssertionError e) {
            TestUtils.assertContains(e.getMessage(), PlanVerifier.DECORRELATION_CARRIER);
            TestUtils.assertContains(e.getMessage(), " at JoinPlan position ");
            TestUtils.assertContains(e.getMessage(), "after " + PASS);
        }
    }

    @Test
    public void testDecorrelationCarrierUnresolved() {
        final JoinPlan join = join(source(), source(5, 6));
        carriers.add(join.getInputs().getQuick(1), 9);
        verifier.recordBarrierJoins(join);
        assertDecorrelationFails(join, PlanVerifier.DECORRELATION_CARRIER_UNRESOLVED + " [column id 9]", "JoinPlan");
    }

    @Test
    public void testDecorrelationStepLost() {
        final JoinPlan join = join(source(), source(5, 6));
        final JoinInput step = join.getInputs().getQuick(1);
        step.setJoinType(JoinKind.LEFT_OUTER);
        verifier.recordBarrierJoins(join);
        verifier.verifyDecorrelation(join, PASS, carriers);
        verifier.recordBarrierJoins(join);
        join.getInputs().setQuick(1, JoinInput.FACTORY.newInstance().of(step.getInput(), JoinKind.LEFT_OUTER, "s", 0));
        assertDecorrelationFails(join, PlanVerifier.DECORRELATION_STEP_LOST, "JoinPlan");
    }

    @Test
    public void testDependentStepSurvivesDecorrelation() {
        final JoinPlan join = join(source(), source(5, 6));
        join.getInputs().getQuick(1).setDependent(true);
        verifier.verifyBound(join);
        assertFails(join, PlanVerifier.DEPENDENT_STEP, "JoinPlan");
    }

    @Test
    public void testDuplicateColumnId() {
        final FunctionSourcePlan source = source();
        source.getOutput().add(1, "again", ColumnType.LONG, true);
        assertFails(source, PlanVerifier.DUPLICATE_COLUMN_ID, "FunctionSourcePlan");
    }

    @Test
    public void testExpressionShared() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE shared_node (a INT, b INT)");
            try (
                    SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                    RecordCursorFactory ignored = compiler.compile("SELECT a FROM shared_node WHERE a = b", sqlExecutionContext).getRecordCursorFactory()
            ) {
                final LogicalPlan plan = compiler.getPlanForTesting();
                LogicalPlan node = plan;
                while (!(node instanceof FilterPlan)) {
                    node = node.inputAt(0);
                }
                final FunctionExpression predicate = (FunctionExpression) ((FilterPlan) node).getPredicate();
                verifier.verify(plan, PASS);
                predicate.getArguments().setQuick(1, predicate.argumentAt(0));
                assertFails(plan, PlanVerifier.EXPRESSION_SHARED, "ScanPlan");
            }
        });
    }

    @Test
    public void testExpressionSharedCursor() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE shared_cursor (a INT, ts TIMESTAMP) TIMESTAMP(ts)");
            try (
                    SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                    RecordCursorFactory ignored = compiler.compile(
                            "SELECT a FROM shared_cursor WHERE a > 0 AND ts > (SELECT min(ts) FROM shared_cursor)",
                            sqlExecutionContext
                    ).getRecordCursorFactory()
            ) {
                final LogicalPlan plan = compiler.getPlanForTesting();
                LogicalPlan node = plan;
                while (!(node instanceof FilterPlan)) {
                    node = node.inputAt(0);
                }
                final FunctionExpression predicate = (FunctionExpression) ((FilterPlan) node).getPredicate();
                final FunctionExpression comparison = (FunctionExpression) predicate.argumentAt(1);
                Assert.assertTrue(comparison.argumentAt(1) instanceof CursorExpression);
                comparison.getArguments().setQuick(0, comparison.argumentAt(1));
                verifier.verify(plan, PASS);
            }
        });
    }

    @Test
    public void testFillAlgorithmOverSampleByRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE grouped (s STRING, sym SYMBOL, x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            assertAlgorithmContradicts("SELECT ts, sym, first(x) f FROM grouped SAMPLE BY 1h FILL(PREV) ALIGN TO FIRST OBSERVATION",
                    FillPlan.class, fill -> {
                        Assert.assertEquals(FillPlan.Algorithm.SAMPLE_BY_ROWS, fill.getAlgorithm());
                        fill.setAlgorithm(FillPlan.Algorithm.SORTED);
                    }, PlanVerifier.FILL_ALGORITHM);
        });
    }

    @Test
    public void testFillTimestamp() {
        final FunctionSourcePlan source = source();
        source.getOutput().add(3, "ts", ColumnType.TIMESTAMP, true);
        final FillPlan fill = FillPlan.FACTORY.newInstance().of(source, 5);
        fill.setTimestampColumnId(3);
        fill.getOutput().copyFrom(source.getOutput());
        assertFails(fill, PlanVerifier.FILL_TIMESTAMP, "FillPlan");
        fill.deriveOutput();
        Assert.assertEquals(2, fill.getOutput().getTimestampIndex());
        verifier.verify(fill, PASS);
    }

    @Test
    public void testFilterAlgorithmWithoutChoice() {
        final FunctionSourcePlan source = source();
        final FilterPlan filter = filter(source, ConstantExpression.FACTORY.newInstance().ofBoolean(true, 0));
        verifier.verify(filter, PASS);
        filter.setAlgorithm(FilterPlan.Algorithm.SERIAL);
        assertFails(filter, PlanVerifier.FILTER_ALGORITHM, "FilterPlan");
    }

    @Test
    public void testFilterWindowKeepFlagWithoutKeepFlag() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE grouped (s STRING, sym SYMBOL, x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            assertAlgorithmContradicts("SELECT * FROM (SELECT ts, x, sum(x) OVER (ORDER BY x) s FROM grouped) WHERE s > 1", FilterPlan.class,
                    filter -> {
                        Assert.assertNotNull(filter.getAlgorithm());
                        filter.setAlgorithm(FilterPlan.Algorithm.WINDOW_KEEP_FLAG);
                    }, PlanVerifier.FILTER_ALGORITHM);
        });
    }

    @Test
    public void testJoinAlgorithmUnplanned() throws Exception {
        assertMemoryLeak(() -> {
            createTemporalTables();
            try (
                    SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                    RecordCursorFactory ignored = compiler.compile("SELECT a.ts, b.v FROM a ASOF JOIN b", sqlExecutionContext).getRecordCursorFactory()
            ) {
                final LogicalPlan plan = compiler.getPlanForTesting();
                final JoinInput step = find(plan, JoinPlan.class).getOrderedInputs().getQuick(1);
                Assert.assertEquals(JoinInput.Algorithm.TEMPORAL_TIME_FRAME, step.getAlgorithm());
                verifier.verifyAccessPaths(plan);
                step.setAlgorithm(null);
                step.setMasterSide(null);
                verifier.verify(plan, PASS);
                assertAccessPathsFail(plan, PlanVerifier.CHOICE_UNPLANNED, "JoinPlan");
            }
        });
    }

    @Test
    public void testJoinFullFatTemporalOverVarSizeSlaveColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (ts TIMESTAMP, k INT) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE b (ts TIMESTAMP, s STRING) TIMESTAMP(ts) PARTITION BY DAY");
            assertAlgorithmContradicts("SELECT a.ts, b.s FROM a ASOF JOIN b", JoinPlan.class, join -> {
                final JoinInput step = join.getOrderedInputs().getQuick(1);
                Assert.assertEquals(JoinInput.Algorithm.TEMPORAL_TIME_FRAME, step.getAlgorithm());
                step.setAlgorithm(JoinInput.Algorithm.FULL_FAT_TEMPORAL);
            }, PlanVerifier.JOIN_SLAVE_VALUE_TYPE);
        });
    }

    @Test
    public void testJoinMaterializedNestedLoopFullOuter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m (ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE b (k INT)");
            execute("CREATE TABLE c (k INT)");
            assertAlgorithmContradicts("SELECT * FROM (SELECT m.ts, b.k FROM m CROSS JOIN b JOIN c ON b.k = c.k) TIMESTAMP(ts)",
                    JoinPlan.class, join -> {
                        final JoinInput step = join.getOrderedInputs().getQuick(1);
                        Assert.assertEquals(JoinInput.Algorithm.MATERIALIZED_NESTED_LOOP, step.getAlgorithm());
                        step.setJoinType(JoinKind.FULL_OUTER);
                        join.getOutput().setTimestampIndex(-1);
                    }, PlanVerifier.JOIN_ALGORITHM);
        });
    }

    @Test
    public void testJoinFilterOrigin() {
        final JoinPlan join = join(source(), source(5, 6));
        join.getFilterConjuncts().add(column(2, ColumnType.BOOLEAN));
        join.getFilterConjunctOrigins().add(-1);
        join.getFilterConjuncts().add(column(6, ColumnType.BOOLEAN));
        join.getFilterConjunctOrigins().add(1);
        verifier.verify(join, PASS);
        join.getFilterConjunctOrigins().setQuick(1, 2);
        assertFails(join, PlanVerifier.JOIN_FILTER_ORIGIN, "JoinPlan");
        join.getFilterConjunctOrigins().setQuick(1, -2);
        assertFails(join, PlanVerifier.JOIN_FILTER_ORIGIN, "JoinPlan");
    }

    @Test
    public void testJoinOnKept() {
        final JoinPlan join = join(source(), source(5, 6));
        final JoinInput step = join.getInputs().getQuick(1);
        step.setJoinType(JoinKind.LEFT_OUTER);
        step.addKey(1, 5, "m.a", "s.a", 0);
        step.addKey(2, 6, "m.b", "s.b", 0);
        step.visitReads(new PlanExpressionVisitor() {
            @Override
            public int visitColumnId(int columnId, int position) {
                return switch (columnId) {
                    case 2 -> 1;
                    case 6 -> 5;
                    default -> columnId;
                };
            }

            @Override
            public BoundExpression visitExpression(BoundExpression expression) {
                return expression;
            }
        });
        step.addKey(1, 5, "m.a", "s.a", 0);
        Assert.assertEquals(2, step.getKeys().size());
        verifier.recordBarrierJoins(join);
        step.removeKey(1);
        step.addKey(1, 5, "m.a", "s.a", 0);
        Assert.assertEquals(1, step.getKeys().size());
        verifier.verifyDecorrelation(join, PASS, carriers);
        verifier.recordBarrierJoins(join);
        step.removeKey(0);
        try {
            verifier.verifyDecorrelation(join, PASS, carriers);
            Assert.fail("expected " + PlanVerifier.JOIN_ON_KEPT);
        } catch (AssertionError e) {
            TestUtils.assertContains(e.getMessage(), PlanVerifier.JOIN_ON_KEPT);
            TestUtils.assertContains(e.getMessage(), "after " + PASS);
        }
    }

    @Test
    public void testJoinOrder() {
        final JoinPlan join = join(source(), source(5, 6));
        join.getOrderedInputs().add(join.getInputs().getQuick(0));
        assertFails(join, PlanVerifier.JOIN_ORDER, "JoinPlan");
    }

    @Test
    public void testJoinOutput() {
        final JoinPlan join = join(source(), source(5, 6));
        join.getOutput().remove(0);
        assertFails(join, PlanVerifier.JOIN_OUTPUT, "JoinPlan");
    }

    @Test
    public void testJoinStolenFilterUnderLinearHint() throws Exception {
        assertMemoryLeak(() -> {
            createTemporalTables();
            assertAlgorithmContradicts("SELECT /*+ asof_linear(a f) */ a.ts, f.v FROM a ASOF JOIN (b WHERE v > 1) f", JoinPlan.class, join -> {
                final JoinInput step = join.getOrderedInputs().getQuick(1);
                Assert.assertEquals(JoinInput.Algorithm.TEMPORAL, step.getAlgorithm());
                step.setAlgorithm(JoinInput.Algorithm.TEMPORAL_STOLEN_FILTER);
            }, PlanVerifier.JOIN_ALGORITHM);
        });
    }

    @Test
    public void testJoinTimeFrameOverKeyedLtJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lt_master (ts TIMESTAMP, k INT) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE lt_slave (ts TIMESTAMP, k INT, v INT) TIMESTAMP(ts) PARTITION BY DAY");
            assertAlgorithmContradicts("SELECT m.ts, s.v FROM lt_master m LT JOIN lt_slave s ON (k)", JoinPlan.class, join -> {
                final JoinInput step = join.getOrderedInputs().getQuick(1);
                Assert.assertEquals(JoinInput.Algorithm.TEMPORAL, step.getAlgorithm());
                step.setAlgorithm(JoinInput.Algorithm.TEMPORAL_TIME_FRAME);
            }, PlanVerifier.JOIN_ALGORITHM);
        });
    }

    @Test
    public void testJoinTimeFrameWithoutTimeFrames() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            assertAlgorithmContradicts("SELECT a.ts, b.x FROM a ASOF JOIN (SELECT * FROM a LIMIT 3) b", JoinPlan.class, join -> {
                final JoinInput step = join.getOrderedInputs().getQuick(1);
                Assert.assertEquals(JoinInput.Algorithm.TEMPORAL, step.getAlgorithm());
                step.setAlgorithm(JoinInput.Algorithm.TEMPORAL_TIME_FRAME);
            }, PlanVerifier.JOIN_ALGORITHM);
        });
    }

    @Test
    public void testJoinTimeFrameUnderLinearHint() throws Exception {
        assertMemoryLeak(() -> {
            createTemporalTables();
            try (
                    SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                    RecordCursorFactory ignored = compiler.compile("SELECT /*+ asof_linear(a b) */ a.ts, b.v FROM a LT JOIN b", sqlExecutionContext)
                            .getRecordCursorFactory()
            ) {
                final LogicalPlan plan = compiler.getPlanForTesting();
                final JoinInput step = find(plan, JoinPlan.class).getOrderedInputs().getQuick(1);
                Assert.assertEquals(JoinInput.Algorithm.TEMPORAL, step.getAlgorithm());
                verifier.verify(plan, PASS);
                step.setAlgorithm(JoinInput.Algorithm.TEMPORAL_TIME_FRAME);
                assertFails(plan, PlanVerifier.JOIN_ALGORITHM, "JoinPlan");
            }
        });
    }

    @Test
    public void testLimitBound() {
        final FunctionSourcePlan source = source();
        final LimitPlan limit = LimitPlan.FACTORY.newInstance().of(source, column(1, ColumnType.INT), null, 3);
        limit.getOutput().copyFrom(source.getOutput());
        assertFails(limit, PlanVerifier.OPERAND_COLUMN, "LimitPlan");
    }

    @Test
    public void testLimitInputWithoutNativeFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE grouped (s STRING, sym SYMBOL, x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            assertAlgorithmContradicts("SELECT x FROM grouped LIMIT 3", LimitPlan.class, limit -> {
                Assert.assertEquals(LimitPlan.Application.OPERATOR, limit.getApplication());
                limit.setApplication(LimitPlan.Application.INPUT);
            }, PlanVerifier.LIMIT_APPLICATION);
        });
    }

    @Test
    public void testNodeShared() {
        final FunctionSourcePlan source = source();
        final SetOperationPlan union = SetOperationPlan.FACTORY.newInstance().of(source, source, SetOperationKind.UNION_ALL, 0, 0, false);
        union.getOutput().copyFrom(source.getOutput());
        assertFails(union, PlanVerifier.NODE_SHARED, "FunctionSourcePlan");
    }

    @Test
    public void testOuterColumnResolvesInPrecedingInput() {
        final FunctionSourcePlan master = source();
        final FunctionSourcePlan slave = source(5, 6);
        final JoinPlan join = join(master, filter(slave, OuterColumnExpression.FACTORY.newInstance().of(2, ColumnType.BOOLEAN, 0)));
        join.getInputs().getQuick(1).setDependent(true);
        verifier.verifyBound(join);
        join.getInputs().getQuick(1).setDependent(false);
        assertFails(join, PlanVerifier.OUTER_COLUMN_SCOPE, "FilterPlan");
    }

    @Test
    public void testOuterColumnUnresolved() {
        final JoinPlan join = join(source(), filter(source(5, 6), OuterColumnExpression.FACTORY.newInstance().of(9, ColumnType.BOOLEAN, 0)));
        join.getInputs().getQuick(1).setDependent(true);
        try {
            verifier.verifyBound(join);
            Assert.fail();
        } catch (AssertionError e) {
            TestUtils.assertContains(e.getMessage(), PlanVerifier.OUTER_COLUMN_UNRESOLVED);
            TestUtils.assertContains(e.getMessage(), "after SqlBinder.bind");
        }
    }

    @Test
    public void testOutputForwarding() {
        final FunctionSourcePlan source = source();
        final FilterPlan filter = filter(source, column(2, ColumnType.BOOLEAN));
        filter.getOutput().remove(0);
        assertFails(filter, PlanVerifier.OUTPUT_FORWARDING, "FilterPlan");
    }

    @Test
    public void testOutputForwardingAttributes() {
        final FunctionSourcePlan source = source();
        final FilterPlan filter = filter(source, column(2, ColumnType.BOOLEAN));
        filter.getOutput().setColumnName(0, "renamed", null);
        assertFails(filter, PlanVerifier.OUTPUT_FORWARDING + " [column id 1]", "FilterPlan");
        filter.deriveOutput();
        filter.getOutput().setSymbolTableStatic(1, true);
        assertFails(filter, PlanVerifier.OUTPUT_FORWARDING + " [column id 2]", "FilterPlan");
        filter.deriveOutput();
        filter.getOutput().protectName(1);
        assertFails(filter, PlanVerifier.OUTPUT_FORWARDING + " [column id 2]", "FilterPlan");
        filter.deriveOutput();
        verifier.verify(filter, PASS);
    }

    @Test
    public void testPredicateConstant() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE constant_predicate (a INT, b INT)");
            try (
                    SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                    RecordCursorFactory ignored = compiler.compile("SELECT a FROM constant_predicate WHERE a = b", sqlExecutionContext).getRecordCursorFactory()
            ) {
                final FunctionExpression comparison = (FunctionExpression) find(compiler.getPlanForTesting(), FilterPlan.class).getPredicate();
                final ObjList<BoundExpression> arguments = new ObjList<>();
                arguments.add(ConstantExpression.FACTORY.newInstance().ofInt(1, 0));
                arguments.add(ConstantExpression.FACTORY.newInstance().ofInt(1, 0));
                final FunctionExpression unfolded = FunctionExpression.FACTORY.newInstance().of(comparison, arguments, BoundExpression.CONSTANT);
                verifier.verify(filter(source(), ConstantExpression.FACTORY.newInstance().ofBoolean(true, 0)), PASS);
                assertFails(filter(source(), unfolded), PlanVerifier.PREDICATE_CONSTANT, "FilterPlan");
            }
        });
    }

    @Test
    public void testPredicateType() {
        final FunctionSourcePlan source = source();
        assertFails(filter(source, column(1, ColumnType.INT)), PlanVerifier.PREDICATE_TYPE, "FilterPlan");
    }

    @Test
    public void testProjectAliasReadsLaterColumn() {
        final ProjectPlan project = ProjectPlan.FACTORY.newInstance().of(source(), 4);
        project.getExpressions().add(column(4, ColumnType.INT));
        project.getExpressions().add(column(1, ColumnType.INT));
        project.getOutput().add(3, "x", ColumnType.INT, true);
        project.getOutput().add(4, "y", ColumnType.INT, true);
        assertFails(project, PlanVerifier.UNRESOLVED_COLUMN + " [column id 4]", "ProjectPlan");
        project.getExpressions().setQuick(0, column(1, ColumnType.INT));
        project.getExpressions().setQuick(1, column(3, ColumnType.INT));
        verifier.verify(project, PASS);
    }

    @Test
    public void testProjectAliasUnderUpdateConversions() {
        final ProjectPlan project = ProjectPlan.FACTORY.newInstance().of(source(), 4);
        project.getExpressions().add(column(1, ColumnType.INT));
        project.getExpressions().add(column(3, ColumnType.INT));
        project.getOutput().add(3, "x", ColumnType.INT, true);
        project.getOutput().add(4, "y", ColumnType.INT, true);
        verifier.verify(project, PASS);
        project.getUpdateTargetTypes().add(ColumnType.INT);
        project.getUpdateTargetTypes().add(ColumnType.INT);
        assertFails(project, PlanVerifier.UNRESOLVED_COLUMN + " [column id 3]", "ProjectPlan");
    }

    @Test
    public void testProjectShape() {
        final FunctionSourcePlan source = source();
        final ProjectPlan project = ProjectPlan.FACTORY.newInstance().of(source, 4);
        project.getExpressions().add(column(1, ColumnType.INT));
        assertFails(project, PlanVerifier.PROJECT_SHAPE, "ProjectPlan");
    }

    @Test
    public void testProjectType() {
        final FunctionSourcePlan source = source();
        final ProjectPlan project = ProjectPlan.FACTORY.newInstance().of(source, 4);
        project.getExpressions().add(column(1, ColumnType.INT));
        project.getOutput().add(3, "a", ColumnType.LONG, true);
        assertFails(project, PlanVerifier.PROJECT_TYPE, "ProjectPlan");
    }

    @Test
    public void testReplaceInputDerivesOutput() {
        final FilterPlan filter = filter(source(), column(2, ColumnType.BOOLEAN));
        final FunctionSourcePlan replacement = source(5, 2);
        replacement.getOutput().add(3, "ts", ColumnType.TIMESTAMP, true);
        replacement.getOutput().setTimestampIndex(2);
        filter.replaceInput(0, replacement);
        Assert.assertEquals(3, filter.getOutput().getColumnCount());
        Assert.assertEquals(5, filter.getOutput().getColumnId(0));
        Assert.assertEquals(2, filter.getOutput().getTimestampIndex());
        verifier.verify(filter, PASS);
        final SortPlan sort = SortPlan.FACTORY.newInstance().of(source(), 4);
        sort.getColumnIds().add(3);
        sort.getDirections().add(SortDirection.DESCENDING);
        sort.replaceInput(0, filter);
        Assert.assertEquals(2, sort.getOutput().getTimestampIndex());
        verifier.verify(sort, PASS);
        sort.getColumnIds().setQuick(0, 5);
        sort.replaceInput(0, filter);
        Assert.assertEquals(-1, sort.getOutput().getTimestampIndex());
        verifier.verify(sort, PASS);
    }

    @Test
    public void testRowIdReadOfInputWithoutRandomAccess() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE ra_a (k INT, x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE ra_b (k INT, x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            final String union = "(SELECT k, x, ts FROM ra_a UNION ALL SELECT k, x, ts FROM ra_b)";
            assertAlgorithmContradicts("SELECT k, x FROM " + union + " ORDER BY x", SortPlan.class, sort -> {
                Assert.assertEquals(SortPlan.Algorithm.MATERIALIZED, sort.getAlgorithm());
                sort.setAlgorithm(SortPlan.Algorithm.LIGHT);
            }, PlanVerifier.INPUT_RANDOM_ACCESS);
            assertAlgorithmContradicts("SELECT a.x, u.x FROM ra_a a JOIN " + union + " u ON a.k = u.k", JoinPlan.class, join -> {
                final JoinInput step = join.getOrderedInputs().getQuick(1);
                Assert.assertEquals(JoinInput.Algorithm.HASH, step.getAlgorithm());
                step.setAlgorithm(JoinInput.Algorithm.LIGHT_HASH);
            }, PlanVerifier.INPUT_RANDOM_ACCESS);
            assertAlgorithmContradicts("SELECT x, sum(x) OVER (ORDER BY k) s FROM " + union, WindowPlan.class, window -> {
                Assert.assertEquals(WindowPlan.Algorithm.CACHED, window.getAlgorithm());
                window.setAlgorithm(WindowPlan.Algorithm.CACHED_LIGHT);
            }, PlanVerifier.INPUT_RANDOM_ACCESS);
            assertAlgorithmContradicts("SELECT ts, k, x FROM (SELECT a.ts, a.k, b.x FROM ra_a a CROSS JOIN ra_b b) LATEST ON ts PARTITION BY k",
                    LatestByPlan.class, latest -> {
                        Assert.assertEquals(LatestByPlan.Algorithm.MATERIALIZED, latest.getAlgorithm());
                        latest.setAlgorithm(LatestByPlan.Algorithm.LIGHT);
                    }, PlanVerifier.INPUT_RANDOM_ACCESS);
        });
    }

    @Test
    public void testSampleByFirstLastWithoutIndex() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE grouped (x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            assertAlgorithmContradicts("SELECT ts, first(x) FROM grouped SAMPLE BY 1h ALIGN TO FIRST OBSERVATION", SampleByPlan.class, sample -> {
                Assert.assertEquals(SampleByPlan.Algorithm.FILL_NONE, sample.getAlgorithm());
                sample.setAlgorithm(SampleByPlan.Algorithm.FIRST_LAST_INDEX);
            }, PlanVerifier.SAMPLE_BY_ALGORITHM);
        });
    }

    @Test
    public void testSampleByFillValueWithoutFill() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE grouped (s STRING, sym SYMBOL, x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            assertAlgorithmContradicts("SELECT ts, first(x) f FROM grouped SAMPLE BY 1h ALIGN TO FIRST OBSERVATION", SampleByPlan.class, sample -> {
                Assert.assertEquals(SampleByPlan.Algorithm.FILL_NONE, sample.getAlgorithm());
                sample.setAlgorithm(SampleByPlan.Algorithm.FILL_VALUE);
            }, PlanVerifier.SAMPLE_BY_ALGORITHM);
        });
    }

    @Test
    public void testSetOperationShape() {
        final FunctionSourcePlan left = source();
        final FunctionSourcePlan right = source(5, 6);
        right.getOutput().remove(1);
        final SetOperationPlan union = SetOperationPlan.FACTORY.newInstance().of(left, right, SetOperationKind.UNION_ALL, 0, 0, false);
        union.getOutput().copyFrom(left.getOutput());
        assertFails(union, PlanVerifier.SET_OPERATION_SHAPE, "SetOperationPlan");
    }

    @Test
    public void testSharedSourceUnrecorded() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT x FROM long_sequence(5))");
            try (
                    SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                    RecordCursorFactory ignored = compiler.compile("""
                            SELECT o.x, r.y
                            FROM (SELECT x, string_agg(x::STRING, ',') s FROM t WHERE x <= 2 GROUP BY x) o
                            JOIN LATERAL (SELECT x y FROM t WHERE x <= o.x) r
                            """, sqlExecutionContext).getRecordCursorFactory()
            ) {
                final LogicalPlan plan = compiler.getPlanForTesting();
                verifier.verifySharedSources(plan, true);
                try {
                    verifier.verifySharedSources(plan, false);
                    Assert.fail("expected " + PlanVerifier.SHARED_SOURCE_UNRECORDED);
                } catch (AssertionError e) {
                    TestUtils.assertContains(e.getMessage(), PlanVerifier.SHARED_SOURCE_UNRECORDED);
                    TestUtils.assertContains(e.getMessage(), " at AggregatePlan position ");
                }
            }
        });
    }

    @Test
    public void testSortKeys() {
        final FunctionSourcePlan source = source();
        final SortPlan sort = SortPlan.FACTORY.newInstance().of(source, 2);
        sort.getOutput().copyFrom(source.getOutput());
        assertFails(sort, PlanVerifier.SORT_KEYS, "SortPlan");
    }

    @Test
    public void testSortTimestamp() {
        final FunctionSourcePlan source = source();
        source.getOutput().add(3, "ts", ColumnType.TIMESTAMP, true);
        final SortPlan sort = SortPlan.FACTORY.newInstance().of(source, 2);
        sort.getColumnIds().add(3);
        sort.getDirections().add(SortDirection.ASCENDING);
        sort.getOutput().copyFrom(source.getOutput());
        assertFails(sort, PlanVerifier.SORT_TIMESTAMP, "SortPlan");
        sort.deriveOutput();
        verifier.verify(sort, PASS);
    }

    @Test
    public void testTimestampReadAfterMasterSidesSettle() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE l (vk VARCHAR, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE r (vk VARCHAR, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            final String join = "l JOIN r ON r.vk = l.vk";
            assertMasterSideBindsTimestamp("SELECT count() FROM " + join + " SAMPLE BY 1d ALIGN TO FIRST OBSERVATION", "SampleByPlan");
            assertMasterSideBindsTimestamp("SELECT q.ts, a.v FROM (SELECT l.ts, l.vk FROM " + join + ") q ASOF JOIN r a ON (vk)", "JoinPlan");
            assertMasterSideBindsTimestamp("SELECT a.ts, q.v FROM r a LT JOIN (SELECT l.ts, l.v, l.vk FROM " + join + ") q ON (vk)", "JoinPlan");
            assertMasterSideBindsTimestamp("SELECT l.ts, sum(l.v) OVER (ORDER BY l.ts RANGE BETWEEN 1 HOUR PRECEDING AND CURRENT ROW) FROM " + join, "WindowPlan");
        });
    }

    @Test
    public void testTimestampType() {
        final FunctionSourcePlan source = source();
        source.getOutput().setTimestampIndex(0);
        assertFails(source, PlanVerifier.TIMESTAMP_TYPE, "FunctionSourcePlan");
    }

    @Test
    public void testUnresolvedColumn() {
        final FunctionSourcePlan source = source();
        assertFails(filter(source, column(9, ColumnType.BOOLEAN)), PlanVerifier.UNRESOLVED_COLUMN + " [column id 9]", "FilterPlan");
    }

    @Test
    public void testValidPlanPasses() {
        final FunctionSourcePlan source = source();
        verifier.verify(filter(source, column(2, ColumnType.BOOLEAN)), PASS);
    }

    @Test
    public void testWindowCachedWhenStreamed() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE grouped (s STRING, sym SYMBOL, x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            assertAlgorithmContradicts("SELECT ts, x, row_number() OVER () rn FROM grouped", WindowPlan.class, window -> {
                Assert.assertEquals(WindowPlan.Algorithm.STREAMING, window.getAlgorithm());
                window.setAlgorithm(WindowPlan.Algorithm.CACHED);
            }, PlanVerifier.WINDOW_ALGORITHM);
        });
    }

    @Test
    public void testWindowCachedLightWithoutEncodedKeys() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE grouped (b BINARY, x INT)");
            assertAlgorithmContradicts("SELECT b, sum(x) OVER (ORDER BY x) FROM grouped", WindowPlan.class, window -> {
                Assert.assertEquals(WindowPlan.Algorithm.CACHED_LIGHT, window.getAlgorithm());
                final OutputSchema input = window.getChild().getOutput();
                window.getSpecs().getQuick(0).getOrderByColumnIds().setQuick(0, input.getColumnId(input.getColumnIndexQuiet("b")));
            }, PlanVerifier.WINDOW_ALGORITHM);
        });
    }

    @Test
    public void testWindowJoinParallelWithoutMasterPageFrames() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            assertAlgorithmContradicts("""
                    SELECT a.ts, sum(b.x) FROM (SELECT * FROM a LIMIT 3) a
                    WINDOW JOIN a b RANGE BETWEEN 1 SECOND PRECEDING AND CURRENT ROW
                    """, WindowJoinPlan.class, join -> {
                final WindowJoinStep step = join.getSteps().getQuick(0);
                Assert.assertEquals(WindowJoinStep.Algorithm.SERIAL, step.getAlgorithm());
                step.setAlgorithm(WindowJoinStep.Algorithm.PARALLEL);
            }, PlanVerifier.WINDOW_JOIN_ALGORITHM);
        });
    }

    @Test
    public void testHorizonProjectionChoicesAndLayout() throws Exception {
        assertMemoryLeak(() -> {
            createTemporalTables();
            sqlExecutionContext.setParallelHorizonJoinEnabled(true);
            final String sql = "SELECT a.ts, h.offset, b.v FROM a a HORIZON JOIN b b LIST (0s, 1s) AS h";
            assertAlgorithmUnplanned(sql, HorizonJoinPlan.class, horizon -> horizon.setAlgorithm(null));
            assertAlgorithmContradicts(sql, HorizonJoinPlan.class,
                    horizon -> horizon.setAlgorithm(HorizonJoinPlan.Algorithm.PARALLEL_STOLEN_FILTER), PlanVerifier.HORIZON_ALGORITHM);
            assertAlgorithmContradicts(sql, HorizonJoinPlan.class,
                    horizon -> horizon.setConsumer(HorizonJoinPlan.Consumer.AGGREGATE), PlanVerifier.HORIZON_CONSUMER);
            assertAlgorithmContradicts(sql, HorizonJoinPlan.class,
                    horizon -> horizon.getOutput().remove(horizon.getMaster().getOutput().getColumnCount()), PlanVerifier.HORIZON_JOIN_SHAPE);
            assertAlgorithmContradicts("SELECT h.offset FROM a a HORIZON JOIN b b RANGE FROM 0s TO 100s STEP 1s AS h",
                    HorizonJoinPlan.class, horizon -> {
                        Assert.assertEquals(HorizonJoinPlan.Algorithm.SERIAL, horizon.getAlgorithm());
                        horizon.setAlgorithm(HorizonJoinPlan.Algorithm.PARALLEL);
                    }, PlanVerifier.HORIZON_ALGORITHM);
            assertAlgorithmContradicts("SELECT h.offset FROM (SELECT * FROM a LIMIT 3) a HORIZON JOIN b b LIST (0s) AS h",
                    HorizonJoinPlan.class, horizon -> {
                        Assert.assertEquals(HorizonJoinPlan.Algorithm.SERIAL, horizon.getAlgorithm());
                        horizon.setAlgorithm(HorizonJoinPlan.Algorithm.PARALLEL);
                    }, PlanVerifier.HORIZON_ALGORITHM);
            final String fused = "SELECT h.offset, count() FROM a a HORIZON JOIN b b LIST (0s) AS h";
            assertAlgorithmContradicts(fused, HorizonJoinPlan.class, horizon -> {
                Assert.assertEquals(HorizonJoinPlan.Consumer.AGGREGATE, horizon.getConsumer());
                Assert.assertNull(horizon.getAlgorithm());
                horizon.setAlgorithm(HorizonJoinPlan.Algorithm.SERIAL);
            }, PlanVerifier.HORIZON_ALGORITHM);
            assertAlgorithmContradicts(fused, HorizonJoinPlan.class,
                    horizon -> horizon.setConsumer(HorizonJoinPlan.Consumer.PROJECTION), PlanVerifier.HORIZON_CONSUMER);
            assertAlgorithmContradicts(fused, HorizonJoinPlan.class, horizon -> horizon.setConsumer(null), PlanVerifier.HORIZON_CONSUMER);
        });
    }

    @Test
    public void testHorizonProjectionCollapsedIntoAggregate() throws Exception {
        assertMemoryLeak(() -> {
            createTemporalTables();
            try (
                    SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                    RecordCursorFactory ignored = compiler.compile(
                            "SELECT count() FROM (SELECT h.offset FROM a a HORIZON JOIN b b LIST (0s) AS h)", sqlExecutionContext
                    ).getRecordCursorFactory()
            ) {
                final LogicalPlan plan = compiler.getPlanForTesting();
                final AggregatePlan aggregate = find(plan, AggregatePlan.class);
                Assert.assertNotNull(aggregate);
                Assert.assertTrue(aggregate.getChild() instanceof ProjectPlan project && project.getChild() instanceof HorizonJoinPlan);
                verifier.verifyAccessPaths(plan);
                aggregate.replaceInput(0, aggregate.getChild().inputAt(0));
                assertFails(plan, PlanVerifier.HORIZON_CONSUMER, "HorizonJoinPlan");
            }
        });
    }

    @Test
    public void testHorizonReadsMasterWithoutRandomAccess() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE cov (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (px), px DOUBLE)
                    TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("CREATE TABLE quotes (ts TIMESTAMP, sym SYMBOL, bid DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            sqlExecutionContext.setParallelHorizonJoinEnabled(true);
            final String join = " FROM cov t HORIZON JOIN quotes q ON (sym) LIST (0s) AS h WHERE t.sym = 'A'";
            assertAlgorithmContradicts("SELECT t.px, h.offset, q.bid" + join, HorizonJoinPlan.class, horizon -> {
                Assert.assertEquals(HorizonJoinPlan.Algorithm.PARALLEL, horizon.getAlgorithm());
                horizon.setAlgorithm(HorizonJoinPlan.Algorithm.SERIAL);
            }, PlanVerifier.INPUT_RANDOM_ACCESS);
            assertAlgorithmContradicts("SELECT h.offset, avg(q.bid)" + join, AggregatePlan.class, aggregate -> {
                Assert.assertEquals(AggregatePlan.Algorithm.PARALLEL, aggregate.getAlgorithm());
                aggregate.setAlgorithm(AggregatePlan.Algorithm.SERIAL);
            }, PlanVerifier.INPUT_RANDOM_ACCESS);
        });
    }

    @Test
    public void testHorizonSlaveWithoutTimeFrames() throws Exception {
        assertMemoryLeak(() -> {
            createTemporalTables();
            for (String sql : new String[]{
                    "SELECT h.offset, b.v FROM a a HORIZON JOIN b b LIST (0s) AS h",
                    "SELECT h.offset, avg(b.v) FROM a a HORIZON JOIN b b LIST (0s) AS h"
            }) {
                try (
                        SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                        RecordCursorFactory ignored = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()
                ) {
                    final LogicalPlan plan = compiler.getPlanForTesting();
                    final HorizonJoinPlan horizon = find(plan, HorizonJoinPlan.class);
                    Assert.assertNotNull(horizon);
                    verifier.verifyAccessPaths(plan);
                    ((ScanPlan) horizon.getSlaves().getQuick(0).getInput()).setRandomAccess(false);
                    verifier.verify(plan, PASS);
                    assertAccessPathsFail(plan, PlanVerifier.HORIZON_SLAVE_TIME_FRAME, "HorizonJoinPlan");
                }
            }
        });
    }

    @Test
    public void testWindowShape() {
        final FunctionSourcePlan source = source();
        final WindowPlan window = WindowPlan.FACTORY.newInstance().of(source, 0);
        window.getOutput().copyFrom(source.getOutput());
        window.getFunctionColumnIds().add(8);
        assertFails(window, PlanVerifier.WINDOW_SHAPE, "WindowPlan");
    }

    private static ColumnExpression column(int columnId, int type) {
        return ColumnExpression.FACTORY.newInstance().of(columnId, type, 0);
    }

    private static void createTemporalTables() throws Exception {
        execute("CREATE TABLE a (ts TIMESTAMP, k INT) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE b (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY");
    }

    private static FilterPlan filter(LogicalPlan input, BoundExpression predicate) {
        final FilterPlan filter = FilterPlan.FACTORY.newInstance().of(input, predicate, 1);
        filter.getOutput().copyFrom(input.getOutput());
        return filter;
    }

    private static <T extends LogicalPlan> T find(LogicalPlan plan, Class<T> type) {
        if (type.isInstance(plan)) {
            return type.cast(plan);
        }
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            final T found = find(plan.inputAt(i), type);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    private static JoinPlan join(LogicalPlan master, LogicalPlan slave) {
        final JoinPlan join = JoinPlan.FACTORY.newInstance().of(0);
        join.getInputs().add(JoinInput.FACTORY.newInstance().of(master, JoinKind.CROSS, "m", 0));
        join.getInputs().add(JoinInput.FACTORY.newInstance().of(slave, JoinKind.INNER, "s", 0));
        final OutputSchema output = join.getOutput();
        for (int i = 0; i < 2; i++) {
            final OutputSchema source = join.getInputs().getQuick(i).getSourceOutput();
            for (int k = 0, n = source.getColumnCount(); k < n; k++) {
                output.add(source.getColumnId(k), source.getColumnName(k), source.getColumnType(k), true);
            }
        }
        return join;
    }

    /**
     * A leaf with an INT column {@code a} and a BOOLEAN column {@code b} under the given ids (1 and 2 by default).
     */
    private static JoinInput lightHashStep(LogicalPlan plan) {
        if (plan instanceof JoinPlan join && join.getOrderedInputs().size() > 1
                && join.getOrderedInputs().getQuick(1).getAlgorithm() == JoinInput.Algorithm.LIGHT_HASH) {
            return join.getOrderedInputs().getQuick(1);
        }
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            final JoinInput step = lightHashStep(plan.inputAt(i));
            if (step != null) {
                return step;
            }
        }
        return null;
    }

    private static FunctionSourcePlan source(int... columnIds) {
        final FunctionSourcePlan source = FunctionSourcePlan.FACTORY.newInstance().of(0);
        source.getOutput().add(columnIds.length > 0 ? columnIds[0] : 1, "a", ColumnType.INT, true);
        source.getOutput().add(columnIds.length > 1 ? columnIds[1] : 2, "b", ColumnType.BOOLEAN, true);
        return source;
    }

    private void assertAccessPathsFail(LogicalPlan plan, String invariant, String nodeName) {
        try {
            verifier.verifyAccessPaths(plan);
            Assert.fail("expected " + invariant);
        } catch (AssertionError e) {
            TestUtils.assertContains(e.getMessage(), invariant);
            TestUtils.assertContains(e.getMessage(), " at " + nodeName + " position ");
        }
    }

    private <T extends LogicalPlan> void assertAlgorithmContradicts(String sql, Class<T> type, PlanChange<T> change, String invariant) throws Exception {
        try (
                SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                RecordCursorFactory ignored = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()
        ) {
            final LogicalPlan plan = compiler.getPlanForTesting();
            final T node = find(plan, type);
            Assert.assertNotNull(node);
            verifier.verifyAccessPaths(plan);
            change.apply(node);
            assertFails(plan, invariant, type.getSimpleName());
        }
    }

    private <T extends LogicalPlan> void assertAlgorithmUnplanned(String sql, Class<T> type, PlanChange<T> change) throws Exception {
        try (
                SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                RecordCursorFactory ignored = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()
        ) {
            final LogicalPlan plan = compiler.getPlanForTesting();
            final T node = find(plan, type);
            Assert.assertNotNull(node);
            verifier.verifyAccessPaths(plan);
            change.apply(node);
            verifier.verify(plan, PASS);
            assertAccessPathsFail(plan, PlanVerifier.CHOICE_UNPLANNED, type.getSimpleName());
        }
    }

    /**
     * The consumer reads the designated timestamp of the light hash join of l and r, so operator planning fixes the
     * join's master side; a side left to the smaller input drops the timestamp and fails the plan.
     */
    private void assertMasterSideBindsTimestamp(String sql, String consumer) throws Exception {
        try (
                SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                RecordCursorFactory ignored = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()
        ) {
            final LogicalPlan plan = compiler.getPlanForTesting();
            final JoinInput step = lightHashStep(plan);
            Assert.assertNotNull(sql, step);
            Assert.assertEquals(sql, JoinInput.MasterSide.FIXED, step.getMasterSide());
            verifier.verifyAccessPaths(plan);
            step.setMasterSide(JoinInput.MasterSide.SMALLER);
            assertAccessPathsFail(plan, PlanVerifier.TIMESTAMP_READ, consumer);
        }
    }

    private void assertDecorrelationFails(LogicalPlan plan, String invariant, String nodeName) {
        try {
            verifier.verifyDecorrelation(plan, PASS, carriers);
            Assert.fail("expected " + invariant);
        } catch (AssertionError e) {
            TestUtils.assertContains(e.getMessage(), invariant);
            TestUtils.assertContains(e.getMessage(), " at " + nodeName + " position ");
            TestUtils.assertContains(e.getMessage(), "after " + PASS);
        }
    }

    private void assertFails(LogicalPlan plan, String invariant, String nodeName) {
        try {
            verifier.verify(plan, PASS);
            Assert.fail("expected " + invariant);
        } catch (AssertionError e) {
            TestUtils.assertContains(e.getMessage(), invariant);
            TestUtils.assertContains(e.getMessage(), " at " + nodeName + " position ");
            TestUtils.assertContains(e.getMessage(), "after " + PASS);
        }
    }

    @FunctionalInterface
    private interface PlanChange<T> {
        void apply(T plan);
    }
}
