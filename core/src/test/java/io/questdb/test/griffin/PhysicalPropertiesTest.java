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
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.engine.ops.UpdateOperation;
import io.questdb.griffin.engine.table.SelectedRecordCursorFactory;
import io.questdb.griffin.optimiser.PlanVerifier;
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.GeneratedShapes;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.PhysicalProperties;
import io.questdb.griffin.plan.logical.PhysicalProperties.Capability;
import io.questdb.griffin.plan.logical.PhysicalProperties.ScanDirection;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.griffin.plan.logical.SetOperationPlan;
import io.questdb.griffin.plan.logical.WindowJoinPlan;
import io.questdb.griffin.plan.logical.WindowJoinStep;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.test.griffin.PlanShape.assertPlanned;
import static io.questdb.test.griffin.PlanShape.find;

public class PhysicalPropertiesTest extends AbstractCairoTest {

    @Test
    public void testCoveringResidualFilterAlgorithm() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE c (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("ALTER TABLE c ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price)");
            execute("""
                    INSERT INTO c VALUES
                        ('2024-01-01T00:00:00.000000Z', 'a', 1.0),
                        ('2024-01-01T01:00:00.000000Z', 'b', 5.0),
                        ('2024-01-01T02:00:00.000000Z', 'a', 3.0),
                        ('2024-01-02T00:00:00.000000Z', 'a', 4.0)
                    """);
            final String sql = "SELECT sym, price FROM c WHERE sym = 'a' AND price > 1";
            assertResidualAlgorithm(FilterPlan.Algorithm.PARALLEL, Capability.YES, sql);
            bindVariableService.setStr("s", "a");
            assertResidualAlgorithm(FilterPlan.Algorithm.SERIAL, Capability.NO, "SELECT sym, price FROM c WHERE sym = :s AND price > 1");
            sqlExecutionContext.setParallelFilterEnabled(false);
            try {
                assertResidualAlgorithm(FilterPlan.Algorithm.SERIAL, Capability.NO, sql);
            } finally {
                sqlExecutionContext.setParallelFilterEnabled(true);
            }
            assertQuery(sql)
                    .withPlanContaining("Async Filter")
                    .returns("""
                            sym\tprice
                            a\t3.0
                            a\t4.0
                            """);
        });
    }

    @Test
    public void testFilterAlgorithmFollowsParallelFilter() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            final String fused = "SELECT ts, x FROM t WHERE x > 2";
            final String grouped = "SELECT * FROM (SELECT x, count() c FROM t GROUP BY x) WHERE c > 0 AND x > 2";
            assertResidualAlgorithm(FilterPlan.Algorithm.PARALLEL, Capability.YES, fused);
            assertFilterAlgorithm(FilterPlan.Algorithm.SERIAL, grouped);
            sqlExecutionContext.setParallelFilterEnabled(false);
            try {
                assertResidualAlgorithm(FilterPlan.Algorithm.SERIAL, Capability.YES, fused);
                assertFilterAlgorithm(FilterPlan.Algorithm.SERIAL, grouped);
            } finally {
                sqlExecutionContext.setParallelFilterEnabled(true);
            }
            assertQuery(fused)
                    .timestamp("ts")
                    .withPlanContaining("Async JIT Filter")
                    .returns("""
                            ts\tx
                            2024-01-01T02:00:00.000000Z\t3
                            2024-01-02T00:00:00.000000Z\t4
                            """);
            assertQuery(grouped + " ORDER BY x")
                    .returns("""
                            x\tc
                            3\t1
                            4\t1
                            """);
        });
    }

    @Test
    public void testFoldedConstantPredicatesKeepPropertiesKnown() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            final String[] truePredicates = {"length('abc') = 3", "1 IN (1, 2)", "CASE WHEN 1 = 2 THEN false ELSE true END", "'true'::boolean"};
            final String[] falsePredicates = {"abs(-1) = 2", "3 IN (1, 2)", "1::INT = 2::LONG AND 2 > 1", "coalesce(NULL, false)"};
            for (String predicate : truePredicates) {
                assertKnownProperties(predicate, JoinInput.Algorithm.TEMPORAL_TIME_FRAME);
            }
            for (String predicate : falsePredicates) {
                assertKnownProperties(predicate, null);
            }
            assertQuery("SELECT * FROM t WHERE 1 IN (1, 2) AND length('abc') = 3")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tx
                            2024-01-01T00:00:00.000000Z\t1
                            2024-01-01T01:00:00.000000Z\t2
                            2024-01-01T02:00:00.000000Z\t3
                            2024-01-02T00:00:00.000000Z\t4
                            """);
            assertQuery("SELECT * FROM (SELECT x, count() c FROM t GROUP BY x) WHERE abs(-1) = 2")
                    .returns("x\tc\n");
            assertQuery("SELECT t.x, s.x sx FROM t ASOF JOIN (SELECT * FROM t WHERE length('abc') = 3) s")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            x\tsx
                            1\t1
                            2\t2
                            3\t3
                            4\t4
                            """);
        });
    }

    @Test
    public void testGenerateSeriesDirectionFollowsConstantStep() throws Exception {
        assertMemoryLeak(() -> {
            assertDirection(ScanDirection.FORWARD, "generate_series('2025-01-01', '2025-02-01', '1d')");
            assertDirection(ScanDirection.BACKWARD, "generate_series('2025-02-01', '2025-01-01', '-1d')");
            assertDirection(ScanDirection.BACKWARD, "generate_series('2025-01-01'::timestamp, '2025-02-01'::timestamp, -86_400_000_000)");
            bindVariableService.setStr("step", "1d");
            assertDirection(ScanDirection.OTHER, "generate_series('2025-01-01', '2025-02-01', :step)");
            assertQuery("SELECT * FROM generate_series('2025-01-03', '2025-01-01', '-1d') ORDER BY generate_series DESC")
                    .timestampDesc("generate_series")
                    .expectSize()
                    .returns("""
                            generate_series
                            2025-01-03T00:00:00.000000Z
                            2025-01-02T00:00:00.000000Z
                            2025-01-01T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testIdentityProjectionOverJoinKeepsPlanNames() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            execute("CREATE TABLE u (ts2 TIMESTAMP, y LONG) TIMESTAMP(ts2) PARTITION BY DAY");
            execute("INSERT INTO u VALUES ('2024-01-01T00:30:00.000000Z', 10), ('2024-01-02T00:30:00.000000Z', 20)");
            final String sql = "SELECT * FROM t a ASOF JOIN u b";
            try (
                    SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                    RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()
            ) {
                final ProjectPlan project = find(compiler.getPlanForTesting(), ProjectPlan.class);
                Assert.assertTrue(project.getInput() instanceof JoinPlan);
                Assert.assertTrue(GeneratedShapes.isIdentityProjection(project));
                Assert.assertTrue(factory.getBaseFactory() instanceof SelectedRecordCursorFactory);
            }
            assertQuery(sql)
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            ts\tx\tts2\ty
                            2024-01-01T00:00:00.000000Z\t1\t\tnull
                            2024-01-01T01:00:00.000000Z\t2\t2024-01-01T00:30:00.000000Z\t10
                            2024-01-01T02:00:00.000000Z\t3\t2024-01-01T00:30:00.000000Z\t10
                            2024-01-02T00:00:00.000000Z\t4\t2024-01-01T00:30:00.000000Z\t10
                            """);
        });
    }

    @Test
    public void testIndexAccessPathDeliversKeyOrder() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE s (ts TIMESTAMP, sym SYMBOL INDEX, x LONG) TIMESTAMP(ts) PARTITION BY NONE");
            execute("INSERT INTO s VALUES ('2024-01-01T00:00:00.000000Z', 'b', 1), ('2024-01-01T01:00:00.000000Z', 'a', 2), ('2024-01-01T02:00:00.000000Z', 'b', 3)");
            final String sql = "SELECT * FROM s WHERE sym IN ('a', 'b') AND x > 0 ORDER BY sym";
            try (
                    SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                    RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()
            ) {
                final LogicalPlan filter = find(compiler.getPlanForTesting(), FilterPlan.class);
                final ScanPlan scan = find(filter, ScanPlan.class);
                Assert.assertEquals(ScanPlan.AccessPath.SYMBOL_INDEX, scan.getAccessPath());
                Assert.assertEquals(ScanPlan.IndexRead.INDEX, scan.getIndexRead());
                Assert.assertEquals(ScanPlan.IndexOrder.KEY, scan.getIndexOrder());
                Assert.assertTrue(scan.isRequestedOrderDelivered());
                Assert.assertEquals(Capability.YES, PhysicalProperties.followsOrderAdvice(filter));
                Assert.assertEquals(Capability.YES, PhysicalProperties.supportsRandomAccess(filter));
                Assert.assertEquals(-1, PhysicalProperties.timestampIndex(filter));
                Assert.assertEquals(-1, factory.getMetadata().getTimestampIndex());
            }
            assertQuery(sql)
                    .returns("""
                            ts	sym	x
                            2024-01-01T01:00:00.000000Z	a	2
                            2024-01-01T00:00:00.000000Z	b	1
                            2024-01-01T02:00:00.000000Z	b	3
                            """);
        });
    }

    @Test
    public void testIntersectPassesLeftOrderAndRandomAccess() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            try (
                    SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                    RecordCursorFactory factory = compiler.compile(
                            "(SELECT ts, x FROM t ORDER BY ts DESC) INTERSECT (SELECT ts, x FROM t)", sqlExecutionContext
                    ).getRecordCursorFactory()
            ) {
                final LogicalPlan plan = compiler.getPlanForTesting();
                Assert.assertTrue(plan instanceof SetOperationPlan);
                Assert.assertEquals(ScanDirection.BACKWARD, PhysicalProperties.scanDirection(plan));
                Assert.assertEquals(Capability.YES, PhysicalProperties.supportsRandomAccess(plan));
                Assert.assertEquals(RecordCursorFactory.SCAN_DIRECTION_BACKWARD, factory.getScanDirection());
                Assert.assertTrue(factory.recordCursorSupportsRandomAccess());
            }
        });
    }

    @Test
    public void testIntrinsicallyFalseIntervalsPlanEmptyAccessPath() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            final String sql = "SELECT ts, x FROM t WHERE ts IN '2024-01-01' AND ts IN '2024-01-02'";
            try (
                    SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                    RecordCursorFactory ignore = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()
            ) {
                final ScanPlan scan = find(compiler.getPlanForTesting(), ScanPlan.class);
                Assert.assertEquals(ScanPlan.AccessPath.EMPTY, scan.getAccessPath());
            }
            assertQuery(sql)
                    .timestamp("ts")
                    .returns("ts\tx\n");
        });
    }

    @Test
    public void testKeepFlagFusionRecordedOnFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE s (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO s VALUES
                        (10.0, '2024-01-01T00:00:00.000000Z'),
                        (20.0, '2024-01-01T01:00:00.000000Z'),
                        (30.0, '2024-01-01T02:00:00.000000Z'),
                        (40.0, '2024-01-01T03:00:00.000000Z')
                    """);
            final String subsample = "SELECT price, ts FROM s SUBSAMPLE lttb(price, 2)";
            final String handWritten = "SELECT price, ts FROM (SELECT price, ts, lttb(ts, price, 2) OVER (ORDER BY ts) keep FROM s) WHERE keep";
            assertFilterAlgorithm(FilterPlan.Algorithm.WINDOW_KEEP_FLAG, subsample);
            assertFilterAlgorithm(FilterPlan.Algorithm.SERIAL, handWritten);
            final String expected = """
                    price\tts
                    10.0\t2024-01-01T00:00:00.000000Z
                    40.0\t2024-01-01T03:00:00.000000Z
                    """;
            assertQuery(subsample)
                    .timestamp("ts")
                    .withPlanContaining("CachedWindowLightSelect")
                    .returns(expected);
            assertQuery(handWritten)
                    .timestamp("ts")
                    .withPlanContaining("Filter filter: keep")
                    .returns(expected);
        });
    }

    @Test
    public void testLimitAbsorbedByParallelFilter() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            final String sql = "SELECT ts, x FROM t WHERE x > 1 LIMIT 2";
            assertFilterLimit(sql, Capability.YES);
            sqlExecutionContext.setParallelFilterEnabled(false);
            assertFilterLimit(sql, Capability.NO);
            assertQuery(sql)
                    .timestamp("ts")
                    .returns("""
                            ts\tx
                            2024-01-01T01:00:00.000000Z\t2
                            2024-01-01T02:00:00.000000Z\t3
                            """);
        });
    }

    @Test
    public void testMarkoutMasterRandomAccessAndLongSequenceSlave() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, order_ts TIMESTAMP) TIMESTAMP(order_ts)");
            execute("INSERT INTO orders VALUES (1, 0::TIMESTAMP), (2, 1::TIMESTAMP)");
            final String sql = """
                    SELECT /*+ markout_horizon(orders offsets) */ id, order_ts + usec_offs AS ts
                    FROM orders CROSS JOIN (SELECT 1_000_000 * (x-1) AS usec_offs FROM long_sequence(2)) offsets
                    ORDER BY order_ts + usec_offs
                    """;
            try (
                    SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                    RecordCursorFactory ignore = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()
            ) {
                final JoinPlan join = find(compiler.getPlanForTesting(), JoinPlan.class);
                final JoinInput master = join.getOrderedInputs().getQuick(0);
                final JoinInput slave = join.getOrderedInputs().getQuick(1);
                Assert.assertEquals(Capability.YES, PhysicalProperties.supportsRandomAccess(master.getInput()));
                Assert.assertEquals(Capability.YES, PhysicalProperties.isLongSequence(slave.getInput()));
                Assert.assertEquals(Capability.NO, PhysicalProperties.isLongSequence(master.getInput()));
                Assert.assertEquals(Capability.YES, PhysicalProperties.followsOrderAdvice(join));
                Assert.assertEquals(-1, PhysicalProperties.timestampIndex(join));
            }
            assertQuery(sql)
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            id\tts
                            1\t1970-01-01T00:00:00.000000Z
                            2\t1970-01-01T00:00:00.000001Z
                            1\t1970-01-01T00:00:01.000000Z
                            2\t1970-01-01T00:00:01.000001Z
                            """);
        });
    }

    @Test
    public void testMergedUnionAllFollowsRequestedOrder() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            try (
                    SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                    RecordCursorFactory factory = compiler.compile(
                            "SELECT ts, x FROM (SELECT ts, x FROM t UNION ALL SELECT ts, x FROM t) ORDER BY ts DESC", sqlExecutionContext
                    ).getRecordCursorFactory()
            ) {
                final LogicalPlan union = find(compiler.getPlanForTesting(), SetOperationPlan.class);
                Assert.assertEquals(ScanDirection.BACKWARD, PhysicalProperties.scanDirection(union));
                Assert.assertEquals(Capability.YES, PhysicalProperties.followsOrderAdvice(union));
                Assert.assertEquals(Capability.NO, PhysicalProperties.supportsRandomAccess(union));
                Assert.assertEquals(RecordCursorFactory.SCAN_DIRECTION_BACKWARD, factory.getScanDirection());
            }
        });
    }

    @Test
    public void testPatternResidualFilterAlgorithm() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE p (s SYMBOL INDEX, x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO p SELECT (x % 12)::SYMBOL, x::INT, (x * 1_000_000)::TIMESTAMP FROM long_sequence(12)");
            final String sql = "SELECT s, x FROM p WHERE s LIKE '1%'";
            final String expected = """
                    s\tx
                    1\t1
                    10\t10
                    11\t11
                    """;
            assertResidualAlgorithm(FilterPlan.Algorithm.PARALLEL, Capability.YES, sql);
            sqlExecutionContext.setParallelFilterEnabled(false);
            try {
                assertResidualAlgorithm(FilterPlan.Algorithm.SERIAL, Capability.YES, sql);
                assertQuery(sql + " ORDER BY x")
                        .returns(expected);
            } finally {
                sqlExecutionContext.setParallelFilterEnabled(true);
            }
            assertQuery(sql + " ORDER BY x")
                    .returns(expected);
        });
    }

    @Test
    public void testProjectionRandomAccessDecidesHashJoin() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertRandomAccess(Capability.YES, "SELECT x, x + 1 v FROM t");
            assertRandomAccess(Capability.NO, "SELECT x, timestamp_sequence(0, 1) v FROM t");
            assertQuery("SELECT a.ts, a.x, b.v FROM t a JOIN (SELECT x, x + 1 v FROM t) b ON a.x = b.x")
                    .noRandomAccess()
                    .withPlan("""
                            SelectedRecord
                                Hash Join Light
                                  condition: b.x=a.x
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                                    Hash
                                        VirtualRecord
                                          functions: [x,x+1]
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: t
                            """)
                    .returns("""
                            ts\tx\tv
                            2024-01-01T00:00:00.000000Z\t1\t2
                            2024-01-01T01:00:00.000000Z\t2\t3
                            2024-01-01T02:00:00.000000Z\t3\t4
                            2024-01-02T00:00:00.000000Z\t4\t5
                            """);
            assertQuery("SELECT a.ts, a.x, b.v FROM t a JOIN (SELECT x, timestamp_sequence(0, 1) v FROM t) b ON a.x = b.x")
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .withPlan("""
                            SelectedRecord
                                Hash Join
                                  condition: b.x=a.x
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                                    Hash
                                        VirtualRecord
                                          functions: [x,timestamp_sequence(0,1)]
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: t
                            """)
                    .returns("""
                            ts\tx\tv
                            2024-01-01T00:00:00.000000Z\t1\t1970-01-01T00:00:00.000000Z
                            2024-01-01T01:00:00.000000Z\t2\t1970-01-01T00:00:00.000001Z
                            2024-01-01T02:00:00.000000Z\t3\t1970-01-01T00:00:00.000002Z
                            2024-01-02T00:00:00.000000Z\t4\t1970-01-01T00:00:00.000003Z
                            """);
        });
    }

    @Test
    public void testProjectionRandomAccessDecidesLimitedSort() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertRandomAccess(Capability.YES, "SELECT x, x + 1 r FROM t");
            assertRandomAccess(Capability.NO, "SELECT x, rnd_int() r FROM t");
            assertRandomAccess(Capability.NO, "SELECT x, timestamp_sequence(0, 1) r FROM t");
            assertQuery("SELECT * FROM (SELECT x, x + 1 r FROM t) ORDER BY x DESC LIMIT 1, 3")
                    .expectSize()
                    .withPlan("""
                            Encode sort light lo: 1 hi: 3
                              keys: [x desc]
                                VirtualRecord
                                  functions: [x,x+1]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                            """)
                    .returns("""
                            x\tr
                            3\t4
                            2\t3
                            """);
            assertQuery("SELECT * FROM (SELECT x, rnd_int() r FROM t) ORDER BY x DESC LIMIT 1, 3")
                    .assertsPlan("""
                            Limit left: 1 right: 3 skip-rows: 1 take-rows: 2
                                Encode sort
                                  keys: [x desc]
                                    VirtualRecord
                                      functions: [x,rnd_int()]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t
                            """);
            assertQuery("SELECT * FROM (SELECT x, timestamp_sequence(0, 1) r FROM t) ORDER BY x DESC LIMIT 1, 3")
                    .expectSize()
                    .withPlan("""
                            Limit left: 1 right: 3 skip-rows: 1 take-rows: 2
                                Encode sort
                                  keys: [x desc]
                                    VirtualRecord
                                      functions: [x,timestamp_sequence(0,1)]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t
                            """)
                    .returns("""
                            x\tr
                            3\t1970-01-01T00:00:00.000002Z
                            2\t1970-01-01T00:00:00.000001Z
                            """);
        });
    }

    @Test
    public void testSharedConsumerCountMakesSharedCursorsKnown() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            final String sql = """
                    SELECT o.x, o.s, r.y, d.z
                    FROM (SELECT x, string_agg(x::STRING, ',') s FROM t WHERE x <= 2 GROUP BY x) o
                    JOIN LATERAL (SELECT x y FROM t WHERE x <= o.x) r
                    JOIN LATERAL (SELECT x z FROM t WHERE x >= o.x + 2) d
                    ORDER BY o.x, r.y, d.z
                    """;
            try (
                    SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                    RecordCursorFactory ignore = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()
            ) {
                final AggregatePlan source = find(compiler.getPlanForTesting(), AggregatePlan.class);
                Assert.assertEquals(AggregatePlan.Algorithm.SERIAL, source.getAlgorithm());
                Assert.assertEquals(2, source.getSharedConsumerCount());
                Assert.assertEquals(Capability.YES, PhysicalProperties.supportsSharedCursors(source));
            }
            assertQuery(sql)
                    .expectSize()
                    .withPlanContaining("(Shared)")
                    .returns("""
                            x\ts\ty\tz
                            1\t1\t1\t3
                            1\t1\t1\t4
                            2\t2\t1\t4
                            2\t2\t2\t4
                            """);
        });
    }

    @Test
    public void testSharedCursorsFollowGroupByAlgorithm() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            execute("CREATE TABLE s (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY NONE");
            execute("INSERT INTO s VALUES ('2024-01-01T00:00:00.000000Z', 'a', 1), ('2024-01-01T01:00:00.000000Z', 'b', 2), ('2024-01-01T02:00:00.000000Z', 'a', 3)");
            final String parallel = "SELECT x, count() c FROM t GROUP BY x";
            final String serial = "SELECT x, string_agg(x::string, ',') s FROM t GROUP BY x";
            assertSharedCursors(AggregatePlan.Algorithm.PARALLEL, Capability.YES, parallel);
            assertSharedCursors(AggregatePlan.Algorithm.VECTORISED, Capability.YES, "SELECT sym, count() c FROM s GROUP BY sym");
            assertSharedCursors(AggregatePlan.Algorithm.SERIAL, Capability.NO, serial);
            sqlExecutionContext.setParallelGroupByEnabled(false);
            try {
                assertSharedCursors(AggregatePlan.Algorithm.SERIAL, Capability.NO, parallel);
            } finally {
                sqlExecutionContext.setParallelGroupByEnabled(true);
            }
            assertQuery(parallel + " ORDER BY x")
                    .expectSize()
                    .withPlanContaining("Async Group By")
                    .returns("""
                            x\tc
                            1\t1
                            2\t1
                            3\t1
                            4\t1
                            """);
            assertQuery(serial + " ORDER BY x")
                    .expectSize()
                    .withPlanContaining("GroupBy")
                    .returns("""
                            x\ts
                            1\t1
                            2\t2
                            3\t3
                            4\t4
                            """);
        });
    }

    @Test
    public void testSymbolCallDeniesParallelGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, l LONG, l256 LONG256) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                        ('2024-01-01T00:00:00.000000Z', 1, 0x01),
                        ('2024-01-01T01:00:00.000000Z', 2, 0x02),
                        ('2024-01-01T02:00:00.000000Z', 3, 0x01)
                    """);
            assertQuery("SELECT l256::symbol k, count() c FROM t GROUP BY k ORDER BY k")
                    .expectSize()
                    .withPlanContaining("GroupBy")
                    .returns("""
                            k\tc
                            1\t2
                            2\t1
                            """);
            assertQuery("SELECT length(l256::symbol) k, count() c FROM t GROUP BY k ORDER BY k")
                    .expectSize()
                    .withPlanContaining("GroupBy")
                    .returns("""
                            k\tc
                            1\t3
                            """);
            assertQuery("SELECT l256::string k, count() c FROM t GROUP BY k ORDER BY k")
                    .expectSize()
                    .withPlanContaining("Async Group By")
                    .returns("""
                            k\tc
                            0x01\t2
                            0x02\t1
                            """);
        });
    }

    @Test
    public void testTimeFrameCursorOverScans() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            execute("CREATE TABLE s (ts TIMESTAMP, sym SYMBOL INDEX, x LONG) TIMESTAMP(ts) PARTITION BY NONE");
            execute("INSERT INTO s VALUES ('2024-01-01T00:00:00.000000Z', 'a', 1), ('2024-01-01T01:00:00.000000Z', 'b', 2)");
            assertTimeFrame(Capability.YES, "SELECT ts, x FROM t");
            assertTimeFrame(Capability.YES, "SELECT ts, x FROM t WHERE ts IN '2024-01-01'");
            assertTimeFrame(Capability.NO, "SELECT ts, x FROM t WHERE x > 1");
            assertTimeFrame(Capability.NO, "SELECT x FROM t");
            assertTimeFrame(Capability.NO, "SELECT ts, x FROM t ORDER BY ts DESC");
            assertTimeFrame(Capability.NO, "SELECT ts, sym FROM s WHERE sym = 'a'");
            assertQuery("SELECT t.ts, t.x, s.sym FROM t ASOF JOIN s")
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("AsOf Join Fast")
                    .returns("""
                            ts\tx\tsym
                            2024-01-01T00:00:00.000000Z\t1\ta
                            2024-01-01T01:00:00.000000Z\t2\tb
                            2024-01-01T02:00:00.000000Z\t3\tb
                            2024-01-02T00:00:00.000000Z\t4\tb
                            """);
        });
    }

    @Test
    public void testWalClientUpdateRecordedAtBinding() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE w (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE nw (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            assertWalClientUpdate(true, "UPDATE w SET x = 2 WHERE x > 0");
            assertWalClientUpdate(false, "UPDATE nw SET x = 2 WHERE x > 0");
        });
    }

    @Test
    public void testWindowJoinAlgorithmFollowsAggregates() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE sl (ts TIMESTAMP, y LONG) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO m VALUES ('2024-01-01T00:00:00.000000Z', 1), ('2024-01-01T01:00:00.000000Z', 2), ('2024-01-01T02:30:00.000000Z', 3)");
            execute("INSERT INTO sl VALUES ('2024-01-01T00:30:00.000000Z', 10), ('2024-01-01T01:30:00.000000Z', 20), ('2024-01-01T02:00:00.000000Z', 30)");
            final String parallel = "SELECT m.ts, sum(sl.y) s FROM m WINDOW JOIN sl RANGE BETWEEN 1 HOUR PRECEDING AND CURRENT ROW";
            final String serial = "SELECT m.ts, string_agg(sl.y::string, ',') s FROM m WINDOW JOIN sl RANGE BETWEEN 1 HOUR PRECEDING AND CURRENT ROW";
            assertWindowJoin(WindowJoinStep.Algorithm.PARALLEL, parallel);
            assertWindowJoin(WindowJoinStep.Algorithm.SERIAL, serial);
            assertQuery(parallel)
                    .timestamp("ts")
                    .noRandomAccess()
                    .withPlanContaining("Async Window Join")
                    .returns("""
                            ts\ts
                            2024-01-01T00:00:00.000000Z\tnull
                            2024-01-01T01:00:00.000000Z\t10
                            2024-01-01T02:30:00.000000Z\t50
                            """);
            assertQuery(serial)
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .withPlan("""
                            Window Join
                              window lo: 3600000000 preceding (include prevailing)
                              window hi: current row
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: m
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: sl
                            """)
                    .returns("""
                            ts\ts
                            2024-01-01T00:00:00.000000Z\t
                            2024-01-01T01:00:00.000000Z\t10
                            2024-01-01T02:30:00.000000Z\t20,30
                            """);
        });
    }

    @Test
    public void testWindowPassCountDecidesLimitedSort() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertRandomAccess(Capability.YES, "SELECT ts, x, sum(x) OVER () s FROM t");
            assertRandomAccess(Capability.NO, "SELECT ts, x, sum(x) OVER (ORDER BY ts) s FROM t");
            assertQuery("SELECT * FROM (SELECT ts, x, sum(x) OVER () s FROM t) ORDER BY x DESC LIMIT 1, 3")
                    .expectSize()
                    .withPlan("""
                            Encode sort light lo: 1 hi: 3
                              keys: [x desc]
                                CachedWindowLight
                                  unorderedFunctions: [sum(x) over ()]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                            """)
                    .returns("""
                            ts\tx\ts
                            2024-01-01T02:00:00.000000Z\t3\t10.0
                            2024-01-01T01:00:00.000000Z\t2\t10.0
                            """);
            assertQuery("SELECT * FROM (SELECT ts, x, sum(x) OVER (ORDER BY ts) s FROM t) ORDER BY x DESC LIMIT 1, 3")
                    .expectSize()
                    .withPlan("""
                            Limit left: 1 right: 3 skip-rows: 1 take-rows: 2
                                Encode sort
                                  keys: [x desc]
                                    Window
                                      functions: [sum(x) over (rows between unbounded preceding and current row)]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t
                            """)
                    .returns("""
                            ts\tx\ts
                            2024-01-01T02:00:00.000000Z\t3\t6.0
                            2024-01-01T01:00:00.000000Z\t2\t3.0
                            """);
        });
    }

    private static void assertDirection(ScanDirection expected, String sql) throws Exception {
        try (
                SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()
        ) {
            Assert.assertEquals(expected, PhysicalProperties.scanDirection(compiler.getPlanForTesting()));
            Assert.assertEquals(expected, ScanDirection.of(factory.getScanDirection()));
        }
    }

    private static void assertFilterAlgorithm(FilterPlan.Algorithm expected, String sql) throws Exception {
        assertPlanned(engine, sqlExecutionContext, sql, FilterPlan.class, filter -> Assert.assertEquals(expected, filter.getAlgorithm()));
    }

    private static void assertFilterLimit(String sql, Capability expected) throws Exception {
        try (
                SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()
        ) {
            final LogicalPlan filter = find(compiler.getPlanForTesting(), FilterPlan.class);
            Assert.assertEquals(expected, PhysicalProperties.implementsLimit(filter));
            Assert.assertEquals(Capability.YES, PhysicalProperties.implementsLimit(compiler.getPlanForTesting()));
            Assert.assertTrue(factory.implementsLimit());
        }
    }

    /**
     * Asserts that a folded constant predicate leaves every property the planner reads known, over a scan, over an
     * aggregate, in an ASOF slave and as a window join filter.
     */
    private static void assertKnownProperties(String predicate, JoinInput.Algorithm temporalAlgorithm) throws Exception {
        final String[] queries = {
                "SELECT * FROM t WHERE " + predicate,
                "SELECT * FROM (SELECT x, count() c FROM t GROUP BY x) WHERE " + predicate,
                "SELECT * FROM t ASOF JOIN (SELECT * FROM t WHERE " + predicate + ") s",
                "SELECT t.ts, sum(s.x) FROM t WINDOW JOIN t s ON " + predicate + " RANGE BETWEEN 1 HOUR PRECEDING AND CURRENT ROW"
        };
        for (String sql : queries) {
            try (
                    SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                    RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()
            ) {
                final LogicalPlan plan = compiler.getPlanForTesting();
                Assert.assertTrue(PlanVerifier.newStandalone().verifyAccessPaths(plan));
                final Capability isRandomAccess = PhysicalProperties.supportsRandomAccess(plan);
                final Capability isPageFrameSupported = PhysicalProperties.supportsPageFrameCursor(plan);
                final Capability isTimeFrameSupported = PhysicalProperties.supportsTimeFrameCursor(plan);
                Assert.assertNotEquals(sql, Capability.UNKNOWN, isRandomAccess);
                Assert.assertNotEquals(sql, Capability.UNKNOWN, isPageFrameSupported);
                Assert.assertNotEquals(sql, Capability.UNKNOWN, isTimeFrameSupported);
                Assert.assertEquals(sql, isRandomAccess == Capability.YES, factory.recordCursorSupportsRandomAccess());
                Assert.assertEquals(sql, isPageFrameSupported == Capability.YES, factory.supportsPageFrameCursor());
                Assert.assertEquals(sql, isTimeFrameSupported == Capability.YES, factory.supportsTimeFrameCursor());
                final JoinPlan join = find(plan, JoinPlan.class);
                if (join != null && temporalAlgorithm != null) {
                    Assert.assertEquals(sql, temporalAlgorithm, join.getOrderedInputs().getQuick(1).getAlgorithm());
                }
            }
        }
    }

    private static void assertRandomAccess(Capability expected, String sql) throws Exception {
        try (
                SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()
        ) {
            Assert.assertEquals(expected, PhysicalProperties.supportsRandomAccess(compiler.getPlanForTesting()));
            Assert.assertEquals(expected == Capability.YES, factory.recordCursorSupportsRandomAccess());
        }
    }

    private static void assertResidualAlgorithm(FilterPlan.Algorithm expected, Capability isRandomAccess, String sql) throws Exception {
        try (
                SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()
        ) {
            final LogicalPlan filter = find(compiler.getPlanForTesting(), FilterPlan.class);
            Assert.assertNull(((FilterPlan) filter).getAlgorithm());
            Assert.assertEquals(expected, find(filter, ScanPlan.class).getResidualAlgorithm());
            Assert.assertEquals(isRandomAccess, PhysicalProperties.supportsRandomAccess(filter));
            Assert.assertEquals(isRandomAccess == Capability.YES, factory.recordCursorSupportsRandomAccess());
        }
    }

    private static void assertSharedCursors(AggregatePlan.Algorithm algorithm, Capability expected, String sql) throws Exception {
        try (
                SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()
        ) {
            final LogicalPlan plan = compiler.getPlanForTesting();
            final AggregatePlan aggregate = find(plan, AggregatePlan.class);
            Assert.assertEquals(algorithm, aggregate.getAlgorithm());
            Assert.assertEquals(expected, PhysicalProperties.supportsSharedCursors(aggregate));
            Assert.assertEquals(expected == Capability.YES, factory.getBaseFactory().supportsSharedCursors());
        }
    }

    private static void assertTimeFrame(Capability expected, String sql) throws Exception {
        try (
                SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()
        ) {
            Assert.assertEquals(expected, PhysicalProperties.supportsTimeFrameCursor(compiler.getPlanForTesting()));
            Assert.assertEquals(expected == Capability.YES, factory.supportsTimeFrameCursor());
        }
    }

    private static void assertWalClientUpdate(boolean expected, String sql) throws Exception {
        try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
            try (UpdateOperation ignore = compiler.compile(sql, sqlExecutionContext).getUpdateOperation()) {
                final FilterPlan filter = find(compiler.getPlanForTesting(), FilterPlan.class);
                Assert.assertEquals(expected, ((ScanPlan) filter.getInput()).isWalClientUpdate());
                Assert.assertEquals(!expected, GeneratedShapes.isFusedFilter(filter));
            }
        }
    }

    private static void assertWindowJoin(WindowJoinStep.Algorithm expected, String sql) throws Exception {
        try (
                SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()
        ) {
            final LogicalPlan plan = compiler.getPlanForTesting();
            final WindowJoinPlan windowJoin = find(plan, WindowJoinPlan.class);
            Assert.assertEquals(expected, windowJoin.getSteps().getQuick(0).getAlgorithm());
            Assert.assertEquals(Capability.NO, PhysicalProperties.followsOrderAdvice(plan));
            Assert.assertEquals(Capability.NO, PhysicalProperties.isLongSequence(plan));
            Assert.assertFalse(factory.followedOrderByAdvice());
        }
    }

    private static void createTable() throws Exception {
        execute("CREATE TABLE t (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY");
        execute("""
                INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 1),
                    ('2024-01-01T01:00:00.000000Z', 2),
                    ('2024-01-01T02:00:00.000000Z', 3),
                    ('2024-01-02T00:00:00.000000Z', 4)
                """);
    }
}
