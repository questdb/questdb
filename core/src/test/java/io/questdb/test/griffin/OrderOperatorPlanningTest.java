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
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.FillPlan;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LatestByPlan;
import io.questdb.griffin.plan.logical.LimitPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.SampleByPlan;
import io.questdb.griffin.plan.logical.SetOperationPlan;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.griffin.plan.logical.WindowPlan;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Pins the order-sensitive operator decisions operator planning records on the plan, and the factories the generator
 * builds from them.
 */
public class OrderOperatorPlanningTest extends AbstractCairoTest {
    private static final String FIXED_JOIN_ROWS = """
            ts\tk\tv
            1970-01-01T00:00:01.000000Z\t2\t5
            1970-01-01T00:00:01.000000Z\t2\t4
            1970-01-01T00:00:01.000000Z\t2\t3
            1970-01-01T00:00:01.000000Z\t2\t2
            1970-01-01T00:00:01.000000Z\t2\t1
            1970-01-01T00:00:02.000000Z\t1\t10
            1970-01-01T00:00:02.000000Z\t1\t9
            1970-01-01T00:00:02.000000Z\t1\t8
            1970-01-01T00:00:02.000000Z\t1\t7
            1970-01-01T00:00:02.000000Z\t1\t6
            1970-01-01T00:00:03.000000Z\t2\t5
            1970-01-01T00:00:03.000000Z\t2\t4
            1970-01-01T00:00:03.000000Z\t2\t3
            1970-01-01T00:00:03.000000Z\t2\t2
            1970-01-01T00:00:03.000000Z\t2\t1
            1970-01-01T00:00:04.000000Z\t1\t10
            1970-01-01T00:00:04.000000Z\t1\t9
            1970-01-01T00:00:04.000000Z\t1\t8
            1970-01-01T00:00:04.000000Z\t1\t7
            1970-01-01T00:00:04.000000Z\t1\t6
            """;
    private static final String RESORTED_JOIN_ROWS = """
            ts\tk\tv
            1970-01-01T00:00:01.000000Z\t2\t1
            1970-01-01T00:00:01.000000Z\t2\t2
            1970-01-01T00:00:01.000000Z\t2\t3
            1970-01-01T00:00:01.000000Z\t2\t4
            1970-01-01T00:00:01.000000Z\t2\t5
            1970-01-01T00:00:02.000000Z\t1\t6
            1970-01-01T00:00:02.000000Z\t1\t7
            1970-01-01T00:00:02.000000Z\t1\t8
            1970-01-01T00:00:02.000000Z\t1\t9
            1970-01-01T00:00:02.000000Z\t1\t10
            1970-01-01T00:00:03.000000Z\t2\t2
            1970-01-01T00:00:03.000000Z\t2\t3
            1970-01-01T00:00:03.000000Z\t2\t4
            1970-01-01T00:00:03.000000Z\t2\t5
            1970-01-01T00:00:03.000000Z\t2\t1
            1970-01-01T00:00:04.000000Z\t1\t6
            1970-01-01T00:00:04.000000Z\t1\t7
            1970-01-01T00:00:04.000000Z\t1\t8
            1970-01-01T00:00:04.000000Z\t1\t9
            1970-01-01T00:00:04.000000Z\t1\t10
            """;
    private static final String SWAPPABLE_JOIN = "SELECT a.ts, a.k, b.v FROM a JOIN b ON a.k = b.k";

    @Test
    public void testCreateMatViewOverSwappableJoin() throws Exception {
        assertMemoryLeak(() -> {
            createSwappableJoinTables();
            execute("""
                    CREATE MATERIALIZED VIEW mv WITH BASE a AS (
                        SELECT a.ts, sum(b.v) s FROM a JOIN b ON a.k = b.k SAMPLE BY 2s
                    )""");
            drainWalAndMatViewQueues();
            assertQuery("mv")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\ts
                            1970-01-01T00:00:00.000000Z\t15
                            1970-01-01T00:00:02.000000Z\t55
                            1970-01-01T00:00:04.000000Z\t40
                            """);
            execute("INSERT INTO a VALUES ('1970-01-01T00:00:05.000000Z', 2)");
            drainWalAndMatViewQueues();
            assertQuery("mv")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\ts
                            1970-01-01T00:00:00.000000Z\t15
                            1970-01-01T00:00:02.000000Z\t55
                            1970-01-01T00:00:04.000000Z\t55
                            """);
        });
    }

    @Test
    public void testCreateTableAsKeyedAggregateOverSwappableJoin() throws Exception {
        assertMemoryLeak(() -> {
            createSwappableJoinTables();
            final String sql = "SELECT k, count() FROM (" + SWAPPABLE_JOIN + ") GROUP BY k";
            sqlExecutionContext.pushTimestampRequiredFlag(true);
            try {
                // The aggregate reads the direction of its input for its order-sensitive functions, which fixes the master.
                assertPlanned(sql, JoinPlan.class,
                        join -> Assert.assertEquals(JoinInput.MasterSide.FIXED, join.getOrderedInputs().getQuick(1).getMasterSide()));
            } finally {
                sqlExecutionContext.popTimestampRequiredFlag();
            }
            execute("CREATE TABLE c AS (" + sql + ")");
            assertQuery("c")
                    .expectSize()
                    .returns("""
                            k\tcount
                            2\t10
                            1\t10
                            """);
        });
    }

    @Test
    public void testCreateTableAsProjectionWithoutTimestampOverSwappableJoin() throws Exception {
        assertMemoryLeak(() -> {
            createSwappableJoinTables();
            final String sql = "SELECT k, v FROM (" + SWAPPABLE_JOIN + ")";
            sqlExecutionContext.pushTimestampRequiredFlag(true);
            try {
                assertPlanned(sql, JoinPlan.class,
                        join -> Assert.assertEquals(JoinInput.MasterSide.SMALLER, join.getOrderedInputs().getQuick(1).getMasterSide()));
            } finally {
                sqlExecutionContext.popTimestampRequiredFlag();
            }
            execute("CREATE TABLE c AS (" + sql + ")");
            assertQuery("c")
                    .expectSize()
                    .returns("""
                            k\tv
                            2\t1
                            2\t1
                            2\t2
                            2\t2
                            2\t3
                            2\t3
                            2\t4
                            2\t4
                            2\t5
                            2\t5
                            1\t6
                            1\t6
                            1\t7
                            1\t7
                            1\t8
                            1\t8
                            1\t9
                            1\t9
                            1\t10
                            1\t10
                            """);
        });
    }

    @Test
    public void testCreateTableAsSwappableJoinInheritsTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            createSwappableJoinTables();
            execute("CREATE TABLE c AS (" + SWAPPABLE_JOIN + ")");
            assertQuery("c")
                    .timestamp("ts")
                    .expectSize()
                    .returns(FIXED_JOIN_ROWS);
        });
    }

    @Test
    public void testCreateTableAsSwappableJoinWithTimestampClause() throws Exception {
        assertMemoryLeak(() -> {
            createSwappableJoinTables();
            execute("CREATE TABLE c AS (" + SWAPPABLE_JOIN + ") TIMESTAMP(ts) PARTITION BY DAY");
            assertQuery("c")
                    .timestamp("ts")
                    .expectSize()
                    .returns(RESORTED_JOIN_ROWS);
        });
    }

    @Test
    public void testFillAlgorithm() throws Exception {
        assertMemoryLeak(() -> {
            createSampledTable();
            final String sampleByRows = "SELECT ts, sym, first(i) f FROM a SAMPLE BY 1h FILL(PREV) ALIGN TO FIRST OBSERVATION";
            final String sorted = "SELECT ts, sym, first(i) f FROM a SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR";
            final String inputOrder = "SELECT ts, sym, first(i) f FROM a WHERE sym = 'S' SAMPLE BY 1h FILL(NULL) ALIGN TO FIRST OBSERVATION";
            assertPlanned(sampleByRows, FillPlan.class, fill -> Assert.assertEquals(FillPlan.Algorithm.SAMPLE_BY_ROWS, fill.getAlgorithm()));
            assertPlanned(sorted, FillPlan.class, fill -> Assert.assertEquals(FillPlan.Algorithm.SORTED, fill.getAlgorithm()));
            assertPlanned(inputOrder, FillPlan.class, fill -> Assert.assertEquals(FillPlan.Algorithm.INPUT_ORDER, fill.getAlgorithm()));
            final String prevRows = """
                    ts\tsym\tf
                    2024-01-01T00:00:00.000000Z\tS\t1
                    2024-01-01T00:00:00.000000Z\tT\t2
                    2024-01-01T01:00:00.000000Z\tS\t1
                    2024-01-01T01:00:00.000000Z\tT\t2
                    2024-01-01T02:00:00.000000Z\tS\t3
                    2024-01-01T02:00:00.000000Z\tT\t2
                    """;
            assertQuery(sampleByRows)
                    .timestamp("ts")
                    .noRandomAccess()
                    .withPlan("""
                            Sample By Fill
                              stride: '1h'
                              fill: prev
                                Sample By
                                  keys: [ts,sym]
                                  values: [first(i)]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                            """)
                    .returns(prevRows);
            assertQuery(sorted)
                    .timestamp("ts")
                    .noRandomAccess()
                    .withPlan("""
                            Sample By Fill
                              stride: '1h'
                              fill: prev
                                Encode sort light
                                  keys: [ts]
                                    Async Group By workers: 1
                                      keys: [ts,sym]
                                      keyFunctions: [timestamp_floor_utc('1h',ts)]
                                      values: [first(i)]
                                      filter: null
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: a
                            """)
                    .returns(prevRows);
            assertQuery(inputOrder)
                    .timestamp("ts")
                    .noRandomAccess()
                    .withPlan("""
                            Sample By Fill
                              stride: '1h'
                              fill: null
                                SampleByFirstLast
                                  keys: [ts, sym]
                                  values: [first(i)]
                                    DeferredSingleSymbolFilterPageFrame
                                        Index forward scan on: sym
                                          filter: sym=1
                                        Frame forward scan on: a
                            """)
                    .returns("""
                            ts\tsym\tf
                            2024-01-01T00:00:00.000000Z\tS\t1
                            2024-01-01T01:00:00.000000Z\tS\tnull
                            2024-01-01T02:00:00.000000Z\tS\t3
                            """);
            assertQuery("SELECT ts, sym, first(v) f FROM a WHERE sym = 'S' SAMPLE BY 1h FILL(PREV) ALIGN TO FIRST OBSERVATION")
                    .fails(0, "FILL(PREV) cannot re-read rows of a base without random access");
        });
    }

    @Test
    public void testInsertAsSelectSwappableJoin() throws Exception {
        assertMemoryLeak(() -> {
            createSwappableJoinTables();
            execute("CREATE TABLE c (ts TIMESTAMP, k INT, v INT) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO c " + SWAPPABLE_JOIN);
            assertQuery("c")
                    .timestamp("ts")
                    .expectSize()
                    .returns(RESORTED_JOIN_ROWS);
        });
    }

    @Test
    public void testLatestByAlgorithm() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String ascending = "SELECT * FROM (SELECT * FROM t WHERE x > 0) LATEST ON ts PARTITION BY x";
            assertPlanned(ascending, LatestByPlan.class, latest -> Assert.assertEquals(LatestByPlan.Algorithm.ASCENDING_LIGHT, latest.getAlgorithm()));
            assertPlanned("SELECT * FROM (SELECT * FROM t ORDER BY ts DESC) LATEST ON ts PARTITION BY x", LatestByPlan.class,
                    latest -> Assert.assertEquals(LatestByPlan.Algorithm.LIGHT, latest.getAlgorithm()));
            assertQuery(ascending)
                    .expectSize()
                    .withPlan("""
                            LatestBy light order_by_timestamp: true
                                Async JIT Filter workers: 1
                                  filter: 0<x
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                            """)
                    .returns("""
                            ts\tx
                            2024-01-01T00:00:00.000000Z\t1
                            2024-01-01T01:00:00.000000Z\t2
                            2024-01-01T02:00:00.000000Z\t3
                            2024-01-02T00:00:00.000000Z\t4
                            """);
        });
    }

    @Test
    public void testLightHashJoinMasterSide() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String unordered = "SELECT a.x, b.y FROM t a JOIN u b ON a.x = b.x";
            final String ordered = unordered + " ORDER BY a.ts";
            assertPlanned(unordered, JoinPlan.class,
                    join -> Assert.assertEquals(JoinInput.MasterSide.SMALLER, join.getOrderedInputs().getQuick(1).getMasterSide()));
            assertPlanned(ordered, JoinPlan.class,
                    join -> Assert.assertEquals(JoinInput.MasterSide.FIXED, join.getOrderedInputs().getQuick(1).getMasterSide()));
            // The smaller master drives the hash join from its slave unless a consumer reads the master's order.
            assertQuery(unordered)
                    .noRandomAccess()
                    .withPlan("""
                            SelectedRecord
                                Hash Join Light
                                  condition: b.x=a.x
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                                    Hash
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: u
                            """)
                    .returns("""
                            x\ty
                            2\t1
                            3\t2
                            4\t3
                            1\t4
                            2\t5
                            3\t6
                            4\t7
                            1\t8
                            2\t9
                            3\t10
                            4\t11
                            1\t12
                            """);
            assertQuery(ordered)
                    .noRandomAccess()
                    .withPlan("""
                            SelectedRecord
                                SelectedRecord
                                    Hash Join Light
                                      condition: b.x=a.x
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t
                                        Hash
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: u
                            """)
                    .returns("""
                            x\ty
                            1\t12
                            1\t8
                            1\t4
                            2\t9
                            2\t5
                            2\t1
                            3\t10
                            3\t6
                            3\t2
                            4\t11
                            4\t7
                            4\t3
                            """);
        });
    }

    @Test
    public void testLimitApplication() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String sql = "SELECT * FROM t WHERE x > 1 LIMIT 2";
            assertPlanned(sql, LimitPlan.class, limit -> Assert.assertEquals(LimitPlan.Application.INPUT, limit.getApplication()));
            assertPlanned("SELECT * FROM t LIMIT 2", LimitPlan.class, limit -> Assert.assertEquals(LimitPlan.Application.OPERATOR, limit.getApplication()));
            assertQuery(sql)
                    .timestamp("ts")
                    .withPlan("""
                            Async JIT Filter workers: 1
                              limit: 2
                              filter: 1<x
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """)
                    .returns("""
                            ts\tx
                            2024-01-01T01:00:00.000000Z\t2
                            2024-01-01T02:00:00.000000Z\t3
                            """);
        });
    }

    @Test
    public void testMarkoutJoinAlgorithm() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, order_ts TIMESTAMP) TIMESTAMP(order_ts)");
            execute("INSERT INTO orders VALUES (1, 0::TIMESTAMP), (2, 1::TIMESTAMP)");
            final String sql = """
                    SELECT /*+ markout_horizon(orders offsets) */ id, order_ts + usec_offs AS ts
                    FROM orders CROSS JOIN (SELECT 1_000_000 * (x-1) AS usec_offs FROM long_sequence(2)) offsets
                    ORDER BY order_ts + usec_offs
                    """;
            assertPlanned(sql, JoinPlan.class,
                    join -> Assert.assertEquals(JoinInput.Algorithm.MARKOUT, join.getOrderedInputs().getQuick(1).getAlgorithm()));
            assertPlanned(sql, SortPlan.class, sort -> Assert.assertEquals(SortPlan.Algorithm.INPUT_ORDER, sort.getAlgorithm()));
            assertQuery(sql)
                    .noRandomAccess()
                    .expectSize()
                    .withPlan("""
                            VirtualRecord
                              functions: [orders.id,orders.order_ts+offsets.usec_offs]
                                Markout Horizon Join timestampColumn: 1 offsetColumn: 0
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: orders
                                    VirtualRecord
                                      functions: [1000000*x-1]
                                        long_sequence count: 2
                            """)
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
    public void testOrderedSwappableJoinDeclaresTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            createSwappableJoinTables();
            final String sql = SWAPPABLE_JOIN + " ORDER BY ts";
            assertPlanned(sql, JoinPlan.class,
                    join -> Assert.assertEquals(JoinInput.MasterSide.FIXED, join.getOrderedInputs().getQuick(1).getMasterSide()));
            assertQuery(sql)
                    .timestamp("ts")
                    .noRandomAccess()
                    .withPlan("""
                            SelectedRecord
                                Hash Join Light
                                  condition: b.k=a.k
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                                    Hash
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: b
                            """)
                    .returns(FIXED_JOIN_ROWS);
        });
    }

    @Test
    public void testSampleByAlgorithm() throws Exception {
        assertMemoryLeak(() -> {
            createSampledTable();
            final String firstLast = "SELECT ts, sym, first(i) f, last(i) l FROM a WHERE sym = 'S' SAMPLE BY 1h ALIGN TO FIRST OBSERVATION";
            final String fillNone = "SELECT ts, sym, first(i) f FROM a SAMPLE BY 1h ALIGN TO FIRST OBSERVATION";
            final String interpolate = "SELECT ts, first(i) f FROM a SAMPLE BY 1h FILL(LINEAR) ALIGN TO FIRST OBSERVATION";
            final String fillValue = "SELECT ts, first(i) f, last(i) l FROM a SAMPLE BY 1h FILL(LINEAR, 42) ALIGN TO FIRST OBSERVATION";
            assertPlanned(firstLast, SampleByPlan.class, sample -> Assert.assertEquals(SampleByPlan.Algorithm.FIRST_LAST_INDEX, sample.getAlgorithm()));
            assertPlanned(fillNone, SampleByPlan.class, sample -> Assert.assertEquals(SampleByPlan.Algorithm.FILL_NONE, sample.getAlgorithm()));
            assertPlanned(interpolate, SampleByPlan.class, sample -> Assert.assertEquals(SampleByPlan.Algorithm.INTERPOLATE, sample.getAlgorithm()));
            assertPlanned(fillValue, SampleByPlan.class, sample -> Assert.assertEquals(SampleByPlan.Algorithm.FILL_VALUE, sample.getAlgorithm()));
            assertQuery(firstLast)
                    .timestamp("ts")
                    .noRandomAccess()
                    .withPlan("""
                            SampleByFirstLast
                              keys: [ts, sym]
                              values: [first(i), last(i)]
                                DeferredSingleSymbolFilterPageFrame
                                    Index forward scan on: sym
                                      filter: sym=1
                                    Frame forward scan on: a
                            """)
                    .returns("""
                            ts\tsym\tf\tl
                            2024-01-01T00:00:00.000000Z\tS\t1\t1
                            2024-01-01T02:00:00.000000Z\tS\t3\t3
                            """);
            assertQuery(fillNone)
                    .timestamp("ts")
                    .noRandomAccess()
                    .withPlan("""
                            Sample By
                              keys: [ts,sym]
                              values: [first(i)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                            """)
                    .returns("""
                            ts\tsym\tf
                            2024-01-01T00:00:00.000000Z\tS\t1
                            2024-01-01T00:00:00.000000Z\tT\t2
                            2024-01-01T02:00:00.000000Z\tS\t3
                            """);
            assertQuery(interpolate)
                    .timestamp("ts")
                    .expectSize()
                    .withPlan("""
                            Sample By
                              fill: linear
                              keys: [ts]
                              values: [first(i)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                            """)
                    .returns("""
                            ts\tf
                            2024-01-01T00:00:00.000000Z\t1
                            2024-01-01T01:00:00.000000Z\t2
                            2024-01-01T02:00:00.000000Z\t3
                            """);
            assertQuery(fillValue)
                    .timestamp("ts")
                    .noRandomAccess()
                    .withPlan("""
                            Sample By
                              fill: value
                              values: [Interpolated(first(i)),last(i)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                            """)
                    .returns("""
                            ts\tf\tl
                            2024-01-01T00:00:00.000000Z\t1\t2
                            2024-01-01T01:00:00.000000Z\t2\t42
                            2024-01-01T02:00:00.000000Z\t3\t3
                            """);

            execute("CREATE TABLE p (sym SYMBOL INDEX, i INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO p VALUES ('S', 1, '1970-01-01T00:00:00.000000Z'), ('S', 3, '1970-01-01T02:00:00.000000Z'), ('S', 5, '1970-01-02T00:00:00.000000Z')");
            drainWalQueue();
            execute("ALTER TABLE p CONVERT PARTITION TO PARQUET LIST '1970-01-01'");
            drainWalQueue();
            final String parquet = "SELECT ts, sym, first(i) f FROM p WHERE sym = 'S' SAMPLE BY 1d ALIGN TO FIRST OBSERVATION";
            assertPlanned(parquet, SampleByPlan.class, sample -> Assert.assertEquals(SampleByPlan.Algorithm.FIRST_LAST_INDEX, sample.getAlgorithm()));
            execute("ALTER TABLE p ALTER COLUMN i TYPE LONG");
            drainWalQueue();
            assertPlanned(parquet, SampleByPlan.class, sample -> Assert.assertEquals(SampleByPlan.Algorithm.FILL_NONE, sample.getAlgorithm()));
            assertQuery(parquet)
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("""
                            ts\tsym\tf
                            1970-01-01T00:00:00.000000Z\tS\t1
                            1970-01-02T00:00:00.000000Z\tS\t5
                            """);
        });
    }

    @Test
    public void testSortAlgorithm() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertSortAlgorithm(SortPlan.Algorithm.INPUT_ORDER, "SELECT * FROM t ORDER BY ts DESC");
            assertSortAlgorithm(SortPlan.Algorithm.LIGHT, "SELECT * FROM t ORDER BY x");
            assertSortAlgorithm(SortPlan.Algorithm.MATERIALIZED, "SELECT * FROM (SELECT x, timestamp_sequence(0, 1) v FROM t) ORDER BY x");
            assertSortAlgorithm(SortPlan.Algorithm.LIMITED, "SELECT * FROM t ORDER BY x LIMIT 1, 3");
            assertSortAlgorithm(SortPlan.Algorithm.PRESORTED_LIMITED, "SELECT * FROM t ORDER BY ts, x LIMIT 1, 3");
            assertSortAlgorithm(SortPlan.Algorithm.LONG_TOP_K, "SELECT x, sum(y) s FROM u GROUP BY x ORDER BY s DESC LIMIT 2");
            assertSortAlgorithm(SortPlan.Algorithm.PARALLEL_FILTERED_TOP_K, "SELECT * FROM t WHERE x > 1 ORDER BY x DESC LIMIT 2");
            assertSortAlgorithm(SortPlan.Algorithm.PARALLEL_TOP_K, "SELECT * FROM t WHERE now() < '2100-01-01' ORDER BY x DESC LIMIT 2");
            assertQuery("SELECT * FROM t ORDER BY ts DESC")
                    .timestampDesc("ts")
                    .expectSize()
                    .withPlan("""
                            PageFrame
                                Row backward scan
                                Frame backward scan on: t
                            """)
                    .returns("""
                            ts\tx
                            2024-01-02T00:00:00.000000Z\t4
                            2024-01-01T02:00:00.000000Z\t3
                            2024-01-01T01:00:00.000000Z\t2
                            2024-01-01T00:00:00.000000Z\t1
                            """);
            assertQuery("SELECT * FROM (SELECT x, timestamp_sequence(0, 1) v FROM t) ORDER BY x")
                    .expectSize()
                    .withPlan("""
                            Encode sort
                              keys: [x]
                                VirtualRecord
                                  functions: [x,timestamp_sequence(0,1)]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                            """)
                    .returns("""
                            x\tv
                            1\t1970-01-01T00:00:00.000000Z
                            2\t1970-01-01T00:00:00.000001Z
                            3\t1970-01-01T00:00:00.000002Z
                            4\t1970-01-01T00:00:00.000003Z
                            """);
            assertQuery("SELECT * FROM t ORDER BY ts, x LIMIT 1, 3")
                    .timestamp("ts")
                    .expectSize()
                    .withPlan("""
                            Encode sort light lo: 1 hi: 3 partiallySorted: true
                              keys: [ts, x]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """)
                    .returns("""
                            ts\tx
                            2024-01-01T01:00:00.000000Z\t2
                            2024-01-01T02:00:00.000000Z\t3
                            """);
            assertQuery("SELECT x, sum(y) s FROM u GROUP BY x ORDER BY s DESC LIMIT 2")
                    .expectSize()
                    .withPlan("""
                            Long Top K lo: 2
                              keys: [s desc]
                                Async Group By workers: 1
                                  keys: [x]
                                  values: [sum(y)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: u
                            """)
                    .returns("""
                            x\ts
                            1\t24
                            4\t21
                            """);
            assertQuery("SELECT * FROM t WHERE x > 1 ORDER BY x DESC LIMIT 2")
                    .expectSize()
                    .withPlan("""
                            Async JIT Top K lo: 2 workers: 1
                              filter: 1<x
                              keys: [x desc]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """)
                    .returns("""
                            ts\tx
                            2024-01-02T00:00:00.000000Z\t4
                            2024-01-01T02:00:00.000000Z\t3
                            """);
        });
    }

    @Test
    public void testSwappableJoinDeclaresNoTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            createSwappableJoinTables();
            assertPlanned(SWAPPABLE_JOIN, JoinPlan.class,
                    join -> Assert.assertEquals(JoinInput.MasterSide.SMALLER, join.getOrderedInputs().getQuick(1).getMasterSide()));
            assertQuery(SWAPPABLE_JOIN)
                    .noRandomAccess()
                    .withPlan("""
                            SelectedRecord
                                Hash Join Light
                                  condition: b.k=a.k
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                                    Hash
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: b
                            """)
                    .returns("""
                            ts\tk\tv
                            1970-01-01T00:00:03.000000Z\t2\t1
                            1970-01-01T00:00:01.000000Z\t2\t1
                            1970-01-01T00:00:03.000000Z\t2\t2
                            1970-01-01T00:00:01.000000Z\t2\t2
                            1970-01-01T00:00:03.000000Z\t2\t3
                            1970-01-01T00:00:01.000000Z\t2\t3
                            1970-01-01T00:00:03.000000Z\t2\t4
                            1970-01-01T00:00:01.000000Z\t2\t4
                            1970-01-01T00:00:03.000000Z\t2\t5
                            1970-01-01T00:00:01.000000Z\t2\t5
                            1970-01-01T00:00:04.000000Z\t1\t6
                            1970-01-01T00:00:02.000000Z\t1\t6
                            1970-01-01T00:00:04.000000Z\t1\t7
                            1970-01-01T00:00:02.000000Z\t1\t7
                            1970-01-01T00:00:04.000000Z\t1\t8
                            1970-01-01T00:00:02.000000Z\t1\t8
                            1970-01-01T00:00:04.000000Z\t1\t9
                            1970-01-01T00:00:02.000000Z\t1\t9
                            1970-01-01T00:00:04.000000Z\t1\t10
                            1970-01-01T00:00:02.000000Z\t1\t10
                            """);
        });
    }

    @Test
    public void testTemporalJoinAlgorithm() throws Exception {
        assertMemoryLeak(() -> {
            createTemporalJoinTables();
            final String unkeyed = """
                    ts\tk\tv
                    1970-01-01T00:00:01.000000Z\t1\t10
                    1970-01-01T00:00:02.000000Z\t2\t20
                    1970-01-01T00:00:03.000000Z\t1\t30
                    1970-01-01T00:00:04.000000Z\t2\t7
                    """;
            final String keyed = """
                    ts\tk\tv
                    1970-01-01T00:00:01.000000Z\t1\t10
                    1970-01-01T00:00:02.000000Z\t2\t20
                    1970-01-01T00:00:03.000000Z\t1\t30
                    1970-01-01T00:00:04.000000Z\t2\t5
                    """;
            assertTemporalJoin(JoinInput.Algorithm.TEMPORAL_TIME_FRAME, "SELECT a.ts, a.k, b.v FROM a ASOF JOIN b",
                    temporalPlan("AsOf Join Fast"), unkeyed);
            assertTemporalJoin(JoinInput.Algorithm.TEMPORAL_TIME_FRAME, "SELECT a.ts, a.k, b.v FROM a ASOF JOIN b ON (ts)",
                    temporalPlan("AsOf Join Fast"), unkeyed);
            assertTemporalJoin(JoinInput.Algorithm.TEMPORAL_TIME_FRAME, "SELECT a.ts, a.k, b.v FROM a ASOF JOIN b ON (k)",
                    temporalPlan("AsOf Join Fast\n      condition: b.k=a.k"), keyed);
            assertTemporalJoin(JoinInput.Algorithm.TEMPORAL, "SELECT /*+ asof_linear(a b) */ a.ts, a.k, b.v FROM a ASOF JOIN b",
                    temporalPlan("AsOf Join"), unkeyed);
            assertTemporalJoin(JoinInput.Algorithm.TEMPORAL, "SELECT /*+ asof_linear(a b) */ a.ts, a.k, b.v FROM a ASOF JOIN b ON (k)",
                    temporalPlan("AsOf Join Light\n      condition: b.k=a.k"), keyed);
            assertTemporalJoin(JoinInput.Algorithm.TEMPORAL, "SELECT a.ts, a.k, b.v FROM a ASOF JOIN (SELECT ts, k, v - 0 AS v FROM b) b",
                    """
                            SelectedRecord
                                AsOf Join
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                                    VirtualRecord
                                      functions: [ts,v-0]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: b
                            """, unkeyed);
            assertTemporalJoin(JoinInput.Algorithm.TEMPORAL_TIME_FRAME, "SELECT a.ts, a.k, b.v FROM a LT JOIN b",
                    temporalPlan("Lt Join Fast"), unkeyed);
            assertTemporalJoin(JoinInput.Algorithm.TEMPORAL_TIME_FRAME, "SELECT a.ts, a.k, b.v FROM a LT JOIN b ON (ts)",
                    temporalPlan("Lt Join Fast"), unkeyed);
            assertTemporalJoin(JoinInput.Algorithm.TEMPORAL, "SELECT a.ts, a.k, b.v FROM a LT JOIN b ON (k)",
                    temporalPlan("Lt Join Light\n      condition: b.k=a.k"), keyed);
            assertTemporalJoin(JoinInput.Algorithm.TEMPORAL, "SELECT /*+ asof_linear(a b) */ a.ts, a.k, b.v FROM a LT JOIN b",
                    temporalPlan("Lt Join"), unkeyed);
        });
    }

    @Test
    public void testTimestampOrderAggregateFlags() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertPlanned("SELECT twap(x, ts), sparkline(x), sum(x) FROM t", AggregatePlan.class, aggregate -> {
                final int mask = BoundExpression.ASCENDING_TIMESTAMP | BoundExpression.TIMESTAMP_ARGUMENT;
                Assert.assertEquals(mask, aggregate.getAggregates().getQuick(0).getFunctionFlags() & mask);
                Assert.assertEquals(BoundExpression.ASCENDING_TIMESTAMP, aggregate.getAggregates().getQuick(1).getFunctionFlags() & mask);
                Assert.assertEquals(0, aggregate.getAggregates().getQuick(2).getFunctionFlags() & mask);
            });
        });
    }

    @Test
    public void testTimestampOrderAggregatesOverSharedDomain() throws Exception {
        assertMemoryLeak(() -> {
            createKeyedTable();
            assertQuery("SELECT a.k, l.tw FROM t a CROSS JOIN LATERAL (SELECT twap(x, ts) tw FROM ((SELECT * FROM t) TIMESTAMP(ts)) b WHERE b.k = a.k) l ORDER BY a.ts")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            k\ttw
                            1\t4.0
                            2\t3.5
                            0\t4.5
                            1\t4.0
                            2\t3.5
                            0\t4.5
                            1\t4.0
                            2\t3.5
                            0\t4.5
                            1\t4.0
                            """);
            assertQuery("SELECT a.k, l.tw FROM t a CROSS JOIN LATERAL (SELECT twap(x, ts) tw FROM ((SELECT * FROM t ORDER BY x) TIMESTAMP(ts)) b WHERE b.k = a.k) l ORDER BY a.ts")
                    .noLeakCheck()
                    .fails(53, "twap() requires the base query to provide ascending designated timestamp order");
        });
    }

    @Test
    public void testTimestampOrderValidation() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String left = "left side of time series join doesn't have ASC timestamp order";
            final String right = "right side of time series join doesn't have ASC timestamp order";
            assertQuery("SELECT * FROM (SELECT * FROM t ORDER BY ts DESC) a ASOF JOIN t b").noLeakCheck().fails(51, left);
            assertQuery("SELECT * FROM t a LT JOIN (SELECT * FROM t ORDER BY ts DESC) b").noLeakCheck().fails(18, right);
            assertQuery("SELECT * FROM t a SPLICE JOIN (SELECT * FROM t ORDER BY ts DESC) b").noLeakCheck().fails(18, right);
            assertQuery("SELECT * FROM (SELECT * FROM t ORDER BY ts DESC) a SPLICE JOIN (SELECT * FROM t LIMIT 3) b").noLeakCheck().fails(51, left);
            assertQuery("SELECT t.ts, sum(b.x) FROM (SELECT * FROM t ORDER BY ts DESC) t WINDOW JOIN t b RANGE BETWEEN 1 SECOND PRECEDING AND CURRENT ROW")
                    .noLeakCheck().fails(64, left);
            assertQuery("SELECT t.ts, sum(b.x) FROM t WINDOW JOIN (SELECT * FROM t ORDER BY ts DESC) b RANGE BETWEEN 1 SECOND PRECEDING AND CURRENT ROW")
                    .noLeakCheck().fails(29, right);
            assertQuery("SELECT sum(b.x) FROM (SELECT * FROM t ORDER BY ts DESC) a HORIZON JOIN t b RANGE FROM 0s TO 0s STEP 1s AS h")
                    .noLeakCheck().fails(58, left);
            assertQuery("SELECT sum(b.x) FROM t a HORIZON JOIN (SELECT * FROM t ORDER BY ts DESC) b RANGE FROM 0s TO 0s STEP 1s AS h")
                    .noLeakCheck().fails(25, right);

            assertQuery("SELECT ts, sparkline(x), twap(x, ts) FROM (SELECT * FROM t ORDER BY ts DESC) SAMPLE BY 1h")
                    .noLeakCheck().fails(0, "base query does not provide ASC order over designated TIMESTAMP column");
            assertQuery("SELECT t.ts, sum(y) FROM (SELECT * FROM t ORDER BY ts DESC) t JOIN u ON (x) SAMPLE BY 1h")
                    .noLeakCheck().fails(0, "ASC order over TIMESTAMP column is required but not provided");
            assertQuery("SELECT ts, twap(c, ts) FROM (SELECT ts, count() c FROM t GROUP BY ts) SAMPLE BY 1h")
                    .noLeakCheck().fails(0, "base query does not provide designated TIMESTAMP column");

            final String twapArgument = "twap() requires the table's designated timestamp as the second argument";
            final String twapOrder = "twap() requires the base query to provide ascending designated timestamp order";
            final String sparklineOrder = "sparkline() requires the base query to provide ascending designated timestamp order";
            assertQuery("SELECT twap(x, ts), twap(x, ts + 1) FROM (SELECT * FROM t ORDER BY x) TIMESTAMP(ts)").noLeakCheck().fails(7, twapOrder);
            assertQuery("SELECT twap(x, x::TIMESTAMP) FROM (SELECT * FROM t ORDER BY x) TIMESTAMP(ts)").noLeakCheck().fails(7, twapArgument);
            assertQuery("SELECT sparkline(x) FROM (SELECT * FROM t ORDER BY x) TIMESTAMP(ts)").noLeakCheck().fails(7, sparklineOrder);
            assertQuery("SELECT ts, twap(x, ts + 1) FROM t SAMPLE BY 1h").noLeakCheck().fails(11, twapArgument);
            assertQuery("SELECT ts, sparkline(x) FROM (SELECT * FROM t ORDER BY x) TIMESTAMP(ts) SAMPLE BY 1h").noLeakCheck().fails(11, sparklineOrder);
            assertQuery("SELECT t.ts, twap(b.x, t.ts) FROM t WINDOW JOIN t b RANGE BETWEEN 1 SECOND PRECEDING AND CURRENT ROW")
                    .noLeakCheck().fails(13, twapOrder);
            assertQuery("SELECT t.ts, twap(b.x, b.ts) FROM t WINDOW JOIN t b RANGE BETWEEN 1 SECOND PRECEDING AND CURRENT ROW")
                    .noLeakCheck().fails(13, twapArgument);
            assertQuery("SELECT sparkline(b.x) FROM t a HORIZON JOIN t b RANGE FROM 0s TO 0s STEP 1s AS h").noLeakCheck().fails(7, sparklineOrder);
            assertQuery("SELECT twap(b.x, b.ts) FROM t a HORIZON JOIN t b RANGE FROM 0s TO 0s STEP 1s AS h").noLeakCheck().fails(7, twapArgument);
        });
    }

    @Test
    public void testUnionAllMerge() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String sql = "SELECT * FROM (SELECT ts, x FROM t UNION ALL SELECT ts, x FROM t WHERE x > 2) ORDER BY ts DESC";
            assertPlanned(sql, SetOperationPlan.class, union -> {
                Assert.assertTrue(union.isMerged());
                Assert.assertNull(union.getRightBranchSort());
            });
            assertQuery(sql)
                    .timestampDesc("ts")
                    .noRandomAccess()
                    .withPlan("""
                            Union All Merge
                              order: [ts desc]
                              branches: 2
                                PageFrame
                                    Row backward scan
                                    Frame backward scan on: t
                                Async JIT Filter workers: 1
                                  filter: 2<x
                                    PageFrame
                                        Row backward scan
                                        Frame backward scan on: t
                            """)
                    .returns("""
                            ts\tx
                            2024-01-02T00:00:00.000000Z\t4
                            2024-01-02T00:00:00.000000Z\t4
                            2024-01-01T02:00:00.000000Z\t3
                            2024-01-01T02:00:00.000000Z\t3
                            2024-01-01T01:00:00.000000Z\t2
                            2024-01-01T00:00:00.000000Z\t1
                            """);
        });
    }

    @Test
    public void testUnionSymbolRestoration() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE s (sym SYMBOL, x INT)");
            execute("INSERT INTO s VALUES ('a', 1), ('b', 2), ('a', 3)");
            final String union = "SELECT sym FROM s UNION SELECT sym FROM s";
            assertQuery(union)
                    .noRandomAccess()
                    .withPlan("""
                            UnionSymbolCast
                              functions: [sym::symbol]
                                Union
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: s
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: s
                            """)
                    .returns("""
                            sym
                            a
                            b
                            """);
            final String intersect = "SELECT sym FROM s INTERSECT SELECT sym FROM s";
            assertQuery(intersect)
                    .withPlan("""
                            Intersect
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: s
                                Hash
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: s
                            """)
                    .returns("""
                            sym
                            a
                            b
                            """);
            final String numeric = "SELECT x FROM s UNION ALL SELECT x FROM s";
            assertQuery(numeric)
                    .noRandomAccess()
                    .expectSize()
                    .withPlan("""
                            Union All
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: s
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: s
                            """)
                    .returns("""
                            x
                            1
                            2
                            3
                            1
                            2
                            3
                            """);
        });
    }

    @Test
    public void testVectorisedAggregateOrder() throws Exception {
        assertMemoryLeak(() -> {
            createKeyedTable();
            execute("CREATE TABLE v AS (SELECT (x * 1_000_000)::TIMESTAMP ts, 1::INT k FROM long_sequence(5)) TIMESTAMP(ts) PARTITION BY DAY");
            final String join = "SELECT * FROM ((SELECT k, max(ts) ts FROM (SELECT * FROM v ORDER BY ts DESC) GROUP BY k) TIMESTAMP(ts)) a ASOF JOIN t b";
            assertPlanned(join, AggregatePlan.class, aggregate -> Assert.assertEquals(AggregatePlan.Algorithm.VECTORISED, aggregate.getAlgorithm()));
            assertQuery(join)
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .timestamp("ts")
                    .returns("""
                            k\tts\tts1\tk1\tx
                            1\t1970-01-01T00:00:05.000000Z\t1970-01-01T00:00:05.000000Z\t2\t5
                            """);
            assertQuery("SELECT ts, sum(x) FROM ((SELECT k, max(ts) ts, sum(x) x FROM (SELECT * FROM t ORDER BY ts DESC) GROUP BY k) TIMESTAMP(ts)) SAMPLE BY 1h")
                    .noLeakCheck()
                    .noRandomAccess()
                    .timestamp("ts")
                    .returns("""
                            ts\tsum
                            1970-01-01T00:00:00.000000Z\t55
                            """);

            execute("CREATE TABLE p AS (SELECT timestamp_sequence(0, 3_600_000_000) ts, (x % 2)::INT k, x::INT d FROM long_sequence(30)) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            execute("ALTER TABLE p CONVERT PARTITION TO PARQUET LIST '1970-01-01'");
            drainWalQueue();
            final String keyed = "SELECT k, sum(d) FROM p";
            assertPlanned(keyed, AggregatePlan.class, aggregate -> Assert.assertEquals(AggregatePlan.Algorithm.VECTORISED, aggregate.getAlgorithm()));
            execute("ALTER TABLE p ALTER COLUMN d TYPE DOUBLE");
            drainWalQueue();
            assertPlanned(keyed, AggregatePlan.class, aggregate -> Assert.assertEquals(AggregatePlan.Algorithm.PARALLEL, aggregate.getAlgorithm()));
            assertQuery("SELECT * FROM ((SELECT k, max(ts) ts FROM (SELECT * FROM p ORDER BY ts DESC) GROUP BY k) TIMESTAMP(ts)) a ASOF JOIN t b")
                    .noLeakCheck()
                    .fails(106, "left side of time series join doesn't have ASC timestamp order");
            assertQuery("SELECT ts, sum(d) FROM ((SELECT k, max(ts) ts, sum(d) d FROM (SELECT * FROM p ORDER BY ts DESC) GROUP BY k) TIMESTAMP(ts)) SAMPLE BY 1h")
                    .noLeakCheck()
                    .fails(0, "base query does not provide ASC order over designated TIMESTAMP column");
        });
    }

    @Test
    public void testWindowAlgorithm() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String cached = "SELECT ts, c, sum(c) OVER (ORDER BY c) s FROM (SELECT ts, count() c FROM t SAMPLE BY 1d ALIGN TO FIRST OBSERVATION)";
            assertPlanned(cached, WindowPlan.class, window -> Assert.assertEquals(WindowPlan.Algorithm.CACHED, window.getAlgorithm()));
            assertQuery(cached)
                    .timestamp("ts")
                    .expectSize()
                    .withPlan("""
                            CachedWindow
                              orderedFunctions: [[c] => [sum(c) over (rows between unbounded preceding and current row)]]
                                Sample By
                                  fill: none
                                  values: [count(*)]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                            """)
                    .returns("""
                            ts\tc\ts
                            2024-01-01T00:00:00.000000Z\t3\t4.0
                            2024-01-02T00:00:00.000000Z\t1\t1.0
                            """);
        });
    }

    @Test
    public void testWindowOrderDelivered() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String delivered = "SELECT ts, x, sum(x) OVER (ORDER BY ts) s FROM t";
            final String reordered = "SELECT ts, x, sum(x) OVER (ORDER BY x) s FROM t";
            assertPlanned(delivered, WindowPlan.class, window -> {
                Assert.assertTrue(window.getSpecs().getQuick(0).isOrderDelivered());
                Assert.assertEquals(WindowPlan.Algorithm.STREAMING, window.getAlgorithm());
            });
            assertPlanned(reordered, WindowPlan.class, window -> {
                Assert.assertFalse(window.getSpecs().getQuick(0).isOrderDelivered());
                Assert.assertEquals(WindowPlan.Algorithm.CACHED_LIGHT, window.getAlgorithm());
            });
            assertQuery(delivered)
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .withPlan("""
                            Window
                              functions: [sum(x) over (rows between unbounded preceding and current row)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """)
                    .returns("""
                            ts\tx\ts
                            2024-01-01T00:00:00.000000Z\t1\t1.0
                            2024-01-01T01:00:00.000000Z\t2\t3.0
                            2024-01-01T02:00:00.000000Z\t3\t6.0
                            2024-01-02T00:00:00.000000Z\t4\t10.0
                            """);
            assertQuery(reordered)
                    .timestamp("ts")
                    .expectSize()
                    .withPlan("""
                            CachedWindowLight
                              orderedFunctions: [[x] => [sum(x) over (rows between unbounded preceding and current row)]]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """)
                    .returns("""
                            ts\tx\ts
                            2024-01-01T00:00:00.000000Z\t1\t1.0
                            2024-01-01T01:00:00.000000Z\t2\t3.0
                            2024-01-01T02:00:00.000000Z\t3\t6.0
                            2024-01-02T00:00:00.000000Z\t4\t10.0
                            """);
        });
    }

    @Test
    public void testWindowOverSwappableJoinDeclaresTimestampWhenOrdered() throws Exception {
        assertMemoryLeak(() -> {
            createSwappableJoinTables();
            final String sql = "SELECT a.ts, a.k, b.v, row_number() OVER (PARTITION BY a.k) rn FROM a JOIN b ON a.k = b.k";
            final String plan = """
                    Window
                      functions: [row_number() over (partition by [k])]
                        SelectedRecord
                            Hash Join Light
                              condition: b.k=a.k
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                                Hash
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: b
                    """;
            assertPlanned(sql, JoinPlan.class,
                    join -> Assert.assertEquals(JoinInput.MasterSide.SMALLER, join.getOrderedInputs().getQuick(1).getMasterSide()));
            assertQuery(sql)
                    .noRandomAccess()
                    .withPlan(plan)
                    .returns("""
                            ts\tk\tv\trn
                            1970-01-01T00:00:03.000000Z\t2\t1\t1
                            1970-01-01T00:00:01.000000Z\t2\t1\t2
                            1970-01-01T00:00:03.000000Z\t2\t2\t3
                            1970-01-01T00:00:01.000000Z\t2\t2\t4
                            1970-01-01T00:00:03.000000Z\t2\t3\t5
                            1970-01-01T00:00:01.000000Z\t2\t3\t6
                            1970-01-01T00:00:03.000000Z\t2\t4\t7
                            1970-01-01T00:00:01.000000Z\t2\t4\t8
                            1970-01-01T00:00:03.000000Z\t2\t5\t9
                            1970-01-01T00:00:01.000000Z\t2\t5\t10
                            1970-01-01T00:00:04.000000Z\t1\t6\t1
                            1970-01-01T00:00:02.000000Z\t1\t6\t2
                            1970-01-01T00:00:04.000000Z\t1\t7\t3
                            1970-01-01T00:00:02.000000Z\t1\t7\t4
                            1970-01-01T00:00:04.000000Z\t1\t8\t5
                            1970-01-01T00:00:02.000000Z\t1\t8\t6
                            1970-01-01T00:00:04.000000Z\t1\t9\t7
                            1970-01-01T00:00:02.000000Z\t1\t9\t8
                            1970-01-01T00:00:04.000000Z\t1\t10\t9
                            1970-01-01T00:00:02.000000Z\t1\t10\t10
                            """);
            final String ordered = sql + " ORDER BY ts";
            assertPlanned(ordered, JoinPlan.class,
                    join -> Assert.assertEquals(JoinInput.MasterSide.FIXED, join.getOrderedInputs().getQuick(1).getMasterSide()));
            assertQuery(ordered)
                    .timestamp("ts")
                    .noRandomAccess()
                    .withPlan(plan)
                    .returns("""
                            ts\tk\tv\trn
                            1970-01-01T00:00:01.000000Z\t2\t5\t1
                            1970-01-01T00:00:01.000000Z\t2\t4\t2
                            1970-01-01T00:00:01.000000Z\t2\t3\t3
                            1970-01-01T00:00:01.000000Z\t2\t2\t4
                            1970-01-01T00:00:01.000000Z\t2\t1\t5
                            1970-01-01T00:00:02.000000Z\t1\t10\t1
                            1970-01-01T00:00:02.000000Z\t1\t9\t2
                            1970-01-01T00:00:02.000000Z\t1\t8\t3
                            1970-01-01T00:00:02.000000Z\t1\t7\t4
                            1970-01-01T00:00:02.000000Z\t1\t6\t5
                            1970-01-01T00:00:03.000000Z\t2\t5\t6
                            1970-01-01T00:00:03.000000Z\t2\t4\t7
                            1970-01-01T00:00:03.000000Z\t2\t3\t8
                            1970-01-01T00:00:03.000000Z\t2\t2\t9
                            1970-01-01T00:00:03.000000Z\t2\t1\t10
                            1970-01-01T00:00:04.000000Z\t1\t10\t6
                            1970-01-01T00:00:04.000000Z\t1\t9\t7
                            1970-01-01T00:00:04.000000Z\t1\t8\t8
                            1970-01-01T00:00:04.000000Z\t1\t7\t9
                            1970-01-01T00:00:04.000000Z\t1\t6\t10
                            """);
        });
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

    private static void createKeyedTable() throws Exception {
        execute("CREATE TABLE t (ts TIMESTAMP, k INT, x LONG) TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO t SELECT (x * 1_000_000)::TIMESTAMP, (x % 3)::INT, x FROM long_sequence(10)");
    }

    private static void createSampledTable() throws Exception {
        execute("CREATE TABLE a (sym SYMBOL INDEX, i INT, v VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("""
                INSERT INTO a VALUES
                    ('S', 1, 'a', '2024-01-01T00:00:00.000000Z'),
                    ('T', 2, 'b', '2024-01-01T00:30:00.000000Z'),
                    ('S', 3, 'c', '2024-01-01T02:00:00.000000Z')
                """);
    }

    private static void createSwappableJoinTables() throws Exception {
        execute("CREATE TABLE a (ts TIMESTAMP, k INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("""
                INSERT INTO a VALUES
                    ('1970-01-01T00:00:01.000000Z', 2),
                    ('1970-01-01T00:00:02.000000Z', 1),
                    ('1970-01-01T00:00:03.000000Z', 2),
                    ('1970-01-01T00:00:04.000000Z', 1)
                """);
        execute("CREATE TABLE b (k INT, v INT)");
        execute("INSERT INTO b SELECT CASE WHEN x <= 5 THEN 2 ELSE 1 END, x::INT FROM long_sequence(10)");
        drainWalQueue();
    }

    private static void createTables() throws Exception {
        execute("CREATE TABLE t (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY");
        execute("""
                INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 1),
                    ('2024-01-01T01:00:00.000000Z', 2),
                    ('2024-01-01T02:00:00.000000Z', 3),
                    ('2024-01-02T00:00:00.000000Z', 4)
                """);
        execute("CREATE TABLE u (x LONG, y LONG)");
        execute("INSERT INTO u SELECT x % 4 + 1, x FROM long_sequence(12)");
    }

    private static void createTemporalJoinTables() throws Exception {
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
                    ('1970-01-01T00:00:03.500000Z', 2, 5),
                    ('1970-01-01T00:00:03.800000Z', 1, 7)
                """);
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

    private static String temporalPlan(String join) {
        return "SelectedRecord\n    " + join + """
                
                        PageFrame
                            Row forward scan
                            Frame forward scan on: a
                        PageFrame
                            Row forward scan
                            Frame forward scan on: b
                """;
    }

    private void assertTemporalJoin(JoinInput.Algorithm expected, String sql, String plan, String rows) throws Exception {
        assertPlanned(sql, JoinPlan.class, join -> Assert.assertEquals(expected, join.getOrderedInputs().getQuick(1).getAlgorithm()));
        assertQuery(sql)
                .timestamp("ts")
                .noRandomAccess()
                .expectSize()
                .withPlan(plan)
                .returns(rows);
    }

    @FunctionalInterface
    private interface PlanAssertion<T> {
        void check(T plan);
    }
}
