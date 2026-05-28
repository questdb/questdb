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

package io.questdb.test.griffin.engine.window;

import io.questdb.PropertyKey;
import io.questdb.test.AbstractCairoTest;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * SQL-level integration tests for the streaming LEAD path with the session flag enabled. Verifies
 * that the planner dispatches to {@code DeferredEmitWindowRecordCursorFactory} for eligible
 * queries, falls back to cached for unsupported shapes, and that the deferred-emit cursor returns
 * correct rows including default values for unfilled flush entries.
 */
public class StreamingLeadIntegrationTest extends AbstractCairoTest {

    @org.junit.Before
    public void reapplyStreamingLeadFlag() {
        // The standard test tearDown (Cairo.tearDown -> overrides.reset()) wipes property overrides
        // after every test, so the flag set in @BeforeClass survives only the first test. Re-apply
        // here so each test executes with the flag enabled. The CairoConfigurationWrapper reads
        // overrides dynamically per call, so this takes effect for queries compiled in the test body.
        setProperty(PropertyKey.CAIRO_SQL_WINDOW_STREAMING_LEAD_ENABLED, "true");
    }

    @BeforeClass
    public static void setUpStatic() throws Exception {
        // Enable the streaming-lead flag before the engine is built so the captured CairoConfiguration
        // reflects it. The engine is constructed once per test class in AbstractCairoTest.setUpStatic
        // and holds a final reference to the configuration thereafter; per-test @Before overrides do
        // not propagate to the engine.
        setProperty(PropertyKey.CAIRO_SQL_WINDOW_STREAMING_LEAD_ENABLED, "true");
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void testIgnoreNullsFallsBackToCached() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t values (10, 0), (20, 1000)");

            // Streaming LEAD does not support IGNORE NULLS in Phase 3; planner uses cached.
            assertPlanNoLeakCheck(
                    "select x, lead(x, 1) ignore nulls over () as lx from t",
                    """
                            CachedWindow
                              unorderedFunctions: [lead(x, 1, NULL) ignore nulls over ()]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """
            );
        });
    }

    @Test
    public void testLeadOneStreamsAndProducesExpectedRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t values (10, 0), (20, 1000), (30, 2000), (40, 3000), (50, 4000)");

            assertQueryNoLeakCheck(
                    """
                            x\tlx
                            10\t20
                            20\t30
                            30\t40
                            40\t50
                            50\tnull
                            """,
                    "select x, lead(x, 1) over () as lx from t",
                    null, false, true
            );
        });
    }

    @Test
    public void testLeadOneStreamsWithDefaultLong() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t values (10, 0), (20, 1000), (30, 2000)");

            // Default value -1 supplied; flushed entries should carry it instead of NULL.
            assertQueryNoLeakCheck(
                    """
                            x\tlx
                            10\t20
                            20\t30
                            30\t-1
                            """,
                    "select x, lead(x, 1, -1) over () as lx from t",
                    null, false, true
            );
        });
    }

    @Test
    public void testLeadOneStreamsWithFlagOn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t values (10, 0), (20, 1000), (30, 2000)");

            // Plan should route through DeferredEmitWindow.
            assertPlanNoLeakCheck(
                    "select x, lead(x, 1) over () as lx from t",
                    """
                            DeferredEmitWindow
                              functions: [lead(x, 1, NULL) over ()]
                              maxLookahead: 1
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """
            );
        });
    }

    @Test
    public void testLeadThreeStreamsWithFlushedTail() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t values (10, 0), (20, 1000), (30, 2000), (40, 3000), (50, 4000)");

            // Lookahead 3 over 5 rows: rows 0,1 in-stream (paired with rows 3,4); rows 2,3,4 flushed.
            assertQueryNoLeakCheck(
                    """
                            x\tlx
                            10\t40
                            20\t50
                            30\tnull
                            40\tnull
                            50\tnull
                            """,
                    "select x, lead(x, 3) over () as lx from t",
                    null, false, true
            );
        });
    }

    @Test
    public void testMixedLagAndLeadFallsBackToCachedWhenNotNormalised() throws Exception {
        // Phase 6.1 cost-model: mixed LAG + LEAD without OVER ORDER BY (or with an order matching the
        // base scan direction) has no sort tree for streaming to eliminate. Cached is already optimal,
        // so the planner routes to CachedWindow.
        assertMemoryLeak(() -> {
            execute("create table t (x long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t values (1, 0), (2, 1000), (3, 2000)");

            assertPlanNoLeakCheck(
                    "select x, lag(x, 1) over () as l, lead(x, 1) over () as ld from t",
                    """
                            CachedWindow
                              unorderedFunctions: [lag(x, 1, NULL) over (),lead(x, 1, NULL) over ()]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """
            );

            assertQueryNoLeakCheck(
                    """
                            x\tl\tld
                            1\tnull\t2
                            2\t1\t3
                            3\t2\tnull
                            """,
                    "select x, lag(x, 1) over () as l, lead(x, 1) over () as ld from t",
                    null, true, true
            );
        });
    }

    @Test
    public void testMixedLagAndLeadPartitionedFallsBackToCachedWhenNotNormalised() throws Exception {
        // Phase 6.1 cost-model: mixed LAG + LEAD with PARTITION BY but no OVER ORDER BY means cached
        // builds no sort tree. CachedWindow's natural-scan path is optimal here, so route to it.
        // Output rows appear in base scan order with per-partition LAG/LEAD values.
        assertMemoryLeak(() -> {
            execute("create table t (x long, sym symbol, ts timestamp) timestamp(ts) partition by day");
            execute(
                    "insert into t values " +
                            "(10, 'A', 0), (20, 'B', 1000), " +
                            "(30, 'A', 2000), (40, 'B', 3000), " +
                            "(50, 'A', 4000)"
            );

            // A: 10,30,50  LAG=null,10,30  LEAD=30,50,null
            // B: 20,40     LAG=null,20     LEAD=40,null
            // Cached emits in base scan order.
            assertQueryNoLeakCheck(
                    """
                            x\tsym\tl\tld
                            10\tA\tnull\t30
                            20\tB\tnull\t40
                            30\tA\t10\t50
                            40\tB\t20\tnull
                            50\tA\t30\tnull
                            """,
                    "select x, sym, lag(x, 1) over (partition by sym) as l, lead(x, 1) over (partition by sym) as ld from t",
                    null, true, true
            );
        });
    }

    @Test
    public void testMixedLagAndLeadStreamsWhenNormalisationFires() throws Exception {
        // Phase 6 + 6.1: mixed LAG + LEAD with OVER ORDER BY ts DESC opposes the base forward scan, so
        // Phase 4 LAG<->LEAD normalisation fires for both columns. Cost model gates streaming on
        // normalisation firing — it does here, so the planner routes to DeferredEmitWindow. After
        // normalisation the LAG becomes LEAD(ASC) and the LEAD becomes LAG(ASC).
        assertMemoryLeak(() -> {
            execute("create table t (x long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t values (1, 0), (2, 1000), (3, 2000)");

            // After Phase 4 normalisation, column 0 (originally lag DESC) becomes lead ASC and column 1
            // (originally lead DESC) becomes lag ASC. The cursor's toPlan groups by category (LAG funcs
            // first, then LEAD funcs). The ORDER BY is dismissed because ASC matches the forward scan.
            assertPlanNoLeakCheck(
                    "select x, " +
                            "lag(x, 1) over (order by ts desc) as l, " +
                            "lead(x, 1) over (order by ts desc) as ld " +
                            "from t",
                    """
                            DeferredEmitWindow
                              functions: [lag(x, 1, NULL) over (),lead(x, 1, NULL) over ()]
                              maxLookahead: 1
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """
            );

            // OVER ORDER BY ts DESC means: for row R, LAG(x,1) = x at R's predecessor in DESC = R's
            // successor in scan; LEAD(x,1) = x at R's successor in DESC = R's predecessor in scan.
            // Rows emit in stream order (DeferredEmitWindow's emission contract; partition-major when
            // partitioned, scan-order otherwise).
            assertQueryNoLeakCheck(
                    """
                            x\tl\tld
                            1\t2\tnull
                            2\t3\t1
                            3\tnull\t2
                            """,
                    "select x, " +
                            "lag(x, 1) over (order by ts desc) as l, " +
                            "lead(x, 1) over (order by ts desc) as ld " +
                            "from t",
                    null, false, true
            );
        });
    }

    @Test
    public void testNonConstantDefaultFallsBackToCached() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x long, y long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t values (10, 100, 0), (20, 200, 1000)");

            // Non-constant default (y is a column) blocks streaming; planner uses cached.
            assertPlanNoLeakCheck(
                    "select x, lead(x, 1, y) over () as lx from t",
                    """
                            CachedWindow
                              unorderedFunctions: [lead(x, 1, y) over ()]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """
            );
        });
    }

    @Test
    public void testPartitionByStreams() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x long, sym symbol, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t values (10, 'A', 0), (20, 'B', 1000), (30, 'A', 2000), (40, 'B', 3000), (50, 'A', 4000)");

            // Phase 5: PARTITION BY streams via DeferredEmitWindow with per-partition ring buffers.
            // LEAD(x,1) per symbol: A's rows pair (10,30) (30,50) (50,NULL); B's rows pair (20,40) (40,NULL).
            // Output is partition-major-resolution order (per documented contract): in-stream
            // emissions happen as the next row in each partition arrives, end-of-cursor flush emits
            // remaining pending entries in map iteration order. Within each partition rows stay in
            // OVER ORDER BY order.
            assertQueryNoLeakCheck(
                    """
                            x\tsym\tlx
                            10\tA\t30
                            20\tB\t40
                            30\tA\t50
                            50\tA\tnull
                            40\tB\tnull
                            """,
                    "select x, sym, lead(x, 1) over (partition by sym) as lx from t",
                    null, false, true
            );
        });
    }

    @Test
    public void testLeadDateStreams() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (d date, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t values (cast(100 as date), 0), (cast(200 as date), 1000), (cast(300 as date), 2000)");

            assertPlanNoLeakCheck(
                    "select d, lead(d, 1) over () as ld from t",
                    """
                            DeferredEmitWindow
                              functions: [lead(d, 1, NULL) over ()]
                              maxLookahead: 1
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """
            );

            assertQueryNoLeakCheck(
                    """
                            d\tld
                            1970-01-01T00:00:00.100Z\t1970-01-01T00:00:00.200Z
                            1970-01-01T00:00:00.200Z\t1970-01-01T00:00:00.300Z
                            1970-01-01T00:00:00.300Z\t
                            """,
                    "select d, lead(d, 1) over () as ld from t",
                    null, false, true
            );
        });
    }

    @Test
    public void testLeadDoubleStreams() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (d double, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t values (1.5, 0), (2.5, 1000), (3.5, 2000)");

            assertPlanNoLeakCheck(
                    "select d, lead(d, 1) over () as ld from t",
                    """
                            DeferredEmitWindow
                              functions: [lead(d, 1, NULL) over ()]
                              maxLookahead: 1
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """
            );

            assertQueryNoLeakCheck(
                    """
                            d\tld
                            1.5\t2.5
                            2.5\t3.5
                            3.5\tnull
                            """,
                    "select d, lead(d, 1) over () as ld from t",
                    null, false, true
            );
        });
    }

    @Test
    public void testLeadTimestampStreams() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp) timestamp(ts) partition by day");
            execute("insert into t values (1000), (2000), (3000)");

            assertPlanNoLeakCheck(
                    "select ts, lead(ts, 1) over () as lts from t",
                    """
                            DeferredEmitWindow
                              functions: [lead(ts, 1, NULL) over ()]
                              maxLookahead: 1
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """
            );

            assertQueryNoLeakCheck(
                    """
                            ts\tlts
                            1970-01-01T00:00:00.001000Z\t1970-01-01T00:00:00.002000Z
                            1970-01-01T00:00:00.002000Z\t1970-01-01T00:00:00.003000Z
                            1970-01-01T00:00:00.003000Z\t
                            """,
                    "select ts, lead(ts, 1) over () as lts from t",
                    "ts", false, true
            );
        });
    }

    @Test
    public void testCardinalityCapTrips() throws Exception {
        // Force the partition cap low so a moderate symbol cardinality trips it.
        setProperty(PropertyKey.CAIRO_SQL_WINDOW_STREAMING_MAX_PARTITIONS, "4");
        assertMemoryLeak(() -> {
            execute("create table t (x long, sym symbol, ts timestamp) timestamp(ts) partition by day");
            // Insert 6 distinct symbols; with cap=4 the 5th unique symbol should trigger the cap.
            // Two rows per symbol so the cursor actually pushes through processBaseRow.
            execute(
                    "insert into t values " +
                            "(1, 'A', 0),  (2, 'A', 1000), " +
                            "(1, 'B', 2000), (2, 'B', 3000), " +
                            "(1, 'C', 4000), (2, 'C', 5000), " +
                            "(1, 'D', 6000), (2, 'D', 7000), " +
                            "(1, 'E', 8000), (2, 'E', 9000), " +
                            "(1, 'F', 10000), (2, 'F', 11000)"
            );

            try {
                assertQueryNoLeakCheck("dummy", "select x, sym, lead(x, 1) over (partition by sym) as lx from t", null, false, true);
                org.junit.Assert.fail("expected CairoException for cap exceeded");
            } catch (io.questdb.cairo.CairoException e) {
                org.junit.Assert.assertTrue(
                        "unexpected error: " + e.getFlyweightMessage(),
                        e.getFlyweightMessage().toString().contains("partition cap exceeded")
                );
            }
        });
        // Restore to default for subsequent tests in this class (matters because @Before re-applies
        // the streaming flag but not this cap; explicit reset keeps the run order independent).
        setProperty(PropertyKey.CAIRO_SQL_WINDOW_STREAMING_MAX_PARTITIONS, "1048576");
    }

    @Test
    public void testOriginalTriggeringQueryShape() throws Exception {
        // This is the query that originally motivated the design: LAG(ts) OVER (PARTITION BY symbol
        // ORDER BY ts DESC) — previously OOM'd via CachedWindow on a 2B-row table. With Phase 4+5 it
        // normalises to LEAD(ts) ASC + PARTITION BY symbol and streams via DeferredEmitWindow.
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol, ts timestamp) timestamp(ts) partition by day");
            // Three timestamps per symbol; the planner streams; per-partition LEAD computes the
            // next-higher ts per symbol.
            execute(
                    "insert into t values " +
                            "('A', 1000), ('B', 2000), ('A', 3000), " +
                            "('B', 4000), ('A', 5000), ('B', 6000)"
            );

            assertQueryNoLeakCheck(
                    """
                            sym\tts\tnext_ts
                            A\t1970-01-01T00:00:00.001000Z\t1970-01-01T00:00:00.003000Z
                            B\t1970-01-01T00:00:00.002000Z\t1970-01-01T00:00:00.004000Z
                            A\t1970-01-01T00:00:00.003000Z\t1970-01-01T00:00:00.005000Z
                            B\t1970-01-01T00:00:00.004000Z\t1970-01-01T00:00:00.006000Z
                            A\t1970-01-01T00:00:00.005000Z\t
                            B\t1970-01-01T00:00:00.006000Z\t
                            """,
                    "select sym, ts, lag(ts, 1) over (partition by sym order by ts desc) as next_ts from t",
                    "ts", false, true
            );
        });
    }

    @Test
    public void testPartitionByLargerLookahead() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x long, sym symbol, ts timestamp) timestamp(ts) partition by day");
            // Two symbols, 4 rows each interleaved.
            execute(
                    "insert into t values " +
                            "(1, 'A', 0), (1, 'B', 1000), " +
                            "(2, 'A', 2000), (2, 'B', 3000), " +
                            "(3, 'A', 4000), (3, 'B', 5000), " +
                            "(4, 'A', 6000), (4, 'B', 7000)"
            );

            // LEAD(x, 2) per partition: A: (1,3) (2,4) (3,NULL) (4,NULL); B: (1,3) (2,4) (3,NULL) (4,NULL).
            // In-stream emissions when R+2 arrives for the same partition; remaining 2 per partition
            // flushed at EOF. Within each partition rows stay in OVER ORDER BY order.
            assertQueryNoLeakCheck(
                    """
                            x\tsym\tlx
                            1\tA\t3
                            1\tB\t3
                            2\tA\t4
                            2\tB\t4
                            3\tA\tnull
                            4\tA\tnull
                            3\tB\tnull
                            4\tB\tnull
                            """,
                    "select x, sym, lead(x, 2) over (partition by sym) as lx from t",
                    null, false, true
            );
        });
    }

    @Test
    public void testPartitionByReexecutesIdentically() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x long, sym symbol, ts timestamp) timestamp(ts) partition by day");
            execute(
                    "insert into t values " +
                            "(10, 'A', 0), (20, 'B', 1000), " +
                            "(30, 'A', 2000), (40, 'B', 3000)"
            );

            // Running the same query twice in the same test should produce the same partition-major
            // output — verifies toTop / partition-map clear is correct under PARTITION BY.
            String expected =
                    """
                            x\tsym\tlx
                            10\tA\t30
                            20\tB\t40
                            30\tA\tnull
                            40\tB\tnull
                            """;
            String sql = "select x, sym, lead(x, 1) over (partition by sym) as lx from t";
            assertQueryNoLeakCheck(expected, sql, null, false, true);
            assertQueryNoLeakCheck(expected, sql, null, false, true);
            assertQueryNoLeakCheck(expected, sql, null, false, true);
        });
    }

    @Test
    public void testSingleRowFlushedAsNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t values (42, 0)");

            assertQueryNoLeakCheck(
                    """
                            x\tlx
                            42\tnull
                            """,
                    "select x, lead(x, 1) over () as lx from t",
                    null, false, true
            );
        });
    }
}
