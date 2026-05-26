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
                    "CachedWindow\n" +
                            "  unorderedFunctions: [lead(x, 1, NULL) ignore nulls over ()]\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: t\n"
            );
        });
    }

    @Test
    public void testLeadOneStreamsAndProducesExpectedRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t values (10, 0), (20, 1000), (30, 2000), (40, 3000), (50, 4000)");

            assertSql(
                    "x\tlx\n" +
                            "10\t20\n" +
                            "20\t30\n" +
                            "30\t40\n" +
                            "40\t50\n" +
                            "50\tnull\n",
                    "select x, lead(x, 1) over () as lx from t"
            );
        });
    }

    @Test
    public void testLeadOneStreamsWithDefaultLong() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t values (10, 0), (20, 1000), (30, 2000)");

            // Default value -1 supplied; flushed entries should carry it instead of NULL.
            assertSql(
                    "x\tlx\n" +
                            "10\t20\n" +
                            "20\t30\n" +
                            "30\t-1\n",
                    "select x, lead(x, 1, -1) over () as lx from t"
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
                    "DeferredEmitWindow\n" +
                            "  functions: [lead(x, 1, NULL) over ()]\n" +
                            "  maxLookahead: 1\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: t\n"
            );
        });
    }

    @Test
    public void testLeadThreeStreamsWithFlushedTail() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t values (10, 0), (20, 1000), (30, 2000), (40, 3000), (50, 4000)");

            // Lookahead 3 over 5 rows: rows 0,1 in-stream (paired with rows 3,4); rows 2,3,4 flushed.
            assertSql(
                    "x\tlx\n" +
                            "10\t40\n" +
                            "20\t50\n" +
                            "30\tnull\n" +
                            "40\tnull\n" +
                            "50\tnull\n",
                    "select x, lead(x, 3) over () as lx from t"
            );
        });
    }

    @Test
    public void testMixedLagAndLeadPartitionedStreams() throws Exception {
        // Phase 6 + Phase 5: mixed LAG + LEAD with PARTITION BY streams via DeferredEmitWindow.
        // For each symbol, lag and lead computed per partition.
        assertMemoryLeak(() -> {
            execute("create table t (x long, sym symbol, ts timestamp) timestamp(ts) partition by day");
            execute(
                    "insert into t values " +
                            "(10, 'A', 0), (20, 'B', 1000), " +
                            "(30, 'A', 2000), (40, 'B', 3000), " +
                            "(50, 'A', 4000)"
            );

            // A's sequence: 10, 30, 50. LAG: NULL, 10, 30. LEAD: 30, 50, NULL.
            // B's sequence: 20, 40. LAG: NULL, 20. LEAD: 40, NULL.
            // Emission is partition-major-resolution; within each partition rows stay in OVER ORDER BY.
            assertSql(
                    "x\tsym\tl\tld\n" +
                            "10\tA\tnull\t30\n" +
                            "20\tB\tnull\t40\n" +
                            "30\tA\t10\t50\n" +
                            "50\tA\t30\tnull\n" +
                            "40\tB\t20\tnull\n",
                    "select x, sym, lag(x, 1) over (partition by sym) as l, lead(x, 1) over (partition by sym) as ld from t"
            );
        });
    }

    @Test
    public void testMixedLagAndLeadStreams() throws Exception {
        // Phase 6: a query mixing LAG (lookahead=0) and LEAD (lookahead>0) now streams via
        // DeferredEmitWindow. LAG values are written into the pending slot at processBaseRow time
        // via the function's own pass1; LEAD values are back-filled via streamingBackfill on the
        // matching future row.
        assertMemoryLeak(() -> {
            execute("create table t (x long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t values (1, 0), (2, 1000), (3, 2000)");

            assertPlanNoLeakCheck(
                    "select x, lag(x, 1) over () as l, lead(x, 1) over () as ld from t",
                    "DeferredEmitWindow\n" +
                            "  functions: [lag(x, 1, NULL) over (),lead(x, 1, NULL) over ()]\n" +
                            "  maxLookahead: 1\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: t\n"
            );

            assertSql(
                    "x\tl\tld\n" +
                            "1\tnull\t2\n" +
                            "2\t1\t3\n" +
                            "3\t2\tnull\n",
                    "select x, lag(x, 1) over () as l, lead(x, 1) over () as ld from t"
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
                    "CachedWindow\n" +
                            "  unorderedFunctions: [lead(x, 1, y) over ()]\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: t\n"
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
            assertSql(
                    "x\tsym\tlx\n" +
                            "10\tA\t30\n" +
                            "20\tB\t40\n" +
                            "30\tA\t50\n" +
                            "50\tA\tnull\n" +
                            "40\tB\tnull\n",
                    "select x, sym, lead(x, 1) over (partition by sym) as lx from t"
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
                    "DeferredEmitWindow\n" +
                            "  functions: [lead(d, 1, NULL) over ()]\n" +
                            "  maxLookahead: 1\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: t\n"
            );

            assertSql(
                    "d\tld\n" +
                            "1970-01-01T00:00:00.100Z\t1970-01-01T00:00:00.200Z\n" +
                            "1970-01-01T00:00:00.200Z\t1970-01-01T00:00:00.300Z\n" +
                            "1970-01-01T00:00:00.300Z\t\n",
                    "select d, lead(d, 1) over () as ld from t"
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
                    "DeferredEmitWindow\n" +
                            "  functions: [lead(d, 1, NULL) over ()]\n" +
                            "  maxLookahead: 1\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: t\n"
            );

            assertSql(
                    "d\tld\n" +
                            "1.5\t2.5\n" +
                            "2.5\t3.5\n" +
                            "3.5\tnull\n",
                    "select d, lead(d, 1) over () as ld from t"
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
                    "DeferredEmitWindow\n" +
                            "  functions: [lead(ts, 1, NULL) over ()]\n" +
                            "  maxLookahead: 1\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: t\n"
            );

            assertSql(
                    "ts\tlts\n" +
                            "1970-01-01T00:00:00.001000Z\t1970-01-01T00:00:00.002000Z\n" +
                            "1970-01-01T00:00:00.002000Z\t1970-01-01T00:00:00.003000Z\n" +
                            "1970-01-01T00:00:00.003000Z\t\n",
                    "select ts, lead(ts, 1) over () as lts from t"
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
                assertSql("dummy", "select x, sym, lead(x, 1) over (partition by sym) as lx from t");
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

            assertSql(
                    "sym\tts\tnext_ts\n" +
                            "A\t1970-01-01T00:00:00.001000Z\t1970-01-01T00:00:00.003000Z\n" +
                            "B\t1970-01-01T00:00:00.002000Z\t1970-01-01T00:00:00.004000Z\n" +
                            "A\t1970-01-01T00:00:00.003000Z\t1970-01-01T00:00:00.005000Z\n" +
                            "B\t1970-01-01T00:00:00.004000Z\t1970-01-01T00:00:00.006000Z\n" +
                            "A\t1970-01-01T00:00:00.005000Z\t\n" +
                            "B\t1970-01-01T00:00:00.006000Z\t\n",
                    "select sym, ts, lag(ts, 1) over (partition by sym order by ts desc) as next_ts from t"
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
            assertSql(
                    "x\tsym\tlx\n" +
                            "1\tA\t3\n" +
                            "1\tB\t3\n" +
                            "2\tA\t4\n" +
                            "2\tB\t4\n" +
                            "3\tA\tnull\n" +
                            "4\tA\tnull\n" +
                            "3\tB\tnull\n" +
                            "4\tB\tnull\n",
                    "select x, sym, lead(x, 2) over (partition by sym) as lx from t"
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
                    "x\tsym\tlx\n" +
                            "10\tA\t30\n" +
                            "20\tB\t40\n" +
                            "30\tA\tnull\n" +
                            "40\tB\tnull\n";
            String sql = "select x, sym, lead(x, 1) over (partition by sym) as lx from t";
            assertSql(expected, sql);
            assertSql(expected, sql);
            assertSql(expected, sql);
        });
    }

    @Test
    public void testSingleRowFlushedAsNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t values (42, 0)");

            assertSql(
                    "x\tlx\n" +
                            "42\tnull\n",
                    "select x, lead(x, 1) over () as lx from t"
            );
        });
    }
}
