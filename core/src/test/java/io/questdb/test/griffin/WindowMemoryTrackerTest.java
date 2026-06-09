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
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.engine.window.CachedWindowRecordCursorFactory;
import io.questdb.griffin.engine.window.WindowRecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * SQL-level tests that exercise the per-query memory limit through the
 * tracker-aware per-partition {@code Map} owned by partitioned window functions
 * ({@link io.questdb.griffin.engine.functions.window.BasePartitionedWindowFunction}
 * and {@link io.questdb.griffin.engine.functions.window.BasePartitionedBivariateWindowFunction}).
 * <p>
 * Both window cursors ({@link WindowRecordCursorFactory} streaming and
 * {@link CachedWindowRecordCursorFactory} cached) bind the active workload's
 * MemoryTracker on each window function before reopening its map, and the base
 * classes free the eager map backing in their constructor so the first reopen()
 * allocates it under the bound tracker. A runaway {@code PARTITION BY} over a
 * high-cardinality key therefore crosses the per-query limit at the map's growth.
 * <p>
 * The per-partition map and the RANGE-frame value ring buffer ({@code MemoryARW})
 * are wired. The {@code PARTITION BY}-only tests exercise the map (their only
 * unbounded native structure); the {@code RANGE BETWEEN ... PRECEDING} tests exercise
 * the ring buffer. The non-partition range test is the cleanest buffer proof: with no
 * {@code PARTITION BY} there is no map on the path, so a breach is charged solely to
 * the buffer. ROWS-frame buffers are sized by the frame literal (bounded) and stay on
 * global-only accounting.
 * <p>
 * The per-query limit is applied per test in {@link #setUp()} via
 * {@code setProperty} so it survives the per-test override reset; the provider
 * reads it live on each tracker acquisition.
 */
public class WindowMemoryTrackerTest extends AbstractCairoTest {

    @Before
    public void setUp() {
        super.setUp();
        // 256 KiB: a high-cardinality PARTITION BY map (tens of thousands of keys)
        // crosses it, while a handful of partitions fit comfortably.
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 256 * 1024L);
        // Shrink the window-store page so a range-frame ring buffer starts small (one
        // 4 KiB page) and a runaway frame breaches the 256 KiB limit after a few
        // doublings, while a small input stays comfortably under it. The default is
        // 1 MiB, which would breach on the first allocation.
        setProperty(PropertyKey.CAIRO_SQL_WINDOW_STORE_PAGE_SIZE, 4 * 1024L);
    }

    @Test
    public void testCachedPartitionWindowReleasesAllocations() throws Exception {
        // rank() over (partition by k order by v) routes through the cached window
        // cursor, which binds the tracker on the partition map (and the ordered tree
        // chains) before reopen(). A small input fits the limit; repeated
        // getCursor/close cycles must release every byte the lazy map allocates.
        // assertMemoryLeak around the loop is the load-bearing check - the
        // constructor's eager-backing free plus the per-cursor reopen/reset must net
        // to zero on the per-query counter.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (SELECT (x % 50) AS k, x::double AS v FROM long_sequence(2_000))");
            drainWalQueue();
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = compiler.compile("SELECT k, rank() OVER (PARTITION BY k ORDER BY v) FROM tab", sqlExecutionContext).getRecordCursorFactory()) {
                assertInTree(factory, CachedWindowRecordCursorFactory.class);
                for (int i = 0; i < 10; i++) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        long rows = 0;
                        while (cursor.hasNext()) {
                            rows++;
                        }
                        Assert.assertEquals("iteration " + i, 2_000, rows);
                    }
                }
            }
        });
    }

    @Test
    public void testCumeDistPartitionReleasesAllocations() throws Exception {
        // cume_dist() over (partition by k order by v) routes through the cached window
        // cursor. Its per-partition map (now lazy/openOnInit=false) and its deferred-offsets
        // ring must free symmetrically across getCursor/close cycles. The (x%20, x%10) shape
        // puts every row of a partition into one peer group, so the ring accumulates the
        // whole partition before flushing, exercising both structures. With the per-query
        // limit active, an asymmetric alloc/free would leak or drive the counter negative;
        // assertMemoryLeak around the loop is the load-bearing check.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (SELECT (x % 20) AS k, (x % 10)::double AS v FROM long_sequence(2_000))");
            drainWalQueue();
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = compiler.compile("SELECT k, cume_dist() OVER (PARTITION BY k ORDER BY v) FROM tab", sqlExecutionContext).getRecordCursorFactory()) {
                assertInTree(factory, CachedWindowRecordCursorFactory.class);
                for (int i = 0; i < 10; i++) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        long rows = 0;
                        while (cursor.hasNext()) {
                            rows++;
                        }
                        Assert.assertEquals("iteration " + i, 2_000, rows);
                    }
                }
            }
        });
    }

    @Test
    public void testPartitionRangeFrameBufferFailsOnHighDensity() throws Exception {
        // avg(v) over (partition by k order by ts range between ... preceding and current
        // row) with only a handful of partitions keeps the per-partition map tiny, so the
        // breach is charged to the shared range ring buffer (MemoryARW) sliced per
        // partition. Exercises the partition variant's setMemoryTracker: super binds the
        // map, the override binds the buffer.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (" +
                    "SELECT (x % 4) AS k, x::double AS v, timestamp_sequence(0, 1) AS ts " +
                    "FROM long_sequence(50_000)) TIMESTAMP(ts) PARTITION BY DAY");
            drainWalQueue();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final CompiledQuery cq = compiler.compile(
                        "SELECT k, avg(v) OVER (PARTITION BY k ORDER BY ts RANGE BETWEEN 100_000_000 PRECEDING AND CURRENT ROW) FROM tab",
                        sqlExecutionContext);
                try (RecordCursorFactory factory = cq.getRecordCursorFactory()) {
                    assertInTree(factory, WindowRecordCursorFactory.class);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        //noinspection StatementWithEmptyBody
                        while (cursor.hasNext()) {
                            // drain until breach
                        }
                        Assert.fail("expected per-query memory breach");
                    } catch (CairoException e) {
                        Assert.assertTrue("expected isOutOfMemory(), got: " + e.getFlyweightMessage(), e.isOutOfMemory());
                        TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                        TestUtils.assertContains(e.getFlyweightMessage(), "workload=QUERY");
                    }
                }
            }
        });
    }

    @Test
    public void testRangeFrameBufferReleasesAllocations() throws Exception {
        // Repeated getCursor/close cycles on a range-frame buffer must release every byte
        // the lazy ring allocates. assertMemoryLeak around the loop is the load-bearing
        // check: the per-cursor reopen (first alloc under the bound tracker) and reset
        // (free under the same tracker) must net to zero on the per-query counter.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (" +
                    "SELECT x::double AS v, timestamp_sequence(0, 1) AS ts " +
                    "FROM long_sequence(2_000)) TIMESTAMP(ts) PARTITION BY DAY");
            drainWalQueue();
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = compiler.compile(
                         "SELECT ts, avg(v) OVER (ORDER BY ts RANGE BETWEEN 100_000_000 PRECEDING AND CURRENT ROW) FROM tab",
                         sqlExecutionContext).getRecordCursorFactory()) {
                assertInTree(factory, WindowRecordCursorFactory.class);
                for (int i = 0; i < 10; i++) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        long rows = 0;
                        while (cursor.hasNext()) {
                            rows++;
                        }
                        Assert.assertEquals("iteration " + i, 2_000, rows);
                    }
                }
            }
        });
    }

    @Test
    public void testStreamingFirstValueRangeBufferFailsOnHighDensity() throws Exception {
        // first_value(v) over (order by ts range ...) buffers the frame in its value ring
        // (no partition by -> no map on the path), representing the First/Last/Nth value-ring
        // family. A wide finite frame over dense timestamps grows the ring past the limit.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (" +
                    "SELECT x::double AS v, timestamp_sequence(0, 1) AS ts " +
                    "FROM long_sequence(50_000)) TIMESTAMP(ts) PARTITION BY DAY");
            drainWalQueue();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final CompiledQuery cq = compiler.compile(
                        "SELECT ts, first_value(v) OVER (ORDER BY ts RANGE BETWEEN 100_000_000 PRECEDING AND CURRENT ROW) FROM tab",
                        sqlExecutionContext);
                try (RecordCursorFactory factory = cq.getRecordCursorFactory()) {
                    assertInTree(factory, WindowRecordCursorFactory.class);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        //noinspection StatementWithEmptyBody
                        while (cursor.hasNext()) {
                            // drain until breach
                        }
                        Assert.fail("expected per-query memory breach");
                    } catch (CairoException e) {
                        Assert.assertTrue("expected isOutOfMemory(), got: " + e.getFlyweightMessage(), e.isOutOfMemory());
                        TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                        TestUtils.assertContains(e.getFlyweightMessage(), "workload=QUERY");
                    }
                }
            }
        });
    }

    @Test
    public void testStreamingMaxRangeBufferFailsOnHighDensity() throws Exception {
        // max(v) over (order by ts range ...) keeps TWO growable buffers: the [ts,value] value
        // ring and a monotonic deque for the frame maximum. A wide finite frame over dense
        // timestamps grows them past the limit; both are now tracker-bound, so the breach is
        // a clean isOutOfMemory(). max() also backs min() (shared MaxMin classes).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (" +
                    "SELECT x::double AS v, timestamp_sequence(0, 1) AS ts " +
                    "FROM long_sequence(50_000)) TIMESTAMP(ts) PARTITION BY DAY");
            drainWalQueue();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final CompiledQuery cq = compiler.compile(
                        "SELECT ts, max(v) OVER (ORDER BY ts RANGE BETWEEN 100_000_000 PRECEDING AND CURRENT ROW) FROM tab",
                        sqlExecutionContext);
                try (RecordCursorFactory factory = cq.getRecordCursorFactory()) {
                    assertInTree(factory, WindowRecordCursorFactory.class);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        //noinspection StatementWithEmptyBody
                        while (cursor.hasNext()) {
                            // drain until breach
                        }
                        Assert.fail("expected per-query memory breach");
                    } catch (CairoException e) {
                        Assert.assertTrue("expected isOutOfMemory(), got: " + e.getFlyweightMessage(), e.isOutOfMemory());
                        TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                        TestUtils.assertContains(e.getFlyweightMessage(), "workload=QUERY");
                    }
                }
            }
        });
    }

    @Test
    public void testStreamingMaxRangeBufferReleasesAllocations() throws Exception {
        // Repeated getCursor/close cycles on max() over range must release BOTH the value ring
        // and the deque buffer. assertMemoryLeak around the loop is the load-bearing check that
        // the deque's lazy alloc/free stays symmetric on the per-query counter.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (" +
                    "SELECT x::double AS v, timestamp_sequence(0, 1) AS ts " +
                    "FROM long_sequence(2_000)) TIMESTAMP(ts) PARTITION BY DAY");
            drainWalQueue();
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = compiler.compile(
                         "SELECT ts, max(v) OVER (ORDER BY ts RANGE BETWEEN 100_000_000 PRECEDING AND CURRENT ROW) FROM tab",
                         sqlExecutionContext).getRecordCursorFactory()) {
                assertInTree(factory, WindowRecordCursorFactory.class);
                for (int i = 0; i < 10; i++) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        long rows = 0;
                        while (cursor.hasNext()) {
                            rows++;
                        }
                        Assert.assertEquals("iteration " + i, 2_000, rows);
                    }
                }
            }
        });
    }

    @Test
    public void testStreamingPartitionMapFailsOnHighCardinality() throws Exception {
        // first_value(v) over (partition by k) routes through the streaming window
        // cursor and grows one map entry per distinct partition key. The map is the
        // only unbounded native structure on this path, so its growth past the
        // per-query limit produces a clean isOutOfMemory() breach.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (SELECT x AS k, x::double AS v FROM long_sequence(200_000))");
            drainWalQueue();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final CompiledQuery cq = compiler.compile("SELECT k, first_value(v) OVER (PARTITION BY k) FROM tab", sqlExecutionContext);
                try (RecordCursorFactory factory = cq.getRecordCursorFactory()) {
                    assertInTree(factory, WindowRecordCursorFactory.class);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        //noinspection StatementWithEmptyBody
                        while (cursor.hasNext()) {
                            // drain until breach
                        }
                        Assert.fail("expected per-query memory breach");
                    } catch (CairoException e) {
                        Assert.assertTrue("expected isOutOfMemory(), got: " + e.getFlyweightMessage(), e.isOutOfMemory());
                        TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                        TestUtils.assertContains(e.getFlyweightMessage(), "workload=QUERY");
                    }
                }
            }
        });
    }

    @Test
    public void testStreamingPartitionMapOpenBreachDoesNotLeakReader() throws Exception {
        // of()-time breach (not the hasNext() drain the other streaming tests use): a
        // tiny limit makes the cursor's of() breach when it reopens the partition map,
        // after super.of() took the base cursor. The getCursor() catch must close the
        // cursor and free that base cursor, otherwise the leak shows as a busy reader.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (SELECT (x % 5) AS k, x::double AS v FROM long_sequence(50))");
            drainWalQueue();
            final String query = "SELECT k, first_value(v) OVER (PARTITION BY k) FROM tab";
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                assertInTree(factory, WindowRecordCursorFactory.class);
                // Shrink the limit now (read live per open) so of()'s map reopen() breaches.
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 64L);
                for (int i = 0; i < 5; i++) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.fail("expected per-query memory breach during cursor open, got cursor: " + cursor);
                    } catch (CairoException e) {
                        Assert.assertTrue("expected isOutOfMemory(), got: " + e.getFlyweightMessage(), e.isOutOfMemory());
                        TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                        TestUtils.assertContains(e.getFlyweightMessage(), "workload=QUERY");
                    }
                }
            }
            // Every failed open must have returned its base cursor's reader.
            Assert.assertEquals("busy reader count", 0, engine.getBusyReaderCount());
        });
    }

    @Test
    public void testStreamingPartitionMapSucceedsOnLowCardinality() throws Exception {
        // A handful of partitions keep the map well under the limit; the query
        // returns one row per input row and the tracker accounting stays balanced.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (SELECT (x % 5) AS k, x::double AS v FROM long_sequence(10_000))");
            drainWalQueue();
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = compiler.compile("SELECT k, first_value(v) OVER (PARTITION BY k) FROM tab", sqlExecutionContext).getRecordCursorFactory()) {
                assertInTree(factory, WindowRecordCursorFactory.class);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    long rows = 0;
                    while (cursor.hasNext()) {
                        rows++;
                    }
                    Assert.assertEquals(10_000, rows);
                }
            }
        });
    }

    @Test
    public void testStreamingRangeFrameBufferFailsOnHighDensity() throws Exception {
        // avg(v) over (order by ts range between ... preceding and current row) has no
        // partition by, so there is no map on the path - the only unbounded native
        // structure is the value ring buffer (MemoryARW) inside AvgOverRangeFrameFunction.
        // A wide finite frame over densely packed timestamps accumulates every row into
        // the ring, so its growth past the per-query limit is charged solely to the
        // buffer. This is the cleanest proof of the buffer wiring.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (" +
                    "SELECT x::double AS v, timestamp_sequence(0, 1) AS ts " +
                    "FROM long_sequence(50_000)) TIMESTAMP(ts) PARTITION BY DAY");
            drainWalQueue();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final CompiledQuery cq = compiler.compile(
                        "SELECT ts, avg(v) OVER (ORDER BY ts RANGE BETWEEN 100_000_000 PRECEDING AND CURRENT ROW) FROM tab",
                        sqlExecutionContext);
                try (RecordCursorFactory factory = cq.getRecordCursorFactory()) {
                    assertInTree(factory, WindowRecordCursorFactory.class);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        //noinspection StatementWithEmptyBody
                        while (cursor.hasNext()) {
                            // drain until breach
                        }
                        Assert.fail("expected per-query memory breach");
                    } catch (CairoException e) {
                        Assert.assertTrue("expected isOutOfMemory(), got: " + e.getFlyweightMessage(), e.isOutOfMemory());
                        TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                        TestUtils.assertContains(e.getFlyweightMessage(), "workload=QUERY");
                    }
                }
            }
        });
    }

    @Test
    public void testStreamingRangeFrameBufferSucceedsUnderLimit() throws Exception {
        // A small input keeps the ring buffer well under the limit; the query returns one
        // row per input row and the tracker accounting stays balanced.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (" +
                    "SELECT x::double AS v, timestamp_sequence(0, 1) AS ts " +
                    "FROM long_sequence(2_000)) TIMESTAMP(ts) PARTITION BY DAY");
            drainWalQueue();
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = compiler.compile(
                         "SELECT ts, avg(v) OVER (ORDER BY ts RANGE BETWEEN 100_000_000 PRECEDING AND CURRENT ROW) FROM tab",
                         sqlExecutionContext).getRecordCursorFactory()) {
                assertInTree(factory, WindowRecordCursorFactory.class);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    long rows = 0;
                    while (cursor.hasNext()) {
                        rows++;
                    }
                    Assert.assertEquals(2_000, rows);
                }
            }
        });
    }

    private static void assertInTree(RecordCursorFactory factory, Class<?> expected) {
        RecordCursorFactory f = factory;
        while (f != null) {
            if (expected.isInstance(f)) {
                return;
            }
            f = f.getBaseFactory();
        }
        Assert.fail("expected " + expected.getSimpleName() + " in the factory tree, but top was " + factory.getClass().getName());
    }
}
