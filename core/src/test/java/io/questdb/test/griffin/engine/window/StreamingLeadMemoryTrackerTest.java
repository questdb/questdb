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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.engine.window.DeferredEmitWindowRecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class StreamingLeadMemoryTrackerTest extends AbstractCairoTest {

    @Before
    public void setUpProperties() {
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 256 * 1024L);
        setProperty(PropertyKey.CAIRO_SQL_WINDOW_STREAMING_LEAD_ENABLED, "true");
    }

    @BeforeClass
    public static void setUpStatic() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_WINDOW_STREAMING_LEAD_ENABLED, "true");
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void testPartitionMapFailsOnOpen() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (" +
                    "SELECT x AS k, x AS v, timestamp_sequence(0, 1) AS ts " +
                    "FROM long_sequence(100)) TIMESTAMP(ts) PARTITION BY DAY");
            drainWalQueue();
            // Inflate only the deferred cursor's map so its first allocation exceeds the limit.
            setProperty(PropertyKey.CAIRO_SQL_SMALL_MAP_KEY_CAPACITY, 64 * 1024);
            final String query = "SELECT k, lag(v, 1) OVER (PARTITION BY k ORDER BY ts DESC) FROM tab";
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                assertInTree(factory, DeferredEmitWindowRecordCursorFactory.class);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.fail("expected per-query memory breach during cursor open, got cursor: " + cursor);
                } catch (CairoException e) {
                    Assert.assertTrue("expected isOutOfMemory(), got: " + e.getFlyweightMessage(), e.isOutOfMemory());
                    TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                    TestUtils.assertContains(e.getFlyweightMessage(), "workload=QUERY");
                }
            }
        });
    }

    @Test
    public void testPartitionMapReleasesAllocations() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (" +
                    "SELECT x % 5 AS k, x AS v, timestamp_sequence(0, 1) AS ts " +
                    "FROM long_sequence(2_000)) TIMESTAMP(ts) PARTITION BY DAY");
            drainWalQueue();
            final String query = "SELECT k, lag(v, 1) OVER (PARTITION BY k ORDER BY ts DESC) FROM tab";
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                assertInTree(factory, DeferredEmitWindowRecordCursorFactory.class);
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
    public void testPendingMemoryFailsOnOpen() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (" +
                    "SELECT x AS v, timestamp_sequence(0, 1) AS ts " +
                    "FROM long_sequence(100)) TIMESTAMP(ts) PARTITION BY DAY");
            drainWalQueue();
            final String query = "SELECT v, lag(v, 63) OVER (ORDER BY ts DESC) FROM tab";
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                assertInTree(factory, DeferredEmitWindowRecordCursorFactory.class);
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 512L);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.fail("expected per-query memory breach during cursor open, got cursor: " + cursor);
                } catch (CairoException e) {
                    Assert.assertTrue("expected isOutOfMemory(), got: " + e.getFlyweightMessage(), e.isOutOfMemory());
                    TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                    TestUtils.assertContains(e.getFlyweightMessage(), "workload=QUERY");
                }
            }
            Assert.assertEquals("busy reader count", 0, engine.getBusyReaderCount());
        });
    }

    // --- C11: no fixed partition-slice reserve. A one-partition (or empty) partitioned query must
    // succeed under a per-query limit far below what the removed 256-slice up-front reserve charged.

    @Test
    public void testEmptyPartitionedSucceedsBelowOldReserve() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (" +
                    "SELECT x AS k, x AS v, timestamp_sequence(0, 1) AS ts " +
                    "FROM long_sequence(0)) TIMESTAMP(ts) PARTITION BY DAY");
            drainWalQueue();
            // pageBytes = slotBytes(8 + 2*8) * ringCapacity(31) = 744; the removed reserve charged
            // 256 * 744 ~= 190 KiB up front, which this 64 KiB limit rejects. The real allocation for
            // an empty table is a single page, so the query must now open and return zero rows.
            setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 64 * 1024L);
            // lag(..DESC) normalises to a single positive-lookahead LEAD (lookahead 30, ringCapacity
            // 31). No streaming LAG, so pendingMem (the reserve target) is the only sizable native
            // allocation; its page is slotBytes(16) * 31 = 496 B, versus the removed 256 * 496 ~=
            // 124 KiB reserve this 64 KiB limit would have rejected.
            final String query = "SELECT k, lag(v, 30) OVER (PARTITION BY k ORDER BY ts DESC) FROM tab";
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                assertInTree(factory, DeferredEmitWindowRecordCursorFactory.class);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    long rows = 0;
                    while (cursor.hasNext()) {
                        rows++;
                    }
                    Assert.assertEquals(0, rows);
                }
            }
        });
    }

    @Test
    public void testGrowthAcrossManyPartitions() throws Exception {
        // 513 distinct partitions crossing the old 256/257/512/513 doubling boundaries. Without the
        // reserve, pendingMem grows geometrically as new partitions are touched; the result must be
        // complete and correct and release symmetrically across a reuse loop.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (" +
                    "SELECT x AS k, x AS v, timestamp_sequence(0, 1) AS ts " +
                    "FROM long_sequence(513)) TIMESTAMP(ts) PARTITION BY DAY");
            drainWalQueue();
            setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 16 * 1024 * 1024L);
            final String query = "SELECT k, lag(v, 1) OVER (PARTITION BY k ORDER BY ts DESC) FROM tab";
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                assertInTree(factory, DeferredEmitWindowRecordCursorFactory.class);
                for (int i = 0; i < 3; i++) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        long rows = 0;
                        while (cursor.hasNext()) {
                            rows++;
                        }
                        Assert.assertEquals("iteration " + i, 513, rows);
                    }
                }
            }
        });
    }

    @Test
    public void testOnePartitionSucceedsBelowOldReserve() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (" +
                    "SELECT 1 AS k, x AS v, timestamp_sequence(0, 1) AS ts " +
                    "FROM long_sequence(200)) TIMESTAMP(ts) PARTITION BY DAY");
            drainWalQueue();
            setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 64 * 1024L);
            // Single positive-lookahead LEAD (ringCapacity 31, pendingMem page 496 B). The removed
            // 256-slice reserve (~124 KiB) would have failed this 64 KiB limit for one partition.
            final String query = "SELECT k, lag(v, 30) OVER (PARTITION BY k ORDER BY ts DESC) FROM tab";
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                assertInTree(factory, DeferredEmitWindowRecordCursorFactory.class);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    long rows = 0;
                    while (cursor.hasNext()) {
                        rows++;
                    }
                    Assert.assertEquals(200, rows);
                }
            }
        });
    }

    // --- C2: each streaming-LAG type binds its per-partition ring to the per-query tracker. A large
    // per-partition ring must breach the limit during iteration, a modest one must run and release.

    @Test
    public void testStreamingLagDateBreachAndRelease() throws Exception {
        assertStreamingLagRingBreachesAndReleases("cast(v as date)");
    }

    @Test
    public void testStreamingLagDoubleBreachAndRelease() throws Exception {
        assertStreamingLagRingBreachesAndReleases("v::double");
    }

    @Test
    public void testStreamingLagLongBreachAndRelease() throws Exception {
        assertStreamingLagRingBreachesAndReleases("v");
    }

    @Test
    public void testStreamingLagTimestampBreachAndRelease() throws Exception {
        assertStreamingLagRingBreachesAndReleases("cast(v as timestamp)");
    }

    private void assertStreamingLagRingBreachesAndReleases(String valueExpr) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (" +
                    "SELECT x % 8 AS k, x AS v, timestamp_sequence(0, 1) AS ts " +
                    "FROM long_sequence(4_000)) TIMESTAMP(ts) PARTITION BY DAY");
            drainWalQueue();

            // The streaming LAG is carried by lead(..DESC), which normalises to a same-type LAG of
            // the given offset; lag(v, 1 DESC) supplies the positive-lookahead driver.
            // Success + release: a small LAG offset ring (2 * 8 bytes per partition) runs to
            // completion repeatedly under a generous limit and releases every ring symmetrically.
            setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 8 * 1024 * 1024L);
            final String smallLag = "SELECT k, lag(v, 1) OVER (PARTITION BY k ORDER BY ts DESC), " +
                    "lead(" + valueExpr + ", 2) OVER (PARTITION BY k ORDER BY ts DESC) FROM tab";
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = compiler.compile(smallLag, sqlExecutionContext).getRecordCursorFactory()) {
                assertInTree(factory, DeferredEmitWindowRecordCursorFactory.class);
                for (int i = 0; i < 10; i++) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        long rows = 0;
                        while (cursor.hasNext()) {
                            rows++;
                        }
                        Assert.assertEquals("iteration " + i, 4_000, rows);
                    }
                }
            }

            // Breach: a large LAG offset makes each partition's ring 40_000 * 8 = 320 KiB; eight
            // partitions total ~2.5 MiB, so a 512 KiB limit trips on the tracker-bound ring during
            // iteration (not on the pending memory or partition map, both far smaller here).
            setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 512 * 1024L);
            final String bigLag = "SELECT k, lag(v, 1) OVER (PARTITION BY k ORDER BY ts DESC), " +
                    "lead(" + valueExpr + ", 40_000) OVER (PARTITION BY k ORDER BY ts DESC) FROM tab";
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = compiler.compile(bigLag, sqlExecutionContext).getRecordCursorFactory()) {
                assertInTree(factory, DeferredEmitWindowRecordCursorFactory.class);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    while (cursor.hasNext()) {
                        // drain until the ring allocation trips the limit
                    }
                    Assert.fail("expected per-query memory breach on the streaming LAG ring");
                } catch (CairoException e) {
                    Assert.assertTrue("expected isOutOfMemory(), got: " + e.getFlyweightMessage(), e.isOutOfMemory());
                    TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                    TestUtils.assertContains(e.getFlyweightMessage(), "workload=QUERY");
                }
            }
        });
    }

    private static void assertInTree(RecordCursorFactory factory, Class<?> expected) {
        RecordCursorFactory current = factory;
        while (current != null) {
            if (expected.isInstance(current)) {
                return;
            }
            current = current.getBaseFactory();
        }
        Assert.fail("expected " + expected.getSimpleName() + " in the factory tree, but top was " + factory.getClass().getName());
    }
}
