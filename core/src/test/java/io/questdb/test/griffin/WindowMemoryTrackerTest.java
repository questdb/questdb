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
import org.junit.BeforeClass;
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
 * Only the per-partition map is wired; the RANGE/ROWS ring buffers
 * ({@code MemoryARW}) that scale with input rows are left for a follow-up, so
 * these tests deliberately use plain (frameless) partitioned functions whose only
 * unbounded native structure is the map.
 * <p>
 * The per-query limit is set in {@link #beforeClass()} because
 * {@code CairoEngine#getMemoryTrackerProvider} caches the
 * {@code PerQueryMemoryTrackerProvider} on first access with the limit then in
 * effect.
 */
public class WindowMemoryTrackerTest extends AbstractCairoTest {

    @BeforeClass
    public static void beforeClass() {
        // 256 KiB: a high-cardinality PARTITION BY map (tens of thousands of keys)
        // crosses it, while a handful of partitions fit comfortably.
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 256 * 1024L);
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
