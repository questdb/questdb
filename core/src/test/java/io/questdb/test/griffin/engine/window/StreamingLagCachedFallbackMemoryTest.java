/*******************************************************************************
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

/**
 * C3: a streaming-LAG variant that ends up on the cached path (no positive-lookahead driver, so the
 * planner uses a cached window cursor rather than {@link DeferredEmitWindowRecordCursorFactory})
 * lazily allocates its fallback partition map. That map must now be bound to the per-query memory
 * tracker so it counts against the workload limit and releases symmetrically.
 * <p>
 * The window store page size is pinned tiny so the lazily allocated ring is negligible and the
 * partition map is the dominant native allocation, isolating the map's tracker accounting.
 */
public class StreamingLagCachedFallbackMemoryTest extends AbstractCairoTest {

    @BeforeClass
    public static void setUpStatic() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_WINDOW_STREAMING_LEAD_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_SQL_WINDOW_STORE_PAGE_SIZE, "64");
        AbstractCairoTest.setUpStatic();
    }

    @Before
    public void reapplyFlag() {
        setProperty(PropertyKey.CAIRO_SQL_WINDOW_STREAMING_LEAD_ENABLED, "true");
    }

    @Test
    public void testCachedFallbackMapBreachesQueryLimit() throws Exception {
        assertMemoryLeak(() -> {
            // One row per partition key -> the fallback partition map holds ~20k entries. With the
            // ring pinned to 64-byte pages, the map is the dominant native allocation; a tight
            // per-query limit trips on it only because C3 now binds the map to the tracker.
            execute("CREATE TABLE tab AS (" +
                    "SELECT x AS k, x AS v, timestamp_sequence(0, 1) AS ts " +
                    "FROM long_sequence(20_000)) TIMESTAMP(ts) PARTITION BY DAY");
            drainWalQueue();
            // Single partitioned LAG, no positive-lookahead driver: the streaming variant is created
            // but the planner routes to a cached window cursor, exercising the lazy map fallback.
            final String query = "SELECT k, lag(v, 1) OVER (PARTITION BY k ORDER BY ts) FROM tab";
            setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 256 * 1024L);
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                Assert.assertFalse(
                        "expected the cached fallback path, not the deferred cursor",
                        isInTree(factory, DeferredEmitWindowRecordCursorFactory.class));
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    while (cursor.hasNext()) {
                        // drain until the fallback map allocation trips the tracker
                    }
                    Assert.fail("expected per-query memory breach on the cached fallback map");
                } catch (CairoException e) {
                    Assert.assertTrue("expected isOutOfMemory(), got: " + e.getFlyweightMessage(), e.isOutOfMemory());
                    TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                    TestUtils.assertContains(e.getFlyweightMessage(), "workload=QUERY");
                }
            }
            Assert.assertEquals("busy reader count", 0, engine.getBusyReaderCount());
        });
    }

    @Test
    public void testCachedFallbackMapReleasesAcrossReuse() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (" +
                    "SELECT x % 5 AS k, x AS v, timestamp_sequence(0, 1) AS ts " +
                    "FROM long_sequence(2_000)) TIMESTAMP(ts) PARTITION BY DAY");
            drainWalQueue();
            final String query = "SELECT k, lag(v, 2) OVER (PARTITION BY k ORDER BY ts) FROM tab";
            setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 8 * 1024 * 1024L);
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                Assert.assertFalse(
                        "expected the cached fallback path, not the deferred cursor",
                        isInTree(factory, DeferredEmitWindowRecordCursorFactory.class));
                // Reuse the factory across ten cursor opens: the tracked map/ring must allocate and
                // free symmetrically each time (assertMemoryLeak plus the wrapping tracker catch a
                // leak or a negative counter from asymmetric binding).
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

    private static boolean isInTree(RecordCursorFactory factory, Class<?> expected) {
        RecordCursorFactory current = factory;
        while (current != null) {
            if (expected.isInstance(current)) {
                return true;
            }
            current = current.getBaseFactory();
        }
        return false;
    }
}
