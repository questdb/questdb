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
