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

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.QueryRegistry;
import io.questdb.griffin.SqlCompiler;
import io.questdb.std.MemoryTracker;
import io.questdb.std.MemoryTrackerWorkload;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Exercises the per-workload memory-tracker acquire/release lifecycle wired
 * into {@link QueryRegistry#register} / {@link QueryRegistry#unregister} by
 * PR 2.0. No allocation site is wired to use the tracker yet, so the asserts
 * focus on lifecycle correctness: the tracker is bound on the execution
 * context for the duration of the workload, released at the boundary, and
 * never leaks on the throw path.
 */
public class QueryRegistryMemoryTrackerTest extends AbstractCairoTest {

    @Test
    public void testCreateTableAcquiresAndReleasesTracker() throws Exception {
        assertMemoryLeak(() -> {
            Assert.assertNull(sqlExecutionContext.getMemoryTracker());
            execute("CREATE TABLE tab (x LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            Assert.assertNull(sqlExecutionContext.getMemoryTracker());
        });
    }

    @Test
    public void testNestedRegisterInheritsOuterTracker() throws Exception {
        assertMemoryLeak(() -> {
            final QueryRegistry registry = engine.getQueryRegistry();
            Assert.assertNull(sqlExecutionContext.getMemoryTracker());

            final long outerId = registry.register("outer", sqlExecutionContext);
            final MemoryTracker outer = sqlExecutionContext.getMemoryTracker();
            Assert.assertNotNull(outer);
            Assert.assertEquals(outerId, outer.getQueryId());
            Assert.assertEquals(MemoryTrackerWorkload.QUERY, outer.getWorkload());

            final long innerId = registry.register("inner", sqlExecutionContext);
            // The inner register() must observe the outer's tracker already on
            // the context and skip its own acquisition.
            Assert.assertSame(outer, sqlExecutionContext.getMemoryTracker());

            registry.unregister(innerId, sqlExecutionContext);
            // Inner unregister must leave the outer's tracker in place.
            Assert.assertSame(outer, sqlExecutionContext.getMemoryTracker());

            registry.unregister(outerId, sqlExecutionContext);
            Assert.assertNull(sqlExecutionContext.getMemoryTracker());
        });
    }

    @Test
    public void testRegisterUnregisterBindsAndClearsTracker() throws Exception {
        assertMemoryLeak(() -> {
            final QueryRegistry registry = engine.getQueryRegistry();
            Assert.assertNull(sqlExecutionContext.getMemoryTracker());

            final long queryId = registry.register("SELECT 1", sqlExecutionContext);
            final MemoryTracker tracker = sqlExecutionContext.getMemoryTracker();
            Assert.assertNotNull(tracker);
            // Tracker queryId echoes the registry-assigned id, so error messages on a
            // limit breach are correlatable with `query_activity` / `cancel query`.
            Assert.assertEquals(queryId, tracker.getQueryId());
            Assert.assertEquals(MemoryTrackerWorkload.QUERY, tracker.getWorkload());

            registry.unregister(queryId, sqlExecutionContext);
            Assert.assertNull(sqlExecutionContext.getMemoryTracker());
        });
    }

    @Test
    public void testSelectCursorAcquiresAndReleasesTracker() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (SELECT x FROM long_sequence(3))");
            drainWalQueue();

            Assert.assertNull(sqlExecutionContext.getMemoryTracker());
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final CompiledQuery cq = compiler.compile("SELECT x FROM tab", sqlExecutionContext);
                try (RecordCursorFactory factory = cq.getRecordCursorFactory()) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        final MemoryTracker tracker = sqlExecutionContext.getMemoryTracker();
                        Assert.assertNotNull(tracker);
                        Assert.assertEquals(MemoryTrackerWorkload.QUERY, tracker.getWorkload());
                        Assert.assertTrue(tracker.getQueryId() >= 0);
                        // Drain the cursor; the tracker stays bound for the full cursor lifetime.
                        while (cursor.hasNext()) {
                            // do nothing, just advance
                        }
                        Assert.assertSame(tracker, sqlExecutionContext.getMemoryTracker());
                    }
                    Assert.assertNull(sqlExecutionContext.getMemoryTracker());
                }
            }
        });
    }

    @Test
    public void testSelectThrowOnCursorOpenReleasesTracker() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (x LONG, ts TIMESTAMP) TIMESTAMP(ts)");

            // A query referencing a column that does not exist compiles but fails
            // at cursor open via SqlException. Either compile or getCursor must
            // not leak a tracker on the throw path.
            Assert.assertNull(sqlExecutionContext.getMemoryTracker());
            try {
                try (SqlCompiler compiler = engine.getSqlCompiler()) {
                    final CompiledQuery cq = compiler.compile("SELECT x FROM does_not_exist", sqlExecutionContext);
                    try (RecordCursorFactory factory = cq.getRecordCursorFactory()) {
                        // unreachable when target table is missing
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            Assert.fail("expected throw before cursor open");
                            Assert.assertNotNull(cursor);
                        }
                    }
                }
                Assert.fail("expected SqlException for missing table");
            } catch (Throwable expected) {
                // expected: missing table or column. The contract under test is the
                // post-condition below; the exception type is incidental.
            }
            Assert.assertNull(sqlExecutionContext.getMemoryTracker());
        });
    }

    @Test
    public void testTrackerCycledAcrossManyStatements() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (SELECT x FROM long_sequence(10))");
            drainWalQueue();
            Assert.assertNull(sqlExecutionContext.getMemoryTracker());

            long previousId = -1;
            for (int i = 0; i < 5; i++) {
                try (SqlCompiler compiler = engine.getSqlCompiler()) {
                    final CompiledQuery cq = compiler.compile("SELECT x FROM tab", sqlExecutionContext);
                    try (RecordCursorFactory factory = cq.getRecordCursorFactory();
                         RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        final MemoryTracker tracker = sqlExecutionContext.getMemoryTracker();
                        Assert.assertNotNull(tracker);
                        // Each statement gets a distinct queryId from the registry.
                        Assert.assertNotEquals(previousId, tracker.getQueryId());
                        previousId = tracker.getQueryId();
                        while (cursor.hasNext()) {
                            // drain
                        }
                    }
                }
                Assert.assertNull(sqlExecutionContext.getMemoryTracker());
            }
        });
    }
}
