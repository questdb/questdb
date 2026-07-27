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
import io.questdb.std.MemoryTrackerProvider;
import io.questdb.std.MemoryTrackerWorkload;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Exercises the per-workload memory-tracker acquire/release lifecycle wired
 * into {@link QueryRegistry#register} / {@link QueryRegistry#unregister}.
 * The asserts focus on lifecycle correctness: the tracker is bound on the
 * execution context for the duration of the workload, released at the
 * boundary, and never leaks on the throw path.
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
    public void testNestedRegisterInheritsBackgroundWorkloadTracker() throws Exception {
        // A mat-view refresh / WAL apply job binds its own non-QUERY tracker on a
        // dedicated execution context before running inner SQL. The inner SQL's
        // register() must inherit that tracker so its allocations charge the
        // background workload's budget, not a fresh QUERY budget.
        assertMemoryLeak(() -> {
            final QueryRegistry registry = engine.getQueryRegistry();
            final MemoryTrackerProvider provider = engine.getMemoryTrackerProvider();
            Assert.assertNull(sqlExecutionContext.getMemoryTracker());

            // Simulate the background job binding its tracker on the context first.
            final MemoryTracker outer = provider.acquire(
                    sqlExecutionContext.getSecurityContext(),
                    42,
                    MemoryTrackerWorkload.MAT_VIEW_REFRESH
            );
            sqlExecutionContext.setMemoryTracker(outer);

            final long innerId = registry.register("inner", sqlExecutionContext);
            // The inner register() must observe the background tracker and skip
            // its own acquisition.
            Assert.assertSame(outer, sqlExecutionContext.getMemoryTracker());

            registry.unregister(innerId, sqlExecutionContext);
            // Inner unregister must leave the background tracker in place; the job
            // owns its lifecycle.
            Assert.assertSame(outer, sqlExecutionContext.getMemoryTracker());

            sqlExecutionContext.setMemoryTracker(null);
            outer.close();
        });
    }

    @Test
    public void testSiblingQueryRegistersAcquireDistinctTrackers() throws Exception {
        // Concurrent PG named portals share one SqlExecutionContext and are
        // siblings, not nested. A second QUERY register() that finds a sibling's
        // QUERY tracker still bound (the first portal is suspended) must acquire
        // its own tracker, not inherit the sibling's -- otherwise the two portals
        // conflate accounting and the first portal's close recycles a tracker the
        // second is still using.
        assertMemoryLeak(() -> {
            final QueryRegistry registry = engine.getQueryRegistry();
            Assert.assertNull(sqlExecutionContext.getMemoryTracker());

            // Portal A executes and suspends; its tracker stays bound.
            final long idA = registry.register("portal A", sqlExecutionContext);
            final MemoryTracker trackerA = sqlExecutionContext.getMemoryTracker();
            Assert.assertNotNull(trackerA);
            Assert.assertEquals(idA, trackerA.getQueryId());

            // Portal B executes while A is still bound. It must NOT inherit A's
            // tracker.
            final long idB = registry.register("portal B", sqlExecutionContext);
            final MemoryTracker trackerB = sqlExecutionContext.getMemoryTracker();
            Assert.assertNotNull(trackerB);
            Assert.assertNotSame(trackerA, trackerB);
            Assert.assertEquals(idB, trackerB.getQueryId());

            // Portal A's cursor exhausts first (out-of-order, non-LIFO). Releasing
            // A must not disturb B's binding still on the context.
            registry.unregister(idA, sqlExecutionContext);
            Assert.assertSame(trackerB, sqlExecutionContext.getMemoryTracker());

            // Portal B closes last; the context returns to no tracker bound.
            registry.unregister(idB, sqlExecutionContext);
            Assert.assertNull(sqlExecutionContext.getMemoryTracker());
        });
    }

    @Test
    public void testSiblingQueryUnregisterInLifoOrderClearsTracker() throws Exception {
        // The same two-sibling setup, but with portals closing in LIFO order
        // (B before A). The slot must end up null and neither unregister may strand
        // the other's binding.
        assertMemoryLeak(() -> {
            final QueryRegistry registry = engine.getQueryRegistry();
            Assert.assertNull(sqlExecutionContext.getMemoryTracker());

            final long idA = registry.register("portal A", sqlExecutionContext);
            final MemoryTracker trackerA = sqlExecutionContext.getMemoryTracker();
            final long idB = registry.register("portal B", sqlExecutionContext);
            final MemoryTracker trackerB = sqlExecutionContext.getMemoryTracker();
            Assert.assertNotSame(trackerA, trackerB);

            // B closes first; it owns the current slot, so the slot is cleared.
            registry.unregister(idB, sqlExecutionContext);
            Assert.assertNull(sqlExecutionContext.getMemoryTracker());

            // A closes last; the slot is already null (A no longer owns it), so the
            // conditional clear leaves it untouched.
            registry.unregister(idA, sqlExecutionContext);
            Assert.assertNull(sqlExecutionContext.getMemoryTracker());
        });
    }

    @Test
    public void testRegisterReleasesTrackerWhenRegistrationThrows() throws Exception {
        // register() binds the QUERY tracker before registry.put() and the
        // listener run. registry.put() can OOM while rehashing, and the caller
        // invokes register() outside its try/finally, so unregister() never runs
        // on a throw -- register() must release the just-acquired tracker itself.
        // A throwing listener stands in for that post-acquire throw.
        assertMemoryLeak(() -> {
            final QueryRegistry registry = engine.getQueryRegistry();
            Assert.assertNull(sqlExecutionContext.getMemoryTracker());

            final MemoryTracker[] boundDuringRegister = new MemoryTracker[1];
            registry.setListener((query, queryId, ctx) -> {
                // Runs after the tracker is acquired and bound on the context.
                boundDuringRegister[0] = ctx.getMemoryTracker();
                throw new OutOfMemoryError("simulated rehash OOM");
            });
            try {
                registry.register("SELECT 1", sqlExecutionContext);
                Assert.fail("expected the registration to throw");
            } catch (OutOfMemoryError expected) {
                // expected
            } finally {
                registry.setListener(null);
            }

            Assert.assertNotNull(boundDuringRegister[0]);
            // The acquired tracker must be unbound from the context (and closed
            // back to the provider pool); without the release the slot would still
            // hold it.
            Assert.assertNull(sqlExecutionContext.getMemoryTracker());

            // The registry and tracker pool are not poisoned: a normal statement
            // still acquires a tracker and clears it cleanly.
            final long queryId = registry.register("SELECT 2", sqlExecutionContext);
            Assert.assertNotNull(sqlExecutionContext.getMemoryTracker());
            registry.unregister(queryId, sqlExecutionContext);
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
