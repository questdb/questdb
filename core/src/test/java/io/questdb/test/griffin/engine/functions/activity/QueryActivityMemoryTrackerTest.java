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

package io.questdb.test.griffin.engine.functions.activity;

import io.questdb.PropertyKey;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.QueryRegistry;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.MemoryTracker;
import io.questdb.std.MemoryTrackerWorkload;
import io.questdb.std.Misc;
import io.questdb.std.PerQueryMemoryTrackerProvider;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Verifies that the {@code memory_limit} column of {@code query_activity}
 * surfaces a configured per-query limit. The limit is applied per test in
 * {@link #setUp()} via {@code setProperty} so it survives the per-test override
 * reset; the provider reads it live on each tracker acquisition. The unlimited
 * counterpart (memory_limit NULL) lives in
 * {@link QueryActivityFunctionFactoryTest}, which runs with the default
 * unlimited config.
 * <p>
 * Also pins the read-safety of the {@code memory_used} / {@code memory_limit}
 * columns under concurrent query churn: those columns deref the registry
 * Entry's tracker from a different thread than the one that registered it, so
 * the value is best-effort but the read must never fault or corrupt the
 * tracker pool.
 */
public class QueryActivityMemoryTrackerTest extends AbstractCairoTest {

    @Before
    public void setUp() {
        super.setUp();
        // 128 MiB: comfortably above anything query_activity itself allocates, so
        // the self-query never breaches. The point is to prove memory_limit
        // reports a configured (non-zero) cap, not to trigger a breach.
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 128 * 1024 * 1024L);
    }

    @Test
    public void testMemoryLimitColumnReflectsConfiguredLimit() throws Exception {
        // query_activity reports the running query against itself; with a limit
        // configured, memory_limit shows the cap and memory_used is non-negative.
        assertQuery("select memory_used >= 0 used_ok, memory_limit from query_activity()")
                .noRandomAccess()
                .returns("used_ok\tmemory_limit\n" +
                        "true\t134217728\n");
    }

    /**
     * query_activity reads {@code memory_used} / {@code memory_limit} off the
     * registry Entry from a different thread than the one that registered it,
     * without synchronization. Under register/unregister churn an Entry can be
     * recycled to another query between the reader resolving it and reading the
     * column, so a row may briefly report another query's bytes -- best-effort,
     * exactly like the existing text columns. This pins the one guarantee that
     * is not best-effort: the read is always safe.
     * <p>
     * A tight register/unregister storm recycles Entry objects (and their pooled
     * trackers) as fast as possible while reader threads hammer query_activity,
     * dereferencing both memory columns on every visible Entry. The storm drives
     * {@link QueryRegistry} directly rather than through full query execution:
     * that recycles the (thread-local) Entry pool far faster than real queries
     * would, widening the race window, while the read side stays the real
     * production path. The values are deliberately not asserted; the contract
     * under test is no crash, no {@code -ea} abort, no leak, and a balanced
     * tracker pool.
     */
    @Test
    public void testMemoryColumnReadsAreSafeUnderChurn() throws Exception {
        final int producerThreads = 6;
        final int readerThreads = 2;
        final int iterations = 20_000;
        assertMemoryLeak(() -> {
            final QueryRegistry registry = engine.getQueryRegistry();
            // One factory per reader: the activity cursor is a single reused
            // instance, so sharing a factory across threads would race the cursor
            // rather than the registry.
            final RecordCursorFactory[] readerFactories = new RecordCursorFactory[readerThreads];
            for (int i = 0; i < readerThreads; i++) {
                readerFactories[i] = engine.select("SELECT memory_used, memory_limit FROM query_activity()", sqlExecutionContext);
            }

            final AtomicInteger errors = new AtomicInteger();
            final AtomicLong scans = new AtomicLong();
            final AtomicBoolean running = new AtomicBoolean(true);
            final CyclicBarrier barrier = new CyclicBarrier(producerThreads + readerThreads);
            final SOCountDownLatch producersHalt = new SOCountDownLatch(producerThreads);
            final SOCountDownLatch readersHalt = new SOCountDownLatch(readerThreads);

            // Producers: each register() acquires a tracker and binds the Entry;
            // unregister() releases the tracker and returns the Entry to the
            // thread-local pool, which the next register() immediately recycles.
            for (int p = 0; p < producerThreads; p++) {
                new Thread(() -> {
                    TestUtils.await(barrier);
                    try (SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(engine)) {
                        for (int j = 0; j < iterations; j++) {
                            final long id = registry.register("churn", ctx);
                            registry.unregister(id, ctx);
                        }
                    } catch (Throwable e) {
                        e.printStackTrace(System.out);
                        errors.incrementAndGet();
                    } finally {
                        Path.clearThreadLocals();
                        producersHalt.countDown();
                    }
                }).start();
            }

            // Readers: scan query_activity until the storm ends, forcing the
            // memory_used (col 0) and memory_limit (col 1) deref on every visible
            // Entry. The do/while guarantees at least one scan even if the storm
            // finishes first.
            for (int r = 0; r < readerThreads; r++) {
                final RecordCursorFactory factory = readerFactories[r];
                new Thread(() -> {
                    TestUtils.await(barrier);
                    try (SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(engine)) {
                        do {
                            try (RecordCursor cursor = factory.getCursor(ctx)) {
                                final Record record = cursor.getRecord();
                                while (cursor.hasNext()) {
                                    record.getLong(0);
                                    record.getLong(1);
                                }
                            }
                            scans.incrementAndGet();
                        } while (running.get());
                    } catch (Throwable e) {
                        e.printStackTrace(System.out);
                        errors.incrementAndGet();
                    } finally {
                        Path.clearThreadLocals();
                        readersHalt.countDown();
                    }
                }).start();
            }

            producersHalt.await();
            running.set(false);
            readersHalt.await();
            Misc.free(readerFactories);

            Assert.assertEquals("unexpected errors", 0, errors.get());
            Assert.assertTrue("expected query_activity scans to run", scans.get() > 0);

            // Pool integrity: the storm is perfectly balanced (every register() is
            // paired with an unregister()), so the tracker pool must be
            // non-negative and bounded by peak concurrency. A negative or runaway
            // count would mean a double-release or a lost tracker.
            Assert.assertTrue(engine.getMemoryTrackerProvider() instanceof PerQueryMemoryTrackerProvider);
            final int pooled = ((PerQueryMemoryTrackerProvider) engine.getMemoryTrackerProvider()).getPooledCount();
            Assert.assertTrue("pooled=" + pooled, pooled >= 0 && pooled <= producerThreads + readerThreads + 1);
        });
    }

    @Test
    public void testMemoryColumnsNullForNestedRegistration() throws Exception {
        // A nested registration (one whose context already has a tracker bound by an
        // outer workload) leaves Entry.memoryTracker null, so register() acquires none
        // of its own. query_activity must then read both memory_used and memory_limit as
        // NULL for that row: the memoryTracker==null branch of the two columns, which
        // neither the limit-configured nor the unlimited case reaches (both keep a bound
        // tracker on the running query). Drive the registry directly, like the churn
        // test, binding a tracker on a separate context first so register() takes the
        // nested path.
        assertMemoryLeak(() -> {
            final QueryRegistry registry = engine.getQueryRegistry();
            try (SqlExecutionContext outerCtx = TestUtils.createSqlExecutionCtx(engine)) {
                final MemoryTracker outerTracker = engine.getMemoryTrackerProvider().acquire(
                        outerCtx.getSecurityContext(), -1, MemoryTrackerWorkload.QUERY);
                outerCtx.setMemoryTracker(outerTracker);
                final long id = registry.register("nested-no-tracker", outerCtx);
                try {
                    assertQuery("SELECT memory_used IS NULL used_null, memory_limit IS NULL limit_null " +
                            "FROM query_activity() WHERE query = 'nested-no-tracker'")
                            .noLeakCheck()
                            .noRandomAccess()
                            .returns("used_null\tlimit_null\n" +
                                    "true\ttrue\n");
                } finally {
                    // The nested register() did not acquire the tracker, so unregister()
                    // leaves it bound; mimic the outer workload and free it by hand.
                    registry.unregister(id, outerCtx);
                    outerCtx.setMemoryTracker(null);
                    outerTracker.close();
                }
            }
        });
    }
}
