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
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.SOCountDownLatch;
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
 * Concurrency tests for the per-query memory limit. Two properties that the
 * per-operator suites ({@link MapMemoryTrackerTest}, {@link WorkloadMemoryTrackerTest},
 * ...) only show implicitly are pinned here:
 * <ul>
 *   <li><b>Many-query stress.</b> Many query threads, each with its own
 *   {@link SqlExecutionContext}, hammer the shared
 *   {@code PerQueryMemoryTrackerProvider} pool with a mix of breaching and
 *   under-limit GROUP BYs. The per-query counter stays balanced (the live
 *   {@code Unsafe.recordPerQueryMemAlloc} assert and the surrounding
 *   {@code assertMemoryLeak} are the load-bearing checks), the pool neither
 *   corrupts nor grows without bound, and every tracker is released.</li>
 *   <li><b>Cross-workload isolation.</b> The three workload limits are
 *   independent. While background threads continuously breach the low
 *   {@code QUERY} limit, a {@code MAT_VIEW_REFRESH} of the same-shape
 *   aggregation and a {@code WAL_APPLY} allocation that both exceed the
 *   {@code QUERY} budget complete cleanly under their own (higher) budgets, and
 *   a foreground {@code QUERY} of that same shape still breaches at the
 *   {@code QUERY} budget. Neither workload draws against another's limit.</li>
 * </ul>
 * <p>
 * One {@link #setUp()} config serves both tests: the {@code QUERY} limit is low
 * (512 KiB) so queries breach, while the two background-workload limits are far
 * higher (256 MiB) so a {@code MAT_VIEW_REFRESH} / {@code WAL_APPLY} footprint
 * above the {@code QUERY} budget still fits with room to spare. The limits are
 * applied per test via {@code node1.setProperty} so they survive the per-test
 * override reset; the provider reads them live on each tracker acquisition.
 * <p>
 * Every thread owns its {@link SqlExecutionContext}: sharing one context across
 * query threads would violate the single-context-per-query-thread invariant the
 * tracker relies on (the tracker is a single slot on the context, reset on pool
 * reuse), which is a test bug, not a production lifecycle.
 */
public class ConcurrentMemoryTrackerTest extends AbstractCairoTest {

    // High enough that a high-cardinality MAT_VIEW_REFRESH (tens of MiB for 50,000
    // keys) clears it with a wide margin, so the only variable in the isolation test
    // is which workload runs the allocation, not its exact footprint.
    private static final long HIGH_LIMIT = 256 * 1024 * 1024L;
    private static final long LOW_LIMIT = 512 * 1024L;
    private static final long MID_ALLOC = 1024 * 1024L;

    @Override
    @Before
    public void setUp() {
        super.setUp();
        node1.setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, LOW_LIMIT);
        node1.setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_MEMORY_LIMIT_BYTES, HIGH_LIMIT);
        node1.setProperty(PropertyKey.CAIRO_WAL_APPLY_MEMORY_LIMIT_BYTES, HIGH_LIMIT);
        // A per-query breach is deterministic across the refresh's step-reduction
        // retries, so drop the backoff sleep - it would only slow the test.
        node1.setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_OOM_RETRY_TIMEOUT, 0);
        // alloc_tracked(l), used to drive a precise WAL_APPLY allocation, is dev-mode
        // only; the static setProperty form does not stick across the class's tests.
        node1.setProperty(PropertyKey.DEV_MODE_ENABLED, true);
    }

    @Test
    public void testManyConcurrentQueriesUnderLowLimitStayIsolated() throws Exception {
        final int threadCount = 8;
        final int iterations = 40;
        assertMemoryLeak(() -> {
            execute("CREATE TABLE big AS (SELECT x AS k, x * 2 AS v FROM long_sequence(100_000))");
            execute("CREATE TABLE small AS (SELECT x % 10 AS k, x AS v FROM long_sequence(100))");

            // Pin the synchronous GROUP BY path so each query allocates on its own
            // thread under its own tracker -- the parallel path would dispatch to a
            // shared worker pool and is covered by ParallelGroupByMemoryTrackerTest.
            sqlExecutionContext.setParallelGroupByEnabled(false);
            final RecordCursorFactory[] bigFactories = new RecordCursorFactory[threadCount];
            final RecordCursorFactory[] smallFactories = new RecordCursorFactory[threadCount];
            for (int i = 0; i < threadCount; i++) {
                bigFactories[i] = engine.select("SELECT k, sum(v) FROM big GROUP BY k", sqlExecutionContext);
                smallFactories[i] = engine.select("SELECT k, sum(v) FROM small GROUP BY k", sqlExecutionContext);
            }

            final AtomicInteger errors = new AtomicInteger();
            final AtomicLong breaches = new AtomicLong();
            final AtomicLong successes = new AtomicLong();
            final CyclicBarrier barrier = new CyclicBarrier(threadCount);
            final SOCountDownLatch halt = new SOCountDownLatch(threadCount);
            for (int i = 0; i < threadCount; i++) {
                final int threadIndex = i;
                new Thread(() -> {
                    // Rendezvous before building the context so a slow or failed context
                    // setup can never strand a sibling that is waiting on the barrier.
                    TestUtils.await(barrier);
                    try (SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(engine)) {
                        for (int j = 0; j < iterations; j++) {
                            final boolean breaching = ((threadIndex + j) & 1) == 0;
                            final RecordCursorFactory factory = breaching ? bigFactories[threadIndex] : smallFactories[threadIndex];
                            try (RecordCursor cursor = factory.getCursor(ctx)) {
                                long rows = 0;
                                while (cursor.hasNext()) {
                                    rows++;
                                }
                                if (breaching) {
                                    // A high-cardinality GROUP BY must cross the limit; reaching the
                                    // end means the budget was not enforced for this thread.
                                    errors.incrementAndGet();
                                } else if (rows == 10) {
                                    successes.incrementAndGet();
                                } else {
                                    errors.incrementAndGet();
                                }
                            } catch (CairoException e) {
                                if (breaching && e.isOutOfMemory()) {
                                    breaches.incrementAndGet();
                                } else {
                                    errors.incrementAndGet();
                                }
                            }
                        }
                    } catch (Throwable e) {
                        e.printStackTrace(System.out);
                        errors.incrementAndGet();
                    } finally {
                        Path.clearThreadLocals();
                        halt.countDown();
                    }
                }).start();
            }
            halt.await();
            Misc.free(bigFactories);
            Misc.free(smallFactories);

            Assert.assertEquals("unexpected errors", 0, errors.get());
            Assert.assertTrue("expected the high-cardinality queries to breach", breaches.get() > 0);
            Assert.assertTrue("expected the low-cardinality queries to succeed", successes.get() > 0);

            // Pool integrity: every thread holds at most one tracker at a time and the
            // main thread holds none here, so a healthy pool retains no more than the
            // peak concurrency. A negative or runaway count would mean a double-release
            // or a lost tracker.
            Assert.assertTrue(engine.getMemoryTrackerProvider() instanceof PerQueryMemoryTrackerProvider);
            final int pooled = ((PerQueryMemoryTrackerProvider) engine.getMemoryTrackerProvider()).getPooledCount();
            Assert.assertTrue("pooled=" + pooled, pooled >= 0 && pooled <= threadCount + 1);
        });
    }

    @Test
    public void testWorkloadBudgetsAreIndependentUnderConcurrency() throws Exception {
        final int pressureThreads = 3;
        assertMemoryLeak(() -> {
            execute("CREATE TABLE qtab AS (SELECT x AS k, x * 2 AS v FROM long_sequence(100_000))");
            sqlExecutionContext.setParallelGroupByEnabled(false);
            final RecordCursorFactory[] pressure = new RecordCursorFactory[pressureThreads];
            for (int i = 0; i < pressureThreads; i++) {
                pressure[i] = engine.select("SELECT k, sum(v) FROM qtab GROUP BY k", sqlExecutionContext);
            }

            final AtomicInteger errors = new AtomicInteger();
            final AtomicLong breaches = new AtomicLong();
            final AtomicBoolean running = new AtomicBoolean(true);
            // pressureThreads + 1: the main thread joins the rendezvous so the QUERY
            // breach storm is already running when the background workloads execute.
            final CyclicBarrier started = new CyclicBarrier(pressureThreads + 1);
            final SOCountDownLatch halt = new SOCountDownLatch(pressureThreads);
            for (int i = 0; i < pressureThreads; i++) {
                final int threadIndex = i;
                new Thread(() -> {
                    // Rendezvous first so the main thread's await(started) below cannot
                    // block forever if a pressure thread fails before reaching it.
                    TestUtils.await(started);
                    try (SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(engine)) {
                        while (running.get()) {
                            try (RecordCursor cursor = pressure[threadIndex].getCursor(ctx)) {
                                while (cursor.hasNext()) {
                                    // drain until the QUERY budget breaches
                                }
                                errors.incrementAndGet();
                            } catch (CairoException e) {
                                if (e.isOutOfMemory()) {
                                    breaches.incrementAndGet();
                                } else {
                                    errors.incrementAndGet();
                                }
                            }
                        }
                    } catch (Throwable e) {
                        e.printStackTrace(System.out);
                        errors.incrementAndGet();
                    } finally {
                        Path.clearThreadLocals();
                        halt.countDown();
                    }
                }).start();
            }
            // Release the storm, then run the two background workloads against it.
            TestUtils.await(started);
            try {
                // MAT_VIEW_REFRESH: refresh the same 50,000-key aggregation that breaches a
                // 512 KiB budget (see WorkloadMemoryTrackerTest) -- under its own far larger
                // budget it must complete despite the concurrent QUERY breaches.
                execute("CREATE TABLE base (k SYMBOL, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute(
                        "INSERT INTO base SELECT ('k' || x)::symbol, x::double, " +
                                "timestamp_sequence('2024-01-01T00:00:00.000000Z', 1) FROM long_sequence(50_000)"
                );
                execute("CREATE MATERIALIZED VIEW mv AS (SELECT k, last(v) AS v, ts FROM base SAMPLE BY 1h) PARTITION BY DAY");
                drainWalAndMatViewQueues();

                // WAL_APPLY: a 1 MiB allocation -- above the 512 KiB QUERY budget -- must apply
                // cleanly under the far larger WAL budget rather than suspend the table.
                execute("CREATE TABLE wt (k INT, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("INSERT INTO wt SELECT x::int, x::long, timestamp_sequence('2024-01-01', 1_000_000) FROM long_sequence(10)");
                drainWalQueue();
                execute("UPDATE wt SET v = alloc_tracked(" + MID_ALLOC + ")");
                drainWalQueue();

                // QUERY: the identical aggregation, run as a user query under the live storm,
                // still breaches the 512 KiB QUERY budget -- the high background-workload
                // budgets did not raise it.
                try (SqlCompiler compiler = engine.getSqlCompiler()) {
                    final CompiledQuery cq = compiler.compile("SELECT k, sum(v) FROM qtab GROUP BY k", sqlExecutionContext);
                    try (RecordCursorFactory factory = cq.getRecordCursorFactory();
                         RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        while (cursor.hasNext()) {
                            // drain until breach
                        }
                        Assert.fail("expected the foreground QUERY to breach its own budget");
                    } catch (CairoException e) {
                        Assert.assertTrue("expected isOutOfMemory(), got: " + e.getFlyweightMessage(), e.isOutOfMemory());
                        TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                        TestUtils.assertContains(e.getFlyweightMessage(), "workload=QUERY");
                    }
                }
            } finally {
                // Always stop the storm before returning: a failure above must not leave the
                // pressure threads running into teardown, where they would fault on a freed reader.
                running.set(false);
                halt.await();
            }
            Misc.free(pressure);

            // Read the outcomes only after the storm is stopped: assertQuery's
            // RSS-after-close check measures the process allocator, which the pressure
            // threads' concurrent malloc/free would otherwise perturb. The refresh and the
            // apply already ran above, under the storm; these reads just inspect the result.
            assertQuery("SELECT view_status FROM materialized_views WHERE view_name = 'mv'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("view_status\nvalid\n");
            assertQuery("SELECT suspended FROM wal_tables() WHERE name = 'wt'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("suspended\nfalse\n");

            Assert.assertEquals("unexpected errors on the QUERY pressure threads", 0, errors.get());
            Assert.assertTrue("expected the QUERY pressure threads to breach", breaches.get() > 0);
        });
    }
}
