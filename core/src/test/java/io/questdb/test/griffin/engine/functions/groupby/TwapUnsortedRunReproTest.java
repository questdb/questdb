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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.groupby.TwapGroupByFunction;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.WorkerPool;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Regression test that locks in the fix for
 * <a href="https://github.com/questdb/questdb/issues/7123">#7123</a>: under
 * heavy cross-query work-stealing, a per-slot buffer in
 * {@link io.questdb.griffin.engine.functions.groupby.TwapGroupByFunction} can
 * accumulate page frames in non-monotonic order. Before the fix, the
 * merge-sort merge step received an unsorted-by-ts input and silently returned
 * a wrong TWAP on a large fraction of runs.
 *
 * <p>The dataset is constructed so the correct TWAP is exactly 2500.0 with no
 * floating-point error:
 * <ul>
 *   <li>5000 rows, ts = rowIdx * 1000, price = rowIdx + 1</li>
 *   <li>Step-function weightedSum = sum(i * 1000) for i = 1..4999
 *       = 1000 * 4999 * 5000 / 2 = 12 497 500 000 (exact in double)</li>
 *   <li>totalDuration = 4999 * 1000 = 4 999 000</li>
 *   <li>TWAP = 12 497 500 000 / 4 999 000 = 2500.0 exactly</li>
 * </ul>
 * All intermediate sums stay well within 2^53, so integer addition in double
 * is exact regardless of summation order - a correct (sorted) buffer always
 * produces exactly 2500.0. Any deviation is direct evidence that the
 * compaction step in
 * {@link io.questdb.griffin.engine.groupby.SortedRunsMerge} either was not
 * called or failed to restore a sorted buffer before the integration walk in
 * {@code getDouble}.
 */
public class TwapUnsortedRunReproTest extends AbstractCairoTest {

    private static final int NUM_ITERATIONS = 30;
    private static final int NUM_THREADS = 8;
    private static final int ROWS = 5_000;
    private static final double EXPECTED_TWAP = 2500.0;

    @Test
    public void testParallelTwapMatchesUnderContention() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 50);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WORK_STEALING_THRESHOLD, 1);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, 1_000_000);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_BATCH_SIZE, 8);

        assertMemoryLeak(() -> {
            try (WorkerPool pool = new WorkerPool(() -> 2)) {
                TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                    engine.execute(
                            "CREATE TABLE tab (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR",
                            sqlExecutionContext
                    );
                    StringBuilder sb = new StringBuilder("INSERT INTO tab VALUES\n");
                    for (int i = 0; i < ROWS; i++) {
                        if (i > 0) {
                            sb.append(",\n");
                        }
                        sb.append('(').append((double) (i + 1)).append(", ").append((long) i * 1000).append(')');
                    }
                    engine.execute(sb.toString(), sqlExecutionContext);

                    final CyclicBarrier barrier = new CyclicBarrier(NUM_THREADS);
                    final SOCountDownLatch latch = new SOCountDownLatch(NUM_THREADS);
                    final Map<Integer, Throwable> errors = new ConcurrentHashMap<>();
                    final AtomicInteger mismatches = new AtomicInteger();
                    final AtomicInteger threadsThatSawMismatches = new AtomicInteger();
                    // Track one observed wrong value so the failure print is concrete.
                    final java.util.concurrent.atomic.AtomicReference<Double> sampleWrongValue =
                            new java.util.concurrent.atomic.AtomicReference<>(null);

                    for (int t = 0; t < NUM_THREADS; t++) {
                        final int threadId = t;
                        new Thread(() -> {
                            int localMismatches = 0;
                            try (SqlExecutionContext threadCtx = TestUtils.createSqlExecutionCtx(engine, pool.getWorkerCount())) {
                                TestUtils.await(barrier);
                                for (int iter = 0; iter < NUM_ITERATIONS; iter++) {
                                    double observed = runTwap(engine, threadCtx);
                                    if (observed != EXPECTED_TWAP) {
                                        localMismatches++;
                                        sampleWrongValue.compareAndSet(null, observed);
                                    }
                                }
                            } catch (Throwable th) {
                                errors.put(threadId, th);
                            } finally {
                                if (localMismatches > 0) {
                                    threadsThatSawMismatches.incrementAndGet();
                                    mismatches.addAndGet(localMismatches);
                                }
                                Path.clearThreadLocals();
                                latch.countDown();
                            }
                        }, "twap-repro-" + threadId).start();
                    }
                    latch.await();

                    for (Map.Entry<Integer, Throwable> e : errors.entrySet()) {
                        e.getValue().printStackTrace(System.out);
                    }
                    Assert.assertTrue("thread errors: " + errors, errors.isEmpty());

                    Assert.assertEquals(
                            "twap() must return the exact expected value (2500.0) on every iteration. "
                                    + "Any deviation under this contention setup would mean the compaction step "
                                    + "in SortedRunsMerge either was not invoked or failed to restore "
                                    + "key-monotonic order before the integration walk. Observed wrong sample: "
                                    + sampleWrongValue.get() + " across "
                                    + threadsThatSawMismatches.get() + " thread(s), "
                                    + mismatches.get() + " mismatches total over "
                                    + NUM_THREADS + " threads x " + NUM_ITERATIONS + " iterations.",
                            0, mismatches.get()
                    );
                }, configuration, LOG);
            }
        });
    }

    /**
     * Regression guard for a separate but related bug: the owner
     * {@code TwapGroupByFunction}'s {@code cachedPtr}/{@code cachedValue}
     * memoization in {@code getDouble} must be reset by {@code clear()} so a
     * reused factory cannot return the previous cursor run's TWAP when the
     * {@code GroupByAllocator}'s next allocation happens to land at the same
     * native address as the previous run's merged buffer (likely under the
     * C heap's thread-local free-list caches).
     *
     * <p>Address collision is probabilistic and depends on the underlying
     * malloc, so a behavioural test would be flaky. Instead this test asserts
     * the invariant directly: after the first cursor closes, the owner
     * instance's {@code cachedPtr} must be zero.
     */
    @Test
    public void testFactoryReuseClearsCachedPtr() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 50);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WORK_STEALING_THRESHOLD, 1);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, 1_000_000);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_BATCH_SIZE, 8);

        assertMemoryLeak(() -> {
            try (WorkerPool pool = new WorkerPool(() -> 2)) {
                TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                    engine.execute(
                            "CREATE TABLE tab (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR",
                            sqlExecutionContext
                    );
                    StringBuilder sb = new StringBuilder("INSERT INTO tab VALUES\n");
                    for (int i = 0; i < ROWS; i++) {
                        if (i > 0) {
                            sb.append(",\n");
                        }
                        sb.append('(').append((double) (i + 1)).append(", ").append((long) i * 1000).append(')');
                    }
                    engine.execute(sb.toString(), sqlExecutionContext);

                    final String sql = "SELECT twap(price, ts) FROM tab";
                    try (RecordCursorFactory factory = engine.select(sql, sqlExecutionContext)) {
                        final TwapGroupByFunction twap = findOwnerTwapFunction(factory);
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            Assert.assertTrue("cursor returned no row", cursor.hasNext());
                            // Force the getDouble cache to populate.
                            final double observed = cursor.getRecord().getDouble(0);
                            Assert.assertEquals(EXPECTED_TWAP, observed, 0.0);
                            Assert.assertNotEquals(
                                    "after getDouble populates the memoization cache, cachedPtr must be non-zero",
                                    0L, readCachedPtr(twap)
                            );
                        }
                        // cursor.close() above triggers frameSequence.reset() ->
                        // atom.clear() -> function.clear() on the owner. The
                        // fix in TwapGroupByFunction.clear() must zero cachedPtr
                        // so the next factory reuse cycle cannot return a stale
                        // value when the allocator hands out the same address
                        // again.
                        Assert.assertEquals(
                                "after cursor close, TwapGroupByFunction.clear() must reset cachedPtr to 0; "
                                        + "otherwise a reused factory can return a previous run's TWAP whenever "
                                        + "the GroupByAllocator's next allocation collides with the stale address.",
                                0L, readCachedPtr(twap)
                        );
                    }
                }, configuration, LOG);
            }
        });
    }

    private static TwapGroupByFunction findOwnerTwapFunction(RecordCursorFactory factory) throws Exception {
        // The query plan wraps AsyncGroupByRecordCursorFactory in projection
        // and casting layers; walk down via getBaseFactory() and scan every
        // ObjList<Function> field on each layer for the TwapGroupByFunction.
        final StringBuilder chain = new StringBuilder();
        RecordCursorFactory f = factory;
        while (f != null) {
            chain.append(f.getClass().getName()).append(" -> ");
            for (Class<?> c = f.getClass(); c != null; c = c.getSuperclass()) {
                for (Field fld : c.getDeclaredFields()) {
                    if (!ObjList.class.isAssignableFrom(fld.getType())) {
                        continue;
                    }
                    fld.setAccessible(true);
                    final Object v = fld.get(f);
                    if (!(v instanceof ObjList<?> list)) {
                        continue;
                    }
                    for (int i = 0, n = list.size(); i < n; i++) {
                        final Object item = list.getQuick(i);
                        if (item instanceof TwapGroupByFunction) {
                            return (TwapGroupByFunction) item;
                        }
                    }
                }
            }
            final RecordCursorFactory base = f.getBaseFactory();
            if (base == f) {
                break;
            }
            f = base;
        }
        throw new AssertionError("TwapGroupByFunction not present in any ObjList field of the factory chain: " + chain);
    }

    private static long readCachedPtr(TwapGroupByFunction twap) throws Exception {
        final Field cp = TwapGroupByFunction.class.getDeclaredField("cachedPtr");
        cp.setAccessible(true);
        return cp.getLong(twap);
    }

    private static double runTwap(CairoEngine engine, SqlExecutionContext ctx) throws Exception {
        final String sql = "SELECT twap(price, ts) FROM tab";

        try (RecordCursorFactory factory = engine.select(sql, ctx);
             RecordCursor cursor = factory.getCursor(ctx)) {
            final Record record = cursor.getRecord();
            if (!cursor.hasNext()) {
                return Double.NaN;
            }
            return record.getDouble(0);
        }
    }
}
