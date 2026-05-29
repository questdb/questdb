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

    /**
     * Keyed counterpart of {@link #testParallelTwapMatchesUnderContention}: a
     * GROUP BY over a SYMBOL key drives the {@code computeKeyedBatch} reduce
     * path, and the low sharding threshold forces the sharded merge path. Every
     * key shares the same (ts, price) sequence, so every group's TWAP is
     * exactly 2500.0; a deviation means the per-frame batch descriptors were
     * not honoured on the keyed path.
     */
    @Test
    public void testParallelKeyedTwapMatchesUnderContention() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 50);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WORK_STEALING_THRESHOLD, 1);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, 2);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_BATCH_SIZE, 8);

        final int keyCount = 4;
        assertMemoryLeak(() -> {
            try (WorkerPool pool = new WorkerPool(() -> 2)) {
                TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                    engine.execute(
                            "CREATE TABLE tab (key SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR",
                            sqlExecutionContext
                    );
                    StringBuilder sb = new StringBuilder("INSERT INTO tab VALUES\n");
                    boolean first = true;
                    for (int i = 0; i < ROWS; i++) {
                        for (int k = 0; k < keyCount; k++) {
                            if (!first) {
                                sb.append(",\n");
                            }
                            first = false;
                            sb.append("('k").append(k).append("', ")
                                    .append((double) (i + 1)).append(", ").append((long) i * 1000).append(')');
                        }
                    }
                    engine.execute(sb.toString(), sqlExecutionContext);

                    final CyclicBarrier barrier = new CyclicBarrier(NUM_THREADS);
                    final SOCountDownLatch latch = new SOCountDownLatch(NUM_THREADS);
                    final Map<Integer, Throwable> errors = new ConcurrentHashMap<>();
                    final AtomicInteger mismatches = new AtomicInteger();

                    for (int t = 0; t < NUM_THREADS; t++) {
                        final int threadId = t;
                        new Thread(() -> {
                            try {
                                TestUtils.await(barrier);
                                for (int iter = 0; iter < NUM_ITERATIONS; iter++) {
                                    mismatches.addAndGet(countKeyedTwapMismatches(engine, sqlExecutionContext, keyCount));
                                }
                            } catch (Throwable th) {
                                errors.put(threadId, th);
                            } finally {
                                latch.countDown();
                            }
                        }, "twap-keyed-" + threadId).start();
                    }
                    latch.await();

                    for (Map.Entry<Integer, Throwable> e : errors.entrySet()) {
                        e.getValue().printStackTrace(System.out);
                    }
                    Assert.assertTrue("thread errors: " + errors, errors.isEmpty());
                    Assert.assertEquals(
                            "every group's twap() must be exactly 2500.0 regardless of per-slot frame "
                                    + "ordering on the keyed (computeKeyedBatch + sharded merge) reduce path",
                            0, mismatches.get()
                    );
                }, configuration, LOG);
            }
        });
    }

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
                            try {
                                TestUtils.await(barrier);
                                for (int iter = 0; iter < NUM_ITERATIONS; iter++) {
                                    double observed = runTwap(engine, sqlExecutionContext);
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
     * Duplicate-timestamp variant of {@link #testParallelTwapMatchesUnderContention}.
     * The dataset is engineered so every {@code BLOCKS}-th pair of frames
     * triggers a {@code firstKey} tie under out-of-order slot arrival:
     * <ul>
     *   <li>A "narrow" frame holds 50 rows of one timestamp (e.g.,
     *       {@code ts = 1000}).</li>
     *   <li>The following "wide" frame starts at the same timestamp and
     *       ramps up through five distinct values 10 rows each (e.g.,
     *       {@code ts in [1000, 2000, 3000, 4000, 5000]}).</li>
     * </ul>
     * If the wide frame lands in the slot before the narrow one, both
     * batches end up with {@code firstKey = 1000} but different
     * {@code lastKey}. A by-{@code firstKey}-only isAscending check
     * false-positives on the tie and skips the compaction sort, leaving
     * the buffer with a 5000 -> 1000 key drop that contaminates the
     * step-function integration. The fix in
     * {@link io.questdb.griffin.engine.groupby.SortedRunsMerge} must
     * still sort here.
     * <p>
     * Each block also appends a second wide frame ({@code [5000..9000]})
     * so successive blocks chain through ts-monotone boundaries
     * ({@code F2_last_ts = next_block_F0_ts}). Ten blocks (30 frames,
     * 1500 rows) give the scheduler plenty of out-of-order opportunities.
     * With {@code price = ts}, identical-ts pairs contribute 0 and the
     * 8M transitions deliver
     * {@code weightedSum = 8M(8M+1)/2 * 1e6}, so TWAP = {@code (8M+1) * 500},
     * which is 40500.0 for M=10 - exact in double.
     */
    @Test
    public void testParallelTwapMatchesUnderContentionWithDuplicateTimestamps() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 50);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WORK_STEALING_THRESHOLD, 1);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, 1_000_000);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_BATCH_SIZE, 8);

        final int blocks = 10;
        final double expected = (8L * blocks + 1) * 500.0; // 40500.0 for blocks=10

        assertMemoryLeak(() -> {
            try (WorkerPool pool = new WorkerPool(() -> 2)) {
                TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                    engine.execute(
                            "CREATE TABLE tab (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR",
                            sqlExecutionContext
                    );
                    engine.execute(buildDuplicateTsInsert(blocks), sqlExecutionContext);

                    final CyclicBarrier barrier = new CyclicBarrier(NUM_THREADS);
                    final SOCountDownLatch latch = new SOCountDownLatch(NUM_THREADS);
                    final Map<Integer, Throwable> errors = new ConcurrentHashMap<>();
                    final AtomicInteger mismatches = new AtomicInteger();
                    final java.util.concurrent.atomic.AtomicReference<Double> sampleWrongValue =
                            new java.util.concurrent.atomic.AtomicReference<>(null);

                    for (int t = 0; t < NUM_THREADS; t++) {
                        final int threadId = t;
                        new Thread(() -> {
                            try {
                                TestUtils.await(barrier);
                                for (int iter = 0; iter < NUM_ITERATIONS; iter++) {
                                    double observed = runTwap(engine, sqlExecutionContext);
                                    if (observed != expected) {
                                        mismatches.incrementAndGet();
                                        sampleWrongValue.compareAndSet(null, observed);
                                    }
                                }
                            } catch (Throwable th) {
                                errors.put(threadId, th);
                            } finally {
                                latch.countDown();
                            }
                        }, "twap-dup-ts-" + threadId).start();
                    }
                    latch.await();

                    for (Map.Entry<Integer, Throwable> e : errors.entrySet()) {
                        e.getValue().printStackTrace(System.out);
                    }
                    Assert.assertTrue("thread errors: " + errors, errors.isEmpty());
                    Assert.assertEquals(
                            "twap() must return exactly " + expected + " regardless of slot-arrival order. "
                                    + "A deviation here means SortedRunsMerge failed to sort batches whose "
                                    + "first keys collided across a page-frame duplicate-ts boundary. "
                                    + "Wrong sample: " + sampleWrongValue.get() + ", " + mismatches.get()
                                    + " mismatches over " + NUM_THREADS + " threads x "
                                    + NUM_ITERATIONS + " iterations.",
                            0, mismatches.get()
                    );
                }, configuration, LOG);
            }
        });
    }

    private static void appendRow(StringBuilder sb, long ts) {
        sb.append('(').append((double) ts).append(", ").append(ts).append(')');
    }

    // Builds the duplicate-ts dataset described on
    // testParallelTwapMatchesUnderContentionWithDuplicateTimestamps. Each block
    // contributes one narrow frame (50 rows at blockStartTs) plus two wide
    // frames (10 rows each at five ascending timestamps), chained so the
    // closing ts of block k equals the opening ts of block k+1.
    private static String buildDuplicateTsInsert(int blocks) {
        StringBuilder sb = new StringBuilder("INSERT INTO tab VALUES\n");
        boolean first = true;
        for (int block = 0; block < blocks; block++) {
            final long blockStartTs = 1000L + 8000L * block;
            for (int i = 0; i < 50; i++) {
                if (!first) sb.append(",\n");
                first = false;
                appendRow(sb, blockStartTs);
            }
            for (int step = 0; step < 5; step++) {
                final long ts = blockStartTs + (long) step * 1000;
                for (int j = 0; j < 10; j++) {
                    sb.append(",\n");
                    appendRow(sb, ts);
                }
            }
            for (int step = 0; step < 5; step++) {
                final long ts = blockStartTs + 4000L + (long) step * 1000;
                for (int j = 0; j < 10; j++) {
                    sb.append(",\n");
                    appendRow(sb, ts);
                }
            }
        }
        return sb.toString();
    }

    // Runs the keyed twap query and returns the number of groups whose twap
    // deviates from the exact expected value; a wrong group count also counts
    // as a mismatch so a dropped group is caught.
    private static int countKeyedTwapMismatches(CairoEngine engine, SqlExecutionContext ctx, int expectedGroups) throws Exception {
        int mismatches = 0;
        int groups = 0;
        try (RecordCursorFactory factory = engine.select("SELECT key, twap(price, ts) FROM tab", ctx);
             RecordCursor cursor = factory.getCursor(ctx)) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                groups++;
                if (record.getDouble(1) != EXPECTED_TWAP) {
                    mismatches++;
                }
            }
        }
        if (groups != expectedGroups) {
            mismatches++;
        }
        return mismatches;
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
