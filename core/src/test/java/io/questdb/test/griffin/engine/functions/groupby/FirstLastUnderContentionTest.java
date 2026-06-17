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
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.WorkerPool;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Regression guard for {@code first(val)} and {@code last(val)} under the
 * same forced-contention setup that triggered
 * <a href="https://github.com/questdb/questdb/issues/7123">#7123</a> in
 * {@code twap}, {@code sparkline}, and {@code array_agg}.
 *
 * <p>Unlike those three, {@code first} / {@code last} compare rowIds
 * explicitly in {@code computeNext}, {@code computeBatch},
 * {@code computeKeyedBatch}, and {@code merge}, so they are immune to
 * per-slot frame ordering by construction - there is no buffer to compact.
 * If first/last ever start relying on insertion order, this test will fail
 * under the same workload that used to break TWAP.
 * <ul>
 *   <li>{@code first(val)} = 1.0 (val of the smallest-rowId row)</li>
 *   <li>{@code last(val)} = 5000.0 (val of the largest-rowId row)</li>
 * </ul>
 */
public class FirstLastUnderContentionTest extends AbstractCairoTest {

    private static final double EXPECTED_FIRST = 1.0;
    private static final double EXPECTED_LAST = 5_000.0;
    private static final int NUM_ITERATIONS = 30;
    private static final int NUM_THREADS = 8;
    private static final int ROWS = 5_000;

    @Test
    public void testParallelFirstLastUnderContention() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 50);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WORK_STEALING_THRESHOLD, 1);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, 1_000_000);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_BATCH_SIZE, 8);

        assertMemoryLeak(() -> {
            try (WorkerPool pool = new WorkerPool(() -> 2)) {
                TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                    engine.execute(
                            "CREATE TABLE tab (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR",
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
                    final AtomicInteger firstMismatches = new AtomicInteger();
                    final AtomicInteger lastMismatches = new AtomicInteger();

                    // Match the shared-pool worker count so the per-thread contexts still drive parallel
                    // GROUP BY (it engages only when getSharedQueryWorkerCount() > 0).
                    final int sharedQueryWorkerCount = sqlExecutionContext.getSharedQueryWorkerCount();

                    for (int t = 0; t < NUM_THREADS; t++) {
                        final int threadId = t;
                        new Thread(() -> {
                            // SqlExecutionContext is not thread-safe (it carries a single
                            // reader-pool supervisor slot, among other per-query state), so
                            // every thread compiles and runs against its own context.
                            try (SqlExecutionContext threadCtx =
                                         TestUtils.createSqlExecutionCtx(engine, sharedQueryWorkerCount)) {
                                TestUtils.await(barrier);
                                for (int iter = 0; iter < NUM_ITERATIONS; iter++) {
                                    double[] observed = runFirstLast(engine, threadCtx);
                                    if (observed[0] != EXPECTED_FIRST) {
                                        firstMismatches.incrementAndGet();
                                    }
                                    if (observed[1] != EXPECTED_LAST) {
                                        lastMismatches.incrementAndGet();
                                    }
                                }
                            } catch (Throwable th) {
                                errors.put(threadId, th);
                            } finally {
                                Path.clearThreadLocals();
                                latch.countDown();
                            }
                        }, "firstlast-" + threadId).start();
                    }
                    latch.await();

                    for (Map.Entry<Integer, Throwable> e : errors.entrySet()) {
                        e.getValue().printStackTrace(System.out);
                    }
                    Assert.assertTrue("thread errors: " + errors, errors.isEmpty());

                    Assert.assertEquals(
                            "first(val) must always return the value at the smallest rowId regardless of "
                                    + "per-slot frame ordering. A mismatch would mean first() started relying "
                                    + "on insertion order - the same class of bug that broke twap/sparkline/"
                                    + "array_agg in #7123.",
                            0, firstMismatches.get());
                    Assert.assertEquals(
                            "last(val) must always return the value at the largest rowId regardless of "
                                    + "per-slot frame ordering. A mismatch would mean last() started relying "
                                    + "on insertion order.",
                            0, lastMismatches.get());
                }, configuration, LOG);
            }
        });
    }

    private static double[] runFirstLast(CairoEngine engine, SqlExecutionContext ctx) throws Exception {
        final String sql = "SELECT first(val), last(val) FROM tab";
        try (RecordCursorFactory factory = engine.select(sql, ctx);
             RecordCursor cursor = factory.getCursor(ctx)) {
            final Record record = cursor.getRecord();
            if (!cursor.hasNext()) {
                return new double[]{Double.NaN, Double.NaN};
            }
            return new double[]{record.getDouble(0), record.getDouble(1)};
        }
    }
}
