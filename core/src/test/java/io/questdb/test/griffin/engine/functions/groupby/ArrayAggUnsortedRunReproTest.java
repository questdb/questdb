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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.WorkerPool;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * End-to-end repro that proves a per-slot buffer in
 * {@code AbstractArrayAggDoubleGroupByFunction} can become unsorted by rowId
 * under realistic parallel-GROUP-BY execution with concurrent queries.
 *
 * <p>The detection is indirect but airtight: if any per-slot buffer is unsorted
 * by rowId, the two-pointer merge produces output that is not strictly
 * monotonically increasing. The data is constructed so the correct array_agg
 * result is exactly [0, 1, 2, ..., N-1] - an inversion in the output is direct
 * evidence that an unsorted run reached the merge step.
 *
 * <p>Forced contention:
 * <ul>
 *   <li>Tiny page frames (many tasks per query).</li>
 *   <li>Small worker pool (queue is shared, easily saturated).</li>
 *   <li>Many concurrent threads firing the same query (cross-query
 *       work-stealing dominates).</li>
 *   <li>Aggressive work-stealing threshold.</li>
 * </ul>
 */
public class ArrayAggUnsortedRunReproTest extends AbstractCairoTest {

    private static final int NUM_ITERATIONS = 30;
    private static final int NUM_THREADS = 8;
    private static final int ROWS = 20_000;

    @Test
    public void testParallelArrayAggCanProduceInvertedOutput() throws Exception {
        // Frame size is small to multiply task count per query.
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
                        // val == rowId, so a correctly-ordered array_agg result is [0.0, 1.0, ..., ROWS-1.0]
                        sb.append('(').append((double) i).append(", ").append((long) i * 1000).append(')');
                    }
                    engine.execute(sb.toString(), sqlExecutionContext);

                    final String query = "SELECT array_agg(val) FROM tab";

                    final CyclicBarrier barrier = new CyclicBarrier(NUM_THREADS);
                    final SOCountDownLatch latch = new SOCountDownLatch(NUM_THREADS);
                    final Map<Integer, Throwable> errors = new ConcurrentHashMap<>();
                    final AtomicInteger totalInversions = new AtomicInteger();
                    final AtomicInteger threadsThatSawInversions = new AtomicInteger();

                    for (int t = 0; t < NUM_THREADS; t++) {
                        final int threadId = t;
                        new Thread(() -> {
                            int localInversions = 0;
                            try {
                                TestUtils.await(barrier);
                                for (int iter = 0; iter < NUM_ITERATIONS; iter++) {
                                    localInversions += countInversions(engine, sqlExecutionContext, query);
                                }
                            } catch (Throwable th) {
                                errors.put(threadId, th);
                            } finally {
                                if (localInversions > 0) {
                                    threadsThatSawInversions.incrementAndGet();
                                    totalInversions.addAndGet(localInversions);
                                }
                                latch.countDown();
                            }
                        }, "repro-" + threadId).start();
                    }
                    latch.await();

                    for (Map.Entry<Integer, Throwable> e : errors.entrySet()) {
                        e.getValue().printStackTrace(System.out);
                    }
                    Assert.assertTrue("thread errors: " + errors, errors.isEmpty());

                    System.out.println("PROOF: threadsThatSawInversions=" + threadsThatSawInversions.get()
                            + " totalInversions=" + totalInversions.get()
                            + " (over " + NUM_THREADS + " threads x " + NUM_ITERATIONS + " iterations)");

                    Assert.assertTrue(
                            "expected at least one inversion under heavy concurrent contention - "
                                    + "an inversion is direct evidence that a per-slot buffer was unsorted by rowId "
                                    + "before merge(). If this assertion ever stops firing, either the bug is fixed "
                                    + "or contention is no longer forcing cross-query work-stealing onto per-slot resources.",
                            totalInversions.get() > 0
                    );
                }, configuration, LOG);
            }
        });
    }

    private static int countInversions(CairoEngine engine, SqlExecutionContext ctx, String sql) throws Exception {
        int inversions = 0;
        try (RecordCursorFactory factory = engine.select(sql, ctx);
             RecordCursor cursor = factory.getCursor(ctx)) {
            final Record record = cursor.getRecord();
            final int arrayType = ColumnType.encodeArrayType(ColumnType.DOUBLE, 1);
            while (cursor.hasNext()) {
                ArrayView arr = record.getArray(0, arrayType);
                if (arr == null) {
                    continue;
                }
                int len = arr.getDimLen(0);
                if (len != ROWS) {
                    return Integer.MAX_VALUE;
                }
                double prev = arr.getDouble(0);
                for (int i = 1; i < len; i++) {
                    double curr = arr.getDouble(i);
                    if (curr <= prev) {
                        inversions++;
                    }
                    prev = curr;
                }
            }
        }
        return inversions;
    }
}
