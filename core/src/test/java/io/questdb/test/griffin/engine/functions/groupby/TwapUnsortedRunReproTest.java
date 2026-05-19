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
 * {@link io.questdb.griffin.engine.functions.groupby.TwapGroupByFunction}
 * can become unsorted by timestamp under realistic parallel-GROUP-BY execution
 * with concurrent queries.
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
 * produces exactly 2500.0. Any deviation is direct evidence that merge()
 * received an unsorted-by-ts input, which can only happen if a per-slot
 * buffer accumulated frames in non-rowId order.
 */
public class TwapUnsortedRunReproTest extends AbstractCairoTest {

    private static final int NUM_ITERATIONS = 30;
    private static final int NUM_THREADS = 8;
    private static final int ROWS = 5_000;
    private static final double EXPECTED_TWAP = 2500.0;

    @Test
    public void testParallelTwapCanProduceWrongResult() throws Exception {
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

                    System.out.println("PROOF: threadsThatSawMismatches=" + threadsThatSawMismatches.get()
                            + " mismatches=" + mismatches.get()
                            + " sampleWrongValue=" + sampleWrongValue.get()
                            + " expected=" + EXPECTED_TWAP
                            + " (over " + NUM_THREADS + " threads x " + NUM_ITERATIONS + " iterations)");

                    Assert.assertTrue(
                            "expected at least one TWAP mismatch under heavy concurrent contention. "
                                    + "A mismatch from the exact expected value (2500.0) can only come from merge() "
                                    + "operating on an unsorted-by-ts input. If this assertion ever stops firing, "
                                    + "either the bug is fixed or contention is no longer forcing cross-query "
                                    + "work-stealing onto per-slot resources.",
                            mismatches.get() > 0
                    );
                }, configuration, LOG);
            }
        });
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
