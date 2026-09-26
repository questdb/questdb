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

package io.questdb.test.cairo.sql.async;

import io.questdb.PropertyKey;
import io.questdb.cairo.sql.async.AdaptiveWorkStealingStrategy;
import io.questdb.cairo.sql.async.AlwaysWorkStealingStrategy;
import io.questdb.cairo.sql.async.WorkStealingStrategy;
import io.questdb.cairo.sql.async.WorkStealingStrategyFactory;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Pins the contract of {@link WorkStealingStrategy#shouldSteal(int)}: callers pass a non-negative
 * count of finished tasks.
 * <p>
 * The owner loops read that count from a {@code SOUnboundedCountDownLatch}, which counts down from
 * zero, so they have to negate it. A caller that passes the raw latch count makes
 * {@link AdaptiveWorkStealingStrategy} compute the tasks in flight as {@code started + finished}.
 * That sum only grows, so after a few tasks the owner never steals again. With no shared worker
 * thread to drain the queue the phase then hangs, which is what the SQL tests below arrange: they
 * compile with a shared worker count that selects the adaptive strategy, but start no worker pool.
 */
public class WorkStealingStrategyTest extends AbstractCairoTest {
    // The strategy factory returns the adaptive strategy for workerCount >= 4 * threshold.
    private static final int STEALING_THRESHOLD = 1;
    private static final int WORKER_COUNT = 4 * STEALING_THRESHOLD;

    @Override
    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WORK_STEALING_THRESHOLD, STEALING_THRESHOLD);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, 1);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_TOP_K_THRESHOLD, 4);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 64);
        super.setUp();
    }

    @Test
    public void testAdaptiveStrategyCountsTasksInFlight() {
        final AtomicInteger startedCounter = new AtomicInteger();
        // A long spin timeout would only slow the test down: nothing bumps the counter meanwhile.
        final WorkStealingStrategy strategy = new AdaptiveWorkStealingStrategy(16, 1).of(startedCounter);

        // Nothing is in flight: the owner steals once the spin times out.
        Assert.assertTrue(strategy.shouldSteal(0));
        startedCounter.set(100);
        Assert.assertTrue(strategy.shouldSteal(100));
        Assert.assertTrue(strategy.shouldSteal(85));

        // 16 or more tasks are in flight: the owner leaves the queue to the workers.
        Assert.assertFalse(strategy.shouldSteal(84));
        Assert.assertFalse(strategy.shouldSteal(0));
    }

    @Test
    public void testLongTopKOwnerDrainsQueueWithoutWorkers() throws Exception {
        assertMemoryLeak(() -> {
            assertAdaptiveStrategy();
            // The owner has to drain at least two queued tasks to observe a non-zero finished count.
            Assert.assertTrue(engine.getMessageBus().getGroupByLongTopKQueue().getCycle() >= 2);

            // Key k occurs 2 * k + 1 times, except for the last one, so the counts are distinct.
            execute("CREATE TABLE tab AS (SELECT sqrt(x)::LONG k FROM long_sequence(40_000))");
            try (SqlExecutionContextImpl context = TestUtils.createSqlExecutionCtx(engine, WORKER_COUNT)) {
                assertQuery("SELECT k, count() c FROM tab ORDER BY c DESC LIMIT 3")
                        .noLeakCheck()
                        .withContext(context)
                        .withPlanContaining("Long Top K lo: 3", "Async Group By workers: " + WORKER_COUNT)
                        .expectSize()
                        .returns("""
                                k\tc
                                199\t399
                                198\t397
                                197\t395
                                """);
            }
        });
    }

    @Test
    public void testNegativeFinishedCountTripsAssertion() {
        boolean isAssertEnabled = false;
        //noinspection AssertWithSideEffects
        assert isAssertEnabled = true;
        if (!isAssertEnabled) {
            return;
        }

        final WorkStealingStrategy adaptive = new AdaptiveWorkStealingStrategy(16, 1).of(new AtomicInteger());
        for (WorkStealingStrategy strategy : new WorkStealingStrategy[]{adaptive, AlwaysWorkStealingStrategy.INSTANCE}) {
            boolean isTripped = false;
            try {
                strategy.shouldSteal(-1);
            } catch (AssertionError e) {
                isTripped = true;
            }
            Assert.assertTrue(strategy.getClass().getSimpleName(), isTripped);
        }
    }

    @Test
    public void testVectorizedGroupByOwnerDrainsQueueWithoutWorkers() throws Exception {
        assertMemoryLeak(() -> {
            assertAdaptiveStrategy();
            // The owner has to drain at least two queued tasks to observe a non-zero finished count.
            Assert.assertTrue(engine.getMessageBus().getVectorAggregateQueue().getCycle() >= 2);

            execute("""
                    CREATE TABLE tab AS (
                      SELECT (x % 4)::INT k, x v, timestamp_sequence('2020-01-01', 60_000_000) ts
                      FROM long_sequence(4_000)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            try (SqlExecutionContextImpl context = TestUtils.createSqlExecutionCtx(engine, WORKER_COUNT)) {
                assertQuery("SELECT k, sum(v), count() FROM tab ORDER BY k")
                        .noLeakCheck()
                        .withContext(context)
                        .withPlanContaining("GroupBy vectorized: true workers: " + WORKER_COUNT)
                        .expectSize()
                        .returns("""
                                k\tsum\tcount
                                0\t2002000\t1000
                                1\t1999000\t1000
                                2\t2000000\t1000
                                3\t2001000\t1000
                                """);
            }
        });
    }

    private static void assertAdaptiveStrategy() {
        Assert.assertTrue(
                WorkStealingStrategyFactory.getInstance(configuration, WORKER_COUNT) instanceof AdaptiveWorkStealingStrategy
        );
    }
}
