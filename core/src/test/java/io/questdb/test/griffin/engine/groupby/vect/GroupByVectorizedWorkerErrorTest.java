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

package io.questdb.test.griffin.engine.groupby.vect;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.std.Chars;
import io.questdb.std.RostiAllocFacadeImpl;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Verifies that the vectorized (rosti) keyed GROUP BY raises a failure a <em>queued</em> worker hit,
 * instead of publishing the shards that happened to succeed as the query's answer.
 * <p>
 * {@code GroupByVectorAggregateJob.doRun()} catches whatever escapes {@code VectorAggregateEntry.run()}
 * and only logs it, and {@code aggregate()} counts the done latch down in its {@code finally}. So a
 * queued worker that fails leaves no trace the coordinator can see: it drains the latch, merges, and
 * hands the caller numbers that silently omit whatever that worker was carrying. The work-stealing
 * path never had this hole - {@code GroupByRecordCursorFactory.runWhatsLeft()} keeps a local
 * {@code firstError} and rethrows it - so the very same failure produced either a clean error or wrong
 * numbers depending on which thread picked the frame up.
 * <p>
 * {@link CoordinatorSafeFailingRostiAllocFacade} injects that failure at
 * {@code VectorAggregateEntry.aggregateUnsafe()}'s {@code raf.updateMemoryUsage()} call, which sits on
 * the keyed branch inside the guarded region. That is where an unreadable page frame or a failed
 * parquet row-group decode lands in production: {@code PageFrameMemoryPool.navigateTo()} reaches JNI
 * from the same try.
 * <p>
 * The facade discriminates by thread and throws only off the coordinator. Letting the coordinator
 * fail would prove nothing: the dispatch loop's own {@code catch} cancels the breaker and rethrows,
 * and {@code runWhatsLeft()} raises its local {@code firstError}, so a coordinator-side failure
 * already surfaced before the fix. Work stealing makes the thread that runs any given frame
 * nondeterministic, which is why the split is explicit rather than statistical.
 */
public class GroupByVectorizedWorkerErrorTest extends AbstractCairoTest {

    // Attempts allowed to land a failure on a queued worker. Every published frame is a race between
    // the four pool workers and the coordinator's own work stealing, so a single attempt could in
    // principle be swept entirely by the coordinator and inject nothing at all.
    private static final int ATTEMPT_COUNT = 16;
    // One distinct key keeps the rosti's output order deterministic without an ORDER BY, whose sort
    // factory would change the cursor's size() contract. Key cardinality has no bearing on the
    // injection: updateMemoryUsage() is called once per (page frame, aggregate) pair regardless.
    private static final String EXPECTED_RESULT = """
            k\tsum
            0\t6144
            """;
    private static final String QUERY = "SELECT k, sum(v) FROM tab";
    private static final int WORKER_COUNT = 4;

    @Test
    public void testQueuedWorkerFailureRaisesInsteadOfPartialResult() throws Exception {
        final CoordinatorSafeFailingRostiAllocFacade facade = new CoordinatorSafeFailingRostiAllocFacade();
        configOverrideRostiAllocFacade(facade);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(new WorkerPoolConfiguration() {
                @Override
                public String getPoolName() {
                    return "vectWorkerError";
                }

                @Override
                public int getWorkerCount() {
                    return WORKER_COUNT;
                }
            });
            WorkerPoolUtils.setupQueryJobs(pool, engine);
            pool.start(null);
            try (SqlExecutionContext parallelCtx = new SqlExecutionContextImpl(engine, WORKER_COUNT)
                    .with(securityContext, bindVariableService, null, -1, circuitBreaker)) {
                parallelCtx.initNow();
                execute("CREATE TABLE tab (ts TIMESTAMP, k INT, v LONG) TIMESTAMP(ts) PARTITION BY DAY", parallelCtx);
                // Rows 30 minutes apart over 128 days, so the scan hands buildRosti ~129 page frames -
                // far more than the four workers can absorb at once, and far fewer than the 1024-slot
                // vector aggregate ring queue holds. The coordinator therefore publishes every frame
                // rather than aggregating any of them inline out of a full queue.
                execute("INSERT INTO tab SELECT (x * 1_800_000_000L)::timestamp, 0, 1::long FROM long_sequence(6_144)", parallelCtx);

                // Pin the vectorized path: a silent fall back to the general GROUP BY would leave this
                // test injecting into code the query never reaches.
                assertQuery(QUERY)
                        .noLeakCheck()
                        .withContext(parallelCtx)
                        .expectSize()
                        .withPlanContaining("GroupBy vectorized: true")
                        .returns(EXPECTED_RESULT);

                boolean hasRaisedInjectedError = false;
                for (int attempt = 0; attempt < ATTEMPT_COUNT && !hasRaisedInjectedError; attempt++) {
                    facade.arm(Thread.currentThread());
                    try {
                        drain(parallelCtx);
                        // The coordinator swept every frame this attempt, so nothing was injected and
                        // the attempt is inconclusive. A worker failure reaching here instead is the
                        // regression: the query answered with the shards that survived.
                        Assert.assertFalse("a queued worker failed, yet the query returned a result "
                                + "instead of raising; the failure was swallowed by "
                                + "GroupByVectorAggregateJob.doRun() and the surviving shards were "
                                + "merged and published as the answer", facade.hasFailedOnWorker);
                    } catch (CairoException e) {
                        Assert.assertTrue("the query failed without any worker having failed, so the "
                                + "error is not the injected one: " + e.getMessage(), facade.hasFailedOnWorker);
                        if (Chars.contains(e.getFlyweightMessage(), CoordinatorSafeFailingRostiAllocFacade.INJECTED_MESSAGE)) {
                            hasRaisedInjectedError = true;
                        } else {
                            // The injected failure cancels the shared breaker, and an entry the
                            // coordinator had already cleared the breaker check for can then find it
                            // tripped inside PerWorkerLocks.acquireSlot() and abort. runWhatsLeft()
                            // raises that from buildRosti's finally, ahead of the recorded worker
                            // error, so the attempt reports cancellation rather than the cause. It
                            // still shows the query failed rather than answering, but only the
                            // injected message identifies the cause, so retry for one that carries it.
                            Assert.assertTrue("expected either the injected error or the cancellation "
                                    + "it triggers, got: " + e.getMessage(), e.isInterruption());
                        }
                    } finally {
                        facade.disarm();
                    }
                }
                Assert.assertTrue(
                        "no attempt landed the injected failure on a queued worker, so every one of them "
                                + "either passed vacuously or reported cancellation; the coordinator "
                                + "work-stole all " + ATTEMPT_COUNT + " sweeps, or the queued path no "
                                + "longer calls RostiAllocFacade.updateMemoryUsage() -- re-derive the "
                                + "injection point",
                        hasRaisedInjectedError
                );

                // The failure path has to leave the rostis, the page frame pools and the shared vector
                // aggregate queue fit for the next query, not merely leak-free.
                assertQuery(QUERY)
                        .noLeakCheck()
                        .withContext(parallelCtx)
                        .expectSize()
                        .returns(EXPECTED_RESULT);
            } finally {
                // haltAndAssertCleanForTest rather than halt: a worker still stuck on this query's
                // entries at shutdown must fail the test, where halt() only logs the timeout and
                // carries on into the teardown that would then report an unrelated leak.
                pool.haltAndAssertCleanForTest(WorkerPool.DEFAULT_HALT_TIMEOUT_NANOS);
            }
        });
    }

    // Opens and drains the query with no result assertion: what matters here is whether it raises at
    // all. Both the factory and the cursor close on the failure path, so the caller's assertMemoryLeak
    // still measures what the aborted build left behind.
    private static void drain(SqlExecutionContext context) throws Exception {
        try (RecordCursorFactory factory = select(QUERY, context)) {
            try (RecordCursor cursor = factory.getCursor(context)) {
                //noinspection StatementWithEmptyBody
                while (cursor.hasNext()) {
                    // Pull every row; no assertion reads them, so formatting is waste.
                }
            }
        }
    }

    // Fails updateMemoryUsage() on every thread except the one that started the query, so only a frame
    // a queued worker picked up can fail. GroupByRecordCursorFactory calls updateMemoryUsage() from
    // four places, but the coordinator owns three of them - the null key insert, each merge source and
    // each wrapUp() - and this facade is silent on all three; only the keyed aggregation in
    // VectorAggregateEntry.aggregateUnsafe() runs on a worker.
    private static class CoordinatorSafeFailingRostiAllocFacade extends RostiAllocFacadeImpl {
        static final String INJECTED_MESSAGE = "injected page frame decode failure";
        // Null disarms the facade, so the surrounding DDL and the unarmed assertions run untouched
        // even though the worker pool is already live.
        private volatile Thread coordinatorThread;
        private volatile boolean hasFailedOnWorker;

        @Override
        public void updateMemoryUsage(long pRosti, long oldSize) {
            // Record the growth before failing. The aggregation that just ran really did grow the map,
            // and dropping that would make close()'s reset over-subtract - turning a test about error
            // propagation into one about NATIVE_ROSTI accounting.
            super.updateMemoryUsage(pRosti, oldSize);
            final Thread coordinator = coordinatorThread;
            if (coordinator == null || Thread.currentThread() == coordinator) {
                return;
            }
            // The write precedes the throw, and the throw precedes the entry's countDown, so the
            // coordinator reads a settled flag once it has drained the latch.
            hasFailedOnWorker = true;
            // A CairoException is what a failed parquet row-group decode raises, and the fix rethrows
            // a RuntimeException as-is, so the query carries this message verbatim.
            throw CairoException.nonCritical().put(INJECTED_MESSAGE);
        }

        void arm(Thread coordinatorThread) {
            // Clear first: arming before the reset would let a worker's flag be wiped by its own arm.
            hasFailedOnWorker = false;
            this.coordinatorThread = coordinatorThread;
        }

        void disarm() {
            coordinatorThread = null;
        }
    }
}
