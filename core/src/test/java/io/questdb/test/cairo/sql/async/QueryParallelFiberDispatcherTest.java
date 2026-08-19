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

import io.questdb.MessageBus;
import io.questdb.Metrics;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.async.QueryParallelFiberDispatcher;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.MPSequence;
import io.questdb.mp.SOUnboundedCountDownLatch;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.mp.continuation.Fiber;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.FiberRuntimeState;
import io.questdb.mp.continuation.FiberTask;
import io.questdb.mp.continuation.LaunchResult;
import io.questdb.mp.continuation.SuspensionScope;
import io.questdb.std.Misc;
import io.questdb.tasks.GroupByMergeShardTask;
import io.questdb.test.AbstractTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class QueryParallelFiberDispatcherTest extends AbstractTest {

    @Test
    public void testAdapterCreationFailureReleasesFiberReservation() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root);
            try (CairoEngine engine = new CairoEngine(configuration)) {
                final FiberRuntime runtime = new FiberRuntime(1);
                final QueryParallelFiberDispatcher dispatcher = new QueryParallelFiberDispatcher(
                        engine,
                        engine.getMessageBus(),
                        runtime
                );
                try {
                    final MessageBus messageBus = engine.getMessageBus();
                    final AtomicBooleanCircuitBreaker circuitBreaker = new AtomicBooleanCircuitBreaker(engine);
                    final AtomicInteger startedCounter = new AtomicInteger();
                    final SOUnboundedCountDownLatch doneLatch = new SOUnboundedCountDownLatch();
                    doneLatch.reset();

                    final MPSequence pubSeq = messageBus.getGroupByMergeShardPubSeq();
                    final long cursor = pubSeq.next();
                    Assert.assertTrue(cursor > -1);
                    final GroupByMergeShardTask task = messageBus.getGroupByMergeShardQueue().get(cursor);
                    task.of(circuitBreaker, startedCounter, doneLatch, null, 0);
                    pubSeq.done(cursor);

                    final RuntimeException injected = new RuntimeException("injected adapter creation failure");
                    dispatcher.setBeforeMergeShardTaskCreationForTesting(() -> {
                        throw injected;
                    });
                    try {
                        dispatcher.consumeMergeShard(-1);
                        Assert.fail("expected injected adapter creation failure");
                    } catch (RuntimeException th) {
                        Assert.assertSame(injected, th);
                    }
                    Assert.assertEquals(0, runtime.getOutstandingTaskCount());
                    final Fiber fiber = runtime.tryReserveFiber();
                    Assert.assertNotNull(fiber);
                    runtime.releaseReservedFiber(fiber, fiber.getReservationEpoch());
                } finally {
                    Misc.free(dispatcher);
                    if (runtime.getOutstandingTaskCount() == 0) {
                        closeRuntime(runtime);
                    }
                }
            }
        });
    }

    @Test
    public void testAllParallelConsumersRunAsFibers() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
                @Override
                public long getGroupByParallelTopKThreshold() {
                    return 0;
                }

                @Override
                public int getGroupByShardingThreshold() {
                    return 1;
                }

                @Override
                public int getSqlPageFrameMaxRows() {
                    return 128;
                }

                @Override
                public int getSqlPageFrameMinRows() {
                    return 1;
                }

                @Override
                public int getVectorAggregateQueueCapacity() {
                    return 2;
                }
            };
            final TestWorkerPool pool = new TestWorkerPool(
                    "query-parallel-fiber-test",
                    4,
                    Metrics.DISABLED,
                    WorkerPoolMode.FIBER_HOST
            );
            TestUtils.execute(
                    pool,
                    (engine, compiler, executionContext) -> {
                        final QueryParallelFiberDispatcher dispatcher =
                                engine.getMessageBus().getQueryParallelFiberDispatcher();
                        Assert.assertNotNull(dispatcher);

                        engine.execute(
                                "create table latest_tab as (" +
                                        "select x id, ('k' || (x % 64))::symbol sym, " +
                                        "timestamp_sequence(0, 1000000) ts from long_sequence(4096)" +
                                        "), index(sym) timestamp(ts) partition by day",
                                executionContext
                        );
                        Assert.assertEquals(
                                64,
                                drain(compiler, executionContext, "select * from latest_tab latest on ts partition by sym")
                        );
                        Assert.assertTrue(dispatcher.getLatestByCreatedTaskCount() > 0);

                        engine.execute(
                                "create table vector_tab as (" +
                                        "select (x % 17)::int k, x v from long_sequence(262144)" +
                                        ")",
                                executionContext
                        );
                        Assert.assertEquals(17, drain(compiler, executionContext, "select k, sum(v) from vector_tab"));
                        Assert.assertTrue(dispatcher.getVectorAggregateCreatedTaskCount() > 0);
                        assertFiberOwnerParks(engine, compiler, executionContext);
                        assertSameRuntimeOwnerRunsLocally(pool, engine, compiler, executionContext, dispatcher);

                        engine.execute(
                                "create table group_tab as (" +
                                        "select timestamp_sequence(0, 1000000) ts, " +
                                        "'k' || (x % 64) key, x::double price, x quantity " +
                                        "from long_sequence(4096)" +
                                        ") timestamp(ts) partition by day",
                                executionContext
                        );
                        Assert.assertEquals(
                                10,
                                drain(
                                        compiler,
                                        executionContext,
                                        "select quantity, max(price) from group_tab order by quantity asc limit 10"
                                )
                        );
                        Assert.assertTrue(dispatcher.getMergeShardCreatedTaskCount() > 0);
                        Assert.assertTrue(dispatcher.getLongTopKCreatedTaskCount() > 0);
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testQuiesceDrainsPublishedTaskWithActivePublisher() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root);
            try (CairoEngine engine = new CairoEngine(configuration)) {
                final FiberRuntime runtime = new FiberRuntime(1);
                final QueryParallelFiberDispatcher dispatcher = new QueryParallelFiberDispatcher(
                        engine,
                        engine.getMessageBus(),
                        runtime
                );
                try {
                    final MessageBus messageBus = engine.getMessageBus();
                    final AtomicBooleanCircuitBreaker circuitBreaker = new AtomicBooleanCircuitBreaker(engine);
                    final AtomicInteger startedCounter = new AtomicInteger();
                    final SOUnboundedCountDownLatch doneLatch = new SOUnboundedCountDownLatch();
                    doneLatch.reset();

                    Assert.assertTrue(dispatcher.tryAcquirePublication());
                    try {
                        final MPSequence pubSeq = messageBus.getGroupByMergeShardPubSeq();
                        final long cursor = pubSeq.next();
                        Assert.assertTrue(cursor > -1);
                        final GroupByMergeShardTask task = messageBus.getGroupByMergeShardQueue().get(cursor);
                        task.of(circuitBreaker, startedCounter, doneLatch, null, 0);
                        pubSeq.done(cursor);

                        dispatcher.beginQuiesce();
                        dispatcher.progressQuiesce();

                        Assert.assertTrue(circuitBreaker.checkIfTripped());
                        Assert.assertEquals(1, startedCounter.get());
                        Assert.assertTrue(doneLatch.done(1));
                        Assert.assertFalse(dispatcher.isQuiesced());
                    } finally {
                        dispatcher.releasePublication();
                    }

                    dispatcher.progressQuiesce();
                    Assert.assertTrue(dispatcher.isQuiesced());
                } finally {
                    closeRuntime(runtime);
                    Misc.free(dispatcher);
                }
            }
        });
    }

    private static void assertFiberOwnerParks(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext executionContext
    ) throws Exception {
        final AtomicReference<Throwable> error = new AtomicReference<>();
        final CountDownLatch finished = new CountDownLatch(1);
        final AtomicBoolean launched = new AtomicBoolean();
        try (TestWorkerPool ownerPool = new TestWorkerPool(
                "query-owner-fiber-test",
                1,
                Metrics.DISABLED,
                WorkerPoolMode.FIBER_HOST
        )) {
            final FiberRuntime runtime = ownerPool.getFiberRuntime();
            final FiberTask task = new FiberTask() {
                @Override
                protected void onDone() {
                    finished.countDown();
                }

                @Override
                protected void onError(Throwable th) {
                    error.compareAndSet(null, th);
                }

                @Override
                protected boolean runStep() {
                    SuspensionScope.enterTimerShards(engine.getTimerShards());
                    try {
                        Assert.assertEquals(17, drain(compiler, executionContext, "select k, sum(v) from vector_tab"));
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                    return true;
                }
            };
            ownerPool.assign(_ -> {
                if (launched.compareAndSet(false, true)) {
                    final LaunchResult result = runtime.launch(task);
                    if (result != LaunchResult.LAUNCHED) {
                        error.set(new AssertionError("owner fiber launch failed [result=" + result + ']'));
                        finished.countDown();
                    }
                    return true;
                }
                return false;
            });
            ownerPool.start(LOG);
            Assert.assertTrue(finished.await(10, TimeUnit.SECONDS));
            ownerPool.haltAndAssertCleanForTest(WorkerPool.DEFAULT_HALT_TIMEOUT_NANOS);
            if (error.get() != null) {
                throw new AssertionError(error.get());
            }
            Assert.assertTrue(runtime.getMountCount() > 1);
        }
    }

    private static long drain(
            SqlCompiler compiler,
            SqlExecutionContext executionContext,
            CharSequence sql
    ) throws Exception {
        try (RecordCursorFactory factory = compiler.compile(sql, executionContext).getRecordCursorFactory();
                RecordCursor cursor = factory.getCursor(executionContext)) {
            long count = 0;
            while (cursor.hasNext()) {
                count++;
            }
            return count;
        }
    }

    private static void assertSameRuntimeOwnerRunsLocally(
            TestWorkerPool queryPool,
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext executionContext,
            QueryParallelFiberDispatcher dispatcher
    ) throws Exception {
        final int createdTaskCount = dispatcher.getVectorAggregateCreatedTaskCount();
        final AtomicReference<Throwable> error = new AtomicReference<>();
        final CountDownLatch finished = new CountDownLatch(1);
        final FiberTask task = new FiberTask() {
            @Override
            protected void onDone() {
                finished.countDown();
            }

            @Override
            protected void onError(Throwable th) {
                error.compareAndSet(null, th);
            }

            @Override
            protected boolean runStep() {
                SuspensionScope.enterTimerShards(engine.getTimerShards());
                try {
                    Assert.assertEquals(17, drain(compiler, executionContext, "select k, sum(v) from vector_tab"));
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
                return true;
            }
        };
        Assert.assertSame(LaunchResult.LAUNCHED, queryPool.getFiberRuntime().launch(task));
        Assert.assertTrue(finished.await(10, TimeUnit.SECONDS));
        if (error.get() != null) {
            throw new AssertionError(error.get());
        }
        Assert.assertEquals(createdTaskCount, dispatcher.getVectorAggregateCreatedTaskCount());
    }

    private static void closeRuntime(FiberRuntime runtime) {
        runtime.beginQuiesce();
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (runtime.state() != FiberRuntimeState.CLOSED && System.nanoTime() < deadline) {
            runtime.drain(8);
        }
        Assert.assertTrue(runtime.awaitClosed(deadline));
        runtime.closeAfterDrained();
    }
}
