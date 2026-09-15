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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.async.AsyncQueryErrorState;
import io.questdb.cairo.sql.async.AsyncQueryProgressState;
import io.questdb.cairo.sql.async.QueryParallelFiberDispatcher;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.groupby.GroupByLongTopKJob;
import io.questdb.griffin.engine.groupby.GroupByMergeShardJob;
import io.questdb.griffin.engine.groupby.PostAggregationCircuitBreaker;
import io.questdb.griffin.engine.groupby.vect.GroupByRecordCursorFactory;
import io.questdb.griffin.engine.groupby.vect.VectorAggregateEntry;
import io.questdb.griffin.engine.orderby.LongTopKRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncGroupByAtom;
import io.questdb.griffin.engine.table.AsyncGroupByRecordCursorFactory;
import io.questdb.griffin.engine.table.GroupByShardingContext;
import io.questdb.griffin.engine.table.LatestByAllIndexedJob;
import io.questdb.griffin.engine.table.LatestByAllIndexedRecordCursorFactory;
import io.questdb.mp.CountDownLatchSPI;
import io.questdb.mp.Job;
import io.questdb.mp.MCSequence;
import io.questdb.mp.MPSequence;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SOUnboundedCountDownLatch;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.mp.continuation.Fiber;
import io.questdb.mp.continuation.FiberCancellationSignal;
import io.questdb.mp.continuation.FiberEventWaitQueue;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.FiberRuntimeState;
import io.questdb.mp.continuation.FiberTask;
import io.questdb.mp.continuation.LaunchResult;
import io.questdb.mp.continuation.SuspensionScope;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.datetime.NanosecondClock;
import io.questdb.tasks.GroupByLongTopKTask;
import io.questdb.tasks.GroupByMergeShardTask;
import io.questdb.tasks.LatestByTask;
import io.questdb.tasks.VectorAggregateTask;
import io.questdb.test.AbstractTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class QueryParallelFiberDispatcherTest extends AbstractTest {

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
                                        "timestamp_sequence(0, 1_000_000) ts from long_sequence(4096)" +
                                        "), index(sym) timestamp(ts) partition by day",
                                executionContext
                        );
                        runAsFiberOwner(engine, "query-owner-latest-by-test", () -> Assert.assertEquals(
                                64,
                                drain(compiler, executionContext, "select * from latest_tab latest on ts partition by sym")
                        ));
                        Assert.assertTrue(dispatcher.getLatestByCreatedTaskCount() > 0);

                        engine.execute(
                                "create table vector_tab as (" +
                                        "select (x % 17)::int k, x v from long_sequence(262_144)" +
                                        ")",
                                executionContext
                        );
                        runAsFiberOwner(engine, "query-owner-vector-test", () -> Assert.assertEquals(
                                17,
                                drain(compiler, executionContext, "select k, sum(v) from vector_tab")
                        ));
                        Assert.assertTrue(dispatcher.getVectorAggregateCreatedTaskCount() > 0);
                        assertFiberOwnerParks(engine, compiler, executionContext);
                        assertSameRuntimeOwnerRunsLocally(pool, engine, compiler, executionContext, dispatcher);

                        engine.execute(
                                "create table group_tab as (" +
                                        "select timestamp_sequence(0, 1_000_000) ts, " +
                                        "'k' || (x % 64) key, x::double price, x quantity " +
                                        "from long_sequence(4096)" +
                                        ") timestamp(ts) partition by day",
                                executionContext
                        );
                        runAsFiberOwner(engine, "query-owner-group-test", () -> Assert.assertEquals(
                                10,
                                drain(
                                        compiler,
                                        executionContext,
                                        "select quantity, max(price) from group_tab order by quantity asc limit 10"
                                )
                        ));
                        Assert.assertTrue(dispatcher.getMergeShardCreatedTaskCount() > 0);
                        Assert.assertTrue(dispatcher.getLongTopKCreatedTaskCount() > 0);
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testForeignLatestByCleanupStealWakesParkedOwner() throws Exception {
        assertForeignLatestByStealWakesParkedOwner(true);
    }

    @Test
    public void testForeignLatestByMainStealWakesParkedOwner() throws Exception {
        assertForeignLatestByStealWakesParkedOwner(false);
    }

    @Test
    public void testForeignLongTopKStealWakesParkedOwner() throws Exception {
        assertForeignGroupByStealWakesParkedOwner(true);
    }

    @Test
    public void testForeignMergeShardStealWakesParkedOwner() throws Exception {
        assertForeignGroupByStealWakesParkedOwner(false);
    }

    @Test
    public void testForeignVectorStealWakesParkedOwner() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
                @Override
                public long getQueryContinuationWakeIntervalMillis() {
                    return TimeUnit.HOURS.toMillis(1);
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
            try (CairoEngine engine = new CairoEngine(configuration);
                 SqlCompiler compiler = engine.getSqlCompiler();
                 SqlExecutionContext victimContext = TestUtils.createSqlExecutionCtx(engine, 2);
                 SqlExecutionContext foreignContext = TestUtils.createSqlExecutionCtx(engine, 2);
                 TestWorkerPool ownerPool = new TestWorkerPool(
                         "foreign-vector-victim",
                         1,
                         Metrics.DISABLED,
                         WorkerPoolMode.FIBER_HOST
                 )) {
                final FiberRuntime dispatcherRuntime = new FiberRuntime(1);
                final QueryParallelFiberDispatcher dispatcher = new QueryParallelFiberDispatcher(
                        engine,
                        engine.getMessageBus(),
                        dispatcherRuntime
                );
                engine.getMessageBus().setQueryParallelFiberDispatcher(dispatcher);
                final String sql = "select k, sum(v) from vector_foreign";
                engine.execute(
                        "create table vector_foreign as "
                                + "(select (x % 17)::int k, x v from long_sequence(256))",
                        foreignContext
                );
                try (RecordCursorFactory factory = compiler.compile(sql, foreignContext).getRecordCursorFactory()) {
                    TestUtils.assertFactoryInTree(
                            factory,
                            io.questdb.griffin.engine.groupby.vect.GroupByRecordCursorFactory.class
                    );
                }

                final FiberRuntime ownerRuntime = ownerPool.getFiberRuntime();
                final AtomicReference<Throwable> ownerFailure = new AtomicReference<>();
                final AtomicBoolean launched = new AtomicBoolean();
                final AtomicInteger victimRows = new AtomicInteger();
                final CountDownLatch victimDone = new CountDownLatch(1);
                final FiberTask victimTask = new FiberTask() {
                    @Override
                    protected void onDone() {
                        victimDone.countDown();
                    }

                    @Override
                    protected void onError(Throwable th) {
                        ownerFailure.compareAndSet(null, th);
                        victimDone.countDown();
                    }

                    @Override
                    protected boolean runStep() {
                        SuspensionScope.enterTimerShards(engine.getTimerShards());
                        try {
                            victimRows.set((int) drain(compiler, victimContext, sql));
                        } catch (Exception e) {
                            throw new AssertionError(e);
                        }
                        return true;
                    }
                };
                ownerPool.assign(_ -> {
                    if (launched.compareAndSet(false, true)) {
                        final LaunchResult result = ownerRuntime.launch(victimTask);
                        if (result != LaunchResult.LAUNCHED) {
                            ownerFailure.compareAndSet(
                                    null,
                                    new AssertionError("victim launch failed [result=" + result + ']')
                            );
                            victimDone.countDown();
                        }
                        return true;
                    }
                    return false;
                });

                AsyncQueryProgressState victimProgress = null;
                boolean ownerPoolStarted = false;
                try {
                    ownerPool.start(LOG);
                    ownerPoolStarted = true;
                    awaitParkedCount(ownerRuntime, 1, ownerFailure);

                    final MessageBus messageBus = engine.getMessageBus();
                    final MCSequence subSeq = messageBus.getVectorAggregateSubSeq();
                    final long firstCursor = subSeq.next();
                    Assert.assertTrue("victim did not publish a vector task", firstCursor > -1);
                    final VectorAggregateTask firstTask = messageBus.getVectorAggregateQueue().get(firstCursor);
                    victimProgress = firstTask.entry.getProgressState();
                    firstTask.entry.run(-1, subSeq, firstCursor);

                    final long observedVersion = victimProgress.getVersion();
                    Assert.assertEquals(1, ownerRuntime.getParkedFiberCount());
                    Assert.assertEquals(17, drain(compiler, foreignContext, sql));
                    Assert.assertTrue(
                            "foreign vector steal did not signal the victim",
                            victimProgress.getVersion() > observedVersion
                    );
                    Assert.assertTrue(
                            "victim did not resume before the continuation timer",
                            victimDone.await(10, TimeUnit.SECONDS)
                    );
                    Assert.assertNull(ownerFailure.get());
                    Assert.assertEquals(17, victimRows.get());
                    awaitParkedCount(ownerRuntime, 0, ownerFailure);
                    Assert.assertEquals(
                            messageBus.getVectorAggregatePubSeq().current(),
                            subSeq.current()
                    );
                    Assert.assertEquals(0, dispatcher.getVectorAggregateCreatedTaskCount());
                    Assert.assertEquals(0, dispatcherRuntime.getOutstandingTaskCount());
                } finally {
                    if (victimDone.getCount() > 0) {
                        victimContext.getCircuitBreaker().cancel();
                        dispatcher.beginQuiesce();
                        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                        while (victimDone.getCount() > 0 && System.nanoTime() < deadline) {
                            dispatcher.progressQuiesce();
                            victimDone.await(10, TimeUnit.MILLISECONDS);
                        }
                    }
                    if (ownerPoolStarted) {
                        ownerPool.haltAndAssertCleanForTest(WorkerPool.DEFAULT_HALT_TIMEOUT_NANOS);
                    }
                    engine.getMessageBus().setQueryParallelFiberDispatcher(null);
                    Misc.free(dispatcher);
                    closeRuntime(dispatcherRuntime);
                }
            }
        });
    }

    @Test
    public void testLatestByAdapterCreationFailureCompletesClaimedTaskOwnership() throws Exception {
        assertLatestByAdapterCreationFailureCompletesOwnership();
    }

    @Test
    public void testLatestByConsumerBatchesQueuedTasks() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
                @Override
                public int getLatestByQueueCapacity() {
                    return 8;
                }

                @Override
                public long getQueryContinuationWakeIntervalMillis() {
                    return TimeUnit.HOURS.toMillis(1);
                }
            };
            try (CairoEngine engine = new CairoEngine(configuration)) {
                final FiberRuntime runtime = new FiberRuntime(2);
                final QueryParallelFiberDispatcher dispatcher = new QueryParallelFiberDispatcher(
                        engine,
                        engine.getMessageBus(),
                        runtime
                );
                try {
                    final MessageBus messageBus = engine.getMessageBus();
                    final RingQueue<LatestByTask> queue = messageBus.getLatestByQueue();
                    final MPSequence pubSeq = messageBus.getLatestByPubSeq();
                    final MCSequence subSeq = messageBus.getLatestBySubSeq();
                    final FiberCancellationSignal signalA = new FiberCancellationSignal();
                    final FiberCancellationSignal signalB = new FiberCancellationSignal();
                    final long generationA = signalA.getGeneration();
                    final long generationB = signalB.getGeneration();
                    final TestScopeCircuitBreaker breakerA = new TestScopeCircuitBreaker(engine, signalB::reopen);
                    final TestScopeCircuitBreaker breakerB = new TestScopeCircuitBreaker(engine, null);
                    breakerA.setCancelledFlag(signalA);
                    breakerB.setCancelledFlag(signalB);
                    final AtomicInteger releaseCount = new AtomicInteger();
                    final CountDownLatchSPI doneLatch = releaseCount::incrementAndGet;
                    final AsyncQueryProgressState progressA = new AsyncQueryProgressState();
                    final AsyncQueryProgressState progressB = new AsyncQueryProgressState();
                    final AsyncQueryProgressState queueWaiterProgress = new AsyncQueryProgressState();
                    final AtomicReference<Throwable> failure = new AtomicReference<>();

                    final long waiterOwnerVersion = queueWaiterProgress.getVersion();
                    final long waiterGlobalVersion = dispatcher.getProgressVersion();
                    final FiberTask queueWaiterTask = new FiberTask() {
                        @Override
                        protected void onError(Throwable th) {
                            failure.compareAndSet(null, th);
                        }

                        @Override
                        protected boolean runStep() {
                            SuspensionScope.enterTimerShards(engine.getTimerShards());
                            Assert.assertTrue(
                                    dispatcher.awaitProgress(
                                            queueWaiterProgress,
                                            waiterOwnerVersion,
                                            waiterGlobalVersion,
                                            SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER
                                    )
                            );
                            return true;
                        }
                    };
                    Assert.assertSame(LaunchResult.LAUNCHED, runtime.launch(queueWaiterTask));
                    Assert.assertEquals(1, runtime.drain(1));
                    Assert.assertFalse(queueWaiterTask.isDone());
                    Assert.assertEquals(1, runtime.getParkedFiberCount());

                    final int taskCount = 4;
                    publishLatestByTask(queue, pubSeq, breakerA, doneLatch, progressA);
                    for (int i = 1; i < taskCount; i++) {
                        publishLatestByTask(queue, pubSeq, breakerB, doneLatch, progressB);
                    }

                    final int createdBefore = dispatcher.getLatestByCreatedTaskCount();
                    final long globalProgressBefore = dispatcher.getProgressVersion();
                    final long mountCountBefore = runtime.getMountCount();
                    final long ownerAProgressBefore = progressA.getVersion();
                    final long ownerBProgressBefore = progressB.getVersion();
                    Assert.assertFalse(dispatcher.consumeLatestBy(-1));
                    Assert.assertEquals(globalProgressBefore, dispatcher.getProgressVersion());
                    Assert.assertEquals(ownerAProgressBefore, progressA.getVersion());
                    Assert.assertEquals(ownerBProgressBefore, progressB.getVersion());

                    final CountDownLatch drainFinished = new CountDownLatch(1);
                    final Thread drainThread = new Thread(() -> {
                        try {
                            final int drained = runtime.drain(1);
                            if (drained != 1) {
                                failure.compareAndSet(
                                        null,
                                        new AssertionError("latest-by test must drain one task [drained=" + drained + ']')
                                );
                            }
                        } catch (Throwable th) {
                            failure.compareAndSet(null, th);
                        } finally {
                            drainFinished.countDown();
                        }
                    }, "latest-by-queue-signal");
                    dispatcher.runWithQueueWaitQueueLockedForTesting(() -> {
                        drainThread.start();
                        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                        boolean isQueueSignalBlocked = false;
                        while (!isQueueSignalBlocked) {
                            boolean hasQueueFireFrame = false;
                            boolean hasQueueSignalFrame = false;
                            boolean hasReleaseCursorFrame = false;
                            for (StackTraceElement frame : drainThread.getStackTrace()) {
                                if (FiberEventWaitQueue.class.getName().equals(frame.getClassName())
                                        && "fire".equals(frame.getMethodName())) {
                                    hasQueueFireFrame = true;
                                } else if (QueryParallelFiberDispatcher.class.getName().equals(frame.getClassName())
                                        && "signalQueueProgress".equals(frame.getMethodName())) {
                                    hasQueueSignalFrame = true;
                                } else if ("io.questdb.cairo.sql.async.LatestByFiberTask".equals(frame.getClassName())
                                        && "releaseCursor".equals(frame.getMethodName())) {
                                    hasReleaseCursorFrame = true;
                                }
                            }
                            isQueueSignalBlocked = drainThread.getState() == Thread.State.BLOCKED
                                    && hasQueueFireFrame
                                    && hasQueueSignalFrame
                                    && hasReleaseCursorFrame;
                            Assert.assertTrue(
                                    "latest-by task did not block at its cursor-release queue-progress fire",
                                    System.nanoTime() < deadline
                            );
                            Assert.assertNull(failure.get());
                            Os.pause();
                        }

                        Assert.assertEquals(1, releaseCount.get());
                        Assert.assertEquals(globalProgressBefore + 1, dispatcher.getProgressVersion());
                        Assert.assertEquals(ownerAProgressBefore, progressA.getVersion());
                        Assert.assertEquals(ownerBProgressBefore, progressB.getVersion());
                    });

                    Assert.assertTrue("latest-by drain did not finish", drainFinished.await(10, TimeUnit.SECONDS));
                    drainThread.join();
                    Assert.assertNull(failure.get());
                    Assert.assertEquals(taskCount, releaseCount.get());
                    Assert.assertEquals(createdBefore + 1, dispatcher.getLatestByCreatedTaskCount());
                    Assert.assertEquals(globalProgressBefore + taskCount, dispatcher.getProgressVersion());
                    Assert.assertEquals(mountCountBefore + 1, runtime.getMountCount());
                    Assert.assertSame(signalA, breakerA.observedSignal);
                    Assert.assertEquals(generationA, breakerA.observedGeneration);
                    Assert.assertSame(signalB, breakerB.observedSignal);
                    Assert.assertEquals(generationB, breakerB.observedGeneration);
                    Assert.assertEquals(generationB + 1, signalB.getGeneration());
                    Assert.assertTrue(signalB.isCancelled(generationB));
                    Assert.assertEquals(ownerAProgressBefore + 1, progressA.getVersion());
                    Assert.assertEquals(ownerBProgressBefore + 3, progressB.getVersion());
                    Assert.assertEquals(pubSeq.current(), subSeq.current());

                    Assert.assertEquals(1, runtime.drain(1));
                    Assert.assertTrue(queueWaiterTask.isDone());
                    Assert.assertNull(failure.get());
                } finally {
                    closeRuntime(runtime);
                    Misc.free(dispatcher);
                }
            }
        });
    }

    @Test
    public void testLatestByConsumerStopsAtBatchLimit() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
                @Override
                public int getLatestByQueueCapacity() {
                    return 128;
                }
            };
            try (CairoEngine engine = new CairoEngine(configuration)) {
                final FiberRuntime runtime = new FiberRuntime(1);
                final QueryParallelFiberDispatcher dispatcher = new QueryParallelFiberDispatcher(
                        engine,
                        engine.getMessageBus(),
                        runtime
                );
                try {
                    final MessageBus messageBus = engine.getMessageBus();
                    final RingQueue<LatestByTask> queue = messageBus.getLatestByQueue();
                    final MPSequence pubSeq = messageBus.getLatestByPubSeq();
                    final MCSequence subSeq = messageBus.getLatestBySubSeq();
                    final AtomicBooleanCircuitBreaker cancelledBreaker = new AtomicBooleanCircuitBreaker(engine);
                    cancelledBreaker.cancel();
                    final AtomicInteger releaseCount = new AtomicInteger();
                    final AsyncQueryProgressState progressState = new AsyncQueryProgressState();

                    final int taskCount = 65;
                    for (int i = 0; i < taskCount; i++) {
                        publishLatestByTask(
                                queue,
                                pubSeq,
                                cancelledBreaker,
                                releaseCount::incrementAndGet,
                                progressState
                        );
                    }

                    final long globalProgressBefore = dispatcher.getProgressVersion();
                    final long mountCountBefore = runtime.getMountCount();
                    final long ownerProgressBefore = progressState.getVersion();
                    Assert.assertFalse(dispatcher.consumeLatestBy(-1));
                    Assert.assertEquals(1, runtime.drain(1));
                    Assert.assertEquals(64, releaseCount.get());
                    Assert.assertEquals(globalProgressBefore + 64, dispatcher.getProgressVersion());
                    Assert.assertEquals(mountCountBefore + 1, runtime.getMountCount());
                    Assert.assertEquals(ownerProgressBefore + 64, progressState.getVersion());
                    Assert.assertEquals(pubSeq.current() - 1, subSeq.current());

                    Assert.assertFalse(dispatcher.consumeLatestBy(-1));
                    Assert.assertEquals(1, runtime.drain(1));
                    Assert.assertEquals(taskCount, releaseCount.get());
                    Assert.assertEquals(globalProgressBefore + taskCount, dispatcher.getProgressVersion());
                    Assert.assertEquals(mountCountBefore + 2, runtime.getMountCount());
                    Assert.assertEquals(ownerProgressBefore + taskCount, progressState.getVersion());
                    Assert.assertEquals(pubSeq.current(), subSeq.current());
                    Assert.assertEquals(1, dispatcher.getLatestByCreatedTaskCount());
                } finally {
                    closeRuntime(runtime);
                    Misc.free(dispatcher);
                }
            }
        });
    }

    @Test
    public void testLatestByFiberOwnerHelpsOwnUnpublishedTasks() throws Exception {
        assertParallelFiberOwnerHelpsOwnWork(DrainTaskType.LATEST_BY);
    }

    @Test
    public void testLatestByJobAcknowledgesQueueSlotAfterTaskFailure() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String injectedError = "injected frame decode failure";
            final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
                @Override
                public int getLatestByQueueCapacity() {
                    return 1;
                }
            };
            try (CairoEngine engine = new CairoEngine(configuration)) {
                final MessageBus messageBus = engine.getMessageBus();
                final RingQueue<LatestByTask> queue = messageBus.getLatestByQueue();
                final MPSequence pubSeq = messageBus.getLatestByPubSeq();
                final LatestByAllIndexedJob job = new LatestByAllIndexedJob(messageBus);
                final Job.WorkerContext workerContext = new Job.WorkerContext() {
                    @Override
                    public int carrierId() {
                        return -1;
                    }

                    @Override
                    public boolean isTerminating() {
                        return false;
                    }
                };

                final AtomicBooleanCircuitBreaker throwingBreaker = new AtomicBooleanCircuitBreaker(engine) {
                    @Override
                    public boolean checkIfTripped() {
                        throw new UnsupportedOperationException(injectedError);
                    }
                };
                final AtomicInteger releaseCount = new AtomicInteger();
                final CountDownLatchSPI doneLatch = releaseCount::incrementAndGet;

                publishLatestByTask(
                        queue,
                        pubSeq,
                        throwingBreaker,
                        doneLatch,
                        new AsyncQueryProgressState()
                );
                try {
                    job.run(workerContext);
                    Assert.fail("expected the injected frame decode failure");
                } catch (UnsupportedOperationException e) {
                    Assert.assertEquals(injectedError, e.getMessage());
                }
                Assert.assertEquals(1, releaseCount.get());

                // The single queue slot only wraps if the failed run acknowledged its cursor.
                final AtomicBooleanCircuitBreaker cancelledBreaker = new AtomicBooleanCircuitBreaker(engine);
                cancelledBreaker.cancel();
                publishLatestByTask(
                        queue,
                        pubSeq,
                        cancelledBreaker,
                        doneLatch,
                        new AsyncQueryProgressState()
                );
                Assert.assertTrue(job.run(workerContext));
                Assert.assertEquals(2, releaseCount.get());
            }
        });
    }

    // A drain aborted by cancellation would leak the native arguments array; the leak check is
    // the regression assertion.
    @Test
    public void testLatestByOwnerCancelledMidDrainReleasesNativeMemory() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
                @Override
                public int getLatestByQueueCapacity() {
                    return 2;
                }

                @Override
                public long getQueryContinuationWakeIntervalMillis() {
                    return TimeUnit.HOURS.toMillis(1);
                }
            };
            try (
                    CairoEngine engine = new CairoEngine(configuration);
                    SqlCompiler compiler = engine.getSqlCompiler();
                    SqlExecutionContext setupContext = TestUtils.createSqlExecutionCtx(engine, 1)
            ) {
                final FiberCancellationSignal victimSignal = new FiberCancellationSignal();
                final AtomicBooleanCircuitBreaker victimBreaker = new AtomicBooleanCircuitBreaker(engine);
                victimBreaker.setCancelledFlag(victimSignal);
                try (
                        SqlExecutionContext victimContext = TestUtils.createSqlExecutionCtx(engine, 1).with(victimBreaker);
                        TestWorkerPool ownerPool = new TestWorkerPool(
                                "latest-by-cancel-mid-drain",
                                1,
                                Metrics.DISABLED,
                                WorkerPoolMode.FIBER_HOST
                        )
                ) {
                    final String sql = "select * from latest_cancel latest on ts partition by sym";
                    engine.execute(
                            "create table latest_cancel as ("
                                    + "select (x * 1_000_000L)::timestamp ts, "
                                    + "('k' || (x % 64))::symbol sym, x value "
                                    + "from long_sequence(512)"
                                    + "), index(sym) timestamp(ts)",
                            setupContext
                    );
                    try (RecordCursorFactory factory = compiler.compile(sql, setupContext).getRecordCursorFactory()) {
                        TestUtils.assertFactoryInTree(factory, LatestByAllIndexedRecordCursorFactory.class);
                    }

                    final FiberRuntime dispatcherRuntime = new FiberRuntime(1);
                    final QueryParallelFiberDispatcher dispatcher = new QueryParallelFiberDispatcher(
                            engine,
                            engine.getMessageBus(),
                            dispatcherRuntime
                    );
                    engine.getMessageBus().setQueryParallelFiberDispatcher(dispatcher);

                    final FiberRuntime ownerRuntime = ownerPool.getFiberRuntime();
                    final AtomicReference<Throwable> victimFailure = new AtomicReference<>();
                    final CountDownLatch victimDone = new CountDownLatch(1);
                    final AtomicBoolean launched = new AtomicBoolean();
                    final FiberTask victimTask = new FiberTask() {
                        @Override
                        protected void onDone() {
                            victimDone.countDown();
                        }

                        @Override
                        protected void onError(Throwable th) {
                            victimFailure.compareAndSet(null, th);
                            victimDone.countDown();
                        }

                        @Override
                        protected boolean runStep() {
                            SuspensionScope.enterTimerShards(engine.getTimerShards());
                            try {
                                drain(compiler, victimContext, sql);
                            } catch (RuntimeException | Error e) {
                                throw e;
                            } catch (Exception e) {
                                throw new AssertionError(e);
                            }
                            return true;
                        }
                    };
                    ownerPool.assign(_ -> {
                        if (launched.compareAndSet(false, true)) {
                            final LaunchResult result = ownerRuntime.launch(victimTask);
                            if (result != LaunchResult.LAUNCHED) {
                                victimFailure.compareAndSet(
                                        null,
                                        new AssertionError("victim launch failed [result=" + result + ']')
                                );
                                victimDone.countDown();
                            }
                            return true;
                        }
                        return false;
                    });

                    AsyncQueryProgressState victimProgress = null;
                    boolean isOwnerPoolStarted = false;
                    try {
                        final MessageBus messageBus = engine.getMessageBus();
                        final MPSequence pubSeq = messageBus.getLatestByPubSeq();
                        final MCSequence subSeq = messageBus.getLatestBySubSeq();
                        final long cursorBefore = pubSeq.current();

                        ownerPool.start(LOG);
                        isOwnerPoolStarted = true;
                        awaitParkedCount(ownerRuntime, 1, victimFailure);
                        final long victimCursor = pubSeq.current();
                        Assert.assertEquals(cursorBefore + 1, victimCursor);
                        Assert.assertEquals(cursorBefore, subSeq.current());
                        final LatestByTask queuedTask = messageBus.getLatestByQueue().get(victimCursor);
                        victimProgress = queuedTask.getProgressState();
                        Assert.assertNotNull(victimProgress);
                        final SqlExecutionCircuitBreaker sharedBreaker = queuedTask.getCircuitBreaker();
                        Assert.assertFalse(sharedBreaker.checkIfTripped());

                        victimBreaker.cancel();

                        // a tripped shared breaker proves the victim left the publish loop
                        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                        while (!sharedBreaker.checkIfTripped()) {
                            Assert.assertTrue(
                                    "victim did not cancel its shared breaker",
                                    System.nanoTime() < deadline
                            );
                            Assert.assertNull(victimFailure.get());
                            Os.pause();
                        }
                        awaitParkedCount(ownerRuntime, 1, victimFailure);
                        Assert.assertEquals(1, victimDone.getCount());

                        final long seq = subSeq.next();
                        Assert.assertTrue("queued latest-by task must be claimable", seq > -1);
                        try {
                            Assert.assertTrue(messageBus.getLatestByQueue().get(seq).run());
                        } finally {
                            subSeq.done(seq);
                            try {
                                dispatcher.signalQueueProgress();
                            } finally {
                                dispatcher.signalOwnerProgress(victimProgress);
                            }
                        }

                        Assert.assertTrue(
                                "victim did not finish after the drain completed",
                                victimDone.await(10, TimeUnit.SECONDS)
                        );
                        final Throwable th = victimFailure.get();
                        Assert.assertNotNull("cancelled query must fail, not return a partial result", th);
                        Assert.assertTrue(String.valueOf(th), th instanceof CairoException);
                        final CairoException e = (CairoException) th;
                        Assert.assertTrue(e.isInterruption());
                        Assert.assertEquals(SqlExecutionCircuitBreaker.STATE_CANCELLED, e.getInterruptionReason());
                        Assert.assertEquals(pubSeq.current(), subSeq.current());
                        Assert.assertEquals(0, dispatcher.getLatestByCreatedTaskCount());
                        Assert.assertEquals(0, dispatcherRuntime.getOutstandingTaskCount());
                    } finally {
                        if (victimDone.getCount() > 0) {
                            victimBreaker.cancel();
                            if (victimProgress != null) {
                                dispatcher.signalOwnerProgress(victimProgress);
                            }
                            dispatcher.beginQuiesce();
                            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                            while (victimDone.getCount() > 0 && System.nanoTime() < deadline) {
                                dispatcher.progressQuiesce();
                                victimDone.await(10, TimeUnit.MILLISECONDS);
                            }
                        }
                        if (isOwnerPoolStarted) {
                            ownerPool.haltAndAssertCleanForTest(WorkerPool.DEFAULT_HALT_TIMEOUT_NANOS);
                        }
                        engine.getMessageBus().setQueryParallelFiberDispatcher(null);
                        Misc.free(dispatcher);
                        closeRuntime(dispatcherRuntime);
                    }
                }
            }
        });
    }

    @Test
    public void testLatestByOwnerHelpsWithoutConsumers() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
                @Override
                public int getLatestByQueueCapacity() {
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
            };
            final WorkerPoolConfiguration poolConfiguration = new WorkerPoolConfiguration() {
                @Override
                public Metrics getMetrics() {
                    return Metrics.DISABLED;
                }

                @Override
                public String getPoolName() {
                    return "zero-consumer-latest-by-test";
                }

                @Override
                public int getWorkerCount() {
                    return 0;
                }

                @Override
                public WorkerPoolMode getWorkerPoolMode() {
                    return WorkerPoolMode.FIBER_HOST;
                }
            };
            final TestWorkerPool pool = new TestWorkerPool(poolConfiguration);
            try (CairoEngine engine = new CairoEngine(configuration);
                 SqlCompiler compiler = engine.getSqlCompiler();
                 SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine, 2)) {
                TestUtils.setupWorkerPool(pool, engine);
                pool.start(LOG);
                final QueryParallelFiberDispatcher dispatcher = engine.getMessageBus().getQueryParallelFiberDispatcher();
                Assert.assertNotNull(dispatcher);
                engine.execute(
                        """
                                CREATE TABLE tab AS (
                                  SELECT (x * 1_000_000L)::timestamp ts, ('s' || (x % 64))::symbol sym, x v
                                  FROM long_sequence(512)
                                ), INDEX(sym) TIMESTAMP(ts)""",
                        executionContext
                );
                try (RecordCursorFactory factory = compiler.compile("SELECT * FROM tab LATEST ON ts PARTITION BY sym", executionContext)
                        .getRecordCursorFactory()) {
                    TestUtils.assertFactoryInTree(factory, LatestByAllIndexedRecordCursorFactory.class);
                }

                final MessageBus messageBus = engine.getMessageBus();
                final MCSequence subSeq = messageBus.getLatestBySubSeq();
                final long consumedBefore = subSeq.current();
                final AtomicReference<Throwable> ownerFailure = new AtomicReference<>();
                final CountDownLatch ownerDone = new CountDownLatch(1);
                final AtomicInteger rowCount = new AtomicInteger();
                final Thread owner = new Thread(() -> {
                    try (RecordCursorFactory factory = engine.select("SELECT * FROM tab LATEST ON ts PARTITION BY sym", executionContext);
                         RecordCursor cursor = factory.getCursor(executionContext)) {
                        while (cursor.hasNext()) {
                            rowCount.incrementAndGet();
                        }
                    } catch (Throwable th) {
                        ownerFailure.set(th);
                    } finally {
                        ownerDone.countDown();
                    }
                }, "latest-by-owner");
                owner.setDaemon(true);
                boolean completed = false;
                boolean publishedTaskConsumed = false;
                InterruptedException interrupted = null;
                owner.start();
                try {
                    completed = ownerDone.await(10, TimeUnit.SECONDS);
                    publishedTaskConsumed = subSeq.current() > consumedBefore;
                } catch (InterruptedException e) {
                    interrupted = e;
                } finally {
                    if (ownerDone.getCount() > 0) {
                        executionContext.getCircuitBreaker().cancel();
                        final RingQueue<LatestByTask> queue = messageBus.getLatestByQueue();
                        while (ownerDone.getCount() > 0) {
                            final long cursor = subSeq.next();
                            if (cursor > -1) {
                                try {
                                    queue.get(cursor).run();
                                } catch (Throwable th) {
                                    ownerFailure.compareAndSet(null, th);
                                } finally {
                                    subSeq.done(cursor);
                                }
                            } else {
                                Os.pause();
                            }
                        }
                    }
                    while (owner.isAlive()) {
                        try {
                            owner.join();
                        } catch (InterruptedException e) {
                            if (interrupted == null) {
                                interrupted = e;
                            }
                            executionContext.getCircuitBreaker().cancel();
                        }
                    }
                    if (interrupted != null) {
                        Thread.currentThread().interrupt();
                    }
                }
                if (interrupted != null) {
                    throw interrupted;
                }
                Assert.assertTrue("latest-by owner did not finish", completed);
                Assert.assertTrue("owner did not consume a published latest-by task", publishedTaskConsumed);
                Assert.assertEquals(64, rowCount.get());
                Assert.assertNull(ownerFailure.get());
                Assert.assertEquals(0, dispatcher.getLatestByCreatedTaskCount());
            } finally {
                pool.haltAndAssertCleanForTest(WorkerPool.DEFAULT_HALT_TIMEOUT_NANOS);
            }
        });
    }

    @Test
    public void testLatestByTaskCancelsSharedBreakerBeforeReleasingOwner() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String injectedError = "injected frame decode failure";
            final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root);
            try (CairoEngine engine = new CairoEngine(configuration)) {
                final AtomicBooleanCircuitBreaker circuitBreaker = new AtomicBooleanCircuitBreaker(engine) {
                    @Override
                    public boolean checkIfTripped() {
                        throw new UnsupportedOperationException(injectedError);
                    }
                };
                final int[] releaseCount = new int[1];
                final boolean[] isCancelledOnRelease = new boolean[1];
                final AsyncQueryErrorState scanError = new AsyncQueryErrorState();
                final boolean[] hasErrorOnRelease = new boolean[1];
                final CountDownLatchSPI doneLatch = () -> {
                    releaseCount[0]++;
                    isCancelledOnRelease[0] = circuitBreaker.getCancelledFlag().get();
                    hasErrorOnRelease[0] = scanError.hasError();
                };
                try (LatestByTask task = new LatestByTask(configuration)) {
                    task.of(null, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0, 0, -1, 0, 0L, 0L, doneLatch, circuitBreaker, new AsyncQueryProgressState(), scanError);
                    try {
                        task.run();
                        Assert.fail("expected the injected frame decode failure");
                    } catch (UnsupportedOperationException e) {
                        Assert.assertEquals(injectedError, e.getMessage());
                    }
                }
                Assert.assertEquals(1, releaseCount[0]);
                Assert.assertTrue(
                        "shared breaker must be cancelled before the owner's done latch is released",
                        isCancelledOnRelease[0]
                );
                Assert.assertTrue(
                        "scan error must be recorded before the owner's done latch is released, "
                                + "or the owner reports a bare cancellation instead of the real failure",
                        hasErrorOnRelease[0]
                );
                try {
                    scanError.throwError();
                    Assert.fail("expected the recorded scan error to be rethrown");
                } catch (UnsupportedOperationException e) {
                    Assert.assertEquals(injectedError, e.getMessage());
                }
            }
        });
    }

    @Test
    public void testLongTopKAdapterCreationFailureCompletesClaimedTaskOwnership() throws Exception {
        assertCountedAdapterCreationFailureCompletesOwnership(true);
    }

    @Test
    public void testLongTopKConsumerBatchesQueuedTasks() throws Exception {
        assertCountedConsumerBatchesQueuedTasks(true);
    }

    @Test
    public void testLongTopKFiberOwnerHelpsOwnUnpublishedShards() throws Exception {
        assertParallelFiberOwnerHelpsOwnWork(DrainTaskType.LONG_TOP_K);
    }

    @Test
    public void testLongTopKOwnerCancelledMidDrainWaitsForCompletion() throws Exception {
        assertParallelOwnerCancelledMidDrain(DrainTaskType.LONG_TOP_K);
    }

    @Test
    public void testLongTopKOwnerHelpsWithoutConsumers() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
                @Override
                public int getGroupByMergeShardQueueCapacity() {
                    return 1;
                }

                @Override
                public long getGroupByParallelTopKThreshold() {
                    return 0;
                }

                @Override
                public int getGroupByShardingThreshold() {
                    return 1;
                }

                @Override
                public int getGroupByTopKQueueCapacity() {
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
            };
            final WorkerPoolConfiguration poolConfiguration = new WorkerPoolConfiguration() {
                @Override
                public Metrics getMetrics() {
                    return Metrics.DISABLED;
                }

                @Override
                public String getPoolName() {
                    return "zero-consumer-long-top-k-test";
                }

                @Override
                public int getWorkerCount() {
                    return 0;
                }

                @Override
                public WorkerPoolMode getWorkerPoolMode() {
                    return WorkerPoolMode.FIBER_HOST;
                }
            };
            final TestWorkerPool pool = new TestWorkerPool(poolConfiguration);
            try (CairoEngine engine = new CairoEngine(configuration);
                 SqlCompiler compiler = engine.getSqlCompiler();
                 SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine, 1)) {
                TestUtils.setupWorkerPool(pool, engine);
                pool.start(LOG);
                final QueryParallelFiberDispatcher dispatcher =
                        engine.getMessageBus().getQueryParallelFiberDispatcher();
                Assert.assertNotNull(dispatcher);
                engine.execute(
                        "create table tab as (select ('k' || x) key, x v from long_sequence(128))",
                        executionContext
                );
                try (RecordCursorFactory factory = compiler.compile(
                        "select key, max(v) m from tab order by m desc limit 10",
                        executionContext
                ).getRecordCursorFactory()) {
                    TestUtils.assertFactoryInTree(factory, LongTopKRecordCursorFactory.class);
                    TestUtils.assertFactoryInTree(factory, AsyncGroupByRecordCursorFactory.class);
                }

                final MessageBus messageBus = engine.getMessageBus();
                final MCSequence subSeq = messageBus.getGroupByLongTopKSubSeq();
                final long consumedBefore = subSeq.current();
                final AtomicReference<Throwable> ownerFailure = new AtomicReference<>();
                final CountDownLatch ownerDone = new CountDownLatch(1);
                final AtomicInteger rowCount = new AtomicInteger();
                final AtomicBoolean exactOutput = new AtomicBoolean(true);
                final Thread owner = new Thread(() -> {
                    try (RecordCursorFactory factory = engine.select(
                            "select key, max(v) m from tab order by m desc limit 10",
                            executionContext
                    );
                         RecordCursor cursor = factory.getCursor(executionContext)) {
                        while (cursor.hasNext()) {
                            final int row = rowCount.getAndIncrement();
                            exactOutput.compareAndSet(
                                    true,
                                    row < 10
                                            && ("k" + (128 - row)).contentEquals(cursor.getRecord().getStrA(0))
                                            && cursor.getRecord().getLong(1) == 128 - row
                            );
                        }
                    } catch (Throwable th) {
                        ownerFailure.set(th);
                    } finally {
                        ownerDone.countDown();
                    }
                }, "long-top-k-owner");
                owner.setDaemon(true);
                boolean completed = false;
                boolean publishedTaskConsumed = false;
                InterruptedException interrupted = null;
                owner.start();
                try {
                    completed = ownerDone.await(10, TimeUnit.SECONDS);
                    publishedTaskConsumed = subSeq.current() > consumedBefore;
                } catch (InterruptedException e) {
                    interrupted = e;
                } finally {
                    if (ownerDone.getCount() > 0) {
                        executionContext.getCircuitBreaker().cancel();
                        final RingQueue<GroupByMergeShardTask> mergeQueue = messageBus.getGroupByMergeShardQueue();
                        final RingQueue<GroupByLongTopKTask> topKQueue = messageBus.getGroupByLongTopKQueue();
                        final MCSequence mergeSubSeq = messageBus.getGroupByMergeShardSubSeq();
                        while (ownerDone.getCount() > 0) {
                            final long mergeCursor = mergeSubSeq.next();
                            if (mergeCursor > -1) {
                                final GroupByMergeShardTask task = mergeQueue.get(mergeCursor);
                                final GroupByShardingContext context = task.getShardingContext();
                                try {
                                    GroupByMergeShardJob.run(-1, task, mergeSubSeq, mergeCursor, context);
                                } catch (Throwable th) {
                                    ownerFailure.compareAndSet(null, th);
                                }
                            }
                            final long topKCursor = subSeq.next();
                            if (topKCursor > -1) {
                                final GroupByLongTopKTask task = topKQueue.get(topKCursor);
                                try {
                                    GroupByLongTopKJob.run(-1, task, subSeq, topKCursor, task.getAtom());
                                } catch (Throwable th) {
                                    ownerFailure.compareAndSet(null, th);
                                }
                            }
                            if (mergeCursor < 0 && topKCursor < 0) {
                                Os.pause();
                            }
                        }
                    }
                    while (owner.isAlive()) {
                        try {
                            owner.join();
                        } catch (InterruptedException e) {
                            if (interrupted == null) {
                                interrupted = e;
                            }
                            executionContext.getCircuitBreaker().cancel();
                        }
                    }
                    if (interrupted != null) {
                        Thread.currentThread().interrupt();
                    }
                }
                if (interrupted != null) {
                    throw interrupted;
                }
                Assert.assertTrue("long top-K owner did not finish", completed);
                Assert.assertTrue("owner did not consume a published long top-K task", publishedTaskConsumed);
                Assert.assertTrue(exactOutput.get());
                Assert.assertEquals(10, rowCount.get());
                Assert.assertNull(ownerFailure.get());
                Assert.assertEquals(0, dispatcher.getLongTopKCreatedTaskCount());
            } finally {
                pool.haltAndAssertCleanForTest(WorkerPool.DEFAULT_HALT_TIMEOUT_NANOS);
            }
        });
    }

    @Test
    public void testLongTopKOwnerStealSignalsBothSourcesWhenTerminalCleanupThrows() throws Exception {
        assertCountedOwnerStealSignalsBothSourcesWhenTerminalCleanupThrows(true);
    }

    @Test
    public void testLongTopKOwnerStealSignalsQueueBeforeTerminalCompletion() throws Exception {
        assertCountedOwnerStealSignalsQueueBeforeTerminalCompletion(true);
    }

    @Test
    public void testMergeShardAdapterCreationFailureCompletesClaimedTaskOwnership() throws Exception {
        assertCountedAdapterCreationFailureCompletesOwnership(false);
    }

    @Test
    public void testMergeShardConsumerBatchesQueuedTasks() throws Exception {
        assertCountedConsumerBatchesQueuedTasks(false);
    }

    @Test
    public void testMergeShardFiberOwnerHelpsOwnUnpublishedShards() throws Exception {
        assertParallelFiberOwnerHelpsOwnWork(DrainTaskType.MERGE_SHARD);
    }

    @Test
    public void testMergeShardOwnerCancelledMidDrainWaitsForCompletion() throws Exception {
        assertParallelOwnerCancelledMidDrain(DrainTaskType.MERGE_SHARD);
    }

    @Test
    public void testMergeShardOwnerHelpsWithoutConsumers() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
                @Override
                public int getGroupByMergeShardQueueCapacity() {
                    return 1;
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
            };
            final WorkerPoolConfiguration poolConfiguration = new WorkerPoolConfiguration() {
                @Override
                public Metrics getMetrics() {
                    return Metrics.DISABLED;
                }

                @Override
                public String getPoolName() {
                    return "zero-consumer-merge-shard-test";
                }

                @Override
                public int getWorkerCount() {
                    return 0;
                }

                @Override
                public WorkerPoolMode getWorkerPoolMode() {
                    return WorkerPoolMode.FIBER_HOST;
                }
            };
            final TestWorkerPool pool = new TestWorkerPool(poolConfiguration);
            try (CairoEngine engine = new CairoEngine(configuration);
                 SqlCompiler compiler = engine.getSqlCompiler();
                 SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine, 1)) {
                TestUtils.setupWorkerPool(pool, engine);
                pool.start(LOG);
                final QueryParallelFiberDispatcher dispatcher = engine.getMessageBus().getQueryParallelFiberDispatcher();
                Assert.assertNotNull(dispatcher);
                engine.execute(
                        "create table tab as (select ('k' || x) key, x v from long_sequence(128))",
                        executionContext
                );
                try (RecordCursorFactory factory = compiler.compile("select key, max(v) m from tab", executionContext)
                        .getRecordCursorFactory()) {
                    TestUtils.assertFactoryInTree(factory, AsyncGroupByRecordCursorFactory.class);
                }

                final MessageBus messageBus = engine.getMessageBus();
                final MCSequence subSeq = messageBus.getGroupByMergeShardSubSeq();
                final long consumedBefore = subSeq.current();
                final AtomicReference<Throwable> ownerFailure = new AtomicReference<>();
                final CountDownLatch ownerDone = new CountDownLatch(1);
                final AtomicInteger rowCount = new AtomicInteger();
                final Thread owner = new Thread(() -> {
                    try (RecordCursorFactory factory = engine.select("select key, max(v) m from tab", executionContext);
                         RecordCursor cursor = factory.getCursor(executionContext)) {
                        while (cursor.hasNext()) {
                            rowCount.incrementAndGet();
                        }
                    } catch (Throwable th) {
                        ownerFailure.set(th);
                    } finally {
                        ownerDone.countDown();
                    }
                }, "merge-shard-owner");
                owner.setDaemon(true);
                boolean completed = false;
                boolean publishedTaskConsumed = false;
                InterruptedException interrupted = null;
                owner.start();
                try {
                    completed = ownerDone.await(10, TimeUnit.SECONDS);
                    publishedTaskConsumed = subSeq.current() > consumedBefore;
                } catch (InterruptedException e) {
                    interrupted = e;
                } finally {
                    if (ownerDone.getCount() > 0) {
                        executionContext.getCircuitBreaker().cancel();
                        final RingQueue<GroupByMergeShardTask> queue = messageBus.getGroupByMergeShardQueue();
                        while (ownerDone.getCount() > 0) {
                            final long cursor = subSeq.next();
                            if (cursor > -1) {
                                final GroupByMergeShardTask task = queue.get(cursor);
                                final GroupByShardingContext context = task.getShardingContext();
                                try {
                                    GroupByMergeShardJob.run(-1, task, subSeq, cursor, context);
                                } catch (Throwable th) {
                                    ownerFailure.compareAndSet(null, th);
                                }
                            } else {
                                Os.pause();
                            }
                        }
                    }
                    while (owner.isAlive()) {
                        try {
                            owner.join();
                        } catch (InterruptedException e) {
                            if (interrupted == null) {
                                interrupted = e;
                            }
                            executionContext.getCircuitBreaker().cancel();
                        }
                    }
                    if (interrupted != null) {
                        Thread.currentThread().interrupt();
                    }
                }
                if (interrupted != null) {
                    throw interrupted;
                }
                Assert.assertTrue("merge-shard owner did not finish", completed);
                Assert.assertTrue("owner did not consume a published merge shard", publishedTaskConsumed);
                Assert.assertEquals(128, rowCount.get());
                Assert.assertNull(ownerFailure.get());
                Assert.assertEquals(0, dispatcher.getMergeShardCreatedTaskCount());
            } finally {
                pool.haltAndAssertCleanForTest(WorkerPool.DEFAULT_HALT_TIMEOUT_NANOS);
            }
        });
    }

    @Test
    public void testMergeShardOwnerStealSignalsBothSourcesWhenTerminalCleanupThrows() throws Exception {
        assertCountedOwnerStealSignalsBothSourcesWhenTerminalCleanupThrows(false);
    }

    @Test
    public void testMergeShardOwnerStealSignalsQueueBeforeTerminalCompletion() throws Exception {
        assertCountedOwnerStealSignalsQueueBeforeTerminalCompletion(false);
    }

    @Test
    public void testProgressSignalWakesOnlyItsOwner() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Push the timer fallback out of reach so progress signals are the only way out.
            final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
                @Override
                public long getQueryContinuationWakeIntervalMillis() {
                    return TimeUnit.HOURS.toMillis(1);
                }
            };
            try (CairoEngine engine = new CairoEngine(configuration)) {
                final int ownerCount = 3;
                final FiberRuntime dispatcherRuntime = new FiberRuntime(1);
                final QueryParallelFiberDispatcher dispatcher = new QueryParallelFiberDispatcher(
                        engine,
                        engine.getMessageBus(),
                        dispatcherRuntime
                );
                try {
                    final AtomicReference<Throwable> failure = new AtomicReference<>();
                    final AsyncQueryProgressState[] progressStates = new AsyncQueryProgressState[ownerCount];
                    final CountDownLatch[] resumed = new CountDownLatch[ownerCount];
                    for (int i = 0; i < ownerCount; i++) {
                        progressStates[i] = new AsyncQueryProgressState();
                        resumed[i] = new CountDownLatch(1);
                    }
                    final AtomicInteger launched = new AtomicInteger();
                    try (TestWorkerPool ownerPool = new TestWorkerPool(
                            "progress-precision",
                            1,
                            Metrics.DISABLED,
                            WorkerPoolMode.FIBER_HOST
                    )) {
                        final FiberRuntime ownerRuntime = ownerPool.getFiberRuntime();
                        ownerPool.assign(_ -> {
                            final int ownerIndex = launched.getAndIncrement();
                            if (ownerIndex >= ownerCount) {
                                return false;
                            }
                            final AsyncQueryProgressState progressState = progressStates[ownerIndex];
                            final CountDownLatch ownerResumed = resumed[ownerIndex];
                            final FiberTask task = new FiberTask() {
                                @Override
                                protected void onDone() {
                                    ownerResumed.countDown();
                                }

                                @Override
                                protected void onError(Throwable th) {
                                    failure.compareAndSet(null, th);
                                    ownerResumed.countDown();
                                }

                                @Override
                                protected boolean runStep() {
                                    SuspensionScope.enterTimerShards(engine.getTimerShards());
                                    // A spuriously woken owner re-parks, exactly like a production
                                    // owner that finds its done latch still open.
                                    while (progressState.getVersion() == 0) {
                                        dispatcher.awaitProgress(
                                                progressState,
                                                progressState.getVersion(),
                                                dispatcher.getProgressVersion(),
                                                SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER
                                        );
                                    }
                                    return true;
                                }
                            };
                            final LaunchResult result = ownerRuntime.launch(task);
                            if (result != LaunchResult.LAUNCHED) {
                                failure.compareAndSet(null, new AssertionError("owner fiber launch failed [result=" + result + ']'));
                                ownerResumed.countDown();
                            }
                            return true;
                        });
                        ownerPool.start(LOG);

                        awaitParkedCount(ownerRuntime, ownerCount, failure);

                        // Signal in reverse launch order, so the FIFO head of the dispatcher's
                        // shared queue is never the signalled owner.
                        final long globalProgressBefore = dispatcher.getProgressVersion();
                        for (int i = ownerCount - 1; i >= 0; i--) {
                            final long ownerProgressBefore = progressStates[i].getVersion();
                            dispatcher.signalOwnerProgress(progressStates[i]);
                            Assert.assertEquals(globalProgressBefore, dispatcher.getProgressVersion());
                            Assert.assertEquals(ownerProgressBefore + 1, progressStates[i].getVersion());
                            Assert.assertTrue(
                                    "signalled owner did not resume",
                                    resumed[i].await(10, TimeUnit.SECONDS)
                            );
                            awaitParkedCount(ownerRuntime, i, failure);
                            for (int j = 0; j < i; j++) {
                                Assert.assertEquals("unsignalled owner must stay parked", 1, resumed[j].getCount());
                            }
                        }
                        ownerPool.haltAndAssertCleanForTest(WorkerPool.DEFAULT_HALT_TIMEOUT_NANOS);
                    }
                    Assert.assertNull(failure.get());
                } finally {
                    closeRuntime(dispatcherRuntime);
                    Misc.free(dispatcher);
                }
            }
        });
    }

    @Test
    public void testProgressWaitChecksBothVersionsAfterArming() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
                @Override
                public long getQueryContinuationWakeIntervalMillis() {
                    return TimeUnit.HOURS.toMillis(1);
                }
            };
            try (CairoEngine engine = new CairoEngine(configuration)) {
                final FiberRuntime runtime = new FiberRuntime(2);
                final QueryParallelFiberDispatcher dispatcher = new QueryParallelFiberDispatcher(
                        engine,
                        engine.getMessageBus(),
                        runtime
                );
                try {
                    for (int sourceIndex = 0; sourceIndex < 2; sourceIndex++) {
                        final boolean isOwnerSignal = sourceIndex == 0;
                        final String sourceName = isOwnerSignal ? "owner" : "queue";
                        final AsyncQueryProgressState progressState = new AsyncQueryProgressState();
                        final AtomicReference<Throwable> failure = new AtomicReference<>();
                        final CountDownLatch drainFinished = new CountDownLatch(1);
                        final CountDownLatch waitEntered = new CountDownLatch(1);
                        final long ownerVersion = progressState.getVersion();
                        final long globalVersion = dispatcher.getProgressVersion();
                        final FiberTask progressTask = new FiberTask() {
                            @Override
                            protected void onError(Throwable th) {
                                failure.compareAndSet(null, th);
                            }

                            @Override
                            protected boolean runStep() {
                                SuspensionScope.enterTimerShards(engine.getTimerShards());
                                waitEntered.countDown();
                                Assert.assertTrue(
                                        dispatcher.awaitProgress(
                                                progressState,
                                                ownerVersion,
                                                globalVersion,
                                                SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER
                                        )
                                );
                                return true;
                            }
                        };
                        final Thread drainThread = new Thread(() -> {
                            try {
                                final int drained = runtime.drain(1);
                                if (drained != 1) {
                                    failure.compareAndSet(
                                            null,
                                            new AssertionError("progress test must drain one task [drained=" + drained + ']')
                                    );
                                }
                            } catch (Throwable th) {
                                failure.compareAndSet(null, th);
                            } finally {
                                drainFinished.countDown();
                            }
                        }, "query-progress-arm-race-" + sourceName);

                        Assert.assertSame(LaunchResult.LAUNCHED, runtime.launch(progressTask));
                        dispatcher.runWithOwnerWaitQueueLockedForTesting(progressState, () -> {
                            drainThread.start();
                            try {
                                Assert.assertTrue(
                                        "progress task did not enter its wait",
                                        waitEntered.await(10, TimeUnit.SECONDS)
                                );
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new AssertionError(e);
                            }

                            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                            boolean isRegistrationBlocked = false;
                            while (!isRegistrationBlocked) {
                                for (StackTraceElement frame : drainThread.getStackTrace()) {
                                    if (FiberEventWaitQueue.class.getName().equals(frame.getClassName())
                                            && "register".equals(frame.getMethodName())) {
                                        isRegistrationBlocked = drainThread.getState() == Thread.State.BLOCKED;
                                        break;
                                    }
                                }
                                Assert.assertTrue(
                                        "progress task did not block before wait registration",
                                        System.nanoTime() < deadline
                                );
                                Assert.assertNull(failure.get());
                                Os.pause();
                            }

                            if (isOwnerSignal) {
                                dispatcher.signalOwnerProgress(progressState);
                            } else {
                                dispatcher.signalQueueProgress();
                            }
                        });

                        Assert.assertTrue(
                                "progress drain did not finish",
                                drainFinished.await(10, TimeUnit.SECONDS)
                        );
                        drainThread.join();
                        Assert.assertNull(failure.get());
                        Assert.assertTrue(
                                sourceName + " version recheck must prevent parking",
                                progressTask.isDone()
                        );
                        Assert.assertEquals(0, runtime.getParkedFiberCount());
                        Assert.assertEquals(
                                ownerVersion + (isOwnerSignal ? 1 : 0),
                                progressState.getVersion()
                        );
                        Assert.assertEquals(
                                globalVersion + (isOwnerSignal ? 0 : 1),
                                dispatcher.getProgressVersion()
                        );
                    }
                } finally {
                    closeRuntime(runtime);
                    Misc.free(dispatcher);
                }
            }
        });
    }

    @Test
    public void testQuiesceDrainFailsParkedLatestByOwner() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root);
            try (
                    CairoEngine engine = new CairoEngine(configuration);
                    SqlCompiler compiler = engine.getSqlCompiler();
                    SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)
            ) {
                engine.execute(
                        "create table latest_tab as (" +
                                "select x id, ('k' || (x % 64))::symbol sym, " +
                                "timestamp_sequence(0, 1_000_000) ts from long_sequence(4096)" +
                                "), index(sym) timestamp(ts) partition by day",
                        executionContext
                );
                assertOwnerCancelledByQuiesceDrain(
                        engine,
                        compiler,
                        executionContext,
                        "owner-latest-by-quiesce",
                        "select * from latest_tab latest on ts partition by sym",
                        LatestByAllIndexedRecordCursorFactory.class
                );
            }
        });
    }

    @Test
    public void testQuiesceDrainFailsParkedVectorAggregateOwner() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root);
            try (
                    CairoEngine engine = new CairoEngine(configuration);
                    SqlCompiler compiler = engine.getSqlCompiler();
                    SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)
            ) {
                engine.execute(
                        "create table vector_tab as (select (x % 17)::int k, x v from long_sequence(65_536))",
                        executionContext
                );
                assertOwnerCancelledByQuiesceDrain(
                        engine,
                        compiler,
                        executionContext,
                        "owner-vector-quiesce",
                        "select k, sum(v) from vector_tab",
                        io.questdb.griffin.engine.groupby.vect.GroupByRecordCursorFactory.class
                );
            }
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
                    final PostAggregationCircuitBreaker circuitBreaker = new PostAggregationCircuitBreaker(engine);
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

                        final AsyncQueryProgressState ownerProgress = new AsyncQueryProgressState();
                        final AtomicBooleanCircuitBreaker vectorBreaker = new AtomicBooleanCircuitBreaker(engine);
                        final AtomicInteger vectorDoneCounter = new AtomicInteger();
                        final RingQueue<VectorAggregateTask> vectorQueue = messageBus.getVectorAggregateQueue();
                        final MPSequence vectorPubSeq = messageBus.getVectorAggregatePubSeq();
                        publishVectorAggregateTask(
                                vectorQueue,
                                vectorPubSeq,
                                new TestVectorAggregateEntry(vectorBreaker, vectorDoneCounter, 1, null, ownerProgress)
                        );
                        publishVectorAggregateTask(
                                vectorQueue,
                                vectorPubSeq,
                                new TestVectorAggregateEntry(vectorBreaker, vectorDoneCounter, 1, null, ownerProgress)
                        );
                        final long globalProgressBefore = dispatcher.getProgressVersion();
                        final long ownerProgressBefore = ownerProgress.getVersion();

                        dispatcher.beginQuiesce();
                        dispatcher.progressQuiesce();

                        Assert.assertTrue(circuitBreaker.checkIfTripped());
                        Assert.assertEquals(1, startedCounter.get());
                        Assert.assertTrue(doneLatch.done(1));
                        Assert.assertEquals(2, vectorDoneCounter.get());
                        Assert.assertEquals(globalProgressBefore + 3, dispatcher.getProgressVersion());
                        Assert.assertEquals(ownerProgressBefore + 2, ownerProgress.getVersion());
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

    @Test
    public void testVectorAggregateAdapterCreationFailureCompletesClaimedTaskOwnership() throws Exception {
        assertVectorAggregateAdapterCreationFailureCompletesOwnership();
    }

    @Test
    public void testVectorAggregateBatchRebindsOwnerAndCancellation() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
                @Override
                public int getSqlPageFrameMaxRows() {
                    return 128;
                }

                @Override
                public int getVectorAggregateQueueCapacity() {
                    return 4;
                }
            };
            try (CairoEngine engine = new CairoEngine(configuration)) {
                final FiberRuntime runtime = new FiberRuntime(1);
                final QueryParallelFiberDispatcher dispatcher = new QueryParallelFiberDispatcher(
                        engine,
                        engine.getMessageBus(),
                        runtime
                );
                try {
                    final MessageBus messageBus = engine.getMessageBus();
                    final AtomicBooleanCircuitBreaker breakerA = new AtomicBooleanCircuitBreaker(engine);
                    final AtomicBooleanCircuitBreaker breakerB = new AtomicBooleanCircuitBreaker(engine);
                    final FiberCancellationSignal signalA = new FiberCancellationSignal();
                    final FiberCancellationSignal signalB = new FiberCancellationSignal();
                    final long generationA = signalA.getGeneration();
                    final long generationB = signalB.getGeneration();
                    breakerA.setCancelledFlag(signalA);
                    breakerB.setCancelledFlag(signalB);
                    final AsyncQueryProgressState progressA = new AsyncQueryProgressState();
                    final AsyncQueryProgressState progressB = new AsyncQueryProgressState();
                    final AtomicInteger doneA = new AtomicInteger();
                    final AtomicInteger doneB = new AtomicInteger();
                    final TestVectorAggregateEntry entryA = new TestVectorAggregateEntry(
                            breakerA,
                            doneA,
                            1,
                            signalB::reopen,
                            progressA
                    );
                    final TestVectorAggregateEntry entryB = new TestVectorAggregateEntry(
                            breakerB,
                            doneB,
                            1,
                            null,
                            progressB
                    );
                    final MPSequence pubSeq = messageBus.getVectorAggregatePubSeq();
                    final MCSequence subSeq = messageBus.getVectorAggregateSubSeq();
                    final RingQueue<VectorAggregateTask> queue = messageBus.getVectorAggregateQueue();
                    publishVectorAggregateTask(queue, pubSeq, entryA);
                    publishVectorAggregateTask(queue, pubSeq, entryB);

                    final long globalProgressBefore = dispatcher.getProgressVersion();
                    final long ownerAProgressBefore = progressA.getVersion();
                    final long ownerBProgressBefore = progressB.getVersion();
                    Assert.assertFalse(dispatcher.consumeVectorAggregate(-1));
                    Assert.assertEquals(globalProgressBefore + 1, dispatcher.getProgressVersion());
                    Assert.assertEquals(ownerAProgressBefore, progressA.getVersion());
                    Assert.assertEquals(ownerBProgressBefore, progressB.getVersion());
                    Assert.assertEquals(1, runtime.drain(1));

                    Assert.assertEquals(1, doneA.get());
                    Assert.assertEquals(1, doneB.get());
                    Assert.assertSame(signalA, entryA.observedSignal);
                    Assert.assertEquals(generationA, entryA.observedGeneration);
                    Assert.assertSame(signalB, entryB.observedSignal);
                    Assert.assertEquals(generationB, entryB.observedGeneration);
                    Assert.assertEquals(generationB + 1, signalB.getGeneration());
                    Assert.assertTrue(signalB.isCancelled(generationB));
                    Assert.assertEquals(globalProgressBefore + 2, dispatcher.getProgressVersion());
                    Assert.assertEquals(ownerAProgressBefore + 1, progressA.getVersion());
                    Assert.assertEquals(ownerBProgressBefore + 1, progressB.getVersion());
                    Assert.assertEquals(pubSeq.current(), subSeq.current());
                    Assert.assertEquals(1, dispatcher.getVectorAggregateCreatedTaskCount());
                } finally {
                    closeRuntime(runtime);
                    Misc.free(dispatcher);
                }
            }
        });
    }

    @Test
    public void testVectorAggregateBatchStopsAtRowBudget() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
                @Override
                public int getSqlPageFrameMaxRows() {
                    return 10;
                }

                @Override
                public int getVectorAggregateQueueCapacity() {
                    return 4;
                }
            };
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
                    final AsyncQueryProgressState progressState = new AsyncQueryProgressState();
                    final AtomicInteger doneCounter = new AtomicInteger();
                    final MPSequence pubSeq = messageBus.getVectorAggregatePubSeq();
                    final MCSequence subSeq = messageBus.getVectorAggregateSubSeq();
                    final RingQueue<VectorAggregateTask> queue = messageBus.getVectorAggregateQueue();
                    publishVectorAggregateTask(
                            queue,
                            pubSeq,
                            new TestVectorAggregateEntry(circuitBreaker, doneCounter, 10, null, progressState)
                    );
                    publishVectorAggregateTask(
                            queue,
                            pubSeq,
                            new TestVectorAggregateEntry(circuitBreaker, doneCounter, 1, null, progressState)
                    );
                    publishVectorAggregateTask(
                            queue,
                            pubSeq,
                            new TestVectorAggregateEntry(circuitBreaker, doneCounter, 1, null, progressState)
                    );

                    final long globalProgressBefore = dispatcher.getProgressVersion();
                    final long mountCountBefore = runtime.getMountCount();
                    final long ownerProgressBefore = progressState.getVersion();
                    Assert.assertFalse(dispatcher.consumeVectorAggregate(-1));
                    Assert.assertEquals(globalProgressBefore + 1, dispatcher.getProgressVersion());
                    Assert.assertEquals(ownerProgressBefore, progressState.getVersion());
                    Assert.assertEquals(1, runtime.drain(1));
                    Assert.assertEquals(1, doneCounter.get());
                    Assert.assertEquals(globalProgressBefore + 1, dispatcher.getProgressVersion());
                    Assert.assertEquals(mountCountBefore + 1, runtime.getMountCount());
                    Assert.assertEquals(ownerProgressBefore + 1, progressState.getVersion());
                    Assert.assertEquals(pubSeq.current() - 2, subSeq.current());

                    Assert.assertFalse(dispatcher.consumeVectorAggregate(-1));
                    Assert.assertEquals(globalProgressBefore + 2, dispatcher.getProgressVersion());
                    Assert.assertEquals(ownerProgressBefore + 1, progressState.getVersion());
                    Assert.assertEquals(1, runtime.drain(1));
                    Assert.assertEquals(3, doneCounter.get());
                    Assert.assertEquals(globalProgressBefore + 3, dispatcher.getProgressVersion());
                    Assert.assertEquals(mountCountBefore + 2, runtime.getMountCount());
                    Assert.assertEquals(ownerProgressBefore + 3, progressState.getVersion());
                    Assert.assertEquals(pubSeq.current(), subSeq.current());
                    Assert.assertEquals(1, dispatcher.getVectorAggregateCreatedTaskCount());
                } finally {
                    closeRuntime(runtime);
                    Misc.free(dispatcher);
                }
            }
        });
    }

    @Test
    public void testVectorAggregateConsumerBatchesQueuedTasks() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
                @Override
                public int getSqlPageFrameMaxRows() {
                    return 128;
                }

                @Override
                public int getVectorAggregateQueueCapacity() {
                    return 8;
                }
            };
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
                    final AsyncQueryProgressState progressState = new AsyncQueryProgressState();
                    final AtomicInteger doneCounter = new AtomicInteger();
                    final MPSequence pubSeq = messageBus.getVectorAggregatePubSeq();
                    final MCSequence subSeq = messageBus.getVectorAggregateSubSeq();
                    final RingQueue<VectorAggregateTask> queue = messageBus.getVectorAggregateQueue();
                    final int taskCount = 4;
                    for (int i = 0; i < taskCount; i++) {
                        publishVectorAggregateTask(
                                queue,
                                pubSeq,
                                new TestVectorAggregateEntry(circuitBreaker, doneCounter, 1, null, progressState)
                        );
                    }

                    final int createdBefore = dispatcher.getVectorAggregateCreatedTaskCount();
                    final long globalProgressBefore = dispatcher.getProgressVersion();
                    final long mountCountBefore = runtime.getMountCount();
                    final long ownerProgressBefore = progressState.getVersion();
                    Assert.assertFalse(dispatcher.consumeVectorAggregate(-1));
                    Assert.assertEquals(globalProgressBefore + 1, dispatcher.getProgressVersion());
                    Assert.assertEquals(ownerProgressBefore, progressState.getVersion());
                    Assert.assertEquals(1, runtime.drain(1));

                    Assert.assertEquals(taskCount, doneCounter.get());
                    Assert.assertEquals(createdBefore + 1, dispatcher.getVectorAggregateCreatedTaskCount());
                    Assert.assertEquals(globalProgressBefore + taskCount, dispatcher.getProgressVersion());
                    Assert.assertEquals(mountCountBefore + 1, runtime.getMountCount());
                    Assert.assertEquals(ownerProgressBefore + taskCount, progressState.getVersion());
                    Assert.assertEquals(pubSeq.current(), subSeq.current());
                } finally {
                    closeRuntime(runtime);
                    Misc.free(dispatcher);
                }
            }
        });
    }

    @Test
    public void testVectorAggregateFiberOwnerHelpsOwnUnpublishedFrames() throws Exception {
        assertParallelFiberOwnerHelpsOwnWork(DrainTaskType.VECTOR_AGGREGATE);
    }

    @Test
    public void testVectorGroupByOwnerCancelledMidDrainWaitsForCompletion() throws Exception {
        assertParallelOwnerCancelledMidDrain(DrainTaskType.VECTOR_AGGREGATE);
    }

    @Test
    public void testVectorOwnerStealSignalsQueueBeforeDetachedCompletion() throws Exception {
        assertVectorOwnerStealSignalsQueueBeforeDetachedCompletion();
    }

    private static void assertClaimedCursorAcknowledgedAndReusable(
            MPSequence pubSeq,
            MCSequence subSeq,
            long cursor
    ) {
        final long reusedCursor = pubSeq.next();
        long reusedConsumerCursor = -1;
        if (reusedCursor > -1) {
            pubSeq.done(reusedCursor);
            reusedConsumerCursor = subSeq.next();
            if (reusedConsumerCursor > -1) {
                subSeq.done(reusedConsumerCursor);
            }
        } else {
            // Keep branch-mutation REDs cleanup-safe. The dispatcher claimed this cursor, so
            // release it before dispatcher.close() attempts to quiesce the capacity-one queue.
            subSeq.done(cursor);
        }

        Assert.assertEquals("adapter failure must release the capacity-one queue slot", cursor + 1, reusedCursor);
        Assert.assertEquals("consumer cursor must remain reusable", reusedCursor, reusedConsumerCursor);
    }

    private static void assertCountedAdapterCreationFailureCompletesOwnership(boolean isLongTopK) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
                @Override
                public int getGroupByMergeShardQueueCapacity() {
                    return 1;
                }

                @Override
                public int getGroupByTopKQueueCapacity() {
                    return 1;
                }
            };
            try (CairoEngine engine = new CairoEngine(configuration);
                 SqlCompiler compiler = engine.getSqlCompiler();
                 SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine, 2)) {
                final String sql = "select key, max(v) from adapter_failure_group_by";
                engine.execute(
                        "create table adapter_failure_group_by as "
                                + "(select ('k' || x) key, x v from long_sequence(2))",
                        executionContext
                );
                try (RecordCursorFactory factory = compiler.compile(sql, executionContext).getRecordCursorFactory()) {
                    final AsyncGroupByAtom atom = (AsyncGroupByAtom) TestUtils.findAtom(factory, sql);
                    final AsyncQueryProgressState progressState = atom.getShardingContext().getProgressState();
                    final FiberRuntime runtime = new FiberRuntime(1);
                    final QueryParallelFiberDispatcher dispatcher = new QueryParallelFiberDispatcher(
                            engine,
                            engine.getMessageBus(),
                            runtime
                    );
                    try {
                        final MessageBus messageBus = engine.getMessageBus();
                        final PostAggregationCircuitBreaker circuitBreaker = new PostAggregationCircuitBreaker(engine);
                        final AtomicInteger startedCounter = new AtomicInteger();
                        final AtomicInteger doneCounter = new AtomicInteger();
                        final RuntimeException cleanupFailure = isLongTopK
                                ? null
                                : new RuntimeException("injected cleanup failure");
                        final CountDownLatchSPI doneLatch = () -> {
                            doneCounter.incrementAndGet();
                            if (cleanupFailure != null) {
                                throw cleanupFailure;
                            }
                        };
                        final MPSequence pubSeq;
                        final MCSequence subSeq;
                        GroupByLongTopKTask longTopKTask = null;
                        GroupByMergeShardTask mergeShardTask = null;
                        final long cursor;
                        if (isLongTopK) {
                            pubSeq = messageBus.getGroupByLongTopKPubSeq();
                            subSeq = messageBus.getGroupByLongTopKSubSeq();
                            cursor = pubSeq.next();
                            Assert.assertTrue("long top-K queue slot must be available", cursor > -1);
                            longTopKTask = messageBus.getGroupByLongTopKQueue().get(cursor);
                            longTopKTask.of(
                                    circuitBreaker,
                                    startedCounter,
                                    doneLatch,
                                    atom,
                                    null,
                                    0,
                                    0,
                                    1
                            );
                        } else {
                            pubSeq = messageBus.getGroupByMergeShardPubSeq();
                            subSeq = messageBus.getGroupByMergeShardSubSeq();
                            cursor = pubSeq.next();
                            Assert.assertTrue("merge-shard queue slot must be available", cursor > -1);
                            mergeShardTask = messageBus.getGroupByMergeShardQueue().get(cursor);
                            mergeShardTask.of(
                                    circuitBreaker,
                                    startedCounter,
                                    doneLatch,
                                    atom.getShardingContext(),
                                    0
                            );
                        }
                        pubSeq.done(cursor);

                        final long globalProgressBefore = dispatcher.getProgressVersion();
                        final long queryProgressBefore = progressState.getVersion();
                        final RuntimeException injected = new RuntimeException("injected adapter creation failure");
                        if (isLongTopK) {
                            setBeforeTaskCreationForTesting(dispatcher, "longTopKTaskPool", () -> {
                                throw injected;
                            });
                        } else {
                            setBeforeTaskCreationForTesting(dispatcher, "mergeShardTaskPool", () -> {
                                throw injected;
                            });
                        }

                        final RuntimeException thrown = Assert.assertThrows(
                                RuntimeException.class,
                                isLongTopK
                                        ? () -> dispatcher.consumeLongTopK(-1)
                                        : () -> dispatcher.consumeMergeShard(-1)
                        );
                        final boolean isCancelled = circuitBreaker.checkIfTripped();
                        final int startedCount = startedCounter.get();
                        final int doneCount = doneCounter.get();
                        final long globalProgressAfter = dispatcher.getProgressVersion();
                        final long queryProgressAfter = progressState.getVersion();
                        final boolean isTaskCleared = isLongTopK
                                ? longTopKTask.getAtom() == null
                                  && longTopKTask.getShardIndex() == -1
                                : mergeShardTask.getShardingContext() == null
                                  && mergeShardTask.getShardIndex() == -1;

                        if (!isTaskCleared) {
                            if (isLongTopK) {
                                longTopKTask.clear();
                            } else {
                                mergeShardTask.clear();
                            }
                        }
                        if (!isCancelled) {
                            circuitBreaker.cancel();
                        }
                        if (startedCount == 0) {
                            startedCounter.incrementAndGet();
                        }
                        if (doneCount == 0) {
                            try {
                                doneLatch.countDown();
                            } catch (Throwable ignored) {
                                // The merge-shard case deliberately throws after recording countDown().
                            }
                        }
                        assertClaimedCursorAcknowledgedAndReusable(pubSeq, subSeq, cursor);
                        assertFiberAndLeaseReleased(dispatcher, runtime);

                        final String family = isLongTopK ? "long top-K" : "merge-shard";
                        Assert.assertSame(family + " must preserve the acquisition failure", injected, thrown);
                        if (cleanupFailure == null) {
                            Assert.assertEquals(0, thrown.getSuppressed().length);
                        } else {
                            Assert.assertEquals(1, thrown.getSuppressed().length);
                            Assert.assertSame(cleanupFailure, thrown.getSuppressed()[0]);
                        }
                        Assert.assertTrue(family + " must cancel the shared breaker", isCancelled);
                        Assert.assertEquals(family + " must count the task as started", 1, startedCount);
                        Assert.assertEquals(family + " must count down its completion latch", 1, doneCount);
                        Assert.assertTrue(family + " must clear the queue task", isTaskCleared);
                        Assert.assertEquals(
                                family + " must signal queue progress once",
                                globalProgressBefore + 1,
                                globalProgressAfter
                        );
                        Assert.assertEquals(
                                family + " must signal real per-query progress once",
                                queryProgressBefore + 1,
                                queryProgressAfter
                        );
                    } finally {
                        Misc.free(dispatcher);
                        closeRuntime(runtime);
                    }
                }
            }
        });
    }

    private static void assertCountedConsumerBatchesQueuedTasks(boolean isLongTopK) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
                @Override
                public int getGroupByMergeShardQueueCapacity() {
                    return 8;
                }

                @Override
                public int getGroupByTopKQueueCapacity() {
                    return 8;
                }
            };
            try (CairoEngine engine = new CairoEngine(configuration);
                 SqlCompiler compiler = engine.getSqlCompiler();
                 SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine, 2)) {
                final String sql = "SELECT key, max(v) FROM batch_group_by";
                engine.execute(
                        "CREATE TABLE batch_group_by AS "
                                + "(SELECT ('k' || x) key, x v FROM long_sequence(2))",
                        executionContext
                );
                try (RecordCursorFactory factoryA = compiler.compile(sql, executionContext).getRecordCursorFactory();
                     RecordCursorFactory factoryB = compiler.compile(sql, executionContext).getRecordCursorFactory()) {
                    final AsyncGroupByAtom atomA = (AsyncGroupByAtom) TestUtils.findAtom(factoryA, sql);
                    final AsyncGroupByAtom atomB = (AsyncGroupByAtom) TestUtils.findAtom(factoryB, sql);
                    final AsyncQueryProgressState progressA = atomA.getShardingContext().getProgressState();
                    final AsyncQueryProgressState progressB = atomB.getShardingContext().getProgressState();
                    Assert.assertNotSame(atomA, atomB);
                    Assert.assertNotSame(progressA, progressB);
                    final FiberRuntime runtime = new FiberRuntime(1);
                    final QueryParallelFiberDispatcher dispatcher = new QueryParallelFiberDispatcher(
                            engine,
                            engine.getMessageBus(),
                            runtime
                    );
                    try {
                        final MessageBus messageBus = engine.getMessageBus();
                        final FiberCancellationSignal signalA = new FiberCancellationSignal();
                        final FiberCancellationSignal signalB = new FiberCancellationSignal();
                        final long generationA = signalA.getGeneration();
                        final long generationB = signalB.getGeneration();
                        final TestScopeCircuitBreaker breakerA = new TestScopeCircuitBreaker(engine, signalB::reopen);
                        final TestScopeCircuitBreaker breakerB = new TestScopeCircuitBreaker(engine, null);
                        breakerA.setCancelledFlag(signalA);
                        breakerB.setCancelledFlag(signalB);
                        final AtomicInteger doneCounter = new AtomicInteger();
                        final AtomicInteger startedCounter = new AtomicInteger();
                        final CountDownLatchSPI doneLatch = doneCounter::incrementAndGet;
                        final MPSequence pubSeq;
                        final MCSequence subSeq;
                        final int taskCount = 4;
                        if (isLongTopK) {
                            pubSeq = messageBus.getGroupByLongTopKPubSeq();
                            subSeq = messageBus.getGroupByLongTopKSubSeq();
                            final RingQueue<GroupByLongTopKTask> queue = messageBus.getGroupByLongTopKQueue();
                            for (int i = 0; i < taskCount; i++) {
                                final long cursor = pubSeq.next();
                                Assert.assertTrue("long top-K queue slot must be available", cursor > -1);
                                final AsyncGroupByAtom atom = i == 0 ? atomA : atomB;
                                final TestScopeCircuitBreaker circuitBreaker = i == 0 ? breakerA : breakerB;
                                queue.get(cursor).of(
                                        circuitBreaker,
                                        startedCounter,
                                        doneLatch,
                                        atom,
                                        null,
                                        0,
                                        0,
                                        1
                                );
                                pubSeq.done(cursor);
                            }
                        } else {
                            pubSeq = messageBus.getGroupByMergeShardPubSeq();
                            subSeq = messageBus.getGroupByMergeShardSubSeq();
                            final RingQueue<GroupByMergeShardTask> queue = messageBus.getGroupByMergeShardQueue();
                            for (int i = 0; i < taskCount; i++) {
                                final long cursor = pubSeq.next();
                                Assert.assertTrue("merge-shard queue slot must be available", cursor > -1);
                                final AsyncGroupByAtom atom = i == 0 ? atomA : atomB;
                                final TestScopeCircuitBreaker circuitBreaker = i == 0 ? breakerA : breakerB;
                                queue.get(cursor).of(
                                        circuitBreaker,
                                        startedCounter,
                                        doneLatch,
                                        atom.getShardingContext(),
                                        0
                                );
                                pubSeq.done(cursor);
                            }
                        }

                        final int createdBefore = isLongTopK
                                ? dispatcher.getLongTopKCreatedTaskCount()
                                : dispatcher.getMergeShardCreatedTaskCount();
                        final long globalProgressBefore = dispatcher.getProgressVersion();
                        final long mountCountBefore = runtime.getMountCount();
                        final long ownerAProgressBefore = progressA.getVersion();
                        final long ownerBProgressBefore = progressB.getVersion();
                        Assert.assertFalse(isLongTopK
                                ? dispatcher.consumeLongTopK(-1)
                                : dispatcher.consumeMergeShard(-1));
                        Assert.assertEquals(globalProgressBefore + 1, dispatcher.getProgressVersion());
                        Assert.assertEquals(ownerAProgressBefore, progressA.getVersion());
                        Assert.assertEquals(ownerBProgressBefore, progressB.getVersion());
                        Assert.assertEquals(1, runtime.drain(1));

                        final String family = isLongTopK ? "long top-K" : "merge-shard";
                        Assert.assertEquals(family + " must complete every entry", taskCount, doneCounter.get());
                        Assert.assertEquals(family + " must start every entry", taskCount, startedCounter.get());
                        Assert.assertEquals(
                                family + " must create one adapter for the batch",
                                createdBefore + 1,
                                isLongTopK
                                        ? dispatcher.getLongTopKCreatedTaskCount()
                                        : dispatcher.getMergeShardCreatedTaskCount()
                        );
                        Assert.assertEquals(
                                family + " must signal queue progress once per entry",
                                globalProgressBefore + taskCount,
                                dispatcher.getProgressVersion()
                        );
                        Assert.assertEquals(
                                family + " must mount one adapter for the batch",
                                mountCountBefore + 1,
                                runtime.getMountCount()
                        );
                        Assert.assertEquals(
                                family + " must signal owner A progress once",
                                ownerAProgressBefore + 1,
                                progressA.getVersion()
                        );
                        Assert.assertEquals(
                                family + " must signal owner B progress once per entry",
                                ownerBProgressBefore + 3,
                                progressB.getVersion()
                        );
                        Assert.assertSame(signalA, breakerA.observedSignal);
                        Assert.assertEquals(generationA, breakerA.observedGeneration);
                        Assert.assertSame(signalB, breakerB.observedSignal);
                        Assert.assertEquals(generationB, breakerB.observedGeneration);
                        Assert.assertEquals(generationB + 1, signalB.getGeneration());
                        Assert.assertTrue(signalB.isCancelled(generationB));
                        Assert.assertEquals(pubSeq.current(), subSeq.current());
                    } finally {
                        closeRuntime(runtime);
                        Misc.free(dispatcher);
                    }
                }
            }
        });
    }

    private static void assertCountedOwnerStealSignalsBothSourcesWhenTerminalCleanupThrows(
            boolean isLongTopK
    ) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
                @Override
                public int getGroupByMergeShardQueueCapacity() {
                    return 1;
                }

                @Override
                public int getGroupByTopKQueueCapacity() {
                    return 1;
                }
            };
            try (CairoEngine engine = new CairoEngine(configuration);
                 SqlCompiler compiler = engine.getSqlCompiler();
                 SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine, 2)) {
                final String sql = "SELECT key, max(v) FROM owner_steal_cleanup";
                engine.execute(
                        "CREATE TABLE owner_steal_cleanup AS "
                                + "(SELECT ('k' || x) key, x v FROM long_sequence(2))",
                        executionContext
                );
                try (RecordCursorFactory factory = compiler.compile(sql, executionContext).getRecordCursorFactory()) {
                    final AsyncGroupByAtom atom = (AsyncGroupByAtom) TestUtils.findAtom(factory, sql);
                    final AsyncQueryProgressState progressState = atom.getShardingContext().getProgressState();
                    final FiberRuntime runtime = new FiberRuntime(1);
                    final QueryParallelFiberDispatcher dispatcher = new QueryParallelFiberDispatcher(
                            engine,
                            engine.getMessageBus(),
                            runtime
                    );
                    try {
                        final MessageBus messageBus = engine.getMessageBus();
                        final PostAggregationCircuitBreaker circuitBreaker = new PostAggregationCircuitBreaker(engine);
                        circuitBreaker.cancel();
                        final AtomicInteger doneCounter = new AtomicInteger();
                        final AtomicInteger startedCounter = new AtomicInteger();
                        final RuntimeException cleanupFailure = new RuntimeException("injected terminal cleanup failure");
                        final CountDownLatchSPI doneLatch = () -> {
                            doneCounter.incrementAndGet();
                            throw cleanupFailure;
                        };
                        final MPSequence pubSeq;
                        final MCSequence subSeq;
                        final long cursor;
                        if (isLongTopK) {
                            pubSeq = messageBus.getGroupByLongTopKPubSeq();
                            subSeq = messageBus.getGroupByLongTopKSubSeq();
                            cursor = pubSeq.next();
                            Assert.assertTrue("long top-K queue slot must be available", cursor > -1);
                            messageBus.getGroupByLongTopKQueue().get(cursor).of(
                                    circuitBreaker,
                                    startedCounter,
                                    doneLatch,
                                    atom,
                                    null,
                                    0,
                                    0,
                                    1
                            );
                        } else {
                            pubSeq = messageBus.getGroupByMergeShardPubSeq();
                            subSeq = messageBus.getGroupByMergeShardSubSeq();
                            cursor = pubSeq.next();
                            Assert.assertTrue("merge-shard queue slot must be available", cursor > -1);
                            messageBus.getGroupByMergeShardQueue().get(cursor).of(
                                    circuitBreaker,
                                    startedCounter,
                                    doneLatch,
                                    atom.getShardingContext(),
                                    0
                            );
                        }
                        pubSeq.done(cursor);
                        Assert.assertEquals(cursor, subSeq.next());

                        final long globalProgressBefore = dispatcher.getProgressVersion();
                        final long ownerProgressBefore = progressState.getVersion();
                        final RuntimeException thrown;
                        if (isLongTopK) {
                            final GroupByLongTopKTask task = messageBus.getGroupByLongTopKQueue().get(cursor);
                            thrown = Assert.assertThrows(
                                    RuntimeException.class,
                                    () -> GroupByLongTopKJob.run(-1, task, subSeq, cursor, atom, dispatcher)
                            );
                        } else {
                            final GroupByMergeShardTask task = messageBus.getGroupByMergeShardQueue().get(cursor);
                            thrown = Assert.assertThrows(
                                    RuntimeException.class,
                                    () -> GroupByMergeShardJob.run(
                                            -1,
                                            task,
                                            subSeq,
                                            cursor,
                                            atom.getShardingContext(),
                                            dispatcher
                                    )
                            );
                        }

                        assertClaimedCursorAcknowledgedAndReusable(pubSeq, subSeq, cursor);
                        final String family = isLongTopK ? "long top-K" : "merge-shard";
                        Assert.assertSame(family + " must preserve the terminal cleanup failure", cleanupFailure, thrown);
                        Assert.assertEquals(0, thrown.getSuppressed().length);
                        Assert.assertEquals(family + " must count the task as started", 1, startedCounter.get());
                        Assert.assertEquals(family + " must publish terminal completion once", 1, doneCounter.get());
                        Assert.assertEquals(
                                family + " must signal queue progress once",
                                globalProgressBefore + 1,
                                dispatcher.getProgressVersion()
                        );
                        Assert.assertEquals(
                                family + " must signal owner progress once",
                                ownerProgressBefore + 1,
                                progressState.getVersion()
                        );
                    } finally {
                        Misc.free(dispatcher);
                        closeRuntime(runtime);
                    }
                }
            }
        });
    }

    private static void assertCountedOwnerStealSignalsQueueBeforeTerminalCompletion(boolean isLongTopK)
            throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
                @Override
                public int getGroupByMergeShardQueueCapacity() {
                    return 1;
                }

                @Override
                public int getGroupByTopKQueueCapacity() {
                    return 1;
                }
            };
            try (CairoEngine engine = new CairoEngine(configuration);
                 SqlCompiler compiler = engine.getSqlCompiler();
                 SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine, 2)) {
                final String sql = "SELECT key, max(v) FROM owner_steal_phase";
                engine.execute(
                        "CREATE TABLE owner_steal_phase AS "
                                + "(SELECT ('k' || x) key, x v FROM long_sequence(2))",
                        executionContext
                );
                try (RecordCursorFactory factory = compiler.compile(sql, executionContext).getRecordCursorFactory()) {
                    final AsyncGroupByAtom atom = (AsyncGroupByAtom) TestUtils.findAtom(factory, sql);
                    final AsyncQueryProgressState progressState = atom.getShardingContext().getProgressState();
                    final FiberRuntime runtime = new FiberRuntime(1);
                    final QueryParallelFiberDispatcher dispatcher = new QueryParallelFiberDispatcher(
                            engine,
                            engine.getMessageBus(),
                            runtime
                    );
                    try {
                        final MessageBus messageBus = engine.getMessageBus();
                        final PostAggregationCircuitBreaker circuitBreaker = new PostAggregationCircuitBreaker(engine);
                        circuitBreaker.cancel();
                        final AtomicInteger doneCounter = new AtomicInteger();
                        final AtomicInteger startedCounter = new AtomicInteger();
                        final CountDownLatch terminalEntered = new CountDownLatch(1);
                        final CountDownLatch terminalRelease = new CountDownLatch(1);
                        final CountDownLatchSPI doneLatch = () -> {
                            terminalEntered.countDown();
                            try {
                                if (!terminalRelease.await(10, TimeUnit.SECONDS)) {
                                    throw new AssertionError("timed out waiting to release terminal completion");
                                }
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new AssertionError(e);
                            }
                            doneCounter.incrementAndGet();
                        };
                        final MPSequence pubSeq;
                        final MCSequence subSeq;
                        final long cursor;
                        final AtomicReference<Throwable> taskFailure = new AtomicReference<>();
                        final Thread taskThread;
                        if (isLongTopK) {
                            pubSeq = messageBus.getGroupByLongTopKPubSeq();
                            subSeq = messageBus.getGroupByLongTopKSubSeq();
                            cursor = pubSeq.next();
                            Assert.assertTrue("long top-K queue slot must be available", cursor > -1);
                            final GroupByLongTopKTask task = messageBus.getGroupByLongTopKQueue().get(cursor);
                            task.of(circuitBreaker, startedCounter, doneLatch, atom, null, 0, 0, 1);
                            taskThread = new Thread(() -> {
                                try {
                                    GroupByLongTopKJob.run(-1, task, subSeq, cursor, atom, dispatcher);
                                } catch (Throwable th) {
                                    taskFailure.set(th);
                                }
                            }, "long-top-k-owner-steal");
                        } else {
                            pubSeq = messageBus.getGroupByMergeShardPubSeq();
                            subSeq = messageBus.getGroupByMergeShardSubSeq();
                            cursor = pubSeq.next();
                            Assert.assertTrue("merge-shard queue slot must be available", cursor > -1);
                            final GroupByMergeShardTask task = messageBus.getGroupByMergeShardQueue().get(cursor);
                            task.of(circuitBreaker, startedCounter, doneLatch, atom.getShardingContext(), 0);
                            taskThread = new Thread(() -> {
                                try {
                                    GroupByMergeShardJob.run(
                                            -1,
                                            task,
                                            subSeq,
                                            cursor,
                                            atom.getShardingContext(),
                                            dispatcher
                                    );
                                } catch (Throwable th) {
                                    taskFailure.set(th);
                                }
                            }, "merge-shard-owner-steal");
                        }
                        pubSeq.done(cursor);
                        Assert.assertEquals(cursor, subSeq.next());

                        final long globalProgressBefore = dispatcher.getProgressVersion();
                        final long ownerProgressBefore = progressState.getVersion();
                        taskThread.setDaemon(true);
                        taskThread.start();
                        try {
                            Assert.assertTrue(
                                    "owner-steal task did not enter terminal completion",
                                    terminalEntered.await(10, TimeUnit.SECONDS)
                            );
                            assertClaimedCursorAcknowledgedAndReusable(pubSeq, subSeq, cursor);
                            Assert.assertEquals(globalProgressBefore + 1, dispatcher.getProgressVersion());
                            Assert.assertEquals(ownerProgressBefore, progressState.getVersion());
                            Assert.assertEquals(0, doneCounter.get());
                        } finally {
                            terminalRelease.countDown();
                            taskThread.join(TimeUnit.SECONDS.toMillis(10));
                        }

                        final String family = isLongTopK ? "long top-K" : "merge-shard";
                        Assert.assertFalse(family + " owner-steal task did not terminate", taskThread.isAlive());
                        Assert.assertNull(taskFailure.get());
                        Assert.assertEquals(family + " must count the task as started", 1, startedCounter.get());
                        Assert.assertEquals(family + " must publish terminal completion once", 1, doneCounter.get());
                        Assert.assertEquals(globalProgressBefore + 1, dispatcher.getProgressVersion());
                        Assert.assertEquals(ownerProgressBefore + 1, progressState.getVersion());
                    } finally {
                        Misc.free(dispatcher);
                        closeRuntime(runtime);
                    }
                }
            }
        });
    }

    private static void assertFiberAndLeaseReleased(
            QueryParallelFiberDispatcher dispatcher,
            FiberRuntime runtime
    ) {
        Assert.assertEquals("adapter failure must not launch a fiber task", 0, runtime.getOutstandingTaskCount());
        final Fiber fiber = runtime.tryReserveFiber();
        Assert.assertNotNull("adapter failure must release its fiber reservation", fiber);
        runtime.releaseReservedFiber(fiber, fiber.getReservationEpoch());

        dispatcher.beginQuiesce();
        dispatcher.progressQuiesce();
        Assert.assertTrue("adapter failure must release its task-pool lease", dispatcher.isQuiesced());
    }

    private static void assertFiberOwnerParks(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext executionContext
    ) throws Exception {
        final FiberRuntime runtime = runAsFiberOwner(
                engine,
                "query-owner-fiber-test",
                () -> Assert.assertEquals(17, drain(compiler, executionContext, "select k, sum(v) from vector_tab"))
        );
        Assert.assertTrue(runtime.getMountCount() > 1);
    }

    private static void assertForeignGroupByStealWakesParkedOwner(boolean longTopK) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
                @Override
                public int getGroupByMergeShardQueueCapacity() {
                    return 2;
                }

                @Override
                public long getGroupByParallelTopKThreshold() {
                    return 0;
                }

                @Override
                public int getGroupByShardingThreshold() {
                    return 1;
                }

                @Override
                public int getGroupByTopKQueueCapacity() {
                    return 2;
                }

                @Override
                public long getQueryContinuationWakeIntervalMillis() {
                    return TimeUnit.HOURS.toMillis(1);
                }

                @Override
                public int getSqlPageFrameMaxRows() {
                    return 128;
                }

                @Override
                public int getSqlPageFrameMinRows() {
                    return 1;
                }
            };
            try (CairoEngine engine = new CairoEngine(configuration);
                 SqlCompiler compiler = engine.getSqlCompiler();
                 SqlExecutionContext victimContext = TestUtils.createSqlExecutionCtx(engine, 2);
                 SqlExecutionContext foreignContext = TestUtils.createSqlExecutionCtx(engine, 2);
                 TestWorkerPool ownerPool = new TestWorkerPool(
                         longTopK ? "foreign-long-top-k-victim" : "foreign-merge-shard-victim",
                         1,
                         Metrics.DISABLED,
                         WorkerPoolMode.FIBER_HOST
                 )) {
                final FiberRuntime dispatcherRuntime = new FiberRuntime(1);
                final QueryParallelFiberDispatcher dispatcher = new QueryParallelFiberDispatcher(
                        engine,
                        engine.getMessageBus(),
                        dispatcherRuntime
                );
                engine.getMessageBus().setQueryParallelFiberDispatcher(dispatcher);
                engine.execute(
                        "create table group_by_foreign as "
                                + "(select ('k' || x) key, x v from long_sequence(256))",
                        foreignContext
                );
                final String sql = longTopK
                        ? "select key, max(v) m from group_by_foreign order by m desc limit 10"
                        : "select key, max(v) m from group_by_foreign";
                try (RecordCursorFactory victimFactory = compiler.compile(sql, victimContext).getRecordCursorFactory()) {
                    TestUtils.assertFactoryInTree(victimFactory, AsyncGroupByRecordCursorFactory.class);
                    if (longTopK) {
                        TestUtils.assertFactoryInTree(victimFactory, LongTopKRecordCursorFactory.class);
                    }
                    final AsyncGroupByAtom victimAtom = (AsyncGroupByAtom) TestUtils.findAtom(victimFactory, sql);
                    final AsyncQueryProgressState victimProgress = victimAtom.getShardingContext().getProgressState();
                    final PostAggregationCircuitBreaker taskBreaker = new PostAggregationCircuitBreaker(engine);
                    final AtomicInteger taskStarted = new AtomicInteger();
                    final AtomicInteger taskDone = new AtomicInteger();
                    taskBreaker.cancel();

                    final MessageBus messageBus = engine.getMessageBus();
                    final MCSequence targetSubSeq;
                    final MPSequence targetPubSeq;
                    if (longTopK) {
                        targetSubSeq = messageBus.getGroupByLongTopKSubSeq();
                        targetPubSeq = messageBus.getGroupByLongTopKPubSeq();
                        final long cursor = targetPubSeq.next();
                        Assert.assertTrue("long top-K queue slot must be available", cursor > -1);
                        messageBus.getGroupByLongTopKQueue().get(cursor).of(
                                taskBreaker,
                                taskStarted,
                                taskDone::incrementAndGet,
                                victimAtom,
                                null,
                                0,
                                0,
                                1
                        );
                        targetPubSeq.done(cursor);
                    } else {
                        targetSubSeq = messageBus.getGroupByMergeShardSubSeq();
                        targetPubSeq = messageBus.getGroupByMergeShardPubSeq();
                        final long cursor = targetPubSeq.next();
                        Assert.assertTrue("merge-shard queue slot must be available", cursor > -1);
                        messageBus.getGroupByMergeShardQueue().get(cursor).of(
                                taskBreaker,
                                taskStarted,
                                taskDone::incrementAndGet,
                                victimAtom.getShardingContext(),
                                0
                        );
                        targetPubSeq.done(cursor);
                    }

                    final FiberRuntime ownerRuntime = ownerPool.getFiberRuntime();
                    final AtomicReference<Throwable> ownerFailure = new AtomicReference<>();
                    final CountDownLatch victimDone = new CountDownLatch(1);
                    final AtomicBoolean launched = new AtomicBoolean();
                    final FiberTask victimTask = new FiberTask() {
                        @Override
                        protected void onDone() {
                            victimDone.countDown();
                        }

                        @Override
                        protected void onError(Throwable th) {
                            ownerFailure.compareAndSet(null, th);
                            victimDone.countDown();
                        }

                        @Override
                        protected boolean runStep() {
                            SuspensionScope.enterTimerShards(engine.getTimerShards());
                            while (victimProgress.getVersion() == 0) {
                                dispatcher.awaitProgress(
                                        victimProgress,
                                        victimProgress.getVersion(),
                                        dispatcher.getProgressVersion(),
                                        SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER
                                );
                            }
                            return true;
                        }
                    };
                    ownerPool.assign(_ -> {
                        if (launched.compareAndSet(false, true)) {
                            final LaunchResult result = ownerRuntime.launch(victimTask);
                            if (result != LaunchResult.LAUNCHED) {
                                ownerFailure.compareAndSet(
                                        null,
                                        new AssertionError("victim launch failed [result=" + result + ']')
                                );
                                victimDone.countDown();
                            }
                            return true;
                        }
                        return false;
                    });

                    boolean ownerPoolStarted = false;
                    try {
                        ownerPool.start(LOG);
                        ownerPoolStarted = true;
                        awaitParkedCount(ownerRuntime, 1, ownerFailure);

                        Assert.assertEquals(longTopK ? 10 : 256, drain(compiler, foreignContext, sql));
                        Assert.assertTrue(
                                longTopK
                                        ? "foreign long top-K steal did not signal the victim"
                                        : "foreign merge-shard steal did not signal the victim",
                                victimProgress.getVersion() > 0
                        );
                        Assert.assertTrue(
                                "victim did not resume before the continuation timer",
                                victimDone.await(10, TimeUnit.SECONDS)
                        );
                        Assert.assertNull(ownerFailure.get());
                        Assert.assertEquals(1, taskStarted.get());
                        Assert.assertEquals(1, taskDone.get());
                        awaitParkedCount(ownerRuntime, 0, ownerFailure);
                        Assert.assertEquals(targetPubSeq.current(), targetSubSeq.current());
                        Assert.assertEquals(
                                0,
                                longTopK
                                        ? dispatcher.getLongTopKCreatedTaskCount()
                                        : dispatcher.getMergeShardCreatedTaskCount()
                        );
                        Assert.assertEquals(0, dispatcherRuntime.getOutstandingTaskCount());
                    } finally {
                        if (victimDone.getCount() > 0) {
                            dispatcher.signalOwnerProgress(victimProgress);
                            victimDone.await(10, TimeUnit.SECONDS);
                        }
                        dispatcher.beginQuiesce();
                        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                        while (!dispatcher.isQuiesced() && System.nanoTime() < deadline) {
                            dispatcher.progressQuiesce();
                            Os.pause();
                        }
                        Assert.assertTrue(dispatcher.isQuiesced());
                        if (ownerPoolStarted) {
                            ownerPool.haltAndAssertCleanForTest(WorkerPool.DEFAULT_HALT_TIMEOUT_NANOS);
                        }
                        engine.getMessageBus().setQueryParallelFiberDispatcher(null);
                        Misc.free(dispatcher);
                        closeRuntime(dispatcherRuntime);
                    }
                }
            }
        });
    }

    private static void assertForeignLatestByStealWakesParkedOwner(boolean cleanupPath) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
                @Override
                public int getLatestByQueueCapacity() {
                    return 2;
                }

                @Override
                public long getQueryContinuationWakeIntervalMillis() {
                    return TimeUnit.HOURS.toMillis(1);
                }
            };
            try (CairoEngine engine = new CairoEngine(configuration);
                 SqlCompiler compiler = engine.getSqlCompiler();
                 SqlExecutionContext setupContext = TestUtils.createSqlExecutionCtx(engine, 1);
                 SqlExecutionContext victimContext = TestUtils.createSqlExecutionCtx(engine, 1);
                 TestWorkerPool ownerPool = new TestWorkerPool(
                         cleanupPath ? "foreign-latest-by-cleanup-victim" : "foreign-latest-by-main-victim",
                         1,
                         Metrics.DISABLED,
                         WorkerPoolMode.FIBER_HOST
                 )) {
                final AtomicBooleanCircuitBreaker foreignBreaker = new AtomicBooleanCircuitBreaker(engine) {
                    @Override
                    public void statefulThrowExceptionIfTrippedTimeThrottled() {
                        if (cleanupPath) {
                            cancel();
                        }
                        statefulThrowExceptionIfTrippedNoThrottle();
                    }
                };
                try (SqlExecutionContext foreignContext = TestUtils.createSqlExecutionCtx(engine, 1).with(foreignBreaker)) {
                    final FiberRuntime dispatcherRuntime = new FiberRuntime(1);
                    final QueryParallelFiberDispatcher dispatcher = new QueryParallelFiberDispatcher(
                            engine,
                            engine.getMessageBus(),
                            dispatcherRuntime
                    );
                    engine.getMessageBus().setQueryParallelFiberDispatcher(dispatcher);
                    final String sql = "select * from latest_foreign latest on ts partition by sym";
                    engine.execute(
                            "create table latest_foreign as ("
                                    + "select (x * 1_000_000L)::timestamp ts, "
                                    + "('k' || (x % 64))::symbol sym, x value "
                                    + "from long_sequence(512)"
                                    + "), index(sym) timestamp(ts)",
                            setupContext
                    );
                    try (RecordCursorFactory factory = compiler.compile(sql, setupContext).getRecordCursorFactory()) {
                        TestUtils.assertFactoryInTree(factory, LatestByAllIndexedRecordCursorFactory.class);
                    }

                    final FiberRuntime ownerRuntime = ownerPool.getFiberRuntime();
                    final AtomicReference<Throwable> ownerFailure = new AtomicReference<>();
                    final AtomicInteger victimRows = new AtomicInteger();
                    final CountDownLatch victimDone = new CountDownLatch(1);
                    final AtomicBoolean launched = new AtomicBoolean();
                    final FiberTask victimTask = new FiberTask() {
                        @Override
                        protected void onDone() {
                            victimDone.countDown();
                        }

                        @Override
                        protected void onError(Throwable th) {
                            ownerFailure.compareAndSet(null, th);
                            victimDone.countDown();
                        }

                        @Override
                        protected boolean runStep() {
                            SuspensionScope.enterTimerShards(engine.getTimerShards());
                            try {
                                victimRows.set((int) drain(compiler, victimContext, sql));
                            } catch (Exception e) {
                                throw new AssertionError(e);
                            }
                            return true;
                        }
                    };
                    ownerPool.assign(_ -> {
                        if (launched.compareAndSet(false, true)) {
                            final LaunchResult result = ownerRuntime.launch(victimTask);
                            if (result != LaunchResult.LAUNCHED) {
                                ownerFailure.compareAndSet(
                                        null,
                                        new AssertionError("victim launch failed [result=" + result + ']')
                                );
                                victimDone.countDown();
                            }
                            return true;
                        }
                        return false;
                    });

                    AsyncQueryProgressState victimProgress = null;
                    boolean ownerPoolStarted = false;
                    try {
                        final MessageBus messageBus = engine.getMessageBus();
                        final MPSequence pubSeq = messageBus.getLatestByPubSeq();
                        final MCSequence subSeq = messageBus.getLatestBySubSeq();
                        final long cursorBefore = pubSeq.current();

                        ownerPool.start(LOG);
                        ownerPoolStarted = true;
                        awaitParkedCount(ownerRuntime, 1, ownerFailure);
                        final long victimCursor = pubSeq.current();
                        Assert.assertEquals(cursorBefore + 1, victimCursor);
                        Assert.assertEquals(cursorBefore, subSeq.current());
                        victimProgress = messageBus.getLatestByQueue().get(victimCursor).getProgressState();
                        Assert.assertNotNull(victimProgress);
                        final long observedVersion = victimProgress.getVersion();

                        if (cleanupPath) {
                            try {
                                drain(compiler, foreignContext, sql);
                                Assert.fail("expected foreign latest-by cancellation");
                            } catch (CairoException e) {
                                Assert.assertTrue(e.isInterruption());
                                Assert.assertEquals(
                                        SqlExecutionCircuitBreaker.STATE_CANCELLED,
                                        e.getInterruptionReason()
                                );
                            }
                        } else {
                            Assert.assertEquals(64, drain(compiler, foreignContext, sql));
                        }
                        Assert.assertTrue(
                                cleanupPath
                                        ? "foreign latest-by cleanup steal did not signal the victim"
                                        : "foreign latest-by main steal did not signal the victim",
                                victimProgress.getVersion() > observedVersion
                        );
                        Assert.assertTrue(
                                "victim did not resume before the continuation timer",
                                victimDone.await(10, TimeUnit.SECONDS)
                        );
                        Assert.assertNull(ownerFailure.get());
                        Assert.assertEquals(64, victimRows.get());
                        awaitParkedCount(ownerRuntime, 0, ownerFailure);
                        Assert.assertEquals(pubSeq.current(), subSeq.current());
                        Assert.assertEquals(0, dispatcher.getLatestByCreatedTaskCount());
                        Assert.assertEquals(0, dispatcherRuntime.getOutstandingTaskCount());
                    } finally {
                        if (victimDone.getCount() > 0) {
                            victimContext.getCircuitBreaker().cancel();
                            foreignBreaker.cancel();
                            if (victimProgress != null) {
                                dispatcher.signalOwnerProgress(victimProgress);
                            }
                            dispatcher.beginQuiesce();
                            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                            while (victimDone.getCount() > 0 && System.nanoTime() < deadline) {
                                dispatcher.progressQuiesce();
                                victimDone.await(10, TimeUnit.MILLISECONDS);
                            }
                        }
                        if (ownerPoolStarted) {
                            ownerPool.haltAndAssertCleanForTest(WorkerPool.DEFAULT_HALT_TIMEOUT_NANOS);
                        }
                        engine.getMessageBus().setQueryParallelFiberDispatcher(null);
                        Misc.free(dispatcher);
                        closeRuntime(dispatcherRuntime);
                    }
                }
            }
        });
    }

    private static void assertLatestByAdapterCreationFailureCompletesOwnership() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
                @Override
                public int getLatestByQueueCapacity() {
                    return 1;
                }
            };
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
                    final AtomicInteger doneCounter = new AtomicInteger();
                    final AsyncQueryProgressState progressState = new AsyncQueryProgressState();
                    final MPSequence pubSeq = messageBus.getLatestByPubSeq();
                    final MCSequence subSeq = messageBus.getLatestBySubSeq();
                    final long cursor = pubSeq.next();
                    Assert.assertTrue("latest-by queue slot must be available", cursor > -1);
                    final LatestByTask task = messageBus.getLatestByQueue().get(cursor);
                    task.of(
                            null, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0, 0, -1, 0, 0L, 0L,
                            doneCounter::incrementAndGet,
                            circuitBreaker,
                            progressState,
                            new AsyncQueryErrorState()
                    );
                    pubSeq.done(cursor);

                    final long globalProgressBefore = dispatcher.getProgressVersion();
                    final long queryProgressBefore = progressState.getVersion();
                    final RuntimeException injected = new RuntimeException("injected adapter creation failure");
                    setBeforeTaskCreationForTesting(dispatcher, "latestByTaskPool", () -> {
                        throw injected;
                    });
                    final RuntimeException thrown = Assert.assertThrows(
                            RuntimeException.class,
                            () -> dispatcher.consumeLatestBy(-1)
                    );
                    final boolean isCancelled = circuitBreaker.checkIfTripped();
                    final int doneCount = doneCounter.get();
                    final long globalProgressAfter = dispatcher.getProgressVersion();
                    final long queryProgressAfter = progressState.getVersion();

                    if (!isCancelled || doneCount == 0) {
                        task.abort();
                    }
                    assertClaimedCursorAcknowledgedAndReusable(pubSeq, subSeq, cursor);
                    assertFiberAndLeaseReleased(dispatcher, runtime);

                    Assert.assertSame("latest-by must preserve the acquisition failure", injected, thrown);
                    Assert.assertEquals(0, thrown.getSuppressed().length);
                    Assert.assertTrue("latest-by must abort and cancel its task", isCancelled);
                    Assert.assertEquals("latest-by must release its owner", 1, doneCount);
                    Assert.assertEquals(globalProgressBefore + 1, globalProgressAfter);
                    Assert.assertEquals(queryProgressBefore + 1, queryProgressAfter);
                } finally {
                    Misc.free(dispatcher);
                    closeRuntime(runtime);
                }
            }
        });
    }

    // The owner runs as a fiber of its own runtime, so it parks on the dispatcher instead of
    // stealing. With no consumers, its published tasks sit in the queue until the quiesce drain
    // aborts them; the owner must then fail the query instead of returning a partial result.
    private static void assertOwnerCancelledByQuiesceDrain(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext executionContext,
            String poolName,
            String sql,
            Class<?> expectedFactoryClass
    ) throws Exception {
        try (RecordCursorFactory factory = compiler.compile(sql, executionContext).getRecordCursorFactory()) {
            TestUtils.assertFactoryInTree(factory, expectedFactoryClass);
        }
        final FiberRuntime dispatcherRuntime = new FiberRuntime(1);
        final QueryParallelFiberDispatcher dispatcher = new QueryParallelFiberDispatcher(
                engine,
                engine.getMessageBus(),
                dispatcherRuntime
        );
        try {
            engine.getMessageBus().setQueryParallelFiberDispatcher(dispatcher);
            final AtomicReference<Throwable> failure = new AtomicReference<>();
            final CountDownLatch finished = new CountDownLatch(1);
            final AtomicBoolean launched = new AtomicBoolean();
            final AtomicBoolean isWorkerGated = new AtomicBoolean();
            final CountDownLatch ownerParked = new CountDownLatch(1);
            final CountDownLatch drainComplete = new CountDownLatch(1);
            try (TestWorkerPool ownerPool = new TestWorkerPool(
                    poolName,
                    1,
                    Metrics.DISABLED,
                    WorkerPoolMode.FIBER_HOST
            )) {
                final FiberRuntime ownerRuntime = ownerPool.getFiberRuntime();
                final FiberTask task = new FiberTask() {
                    @Override
                    protected void onDone() {
                        finished.countDown();
                    }

                    @Override
                    protected void onError(Throwable th) {
                        failure.compareAndSet(null, th);
                        finished.countDown();
                    }

                    @Override
                    protected boolean runStep() {
                        SuspensionScope.enterTimerShards(engine.getTimerShards());
                        try {
                            drain(compiler, executionContext, sql);
                        } catch (RuntimeException | Error e) {
                            throw e;
                        } catch (Exception e) {
                            throw new AssertionError(e);
                        }
                        return true;
                    }
                };
                // The single worker both launches the owner and, once the owner has published and
                // parked, blocks in this job so nothing can mount the owner while the test thread
                // drains the dispatcher. The owner resumes only after the drain aborted its tasks.
                ownerPool.assign(_ -> {
                    if (launched.compareAndSet(false, true)) {
                        final LaunchResult result = ownerRuntime.launch(task);
                        if (result != LaunchResult.LAUNCHED) {
                            failure.set(new AssertionError("owner fiber launch failed [result=" + result + ']'));
                            finished.countDown();
                        }
                        return true;
                    }
                    if (isWorkerGated.compareAndSet(false, ownerRuntime.getParkedFiberCount() == 1)) {
                        return false;
                    }
                    ownerParked.countDown();
                    try {
                        if (!drainComplete.await(10, TimeUnit.SECONDS)) {
                            failure.compareAndSet(null, new AssertionError("dispatcher drain timed out"));
                            finished.countDown();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return true;
                });
                ownerPool.start(LOG);
                Assert.assertTrue("owner fiber did not park", ownerParked.await(10, TimeUnit.SECONDS));

                dispatcher.beginQuiesce();
                dispatcher.progressQuiesce();
                drainComplete.countDown();

                final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (!finished.await(10, TimeUnit.MILLISECONDS)) {
                    Assert.assertTrue("owner did not finish after quiesce drain", System.nanoTime() < deadline);
                    dispatcher.progressQuiesce();
                }
                ownerPool.haltAndAssertCleanForTest(WorkerPool.DEFAULT_HALT_TIMEOUT_NANOS);
            }

            final Throwable th = failure.get();
            Assert.assertNotNull("query with aborted tasks must fail, not return a partial result", th);
            Assert.assertTrue(String.valueOf(th), th instanceof CairoException);
            final CairoException e = (CairoException) th;
            TestUtils.assertContains(e.getFlyweightMessage(), "cancelled by user");
            Assert.assertEquals(SqlExecutionCircuitBreaker.STATE_CANCELLED, e.getInterruptionReason());
        } finally {
            closeRuntime(dispatcherRuntime);
            Misc.free(dispatcher);
        }
    }

    private static void assertParallelFiberOwnerHelpsOwnWork(DrainTaskType taskType) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final AtomicLong clockTicks = new AtomicLong();
            // Every clock read advances by the owner yield interval, so each helped unit of work
            // after the first one yields the carrier.
            final NanosecondClock nanosecondClock = () -> clockTicks.getAndAdd(1_000_000L);
            final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
                @Override
                public int getGroupByMergeShardQueueCapacity() {
                    return 1;
                }

                @Override
                public long getGroupByParallelTopKThreshold() {
                    return 0;
                }

                @Override
                public int getGroupByShardingThreshold() {
                    return 1;
                }

                @Override
                public int getGroupByTopKQueueCapacity() {
                    return 1;
                }

                @Override
                public int getLatestByQueueCapacity() {
                    return 1;
                }

                @Override
                public NanosecondClock getNanosecondClock() {
                    return nanosecondClock;
                }

                @Override
                public long getQueryContinuationWakeIntervalMillis() {
                    return TimeUnit.HOURS.toMillis(1);
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
                    return 1;
                }
            };
            final WorkerPoolConfiguration poolConfiguration = new WorkerPoolConfiguration() {
                @Override
                public Metrics getMetrics() {
                    return Metrics.DISABLED;
                }

                @Override
                public String getPoolName() {
                    return "fiber-owner-helps-" + taskType;
                }

                @Override
                public int getWorkerCount() {
                    return 0;
                }

                @Override
                public WorkerPoolMode getWorkerPoolMode() {
                    return WorkerPoolMode.FIBER_HOST;
                }
            };
            final TestWorkerPool queryPool = new TestWorkerPool(poolConfiguration);
            // LATEST BY splits its key range into one task per shared worker, so it needs
            // several of them to leave the owner unpublished tasks to help with.
            final int sharedWorkerCount = taskType == DrainTaskType.LATEST_BY ? 8 : 1;
            try (CairoEngine engine = new CairoEngine(configuration);
                 SqlCompiler compiler = engine.getSqlCompiler();
                 SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine, sharedWorkerCount);
                 TestWorkerPool ownerPool = new TestWorkerPool(
                         "fiber-owner-helps-owner-" + taskType,
                         1,
                         Metrics.DISABLED,
                         WorkerPoolMode.FIBER_HOST
                 )) {
                TestUtils.setupWorkerPool(queryPool, engine);
                queryPool.start(LOG);
                final QueryParallelFiberDispatcher dispatcher = engine.getMessageBus()
                        .getQueryParallelFiberDispatcher();
                Assert.assertNotNull(dispatcher);

                final String sql;
                final Class<?> expectedFactoryClass;
                final int expectedRowCount;
                final int valueColumnIndex;
                final long expectedValueSum;
                switch (taskType) {
                    case LATEST_BY -> {
                        engine.execute(
                                """
                                        CREATE TABLE tab AS (
                                            SELECT x id, ('k' || (x % 64))::SYMBOL sym, timestamp_sequence(0, 1_000_000) ts
                                            FROM long_sequence(4096)
                                        ), INDEX(sym) TIMESTAMP(ts) PARTITION BY DAY""",
                                executionContext
                        );
                        sql = "SELECT * FROM tab LATEST ON ts PARTITION BY sym";
                        expectedFactoryClass = LatestByAllIndexedRecordCursorFactory.class;
                        expectedRowCount = 64;
                        valueColumnIndex = 0;
                        expectedValueSum = 260_128;
                    }
                    case VECTOR_AGGREGATE -> {
                        engine.execute(
                                "CREATE TABLE tab AS (SELECT (x % 17)::INT k, x v FROM long_sequence(1024))",
                                executionContext
                        );
                        sql = "SELECT k, SUM(v) s FROM tab";
                        expectedFactoryClass = GroupByRecordCursorFactory.class;
                        expectedRowCount = 17;
                        valueColumnIndex = 1;
                        expectedValueSum = 524_800;
                    }
                    case MERGE_SHARD -> {
                        engine.execute(
                                "CREATE TABLE tab AS (SELECT ('k' || x) key, x v FROM long_sequence(128))",
                                executionContext
                        );
                        sql = "SELECT key, MAX(v) m FROM tab";
                        expectedFactoryClass = AsyncGroupByRecordCursorFactory.class;
                        expectedRowCount = 128;
                        valueColumnIndex = 1;
                        expectedValueSum = 8_256;
                    }
                    default -> {
                        engine.execute(
                                "CREATE TABLE tab AS (SELECT ('k' || x) key, x v FROM long_sequence(256))",
                                executionContext
                        );
                        sql = "SELECT key, MAX(v) m FROM tab ORDER BY m DESC LIMIT 10";
                        expectedFactoryClass = LongTopKRecordCursorFactory.class;
                        expectedRowCount = 10;
                        valueColumnIndex = 1;
                        expectedValueSum = 2_515;
                    }
                }
                try (RecordCursorFactory factory = compiler.compile(sql, executionContext).getRecordCursorFactory()) {
                    TestUtils.assertFactoryInTree(factory, expectedFactoryClass);
                }

                final FiberRuntime ownerRuntime = ownerPool.getFiberRuntime();
                final AtomicReference<Throwable> ownerFailure = new AtomicReference<>();
                final CountDownLatch ownerDone = new CountDownLatch(1);
                final AtomicBoolean launched = new AtomicBoolean();
                final AtomicInteger rowCount = new AtomicInteger();
                final AtomicLong valueSum = new AtomicLong();
                final FiberTask ownerTask = new FiberTask() {
                    @Override
                    protected void onDone() {
                        ownerDone.countDown();
                    }

                    @Override
                    protected void onError(Throwable th) {
                        ownerFailure.compareAndSet(null, th);
                        ownerDone.countDown();
                    }

                    @Override
                    protected boolean runStep() {
                        SuspensionScope.enterTimerShards(engine.getTimerShards());
                        try (RecordCursorFactory factory = engine.select(sql, executionContext);
                             RecordCursor cursor = factory.getCursor(executionContext)) {
                            final Record record = cursor.getRecord();
                            while (cursor.hasNext()) {
                                rowCount.incrementAndGet();
                                valueSum.addAndGet(record.getLong(valueColumnIndex));
                            }
                        } catch (SqlException e) {
                            throw new AssertionError(e);
                        }
                        return true;
                    }
                };
                ownerPool.assign(_ -> {
                    if (launched.compareAndSet(false, true)) {
                        final LaunchResult result = ownerRuntime.launch(ownerTask);
                        if (result != LaunchResult.LAUNCHED) {
                            ownerFailure.compareAndSet(
                                    null,
                                    new AssertionError("owner Fiber launch failed [result=" + result + ']')
                            );
                            ownerDone.countDown();
                        }
                        return true;
                    }
                    return false;
                });

                final MessageBus messageBus = engine.getMessageBus();
                final MPSequence pubSeq = pubSeqOf(taskType, messageBus);
                final MCSequence subSeq = subSeqOf(taskType, messageBus);
                final long publishedBefore = pubSeq.current();
                boolean isOwnerPoolStarted = false;
                try {
                    ownerPool.start(LOG);
                    isOwnerPoolStarted = true;
                    // The launch mount is the only one before the owner starts helping.
                    long mountCountBeforeHelp = 1;
                    if (taskType == DrainTaskType.LONG_TOP_K) {
                        // The merge-shard stage parks first. Its single published task wakes the
                        // owner at most twice, once per progress signal, before the top-K stage.
                        awaitParkedCount(ownerRuntime, 1, ownerFailure);
                        mountCountBeforeHelp = ownerRuntime.getMountCount() + 2;
                        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                        while (pubSeq.current() == publishedBefore) {
                            drainPublishedTasks(DrainTaskType.MERGE_SHARD, messageBus, dispatcher, ownerFailure);
                            Assert.assertTrue(
                                    "owner did not reach the long top-K stage",
                                    System.nanoTime() < deadline
                            );
                            Assert.assertNull(ownerFailure.get());
                            Os.pause();
                        }
                    }
                    awaitParkedCount(ownerRuntime, 1, ownerFailure);
                    Assert.assertEquals(
                            "the Fiber owner must leave only the first published task for consumers",
                            publishedBefore + 1,
                            pubSeq.current()
                    );
                    Assert.assertTrue(
                            "Fiber owner helping must honor its cooperative deadline",
                            ownerRuntime.getMountCount() > mountCountBeforeHelp
                    );

                    final long cursor = subSeq.next();
                    Assert.assertEquals(publishedBefore + 1, cursor);
                    runPublishedTask(taskType, messageBus, dispatcher, subSeq, cursor);

                    // LATEST BY publishes one task per page frame, so keep serving the owner
                    // until it is done.
                    final long finishDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                    while (!ownerDone.await(10, TimeUnit.MILLISECONDS)) {
                        Assert.assertTrue("Fiber owner did not finish", System.nanoTime() < finishDeadline);
                        drainPublishedTasks(taskType, messageBus, dispatcher, ownerFailure);
                    }
                    Assert.assertNull(ownerFailure.get());
                    Assert.assertEquals(expectedRowCount, rowCount.get());
                    Assert.assertEquals(expectedValueSum, valueSum.get());
                    Assert.assertEquals(pubSeq.current(), subSeq.current());
                    Assert.assertEquals(
                            messageBus.getGroupByMergeShardPubSeq().current(),
                            messageBus.getGroupByMergeShardSubSeq().current()
                    );
                    Assert.assertEquals(0, dispatcher.getLatestByCreatedTaskCount());
                    Assert.assertEquals(0, dispatcher.getMergeShardCreatedTaskCount());
                    Assert.assertEquals(0, dispatcher.getLongTopKCreatedTaskCount());
                    Assert.assertEquals(0, dispatcher.getVectorAggregateCreatedTaskCount());
                } finally {
                    if (ownerDone.getCount() > 0) {
                        executionContext.getCircuitBreaker().cancel();
                        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                        while (ownerDone.getCount() > 0 && System.nanoTime() < deadline) {
                            boolean hasRun = drainPublishedTasks(taskType, messageBus, dispatcher, ownerFailure);
                            if (taskType == DrainTaskType.LONG_TOP_K) {
                                hasRun |= drainPublishedTasks(
                                        DrainTaskType.MERGE_SHARD,
                                        messageBus,
                                        dispatcher,
                                        ownerFailure
                                );
                            }
                            if (!hasRun) {
                                Os.pause();
                            }
                        }
                    }
                    if (isOwnerPoolStarted) {
                        ownerPool.haltAndAssertCleanForTest(WorkerPool.DEFAULT_HALT_TIMEOUT_NANOS);
                    }
                }
            } finally {
                queryPool.haltAndAssertCleanForTest(WorkerPool.DEFAULT_HALT_TIMEOUT_NANOS);
            }
        });
    }

    private static void assertParallelOwnerCancelledMidDrain(DrainTaskType taskType) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
                @Override
                public int getGroupByMergeShardQueueCapacity() {
                    return taskType == DrainTaskType.LONG_TOP_K ? 128 : 2;
                }

                @Override
                public long getGroupByParallelTopKThreshold() {
                    return 0;
                }

                @Override
                public int getGroupByShardingThreshold() {
                    return 1;
                }

                @Override
                public int getGroupByTopKQueueCapacity() {
                    return 2;
                }

                @Override
                public long getQueryContinuationWakeIntervalMillis() {
                    return TimeUnit.HOURS.toMillis(1);
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
            try (
                    CairoEngine engine = new CairoEngine(configuration);
                    SqlCompiler compiler = engine.getSqlCompiler();
                    SqlExecutionContext setupContext = TestUtils.createSqlExecutionCtx(engine, 2)
            ) {
                final FiberCancellationSignal victimSignal = new FiberCancellationSignal();
                final AtomicBooleanCircuitBreaker victimBreaker = new AtomicBooleanCircuitBreaker(engine);
                victimBreaker.setCancelledFlag(victimSignal);
                try (
                        SqlExecutionContext victimContext = TestUtils.createSqlExecutionCtx(engine, 2).with(victimBreaker);
                        TestWorkerPool ownerPool = new TestWorkerPool(
                                "parallel-cancel-mid-drain-" + taskType,
                                1,
                                Metrics.DISABLED,
                                WorkerPoolMode.FIBER_HOST
                        )
                ) {
                    final String sql;
                    final Class<?> expectedFactoryClass;
                    if (taskType == DrainTaskType.VECTOR_AGGREGATE) {
                        engine.execute(
                                "CREATE TABLE vector_cancel AS "
                                        + "(SELECT (x % 17)::INT k, x v FROM long_sequence(512))",
                                setupContext
                        );
                        sql = "SELECT k, sum(v) FROM vector_cancel";
                        expectedFactoryClass =
                                io.questdb.griffin.engine.groupby.vect.GroupByRecordCursorFactory.class;
                    } else {
                        engine.execute(
                                "CREATE TABLE group_cancel AS "
                                        + "(SELECT ('k' || x) key, x v FROM long_sequence(256))",
                                setupContext
                        );
                        if (taskType == DrainTaskType.LONG_TOP_K) {
                            sql = "SELECT key, max(v) m FROM group_cancel ORDER BY m DESC LIMIT 10";
                            expectedFactoryClass = LongTopKRecordCursorFactory.class;
                        } else {
                            sql = "SELECT key, max(v) m FROM group_cancel";
                            expectedFactoryClass = AsyncGroupByRecordCursorFactory.class;
                        }
                    }
                    try (RecordCursorFactory factory = compiler.compile(sql, setupContext).getRecordCursorFactory()) {
                        TestUtils.assertFactoryInTree(factory, expectedFactoryClass);
                    }

                    final FiberRuntime dispatcherRuntime = new FiberRuntime(1);
                    final QueryParallelFiberDispatcher dispatcher = new QueryParallelFiberDispatcher(
                            engine,
                            engine.getMessageBus(),
                            dispatcherRuntime
                    );
                    engine.getMessageBus().setQueryParallelFiberDispatcher(dispatcher);

                    final FiberRuntime ownerRuntime = ownerPool.getFiberRuntime();
                    final AtomicReference<Throwable> victimFailure = new AtomicReference<>();
                    final CountDownLatch victimDone = new CountDownLatch(1);
                    final AtomicBoolean launched = new AtomicBoolean();
                    final FiberTask victimTask = new FiberTask() {
                        @Override
                        protected void onDone() {
                            victimDone.countDown();
                        }

                        @Override
                        protected void onError(Throwable th) {
                            victimFailure.compareAndSet(null, th);
                            victimDone.countDown();
                        }

                        @Override
                        protected boolean runStep() {
                            SuspensionScope.enterTimerShards(engine.getTimerShards());
                            try {
                                drain(compiler, victimContext, sql);
                            } catch (RuntimeException | Error e) {
                                throw e;
                            } catch (Exception e) {
                                throw new AssertionError(e);
                            }
                            return true;
                        }
                    };
                    ownerPool.assign(_ -> {
                        if (launched.compareAndSet(false, true)) {
                            final LaunchResult result = ownerRuntime.launch(victimTask);
                            if (result != LaunchResult.LAUNCHED) {
                                victimFailure.compareAndSet(
                                        null,
                                        new AssertionError("victim launch failed [result=" + result + ']')
                                );
                                victimDone.countDown();
                            }
                            return true;
                        }
                        return false;
                    });

                    AsyncQueryProgressState victimProgress = null;
                    boolean isOwnerPoolStarted = false;
                    try {
                        final MessageBus messageBus = engine.getMessageBus();
                        ownerPool.start(LOG);
                        isOwnerPoolStarted = true;
                        awaitParkedCount(ownerRuntime, 1, victimFailure);

                        if (taskType == DrainTaskType.LONG_TOP_K) {
                            final MCSequence mergeSubSeq = messageBus.getGroupByMergeShardSubSeq();
                            final RingQueue<GroupByMergeShardTask> mergeQueue = messageBus.getGroupByMergeShardQueue();
                            final MPSequence topKPubSeq = messageBus.getGroupByLongTopKPubSeq();
                            final MCSequence topKSubSeq = messageBus.getGroupByLongTopKSubSeq();
                            int mergedCount = 0;
                            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                            while (topKPubSeq.current() == topKSubSeq.current()) {
                                long seq;
                                while ((seq = mergeSubSeq.next()) > -1) {
                                    final GroupByMergeShardTask task = mergeQueue.get(seq);
                                    final GroupByShardingContext shardingContext = task.getShardingContext();
                                    final AsyncQueryProgressState mergeProgress = shardingContext.getProgressState();
                                    GroupByMergeShardJob.run(-1, task, mergeSubSeq, seq, shardingContext);
                                    try {
                                        dispatcher.signalQueueProgress();
                                    } finally {
                                        dispatcher.signalOwnerProgress(mergeProgress);
                                    }
                                    mergedCount++;
                                }
                                Assert.assertTrue(
                                        "owner did not reach the long top-K drain",
                                        System.nanoTime() < deadline
                                );
                                Assert.assertNull(victimFailure.get());
                                Os.pause();
                            }
                            Assert.assertTrue("long top-K setup did not publish merge tasks", mergedCount > 0);
                            awaitParkedCount(ownerRuntime, 1, victimFailure);
                        }

                        final MPSequence pubSeq = taskType == DrainTaskType.VECTOR_AGGREGATE
                                ? messageBus.getVectorAggregatePubSeq()
                                : taskType == DrainTaskType.MERGE_SHARD
                                  ? messageBus.getGroupByMergeShardPubSeq()
                                  : messageBus.getGroupByLongTopKPubSeq();
                        final MCSequence subSeq = taskType == DrainTaskType.VECTOR_AGGREGATE
                                ? messageBus.getVectorAggregateSubSeq()
                                : taskType == DrainTaskType.MERGE_SHARD
                                  ? messageBus.getGroupByMergeShardSubSeq()
                                  : messageBus.getGroupByLongTopKSubSeq();
                        final long victimCursor = subSeq.current() + 1;
                        Assert.assertTrue("owner did not publish a drain task", victimCursor <= pubSeq.current());

                        final SqlExecutionCircuitBreaker sharedBreaker;
                        if (taskType == DrainTaskType.VECTOR_AGGREGATE) {
                            final VectorAggregateEntry entry = messageBus.getVectorAggregateQueue()
                                    .get(victimCursor).entry;
                            victimProgress = entry.getProgressState();
                            sharedBreaker = entry.getCircuitBreaker();
                        } else if (taskType == DrainTaskType.MERGE_SHARD) {
                            final GroupByMergeShardTask task = messageBus.getGroupByMergeShardQueue().get(victimCursor);
                            victimProgress = task.getShardingContext().getProgressState();
                            sharedBreaker = task.getCircuitBreaker();
                        } else {
                            final GroupByLongTopKTask task = messageBus.getGroupByLongTopKQueue().get(victimCursor);
                            victimProgress = task.getAtom().getShardingContext().getProgressState();
                            sharedBreaker = task.getCircuitBreaker();
                        }
                        Assert.assertNotNull(victimProgress);
                        Assert.assertFalse(sharedBreaker.checkIfTripped());

                        victimBreaker.cancel();

                        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                        while (!sharedBreaker.checkIfTripped()) {
                            Assert.assertTrue(
                                    "owner did not cancel the task breaker",
                                    System.nanoTime() < deadline
                            );
                            Assert.assertNull(victimFailure.get());
                            Os.pause();
                        }
                        awaitParkedCount(ownerRuntime, 1, victimFailure);
                        Assert.assertEquals(1, victimDone.getCount());

                        int completedCount = 0;
                        long seq;
                        while ((seq = subSeq.next()) > -1) {
                            if (completedCount == 0) {
                                Assert.assertEquals("published drain task must be claimable", victimCursor, seq);
                            }
                            if (taskType == DrainTaskType.VECTOR_AGGREGATE) {
                                messageBus.getVectorAggregateQueue().get(seq).entry.run(-1, subSeq, seq);
                            } else if (taskType == DrainTaskType.MERGE_SHARD) {
                                final GroupByMergeShardTask task = messageBus.getGroupByMergeShardQueue().get(seq);
                                final GroupByShardingContext shardingContext = task.getShardingContext();
                                GroupByMergeShardJob.run(-1, task, subSeq, seq, shardingContext);
                            } else {
                                final GroupByLongTopKTask task = messageBus.getGroupByLongTopKQueue().get(seq);
                                final AsyncGroupByAtom atom = task.getAtom();
                                GroupByLongTopKJob.run(-1, task, subSeq, seq, atom);
                            }
                            try {
                                dispatcher.signalQueueProgress();
                            } finally {
                                dispatcher.signalOwnerProgress(victimProgress);
                            }
                            completedCount++;
                        }
                        Assert.assertTrue("published drain tasks must be claimable", completedCount > 0);

                        Assert.assertTrue(
                                "owner did not finish after the drain completed",
                                victimDone.await(10, TimeUnit.SECONDS)
                        );
                        final Throwable th = victimFailure.get();
                        Assert.assertNotNull("cancelled query must fail, not return a partial result", th);
                        Assert.assertTrue(String.valueOf(th), th instanceof CairoException);
                        final CairoException e = (CairoException) th;
                        Assert.assertTrue(e.isInterruption());
                        Assert.assertEquals(SqlExecutionCircuitBreaker.STATE_CANCELLED, e.getInterruptionReason());
                        Assert.assertEquals(pubSeq.current(), subSeq.current());
                        Assert.assertEquals(
                                0,
                                taskType == DrainTaskType.VECTOR_AGGREGATE
                                        ? dispatcher.getVectorAggregateCreatedTaskCount()
                                        : taskType == DrainTaskType.MERGE_SHARD
                                          ? dispatcher.getMergeShardCreatedTaskCount()
                                          : dispatcher.getLongTopKCreatedTaskCount()
                        );
                        Assert.assertEquals(0, dispatcherRuntime.getOutstandingTaskCount());
                    } finally {
                        if (victimDone.getCount() > 0) {
                            victimBreaker.cancel();
                            if (victimProgress != null) {
                                dispatcher.signalOwnerProgress(victimProgress);
                            }
                            dispatcher.beginQuiesce();
                            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                            while (victimDone.getCount() > 0 && System.nanoTime() < deadline) {
                                dispatcher.progressQuiesce();
                                victimDone.await(10, TimeUnit.MILLISECONDS);
                            }
                        }
                        if (isOwnerPoolStarted) {
                            ownerPool.haltAndAssertCleanForTest(WorkerPool.DEFAULT_HALT_TIMEOUT_NANOS);
                        }
                        engine.getMessageBus().setQueryParallelFiberDispatcher(null);
                        Misc.free(dispatcher);
                        closeRuntime(dispatcherRuntime);
                    }
                }
            }
        });
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

    private static void assertVectorAggregateAdapterCreationFailureCompletesOwnership() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
                @Override
                public int getVectorAggregateQueueCapacity() {
                    return 1;
                }
            };
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
                    final AtomicInteger doneCounter = new AtomicInteger();
                    final AsyncQueryProgressState progressState = new AsyncQueryProgressState();
                    final AtomicBoolean isAborted = new AtomicBoolean();
                    final AtomicBoolean isStartedArgument = new AtomicBoolean();
                    final VectorAggregateEntry entry = new VectorAggregateEntry() {
                        @Override
                        public void abort(boolean isStarted) {
                            isAborted.set(true);
                            isStartedArgument.set(isStarted);
                            if (!isStarted) {
                                startedCounter.incrementAndGet();
                            }
                            try {
                                circuitBreaker.cancel();
                            } finally {
                                doneCounter.incrementAndGet();
                            }
                        }

                        @Override
                        public AsyncQueryProgressState getProgressState() {
                            return progressState;
                        }
                    };
                    final MPSequence pubSeq = messageBus.getVectorAggregatePubSeq();
                    final MCSequence subSeq = messageBus.getVectorAggregateSubSeq();
                    final long cursor = pubSeq.next();
                    Assert.assertTrue("vector aggregate queue slot must be available", cursor > -1);
                    final VectorAggregateTask task = messageBus.getVectorAggregateQueue().get(cursor);
                    task.entry = entry;
                    pubSeq.done(cursor);

                    final long globalProgressBefore = dispatcher.getProgressVersion();
                    final long queryProgressBefore = progressState.getVersion();
                    final RuntimeException injected = new RuntimeException("injected adapter creation failure");
                    setBeforeTaskCreationForTesting(dispatcher, "vectorAggregateTaskPool", () -> {
                        throw injected;
                    });
                    final RuntimeException thrown = Assert.assertThrows(
                            RuntimeException.class,
                            () -> dispatcher.consumeVectorAggregate(-1)
                    );
                    final boolean isTaskDetached = task.entry == null;
                    final boolean isAbortedAfterFailure = isAborted.get();
                    final boolean isStartedArgumentAfterFailure = isStartedArgument.get();
                    final boolean isCancelled = circuitBreaker.checkIfTripped();
                    final int startedCount = startedCounter.get();
                    final int doneCount = doneCounter.get();
                    final long globalProgressAfter = dispatcher.getProgressVersion();
                    final long queryProgressAfter = progressState.getVersion();

                    task.entry = null;
                    if (!isAbortedAfterFailure) {
                        entry.abort(false);
                    }
                    assertClaimedCursorAcknowledgedAndReusable(pubSeq, subSeq, cursor);
                    assertFiberAndLeaseReleased(dispatcher, runtime);

                    Assert.assertSame("vector aggregate must preserve the acquisition failure", injected, thrown);
                    Assert.assertEquals(0, thrown.getSuppressed().length);
                    Assert.assertTrue("vector aggregate must detach the queue entry", isTaskDetached);
                    Assert.assertTrue("vector aggregate must abort the detached entry", isAbortedAfterFailure);
                    Assert.assertFalse("never-started vector work must use abort(false)", isStartedArgumentAfterFailure);
                    Assert.assertTrue("vector aggregate must cancel the shared breaker", isCancelled);
                    Assert.assertEquals("vector aggregate must count the task as started", 1, startedCount);
                    Assert.assertEquals("vector aggregate must release its owner", 1, doneCount);
                    Assert.assertEquals(globalProgressBefore + 1, globalProgressAfter);
                    Assert.assertEquals(queryProgressBefore + 1, queryProgressAfter);
                } finally {
                    Misc.free(dispatcher);
                    closeRuntime(runtime);
                }
            }
        });
    }

    private static void assertVectorOwnerStealSignalsQueueBeforeDetachedCompletion() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
                @Override
                public int getVectorAggregateQueueCapacity() {
                    return 1;
                }
            };
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
                    final AtomicInteger doneCounter = new AtomicInteger();
                    final AsyncQueryProgressState progressState = new AsyncQueryProgressState();
                    final CountDownLatch detachedEntered = new CountDownLatch(1);
                    final CountDownLatch detachedRelease = new CountDownLatch(1);
                    final TestVectorAggregateEntry entry = new TestVectorAggregateEntry(
                            circuitBreaker,
                            doneCounter,
                            1,
                            () -> {
                                detachedEntered.countDown();
                                try {
                                    if (!detachedRelease.await(10, TimeUnit.SECONDS)) {
                                        throw new AssertionError("timed out waiting to release detached work");
                                    }
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                    throw new AssertionError(e);
                                }
                            },
                            progressState
                    );
                    final MPSequence pubSeq = messageBus.getVectorAggregatePubSeq();
                    final MCSequence subSeq = messageBus.getVectorAggregateSubSeq();
                    final long cursor = pubSeq.next();
                    Assert.assertTrue("vector aggregate queue slot must be available", cursor > -1);
                    pubSeq.done(cursor);
                    Assert.assertEquals(cursor, subSeq.next());

                    final AtomicReference<Throwable> taskFailure = new AtomicReference<>();
                    final Thread taskThread = new Thread(() -> {
                        try {
                            entry.run(-1, subSeq, cursor, dispatcher);
                        } catch (Throwable th) {
                            taskFailure.set(th);
                        }
                    }, "vector-owner-steal");
                    final long globalProgressBefore = dispatcher.getProgressVersion();
                    final long ownerProgressBefore = progressState.getVersion();
                    taskThread.setDaemon(true);
                    taskThread.start();
                    try {
                        Assert.assertTrue(
                                "vector task did not enter detached work",
                                detachedEntered.await(10, TimeUnit.SECONDS)
                        );
                        assertClaimedCursorAcknowledgedAndReusable(pubSeq, subSeq, cursor);
                        Assert.assertEquals(globalProgressBefore + 1, dispatcher.getProgressVersion());
                        Assert.assertEquals(ownerProgressBefore, progressState.getVersion());
                        Assert.assertEquals(0, doneCounter.get());
                    } finally {
                        detachedRelease.countDown();
                        taskThread.join(TimeUnit.SECONDS.toMillis(10));
                    }

                    Assert.assertFalse("vector owner-steal task did not terminate", taskThread.isAlive());
                    Assert.assertNull(taskFailure.get());
                    Assert.assertEquals(1, doneCounter.get());
                    Assert.assertEquals(globalProgressBefore + 1, dispatcher.getProgressVersion());
                    Assert.assertEquals(ownerProgressBefore + 1, progressState.getVersion());
                } finally {
                    Misc.free(dispatcher);
                    closeRuntime(runtime);
                }
            }
        });
    }

    private static void awaitParkedCount(FiberRuntime runtime, int expected, AtomicReference<Throwable> failure) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (runtime.getParkedFiberCount() != expected) {
            Assert.assertTrue("parked owner count did not settle at " + expected, System.nanoTime() < deadline);
            Assert.assertNull(failure.get());
            Os.pause();
        }
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

    private static boolean drainPublishedTasks(
            DrainTaskType taskType,
            MessageBus messageBus,
            QueryParallelFiberDispatcher dispatcher,
            AtomicReference<Throwable> failure
    ) {
        final MCSequence subSeq = subSeqOf(taskType, messageBus);
        boolean hasRun = false;
        long cursor;
        while ((cursor = subSeq.next()) > -1) {
            try {
                runPublishedTask(taskType, messageBus, dispatcher, subSeq, cursor);
            } catch (Throwable th) {
                failure.compareAndSet(null, th);
            }
            hasRun = true;
        }
        return hasRun;
    }

    private static void publishLatestByTask(
            RingQueue<LatestByTask> queue,
            MPSequence pubSeq,
            SqlExecutionCircuitBreaker circuitBreaker,
            CountDownLatchSPI doneLatch,
            AsyncQueryProgressState progressState
    ) {
        final long cursor = pubSeq.next();
        Assert.assertTrue("latest-by queue slot must be available", cursor > -1);
        queue.get(cursor).of(
                null, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0, 0, -1, 0, 0L, 0L,
                doneLatch, circuitBreaker, progressState, new AsyncQueryErrorState()
        );
        pubSeq.done(cursor);
    }

    private static void publishVectorAggregateTask(
            RingQueue<VectorAggregateTask> queue,
            MPSequence pubSeq,
            VectorAggregateEntry entry
    ) {
        final long cursor = pubSeq.next();
        Assert.assertTrue("vector aggregate queue slot must be available", cursor > -1);
        queue.get(cursor).entry = entry;
        pubSeq.done(cursor);
    }

    // A fiber owner parks on the dispatcher instead of stealing, so every published task is left
    // for the query pool to consume. An ordinary owner helps out and can drain the queue itself,
    // which makes the dispatcher task counters racy.
    private static MPSequence pubSeqOf(DrainTaskType taskType, MessageBus messageBus) {
        return switch (taskType) {
            case LATEST_BY -> messageBus.getLatestByPubSeq();
            case VECTOR_AGGREGATE -> messageBus.getVectorAggregatePubSeq();
            case MERGE_SHARD -> messageBus.getGroupByMergeShardPubSeq();
            case LONG_TOP_K -> messageBus.getGroupByLongTopKPubSeq();
        };
    }

    private static FiberRuntime runAsFiberOwner(
            CairoEngine engine,
            String poolName,
            FiberOwnerBody body
    ) throws Exception {
        final AtomicReference<Throwable> error = new AtomicReference<>();
        final CountDownLatch finished = new CountDownLatch(1);
        final AtomicBoolean launched = new AtomicBoolean();
        try (TestWorkerPool ownerPool = new TestWorkerPool(
                poolName,
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
                    finished.countDown();
                }

                @Override
                protected boolean runStep() {
                    SuspensionScope.enterTimerShards(engine.getTimerShards());
                    try {
                        body.run();
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
            return runtime;
        }
    }

    private static void runPublishedTask(
            DrainTaskType taskType,
            MessageBus messageBus,
            QueryParallelFiberDispatcher dispatcher,
            MCSequence subSeq,
            long cursor
    ) {
        switch (taskType) {
            case LATEST_BY -> {
                final LatestByTask task = messageBus.getLatestByQueue().get(cursor);
                final AsyncQueryProgressState progressState = task.getProgressState();
                try {
                    task.run();
                } finally {
                    subSeq.done(cursor);
                    try {
                        dispatcher.signalQueueProgress();
                    } finally {
                        dispatcher.signalOwnerProgress(progressState);
                    }
                }
            }
            case VECTOR_AGGREGATE -> messageBus.getVectorAggregateQueue()
                    .get(cursor)
                    .entry
                    .run(-1, subSeq, cursor, dispatcher);
            case MERGE_SHARD -> {
                final GroupByMergeShardTask task = messageBus.getGroupByMergeShardQueue().get(cursor);
                GroupByMergeShardJob.run(-1, task, subSeq, cursor, task.getShardingContext(), dispatcher);
            }
            case LONG_TOP_K -> {
                final GroupByLongTopKTask task = messageBus.getGroupByLongTopKQueue().get(cursor);
                GroupByLongTopKJob.run(-1, task, subSeq, cursor, task.getAtom(), dispatcher);
            }
        }
    }

    private static void setBeforeTaskCreationForTesting(
            QueryParallelFiberDispatcher dispatcher,
            String poolFieldName,
            Runnable hook
    ) throws Exception {
        final Field poolField = QueryParallelFiberDispatcher.class.getDeclaredField(poolFieldName);
        poolField.setAccessible(true);
        final Object taskPool = poolField.get(dispatcher);
        final Method setter = taskPool.getClass().getDeclaredMethod("setBeforeNewTaskForTesting", Runnable.class);
        setter.setAccessible(true);
        setter.invoke(taskPool, hook);
    }

    private static MCSequence subSeqOf(DrainTaskType taskType, MessageBus messageBus) {
        return switch (taskType) {
            case LATEST_BY -> messageBus.getLatestBySubSeq();
            case VECTOR_AGGREGATE -> messageBus.getVectorAggregateSubSeq();
            case MERGE_SHARD -> messageBus.getGroupByMergeShardSubSeq();
            case LONG_TOP_K -> messageBus.getGroupByLongTopKSubSeq();
        };
    }

    private enum DrainTaskType {
        LATEST_BY,
        LONG_TOP_K,
        MERGE_SHARD,
        VECTOR_AGGREGATE
    }

    @FunctionalInterface
    private interface FiberOwnerBody {
        void run() throws Exception;
    }

    private static final class TestScopeCircuitBreaker extends PostAggregationCircuitBreaker {
        private final Runnable onFirstCheck;
        private boolean hasObservedScope;
        private long observedGeneration = -1;
        private FiberCancellationSignal observedSignal;

        private TestScopeCircuitBreaker(CairoEngine engine, Runnable onFirstCheck) {
            super(engine);
            this.onFirstCheck = onFirstCheck;
        }

        @Override
        public boolean checkIfTripped() {
            if (!hasObservedScope) {
                hasObservedScope = true;
                observedSignal = SuspensionScope.getCancellationSignal();
                observedGeneration = SuspensionScope.getCancellationSignalGeneration();
                if (onFirstCheck != null) {
                    onFirstCheck.run();
                }
            }
            return true;
        }
    }

    private static final class TestVectorAggregateEntry extends VectorAggregateEntry {
        private final AtomicBooleanCircuitBreaker circuitBreaker;
        private final AtomicInteger doneCounter;
        private final long frameRowCount;
        private final Runnable onRun;
        private final AsyncQueryProgressState progressState;
        private long observedGeneration = -1;
        private FiberCancellationSignal observedSignal;

        private TestVectorAggregateEntry(
                AtomicBooleanCircuitBreaker circuitBreaker,
                AtomicInteger doneCounter,
                long frameRowCount,
                Runnable onRun,
                AsyncQueryProgressState progressState
        ) {
            this.circuitBreaker = circuitBreaker;
            this.doneCounter = doneCounter;
            this.frameRowCount = frameRowCount;
            this.onRun = onRun;
            this.progressState = progressState;
        }

        @Override
        public void abort(boolean isStarted) {
            try {
                circuitBreaker.cancel();
            } finally {
                doneCounter.incrementAndGet();
            }
        }

        @Override
        public SqlExecutionCircuitBreaker getCircuitBreaker() {
            return circuitBreaker;
        }

        @Override
        public long getFrameRowCount() {
            return frameRowCount;
        }

        @Override
        public AsyncQueryProgressState getProgressState() {
            return progressState;
        }

        @Override
        public void runDetached(int workerId) {
            observedSignal = SuspensionScope.getCancellationSignal();
            observedGeneration = SuspensionScope.getCancellationSignalGeneration();
            if (onRun != null) {
                onRun.run();
            }
            doneCounter.incrementAndGet();
        }
    }
}
