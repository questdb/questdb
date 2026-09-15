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

import io.questdb.DefaultFactoryProvider;
import io.questdb.FactoryProvider;
import io.questdb.PropertyKey;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoConfigurationWrapper;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerWrapper;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.async.PageFrameReduceDispatcher;
import io.questdb.cairo.sql.async.PageFrameReduceJob;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.cairo.sql.async.UnorderedPageFrameReduceJob;
import io.questdb.cairo.sql.async.UnorderedPageFrameReduceTask;
import io.questdb.cairo.sql.async.UnorderedPageFrameSequence;
import io.questdb.cairo.sql.async.WorkStealingStrategy;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.table.AsyncFilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncGroupByRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncJitFilteredRecordCursorFactory;
import io.questdb.mp.MCSequence;
import io.questdb.mp.MPSequence;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.mp.continuation.Fiber;
import io.questdb.mp.continuation.FiberCancellationSignal;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.FiberRuntimeState;
import io.questdb.mp.continuation.FiberTask;
import io.questdb.mp.continuation.FiberWaitCoordinator;
import io.questdb.mp.continuation.FiberWalWaitQueue;
import io.questdb.mp.continuation.FiberWalWaitRegistration;
import io.questdb.mp.continuation.LaunchResult;
import io.questdb.mp.continuation.SourceRegistrationResult;
import io.questdb.mp.continuation.SuspensionScope;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class PageFrameReduceDispatcherTest extends AbstractCairoTest {
    @Test
    public void testFiberTaskPoolLimitsFollowRuntimeConfiguration() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                    engine,
                    engine.getMessageBus(),
                    runtime
            );
            try {
                Assert.assertEquals(1, dispatcher.getTaskCapacity());
                Assert.assertEquals(1, dispatcher.getTaskMaxRetainedCount());

                runtime.updateConfiguration(4, 2, 7);
                Assert.assertEquals(4, dispatcher.getTaskCapacity());
                Assert.assertEquals(2, dispatcher.getTaskMaxRetainedCount());

                runtime.updateConfiguration(1, 4, 3);
                Assert.assertEquals(1, dispatcher.getTaskCapacity());
                Assert.assertEquals(1, dispatcher.getTaskMaxRetainedCount());
            } finally {
                close(runtime);
                Misc.free(dispatcher);
            }
        });
    }

    @Test
    public void testOrderedProducerDoesNotEnterTaskPoolMonitor() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final RingQueue<PageFrameReduceTask> queue = new RingQueue<>(
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    1
            );
            final MPSequence pubSeq = new MPSequence(queue.getCycle());
            final ClaimNotifyingMCSequence subSeq = new ClaimNotifyingMCSequence(queue.getCycle());
            pubSeq.then(subSeq).then(pubSeq);
            final PageFrameSequence<StatefulAtom> frameSequence = new PageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    (_, _, _, _, _) -> {
                    },
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    1,
                    PageFrameReduceTask.TYPE_FILTER
            ) {
                @Override
                public SqlExecutionCircuitBreaker getCircuitBreaker() {
                    return SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
                }
            };
            final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                    engine,
                    engine.getMessageBus(),
                    runtime
            );
            final AtomicReference<Throwable> failure = new AtomicReference<>();
            final CountDownLatch consumerDone = new CountDownLatch(1);
            final Thread consumer = new Thread(() -> {
                try {
                    Assert.assertFalse(dispatcher.consumeOrdered(-1, queue, subSeq, null));
                } catch (Throwable th) {
                    failure.set(th);
                } finally {
                    consumerDone.countDown();
                }
            });
            try {
                final long cursor = pubSeq.next();
                Assert.assertTrue(cursor > -1);
                queue.get(cursor).of(frameSequence, 0, false);
                pubSeq.done(cursor);

                dispatcher.runWithTaskPoolLockedForTesting(() -> {
                    consumer.start();
                    try {
                        Assert.assertTrue("consumer did not claim the cursor", subSeq.awaitClaim());
                        Assert.assertTrue(
                                "ordered producer entered the task-pool monitor after claiming the cursor",
                                consumerDone.await(5, TimeUnit.SECONDS)
                        );
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(e);
                    }
                });
                consumer.join(5_000);
                Assert.assertFalse("consumer did not return", consumer.isAlive());
                Assert.assertNull(failure.get());
                Assert.assertEquals(1, runtime.drain(1));
                Assert.assertEquals(1, frameSequence.getReduceFinishedCounter().get());
            } finally {
                consumer.join(5_000);
                runtime.drain(8);
                close(runtime);
                Misc.free(dispatcher);
                Misc.free(frameSequence);
                Misc.free(queue);
            }
        });
    }

    @Test
    public void testOrderedTaskCreationFailureCompletesOwnership() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final RingQueue<PageFrameReduceTask> queue = new RingQueue<>(
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    1
            );
            final MPSequence pubSeq = new MPSequence(queue.getCycle());
            final MCSequence subSeq = new MCSequence(queue.getCycle());
            pubSeq.then(subSeq).then(pubSeq);
            final PageFrameSequence<StatefulAtom> frameSequence = new PageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    (_, _, _, _, _) -> {
                    },
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    1,
                    PageFrameReduceTask.TYPE_FILTER
            ) {
                @Override
                public SqlExecutionCircuitBreaker getCircuitBreaker() {
                    return SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
                }
            };
            final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                    engine,
                    engine.getMessageBus(),
                    runtime
            );
            final RuntimeException injected = new RuntimeException("injected page-frame task creation failure");
            try {
                circuitBreakerConfiguration = failingCircuitBreakerConfiguration(injected);
                try {
                    runOrdered(dispatcher, frameSequence, pubSeq, queue, subSeq);
                    Assert.fail("expected injected task creation failure");
                } catch (RuntimeException th) {
                    Assert.assertSame(injected, th);
                } finally {
                    circuitBreakerConfiguration = null;
                }

                Assert.assertEquals(0, subSeq.current());
                Assert.assertEquals(1, frameSequence.getReduceFinishedCounter().get());
                Assert.assertEquals(0, dispatcher.getCreatedTaskCount());
                Assert.assertEquals(0, runtime.getOutstandingTaskCount());
                final Fiber fiber = runtime.tryReserveFiber();
                Assert.assertNotNull(fiber);
                runtime.releaseReservedFiber(fiber, fiber.getReservationEpoch());

                runOrdered(dispatcher, frameSequence, pubSeq, queue, subSeq);
                Assert.assertEquals(1, dispatcher.getCreatedTaskCount());
                Assert.assertEquals(2, frameSequence.getReduceFinishedCounter().get());
            } finally {
                circuitBreakerConfiguration = null;
                close(runtime);
                Misc.free(dispatcher);
                Misc.free(frameSequence);
                Misc.free(queue);
            }
        });
    }

    @Test
    public void testUnorderedTaskCreationFailureCompletesOwnership() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final RingQueue<UnorderedPageFrameReduceTask> queue = new RingQueue<>(
                    UnorderedPageFrameReduceTask::new,
                    1
            );
            final MPSequence pubSeq = new MPSequence(queue.getCycle());
            final MCSequence subSeq = new MCSequence(queue.getCycle());
            pubSeq.then(subSeq).then(pubSeq);
            final UnorderedPageFrameSequence<StatefulAtom> frameSequence = new UnorderedPageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    (_, _, _, _, _, _) -> {
                    },
                    1
            ) {
                @Override
                public SqlExecutionCircuitBreaker getCircuitBreaker() {
                    return SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
                }
            };
            final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                    engine,
                    engine.getMessageBus(),
                    runtime
            );
            final RuntimeException injected = new RuntimeException("injected page-frame task creation failure");
            try {
                final long doneBefore = frameSequence.getDoneLatch().getCount();
                circuitBreakerConfiguration = failingCircuitBreakerConfiguration(injected);
                try {
                    runUnordered(dispatcher, frameSequence, pubSeq, queue, subSeq);
                    Assert.fail("expected injected task creation failure");
                } catch (RuntimeException th) {
                    Assert.assertSame(injected, th);
                } finally {
                    circuitBreakerConfiguration = null;
                }

                Assert.assertEquals(0, subSeq.current());
                Assert.assertEquals(doneBefore - 1, frameSequence.getDoneLatch().getCount());
                Assert.assertEquals(0, dispatcher.getCreatedTaskCount());
                Assert.assertEquals(0, runtime.getOutstandingTaskCount());
                final Fiber fiber = runtime.tryReserveFiber();
                Assert.assertNotNull(fiber);
                runtime.releaseReservedFiber(fiber, fiber.getReservationEpoch());

                runUnordered(dispatcher, frameSequence, pubSeq, queue, subSeq);
                Assert.assertEquals(1, dispatcher.getCreatedTaskCount());
                Assert.assertEquals(doneBefore - 2, frameSequence.getDoneLatch().getCount());
            } finally {
                circuitBreakerConfiguration = null;
                close(runtime);
                Misc.free(dispatcher);
                Misc.free(frameSequence);
                Misc.free(queue);
            }
        });
    }

    @Test
    public void testTaskPoolAcquisitionFailureAndDoubleRelease() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                    engine,
                    engine.getMessageBus(),
                    runtime
            );
            final RuntimeException injected = new RuntimeException("injected page-frame task creation failure");
            try {
                circuitBreakerConfiguration = failingCircuitBreakerConfiguration(injected);
                try {
                    dispatcher.acquireTaskLeaseForTesting();
                    Assert.fail("expected injected task creation failure");
                } catch (RuntimeException th) {
                    Assert.assertSame(injected, th);
                } finally {
                    circuitBreakerConfiguration = null;
                }

                final boolean isRawLeaseGranted = dispatcher.tryLeaseTaskForTesting();
                try {
                    Assert.assertTrue(isRawLeaseGranted);
                } finally {
                    dispatcher.releaseTaskLeaseForTesting();
                }

                final PageFrameReduceDispatcher.TaskLeaseForTesting taskLease =
                        dispatcher.acquireTaskLeaseForTesting();
                taskLease.release();
                try {
                    taskLease.release();
                    Assert.fail("expected repeated task lease release to fail");
                } catch (IllegalStateException e) {
                    TestUtils.assertContains(e.getMessage(), "already released");
                }
            } finally {
                circuitBreakerConfiguration = null;
                close(runtime);
                Misc.free(dispatcher);
            }
        });
    }

    @Test
    public void testTaskPoolReleaseRacingCloseDoesNotRetainTask() throws Exception {
        assertMemoryLeak(() -> {
            for (int i = 0; i < 128; i++) {
                final FiberRuntime runtime = new FiberRuntime(1);
                final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                        engine,
                        engine.getMessageBus(),
                        runtime
                );
                try {
                    final PageFrameReduceDispatcher.TaskLeaseForTesting taskLease =
                            dispatcher.acquireTaskLeaseForTesting();

                    final CountDownLatch start = new CountDownLatch(1);
                    final AtomicReference<Throwable> closeFailure = new AtomicReference<>();
                    final AtomicReference<Throwable> releaseFailure = new AtomicReference<>();
                    final Thread closeThread = new Thread(() -> {
                        try {
                            start.await();
                            dispatcher.closeTaskPoolForTesting();
                        } catch (Throwable th) {
                            closeFailure.set(th);
                        }
                    });
                    final Thread releaseThread = new Thread(() -> {
                        try {
                            start.await();
                            taskLease.release();
                        } catch (Throwable th) {
                            releaseFailure.set(th);
                        }
                    });
                    closeThread.start();
                    releaseThread.start();
                    start.countDown();
                    closeThread.join(5_000);
                    releaseThread.join(5_000);

                    Assert.assertFalse("task-pool close did not return", closeThread.isAlive());
                    Assert.assertFalse("task release did not return", releaseThread.isAlive());
                    Assert.assertNull(releaseFailure.get());
                    if (closeFailure.get() != null) {
                        Assert.assertTrue(closeFailure.get() instanceof IllegalStateException);
                        TestUtils.assertContains(closeFailure.get().getMessage(), "closed with leased tasks");
                    }
                    Assert.assertEquals(0, dispatcher.getCreatedTaskCount());
                    Assert.assertFalse(dispatcher.tryLeaseTaskForTesting());
                } finally {
                    close(runtime);
                    Misc.free(dispatcher);
                }
            }
        });
    }

    @Test
    public void testBatchRowBudgetStopsDrainAfterFirstFrame() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final RingQueue<PageFrameReduceTask> queue = new RingQueue<>(
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    2
            );
            final MPSequence pubSeq = new MPSequence(queue.getCycle());
            final MCSequence subSeq = new MCSequence(queue.getCycle());
            pubSeq.then(subSeq).then(pubSeq);
            final PageFrameSequence<StatefulAtom> frameSequence = new PageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    (_, _, _, _, _) -> {
                    },
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    1,
                    PageFrameReduceTask.TYPE_FILTER
            ) {
                @Override
                public SqlExecutionCircuitBreaker getCircuitBreaker() {
                    return SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
                }

                @Override
                public long getFrameRowCount(int frameIndex) {
                    return 1_000;
                }
            };
            final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                    engine,
                    engine.getMessageBus(),
                    runtime
            );
            try {
                dispatcher.setBatchRowBudgetForTesting(1_000);
                for (int i = 0; i < 2; i++) {
                    final long cursor = pubSeq.next();
                    Assert.assertTrue(cursor > -1);
                    queue.get(cursor).of(frameSequence, i, false);
                    pubSeq.done(cursor);
                }

                // the first frame fills the budget, so the direct-mounted batch must not claim the second cursor
                Assert.assertFalse(dispatcher.consumeOrdered(0, queue, subSeq, null));
                Assert.assertEquals(0, subSeq.current());
                Assert.assertEquals(1, frameSequence.getReduceFinishedCounter().get());

                Assert.assertFalse(dispatcher.consumeOrdered(0, queue, subSeq, null));
                Assert.assertEquals(1, subSeq.current());
                Assert.assertEquals(2, frameSequence.getReduceFinishedCounter().get());

                dispatcher.setBatchRowBudgetForTesting(0);
                Assert.assertEquals(configuration.getSqlPageFrameMaxRows(), dispatcher.getBatchRowBudget());
            } finally {
                close(runtime);
                Misc.free(dispatcher);
                Misc.free(frameSequence);
                Misc.free(queue);
            }
        });
    }

    @Test
    public void testBatchUnorderedRowCountFailureCompletesOwnership() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final RingQueue<UnorderedPageFrameReduceTask> queue = new RingQueue<>(
                    UnorderedPageFrameReduceTask::new,
                    2
            );
            final MPSequence pubSeq = new MPSequence(queue.getCycle());
            final MCSequence subSeq = new MCSequence(queue.getCycle());
            pubSeq.then(subSeq).then(pubSeq);
            final UnorderedPageFrameSequence<StatefulAtom> frameSequence = new UnorderedPageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    (_, _, _, _, _, _) -> {
                    },
                    1
            ) {
                @Override
                public SqlExecutionCircuitBreaker getCircuitBreaker() {
                    return SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
                }

                @Override
                public long getFrameRowCount(int frameIndex) {
                    if (frameIndex == 1) {
                        throw new IllegalStateException("row count failure");
                    }
                    return 1;
                }
            };
            final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                    engine,
                    engine.getMessageBus(),
                    runtime
            );
            try {
                for (int i = 0; i < 2; i++) {
                    final long cursor = pubSeq.next();
                    Assert.assertTrue(cursor > -1);
                    queue.get(cursor).of(frameSequence, i);
                    pubSeq.done(cursor);
                }

                Assert.assertFalse(dispatcher.consumeUnordered(0, queue, subSeq, null));
                Assert.assertEquals(-2, frameSequence.getDoneLatch().getCount());
                Assert.assertEquals(0, runtime.getOutstandingTaskCount());
            } finally {
                close(runtime);
                Misc.free(dispatcher);
                Misc.free(frameSequence);
                Misc.free(queue);
            }
        });
    }

    @Test
    public void testBlockingScopeDoesNotParkGlobalProgressWait() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                    engine,
                    engine.getMessageBus(),
                    runtime
            );
            final long observedProgress = dispatcher.getProgressVersion();
            final AtomicInteger waitReason = new AtomicInteger(Integer.MIN_VALUE);
            final AtomicReference<Throwable> failure = new AtomicReference<>();
            final FiberTask task = new FiberTask() {
                @Override
                protected void onError(Throwable th) {
                    failure.set(th);
                }

                @Override
                protected boolean runStep() {
                    final SuspensionScope.Mode previousMode = SuspensionScope.enter(SuspensionScope.Mode.BLOCKING);
                    try {
                        waitReason.set(dispatcher.awaitProgress(observedProgress, null));
                    } finally {
                        SuspensionScope.restore(previousMode);
                    }
                    return true;
                }
            };
            try {
                Assert.assertSame(LaunchResult.LAUNCHED, runtime.launch(task));
                Assert.assertEquals(1, runtime.drain(1));

                Assert.assertEquals(0, runtime.getParkedFiberCount());
                Assert.assertNull(failure.get());
                Assert.assertEquals(FiberWaitCoordinator.REASON_NONE, waitReason.get());
                Assert.assertTrue(task.isDone());
            } finally {
                dispatcher.beginQuiesce();
                close(runtime);
                Misc.free(dispatcher);
            }
        });
    }

    @Test
    public void testBlockingScopeDoesNotParkSequenceProgressWait() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final PageFrameSequence<StatefulAtom> frameSequence = new PageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    (_, _, _, _, _) -> {
                    },
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    1,
                    PageFrameReduceTask.TYPE_FILTER
            ) {
                @Override
                public SqlExecutionCircuitBreaker getCircuitBreaker() {
                    return SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
                }
            };
            final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                    engine,
                    engine.getMessageBus(),
                    runtime
            );
            final long observedSequenceProgress = frameSequence.getProgressVersion();
            final long observedGlobalProgress = dispatcher.getProgressVersion();
            final AtomicInteger waitReason = new AtomicInteger(Integer.MIN_VALUE);
            final AtomicReference<Throwable> failure = new AtomicReference<>();
            final FiberTask task = new FiberTask() {
                @Override
                protected void onError(Throwable th) {
                    failure.set(th);
                }

                @Override
                protected boolean runStep() {
                    final SuspensionScope.Mode previousMode = SuspensionScope.enter(SuspensionScope.Mode.BLOCKING);
                    try {
                        waitReason.set(dispatcher.awaitProgress(
                                frameSequence,
                                observedSequenceProgress,
                                observedGlobalProgress,
                                null
                        ));
                    } finally {
                        SuspensionScope.restore(previousMode);
                    }
                    return true;
                }
            };
            try {
                Assert.assertSame(LaunchResult.LAUNCHED, runtime.launch(task));
                Assert.assertEquals(1, runtime.drain(1));

                Assert.assertEquals(0, runtime.getParkedFiberCount());
                Assert.assertNull(failure.get());
                Assert.assertEquals(FiberWaitCoordinator.REASON_NONE, waitReason.get());
                Assert.assertTrue(task.isDone());
            } finally {
                dispatcher.beginQuiesce();
                close(runtime);
                Misc.free(dispatcher);
                Misc.free(frameSequence);
            }
        });
    }

    @Test
    public void testBrokenConnectionCancellationPreservesReason() throws Exception {
        assertMemoryLeak(() -> {
            final PageFrameSequence<StatefulAtom> orderedFrameSequence = new PageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    (_, _, _, _, _) -> {
                    },
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    1,
                    PageFrameReduceTask.TYPE_FILTER
            ) {
                @Override
                public SqlExecutionCircuitBreaker getCircuitBreaker() {
                    return SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
                }
            };
            final UnorderedPageFrameSequence<StatefulAtom> unorderedFrameSequence = new UnorderedPageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    (_, _, _, _, _, _) -> {
                    },
                    1
            ) {
                @Override
                public SqlExecutionCircuitBreaker getCircuitBreaker() {
                    return SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
                }
            };
            try {
                orderedFrameSequence.cancel(SqlExecutionCircuitBreaker.STATE_BROKEN_CONNECTION);
                orderedFrameSequence.cancel(SqlExecutionCircuitBreaker.STATE_TIMEOUT);
                assertBrokenConnection(orderedFrameSequence.buildInterruptionException());

                unorderedFrameSequence.cancel(SqlExecutionCircuitBreaker.STATE_BROKEN_CONNECTION);
                unorderedFrameSequence.cancel(SqlExecutionCircuitBreaker.STATE_TIMEOUT);
                assertBrokenConnection(unorderedFrameSequence.buildInterruptionException());
            } finally {
                Misc.free(orderedFrameSequence);
                Misc.free(unorderedFrameSequence);
            }
        });
    }

    @Test
    public void testBrokenConnectionFromReducerPreservesReason() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final RingQueue<PageFrameReduceTask> orderedQueue = new RingQueue<>(
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    1
            );
            final MPSequence orderedPubSeq = new MPSequence(orderedQueue.getCycle());
            final MCSequence orderedSubSeq = new MCSequence(orderedQueue.getCycle());
            orderedPubSeq.then(orderedSubSeq).then(orderedPubSeq);
            final PageFrameSequence<StatefulAtom> orderedFrameSequence = new PageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    (_, _, _, _, _) -> {
                        throw CairoException.queryDisconnected(42);
                    },
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    1,
                    PageFrameReduceTask.TYPE_FILTER
            ) {
                @Override
                public SqlExecutionCircuitBreaker getCircuitBreaker() {
                    return SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
                }
            };
            final RingQueue<UnorderedPageFrameReduceTask> unorderedQueue = new RingQueue<>(
                    UnorderedPageFrameReduceTask::new,
                    1
            );
            final MPSequence unorderedPubSeq = new MPSequence(unorderedQueue.getCycle());
            final MCSequence unorderedSubSeq = new MCSequence(unorderedQueue.getCycle());
            unorderedPubSeq.then(unorderedSubSeq).then(unorderedPubSeq);
            final UnorderedPageFrameSequence<StatefulAtom> unorderedFrameSequence = new UnorderedPageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    (_, _, _, _, _, _) -> {
                        throw CairoException.queryDisconnected(43);
                    },
                    1
            ) {
                @Override
                public SqlExecutionCircuitBreaker getCircuitBreaker() {
                    return SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
                }
            };
            final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                    engine,
                    engine.getMessageBus(),
                    runtime
            );
            try {
                long cursor = orderedPubSeq.next();
                Assert.assertTrue(cursor > -1);
                orderedQueue.get(cursor).of(orderedFrameSequence, 0, false);
                orderedPubSeq.done(cursor);
                Assert.assertFalse(dispatcher.consumeOrdered(0, orderedQueue, orderedSubSeq, null));
                Assert.assertEquals(
                        SqlExecutionCircuitBreaker.STATE_BROKEN_CONNECTION,
                        orderedFrameSequence.getCancelReason()
                );
                assertBrokenConnection(orderedFrameSequence.buildInterruptionException());

                cursor = unorderedPubSeq.next();
                Assert.assertTrue(cursor > -1);
                unorderedQueue.get(cursor).of(unorderedFrameSequence, 0);
                unorderedPubSeq.done(cursor);
                Assert.assertFalse(dispatcher.consumeUnordered(0, unorderedQueue, unorderedSubSeq, null));
                Assert.assertEquals(
                        SqlExecutionCircuitBreaker.STATE_BROKEN_CONNECTION,
                        unorderedFrameSequence.getCancelReason()
                );
                assertBrokenConnection(unorderedFrameSequence.buildInterruptionException());
            } finally {
                close(runtime);
                Misc.free(dispatcher);
                Misc.free(orderedFrameSequence);
                Misc.free(unorderedFrameSequence);
                Misc.free(orderedQueue);
                Misc.free(unorderedQueue);
            }
        });
    }

    @Test
    public void testCloseUnregistersRuntimeListeners() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            try (PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                    engine,
                    engine.getMessageBus(),
                    runtime
            )) {
                Assert.assertEquals(1, runtime.getConfigurationListenerCountForTesting());
                Assert.assertEquals(1, runtime.getQuiesceListenerCountForTesting());

                dispatcher.close();

                Assert.assertEquals(0, runtime.getConfigurationListenerCountForTesting());
                Assert.assertEquals(0, runtime.getQuiesceListenerCountForTesting());
            } finally {
                close(runtime);
            }
        });
    }

    @Test
    public void testForeignProgressBeforeWaitDoesNotGetLost() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final PageFrameSequence<StatefulAtom> frameSequence = new PageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    (_, _, _, _, _) -> {
                    },
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    1,
                    PageFrameReduceTask.TYPE_FILTER
            ) {
                @Override
                public SqlExecutionCircuitBreaker getCircuitBreaker() {
                    return SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
                }
            };
            final PageFrameSequence<StatefulAtom> foreignFrameSequence = new PageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    (_, _, _, _, _) -> {
                    },
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    1,
                    PageFrameReduceTask.TYPE_FILTER
            ) {
                @Override
                public SqlExecutionCircuitBreaker getCircuitBreaker() {
                    return SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
                }
            };
            final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                    engine,
                    engine.getMessageBus(),
                    runtime
            );
            final long observedProgress = frameSequence.getProgressVersion();
            final long observedGlobalProgress = dispatcher.getProgressVersion();
            final AtomicReference<Throwable> failure = new AtomicReference<>();
            final FiberTask task = new FiberTask() {
                @Override
                protected void onError(Throwable th) {
                    failure.set(th);
                }

                @Override
                protected boolean runStep() {
                    Assert.assertEquals(
                            FiberWaitCoordinator.REASON_PROGRESS,
                            dispatcher.awaitProgress(
                                    frameSequence,
                                    observedProgress,
                                    observedGlobalProgress,
                                    null
                            )
                    );
                    return true;
                }
            };
            try {
                dispatcher.signalProgressForTesting(foreignFrameSequence);
                Assert.assertSame(LaunchResult.LAUNCHED, runtime.launch(task));
                Assert.assertEquals(1, runtime.drain(1));

                Assert.assertTrue(task.isDone());
                Assert.assertNull(failure.get());
                Assert.assertEquals(0, runtime.getParkedFiberCount());
            } finally {
                close(runtime);
                Misc.free(dispatcher);
                Misc.free(foreignFrameSequence);
                Misc.free(frameSequence);
            }
        });
    }

    @Test
    public void testForeignRuntimeFiberOwnerDispatchesInsteadOfLocalReduce() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE tab AS (
                        SELECT
                            x,
                            x::varchar AS k,
                            timestamp_sequence(0, 1_000_000) AS ts
                        FROM long_sequence(1_000)
                    ) TIMESTAMP(ts)
                    """);
            drainWalQueue();
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);

            final FiberRuntime ownerRuntime = new FiberRuntime(1);
            final FiberRuntime queryRuntime = new FiberRuntime(4);
            final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                    engine,
                    engine.getMessageBus(),
                    queryRuntime
            );
            try {
                engine.getMessageBus().setPageFrameReduceDispatcher(dispatcher);
                final AtomicReference<Throwable> failure = new AtomicReference<>();
                final AtomicInteger rowCount = new AtomicInteger();
                try (
                        SqlCompiler compiler = engine.getSqlCompiler();
                        RecordCursorFactory factory = compiler.compile("SELECT * FROM tab WHERE x > 0", sqlExecutionContext).getRecordCursorFactory()
                ) {
                    TestUtils.assertFactoryInTree(factory, AsyncFilteredRecordCursorFactory.class);
                    final FiberTask ownerTask = new FiberTask() {
                        @Override
                        protected void onError(Throwable th) {
                            failure.set(th);
                        }

                        @Override
                        protected boolean runStep() {
                            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                                while (cursor.hasNext()) {
                                    rowCount.incrementAndGet();
                                }
                            } catch (SqlException e) {
                                throw new AssertionError(e);
                            }
                            return true;
                        }
                    };

                    final int shardCount = engine.getMessageBus().getPageFrameReduceShardCount();
                    final LongList publicationCursors = new LongList(shardCount);
                    for (int shard = 0; shard < shardCount; shard++) {
                        publicationCursors.add(engine.getMessageBus().getPageFrameReducePubSeq(shard).current());
                    }

                    Assert.assertSame(LaunchResult.LAUNCHED, ownerRuntime.launch(ownerTask));
                    final long deadline = System.nanoTime() + 5_000_000_000L;
                    while (!ownerTask.isDone() && System.nanoTime() < deadline) {
                        ownerRuntime.drain(8);
                        for (int shard = 0; shard < shardCount; shard++) {
                            dispatcher.consumeOrdered(
                                    -1,
                                    engine.getMessageBus().getPageFrameReduceQueue(shard),
                                    engine.getMessageBus().getPageFrameReduceSubSeq(shard),
                                    null
                            );
                        }
                        queryRuntime.drain(8);
                    }

                    Assert.assertTrue(ownerTask.isDone());
                    Assert.assertNull(failure.get());
                    Assert.assertEquals(1000, rowCount.get());
                    // a foreign-runtime fiber owner publishes into the dispatcher's queue;
                    // the same-runtime twin asserts the inverse (publication cursors frozen)
                    boolean hasPublishedFrame = false;
                    for (int shard = 0; shard < shardCount; shard++) {
                        hasPublishedFrame |= engine.getMessageBus().getPageFrameReducePubSeq(shard).current()
                                > publicationCursors.getQuick(shard);
                    }
                    Assert.assertTrue(hasPublishedFrame);
                    Assert.assertEquals(0, ownerRuntime.getOutstandingTaskCount());
                    Assert.assertEquals(0, queryRuntime.getOutstandingTaskCount());
                }
            } finally {
                close(ownerRuntime);
                close(queryRuntime);
                Misc.free(dispatcher);
            }
        });
    }

    @Test
    public void testForeignRuntimeOwnerCanPublish() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime dispatcherRuntime = new FiberRuntime(1);
            final FiberRuntime ownerRuntime = new FiberRuntime(1);
            final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                    engine,
                    engine.getMessageBus(),
                    dispatcherRuntime
            );
            final AtomicReference<Throwable> failure = new AtomicReference<>();
            final FiberTask ownerTask = new FiberTask() {
                @Override
                protected void onError(Throwable th) {
                    failure.set(th);
                }

                @Override
                protected boolean runStep() {
                    Assert.assertTrue(dispatcher.tryAcquirePublication());
                    dispatcher.releasePublication();
                    return true;
                }
            };
            try {
                Assert.assertSame(LaunchResult.LAUNCHED, ownerRuntime.launch(ownerTask));
                Assert.assertEquals(1, ownerRuntime.drain(1));

                Assert.assertTrue(ownerTask.isDone());
                Assert.assertNull(failure.get());
                Assert.assertEquals(0, ownerRuntime.getOutstandingTaskCount());
            } finally {
                close(ownerRuntime);
                close(dispatcherRuntime);
                Misc.free(dispatcher);
            }
        });
    }

    @Test
    public void testInterruptionFromReducerOverridesNormalEarlyExit() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final FiberWalWaitQueue waitQueue = new FiberWalWaitQueue();
            final FiberCancellationSignal queryCancellationSignal = new FiberCancellationSignal();
            final AtomicBooleanCircuitBreaker circuitBreaker = new AtomicBooleanCircuitBreaker(engine);
            circuitBreaker.setCancelledFlag(
                    queryCancellationSignal,
                    queryCancellationSignal.getGeneration()
            );
            final RingQueue<PageFrameReduceTask> queue = new RingQueue<>(
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    1
            );
            final MPSequence pubSeq = new MPSequence(queue.getCycle());
            final MCSequence subSeq = new MCSequence(queue.getCycle());
            pubSeq.then(subSeq).then(pubSeq);
            final PageFrameSequence<StatefulAtom> frameSequence = new PageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    (_, _, _, _, _) -> {
                        final int reason = parkWithCancellation(waitQueue);
                        if (reason == FiberWaitCoordinator.REASON_CANCEL) {
                            throw CairoException.queryCancelled();
                        }
                        if (reason != FiberWaitCoordinator.REASON_WAL) {
                            throw new IllegalStateException("unexpected wait reason [reason=" + reason + ']');
                        }
                    },
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    1,
                    PageFrameReduceTask.TYPE_FILTER
            ) {
                @Override
                public SqlExecutionCircuitBreaker getCircuitBreaker() {
                    return circuitBreaker;
                }
            };
            final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                    engine,
                    engine.getMessageBus(),
                    runtime
            );
            try {
                final long cursor = pubSeq.next();
                Assert.assertTrue(cursor > -1);
                queue.get(cursor).of(frameSequence, 0, false);
                pubSeq.done(cursor);

                Assert.assertFalse(dispatcher.consumeOrdered(0, queue, subSeq, null));
                Assert.assertEquals(1, runtime.getParkedFiberCount());

                frameSequence.cancel(SqlExecutionCircuitBreaker.STATE_OK);
                Assert.assertEquals(0, runtime.drain(1));
                Assert.assertEquals(SqlExecutionCircuitBreaker.STATE_OK, frameSequence.getCancelReason());

                circuitBreaker.cancel();
                Assert.assertEquals(1, runtime.drain(1));

                Assert.assertEquals(
                        SqlExecutionCircuitBreaker.STATE_CANCELLED,
                        frameSequence.getCancelReason()
                );
                Assert.assertTrue(queue.get(cursor).isCancelled());
                Assert.assertEquals(1, frameSequence.getReduceFinishedCounter().get());
                Assert.assertEquals(0, runtime.getOutstandingTaskCount());
                Assert.assertEquals(0, runtime.getParkedFiberCount());
            } finally {
                waitQueue.fire(1, false);
                close(runtime);
                Misc.free(dispatcher);
                Misc.free(frameSequence);
                Misc.free(queue);
            }
        });
    }

    @Test
    public void testInterruptionOverridesNormalEarlyExit() throws Exception {
        assertMemoryLeak(() -> {
            final PageFrameSequence<StatefulAtom> frameSequence = new PageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    (_, _, _, _, _) -> {
                    },
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    1,
                    PageFrameReduceTask.TYPE_FILTER
            ) {
                @Override
                public SqlExecutionCircuitBreaker getCircuitBreaker() {
                    return SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
                }
            };
            try {
                frameSequence.cancel(SqlExecutionCircuitBreaker.STATE_OK);
                Assert.assertEquals(
                        SqlExecutionCircuitBreaker.STATE_OK,
                        frameSequence.getCancelReason()
                );
                Assert.assertFalse(frameSequence.getCancellationSignal().get());

                frameSequence.cancel(SqlExecutionCircuitBreaker.STATE_BROKEN_CONNECTION);
                Assert.assertEquals(
                        SqlExecutionCircuitBreaker.STATE_BROKEN_CONNECTION,
                        frameSequence.getCancelReason()
                );
                assertBrokenConnection(frameSequence.buildInterruptionException());
            } finally {
                Misc.free(frameSequence);
            }
        });
    }

    @Test
    public void testNormalEarlyExitDoesNotCancelParkedReducer() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final FiberWalWaitQueue waitQueue = new FiberWalWaitQueue();
            final RingQueue<PageFrameReduceTask> queue = new RingQueue<>(
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    1
            );
            final MPSequence pubSeq = new MPSequence(queue.getCycle());
            final MCSequence subSeq = new MCSequence(queue.getCycle());
            pubSeq.then(subSeq).then(pubSeq);
            final PageFrameSequence<StatefulAtom> frameSequence = new PageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    (_, _, _, _, _) -> park(waitQueue),
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    1,
                    PageFrameReduceTask.TYPE_FILTER
            ) {
                @Override
                public SqlExecutionCircuitBreaker getCircuitBreaker() {
                    return SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
                }
            };
            final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                    engine,
                    engine.getMessageBus(),
                    runtime
            );
            try {
                final long cursor = pubSeq.next();
                Assert.assertTrue(cursor > -1);
                queue.get(cursor).of(frameSequence, 0, false);
                pubSeq.done(cursor);

                Assert.assertFalse(dispatcher.consumeOrdered(0, queue, subSeq, null));
                Assert.assertEquals(1, runtime.getParkedFiberCount());

                frameSequence.cancel(SqlExecutionCircuitBreaker.STATE_OK);
                Assert.assertEquals(0, runtime.drain(1));
                Assert.assertEquals(1, runtime.getParkedFiberCount());

                waitQueue.fire(1, false);
                Assert.assertEquals(1, runtime.drain(1));

                Assert.assertEquals(SqlExecutionCircuitBreaker.STATE_OK, frameSequence.getCancelReason());
                Assert.assertEquals(1, frameSequence.getReduceFinishedCounter().get());
                Assert.assertEquals(0, runtime.getOutstandingTaskCount());
                Assert.assertEquals(0, runtime.getParkedFiberCount());
            } finally {
                waitQueue.fire(1, false);
                close(runtime);
                Misc.free(dispatcher);
                Misc.free(frameSequence);
                Misc.free(queue);
            }
        });
    }

    @Test
    public void testNullSequenceCollectionSignalsGlobalProgress() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE null_sequence_progress AS (SELECT x FROM long_sequence(1))");
            final FiberRuntime runtime = new FiberRuntime(1);
            final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                    engine,
                    engine.getMessageBus(),
                    runtime
            );
            final PageFrameReduceDispatcher previousDispatcher = engine.getMessageBus().getPageFrameReduceDispatcher();
            engine.getMessageBus().setPageFrameReduceDispatcher(dispatcher);
            final PageFrameSequence<StatefulAtom> frameSequence = new PageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    (_, _, _, _, _) -> {
                    },
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    1,
                    PageFrameReduceTask.TYPE_FILTER
            );
            final SCSequence collectSubSeq = new SCSequence();
            try (RecordCursorFactory factory = select("SELECT * FROM null_sequence_progress")) {
                frameSequence.of(
                        factory,
                        sqlExecutionContext,
                        collectSubSeq,
                        PartitionFrameCursorFactory.ORDER_ASC
                );
                frameSequence.prepareForDispatch();
                final int shard = frameSequence.getShard();
                final RingQueue<PageFrameReduceTask> queue = engine.getMessageBus().getPageFrameReduceQueue(shard);
                final MPSequence pubSeq = engine.getMessageBus().getPageFrameReducePubSeq(shard);
                final MCSequence reduceSubSeq = engine.getMessageBus().getPageFrameReduceSubSeq(shard);
                final long cursor = pubSeq.next();
                Assert.assertTrue(cursor > -1);
                Assert.assertNull(queue.get(cursor).getFrameSequence());
                pubSeq.done(cursor);
                Assert.assertEquals(cursor, reduceSubSeq.next());
                reduceSubSeq.done(cursor);
                Assert.assertEquals(cursor, collectSubSeq.next());

                frameSequence.cancel(SqlExecutionCircuitBreaker.STATE_CANCELLED);
                final long observedProgress = dispatcher.getProgressVersion();
                try {
                    frameSequence.next();
                    Assert.fail();
                } catch (CairoException e) {
                    Assert.assertTrue(e.isCancellation());
                }
                Assert.assertEquals(observedProgress + 1, dispatcher.getProgressVersion());
            } finally {
                engine.getMessageBus().setPageFrameReduceDispatcher(previousDispatcher);
                close(runtime);
                Misc.free(dispatcher);
                Misc.free(frameSequence);
            }
        });
    }

    @Test
    public void testOrderedCompletionWakesProgressWaiter() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime dispatcherRuntime = new FiberRuntime(1);
            final FiberRuntime ownerRuntime = new FiberRuntime(1);
            final FiberWalWaitQueue waitQueue = new FiberWalWaitQueue();
            final RingQueue<PageFrameReduceTask> queue = new RingQueue<>(
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    1
            );
            final MPSequence pubSeq = new MPSequence(queue.getCycle());
            final MCSequence subSeq = new MCSequence(queue.getCycle());
            pubSeq.then(subSeq).then(pubSeq);
            final PageFrameSequence<StatefulAtom> frameSequence = new PageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    (_, _, _, _, _) -> park(waitQueue),
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    1,
                    PageFrameReduceTask.TYPE_FILTER
            ) {
                @Override
                public SqlExecutionCircuitBreaker getCircuitBreaker() {
                    return SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
                }
            };
            final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                    engine,
                    engine.getMessageBus(),
                    dispatcherRuntime
            );
            final AtomicReference<Throwable> failure = new AtomicReference<>();
            final FiberTask ownerTask = new FiberTask() {
                @Override
                protected void onError(Throwable th) {
                    failure.set(th);
                }

                @Override
                protected boolean runStep() {
                    final long observedProgress = frameSequence.getProgressVersion();
                    final long observedGlobalProgress = dispatcher.getProgressVersion();
                    while (true) {
                        final int reason = dispatcher.awaitProgress(
                                frameSequence,
                                observedProgress,
                                observedGlobalProgress,
                                null
                        );
                        if (reason == FiberWaitCoordinator.REASON_PROGRESS) {
                            return true;
                        }
                        if (reason != FiberWaitCoordinator.REASON_TIMER) {
                            throw new IllegalStateException("unexpected progress wait reason [reason=" + reason + ']');
                        }
                    }
                }
            };
            try {
                final long cursor = pubSeq.next();
                Assert.assertTrue(cursor > -1);
                queue.get(cursor).of(frameSequence, 0, false);
                pubSeq.done(cursor);

                Assert.assertFalse(dispatcher.consumeOrdered(0, queue, subSeq, null));
                Assert.assertEquals(1, dispatcherRuntime.getParkedFiberCount());
                Assert.assertSame(LaunchResult.LAUNCHED, ownerRuntime.launch(ownerTask));
                Assert.assertEquals(1, ownerRuntime.drain(1));
                Assert.assertFalse(ownerTask.isDone());
                Assert.assertEquals(1, ownerRuntime.getOutstandingTaskCount());

                waitQueue.fire(1, false);
                Assert.assertEquals(1, dispatcherRuntime.drain(1));
                Assert.assertEquals(1, ownerRuntime.drain(1));

                Assert.assertTrue(ownerTask.isDone());
                Assert.assertNull(failure.get());
                Assert.assertEquals(0, ownerRuntime.getOutstandingTaskCount());
            } finally {
                waitQueue.fire(1, false);
                dispatcherRuntime.drain(1);
                ownerRuntime.drain(1);
                close(ownerRuntime);
                close(dispatcherRuntime);
                Misc.free(dispatcher);
                Misc.free(frameSequence);
                Misc.free(queue);
            }
        });
    }

    @Test
    public void testOrderedOwnerInlineStealsForeignTaskAcrossSuspend() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final FiberWalWaitQueue waitQueue = new FiberWalWaitQueue();
            final RingQueue<PageFrameReduceTask> queue = new RingQueue<>(
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    1
            );
            final MPSequence pubSeq = new MPSequence(queue.getCycle());
            final MCSequence subSeq = new MCSequence(queue.getCycle());
            pubSeq.then(subSeq).then(pubSeq);
            final AtomicReference<Fiber> ownerFiber = new AtomicReference<>();
            final AtomicReference<Fiber> reducerFiber = new AtomicReference<>();
            final AtomicReference<PageFrameSequence<?>> stealingSequence = new AtomicReference<>();
            final PageFrameSequence<StatefulAtom> ownerSequence = new PageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    (_, _, _, _, _) -> {
                    },
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    1,
                    PageFrameReduceTask.TYPE_FILTER
            ) {
                @Override
                public SqlExecutionCircuitBreaker getCircuitBreaker() {
                    return SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
                }
            };
            final PageFrameSequence<StatefulAtom> foreignSequence = new PageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    (_, _, _, _, stealingFrameSequence) -> {
                        reducerFiber.set(Fiber.current());
                        stealingSequence.set(stealingFrameSequence);
                        park(waitQueue);
                    },
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    1,
                    PageFrameReduceTask.TYPE_FILTER
            ) {
                @Override
                public SqlExecutionCircuitBreaker getCircuitBreaker() {
                    return SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
                }
            };
            final PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
            final SqlExecutionCircuitBreakerWrapper circuitBreaker = new SqlExecutionCircuitBreakerWrapper(
                    engine,
                    configuration.getCircuitBreakerConfiguration()
            );
            final AtomicReference<Throwable> failure = new AtomicReference<>();
            final FiberTask ownerTask = new FiberTask() {
                @Override
                protected void onError(Throwable th) {
                    failure.set(th);
                }

                @Override
                protected boolean runStep() {
                    ownerFiber.set(Fiber.current());
                    Assert.assertFalse(PageFrameReduceJob.consumeQueue(
                            queue,
                            subSeq,
                            record,
                            circuitBreaker,
                            ownerSequence
                    ));
                    return true;
                }
            };
            try {
                final long cursor = pubSeq.next();
                Assert.assertTrue(cursor > -1);
                queue.get(cursor).of(foreignSequence, 0, false);
                pubSeq.done(cursor);

                Assert.assertSame(LaunchResult.LAUNCHED, runtime.launch(ownerTask));
                Assert.assertEquals(1, runtime.drain(1));
                Assert.assertFalse(ownerTask.isDone());
                Assert.assertEquals(1, runtime.getParkedFiberCount());
                Assert.assertEquals(0, foreignSequence.getReduceFinishedCounter().get());
                Assert.assertEquals(-1, pubSeq.next());

                waitQueue.fire(1, false);
                Assert.assertEquals(1, runtime.drain(1));
                Assert.assertTrue(ownerTask.isDone());
                Assert.assertNull(failure.get());
                Assert.assertSame(ownerFiber.get(), reducerFiber.get());
                Assert.assertSame(ownerSequence, stealingSequence.get());
                Assert.assertEquals(1, foreignSequence.getReduceFinishedCounter().get());
            } finally {
                waitQueue.fire(1, false);
                close(runtime);
                Misc.free(circuitBreaker);
                Misc.free(record);
                Misc.free(foreignSequence);
                Misc.free(ownerSequence);
                Misc.free(queue);
            }
        });
    }

    @Test
    public void testOrderedOwnerInlineUsesForeignCancellationScope() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final FiberWalWaitQueue waitQueue = new FiberWalWaitQueue();
            final FiberCancellationSignal foreignCancellation = new FiberCancellationSignal();
            final FiberCancellationSignal ownerCancellation = new FiberCancellationSignal();
            final AtomicBooleanCircuitBreaker foreignCircuitBreaker = new AtomicBooleanCircuitBreaker(engine);
            foreignCircuitBreaker.setCancelledFlag(foreignCancellation, foreignCancellation.getGeneration());
            final RingQueue<PageFrameReduceTask> queue = new RingQueue<>(
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    1
            );
            final MPSequence pubSeq = new MPSequence(queue.getCycle());
            final MCSequence subSeq = new MCSequence(queue.getCycle());
            pubSeq.then(subSeq).then(pubSeq);
            final PageFrameSequence<StatefulAtom> ownerSequence = new PageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    (_, _, _, _, _) -> {
                    },
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    1,
                    PageFrameReduceTask.TYPE_FILTER
            ) {
                @Override
                public SqlExecutionCircuitBreaker getCircuitBreaker() {
                    return SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
                }
            };
            final PageFrameSequence<StatefulAtom> foreignSequence = new PageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    (_, _, _, _, _) -> {
                        final int reason = parkWithCancellation(waitQueue);
                        if (reason == FiberWaitCoordinator.REASON_CANCEL) {
                            throw CairoException.queryCancelled();
                        }
                        if (reason != FiberWaitCoordinator.REASON_WAL) {
                            throw new IllegalStateException("unexpected wait reason [reason=" + reason + ']');
                        }
                    },
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    1,
                    PageFrameReduceTask.TYPE_FILTER
            ) {
                @Override
                public SqlExecutionCircuitBreaker getCircuitBreaker() {
                    return foreignCircuitBreaker;
                }
            };
            final PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
            final SqlExecutionCircuitBreakerWrapper circuitBreaker = new SqlExecutionCircuitBreakerWrapper(
                    engine,
                    configuration.getCircuitBreakerConfiguration()
            );
            final AtomicReference<Throwable> failure = new AtomicReference<>();
            final AtomicReference<FiberCancellationSignal> restoredCancellation = new AtomicReference<>();
            final FiberTask ownerTask = new FiberTask() {
                @Override
                public FiberCancellationSignal getCancellationSignal() {
                    return ownerCancellation;
                }

                @Override
                protected void onError(Throwable th) {
                    failure.set(th);
                }

                @Override
                protected boolean runStep() {
                    Assert.assertFalse(PageFrameReduceJob.consumeQueue(
                            queue,
                            subSeq,
                            record,
                            circuitBreaker,
                            ownerSequence
                    ));
                    restoredCancellation.set(SuspensionScope.getCancellationSignal());
                    return true;
                }
            };
            try {
                final long cursor = pubSeq.next();
                Assert.assertTrue(cursor > -1);
                queue.get(cursor).of(foreignSequence, 0, false);
                pubSeq.done(cursor);

                Assert.assertSame(LaunchResult.LAUNCHED, runtime.launch(ownerTask));
                Assert.assertEquals(1, runtime.drain(1));
                Assert.assertEquals(1, runtime.getParkedFiberCount());

                ownerCancellation.cancel();
                Assert.assertEquals(0, runtime.drain(1));
                Assert.assertFalse(ownerTask.isDone());

                foreignCancellation.cancel();
                Assert.assertEquals(1, runtime.drain(1));
                Assert.assertTrue(ownerTask.isDone());
                Assert.assertNull(failure.get());
                Assert.assertSame(ownerCancellation, restoredCancellation.get());
                Assert.assertEquals(
                        SqlExecutionCircuitBreaker.STATE_CANCELLED,
                        foreignSequence.getCancelReason()
                );
                Assert.assertEquals(1, foreignSequence.getReduceFinishedCounter().get());
                Assert.assertTrue(queue.get(0).hasError());
            } finally {
                waitQueue.fire(1, false);
                close(runtime);
                Misc.free(circuitBreaker);
                Misc.free(record);
                Misc.free(foreignSequence);
                Misc.free(ownerSequence);
                Misc.free(queue);
            }
        });
    }

    @Test
    public void testOrderedLaunchFailureTransfersFiberAndTaskOwnership() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime dispatcherRuntime = new FiberRuntime(1);
            final FiberRuntime ownerRuntime = new FiberRuntime(1);
            final RingQueue<PageFrameReduceTask> queue = new RingQueue<>(
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    1
            );
            final MPSequence pubSeq = new MPSequence(queue.getCycle());
            final MCSequence subSeq = new MCSequence(queue.getCycle());
            pubSeq.then(subSeq).then(pubSeq);
            final PageFrameSequence<StatefulAtom> failedFrameSequence = new PageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    (_, _, _, _, _) -> {
                    },
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    1,
                    PageFrameReduceTask.TYPE_FILTER
            ) {
                @Override
                public SqlExecutionCircuitBreaker getCircuitBreaker() {
                    return SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
                }
            };
            final PageFrameSequence<StatefulAtom> replacementFrameSequence = new PageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    (_, _, _, _, _) -> {
                    },
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    1,
                    PageFrameReduceTask.TYPE_FILTER
            ) {
                @Override
                public SqlExecutionCircuitBreaker getCircuitBreaker() {
                    return SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
                }
            };
            final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                    engine,
                    engine.getMessageBus(),
                    dispatcherRuntime
            );
            final AtomicReference<Throwable> failure = new AtomicReference<>();
            final FiberTask ownerTask = new FiberTask() {
                @Override
                protected void onError(Throwable th) {
                    failure.set(th);
                }

                @Override
                protected boolean runStep() {
                    runOrdered(dispatcher, failedFrameSequence, pubSeq, queue, subSeq);
                    return true;
                }
            };
            try {
                dispatcherRuntime.setRunQueueDepthForTesting(dispatcherRuntime.getRunQueueCapacity());
                Assert.assertSame(LaunchResult.LAUNCHED, ownerRuntime.launch(ownerTask));
                Assert.assertEquals(1, ownerRuntime.drain(1));

                Assert.assertTrue(ownerTask.isDone());
                Assert.assertNotNull(failure.get());
                TestUtils.assertContains(
                        failure.get().getMessage(),
                        "page frame fiber launch failed [result=TERMINAL]"
                );
                Assert.assertEquals(0, dispatcherRuntime.getOutstandingTaskCount());
                Assert.assertEquals(1, dispatcherRuntime.getCreatedFiberCount());
                Assert.assertEquals(1, dispatcher.getCreatedTaskCount());

                dispatcherRuntime.setRunQueueDepthForTesting(0);
                runOrdered(dispatcher, replacementFrameSequence, pubSeq, queue, subSeq);
                Assert.assertEquals(0, dispatcherRuntime.getOutstandingTaskCount());
                Assert.assertEquals(1, dispatcherRuntime.getCreatedFiberCount());
                Assert.assertEquals(1, dispatcher.getCreatedTaskCount());
                Assert.assertEquals(1, replacementFrameSequence.getReduceFinishedCounter().get());
            } finally {
                dispatcherRuntime.setRunQueueDepthForTesting(0);
                close(ownerRuntime);
                close(dispatcherRuntime);
                Misc.free(dispatcher);
                Misc.free(failedFrameSequence);
                Misc.free(replacementFrameSequence);
                Misc.free(queue);
            }
        });
    }

    @Test
    public void testOrderedNonIdleFiberTaskIsRetiredBeforeReuse() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final RingQueue<PageFrameReduceTask> queue = new RingQueue<>(
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    1
            );
            final MPSequence pubSeq = new MPSequence(queue.getCycle());
            final MCSequence subSeq = new MCSequence(queue.getCycle());
            pubSeq.then(subSeq).then(pubSeq);
            final PageFrameSequence<StatefulAtom> frameSequence = new PageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    (_, _, _, _, _) -> {
                    },
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    1,
                    PageFrameReduceTask.TYPE_FILTER
            ) {
                @Override
                public SqlExecutionCircuitBreaker getCircuitBreaker() {
                    return SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
                }
            };
            final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                    engine,
                    engine.getMessageBus(),
                    runtime
            );
            try {
                runOrdered(dispatcher, frameSequence, pubSeq, queue, subSeq);
                Assert.assertEquals(1, dispatcher.getCreatedTaskCount());
                Assert.assertEquals(1, frameSequence.getReduceFinishedCounter().get());

                dispatcher.setFreeTaskScheduleStateForTesting(FiberTask.STATE_IDLE, FiberTask.STATE_OWNED);
                try {
                    runOrdered(dispatcher, frameSequence, pubSeq, queue, subSeq);
                    Assert.fail("expected non-idle task launch failure");
                } catch (IllegalStateException e) {
                    TestUtils.assertContains(
                            e.getMessage(),
                            "page frame fiber launch failed [result=ALREADY_OWNED]"
                    );
                }
                Assert.assertEquals(0, runtime.getOutstandingTaskCount());
                Assert.assertEquals(0, dispatcher.getCreatedTaskCount());
                Assert.assertEquals(2, frameSequence.getReduceFinishedCounter().get());

                runOrdered(dispatcher, frameSequence, pubSeq, queue, subSeq);
                Assert.assertEquals(0, runtime.getOutstandingTaskCount());
                Assert.assertEquals(1, dispatcher.getCreatedTaskCount());
                Assert.assertEquals(3, frameSequence.getReduceFinishedCounter().get());

                dispatcher.close();
                Assert.assertEquals(0, dispatcher.getCreatedTaskCount());
            } finally {
                close(runtime);
                Misc.free(dispatcher);
                Misc.free(frameSequence);
                Misc.free(queue);
            }
        });
    }

    @Test
    public void testOrderedOwnerInlineReleasesPublicationBeforeSuspend() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE ordered_publication AS (SELECT x FROM long_sequence(1))");
            final FiberRuntime dispatcherRuntime = new FiberRuntime(1);
            final FiberRuntime ownerRuntime = new FiberRuntime(1);
            final FiberWalWaitQueue dispatcherWaitQueue = new FiberWalWaitQueue();
            final FiberWalWaitQueue reducerWaitQueue = new FiberWalWaitQueue();
            final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                    engine,
                    engine.getMessageBus(),
                    dispatcherRuntime
            );
            engine.getMessageBus().setPageFrameReduceDispatcher(dispatcher);
            final PageFrameSequence<StatefulAtom> frameSequence = new PageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    (_, _, _, _, _) -> park(reducerWaitQueue),
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    1,
                    PageFrameReduceTask.TYPE_FILTER
            );
            final AtomicReference<Throwable> ownerFailure = new AtomicReference<>();
            try (RecordCursorFactory factory = select("SELECT * FROM ordered_publication")) {
                frameSequence.of(
                        factory,
                        sqlExecutionContext,
                        new SCSequence(),
                        PartitionFrameCursorFactory.ORDER_ASC
                );
                frameSequence.prepareForDispatch();
                final FiberTask blockerTask = new FiberTask() {
                    @Override
                    protected boolean runStep() {
                        park(dispatcherWaitQueue);
                        return true;
                    }
                };
                final FiberTask ownerTask = new FiberTask() {
                    @Override
                    protected void onError(Throwable th) {
                        ownerFailure.set(th);
                    }

                    @Override
                    protected boolean runStep() {
                        final long cursor = frameSequence.next();
                        if (cursor > -1) {
                            frameSequence.collect(cursor, false);
                        }
                        return true;
                    }
                };

                Assert.assertSame(LaunchResult.LAUNCHED, dispatcherRuntime.launch(blockerTask));
                Assert.assertEquals(1, dispatcherRuntime.drain(1));
                Assert.assertEquals(1, dispatcherRuntime.getParkedFiberCount());

                Assert.assertSame(LaunchResult.LAUNCHED, ownerRuntime.launch(ownerTask));
                Assert.assertEquals(1, ownerRuntime.drain(1));
                Assert.assertEquals(1, ownerRuntime.getParkedFiberCount());
                Assert.assertEquals(0, dispatcher.getCreatedTaskCount());
                Assert.assertEquals(0, frameSequence.getReduceFinishedCounter().get());

                Assert.assertTrue(dispatcher.tryAcquirePublication());
                dispatcher.releasePublication();

                reducerWaitQueue.fire(1, false);
                Assert.assertEquals(1, ownerRuntime.drain(1));
                Assert.assertTrue(ownerTask.isDone());
                Assert.assertNull(ownerFailure.get());
                Assert.assertEquals(1, frameSequence.getReduceFinishedCounter().get());

                dispatcherWaitQueue.fire(1, false);
                Assert.assertEquals(1, dispatcherRuntime.drain(1));
            } finally {
                dispatcherWaitQueue.fire(1, false);
                reducerWaitQueue.fire(1, false);
                dispatcherRuntime.drain(8);
                close(dispatcherRuntime);
                ownerRuntime.drain(8);
                close(ownerRuntime);
                Misc.free(dispatcher);
                Misc.free(frameSequence);
            }
        });
    }

    @Test
    public void testOrderedQuiescingLaunchCleanupSurvivesCancellationFailure() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final RingQueue<PageFrameReduceTask> queue = new RingQueue<>(
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    1
            );
            final MPSequence pubSeq = new MPSequence(queue.getCycle());
            final MCSequence subSeq = new MCSequence(queue.getCycle()) {
                private boolean hasStartedQuiesce;

                @Override
                public long next() {
                    final long cursor = super.next();
                    if (cursor > -1 && !hasStartedQuiesce) {
                        hasStartedQuiesce = true;
                        runtime.beginQuiesce();
                    }
                    return cursor;
                }
            };
            pubSeq.then(subSeq).then(pubSeq);
            final PageFrameSequence<StatefulAtom> frameSequence = new PageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    (_, _, _, _, _) -> {
                    },
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    1,
                    PageFrameReduceTask.TYPE_FILTER
            ) {
                @Override
                public void cancel(int reason) {
                    super.cancel(reason);
                    throw new IllegalStateException("forced cancellation callback failure");
                }

                @Override
                public SqlExecutionCircuitBreaker getCircuitBreaker() {
                    return SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
                }
            };
            final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                    engine,
                    engine.getMessageBus(),
                    runtime
            );
            try {
                try {
                    runOrdered(dispatcher, frameSequence, pubSeq, queue, subSeq);
                    Assert.fail("expected cancellation callback failure");
                } catch (IllegalStateException e) {
                    TestUtils.assertContains(e.getMessage(), "forced cancellation callback failure");
                }

                Assert.assertEquals(0, runtime.getOutstandingTaskCount());
                Assert.assertEquals(1, runtime.getCreatedFiberCount());
                Assert.assertEquals(1, frameSequence.getReduceFinishedCounter().get());
                dispatcher.close();
                Assert.assertEquals(0, dispatcher.getCreatedTaskCount());
            } finally {
                close(runtime);
                Misc.free(dispatcher);
                Misc.free(frameSequence);
                Misc.free(queue);
            }
        });
    }

    @Test
    public void testOrderedOwnerInlinePreservesReducerError() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE ordered_error AS (SELECT x FROM long_sequence(1))");
            final FiberRuntime dispatcherRuntime = new FiberRuntime(1);
            final FiberRuntime ownerRuntime = new FiberRuntime(1);
            final AtomicReference<Throwable> ownerFailure = new AtomicReference<>();
            final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                    engine,
                    engine.getMessageBus(),
                    dispatcherRuntime
            );
            engine.getMessageBus().setPageFrameReduceDispatcher(dispatcher);
            final PageFrameSequence<StatefulAtom> frameSequence = new PageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    (_, _, _, _, _) -> {
                        throw CairoException.nonCritical().put("ordered reducer failure");
                    },
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    1,
                    PageFrameReduceTask.TYPE_FILTER
            );
            try (RecordCursorFactory factory = select("SELECT * FROM ordered_error")) {
                frameSequence.of(
                        factory,
                        sqlExecutionContext,
                        new SCSequence(),
                        PartitionFrameCursorFactory.ORDER_ASC
                );
                frameSequence.prepareForDispatch();
                final FiberTask ownerTask = new FiberTask() {
                    @Override
                    protected void onError(Throwable th) {
                        ownerFailure.set(th);
                    }

                    @Override
                    protected boolean runStep() {
                        final long cursor = frameSequence.next();
                        if (cursor < 0) {
                            throw new AssertionError("ordered result task is unavailable");
                        }
                        final PageFrameReduceTask task = frameSequence.getTask(cursor);
                        try {
                            if (!task.hasError()) {
                                throw new AssertionError("ordered reducer error is unavailable");
                            }
                            throw task.buildError();
                        } finally {
                            frameSequence.collect(cursor, false);
                        }
                    }
                };

                Assert.assertSame(LaunchResult.LAUNCHED, ownerRuntime.launch(ownerTask));
                Assert.assertEquals(1, ownerRuntime.drain(1));
                Assert.assertTrue(ownerTask.isDone());
                Assert.assertTrue(ownerFailure.get() instanceof CairoException);
                TestUtils.assertContains(ownerFailure.get().getMessage(), "ordered reducer failure");
                Assert.assertEquals(0, dispatcher.getCreatedTaskCount());
                Assert.assertEquals(0, dispatcherRuntime.getOutstandingTaskCount());
                Assert.assertEquals(1, frameSequence.getReduceFinishedCounter().get());
            } finally {
                close(ownerRuntime);
                close(dispatcherRuntime);
                Misc.free(dispatcher);
                Misc.free(frameSequence);
            }
        });
    }

    @Test
    public void testOrderedOwnerInlinePreservesWinningSequenceCancellation() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final FiberWalWaitQueue waitQueue = new FiberWalWaitQueue();
            final RingQueue<PageFrameReduceTask> queue = new RingQueue<>(
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    1
            );
            final MPSequence pubSeq = new MPSequence(queue.getCycle());
            final MCSequence subSeq = new MCSequence(queue.getCycle());
            pubSeq.then(subSeq).then(pubSeq);
            final PageFrameSequence<StatefulAtom> ownerSequence = new PageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    (_, _, _, _, _) -> {
                    },
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    1,
                    PageFrameReduceTask.TYPE_FILTER
            ) {
                @Override
                public SqlExecutionCircuitBreaker getCircuitBreaker() {
                    return SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
                }
            };
            final PageFrameSequence<StatefulAtom> foreignSequence = new PageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    (_, _, _, _, _) -> {
                        final int reason = parkWithCancellation(waitQueue);
                        if (reason != FiberWaitCoordinator.REASON_CANCEL) {
                            throw new IllegalStateException("unexpected wait reason [reason=" + reason + ']');
                        }
                        throw CairoException.queryCancelled();
                    },
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    1,
                    PageFrameReduceTask.TYPE_FILTER
            ) {
                @Override
                public SqlExecutionCircuitBreaker getCircuitBreaker() {
                    return SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
                }
            };
            final PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
            final SqlExecutionCircuitBreakerWrapper circuitBreaker = new SqlExecutionCircuitBreakerWrapper(
                    engine,
                    configuration.getCircuitBreakerConfiguration()
            );
            final AtomicReference<Throwable> failure = new AtomicReference<>();
            final FiberTask ownerTask = new FiberTask() {
                @Override
                protected void onError(Throwable th) {
                    failure.set(th);
                }

                @Override
                protected boolean runStep() {
                    Assert.assertFalse(PageFrameReduceJob.consumeQueue(
                            queue,
                            subSeq,
                            record,
                            circuitBreaker,
                            ownerSequence
                    ));
                    return true;
                }
            };
            try {
                final long cursor = pubSeq.next();
                Assert.assertTrue(cursor > -1);
                queue.get(cursor).of(foreignSequence, 0, false);
                pubSeq.done(cursor);

                Assert.assertSame(LaunchResult.LAUNCHED, runtime.launch(ownerTask));
                Assert.assertEquals(1, runtime.drain(1));
                Assert.assertFalse(ownerTask.isDone());
                Assert.assertEquals(1, runtime.getParkedFiberCount());

                foreignSequence.cancel(SqlExecutionCircuitBreaker.STATE_TIMEOUT);
                Assert.assertEquals(1, runtime.drain(1));

                Assert.assertTrue(ownerTask.isDone());
                Assert.assertNull(failure.get());
                Assert.assertFalse(queue.get(cursor).hasError());
                Assert.assertEquals(
                        SqlExecutionCircuitBreaker.STATE_TIMEOUT,
                        foreignSequence.getCancelReason()
                );
                Assert.assertEquals(1, foreignSequence.getReduceFinishedCounter().get());
            } finally {
                waitQueue.fire(1, false);
                close(runtime);
                Misc.free(circuitBreaker);
                Misc.free(record);
                Misc.free(foreignSequence);
                Misc.free(ownerSequence);
                Misc.free(queue);
            }
        });
    }

    @Test
    public void testOrderedStoppedSequenceSkipsUndispatchedFramesOnQuiesce() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE ordered_stop AS (SELECT x FROM long_sequence(1))");
            final FiberRuntime runtime = new FiberRuntime(1);
            final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                    engine,
                    engine.getMessageBus(),
                    runtime
            );
            engine.getMessageBus().setPageFrameReduceDispatcher(dispatcher);
            final PageFrameSequence<StatefulAtom> frameSequence = new PageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    (_, _, _, _, _) -> {
                    },
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    1,
                    PageFrameReduceTask.TYPE_FILTER
            );
            try (RecordCursorFactory factory = select("SELECT * FROM ordered_stop")) {
                frameSequence.of(
                        factory,
                        sqlExecutionContext,
                        new SCSequence(),
                        PartitionFrameCursorFactory.ORDER_ASC
                );
                frameSequence.prepareForDispatch();
                Assert.assertTrue(frameSequence.getFrameCount() > 0);

                frameSequence.cancel(SqlExecutionCircuitBreaker.STATE_OK);
                runtime.beginQuiesce();

                Assert.assertEquals(-2, frameSequence.next());
            } finally {
                close(runtime);
                Misc.free(dispatcher);
                Misc.free(frameSequence);
            }
        });
    }

    @Test
    public void testOrderedTaskFiberRetentionLimitBoundsPool() throws Exception {
        assertMemoryLeak(() -> {
            final int taskCount = 4;
            final FiberRuntime runtime = new FiberRuntime(taskCount, taskCount);
            final FiberWalWaitQueue waitQueue = new FiberWalWaitQueue();
            final RingQueue<PageFrameReduceTask> queue = new RingQueue<>(
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    taskCount
            );
            final MPSequence pubSeq = new MPSequence(queue.getCycle());
            final MCSequence subSeq = new MCSequence(queue.getCycle());
            pubSeq.then(subSeq).then(pubSeq);
            final PageFrameSequence<StatefulAtom> frameSequence = new PageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    (_, _, _, _, _) -> park(waitQueue),
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    1,
                    PageFrameReduceTask.TYPE_FILTER
            ) {
                @Override
                public SqlExecutionCircuitBreaker getCircuitBreaker() {
                    return SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
                }

                @Override
                public long getFrameRowCount(int frameIndex) {
                    return 1;
                }
            };
            final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                    engine,
                    engine.getMessageBus(),
                    runtime
            );
            try {
                for (int i = 0; i < taskCount; i++) {
                    final long cursor = pubSeq.next();
                    Assert.assertTrue(cursor > -1);
                    queue.get(cursor).of(frameSequence, i, false);
                    pubSeq.done(cursor);
                }
                for (int i = 0; i < taskCount; i++) {
                    Assert.assertFalse(dispatcher.consumeOrdered(i, queue, subSeq, null));
                    Assert.assertEquals(i + 1, runtime.getParkedFiberCount());
                }
                Assert.assertEquals(taskCount, dispatcher.getCreatedTaskCount());
                Assert.assertEquals(taskCount, runtime.getParkedFiberCount());

                runtime.updateConfiguration(2, 1, 64);
                Assert.assertEquals(2, dispatcher.getTaskCapacity());
                Assert.assertEquals(1, dispatcher.getTaskMaxRetainedCount());
                Assert.assertEquals(taskCount, dispatcher.getCreatedTaskCount());

                waitQueue.fire(1, false);
                Assert.assertEquals(taskCount, runtime.drain(taskCount));
                Assert.assertEquals(taskCount - 1, runtime.drain(taskCount));

                Assert.assertEquals(1, dispatcher.getCreatedTaskCount());
                Assert.assertEquals(taskCount, runtime.getCreatedFiberCount());
                Assert.assertEquals(1, runtime.getLiveFiberCount());
                Assert.assertEquals(0, runtime.getMountedCount());
                Assert.assertEquals(0, runtime.getOutstandingTaskCount());
                Assert.assertEquals(0, runtime.getParkedFiberCount());
                Assert.assertEquals(0, runtime.getQueuedCount());
                Assert.assertEquals(1, runtime.getRetainedFiberCount());
                Assert.assertEquals(taskCount - 1, runtime.getRetiredFiberCount());
                Assert.assertEquals(taskCount, frameSequence.getReduceFinishedCounter().get());

                dispatcher.close();
                Assert.assertEquals(0, dispatcher.getCreatedTaskCount());
            } finally {
                while (waitQueue.size() > 0) {
                    waitQueue.fire(1, true);
                    runtime.drain(taskCount);
                }
                close(runtime);
                Misc.free(dispatcher);
                Misc.free(frameSequence);
                Misc.free(queue);
            }
        });
    }

    @Test
    public void testOrderedTaskHoldsCursorWhileParked() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final FiberWalWaitQueue waitQueue = new FiberWalWaitQueue();
            final RingQueue<PageFrameReduceTask> queue = new RingQueue<>(
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    2
            );
            final MPSequence pubSeq = new MPSequence(queue.getCycle());
            final MCSequence subSeq = new MCSequence(queue.getCycle());
            pubSeq.then(subSeq).then(pubSeq);
            final PageFrameSequence<StatefulAtom> frameSequence = new PageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    (_, _, _, _, _) -> park(waitQueue),
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    1,
                    PageFrameReduceTask.TYPE_FILTER
            ) {
                @Override
                public SqlExecutionCircuitBreaker getCircuitBreaker() {
                    return SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
                }
            };
            final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                    engine,
                    engine.getMessageBus(),
                    runtime
            );
            try {
                Assert.assertEquals(0, dispatcher.getCreatedTaskCount());
                final long cursor = pubSeq.next();
                Assert.assertTrue(cursor > -1);
                queue.get(cursor).of(frameSequence, 0, false);
                pubSeq.done(cursor);

                Assert.assertFalse(dispatcher.consumeOrdered(0, queue, subSeq, null));
                Assert.assertEquals(1, dispatcher.getCreatedTaskCount());
                Assert.assertEquals(0, frameSequence.getReduceFinishedCounter().get());
                Assert.assertEquals(1, runtime.getOutstandingTaskCount());
                Assert.assertEquals(1, runtime.getParkedFiberCount());

                waitQueue.fire(1, false);
                Assert.assertEquals(1, runtime.drain(1));
                Assert.assertEquals(1, frameSequence.getReduceFinishedCounter().get());
                Assert.assertEquals(0, runtime.getOutstandingTaskCount());
            } finally {
                close(runtime);
                Misc.free(dispatcher);
                Misc.free(frameSequence);
                Misc.free(queue);
            }
        });
    }

    @Test
    public void testPlainOwnerReducesLocallyWithoutMountingFiber() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE plain_owner AS (
                        SELECT timestamp_sequence(0, 1_000_000) AS completed
                        FROM long_sequence(1_000)
                    )
                    """);
            drainWalQueue();
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);

            final FiberRuntime runtime = new FiberRuntime(1);
            final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                    engine,
                    engine.getMessageBus(),
                    runtime
            );
            final PageFrameReduceDispatcher previousDispatcher = engine.getMessageBus().getPageFrameReduceDispatcher();
            engine.getMessageBus().setPageFrameReduceDispatcher(dispatcher);
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    RecordCursorFactory factory = compiler.compile(
                            "SELECT * FROM plain_owner WHERE completed = null",
                            sqlExecutionContext
                    ).getRecordCursorFactory()
            ) {
                TestUtils.assertFactoryInTree(factory, AsyncJitFilteredRecordCursorFactory.class);
                final long mountCount = runtime.getMountCount();
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertFalse(cursor.hasNext());
                }
                Assert.assertEquals(mountCount, runtime.getMountCount());
                Assert.assertEquals(0, dispatcher.getCreatedTaskCount());
            } finally {
                try {
                    close(runtime);
                } finally {
                    engine.getMessageBus().setPageFrameReduceDispatcher(previousDispatcher);
                    Misc.free(dispatcher);
                }
            }
        });
    }

    @Test
    public void testQuiesceDoesNotWaitForActivePublication() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                    engine,
                    engine.getMessageBus(),
                    runtime
            );
            final AtomicReference<Throwable> failure = new AtomicReference<>();
            final Thread quiesceThread = new Thread(() -> {
                try {
                    runtime.beginQuiesce();
                } catch (Throwable th) {
                    failure.set(th);
                }
            });
            boolean isPublicationHeld = false;
            try {
                Assert.assertTrue(dispatcher.tryAcquirePublication());
                isPublicationHeld = true;
                quiesceThread.start();
                quiesceThread.join(1_000);
                final boolean hasReturnedBeforeRelease = !quiesceThread.isAlive();
                dispatcher.releasePublication();
                isPublicationHeld = false;
                quiesceThread.join(5_000);

                Assert.assertTrue("beginQuiesce() waited for publication release", hasReturnedBeforeRelease);
                Assert.assertFalse("beginQuiesce() did not return", quiesceThread.isAlive());
                Assert.assertNull(failure.get());
                Assert.assertFalse(dispatcher.tryAcquirePublication());
            } finally {
                if (isPublicationHeld) {
                    dispatcher.releasePublication();
                }
                if (quiesceThread.isAlive()) {
                    quiesceThread.join(5_000);
                }
                close(runtime);
                Misc.free(dispatcher);
            }
        });
    }

    @Test
    public void testQuiesceDrainsPublishedTasksWithoutRunningReducers() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final PageFrameSequence<StatefulAtom> orderedFrameSequence = new PageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    (_, _, _, _, _) -> Assert.fail("ordered reducer must not run during shutdown drain"),
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    1,
                    PageFrameReduceTask.TYPE_FILTER
            );
            final UnorderedPageFrameSequence<StatefulAtom> unorderedFrameSequence = new UnorderedPageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    (_, _, _, _, _, _) -> Assert.fail("unordered reducer must not run during shutdown drain"),
                    1
            );
            final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                    engine,
                    engine.getMessageBus(),
                    runtime
            );
            try {
                engine.getMessageBus().setPageFrameReduceDispatcher(dispatcher);

                final int shard = 0;
                final long orderedCursor = engine.getMessageBus().getPageFrameReducePubSeq(shard).next();
                Assert.assertTrue(orderedCursor > -1);
                engine.getMessageBus()
                        .getPageFrameReduceQueue(shard)
                        .get(orderedCursor)
                        .of(orderedFrameSequence, 0, false);
                engine.getMessageBus().getPageFrameReducePubSeq(shard).done(orderedCursor);

                final long unorderedCursor = engine.getMessageBus().getUnorderedPageFrameReducePubSeq().next();
                Assert.assertTrue(unorderedCursor > -1);
                engine.getMessageBus()
                        .getUnorderedPageFrameReduceQueue()
                        .get(unorderedCursor)
                        .of(unorderedFrameSequence, 0);
                engine.getMessageBus().getUnorderedPageFrameReducePubSeq().done(unorderedCursor);

                runtime.beginQuiesce();

                Assert.assertFalse(dispatcher.tryAcquirePublication());
                Assert.assertFalse(orderedFrameSequence.isActive());
                Assert.assertEquals(
                        SqlExecutionCircuitBreaker.STATE_CANCELLED,
                        orderedFrameSequence.getCancelReason()
                );
                Assert.assertEquals(1, orderedFrameSequence.getReduceFinishedCounter().get());
                Assert.assertFalse(unorderedFrameSequence.isActive());
                Assert.assertEquals(
                        SqlExecutionCircuitBreaker.STATE_CANCELLED,
                        unorderedFrameSequence.getCancelReason()
                );
                Assert.assertEquals(-1, unorderedFrameSequence.getDoneLatch().getCount());
            } finally {
                close(runtime);
                Misc.free(dispatcher);
                Misc.free(orderedFrameSequence);
                Misc.free(unorderedFrameSequence);
            }
        });
    }

    @Test
    public void testQuiescePreservesSuccessfulOrderedSequence() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final PageFrameSequence<StatefulAtom> frameSequence = new PageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    (_, _, _, _, _) -> Assert.fail("ordered reducer must not run during shutdown drain"),
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    1,
                    PageFrameReduceTask.TYPE_FILTER
            );
            final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                    engine,
                    engine.getMessageBus(),
                    runtime
            );
            try {
                engine.getMessageBus().setPageFrameReduceDispatcher(dispatcher);
                frameSequence.cancel(SqlExecutionCircuitBreaker.STATE_OK);
                Assert.assertFalse(frameSequence.isActive());

                final int shard = 0;
                final MCSequence subSeq = engine.getMessageBus().getPageFrameReduceSubSeq(shard);
                final long cursor = engine.getMessageBus().getPageFrameReducePubSeq(shard).next();
                Assert.assertTrue(cursor > -1);
                engine.getMessageBus()
                        .getPageFrameReduceQueue(shard)
                        .get(cursor)
                        .of(frameSequence, 0, false);
                engine.getMessageBus().getPageFrameReducePubSeq(shard).done(cursor);

                runtime.beginQuiesce();

                Assert.assertTrue(dispatcher.isQuiesced());
                Assert.assertEquals(SqlExecutionCircuitBreaker.STATE_OK, frameSequence.getCancelReason());
                Assert.assertEquals(1, frameSequence.getReduceFinishedCounter().get());
                Assert.assertEquals(cursor, subSeq.current());
            } finally {
                close(runtime);
                Misc.free(dispatcher);
                Misc.free(frameSequence);
            }
        });
    }

    @Test
    public void testQuiescePreservesSuccessfulUnorderedSequence() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final UnorderedPageFrameSequence<StatefulAtom> frameSequence = new UnorderedPageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    (_, _, _, _, _, _) -> Assert.fail("unordered reducer must not run during shutdown drain"),
                    1
            );
            final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                    engine,
                    engine.getMessageBus(),
                    runtime
            );
            try {
                engine.getMessageBus().setPageFrameReduceDispatcher(dispatcher);
                frameSequence.cancel(SqlExecutionCircuitBreaker.STATE_OK);
                Assert.assertFalse(frameSequence.isActive());

                final RingQueue<UnorderedPageFrameReduceTask> queue = engine.getMessageBus()
                        .getUnorderedPageFrameReduceQueue();
                final MCSequence subSeq = engine.getMessageBus().getUnorderedPageFrameReduceSubSeq();
                final long cursor = engine.getMessageBus().getUnorderedPageFrameReducePubSeq().next();
                Assert.assertTrue(cursor > -1);
                queue.get(cursor).of(frameSequence, 0);
                engine.getMessageBus().getUnorderedPageFrameReducePubSeq().done(cursor);

                runtime.beginQuiesce();

                Assert.assertTrue(dispatcher.isQuiesced());
                Assert.assertEquals(SqlExecutionCircuitBreaker.STATE_OK, frameSequence.getCancelReason());
                Assert.assertEquals(-1, frameSequence.getDoneLatch().getCount());
                Assert.assertEquals(cursor, subSeq.current());
                Assert.assertNull(queue.get(cursor).getFrameSequence());
            } finally {
                close(runtime);
                Misc.free(dispatcher);
                Misc.free(frameSequence);
            }
        });
    }

    @Test
    public void testQuiesceWakesProgressWaiterHoldingPublication() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime dispatcherRuntime = new FiberRuntime(1);
            final FiberRuntime ownerRuntime = new FiberRuntime(1);
            final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                    engine,
                    engine.getMessageBus(),
                    dispatcherRuntime
            );
            final AtomicReference<Throwable> failure = new AtomicReference<>();
            final AtomicInteger waitReason = new AtomicInteger(FiberWaitCoordinator.REASON_NONE);
            final FiberTask ownerTask = new FiberTask() {
                @Override
                protected void onError(Throwable th) {
                    failure.set(th);
                }

                @Override
                protected boolean runStep() {
                    Assert.assertTrue(dispatcher.tryAcquirePublication());
                    try {
                        final long observedProgress = dispatcher.getProgressVersion();
                        while (true) {
                            final int reason = dispatcher.awaitProgress(observedProgress, null);
                            if (reason != FiberWaitCoordinator.REASON_TIMER) {
                                waitReason.set(reason);
                                return true;
                            }
                        }
                    } finally {
                        dispatcher.releasePublication();
                    }
                }
            };
            try {
                Assert.assertSame(LaunchResult.LAUNCHED, ownerRuntime.launch(ownerTask));
                Assert.assertEquals(1, ownerRuntime.drain(1));
                Assert.assertFalse(ownerTask.isDone());

                dispatcherRuntime.beginQuiesce();
                Assert.assertFalse(dispatcher.isQuiesced());
                Assert.assertEquals(1, ownerRuntime.drain(1));
                dispatcherRuntime.drain(1);

                Assert.assertTrue(ownerTask.isDone());
                Assert.assertNull(failure.get());
                Assert.assertEquals(FiberWaitCoordinator.REASON_PROGRESS, waitReason.get());
                Assert.assertTrue(dispatcher.isQuiesced());
            } finally {
                close(ownerRuntime);
                close(dispatcherRuntime);
                Misc.free(dispatcher);
            }
        });
    }

    @Test
    public void testResetSignalsGlobalProgress() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE reset_progress AS (SELECT x FROM long_sequence(1))");
            final FiberRuntime runtime = new FiberRuntime(1);
            final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                    engine,
                    engine.getMessageBus(),
                    runtime
            );
            final PageFrameReduceDispatcher previousDispatcher = engine.getMessageBus().getPageFrameReduceDispatcher();
            engine.getMessageBus().setPageFrameReduceDispatcher(dispatcher);
            final PageFrameSequence<StatefulAtom> frameSequence = new PageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    (_, _, _, _, _) -> {
                    },
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    1,
                    PageFrameReduceTask.TYPE_FILTER
            );
            try (RecordCursorFactory factory = select("SELECT * FROM reset_progress")) {
                frameSequence.of(
                        factory,
                        sqlExecutionContext,
                        new SCSequence(),
                        PartitionFrameCursorFactory.ORDER_ASC
                );
                frameSequence.prepareForDispatch();

                final long observedProgress = dispatcher.getProgressVersion();
                frameSequence.reset();
                Assert.assertEquals(observedProgress + 1, dispatcher.getProgressVersion());
            } finally {
                engine.getMessageBus().setPageFrameReduceDispatcher(previousDispatcher);
                close(runtime);
                Misc.free(dispatcher);
                Misc.free(frameSequence);
            }
        });
    }

    @Test
    public void testSameRuntimeOwnerFallsBackToLocalReduce() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                    engine,
                    engine.getMessageBus(),
                    runtime
            );
            final AtomicReference<Throwable> failure = new AtomicReference<>();
            final AtomicInteger publicationCount = new AtomicInteger();
            final FiberTask ownerTask = new FiberTask() {
                @Override
                protected void onError(Throwable th) {
                    failure.set(th);
                }

                @Override
                protected boolean runStep() {
                    if (dispatcher.tryAcquirePublication()) {
                        publicationCount.incrementAndGet();
                        dispatcher.releasePublication();
                    }
                    return true;
                }
            };
            try {
                Assert.assertSame(LaunchResult.LAUNCHED, runtime.launch(ownerTask));
                Assert.assertEquals(1, runtime.drain(1));

                Assert.assertTrue(ownerTask.isDone());
                Assert.assertNull(failure.get());
                Assert.assertEquals(0, publicationCount.get());
                Assert.assertEquals(0, runtime.getOutstandingTaskCount());
                Assert.assertEquals(0, runtime.getParkedFiberCount());
                Assert.assertTrue(dispatcher.tryAcquirePublication());
                dispatcher.releasePublication();
            } finally {
                close(runtime);
                Misc.free(dispatcher);
            }
        });
    }

    @Test
    public void testQuiescedDispatcherCancelsQueriesInsteadOfLocalReduce() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE tab AS (
                        SELECT
                            x,
                            x::varchar AS k,
                            timestamp_sequence(0, 1_000_000) AS ts
                        FROM long_sequence(1_000)
                    ) TIMESTAMP(ts)
                    """);
            drainWalQueue();
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);

            final FiberRuntime runtime = new FiberRuntime(1);
            final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                    engine,
                    engine.getMessageBus(),
                    runtime
            );
            try {
                engine.getMessageBus().setPageFrameReduceDispatcher(dispatcher);
                runtime.beginQuiesce();
                assertQueryCancelledByQuiesce("SELECT * FROM tab WHERE x > 0");
                assertQueryCancelledByQuiesce("SELECT k, count() FROM tab GROUP BY k");
            } finally {
                close(runtime);
                Misc.free(dispatcher);
            }
        });
    }

    @Test
    public void testSameRuntimeProductionPublishersReduceLocally() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE tab AS (
                        SELECT
                            x,
                            x::varchar AS k,
                            timestamp_sequence(0, 1_000_000) AS ts
                        FROM long_sequence(1_000)
                    ) TIMESTAMP(ts)
                    """);
            drainWalQueue();
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);

            final FiberRuntime runtime = new FiberRuntime(1);
            final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                    engine,
                    engine.getMessageBus(),
                    runtime
            );
            try {
                engine.getMessageBus().setPageFrameReduceDispatcher(dispatcher);
                final int shardCount = engine.getMessageBus().getPageFrameReduceShardCount();
                final LongList orderedPublicationCursors = new LongList(shardCount);
                for (int shard = 0; shard < shardCount; shard++) {
                    orderedPublicationCursors.add(
                            engine.getMessageBus().getPageFrameReducePubSeq(shard).current()
                    );
                }
                final long unorderedPublicationCursor = engine.getMessageBus()
                        .getUnorderedPageFrameReducePubSeq()
                        .current();

                assertSameRuntimeQueryReducesLocally(
                        runtime,
                        dispatcher,
                        "SELECT * FROM tab WHERE x > 0",
                        AsyncFilteredRecordCursorFactory.class
                );
                assertSameRuntimeQueryReducesLocally(
                        runtime,
                        dispatcher,
                        "SELECT k, count() FROM tab GROUP BY k",
                        AsyncGroupByRecordCursorFactory.class
                );
                for (int shard = 0; shard < shardCount; shard++) {
                    Assert.assertEquals(
                            orderedPublicationCursors.getQuick(shard),
                            engine.getMessageBus().getPageFrameReducePubSeq(shard).current()
                    );
                }
                Assert.assertEquals(
                        unorderedPublicationCursor,
                        engine.getMessageBus().getUnorderedPageFrameReducePubSeq().current()
                );
            } finally {
                close(runtime);
                Misc.free(dispatcher);
            }
        });
    }

    @Test
    public void testSaturatedOwnerFiberWaitsWithoutClaimingCursor() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime ownerRuntime = new FiberRuntime(1);
            final FiberRuntime dispatcherRuntime = new FiberRuntime(1);
            final FiberWalWaitQueue waitQueue = new FiberWalWaitQueue();
            final RingQueue<PageFrameReduceTask> queue = new RingQueue<>(
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    2
            );
            final MPSequence pubSeq = new MPSequence(queue.getCycle());
            final MCSequence subSeq = new MCSequence(queue.getCycle());
            pubSeq.then(subSeq).then(pubSeq);
            final AtomicReference<PageFrameSequence<?>> observedStealingSequence = new AtomicReference<>();
            final PageFrameSequence<StatefulAtom> frameSequence = new PageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    (_, _, task, _, stealingFrameSequence) -> {
                        observedStealingSequence.set(stealingFrameSequence);
                        if (task.getFrameIndex() == 0) {
                            park(waitQueue);
                        }
                    },
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    1,
                    PageFrameReduceTask.TYPE_FILTER
            ) {
                @Override
                public SqlExecutionCircuitBreaker getCircuitBreaker() {
                    return SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
                }
            };
            final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                    engine,
                    engine.getMessageBus(),
                    dispatcherRuntime
            );
            final AtomicReference<Throwable> ownerFailure = new AtomicReference<>();
            final FiberTask ownerTask = new FiberTask() {
                @Override
                protected boolean runStep() {
                    // the resumed dispatcher fiber drains the whole queue in one batch
                    Assert.assertTrue(dispatcher.consumeOrdered(-1, queue, subSeq, frameSequence));
                    return true;
                }

                @Override
                protected void onError(Throwable th) {
                    ownerFailure.compareAndSet(null, th);
                }
            };
            try {
                for (int i = 0; i < 2; i++) {
                    final long cursor = pubSeq.next();
                    Assert.assertTrue(cursor > -1);
                    queue.get(cursor).of(frameSequence, i, false);
                    pubSeq.done(cursor);
                }

                Assert.assertFalse(dispatcher.consumeOrdered(0, queue, subSeq, null));
                Assert.assertEquals(0, subSeq.current());
                Assert.assertEquals(1, dispatcherRuntime.getParkedFiberCount());

                Assert.assertSame(LaunchResult.LAUNCHED, ownerRuntime.launch(ownerTask));
                Assert.assertEquals(1, ownerRuntime.drain(1));
                Assert.assertEquals(1, ownerRuntime.getParkedFiberCount());
                Assert.assertEquals(0, subSeq.current());

                waitQueue.fire(1, false);
                Assert.assertEquals(1, dispatcherRuntime.drain(1));
                Assert.assertEquals(1, subSeq.current());
                Assert.assertEquals(1, ownerRuntime.drain(1));
                Assert.assertTrue(ownerTask.isDone());
                Assert.assertNull(ownerFailure.get());
                // both frames completed inside the batch: no second mount
                Assert.assertEquals(0, dispatcherRuntime.drain(1));
                Assert.assertNull(observedStealingSequence.get());
                Assert.assertEquals(2, frameSequence.getReduceFinishedCounter().get());
            } finally {
                close(ownerRuntime);
                close(dispatcherRuntime);
                Misc.free(dispatcher);
                Misc.free(frameSequence);
                Misc.free(queue);
            }
        });
    }

    @Test
    public void testSteadyStateOrderedDispatchAllocatesNoJavaHeap() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final RingQueue<PageFrameReduceTask> queue = new RingQueue<>(
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    2
            );
            final MPSequence pubSeq = new MPSequence(queue.getCycle());
            final MCSequence subSeq = new MCSequence(queue.getCycle());
            pubSeq.then(subSeq).then(pubSeq);
            final PageFrameSequence<StatefulAtom> frameSequence = new PageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    (_, _, _, _, _) -> {
                    },
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    1,
                    PageFrameReduceTask.TYPE_FILTER
            ) {
                @Override
                public SqlExecutionCircuitBreaker getCircuitBreaker() {
                    return SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
                }
            };
            final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                    engine,
                    engine.getMessageBus(),
                    runtime
            );
            try (TestUtils.ThreadMetricsScope<com.sun.management.ThreadMXBean> scope = TestUtils.threadAllocationScope()) {
                final com.sun.management.ThreadMXBean threadMXBean = scope.getBean();
                for (int i = 0; i < 10_000; i++) {
                    runOrdered(dispatcher, frameSequence, pubSeq, queue, subSeq);
                }

                long minAllocatedBytes = Long.MAX_VALUE;
                for (int round = 0; round < 5; round++) {
                    final long allocatedBefore = threadMXBean.getCurrentThreadAllocatedBytes();
                    for (int i = 0; i < 100_000; i++) {
                        runOrdered(dispatcher, frameSequence, pubSeq, queue, subSeq);
                    }
                    minAllocatedBytes = Math.min(
                            minAllocatedBytes,
                            threadMXBean.getCurrentThreadAllocatedBytes() - allocatedBefore
                    );
                }
                Assert.assertEquals(0, minAllocatedBytes);
            } finally {
                close(runtime);
                Misc.free(dispatcher);
                Misc.free(frameSequence);
                Misc.free(queue);
            }
        });
    }

    @Test
    public void testSupplementalCancellationWakesProgressWaiter() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final FiberCancellationSignal cancellationSignal = new FiberCancellationSignal();
            final FiberCancellationSignal supplementalCancellationSignal = new FiberCancellationSignal();
            final PageFrameSequence<StatefulAtom> frameSequence = new PageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    (_, _, _, _, _) -> {
                    },
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    1,
                    PageFrameReduceTask.TYPE_FILTER
            ) {
                @Override
                public SqlExecutionCircuitBreaker getCircuitBreaker() {
                    return SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
                }
            };
            final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                    engine,
                    engine.getMessageBus(),
                    runtime
            );
            final AtomicReference<Throwable> failure = new AtomicReference<>();
            final AtomicInteger waitReason = new AtomicInteger(FiberWaitCoordinator.REASON_NONE);
            final FiberTask task = new FiberTask() {
                @Override
                protected void onError(Throwable th) {
                    failure.set(th);
                }

                @Override
                protected boolean runStep() {
                    waitReason.set(dispatcher.awaitProgress(
                            frameSequence,
                            frameSequence.getProgressVersion(),
                            dispatcher.getProgressVersion(),
                            cancellationSignal,
                            supplementalCancellationSignal
                    ));
                    return true;
                }
            };
            try {
                Assert.assertSame(LaunchResult.LAUNCHED, runtime.launch(task));
                Assert.assertEquals(1, runtime.drain(1));
                Assert.assertFalse(task.isDone());
                Assert.assertEquals(1, runtime.getParkedFiberCount());

                supplementalCancellationSignal.cancel();
                Assert.assertEquals(1, runtime.drain(1));

                Assert.assertTrue(task.isDone());
                Assert.assertNull(failure.get());
                Assert.assertEquals(FiberWaitCoordinator.REASON_CANCEL, waitReason.get());
                Assert.assertFalse(cancellationSignal.isCancelled(cancellationSignal.getGeneration()));
            } finally {
                close(runtime);
                Misc.free(dispatcher);
                Misc.free(frameSequence);
            }
        });
    }

    @Test
    public void testBusyBatchingFiberLeavesOrderedCursorUnclaimed() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final RingQueue<PageFrameReduceTask> queue = new RingQueue<>(
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    2
            );
            final MPSequence pubSeq = new MPSequence(queue.getCycle());
            final BlockingDoneMCSequence subSeq = new BlockingDoneMCSequence(queue.getCycle());
            pubSeq.then(subSeq).then(pubSeq);
            final PageFrameSequence<StatefulAtom> frameSequence = new PageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    (_, _, _, _, _) -> {
                    },
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    2,
                    PageFrameReduceTask.TYPE_FILTER
            ) {
                @Override
                public SqlExecutionCircuitBreaker getCircuitBreaker() {
                    return SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
                }
            };
            final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                    engine,
                    engine.getMessageBus(),
                    runtime
            );
            final AtomicReference<Throwable> failure = new AtomicReference<>();
            final Thread firstConsumer = new Thread(() -> {
                try {
                    if (dispatcher.consumeOrdered(0, queue, subSeq, null)) {
                        throw new AssertionError("first task was not consumed");
                    }
                } catch (Throwable th) {
                    failure.set(th);
                }
            });
            try {
                for (int i = 0; i < 2; i++) {
                    final long cursor = pubSeq.next();
                    Assert.assertTrue(cursor > -1);
                    queue.get(cursor).of(frameSequence, i, false);
                    pubSeq.done(cursor);
                }

                firstConsumer.start();
                Assert.assertTrue(subSeq.awaitDoneEntry());
                // the batching fiber is blocked inside its first frame's cursor release
                Assert.assertEquals(1, runtime.getOutstandingTaskCount());
                Assert.assertEquals(1, dispatcher.getCreatedTaskCount());

                Assert.assertTrue(dispatcher.consumeOrdered(1, queue, subSeq, null));
                Assert.assertEquals(0, subSeq.current());

                subSeq.releaseDone();
                firstConsumer.join(5_000);
                Assert.assertFalse("first consumer did not return", firstConsumer.isAlive());
                Assert.assertNull(failure.get());

                // the released fiber consumed the second frame inside the same batch
                Assert.assertTrue(dispatcher.consumeOrdered(1, queue, subSeq, null));
                Assert.assertEquals(2, frameSequence.getReduceFinishedCounter().get());
                Assert.assertEquals(0, runtime.getOutstandingTaskCount());
            } finally {
                subSeq.releaseDone();
                firstConsumer.join(5_000);
                close(runtime);
                Misc.free(dispatcher);
                Misc.free(frameSequence);
                Misc.free(queue);
            }
        });
    }

    @Test
    public void testUnorderedOwnerInlineStealsForeignTaskAcrossSuspend() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final FiberWalWaitQueue waitQueue = new FiberWalWaitQueue();
            final RingQueue<UnorderedPageFrameReduceTask> queue = new RingQueue<>(
                    UnorderedPageFrameReduceTask::new,
                    1
            );
            final MPSequence pubSeq = new MPSequence(queue.getCycle());
            final MCSequence subSeq = new MCSequence(queue.getCycle());
            pubSeq.then(subSeq).then(pubSeq);
            final AtomicReference<Fiber> ownerFiber = new AtomicReference<>();
            final AtomicReference<Fiber> reducerFiber = new AtomicReference<>();
            final AtomicReference<UnorderedPageFrameSequence<?>> stealingSequence = new AtomicReference<>();
            final UnorderedPageFrameSequence<StatefulAtom> ownerSequence = new UnorderedPageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    (_, _, _, _, _, _) -> {
                    },
                    1
            ) {
                @Override
                public SqlExecutionCircuitBreaker getCircuitBreaker() {
                    return SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
                }
            };
            final UnorderedPageFrameSequence<StatefulAtom> foreignSequence = new UnorderedPageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    (_, _, _, _, _, stealingFrameSequence) -> {
                        reducerFiber.set(Fiber.current());
                        stealingSequence.set(stealingFrameSequence);
                        park(waitQueue);
                    },
                    1
            ) {
                @Override
                public SqlExecutionCircuitBreaker getCircuitBreaker() {
                    return SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
                }
            };
            final PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
            final SqlExecutionCircuitBreakerWrapper circuitBreaker = new SqlExecutionCircuitBreakerWrapper(
                    engine,
                    configuration.getCircuitBreakerConfiguration()
            );
            final AtomicReference<Throwable> failure = new AtomicReference<>();
            final FiberTask ownerTask = new FiberTask() {
                @Override
                protected void onError(Throwable th) {
                    failure.set(th);
                }

                @Override
                protected boolean runStep() {
                    ownerFiber.set(Fiber.current());
                    Assert.assertFalse(UnorderedPageFrameReduceJob.consumeQueue(
                            queue,
                            subSeq,
                            record,
                            circuitBreaker,
                            ownerSequence
                    ));
                    return true;
                }
            };
            try {
                final long cursor = pubSeq.next();
                Assert.assertTrue(cursor > -1);
                queue.get(cursor).of(foreignSequence, 0);
                pubSeq.done(cursor);

                Assert.assertSame(LaunchResult.LAUNCHED, runtime.launch(ownerTask));
                Assert.assertEquals(1, runtime.drain(1));
                Assert.assertFalse(ownerTask.isDone());
                Assert.assertEquals(1, runtime.getParkedFiberCount());
                Assert.assertEquals(0, subSeq.current());
                Assert.assertEquals(0, foreignSequence.getDoneLatch().getCount());

                waitQueue.fire(1, false);
                Assert.assertEquals(1, runtime.drain(1));
                Assert.assertTrue(ownerTask.isDone());
                Assert.assertNull(failure.get());
                Assert.assertSame(ownerFiber.get(), reducerFiber.get());
                Assert.assertSame(ownerSequence, stealingSequence.get());
                Assert.assertEquals(-1, foreignSequence.getDoneLatch().getCount());
            } finally {
                waitQueue.fire(1, false);
                close(runtime);
                Misc.free(circuitBreaker);
                Misc.free(record);
                Misc.free(foreignSequence);
                Misc.free(ownerSequence);
                Misc.free(queue);
            }
        });
    }

    @Test
    public void testUnorderedDirectStealRebindsWorkStealBreakerToOwnQuery() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE unordered_steal_rebind AS (
                        SELECT x, timestamp_sequence(0, 86_400_000_000) ts
                        FROM long_sequence(2)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            final FiberRuntime runtime = new FiberRuntime(1);
            final AtomicBooleanCircuitBreaker foreignBreaker = new AtomicBooleanCircuitBreaker(engine);
            foreignBreaker.cancel();
            final UnorderedPageFrameSequence<StatefulAtom> foreignSequence = new UnorderedPageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    (_, _, _, _, _, _) -> {
                    },
                    1
            ) {
                @Override
                public SqlExecutionCircuitBreaker getCircuitBreaker() {
                    return foreignBreaker;
                }
            };
            final AtomicInteger localReduceCount = new AtomicInteger();
            final UnorderedPageFrameSequence<StatefulAtom> frameSequence = new UnorderedPageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    (_, _, _, _, _, _) -> localReduceCount.incrementAndGet(),
                    1
            );
            final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                    engine,
                    engine.getMessageBus(),
                    runtime
            );
            engine.getMessageBus().setPageFrameReduceDispatcher(dispatcher);
            final RingQueue<UnorderedPageFrameReduceTask> queue = engine.getMessageBus().getUnorderedPageFrameReduceQueue();
            final MPSequence pubSeq = engine.getMessageBus().getUnorderedPageFrameReducePubSeq();
            final MCSequence subSeq = engine.getMessageBus().getUnorderedPageFrameReduceSubSeq();
            final LongList claimedCursors = new LongList();
            try (RecordCursorFactory factory = select("SELECT * FROM unordered_steal_rebind")) {
                // Saturate the global queue with foreign tasks so the owner's publish attempt
                // fails, then leave exactly one task available: the owner's first direct steal
                // succeeds (rebinding the work-steal breaker), the second finds nothing and
                // consults the breaker before falling back to a local reduce.
                final int capacity = queue.getCycle();
                for (int i = 0; i < capacity; i++) {
                    final long cursor = pubSeq.next();
                    Assert.assertTrue(cursor > -1);
                    queue.get(cursor).of(foreignSequence, 0);
                    pubSeq.done(cursor);
                }
                for (int i = 0; i < capacity - 1; i++) {
                    final long cursor = subSeq.next();
                    Assert.assertTrue(cursor > -1);
                    claimedCursors.add(cursor);
                }

                frameSequence.of(factory, sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                frameSequence.prepareForDispatch();
                Assert.assertEquals(2, frameSequence.getFrameCount());
                frameSequence.dispatchAndAwait();

                Assert.assertTrue(frameSequence.isActive());
                Assert.assertEquals(SqlExecutionCircuitBreaker.STATE_OK, frameSequence.getCancelReason());
                Assert.assertEquals(2, localReduceCount.get());
                Assert.assertFalse(foreignSequence.isActive());
                Assert.assertEquals(SqlExecutionCircuitBreaker.STATE_CANCELLED, foreignSequence.getCancelReason());
            } finally {
                for (int i = 0, n = claimedCursors.size(); i < n; i++) {
                    final long cursor = claimedCursors.getQuick(i);
                    queue.get(cursor).clear();
                    subSeq.done(cursor);
                }
                close(runtime);
                Misc.free(dispatcher);
                Misc.free(frameSequence);
                Misc.free(foreignSequence);
            }
        });
    }

    @Test
    public void testUnorderedLaunchFailureTransfersFiberAndTaskOwnership() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime dispatcherRuntime = new FiberRuntime(1);
            final FiberRuntime ownerRuntime = new FiberRuntime(1);
            final RingQueue<UnorderedPageFrameReduceTask> queue = new RingQueue<>(
                    UnorderedPageFrameReduceTask::new,
                    1
            );
            final MPSequence pubSeq = new MPSequence(queue.getCycle());
            final MCSequence subSeq = new MCSequence(queue.getCycle());
            pubSeq.then(subSeq).then(pubSeq);
            final UnorderedPageFrameSequence<StatefulAtom> failedFrameSequence = new UnorderedPageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    (_, _, _, _, _, _) -> {
                    },
                    1
            ) {
                @Override
                public SqlExecutionCircuitBreaker getCircuitBreaker() {
                    return SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
                }
            };
            final UnorderedPageFrameSequence<StatefulAtom> replacementFrameSequence = new UnorderedPageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    (_, _, _, _, _, _) -> {
                    },
                    1
            ) {
                @Override
                public SqlExecutionCircuitBreaker getCircuitBreaker() {
                    return SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
                }
            };
            final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                    engine,
                    engine.getMessageBus(),
                    dispatcherRuntime
            );
            final AtomicReference<Throwable> failure = new AtomicReference<>();
            final FiberTask ownerTask = new FiberTask() {
                @Override
                protected void onError(Throwable th) {
                    failure.set(th);
                }

                @Override
                protected boolean runStep() {
                    runUnordered(dispatcher, failedFrameSequence, pubSeq, queue, subSeq);
                    return true;
                }
            };
            try {
                dispatcherRuntime.setRunQueueDepthForTesting(dispatcherRuntime.getRunQueueCapacity());
                Assert.assertSame(LaunchResult.LAUNCHED, ownerRuntime.launch(ownerTask));
                Assert.assertEquals(1, ownerRuntime.drain(1));

                Assert.assertTrue(ownerTask.isDone());
                Assert.assertNotNull(failure.get());
                TestUtils.assertContains(
                        failure.get().getMessage(),
                        "page frame fiber launch failed [result=TERMINAL]"
                );
                Assert.assertEquals(0, dispatcherRuntime.getOutstandingTaskCount());
                Assert.assertEquals(1, dispatcherRuntime.getCreatedFiberCount());
                Assert.assertEquals(1, dispatcher.getCreatedTaskCount());

                dispatcherRuntime.setRunQueueDepthForTesting(0);
                runUnordered(dispatcher, replacementFrameSequence, pubSeq, queue, subSeq);
                Assert.assertEquals(0, dispatcherRuntime.getOutstandingTaskCount());
                Assert.assertEquals(1, dispatcherRuntime.getCreatedFiberCount());
                Assert.assertEquals(1, dispatcher.getCreatedTaskCount());
                Assert.assertEquals(-1, replacementFrameSequence.getDoneLatch().getCount());
            } finally {
                dispatcherRuntime.setRunQueueDepthForTesting(0);
                close(ownerRuntime);
                close(dispatcherRuntime);
                Misc.free(dispatcher);
                Misc.free(failedFrameSequence);
                Misc.free(replacementFrameSequence);
                Misc.free(queue);
            }
        });
    }

    @Test
    public void testUnorderedOwnerHelpDoesNotBorrowOwnerState() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime ownerRuntime = new FiberRuntime(1);
            final FiberRuntime dispatcherRuntime = new FiberRuntime(1);
            final RingQueue<UnorderedPageFrameReduceTask> queue = new RingQueue<>(
                    UnorderedPageFrameReduceTask::new,
                    2
            );
            final MPSequence pubSeq = new MPSequence(queue.getCycle());
            final MCSequence subSeq = new MCSequence(queue.getCycle());
            pubSeq.then(subSeq).then(pubSeq);
            final AtomicReference<UnorderedPageFrameSequence<?>> observedStealingSequence = new AtomicReference<>();
            final UnorderedPageFrameSequence<StatefulAtom> frameSequence = new UnorderedPageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    (_, _, _, _, _, stealingFrameSequence) -> observedStealingSequence.set(stealingFrameSequence),
                    1
            ) {
                @Override
                public SqlExecutionCircuitBreaker getCircuitBreaker() {
                    return SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
                }
            };
            final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                    engine,
                    engine.getMessageBus(),
                    dispatcherRuntime
            );
            final AtomicReference<Throwable> ownerTaskError = new AtomicReference<>();
            final FiberTask ownerTask = new FiberTask() {
                @Override
                protected void onError(Throwable th) {
                    ownerTaskError.set(th);
                }

                @Override
                protected boolean runStep() {
                    Assert.assertFalse(dispatcher.consumeUnordered(-1, queue, subSeq, frameSequence));
                    return true;
                }
            };
            try {
                final long cursor = pubSeq.next();
                Assert.assertTrue(cursor > -1);
                queue.get(cursor).of(frameSequence, 0);
                pubSeq.done(cursor);

                Assert.assertSame(LaunchResult.LAUNCHED, ownerRuntime.launch(ownerTask));
                Assert.assertEquals(1, ownerRuntime.drain(1));
                if (ownerTaskError.get() != null) {
                    throw new AssertionError(ownerTaskError.get());
                }
                Assert.assertEquals(0, frameSequence.getDoneLatch().getCount());
                Assert.assertEquals(1, dispatcherRuntime.getOutstandingTaskCount());

                Assert.assertEquals(1, dispatcherRuntime.drain(1));
                Assert.assertNull(observedStealingSequence.get());
                Assert.assertEquals(-1, frameSequence.getDoneLatch().getCount());
                Assert.assertEquals(0, dispatcherRuntime.getOutstandingTaskCount());
            } finally {
                close(ownerRuntime);
                close(dispatcherRuntime);
                Misc.free(dispatcher);
                Misc.free(frameSequence);
                Misc.free(queue);
            }
        });
    }

    @Test
    public void testUnorderedOwnerInlineReleasesPublicationBeforeSuspend() throws Exception {
        setProperty(PropertyKey.CAIRO_UNORDERED_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, 1);
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE unordered_publication AS (
                        SELECT x, timestamp_sequence(0, 86_400_000_000) ts
                        FROM long_sequence(2)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            final FiberRuntime dispatcherRuntime = new FiberRuntime(1);
            final FiberRuntime ownerRuntime = new FiberRuntime(1);
            final FiberWalWaitQueue dispatcherWaitQueue = new FiberWalWaitQueue();
            final FiberWalWaitQueue reducerWaitQueue = new FiberWalWaitQueue();
            final AtomicInteger directStealCount = new AtomicInteger();
            final WorkStealingStrategy countingStrategy = new WorkStealingStrategy() {
                @Override
                public WorkStealingStrategy of(AtomicInteger startedCounter) {
                    return this;
                }

                @Override
                public void onBeforeDirectSteal() {
                    directStealCount.incrementAndGet();
                }

                @Override
                public boolean shouldSteal(int finishedCount) {
                    return true;
                }
            };
            final FactoryProvider countingStrategyProvider = new DefaultFactoryProvider() {
                @Override
                public @NotNull WorkStealingStrategy getWorkStealingStrategy(
                        @NotNull CairoConfiguration configuration,
                        int workerCount,
                        @NotNull StatefulAtom atom
                ) {
                    return countingStrategy;
                }
            };
            final CairoConfiguration sequenceConfiguration = new CairoConfigurationWrapper(configuration) {
                @Override
                public @NotNull FactoryProvider getFactoryProvider() {
                    return countingStrategyProvider;
                }
            };
            final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                    engine,
                    engine.getMessageBus(),
                    dispatcherRuntime
            );
            engine.getMessageBus().setPageFrameReduceDispatcher(dispatcher);
            final UnorderedPageFrameSequence<StatefulAtom> frameSequence = new UnorderedPageFrameSequence<>(
                    engine,
                    sequenceConfiguration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    (_, _, _, _, _, _) -> park(reducerWaitQueue),
                    1
            );
            final AtomicReference<Throwable> ownerFailure = new AtomicReference<>();
            try (RecordCursorFactory factory = select("SELECT * FROM unordered_publication")) {
                frameSequence.of(factory, sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                frameSequence.prepareForDispatch();
                Assert.assertEquals(2, frameSequence.getFrameCount());
                final FiberTask blockerTask = new FiberTask() {
                    @Override
                    protected boolean runStep() {
                        park(dispatcherWaitQueue);
                        return true;
                    }
                };
                final FiberTask ownerTask = new FiberTask() {
                    @Override
                    protected void onError(Throwable th) {
                        ownerFailure.set(th);
                    }

                    @Override
                    protected boolean runStep() {
                        frameSequence.dispatchAndAwait();
                        return true;
                    }
                };

                Assert.assertSame(LaunchResult.LAUNCHED, dispatcherRuntime.launch(blockerTask));
                Assert.assertEquals(1, dispatcherRuntime.drain(1));
                Assert.assertEquals(1, dispatcherRuntime.getParkedFiberCount());

                Assert.assertSame(LaunchResult.LAUNCHED, ownerRuntime.launch(ownerTask));
                Assert.assertEquals(1, ownerRuntime.drain(1));
                Assert.assertEquals(1, ownerRuntime.getParkedFiberCount());
                Assert.assertEquals(0, dispatcher.getCreatedTaskCount());
                Assert.assertEquals(0, frameSequence.getDoneLatch().getCount());
                Assert.assertEquals(1, directStealCount.get());

                Assert.assertTrue(dispatcher.tryAcquirePublication());
                dispatcher.releasePublication();

                reducerWaitQueue.fire(1, false);
                Assert.assertEquals(1, ownerRuntime.drain(1));
                Assert.assertFalse(ownerTask.isDone());
                Assert.assertEquals(1, ownerRuntime.getParkedFiberCount());
                Assert.assertEquals(-1, frameSequence.getDoneLatch().getCount());
                Assert.assertEquals(2, directStealCount.get());

                reducerWaitQueue.fire(1, false);
                Assert.assertEquals(1, ownerRuntime.drain(1));
                Assert.assertTrue(ownerTask.isDone());
                Assert.assertNull(ownerFailure.get());
                Assert.assertEquals(-2, frameSequence.getDoneLatch().getCount());

                dispatcherWaitQueue.fire(1, false);
                Assert.assertEquals(1, dispatcherRuntime.drain(1));
            } finally {
                dispatcherWaitQueue.fire(1, false);
                reducerWaitQueue.fire(1, false);
                dispatcherRuntime.drain(8);
                close(dispatcherRuntime);
                ownerRuntime.drain(8);
                close(ownerRuntime);
                Misc.free(dispatcher);
                Misc.free(frameSequence);
            }
        });
    }

    @Test
    public void testUnorderedOwnerInlineNormalizesReducerError() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE unordered_error AS (SELECT x FROM long_sequence(1))");
            final FiberRuntime dispatcherRuntime = new FiberRuntime(1);
            final FiberRuntime ownerRuntime = new FiberRuntime(1);
            final AtomicReference<Throwable> ownerFailure = new AtomicReference<>();
            final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                    engine,
                    engine.getMessageBus(),
                    dispatcherRuntime
            );
            engine.getMessageBus().setPageFrameReduceDispatcher(dispatcher);
            final UnorderedPageFrameSequence<StatefulAtom> frameSequence = new UnorderedPageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    (_, _, _, _, _, _) -> {
                        throw new IllegalStateException("unordered reducer failure");
                    },
                    1
            );
            try (RecordCursorFactory factory = select("SELECT * FROM unordered_error")) {
                frameSequence.of(factory, sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                frameSequence.prepareForDispatch();
                final FiberTask ownerTask = new FiberTask() {
                    @Override
                    protected void onError(Throwable th) {
                        ownerFailure.set(th);
                    }

                    @Override
                    protected boolean runStep() {
                        frameSequence.dispatchAndAwait();
                        throw new AssertionError("unordered reducer error is unavailable");
                    }
                };

                Assert.assertSame(LaunchResult.LAUNCHED, ownerRuntime.launch(ownerTask));
                Assert.assertEquals(1, ownerRuntime.drain(1));
                Assert.assertTrue(ownerTask.isDone());
                Assert.assertTrue(ownerFailure.get() instanceof CairoException);
                TestUtils.assertContains(
                        ownerFailure.get().getMessage(),
                        "unexpected reduce error: unordered reducer failure"
                );
                Assert.assertEquals(0, dispatcher.getCreatedTaskCount());
                Assert.assertEquals(0, dispatcherRuntime.getOutstandingTaskCount());
                Assert.assertEquals(-1, frameSequence.getDoneLatch().getCount());
            } finally {
                close(ownerRuntime);
                close(dispatcherRuntime);
                Misc.free(dispatcher);
                Misc.free(frameSequence);
            }
        });
    }

    @Test
    public void testUnorderedTaskReleasesCursorBeforeParking() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final FiberWalWaitQueue waitQueue = new FiberWalWaitQueue();
            final RingQueue<UnorderedPageFrameReduceTask> queue = new RingQueue<>(
                    UnorderedPageFrameReduceTask::new,
                    1
            );
            final MPSequence pubSeq = new MPSequence(queue.getCycle());
            final MCSequence subSeq = new MCSequence(queue.getCycle());
            pubSeq.then(subSeq).then(pubSeq);
            final UnorderedPageFrameSequence<StatefulAtom> frameSequence = new UnorderedPageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    (_, _, _, _, _, _) -> park(waitQueue),
                    1
            ) {
                @Override
                public SqlExecutionCircuitBreaker getCircuitBreaker() {
                    return SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
                }
            };
            final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                    engine,
                    engine.getMessageBus(),
                    runtime
            );
            try {
                final long cursor = pubSeq.next();
                Assert.assertTrue(cursor > -1);
                queue.get(cursor).of(frameSequence, 0);
                pubSeq.done(cursor);

                Assert.assertFalse(dispatcher.consumeUnordered(0, queue, subSeq, null));
                Assert.assertEquals(0, frameSequence.getDoneLatch().getCount());
                Assert.assertTrue(pubSeq.next() > -1);
                Assert.assertEquals(1, runtime.getParkedFiberCount());

                waitQueue.fire(1, false);
                Assert.assertEquals(1, runtime.drain(1));
                Assert.assertEquals(-1, frameSequence.getDoneLatch().getCount());
                Assert.assertEquals(0, runtime.getOutstandingTaskCount());
            } finally {
                close(runtime);
                Misc.free(dispatcher);
                Misc.free(frameSequence);
                Misc.free(queue);
            }
        });
    }

    private static void assertBrokenConnection(CairoException exception) {
        TestUtils.assertEquals("remote disconnected, query aborted", exception.getFlyweightMessage());
        Assert.assertFalse(exception.isCancellation());
        Assert.assertTrue(exception.isInterruption());
    }

    private void assertQueryCancelledByQuiesce(String sql) throws SqlException {
        try (
                RecordCursorFactory factory = select(sql);
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            //noinspection StatementWithEmptyBody
            while (cursor.hasNext()) {
            }
            Assert.fail("query over a quiescing dispatcher must cancel");
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "cancelled by user");
            Assert.assertEquals(SqlExecutionCircuitBreaker.STATE_CANCELLED, e.getInterruptionReason());
        }
    }

    private void assertSameRuntimeQueryReducesLocally(
            FiberRuntime runtime,
            PageFrameReduceDispatcher dispatcher,
            String sql,
            Class<?> expectedFactoryClass
    ) throws Exception {
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final AtomicInteger rowCount = new AtomicInteger();
        try (
                SqlCompiler compiler = engine.getSqlCompiler();
                RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()
        ) {
            TestUtils.assertFactoryInTree(factory, expectedFactoryClass);
            final FiberTask ownerTask = new FiberTask() {
                @Override
                protected void onError(Throwable th) {
                    failure.set(th);
                }

                @Override
                protected boolean runStep() {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        while (cursor.hasNext()) {
                            rowCount.incrementAndGet();
                        }
                    } catch (SqlException e) {
                        throw new AssertionError(e);
                    }
                    return true;
                }
            };

            Assert.assertSame(LaunchResult.LAUNCHED, runtime.launch(ownerTask));
            final long deadline = System.nanoTime() + 5_000_000_000L;
            while (!ownerTask.isDone() && System.nanoTime() < deadline) {
                runtime.drain(8);
            }

            Assert.assertTrue(ownerTask.isDone());
            Assert.assertNull(failure.get());
            Assert.assertEquals(1000, rowCount.get());
            Assert.assertEquals(0, runtime.getOutstandingTaskCount());
            Assert.assertEquals(0, runtime.getParkedFiberCount());
            Assert.assertEquals(0, dispatcher.getCreatedTaskCount());
        }
    }

    private static void close(FiberRuntime runtime) {
        runtime.beginQuiesce();
        final long deadline = System.nanoTime() + 5_000_000_000L;
        while (runtime.state() != FiberRuntimeState.CLOSED && System.nanoTime() < deadline) {
            runtime.drain(8);
        }
        Assert.assertTrue(
                "fiber runtime did not close [state=" + runtime.state()
                        + ", created=" + runtime.getCreatedFiberCount()
                        + ", live=" + runtime.getLiveFiberCount()
                        + ", retained=" + runtime.getRetainedFiberCount()
                        + ", retired=" + runtime.getRetiredFiberCount()
                        + ", parked=" + runtime.getParkedFiberCount()
                        + ", mounted=" + runtime.getMountedCount()
                        + ", queued=" + runtime.getQueuedCount()
                        + ", outstanding=" + runtime.getOutstandingTaskCount()
                        + ", finalizers=" + runtime.getFinalizerCount()
                        + ']',
                runtime.awaitClosed(deadline)
        );
        runtime.closeAfterDrained();
    }

    private static DefaultSqlExecutionCircuitBreakerConfiguration failingCircuitBreakerConfiguration(
            RuntimeException failure
    ) {
        final AtomicBoolean isArmed = new AtomicBoolean(true);
        return new DefaultSqlExecutionCircuitBreakerConfiguration() {
            @Override
            public int getCircuitBreakerThrottle() {
                if (isArmed.compareAndSet(true, false)) {
                    throw failure;
                }
                return super.getCircuitBreakerThrottle();
            }
        };
    }

    private static void park(FiberWalWaitQueue waitQueue) {
        final Fiber fiber = Objects.requireNonNull(Fiber.current());
        final FiberWaitCoordinator coordinator = fiber.getWaitCoordinator();
        final long token = fiber.beginWaitBuild(1);
        FiberWalWaitRegistration registration = null;
        try {
            registration = coordinator.acquireWal(token, 1);
            if (registration.register(waitQueue) != SourceRegistrationResult.ACCEPTED) {
                throw new IllegalStateException("test wait registration failed");
            }
            final int reason = fiber.suspendWait(token);
            registration.cancel();
            if (reason != FiberWaitCoordinator.REASON_WAL) {
                throw new IllegalStateException("unexpected wait reason");
            }
        } catch (Throwable th) {
            if (registration != null) {
                registration.cancel();
            }
            coordinator.abort(token);
            coordinator.consume(token);
            throw th;
        }
    }

    private static int parkWithCancellation(FiberWalWaitQueue waitQueue) {
        final Fiber fiber = Objects.requireNonNull(Fiber.current());
        final FiberWaitCoordinator coordinator = fiber.getWaitCoordinator();
        final FiberCancellationSignal cancellationSignal = SuspensionScope.getCancellationSignal();
        FiberCancellationSignal supplementalCancellationSignal =
                SuspensionScope.getSupplementalCancellationSignal();
        if (supplementalCancellationSignal == cancellationSignal) {
            supplementalCancellationSignal = null;
        }
        final int sourceCount = 1
                + (cancellationSignal != null ? 1 : 0)
                + (supplementalCancellationSignal != null ? 1 : 0);
        final long token = fiber.beginWaitBuild(sourceCount);
        try {
            final FiberWalWaitRegistration registration = coordinator.acquireWal(token, 1);
            if (registration.register(waitQueue) != SourceRegistrationResult.ACCEPTED) {
                throw new IllegalStateException("test wait registration failed");
            }
            if (cancellationSignal != null
                    && !coordinator.armCancellation(
                    token,
                    cancellationSignal,
                    SuspensionScope.getCancellationSignalGeneration()
            )) {
                throw new IllegalStateException("test cancellation registration failed");
            }
            if (supplementalCancellationSignal != null
                    && !coordinator.armCancellation(
                    token,
                    supplementalCancellationSignal,
                    SuspensionScope.getSupplementalCancellationSignalGeneration()
            )) {
                throw new IllegalStateException("test supplemental cancellation registration failed");
            }
            return fiber.suspendWait(token);
        } finally {
            coordinator.teardownWait(token);
        }
    }

    private static void runOrdered(
            PageFrameReduceDispatcher dispatcher,
            PageFrameSequence<?> frameSequence,
            MPSequence pubSeq,
            RingQueue<PageFrameReduceTask> queue,
            MCSequence subSeq
    ) {
        if (!dispatcher.tryAcquirePublication()) {
            throw new IllegalStateException("test dispatcher is unexpectedly quiescing");
        }
        try {
            final long cursor = pubSeq.next();
            if (cursor < 0) {
                throw new IllegalStateException("test publisher is unexpectedly blocked");
            }
            queue.get(cursor).of(frameSequence, 0, false);
            pubSeq.done(cursor);
        } finally {
            dispatcher.releasePublication();
        }
        if (dispatcher.consumeOrdered(0, queue, subSeq, null)) {
            throw new IllegalStateException("test dispatcher did not consume the task");
        }
    }

    private static void runUnordered(
            PageFrameReduceDispatcher dispatcher,
            UnorderedPageFrameSequence<?> frameSequence,
            MPSequence pubSeq,
            RingQueue<UnorderedPageFrameReduceTask> queue,
            MCSequence subSeq
    ) {
        if (!dispatcher.tryAcquirePublication()) {
            throw new IllegalStateException("test dispatcher is unexpectedly quiescing");
        }
        try {
            final long cursor = pubSeq.next();
            if (cursor < 0) {
                throw new IllegalStateException("test publisher is unexpectedly blocked");
            }
            queue.get(cursor).of(frameSequence, 0);
            pubSeq.done(cursor);
        } finally {
            dispatcher.releasePublication();
        }
        if (dispatcher.consumeUnordered(0, queue, subSeq, null)) {
            throw new IllegalStateException("test dispatcher did not consume the task");
        }
    }

    private static final class ClaimNotifyingMCSequence extends MCSequence {
        private final CountDownLatch cursorClaimed = new CountDownLatch(1);

        private ClaimNotifyingMCSequence(int cycle) {
            super(cycle);
        }

        @Override
        public long next() {
            final long cursor = super.next();
            if (cursor > -1) {
                cursorClaimed.countDown();
            }
            return cursor;
        }

        private boolean awaitClaim() throws InterruptedException {
            return cursorClaimed.await(5, TimeUnit.SECONDS);
        }
    }

    private static final class BlockingDoneMCSequence extends MCSequence {
        private final CountDownLatch doneEntered = new CountDownLatch(1);
        private final CountDownLatch doneRelease = new CountDownLatch(1);

        private BlockingDoneMCSequence(int cycle) {
            super(cycle);
        }

        @Override
        public void done(long cursor) {
            if (cursor == 0) {
                doneEntered.countDown();
                try {
                    if (!doneRelease.await(5, TimeUnit.SECONDS)) {
                        throw new AssertionError("timed out waiting to release sequence completion");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
            }
            super.done(cursor);
        }

        private boolean awaitDoneEntry() throws InterruptedException {
            return doneEntered.await(5, TimeUnit.SECONDS);
        }

        private void releaseDone() {
            doneRelease.countDown();
        }
    }
}
