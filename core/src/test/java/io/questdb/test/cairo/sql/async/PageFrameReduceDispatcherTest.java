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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.async.PageFrameReduceDispatcher;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.cairo.sql.async.UnorderedPageFrameReduceTask;
import io.questdb.cairo.sql.async.UnorderedPageFrameSequence;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.table.AsyncFilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncGroupByRecordCursorFactory;
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
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.lang.management.ManagementFactory;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class PageFrameReduceDispatcherTest extends AbstractCairoTest {
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
                    final long observedProgress = dispatcher.getProgressVersion();
                    while (true) {
                        final int reason = dispatcher.awaitProgress(observedProgress, null);
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
                close(ownerRuntime);
                close(dispatcherRuntime);
                Misc.free(dispatcher);
                Misc.free(frameSequence);
                Misc.free(queue);
            }
        });
    }

    @Test
    public void testOrderedReducerErrorWinsCancellationBeforeCompletion() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE ordered_error AS (SELECT x FROM long_sequence(1))");
            final FiberRuntime dispatcherRuntime = new FiberRuntime(1);
            final FiberRuntime ownerRuntime = new FiberRuntime(1);
            final FiberCancellationSignal ownerCancellation = new FiberCancellationSignal();
            final CountDownLatch errorRecorded = new CountDownLatch(1);
            final CountDownLatch errorRelease = new CountDownLatch(1);
            final AtomicReference<Throwable> dispatcherFailure = new AtomicReference<>();
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
                        throw new IllegalStateException("ordered reducer failure");
                    },
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    1,
                    PageFrameReduceTask.TYPE_FILTER
            ) {
                @Override
                public void cancel(int reason) {
                    super.cancel(reason);
                    if (reason == SqlExecutionCircuitBreaker.STATE_OK && errorRecorded.getCount() != 0) {
                        errorRecorded.countDown();
                        TestUtils.await(errorRelease);
                    }
                }
            };
            Thread dispatcherThread = null;
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
                    public FiberCancellationSignal getCancellationSignal() {
                        return ownerCancellation;
                    }

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
                Assert.assertFalse(ownerTask.isDone());

                dispatcherThread = new Thread(() -> {
                    try {
                        dispatcherRuntime.drain(1);
                    } catch (Throwable th) {
                        dispatcherFailure.set(th);
                    }
                });
                dispatcherThread.start();
                Assert.assertTrue(errorRecorded.await(5, TimeUnit.SECONDS));

                ownerCancellation.cancel();
                Assert.assertEquals(1, ownerRuntime.drain(1));
                Assert.assertFalse(ownerTask.isDone());
                Assert.assertNull(ownerFailure.get());

                errorRelease.countDown();
                dispatcherThread.join(5_000);
                Assert.assertFalse(dispatcherThread.isAlive());
                Assert.assertNull(dispatcherFailure.get());
                Assert.assertEquals(1, ownerRuntime.drain(1));
                Assert.assertTrue(ownerTask.isDone());
                Assert.assertTrue(ownerFailure.get() instanceof CairoException);
                TestUtils.assertContains(ownerFailure.get().getMessage(), "ordered reducer failure");
            } finally {
                errorRelease.countDown();
                if (dispatcherThread != null) {
                    dispatcherThread.join(5_000);
                }
                close(ownerRuntime);
                close(dispatcherRuntime);
                Misc.free(dispatcher);
                Misc.free(frameSequence);
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
                Assert.assertEquals(FiberWaitCoordinator.REASON_SHUTDOWN, waitReason.get());
                Assert.assertTrue(dispatcher.isQuiesced());
            } finally {
                close(ownerRuntime);
                close(dispatcherRuntime);
                Misc.free(dispatcher);
            }
        });
    }

    @Test
    public void testSameRuntimeOwnerCannotPublish() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                    engine,
                    engine.getMessageBus(),
                    runtime
            );
            final AtomicReference<Throwable> failure = new AtomicReference<>();
            final FiberTask ownerTask = new FiberTask() {
                @Override
                protected void onError(Throwable th) {
                    failure.set(th);
                }

                @Override
                protected boolean runStep() {
                    if (dispatcher.tryAcquirePublication()) {
                        dispatcher.releasePublication();
                    }
                    return true;
                }
            };
            try {
                Assert.assertSame(LaunchResult.LAUNCHED, runtime.launch(ownerTask));
                Assert.assertEquals(1, runtime.drain(1));

                Assert.assertTrue(ownerTask.isDone());
                Assert.assertNotNull(failure.get());
                Assert.assertEquals(IllegalStateException.class, failure.get().getClass());
                Assert.assertEquals(
                        "page frame owner cannot publish work to its current fiber runtime",
                        failure.get().getMessage()
                );
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
    public void testSameRuntimeProductionPublishersCannotPublish() throws Exception {
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

                assertSameRuntimeQueryCannotPublish(
                        runtime,
                        dispatcher,
                        "SELECT * FROM tab WHERE x > 0",
                        AsyncFilteredRecordCursorFactory.class
                );
                assertSameRuntimeQueryCannotPublish(
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
            final FiberTask ownerTask = new FiberTask() {
                @Override
                protected boolean runStep() {
                    Assert.assertFalse(dispatcher.consumeOrdered(-1, queue, subSeq, frameSequence));
                    return true;
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
                Assert.assertEquals(1, ownerRuntime.drain(1));
                Assert.assertEquals(1, subSeq.current());
                Assert.assertEquals(1, dispatcherRuntime.drain(1));
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
            final java.lang.management.ThreadMXBean mxBean = ManagementFactory.getThreadMXBean();
            Assume.assumeTrue(mxBean instanceof com.sun.management.ThreadMXBean);
            final com.sun.management.ThreadMXBean threadMXBean = (com.sun.management.ThreadMXBean) mxBean;
            Assume.assumeTrue(threadMXBean.isThreadAllocatedMemorySupported());
            if (!threadMXBean.isThreadAllocatedMemoryEnabled()) {
                threadMXBean.setThreadAllocatedMemoryEnabled(true);
            }

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
            try {
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
    public void testTaskPoolGapLeavesOrderedCursorUnclaimed() throws Exception {
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
                Assert.assertEquals(0, runtime.getOutstandingTaskCount());
                Assert.assertEquals(1, dispatcher.getCreatedTaskCount());

                Assert.assertTrue(dispatcher.consumeOrdered(1, queue, subSeq, null));
                Assert.assertEquals(0, subSeq.current());

                subSeq.releaseDone();
                firstConsumer.join(5_000);
                Assert.assertFalse("first consumer did not return", firstConsumer.isAlive());
                Assert.assertNull(failure.get());

                Assert.assertFalse(dispatcher.consumeOrdered(1, queue, subSeq, null));
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
            final FiberTask ownerTask = new FiberTask() {
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
    public void testUnorderedReducerErrorWinsCancellationBeforeCompletion() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE unordered_error AS (SELECT x FROM long_sequence(1))");
            final FiberRuntime dispatcherRuntime = new FiberRuntime(1);
            final FiberRuntime ownerRuntime = new FiberRuntime(1);
            final FiberCancellationSignal ownerCancellation = new FiberCancellationSignal();
            final CountDownLatch errorRecorded = new CountDownLatch(1);
            final CountDownLatch errorRelease = new CountDownLatch(1);
            final AtomicReference<Throwable> dispatcherFailure = new AtomicReference<>();
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
            ) {
                @Override
                public void cancel(int reason) {
                    super.cancel(reason);
                    if (reason == SqlExecutionCircuitBreaker.STATE_OK && errorRecorded.getCount() != 0) {
                        errorRecorded.countDown();
                        TestUtils.await(errorRelease);
                    }
                }
            };
            Thread dispatcherThread = null;
            try (RecordCursorFactory factory = select("SELECT * FROM unordered_error")) {
                frameSequence.of(factory, sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                frameSequence.prepareForDispatch();
                final FiberTask ownerTask = new FiberTask() {
                    @Override
                    public FiberCancellationSignal getCancellationSignal() {
                        return ownerCancellation;
                    }

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
                Assert.assertFalse(ownerTask.isDone());

                dispatcherThread = new Thread(() -> {
                    try {
                        dispatcherRuntime.drain(1);
                    } catch (Throwable th) {
                        dispatcherFailure.set(th);
                    }
                });
                dispatcherThread.start();
                Assert.assertTrue(errorRecorded.await(5, TimeUnit.SECONDS));

                ownerCancellation.cancel();
                Assert.assertEquals(1, ownerRuntime.drain(1));
                Assert.assertFalse(ownerTask.isDone());
                Assert.assertNull(ownerFailure.get());

                errorRelease.countDown();
                dispatcherThread.join(5_000);
                Assert.assertFalse(dispatcherThread.isAlive());
                Assert.assertNull(dispatcherFailure.get());
                Assert.assertEquals(1, ownerRuntime.drain(1));
                Assert.assertTrue(ownerTask.isDone());
                Assert.assertTrue(ownerFailure.get() instanceof CairoException);
                TestUtils.assertContains(ownerFailure.get().getMessage(), "unordered reducer failure");
            } finally {
                errorRelease.countDown();
                if (dispatcherThread != null) {
                    dispatcherThread.join(5_000);
                }
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

    private void assertSameRuntimeQueryCannotPublish(
            FiberRuntime runtime,
            PageFrameReduceDispatcher dispatcher,
            String sql,
            Class<?> expectedFactoryClass
    ) throws Exception {
        final AtomicReference<Throwable> failure = new AtomicReference<>();
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
                        cursor.hasNext();
                    } catch (SqlException e) {
                        throw new AssertionError(e);
                    }
                    return true;
                }
            };

            Assert.assertSame(LaunchResult.LAUNCHED, runtime.launch(ownerTask));
            Assert.assertEquals(1, runtime.drain(1));

            Assert.assertTrue(ownerTask.isDone());
            Assert.assertNotNull(failure.get());
            Assert.assertTrue(
                    failure.get().toString(),
                    failure.get() instanceof CairoException || failure.get() instanceof IllegalStateException
            );
            TestUtils.assertContains(
                    failure.get().getMessage(),
                    "page frame owner cannot publish work to its current fiber runtime"
            );
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
        Assert.assertTrue(runtime.awaitClosed(deadline));
        runtime.closeAfterDrained();
    }

    private static void park(FiberWalWaitQueue waitQueue) {
        final Fiber fiber = Objects.requireNonNull(Fiber.current());
        final FiberWaitCoordinator coordinator = fiber.getWaitCoordinator();
        final long token = fiber.beginWaitBuild(1);
        FiberWalWaitRegistration registration = null;
        try {
            registration = coordinator.acquireWal(token, 1);
            if (registration.register(waitQueue) != SourceRegistrationResult.ACCEPTED
                    || !coordinator.tryAcceptSource(token)) {
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
