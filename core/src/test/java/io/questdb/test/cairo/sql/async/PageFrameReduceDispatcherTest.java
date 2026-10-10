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
import io.questdb.MessageBusImpl;
import io.questdb.PropertyKey;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoConfigurationWrapper;
import io.questdb.cairo.CairoEngine;
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
import io.questdb.cairo.sql.async.UnorderedPageFrameReducer;
import io.questdb.cairo.sql.async.UnorderedPageFrameSequence;
import io.questdb.cairo.sql.async.WorkStealingStrategy;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.table.AsyncFilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncGroupByRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncJitFilteredRecordCursorFactory;
import io.questdb.mp.Job;
import io.questdb.mp.MCSequence;
import io.questdb.mp.MPSequence;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.mp.continuation.Fiber;
import io.questdb.mp.continuation.FiberCancellationSignal;
import io.questdb.mp.continuation.FiberDispatchContext;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.FiberRuntimeState;
import io.questdb.mp.continuation.FiberTask;
import io.questdb.mp.continuation.FiberWaitCoordinator;
import io.questdb.mp.continuation.FiberWalWaitQueue;
import io.questdb.mp.continuation.FiberWalWaitRegistration;
import io.questdb.mp.continuation.LaunchResult;
import io.questdb.mp.continuation.SourceRegistrationResult;
import io.questdb.mp.continuation.SuspensionScope;
import io.questdb.mp.continuation.TimerShards;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

public class PageFrameReduceDispatcherTest extends AbstractCairoTest {

    @Test
    public void testBatchFrameLimitRetainsMasterBoundary() throws Exception {
        assertBatchFrameLimit(false);
    }

    @Test
    public void testBatchFrameLimitSurvivesPreemption() throws Exception {
        assertBatchFrameLimit(true);
    }

    @Test
    public void testBatchOrderedBoundaryPollFailureCompletesOwnership() throws Exception {
        assertMemoryLeak(() -> {
            final RuntimeException injected = new RuntimeException("injected ordered boundary poll failure");
            final FiberDispatchContext context = new FiberDispatchContext() {
            };
            final FiberCancellationSignal supplementalSignal = new FiberCancellationSignal();
            final AtomicBooleanCircuitBreaker circuitBreaker = new AtomicBooleanCircuitBreaker(engine);
            circuitBreaker.setCancelledFlag(supplementalSignal);
            final RecordingFiberDispatchController controller = new RecordingFiberDispatchController();
            final FiberRuntime runtime = controller.createRuntime(1);
            final RingQueue<PageFrameReduceTask> queue = new RingQueue<>(
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    2
            );
            final MPSequence pubSeq = new MPSequence(queue.getCycle());
            final MCSequence subSeq = new MCSequence(queue.getCycle());
            pubSeq.then(subSeq).then(pubSeq);
            final AtomicInteger callbackCount = new AtomicInteger();
            final PageFrameSequence<StatefulAtom> frameSequence = new PageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    (_, _, _, _, _) -> callbackCount.incrementAndGet(),
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    1,
                    PageFrameReduceTask.TYPE_FILTER
            ) {
                @Override
                public SqlExecutionCircuitBreaker getCircuitBreaker() {
                    return circuitBreaker;
                }

                @Override
                public FiberDispatchContext getDispatchContext() {
                    return context;
                }

                @Override
                public long getFrameRowCount(int frameIndex) {
                    return 1;
                }
            };
            controller.setCooperativePollAction(() -> {
                Assert.assertSame(frameSequence.getCancellationSignal(), SuspensionScope.getCancellationSignal());
                Assert.assertSame(supplementalSignal, SuspensionScope.getSupplementalCancellationSignal());
                throw injected;
            });
            final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                    engine,
                    engine.getMessageBus(),
                    runtime
            );
            dispatcher.setBatchCheckRowsForTesting(0);
            try {
                for (int i = 0; i < 2; i++) {
                    final long cursor = pubSeq.next();
                    Assert.assertTrue(cursor > -1);
                    queue.get(cursor).of(frameSequence, i, false);
                    pubSeq.done(cursor);
                }

                Assert.assertFalse(dispatcher.consumeOrdered(-1, queue, subSeq, null));
                final long deadline = System.nanoTime() + 5_000_000_000L;
                while (runtime.getOutstandingTaskCount() > 0 && System.nanoTime() < deadline) {
                    runtime.drain(1);
                }

                Assert.assertEquals(0, runtime.getOutstandingTaskCount());
                Assert.assertEquals(0, runtime.drain(1));
                Assert.assertEquals(1, controller.getCooperativePollCount());
                Assert.assertEquals(1, controller.getMountCount());
                Assert.assertEquals(1, controller.getUnmountCount());
                Assert.assertEquals(1, callbackCount.get());
                Assert.assertEquals(1, subSeq.current());
                Assert.assertEquals(2, frameSequence.getReduceFinishedCounter().get());
                Assert.assertFalse(frameSequence.isActive());
                Assert.assertTrue(queue.get(1).hasError());
                TestUtils.assertContains(queue.get(1).buildError().getMessage(), injected.getMessage());
            } finally {
                close(runtime);
                Misc.free(dispatcher);
                Misc.free(frameSequence);
                Misc.free(queue);
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
    public void testBatchSwitchesDispatchContextBetweenOwners() throws Exception {
        assertMemoryLeak(() -> {
            final FiberDispatchContext contextA = new FiberDispatchContext() {
            };
            final FiberDispatchContext contextB = new FiberDispatchContext() {
            };
            final FiberCancellationSignal supplementalSignalA = new FiberCancellationSignal();
            final FiberCancellationSignal supplementalSignalB = new FiberCancellationSignal();
            final AtomicBooleanCircuitBreaker circuitBreakerA = new AtomicBooleanCircuitBreaker(engine);
            final AtomicBooleanCircuitBreaker circuitBreakerB = new AtomicBooleanCircuitBreaker(engine);
            circuitBreakerA.setCancelledFlag(supplementalSignalA);
            circuitBreakerB.setCancelledFlag(supplementalSignalB);
            final RecordingFiberDispatchController controller = new RecordingFiberDispatchController();
            controller.setCooperativePollAction(() -> Assert.assertTrue(Fiber.yieldForDispatch()));
            final FiberRuntime runtime = controller.createRuntime(1);
            final RingQueue<PageFrameReduceTask> queue = new RingQueue<>(
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    4
            );
            final MPSequence pubSeq = new MPSequence(queue.getCycle());
            final MCSequence subSeq = new MCSequence(queue.getCycle());
            pubSeq.then(subSeq).then(pubSeq);
            final AtomicInteger callbackCount = new AtomicInteger();
            final AtomicReference<PageFrameSequence<?>> frameSequenceARef = new AtomicReference<>();
            final FiberCancellationSignal[] observedPrimarySignals = new FiberCancellationSignal[2];
            final FiberCancellationSignal[] observedSupplementalSignals = new FiberCancellationSignal[2];
            final PageFrameSequence<StatefulAtom> frameSequenceA = new PageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    (_, _, task, _, _) -> {
                        final PageFrameSequence<?> frameSequence = task.getFrameSequence();
                        final int index = frameSequence == frameSequenceARef.get() ? 0 : 1;
                        observedPrimarySignals[index] = SuspensionScope.getCancellationSignal();
                        observedSupplementalSignals[index] = SuspensionScope.getSupplementalCancellationSignal();
                        callbackCount.incrementAndGet();
                    },
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    1,
                    PageFrameReduceTask.TYPE_FILTER
            ) {
                @Override
                public SqlExecutionCircuitBreaker getCircuitBreaker() {
                    return circuitBreakerA;
                }

                @Override
                public FiberDispatchContext getDispatchContext() {
                    return contextA;
                }

                @Override
                public long getFrameRowCount(int frameIndex) {
                    return 1;
                }
            };
            final PageFrameSequence<StatefulAtom> frameSequenceB = new PageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    (_, _, task, _, _) -> {
                        final PageFrameSequence<?> frameSequence = task.getFrameSequence();
                        final int index = frameSequence == frameSequenceARef.get() ? 0 : 1;
                        observedPrimarySignals[index] = SuspensionScope.getCancellationSignal();
                        observedSupplementalSignals[index] = SuspensionScope.getSupplementalCancellationSignal();
                        callbackCount.incrementAndGet();
                    },
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    1,
                    PageFrameReduceTask.TYPE_FILTER
            ) {
                @Override
                public SqlExecutionCircuitBreaker getCircuitBreaker() {
                    return circuitBreakerB;
                }

                @Override
                public FiberDispatchContext getDispatchContext() {
                    return contextB;
                }
            };
            final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                    engine,
                    engine.getMessageBus(),
                    runtime
            );
            dispatcher.setBatchCheckRowsForTesting(0);
            try {
                frameSequenceARef.set(frameSequenceA);
                long cursor = pubSeq.next();
                Assert.assertTrue(cursor > -1);
                queue.get(cursor).of(frameSequenceA, 0, false);
                pubSeq.done(cursor);
                cursor = pubSeq.next();
                Assert.assertTrue(cursor > -1);
                queue.get(cursor).of(frameSequenceB, 0, false);
                pubSeq.done(cursor);
                cursor = pubSeq.next();
                Assert.assertTrue(cursor > -1);
                queue.get(cursor).of(frameSequenceB, 0, false);
                pubSeq.done(cursor);

                Assert.assertFalse(dispatcher.consumeOrdered(-1, queue, subSeq, null));
                final long deadline = System.nanoTime() + 5_000_000_000L;
                while (runtime.getOutstandingTaskCount() > 0 && System.nanoTime() < deadline) {
                    runtime.drain(1);
                }

                Assert.assertEquals(0, runtime.getOutstandingTaskCount());
                Assert.assertEquals(1, controller.getCooperativePollCount());
                Assert.assertSame(contextB, controller.getPolledContext(0));
                Assert.assertEquals(3, controller.getMountCount());
                Assert.assertSame(contextA, controller.getMountedContext(0));
                Assert.assertSame(contextB, controller.getMountedContext(1));
                Assert.assertSame(contextB, controller.getMountedContext(2));
                Assert.assertEquals(3, controller.getUnmountCount());
                Assert.assertEquals(3, callbackCount.get());
                Assert.assertSame(frameSequenceA.getCancellationSignal(), observedPrimarySignals[0]);
                Assert.assertSame(frameSequenceB.getCancellationSignal(), observedPrimarySignals[1]);
                Assert.assertSame(supplementalSignalA, observedSupplementalSignals[0]);
                Assert.assertSame(supplementalSignalB, observedSupplementalSignals[1]);
                Assert.assertEquals(1, frameSequenceA.getReduceFinishedCounter().get());
                Assert.assertEquals(2, frameSequenceB.getReduceFinishedCounter().get());
            } finally {
                close(runtime);
                Misc.free(dispatcher);
                Misc.free(frameSequenceA);
                Misc.free(frameSequenceB);
                Misc.free(queue);
            }
        });
    }

    @Test
    public void testBatchSwitchesUnorderedCancellationScopeBetweenOwners() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE batch_a AS (
                        SELECT x, timestamp_sequence(0, 86_400_000_000) ts
                        FROM long_sequence(2)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    CREATE TABLE batch_b AS (
                        SELECT x, timestamp_sequence(0, 86_400_000_000) ts
                        FROM long_sequence(3)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            final AtomicLong dispatchOwnerId = new AtomicLong(1);
            final FiberDispatchContext context = new FiberDispatchContext() {
                @Override
                public long getQueryRegistryOwnerId() {
                    return dispatchOwnerId.get();
                }
            };
            final FiberCancellationSignal supplementalSignalA = new FiberCancellationSignal();
            final FiberCancellationSignal supplementalSignalB = new FiberCancellationSignal();
            final AtomicBooleanCircuitBreaker circuitBreakerA = new AtomicBooleanCircuitBreaker(engine);
            final AtomicBooleanCircuitBreaker circuitBreakerB = new AtomicBooleanCircuitBreaker(engine);
            circuitBreakerA.setCancelledFlag(supplementalSignalA);
            circuitBreakerB.setCancelledFlag(supplementalSignalB);
            final RecordingFiberDispatchController controller = new RecordingFiberDispatchController();
            controller.setCooperativePollAction(() -> Assert.assertTrue(Fiber.yieldForDispatch()));
            final FiberRuntime runtime = controller.createRuntime(1);
            final RingQueue<UnorderedPageFrameReduceTask> queue = engine.getMessageBus().getUnorderedPageFrameReduceQueue();
            final MCSequence subSeq = engine.getMessageBus().getUnorderedPageFrameReduceSubSeq();
            final AtomicInteger callbackCount = new AtomicInteger();
            final AtomicReference<UnorderedPageFrameSequence<?>> frameSequenceARef = new AtomicReference<>();
            final FiberCancellationSignal[] observedPrimarySignals = new FiberCancellationSignal[2];
            final FiberCancellationSignal[] observedSupplementalSignals = new FiberCancellationSignal[2];
            final UnorderedPageFrameReducer reducer = (_, _, _, _, frameSequence, _) -> {
                final int index = frameSequence == frameSequenceARef.get() ? 0 : 1;
                observedPrimarySignals[index] = SuspensionScope.getCancellationSignal();
                observedSupplementalSignals[index] = SuspensionScope.getSupplementalCancellationSignal();
                callbackCount.incrementAndGet();
            };
            final BlockedOwner ownerA = new BlockedOwner();
            final BlockedOwner ownerB = new BlockedOwner();
            final UnorderedPageFrameSequence<StatefulAtom> frameSequenceA = new UnorderedPageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    ownerA.wrap(reducer),
                    1
            ) {
                @Override
                public SqlExecutionCircuitBreaker getCircuitBreaker() {
                    return circuitBreakerA;
                }

                @Override
                public FiberDispatchContext getDispatchContext() {
                    return context;
                }

                @Override
                public long getFrameRowCount(int frameIndex) {
                    return 1;
                }
            };
            final UnorderedPageFrameSequence<StatefulAtom> frameSequenceB = new UnorderedPageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    ownerB.wrap(reducer),
                    1
            ) {
                @Override
                public SqlExecutionCircuitBreaker getCircuitBreaker() {
                    return circuitBreakerB;
                }

                @Override
                public FiberDispatchContext getDispatchContext() {
                    dispatchOwnerId.set(2);
                    return context;
                }
            };
            final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                    engine,
                    engine.getMessageBus(),
                    runtime
            );
            dispatcher.setBatchCheckRowsForTesting(0);
            try (
                    RecordCursorFactory factoryA = select("SELECT * FROM batch_a");
                    RecordCursorFactory factoryB = select("SELECT * FROM batch_b")
            ) {
                frameSequenceARef.set(frameSequenceA);
                frameSequenceA.of(factoryA, sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                frameSequenceA.prepareForDispatch();
                ownerA.start(frameSequenceA);
                frameSequenceB.of(factoryB, sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                frameSequenceB.prepareForDispatch();
                ownerB.start(frameSequenceB);

                // A's ticket claims A's last frame; B's ticket claims frame 1 and, handed back, frame 2.
                Assert.assertFalse(dispatcher.consumeUnordered(-1, queue, subSeq));
                final long deadline = System.nanoTime() + 5_000_000_000L;
                while (runtime.getOutstandingTaskCount() > 0 && System.nanoTime() < deadline) {
                    runtime.drain(1);
                }

                Assert.assertEquals(0, runtime.getOutstandingTaskCount());
                Assert.assertEquals(1, controller.getCooperativePollCount());
                Assert.assertSame(context, controller.getPolledContext(0));
                Assert.assertEquals(2, controller.getPolledOwnerId(0));
                Assert.assertEquals(3, controller.getMountCount());
                Assert.assertSame(context, controller.getMountedContext(0));
                Assert.assertSame(context, controller.getMountedContext(1));
                Assert.assertSame(context, controller.getMountedContext(2));
                Assert.assertEquals(1, controller.getMountedOwnerId(0));
                Assert.assertEquals(2, controller.getMountedOwnerId(1));
                Assert.assertEquals(2, controller.getMountedOwnerId(2));
                Assert.assertEquals(3, controller.getUnmountCount());
                Assert.assertEquals(3, callbackCount.get());
                Assert.assertSame(frameSequenceA.getCancellationSignal(), observedPrimarySignals[0]);
                Assert.assertSame(frameSequenceB.getCancellationSignal(), observedPrimarySignals[1]);
                Assert.assertSame(supplementalSignalA, observedSupplementalSignals[0]);
                Assert.assertSame(supplementalSignalB, observedSupplementalSignals[1]);
                Assert.assertEquals(-1, frameSequenceA.getDoneLatch().getCount());
                Assert.assertEquals(-2, frameSequenceB.getDoneLatch().getCount());
            } finally {
                ownerA.close();
                ownerB.close();
                close(runtime);
                Misc.free(dispatcher);
                Misc.free(frameSequenceA);
                Misc.free(frameSequenceB);
            }
        });
    }

    @Test
    public void testBatchUnorderedBoundaryPollFailureCompletesOwnership() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE boundary_tab AS (
                        SELECT x, timestamp_sequence(0, 86_400_000_000) ts
                        FROM long_sequence(3)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            final RuntimeException injected = new RuntimeException("injected unordered boundary poll failure");
            final FiberDispatchContext context = new FiberDispatchContext() {
            };
            final FiberCancellationSignal supplementalSignal = new FiberCancellationSignal();
            final AtomicBooleanCircuitBreaker circuitBreaker = new AtomicBooleanCircuitBreaker(engine);
            circuitBreaker.setCancelledFlag(supplementalSignal);
            final RecordingFiberDispatchController controller = new RecordingFiberDispatchController();
            final FiberRuntime runtime = controller.createRuntime(1);
            final RingQueue<UnorderedPageFrameReduceTask> queue = engine.getMessageBus().getUnorderedPageFrameReduceQueue();
            final MCSequence subSeq = engine.getMessageBus().getUnorderedPageFrameReduceSubSeq();
            final AtomicInteger callbackCount = new AtomicInteger();
            final BlockedOwner owner = new BlockedOwner();
            final UnorderedPageFrameSequence<StatefulAtom> frameSequence = new UnorderedPageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    owner.wrap((_, _, _, _, _, _) -> callbackCount.incrementAndGet()),
                    1
            ) {
                @Override
                public SqlExecutionCircuitBreaker getCircuitBreaker() {
                    return circuitBreaker;
                }

                @Override
                public FiberDispatchContext getDispatchContext() {
                    return context;
                }

                @Override
                public long getFrameRowCount(int frameIndex) {
                    return 1;
                }
            };
            controller.setCooperativePollAction(() -> {
                Assert.assertSame(frameSequence.getCancellationSignal(), SuspensionScope.getCancellationSignal());
                Assert.assertSame(supplementalSignal, SuspensionScope.getSupplementalCancellationSignal());
                throw injected;
            });
            final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                    engine,
                    engine.getMessageBus(),
                    runtime
            );
            dispatcher.setBatchCheckRowsForTesting(0);
            try (RecordCursorFactory factory = select("SELECT * FROM boundary_tab")) {
                frameSequence.of(factory, sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                frameSequence.prepareForDispatch();
                owner.start(frameSequence);
                final long subSeqStart = subSeq.current();

                // The ticket claims frame 1; handed back, it claims frame 2, whose boundary poll fails.
                Assert.assertFalse(dispatcher.consumeUnordered(-1, queue, subSeq));
                final long deadline = System.nanoTime() + 5_000_000_000L;
                while (runtime.getOutstandingTaskCount() > 0 && System.nanoTime() < deadline) {
                    runtime.drain(1);
                }

                Assert.assertEquals(0, runtime.getOutstandingTaskCount());
                Assert.assertEquals(0, runtime.drain(1));
                Assert.assertEquals(1, controller.getCooperativePollCount());
                Assert.assertEquals(1, controller.getMountCount());
                Assert.assertEquals(1, controller.getUnmountCount());
                Assert.assertEquals(1, callbackCount.get());
                Assert.assertEquals(subSeqStart + 2, subSeq.current());
                Assert.assertEquals(-2, frameSequence.getDoneLatch().getCount());
                Assert.assertFalse(frameSequence.isActive());
                TestUtils.assertContains(frameSequence.buildError().getMessage(), injected.getMessage());
            } finally {
                owner.close();
                close(runtime);
                Misc.free(dispatcher);
                Misc.free(frameSequence);
            }
        });
    }

    @Test
    public void testBatchUnorderedRowCountFailureCompletesOwnership() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE row_count_tab AS (
                        SELECT x, timestamp_sequence(0, 86_400_000_000) ts
                        FROM long_sequence(3)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            final FiberRuntime runtime = new FiberRuntime(1);
            final BlockedOwner owner = new BlockedOwner();
            final UnorderedPageFrameSequence<StatefulAtom> frameSequence = new UnorderedPageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    owner.wrap((_, _, _, _, _, _) -> {
                    }),
                    1
            ) {
                @Override
                public SqlExecutionCircuitBreaker getCircuitBreaker() {
                    return SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
                }

                @Override
                public long getFrameRowCount(int frameIndex) {
                    if (frameIndex == 2) {
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
            try (RecordCursorFactory factory = select("SELECT * FROM row_count_tab")) {
                frameSequence.of(factory, sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                frameSequence.prepareForDispatch();
                owner.start(frameSequence);

                // The ticket claims frame 1; handed back, it claims frame 2, whose row count fails.
                Assert.assertFalse(dispatcher.consumeUnordered(
                        0,
                        engine.getMessageBus().getUnorderedPageFrameReduceQueue(),
                        engine.getMessageBus().getUnorderedPageFrameReduceSubSeq()
                ));
                Assert.assertEquals(-2, frameSequence.getDoneLatch().getCount());
                Assert.assertEquals(0, runtime.getOutstandingTaskCount());
            } finally {
                owner.close();
                close(runtime);
                Misc.free(dispatcher);
                Misc.free(frameSequence);
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
            execute("CREATE TABLE broken_tab AS (SELECT x FROM long_sequence(1))");
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
            try (RecordCursorFactory factory = select("SELECT * FROM broken_tab")) {
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

                unorderedFrameSequence.of(factory, sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                unorderedFrameSequence.prepareForDispatch();
                try {
                    unorderedFrameSequence.dispatchAndAwait();
                    Assert.fail("expected the reducer's broken connection");
                } catch (CairoException e) {
                    Assert.assertTrue(e.isInterruption());
                }
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
    public void testProgressBeforeTimerPreservesCancellation() throws Exception {
        assertProgressBeforeTimer(false, ProgressBeforeTimerScenario.PRIMARY_CANCEL);
        assertProgressBeforeTimer(true, ProgressBeforeTimerScenario.PRIMARY_CANCEL);
        assertProgressBeforeTimer(true, ProgressBeforeTimerScenario.SUPPLEMENTAL_CANCEL);
    }

    @Test
    public void testProgressBeforeTimerPreservesRuntimeQuiesce() throws Exception {
        assertProgressBeforeTimer(false, ProgressBeforeTimerScenario.RUNTIME_QUIESCE);
        assertProgressBeforeTimer(true, ProgressBeforeTimerScenario.RUNTIME_QUIESCE);
    }

    @Test
    public void testProgressBeforeTimerPreservesTimerShutdown() throws Exception {
        assertProgressBeforeTimer(false, ProgressBeforeTimerScenario.TIMER_SHUTDOWN);
        assertProgressBeforeTimer(true, ProgressBeforeTimerScenario.TIMER_SHUTDOWN);
        assertProgressBeforeTimer(false, ProgressBeforeTimerScenario.TIMER_SHUTDOWN_DURING_REGISTRATION);
        assertProgressBeforeTimer(true, ProgressBeforeTimerScenario.TIMER_SHUTDOWN_DURING_REGISTRATION);
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
            execute("""
                    CREATE TABLE quiesce_tab AS (
                        SELECT x, timestamp_sequence(0, 86_400_000_000) ts
                        FROM long_sequence(2)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
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
            final BlockedOwner owner = new BlockedOwner();
            final UnorderedPageFrameSequence<StatefulAtom> unorderedFrameSequence = new UnorderedPageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    owner.wrap((_, _, _, _, _, _) -> Assert.fail("unordered reducer must not run during shutdown drain")),
                    1
            );
            final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                    engine,
                    engine.getMessageBus(),
                    runtime
            );
            try (RecordCursorFactory factory = select("SELECT * FROM quiesce_tab")) {
                engine.getMessageBus().setPageFrameReduceDispatcher(dispatcher);

                final int shard = 0;
                final long orderedCursor = engine.getMessageBus().getPageFrameReducePubSeq(shard).next();
                Assert.assertTrue(orderedCursor > -1);
                engine.getMessageBus()
                        .getPageFrameReduceQueue(shard)
                        .get(orderedCursor)
                        .of(orderedFrameSequence, 0, false);
                engine.getMessageBus().getPageFrameReducePubSeq(shard).done(orderedCursor);

                // The owner holds frame 0 and leaves frame 1 to the ticket it queued.
                unorderedFrameSequence.of(factory, sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                unorderedFrameSequence.prepareForDispatch();
                owner.start(unorderedFrameSequence);

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
                // The drained ticket claimed nothing, and the owner is still inside frame 0.
                Assert.assertEquals(0, unorderedFrameSequence.getDoneLatch().getCount());
                owner.close();
                Assert.assertTrue(owner.getError() instanceof CairoException e && e.isCancellation());
            } finally {
                owner.close();
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
            execute("CREATE TABLE finished_tab AS (SELECT x FROM long_sequence(1))");
            final FiberRuntime runtime = new FiberRuntime(1);
            final AtomicInteger reduceCount = new AtomicInteger();
            final UnorderedPageFrameSequence<StatefulAtom> frameSequence = new UnorderedPageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    (_, _, _, _, _, _) -> reduceCount.incrementAndGet(),
                    1
            );
            final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                    engine,
                    engine.getMessageBus(),
                    runtime
            );
            try (RecordCursorFactory factory = select("SELECT * FROM finished_tab")) {
                engine.getMessageBus().setPageFrameReduceDispatcher(dispatcher);
                // No worker runs: the owner reduces the frame itself and leaves its ticket queued.
                frameSequence.of(factory, sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                frameSequence.prepareForDispatch();
                frameSequence.dispatchAndAwait();
                Assert.assertEquals(1, reduceCount.get());
                Assert.assertEquals(-1, frameSequence.getDoneLatch().getCount());

                runtime.beginQuiesce();

                Assert.assertTrue(dispatcher.isQuiesced());
                Assert.assertTrue(frameSequence.isActive());
                Assert.assertEquals(SqlExecutionCircuitBreaker.STATE_OK, frameSequence.getCancelReason());
                Assert.assertEquals(1, reduceCount.get());
                Assert.assertEquals(-1, frameSequence.getDoneLatch().getCount());
                Assert.assertEquals(-1, engine.getMessageBus().getUnorderedPageFrameReduceSubSeq().next());
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
                protected void onError(Throwable th) {
                    ownerFailure.compareAndSet(null, th);
                }

                @Override
                protected boolean runStep() {
                    // the resumed dispatcher fiber drains the whole queue in one batch
                    Assert.assertTrue(dispatcher.consumeOrdered(-1, queue, subSeq, frameSequence));
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
    public void testUnorderedLaunchFailureTransfersFiberAndTaskOwnership() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE launch_tab AS (
                        SELECT x, timestamp_sequence(0, 86_400_000_000) ts
                        FROM long_sequence(2)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            final FiberRuntime dispatcherRuntime = new FiberRuntime(1);
            final FiberRuntime ownerRuntime = new FiberRuntime(1);
            final RingQueue<UnorderedPageFrameReduceTask> queue = engine.getMessageBus().getUnorderedPageFrameReduceQueue();
            final MCSequence subSeq = engine.getMessageBus().getUnorderedPageFrameReduceSubSeq();
            final BlockedOwner failedOwner = new BlockedOwner();
            final BlockedOwner replacementOwner = new BlockedOwner();
            final UnorderedPageFrameSequence<StatefulAtom> failedFrameSequence = new UnorderedPageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    failedOwner.wrap((_, _, _, _, _, _) -> {
                    }),
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
                    replacementOwner.wrap((_, _, _, _, _, _) -> {
                    }),
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
                    runUnordered(dispatcher, queue, subSeq);
                    return true;
                }
            };
            try (
                    RecordCursorFactory failedFactory = select("SELECT * FROM launch_tab");
                    RecordCursorFactory replacementFactory = select("SELECT * FROM launch_tab")
            ) {
                failedFrameSequence.of(failedFactory, sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                failedFrameSequence.prepareForDispatch();
                failedOwner.start(failedFrameSequence);
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
                replacementFrameSequence.of(replacementFactory, sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                replacementFrameSequence.prepareForDispatch();
                replacementOwner.start(replacementFrameSequence);
                runUnordered(dispatcher, queue, subSeq);
                Assert.assertEquals(0, dispatcherRuntime.getOutstandingTaskCount());
                Assert.assertEquals(1, dispatcherRuntime.getCreatedFiberCount());
                Assert.assertEquals(1, dispatcher.getCreatedTaskCount());
                Assert.assertEquals(-1, replacementFrameSequence.getDoneLatch().getCount());
            } finally {
                dispatcherRuntime.setRunQueueDepthForTesting(0);
                failedOwner.close();
                replacementOwner.close();
                close(ownerRuntime);
                close(dispatcherRuntime);
                Misc.free(dispatcher);
                Misc.free(failedFrameSequence);
                Misc.free(replacementFrameSequence);
            }
        });
    }

    @Test
    public void testUnorderedManagedTailCompletionPollFailureCleansWait() throws Exception {
        assertUnorderedTailCompletion(TailCompletionScenario.MANAGED_POLL_FAILURE);
    }

    @Test
    public void testUnorderedManagedTailCompletionPollsAfterWaitTeardown() throws Exception {
        assertUnorderedTailCompletion(TailCompletionScenario.MANAGED_POLL);
    }

    @Test
    public void testUnorderedManagedTailCompletionPreservesCancellationAndShutdown() throws Exception {
        assertUnorderedTailCompletion(TailCompletionScenario.MANAGED_DISPATCHER_QUIESCE);
        assertUnorderedTailCompletion(TailCompletionScenario.MANAGED_OWNER_QUIESCE);
        assertUnorderedTailCompletion(TailCompletionScenario.MANAGED_PRIMARY_CANCEL);
        assertUnorderedTailCompletion(TailCompletionScenario.MANAGED_SUPPLEMENTAL_CANCEL);
        assertUnorderedTailCompletion(TailCompletionScenario.MANAGED_TIMER_SHUTDOWN);
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
                public void onBeforeOwnerStep() {
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
    public void testUnorderedOwnerObservesCancellationWhileQueueIsFull() throws Exception {
        // A leftover ticket keeps a one-slot queue full, so the owner reduces every frame itself on
        // the queue-full path. Cancelling during the first frame must stop it before the second.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE warm AS (SELECT x FROM long_sequence(1))");
            execute("""
                    CREATE TABLE scan AS (
                        SELECT x, timestamp_sequence(0, 86_400_000_000) ts
                        FROM long_sequence(16)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            final CairoConfiguration queueConfiguration = new CairoConfigurationWrapper(configuration) {
                @Override
                public int getUnorderedPageFrameReduceQueueCapacity() {
                    return 1;
                }
            };
            final AtomicInteger reduceCount = new AtomicInteger();
            final AtomicBooleanCircuitBreaker breaker = new AtomicBooleanCircuitBreaker(engine);
            try (
                    MessageBusImpl bus = new MessageBusImpl(queueConfiguration);
                    RecordCursorFactory warmFactory = select("SELECT * FROM warm");
                    RecordCursorFactory scanFactory = select("SELECT * FROM scan");
                    SqlExecutionContextImpl context = TestUtils.createSqlExecutionCtx(engine, 1)
            ) {
                // No worker runs, so the warm query's ticket stays in the queue after it finishes.
                try (UnorderedPageFrameSequence<StatefulAtom> warm = new UnorderedPageFrameSequence<>(
                        engine,
                        queueConfiguration,
                        bus,
                        new StatefulAtom() {
                        },
                        (_, _, _, _, _, _) -> {
                        },
                        1
                )) {
                    warm.of(warmFactory, sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                    warm.prepareForDispatch();
                    warm.dispatchAndAwait();
                }
                context.with(breaker);
                try (UnorderedPageFrameSequence<StatefulAtom> scan = new UnorderedPageFrameSequence<>(
                        engine,
                        queueConfiguration,
                        bus,
                        new StatefulAtom() {
                        },
                        (_, _, _, _, _, _) -> {
                            if (reduceCount.incrementAndGet() == 1) {
                                breaker.cancel();
                            }
                        },
                        1
                )) {
                    scan.of(scanFactory, context, PartitionFrameCursorFactory.ORDER_ASC);
                    scan.prepareForDispatch();
                    Assert.assertEquals(16, scan.getFrameCount());
                    try {
                        scan.dispatchAndAwait();
                        Assert.fail("expected cancellation");
                    } catch (CairoException e) {
                        Assert.assertTrue(e.isCancellation());
                    } finally {
                        scan.await();
                    }
                    Assert.assertEquals(1, reduceCount.get());
                }
            }
        });
    }

    @Test
    public void testUnorderedOwnerObservesCancellationWhileWaitingForWorkers() throws Exception {
        // A worker claims the only frame, and the query is cancelled while the worker reduces it.
        // The owner runs on a plain thread, so it cannot park: its wait for the worker's frame must
        // still notice the cancellation, otherwise the query completes as if it never happened.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tail_tab AS (SELECT x FROM long_sequence(1))");
            final Thread ownerThread = Thread.currentThread();
            final CountDownLatch workerFrameStarted = new CountDownLatch(1);
            final CountDownLatch cancelled = new CountDownLatch(1);
            final AtomicBooleanCircuitBreaker breaker = new AtomicBooleanCircuitBreaker(engine);
            final AtomicReference<Throwable> workerError = new AtomicReference<>();
            final WorkStealingStrategy strategy = new WorkStealingStrategy() {
                @Override
                public void onBeforeOwnerStep() {
                    TestUtils.await(workerFrameStarted);
                    breaker.cancel();
                    cancelled.countDown();
                }

                @Override
                public WorkStealingStrategy of(AtomicInteger startedCounter) {
                    return this;
                }

                @Override
                public boolean shouldSteal(int finishedCount) {
                    return true;
                }
            };
            final FactoryProvider factoryProvider = new DefaultFactoryProvider() {
                @Override
                public @NotNull WorkStealingStrategy getWorkStealingStrategy(
                        @NotNull CairoConfiguration configuration,
                        int workerCount,
                        @NotNull StatefulAtom atom
                ) {
                    return strategy;
                }
            };
            final CairoConfiguration sequenceConfiguration = new CairoConfigurationWrapper(configuration) {
                @Override
                public @NotNull FactoryProvider getFactoryProvider() {
                    return factoryProvider;
                }
            };
            try (
                    RecordCursorFactory factory = select("SELECT * FROM tail_tab");
                    SqlExecutionContextImpl context = TestUtils.createSqlExecutionCtx(engine, 1);
                    UnorderedPageFrameReduceJob job = new UnorderedPageFrameReduceJob(engine, engine.getMessageBus());
                    UnorderedPageFrameSequence<StatefulAtom> frameSequence = new UnorderedPageFrameSequence<>(
                            engine,
                            sequenceConfiguration,
                            engine.getMessageBus(),
                            new StatefulAtom() {
                            },
                            (_, _, _, _, sequence, _) -> {
                                Assert.assertNotSame(ownerThread, Thread.currentThread());
                                workerFrameStarted.countDown();
                                TestUtils.await(cancelled);
                                // Only an owner that observed the cancellation cancels the sequence.
                                final long deadline = System.nanoTime() + 500_000_000L;
                                while (sequence.isActive() && System.nanoTime() < deadline) {
                                    LockSupport.parkNanos(100_000);
                                }
                            },
                            1
                    )
            ) {
                context.with(breaker);
                frameSequence.of(factory, context, PartitionFrameCursorFactory.ORDER_ASC);
                frameSequence.prepareForDispatch();
                Assert.assertEquals(1, frameSequence.getFrameCount());
                final Thread worker = new Thread(() -> {
                    try {
                        final long deadline = System.nanoTime() + 5_000_000_000L;
                        while (!job.run(Job.RUNNING_STATUS) && System.nanoTime() < deadline) {
                            Thread.onSpinWait();
                        }
                    } catch (Throwable th) {
                        workerError.set(th);
                        workerFrameStarted.countDown();
                    }
                });
                worker.start();
                boolean isCancellationObserved = false;
                try {
                    frameSequence.dispatchAndAwait();
                } catch (CairoException e) {
                    isCancellationObserved = e.isCancellation();
                } finally {
                    cancelled.countDown();
                    worker.join(5_000);
                    frameSequence.await();
                }
                Assert.assertFalse(worker.isAlive());
                if (workerError.get() != null) {
                    throw new AssertionError(workerError.get());
                }
                Assert.assertTrue(isCancellationObserved);
            }
        });
    }

    @Test
    public void testUnorderedOwnerReducesOnlyOwnFrames() throws Exception {
        // A slow query's owner blocks inside its first frame, leaving its other frames unclaimed
        // and its tickets queued. A fast query must finish without running any of those frames.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE slow_tab AS (
                        SELECT x, timestamp_sequence(0, 86_400_000_000) ts
                        FROM long_sequence(4)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    CREATE TABLE fast_tab AS (
                        SELECT x, timestamp_sequence(0, 86_400_000_000) ts
                        FROM long_sequence(2)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            final Thread testThread = Thread.currentThread();
            final CountDownLatch slowFrameStarted = new CountDownLatch(1);
            final CountDownLatch slowFrameRelease = new CountDownLatch(1);
            final AtomicInteger slowReduceCount = new AtomicInteger();
            final AtomicInteger stolenSlowReduceCount = new AtomicInteger();
            final AtomicInteger fastReduceCount = new AtomicInteger();
            final AtomicReference<Throwable> slowOwnerError = new AtomicReference<>();
            try (
                    RecordCursorFactory slowFactory = select("SELECT * FROM slow_tab");
                    RecordCursorFactory fastFactory = select("SELECT * FROM fast_tab");
                    PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
                    SqlExecutionCircuitBreakerWrapper circuitBreaker = new SqlExecutionCircuitBreakerWrapper(
                            engine,
                            configuration.getCircuitBreakerConfiguration()
                    );
                    UnorderedPageFrameSequence<StatefulAtom> slowSequence = new UnorderedPageFrameSequence<>(
                            engine,
                            configuration,
                            engine.getMessageBus(),
                            new StatefulAtom() {
                            },
                            (_, _, _, _, _, _) -> {
                                if (Thread.currentThread() == testThread) {
                                    stolenSlowReduceCount.incrementAndGet();
                                    return;
                                }
                                if (slowReduceCount.getAndIncrement() == 0) {
                                    slowFrameStarted.countDown();
                                    TestUtils.await(slowFrameRelease);
                                }
                            },
                            1
                    );
                    UnorderedPageFrameSequence<StatefulAtom> fastSequence = new UnorderedPageFrameSequence<>(
                            engine,
                            configuration,
                            engine.getMessageBus(),
                            new StatefulAtom() {
                            },
                            (_, _, _, _, _, _) -> fastReduceCount.incrementAndGet(),
                            1
                    )
            ) {
                slowSequence.of(slowFactory, TestUtils.createSqlExecutionCtx(engine), PartitionFrameCursorFactory.ORDER_ASC);
                slowSequence.prepareForDispatch();
                Assert.assertEquals(4, slowSequence.getFrameCount());
                final Thread slowOwner = new Thread(() -> {
                    try {
                        slowSequence.dispatchAndAwait();
                    } catch (Throwable th) {
                        slowOwnerError.set(th);
                    } finally {
                        slowFrameStarted.countDown();
                    }
                });
                slowOwner.start();
                try {
                    TestUtils.await(slowFrameStarted);

                    fastSequence.of(fastFactory, sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                    fastSequence.prepareForDispatch();
                    Assert.assertEquals(2, fastSequence.getFrameCount());
                    fastSequence.dispatchAndAwait();

                    Assert.assertEquals(2, fastReduceCount.get());
                    Assert.assertEquals(0, stolenSlowReduceCount.get());
                    Assert.assertEquals(1, slowReduceCount.get());
                } finally {
                    slowFrameRelease.countDown();
                    slowOwner.join();
                }
                if (slowOwnerError.get() != null) {
                    throw new AssertionError(slowOwnerError.get());
                }
                Assert.assertEquals(4, slowReduceCount.get());

                // Each owner published one ticket (one worker) and claimed every frame itself, so both
                // leftover tickets claim nothing.
                final RingQueue<UnorderedPageFrameReduceTask> queue = engine.getMessageBus().getUnorderedPageFrameReduceQueue();
                final MCSequence subSeq = engine.getMessageBus().getUnorderedPageFrameReduceSubSeq();
                int leftoverTicketCount = 0;
                while (!UnorderedPageFrameReduceJob.consumeQueue(queue, subSeq, record, circuitBreaker)) {
                    leftoverTicketCount++;
                }
                Assert.assertEquals(2, leftoverTicketCount);
                Assert.assertEquals(2, fastReduceCount.get());
                Assert.assertEquals(4, slowReduceCount.get());
                Assert.assertEquals(0, stolenSlowReduceCount.get());
            }
        });
    }

    @Test
    public void testUnorderedTailCompletionCanBeReused() throws Exception {
        assertUnorderedTailCompletion(TailCompletionScenario.HEALTHY_REUSE);
    }

    @Test
    public void testUnorderedTailCompletionPreservesCancellation() throws Exception {
        assertUnorderedTailCompletion(TailCompletionScenario.PRIMARY_CANCEL);
        assertUnorderedTailCompletion(TailCompletionScenario.SUPPLEMENTAL_CANCEL);
    }

    @Test
    public void testUnorderedTailCompletionPreservesShutdown() throws Exception {
        assertUnorderedTailCompletion(TailCompletionScenario.DISPATCHER_QUIESCE);
        assertUnorderedTailCompletion(TailCompletionScenario.OWNER_QUIESCE);
        assertUnorderedTailCompletion(TailCompletionScenario.TIMER_SHUTDOWN);
    }

    @Test
    public void testUnorderedTaskCreationFailureCompletesOwnership() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE creation_tab AS (
                        SELECT x, timestamp_sequence(0, 86_400_000_000) ts
                        FROM long_sequence(2)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            final FiberRuntime runtime = new FiberRuntime(1);
            final RingQueue<UnorderedPageFrameReduceTask> queue = engine.getMessageBus().getUnorderedPageFrameReduceQueue();
            final MCSequence subSeq = engine.getMessageBus().getUnorderedPageFrameReduceSubSeq();
            final BlockedOwner failedOwner = new BlockedOwner();
            final BlockedOwner replacementOwner = new BlockedOwner();
            final UnorderedPageFrameSequence<StatefulAtom> failedFrameSequence = new UnorderedPageFrameSequence<>(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    new StatefulAtom() {
                    },
                    failedOwner.wrap((_, _, _, _, _, _) -> {
                    }),
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
                    replacementOwner.wrap((_, _, _, _, _, _) -> {
                    }),
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
            try (
                    RecordCursorFactory failedFactory = select("SELECT * FROM creation_tab");
                    RecordCursorFactory replacementFactory = select("SELECT * FROM creation_tab")
            ) {
                failedFrameSequence.of(failedFactory, sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                failedFrameSequence.prepareForDispatch();
                failedOwner.start(failedFrameSequence);
                final long subSeqStart = subSeq.current();
                circuitBreakerConfiguration = failingCircuitBreakerConfiguration(injected);
                try {
                    runUnordered(dispatcher, queue, subSeq);
                    Assert.fail("expected injected task creation failure");
                } catch (RuntimeException th) {
                    Assert.assertSame(injected, th);
                } finally {
                    circuitBreakerConfiguration = null;
                }
                // The failed creation released the queue slot and completed the claimed frame.
                Assert.assertEquals(subSeqStart + 1, subSeq.current());
                Assert.assertEquals(-1, failedFrameSequence.getDoneLatch().getCount());
                Assert.assertEquals(0, dispatcher.getCreatedTaskCount());
                Assert.assertEquals(0, runtime.getOutstandingTaskCount());
                final Fiber fiber = runtime.tryReserveFiber();
                Assert.assertNotNull(fiber);
                runtime.releaseReservedFiber(fiber, fiber.getReservationEpoch());

                replacementFrameSequence.of(replacementFactory, sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                replacementFrameSequence.prepareForDispatch();
                replacementOwner.start(replacementFrameSequence);
                runUnordered(dispatcher, queue, subSeq);
                Assert.assertEquals(1, dispatcher.getCreatedTaskCount());
                Assert.assertEquals(-1, replacementFrameSequence.getDoneLatch().getCount());
            } finally {
                circuitBreakerConfiguration = null;
                failedOwner.close();
                replacementOwner.close();
                close(runtime);
                Misc.free(dispatcher);
                Misc.free(failedFrameSequence);
                Misc.free(replacementFrameSequence);
            }
        });
    }

    @Test
    public void testUnorderedTaskReleasesCursorBeforeParking() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE park_tab AS (
                        SELECT x, timestamp_sequence(0, 86_400_000_000) ts
                        FROM long_sequence(2)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            final CairoConfiguration queueConfiguration = new CairoConfigurationWrapper(configuration) {
                @Override
                public int getUnorderedPageFrameReduceQueueCapacity() {
                    return 1;
                }
            };
            final FiberRuntime runtime = new FiberRuntime(1);
            final FiberWalWaitQueue waitQueue = new FiberWalWaitQueue();
            final BlockedOwner owner = new BlockedOwner();
            try (
                    MessageBusImpl bus = new MessageBusImpl(queueConfiguration);
                    RecordCursorFactory factory = select("SELECT * FROM park_tab")
            ) {
                final UnorderedPageFrameSequence<StatefulAtom> frameSequence = new UnorderedPageFrameSequence<>(
                        engine,
                        queueConfiguration,
                        bus,
                        new StatefulAtom() {
                        },
                        owner.wrap((_, _, _, _, _, _) -> park(waitQueue)),
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
                    frameSequence.of(factory, sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                    frameSequence.prepareForDispatch();
                    owner.start(frameSequence);
                    // The owner's ticket fills the one-slot queue; the worker takes it and parks in frame 1.
                    Assert.assertFalse(dispatcher.consumeUnordered(
                            0,
                            bus.getUnorderedPageFrameReduceQueue(),
                            bus.getUnorderedPageFrameReduceSubSeq()
                    ));
                    Assert.assertEquals(0, frameSequence.getDoneLatch().getCount());
                    // The parked worker released its slot, so the one-slot queue has room again.
                    Assert.assertTrue(bus.getUnorderedPageFrameReducePubSeq().next() > -1);
                    Assert.assertEquals(1, runtime.getParkedFiberCount());
                    waitQueue.fire(1, false);
                    Assert.assertEquals(1, runtime.drain(1));
                    Assert.assertEquals(-1, frameSequence.getDoneLatch().getCount());
                    Assert.assertEquals(0, runtime.getOutstandingTaskCount());
                } finally {
                    owner.close();
                    close(runtime);
                    Misc.free(dispatcher);
                    Misc.free(frameSequence);
                }
            }
        });
    }

    @Test
    public void testUnorderedWorkerFansTicketsOutUpToWorkerCount() throws Exception {
        // The owner publishes one ticket and blocks in frame 0. Each claim fans out up to two more
        // tickets while fewer than four (the worker count) are out, so a single worker sees 3, then
        // 4 tickets, and never more: frames 1-7 take 7 claims, and the 3 tickets still out when the
        // frames run out claim nothing and retire.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE fan_tab AS (
                        SELECT x, timestamp_sequence(0, 86_400_000_000) ts
                        FROM long_sequence(8)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            final Thread testThread = Thread.currentThread();
            final CountDownLatch ownerFrameStarted = new CountDownLatch(1);
            final CountDownLatch ownerFrameRelease = new CountDownLatch(1);
            final AtomicInteger ownerReduceCount = new AtomicInteger();
            final AtomicInteger workerReduceCount = new AtomicInteger();
            final AtomicReference<Throwable> ownerError = new AtomicReference<>();
            try (
                    RecordCursorFactory factory = select("SELECT * FROM fan_tab");
                    PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
                    SqlExecutionCircuitBreakerWrapper circuitBreaker = new SqlExecutionCircuitBreakerWrapper(
                            engine,
                            configuration.getCircuitBreakerConfiguration()
                    );
                    UnorderedPageFrameSequence<StatefulAtom> frameSequence = new UnorderedPageFrameSequence<>(
                            engine,
                            configuration,
                            engine.getMessageBus(),
                            new StatefulAtom() {
                            },
                            (_, _, _, _, _, _) -> {
                                if (Thread.currentThread() == testThread) {
                                    workerReduceCount.incrementAndGet();
                                } else if (ownerReduceCount.getAndIncrement() == 0) {
                                    ownerFrameStarted.countDown();
                                    TestUtils.await(ownerFrameRelease);
                                }
                            },
                            4
                    )
            ) {
                frameSequence.of(factory, TestUtils.createSqlExecutionCtx(engine), PartitionFrameCursorFactory.ORDER_ASC);
                frameSequence.prepareForDispatch();
                Assert.assertEquals(8, frameSequence.getFrameCount());
                final Thread owner = new Thread(() -> {
                    try {
                        frameSequence.dispatchAndAwait();
                    } catch (Throwable th) {
                        ownerError.set(th);
                    } finally {
                        ownerFrameStarted.countDown();
                    }
                });
                owner.start();
                final RingQueue<UnorderedPageFrameReduceTask> queue = engine.getMessageBus().getUnorderedPageFrameReduceQueue();
                final MCSequence subSeq = engine.getMessageBus().getUnorderedPageFrameReduceSubSeq();
                int consumedTicketCount = 0;
                try {
                    TestUtils.await(ownerFrameStarted);
                    while (!UnorderedPageFrameReduceJob.consumeQueue(queue, subSeq, record, circuitBreaker)) {
                        consumedTicketCount++;
                    }
                } finally {
                    ownerFrameRelease.countDown();
                    owner.join();
                }
                if (ownerError.get() != null) {
                    throw new AssertionError(ownerError.get());
                }
                Assert.assertEquals(7, workerReduceCount.get());
                Assert.assertEquals(1, ownerReduceCount.get());
                Assert.assertEquals(10, consumedTicketCount);
                // The owner found no unclaimed frame after its release, so it published nothing.
                Assert.assertTrue(UnorderedPageFrameReduceJob.consumeQueue(queue, subSeq, record, circuitBreaker));
            }
        });
    }

    @Test
    public void testUnorderedWorkerHandsTicketBackWhileFramesRemain() throws Exception {
        // The owner publishes a single ticket (one worker) and blocks in its first frame. A worker
        // that takes the ticket reduces one frame and puts the ticket back while frames remain, so
        // the one ticket carries the worker through every remaining frame and then dies.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE rotate_tab AS (
                        SELECT x, timestamp_sequence(0, 86_400_000_000) ts
                        FROM long_sequence(4)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            final Thread testThread = Thread.currentThread();
            final CountDownLatch ownerFrameStarted = new CountDownLatch(1);
            final CountDownLatch ownerFrameRelease = new CountDownLatch(1);
            final AtomicInteger ownerReduceCount = new AtomicInteger();
            final AtomicInteger workerReduceCount = new AtomicInteger();
            final AtomicReference<Throwable> ownerError = new AtomicReference<>();
            try (
                    RecordCursorFactory factory = select("SELECT * FROM rotate_tab");
                    PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
                    SqlExecutionCircuitBreakerWrapper circuitBreaker = new SqlExecutionCircuitBreakerWrapper(
                            engine,
                            configuration.getCircuitBreakerConfiguration()
                    );
                    UnorderedPageFrameSequence<StatefulAtom> frameSequence = new UnorderedPageFrameSequence<>(
                            engine,
                            configuration,
                            engine.getMessageBus(),
                            new StatefulAtom() {
                            },
                            (_, _, _, _, _, _) -> {
                                if (Thread.currentThread() == testThread) {
                                    workerReduceCount.incrementAndGet();
                                } else if (ownerReduceCount.getAndIncrement() == 0) {
                                    ownerFrameStarted.countDown();
                                    TestUtils.await(ownerFrameRelease);
                                }
                            },
                            1
                    )
            ) {
                frameSequence.of(factory, TestUtils.createSqlExecutionCtx(engine), PartitionFrameCursorFactory.ORDER_ASC);
                frameSequence.prepareForDispatch();
                Assert.assertEquals(4, frameSequence.getFrameCount());
                final Thread owner = new Thread(() -> {
                    try {
                        frameSequence.dispatchAndAwait();
                    } catch (Throwable th) {
                        ownerError.set(th);
                    } finally {
                        ownerFrameStarted.countDown();
                    }
                });
                owner.start();
                final RingQueue<UnorderedPageFrameReduceTask> queue = engine.getMessageBus().getUnorderedPageFrameReduceQueue();
                final MCSequence subSeq = engine.getMessageBus().getUnorderedPageFrameReduceSubSeq();
                int consumedTicketCount = 0;
                try {
                    TestUtils.await(ownerFrameStarted);
                    while (!UnorderedPageFrameReduceJob.consumeQueue(queue, subSeq, record, circuitBreaker)) {
                        consumedTicketCount++;
                    }
                } finally {
                    ownerFrameRelease.countDown();
                    owner.join();
                }
                if (ownerError.get() != null) {
                    throw new AssertionError(ownerError.get());
                }
                Assert.assertEquals(3, consumedTicketCount);
                Assert.assertEquals(3, workerReduceCount.get());
                Assert.assertEquals(1, ownerReduceCount.get());
            }
        });
    }

    @Test
    public void testUnorderedWorkerKeepsTicketWhenQueueIsFull() throws Exception {
        // The owner publishes its single ticket into a one-slot queue and blocks in its first frame.
        // While the worker reduces the next frame, a foreign ticket fills the queue, so the worker
        // cannot hand its ticket back. It must keep the ticket and reduce every remaining frame in
        // the same call instead of dropping the query's only helper.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE keep_tab AS (
                        SELECT x, timestamp_sequence(0, 86_400_000_000) ts
                        FROM long_sequence(4)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            final CairoConfiguration queueConfiguration = new CairoConfigurationWrapper(configuration) {
                @Override
                public int getUnorderedPageFrameReduceQueueCapacity() {
                    return 1;
                }
            };
            final Thread testThread = Thread.currentThread();
            final CountDownLatch ownerFrameStarted = new CountDownLatch(1);
            final CountDownLatch ownerFrameRelease = new CountDownLatch(1);
            final AtomicInteger ownerReduceCount = new AtomicInteger();
            final AtomicInteger workerReduceCount = new AtomicInteger();
            final AtomicReference<Throwable> ownerError = new AtomicReference<>();
            try (
                    MessageBusImpl bus = new MessageBusImpl(queueConfiguration);
                    RecordCursorFactory factory = select("SELECT * FROM keep_tab");
                    PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
                    SqlExecutionCircuitBreakerWrapper circuitBreaker = new SqlExecutionCircuitBreakerWrapper(
                            engine,
                            configuration.getCircuitBreakerConfiguration()
                    );
                    UnorderedPageFrameSequence<StatefulAtom> foreignSequence = new UnorderedPageFrameSequence<>(
                            engine,
                            queueConfiguration,
                            bus,
                            new StatefulAtom() {
                            },
                            (_, _, _, _, _, _) -> {
                            },
                            1
                    );
                    UnorderedPageFrameSequence<StatefulAtom> frameSequence = new UnorderedPageFrameSequence<>(
                            engine,
                            queueConfiguration,
                            bus,
                            new StatefulAtom() {
                            },
                            (_, _, _, _, _, _) -> {
                                if (Thread.currentThread() == testThread) {
                                    if (workerReduceCount.getAndIncrement() == 0) {
                                        final long cursor = bus.getUnorderedPageFrameReducePubSeq().next();
                                        Assert.assertTrue(cursor > -1);
                                        bus.getUnorderedPageFrameReduceQueue().get(cursor).of(foreignSequence);
                                        bus.getUnorderedPageFrameReducePubSeq().done(cursor);
                                    }
                                } else if (ownerReduceCount.getAndIncrement() == 0) {
                                    ownerFrameStarted.countDown();
                                    TestUtils.await(ownerFrameRelease);
                                }
                            },
                            1
                    )
            ) {
                frameSequence.of(factory, TestUtils.createSqlExecutionCtx(engine), PartitionFrameCursorFactory.ORDER_ASC);
                frameSequence.prepareForDispatch();
                Assert.assertEquals(4, frameSequence.getFrameCount());
                final Thread owner = new Thread(() -> {
                    try {
                        frameSequence.dispatchAndAwait();
                    } catch (Throwable th) {
                        ownerError.set(th);
                    } finally {
                        ownerFrameStarted.countDown();
                    }
                });
                owner.start();
                final RingQueue<UnorderedPageFrameReduceTask> queue = bus.getUnorderedPageFrameReduceQueue();
                final MCSequence subSeq = bus.getUnorderedPageFrameReduceSubSeq();
                try {
                    TestUtils.await(ownerFrameStarted);
                    // One call takes the owner's ticket and, unable to hand it back, reduces frames 1-3.
                    Assert.assertFalse(UnorderedPageFrameReduceJob.consumeQueue(queue, subSeq, record, circuitBreaker));
                    Assert.assertEquals(3, workerReduceCount.get());
                    // Only the foreign ticket is left, and it claims nothing.
                    Assert.assertFalse(UnorderedPageFrameReduceJob.consumeQueue(queue, subSeq, record, circuitBreaker));
                    Assert.assertTrue(UnorderedPageFrameReduceJob.consumeQueue(queue, subSeq, record, circuitBreaker));
                } finally {
                    ownerFrameRelease.countDown();
                    owner.join();
                }
                if (ownerError.get() != null) {
                    throw new AssertionError(ownerError.get());
                }
                Assert.assertEquals(3, workerReduceCount.get());
                Assert.assertEquals(1, ownerReduceCount.get());
            }
        });
    }

    private static void assertBrokenConnection(CairoException exception) {
        TestUtils.assertEquals("remote disconnected, query aborted", exception.getFlyweightMessage());
        Assert.assertFalse(exception.isCancellation());
        Assert.assertTrue(exception.isInterruption());
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
            RingQueue<UnorderedPageFrameReduceTask> queue,
            MCSequence subSeq
    ) {
        if (dispatcher.consumeUnordered(0, queue, subSeq)) {
            throw new IllegalStateException("test dispatcher did not consume the task");
        }
    }

    private void assertBatchFrameLimit(boolean isPreempted) throws Exception {
        assertMemoryLeak(() -> {
            final RecordingFiberDispatchController controller = new RecordingFiberDispatchController();
            final FiberRuntime runtime = controller.createRuntime(2);
            final RingQueue<PageFrameReduceTask> queue = new RingQueue<>(
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD), 128
            );
            final MPSequence pubSeq = new MPSequence(queue.getCycle());
            final MCSequence subSeq = new MCSequence(queue.getCycle());
            pubSeq.then(subSeq).then(pubSeq);
            final AtomicInteger reduced = new AtomicInteger();
            final PageFrameSequence<StatefulAtom> frameSequence = new PageFrameSequence<>(
                    engine, configuration, engine.getMessageBus(), new StatefulAtom() {
            },
                    (_, _, _, _, _) -> Assert.assertEquals(reduced.getAndIncrement() / 10, controller.getCooperativePollCount()),
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD), 1, PageFrameReduceTask.TYPE_FILTER
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
            final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(engine, engine.getMessageBus(), runtime);
            dispatcher.setBatchCheckRowsForTesting(10);
            dispatcher.setBatchRowBudgetForTesting(Long.MAX_VALUE);
            controller.setCooperativePollAction(() -> {
                Assert.assertEquals(controller.getCooperativePollCount() * 10, reduced.get());
                if (isPreempted && controller.getCooperativePollCount() == 1) {
                    Assert.assertTrue(Fiber.yieldForPreemption());
                }
            });
            try {
                for (int index = 0; index < 65; index++) {
                    final long cursor = pubSeq.next();
                    Assert.assertTrue(cursor >= 0);
                    queue.get(cursor).of(frameSequence, index, false);
                    pubSeq.done(cursor);
                }
                final OneShotTask competitor = new OneShotTask();
                Assert.assertFalse(dispatcher.consumeOrdered(-1, queue, subSeq, null));
                Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(competitor));
                Assert.assertEquals(1, runtime.drain(1));
                Assert.assertEquals(isPreempted ? 10 : 64, reduced.get());
                Assert.assertFalse(competitor.isDone());
                Assert.assertEquals(1, runtime.drain(1));
                Assert.assertTrue(competitor.isDone());
                if (isPreempted) {
                    Assert.assertEquals(1, runtime.drain(1));
                }
                Assert.assertEquals(64, reduced.get());
                Assert.assertEquals(63, subSeq.current());
                Assert.assertEquals(0, runtime.getOutstandingTaskCount());
                Assert.assertFalse(dispatcher.consumeOrdered(-1, queue, subSeq, null));
                Assert.assertEquals(1, runtime.drain(1));
                Assert.assertEquals(65, reduced.get());
                Assert.assertEquals(65, frameSequence.getReduceFinishedCounter().get());
                Assert.assertEquals(6, controller.getCooperativePollCount());
                Assert.assertTrue(frameSequence.isActive());
            } finally {
                close(runtime);
                Misc.free(dispatcher);
                Misc.free(frameSequence);
                Misc.free(queue);
            }
        });
    }

    private void assertProgressBeforeTimer(boolean isSequenceWait, ProgressBeforeTimerScenario scenario) throws Exception {
        assertMemoryLeak(() -> {
            final TimerShards progressTimerShards = new TimerShards(1, "test-progress-before-timer", LOG);
            final CairoConfiguration timerConfiguration = new CairoConfigurationWrapper(configuration) {
                @Override
                public long getQueryContinuationWakeIntervalMillis() {
                    return TimeUnit.HOURS.toMillis(1);
                }
            };
            try {
                progressTimerShards.start();
                try (CairoEngine testEngine = new CairoEngine(timerConfiguration) {
                    @Override
                    public TimerShards getTimerShards() {
                        return progressTimerShards;
                    }
                }) {
                    // Keep the dispatcher's runtime open when the HTTP-like owner's admission closes.
                    final FiberRuntime dispatcherRuntime = new FiberRuntime(1);
                    final FiberRuntime ownerRuntime = new FiberRuntime(1);
                    try (
                            PageFrameSequence<StatefulAtom> frameSequence = new PageFrameSequence<>(
                                    testEngine,
                                    timerConfiguration,
                                    testEngine.getMessageBus(),
                                    new StatefulAtom() {
                                    },
                                    (_, _, _, _, _) -> {
                                    },
                                    () -> new PageFrameReduceTask(timerConfiguration, MemoryTag.NATIVE_OFFLOAD),
                                    1,
                                    PageFrameReduceTask.TYPE_FILTER
                            );
                            PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                                    testEngine,
                                    testEngine.getMessageBus(),
                                    dispatcherRuntime
                            )
                    ) {
                        final AtomicInteger primaryRegistrations = new AtomicInteger();
                        final AtomicInteger supplementalRegistrations = new AtomicInteger();
                        final FiberCancellationSignal cancellationSignal = new FiberCancellationSignal(() -> {
                            primaryRegistrations.incrementAndGet();
                            if (scenario == ProgressBeforeTimerScenario.TIMER_SHUTDOWN_DURING_REGISTRATION) {
                                // Cancellation registration runs before timer registration in both overloads.
                                progressTimerShards.shutdown();
                            }
                        });
                        final FiberCancellationSignal supplementalSignal = new FiberCancellationSignal(
                                supplementalRegistrations::incrementAndGet
                        );
                        final AtomicReference<Throwable> failure = new AtomicReference<>();
                        final FiberTask ownerTask = new FiberTask() {
                            @Override
                            protected void onError(Throwable th) {
                                failure.set(th);
                            }

                            @Override
                            protected boolean runStep() {
                                final long sequenceVersion = frameSequence.getProgressVersion();
                                final long globalVersion = dispatcher.getProgressVersion();
                                dispatcher.signalProgressForTesting(frameSequence);
                                switch (scenario) {
                                    case PRIMARY_CANCEL -> cancellationSignal.cancel();
                                    case RUNTIME_QUIESCE -> ownerRuntime.beginQuiesce();
                                    case SUPPLEMENTAL_CANCEL -> supplementalSignal.cancel();
                                    case TIMER_SHUTDOWN -> progressTimerShards.shutdown();
                                    default -> {
                                    }
                                }
                                final int reason = isSequenceWait
                                        ? dispatcher.awaitProgress(
                                        frameSequence,
                                        sequenceVersion,
                                        globalVersion,
                                        cancellationSignal,
                                        supplementalSignal
                                )
                                        : dispatcher.awaitProgress(globalVersion, cancellationSignal);
                                final int expectedReason = switch (scenario) {
                                    case PRIMARY_CANCEL, SUPPLEMENTAL_CANCEL -> FiberWaitCoordinator.REASON_CANCEL;
                                    case RUNTIME_QUIESCE, TIMER_SHUTDOWN, TIMER_SHUTDOWN_DURING_REGISTRATION ->
                                            FiberWaitCoordinator.REASON_SHUTDOWN;
                                };
                                Assert.assertEquals(expectedReason, reason);
                                final FiberWaitCoordinator coordinator = Objects.requireNonNull(Fiber.current()).getWaitCoordinator();
                                Assert.assertEquals(0, coordinator.currentToken());
                                Assert.assertFalse(coordinator.hasInFlightRegistrations());
                                return true;
                            }
                        };
                        try {
                            Assert.assertSame(LaunchResult.LAUNCHED, ownerRuntime.launch(ownerTask));
                            Assert.assertEquals(1, ownerRuntime.drain(1));
                            Assert.assertTrue(ownerTask.isDone());
                            Assert.assertNull(failure.get());
                            Assert.assertEquals(0, ownerRuntime.getParkedFiberCount());
                            Assert.assertEquals(0, ownerRuntime.getOutstandingTaskCount());
                            Assert.assertEquals(0, progressTimerShards.size());
                            final int expectedRegistrations = scenario == ProgressBeforeTimerScenario.RUNTIME_QUIESCE ? 0 : 1;
                            Assert.assertEquals(expectedRegistrations, primaryRegistrations.get());
                            Assert.assertEquals(isSequenceWait ? expectedRegistrations : 0, supplementalRegistrations.get());
                            Assert.assertEquals(FiberRuntimeState.OPEN, dispatcherRuntime.state());
                            // Reset rejects leaked cancellation registrations even when no park occurred.
                            cancellationSignal.reset();
                            supplementalSignal.reset();
                        } finally {
                            try {
                                close(ownerRuntime);
                            } finally {
                                close(dispatcherRuntime);
                            }
                        }
                    }
                }
            } finally {
                progressTimerShards.shutdown();
            }
        });
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

    private void assertUnorderedTailCompletion(TailCompletionScenario scenario) throws Exception {
        assertMemoryLeak(() -> {
            final boolean hasManagedOwner = switch (scenario) {
                case MANAGED_DISPATCHER_QUIESCE, MANAGED_OWNER_QUIESCE, MANAGED_POLL,
                     MANAGED_POLL_FAILURE, MANAGED_PRIMARY_CANCEL, MANAGED_SUPPLEMENTAL_CANCEL,
                     MANAGED_TIMER_SHUTDOWN -> true;
                default -> false;
            };
            final boolean isHealthyCompletion = scenario == TailCompletionScenario.HEALTHY_REUSE
                    || scenario == TailCompletionScenario.MANAGED_POLL;
            final boolean isPollFailure = scenario == TailCompletionScenario.MANAGED_POLL_FAILURE;
            final RuntimeException pollFailure = new RuntimeException("injected managed tail poll failure");
            final TimerShards tailTimerShards = new TimerShards(1, "test-unordered-tail", LOG);
            final CairoConfiguration timerConfiguration = new CairoConfigurationWrapper(configuration) {
                @Override
                public long getQueryContinuationWakeIntervalMillis() {
                    return TimeUnit.HOURS.toMillis(1);
                }

                @Override
                public long getSqlParallelWorkStealingSpinTimeout() {
                    return 16_000;
                }
            };
            try {
                tailTimerShards.start();
                try (
                        CairoEngine testEngine = new CairoEngine(timerConfiguration) {
                            @Override
                            public TimerShards getTimerShards() {
                                return tailTimerShards;
                            }
                        };
                        SqlExecutionContextImpl executionContext = new SqlExecutionContextImpl(testEngine, 1)
                                .with(sqlExecutionContext.getSecurityContext())
                ) {
                    final String tableName = "unordered_tail_" + scenario.name();
                    testEngine.execute("CREATE TABLE " + tableName + " AS (SELECT x FROM long_sequence(1))", executionContext);
                    final FiberRuntime dispatcherRuntime = new FiberRuntime(1);
                    final RecordingFiberDispatchController controller = hasManagedOwner
                            ? new RecordingFiberDispatchController()
                            : null;
                    final FiberRuntime ownerRuntime = controller != null
                            ? controller.createRuntime(1)
                            : new FiberRuntime(1);
                    final FiberDispatchContext parallelContext = new FiberDispatchContext() {
                        @Override
                        public long getQueryRegistryOwnerId() {
                            return 2;
                        }
                    };
                    final FiberDispatchContext queryContext = new FiberDispatchContext() {
                        @Override
                        public FiberDispatchContext getParallelDispatchContext() {
                            return parallelContext;
                        }

                        @Override
                        public long getQueryRegistryOwnerId() {
                            return 1;
                        }
                    };
                    final FiberCancellationSignal primaryCancellation = new FiberCancellationSignal();
                    final FiberCancellationSignal supplementalCancellation = new FiberCancellationSignal();
                    final AtomicInteger ownerStepCount = new AtomicInteger();
                    final AtomicReference<Thread> helperThread = new AtomicReference<>();
                    final AtomicReference<CountDownLatch> helperClaimed = new AtomicReference<>();
                    final AtomicReference<CountDownLatch> helperRelease = new AtomicReference<>();
                    final AtomicInteger ownerCompletionCount = new AtomicInteger();
                    final AtomicReference<Throwable> ownerFailure = new AtomicReference<>();
                    try (
                            SqlCompiler compiler = testEngine.getSqlCompiler();
                            RecordCursorFactory factory = compiler.compile("SELECT * FROM " + tableName, executionContext)
                                    .getRecordCursorFactory();
                            PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                                    testEngine,
                                    testEngine.getMessageBus(),
                                    dispatcherRuntime
                            )
                    ) {
                        final WorkStealingStrategy strategy = new WorkStealingStrategy() {
                            @Override
                            public WorkStealingStrategy of(AtomicInteger startedCounter) {
                                return this;
                            }

                            @Override
                            public void onBeforeOwnerStep() {
                                final int step = ownerStepCount.incrementAndGet();
                                if (step == 1) {
                                    // Before the owner claims its own frame, a helper on the worker path claims
                                    // the only frame and holds it.
                                    final Thread helper = new Thread(() -> {
                                        try (
                                                PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
                                                SqlExecutionCircuitBreakerWrapper circuitBreaker = new SqlExecutionCircuitBreakerWrapper(
                                                        testEngine,
                                                        timerConfiguration.getCircuitBreakerConfiguration()
                                                )
                                        ) {
                                            UnorderedPageFrameReduceJob.consumeQueue(
                                                    testEngine.getMessageBus().getUnorderedPageFrameReduceQueue(),
                                                    testEngine.getMessageBus().getUnorderedPageFrameReduceSubSeq(),
                                                    record,
                                                    circuitBreaker
                                            );
                                        }
                                    });
                                    helperThread.set(helper);
                                    helper.start();
                                    TestUtils.await(helperClaimed.get());
                                } else if (step == 2) {
                                    // The owner passed its completion check and is about to register its wait.
                                    // Model the helper finishing before the registration.
                                    try {
                                        switch (scenario) {
                                            case DISPATCHER_QUIESCE -> dispatcher.beginQuiesce();
                                            case HEALTHY_REUSE -> {
                                            }
                                            case MANAGED_DISPATCHER_QUIESCE -> dispatcher.beginQuiesce();
                                            case MANAGED_OWNER_QUIESCE -> ownerRuntime.beginQuiesce();
                                            case MANAGED_POLL, MANAGED_POLL_FAILURE -> {
                                            }
                                            case MANAGED_PRIMARY_CANCEL -> primaryCancellation.cancel();
                                            case MANAGED_SUPPLEMENTAL_CANCEL -> supplementalCancellation.cancel();
                                            case MANAGED_TIMER_SHUTDOWN -> tailTimerShards.shutdown();
                                            case OWNER_QUIESCE -> ownerRuntime.beginQuiesce();
                                            case PRIMARY_CANCEL -> primaryCancellation.cancel();
                                            case SUPPLEMENTAL_CANCEL -> supplementalCancellation.cancel();
                                            case TIMER_SHUTDOWN -> tailTimerShards.shutdown();
                                        }
                                    } finally {
                                        // The helper's worker path counts its frame down without publishing
                                        // progress: cancellation must win at registration.
                                        helperRelease.get().countDown();
                                        try {
                                            helperThread.get().join();
                                        } catch (InterruptedException e) {
                                            Thread.currentThread().interrupt();
                                        }
                                    }
                                }
                            }

                            @Override
                            public boolean shouldSteal(int finishedCount) {
                                return true;
                            }
                        };
                        final FactoryProvider strategyProvider = new DefaultFactoryProvider() {
                            @Override
                            public @NotNull WorkStealingStrategy getWorkStealingStrategy(
                                    @NotNull CairoConfiguration configuration,
                                    int workerCount,
                                    @NotNull StatefulAtom atom
                            ) {
                                return strategy;
                            }
                        };
                        final CairoConfiguration sequenceConfiguration = new CairoConfigurationWrapper(timerConfiguration) {
                            @Override
                            public @NotNull FactoryProvider getFactoryProvider() {
                                return strategyProvider;
                            }
                        };
                        testEngine.getMessageBus().setPageFrameReduceDispatcher(dispatcher);
                        try (UnorderedPageFrameSequence<StatefulAtom> frameSequence = new UnorderedPageFrameSequence<>(
                                testEngine,
                                sequenceConfiguration,
                                testEngine.getMessageBus(),
                                new StatefulAtom() {
                                },
                                (_, _, _, _, _, _) -> {
                                    // Only the helper reduces: it claims the only frame before the owner does.
                                    Assert.assertSame(helperThread.get(), Thread.currentThread());
                                    helperClaimed.get().countDown();
                                    TestUtils.await(helperRelease.get());
                                },
                                1
                        ) {
                            @Override
                            public FiberDispatchContext getDispatchContext() {
                                return hasManagedOwner ? parallelContext : super.getDispatchContext();
                            }
                        }) {
                            if (controller != null) {
                                controller.setCooperativePollAction(() -> {
                                    final Fiber currentFiber = Objects.requireNonNull(Fiber.current());
                                    Assert.assertSame(queryContext, Fiber.getDispatchContext());
                                    Assert.assertEquals(0, currentFiber.getWaitCoordinator().currentToken());
                                    Assert.assertFalse(currentFiber.getWaitCoordinator().hasInFlightRegistrations());
                                    if (isPollFailure) {
                                        throw pollFailure;
                                    }
                                    if (scenario == TailCompletionScenario.MANAGED_POLL) {
                                        Assert.assertTrue(Fiber.yieldForDispatch());
                                    } else {
                                        Assert.fail("managed tail cancellation or shutdown reached the ticket poll");
                                    }
                                });
                            }
                            try {
                                final int runCount = scenario == TailCompletionScenario.HEALTHY_REUSE ? 2 : 1;
                                for (int i = 0; i < runCount; i++) {
                                    if (i > 0) {
                                        frameSequence.reset();
                                    }
                                    ownerStepCount.set(0);
                                    helperClaimed.set(new CountDownLatch(1));
                                    helperRelease.set(new CountDownLatch(1));
                                    frameSequence.of(factory, executionContext, PartitionFrameCursorFactory.ORDER_ASC);
                                    frameSequence.prepareForDispatch();
                                    Assert.assertEquals(1, frameSequence.getFrameCount());
                                    if (hasManagedOwner) {
                                        Assert.assertSame(parallelContext, frameSequence.getDispatchContext());
                                    } else {
                                        Assert.assertNull(frameSequence.getDispatchContext());
                                    }
                                    final FiberTask ownerTask = new FiberTask() {
                                        @Override
                                        public FiberCancellationSignal getCancellationSignal() {
                                            return primaryCancellation;
                                        }

                                        @Override
                                        protected void onError(Throwable th) {
                                            ownerFailure.set(th);
                                        }

                                        @Override
                                        protected boolean runStep() {
                                            final FiberCancellationSignal previousSignal = SuspensionScope.getSupplementalCancellationSignal();
                                            final long previousGeneration = SuspensionScope.getSupplementalCancellationSignalGeneration();
                                            SuspensionScope.enterSupplementalCancellationSignal(
                                                    supplementalCancellation,
                                                    supplementalCancellation.getGeneration()
                                            );
                                            try {
                                                // A healthy SQL breaker must not hide cancellation in either scope slot.
                                                frameSequence.dispatchAndAwait();
                                                if (!isHealthyCompletion) {
                                                    throw new AssertionError("tail completion bypassed " + scenario);
                                                }
                                                final FiberWaitCoordinator coordinator = Objects.requireNonNull(Fiber.current()).getWaitCoordinator();
                                                Assert.assertEquals(0, coordinator.currentToken());
                                                Assert.assertFalse(coordinator.hasInFlightRegistrations());
                                                ownerCompletionCount.incrementAndGet();
                                                return true;
                                            } finally {
                                                SuspensionScope.enterSupplementalCancellationSignal(previousSignal, previousGeneration);
                                            }
                                        }
                                    };
                                    Assert.assertSame(
                                            LaunchResult.LAUNCHED,
                                            ownerRuntime.launch(ownerTask, hasManagedOwner ? queryContext : null)
                                    );
                                    Assert.assertEquals(1, ownerRuntime.drain(1));
                                    if (scenario == TailCompletionScenario.MANAGED_POLL) {
                                        Assert.assertFalse("the dispatch yield must end the first mount", ownerTask.isDone());
                                        Assert.assertEquals(1, ownerRuntime.drain(1));
                                    }
                                    Assert.assertTrue(ownerTask.isDone());
                                    if (isHealthyCompletion) {
                                        Assert.assertNull(ownerFailure.get());
                                        Assert.assertTrue(frameSequence.isActive());
                                        Assert.assertEquals(i + 1, ownerCompletionCount.get());
                                        Assert.assertEquals(1, ownerRuntime.getCreatedFiberCount());
                                        Assert.assertEquals(1, ownerRuntime.getLiveFiberCount());
                                        Assert.assertEquals(1, ownerRuntime.getRetainedFiberCount());
                                        Assert.assertEquals(0, ownerRuntime.getMountedCount());
                                        Assert.assertEquals(0, ownerRuntime.getQueuedCount());
                                        Assert.assertEquals(0, dispatcherRuntime.getLiveFiberCount());
                                        Assert.assertTrue(tailTimerShards.isRunning());
                                    } else if (isPollFailure) {
                                        Assert.assertSame(pollFailure, ownerFailure.get());
                                        Assert.assertTrue(frameSequence.isActive());
                                    } else {
                                        Assert.assertTrue("expected query cancellation for " + scenario, ownerFailure.get() instanceof CairoException);
                                        final CairoException exception = (CairoException) ownerFailure.get();
                                        Assert.assertTrue(exception.isCancellation());
                                        Assert.assertEquals(SqlExecutionCircuitBreaker.STATE_CANCELLED, exception.getInterruptionReason());
                                        Assert.assertEquals(SqlExecutionCircuitBreaker.STATE_CANCELLED, frameSequence.getCancelReason());
                                    }
                                    Assert.assertEquals(2, ownerStepCount.get());
                                    Assert.assertEquals(-1, frameSequence.getDoneLatch().getCount());
                                    Assert.assertEquals(0, ownerRuntime.getOutstandingTaskCount());
                                    Assert.assertEquals(0, ownerRuntime.getParkedFiberCount());
                                    Assert.assertEquals(0, dispatcher.getCreatedTaskCount());
                                    Assert.assertEquals(0, tailTimerShards.size());
                                    if (controller != null) {
                                        final int expectedPollCount = scenario == TailCompletionScenario.MANAGED_POLL
                                                || isPollFailure ? 1 : 0;
                                        Assert.assertEquals(expectedPollCount, controller.getCooperativePollCount());
                                        if (expectedPollCount > 0) {
                                            Assert.assertSame(queryContext, controller.getPolledContext(0));
                                            Assert.assertEquals(1, controller.getPolledOwnerId(0));
                                        }
                                        final int expectedMountCount = scenario == TailCompletionScenario.MANAGED_POLL ? 2 : 1;
                                        Assert.assertEquals(expectedMountCount, controller.getMountCount());
                                        Assert.assertEquals(expectedMountCount, controller.getUnmountCount());
                                        for (int mount = 0; mount < expectedMountCount; mount++) {
                                            Assert.assertSame(queryContext, controller.getMountedContext(mount));
                                            Assert.assertEquals(1, controller.getMountedOwnerId(mount));
                                        }
                                    }
                                    primaryCancellation.reset();
                                    supplementalCancellation.reset();
                                }
                            } finally {
                                // Drain abandoned publications before closing a possibly parked owner.
                                try {
                                    close(dispatcherRuntime);
                                } finally {
                                    close(ownerRuntime);
                                }
                            }
                        }
                    }
                }
            } finally {
                tailTimerShards.shutdown();
            }
        });
    }

    private enum ProgressBeforeTimerScenario {
        PRIMARY_CANCEL,
        RUNTIME_QUIESCE,
        SUPPLEMENTAL_CANCEL,
        TIMER_SHUTDOWN,
        TIMER_SHUTDOWN_DURING_REGISTRATION
    }

    private enum TailCompletionScenario {
        DISPATCHER_QUIESCE,
        HEALTHY_REUSE,
        MANAGED_DISPATCHER_QUIESCE,
        MANAGED_OWNER_QUIESCE,
        MANAGED_POLL,
        MANAGED_POLL_FAILURE,
        MANAGED_PRIMARY_CANCEL,
        MANAGED_SUPPLEMENTAL_CANCEL,
        MANAGED_TIMER_SHUTDOWN,
        OWNER_QUIESCE,
        PRIMARY_CANCEL,
        SUPPLEMENTAL_CANCEL,
        TIMER_SHUTDOWN
    }

    /**
     * Runs dispatchAndAwait() on its own thread and holds the owner inside its first frame, which
     * leaves the other frames, and the owner's ticket in the sequence's queue, to the test's workers.
     */
    private static final class BlockedOwner implements QuietCloseable {
        private final CountDownLatch blocked = new CountDownLatch(1);
        private final AtomicReference<Throwable> error = new AtomicReference<>();
        private final AtomicBoolean isFirstFrame = new AtomicBoolean(true);
        private final CountDownLatch release = new CountDownLatch(1);
        private volatile Thread thread;

        @Override
        public void close() {
            release.countDown();
            final Thread ownerThread = thread;
            if (ownerThread != null) {
                try {
                    ownerThread.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        // The owner's dispatchAndAwait() failure, once close() has returned.
        Throwable getError() {
            return error.get();
        }

        void start(UnorderedPageFrameSequence<?> frameSequence) {
            thread = new Thread(() -> {
                try {
                    frameSequence.dispatchAndAwait();
                } catch (Throwable th) {
                    error.set(th);
                } finally {
                    blocked.countDown();
                }
            });
            thread.start();
            TestUtils.await(blocked);
        }

        // The owner blocks in its first frame and skips the others; workers run the given reducer.
        UnorderedPageFrameReducer wrap(UnorderedPageFrameReducer workerReducer) {
            return (workerId, record, frameIndex, circuitBreaker, frameSequence, stealingFrameSequence) -> {
                if (Thread.currentThread() != thread) {
                    workerReducer.reduce(workerId, record, frameIndex, circuitBreaker, frameSequence, stealingFrameSequence);
                } else if (isFirstFrame.compareAndSet(true, false)) {
                    blocked.countDown();
                    TestUtils.await(release);
                }
            };
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

    private static class OneShotTask extends FiberTask {
        @Override
        protected boolean runStep() {
            return true;
        }
    }
}
