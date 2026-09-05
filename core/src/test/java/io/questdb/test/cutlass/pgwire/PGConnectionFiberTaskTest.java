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

package io.questdb.test.cutlass.pgwire;

import io.questdb.Metrics;
import io.questdb.cairo.CairoConfigurationWrapper;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cutlass.pgwire.PGConfiguration;
import io.questdb.cutlass.pgwire.PGConnectionContext;
import io.questdb.cutlass.pgwire.PGConnectionFiberTask;
import io.questdb.cutlass.pgwire.PGMessageProcessingException;
import io.questdb.cutlass.pgwire.PGPipelineEntry;
import io.questdb.cutlass.pgwire.PGServer;
import io.questdb.cutlass.pgwire.TypesAndSelect;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.mp.Job;
import io.questdb.mp.continuation.Fiber;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.FiberRuntimeState;
import io.questdb.mp.continuation.FiberTask;
import io.questdb.mp.continuation.FiberWaitCoordinator;
import io.questdb.mp.continuation.LaunchResult;
import io.questdb.mp.continuation.SuspensionScope;
import io.questdb.mp.continuation.TimerShards;
import io.questdb.network.IODispatcher;
import io.questdb.network.IOOperation;
import io.questdb.network.IORequestProcessor;
import io.questdb.network.PeerIsSlowToWriteException;
import io.questdb.std.Misc;
import io.questdb.std.NoOpAssociativeCache;
import io.questdb.test.AbstractCairoTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

public class PGConnectionFiberTaskTest extends AbstractCairoTest {

    @Test
    public void testConcurrentTerminalLaunchesShareReopenedIncarnation() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(2);
            try (final TestContext context = newTestContext()) {
                final TestDispatcher dispatcher = new TestDispatcher();
                final PGConnectionFiberTask task = context.getFiberTask(dispatcher, Metrics.DISABLED);
                final CountDownLatch start = new CountDownLatch(1);
                final AtomicReference<Throwable> error = new AtomicReference<>();
                final AtomicReference<LaunchResult> firstResult = new AtomicReference<>();
                final AtomicReference<LaunchResult> secondResult = new AtomicReference<>();

                Assert.assertTrue(task.tryCancel());
                final Thread first = new Thread(() -> launch(start, error, firstResult, task, runtime));
                final Thread second = new Thread(() -> launch(start, error, secondResult, task, runtime));
                first.start();
                second.start();
                start.countDown();
                first.join();
                second.join();

                Assert.assertNull(error.get());
                Assert.assertTrue(
                        firstResult.get() == LaunchResult.LAUNCHED && secondResult.get() == LaunchResult.ALREADY_OWNED
                                || firstResult.get() == LaunchResult.ALREADY_OWNED && secondResult.get() == LaunchResult.LAUNCHED
                );
                Assert.assertEquals(1, runtime.drain(8));
                Assert.assertEquals(1, context.callCount);

                close(runtime);
            }
        });
    }

    @Test
    public void testEarlyReadyStagesNextOperation() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(2);
            try (final TestContext context = newTestContext()) {
                final TestDispatcher dispatcher = new TestDispatcher();
                final PGConnectionFiberTask task = context.getFiberTask(dispatcher, Metrics.DISABLED);
                dispatcher.task = task;
                dispatcher.runtime = runtime;
                dispatcher.wakeOperation = IOOperation.WRITE;

                Assert.assertEquals(LaunchResult.LAUNCHED, task.launch(runtime, IOOperation.READ));
                Assert.assertEquals(2, runtime.drain(8));
                Assert.assertEquals(2, context.callCount);
                Assert.assertEquals(IOOperation.READ, context.firstOperation);
                Assert.assertEquals(IOOperation.WRITE, context.secondOperation);
                Assert.assertEquals(LaunchResult.ALREADY_OWNED, dispatcher.wakeResult);
                Assert.assertEquals(FiberTask.STATE_IDLE, task.getScheduleState());

                close(runtime);
            }
        });
    }

    @Test
    public void testMessageProcessingExceptionIsPipelineEntryOwned() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    final PGPipelineEntry firstEntry = new PGPipelineEntry(engine);
                    final PGPipelineEntry secondEntry = new PGPipelineEntry(engine)
            ) {
                final PGMessageProcessingException first = PGMessageProcessingException.instance(firstEntry).put("first");
                final PGMessageProcessingException second = PGMessageProcessingException.instance(secondEntry).put("second");

                Assert.assertSame(first, PGMessageProcessingException.instance(firstEntry));
                Assert.assertNotSame(first, second);
                first.put("-tail");

                Assert.assertEquals("first-tail", firstEntry.getErrorMessageSink().toString());
                Assert.assertEquals("second", secondEntry.getErrorMessageSink().toString());
            }
        });
    }

    @Test
    public void testOwnedLaunchDoesNotOverwriteCurrentOperation() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(2);
            try (final TestContext context = newTestContext()) {
                final TestDispatcher dispatcher = new TestDispatcher();
                final PGConnectionFiberTask task = context.getFiberTask(dispatcher, Metrics.DISABLED);

                Assert.assertEquals(LaunchResult.LAUNCHED, task.launch(runtime, IOOperation.READ));
                Assert.assertEquals(LaunchResult.ALREADY_OWNED, task.launch(runtime, IOOperation.WRITE));
                Assert.assertEquals(1, runtime.drain(8));
                Assert.assertEquals(1, context.callCount);
                Assert.assertEquals(IOOperation.READ, context.firstOperation);
                Assert.assertEquals(FiberTask.STATE_IDLE, task.getScheduleState());

                close(runtime);
            }
        });
    }

    @Test
    public void testQuiescingDisconnectsConsumedRearmEvent() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(2);
            try (final TestContext context = newTestContext()) {
                final TestDispatcher dispatcher = new TestDispatcher();
                final PGConnectionFiberTask task = context.getFiberTask(dispatcher, Metrics.DISABLED);
                final Fiber reservedFiber = runtime.tryReserveFiber();
                Assert.assertNotNull(reservedFiber);
                dispatcher.isQuiesceBeforeWake = true;
                dispatcher.reservationEpoch = reservedFiber.getReservationEpoch();
                dispatcher.reservedFiber = reservedFiber;
                dispatcher.task = task;
                dispatcher.runtime = runtime;
                dispatcher.wakeOperation = IOOperation.WRITE;

                Assert.assertEquals(LaunchResult.LAUNCHED, task.launch(runtime, IOOperation.READ));
                Assert.assertTrue(runtime.drain(8) > 0);
                Assert.assertEquals(LaunchResult.ALREADY_OWNED, dispatcher.wakeResult);
                Assert.assertTrue(task.isCancelled());
                Assert.assertEquals(1, dispatcher.registerCount);
                Assert.assertEquals(1, dispatcher.disconnectCount);
                Assert.assertEquals(0, runtime.getOutstandingTaskCount());

                close(runtime);
            }
        });
    }

    @Test
    public void testRequestJobReservesFiberForEachIoEvent() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(2);
            try (
                    final TestContext firstContext = newTestContext();
                    final TestContext secondContext = newTestContext()
            ) {
                try {
                    final TestDispatcher dispatcher = new TestDispatcher();
                    dispatcher.firstRequestContext = firstContext;
                    dispatcher.secondRequestContext = secondContext;

                    Assert.assertTrue(PGServer.runFiberRequestJobForTesting(
                            dispatcher,
                            Metrics.DISABLED,
                            runtime
                    ));
                    Assert.assertEquals(2, runtime.getOutstandingTaskCount());
                    Assert.assertEquals(2, runtime.drain(8));
                    Assert.assertEquals(0, runtime.getOutstandingTaskCount());
                    Assert.assertEquals(1, firstContext.callCount);
                    Assert.assertEquals(1, secondContext.callCount);
                    Assert.assertEquals(
                            FiberTask.STATE_IDLE,
                            firstContext.getFiberTask(dispatcher, Metrics.DISABLED).getScheduleState()
                    );
                    Assert.assertEquals(
                            FiberTask.STATE_IDLE,
                            secondContext.getFiberTask(dispatcher, Metrics.DISABLED).getScheduleState()
                    );
                } finally {
                    close(runtime);
                }
            }
        });
    }

    @Test
    public void testReservedOwnedLaunchReleasesReservation() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(2);
            try (final TestContext context = newTestContext()) {
                final TestDispatcher dispatcher = new TestDispatcher();
                final PGConnectionFiberTask task = context.getFiberTask(dispatcher, Metrics.DISABLED);

                Assert.assertEquals(LaunchResult.LAUNCHED, task.launch(runtime, IOOperation.READ));
                final Fiber fiber = runtime.tryReserveFiber();
                Assert.assertNotNull(fiber);
                Assert.assertEquals(2, runtime.getOutstandingTaskCount());
                Assert.assertEquals(
                        LaunchResult.ALREADY_OWNED,
                        task.launchReserved(runtime, fiber, fiber.getReservationEpoch(), IOOperation.WRITE)
                );
                Assert.assertEquals(1, runtime.getOutstandingTaskCount());

                Assert.assertEquals(1, runtime.drain(8));
                Assert.assertEquals(0, runtime.getOutstandingTaskCount());
                close(runtime);
            }
        });
    }

    @Test
    public void testStaleStagedEventDoesNotBlockCurrentIncarnation() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(2);
            try (final TestContext context = newTestContext()) {
                final TestDispatcher dispatcher = new TestDispatcher();
                final PGConnectionFiberTask task = context.getFiberTask(dispatcher, Metrics.DISABLED);

                Assert.assertEquals(LaunchResult.LAUNCHED, task.launch(runtime, IOOperation.READ));
                Assert.assertEquals(1, runtime.drain(8));
                final long staleIncarnation = task.getIncarnation();
                Assert.assertTrue(task.tryCancel());
                task.reopen();

                task.setReadyEventForTesting(staleIncarnation);

                Assert.assertEquals(LaunchResult.LAUNCHED, task.launch(runtime, IOOperation.WRITE));
                Assert.assertEquals(1, runtime.drain(8));
                Assert.assertEquals(2, context.callCount);
                Assert.assertEquals(IOOperation.WRITE, context.secondOperation);

                close(runtime);
            }
        });
    }

    @Test
    public void testTimerWakesContendedSlotWaiter() throws Exception {
        assertMemoryLeak(() -> {
            final PerWorkerLocks locks = new PerWorkerLocks(new CairoConfigurationWrapper(configuration) {
                @Override
                public long getQueryContinuationWakeIntervalMillis() {
                    return 60_000;
                }
            }, 1);
            final int heldSlot = locks.acquireSlot(0, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
            final AtomicBooleanCircuitBreaker circuitBreaker = new AtomicBooleanCircuitBreaker(engine);
            final FiberRuntime runtime = new FiberRuntime(2, 2);
            final TimerShards timerShards = engine.getTimerShards();
            final int timerCountBefore = timerShards.size();
            try (final SlotWaitingTestContext context = newSlotWaitingTestContext(locks, circuitBreaker)) {
                final TestDispatcher dispatcher = new TestDispatcher();
                final PGConnectionFiberTask task = context.getFiberTask(dispatcher, Metrics.DISABLED);

                Assert.assertEquals(LaunchResult.LAUNCHED, task.launch(runtime, IOOperation.READ));
                Assert.assertEquals(1, runtime.drain(1));

                Assert.assertSame(timerShards, context.observedTimerShards);
                Assert.assertNotNull(context.waitingFiber);
                Assert.assertEquals(1, runtime.getParkedFiberCount());
                Assert.assertEquals(timerCountBefore + 1, timerShards.size());

                circuitBreaker.cancel();
                final FiberWaitCoordinator coordinator = context.waitingFiber.getWaitCoordinator();
                Assert.assertTrue(coordinator.fire(coordinator.currentToken(), FiberWaitCoordinator.REASON_TIMER));
                Assert.assertEquals(1, runtime.drain(1));

                Assert.assertNotNull(context.breakerErrorMessage);
                Assert.assertTrue(context.breakerErrorMessage.contains("cancelled"));
                Assert.assertEquals(timerCountBefore, timerShards.size());
                Assert.assertEquals(1, locks.getAcquiredSlotCount());
            } finally {
                locks.releaseSlot(heldSlot);
                close(runtime);
            }
            Assert.assertEquals(0, locks.getAcquiredSlotCount());
        });
    }

    private static void close(FiberRuntime runtime) {
        runtime.beginQuiesce();
        final long deadline = System.nanoTime() + 5_000_000_000L;
        while (runtime.state() != FiberRuntimeState.CLOSED && System.nanoTime() < deadline) {
            runtime.drain(64);
        }
        Assert.assertTrue(runtime.awaitClosed(deadline));
        runtime.closeAfterDrained();
    }

    private static void launch(
            CountDownLatch start,
            AtomicReference<Throwable> error,
            AtomicReference<LaunchResult> result,
            PGConnectionFiberTask task,
            FiberRuntime runtime
    ) {
        try {
            start.await();
            result.set(task.launch(runtime, IOOperation.READ));
        } catch (Throwable th) {
            error.compareAndSet(null, th);
        }
    }

    private static TestContext newTestContext() {
        final PGConfiguration configuration = new Port0PGConfiguration(-1);
        final NetworkSqlExecutionCircuitBreaker circuitBreaker = new NetworkSqlExecutionCircuitBreaker(
                engine,
                configuration.getCircuitBreakerConfiguration()
        );
        final SqlExecutionContextImpl sqlExecutionContext = new SqlExecutionContextImpl(engine, 1);
        final NoOpAssociativeCache<TypesAndSelect> typesAndSelectCache = new NoOpAssociativeCache<>();
        try {
            return new TestContext(
                    engine,
                    configuration,
                    sqlExecutionContext,
                    circuitBreaker,
                    typesAndSelectCache
            );
        } catch (Throwable th) {
            Misc.free(typesAndSelectCache, th);
            Misc.free(sqlExecutionContext, th);
            Misc.free(circuitBreaker, th);
            throw th;
        }
    }

    private static SlotWaitingTestContext newSlotWaitingTestContext(
            PerWorkerLocks locks,
            SqlExecutionCircuitBreaker circuitBreaker
    ) {
        final PGConfiguration configuration = new Port0PGConfiguration(-1);
        final NetworkSqlExecutionCircuitBreaker networkCircuitBreaker = new NetworkSqlExecutionCircuitBreaker(
                engine,
                configuration.getCircuitBreakerConfiguration()
        );
        final SqlExecutionContextImpl sqlExecutionContext = new SqlExecutionContextImpl(engine, 1);
        final NoOpAssociativeCache<TypesAndSelect> typesAndSelectCache = new NoOpAssociativeCache<>();
        try {
            return new SlotWaitingTestContext(
                    engine,
                    configuration,
                    sqlExecutionContext,
                    networkCircuitBreaker,
                    typesAndSelectCache,
                    locks,
                    circuitBreaker
            );
        } catch (Throwable th) {
            Misc.free(typesAndSelectCache, th);
            Misc.free(sqlExecutionContext, th);
            Misc.free(networkCircuitBreaker, th);
            throw th;
        }
    }

    private static class SlotWaitingTestContext extends PGConnectionContext {
        private String breakerErrorMessage;
        private final SqlExecutionCircuitBreaker circuitBreaker;
        private final PerWorkerLocks locks;
        private NetworkSqlExecutionCircuitBreaker networkCircuitBreaker;
        private TimerShards observedTimerShards;
        private SqlExecutionContextImpl sqlExecutionContext;
        private NoOpAssociativeCache<TypesAndSelect> typesAndSelectCache;
        private Fiber waitingFiber;

        private SlotWaitingTestContext(
                CairoEngine engine,
                PGConfiguration configuration,
                SqlExecutionContextImpl sqlExecutionContext,
                NetworkSqlExecutionCircuitBreaker networkCircuitBreaker,
                NoOpAssociativeCache<TypesAndSelect> typesAndSelectCache,
                PerWorkerLocks locks,
                SqlExecutionCircuitBreaker circuitBreaker
        ) {
            super(engine, configuration, sqlExecutionContext, networkCircuitBreaker, typesAndSelectCache);
            this.circuitBreaker = circuitBreaker;
            this.locks = locks;
            this.networkCircuitBreaker = networkCircuitBreaker;
            this.sqlExecutionContext = sqlExecutionContext;
            this.typesAndSelectCache = typesAndSelectCache;
        }

        @Override
        public void close() {
            super.close();
            typesAndSelectCache = Misc.free(typesAndSelectCache);
            sqlExecutionContext = Misc.free(sqlExecutionContext);
            networkCircuitBreaker = Misc.free(networkCircuitBreaker);
        }

        @Override
        public void handleClientOperation(int operation) throws PeerIsSlowToWriteException {
            observedTimerShards = SuspensionScope.getTimerShards(SuspensionScope.scope());
            waitingFiber = Fiber.current();
            try {
                final int slot = locks.acquireSlot(0, circuitBreaker);
                locks.releaseSlot(slot);
            } catch (CairoException e) {
                breakerErrorMessage = e.getFlyweightMessage().toString();
            }
            throw PeerIsSlowToWriteException.INSTANCE;
        }
    }

    private static class TestContext extends PGConnectionContext {
        private int callCount;
        private NetworkSqlExecutionCircuitBreaker circuitBreaker;
        private int firstOperation;
        private int secondOperation;
        private SqlExecutionContextImpl sqlExecutionContext;
        private NoOpAssociativeCache<TypesAndSelect> typesAndSelectCache;

        private TestContext(
                CairoEngine engine,
                PGConfiguration configuration,
                SqlExecutionContextImpl sqlExecutionContext,
                NetworkSqlExecutionCircuitBreaker circuitBreaker,
                NoOpAssociativeCache<TypesAndSelect> typesAndSelectCache
        ) {
            super(engine, configuration, sqlExecutionContext, circuitBreaker, typesAndSelectCache);
            this.circuitBreaker = circuitBreaker;
            this.sqlExecutionContext = sqlExecutionContext;
            this.typesAndSelectCache = typesAndSelectCache;
        }

        @Override
        public void close() {
            super.close();
            typesAndSelectCache = Misc.free(typesAndSelectCache);
            sqlExecutionContext = Misc.free(sqlExecutionContext);
            circuitBreaker = Misc.free(circuitBreaker);
        }

        @Override
        public void handleClientOperation(int operation) throws PeerIsSlowToWriteException {
            if (callCount++ == 0) {
                firstOperation = operation;
            } else {
                secondOperation = operation;
            }
            throw PeerIsSlowToWriteException.INSTANCE;
        }
    }

    private static class TestDispatcher implements IODispatcher<PGConnectionContext> {
        private int disconnectCount;
        private TestContext firstRequestContext;
        private boolean isQuiesceBeforeWake;
        private long reservationEpoch;
        private Fiber reservedFiber;
        private FiberRuntime runtime;
        private TestContext secondRequestContext;
        private PGConnectionFiberTask task;
        private int registerCount;
        private int wakeOperation;
        private LaunchResult wakeResult;

        @Override
        public void close() {
        }

        @Override
        public void disconnect(PGConnectionContext context, int reason) {
            disconnectCount++;
        }

        @Override
        public int getConnectionCount() {
            return 0;
        }

        @Override
        public int getPort() {
            return 0;
        }

        @Override
        public boolean isListening() {
            return false;
        }

        @Override
        public boolean processIOQueue(IORequestProcessor<PGConnectionContext> processor) {
            if (firstRequestContext == null) {
                return false;
            }
            final TestContext firstContext = firstRequestContext;
            final TestContext secondContext = secondRequestContext;
            firstRequestContext = null;
            secondRequestContext = null;
            return processor.onRequest(IOOperation.READ, firstContext, this)
                    | processor.onRequest(IOOperation.READ, secondContext, this);
        }

        @Override
        public void registerChannel(PGConnectionContext context, int operation) {
            registerCount++;
            if (wakeOperation != 0) {
                final int nextOperation = wakeOperation;
                wakeOperation = 0;
                if (isQuiesceBeforeWake) {
                    runtime.beginQuiesce();
                }
                if (reservedFiber != null) {
                    wakeResult = task.launchReserved(runtime, reservedFiber, reservationEpoch, nextOperation);
                    reservedFiber = null;
                } else {
                    wakeResult = task.launch(runtime, nextOperation);
                }
            }
        }

        @Override
        public boolean run(@NotNull Job.WorkerContext workerContext) {
            return false;
        }
    }
}
