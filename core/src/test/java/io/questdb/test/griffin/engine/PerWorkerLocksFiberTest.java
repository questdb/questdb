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

package io.questdb.test.griffin.engine;

import io.questdb.MessageBus;
import io.questdb.cairo.CairoConfigurationWrapper;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.ExecutionCircuitBreaker;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.griffin.engine.groupby.vect.GroupByVectorAggregateJob;
import io.questdb.griffin.engine.groupby.vect.VectorAggregateEntry;
import io.questdb.log.LogFactory;
import io.questdb.mp.MPSequence;
import io.questdb.mp.RingQueue;
import io.questdb.mp.Sequence;
import io.questdb.mp.continuation.CancellationBinding;
import io.questdb.mp.continuation.DelayedFireable;
import io.questdb.mp.continuation.Fiber;
import io.questdb.mp.continuation.FiberCancellationSignal;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.FiberRuntimeState;
import io.questdb.mp.continuation.FiberTask;
import io.questdb.mp.continuation.LaunchResult;
import io.questdb.mp.continuation.SourceRegistrationResult;
import io.questdb.mp.continuation.SuspensionScope;
import io.questdb.mp.continuation.TimerShards;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.tasks.VectorAggregateTask;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestMillisecondClock;
import io.questdb.test.tools.TestNetworkSqlExecutionCircuitBreaker;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.ToIntFunction;

public class PerWorkerLocksFiberTest extends AbstractCairoTest {

    @Test
    public void testCancellationWakesSlotWaiterAndDoesNotLeakSlot() {
        final PerWorkerLocks locks = new PerWorkerLocks(configuration, 1);
        final int heldSlot = locks.acquireSlot(0, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
        final FiberCancellationSignal cancellationSignal = new FiberCancellationSignal();
        final FiberRuntime runtime = new FiberRuntime(2, 2);
        final SlotTask task = new SlotTask(locks, cancellationSignal);
        try {
            Assert.assertSame(LaunchResult.LAUNCHED, runtime.launch(task));
            Assert.assertEquals(1, runtime.drain(1));
            Assert.assertEquals(1, runtime.getParkedFiberCount());

            cancellationSignal.cancel();
            Assert.assertEquals(1, runtime.drain(1));

            Assert.assertTrue(task.hasError);
            Assert.assertFalse(task.hasRun);
            Assert.assertEquals(1, locks.getAcquiredSlotCount());
        } finally {
            locks.releaseSlot(heldSlot);
            close(runtime);
        }
        Assert.assertEquals(0, locks.getAcquiredSlotCount());
    }

    @Test
    public void testConcurrentSlotTransfersPreserveExclusiveOwnership() throws Exception {
        final int carrierCount = 4;
        final int taskCount = 64;
        final AtomicInteger activeCount = new AtomicInteger();
        final CountDownLatch doneLatch = new CountDownLatch(taskCount);
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final PerWorkerLocks locks = new PerWorkerLocks(configuration, 1);
        final FiberRuntime runtime = new FiberRuntime(taskCount, taskCount);
        final ObjList<Thread> carriers = new ObjList<>(carrierCount);
        try {
            for (int i = 0; i < taskCount; i++) {
                Assert.assertSame(
                        LaunchResult.LAUNCHED,
                        runtime.launch(new ExclusiveSlotTask(locks, activeCount, doneLatch, failure))
                );
            }
            for (int i = 0; i < carrierCount; i++) {
                final Thread carrier = new Thread(() -> {
                    runtime.initializeCarrier();
                    try {
                        while (doneLatch.getCount() > 0) {
                            runtime.drain(8);
                            Os.pause();
                        }
                    } catch (Throwable th) {
                        failure.compareAndSet(null, th);
                    }
                });
                carriers.add(carrier);
                carrier.start();
            }

            Assert.assertTrue(doneLatch.await(10, TimeUnit.SECONDS));
            for (int i = 0; i < carriers.size(); i++) {
                final Thread carrier = carriers.getQuick(i);
                carrier.join(10_000);
                Assert.assertFalse(carrier.isAlive());
            }
            Assert.assertNull(failure.get());
            Assert.assertEquals(0, activeCount.get());
            Assert.assertEquals(0, locks.getAcquiredSlotCount());
        } finally {
            close(runtime);
        }
    }

    @Test
    public void testGenericCancellationAfterSlotGrantDoesNotLeakSlot() throws Exception {
        assertGenericCancellation(false);
    }

    @Test
    public void testGenericCancellationWakesSlotWaiterDoesNotLeakSlot() throws Exception {
        assertGenericCancellation(true);
    }

    @Test
    public void testGenericSqlCancellationAfterSlotGrantPreservesReason() throws Exception {
        assertSqlCancellation(false, false);
    }

    @Test
    public void testGenericSqlCancellationWakesSlotWaiterPreservesReason() throws Exception {
        assertSqlCancellation(true, false);
    }

    @Test
    public void testGrantedSlotPreservesConnectionCheckThrottle() throws Exception {
        assertMemoryLeak(() -> {
            try (TestNetworkSqlExecutionCircuitBreaker circuitBreaker = TestNetworkSqlExecutionCircuitBreaker.create(
                    engine, new TestMillisecondClock(1_000), 100, Long.MAX_VALUE
            )) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                Assert.assertEquals(1, circuitBreaker.probeCount);
                final PerWorkerLocks locks = new PerWorkerLocks(configuration, 1);
                final int heldSlot = locks.acquireSlot(0, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
                final FiberRuntime runtime = new FiberRuntime(1);
                final SlotTask task = new SlotTask(locks, null, circuitBreaker, null);
                boolean isHeldSlotReleased = false;
                try {
                    Assert.assertSame(LaunchResult.LAUNCHED, runtime.launch(task));
                    Assert.assertEquals(1, runtime.drain(1));
                    Assert.assertEquals(1, runtime.getParkedFiberCount());
                    isHeldSlotReleased = true;
                    locks.releaseSlot(heldSlot);
                    Assert.assertEquals(1, runtime.drain(1));

                    Assert.assertNull(task.error);
                    Assert.assertTrue(task.hasRun);
                    Assert.assertEquals(1, circuitBreaker.probeCount);
                    Assert.assertEquals(0, locks.getAcquiredSlotCount());
                } finally {
                    if (!isHeldSlotReleased) {
                        locks.releaseSlot(heldSlot);
                    }
                    close(runtime);
                }
            }
        });
    }

    @Test
    public void testPinnedSlotWaitFailsWithoutBlockingCarrier() {
        final PerWorkerLocks locks = new PerWorkerLocks(configuration, 1);
        final int heldSlot = locks.acquireSlot(0, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
        final FiberRuntime runtime = new FiberRuntime(1);
        final PinnedSlotTask task = new PinnedSlotTask(locks);
        try {
            Assert.assertSame(LaunchResult.LAUNCHED, runtime.launch(task));
            Assert.assertEquals(1, runtime.drain(1));

            Assert.assertNotNull(task.error);
            Throwable error = task.error;
            while (error.getCause() != null) {
                error = error.getCause();
            }
            TestUtils.assertContains(error.getMessage(), "reducer slot wait could not suspend");
            Assert.assertFalse(task.hasRun);
            Assert.assertTrue(runtime.getInlineSuspendViolationCount() > 0);
            Assert.assertEquals(1, locks.getAcquiredSlotCount());
        } finally {
            locks.releaseSlot(heldSlot);
            close(runtime);
        }
        Assert.assertEquals(0, locks.getAcquiredSlotCount());
    }

    @Test
    public void testReleaseDoesNotLoseWaiterRegisteredDuringRelease() throws Exception {
        final PerWorkerLocks locks = new PerWorkerLocks(configuration, 1);
        final int heldSlot = locks.acquireSlot(0, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
        final FiberRuntime runtime = new FiberRuntime(2, 2);
        final SlotTask task = new SlotTask(locks, null);
        final CountDownLatch releasePaused = new CountDownLatch(1);
        final CountDownLatch resumeRelease = new CountDownLatch(1);
        final AtomicReference<Throwable> releaseFailure = new AtomicReference<>();
        final Thread releaseThread = new Thread(() -> {
            try {
                locks.releaseSlot(heldSlot);
            } catch (Throwable th) {
                releaseFailure.set(th);
            }
        });
        locks.setTestBeforeSlotRelease(() -> {
            releasePaused.countDown();
            try {
                if (!resumeRelease.await(5, TimeUnit.SECONDS)) {
                    throw new AssertionError("release did not resume");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        });
        try {
            releaseThread.start();
            Assert.assertTrue(releasePaused.await(5, TimeUnit.SECONDS));
            Assert.assertSame(LaunchResult.LAUNCHED, runtime.launch(task));
            Assert.assertEquals(1, runtime.drain(1));
            Assert.assertEquals(1, runtime.getParkedFiberCount());

            resumeRelease.countDown();
            releaseThread.join(5_000);
            Assert.assertFalse(releaseThread.isAlive());
            Assert.assertNull(releaseFailure.get());
            Assert.assertEquals(1, runtime.drain(1));
            Assert.assertTrue(task.hasRun);
            Assert.assertEquals(0, locks.getAcquiredSlotCount());
        } finally {
            resumeRelease.countDown();
            locks.setTestBeforeSlotRelease(null);
            releaseThread.join(5_000);
            close(runtime);
        }
    }

    @Test
    public void testReleaseTransfersSlotToParkedFiber() {
        final PerWorkerLocks locks = new PerWorkerLocks(configuration, 1);
        final int heldSlot = locks.acquireSlot(0, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
        final FiberRuntime runtime = new FiberRuntime(2, 2);
        final SlotTask task = new SlotTask(locks, null);
        try {
            Assert.assertSame(LaunchResult.LAUNCHED, runtime.launch(task));
            Assert.assertEquals(1, runtime.drain(1));
            Assert.assertEquals(1, runtime.getParkedFiberCount());

            locks.releaseSlot(heldSlot);
            Assert.assertEquals(1, runtime.drain(1));

            Assert.assertFalse(task.hasError);
            Assert.assertTrue(task.hasRun);
            Assert.assertEquals(0, locks.getAcquiredSlotCount());
        } finally {
            close(runtime);
        }
    }

    @Test
    public void testShutdownUnlinksSlotWaiter() {
        final PerWorkerLocks locks = new PerWorkerLocks(configuration, 1);
        final int heldSlot = locks.acquireSlot(0, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
        final FiberRuntime runtime = new FiberRuntime(2, 2);
        final SlotTask task = new SlotTask(locks, null);
        try {
            Assert.assertSame(LaunchResult.LAUNCHED, runtime.launch(task));
            Assert.assertEquals(1, runtime.drain(1));
            Assert.assertEquals(1, runtime.getParkedFiberCount());

            runtime.beginQuiesce();
            close(runtime);

            Assert.assertFalse(task.hasRun);
            Assert.assertEquals(1, locks.getAcquiredSlotCount());
        } finally {
            locks.releaseSlot(heldSlot);
            if (runtime.state() != FiberRuntimeState.CLOSED) {
                close(runtime);
            }
        }
        Assert.assertEquals(0, locks.getAcquiredSlotCount());
    }

    @Test
    public void testSqlCancellationAfterSlotGrantPreservesReason() throws Exception {
        assertSqlCancellation(false, true);
    }

    @Test
    public void testSqlCancellationWakesSlotWaiterPreservesReason() throws Exception {
        assertSqlCancellation(true, true);
    }

    @Test
    public void testSupplementalCancellationWakesSlotWaiterAndDoesNotLeakSlot() {
        final PerWorkerLocks locks = new PerWorkerLocks(configuration, 1);
        final int heldSlot = locks.acquireSlot(0, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
        final FiberCancellationSignal cancellationSignal = new FiberCancellationSignal();
        final FiberCancellationSignal supplementalCancellationSignal = new FiberCancellationSignal();
        final FiberRuntime runtime = new FiberRuntime(2, 2);
        final SlotTask task = new SlotTask(locks, cancellationSignal, supplementalCancellationSignal);
        try {
            Assert.assertSame(LaunchResult.LAUNCHED, runtime.launch(task));
            Assert.assertEquals(1, runtime.drain(1));
            Assert.assertEquals(1, runtime.getParkedFiberCount());

            supplementalCancellationSignal.cancel();
            Assert.assertEquals(1, runtime.drain(1));

            Assert.assertTrue(task.hasError);
            Assert.assertFalse(task.hasRun);
            Assert.assertFalse(cancellationSignal.get());
            Assert.assertEquals(1, locks.getAcquiredSlotCount());
        } finally {
            locks.releaseSlot(heldSlot);
            close(runtime);
        }
        Assert.assertEquals(0, locks.getAcquiredSlotCount());
    }

    @Test
    public void testTimerRefusalDuringShutdownReportsClosingAndCleansUp() throws Exception {
        final CountDownLatch blockerEntered = new CountDownLatch(1);
        final CountDownLatch releaseBlocker = new CountDownLatch(1);
        final AtomicBoolean isShutdownHookEnabled = new AtomicBoolean();
        final AtomicReference<Throwable> shutdownFailure = new AtomicReference<>();
        final AtomicReference<Thread> shutdownThreadRef = new AtomicReference<>();
        final AtomicReference<TimerShards> timerShardsRef = new AtomicReference<>();
        final TimerShards timerShards = new TimerShards(
                1,
                "test-slot-shutdown-timer",
                LogFactory.getLog(PerWorkerLocksFiberTest.class),
                () -> {
                    if (isShutdownHookEnabled.compareAndSet(true, false)) {
                        final Thread shutdownThread = new Thread(() -> {
                            try {
                                timerShardsRef.get().shutdown();
                            } catch (Throwable th) {
                                shutdownFailure.set(th);
                            }
                        }, "test-slot-timer-shutdown");
                        shutdownThread.setDaemon(true);
                        shutdownThreadRef.set(shutdownThread);
                        shutdownThread.start();
                        awaitThreadWaiting(shutdownThread);
                    }
                }
        );
        timerShardsRef.set(timerShards);

        final PerWorkerLocks locks = new PerWorkerLocks(configuration, 1);
        final int heldSlot = locks.acquireSlot(0, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
        final FiberRuntime runtime = new FiberRuntime(2, 2);
        final SlotTask task = new SlotTask(
                locks,
                null,
                SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER,
                timerShards
        );
        timerShards.start();
        try {
            Assert.assertSame(
                    SourceRegistrationResult.ACCEPTED,
                    timerShards.register(new TestTimerEntry(System.currentTimeMillis(), () -> {
                        blockerEntered.countDown();
                        try {
                            releaseBlocker.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }, null))
            );
            Assert.assertTrue(blockerEntered.await(5, TimeUnit.SECONDS));

            isShutdownHookEnabled.set(true);
            Assert.assertSame(LaunchResult.LAUNCHED, runtime.launch(task));
            Assert.assertEquals(1, runtime.drain(1));

            Assert.assertNotNull(task.error);
            Assert.assertTrue(task.error instanceof CairoException);
            final CairoException error = (CairoException) task.error;
            Assert.assertTrue(error.isInterruption());
            TestUtils.assertContains(error.getMessage(), "query aborted, server is closing");
            Assert.assertFalse(error.getMessage().contains("reducer slot wait could not suspend"));
            Assert.assertTrue(task.hasError);
            Assert.assertFalse(task.hasRun);
            Assert.assertEquals(1, locks.getAcquiredSlotCount());
            Assert.assertEquals(0, runtime.getMountedCount());
            Assert.assertEquals(0, runtime.getOutstandingTaskCount());
            Assert.assertEquals(0, runtime.getParkedFiberCount());
            Assert.assertEquals(0, runtime.getQueuedCount());

            releaseBlocker.countDown();
            final Thread shutdownThread = shutdownThreadRef.get();
            Assert.assertNotNull(shutdownThread);
            shutdownThread.join(TimeUnit.SECONDS.toMillis(10));
            Assert.assertFalse(shutdownThread.isAlive());
            Assert.assertNull(shutdownFailure.get());
            Assert.assertEquals(0, timerShards.size());

            close(runtime);
            Assert.assertEquals(FiberRuntimeState.CLOSED, runtime.state());
            Assert.assertEquals(0, runtime.getLiveFiberCount());
            Assert.assertEquals(0, runtime.getMountedCount());
            Assert.assertEquals(0, runtime.getOutstandingTaskCount());
            Assert.assertEquals(0, runtime.getParkedFiberCount());
            Assert.assertEquals(0, runtime.getQueuedCount());
            Assert.assertEquals(0, runtime.getRetainedFiberCount());
        } finally {
            releaseBlocker.countDown();
            final Thread shutdownThread = shutdownThreadRef.get();
            if (shutdownThread != null) {
                shutdownThread.join(TimeUnit.SECONDS.toMillis(10));
            }
            timerShards.shutdown();
            locks.releaseSlot(heldSlot);
            if (runtime.state() != FiberRuntimeState.CLOSED) {
                close(runtime);
            }
        }
        Assert.assertEquals(0, locks.getAcquiredSlotCount());
    }

    @Test
    public void testTimerWakesSlotWaiterToObserveCancellation() {
        final PerWorkerLocks locks = new PerWorkerLocks(new CairoConfigurationWrapper(configuration) {
            @Override
            public long getQueryContinuationWakeIntervalMillis() {
                return 20;
            }
        }, 1);
        final int heldSlot = locks.acquireSlot(0, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
        final AtomicBooleanCircuitBreaker circuitBreaker = new AtomicBooleanCircuitBreaker(engine);
        final FiberRuntime runtime = new FiberRuntime(2, 2);
        final SlotTask task = new SlotTask(locks, null, circuitBreaker, engine.getTimerShards());
        try {
            Assert.assertSame(LaunchResult.LAUNCHED, runtime.launch(task));
            Assert.assertEquals(1, runtime.drain(1));
            Assert.assertEquals(1, runtime.getParkedFiberCount());

            circuitBreaker.cancel();
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (!task.hasError && System.nanoTime() < deadline) {
                runtime.drain(1);
                Os.pause();
            }

            Assert.assertTrue(task.hasError);
            Assert.assertFalse(task.hasRun);
            Assert.assertEquals(1, locks.getAcquiredSlotCount());
        } finally {
            locks.releaseSlot(heldSlot);
            close(runtime);
        }
        Assert.assertEquals(0, locks.getAcquiredSlotCount());
    }

    @Test
    public void testVectorAggregateJobRunsEntryInBlockingScope() {
        final MessageBus messageBus = engine.getMessageBus();
        final MPSequence pubSeq = messageBus.getVectorAggregatePubSeq();
        final RingQueue<VectorAggregateTask> queue = messageBus.getVectorAggregateQueue();
        final long cursor = pubSeq.next();
        Assert.assertTrue(cursor > -1);

        final ScopeAssertingEntry entry = new ScopeAssertingEntry();
        queue.get(cursor).entry = entry;
        pubSeq.done(cursor);

        final GroupByVectorAggregateJob job = new GroupByVectorAggregateJob(messageBus);
        final SuspensionScope.Mode previousMode = SuspensionScope.enter(SuspensionScope.Mode.BLOCKING);
        try {
            Assert.assertTrue(job.run());
            Assert.assertTrue(entry.hasRun);
            Assert.assertEquals(SuspensionScope.Mode.BLOCKING, entry.observedMode);
            Assert.assertEquals(SuspensionScope.Mode.BLOCKING, SuspensionScope.getMode());
        } finally {
            SuspensionScope.restore(previousMode);
        }
    }

    private void assertGenericCancellation(boolean isCancellationFirst) throws Exception {
        assertMemoryLeak(() -> {
            final FiberCancellationSignal signal = new FiberCancellationSignal();
            final ExecutionCircuitBreaker circuitBreaker = signal::get;
            final CairoException error = assertSlotWaitCancelled(
                    signal, signal, locks -> locks.acquireSlot(0, circuitBreaker), isCancellationFirst
            );
            Assert.assertEquals(SqlExecutionCircuitBreaker.STATE_TIMEOUT, error.getInterruptionReason());
            TestUtils.assertEquals("query aborted", error.getFlyweightMessage());
        });
    }

    private CairoException assertSlotWaitCancelled(
            FiberCancellationSignal signal,
            @Nullable FiberCancellationSignal taskSignal,
            ToIntFunction<PerWorkerLocks> acquireSlot,
            boolean isCancellationFirst
    ) {
        final AtomicInteger releaseCount = new AtomicInteger();
        final PerWorkerLocks locks = new PerWorkerLocks(configuration, 1) {
            @Override
            public void releaseSlot(int slot) {
                releaseCount.incrementAndGet();
                super.releaseSlot(slot);
            }
        };
        final int heldSlot = locks.acquireSlot(0, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
        final FiberRuntime runtime = new FiberRuntime(1);
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final AtomicReference<Fiber> fiber = new AtomicReference<>();
        final AtomicBoolean hasRun = new AtomicBoolean();
        final FiberTask task = new FiberTask() {
            @Override
            public @Nullable FiberCancellationSignal getCancellationSignal() {
                return taskSignal;
            }

            @Override
            protected void onError(Throwable th) {
                failure.set(th);
            }

            @Override
            protected boolean runStep() {
                fiber.set(Fiber.current());
                final int slot = acquireSlot.applyAsInt(locks);
                try {
                    hasRun.set(true);
                } finally {
                    locks.releaseSlot(slot);
                }
                return true;
            }
        };
        boolean isHeldSlotReleased = false;
        try {
            Assert.assertSame(LaunchResult.LAUNCHED, runtime.launch(task));
            Assert.assertEquals(1, runtime.drain(1));
            Assert.assertEquals(1, runtime.getParkedFiberCount());
            Assert.assertTrue(fiber.get().getWaitCoordinator().hasInFlightRegistrations());

            if (!isCancellationFirst) {
                // Grant the slot while the continuation is parked, then cancel before it resumes.
                isHeldSlotReleased = true;
                locks.releaseSlot(heldSlot);
                Assert.assertEquals(1, releaseCount.get());
                Assert.assertEquals(1, locks.getAcquiredSlotCount());
            }
            signal.cancel();
            Assert.assertEquals(1, runtime.drain(1));

            Assert.assertFalse(hasRun.get());
            Assert.assertTrue(failure.get() instanceof CairoException);
            Assert.assertTrue(((CairoException) failure.get()).isInterruption());
            Assert.assertEquals(isCancellationFirst ? 1 : 0, locks.getAcquiredSlotCount());
            Assert.assertEquals(isCancellationFirst ? 0 : 2, releaseCount.get());
            Assert.assertFalse(fiber.get().getWaitCoordinator().hasInFlightRegistrations());
            Assert.assertEquals(0, runtime.getOutstandingTaskCount());
            Assert.assertEquals(0, runtime.getMountedCount());
            Assert.assertEquals(0, runtime.getParkedFiberCount());
            Assert.assertEquals(0, runtime.getQueuedCount());
            signal.reset();
        } finally {
            if (!isHeldSlotReleased) {
                locks.releaseSlot(heldSlot);
            }
            close(runtime);
        }
        Assert.assertEquals(0, locks.getAcquiredSlotCount());
        Assert.assertEquals(isCancellationFirst ? 1 : 2, releaseCount.get());
        return (CairoException) failure.get();
    }

    private void assertSqlCancellation(boolean isCancellationFirst, boolean isSqlOverload) throws Exception {
        assertMemoryLeak(() -> {
            try (NetworkSqlExecutionCircuitBreaker circuitBreaker = new NetworkSqlExecutionCircuitBreaker(
                    engine, new DefaultSqlExecutionCircuitBreakerConfiguration()
            )) {
                final FiberCancellationSignal signal = new FiberCancellationSignal();
                circuitBreaker.setCancelledFlag(signal);
                // Leave the next call inside the count throttle window before the wait starts.
                circuitBreaker.statefulThrowExceptionIfTripped();
                final CairoException error = assertSlotWaitCancelled(
                        signal,
                        null,
                        locks -> isSqlOverload
                                ? locks.acquireSlot(0, circuitBreaker)
                                : locks.acquireSlot(0, (ExecutionCircuitBreaker) circuitBreaker),
                        isCancellationFirst
                );
                Assert.assertEquals(SqlExecutionCircuitBreaker.STATE_CANCELLED, error.getInterruptionReason());
                TestUtils.assertEquals("cancelled by user", error.getFlyweightMessage());
            }
        });
    }

    private static void awaitThreadWaiting(Thread thread) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            final Thread.State state = thread.getState();
            if (state == Thread.State.BLOCKED
                    || state == Thread.State.TIMED_WAITING
                    || state == Thread.State.WAITING) {
                return;
            }
            if (state == Thread.State.TERMINATED) {
                Assert.fail("thread terminated before waiting [name=" + thread.getName() + ']');
            }
            LockSupport.parkNanos(100_000);
        }
        Assert.fail("thread did not wait [name=" + thread.getName() + ", state=" + thread.getState() + ']');
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

    private static class ExclusiveSlotTask extends FiberTask {
        private final AtomicInteger activeCount;
        private final CountDownLatch doneLatch;
        private final AtomicReference<Throwable> failure;
        private final PerWorkerLocks locks;

        private ExclusiveSlotTask(
                PerWorkerLocks locks,
                AtomicInteger activeCount,
                CountDownLatch doneLatch,
                AtomicReference<Throwable> failure
        ) {
            this.activeCount = activeCount;
            this.doneLatch = doneLatch;
            this.failure = failure;
            this.locks = locks;
        }

        @Override
        protected void onDone() {
            doneLatch.countDown();
        }

        @Override
        protected void onError(Throwable th) {
            failure.compareAndSet(null, th);
        }

        @Override
        protected boolean runStep() {
            final int slot = locks.acquireSlot(0, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
            boolean isActive = false;
            try {
                if (!activeCount.compareAndSet(0, 1)) {
                    throw new AssertionError("slot has multiple owners");
                }
                isActive = true;
                for (int i = 0; i < 1_000; i++) {
                    Os.pause();
                }
            } finally {
                if (isActive && !activeCount.compareAndSet(1, 0)) {
                    failure.compareAndSet(null, new AssertionError("slot ownership changed"));
                }
                locks.releaseSlot(slot);
            }
            return true;
        }
    }

    private static class PinnedSlotTask extends FiberTask {
        private static final ThreadLocal<PinnedSlotTask> CURRENT_TASK = new ThreadLocal<>();
        private Throwable error;
        private boolean hasRun;
        private final PerWorkerLocks locks;

        private PinnedSlotTask(PerWorkerLocks locks) {
            this.locks = locks;
        }

        @Override
        protected void onError(Throwable th) {
            error = th;
        }

        @Override
        protected boolean runStep() {
            CURRENT_TASK.set(this);
            try {
                PinnedSlotTaskInitializer.initialize();
            } finally {
                CURRENT_TASK.remove();
            }
            return true;
        }

        private void runPinned() {
            final int slot = locks.acquireSlot(0, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
            try {
                hasRun = true;
            } finally {
                locks.releaseSlot(slot);
            }
        }
    }

    private static class PinnedSlotTaskInitializer {
        // Class initialization pins the continuation while runPinned() attempts to suspend.
        static {
            PinnedSlotTask.CURRENT_TASK.get().runPinned();
        }

        private static void initialize() {
        }
    }

    private static class ScopeAssertingEntry extends VectorAggregateEntry {
        private boolean hasRun;
        private SuspensionScope.Mode observedMode;

        @Override
        public void run(int workerId, Sequence seq, long cursor) {
            hasRun = true;
            observedMode = SuspensionScope.getMode();
            seq.done(cursor);
        }
    }

    private static class SlotTask extends FiberTask {
        private final @Nullable FiberCancellationSignal cancellationSignal;
        private final SqlExecutionCircuitBreaker circuitBreaker;
        private final PerWorkerLocks locks;
        private final @Nullable FiberCancellationSignal supplementalCancellationSignal;
        private final @Nullable TimerShards timerShards;
        private @Nullable Throwable error;
        private boolean hasError;
        private boolean hasRun;

        private SlotTask(PerWorkerLocks locks, @Nullable FiberCancellationSignal cancellationSignal) {
            this(locks, cancellationSignal, null);
        }

        private SlotTask(
                PerWorkerLocks locks,
                @Nullable FiberCancellationSignal cancellationSignal,
                @Nullable FiberCancellationSignal supplementalCancellationSignal
        ) {
            this(
                    locks,
                    cancellationSignal,
                    supplementalCancellationSignal,
                    SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER,
                    null
            );
        }

        private SlotTask(
                PerWorkerLocks locks,
                @Nullable FiberCancellationSignal cancellationSignal,
                SqlExecutionCircuitBreaker circuitBreaker,
                @Nullable TimerShards timerShards
        ) {
            this(locks, cancellationSignal, null, circuitBreaker, timerShards);
        }

        private SlotTask(
                PerWorkerLocks locks,
                @Nullable FiberCancellationSignal cancellationSignal,
                @Nullable FiberCancellationSignal supplementalCancellationSignal,
                SqlExecutionCircuitBreaker circuitBreaker,
                @Nullable TimerShards timerShards
        ) {
            this.cancellationSignal = cancellationSignal;
            this.circuitBreaker = circuitBreaker;
            this.locks = locks;
            this.supplementalCancellationSignal = supplementalCancellationSignal;
            this.timerShards = timerShards;
        }

        @Override
        public @Nullable FiberCancellationSignal getCancellationSignal() {
            return cancellationSignal;
        }

        @Override
        protected void onError(Throwable th) {
            error = th;
            hasError = true;
        }

        @Override
        protected boolean runStep() {
            final FiberCancellationSignal signal = supplementalCancellationSignal;
            SuspensionScope.enterSupplementalCancellationSignal(
                    signal,
                    signal != null ? signal.getGeneration() : CancellationBinding.NO_GENERATION
            );
            SuspensionScope.enterTimerShards(timerShards);
            final int slot = locks.acquireSlot(0, circuitBreaker);
            try {
                hasRun = true;
            } finally {
                locks.releaseSlot(slot);
            }
            return true;
        }
    }

    private static class TestTimerEntry implements DelayedFireable {
        private final long deadlineMillis;
        private int heapIndex = -1;
        private final @Nullable Runnable onExpire;
        private final @Nullable Runnable onShutdown;

        private TestTimerEntry(
                long deadlineMillis,
                @Nullable Runnable onExpire,
                @Nullable Runnable onShutdown
        ) {
            this.deadlineMillis = deadlineMillis;
            this.onExpire = onExpire;
            this.onShutdown = onShutdown;
        }

        @Override
        public int compareTo(Delayed other) {
            return Long.compare(getDelay(TimeUnit.NANOSECONDS), other.getDelay(TimeUnit.NANOSECONDS));
        }

        @Override
        public void expire() {
            if (onExpire != null) {
                onExpire.run();
            }
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(deadlineMillis - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        }

        @Override
        public int getHeapIndex() {
            return heapIndex;
        }

        @Override
        public void setHeapIndex(int heapIndex) {
            this.heapIndex = heapIndex;
        }

        @Override
        public void shutdown() {
            if (onShutdown != null) {
                onShutdown.run();
            }
        }
    }
}
