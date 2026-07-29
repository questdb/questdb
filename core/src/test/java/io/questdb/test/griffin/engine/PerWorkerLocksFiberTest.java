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
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.griffin.engine.groupby.vect.GroupByVectorAggregateJob;
import io.questdb.griffin.engine.groupby.vect.VectorAggregateEntry;
import io.questdb.mp.MPSequence;
import io.questdb.mp.RingQueue;
import io.questdb.mp.Sequence;
import io.questdb.mp.continuation.FiberCancellationSignal;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.FiberRuntimeState;
import io.questdb.mp.continuation.FiberTask;
import io.questdb.mp.continuation.LaunchResult;
import io.questdb.mp.continuation.SuspensionScope;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.tasks.VectorAggregateTask;
import io.questdb.test.AbstractCairoTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

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
        final SuspensionScope.Mode previousMode = SuspensionScope.enter(SuspensionScope.Mode.FORBIDDEN);
        try {
            Assert.assertTrue(job.run());
            Assert.assertTrue(entry.hasRun);
            Assert.assertEquals(SuspensionScope.Mode.BLOCKING, entry.observedMode);
            Assert.assertEquals(SuspensionScope.Mode.FORBIDDEN, SuspensionScope.getMode());
        } finally {
            SuspensionScope.restore(previousMode);
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
        private final PerWorkerLocks locks;
        private boolean hasError;
        private boolean hasRun;

        private SlotTask(PerWorkerLocks locks, @Nullable FiberCancellationSignal cancellationSignal) {
            this.cancellationSignal = cancellationSignal;
            this.locks = locks;
        }

        @Override
        public @Nullable FiberCancellationSignal getCancellationSignal() {
            return cancellationSignal;
        }

        @Override
        protected void onError(Throwable th) {
            hasError = true;
        }

        @Override
        protected boolean runStep() {
            final int slot = locks.acquireSlot(0, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
            try {
                hasRun = true;
            } finally {
                locks.releaseSlot(slot);
            }
            return true;
        }
    }
}
