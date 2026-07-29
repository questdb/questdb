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

import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.async.PageFrameReduceDispatcher;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.cairo.sql.async.UnorderedPageFrameReduceTask;
import io.questdb.cairo.sql.async.UnorderedPageFrameSequence;
import io.questdb.mp.MCSequence;
import io.questdb.mp.MPSequence;
import io.questdb.mp.RingQueue;
import io.questdb.mp.continuation.Fiber;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.FiberRuntimeState;
import io.questdb.mp.continuation.FiberTask;
import io.questdb.mp.continuation.FiberWaitCoordinator;
import io.questdb.mp.continuation.FiberWalWaitQueue;
import io.questdb.mp.continuation.FiberWalWaitRegistration;
import io.questdb.mp.continuation.LaunchResult;
import io.questdb.mp.continuation.SourceRegistrationResult;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.lang.management.ManagementFactory;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class PageFrameReduceDispatcherTest extends AbstractCairoTest {
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
        final long cursor = pubSeq.next();
        if (cursor < 0) {
            throw new IllegalStateException("test publisher is unexpectedly blocked");
        }
        queue.get(cursor).of(frameSequence, 0, false);
        pubSeq.done(cursor);
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
