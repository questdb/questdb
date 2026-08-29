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

package io.questdb.test.mp;

import io.questdb.mp.continuation.Fiber;
import io.questdb.mp.continuation.FiberDispatchContext;
import io.questdb.mp.continuation.FiberDispatchController;
import io.questdb.mp.continuation.FiberDispatchRequest;
import io.questdb.mp.continuation.FiberDispatchRoute;
import io.questdb.mp.continuation.FiberDispatchSession;
import io.questdb.mp.continuation.FiberDispatchTicket;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.FiberRuntimeState;
import io.questdb.mp.continuation.FiberTask;
import io.questdb.mp.continuation.FiberWaitCoordinator;
import io.questdb.mp.continuation.FiberWakeSink;
import io.questdb.mp.continuation.FiberWalWaitQueue;
import io.questdb.mp.continuation.FiberWalWaitRegistration;
import io.questdb.mp.continuation.LaunchResult;
import io.questdb.mp.continuation.SourceRegistrationResult;
import io.questdb.test.tools.TestUtils;
import jdk.internal.vm.Continuation;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class FiberDispatchControllerTest {

    @Test
    public void testControllerFailureCannotRunTask() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final TestController controller = new TestController();
            final FiberRuntime runtime = newRuntime(1, controller);
            final CountingTask task = new CountingTask();
            final RuntimeException failure = new RuntimeException("dispatch failed");
            controller.session.requestFailure = failure;

            Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(task));
            Assert.assertEquals(1, runtime.getQueuedCount());
            Assert.assertEquals(1, runtime.drain(1));
            Assert.assertEquals(0, task.runCount);
            Assert.assertSame(failure, task.error);

            close(runtime);
        });
    }

    @Test
    public void testDispatchContextChangesOnlyAcrossAuthorizedMounts() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final TestController controller = new TestController();
            final FiberRuntime runtime = newRuntime(1, controller);
            final ContextYieldTask task = new ContextYieldTask();

            Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(task));
            Assert.assertNull(controller.session.pending.element().request.getDispatchContext());
            controller.session.grantNext();
            Assert.assertEquals(1, runtime.drain(1));

            Assert.assertSame(TestDispatchContext.INSTANCE, controller.session.pending.element().request.getDispatchContext());
            Assert.assertFalse(task.observedNewContext);
            controller.session.grantNext();
            Assert.assertEquals(1, runtime.drain(1));

            Assert.assertNull(controller.session.pending.element().request.getDispatchContext());
            Assert.assertTrue(task.observedNewContext);
            Assert.assertFalse(task.observedRestoredContext);
            controller.session.grantNext();
            Assert.assertEquals(1, runtime.drain(1));

            Assert.assertTrue(task.observedRestoredContext);
            Assert.assertEquals(3, controller.ticket.mountContexts.size());
            Assert.assertNull(controller.ticket.mountContexts.get(0));
            Assert.assertSame(TestDispatchContext.INSTANCE, controller.ticket.mountContexts.get(1));
            Assert.assertNull(controller.ticket.mountContexts.get(2));
            close(runtime);
        });
    }

    @Test
    public void testDeniedDirectMountBecomesPendingDemand() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final TestController controller = new TestController();
            final FiberRuntime runtime = newRuntime(1, controller);
            final CountingTask pendingTask = new CountingTask();

            final Fiber pendingFiber = Objects.requireNonNull(runtime.tryReserveFiber());
            Assert.assertEquals(
                    LaunchResult.LAUNCHED,
                    runtime.launchReservedDirect(
                            pendingFiber,
                            pendingFiber.getReservationEpoch(),
                            pendingTask,
                            pendingTask.getIncarnation()
                    )
            );
            Assert.assertEquals(0, pendingTask.runCount);
            Assert.assertEquals(0, runtime.getQueuedCount());
            Assert.assertEquals(FiberDispatchRoute.DIRECT_PENDING, controller.session.peekRoute());

            controller.session.grantNext();
            Assert.assertEquals(1, runtime.getQueuedCount());
            Assert.assertEquals(1, runtime.drain(1));
            Assert.assertEquals(1, pendingTask.runCount);

            controller.session.allowDirect = true;
            final CountingTask inlineTask = new CountingTask();
            final Fiber inlineFiber = Objects.requireNonNull(runtime.tryReserveFiber());
            Assert.assertEquals(
                    LaunchResult.LAUNCHED,
                    runtime.launchReservedDirect(
                            inlineFiber,
                            inlineFiber.getReservationEpoch(),
                            inlineTask,
                            inlineTask.getIncarnation()
                    )
            );
            Assert.assertEquals(1, inlineTask.runCount);
            Assert.assertEquals(0, runtime.getQueuedCount());
            Assert.assertEquals(2, controller.ticket.mountCount);
            Assert.assertEquals(2, controller.ticket.unmountCount);

            close(runtime);
        });
    }

    @Test
    public void testInitialWakeAndPostProcessResignalRequireGrant() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final TestController controller = new TestController();
            final FiberRuntime runtime = newRuntime(2, controller);

            final CountingTask initialTask = new CountingTask();
            Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(initialTask));
            Assert.assertEquals(FiberDispatchRoute.REQUEST_RUN, controller.session.peekRoute());
            Assert.assertEquals(0, runtime.getQueuedCount());
            controller.session.grantNext();
            Assert.assertEquals(1, runtime.drain(1));
            Assert.assertEquals(1, initialTask.runCount);

            final FiberWalWaitQueue wakeQueue = new FiberWalWaitQueue();
            final WaitingTask wakeTask = new WaitingTask(wakeQueue);
            Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(wakeTask));
            controller.session.grantNext();
            Assert.assertEquals(1, runtime.drain(1));
            Assert.assertEquals(1, runtime.getParkedFiberCount());
            wakeQueue.fire(1, false);
            Assert.assertEquals(FiberDispatchRoute.REQUEST_RUN, controller.session.peekRoute());
            Assert.assertEquals(0, runtime.getQueuedCount());
            controller.session.grantNext();
            Assert.assertEquals(1, runtime.drain(1));
            Assert.assertTrue(wakeTask.isDone());

            final FiberWalWaitQueue resignalQueue = new FiberWalWaitQueue();
            final WaitingTask resignalTask = new WaitingTask(resignalQueue);
            Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(resignalTask));
            controller.session.grantNext();
            runtime.setAfterProcessForTesting(() -> {
                runtime.setAfterProcessForTesting(null);
                resignalQueue.fire(1, false);
            });
            Assert.assertEquals(1, runtime.drain(1));
            Assert.assertEquals(FiberDispatchRoute.POST_PROCESS_RESIGNAL, controller.session.peekRoute());
            Assert.assertEquals(0, runtime.getQueuedCount());
            controller.session.grantNext();
            Assert.assertEquals(1, runtime.drain(1));
            Assert.assertTrue(resignalTask.isDone());

            Assert.assertEquals(5, controller.ticket.mountCount);
            Assert.assertEquals(5, controller.ticket.unmountCount);
            close(runtime);
        });
    }

    @Test
    public void testLaunchCapturesInitialDispatchContext() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final TestController controller = new TestController();
            final FiberRuntime runtime = newRuntime(1, controller);
            final InitialContextTask task = new InitialContextTask();

            Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(task, TestDispatchContext.INSTANCE));
            Assert.assertSame(
                    TestDispatchContext.INSTANCE,
                    controller.session.pending.element().request.getDispatchContext()
            );
            controller.session.grantNext();
            Assert.assertEquals(1, runtime.drain(1));
            Assert.assertTrue(task.observedInitialContext);
            Assert.assertSame(TestDispatchContext.INSTANCE, controller.ticket.mountContexts.get(0));

            close(runtime);
        });
    }

    @Test
    public void testMountedTaskYieldsThroughDispatchController() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final TestController controller = new TestController();
            final FiberRuntime runtime = newRuntime(1, controller);
            final DispatchYieldTask task = new DispatchYieldTask();

            Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(task));
            controller.session.grantNext();
            Assert.assertEquals(1, runtime.drain(1));
            Assert.assertEquals(0, runtime.getQueuedCount());
            Assert.assertEquals(FiberDispatchRoute.DISPATCH_YIELD, controller.session.peekRoute());
            Assert.assertEquals(1, task.runCount);
            Assert.assertEquals(0, task.resumeCount);
            Assert.assertFalse(task.isDone());
            Assert.assertEquals(1, controller.ticket.mountCount);
            Assert.assertEquals(1, controller.ticket.unmountCount);

            controller.session.grantNext();
            Assert.assertEquals(1, runtime.drain(1));
            Assert.assertEquals(1, task.runCount);
            Assert.assertEquals(1, task.resumeCount);
            Assert.assertTrue(task.isDone());
            Assert.assertEquals(2, controller.ticket.mountCount);
            Assert.assertEquals(2, controller.ticket.unmountCount);

            close(runtime);
        });
    }

    @Test
    public void testMountFailureReleasesTicketWithoutRunningTask() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final RuntimeException failure = new RuntimeException("pre-mount failure");
            final FailingTicket ticket = new FailingTicket(failure);
            final TestController controller = new TestController(ticket);
            final FiberRuntime runtime = newRuntime(1, controller);
            final CountingTask task = new CountingTask();

            Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(task));
            controller.session.grantNext();
            Assert.assertEquals(1, runtime.drain(1));
            Assert.assertEquals(0, task.runCount);
            Assert.assertSame(failure, task.error);
            Assert.assertEquals(1, ticket.mountCount);
            Assert.assertEquals(1, ticket.unmountCount);
            Assert.assertFalse(ticket.wasMounted);

            close(runtime);
        });
    }

    @Test
    public void testParallelCaptureUsesContextSelectedLane() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final TestController controller = new TestController();
            final FiberRuntime runtime = newRuntime(1, controller);
            final ParallelContextTask task = new ParallelContextTask();

            Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(task, ParallelDispatchContext.CONTROL));
            controller.session.grantNext();
            Assert.assertEquals(1, runtime.drain(1));
            Assert.assertSame(ParallelDispatchContext.PARALLEL, task.capturedContext);

            close(runtime);
        });
    }

    @Test
    public void testPinnedDispatchYieldRestoresMountedStateAndContext() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final TestController controller = new TestController();
            final FiberRuntime runtime = newRuntime(1, controller);
            final PinnedDispatchYieldTask task = new PinnedDispatchYieldTask();

            Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(task));
            controller.session.grantNext();
            Assert.assertEquals(1, runtime.drain(1));
            Assert.assertTrue(task.refusalObserved);
            Assert.assertNull(controller.session.pending.element().request.getDispatchContext());

            controller.session.grantNext();
            Assert.assertEquals(1, runtime.drain(1));
            Assert.assertTrue(task.resumedAfterRefusal);
            close(runtime, 1);
        });
    }

    @Test
    public void testQuiesceDrainsPendingAndGatesShutdownCleanup() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final TestController controller = new TestController();
            final FiberRuntime runtime = newRuntime(2, controller);
            final CountingTask pendingTask = new CountingTask();
            Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(pendingTask));
            Assert.assertEquals(1, controller.session.pending.size());

            final FiberWalWaitQueue waitQueue = new FiberWalWaitQueue();
            final WaitingTask waitingTask = new WaitingTask(waitQueue);
            Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(waitingTask));
            controller.session.grantNextMatching(waitingTask);
            Assert.assertEquals(1, runtime.drain(1));
            Assert.assertEquals(1, runtime.getParkedFiberCount());

            runtime.beginQuiesce();
            Assert.assertEquals(1, controller.session.beginQuiesceCount);
            Assert.assertTrue(controller.session.pending.isEmpty());
            Assert.assertTrue(controller.session.routes.contains(FiberDispatchRoute.SHUTDOWN_CLEANUP));
            drainUntilClosed(runtime);
            runtime.closeAfterDrained();

            Assert.assertEquals(0, pendingTask.runCount);
            Assert.assertTrue(pendingTask.isCancelled());
            Assert.assertTrue(waitingTask.isDone());
        });
    }

    @Test
    public void testRequestEpochRejectsDuplicateAndLateGrant() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final TestController controller = new TestController();
            final FiberRuntime runtime = newRuntime(1, controller);
            final CountingTask task = new CountingTask();

            Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(task));
            final Pending first = controller.session.removeNext();
            Assert.assertTrue(first.request.grant(first.dispatchEpoch, controller.ticket));
            Assert.assertFalse(first.request.grant(first.dispatchEpoch, controller.ticket));
            Assert.assertEquals(1, runtime.drain(1));
            Assert.assertNull(first.request.getRoute());
            Assert.assertNull(first.request.getTask());
            Assert.assertEquals(-1, first.request.getTaskIncarnation());

            task.reopen();
            Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(task));
            final Pending second = controller.session.removeNext();
            Assert.assertSame(first.request, second.request);
            Assert.assertNotEquals(first.dispatchEpoch, second.dispatchEpoch);
            Assert.assertFalse(first.request.grant(first.dispatchEpoch, controller.ticket));
            Assert.assertTrue(second.request.grant(second.dispatchEpoch, controller.ticket));
            Assert.assertEquals(1, runtime.drain(1));
            Assert.assertEquals(2, task.runCount);

            close(runtime);
        });
    }

    private static void close(FiberRuntime runtime) {
        close(runtime, 0);
    }

    private static void close(FiberRuntime runtime, long expectedInlineSuspendViolationCount) {
        runtime.beginQuiesce();
        drainUntilClosed(runtime, expectedInlineSuspendViolationCount);
        runtime.closeAfterDrained();
    }

    private static void drainUntilClosed(FiberRuntime runtime) {
        drainUntilClosed(runtime, 0);
    }

    private static void drainUntilClosed(FiberRuntime runtime, long expectedInlineSuspendViolationCount) {
        final long deadline = System.nanoTime() + 5_000_000_000L;
        while (runtime.state() != FiberRuntimeState.CLOSED && System.nanoTime() < deadline) {
            runtime.drain(64);
        }
        Assert.assertTrue(runtime.awaitClosed(deadline));
        Assert.assertEquals(expectedInlineSuspendViolationCount, runtime.getInlineSuspendViolationCount());
    }

    private static FiberRuntime newRuntime(int maxLiveFiberCount, FiberDispatchController controller) {
        return new FiberRuntime(
                maxLiveFiberCount,
                maxLiveFiberCount,
                64,
                0,
                controller,
                FiberWakeSink.NO_OP
        );
    }

    private static class CountingTask extends FiberTask {
        private Throwable error;
        private int runCount;

        @Override
        protected void onError(Throwable th) {
            error = th;
        }

        @Override
        protected boolean runStep() {
            runCount++;
            return true;
        }
    }

    private static class ContextYieldTask extends FiberTask {
        private boolean observedNewContext;
        private boolean observedRestoredContext;

        @Override
        protected boolean runStep() {
            Assert.assertNull(Fiber.getDispatchContext());
            Assert.assertTrue(Fiber.yieldForDispatch(TestDispatchContext.INSTANCE));
            observedNewContext = Fiber.getDispatchContext() == TestDispatchContext.INSTANCE;
            Assert.assertTrue(Fiber.yieldForDispatch(null));
            observedRestoredContext = Fiber.getDispatchContext() == null;
            return true;
        }
    }

    private static class DispatchYieldTask extends FiberTask {
        private int resumeCount;
        private int runCount;

        @Override
        protected boolean runStep() {
            runCount++;
            Assert.assertTrue(Fiber.yieldForDispatch());
            resumeCount++;
            return true;
        }
    }

    private static class FailingTicket extends TestTicket {
        private final RuntimeException failure;

        private FailingTicket(RuntimeException failure) {
            this.failure = failure;
        }

        @Override
        public void onMount(FiberDispatchRequest request) {
            super.onMount(request);
            throw failure;
        }
    }

    private static class InitialContextTask extends FiberTask {
        private boolean observedInitialContext;

        @Override
        protected boolean runStep() {
            observedInitialContext = Fiber.getDispatchContext() == TestDispatchContext.INSTANCE;
            return true;
        }
    }

    private static class Pending {
        private final long dispatchEpoch;
        private final FiberDispatchRequest request;
        private final FiberDispatchRoute route;
        private final FiberTask task;

        private Pending(FiberDispatchRequest request) {
            this.dispatchEpoch = request.getDispatchEpoch();
            this.request = request;
            this.route = Objects.requireNonNull(request.getRoute());
            this.task = request.getTask();
        }
    }

    private enum ParallelDispatchContext implements FiberDispatchContext {
        CONTROL {
            @Override
            public FiberDispatchContext getParallelDispatchContext() {
                return PARALLEL;
            }
        },
        PARALLEL
    }

    private static class ParallelContextTask extends FiberTask {
        private FiberDispatchContext capturedContext;

        @Override
        protected boolean runStep() {
            capturedContext = Fiber.captureParallelDispatchContext();
            return true;
        }
    }

    private static class PinnedDispatchYieldTask extends FiberTask {
        private boolean refusalObserved;
        private boolean resumedAfterRefusal;

        @Override
        protected boolean runStep() {
            Continuation.pin();
            try {
                Assert.assertFalse(Fiber.yieldForDispatch(TestDispatchContext.INSTANCE));
                refusalObserved = true;
                Assert.assertNull(Fiber.getDispatchContext());
            } finally {
                Continuation.unpin();
            }
            Assert.assertTrue(Fiber.yieldForDispatch());
            resumedAfterRefusal = true;
            return true;
        }
    }

    private enum TestDispatchContext implements FiberDispatchContext {
        INSTANCE
    }

    private static class TestController implements FiberDispatchController {
        private TestSession session;
        private final TestTicket ticket;

        private TestController() {
            this(new TestTicket());
        }

        private TestController(TestTicket ticket) {
            this.ticket = ticket;
        }

        @Override
        public FiberDispatchSession openSession(FiberRuntime runtime) {
            if (session != null) {
                throw new IllegalStateException("test controller supports one runtime");
            }
            return session = new TestSession(ticket);
        }
    }

    private static class TestSession implements FiberDispatchSession {
        private boolean allowDirect;
        private int beginQuiesceCount;
        private boolean isDraining;
        private final ArrayDeque<Pending> pending = new ArrayDeque<>();
        private RuntimeException requestFailure;
        private final List<FiberDispatchRoute> routes = new ArrayList<>();
        private final TestTicket ticket;

        private TestSession(TestTicket ticket) {
            this.ticket = ticket;
        }

        @Override
        public synchronized void beginQuiesce() {
            beginQuiesceCount++;
            isDraining = true;
            grantAll();
        }

        private void grantAll() {
            Pending pending;
            while ((pending = this.pending.poll()) != null) {
                Assert.assertTrue(pending.request.grant(pending.dispatchEpoch, ticket));
            }
        }

        private synchronized void grantNext() {
            final Pending pending = removeNext();
            Assert.assertTrue(pending.request.grant(pending.dispatchEpoch, ticket));
        }

        private synchronized void grantNextMatching(FiberTask task) {
            final int pendingCount = pending.size();
            for (int i = 0; i < pendingCount; i++) {
                final Pending candidate = pending.remove();
                if (candidate.task == task) {
                    Assert.assertTrue(candidate.request.grant(candidate.dispatchEpoch, ticket));
                    return;
                }
                pending.add(candidate);
            }
            Assert.fail("matching dispatch request not found");
        }

        @Override
        public synchronized boolean isQuiesced() {
            return isDraining && pending.isEmpty();
        }

        private synchronized FiberDispatchRoute peekRoute() {
            return pending.element().route;
        }

        @Override
        public synchronized void progressQuiesce() {
            grantAll();
        }

        private synchronized Pending removeNext() {
            return pending.remove();
        }

        @Override
        public synchronized void requestDispatch(FiberDispatchRequest request) {
            routes.add(Objects.requireNonNull(request.getRoute()));
            if (requestFailure != null) {
                throw requestFailure;
            }
            final Pending pending = new Pending(request);
            if (isDraining) {
                Assert.assertTrue(request.grant(pending.dispatchEpoch, ticket));
            } else {
                this.pending.add(pending);
            }
        }

        @Override
        public synchronized FiberDispatchTicket tryDispatchDirect(FiberDispatchRequest request) {
            return allowDirect ? ticket : null;
        }
    }

    private static class TestTicket implements FiberDispatchTicket {
        private final List<FiberDispatchContext> mountContexts = new ArrayList<>();
        protected int mountCount;
        protected int unmountCount;
        protected boolean wasMounted;

        @Override
        public void onMount(FiberDispatchRequest request) {
            mountCount++;
            mountContexts.add(request.getDispatchContext());
        }

        @Override
        public void onUnmount(FiberDispatchRequest request, boolean wasMounted) {
            unmountCount++;
            this.wasMounted = wasMounted;
        }
    }

    private static class WaitingTask extends FiberTask {
        private final FiberWalWaitQueue waitQueue;

        private WaitingTask(FiberWalWaitQueue waitQueue) {
            this.waitQueue = waitQueue;
        }

        @Override
        protected boolean runStep() {
            final Fiber fiber = Objects.requireNonNull(Fiber.current());
            final FiberWaitCoordinator coordinator = fiber.getWaitCoordinator();
            final long token = fiber.beginWaitBuild(1);
            FiberWalWaitRegistration registration = null;
            try {
                registration = coordinator.acquireWal(token, 1);
                if (registration.register(waitQueue) != SourceRegistrationResult.ACCEPTED) {
                    throw new IllegalStateException("wait registration failed");
                }
                final int reason = fiber.suspendWait(token);
                registration.cancel();
                if (reason != FiberWaitCoordinator.REASON_WAL) {
                    throw new IllegalStateException("unexpected wait reason");
                }
                return true;
            } catch (RuntimeException | Error th) {
                if (registration != null) {
                    registration.cancel();
                }
                coordinator.abort(token);
                coordinator.consume(token);
                throw th;
            }
        }
    }
}
