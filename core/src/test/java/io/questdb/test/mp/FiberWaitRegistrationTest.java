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

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.continuation.FiberCancellationSignal;
import io.questdb.mp.continuation.FiberCancellationWaitRegistration;
import io.questdb.mp.continuation.FiberEventWaitQueue;
import io.questdb.mp.continuation.FiberEventWaitRegistration;
import io.questdb.mp.continuation.FiberSlotWaitQueue;
import io.questdb.mp.continuation.FiberSlotWaitRegistration;
import io.questdb.mp.continuation.FiberTimerWaitRegistration;
import io.questdb.mp.continuation.FiberWaitCoordinator;
import io.questdb.mp.continuation.FiberWalWaitQueue;
import io.questdb.mp.continuation.FiberWalWaitRegistration;
import io.questdb.mp.continuation.SourceRegistrationResult;
import io.questdb.mp.continuation.TimerShards;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.datetime.millitime.MillisecondClockImpl;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.lang.management.ManagementFactory;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class FiberWaitRegistrationTest {
    private static final Log LOG = LogFactory.getLog(FiberWaitRegistrationTest.class);

    @Test
    public void testCancellationEarlyFireAndReuse() {
        final FiberCancellationSignal cancellationSignal = new FiberCancellationSignal();
        final TestTarget target = new TestTarget();
        final FiberWaitCoordinator coordinator = new FiberWaitCoordinator(target);

        cancellationSignal.cancel();
        final long token = coordinator.beginBuild(1);
        final FiberCancellationWaitRegistration registration = coordinator.acquireCancellation(token, cancellationSignal);
        Assert.assertSame(SourceRegistrationResult.ACCEPTED, registration.register());
        Assert.assertTrue(coordinator.tryAcceptSource(token));
        Assert.assertTrue(coordinator.seal(token));
        Assert.assertFalse(coordinator.hasInFlightRegistrations());
        Assert.assertEquals(FiberWaitCoordinator.REASON_CANCEL, coordinator.consume(token));

        cancellationSignal.reset();
        final long nextToken = coordinator.beginBuild(1);
        final FiberCancellationWaitRegistration next = coordinator.acquireCancellation(nextToken, cancellationSignal);
        Assert.assertSame(registration, next);
        Assert.assertSame(SourceRegistrationResult.ACCEPTED, next.register());
        Assert.assertTrue(coordinator.tryAcceptSource(nextToken));
        Assert.assertTrue(coordinator.seal(nextToken));
        cancellationSignal.cancel();
        Assert.assertFalse(coordinator.hasInFlightRegistrations());
        Assert.assertEquals(FiberWaitCoordinator.REASON_CANCEL, coordinator.consume(nextToken));
        Assert.assertEquals(2, target.fireCount);
    }

    @Test
    public void testCancellationNullSourceRejectedBeforeAcquire() {
        final TestTarget target = new TestTarget();
        final FiberWaitCoordinator coordinator = new FiberWaitCoordinator(target);
        final long token = coordinator.beginBuild(1);

        try {
            coordinator.acquireCancellation(token, null);
            Assert.fail("expected null cancellation signal rejection");
        } catch (IllegalArgumentException expected) {
            Assert.assertEquals("cancellation signal must not be null", expected.getMessage());
        }

        Assert.assertFalse(coordinator.hasInFlightRegistrations());
        Assert.assertEquals(0, target.acquiredRegistrationCount);
        Assert.assertTrue(coordinator.abort(token));
        Assert.assertEquals(FiberWaitCoordinator.REASON_ABORTED, coordinator.consume(token));
    }

    @Test
    public void testCancellationRegisterFailureReleasesHolder() {
        final FiberCancellationSignal cancellationSignal = new FiberCancellationSignal(() -> {
            throw new IllegalStateException("test cancellation registration failure");
        });
        final TestTarget target = new TestTarget();
        final FiberWaitCoordinator coordinator = new FiberWaitCoordinator(target);
        final long token = coordinator.beginBuild(1);
        final FiberCancellationWaitRegistration registration = coordinator.acquireCancellation(token, cancellationSignal);

        try {
            registration.register();
            Assert.fail("expected cancellation registration failure");
        } catch (IllegalStateException expected) {
            Assert.assertEquals("test cancellation registration failure", expected.getMessage());
        }

        Assert.assertFalse(coordinator.hasInFlightRegistrations());
        Assert.assertEquals(1, target.acquiredRegistrationCount);
        Assert.assertEquals(1, target.releasedRegistrationCount);
        Assert.assertTrue(coordinator.abort(token));
        Assert.assertEquals(FiberWaitCoordinator.REASON_ABORTED, coordinator.consume(token));

        final long nextToken = coordinator.beginBuild(1);
        final FiberCancellationWaitRegistration next = coordinator.acquireCancellation(nextToken, cancellationSignal);
        Assert.assertSame(registration, next);
        Assert.assertTrue(next.cancel());
        Assert.assertTrue(coordinator.abort(nextToken));
        Assert.assertEquals(FiberWaitCoordinator.REASON_ABORTED, coordinator.consume(nextToken));
    }

    @Test
    public void testCancellationResetWaitsForDetachedCallbacks() throws Exception {
        final FiberCancellationSignal cancellationSignal = new FiberCancellationSignal();
        final CountDownLatch callbackEntered = new CountDownLatch(1);
        final CountDownLatch releaseCallback = new CountDownLatch(1);
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final FiberWaitCoordinator coordinator = new FiberWaitCoordinator(new FiberWaitCoordinator.Target() {
            @Override
            public void abortWait(long token) {
            }

            @Override
            public boolean fireWait(long token, int reason) {
                callbackEntered.countDown();
                try {
                    if (!releaseCallback.await(10, TimeUnit.SECONDS)) {
                        throw new AssertionError("timed out waiting to release cancellation callback");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
                return true;
            }
        });
        final long token = coordinator.beginBuild(1);
        final FiberCancellationWaitRegistration registration = coordinator.acquireCancellation(
                token,
                cancellationSignal
        );
        Assert.assertSame(SourceRegistrationResult.ACCEPTED, registration.register());
        Assert.assertTrue(coordinator.tryAcceptSource(token));
        Assert.assertTrue(coordinator.seal(token));

        final Thread cancelThread = new Thread(() -> {
            try {
                cancellationSignal.cancel();
            } catch (Throwable th) {
                failure.set(th);
            }
        });
        cancelThread.start();
        Assert.assertTrue(callbackEntered.await(10, TimeUnit.SECONDS));
        try {
            cancellationSignal.reset();
            Assert.fail("expected reset to reject a detached callback");
        } catch (IllegalStateException expected) {
            Assert.assertEquals("cannot reset cancellation signal with an active wait", expected.getMessage());
        }
        releaseCallback.countDown();
        cancelThread.join(10_000);
        Assert.assertFalse(cancelThread.isAlive());
        Assert.assertNull(failure.get());
        Assert.assertEquals(FiberWaitCoordinator.REASON_CANCEL, coordinator.consume(token));

        cancellationSignal.reset();
        Assert.assertFalse(cancellationSignal.get());
    }

    @Test
    public void testCancellationRegistrationCanBeRemoved() {
        final FiberCancellationSignal cancellationSignal = new FiberCancellationSignal();
        final TestTarget target = new TestTarget();
        final FiberWaitCoordinator coordinator = new FiberWaitCoordinator(target);
        final long token = coordinator.beginBuild(1);
        final FiberCancellationWaitRegistration registration = coordinator.acquireCancellation(token, cancellationSignal);
        Assert.assertSame(SourceRegistrationResult.ACCEPTED, registration.register());
        Assert.assertTrue(coordinator.tryAcceptSource(token));
        Assert.assertTrue(coordinator.seal(token));
        Assert.assertTrue(registration.cancel());
        Assert.assertFalse(coordinator.hasInFlightRegistrations());
        cancellationSignal.cancel();
        Assert.assertEquals(0, target.fireCount);
        Assert.assertTrue(coordinator.abort(token));
        Assert.assertEquals(FiberWaitCoordinator.REASON_ABORTED, coordinator.consume(token));
    }

    @Test
    public void testCancellationWakesAllWaiters() {
        final FiberCancellationSignal cancellationSignal = new FiberCancellationSignal();
        final ObjList<FiberWaitCoordinator> coordinators = new ObjList<>();
        final ObjList<TestTarget> targets = new ObjList<>();
        final LongList tokens = new LongList();

        for (int i = 0; i < 32; i++) {
            final TestTarget target = new TestTarget();
            final FiberWaitCoordinator coordinator = new FiberWaitCoordinator(target);
            final long token = coordinator.beginBuild(1);
            final FiberCancellationWaitRegistration registration = coordinator.acquireCancellation(
                    token,
                    cancellationSignal
            );
            Assert.assertSame(SourceRegistrationResult.ACCEPTED, registration.register());
            Assert.assertTrue(coordinator.tryAcceptSource(token));
            Assert.assertTrue(coordinator.seal(token));
            coordinators.add(coordinator);
            targets.add(target);
            tokens.add(token);
        }

        cancellationSignal.cancel();

        for (int i = 0; i < coordinators.size(); i++) {
            final FiberWaitCoordinator coordinator = coordinators.getQuick(i);
            Assert.assertFalse(coordinator.hasInFlightRegistrations());
            Assert.assertEquals(FiberWaitCoordinator.REASON_CANCEL, coordinator.consume(tokens.getQuick(i)));
            Assert.assertEquals(1, targets.getQuick(i).fireCount);
        }
    }

    @Test
    public void testCancellationWakesRemainingWaitersAfterCallbackFailure() {
        final FiberCancellationSignal cancellationSignal = new FiberCancellationSignal();
        final TestTarget firstTarget = new TestTarget();
        final TestTarget secondTarget = new TestTarget();
        final TestTarget thirdTarget = new TestTarget();
        final TestRegistrationException firstError = new TestRegistrationException("first cancellation failure");
        final TestRegistrationException secondError = new TestRegistrationException("second cancellation failure");
        thirdTarget.fireException = firstError;
        secondTarget.fireException = secondError;
        final FiberWaitCoordinator first = new FiberWaitCoordinator(firstTarget);
        final FiberWaitCoordinator second = new FiberWaitCoordinator(secondTarget);
        final FiberWaitCoordinator third = new FiberWaitCoordinator(thirdTarget);
        final long firstToken = first.beginBuild(1);
        final long secondToken = second.beginBuild(1);
        final long thirdToken = third.beginBuild(1);
        final FiberCancellationWaitRegistration firstRegistration = first.acquireCancellation(
                firstToken,
                cancellationSignal
        );
        final FiberCancellationWaitRegistration secondRegistration = second.acquireCancellation(
                secondToken,
                cancellationSignal
        );
        final FiberCancellationWaitRegistration thirdRegistration = third.acquireCancellation(
                thirdToken,
                cancellationSignal
        );
        Assert.assertSame(SourceRegistrationResult.ACCEPTED, firstRegistration.register());
        Assert.assertSame(SourceRegistrationResult.ACCEPTED, secondRegistration.register());
        Assert.assertSame(SourceRegistrationResult.ACCEPTED, thirdRegistration.register());
        Assert.assertTrue(first.tryAcceptSource(firstToken));
        Assert.assertTrue(second.tryAcceptSource(secondToken));
        Assert.assertTrue(third.tryAcceptSource(thirdToken));
        Assert.assertTrue(first.seal(firstToken));
        Assert.assertTrue(second.seal(secondToken));
        Assert.assertTrue(third.seal(thirdToken));

        try {
            cancellationSignal.cancel();
            Assert.fail("expected cancellation failure");
        } catch (TestRegistrationException expected) {
            Assert.assertSame(firstError, expected);
            Assert.assertArrayEquals(new Throwable[]{secondError}, expected.getSuppressed());
        }

        Assert.assertFalse(first.hasInFlightRegistrations());
        Assert.assertFalse(second.hasInFlightRegistrations());
        Assert.assertFalse(third.hasInFlightRegistrations());
        Assert.assertEquals(FiberWaitCoordinator.REASON_CANCEL, first.consume(firstToken));
        Assert.assertEquals(FiberWaitCoordinator.REASON_CANCEL, second.consume(secondToken));
        Assert.assertEquals(FiberWaitCoordinator.REASON_CANCEL, third.consume(thirdToken));
    }

    @Test
    public void testEventFireAllPreservesReasonAndLeavesQueueOpen() {
        final FiberEventWaitQueue queue = new FiberEventWaitQueue(FiberWaitCoordinator.REASON_WRITER);
        final ObjList<FiberWaitCoordinator> coordinators = new ObjList<>();
        final ObjList<TestTarget> targets = new ObjList<>();
        final LongList tokens = new LongList();

        for (int i = 0; i < 3; i++) {
            final TestTarget target = new TestTarget();
            final FiberWaitCoordinator coordinator = new FiberWaitCoordinator(target);
            final long token = coordinator.beginBuild(1);
            final FiberEventWaitRegistration registration = coordinator.acquireEvent(token);
            Assert.assertSame(SourceRegistrationResult.ACCEPTED, registration.register(queue));
            Assert.assertTrue(coordinator.tryAcceptSource(token));
            Assert.assertTrue(coordinator.seal(token));
            coordinators.add(coordinator);
            targets.add(target);
            tokens.add(token);
        }

        queue.fireAll();

        for (int i = 0; i < coordinators.size(); i++) {
            final FiberWaitCoordinator coordinator = coordinators.getQuick(i);
            Assert.assertFalse(coordinator.hasInFlightRegistrations());
            Assert.assertEquals(FiberWaitCoordinator.REASON_WRITER, coordinator.consume(tokens.getQuick(i)));
            Assert.assertEquals(1, targets.getQuick(i).fireCount);
        }

        final TestTarget nextTarget = new TestTarget();
        final FiberWaitCoordinator nextCoordinator = new FiberWaitCoordinator(nextTarget);
        final long nextToken = nextCoordinator.beginBuild(1);
        final FiberEventWaitRegistration nextRegistration = nextCoordinator.acquireEvent(nextToken);
        Assert.assertSame(SourceRegistrationResult.ACCEPTED, nextRegistration.register(queue));
        Assert.assertTrue(nextCoordinator.tryAcceptSource(nextToken));
        Assert.assertTrue(nextCoordinator.seal(nextToken));
        queue.fire();
        Assert.assertEquals(FiberWaitCoordinator.REASON_WRITER, nextCoordinator.consume(nextToken));
        Assert.assertEquals(1, nextTarget.fireCount);
    }

    @Test
    public void testEventShutdownWakesRemainingWaitersAfterCallbackFailure() {
        final FiberEventWaitQueue queue = new FiberEventWaitQueue(FiberWaitCoordinator.REASON_CAPACITY);
        final TestTarget firstTarget = new TestTarget();
        final TestTarget secondTarget = new TestTarget();
        final TestTarget thirdTarget = new TestTarget();
        final TestRegistrationException firstError = new TestRegistrationException("first event failure");
        final TestRegistrationException secondError = new TestRegistrationException("second event failure");
        firstTarget.fireException = firstError;
        secondTarget.fireException = secondError;
        final FiberWaitCoordinator first = new FiberWaitCoordinator(firstTarget);
        final FiberWaitCoordinator second = new FiberWaitCoordinator(secondTarget);
        final FiberWaitCoordinator third = new FiberWaitCoordinator(thirdTarget);
        final long firstToken = first.beginBuild(1);
        final long secondToken = second.beginBuild(1);
        final long thirdToken = third.beginBuild(1);
        final FiberEventWaitRegistration firstRegistration = first.acquireEvent(firstToken);
        final FiberEventWaitRegistration secondRegistration = second.acquireEvent(secondToken);
        final FiberEventWaitRegistration thirdRegistration = third.acquireEvent(thirdToken);
        Assert.assertSame(SourceRegistrationResult.ACCEPTED, firstRegistration.register(queue));
        Assert.assertSame(SourceRegistrationResult.ACCEPTED, secondRegistration.register(queue));
        Assert.assertSame(SourceRegistrationResult.ACCEPTED, thirdRegistration.register(queue));
        Assert.assertTrue(first.tryAcceptSource(firstToken));
        Assert.assertTrue(second.tryAcceptSource(secondToken));
        Assert.assertTrue(third.tryAcceptSource(thirdToken));
        Assert.assertTrue(first.seal(firstToken));
        Assert.assertTrue(second.seal(secondToken));
        Assert.assertTrue(third.seal(thirdToken));

        try {
            queue.shutdown();
            Assert.fail("expected event failure");
        } catch (TestRegistrationException expected) {
            Assert.assertSame(firstError, expected);
            Assert.assertArrayEquals(new Throwable[]{secondError}, expected.getSuppressed());
        }

        Assert.assertFalse(first.hasInFlightRegistrations());
        Assert.assertFalse(second.hasInFlightRegistrations());
        Assert.assertFalse(third.hasInFlightRegistrations());
        Assert.assertEquals(FiberWaitCoordinator.REASON_SHUTDOWN, first.consume(firstToken));
        Assert.assertEquals(FiberWaitCoordinator.REASON_SHUTDOWN, second.consume(secondToken));
        Assert.assertEquals(FiberWaitCoordinator.REASON_SHUTDOWN, third.consume(thirdToken));
    }

    @Test
    public void testShutdownCancelsAllActiveRegistrations() {
        final FiberWalWaitQueue firstQueue = new FiberWalWaitQueue();
        final FiberWalWaitQueue secondQueue = new FiberWalWaitQueue();
        final TestTarget target = new TestTarget();
        final FiberWaitCoordinator coordinator = new FiberWaitCoordinator(target);
        final long token = coordinator.beginBuild(2);
        final FiberWalWaitRegistration first = coordinator.acquireWal(token, 1);
        final FiberWalWaitRegistration second = coordinator.acquireWal(token, 1);
        Assert.assertSame(SourceRegistrationResult.ACCEPTED, first.register(firstQueue));
        Assert.assertTrue(coordinator.tryAcceptSource(token));
        Assert.assertSame(SourceRegistrationResult.ACCEPTED, second.register(secondQueue));
        Assert.assertTrue(coordinator.tryAcceptSource(token));
        Assert.assertTrue(coordinator.seal(token));

        coordinator.shutdown();

        Assert.assertFalse(coordinator.hasInFlightRegistrations());
        Assert.assertEquals(0, firstQueue.size());
        Assert.assertEquals(0, secondQueue.size());
        Assert.assertTrue(coordinator.isFired(token));
        Assert.assertEquals(FiberWaitCoordinator.REASON_SHUTDOWN, coordinator.consume(token));
    }

    @Test
    public void testSlotCallbackFailureReturnsGrantedSlot() {
        final AtomicInteger releasedSlot = new AtomicInteger(-1);
        final FiberSlotWaitQueue queue = new FiberSlotWaitQueue(releasedSlot::set);
        final TestTarget target = new TestTarget();
        final FiberWaitCoordinator coordinator = new FiberWaitCoordinator(target);
        final long token = coordinator.beginBuild(1);
        final FiberSlotWaitRegistration registration = coordinator.acquireSlot(token);
        Assert.assertSame(SourceRegistrationResult.ACCEPTED, registration.register(queue));
        Assert.assertTrue(coordinator.tryAcceptSource(token));
        Assert.assertTrue(coordinator.seal(token));
        target.fireException = new TestRegistrationException("test slot callback failure");

        try {
            queue.transfer(7);
            Assert.fail("expected slot callback failure");
        } catch (TestRegistrationException expected) {
            Assert.assertEquals("test slot callback failure", expected.getMessage());
        }

        Assert.assertEquals(7, releasedSlot.get());
        Assert.assertFalse(coordinator.hasInFlightRegistrations());
        Assert.assertEquals(FiberWaitCoordinator.REASON_SLOT, coordinator.consume(token));
    }

    @Test
    public void testSteadyStateWaitRegistrationReuseAllocatesNoJavaHeap() {
        final java.lang.management.ThreadMXBean mxBean = ManagementFactory.getThreadMXBean();
        Assume.assumeTrue(mxBean instanceof com.sun.management.ThreadMXBean);
        final com.sun.management.ThreadMXBean threadMXBean = (com.sun.management.ThreadMXBean) mxBean;
        Assume.assumeTrue(threadMXBean.isThreadAllocatedMemorySupported());
        if (!threadMXBean.isThreadAllocatedMemoryEnabled()) {
            threadMXBean.setThreadAllocatedMemoryEnabled(true);
        }

        final TimerShards timerShards = new TimerShards(1, "test-timer", LOG);
        timerShards.start();
        final FiberCancellationSignal cancellationSignal = new FiberCancellationSignal();
        final FiberWaitCoordinator coordinator = new FiberWaitCoordinator(new TestTarget());
        final FiberWalWaitQueue walWaitQueue = new FiberWalWaitQueue();
        try {
            for (int i = 0; i < 10_000; i++) {
                runRegistrationCycle(cancellationSignal, coordinator, timerShards, walWaitQueue);
            }

            long minAllocatedBytes = Long.MAX_VALUE;
            for (int round = 0; round < 5; round++) {
                final long allocatedBefore = threadMXBean.getCurrentThreadAllocatedBytes();
                for (int i = 0; i < 100_000; i++) {
                    runRegistrationCycle(cancellationSignal, coordinator, timerShards, walWaitQueue);
                }
                minAllocatedBytes = Math.min(
                        minAllocatedBytes,
                        threadMXBean.getCurrentThreadAllocatedBytes() - allocatedBefore
                );
            }

            Assert.assertEquals(0, minAllocatedBytes);
        } finally {
            timerShards.shutdown();
        }
    }

    @Test
    public void testTimerCancellationReusesHolder() {
        TimerShards timerShards = new TimerShards(1, "test-timer", LOG);
        timerShards.start();
        try {
            TestTarget target = new TestTarget();
            FiberWaitCoordinator coordinator = new FiberWaitCoordinator(target);
            long token = coordinator.beginBuild(1);
            FiberTimerWaitRegistration registration = coordinator.acquireTimer(
                    token,
                    timerShards,
                    MillisecondClockImpl.INSTANCE,
                    60_000
            );
            Assert.assertTrue(coordinator.hasInFlightRegistrations());
            Assert.assertEquals(1, target.acquiredRegistrationCount);
            Assert.assertEquals(0, target.releasedRegistrationCount);
            Assert.assertSame(SourceRegistrationResult.ACCEPTED, registration.register());
            Assert.assertTrue(coordinator.tryAcceptSource(token));
            Assert.assertTrue(coordinator.seal(token));
            Assert.assertTrue(registration.cancel());
            Assert.assertFalse(coordinator.hasInFlightRegistrations());
            Assert.assertEquals(1, target.releasedRegistrationCount);
            Assert.assertTrue(coordinator.abort(token));
            Assert.assertEquals(FiberWaitCoordinator.REASON_ABORTED, coordinator.consume(token));

            long nextToken = coordinator.beginBuild(1);
            FiberTimerWaitRegistration next = coordinator.acquireTimer(
                    nextToken,
                    timerShards,
                    MillisecondClockImpl.INSTANCE,
                    60_000
            );
            Assert.assertSame(registration, next);
            Assert.assertEquals(2, target.acquiredRegistrationCount);
            Assert.assertSame(SourceRegistrationResult.ACCEPTED, next.register());
            Assert.assertTrue(next.cancel());
            Assert.assertEquals(2, target.releasedRegistrationCount);
            Assert.assertTrue(coordinator.abort(nextToken));
            Assert.assertEquals(FiberWaitCoordinator.REASON_ABORTED, coordinator.consume(nextToken));
        } finally {
            timerShards.shutdown();
        }
    }

    @Test
    public void testTimerNullSourcesRejectedBeforeAcquire() {
        final TimerShards timerShards = new TimerShards(1, "test-timer", LOG);
        final TestTarget target = new TestTarget();
        final FiberWaitCoordinator coordinator = new FiberWaitCoordinator(target);
        final long token = coordinator.beginBuild(1);

        try {
            coordinator.acquireTimer(token, null, MillisecondClockImpl.INSTANCE, 1);
            Assert.fail("expected null timer shards rejection");
        } catch (IllegalArgumentException expected) {
            Assert.assertEquals("timer shards must not be null", expected.getMessage());
        }
        try {
            coordinator.acquireTimer(token, timerShards, null, 1);
            Assert.fail("expected null timer clock rejection");
        } catch (IllegalArgumentException expected) {
            Assert.assertEquals("timer clock must not be null", expected.getMessage());
        }

        Assert.assertFalse(coordinator.hasInFlightRegistrations());
        Assert.assertEquals(0, target.acquiredRegistrationCount);
        Assert.assertTrue(coordinator.abort(token));
        Assert.assertEquals(FiberWaitCoordinator.REASON_ABORTED, coordinator.consume(token));
    }

    @Test
    public void testTimerRegistrationCleanupFailureDiscardsHolder() {
        final TimerShards timerShards = new TimerShards(1, "test-timer", LOG);
        timerShards.start();
        final FiberWaitCoordinator blockerCoordinator = registerTimerBlockers(timerShards);
        final long blockerToken = blockerCoordinator.currentToken();
        try {
            final TestTarget target = new TestTarget();
            final FiberWaitCoordinator coordinator = new FiberWaitCoordinator(target);
            final long token = coordinator.beginBuild(1);
            final FiberTimerWaitRegistration registration = coordinator.acquireTimer(
                    token,
                    timerShards,
                    new FailOnSecondHeapComparisonClock(),
                    60_000
            );
            final TestCleanupException cleanupError = new TestCleanupException();
            registration.setBeforeRegisterCleanupForTesting(() -> {
                throw cleanupError;
            });
            try {
                registration.register();
                Assert.fail("expected timer registration failure");
            } catch (TestRegistrationException expected) {
                Assert.assertEquals("test timer registration failure", expected.getMessage());
                Assert.assertArrayEquals(new Throwable[]{cleanupError}, expected.getSuppressed());
            } finally {
                registration.setBeforeRegisterCleanupForTesting(null);
            }

            Assert.assertFalse(coordinator.hasInFlightRegistrations());
            Assert.assertEquals(1, target.acquiredRegistrationCount);
            Assert.assertEquals(1, target.releasedRegistrationCount);
            Assert.assertTrue(coordinator.abort(token));
            Assert.assertEquals(FiberWaitCoordinator.REASON_ABORTED, coordinator.consume(token));

            registration.setClockForTesting(MillisecondClockImpl.INSTANCE);
            Assert.assertSame(SourceRegistrationResult.ACCEPTED, timerShards.register(registration));
            Assert.assertTrue(registration.cancel());
            Assert.assertEquals(3, timerShards.size());

            final long nextToken = coordinator.beginBuild(1);
            final FiberTimerWaitRegistration next = coordinator.acquireTimer(
                    nextToken,
                    timerShards,
                    MillisecondClockImpl.INSTANCE,
                    60_000
            );
            Assert.assertNotSame(registration, next);
            Assert.assertTrue(next.cancel());
            Assert.assertTrue(coordinator.abort(nextToken));
            Assert.assertEquals(FiberWaitCoordinator.REASON_ABORTED, coordinator.consume(nextToken));
        } finally {
            blockerCoordinator.shutdown();
            blockerCoordinator.consume(blockerToken);
            timerShards.shutdown();
        }
    }

    @Test
    public void testTimerRegistrationFailureReleasesHolder() {
        final TimerShards timerShards = new TimerShards(1, "test-timer", LOG);
        timerShards.start();
        final FiberWaitCoordinator blockerCoordinator = registerTimerBlockers(timerShards);
        final long blockerToken = blockerCoordinator.currentToken();
        try {
            final TestTarget target = new TestTarget();
            final FiberWaitCoordinator coordinator = new FiberWaitCoordinator(target);
            final long token = coordinator.beginBuild(1);
            final FiberTimerWaitRegistration registration = coordinator.acquireTimer(
                    token,
                    timerShards,
                    new FailOnSecondHeapComparisonClock(),
                    60_000
            );

            try {
                registration.register();
                Assert.fail("expected timer registration failure");
            } catch (TestRegistrationException expected) {
                Assert.assertEquals("test timer registration failure", expected.getMessage());
            }

            Assert.assertFalse(coordinator.hasInFlightRegistrations());
            Assert.assertEquals(1, target.acquiredRegistrationCount);
            Assert.assertEquals(1, target.releasedRegistrationCount);
            Assert.assertEquals(3, timerShards.size());
            Assert.assertTrue(coordinator.abort(token));
            Assert.assertEquals(FiberWaitCoordinator.REASON_ABORTED, coordinator.consume(token));

            final long nextToken = coordinator.beginBuild(1);
            final FiberTimerWaitRegistration next = coordinator.acquireTimer(
                    nextToken,
                    timerShards,
                    MillisecondClockImpl.INSTANCE,
                    60_000
            );
            Assert.assertSame(registration, next);
            Assert.assertTrue(next.cancel());
            Assert.assertTrue(coordinator.abort(nextToken));
            Assert.assertEquals(FiberWaitCoordinator.REASON_ABORTED, coordinator.consume(nextToken));
        } finally {
            blockerCoordinator.shutdown();
            blockerCoordinator.consume(blockerToken);
            timerShards.shutdown();
        }
    }

    @Test
    public void testTimerShutdownFiresCoordinator() {
        TimerShards timerShards = new TimerShards(1, "test-timer", LOG);
        timerShards.start();
        TestTarget target = new TestTarget();
        FiberWaitCoordinator coordinator = new FiberWaitCoordinator(target);
        long token = coordinator.beginBuild(1);
        FiberTimerWaitRegistration registration = coordinator.acquireTimer(
                token,
                timerShards,
                MillisecondClockImpl.INSTANCE,
                60_000
        );
        Assert.assertSame(SourceRegistrationResult.ACCEPTED, registration.register());
        Assert.assertTrue(coordinator.tryAcceptSource(token));
        Assert.assertTrue(coordinator.seal(token));

        timerShards.shutdown();

        Assert.assertTrue(coordinator.isFired(token));
        Assert.assertFalse(coordinator.hasInFlightRegistrations());
        Assert.assertEquals(FiberWaitCoordinator.REASON_SHUTDOWN, coordinator.consume(token));
        Assert.assertEquals(FiberWaitCoordinator.REASON_SHUTDOWN, target.reason);
    }

    @Test
    public void testWalCancellationUnlinksAndReusesHolder() {
        FiberWalWaitQueue queue = new FiberWalWaitQueue();
        TestTarget target = new TestTarget();
        FiberWaitCoordinator coordinator = new FiberWaitCoordinator(target);
        long token = coordinator.beginBuild(1);
        FiberWalWaitRegistration registration = coordinator.acquireWal(token, 10);
        Assert.assertTrue(coordinator.hasInFlightRegistrations());
        Assert.assertEquals(1, target.acquiredRegistrationCount);
        Assert.assertSame(SourceRegistrationResult.ACCEPTED, registration.register(queue));
        Assert.assertEquals(1, queue.size());
        Assert.assertTrue(coordinator.tryAcceptSource(token));
        Assert.assertTrue(coordinator.seal(token));

        Assert.assertTrue(registration.cancel());
        Assert.assertFalse(coordinator.hasInFlightRegistrations());
        Assert.assertEquals(1, target.releasedRegistrationCount);
        Assert.assertEquals(0, queue.size());
        Assert.assertTrue(coordinator.abort(token));
        Assert.assertEquals(FiberWaitCoordinator.REASON_ABORTED, coordinator.consume(token));

        long nextToken = coordinator.beginBuild(1);
        FiberWalWaitRegistration next = coordinator.acquireWal(nextToken, 20);
        Assert.assertSame(registration, next);
        Assert.assertEquals(2, target.acquiredRegistrationCount);
        Assert.assertTrue(next.cancel());
        Assert.assertEquals(2, target.releasedRegistrationCount);
        Assert.assertTrue(coordinator.abort(nextToken));
        Assert.assertEquals(FiberWaitCoordinator.REASON_ABORTED, coordinator.consume(nextToken));
    }

    @Test
    public void testWalFireDrainsDetachedRegistrationsAfterCallbackFailure() {
        final FiberWalWaitQueue queue = new FiberWalWaitQueue();
        final TestTarget firstTarget = new TestTarget();
        final TestTarget secondTarget = new TestTarget();
        final TestTarget thirdTarget = new TestTarget();
        final TestRegistrationException firstError = new TestRegistrationException("first WAL fire failure");
        final TestRegistrationException secondError = new TestRegistrationException("second WAL fire failure");
        firstTarget.fireException = firstError;
        secondTarget.fireException = secondError;
        final FiberWaitCoordinator first = new FiberWaitCoordinator(firstTarget);
        final FiberWaitCoordinator second = new FiberWaitCoordinator(secondTarget);
        final FiberWaitCoordinator third = new FiberWaitCoordinator(thirdTarget);
        final long firstToken = first.beginBuild(1);
        final long secondToken = second.beginBuild(1);
        final long thirdToken = third.beginBuild(1);
        final FiberWalWaitRegistration firstRegistration = first.acquireWal(firstToken, 1);
        final FiberWalWaitRegistration secondRegistration = second.acquireWal(secondToken, 1);
        final FiberWalWaitRegistration thirdRegistration = third.acquireWal(thirdToken, 1);
        Assert.assertSame(SourceRegistrationResult.ACCEPTED, firstRegistration.register(queue));
        Assert.assertSame(SourceRegistrationResult.ACCEPTED, secondRegistration.register(queue));
        Assert.assertSame(SourceRegistrationResult.ACCEPTED, thirdRegistration.register(queue));
        Assert.assertTrue(first.tryAcceptSource(firstToken));
        Assert.assertTrue(second.tryAcceptSource(secondToken));
        Assert.assertTrue(third.tryAcceptSource(thirdToken));
        Assert.assertTrue(first.seal(firstToken));
        Assert.assertTrue(second.seal(secondToken));
        Assert.assertTrue(third.seal(thirdToken));

        try {
            queue.fire(1, false);
            Assert.fail("expected WAL fire failure");
        } catch (TestRegistrationException expected) {
            Assert.assertSame(firstError, expected);
            Assert.assertArrayEquals(new Throwable[]{secondError}, expected.getSuppressed());
        }

        Assert.assertEquals(0, queue.size());
        Assert.assertFalse(first.hasInFlightRegistrations());
        Assert.assertFalse(second.hasInFlightRegistrations());
        Assert.assertFalse(third.hasInFlightRegistrations());
        Assert.assertFalse(first.isFired(firstToken));
        Assert.assertFalse(second.isFired(secondToken));
        Assert.assertTrue(third.isFired(thirdToken));
        Assert.assertEquals(FiberWaitCoordinator.REASON_WAL, first.consume(firstToken));
        Assert.assertEquals(FiberWaitCoordinator.REASON_WAL, second.consume(secondToken));
        Assert.assertEquals(FiberWaitCoordinator.REASON_WAL, third.consume(thirdToken));
    }

    @Test
    public void testWalQueueFiresOnlyReachedTargets() {
        FiberWalWaitQueue queue = new FiberWalWaitQueue();
        TestTarget firstTarget = new TestTarget();
        TestTarget secondTarget = new TestTarget();
        FiberWaitCoordinator first = new FiberWaitCoordinator(firstTarget);
        FiberWaitCoordinator second = new FiberWaitCoordinator(secondTarget);
        long firstToken = first.beginBuild(1);
        long secondToken = second.beginBuild(1);
        FiberWalWaitRegistration firstRegistration = first.acquireWal(firstToken, 5);
        FiberWalWaitRegistration secondRegistration = second.acquireWal(secondToken, 10);
        Assert.assertSame(SourceRegistrationResult.ACCEPTED, firstRegistration.register(queue));
        Assert.assertSame(SourceRegistrationResult.ACCEPTED, secondRegistration.register(queue));
        Assert.assertTrue(first.tryAcceptSource(firstToken));
        Assert.assertTrue(second.tryAcceptSource(secondToken));
        Assert.assertTrue(first.seal(firstToken));
        Assert.assertTrue(second.seal(secondToken));

        queue.fire(5, false);

        Assert.assertFalse(first.hasInFlightRegistrations());
        Assert.assertTrue(second.hasInFlightRegistrations());
        Assert.assertTrue(first.isFired(firstToken));
        Assert.assertTrue(second.isArmed(secondToken));
        Assert.assertEquals(1, queue.size());

        queue.fire(9, true);

        Assert.assertFalse(second.hasInFlightRegistrations());
        Assert.assertTrue(second.isFired(secondToken));
        Assert.assertEquals(0, queue.size());
    }

    @Test
    public void testWalStaleHolderCannotFireReusedArm() {
        FiberWalWaitQueue queue = new FiberWalWaitQueue();
        TestTarget target = new TestTarget();
        FiberWaitCoordinator coordinator = new FiberWaitCoordinator(target);
        long oldToken = coordinator.beginBuild(2);
        FiberWalWaitRegistration oldRegistration = coordinator.acquireWal(oldToken, 10);
        Assert.assertSame(SourceRegistrationResult.ACCEPTED, oldRegistration.register(queue));
        Assert.assertTrue(coordinator.tryAcceptSource(oldToken));
        Assert.assertTrue(coordinator.tryAcceptSource(oldToken));
        Assert.assertTrue(coordinator.seal(oldToken));
        Assert.assertTrue(coordinator.fire(oldToken, FiberWaitCoordinator.REASON_TIMER));
        Assert.assertEquals(FiberWaitCoordinator.REASON_TIMER, coordinator.consume(oldToken));

        long token = coordinator.beginBuild(1);
        FiberWalWaitRegistration registration = coordinator.acquireWal(token, 10);
        Assert.assertNotSame(oldRegistration, registration);
        Assert.assertSame(SourceRegistrationResult.ACCEPTED, registration.register(queue));
        Assert.assertTrue(coordinator.tryAcceptSource(token));
        Assert.assertTrue(coordinator.seal(token));

        queue.fire(10, false);

        Assert.assertTrue(coordinator.isFired(token));
        Assert.assertEquals(2, target.fireCount);
        Assert.assertFalse(coordinator.hasInFlightRegistrations());
        Assert.assertEquals(FiberWaitCoordinator.REASON_WAL, coordinator.consume(token));
    }

    private static FiberWaitCoordinator registerTimerBlockers(TimerShards timerShards) {
        final FiberWaitCoordinator coordinator = new FiberWaitCoordinator(new TestTarget());
        final long token = coordinator.beginBuild(3);
        for (int i = 0; i < 3; i++) {
            final FiberTimerWaitRegistration registration = coordinator.acquireTimer(
                    token,
                    timerShards,
                    MillisecondClockImpl.INSTANCE,
                    120_000
            );
            Assert.assertSame(SourceRegistrationResult.ACCEPTED, registration.register());
            Assert.assertTrue(coordinator.tryAcceptSource(token));
        }
        Assert.assertTrue(coordinator.seal(token));
        return coordinator;
    }

    private static void runRegistrationCycle(
            FiberCancellationSignal cancellationSignal,
            FiberWaitCoordinator coordinator,
            TimerShards timerShards,
            FiberWalWaitQueue walWaitQueue
    ) {
        cancellationSignal.reset();
        final long token = coordinator.beginBuild(3);
        final FiberCancellationWaitRegistration cancellation = coordinator.acquireCancellation(token, cancellationSignal);
        final FiberTimerWaitRegistration timer = coordinator.acquireTimer(
                token,
                timerShards,
                MillisecondClockImpl.INSTANCE,
                60_000
        );
        final FiberWalWaitRegistration wal = coordinator.acquireWal(token, 1);
        if (cancellation.register() != SourceRegistrationResult.ACCEPTED
                || !coordinator.tryAcceptSource(token)
                || timer.register() != SourceRegistrationResult.ACCEPTED
                || !coordinator.tryAcceptSource(token)
                || wal.register(walWaitQueue) != SourceRegistrationResult.ACCEPTED
                || !coordinator.tryAcceptSource(token)
                || !coordinator.seal(token)
                || !cancellation.cancel()
                || !timer.cancel()
                || !wal.cancel()
                || !coordinator.abort(token)
                || coordinator.consume(token) != FiberWaitCoordinator.REASON_ABORTED) {
            throw new AssertionError("wait registration cycle did not complete");
        }
    }

    private static final class FailOnSecondHeapComparisonClock implements MillisecondClock {
        private int invocationCount;

        @Override
        public long getTicks() {
            // acquireTimer() and the first heap level consume the two successful reads.
            if (invocationCount++ < 2) {
                return MillisecondClockImpl.INSTANCE.getTicks();
            }
            throw new TestRegistrationException();
        }
    }

    private static final class TestCleanupException extends RuntimeException {
    }

    private static final class TestRegistrationException extends RuntimeException {
        private TestRegistrationException() {
            this("test timer registration failure");
        }

        private TestRegistrationException(String message) {
            super(message);
        }
    }

    private static final class TestTarget implements FiberWaitCoordinator.Target {
        private int acquiredRegistrationCount;
        private int fireCount;
        private RuntimeException fireException;
        private int reason;
        private int releasedRegistrationCount;

        @Override
        public void abortWait(long token) {
        }

        @Override
        public boolean fireWait(long token, int reason) {
            fireCount++;
            this.reason = reason;
            final RuntimeException fireException = this.fireException;
            this.fireException = null;
            if (fireException != null) {
                throw fireException;
            }
            return true;
        }

        @Override
        public void onWaitRegistrationAcquired() {
            acquiredRegistrationCount++;
        }

        @Override
        public void onWaitRegistrationReleased() {
            releasedRegistrationCount++;
        }
    }
}
