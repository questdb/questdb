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
import io.questdb.mp.continuation.DelayedFireable;
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
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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
        Assert.assertTrue(coordinator.seal(token));
        Assert.assertFalse(coordinator.hasInFlightRegistrations());
        Assert.assertEquals(FiberWaitCoordinator.REASON_CANCEL, coordinator.consume(token));

        cancellationSignal.reset();
        final long nextToken = coordinator.beginBuild(1);
        final FiberCancellationWaitRegistration next = coordinator.acquireCancellation(nextToken, cancellationSignal);
        Assert.assertSame(registration, next);
        Assert.assertSame(SourceRegistrationResult.ACCEPTED, next.register());
        Assert.assertTrue(coordinator.seal(nextToken));
        cancellationSignal.cancel();
        Assert.assertFalse(coordinator.hasInFlightRegistrations());
        Assert.assertEquals(FiberWaitCoordinator.REASON_CANCEL, coordinator.consume(nextToken));
        Assert.assertEquals(2, target.fireCount);
    }

    @Test
    public void testCancellationEarlyFireDoesNotCancelReusedRegistration() {
        final FiberCancellationSignal oldSignal = new FiberCancellationSignal();
        final FiberCancellationSignal newSignal = new FiberCancellationSignal();
        final TestTarget target = new TestTarget();
        final FiberWaitCoordinator coordinator = new FiberWaitCoordinator(target);
        oldSignal.cancel();
        final long oldToken = coordinator.beginBuild(1);
        final FiberCancellationWaitRegistration oldRegistration = coordinator.acquireCancellation(
                oldToken,
                oldSignal
        );
        final AtomicReference<FiberCancellationWaitRegistration> newRegistration = new AtomicReference<>();
        final AtomicLong newToken = new AtomicLong();
        target.releaseAction = () -> {
            Assert.assertTrue(coordinator.abort(oldToken));
            Assert.assertEquals(FiberWaitCoordinator.REASON_NONE, coordinator.consume(oldToken));
            final long token = coordinator.beginBuild(1);
            newToken.set(token);
            final FiberCancellationWaitRegistration registration = coordinator.acquireCancellation(token, newSignal);
            newRegistration.set(registration);
            Assert.assertSame(oldRegistration, registration);
            Assert.assertSame(SourceRegistrationResult.ACCEPTED, registration.register());
            Assert.assertTrue(coordinator.seal(token));
        };

        Assert.assertSame(SourceRegistrationResult.NOT_ACCEPTED, oldRegistration.register());
        Assert.assertSame(oldRegistration, newRegistration.get());
        Assert.assertTrue(coordinator.isArmed(newToken.get()));
        Assert.assertTrue(coordinator.hasInFlightRegistrations());

        newSignal.cancel();

        Assert.assertFalse(coordinator.hasInFlightRegistrations());
        Assert.assertEquals(FiberWaitCoordinator.REASON_CANCEL, coordinator.consume(newToken.get()));
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
        Assert.assertEquals(FiberWaitCoordinator.REASON_NONE, coordinator.consume(token));
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
        Assert.assertEquals(FiberWaitCoordinator.REASON_NONE, coordinator.consume(token));

        final long nextToken = coordinator.beginBuild(1);
        final FiberCancellationWaitRegistration next = coordinator.acquireCancellation(nextToken, cancellationSignal);
        Assert.assertSame(registration, next);
        Assert.assertTrue(next.cancel());
        Assert.assertTrue(coordinator.abort(nextToken));
        Assert.assertEquals(FiberWaitCoordinator.REASON_NONE, coordinator.consume(nextToken));
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
        Assert.assertTrue(coordinator.seal(token));
        Assert.assertTrue(registration.cancel());
        Assert.assertFalse(coordinator.hasInFlightRegistrations());
        cancellationSignal.cancel();
        Assert.assertEquals(0, target.fireCount);
        Assert.assertTrue(coordinator.abort(token));
        Assert.assertEquals(FiberWaitCoordinator.REASON_NONE, coordinator.consume(token));
    }

    @Test
    public void testCancellationStaleGenerationFailsClosed() {
        final FiberCancellationSignal cancellationSignal = new FiberCancellationSignal();
        final long staleGeneration = cancellationSignal.getGeneration();
        final long currentGeneration = cancellationSignal.reopen();

        final TestTarget staleTarget = new TestTarget();
        final FiberWaitCoordinator staleCoordinator = new FiberWaitCoordinator(staleTarget);
        final long staleToken = staleCoordinator.beginBuild(1);
        final FiberCancellationWaitRegistration staleRegistration = staleCoordinator.acquireCancellation(
                staleToken,
                cancellationSignal,
                staleGeneration
        );
        Assert.assertSame(SourceRegistrationResult.ACCEPTED, staleRegistration.register());
        Assert.assertTrue(staleCoordinator.seal(staleToken));
        Assert.assertEquals(FiberWaitCoordinator.REASON_CANCEL, staleCoordinator.consume(staleToken));
        Assert.assertEquals(1, staleTarget.fireCount);

        final TestTarget currentTarget = new TestTarget();
        final FiberWaitCoordinator currentCoordinator = new FiberWaitCoordinator(currentTarget);
        final long currentToken = currentCoordinator.beginBuild(1);
        final FiberCancellationWaitRegistration currentRegistration = currentCoordinator.acquireCancellation(
                currentToken,
                cancellationSignal,
                currentGeneration
        );
        Assert.assertSame(SourceRegistrationResult.ACCEPTED, currentRegistration.register());
        Assert.assertTrue(currentCoordinator.seal(currentToken));

        Assert.assertFalse(cancellationSignal.cancel(staleGeneration));
        Assert.assertEquals(0, currentTarget.fireCount);
        Assert.assertTrue(cancellationSignal.cancel(currentGeneration));
        Assert.assertEquals(FiberWaitCoordinator.REASON_CANCEL, currentCoordinator.consume(currentToken));
        Assert.assertEquals(1, currentTarget.fireCount);
    }

    @Test
    public void testCancellationStaleRegistrationCannotAttachAfterReopen() throws Exception {
        final CountDownLatch registrationStarted = new CountDownLatch(1);
        final CountDownLatch resumeRegistration = new CountDownLatch(1);
        final FiberCancellationSignal cancellationSignal = new FiberCancellationSignal(() -> {
            registrationStarted.countDown();
            try {
                if (!resumeRegistration.await(10, TimeUnit.SECONDS)) {
                    throw new AssertionError("timed out waiting to resume cancellation registration");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        });
        final long staleGeneration = cancellationSignal.getGeneration();
        final TestTarget target = new TestTarget();
        final FiberWaitCoordinator coordinator = new FiberWaitCoordinator(target);
        final long token = coordinator.beginBuild(1);
        final FiberCancellationWaitRegistration registration = coordinator.acquireCancellation(
                token,
                cancellationSignal,
                staleGeneration
        );
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final Thread registrationThread = new Thread(() -> {
            try {
                Assert.assertSame(SourceRegistrationResult.ACCEPTED, registration.register());
            } catch (Throwable th) {
                failure.set(th);
            }
        }, "stale-cancellation-registration");

        registrationThread.start();
        try {
            Assert.assertTrue(registrationStarted.await(10, TimeUnit.SECONDS));
            cancellationSignal.reopen();
        } finally {
            resumeRegistration.countDown();
            registrationThread.join(10_000);
        }

        Assert.assertFalse(registrationThread.isAlive());
        Assert.assertNull(failure.get());
        Assert.assertTrue(coordinator.seal(token));
        Assert.assertEquals(FiberWaitCoordinator.REASON_CANCEL, coordinator.consume(token));
        Assert.assertEquals(1, target.fireCount);
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
        final FiberEventWaitQueue queue = new FiberEventWaitQueue(FiberWaitCoordinator.REASON_CAPACITY);
        final ObjList<FiberWaitCoordinator> coordinators = new ObjList<>();
        final ObjList<TestTarget> targets = new ObjList<>();
        final LongList tokens = new LongList();

        for (int i = 0; i < 3; i++) {
            final TestTarget target = new TestTarget();
            final FiberWaitCoordinator coordinator = new FiberWaitCoordinator(target);
            final long token = coordinator.beginBuild(1);
            final FiberEventWaitRegistration registration = coordinator.acquireEvent(token);
            Assert.assertSame(SourceRegistrationResult.ACCEPTED, registration.register(queue));
            Assert.assertTrue(coordinator.seal(token));
            coordinators.add(coordinator);
            targets.add(target);
            tokens.add(token);
        }

        queue.fireAll();

        for (int i = 0; i < coordinators.size(); i++) {
            final FiberWaitCoordinator coordinator = coordinators.getQuick(i);
            Assert.assertFalse(coordinator.hasInFlightRegistrations());
            Assert.assertEquals(FiberWaitCoordinator.REASON_CAPACITY, coordinator.consume(tokens.getQuick(i)));
            Assert.assertEquals(1, targets.getQuick(i).fireCount);
        }

        final TestTarget nextTarget = new TestTarget();
        final FiberWaitCoordinator nextCoordinator = new FiberWaitCoordinator(nextTarget);
        final long nextToken = nextCoordinator.beginBuild(1);
        final FiberEventWaitRegistration nextRegistration = nextCoordinator.acquireEvent(nextToken);
        Assert.assertSame(SourceRegistrationResult.ACCEPTED, nextRegistration.register(queue));
        Assert.assertTrue(nextCoordinator.seal(nextToken));
        queue.fire();
        Assert.assertEquals(FiberWaitCoordinator.REASON_CAPACITY, nextCoordinator.consume(nextToken));
        Assert.assertEquals(1, nextTarget.fireCount);
    }

    @Test
    public void testEventRegistrationRollsBackWhenWaitAborts() throws Exception {
        final FiberEventWaitQueue queue = new FiberEventWaitQueue(FiberWaitCoordinator.REASON_PROGRESS);
        final TestTarget target = new TestTarget();
        final FiberWaitCoordinator coordinator = new FiberWaitCoordinator(target);
        final long token = coordinator.beginBuild(1);
        final FiberEventWaitRegistration registration = coordinator.acquireEvent(token);
        final CountDownLatch registrationStarted = new CountDownLatch(1);
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final AtomicReference<SourceRegistrationResult> result = new AtomicReference<>();
        final Thread registrationThread = new Thread(() -> {
            registrationStarted.countDown();
            try {
                result.set(registration.register(queue));
            } catch (Throwable th) {
                failure.set(th);
            }
        });

        synchronized (queue) {
            registrationThread.start();
            Assert.assertTrue(registrationStarted.await(10, TimeUnit.SECONDS));
            Assert.assertTrue(coordinator.abort(token));
        }
        registrationThread.join(10_000);

        Assert.assertFalse(registrationThread.isAlive());
        Assert.assertNull(failure.get());
        Assert.assertSame(SourceRegistrationResult.NOT_ACCEPTED, result.get());
        Assert.assertFalse(coordinator.hasInFlightRegistrations());
        Assert.assertEquals(FiberWaitCoordinator.REASON_NONE, coordinator.consume(token));
        Assert.assertEquals(1, target.acquiredRegistrationCount);
        Assert.assertEquals(1, target.releasedRegistrationCount);
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
        Assert.assertSame(SourceRegistrationResult.ACCEPTED, second.register(secondQueue));
        Assert.assertTrue(coordinator.seal(token));

        coordinator.shutdown();

        Assert.assertFalse(coordinator.hasInFlightRegistrations());
        Assert.assertEquals(0, firstQueue.size());
        Assert.assertEquals(0, secondQueue.size());
        Assert.assertTrue(coordinator.isFired(token));
        Assert.assertEquals(FiberWaitCoordinator.REASON_SHUTDOWN, coordinator.consume(token));
    }

    @Test
    public void testSlotCallbackAndCoordinatorReleaseFailureReturnsGrantedSlot() {
        final AtomicInteger releasedSlot = new AtomicInteger(-1);
        final FiberSlotWaitQueue queue = new FiberSlotWaitQueue(releasedSlot::set);
        final TestTarget target = new TestTarget();
        final FiberWaitCoordinator coordinator = new FiberWaitCoordinator(target);
        final long token = coordinator.beginBuild(1);
        final FiberSlotWaitRegistration registration = coordinator.acquireSlot(token);
        Assert.assertSame(SourceRegistrationResult.ACCEPTED, registration.register(queue));
        Assert.assertTrue(coordinator.seal(token));
        final TestRegistrationException fireFailure =
                new TestRegistrationException("test slot callback failure");
        final TestRegistrationException releaseFailure =
                new TestRegistrationException("test slot release callback failure");
        target.fireException = fireFailure;
        target.releaseException = releaseFailure;

        try {
            queue.transfer(7);
            Assert.fail("expected slot callback failure");
        } catch (TestRegistrationException expected) {
            Assert.assertSame(fireFailure, expected);
            Assert.assertArrayEquals(new Throwable[]{releaseFailure}, expected.getSuppressed());
        }

        Assert.assertEquals(7, releasedSlot.get());
        Assert.assertFalse(coordinator.hasInFlightRegistrations());
        Assert.assertEquals(FiberWaitCoordinator.REASON_SLOT, coordinator.consume(token));
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
    public void testSlotCancellationReleasesGrantedSlotOutsideCoordinatorMonitor() throws Exception {
        final TestTarget target = new TestTarget();
        final FiberWaitCoordinator coordinator = new FiberWaitCoordinator(target);
        final CountDownLatch monitorEntered = new CountDownLatch(1);
        final AtomicReference<Thread> monitorThread = new AtomicReference<>();
        final FiberSlotWaitQueue queue = new FiberSlotWaitQueue(slot -> {
            Assert.assertEquals(7, slot);
            final Thread thread = new Thread(() -> {
                synchronized (coordinator) {
                    monitorEntered.countDown();
                }
            }, "slot-cancel-monitor-probe");
            thread.setDaemon(true);
            monitorThread.set(thread);
            thread.start();
            try {
                Assert.assertTrue(monitorEntered.await(5, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        });
        final long token = coordinator.beginBuild(1);
        final FiberSlotWaitRegistration registration = coordinator.acquireSlot(token);
        Assert.assertSame(SourceRegistrationResult.ACCEPTED, registration.register(queue));
        Assert.assertTrue(coordinator.seal(token));
        Assert.assertTrue(queue.transfer(7));

        coordinator.shutdown();

        final Thread thread = monitorThread.get();
        Assert.assertNotNull(thread);
        thread.join(5_000);
        Assert.assertFalse(thread.isAlive());
        Assert.assertFalse(coordinator.hasInFlightRegistrations());
        Assert.assertEquals(FiberWaitCoordinator.REASON_SLOT, coordinator.consume(token));
    }

    @Test
    public void testSlotRegistrationRollbackReleasesGrantedSlotOutsideCoordinatorMonitor() throws Exception {
        final TestTarget target = new TestTarget();
        final FiberWaitCoordinator coordinator = new FiberWaitCoordinator(target);
        final AtomicInteger releasedSlot = new AtomicInteger(-1);
        final FiberSlotWaitQueue queue = new FiberSlotWaitQueue(slot -> {
            releasedSlot.set(slot);
            Assert.assertFalse(
                    "slot registration rollback held the coordinator monitor",
                    Thread.holdsLock(coordinator)
            );
        });
        final long token = coordinator.beginBuild(1);
        final FiberSlotWaitRegistration registration = coordinator.acquireSlot(token);
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final AtomicReference<SourceRegistrationResult> result = new AtomicReference<>();
        final Thread registrationThread = new Thread(() -> {
            try {
                result.set(registration.register(queue));
            } catch (Throwable th) {
                failure.set(th);
            }
        }, "slot-registration-rollback");
        registrationThread.setDaemon(true);

        boolean isSlotGranted = false;
        try {
            synchronized (coordinator) {
                registrationThread.start();
                final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (!queue.hasWaiters()
                        && registrationThread.isAlive()
                        && System.nanoTime() < deadline) {
                    Thread.onSpinWait();
                }
                Assert.assertTrue("slot registration did not reach the queue", queue.hasWaiters());
                Assert.assertTrue("slot registration did not accept the grant", queue.transfer(7));
                isSlotGranted = true;
                Assert.assertTrue("wait build did not abort", coordinator.abort(token));
            }
            registrationThread.join(10_000);
        } finally {
            registrationThread.interrupt();
            registration.cancel();
            coordinator.abort(token);
            try {
                registrationThread.join(10_000);
            } finally {
                Assert.assertFalse("slot registration thread did not stop", registrationThread.isAlive());
                Assert.assertFalse(queue.hasWaiters());
                Assert.assertFalse(coordinator.hasInFlightRegistrations());
                Assert.assertEquals(1, target.acquiredRegistrationCount);
                Assert.assertEquals(1, target.releasedRegistrationCount);
                Assert.assertEquals(FiberWaitCoordinator.REASON_NONE, coordinator.consume(token));
                if (isSlotGranted) {
                    Assert.assertEquals(7, releasedSlot.get());
                }
            }
        }
        final Throwable th = failure.get();
        if (th != null) {
            throw new AssertionError("slot registration rollback failed", th);
        }
        Assert.assertSame(SourceRegistrationResult.NOT_ACCEPTED, result.get());
    }

    @Test
    public void testSlotWaitBuildRejectsSecondRegistration() {
        final TestTarget target = new TestTarget();
        final FiberWaitCoordinator coordinator = new FiberWaitCoordinator(target);
        final long token = coordinator.beginBuild(2);
        final FiberSlotWaitRegistration registration = coordinator.acquireSlot(token);
        try {
            coordinator.acquireSlot(token);
            Assert.fail("expected duplicate slot registration rejection");
        } catch (IllegalStateException e) {
            Assert.assertEquals("wait coordinator already has a slot registration", e.getMessage());
        }
        Assert.assertTrue(registration.cancel());
        Assert.assertTrue(coordinator.abort(token));
        Assert.assertEquals(FiberWaitCoordinator.REASON_NONE, coordinator.consume(token));
        Assert.assertFalse(coordinator.hasInFlightRegistrations());
    }

    @Test
    public void testSteadyStateWaitRegistrationReuseAllocatesNoJavaHeap() {
        try (TestUtils.ThreadMetricsScope<com.sun.management.ThreadMXBean> scope = TestUtils.threadAllocationScope()) {
            final com.sun.management.ThreadMXBean threadMXBean = scope.getBean();
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
            Assert.assertTrue(coordinator.seal(token));
            Assert.assertTrue(registration.cancel());
            Assert.assertFalse(coordinator.hasInFlightRegistrations());
            Assert.assertEquals(1, target.releasedRegistrationCount);
            Assert.assertTrue(coordinator.abort(token));
            Assert.assertEquals(FiberWaitCoordinator.REASON_NONE, coordinator.consume(token));

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
            Assert.assertEquals(FiberWaitCoordinator.REASON_NONE, coordinator.consume(nextToken));
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
        Assert.assertEquals(FiberWaitCoordinator.REASON_NONE, coordinator.consume(token));
    }

    @Test
    public void testTimerRegistrationCleanupFailureDiscardsHolder() {
        final TimerShards timerShards = new TimerShards(1, "test-timer", LOG);
        timerShards.start();
        registerTimerBlockers(timerShards);
        try {
            final TestTarget target = new TestTarget();
            final FiberWaitCoordinator coordinator = new FiberWaitCoordinator(target);
            final long token = coordinator.beginBuild(1);
            final FiberTimerWaitRegistration registration = coordinator.acquireTimer(
                    token,
                    timerShards,
                    new FailOnHeapComparisonClock(),
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
            Assert.assertEquals(FiberWaitCoordinator.REASON_NONE, coordinator.consume(token));

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
            Assert.assertEquals(FiberWaitCoordinator.REASON_NONE, coordinator.consume(nextToken));
        } finally {
            timerShards.shutdown();
        }
    }

    @Test
    public void testTimerRegistrationFailureReleasesHolder() {
        final TimerShards timerShards = new TimerShards(1, "test-timer", LOG);
        timerShards.start();
        registerTimerBlockers(timerShards);
        try {
            final TestTarget target = new TestTarget();
            final FiberWaitCoordinator coordinator = new FiberWaitCoordinator(target);
            final long token = coordinator.beginBuild(1);
            final FiberTimerWaitRegistration registration = coordinator.acquireTimer(
                    token,
                    timerShards,
                    new FailOnHeapComparisonClock(),
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
            Assert.assertEquals(FiberWaitCoordinator.REASON_NONE, coordinator.consume(token));

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
            Assert.assertEquals(FiberWaitCoordinator.REASON_NONE, coordinator.consume(nextToken));
        } finally {
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
        Assert.assertTrue(coordinator.seal(token));

        Assert.assertTrue(registration.cancel());
        Assert.assertFalse(coordinator.hasInFlightRegistrations());
        Assert.assertEquals(1, target.releasedRegistrationCount);
        Assert.assertEquals(0, queue.size());
        Assert.assertTrue(coordinator.abort(token));
        Assert.assertEquals(FiberWaitCoordinator.REASON_NONE, coordinator.consume(token));

        long nextToken = coordinator.beginBuild(1);
        FiberWalWaitRegistration next = coordinator.acquireWal(nextToken, 20);
        Assert.assertSame(registration, next);
        Assert.assertEquals(2, target.acquiredRegistrationCount);
        Assert.assertTrue(next.cancel());
        Assert.assertEquals(2, target.releasedRegistrationCount);
        Assert.assertTrue(coordinator.abort(nextToken));
        Assert.assertEquals(FiberWaitCoordinator.REASON_NONE, coordinator.consume(nextToken));
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
        long oldToken = coordinator.beginBuild(1);
        FiberWalWaitRegistration oldRegistration = coordinator.acquireWal(oldToken, 10);
        Assert.assertSame(SourceRegistrationResult.ACCEPTED, oldRegistration.register(queue));
        Assert.assertTrue(coordinator.seal(oldToken));
        Assert.assertTrue(coordinator.fire(oldToken, FiberWaitCoordinator.REASON_TIMER));
        Assert.assertEquals(FiberWaitCoordinator.REASON_TIMER, coordinator.consume(oldToken));

        long token = coordinator.beginBuild(1);
        FiberWalWaitRegistration registration = coordinator.acquireWal(token, 10);
        Assert.assertNotSame(oldRegistration, registration);
        Assert.assertSame(SourceRegistrationResult.ACCEPTED, registration.register(queue));
        Assert.assertTrue(coordinator.seal(token));

        queue.fire(10, false);

        Assert.assertTrue(coordinator.isFired(token));
        Assert.assertEquals(2, target.fireCount);
        Assert.assertFalse(coordinator.hasInFlightRegistrations());
        Assert.assertEquals(FiberWaitCoordinator.REASON_WAL, coordinator.consume(token));
    }

    private static void registerTimerBlockers(TimerShards timerShards) {
        // Plain entries rather than FiberTimerWaitRegistrations: sift comparisons against a foreign
        // Delayed type take the getDelay() fallback, which is what lets FailOnHeapComparisonClock
        // inject a registration failure inside DelayHeap.offer().
        for (int i = 0; i < 3; i++) {
            Assert.assertSame(
                    SourceRegistrationResult.ACCEPTED,
                    timerShards.register(new BlockerEntry(MillisecondClockImpl.INSTANCE.getTicks() + 120_000))
            );
        }
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
                || timer.register() != SourceRegistrationResult.ACCEPTED
                || wal.register(walWaitQueue) != SourceRegistrationResult.ACCEPTED
                || !coordinator.seal(token)
                || !cancellation.cancel()
                || !timer.cancel()
                || !wal.cancel()
                || !coordinator.abort(token)
                || coordinator.consume(token) != FiberWaitCoordinator.REASON_NONE) {
            throw new AssertionError("wait registration cycle did not complete");
        }
    }

    private static final class BlockerEntry implements DelayedFireable {
        private final long deadlineMillis;
        private int heapIndex = -1;

        private BlockerEntry(long deadlineMillis) {
            this.deadlineMillis = deadlineMillis;
        }

        @Override
        public int compareTo(@NotNull Delayed other) {
            return Long.compare(getDelay(TimeUnit.NANOSECONDS), other.getDelay(TimeUnit.NANOSECONDS));
        }

        @Override
        public void expire() {
        }

        @Override
        public long getDelay(@NotNull TimeUnit unit) {
            return unit.convert(deadlineMillis - MillisecondClockImpl.INSTANCE.getTicks(), TimeUnit.MILLISECONDS);
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
        }
    }

    private static final class FailOnHeapComparisonClock implements MillisecondClock {
        private int invocationCount;

        @Override
        public long getTicks() {
            // acquireTimer() consumes the only successful read; the first sift comparison against
            // a BlockerEntry takes the getDelay() fallback and throws.
            if (invocationCount++ < 1) {
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
        private Runnable releaseAction;
        private int releasedRegistrationCount;
        private RuntimeException releaseException;

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
            final Runnable releaseAction = this.releaseAction;
            this.releaseAction = null;
            if (releaseAction != null) {
                releaseAction.run();
            }
            final RuntimeException releaseException = this.releaseException;
            this.releaseException = null;
            if (releaseException != null) {
                throw releaseException;
            }
        }
    }
}
