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

package io.questdb.mp.continuation;

import io.questdb.std.datetime.millitime.MillisecondClock;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

public final class FiberWaitCoordinator {
    public static final int REASON_CANCEL = 4;
    public static final int REASON_CAPACITY = 6;
    public static final int REASON_NONE = 0;
    public static final int REASON_PROGRESS = 7;
    public static final int REASON_SHUTDOWN = 3;
    public static final int REASON_SLOT = 5;
    public static final int REASON_TIMER = 1;
    public static final int REASON_WAL = 2;
    private static final int RESULT_NOT_CONSUMED = Integer.MIN_VALUE;
    private static final int STATE_ABORTED = 5;
    private static final int STATE_ARMED = 2;
    private static final int STATE_BUILDING = 1;
    private static final int STATE_FIRED = 4;
    private static final int STATE_FIRING = 3;
    private static final int STATE_UNARMED = 0;
    private static final long TOKEN_MASK = -1L >>> Fiber.EXECUTION_STATE_BITS;
    private int acceptedSourceCount;
    private FiberCancellationWaitRegistration activeCancellationRegistrations;
    private FiberEventWaitRegistration activeEventRegistrations;
    private FiberSlotWaitRegistration activeSlotRegistrations;
    private FiberTimerWaitRegistration activeTimerRegistrations;
    private FiberWalWaitRegistration activeWalRegistrations;
    private final @Nullable Runnable beforeFireWaitForTesting;
    private int expectedSourceCount;
    private FiberCancellationWaitRegistration freeCancellationRegistrations;
    private FiberEventWaitRegistration freeEventRegistrations;
    private FiberSlotWaitRegistration freeSlotRegistrations;
    private FiberTimerWaitRegistration freeTimerRegistrations;
    private FiberWalWaitRegistration freeWalRegistrations;
    private int inFlightRegistrationCount;
    private int pendingReason;
    private int state = STATE_UNARMED;
    private final Target target;
    private long token;
    private int wakeReason;

    public FiberWaitCoordinator(Target target) {
        this(target, null);
    }

    FiberWaitCoordinator(Target target, @Nullable Runnable beforeFireWaitForTesting) {
        this.beforeFireWaitForTesting = beforeFireWaitForTesting;
        this.target = target;
        freeCancellationRegistrations = new FiberCancellationWaitRegistration(this);
        freeEventRegistrations = new FiberEventWaitRegistration(this);
        freeSlotRegistrations = new FiberSlotWaitRegistration(this);
        freeTimerRegistrations = new FiberTimerWaitRegistration(this);
        freeWalRegistrations = new FiberWalWaitRegistration(this);
    }

    public boolean abort(long token) {
        boolean isAborted;
        synchronized (this) {
            isAborted = this.token == token && (state == STATE_BUILDING || state == STATE_ARMED);
            if (isAborted) {
                pendingReason = REASON_NONE;
                state = STATE_ABORTED;
            }
        }
        if (isAborted) {
            target.abortWait(token);
        } else {
            helpFire(token);
        }
        return isAborted;
    }

    public synchronized FiberCancellationWaitRegistration acquireCancellation(
            long token,
            FiberCancellationSignal cancellationSignal
    ) {
        if (cancellationSignal == null) {
            throw new IllegalArgumentException("cancellation signal must not be null");
        }
        return acquireCancellation(token, cancellationSignal, cancellationSignal.getGeneration());
    }

    public synchronized FiberCancellationWaitRegistration acquireCancellation(
            long token,
            FiberCancellationSignal cancellationSignal,
            long expectedGeneration
    ) {
        if (cancellationSignal == null) {
            throw new IllegalArgumentException("cancellation signal must not be null");
        }
        checkBuilding(token);
        FiberCancellationWaitRegistration registration = freeCancellationRegistrations;
        if (registration == null) {
            registration = new FiberCancellationWaitRegistration(this);
        } else {
            freeCancellationRegistrations = registration.nextFree;
        }
        registration.of(token, cancellationSignal, expectedGeneration);
        target.onWaitRegistrationAcquired();
        inFlightRegistrationCount++;
        activeCancellationRegistrations = linkActive(activeCancellationRegistrations, registration);
        return registration;
    }

    public synchronized FiberEventWaitRegistration acquireEvent(long token) {
        checkBuilding(token);
        FiberEventWaitRegistration registration = freeEventRegistrations;
        if (registration == null) {
            registration = new FiberEventWaitRegistration(this);
        } else {
            freeEventRegistrations = registration.nextFree;
        }
        registration.of(token);
        target.onWaitRegistrationAcquired();
        inFlightRegistrationCount++;
        activeEventRegistrations = linkActive(activeEventRegistrations, registration);
        return registration;
    }

    public synchronized FiberSlotWaitRegistration acquireSlot(long token) {
        checkBuilding(token);
        // A granted slot may remain active while peer handoff completes. At most one other
        // cancellable slot registration may belong to the current build.
        FiberSlotWaitRegistration activeRegistration = activeSlotRegistrations;
        while (activeRegistration != null) {
            if (!activeRegistration.isPeerOwned()) {
                throw new IllegalStateException("wait coordinator already has a slot registration");
            }
            activeRegistration = activeRegistration.nextActive;
        }
        FiberSlotWaitRegistration registration = freeSlotRegistrations;
        if (registration == null) {
            registration = new FiberSlotWaitRegistration(this);
        } else {
            freeSlotRegistrations = registration.nextFree;
        }
        registration.of(token);
        target.onWaitRegistrationAcquired();
        inFlightRegistrationCount++;
        activeSlotRegistrations = linkActive(activeSlotRegistrations, registration);
        return registration;
    }

    public synchronized FiberTimerWaitRegistration acquireTimer(
            long token,
            TimerShards timerShards,
            MillisecondClock clock,
            long delayMillis
    ) {
        if (timerShards == null) {
            throw new IllegalArgumentException("timer shards must not be null");
        }
        if (clock == null) {
            throw new IllegalArgumentException("timer clock must not be null");
        }
        checkBuilding(token);
        FiberTimerWaitRegistration registration = freeTimerRegistrations;
        if (registration == null) {
            registration = new FiberTimerWaitRegistration(this);
        } else {
            freeTimerRegistrations = registration.nextFree;
        }
        try {
            registration.of(token, timerShards, clock, delayMillis);
            target.onWaitRegistrationAcquired();
            inFlightRegistrationCount++;
            activeTimerRegistrations = linkActive(activeTimerRegistrations, registration);
            return registration;
        } catch (Throwable th) {
            registration.nextFree = freeTimerRegistrations;
            freeTimerRegistrations = registration;
            throw th;
        }
    }

    public synchronized FiberWalWaitRegistration acquireWal(long token, long targetWriterTxn) {
        checkBuilding(token);
        FiberWalWaitRegistration registration = freeWalRegistrations;
        if (registration == null) {
            registration = new FiberWalWaitRegistration(this);
        } else {
            freeWalRegistrations = registration.nextFree;
        }
        registration.of(token, targetWriterTxn);
        target.onWaitRegistrationAcquired();
        inFlightRegistrationCount++;
        activeWalRegistrations = linkActive(activeWalRegistrations, registration);
        return registration;
    }

    public boolean armCancellation(long token, FiberCancellationSignal cancellationSignal) {
        return armCancellation(token, cancellationSignal, cancellationSignal.getGeneration());
    }

    public boolean armCancellation(
            long token,
            FiberCancellationSignal cancellationSignal,
            long expectedGeneration
    ) {
        final FiberCancellationWaitRegistration registration = acquireCancellation(
                token,
                cancellationSignal,
                expectedGeneration
        );
        return registration.register() == SourceRegistrationResult.ACCEPTED;
    }

    public boolean armEvent(long token, FiberEventWaitQueue queue) {
        final FiberEventWaitRegistration registration = acquireEvent(token);
        return registration.register(queue) == SourceRegistrationResult.ACCEPTED;
    }

    public boolean armTimer(long token, TimerShards timerShards, MillisecondClock clock, long delayMillis) {
        final FiberTimerWaitRegistration registration = acquireTimer(token, timerShards, clock, delayMillis);
        return registration.register() == SourceRegistrationResult.ACCEPTED;
    }

    public synchronized long beginBuild(int expectedSourceCount) {
        if (expectedSourceCount < 1) {
            throw new IllegalArgumentException("expectedSourceCount must be positive");
        }
        if (state != STATE_UNARMED) {
            throw new IllegalStateException("wait coordinator is already armed");
        }
        if (token >= TOKEN_MASK) {
            throw new IllegalStateException("wait coordinator token exhausted");
        }
        final long nextToken = token + 1;
        acceptedSourceCount = 0;
        this.expectedSourceCount = expectedSourceCount;
        pendingReason = REASON_NONE;
        state = STATE_BUILDING;
        token = nextToken;
        wakeReason = REASON_NONE;
        return nextToken;
    }

    public int consume(long token) {
        final int reason = consume0(token, REASON_NONE);
        return reason == RESULT_NOT_CONSUMED ? REASON_NONE : reason;
    }

    public synchronized long currentToken() {
        return state == STATE_UNARMED ? 0 : token;
    }

    public boolean fire(long token, int reason) {
        if (reason <= REASON_NONE) {
            throw new IllegalArgumentException("reason must be positive");
        }
        boolean isFireOwner = false;
        synchronized (this) {
            if (this.token != token) {
                return false;
            }
            if (state == STATE_BUILDING) {
                if (pendingReason == REASON_NONE) {
                    pendingReason = reason;
                    return true;
                }
                return false;
            }
            if (state == STATE_ARMED) {
                state = STATE_FIRING;
                wakeReason = reason;
                isFireOwner = true;
            } else if (state != STATE_FIRING) {
                return false;
            }
        }
        finishFire(token);
        return isFireOwner;
    }

    public synchronized boolean hasInFlightRegistrations() {
        return inFlightRegistrationCount > 0;
    }

    public synchronized boolean isArmed(long token) {
        return this.token == token && state == STATE_ARMED;
    }

    public synchronized boolean isFired(long token) {
        return this.token == token && state == STATE_FIRED;
    }

    /**
     * Resolves an early return taken between arming and {@link Fiber#suspendWait}. A cancellation
     * that fires while the wait is still building is only recorded as a pending reason; the
     * subsequent {@code teardownWait} discards it, so a site that skips the suspension must ask
     * for it here or the cancellation is lost until the next wait.
     */
    public synchronized int preferPendingCancel(long token, int reason) {
        if (this.token == token && state == STATE_BUILDING && pendingReason == REASON_CANCEL) {
            return REASON_CANCEL;
        }
        return reason;
    }

    public void quarantine() {
        final long token;
        synchronized (this) {
            token = state == STATE_UNARMED ? 0 : this.token;
        }
        if (token != 0) {
            abort(token);
            consume(token);
        }
        cancelInFlightRegistrations();
    }

    public boolean seal(long token) {
        synchronized (this) {
            if (this.token != token || state != STATE_BUILDING) {
                return false;
            }
            if (acceptedSourceCount != expectedSourceCount) {
                throw new IllegalStateException("cannot seal an incomplete wait registration");
            }
            if (pendingReason == REASON_NONE) {
                state = STATE_ARMED;
                return true;
            }
            state = STATE_FIRING;
            wakeReason = pendingReason;
            pendingReason = REASON_NONE;
        }
        finishFire(token);
        return true;
    }

    @TestOnly
    public synchronized void setTokenForTesting(long token) {
        if (state != STATE_UNARMED) {
            throw new IllegalStateException("wait coordinator is already armed");
        }
        this.token = token;
    }

    public void shutdown() {
        final boolean hasWait;
        final long token;
        synchronized (this) {
            hasWait = state == STATE_BUILDING || state == STATE_ARMED || state == STATE_FIRING;
            token = this.token;
        }
        if (hasWait) {
            fire(token, REASON_SHUTDOWN);
        }
        cancelInFlightRegistrations();
    }

    public void teardownWait(long token) {
        cancelInFlightRegistrations();
        abort(token);
        consume(token);
    }

    SourceRegistrationResult completeSourceRegistration(
            long token,
            FiberWaitRegistrationNode<?> registration,
            SourceRegistrationResult result
    ) {
        if (result != SourceRegistrationResult.ACCEPTED) {
            return result;
        }
        final boolean isCancelPending;
        synchronized (this) {
            final boolean isAccepted = this.token == token
                    && state == STATE_BUILDING
                    && acceptedSourceCount < expectedSourceCount;
            if (isAccepted) {
                acceptedSourceCount++;
                return SourceRegistrationResult.ACCEPTED;
            }
            isCancelPending = registration.isForToken(token);
        }
        // cancel outside the monitor: a granted slot cancel re-enters a peer coordinator
        if (isCancelPending) {
            registration.cancel();
        }
        return SourceRegistrationResult.NOT_ACCEPTED;
    }

    int consumeWait(long token, int abortedReason) {
        final int reason = consume0(token, abortedReason);
        if (reason == RESULT_NOT_CONSUMED) {
            throw new IllegalStateException("fiber wait cannot be consumed");
        }
        return reason;
    }

    synchronized void discard(FiberTimerWaitRegistration registration) {
        ensureInFlight();
        activeTimerRegistrations = unlinkActive(activeTimerRegistrations, registration);
        completeRelease();
    }

    synchronized void release(FiberCancellationWaitRegistration registration) {
        ensureInFlight();
        activeCancellationRegistrations = unlinkActive(activeCancellationRegistrations, registration);
        registration.nextFree = freeCancellationRegistrations;
        freeCancellationRegistrations = registration;
        completeRelease();
    }

    synchronized void release(FiberEventWaitRegistration registration) {
        ensureInFlight();
        activeEventRegistrations = unlinkActive(activeEventRegistrations, registration);
        registration.nextFree = freeEventRegistrations;
        freeEventRegistrations = registration;
        completeRelease();
    }

    synchronized void release(FiberSlotWaitRegistration registration) {
        ensureInFlight();
        activeSlotRegistrations = unlinkActive(activeSlotRegistrations, registration);
        registration.nextFree = freeSlotRegistrations;
        freeSlotRegistrations = registration;
        completeRelease();
    }

    synchronized void release(FiberTimerWaitRegistration registration) {
        ensureInFlight();
        activeTimerRegistrations = unlinkActive(activeTimerRegistrations, registration);
        registration.nextFree = freeTimerRegistrations;
        freeTimerRegistrations = registration;
        completeRelease();
    }

    synchronized void release(FiberWalWaitRegistration registration) {
        ensureInFlight();
        activeWalRegistrations = unlinkActive(activeWalRegistrations, registration);
        registration.nextFree = freeWalRegistrations;
        freeWalRegistrations = registration;
        completeRelease();
    }

    private static <T extends FiberWaitRegistrationNode<T>> void cancelAllActive(T head) {
        T registration = head;
        while (registration != null) {
            final T next = registration.nextActive;
            registration.cancel();
            registration = next;
        }
    }

    private static <T extends FiberWaitRegistrationNode<T>> T linkActive(T head, T registration) {
        registration.nextActive = head;
        registration.prevActive = null;
        if (head != null) {
            head.prevActive = registration;
        }
        return registration;
    }

    private static <T extends FiberWaitRegistrationNode<T>> T unlinkActive(T head, T registration) {
        final T next = registration.nextActive;
        final T prev = registration.prevActive;
        if (prev == null) {
            if (head != registration) {
                throw new IllegalStateException("wait registration is not active");
            }
            head = next;
        } else {
            prev.nextActive = next;
        }
        if (next != null) {
            next.prevActive = prev;
        }
        registration.nextActive = null;
        registration.prevActive = null;
        return head;
    }

    private void cancelInFlightRegistrations() {
        final FiberSlotWaitRegistration slotRegistration;
        synchronized (this) {
            cancelAllActive(activeCancellationRegistrations);
            cancelAllActive(activeEventRegistrations);
            cancelAllActive(activeTimerRegistrations);
            cancelAllActive(activeWalRegistrations);
            FiberSlotWaitRegistration registration = activeSlotRegistrations;
            FiberSlotWaitRegistration cancellableRegistration = null;
            while (registration != null) {
                if (!registration.isPeerOwned()) {
                    if (cancellableRegistration != null) {
                        throw new IllegalStateException("wait coordinator has multiple slot registrations");
                    }
                    cancellableRegistration = registration;
                }
                registration = registration.nextActive;
            }
            slotRegistration = cancellableRegistration;
        }
        if (slotRegistration != null) {
            slotRegistration.cancel();
        }
    }

    private void checkBuilding(long token) {
        if (this.token != token || state != STATE_BUILDING) {
            throw new IllegalStateException("wait coordinator is not building this token");
        }
    }

    private void completeRelease() {
        inFlightRegistrationCount--;
        target.onWaitRegistrationReleased();
    }

    private int consume0(long token, int abortedReason) {
        helpFire(token);
        final int reason;
        final boolean isFired;
        synchronized (this) {
            if (this.token != token || (state != STATE_ABORTED && state != STATE_FIRED)) {
                return RESULT_NOT_CONSUMED;
            }
            isFired = state == STATE_FIRED;
            reason = isFired ? wakeReason : abortedReason;
            acceptedSourceCount = 0;
            expectedSourceCount = 0;
            pendingReason = REASON_NONE;
            state = STATE_UNARMED;
            wakeReason = REASON_NONE;
        }
        if (isFired) {
            target.abortWait(token);
        }
        return reason;
    }

    private void ensureInFlight() {
        if (inFlightRegistrationCount < 1) {
            throw new IllegalStateException("wait registration is not in flight");
        }
    }

    private void finishFire(long token) {
        final int reason;
        synchronized (this) {
            if (this.token != token || state != STATE_FIRING) {
                return;
            }
            reason = wakeReason;
        }
        final Runnable hook = beforeFireWaitForTesting;
        if (hook != null) {
            hook.run();
        }
        if (target.fireWait(token, reason)) {
            synchronized (this) {
                if (this.token == token && state == STATE_FIRING && wakeReason == reason) {
                    state = STATE_FIRED;
                }
            }
        }
    }

    private void helpFire(long token) {
        synchronized (this) {
            if (this.token != token || state != STATE_FIRING) {
                return;
            }
        }
        finishFire(token);
    }

    public interface Target {
        void abortWait(long token);

        boolean fireWait(long token, int reason);

        default void onWaitRegistrationAcquired() {
        }

        default void onWaitRegistrationReleased() {
        }
    }
}
