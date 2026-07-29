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
    public static final int REASON_ABORTED = -1;
    public static final int REASON_CAPACITY = 6;
    public static final int REASON_CANCEL = 4;
    public static final int REASON_NONE = 0;
    public static final int REASON_SHUTDOWN = 3;
    public static final int REASON_SLOT = 5;
    public static final int REASON_TIMER = 1;
    public static final int REASON_WAL = 2;
    public static final int REASON_WRITER = 7;
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
        checkBuilding(token);
        FiberCancellationWaitRegistration registration = freeCancellationRegistrations;
        if (registration == null) {
            registration = new FiberCancellationWaitRegistration(this);
        } else {
            freeCancellationRegistrations = registration.nextFree;
        }
        registration.of(token, cancellationSignal);
        target.onWaitRegistrationAcquired();
        inFlightRegistrationCount++;
        link(registration);
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
        link(registration);
        return registration;
    }

    public synchronized FiberSlotWaitRegistration acquireSlot(long token) {
        checkBuilding(token);
        FiberSlotWaitRegistration registration = freeSlotRegistrations;
        if (registration == null) {
            registration = new FiberSlotWaitRegistration(this);
        } else {
            freeSlotRegistrations = registration.nextFree;
        }
        registration.of(token);
        target.onWaitRegistrationAcquired();
        inFlightRegistrationCount++;
        link(registration);
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
            link(registration);
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
        link(registration);
        return registration;
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
        helpFire(token);
        final int reason;
        final boolean isFired;
        synchronized (this) {
            if (this.token != token || (state != STATE_ABORTED && state != STATE_FIRED)) {
                return REASON_NONE;
            }
            isFired = state == STATE_FIRED;
            reason = isFired ? wakeReason : REASON_ABORTED;
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
                }
                return true;
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

    public synchronized boolean tryAcceptSource(long token) {
        if (this.token != token || state != STATE_BUILDING || acceptedSourceCount >= expectedSourceCount) {
            return false;
        }
        acceptedSourceCount++;
        return true;
    }

    synchronized void discard(FiberTimerWaitRegistration registration) {
        if (inFlightRegistrationCount < 1) {
            throw new IllegalStateException("timer registration is not in flight");
        }
        unlink(registration);
        inFlightRegistrationCount--;
        target.onWaitRegistrationReleased();
    }

    synchronized void release(FiberCancellationWaitRegistration registration) {
        if (inFlightRegistrationCount < 1) {
            throw new IllegalStateException("cancellation registration is not in flight");
        }
        unlink(registration);
        inFlightRegistrationCount--;
        registration.nextFree = freeCancellationRegistrations;
        freeCancellationRegistrations = registration;
        target.onWaitRegistrationReleased();
    }

    synchronized void release(FiberEventWaitRegistration registration) {
        if (inFlightRegistrationCount < 1) {
            throw new IllegalStateException("event registration is not in flight");
        }
        unlink(registration);
        inFlightRegistrationCount--;
        registration.nextFree = freeEventRegistrations;
        freeEventRegistrations = registration;
        target.onWaitRegistrationReleased();
    }

    synchronized void release(FiberSlotWaitRegistration registration) {
        if (inFlightRegistrationCount < 1) {
            throw new IllegalStateException("slot registration is not in flight");
        }
        unlink(registration);
        inFlightRegistrationCount--;
        registration.nextFree = freeSlotRegistrations;
        freeSlotRegistrations = registration;
        target.onWaitRegistrationReleased();
    }

    synchronized void release(FiberTimerWaitRegistration registration) {
        if (inFlightRegistrationCount < 1) {
            throw new IllegalStateException("timer registration is not in flight");
        }
        unlink(registration);
        inFlightRegistrationCount--;
        registration.nextFree = freeTimerRegistrations;
        freeTimerRegistrations = registration;
        target.onWaitRegistrationReleased();
    }

    synchronized void release(FiberWalWaitRegistration registration) {
        if (inFlightRegistrationCount < 1) {
            throw new IllegalStateException("WAL registration is not in flight");
        }
        unlink(registration);
        inFlightRegistrationCount--;
        registration.nextFree = freeWalRegistrations;
        freeWalRegistrations = registration;
        target.onWaitRegistrationReleased();
    }

    private synchronized void cancelInFlightRegistrations() {
        FiberCancellationWaitRegistration cancellationRegistration = activeCancellationRegistrations;
        while (cancellationRegistration != null) {
            final FiberCancellationWaitRegistration next = cancellationRegistration.nextActive;
            cancellationRegistration.cancel();
            cancellationRegistration = next;
        }
        FiberEventWaitRegistration eventRegistration = activeEventRegistrations;
        while (eventRegistration != null) {
            final FiberEventWaitRegistration next = eventRegistration.nextActive;
            eventRegistration.cancel();
            eventRegistration = next;
        }
        FiberSlotWaitRegistration slotRegistration = activeSlotRegistrations;
        while (slotRegistration != null) {
            final FiberSlotWaitRegistration next = slotRegistration.nextActive;
            slotRegistration.cancel();
            slotRegistration = next;
        }
        FiberTimerWaitRegistration timerRegistration = activeTimerRegistrations;
        while (timerRegistration != null) {
            final FiberTimerWaitRegistration next = timerRegistration.nextActive;
            timerRegistration.cancel();
            timerRegistration = next;
        }
        FiberWalWaitRegistration walRegistration = activeWalRegistrations;
        while (walRegistration != null) {
            final FiberWalWaitRegistration next = walRegistration.nextActive;
            walRegistration.cancel();
            walRegistration = next;
        }
    }

    private void checkBuilding(long token) {
        if (this.token != token || state != STATE_BUILDING) {
            throw new IllegalStateException("wait coordinator is not building this token");
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

    private void link(FiberCancellationWaitRegistration registration) {
        registration.nextActive = activeCancellationRegistrations;
        registration.prevActive = null;
        if (activeCancellationRegistrations != null) {
            activeCancellationRegistrations.prevActive = registration;
        }
        activeCancellationRegistrations = registration;
    }

    private void link(FiberEventWaitRegistration registration) {
        registration.nextActive = activeEventRegistrations;
        registration.prevActive = null;
        if (activeEventRegistrations != null) {
            activeEventRegistrations.prevActive = registration;
        }
        activeEventRegistrations = registration;
    }

    private void link(FiberSlotWaitRegistration registration) {
        registration.nextActive = activeSlotRegistrations;
        registration.prevActive = null;
        if (activeSlotRegistrations != null) {
            activeSlotRegistrations.prevActive = registration;
        }
        activeSlotRegistrations = registration;
    }

    private void link(FiberTimerWaitRegistration registration) {
        registration.nextActive = activeTimerRegistrations;
        registration.prevActive = null;
        if (activeTimerRegistrations != null) {
            activeTimerRegistrations.prevActive = registration;
        }
        activeTimerRegistrations = registration;
    }

    private void link(FiberWalWaitRegistration registration) {
        registration.nextActive = activeWalRegistrations;
        registration.prevActive = null;
        if (activeWalRegistrations != null) {
            activeWalRegistrations.prevActive = registration;
        }
        activeWalRegistrations = registration;
    }

    private void unlink(FiberCancellationWaitRegistration registration) {
        final FiberCancellationWaitRegistration next = registration.nextActive;
        final FiberCancellationWaitRegistration prev = registration.prevActive;
        if (prev == null) {
            if (activeCancellationRegistrations != registration) {
                throw new IllegalStateException("cancellation registration is not active");
            }
            activeCancellationRegistrations = next;
        } else {
            prev.nextActive = next;
        }
        if (next != null) {
            next.prevActive = prev;
        }
        registration.nextActive = null;
        registration.prevActive = null;
    }

    private void unlink(FiberEventWaitRegistration registration) {
        final FiberEventWaitRegistration next = registration.nextActive;
        final FiberEventWaitRegistration prev = registration.prevActive;
        if (prev == null) {
            if (activeEventRegistrations != registration) {
                throw new IllegalStateException("event registration is not active");
            }
            activeEventRegistrations = next;
        } else {
            prev.nextActive = next;
        }
        if (next != null) {
            next.prevActive = prev;
        }
        registration.nextActive = null;
        registration.prevActive = null;
    }

    private void unlink(FiberSlotWaitRegistration registration) {
        final FiberSlotWaitRegistration next = registration.nextActive;
        final FiberSlotWaitRegistration prev = registration.prevActive;
        if (prev == null) {
            if (activeSlotRegistrations != registration) {
                throw new IllegalStateException("slot registration is not active");
            }
            activeSlotRegistrations = next;
        } else {
            prev.nextActive = next;
        }
        if (next != null) {
            next.prevActive = prev;
        }
        registration.nextActive = null;
        registration.prevActive = null;
    }

    private void unlink(FiberTimerWaitRegistration registration) {
        final FiberTimerWaitRegistration next = registration.nextActive;
        final FiberTimerWaitRegistration prev = registration.prevActive;
        if (prev == null) {
            if (activeTimerRegistrations != registration) {
                throw new IllegalStateException("timer registration is not active");
            }
            activeTimerRegistrations = next;
        } else {
            prev.nextActive = next;
        }
        if (next != null) {
            next.prevActive = prev;
        }
        registration.nextActive = null;
        registration.prevActive = null;
    }

    private void unlink(FiberWalWaitRegistration registration) {
        final FiberWalWaitRegistration next = registration.nextActive;
        final FiberWalWaitRegistration prev = registration.prevActive;
        if (prev == null) {
            if (activeWalRegistrations != registration) {
                throw new IllegalStateException("WAL registration is not active");
            }
            activeWalRegistrations = next;
        } else {
            prev.nextActive = next;
        }
        if (next != null) {
            next.prevActive = prev;
        }
        registration.nextActive = null;
        registration.prevActive = null;
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
