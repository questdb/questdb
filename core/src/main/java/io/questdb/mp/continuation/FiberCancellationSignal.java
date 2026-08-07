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

import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.atomic.AtomicBoolean;

public final class FiberCancellationSignal extends AtomicBoolean {
    private static final long CANCELLED_MASK = 1;
    private static final long INITIAL_GENERATION = 1;
    private static final long MAX_GENERATION = Long.MAX_VALUE >>> 1;
    private static final long serialVersionUID = 1L;
    private final transient @Nullable Runnable beforeRegisterForTesting;
    private int firingRegistrationCount;
    private FiberCancellationWaitRegistration registrations;
    private volatile long state = pack(INITIAL_GENERATION);

    public FiberCancellationSignal() {
        this(null);
    }

    @TestOnly
    public FiberCancellationSignal(@Nullable Runnable beforeRegisterForTesting) {
        this.beforeRegisterForTesting = beforeRegisterForTesting;
    }

    public void cancel() {
        cancel(getGeneration());
    }

    public boolean cancel(long expectedGeneration) {
        final FiberCancellationWaitRegistration fireHead;
        synchronized (this) {
            final long state = this.state;
            if (generation(state) != expectedGeneration) {
                return false;
            }
            this.state = state | CANCELLED_MASK;
            set(true);
            fireHead = detachRegistrations();
        }
        fireRegistrations(fireHead);
        return true;
    }

    public long getGeneration() {
        return generation(state);
    }

    public boolean isCancelled(long expectedGeneration) {
        final long state = this.state;
        return generation(state) != expectedGeneration || (state & CANCELLED_MASK) != 0;
    }

    public long reopen() {
        final FiberCancellationWaitRegistration fireHead;
        final long nextGeneration;
        synchronized (this) {
            final long generation = generation(state);
            if (generation == MAX_GENERATION) {
                throw new IllegalStateException("fiber cancellation generation exhausted");
            }
            nextGeneration = generation + 1;
            fireHead = detachRegistrations();
            state = pack(nextGeneration);
            set(false);
        }
        fireRegistrations(fireHead);
        return nextGeneration;
    }

    public synchronized void reset() {
        reset(getGeneration());
    }

    public synchronized boolean reset(long expectedGeneration) {
        if (generation(state) != expectedGeneration) {
            return false;
        }
        if (registrations != null || firingRegistrationCount != 0) {
            throw new IllegalStateException("cannot reset cancellation signal with an active wait");
        }
        state = pack(expectedGeneration);
        set(false);
        return true;
    }

    private static long generation(long state) {
        return state >>> 1;
    }

    private static long pack(long generation) {
        return generation << 1;
    }

    private FiberCancellationWaitRegistration detachRegistrations() {
        FiberCancellationWaitRegistration fireHead = null;
        FiberCancellationWaitRegistration fireTail = null;
        FiberCancellationWaitRegistration registration = registrations;
        while (registration != null) {
            final FiberCancellationWaitRegistration next = registration.nextSignal;
            if (registration.markFiring()) {
                unlink(registration);
                firingRegistrationCount++;
                if (fireHead == null) {
                    fireHead = registration;
                } else {
                    fireTail.nextFire = registration;
                }
                fireTail = registration;
            }
            registration = next;
        }
        return fireHead;
    }

    private void fireRegistrations(FiberCancellationWaitRegistration fireHead) {
        Throwable failure = null;
        while (fireHead != null) {
            final FiberCancellationWaitRegistration next = fireHead.nextFire;
            fireHead.nextFire = null;
            try {
                fireHead.fire();
            } catch (RuntimeException | Error th) {
                if (failure == null) {
                    failure = th;
                } else if (failure != th) {
                    failure.addSuppressed(th);
                }
            } finally {
                onRegistrationFired();
            }
            fireHead = next;
        }
        if (failure instanceof RuntimeException runtimeException) {
            throw runtimeException;
        }
        if (failure != null) {
            throw (Error) failure;
        }
    }

    private synchronized void onRegistrationFired() {
        if (firingRegistrationCount < 1) {
            throw new IllegalStateException("cancellation firing registration underflow");
        }
        firingRegistrationCount--;
    }

    private void unlink(FiberCancellationWaitRegistration registration) {
        final FiberCancellationWaitRegistration next = registration.nextSignal;
        final FiberCancellationWaitRegistration prev = registration.prevSignal;
        if (prev == null) {
            registrations = next;
        } else {
            prev.nextSignal = next;
        }
        if (next != null) {
            next.prevSignal = prev;
        }
        registration.nextSignal = null;
        registration.prevSignal = null;
    }

    SourceRegistrationResult register(FiberCancellationWaitRegistration registration) {
        final Runnable hook = beforeRegisterForTesting;
        if (hook != null) {
            hook.run();
        }
        final boolean isCancelled;
        synchronized (this) {
            final long state = this.state;
            isCancelled = generation(state) != registration.getExpectedGeneration()
                    || (state & CANCELLED_MASK) != 0;
            if (!isCancelled) {
                registration.nextSignal = registrations;
                registration.prevSignal = null;
                if (registrations != null) {
                    registrations.prevSignal = registration;
                }
                registrations = registration;
            } else if (!registration.markFiring()) {
                return SourceRegistrationResult.NOT_ACCEPTED;
            } else {
                firingRegistrationCount++;
            }
        }
        if (isCancelled) {
            try {
                registration.fire();
            } finally {
                onRegistrationFired();
            }
        }
        return SourceRegistrationResult.ACCEPTED;
    }

    synchronized boolean unregister(FiberCancellationWaitRegistration registration) {
        final FiberCancellationWaitRegistration next = registration.nextSignal;
        final FiberCancellationWaitRegistration prev = registration.prevSignal;
        if ((prev == null && registrations != registration) || !registration.markUnregistered()) {
            return false;
        }
        unlink(registration);
        return true;
    }
}
