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
    private static final long serialVersionUID = 1L;
    private final transient @Nullable Runnable beforeRegisterForTesting;
    private int firingRegistrationCount;
    private FiberCancellationWaitRegistration registrations;

    public FiberCancellationSignal() {
        this(null);
    }

    @TestOnly
    public FiberCancellationSignal(@Nullable Runnable beforeRegisterForTesting) {
        this.beforeRegisterForTesting = beforeRegisterForTesting;
    }

    public void cancel() {
        FiberCancellationWaitRegistration fireHead = null;
        FiberCancellationWaitRegistration fireTail = null;
        synchronized (this) {
            set(true);
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
        }
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

    public synchronized void reset() {
        if (registrations != null || firingRegistrationCount != 0) {
            throw new IllegalStateException("cannot reset cancellation signal with an active wait");
        }
        set(false);
    }

    SourceRegistrationResult register(FiberCancellationWaitRegistration registration) {
        final Runnable hook = beforeRegisterForTesting;
        if (hook != null) {
            hook.run();
        }
        final boolean isCancelled;
        synchronized (this) {
            isCancelled = get();
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
}
