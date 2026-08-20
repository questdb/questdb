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

public final class FiberEventWaitQueue {
    private FiberEventWaitRegistration head;
    private volatile boolean isClosed;
    private final int reason;
    private FiberEventWaitRegistration tail;
    private volatile int waiterCount;

    public FiberEventWaitQueue(int reason) {
        if (reason <= FiberWaitCoordinator.REASON_NONE) {
            throw new IllegalArgumentException("event reason must be positive");
        }
        this.reason = reason;
    }

    public void fire() {
        while (true) {
            final FiberEventWaitRegistration registration;
            if (waiterCount == 0) {
                return;
            }
            synchronized (this) {
                registration = head;
                if (registration == null || !registration.markFiring()) {
                    return;
                }
                unlink(registration);
            }
            if (registration.fire(reason)) {
                return;
            }
        }
    }

    public void fireAll() {
        if (waiterCount == 0) {
            return;
        }
        fireAll(reason, false);
    }

    public SourceRegistrationResult register(FiberEventWaitRegistration registration) {
        final boolean isFiring;
        synchronized (this) {
            if (registration.queue != null) {
                return SourceRegistrationResult.NOT_ACCEPTED;
            }
            registration.queue = this;
            if (!registration.markQueued()) {
                registration.queue = null;
                return SourceRegistrationResult.NOT_ACCEPTED;
            }
            isFiring = isClosed;
            if (isFiring) {
                registration.queue = null;
                if (!registration.markFiring()) {
                    return SourceRegistrationResult.NOT_ACCEPTED;
                }
            } else {
                registration.prevQueue = tail;
                if (tail == null) {
                    head = registration;
                } else {
                    tail.nextQueue = registration;
                }
                tail = registration;
                waiterCount++;
            }
        }
        if (isFiring) {
            registration.fire(FiberWaitCoordinator.REASON_SHUTDOWN);
        }
        return SourceRegistrationResult.ACCEPTED;
    }

    public void shutdown() {
        fireAll(FiberWaitCoordinator.REASON_SHUTDOWN, true);
    }

    public synchronized boolean unregister(FiberEventWaitRegistration registration) {
        if (registration.queue != this || !registration.markUnregistered()) {
            return false;
        }
        unlink(registration);
        return true;
    }

    private void fireAll(int reason, boolean isShutdown) {
        FiberEventWaitRegistration fireHead = null;
        FiberEventWaitRegistration fireTail = null;
        synchronized (this) {
            if (isShutdown) {
                isClosed = true;
            }
            FiberEventWaitRegistration registration = head;
            while (registration != null) {
                final FiberEventWaitRegistration next = registration.nextQueue;
                if (registration.markFiring()) {
                    unlink(registration);
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
            final FiberEventWaitRegistration next = fireHead.nextFire;
            fireHead.nextFire = null;
            try {
                fireHead.fire(reason);
            } catch (RuntimeException | Error th) {
                if (failure == null) {
                    failure = th;
                } else if (failure != th) {
                    failure.addSuppressed(th);
                }
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

    private void unlink(FiberEventWaitRegistration registration) {
        final FiberEventWaitRegistration next = registration.nextQueue;
        final FiberEventWaitRegistration prev = registration.prevQueue;
        if (prev == null) {
            head = next;
        } else {
            prev.nextQueue = next;
        }
        if (next == null) {
            tail = prev;
        } else {
            next.prevQueue = prev;
        }
        registration.nextQueue = null;
        registration.prevQueue = null;
        registration.queue = null;
        waiterCount--;
    }
}
