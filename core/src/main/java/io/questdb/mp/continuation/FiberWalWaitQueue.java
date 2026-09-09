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

public final class FiberWalWaitQueue {
    private FiberWalWaitRegistration head;
    private volatile int size;
    private FiberWalWaitRegistration tail;

    public void fire(long writerTxn, boolean isTerminal) {
        if (size == 0) {
            return;
        }
        FiberWalWaitRegistration fireHead = null;
        FiberWalWaitRegistration fireTail = null;
        synchronized (this) {
            FiberWalWaitRegistration registration = head;
            while (registration != null) {
                FiberWalWaitRegistration next = registration.nextList;
                if (isTerminal || writerTxn >= registration.getTargetWriterTxn()) {
                    if (registration.markFiring()) {
                        unlink(registration);
                        if (fireHead == null) {
                            fireHead = registration;
                        } else {
                            fireTail.nextFire = registration;
                        }
                        fireTail = registration;
                    }
                }
                registration = next;
            }
        }
        Throwable failure = null;
        while (fireHead != null) {
            FiberWalWaitRegistration next = fireHead.nextFire;
            fireHead.nextFire = null;
            try {
                fireHead.fire();
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

    public synchronized SourceRegistrationResult register(FiberWalWaitRegistration registration) {
        if (registration.queue != null) {
            return SourceRegistrationResult.NOT_ACCEPTED;
        }
        registration.queue = this;
        if (!registration.markQueued()) {
            registration.queue = null;
            return SourceRegistrationResult.NOT_ACCEPTED;
        }
        registration.prevList = tail;
        if (tail == null) {
            head = registration;
        } else {
            tail.nextList = registration;
        }
        tail = registration;
        size++;
        return SourceRegistrationResult.ACCEPTED;
    }

    public synchronized int size() {
        return size;
    }

    public synchronized boolean unregister(FiberWalWaitRegistration registration) {
        if (registration.queue != this || !registration.markUnregistered()) {
            return false;
        }
        unlink(registration);
        return true;
    }

    private void unlink(FiberWalWaitRegistration registration) {
        FiberWalWaitRegistration next = registration.nextList;
        FiberWalWaitRegistration prev = registration.prevList;
        if (prev == null) {
            head = next;
        } else {
            prev.nextList = next;
        }
        if (next == null) {
            tail = prev;
        } else {
            next.prevList = prev;
        }
        registration.nextList = null;
        registration.prevList = null;
        registration.queue = null;
        // volatile only for fire()'s unlocked fast path; every write is under this monitor
        size--;
    }
}
