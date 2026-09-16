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

public final class FiberSlotWaitQueue {
    private FiberSlotWaitRegistration head;
    private final SlotReleaser slotReleaser;
    private FiberSlotWaitRegistration tail;
    // Volatile only for transfer()'s unlocked fast path; every write is under this monitor.
    private volatile int waiterCount;

    public FiberSlotWaitQueue(SlotReleaser slotReleaser) {
        if (slotReleaser == null) {
            throw new IllegalArgumentException("slot releaser must not be null");
        }
        this.slotReleaser = slotReleaser;
    }

    public boolean hasWaiters() {
        return waiterCount != 0;
    }

    public synchronized SourceRegistrationResult register(FiberSlotWaitRegistration registration) {
        if (registration.queue != null) {
            return SourceRegistrationResult.NOT_ACCEPTED;
        }
        registration.queue = this;
        if (!registration.markQueued()) {
            registration.queue = null;
            return SourceRegistrationResult.NOT_ACCEPTED;
        }
        registration.prevQueue = tail;
        if (tail == null) {
            head = registration;
        } else {
            tail.nextQueue = registration;
        }
        tail = registration;
        waiterCount++;
        return SourceRegistrationResult.ACCEPTED;
    }

    public boolean transfer(int slot) {
        while (waiterCount != 0) {
            FiberSlotWaitRegistration registration;
            synchronized (this) {
                while ((registration = head) != null) {
                    if (registration.markGranted(slot)) {
                        unlink(registration, false);
                        break;
                    }
                    unlink(registration, true);
                }
            }
            if (registration == null) {
                return false;
            }
            if (registration.tryFire()) {
                return true;
            }
        }
        return false;
    }

    public synchronized boolean unregister(FiberSlotWaitRegistration registration) {
        if (registration.queue != this || !registration.markUnregistered()) {
            return false;
        }
        unlink(registration, true);
        return true;
    }

    void releaseGrantedSlot(int slot) {
        slotReleaser.releaseSlot(slot);
    }

    private void unlink(FiberSlotWaitRegistration registration, boolean isClearingQueue) {
        final FiberSlotWaitRegistration next = registration.nextQueue;
        final FiberSlotWaitRegistration prev = registration.prevQueue;
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
        waiterCount--;
        if (isClearingQueue) {
            registration.queue = null;
        }
    }

    public interface SlotReleaser {
        void releaseSlot(int slot);
    }
}
