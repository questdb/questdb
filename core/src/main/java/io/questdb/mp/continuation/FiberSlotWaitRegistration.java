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

import io.questdb.std.Unsafe;

public final class FiberSlotWaitRegistration extends FiberWaitRegistrationNode<FiberSlotWaitRegistration> {
    private static final int STATE_FIRING = 4;
    private static final int STATE_FIRING_CANCELLED = 6;
    private static final int STATE_FREE = 0;
    private static final int STATE_GRANTED = 3;
    private static final int STATE_NEW = 1;
    private static final int STATE_NOTIFIED = 5;
    private static final long STATE_OFFSET = Unsafe.getFieldOffset(FiberSlotWaitRegistration.class, "state");
    private static final int STATE_QUEUED = 2;
    FiberSlotWaitRegistration nextQueue;
    FiberSlotWaitRegistration prevQueue;
    FiberSlotWaitQueue queue;
    private final FiberWaitCoordinator coordinator;
    private int grantedSlot = -1;
    @SuppressWarnings("FieldMayBeFinal")
    private volatile int state = STATE_FREE;
    private long token;

    FiberSlotWaitRegistration(FiberWaitCoordinator coordinator) {
        this.coordinator = coordinator;
    }

    @Override
    public boolean cancel() {
        if (state == STATE_NEW) {
            return releaseNew();
        }
        final FiberSlotWaitQueue queue = this.queue;
        if (queue != null && queue.unregister(this)) {
            return releaseNew();
        }
        while (true) {
            final int state = this.state;
            if (state == STATE_FIRING) {
                if (Unsafe.cas(this, STATE_OFFSET, STATE_FIRING, STATE_FIRING_CANCELLED)) {
                    return true;
                }
                continue;
            }
            if (state != STATE_GRANTED && state != STATE_NOTIFIED) {
                return false;
            }
            if (Unsafe.cas(this, STATE_OFFSET, state, STATE_FREE)) {
                releaseGrantedSlot();
                return true;
            }
        }
    }

    public SourceRegistrationResult register(FiberSlotWaitQueue queue) {
        final long registrationToken = token;
        final SourceRegistrationResult result = queue.register(this);
        if (result == SourceRegistrationResult.NOT_ACCEPTED) {
            releaseNew();
        }
        return coordinator.completeSourceRegistration(registrationToken, this, result);
    }

    public int takeSlot() {
        if (!Unsafe.cas(this, STATE_OFFSET, STATE_NOTIFIED, STATE_FREE)) {
            return -1;
        }
        final int slot = grantedSlot;
        clear();
        coordinator.release(this);
        return slot;
    }

    boolean tryFire() {
        if (!Unsafe.cas(this, STATE_OFFSET, STATE_GRANTED, STATE_FIRING)) {
            return true;
        }
        final long token = this.token;
        if (Unsafe.cas(this, STATE_OFFSET, STATE_FIRING, STATE_NOTIFIED)) {
            try {
                coordinator.fire(token, FiberWaitCoordinator.REASON_SLOT);
            } catch (RuntimeException | Error th) {
                if (Unsafe.cas(this, STATE_OFFSET, STATE_NOTIFIED, STATE_FREE)) {
                    try {
                        releaseGrantedSlot();
                    } catch (RuntimeException | Error cleanupError) {
                        if (cleanupError != th) {
                            th.addSuppressed(cleanupError);
                        }
                    }
                }
                throw th;
            }
        } else if (Unsafe.cas(this, STATE_OFFSET, STATE_FIRING_CANCELLED, STATE_FREE)) {
            releaseCancelledGrant();
            return false;
        }
        return true;
    }

    @Override
    boolean isForToken(long token) {
        return this.token == token;
    }

    // A granted registration stays linked until its releaser reaches coordinator.release(). It is
    // FIRING_CANCELLED while the handoff runs and FREE between the terminal CAS and the unlink, so
    // both states mean "a peer owns the completion" and neither belongs to the current build.
    boolean isPeerOwned() {
        final int state = this.state;
        return state == STATE_FIRING_CANCELLED || state == STATE_FREE;
    }

    boolean markGranted(int slot) {
        grantedSlot = slot;
        if (Unsafe.cas(this, STATE_OFFSET, STATE_QUEUED, STATE_GRANTED)) {
            return true;
        }
        grantedSlot = -1;
        return false;
    }

    boolean markQueued() {
        return Unsafe.cas(this, STATE_OFFSET, STATE_NEW, STATE_QUEUED);
    }

    boolean markUnregistered() {
        return Unsafe.cas(this, STATE_OFFSET, STATE_QUEUED, STATE_NEW);
    }

    void of(long token) {
        grantedSlot = -1;
        nextActive = null;
        nextFree = null;
        nextQueue = null;
        prevActive = null;
        prevQueue = null;
        queue = null;
        state = STATE_NEW;
        this.token = token;
    }

    private void clear() {
        grantedSlot = -1;
        nextQueue = null;
        prevQueue = null;
        queue = null;
        token = 0;
    }

    private void releaseGrantedSlot() {
        final int slot = grantedSlot;
        final FiberSlotWaitQueue grantedQueue = queue;
        clear();
        Throwable failure = null;
        try {
            coordinator.release(this);
        } catch (RuntimeException | Error th) {
            failure = th;
        }
        if (grantedQueue == null) {
            final IllegalStateException th =
                    new IllegalStateException("granted slot registration has no queue");
            if (failure == null) {
                failure = th;
            } else {
                failure.addSuppressed(th);
            }
        } else {
            try {
                grantedQueue.releaseGrantedSlot(slot);
            } catch (RuntimeException | Error th) {
                if (failure == null) {
                    failure = th;
                } else if (failure != th) {
                    failure.addSuppressed(th);
                }
            }
        }
        if (failure instanceof RuntimeException runtimeException) {
            throw runtimeException;
        }
        if (failure != null) {
            throw (Error) failure;
        }
    }

    private void releaseCancelledGrant() {
        final int slot = grantedSlot;
        final FiberSlotWaitQueue grantedQueue = queue;
        clear();
        try {
            coordinator.release(this);
        } catch (RuntimeException | Error th) {
            try {
                grantedQueue.releaseGrantedSlot(slot);
            } catch (RuntimeException | Error cleanupError) {
                if (cleanupError != th) {
                    th.addSuppressed(cleanupError);
                }
            }
            throw th;
        }
    }

    private boolean releaseNew() {
        if (Unsafe.cas(this, STATE_OFFSET, STATE_NEW, STATE_FREE)) {
            clear();
            coordinator.release(this);
            return true;
        }
        return false;
    }
}
