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

public final class FiberEventWaitRegistration extends FiberWaitRegistrationNode<FiberEventWaitRegistration> {
    private static final int STATE_FIRING = 3;
    private static final int STATE_FREE = 0;
    private static final int STATE_NEW = 1;
    private static final long STATE_OFFSET = Unsafe.getFieldOffset(FiberEventWaitRegistration.class, "state");
    private static final int STATE_QUEUED = 2;
    FiberEventWaitRegistration nextFire;
    FiberEventWaitRegistration nextQueue;
    FiberEventWaitRegistration prevQueue;
    FiberEventWaitQueue queue;
    private final FiberWaitCoordinator coordinator;
    @SuppressWarnings("FieldMayBeFinal")
    private volatile int state = STATE_FREE;
    private long token;

    FiberEventWaitRegistration(FiberWaitCoordinator coordinator) {
        this.coordinator = coordinator;
    }

    @Override
    public boolean cancel() {
        if (state == STATE_NEW) {
            return releaseNew();
        }
        final FiberEventWaitQueue queue = this.queue;
        if (queue != null && queue.unregister(this)) {
            return releaseNew();
        }
        return false;
    }

    public SourceRegistrationResult register(FiberEventWaitQueue queue) {
        final long registrationToken = token;
        final SourceRegistrationResult result = queue.register(this);
        if (result == SourceRegistrationResult.NOT_ACCEPTED) {
            releaseNew();
        }
        return coordinator.completeSourceRegistration(registrationToken, this, result);
    }

    boolean fire(int reason) {
        if (Unsafe.cas(this, STATE_OFFSET, STATE_FIRING, STATE_FREE)) {
            final long token = this.token;
            try {
                return coordinator.fire(token, reason);
            } finally {
                clear();
                coordinator.release(this);
            }
        }
        return false;
    }

    @Override
    boolean isForToken(long token) {
        return this.token == token;
    }

    boolean markFiring() {
        return Unsafe.cas(this, STATE_OFFSET, STATE_QUEUED, STATE_FIRING);
    }

    boolean markQueued() {
        return Unsafe.cas(this, STATE_OFFSET, STATE_NEW, STATE_QUEUED);
    }

    boolean markUnregistered() {
        return Unsafe.cas(this, STATE_OFFSET, STATE_QUEUED, STATE_NEW);
    }

    void of(long token) {
        nextActive = null;
        nextFire = null;
        nextFree = null;
        nextQueue = null;
        prevActive = null;
        prevQueue = null;
        queue = null;
        state = STATE_NEW;
        this.token = token;
    }

    private void clear() {
        nextFire = null;
        nextQueue = null;
        prevQueue = null;
        queue = null;
        token = 0;
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
