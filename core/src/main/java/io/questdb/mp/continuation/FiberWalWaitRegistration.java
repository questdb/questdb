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

public final class FiberWalWaitRegistration extends FiberWaitRegistrationNode<FiberWalWaitRegistration> {
    private static final int STATE_FIRING = 3;
    private static final int STATE_FREE = 0;
    private static final int STATE_NEW = 1;
    private static final long STATE_OFFSET = Unsafe.getFieldOffset(FiberWalWaitRegistration.class, "state");
    private static final int STATE_QUEUED = 2;
    FiberWalWaitRegistration nextFire;
    FiberWalWaitRegistration nextList;
    FiberWalWaitRegistration prevList;
    FiberWalWaitQueue queue;
    private final FiberWaitCoordinator coordinator;
    @SuppressWarnings("FieldMayBeFinal")
    private volatile int state = STATE_FREE;
    private long targetWriterTxn;
    private long token;

    FiberWalWaitRegistration(FiberWaitCoordinator coordinator) {
        this.coordinator = coordinator;
    }

    @Override
    public boolean cancel() {
        if (state == STATE_NEW) {
            return releaseNew();
        }
        FiberWalWaitQueue queue = this.queue;
        if (queue != null && queue.unregister(this)) {
            return releaseNew();
        }
        return false;
    }

    public long getTargetWriterTxn() {
        return targetWriterTxn;
    }

    public SourceRegistrationResult register(FiberWalWaitQueue queue) {
        final long registrationToken = token;
        SourceRegistrationResult result = queue.register(this);
        if (result == SourceRegistrationResult.NOT_ACCEPTED) {
            releaseNew();
        }
        return coordinator.completeSourceRegistration(registrationToken, this, result);
    }

    void fire() {
        if (Unsafe.cas(this, STATE_OFFSET, STATE_FIRING, STATE_FREE)) {
            long token = this.token;
            try {
                coordinator.fire(token, FiberWaitCoordinator.REASON_WAL);
            } finally {
                clear();
                coordinator.release(this);
            }
        }
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

    FiberWalWaitRegistration of(long token, long targetWriterTxn) {
        nextActive = null;
        nextFire = null;
        nextFree = null;
        nextList = null;
        prevActive = null;
        prevList = null;
        queue = null;
        state = STATE_NEW;
        this.targetWriterTxn = targetWriterTxn;
        this.token = token;
        return this;
    }

    private void clear() {
        nextFire = null;
        nextList = null;
        prevList = null;
        queue = null;
        targetWriterTxn = 0;
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
