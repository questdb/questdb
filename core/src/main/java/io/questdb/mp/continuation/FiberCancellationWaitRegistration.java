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

public final class FiberCancellationWaitRegistration extends FiberWaitRegistrationNode<FiberCancellationWaitRegistration> {
    private static final int STATE_FIRING = 3;
    private static final int STATE_FREE = 0;
    private static final int STATE_NEW = 1;
    private static final long STATE_OFFSET = Unsafe.getFieldOffset(FiberCancellationWaitRegistration.class, "state");
    private static final int STATE_QUEUED = 2;
    FiberCancellationWaitRegistration nextFire;
    FiberCancellationWaitRegistration nextSignal;
    FiberCancellationWaitRegistration prevSignal;
    private FiberCancellationSignal cancellationSignal;
    private final FiberWaitCoordinator coordinator;
    private long expectedGeneration;
    @SuppressWarnings("FieldMayBeFinal")
    private volatile int state = STATE_FREE;
    private long token;

    FiberCancellationWaitRegistration(FiberWaitCoordinator coordinator) {
        this.coordinator = coordinator;
    }

    @Override
    public boolean cancel() {
        if (state == STATE_NEW) {
            return releaseNew();
        }
        final FiberCancellationSignal cancellationSignal = this.cancellationSignal;
        if (cancellationSignal != null
                && cancellationSignal.unregister(this)) {
            return releaseNew();
        }
        return false;
    }

    public SourceRegistrationResult register() {
        if (!Unsafe.cas(this, STATE_OFFSET, STATE_NEW, STATE_QUEUED)) {
            return SourceRegistrationResult.NOT_ACCEPTED;
        }
        final long registrationToken = token;
        final SourceRegistrationResult result;
        try {
            final FiberCancellationSignal cancellationSignal = this.cancellationSignal;
            if (cancellationSignal == null) {
                throw new IllegalStateException("cancellation signal is not configured");
            }
            result = cancellationSignal.register(this);
        } catch (Throwable th) {
            rollbackRegistration(th);
            throw th;
        }
        if (result == SourceRegistrationResult.NOT_ACCEPTED
                && Unsafe.cas(this, STATE_OFFSET, STATE_QUEUED, STATE_NEW)) {
            releaseNew();
        }
        return coordinator.completeSourceRegistration(registrationToken, this, result);
    }

    long getExpectedGeneration() {
        return expectedGeneration;
    }

    @Override
    boolean isForToken(long token) {
        return this.token == token;
    }

    private void clear() {
        cancellationSignal = null;
        expectedGeneration = CancellationBinding.NO_GENERATION;
        nextFire = null;
        nextSignal = null;
        prevSignal = null;
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

    private void rollbackRegistration(Throwable failure) {
        final FiberCancellationSignal cancellationSignal = this.cancellationSignal;
        boolean isUnregistered = false;
        if (cancellationSignal != null) {
            try {
                isUnregistered = cancellationSignal.unregister(this);
            } catch (Throwable cleanupError) {
                if (cleanupError != failure) {
                    failure.addSuppressed(cleanupError);
                }
            }
        }
        if (isUnregistered || Unsafe.cas(this, STATE_OFFSET, STATE_QUEUED, STATE_NEW)) {
            try {
                releaseNew();
            } catch (Throwable cleanupError) {
                if (cleanupError != failure) {
                    failure.addSuppressed(cleanupError);
                }
            }
        }
    }

    void fire() {
        if (Unsafe.cas(this, STATE_OFFSET, STATE_FIRING, STATE_FREE)) {
            final long token = this.token;
            try {
                coordinator.fire(token, FiberWaitCoordinator.REASON_CANCEL);
            } finally {
                clear();
                coordinator.release(this);
            }
        }
    }

    boolean markFiring() {
        return Unsafe.cas(this, STATE_OFFSET, STATE_QUEUED, STATE_FIRING);
    }

    boolean markUnregistered() {
        return Unsafe.cas(this, STATE_OFFSET, STATE_QUEUED, STATE_NEW);
    }

    void of(long token, FiberCancellationSignal cancellationSignal, long expectedGeneration) {
        this.cancellationSignal = cancellationSignal;
        this.expectedGeneration = expectedGeneration;
        nextActive = null;
        nextFire = null;
        nextFree = null;
        nextSignal = null;
        prevActive = null;
        prevSignal = null;
        state = STATE_NEW;
        this.token = token;
    }
}
