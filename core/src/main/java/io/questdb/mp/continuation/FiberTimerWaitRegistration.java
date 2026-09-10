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
import io.questdb.std.datetime.millitime.MillisecondClock;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public final class FiberTimerWaitRegistration extends FiberWaitRegistrationNode<FiberTimerWaitRegistration> implements DelayedFireable {
    private static final int STATE_DISCARDED = 3;
    private static final int STATE_FREE = 0;
    private static final int STATE_NEW = 1;
    private static final long STATE_OFFSET = Unsafe.getFieldOffset(FiberTimerWaitRegistration.class, "state");
    private static final int STATE_QUEUED = 2;
    @TestOnly
    private @Nullable Runnable beforeRegisterCleanupForTesting;
    private MillisecondClock clock;
    private final FiberWaitCoordinator coordinator;
    private long deadlineMillis;
    private int heapIndex = -1;
    @SuppressWarnings("FieldMayBeFinal")
    private volatile int state = STATE_FREE;
    private TimerShards timerShards;
    private long token;

    FiberTimerWaitRegistration(FiberWaitCoordinator coordinator) {
        this.coordinator = coordinator;
    }

    @Override
    public boolean cancel() {
        if (state == STATE_NEW) {
            return releaseNew();
        }
        TimerShards timerShards = this.timerShards;
        if (timerShards != null && timerShards.unregister(this)) {
            if (Unsafe.cas(this, STATE_OFFSET, STATE_QUEUED, STATE_NEW)) {
                return releaseNew();
            }
            if (Unsafe.cas(this, STATE_OFFSET, STATE_DISCARDED, STATE_FREE)) {
                clear();
                return true;
            }
        }
        return false;
    }

    @Override
    public int compareTo(@NotNull Delayed other) {
        // deadlines order identically to remaining delays but need no clock read under the shard
        // monitor; the fallback only serves foreign Delayed types such as the shutdown sentinel
        if (other instanceof FiberTimerWaitRegistration registration) {
            return Long.compare(deadlineMillis, registration.deadlineMillis);
        }
        return Long.compare(getDelay(TimeUnit.NANOSECONDS), other.getDelay(TimeUnit.NANOSECONDS));
    }

    @Override
    public void expire() {
        fire(FiberWaitCoordinator.REASON_TIMER);
    }

    @Override
    public long getDelay(@NotNull TimeUnit unit) {
        final MillisecondClock clock = this.clock;
        if (clock == null) {
            throw new IllegalStateException("timer clock is not configured");
        }
        return unit.convert(deadlineMillis - clock.getTicks(), TimeUnit.MILLISECONDS);
    }

    @Override
    public int getHeapIndex() {
        return heapIndex;
    }

    public SourceRegistrationResult register() {
        if (!Unsafe.cas(this, STATE_OFFSET, STATE_NEW, STATE_QUEUED)) {
            return SourceRegistrationResult.NOT_ACCEPTED;
        }
        final long registrationToken = token;
        final SourceRegistrationResult result;
        try {
            final TimerShards timerShards = this.timerShards;
            if (timerShards == null) {
                throw new IllegalStateException("timer shards are not configured");
            }
            result = timerShards.register(this);
        } catch (Throwable th) {
            boolean isCleanupFailed = false;
            try {
                final Runnable hook = beforeRegisterCleanupForTesting;
                if (hook != null) {
                    hook.run();
                }
                final TimerShards timerShards = this.timerShards;
                if (timerShards != null) {
                    timerShards.unregister(this);
                }
            } catch (Throwable cleanupError) {
                if (cleanupError != th) {
                    th.addSuppressed(cleanupError);
                }
                isCleanupFailed = true;
            }
            if (isCleanupFailed) {
                discardRegistration(th);
            } else {
                rollbackRegistration(th);
            }
            throw th;
        }
        if (result == SourceRegistrationResult.NOT_ACCEPTED) {
            if (Unsafe.cas(this, STATE_OFFSET, STATE_QUEUED, STATE_NEW)) {
                releaseNew();
            }
        }
        return coordinator.completeSourceRegistration(registrationToken, this, result);
    }

    @TestOnly
    public void setBeforeRegisterCleanupForTesting(@Nullable Runnable hook) {
        beforeRegisterCleanupForTesting = hook;
    }

    @TestOnly
    public void setClockForTesting(MillisecondClock clock) {
        this.clock = clock;
    }

    @Override
    public void setHeapIndex(int heapIndex) {
        this.heapIndex = heapIndex;
    }

    @Override
    public void shutdown() {
        fire(FiberWaitCoordinator.REASON_SHUTDOWN);
    }

    @Override
    boolean isForToken(long token) {
        return this.token == token;
    }

    FiberTimerWaitRegistration of(
            long token,
            TimerShards timerShards,
            MillisecondClock clock,
            long delayMillis
    ) {
        this.clock = clock;
        deadlineMillis = clock.getTicks() + delayMillis;
        nextActive = null;
        nextFree = null;
        prevActive = null;
        state = STATE_NEW;
        this.timerShards = timerShards;
        this.token = token;
        return this;
    }

    private void clear() {
        clock = null;
        deadlineMillis = 0;
        timerShards = null;
        token = 0;
    }

    private void discardRegistration(Throwable failure) {
        if (Unsafe.cas(this, STATE_OFFSET, STATE_QUEUED, STATE_DISCARDED)) {
            try {
                coordinator.discard(this);
            } catch (Throwable cleanupError) {
                if (cleanupError != failure) {
                    failure.addSuppressed(cleanupError);
                }
            }
        }
    }

    private boolean fire(int reason) {
        if (Unsafe.cas(this, STATE_OFFSET, STATE_QUEUED, STATE_FREE)) {
            long token = this.token;
            try {
                coordinator.fire(token, reason);
            } finally {
                clear();
                coordinator.release(this);
            }
            return true;
        }
        if (Unsafe.cas(this, STATE_OFFSET, STATE_DISCARDED, STATE_FREE)) {
            clear();
            return true;
        }
        return false;
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
        if (Unsafe.cas(this, STATE_OFFSET, STATE_QUEUED, STATE_NEW)) {
            try {
                releaseNew();
            } catch (Throwable cleanupError) {
                if (cleanupError != failure) {
                    failure.addSuppressed(cleanupError);
                }
            }
        }
    }
}
