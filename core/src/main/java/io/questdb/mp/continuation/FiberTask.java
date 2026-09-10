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
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

public abstract class FiberTask {
    public static final int SIGNAL_CANCEL = 2;
    public static final int SIGNAL_DISCONNECT = 3;
    public static final int SIGNAL_READY = 1;
    public static final int STATE_ARMING = 2;
    public static final int STATE_ARMING_CANCELLED = 4;
    public static final int STATE_ARMING_DISCONNECTED = 5;
    public static final int STATE_ARMING_SIGNALLED = 3;
    public static final int STATE_CANCELLED = 7;
    public static final int STATE_DONE = 6;
    public static final int STATE_IDLE = 0;
    public static final int STATE_OWNED = 1;
    public static final int STATE_SHIFT = 3;
    static final int CLAIM_ALREADY_OWNED = 1;
    static final int CLAIM_LAUNCHED = 0;
    static final int CLAIM_SIGNALLED = 4;
    static final int CLAIM_STALE = 2;
    static final int CLAIM_TERMINAL = 3;
    static final int PARK_CANCEL = 2;
    static final int PARK_DISCONNECT = 3;
    static final int PARK_IDLE = 0;
    static final int PARK_RELAUNCH = 1;
    private static final long MAX_INCARNATION = Long.MAX_VALUE >>> STATE_SHIFT;
    private static final long SCHEDULE_STATE_OFFSET = Unsafe.getFieldOffset(FiberTask.class, "scheduleState");
    private static final long STATE_MASK = 7;
    private FiberCancellationSignal cancellationSignal;
    private long cancellationSignalGeneration = CancellationBinding.NO_GENERATION;
    private long cancellationSignalIncarnation;
    @SuppressWarnings("FieldMayBeFinal")
    private volatile long scheduleState = pack(1);

    public @Nullable FiberCancellationSignal getCancellationSignal() {
        return null;
    }

    public final long getIncarnation() {
        return incarnation(scheduleState);
    }

    public final int getScheduleState() {
        return state(scheduleState);
    }

    public final boolean isActive(long expectedIncarnation) {
        final long current = scheduleState;
        if (incarnation(current) != expectedIncarnation) {
            return false;
        }

        final int state = state(current);
        return state != STATE_DONE && state != STATE_CANCELLED;
    }

    public final boolean isCancelled() {
        return state(scheduleState) == STATE_CANCELLED;
    }

    public final boolean isDone() {
        final int state = state(scheduleState);
        return state == STATE_DONE || state == STATE_CANCELLED;
    }

    public final boolean isIdle(long expectedIncarnation) {
        final long current = scheduleState;
        return incarnation(current) == expectedIncarnation && state(current) == STATE_IDLE;
    }

    public final void reopen() {
        if (!tryReopen()) {
            throw nonTerminalReopen(state(scheduleState));
        }
    }

    @TestOnly
    public final void setScheduleStateForTesting(int expectedState, int targetState) {
        while (true) {
            final long current = scheduleState;
            if (state(current) != expectedState) {
                throw new IllegalStateException(
                        "unexpected fiber task state [expected=" + expectedState
                                + ", actual=" + state(current)
                                + ']'
                );
            }
            if (Unsafe.cas(this, SCHEDULE_STATE_OFFSET, current, withState(current, targetState))) {
                return;
            }
        }
    }

    public final boolean signalAxisA(int reason) {
        return signalAxisA(getIncarnation(), reason);
    }

    public final boolean signalAxisA(long expectedIncarnation, int reason) {
        final int targetState = signalState(reason);
        while (true) {
            final long current = scheduleState;
            if (incarnation(current) != expectedIncarnation) {
                return false;
            }
            final int state = state(current);
            if (!isArmingState(state)) {
                return false;
            }
            if (state >= targetState
                    || Unsafe.cas(this, SCHEDULE_STATE_OFFSET, current, withState(current, targetState))) {
                return true;
            }
        }
    }

    public final boolean tryCancel() {
        return tryCancel(getIncarnation());
    }

    public final boolean tryCancel(long expectedIncarnation) {
        while (true) {
            final long current = scheduleState;
            if (incarnation(current) != expectedIncarnation) {
                return false;
            }
            final int state = state(current);
            if (state == STATE_IDLE) {
                if (Unsafe.cas(this, SCHEDULE_STATE_OFFSET, current, withState(current, STATE_CANCELLED))) {
                    return true;
                }
            } else if (isArmingState(state)) {
                if (state >= STATE_ARMING_CANCELLED
                        || Unsafe.cas(
                        this,
                        SCHEDULE_STATE_OFFSET,
                        current,
                        withState(current, STATE_ARMING_CANCELLED)
                )) {
                    return true;
                }
            } else {
                return false;
            }
        }
    }

    public final boolean tryCancelIdle(long expectedIncarnation) {
        while (true) {
            final long current = scheduleState;
            if (incarnation(current) != expectedIncarnation || state(current) != STATE_IDLE) {
                return false;
            }
            if (Unsafe.cas(this, SCHEDULE_STATE_OFFSET, current, withState(current, STATE_CANCELLED))) {
                return true;
            }
        }
    }

    public final boolean tryReopen() {
        while (true) {
            final long current = scheduleState;
            final int state = state(current);
            if (state != STATE_DONE && state != STATE_CANCELLED) {
                return false;
            }
            final long incarnation = incarnation(current);
            if (incarnation == MAX_INCARNATION) {
                throw new IllegalStateException("fiber task incarnation exhausted");
            }
            if (Unsafe.cas(this, SCHEDULE_STATE_OFFSET, current, pack(incarnation + 1))) {
                return true;
            }
        }
    }

    private static long incarnation(long scheduleState) {
        return scheduleState >>> STATE_SHIFT;
    }

    private static IllegalStateException invalidArmingState(int state) {
        return new IllegalStateException("invalid fiber task arming state [state=" + state + ']');
    }

    private static IllegalArgumentException invalidSignal(int reason) {
        return new IllegalArgumentException("invalid Axis-A signal [reason=" + reason + ']');
    }

    private static boolean isArmingState(int state) {
        return state >= STATE_ARMING && state <= STATE_ARMING_DISCONNECTED;
    }

    private static IllegalStateException nonTerminalReopen(int state) {
        return new IllegalStateException("cannot reopen non-terminal fiber task [state=" + state + ']');
    }

    private static long pack(long incarnation) {
        return (incarnation << STATE_SHIFT) | FiberTask.STATE_IDLE;
    }

    private static int signalState(int reason) {
        return switch (reason) {
            case SIGNAL_READY -> STATE_ARMING_SIGNALLED;
            case SIGNAL_CANCEL -> STATE_ARMING_CANCELLED;
            case SIGNAL_DISCONNECT -> STATE_ARMING_DISCONNECTED;
            default -> throw invalidSignal(reason);
        };
    }

    private static int state(long scheduleState) {
        return (int) (scheduleState & STATE_MASK);
    }

    private static void validateCancellationBinding(
            @Nullable FiberCancellationSignal cancellationSignal,
            long cancellationSignalGeneration
    ) {
        if (cancellationSignal == null) {
            if (cancellationSignalGeneration != CancellationBinding.NO_GENERATION) {
                throw new IllegalArgumentException("null cancellation signal cannot have a generation");
            }
        } else if (cancellationSignalGeneration < 0) {
            throw new IllegalArgumentException("fiber cancellation generation must be non-negative");
        }
    }

    private static long withState(long scheduleState, int state) {
        return (scheduleState & ~STATE_MASK) | state;
    }

    final boolean abortArming() {
        while (true) {
            final long current = scheduleState;
            final int state = state(current);
            if (!isArmingState(state)) {
                return state == STATE_OWNED;
            }
            if (Unsafe.cas(this, SCHEDULE_STATE_OFFSET, current, withState(current, STATE_OWNED))) {
                return true;
            }
        }
    }

    final boolean beginArming() {
        while (true) {
            final long current = scheduleState;
            if (state(current) != STATE_OWNED) {
                return false;
            }
            if (Unsafe.cas(this, SCHEDULE_STATE_OFFSET, current, withState(current, STATE_ARMING))) {
                return true;
            }
        }
    }

    final void captureCancellationBinding() {
        final long incarnation = getIncarnation();
        if (cancellationSignalIncarnation != incarnation) {
            final FiberCancellationSignal cancellationSignal = getCancellationSignal();
            final long cancellationSignalGeneration = cancellationSignal != null
                    ? getCancellationSignalGeneration(cancellationSignal)
                    : CancellationBinding.NO_GENERATION;
            validateCancellationBinding(cancellationSignal, cancellationSignalGeneration);
            this.cancellationSignal = cancellationSignal;
            this.cancellationSignalGeneration = cancellationSignalGeneration;
            cancellationSignalIncarnation = incarnation;
        }
    }

    final int claim(long expectedIncarnation) {
        while (true) {
            final long current = scheduleState;
            if (incarnation(current) != expectedIncarnation) {
                return CLAIM_STALE;
            }
            final int state = state(current);
            if (state == STATE_IDLE) {
                if (Unsafe.cas(this, SCHEDULE_STATE_OFFSET, current, withState(current, STATE_OWNED))) {
                    return CLAIM_LAUNCHED;
                }
            } else if (isArmingState(state)) {
                if (state != STATE_ARMING
                        || Unsafe.cas(
                        this,
                        SCHEDULE_STATE_OFFSET,
                        current,
                        withState(current, STATE_ARMING_SIGNALLED)
                )) {
                    return CLAIM_SIGNALLED;
                }
            } else if (state == STATE_OWNED) {
                return CLAIM_ALREADY_OWNED;
            } else {
                return CLAIM_TERMINAL;
            }
        }
    }

    final @Nullable FiberCancellationSignal getBoundCancellationSignal() {
        return cancellationSignal;
    }

    final long getBoundCancellationSignalGeneration() {
        return cancellationSignalGeneration;
    }

    protected long getCancellationSignalGeneration(FiberCancellationSignal cancellationSignal) {
        return cancellationSignal.getGeneration();
    }

    final void markCancelledFromOwned() {
        while (true) {
            final long current = scheduleState;
            if (state(current) != STATE_OWNED) {
                throw new IllegalStateException("fiber task is not owned");
            }
            if (Unsafe.cas(this, SCHEDULE_STATE_OFFSET, current, withState(current, STATE_CANCELLED))) {
                return;
            }
        }
    }

    final void markDoneFromOwned() {
        while (true) {
            final long current = scheduleState;
            if (state(current) != STATE_OWNED) {
                throw new IllegalStateException("fiber task is not owned");
            }
            if (Unsafe.cas(this, SCHEDULE_STATE_OFFSET, current, withState(current, STATE_DONE))) {
                return;
            }
        }
    }

    final void notifyAbandoned() {
        onAbandoned();
    }

    final void notifyDone() {
        onDone();
    }

    final void notifyError(Throwable th) {
        onError(th);
    }

    protected void onAbandoned() {
    }

    protected void onDone() {
    }

    protected void onError(Throwable th) {
    }

    protected void onParkPrepare() {
    }

    protected void onParked() {
    }

    final void preparePark() {
        onParkPrepare();
    }

    final void publishPark() {
        onParked();
    }

    final int resolveArming() {
        while (true) {
            final long current = scheduleState;
            final int state = state(current);
            final int targetState;
            final int result = switch (state) {
                case STATE_ARMING -> {
                    targetState = STATE_IDLE;
                    yield PARK_IDLE;
                }
                case STATE_ARMING_SIGNALLED -> {
                    targetState = STATE_OWNED;
                    yield PARK_RELAUNCH;
                }
                case STATE_ARMING_CANCELLED -> {
                    targetState = STATE_CANCELLED;
                    yield PARK_CANCEL;
                }
                case STATE_ARMING_DISCONNECTED -> {
                    targetState = STATE_CANCELLED;
                    yield PARK_DISCONNECT;
                }
                default -> throw invalidArmingState(state);
            };
            if (Unsafe.cas(this, SCHEDULE_STATE_OFFSET, current, withState(current, targetState))) {
                return result;
            }
        }
    }

    protected abstract boolean runStep();

    final void updateCancellationBinding(
            @Nullable FiberCancellationSignal cancellationSignal,
            long cancellationSignalGeneration
    ) {
        validateCancellationBinding(cancellationSignal, cancellationSignalGeneration);
        final long incarnation = getIncarnation();
        if (cancellationSignalIncarnation != incarnation) {
            throw new IllegalStateException("fiber task cancellation binding is stale");
        }
        this.cancellationSignal = cancellationSignal;
        this.cancellationSignalGeneration = cancellationSignalGeneration;
    }
}
