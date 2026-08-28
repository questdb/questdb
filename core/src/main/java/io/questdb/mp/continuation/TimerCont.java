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

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * "Sleep N milliseconds, then resume" primitive built on {@link TimerShards}. Lets a
 * worker continuation park for a fixed duration without burning a thread or pinning a
 * carrier. Replaces the periodic-poll pattern when the wait is purely time-bound.
 *
 * <p>Usage shape:
 * <pre>
 *   TimerCont t = TimerCont.scheduleAfter(engine.getTimerShards(), clock, 50);
 *   if (!WorkerContinuation.suspend()) {
 *       t.abortContinuation();
 *       // fall back to legacy polling
 *   }
 *   // resumed: either the deadline elapsed or shutdown drained the shard;
 *   // check t.isShuttingDown() before continuing
 * </pre>
 *
 * <p>One-shot: each {@link #scheduleAfter} call allocates a fresh instance with its own
 * CAS lifecycle. No pool is provided; pooling is the caller's job if needed.
 *
 * <p>Loop discipline: callers that re-park inside a loop must check
 * {@link #isShuttingDown()} between iterations and exit instead of re-arming, so the
 * close path can drive the body to completion.
 */
public final class TimerCont implements DelayedFireable {
    private static final int STATE_CANCELLED = 2;
    private static final int STATE_FIRED = 1;
    private static final long STATE_OFFSET = Unsafe.getFieldOffset(TimerCont.class, "state");
    private static final int STATE_PENDING = 0;
    private final MillisecondClock clock;
    private final WorkerContinuation cont;
    private final long deadlineMillis;
    @SuppressWarnings("FieldMayBeFinal")
    private volatile int state = STATE_PENDING;

    private TimerCont(WorkerContinuation cont, MillisecondClock clock, long deadlineMillis) {
        this.cont = cont;
        this.clock = clock;
        this.deadlineMillis = deadlineMillis;
    }

    /**
     * Schedules a fresh timer entry due {@code afterMillis} from now in the supplied
     * shard set. The continuation currently mounted on the calling thread is captured;
     * callers must invoke this from inside a worker continuation (verified by
     * {@link WorkerContinuation#isMounted}).
     *
     * <p>The {@code clock} is used both for the deadline computation here and for the
     * subsequent {@link #getDelay} reads. Callers should pass the same clock that gates
     * their own deadline math so the two cannot diverge under a mocked clock.
     *
     * @throws IllegalStateException when no continuation is mounted on the calling thread
     */
    public static TimerCont scheduleAfter(@NotNull TimerShards shards, @NotNull MillisecondClock clock, long afterMillis) {
        WorkerContinuation cont = WorkerContinuation.current();
        if (cont == null || !WorkerContinuation.isMounted()) {
            throw new IllegalStateException("TimerCont.scheduleAfter requires a mounted WorkerContinuation");
        }
        TimerCont t = new TimerCont(cont, clock, clock.getTicks() + afterMillis);
        shards.register(t);
        return t;
    }

    /**
     * If a {@link WorkerContinuation#suspend()} returned {@code false} after the entry
     * was already registered, marks the cont's resume queue entry as a phantom so the
     * dequeuing peer drops it instead of busy-spinning to remount a still-mounted cont.
     * Mirrors {@code TxnWaiter.abortContinuation}.
     */
    public void abortContinuation() {
        if (!Unsafe.cas(this, STATE_OFFSET, STATE_PENDING, STATE_CANCELLED)) {
            cont.markParkRefused();
        }
    }

    @Override
    public int compareTo(@NotNull Delayed o) {
        return Long.compare(getDelay(TimeUnit.NANOSECONDS), o.getDelay(TimeUnit.NANOSECONDS));
    }

    /**
     * Timer-shard pop. CAS PENDING -> FIRED, then resume the bound cont. The
     * resume is unconditional: even if the cont raced ahead to done state via
     * another path, the dequeuing peer worker silently drops it on remount,
     * so we never strand a body that is still parked.
     */
    @Override
    public void expire() {
        if (Unsafe.cas(this, STATE_OFFSET, STATE_PENDING, STATE_FIRED)) {
            cont.scheduleResume();
        }
    }

    @Override
    public long getDelay(@NotNull TimeUnit unit) {
        return unit.convert(deadlineMillis - clock.getTicks(), TimeUnit.MILLISECONDS);
    }

    /**
     * True once the bound continuation has been flagged as shutting down. Body code
     * that loops on suspend/resume must consult this and exit instead of re-arming a
     * fresh {@link TimerCont}, so engine close can drive the body to completion.
     */
    public boolean isShuttingDown() {
        return cont.isShutdown();
    }

    /**
     * Engine-shutdown drain. Marks the bound continuation as shutting down so the
     * body's loop exits, then transitions PENDING -> CANCELLED and resumes so the
     * cont remounts and observes the flag. Subject to the usual CAS no-op rule: if
     * another path already moved this entry to a terminal state, only the shutdown
     * flag is set; the pending dequeue carries the body to the close-aware exit.
     * If the cont has already completed via another path, the dequeuing peer
     * worker silently drops it on remount.
     */
    @Override
    public void shutdown() {
        cont.shutdown();
        if (Unsafe.cas(this, STATE_OFFSET, STATE_PENDING, STATE_CANCELLED)) {
            cont.scheduleResume();
        }
    }
}
