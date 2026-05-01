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

package io.questdb.cairo.wal.seq;

import io.questdb.mp.DelayedFireable;
import io.questdb.mp.TimerShards;
import io.questdb.mp.WorkerContinuation;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * Represents a single SQL evaluation parked inside a {@link SeqTxnTracker}, waiting for
 * the tracker's {@code writerTxn} to reach {@link #targetWriterTxn}.
 *
 * <p>Allocated once per {@code wait_wal_table} call and reused across the wake/sleep
 * loop via {@link #reset(long, long)}: a fresh instance binds the carrier's
 * {@link WorkerContinuation} via {@link #tryBindCurrent}, then each iteration calls
 * reset to clear {@link #state} back to PENDING and publish a new target/delay
 * before re-registering. The waiter is not pooled across calls; the cost is one
 * allocation per active wait.
 *
 * <p>The {@link #state} field is a CAS'd 3-way marker. Only one concurrent thread wins
 * the CAS from PENDING to FIRED or CANCELLED, so {@code cont.scheduleResume()} is invoked
 * at most once per wait.
 *
 * <p>The continuation knows its origin pool's resume sink at construction; firing/cancelling
 * therefore does not need to thread a resume job through the waiter.
 */
public final class TxnWaiter implements DelayedFireable {
    public static final long NO_DELAY = Long.MAX_VALUE;
    public static final int STATE_CANCELLED = 2;
    public static final int STATE_FIRED = 1;
    public static final int STATE_PENDING = 0;
    static final long STATE_OFFSET = Unsafe.getFieldOffset(TxnWaiter.class, "state");
    private final TimerShards timerShards;
    volatile long deadlineMillis = NO_DELAY;
    volatile int state = STATE_PENDING;
    long targetWriterTxn;
    private WorkerContinuation cont;

    public TxnWaiter() {
        this(null);
    }

    public TxnWaiter(@Nullable TimerShards timerShards) {
        this.timerShards = timerShards;
    }

    public TxnWaiter(long targetWriterTxn, WorkerContinuation cont) {
        this.timerShards = null;
        this.targetWriterTxn = targetWriterTxn;
        this.cont = cont;
    }

    public void abortContinuation() {
        // Try to cancel the waiter first: if PENDING -> CANCELLED
        // wins, no scheduleResume happens and there is no phantom queue
        // entry. If tryCancel loses (waiter already FIRED, or the timeout
        // job got there first), a scheduleResume has pushed this cont onto
        // the resume queue while the cont is still mounted here -- mark
        // parkRefused so ContinuationQueue.run drops the phantom dequeue
        // instead of burning a peer carrier for the duration of the legacy
        // polling fallback below.
        if (!Unsafe.cas(this, STATE_OFFSET, STATE_PENDING, STATE_CANCELLED)) {
            cont.markParkRefused();
        }
    }

    @Override
    public int compareTo(@NotNull Delayed o) {
        return Long.compare(getDelay(TimeUnit.NANOSECONDS), o.getDelay(TimeUnit.NANOSECONDS));
    }

    /**
     * Timer-shard pop. State-driven: if PENDING, CAS to CANCELLED and resume the body
     * so the loop top observes the timeout. Never reads {@link #deadlineMillis}; a stale
     * heap entry from a previous {@link #reset(long, long)} cycle finds either a fresh
     * PENDING (cancels it; the new wait was on the same waiter and is now timed out),
     * or a terminal state (no-op).
     */
    @Override
    public void expire() {
        if (Unsafe.cas(this, STATE_OFFSET, STATE_PENDING, STATE_CANCELLED)) {
            cont.scheduleResume();
        }
    }

    @Override
    public long getDelay(@NotNull TimeUnit unit) {
        return unit.convert(deadlineMillis - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }

    public boolean isCancelled() {
        return state == STATE_CANCELLED;
    }

    public boolean isFired() {
        return state == STATE_FIRED;
    }

    /**
     * Returns {@code true} if the bound continuation's owning context is closing.
     * Suspending callers must check this between waits and exit instead of re-parking,
     * so the close path can drive the body through the cont loop's tail suspend.
     */
    public boolean isShuttingDown() {
        return cont.isShutdown();
    }

    /**
     * Resets this waiter for reuse across the wake/sleep loop, keeping the bound
     * continuation. Publishes the new fields and clears {@link #state} back to PENDING;
     * the volatile write on {@code state} synchronizes the reset so any observer that
     * reads state == PENDING sees the refreshed fields.
     *
     * <p>The {@code delayMillis} argument is relative: it is added to
     * {@link System#currentTimeMillis()} to form the absolute deadline used by the
     * {@link Delayed} contract. Pass {@link #NO_DELAY} to opt out of timer-shard
     * registration entirely.
     *
     * <p>If a {@link TimerShards} reference is bound and the delay is finite, also
     * registers this waiter into a timer shard. A previous cycle may have left a stale
     * entry in the heap (its {@code tryFire} won, leaving the entry sorted by an older
     * deadline). That stale entry is harmless: {@link #expire()} is state-driven, never
     * consults {@code deadlineMillis}, and the per-instance CAS makes a double-fire
     * impossible.
     */
    public void reset(long targetWriterTxn, long delayMillis) {
        this.targetWriterTxn = targetWriterTxn;
        this.deadlineMillis = delayMillis == NO_DELAY ? NO_DELAY : System.currentTimeMillis() + delayMillis;
        this.state = STATE_PENDING;
        if (timerShards != null && delayMillis != NO_DELAY) {
            timerShards.register(this);
        }
    }

    public void scheduleResume() {
        cont.scheduleResume();
    }

    /**
     * Engine-shutdown path. Marks the bound continuation as shutting down so the
     * body's wake loop exits instead of re-parking, then transitions PENDING ->
     * CANCELLED and schedules a resume so a worker remounts the cont and observes
     * the flag. If state is already FIRED or CANCELLED a {@code scheduleResume}
     * has already been issued; setting the shutdown flag alone is enough -- the
     * pending dequeue will see it on remount.
     */
    public void shutdown() {
        cont.shutdown();
        if (Unsafe.cas(this, STATE_OFFSET, STATE_PENDING, STATE_CANCELLED)) {
            cont.scheduleResume();
        }
    }

    public boolean suspend() {
        if (cont == null) {
            throw new IllegalStateException("Cannot suspend TxnWaiter: no continuation bound");
        }
        if (!WorkerContinuation.suspend()) {
            abortContinuation();
            return false;
        }
        return true;
    }

    /**
     * Binds this waiter to the {@link WorkerContinuation} currently mounted on the calling
     * thread, if any. Returns {@code true} on success; {@code false} when no cont is mounted
     * in {@link WorkerContinuation#SCOPE} and callers must fall back to legacy polling.
     */
    public boolean tryBindCurrent() {
        WorkerContinuation c = WorkerContinuation.current();
        if (c == null || !WorkerContinuation.isMounted()) {
            return false;
        }
        this.cont = c;
        return true;
    }

    /**
     * Attempts to transition this waiter from PENDING to CANCELLED. Returns {@code true}
     * if the CAS won, in which case the caller is responsible for calling
     * {@code cont.scheduleResume()} so the parked body can observe the cancellation.
     */
    public boolean tryCancel() {
        return Unsafe.cas(this, STATE_OFFSET, STATE_PENDING, STATE_CANCELLED);
    }

    public void tryFire() {
        if (Unsafe.cas(this, STATE_OFFSET, STATE_PENDING, STATE_FIRED)) {
            cont.scheduleResume();
        }
        // cancelled waiters were already enqueued by the canceller; drop on the floor
    }
}
