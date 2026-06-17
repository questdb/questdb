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

import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * A single SQL evaluation parked inside a {@link SeqTxnTracker}, waiting for the tracker's
 * {@code writerTxn} to reach {@link #targetWriterTxn}.
 *
 * <p>Allocated once per {@code wait_wal_table} call and reused across the wake/sleep loop:
 * a fresh instance binds the carrier's {@link WorkerContinuation} via {@link #tryBindCurrent},
 * then each iteration {@link #reset()}s {@link #state} from FIRED back to PENDING and publishes
 * a new target before re-registering. Not pooled across calls -- one allocation per active wait.
 *
 * <p>{@link #state} is a 3-way marker (PENDING / FIRED / CANCELLED). Each concurrent path leaves
 * PENDING with a single CAS, so at most one wins per cycle and {@code cont.scheduleResume()} runs
 * at most once:
 * <ul>
 *   <li>{@code tryFire} (data arrived / table terminal) and {@code expire} (timer pop): CAS to
 *       FIRED, winner schedules the resume.</li>
 *   <li>{@code shutdown} (engine close): CAS to CANCELLED, winner schedules the resume so the body
 *       sees the shutdown flag on remount.</li>
 *   <li>{@code abortContinuation} (yield refused): CAS to CANCELLED, no resume; on a lost CAS marks
 *       parkRefused.</li>
 * </ul>
 * The body tells wake causes apart via its own exit checks ({@code writerTxn}, {@code isSuspended},
 * {@code isDropped}, {@code isShuttingDown}), not the state value.
 *
 * <p>{@link #cancel()} is the lone non-CAS transition: an unconditional terminal write to CANCELLED
 * from {@code WaitWalFunction.getBool}'s finally, once, as the body unwinds -- see its contract.
 *
 * <p>The continuation knows its origin pool's resume sink at construction, so firing need not thread
 * a resume job through the waiter.
 */
public final class TxnWaiter implements DelayedFireable {
    public static final long NO_DELAY = Long.MAX_VALUE;
    public static final int STATE_CANCELLED = 2;
    public static final int STATE_FIRED = 1;
    public static final int STATE_PENDING = 0;
    private static final long STATE_OFFSET = Unsafe.getFieldOffset(TxnWaiter.class, "state");
    private final long targetWriterTxn;
    private final TimerShards timerShards;
    private final long waitIntervalMillis;
    private WorkerContinuation cont;
    private volatile long registeredAtMillis;
    @SuppressWarnings("FieldMayBeFinal")
    private volatile int state = STATE_PENDING;

    public TxnWaiter(@Nullable TimerShards timerShards, long waitIntervalMillis, long targetWriterTxn) {
        this.timerShards = timerShards;
        this.waitIntervalMillis = waitIntervalMillis;
        this.targetWriterTxn = targetWriterTxn;
    }

    public TxnWaiter(long targetWriterTxn, WorkerContinuation cont) {
        this.timerShards = null;
        this.waitIntervalMillis = NO_DELAY;
        this.targetWriterTxn = targetWriterTxn;
        this.cont = cont;
    }

    public void abortContinuation() {
        // The only true cancellation path: the body is bailing to legacy
        // polling because suspend was refused, NOT being resumed. Use
        // STATE_CANCELLED so the state value reflects "this waiter will not
        // see another reset()": every other terminal path (tryFire / expire /
        // shutdown) is a wakeup that the body's reset() flips back to PENDING,
        // but a cancelled waiter stays terminal. If the CAS wins, no
        // scheduleResume has happened and there is no phantom queue entry.
        // If we lose (someone already fired this waiter), a scheduleResume
        // has pushed this cont onto the resume queue while the cont is still
        // mounted here -- mark parkRefused so the dequeuing peer worker drops
        // the phantom dequeue instead of busy-spinning for the duration of the
        // legacy polling fallback below.
        if (!Unsafe.cas(this, STATE_OFFSET, STATE_PENDING, STATE_CANCELLED)) {
            cont.markParkRefused();
        }
    }

    /**
     * Terminal cancel: unconditionally writes CANCELLED -- a plain write, NOT a CAS, and
     * no {@code cont.scheduleResume()} since the body is already unwinding. This lets the
     * next {@code SeqTxnTracker.fireWaiters} walk drop the tracker holder immediately (it
     * re-enqueues only non-CANCELLED waiters) instead of leaving it queued until the timer
     * pops at {@code waitIntervalMillis}.
     *
     * <p>Clobbering any prior state is safe: from PENDING the write is the cancellation;
     * from FIRED it is unobservable -- a fired waiter was already dequeued by
     * {@code fireWaiters}, {@code expire()} is a no-op on terminal states, and the resume
     * already ran. This holds only because {@code cancel()} is the LAST operation on the
     * waiter. Do not call it on a path that may {@link #reset()} or {@link #suspend()} the
     * same waiter again, or move it off the unwinding tail -- that needs the
     * CAS-and-parkRefused discipline of {@link #abortContinuation()} instead.
     *
     * <p>Deliberately does NOT call {@code cont.markParkRefused()}: the FIRED case is the
     * happy-path tail of {@code WaitWalFunction#getBool} on a healthy remounted cont, and
     * parkRefused would poison its NEXT yield ({@code Worker#mountForeignCont} consumes the
     * flag and drops the dequeue), parking the cont forever. See DESIGN_NOTES.md
     * "cancel() in WaitWalFunction.getBool's finally".
     */
    public void cancel() {
        state = STATE_CANCELLED;
    }

    @Override
    public int compareTo(@NotNull Delayed o) {
        return Long.compare(getDelay(TimeUnit.NANOSECONDS), o.getDelay(TimeUnit.NANOSECONDS));
    }

    /**
     * Timer-shard pop. State-driven: if PENDING, CAS to FIRED and resume the body
     * so the loop top observes the timeout. Never reads {@link #registeredAtMillis}; a
     * stale heap entry from a previous {@link #reset()} cycle finds either a fresh
     * PENDING (fires it; the new wait was on the same waiter and is now timed out),
     * or a terminal state (no-op).
     */
    @Override
    public void expire() {
        if (Unsafe.cas(this, STATE_OFFSET, STATE_PENDING, STATE_FIRED)) {
            cont.scheduleResume();
        }
    }

    @Override
    public long getDelay(@NotNull TimeUnit unit) {
        long elapsed = System.currentTimeMillis() - registeredAtMillis;
        return unit.convert(waitIntervalMillis - elapsed, TimeUnit.MILLISECONDS);
    }

    public int getState() {
        return state;
    }

    public long getTargetWriterTxn() {
        return targetWriterTxn;
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
     * continuation. Stamps {@link #registeredAtMillis} for the {@link Delayed} contract
     * and CASes {@link #state} back to PENDING from FIRED; returns {@code true} if the
     * CAS won, meaning the waiter was woken by a previous wakeup path (tryFire / expire
     * / shutdown / abort) and needs the caller to re-register on the tracker queue.
     * Returns {@code false} on the very first reset of a fresh waiter (state already
     * PENDING) - the caller is expected to register unconditionally on the first pass.
     *
     * <p>If a {@link TimerShards} reference is bound, also registers this waiter into a
     * timer shard. A previous cycle may have left a stale entry in the heap (its
     * CAS-to-FIRED won, leaving the entry sorted by an older deadline). That stale
     * entry is harmless: {@link #expire()} is state-driven, never consults the
     * registration timestamp, and the per-instance CAS makes a double-fire impossible.
     */
    public boolean reset() {
        this.registeredAtMillis = System.currentTimeMillis();
        boolean resumed = Unsafe.cas(this, STATE_OFFSET, STATE_FIRED, STATE_PENDING);
        if (timerShards != null) {
            timerShards.register(this);
        }
        return resumed;
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

    public void tryFire() {
        if (Unsafe.cas(this, STATE_OFFSET, STATE_PENDING, STATE_FIRED)) {
            cont.scheduleResume();
        }
        // cancelled waiters were already enqueued by the canceller; drop on the floor
    }
}
