/*******************************************************************************
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

/**
 * The reified unit of resumable query work: one connection's (or one internal job's)
 * current resumable work, run in steps on a pooled {@link QueryFiber}. Entry-point
 * adapters (PG pipeline sync, HTTP resumeSend, QWP streamResults, mat-view refresh)
 * subclass this and implement {@link #runStep()} by calling their existing resume
 * logic.
 *
 * <p>The schedule gate guarantees a task is launched by at most one trigger at a
 * time. States and who moves them:
 * <ul>
 *   <li>IDLE -&gt; ENQUEUED -&gt; RUNNING: an external trigger (fd event, CREDIT frame,
 *       query arrival) via {@link QueryFiberPool#launch(QueryTask)}, which claims
 *       both transitions back to back before assigning a fiber. The first CAS
 *       refuses duplicate triggers -- a level-triggered WRITE event cannot
 *       double-launch; a {@link #tryCancel()} racing between the two CASes makes
 *       the launch return without a fiber.</li>
 *   <li>RUNNING -&gt; IDLE or DONE: the fiber, after the step ends. The transition to
 *       IDLE happens BEFORE {@link #onParked()} so the wakeup trigger is armed only
 *       once the gate accepts a re-launch -- this ordering is what makes a lost
 *       wakeup impossible.</li>
 *   <li>IDLE/ENQUEUED -&gt; CANCELLED: {@link #tryCancel()}. A RUNNING task cancels
 *       cooperatively through its circuit breaker, as queries do today.</li>
 * </ul>
 *
 * <p>Park-state mapping: a task parked on its sink (socket write, credit) is IDLE --
 * its protocol state object records why, and only the matching external event
 * re-schedules it. A task parked on a wait (wait_wal_table/sleep froze the fiber
 * mid-step) stays RUNNING for the whole park, so spurious re-schedule attempts are
 * refused by the gate; the frozen fiber resumes via
 * {@link WorkerContinuation#scheduleResume()} instead. A task is therefore in at
 * most one park state at a time, and exactly one trigger can wake it.
 */
public abstract class QueryTask {
    public static final int STATE_CANCELLED = 4;
    public static final int STATE_DONE = 3;
    public static final int STATE_ENQUEUED = 1;
    public static final int STATE_IDLE = 0;
    public static final int STATE_RUNNING = 2;
    private static final long SCHEDULE_STATE_OFFSET = Unsafe.getFieldOffset(QueryTask.class, "scheduleState");
    @SuppressWarnings("FieldMayBeFinal")
    private volatile int scheduleState = STATE_IDLE;

    public final int getScheduleState() {
        return scheduleState;
    }

    public final boolean isCancelled() {
        return scheduleState == STATE_CANCELLED;
    }

    public final boolean isDone() {
        return scheduleState == STATE_DONE;
    }

    /**
     * Makes a terminal task schedulable again, for per-connection task reuse across
     * queries (the task is "this connection's current resumable work"). Only legal
     * on a DONE or CANCELLED task.
     */
    public final void reopen() {
        boolean ok = Unsafe.cas(this, SCHEDULE_STATE_OFFSET, STATE_DONE, STATE_IDLE)
                || Unsafe.cas(this, SCHEDULE_STATE_OFFSET, STATE_CANCELLED, STATE_IDLE);
        assert ok : "reopen: task not terminal";
    }

    /**
     * Cancels a task that is not currently mounted. Returns {@code true} if this
     * call performed the cancellation; {@code false} when the task is RUNNING (use
     * the circuit breaker) or already terminal. A cancelled task still ENQUEUED is
     * makes its in-flight launch return without acquiring a fiber.
     */
    public final boolean tryCancel() {
        return Unsafe.cas(this, SCHEDULE_STATE_OFFSET, STATE_IDLE, STATE_CANCELLED)
                || Unsafe.cas(this, SCHEDULE_STATE_OFFSET, STATE_ENQUEUED, STATE_CANCELLED);
    }

    /**
     * Invoked by the fiber instead of a step when it shuts down with this task
     * staged but never run: the launch happened, but the fiber was marked for
     * shutdown before its first mount. Adapters release what a normal step's
     * terminal path would have released -- e.g. disconnect the checked-out
     * connection context, which otherwise leaks with its socket. Runs before
     * {@link #onDone()}. Must not throw.
     */
    protected void onAbandoned() {
    }

    /**
     * Invoked by the fiber after the gate reached DONE, on both the success and
     * the error path. This is the adapter's completion hook: release or recycle
     * per-task resources here rather than inside {@link #runStep()}, because the
     * gate transition happens after the step returns and a self-recycling step
     * would race it. Must not throw; the fiber survives a misbehaving hook
     * regardless.
     */
    protected void onDone() {
    }

    /**
     * Invoked by the fiber when {@link #runStep()} threw anything other than
     * {@link BackpressureSignal}; the task is marked DONE afterwards (and
     * {@link #onDone()} runs after that). Adapters report the protocol-level
     * error here. Must not throw; the fiber survives a misbehaving hook
     * regardless.
     */
    protected void onError(Throwable th) {
    }

    /**
     * Invoked by the fiber after every non-terminal step (a {@code false} return or
     * a {@link BackpressureSignal}), strictly AFTER the gate returned to IDLE.
     * Adapters arm the external wakeup trigger here -- register the fd for
     * READ/WRITE, arm a credit wait -- with the step recording which trigger in its
     * own state; because the gate already accepts a re-schedule, the wakeup the
     * registration produces can never be lost. Must not throw; the fiber survives a
     * misbehaving hook regardless.
     */
    protected void onParked() {
    }

    /**
     * Advances the query until it completes, blocks on its sink, or suspends on a
     * wait. Returns {@code true} when the task is finished (no more steps);
     * {@code false} when more steps remain but only an external event may schedule
     * them (e.g. a suspended PG portal awaiting the next Execute). Raises
     * {@link BackpressureSignal} for an axis-A park. An axis-B park
     * (wait_wal_table/sleep) freezes the whole fiber inside this call and resumes
     * transparently -- from the adapter's point of view the call just took longer.
     */
    protected abstract boolean runStep() throws BackpressureSignal;

    final void markDone() {
        boolean ok = Unsafe.cas(this, SCHEDULE_STATE_OFFSET, STATE_RUNNING, STATE_DONE);
        assert ok : "markDone: gate not RUNNING";
    }

    final void releaseToIdle() {
        boolean ok = Unsafe.cas(this, SCHEDULE_STATE_OFFSET, STATE_RUNNING, STATE_IDLE);
        assert ok : "releaseToIdle: gate not RUNNING";
    }

    final boolean tryClaimRun() {
        return Unsafe.cas(this, SCHEDULE_STATE_OFFSET, STATE_ENQUEUED, STATE_RUNNING);
    }

    final boolean tryEnqueue() {
        return Unsafe.cas(this, SCHEDULE_STATE_OFFSET, STATE_IDLE, STATE_ENQUEUED);
    }
}
