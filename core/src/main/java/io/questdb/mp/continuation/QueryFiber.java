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

import io.questdb.std.Os;

/**
 * A pooled, non-completing continuation that runs {@link QueryTask}s. The body is a
 * task-runner loop that never returns while the fiber is live, so the underlying
 * native stack chunk is allocated once and reused across every task and every park
 * -- unlike the worker loop's fresh-continuation-per-park pattern, which allocates a
 * new chunk each time. This is what makes suspended queries allocation-free at
 * steady state.
 *
 * <p>A fiber yields back to its mounting driver for one of two reasons, recorded in
 * the yield-reason slot the driver reads after {@link #run()} returns:
 * <ul>
 *   <li>{@link #YIELD_FREE} -- the step ended (task done, parked-for-write, or
 *       errored); the driver reclaims the fiber to its {@link QueryFiberPool} via
 *       {@link #reclaimIfIdle(WorkerContinuation)}.</li>
 *   <li>{@link #YIELD_WAIT} -- a wait function (wait_wal_table/sleep) froze the
 *       fiber deep inside {@link QueryTask#runStep()}; the registered waiter owns
 *       the resume and will push this fiber back via
 *       {@link WorkerContinuation#scheduleResume()}. The driver must not reclaim
 *       it.</li>
 * </ul>
 * The body stamps {@code YIELD_WAIT} before each step so a deep freeze needs no
 * cooperation from the wait function; it stamps {@code YIELD_FREE} only at its own
 * end-of-step yield. The slot needs no volatile: it has a single writer (the carrier
 * the fiber is mounted on) and is read by the thread that called {@code run()} after
 * it returns -- the yield/run boundary supplies the fence, same as the handoff slot.
 *
 * <p>The body discovers its own instance via {@link WorkerContinuation#current()}
 * (set by {@code run()}), which lets the task-runner be a static, non-capturing
 * method reference -- resolving the this-before-super constructor cycle without
 * weakening the parent's finals.
 *
 * <p>On {@link #isShutdown()} the loop returns, completing the continuation and
 * releasing the native chunk deterministically.
 */
public final class QueryFiber extends WorkerContinuation {
    public static final int YIELD_FREE = 0;
    public static final int YIELD_WAIT = 1;
    private final QueryFiberPool pool;
    private QueryTask assignedTask;
    // Embedded wait primitives, lazily created and reused across every wait this
    // fiber runs: a fiber runs one wait at a time, and each wait ends terminal, so
    // stale references from a previous wait no-op (see TxnWaiter/TimerCont docs).
    // Touched only while mounted; the mount boundaries supply the fences.
    private TimerCont timerCont;
    private TxnWaiter txnWaiter;
    private int yieldReason = YIELD_WAIT;

    QueryFiber(QueryFiberPool pool, ContinuationSink resumeSink) {
        super(QueryFiber::taskRunnerLoop, resumeSink);
        this.pool = pool;
    }

    /**
     * Driver-side reclaim hook: when the given continuation is a fiber that has
     * free-yielded (its step ended) or completed (shutdown unwind), releases it to
     * its pool and returns {@code true} -- the driver stops treating it as a parked
     * continuation. Returns {@code false} for non-fibers and for fibers frozen in a
     * wait, whose waiter owns the resume. Call this after every mount site's
     * {@code run()} returns; without it, free fibers leak off-queue.
     */
    public static boolean reclaimIfIdle(WorkerContinuation cont) {
        if (cont instanceof QueryFiber fiber && (fiber.isDone() || fiber.yieldReason == YIELD_FREE)) {
            fiber.pool.release(fiber);
            return true;
        }
        return false;
    }

    public int getYieldReason() {
        return yieldReason;
    }

    /**
     * Hands a claimed task to this fiber. Only legal on an unassigned fiber (fresh
     * from the pool, or reclaimed after a free-yield); the caller must {@link #run()}
     * it on the same thread, which supplies the ordering for the plain field write.
     */
    void assign(QueryTask task) {
        assert assignedTask == null : "fiber already assigned";
        assignedTask = task;
    }

    TimerCont getTimerCont() {
        if (timerCont == null) {
            timerCont = new TimerCont(this);
        }
        return timerCont;
    }

    TxnWaiter getTxnWaiter() {
        if (txnWaiter == null) {
            txnWaiter = new TxnWaiter();
        }
        return txnWaiter;
    }

    private static void taskRunnerLoop() {
        final QueryFiber self = (QueryFiber) current();
        while (!self.isShutdown()) {
            final QueryTask task = self.assignedTask;
            if (task == null) {
                self.yieldReason = YIELD_FREE;
                if (!suspend()) {
                    // Pure-Java frames cannot pin the carrier on this JDK; if the
                    // yield is ever refused regardless, back off and retry.
                    Os.pause();
                }
                continue;
            }
            // Default reason: a wait function freezing us deep inside runStep()
            // must read as "parked, the waiter owns the resume".
            self.yieldReason = YIELD_WAIT;
            boolean done = false;
            Throwable error = null;
            try {
                done = task.runStep();
            } catch (BackpressureSignal ignore) {
                // equivalent to a false return; the task records the wakeup to arm
            } catch (Throwable th) {
                error = th;
            }
            self.assignedTask = null;
            boolean isTerminal = true;
            if (error != null) {
                try {
                    task.onError(error);
                } catch (Throwable ignore) {
                    // the fiber must survive a misbehaving hook
                }
                task.markDone();
            } else if (done) {
                task.markDone();
            } else {
                isTerminal = false;
                // Gate first, hook second: the wakeup registration inside onParked()
                // may fire immediately, and the gate must already accept the
                // re-schedule.
                task.releaseToIdle();
                try {
                    task.onParked();
                } catch (Throwable ignore) {
                    // the fiber must survive a misbehaving hook
                }
            }
            if (isTerminal) {
                try {
                    task.onDone();
                } catch (Throwable ignore) {
                    // the fiber must survive a misbehaving hook
                }
            }
            self.yieldReason = YIELD_FREE;
            if (!suspend()) {
                Os.pause();
            }
        }
        // a task staged between launch and first mount never got its step; run
        // its terminal hooks so adapter resources release
        final QueryTask task = self.assignedTask;
        if (task != null) {
            self.assignedTask = null;
            task.markDone();
            try {
                task.onDone();
            } catch (Throwable ignore) {
                // the fiber must survive a misbehaving hook
            }
        }
    }
}
