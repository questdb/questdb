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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewState;
import io.questdb.mp.Job;
import io.questdb.std.Os;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;

/**
 * Shared driver helpers for the live view tests. Every test in this package advances a live view by
 * hand-driving a {@link LiveViewRefreshJob} rather than running a worker pool, so each of them needs
 * the same three loops. They used to be copy-pasted into ten test classes, where the backfill bound
 * had drifted to three different values (500 / 1000 / 2000).
 * <p>
 * The important part is not the dedup: it is that {@link #driveRefreshToQuiescence} and
 * {@link #driveBackfillToCompletion} now <b>fail</b> when they exhaust their pass budget. The
 * copy-pasted versions fell out of the loop silently and let the test carry on against a view that
 * had never converged, so a refresh that stalled surfaced hundreds of lines later as an unexplained
 * row-content mismatch - or, if the stalled view happened to hold the right rows already, as a pass.
 */
public abstract class AbstractLiveViewTest extends AbstractCairoTest {

    // Enough passes for the slowest backfill in the suite (the parquet base test, which used 2000
    // when the helper was still copy-pasted). The loop exits as soon as the view leaves the
    // BACKFILLING state, so a generous bound costs nothing on the tests that converge quickly.
    protected static final int BACKFILL_COMPLETION_PASSES = 2000;
    // Longer than the 100ms FLUSH EVERY the tests declare, so one pass always crosses a flush
    // deadline rather than leaving the view waiting on the clock.
    protected static final long CLOCK_ADVANCE_MICROS = 250_000;
    protected static final int REFRESH_QUIESCENCE_PASSES = 512;

    /**
     * Runs the job until it reports no more work, up to a bounded burst. Unlike the two drive*
     * helpers this is not an await - the caller loops over it - so exhausting the bound is not a
     * failure.
     */
    protected static boolean drainJob(Job job) {
        boolean any = false;
        for (int i = 0; i < 64 && job.run(); i++) {
            any = true;
        }
        return any;
    }

    /**
     * Spins until {@code thread} is observed inside a frame for {@code methodName}, and fails if it
     * is not within {@code timeoutMs}.
     * <p>
     * For threads that block by busy-spinning rather than parking - {@code fenceRefresh} spins on
     * {@link Os#pause()} - the thread stays RUNNABLE, so {@link Thread#getState()} is no use as a
     * fence and neither is a fixed sleep: a test that just sleeps and then asserts the thread has not
     * finished passes trivially if the thread had not started yet.
     */
    protected static void awaitThreadInMethod(Thread thread, String methodName, long timeoutMs) {
        final long deadlineNanos = System.nanoTime() + timeoutMs * 1_000_000L;
        do {
            for (StackTraceElement frame : thread.getStackTrace()) {
                if (methodName.equals(frame.getMethodName())) {
                    return;
                }
            }
            Os.pause();
        } while (System.nanoTime() < deadlineNanos);
        Assert.fail("thread '" + thread.getName() + "' was never observed inside " + methodName
                + "() within " + timeoutMs + "ms");
    }

    /**
     * Asserts the named view refreshed without a single faulting cycle.
     * <p>
     * Any test whose oracle compares the live view against a from-base recompute of its own SELECT
     * needs this. A refresh fault does not fail the cycle: {@code handleRefreshFailure} recomputes
     * the window from the applied base, calls {@code recordRefreshSuccess()} and returns, so the view
     * converges on the recompute - which is precisely what such an oracle compares it against. The
     * comparison therefore passes whether the incremental path worked or faulted on every commit and
     * rebuilt itself each time. Only the fault count can tell those apart.
     */
    protected void assertNoRefreshFaults(String viewName) {
        LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance(viewName);
        Assert.assertNotNull("live view '" + viewName + "' is not registered", instance);
        Assert.assertEquals(
                "live view '" + viewName + "' refresh cycles must not fault: a fault self-heals into a"
                        + " full recompute from the applied base, so a recompute-based oracle passes either way",
                0L,
                instance.getRefreshFaultCount()
        );
    }

    /**
     * Drives the refresh job until the named view leaves the BACKFILLING state, then drains the WAL
     * queue so the backfilled rows are visible to a reader. Fails if the view is still backfilling
     * after {@link #BACKFILL_COMPLETION_PASSES} passes.
     */
    protected void driveBackfillToCompletion(LiveViewRefreshJob job, String viewName) {
        boolean completed = false;
        for (int i = 0; i < BACKFILL_COMPLETION_PASSES; i++) {
            LiveViewInstance inst = engine.getLiveViewRegistry().getViewInstance(viewName);
            if (inst == null
                    || inst.getStateReader().getBackfillState() != LiveViewState.BACKFILL_STATE_BACKFILLING) {
                completed = true;
                break;
            }
            drainJob(job);
        }
        drainWalQueue();
        if (!completed) {
            Assert.fail("backfill of live view '" + viewName + "' did not complete within "
                    + BACKFILL_COMPLETION_PASSES + " passes; the view is still BACKFILLING");
        }
    }

    /**
     * Advances the clock and drives the refresh job until it makes no further progress. Fails if the
     * job is still finding work after {@link #REFRESH_QUIESCENCE_PASSES} passes, which means the view
     * never converged and any assertion the caller makes next would be reading a half-refreshed view.
     */
    protected void driveRefreshToQuiescence(LiveViewRefreshJob job) {
        for (int i = 0; i < REFRESH_QUIESCENCE_PASSES; i++) {
            setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
            drainWalQueue();
            boolean progressed = drainJob(job);
            drainWalQueue();
            if (!progressed) {
                return;
            }
        }
        Assert.fail("live view refresh did not quiesce within " + REFRESH_QUIESCENCE_PASSES
                + " passes; the refresh job is still finding work");
    }
}
