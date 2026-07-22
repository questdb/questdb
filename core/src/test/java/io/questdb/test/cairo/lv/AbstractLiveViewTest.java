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

import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointLifecycle;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewState;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.mp.Job;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;

/**
 * Shared driver helpers for the live view tests. Every test in this package advances a live view by
 * hand-driving a {@link LiveViewRefreshJob} rather than running a worker pool, so each of them needs
 * the same three loops. They used to be copy-pasted into ten test classes, where the seed bound
 * had drifted to three different values (500 / 1000 / 2000).
 * <p>
 * The important part is not the dedup: it is that {@link #driveRefreshToQuiescence} and
 * {@link #driveSeedToCompletion} now <b>fail</b> when they exhaust their pass budget. The
 * copy-pasted versions fell out of the loop silently and let the test carry on against a view that
 * had never converged, so a refresh that stalled surfaced hundreds of lines later as an unexplained
 * row-content mismatch - or, if the stalled view happened to hold the right rows already, as a pass.
 */
public abstract class AbstractLiveViewTest extends AbstractCairoTest {

    // Longer than the 100ms FLUSH EVERY the tests declare, so one pass always crosses a flush
    // deadline rather than leaving the view waiting on the clock.
    protected static final long CLOCK_ADVANCE_MICROS = 250_000;
    protected static final int REFRESH_QUIESCENCE_PASSES = 512;
    // Enough passes for the slowest seed in the suite (the parquet base test, which used 2000
    // when the helper was still copy-pasted). The loop exits as soon as the view leaves the
    // SEEDING state, so a generous bound costs nothing on the tests that converge quickly.
    protected static final int SEED_COMPLETION_PASSES = 2000;


    /**
     * Appends one {@code (ts, x)} row through a WalWriter without committing, so the caller
     * decides the commit mode. A plain INSERT cannot produce a REPLACE_RANGE commit; only the
     * WalWriter API can, so any test that needs one builds its rows this way. Assumes the base
     * table's second column is a LONG named x.
     */
    protected static void appendRow(WalWriter walWriter, long ts, long x) {
        TableWriter.Row row = walWriter.newRow(ts);
        row.putLong(1, x);
        row.append();
    }

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
     * Waits until {@code instance} has been marked dropped, then returns. This is the deterministic
     * fence a concurrent {@code engine.dropLiveView} crosses: {@code markDroppedAndAwaitCheckpoint}
     * publishes {@code dropped = true} (a volatile) BEFORE it commits to its blocking branch - the
     * {@code waitForUnfrozen} park under a checkpoint freeze, or the {@code fenceRefresh} spin behind
     * a held refresh latch. Keying off the view's own published drop state - rather than probing the
     * dropper thread's stack for a private method name - makes the fence immune to method renames,
     * JIT frame materialization and scheduling; the {@code deadlineNanos} guard is only a safety net,
     * never the load-bearing timing. Once this returns the dropper cannot complete the drop until the
     * blocker (freeze / latch) is released, so callers may assert the drop is still in progress.
     */
    protected static void awaitDropped(LiveViewInstance instance, long timeoutMs) {
        final long deadlineNanos = System.nanoTime() + timeoutMs * 1_000_000L;
        while (!instance.isDropped()) {
            if (System.nanoTime() >= deadlineNanos) {
                Assert.fail("live view was never marked dropped within " + timeoutMs + "ms");
            }
            Os.pause();
        }
    }

    /**
     * Parses a microsecond timestamp literal, for the tests that hand raw timestamps to a
     * WalWriter or compare against a live view's persisted START FROM boundary.
     */
    protected static long ts(String timestamp) {
        return MicrosTimestampDriver.floor(timestamp);
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

    /**
     * Drives the refresh job until the named view leaves the SEEDING state, then drains the WAL
     * queue so the seeded rows are visible to a reader. Fails if the view is still seeding
     * after {@link #SEED_COMPLETION_PASSES} passes.
     */
    protected void driveSeedToCompletion(LiveViewRefreshJob job, String viewName) {
        boolean completed = false;
        for (int i = 0; i < SEED_COMPLETION_PASSES; i++) {
            LiveViewInstance inst = engine.getLiveViewRegistry().getViewInstance(viewName);
            if (inst == null
                    || inst.getStateReader().getSeedState() != LiveViewState.SEED_STATE_SEEDING) {
                completed = true;
                break;
            }
            drainJob(job);
        }
        drainWalQueue();
        if (!completed) {
            Assert.fail("seed of live view '" + viewName + "' did not complete within "
                    + SEED_COMPLETION_PASSES + " passes; the view is still SEEDING");
        }
    }

    /**
     * Retires the checkpoint timeline the seed sweep has been sealing into, so a following restart
     * has no resume source and the sweep has to re-run from offset 0 behind its skip-write floor. A
     * no-op when the view has no timeline (nothing has been sealed yet, or the sweep completed and
     * retired it).
     */
    protected void retireSeedCheckpointTimeline(LiveViewInstance instance) {
        try (Path p = new Path()) {
            p.of(engine.getConfiguration().getDbRoot())
                    .concat(instance.getLiveViewToken())
                    .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
            LiveViewCheckpointLifecycle.retireTimeline(engine.getConfiguration(), p, null, true);
        }
    }
}
