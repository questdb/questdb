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
import io.questdb.cairo.lv.LiveViewCompiledPlan;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewState;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.mp.Job;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;

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
     * Returns the view's window function list, which is how a test reaches a non-anchored
     * window - {@code LiveViewInstance.getAnchorWindow()} does not surface one.
     * <p>
     * Reads the decomposed plan rather than walking the factory tree here. A SELECT that
     * wraps its window function in an expression puts a projection above the window
     * factory, so a hand-rolled walk that peels only {@code QueryProgress} stops one node
     * short and reports the view has no window at all.
     */
    protected static ObjList<WindowFunction> unwrapWindowFunctions(LiveViewInstance instance) {
        final LiveViewCompiledPlan plan = instance.getCompiledPlan();
        if (plan == null) {
            throw new IllegalStateException("live view has no compiled plan; refresh it first");
        }
        return plan.getWindowFactory().getWindowFunctions();
    }

    /**
     * Whether the engine's {@code MillisecondClock} follows the simulated microsecond clock. Probed
     * rather than declared: moves {@code currentMicros} to a stamp no wall clock reads, asks the
     * engine's own configuration for the time, then puts the clock back. A derived clock answers
     * with the stamp; a pinned real one answers with wall time.
     */
    private static boolean isEngineMillisecondClockDerivedFromTestClock() {
        // 2100-01-01T00:00:00Z in micros, far enough from now that no real clock reads it.
        final long probeMicros = 4_102_444_800_000_000L;
        final long savedMicros = currentMicros;
        try {
            setCurrentMicros(probeMicros);
            return engine.getConfiguration().getMillisecondClock().getTicks() == probeMicros / 1000;
        } finally {
            setCurrentMicros(savedMicros);
        }
    }

    @Before
    @Override
    public void setUp() {
        super.setUp();
        Assert.assertEquals(
                "isMillisecondClockSimulated() disagrees with the MillisecondClock the engine actually runs on; "
                        + "override it to match, because setUp() sizes the engine's spin deadlines from this answer",
                isMillisecondClockSimulated(),
                isEngineMillisecondClockDerivedFromTestClock()
        );
        if (isMillisecondClockSimulated()) {
            // These tests hand-drive the clock, and CairoTestConfiguration derives the millisecond clock
            // the engine's spin deadlines run on from that same simulated clock. So a soak whose refresh
            // driver advances the clock by CLOCK_ADVANCE_MICROS per tick, on its own thread, fast-forwards
            // every concurrent reader's deadline with it: a reader that loses one benign race in
            // TableReader.readTxnSlow - the writer commits between its txn read and its scoreboard acquire -
            // re-checks a deadline the driver has already blown by tens of simulated seconds and throws
            // "Transaction read timeout" milliseconds after entering the loop. Raising the budget past any
            // span such a test can simulate takes the simulated clock out of the spin loops. It costs no
            // real liveness cover THERE: the clock is simulated, so the deadline never measured real time
            // in the first place, and AbstractCairoTest's 20-minute JUnit timeout rule still fails a
            // genuinely stuck reader.
            //
            // isMillisecondClockSimulated() scopes the raise, because that argument is false for a
            // subclass that pins the production wall clock instead: there the deadline measures real
            // time, so raising it would trade a 5s reader-side "Transaction read timeout" naming the
            // stalled table for the 20-minute class timeout - 240x coarser, with no reader diagnostic.
            spinLockTimeout = 365L * 24 * 60 * 60 * 1000; // a simulated year
        }
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
     * Whether the millisecond clock this test's engine reads - the clock the storage engine measures
     * every spin deadline against - is the simulated one the tests hand-drive through
     * {@code setCurrentMicros}, rather than the production wall clock.
     * <p>
     * True by default, because {@link io.questdb.test.cairo.CairoTestConfiguration} derives its
     * millisecond clock from the same microsecond clock {@code setCurrentMicros} moves. A subclass
     * that installs a real {@code MillisecondClock} of its own must override this and return false:
     * {@link #setUp} then leaves it {@link io.questdb.test.AbstractCairoTest#DEFAULT_SPIN_LOCK_TIMEOUT},
     * which on a real clock is genuine liveness cover rather than an artifact of the simulated one.
     * <p>
     * It describes the wiring, not every instant: the derived clock falls through to real wall time
     * while {@code currentMicros == -1}, so a simulated suite still reads real time until its first
     * {@code setCurrentMicros}.
     * <p>
     * {@link #setUp} asserts the answer against the engine's actual clock rather than trusting it,
     * so a subclass that pins a real clock and forgets to override this fails immediately instead of
     * silently inheriting a simulated-year {@code spinLockTimeout} - which is the defect the hook
     * exists to prevent.
     */
    protected boolean isMillisecondClockSimulated() {
        return true;
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
