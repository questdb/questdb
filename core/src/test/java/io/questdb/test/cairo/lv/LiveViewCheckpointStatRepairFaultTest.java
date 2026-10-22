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

package io.questdb.test.cairo.lv;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.lv.LiveViewCheckpointRepairPlan;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.std.Rnd;
import io.questdb.test.tools.LogCapture;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Faults on the repair and restore routes of a live view that holds the cancelling statistical
 * window functions over bounded frames: stddev/variance and covariance/correlation, over ROWS and
 * RANGE frames. Their formulas subtract nearly equal running sums, so they decline frame-local
 * state, and a view that holds one of them loses the localized repair plan as a whole, ksum
 * included. Each late row then either resumes from a sealed root or rebuilds from the view's
 * START FROM boundary, and either route replays on the primary runtime.
 * <p>
 * Each case fails one step of such a route: the resume's replay, the restore of the root it
 * resumes from (a lost file, and a read error a retry may get past), the rebuild's scan of the
 * applied base, and the timeline open a restart makes. Data is tick-like, so a route that
 * re-summed a frame rather than carrying its sums would print different bits. After the fault
 * clears, the view must be active and print exactly what the plain query prints; it must do so
 * again after an in-order commit, after a restart, and after a late row the restarted view
 * resumes from a sealed root.
 */
public class LiveViewCheckpointStatRepairFaultTest extends AbstractLiveViewTest {
    // Where a late row lands that a resume serves: 50 seconds below the head of five 60-second
    // commits, above the root the fourth commit sealed at 239s.
    private static final int NEAR_LATE_SECOND = 250;
    // Every cancelling family over a RANGE and a ROWS frame, one frame of each kind lagged, and
    // ksum, which keeps frame-local state of its own but loses the localized plan with the view.
    private static final String PROJECTION = """
            stddev_samp(px) OVER wr AS sd,
            corr(px, qty) OVER wl AS c,
            var_pop(px) OVER ww AS vr,
            covar_samp(px, qty) OVER wwl AS cv,
            ksum(px) OVER wr AS k
            FROM fx
            WINDOW wr AS (PARTITION BY sym ORDER BY ts RANGE BETWEEN '20' SECOND PRECEDING AND CURRENT ROW),
                   wl AS (PARTITION BY sym ORDER BY ts RANGE BETWEEN '10' SECOND PRECEDING AND '3' SECOND PRECEDING),
                   ww AS (PARTITION BY sym ORDER BY ts ROWS 19 PRECEDING),
                   wwl AS (PARTITION BY sym ORDER BY ts ROWS BETWEEN 12 PRECEDING AND 3 PRECEDING)""";
    private static final String PLAIN = "(SELECT ts, sym, " + PROJECTION + ") ORDER BY sym, ts";
    // How a restarted view recovers its window state: a restore of the newest root by the
    // restart's own attempt, a rebuild from the applied base after that restore failed for good,
    // or a restore in place by a later turn after that restore failed on an error a retry may get
    // past.
    private static final int RECOVERED_BY_REBUILD = 1;
    private static final int RECOVERED_FROM_TIMELINE = 0;
    private static final int RECOVERED_IN_PLACE = 2;
    private static final int TICK_COMMITS = 5;
    private static final int TICK_COMMIT_SECONDS = 60;
    private static final String VIEW = "SELECT * FROM lv ORDER BY sym, ts";
    private static final LogCapture capture = new LogCapture();

    @After
    public void resetClock() {
        capture.stop();
        setCurrentMicros(-1);
    }

    @Before
    public void setUpCadence() {
        // One logical root per commit, the densest cadence the view can seal.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setCurrentMicros(0);
        capture.start();
    }

    @Test
    public void testAppliedScanFaultDuringBoundaryRebuildIsRetried() throws Exception {
        // A late row below every sealed root leaves the view a rebuild from its START FROM
        // boundary, which scans the applied base. The fault fails that scan's first open of the
        // base's px column: the turn faults with nothing published, and the retry rebuilds again.
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault("ts", "px", "sym");
        assertFaultedRepairStaysBitExact(fault, (job, instance) -> {
            fault.armAppliedScan();
            commitLateRow(0, "btc", 65_001.37, 1.234);
            driveRefreshToQuiescence(job);
            Assert.assertTrue("the rebuild's scan of the applied base must have met the fault", fault.hasAppliedScanFired());
            Assert.assertEquals("the scan fault must cost exactly one refresh fault", 1, instance.getRefreshFaultCount());
            assertRepairDisposition(instance, "boundary rebuild", "incomplete dependency");
        });
    }

    @Test
    public void testCriticalFaultAtResumeReplayStartIsRetriedAsResume() throws Exception {
        assertResumeReplayFaultIsRetried(true);
    }

    @Test
    public void testLostResumeRootFallsBackToRebuildFromAppliedBase() throws Exception {
        // The resume finds the root below the late row gone: the open of the timeline it restores
        // that root from reports a missing file, which no retry gets past. The repair absorbs the
        // failure without a refresh fault, retires the timeline and leaves the window state for
        // the whole-view rebuild from the applied base, which recomputes every frame from the
        // first row on, in the order the plain query slides them.
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault("ts", "px", "sym");
        assertFaultedRepairStaysBitExact(fault, (job, instance) -> {
            fault.armAnchorRestoreOpen(CairoException.ERRNO_FILE_DOES_NOT_EXIST);
            commitLateRow(NEAR_LATE_SECOND, "btc", 65_001.37, 1.234);
            driveRefreshToQuiescence(job);
            Assert.assertEquals("the resume's root restore must have met the fault", 1, fault.getAnchorRestoreOpenFailures());
            Assert.assertEquals("a lost root must be absorbed without a refresh fault", 0, instance.getRefreshFaultCount());
            assertRepairDisposition(instance, "boundary rebuild", "non-data trigger");
            capture.drain();
            capture.assertLogged("could not restore live view O3 resume anchor [view=lv");
            capture.assertLogged("live view cannot restore its runtime from the checkpoint timeline, rebuilding from the applied base"
                    + " [view=lv, cause=mid-drain refresh failure, reason=timeline is absent]");
        });
    }

    @Test
    public void testNonCriticalFaultAtResumeReplayStartIsRetriedAsResume() throws Exception {
        assertResumeReplayFaultIsRetried(false);
    }

    @Test
    public void testResumeRootReadErrorIsRetriedAsResume() throws Exception {
        // The resume's restore of its root fails on a read error a retry may get past. The repair
        // keeps the timeline and faults the turn, and the retry resumes from the same root.
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault("ts", "px", "sym");
        assertFaultedRepairStaysBitExact(fault, (job, instance) -> {
            fault.armAnchorRestoreOpen(LiveViewMidDrainFault.ERRNO_EIO);
            commitLateRow(NEAR_LATE_SECOND, "btc", 65_001.37, 1.234);
            driveRefreshToQuiescence(job);
            Assert.assertEquals("the resume's root restore must have met the fault", 1, fault.getAnchorRestoreOpenFailures());
            Assert.assertEquals("the read error must cost exactly one refresh fault", 1, instance.getRefreshFaultCount());
            assertRepairDisposition(instance, "resume from anchor", "incomplete dependency");
            capture.drain();
            capture.assertLogged("could not restore live view O3 resume anchor, retrying [view=lv");
        });
    }

    @Test
    public void testTimelineLostAtRestartRebuildsFromAppliedBase() throws Exception {
        // The restart's restore finds the timeline file missing, which no retry gets past, so the
        // recovery rebuilds every frame from the applied base instead, the commit that woke it
        // included. The rebuild seals no root, so the late row after it rebuilds from the
        // boundary as well.
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault("ts", "px", "sym");
        assertRestartFaultStaysBitExact(fault, () -> fault.armTimelineOpen(), RECOVERED_BY_REBUILD);
    }

    @Test
    public void testTimelineReadErrorAtRestartRetriesTheRestore() throws Exception {
        // The restart's restore fails on a read error a retry may get past, so the restart leaves
        // the window-state debt, and the gate of the turn after the back-off restores the runtime
        // in place rather than rebuilding it.
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault("ts", "px", "sym");
        assertRestartFaultStaysBitExact(fault, () -> fault.armTimelineOpen(LiveViewMidDrainFault.ERRNO_EIO), RECOVERED_IN_PLACE);
        capture.drain();
        capture.assertLogged("could not restore live view from checkpoint timeline, retrying [view=lv");
        capture.assertLogged("live view restored its runtime from the checkpoint timeline [view=lv, cause=mid-drain refresh failure");
    }

    @Test
    public void testTimelineValidationFaultAtRegistrationIsAbsorbed() throws Exception {
        // The registration a restart runs validates every live view's timeline, and the fault
        // fails that open. The validation logs it and moves on, and the restore behind it opens
        // the timeline unharmed.
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault("ts", "px", "sym");
        assertMemoryLeak(fault.facade(), () -> {
            createBaseAndView(fault);
            final Rnd rnd = new Rnd(42, 4242);
            final double[] prices = {65_000.0, 3_100.0};
            int second = 0;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                second = commitTickHistory(job, rnd, prices, second);
            }

            fault.armTimelineOpen();
            restartCycle();
            Assert.assertEquals("the registration's timeline open must have met the fault", 1, fault.getTimelineOpenFailures());
            capture.drain();
            capture.assertLogged("could not validate live view checkpoint timeline [view=lv~");
            fault.disarmAll();
            assertRestartedViewStaysBitExact(rnd, prices, second, RECOVERED_FROM_TIMELINE);
        });
    }

    // The view's last repair took the route `expectedDisposition` names, for the denial
    // `expectedDenial` names.
    private static void assertRepairDisposition(LiveViewInstance instance, String expectedDisposition, String expectedDenial) {
        final int denial = instance.getCheckpointRepairLastDenialReason();
        Assert.assertEquals(
                "the repair's denial",
                expectedDenial,
                LiveViewCheckpointRepairPlan.denialReasonName(denial)
        );
        Assert.assertEquals(
                "the repair's route",
                expectedDisposition,
                LiveViewCheckpointRepairPlan.dispositionName(instance.getCheckpointRepairLastDisposition(), denial)
        );
    }

    private static String timestamp(int second) {
        return String.format(
                "2026-01-01T%02d:%02d:%02d.000000Z",
                second / 3600,
                (second % 3600) / 60,
                second % 60
        );
    }

    // The fault at the start of a resume's replay: thrown once, on the primary runtime the
    // resume restored its root onto, before the replay emits a row. The turn faults with
    // nothing published, and the retry resumes from the same root.
    private void assertResumeReplayFaultIsRetried(boolean isCritical) throws Exception {
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault("ts", "px", "sym");
        assertFaultedRepairStaysBitExact(fault, (job, instance) -> {
            final AtomicBoolean hasFired = new AtomicBoolean();
            hooks(job).setSimulateResumeReplayStart(() -> {
                hasFired.set(true);
                if (isCritical) {
                    throw CairoException.critical(0).put("simulated fault at the start of the resume replay");
                }
                throw CairoException.nonCritical().put("simulated fault at the start of the resume replay");
            });
            commitLateRow(NEAR_LATE_SECOND, "btc", 65_001.37, 1.234);
            driveRefreshToQuiescence(job);
            Assert.assertTrue("the resume's replay must have met the fault", hasFired.get());
            Assert.assertEquals("the fault must cost exactly one refresh fault", 1, instance.getRefreshFaultCount());
            Assert.assertEquals("the retried resume must not count a repair failure", 0, instance.getCheckpointRepairFailures());
            assertRepairDisposition(instance, "resume from anchor", "incomplete dependency");
        });
    }

    // Lays down five 60-second commits, then hands the job to `faultedTurn`, which arms one fault,
    // commits the late row that meets it, drives the view to quiescence and asserts the case.
    // Then the view must be active and settled, and print exactly what the plain query prints,
    // after the repair and after a tail commit; and again after a restart, see
    // assertRestartedViewStaysBitExact.
    private void assertFaultedRepairStaysBitExact(LiveViewMidDrainFault fault, FaultedTurn faultedTurn) throws Exception {
        assertMemoryLeak(fault.facade(), () -> {
            createBaseAndView(fault);
            final Rnd rnd = new Rnd(42, 4242);
            final double[] prices = {65_000.0, 3_100.0};
            int second = 0;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                second = commitTickHistory(job, rnd, prices, second);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                faultedTurn.run(job, instance);
                fault.disarmAll();
                assertSettled(instance);
                assertViewPrintsLikePlain(VIEW, PLAIN);

                commitTicks(job, rnd, prices, second);
                second += TICK_COMMIT_SECONDS;
                driveRefreshToQuiescence(job);
                assertSettled(instance);
                assertViewPrintsLikePlain(VIEW, PLAIN);
            }

            restartCycle();
            assertRestartedViewStaysBitExact(rnd, prices, second, RECOVERED_FROM_TIMELINE);
        });
    }

    // Lays down five 60-second commits and restarts, with `armFault` arming a fault on the
    // restart's restore after the registration and before the first turn. The view must recover
    // on the route `recovery` names.
    private void assertRestartFaultStaysBitExact(
            LiveViewMidDrainFault fault,
            Runnable armFault,
            int recovery
    ) throws Exception {
        assertMemoryLeak(fault.facade(), () -> {
            createBaseAndView(fault);
            final Rnd rnd = new Rnd(42, 4242);
            final double[] prices = {65_000.0, 3_100.0};
            int second = 0;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                second = commitTickHistory(job, rnd, prices, second);
            }

            restartCycle();
            armFault.run();
            assertRestartedViewStaysBitExact(rnd, prices, second, recovery);
            Assert.assertEquals("the restart's restore must have met the fault", 1, fault.getTimelineOpenFailures());
            fault.disarmAll();
        });
    }

    // After a restart: an in-order commit, whose frames reach back into rows only the recovered
    // state holds, then a late row five seconds below the head. The view must have recovered on
    // the route `recovery` names, and print exactly what the plain query prints after each. A
    // recovery that restored a root leaves the late row a root below it to resume from; a
    // rebuild from the applied base seals none, so the late row rebuilds from the boundary.
    private void assertRestartedViewStaysBitExact(Rnd rnd, double[] prices, int second, int recovery) throws Exception {
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            commitTicks(job, rnd, prices, second);
            second += TICK_COMMIT_SECONDS;
            driveRefreshToQuiescence(job);
            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            switch (recovery) {
                case RECOVERED_FROM_TIMELINE -> {
                    assertRestoredFromTimeline("lv");
                    assertNoRefreshFaults("lv");
                }
                case RECOVERED_BY_REBUILD -> {
                    assertRebuiltFromAppliedBase("lv");
                    assertNoRefreshFaults("lv");
                }
                case RECOVERED_IN_PLACE -> {
                    Assert.assertEquals("the failed restore must cost exactly one refresh fault", 1, instance.getRefreshFaultCount());
                    Assert.assertEquals("a later turn must have restored the runtime in place", 1, instance.getCheckpointRuntimeRestores());
                    Assert.assertEquals("no whole-view rebuild may have been asked for", 0, instance.getCheckpointRebuildAttempts());
                }
                default -> throw new IllegalArgumentException("recovery=" + recovery);
            }
            assertSettled(instance);
            assertViewPrintsLikePlain(VIEW, PLAIN);

            final long faults = instance.getRefreshFaultCount();
            commitLateRow(second - 5, "eth", 3_100.07, 2.5);
            driveRefreshToQuiescence(job);
            assertRepairDisposition(
                    instance,
                    recovery == RECOVERED_BY_REBUILD ? "boundary rebuild" : "resume from anchor",
                    "incomplete dependency"
            );
            Assert.assertEquals("the late row must not fault", faults, instance.getRefreshFaultCount());
            assertSettled(instance);
            assertViewPrintsLikePlain(VIEW, PLAIN);
        }
    }

    private void assertSettled(LiveViewInstance instance) throws Exception {
        Assert.assertFalse("the view must not be invalid", instance.isInvalid());
        Assert.assertFalse("the view must owe no window state", instance.isWindowStateDirty());
        assertQuery("SELECT view_status FROM live_views() WHERE view_name = 'lv'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("view_status\nactive\n");
    }

    // Commits one trade for `sym` half a second past an in-order second: a fresh designated
    // timestamp, which ties with no in-order row. The caller drives the job.
    private void commitLateRow(int second, String sym, double px, double qty) throws Exception {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        execute("INSERT INTO fx (ts, sym, px, qty) VALUES ('" + timestamp(second).replace(".000000Z", ".500000Z")
                + "', '" + sym + "', " + px + ", " + qty + ")");
        drainWalQueue();
    }

    // Five 60-second commits from `second`, each driven to quiescence: one root each. The view
    // must print exactly what the plain query prints after them, with no refresh fault.
    private int commitTickHistory(LiveViewRefreshJob job, Rnd rnd, double[] prices, int second) throws Exception {
        for (int c = 0; c < TICK_COMMITS; c++) {
            commitTicks(job, rnd, prices, second);
            second += TICK_COMMIT_SECONDS;
            driveRefreshToQuiescence(job);
        }
        assertNoRefreshFaults("lv");
        assertViewPrintsLikePlain(VIEW, PLAIN);
        return second;
    }

    // Commits TICK_COMMIT_SECONDS ascending seconds from `firstSecond` of tick-like trades for
    // 'btc' and 'eth' and gives the refresh job a turn. Each price walks in one-cent steps from
    // where `prices` left it and each size is a whole number of thousandths, so no value is an
    // integer. Every eleventh 'btc' second has a NULL price and every thirteenth 'eth' second a
    // NULL size.
    private void commitTicks(LiveViewRefreshJob job, Rnd rnd, double[] prices, int firstSecond) throws Exception {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        final StringBuilder sql = new StringBuilder("INSERT INTO fx (ts, sym, px, qty) VALUES ");
        for (int i = 0; i < TICK_COMMIT_SECONDS; i++) {
            final int second = firstSecond + i;
            final String ts = timestamp(second);
            for (int s = 0; s < 2; s++) {
                prices[s] += (rnd.nextInt(201) - 100) * 0.01;
                final double qty = (1 + rnd.nextInt(5_000)) * 0.001;
                final boolean isPxNull = s == 0 && second % 11 == 5;
                final boolean isQtyNull = s == 1 && second % 13 == 7;
                if (i > 0 || s > 0) {
                    sql.append(", ");
                }
                sql.append("('").append(ts).append("', '").append(s == 0 ? "btc" : "eth").append("', ")
                        .append(isPxNull ? "null" : Double.toString(prices[s])).append(", ")
                        .append(isQtyNull ? "null" : Double.toString(qty)).append(')');
            }
        }
        execute(sql.toString());
        drainWalQueue();
        drainJob(job);
        drainWalQueue();
    }

    private void createBaseAndView(LiveViewMidDrainFault fault) throws Exception {
        execute("CREATE TABLE fx (ts TIMESTAMP, sym SYMBOL, px DOUBLE, qty DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
        fault.of(engine.verifyTableName("fx").getDirName());
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS SELECT ts, sym, " + PROJECTION);
    }

    private void restartCycle() {
        engine.getLiveViewRegistry().clear();
        engine.buildViewGraphs();
    }

    @FunctionalInterface
    private interface FaultedTurn {
        void run(LiveViewRefreshJob job, LiveViewInstance instance) throws Exception;
    }
}
