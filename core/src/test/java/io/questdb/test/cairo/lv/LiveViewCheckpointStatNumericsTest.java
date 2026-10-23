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
import io.questdb.cairo.lv.LiveViewCheckpointContracts;
import io.questdb.cairo.lv.LiveViewCheckpointRepairPlan;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.std.Rnd;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Numeric contracts of the statistical window functions over bounded frames in a live view,
 * on the data shapes where floating-point rounding decides the outcome. The five checkpointed
 * classes are stddev/variance over bounded ROWS and RANGE, covariance/correlation over bounded
 * ROWS and RANGE, and {@code ksum} over bounded RANGE.
 * <p>
 * A restore must be bit-exact: {@link LiveViewCheckpointContracts} states that restored
 * checkpoint bits are exact. {@link #testRestartCarriesAbsorbedAndOverflowedSumsBitExactly()}
 * pins it end to end on data that drives the running sums through absorption (a value lost
 * against 1e17) and overflow (a square of 1e200 that reaches +Inf, then NaN once the value
 * leaves the frame). Only a restore that takes the carried sums verbatim, NaN and Inf
 * included, prints what the plain query prints after a restart.
 * <p>
 * A localized repair re-accumulates the frame from its lower edge during warm-up, so the
 * sums lose the add/drop rounding history the plain query's sliding sums carry. The contract
 * caps what that may cost: approved floating outputs after a localized replay stay within
 * {@link LiveViewCheckpointContracts#FLOATING_RELATIVE_TOLERANCE} of a fresh recompute. The
 * stddev/variance and covariance/correlation formulas subtract nearly equal sums, which
 * scales that difference by mean^2 / variance: on this class's tick-like prices a warm-up
 * drifted from the plain query by as much as 1.5e-4 relative, and no fixed ceiling bounds it
 * in general. Their bounded frames therefore decline frame-local state, and a late row
 * resumes the view from a sealed root, whose carried sums re-emit the plain query's bits.
 * <p>
 * The {@code ...LocalizedRepairStaysWithinFloatingCeiling...} cases pin both halves on the
 * same tick-like data and the same deep late row, on both repair runtimes: the isolated one,
 * and the primary one that copies its state aside first. A control view of {@code avg} (and
 * {@code ksum} over RANGE) takes the localized rebuild and stays within the ceiling row by
 * row. A view that adds the four cancelling functions loses the localized plan, resumes from
 * a sealed root, and prints exactly what the plain query prints. Pathological data stays out
 * of those cases: after an extreme value leaves a sliding frame the plain query's own sums
 * stay corrupted, so any re-accumulation departs from it, the released {@code avg} included.
 * <p>
 * Resuming is one of the two routes left to such a view. A late row below every sealed root
 * rebuilds the view from its START FROM boundary, which
 * {@link #testBoundaryRebuildsStayBitExactFromBeginningAndFromALiteral()} pins from the beginning
 * and from a literal boundary. {@link #testViewMixingEveryFamilyAndRingWidthRepairsBitExactly()}
 * puts every family a live view checkpoints over a bounded frame into one view next to the
 * cancelling functions, and takes it through a restart and both routes.
 */
public class LiveViewCheckpointStatNumericsTest extends AbstractLiveViewTest {

    // The four cancelling functions, each over a frame of its own: a RANGE frame ending at the
    // current row, a lagged RANGE frame, a ROWS frame ending at the current row and a lagged
    // ROWS frame. %s is where a view whose START FROM is a literal restates it for its plain
    // query.
    private static final String CANCELLING_PROJECTION = """
            stddev_samp(px) OVER wr AS sd,
            corr(px, qty) OVER wl AS c,
            var_pop(px) OVER ww AS vr,
            covar_samp(px, qty) OVER wwl AS cv
            FROM fx%s
            WINDOW wr AS (PARTITION BY sym ORDER BY ts RANGE BETWEEN '20' SECOND PRECEDING AND CURRENT ROW),
                   wl AS (PARTITION BY sym ORDER BY ts RANGE BETWEEN '10' SECOND PRECEDING AND '3' SECOND PRECEDING),
                   ww AS (PARTITION BY sym ORDER BY ts ROWS 19 PRECEDING),
                   wwl AS (PARTITION BY sym ORDER BY ts ROWS BETWEEN 12 PRECEDING AND 3 PRECEDING)""";
    private static final int MIXED_COMMITS = 4;
    private static final int MIXED_COMMIT_SECONDS = 40;
    // Every window family a live view checkpoints over a bounded frame, and every scalar width
    // the RANGE ring seals: avg over DOUBLE (one word), avg over DECIMAL(20, 4) (four words),
    // stddev_samp and ksum (two words), corr and covar_pop (five words), plus the deque of max,
    // first_value, and the ROWS whole-state images of var_pop and avg.
    private static final String MIXED_SELECT = """
            SELECT ts, sym,
                   avg(px) OVER wr AS a1,
                   avg(d) OVER wr AS a4,
                   stddev_samp(px) OVER wr AS sd2,
                   ksum(px) OVER wr AS k2,
                   corr(px, qty) OVER wr AS c5,
                   covar_pop(px, qty) OVER wl AS cv5,
                   first_value(px) OVER wr AS fv,
                   max(px) OVER wr AS mx,
                   var_pop(px) OVER ww AS vr,
                   avg(px) OVER ww AS ar
            FROM fx
            WINDOW wr AS (PARTITION BY sym ORDER BY ts RANGE BETWEEN '30' SECOND PRECEDING AND CURRENT ROW),
                   wl AS (PARTITION BY sym ORDER BY ts RANGE BETWEEN '20' SECOND PRECEDING AND '5' SECOND PRECEDING),
                   ww AS (PARTITION BY sym ORDER BY ts ROWS BETWEEN 7 PRECEDING AND CURRENT ROW)""";

    // A frame ending at the current row and a lagged one.
    private static final String[] RANGE_FRAMES = {
            "PARTITION BY sym ORDER BY ts RANGE BETWEEN '20' SECOND PRECEDING AND CURRENT ROW",
            "PARTITION BY sym ORDER BY ts RANGE BETWEEN '10' SECOND PRECEDING AND '3' SECOND PRECEDING"
    };
    // The RANGE control view's functions: formulas that do not subtract nearly equal sums, so
    // they keep frame-local state and the localized rebuild.
    private static final String[] RANGE_CONTROL_FUNCTIONS = {"avg(px)", "ksum(px)"};
    // Where the RANGE case's late row lands: 50 seconds below the head of five 60-second
    // commits, deep enough that rebuilding the frame's reach reads fewer rows than resuming
    // from a sealed root and replaying everything above it.
    private static final int RANGE_LATE_SECOND = 250;
    // The controls, then the four functions whose formulas subtract nearly equal sums.
    private static final String[] RANGE_REPAIR_FUNCTIONS = {
            "avg(px)", "ksum(px)", "stddev_samp(px)", "var_pop(px)", "covar_samp(px, qty)", "corr(px, qty)"
    };
    // The issue's frame, which ends at the current row, and a lagged one.
    private static final String[] ROWS_FRAMES = {
            "PARTITION BY sym ORDER BY ts ROWS 19 PRECEDING",
            "PARTITION BY sym ORDER BY ts ROWS BETWEEN 12 PRECEDING AND 3 PRECEDING"
    };
    // ksum over a bounded ROWS frame declares no frame-local state, so it would deny the
    // control view its ROWS repair plan; avg is the sole control here.
    private static final String[] ROWS_CONTROL_FUNCTIONS = {"avg(px)"};
    // The control, then the four functions whose formulas subtract nearly equal sums.
    private static final String[] ROWS_REPAIR_FUNCTIONS = {
            "avg(px)", "stddev_samp(px)", "var_pop(px)", "covar_samp(px, qty)", "corr(px, qty)"
    };
    // Where the ROWS case's late row lands: 200 seconds below the head.
    private static final int ROWS_LATE_SECOND = 100;
    private static final int TICK_COMMIT_SECONDS = 60;
    private static final int TICK_COMMITS = 5;

    @After
    public void resetClock() {
        setCurrentMicros(-1);
    }

    @Before
    public void setUpCadence() {
        // One logical root per commit, the densest cadence the view can seal.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setCurrentMicros(0);
    }

    @Test
    public void testBoundaryRebuildsStayBitExactFromBeginningAndFromALiteral() throws Exception {
        // A late row below every sealed root leaves a resume nothing to stand on, so the view
        // rebuilds from its START FROM boundary. The cancelling functions decline frame-local
        // state, so that rebuild is the only route such a row has: it replays the frames from
        // the boundary on the primary runtime, in the order the plain query slides them, and
        // must print exactly what the plain query prints. `lv` starts from the beginning and
        // `lvs` from a literal 100 seconds in, so `lvs`'s plain query restates the boundary.
        //
        // A rebuild leaves no root below the rebuilt head either, so each later late row
        // rebuilds from the boundary again, until in-order commits seal new roots above it.
        // The restart then restores both views from the roots those commits sealed.
        final String startFrom = "2026-01-01T00:01:40.000000Z";
        final String plain = "(SELECT ts, sym, " + CANCELLING_PROJECTION.formatted("") + ") ORDER BY sym, ts";
        final String literalPlain = "(SELECT ts, sym, "
                + CANCELLING_PROJECTION.formatted(" WHERE ts >= '" + startFrom + "'") + ") ORDER BY sym, ts";
        final String view = "SELECT * FROM lv ORDER BY sym, ts";
        final String literalView = "SELECT * FROM lvs ORDER BY sym, ts";
        assertMemoryLeak(() -> {
            createBase();
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS SELECT ts, sym, "
                    + CANCELLING_PROJECTION.formatted(""));
            execute("CREATE LIVE VIEW lvs FLUSH EVERY 100ms START FROM '" + startFrom + "' AS SELECT ts, sym, "
                    + CANCELLING_PROJECTION.formatted(""));
            final Rnd rnd = new Rnd(42, 4242);
            final double[] prices = {65_000.0, 3_100.0};
            int second = 0;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int c = 0; c < TICK_COMMITS; c++) {
                    commitTicks(job, rnd, prices, second);
                    second += TICK_COMMIT_SECONDS;
                    driveRefreshToQuiescence(job);
                }
                assertViewPrintsLikePlain(view, plain);
                assertViewPrintsLikePlain(literalView, literalPlain);

                // 0.5s sits below every root of `lv`, whose first root seals at 59s, and below
                // `lvs`'s boundary; 30.5s sits below every root of `lv` again, now that the
                // first rebuild left none; 101.5s sits just above `lvs`'s boundary, which seals
                // its first root at 119s.
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                final LiveViewInstance literalInstance = engine.getLiveViewRegistry().getViewInstance("lvs");
                Assert.assertNotNull(literalInstance);
                final int[] lateSeconds = {0, 30, 101};
                final String[] lateSymbols = {"btc", "eth", "btc"};
                final double[] latePrices = {65_001.37, 3_100.07, 64_999.99};
                final double[] lateSizes = {1.234, 2.5, 0.5};
                for (int k = 0; k < lateSeconds.length; k++) {
                    final long boundaryRows = instance.getO3BoundaryReplayRows();
                    final long literalBoundaryRows = literalInstance.getO3BoundaryReplayRows();
                    commitLateRow(job, lateSeconds[k], lateSymbols[k], latePrices[k], lateSizes[k]);
                    driveRefreshToQuiescence(job);
                    assertBoundaryRebuild(instance, boundaryRows);
                    assertBoundaryRebuild(literalInstance, literalBoundaryRows);
                    assertViewPrintsLikePlain(view, plain);
                    assertViewPrintsLikePlain(literalView, literalPlain);
                }

                for (int c = 0; c < 2; c++) {
                    commitTicks(job, rnd, prices, second);
                    second += TICK_COMMIT_SECONDS;
                    driveRefreshToQuiescence(job);
                }
                assertViewPrintsLikePlain(view, plain);
                assertViewPrintsLikePlain(literalView, literalPlain);
                assertNoRefreshFaults("lv");
                assertNoRefreshFaults("lvs");
            }

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                commitTicks(job, rnd, prices, second);
                driveRefreshToQuiescence(job);
                assertRestoredFromTimeline("lv");
                assertRestoredFromTimeline("lvs");
                assertNoRefreshFaults("lv");
                assertNoRefreshFaults("lvs");
                assertViewPrintsLikePlain(view, plain);
                assertViewPrintsLikePlain(literalView, literalPlain);
            }
        });
    }

    @Test
    public void testRangeLocalizedRepairStaysWithinFloatingCeiling() throws Exception {
        assertLocalizedRepairStaysWithinFloatingCeiling(RANGE_FRAMES, RANGE_REPAIR_FUNCTIONS, RANGE_CONTROL_FUNCTIONS, RANGE_LATE_SECOND, false);
    }

    @Test
    public void testRangeLocalizedRepairStaysWithinFloatingCeilingWithoutIsolatedRuntime() throws Exception {
        assertLocalizedRepairStaysWithinFloatingCeiling(RANGE_FRAMES, RANGE_REPAIR_FUNCTIONS, RANGE_CONTROL_FUNCTIONS, RANGE_LATE_SECOND, true);
    }

    @Test
    public void testRestartCarriesAbsorbedAndOverflowedSumsBitExactly() throws Exception {
        // All five checkpointed classes in one view, with avg over both frames as the released
        // reference. Partition 'a' opens with (1e17, 1e17): every small value after it is
        // absorbed into the running sums, so once 1e17 leaves the frame the sums hold rounding
        // residue rather than the frame's true totals. Partition 'b' opens with
        // (1e200, 1e200): its square and product overflow to +Inf, and the drop of 1e200 at the
        // eleventh second turns those sums into NaN, which every later frame inherits.
        final String projection = """
                covar_pop(px, qty) OVER wr AS covar_range,
                corr(px, qty) OVER wr AS corr_range,
                stddev_pop(px) OVER wr AS stddev_range,
                ksum(px) OVER wr AS ksum_range,
                avg(px) OVER wr AS avg_range,
                covar_pop(px, qty) OVER ww AS covar_rows,
                corr(px, qty) OVER ww AS corr_rows,
                stddev_pop(px) OVER ww AS stddev_rows,
                avg(px) OVER ww AS avg_rows
                FROM fx
                WINDOW wr AS (PARTITION BY sym ORDER BY ts RANGE BETWEEN '10' SECOND PRECEDING AND CURRENT ROW),
                       ww AS (PARTITION BY sym ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)""";
        final String plain = "(SELECT ts, sym, " + projection + ") ORDER BY sym, ts";
        final String view = "SELECT * FROM lv ORDER BY sym, ts";
        assertMemoryLeak(() -> {
            createBase();
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS SELECT ts, sym, " + projection);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                commit(job, """
                        INSERT INTO fx (ts, sym, px, qty) VALUES
                        ('2026-01-01T00:00:00.000000Z', 'a', 1e17, 1e17),
                        ('2026-01-01T00:00:01.000000Z', 'a', 1.0, 1.0),
                        ('2026-01-01T00:00:02.000000Z', 'a', 2.0, 3.0),
                        ('2026-01-01T00:00:03.000000Z', 'a', 4.0, 2.0),
                        ('2026-01-01T00:00:00.000000Z', 'b', 1e200, 1e200),
                        ('2026-01-01T00:00:01.000000Z', 'b', 1.0, 1.0),
                        ('2026-01-01T00:00:02.000000Z', 'b', 2.0, 3.0),
                        ('2026-01-01T00:00:03.000000Z', 'b', 4.0, 2.0)""");
                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);
                // The eleventh second drops the opening pair from both frames; the root this
                // commit seals carries the residue and the NaN sums.
                commit(job, """
                        INSERT INTO fx (ts, sym, px, qty) VALUES
                        ('2026-01-01T00:00:11.000000Z', 'a', 3.0, 5.0),
                        ('2026-01-01T00:00:11.000000Z', 'b', 3.0, 5.0)""");
                driveRefreshToQuiescence(job);
                assertNoRefreshFaults("lv");
                assertViewPrintsLikePlain(view, plain);
            }

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                commit(job, """
                        INSERT INTO fx (ts, sym, px, qty) VALUES
                        ('2026-01-01T00:00:12.000000Z', 'a', 5.0, 1.0),
                        ('2026-01-01T00:00:12.000000Z', 'b', 5.0, 1.0),
                        ('2026-01-01T00:00:13.000000Z', 'a', 6.0, 4.0),
                        ('2026-01-01T00:00:13.000000Z', 'b', 6.0, 4.0),
                        ('2026-01-01T00:00:14.000000Z', 'a', 2.5, 7.5),
                        ('2026-01-01T00:00:14.000000Z', 'b', 2.5, 7.5)""");
                driveRefreshToQuiescence(job);
                assertRestoredFromTimeline("lv");
                assertNoRefreshFaults("lv");
                // The restored frames no longer hold 1e17 or 1e200, so a restore that re-summed
                // them would emit the frames' true statistics: finite for 'b', and free of the
                // absorption residue for 'a'. The plain query keeps the residue and the NaN, so
                // equal text, which is equal bits, means the restore carried both verbatim.
                assertViewPrintsLikePlain(view, plain);
                // Guard the scenario itself: if the plain query stopped carrying NaN sums for
                // 'b' past the drop, the bit-exact comparison above would no longer tell a
                // carried restore from a re-summed one.
                assertQuery("""
                        SELECT ts, covar_range, corr_range, stddev_range, covar_rows, corr_rows, stddev_rows
                        FROM lv
                        WHERE sym = 'b' AND ts > '2026-01-01T00:00:11.000000Z'
                        ORDER BY ts""")
                        .noLeakCheck()
                        .timestamp("ts")
                        .returns("""
                                ts\tcovar_range\tcorr_range\tstddev_range\tcovar_rows\tcorr_rows\tstddev_rows
                                2026-01-01T00:00:12.000000Z\tnull\tnull\tnull\tnull\tnull\tnull
                                2026-01-01T00:00:13.000000Z\tnull\tnull\tnull\tnull\tnull\tnull
                                2026-01-01T00:00:14.000000Z\tnull\tnull\tnull\tnull\tnull\tnull
                                """);
            }
        });
    }

    @Test
    public void testRowsLocalizedRepairStaysWithinFloatingCeiling() throws Exception {
        assertLocalizedRepairStaysWithinFloatingCeiling(ROWS_FRAMES, ROWS_REPAIR_FUNCTIONS, ROWS_CONTROL_FUNCTIONS, ROWS_LATE_SECOND, false);
    }

    @Test
    public void testRowsLocalizedRepairStaysWithinFloatingCeilingWithoutIsolatedRuntime() throws Exception {
        assertLocalizedRepairStaysWithinFloatingCeiling(ROWS_FRAMES, ROWS_REPAIR_FUNCTIONS, ROWS_CONTROL_FUNCTIONS, ROWS_LATE_SECOND, true);
    }

    @Test
    public void testViewMixingEveryFamilyAndRingWidthRepairsBitExactly() throws Exception {
        // One view holds every window family a live view checkpoints over a bounded frame, at
        // every scalar width the RANGE ring seals, so one root switches widths between
        // functions. The cancelling functions take the localized plan away from the whole view,
        // the deque, first_value and DECIMAL functions included: a late row near the head
        // resumes from a sealed root, and one below every root rebuilds from the boundary.
        // Every function rides that route, so the view must print exactly what the plain
        // query prints after the restart and after each repair.
        final String plain = "(" + MIXED_SELECT + ") ORDER BY sym, ts";
        final String view = "SELECT * FROM lv ORDER BY sym, ts";
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE fx (ts TIMESTAMP, sym SYMBOL, px DOUBLE, qty DOUBLE, d DECIMAL(20, 4))
                    TIMESTAMP(ts) PARTITION BY DAY WAL""");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " + MIXED_SELECT);
            final Rnd rnd = new Rnd(11, 1111);
            final double[] prices = {65_000.0, 3_100.0};
            int second = 0;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int c = 0; c < MIXED_COMMITS; c++) {
                    commitTicks(job, rnd, prices, second, MIXED_COMMIT_SECONDS, true);
                    second += MIXED_COMMIT_SECONDS;
                    driveRefreshToQuiescence(job);
                }
                assertViewPrintsLikePlain(view, plain);
            }

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                commitTicks(job, rnd, prices, second, MIXED_COMMIT_SECONDS, true);
                second += MIXED_COMMIT_SECONDS;
                driveRefreshToQuiescence(job);
                assertRestoredFromTimeline("lv");
                assertViewPrintsLikePlain(view, plain);

                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                // Three seconds below the head: the root sealed one commit back stands below it.
                commitDecimalLateRow(job, second - 3, "btc");
                driveRefreshToQuiescence(job);
                assertRepairDisposition(instance, "resume from anchor");
                assertViewPrintsLikePlain(view, plain);

                // Below every root: the first one sealed at 39s.
                commitDecimalLateRow(job, 20, "eth");
                driveRefreshToQuiescence(job);
                assertRepairDisposition(instance, "boundary rebuild");
                assertViewPrintsLikePlain(view, plain);

                commitTicks(job, rnd, prices, second, MIXED_COMMIT_SECONDS, true);
                driveRefreshToQuiescence(job);
                assertViewPrintsLikePlain(view, plain);
                assertNoRefreshFaults("lv");
            }
        });
    }

    // Each function over each frame, as columns named <function>_w<frame index>.
    private static String columnName(String function, int frame) {
        return function.substring(0, function.indexOf('(')) + "_w" + frame;
    }

    private static String repairColumns(String[] frames, String[] functions) {
        final StringBuilder sb = new StringBuilder();
        for (int f = 0; f < frames.length; f++) {
            for (String function : functions) {
                if (sb.length() > 0) {
                    sb.append(", ");
                }
                sb.append(columnName(function, f));
            }
        }
        return sb.toString();
    }

    private static String repairProjection(String[] frames, String[] functions) {
        final StringBuilder sb = new StringBuilder();
        for (int f = 0; f < frames.length; f++) {
            for (String function : functions) {
                if (sb.length() > 0) {
                    sb.append(", ");
                }
                sb.append(function).append(" OVER (").append(frames[f]).append(") AS ").append(columnName(function, f));
            }
        }
        return sb.toString();
    }

    // Half a second past `second`: a fresh designated timestamp, which ties with no in-order row.
    private static String lateTimestamp(int second) {
        return timestamp(second).replace(".000000Z", ".500000Z");
    }

    private static String timestamp(int second) {
        return String.format(
                "2026-01-%02dT%02d:%02d:%02d.000000Z",
                1 + second / 86_400,
                (second % 86_400) / 3600,
                (second % 3600) / 60,
                second % 60
        );
    }

    // Lays down five 60-second commits of tick-like data under two views over the same frames:
    // `lv` carries every repair function and `lv_ctl` only the controls. Checks forward
    // processing is bit-exact, then commits one late row deep below the head. The control view
    // must take the localized rebuild, whose warm-up re-accumulates the frame, and after two
    // more in-order commits every output must sit within the contract's floating ceiling of
    // the plain query. The cancelling functions decline frame-local state, so `lv` must lose
    // the localized plan, resume from a sealed root and print exactly what the plain query
    // prints. With the isolated runtime declined, the control view's repair replays on the
    // primary runtime after copying every partition's state aside; a one-row replay budget
    // parks it after the copy, which is what proves that route ran.
    private void assertLocalizedRepairStaysWithinFloatingCeiling(
            String[] frames,
            String[] functions,
            String[] controlFunctions,
            int lateSecond,
            boolean isCopyAside
    ) throws Exception {
        if (isCopyAside) {
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_ISOLATED_RUNTIME_ENABLED, "false");
        }
        final String projection = repairProjection(frames, functions);
        final String plain = "(SELECT ts, sym, " + projection + " FROM fx) ORDER BY sym, ts";
        final String view = "SELECT ts, sym, " + repairColumns(frames, functions) + " FROM lv ORDER BY sym, ts";
        final String controlProjection = repairProjection(frames, controlFunctions);
        final String controlPlain = "(SELECT ts, sym, " + controlProjection + " FROM fx) ORDER BY sym, ts";
        final String controlView = "SELECT ts, sym, " + repairColumns(frames, controlFunctions)
                + " FROM lv_ctl ORDER BY sym, ts";
        assertMemoryLeak(() -> {
            createBase();
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS SELECT ts, sym, "
                    + projection + " FROM fx");
            execute("CREATE LIVE VIEW lv_ctl FLUSH EVERY 100ms START FROM BEGINNING AS SELECT ts, sym, "
                    + controlProjection + " FROM fx");
            final Rnd rnd = new Rnd(42, 4242);
            final double[] prices = {65_000.0, 3_100.0};
            int second = 0;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int c = 0; c < TICK_COMMITS; c++) {
                    commitTicks(job, rnd, prices, second);
                    second += TICK_COMMIT_SECONDS;
                    if (c == 0) {
                        driveSeedToCompletion(job, "lv");
                        driveSeedToCompletion(job, "lv_ctl");
                    }
                    driveRefreshToQuiescence(job);
                }
                assertNoRefreshFaults("lv");
                assertNoRefreshFaults("lv_ctl");
                // Forward processing runs the same sliding sums as the plain query, so up to the
                // repair both views match it bit for bit. Whatever drifts below comes from the
                // repair.
                assertViewPrintsLikePlain(view, plain);
                assertViewPrintsLikePlain(controlView, controlPlain);

                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                final LiveViewInstance controlInstance = engine.getLiveViewRegistry().getViewInstance("lv_ctl");
                Assert.assertNotNull(controlInstance);
                if (isCopyAside) {
                    setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
                    commitLateRow(null, lateSecond);
                    driveUntilParked(job, "lv_ctl");
                    Assert.assertTrue(
                            "a converging repair on the primary runtime must have copied its state aside",
                            controlInstance.getSuspendedRepair().getOverlay().isCaptured()
                    );
                    setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1_000_000);
                } else {
                    commitLateRow(job, lateSecond);
                }
                driveRefreshToQuiescence(job);
                final int controlDenial = controlInstance.getCheckpointRepairLastDenialReason();
                Assert.assertEquals(
                        "the control view's late row must take the localized rebuild, whose warm-up re-accumulates the frame [denial="
                                + LiveViewCheckpointRepairPlan.denialReasonName(controlDenial) + ']',
                        "localized rebuild",
                        LiveViewCheckpointRepairPlan.dispositionName(controlInstance.getCheckpointRepairLastDisposition(), controlDenial)
                );
                // The cancelling functions decline frame-local state, which leaves the view
                // without a localized plan, so the late row resumes from the root sealed below it.
                final int denial = instance.getCheckpointRepairLastDenialReason();
                Assert.assertEquals(
                        "the cancelling functions must deny the view its localized plan",
                        "incomplete dependency",
                        LiveViewCheckpointRepairPlan.denialReasonName(denial)
                );
                Assert.assertEquals(
                        "the late row must resume the view from a sealed root",
                        "resume from anchor",
                        LiveViewCheckpointRepairPlan.dispositionName(instance.getCheckpointRepairLastDisposition(), denial)
                );

                // In-order commits keep running on the state the repairs left behind.
                for (int c = 0; c < 2; c++) {
                    commitTicks(job, rnd, prices, second);
                    second += TICK_COMMIT_SECONDS;
                    driveRefreshToQuiescence(job);
                }
                for (String name : new String[]{"lv", "lv_ctl"}) {
                    assertNoRefreshFaults(name);
                    assertQuery("SELECT view_status FROM live_views() WHERE view_name = '" + name + "'")
                            .noLeakCheck()
                            .noRandomAccess()
                            .returns("view_status\nactive\n");
                }
                Assert.assertTrue("nothing compared", assertAllRowsWithinFloatingCeiling(controlView, controlPlain) > 0);
                // Equal shortest round-trip text is equal bits: the resumed view carries the
                // plain query's rounding history through the repair and after it.
                assertViewPrintsLikePlain(view, plain);
            }
        });
    }

    // The late row the view last repaired for took the boundary rebuild, and the rebuild
    // re-emitted rows since `boundaryRowsBefore` was read, so the disposition is this row's.
    private static void assertBoundaryRebuild(LiveViewInstance instance, long boundaryRowsBefore) {
        assertRepairDisposition(instance, "boundary rebuild");
        Assert.assertTrue(
                "the boundary rebuild must have re-emitted the view's rows from its START FROM boundary",
                instance.getO3BoundaryReplayRows() > boundaryRowsBefore
        );
    }

    // The cancelling functions deny the view its localized plan, so its last repair must name
    // that denial next to the route it took instead.
    private static void assertRepairDisposition(LiveViewInstance instance, String expectedDisposition) {
        final int denial = instance.getCheckpointRepairLastDenialReason();
        Assert.assertEquals(
                "the cancelling functions must deny the view its localized plan",
                "incomplete dependency",
                LiveViewCheckpointRepairPlan.denialReasonName(denial)
        );
        Assert.assertEquals(
                "the late row took the wrong repair route",
                expectedDisposition,
                LiveViewCheckpointRepairPlan.dispositionName(instance.getCheckpointRepairLastDisposition(), denial)
        );
    }

    private void commit(LiveViewRefreshJob job, String insert) throws Exception {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        execute(insert);
        drainWalQueue();
        drainJob(job);
        drainWalQueue();
    }

    // Commits one 'btc' trade half a second past an in-order second - a fresh designated
    // timestamp, so it ties with no existing row and both orderings stay deterministic - and,
    // given a job, gives it a turn.
    private void commitLateRow(LiveViewRefreshJob job, int second) throws Exception {
        commitLateRow(job, second, "btc", 65_001.37, 1.234);
    }

    // Commits one trade for `sym` half a second past an in-order second, as above.
    private void commitLateRow(LiveViewRefreshJob job, int second, String sym, double px, double qty) throws Exception {
        commitLate(job, "INSERT INTO fx (ts, sym, px, qty) VALUES ('" + lateTimestamp(second) + "', '" + sym + "', "
                + px + ", " + qty + ")");
    }

    // Commits one trade for `sym`, with a DECIMAL as well, half a second past an in-order
    // second, as above.
    private void commitDecimalLateRow(LiveViewRefreshJob job, int second, String sym) throws Exception {
        commitLate(job, "INSERT INTO fx (ts, sym, px, qty, d) VALUES ('" + lateTimestamp(second) + "', '" + sym
                + "', 3333.37, 1.111, 12.5m)");
    }

    private void commitLate(LiveViewRefreshJob job, String insert) throws Exception {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        execute(insert);
        drainWalQueue();
        if (job != null) {
            drainJob(job);
            drainWalQueue();
        }
    }

    // Commits TICK_COMMIT_SECONDS ascending seconds from `firstSecond` of tick-like trades for
    // 'btc' and 'eth' and gives the refresh job a turn. Each price walks in one-cent steps from
    // where `prices` left it and each size is a whole number of thousandths, so no value is an
    // integer. Every eleventh 'btc' second has a NULL price and every thirteenth 'eth' second a
    // NULL size.
    private void commitTicks(LiveViewRefreshJob job, Rnd rnd, double[] prices, int firstSecond) throws Exception {
        commitTicks(job, rnd, prices, firstSecond, TICK_COMMIT_SECONDS, false);
    }

    // As above, over `seconds` seconds. With `hasDecimal`, each trade also carries a DECIMAL
    // with one fractional digit, NULL on every seventeenth 'eth' second.
    private void commitTicks(
            LiveViewRefreshJob job,
            Rnd rnd,
            double[] prices,
            int firstSecond,
            int seconds,
            boolean hasDecimal
    ) throws Exception {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        final StringBuilder sql = new StringBuilder("INSERT INTO fx (ts, sym, px, qty")
                .append(hasDecimal ? ", d) VALUES " : ") VALUES ");
        for (int i = 0; i < seconds; i++) {
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
                        .append(isQtyNull ? "null" : Double.toString(qty));
                if (hasDecimal) {
                    final int units = rnd.nextInt(100_000) - 50_000;
                    final int tenths = rnd.nextInt(10);
                    sql.append(", ");
                    if (s == 1 && second % 17 == 3) {
                        sql.append("null");
                    } else {
                        sql.append(units).append('.').append(tenths).append('m');
                    }
                }
                sql.append(')');
            }
        }
        execute(sql.toString());
        drainWalQueue();
        drainJob(job);
        drainWalQueue();
    }

    private void createBase() throws Exception {
        execute("CREATE TABLE fx (ts TIMESTAMP, sym SYMBOL, px DOUBLE, qty DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
    }

    private void restartCycle() {
        engine.getLiveViewRegistry().clear();
        engine.buildViewGraphs();
    }
}
