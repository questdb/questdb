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
import io.questdb.cairo.lv.LiveViewCheckpointDependency;
import io.questdb.cairo.lv.LiveViewCheckpointDependency.NumericConvergence;
import io.questdb.cairo.lv.LiveViewCheckpointGenerationPin;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointMetaStore;
import io.questdb.cairo.lv.LiveViewCheckpointRepairPlan;
import io.questdb.cairo.lv.LiveViewCheckpointRepairSession;
import io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectoryReader;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.engine.window.WindowRecordCursorFactory;
import io.questdb.std.Chars;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * The stddev/variance family over a bounded, partitioned RANGE frame persists each
 * partition's ring through the chunked ring codec: the buffered {@code (timestamp, value)}
 * pairs, oldest first, behind a two-word scalar carrying the exact running
 * {@code (sum, sumSq)} and a frame size carrying the in-frame count. One factory backs all
 * six spellings - {@code stddev}, {@code stddev_samp}, {@code stddev_pop}, {@code variance},
 * {@code var_samp} and {@code var_pop} - so the same codec covers every one of them.
 * <p>
 * {@link #testBoundedRangeFramesDeclareRingCheckpoint()} pins the disposition per function
 * and frame shape. {@link #testBoundedRangeSurvivesRestartAndRepair()} proves the durable
 * ring round-trips: the views restore from the timeline after a restart, share chunk pages
 * across roots, and survive out-of-order corrections; the last one resumes every view from
 * a sealed root, so the repair itself restores the ring. {@link
 * #testBoundedRangeRepairCopiesStateAsideWithoutIsolatedRuntime()} drives the in-RAM form
 * through the repair's copy-aside overlay. {@link
 * #testBoundedRangeRestoresExactValuesAcrossRestart()} pins restored values against
 * hand-computed ones, including a ring that restores a pair the frame has not reached yet.
 * <p>
 * The codec covers a bounded frame start only. The rejection cases pin that every
 * unbounded-start spelling stays out of a live view: the CREATE-time gates refuse the
 * unanchored ones, and the class itself declines the anchored one those gates let through.
 */
public class LiveViewCheckpointStdDevRangeTest extends AbstractLiveViewTest {

    private static final int COMMITS = 6;
    // Commits the copy-aside case lays down before its correction: enough history above
    // DEEP_SECOND_A that every narrow frame's influence stops short of the head, which is what
    // makes its repair converge.
    private static final int COPY_ASIDE_COMMITS = 9;
    // Where the deep corrections land: above the widest frame's width, so no frame's reach
    // runs down into the views' START FROM floor, and below the restart's head.
    private static final int DEEP_SECOND_A = 217;
    private static final int DEEP_SECOND_B = 233;
    // Wide enough that a partition's ring holds more than twice
    // LiveViewCheckpointRingSeal.MIN_SHARED_CHUNK_ROWS pairs, which is what lets a seal keep
    // the previous root's chunk and reference it instead of re-imaging the ring.
    private static final String FRAME_CURRENT = "PARTITION BY sym ORDER BY ts RANGE BETWEEN '200' SECOND PRECEDING AND CURRENT ROW";
    // EXCLUDE CURRENT ROW folds into a high bound one tick below the current row, so the
    // current pair waits in the ring outside the frame.
    private static final String FRAME_EXCLUDED = "PARTITION BY sym ORDER BY ts RANGE BETWEEN '6' SECOND PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW";
    // The issue's own spelling: the short form of N PRECEDING AND CURRENT ROW.
    private static final String FRAME_ISSUE = "PARTITION BY sym ORDER BY ts RANGE '1' MINUTE PRECEDING";
    // A high bound below the current row: the ring holds pairs the frame has not reached yet.
    private static final String FRAME_LAGGED = "PARTITION BY sym ORDER BY ts RANGE BETWEEN '10' SECOND PRECEDING AND '3' SECOND PRECEDING";
    private static final String[] FUNCTIONS = {"stddev", "stddev_samp", "stddev_pop", "variance", "var_samp", "var_pop"};
    private static final String[] NARROW_VIEWS = {"lv_lag", "lv_excl", "lv_issue"};
    // Distinct ascending seconds per key per commit.
    private static final int ROWS_PER_COMMIT = 40;
    // Seconds in the short commit the near-head correction lands in.
    private static final int TAIL_ROWS = 3;
    private static final String[] VIEWS = {"lv_cur", "lv_lag", "lv_excl", "lv_issue"};

    @After
    public void resetClock() {
        setCurrentMicros(-1);
    }

    @Before
    public void setUpCadence() {
        // One logical root per commit, the densest cadence the view can seal, so the
        // checkpoint timeline a restart or a repair restores from is deep.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setCurrentMicros(0);
    }

    @Test
    public void testAnchoredUnboundedRangeStaysRejected() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            // EXCLUDE CURRENT ROW keeps the ANCHOR (the anchor validator reads the bounds, not
            // the exclusion) and the folded high bound sends the call to the bounded RANGE class
            // with an unbounded start. The finite-influence gate lets an anchored window through,
            // so the class itself has to decline: its ring codec covers a bounded start only.
            final String frame = "PARTITION BY sym ORDER BY ts "
                    + "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW";
            for (String fn : FUNCTIONS) {
                final CompiledFunction compiled = compileSoleWindowFunction(fn + "(px)", frame, true);
                Assert.assertEquals(fn, "StdDevOverPartitionRangeFrameFunction", compiled.className);
                Assert.assertFalse(fn + " over an unbounded start must decline", compiled.supportsCheckpointState);
                Assert.assertFalse(fn + " over an unbounded start must decline", compiled.supportsCheckpointRingState);
                try {
                    execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS "
                            + "SELECT ts, sym, " + fn + "(px) OVER w AS v FROM fx "
                            + "WINDOW w AS (" + frame + " ANCHOR DAILY '00:00')");
                    Assert.fail("an anchored unbounded-start RANGE " + fn + " must be refused at CREATE");
                } catch (SqlException e) {
                    TestUtils.assertContains(
                            e.getFlyweightMessage(),
                            "live view select cannot use window function " + fn
                                    + "(); incremental snapshot is not supported for this function yet"
                    );
                }
            }
        });
    }

    @Test
    public void testBoundedRangeFramesDeclareRingCheckpoint() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            final String[] frames = {FRAME_CURRENT, FRAME_LAGGED, FRAME_EXCLUDED, FRAME_ISSUE};
            for (String fn : FUNCTIONS) {
                for (String frame : frames) {
                    assertRingDisposition(fn + "(px)", frame);
                }
                // Outside a live view compile the same class declines: the codec needs the
                // tombstone slot and the key layout copy only a live view compile builds.
                final CompiledFunction plain = compileSoleWindowFunction(fn + "(px)", FRAME_CURRENT, false);
                Assert.assertFalse(fn + " outside a live view compile must not claim checkpoint state", plain.supportsCheckpointState);
                Assert.assertFalse(fn + " outside a live view compile must not claim a ring", plain.supportsCheckpointRingState);
            }
        });
    }

    @Test
    public void testBoundedRangeRejectsUnboundedFrameStart() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            for (String fn : FUNCTIONS) {
                // Both spellings compile to the bounded-RANGE class with an unbounded low bound.
                // A trailing high bound reads as a bounded frame, so the finite-influence gate is
                // what turns it away.
                assertUnboundedFrameStartRejected(
                        fn,
                        "RANGE BETWEEN UNBOUNDED PRECEDING AND '3' SECOND PRECEDING",
                        "frame starting at UNBOUNDED PRECEDING"
                );
                // CURRENT ROW folded one tick down by EXCLUDE CURRENT ROW reads as the default
                // frame, so without an ANCHOR the bare-unbounded-window rule refuses it first.
                assertUnboundedFrameStartRejected(
                        fn,
                        "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW",
                        "live view unbounded window must have an ANCHOR clause"
                );
            }
        });
    }

    @Test
    public void testBoundedRangeRejectsUnsupportedPartitionKey() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE ux (ts TIMESTAMP, u UUID, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            for (String fn : FUNCTIONS) {
                try {
                    execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS "
                            + "SELECT ts, u, " + fn + "(px) OVER (PARTITION BY u ORDER BY ts "
                            + "RANGE BETWEEN '3' SECOND PRECEDING AND CURRENT ROW) AS v FROM ux");
                    Assert.fail("a UUID partition key must keep " + fn + " out of a live view");
                } catch (SqlException e) {
                    TestUtils.assertContains(
                            e.getFlyweightMessage(),
                            "live view select cannot use window function " + fn
                                    + "(); incremental snapshot is not supported for this function yet"
                    );
                }
            }
        });
    }

    @Test
    public void testBoundedRangeRepairCopiesStateAsideWithoutIsolatedRuntime() throws Exception {
        // With the isolated repair runtime declined, a repair whose influence converges below
        // the frontier replays on the primary runtime and copies every partition's state aside
        // first, through the in-RAM freezeCheckpointState / restoreCheckpointState pair. A
        // one-row replay budget parks the repair after the copy, which is what lets the test
        // see it was taken. The narrow frames are the ones whose localized rebuild beats a
        // resume from the root below the change; the wide one resumes instead, through the
        // durable ring, which testBoundedRangeSurvivesRestartAndRepair covers.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_ISOLATED_RUNTIME_ENABLED, "false");
        assertMemoryLeak(() -> {
            createBase();
            createViews(NARROW_VIEWS);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int commit = 1; commit <= COPY_ASIDE_COMMITS; commit++) {
                    commitDense(job, commit);
                }
                driveRefreshToQuiescence(job);
                assertViewsMatchRecompute(NARROW_VIEWS);

                setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
                commitOutOfOrderHalfSecond(null, DEEP_SECOND_A, "a", "4242");
                for (String view : NARROW_VIEWS) {
                    final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance(view);
                    Assert.assertNotNull(view, instance);
                    driveUntilRepairParks(job, instance);
                    final LiveViewCheckpointRepairSession parked = instance.getSuspendedRepair();
                    Assert.assertTrue(
                            view + ": a converging repair on the primary must have copied its state aside",
                            parked.getOverlay().isCaptured()
                    );
                }
                setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1_000_000);
                driveRefreshToQuiescence(job);
                assertViewsMatchRecompute(NARROW_VIEWS);
                assertRepairLocalized(NARROW_VIEWS);

                // The forward drain after the repair runs on whatever the copy-aside put back,
                // so a state image that lost a pair or a sum shows up here.
                for (int commit = COPY_ASIDE_COMMITS + 1; commit <= COPY_ASIDE_COMMITS + 2; commit++) {
                    commitDense(job, commit);
                }
                driveRefreshToQuiescence(job);
                assertViewsMatchRecompute(NARROW_VIEWS);
            }
        });
    }

    @Test
    public void testBoundedRangeRestoresExactValuesAcrossRestart() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            // The issue's lv2 spelling of the frame, over stddev_samp.
            execute("CREATE LIVE VIEW lv2 FLUSH EVERY 1s START FROM BEGINNING AS "
                    + "SELECT ts, sym, " + issueProjection() + " FROM fx");
            execute("""
                    CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS
                    SELECT ts, sym,
                           var_pop(px) OVER w1 AS vp,
                           var_samp(px) OVER w1 AS vs,
                           stddev_pop(px) OVER w1 AS sp,
                           variance(px) OVER w1 AS va,
                           var_pop(px) OVER w2 AS vp2,
                           var_samp(px) OVER w2 AS vs2
                    FROM fx
                    WINDOW w1 AS (PARTITION BY sym ORDER BY ts RANGE BETWEEN '2' SECOND PRECEDING AND CURRENT ROW),
                           w2 AS (PARTITION BY sym ORDER BY ts RANGE BETWEEN '3' SECOND PRECEDING AND '1' SECOND PRECEDING)""");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("""
                        INSERT INTO fx (ts, sym, px, qty) VALUES
                        ('2026-01-01T00:00:00.000000Z', 'a', 1.0, 1.0),
                        ('2026-01-01T00:00:00.000000Z', 'b', 10.0, 1.0),
                        ('2026-01-01T00:00:01.000000Z', 'a', 3.0, 1.0),
                        ('2026-01-01T00:00:01.000000Z', 'b', 14.0, 1.0),
                        ('2026-01-01T00:00:02.000000Z', 'a', null, 1.0),
                        ('2026-01-01T00:00:04.000000Z', 'a', 7.0, 1.0)""");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
                driveSeedToCompletion(job, "lv");
                driveSeedToCompletion(job, "lv2");
                driveRefreshToQuiescence(job);
                assertNoRefreshFaults("lv");
                assertQuery("SELECT ts, sym, vp, vs, sp, va, vp2, vs2 FROM lv ORDER BY sym, ts")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                ts\tsym\tvp\tvs\tsp\tva\tvp2\tvs2
                                2026-01-01T00:00:00.000000Z\ta\t0.0\tnull\t0.0\tnull\tnull\tnull
                                2026-01-01T00:00:01.000000Z\ta\t1.0\t2.0\t1.0\t2.0\t0.0\tnull
                                2026-01-01T00:00:02.000000Z\ta\t1.0\t2.0\t1.0\t2.0\t1.0\t2.0
                                2026-01-01T00:00:04.000000Z\ta\t0.0\tnull\t0.0\tnull\t0.0\tnull
                                2026-01-01T00:00:00.000000Z\tb\t0.0\tnull\t0.0\tnull\tnull\tnull
                                2026-01-01T00:00:01.000000Z\tb\t4.0\t8.0\t2.0\t8.0\t0.0\tnull
                                """);
            }

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // a's w1 ring restores holding 7 (04s), which 05s pairs with 5. a's w2 ring
                // restores holding 3 (01s) in its frame and 7 (04s) waiting outside it: 05s
                // expires the first and admits the second, so a ring that lost the waiting pair
                // reads an empty frame there. b's rings restore holding 10 and 14, which 05s
                // expires in both frames.
                execute("""
                        INSERT INTO fx (ts, sym, px, qty) VALUES
                        ('2026-01-01T00:00:05.000000Z', 'a', 5.0, 1.0),
                        ('2026-01-01T00:00:05.000000Z', 'b', 20.0, 1.0),
                        ('2026-01-01T00:00:07.000000Z', 'a', 9.0, 1.0)""");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
                driveRefreshToQuiescence(job);
                assertRestoredFromTimeline("lv");
                assertRestoredFromTimeline("lv2");
                assertNoRefreshFaults("lv");
                assertQuery("SELECT ts, sym, vp, vs, sp, va, vp2, vs2 FROM lv ORDER BY sym, ts")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                ts\tsym\tvp\tvs\tsp\tva\tvp2\tvs2
                                2026-01-01T00:00:00.000000Z\ta\t0.0\tnull\t0.0\tnull\tnull\tnull
                                2026-01-01T00:00:01.000000Z\ta\t1.0\t2.0\t1.0\t2.0\t0.0\tnull
                                2026-01-01T00:00:02.000000Z\ta\t1.0\t2.0\t1.0\t2.0\t1.0\t2.0
                                2026-01-01T00:00:04.000000Z\ta\t0.0\tnull\t0.0\tnull\t0.0\tnull
                                2026-01-01T00:00:05.000000Z\ta\t1.0\t2.0\t1.0\t2.0\t0.0\tnull
                                2026-01-01T00:00:07.000000Z\ta\t4.0\t8.0\t2.0\t8.0\t1.0\t2.0
                                2026-01-01T00:00:00.000000Z\tb\t0.0\tnull\t0.0\tnull\tnull\tnull
                                2026-01-01T00:00:01.000000Z\tb\t4.0\t8.0\t2.0\t8.0\t0.0\tnull
                                2026-01-01T00:00:05.000000Z\tb\t0.0\tnull\t0.0\tnull\tnull\tnull
                                """);
                assertViewMatchesRecompute("lv2", issueProjection(), "vol");
            }
        });
    }

    @Test
    public void testBoundedRangeSurvivesRestartAndRepair() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            createViews(VIEWS);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int commit = 1; commit <= COMMITS; commit++) {
                    commitDense(job, commit);
                }
                driveRefreshToQuiescence(job);
                assertViewsMatchRecompute(VIEWS);
                // The ring codec's point: a seal reuses the pages the previous root wrote for
                // the pairs both rings hold, so some data segment has more than one referrer.
                assertSomeSegmentShared("lv_cur");
            }

            // A restart rebuilds each view's runtime RANGE state from the timeline through
            // restoreCheckpointRingState, so a following refresh that matches a fresh recompute
            // proves the ring round-tripped.
            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int commit = COMMITS + 1; commit <= COMMITS + 3; commit++) {
                    commitDense(job, commit);
                }
                driveRefreshToQuiescence(job);
                for (String view : VIEWS) {
                    assertRestoredFromTimeline(view);
                }
                assertViewsMatchRecompute(VIEWS);

                // Out-of-order rows deep in history at fresh sub-second timestamps (no
                // designated-timestamp tie with an existing row) force the repair path. The
                // second one is a NULL, which the ring never stores.
                commitOutOfOrderHalfSecond(job, DEEP_SECOND_A, "a", "4242");
                driveRefreshToQuiescence(job);
                assertViewsMatchRecompute(VIEWS);
                assertRepairLocalized(VIEWS);

                commitOutOfOrderHalfSecond(job, DEEP_SECOND_B, "b", "null");
                driveRefreshToQuiescence(job);
                assertViewsMatchRecompute(VIEWS);
                assertRepairLocalized(VIEWS);

                // A short tail, then a correction just above the root sealed below it: replaying
                // the tail from that root reads fewer base rows than rebuilding the frame's
                // reach, so the repair resumes from the root, restoring every partition's ring
                // from the timeline before it replays.
                final int tailSecond = (COMMITS + 3) * ROWS_PER_COMMIT;
                commitSeconds(job, tailSecond, TAIL_ROWS);
                driveRefreshToQuiescence(job);
                commitOutOfOrderHalfSecond(job, tailSecond, "a", "77");
                driveRefreshToQuiescence(job);
                assertViewsMatchRecompute(VIEWS);
                for (String view : VIEWS) {
                    final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance(view);
                    Assert.assertNotNull(view, instance);
                    Assert.assertEquals(
                            view + " must resume from a sealed root",
                            "resume from anchor",
                            LiveViewCheckpointRepairPlan.dispositionName(
                                    instance.getCheckpointRepairLastDisposition(),
                                    instance.getCheckpointRepairLastDenialReason()
                            )
                    );
                }

                // And the in-order path keeps going on the state the repairs left behind.
                commitSeconds(job, tailSecond + TAIL_ROWS, ROWS_PER_COMMIT);
                driveRefreshToQuiescence(job);
                assertViewsMatchRecompute(VIEWS);
            }
        });
    }

    private static Path checkpointsDir(LiveViewInstance instance) {
        return new Path().of(configuration.getDbRoot())
                .concat(instance.getLiveViewToken())
                .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
    }

    private static CompiledFunction compileSoleWindowFunction(String fn, String frame, boolean liveViewCompile) throws Exception {
        final String sql = "SELECT ts, sym, " + fn + " OVER (" + frame + ") AS v FROM fx";
        sqlExecutionContext.setLiveViewCompile(liveViewCompile);
        try (
                SqlCompiler compiler = engine.getSqlCompiler();
                RecordCursorFactory factory = select(compiler, sql, sqlExecutionContext)
        ) {
            RecordCursorFactory root = factory;
            while (root instanceof QueryProgress) {
                root = root.getBaseFactory();
            }
            Assert.assertTrue(sql, root instanceof WindowRecordCursorFactory);
            final ObjList<WindowFunction> functions = ((WindowRecordCursorFactory) root).getWindowFunctions();
            Assert.assertEquals(sql + ": expected exactly one window function", 1, functions.size());
            return new CompiledFunction(functions.getQuick(0));
        } finally {
            sqlExecutionContext.setLiveViewCompile(false);
        }
    }

    private static String frameOf(String view) {
        return switch (view) {
            case "lv_cur" -> FRAME_CURRENT;
            case "lv_lag" -> FRAME_LAGGED;
            case "lv_excl" -> FRAME_EXCLUDED;
            default -> throw new AssertionError("no frame for view " + view);
        };
    }

    // The issue's lv2 frame over a single stddev_samp column.
    private static String issueProjection() {
        return "stddev_samp(px) OVER (" + FRAME_ISSUE + ") AS vol";
    }

    private static String projection(String frame) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < FUNCTIONS.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(FUNCTIONS[i]).append("(px) OVER (").append(frame).append(") AS f").append(i);
        }
        return sb.toString();
    }

    private static String projectionColumns() {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < FUNCTIONS.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append('f').append(i);
        }
        return sb.toString();
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

    private void assertRepairLocalized(String[] views) {
        for (String view : views) {
            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance(view);
            Assert.assertNotNull(view, instance);
            // The frame-local claim is what gives the view a RANGE repair plan, and the plan
            // is what bounds the repair below by the frame's width rather than rebuilding the
            // whole view from its START FROM boundary.
            Assert.assertEquals(view, LiveViewInstance.REPAIR_PLAN_RANGE, instance.getCheckpointRepairDependencyPlans());
            final int denial = instance.getCheckpointRepairLastDenialReason();
            final String disposition = LiveViewCheckpointRepairPlan.dispositionName(
                    instance.getCheckpointRepairLastDisposition(),
                    denial
            );
            Assert.assertNotNull(view + " must have run a repair", disposition);
            Assert.assertNotEquals(
                    view + " must localize the repair [denial="
                            + LiveViewCheckpointRepairPlan.denialReasonName(denial) + ']',
                    "boundary rebuild",
                    disposition
            );
        }
    }

    private void assertRingDisposition(String fn, String frame) throws Exception {
        final String label = fn + " OVER (" + frame + ")";
        final CompiledFunction compiled = compileSoleWindowFunction(fn, frame, true);
        Assert.assertEquals(label, "StdDevOverPartitionRangeFrameFunction", compiled.className);
        Assert.assertTrue(label + " must support checkpoint state", compiled.supportsCheckpointState);
        Assert.assertTrue(label + " must persist through the chunked ring codec", compiled.supportsCheckpointRingState);
        Assert.assertEquals(label + " carries (sum, sumSq) as a two-word scalar", 2, compiled.ringScalarWords);
        Assert.assertTrue(label + " must expose its ring arena", compiled.hasRingArena);
        // A ring-shaped function never declares a fixed image width.
        Assert.assertEquals(label, -1, compiled.fixedStateLength);
        Assert.assertTrue(label + " must declare frame-local state", compiled.hasFrameLocalCheckpointState);
        final LiveViewCheckpointDependency dependency = compiled.dependency;
        Assert.assertNotNull(label, dependency);
        Assert.assertTrue(label, dependency.isFiniteRange());
        Assert.assertTrue(label, dependency.hasFrameLocalState());
        Assert.assertEquals(label, NumericConvergence.FLOATING_TOLERANCE, dependency.getNumericConvergence());
    }

    private void assertSomeSegmentShared(String viewName) {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance(viewName);
        Assert.assertNotNull(viewName, instance);
        try (
                LiveViewCheckpointMetaStore store = openStore(instance);
                LiveViewCheckpointGenerationPin pin = store.pin();
                LiveViewCheckpointSegmentDirectoryReader directory =
                        new LiveViewCheckpointSegmentDirectoryReader(configuration);
                Path checkpointsDir = checkpointsDir(instance)
        ) {
            directory.of(checkpointsDir, pin.getSegmentDirectoryRootRef());
            final int[] sharedSegments = {0};
            directory.iterateAll(entry -> {
                if (entry.referenceCount > 1) {
                    sharedSegments[0]++;
                }
            });
            Assert.assertTrue(
                    viewName + ": no data segment is referenced by more than one root",
                    sharedSegments[0] > 0
            );
        }
    }

    private void assertUnboundedFrameStartRejected(String fn, String frame, String expectedMessage) throws Exception {
        try {
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS "
                    + "SELECT ts, sym, " + fn + "(px) OVER (PARTITION BY sym ORDER BY ts " + frame + ") AS v FROM fx");
            Assert.fail("expected an unbounded-frame-start reject for " + fn + " over " + frame);
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), expectedMessage);
            if (expectedMessage.startsWith("frame starting")) {
                Assert.assertTrue(
                        "the reject must name " + fn + " [msg=" + e.getFlyweightMessage() + ']',
                        Chars.contains(e.getFlyweightMessage(), "cannot use " + fn + "()")
                );
            }
        }
    }

    private void assertViewMatchesRecompute(String viewName, String projection, String columns) throws Exception {
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(SELECT ts, sym, " + projection + " FROM fx) ORDER BY 2, 1",
                "(SELECT ts, sym, " + columns + " FROM " + viewName + ") ORDER BY 2, 1",
                LOG,
                true
        );
        assertNoRefreshFaults(viewName);
    }

    private void assertViewsMatchRecompute(String[] views) throws Exception {
        for (String view : views) {
            if ("lv_issue".equals(view)) {
                assertViewMatchesRecompute(view, issueProjection(), "vol");
            } else {
                assertViewMatchesRecompute(view, projection(frameOf(view)), projectionColumns());
            }
        }
    }

    // Commits ROWS_PER_COMMIT ascending seconds and gives the refresh job a turn. Each second
    // carries a row for 'a' and 'b'; every fifteenth carries one for a NULL key too, so that
    // partition's narrow frames empty out between rows and its rings seal with nothing in
    // them. Scattered NULL values, plus a run of them for 'b' wider than the one-minute frame,
    // keep NULLs out of the ring and drive the finite count to zero across a seal. Values are
    // integers whose squares sum well under 2^53, so sum and sumSq stay exact in double
    // whatever order a frame adds and drops pairs; that keeps the exact recompute comparison
    // valid after a localized repair, which otherwise reaches the same value only up to
    // floating-point rounding.
    private void commitDense(LiveViewRefreshJob job, int commit) throws Exception {
        commitSeconds(job, (commit - 1) * ROWS_PER_COMMIT, ROWS_PER_COMMIT);
    }

    // Commits `count` ascending seconds from `firstSecond`; see commitDense for the data.
    private void commitSeconds(LiveViewRefreshJob job, int firstSecond, int count) throws Exception {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        final StringBuilder sql = new StringBuilder("INSERT INTO fx (ts, sym, px, qty) VALUES ");
        for (int i = 0; i < count; i++) {
            if (i > 0) {
                sql.append(", ");
            }
            final int second = firstSecond + i;
            final String ts = timestamp(second);
            final String valueA = second % 7 == 0 ? "null" : Integer.toString(second);
            final String valueB = second % 5 == 0 || (second >= 100 && second < 170)
                    ? "null"
                    : Integer.toString(second + 1_000);
            sql.append("('").append(ts).append("', 'a', ").append(valueA).append(", 1.0), ")
                    .append("('").append(ts).append("', 'b', ").append(valueB).append(", 1.0)");
            if (second % 15 == 0) {
                final String valueN = second % 45 == 0 ? "null" : Integer.toString(2 * second);
                sql.append(", ('").append(ts).append("', null, ").append(valueN).append(", 1.0)");
            }
        }
        execute(sql.toString());
        drainWalQueue();
        drainJob(job);
        drainWalQueue();
    }

    // Commits one out-of-order row half a second past an in-order timestamp - a fresh
    // designated timestamp, so it ties with no existing row and the recompute ordering stays
    // deterministic - and, given a job, gives it a turn.
    private void commitOutOfOrderHalfSecond(LiveViewRefreshJob job, int second, String sym, String value) throws Exception {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        final String ts = timestamp(second).replace(".000000Z", ".500000Z");
        execute("INSERT INTO fx (ts, sym, px, qty) VALUES ('" + ts + "', '" + sym + "', " + value + ", 1.0)");
        drainWalQueue();
        if (job != null) {
            drainJob(job);
            drainWalQueue();
        }
    }

    private void createBase() throws Exception {
        execute("CREATE TABLE fx (ts TIMESTAMP, sym SYMBOL, px DOUBLE, qty DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
    }

    private void createViews(String[] views) throws Exception {
        for (String view : views) {
            execute("CREATE LIVE VIEW " + view + " FLUSH EVERY 100ms START FROM NOW AS SELECT ts, sym, "
                    + ("lv_issue".equals(view) ? issueProjection() : projection(frameOf(view))) + " FROM fx");
        }
    }

    private void driveUntilRepairParks(LiveViewRefreshJob job, LiveViewInstance instance) {
        for (int pass = 0; pass < 64; pass++) {
            if (instance.getSuspendedRepair() != null) {
                return;
            }
            if (!job.processNotificationsForTest()) {
                break;
            }
        }
        Assert.assertNotNull("the repair never parked on its turn budget", instance.getSuspendedRepair());
    }

    private LiveViewCheckpointMetaStore openStore(LiveViewInstance instance) {
        final LiveViewCheckpointMetaStore store = new LiveViewCheckpointMetaStore(configuration);
        try (Path checkpointsDir = checkpointsDir(instance)) {
            store.of(checkpointsDir);
        }
        return store;
    }

    private void restartCycle() {
        engine.getLiveViewRegistry().clear();
        engine.buildViewGraphs();
    }

    // What the assertions read off the sole window function, captured before the factory
    // closes.
    private static class CompiledFunction {
        private final String className;
        private final LiveViewCheckpointDependency dependency;
        private final int fixedStateLength;
        private final boolean hasFrameLocalCheckpointState;
        private final boolean hasRingArena;
        private final int ringScalarWords;
        private final boolean supportsCheckpointRingState;
        private final boolean supportsCheckpointState;

        private CompiledFunction(WindowFunction function) {
            this.className = function.getClass().getSimpleName();
            this.dependency = function.checkpointDependency();
            this.fixedStateLength = function.checkpointStateFixedLength();
            this.hasFrameLocalCheckpointState = function.hasFrameLocalCheckpointState();
            this.hasRingArena = function.getRingArena() != null;
            this.ringScalarWords = function.checkpointRingScalarWords();
            this.supportsCheckpointRingState = function.supportsCheckpointRingState();
            this.supportsCheckpointState = function.supportsCheckpointState();
        }
    }
}
