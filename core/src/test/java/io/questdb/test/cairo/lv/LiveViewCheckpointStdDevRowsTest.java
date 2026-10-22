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
import io.questdb.cairo.lv.LiveViewCheckpointRepairPlan;
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
import io.questdb.std.Rnd;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * The stddev/variance family over a bounded, partitioned ROWS frame keeps a whole-state
 * checkpoint image: {@code (sum, sumSq, count, loIdx)} plus the frame's fixed-size ring of
 * raw values, NaN placeholders included. One factory backs all six spellings -
 * {@code stddev}, {@code stddev_samp}, {@code stddev_pop}, {@code variance},
 * {@code var_samp} and {@code var_pop} - and two flags pick the output, so the same
 * checkpoint codec covers every one of them.
 * <p>
 * {@link #testBoundedRowsFramesDeclareWholeStateCheckpoint()} pins the disposition per
 * function and frame shape. {@link #testBoundedRowsSurvivesRestartAndRepair()} proves the
 * image round-trips: the views restore from the timeline after a restart, and an
 * out-of-order correction resumes each of them from a sealed root, matching a fresh
 * recompute of the plain query throughout. The class declines frame-local state, so no
 * correction takes the localized rebuild: its warm-up would re-sum the frame and drift from
 * the plain query's running sums past the contract's floating ceiling.
 * {@link #testBoundedRowsRestoresExactValuesAcrossRestart()} pins the restored values
 * against hand-computed ones, including a frame whose only cell is a restored NULL.
 * {@link #testBoundedRowsContinuesBitExactlyAcrossRestart()} proves on tick-like data that
 * every row before and after a restart equals the plain query's bit for bit, which only a
 * restore that carries the running sums verbatim achieves.
 */
public class LiveViewCheckpointStdDevRowsTest extends AbstractLiveViewTest {

    private static final int COMMITS = 6;
    private static final String FRAME_CURRENT = "PARTITION BY sym ORDER BY ts ROWS BETWEEN 10 PRECEDING AND CURRENT ROW";
    // EXCLUDE CURRENT ROW folds into a high bound one row below the current one.
    private static final String FRAME_EXCLUDED = "PARTITION BY sym ORDER BY ts ROWS BETWEEN 6 PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW";
    // The issue's own spelling: the short form of N PRECEDING AND CURRENT ROW.
    private static final String FRAME_ISSUE = "PARTITION BY sym ORDER BY ts ROWS 19 PRECEDING";
    // A high bound below the current row: the value entering the frame comes off the ring.
    private static final String FRAME_LAGGED = "PARTITION BY sym ORDER BY ts ROWS BETWEEN 10 PRECEDING AND 3 PRECEDING";
    private static final String[] FUNCTIONS = {"stddev", "stddev_samp", "stddev_pop", "variance", "var_samp", "var_pop"};
    // Distinct ascending seconds per key per commit. Well above every frame width, so the
    // frames fill and the sealed roots carry a warmed-up ring.
    private static final int ROWS_PER_COMMIT = 40;
    // Seconds per commit in the bit-exact case.
    private static final int TICK_COMMIT_SECONDS = 60;
    // The bit-exact case's frames: the issue's short form (frameIncludesCurrentValue), a lagged
    // high bound, and EXCLUDE CURRENT ROW, which folds into a high bound one row down. All three
    // drop a value from the running sums on every row once full, so the sums carry the rounding
    // of hundreds of adds and drops by the time the view seals.
    private static final String[] TICK_FRAMES = {
            FRAME_ISSUE,
            "PARTITION BY sym ORDER BY ts ROWS BETWEEN 12 PRECEDING AND 3 PRECEDING",
            FRAME_EXCLUDED
    };

    @After
    public void resetClock() {
        setCurrentMicros(-1);
    }

    @Before
    public void setUpCadence() {
        // One logical root per commit, the densest cadence the view can seal, so the
        // checkpoint timeline a restart restores from is deep.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setCurrentMicros(0);
    }

    @Test
    public void testBoundedRowsContinuesBitExactlyAcrossRestart() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            // Tick-like prices on a one-cent random walk near 65_000 and 3_100. The squares leave
            // rounding residue in sumSq as values enter and leave the frame, and the variance
            // formula subtracts two nearly equal sums, which amplifies it. A restore that
            // re-summed the ring instead of taking the carried sum and sumSq emits different bits
            // from the plain query here; integer data would hide that, as it sums to the same
            // doubles in every order.
            final String projection = tickProjection();
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS SELECT ts, sym, "
                    + projection + " FROM fx");
            final String plain = "(SELECT ts, sym, " + projection + " FROM fx) ORDER BY sym, ts";
            final String view = "SELECT ts, sym, " + tickColumns() + " FROM lv ORDER BY sym, ts";
            final Rnd rnd = new Rnd(4242, 42);
            final double[] prices = {65_000.0, 3_100.0};
            int second = 0;

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int commit = 0; commit < 5; commit++) {
                    commitTicks(job, rnd, prices, second);
                    second += TICK_COMMIT_SECONDS;
                    if (commit == 0) {
                        driveSeedToCompletion(job, "lv");
                    }
                    driveRefreshToQuiescence(job);
                }
                assertNoRefreshFaults("lv");
                assertViewPrintsLikePlain(view, plain);
            }

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int commit = 0; commit < 3; commit++) {
                    commitTicks(job, rnd, prices, second);
                    second += TICK_COMMIT_SECONDS;
                    driveRefreshToQuiescence(job);
                }
                assertRestoredFromTimeline("lv");
                assertNoRefreshFaults("lv");
                // The printed form is the shortest text that round-trips a double, so equal text
                // is equal bits: every row after the restart runs on the carried sums.
                assertViewPrintsLikePlain(view, plain);
            }
        });
    }

    @Test
    public void testBoundedRowsFramesDeclareWholeStateCheckpoint() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            final String[] frames = {FRAME_CURRENT, FRAME_LAGGED, FRAME_EXCLUDED, FRAME_ISSUE};
            for (String fn : FUNCTIONS) {
                for (String frame : frames) {
                    assertWholeStateDisposition(fn + "(px)", frame);
                }
                // Outside a live view compile the same class declines: the codec needs the
                // tombstone slot and the key layout copy only a live view compile builds.
                Assert.assertFalse(
                        fn + " outside a live view compile must not claim checkpoint state",
                        compileSoleWindowFunction(fn + "(px)", FRAME_CURRENT, false).supportsCheckpointState
                );
            }
        });
    }

    @Test
    public void testBoundedRowsRejectsUnboundedFrameStart() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            for (String fn : FUNCTIONS) {
                // UNBOUNDED PRECEDING AND 1 PRECEDING compiles to the same bounded-ROWS class
                // with an unbounded low bound; the finite-influence gate turns it away first.
                assertUnboundedFrameStartRejected(fn, "ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING");
                assertUnboundedFrameStartRejected(fn, "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");
            }
        });
    }

    @Test
    public void testBoundedRowsRejectsUnsupportedPartitionKey() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE ux (ts TIMESTAMP, u UUID, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            for (String fn : FUNCTIONS) {
                try {
                    execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS "
                            + "SELECT ts, u, " + fn + "(px) OVER (PARTITION BY u ORDER BY ts "
                            + "ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS v FROM ux");
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
    public void testBoundedRowsRestoresExactValuesAcrossRestart() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            // The issue's view, verbatim apart from the table name the base already carries.
            execute("CREATE LIVE VIEW lv1 FLUSH EVERY 1s START FROM BEGINNING AS "
                    + "SELECT ts, sym, stddev_samp(px) OVER (" + FRAME_ISSUE + ") AS vol FROM fx");
            execute("""
                    CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS
                    SELECT ts, sym,
                           var_pop(px) OVER w AS vp,
                           var_samp(px) OVER w AS vs,
                           stddev_pop(px) OVER w AS sp,
                           variance(px) OVER w AS va
                    FROM fx
                    WINDOW w AS (PARTITION BY sym ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)""");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("""
                        INSERT INTO fx (ts, sym, px, qty) VALUES
                        ('2026-01-01T00:00:00.000000Z', 'a', 1.0, 1.0),
                        ('2026-01-01T00:00:00.000000Z', 'b', 10.0, 1.0),
                        ('2026-01-01T00:00:01.000000Z', 'a', 3.0, 1.0),
                        ('2026-01-01T00:00:01.000000Z', 'b', 14.0, 1.0),
                        ('2026-01-01T00:00:02.000000Z', 'a', null, 1.0)""");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
                driveSeedToCompletion(job, "lv");
                driveSeedToCompletion(job, "lv1");
                driveRefreshToQuiescence(job);
                assertNoRefreshFaults("lv");
                assertQuery("SELECT ts, sym, vp, vs, sp, va FROM lv ORDER BY sym, ts")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                ts\tsym\tvp\tvs\tsp\tva
                                2026-01-01T00:00:00.000000Z\ta\t0.0\tnull\t0.0\tnull
                                2026-01-01T00:00:01.000000Z\ta\t1.0\t2.0\t1.0\t2.0
                                2026-01-01T00:00:02.000000Z\ta\t0.0\tnull\t0.0\tnull
                                2026-01-01T00:00:00.000000Z\tb\t0.0\tnull\t0.0\tnull
                                2026-01-01T00:00:01.000000Z\tb\t4.0\t8.0\t2.0\t8.0
                                """);
            }

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // a's ring restores holding only the NULL, so its next frame has one finite
                // value; b's ring restores holding 14, which its next frame pairs with 20.
                execute("""
                        INSERT INTO fx (ts, sym, px, qty) VALUES
                        ('2026-01-01T00:00:03.000000Z', 'a', 5.0, 1.0),
                        ('2026-01-01T00:00:03.000000Z', 'b', 20.0, 1.0),
                        ('2026-01-01T00:00:04.000000Z', 'a', 9.0, 1.0)""");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
                driveRefreshToQuiescence(job);
                assertRestoredFromTimeline("lv");
                assertRestoredFromTimeline("lv1");
                assertNoRefreshFaults("lv");
                assertQuery("SELECT ts, sym, vp, vs, sp, va FROM lv ORDER BY sym, ts")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                ts\tsym\tvp\tvs\tsp\tva
                                2026-01-01T00:00:00.000000Z\ta\t0.0\tnull\t0.0\tnull
                                2026-01-01T00:00:01.000000Z\ta\t1.0\t2.0\t1.0\t2.0
                                2026-01-01T00:00:02.000000Z\ta\t0.0\tnull\t0.0\tnull
                                2026-01-01T00:00:03.000000Z\ta\t0.0\tnull\t0.0\tnull
                                2026-01-01T00:00:04.000000Z\ta\t4.0\t8.0\t2.0\t8.0
                                2026-01-01T00:00:00.000000Z\tb\t0.0\tnull\t0.0\tnull
                                2026-01-01T00:00:01.000000Z\tb\t4.0\t8.0\t2.0\t8.0
                                2026-01-01T00:00:03.000000Z\tb\t9.0\t18.0\t3.0\t18.0
                                """);
                assertViewMatchesRecompute("lv1", "stddev_samp(px) OVER (" + FRAME_ISSUE + ") AS vol", "vol");
            }
        });
    }

    @Test
    public void testBoundedRowsSurvivesRestartAndRepair() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            createView("lv_cur", FRAME_CURRENT);
            createView("lv_lag", FRAME_LAGGED);
            createView("lv_excl", FRAME_EXCLUDED);
            execute("CREATE LIVE VIEW lv1 FLUSH EVERY 100ms START FROM NOW AS "
                    + "SELECT ts, sym, stddev_samp(px) OVER (" + FRAME_ISSUE + ") AS vol FROM fx");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int commit = 1; commit <= COMMITS; commit++) {
                    commitDense(job, commit);
                }
                driveRefreshToQuiescence(job);
                assertViewsMatchRecompute();
            }

            // A restart rebuilds each view's runtime ROWS state from the timeline through
            // restoreCheckpointState, so a following refresh that matches a fresh recompute
            // proves the whole-state image round-tripped.
            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int commit = COMMITS + 1; commit <= COMMITS + 3; commit++) {
                    commitDense(job, commit);
                }
                driveRefreshToQuiescence(job);
                assertRestoredFromTimeline("lv_cur");
                assertRestoredFromTimeline("lv_lag");
                assertRestoredFromTimeline("lv_excl");
                assertRestoredFromTimeline("lv1");
                assertViewsMatchRecompute();

                // Out-of-order rows deep in history at fresh sub-second timestamps (no
                // designated-timestamp tie with an existing row) force the repair path, which
                // restores a predecessor whole-state root and replays the corrected interval
                // forward. The second one is a NULL, which lands as a NaN cell in the ring.
                commitOutOfOrderHalfSecond(job, 57, "a", "4242");
                driveRefreshToQuiescence(job);
                assertViewsMatchRecompute();
                assertRepairResumedFromRoot();

                commitOutOfOrderHalfSecond(job, 133, "b", "null");
                driveRefreshToQuiescence(job);
                assertViewsMatchRecompute();
                assertRepairResumedFromRoot();
            }
        });
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

    // Every spelling over every bit-exact frame, as columns c0 to c17.
    private static String tickColumns() {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0, n = TICK_FRAMES.length * FUNCTIONS.length; i < n; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append('c').append(i);
        }
        return sb.toString();
    }

    private static String tickProjection() {
        final StringBuilder sb = new StringBuilder();
        int column = 0;
        for (String frame : TICK_FRAMES) {
            for (String fn : FUNCTIONS) {
                if (column > 0) {
                    sb.append(", ");
                }
                sb.append(fn).append("(px) OVER (").append(frame).append(") AS c").append(column++);
            }
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

    private void assertRepairResumedFromRoot() {
        for (String view : new String[]{"lv_cur", "lv_lag", "lv_excl", "lv1"}) {
            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance(view);
            Assert.assertNotNull(view, instance);
            // The function declines frame-local state, so the view has no ROWS repair plan and
            // the late row cannot take the localized rebuild, whose warm-up would re-sum the
            // frame. It resumes from the sealed root below the change instead, restoring the
            // whole-state image the root carries and replaying forward from it.
            Assert.assertEquals(view, LiveViewInstance.REPAIR_PLAN_NONE, instance.getCheckpointRepairDependencyPlans());
            final int denial = instance.getCheckpointRepairLastDenialReason();
            Assert.assertEquals(
                    view + " must lose the localized plan to the declined frame-local claim",
                    "incomplete dependency",
                    LiveViewCheckpointRepairPlan.denialReasonName(denial)
            );
            Assert.assertEquals(
                    view + " must resume from a sealed root",
                    "resume from anchor",
                    LiveViewCheckpointRepairPlan.dispositionName(instance.getCheckpointRepairLastDisposition(), denial)
            );
        }
    }

    private void assertUnboundedFrameStartRejected(String fn, String frame) throws Exception {
        try {
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS "
                    + "SELECT ts, sym, " + fn + "(px) OVER (PARTITION BY sym ORDER BY ts " + frame + ") AS v FROM fx");
            Assert.fail("expected an unbounded-frame-start reject for " + fn + " over " + frame);
        } catch (SqlException e) {
            Assert.assertTrue(
                    "wrong message [msg=" + e.getFlyweightMessage() + "] for " + fn + " over " + frame,
                    Chars.contains(e.getFlyweightMessage(), "cannot use " + fn + "()")
                            && Chars.contains(e.getFlyweightMessage(), "frame starting at UNBOUNDED PRECEDING")
            );
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

    // Prints the plain query and asserts the view prints the same text. Unlike the
    // assertSqlCursors comparison, which allows a small tolerance on doubles, equal shortest
    // round-trip text means equal bits.
    private void assertViewPrintsLikePlain(String viewQuery, String plainQuery) throws Exception {
        printSql(plainQuery);
        final String expected = sink.toString();
        assertQuery(viewQuery)
                .noLeakCheck()
                .expectSize()
                .returns(expected);
    }

    private void assertViewsMatchRecompute() throws Exception {
        assertViewMatchesRecompute("lv_cur", projection(FRAME_CURRENT), projectionColumns());
        assertViewMatchesRecompute("lv_lag", projection(FRAME_LAGGED), projectionColumns());
        assertViewMatchesRecompute("lv_excl", projection(FRAME_EXCLUDED), projectionColumns());
        assertViewMatchesRecompute("lv1", "stddev_samp(px) OVER (" + FRAME_ISSUE + ") AS vol", "vol");
    }

    private void assertWholeStateDisposition(String fn, String frame) throws Exception {
        final String label = fn + " OVER (" + frame + ")";
        final CompiledFunction compiled = compileSoleWindowFunction(fn, frame, true);
        Assert.assertEquals(label, "StdDevOverPartitionRowsFrameFunction", compiled.className);
        Assert.assertTrue(label + " must support checkpoint state", compiled.supportsCheckpointState);
        Assert.assertFalse(
                label + " must keep the whole-state image: a ROWS ring is positional, not timestamp-keyed",
                compiled.supportsCheckpointRingState
        );
        // The image carries the frame's ring, so its width is not fixed.
        Assert.assertEquals(label, -1, compiled.fixedStateLength);
        // A warm-up from the frame's lower edge would re-sum the running sums, and the
        // formula's cancellation would carry the difference past the floating ceiling, so the
        // class declines the frame-local claim and every repair it takes is exact.
        Assert.assertFalse(label + " must decline frame-local state", compiled.hasFrameLocalCheckpointState);
        final LiveViewCheckpointDependency dependency = compiled.dependency;
        Assert.assertNotNull(label, dependency);
        Assert.assertTrue(label, dependency.isFiniteRows());
        Assert.assertFalse(label, dependency.hasFrameLocalState());
        Assert.assertEquals(label, NumericConvergence.EXACT, dependency.getNumericConvergence());
    }

    // Commits ROWS_PER_COMMIT ascending seconds and gives the refresh job a turn. Each second
    // carries a row for 'a', 'b' and a NULL key. Scattered NULL values, plus a run of them for
    // 'b' longer than every frame, put NaN cells in the ring and drive the finite count to
    // zero across the freeze/restore round trip. Values are integers whose squares sum well
    // under 2^53, so sum and sumSq stay exact in double whatever order a frame adds and drops
    // rows.
    private void commitDense(LiveViewRefreshJob job, int commit) throws Exception {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        final StringBuilder sql = new StringBuilder("INSERT INTO fx (ts, sym, px, qty) VALUES ");
        final int firstSecond = (commit - 1) * ROWS_PER_COMMIT;
        for (int i = 0; i < ROWS_PER_COMMIT; i++) {
            if (i > 0) {
                sql.append(", ");
            }
            final int second = firstSecond + i;
            final String ts = timestamp(second);
            final String valueA = second % 7 == 0 ? "null" : Integer.toString(second);
            final String valueB = second % 5 == 0 || (second >= 100 && second < 125)
                    ? "null"
                    : Integer.toString(second + 1_000);
            final String valueN = second % 4 == 0 ? "null" : Integer.toString(2 * second);
            sql.append("('").append(ts).append("', 'a', ").append(valueA).append(", 1.0), ")
                    .append("('").append(ts).append("', 'b', ").append(valueB).append(", 1.0), ")
                    .append("('").append(ts).append("', null, ").append(valueN).append(", 1.0)");
        }
        execute(sql.toString());
        drainWalQueue();
        drainJob(job);
        drainWalQueue();
    }

    // Commits one out-of-order row half a second past an in-order timestamp - a fresh
    // designated timestamp, so it ties with no existing row and the recompute ordering stays
    // deterministic - and gives the refresh job a turn.
    private void commitOutOfOrderHalfSecond(LiveViewRefreshJob job, int second, String sym, String value) throws Exception {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        final String ts = timestamp(second).replace(".000000Z", ".500000Z");
        execute("INSERT INTO fx (ts, sym, px, qty) VALUES ('" + ts + "', '" + sym + "', " + value + ", 1.0)");
        drainWalQueue();
        drainJob(job);
        drainWalQueue();
    }

    // Commits TICK_COMMIT_SECONDS ascending seconds from `firstSecond` of tick-like trades for
    // 'btc' and 'eth' and gives the refresh job a turn. Each price walks in one-cent steps from
    // where `prices` left it, so no value is an integer. Every eleventh 'btc' second and every
    // thirteenth 'eth' second has a NULL price, which lands as a NaN cell in the ring.
    private void commitTicks(LiveViewRefreshJob job, Rnd rnd, double[] prices, int firstSecond) throws Exception {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        final StringBuilder sql = new StringBuilder("INSERT INTO fx (ts, sym, px, qty) VALUES ");
        for (int i = 0; i < TICK_COMMIT_SECONDS; i++) {
            final int second = firstSecond + i;
            final String ts = timestamp(second);
            for (int s = 0; s < 2; s++) {
                prices[s] += (rnd.nextInt(201) - 100) * 0.01;
                final boolean isPxNull = s == 0 ? second % 11 == 5 : second % 13 == 7;
                if (i > 0 || s > 0) {
                    sql.append(", ");
                }
                sql.append("('").append(ts).append("', '").append(s == 0 ? "btc" : "eth").append("', ")
                        .append(isPxNull ? "null" : Double.toString(prices[s])).append(", 1.0)");
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

    private void createView(String name, String frame) throws Exception {
        execute("CREATE LIVE VIEW " + name + " FLUSH EVERY 100ms START FROM NOW AS "
                + "SELECT ts, sym, " + projection(frame) + " FROM fx");
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
        private final boolean supportsCheckpointRingState;
        private final boolean supportsCheckpointState;

        private CompiledFunction(WindowFunction function) {
            this.className = function.getClass().getSimpleName();
            this.dependency = function.checkpointDependency();
            this.fixedStateLength = function.checkpointStateFixedLength();
            this.hasFrameLocalCheckpointState = function.hasFrameLocalCheckpointState();
            this.supportsCheckpointRingState = function.supportsCheckpointRingState();
            this.supportsCheckpointState = function.supportsCheckpointState();
        }
    }
}
