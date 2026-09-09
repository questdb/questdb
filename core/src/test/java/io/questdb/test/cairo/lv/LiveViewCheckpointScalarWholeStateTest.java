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
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.engine.window.WindowRecordCursorFactory;
import io.questdb.std.ObjList;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * The scalar running aggregates keep the whole-state checkpoint image rather than the
 * timestamp-keyed ring the bounded-RANGE families share.
 * <p>
 * {@code stddev}/{@code variance}, the bivariate stats, {@code ema}/{@code vwema},
 * {@code ksum}, {@code count} over an unbounded frame, {@code row_number} and
 * {@code rank} each carry a fixed handful of accumulator words per partition however
 * long the view has run. One complete image per root is already the smallest thing a
 * root can write for them: a chunk reference costs more metadata than the image it
 * would replace, and an accumulator expires no rows, so adjacent roots hold no suffix
 * in common to reference. {@code lag} does keep a ring, but a positional one of the
 * last {@code offset} values with no timestamp in it, so it falls under the same
 * exclusion the ROWS families do (see {@link LiveViewCheckpointRowsWholeStateTest}) and
 * its declared offset bounds the image.
 * <p>
 * {@link #testScalarRunningAggregatesKeepWholeStateImage()} locks that disposition per
 * function, contrasting it with the {@code count} whose bounded RANGE frame does share
 * a ring, so the check is about the state shape and not a blanket opt-out.
 * {@link #testScalarWholeStateSurvivesRestartAndRepair()} proves the whole-state path is
 * complete: the views round-trip through a restart and an out-of-order correction,
 * matching a fresh recompute throughout.
 */
public class LiveViewCheckpointScalarWholeStateTest extends AbstractLiveViewTest {

    private static final int COMMITS = 5;
    // Every row sits inside one day, so the anchor never fires and the recompute oracle
    // is the equivalent plain PARTITION BY window.
    private static final String ANCHORED_WINDOW =
            "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))";
    // lag reads the row `offset` back and ignores its frame, but a live view turns away a
    // bare unbounded window, so it carries a bounded ROWS frame it never looks at.
    private static final String LAG_FRAME = "PARTITION BY sym ORDER BY ts ROWS BETWEEN 5 PRECEDING AND CURRENT ROW";
    private static final int ROWS_PER_COMMIT = 30;
    private static final String SCALAR_PROJECTION =
            "stddev_samp(x) OVER w AS sd, "
                    + "var_pop(x) OVER w AS va, "
                    + "corr(x, y) OVER w AS cr, "
                    + "covar_samp(x, y) OVER w AS cv, "
                    + "avg(x, 'period', 5) OVER w AS ep, "
                    + "avg(x, 'minute', 5) OVER w AS et, "
                    + "avg(x, 'period', 5, vol) OVER w AS vp, "
                    + "avg(x, 'minute', 5, vol) OVER w AS vt, "
                    + "ksum(x) OVER w AS ks, "
                    + "count(x) OVER w AS cn, "
                    + "row_number() OVER w AS rn, "
                    + "rank() OVER w AS rk";
    // The same projection against a plain partitioned window, which is what the anchored
    // one collapses to while every row stays inside one anchor bucket.
    private static final String SCALAR_RECOMPUTE = SCALAR_PROJECTION.replace(
            "OVER w", "OVER (PARTITION BY sym ORDER BY ts)"
    );

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
    public void testScalarRunningAggregatesKeepWholeStateImage() throws Exception {
        assertMemoryLeak(() -> {
            createBase();

            assertKeepsWholeState("stddev_samp(x)", "w", ANCHORED_WINDOW);
            assertKeepsWholeState("var_pop(x)", "w", ANCHORED_WINDOW);
            assertKeepsWholeState("corr(x, y)", "w", ANCHORED_WINDOW);
            assertKeepsWholeState("covar_samp(x, y)", "w", ANCHORED_WINDOW);
            assertKeepsWholeState("avg(x, 'period', 5)", "w", ANCHORED_WINDOW);
            assertKeepsWholeState("avg(x, 'minute', 5)", "w", ANCHORED_WINDOW);
            assertKeepsWholeState("avg(x, 'period', 5, vol)", "w", ANCHORED_WINDOW);
            assertKeepsWholeState("avg(x, 'minute', 5, vol)", "w", ANCHORED_WINDOW);
            assertKeepsWholeState("ksum(x)", "w", ANCHORED_WINDOW);
            assertKeepsWholeState("count(x)", "w", ANCHORED_WINDOW);
            assertKeepsWholeState("row_number()", "w", ANCHORED_WINDOW);
            assertKeepsWholeState("rank()", "w", ANCHORED_WINDOW);
            // lag's ring is positional and timestampless, so it is excluded for the
            // reason a ROWS ring is rather than for holding no ring at all.
            assertKeepsWholeState("lag(x, 2)", "(" + LAG_FRAME + ")", "");

            // Contrast: count over a bounded RANGE frame buffers a timestamp per
            // in-window row and does share the ring, so the assertions above pin the
            // state shape, not a blanket opt-out.
            final boolean[] disposition = checkpointDisposition(
                    "count(x)",
                    "(PARTITION BY sym ORDER BY ts RANGE BETWEEN '10' SECOND PRECEDING AND CURRENT ROW)",
                    ""
            );
            Assert.assertTrue("count over a bounded RANGE frame must support checkpoint state", disposition[0]);
            Assert.assertTrue("count over a bounded RANGE frame must share the checkpoint ring", disposition[1]);
        });
    }

    @Test
    public void testScalarWholeStateSurvivesRestartAndRepair() throws Exception {
        assertMemoryLeak(() -> {
            createBaseAndViews();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int commit = 1; commit <= COMMITS; commit++) {
                    commitDense(job, commit);
                }
                driveRefreshToQuiescence(job);
                assertViewsMatchRecompute();
                // Non-vacuity: both views carry every base row, so the comparisons
                // above run over a populated cursor rather than two empty ones.
                assertRowCount(2L * COMMITS * ROWS_PER_COMMIT);
            }

            // A restart rebuilds each view's runtime accumulators from the timeline
            // through restoreCheckpointState, so a following fault-free refresh that
            // matches a fresh recompute proves the whole-state image round-tripped.
            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // A pure-restore tick, before any new commit: the flag proves the
                // runtime state came back off the timeline rather than from a replay.
                drainJob(job);
                Assert.assertTrue(
                        "lv_scalar must restore its accumulators from the checkpoint timeline",
                        viewInstance("lv_scalar").isCheckpointRestoreSucceeded()
                );
                Assert.assertTrue(
                        "lv_lag must restore its ring from the checkpoint timeline",
                        viewInstance("lv_lag").isCheckpointRestoreSucceeded()
                );
                for (int commit = COMMITS + 1; commit <= COMMITS + 2; commit++) {
                    commitDense(job, commit);
                }
                driveRefreshToQuiescence(job);
                assertViewsMatchRecompute();

                // An out-of-order row deep in history at a fresh sub-second timestamp
                // (no designated-timestamp tie with an existing row) forces the repair
                // path. None of these functions declares frame-local state, so the
                // repair replays from the view boundary rather than localizing - the
                // result must still match the recompute.
                commitOutOfOrderHalfSecond(job, 43);
                driveRefreshToQuiescence(job);
                assertViewsMatchRecompute();
                // The late row is in the views, so the repair really re-emitted the
                // history above it rather than dropping the correction.
                assertRowCount(2L * (COMMITS + 2) * ROWS_PER_COMMIT + 1);
            }
        });
    }

    private void assertKeepsWholeState(String fn, String over, String windowClause) throws Exception {
        final boolean[] disposition = checkpointDisposition(fn, over, windowClause);
        final String label = fn + " OVER " + over;
        Assert.assertTrue(label + " must support checkpoint state (whole-state image)", disposition[0]);
        Assert.assertFalse(
                label + " must not share the timestamp-keyed checkpoint ring: it holds no timestamp-ordered ring",
                disposition[1]
        );
    }

    private void assertRowCount(long expectedRows) throws Exception {
        final String expected = "count\n" + expectedRows + "\n";
        TestUtils.printSql(engine, sqlExecutionContext, "SELECT count() FROM lv_scalar", sink);
        TestUtils.assertEquals(expected, sink);
        TestUtils.printSql(engine, sqlExecutionContext, "SELECT count() FROM lv_lag", sink);
        TestUtils.assertEquals(expected, sink);
    }

    private void assertViewsMatchRecompute() throws Exception {
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(SELECT ts, sym, " + SCALAR_RECOMPUTE + " FROM base) ORDER BY 2, 1",
                "(SELECT ts, sym, sd, va, cr, cv, ep, et, vp, vt, ks, cn, rn, rk FROM lv_scalar) ORDER BY 2, 1",
                LOG,
                true
        );
        assertNoRefreshFaults("lv_scalar");
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(SELECT ts, sym, lag(x, 2) OVER (" + LAG_FRAME + ") AS v FROM base) ORDER BY 2, 1",
                "(SELECT ts, sym, v FROM lv_lag) ORDER BY 2, 1",
                LOG,
                true
        );
        assertNoRefreshFaults("lv_lag");
    }

    // {supportsCheckpointState, supportsCheckpointRingState} for the sole window
    // function in a live-view SELECT. Evaluated before the factory closes.
    private boolean[] checkpointDisposition(String fn, String over, String windowClause) throws Exception {
        final String sql = "SELECT ts, sym, " + fn + " OVER " + over + " AS v FROM base " + windowClause;
        sqlExecutionContext.setLiveViewCompile(true);
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
            final WindowFunction function = functions.getQuick(0);
            return new boolean[]{function.supportsCheckpointState(), function.supportsCheckpointRingState()};
        } finally {
            sqlExecutionContext.setLiveViewCompile(false);
        }
    }

    // Commits ROWS_PER_COMMIT ascending rows per key at one-minute spacing and gives the
    // refresh job a turn. Every seventh x and every fifth y is NULL, so the accumulators
    // exercise their null-skip paths, and both the view and the oracle see the same rows.
    private void commitDense(LiveViewRefreshJob job, int commit) throws Exception {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        final StringBuilder sql = new StringBuilder("INSERT INTO base (ts, sym, x, y, vol) VALUES ");
        final int firstMinute = (commit - 1) * ROWS_PER_COMMIT;
        for (int i = 0; i < ROWS_PER_COMMIT; i++) {
            if (i > 0) {
                sql.append(", ");
            }
            final int minute = firstMinute + i;
            for (int k = 0; k < 2; k++) {
                if (k > 0) {
                    sql.append(", ");
                }
                sql.append("('").append(timestamp(minute, 0)).append("', '").append(k == 0 ? 'a' : 'b').append("', ")
                        .append(minute % 7 == k ? "null" : Double.toString(((minute * 31 + k * 29) % 101) + 0.25))
                        .append(", ")
                        .append(minute % 5 == k ? "null" : Double.toString(((minute * 17 + k * 13) % 89) + 0.5))
                        .append(", ")
                        .append(100.0 + ((minute * 7 + k) % 37))
                        .append(')');
            }
        }
        execute(sql.toString());
        drainWalQueue();
        drainJob(job);
        drainWalQueue();
    }

    // Commits one out-of-order row half a minute past an in-order timestamp - a fresh
    // designated timestamp, so it ties with no existing row and the recompute ordering
    // stays deterministic - and gives the refresh job a turn.
    private void commitOutOfOrderHalfSecond(LiveViewRefreshJob job, int minute) throws Exception {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        execute("INSERT INTO base (ts, sym, x, y, vol) VALUES ('"
                + timestamp(minute, 30) + "', 'a', 4242.5, 77.5, 250.0)");
        drainWalQueue();
        drainJob(job);
        drainWalQueue();
    }

    private void createBase() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE, y DOUBLE, vol DOUBLE) " +
                "TIMESTAMP(ts) PARTITION BY DAY WAL");
    }

    private void createBaseAndViews() throws Exception {
        createBase();
        execute("CREATE LIVE VIEW lv_scalar FLUSH EVERY 100ms START FROM NOW AS "
                + "SELECT ts, sym, " + SCALAR_PROJECTION + " FROM base " + ANCHORED_WINDOW);
        execute("CREATE LIVE VIEW lv_lag FLUSH EVERY 100ms START FROM NOW AS "
                + "SELECT ts, sym, lag(x, 2) OVER (" + LAG_FRAME + ") AS v FROM base");
    }

    private void restartCycle() {
        engine.getLiveViewRegistry().clear();
        engine.buildViewGraphs();
    }

    private static String timestamp(int minute, int second) {
        return String.format("2026-03-01T%02d:%02d:%02d.000000Z", minute / 60, minute % 60, second);
    }

    private LiveViewInstance viewInstance(String viewName) {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance(viewName);
        Assert.assertNotNull("live view '" + viewName + "' must be registered", instance);
        return instance;
    }
}
