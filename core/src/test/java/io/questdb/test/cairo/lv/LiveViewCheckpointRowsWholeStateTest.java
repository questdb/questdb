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
 * The value and aggregate window functions over a partitioned ROWS frame -
 * {@code avg}, {@code sum}, {@code ksum}, {@code first_value}, {@code last_value},
 * {@code nth_value} - keep the whole-state checkpoint image rather than sharing the
 * timestamp-keyed ring that the RANGE-framed forms of these functions use.
 * <p>
 * A ROWS frame keeps a fixed count of live rows regardless of timestamp, and QuestDB
 * admits many rows at one designated timestamp. The shared ring's seal splits a
 * partition's stream at the previous boundary's maximum timestamp; a boundary can drop
 * and add ROWS rows that all sit at that split timestamp, which the split cannot tell
 * apart, so a ROWS ring cannot use the timestamp-keyed chunk layer without a distinct
 * positional model. Its state is bounded by the frame's declared row count, so the
 * family writes one complete state image per root instead.
 * <p>
 * {@link #testRowsFramedFunctionsKeepWholeStateImage()} locks that disposition per
 * function, contrasting it with a RANGE frame that does share the ring, so the check is
 * about the framing and not a blanket opt-out.
 * {@link #testRowsWholeStateSurvivesRestartAndRepair()} proves the whole-state path is
 * complete: the ROWS views round-trip through a restart and localize an out-of-order
 * correction, matching a fresh recompute throughout.
 */
public class LiveViewCheckpointRowsWholeStateTest extends AbstractLiveViewTest {

    private static final int COMMITS = 6;
    // Distinct ascending seconds per key per commit. Well above the ROWS frame width
    // so the frames fill and the sealed roots carry a warmed-up ring.
    private static final int ROWS_PER_COMMIT = 40;

    private static final String AVG_FN = "avg(x)";
    private static final String AVG_FRAME = "PARTITION BY sym ORDER BY ts ROWS BETWEEN 10 PRECEDING AND CURRENT ROW";
    private static final String FIRST_FN = "first_value(x)";
    private static final String FIRST_FRAME = "PARTITION BY sym ORDER BY ts ROWS BETWEEN 10 PRECEDING AND CURRENT ROW";
    private static final String KSUM_FN = "ksum(x)";
    private static final String KSUM_FRAME = "PARTITION BY sym ORDER BY ts ROWS BETWEEN 10 PRECEDING AND CURRENT ROW";
    // last_value over a frame ending at the current row is a stateless per-row
    // projection, so its trailing frame keeps state to checkpoint.
    private static final String LAST_FN = "last_value(x)";
    private static final String LAST_FRAME = "PARTITION BY sym ORDER BY ts ROWS BETWEEN 10 PRECEDING AND 2 PRECEDING";
    private static final String NTH_FN = "nth_value(x, 3)";
    private static final String NTH_FRAME = "PARTITION BY sym ORDER BY ts ROWS BETWEEN 10 PRECEDING AND CURRENT ROW";
    private static final String SUM_FN = "sum(x)";
    private static final String SUM_FRAME = "PARTITION BY sym ORDER BY ts ROWS BETWEEN 10 PRECEDING AND CURRENT ROW";

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
    public void testRowsFramedFunctionsKeepWholeStateImage() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");

            assertRowsFunctionKeepsWholeState(AVG_FN, AVG_FRAME);
            assertRowsFunctionKeepsWholeState(SUM_FN, SUM_FRAME);
            assertRowsFunctionKeepsWholeState(KSUM_FN, KSUM_FRAME);
            assertRowsFunctionKeepsWholeState(FIRST_FN, FIRST_FRAME);
            assertRowsFunctionKeepsWholeState(NTH_FN, NTH_FRAME);
            assertRowsFunctionKeepsWholeState(LAST_FN, LAST_FRAME);
            // Unbounded-preceding ROWS resolves to a distinct running-aggregate class;
            // it is whole-state too (a scalar sum/count per partition, still not a ring).
            assertRowsFunctionKeepsWholeState(AVG_FN, "PARTITION BY sym ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");

            // Contrast: the same family over a bounded RANGE frame does share the ring,
            // so the assertions above pin the framing, not a blanket opt-out.
            assertRangeFunctionSharesRing(AVG_FN, "PARTITION BY sym ORDER BY ts RANGE BETWEEN 10 PRECEDING AND CURRENT ROW");
        });
    }

    @Test
    public void testRowsWholeStateSurvivesRestartAndRepair() throws Exception {
        assertMemoryLeak(() -> {
            createBaseAndViews();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int commit = 1; commit <= COMMITS; commit++) {
                    commitDense(job, commit);
                }
                driveRefreshToQuiescence(job);
                assertViewsMatchRecompute();
            }

            // A restart rebuilds each view's runtime ROWS state from the timeline
            // through restoreCheckpointState, so a following fault-free refresh that
            // matches a fresh recompute proves the whole-state image round-tripped.
            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int commit = COMMITS + 1; commit <= COMMITS + 3; commit++) {
                    commitDense(job, commit);
                }
                driveRefreshToQuiescence(job);
                assertViewsMatchRecompute();

                // An out-of-order row deep in history at a fresh sub-second timestamp
                // (no designated-timestamp tie with an existing row) forces the repair
                // path, which restores a predecessor whole-state root and replays the
                // corrected interval forward.
                commitOutOfOrderHalfSecond(job, 57, 4242);
                driveRefreshToQuiescence(job);
                assertViewsMatchRecompute();
            }
        });
    }

    private void assertRangeFunctionSharesRing(String fn, String frame) throws Exception {
        final boolean[] disposition = checkpointDisposition(fn, frame);
        final String label = fn + " OVER (" + frame + ")";
        Assert.assertTrue(label + " must support checkpoint state", disposition[0]);
        Assert.assertTrue(label + " over a bounded RANGE frame must share the checkpoint ring", disposition[1]);
    }

    private void assertRowsFunctionKeepsWholeState(String fn, String frame) throws Exception {
        final boolean[] disposition = checkpointDisposition(fn, frame);
        final String label = fn + " OVER (" + frame + ")";
        Assert.assertTrue(label + " must support checkpoint state (whole-state image)", disposition[0]);
        Assert.assertFalse(
                label + " must not share the timestamp-keyed checkpoint ring: a ROWS frame is positional",
                disposition[1]
        );
    }

    private void assertViewMatchesRecompute(String viewName, String fn, String frame) throws Exception {
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(SELECT ts, sym, " + fn + " OVER (" + frame + ") AS v FROM base) ORDER BY 2, 1",
                "(SELECT ts, sym, v FROM " + viewName + ") ORDER BY 2, 1",
                LOG,
                true
        );
        assertNoRefreshFaults(viewName);
    }

    private void assertViewsMatchRecompute() throws Exception {
        assertViewMatchesRecompute("lv_avg", AVG_FN, AVG_FRAME);
        assertViewMatchesRecompute("lv_sum", SUM_FN, SUM_FRAME);
        assertViewMatchesRecompute("lv_ksum", KSUM_FN, KSUM_FRAME);
        assertViewMatchesRecompute("lv_first", FIRST_FN, FIRST_FRAME);
        assertViewMatchesRecompute("lv_nth", NTH_FN, NTH_FRAME);
        assertViewMatchesRecompute("lv_last", LAST_FN, LAST_FRAME);
    }

    // {supportsCheckpointState, supportsCheckpointRingState} for the sole window
    // function in a live-view SELECT. Evaluated before the factory closes.
    private boolean[] checkpointDisposition(String fn, String frame) throws Exception {
        final String sql = "SELECT ts, sym, " + fn + " OVER (" + frame + ") AS v FROM base";
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

    // Commits ROWS_PER_COMMIT ascending rows per key at one-second spacing and gives the
    // refresh job a turn. Every seventh/fifth row's value is NULL so the whole-state ring
    // carries NaN across the freeze/restore round trip. Values are integers well under
    // 2^53, so a ROWS avg/sum/ksum running total is exact in double regardless of the
    // order its frame adds and drops rows; that keeps the exact recompute comparison valid
    // for the accumulating functions after a warm-up-localized repair, which otherwise
    // reaches the same value only up to floating-point rounding.
    private void commitDense(LiveViewRefreshJob job, int commit) throws Exception {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        final StringBuilder sql = new StringBuilder("INSERT INTO base (ts, sym, x) VALUES ");
        final int firstSecond = (commit - 1) * ROWS_PER_COMMIT;
        for (int i = 0; i < ROWS_PER_COMMIT; i++) {
            if (i > 0) {
                sql.append(", ");
            }
            final int second = firstSecond + i;
            final String value = second % 7 == 0 ? "null" : Integer.toString(second);
            final String valueB = second % 5 == 0 ? "null" : Integer.toString(second + 1_000);
            sql.append("('").append(timestamp(second)).append("', 'a', ").append(value).append("), ")
                    .append("('").append(timestamp(second)).append("', 'b', ").append(valueB).append(')');
        }
        execute(sql.toString());
        drainWalQueue();
        drainJob(job);
        drainWalQueue();
    }

    // Commits one out-of-order row half a second past an in-order timestamp - a fresh
    // designated timestamp, so it ties with no existing row and the recompute ordering
    // stays deterministic - and gives the refresh job a turn.
    private void commitOutOfOrderHalfSecond(LiveViewRefreshJob job, int second, int value) throws Exception {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        final String ts = timestamp(second).replace(".000000Z", ".500000Z");
        execute("INSERT INTO base (ts, sym, x) VALUES ('" + ts + "', 'a', " + value + ")");
        drainWalQueue();
        drainJob(job);
        drainWalQueue();
    }

    private void createBaseAndViews() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE LIVE VIEW lv_avg FLUSH EVERY 100ms START FROM NOW AS "
                + "SELECT ts, sym, " + AVG_FN + " OVER (" + AVG_FRAME + ") AS v FROM base");
        execute("CREATE LIVE VIEW lv_sum FLUSH EVERY 100ms START FROM NOW AS "
                + "SELECT ts, sym, " + SUM_FN + " OVER (" + SUM_FRAME + ") AS v FROM base");
        execute("CREATE LIVE VIEW lv_ksum FLUSH EVERY 100ms START FROM NOW AS "
                + "SELECT ts, sym, " + KSUM_FN + " OVER (" + KSUM_FRAME + ") AS v FROM base");
        execute("CREATE LIVE VIEW lv_first FLUSH EVERY 100ms START FROM NOW AS "
                + "SELECT ts, sym, " + FIRST_FN + " OVER (" + FIRST_FRAME + ") AS v FROM base");
        execute("CREATE LIVE VIEW lv_nth FLUSH EVERY 100ms START FROM NOW AS "
                + "SELECT ts, sym, " + NTH_FN + " OVER (" + NTH_FRAME + ") AS v FROM base");
        execute("CREATE LIVE VIEW lv_last FLUSH EVERY 100ms START FROM NOW AS "
                + "SELECT ts, sym, " + LAST_FN + " OVER (" + LAST_FRAME + ") AS v FROM base");
    }

    private void restartCycle() {
        engine.getLiveViewRegistry().clear();
        engine.buildViewGraphs();
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
}
