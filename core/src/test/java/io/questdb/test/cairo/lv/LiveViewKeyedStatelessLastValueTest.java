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
import io.questdb.griffin.SqlException;
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
 * {@code last_value(x) OVER (PARTITION BY key ORDER BY ts RANGE BETWEEN UNBOUNDED
 * PRECEDING AND CURRENT ROW)} - and the implicit spelling of the same frame,
 * {@code OVER (PARTITION BY key ORDER BY ts)} - is a per-row projection of its own
 * argument: it compiles to a class whose {@code computeNext} reads the row it was
 * handed, constructed with no partition map at all. It therefore keeps no
 * per-partition state and has no forward influence, which is what the
 * bare-unbounded-window rule exists to bound.
 * <p>
 * That rule used to run per window definition and so could not see which call used
 * the window; it refused this shape along with the accumulators it is aimed at. It
 * now runs per window-function call, and this suite pins both halves of the result:
 * {@link #testKeyedStatelessRangeLastValueEligibility()} covers what the carve-out
 * admits and, at equal length, what must stay rejected, and
 * {@link #testKeyedStatelessRangeLastValueSurvivesRestartAndRepair()} proves the
 * admitted shape is complete at runtime - it seals checkpoint roots, restores across
 * a restart, localizes an out-of-order correction, and matches a fresh recompute
 * throughout.
 */
public class LiveViewKeyedStatelessLastValueTest extends AbstractLiveViewTest {

    private static final int COMMITS = 6;
    private static final String FN = "last_value(x)";
    // The shape the finding is about, spelled out. Its implicit spelling -
    // OVER (PARTITION BY sym ORDER BY ts) - is the same frame and rides the same rule.
    private static final String FRAME = "PARTITION BY sym ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW";
    private static final int ROWS_PER_COMMIT = 20;

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
    public void testKeyedStatelessRangeLastValueEligibility() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE, y DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");

            // The class the admitted shape compiles to declares itself stateless rather
            // than carrying a checkpoint image, which is the property the carve-out rests
            // on. Assert it before the CREATEs, so a future dispatch change that quietly
            // routed the shape to a map-keeping class fails here and not only downstream.
            assertStatelessProjection(FRAME);
            assertStatelessProjection("PARTITION BY sym ORDER BY ts");

            // Named-window spelling, explicit frame.
            execute("CREATE LIVE VIEW lv_named FLUSH EVERY 1s START FROM NOW AS "
                    + "SELECT ts, sym, " + FN + " OVER w AS l FROM base "
                    + "WINDOW w AS (" + FRAME + ")");
            execute("DROP LIVE VIEW lv_named");
            // Named-window spelling, frame left implicit.
            execute("CREATE LIVE VIEW lv_named_default FLUSH EVERY 1s START FROM NOW AS "
                    + "SELECT ts, sym, " + FN + " OVER w AS l FROM base "
                    + "WINDOW w AS (PARTITION BY sym ORDER BY ts)");
            execute("DROP LIVE VIEW lv_named_default");
            // Inline OVER (...) spelling.
            execute("CREATE LIVE VIEW lv_inline FLUSH EVERY 1s START FROM NOW AS "
                    + "SELECT ts, sym, " + FN + " OVER (PARTITION BY sym ORDER BY ts) AS l FROM base");
            execute("DROP LIVE VIEW lv_inline");
            // Two stateless calls over one definition clear it together.
            execute("CREATE LIVE VIEW lv_two_calls FLUSH EVERY 1s START FROM NOW AS "
                    + "SELECT ts, sym, " + FN + " OVER w AS l, last_value(y) OVER w AS l2 FROM base "
                    + "WINDOW w AS (" + FRAME + ")");
            execute("DROP LIVE VIEW lv_two_calls");

            // An accumulator over the very same window keeps a map per partition and
            // keeps the reject: the carve-out is the call's, not the window's.
            assertBareUnboundedRejected(
                    "SELECT ts, sym, sum(x) OVER w AS s FROM base WINDOW w AS (" + FRAME + ")"
            );
            // One non-stateless call is enough to refuse the window for every call over it.
            assertBareUnboundedRejected(
                    "SELECT ts, sym, " + FN + " OVER w AS l, sum(x) OVER w AS s FROM base "
                            + "WINDOW w AS (" + FRAME + ")"
            );
            // IGNORE NULLS keeps the last non-null across rows, so it reads the frame
            // after all and is bounded by its unbounded start.
            assertBareUnboundedRejected(
                    "SELECT ts, sym, " + FN + " IGNORE NULLS OVER w AS l FROM base WINDOW w AS (" + FRAME + ")"
            );
            // EXCLUDE CURRENT ROW rewrites the frame end below the current row, which is a
            // trailing RANGE frame with a ring per partition rather than no state at all.
            assertBareUnboundedRejected(
                    "SELECT ts, sym, " + FN + " OVER w AS l FROM base "
                            + "WINDOW w AS (" + FRAME + " EXCLUDE CURRENT ROW)"
            );
            // Without an ORDER BY a default RANGE frame makes every row a peer of every
            // other, which compiles to the whole-partition last_value - a map after all.
            // Only reachable inline: a named live-view WINDOW must ORDER BY the
            // designated timestamp, and that reject fires first.
            assertBareUnboundedRejected(
                    "SELECT ts, sym, " + FN + " OVER (PARTITION BY sym) AS l FROM base"
            );
            // A definition no call references is refused as before. Vacuously every one
            // of its zero calls is stateless; that is not the shape the carve-out proves.
            assertBareUnboundedRejected(
                    "SELECT ts, sym FROM base WINDOW w AS (" + FRAME + ")"
            );
            // The other functions keep the reject: first_value reads the frame's oldest
            // row, so a row inserted below a partition's earliest one moves every output
            // above it.
            assertBareUnboundedRejected(
                    "SELECT ts, sym, first_value(x) OVER w AS f FROM base WINDOW w AS (" + FRAME + ")"
            );
            assertBareUnboundedRejected(
                    "SELECT ts, sym, row_number() OVER w AS rn FROM base WINDOW w AS (PARTITION BY sym ORDER BY ts)"
            );
            // A call nested in an arithmetic tree carries its OVER clause on the function
            // node, which the rule reaches through its own walk rather than through the
            // SELECT column.
            assertBareUnboundedRejected(
                    "SELECT ts, sym, sum(x) OVER (PARTITION BY sym ORDER BY ts) + 1 AS s FROM base"
            );
            // The stateless call in that position clears this rule and then meets the
            // separate live-view limit on nested window calls: the arithmetic compiles to
            // a virtual column over the window factory, which the factory-shape gate does
            // not recognize as a window query at all.
            assertLiveViewRejected(
                    "SELECT ts, sym, " + FN + " OVER (" + FRAME + ") + 1 AS l FROM base",
                    "live view select must contain at least one window function"
            );
        });
    }

    @Test
    public void testKeyedStatelessRangeLastValueSurvivesRestartAndRepair() throws Exception {
        assertMemoryLeak(() -> {
            createBaseAndViews();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int commit = 1; commit <= COMMITS; commit++) {
                    commitDense(job, commit);
                }
                driveRefreshToQuiescence(job);
                assertViewsMatchRecompute();
            }

            // A restart rebuilds each view's runtime state from the checkpoint timeline;
            // a following fault-free refresh matching a fresh recompute proves the
            // stateless root round-tripped rather than silently forcing a full rebuild.
            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int commit = COMMITS + 1; commit <= COMMITS + 3; commit++) {
                    commitDense(job, commit);
                }
                driveRefreshToQuiescence(job);
                assertViewsMatchRecompute();

                // An out-of-order row deep in history at a fresh sub-second timestamp (no
                // designated-timestamp tie with an existing row) forces the repair path.
                // A stateless call's forward influence is one tick, so the repair rewrites
                // the corrected row's own output and nothing above it.
                commitOutOfOrderHalfSecond(job, 37, 4242);
                driveRefreshToQuiescence(job);
                assertViewsMatchRecompute();

                // A second correction, older still, proves the first repair left the
                // timeline addressable below it.
                commitOutOfOrderHalfSecond(job, 11, 909);
                driveRefreshToQuiescence(job);
                assertViewsMatchRecompute();
            }
        });
    }

    @Test
    public void testStatelessCallSharesAViewWithAnAnchoredWindow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // The mix the carve-out newly admits: an anchored window, whose per-partition
            // state the ANCHOR runtime resets at every segment boundary, beside an
            // unanchored keyed window carrying only the stateless call. Both frames run
            // UNBOUNDED PRECEDING ... CURRENT ROW, so the stateless call joins the
            // anchorable set and takes resetPartition() calls at the anchored window's
            // boundaries - which it must ignore, keeping no state to reset.
            execute("CREATE LIVE VIEW lv_mixed FLUSH EVERY 100ms START FROM NOW AS "
                    + "SELECT ts, sym, sum(x) OVER wa AS s, " + FN + " OVER wb AS v FROM base "
                    + "WINDOW wa AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1m', ts)), "
                    + "wb AS (" + FRAME + ")");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // ROWS_PER_COMMIT * COMMITS seconds of data crosses the one-minute anchor
                // several times per key, so the resets actually fire.
                for (int commit = 1; commit <= COMMITS; commit++) {
                    commitDense(job, commit);
                }
                driveRefreshToQuiescence(job);
                // The stateless column projects its own argument, reset or not...
                TestUtils.assertSqlCursors(
                        engine,
                        sqlExecutionContext,
                        "(SELECT ts, sym, x AS v FROM base) ORDER BY 2, 1",
                        "(SELECT ts, sym, v FROM lv_mixed) ORDER BY 2, 1",
                        LOG,
                        true
                );
                // ...and the anchored accumulator beside it still restarts per segment,
                // which a recompute keyed on the same floor reproduces.
                TestUtils.assertSqlCursors(
                        engine,
                        sqlExecutionContext,
                        "(SELECT ts, sym, sum(x) OVER (PARTITION BY sym, seg ORDER BY ts) AS s "
                                + "FROM (SELECT ts, sym, x, timestamp_floor('1m', ts) AS seg FROM base)) ORDER BY 2, 1",
                        "(SELECT ts, sym, s FROM lv_mixed) ORDER BY 2, 1",
                        LOG,
                        true
                );
                assertNoRefreshFaults("lv_mixed");
            }
        });
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

    private void assertBareUnboundedRejected(String selectSql) {
        assertLiveViewRejected(
                selectSql,
                "live view unbounded window must have an ANCHOR clause; bare unbounded windows are not supported"
        );
    }

    private void assertLiveViewRejected(String selectSql, String expectedMessage) {
        try {
            execute("CREATE LIVE VIEW lv_rejected FLUSH EVERY 1s START FROM NOW AS " + selectSql);
            execute("DROP LIVE VIEW lv_rejected");
            Assert.fail("expected reject for: " + selectSql);
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), expectedMessage);
        }
    }

    // Asserts the sole window function of a live-view SELECT over the given window
    // declares no checkpoint state at all, the disposition the carve-out rests on.
    private void assertStatelessProjection(String window) throws Exception {
        final String sql = "SELECT ts, sym, " + FN + " OVER (" + window + ") AS l FROM base";
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
            Assert.assertTrue(sql + " must declare itself checkpoint-stateless", function.isCheckpointStateless());
            Assert.assertFalse(sql + " must carry no checkpoint state", function.supportsCheckpointState());
        } finally {
            sqlExecutionContext.setLiveViewCompile(false);
        }
    }

    private void assertViewMatchesRecompute(String viewName) throws Exception {
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(SELECT ts, sym, " + FN + " OVER (" + FRAME + ") AS v FROM base) ORDER BY 2, 1",
                "(SELECT ts, sym, v FROM " + viewName + ") ORDER BY 2, 1",
                LOG,
                true
        );
        assertNoRefreshFaults(viewName);
    }

    private void assertViewsMatchRecompute() throws Exception {
        assertViewMatchesRecompute("lv_named");
        assertViewMatchesRecompute("lv_inline");
    }

    // Commits ROWS_PER_COMMIT ascending rows per key at one-second spacing and gives the
    // refresh job a turn. Every seventh/fifth row's value is NULL, so the projected value
    // round-trips a NULL through the refresh and the recompute alike.
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
        // The two spellings the parser reaches through different paths: a named WINDOW
        // resolved from the reference, and an inline OVER (...) carrying the frame itself.
        execute("CREATE LIVE VIEW lv_named FLUSH EVERY 100ms START FROM NOW AS "
                + "SELECT ts, sym, " + FN + " OVER w AS v FROM base WINDOW w AS (" + FRAME + ")");
        execute("CREATE LIVE VIEW lv_inline FLUSH EVERY 100ms START FROM NOW AS "
                + "SELECT ts, sym, " + FN + " OVER (PARTITION BY sym ORDER BY ts) AS v FROM base");
    }

    private void restartCycle() {
        engine.getLiveViewRegistry().clear();
        engine.buildViewGraphs();
    }
}
