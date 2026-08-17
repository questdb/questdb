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

import io.questdb.PropertyKey;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * A live view whose SELECT wraps a window function in an expression, aliases a base
 * column, or projects a scalar alongside the window - the shapes CREATE used to turn
 * away with "live view select must contain at least one window function".
 * <p>
 * The planner splits such a SELECT into a projection on either side of the window
 * factory, and the refresh path rebuilds both over WAL segment rows:
 * <pre>
 * [projection]   the view's own schema        px - avg(px) OVER (...)
 * window
 *   [projection] the window's input           px * 2 AS p2
 *     [mapping]  alias / reorder / drop       sym AS s
 *       [filter] residual WHERE
 *         base scan
 * </pre>
 * Each is a pure function of the row it is handed, which is why none of it changes what
 * incremental maintenance has to reproduce: replaying the same base rows through the same
 * window state re-derives the same projected values. The tests below hold that claim to
 * the paths where it could go wrong - a restart that recompiles from stored SQL, an
 * out-of-order row that forces a replay, and the in-memory tier that serves rows the WAL
 * has not flushed yet.
 *
 * @see io.questdb.cairo.lv.LiveViewCompiledPlan
 */
public class LiveViewProjectionTest extends AbstractLiveViewTest {

    private static final String FRAME = "PARTITION BY sym ORDER BY ts ROWS 10 PRECEDING";

    @Before
    public void pinClockBelowTestData() {
        setCurrentMicros(0L);
    }

    @Test
    public void testAnchoredWindowPartitionedByAnAliasedColumn() throws Exception {
        // The anchor expression and the window's PARTITION BY keys resolve against the
        // window's input, which an alias reshapes. Resolving them against the base scan
        // instead - as the refresh path did when no projection could sit in between -
        // fails to find `s` at all.
        assertMemoryLeak(() -> {
            createBase();
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " +
                    "SELECT ts, sym AS s, sum(px) OVER w AS running FROM base " +
                    "WINDOW w AS (PARTITION BY s ORDER BY ts ANCHOR DAILY '00:00')");
            insertFourRows();
            refresh();

            assertQuery("SELECT * FROM lv")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\ts\trunning
                            2026-08-07T12:00:00.000000Z\tA\t10.0
                            2026-08-07T12:00:01.000000Z\tA\t21.0
                            2026-08-07T12:00:02.000000Z\tB\t20.0
                            2026-08-07T12:00:03.000000Z\tA\t34.0
                            """);
        });
    }

    @Test
    public void testOutOfOrderRowReplaysThroughTheProjection() throws Exception {
        // An O3 row rewinds the view and recomputes the affected range through the same
        // window state and the same projection. The projection holds nothing across rows,
        // so the replayed values must match a from-scratch computation exactly - including
        // the rows whose `dev` changes because their moving average moved.
        assertMemoryLeak(() -> {
            createBase();
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " +
                    "SELECT ts, sym, px - avg(px) OVER (" + FRAME + ") AS dev FROM base");
            insertFourRows();
            refresh();

            // Back-dated between the first two A rows, so every later A row's average -
            // and therefore its dev - is recomputed.
            execute("INSERT INTO base VALUES ('2026-08-07T12:00:00.500000Z','A',30.0)");
            refresh();

            assertQuery("SELECT * FROM lv")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tsym\tdev
                            2026-08-07T12:00:00.000000Z\tA\t0.0
                            2026-08-07T12:00:00.500000Z\tA\t10.0
                            2026-08-07T12:00:01.000000Z\tA\t-6.0
                            2026-08-07T12:00:02.000000Z\tB\t0.0
                            2026-08-07T12:00:03.000000Z\tA\t-3.0
                            """);
        });
    }

    @Test
    public void testPreWindowAliasAndScalarProjection() throws Exception {
        // Both pre-window nodes at once: the alias is a mapping, the scalar a projection,
        // and the window reads through both.
        assertMemoryLeak(() -> {
            createBase();
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " +
                    "SELECT ts, sym AS s, px * 2 AS p2, avg(px) OVER (" + FRAME + ") AS ma FROM base");
            insertFourRows();
            refresh();

            assertQuery("SELECT * FROM lv")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\ts\tp2\tma
                            2026-08-07T12:00:00.000000Z\tA\t20.0\t10.0
                            2026-08-07T12:00:01.000000Z\tA\t22.0\t10.5
                            2026-08-07T12:00:02.000000Z\tB\t40.0\t20.0
                            2026-08-07T12:00:03.000000Z\tA\t26.0\t11.333333333333334
                            """);
        });
    }

    @Test
    public void testProjectionOnBothSidesOfTheWindowWithAFilter() throws Exception {
        // The deepest tree the planner emits for a live view: an output projection over
        // the window, an input projection and a mapping under it, and a residual filter
        // under those. The filter still resolves against the base scan, which is why it
        // stays below the mapping in the rebuilt chain.
        assertMemoryLeak(() -> {
            createBase();
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " +
                    "SELECT ts, sym AS s, px * 2 AS p2, px - avg(px) OVER (" + FRAME + ") AS dev " +
                    "FROM base WHERE px > 10");
            insertFourRows();
            refresh();

            // The px=10 row is filtered out, so A's average starts at 11.
            assertQuery("SELECT * FROM lv")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\ts\tp2\tdev
                            2026-08-07T12:00:01.000000Z\tA\t22.0\t0.0
                            2026-08-07T12:00:02.000000Z\tB\t40.0\t0.0
                            2026-08-07T12:00:03.000000Z\tA\t26.0\t1.0
                            """);
        });
    }

    @Test
    public void testProjectedRowsServeFromTheInMemoryTierBeforeTheyFlush() throws Exception {
        // Between a drain and a flush the view's newest rows live only in the in-memory
        // tier, and a read is served from there rather than from the LV table. The tier is
        // shaped by the view's own schema, so it stores what the projection produced - not
        // what the window factory emitted. Shaping it off the window factory instead puts
        // four columns' worth of window output into a three-column buffer.
        assertMemoryLeak(() -> {
            createBase();
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1h START FROM BEGINNING AS " +
                    "SELECT ts, sym, px - avg(px) OVER (" + FRAME + ") AS dev FROM base");
            insertFourRows();
            refresh();

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Pin the flush clock to now, then drain a forward batch without advancing
                // past FLUSH EVERY: the rows publish into the tier as the lead and never
                // reach the LV table.
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                instance.setLastFlushTimeUs(currentMicros);
                execute("INSERT INTO base VALUES ('2026-08-07T12:00:04.000000Z','A',16.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                Assert.assertTrue(
                        "test must build an un-flushed lead; nothing would exercise the tier",
                        instance.getLeadRowCount() > 0
                );

                // The last row exists only in the tier, and its dev is what the projection
                // computed on the way in. Compared against the same SELECT evaluated
                // directly over the base - the cursor-level oracle the rest of the suite
                // uses for an un-flushed lead, and the read path that observes the tier.
                assertMatchesRecompute(
                        "SELECT ts, sym, px - avg(px) OVER (" + FRAME + ") AS dev FROM base"
                );
            }
        });
    }

    @Test
    public void testProjectionSurvivesCheckpointRestore() throws Exception {
        // A checkpoint captures window state, keyed by the window factory's own output
        // positions rather than by the view's columns - which is what lets a projection sit
        // above the window without invalidating anything already written. Seal a checkpoint
        // per row, restart onto it, and the recomputed rows must still agree.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createBase();
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " +
                    "SELECT ts, sym, px - avg(px) OVER (" + FRAME + ") AS dev FROM base");
            insertFourRows();
            refresh();

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            // Back-dated, so the restored view has to replay from a checkpoint rather than
            // simply append.
            execute("INSERT INTO base VALUES ('2026-08-07T12:00:02.500000Z','A',19.0)");
            refresh();

            assertQuery("SELECT * FROM lv")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tsym\tdev
                            2026-08-07T12:00:00.000000Z\tA\t0.0
                            2026-08-07T12:00:01.000000Z\tA\t0.5
                            2026-08-07T12:00:02.000000Z\tB\t0.0
                            2026-08-07T12:00:02.500000Z\tA\t5.666666666666666
                            2026-08-07T12:00:03.000000Z\tA\t-0.25
                            """);
        });
    }

    @Test
    public void testProjectionSurvivesRestart() throws Exception {
        // A restart drops the compiled factory and recompiles from the stored SQL, so the
        // plan - both projections included - is rebuilt from scratch and has to land on
        // the same shape the view's table was created with. A mismatch would surface as a
        // copier built for the wrong column count rather than as wrong values.
        assertMemoryLeak(() -> {
            createBase();
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " +
                    "SELECT ts, sym, px - avg(px) OVER (" + FRAME + ") AS dev FROM base");
            insertFourRows();
            refresh();

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            execute("INSERT INTO base VALUES ('2026-08-07T12:00:04.000000Z','A',16.0)");
            refresh();

            assertQuery("SELECT * FROM lv")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tsym\tdev
                            2026-08-07T12:00:00.000000Z\tA\t0.0
                            2026-08-07T12:00:01.000000Z\tA\t0.5
                            2026-08-07T12:00:02.000000Z\tB\t0.0
                            2026-08-07T12:00:03.000000Z\tA\t1.666666666666666
                            2026-08-07T12:00:04.000000Z\tA\t3.5
                            """);
        });
    }

    @Test
    public void testWindowFunctionWrappedInAnExpression() throws Exception {
        // The reported shape: a bare window function beside one wrapped in arithmetic.
        // Both columns come out of the same window state, one straight and one projected.
        assertMemoryLeak(() -> {
            createBase();
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " +
                    "SELECT ts, sym, " +
                    "px - avg(px) OVER (" + FRAME + ") AS dev, " +
                    "avg(px) OVER (" + FRAME + ") AS ma " +
                    "FROM base");
            insertFourRows();
            refresh();

            assertQuery("SELECT * FROM lv")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tsym\tdev\tma
                            2026-08-07T12:00:00.000000Z\tA\t0.0\t10.0
                            2026-08-07T12:00:01.000000Z\tA\t0.5\t10.5
                            2026-08-07T12:00:02.000000Z\tB\t0.0\t20.0
                            2026-08-07T12:00:03.000000Z\tA\t1.666666666666666\t11.333333333333334
                            """);
        });
    }

    @Test
    public void testWrappingFormsAllCompile() throws Exception {
        // The issue's table: every one of these was turned away, and the reject claimed
        // the query contained no window function.
        assertMemoryLeak(() -> {
            createBase();
            assertViewCompiles("SELECT ts, sym, px - avg(px) OVER (" + FRAME + ") AS c FROM base");
            assertViewCompiles("SELECT ts, sym, cast(avg(px) OVER (" + FRAME + ") AS DOUBLE) AS c FROM base");
            assertViewCompiles("SELECT ts, sym, round(avg(px) OVER (" + FRAME + "), 2) AS c FROM base");
            assertViewCompiles("SELECT ts, sym, coalesce(avg(px) OVER (" + FRAME + "), 0.0) AS c FROM base");
            assertViewCompiles("SELECT ts, sym, avg(px) OVER (" + FRAME + ") + 1 AS c FROM base");
            assertViewCompiles("SELECT ts, sym, -avg(px) OVER (" + FRAME + ") AS c FROM base");
            assertViewCompiles("SELECT ts, sym, px - avg(px) OVER (" + FRAME + ") AS c FROM base WHERE px > 1");
            // Two window functions combined into one output column.
            assertViewCompiles("SELECT ts, sym, avg(px) OVER (" + FRAME + ") - lag(px) OVER (" + FRAME + ") AS c FROM base");
            // A projected column that references an earlier projected column.
            assertViewCompiles("SELECT ts, sym, px - avg(px) OVER (" + FRAME + ") AS c, " +
                    "(px - avg(px) OVER (" + FRAME + ")) * 2 AS c2 FROM base");
            // The anchored named-window form from the issue's table.
            assertViewCompiles("SELECT ts, sym, px - lag(px) OVER w AS c FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00')");
        });
    }

    @Test
    public void testRejectsNameWhatStandsInTheWay() throws Exception {
        // A shape the refresh path genuinely cannot rebuild is still turned away - but by
        // a message that names it. LIMIT and ORDER BY are properties of the whole result
        // set rather than of the row in hand, so no per-row rebuild reproduces them.
        assertMemoryLeak(() -> {
            createBase();
            assertQuery("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT ts, sym, px - avg(px) OVER (" + FRAME + ") AS dev FROM base LIMIT 10")
                    .failsWith("LIMIT over a window function is not supported yet");
            assertQuery("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT ts, sym, px - avg(px) OVER (" + FRAME + ") AS dev FROM base ORDER BY sym DESC")
                    .failsWith("ORDER BY over a window function is not supported yet");
            // A projection over a window-free scan is still a view with no window function,
            // and the reject has to say that rather than blame the projection.
            assertQuery("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT ts, sym, px * 2 AS p2 FROM base")
                    .failsWith("live view select must contain at least one window function");
        });
    }

    // Compares the view against its own SELECT evaluated directly over the base. Ordered
    // by the key then the timestamp so the two cursors are comparable row for row.
    private void assertMatchesRecompute(String viewSql) throws Exception {
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + viewSql + ") ORDER BY 2, 1",
                "(lv) ORDER BY 2, 1",
                LOG,
                true
        );
    }

    private void assertViewCompiles(String selectSql) throws Exception {
        execute("CREATE LIVE VIEW lv_probe FLUSH EVERY 1s START FROM NOW AS " + selectSql);
        execute("DROP LIVE VIEW lv_probe");
        drainWalQueue();
    }

    private void createBase() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
    }

    private void insertFourRows() throws Exception {
        execute("""
                INSERT INTO base VALUES
                  ('2026-08-07T12:00:00.000000Z','A',10.0),
                  ('2026-08-07T12:00:01.000000Z','A',11.0),
                  ('2026-08-07T12:00:02.000000Z','B',20.0),
                  ('2026-08-07T12:00:03.000000Z','A',13.0)""");
    }

    private void refresh() {
        drainWalQueue();
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            driveSeedToCompletion(job, "lv");
            driveRefreshToQuiescence(job);
        }
    }
}
