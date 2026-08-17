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
            execute("INSERT INTO base (ts, sym, px) VALUES ('2026-08-07T12:00:00.500000Z','A',30.0)");
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
                execute("INSERT INTO base (ts, sym, px) VALUES ('2026-08-07T12:00:04.000000Z','A',16.0)");
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
            execute("INSERT INTO base (ts, sym, px) VALUES ('2026-08-07T12:00:02.500000Z','A',19.0)");
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

            execute("INSERT INTO base (ts, sym, px) VALUES ('2026-08-07T12:00:04.000000Z','A',16.0)");
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

    @Test
    public void testArrayValuedProjection() throws Exception {
        // A projection may build a composite out of a base column and a window value. The
        // tier stores ARRAY, so this stays lead-eligible rather than falling back to
        // disk-only, and the copier has to carry a var-size column out of the projection.
        assertMemoryLeak(() -> {
            createBase();
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " +
                    "SELECT ts, sym, ARRAY[px, avg(px) OVER (" + FRAME + ")] AS pair FROM base");
            insertFourRows();
            refresh();

            assertQuery("SELECT * FROM lv")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tsym\tpair
                            2026-08-07T12:00:00.000000Z\tA\t[10.0,10.0]
                            2026-08-07T12:00:01.000000Z\tA\t[11.0,10.5]
                            2026-08-07T12:00:02.000000Z\tB\t[20.0,20.0]
                            2026-08-07T12:00:03.000000Z\tA\t[13.0,11.333333333333334]
                            """);
        });
    }

    @Test
    public void testDroppingAProjectedBaseColumnInvalidatesTheView() throws Exception {
        // The base columns a view depends on come from the base scan, which sits below
        // both projections - so a column the SELECT only ever touches through an
        // expression still has to register as a dependency. Reading the dependency set off
        // the view's own output instead would miss `px` here: no output column is named
        // after it.
        assertMemoryLeak(() -> {
            createBase();
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " +
                    "SELECT ts, sym, px - avg(px) OVER (" + FRAME + ") AS dev FROM base");
            insertFourRows();
            refresh();

            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            Assert.assertFalse("the view must start valid", instance.isInvalid());

            // Negative control first: a column no part of the view reads must leave it
            // alone. Without this the assertion below holds even if every DROP invalidated
            // every view, which would prove nothing about the dependency set.
            execute("ALTER TABLE base DROP COLUMN qty");
            drainWalQueue();
            Assert.assertFalse(
                    "dropping an unreferenced base column must leave the view valid",
                    instance.isInvalid()
            );

            execute("ALTER TABLE base DROP COLUMN px");
            drainWalQueue();
            Assert.assertTrue(
                    "dropping a base column the projection reads must invalidate the view",
                    instance.isInvalid()
            );
        });
    }

    @Test
    public void testFilterMatchingNoRowsLeavesTheViewEmpty() throws Exception {
        // The filter sits below both projections, so a filter that admits nothing must
        // leave the projections with no row to evaluate rather than producing a row of
        // nulls. An empty view is also the shape a reader sees before any data arrives.
        assertMemoryLeak(() -> {
            createBase();
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " +
                    "SELECT ts, sym AS s, px - avg(px) OVER (" + FRAME + ") AS dev " +
                    "FROM base WHERE px > 1000");
            insertFourRows();
            refresh();

            assertQuery("SELECT * FROM lv")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\ts\tdev
                            """);
        });
    }

    @Test
    public void testIncrementalCommitsAgreeWithARecompute() throws Exception {
        // The refresh rebuilds the whole chain per commit rather than once, so a
        // projection re-bound wrongly on a later commit diverges here and nowhere in a
        // single-commit test. Each commit also back-dates below the previous commit's last
        // row, so the run alternates forward drain and out-of-order replay and the oracle
        // is checked against both.
        assertMemoryLeak(() -> {
            createBase();
            final String viewSql = "SELECT ts, sym AS s, px * 2 AS p2, px - avg(px) OVER ("
                    + FRAME + ") AS dev FROM base";
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " + viewSql);
            refresh();

            for (int i = 0; i < 5; i++) {
                execute("INSERT INTO base (ts, sym, px) VALUES " +
                        "('2026-08-07T12:00:1" + i + ".000000Z','A'," + (10 + i) + ".0)," +
                        "('2026-08-07T12:00:2" + i + ".000000Z','B'," + (20 + i) + ".0)," +
                        "('2026-08-07T12:00:3" + i + ".000000Z',NULL," + (30 + i) + ".0)");
                refresh();
                assertMatchesRecompute(viewSql);
            }
        });
    }

    @Test
    public void testNullsPropagateThroughTheProjection() throws Exception {
        // Two distinct nulls meet in one row. `lag` has no previous row at a partition's
        // first, so the window itself yields null and the arithmetic around it must stay
        // null rather than reading a zeroed slot; `coalesce` then has to see that same null
        // to substitute for it. The NULL symbol is a third partition, not a missing one.
        assertMemoryLeak(() -> {
            createBase();
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " +
                    "SELECT ts, sym, " +
                    "px - lag(px) OVER (" + FRAME + ") AS delta, " +
                    "coalesce(lag(px) OVER (" + FRAME + "), -1.0) AS prev " +
                    "FROM base");
            insertRowsIncludingANullSymbol();
            refresh();

            assertQuery("SELECT * FROM lv")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tsym\tdelta\tprev
                            2026-08-07T12:00:00.000000Z\tA\tnull\t-1.0
                            2026-08-07T12:00:01.000000Z\tA\t1.0\t10.0
                            2026-08-07T12:00:02.000000Z\tB\tnull\t-1.0
                            2026-08-07T12:00:03.000000Z\t\tnull\t-1.0
                            2026-08-07T12:00:04.000000Z\tA\t2.0\t11.0
                            """);
        });
    }

    @Test
    public void testProjectedColumnTypes() throws Exception {
        // The projection decides the view's column types, and the table is created from
        // them - so each of these lands a differently-typed column in the LV table and
        // exercises a different arm of the generated copier. A BOOLEAN and an INT are
        // narrower than the DOUBLE the window produced; the VARCHAR is var-size; the
        // constant is folded and depends on no row at all.
        assertMemoryLeak(() -> {
            createBase();
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " +
                    "SELECT ts, sym, " +
                    "avg(px) OVER (" + FRAME + ") > 11.0 AS above, " +
                    "CASE WHEN avg(px) OVER (" + FRAME + ") > 11.0 THEN 'hi' ELSE 'lo' END AS band, " +
                    "(avg(px) OVER (" + FRAME + "))::int AS rounded, " +
                    "(avg(px) OVER (" + FRAME + "))::varchar AS text, " +
                    "1 AS one " +
                    "FROM base");
            insertFourRows();
            refresh();

            assertQuery("SELECT * FROM lv")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tsym\tabove\tband\trounded\ttext\tone
                            2026-08-07T12:00:00.000000Z\tA\tfalse\tlo\t10\t10.0\t1
                            2026-08-07T12:00:01.000000Z\tA\tfalse\tlo\t10\t10.5\t1
                            2026-08-07T12:00:02.000000Z\tB\ttrue\thi\t20\t20.0\t1
                            2026-08-07T12:00:03.000000Z\tA\ttrue\thi\t11\t11.333333333333334\t1
                            """);
        });
    }

    @Test
    public void testProjectionDroppingEveryBaseColumnButTheTimestamp() throws Exception {
        // The narrowest shape that still refreshes: the timestamp the view is ordered by,
        // and one computed column. Nothing the window read survives into the output, so
        // the view's schema and the window factory's have no column in common beyond ts.
        assertMemoryLeak(() -> {
            createBase();
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " +
                    "SELECT ts, px - avg(px) OVER (" + FRAME + ") AS dev FROM base");
            insertFourRows();
            refresh();

            assertQuery("SELECT * FROM lv")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tdev
                            2026-08-07T12:00:00.000000Z\t0.0
                            2026-08-07T12:00:01.000000Z\t0.5
                            2026-08-07T12:00:02.000000Z\t0.0
                            2026-08-07T12:00:03.000000Z\t1.666666666666666
                            """);
        });
    }

    @Test
    public void testStartFromNowSkipsHistoryThroughTheProjection() throws Exception {
        // START FROM NOW drops every base row below the CREATE moment before the window
        // sees it, so the projection must never evaluate over them either - and the
        // averages the surviving rows carry must start from the survivors, not continue a
        // history the view excluded.
        assertMemoryLeak(() -> {
            createBase();
            insertFourRows();
            drainWalQueue();
            // CREATE after the existing rows, and above their timestamps.
            setCurrentMicros(ts("2026-08-07T12:00:10.000000Z"));
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " +
                    "SELECT ts, sym, px - avg(px) OVER (" + FRAME + ") AS dev FROM base");
            refresh();

            execute("INSERT INTO base (ts, sym, px) VALUES " +
                    "('2026-08-07T12:00:20.000000Z','A',100.0)," +
                    "('2026-08-07T12:00:21.000000Z','A',102.0)");
            refresh();

            assertQuery("SELECT * FROM lv")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tsym\tdev
                            2026-08-07T12:00:20.000000Z\tA\t0.0
                            2026-08-07T12:00:21.000000Z\tA\t1.0
                            """);
        });
    }

    @Test
    public void testWindowPartitionedByADerivedColumn() throws Exception {
        // The PARTITION BY key is a column the input projection computes, so the key the
        // window groups by does not exist in the base table at all. Placing the projection
        // below the window is what makes that resolvable; placing the anchor dispatch above
        // it is what makes the key readable when the window asks for it.
        assertMemoryLeak(() -> {
            createBase();
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " +
                    "SELECT ts, sym, px * 2 AS p2, " +
                    "avg(px) OVER (PARTITION BY p2 ORDER BY ts ROWS 10 PRECEDING) AS ma FROM base");
            insertRowsIncludingANullSymbol();
            refresh();

            // Every p2 here is distinct, so each row averages only itself. That is what
            // makes the assertion discriminating: partitioning by sym instead would group
            // the three A rows and give 10.0 / 10.5 / 11.33 rather than each row's own px.
            assertQuery("SELECT * FROM lv")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tsym\tp2\tma
                            2026-08-07T12:00:00.000000Z\tA\t20.0\t10.0
                            2026-08-07T12:00:01.000000Z\tA\t22.0\t11.0
                            2026-08-07T12:00:02.000000Z\tB\t40.0\t20.0
                            2026-08-07T12:00:03.000000Z\t\t10.0\t5.0
                            2026-08-07T12:00:04.000000Z\tA\t26.0\t13.0
                            """);
        });
    }

    private void assertViewCompiles(String selectSql) throws Exception {
        execute("CREATE LIVE VIEW lv_probe FLUSH EVERY 1s START FROM NOW AS " + selectSql);
        execute("DROP LIVE VIEW lv_probe");
        drainWalQueue();
    }

    private void createBase() throws Exception {
        // qty is projected by nothing below; it is the negative control for the
        // dependency tests, which need a base column the views provably do not read.
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, px DOUBLE, qty LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
    }

    private void insertRowsIncludingANullSymbol() throws Exception {
        // The NULL symbol is its own partition, not a row the window skips.
        execute("""
                INSERT INTO base (ts, sym, px) VALUES
                  ('2026-08-07T12:00:00.000000Z','A',10.0),
                  ('2026-08-07T12:00:01.000000Z','A',11.0),
                  ('2026-08-07T12:00:02.000000Z','B',20.0),
                  ('2026-08-07T12:00:03.000000Z',NULL,5.0),
                  ('2026-08-07T12:00:04.000000Z','A',13.0)""");
    }

    private void insertFourRows() throws Exception {
        execute("""
                INSERT INTO base (ts, sym, px) VALUES
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
