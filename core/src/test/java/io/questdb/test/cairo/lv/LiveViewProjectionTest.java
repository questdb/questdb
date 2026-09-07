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

    /**
     * Compiles every test in this suite with function memoization on, which is what a server does
     * and what {@code AbstractTest.setUp()} turns off for the corpus at large. The subject matter
     * here IS the projection, so the plan shape production runs is the one worth covering: a
     * memoized output column is what turned a driven-cursor mistake on the O3 replay path into
     * wrong stored values rather than a slower recompute.
     * <p>
     * The clock is pinned below the suite's test data here rather than in a second {@code @Before},
     * because JUnit 4 does not order two {@code @Before} methods declared on one class and
     * {@link AbstractLiveViewTest#setUp()} moves {@code currentMicros} itself - so which of the two
     * ran last decided where the clock ended up.
     */
    @Before
    @Override
    public void setUp() {
        super.setUp();
        setCurrentMicros(0L);
        allowFunctionMemoization();
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
    public void testFoldedWindowColumnReplaysOutOfOrder() throws Exception {
        // testTwoWindowFunctionsFoldedIntoOneColumn pins the folded shape on the drain path
        // only. Slot resolution is fixed at compile time and one ProjectingRecordCursor serves
        // every path, so the drain is an argument that the repair paths agree - but it is an
        // argument, and the two bugs this suite was written for both hid on a repair path the
        // drain covered by that same argument. The three tests below drive the folded column
        // through each of them instead.
        //
        // The back-dated row lands inside partition A, so both window functions rewind: every
        // later A row's average moves, and the row that used to follow 12:00:00 now lags the
        // new one. A replay that reads either operand from the wrong slot swaps them, and
        // `lag - avg` is as valid a DOUBLE as `avg - lag`.
        assertMemoryLeak(() -> {
            createBase();
            final String viewSql = "SELECT ts, sym, avg(px) OVER (" + FRAME + ") - lag(px) OVER ("
                    + FRAME + ") AS c FROM base";
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " + viewSql);
            insertRowsWithExactAverages();
            refresh();

            execute("INSERT INTO base (ts, sym, px) VALUES ('2026-08-07T12:00:00.500000Z','A',6.0)");
            refresh();

            // A refresh fault recomputes the view from the applied base, which would repair a
            // stale replay before the assertions below could read it.
            assertNoRefreshFaults("lv");
            assertQuery("SELECT * FROM lv")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tsym\tc
                            2026-08-07T12:00:00.000000Z\tA\tnull
                            2026-08-07T12:00:00.500000Z\tA\t-2.0
                            2026-08-07T12:00:01.000000Z\tA\t4.0
                            2026-08-07T12:00:02.000000Z\tB\tnull
                            2026-08-07T12:00:03.000000Z\tA\t1.0
                            2026-08-07T12:00:04.000000Z\tB\t2.0
                            """);
            assertMatchesRecompute(viewSql);
        });
    }

    @Test
    public void testFoldedWindowColumnSurvivesCheckpointRestore() throws Exception {
        // The folded column through a checkpoint restore: the view resumes from window state a
        // previous process sealed, keyed by the window factory's own output positions. Two
        // window functions mean two sets of those positions, so a restore that reassembles them
        // in the compiled order rather than the sealed one feeds the projection the wrong pair.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createBase();
            final String viewSql = "SELECT ts, sym, avg(px) OVER (" + FRAME + ") - lag(px) OVER ("
                    + FRAME + ") AS c FROM base";
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " + viewSql);
            insertRowsWithExactAverages();
            refresh();

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            // Back-dated, so the restored view replays from a checkpoint rather than appending.
            execute("INSERT INTO base (ts, sym, px) VALUES ('2026-08-07T12:00:00.500000Z','A',6.0)");
            refresh();

            assertNoRefreshFaults("lv");
            assertQuery("SELECT * FROM lv")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tsym\tc
                            2026-08-07T12:00:00.000000Z\tA\tnull
                            2026-08-07T12:00:00.500000Z\tA\t-2.0
                            2026-08-07T12:00:01.000000Z\tA\t4.0
                            2026-08-07T12:00:02.000000Z\tB\tnull
                            2026-08-07T12:00:03.000000Z\tA\t1.0
                            2026-08-07T12:00:04.000000Z\tB\t2.0
                            """);
            assertMatchesRecompute(viewSql);
        });
    }

    @Test
    public void testFoldedWindowColumnSurvivesRestart() throws Exception {
        // The folded column through a restart: the compiled factory is gone and the plan is
        // rebuilt from the stored SQL, so the projection has to land on the same two window
        // outputs in the same two slots the view's single stored column was built from.
        assertMemoryLeak(() -> {
            createBase();
            final String viewSql = "SELECT ts, sym, avg(px) OVER (" + FRAME + ") - lag(px) OVER ("
                    + FRAME + ") AS c FROM base";
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " + viewSql);
            insertRowsWithExactAverages();
            refresh();

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            execute("INSERT INTO base (ts, sym, px) VALUES ('2026-08-07T12:00:05.000000Z','A',26.0)");
            refresh();

            assertNoRefreshFaults("lv");
            assertQuery("SELECT * FROM lv")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tsym\tc
                            2026-08-07T12:00:00.000000Z\tA\tnull
                            2026-08-07T12:00:01.000000Z\tA\t2.0
                            2026-08-07T12:00:02.000000Z\tB\tnull
                            2026-08-07T12:00:03.000000Z\tA\t4.0
                            2026-08-07T12:00:04.000000Z\tB\t2.0
                            2026-08-07T12:00:05.000000Z\tA\t-10.0
                            """);
            assertMatchesRecompute(viewSql);
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
                // Per commit, because a rebind that goes wrong on commit i and faults from then
                // on converges the view back onto this oracle - see assertNoRefreshFaults. The
                // wrong rebind this test exists to catch is exactly that shape.
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testMemoizedProjectionColumnSurvivesCheckpointRestore() throws Exception {
        // The two memoized replay tests cover the O3 paths; this one covers the restore. What
        // it adds is coverage rather than a proof: the restore recompiles, so its memoizers are
        // new objects and the rebind clear MemoizerFunction.init() performs is not observable
        // from here - MemoizerFunctionTest pins that directly. What is observable is whether a
        // view whose output column compiles to a memoizer survives the restore with the right
        // values, which is the cell of the shape x path matrix this suite was missing.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createBaseWithAJsonColumn();
            final String viewSql = "SELECT ts, sym, json_extract(j, '$.px')::double - avg(px) OVER ("
                    + FRAME + ") AS dev FROM base";
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " + viewSql);
            // Without a memoizer this is a slower green copy of testProjectionSurvivesCheckpointRestore.
            // EXPLAIN renders the wrap as memoize(...); noLeakCheck() keeps the assertion from
            // clearing the engine out from under the live view the rest of the test drives.
            assertQuery(viewSql)
                    .noLeakCheck()
                    .assertsPlanContaining("memoize(");
            insertJsonRowsWithExactAverages();
            refresh();

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            // Back-dated, so the restored view replays from a checkpoint rather than appending.
            execute("INSERT INTO base (ts, sym, px, j) VALUES ('2026-08-07T12:00:00.500000Z','A',6.0,'{\"px\":6.0}')");
            refresh();

            // A refresh fault recomputes the view from the applied base, which would repair a
            // stale projection before the assertion below could read it.
            assertNoRefreshFaults("lv");
            assertQuery("SELECT * FROM lv")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tsym\tdev
                            2026-08-07T12:00:00.000000Z\tA\t0.0
                            2026-08-07T12:00:00.500000Z\tA\t-2.0
                            2026-08-07T12:00:01.000000Z\tA\t4.0
                            2026-08-07T12:00:02.000000Z\tB\t0.0
                            2026-08-07T12:00:03.000000Z\tA\t15.0
                            2026-08-07T12:00:04.000000Z\tB\t2.0
                            """);
            assertMatchesRecompute(viewSql);
        });
    }

    @Test
    public void testMemoizedProjectionColumnSurvivesRestart() throws Exception {
        // The memoized column through a restart. Same standing as the restore case above: the
        // rebuilt plan carries new memoizers, so this covers the shape on the path rather than
        // proving the clear. The rows it appends sit beside rows the previous process stored,
        // and only the values say whether the projection resumed on the right column.
        assertMemoryLeak(() -> {
            createBaseWithAJsonColumn();
            final String viewSql = "SELECT ts, sym, json_extract(j, '$.px')::double - avg(px) OVER ("
                    + FRAME + ") AS dev FROM base";
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " + viewSql);
            assertQuery(viewSql)
                    .noLeakCheck()
                    .assertsPlanContaining("memoize(");
            insertJsonRowsWithExactAverages();
            refresh();

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            execute("INSERT INTO base (ts, sym, px, j) VALUES ('2026-08-07T12:00:05.000000Z','A',26.0,'{\"px\":26.0}')");
            refresh();

            assertNoRefreshFaults("lv");
            assertQuery("SELECT * FROM lv")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tsym\tdev
                            2026-08-07T12:00:00.000000Z\tA\t0.0
                            2026-08-07T12:00:01.000000Z\tA\t2.0
                            2026-08-07T12:00:02.000000Z\tB\t0.0
                            2026-08-07T12:00:03.000000Z\tA\t12.0
                            2026-08-07T12:00:04.000000Z\tB\t2.0
                            2026-08-07T12:00:05.000000Z\tA\t6.0
                            """);
            assertMatchesRecompute(viewSql);
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
    public void testOutOfOrderRowReplaysAMemoizedProjectionColumn() throws Exception {
        // json_extract() reports shouldMemoize(), and that propagates up the expression
        // it sits in, so the output projection compiles `dev` to a DoubleFunctionMemoizer.
        // Within a traversal, only ProjectingRecordCursor.hasNext() invalidates that cache
        // - MemoizerFunction.init()/toTop() cover a rebind and a rewind, neither of which a
        // driven replay performs per row. The replay must therefore drive the projected
        // cursor: driving the raw window cursor skips every invalidation and pins `dev` to
        // whatever the preceding in-order drain left cached, for every row the replay
        // re-emits, with no refresh fault to show for it.
        assertMemoryLeak(() -> {
            createBaseWithAJsonColumn();
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " +
                    "SELECT ts, sym, json_extract(j, '$.px')::double - avg(px) OVER (" + FRAME + ") AS dev FROM base");
            // Nothing below fails loudly when the memoizer is absent - `dev` simply
            // recomputes per read and every assertion still passes - so without this pin the
            // test degrades into a green duplicate of testOutOfOrderRowReplaysThroughTheProjection
            // the moment shouldMemoize() stops propagating from json_extract() through the
            // cast and the subtraction to SqlCodeGenerator's memoizer wrap. EXPLAIN renders
            // the wrap as memoize(...), so assert it on the same SELECT the view compiled.
            // noLeakCheck() keeps the assertion from clearing the engine out from under the
            // live view the surrounding test is still driving.
            assertQuery("SELECT ts, sym, json_extract(j, '$.px')::double - avg(px) OVER (" + FRAME + ") AS dev FROM base")
                    .noLeakCheck()
                    .assertsPlanContaining("memoize(");
            // j.px mirrors px, so `dev` is the same moving deviation
            // testOutOfOrderRowReplaysThroughTheProjection asserts - the memoizer is the only
            // difference between the two views.
            execute("""
                    INSERT INTO base (ts, sym, px, j) VALUES
                      ('2026-08-07T12:00:00.000000Z','A',10.0,'{"px":10.0}'),
                      ('2026-08-07T12:00:01.000000Z','A',11.0,'{"px":11.0}'),
                      ('2026-08-07T12:00:02.000000Z','B',20.0,'{"px":20.0}'),
                      ('2026-08-07T12:00:03.000000Z','A',13.0,'{"px":13.0}')""");
            refresh();

            execute("INSERT INTO base (ts, sym, px, j) VALUES ('2026-08-07T12:00:00.500000Z','A',30.0,'{\"px\":30.0}')");
            refresh();

            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            // Guards the test against silently degrading into the resume-from-anchor
            // disposition after an unrelated planner change: only o3HeadMissReplay bumps
            // this counter.
            Assert.assertTrue(
                    "test must take a disposition that runs o3HeadMissReplay - the boundary rebuild or the unlocalized full rebuild; nothing would exercise the replay otherwise",
                    instance.getO3BoundaryReplayRows() > 0
            );
            // A faulting cycle recomputes the whole view from the applied base, which would
            // repair the stale rows before the assertion below reads them.
            assertNoRefreshFaults("lv");

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
    public void testOutOfOrderRowResumesFromAnAnchorReplayingAMemoizedProjectionColumn() throws Exception {
        // testOutOfOrderRowResumesFromAnAnchorThroughTheProjection pins that replayFromAnchor
        // WRAPS the window cursor in the output projection. This one pins that it also DRIVES
        // the wrapped cursor, which is a separate claim: with no memoizer in the projection,
        // ProjectingRecordCursor.hasNext() is base.hasNext() verbatim, so calling hasNext() on
        // the raw window cursor while reading through the wrapped record yields identical
        // values and no assertion in that test moves.
        //
        // A memoized column is what separates them. json_extract() reports shouldMemoize(),
        // and that propagates up the expression it sits in, so the output projection compiles
        // `dev` to a DoubleFunctionMemoizer, whose cache within a traversal only
        // ProjectingRecordCursor.hasNext() invalidates. Driving the window cursor instead
        // therefore hands every row the resume re-emits whatever `dev` the preceding in-order
        // drain left cached, with no refresh fault to show for it. That same mutation shipped
        // at the sibling o3HeadMissReplay site, which is why replayFromAnchor carries its own
        // memoized case instead of leaning on testOutOfOrderRowReplaysAMemoizedProjectionColumn.
        // One commit per row (checkpoint.rows = 1 seals a root per drain) leaves a timeline
        // of anchors below the back-dated row, which is what lets planO3Repair resume instead
        // of rebuilding the whole affected interval.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createBaseWithAJsonColumn();
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " +
                    "SELECT ts, sym, json_extract(j, '$.px')::double - avg(px) OVER (PARTITION BY sym ORDER BY ts ROWS 1 PRECEDING) AS dev FROM base");
            // The whole test rests on `dev` compiling to a memoizer: without one the replay
            // reads the same values whichever cursor it drives, and this becomes a slower
            // green copy of testOutOfOrderRowResumesFromAnAnchorThroughTheProjection. EXPLAIN
            // renders the wrap as memoize(...), so assert it on the same SELECT the view
            // compiled. noLeakCheck() keeps the assertion from clearing the engine out from
            // under the live view the rest of the test drives.
            assertQuery("SELECT ts, sym, json_extract(j, '$.px')::double - avg(px) OVER (PARTITION BY sym ORDER BY ts ROWS 1 PRECEDING) AS dev FROM base")
                    .noLeakCheck()
                    .assertsPlanContaining("memoize(");
            for (int i = 1; i <= 12; i++) {
                final long seconds = i * 10L;
                // Alternating 10.0 / 14.0 over a two-row frame averages to exactly 12.0, so
                // every expected value below is exact rather than a repeating fraction. j.px
                // mirrors px, so `dev` is the same moving deviation
                // testOutOfOrderRowResumesFromAnAnchorThroughTheProjection asserts - the
                // memoizer is the only difference between the two views.
                final String px = (i & 1) == 1 ? "10.0" : "14.0";
                execute(String.format(
                        "INSERT INTO base (ts, sym, px, j) VALUES ('2026-08-07T12:%02d:%02d.000000Z','A',%s,'{\"px\":%s}')",
                        seconds / 60,
                        seconds % 60,
                        px,
                        px
                ));
                refresh();
            }

            // Back-dated above the roots sealed for the first eleven rows, so an anchor
            // exists below it.
            execute("INSERT INTO base (ts, sym, px, j) VALUES ('2026-08-07T12:01:55.000000Z','A',20.0,'{\"px\":20.0}')");
            refresh();

            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            // Guards the test against silently degrading into the boundary rebuild or the
            // head-miss replay after an unrelated planner change: only replayFromAnchor
            // bumps this counter.
            Assert.assertTrue(
                    "test must take the resume-from-anchor disposition; nothing would exercise replayFromAnchor",
                    instance.getO3ResumeReplayRows() > 0
            );
            // The two dispositions are disjoint per replay, so a non-zero boundary count
            // means a second, unasked-for rebuild ran as well - and a rebuild re-emits the
            // same rows correctly, masking whatever the resume wrote.
            Assert.assertEquals(
                    "no boundary rebuild may run alongside the resume; it would mask what replayFromAnchor wrote",
                    0,
                    instance.getO3BoundaryReplayRows()
            );
            // A faulting cycle recomputes the whole view from the applied base, which would
            // repair the stale rows before the assertion below reads them.
            assertNoRefreshFaults("lv");

            assertQuery("SELECT * FROM lv")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tsym\tdev
                            2026-08-07T12:00:10.000000Z\tA\t0.0
                            2026-08-07T12:00:20.000000Z\tA\t2.0
                            2026-08-07T12:00:30.000000Z\tA\t-2.0
                            2026-08-07T12:00:40.000000Z\tA\t2.0
                            2026-08-07T12:00:50.000000Z\tA\t-2.0
                            2026-08-07T12:01:00.000000Z\tA\t2.0
                            2026-08-07T12:01:10.000000Z\tA\t-2.0
                            2026-08-07T12:01:20.000000Z\tA\t2.0
                            2026-08-07T12:01:30.000000Z\tA\t-2.0
                            2026-08-07T12:01:40.000000Z\tA\t2.0
                            2026-08-07T12:01:50.000000Z\tA\t-2.0
                            2026-08-07T12:01:55.000000Z\tA\t5.0
                            2026-08-07T12:02:00.000000Z\tA\t-3.0
                            """);
        });
    }

    @Test
    public void testOutOfOrderRowResumesFromAnAnchorThroughTheProjection() throws Exception {
        // The other O3 disposition, the one testOutOfOrderRowReplaysThroughTheProjection
        // does not take. A checkpoint root sealed strictly below the back-dated
        // row lets planO3Repair resume from that anchor instead of rebuilding the whole
        // affected interval, and the resume is the only path through replayFromAnchor. Its
        // rows still leave the window factory in the window's shape, so they have to pass
        // through the output projection before the copier writes them: driving the raw
        // window cursor instead stores the window's own column 2 - the bare avg - into the
        // view's `dev`, silently and with no refresh fault.
        //
        // One commit per row (checkpoint.rows = 1 seals a root per drain) leaves a
        // timeline of anchors below the back-dated row, and the ten-row frame prices the
        // rebuild above the resume, which is what selects the disposition.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createBase();
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " +
                    "SELECT ts, sym, px - avg(px) OVER (PARTITION BY sym ORDER BY ts ROWS 1 PRECEDING) AS dev FROM base");
            for (int i = 1; i <= 12; i++) {
                final long seconds = i * 10L;
                // Alternating 10.0 / 14.0 over a two-row frame averages to exactly 12.0, so
                // every expected value below is exact rather than a repeating fraction.
                execute(String.format(
                        "INSERT INTO base (ts, sym, px) VALUES ('2026-08-07T12:%02d:%02d.000000Z','A',%s)",
                        seconds / 60,
                        seconds % 60,
                        (i & 1) == 1 ? "10.0" : "14.0"
                ));
                refresh();
            }

            // Back-dated above the roots sealed for the first eleven rows, so an anchor
            // exists below it.
            execute("INSERT INTO base (ts, sym, px) VALUES ('2026-08-07T12:01:55.000000Z','A',20.0)");
            refresh();

            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            // Guards the test against silently degrading into the boundary rebuild or the
            // head-miss replay after an unrelated planner change: only replayFromAnchor
            // bumps this counter.
            Assert.assertTrue(
                    "test must take the resume-from-anchor disposition; nothing would exercise replayFromAnchor",
                    instance.getO3ResumeReplayRows() > 0
            );
            // The two dispositions are disjoint per replay, so a non-zero boundary count
            // means a second, unasked-for rebuild ran as well - and a rebuild re-emits the
            // same rows correctly, masking whatever the resume wrote.
            Assert.assertEquals(
                    "no boundary rebuild may run alongside the resume; it would mask what replayFromAnchor wrote",
                    0,
                    instance.getO3BoundaryReplayRows()
            );
            // A faulting cycle recomputes the whole view from the applied base, which would
            // repair the mis-written rows before the assertion below reads them.
            assertNoRefreshFaults("lv");

            assertQuery("SELECT * FROM lv")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tsym\tdev
                            2026-08-07T12:00:10.000000Z\tA\t0.0
                            2026-08-07T12:00:20.000000Z\tA\t2.0
                            2026-08-07T12:00:30.000000Z\tA\t-2.0
                            2026-08-07T12:00:40.000000Z\tA\t2.0
                            2026-08-07T12:00:50.000000Z\tA\t-2.0
                            2026-08-07T12:01:00.000000Z\tA\t2.0
                            2026-08-07T12:01:10.000000Z\tA\t-2.0
                            2026-08-07T12:01:20.000000Z\tA\t2.0
                            2026-08-07T12:01:30.000000Z\tA\t-2.0
                            2026-08-07T12:01:40.000000Z\tA\t2.0
                            2026-08-07T12:01:50.000000Z\tA\t-2.0
                            2026-08-07T12:01:55.000000Z\tA\t5.0
                            2026-08-07T12:02:00.000000Z\tA\t-3.0
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
    public void testProjectedColumnWrappingAnotherProjectedExpression() throws Exception {
        // `c2` restates the whole of `c` and doubles it, so the output projection carries
        // four output functions and its priority metadata reserves a slot for each (plus
        // one for an internal timestamp) ahead of the window's own columns. Every base
        // reference inside the arithmetic - `px` here - addresses the window's output
        // across that offset. A slot resolved one place off hands the subtraction the
        // window's average instead of `px`, which still compiles, still drops cleanly and
        // still yields a plausible DOUBLE, so only the values catch it. The row at
        // 12:00:03 is the discriminating one: `px`, the average, `c` and `c2` are four
        // distinct numbers there.
        assertMemoryLeak(() -> {
            createBase();
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " +
                    "SELECT ts, sym, px - avg(px) OVER (" + FRAME + ") AS c, " +
                    "(px - avg(px) OVER (" + FRAME + ")) * 2 AS c2 FROM base");
            insertRowsWithExactAverages();
            refresh();

            assertQuery("SELECT * FROM lv")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tsym\tc\tc2
                            2026-08-07T12:00:00.000000Z\tA\t0.0\t0.0
                            2026-08-07T12:00:01.000000Z\tA\t2.0\t4.0
                            2026-08-07T12:00:02.000000Z\tB\t0.0\t0.0
                            2026-08-07T12:00:03.000000Z\tA\t12.0\t24.0
                            2026-08-07T12:00:04.000000Z\tB\t2.0\t4.0
                            """);
        });
    }

    @Test
    public void testProjectedColumnWrappingAnotherProjectedExpressionReplaysOutOfOrder() throws Exception {
        // The doubled shape on the O3 replay path. The test above pins the slot arithmetic on
        // the drain; a replay re-emits rows through the same compiled projection but from a
        // rewound window, so a projection that reads `px` across the wrong offset diverges here
        // on the rows whose average moved rather than on every row.
        assertMemoryLeak(() -> {
            createBase();
            final String viewSql = "SELECT ts, sym, px - avg(px) OVER (" + FRAME + ") AS c, "
                    + "(px - avg(px) OVER (" + FRAME + ")) * 2 AS c2 FROM base";
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " + viewSql);
            insertRowsWithExactAverages();
            refresh();

            execute("INSERT INTO base (ts, sym, px) VALUES ('2026-08-07T12:00:00.500000Z','A',6.0)");
            refresh();

            assertNoRefreshFaults("lv");
            assertQuery("SELECT * FROM lv")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tsym\tc\tc2
                            2026-08-07T12:00:00.000000Z\tA\t0.0\t0.0
                            2026-08-07T12:00:00.500000Z\tA\t-2.0\t-4.0
                            2026-08-07T12:00:01.000000Z\tA\t4.0\t8.0
                            2026-08-07T12:00:02.000000Z\tB\t0.0\t0.0
                            2026-08-07T12:00:03.000000Z\tA\t15.0\t30.0
                            2026-08-07T12:00:04.000000Z\tB\t2.0\t4.0
                            """);
            assertMatchesRecompute(viewSql);
        });
    }

    @Test
    public void testProjectedColumnWrappingAnotherProjectedExpressionSurvivesCheckpointRestore() throws Exception {
        // The doubled shape through a checkpoint restore. Four output functions plus a reserved
        // internal timestamp slot have to be rebuilt in the same order the sealed window state
        // was indexed by, and a restore that reassembles them one place off still yields four
        // plausible DOUBLEs.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createBase();
            final String viewSql = "SELECT ts, sym, px - avg(px) OVER (" + FRAME + ") AS c, "
                    + "(px - avg(px) OVER (" + FRAME + ")) * 2 AS c2 FROM base";
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " + viewSql);
            insertRowsWithExactAverages();
            refresh();

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            execute("INSERT INTO base (ts, sym, px) VALUES ('2026-08-07T12:00:00.500000Z','A',6.0)");
            refresh();

            assertNoRefreshFaults("lv");
            assertQuery("SELECT * FROM lv")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tsym\tc\tc2
                            2026-08-07T12:00:00.000000Z\tA\t0.0\t0.0
                            2026-08-07T12:00:00.500000Z\tA\t-2.0\t-4.0
                            2026-08-07T12:00:01.000000Z\tA\t4.0\t8.0
                            2026-08-07T12:00:02.000000Z\tB\t0.0\t0.0
                            2026-08-07T12:00:03.000000Z\tA\t15.0\t30.0
                            2026-08-07T12:00:04.000000Z\tB\t2.0\t4.0
                            """);
            assertMatchesRecompute(viewSql);
        });
    }

    @Test
    public void testProjectedColumnWrappingAnotherProjectedExpressionSurvivesRestart() throws Exception {
        // The doubled shape through a restart. The stored table has two data columns built from
        // one window output; the recompiled plan has to produce both again, in that order, or
        // the copier writes `c2` into `c`.
        assertMemoryLeak(() -> {
            createBase();
            final String viewSql = "SELECT ts, sym, px - avg(px) OVER (" + FRAME + ") AS c, "
                    + "(px - avg(px) OVER (" + FRAME + ")) * 2 AS c2 FROM base";
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " + viewSql);
            insertRowsWithExactAverages();
            refresh();

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            execute("INSERT INTO base (ts, sym, px) VALUES ('2026-08-07T12:00:05.000000Z','A',26.0)");
            refresh();

            assertNoRefreshFaults("lv");
            assertQuery("SELECT * FROM lv")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tsym\tc\tc2
                            2026-08-07T12:00:00.000000Z\tA\t0.0\t0.0
                            2026-08-07T12:00:01.000000Z\tA\t2.0\t4.0
                            2026-08-07T12:00:02.000000Z\tB\t0.0\t0.0
                            2026-08-07T12:00:03.000000Z\tA\t12.0\t24.0
                            2026-08-07T12:00:04.000000Z\tB\t2.0\t4.0
                            2026-08-07T12:00:05.000000Z\tA\t6.0\t12.0
                            """);
            assertMatchesRecompute(viewSql);
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
                // A tier shaped off the window factory rather than the view would fault on the
                // publish and recompute its way back onto the oracle above, so the recompute
                // alone does not separate a served tier read from a repaired one.
                assertNoRefreshFaults("lv");
            }
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
    public void testTwoWindowFunctionsFoldedIntoOneColumn() throws Exception {
        // One output column over two different window functions. The window factory emits
        // both, side by side, and the projection subtracts the second from the first - so
        // the column the view stores exists in neither the window's output nor the base.
        // A slot resolved one place off swaps the operands, and `lag - avg` is as valid a
        // DOUBLE as `avg - lag`; CREATE and DROP see no difference between them.
        //
        // `lag` has no predecessor at a partition's first row, so it yields the DOUBLE
        // null rather than a zeroed slot, and the subtraction around it must carry that
        // null out - the whole column is null there, not the bare average. Both partitions
        // start on such a row, so the null is the partition's property and not the view's
        // first row only.
        assertMemoryLeak(() -> {
            createBase();
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " +
                    "SELECT ts, sym, avg(px) OVER (" + FRAME + ") - lag(px) OVER (" + FRAME + ") AS c FROM base");
            insertRowsWithExactAverages();
            refresh();

            assertQuery("SELECT * FROM lv")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tsym\tc
                            2026-08-07T12:00:00.000000Z\tA\tnull
                            2026-08-07T12:00:01.000000Z\tA\t2.0
                            2026-08-07T12:00:02.000000Z\tB\tnull
                            2026-08-07T12:00:03.000000Z\tA\t4.0
                            2026-08-07T12:00:04.000000Z\tB\t2.0
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
        // qty is projected by nothing below; it is the negative control for the
        // dependency tests, which need a base column the views provably do not read.
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, px DOUBLE, qty LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
    }

    private void createBaseWithAJsonColumn() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, px DOUBLE, qty LONG, j VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL");
    }

    private void insertFourRows() throws Exception {
        execute("""
                INSERT INTO base (ts, sym, px) VALUES
                  ('2026-08-07T12:00:00.000000Z','A',10.0),
                  ('2026-08-07T12:00:01.000000Z','A',11.0),
                  ('2026-08-07T12:00:02.000000Z','B',20.0),
                  ('2026-08-07T12:00:03.000000Z','A',13.0)""");
    }

    private void insertJsonRowsWithExactAverages() throws Exception {
        // insertRowsWithExactAverages() with a JSON mirror of px, so a view over
        // json_extract(j, '$.px') carries the same values as one over px - and compiles a
        // memoizer, because json_extract() reports shouldMemoize() and that propagates up the
        // expression it sits in.
        execute("""
                INSERT INTO base (ts, sym, px, j) VALUES
                  ('2026-08-07T12:00:00.000000Z','A',10.0,'{"px":10.0}'),
                  ('2026-08-07T12:00:01.000000Z','A',14.0,'{"px":14.0}'),
                  ('2026-08-07T12:00:02.000000Z','B',20.0,'{"px":20.0}'),
                  ('2026-08-07T12:00:03.000000Z','A',30.0,'{"px":30.0}'),
                  ('2026-08-07T12:00:04.000000Z','B',24.0,'{"px":24.0}')""");
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

    private void insertRowsWithExactAverages() throws Exception {
        // Every frame average these rows produce divides exactly: 10+14 over two rows is
        // 12.0, 10+14+30 over three is 18.0, 20+24 over two is 22.0. That keeps each
        // expected value exact under IEEE-754 instead of a repeating fraction the
        // assertion would have to spell out digit for digit. The two partitions also
        // interleave, so a partition boundary sits between consecutive rows.
        execute("""
                INSERT INTO base (ts, sym, px) VALUES
                  ('2026-08-07T12:00:00.000000Z','A',10.0),
                  ('2026-08-07T12:00:01.000000Z','A',14.0),
                  ('2026-08-07T12:00:02.000000Z','B',20.0),
                  ('2026-08-07T12:00:03.000000Z','A',30.0),
                  ('2026-08-07T12:00:04.000000Z','B',24.0)""");
    }

    private void refresh() {
        drainWalQueue();
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            driveSeedToCompletion(job, "lv");
            driveRefreshToQuiescence(job);
        }
    }
}
