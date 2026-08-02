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
import io.questdb.cairo.lv.LiveViewWindow;
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
 * The live-view ANCHOR runtime dispatches {@code resetPartition} to the subset of window
 * functions {@code SqlCodeGenerator} collects as "anchorable". Membership of that subset
 * decides correctness for every window function in the view, not just the anchored one:
 * a function that is reset at an anchor boundary it does not belong to loses the frame
 * state it was still supposed to be sliding over.
 * <p>
 * The subset must therefore hold exactly the functions for which a reset is a no-op or is
 * the intended semantics - the anchored window's own functions, plus the
 * checkpoint-stateless calls that keep no per-partition state at all. A bounded
 * ROWS/RANGE window declared beside the anchored one keeps sliding across bucket
 * crossings and must never be reset; {@code LiveViewRefreshJob}'s own comment above
 * {@code getAnchorableWindowFunctions()} states that requirement.
 * <p>
 * The shape this class pins is the one that reaches the subset without being anchored and
 * without being stateless: an explicit {@code ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT
 * ROW EXCLUDE CURRENT ROW} frame. It is a *non-default* frame, so the bare-unbounded
 * CREATE reject ({@code SqlParser.isBareUnboundedWindow}, which requires
 * {@code !isNonDefaultFrame()}) does not fire; and {@code EXCLUDE CURRENT ROW} folds the
 * frame end to {@code -1} only later, inside {@code WindowContextImpl.getRowsHi()}, so at
 * the point the subset is collected the model still reads as UNBOUNDED PRECEDING ...
 * CURRENT ROW.
 */
public class LiveViewAnchorResetScopeTest extends AbstractLiveViewTest {

    @After
    public void resetClock() {
        setCurrentMicros(-1);
    }

    @Before
    public void setUpClock() {
        // START FROM NOW records the wall clock at CREATE as the view's lower boundary, so
        // the clock has to sit below the rows the tests commit for the view to admit them.
        setCurrentMicros(0);
    }

    /**
     * An anchored WINDOW beside an unanchored, non-stateless, bounded ROWS window.
     * {@code prev} is {@code last_value} over ROWS UNBOUNDED PRECEDING .. 1 PRECEDING,
     * i.e. the partition's previous row - a value that must not restart at the anchor's
     * one-minute bucket boundaries, because that window declares no anchor.
     * <p>
     * Rows sit at 00:00:00, 00:00:30, 00:01:00, 00:01:30 and 00:02:00, so each key crosses
     * the {@code timestamp_floor('1m', ts)} boundary twice. If the anchored reset reaches
     * {@code prev}, its one-slot ring is refilled with NULL at 00:01:00 and 00:02:00 and
     * those two rows per key report NULL instead of the preceding row's value.
     */
    @Test
    public void testAnchorDoesNotResetUnanchoredExcludeCurrentRowWindow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, y DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS
                    SELECT ts, sym, y,
                           sum(y) OVER w AS s,
                           last_value(y) OVER (PARTITION BY sym ORDER BY ts
                                               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                                               EXCLUDE CURRENT ROW) AS prev
                    FROM base
                    WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1m', ts))""");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("""
                        INSERT INTO base (ts, sym, y) VALUES
                        ('2026-01-01T00:00:00.000000Z', 'a', 1.0),
                        ('2026-01-01T00:00:00.000000Z', 'b', 10.0),
                        ('2026-01-01T00:00:30.000000Z', 'a', 2.0),
                        ('2026-01-01T00:00:30.000000Z', 'b', 20.0),
                        ('2026-01-01T00:01:00.000000Z', 'a', 3.0),
                        ('2026-01-01T00:01:00.000000Z', 'b', 30.0),
                        ('2026-01-01T00:01:30.000000Z', 'a', 4.0),
                        ('2026-01-01T00:01:30.000000Z', 'b', 40.0),
                        ('2026-01-01T00:02:00.000000Z', 'a', 5.0),
                        ('2026-01-01T00:02:00.000000Z', 'b', 50.0)""");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
                driveRefreshToQuiescence(job);

                // Ahead of the fluent assertion, which runs the query under its own
                // memory-leak harness and leaves no registered view behind it.
                assertNoRefreshFaults("lv");
                // s restarts per one-minute bucket per key - that window IS anchored.
                // prev never restarts: it is the previous row of the whole partition.
                assertQuery("SELECT ts, sym, y, s, prev FROM lv ORDER BY sym, ts")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                ts\tsym\ty\ts\tprev
                                2026-01-01T00:00:00.000000Z\ta\t1.0\t1.0\tnull
                                2026-01-01T00:00:30.000000Z\ta\t2.0\t3.0\t1.0
                                2026-01-01T00:01:00.000000Z\ta\t3.0\t3.0\t2.0
                                2026-01-01T00:01:30.000000Z\ta\t4.0\t7.0\t3.0
                                2026-01-01T00:02:00.000000Z\ta\t5.0\t5.0\t4.0
                                2026-01-01T00:00:00.000000Z\tb\t10.0\t10.0\tnull
                                2026-01-01T00:00:30.000000Z\tb\t20.0\t30.0\t10.0
                                2026-01-01T00:01:00.000000Z\tb\t30.0\t30.0\t20.0
                                2026-01-01T00:01:30.000000Z\tb\t40.0\t70.0\t30.0
                                2026-01-01T00:02:00.000000Z\tb\t50.0\t50.0\t40.0
                                """);
            }
        });
    }

    /**
     * The control that proves the two spellings currently diverge. {@code ROWS BETWEEN
     * UNBOUNDED PRECEDING AND 2 PRECEDING} is the same kind of unanchored bounded ROWS
     * window, but it carries a frame-end expression, so the collection predicate's
     * {@code getRowsHiExpr() == null} arm already excludes it. It must stay excluded, and
     * its output must be identical in shape to {@code prev}'s: never restarting at a
     * bucket boundary.
     */
    @Test
    public void testAnchorDoesNotResetUnanchoredTrailingRowsWindow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, y DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS
                    SELECT ts, sym, y,
                           sum(y) OVER w AS s,
                           last_value(y) OVER (PARTITION BY sym ORDER BY ts
                                               ROWS BETWEEN UNBOUNDED PRECEDING AND 2 PRECEDING) AS prev2
                    FROM base
                    WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1m', ts))""");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("""
                        INSERT INTO base (ts, sym, y) VALUES
                        ('2026-01-01T00:00:00.000000Z', 'a', 1.0),
                        ('2026-01-01T00:00:30.000000Z', 'a', 2.0),
                        ('2026-01-01T00:01:00.000000Z', 'a', 3.0),
                        ('2026-01-01T00:01:30.000000Z', 'a', 4.0),
                        ('2026-01-01T00:02:00.000000Z', 'a', 5.0)""");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
                driveRefreshToQuiescence(job);

                assertNoRefreshFaults("lv");
                assertQuery("SELECT ts, sym, y, s, prev2 FROM lv ORDER BY sym, ts")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                ts\tsym\ty\ts\tprev2
                                2026-01-01T00:00:00.000000Z\ta\t1.0\t1.0\tnull
                                2026-01-01T00:00:30.000000Z\ta\t2.0\t3.0\tnull
                                2026-01-01T00:01:00.000000Z\ta\t3.0\t3.0\t1.0
                                2026-01-01T00:01:30.000000Z\ta\t4.0\t7.0\t2.0
                                2026-01-01T00:02:00.000000Z\ta\t5.0\t5.0\t3.0
                                """);
            }
        });
    }

    /**
     * Pins the membership rule directly, independent of any particular function's output:
     * the anchorable subset a live-view compile produces must not contain a function whose
     * window is neither anchored nor checkpoint-stateless.
     */
    @Test
    public void testAnchorableSubsetExcludesUnanchoredStatefulWindow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, y DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final String sql = """
                    SELECT ts, sym,
                           sum(y) OVER w AS s,
                           last_value(y) OVER (PARTITION BY sym ORDER BY ts
                                               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                                               EXCLUDE CURRENT ROW) AS prev
                    FROM base
                    WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1m', ts))""";

            sqlExecutionContext.setLiveViewCompile(true);
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    RecordCursorFactory factory = select(compiler, sql, sqlExecutionContext)
            ) {
                RecordCursorFactory root = factory;
                while (root instanceof QueryProgress) {
                    root = root.getBaseFactory();
                }
                Assert.assertTrue(root instanceof WindowRecordCursorFactory);
                final WindowRecordCursorFactory wf = (WindowRecordCursorFactory) root;
                final ObjList<WindowFunction> anchorable = wf.getAnchorableWindowFunctions();
                Assert.assertNotNull("the anchored window must contribute a function", anchorable);
                // sum() over the anchored window is the only member; the EXCLUDE CURRENT
                // ROW call keeps a per-partition ring the anchor must not touch.
                Assert.assertEquals(
                        "only the anchored window's function may take resetPartition",
                        1,
                        anchorable.size()
                );
                Assert.assertEquals("sum", anchorable.getQuick(0).getName());
            } finally {
                sqlExecutionContext.setLiveViewCompile(false);
            }
        });
    }

    /**
     * The frontier sweep and a ring-shaped member of the anchorable subset.
     * <p>
     * A ring-shaped function does reach the subset. {@code SqlParser.validateLiveViewAnchors}
     * refuses an ANCHOR on a non-default frame, but {@code WindowExpression.isNonDefaultFrame()}
     * reads the framing mode and the two bounds only - it never looks at the exclusion mode -
     * so {@code RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW} reads
     * as the default frame there and keeps its ANCHOR. The fold in
     * {@code WindowContextImpl.getRowsHi()} then moves the frame end below the current row,
     * which sends {@code avg} to its RANGE-frame implementation: one resizable ring slab per
     * partition in a {@code MemoryARW} arena, and {@code supportsCheckpointRingState()} true.
     * <p>
     * What this pins is that the sweep leaves that function's partition state alone.
     * {@code LiveViewWindow.compact()} does call {@code retainPartitions} on it, but the
     * rebuild needs a scratch map of the function's own layout and every ring-holding class
     * leaves {@code newCompactionScratch()} at its {@code null} default, so the call returns
     * before touching the map. That is what keeps the arena consistent: the survivor-driven
     * rebuild never visits an evicted entry, so a ring partition dropped from the map would
     * leave its slab allocated with nothing naming it and no way back onto the free list.
     * Enrolling a ring function in the sweep therefore has to hand the slab back in the same
     * change; this test is what fails if the first half lands without the second.
     */
    @Test
    public void testFrontierSweepLeavesARingShapedAnchoredFunctionsStateIntact() throws Exception {
        // Four accounts in the seed bucket, so three have to fall behind the frontier before
        // the trigger's stale-percent arm - half the map at its default - lets a sweep fire.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_PARTITION_COMPACT_THRESHOLD, 2);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, y DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO base (ts, sym, y) VALUES
                    ('2026-01-01T11:00:00.000000Z', 'a', 1.0),
                    ('2026-01-01T11:00:01.000000Z', 'b', 2.0),
                    ('2026-01-01T11:00:02.000000Z', 'c', 3.0),
                    ('2026-01-01T11:00:03.000000Z', 'd', 4.0)""");
            drainWalQueue();
            execute("""
                    CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS
                    SELECT ts, sym, y, avg(y) OVER w AS a
                    FROM base
                    WINDOW w AS (PARTITION BY sym ORDER BY ts
                                 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW
                                 ANCHOR DAILY '00:00')""");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                driveRefreshToQuiescence(job);

                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull("live view 'lv' must be registered", instance);
                final ObjList<WindowFunction> anchorable = anchorableFunctions(instance);
                Assert.assertEquals("the anchored window carries one call", 1, anchorable.size());
                final WindowFunction ring = anchorable.getQuick(0);
                Assert.assertTrue(
                        "EXCLUDE CURRENT ROW must fold the anchored frame into the ring-shaped avg",
                        ring.supportsCheckpointRingState()
                );
                Assert.assertNotNull("a ring-shaped avg keeps its partitions in a map", ring.getPartitionMap());

                final LiveViewWindow window = instance.getAnchorWindow();
                Assert.assertNotNull("the view must carry an anchored window", window);
                Assert.assertEquals(4, window.getAnchorMapSize());
                Assert.assertEquals(4, ring.getPartitionMap().size());

                // Two bucket advances with only 'a' following the frontier. The second one
                // puts three accounts a full bucket behind it, which is what fires the sweep.
                commit("('2026-01-02T01:00:00.000000Z', 'a', 5.0)", job);
                commit("('2026-01-03T01:00:00.000000Z', 'a', 6.0), "
                        + "('2026-01-03T02:00:00.000000Z', 'a', 8.0)", job);

                Assert.assertEquals(1, window.getCompactionCount());
                Assert.assertEquals(
                        "only the account that followed the frontier survives in the anchor map",
                        1,
                        window.getAnchorMapSize()
                );
                Assert.assertEquals(
                        "the sweep must leave a ring-shaped function's partition state alone",
                        4,
                        ring.getPartitionMap().size()
                );

                assertNoRefreshFaults("lv");
                // The anchor still resets the ring at every bucket crossing, so the first row
                // of each of a's three buckets sees an empty frame; only the second row of the
                // last bucket has a predecessor to average.
                assertQuery("SELECT ts, sym, y, a FROM lv ORDER BY sym, ts")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                ts\tsym\ty\ta
                                2026-01-01T11:00:00.000000Z\ta\t1.0\tnull
                                2026-01-02T01:00:00.000000Z\ta\t5.0\tnull
                                2026-01-03T01:00:00.000000Z\ta\t6.0\tnull
                                2026-01-03T02:00:00.000000Z\ta\t8.0\t6.0
                                2026-01-01T11:00:01.000000Z\tb\t2.0\tnull
                                2026-01-01T11:00:02.000000Z\tc\t3.0\tnull
                                2026-01-01T11:00:03.000000Z\td\t4.0\tnull
                                """);
            }
        });
    }

    /**
     * Documents why {@code EXCLUDE CURRENT ROW} is the spelling that reaches the subset
     * unanchored, and pins the neighbouring route closed. An accumulator over a plain
     * {@code ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW} is refused at CREATE by the
     * finite-influence rule, because an unbounded frame start gives a late row no bounded
     * replay. {@code EXCLUDE CURRENT ROW} clears that rule instead of tripping it: the fold
     * in {@code WindowContextImpl.getRowsHi()} moves the frame end below the current row,
     * which is a bounded trailing frame. So the collection predicate cannot be tightened by
     * looking at the exclusion mode - it has to ask whether the window is anchored.
     */
    @Test
    public void testUnanchoredUnboundedAccumulatorStaysRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, y DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("""
                        CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS
                        SELECT ts, sym, y,
                               sum(y) OVER w AS s,
                               sum(y) OVER (PARTITION BY sym ORDER BY ts
                                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running
                        FROM base
                        WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1m', ts))""");
                Assert.fail("an unanchored unbounded-start accumulator must be rejected at CREATE");
            } catch (SqlException e) {
                TestUtils.assertContains(
                        e.getFlyweightMessage(),
                        "live view select cannot use sum() over a frame starting at UNBOUNDED PRECEDING"
                );
            }
        });
    }

    /**
     * An ANCHOR bounds the state of the calls over its window, so an anchored WINDOW no
     * call references anchors nothing. Once the anchorable subset holds only the anchor's
     * own functions, such a definition leaves it empty, and the refresh job's
     * "an anchored window always has at least one function" invariant would fail on every
     * cycle until the flush-retry budget invalidated the view. CREATE refuses it instead.
     */
    @Test
    public void testUnreferencedAnchoredWindowRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, y DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("""
                        CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS
                        SELECT ts, sym, y,
                               last_value(y) OVER (PARTITION BY sym ORDER BY ts
                                                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                                                   EXCLUDE CURRENT ROW) AS prev
                        FROM base
                        WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1m', ts))""");
                Assert.fail("an anchored WINDOW no call references must be rejected at CREATE");
            } catch (SqlException e) {
                TestUtils.assertContains(
                        e.getFlyweightMessage(),
                        "live view anchored WINDOW 'w' is not referenced by any window function"
                );
            }
        });
    }

    /**
     * The live compiled subset the ANCHOR runtime dispatches to, read off the registered
     * view rather than off a standalone compile, so a case can assert what the running
     * refresh worker actually holds.
     */
    private static ObjList<WindowFunction> anchorableFunctions(LiveViewInstance instance) {
        RecordCursorFactory factory = instance.getCompiledFactory();
        while (factory != null) {
            if (factory instanceof WindowRecordCursorFactory windowFactory) {
                final ObjList<WindowFunction> anchorable = windowFactory.getAnchorableWindowFunctions();
                Assert.assertNotNull("the anchored window must contribute a function", anchorable);
                return anchorable;
            }
            if (factory instanceof QueryProgress) {
                factory = factory.getBaseFactory();
                continue;
            }
            break;
        }
        throw new IllegalStateException("compiled factory does not contain a WindowRecordCursorFactory");
    }

    private void commit(String values, LiveViewRefreshJob job) throws Exception {
        execute("INSERT INTO base (ts, sym, y) VALUES " + values);
        drainWalQueue();
        driveRefreshToQuiescence(job);
    }
}
