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
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkSPI;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewWindow;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.engine.functions.columns.LongColumn;
import io.questdb.griffin.engine.functions.window.BasePartitionedBivariateWindowFunction;
import io.questdb.griffin.engine.functions.window.BasePartitionedWindowFunction;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.engine.window.WindowRecordCursorFactory;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.MemoryTrackerWorkload;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
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
    // Reads the single partition key off a sweep stub's own partitionByRecord, which is a
    // VirtualRecord over one LongColumn and so carries the key at column 0.
    private static final RecordSink PARTITION_BY_SINK = new RecordSink() {
        @Override
        public void copy(Record r, RecordSinkSPI w) {
            w.putLong(r.getLong(0));
        }

        @Override
        public void setFunctions(ObjList<Function> keyFunctions) {
        }
    };
    // Reads the same key off the survivor MAP's record, where it sits behind the value
    // columns: a map record lays values out first and keys after them, so one BYTE value
    // puts the key at column 1. This is the shape LiveViewWindow.compact passes as
    // activeKeySink.
    private static final RecordSink SURVIVOR_KEY_SINK = new RecordSink() {
        @Override
        public void copy(Record r, RecordSinkSPI w) {
            w.putLong(r.getLong(1));
        }

        @Override
        public void setFunctions(ObjList<Function> keyFunctions) {
        }
    };
    private static final ArrayColumnTypes SWEEP_KEY_TYPES = new ArrayColumnTypes().add(ColumnType.LONG);
    // Roomy enough that seeding and the retried sweep never come near it, so the only
    // breach a case sees is the one it asks for.
    private static final long SWEEP_ROOMY_LIMIT_BYTES = 64 * 1024 * 1024L;
    private static final ArrayColumnTypes SWEEP_VALUE_TYPES = new ArrayColumnTypes().add(ColumnType.BYTE);

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
     * {@code max} and {@code min} are the one aggregate family a live view cannot fold into a
     * ring, and this is what holds them there.
     * <p>
     * Their RANGE implementation - one resizable slab per partition in a {@code MemoryARW}
     * arena - carries a live-view value layout only for a BOUNDED frame start
     * ({@code MaxMinWindowFunctionFactoryHelper.MaxMinOverPartitionRangeFrameBase}); the
     * unbounded-lo branch keeps the plain layout and so reports no checkpoint support. The
     * anchorable subset needs the opposite: a frame reading as UNBOUNDED PRECEDING ... CURRENT
     * ROW over an anchored window. The two do not meet, and both halves are pinned here.
     * <p>
     * The spelling that folds every other family into an anchored ring is EXCLUDE CURRENT ROW,
     * which keeps its ANCHOR because {@code WindowExpression.isNonDefaultFrame()} never reads
     * the exclusion mode. Here it lands on the unbounded-lo branch instead, and
     * {@code CairoEngine.validateLiveViewWindowFunction} refuses the view at CREATE. All nine
     * ring classes are covered - DOUBLE, LONG, the abstract base the DATE and TIMESTAMP leaves
     * share, and the six decimal widths - each read as {@code max} and as {@code min}, which
     * reuse the same nine classes with an inverted comparator.
     * <p>
     * The spelling that IS accepted folds to {@code MaxMinOverUnboundedPartitionRowsFrameBase},
     * which keeps one scalar per partition and no arena, so the subset the frontier sweep walks
     * holds nothing ring-shaped to reclaim. That is why these nine classes carry none of the
     * sweep's hooks while {@code avg}, {@code sum}, {@code count}, {@code first_value},
     * {@code last_value} and {@code nth_value} all do.
     * <p>
     * A live view can still carry a ring-shaped {@code max}, through a bounded RANGE frame on
     * an unanchored window. The sweep does not reach that one either, for the reason it
     * reaches no unanchored window: it is driven by an anchor's monotone bucket advance, and a
     * window with no anchor has no frontier to sweep by. Giving the unbounded-lo branch a
     * live-view layout would instead move max/min into the anchored subset ring-shaped, and
     * the same change would then have to enrol it in the sweep; the first assertion below is
     * what fails on the day that happens.
     */
    @Test
    public void testAnchoredMaxAndMinNeverReachTheRingShape() throws Exception {
        assertMemoryLeak(() -> {
            // One column per ring class: DOUBLE and LONG carry their own, DATE routes through
            // the shared base the TIMESTAMP leaves also use, and the six decimal widths pick
            // six more (precision 2 -> DECIMAL8, 4 -> DECIMAL16, 9 -> DECIMAL32,
            // 18 -> DECIMAL64, 38 -> DECIMAL128, 75 -> DECIMAL256).
            execute("""
                    CREATE TABLE base (
                        ts TIMESTAMP, sym SYMBOL, y DOUBLE, n LONG, dt DATE,
                        d8 DECIMAL(2, 1), d16 DECIMAL(4, 1), d32 DECIMAL(9, 1),
                        d64 DECIMAL(18, 1), d128 DECIMAL(38, 1), d256 DECIMAL(75, 1)
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL""");
            execute("INSERT INTO base (ts, sym, y, n, dt, d8, d16, d32, d64, d128, d256) VALUES "
                    + positionalValueRow("2026-01-01T11:00:00.000000Z", "a", "1") + ", "
                    + positionalValueRow("2026-01-01T12:00:00.000000Z", "a", "3") + ", "
                    + positionalValueRow("2026-01-02T11:00:00.000000Z", "a", "2"));
            drainWalQueue();

            final String[] columns = {"y", "n", "dt", "d8", "d16", "d32", "d64", "d128", "d256"};
            for (String name : new String[]{"max", "min"}) {
                for (String column : columns) {
                    try {
                        execute("CREATE LIVE VIEW lv_ring FLUSH EVERY 100ms START FROM BEGINNING AS "
                                + "SELECT ts, sym, " + name + "(" + column + ") OVER w AS v FROM base "
                                + "WINDOW w AS (PARTITION BY sym ORDER BY ts "
                                + "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW "
                                + "ANCHOR DAILY '00:00')");
                        Assert.fail("an anchored ring-shaped " + name + "(" + column + ") must be refused at CREATE");
                    } catch (SqlException e) {
                        TestUtils.assertContains(
                                e.getFlyweightMessage(),
                                "live view select cannot use window function " + name
                                        + "(); incremental snapshot is not supported for this function yet"
                        );
                    }
                }
            }

            execute("""
                    CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS
                    SELECT ts, sym,
                           max(y) OVER w AS my, max(n) OVER w AS mn, max(dt) OVER w AS mt,
                           max(d8) OVER w AS m8, max(d16) OVER w AS m16, max(d32) OVER w AS m32,
                           max(d64) OVER w AS m64, max(d128) OVER w AS m128, max(d256) OVER w AS m256
                    FROM base
                    WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00')""");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                driveRefreshToQuiescence(job);

                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull("live view 'lv' must be registered", instance);
                final ObjList<WindowFunction> anchorable = anchorableFunctions(instance);
                Assert.assertEquals("the anchored window carries all 9 max calls", 9, anchorable.size());
                for (int i = 0, n = anchorable.size(); i < n; i++) {
                    final WindowFunction f = anchorable.getQuick(i);
                    Assert.assertFalse(
                            "the accepted anchored max/min shape must not fold to a ring",
                            f.supportsCheckpointRingState()
                    );
                    Assert.assertNull(
                            "a sweep of the anchored subset must find no max/min arena to reclaim",
                            f.getRingArena()
                    );
                }

                assertNoRefreshFaults("lv");
                // The anchor resets each partition at every bucket crossing, so the row in the
                // second bucket reports its own value rather than the larger one before it.
                assertQuery("""
                        SELECT ts, sym, my, mn, mt, m8, m16, m32, m64, m128, m256
                        FROM lv ORDER BY ts""")
                        .noLeakCheck()
                        .expectSize()
                        .timestamp("ts")
                        .returns("""
                                ts\tsym\tmy\tmn\tmt\tm8\tm16\tm32\tm64\tm128\tm256
                                2026-01-01T11:00:00.000000Z\ta\t1.0\t1\t2026-01-01T11:00:00.000Z\t1.0\t1.0\t1.0\t1.0\t1.0\t1.0
                                2026-01-01T12:00:00.000000Z\ta\t3.0\t3\t2026-01-01T12:00:00.000Z\t3.0\t3.0\t3.0\t3.0\t3.0\t3.0
                                2026-01-02T11:00:00.000000Z\ta\t2.0\t2\t2026-01-02T11:00:00.000Z\t2.0\t2.0\t2.0\t2.0\t2.0\t2.0
                                """);
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
     * What this pins is that the sweep reclaims that function's partition state, arena and all.
     * {@code LiveViewWindow.compact()} calls {@code retainPartitions} on it; the rebuild drops
     * the evicted keys from the map, and because a map value names its ring slab by a
     * {@code (startOffset, capacity)} pair rather than a reference, the arena is compacted in
     * the same pass - surviving slabs are re-homed into a scratch arena, their offsets rewritten,
     * and the block copied back over the truncated original. Both halves have to land together:
     * dropping the entries alone would orphan every evicted slab, and re-homing alone would
     * leave the map growing with the view's lifetime partition cardinality.
     * <p>
     * The arena assertion is the half that matters most, and it is the one a map-size check
     * cannot stand in for: {@code MemoryARW} only ever appends, so without the truncate-and-copy
     * the arena holds its high-water mark however small the map gets.
     * <p>
     * Two sweeps, not one. The scratch arena's INSTANCE survives a sweep but its pages do not,
     * so a second sweep takes a different path through {@code retainPartitions} than the first:
     * it finds a scratch that is already there and already empty, and re-allocates its backing
     * on the first slab it appends. A scratch left resident between sweeps instead would charge
     * the view one {@code cairo.sql.window.store.page.size} page per ring-shaped call for the
     * whole of its life, which is what the footprint assertion below holds it to.
     */
    @Test
    public void testFrontierSweepReclaimsARingShapedAnchoredFunctionsStateAndArena() throws Exception {
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
                    SELECT ts, sym, y, avg(y) OVER w AS a, count(y) OVER w AS c
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
                Assert.assertEquals("the anchored window carries two calls", 2, anchorable.size());
                // Both calls fold to a ring under EXCLUDE CURRENT ROW, and they sit on different
                // value layouts - avg puts its (startOffset, capacity) pair at slots 2 and 4,
                // count at 1 and 3 - so asserting over both is what catches an enrolment that
                // reads the right pair for one shape and the wrong one for another.
                final long[] fourPartitionArenaBytes = captureRingArenas(anchorable, 4);
                // Every slab of every seeded partition is in the arenas by now, so this is the
                // view's ring footprint at its pre-sweep peak. Read per memory tag rather than
                // off the per-view tracker, which also carries the maps and the in-memory tier -
                // their churn across a refresh would swamp a page.
                final long peakRingBytes = circularBufferBytes();

                final LiveViewWindow window = instance.getAnchorWindow();
                Assert.assertNotNull("the view must carry an anchored window", window);
                Assert.assertEquals(4, window.getAnchorMapSize());

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
                assertRingArenasReclaimed(anchorable, fourPartitionArenaBytes);
                assertRingScratchNotResident(peakRingBytes);
                final long[] afterFirstSweepArenaBytes = ringArenaExtents(anchorable);

                // The survivor's ring has to still be readable AT ITS NEW HOME. This row lands in
                // the same bucket as the two before it, so the frame walks the re-homed slab to
                // decide what is still in range; a slab that was never copied, or an offset left
                // naming the arena as it was before the truncate, gives a wrong average here.
                commit("('2026-01-03T03:00:00.000000Z', 'a', 10.0)", job);

                // A SECOND sweep, which is the only way to reach the scratch-reuse arm: the
                // first one left compactionRingScratch allocated, so this one has to empty it up
                // front and append the survivors from logical 0 again. Appending on top of the
                // first sweep's tail instead would copy that dead prefix back over the primary
                // arena and give the reclamation away. Reviving b, c and d in a's current bucket
                // puts the map back to four; two more advances then leave them behind again.
                commit("('2026-01-03T03:00:01.000000Z', 'b', 20.0), "
                        + "('2026-01-03T03:00:02.000000Z', 'c', 30.0), "
                        + "('2026-01-03T03:00:03.000000Z', 'd', 40.0)", job);
                final long[] revivedArenaBytes = captureRingArenas(anchorable, 4);
                commit("('2026-01-04T01:00:00.000000Z', 'a', 12.0)", job);
                commit("('2026-01-05T01:00:00.000000Z', 'a', 14.0)", job);

                Assert.assertEquals(2, window.getCompactionCount());
                Assert.assertEquals(1, window.getAnchorMapSize());
                assertRingArenasReclaimed(anchorable, revivedArenaBytes);
                // And so does the second sweep, which is a different path: the first ran against
                // a scratch that had never held anything, this one against one that has been
                // closed once and re-allocated since. A release that only works the first time
                // would leave the view a page per call heavier from here on.
                assertRingScratchNotResident(peakRingBytes);
                // Both sweeps leave the same one partition with the same slab, so the arena has
                // to land on the same extent twice. A strict shrink alone would not say this: a
                // second sweep that appended onto the first one's leftover scratch still shrinks
                // four slabs to two, and only carrying the dead prefix forward sweep after sweep
                // would eventually show up as growth.
                for (int i = 0, n = anchorable.size(); i < n; i++) {
                    Assert.assertEquals(
                            "a second sweep must not carry the first one's re-homed bytes forward",
                            afterFirstSweepArenaBytes[i],
                            anchorable.getQuick(i).getRingArena().getAppendOffset()
                    );
                }

                // Same reasoning as the row after the first sweep: read the re-homed ring at the
                // home the SECOND compaction gave it.
                commit("('2026-01-05T02:00:00.000000Z', 'a', 16.0)", job);

                assertNoRefreshFaults("lv");
                // The anchor still resets the ring at every bucket crossing, so the first row of
                // each of a's five buckets sees an empty frame; only a row that has a predecessor
                // within its own bucket averages anything. The two that do - 03T02:00 and
                // 05T02:00 - are the ones reading a slab the sweep re-homed.
                assertQuery("SELECT ts, sym, y, a, c FROM lv ORDER BY sym, ts")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                ts\tsym\ty\ta\tc
                                2026-01-01T11:00:00.000000Z\ta\t1.0\tnull\t0
                                2026-01-02T01:00:00.000000Z\ta\t5.0\tnull\t0
                                2026-01-03T01:00:00.000000Z\ta\t6.0\tnull\t0
                                2026-01-03T02:00:00.000000Z\ta\t8.0\t6.0\t1
                                2026-01-03T03:00:00.000000Z\ta\t10.0\t7.0\t2
                                2026-01-04T01:00:00.000000Z\ta\t12.0\tnull\t0
                                2026-01-05T01:00:00.000000Z\ta\t14.0\tnull\t0
                                2026-01-05T02:00:00.000000Z\ta\t16.0\t14.0\t1
                                2026-01-01T11:00:01.000000Z\tb\t2.0\tnull\t0
                                2026-01-03T03:00:01.000000Z\tb\t20.0\tnull\t0
                                2026-01-01T11:00:02.000000Z\tc\t3.0\tnull\t0
                                2026-01-03T03:00:02.000000Z\tc\t30.0\tnull\t0
                                2026-01-01T11:00:03.000000Z\td\t4.0\tnull\t0
                                2026-01-03T03:00:03.000000Z\td\t40.0\tnull\t0
                                """);
            }
        });
    }

    /**
     * The same sweep over the decimal {@code avg}, {@code sum} and {@code avg(x, scale)}
     * families: 18 ring-shaped classes, one per (aggregate, decimal width) pair, each
     * declaring its own {@code (startOffset, capacity)} value indices.
     * <p>
     * All 18 happen to carry the ring geometry at slots 2 and 4 today, and this is what holds
     * them there. Naming the wrong pair no longer corrupts silently - it trips the range check
     * in {@code AbstractWindowFunctionFactory.copyRingSlab} - but that check fires at refresh
     * time, so a sweep has to reach every one of the 18 for it to say anything.
     */
    @Test
    public void testFrontierSweepReclaimsEveryDecimalRingArena() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_PARTITION_COMPACT_THRESHOLD, 2);
        assertMemoryLeak(() -> {
            // One column per decimal storage width, since the width picks the implementation
            // class: precision 2 -> DECIMAL8, 4 -> DECIMAL16, 9 -> DECIMAL32, 18 -> DECIMAL64,
            // 38 -> DECIMAL128, 75 -> DECIMAL256.
            execute("""
                    CREATE TABLE base (
                        ts TIMESTAMP, sym SYMBOL,
                        d8 DECIMAL(2, 1), d16 DECIMAL(4, 1), d32 DECIMAL(9, 1),
                        d64 DECIMAL(18, 1), d128 DECIMAL(38, 1), d256 DECIMAL(75, 1)
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL""");
            execute("INSERT INTO base (ts, sym, d8, d16, d32, d64, d128, d256) VALUES "
                    + decimalRow("2026-01-01T11:00:00.000000Z", "a", "1.0") + ", "
                    + decimalRow("2026-01-01T11:00:01.000000Z", "b", "2.0") + ", "
                    + decimalRow("2026-01-01T11:00:02.000000Z", "c", "3.0") + ", "
                    + decimalRow("2026-01-01T11:00:03.000000Z", "d", "4.0"));
            drainWalQueue();
            // avg(x) and sum(x) dispatch on the argument's width; avg(x, scale) is a separate
            // rescaling family that dispatches on it too, which is the third set of six.
            execute("""
                    CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS
                    SELECT ts, sym,
                           avg(d8) OVER w AS a8, sum(d8) OVER w AS s8, avg(d8, 2) OVER w AS r8,
                           avg(d16) OVER w AS a16, sum(d16) OVER w AS s16, avg(d16, 2) OVER w AS r16,
                           avg(d32) OVER w AS a32, sum(d32) OVER w AS s32, avg(d32, 2) OVER w AS r32,
                           avg(d64) OVER w AS a64, sum(d64) OVER w AS s64, avg(d64, 2) OVER w AS r64,
                           avg(d128) OVER w AS a128, sum(d128) OVER w AS s128, avg(d128, 2) OVER w AS r128,
                           avg(d256) OVER w AS a256, sum(d256) OVER w AS s256, avg(d256, 2) OVER w AS r256
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
                Assert.assertEquals("the anchored window carries all 18 decimal calls", 18, anchorable.size());
                final long[] fourPartitionArenaBytes = captureRingArenas(anchorable, 4);

                final LiveViewWindow window = instance.getAnchorWindow();
                Assert.assertNotNull("the view must carry an anchored window", window);
                Assert.assertEquals(4, window.getAnchorMapSize());

                commitDecimals(decimalRow("2026-01-02T01:00:00.000000Z", "a", "5.0"), job);
                commitDecimals(decimalRow("2026-01-03T01:00:00.000000Z", "a", "6.0") + ", "
                        + decimalRow("2026-01-03T02:00:00.000000Z", "a", "8.0"), job);

                Assert.assertEquals(1, window.getCompactionCount());
                Assert.assertEquals(
                        "only the account that followed the frontier survives in the anchor map",
                        1,
                        window.getAnchorMapSize()
                );
                assertRingArenasReclaimed(anchorable, fourPartitionArenaBytes);

                commitDecimals(decimalRow("2026-01-03T03:00:00.000000Z", "a", "9.0"), job);

                assertNoRefreshFaults("lv");
                // The anchor resets every ring at each bucket crossing, so the first row of each
                // of a's three buckets sees an empty frame. All six widths must agree row for
                // row: a re-homing that read the wrong slot pair for one width would answer only
                // that width wrongly.
                assertQuery("""
                        SELECT ts, sym, a8, s8, r8, a16, s16, r16, a32, s32, r32,
                               a64, s64, r64, a128, s128, r128, a256, s256, r256
                        FROM lv ORDER BY sym, ts""")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                ts\tsym\ta8\ts8\tr8\ta16\ts16\tr16\ta32\ts32\tr32\ta64\ts64\tr64\ta128\ts128\tr128\ta256\ts256\tr256
                                2026-01-01T11:00:00.000000Z\ta\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t
                                2026-01-02T01:00:00.000000Z\ta\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t
                                2026-01-03T01:00:00.000000Z\ta\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t
                                2026-01-03T02:00:00.000000Z\ta\t6.0\t6.0\t6.00\t6.0\t6.0\t6.00\t6.0\t6.0\t6.00\t6.0\t6.0\t6.00\t6.0\t6.0\t6.00\t6.0\t6.0\t6.00
                                2026-01-03T03:00:00.000000Z\ta\t7.0\t14.0\t7.00\t7.0\t14.0\t7.00\t7.0\t14.0\t7.00\t7.0\t14.0\t7.00\t7.0\t14.0\t7.00\t7.0\t14.0\t7.00
                                2026-01-01T11:00:01.000000Z\tb\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t
                                2026-01-01T11:00:02.000000Z\tc\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t
                                2026-01-01T11:00:03.000000Z\td\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t
                                """);
            }
        });
    }

    /**
     * The same sweep over the {@code first_value} family: 9 classes declaring the hooks plus
     * 9 IGNORE NULLS subclasses that inherit everything except the value layout.
     * <p>
     * The parent/child split is what this case exists for. IGNORE NULLS drops the frame-size
     * slot, so the child's geometry sits one slot lower than the parent's - 0 and 2 against 1
     * and 3 - and a child that inherited the parent's pair would read {@code size} as a start
     * offset and {@code firstIdx} as a capacity. Every column therefore appears twice, once
     * each way, and the DATE pair reaches the two abstract bases the DATE and TIMESTAMP leaves
     * share.
     */
    @Test
    public void testFrontierSweepReclaimsEveryFirstValueRingArena() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_PARTITION_COMPACT_THRESHOLD, 2);
        assertMemoryLeak(() -> {
            // One column per implementation family: DOUBLE and LONG carry their own classes,
            // DATE routes through the shared helper bases, and the six decimal widths pick six
            // more pairs (precision 2 -> DECIMAL8, 4 -> DECIMAL16, 9 -> DECIMAL32,
            // 18 -> DECIMAL64, 38 -> DECIMAL128, 75 -> DECIMAL256).
            execute("""
                    CREATE TABLE base (
                        ts TIMESTAMP, sym SYMBOL, y DOUBLE, n LONG, dt DATE,
                        d8 DECIMAL(2, 1), d16 DECIMAL(4, 1), d32 DECIMAL(9, 1),
                        d64 DECIMAL(18, 1), d128 DECIMAL(38, 1), d256 DECIMAL(75, 1)
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL""");
            execute("INSERT INTO base (ts, sym, y, n, dt, d8, d16, d32, d64, d128, d256) VALUES "
                    + positionalValueRow("2026-01-01T11:00:00.000000Z", "a", "1") + ", "
                    + positionalValueRow("2026-01-01T11:00:01.000000Z", "b", "2") + ", "
                    + positionalValueRow("2026-01-01T11:00:02.000000Z", "c", "3") + ", "
                    + positionalValueRow("2026-01-01T11:00:03.000000Z", "d", "4"));
            drainWalQueue();
            execute("""
                    CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS
                    SELECT ts, sym,
                           first_value(y) OVER w AS fy, first_value(y) IGNORE NULLS OVER w AS gy,
                           first_value(n) OVER w AS fn, first_value(n) IGNORE NULLS OVER w AS gn,
                           first_value(dt) OVER w AS ft, first_value(dt) IGNORE NULLS OVER w AS gt,
                           first_value(d8) OVER w AS f8, first_value(d8) IGNORE NULLS OVER w AS g8,
                           first_value(d16) OVER w AS f16, first_value(d16) IGNORE NULLS OVER w AS g16,
                           first_value(d32) OVER w AS f32, first_value(d32) IGNORE NULLS OVER w AS g32,
                           first_value(d64) OVER w AS f64, first_value(d64) IGNORE NULLS OVER w AS g64,
                           first_value(d128) OVER w AS f128, first_value(d128) IGNORE NULLS OVER w AS g128,
                           first_value(d256) OVER w AS f256, first_value(d256) IGNORE NULLS OVER w AS g256
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
                Assert.assertEquals("the anchored window carries all 18 first_value calls", 18, anchorable.size());
                final long[] fourPartitionArenaBytes = captureRingArenas(anchorable, 4);

                final LiveViewWindow window = instance.getAnchorWindow();
                Assert.assertNotNull("the view must carry an anchored window", window);
                Assert.assertEquals(4, window.getAnchorMapSize());

                commitPositionalValues(positionalValueRow("2026-01-02T01:00:00.000000Z", "a", "5"), job);
                commitPositionalValues(positionalValueRow("2026-01-03T01:00:00.000000Z", "a", "6") + ", "
                        + positionalValueRow("2026-01-03T02:00:00.000000Z", "a", "8"), job);

                Assert.assertEquals(1, window.getCompactionCount());
                Assert.assertEquals(
                        "only the account that followed the frontier survives in the anchor map",
                        1,
                        window.getAnchorMapSize()
                );
                assertRingArenasReclaimed(anchorable, fourPartitionArenaBytes);

                commitPositionalValues(positionalValueRow("2026-01-03T03:00:00.000000Z", "a", "9"), job);

                assertNoRefreshFaults("lv");
                // The anchor resets every ring at each bucket crossing, so the first row of each
                // of a's three buckets sees an empty frame and every later row of a bucket
                // reports that bucket's opening row. The two spellings must agree column for
                // column: a child reading its parent's slot pair would answer only its own
                // column wrongly, and only once the sweep has re-homed the slab it reads.
                assertQuery("""
                        SELECT ts, sym, fy, gy, fn, gn, ft, gt, f8, g8, f16, g16,
                               f32, g32, f64, g64, f128, g128, f256, g256
                        FROM lv ORDER BY sym, ts""")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                ts\tsym\tfy\tgy\tfn\tgn\tft\tgt\tf8\tg8\tf16\tg16\tf32\tg32\tf64\tg64\tf128\tg128\tf256\tg256
                                2026-01-01T11:00:00.000000Z\ta\tnull\tnull\tnull\tnull\t\t\t\t\t\t\t\t\t\t\t\t\t\t
                                2026-01-02T01:00:00.000000Z\ta\tnull\tnull\tnull\tnull\t\t\t\t\t\t\t\t\t\t\t\t\t\t
                                2026-01-03T01:00:00.000000Z\ta\tnull\tnull\tnull\tnull\t\t\t\t\t\t\t\t\t\t\t\t\t\t
                                2026-01-03T02:00:00.000000Z\ta\t6.0\t6.0\t6\t6\t2026-01-03T01:00:00.000Z\t2026-01-03T01:00:00.000Z\t6.0\t6.0\t6.0\t6.0\t6.0\t6.0\t6.0\t6.0\t6.0\t6.0\t6.0\t6.0
                                2026-01-03T03:00:00.000000Z\ta\t6.0\t6.0\t6\t6\t2026-01-03T01:00:00.000Z\t2026-01-03T01:00:00.000Z\t6.0\t6.0\t6.0\t6.0\t6.0\t6.0\t6.0\t6.0\t6.0\t6.0\t6.0\t6.0
                                2026-01-01T11:00:01.000000Z\tb\tnull\tnull\tnull\tnull\t\t\t\t\t\t\t\t\t\t\t\t\t\t
                                2026-01-01T11:00:02.000000Z\tc\tnull\tnull\tnull\tnull\t\t\t\t\t\t\t\t\t\t\t\t\t\t
                                2026-01-01T11:00:03.000000Z\td\tnull\tnull\tnull\tnull\t\t\t\t\t\t\t\t\t\t\t\t\t\t
                                """);
            }
        });
    }

    /**
     * The same sweep over the {@code last_value} family: 9 classes declaring the hooks plus 9
     * IGNORE NULLS subclasses that inherit them whole.
     * <p>
     * This family is where the parent/child hazard C2.2 met does NOT bite, and the case exists
     * to hold that reading. {@code last_value} keeps no frame-size slot for IGNORE NULLS to
     * drop, so parent and child both put the ring geometry at slots 0 and 2 and the child
     * inherits its parent's pair rather than declaring one. Reading every column both ways is
     * what makes that inheritance a tested claim rather than an assumption: a child whose
     * layout had in fact shifted would read {@code size} as a start offset, and the range check
     * in {@code AbstractWindowFunctionFactory.copyRingSlab} would fail the refresh.
     */
    @Test
    public void testFrontierSweepReclaimsEveryLastValueRingArena() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_PARTITION_COMPACT_THRESHOLD, 2);
        assertMemoryLeak(() -> {
            // One column per implementation family: DOUBLE and LONG carry their own classes,
            // DATE routes through the shared helper bases, and the six decimal widths pick six
            // more pairs (precision 2 -> DECIMAL8, 4 -> DECIMAL16, 9 -> DECIMAL32,
            // 18 -> DECIMAL64, 38 -> DECIMAL128, 75 -> DECIMAL256).
            execute("""
                    CREATE TABLE base (
                        ts TIMESTAMP, sym SYMBOL, y DOUBLE, n LONG, dt DATE,
                        d8 DECIMAL(2, 1), d16 DECIMAL(4, 1), d32 DECIMAL(9, 1),
                        d64 DECIMAL(18, 1), d128 DECIMAL(38, 1), d256 DECIMAL(75, 1)
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL""");
            execute("INSERT INTO base (ts, sym, y, n, dt, d8, d16, d32, d64, d128, d256) VALUES "
                    + positionalValueRow("2026-01-01T11:00:00.000000Z", "a", "1") + ", "
                    + positionalValueRow("2026-01-01T11:00:01.000000Z", "b", "2") + ", "
                    + positionalValueRow("2026-01-01T11:00:02.000000Z", "c", "3") + ", "
                    + positionalValueRow("2026-01-01T11:00:03.000000Z", "d", "4"));
            drainWalQueue();
            execute("""
                    CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS
                    SELECT ts, sym,
                           last_value(y) OVER w AS ly, last_value(y) IGNORE NULLS OVER w AS my,
                           last_value(n) OVER w AS ln, last_value(n) IGNORE NULLS OVER w AS mn,
                           last_value(dt) OVER w AS lt, last_value(dt) IGNORE NULLS OVER w AS mt,
                           last_value(d8) OVER w AS l8, last_value(d8) IGNORE NULLS OVER w AS m8,
                           last_value(d16) OVER w AS l16, last_value(d16) IGNORE NULLS OVER w AS m16,
                           last_value(d32) OVER w AS l32, last_value(d32) IGNORE NULLS OVER w AS m32,
                           last_value(d64) OVER w AS l64, last_value(d64) IGNORE NULLS OVER w AS m64,
                           last_value(d128) OVER w AS l128, last_value(d128) IGNORE NULLS OVER w AS m128,
                           last_value(d256) OVER w AS l256, last_value(d256) IGNORE NULLS OVER w AS m256
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
                Assert.assertEquals("the anchored window carries all 18 last_value calls", 18, anchorable.size());
                final long[] fourPartitionArenaBytes = captureRingArenas(anchorable, 4);

                final LiveViewWindow window = instance.getAnchorWindow();
                Assert.assertNotNull("the view must carry an anchored window", window);
                Assert.assertEquals(4, window.getAnchorMapSize());

                commitPositionalValues(positionalValueRow("2026-01-02T01:00:00.000000Z", "a", "5"), job);
                commitPositionalValues(positionalValueRow("2026-01-03T01:00:00.000000Z", "a", "6") + ", "
                        + positionalValueRow("2026-01-03T02:00:00.000000Z", "a", "8"), job);

                Assert.assertEquals(1, window.getCompactionCount());
                Assert.assertEquals(
                        "only the account that followed the frontier survives in the anchor map",
                        1,
                        window.getAnchorMapSize()
                );
                assertRingArenasReclaimed(anchorable, fourPartitionArenaBytes);

                commitPositionalValues(positionalValueRow("2026-01-03T03:00:00.000000Z", "a", "9"), job);

                assertNoRefreshFaults("lv");
                // The anchor resets every ring at each bucket crossing, so the first row of each
                // of a's three buckets sees an empty frame and every later row reports its own
                // immediate predecessor - which is what tells this expected table apart from the
                // first_value one over the same fixture, where both later rows report the
                // bucket's opening row instead. The two spellings must agree column for column.
                assertQuery("""
                        SELECT ts, sym, ly, my, ln, mn, lt, mt, l8, m8, l16, m16,
                               l32, m32, l64, m64, l128, m128, l256, m256
                        FROM lv ORDER BY sym, ts""")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                ts\tsym\tly\tmy\tln\tmn\tlt\tmt\tl8\tm8\tl16\tm16\tl32\tm32\tl64\tm64\tl128\tm128\tl256\tm256
                                2026-01-01T11:00:00.000000Z\ta\tnull\tnull\tnull\tnull\t\t\t\t\t\t\t\t\t\t\t\t\t\t
                                2026-01-02T01:00:00.000000Z\ta\tnull\tnull\tnull\tnull\t\t\t\t\t\t\t\t\t\t\t\t\t\t
                                2026-01-03T01:00:00.000000Z\ta\tnull\tnull\tnull\tnull\t\t\t\t\t\t\t\t\t\t\t\t\t\t
                                2026-01-03T02:00:00.000000Z\ta\t6.0\t6.0\t6\t6\t2026-01-03T01:00:00.000Z\t2026-01-03T01:00:00.000Z\t6.0\t6.0\t6.0\t6.0\t6.0\t6.0\t6.0\t6.0\t6.0\t6.0\t6.0\t6.0
                                2026-01-03T03:00:00.000000Z\ta\t8.0\t8.0\t8\t8\t2026-01-03T02:00:00.000Z\t2026-01-03T02:00:00.000Z\t8.0\t8.0\t8.0\t8.0\t8.0\t8.0\t8.0\t8.0\t8.0\t8.0\t8.0\t8.0
                                2026-01-01T11:00:01.000000Z\tb\tnull\tnull\tnull\tnull\t\t\t\t\t\t\t\t\t\t\t\t\t\t
                                2026-01-01T11:00:02.000000Z\tc\tnull\tnull\tnull\tnull\t\t\t\t\t\t\t\t\t\t\t\t\t\t
                                2026-01-01T11:00:03.000000Z\td\tnull\tnull\tnull\tnull\t\t\t\t\t\t\t\t\t\t\t\t\t\t
                                """);
            }
        });
    }

    /**
     * The same sweep over the {@code nth_value} family: 9 classes, and no subclass anywhere -
     * {@code nth_value} refuses IGNORE NULLS at validation, so the parent/child hazard C2.2 met
     * cannot arise here and every one of the 9 carries the hooks itself.
     * <p>
     * What this family adds over the other two is the locked read. Once the frame holds n rows
     * an unbounded-preceding {@code nth_value} freezes its answer: {@code computeNext} returns
     * early, reading the ring's geometry out of the map value and the value out of the arena
     * and writing nothing back. The fixture's last row takes that path over a slab the sweep
     * re-homed, which neither {@code first_value} nor {@code last_value} has an equivalent of.
     */
    @Test
    public void testFrontierSweepReclaimsEveryNthValueRingArena() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_PARTITION_COMPACT_THRESHOLD, 2);
        assertMemoryLeak(() -> {
            // One column per implementation family: DOUBLE and LONG carry their own classes,
            // DATE routes through the shared helper base, and the six decimal widths pick six
            // more pairs (precision 2 -> DECIMAL8, 4 -> DECIMAL16, 9 -> DECIMAL32,
            // 18 -> DECIMAL64, 38 -> DECIMAL128, 75 -> DECIMAL256).
            execute("""
                    CREATE TABLE base (
                        ts TIMESTAMP, sym SYMBOL, y DOUBLE, n LONG, dt DATE,
                        d8 DECIMAL(2, 1), d16 DECIMAL(4, 1), d32 DECIMAL(9, 1),
                        d64 DECIMAL(18, 1), d128 DECIMAL(38, 1), d256 DECIMAL(75, 1)
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL""");
            execute("INSERT INTO base (ts, sym, y, n, dt, d8, d16, d32, d64, d128, d256) VALUES "
                    + positionalValueRow("2026-01-01T11:00:00.000000Z", "a", "1") + ", "
                    + positionalValueRow("2026-01-01T11:00:01.000000Z", "b", "2") + ", "
                    + positionalValueRow("2026-01-01T11:00:02.000000Z", "c", "3") + ", "
                    + positionalValueRow("2026-01-01T11:00:03.000000Z", "d", "4"));
            drainWalQueue();
            execute("""
                    CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS
                    SELECT ts, sym,
                           nth_value(y, 2) OVER w AS vy, nth_value(n, 2) OVER w AS vn,
                           nth_value(dt, 2) OVER w AS vt,
                           nth_value(d8, 2) OVER w AS v8, nth_value(d16, 2) OVER w AS v16,
                           nth_value(d32, 2) OVER w AS v32, nth_value(d64, 2) OVER w AS v64,
                           nth_value(d128, 2) OVER w AS v128, nth_value(d256, 2) OVER w AS v256
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
                Assert.assertEquals("the anchored window carries all 9 nth_value calls", 9, anchorable.size());
                final long[] fourPartitionArenaBytes = captureRingArenas(anchorable, 4);

                final LiveViewWindow window = instance.getAnchorWindow();
                Assert.assertNotNull("the view must carry an anchored window", window);
                Assert.assertEquals(4, window.getAnchorMapSize());

                commitPositionalValues(positionalValueRow("2026-01-02T01:00:00.000000Z", "a", "5"), job);
                commitPositionalValues(positionalValueRow("2026-01-03T01:00:00.000000Z", "a", "6") + ", "
                        + positionalValueRow("2026-01-03T02:00:00.000000Z", "a", "8"), job);

                Assert.assertEquals(1, window.getCompactionCount());
                Assert.assertEquals(
                        "only the account that followed the frontier survives in the anchor map",
                        1,
                        window.getAnchorMapSize()
                );
                assertRingArenasReclaimed(anchorable, fourPartitionArenaBytes);

                // Two rows past the sweep, not one: the first still appends to the re-homed
                // slab and completes the frame, the second finds it already n rows deep and
                // takes the locked early return over the same slab.
                commitPositionalValues(positionalValueRow("2026-01-03T03:00:00.000000Z", "a", "9") + ", "
                        + positionalValueRow("2026-01-03T04:00:00.000000Z", "a", "7"), job);

                assertNoRefreshFaults("lv");
                // The anchor resets every ring at each bucket crossing, and n is 2 over a frame
                // that excludes the current row, so a bucket's first two rows answer NULL and
                // every row after that names the bucket's SECOND row - 8 here, not 6 and not the
                // row's own predecessor, which is what tells this expected table apart from the
                // first_value and last_value ones over the same fixture.
                assertQuery("""
                        SELECT ts, sym, vy, vn, vt, v8, v16, v32, v64, v128, v256
                        FROM lv ORDER BY sym, ts""")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                ts\tsym\tvy\tvn\tvt\tv8\tv16\tv32\tv64\tv128\tv256
                                2026-01-01T11:00:00.000000Z\ta\tnull\tnull\t\t\t\t\t\t\t
                                2026-01-02T01:00:00.000000Z\ta\tnull\tnull\t\t\t\t\t\t\t
                                2026-01-03T01:00:00.000000Z\ta\tnull\tnull\t\t\t\t\t\t\t
                                2026-01-03T02:00:00.000000Z\ta\tnull\tnull\t\t\t\t\t\t\t
                                2026-01-03T03:00:00.000000Z\ta\t8.0\t8\t2026-01-03T02:00:00.000Z\t8.0\t8.0\t8.0\t8.0\t8.0\t8.0
                                2026-01-03T04:00:00.000000Z\ta\t8.0\t8\t2026-01-03T02:00:00.000Z\t8.0\t8.0\t8.0\t8.0\t8.0\t8.0
                                2026-01-01T11:00:01.000000Z\tb\tnull\tnull\t\t\t\t\t\t\t
                                2026-01-01T11:00:02.000000Z\tc\tnull\tnull\t\t\t\t\t\t\t
                                2026-01-01T11:00:03.000000Z\td\tnull\tnull\t\t\t\t\t\t\t
                                """);
            }
        });
    }

    /**
     * The same rebind on {@code BasePartitionedBivariateWindowFunction}, which carries its
     * own copy of the two-step (create, then rebind onto the tracker) and so its own copy
     * of the hazard. {@code covar_samp} and {@code corr} take it: their unbounded-rows
     * shape overrides {@code newCompactionScratch()}, so the sweep does allocate a scratch
     * for them.
     */
    @Test
    public void testFrontierSweepScratchRebindBreachLeavesNoClosedBivariateScratch() throws Exception {
        assertMemoryLeak(() -> assertScratchRebindBreachLeavesNoClosedScratch(new BivariateSweepStub()));
    }

    /**
     * A frontier sweep whose scratch map cannot be charged to the per-view budget.
     * <p>
     * The sweep's scratch is created untracked and open, then re-homed onto the per-view
     * {@code MemoryTracker} by closing it, binding the tracker and reopening - and that
     * reopen is the first allocation the tracker sees, so it is where a view already at
     * {@code cairo.live.view.refresh.memory.limit.bytes} raises. Publishing the field
     * before the reopen leaves it naming a CLOSED map, and the next sweep takes the reuse
     * arm on the strength of the non-null field alone: {@code clear()} and then a rebuild
     * that writes keys and values through a zero heap address. That write is not an
     * assertion and not a {@code CairoException} - {@code OrderedMap} probes
     * {@code offsetsAddr + (index << 3)} and {@code Unordered8Map} probes
     * {@code memStart + entrySize * index}, both from a base of {@code 0}, so it is a raw
     * near-null native access that takes the process down.
     * <p>
     * A second sweep really does follow the first: a mid-drain refresh failure recovers
     * through {@code LiveViewRefreshJob.clearWindowState}, which rewinds each function with
     * {@code toTop()} precisely so the instances stay live, and {@code toTop()} does not
     * touch the scratch. The invariant is asserted before the retry below reaches a write,
     * so a regression fails on the assertion rather than on the fault.
     */
    @Test
    public void testFrontierSweepScratchRebindBreachLeavesNoClosedScratch() throws Exception {
        assertMemoryLeak(() -> assertScratchRebindBreachLeavesNoClosedScratch(new PartitionedSweepStub()));
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

    private static void assertRingArenasReclaimed(ObjList<WindowFunction> anchorable, long[] seededArenaBytes) {
        for (int i = 0, n = anchorable.size(); i < n; i++) {
            final WindowFunction f = anchorable.getQuick(i);
            Assert.assertEquals(
                    "the sweep must drop the evicted partitions from every ring function's map",
                    1,
                    f.getPartitionMap().size()
            );
            // The surviving partition's slab is re-homed and the arena truncated to it, so
            // the evicted partitions' bytes come back. Asserted as a strict shrink against
            // the captured baseline rather than an exact figure, because the survivor has
            // crossed bucket boundaries by now and its own ring may have grown meanwhile.
            Assert.assertTrue(
                    "the sweep must hand back the evicted partitions' arena bytes, but "
                            + f.getName() + "#" + i + " held " + f.getRingArena().getAppendOffset()
                            + " of " + seededArenaBytes[i],
                    f.getRingArena().getAppendOffset() < seededArenaBytes[i]
            );
        }
    }

    /**
     * Asserts the sweep left none of its scratch arena behind.
     * <p>
     * A sweep can only ever shrink a ring arena's logical extent - it just evicted partitions -
     * so the view's native ring footprint must come out of one no higher than it went in. What
     * pushes it above the peak is the scratch: truncated rather than freed, it stays at one
     * {@code cairo.sql.window.store.page.size} page (1 MB by default) per ring-shaped call for
     * the life of the view, charged to {@code cairo.live.view.refresh.memory.limit.bytes} for
     * memory only a sweep ever reads.
     * <p>
     * The bound is {@code <=} rather than {@code <} because a {@code MemoryARW} allocates whole
     * pages: four slabs and one slab both fit in the arena's single page, so reclaiming three
     * partitions moves the append offset - which the sibling assertion covers - and not the
     * footprint. The refresh is quiesced at every call site, so nothing else is moving the tag.
     */
    private static void assertRingScratchNotResident(long peakRingBytes) {
        final long ringBytes = circularBufferBytes();
        Assert.assertTrue(
                "a sweep must hand its scratch arena back, but the view's ring footprint went"
                        + " from " + peakRingBytes + " at its pre-sweep peak to " + ringBytes
                        + " after a sweep that dropped three of its four partitions",
                ringBytes <= peakRingBytes
        );
    }

    /**
     * Drives one sweep whose scratch rebind exhausts the per-view budget, then holds the
     * function to the one thing that keeps the next sweep safe: the scratch field never
     * names a map the rebind closed and could not reopen.
     * <p>
     * Two seeded partitions and one survivor, so the retried sweep has an entry to copy and
     * an entry to drop - a rebuild that never writes would prove nothing about the map it
     * writes into.
     */
    private static void assertScratchRebindBreachLeavesNoClosedScratch(SweepScratchStub stub) {
        final LimitedMemoryTracker tracker = new LimitedMemoryTracker(SWEEP_ROOMY_LIMIT_BYTES);
        try {
            Map survivors = null;
            try {
                stub.bindTracker(tracker);
                stub.openState();
                seedSweepKeys(stub.partitionMap(), 1L, 2L);
                survivors = newSweepMap();
                seedSweepKeys(survivors, 1L);

                // Squeeze the budget to nothing. newCompactionScratch() allocates under no
                // tracker and still succeeds, so the rebind's reopen() is the first
                // allocation the tracker sees - which is exactly where the sweep charges
                // the scratch to cairo.live.view.refresh.memory.limit.bytes.
                tracker.setLimit(1);
                try {
                    stub.retain(survivors, SURVIVOR_KEY_SINK);
                    Assert.fail("the scratch rebind must raise once the refresh memory budget is exhausted");
                } catch (CairoException e) {
                    Assert.assertTrue(
                            "expected an out-of-memory CairoException, got: " + e.getFlyweightMessage(),
                            e.isOutOfMemory()
                    );
                    TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                }

                // Asserted before the retry below reaches a write: the next sweep takes the
                // reuse arm on the strength of a non-null field alone, and a closed map
                // there means clear() and rebuild both address a zero heap.
                final Map scratch = stub.scratchMap();
                Assert.assertTrue(
                        "a rebind that raised must leave the scratch field null rather than naming a closed map",
                        scratch == null || scratch.isOpen()
                );

                // And the retry has to complete on this instance: the mid-drain recovery
                // rewinds each function with toTop() precisely to keep it live.
                tracker.setLimit(SWEEP_ROOMY_LIMIT_BYTES);
                stub.retain(survivors, SURVIVOR_KEY_SINK);
                Assert.assertEquals(
                        "the retried sweep must keep exactly the survivor",
                        1L,
                        stub.partitionMap().size()
                );
            } finally {
                stub.closeStub();
                Misc.free(survivors);
            }
            // The rebind exists to keep the scratch's malloc and its free on the same
            // counter across the ping-pong swap; a freed function must hand it all back.
            Assert.assertEquals("the sweep's maps must come back off the tracker", 0L, tracker.getUsed());
        } finally {
            tracker.close();
        }
    }

    /**
     * Asserts every anchorable call folded to a ring and is holding one slab per seeded
     * partition, and returns each one's arena extent for the post-sweep shrink to be
     * measured against.
     */
    private static long[] captureRingArenas(ObjList<WindowFunction> anchorable, int seededPartitions) {
        final long[] seededArenaBytes = new long[anchorable.size()];
        for (int i = 0, n = anchorable.size(); i < n; i++) {
            final WindowFunction f = anchorable.getQuick(i);
            Assert.assertTrue(
                    "EXCLUDE CURRENT ROW must fold every anchored call here into a ring shape",
                    f.supportsCheckpointRingState()
            );
            Assert.assertNotNull("a ring-shaped function keeps its partitions in a map", f.getPartitionMap());
            Assert.assertNotNull("a ring-shaped function keeps its slabs in an arena", f.getRingArena());
            Assert.assertEquals("one map entry per seeded account", seededPartitions, f.getPartitionMap().size());
            // One slab per partition, so the arena is holding them all. Captured rather than
            // asserted as an absolute: the slab width follows
            // cairo.sql.window.initial.range.buffer.size, and what the sweep has to change is
            // the ratio, not the constant.
            seededArenaBytes[i] = f.getRingArena().getAppendOffset();
            Assert.assertTrue(
                    "the seeded partitions must have put something in the arena",
                    seededArenaBytes[i] > 0
            );
        }
        return seededArenaBytes;
    }

    /**
     * The process's native footprint under the tag every ring arena and every sweep scratch
     * allocates from. A live-view test drives one view at a time and quiesces its refresh
     * before reading this, so the figure moves only when that view's rings do.
     */
    private static long circularBufferBytes() {
        return Unsafe.getMemUsedByTag(MemoryTag.NATIVE_CIRCULAR_BUFFER);
    }

    /**
     * One row carrying the same magnitude in all six decimal widths, so a case can state the
     * expected frame arithmetic once instead of six times.
     */
    private static String decimalRow(String ts, String sym, String value) {
        return "('" + ts + "', '" + sym + "'"
                + (", " + value + "m").repeat(6)
                + ")";
    }

    /**
     * A partition-state map with the sweep stubs' layout: one LONG key, one BYTE value.
     * Open and charged to no tracker, which is what {@code newCompactionScratch()} hands
     * the sweep and what the rebind then re-homes.
     */
    private static Map newSweepMap() {
        return MapFactory.createUnorderedMap(configuration, SWEEP_KEY_TYPES, SWEEP_VALUE_TYPES);
    }

    /**
     * One row carrying the same magnitude in every type {@code first_value},
     * {@code last_value}, {@code nth_value} and {@code max}/{@code min} each have their own
     * RANGE implementation for. The DATE column takes the row's own timestamp at millisecond
     * resolution, so the expected tables read a row of the bucket back rather than an opaque
     * epoch offset.
     */
    private static String positionalValueRow(String ts, String sym, String value) {
        return "('" + ts + "', '" + sym + "', " + value + ".0, " + value
                + ", '" + ts.substring(0, ts.length() - 4) + "Z'::date"
                + (", " + value + ".0m").repeat(6)
                + ")";
    }

    private static long[] ringArenaExtents(ObjList<WindowFunction> anchorable) {
        final long[] extents = new long[anchorable.size()];
        for (int i = 0, n = anchorable.size(); i < n; i++) {
            extents[i] = anchorable.getQuick(i).getRingArena().getAppendOffset();
        }
        return extents;
    }

    private static void seedSweepKeys(Map map, long... keys) {
        for (long k : keys) {
            final MapKey key = map.withKey();
            key.putLong(k);
            key.createValue().putByte(0, (byte) 0);
        }
    }

    private static ObjList<Function> sweepKeyFunctions() {
        final ObjList<Function> functions = new ObjList<>();
        functions.add(LongColumn.newInstance(0));
        return functions;
    }

    private void commit(String values, LiveViewRefreshJob job) throws Exception {
        execute("INSERT INTO base (ts, sym, y) VALUES " + values);
        drainWalQueue();
        driveRefreshToQuiescence(job);
    }

    private void commitDecimals(String row, LiveViewRefreshJob job) throws Exception {
        execute("INSERT INTO base (ts, sym, d8, d16, d32, d64, d128, d256) VALUES " + row);
        drainWalQueue();
        driveRefreshToQuiescence(job);
    }

    private void commitPositionalValues(String row, LiveViewRefreshJob job) throws Exception {
        execute("INSERT INTO base (ts, sym, y, n, dt, d8, d16, d32, d64, d128, d256) VALUES " + row);
        drainWalQueue();
        driveRefreshToQuiescence(job);
    }

    /**
     * What the two scratch-rebind cases drive, so one helper can hold both partitioned base
     * classes to the same invariant. Each stub reads its own {@code compactionScratch},
     * which neither base class exposes.
     */
    private interface SweepScratchStub {

        void bindTracker(MemoryTracker tracker);

        void closeStub();

        void openState();

        Map partitionMap();

        void retain(Map survivingKeys, RecordSink survivingKeySink);

        Map scratchMap();
    }

    /**
     * A bivariate partitioned window function carrying nothing but what the frontier sweep
     * reads: a partition map and a scratch factory. The shape {@code covar_samp} and
     * {@code corr} reach the sweep in - their unbounded-rows implementation is the one
     * bivariate shape that overrides {@code newCompactionScratch()}.
     */
    private static final class BivariateSweepStub extends BasePartitionedBivariateWindowFunction implements SweepScratchStub {

        private BivariateSweepStub() {
            super(newSweepMap(), new VirtualRecord(sweepKeyFunctions()), PARTITION_BY_SINK, null, null);
        }

        @Override
        public void bindTracker(MemoryTracker tracker) {
            setMemoryTracker(tracker);
        }

        @Override
        public void closeStub() {
            close();
        }

        @Override
        public String getName() {
            return "bivariate_sweep_stub";
        }

        @Override
        public int getType() {
            return ColumnType.DOUBLE;
        }

        @Override
        public void openState() {
            reopen();
        }

        @Override
        public Map partitionMap() {
            return map;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void retain(Map survivingKeys, RecordSink survivingKeySink) {
            retainPartitions(survivingKeys, survivingKeySink);
        }

        @Override
        public Map scratchMap() {
            return compactionScratch;
        }

        @Override
        protected Map newCompactionScratch() {
            return newSweepMap();
        }
    }

    /**
     * Minimal {@link MemoryTracker} with a settable limit, backed by its own native
     * {@code {used, limit}} block. The production {@code Unsafe} allocation path reads and
     * updates the block through {@link #nativeAddress()} exactly as it does for the pooled
     * tracker a live view's refresh acquires, so moving the limit mid-test reproduces a
     * view arriving at a sweep with its budget already spent.
     */
    private static final class LimitedMemoryTracker extends MemoryTracker {
        private long nativeAddress;

        private LimitedMemoryTracker(long limitBytes) {
            nativeAddress = Unsafe.malloc(Unsafe.MEMORY_TRACKER_BLOCK_SIZE, MemoryTag.NATIVE_MEMORY_TRACKER);
            Unsafe.putLong(nativeAddress + Unsafe.MEMORY_TRACKER_USED_OFFSET, 0L);
            Unsafe.putLong(nativeAddress + Unsafe.MEMORY_TRACKER_LIMIT_OFFSET, limitBytes);
        }

        @Override
        public void close() {
            if (nativeAddress != 0) {
                freeNativeAllocators();
                nativeAddress = Unsafe.free(nativeAddress, Unsafe.MEMORY_TRACKER_BLOCK_SIZE, MemoryTag.NATIVE_MEMORY_TRACKER);
            }
        }

        @Override
        public long getLimit() {
            return Unsafe.getLongVolatile(nativeAddress + Unsafe.MEMORY_TRACKER_LIMIT_OFFSET);
        }

        @Override
        public long getQueryId() {
            return 1;
        }

        @Override
        public long getUsed() {
            return Unsafe.getLongVolatile(nativeAddress + Unsafe.MEMORY_TRACKER_USED_OFFSET);
        }

        @Override
        public MemoryTrackerWorkload getWorkload() {
            return MemoryTrackerWorkload.LIVE_VIEW_REFRESH;
        }

        @Override
        public long nativeAddress() {
            return nativeAddress;
        }

        private void setLimit(long limitBytes) {
            Unsafe.putLongVolatile(nativeAddress + Unsafe.MEMORY_TRACKER_LIMIT_OFFSET, limitBytes);
        }
    }

    /**
     * The univariate counterpart: a partitioned window function carrying the frontier
     * sweep's contract and nothing else. The shape every anchored accumulator - avg, sum,
     * count, the extremum and positional families - takes through
     * {@code BasePartitionedWindowFunction.retainPartitions}.
     */
    private static final class PartitionedSweepStub extends BasePartitionedWindowFunction implements SweepScratchStub {

        private PartitionedSweepStub() {
            super(newSweepMap(), new VirtualRecord(sweepKeyFunctions()), PARTITION_BY_SINK, null);
        }

        @Override
        public void bindTracker(MemoryTracker tracker) {
            setMemoryTracker(tracker);
        }

        @Override
        public void closeStub() {
            close();
        }

        @Override
        public String getName() {
            return "partitioned_sweep_stub";
        }

        @Override
        public int getType() {
            return ColumnType.DOUBLE;
        }

        @Override
        public void openState() {
            reopen();
        }

        @Override
        public Map partitionMap() {
            return map;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void retain(Map survivingKeys, RecordSink survivingKeySink) {
            retainPartitions(survivingKeys, survivingKeySink);
        }

        @Override
        public Map scratchMap() {
            return compactionScratch;
        }

        @Override
        protected Map newCompactionScratch() {
            return newSweepMap();
        }
    }
}
