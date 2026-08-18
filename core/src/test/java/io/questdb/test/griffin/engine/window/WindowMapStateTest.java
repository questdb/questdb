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

package io.questdb.test.griffin.engine.window;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.window.WindowAccumulatorDescriptor;
import io.questdb.griffin.engine.window.WindowAccumulatorPlan;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.engine.window.WindowMapState;
import io.questdb.griffin.engine.window.WindowRecordCursorFactory;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * The bound window Map group: one map, one key projection and one lookup per row serving
 * several outputs of one window on the streaming cursor.
 * <p>
 * What this build binds is both halves of the optimization: physical co-location, where every
 * output keeps a state slice of its own behind one shared key, and the merge, where several
 * outputs read one accumulator that one of them maintains. The differential cases below check
 * both against a reference this tree already runs: a window carrying <b>one</b> fusible
 * function forms no group, so the same output asked for on its own is the unfused path.
 * <p>
 * The group's compile-time decisions - which functions join, in what order, sharing what -
 * are {@code WindowAccumulatorPlanTest}'s and are not repeated here.
 */
public class WindowMapStateTest extends AbstractCairoTest {

    private static final int CAPTURE_SHAPE_ROW_COUNT = 9;
    private static final int DECIMAL_KEY_SHAPE_ROW_COUNT = 9;
    /**
     * The same window {@link #WINDOW} names, keyed by an expression over two of the base's
     * columns rather than by one of them. {@code concat} over a SYMBOL pair is the shape worth
     * running: it reads two columns, it answers for a NULL rather than propagating it, and no
     * column of the record carries its value - so the group has to evaluate it to find its key
     * at all.
     */
    private static final String EXPRESSION_WINDOW =
            "window w as (partition by concat(k, k2) order by ts "
                    + "rows between unbounded preceding and current row)";
    private static final int KEY_SHAPE_ROW_COUNT = 9;
    private static final int ORDINARY_ROW_COUNT = 9;
    /**
     * The same three shapes {@link #ROWS_FRAME_GEOMETRIES} names, spelled as a span of time. The
     * rows below are one second apart, so a three-second frame is three preceding rows where a
     * partition is dense and fewer where it is not - which is the difference from the ROWS
     * spelling that matters: how many rows a RANGE frame holds is the timestamps' answer, so the
     * ring it needs grows with the data.
     * <p>
     * Every one of them orders by the designated timestamp in the direction the base is already
     * scanned in. That is not a style choice: a RANGE frame is compiled only where the window's
     * order was dismissed against the base cursor, so a bounded RANGE window is always a
     * natural-order one.
     */
    private static final String[] RANGE_FRAME_GEOMETRIES = {
            "window w as (partition by k order by ts "
                    + "range between 3_000_000 microseconds preceding and current row)",
            "window w as (partition by k order by ts "
                    + "range between 5_000_000 microseconds preceding and 2_000_000 microseconds preceding)",
            "window w as (partition by k order by ts "
                    + "range between unbounded preceding and 2_000_000 microseconds preceding)",
    };
    /**
     * The bounded-RANGE reference window, the geometry a moving aggregate over a time span is
     * written in.
     */
    private static final String RANGE_FRAME_WINDOW = RANGE_FRAME_GEOMETRIES[0];
    /**
     * The three shapes a bounded ROWS frame comes in, which are three different rings: one ending
     * at the current row, one whose high bound lags it, and one with no low bound at all - the
     * last being the only one nothing ever leaves.
     */
    private static final String[] ROWS_FRAME_GEOMETRIES = {
            "window w as (partition by k order by ts rows between 3 preceding and current row)",
            "window w as (partition by k order by ts rows between 5 preceding and 2 preceding)",
            "window w as (partition by k order by ts rows between unbounded preceding and 2 preceding)",
    };
    /**
     * The bounded-ROWS reference window: the geometry an ordinary moving aggregate is written in,
     * and the one whose ring the deferred subtraction is easiest to read against.
     */
    private static final String ROWS_FRAME_WINDOW = ROWS_FRAME_GEOMETRIES[0];
    private static final String SUM_AND_COUNT_PLAN = """
            Window
              functions: [sum(x) over (partition by [k] rows between unbounded preceding and current row),count(y) over (partition by [k] rows between unbounded preceding and current row)]
                PageFrame
                    Row forward scan
                    Frame forward scan on: t
            """;
    private static final String WINDOW =
            "window w as (partition by k order by ts rows between unbounded preceding and current row)";

    @Test
    public void testABoundFunctionsPrivateMapNeverOpens() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            insertOrdinaryRows();
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = select(compiler, sumAndCount(), sqlExecutionContext)) {
                final WindowRecordCursorFactory windowFactory = windowFactory(factory);
                assertBoundGroupCount(windowFactory, 1);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    drain(cursor);
                    final ObjList<WindowFunction> functions = windowFactory.getWindowFunctions();
                    int bound = 0;
                    for (int i = 0, n = functions.size(); i < n; i++) {
                        final WindowFunction function = functions.getQuick(i);
                        if (!function.isWindowStateOwned()) {
                            continue;
                        }
                        bound++;
                        // The private map object survives - live-view validation and the
                        // checkpoint adapters read its nullness and its openness - but its
                        // native backing is never allocated, which is the whole saving.
                        Assert.assertNotNull(function.getPartitionMap());
                        Assert.assertFalse(
                                "bound function reopened its private map",
                                function.getPartitionMap().isOpen()
                        );
                    }
                    Assert.assertEquals(2, bound);
                }
            }
        });
    }

    @Test
    public void testABoundedAndACumulativeFrameAreTwoGroups() throws Exception {
        // Two windows over one key that differ in nothing but the frame. Their sums keep
        // different states - one gives values back and the other never does - so the two must not
        // meet, and what keeps them apart is the frame in the group's spec rather than anything
        // about the families. Two groups and two maps: this is the shape that
        // would silently produce a cumulative total for a bounded output if the spec stopped
        // discriminating.
        assertMemoryLeak(() -> {
            createTable();
            insertKeyShapes();
            final String sql = "select ts, sum(x) over w, avg(x) over w, sum(x) over c, avg(x) over c from t "
                    + "window w as (partition by k order by ts rows between 3 preceding and current row), "
                    + "c as (partition by k order by ts rows between unbounded preceding and current row)";
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = select(compiler, sql, sqlExecutionContext)) {
                final WindowRecordCursorFactory windowFactory = windowFactory(factory);
                assertBoundGroupCount(windowFactory, 2);
                final ObjList<WindowMapState> states = windowFactory.getWindowMapStates();
                int ringBacked = 0;
                for (int i = 0; i < 2; i++) {
                    final WindowAccumulatorPlan plan = states.getQuick(i).getPlan();
                    Assert.assertEquals(1, plan.getComponentCount());
                    Assert.assertEquals(2, plan.getProjectionCount());
                    if (plan.getComponent(0).isRingBacked()) {
                        ringBacked++;
                        Assert.assertEquals(4, plan.getSlotCount());
                    } else {
                        Assert.assertEquals(2, plan.getSlotCount());
                    }
                }
                Assert.assertEquals("exactly one group is the bounded one", 1, ringBacked);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final long rows = drain(cursor);
                    Assert.assertEquals(KEY_SHAPE_ROW_COUNT, rows);
                }
            }
        });
    }

    @Test
    public void testABoundedRangeAndABoundedRowsFrameAreTwoGroups() throws Exception {
        // Two windows over one key that differ in nothing but how the frame is measured. Both are
        // ring-backed and their states are different shapes, and what keeps them apart is the
        // framing mode in the group's spec: this is the pair that would answer a span of time with
        // a count of rows if the spec stopped discriminating.
        assertMemoryLeak(() -> {
            createTable();
            insertKeyShapes();
            final String sql = "select ts, sum(x) over w, avg(x) over w, sum(x) over r, avg(x) over r from t "
                    + "window w as (partition by k order by ts rows between 3 preceding and current row), "
                    + "r as (partition by k order by ts "
                    + "range between 3_000_000 microseconds preceding and current row)";
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = select(compiler, sql, sqlExecutionContext)) {
                final WindowRecordCursorFactory windowFactory = windowFactory(factory);
                assertBoundGroupCount(windowFactory, 2);
                final ObjList<WindowMapState> states = windowFactory.getWindowMapStates();
                int range = 0;
                for (int i = 0; i < 2; i++) {
                    final WindowAccumulatorPlan plan = states.getQuick(i).getPlan();
                    Assert.assertEquals(1, plan.getComponentCount());
                    Assert.assertEquals(2, plan.getProjectionCount());
                    Assert.assertTrue(plan.getComponent(0).isRingBacked());
                    if (plan.getComponent(0).getFamily()
                            == WindowAccumulatorDescriptor.FAMILY_DOUBLE_RANGE_SUM_COUNT) {
                        range++;
                        Assert.assertEquals(6, plan.getSlotCount());
                    } else {
                        Assert.assertEquals(4, plan.getSlotCount());
                    }
                }
                Assert.assertEquals("exactly one group is the RANGE one", 1, range);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final long rows = drain(cursor);
                    Assert.assertEquals(KEY_SHAPE_ROW_COUNT, rows);
                }
            }
        });
    }

    @Test
    public void testABoundedRangeCountKeepsItsOwnRing() throws Exception {
        // The bounded-RANGE half of the decline the ROWS families made: a bounded count(x) emits
        // the very number the bounded sum(x) beside it keeps in its counter and still gets a
        // component of its own, because its state continues outside the slice in a ring of its own
        // shape - timestamps where the host keeps (timestamp, value) pairs. Here a second reason
        // stands behind the first: the guest's five slots are not a run inside the host's six
        // either, since the host keeps a total in front of its counter.
        assertMemoryLeak(() -> {
            createTable();
            insertKeyShapes();
            final String sql = "select ts, sum(x) over w, count(x) over w from t " + RANGE_FRAME_WINDOW;
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = select(compiler, sql, sqlExecutionContext)) {
                final WindowRecordCursorFactory windowFactory = windowFactory(factory);
                assertBoundGroupCount(windowFactory, 1);
                final WindowMapState state = windowFactory.getWindowMapStates().getQuick(0);
                final WindowAccumulatorPlan plan = state.getPlan();
                Assert.assertEquals(2, plan.getComponentCount());
                Assert.assertEquals(2, plan.getProjectionCount());
                // The six-slot pair then the five-slot counter.
                Assert.assertEquals(11, plan.getSlotCount());
                Assert.assertEquals(0, plan.getComponentSlotBase(0));
                Assert.assertEquals(6, plan.getComponentSlotBase(1));
                Assert.assertTrue(plan.getComponent(0).isRingBacked());
                Assert.assertTrue(plan.getComponent(1).isRingBacked());
                Assert.assertFalse(plan.getProjection(0).isDerived());
                Assert.assertFalse(plan.getProjection(1).isDerived());
                // Two counters, and they are two slots.
                Assert.assertEquals(1, plan.getProjection(0).getNonNullCountSlot());
                Assert.assertEquals(6, plan.getProjection(1).getNonNullCountSlot());
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final long rows = drain(cursor);
                    Assert.assertEquals(KEY_SHAPE_ROW_COUNT, rows);
                }
            }
            assertFusedMatchesUnfusedOnWindow("t", RANGE_FRAME_WINDOW, "sum(x) over w", "count(x) over w");
        });
    }

    @Test
    public void testABoundedRangeRingGrowsWithTheData() throws Exception {
        // The one thing a RANGE ring does that a ROWS ring cannot: outgrow itself mid-traversal.
        // A partition denser than the configured initial buffer expands its slab, which moves the
        // ring's address, its read cursor and its capacity - three of the six slots the group's
        // value carries - so a contributor that failed to carry any of them back into the slice
        // would answer the next row out of the wrong slab.
        //
        // The frame has to be both wider than the configured initial buffer of 32 and narrower
        // than the partition, and both halves are load-bearing. The first is what makes the ring
        // grow; the second is what makes the corruption visible, because a frame that never drops
        // a row never reads a cell back - the accumulator is incremental, so a stale slab is only
        // an answer once a value has to leave it. Fifty-one rows of a two-hundred-row partition
        // is both.
        //
        // Driven rather than asserted: what a resize has to produce is the rows the unfused path
        // produces, and the group's own map is not where the resize happens.
        assertMemoryLeak(() -> {
            createTable();
            execute("insert into t select (x * 1_000_000L)::timestamp, "
                    + "'k' || (x % 2), 'p', "
                    + "case when x % 7 = 0 then null else (x % 29)::double end, "
                    + "case when x % 5 = 0 then null else (x % 13)::double end, "
                    + "x from long_sequence(400)");
            final String window = "window w as (partition by k order by ts "
                    + "range between 100_000_000 microseconds preceding and current row)";
            assertFusedMatchesUnfusedOnWindow("t", window, "sum(x) over w", "avg(x) over w");
            assertFusedMatchesUnfusedOnWindow(
                    "t",
                    window,
                    "sum(x) over w",
                    "avg(x) over w",
                    "count(x) over w",
                    "count(y) over w"
            );
        });
    }

    @Test
    public void testABoundedRangeSumAndAvgShareOneFrame() throws Exception {
        // The acceptance shape for the bounded-RANGE families. It is the bounded-ROWS one with a
        // wider slice: a RANGE frame's length is the timestamps' answer, so the ring grows on
        // demand and the value carries its length and capacity beside its address. sum(x) and
        // avg(x) over one such frame are two readings of one state, so the group keeps one
        // component, one ring and one argument evaluation where the unfused pair keeps two of each.
        assertMemoryLeak(() -> {
            createTable();
            insertKeyShapes();
            final String sql = "select ts, sum(x) over w, avg(x) over w from t " + RANGE_FRAME_WINDOW;
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = select(compiler, sql, sqlExecutionContext)) {
                final WindowRecordCursorFactory windowFactory = windowFactory(factory);
                assertBoundGroupCount(windowFactory, 1);
                final WindowMapState state = windowFactory.getWindowMapStates().getQuick(0);
                final WindowAccumulatorPlan plan = state.getPlan();
                Assert.assertEquals(1, plan.getComponentCount());
                Assert.assertEquals(2, plan.getProjectionCount());
                // [sum, count, ringIndex, ringOffset, ringSize, ringCapacity] - the widest single
                // component this build fuses, and every slot of it is the state's.
                Assert.assertEquals(6, plan.getSlotCount());
                Assert.assertTrue(plan.getComponent(0).isRingBacked());
                Assert.assertEquals(
                        WindowAccumulatorDescriptor.FAMILY_DOUBLE_RANGE_SUM_COUNT,
                        plan.getComponent(0).getFamily()
                );
                Assert.assertFalse(plan.getProjection(0).isDerived());
                Assert.assertFalse(plan.getProjection(1).isDerived());
                Assert.assertEquals(0, plan.getProjection(0).getSumSlot());
                Assert.assertEquals(1, plan.getProjection(0).getNonNullCountSlot());
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final long rows = drain(cursor);
                    Assert.assertEquals(KEY_SHAPE_ROW_COUNT, rows);
                    // One accumulator for two outputs, so the frame is maintained once.
                }
            }
            assertFusedMatchesUnfusedOnWindow("t", RANGE_FRAME_WINDOW, "sum(x) over w", "avg(x) over w");
        });
    }

    @Test
    public void testABoundedRowsCountKeepsItsOwnRing() throws Exception {
        // The pair that says the ring families are co-located and not merged. A bounded count(x)
        // emits the very number the bounded sum(x) beside it keeps in its counter, and it still
        // gets a component of its own: what the fold licenses is that the guest's whole state is
        // a run inside the host's, and this guest's state continues outside the slice in a ring
        // of flags where the host's holds doubles. So the group buys them one key, one hash table
        // and one lookup a row, and each keeps the frame it maintains.
        assertMemoryLeak(() -> {
            createTable();
            insertKeyShapes();
            final String sql = "select ts, sum(x) over w, count(x) over w from t " + ROWS_FRAME_WINDOW;
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = select(compiler, sql, sqlExecutionContext)) {
                final WindowRecordCursorFactory windowFactory = windowFactory(factory);
                assertBoundGroupCount(windowFactory, 1);
                final WindowMapState state = windowFactory.getWindowMapStates().getQuick(0);
                final WindowAccumulatorPlan plan = state.getPlan();
                Assert.assertEquals(2, plan.getComponentCount());
                Assert.assertEquals(2, plan.getProjectionCount());
                // [sum, count, ringIndex, ringOffset] then [count, ringIndex, ringOffset].
                Assert.assertEquals(7, plan.getSlotCount());
                Assert.assertEquals(0, plan.getComponentSlotBase(0));
                Assert.assertEquals(4, plan.getComponentSlotBase(1));
                Assert.assertTrue(plan.getComponent(0).isRingBacked());
                Assert.assertTrue(plan.getComponent(1).isRingBacked());
                Assert.assertFalse(plan.getProjection(0).isDerived());
                Assert.assertFalse(plan.getProjection(1).isDerived());
                // Two counters, and they are two slots: the count's own is the component the
                // fold declined to move.
                Assert.assertEquals(1, plan.getProjection(0).getNonNullCountSlot());
                Assert.assertEquals(4, plan.getProjection(1).getNonNullCountSlot());
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final long rows = drain(cursor);
                    Assert.assertEquals(KEY_SHAPE_ROW_COUNT, rows);
                }
            }
            assertFusedMatchesUnfusedOnWindow("t", ROWS_FRAME_WINDOW, "sum(x) over w", "count(x) over w");
        });
    }

    @Test
    public void testABoundedRowsSumAndAvgShareOneFrame() throws Exception {
        // The acceptance shape for the ring-backed families, and the first group in this build
        // whose state is not all in the map value: the slice carries the frame's total, its
        // counter and the ring's address, and the ring itself stays in the arena the contributing
        // function already owned. sum(x) and avg(x) over one bounded frame are two readings of
        // one such state, so the group keeps one component, one ring and one argument evaluation
        // where the unfused pair keeps two of each.
        //
        // The data is the key-shape one, whose partitions include a NULL key, one of a single row
        // and one whose only non-null x is an infinity - a frame with rows in it and no value the
        // total contributes.
        assertMemoryLeak(() -> {
            createTable();
            insertKeyShapes();
            final String sql = "select ts, sum(x) over w, avg(x) over w from t " + ROWS_FRAME_WINDOW;
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = select(compiler, sql, sqlExecutionContext)) {
                final WindowRecordCursorFactory windowFactory = windowFactory(factory);
                assertBoundGroupCount(windowFactory, 1);
                final WindowMapState state = windowFactory.getWindowMapStates().getQuick(0);
                final WindowAccumulatorPlan plan = state.getPlan();
                Assert.assertEquals(1, plan.getComponentCount());
                Assert.assertEquals(2, plan.getProjectionCount());
                // [sum, count, ringIndex, ringOffset] - the two index slots are the state's as
                // much as the total is, and they are what the group's value carries instead of
                // the ring.
                Assert.assertEquals(4, plan.getSlotCount());
                Assert.assertTrue(plan.getComponent(0).isRingBacked());
                Assert.assertEquals(
                        WindowAccumulatorDescriptor.FAMILY_DOUBLE_ROWS_SUM_COUNT,
                        plan.getComponent(0).getFamily()
                );
                // Neither output is derived: both read the whole component, which is what a
                // merge on identity looks like against a fold onto a wider host.
                Assert.assertFalse(plan.getProjection(0).isDerived());
                Assert.assertFalse(plan.getProjection(1).isDerived());
                Assert.assertEquals(0, plan.getProjection(0).getSumSlot());
                Assert.assertEquals(1, plan.getProjection(0).getNonNullCountSlot());
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final long rows = drain(cursor);
                    Assert.assertEquals(KEY_SHAPE_ROW_COUNT, rows);
                    // One accumulator for two outputs, so the frame is maintained once.
                }
            }
            assertFusedMatchesUnfusedOnWindow("t", ROWS_FRAME_WINDOW, "sum(x) over w", "avg(x) over w");
        });
    }

    @Test
    public void testACapturedValueSharesTheKeyWithAnAccumulator() throws Exception {
        // A capture beside a running total over the same argument. Nothing merges - what a sum
        // keeps is not one row's value - so the group buys them one key and one lookup and each
        // keeps what it keeps, which is physical co-location doing exactly what it was for. The
        // count still folds onto the sum, which is what says admitting a family did not disturb
        // the folds already proved.
        assertMemoryLeak(() -> {
            createTable();
            insertKeyShapes();
            final String sql = "select ts, sum(x) over w, count(x) over w, first_value(x) over w from t " + WINDOW;
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = select(compiler, sql, sqlExecutionContext)) {
                final WindowRecordCursorFactory windowFactory = windowFactory(factory);
                assertBoundGroupCount(windowFactory, 1);
                final WindowMapState state = windowFactory.getWindowMapStates().getQuick(0);
                final WindowAccumulatorPlan plan = state.getPlan();
                Assert.assertEquals(2, plan.getComponentCount());
                Assert.assertEquals(3, plan.getProjectionCount());
                // [sum, count] then [value, captured] - the sum family's id is the lower one.
                Assert.assertEquals(4, plan.getSlotCount());
                Assert.assertEquals(0, plan.getComponentSlotBase(0));
                Assert.assertEquals(2, plan.getComponentSlotBase(1));
                Assert.assertTrue(plan.getProjection(1).isDerived());
                Assert.assertEquals(1, plan.getProjection(1).getNonNullCountSlot());
                Assert.assertFalse(plan.getProjection(2).isDerived());
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final long rows = drain(cursor);
                    Assert.assertEquals(KEY_SHAPE_ROW_COUNT, rows);
                }
            }
            assertFusedMatchesUnfused("sum(x) over w", "count(x) over w", "first_value(x) over w");
        });
    }

    @Test
    public void testACloseFreesTheGroupsBackingAndAReopenAllocatesItAgain() throws Exception {
        // The group's map is lazy for the same reason every other tracker-aware window state
        // is: the backing has to be allocated after the per-query tracker is bound and handed
        // back before it is unbound, so the malloc and the free land on one counter. Asserted
        // as openness rather than as bytes, which is the part a caller can see.
        assertMemoryLeak(() -> {
            createTable();
            insertOrdinaryRows();
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = select(compiler, sumAndCount(), sqlExecutionContext)) {
                final WindowRecordCursorFactory windowFactory = windowFactory(factory);
                assertBoundGroupCount(windowFactory, 1);
                final WindowMapState state = windowFactory.getWindowMapStates().getQuick(0);
                Assert.assertFalse("the group allocated before a tracker was bound", state.isMapOpen());
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(state.isMapOpen());
                    drain(cursor);
                }
                Assert.assertFalse("close left the group's backing allocated", state.isMapOpen());
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue("a reopen after close allocated nothing", state.isMapOpen());
                    // A second execution starts from an empty key domain, so the running
                    // totals restart rather than continuing the first cursor's.
                    Assert.assertEquals(ORDINARY_ROW_COUNT, drain(cursor));
                }
            }
        });
    }

    @Test
    public void testAFailedOpenLeavesTheFactoryReusable() throws Exception {
        // An of()-time breach: the group's map is the first thing the open allocates, because
        // every member's private map stays closed. The failure has to free whatever the open
        // did manage - assertMemoryLeak is what says it did - return the base cursor's reader,
        // and leave the cursor closed rather than half-open, which the successful drain
        // afterwards is what proves.
        assertMemoryLeak(() -> {
            createTable();
            insertOrdinaryRows();
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = select(compiler, twoWindows(), sqlExecutionContext)) {
                final WindowRecordCursorFactory windowFactory = windowFactory(factory);
                assertBoundGroupCount(windowFactory, 2);
                final ObjList<WindowMapState> states = windowFactory.getWindowMapStates();
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 64L);
                for (int i = 0; i < 5; i++) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.fail("expected a per-query memory breach during open, got: " + cursor);
                    } catch (CairoException e) {
                        Assert.assertTrue(
                                "expected isOutOfMemory(), got: " + e.getFlyweightMessage(),
                                e.isOutOfMemory()
                        );
                        TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                    }
                    // Whichever group the breach landed on, none is left holding backing: the
                    // close the failed open runs frees every group rather than the ones after
                    // the failure. Stated as an invariant because which group breaches is a
                    // function of the configured allocation sizes and not of this test.
                    for (int g = 0, n = states.size(); g < n; g++) {
                        Assert.assertFalse("group " + g + " kept its map open", states.getQuick(g).isMapOpen());
                    }
                }
                Assert.assertEquals("busy reader count", 0, engine.getBusyReaderCount());
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0L);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertEquals(ORDINARY_ROW_COUNT, drain(cursor));
                }
            }
        });
    }

    @Test
    public void testAGroupWhoseMemberIsAlreadyOrderedKeepsThatMap() throws Exception {
        // sum(x)'s own [DOUBLE, LONG] value is 4 + 16 = 20 against the 16-byte limit
        // DefaultCairoConfiguration returns, so its private map is an OrderedMap before any
        // fusion; co-locating count(y)'s counter beside it removes a map without changing the
        // implementation of the one left. The group's map is the one MapFactory selects for the
        // widened value, which is what this pins - the second point of that function beside the
        // transition the case below walks.
        assertMemoryLeak(() -> {
            createTable();
            insertOrdinaryRows();
            setProperty(PropertyKey.CAIRO_SQL_UNORDERED_MAP_MAX_ENTRY_SIZE, 16);
            assertBoundMapImplementation(sumAndCount(), "OrderedMap", 16);
        });
    }

    @Test
    public void testAKahanSumKeepsItsOwnTotalAndLendsItsCounter() throws Exception {
        // The other fixed scalar this step admits, and the pair that says a component's
        // identity is its arithmetic rather than its layout. ksum(x) and sum(x) contribute on
        // one predicate over one argument and both start at zero, and they are two components
        // because a compensated total and a plain one are different numbers over the same
        // rows - so neither may read the other's first slot. Their counters do agree, and
        // count(x) folds onto whichever host the canonical order puts first.
        assertMemoryLeak(() -> {
            createTable();
            insertKeyShapes();
            // ksum alone with the count: one three-slot component for two outputs, with the
            // count reading the Kahan counter at the component's third slot. This is the fold
            // this step adds, and the compensation term is what makes the counter's slot 2
            // rather than the 1 a plain sum's is.
            final String withCount = "select ts, ksum(x) over w, count(x) over w from t " + WINDOW;
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = select(compiler, withCount, sqlExecutionContext)) {
                final WindowRecordCursorFactory windowFactory = windowFactory(factory);
                assertBoundGroupCount(windowFactory, 1);
                final WindowMapState state = windowFactory.getWindowMapStates().getQuick(0);
                final WindowAccumulatorPlan plan = state.getPlan();
                Assert.assertEquals(1, plan.getComponentCount());
                Assert.assertEquals(2, plan.getProjectionCount());
                Assert.assertEquals(3, plan.getSlotCount());
                Assert.assertFalse(plan.getProjection(0).isDerived());
                Assert.assertTrue(plan.getProjection(1).isDerived());
                Assert.assertEquals(2, plan.getProjection(1).getNonNullCountSlot());
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final long rows = drain(cursor);
                    Assert.assertEquals(KEY_SHAPE_ROW_COUNT, rows);
                    // One accumulator for two outputs, so x is read and compensated once.
                }
            }
            // Both sums over one argument: five slots, and each total maintained by the
            // function that emits it.
            final String bothSums = "select ts, ksum(x) over w, sum(x) over w from t " + WINDOW;
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = select(compiler, bothSums, sqlExecutionContext)) {
                final WindowRecordCursorFactory windowFactory = windowFactory(factory);
                assertBoundGroupCount(windowFactory, 1);
                final WindowMapState state = windowFactory.getWindowMapStates().getQuick(0);
                final WindowAccumulatorPlan plan = state.getPlan();
                Assert.assertEquals(2, plan.getComponentCount());
                Assert.assertEquals(2, plan.getProjectionCount());
                Assert.assertEquals(5, plan.getSlotCount());
                Assert.assertFalse(plan.getProjection(0).isDerived());
                Assert.assertFalse(plan.getProjection(1).isDerived());
                Assert.assertNotEquals(
                        "the two totals must not share a slot",
                        plan.getProjection(0).getComponentSlotBase(),
                        plan.getProjection(1).getComponentSlotBase()
                );
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final long rows = drain(cursor);
                }
            }
            assertFusedMatchesUnfused("ksum(x) over w", "count(x) over w");
            assertFusedMatchesUnfused("ksum(x) over w", "sum(x) over w");
            assertFusedMatchesUnfused("ksum(x) over w", "sum(x) over w", "count(x) over w");
        });
    }

    @Test
    public void testAMaxAndAMinOverOneArgumentKeepTwoComponents() throws Exception {
        // The negative control the extremum families need. max(x) and min(x) agree on which
        // rows contribute and on the width and type of what they keep, and share nothing: a
        // running maximum cannot be read out of a running minimum, so the fold table admits
        // neither into the other and the group keeps two slots for two outputs. What it does
        // share is the key domain and the row's one lookup, which is the whole of what
        // physical co-location was for.
        //
        // The data is the key-shape one, whose partitions include a NULL key, one whose only
        // non-null x is an infinity - so it has rows, and no value either extremum
        // contributes - and one of a single row.
        assertMemoryLeak(() -> {
            createTable();
            insertKeyShapes();
            final String sql = "select ts, max(x) over w, min(x) over w from t " + WINDOW;
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = select(compiler, sql, sqlExecutionContext)) {
                final WindowRecordCursorFactory windowFactory = windowFactory(factory);
                assertBoundGroupCount(windowFactory, 1);
                final WindowMapState state = windowFactory.getWindowMapStates().getQuick(0);
                final WindowAccumulatorPlan plan = state.getPlan();
                Assert.assertEquals(2, plan.getComponentCount());
                Assert.assertEquals(2, plan.getProjectionCount());
                Assert.assertEquals(2, plan.getSlotCount());
                // Neither output reads a component wider than its own, which is what says the
                // two states stayed apart.
                Assert.assertFalse(plan.getProjection(0).isDerived());
                Assert.assertFalse(plan.getProjection(1).isDerived());
                Assert.assertEquals(0, plan.getProjection(0).getComponentSlotBase());
                Assert.assertEquals(1, plan.getProjection(1).getComponentSlotBase());
                // No counter behind either of them - the extremum is the whole state - which is
                // why the binding is what says a function is fused and the counter no longer is.
                Assert.assertEquals(-1, plan.getProjection(0).getNonNullCountSlot());
                Assert.assertEquals(-1, plan.getProjection(1).getNonNullCountSlot());
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final long rows = drain(cursor);
                    Assert.assertEquals(KEY_SHAPE_ROW_COUNT, rows);
                }
            }
            assertFusedMatchesUnfused("max(x) over w", "min(x) over w");
        });
    }

    @Test
    public void testAWelfordComponentNeverLendsItsMeanToASum() throws Exception {
        // The negative control for the merge. stddev_samp(x) and sum(x) are two accumulators
        // over one argument that agree on which rows contribute and on nothing else, and both
        // carry a DOUBLE in their first slot - Welford's running mean and the running sum. The
        // fold table admits neither into the other, so the group keeps two components, and the
        // count folds onto the sum rather than onto Welford because the sum's identity is the
        // smaller of the two hosts that could serve it.
        assertMemoryLeak(() -> {
            createTable();
            insertKeyShapes();
            final String sql = "select ts, stddev_samp(x) over w, sum(x) over w, avg(x) over w, "
                    + "count(x) over w from t " + WINDOW;
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = select(compiler, sql, sqlExecutionContext)) {
                final WindowRecordCursorFactory windowFactory = windowFactory(factory);
                assertBoundGroupCount(windowFactory, 1);
                final WindowMapState state = windowFactory.getWindowMapStates().getQuick(0);
                final WindowAccumulatorPlan plan = state.getPlan();
                Assert.assertEquals(2, plan.getComponentCount());
                Assert.assertEquals(4, plan.getProjectionCount());
                // [sum, count] then [mean, m2, count] - components sort by identity, and the
                // sum family's id is the lower of the two.
                Assert.assertEquals(5, plan.getSlotCount());
                Assert.assertEquals(0, plan.getComponentSlotBase(0));
                Assert.assertEquals(2, plan.getComponentSlotBase(1));
                // The count reads the sum's counter at slot 1, not Welford's at slot 4.
                Assert.assertTrue(plan.getProjection(3).isDerived());
                Assert.assertEquals(1, plan.getProjection(3).getNonNullCountSlot());
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final long rows = drain(cursor);
                    Assert.assertEquals(KEY_SHAPE_ROW_COUNT, rows);
                    // Two accumulators, four outputs: x is read once per component and not
                    // once per call.
                }
            }
            assertFusedMatchesUnfused(
                    "stddev_samp(x) over w",
                    "sum(x) over w",
                    "avg(x) over w",
                    "count(x) over w"
            );
        });
    }

    @Test
    public void testAWideDecimalExtremumSitsBesideACounterInOneValue() throws Exception {
        // The wide slot with something in front of it. count(d128) keeps a LONG counter and
        // sorts first, so the DECIMAL128 the extremum keeps starts at the value's second slot -
        // which is the reading a slot base has to get right and a single-component group cannot
        // exercise.
        //
        // The two also share an argument and a contribution predicate - both skip exactly the
        // rows where d128 is absent - and are still two components, because a counter is not a
        // run inside an extremum and an extremum keeps no counter to lend.
        assertMemoryLeak(() -> {
            createDecimalTable();
            insertDecimalKeyShapes();
            final String sql = "select ts, count(d128) over w, max(d128) over w from td " + WINDOW;
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = select(compiler, sql, sqlExecutionContext)) {
                final WindowRecordCursorFactory windowFactory = windowFactory(factory);
                assertBoundGroupCount(windowFactory, 1);
                final WindowMapState state = windowFactory.getWindowMapStates().getQuick(0);
                final WindowAccumulatorPlan plan = state.getPlan();
                Assert.assertEquals(2, plan.getComponentCount());
                Assert.assertEquals(2, plan.getProjectionCount());
                Assert.assertEquals(2, plan.getSlotCount());
                Assert.assertEquals(ColumnType.LONG, plan.getComponent(0).getSlotColumnType(0));
                Assert.assertEquals(ColumnType.DECIMAL128, plan.getComponent(1).getSlotColumnType(0));
                Assert.assertEquals(0, plan.getComponentSlotBase(0));
                Assert.assertEquals(1, plan.getComponentSlotBase(1));
                Assert.assertFalse(plan.getProjection(0).isDerived());
                Assert.assertFalse(plan.getProjection(1).isDerived());
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final long rows = drain(cursor);
                    Assert.assertEquals(DECIMAL_KEY_SHAPE_ROW_COUNT, rows);
                }
            }
            assertFusedMatchesUnfusedOn("td", "count(d128) over w", "max(d128) over w");
        });
    }

    @Test
    public void testAllNullPartitionAndNullKey() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            // 'nk' has no x at all, so its sum stays NULL however many rows it counts, and the
            // NULL key is a partition of its own that both outputs must find again on every
            // row rather than treating as absent.
            execute("insert into t values " +
                    "('2024-01-01T00:00:00.000000Z', 'nk', 'p', null, 1.0, null), " +
                    "('2024-01-01T00:00:01.000000Z', null, 'p', 2.0, null, 1), " +
                    "('2024-01-01T00:00:02.000000Z', 'nk', 'q', null, null, null), " +
                    "('2024-01-01T00:00:03.000000Z', null, 'q', 3.0, 4.0, 2), " +
                    "('2024-01-01T00:00:04.000000Z', 'nk', 'p', null, 5.0, null), " +
                    "('2024-01-01T00:00:05.000000Z', null, 'p', null, 6.0, 3)");
            assertFusedMatchesUnfused("sum(x) over w", "count(y) over w");
        });
    }

    @Test
    public void testAnExpressionKeyAndItsColumnsAreTwoGroups() throws Exception {
        // concat(k, k2) is not k, and nothing about the two rendered identities could make it
        // one - which is the negative control an expression key needs and a column key has in
        // testTwoWindowsGetTwoGroupsAndShareNothing.
        assertMemoryLeak(() -> {
            createTable();
            insertOrdinaryRows();
            final String sql = "select ts, sum(x) over w, count(y) over w, sum(x) over w2, count(y) over w2 from t "
                    + "window w as (partition by concat(k, k2) order by ts "
                    + "rows between unbounded preceding and current row), "
                    + "w2 as (partition by k order by ts rows between unbounded preceding and current row)";
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = select(compiler, sql, sqlExecutionContext)) {
                final WindowRecordCursorFactory windowFactory = windowFactory(factory);
                assertBoundGroupCount(windowFactory, 2);
                final ObjList<WindowMapState> states = windowFactory.getWindowMapStates();
                Assert.assertFalse(
                        states.getQuick(0).getPlan().getSpec().isSameSpec(states.getQuick(1).getPlan().getSpec())
                );
                // One of the two writes its key through compiled terms and the other off the
                // record's own columns, which is the two ways a group has of doing it.
                Assert.assertNotEquals(
                        states.getQuick(0).getPlan().getSpec().hasExpressionPartitionKey(),
                        states.getQuick(1).getPlan().getSpec().hasExpressionPartitionKey()
                );
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final long rows = drain(cursor);
                }
            }
        });
    }

    @Test
    public void testAnExpressionKeyIsEvaluatedOnceForTheWholeGroup() throws Exception {
        // A key no column carries. What the group removes here is one more thing than it
        // removes from a column-keyed query: the members would each have evaluated the
        // expression a row through a partitionByRecord of their own, and the group evaluates
        // it once through the terms it borrows from one of them.
        assertMemoryLeak(() -> {
            createTable();
            insertKeyShapes();
            final String sql = "select ts, sum(x) over w, avg(x) over w, count(y) over w from t "
                    + EXPRESSION_WINDOW;
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = select(compiler, sql, sqlExecutionContext)) {
                final WindowRecordCursorFactory windowFactory = windowFactory(factory);
                assertBoundGroupCount(windowFactory, 1);
                final WindowMapState state = windowFactory.getWindowMapStates().getQuick(0);
                // sum and avg on one component, count(y) on its own - the sharing is the
                // arguments' business and the key is the window's.
                Assert.assertEquals(2, state.getPlan().getComponentCount());
                Assert.assertEquals(3, state.getPlan().getProjectionCount());
                Assert.assertTrue(state.getPlan().getSpec().hasExpressionPartitionKey());
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final long rows = drain(cursor);
                    Assert.assertEquals(KEY_SHAPE_ROW_COUNT, rows);
                }
            }
            // And the answers are the unfused path's, over the shapes an expression key is
            // most able to get wrong: a NULL in one of the columns it reads, which concat
            // answers for rather than propagates, and a partition of a single row.
            assertFusedMatchesUnfusedOnWindow(
                    "t",
                    EXPRESSION_WINDOW,
                    "sum(x) over w",
                    "avg(x) over w",
                    "count(y) over w"
            );
            assertFusedMatchesUnfusedOnWindow(
                    "t",
                    EXPRESSION_WINDOW,
                    "count(*) over w",
                    "row_number() over w",
                    "max(l) over w"
            );
            // A second cursor over the same factory: the group positions its borrowed record
            // on every row of every traversal rather than once, so a rewind that re-read a
            // stale one would show here.
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = select(compiler, sql, sqlExecutionContext)) {
                final StringSink first = new StringSink();
                final StringSink second = new StringSink();
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    CursorPrinter.println(cursor, factory.getMetadata(), first, true, false);
                    cursor.toTop();
                    CursorPrinter.println(cursor, factory.getMetadata(), second, true, false);
                }
                TestUtils.assertEquals(first, second);
            }
        });
    }

    @Test
    public void testAnExtremumSharesTheKeyWithASumOverTheSameArgument() throws Exception {
        // The extremum families beside the accumulating ones, over one argument. Nothing
        // merges: a sum's first slot is a running total and not the largest thing ever added
        // to it, so max(x) keeps a slot of its own next to the (sum, count) pair - and the
        // count still folds onto that pair, which is what says admitting a family did not
        // disturb the folds already proved.
        assertMemoryLeak(() -> {
            createTable();
            insertKeyShapes();
            final String sql = "select ts, sum(x) over w, max(x) over w, count(x) over w from t " + WINDOW;
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = select(compiler, sql, sqlExecutionContext)) {
                final WindowRecordCursorFactory windowFactory = windowFactory(factory);
                assertBoundGroupCount(windowFactory, 1);
                final WindowMapState state = windowFactory.getWindowMapStates().getQuick(0);
                final WindowAccumulatorPlan plan = state.getPlan();
                Assert.assertEquals(2, plan.getComponentCount());
                Assert.assertEquals(3, plan.getProjectionCount());
                // [sum, count] then [max] - components sort by identity, and the sum family's
                // id is the lower of the two.
                Assert.assertEquals(3, plan.getSlotCount());
                Assert.assertEquals(0, plan.getComponentSlotBase(0));
                Assert.assertEquals(2, plan.getComponentSlotBase(1));
                // The count reads the sum's counter at slot 1; the extremum reads slot 2 and
                // lends nothing.
                Assert.assertTrue(plan.getProjection(2).isDerived());
                Assert.assertEquals(1, plan.getProjection(2).getNonNullCountSlot());
                Assert.assertFalse(plan.getProjection(1).isDerived());
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final long rows = drain(cursor);
                    Assert.assertEquals(KEY_SHAPE_ROW_COUNT, rows);
                    // Two accumulators for three outputs: x is read once per component.
                }
            }
            assertFusedMatchesUnfused("sum(x) over w", "max(x) over w", "count(x) over w");
        });
    }

    @Test
    public void testCapturesOverEveryAdmittedStateTypeShareOneKey() throws Exception {
        // The capture families at both of the state widths they are split by, and the three
        // implementations behind them: x through the DOUBLE factory, l through the LONG one and
        // ts through the timestamp one, which is a separate class over the shared helper base.
        // Four maps and four probes unfused; seven slots behind one key here.
        //
        // The capture-shape data is what makes the 64-bit flag do work: partition 'a' opens on an
        // absent l and carries a present one after it, so a respect-nulls capture that read its
        // emptiness off LONG_NULL would answer the second row's payload for the whole partition.
        assertMemoryLeak(() -> {
            createTable();
            insertCaptureShapes();
            final String sql = "select ts, first_value(x) over w, first_value(l) over w, "
                    + "first_value(ts) ignore nulls over w, last_value(l) ignore nulls over w from t " + WINDOW;
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = select(compiler, sql, sqlExecutionContext)) {
                final WindowRecordCursorFactory windowFactory = windowFactory(factory);
                assertBoundGroupCount(windowFactory, 1);
                final WindowMapState state = windowFactory.getWindowMapStates().getQuick(0);
                final WindowAccumulatorPlan plan = state.getPlan();
                Assert.assertEquals(4, plan.getComponentCount());
                Assert.assertEquals(4, plan.getProjectionCount());
                // Two flagged captures and one flat one, plus the DOUBLE capture's own pair.
                Assert.assertEquals(7, plan.getSlotCount());
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final long rows = drain(cursor);
                    Assert.assertEquals(CAPTURE_SHAPE_ROW_COUNT, rows);
                }
            }
            assertFusedMatchesUnfused(
                    "first_value(x) over w",
                    "first_value(l) over w",
                    "first_value(ts) ignore nulls over w",
                    "last_value(l) ignore nulls over w"
            );
        });
    }

    @Test
    public void testCountStarNeverAliasesCountOfAColumn() throws Exception {
        // Two counters over one window that agree on nothing but the rows where x is present.
        // They are separate components by identity - a row count takes no argument at all -
        // and the group fuses the key and the lookup around them without touching either.
        assertMemoryLeak(() -> {
            createTable();
            insertNullsAndInfinities();
            assertFusedMatchesUnfused("count(*) over w", "count(x) over w");
        });
    }

    @Test
    public void testCountStarRowNumberAndAKeyCountShareOneCounter() throws Exception {
        // The row-count family, and the one projection whose value is not a function of the
        // state alone: count(k) over the very column its window partitions by emits the
        // partition's row count where k is present and zero where it is not. The three share
        // one LONG, and the guarded call is never the one that maintains it - its own counter
        // would be zero for the whole NULL-key partition, which the data below has.
        assertMemoryLeak(() -> {
            createTable();
            insertKeyShapes();
            final String sql = "select ts, count(*) over w, row_number() over w, count(k) over w from t " + WINDOW;
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = select(compiler, sql, sqlExecutionContext)) {
                final WindowRecordCursorFactory windowFactory = windowFactory(factory);
                assertBoundGroupCount(windowFactory, 1);
                final WindowMapState state = windowFactory.getWindowMapStates().getQuick(0);
                final WindowAccumulatorPlan plan = state.getPlan();
                Assert.assertEquals(1, plan.getComponentCount());
                Assert.assertEquals(3, plan.getProjectionCount());
                Assert.assertEquals(1, plan.getSlotCount());
                Assert.assertTrue(plan.getProjection(2).isPartitionKeyGuarded());
                Assert.assertNotEquals(2, plan.getContributorIndex(0));
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final long rows = drain(cursor);
                    Assert.assertEquals(KEY_SHAPE_ROW_COUNT, rows);
                }
            }
            assertFusedMatchesUnfused("count(*) over w", "row_number() over w", "count(k) over w");
        });
    }

    @Test
    public void testDecimalExtremaOverEveryWidthShareOneKey() throws Exception {
        // A DECIMAL extremum keeps its argument's own payload, so this is the group where the
        // fused value stops being a list of 64-bit words: the four narrow widths take a LONG
        // slot each and the two wide ones take a DECIMAL128 and a DECIMAL256 of the group's own
        // value. Twelve calls that would be twelve maps and twelve probes a row unfused are
        // twelve slots behind one key.
        //
        // The data is the decimal key-shape one, whose partitions include a NULL key, one of a
        // single row and one whose decimals are absent on every row - the last is what says an
        // empty state reads back as this width's own NULL rather than as a zero.
        assertMemoryLeak(() -> {
            createDecimalTable();
            insertDecimalKeyShapes();
            final String sql = "select ts"
                    + ", max(d8) over w, min(d8) over w"
                    + ", max(d16) over w, min(d16) over w"
                    + ", max(d32) over w, min(d32) over w"
                    + ", max(d64) over w, min(d64) over w"
                    + ", max(d128) over w, min(d128) over w"
                    + ", max(d256) over w, min(d256) over w"
                    + " from td " + WINDOW;
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = select(compiler, sql, sqlExecutionContext)) {
                final WindowRecordCursorFactory windowFactory = windowFactory(factory);
                assertBoundGroupCount(windowFactory, 1);
                final WindowMapState state = windowFactory.getWindowMapStates().getQuick(0);
                final WindowAccumulatorPlan plan = state.getPlan();
                // Nothing merges: a max is not a min, and two widths of one direction are two
                // states over two columns.
                Assert.assertEquals(12, plan.getComponentCount());
                Assert.assertEquals(12, plan.getProjectionCount());
                Assert.assertEquals(12, plan.getSlotCount());
                int narrow = 0;
                int wide128 = 0;
                int wide256 = 0;
                for (int i = 0; i < 12; i++) {
                    Assert.assertFalse(plan.getProjection(i).isDerived());
                    // No counter behind any of them - an extremum is its own whole state at
                    // every width.
                    Assert.assertEquals(-1, plan.getProjection(i).getNonNullCountSlot());
                    switch (plan.getComponent(i).getSlotColumnType(0)) {
                        case ColumnType.LONG:
                            narrow++;
                            break;
                        case ColumnType.DECIMAL128:
                            wide128++;
                            break;
                        case ColumnType.DECIMAL256:
                            wide256++;
                            break;
                        default:
                            Assert.fail("unexpected slot type for component " + i);
                    }
                }
                Assert.assertEquals(8, narrow);
                Assert.assertEquals(2, wide128);
                Assert.assertEquals(2, wide256);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final long rows = drain(cursor);
                    Assert.assertEquals(DECIMAL_KEY_SHAPE_ROW_COUNT, rows);
                }
            }
            assertFusedMatchesUnfusedOn(
                    "td",
                    "max(d8) over w", "min(d8) over w",
                    "max(d16) over w", "min(d16) over w",
                    "max(d32) over w", "min(d32) over w",
                    "max(d64) over w", "min(d64) over w",
                    "max(d128) over w", "min(d128) over w",
                    "max(d256) over w", "min(d256) over w"
            );
        });
    }

    @Test
    public void testEveryBoundedRangeGeometryMatchesTheUnfusedPath() throws Exception {
        // The three shapes a bounded RANGE frame comes in - ending at the current row, ending
        // short of it, and with no low bound at all - which are the three arms the contributor's
        // ring bookkeeping takes. What a RANGE frame adds over the ROWS spelling is the resize: a
        // partition denser than the initial buffer grows its ring mid-traversal, which moves the
        // address and the read cursor the slice carries, so a group that dropped either would
        // answer from the wrong slab rather than merely from the wrong row.
        //
        // Run over both data sets, for the reasons the ROWS case gives: the key shapes carry a
        // partition of a single row and one whose only non-null x is an infinity, and the
        // nulls-and-infinities one puts the absent and non-finite values inside the frame.
        assertMemoryLeak(() -> {
            createTable();
            insertKeyShapes();
            for (int i = 0; i < RANGE_FRAME_GEOMETRIES.length; i++) {
                final String window = RANGE_FRAME_GEOMETRIES[i];
                assertFusedMatchesUnfusedOnWindow("t", window, "sum(x) over w", "avg(x) over w");
                assertFusedMatchesUnfusedOnWindow(
                        "t",
                        window,
                        "sum(x) over w",
                        "avg(x) over w",
                        "count(x) over w",
                        "count(y) over w"
                );
                assertFusedMatchesUnfusedOnWindow("t", window, "count(k) over w", "count(y) over w");
            }
            execute("truncate table t");
            insertNullsAndInfinities();
            for (int i = 0; i < RANGE_FRAME_GEOMETRIES.length; i++) {
                assertFusedMatchesUnfusedOnWindow(
                        "t",
                        RANGE_FRAME_GEOMETRIES[i],
                        "sum(x) over w",
                        "avg(x) over w",
                        "count(x) over w",
                        "count(y) over w"
                );
            }
        });
    }

    @Test
    public void testEveryBoundedRowsGeometryMatchesTheUnfusedPath() throws Exception {
        // The three shapes a bounded ROWS frame comes in, which are three different rings and
        // the whole of what the deferred subtraction has to get right:
        //
        //   - a frame ending at the current row, whose entering value is the row's own and whose
        //     ring is one cell longer than the number of preceding rows it spans;
        //   - a frame whose high bound lags, so the entering value comes out of the ring too and
        //     the two reads are a computed distance apart;
        //   - one with no low bound at all, where nothing ever leaves, the ring keeps its unfused
        //     length and the unfused arithmetic already left the answer in the slots.
        //
        // Run over both data sets: the key shapes, whose partitions include one of a single row -
        // fewer rows than the frame spans - and the nulls-and-infinities one, where the absent
        // and non-finite values land inside the ring rather than only at a partition's start.
        assertMemoryLeak(() -> {
            createTable();
            insertKeyShapes();
            for (int i = 0; i < ROWS_FRAME_GEOMETRIES.length; i++) {
                final String window = ROWS_FRAME_GEOMETRIES[i];
                assertFusedMatchesUnfusedOnWindow("t", window, "sum(x) over w", "avg(x) over w");
                assertFusedMatchesUnfusedOnWindow(
                        "t",
                        window,
                        "sum(x) over w",
                        "avg(x) over w",
                        "count(x) over w",
                        "count(y) over w"
                );
                assertFusedMatchesUnfusedOnWindow("t", window, "count(k) over w", "count(y) over w");
            }
            execute("truncate table t");
            insertNullsAndInfinities();
            for (int i = 0; i < ROWS_FRAME_GEOMETRIES.length; i++) {
                assertFusedMatchesUnfusedOnWindow(
                        "t",
                        ROWS_FRAME_GEOMETRIES[i],
                        "sum(x) over w",
                        "avg(x) over w",
                        "count(x) over w",
                        "count(y) over w"
                );
            }
        });
    }

    @Test
    public void testExplainOutputIsUnchanged() throws Exception {
        // A group is an internal decision about how the same rows are computed, and window plan
        // text is asserted across a large number of existing tests. Pinned here so a group line
        // cannot arrive unnoticed in either direction - and pinned at both settings of the kill
        // switch, so the two runs of the differential suite compare like with like and a plan
        // that quietly depended on the switch could not pass.
        assertMemoryLeak(() -> {
            createTable();
            assertQuery(sumAndCount())
                    .noLeakCheck()
                    .assertsPlan(SUM_AND_COUNT_PLAN);
            setProperty(PropertyKey.CAIRO_SQL_WINDOW_MAP_FUSION_ENABLED, "false");
            assertQuery(sumAndCount())
                    .noLeakCheck()
                    .assertsPlan(SUM_AND_COUNT_PLAN);
        });
    }

    @Test
    public void testExtremaOverEveryAdmittedStateTypeShareOneKey() throws Exception {
        // The four families in one group, and the two implementations behind them. max(x) and
        // min(y) keep a DOUBLE slot each and contribute on isFinite; max(l) and min(ts) keep a
        // raw 64-bit word each and contribute on their own type's null test - l through the
        // max(L) factory and ts through the timestamp one, which are separate classes over one
        // shared base. Eight slots would be four maps and four probes unfused; here they are
        // four slots behind one key.
        assertMemoryLeak(() -> {
            createTable();
            insertKeyShapes();
            final String sql = "select ts, max(x) over w, min(y) over w, max(l) over w, min(ts) over w "
                    + "from t " + WINDOW;
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = select(compiler, sql, sqlExecutionContext)) {
                final WindowRecordCursorFactory windowFactory = windowFactory(factory);
                assertBoundGroupCount(windowFactory, 1);
                final WindowMapState state = windowFactory.getWindowMapStates().getQuick(0);
                final WindowAccumulatorPlan plan = state.getPlan();
                Assert.assertEquals(4, plan.getComponentCount());
                Assert.assertEquals(4, plan.getProjectionCount());
                Assert.assertEquals(4, plan.getSlotCount());
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final long rows = drain(cursor);
                    Assert.assertEquals(KEY_SHAPE_ROW_COUNT, rows);
                }
            }
            assertFusedMatchesUnfused(
                    "max(x) over w",
                    "min(y) over w",
                    "max(l) over w",
                    "min(ts) over w"
            );
        });
    }

    @Test
    public void testManyKeysResizeTheMap() throws Exception {
        // Enough distinct keys to take the group's map through several rehashes, with each key
        // revisited so a rehash that lost or aliased an entry shows up as a wrong running value
        // rather than as a missing row.
        assertMemoryLeak(() -> {
            execute("create table t as (" +
                    "select (x * 1_000_000L)::timestamp as ts, " +
                    "('k' || (x % 5000))::symbol as k, " +
                    "case when x % 7 = 0 then null::double else x::double end as x, " +
                    "case when x % 11 = 0 then null::double else (x * 2)::double end as y " +
                    "from long_sequence(40_000)) timestamp(ts) partition by day");
            assertFusedMatchesUnfused("sum(x) over w", "count(y) over w");
        });
    }

    @Test
    public void testNullsAndInfinities() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            insertNullsAndInfinities();
            assertFusedMatchesUnfused("sum(x) over w", "count(y) over w");
        });
    }

    @Test
    public void testOrdinaryValues() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            insertOrdinaryRows();
            assertFusedMatchesUnfused("sum(x) over w", "count(y) over w");
        });
    }

    @Test
    public void testRepeatedCursorCyclesReleaseEveryByte() throws Exception {
        // The group's map is opened under the per-query tracker at of() and handed back at
        // close(), so ten cycles must net to zero on that counter - which is what
        // assertMemoryLeak around the loop asserts. A group freed only at factory close, or
        // reopened without being freed, fails here.
        assertMemoryLeak(() -> {
            createTable();
            insertOrdinaryRows();
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = select(compiler, sumAndCount(), sqlExecutionContext)) {
                assertBoundGroupCount(windowFactory(factory), 1);
                for (int i = 0; i < 10; i++) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.assertEquals("iteration " + i, ORDINARY_ROW_COUNT, drain(cursor));
                    }
                }
            }
        });
    }

    @Test
    public void testSumAndCountShareOneMap() throws Exception {
        // The headline shape. sum(x) counts finite x values and count(y) counts non-null y
        // values, so the two disagree on every row where exactly one is absent and keep
        // separate counters - what they share is the key domain and hash table.
        assertMemoryLeak(() -> {
            createTable();
            insertOrdinaryRows();
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = select(compiler, sumAndCount(), sqlExecutionContext)) {
                final WindowRecordCursorFactory windowFactory = windowFactory(factory);
                final ObjList<WindowMapState> states = windowFactory.getWindowMapStates();
                assertBoundGroupCount(windowFactory, 1);
                final WindowMapState state = states.getQuick(0);
                Assert.assertEquals(2, state.getPlan().getComponentCount());
                Assert.assertEquals(2, state.getPlan().getProjectionCount());
                // sum, its counter, and the count's own counter - three slots behind one key.
                Assert.assertEquals(3, state.getPlan().getSlotCount());
                Assert.assertEquals(0, state.getPlan().getSlotPrefix());
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final long rows = drain(cursor);
                    Assert.assertEquals(ORDINARY_ROW_COUNT, rows);
                    // Reported together on purpose: MapFactory selects on the key and the
                    // widened value against this limit, so neither number explains the choice
                    // on its own.
                    Assert.assertNotNull(state.getMapImplementation());
                    Assert.assertTrue(state.getUnorderedMapMaxEntrySize() > 0);
                }
            }
        });
    }

    @Test
    public void testSumAvgAndCountShareOneComponentAndOneArgumentEvaluation() throws Exception {
        // The acceptance shape of the whole design: three maps, three probes, three components,
        // five value slots, three updates and three evaluations of x a row become one of each -
        // except the projections, which are three reads of two slots and cost no state at all.
        assertMemoryLeak(() -> {
            createTable();
            insertKeyShapes();
            final String sql = "select ts, sum(x) over w, avg(x) over w, count(x) over w from t " + WINDOW;
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = select(compiler, sql, sqlExecutionContext)) {
                final WindowRecordCursorFactory windowFactory = windowFactory(factory);
                assertBoundGroupCount(windowFactory, 1);
                final WindowMapState state = windowFactory.getWindowMapStates().getQuick(0);
                final WindowAccumulatorPlan plan = state.getPlan();
                Assert.assertEquals(1, plan.getComponentCount());
                Assert.assertEquals(3, plan.getProjectionCount());
                Assert.assertEquals(2, plan.getSlotCount());
                // sum and avg read the component their own function would have kept; the count
                // reads a counter it no longer maintains, which is what the fold bought.
                Assert.assertFalse(plan.getProjection(0).isDerived());
                Assert.assertFalse(plan.getProjection(1).isDerived());
                Assert.assertTrue(plan.getProjection(2).isDerived());
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final long rows = drain(cursor);
                    Assert.assertEquals(KEY_SHAPE_ROW_COUNT, rows);
                }
            }
            assertFusedMatchesUnfused("sum(x) over w", "avg(x) over w", "count(x) over w");
        });
    }

    @Test
    public void testTheFourDispersionProjectionsShareOneComponent() throws Exception {
        // stddev_samp, stddev_pop, var_samp and var_pop are one Welford accumulator read four
        // ways, and count(x) reads the counter behind it. One three-slot component serves all
        // five, and the data carries the two partitions where the readings part company: one
        // with a single contributing row, where a sample dispersion is NULL and a population
        // one is 0, and one with no finite x at all.
        assertMemoryLeak(() -> {
            createTable();
            insertKeyShapes();
            final String sql = "select ts, stddev_samp(x) over w, stddev_pop(x) over w, "
                    + "var_samp(x) over w, var_pop(x) over w, count(x) over w from t " + WINDOW;
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = select(compiler, sql, sqlExecutionContext)) {
                final WindowRecordCursorFactory windowFactory = windowFactory(factory);
                assertBoundGroupCount(windowFactory, 1);
                final WindowMapState state = windowFactory.getWindowMapStates().getQuick(0);
                final WindowAccumulatorPlan plan = state.getPlan();
                Assert.assertEquals(1, plan.getComponentCount());
                Assert.assertEquals(5, plan.getProjectionCount());
                Assert.assertEquals(3, plan.getSlotCount());
                Assert.assertTrue(plan.getProjection(4).isDerived());
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final long rows = drain(cursor);
                    Assert.assertEquals(KEY_SHAPE_ROW_COUNT, rows);
                }
            }
            assertFusedMatchesUnfused(
                    "stddev_samp(x) over w",
                    "stddev_pop(x) over w",
                    "var_samp(x) over w",
                    "var_pop(x) over w",
                    "count(x) over w"
            );
        });
    }

    @Test
    public void testTheKillSwitchLeavesEveryFunctionOnItsOwnMap() throws Exception {
        // cairo.sql.window.map.fusion.enabled is the operational escape hatch, so what it turns
        // off has to be the whole runtime and nothing else: no group owns a map, every function
        // is back on its own, and the rows are the ones the fused run produced.
        assertMemoryLeak(() -> {
            createTable();
            insertOrdinaryRows();
            final String fused = render(sumAndCount());
            setProperty(PropertyKey.CAIRO_SQL_WINDOW_MAP_FUSION_ENABLED, "false");
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = select(compiler, sumAndCount(), sqlExecutionContext)) {
                final WindowRecordCursorFactory windowFactory = windowFactory(factory);
                // The compile is untouched: the group is still worked out and still has the
                // shape this build binds. The switch gates the binding, which is the only part
                // a query pays for.
                final ObjList<WindowAccumulatorPlan> plans = windowFactory.getWindowAccumulatorPlans();
                Assert.assertNotNull(plans);
                Assert.assertEquals(1, plans.size());
                Assert.assertEquals(2, plans.getQuick(0).getComponentCount());
                Assert.assertEquals(2, plans.getQuick(0).getProjectionCount());
                Assert.assertNull(windowFactory.getWindowMapStates());
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertEquals(ORDINARY_ROW_COUNT, drain(cursor));
                    final ObjList<WindowFunction> functions = windowFactory.getWindowFunctions();
                    Assert.assertEquals(2, functions.size());
                    for (int i = 0, n = functions.size(); i < n; i++) {
                        final WindowFunction function = functions.getQuick(i);
                        Assert.assertFalse("a function stayed bound with fusion off", function.isWindowStateOwned());
                        Assert.assertNotNull(function.getPartitionMap());
                        Assert.assertTrue(
                                "a function's own map never opened",
                                function.getPartitionMap().isOpen()
                        );
                    }
                }
            }
            // Same answers with the switch either way, which is the whole of its contract and
            // what makes running a suite twice a test rather than a comparison of two unknowns.
            Assert.assertEquals(fused, render(sumAndCount()));
        });
    }

    @Test
    public void testTheThreeCaptureFamiliesShareOneKey() throws Exception {
        // The acceptance shape for the capture families, and the negative control they need in
        // the same breath. first_value(x), first_value(x) ignore nulls and last_value(x) ignore
        // nulls over one window read one column under one key and keep three components, because
        // they capture three different rows: the partition's first, its first finite one, and its
        // most recent finite one. Two of them are two slots and one is one - the IGNORE NULLS
        // first value needs no flag, since it only ever writes a value its own predicate admits
        // and so reads its emptiness off the slot.
        //
        // The data is the capture-shape one, whose partition 'a' opens on an absent value and
        // carries a finite one after it. That is the partition the flag exists for: a
        // respect-nulls capture has to answer NULL for every one of its rows, where a flagless
        // reading would take it as empty and capture the 5.0 behind it.
        assertMemoryLeak(() -> {
            createTable();
            insertCaptureShapes();
            final String sql = "select ts, first_value(x) over w, first_value(x) ignore nulls over w, "
                    + "last_value(x) ignore nulls over w from t " + WINDOW;
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = select(compiler, sql, sqlExecutionContext)) {
                final WindowRecordCursorFactory windowFactory = windowFactory(factory);
                assertBoundGroupCount(windowFactory, 1);
                final WindowMapState state = windowFactory.getWindowMapStates().getQuick(0);
                final WindowAccumulatorPlan plan = state.getPlan();
                Assert.assertEquals(3, plan.getComponentCount());
                Assert.assertEquals(3, plan.getProjectionCount());
                // [value, captured] then [value] then [value, captured], in family order.
                Assert.assertEquals(5, plan.getSlotCount());
                Assert.assertEquals(0, plan.getComponentSlotBase(0));
                Assert.assertEquals(2, plan.getComponentSlotBase(1));
                Assert.assertEquals(3, plan.getComponentSlotBase(2));
                Assert.assertEquals(
                        WindowAccumulatorDescriptor.FAMILY_DOUBLE_FIRST_VALUE,
                        plan.getComponent(0).getFamily()
                );
                Assert.assertEquals(
                        WindowAccumulatorDescriptor.FAMILY_DOUBLE_FIRST_NOT_NULL_VALUE,
                        plan.getComponent(1).getFamily()
                );
                Assert.assertEquals(
                        WindowAccumulatorDescriptor.FAMILY_DOUBLE_LAST_NOT_NULL_VALUE,
                        plan.getComponent(2).getFamily()
                );
                // Nothing is derived and nothing folds: a captured value is one row's own, so no
                // capture is a run inside another however alike the two slices look.
                for (int i = 0; i < 3; i++) {
                    Assert.assertFalse(plan.getProjection(i).isDerived());
                    Assert.assertEquals(-1, plan.getProjection(i).getNonNullCountSlot());
                }
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final long rows = drain(cursor);
                    Assert.assertEquals(CAPTURE_SHAPE_ROW_COUNT, rows);
                }
            }
            // The differential, and then the one answer it is worth naming outright: partition
            // 'a' opens on an absent value, so its respect-nulls capture is NULL for all four of
            // its rows while the IGNORE NULLS one is 5.0 from the second row on.
            assertFusedMatchesUnfused(
                    "first_value(x) over w",
                    "first_value(x) ignore nulls over w",
                    "last_value(x) ignore nulls over w"
            );
            Assert.assertEquals(
                    "ts\tfirst_value\tfirst_value_ignore_nulls\n"
                            + "2024-01-01T00:00:00.000000Z\tnull\tnull\n"
                            + "2024-01-01T00:00:01.000000Z\tnull\t5.0\n"
                            + "2024-01-01T00:00:02.000000Z\tnull\t5.0\n"
                            + "2024-01-01T00:00:03.000000Z\tnull\t5.0\n"
                            + "2024-01-01T00:00:04.000000Z\tnull\tnull\n"
                            + "2024-01-01T00:00:05.000000Z\tnull\t2.5\n"
                            + "2024-01-01T00:00:06.000000Z\t3.0\t3.0\n"
                            + "2024-01-01T00:00:07.000000Z\t7.0\t7.0\n"
                            + "2024-01-01T00:00:08.000000Z\t7.0\t7.0\n",
                    render("select ts, first_value(x) over w, first_value(x) ignore nulls over w from t "
                            + WINDOW)
            );
        });
    }

    @Test
    public void testToTopRestartsTheSharedKeyDomain() throws Exception {
        // toTop clears the group once - the cursor loops over groups, not over the bound
        // functions that read them, and a bound function's own toTop deliberately leaves the
        // shared state alone. A domain not cleared would carry the first pass's running totals
        // into the second; one cleared per member would be the same clear twice.
        assertMemoryLeak(() -> {
            createTable();
            insertOrdinaryRows();
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = select(compiler, sumAndCount(), sqlExecutionContext)) {
                final WindowRecordCursorFactory windowFactory = windowFactory(factory);
                assertBoundGroupCount(windowFactory, 1);
                final WindowMapState state = windowFactory.getWindowMapStates().getQuick(0);
                final StringSink first = new StringSink();
                final StringSink second = new StringSink();
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    CursorPrinter.println(cursor, factory.getMetadata(), first, true, false);
                    cursor.toTop();
                    CursorPrinter.println(cursor, factory.getMetadata(), second, true, false);
                }
                TestUtils.assertEquals(first, second);
            }
        });
    }

    @Test
    public void testTwoCountersBindOnBothSidesOfTheEntryLimit() throws Exception {
        // The shape a Map-implementation decline rule used to refuse, walked at both settings
        // that ship. count(x) and count(y) over one SYMBOL key are 4 + 8 = 12 each, so on their
        // own each takes an Unordered4Map, while the fused value is two counters at 4 + 16 = 20:
        // at the 16-byte limit DefaultCairoConfiguration returns, the group trades two narrow
        // maps for one OrderedMap, and at the 32 a server defaults to it keeps the Unordered4Map
        // its members had. It binds at both. The trade the smaller limit makes is the one the
        // rule was written against and it measured a win - 65.2 ns/row fused against 132.2
        // unfused over 1e6 keys, and 33.2 against 34.7 over 1e3 - because Unordered4Map is the
        // faster map only while the key domain is small.
        assertMemoryLeak(() -> {
            createTable();
            insertOrdinaryRows();
            final String sql = "select ts, count(x) over w, count(y) over w from t " + WINDOW;
            setProperty(PropertyKey.CAIRO_SQL_UNORDERED_MAP_MAX_ENTRY_SIZE, 16);
            final String ordered = assertBoundMapImplementation(sql, "OrderedMap", 16);
            setProperty(PropertyKey.CAIRO_SQL_UNORDERED_MAP_MAX_ENTRY_SIZE, 32);
            final String unordered = assertBoundMapImplementation(sql, "Unordered4Map", 32);
            // The map the value width selects is a physical choice and nothing else, which is
            // what says the limit may be moved for performance without moving an answer.
            Assert.assertEquals(ordered, unordered);
            // And both are the answer the two counters produce on maps of their own.
            assertFusedMatchesUnfused("count(x) over w", "count(y) over w");
        });
    }

    @Test
    public void testTwoWindowsGetTwoGroupsAndShareNothing() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            insertOrdinaryRows();
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = select(compiler, twoWindows(), sqlExecutionContext)) {
                final WindowRecordCursorFactory windowFactory = windowFactory(factory);
                assertBoundGroupCount(windowFactory, 2);
                final ObjList<WindowMapState> states = windowFactory.getWindowMapStates();
                Assert.assertFalse(
                        states.getQuick(0).getPlan().getSpec().isSameSpec(states.getQuick(1).getPlan().getSpec())
                );
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final long rows = drain(cursor);
                    // Co-location is per window and never across windows.
                }
            }
        });
    }

    private static void assertBoundGroupCount(WindowRecordCursorFactory factory, int expected) {
        final ObjList<WindowMapState> states = factory.getWindowMapStates();
        Assert.assertNotNull("no window Map group was bound", states);
        Assert.assertEquals(expected, states.size());
    }

    /**
     * Compiles {@code sql} over the ordinary rows, requires one bound group holding the named
     * {@link io.questdb.cairo.map.Map} implementation under the given configured entry-size
     * limit, and returns the rows it produced.
     * <p>
     * The two numbers are asserted together because {@code MapFactory} selects on the key and
     * the widened value against that limit, so neither explains the choice on its own - and a
     * limit that stopped being read would otherwise leave the implementation assertion passing
     * for the wrong reason.
     */
    private static String assertBoundMapImplementation(
            String sql,
            String mapImplementation,
            int maxEntrySize
    ) throws SqlException {
        final StringSink localSink = new StringSink();
        try (SqlCompiler compiler = engine.getSqlCompiler();
             RecordCursorFactory factory = select(compiler, sql, sqlExecutionContext)) {
            final WindowRecordCursorFactory windowFactory = windowFactory(factory);
            assertBoundGroupCount(windowFactory, 1);
            final WindowMapState state = windowFactory.getWindowMapStates().getQuick(0);
            Assert.assertEquals(maxEntrySize, state.getUnorderedMapMaxEntrySize());
            Assert.assertEquals(mapImplementation, state.getMapImplementation());
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                CursorPrinter.println(cursor, factory.getMetadata(), localSink, true, false);
            }
        }
        return localSink.toString();
    }

    /**
     * Requires the fused query to produce, row for row, what the same outputs produce one at a
     * time.
     * <p>
     * The references are genuinely the unfused path rather than a second fused one: a window
     * carrying a single fusible function forms no group at all - moving a map is not removing
     * one - so each of them runs exactly the per-function map and per-function probe this
     * build had before the group existed. That holds for a merged shape too: asked for on its
     * own, a {@code count(x)} that reads a {@code sum(x)}'s counter in the fused query keeps
     * and maintains a counter of its own here.
     */
    private static void assertFusedMatchesUnfused(String... outputs) throws SqlException {
        assertFusedMatchesUnfusedOn("t", outputs);
    }

    /**
     * The same comparison over {@code table}, which carries the same {@code ts} and {@code k}
     * columns {@link #WINDOW} names and whatever value columns the case is about.
     */
    private static void assertFusedMatchesUnfusedOn(String table, String... outputs) throws SqlException {
        assertFusedMatchesUnfusedOnWindow(table, WINDOW, outputs);
    }

    /**
     * The same comparison over {@code table} and an arbitrary {@code window} clause naming
     * {@code w}, which is what the bounded-ROWS cases need: the frame is part of the group's
     * identity, so a geometry is a different group and deserves its own comparison.
     */
    private static void assertFusedMatchesUnfusedOnWindow(
            String table,
            String window,
            String... outputs
    ) throws SqlException {
        final StringBuilder fused = new StringBuilder("select ts");
        for (int i = 0; i < outputs.length; i++) {
            fused.append(", ").append(outputs[i]);
        }
        fused.append(" from ").append(table).append(' ').append(window);
        assertIsBound(fused.toString(), true);
        final String[] references = new String[outputs.length];
        for (int i = 0; i < outputs.length; i++) {
            final String reference = "select ts, " + outputs[i] + " from " + table + " " + window;
            assertIsBound(reference, false);
            references[i] = body(render(reference));
        }
        final String expected = zipLastColumns(references);
        // A comparison of two empty renderings would pass and prove nothing, and every way the
        // helpers above could go wrong ends in one.
        Assert.assertFalse("the references produced no rows", expected.trim().isEmpty());
        Assert.assertEquals(expected, body(render(fused.toString())));
    }

    /**
     * Whether compiling {@code sql} binds a group at all, which is a lower bound and
     * deliberately not a count: what a shape fuses into is this class's other cases' subject.
     * <p>
     * Package-visible for the {@code *FusionDisabled} runs, which are copies of another suite
     * at the other setting and so have to prove both halves of the differential they are - that
     * the switch leaves their compiles unbound, and that the run they copy binds something.
     * Unbound covers both spellings of it, a null list and an empty one, because the switch
     * leaves the plan compiled and unbound rather than absent.
     */
    static void assertIsBound(String sql, boolean bound) throws SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler();
             RecordCursorFactory factory = select(compiler, sql, sqlExecutionContext)) {
            final ObjList<WindowMapState> states = windowFactory(factory).getWindowMapStates();
            Assert.assertEquals(sql, bound, states != null && states.size() > 0);
        }
    }

    /**
     * Everything after the header. The queries carry different column aliases - {@code count}
     * alone versus {@code count} beside {@code count1} - and the header is not what the
     * comparison is about.
     * <p>
     * Package-visible: {@link CachedWindowMapFusionTest} drives the same comparison over the
     * cached cursors and reads the same shape out of the rendered result.
     */
    static String body(String rendered) {
        final int lineEnd = rendered.indexOf('\n');
        return lineEnd < 0 ? "" : rendered.substring(lineEnd + 1);
    }

    /**
     * One row's worth of DECIMAL literals, each cast to the width of the column it goes in -
     * a numeric literal is a DOUBLE and does not convert on its own.
     */
    private static String decimals(String d8, String d16, String d32, String d64, String d128, String d256) {
        return d8 + "::decimal(2, 1), "
                + d16 + "::decimal(4, 1), "
                + d32 + "::decimal(9, 3), "
                + d64 + "::decimal(18, 2), "
                + d128 + "::decimal(38, 6), "
                + d256 + "::decimal(60, 0)";
    }

    static long drain(RecordCursor cursor) {
        long rows = 0;
        while (cursor.hasNext()) {
            rows++;
        }
        return rows;
    }

    static String render(String sql) throws SqlException {
        final StringSink localSink = new StringSink();
        printSql(sql, localSink);
        return localSink.toString();
    }

    private static String sumAndCount() {
        return "select ts, sum(x) over w, count(y) over w from t " + WINDOW;
    }

    private static String twoWindows() {
        return "select ts, sum(x) over w, count(y) over w, sum(x) over w2, count(y) over w2 from t "
                + "window w as (partition by k order by ts rows between unbounded preceding and current row), "
                + "w2 as (partition by k2 order by ts rows between unbounded preceding and current row)";
    }

    private static WindowRecordCursorFactory windowFactory(RecordCursorFactory factory) {
        // A projection wrapper can sit above the window factory, so the search walks the whole
        // chain rather than unwrapping one known level.
        RecordCursorFactory root = factory;
        while (root != null && !(root instanceof WindowRecordCursorFactory)) {
            root = root.getBaseFactory();
        }
        Assert.assertNotNull("no window factory in the tree", root);
        return (WindowRecordCursorFactory) root;
    }

    /**
     * Glues the last column of every reference onto the first one's rows, which is the fused
     * query's row when they all carry the same leading columns - asserted here rather than
     * assumed, since a mismatch there would silently weaken every comparison.
     */
    static String zipLastColumns(String[] bodies) {
        final String[][] rows = new String[bodies.length][];
        for (int i = 0; i < bodies.length; i++) {
            rows[i] = bodies[i].split("\n", -1);
            Assert.assertEquals("reference row counts differ", rows[0].length, rows[i].length);
        }
        final StringBuilder out = new StringBuilder();
        for (int r = 0; r < rows[0].length; r++) {
            if (r > 0) {
                out.append('\n');
            }
            final int split = rows[0][r].lastIndexOf('\t');
            if (rows[0][r].isEmpty()) {
                for (int i = 1; i < rows.length; i++) {
                    Assert.assertTrue(rows[i][r].isEmpty());
                }
                continue;
            }
            out.append(rows[0][r]);
            for (int i = 1; i < rows.length; i++) {
                final int otherSplit = rows[i][r].lastIndexOf('\t');
                Assert.assertEquals(
                        "reference rows are not aligned",
                        rows[0][r].substring(0, split),
                        rows[i][r].substring(0, otherSplit)
                );
                out.append('\t').append(rows[i][r], otherSplit + 1, rows[i][r].length());
            }
        }
        return out.toString();
    }

    /**
     * One column per DECIMAL width, because a DECIMAL extremum's state is its argument's own
     * payload: {@code d8} through {@code d64} land in a LONG slot and {@code d128} and
     * {@code d256} in a slot of their own type. Kept apart from {@code t} rather than added to
     * it - the widths are six columns and every case above would have to carry them.
     */
    private void createDecimalTable() throws SqlException {
        execute("create table td (ts timestamp, k symbol, d8 decimal(2, 1), d16 decimal(4, 1), "
                + "d32 decimal(9, 3), d64 decimal(18, 2), d128 decimal(38, 6), d256 decimal(60, 0)) "
                + "timestamp(ts) partition by day");
    }

    private void createTable() throws SqlException {
        // l is a LONG so that the extremum families can be reached at both of the state types
        // they are split by - the DOUBLE one through x and y, the 64-bit one through l and
        // through ts, which are two separate implementations.
        execute("create table t (ts timestamp, k symbol, k2 symbol, x double, y double, l long) "
                + "timestamp(ts) partition by day");
    }

    /**
     * The partition shapes the capture families part company on, which neither
     * {@link #insertKeyShapes()} nor {@link #insertDecimalKeyShapes()} makes: a partition whose
     * <b>first</b> row is absent and whose second is not - where a respect-nulls capture emits
     * NULL for every row and an IGNORE NULLS one emits the second row's value - one that opens
     * on an infinity, one of a single row, and a NULL key. The {@code l} column carries the same
     * shape at the 64-bit state width.
     */
    private void insertCaptureShapes() throws SqlException {
        execute("insert into t values " +
                "('2024-01-01T00:00:00.000000Z', 'a', 'p', null, 1.0, null), " +
                "('2024-01-01T00:00:01.000000Z', 'a', 'q', 5.0, 2.0, 5), " +
                "('2024-01-01T00:00:02.000000Z', 'a', 'p', null, 3.0, null), " +
                "('2024-01-01T00:00:03.000000Z', 'a', 'q', '-Infinity'::double, 4.0, -1), " +
                "('2024-01-01T00:00:04.000000Z', 'b', 'p', '-Infinity'::double, 5.0, null), " +
                "('2024-01-01T00:00:05.000000Z', 'b', 'q', 2.5, 6.0, 2), " +
                "('2024-01-01T00:00:06.000000Z', 'c', 'p', 3.0, 7.0, 3), " +
                "('2024-01-01T00:00:07.000000Z', null, 'q', 7.0, 8.0, null), " +
                "('2024-01-01T00:00:08.000000Z', null, 'p', null, 9.0, 9)");
    }

    /**
     * The partition shapes a DECIMAL extremum parts company on: a NULL key; a partition of one
     * row; and one - {@code 'nn'} - whose decimals are absent on every row, so it has rows and
     * no value either direction contributes, which is the state that has to read back as this
     * width's own NULL rather than as a zero.
     */
    private void insertDecimalKeyShapes() throws SqlException {
        execute("insert into td values " +
                "('2024-01-01T00:00:00.000000Z', 'a', " + decimals("1", "10", "10", "10", "10", "10") + "), " +
                "('2024-01-01T00:00:01.000000Z', null, " + decimals("2", "20", "20", "20", "20", "20") + "), " +
                "('2024-01-01T00:00:02.000000Z', 'a', null, null, null, null, null, null), " +
                "('2024-01-01T00:00:03.000000Z', null, " + decimals("-3", "-30", "-30", "-30", "-30", "-30") + "), " +
                "('2024-01-01T00:00:04.000000Z', 'one', " + decimals("5", "50", "50", "50", "50", "50") + "), " +
                "('2024-01-01T00:00:05.000000Z', 'nn', null, null, null, null, null, null), " +
                "('2024-01-01T00:00:06.000000Z', 'nn', null, null, null, null, null, null), " +
                "('2024-01-01T00:00:07.000000Z', 'a', " + decimals("-1", "-1", "-1", "-1", "-1", "-1") + "), " +
                "('2024-01-01T00:00:08.000000Z', null, " + decimals("9", "90", "90", "90", "90", "90") + ")");
    }

    /**
     * The partition shapes the merged families part company on: a NULL key, whose
     * {@code count(k)} is zero while its row count is not; a partition of one row, where a
     * sample dispersion is NULL and a population one is 0; and one whose only non-null
     * {@code x} is an infinity, so it has rows, a non-null count and no finite value.
     */
    private void insertKeyShapes() throws SqlException {
        execute("insert into t values " +
                "('2024-01-01T00:00:00.000000Z', 'a', 'p', 1.0, 10.0, 5), " +
                "('2024-01-01T00:00:01.000000Z', null, 'p', 2.0, 20.0, -3), " +
                "('2024-01-01T00:00:02.000000Z', 'a', 'q', 4.0, null, null), " +
                "('2024-01-01T00:00:03.000000Z', null, 'q', null, 40.0, 7), " +
                "('2024-01-01T00:00:04.000000Z', 'one', 'p', 5.0, 50.0, null), " +
                "('2024-01-01T00:00:05.000000Z', 'nx', 'q', null, 60.0, 0), " +
                "('2024-01-01T00:00:06.000000Z', 'nx', 'p', 'Infinity'::double, 70.0, -9), " +
                "('2024-01-01T00:00:07.000000Z', 'a', 'q', 8.0, 80.0, 2), " +
                "('2024-01-01T00:00:08.000000Z', null, 'p', 9.0, null, null)");
    }

    private void insertNullsAndInfinities() throws SqlException {
        // sum contributes on Numbers.isFinite and count(y) on a null test, so an infinity is a
        // row the two disagree about in a way no NULL reproduces.
        execute("insert into t values " +
                "('2024-01-01T00:00:00.000000Z', 'a', 'p', 1.0, 1.0, 1), " +
                "('2024-01-01T00:00:01.000000Z', 'a', 'p', null, null, null), " +
                "('2024-01-01T00:00:02.000000Z', 'a', 'q', 'Infinity'::double, 2.0, -2), " +
                "('2024-01-01T00:00:03.000000Z', 'b', 'q', '-Infinity'::double, null, null), " +
                "('2024-01-01T00:00:04.000000Z', 'b', 'p', 2.5, 3.0, 3), " +
                "('2024-01-01T00:00:05.000000Z', 'b', 'p', null, 4.0, -4), " +
                "('2024-01-01T00:00:06.000000Z', 'a', 'q', -3.5, null, 0)");
    }

    private void insertOrdinaryRows() throws SqlException {
        execute("insert into t values " +
                "('2024-01-01T00:00:00.000000Z', 'a', 'p', 1.0, 10.0, 100), " +
                "('2024-01-01T00:00:01.000000Z', 'b', 'p', 2.0, 20.0, -200), " +
                "('2024-01-01T00:00:02.000000Z', 'a', 'q', 3.0, 30.0, 300), " +
                "('2024-01-01T00:00:03.000000Z', 'c', 'q', 4.0, 40.0, -400), " +
                "('2024-01-01T00:00:04.000000Z', 'a', 'p', 5.0, 50.0, 500), " +
                "('2024-01-01T00:00:05.000000Z', 'b', 'q', 6.0, 60.0, -600), " +
                "('2024-01-01T00:00:06.000000Z', 'c', 'p', 7.0, 70.0, 700), " +
                "('2024-01-01T00:00:07.000000Z', 'a', 'q', 8.0, 80.0, -800), " +
                "('2024-01-01T00:00:08.000000Z', 'b', 'p', 9.0, 90.0, 900)");
    }
}
