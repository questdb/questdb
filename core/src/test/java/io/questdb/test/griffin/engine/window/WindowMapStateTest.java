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
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
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

    private static final int KEY_SHAPE_ROW_COUNT = 9;
    private static final int ORDINARY_ROW_COUNT = 9;
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
                    Assert.assertEquals(rows, state.getLookupCount());
                    // Two accumulators, four outputs: x is read once per component and not
                    // once per call.
                    Assert.assertEquals(2 * rows, state.getContributorUpdateCount());
                    Assert.assertEquals(4 * rows, state.getProjectionWriteCount());
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
                    Assert.assertEquals(ORDINARY_ROW_COUNT, state.getLookupCount());
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
                    Assert.assertEquals(rows, state.getLookupCount());
                    Assert.assertEquals(rows, state.getContributorUpdateCount());
                    Assert.assertEquals(3 * rows, state.getProjectionWriteCount());
                }
            }
            assertFusedMatchesUnfused("count(*) over w", "row_number() over w", "count(k) over w");
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
    public void testManyKeysResizeTheMap() throws Exception {
        // Enough distinct keys to take the group's map through several rehashes, with each key
        // revisited so a rehash that lost or aliased an entry shows up as a wrong running value
        // rather than as a missing row.
        assertMemoryLeak(() -> {
            execute("create table t as (" +
                    "select (x * 1000000L)::timestamp as ts, " +
                    "('k' || (x % 5000))::symbol as k, " +
                    "case when x % 7 = 0 then null::double else x::double end as x, " +
                    "case when x % 11 = 0 then null::double else (x * 2)::double end as y " +
                    "from long_sequence(40000)) timestamp(ts) partition by day");
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
                    Assert.assertEquals(rows, state.getLookupCount());
                    // One update a row for three outputs - and one evaluation of x with it.
                    // accumulateWindowState is the only place any of these families reads its
                    // argument, and only the component's one contributor is asked to run it,
                    // so the update count is the argument-evaluation count for this shape.
                    Assert.assertEquals(rows, state.getContributorUpdateCount());
                    Assert.assertEquals(3 * rows, state.getProjectionWriteCount());
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
                    Assert.assertEquals(rows, state.getLookupCount());
                    Assert.assertEquals(rows, state.getContributorUpdateCount());
                    Assert.assertEquals(5 * rows, state.getProjectionWriteCount());
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
                    Assert.assertEquals(rows, state.getLookupCount());
                    // One accumulator for two outputs, so x is read and compensated once.
                    Assert.assertEquals(rows, state.getContributorUpdateCount());
                    Assert.assertEquals(2 * rows, state.getProjectionWriteCount());
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
                    Assert.assertEquals(rows, state.getLookupCount());
                    Assert.assertEquals(2 * rows, state.getContributorUpdateCount());
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
                    Assert.assertEquals(rows, state.getLookupCount());
                    Assert.assertEquals(2 * rows, state.getContributorUpdateCount());
                    Assert.assertEquals(2 * rows, state.getProjectionWriteCount());
                }
            }
            assertFusedMatchesUnfused("max(x) over w", "min(x) over w");
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
                    Assert.assertEquals(rows, state.getLookupCount());
                    // Two accumulators for three outputs: x is read once per component.
                    Assert.assertEquals(2 * rows, state.getContributorUpdateCount());
                    Assert.assertEquals(3 * rows, state.getProjectionWriteCount());
                }
            }
            assertFusedMatchesUnfused("sum(x) over w", "max(x) over w", "count(x) over w");
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
                    Assert.assertEquals(rows, state.getLookupCount());
                    Assert.assertEquals(4 * rows, state.getContributorUpdateCount());
                    Assert.assertEquals(4 * rows, state.getProjectionWriteCount());
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
    public void testSumAndCountShareOneMapAndOneLookup() throws Exception {
        // The headline shape. sum(x) counts finite x values and count(y) counts non-null y
        // values, so the two disagree on every row where exactly one is absent and keep
        // separate counters - what they share is the key domain, the hash table and the row's
        // one lookup.
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
                    // One lookup per row for both outputs, and one update per component.
                    Assert.assertEquals(rows, state.getLookupCount());
                    Assert.assertEquals(2 * rows, state.getContributorUpdateCount());
                    Assert.assertEquals(2 * rows, state.getProjectionWriteCount());
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
                    Assert.assertEquals(ORDINARY_ROW_COUNT, state.getLookupCount());
                    cursor.toTop();
                    Assert.assertEquals(0, state.getLookupCount());
                    CursorPrinter.println(cursor, factory.getMetadata(), second, true, false);
                    Assert.assertEquals(ORDINARY_ROW_COUNT, state.getLookupCount());
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
                    // Two key domains, two lookups a row: co-location is per window and never
                    // across windows.
                    Assert.assertEquals(rows, states.getQuick(0).getLookupCount());
                    Assert.assertEquals(rows, states.getQuick(1).getLookupCount());
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
                Assert.assertEquals(ORDINARY_ROW_COUNT, state.getLookupCount());
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
        final StringBuilder fused = new StringBuilder("select ts");
        for (int i = 0; i < outputs.length; i++) {
            fused.append(", ").append(outputs[i]);
        }
        fused.append(" from t ").append(WINDOW);
        assertIsBound(fused.toString(), true);
        final String[] references = new String[outputs.length];
        for (int i = 0; i < outputs.length; i++) {
            final String reference = "select ts, " + outputs[i] + " from t " + WINDOW;
            assertIsBound(reference, false);
            references[i] = body(render(reference));
        }
        final String expected = zipLastColumns(references);
        // A comparison of two empty renderings would pass and prove nothing, and every way the
        // helpers above could go wrong ends in one.
        Assert.assertFalse("the references produced no rows", expected.trim().isEmpty());
        Assert.assertEquals(expected, body(render(fused.toString())));
    }

    private static void assertIsBound(String sql, boolean bound) throws SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler();
             RecordCursorFactory factory = select(compiler, sql, sqlExecutionContext)) {
            final ObjList<WindowMapState> states = windowFactory(factory).getWindowMapStates();
            Assert.assertEquals(sql, bound, states != null && states.size() > 0);
        }
    }

    /**
     * Everything after the header. The three queries carry different column aliases -
     * {@code count} alone versus {@code count} beside {@code count1} - and the header is not
     * what the comparison is about.
     */
    private static String body(String rendered) {
        final int lineEnd = rendered.indexOf('\n');
        return lineEnd < 0 ? "" : rendered.substring(lineEnd + 1);
    }

    private static long drain(RecordCursor cursor) {
        long rows = 0;
        while (cursor.hasNext()) {
            rows++;
        }
        return rows;
    }

    private static String render(String sql) throws SqlException {
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
    private static String zipLastColumns(String[] bodies) {
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

    private void createTable() throws SqlException {
        // l is a LONG so that the extremum families can be reached at both of the state types
        // they are split by - the DOUBLE one through x and y, the 64-bit one through l and
        // through ts, which are two separate implementations.
        execute("create table t (ts timestamp, k symbol, k2 symbol, x double, y double, l long) "
                + "timestamp(ts) partition by day");
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
