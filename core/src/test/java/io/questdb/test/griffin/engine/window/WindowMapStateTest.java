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
 * What this build binds is physical co-location alone - a group whose plan lays out one
 * accumulator component per output. Every member still keeps a state slice of its own,
 * maintains it itself and reads it back itself, so the answers are the unfused ones by
 * construction and a difference could only be the co-location's doing. That is what the
 * differential cases below check, and they check it against a reference this tree already
 * runs: a window carrying <b>one</b> fusible function forms no group, so the same output
 * asked for on its own is the unfused path.
 * <p>
 * The group's compile-time decisions - which functions join, in what order, sharing what -
 * are {@code WindowAccumulatorPlanTest}'s and are not repeated here.
 */
public class WindowMapStateTest extends AbstractCairoTest {

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
    public void testAMergedGroupIsCompiledButNotBound() throws Exception {
        // sum(x) + avg(x) + count(x) is one component and three outputs. The merge and the
        // fold are proved and a live view runs them, but they are not what this slice binds:
        // an output that changed when the group bound would then have two suspects instead of
        // one. The plan is compiled all the same, so the next step turns it on rather than
        // discovering it.
        assertMemoryLeak(() -> {
            createTable();
            insertOrdinaryRows();
            final String sql = "select ts, sum(x) over w, avg(x) over w, count(x) over w from t " + WINDOW;
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = select(compiler, sql, sqlExecutionContext)) {
                final WindowRecordCursorFactory windowFactory = windowFactory(factory);
                final ObjList<WindowAccumulatorPlan> plans = windowFactory.getWindowAccumulatorPlans();
                Assert.assertNotNull(plans);
                Assert.assertEquals(1, plans.size());
                Assert.assertEquals(1, plans.getQuick(0).getComponentCount());
                Assert.assertEquals(3, plans.getQuick(0).getProjectionCount());
                Assert.assertFalse(WindowMapState.isPhysicalCoLocationOnly(plans.getQuick(0)));
                Assert.assertNull(windowFactory.getWindowMapStates());
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertEquals(ORDINARY_ROW_COUNT, drain(cursor));
                }
            }
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
    public void testAGroupWhoseMemberIsAlreadyOrderedStillBinds() throws Exception {
        // The other half of the Map-implementation rule. sum(x)'s own [DOUBLE, LONG] value is
        // 4 + 16 = 20 against a 16-byte limit, so its private map is an OrderedMap before any
        // fusion; co-locating count(y)'s counter beside it removes a map without changing the
        // implementation of the one left. Nothing is traded away, so nothing declines.
        assertMemoryLeak(() -> {
            createTable();
            insertOrdinaryRows();
            setProperty(PropertyKey.CAIRO_SQL_UNORDERED_MAP_MAX_ENTRY_SIZE, 16);
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = select(compiler, sumAndCount(), sqlExecutionContext)) {
                final WindowRecordCursorFactory windowFactory = windowFactory(factory);
                assertBoundGroupCount(windowFactory, 1);
                final WindowMapState state = windowFactory.getWindowMapStates().getQuick(0);
                Assert.assertFalse(WindowMapState.declinesForMapImplementation(configuration, state.getPlan()));
                Assert.assertEquals(16, state.getUnorderedMapMaxEntrySize());
                Assert.assertEquals("OrderedMap", state.getMapImplementation());
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertEquals(ORDINARY_ROW_COUNT, drain(cursor));
                    Assert.assertEquals(ORDINARY_ROW_COUNT, state.getLookupCount());
                }
            }
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
                    "('2024-01-01T00:00:00.000000Z', 'nk', 'p', null, 1.0), " +
                    "('2024-01-01T00:00:01.000000Z', null, 'p', 2.0, null), " +
                    "('2024-01-01T00:00:02.000000Z', 'nk', 'q', null, null), " +
                    "('2024-01-01T00:00:03.000000Z', null, 'q', 3.0, 4.0), " +
                    "('2024-01-01T00:00:04.000000Z', 'nk', 'p', null, 5.0), " +
                    "('2024-01-01T00:00:05.000000Z', null, 'p', null, 6.0)");
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
                Assert.assertTrue(WindowMapState.isPhysicalCoLocationOnly(plans.getQuick(0)));
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
    public void testTwoCountersDeclineWhenFusionWouldCrossTheEntryLimit() throws Exception {
        // The shape the Map-implementation rule exists for, asserted at both settings that ship:
        // count(x) and count(y) over one SYMBOL key are 4 + 8 = 12 each, so each takes an
        // Unordered4Map, while the fused value is two counters at 4 + 16 = 20. At the 16-byte
        // limit DefaultCairoConfiguration returns that entry is an OrderedMap and the group
        // declines; at the 32 a server defaults to it is the same Unordered4Map its members had
        // and the group binds. The answers do not move with it.
        assertMemoryLeak(() -> {
            createTable();
            insertOrdinaryRows();
            final String sql = "select ts, count(x) over w, count(y) over w from t " + WINDOW;
            setProperty(PropertyKey.CAIRO_SQL_UNORDERED_MAP_MAX_ENTRY_SIZE, 16);
            final String declined = render(sql);
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = select(compiler, sql, sqlExecutionContext)) {
                final WindowRecordCursorFactory windowFactory = windowFactory(factory);
                // Compiled, and compiled as the shape this build binds - so what declined it is
                // the Map-implementation rule and not the merge rule that leaves
                // sum(x) + avg(x) + count(x) unbound.
                final ObjList<WindowAccumulatorPlan> plans = windowFactory.getWindowAccumulatorPlans();
                Assert.assertNotNull(plans);
                Assert.assertEquals(1, plans.size());
                Assert.assertTrue(WindowMapState.isPhysicalCoLocationOnly(plans.getQuick(0)));
                Assert.assertTrue(WindowMapState.declinesForMapImplementation(configuration, plans.getQuick(0)));
                Assert.assertNull(windowFactory.getWindowMapStates());
            }
            setProperty(PropertyKey.CAIRO_SQL_UNORDERED_MAP_MAX_ENTRY_SIZE, 32);
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = select(compiler, sql, sqlExecutionContext)) {
                final WindowRecordCursorFactory windowFactory = windowFactory(factory);
                assertBoundGroupCount(windowFactory, 1);
                final WindowMapState state = windowFactory.getWindowMapStates().getQuick(0);
                Assert.assertFalse(WindowMapState.declinesForMapImplementation(configuration, state.getPlan()));
                Assert.assertEquals(32, state.getUnorderedMapMaxEntrySize());
                // The rule's prediction against what MapFactory actually built: the two answer
                // the same question through the same code, and this is where that shows.
                Assert.assertEquals("Unordered4Map", state.getMapImplementation());
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertEquals(ORDINARY_ROW_COUNT, drain(cursor));
                    Assert.assertEquals(ORDINARY_ROW_COUNT, state.getLookupCount());
                }
            }
            Assert.assertEquals(declined, render(sql));
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
     * Requires the fused two-output query to produce, row for row, what the same two outputs
     * produce one at a time.
     * <p>
     * The references are genuinely the unfused path rather than a second fused one: a window
     * carrying a single fusible function forms no group at all - moving a map is not removing
     * one - so each of them runs exactly the per-function map and per-function probe this
     * build had before the group existed.
     */
    private static void assertFusedMatchesUnfused(String outputA, String outputB) throws SqlException {
        final String fused = "select ts, " + outputA + ", " + outputB + " from t " + WINDOW;
        final String referenceA = "select ts, " + outputA + " from t " + WINDOW;
        final String referenceB = "select ts, " + outputB + " from t " + WINDOW;
        assertIsBound(fused, true);
        assertIsBound(referenceA, false);
        assertIsBound(referenceB, false);
        final String expected = zipLastColumn(body(render(referenceA)), body(render(referenceB)));
        // A comparison of two empty renderings would pass and prove nothing, and every way the
        // helpers above could go wrong ends in one.
        Assert.assertFalse("the references produced no rows", expected.trim().isEmpty());
        Assert.assertEquals(expected, body(render(fused)));
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
     * Glues the last column of {@code right} onto every row of {@code left}, which is the fused
     * query's row when both references carry the same leading columns - asserted here rather
     * than assumed, since a mismatch there would silently weaken every comparison.
     */
    private static String zipLastColumn(String left, String right) {
        final String[] leftRows = left.split("\n", -1);
        final String[] rightRows = right.split("\n", -1);
        Assert.assertEquals("reference row counts differ", leftRows.length, rightRows.length);
        final StringBuilder out = new StringBuilder();
        for (int i = 0; i < leftRows.length; i++) {
            if (i > 0) {
                out.append('\n');
            }
            if (leftRows[i].isEmpty()) {
                Assert.assertTrue(rightRows[i].isEmpty());
                continue;
            }
            final int leftSplit = leftRows[i].lastIndexOf('\t');
            final int rightSplit = rightRows[i].lastIndexOf('\t');
            Assert.assertEquals(
                    "reference rows are not aligned",
                    leftRows[i].substring(0, leftSplit),
                    rightRows[i].substring(0, rightSplit)
            );
            out.append(leftRows[i]).append('\t').append(rightRows[i], rightSplit + 1, rightRows[i].length());
        }
        return out.toString();
    }

    private void createTable() throws SqlException {
        execute("create table t (ts timestamp, k symbol, k2 symbol, x double, y double) "
                + "timestamp(ts) partition by day");
    }

    private void insertNullsAndInfinities() throws SqlException {
        // sum contributes on Numbers.isFinite and count(y) on a null test, so an infinity is a
        // row the two disagree about in a way no NULL reproduces.
        execute("insert into t values " +
                "('2024-01-01T00:00:00.000000Z', 'a', 'p', 1.0, 1.0), " +
                "('2024-01-01T00:00:01.000000Z', 'a', 'p', null, null), " +
                "('2024-01-01T00:00:02.000000Z', 'a', 'q', 'Infinity'::double, 2.0), " +
                "('2024-01-01T00:00:03.000000Z', 'b', 'q', '-Infinity'::double, null), " +
                "('2024-01-01T00:00:04.000000Z', 'b', 'p', 2.5, 3.0), " +
                "('2024-01-01T00:00:05.000000Z', 'b', 'p', null, 4.0), " +
                "('2024-01-01T00:00:06.000000Z', 'a', 'q', -3.5, null)");
    }

    private void insertOrdinaryRows() throws SqlException {
        execute("insert into t values " +
                "('2024-01-01T00:00:00.000000Z', 'a', 'p', 1.0, 10.0), " +
                "('2024-01-01T00:00:01.000000Z', 'b', 'p', 2.0, 20.0), " +
                "('2024-01-01T00:00:02.000000Z', 'a', 'q', 3.0, 30.0), " +
                "('2024-01-01T00:00:03.000000Z', 'c', 'q', 4.0, 40.0), " +
                "('2024-01-01T00:00:04.000000Z', 'a', 'p', 5.0, 50.0), " +
                "('2024-01-01T00:00:05.000000Z', 'b', 'q', 6.0, 60.0), " +
                "('2024-01-01T00:00:06.000000Z', 'c', 'p', 7.0, 70.0), " +
                "('2024-01-01T00:00:07.000000Z', 'a', 'q', 8.0, 80.0), " +
                "('2024-01-01T00:00:08.000000Z', 'b', 'p', 9.0, 90.0)");
    }
}
