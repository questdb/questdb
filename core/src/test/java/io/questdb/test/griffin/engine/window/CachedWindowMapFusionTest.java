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
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.window.CachedWindowLightRecordCursorFactory;
import io.questdb.griffin.engine.window.CachedWindowMapGroups;
import io.questdb.griffin.engine.window.CachedWindowRecordCursorFactory;
import io.questdb.griffin.engine.window.WindowAccumulatorPlan;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.engine.window.WindowMapState;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * The window Map group on the cached cursors, where the rows reach a function through a sorted
 * record chain rather than straight off the base cursor.
 * <p>
 * The runtime is {@code WindowMapStateTest}'s - one map, one lookup a row, every contributor
 * then every projection - and what this class covers is the two things the cached path adds.
 * The first is where a group is driven from: a cached factory traverses its functions in
 * buckets (one per ORDER BY sort group, plus the natural-order functions), and a group belongs
 * to exactly one of them. The second is how the answer leaves the group: a bound function's
 * {@code pass1} is a no-op {@code computeNext} followed by the write of whatever
 * {@code projectWindowState} last materialized, so the group has to run before the bucket's
 * {@code pass1} loop and not after it.
 * <p>
 * Every case runs on both cached factories. They have intentionally parallel pass loops, and
 * the physical plan a query lands on must not decide whether its state fuses.
 */
public class CachedWindowMapFusionTest extends AbstractCairoTest {

    private static final String FORCING_CALL = "avg(x) over (partition by k)";
    // A window whose ORDER BY the base cursor does not already produce, so the query is
    // sorted and its functions land in a sort group. Descending on the designated timestamp
    // rather than on an ordinary column, so no two rows tie and the cumulative answers are a
    // function of the data alone.
    private static final String ORDERED_WINDOW =
            " from t window w as (partition by k order by ts desc rows between unbounded preceding and current row)";
    private static final int ROW_COUNT = 9;
    // The same window written so the base cursor's own order satisfies it. Its functions need
    // no sort of their own and are traversed with the base scan that fills the chain - the
    // natural-order bucket - which a query reaches only when something else forces the cached
    // path. FORCING_CALL is that something: a whole-partition avg is a two-pass function, and
    // one of those in the SELECT list is what the streaming fast path declines on.
    private static final String NATURAL_WINDOW =
            " from t window w as (partition by k order by ts rows between unbounded preceding and current row)";
    // Two windows that agree on their ORDER BY and differ in their partition key: one sort
    // group, two Map subgroups.
    private static final String TWO_WINDOWS = " from t "
            + "window w as (partition by k order by ts desc rows between unbounded preceding and current row), "
            + "w2 as (partition by k2 order by ts desc rows between unbounded preceding and current row)";

    @Test
    public void testAFailedOpenLeavesNoGroupHoldingBacking() throws Exception {
        // The cached open allocates a record chain, a sort buffer and the group maps, and a
        // per-query breach can land on any of them. Whichever it lands on, the close the
        // failed open runs has to leave every group empty-handed - which assertMemoryLeak
        // measures and the successful drain afterwards says was not achieved by staying shut.
        assertMemoryLeak(() -> {
            createTable();
            insertRows();
            setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, "false");
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = select(compiler, orderedSumAndCount(), sqlExecutionContext)) {
                final CachedWindowMapGroups groups = groups(factory);
                assertBoundGroupCount(groups, 1);
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 64L);
                for (int i = 0; i < 5; i++) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        drain(cursor);
                        Assert.fail("expected a per-query memory breach");
                    } catch (CairoException e) {
                        Assert.assertTrue(
                                "expected isOutOfMemory(), got: " + e.getFlyweightMessage(),
                                e.isOutOfMemory()
                        );
                    }
                    final ObjList<WindowMapState> states = groups.getStates();
                    for (int g = 0, n = states.size(); g < n; g++) {
                        Assert.assertFalse("group " + g + " kept its map open", states.getQuick(g).isMapOpen());
                    }
                }
                Assert.assertEquals("busy reader count", 0, engine.getBusyReaderCount());
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0L);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertEquals(ROW_COUNT, drain(cursor));
                }
            }
        });
    }

    @Test
    public void testEveryFusibleShapeMatchesTheUnfusedPath() throws Exception {
        // The differential, over every family the first slice admits, in both buckets and on
        // both factories. The reference is this tree's unfused cached path rather than a
        // second fused run: a window carrying one fusible function forms no group, so each
        // single-output query runs the per-function map and per-function probe the cached
        // cursors have always run.
        assertMemoryLeak(() -> {
            createTable();
            insertRows();
            for (int light = 0; light < 2; light++) {
                setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, light == 0 ? "false" : "true");
                for (int natural = 0; natural < 2; natural++) {
                    final String window = natural == 0 ? ORDERED_WINDOW : NATURAL_WINDOW;
                    // The two-pass call that forces the cached path stays in every query of
                    // the natural comparison, references included: without it a single-output
                    // reference would take the streaming cursor, and this comparison is about
                    // the cached one. An ordered window needs no such call - a sort the base
                    // cursor does not already produce is itself what declines the fast path.
                    final String lead = natural == 0 ? "" : ", " + FORCING_CALL;
                    assertFusedMatchesUnfused(window, lead, "sum(x) over w", "count(y) over w");
                    assertFusedMatchesUnfused(window, lead, "sum(x) over w", "avg(x) over w", "count(x) over w");
                    assertFusedMatchesUnfused(
                            window,
                            lead,
                            "count(*) over w",
                            "row_number() over w",
                            "count(k) over w"
                    );
                    assertFusedMatchesUnfused(
                            window,
                            lead,
                            "stddev_samp(x) over w",
                            "stddev_pop(x) over w",
                            "var_samp(x) over w",
                            "var_pop(x) over w",
                            "count(x) over w"
                    );
                }
            }
        });
    }

    @Test
    public void testExplainOutputIsUnchanged() throws Exception {
        // A group is an internal decision about how the same rows are computed, and both
        // cached plans are asserted across a large number of existing tests. Pinned at both
        // settings of the kill switch and on both factories, so a group line cannot arrive
        // unnoticed in either direction.
        assertMemoryLeak(() -> {
            createTable();
            for (int light = 0; light < 2; light++) {
                setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, light == 0 ? "false" : "true");
                setProperty(PropertyKey.CAIRO_SQL_WINDOW_MAP_FUSION_ENABLED, "true");
                final String fused = plan(orderedSumAndCount());
                setProperty(PropertyKey.CAIRO_SQL_WINDOW_MAP_FUSION_ENABLED, "false");
                Assert.assertEquals(fused, plan(orderedSumAndCount()));
                TestUtils.assertContains(fused, "CachedWindow");
                Assert.assertEquals(
                        "the query did not take the expected cached factory",
                        light == 1,
                        fused.contains("CachedWindowLight")
                );
            }
        });
    }

    @Test
    public void testMergedProjectionsShareOneComponentAndOneArgumentEvaluation() throws Exception {
        // The acceptance shape, read off a sorted traversal: three maps, three probes, three
        // components, five slots, three updates and three evaluations of x a row become one of
        // each. The update count is the argument-evaluation count for these families, because
        // accumulateWindowState is the only place any of them reads its argument and only the
        // component's one contributor is asked to run it.
        assertMemoryLeak(() -> {
            createTable();
            insertRows();
            for (int light = 0; light < 2; light++) {
                setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, light == 0 ? "false" : "true");
                final String sql = "select ts, sum(x) over w, avg(x) over w, count(x) over w" + ORDERED_WINDOW;
                try (SqlCompiler compiler = engine.getSqlCompiler();
                     RecordCursorFactory factory = select(compiler, sql, sqlExecutionContext)) {
                    assertFactoryKind(factory, light == 1);
                    final CachedWindowMapGroups groups = groups(factory);
                    assertBoundGroupCount(groups, 1);
                    final WindowMapState state = groups.getStates().getQuick(0);
                    final WindowAccumulatorPlan plan = state.getPlan();
                    Assert.assertEquals(1, plan.getComponentCount());
                    Assert.assertEquals(3, plan.getProjectionCount());
                    Assert.assertEquals(2, plan.getSlotCount());
                    Assert.assertTrue(plan.getProjection(2).isDerived());
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        final long rows = drain(cursor);
                        Assert.assertEquals(ROW_COUNT, rows);
                        Assert.assertEquals(rows, state.getLookupCount());
                        Assert.assertEquals(rows, state.getContributorUpdateCount());
                        Assert.assertEquals(3 * rows, state.getProjectionWriteCount());
                    }
                }
            }
        });
    }

    @Test
    public void testNaturalOrderFunctionsFormTheirOwnGroupBesideATwoPassResidual() throws Exception {
        // The natural-order bucket: the group is driven by the base scan that fills the chain,
        // beside a whole-partition avg that keeps its own map and its own two passes. A
        // two-pass function is what forces the cached path here, so this is also the shape
        // that says a residual and a group share a cursor without sharing anything else.
        assertMemoryLeak(() -> {
            createTable();
            insertRows();
            for (int light = 0; light < 2; light++) {
                setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, light == 0 ? "false" : "true");
                final String sql = "select ts, " + FORCING_CALL + ", sum(x) over w, count(y) over w" + NATURAL_WINDOW;
                try (SqlCompiler compiler = engine.getSqlCompiler();
                     RecordCursorFactory factory = select(compiler, sql, sqlExecutionContext)) {
                    assertFactoryKind(factory, light == 1);
                    final CachedWindowMapGroups groups = groups(factory);
                    assertBoundGroupCount(groups, 1);
                    // The bucket, not just the count: a natural-order group runs in the
                    // forward scan and there is no sort group for it to have landed in.
                    Assert.assertNull(groups.getOrderedStates(0));
                    Assert.assertNull(groups.getBackwardUnorderedStates());
                    Assert.assertNotNull(groups.getForwardUnorderedStates());
                    Assert.assertEquals(1, groups.getForwardUnorderedStates().size());
                    final WindowMapState state = groups.getStates().getQuick(0);
                    Assert.assertEquals(2, state.getPlan().getComponentCount());
                    Assert.assertEquals(2, state.getPlan().getProjectionCount());
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        final long rows = drain(cursor);
                        Assert.assertEquals(ROW_COUNT, rows);
                        Assert.assertEquals(rows, state.getLookupCount());
                    }
                }
            }
        });
    }

    @Test
    public void testOrderedSortGroupSharesOneMapAndOneLookup() throws Exception {
        // The headline shape on a sorted traversal. sum(x) counts finite x values and count(y)
        // counts non-null y values, so the two keep separate counters; what they share is the
        // key domain, the hash table and the row's one lookup.
        assertMemoryLeak(() -> {
            createTable();
            insertRows();
            for (int light = 0; light < 2; light++) {
                setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, light == 0 ? "false" : "true");
                try (SqlCompiler compiler = engine.getSqlCompiler();
                     RecordCursorFactory factory = select(compiler, orderedSumAndCount(), sqlExecutionContext)) {
                    assertFactoryKind(factory, light == 1);
                    final CachedWindowMapGroups groups = groups(factory);
                    assertBoundGroupCount(groups, 1);
                    // The bucket: an ordered group runs inside its sort group's traversal and
                    // never in the natural-order loops.
                    Assert.assertNotNull(groups.getOrderedStates(0));
                    Assert.assertEquals(1, groups.getOrderedStates(0).size());
                    Assert.assertNull(groups.getForwardUnorderedStates());
                    Assert.assertNull(groups.getBackwardUnorderedStates());
                    final WindowMapState state = groups.getStates().getQuick(0);
                    Assert.assertEquals(2, state.getPlan().getComponentCount());
                    Assert.assertEquals(2, state.getPlan().getProjectionCount());
                    Assert.assertEquals(3, state.getPlan().getSlotCount());
                    Assert.assertFalse("the group allocated before a tracker was bound", state.isMapOpen());
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.assertTrue(state.isMapOpen());
                        final long rows = drain(cursor);
                        Assert.assertEquals(ROW_COUNT, rows);
                        Assert.assertEquals(rows, state.getLookupCount());
                        Assert.assertEquals(2 * rows, state.getContributorUpdateCount());
                        Assert.assertEquals(2 * rows, state.getProjectionWriteCount());
                        Assert.assertNotNull(state.getMapImplementation());
                        Assert.assertTrue(state.getUnorderedMapMaxEntrySize() > 0);
                    }
                    Assert.assertFalse("close left the group's backing allocated", state.isMapOpen());
                    // Ten more cycles inside the leak check: the map is allocated under the
                    // per-query tracker at open and handed back at close, so the counter has
                    // to net to zero however many times the factory is re-executed.
                    for (int i = 0; i < 10; i++) {
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            Assert.assertEquals("iteration " + i, ROW_COUNT, drain(cursor));
                            Assert.assertEquals(ROW_COUNT, state.getLookupCount());
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testRandomAccessAndASecondPassReadTheSameValues() throws Exception {
        // Cached execution materializes every output into the row it belongs to, so a value
        // read again - by rewinding the cursor, or by addressing a row directly - must be the
        // one the traversal wrote. A group that left its answer in a function's scalar field
        // instead of the row would pass a single forward drain and fail both of these.
        assertMemoryLeak(() -> {
            createTable();
            insertRows();
            for (int light = 0; light < 2; light++) {
                setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, light == 0 ? "false" : "true");
                try (SqlCompiler compiler = engine.getSqlCompiler();
                     RecordCursorFactory factory = select(compiler, orderedSumAndCount(), sqlExecutionContext)) {
                    assertFactoryKind(factory, light == 1);
                    assertBoundGroupCount(groups(factory), 1);
                    final StringSink first = new StringSink();
                    final StringSink second = new StringSink();
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        final LongList rowIds = new LongList();
                        final Record record = cursor.getRecord();
                        while (cursor.hasNext()) {
                            rowIds.add(record.getRowId());
                            printRow(record, first);
                        }
                        Assert.assertEquals(ROW_COUNT, rowIds.size());
                        cursor.toTop();
                        while (cursor.hasNext()) {
                            printRow(record, second);
                        }
                        TestUtils.assertEquals(first, second);
                        // Backwards, through recordB, so neither the order the rows are read
                        // in nor the record they are read through is the one that produced
                        // them.
                        final StringSink random = new StringSink();
                        final Record recordB = cursor.getRecordB();
                        for (int i = rowIds.size() - 1; i >= 0; i--) {
                            cursor.recordAt(recordB, rowIds.getQuick(i));
                            printRow(recordB, random);
                        }
                        Assert.assertEquals(reverseLines(first.toString()), random.toString());
                    }
                }
            }
        });
    }

    @Test
    public void testSharingASortIsNotSharingAMap() throws Exception {
        // Two windows that agree on their ORDER BY and differ in their partition key are one
        // sort group and two Map subgroups. The compiler's own sort-sharing key would have put
        // all four accumulators in one map value keyed by whichever partition came first; the
        // window spec is what keeps them apart, and the two lookups a row are what says so.
        assertMemoryLeak(() -> {
            createTable();
            insertRows();
            for (int light = 0; light < 2; light++) {
                setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, light == 0 ? "false" : "true");
                final String sql = "select ts, sum(x) over w, count(y) over w, sum(x) over w2, count(y) over w2"
                        + TWO_WINDOWS;
                try (SqlCompiler compiler = engine.getSqlCompiler();
                     RecordCursorFactory factory = select(compiler, sql, sqlExecutionContext)) {
                    assertFactoryKind(factory, light == 1);
                    final CachedWindowMapGroups groups = groups(factory);
                    assertBoundGroupCount(groups, 2);
                    // One sort group carrying both, which is what makes this the subgroup case
                    // rather than two unrelated traversals.
                    Assert.assertNotNull(groups.getOrderedStates(0));
                    Assert.assertEquals(2, groups.getOrderedStates(0).size());
                    Assert.assertNull(groups.getOrderedStates(1));
                    final ObjList<WindowMapState> states = groups.getStates();
                    Assert.assertFalse(
                            states.getQuick(0).getPlan().getSpec().isSameSpec(states.getQuick(1).getPlan().getSpec())
                    );
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        final long rows = drain(cursor);
                        Assert.assertEquals(rows, states.getQuick(0).getLookupCount());
                        Assert.assertEquals(rows, states.getQuick(1).getLookupCount());
                    }
                }
                assertFusedMatchesUnfused(
                        TWO_WINDOWS,
                        "",
                        "sum(x) over w", "count(y) over w", "sum(x) over w2", "count(y) over w2"
                );
            }
        });
    }

    @Test
    public void testTheKillSwitchLeavesEveryFunctionOnItsOwnMap() throws Exception {
        // What the switch turns off is the binding and nothing else: the group is still
        // compiled - a plan no runtime reads costs a query nothing - every function is back on
        // its own map, and the rows are the ones the fused run produced.
        assertMemoryLeak(() -> {
            createTable();
            insertRows();
            for (int light = 0; light < 2; light++) {
                setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, light == 0 ? "false" : "true");
                setProperty(PropertyKey.CAIRO_SQL_WINDOW_MAP_FUSION_ENABLED, "true");
                final String fused = render(orderedSumAndCount());
                setProperty(PropertyKey.CAIRO_SQL_WINDOW_MAP_FUSION_ENABLED, "false");
                try (SqlCompiler compiler = engine.getSqlCompiler();
                     RecordCursorFactory factory = select(compiler, orderedSumAndCount(), sqlExecutionContext)) {
                    final CachedWindowMapGroups groups = groups(factory);
                    Assert.assertNotNull(groups);
                    Assert.assertEquals(1, groups.getPlans().size());
                    Assert.assertEquals(2, groups.getPlans().getQuick(0).getComponentCount());
                    Assert.assertEquals(0, groups.getStates().size());
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.assertEquals(ROW_COUNT, drain(cursor));
                        final ObjList<WindowFunction> functions = windowFunctions(factory);
                        Assert.assertEquals(2, functions.size());
                        for (int i = 0, n = functions.size(); i < n; i++) {
                            final WindowFunction function = functions.getQuick(i);
                            Assert.assertFalse("a function stayed bound with fusion off", function.isWindowStateOwned());
                            Assert.assertTrue(
                                    "a function's own map never opened",
                                    function.getPartitionMap().isOpen()
                            );
                        }
                    }
                }
                Assert.assertEquals(fused, render(orderedSumAndCount()));
            }
        });
    }

    private static void assertBoundGroupCount(CachedWindowMapGroups groups, int expected) {
        Assert.assertNotNull("no window Map group was compiled", groups);
        Assert.assertEquals(expected, groups.getStates().size());
    }

    private static void assertFactoryKind(RecordCursorFactory factory, boolean light) {
        final RecordCursorFactory root = cachedFactory(factory);
        Assert.assertEquals(
                "the query did not take the expected cached factory",
                light,
                root instanceof CachedWindowLightRecordCursorFactory
        );
    }

    /**
     * Requires the fused query to produce, row for row, what the same outputs produce one at a
     * time on the same factory.
     * <p>
     * The references are the unfused cached path rather than a second fused one: a window
     * carrying a single fusible function forms no group at all - moving a map is not removing
     * one - so each of them runs the per-function map and per-function probe the cached
     * cursors have always run. That holds for a merged shape too: asked for on its own, a
     * {@code count(x)} that reads a {@code sum(x)}'s counter in the fused query keeps and
     * maintains a counter of its own here.
     */
    private static void assertFusedMatchesUnfused(
            String window,
            String lead,
            String... outputs
    ) throws SqlException {
        final StringBuilder fused = new StringBuilder("select ts").append(lead);
        for (String output : outputs) {
            fused.append(", ").append(output);
        }
        fused.append(window);
        assertIsBound(fused.toString(), true);
        final String[] references = new String[outputs.length];
        for (int i = 0; i < outputs.length; i++) {
            final String reference = "select ts" + lead + ", " + outputs[i] + window;
            assertIsBound(reference, false);
            references[i] = body(render(reference));
        }
        final String expected = zipLastColumns(references);
        // A comparison of two empty renderings would pass and prove nothing, and every way the
        // helpers above could go wrong ends in one.
        Assert.assertFalse("the references produced no rows", expected.trim().isEmpty());
        Assert.assertEquals(fused.toString(), expected, body(render(fused.toString())));
    }

    private static void assertIsBound(String sql, boolean bound) throws SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler();
             RecordCursorFactory factory = select(compiler, sql, sqlExecutionContext)) {
            final CachedWindowMapGroups groups = groups(factory);
            Assert.assertEquals(sql, bound, groups != null && groups.getStates().size() > 0);
        }
    }

    /**
     * Everything after the header. The queries carry different column aliases - {@code count}
     * alone versus {@code count} beside {@code count1} - and the header is not what the
     * comparison is about.
     */
    private static String body(String rendered) {
        final int lineEnd = rendered.indexOf('\n');
        return lineEnd < 0 ? "" : rendered.substring(lineEnd + 1);
    }

    private static RecordCursorFactory cachedFactory(RecordCursorFactory factory) {
        // A projection wrapper can sit above the window factory, so the search walks the whole
        // chain rather than unwrapping one known level.
        RecordCursorFactory root = factory;
        while (root != null
                && !(root instanceof CachedWindowRecordCursorFactory)
                && !(root instanceof CachedWindowLightRecordCursorFactory)) {
            root = root.getBaseFactory();
        }
        Assert.assertNotNull("no cached window factory in the tree", root);
        return root;
    }

    private static long drain(RecordCursor cursor) {
        long rows = 0;
        while (cursor.hasNext()) {
            rows++;
        }
        return rows;
    }

    private static CachedWindowMapGroups groups(RecordCursorFactory factory) {
        final RecordCursorFactory root = cachedFactory(factory);
        return root instanceof CachedWindowRecordCursorFactory f
                ? f.getWindowMapGroups()
                : ((CachedWindowLightRecordCursorFactory) root).getWindowMapGroups();
    }

    private static String plan(String sql) throws SqlException {
        return render("explain " + sql);
    }

    /**
     * The three columns of {@link #orderedSumAndCount()}, rendered the same way whichever
     * record they are read through, so the three readings this class compares differ in
     * nothing but how they were obtained.
     */
    private static void printRow(Record record, StringSink sink) {
        sink.put(record.getTimestamp(0)).put('\t');
        sink.put(record.getDouble(1)).put('\t');
        sink.put(record.getLong(2)).put('\n');
    }

    private static String render(String sql) throws SqlException {
        final StringSink localSink = new StringSink();
        printSql(sql, localSink);
        return localSink.toString();
    }

    private static String reverseLines(String text) {
        final String[] lines = text.split("\n", -1);
        final StringBuilder out = new StringBuilder();
        for (int i = lines.length - 1; i >= 0; i--) {
            if (lines[i].isEmpty()) {
                continue;
            }
            out.append(lines[i]).append('\n');
        }
        return out.toString();
    }

    private static ObjList<WindowFunction> windowFunctions(RecordCursorFactory factory) {
        final RecordCursorFactory root = cachedFactory(factory);
        return root instanceof CachedWindowRecordCursorFactory f
                ? f.getAllWindowFunctions()
                : ((CachedWindowLightRecordCursorFactory) root).getAllWindowFunctions();
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

    private static String orderedSumAndCount() {
        return "select ts, sum(x) over w, count(y) over w" + ORDERED_WINDOW;
    }

    private void createTable() throws SqlException {
        execute("create table t (ts timestamp, k symbol, k2 symbol, x double, y double) "
                + "timestamp(ts) partition by day");
    }

    /**
     * The partition shapes the merged families part company on: a NULL key, whose
     * {@code count(k)} is zero while its row count is not; a partition of one row, where a
     * sample dispersion is NULL and a population one is 0; one whose only non-null {@code x}
     * is an infinity, so it has rows, a non-null count and no finite value; and rows where
     * exactly one of {@code x} and {@code y} is absent, which is where two counters part.
     */
    private void insertRows() throws SqlException {
        execute("insert into t values " +
                "('2024-01-01T00:00:00.000000Z', 'a', 'p', 1.0, 10.0), " +
                "('2024-01-01T00:00:01.000000Z', null, 'p', 2.0, 20.0), " +
                "('2024-01-01T00:00:02.000000Z', 'a', 'q', 4.0, null), " +
                "('2024-01-01T00:00:03.000000Z', null, 'q', null, 40.0), " +
                "('2024-01-01T00:00:04.000000Z', 'one', 'p', 5.0, 50.0), " +
                "('2024-01-01T00:00:05.000000Z', 'nx', 'q', null, 60.0), " +
                "('2024-01-01T00:00:06.000000Z', 'nx', 'p', 'Infinity'::double, 70.0), " +
                "('2024-01-01T00:00:07.000000Z', 'a', 'q', 8.0, 80.0), " +
                "('2024-01-01T00:00:08.000000Z', null, 'p', 9.0, null)");
    }
}
