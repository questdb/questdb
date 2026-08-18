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
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.window.CachedWindowLightRecordCursorFactory;
import io.questdb.griffin.engine.window.CachedWindowMapGroups;
import io.questdb.griffin.engine.window.CachedWindowRecordCursorFactory;
import io.questdb.griffin.engine.window.WindowMapState;
import io.questdb.griffin.engine.window.WindowRecordCursorFactory;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * The differential harness for window Map fusion: random multi-function window queries over
 * random data, each answered twice - once with {@code cairo.sql.window.map.fusion.enabled} on
 * and once with it off - and required to produce the same rows both ways.
 * <p>
 * The enumerated cases in {@code WindowMapStateTest} pin the shapes this feature was designed
 * around. What they cannot do is cover the combinations, and the whole contract here is "same
 * answers, fewer maps" - so a query nobody thought to write is exactly the one that finds a
 * slot base read against the wrong component. The generator therefore varies the things the
 * plan is a function of: the partition key's type and cardinality (including a NULL key), how
 * often an accumulator argument is absent, which families sit in one window, how many windows
 * a query carries, whether a window is named or spelled inline, the SELECT-list order, and
 * {@code cairo.sql.unordered.map.max.entry.size}, which decides which {@link io.questdb.cairo.map.Map}
 * implementation the group's widened value lands on.
 * <p>
 * Two properties make the comparison meaningful rather than merely green:
 * <ul>
 *     <li><b>the reference is this tree's own unfused path.</b> With the switch off no group
 *     owns a map and every function is back on the private one it has always had, which the
 *     run asserts rather than assumes;</li>
 *     <li><b>the run must actually fuse something.</b> A generator that drifted into shapes
 *     that never bind would still pass every comparison, so the case fails at the end if no
 *     iteration bound a group, and reports how many did.</li>
 * </ul>
 * Every rendering is taken twice off one cursor with a {@code toTop} between, because state
 * that survives a re-read is the one class of bug a group can introduce that a single pass
 * cannot see.
 * <p>
 * The seed is {@link TestUtils#generateRandom}'s and is logged, so a failure reproduces; the
 * assertion message carries the table, the query and the iteration that produced it.
 */
public class WindowMapFusionFuzzTest extends AbstractCairoTest {

    /**
     * The Map-entry limits that ship: 16 is what {@code DefaultCairoConfiguration} returns for
     * embedded use and the benchmarks, 32 is a server's default, and 64 is neither - it is here
     * so a wide group runs on an unordered map too, which the other two put on an OrderedMap.
     */
    private static final int[] ENTRY_SIZES = {16, 32, 64};
    /**
     * A ROWS frame with a bounded low bound, a bounded high bound, or both - the shapes whose
     * accumulator has to give rows back, and so the ones the ring-backed families serve.
     */
    private static final int FRAME_BOUNDED_ROWS = 2;
    /**
     * A RANGE frame with a bounded low bound, a bounded high bound, or both - the same three
     * geometries as {@link #FRAME_BOUNDED_ROWS}, over a ring that grows with the data rather than
     * one the query's own span sizes.
     */
    private static final int FRAME_BOUNDED_RANGE = 3;
    private static final int FRAME_RANGE = 0;
    private static final int FRAME_ROWS = 1;
    /**
     * A window with no ORDER BY, whose every row is a peer and whose frame is therefore the whole
     * partition. It is the only frame here whose functions are two-pass, so it is also the only
     * one that reaches a cached cursor - and the only one whose group may leave a row out of its
     * map in pass 1, which is the path this arm exists to compare.
     */
    private static final int FRAME_WHOLE_PARTITION = 4;
    private static final int ITERATIONS = 40;
    private static final String[] KEY_COLUMNS = {"ki", "ks", "kv"};
    /**
     * Keys no column of the base carries, over the same columns the plain keys use - so a
     * window keyed by one of these partitions the same rows differently, and a fused arm that
     * read the key off a column would answer visibly wrong rather than subtly so.
     */
    private static final String[] KEY_EXPRESSIONS = {"concat(ks, kv)", "ki + 1", "concat(ks, 'z')"};
    /**
     * Calls whose class declares an accumulator family, so a group can carry them. The
     * arguments are direct columns of their own type, which is what the first slice admits.
     */
    private static final String[] ROWS_FRAME_CALLS = {
            "sum(xd)",
            "sum(yd)",
            "avg(xd)",
            "avg(yd)",
            "count(xd)",
            "count(yd)",
            "count(*)",
            "count(ki)",
            "count(ks)",
            "count(kv)",
            "row_number()",
            "stddev_samp(xd)",
            "stddev_pop(xd)",
            "var_samp(xd)",
            "var_pop(xd)",
            // The four extremum families, one call per (direction, state type) pair plus the
            // pairs that share an argument: max and min over one column keep two components
            // and never merge, which is the shape most likely to read the wrong slot.
            "max(xd)",
            "min(xd)",
            "max(yd)",
            "min(yd)",
            "max(xl)",
            "min(xl)",
            "max(ts)",
            "min(ts)",
            // The compensated sum, which is a component of its own beside sum(xd) and lends
            // its counter to count(xd) - so an iteration carrying all three is where a total
            // read off the wrong host would show.
            "ksum(xd)",
            "ksum(yd)",
            // The DECIMAL extrema, whose slot is the argument's own payload: a LONG for the
            // narrow width and a DECIMAL128 for the wide one. The counts over the same columns
            // are here for the same reason the pairs above are - they share an argument and a
            // contribution predicate with the extrema and must still keep components of their
            // own - and a wide slot behind a narrow one is what moves a slot base.
            "max(xdec)",
            "min(xdec)",
            "count(xdec)",
            "max(xdec128)",
            "min(xdec128)",
            "count(xdec128)",
            // The capture families: three spellings over one column, which are three components
            // and the shape most likely to read a neighbour's value slot, plus the 64-bit state
            // width through xl and ts. A partition whose first row is absent or infinite is where
            // the three answers part company, and the generator makes both.
            "first_value(xd)",
            "first_value(xd) ignore nulls",
            "last_value(xd) ignore nulls",
            "first_value(yd)",
            "first_value(xl)",
            "first_value(xl) ignore nulls",
            "last_value(ts) ignore nulls",
    };
    /**
     * The calls a <b>bounded</b> ROWS window can fuse: the two ring-backed families and nothing
     * else. A {@code count(*)} over such a frame keeps the partition's row count and saturates the
     * output against the frame size, which is a reading no family here describes, and the
     * dispersion, extremum and compensated-sum implementations behind a bounded frame are separate
     * classes that declare none - so all of those stay residual and share the query rather than the
     * group.
     */
    private static final String[] BOUNDED_ROWS_FRAME_CALLS = {
            "sum(xd)",
            "sum(yd)",
            "avg(xd)",
            "avg(yd)",
            "count(xd)",
            "count(yd)",
            "count(ki)",
            "count(ks)",
            "count(kv)",
            "count(xdec)",
            "count(xdec128)",
            // Residual over this frame, and here on purpose: a bound group and a function still on
            // its own map and its own ring in one cursor is what an ordinary query looks like.
            "count(*)",
            "max(xd)",
            "min(xd)",
            "ksum(xd)",
    };
    /**
     * The calls a <b>bounded</b> RANGE window can fuse. The same two families the bounded ROWS arm
     * carries, spelled over a resizable ring - and the same residuals for the same reason: a
     * {@code count(*)} over such a frame counts rows rather than a column's values, and the
     * extremum and compensated-sum implementations behind a bounded RANGE frame are separate
     * classes that declare no family.
     */
    private static final String[] BOUNDED_RANGE_FRAME_CALLS = {
            "sum(xd)",
            "sum(yd)",
            "avg(xd)",
            "avg(yd)",
            "count(xd)",
            "count(yd)",
            "count(ki)",
            "count(ks)",
            "count(kv)",
            "count(xdec)",
            "count(xdec128)",
            // Residual over this frame, and here on purpose: a bound group and a function still on
            // its own map and its own ring in one cursor is what an ordinary query looks like.
            "count(*)",
            "max(xd)",
            "min(xd)",
            "ksum(xd)",
    };
    /**
     * A RANGE-framed window carries no ranking call - {@code row_number()} has no frame - and
     * this build's dispersion factories dispatch RANGE elsewhere, so the arm keeps the families
     * both spellings reach. The extremum families are among them: RANGE unbounded-preceding-to-
     * current-row reaches the same class the ROWS spelling does.
     */
    private static final String[] RANGE_FRAME_CALLS = {
            "sum(xd)",
            "sum(yd)",
            "avg(xd)",
            "avg(yd)",
            "count(xd)",
            "count(yd)",
            "count(*)",
            "count(ki)",
            "count(ks)",
            "count(kv)",
            "max(xd)",
            "min(xd)",
            "max(xl)",
            "min(xl)",
            "max(ts)",
            "min(ts)",
            "ksum(xd)",
            "ksum(yd)",
            "max(xdec)",
            "min(xdec)",
            "count(xdec)",
            "max(xdec128)",
            "min(xdec128)",
            "count(xdec128)",
            // The capture families reach a RANGE unbounded-preceding-to-current-row window
            // through the very classes the ROWS spelling does, so the arm carries them too.
            "first_value(xd)",
            "first_value(xd) ignore nulls",
            "last_value(xd) ignore nulls",
            "first_value(ts)",
    };
    /**
     * The calls a <b>whole-partition</b> window can fuse. A short list, and deliberately so: the
     * extremum, dispersion, compensated-sum and capture implementations behind an unordered
     * window are separate classes that declare no family, so what is left is the DOUBLE
     * {@code (sum, nonNullCount)} pair, the counters that fold onto it, and the row count.
     * <p>
     * All three contribution predicates a two-pass group can carry are here, which is what makes
     * the arm differential about the pass-1 skip rather than only about the answers: the DOUBLE
     * calls, {@code count(xd)} and {@code count(ki)} refuse a row on {@code Numbers.isFinite},
     * which a group can evaluate for itself and skip on; the SYMBOL, VARCHAR and DECIMAL counters
     * refuse on their own type's null test, which it cannot; and {@code count(*)} refuses nothing
     * at all. A query mixing them is one whose group must decline the skip it would take for the
     * DOUBLE calls alone.
     */
    private static final String[] WHOLE_PARTITION_CALLS = {
            "sum(xd)",
            "sum(yd)",
            "avg(xd)",
            "avg(yd)",
            "count(xd)",
            "count(yd)",
            "count(*)",
            "count(ki)",
            "count(ks)",
            "count(kv)",
            "count(xdec)",
            "count(xdec128)",
    };
    /**
     * Calls no family describes. One of them lands in a query now and then so that a residual
     * function and a bound group share a cursor, which is what an ordinary query looks like.
     * <p>
     * A DECIMAL capture is what fills the list now that the DOUBLE and 64-bit ones fuse: the six
     * capture families are split by the width their state is kept at, and a DECIMAL first value
     * accumulates into its argument's own width, which none of them describes.
     */
    private static final String[] RESIDUAL_CALLS = {
            "first_value(xdec)",
            "first_value(xdec128)",
            "last_value(xdec) ignore nulls",
    };
    private static final String[] SKIP_ENABLED_WHOLE_PARTITION_CALLS = {
            "sum(xd)",
            "avg(xd)",
            "count(xd)",
    };
    private Rnd rnd;

    @Override
    @Before
    public void setUp() {
        rnd = TestUtils.generateRandom(LOG);
        super.setUp();
    }

    @Test
    public void testFusedMatchesUnfused() throws Exception {
        assertMemoryLeak(() -> {
            int boundIterations = 0;
            int boundGroups = 0;
            int skipEnabledGroups = 0;
            for (int i = 0; i < ITERATIONS; i++) {
                final String table = "t" + i;
                final String data = createTable(table);
                setProperty(
                        PropertyKey.CAIRO_SQL_UNORDERED_MAP_MAX_ENTRY_SIZE,
                        ENTRY_SIZES[rnd.nextInt(ENTRY_SIZES.length)]
                );
                // Preserve all random draws unless the last iteration must supply the run's
                // skip-enabled group. The assertion below still catches compiler or binding
                // drift that makes this known-compatible shape ineligible.
                final boolean isSkipEnabledGroupRequired = i == ITERATIONS - 1 && skipEnabledGroups == 0;
                final String sql = randomQuery(table, isSkipEnabledGroupRequired);
                final int[] groups = new int[1];
                final int[] skipEnabled = new int[1];
                final int[] unfusedGroups = new int[1];
                final String fused;
                final String unfused;
                try {
                    fused = render(sql, groups, skipEnabled);
                    setProperty(PropertyKey.CAIRO_SQL_WINDOW_MAP_FUSION_ENABLED, "false");
                    try {
                        unfused = render(sql, unfusedGroups, null);
                    } finally {
                        setProperty(PropertyKey.CAIRO_SQL_WINDOW_MAP_FUSION_ENABLED, "true");
                    }
                } catch (Throwable th) {
                    throw new AssertionError(context(i, data, sql), th);
                }
                // What the reference run is depends on this: with the switch off nothing owns a
                // shared map, so the rows it produced came from the private map every function
                // has always had.
                Assert.assertEquals(context(i, data, sql), 0, unfusedGroups[0]);
                if (groups[0] > 0) {
                    boundIterations++;
                    boundGroups += groups[0];
                }
                skipEnabledGroups += skipEnabled[0];
                Assert.assertEquals(context(i, data, sql), unfused, fused);
            }
            // A generator that stopped producing fusible shapes - a family withdrawn, a decline
            // rule widened, a spec discrimination added - would leave every comparison above
            // comparing the unfused path against itself and still pass.
            Assert.assertTrue(
                    "no iteration bound a window Map group, so nothing here was differential",
                    boundIterations > 0
            );
            Assert.assertTrue(
                    "no bound group enabled pass-1 skipping, so the refused-row path was never eligible",
                    skipEnabledGroups > 0
            );
            LOG.info().$("window map fusion fuzz [iterations=").$(ITERATIONS)
                    .$(", fusedIterations=").$(boundIterations)
                    .$(", boundGroups=").$(boundGroups)
                    .$(", skipEnabledGroups=").$(skipEnabledGroups)
                    .I$();
        });
    }

    private static String context(int iteration, String data, String sql) {
        return "iteration " + iteration + "\n" + data + "\n" + sql + "\n";
    }

    /**
     * Renders {@code sql} twice off one cursor, requires the two passes to agree, and reports
     * how many Map groups the compile bound into {@code groups} and how many of those enabled
     * pass-1 skipping into {@code skipEnabled}.
     * <p>
     * The second pass is not a formality: a group's map is cleared by the cursor's
     * {@code toTop} and by nothing else, so a domain that survived one - or one cleared once
     * per bound member rather than once per group - shows up here and nowhere else in this
     * class.
     */
    private static String render(String sql, int[] groups, int[] skipEnabled) throws SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler();
             RecordCursorFactory factory = select(compiler, sql, sqlExecutionContext)) {
            final ObjList<WindowMapState> states = windowMapStates(factory);
            if (groups != null) {
                groups[0] = states == null ? 0 : states.size();
            }
            if (skipEnabled != null && states != null) {
                for (int i = 0, n = states.size(); i < n; i++) {
                    if (states.getQuick(i).isPass1SkipEnabled()) {
                        skipEnabled[0]++;
                    }
                }
            }
            final StringSink first = new StringSink();
            final StringSink second = new StringSink();
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                CursorPrinter.println(cursor, factory.getMetadata(), first, true, false);
                cursor.toTop();
                CursorPrinter.println(cursor, factory.getMetadata(), second, true, false);
            }
            TestUtils.assertEquals(first, second);
            return first.toString();
        }
    }

    /**
     * The bound groups of the window factory in {@code factory}'s chain, or null when the query
     * has none - a projection wrapper can sit above the window factory, so the search walks the
     * whole chain rather than unwrapping one known level.
     * <p>
     * All three window factories, because which one a query lands on is the compiler's answer
     * rather than the generator's: one whole-partition call in the SELECT list is what declines
     * the streaming fast path, and a run that read groups off the streaming factory alone would
     * report every cached query as having bound none - which the differential assertion below
     * would then pass without comparing anything.
     */
    private static @Nullable ObjList<WindowMapState> windowMapStates(RecordCursorFactory factory) {
        RecordCursorFactory root = factory;
        while (root != null) {
            if (root instanceof WindowRecordCursorFactory f) {
                return f.getWindowMapStates();
            }
            if (root instanceof CachedWindowRecordCursorFactory f) {
                final CachedWindowMapGroups groups = f.getWindowMapGroups();
                return groups == null ? null : groups.getStates();
            }
            if (root instanceof CachedWindowLightRecordCursorFactory f) {
                final CachedWindowMapGroups groups = f.getWindowMapGroups();
                return groups == null ? null : groups.getStates();
            }
            root = root.getBaseFactory();
        }
        return null;
    }

    /**
     * Builds and fills one random table, returning the statements that produced it so a failure
     * can be replayed without the seed.
     * <p>
     * Every column the queries read is deliberately shaped by the row number rather than by a
     * {@code rnd_} call: the content then follows from the rendered SQL alone, and the SQL is in
     * the failure message.
     */
    private String createTable(String table) throws SqlException {
        final int rows = 200 + rnd.nextInt(rnd.nextInt(4) == 0 ? 4000 : 800);
        // One partition, a handful, or near-unique keys - the last takes the group's map through
        // several rehashes, which is where a key that stopped hashing to its own entry shows.
        final int cardinality = 1 + rnd.nextInt(rnd.nextInt(4) == 0 ? 400 : 12);
        final int nullKeyEvery = 3 + rnd.nextInt(20);
        // 1 is a column that is null on every row, which is the all-null partition the
        // "Runtime values" list asks for.
        final int xNullEvery = 1 + rnd.nextInt(9);
        final int yNullEvery = 1 + rnd.nextInt(9);
        // sum contributes on Numbers.isFinite and count on a null test, so an infinity is a row
        // the two disagree about in a way no NULL reproduces.
        final int infiniteEvery = 5 + rnd.nextInt(30);
        // A LONG argument as well as the two DOUBLE ones, because the extremum families are
        // split by the state's type and the two halves are separate implementations.
        final int lNullEvery = 1 + rnd.nextInt(9);
        // Two DECIMAL arguments, one of each state width an extremum over one can keep: xdec
        // lands in a LONG slot and xdec128 in a DECIMAL128 slot of the group's own value.
        final int decNullEvery = 1 + rnd.nextInt(9);
        final int dec128NullEvery = 1 + rnd.nextInt(9);
        final String ddl = "create table " + table
                + " (ts timestamp, ki int, ks symbol, kv varchar, xd double, yd double, xl long,"
                + " xdec decimal(18, 2), xdec128 decimal(38, 6))"
                + " timestamp(ts) partition by day";
        final String dml = "insert into " + table + " select"
                + " (x * 1000000L)::timestamp,"
                + " case when x % " + nullKeyEvery + " = 0 then null else (x % " + cardinality + ")::int end,"
                + " case when x % " + nullKeyEvery + " = 0 then null else 'k' || (x % " + cardinality + ") end,"
                + " case when x % " + nullKeyEvery + " = 0 then null else 'v' || (x % " + cardinality + ") end,"
                + " case when x % " + xNullEvery + " = 0 then null"
                + " when x % " + infiniteEvery + " = 0 then 'Infinity'::double"
                + " else (x % 97)::double / 7.0 end,"
                + " case when x % " + yNullEvery + " = 0 then null"
                + " when x % " + infiniteEvery + " = 0 then '-Infinity'::double"
                + " else (x % 89)::double end,"
                + " case when x % " + lNullEvery + " = 0 then null else (x % 83) - 41 end,"
                + " case when x % " + decNullEvery + " = 0 then null"
                + " else ((x % 79) - 39)::decimal(18, 2) end,"
                + " case when x % " + dec128NullEvery + " = 0 then null"
                + " else ((x % 71) - 35)::decimal(38, 6) end"
                + " from long_sequence(" + rows + ")";
        execute(ddl);
        execute(dml);
        return ddl + ";\n" + dml;
    }

    /**
     * One random bounded ROWS frame, in one of the three geometries a ring comes in: ending at the
     * current row, ending short of it, or with no low bound at all. The spans are small so that a
     * frame is crossed many times over the rows a table carries, and so that partitions shorter
     * than the frame are ordinary rather than exotic.
     */
    private String randomBoundedRowsFrame() {
        final int preceding = 1 + rnd.nextInt(8);
        if (rnd.nextInt(3) == 0) {
            // No low bound: nothing ever leaves the frame, so the ring keeps its unfused length.
            return "unbounded preceding and " + (1 + rnd.nextInt(preceding)) + " preceding";
        }
        if (rnd.nextBoolean()) {
            return preceding + " preceding and current row";
        }
        // A lagging high bound, where the entering value is read out of the ring as well.
        return preceding + " preceding and " + (1 + rnd.nextInt(preceding)) + " preceding";
    }

    /**
     * One random bounded RANGE frame, in the same three geometries. The spans are multiples of the
     * one-second row spacing the table is built with, so a frame holds a handful of rows and the
     * ring is crossed and regrown many times over the rows a table carries.
     */
    private String randomBoundedRangeFrame() {
        final int precedingSeconds = 1 + rnd.nextInt(8);
        final long preceding = precedingSeconds * 1_000_000L;
        final long lagging = (1 + rnd.nextInt(precedingSeconds)) * 1_000_000L;
        if (rnd.nextInt(3) == 0) {
            // No low bound: nothing ever leaves the frame, and the contributor takes the arm that
            // consumes the ring from its head rather than trimming it from behind.
            return "unbounded preceding and " + lagging + " microseconds preceding";
        }
        if (rnd.nextBoolean()) {
            return preceding + " microseconds preceding and current row";
        }
        // A lagging high bound, where the frame's own top edge trails the row being answered.
        return preceding + " microseconds preceding and " + lagging + " microseconds preceding";
    }

    /**
     * One random query: one or two partitioned windows, two to five outputs spread over them in
     * random order, each window referenced by name or spelled inline.
     * <p>
     * A query is drawn as one of two kinds, because the two do not mix. A <b>streaming</b> query's
     * windows are all partitioned and ordered by the designated timestamp the base is already
     * scanned in, which is what keeps it on the streaming path - and what a bounded RANGE frame
     * requires outright, since one is compiled only where that order was dismissed. A
     * <b>whole-partition</b> query's windows carry no ORDER BY at all, so every row of a partition
     * is a peer and every call over it is two-pass, which lands the query on a cached cursor
     * instead. Drawing one kind per query rather than one per window is what keeps a bounded RANGE
     * frame off a cursor that would not compile it.
     * <p>
     * What varies within a kind is everything the group identity is a function of, the frame
     * included: a cumulative ROWS or RANGE frame, or a bounded ROWS or RANGE one in each of the
     * three geometries its ring comes in.
     */
    private String randomQuery(String table, boolean isSkipEnabledGroupRequired) {
        final int windowCount = isSkipEnabledGroupRequired ? 1 : 1 + rnd.nextInt(2);
        final String[] specs = new String[windowCount];
        final int[] frameKinds = new int[windowCount];
        final boolean wholePartition = isSkipEnabledGroupRequired || rnd.nextInt(3) == 0;
        for (int w = 0; w < windowCount; w++) {
            // A key is a column or an expression over one or two of them, and which it is
            // decides how the group writes it: off the record's own columns, or through the
            // compiled terms it borrows from a member. The expressions are deliberately ones
            // whose value no column carries, so the two arms cannot agree by accident.
            final String key = rnd.nextInt(4) == 0
                    ? KEY_EXPRESSIONS[rnd.nextInt(KEY_EXPRESSIONS.length)]
                    : KEY_COLUMNS[rnd.nextInt(KEY_COLUMNS.length)];
            if (wholePartition) {
                frameKinds[w] = FRAME_WHOLE_PARTITION;
                specs[w] = "partition by " + key;
                continue;
            }
            final int roll = rnd.nextInt(8);
            frameKinds[w] = roll < 2
                    ? FRAME_RANGE
                    : (roll < 4 ? FRAME_BOUNDED_RANGE : (rnd.nextBoolean() ? FRAME_ROWS : FRAME_BOUNDED_ROWS));
            final boolean range = frameKinds[w] == FRAME_RANGE || frameKinds[w] == FRAME_BOUNDED_RANGE;
            final String frame;
            if (frameKinds[w] == FRAME_BOUNDED_ROWS) {
                frame = randomBoundedRowsFrame();
            } else if (frameKinds[w] == FRAME_BOUNDED_RANGE) {
                frame = randomBoundedRangeFrame();
            } else {
                frame = "unbounded preceding and current row";
            }
            specs[w] = "partition by " + key
                    + " order by ts "
                    + (range ? "range" : "rows")
                    + " between "
                    + frame;
        }
        final int outputs = isSkipEnabledGroupRequired ? SKIP_ENABLED_WHOLE_PARTITION_CALLS.length : 2 + rnd.nextInt(4);
        final StringBuilder sql = new StringBuilder("select ts");
        for (int o = 0; o < outputs; o++) {
            final int w = isSkipEnabledGroupRequired ? 0 : rnd.nextInt(windowCount);
            final String[] calls;
            switch (frameKinds[w]) {
                case FRAME_RANGE:
                    calls = RANGE_FRAME_CALLS;
                    break;
                case FRAME_BOUNDED_RANGE:
                    calls = BOUNDED_RANGE_FRAME_CALLS;
                    break;
                case FRAME_ROWS:
                    calls = ROWS_FRAME_CALLS;
                    break;
                case FRAME_WHOLE_PARTITION:
                    calls = WHOLE_PARTITION_CALLS;
                    break;
                default:
                    calls = BOUNDED_ROWS_FRAME_CALLS;
                    break;
            }
            final String call = isSkipEnabledGroupRequired
                    ? SKIP_ENABLED_WHOLE_PARTITION_CALLS[o]
                    : frameKinds[w] != FRAME_WHOLE_PARTITION && rnd.nextInt(8) == 0
                      ? RESIDUAL_CALLS[rnd.nextInt(RESIDUAL_CALLS.length)]
                      : calls[rnd.nextInt(calls.length)];
            sql.append(", ").append(call).append(" over ");
            if (rnd.nextBoolean()) {
                sql.append('w').append(w);
            } else {
                sql.append('(').append(specs[w]).append(')');
            }
            sql.append(" c").append(o);
        }
        sql.append(" from ").append(table).append(" window ");
        for (int w = 0; w < windowCount; w++) {
            if (w > 0) {
                sql.append(", ");
            }
            sql.append('w').append(w).append(" as (").append(specs[w]).append(')');
        }
        return sql.toString();
    }
}
