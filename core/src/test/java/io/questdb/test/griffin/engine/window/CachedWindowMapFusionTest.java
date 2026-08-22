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
import io.questdb.griffin.engine.window.WindowAccumulatorDescriptor;
import io.questdb.griffin.engine.window.WindowAccumulatorPlan;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.engine.window.WindowMapState;
import io.questdb.griffin.engine.window.WindowRecordCursorFactory;
import io.questdb.std.Chars;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
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
 * The third is the whole-partition families, whose outputs are not final until the traversal
 * has ended. Their group is driven from two loops - {@code computeNext} in pass 1, which
 * projects nothing, and {@code projectPass2} in pass 2 - and their bound functions'
 * {@code preparePass2} does nothing at all, which is how {@code avg} stops overwriting the sum
 * slot a {@code sum} beside it still needs.
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
    // The two windows above keyed by an expression over two columns rather than by one of
    // them, which is a key the record does not carry: the group evaluates the compiled terms
    // through a virtual record of its own, positioned on whichever row the traversal is on.
    private static final String ORDERED_EXPRESSION_WINDOW =
            " from t window w as (partition by concat(k, k2) order by ts desc "
                    + "rows between unbounded preceding and current row)";
    private static final String NATURAL_EXPRESSION_WINDOW =
            " from t window w as (partition by concat(k, k2) order by ts "
                    + "rows between unbounded preceding and current row)";
    // The bounded-ROWS spellings of the two windows above. Their families are ring-backed: the
    // group's map value addresses a ring in the contributing function's own arena, and a cached
    // traversal reaches that contributor through the sorted chain rather than off the base scan -
    // so the ring is filled in chain order, which is the thing here that a streaming case cannot
    // check.
    private static final String ORDERED_ROWS_FRAME_WINDOW =
            " from t window w as (partition by k order by ts desc rows between 3 preceding and current row)";
    private static final String NATURAL_ROWS_FRAME_WINDOW =
            " from t window w as (partition by k order by ts rows between 3 preceding and current row)";
    // The lagging spelling, where the value entering the frame comes out of the ring as well as
    // the one leaving it.
    private static final String ORDERED_LAGGING_ROWS_FRAME_WINDOW =
            " from t window w as (partition by k order by ts desc rows between 5 preceding and 2 preceding)";
    // The bounded-RANGE windows, whose ring holds (timestamp, value) pairs and grows with the
    // data. There is no ordered spelling of one: a RANGE frame is compiled only where the
    // window's order was dismissed against the base cursor, so such a window is always in the
    // natural-order bucket and something else has to force the cached path. What these add over
    // the ROWS frames above is the resize - a chain-fed contributor grows its slab mid-traversal
    // and the slice's address and read cursor move with it.
    private static final String NATURAL_RANGE_FRAME_WINDOW =
            " from t window w as (partition by k order by ts "
                    + "range between 3_000_000 microseconds preceding and current row)";
    private static final String NATURAL_LAGGING_RANGE_FRAME_WINDOW =
            " from t window w as (partition by k order by ts "
                    + "range between 5_000_000 microseconds preceding and 2_000_000 microseconds preceding)";
    private static final String NATURAL_UNBOUNDED_LO_RANGE_FRAME_WINDOW =
            " from t window w as (partition by k order by ts "
                    + "range between unbounded preceding and 2_000_000 microseconds preceding)";
    // A whole-partition window that needs a sort of its own, so its two-pass group runs inside
    // a sort group's traversal rather than in the natural-order loops. The same ORDER BY as
    // ORDERED_WINDOW, which is what puts the two in one sort bucket.
    private static final String ORDERED_PARTITION_WINDOW = " from t window p as "
            + "(partition by k order by ts desc rows between unbounded preceding and unbounded following)";
    // The whole-partition spelling written inline, so a query needs no window clause at all.
    // Every call over it is two-pass, which is by itself what declines the streaming fast path.
    private static final String PARTITION_WINDOW = " from t";
    // Two windows that agree on their ORDER BY and differ in their partition key: one sort
    // group, two Map subgroups.
    private static final String TWO_WINDOWS = " from t "
            + "window w as (partition by k order by ts desc rows between unbounded preceding and current row), "
            + "w2 as (partition by k2 order by ts desc rows between unbounded preceding and current row)";
    // One sort shared by a cumulative window and a whole-partition one: one sort group, two Map
    // subgroups, and only the second of them driven by the pass-2 traversal.
    private static final String TWO_FRAMES = " from t "
            + "window w as (partition by k order by ts desc rows between unbounded preceding and current row), "
            + "p as (partition by k order by ts desc rows between unbounded preceding and unbounded following)";

    @Test
    public void testABoundedRangeSumAnswersTheSameOnBothCursors() throws Exception {
        // Not a fusion assertion, and here because no fusion assertion can be one. Every other
        // comparison in this class holds the fused arm against the unfused arm of the same cursor,
        // so a shape that answered wrongly on both would pass all of them - and a bounded RANGE
        // {@code sum} did exactly that: the two sum classes over a RANGE frame inherited the avg
        // class's pass1, which writes the average, and pass1 is what the cached cursors call and
        // the streaming one does not. The reference here is therefore the other cursor.
        //
        // Both spellings, because the omission was in both: the partitioned class this box fuses
        // and the unpartitioned one beside it, which owns no map and joins no group.
        assertMemoryLeak(() -> {
            createTable();
            insertRows();
            final String window = " window w as (partition by k order by ts "
                    + "range between 3_000_000 microseconds preceding and current row), "
                    + "u as (order by ts range between 3_000_000 microseconds preceding and current row)";
            final String outputs = "sum(x) over w, avg(x) over w, sum(x) over u, avg(x) over u";
            final String streamingSql = "select ts, " + outputs + " from t" + window;
            // The two arms are a reference for each other only while they land on different
            // cursors, and nothing about the readings themselves says they did. Were a change to
            // what declines the streaming fast path to move this arm onto a cached cursor, the
            // comparison below would hold a cached reading against another cached reading and
            // pass with the cross-cursor property it exists to check no longer under it.
            assertIsStreamingCursor(streamingSql);
            final String streaming = WindowMapStateTest.render(streamingSql);
            for (int light = 0; light < 2; light++) {
                setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, light == 0 ? "false" : "true");
                for (int fusion = 0; fusion < 2; fusion++) {
                    setProperty(
                            PropertyKey.CAIRO_SQL_WINDOW_MAP_FUSION_ENABLED,
                            fusion == 0 ? "true" : "false"
                    );
                    // FORCING_CALL is what moves the same SELECT list onto a cached cursor; its
                    // own column is dropped from the comparison by taking the streaming reference
                    // without it and the cached one with it in front.
                    final String cachedSql = "select ts, " + FORCING_CALL + " forced, " + outputs + " from t" + window;
                    assertCachedFactoryKind(cachedSql, light == 1);
                    final String cached = WindowMapStateTest.render(cachedSql);
                    Assert.assertEquals(
                            "light=" + light + " fusion=" + fusion,
                            WindowMapStateTest.body(streaming),
                            dropSecondColumn(WindowMapStateTest.body(cached))
                    );
                }
            }
        });
    }

    @Test
    public void testAComponentTheGroupCannotPredicateDisablesTheSkip() throws Exception {
        // A group makes one decision per row, so one component whose refused row the group cannot
        // name is enough to turn the skip off for every component beside it. Both halves of that:
        // a count(*) row count, which has no refused row at all, and a count over a SYMBOL, whose
        // predicate is the type's own null test rather than the finite-DOUBLE one the group
        // evaluates.
        assertMemoryLeak(() -> {
            createTable();
            insertRows();
            for (int light = 0; light < 2; light++) {
                setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, light == 0 ? "false" : "true");
                for (String beside : new String[]{"count(*) over (partition by k)", "count(k2) over (partition by k)"}) {
                    final String sql = "select ts, sum(x) over (partition by k), " + beside + PARTITION_WINDOW;
                    try (SqlCompiler compiler = engine.getSqlCompiler();
                         RecordCursorFactory factory = select(compiler, sql, sqlExecutionContext)) {
                        assertFactoryKind(factory, light == 1);
                        final CachedWindowMapGroups groups = groups(factory);
                        assertBoundGroupCount(groups, 1);
                        final WindowMapState state = groups.getStates().getQuick(0);
                        Assert.assertEquals(2, state.getPlan().getComponentCount());
                        Assert.assertFalse(sql, state.isPass1SkipEnabled());
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            final long rows = WindowMapStateTest.drain(cursor);
                            Assert.assertEquals(ROW_COUNT, rows);
                        }
                    }
                }
                assertFusedMatchesUnfused(
                        PARTITION_WINDOW,
                        "",
                        "sum(x) over (partition by k)", "count(*) over (partition by k)"
                );
                assertFusedMatchesUnfused(
                        PARTITION_WINDOW,
                        "",
                        "sum(x) over (partition by k)", "count(k2) over (partition by k)"
                );
            }
        });
    }

    @Test
    public void testAFailedOpenLeavesNoGroupHoldingBacking() throws Exception {
        // The cached open allocates a record chain, a sort buffer, the group maps and - for a
        // group whose pass 1 skips - the buffer its pass 2 projects a missing partition off. A
        // per-query breach can land on the record chain, the sort buffer or the group maps: the
        // cursor binds the per-query tracker on all three, and only an allocation carrying that
        // tracker checks the cap. Whichever it lands on, every group's map has to read shut
        // afterwards - which the assertion below does - because the breach either beat of() to
        // the group entirely or broke the group's own map.reopen(), which assigns the backing
        // pointer isMapOpen() reads only once its malloc has returned. The close getCursor()
        // runs on the failed open then hands back every allocation the open did make, which
        // assertMemoryLeak measures and the successful drain afterwards says was not achieved
        // by staying shut.
        //
        // Both shapes, because a failed open has to leave both kinds of group holding nothing:
        // the cumulative one owns a map alone, and the whole-partition one owns a map and an
        // identity buffer.
        //
        // The identity buffer is the one backing this cap says nothing about, so nothing here
        // asserts over it. It takes Unsafe's untracked malloc overload, which answers to the
        // global RSS ceiling alone, so the per-query cap cannot break that malloc itself - and no
        // breach reaches it either, because reopen() allocates the map's backing first and the
        // 64-byte cap below always breaks that tracked malloc. Every breach this test can reach
        // therefore lands before the group holds a buffer at all, and an assertion over it would
        // read back the zero it started from rather than any unwind. Two other tests cover the
        // buffer where a failure does reach it: testAnIdentityBufferThatCannotBeAllocatedGivesTheMapBack
        // arms a global RSS ceiling and reads reopen()'s own unwind, and
        // testAPartitionRefusedWholeStaysOutOfTheMap reads reset() taking the buffer back off a
        // group that holds one.
        assertMemoryLeak(() -> {
            createTable();
            insertRows();
            setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, "false");
            final String skipping = "select ts, sum(x) over (partition by k), avg(x) over (partition by k)"
                    + PARTITION_WINDOW;
            for (String sql : new String[]{orderedSumAndCount(), skipping}) {
                try (SqlCompiler compiler = engine.getSqlCompiler();
                     RecordCursorFactory factory = select(compiler, sql, sqlExecutionContext)) {
                    final CachedWindowMapGroups groups = groups(factory);
                    assertBoundGroupCount(groups, 1);
                    final boolean isSkipping = groups.getStates().getQuick(0).isPass1SkipEnabled();
                    Assert.assertEquals(sql, sql.equals(skipping), isSkipping);
                    setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 64L);
                    for (int i = 0; i < 5; i++) {
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            WindowMapStateTest.drain(cursor);
                            Assert.fail("expected a per-query memory breach");
                        } catch (CairoException e) {
                            Assert.assertTrue(
                                    "expected isOutOfMemory(), got: " + e.getFlyweightMessage(),
                                    e.isOutOfMemory()
                            );
                        }
                        final ObjList<WindowMapState> states = groups.getStates();
                        for (int g = 0, n = states.size(); g < n; g++) {
                            final WindowMapState state = states.getQuick(g);
                            Assert.assertFalse("group " + g + " kept its map open", state.isMapOpen());
                        }
                    }
                    Assert.assertEquals("busy reader count", 0, engine.getBusyReaderCount());
                    setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0L);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.assertEquals(ROW_COUNT, WindowMapStateTest.drain(cursor));
                    }
                }
            }
        });
    }

    @Test
    public void testAnIdentityBufferThatCannotBeAllocatedGivesTheMapBack() throws Exception {
        // reopen() allocates the map's backing and then, for a skipping group, the identity
        // buffer - and only the second of those has an owner problem. The group is half-open
        // when it fails, and a caller that never saw the group open has no reason to reset it,
        // so reopen() closes the map itself on the way out.
        //
        // Driving the group directly is what makes that observable. A cursor's own close
        // resets every group whatever reopen() managed, so an end-to-end breach reads the same
        // whether or not reopen() unwinds; here nothing but reopen()'s own catch can close the
        // map. The ceiling is the global RSS one rather than the per-query cap, because the cap
        // reaches only allocations that carry the per-query tracker and the identity buffer
        // takes the untracked overload - see testAFailedOpenLeavesNoGroupHoldingBacking.
        assertMemoryLeak(() -> {
            createTable();
            insertRows();
            setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, "false");
            final String sql = "select ts, sum(x) over (partition by k), avg(x) over (partition by k)"
                    + PARTITION_WINDOW;
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = select(compiler, sql, sqlExecutionContext)) {
                final CachedWindowMapGroups groups = groups(factory);
                assertBoundGroupCount(groups, 1);
                final WindowMapState state = groups.getStates().getQuick(0);
                // Only a skipping group carries an identity buffer at all, so only one of them
                // can reach the malloc under test.
                Assert.assertTrue(
                        "the group does not skip, so reopen() allocates no buffer",
                        state.isPass1SkipEnabled()
                );
                Assert.assertFalse("the group allocated before a cursor opened it", state.isMapOpen());

                // No memory tracker is bound, so the map's tracked malloc degrades to the
                // untracked overload the buffer already takes, and the global ceiling armed
                // below is the only limit either of them answers to.
                //
                // What one whole open costs, measured rather than assumed - the map's backing
                // plus the buffer, both charged to the global RSS counter. The buffer is the
                // only allocation of the open that takes NATIVE_DEFAULT, so that tag's own
                // counter splits the cost in two and says which ceilings the map still clears.
                final long usedBeforeOpen = Unsafe.getRssMemUsed();
                final long defaultTagBeforeOpen = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT);
                state.reopen();
                final long openCost = Unsafe.getRssMemUsed() - usedBeforeOpen;
                final long bufferCost = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT) - defaultTagBeforeOpen;
                final long mapCost = openCost - bufferCost;
                state.reset();
                Assert.assertTrue("a successful open charged the RSS counter nothing", openCost > 0);
                Assert.assertTrue(
                        "NATIVE_DEFAULT took " + bufferCost + " of the open's " + openCost + ", so the tag no "
                                + "longer separates the identity buffer's cost from the map's",
                        bufferCost > 0 && bufferCost < openCost
                );

                // A ceiling that admits the map and refuses the buffer sits somewhere in
                // [0, openCost], and where exactly is how the two allocations split that cost -
                // which the tag counter above just measured. A ceiling of mapCost - 1 is the
                // highest one the map's own malloc still has to break, and one of mapCost is the
                // lowest that admits the map and leaves the buffer's malloc the first allocation
                // over the line. So the walk starts on those two rather than climbing to them
                // from zero: the counter belongs to the whole process, and every ceiling this
                // arms below real usage is one another thread's allocation can break on instead.
                //
                // The measured split seeds the walk rather than predicting its answer. An
                // allocation or a free elsewhere between the reading of the counter and the open
                // moves the transition off the measured byte, so each probe reads which malloc
                // actually broke the ceiling off the exception's own memoryTag and steps the
                // next ceiling toward whichever breach is still missing. Two probes is what a
                // still counter costs; the budget is headroom for a moving one, and the
                // assertions below fail loudly rather than quietly if it runs out with a breach
                // unseen.
                final int probeBudget = 64;
                boolean hasMapMallocFailed = false;
                boolean hasIdentityMallocFailed = false;
                long slack = mapCost - 1;
                int probe = 0;
                final long savedRssLimit = Unsafe.getRssMemLimit();
                try {
                    for (; probe < probeBudget && slack > 0
                            && !(hasMapMallocFailed && hasIdentityMallocFailed); probe++) {
                        boolean hasOpenBreached = false;
                        Unsafe.setRssMemLimit(Unsafe.getRssMemUsed() + slack);
                        try {
                            state.reopen();
                        } catch (CairoException e) {
                            hasOpenBreached = true;
                            Assert.assertTrue("expected an out-of-memory error", e.isOutOfMemory());
                            TestUtils.assertContains(e.getFlyweightMessage(), "global RSS memory limit exceeded");
                            if (Chars.contains(e.getFlyweightMessage(), "memoryTag=" + MemoryTag.NATIVE_DEFAULT + "]")) {
                                hasIdentityMallocFailed = true;
                                // The whole of what the branch exists for: reopen() allocated
                                // the map's backing at the top of the same call, and gave it
                                // back rather than leaving it on a group nobody has a reason
                                // to reset.
                                Assert.assertFalse(
                                        "reopen() left the map open after the identity buffer's malloc failed",
                                        state.isMapOpen()
                                );
                                Assert.assertFalse(state.isIdentityValueAllocated());
                            } else {
                                hasMapMallocFailed = true;
                            }
                        } finally {
                            Unsafe.setRssMemLimit(savedRssLimit);
                            // Whichever way the open went, the group holds nothing before the
                            // next ceiling: a success allocated both and this hands both back,
                            // and a breach left the group closed already.
                            state.reset();
                        }
                        // Step toward the breach still missing: halve the slack while the map's
                        // is missing, since every ceiling below the split breaks the map's
                        // malloc; walk up a byte at a time while the buffer's is missing and the
                        // map still breaks, since that is where a counter that moved up carries
                        // the transition - and clamp that step to mapCost, so a walk that halving
                        // took below the measured split resumes at it rather than grinding back up
                        // a byte at a time; and give a byte back when the open cleared the ceiling
                        // outright, which says the transition moved the other way.
                        if (!hasMapMallocFailed) {
                            slack /= 2;
                        } else if (hasOpenBreached) {
                            slack = Math.max(slack + 1, mapCost);
                        } else {
                            slack--;
                        }
                    }
                } finally {
                    Unsafe.setRssMemLimit(savedRssLimit);
                }
                // The map's own breach is what says the tag still tells the two mallocs apart.
                // Were the map to take NATIVE_DEFAULT too, every reading above would be the
                // map's and the assertion this test rests on would never run.
                Assert.assertTrue(
                        "the walk armed " + probe + " of its " + probeBudget + " ceilings around the measured map "
                                + "cost " + mapCost + " and stopped at slack " + slack + " without one breaking on "
                                + "the map's malloc, so it either ran out of probes against a counter the whole "
                                + "process moves or the map's malloc now carries NATIVE_DEFAULT like the identity "
                                + "buffer's (identity breach seen: " + hasIdentityMallocFailed + ")",
                        hasMapMallocFailed
                );
                Assert.assertTrue(
                        "the walk armed " + probe + " of its " + probeBudget + " ceilings around the measured map "
                                + "cost " + mapCost + " and stopped at slack " + slack + " without one making the "
                                + "identity buffer's malloc the failing one, so the branch under test never ran - "
                                + "the walk either ran out of probes against a counter the whole process moves or "
                                + "the buffer's malloc no longer carries NATIVE_DEFAULT",
                        hasIdentityMallocFailed
                );
                // And the group is reusable: with the ceiling down the same reopen() allocates
                // both again, which is what says the unwound open left it closed rather than
                // inconsistent.
                state.reopen();
                Assert.assertTrue(state.isMapOpen());
                Assert.assertTrue(state.isIdentityValueAllocated());
                state.reset();
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertEquals(ROW_COUNT, WindowMapStateTest.drain(cursor));
                }
            }
        });
    }

    @Test
    public void testAPartitionRefusedWholeStaysOutOfTheMap() throws Exception {
        // The one shape the pass-1 skip changes about the map: a partition whose every row its
        // component refuses is not in the map when pass 1 ends, and pass 2 does not put it back.
        // What the outputs read for it is the identity a partition nothing contributed to would
        // have been left sitting at anyway - a NULL sum, a NULL average and a zero count - read
        // off the group's own buffer, which is what the reference arm below is asserting as
        // well. The map is therefore the contributing partitions rather than every partition the
        // traversal saw, and a row of a refused-whole partition costs one failed lookup and no
        // insertion.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE s (ts TIMESTAMP, k INT, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            // Two partitions refused whole rather than one, because one buffer serves every
            // missing partition: were a projection to leave anything in it, key 3 would read
            // what key 2 left there.
            execute("""
                    INSERT INTO s VALUES
                    ('2024-01-01T00:00:00.000000Z', 1, 1.0),
                    ('2024-01-01T00:00:01.000000Z', 2, null),
                    ('2024-01-01T00:00:02.000000Z', 2, 'Infinity'::double),
                    ('2024-01-01T00:00:03.000000Z', 1, 4.0),
                    ('2024-01-01T00:00:04.000000Z', 2, null),
                    ('2024-01-01T00:00:05.000000Z', 3, null),
                    ('2024-01-01T00:00:06.000000Z', 3, '-Infinity'::double)""");
            final String sql = "SELECT k, sum(x) OVER (PARTITION BY k) AS s, avg(x) OVER (PARTITION BY k) AS a, "
                    + "count(x) OVER (PARTITION BY k) AS c FROM s";
            for (int light = 0; light < 2; light++) {
                setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, light == 0 ? "false" : "true");
                try (SqlCompiler compiler = engine.getSqlCompiler();
                     RecordCursorFactory factory = select(compiler, sql, sqlExecutionContext)) {
                    assertFactoryKind(factory, light == 1);
                    final CachedWindowMapGroups groups = groups(factory);
                    assertBoundGroupCount(groups, 1);
                    final WindowMapState state = groups.getStates().getQuick(0);
                    Assert.assertTrue(state.isPass1SkipEnabled());
                    state.setMemoryTracker(sqlExecutionContext.getMemoryTracker());
                    state.reopen();
                    try {
                        final int[] key = new int[1];
                        final int[] keyReads = new int[1];
                        final double[] value = new double[1];
                        final Record record = new Record() {
                            @Override
                            public double getDouble(int col) {
                                return value[0];
                            }

                            @Override
                            public int getInt(int col) {
                                keyReads[0]++;
                                return key[0];
                            }
                        };
                        final int[] keys = {1, 2, 2, 1, 2, 3, 3};
                        final double[] values = {
                                1.0, Double.NaN, Double.POSITIVE_INFINITY, 4.0, Double.NaN,
                                Double.NaN, Double.NEGATIVE_INFINITY
                        };
                        for (int i = 0; i < keys.length; i++) {
                            key[0] = keys[i];
                            value[0] = values[i];
                            state.computeNext(record);
                        }
                        // Pass 1 saw three partition keys, but keys 2 and 3 held only rows the
                        // group refused. Their absence from the map directly proves that
                        // computeNext took the skip branch for those rows.
                        Assert.assertEquals(1, state.getMapSize());
                        // Twice over, which is what says pass 2 leaves no state of its own: the
                        // second drain reads the map the first one left and finds it unchanged,
                        // which is what a cached cursor's random access and its second drain
                        // both need.
                        for (int pass = 0; pass < 2; pass++) {
                            keyReads[0] = 0;
                            for (int i = 0; i < keys.length; i++) {
                                key[0] = keys[i];
                                value[0] = values[i];
                                state.projectPass2(record);
                            }
                            // One key read a row and no more: a miss projects the identity off
                            // the group's own buffer, so it writes no second key and creates no
                            // entry. The old shape read one key extra per refused-whole
                            // partition and grew the map by one entry for each.
                            Assert.assertEquals("pass " + pass, keys.length, keyReads[0]);
                            // And the partitions the skip left out stay out. The map a skipping
                            // group ends with holds the contributing partitions alone, which is
                            // the whole saving on an input whose keys are mostly refused.
                            Assert.assertEquals("pass " + pass, 1, state.getMapSize());
                        }
                        // The buffer is the group's for as long as its map is, which is what
                        // the reset below then takes back.
                        Assert.assertTrue(state.isIdentityValueAllocated());
                    } finally {
                        state.reset();
                    }
                    Assert.assertFalse(state.isIdentityValueAllocated());
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.assertEquals(7, WindowMapStateTest.drain(cursor));
                    }
                }
                // Read twice over, which is what says the identity projection is idempotent: the
                // second cursor projects the same identity off the same buffer, for both of the
                // partitions that have no entry to read it from.
                assertQuery(sql).expectSize().returns("""
                        k\ts\ta\tc
                        1\t5.0\t2.5\t2
                        2\tnull\tnull\t0
                        2\tnull\tnull\t0
                        1\t5.0\t2.5\t2
                        2\tnull\tnull\t0
                        3\tnull\tnull\t0
                        3\tnull\tnull\t0
                        """);
            }
        });
    }

    @Test
    public void testASortSharedByACumulativeAndAWholePartitionGroup() throws Exception {
        // One ORDER BY, two frames, two Map subgroups - and only one of them has anything left
        // to do when the pass-2 traversal of that sort group runs. The cumulative group's
        // outputs were final row by row and it is absent from the pass-2 list; the
        // whole-partition one is in both lists.
        assertMemoryLeak(() -> {
            createTable();
            insertRows();
            for (int light = 0; light < 2; light++) {
                setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, light == 0 ? "false" : "true");
                final String sql = "select ts, sum(x) over w, count(y) over w, sum(x) over p, avg(x) over p"
                        + TWO_FRAMES;
                try (SqlCompiler compiler = engine.getSqlCompiler();
                     RecordCursorFactory factory = select(compiler, sql, sqlExecutionContext)) {
                    assertFactoryKind(factory, light == 1);
                    final CachedWindowMapGroups groups = groups(factory);
                    assertBoundGroupCount(groups, 2);
                    Assert.assertNotNull(groups.getOrderedStates(0));
                    Assert.assertEquals(2, groups.getOrderedStates(0).size());
                    Assert.assertNotNull(groups.getOrderedPass2States(0));
                    Assert.assertEquals(1, groups.getOrderedPass2States(0).size());
                    Assert.assertNull(groups.getUnorderedPass2States());
                    final WindowMapState pass2State = groups.getOrderedPass2States(0).getQuick(0);
                    Assert.assertTrue(pass2State.isTwoPass());
                    final WindowMapState cumulative = otherState(groups, pass2State);
                    Assert.assertFalse(cumulative.isTwoPass());
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        final long rows = WindowMapStateTest.drain(cursor);
                        Assert.assertEquals(ROW_COUNT, rows);
                        // The whole-partition component can refuse non-finite x values in pass 1.
                        // The cumulative group beside it cannot skip because it projects from
                        // the value it loads whether or not the row contributed.
                        Assert.assertTrue(pass2State.isPass1SkipEnabled());
                        Assert.assertFalse(cumulative.isPass1SkipEnabled());
                    }
                }
                assertFusedMatchesUnfused(
                        TWO_FRAMES,
                        "",
                        "sum(x) over w", "count(y) over w", "sum(x) over p", "avg(x) over p"
                );
            }
        });
    }

    @Test
    public void testAWholePartitionGroupIsDrivenFromBothTraversals() throws Exception {
        // A two-pass group is listed twice, and by the traversal rather than by the function:
        // once in its bucket's pass-1 list, where computeNext fills it, and once in the pass-2
        // list, where projectPass2 empties it into the rows. Both spellings of the bucket - a
        // whole-partition window needing no sort, and one carrying an ORDER BY of its own.
        assertMemoryLeak(() -> {
            createTable();
            insertRows();
            for (int light = 0; light < 2; light++) {
                setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, light == 0 ? "false" : "true");
                final String natural = "select ts, sum(x) over (partition by k), count(y) over (partition by k)"
                        + PARTITION_WINDOW;
                try (SqlCompiler compiler = engine.getSqlCompiler();
                     RecordCursorFactory factory = select(compiler, natural, sqlExecutionContext)) {
                    assertFactoryKind(factory, light == 1);
                    final CachedWindowMapGroups groups = groups(factory);
                    assertBoundGroupCount(groups, 1);
                    final WindowMapState state = groups.getStates().getQuick(0);
                    Assert.assertTrue(state.isTwoPass());
                    Assert.assertNull(groups.getOrderedStates(0));
                    Assert.assertNull(groups.getOrderedPass2States(0));
                    Assert.assertNotNull(groups.getForwardUnorderedStates());
                    Assert.assertEquals(1, groups.getForwardUnorderedStates().size());
                    Assert.assertSame(state, groups.getForwardUnorderedStates().getQuick(0));
                    Assert.assertNotNull(groups.getUnorderedPass2States());
                    Assert.assertEquals(1, groups.getUnorderedPass2States().size());
                    Assert.assertSame(state, groups.getUnorderedPass2States().getQuick(0));
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.assertEquals(ROW_COUNT, WindowMapStateTest.drain(cursor));
                    }
                }
                final String ordered = "select ts, sum(x) over p, count(y) over p" + ORDERED_PARTITION_WINDOW;
                try (SqlCompiler compiler = engine.getSqlCompiler();
                     RecordCursorFactory factory = select(compiler, ordered, sqlExecutionContext)) {
                    final CachedWindowMapGroups groups = groups(factory);
                    assertBoundGroupCount(groups, 1);
                    final WindowMapState state = groups.getStates().getQuick(0);
                    Assert.assertNotNull(groups.getOrderedStates(0));
                    Assert.assertSame(state, groups.getOrderedStates(0).getQuick(0));
                    Assert.assertNotNull(groups.getOrderedPass2States(0));
                    Assert.assertSame(state, groups.getOrderedPass2States(0).getQuick(0));
                    Assert.assertNull(groups.getForwardUnorderedStates());
                    Assert.assertNull(groups.getUnorderedPass2States());
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.assertEquals(ROW_COUNT, WindowMapStateTest.drain(cursor));
                    }
                }
            }
        });
    }

    @Test
    public void testEveryComponentMustRefuseARowBeforePassOneSkipsIt() throws Exception {
        // Two components over two columns, so the group's decision is theirs jointly. Three rows
        // of the fixture hold a non-finite x and two a NULL y, and no row holds both - so the
        // group skips nothing even though one of its two components refuses a third of the scan,
        // and the unfused reference is what says the answers are unmoved by that.
        assertMemoryLeak(() -> {
            createTable();
            insertRows();
            for (int light = 0; light < 2; light++) {
                setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, light == 0 ? "false" : "true");
                final String sql = "select ts, sum(x) over (partition by k), avg(y) over (partition by k)"
                        + PARTITION_WINDOW;
                try (SqlCompiler compiler = engine.getSqlCompiler();
                     RecordCursorFactory factory = select(compiler, sql, sqlExecutionContext)) {
                    assertFactoryKind(factory, light == 1);
                    final CachedWindowMapGroups groups = groups(factory);
                    assertBoundGroupCount(groups, 1);
                    final WindowMapState state = groups.getStates().getQuick(0);
                    Assert.assertEquals(2, state.getPlan().getComponentCount());
                    Assert.assertTrue(state.isPass1SkipEnabled());
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        final long rows = WindowMapStateTest.drain(cursor);
                        Assert.assertEquals(ROW_COUNT, rows);
                    }
                }
                assertFusedMatchesUnfused(
                        PARTITION_WINDOW,
                        "",
                        "sum(x) over (partition by k)", "avg(y) over (partition by k)"
                );
            }
        });
    }

    @Test
    public void testEveryFusibleShapeMatchesTheUnfusedPath() throws Exception {
        // The differential, over every cumulative family the build admits, in both buckets and
        // on both factories. The reference is this tree's unfused cached path rather than a
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
                    // The extremum families, whose empty state is the one thing in this build
                    // that is not a zeroed slice: a partition no row has contributed to has to
                    // read back as NULL, which is what a cached traversal's second drain and
                    // its random access are most likely to expose. ts is here as an argument
                    // as well as an ORDER BY term, which is the one column the chain and the
                    // base disagree about the position of.
                    assertFusedMatchesUnfused(window, lead, "max(x) over w", "min(x) over w");
                    assertFusedMatchesUnfused(
                            window,
                            lead,
                            "sum(x) over w",
                            "max(x) over w",
                            "min(ts) over w"
                    );
                    // The compensated sum beside the plain one and the counter they share:
                    // two totals over one argument that must not be read off one slot, and a
                    // count that may be read off either.
                    assertFusedMatchesUnfused(
                            window,
                            lead,
                            "ksum(x) over w",
                            "sum(x) over w",
                            "count(x) over w"
                    );
                    // The DECIMAL extrema, whose slot is the argument's own payload: a LONG for
                    // d64 and a DECIMAL128 for d128. The counter in front of the wide one is
                    // what puts it at a slot base other than zero, which is the reading a
                    // chain-fed traversal is as able to get wrong as a streaming one.
                    assertFusedMatchesUnfused(window, lead, "max(d64) over w", "min(d64) over w");
                    assertFusedMatchesUnfused(
                            window,
                            lead,
                            "count(d128) over w",
                            "max(d128) over w",
                            "min(d128) over w"
                    );
                    // The capture families, which keep one row's value rather than a summary of
                    // many - so which row the traversal reached first is the whole of their
                    // state, and a bucket fed by the sorted chain rather than by the base scan is
                    // where a wrong answer to that would show. The three spellings are three
                    // components over one column, and the flag two of them carry is what a second
                    // drain would expose if it were an isNew() reading instead.
                    assertFusedMatchesUnfused(
                            window,
                            lead,
                            "first_value(x) over w",
                            "first_value(x) ignore nulls over w",
                            "last_value(x) ignore nulls over w"
                    );
                    assertFusedMatchesUnfused(
                            window,
                            lead,
                            "sum(x) over w",
                            "first_value(x) over w",
                            "first_value(ts) ignore nulls over w"
                    );
                    // An expression key, in both buckets. What it adds is the one thing a
                    // cached traversal does to a key that a streaming one does not: the terms
                    // are evaluated against the sorted chain record rather than the base scan's,
                    // through a virtual record the group positions on every row of both passes.
                    final String expressionWindow = natural == 0
                            ? ORDERED_EXPRESSION_WINDOW
                            : NATURAL_EXPRESSION_WINDOW;
                    assertFusedMatchesUnfused(
                            expressionWindow,
                            lead,
                            "sum(x) over w",
                            "avg(x) over w",
                            "count(y) over w"
                    );
                }
                // The ring-backed families, in all three bucket spellings a bounded frame reaches
                // here. What they add to the differential is an accumulator whose state is partly
                // in an arena the group does not own: the chain feeds the contributor in sort
                // order, and a ring filled out of order or carried across a bucket would show as
                // a wrong frame rather than as a wrong slot.
                for (int rows = 0; rows < 3; rows++) {
                    final String window = rows == 0
                            ? ORDERED_ROWS_FRAME_WINDOW
                            : (rows == 1 ? NATURAL_ROWS_FRAME_WINDOW : ORDERED_LAGGING_ROWS_FRAME_WINDOW);
                    final String lead = rows == 1 ? ", " + FORCING_CALL : "";
                    assertFusedMatchesUnfused(window, lead, "sum(x) over w", "avg(x) over w");
                    assertFusedMatchesUnfused(
                            window,
                            lead,
                            "sum(x) over w",
                            "avg(x) over w",
                            "count(x) over w",
                            "count(y) over w"
                    );
                }
                // The bounded-RANGE families, in the three geometries their ring bookkeeping takes.
                // All three are natural-order windows and so all three need the forcing call - see
                // the constants. The counter beside the pair is the shape whose two rings are
                // filled by two contributors off one chain record.
                for (int range = 0; range < 3; range++) {
                    final String window = range == 0
                            ? NATURAL_RANGE_FRAME_WINDOW
                            : (range == 1
                               ? NATURAL_LAGGING_RANGE_FRAME_WINDOW
                               : NATURAL_UNBOUNDED_LO_RANGE_FRAME_WINDOW);
                    final String lead = ", " + FORCING_CALL;
                    assertFusedMatchesUnfused(window, lead, "sum(x) over w", "avg(x) over w");
                    assertFusedMatchesUnfused(
                            window,
                            lead,
                            "sum(x) over w",
                            "avg(x) over w",
                            "count(x) over w",
                            "count(y) over w"
                    );
                }
            }
        });
    }

    @Test
    public void testEveryWholePartitionShapeMatchesTheUnfusedPath() throws Exception {
        // The same differential over the two-pass families, in both bucket spellings and on
        // both factories. The references are the unfused whole-partition path: asked for on
        // its own each call keeps its own map, its own two probes a row and - for avg - the
        // destructive preparePass2 that replaces the sum with the average, which is exactly
        // the arithmetic the fused arm has to reproduce without performing it.
        assertMemoryLeak(() -> {
            createTable();
            insertRows();
            for (int light = 0; light < 2; light++) {
                setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, light == 0 ? "false" : "true");
                for (int ordered = 0; ordered < 2; ordered++) {
                    final String window = ordered == 0 ? PARTITION_WINDOW : ORDERED_PARTITION_WINDOW;
                    final String over = ordered == 0 ? " over (partition by k)" : " over p";
                    assertFusedMatchesUnfused(
                            window,
                            "",
                            "sum(x)" + over,
                            "avg(x)" + over,
                            "count(x)" + over
                    );
                    assertFusedMatchesUnfused(window, "", "sum(x)" + over, "count(y)" + over);
                    assertFusedMatchesUnfused(window, "", "count(*)" + over, "count(k)" + over);
                    // The same over an expression key, which is the shape that probes a
                    // borrowed key projection twice a row: once as pass 1 absorbs the row and
                    // once as pass 2 materializes the outputs off the finished accumulator.
                    final String expressionOver = ordered == 0
                            ? " over (partition by concat(k, k2))"
                            : " over q";
                    final String expressionWindow = ordered == 0
                            ? PARTITION_WINDOW
                            : " from t window q as (partition by concat(k, k2) order by ts desc "
                              + "rows between unbounded preceding and unbounded following)";
                    assertFusedMatchesUnfused(
                            expressionWindow,
                            "",
                            "sum(x)" + expressionOver,
                            "avg(x)" + expressionOver,
                            "count(x)" + expressionOver
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
        // The acceptance shape, read off a sorted traversal: the three projections share one
        // component and its two slots, so one contributor evaluates x for the group.
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
                        final long rows = WindowMapStateTest.drain(cursor);
                        Assert.assertEquals(ROW_COUNT, rows);
                    }
                }
            }
        });
    }

    @Test
    public void testNaturalOrderFunctionsFormTheirOwnGroupBesideATwoPassResidual() throws Exception {
        // The natural-order bucket: the group is driven by the base scan that fills the chain,
        // beside a whole-partition avg that keeps its own map and its own two passes - not
        // because its family is unfusible, which it no longer is, but because it is the only
        // call over its own window and moving one map is not removing one. A two-pass function
        // is what forces the cached path here, so this is also the shape that says a residual
        // and a group share a cursor without sharing anything else.
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
                        final long rows = WindowMapStateTest.drain(cursor);
                        Assert.assertEquals(ROW_COUNT, rows);
                    }
                }
            }
        });
    }

    @Test
    public void testOrderedSortGroupSharesOneMap() throws Exception {
        // The headline shape on a sorted traversal. sum(x) counts finite x values and count(y)
        // counts non-null y values, so the two keep separate counters; what they share is the
        // key domain and hash table.
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
                        final long rows = WindowMapStateTest.drain(cursor);
                        Assert.assertEquals(ROW_COUNT, rows);
                        Assert.assertNotNull(state.getMapImplementation());
                        Assert.assertTrue(state.getUnorderedMapMaxEntrySize() > 0);
                    }
                    Assert.assertFalse("close left the group's backing allocated", state.isMapOpen());
                    // Ten more cycles inside the leak check: the map is allocated under the
                    // per-query tracker at open and handed back at close, so the counter has
                    // to net to zero however many times the factory is re-executed.
                    for (int i = 0; i < 10; i++) {
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            Assert.assertEquals("iteration " + i, ROW_COUNT, WindowMapStateTest.drain(cursor));
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
        // window spec is what keeps them apart.
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
                        WindowMapStateTest.drain(cursor);
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
                final String fused = WindowMapStateTest.render(orderedSumAndCount());
                setProperty(PropertyKey.CAIRO_SQL_WINDOW_MAP_FUSION_ENABLED, "false");
                try (SqlCompiler compiler = engine.getSqlCompiler();
                     RecordCursorFactory factory = select(compiler, orderedSumAndCount(), sqlExecutionContext)) {
                    final CachedWindowMapGroups groups = groups(factory);
                    Assert.assertNotNull(groups);
                    Assert.assertEquals(1, groups.getPlans().size());
                    Assert.assertEquals(2, groups.getPlans().getQuick(0).getComponentCount());
                    Assert.assertEquals(0, groups.getStates().size());
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.assertEquals(ROW_COUNT, WindowMapStateTest.drain(cursor));
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
                Assert.assertEquals(fused, WindowMapStateTest.render(orderedSumAndCount()));
            }
        });
    }

    @Test
    public void testTheKillSwitchRestoresTheDestructiveFinalization() throws Exception {
        // The switch has more to turn back on for a whole-partition shape than for a
        // cumulative one: with it off, avg's preparePass2 walks its own map again and replaces
        // every partition's sum slot with the average. That is only safe because each function
        // is back on a map of its own, which is what the case asserts beside the rows.
        assertMemoryLeak(() -> {
            createTable();
            insertRows();
            final String sql = "select ts, sum(x) over (partition by k), avg(x) over (partition by k)"
                    + PARTITION_WINDOW;
            for (int light = 0; light < 2; light++) {
                setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, light == 0 ? "false" : "true");
                setProperty(PropertyKey.CAIRO_SQL_WINDOW_MAP_FUSION_ENABLED, "true");
                final String fused = WindowMapStateTest.render(sql);
                setProperty(PropertyKey.CAIRO_SQL_WINDOW_MAP_FUSION_ENABLED, "false");
                try (SqlCompiler compiler = engine.getSqlCompiler();
                     RecordCursorFactory factory = select(compiler, sql, sqlExecutionContext)) {
                    final CachedWindowMapGroups groups = groups(factory);
                    Assert.assertNotNull(groups);
                    Assert.assertEquals(1, groups.getPlans().size());
                    Assert.assertEquals(0, groups.getStates().size());
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.assertEquals(ROW_COUNT, WindowMapStateTest.drain(cursor));
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
                Assert.assertEquals(fused, WindowMapStateTest.render(sql));
            }
        });
    }

    @Test
    public void testRefusedPartitionProjectsSumCountIdentityBesideResidualOutputs() throws Exception {
        // The one identity a skipping group reaches today, projected for a partition pass 1
        // refused whole, beside eight outputs the group declined. Under a whole-partition frame
        // ksum, the two DOUBLE extrema, the ignore-nulls first capture and the four Welford
        // outputs compile to their *OverPartitionFunction classes, and none of those declares an
        // accumulator projection - so WindowAccumulatorCandidate.of turns each away and each
        // answers the refused partition through its own map. What stays in the group is sum, avg
        // and a count derived off the same counter slot: one FAMILY_DOUBLE_SUM_COUNT component,
        // whose identity is the zeroed slice, which the plan assertions below pin down.
        // isRefusedRowInert() admits seven families; this one and the non-null count are the
        // only two any two-pass function declares, so this suite cannot build a skipping group
        // that carries one of the other five. It does build ordinary groups that carry such a
        // family - the extremum pair and the four Welford outputs over the cumulative window
        // above are two - but every class declaring one of the five reports ZERO_PASS, so a
        // group carrying one is never two-pass and never skips - see that method's javadoc.
        // The reference arm is the same eleven outputs unfused, which compute the refused
        // partition's answers through each function's own map instead.
        assertMemoryLeak(() -> {
            createTable();
            insertRows();
            final String[] outputs = {
                    "sum(x) over (partition by k)",
                    "avg(x) over (partition by k)",
                    "ksum(x) over (partition by k)",
                    "max(x) over (partition by k)",
                    "min(x) over (partition by k)",
                    "first_value(x) ignore nulls over (partition by k)",
                    "count(x) over (partition by k)",
                    "var_samp(x) over (partition by k)",
                    "var_pop(x) over (partition by k)",
                    "stddev_samp(x) over (partition by k)",
                    "stddev_pop(x) over (partition by k)",
            };
            final StringBuilder sql = new StringBuilder("select ts");
            for (String output : outputs) {
                sql.append(", ").append(output);
            }
            sql.append(PARTITION_WINDOW);
            for (int light = 0; light < 2; light++) {
                setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, light == 0 ? "false" : "true");
                try (SqlCompiler compiler = engine.getSqlCompiler();
                     RecordCursorFactory factory = select(compiler, sql.toString(), sqlExecutionContext)) {
                    assertFactoryKind(factory, light == 1);
                    final CachedWindowMapGroups groups = groups(factory);
                    assertBoundGroupCount(groups, 1);
                    final WindowMapState state = groups.getStates().getQuick(0);
                    // The shape the group really has, which is the whole of what the identity
                    // path here covers: one component, the sum family's, and its two slots.
                    // Three of the eleven outputs project off it - sum, avg and the derived
                    // count - and the assertion on how many functions the factory left bound is
                    // what says the other eight sit outside.
                    final WindowAccumulatorPlan plan = state.getPlan();
                    Assert.assertEquals(1, plan.getComponentCount());
                    Assert.assertEquals(
                            WindowAccumulatorDescriptor.FAMILY_DOUBLE_SUM_COUNT,
                            plan.getComponent(0).getFamily()
                    );
                    Assert.assertEquals(2, plan.getSlotCount());
                    Assert.assertEquals(3, plan.getProjectionCount());
                    final ObjList<WindowFunction> functions = windowFunctions(factory);
                    Assert.assertEquals(outputs.length, functions.size());
                    int boundCount = 0;
                    for (int i = 0, n = functions.size(); i < n; i++) {
                        if (functions.getQuick(i).isWindowStateOwned()) {
                            boundCount++;
                        }
                    }
                    Assert.assertEquals("the group's membership moved", 3, boundCount);
                    // The two properties that put these outputs on the identity path at all: the
                    // group is driven twice, and pass 1 is allowed to leave a refused row's key
                    // out of the map. Were a family here not inert on a refused row, the skip
                    // would be off for the whole group and this test would prove nothing.
                    Assert.assertTrue(state.isTwoPass());
                    Assert.assertTrue(state.isPass1SkipEnabled());
                }
                assertFusedMatchesUnfused(PARTITION_WINDOW, "", outputs);
                // And what the refused partition answers, output by output: SQL NULL everywhere
                // but the counter, which is exact and zero. The group projects the first three
                // off its identity buffer and every other output computes its own; these are the
                // two rows the map no longer holds an entry for, read twice over by the
                // assertion itself.
                assertQuery("""
                        SELECT * FROM (
                          SELECT k, sum(x) OVER w AS s, avg(x) OVER w AS a, ksum(x) OVER w AS ks,
                                 max(x) OVER w AS mx, min(x) OVER w AS mn,
                                 first_value(x) IGNORE NULLS OVER w AS fv, count(x) OVER w AS c,
                                 var_samp(x) OVER w AS vs, var_pop(x) OVER w AS vp,
                                 stddev_samp(x) OVER w AS ss, stddev_pop(x) OVER w AS sp
                          FROM t WINDOW w AS (PARTITION BY k)
                        ) WHERE k = 'nx'""").returns("""
                        k\ts\ta\tks\tmx\tmn\tfv\tc\tvs\tvp\tss\tsp
                        nx\tnull\tnull\tnull\tnull\tnull\tnull\t0\tnull\tnull\tnull\tnull
                        nx\tnull\tnull\tnull\tnull\tnull\tnull\t0\tnull\tnull\tnull\tnull
                        """);
            }
        });
    }

    @Test
    public void testWholePartitionSumAvgAndCountShareOneComponent() throws Exception {
        // The key regression case of this step, and the one the design named years before it
        // was built: unfused, avg's preparePass2 replaces each partition's sum slot with its
        // average, which a shared component cannot carry because the sum projection beside it
        // still needs the sum. Fused, nothing finalizes - the group keeps the raw
        // (sum, nonNullCount) pair through both passes and each of the three outputs computes
        // its own answer from it at projection time.
        assertMemoryLeak(() -> {
            createTable();
            insertRows();
            for (int light = 0; light < 2; light++) {
                setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, light == 0 ? "false" : "true");
                final String sql = "select ts, sum(x) over (partition by k), avg(x) over (partition by k), "
                        + "count(x) over (partition by k)" + PARTITION_WINDOW;
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
                        final long rows = WindowMapStateTest.drain(cursor);
                        Assert.assertEquals(ROW_COUNT, rows);
                        // The 'nx' partition is refused whole in pass 1 and stays out of the
                        // map. The three outputs still read the identity it would have been left
                        // sitting at; see the reference comparison below.
                        Assert.assertTrue(state.isPass1SkipEnabled());
                        // The property the skipped preparePass2 rests on. A bound function's
                        // own map stays closed for the factory's whole life, so the walk it
                        // would perform has nothing to walk - and the guard that skips it is
                        // what says so rather than leaving it to whatever a closed map's
                        // cursor happens to answer.
                        final ObjList<WindowFunction> functions = windowFunctions(factory);
                        Assert.assertEquals(3, functions.size());
                        for (int i = 0, n = functions.size(); i < n; i++) {
                            final WindowFunction function = functions.getQuick(i);
                            Assert.assertTrue("a function was left unbound", function.isWindowStateOwned());
                            Assert.assertFalse(
                                    "a bound function's own map opened",
                                    function.getPartitionMap().isOpen()
                            );
                        }
                    }
                }
                assertFusedMatchesUnfused(
                        PARTITION_WINDOW,
                        "",
                        "sum(x) over (partition by k)",
                        "avg(x) over (partition by k)",
                        "count(x) over (partition by k)"
                );
            }
        });
    }

    private static void assertBoundGroupCount(CachedWindowMapGroups groups, int expected) {
        Assert.assertNotNull("no window Map group was compiled", groups);
        Assert.assertEquals(expected, groups.getStates().size());
    }

    /**
     * Compiles {@code sql} and requires it to have taken the cached cursor the light setting
     * calls for.
     */
    private static void assertCachedFactoryKind(String sql, boolean light) throws SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler();
             RecordCursorFactory factory = select(compiler, sql, sqlExecutionContext)) {
            assertFactoryKind(factory, light);
        }
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
            references[i] = WindowMapStateTest.body(WindowMapStateTest.render(reference));
        }
        final String expected = WindowMapStateTest.zipLastColumns(references);
        // A comparison of two empty renderings would pass and prove nothing, and every way the
        // helpers above could go wrong ends in one.
        Assert.assertFalse("the references produced no rows", expected.trim().isEmpty());
        Assert.assertEquals(
                fused.toString(),
                expected,
                WindowMapStateTest.body(WindowMapStateTest.render(fused.toString()))
        );
    }

    private static void assertIsBound(String sql, boolean bound) throws SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler();
             RecordCursorFactory factory = select(compiler, sql, sqlExecutionContext)) {
            final CachedWindowMapGroups groups = groups(factory);
            Assert.assertEquals(sql, bound, groups != null && groups.getStates().size() > 0);
        }
    }

    /**
     * Compiles {@code sql} and requires it to have taken the streaming cursor, which is the one
     * that runs no {@code pass1} and so is the only reference a bug shared by both cached
     * factories can be caught against.
     */
    private static void assertIsStreamingCursor(String sql) throws SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler();
             RecordCursorFactory factory = select(compiler, sql, sqlExecutionContext)) {
            // Unlike cachedFactory this stops on any of the three kinds, so the failure names
            // the cursor the query actually took instead of asserting one is absent.
            RecordCursorFactory root = factory;
            while (root != null
                    && !(root instanceof CachedWindowRecordCursorFactory)
                    && !(root instanceof CachedWindowLightRecordCursorFactory)
                    && !(root instanceof WindowRecordCursorFactory)) {
                root = root.getBaseFactory();
            }
            Assert.assertNotNull("no window factory in the tree: " + sql, root);
            Assert.assertTrue(
                    "expected the streaming window cursor, took " + root.getClass().getSimpleName() + ": " + sql,
                    root instanceof WindowRecordCursorFactory
            );
        }
    }

    /**
     * Drops each row's second field, which is where a comparison against a query that carries no
     * forcing call puts that call's output.
     */
    private static String dropSecondColumn(String body) {
        final StringBuilder out = new StringBuilder();
        final String[] rows = body.split("\n", -1);
        for (int i = 0; i < rows.length; i++) {
            if (i > 0) {
                out.append('\n');
            }
            final String row = rows[i];
            if (row.isEmpty()) {
                continue;
            }
            final int first = row.indexOf('\t');
            Assert.assertTrue("a row with no second field: " + row, first >= 0);
            final int second = row.indexOf('\t', first + 1);
            Assert.assertTrue("a row with no third field: " + row, second >= 0);
            out.append(row, 0, first).append(row, second, row.length());
        }
        return out.toString();
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

    private static CachedWindowMapGroups groups(RecordCursorFactory factory) {
        final RecordCursorFactory root = cachedFactory(factory);
        return root instanceof CachedWindowRecordCursorFactory f
                ? f.getWindowMapGroups()
                : ((CachedWindowLightRecordCursorFactory) root).getWindowMapGroups();
    }

    /**
     * The one bound group that is not {@code state}, for a query that forms exactly two. Found
     * rather than indexed because which of them is listed first is the compiler's business.
     */
    private static WindowMapState otherState(CachedWindowMapGroups groups, WindowMapState state) {
        final ObjList<WindowMapState> states = groups.getStates();
        Assert.assertEquals(2, states.size());
        return states.getQuick(0) == state ? states.getQuick(1) : states.getQuick(0);
    }

    private static String plan(String sql) throws SqlException {
        return WindowMapStateTest.render("explain " + sql);
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
     * One row's worth of DECIMAL literals, each cast to the width of the column it goes in - a
     * numeric literal is a DOUBLE and does not convert on its own.
     */
    private static String decimals(String d64, String d128) {
        return d64 + "::decimal(18, 2), " + d128 + "::decimal(38, 6)";
    }

    private static String orderedSumAndCount() {
        return "select ts, sum(x) over w, count(y) over w" + ORDERED_WINDOW;
    }

    private void createTable() throws SqlException {
        // d64 and d128 are the two state widths a DECIMAL extremum can keep - a LONG slot and a
        // DECIMAL128 one - so the cached traversals are exercised over a fused value that is
        // not a list of 64-bit words.
        execute("create table t (ts timestamp, k symbol, k2 symbol, x double, y double, "
                + "d64 decimal(18, 2), d128 decimal(38, 6)) "
                + "timestamp(ts) partition by day");
    }

    /**
     * The partition shapes the merged families part company on: a NULL key, whose
     * {@code count(k)} is zero while its row count is not; a partition of one row, where a
     * sample dispersion is NULL and a population one is 0; one whose only non-null {@code x}
     * is an infinity, so it has rows, a non-null count and no finite value; and rows where
     * exactly one of {@code x} and {@code y} is absent, which is where two counters part.
     * <p>
     * The two DECIMAL columns are absent on every row of the {@code 'nx'} partition, which is
     * the shape a DECIMAL extremum parts company on: it has rows and no value either direction
     * contributes, so its state must read back as that width's own NULL.
     */
    private void insertRows() throws SqlException {
        execute("insert into t values " +
                "('2024-01-01T00:00:00.000000Z', 'a', 'p', 1.0, 10.0, " + decimals("1", "1") + "), " +
                "('2024-01-01T00:00:01.000000Z', null, 'p', 2.0, 20.0, " + decimals("-2", "-2") + "), " +
                "('2024-01-01T00:00:02.000000Z', 'a', 'q', 4.0, null, 4.00::decimal(18, 2), null), " +
                "('2024-01-01T00:00:03.000000Z', null, 'q', null, 40.0, null, 40.000000::decimal(38, 6)), " +
                "('2024-01-01T00:00:04.000000Z', 'one', 'p', 5.0, 50.0, " + decimals("5", "5") + "), " +
                "('2024-01-01T00:00:05.000000Z', 'nx', 'q', null, 60.0, null, null), " +
                "('2024-01-01T00:00:06.000000Z', 'nx', 'p', 'Infinity'::double, 70.0, null, null), " +
                "('2024-01-01T00:00:07.000000Z', 'a', 'q', 8.0, 80.0, " + decimals("-8", "8") + "), " +
                "('2024-01-01T00:00:08.000000Z', null, 'p', 9.0, null, " + decimals("9", "-9") + ")");
    }
}
