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

package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.test.TestLatchedCounterFunctionFactory;
import io.questdb.griffin.engine.join.FrozenHashJoinBuild;
import io.questdb.griffin.engine.table.AsyncHashJoinRecordCursorFactory;
import io.questdb.griffin.engine.table.HashJoinBuildRoundAtom;
import io.questdb.griffin.engine.table.HashJoinBuildStage;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static io.questdb.test.griffin.ParallelHashJoinTestUtils.*;

/**
 * The parallel hash join's concurrent small builds: the stages whose build inputs hold fewer rows
 * than {@code cairo.sql.parallel.hash.join.groupby.build.parallel.min.rows} build at once, in one
 * round of the join's build sequence with one task per stage, when there are more than one, and the
 * larger stages build one at a time, each in rounds of its own, as does a smaller stage whose filter
 * runs interpreted over more than one frame. The builds equal those that the
 * stages make one at a time: every result compares with the ordinary plan, in order, and with the
 * operator's own results when every stage builds alone, key and row counts included. A failure and
 * a cancellation inside one of the concurrent builds release every build and slot, and the factory
 * reads the rows again. The tasks run on the owner here, as no worker serves the test's context;
 * {@link AsyncHashJoinConcurrentBuildsWorkersTest} runs them on workers.
 */
public class AsyncHashJoinConcurrentBuildsTest extends AbstractCairoTest {
    // Three stages over t: a duplicate INT key, a duplicate LONG key that stages into a map and a
    // translated SYMBOL key, of 20, 15 and 4 build rows.
    private static final String STAR = "SELECT t.v, d1.w, d2.w2, d3.w3 FROM t LEFT JOIN d1 ON t.k = d1.k LEFT JOIN d2 ON t.lk = d2.lk LEFT JOIN d3 ON t.sym = d3.s";
    private static final String STAGE = HashJoinBuildStage.class.getName();
    private static final StackWalker STACK_WALKER = StackWalker.getInstance();

    @Override
    @Before
    public void setUp() {
        super.setUp();
        sqlExecutionContext.setParallelHashJoinEnabled(true);
    }

    @After
    public void tearDownBuilds() {
        TestLatchedCounterFunctionFactory.reset(null);
        sqlExecutionContext.restoreToDefaultPageFrameSizes();
    }

    @Test
    public void testCancellationInsideOneConcurrentBuild() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            sqlExecutionContext.changePageFrameSizes(4, 4);
            // The first stage's build filter trips the breaker from inside its task of the round. A
            // filter that the JIT does not compile builds a stage in rounds of its own over more than
            // one frame, so the filtered stages here build d3's four rows and u1's five, one frame
            // each, and stay small.
            final String sql = "SELECT t.v, x.w3, d1.w, y.w FROM t LEFT JOIN (SELECT * FROM d3 WHERE test_latched_counter()) x ON t.sym = x.s"
                    + " LEFT JOIN d1 ON t.k = d1.k LEFT JOIN (SELECT * FROM u1 WHERE test_latched_counter()) y ON t.k = y.k";
            final String expected = ordinary(sql, sqlExecutionContext);
            final AtomicBooleanCircuitBreaker breaker = new AtomicBooleanCircuitBreaker(engine);
            final AtomicBoolean isArmed = new AtomicBoolean();
            final AtomicBoolean isInsideTask = new AtomicBoolean();
            final TestLatchedCounterFunctionFactory.Callback callback = new TestLatchedCounterFunctionFactory.Callback() {
                @Override
                public boolean onGet(Record rec, int count) {
                    if (isArmed.get() && count == 2) {
                        isInsideTask.set(isOnStack("buildStage"));
                        breaker.cancel();
                    }
                    return true;
                }
            };
            TestLatchedCounterFunctionFactory.reset(callback);
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                TestUtils.assertContains(plan(factory, sqlExecutionContext), "stages: 3");
                final HashJoinBuildRoundAtom roundAtom = join.getBuildRoundAtom();
                final long rounds = roundAtom.getConcurrentBuildRoundCount();
                TestLatchedCounterFunctionFactory.reset(callback);
                isArmed.set(true);
                final SqlExecutionCircuitBreaker previous = sqlExecutionContext.getCircuitBreaker();
                ((SqlExecutionContextImpl) sqlExecutionContext).with(breaker);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    //noinspection StatementWithEmptyBody
                    while (cursor.hasNext()) {
                    }
                    Assert.fail("expected cancellation");
                } catch (CairoException e) {
                    Assert.assertTrue(e.getMessage(), e.isCancellation());
                    TestUtils.assertContains(e.getFlyweightMessage(), "cancelled by user");
                } finally {
                    isArmed.set(false);
                    ((SqlExecutionContextImpl) sqlExecutionContext).with(previous);
                }
                Assert.assertTrue(isInsideTask.get());
                Assert.assertEquals(rounds + 1, roundAtom.getConcurrentBuildRoundCount());
                // d3's four rows went through the filter, its frame to the end, and u1's five never
                // did: the round's tasks after the cancelled one found the query cancelled.
                Assert.assertEquals(4, TestLatchedCounterFunctionFactory.getCount());
                assertReleased(join);
                Assert.assertEquals(expected, result(factory, sqlExecutionContext));
                Assert.assertEquals(rounds + 2, roundAtom.getConcurrentBuildRoundCount());
            }
            assertQuery("SELECT count() FROM d3").noLeakCheck().noRandomAccess().expectSize().returns("count\n4\n");
        });
    }

    @Test
    public void testCancellationInsideSmallFilteredStageRounds() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            sqlExecutionContext.changePageFrameSizes(4, 4);
            // The second stage's build filter, which the JIT does not compile, spans d2's four frames,
            // so the stage builds in rounds of its own, after the round that builds d1 and d3 at once,
            // and the filter trips the breaker from inside a task of its first round.
            final String sql = "SELECT t.v, d1.w, x.w2, d3.w3 FROM t LEFT JOIN d1 ON t.k = d1.k"
                    + " LEFT JOIN (SELECT * FROM d2 WHERE test_latched_counter()) x ON t.lk = x.lk LEFT JOIN d3 ON t.sym = d3.s";
            final String expected = ordinary(sql, sqlExecutionContext);
            final AtomicBooleanCircuitBreaker breaker = new AtomicBooleanCircuitBreaker(engine);
            final AtomicBoolean isArmed = new AtomicBoolean();
            final AtomicBoolean isInsideTask = new AtomicBoolean();
            final TestLatchedCounterFunctionFactory.Callback callback = new TestLatchedCounterFunctionFactory.Callback() {
                @Override
                public boolean onGet(Record rec, int count) {
                    if (isArmed.get() && count == 5) {
                        isInsideTask.set(isOnStack("partitionFrame"));
                        breaker.cancel();
                    }
                    return true;
                }
            };
            TestLatchedCounterFunctionFactory.reset(callback);
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                final HashJoinBuildRoundAtom roundAtom = join.getBuildRoundAtom();
                final HashJoinBuildStage stage = join.getAtom().getStage(1);
                final long concurrentRounds = roundAtom.getConcurrentBuildRoundCount();
                final long stageRounds = stage.getRoundCount();
                TestLatchedCounterFunctionFactory.reset(callback);
                isArmed.set(true);
                final SqlExecutionCircuitBreaker previous = sqlExecutionContext.getCircuitBreaker();
                ((SqlExecutionContextImpl) sqlExecutionContext).with(breaker);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    //noinspection StatementWithEmptyBody
                    while (cursor.hasNext()) {
                    }
                    Assert.fail("expected cancellation");
                } catch (CairoException e) {
                    Assert.assertTrue(e.getMessage(), e.isCancellation());
                    TestUtils.assertContains(e.getFlyweightMessage(), "cancelled by user");
                } finally {
                    isArmed.set(false);
                    ((SqlExecutionContextImpl) sqlExecutionContext).with(previous);
                }
                Assert.assertTrue(isInsideTask.get());
                Assert.assertEquals(concurrentRounds + 1, roundAtom.getConcurrentBuildRoundCount());
                // The first round of the stage's own, which the cancellation ended.
                Assert.assertEquals(stageRounds + 1, stage.getRoundCount());
                // The 15 rows of the build went through the filter only up to the cancelled frame.
                Assert.assertTrue(String.valueOf(TestLatchedCounterFunctionFactory.getCount()), TestLatchedCounterFunctionFactory.getCount() < 15);
                assertReleased(join);
                Assert.assertEquals(expected, result(factory, sqlExecutionContext));
                Assert.assertEquals(concurrentRounds + 2, roundAtom.getConcurrentBuildRoundCount());
                // The next execution built in two rounds and copied its payload in a third: the
                // probe's 60 rows are more than 0.125 times the build's 15.
                Assert.assertEquals(stageRounds + 4, stage.getRoundCount());
            }
            assertQuery("SELECT count() FROM d2").noLeakCheck().noRandomAccess().expectSize().returns("count\n15\n");
        });
    }

    @Test
    public void testConcurrentBuildsEqualBuildsAlone() throws Exception {
        // No build copies its payload, so that a build in rounds of its own runs two rounds.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_MIN_PROBE_RATIO, "1000000");
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_PARALLEL_MIN_PROBE_RATIO, "1000000");
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_ROWS_PER_PARTITION, 4);
        assertMemoryLeak(() -> {
            createTables();
            sqlExecutionContext.changePageFrameSizes(4, 4);
            final String[] queries = {
                    STAR,
                    // An INNER stage over a filtered build input, a backward probe scan, and four stages.
                    "SELECT t.v, f.w, d2.w2, d3.w3, u1.w FROM t JOIN (SELECT * FROM d1 WHERE w % 3 <> 0) f ON t.k = f.k"
                            + " LEFT JOIN d2 ON t.lk = d2.lk LEFT JOIN d3 ON t.sym = d3.s LEFT JOIN u1 ON t.k = u1.k ORDER BY t.ts DESC",
                    // A swapped stack: the first stage hashes d1, and the second reads its key off d1's rows.
                    "SELECT d1.w, t.v, d3.w3 FROM d1 JOIN t ON d1.k = t.k LEFT JOIN d3 ON d1.s = d3.s",
                    // One stage builds alone, as it always did.
                    "SELECT t.v, d1.w FROM t LEFT JOIN d1 ON t.k = d1.k",
                    // A build input without frames is small whatever the configured rows.
                    "SELECT t.v, d1.w, e.w FROM t LEFT JOIN d1 ON t.k = d1.k LEFT JOIN e ON t.k = e.k",
            };
            for (String sql : queries) {
                // Every stage small, every stage large, and d1 large among small ones.
                final String[] buildsAlone = new String[1];
                for (long minRows : new long[]{0, Long.MAX_VALUE, 16}) {
                    setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, minRows);
                    final String plan = assertParallelHashJoin(sql, sqlExecutionContext);
                    try (RecordCursorFactory factory = select(sql)) {
                        final AsyncHashJoinRecordCursorFactory join = find(factory);
                        Assert.assertNotNull(join);
                        final String builds = assertBuildCounts(factory, join, sql + ", min rows " + minRows);
                        if (minRows == 0) {
                            buildsAlone[0] = builds;
                        } else {
                            // The key and row counts of every build equal those of the builds alone.
                            Assert.assertEquals(sql + '\n' + plan, buildsAlone[0], builds);
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testConcurrentBuildsWithPayloadCopies() throws Exception {
        // Every stage copies its payload when it can, from a probe of any size.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_MIN_PROBE_RATIO, "0");
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, Long.MAX_VALUE);
        assertMemoryLeak(() -> {
            createTables();
            sqlExecutionContext.changePageFrameSizes(4, 4);
            assertParallelHashJoin(STAR, sqlExecutionContext);
            try (RecordCursorFactory factory = select(STAR)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                final long rounds = join.getBuildRoundAtom().getConcurrentBuildRoundCount();
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    for (int k = 0; k < 3; k++) {
                        Assert.assertTrue(join.getAtom().getStage(k).isPayloadCopied());
                    }
                }
                Assert.assertEquals(rounds + 1, join.getBuildRoundAtom().getConcurrentBuildRoundCount());
            }
        });
    }

    @Test
    public void testFailureInsideOneConcurrentBuild() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            sqlExecutionContext.changePageFrameSizes(4, 4);
            // d3.w3 * 100 does not fit DECIMAL(4,0) from w3 = 100 on, so the third stage's build filter
            // fails on its first row, inside its task of the round, or in its rounds of its own. A
            // filter that the JIT does not compile builds a stage in rounds of its own over more than
            // one frame, so the filter runs over d3's four rows, one frame, which stay small.
            final String sql = "SELECT t.v, d1.w, d2.w2, x.w3 FROM t LEFT JOIN d1 ON t.k = d1.k"
                    + " LEFT JOIN d2 ON t.lk = d2.lk LEFT JOIN (SELECT * FROM d3 WHERE (w3 * 100)::DECIMAL(4,0) > 0) x ON t.sym = x.s";
            for (long minRows : new long[]{Long.MAX_VALUE, 0}) {
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, minRows);
                final String expected = error(sql, false, sqlExecutionContext);
                TestUtils.assertContains(expected, "inconvertible value: 10000 [LONG -> DECIMAL(4,0)]");
                Assert.assertEquals(sql, expected, error(sql, true, sqlExecutionContext));
                try (RecordCursorFactory factory = select(sql)) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    Assert.assertNotNull(join);
                    final long rounds = join.getBuildRoundAtom().getConcurrentBuildRoundCount();
                    for (int run = 0; run < 2; run++) {
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            //noinspection StatementWithEmptyBody
                            while (cursor.hasNext()) {
                            }
                            Assert.fail("expected the build filter to fail");
                        } catch (CairoException | ImplicitCastException e) {
                            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value");
                        }
                        assertReleased(join);
                    }
                    Assert.assertEquals(rounds + (minRows == 0 ? 0 : 2), join.getBuildRoundAtom().getConcurrentBuildRoundCount());
                }
            }
            // The tables still work, and so does the stack whose filter passes the rows.
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, Long.MAX_VALUE);
            assertParallelHashJoin("SELECT t.v, d1.w, d2.w2, x.w3 FROM t LEFT JOIN d1 ON t.k = d1.k"
                    + " LEFT JOIN d2 ON t.lk = d2.lk LEFT JOIN (SELECT * FROM d3 WHERE (w3 * 10)::DECIMAL(4,0) > 0) x ON t.sym = x.s", sqlExecutionContext);
        });
    }

    @Test
    public void testFailureInsideSmallFilteredStageRounds() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            sqlExecutionContext.changePageFrameSizes(4, 4);
            // d2.w2 * 1000 does not fit DECIMAL(4,0) from w2 = 10 on, so the second stage's build filter
            // fails on its first row. The filter runs interpreted over d2's four frames, so the stage
            // builds in rounds of its own, after the round that builds d1 and d3 at once, and fails in
            // a task of its first round.
            final String sql = "SELECT t.v, d1.w, x.w2, d3.w3 FROM t LEFT JOIN d1 ON t.k = d1.k"
                    + " LEFT JOIN (SELECT * FROM d2 WHERE (w2 * 1000)::DECIMAL(4,0) > 0) x ON t.lk = x.lk LEFT JOIN d3 ON t.sym = d3.s";
            final String expected = error(sql, false, sqlExecutionContext);
            TestUtils.assertContains(expected, "inconvertible value: 10000 [LONG -> DECIMAL(4,0)]");
            Assert.assertEquals(sql, expected, error(sql, true, sqlExecutionContext));
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                final HashJoinBuildStage stage = join.getAtom().getStage(1);
                final long concurrentRounds = join.getBuildRoundAtom().getConcurrentBuildRoundCount();
                final long stageRounds = stage.getRoundCount();
                for (int run = 0; run < 2; run++) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        //noinspection StatementWithEmptyBody
                        while (cursor.hasNext()) {
                        }
                        Assert.fail("expected the build filter to fail");
                    } catch (CairoException | ImplicitCastException e) {
                        Assert.assertEquals(sql, expected, e.getPosition() + ": " + e.getFlyweightMessage());
                    }
                    assertReleased(join);
                }
                // Each run built d1 and d3 at once, then failed in the first round of d2's own.
                Assert.assertEquals(concurrentRounds + 2, join.getBuildRoundAtom().getConcurrentBuildRoundCount());
                Assert.assertEquals(stageRounds + 2, stage.getRoundCount());
            }
            // The tables still work, and so does the stack whose filter passes the rows.
            assertParallelHashJoin("SELECT t.v, d1.w, x.w2, d3.w3 FROM t LEFT JOIN d1 ON t.k = d1.k"
                    + " LEFT JOIN (SELECT * FROM d2 WHERE (w2 * 10)::DECIMAL(4,0) > 0) x ON t.lk = x.lk LEFT JOIN d3 ON t.sym = d3.s", sqlExecutionContext);
        });
    }

    @Test
    public void testMemoryLimitInsideConcurrentBuilds() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, Long.MAX_VALUE);
        assertMemoryLeak(() -> {
            createTables();
            sqlExecutionContext.changePageFrameSizes(4, 4);
            final String expected = ordinary(STAR, sqlExecutionContext);
            try (RecordCursorFactory factory = select(STAR)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                // Limits from a few bytes up fail the query at its allocations in turn, the concurrent
                // builds' among them; each failure releases every build and slot, and the first limit
                // that admits the query reads the rows.
                int concurrentFailures = 0;
                for (long limit = 4; ; limit *= 2) {
                    setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, limit);
                    try {
                        Assert.assertEquals(expected, result(factory, sqlExecutionContext));
                        break;
                    } catch (CairoException e) {
                        Assert.assertTrue(e.getMessage(), e.isOutOfMemory());
                        TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                        if (isThrownFrom(e, HashJoinBuildRoundAtom.class.getName(), "buildConcurrently")) {
                            concurrentFailures++;
                        }
                    }
                    assertReleased(join);
                }
                Assert.assertTrue(concurrentFailures > 0);
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0);
                Assert.assertEquals(expected, result(factory, sqlExecutionContext));
            }
            assertQuery("SELECT count() FROM d1").noLeakCheck().noRandomAccess().expectSize().returns("count\n20\n");
        });
    }

    @Test
    public void testMemoryLimitInsideSmallFilteredStageRounds() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            sqlExecutionContext.changePageFrameSizes(4, 4);
            // The stage's filter runs interpreted over d1's five frames, so it builds in rounds.
            final String sql = "SELECT t.v, x.w FROM t LEFT JOIN (SELECT * FROM d1 WHERE s::STRING ~ 's[1-3]') x ON t.k = x.k";
            final String expected = ordinary(sql, sqlExecutionContext);
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                // Limits from a few bytes up fail the query at its allocations in turn, those of the
                // build's rounds among them; each failure releases the build and every slot, and the
                // first limit that admits the query reads the rows.
                int roundFailures = 0;
                for (long limit = 4; ; limit *= 2) {
                    setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, limit);
                    try {
                        Assert.assertEquals(expected, result(factory, sqlExecutionContext));
                        break;
                    } catch (CairoException e) {
                        Assert.assertTrue(e.getMessage(), e.isOutOfMemory());
                        TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                        if (isThrownFrom(e, STAGE, "buildInRounds")) {
                            roundFailures++;
                        }
                    }
                    assertReleased(join);
                }
                Assert.assertTrue(roundFailures > 0);
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0);
                Assert.assertEquals(expected, result(factory, sqlExecutionContext));
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertTrue(join.getAtom().getStage(0).isBuiltInRounds());
                }
            }
            assertQuery("SELECT count() FROM d1").noLeakCheck().noRandomAccess().expectSize().returns("count\n20\n");
        });
    }

    @Test
    public void testSmallFilteredStagesBuildInRoundsOfTheirOwn() throws Exception {
        // No build copies its payload, so that a build in rounds of its own runs two rounds.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_MIN_PROBE_RATIO, "1000000");
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_PARALLEL_MIN_PROBE_RATIO, "1000000");
        assertMemoryLeak(() -> {
            createTables();
            sqlExecutionContext.changePageFrameSizes(4, 4);
            // Every build holds fewer rows than the configured 300_000. A stage whose filter the JIT
            // does not compile spans more than one frame, d1's five or d2's four, and builds in rounds
            // of its own. Two such stages and one unfiltered: the unfiltered stage is the only small
            // one, which the owner builds alone, with no round of concurrent builds.
            final String twoFiltered = "SELECT t.v, x.w, y.w2, d3.w3 FROM t LEFT JOIN (SELECT * FROM d1 WHERE s::STRING ~ 's[1-3]') x ON t.k = x.k"
                    + " LEFT JOIN (SELECT * FROM d2 WHERE w2::STRING LIKE '1%') y ON t.lk = y.lk LEFT JOIN d3 ON t.sym = d3.s";
            assertStageBuilds(twoFiltered, new boolean[]{true, true, false}, 0);
            // One such stage and two unfiltered: the two small stages build at once, in one round.
            final String oneFiltered = "SELECT t.v, x.w, d2.w2, d3.w3 FROM t LEFT JOIN (SELECT * FROM d1 WHERE s::STRING ~ 's[1-3]') x ON t.k = x.k"
                    + " LEFT JOIN d2 ON t.lk = d2.lk LEFT JOIN d3 ON t.sym = d3.s";
            assertStageBuilds(oneFiltered, new boolean[]{true, false, false}, 1);
        });
    }

    // Executes the query twice and asserts, for each execution, that each stage built once, that the
    // stages too small to build in rounds of their own built at once in one round when there were more
    // than one, and that every other stage ran the two rounds of a build of its own. Returns the
    // stages' key and row counts.
    private static String assertBuildCounts(RecordCursorFactory factory, AsyncHashJoinRecordCursorFactory join, String label) throws SqlException {
        final int stageCount = join.getSpec().getStageCount();
        final String result = result(factory, sqlExecutionContext);
        final StringBuilder builds = new StringBuilder();
        for (int execution = 0; execution < 2; execution++) {
            final long concurrentRounds = join.getBuildRoundAtom().getConcurrentBuildRoundCount();
            final long[] buildCounts = new long[stageCount];
            final long[] roundCounts = new long[stageCount];
            builds.setLength(0);
            int smallCount = 0;
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                for (int k = 0; k < stageCount; k++) {
                    // The execution's stages, as its orientation picks them.
                    final HashJoinBuildStage stage = join.getAtom().getStage(k);
                    buildCounts[k] = stage.getBuildCount();
                    roundCounts[k] = stage.getRoundCount();
                }
                Assert.assertTrue(label, cursor.hasNext());
                for (int k = 0; k < stageCount; k++) {
                    final HashJoinBuildStage stage = join.getAtom().getStage(k);
                    Assert.assertEquals(label, buildCounts[k] + 1, stage.getBuildCount());
                    Assert.assertEquals(label, roundCounts[k] + (stage.isBuiltInRounds() ? 2 : 0), stage.getRoundCount());
                    if (!stage.isBuiltInRounds()) {
                        smallCount++;
                    }
                    final FrozenHashJoinBuild build = stage.getFrozenBuild();
                    builds.append(k).append(": ").append(build.getRowCount()).append(" rows, ").append(build.getKeyCount()).append(" keys\n");
                }
            }
            Assert.assertEquals(label, concurrentRounds + (smallCount > 1 ? 1 : 0), join.getBuildRoundAtom().getConcurrentBuildRoundCount());
        }
        Assert.assertEquals(label, result, result(factory, sqlExecutionContext));
        return builds.toString();
    }

    // The failure released every slot, reader and build of every stage.
    private static void assertReleased(AsyncHashJoinRecordCursorFactory join) {
        Assert.assertEquals(0, join.getBuildRoundAtom().getPerWorkerLocks().getAcquiredSlotCount());
        Assert.assertEquals(0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
        for (int k = 0, n = join.getSpec().getStageCount(); k < n; k++) {
            Assert.assertNull(join.getAtom().getStage(k).getFrozenBuild());
        }
        Assert.assertEquals(0, join.getAtom().getRing().getAllocatedBytes());
        Assert.assertEquals(0, engine.getBusyReaderCount());
    }

    // Executes the query twice, compares its rows with the ordinary plan's, and asserts for each
    // execution which stages built in rounds of their own, two rounds each, and how many rounds of
    // concurrent builds ran.
    private static void assertStageBuilds(String sql, boolean[] isBuiltInRounds, int concurrentRoundsPerExecution) throws SqlException {
        assertParallelHashJoin(sql, sqlExecutionContext);
        try (RecordCursorFactory factory = select(sql)) {
            final AsyncHashJoinRecordCursorFactory join = find(factory);
            Assert.assertNotNull(sql, join);
            final int stageCount = join.getSpec().getStageCount();
            Assert.assertEquals(sql, isBuiltInRounds.length, stageCount);
            for (int execution = 0; execution < 2; execution++) {
                final long concurrentRounds = join.getBuildRoundAtom().getConcurrentBuildRoundCount();
                final long[] stageRounds = new long[stageCount];
                for (int k = 0; k < stageCount; k++) {
                    stageRounds[k] = join.getAtom().getStage(k).getRoundCount();
                }
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(sql, cursor.hasNext());
                    for (int k = 0; k < stageCount; k++) {
                        final HashJoinBuildStage stage = join.getAtom().getStage(k);
                        Assert.assertEquals(sql + ", stage " + k, isBuiltInRounds[k], stage.isBuiltInRounds());
                        Assert.assertEquals(sql + ", stage " + k, stageRounds[k] + (isBuiltInRounds[k] ? 2 : 0), stage.getRoundCount());
                    }
                }
                Assert.assertEquals(sql, concurrentRounds + concurrentRoundsPerExecution, join.getBuildRoundAtom().getConcurrentBuildRoundCount());
            }
        }
    }

    private static void createTables() throws Exception {
        // t, the fact table: a NULL INT key every eleventh row, and keys that some dimensions lack.
        execute("""
                CREATE TABLE t AS (
                    SELECT
                        CASE WHEN x % 11 = 0 THEN NULL ELSE (x % 7)::INT END k,
                        (x % 6)::LONG lk,
                        ('s' || (x % 4))::SYMBOL sym,
                        x v,
                        timestamp_sequence('2024-01-01', 600_000_000) ts
                    FROM long_sequence(60)
                ) TIMESTAMP(ts) PARTITION BY HOUR
                """);
        // d1 repeats its INT keys, NULL among them, and lacks keys 5 and 6; its symbols are t's texts,
        // and one more.
        execute("""
                CREATE TABLE d1 AS (
                    SELECT CASE WHEN x % 6 = 0 THEN NULL ELSE (x % 5)::INT END k, x w, ('s' || (5 - x % 5))::SYMBOL s
                    FROM long_sequence(20)
                )
                """);
        // d2 repeats its LONG keys, which stage into a map, and lacks key 0.
        execute("CREATE TABLE d2 AS (SELECT 1 + x % 5 lk, x * 10 w2 FROM long_sequence(15))");
        // d3 holds one row per SYMBOL, in another order than t's dictionary, and lacks s0.
        execute("CREATE TABLE d3 AS (SELECT ('s' || (5 - x))::SYMBOL s, x * 100 w3 FROM long_sequence(4))");
        // u1 holds one row per key, and lacks keys 5 and 6.
        execute("CREATE TABLE u1 AS (SELECT (x - 1)::INT k, x * 3 w FROM long_sequence(5))");
        // e holds no row.
        execute("CREATE TABLE e (k INT, w LONG)");
    }

    private static boolean isOnStack(String methodName) {
        return STACK_WALKER.walk(frames -> frames.anyMatch(frame -> frame.getClassName().equals(STAGE) && frame.getMethodName().equals(methodName)));
    }

    private static boolean isThrownFrom(Throwable e, String className, String methodName) {
        for (StackTraceElement frame : e.getStackTrace()) {
            if (frame.getClassName().equals(className) && frame.getMethodName().equals(methodName)) {
                return true;
            }
        }
        return false;
    }
}
