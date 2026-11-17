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
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.griffin.HashJoinGroupByKeys;
import io.questdb.griffin.HashJoinKeyLayout;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.test.TestLatchedCounterFunctionFactory;
import io.questdb.griffin.engine.functions.test.TestThrowingFilterFunctionFactory;
import io.questdb.griffin.engine.join.JoinRecordMetadata;
import io.questdb.griffin.engine.table.AsyncFilterContext;
import io.questdb.griffin.engine.table.AsyncHashJoinRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncHashJoinSpec;
import io.questdb.griffin.engine.table.HashJoinBuildStage;
import io.questdb.griffin.engine.table.HashJoinInput;
import io.questdb.griffin.engine.table.HashJoinStageSpec;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.test.griffin.ParallelHashJoinTestUtils.*;

/**
 * Stacks of the parallel hash join: the later joins of a star query, whose keys read the fact
 * table's columns, run as later stages of one operator, which a probe task runs nested, each match of
 * one stage going on to the next, and whose consumer expands the chains that the stages after the
 * last enumerated one leave to it. Every result compares with the ordinary plan of the same SQL, in
 * order, and the shapes of nesting order and fan-out also with rows worked out by hand. The plans of
 * the stacked shapes, and of each later join that the operator leaves to an ordinary join over it,
 * print as EXPLAIN shows them.
 * <p>
 * The first join of a stack never swaps its inputs in this suite: it is a LEFT join, or an INNER one
 * whose left input has a filter or scans an interval. {@link AsyncHashJoinStackSwapTest} covers the
 * stacks whose first join may swap.
 */
public class AsyncHashJoinStackTest extends AbstractCairoTest {
    // Eight stages over t: duplicate and unique builds, INT, staged LONG and translated SYMBOL keys,
    // INNER and LEFT joins, and an ON residual on the seventh stage, so that the workers walk the
    // first seven stages and the consumer expands the eighth's chains.
    private static final String EIGHT_STAGES = "SELECT t.k, t.v, t.ts, a.w, b.w2, c.w3, e.w, f.w2, g.w3, h.w, i.w2 FROM t"
            + " LEFT JOIN d1 a ON t.k = a.k"
            + " LEFT JOIN u2 b ON t.lk = b.lk"
            + " LEFT JOIN d3 c ON t.sym = c.s"
            + " LEFT JOIN u1 e ON t.k = e.k"
            + " JOIN u2 f ON t.lk = f.lk"
            + " LEFT JOIN d3 g ON t.sym = g.s"
            + " LEFT JOIN u1 h ON t.k = h.k AND h.w > t.v % 5"
            + " LEFT JOIN d2 i ON t.lk = i.lk";
    private static final String STAGE = HashJoinBuildStage.class.getName();
    private static final StackWalker STACK_WALKER = StackWalker.getInstance();

    @Override
    @Before
    public void setUp() {
        super.setUp();
        sqlExecutionContext.setParallelHashJoinEnabled(true);
    }

    @After
    public void tearDownStack() {
        TestLatchedCounterFunctionFactory.reset(null);
        sqlExecutionContext.restoreToDefaultPageFrameSizes();
        sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
    }

    @Test
    public void testBuildsOnOwnerAndInRoundsWithPayloadCopies() throws Exception {
        // Every stage copies its payload when it can, from a probe of any size.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_MIN_PROBE_RATIO, "0");
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_PARALLEL_MIN_PROBE_RATIO, "0");
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_ROWS_PER_PARTITION, 2);
        assertMemoryLeak(() -> {
            createTables();
            sqlExecutionContext.changePageFrameSizes(3, 3);
            // An INT, a staged LONG and a translated SYMBOL key, each stage with a payload that the
            // copy holds.
            final String sql = "SELECT t.k, t.v, d1.w, d1.s, d2.w2, d2.q, d3.w3 FROM t LEFT JOIN d1 ON t.k = d1.k LEFT JOIN d2 ON t.lk = d2.lk LEFT JOIN d3 ON t.sym = d3.s";
            for (long minRows : new long[]{0, Long.MAX_VALUE}) {
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, minRows);
                assertStack(sql, 3);
                try (RecordCursorFactory factory = select(sql)) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    Assert.assertNotNull(join);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.assertTrue(cursor.hasNext());
                        for (int k = 0; k < 3; k++) {
                            final HashJoinBuildStage stage = join.getAtom().getStage(k);
                            Assert.assertEquals(minRows == 0, stage.isBuiltInRounds());
                            Assert.assertTrue(stage.isPayloadCopied());
                        }
                        // A later stage's build in rounds holds more rows than a partition does.
                        Assert.assertEquals(minRows == 0, join.getAtom().getStage(1).isBuildPartitioned());
                    }
                }
            }
        });
    }

    @Test
    public void testCancellationInLaterStageBuild() throws Exception {
        // The first stage's 20 rows build on the owner, and the second stage's 300 in rounds.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 100);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_ROWS_PER_PARTITION, 16);
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE TABLE big AS (SELECT (x % 6)::LONG lk, x bw FROM long_sequence(300))");
            sqlExecutionContext.changePageFrameSizes(32, 32);
            final String sql = "SELECT t.v, d1.w, b.bw FROM t LEFT JOIN d1 ON t.k = d1.k LEFT JOIN (SELECT * FROM big WHERE test_latched_counter()) b ON t.lk = b.lk";
            final String expected = ordinary(sql, sqlExecutionContext);
            final AtomicBooleanCircuitBreaker breaker = new AtomicBooleanCircuitBreaker(engine);
            final AtomicBoolean isArmed = new AtomicBoolean();
            final AtomicReference<AsyncHashJoinRecordCursorFactory> joinRef = new AtomicReference<>();
            final AtomicReference<String> buildStates = new AtomicReference<>();
            final TestLatchedCounterFunctionFactory.Callback callback = new TestLatchedCounterFunctionFactory.Callback() {
                @Override
                public boolean onGet(Record rec, int count) {
                    if (isArmed.get() && count == 40) {
                        // The second stage's first round runs once the first stage has built.
                        final AsyncHashJoinRecordCursorFactory join = joinRef.get();
                        buildStates.set(isOnStack(STAGE, "partitionFrame")
                                + " " + (join.getAtom().getStage(0).getFrozenBuild() != null)
                                + " " + (join.getAtom().getStage(1).getFrozenBuild() != null));
                        breaker.cancel();
                    }
                    return true;
                }
            };
            TestLatchedCounterFunctionFactory.reset(callback);
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                TestUtils.assertContains(plan(factory, sqlExecutionContext), "stages: 2");
                TestUtils.assertContains(plan(factory, sqlExecutionContext), "buildFilter: test_latched_counter()");
                joinRef.set(join);
                TestLatchedCounterFunctionFactory.reset(callback);
                isArmed.set(true);
                assertCancelled(factory, breaker, sql);
                isArmed.set(false);
                Assert.assertEquals("true true false", buildStates.get());
                // The 300 rows of the build went through the filter only up to the cancelled task.
                Assert.assertTrue(String.valueOf(TestLatchedCounterFunctionFactory.getCount()), TestLatchedCounterFunctionFactory.getCount() < 300);
                assertReused(factory, join, sql, expected);
                Assert.assertFalse(join.getAtom().getStage(0).isBuiltInRounds());
            }
            // The tables still work on their own.
            assertQuery("SELECT count() FROM big").noLeakCheck().noRandomAccess().expectSize().returns("count\n300\n");
        });
    }

    @Test
    public void testCancellationInsideNestedStages() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE TABLE none AS (SELECT (x + 100)::INT k, x nw FROM long_sequence(3))");
            // Frames of one row, so that a stage checks the breaker before each of its pairs. The
            // second stage's ON residual cancels the query at its first pair, and the stage finds the
            // query cancelled before its next pair, inside the first stage's walk of its matches: a
            // chain of several matches, the one match of a unique build, or a LEFT miss.
            sqlExecutionContext.changePageFrameSizes(1, 1);
            for (boolean isCopiesEnabled : new boolean[]{false, true}) {
                // The copies run from the first task.
                useSteppers(isCopiesEnabled);
                try {
                    for (String firstJoin : new String[]{"d1 a ON t.k = a.k", "u1 a ON t.k = a.k", "none a ON t.k = a.k"}) {
                        final String sql = "SELECT t.v, a.k, d2.w2 FROM t LEFT JOIN " + firstJoin
                                + " LEFT JOIN d2 ON t.lk = d2.lk AND (test_latched_counter() OR d2.w2 = t.v)";
                        final String expected = ordinary(sql, sqlExecutionContext);
                        final AtomicBooleanCircuitBreaker breaker = new AtomicBooleanCircuitBreaker(engine);
                        final AtomicBoolean isArmed = new AtomicBoolean();
                        final AtomicReference<Integer> cancelledAt = new AtomicReference<>();
                        final TestLatchedCounterFunctionFactory.Callback callback = new TestLatchedCounterFunctionFactory.Callback() {
                            @Override
                            public boolean onGet(Record rec, int count) {
                                if (isArmed.get() && cancelledAt.get() == null) {
                                    cancelledAt.set(count);
                                    breaker.cancel();
                                }
                                return true;
                            }
                        };
                        TestLatchedCounterFunctionFactory.reset(callback);
                        try (RecordCursorFactory factory = select(sql)) {
                            final AsyncHashJoinRecordCursorFactory join = find(factory);
                            Assert.assertNotNull(join);
                            TestUtils.assertContains(plan(factory, sqlExecutionContext), "stages: 2");
                            Assert.assertEquals(!isCopiesEnabled, !join.getStepperClass(0).isHidden());
                            TestLatchedCounterFunctionFactory.reset(callback);
                            isArmed.set(true);
                            assertCancelled(factory, breaker, sql);
                            isArmed.set(false);
                            // The cancelled tasks ran the stepper under test.
                            assertStepperRuns(join, isCopiesEnabled, sql);
                            // The residual ran over no pair after the one that cancelled the query.
                            Assert.assertEquals(sql, cancelledAt.get().intValue(), TestLatchedCounterFunctionFactory.getCount());
                            assertReused(factory, join, sql, expected);
                        }
                    }
                } finally {
                    restoreSteppers();
                }
            }
        });
    }

    @Test
    public void testCompileUnderMemoryLimitLeaksNothing() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // The later stage's build filter, ON residual and post-join filter are regexes, which have
            // worker copies, and the join is constructed three times: for the first join, the second
            // and the post-join filter. A native memory limit just above what the process holds fails
            // the compile at each of its allocations in turn, after the first join gave up its spec;
            // every failure releases what the compile took, as assertMemoryLeak checks, and the first
            // limit that admits the compile plans the stack.
            final String sql = "SELECT t.k, t.v, d2.w2, x.w FROM t LEFT JOIN d2 ON t.lk = d2.lk"
                    + " LEFT JOIN (SELECT * FROM d1 WHERE vch ~ 'v[1-9]') x ON t.k = x.k AND (x.vch ~ 'v[2-4]' OR t.str ~ 'str[1]')"
                    + " WHERE x.w IS NULL OR d2.w2 IS NULL OR t.str ~ 'str[12]'";
            final String expected = ordinary(sql, sqlExecutionContext);
            int failures = 0;
            for (long extra = 0; ; extra += 64) {
                RecordCursorFactory factory = null;
                try {
                    Unsafe.setRssMemLimit(Unsafe.getRssMemUsed() + extra);
                    factory = select(sql);
                } catch (CairoException e) {
                    Assert.assertTrue(e.getMessage(), e.isOutOfMemory());
                    failures++;
                    continue;
                } finally {
                    Unsafe.setRssMemLimit(0);
                }
                try {
                    final String plan = plan(factory, sqlExecutionContext);
                    TestUtils.assertContains(plan, "stages: 2");
                    TestUtils.assertContains(plan, "postJoinFilter: ");
                    Assert.assertEquals(expected, result(factory, sqlExecutionContext));
                } finally {
                    factory.close();
                }
                break;
            }
            Assert.assertTrue(failures > 0);
            assertStack(sql, 2);
        });
    }

    @Test
    public void testConstructionFailureWhileStacking() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // The first join, the second, its post-join filter and the third make four constructions;
            // the residual of the second join is a regex, which has worker copies. Each construction
            // fails in turn, after the joins before it gave up their specs.
            final String sql = "SELECT t.k, t.v, d1.w, d2.w2, d3.w3 FROM t LEFT JOIN d1 ON t.k = d1.k"
                    + " LEFT JOIN d2 ON t.lk = d2.lk AND (d2.q ~ 'q[12]' OR t.v > 30)"
                    + " LEFT JOIN d3 ON t.sym = d3.s"
                    + " WHERE d2.w2 IS NULL OR d2.w2 > d1.w";
            final String plan = assertStack(sql, 3);
            TestUtils.assertContains(plan, "postJoinFilter: ");
            for (int constructions = 0; constructions < 4; constructions++) {
                AsyncHashJoinRecordCursorFactory.setConstructorFailureForTesting(CairoException.nonCritical().put("injected construction failure"), constructions);
                try {
                    assertExceptionNoLeakCheck(sql, 0, "injected construction failure");
                } finally {
                    AsyncHashJoinRecordCursorFactory.setConstructorFailureForTesting(null);
                }
            }
            // The tables still work.
            assertStack(sql, 3);
        });
    }

    @Test
    public void testDeferredStagesExpandInNestingOrder() throws Exception {
        assertMemoryLeak(() -> {
            createNestingTables();
            // No stage runs a filter, so every stage hands its chains to the consumer, which reads
            // every combination of a probe row's matches, the last stage's fastest, each chain newest
            // first: f's key 1 matches a4, a2, a1 in a, b3, b1 in b and c2, c1 in c.
            final String sql = "SELECT f.v, a.a, b.b, c.c FROM f LEFT JOIN a ON f.k = a.k LEFT JOIN b ON f.k = b.k LEFT JOIN c ON f.k = c.k";
            final String expected = """
                    v\ta\tb\tc
                    10\ta4\tb3\tc2
                    10\ta4\tb3\tc1
                    10\ta4\tb1\tc2
                    10\ta4\tb1\tc1
                    10\ta2\tb3\tc2
                    10\ta2\tb3\tc1
                    10\ta2\tb1\tc2
                    10\ta2\tb1\tc1
                    10\ta1\tb3\tc2
                    10\ta1\tb3\tc1
                    10\ta1\tb1\tc2
                    10\ta1\tb1\tc1
                    20\ta3\tb2\t
                    30\t\tb4\t
                    """;
            for (int frameRows : new int[]{1, 2, 1_000_000}) {
                sqlExecutionContext.changePageFrameSizes(frameRows, frameRows);
                assertQuery(sql).noLeakCheck().noRandomAccess().returns(expected);
                assertStack(sql, 3);
                // A LEFT miss and a single match between two chains, and an INNER join that drops the
                // probe rows its build lacks.
                assertQuery("SELECT f.v, a.a, x.b, c.c FROM f LEFT JOIN a ON f.k = a.k LEFT JOIN (SELECT * FROM b WHERE k <> 1) x ON f.k = x.k JOIN c ON f.k = c.k")
                        .noLeakCheck()
                        .noRandomAccess()
                        .returns("""
                                v\ta\tb\tc
                                10\ta4\t\tc2
                                10\ta4\t\tc1
                                10\ta2\t\tc2
                                10\ta2\t\tc1
                                10\ta1\t\tc2
                                10\ta1\t\tc1
                                """);
                assertStack("SELECT f.v, a.a, x.b, c.c FROM f LEFT JOIN a ON f.k = a.k LEFT JOIN (SELECT * FROM b WHERE k <> 1) x ON f.k = x.k JOIN c ON f.k = c.k", 3);
            }
            // toTop() before the first read, inside the chains and after the last row each read the
            // whole result again, from the first combination.
            try (RecordCursorFactory factory = select(sql)) {
                Assert.assertNotNull(find(factory));
                final String rows = rows(expected);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    for (int stop : new int[]{0, 1, 3, 5, 13, Integer.MAX_VALUE}) {
                        final String read = printRows(cursor, factory.getMetadata(), stop);
                        Assert.assertTrue(read, rows.startsWith(read));
                        cursor.toTop();
                    }
                    Assert.assertEquals(rows, printRows(cursor, factory.getMetadata(), Integer.MAX_VALUE));
                }
            }
        });
    }

    @Test
    public void testDuplicatesAcrossTwoStagesInNestingOrder() throws Exception {
        assertMemoryLeak(() -> {
            createNestingTables();
            // For each match of a, newest first, every match of b, newest first, as the serial joins'
            // nested loops read them: whether the consumer expands both chains, or the workers walk
            // both stages because the second runs a post-join filter that reads the first.
            final String sql = "SELECT f.v, a.a, b.b FROM f LEFT JOIN a ON f.k = a.k LEFT JOIN b ON f.k = b.k";
            final String filtered = sql + " WHERE b.b <> 'b1' OR a.a = 'a2'";
            for (int frameRows : new int[]{1, 2, 1_000_000}) {
                sqlExecutionContext.changePageFrameSizes(frameRows, frameRows);
                assertQuery(sql)
                        .noLeakCheck()
                        .noRandomAccess()
                        .returns("""
                                v\ta\tb
                                10\ta4\tb3
                                10\ta4\tb1
                                10\ta2\tb3
                                10\ta2\tb1
                                10\ta1\tb3
                                10\ta1\tb1
                                20\ta3\tb2
                                30\t\tb4
                                """);
                assertQuery(filtered)
                        .noLeakCheck()
                        .noRandomAccess()
                        .returns("""
                                v\ta\tb
                                10\ta4\tb3
                                10\ta2\tb3
                                10\ta2\tb1
                                10\ta1\tb3
                                20\ta3\tb2
                                30\t\tb4
                                """);
                assertStack(sql, 2);
                TestUtils.assertContains(assertStack(filtered, 2), "postJoinFilter: ");
                // INNER joins of the same keys, over an interval of f, which never swaps.
                assertStack("SELECT f.v, a.a, b.b FROM f JOIN a ON f.k = a.k JOIN b ON f.k = b.k WHERE f.ts IN '2024-01-01'", 2);
            }
        });
    }

    @Test
    public void testEmptyBuildShortCircuitsOverAllStages() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // An INNER stage over an empty build, first, in the middle or last, empties the result,
            // and the execution joins no probe frame; every stage builds all the same, as every serial
            // join builds before it reads its master.
            final String[] emptyQueries = {
                    "SELECT t.v, e.w, d2.w2 FROM t JOIN (SELECT * FROM d1 WHERE w < 0) e ON t.k = e.k LEFT JOIN d2 ON t.lk = d2.lk WHERE t.v > 0",
                    "SELECT t.v, d1.w, e.w2, d3.w3 FROM t LEFT JOIN d1 ON t.k = d1.k JOIN (SELECT * FROM d2 WHERE w2 < 0) e ON t.lk = e.lk LEFT JOIN d3 ON t.sym = d3.s",
                    "SELECT t.v, d1.w, e.w3 FROM t LEFT JOIN d1 ON t.k = d1.k JOIN (SELECT * FROM d3 WHERE w3 < 0) e ON t.sym = e.s",
            };
            for (String sql : emptyQueries) {
                final int stageCount = sql.contains("d3.w3") ? 3 : 2;
                assertStack(sql, stageCount);
                try (RecordCursorFactory factory = select(sql)) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    Assert.assertNotNull(join);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.assertFalse(sql, cursor.hasNext());
                        Assert.assertEquals(sql, 0, join.getProbeFrameCount());
                        for (int k = 0; k < stageCount; k++) {
                            Assert.assertNotNull(sql, join.getAtom().getStage(k).getFrozenBuild());
                        }
                    }
                }
            }
            // A LEFT stage over an empty build reads its columns as NULLs.
            final String left = "SELECT t.v, d1.w, e.w2 FROM t LEFT JOIN d1 ON t.k = d1.k LEFT JOIN (SELECT * FROM d2 WHERE w2 < 0) e ON t.lk = e.lk";
            assertStack(left, 2);
            try (RecordCursorFactory factory = select(left)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertTrue(join.getProbeFrameCount() > 0);
                    Assert.assertEquals(0, join.getAtom().getStage(1).getFrozenBuild().getRowCount());
                }
            }
        });
    }

    @Test
    public void testFanOutAboveCapAheadOfDeferredStages() throws Exception {
        assertMemoryLeak(() -> {
            createNestingTables();
            // Frames of two rows and a cap of one tuple: the first stage runs an ON residual, so the
            // workers walk its matches, and f's first row yields three of them, which brings the task
            // past its cap of two tuples and leaves the frame's second row to a continuation. The
            // second stage hands its chains to the consumer, which expands them after the first
            // stage's matches, in the frame's tuples and in the continuation's.
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 1);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 1);
            sqlExecutionContext.changePageFrameSizes(2, 2);
            final String sql = "SELECT f.v, a.a, b.b, c.c FROM f LEFT JOIN a ON f.k = a.k AND a.w < f.v LEFT JOIN b ON f.k = b.k LEFT JOIN c ON f.k = c.k";
            assertQuery(sql)
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            v\ta\tb\tc
                            10\ta4\tb3\tc2
                            10\ta4\tb3\tc1
                            10\ta4\tb1\tc2
                            10\ta4\tb1\tc1
                            10\ta2\tb3\tc2
                            10\ta2\tb3\tc1
                            10\ta2\tb1\tc2
                            10\ta2\tb1\tc1
                            10\ta1\tb3\tc2
                            10\ta1\tb3\tc1
                            10\ta1\tb1\tc2
                            10\ta1\tb1\tc1
                            20\t\tb2\t
                            30\t\tb4\t
                            """);
            TestUtils.assertContains(assertStack(sql, 3), "onFilter: ");
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                Assert.assertEquals(ordinary(sql, sqlExecutionContext), result(factory, sqlExecutionContext));
                Assert.assertEquals(1, join.getAtom().getContinuationCount());
            }
            // The same over larger tables, with a backward probe scan too.
            createTables();
            sqlExecutionContext.changePageFrameSizes(3, 3);
            for (String sql2 : new String[]{
                    "SELECT t.k, t.v, d1.w, d2.w2, d3.w3 FROM t LEFT JOIN d1 ON t.k = d1.k AND d1.w % 3 <> t.v % 3 LEFT JOIN d2 ON t.lk = d2.lk LEFT JOIN d3 ON t.sym = d3.s",
                    "SELECT t.k, t.v, t.ts, d1.w, d2.w2 FROM t LEFT JOIN d1 ON t.k = d1.k AND d1.w % 3 <> t.v % 3 LEFT JOIN d2 ON t.lk = d2.lk ORDER BY t.ts DESC",
            }) {
                assertStack(sql2, sql2.contains("d3") ? 3 : 2);
                try (RecordCursorFactory factory = select(sql2)) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    Assert.assertNotNull(join);
                    result(factory, sqlExecutionContext);
                    Assert.assertTrue(join.getAtom().getContinuationCount() > 0);
                }
            }
        });
    }

    @Test
    public void testFilterCompileFailureWhileStacking() throws Exception {
        // test_throwing_filter() throws only in dev mode.
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        assertMemoryLeak(() -> {
            createTables();
            // The second join's ON residual compiles for the join's own state, and once more for the
            // worker's copy, which the planner compiles before the first join gives up its spec. Each
            // construction of the residual in turn fails the compile, and releases the residuals
            // constructed before it and the first join, until a compile constructs them all.
            final String sql = "SELECT t.k, t.v, d1.w, d2.w2 FROM t LEFT JOIN d1 ON t.k = d1.k"
                    + " LEFT JOIN d2 ON t.lk = d2.lk AND (test_throwing_filter() OR d2.w2 > t.v)";
            boolean isWorkerCopyFailed = false;
            int call = 1;
            for (; ; call++) {
                TestThrowingFilterFunctionFactory.reset(call);
                try (RecordCursorFactory factory = select(sql)) {
                    Assert.assertNotNull(find(factory));
                    break;
                } catch (SqlException e) {
                    Assert.assertEquals(sql.indexOf("test_throwing_filter"), e.getPosition());
                    TestUtils.assertContains(e.getFlyweightMessage(), "test_throwing_filter: configured to throw on call " + call);
                    Assert.assertEquals(call, TestThrowingFilterFunctionFactory.CONSTRUCT_COUNT.get());
                    Assert.assertEquals(call - 1, TestThrowingFilterFunctionFactory.CLOSE_COUNT.get());
                    for (StackTraceElement frame : e.getStackTrace()) {
                        isWorkerCopyFailed |= frame.getMethodName().equals("stackParallelHashJoin");
                    }
                } finally {
                    TestThrowingFilterFunctionFactory.reset(-1);
                }
            }
            Assert.assertTrue(call > 1);
            Assert.assertTrue(isWorkerCopyFailed);
            // The tables still work.
            TestUtils.assertContains(assertStack(sql, 2), "onFilter: (test_throwing_filter() or t.v<d2.w2)");
        });
    }

    @Test
    public void testGroupByOverStack() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // The fused GROUP BY takes a single join; over a stack, a serial GROUP BY reads the
            // operator's rows.
            final String sql = "SELECT t.k, count() c, sum(d1.w) sw, sum(d2.w2) sw2, max(d3.w3) mw3"
                    + " FROM t LEFT JOIN d1 ON t.k = d1.k JOIN d2 ON t.lk = d2.lk LEFT JOIN d3 ON t.sym = d3.s ORDER BY t.k";
            final String plan = assertStack(sql, 3);
            TestUtils.assertContains(plan, "GroupBy vectorized: false");
            Assert.assertFalse(plan, plan.contains("Async Hash Join Group By"));
            // A single join under GROUP BY stays fused.
            final String single = "SELECT t.k, count() c, sum(d1.w) sw FROM t LEFT JOIN d1 ON t.k = d1.k ORDER BY t.k";
            try (RecordCursorFactory factory = select(single)) {
                TestUtils.assertContains(plan(factory, sqlExecutionContext), "Async Hash Join Group By");
            }
        });
    }

    @Test
    public void testHandStackedSpecCarriesStagesAndOrder() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String expected = rows(ordinary("SELECT * FROM t LEFT JOIN d1 ON t.k = d1.k LEFT JOIN d2 ON t.lk = d2.lk", sqlExecutionContext));
            try (
                    RecordCursorFactory query = select("SELECT * FROM t LEFT JOIN d1 ON t.k = d1.k");
                    RecordCursorFactory d2Query = select("SELECT * FROM d2")
            ) {
                final AsyncHashJoinRecordCursorFactory join = find(query);
                Assert.assertNotNull(join);
                Assert.assertFalse(join.getSpec().isOrderDetermined());
                // A parent asks the first factory about its order, which fixes it.
                Assert.assertEquals(RecordCursorFactory.SCAN_DIRECTION_FORWARD, join.getScanDirection());
                Assert.assertTrue(join.getSpec().isOrderDetermined());
                // A factory that is not warmed up defines no stepper copy.
                Assert.assertEquals(0, join.getAtom().getStepperCopyCount());
                // A later join steals the spec, as the planner does, and appends its stage.
                final AsyncHashJoinSpec spec = join.stealSpec();
                join.halfClose();
                final RecordCursorFactory d2 = d2Query.getBaseFactory();
                final RecordMetadata leftMetadata = spec.getLeftInput().getMetadata();
                final RecordMetadata d2Metadata = d2.getMetadata();
                final HashJoinGroupByKeys keys = new HashJoinGroupByKeys();
                final int lk = leftMetadata.getColumnIndex("lk");
                final int d2lk = d2Metadata.getColumnIndex("lk");
                Assert.assertTrue(keys.add(lk, leftMetadata.getColumnType(lk), d2lk, d2Metadata.getColumnType(d2lk)));
                final HashJoinKeyLayout keyLayout = new HashJoinKeyLayout(configuration, new BytecodeAssembler(), keys,
                        "t", leftMetadata, identity(leftMetadata.getColumnCount()), "d2", d2Metadata, identity(d2Metadata.getColumnCount()));
                final JoinRecordMetadata metadata = new JoinRecordMetadata(configuration, spec.getMetadata().getColumnCount() + d2Metadata.getColumnCount());
                metadata.copyColumnMetadataFrom(null, spec.getMetadata());
                metadata.copyColumnMetadataFrom("d2", d2Metadata);
                metadata.setTimestampIndex(spec.getMetadata().getTimestampIndex());
                spec.addStage(
                        new HashJoinStageSpec(
                                new HashJoinInput(d2, identity(d2Metadata.getColumnCount()), new AsyncFilterContext(configuration, null, null, null, null, null, null, 1, 0, 0, 0)),
                                keyLayout,
                                true,
                                "d2.lk=t.lk",
                                metadata,
                                null,
                                null
                        ),
                        metadata
                );
                try (
                        AsyncHashJoinRecordCursorFactory stack = new AsyncHashJoinRecordCursorFactory(
                                engine,
                                configuration,
                                engine.getMessageBus(),
                                spec,
                                () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                                1
                        )
                ) {
                    // The order answer went on with the spec. The stack defines no stepper copy
                    // either, until it is warmed up.
                    Assert.assertTrue(stack.getSpec().isOrderDetermined());
                    TestUtils.assertContains(plan(stack, sqlExecutionContext), "stages: 2");
                    Assert.assertEquals(expected, rows(result(stack, sqlExecutionContext)));
                    Assert.assertEquals(expected, rows(result(stack, sqlExecutionContext)));
                    Assert.assertEquals(0, stack.getAtom().getStepperCopyCount());
                    Assert.assertFalse(stack.getStepperClass(0).isHidden());
                    Assert.assertFalse(stack.getStepperClass(1).isHidden());
                }
            }
            // A first join that may swap its inputs keeps a parent's answer through a rebuild over
            // its stolen spec, and swaps no more.
            try (RecordCursorFactory query = select("SELECT * FROM t JOIN d1 ON t.k = d1.k")) {
                final AsyncHashJoinRecordCursorFactory join = find(query);
                Assert.assertNotNull(join);
                Assert.assertTrue(join.getSpec().isInputSwapPossible());
                join.getScanDirection();
                final AsyncHashJoinSpec spec = join.stealSpec();
                join.halfClose();
                try (
                        AsyncHashJoinRecordCursorFactory rebuilt = new AsyncHashJoinRecordCursorFactory(
                                engine,
                                configuration,
                                engine.getMessageBus(),
                                spec,
                                () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                                1
                        )
                ) {
                    Assert.assertTrue(rebuilt.getSpec().isOrderDetermined());
                    Assert.assertFalse(rebuilt.getSpec().isInputSwapPossible());
                    Assert.assertFalse(plan(rebuilt, sqlExecutionContext).contains("inputSwap"));
                }
            }
        });
    }

    @Test
    public void testLaterStageBuildFailure() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // d2.w2 * 1000 does not fit DECIMAL(4,0) from w2 = 10 on, so the second stage's build
            // filter fails on its first row, on the owner and in rounds, after the first stage built.
            final String sql = "SELECT t.v, d1.w, x.w2 FROM t LEFT JOIN d1 ON t.k = d1.k LEFT JOIN (SELECT * FROM d2 WHERE (w2 * 1000)::DECIMAL(4,0) > 0) x ON t.lk = x.lk";
            for (long minRows : new long[]{0, Long.MAX_VALUE}) {
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, minRows);
                final String expected = error(sql, false);
                TestUtils.assertContains(expected, "inconvertible value: 10000 [LONG -> DECIMAL(4,0)]");
                Assert.assertEquals(sql, expected, error(sql, true));
                try (RecordCursorFactory factory = select(sql)) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    Assert.assertNotNull(join);
                    TestUtils.assertContains(plan(factory, sqlExecutionContext), "buildFilter: ");
                    for (int run = 0; run < 2; run++) {
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            //noinspection StatementWithEmptyBody
                            while (cursor.hasNext()) {
                            }
                            Assert.fail("expected the build filter to fail");
                        } catch (CairoException | ImplicitCastException e) {
                            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value");
                        }
                        assertReleased(join, 2);
                    }
                }
            }
            // The tables still work, and so does a stack whose filter passes the rows.
            assertStack("SELECT t.v, d1.w, x.w2 FROM t LEFT JOIN d1 ON t.k = d1.k LEFT JOIN (SELECT * FROM d2 WHERE (w2 * 10)::DECIMAL(4,0) > 0) x ON t.lk = x.lk", 2);
        });
    }

    @Test
    public void testLeftThenInnerAndInnerThenLeft() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            for (int frameRows : new int[]{1, 3, 1_000_000}) {
                sqlExecutionContext.changePageFrameSizes(frameRows, frameRows);
                for (String sql : new String[]{
                        // LEFT then INNER
                        "SELECT t.k, t.v, d1.w, d1.s, d2.w2, d2.q FROM t LEFT JOIN d1 ON t.k = d1.k JOIN d2 ON t.lk = d2.lk",
                        "SELECT t.k, t.v, d1.w, d3.w3, d3.note FROM t LEFT JOIN d1 ON t.k = d1.k JOIN d3 ON t.sym = d3.s",
                        // INNER then LEFT, the first join over a filtered or an interval scan of t, so
                        // that it never swaps
                        "SELECT t.k, t.v, d1.w, d1.vch, d2.w2 FROM t JOIN d1 ON t.k = d1.k LEFT JOIN d2 ON t.lk = d2.lk WHERE t.v > 7",
                        "SELECT t.k, t.v, d1.w, d3.note FROM t JOIN d1 ON t.k = d1.k LEFT JOIN d3 ON t.sym = d3.s WHERE t.ts IN '2024-01-01T02'",
                        // INNER then INNER, and LEFT then LEFT
                        "SELECT t.k, t.v, d1.w, d2.w2, d2.q FROM t JOIN d1 ON t.k = d1.k JOIN d2 ON t.lk = d2.lk WHERE t.v > 7",
                        "SELECT * FROM t LEFT JOIN d1 ON t.k = d1.k LEFT JOIN d2 ON t.lk = d2.lk",
                }) {
                    assertStack(sql, 2);
                }
            }
        });
    }

    @Test
    public void testMixedPlans() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // A later join whose key reads a column of an earlier stage's build input, a snowflake
            // join, wraps the operator as an ordinary join's master.
            assertQuery("SELECT t.k, d1.w, x.w2 FROM t LEFT JOIN d1 ON t.k = d1.k LEFT JOIN d2 x ON d1.w = x.w2")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Hash Left Outer Join Light
                                  condition: x.w2=d1.w
                                    Async Hash Join workers: 1
                                      stages: 1
                                        Probe
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: t
                                        Build
                                          joinType: left outer
                                          condition: d1.k=t.k
                                          buildPayload: copied when the probe is large enough
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: d1
                                    Hash
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: d2
                            """);
            // A first join that may swap its inputs takes a later join as a stage of its own, whose keys
            // read the left input's columns in either orientation; only the first join names the swap.
            assertQuery("SELECT t.k, d1.w, d2.w2 FROM t JOIN d1 ON t.k = d1.k JOIN d2 ON t.lk = d2.lk")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Async Hash Join workers: 1
                                  stages: 2
                                    Probe
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t
                                    Build
                                      joinType: inner
                                      condition: d1.k=t.k
                                      inputSwap: when the left input is smaller
                                      buildPayload: copied when the probe is large enough
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: d1
                                    Build
                                      joinType: inner
                                      condition: d2.lk=t.lk
                                      buildPayload: copied when the probe is large enough
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: d2
                            """);
            assertStack("SELECT t.k, d1.w, d2.w2 FROM t JOIN d1 ON t.k = d1.k JOIN d2 ON t.lk = d2.lk", 2);
            for (String sql : new String[]{
                    "SELECT t.k, d1.w, x.w2 FROM t LEFT JOIN d1 ON t.k = d1.k LEFT JOIN d2 x ON d1.w = x.w2",
                    // A build that exposes no page frames.
                    "SELECT t.k, d1.w, g.w2 FROM t LEFT JOIN d1 ON t.k = d1.k LEFT JOIN (SELECT lk, max(w2) w2 FROM d2) g ON t.lk = g.lk",
                    // A RIGHT and a FULL join.
                    "SELECT t.k, d1.w, d2.w2 FROM t LEFT JOIN d1 ON t.k = d1.k RIGHT JOIN d2 ON t.lk = d2.lk",
                    "SELECT t.k, d1.w, d2.w2 FROM t LEFT JOIN d1 ON t.k = d1.k FULL JOIN d2 ON t.lk = d2.lk",
                    // A later join with an ON residual that the workers cannot run.
                    "SELECT t.k, d1.w, d2.w2 FROM t LEFT JOIN d1 ON t.k = d1.k LEFT JOIN d2 ON t.lk = d2.lk AND d2.w2::SYMBOL <> '30'",
                    // A post-join filter that the workers cannot run sits between the first join and
                    // the second, which then joins the filter's rows.
                    "SELECT t.k, d1.w, d2.w2 FROM t LEFT JOIN d1 ON t.k = d1.k AND d1.w > t.v LEFT JOIN d2 ON t.lk = d2.lk WHERE d1.w::SYMBOL <> '3' OR t.v > 50",
            }) {
                final String plan = assertParallelHashJoin(sql, sqlExecutionContext);
                TestUtils.assertContains(plan, "stages: 1");
                Assert.assertTrue(sql + '\n' + plan, plan.contains("Hash Join Light") || plan.contains("Hash Left Outer Join") || plan.contains("Hash Full Outer Join") || plan.contains("Hash Right Outer Join"));
            }
            // A stack, then a snowflake join over it, then a star join over that, which no operator
            // absorbs: its master is an ordinary join.
            final String plan = assertParallelHashJoin(
                    "SELECT t.k, d1.w, x.w2, d3.w3 FROM t LEFT JOIN d1 ON t.k = d1.k LEFT JOIN d2 ON t.lk = d2.lk LEFT JOIN d2 x ON d1.w = x.w2 LEFT JOIN d3 ON t.sym = d3.s",
                    sqlExecutionContext
            );
            TestUtils.assertContains(plan, "stages: 2");
            Assert.assertEquals(plan, 2, countMatches(plan, "Hash Left Outer Join Light"));
            // A WHERE whose value is a runtime constant gates the stack's rows, or the rows of the
            // joins it filters.
            execute("CREATE TABLE flag AS (SELECT true b FROM long_sequence(1))");
            assertParallelHashJoin("SELECT t.k, d1.w, d2.w2 FROM t LEFT JOIN d1 ON t.k = d1.k LEFT JOIN d2 ON t.lk = d2.lk WHERE (SELECT b FROM flag LIMIT 1)", sqlExecutionContext);
            assertParallelHashJoin("SELECT t.k, d1.w, d2.w2 FROM t LEFT JOIN d1 ON t.k = d1.k LEFT JOIN d2 ON t.lk = d2.lk WHERE (SELECT b FROM flag LIMIT 1) AND d2.w2 > d1.w", sqlExecutionContext);
        });
    }

    @Test
    public void testOrderDeterminedThroughStack() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // ORDER BY the fact table's timestamp asks the stack for its scan direction, which its left
            // input answers as compiled, so the stack keeps its order and nothing sorts, as in the
            // ordinary plan.
            for (String sql : new String[]{
                    "SELECT t.ts, t.k, d1.w, d2.w2, d3.w3 FROM t LEFT JOIN d1 ON t.k = d1.k LEFT JOIN d2 ON t.lk = d2.lk LEFT JOIN d3 ON t.sym = d3.s ORDER BY t.ts",
                    "SELECT t.ts, t.k, d1.w, d2.w2, d3.w3 FROM t LEFT JOIN d1 ON t.k = d1.k LEFT JOIN d2 ON t.lk = d2.lk LEFT JOIN d3 ON t.sym = d3.s ORDER BY t.ts DESC",
                    "SELECT t.ts, t.k, d1.w, d2.w2 FROM t JOIN d1 ON t.k = d1.k JOIN d2 ON t.lk = d2.lk WHERE t.v > 3 ORDER BY t.ts DESC",
            }) {
                final String plan = assertStack(sql, sql.contains("d3") ? 3 : 2);
                Assert.assertEquals(sql + '\n' + plan, ordinaryPlan(sql).contains("Sort"), plan.contains("Sort"));
                Assert.assertFalse(plan, plan.contains("Sort"));
                try (RecordCursorFactory factory = select(sql)) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    Assert.assertNotNull(join);
                    Assert.assertTrue(join.getSpec().isOrderDetermined());
                    Assert.assertEquals(sql.endsWith("DESC") ? RecordCursorFactory.SCAN_DIRECTION_BACKWARD : RecordCursorFactory.SCAN_DIRECTION_FORWARD, join.getScanDirection());
                }
            }
            // The rows of a stack whose order no parent asked about come in the left input's order
            // too, and the spec records no answer.
            try (RecordCursorFactory factory = select("SELECT t.ts, t.k, d1.w, d2.w2 FROM t LEFT JOIN d1 ON t.k = d1.k LEFT JOIN d2 ON t.lk = d2.lk")) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                Assert.assertFalse(join.getSpec().isOrderDetermined());
            }
        });
    }

    @Test
    public void testPlans() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            // One Build node per stage, in stage order, each with its own join type, keys, residual,
            // post-join filter and build filter; the probe filter prints once, on the operator.
            final String sql = "SELECT t.k, d1.w, x.w2, d3.w3 FROM t"
                    + " LEFT JOIN d1 ON t.k = d1.k AND d1.w > t.v"
                    + " JOIN (SELECT * FROM d2 WHERE w2 > 10) x ON t.lk = x.lk"
                    + " LEFT JOIN d3 ON t.sym = d3.s"
                    + " WHERE t.v > 2 AND x.w2 < t.v * 10";
            assertQuery(sql)
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Async Hash Join workers: 1
                                  stages: 3
                                  filter: 2<v
                                    Probe
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t
                                    Build
                                      joinType: left outer
                                      condition: d1.k=t.k
                                      buildPayload: copied when the probe is large enough
                                      onFilter: t.v<d1.w
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: d1
                                    Build
                                      joinType: inner
                                      condition: x.lk=t.lk
                                      buildPayload: copied when the probe is large enough
                                      postJoinFilter: x.w2<t.v*10
                                      buildFilter: 10<w2
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: d2
                                    Build
                                      joinType: left outer
                                      condition: d3.s=t.sym
                                      symbolKeyJoin: true
                                      buildPayload: copied when the probe is large enough
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: d3
                            """);
            assertStack(sql, 3);
            // The ordinary plan prints the same filters.
            final String ordinaryPlan = ordinaryPlan(sql);
            TestUtils.assertContains(ordinaryPlan, "filter: t.v<d1.w");
            TestUtils.assertContains(ordinaryPlan, "filter: x.w2<t.v*10");
            // A plan of eight stages.
            final String eight = assertStack(EIGHT_STAGES, 8);
            Assert.assertEquals(eight, 8, countMatches(eight, " Build "));
        });
    }

    @Test
    public void testPostFilterOnLaterStageUnderOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // The serial filter over the second join answers followedOrderByAdvice() with false,
            // without asking the joins, and so does the stack that absorbed it into its second stage;
            // a parent that asks then plans its sort as over the serial filter.
            for (String orderBy : new String[]{"t.ts DESC", "t.ts", "d2.w2, t.v", "t.v DESC"}) {
                for (String sql : new String[]{
                        "SELECT t.k, t.v, t.ts, d1.w, d2.w2 FROM t LEFT JOIN d1 ON t.k = d1.k LEFT JOIN d2 ON t.lk = d2.lk WHERE d2.w2 IS NULL OR d2.w2 < t.v ORDER BY " + orderBy,
                        "SELECT t.k, t.v, t.ts, d1.w, d2.w2, d3.w3 FROM t LEFT JOIN d1 ON t.k = d1.k LEFT JOIN d2 ON t.lk = d2.lk LEFT JOIN d3 ON t.sym = d3.s WHERE d2.w2 IS NULL OR d2.w2 < t.v ORDER BY " + orderBy,
                }) {
                    final String plan = assertStack(sql, sql.contains("d3") ? 3 : 2);
                    TestUtils.assertContains(plan, "postJoinFilter: ");
                    Assert.assertEquals(sql + '\n' + plan, ordinaryPlan(sql).contains("Sort"), plan.contains("Sort"));
                }
            }
            try (RecordCursorFactory factory = select("SELECT t.k, t.ts, d1.w, d2.w2 FROM t LEFT JOIN d1 ON t.k = d1.k LEFT JOIN d2 ON t.lk = d2.lk WHERE d2.w2 IS NULL ORDER BY t.ts DESC")) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                Assert.assertFalse(join.followedOrderByAdvice());
                Assert.assertEquals(RecordCursorFactory.SCAN_DIRECTION_BACKWARD, join.getScanDirection());
            }
            try (RecordCursorFactory factory = select("SELECT t.k, t.ts, d1.w, d2.w2 FROM t LEFT JOIN d1 ON t.k = d1.k LEFT JOIN d2 ON t.lk = d2.lk ORDER BY t.ts DESC")) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                Assert.assertTrue(join.followedOrderByAdvice());
            }
        });
    }

    @Test
    public void testResidualsAndPostFiltersOnEachStage() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // Each stage of a three-stage star in turn, and all of them at once, runs an ON residual
            // and a post-join filter that read the stages before it, so that the workers walk every
            // stage up to it and the consumer expands the chains of the stages after it.
            final String[] residuals = {
                    " AND d1.w % 3 <> t.v % 3",
                    " AND (d2.w2 > t.v OR d2.q <> t.sym)",
                    " AND (d3.w3 > d1.w OR d3.note ~ 'd3_[12]')",
            };
            final String[] postFilters = {
                    "d1.w IS NULL OR d1.w < t.v",
                    "d2.w2 IS NULL OR d1.w IS NULL OR d2.w2 > d1.w",
                    "d3.w3 IS NULL OR d3.w3 > d2.w2 OR d3.s = t.sym",
            };
            for (int frameRows : new int[]{1, 3, 1_000_000}) {
                sqlExecutionContext.changePageFrameSizes(frameRows, frameRows);
                for (int stage = 0; stage < 4; stage++) {
                    for (int kind = 0; kind < 3; kind++) {
                        final boolean hasResidual = kind != 1;
                        final boolean hasPostFilter = kind != 0;
                        final StringSink sql = new StringSink();
                        sql.put("SELECT t.k, t.v, t.sym, d1.w, d1.s, d2.w2, d2.q, d3.w3, d3.note FROM t");
                        sql.put(" LEFT JOIN d1 ON t.k = d1.k").put(hasResidual && (stage == 0 || stage == 3) ? residuals[0] : "");
                        sql.put(" LEFT JOIN d2 ON t.lk = d2.lk").put(hasResidual && (stage == 1 || stage == 3) ? residuals[1] : "");
                        sql.put(" LEFT JOIN d3 ON t.sym = d3.s").put(hasResidual && (stage == 2 || stage == 3) ? residuals[2] : "");
                        if (hasPostFilter) {
                            sql.put(" WHERE ");
                            boolean isFirst = true;
                            for (int k = 0; k < 3; k++) {
                                if (k == stage || stage == 3) {
                                    sql.put(isFirst ? "(" : " AND (").put(postFilters[k]).put(')');
                                    isFirst = false;
                                }
                            }
                        }
                        final String plan = assertStack(sql.toString(), 3);
                        if (hasResidual) {
                            TestUtils.assertContains(plan, "onFilter: ");
                        }
                        if (hasPostFilter) {
                            TestUtils.assertContains(plan, "postJoinFilter: ");
                        }
                    }
                }
            }
            // An INNER stage's post-join filter, under a probe filter that keeps the first join from
            // swapping.
            assertStack("SELECT t.k, t.v, d1.w, d2.w2 FROM t JOIN d1 ON t.k = d1.k JOIN d2 ON t.lk = d2.lk WHERE t.v > 2 AND d1.w < t.v AND d2.w2 > d1.w", 2);
        });
    }

    @Test
    public void testRingSizedForStride() throws Exception {
        // Frames of four rows, and a ring budget of 576 bytes: a frame of tuples of three longs, a
        // probe row and two stages' entries, takes 96 bytes, and a frame of tuples of nine longs,
        // eight stages' entries, 288.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 16);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TUPLE_RING_MAX_SIZE, 576);
        assertMemoryLeak(() -> {
            createTables();
            // rt holds four rows a day, so each of its frames holds four rows.
            execute("CREATE TABLE rt AS (SELECT (x % 7)::INT k, (x % 6)::LONG lk, ('s' || (x % 4))::SYMBOL sym, x v, timestamp_sequence('2024-01-01', 21_600_000_000) ts FROM long_sequence(8)) TIMESTAMP(ts) PARTITION BY DAY");
            final int queueCapacity = configuration.getPageFrameReduceQueueCapacity();
            final String two = "SELECT t.v, d1.w, d2.w2 FROM rt t LEFT JOIN d1 ON t.k = d1.k LEFT JOIN d2 ON t.lk = d2.lk";
            assertRing(two, 3, Math.max(1, Math.min(16, Math.min(queueCapacity, 576 / (4 * 3 * 8)))));
            assertRing(EIGHT_STAGES.replace(" FROM t", " FROM rt t"), 9, Math.max(1, Math.min(16, Math.min(queueCapacity, 576 / (4 * 9 * 8)))));
        });
    }

    @Test
    public void testSharedStepperTemplate() throws Exception {
        assertMemoryLeak(() -> {
            // A JVM that cannot define hidden classes hands every stage the shared stepper template,
            // whose recursion into the next stage runs here, enumerated and deferred.
            AsyncHashJoinRecordCursorFactory.setStepperCopiesEnabledForTesting(false);
            try {
                createTables();
                sqlExecutionContext.changePageFrameSizes(3, 3);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 1);
                for (String sql : new String[]{
                        "SELECT t.k, t.v, d1.w, d2.w2, d3.w3 FROM t LEFT JOIN d1 ON t.k = d1.k LEFT JOIN d2 ON t.lk = d2.lk JOIN d3 ON t.sym = d3.s",
                        "SELECT t.k, t.v, d1.w, d2.w2, d3.w3 FROM t LEFT JOIN d1 ON t.k = d1.k LEFT JOIN d2 ON t.lk = d2.lk AND d2.w2 > t.v JOIN d3 ON t.sym = d3.s",
                        "SELECT t.k, t.v, d1.w, d2.w2 FROM t LEFT JOIN d1 ON t.k = d1.k JOIN d2 ON t.lk = d2.lk WHERE d2.w2 > d1.w",
                        // Unique builds that the workers walk, ahead of a stage with a residual, and
                        // LEFT misses of both.
                        "SELECT t.k, t.v, u1.w, u2.w2, d2.w2 FROM t LEFT JOIN u1 ON t.k = u1.k LEFT JOIN u2 ON t.lk = u2.lk LEFT JOIN d2 ON t.lk = d2.lk AND d2.w2 > t.v",
                        "SELECT t.k, t.v, u1.w, u2.w2 FROM t LEFT JOIN u1 ON t.k = u1.k AND u1.w > t.v % 4 JOIN u2 ON t.lk = u2.lk WHERE u2.w2 > u1.w OR u1.w IS NULL",
                        // A LEFT stage's post-join filter that keeps some of its misses and drops the
                        // rest.
                        "SELECT t.k, t.v, d1.w, d2.w2 FROM t LEFT JOIN d1 ON t.k = d1.k LEFT JOIN d2 ON t.lk = d2.lk WHERE d2.w2 > d1.w OR (d2.w2 IS NULL AND t.v % 4 = 0)",
                }) {
                    assertStack(sql, countMatches(sql, " JOIN "));
                    try (RecordCursorFactory factory = select(sql)) {
                        Assert.assertFalse(find(factory).getStepperClass(1).isHidden());
                    }
                }
            } finally {
                AsyncHashJoinRecordCursorFactory.setStepperCopiesEnabledForTesting(true);
            }
        });
    }

    @Test
    public void testStageCap() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // 32 joins of t's key run as the 32 stages of one operator, each of which probes through a
            // stepper class of its own.
            final String full = starOfJoins(AsyncHashJoinSpec.MAX_STAGE_COUNT);
            final String fullPlan = assertStack(full, AsyncHashJoinSpec.MAX_STAGE_COUNT);
            Assert.assertEquals(fullPlan, AsyncHashJoinSpec.MAX_STAGE_COUNT, countMatches(fullPlan, " Build "));
            Assert.assertFalse(fullPlan, fullPlan.contains("Hash Left Outer Join Light"));
            // The factory runs its copies from the first task, so it defines them as it compiles.
            useSteppers(true);
            try (RecordCursorFactory factory = select(full)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                Assert.assertEquals(AsyncHashJoinSpec.MAX_STAGE_COUNT, join.getSpec().getStageCount());
                final Set<Class<?>> stepperClasses = new HashSet<>();
                for (int k = 0; k < AsyncHashJoinSpec.MAX_STAGE_COUNT; k++) {
                    Assert.assertTrue(join.getStepperClass(k).isHidden());
                    stepperClasses.add(join.getStepperClass(k));
                }
                Assert.assertEquals(AsyncHashJoinSpec.MAX_STAGE_COUNT, stepperClasses.size());
            } finally {
                restoreSteppers();
            }
            // A 33rd join wraps the operator of the first 32 as an ordinary join's master.
            final String plan = assertParallelHashJoin(starOfJoins(AsyncHashJoinSpec.MAX_STAGE_COUNT + 1), sqlExecutionContext);
            Assert.assertEquals(plan, AsyncHashJoinSpec.MAX_STAGE_COUNT, getStageCount(plan));
            Assert.assertEquals(plan, AsyncHashJoinSpec.MAX_STAGE_COUNT, countMatches(plan, " Build "));
            Assert.assertEquals(plan, 1, countMatches(plan, "Hash Left Outer Join Light"));
            Assert.assertTrue(plan, plan.startsWith("SelectedRecord"));
            Assert.assertTrue(plan, plan.indexOf("Hash Left Outer Join Light") < plan.indexOf("Async Hash Join"));
            Assert.assertTrue(plan, plan.indexOf("condition: j32.s=t.sym") < plan.indexOf("Async Hash Join"));
        });
    }

    @Test
    public void testStars() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            for (int frameRows : new int[]{1, 3, 1_000_000}) {
                sqlExecutionContext.changePageFrameSizes(frameRows, frameRows);
                for (int jitMode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                    sqlExecutionContext.setJitMode(jitMode);
                    // Two and three stages: an INT, a staged LONG and a translated SYMBOL key, SYMBOL
                    // and var-size payloads, NULL keys, misses and duplicate keys.
                    assertStack("SELECT t.k, t.v, d1.w, d1.s, d1.vch, d2.w2, d2.q FROM t LEFT JOIN d1 ON t.k = d1.k LEFT JOIN d2 ON t.lk = d2.lk", 2);
                    assertStack("SELECT * FROM t LEFT JOIN d1 ON t.k = d1.k LEFT JOIN d2 ON t.lk = d2.lk LEFT JOIN d3 ON t.sym = d3.s", 3);
                    assertStack("SELECT t.k, t.v, d1.w, d2.w2, d3.w3, d3.note FROM t JOIN d1 ON t.k = d1.k LEFT JOIN d2 ON t.lk = d2.lk JOIN d3 ON t.sym = d3.s WHERE t.v % 3 <> 0", 3);
                    // Projections over the inputs, a stolen build filter on a later stage, a staged
                    // key of two columns and a key whose two sides differ in type.
                    assertStack("SELECT t2.v, x.w, y.w2, z.w3 FROM (SELECT v, sym, lk, k, ts FROM t) t2 LEFT JOIN (SELECT w, k FROM d1 WHERE w > 4) x ON t2.k = x.k"
                            + " LEFT JOIN (SELECT q, w2, lk FROM d2) y ON t2.lk = y.lk LEFT JOIN d3 z ON t2.sym = z.s AND t2.k = z.w3 % 7", 3);
                    assertStack("SELECT t.v, d1.w, d3.w3 FROM t LEFT JOIN d1 ON t.k = d1.k LEFT JOIN d3 ON t.sym = d3.ss", 2);
                    // Eight stages, a backward probe scan of them, and a LIMIT and a negative LIMIT.
                    assertStack(EIGHT_STAGES, 8);
                    assertStack(EIGHT_STAGES + " ORDER BY t.ts DESC", 8);
                    assertStack(EIGHT_STAGES + " LIMIT 7", 8);
                    assertStack(EIGHT_STAGES + " LIMIT -7", 8);
                }
            }
            useSteppers(true);
            try (RecordCursorFactory factory = select(EIGHT_STAGES)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                final Set<Class<?>> stepperClasses = new HashSet<>();
                for (int k = 0; k < 8; k++) {
                    stepperClasses.add(join.getStepperClass(k));
                }
                Assert.assertEquals(8, stepperClasses.size());
            } finally {
                restoreSteppers();
            }
        });
    }

    private static void assertCancelled(RecordCursorFactory factory, SqlExecutionCircuitBreaker breaker, String label) throws SqlException {
        final SqlExecutionCircuitBreaker previous = sqlExecutionContext.getCircuitBreaker();
        ((SqlExecutionContextImpl) sqlExecutionContext).with(breaker);
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            //noinspection StatementWithEmptyBody
            while (cursor.hasNext()) {
            }
            Assert.fail("expected cancellation: " + label);
        } catch (CairoException e) {
            Assert.assertTrue(label + ": " + e.getMessage(), e.isCancellation());
            TestUtils.assertContains(e.getFlyweightMessage(), "cancelled by user");
        } finally {
            ((SqlExecutionContextImpl) sqlExecutionContext).with(previous);
        }
        Assert.assertEquals(label, 0, engine.getBusyReaderCount());
    }

    // The failure released every slot, reader and build of every stage.
    private static void assertReleased(AsyncHashJoinRecordCursorFactory join, int stageCount) {
        Assert.assertEquals(0, join.getBuildRoundAtom().getPerWorkerLocks().getAcquiredSlotCount());
        Assert.assertEquals(0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
        for (int k = 0; k < stageCount; k++) {
            Assert.assertNull(join.getAtom().getStage(k).getFrozenBuild());
        }
        Assert.assertEquals(0, join.getAtom().getRing().getAllocatedBytes());
        Assert.assertEquals(0, engine.getBusyReaderCount());
    }

    // The failure released everything, and the same factory reads the ordinary plan's rows again.
    private static void assertReused(RecordCursorFactory factory, AsyncHashJoinRecordCursorFactory join, String sql, String expected) throws SqlException {
        assertReleased(join, join.getSpec().getStageCount());
        Assert.assertEquals(sql, expected, result(factory, sqlExecutionContext));
    }

    // While a cursor of the query is open, its ring holds tuples of the given longs, in as many entries
    // as the effective dispatch limit.
    private static void assertRing(String sql, int tupleStride, int dispatchLimit) throws SqlException {
        try (RecordCursorFactory factory = select(sql)) {
            final AsyncHashJoinRecordCursorFactory join = find(factory);
            Assert.assertNotNull(join);
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(sql, tupleStride, join.getAtom().getRing().getTupleStride());
                Assert.assertEquals(sql, dispatchLimit, join.getAtom().getRing().getDispatchLimit());
            }
        }
        assertStack(sql, tupleStride - 1);
    }

    // The query runs a stack of the given stages, whose rows equal the ordinary plan's, in order;
    // returns the operator's plan.
    private static String assertStack(String sql, int stageCount) throws SqlException {
        final String plan = assertParallelHashJoin(sql, sqlExecutionContext);
        Assert.assertEquals(sql + '\n' + plan, stageCount, getStageCount(plan));
        Assert.assertEquals(sql + '\n' + plan, 1, countMatches(plan, "Hash Join workers:"));
        return plan;
    }

    private static int countMatches(String text, String sub) {
        int count = 0;
        for (int i = text.indexOf(sub); i > -1; i = text.indexOf(sub, i + sub.length())) {
            count++;
        }
        return count;
    }

    // f, the fact table, and a, b and c, whose keys repeat, inserted so that each key's chain lists
    // its rows newest first: a's key 1 holds a1, a2 and a4, b's b1 and b3, c's c1 and c2.
    private static void createNestingTables() throws Exception {
        execute("CREATE TABLE f (k INT, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO f VALUES (1, 10, '2024-01-01T00:00:00.000000Z'), (2, 20, '2024-01-01T01:00:00.000000Z'), (3, 30, '2024-01-01T02:00:00.000000Z')");
        execute("CREATE TABLE a (k INT, a STRING, w LONG)");
        execute("INSERT INTO a VALUES (1, 'a1', 1), (1, 'a2', 2), (2, 'a3', 30), (1, 'a4', 4)");
        execute("CREATE TABLE b (k INT, b STRING)");
        execute("INSERT INTO b VALUES (1, 'b1'), (2, 'b2'), (1, 'b3'), (3, 'b4')");
        execute("CREATE TABLE c (k INT, c STRING)");
        execute("INSERT INTO c VALUES (1, 'c1'), (1, 'c2')");
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
                        CASE WHEN x % 5 = 0 THEN NULL ELSE 'str' || (x % 6) END str,
                        timestamp_sequence('2024-01-01', 600_000_000) ts
                    FROM long_sequence(60)
                ) TIMESTAMP(ts) PARTITION BY HOUR
                """);
        // d1 repeats its INT keys, NULL among them, inserts its symbols in another order than t, and
        // lacks keys 5 and 6.
        execute("""
                CREATE TABLE d1 AS (
                    SELECT
                        CASE WHEN x % 6 = 0 THEN NULL ELSE (x % 5)::INT END k,
                        x w,
                        ('s' || (5 - x % 5))::SYMBOL s,
                        CASE WHEN x % 4 = 0 THEN NULL ELSE 'v' || x END::VARCHAR vch
                    FROM long_sequence(20)
                )
                """);
        // d2 repeats its LONG keys, which stage into a map, and lacks key 0.
        execute("""
                CREATE TABLE d2 AS (
                    SELECT 1 + x % 5 lk, x * 10 w2, ('q' || (x % 3))::SYMBOL q, timestamp_sequence('2023-12-31', 600_000_000) dts
                    FROM long_sequence(15)
                ) TIMESTAMP(dts) PARTITION BY HOUR
                """);
        // d3 holds one row per SYMBOL, in another order than t's dictionary, and lacks s0; ss holds the
        // same texts as STRING.
        execute("""
                CREATE TABLE d3 AS (
                    SELECT ('s' || (5 - x))::SYMBOL s, x * 100 w3, 'd3_' || x note, 's' || (5 - x) ss
                    FROM long_sequence(4)
                )
                """);
        // u1 and u2 hold one row per key.
        execute("CREATE TABLE u1 AS (SELECT (x - 1)::INT k, x * 3 w FROM long_sequence(5))");
        execute("CREATE TABLE u2 AS (SELECT x lk, x * 7 w2 FROM long_sequence(4))");
    }

    private static String error(String sql, boolean isParallel) throws SqlException {
        sqlExecutionContext.setParallelHashJoinEnabled(isParallel);
        try (RecordCursorFactory factory = select(sql)) {
            Assert.assertEquals(sql, isParallel, find(factory) != null);
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                //noinspection StatementWithEmptyBody
                while (cursor.hasNext()) {
                }
                Assert.fail("expected the filter to fail: " + sql);
            } catch (CairoException | ImplicitCastException e) {
                return e.getPosition() + ": " + e.getFlyweightMessage();
            }
        } finally {
            sqlExecutionContext.setParallelHashJoinEnabled(true);
        }
        return null;
    }

    // The stages that the operator's plan names; the plan text holds no line breaks.
    private static int getStageCount(String plan) {
        final int start = plan.indexOf("stages: ") + "stages: ".length();
        int end = start;
        while (end < plan.length() && Character.isDigit(plan.charAt(end))) {
            end++;
        }
        return Integer.parseInt(plan.substring(start, end));
    }

    private static IntList identity(int columnCount) {
        final IntList columns = new IntList();
        for (int i = 0; i < columnCount; i++) {
            columns.add(i);
        }
        return columns;
    }

    private static boolean isOnStack(String className, String methodName) {
        return STACK_WALKER.walk(frames -> frames.anyMatch(frame -> frame.getClassName().equals(className) && frame.getMethodName().equals(methodName)));
    }

    private static String ordinaryPlan(String sql) throws SqlException {
        sqlExecutionContext.setParallelHashJoinEnabled(false);
        try (RecordCursorFactory factory = select(sql)) {
            return plan(factory, sqlExecutionContext);
        } finally {
            sqlExecutionContext.setParallelHashJoinEnabled(true);
        }
    }

    // Up to the given number of the cursor's next rows, as CursorPrinter prints them.
    private static String printRows(RecordCursor cursor, RecordMetadata metadata, int maxRows) {
        final StringSink sink = new StringSink();
        for (int rows = 0; rows < maxRows && cursor.hasNext(); rows++) {
            CursorPrinter.println(cursor.getRecord(), metadata, sink);
        }
        return sink.toString();
    }

    // A result's rows, without its header: the metadata lines of ParallelHashJoinTestUtils.result(),
    // or the column names of an assertQuery() expectation.
    private static String rows(String result) {
        if (result.startsWith("timestamp: ")) {
            return result.substring(result.indexOf('\n', result.indexOf('\n') + 1) + 1);
        }
        return result.substring(result.indexOf('\n') + 1);
    }

    // The error that reading the query's rows ends with, under the operator or under the ordinary plan.
    // A star of the given LEFT joins of d3 on t's SYMBOL key.
    private static String starOfJoins(int joinCount) {
        final StringSink sql = new StringSink();
        sql.put("SELECT t.v");
        for (int j = 0; j < joinCount; j++) {
            sql.put(", j").put(j).put(".w3");
        }
        sql.put(" FROM t");
        for (int j = 0; j < joinCount; j++) {
            sql.put(" LEFT JOIN d3 j").put(j).put(" ON t.sym = j").put(j).put(".s");
        }
        return sql.toString();
    }
}
