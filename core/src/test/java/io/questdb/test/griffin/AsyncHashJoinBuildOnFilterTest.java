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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.test.TestLatchedCounterFunctionFactory;
import io.questdb.griffin.engine.functions.test.TestThrowingFilterFunctionFactory;
import io.questdb.griffin.engine.table.AsyncHashJoinAtom;
import io.questdb.griffin.engine.table.AsyncHashJoinRecordCursorFactory;
import io.questdb.griffin.engine.table.HashJoinBuildStage;
import io.questdb.griffin.engine.table.MultiHashJoinRecord;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.test.griffin.ParallelHashJoinTestUtils.*;

/**
 * The parallel hash join's build-only ON pushdown: the leading conjuncts of a LEFT join's ON residual
 * that read the build's columns alone filter the build's rows instead of the pairs, and the pairs run
 * the rest of the residual. A conjunct that can cost far more on a build row than the hash insert it
 * spares, by the fused GROUP BY's rule (it is no comparison of columns, constants and bind variables,
 * and reads a variable-size value), runs over the build's rows only in an execution whose build table
 * holds no more rows than its probe table; any other execution runs the whole residual over the pairs.
 * A build whose conjuncts throw on a row gives them up for the execution, since the serial join may
 * never run them over that row. An execution whose build ran the whole residual leaves the pairs no
 * filter, so its stage hands its chains to the consumer, as a stage without a residual does, while
 * any other execution of the same factory walks the matches on the workers. Every query compares with
 * the ordinary plan, in order, and the build's row count with the rows of the build table that pass
 * the pushed conjuncts, counted by a plain query.
 */
public class AsyncHashJoinBuildOnFilterTest extends AbstractCairoTest {
    private static final String PUSHDOWN_RULE = "buildOnFilterPushdown: when the build table holds no more rows than the probe table";
    // ON residuals of t LEFT JOIN d ON t.k = d.k: the residual, the leading conjuncts that read d
    // alone, null for none, and whether one of them can cost far more than the hash insert it spares.
    private static final Object[][] RESIDUALS = {
            // Comparisons of columns, constants and bind variables, cheap whatever their columns.
            {"d.w > 10", "d.w > 10", false},
            {"d.s = 's2'", "d.s = 's2'", false},
            {"d.vch = 'v3'", "d.vch = 'v3'", false},
            {"d.vch IS NULL", "d.vch IS NULL", false},
            {"(d.str <> 'a' OR d.w < 5)", "(d.str <> 'a' OR d.w < 5)", false},
            {"NOT (d.w >= 10 AND d.s = 's1')", "NOT (d.w >= 10 AND d.s = 's1')", false},
            // Functions over fixed-size columns, and over SYMBOL columns, whose value they compare.
            {"d.w % 3 = 0", "d.w % 3 = 0", false},
            {"(d.w * 10)::DECIMAL(5,0) > 0::DECIMAL(5,0)", "(d.w * 10)::DECIMAL(5,0) > 0::DECIMAL(5,0)", false},
            {"d.w IN (3, 5, 7, 11)", "d.w IN (3, 5, 7, 11)", false},
            {"abs(d.w - 20) > 5", "abs(d.w - 20) > 5", false},
            {"coalesce(d.w, d.k, 0) > 10", "coalesce(d.w, d.k, 0) > 10", false},
            {"lower(d.s) = 's2'", "lower(d.s) = 's2'", false},
            {"CASE WHEN d.w > 3 THEN 'a' ELSE 'b' END = 'a'", "CASE WHEN d.w > 3 THEN 'a' ELSE 'b' END = 'a'", false},
            // Functions over variable-size columns, or over variable-size values that they compute.
            {"d.vch ~ 'v[12]'", "d.vch ~ 'v[12]'", true},
            {"length(d.str) > 4", "length(d.str) > 4", true},
            {"replace(d.vch, 'v', 'w') = 'w3'", "replace(d.vch, 'v', 'w') = 'w3'", true},
            {"lower(d.s) ~ 's[23]'", "lower(d.s) ~ 's[23]'", true},
            {"concat('x', lower(d.s)) = 'xs1'", "concat('x', lower(d.s)) = 'xs1'", true},
            {"replace(lower(d.s), 's', 'x') = 'x1'", "replace(lower(d.s), 's', 'x') = 'x1'", true},
            {"concat('a', 'b', lower(d.s)) = 'abs1'", "concat('a', 'b', lower(d.s)) = 'abs1'", true},
            {"concat(lower(d.s), 'a', 'b') = 's1ab'", "concat(lower(d.s), 'a', 'b') = 's1ab'", true},
            // Several leading conjuncts, the rest of the residual after them.
            {"d.w > 3 AND d.w < 30", "d.w > 3 AND d.w < 30", false},
            {"d.w % 2 = 0 AND d.w > t.v", "d.w % 2 = 0", false},
            {"d.w % 2 = 0 AND d.vch ~ 'v1' AND t.v > 3", "d.w % 2 = 0 AND d.vch ~ 'v1'", true},
            {"d.vch ~ 'v1' AND d.w % 2 = 0 AND (d.w < t.v OR t.str IS NULL)", "d.vch ~ 'v1' AND d.w % 2 = 0", true},
            // A conjunct of the build alone after one that reads the probe row stays with the pairs.
            {"d.w > t.v AND d.w % 2 = 0", null, false},
            {"coalesce(d.w, t.v, 0) > 10 AND d.w % 2 = 0", null, false},
            {"t.v % 4 = 0", null, false},
            {"t.v % 4 = 0 AND d.w > 2", null, false},
            {"(d.vch IS NULL OR t.str IS NULL)", null, false},
    };

    @Override
    @Before
    public void setUp() {
        super.setUp();
        // read_parquet() reads files under the input root.
        inputRoot = root;
        sqlExecutionContext.setParallelHashJoinEnabled(true);
    }

    @After
    public void tearDownPushdown() {
        sqlExecutionContext.restoreToDefaultPageFrameSizes();
        TestLatchedCounterFunctionFactory.reset(null);
        TestThrowingFilterFunctionFactory.reset(-1);
    }

    @Test
    public void testAbandonedConjunctsLeaveTheResidualToThePairs() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // test_latched_counter() reads no column, so it leads the residual's conjuncts of the build
            // alone, and fails on the build's rows alone, as a value it cannot convert would: the build
            // gives the conjuncts up, and the pairs run the whole residual, which never fails on them.
            final String sql = "SELECT t.k, t.v, d.w FROM t LEFT JOIN d ON t.k = d.k AND test_latched_counter() AND d.w % 3 = 0";
            final AtomicInteger buildCalls = new AtomicInteger();
            final AtomicInteger pairCalls = new AtomicInteger();
            final int[] failingCall = {0};
            final TestLatchedCounterFunctionFactory.Callback callback = new TestLatchedCounterFunctionFactory.Callback() {
                @Override
                public boolean onGet(Record rec, int count) {
                    if (rec instanceof PageFrameMemoryRecord) {
                        if (buildCalls.incrementAndGet() == failingCall[0]) {
                            throw failingCall[0] % 2 == 0
                                    ? CairoException.nonCritical().put("test value failure")
                                    : ImplicitCastException.inconvertibleValue(count, ColumnType.INT, ColumnType.BOOLEAN);
                        }
                    } else if (rec instanceof MultiHashJoinRecord) {
                        pairCalls.incrementAndGet();
                    }
                    return true;
                }
            };
            TestLatchedCounterFunctionFactory.reset(callback);
            final String expected = ordinary(sql, sqlExecutionContext);
            sqlExecutionContext.changePageFrameSizes(3, 3);
            for (long minRows : new long[]{Long.MAX_VALUE, 0}) {
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, minRows);
                try (RecordCursorFactory factory = select(sql)) {
                    final HashJoinBuildStage stage = find(factory).getAtom().getStage(0);
                    // d's 40 rows: the first build row, one in the middle of a frame, and the last row;
                    // an execution without failures between them runs the conjuncts again.
                    for (int failing : new int[]{0, 1, 0, 20, 40, 0}) {
                        failingCall[0] = failing;
                        buildCalls.set(0);
                        pairCalls.set(0);
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            Assert.assertTrue(cursor.hasNext());
                            Assert.assertEquals(failing == 0, stage.isBuildOnFilterApplied());
                            // The build ran the conjuncts up to the failing row, d's row of w = failing,
                            // and then gave them up: it keeps that row and every later one.
                            Assert.assertEquals(failing == 0 ? 40 : failing, buildCalls.get());
                            Assert.assertEquals(
                                    failing == 0
                                            ? count("SELECT count() FROM d WHERE w % 3 = 0")
                                            : count("SELECT count() FROM d WHERE w < " + failing + " AND w % 3 = 0") + 41 - failing,
                                    stage.getFrozenBuild().getRowCount()
                            );
                        }
                        // Without failures the pairs run the rest of the residual alone.
                        Assert.assertEquals(failing == 0, pairCalls.get() == 0);
                        buildCalls.set(-1_000_000);
                        Assert.assertEquals(expected, result(factory, sqlExecutionContext));
                    }
                }
            }
        });
    }

    @Test
    public void testBindVariables() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // One factory runs each execution with that execution's values, in the build's rows and in
            // the pairs.
            final String sql = "SELECT t.k, t.v, d.w FROM t LEFT JOIN d ON t.k = d.k AND d.w > $1 AND d.w < t.v + $2";
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                TestUtils.assertContains(plan(factory, sqlExecutionContext), "buildOnFilter: $0::long<w");
                for (long[] values : new long[][]{{5, 0}, {20, 10}, {-1, 100}, {100, 0}}) {
                    bindVariableService.clear();
                    bindVariableService.setLong(0, values[0]);
                    bindVariableService.setLong(1, values[1]);
                    Assert.assertEquals(ordinary(sql, sqlExecutionContext), result(factory, sqlExecutionContext));
                    assertBuildRows(factory, 0, true, count("SELECT count() FROM d WHERE w > " + values[0]));
                }
            }
            // A BOOLEAN bind variable reads no column, so it leads the conjuncts of the build alone.
            final String flagged = "SELECT t.k, t.v, d.w FROM t LEFT JOIN d ON t.k = d.k AND $1 AND d.w % 2 = 0";
            for (boolean value : new boolean[]{true, false}) {
                bindVariableService.clear();
                bindVariableService.setBoolean(0, value);
                TestUtils.assertContains(assertParallelHashJoin(flagged, sqlExecutionContext), "buildOnFilter: ($0::boolean and w%2=0)");
                try (RecordCursorFactory factory = select(flagged)) {
                    assertBuildRows(factory, 0, true, value ? count("SELECT count() FROM d WHERE w % 2 = 0") : 0);
                }
            }
        });
    }

    @Test
    public void testCompileFailuresReleaseTheFilters() throws Exception {
        // test_throwing_filter() throws only in dev mode.
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        assertMemoryLeak(() -> {
            createTables();
            // The residual's first conjunct reads d alone, and its cost check parses the text that the
            // CASE computes for the regex, which constructs test_throwing_filter() once more; then the leading conjunct
            // compiles against d's columns, and the rest of the residual against the joined rows, each
            // with a worker's copy. Each construction of test_throwing_filter() in turn fails the
            // compile and releases what the compile constructed before it, except the one of the cost
            // check, which leaves the conjunct to the pairs.
            final String sql = "SELECT t.k, t.v, d.w FROM t LEFT JOIN d ON t.k = d.k"
                    + " AND CASE WHEN test_throwing_filter() THEN d.s::STRING END ~ 's[12]' AND (test_throwing_filter() OR d.w > t.v)";
            final String expected = ordinary(sql, sqlExecutionContext);
            int unpushedCompiles = 0;
            int failures = 0;
            for (int call = 1; ; call++) {
                TestThrowingFilterFunctionFactory.reset(call);
                try (RecordCursorFactory factory = select(sql)) {
                    Assert.assertNotNull(find(factory));
                    if (plan(factory, sqlExecutionContext).contains("buildOnFilter: ")) {
                        Assert.assertEquals(expected, result(factory, sqlExecutionContext));
                        break;
                    }
                    // The cost check failed: the compile went on without the pushdown.
                    Assert.assertEquals(1, ++unpushedCompiles);
                    Assert.assertEquals(call, TestThrowingFilterFunctionFactory.CONSTRUCT_COUNT.get());
                    Assert.assertEquals(expected, result(factory, sqlExecutionContext));
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "test_throwing_filter: configured to throw on call " + call);
                    Assert.assertEquals(call, TestThrowingFilterFunctionFactory.CONSTRUCT_COUNT.get());
                    Assert.assertEquals(call - 1, TestThrowingFilterFunctionFactory.CLOSE_COUNT.get());
                    failures++;
                } finally {
                    TestThrowingFilterFunctionFactory.reset(-1);
                }
            }
            Assert.assertEquals(1, unpushedCompiles);
            // The ordinary compile's residual and its worker copy, the pushed conjunct and its copy, the
            // rest of the residual and its copy.
            Assert.assertTrue(String.valueOf(failures), failures >= 6);

            // The join fails to construct once it holds the pushdown, which the compile releases.
            AsyncHashJoinRecordCursorFactory.setConstructorFailureForTesting(CairoException.nonCritical().put("injected construction failure"));
            try {
                assertExceptionNoLeakCheck(sql, 0, "injected construction failure");
            } finally {
                AsyncHashJoinRecordCursorFactory.setConstructorFailureForTesting(null);
            }

            // A native memory limit just above what the process holds fails the compile at each of its
            // allocations in turn, the pushdown's included; every failure releases what the compile
            // took, as assertMemoryLeak checks, and the first limit that admits the compile plans the
            // pushdown.
            // The second query takes the pushdown in a later stage of a stack.
            for (String regexes : new String[]{
                    "SELECT t.k, t.v, d.w FROM t LEFT JOIN d ON t.k = d.k AND d.vch ~ 'v[1-3]' AND (t.str ~ 'str[12]' OR d.w > t.v)",
                    "SELECT t.k, t.v, d.w, d2.w w2 FROM t LEFT JOIN d ON t.k = d.k LEFT JOIN d d2 ON t.sym = d2.s AND d2.vch ~ 'v[1-3]' AND (t.str ~ 'str[12]' OR d2.w > t.v)",
            }) {
                final String expectedRegexes = ordinary(regexes, sqlExecutionContext);
                int oomFailures = 0;
                for (long extra = 0; ; extra += 64) {
                    RecordCursorFactory factory = null;
                    try {
                        Unsafe.setRssMemLimit(Unsafe.getRssMemUsed() + extra);
                        factory = select(regexes);
                    } catch (CairoException e) {
                        Assert.assertTrue(e.getMessage(), e.isOutOfMemory());
                        oomFailures++;
                        continue;
                    } finally {
                        Unsafe.setRssMemLimit(0);
                    }
                    try {
                        TestUtils.assertContains(plan(factory, sqlExecutionContext), "buildOnFilter: vch ~ v[1-3]");
                        Assert.assertEquals(expectedRegexes, result(factory, sqlExecutionContext));
                    } finally {
                        factory.close();
                    }
                    break;
                }
                Assert.assertTrue(oomFailures > 0);
            }
            // The tables still work.
            assertPushdown("t", "d", "t.k = d.k", "d.vch ~ 'v[1-3]' AND (t.str ~ 'str[12]' OR d.w > t.v)", "d.vch ~ 'v[1-3]'", true, 0);
        });
    }

    @Test
    public void testConcurrentAndRoundBuilds() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            sqlExecutionContext.changePageFrameSizes(3, 3);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_ROWS_PER_PARTITION, 4);
            // A cap of one tuple, which a task lifts to its frame's three rows.
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 1);
            // Two small LEFT stages build at once, in one round with a task per stage, and with the
            // property at zero every stage builds in rounds of its own, each with its pushdown, one
            // of them not thread safe.
            final String sql = "SELECT t.k, t.v, d.w, u.w uw FROM t LEFT JOIN d ON t.k = d.k AND d.w % 3 = 0 AND d.w > t.v"
                    + " LEFT JOIN u ON t.k = u.k AND u.vch ~ 'v[1-4]'";
            // Both stages' builds run their whole residuals, which keep rows that repeat keys, so both
            // stages hand their chains on, one tuple per probe row, however the builds run.
            final String handedOn = "SELECT t.k, t.v, d.w, d2.w w2 FROM t LEFT JOIN d ON t.k = d.k AND d.w % 3 = 0"
                    + " LEFT JOIN d d2 ON t.sym = d2.s AND d2.vch ~ 'v[1-4]'";
            for (long minRows : new long[]{Long.MAX_VALUE, 0}) {
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, minRows);
                final String plan = assertParallelHashJoin(sql, sqlExecutionContext);
                TestUtils.assertContains(plan, "buildOnFilter: w%3=0");
                TestUtils.assertContains(plan, "buildOnFilter: vch ~ v[1-4]");
                try (RecordCursorFactory factory = select(sql)) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    final long concurrentRounds = join.getBuildRoundAtom().getConcurrentBuildRoundCount();
                    assertBuildRows(factory, 0, true, count("SELECT count() FROM d WHERE w % 3 = 0"));
                    assertBuildRows(factory, 1, true, count("SELECT count() FROM u WHERE vch ~ 'v[1-4]'"));
                    Assert.assertEquals(minRows == Long.MAX_VALUE ? 2 : 0, join.getBuildRoundAtom().getConcurrentBuildRoundCount() - concurrentRounds);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertEquals(minRows == 0, join.getAtom().getStage(0).isBuiltInRounds());
                        Assert.assertEquals(minRows == 0, join.getAtom().getStage(1).isBuiltInRounds());
                    }
                }
                assertParallelHashJoin(handedOn, sqlExecutionContext);
                try (RecordCursorFactory factory = select(handedOn)) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    final long concurrentRounds = join.getBuildRoundAtom().getConcurrentBuildRoundCount();
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.assertTrue(cursor.hasNext());
                        for (int k = 0; k < 2; k++) {
                            Assert.assertTrue(join.getAtom().getStage(k).isBuildOnFilterApplied());
                            Assert.assertEquals(minRows == 0, join.getAtom().getStage(k).isBuiltInRounds());
                        }
                    }
                    Assert.assertEquals(minRows == Long.MAX_VALUE ? 1 : 0, join.getBuildRoundAtom().getConcurrentBuildRoundCount() - concurrentRounds);
                    Assert.assertEquals(ordinary(handedOn, sqlExecutionContext), result(factory, sqlExecutionContext));
                    Assert.assertEquals(0, join.getAtom().getContinuationCount());
                }
            }
        });
    }

    @Test
    public void testConstantLeadingConjunct() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // A TRUE constant leads the residual, and drops no build row, so the build runs no filter.
            final String plan = assertParallelHashJoin("SELECT t.k, t.v, d.w FROM t LEFT JOIN d ON t.k = d.k AND true AND d.w > t.v", sqlExecutionContext);
            Assert.assertFalse(plan, plan.contains("buildOnFilter"));
            TestUtils.assertContains(plan, "onFilter: t.v<d.w");
            // After it, a conjunct of the build alone leads the rest.
            assertPushdown("t", "d", "t.k = d.k", "true AND d.w > 4 AND d.w > t.v", "d.w > 4", false, 0);
        });
    }

    @Test
    public void testNonAbandonableFailuresFailTheQuery() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // A cancellation, a memory limit breach and a critical error that the pushed conjuncts meet
            // fail the query, as they fail any build, rather than leave the conjuncts to the pairs.
            final String sql = "SELECT t.k, t.v, d.w FROM t LEFT JOIN d ON t.k = d.k AND test_latched_counter() AND d.w > t.v";
            final String expected = ordinary(sql, sqlExecutionContext);
            final int[] failureKind = {-1};
            final TestLatchedCounterFunctionFactory.Callback callback = new TestLatchedCounterFunctionFactory.Callback() {
                @Override
                public boolean onGet(Record rec, int count) {
                    if (rec instanceof PageFrameMemoryRecord && count == 7) {
                        switch (failureKind[0]) {
                            case 0 -> throw CairoException.queryCancelled();
                            case 1 -> throw CairoException.nonCritical().setOutOfMemory(true).put("test memory limit");
                            case 2 -> throw CairoException.critical(5).put("test critical failure");
                            default -> {
                            }
                        }
                    }
                    return true;
                }
            };
            TestLatchedCounterFunctionFactory.reset(callback);
            final String[] messages = {"cancelled by user", "test memory limit", "test critical failure"};
            sqlExecutionContext.changePageFrameSizes(3, 3);
            for (long minRows : new long[]{Long.MAX_VALUE, 0}) {
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, minRows);
                try (RecordCursorFactory factory = select(sql)) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    TestUtils.assertContains(plan(factory, sqlExecutionContext), "buildOnFilter: test_latched_counter()");
                    for (int kind = 0; kind < 3; kind++) {
                        failureKind[0] = kind;
                        // Restarts the count.
                        TestLatchedCounterFunctionFactory.reset(callback);
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            //noinspection StatementWithEmptyBody
                            while (cursor.hasNext()) {
                            }
                            Assert.fail("expected the build to fail: " + messages[kind]);
                        } catch (CairoException e) {
                            TestUtils.assertContains(e.getFlyweightMessage(), messages[kind]);
                            Assert.assertEquals(kind == 0, e.isInterruption());
                            Assert.assertEquals(kind == 1, e.isOutOfMemory());
                            Assert.assertEquals(kind == 2, e.isCritical());
                        }
                        Assert.assertEquals(0, join.getBuildRoundAtom().getPerWorkerLocks().getAcquiredSlotCount());
                        failureKind[0] = -1;
                        // The factory and the tables still work.
                        Assert.assertEquals(expected, result(factory, sqlExecutionContext));
                    }
                }
            }
        });
    }

    @Test
    public void testParquetBuildPartitions() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            sqlExecutionContext.changePageFrameSizes(3, 3);
            // A cap of one tuple, which a task lifts to its frame's three rows.
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 1);
            // d's first partitions in Parquet, the rest native, and then every partition but the last,
            // which is active, in Parquet: the pushed conjuncts read the build's rows from both.
            for (String converted : new String[]{"dts < '2023-12-31T03'", "dts < '2023-12-31T06'"}) {
                execute("ALTER TABLE d CONVERT PARTITION TO PARQUET WHERE " + converted);
                for (long minRows : new long[]{Long.MAX_VALUE, 0}) {
                    setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, minRows);
                    assertPushdown("t", "d", "t.k = d.k", "d.w % 3 = 0 AND d.w > t.v", "d.w % 3 = 0", false, 0);
                    // A residual over d's VARCHAR and STRING, which the pairs would read from its
                    // Parquet frames through a share of the decode budget when an execution runs it
                    // over them, keeps the ordinary plan; one that computes a VARCHAR from a
                    // fixed-size column pushes the same way.
                    assertOrdinaryPlan("SELECT t.k, t.v, d.* FROM t t LEFT JOIN d d ON t.k = d.k AND d.vch ~ 'v[12]' AND d.str IS NOT NULL", sqlExecutionContext);
                    assertPushdown("t", "d", "t.k = d.k", "d.w::VARCHAR ~ '[12]' AND d.w > 1", "d.w::VARCHAR ~ '[12]' AND d.w > 1", true, 0);
                    assertPushdown("t", "(SELECT * FROM d WHERE w > 2)", "t.sym = d.s", "d.s <> 's3' AND d.w < t.v", "d.s <> 's3'", false, 0);
                    // The build runs a residual of d alone whole over the rows of both kinds of
                    // partitions, and the stage hands the chains of the rows it kept on.
                    for (String residual : new String[]{"d.w % 3 = 0", "d.w::VARCHAR ~ '[12]' AND d.w > 1"}) {
                        assertChainsHandedOn(
                                "SELECT t.k, t.v, d.* FROM t LEFT JOIN d ON t.k = d.k AND " + residual,
                                "SELECT count() FROM (SELECT k, count() c FROM d WHERE " + residual.replace("d.", "") + ") WHERE c > 1"
                        );
                    }
                }
            }
        });
    }

    @Test
    public void testPlans() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            // The pushed conjuncts print against the build scan's columns, as the fused GROUP BY prints
            // its ON condition on the build alone, and the ON residual prints whole, as the serial join
            // prints its filter.
            assertQuery("SELECT t.k, d.w FROM t LEFT JOIN d ON t.k = d.k AND d.w % 3 = 0")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Async Hash Join workers: 1
                                  stages: 1
                                    Probe
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t
                                    Build
                                      joinType: left outer
                                      condition: d.k=t.k
                                      buildPayload: copied when the probe is large enough
                                      onFilter: d.w%3=0
                                      buildOnFilter: w%3=0
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: d
                            """);
            // Conjuncts that the size rule governs print it, after the build scan's WHERE filter, and
            // a projection over the build names their columns.
            assertQuery("SELECT t.k, d2.w2 FROM t LEFT JOIN (SELECT k, vch, w w2 FROM d WHERE w > 3) d2 ON t.k = d2.k AND d2.vch ~ 'v[12]' AND d2.w2 > t.v")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Async Hash Join workers: 1
                                  stages: 1
                                    Probe
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t
                                    Build
                                      joinType: left outer
                                      condition: d2.k=t.k
                                      onFilter: (d2.vch ~ v[12] and t.v<d2.w2)
                                      buildFilter: 3<w
                                      buildOnFilter: vch ~ v[12]
                                      buildOnFilterPushdown: when the build table holds no more rows than the probe table
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: d
                            """);
            // A projection over the build scan's filter renames a column, which the pushed conjunct
            // reads by the scan's name.
            final String renamed = "SELECT t.k, d2.w2 FROM t LEFT JOIN (SELECT k, w w2 FROM d WHERE w > 3) d2 ON t.k = d2.k AND d2.w2 % 2 = 0 AND d2.w2 < t.v";
            final String renamedPlan = assertParallelHashJoin(renamed, sqlExecutionContext);
            TestUtils.assertContains(renamedPlan, "onFilter: (d2.w2%2=0 and d2.w2<t.v)");
            TestUtils.assertContains(renamedPlan, "buildFilter: 3<w");
            TestUtils.assertContains(renamedPlan, "buildOnFilter: w%2=0");
            try (RecordCursorFactory factory = select(renamed)) {
                assertBuildRows(factory, 0, true, count("SELECT count() FROM d WHERE w > 3 AND w % 2 = 0"));
            }
            // A conjunct of the build alone after one that reads the probe row prints nothing more.
            assertQuery("SELECT t.k, d.w FROM t LEFT JOIN d ON t.k = d.k AND d.w > t.v AND d.w % 3 = 0")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Async Hash Join workers: 1
                                  stages: 1
                                    Probe
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t
                                    Build
                                      joinType: left outer
                                      condition: d.k=t.k
                                      buildPayload: copied when the probe is large enough
                                      onFilter: (t.v<d.w and d.w%3=0)
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: d
                            """);
            // The ordinary plan prints the residual as the join's filter.
            sqlExecutionContext.setParallelHashJoinEnabled(false);
            try (RecordCursorFactory factory = select("SELECT t.k, d.w FROM t LEFT JOIN d ON t.k = d.k AND d.w % 3 = 0")) {
                TestUtils.assertContains(plan(factory, sqlExecutionContext), "filter: d.w%3=0");
            } finally {
                sqlExecutionContext.setParallelHashJoinEnabled(true);
            }
        });
    }

    @Test
    public void testPushdownDecidesTheWalkPerExecution() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // A cap of one tuple, which a task lifts to its frame's three rows.
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 1);
            sqlExecutionContext.changePageFrameSizes(3, 3);

            // The size rule: b holds no more rows than p, then more, then, after p grew, fewer again.
            // One factory hands b's chains on while its build runs the regex, which keeps rows that
            // repeat each key, and walks b's matches under the whole residual otherwise.
            execute("CREATE TABLE p AS (SELECT (x % 3)::INT k, x v, timestamp_sequence('2024-01-01', 600_000_000) ts FROM long_sequence(30)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("CREATE TABLE b AS (SELECT (x % 3)::INT k, x w, ('v' || x)::VARCHAR vch FROM long_sequence(12))");
            final String sized = "SELECT p.k, p.v, b.w FROM p LEFT JOIN b ON p.k = b.k AND b.vch ~ '[1-6]$'";
            final String sizedKeys = "SELECT count() FROM (SELECT k, count() c FROM b WHERE vch ~ '[1-6]$') WHERE c > 1";
            try (RecordCursorFactory factory = select(sized)) {
                TestUtils.assertContains(plan(factory, sqlExecutionContext), PUSHDOWN_RULE);
                final long[] buildInserts = {0, 30, 0};
                final long[] probeInserts = {0, 0, 40};
                long bRows = 12;
                long pRows = 30;
                for (int i = 0; i < buildInserts.length; i++) {
                    if (buildInserts[i] > 0) {
                        execute("INSERT INTO b SELECT (x % 3)::INT, x + " + bRows + ", ('v' || (x + " + bRows + "))::VARCHAR FROM long_sequence(" + buildInserts[i] + ")");
                        bRows += buildInserts[i];
                    }
                    if (probeInserts[i] > 0) {
                        execute("INSERT INTO p SELECT (x % 3)::INT, x + " + pRows + ", timestamp_sequence('2024-01-02', 600_000_000) FROM long_sequence(" + probeInserts[i] + ")");
                        pRows += probeInserts[i];
                    }
                    final boolean isApplied = bRows <= pRows;
                    assertExecution(factory, sized, isApplied, isApplied, sizedKeys);
                }
            }

            // test_latched_counter() reads no column, so it leads the residual's conjuncts of the build
            // alone. An execution whose build meets a failure of it gives the conjuncts up and walks
            // d's matches under the whole residual; the next one runs them over d's rows and hands d's
            // chains on, and so on, whichever stepper the tasks run: the shared template's instance,
            // here also in the stage's own place, as on a JVM without hidden classes, or the stage's
            // hidden copy, from the first task.
            final String latched = "SELECT t.k, t.v, d.w FROM t LEFT JOIN d ON t.k = d.k AND test_latched_counter() AND d.w % 3 = 0";
            final String latchedKeys = "SELECT count() FROM (SELECT k, count() c FROM d WHERE w % 3 = 0) WHERE c > 1";
            final AtomicBoolean isFailing = new AtomicBoolean();
            TestLatchedCounterFunctionFactory.reset(new TestLatchedCounterFunctionFactory.Callback() {
                @Override
                public boolean onGet(Record rec, int count) {
                    if (isFailing.get() && isOnStack("applyAbandonableBuildOnFilter")) {
                        throw CairoException.nonCritical().put("test value failure");
                    }
                    return true;
                }
            });
            for (boolean isCopy : new boolean[]{false, true}) {
                if (isCopy) {
                    AsyncHashJoinRecordCursorFactory.setStepperWarmUpRowsForTesting(0);
                } else {
                    AsyncHashJoinRecordCursorFactory.setStepperCopiesEnabledForTesting(false);
                }
                try (RecordCursorFactory factory = select(latched)) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    Assert.assertEquals(isCopy, join.getStepperClass(0).isHidden());
                    for (boolean isWalking : new boolean[]{true, false, true, false}) {
                        isFailing.set(isWalking);
                        assertExecution(factory, latched, !isWalking, !isWalking, latchedKeys);
                        // The count's tasks ran the stepper under test.
                        Assert.assertTrue(latched, join.getAtom().getStepperRunCount(AsyncHashJoinAtom.STEPPER_RUN_COUNT_TASK, isCopy) > 0);
                        Assert.assertEquals(latched, 0, join.getAtom().getStepperRunCount(AsyncHashJoinAtom.STEPPER_RUN_COUNT_TASK, !isCopy));
                    }
                } finally {
                    isFailing.set(false);
                    AsyncHashJoinRecordCursorFactory.setStepperCopiesEnabledForTesting(true);
                    AsyncHashJoinRecordCursorFactory.setStepperWarmUpRowsForTesting(AsyncHashJoinAtom.STEPPER_WARM_UP_ROWS);
                }
            }

            // A build table truncated and filled again: the empty build leaves every probe row one
            // row of NULLs and no chain to index, and the stage hands its chains on either way.
            execute("CREATE TABLE e AS (SELECT * FROM d)");
            final String truncated = "SELECT t.k, t.v, e.w FROM t LEFT JOIN e ON t.k = e.k AND e.w % 3 = 0";
            final String truncatedKeys = "SELECT count() FROM (SELECT k, count() c FROM e WHERE w % 3 = 0) WHERE c > 1";
            try (RecordCursorFactory factory = select(truncated)) {
                assertExecution(factory, truncated, true, true, truncatedKeys);
                execute("TRUNCATE TABLE e");
                assertExecution(factory, truncated, true, true, truncatedKeys);
                Assert.assertEquals(count("SELECT count() FROM t"), ordinaryRowCount(truncated));
                execute("INSERT INTO e SELECT * FROM d");
                assertExecution(factory, truncated, true, true, truncatedKeys);
            }

            // A bind variable that each execution of one factory binds anew.
            final String bound = "SELECT t.k, t.v, d.w FROM t LEFT JOIN d ON t.k = d.k AND d.w > $1";
            bindVariableService.clear();
            bindVariableService.setLong(0, 5);
            try (RecordCursorFactory factory = select(bound)) {
                for (long lo : new long[]{5, 20}) {
                    bindVariableService.clear();
                    bindVariableService.setLong(0, lo);
                    assertExecution(factory, bound, true, true, "SELECT count() FROM (SELECT k, count() c FROM d WHERE w > " + lo + ") WHERE c > 1");
                }
            }
        });
    }

    @Test
    public void testResiduals() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            for (int frameRows : new int[]{1, 3, 1_000_000}) {
                sqlExecutionContext.changePageFrameSizes(frameRows, frameRows);
                for (Object[] residual : RESIDUALS) {
                    final String on = (String) residual[0];
                    final String pushed = (String) residual[1];
                    final boolean isSized = (Boolean) residual[2];
                    // INT, SYMBOL and staged keys, a build of unique keys, and a backward probe scan.
                    assertPushdown("t", "d", "t.k = d.k", on, pushed, isSized, 0);
                    assertPushdown("t", "d", "t.sym = d.s", on, pushed, isSized, 0);
                    assertPushdown("t", "d", "t.k = d.k AND t.sym = d.s", on, pushed, isSized, 0);
                    assertPushdown("t", "u", "t.k = d.k", on, pushed, isSized, 0);
                    assertPushdown("t", "d", "t.k = d.k", on + " ORDER BY t.ts DESC", pushed, isSized, 0);
                    // The build scan's own filter, compiled or not, runs before the pushed conjuncts.
                    for (int jitMode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                        sqlExecutionContext.setJitMode(jitMode);
                        assertPushdown("t", "(SELECT * FROM d WHERE w > 2)", "t.k = d.k", on, pushed, isSized, 0);
                    }
                }
            }
        });
    }

    @Test
    public void testScanWithoutTableReader() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            sqlExecutionContext.setParallelReadParquetEnabled(true);
            try (
                    Path path = new Path();
                    PartitionDescriptor descriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("d")
            ) {
                path.of(root).concat("d.parquet");
                engine.getConfiguration().getFilesFacade().remove(path.$());
                PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
                PartitionEncoder.encode(descriptor, path);
            }
            // read_parquet() has no table reader, so the size rule fails, as the presize rule does, and
            // the pairs run the conjuncts that it governs; the others filter the build.
            final String build = "read_parquet('d.parquet')";
            assertPushdown("t", build, "t.k = d.k", "d.w % 3 = 0 AND d.w > t.v", "d.w % 3 = 0", false, 0);
            assertPushdown("t", build, "t.k = d.k", "d.w::STRING ~ '[12]' AND d.w > t.v", "d.w::STRING ~ '[12]'", true, 0);
            // A residual over d.vch, a VARCHAR that the pairs would read from read_parquet()'s frames
            // through a share of the decode budget, keeps the ordinary plan.
            assertOrdinaryPlan("SELECT t.k, t.v, d.* FROM t LEFT JOIN " + build + " d ON t.k = d.k AND d.vch ~ 'v[12]' AND d.w > t.v", sqlExecutionContext);
            assertOrdinaryPlan("SELECT t.k, d.w FROM t LEFT JOIN read_parquet('d.parquet') d ON t.k = d.k AND d.vch ~ 'v[12]'", sqlExecutionContext);
            try (RecordCursorFactory factory = select("SELECT t.k, d.w FROM t LEFT JOIN read_parquet('d.parquet') d ON t.k = d.k AND d.w::STRING ~ '[12]'")) {
                TestUtils.assertContains(plan(factory, sqlExecutionContext), PUSHDOWN_RULE);
                final HashJoinBuildStage stage = find(factory).getAtom().getStage(0);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertFalse(stage.isKeyCapacityPresized());
                    Assert.assertFalse(stage.isBuildOnFilterApplied());
                    Assert.assertEquals(count("SELECT count() FROM read_parquet('d.parquet')"), stage.getFrozenBuild().getRowCount());
                }
            }
        });
    }

    @Test
    public void testSizeRuleFollowsTheTablesOfEachExecution() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE p AS (SELECT (x % 5)::INT k, x v FROM long_sequence(20))");
            execute("CREATE TABLE b AS (SELECT (x % 5)::INT k, x w, ('v' || x)::VARCHAR vch FROM long_sequence(10))");
            final String sized = "SELECT p.k, p.v, b.w, b.vch FROM p LEFT JOIN b ON p.k = b.k AND b.vch ~ '[13579]$' AND b.w < p.v";
            final String cheap = "SELECT p.k, p.v, b.w, b.vch FROM p LEFT JOIN b ON p.k = b.k AND b.w % 2 = 1 AND b.w < p.v";
            try (
                    RecordCursorFactory sizedFactory = select(sized);
                    RecordCursorFactory cheapFactory = select(cheap)
            ) {
                TestUtils.assertContains(plan(sizedFactory, sqlExecutionContext), PUSHDOWN_RULE);
                Assert.assertFalse(plan(cheapFactory, sqlExecutionContext).contains(PUSHDOWN_RULE));
                // b holds no more rows than p, then more, then, after p grew, fewer again; one factory of
                // each query decides in every execution, from the tables' sizes at the time.
                final long[] inserts = {0, 15, 0};
                final long[] probeInserts = {0, 0, 30};
                long bRows = 10;
                long pRows = 20;
                for (int i = 0; i < inserts.length; i++) {
                    if (inserts[i] > 0) {
                        execute("INSERT INTO b SELECT (x % 5)::INT, x + " + bRows + ", ('v' || (x + " + bRows + "))::VARCHAR FROM long_sequence(" + inserts[i] + ")");
                        bRows += inserts[i];
                    }
                    if (probeInserts[i] > 0) {
                        execute("INSERT INTO p SELECT (x % 5)::INT, x + " + pRows + " FROM long_sequence(" + probeInserts[i] + ")");
                        pRows += probeInserts[i];
                    }
                    final boolean isPresized = bRows <= pRows;
                    Assert.assertEquals(ordinary(sized, sqlExecutionContext), result(sizedFactory, sqlExecutionContext));
                    Assert.assertEquals(ordinary(cheap, sqlExecutionContext), result(cheapFactory, sqlExecutionContext));
                    assertBuildRows(sizedFactory, 0, isPresized, isPresized ? count("SELECT count() FROM b WHERE vch ~ '[13579]$'") : bRows);
                    assertBuildRows(cheapFactory, 0, true, count("SELECT count() FROM b WHERE w % 2 = 1"));
                    try (RecordCursor cursor = sizedFactory.getCursor(sqlExecutionContext)) {
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertEquals(isPresized, find(sizedFactory).getAtom().getStage(0).isKeyCapacityPresized());
                    }
                }
            }
        });
    }

    @Test
    public void testStackPushdownDecidesTheWalkPerExecution() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // A cap of one tuple, which a task lifts to its frame's three rows.
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 1);
            sqlExecutionContext.changePageFrameSizes(3, 3);
            // The first stage has no residual, and the second stage's residual consists of conjuncts of
            // d2 alone; test_latched_counter() reads no column. An execution whose build of d2 meets a
            // failure of it gives the conjuncts up, so the second stage's pairs run the whole residual:
            // the workers walk the second stage's matches, and the first stage's, whose columns the
            // residual may read. The next execution runs the conjuncts over d2's rows, which leaves no
            // stage a filter, so both stages hand their chains on; the one after it gives them up again.
            final String sql = "SELECT t.k, t.v, d.w, d2.w w2 FROM t LEFT JOIN d ON t.k = d.k"
                    + " LEFT JOIN d d2 ON t.sym = d2.s AND test_latched_counter() AND d2.w % 3 = 0";
            // Whether each stage's build repeats a key in an execution that ran the conjuncts over d2's
            // rows, as plain queries tell.
            final boolean[] isRepeated = {
                    count("SELECT count() FROM (SELECT k, count() c FROM d) WHERE c > 1") > 0,
                    count("SELECT count() FROM (SELECT s, count() c FROM d WHERE w % 3 = 0) WHERE c > 1") > 0,
            };
            final AtomicBoolean isFailing = new AtomicBoolean();
            TestLatchedCounterFunctionFactory.reset(new TestLatchedCounterFunctionFactory.Callback() {
                @Override
                public boolean onGet(Record rec, int count) {
                    if (isFailing.get() && isOnStack("applyAbandonableBuildOnFilter")) {
                        throw CairoException.nonCritical().put("test value failure");
                    }
                    return true;
                }
            });
            final String expected = ordinary(sql, sqlExecutionContext);
            final long expectedRows = ordinaryRowCount(sql);
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                final AsyncHashJoinAtom atom = join.getAtom();
                final boolean[] isWalkingByExecution = {true, false, true};
                for (int execution = 0; execution < isWalkingByExecution.length; execution++) {
                    final boolean isWalking = isWalkingByExecution[execution];
                    final String message = sql + ", execution " + execution;
                    isFailing.set(isWalking);
                    // The first execution after each flip reads the rows, so that a decision left over
                    // from the execution before shows: stages that walk the matches, which d and d2
                    // repeat for each key, leave the rest of some frame to a continuation, while stages
                    // that hand their chains on write one tuple per probe row.
                    Assert.assertEquals(message, expected, result(factory, sqlExecutionContext));
                    Assert.assertEquals(message, isWalking, atom.getContinuationCount() > 0);
                    // A count from the top indexes the chain lengths of the builds of the stages that
                    // hand their chains on, and of no stage that walks its matches.
                    final long[] passes = {atom.getStage(0).getChainLengthIndexCount(), atom.getStage(1).getChainLengthIndexCount()};
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        final RecordCursor.Counter counter = new RecordCursor.Counter();
                        cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
                        Assert.assertEquals(message, expectedRows, counter.get());
                        Assert.assertEquals(message, !isWalking, atom.getStage(1).isBuildOnFilterApplied());
                    }
                    for (int k = 0; k < passes.length; k++) {
                        final boolean isIndexed = !isWalking && isRepeated[k];
                        Assert.assertEquals(message + ", stage " + k, passes[k] + (isIndexed ? 1 : 0), atom.getStage(k).getChainLengthIndexCount());
                    }
                }
            }
        });
    }

    @Test
    public void testStacks() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            sqlExecutionContext.changePageFrameSizes(3, 3);
            // Each LEFT stage of a stack takes its own pushdown, the later stages' compiled as the stack
            // grows; a stage ahead of a pushdown keeps walking its matches.
            final String sql = "SELECT t.k, t.v, d.w, u.w uw FROM t LEFT JOIN d ON t.k = d.k AND d.vch ~ 'v[12]'"
                    + " JOIN u ON t.sym = u.s LEFT JOIN d d3 ON t.k = d3.k AND d3.w % 4 = 0 AND d3.w < t.v";
            final String plan = assertParallelHashJoin(sql, sqlExecutionContext);
            TestUtils.assertContains(plan, "stages: 3");
            TestUtils.assertContains(plan, "buildOnFilter: vch ~ v[12]");
            TestUtils.assertContains(plan, "buildOnFilter: w%4=0");
            try (RecordCursorFactory factory = select(sql)) {
                assertBuildRows(factory, 0, true, count("SELECT count() FROM d WHERE vch ~ 'v[12]'"));
                assertBuildRows(factory, 1, false, 6);
                assertBuildRows(factory, 2, true, count("SELECT count() FROM d WHERE w % 4 = 0"));
            }
            // A stack over a first join that may swap its inputs: the later stage's size rule compares
            // its build with the left input in both orientations.
            execute("CREATE TABLE l AS (SELECT (x % 5)::INT k, x lv FROM long_sequence(30))");
            execute("CREATE TABLE r AS (SELECT (x % 5)::INT k, x rv FROM long_sequence(50))");
            final String swapped = "SELECT l.k, l.lv, r.rv, d.w FROM l JOIN r ON l.k = r.k LEFT JOIN d ON l.k = d.k AND d.vch ~ 'v[1-3]' AND d.w < l.lv";
            for (int run = 0; run < 2; run++) {
                final String swapPlan = assertParallelHashJoin(swapped, sqlExecutionContext);
                TestUtils.assertContains(swapPlan, "inputSwap: when the left input is smaller");
                TestUtils.assertContains(swapPlan, PUSHDOWN_RULE);
                try (RecordCursorFactory factory = select(swapped)) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.assertTrue(cursor.hasNext());
                        // l is the smaller input at first, and the larger one once it grew.
                        Assert.assertEquals(run == 0, join.getAtom().isSwapped());
                        // d's 40 rows stay within l's 30 rows only once l grew.
                        Assert.assertEquals(run == 1, join.getAtom().getStage(1).isBuildOnFilterApplied());
                    }
                }
                execute("INSERT INTO l SELECT (x % 5)::INT, x + 30 FROM long_sequence(40)");
            }
        });
    }

    @Test
    public void testThrowingConjuncts() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // dp's row of key 20 holds w = 10, whose w * 1000 does not fit DECIMAL(4,0); t holds no key
            // 20, and dq holds the same row under key 2, which t holds.
            for (String table : new String[]{"dp", "dq"}) {
                execute("CREATE TABLE " + table + " AS (SELECT (x % 5)::INT k, x % 9 w FROM long_sequence(15))");
                execute("INSERT INTO " + table + " VALUES (" + (table.equals("dp") ? 20 : 2) + ", 10)");
                execute("INSERT INTO " + table + " SELECT (x % 5)::INT, x % 9 FROM long_sequence(15)");
            }
            final String poison = "(d.w * 1000)::DECIMAL(4,0) > d.k::DECIMAL(4,0)";
            sqlExecutionContext.changePageFrameSizes(3, 3);
            for (long minRows : new long[]{Long.MAX_VALUE, 0}) {
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, minRows);
                for (String build : new String[]{"dp", "(SELECT * FROM dp WHERE w >= 0)"}) {
                    // The serial join never runs the conjunct over the row without a match, so neither
                    // plan fails: the build gives the conjunct up and the pairs run the residual.
                    for (String residual : new String[]{poison, poison + " AND d.w <= t.v"}) {
                        final String sql = "SELECT t.k, t.v, d.w FROM t LEFT JOIN " + build + " d ON t.k = d.k AND " + residual;
                        TestUtils.assertContains(assertParallelHashJoin(sql, sqlExecutionContext), "buildOnFilter: ");
                        try (RecordCursorFactory factory = select(sql)) {
                            assertBuildRows(factory, 0, false, -1);
                        }
                    }
                }
                // The row with a match fails both plans alike.
                for (String residual : new String[]{poison, poison + " AND d.w <= t.v"}) {
                    final String sql = "SELECT t.k, t.v, d.w FROM t LEFT JOIN dq d ON t.k = d.k AND " + residual;
                    final String expected = error(sql, false);
                    TestUtils.assertContains(expected, "inconvertible value: 10000 [LONG -> DECIMAL(4,0)]");
                    Assert.assertEquals(sql, expected, error(sql, true));
                }
                // The serial join stops at the leading conjunct that drops the row, before the one that
                // would fail on its pairs, and so does the build.
                final String dropped = "SELECT t.k, t.v, d.w FROM t LEFT JOIN dq d ON t.k = d.k AND d.w < 5 AND (d.w * 2000)::DECIMAL(4,0) > t.v::DECIMAL(4,0)";
                TestUtils.assertContains(assertParallelHashJoin(dropped, sqlExecutionContext), "buildOnFilter: w<5");
                // The other way round, the conjunct that fails leads, reads the probe row too and fails
                // both plans alike, so the conjunct of the build alone stays with the pairs.
                final String failing = "SELECT t.k, t.v, d.w FROM t LEFT JOIN dq d ON t.k = d.k AND (d.w * 2000)::DECIMAL(4,0) > t.v::DECIMAL(4,0) AND d.w < 5";
                final String expected = error(failing, false);
                TestUtils.assertContains(expected, "inconvertible value: ");
                Assert.assertEquals(failing, expected, error(failing, true));
                try (RecordCursorFactory factory = select(failing)) {
                    Assert.assertFalse(plan(factory, sqlExecutionContext).contains("buildOnFilter"));
                }
            }
        });
    }

    @Test
    public void testToTopLimitsAndCounts() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            sqlExecutionContext.changePageFrameSizes(2, 3);
            final String sql = "SELECT t.k, t.v, d.w, d.vch FROM t LEFT JOIN d ON t.k = d.k AND d.vch ~ 'v[1-3]' AND (d.w > t.v OR t.str IS NULL)";
            final String expected = ordinary(sql, sqlExecutionContext);
            final long expectedRows = ordinaryCount("SELECT count() FROM (" + sql + ")");
            try (RecordCursorFactory factory = select(sql)) {
                TestUtils.assertContains(plan(factory, sqlExecutionContext), "buildOnFilter: vch ~ v[1-3]");
                final HashJoinBuildStage stage = find(factory).getAtom().getStage(0);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    // toTop() before the first read, mid-stream and at the end goes back to the build as
                    // it was, whose rows the pushed conjunct filtered, and the rest of the residual
                    // starts over.
                    for (int stop : new int[]{0, 1, 5, Integer.MAX_VALUE}) {
                        long rows = 0;
                        while (rows < stop && cursor.hasNext()) {
                            rows++;
                        }
                        if (stop == Integer.MAX_VALUE) {
                            Assert.assertEquals(expectedRows, rows);
                        }
                        cursor.toTop();
                    }
                    Assert.assertTrue(stage.isBuildOnFilterApplied());
                    // A count of the rows from the top, without reading them.
                    final RecordCursor.Counter counter = new RecordCursor.Counter();
                    cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
                    Assert.assertEquals(expectedRows, counter.get());
                }
                Assert.assertEquals(expected, result(factory, sqlExecutionContext));
            }
            // A negative LIMIT counts the rows first and then reads the last ones; a LIMIT stops early.
            assertParallelHashJoin(sql + " LIMIT -5", sqlExecutionContext);
            assertParallelHashJoin(sql + " LIMIT 3", sqlExecutionContext);
        });
    }

    @Test
    public void testVarSizeColumns() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("""
                    CREATE TABLE dv AS (
                        SELECT
                            (x % 5)::INT k,
                            x w,
                            CASE x % 5 WHEN 0 THEN NULL WHEN 1 THEN '' WHEN 2 THEN 'é😀' ELSE 'str' || x END str,
                            CASE x % 5 WHEN 0 THEN NULL WHEN 1 THEN '' WHEN 2 THEN 'é😀' ELSE 'a varchar that does not fit inline ' || x END::VARCHAR vch,
                            CASE x % 4 WHEN 0 THEN NULL ELSE rnd_bin(1, 8, 0) END bin,
                            CASE x % 4 WHEN 0 THEN NULL WHEN 1 THEN ARRAY[]::DOUBLE[] ELSE ARRAY[x::DOUBLE, -x, 1.5] END arr,
                            ARRAY[(x % 6)::DOUBLE, -x, 1.5] arr2
                        FROM long_sequence(30)
                    )
                    """);
            sqlExecutionContext.changePageFrameSizes(3, 3);
            // A cap of one tuple, which a task lifts to its frame's three rows.
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 1);
            // Comparisons of variable-size columns are cheap; functions over them are not. arr2 holds no
            // NULL: two NULL arrays compare by what their views read before, in either plan.
            final Object[][] conjuncts = {
                    {"d.str = 'str3'", false},
                    {"d.str = ''", false},
                    {"d.str IS NULL", false},
                    {"d.str ~ 'é|str[12]'", true},
                    {"d.vch <> 'é😀'", false},
                    {"d.vch IS NOT NULL", false},
                    {"d.vch LIKE 'a varchar%1'", true},
                    {"d.bin IS NULL", false},
                    {"length(d.bin) > 4", true},
                    {"d.arr <> d.arr2", false},
                    {"d.arr[1] > 3", true},
                    {"array_sum(d.arr) > 1", true},
            };
            for (Object[] conjunct : conjuncts) {
                final String pushed = (String) conjunct[0];
                final boolean isSized = (Boolean) conjunct[1];
                assertPushdown("t", "dv", "t.k = d.k", pushed + " AND d.w > t.v % 20", pushed, isSized, 0);
                assertPushdown("t", "dv", "t.k = d.k", pushed, pushed, isSized, 0);
                // dv holds no more rows than t, so the build runs the whole residual, and the stage
                // hands the chains of the rows it kept to the consumer, which reads their payload.
                assertChainsHandedOn(
                        "SELECT t.k, t.v, d.* FROM t LEFT JOIN dv d ON t.k = d.k AND " + pushed,
                        "SELECT count() FROM (SELECT k, count() c FROM dv WHERE " + pushed.replace("d.", "") + ") WHERE c > 1"
                );
            }
        });
    }

    @Test
    public void testWholePushedResidualHandsChainsOn() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // A cap of one tuple, which a task lifts to its frame's rows: a stage that walked a build of
            // several rows per key would leave the rest of a frame to a continuation.
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 1);
            // The keys, the build's columns that they read, and the build: INT, SYMBOL and staged keys,
            // and a build of unique keys.
            final String[][] keys = {
                    {"t.k = d.k", "k", "d"},
                    {"t.sym = d.s", "s", "d"},
                    {"t.k = d.k AND t.sym = d.s", "k, s", "d"},
                    {"t.k = d.k", "k", "u"},
            };
            for (int frameRows : new int[]{1, 3, 1_000_000}) {
                sqlExecutionContext.changePageFrameSizes(frameRows, frameRows);
                for (Object[] residual : RESIDUALS) {
                    final String on = (String) residual[0];
                    // The residuals that the build runs whole in every execution: those whose leading
                    // conjuncts of the build alone make up the residual, none of which the size rule
                    // governs.
                    if (!on.equals(residual[1]) || (Boolean) residual[2]) {
                        continue;
                    }
                    final String kept = on.replace("d.", "");
                    for (String[] key : keys) {
                        final String sql = "SELECT t.k, t.v, d.* FROM t LEFT JOIN " + key[2] + " d ON " + key[0] + " AND " + on;
                        final String repeatedKeys = "SELECT count() FROM (SELECT " + key[1] + ", count() c FROM " + key[2] + " WHERE " + kept + ") WHERE c > 1";
                        assertChainsHandedOn(sql, repeatedKeys);
                    }
                    // A backward probe scan, and the build scan's own filter, compiled or not, which
                    // runs before the residual.
                    final String repeatedKeys = "SELECT count() FROM (SELECT k, count() c FROM d WHERE " + kept + ") WHERE c > 1";
                    assertChainsHandedOn("SELECT t.k, t.v, d.* FROM t LEFT JOIN d ON t.k = d.k AND " + on + " ORDER BY t.ts DESC", repeatedKeys);
                    for (int jitMode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                        sqlExecutionContext.setJitMode(jitMode);
                        assertChainsHandedOn(
                                "SELECT t.k, t.v, d.* FROM t LEFT JOIN (SELECT * FROM d WHERE w > 2) d ON t.k = d.k AND " + on,
                                "SELECT count() FROM (SELECT k, count() c FROM d WHERE w > 2 AND " + kept + ") WHERE c > 1"
                        );
                    }
                    sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
                }
            }

            // dp's row of key 20 holds w = 10, whose w * 1000 does not fit DECIMAL(4,0), and t holds no
            // key 20: the build gives the conjunct up, since the serial join never runs it over that
            // row, and the stage walks its matches under the whole residual, past the cap.
            execute("CREATE TABLE dp AS (SELECT (x % 5)::INT k, x % 9 w FROM long_sequence(15))");
            execute("INSERT INTO dp VALUES (20, 10)");
            execute("INSERT INTO dp SELECT (x % 5)::INT, x % 9 FROM long_sequence(15)");
            sqlExecutionContext.changePageFrameSizes(3, 3);
            final String abandoned = "SELECT t.k, t.v, d.w FROM t LEFT JOIN dp d ON t.k = d.k AND (d.w * 1000)::DECIMAL(4,0) > d.k::DECIMAL(4,0)";
            TestUtils.assertContains(assertParallelHashJoin(abandoned, sqlExecutionContext), "buildOnFilter: ");
            try (RecordCursorFactory factory = select(abandoned)) {
                assertExecution(factory, abandoned, false, false, null);
            }
        });
    }

    @Test
    public void testWholePushedResidualInSqlContexts() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // A cap of one tuple, which a task lifts to its frame's three rows.
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 1);
            sqlExecutionContext.changePageFrameSizes(3, 3);
            // The residual keeps two of d's rows in every three, which repeat each key.
            final String join = "SELECT t.k, t.v, t.ts, d.w FROM t LEFT JOIN d ON t.k = d.k AND d.w % 3 <> 0";
            final String repeatedKeys = "SELECT count() FROM (SELECT k, count() c FROM d WHERE w % 3 <> 0) WHERE c > 1";
            assertChainsHandedOn(join, repeatedKeys);
            // The plain SQL that the residual means: a LEFT join of d's rows that pass it, on the
            // ordinary plan.
            try (RecordCursorFactory factory = select(join)) {
                Assert.assertEquals(
                        ordinary("SELECT t.k, t.v, t.ts, d.w FROM t LEFT JOIN (SELECT * FROM d WHERE w % 3 <> 0) d ON t.k = d.k", sqlExecutionContext),
                        result(factory, sqlExecutionContext)
                );
            }
            // A sub-query, a CTE, ORDER BY the probe's timestamp descending, a window function over
            // the join, and a LIMIT, whose rows the first frame's three probe rows hold.
            for (String sql : new String[]{
                    "SELECT * FROM (" + join + ")",
                    "WITH j AS (" + join + ") SELECT * FROM j",
                    join + " ORDER BY t.ts DESC",
                    "SELECT k, v, w, row_number() OVER (ORDER BY ts) rn FROM (" + join + ")",
                    join + " LIMIT 10",
            }) {
                assertParallelHashJoin(sql, sqlExecutionContext);
                try (RecordCursorFactory factory = select(sql)) {
                    final AsyncHashJoinRecordCursorFactory asyncJoin = find(factory);
                    Assert.assertNotNull(sql, asyncJoin);
                    Assert.assertEquals(sql, ordinary(sql, sqlExecutionContext), result(factory, sqlExecutionContext));
                    Assert.assertEquals(sql, 0, asyncJoin.getAtom().getContinuationCount());
                }
            }
            // A negative LIMIT counts the rows first, which indexes the chain lengths of d's kept rows.
            final String lastRows = join + " LIMIT -5";
            assertParallelHashJoin(lastRows, sqlExecutionContext);
            try (RecordCursorFactory factory = select(lastRows)) {
                final AsyncHashJoinRecordCursorFactory asyncJoin = find(factory);
                Assert.assertNotNull(lastRows, asyncJoin);
                final HashJoinBuildStage stage = asyncJoin.getAtom().getStage(0);
                final long passes = stage.getChainLengthIndexCount();
                Assert.assertEquals(lastRows, ordinary(lastRows, sqlExecutionContext), result(factory, sqlExecutionContext));
                Assert.assertEquals(lastRows, passes + 1, stage.getChainLengthIndexCount());
            }
            // UNION ALL of two such joins.
            assertParallelHashJoin(join + " UNION ALL " + join.replace("d.w % 3 <> 0", "d.w % 3 = 0"), sqlExecutionContext);
        });
    }

    @Test
    public void testWholePushedResidualOverDataShapes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // A cap of one tuple, which a task lifts to its frame's three rows.
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 1);
            sqlExecutionContext.changePageFrameSizes(3, 3);
            final long probeRows = count("SELECT count() FROM t");
            // A residual that keeps none of d's rows leaves every probe row one row of NULLs, and one
            // that keeps a single row one match at most; neither build repeats a key.
            for (String residual : new String[]{"d.w > 1_000_000", "d.w = 7"}) {
                final String sql = "SELECT t.k, t.v, d.w FROM t LEFT JOIN d ON t.k = d.k AND " + residual;
                assertChainsHandedOn(sql, "SELECT count() FROM (SELECT k, count() c FROM d WHERE " + residual.replace("d.", "") + ") WHERE c > 1");
                Assert.assertEquals(sql, probeRows, ordinaryRowCount(sql));
            }
            // A column of NULLs alone: one residual over it keeps every row, and another none.
            execute("CREATE TABLE dn AS (SELECT (x % 5)::INT k, x w, NULL::LONG n FROM long_sequence(20))");
            for (String residual : new String[]{"d.n IS NULL", "d.n > 0"}) {
                assertChainsHandedOn(
                        "SELECT t.k, t.v, d.w FROM t LEFT JOIN dn d ON t.k = d.k AND " + residual,
                        "SELECT count() FROM (SELECT k, count() c FROM dn WHERE " + residual.replace("d.", "") + ") WHERE c > 1"
                );
            }
            // A column added after the table held rows, which the earlier rows read as NULLs: the
            // residual over it drops them, and keeps the later rows, which repeat each key.
            execute("CREATE TABLE dc AS (SELECT k, w FROM d)");
            execute("ALTER TABLE dc ADD COLUMN w2 LONG");
            execute("INSERT INTO dc SELECT k, w + 40, w FROM d");
            assertChainsHandedOn(
                    "SELECT t.k, t.v, d.w, d.w2 FROM t LEFT JOIN dc d ON t.k = d.k AND d.w2 > 0",
                    "SELECT count() FROM (SELECT k, count() c FROM dc WHERE w2 > 0) WHERE c > 1"
            );
        });
    }

    // Asserts that the stage's build of an execution of the factory ran its pushed conjuncts, or not,
    // and holds the given rows; -1 for rows that the test does not tell.
    private static void assertBuildRows(RecordCursorFactory factory, int stage, boolean isApplied, long rows) throws SqlException {
        final HashJoinBuildStage buildStage = find(factory).getAtom().getStage(stage);
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            cursor.hasNext();
            Assert.assertEquals(isApplied, buildStage.isBuildOnFilterApplied());
            if (rows >= 0) {
                Assert.assertEquals(rows, buildStage.getFrozenBuild().getRowCount());
            }
        }
    }

    // Asserts the query, whose first stage is a LEFT join of t under an ON residual that its build runs
    // whole, against the ordinary plan, in order, and that the stage hands its chains to the
    // consumer; see assertExecution().
    private static void assertChainsHandedOn(String sql, String repeatedKeys) throws SqlException {
        assertParallelHashJoin(sql, sqlExecutionContext);
        try (RecordCursorFactory factory = select(sql)) {
            assertExecution(factory, sql, true, true, repeatedKeys);
        }
    }

    // Executes the factory of the query, a join of t whose first stage is a LEFT join under an ON
    // residual that leads with conjuncts of the build alone, and asserts that the stage's build ran
    // them, or not; that the rows equal the ordinary plan's, in order; and that the stage handed its
    // chains to the consumer, or walked its matches on the workers. A stage that hands its chains on
    // writes one tuple per probe row, so no frame continues past a cap of one tuple, and a count of
    // the join's rows from the top indexes the chain lengths of its build when the build's rows repeat
    // a key, which the given plain query, of the keys of more than one row that the build kept,
    // tells. A stage that walks its matches, which the callers' builds repeat each key for, leaves
    // the rest of some frame to a continuation, and indexes no build.
    private static void assertExecution(
            RecordCursorFactory factory,
            String sql,
            boolean isApplied,
            boolean isHandedOn,
            @Nullable String repeatedKeys
    ) throws SqlException {
        final AsyncHashJoinRecordCursorFactory join = find(factory);
        Assert.assertNotNull(sql, join);
        final HashJoinBuildStage stage = join.getAtom().getStage(0);
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            cursor.hasNext();
            Assert.assertEquals(sql, isApplied, stage.isBuildOnFilterApplied());
        }
        Assert.assertEquals(sql, ordinary(sql, sqlExecutionContext), result(factory, sqlExecutionContext));
        Assert.assertEquals(sql, isHandedOn, join.getAtom().getContinuationCount() == 0);
        final long passes = stage.getChainLengthIndexCount();
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            final RecordCursor.Counter counter = new RecordCursor.Counter();
            cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
            Assert.assertEquals(sql, ordinaryRowCount(sql), counter.get());
        }
        final boolean isIndexed = isHandedOn && repeatedKeys != null && count(repeatedKeys) > 0;
        Assert.assertEquals(sql, passes + (isIndexed ? 1 : 0), stage.getChainLengthIndexCount());
    }

    // Asserts the parallel hash join of t LEFT JOIN build d ON keys AND residual against the ordinary
    // plan, in order, and that its build ran the expected leading conjuncts over its rows, null for
    // none, which a plain count of the build's rows that pass them confirms.
    private static void assertPushdown(String probe, String build, String keys, String residual, String pushed, boolean isSized, int stage) throws SqlException {
        final String sql = "SELECT t.k, t.v, d.* FROM " + probe + " t LEFT JOIN " + build + " d ON " + keys + " AND " + residual;
        final String plan = assertParallelHashJoin(sql, sqlExecutionContext);
        Assert.assertEquals(sql + '\n' + plan, pushed != null, plan.contains("buildOnFilter: "));
        Assert.assertEquals(sql + '\n' + plan, isSized, plan.contains(PUSHDOWN_RULE));
        TestUtils.assertContains(plan, "onFilter: ");
        // The probe table holds more rows than every build table here, so the size rule holds, but for
        // read_parquet(), which has no table.
        final boolean isApplied = pushed != null && (!isSized || !build.startsWith("read_parquet"));
        // The count names the build's columns alone: the optimiser cannot resolve an array access of a
        // column that names its table, as in d.arr[1], in the WHERE clause under the count.
        final String buildRows = "SELECT count() FROM " + build + " d" + (isApplied ? " WHERE " + pushed.replace("d.", "") : "");
        try (RecordCursorFactory factory = select(sql)) {
            assertBuildRows(factory, stage, isApplied, count(buildRows));
        }
    }

    // The count that the query's single LONG value holds.
    private static long count(String sql) throws SqlException {
        try (RecordCursorFactory factory = select(sql); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            Assert.assertTrue(cursor.hasNext());
            return cursor.getRecord().getLong(0);
        }
    }

    private static void createTables() throws Exception {
        execute("""
                CREATE TABLE t AS (
                    SELECT
                        CASE WHEN x % 9 = 0 THEN NULL ELSE (x % 7)::INT END k,
                        x v,
                        ('s' || (x % 4))::SYMBOL sym,
                        CASE WHEN x % 5 = 0 THEN NULL ELSE 'str' || (x % 6) END str,
                        timestamp_sequence('2024-01-01', 600_000_000) ts
                    FROM long_sequence(60)
                ) TIMESTAMP(ts) PARTITION BY HOUR
                """);
        // d repeats its keys and inserts its symbols in another order than t, so equal texts have
        // different keys; u holds one row per key, and per SYMBOL.
        execute("""
                CREATE TABLE d AS (
                    SELECT
                        CASE WHEN x % 6 = 0 THEN NULL ELSE (x % 5)::INT END k,
                        x w,
                        ('s' || (5 - x % 5))::SYMBOL s,
                        CASE WHEN x % 4 = 0 THEN NULL ELSE 'v' || x END::VARCHAR vch,
                        CASE WHEN x % 3 = 0 THEN NULL ELSE 'str' || (x % 7) END str,
                        timestamp_sequence('2023-12-31', 600_000_000) dts
                    FROM long_sequence(40)
                ) TIMESTAMP(dts) PARTITION BY HOUR
                """);
        execute("""
                CREATE TABLE u AS (
                    SELECT
                        (x % 6)::INT k,
                        x * 7 w,
                        ('s' || (6 - x))::SYMBOL s,
                        CASE WHEN x % 4 = 0 THEN NULL ELSE 'v' || x END::VARCHAR vch,
                        CASE WHEN x % 3 = 0 THEN NULL ELSE 'str' || x END str
                    FROM long_sequence(6)
                )
                """);
    }

    // The error that reading the query's rows ends with, under the operator or under the ordinary plan.
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

    // True when the HashJoinBuildStage method is on the current thread's stack.
    private static boolean isOnStack(String methodName) {
        return StackWalker.getInstance().walk(frames -> frames.anyMatch(frame -> frame.getClassName().equals(HashJoinBuildStage.class.getName())
                && frame.getMethodName().equals(methodName)));
    }

    private static long ordinaryCount(String sql) throws SqlException {
        sqlExecutionContext.setParallelHashJoinEnabled(false);
        try {
            return count(sql);
        } finally {
            sqlExecutionContext.setParallelHashJoinEnabled(true);
        }
    }

    // The rows of the query's ordinary plan, which runs no operator, read one by one.
    private static long ordinaryRowCount(String sql) throws SqlException {
        sqlExecutionContext.setParallelHashJoinEnabled(false);
        try (RecordCursorFactory factory = select(sql); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            Assert.assertNull(sql, find(factory));
            long count = 0;
            while (cursor.hasNext()) {
                count++;
            }
            return count;
        } finally {
            sqlExecutionContext.setParallelHashJoinEnabled(true);
        }
    }
}
