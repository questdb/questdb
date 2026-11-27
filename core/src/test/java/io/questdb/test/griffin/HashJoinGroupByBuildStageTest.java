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
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.test.TestLatchedCounterFunctionFactory;
import io.questdb.griffin.engine.join.FrozenHashJoinBuild;
import io.questdb.griffin.engine.table.AsyncHashJoinGroupByAtom;
import io.questdb.griffin.engine.table.HashJoinBuildStage;
import io.questdb.jit.JitUtil;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.LimitedMemoryTracker;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.context;
import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.fused;
import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.ordinary;
import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.plan;
import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.result;

/**
 * Runs the build stage of the fused hash join GROUP BY through the paths that the other suites of
 * the operator leave out: probe loops over a partitioned build of one row per key under a probe
 * filter, the compiled build filter's fallback to the interpreted one, a small build whose filter
 * runs interpreted over more than one frame, which builds in rounds in every key layout, context and
 * under a memory limit, an execution that tolerates
 * row count changes, a build filter whose cleanup fails, and the stage's accessors for a layout
 * that has no stager or no such SYMBOL key pair. Every result compares with the ordinary plan.
 */
public class HashJoinGroupByBuildStageTest extends AbstractCairoTest {
    // The INT layout's INT and lone SYMBOL keys, a LONG key staged into the build's map, and a staged
    // key with a SYMBOL pair, which translates through the staged key's record.
    private static final String[] KEYS = {"r.k = b.k", "r.s = b.s", "r.l = b.l", "r.s = b.s AND r.l = b.l"};
    private static final StackWalker STACK_WALKER = StackWalker.getInstance();
    private static final String STAGE = HashJoinBuildStage.class.getName();

    @After
    public void resetLatchedCounter() {
        TestLatchedCounterFunctionFactory.reset(null);
    }

    @Test
    public void testBuildAcrossKeyLayoutsAndBuildModes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // One row per key: the first 20 rows of u.
            execute("CREATE TABLE d AS (SELECT * FROM u WHERE ts < '2020-01-01T03:20')");
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                for (HashJoinBuildMode mode : HashJoinBuildMode.values()) {
                    // The parallel mode also shrinks the page frames, so that the builds spread over
                    // several frames and hash partitions.
                    mode.apply(node1.getConfigurationOverrides(), context);
                    final boolean isParallel = mode == HashJoinBuildMode.PARALLEL;
                    // A build that repeats its keys, and one of a row per key.
                    for (String build : new String[]{"u", "d"}) {
                        for (String key : KEYS) {
                            // No filter, a probe filter, a build filter, and both.
                            for (String where : new String[]{"", " WHERE r.v > 100", " WHERE b.w > 10", " WHERE r.v > 100 AND b.w > 10"}) {
                                final String from = " FROM r JOIN " + build + " b ON " + key + where;
                                assertBuild("SELECT r.g, count() n, sum(b.w) w, sum(r.v) v" + from + " ORDER BY r.g", isParallel, context);
                                // A lone count takes each key's row count from the build.
                                assertBuild("SELECT count() n" + from, isParallel, context);
                            }
                            // An outer join keeps every probe row, and its ON condition on the build
                            // alone drops build rows.
                            for (String on : new String[]{"", " AND b.w > 10"}) {
                                for (String where : new String[]{"", " WHERE r.v > 100"}) {
                                    assertBuild("SELECT r.g, count() n, sum(b.w) w, sum(r.v) v FROM r LEFT JOIN " + build + " b ON "
                                            + key + on + where + " ORDER BY r.g", isParallel, context);
                                }
                            }
                        }
                    }
                }
                context.restoreToDefaultPageFrameSizes();
            }
        });
    }

    @Test
    public void testBuildFilterCursorClosedFailure() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String sql = "SELECT count() n, sum(u.w) w FROM r JOIN (SELECT * FROM u WHERE test_latched_counter()) u ON r.k = u.k";
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                final String expected = ordinary(engine, sql, context);
                // The function takes the callback when it compiles, so each case compiles anew.
                final AtomicInteger cursorClosedCalls = new AtomicInteger();
                TestLatchedCounterFunctionFactory.reset(new TestLatchedCounterFunctionFactory.Callback() {
                    @Override
                    public void onCursorClosed() {
                        cursorClosedCalls.incrementAndGet();
                        throw CairoException.nonCritical().put("injected build filter cleanup failure");
                    }
                });
                try (RecordCursorFactory factory = engine.select(sql, context)) {
                    final String plan = plan(factory, context);
                    Assert.assertTrue(plan, plan.contains("Hash Join Group By"));
                    Assert.assertTrue(plan, plan.contains("buildFilter: test_latched_counter()"));
                    // The query's own wrapper logs a cursor's close failure, so the case reads the
                    // fused factory's cursor directly.
                    final RecordCursorFactory fusedFactory = fused(factory);
                    final AsyncHashJoinGroupByAtom atom = fused(factory).getAtom();
                    for (int run = 0; run < 2; run++) {
                        final RecordCursor cursor = fusedFactory.getCursor(context);
                        Assert.assertEquals(sql, expected, print(cursor, fusedFactory));
                        try {
                            cursor.close();
                            Assert.fail("expected the build filter's cleanup to fail");
                        } catch (CairoException e) {
                            TestUtils.assertContains(e.getFlyweightMessage(), "injected build filter cleanup failure");
                        }
                        // The failure did not stop the rest of the cleanup: the build, its scan and
                        // the slots are released, and the next execution builds anew.
                        Assert.assertEquals(run + 1, cursorClosedCalls.get());
                        Assert.assertNull(atom.getFrozenBuild());
                        Assert.assertEquals(0, atom.getBuildFrameCount());
                        Assert.assertEquals(0, atom.getPerWorkerLocks().getAcquiredSlotCount());
                    }
                }
                // A callback that keeps the default cleanup, and no callback at all, close cleanly.
                TestLatchedCounterFunctionFactory.reset(new TestLatchedCounterFunctionFactory.Callback() {
                });
                assertFusedMatches(sql, expected, context);
                TestLatchedCounterFunctionFactory.reset(null);
                assertFusedMatches(sql, expected, context);
            }
        });
    }

    @Test
    public void testCompiledBuildFilterFallsBackOnColumnTopsAndTypeCasts() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                // The column exists only from here on, so the build's partitions written before it
                // carry a column top, which the compiled build filter, reading raw column addresses,
                // cannot read.
                execute("ALTER TABLE u ADD COLUMN extra DOUBLE");
                execute("INSERT INTO u SELECT (x % 20)::INT, (x % 20)::LONG, ('s' || (x % 20))::SYMBOL, x * 0.25, x::DOUBLE,"
                        + " timestamp_sequence('2021-01-01', 600_000_000), (x % 7)::DOUBLE FROM long_sequence(40)");
                assertCompiledBuildFilter("SELECT r.g, count() n, sum(u.w) w FROM r JOIN u ON r.k = u.k"
                        + " WHERE u.w > 2 AND (u.extra IS NULL OR u.extra > 3) ORDER BY r.g", context);
                // A Parquet column converted from a variable to a fixed size resolves per row
                // through the logical record, which the compiled filter cannot address either. The
                // active partition never converts, so native frames stay in the mix.
                execute("""
                        CREATE TABLE v AS (
                            SELECT (x % 20)::INT k, x * 0.5 w, (x % 7)::VARCHAR q, timestamp_sequence('2020-01-01', 600_000_000) ts
                            FROM long_sequence(400)
                        ) TIMESTAMP(ts) PARTITION BY DAY""");
                execute("ALTER TABLE v CONVERT PARTITION TO PARQUET WHERE ts < '2020-01-03'");
                execute("ALTER TABLE v ALTER COLUMN q TYPE INT");
                assertCompiledBuildFilter("SELECT r.g, count() n, sum(v.w) w FROM r JOIN v ON r.k = v.k"
                        + " WHERE v.q > 3 ORDER BY r.g", context);
            }
        });
    }

    @Test
    public void testInterpretedBuildFilterBuildsInRounds() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // One frame: the first 20 rows of u, in a table without partitions.
            execute("CREATE TABLE d AS (SELECT * FROM u WHERE ts < '2020-01-01T03:20')");
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                // u's 200 rows over two days, a frame each, far fewer than the configured 300_000. A
                // filter that the JIT does not compile, a LIKE over a DOUBLE read as a STRING and a
                // regex over a SYMBOL read as one, or one that it would compile, run with the JIT off,
                // builds in rounds: keyed, scalar and lone count, over every key layout.
                final String[] filters = {"b.note::STRING LIKE '1%'", "b.s::STRING ~ 's1[0-9]'", "b.w > 2"};
                for (String key : KEYS) {
                    for (String filter : filters) {
                        context.setJitMode(filter.equals("b.w > 2") ? SqlJitMode.JIT_MODE_DISABLED : SqlJitMode.JIT_MODE_ENABLED);
                        final String from = " FROM r JOIN u b ON " + key + " WHERE " + filter;
                        assertBuild("SELECT r.g, count() n, sum(b.w) w, sum(r.v) v" + from + " ORDER BY r.g", true, context);
                        assertBuild("SELECT count() n, sum(b.w) w" + from, true, context);
                        assertBuild("SELECT count() n" + from, true, context);
                    }
                }
                context.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
                // A filter that the JIT compiled, a build of one frame, a build without a filter, and an
                // outer join's ON condition on the build alone leave the decision to the rows, so the
                // owner builds.
                final boolean isJit = JitUtil.isJitSupported();
                assertBuild("SELECT r.g, count() n, sum(b.w) w FROM r JOIN u b ON r.k = b.k WHERE b.w > 2 ORDER BY r.g", !isJit, context);
                assertBuild("SELECT r.g, count() n, sum(b.w) w FROM r JOIN d b ON r.k = b.k WHERE b.note::STRING LIKE '1%' ORDER BY r.g", false, context);
                assertBuild("SELECT r.g, count() n, sum(b.w) w FROM r JOIN u b ON r.k = b.k ORDER BY r.g", false, context);
                final String buildOn = "SELECT r.g, count() n, sum(b.w) w FROM r LEFT JOIN u b ON r.k = b.k AND b.note::STRING LIKE '1%' ORDER BY r.g";
                try (RecordCursorFactory factory = engine.select(buildOn, context)) {
                    TestUtils.assertContains(plan(factory, context), "buildOnFilter: ");
                }
                assertBuild(buildOn, false, context);
                // The rounds split the kept rows into hash partitions of about rows.per.partition rows:
                // the LIKE keeps the 111 rows with note 1, 10 to 19 and 100 to 199, more than 16.
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_ROWS_PER_PARTITION, 16);
                final String partitioned = "SELECT r.g, count() n, sum(b.w) w FROM r JOIN u b ON r.l = b.l WHERE b.note::STRING LIKE '1%' ORDER BY r.g";
                assertBuild(partitioned, true, context);
                try (RecordCursorFactory factory = engine.select(partitioned, context)) {
                    final AsyncHashJoinGroupByAtom atom = fused(factory).getAtom();
                    try (RecordCursor cursor = factory.getCursor(context)) {
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertEquals(111, atom.getFrozenBuild().getRowCount());
                        Assert.assertTrue(atom.getBuildPartitionCount() > 1);
                    }
                }
            }
        });
    }

    @Test
    public void testInterpretedBuildFilterInSqlContexts() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                // The LIKE over u's two frames builds in rounds, as the test_latched_counter() of the
                // build scan's filter records, wherever the fused GROUP BY runs: under SAMPLE BY, ORDER
                // BY with LIMIT, a negative LIMIT, a CTE, UNION ALL, and a build choice whose chosen
                // orientation builds u.
                final String build = "(SELECT * FROM u WHERE note::STRING LIKE '1%' AND test_latched_counter()) b";
                final String grouped = "SELECT r.g, count() n, sum(b.w) w FROM r JOIN " + build + " ON r.k = b.k";
                for (String sql : new String[]{
                        "SELECT r.ts, count() n, sum(b.w) w FROM r JOIN " + build + " ON r.k = b.k SAMPLE BY 1h ALIGN TO CALENDAR",
                        grouped + " ORDER BY n DESC, r.g LIMIT 3",
                        grouped + " ORDER BY r.g LIMIT -2",
                        "WITH j AS (" + grouped + ") SELECT g, n, w FROM j ORDER BY g",
                        "(" + grouped + " UNION ALL SELECT r.g, count() n, sum(b.w) w FROM r JOIN " + build + " ON r.l = b.l) ORDER BY 1, 2, 3",
                }) {
                    assertFusedFilterRunsInRounds(sql, context);
                }
                // r's interval holds ten hours of rows, 600, more than u's 200, so the build choice
                // builds u.
                final String choice = "SELECT r.g, count() n, sum(b.w) w FROM r JOIN " + build + " ON r.k = b.k WHERE r.ts IN '2020-01-01T00:00;10h' ORDER BY r.g";
                try (RecordCursorFactory factory = engine.select(choice, context)) {
                    TestUtils.assertContains(plan(factory, context), "Hash Join Group By Build Choice");
                }
                assertFusedFilterRunsInRounds(choice, context);
                // EXPLAIN shows the filter as before: the decision is the execution's.
                try (RecordCursorFactory factory = engine.select("SELECT r.g, count() n, sum(b.w) w FROM r JOIN u b ON r.k = b.k WHERE b.note::STRING LIKE '1%'", context)) {
                    TestUtils.assertContains(plan(factory, context), "buildFilter: note::string like 1%");
                }
            }
        });
    }

    @Test
    public void testPartitionedUniqueBuildUnderProbeFilter() throws Exception {
        HashJoinBuildMode.PARALLEL.applyProperties(node1.getConfigurationOverrides());
        assertMemoryLeak(() -> {
            createTables();
            // One row per key: the first 20 rows of u.
            execute("CREATE TABLE d AS (SELECT * FROM u WHERE ts < '2020-01-01T03:20')");
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                // Frames of four rows spread the 20 build rows over several hash partitions.
                context.changePageFrameSizes(4, 4);
                // A key staged into the build's map, and the INT layout's translated SYMBOL key.
                for (String key : new String[]{"r.l = d.l", "r.s = d.s"}) {
                    final String sql = "SELECT r.g, count() n, sum(d.w) w, sum(r.v) v FROM r JOIN d ON " + key
                            + " WHERE r.v > 100 ORDER BY r.g";
                    final String expected = ordinary(engine, sql, context);
                    try (RecordCursorFactory factory = engine.select(sql, context)) {
                        final String plan = plan(factory, context);
                        Assert.assertTrue(plan, plan.contains("Hash Join Group By"));
                        Assert.assertTrue(plan, plan.contains("probeFilter: 100<v"));
                        final AsyncHashJoinGroupByAtom atom = fused(factory).getAtom();
                        for (int run = 0; run < 2; run++) {
                            try (RecordCursor cursor = factory.getCursor(context)) {
                                Assert.assertEquals(sql, expected, print(cursor, factory));
                                Assert.assertTrue(sql, atom.isBuiltInRounds());
                                Assert.assertTrue(sql, atom.getBuildPartitionCount() > 1);
                                final FrozenHashJoinBuild build = atom.getFrozenBuild();
                                Assert.assertEquals(sql, 20, build.getRowCount());
                                Assert.assertEquals(sql, 20, build.getKeyCount());
                            }
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testRowCountChangeToleratedKeepsCompiledChoice() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // r holds 2_000 rows and u 200, so the LEFT join presizes its key table by u's rows.
            final String sql = "SELECT r.g, count() n, sum(u.w) w FROM r LEFT JOIN u ON r.k = u.k ORDER BY r.g";
            try (
                    SqlExecutionContextImpl context = context(engine, 4);
                    SqlExecutionContextImpl tolerant = new SqlExecutionContextImpl(engine, 4) {
                        @Override
                        public boolean isTableRowCountChangeTolerated() {
                            return true;
                        }
                    }.with(AllowAllSecurityContext.INSTANCE, null, null, -1, null)
            ) {
                tolerant.setParallelGroupByEnabled(true);
                tolerant.setParallelHashJoinGroupByEnabled(true);
                try (RecordCursorFactory factory = engine.select(sql, context)) {
                    Assert.assertTrue(plan(factory, context).contains("Hash Join Group By"));
                    // u grows past twice r's rows, so the compile-time presize no longer holds.
                    execute("INSERT INTO u SELECT (x % 20)::INT, (x % 20)::LONG, ('s' || (x % 20))::SYMBOL, x * 0.5,"
                            + " x::DOUBLE, timestamp_sequence('2021-01-01', 60_000_000) FROM long_sequence(4_000)");
                    final String expected = ordinary(engine, sql, context);
                    try {
                        result(factory, context);
                        Assert.fail("expected a recompile request");
                    } catch (TableReferenceOutOfDateException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "table schema has changed [table=u]");
                    }
                    // A context that tolerates the change keeps the compile-time choice and runs
                    // to the same result.
                    Assert.assertEquals(expected, result(factory, tolerant));
                    Assert.assertEquals(expected, result(factory, tolerant));
                    Assert.assertEquals(0, fused(factory).getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                }
            }
        });
    }

    @Test
    public void testSmallFilteredBuildMemoryLimitAndReuse() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (
                    SqlExecutionContextImpl context = context(engine, 4);
                    LimitedMemoryTracker tracker = new LimitedMemoryTracker(Long.MAX_VALUE)
            ) {
                // The LIKE runs interpreted over u's two frames, so the build runs in rounds although u
                // holds fewer rows than the configured 300_000.
                final String sql = "SELECT r.g, count() n, sum(b.w) w FROM r JOIN u b ON r.l = b.l WHERE b.note::STRING LIKE '1%' ORDER BY r.g";
                final String expected = ordinary(engine, sql, context);
                try (RecordCursorFactory factory = engine.select(sql, context)) {
                    final AsyncHashJoinGroupByAtom atom = fused(factory).getAtom();
                    // Limits from a few bytes up fail the execution at its allocations in turn, those
                    // of the build's rounds among them; each failure leaves nothing charged, no build
                    // and no slot taken, and the first limit that admits the execution reads its rows.
                    int roundFailures = 0;
                    for (long limit = 4; ; limit *= 2) {
                        tracker.setLimit(limit);
                        context.setMemoryTracker(tracker);
                        // The sort above the operator charges when it opens, so the sweep runs the
                        // operator alone.
                        try (RecordCursor cursor = fused(factory).getCursor(context)) {
                            //noinspection StatementWithEmptyBody
                            while (cursor.hasNext()) {
                            }
                            Assert.assertTrue(atom.isBuiltInRounds());
                            break;
                        } catch (CairoException e) {
                            Assert.assertTrue(e.getFlyweightMessage().toString(), e.isOutOfMemory());
                            if (isThrownFrom(e, HashJoinBuildStage.class.getName(), "buildInRounds")) {
                                roundFailures++;
                            }
                        } finally {
                            context.setMemoryTracker(null);
                        }
                        Assert.assertEquals(0, tracker.getUsed());
                        Assert.assertNull(atom.getFrozenBuild());
                        Assert.assertEquals(0, atom.getPerWorkerLocks().getAcquiredSlotCount());
                    }
                    Assert.assertTrue(roundFailures > 0);
                    Assert.assertEquals(expected, result(factory, context));
                    Assert.assertEquals(0, atom.getPerWorkerLocks().getAcquiredSlotCount());
                }
            }
        });
    }

    @Test
    public void testStageAccessorsOutsideTheKeyLayout() throws Exception {
        HashJoinBuildMode.PARALLEL.applyProperties(node1.getConfigurationOverrides());
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                context.changePageFrameSizes(16, 16);
                // The INT layout stages no key, so its slots hold no stager.
                final String intSql = "SELECT r.g, count() n, sum(u.w) w FROM r JOIN u ON r.k = u.k ORDER BY r.g";
                final String intExpected = ordinary(engine, intSql, context);
                try (RecordCursorFactory factory = engine.select(intSql, context)) {
                    final AsyncHashJoinGroupByAtom atom = fused(factory).getAtom();
                    try (RecordCursor cursor = factory.getCursor(context)) {
                        Assert.assertEquals(intExpected, print(cursor, factory));
                        Assert.assertTrue(atom.isBuiltInRounds());
                        Assert.assertEquals(0, atom.getBuildKeyStagerSizeInBytes());
                        // The key has no SYMBOL pair, so no translation cache exists.
                        Assert.assertFalse(atom.isSymbolKeyCacheFilledFromBuild(0));
                    }
                }
                // A staged key's stagers release their scratch once the frame tasks are done.
                final String stagedSql = "SELECT r.g, count() n, sum(u.w) w FROM r JOIN u ON r.l = u.l ORDER BY r.g";
                final String stagedExpected = ordinary(engine, stagedSql, context);
                try (RecordCursorFactory factory = engine.select(stagedSql, context)) {
                    final AsyncHashJoinGroupByAtom atom = fused(factory).getAtom();
                    try (RecordCursor cursor = factory.getCursor(context)) {
                        Assert.assertEquals(stagedExpected, print(cursor, factory));
                        Assert.assertTrue(atom.isBuiltInRounds());
                        Assert.assertEquals(0, atom.getBuildKeyStagerSizeInBytes());
                    }
                }
                // The lone SYMBOL pair has one cache, which opens unresolved: r's dictionary does not
                // hold eight times u's keys. There is no second pair.
                final String symbolSql = "SELECT r.g, count() n, sum(u.w) w FROM r JOIN u ON r.s = u.s ORDER BY r.g";
                final String symbolExpected = ordinary(engine, symbolSql, context);
                try (RecordCursorFactory factory = engine.select(symbolSql, context)) {
                    final AsyncHashJoinGroupByAtom atom = fused(factory).getAtom();
                    try (RecordCursor cursor = factory.getCursor(context)) {
                        Assert.assertEquals(symbolExpected, print(cursor, factory));
                        Assert.assertFalse(atom.isSymbolKeyCacheFilledFromBuild(0));
                        Assert.assertFalse(atom.isSymbolKeyCacheFilledFromBuild(1));
                    }
                }
            }
        });
    }

    private static void assertBuild(String sql, boolean isParallel, SqlExecutionContextImpl context) throws Exception {
        final String expected = ordinary(engine, sql, context);
        try (RecordCursorFactory factory = engine.select(sql, context)) {
            final String plan = plan(factory, context);
            Assert.assertTrue(sql + '\n' + plan, plan.contains("Hash Join Group By"));
            final AsyncHashJoinGroupByAtom atom = fused(factory).getAtom();
            for (int run = 0; run < 2; run++) {
                try (RecordCursor cursor = factory.getCursor(context)) {
                    Assert.assertEquals(sql, expected, print(cursor, factory));
                    Assert.assertEquals(sql, isParallel, atom.isBuiltInRounds());
                }
                Assert.assertNull(sql, atom.getFrozenBuild());
                Assert.assertEquals(sql, 0, atom.getPerWorkerLocks().getAcquiredSlotCount());
            }
        }
    }

    // Compares the query with its plan without the fused GROUP BY, and asserts that every build
    // filter's test_latched_counter() ran in a frame task of a build's rounds, never on the owner's
    // build.
    private static void assertFusedFilterRunsInRounds(String sql, SqlExecutionContextImpl context) throws Exception {
        TestLatchedCounterFunctionFactory.reset(null);
        final String expected = ordinary(engine, sql, context);
        final AtomicInteger roundCalls = new AtomicInteger();
        final AtomicInteger ownerCalls = new AtomicInteger();
        // The function takes the callback when it compiles.
        TestLatchedCounterFunctionFactory.reset(new TestLatchedCounterFunctionFactory.Callback() {
            @Override
            public boolean onGet(Record rec, int count) {
                if (isOnStack("partitionFrame")) {
                    roundCalls.incrementAndGet();
                } else if (isOnStack("buildOnOwner")) {
                    ownerCalls.incrementAndGet();
                }
                return true;
            }
        });
        try (RecordCursorFactory factory = engine.select(sql, context)) {
            final String plan = plan(factory, context);
            TestUtils.assertContains(plan, "Hash Join Group By");
            TestUtils.assertContains(plan, "buildFilter: ");
            Assert.assertEquals(sql, expected, result(factory, context));
            Assert.assertEquals(sql, expected, result(factory, context));
        } finally {
            TestLatchedCounterFunctionFactory.reset(null);
        }
        Assert.assertTrue(sql, roundCalls.get() > 0);
        Assert.assertEquals(sql, 0, ownerCalls.get());
    }

    private static void assertCompiledBuildFilter(String sql, SqlExecutionContextImpl context) throws Exception {
        final String expected = ordinary(engine, sql, context);
        try (RecordCursorFactory factory = engine.select(sql, context)) {
            final String plan = plan(factory, context);
            Assert.assertTrue(plan, plan.contains("Hash Join Group By"));
            Assert.assertTrue(plan, fused(factory).hasCompiledBuildFilter());
            Assert.assertEquals(sql, expected, result(factory, context));
        }
    }

    private static void assertFusedMatches(String sql, String expected, SqlExecutionContextImpl context) throws Exception {
        try (RecordCursorFactory factory = engine.select(sql, context)) {
            Assert.assertTrue(plan(factory, context).contains("Hash Join Group By"));
            Assert.assertEquals(sql, expected, result(factory, context));
            Assert.assertEquals(expected, result(factory, context));
        }
    }

    // r holds 2_000 probe rows of 20 keys over two days, and u 200 build rows of the same 20 keys,
    // ten per key, over two weeks. "note" takes part in no query, so a storage-level conversion of
    // it changes which filter the build runs without changing what the results must be.
    private static void createTables() throws Exception {
        execute("""
                CREATE TABLE r AS (
                    SELECT (x % 20)::INT k, (x % 20)::LONG l, ('s' || (x % 20))::SYMBOL s, (x % 5)::INT g, x::DOUBLE v,
                        timestamp_sequence('2020-01-01', 60_000_000) ts
                    FROM long_sequence(2_000)
                ) TIMESTAMP(ts) PARTITION BY DAY""");
        execute("""
                CREATE TABLE u AS (
                    SELECT (x % 20)::INT k, (x % 20)::LONG l, ('s' || (x % 20))::SYMBOL s, x * 0.5 w, x::DOUBLE note,
                        timestamp_sequence('2020-01-01', 600_000_000) ts
                    FROM long_sequence(200)
                ) TIMESTAMP(ts) PARTITION BY DAY""");
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

    private static String print(RecordCursor cursor, RecordCursorFactory factory) {
        final StringSink sink = new StringSink();
        CursorPrinter.println(cursor, factory.getMetadata(), sink, true, true);
        return sink.toString();
    }
}
