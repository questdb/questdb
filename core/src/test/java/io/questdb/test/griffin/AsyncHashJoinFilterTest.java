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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.HashJoinGroupByKeys;
import io.questdb.griffin.HashJoinKeyLayout;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.join.JoinRecordMetadata;
import io.questdb.griffin.engine.table.AsyncFilterContext;
import io.questdb.griffin.engine.table.AsyncHashJoinRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncHashJoinSpec;
import io.questdb.griffin.engine.table.HashJoinBuildStage;
import io.questdb.griffin.engine.table.HashJoinInput;
import io.questdb.griffin.engine.table.HashJoinStageSpec;
import io.questdb.jit.CompiledFilter;
import io.questdb.jit.JitUtil;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.test.griffin.ParallelHashJoinTestUtils.*;

/**
 * The parallel hash join over inputs with a filter that it steals: the left input's filter runs on
 * the workers, over each probe frame, before the frame's rows probe the build, and the build input's
 * filter runs as the build walks its frames, on the owner or in the build's rounds. Each runs as JIT
 * code when the filter it was stolen from did, and falls back to the interpreted filter on frames
 * with column tops or Parquet columns that the record converts per row. The rows, their order,
 * types and symbol tables match the ordinary plan of the same SQL, whose filters run in their own
 * filter factories. A filtered input reports no size to the ordinary plan, which therefore never
 * swaps its inputs over it, so INNER joins over a filtered input take the operator.
 */
public class AsyncHashJoinFilterTest extends AbstractCairoTest {
    private static final String[] BUILD_FILTERS = {
            // Some rows, every row and no row.
            "w > 10",
            "w > 0",
            "w > 1_000",
            // A SYMBOL constant, NULL keys, and a conjunction.
            "s = 's2'",
            "k IS NULL",
            "w % 3 = 0 AND s <> 's2'",
            // A regex, which the JIT does not compile and which is not thread safe.
            "vch ~ 'v[12]'",
    };
    private static final String[] JOINS = {" LEFT JOIN ", " JOIN "};
    private static final String[] PROBE_FILTERS = {
            // Some rows, every row and no row.
            "t.v > 20",
            "t.v > 0",
            "t.v > 1_000",
            // A SYMBOL constant, NULL keys, and a conjunction and a disjunction.
            "t.sym = 's1'",
            "t.k IS NULL",
            "t.sym IN ('s1', 's3') AND t.v % 2 = 0",
            "t.str IS NULL OR t.v BETWEEN 10 AND 30",
            // A regex, which the JIT does not compile and which is not thread safe.
            "t.str ~ 'str[13]'",
    };

    @Override
    @Before
    public void setUp() {
        super.setUp();
        sqlExecutionContext.setParallelHashJoinEnabled(true);
    }

    @After
    public void restorePageFrameSizes() {
        sqlExecutionContext.restoreToDefaultPageFrameSizes();
    }

    @Test
    public void testBackwardBuildScanWithFilter() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // Pairs of consecutive rows share a key, so a frame of the build holds duplicates, whose
            // order in the frame the build's row order decides, and the filter drops some of them.
            execute("""
                    CREATE TABLE b AS (
                        SELECT (x / 2 % 5)::INT k, x w, ('b' || (x % 3))::SYMBOL s, timestamp_sequence('2023-12-31', 600_000_000) dts
                        FROM long_sequence(40)
                    ) TIMESTAMP(dts) PARTITION BY HOUR
                    """);
            // A sub-query's ORDER BY sorts its rows, so no SQL hashes a backward scan yet. The test
            // plans the join itself, over the filter of a build query that scans b backward, and
            // compares it with the ordinary join over the sorted, filtered sub-query: b's timestamps
            // are unique, so the sort yields the rows in the backward scan's order, and the serial
            // join chains them in that order.
            final String expected = rows(ordinary(
                    "SELECT t.k, t.v, t.sym, t.str, t.ts, b.k, b.w, b.s, b.dts FROM t LEFT JOIN (SELECT * FROM b WHERE w > 5 AND w < 35 ORDER BY dts DESC) b ON t.k = b.k",
                    sqlExecutionContext
            ));
            sqlExecutionContext.changePageFrameSizes(2, 3);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_ROWS_PER_PARTITION, 1);
            for (int jitMode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                sqlExecutionContext.setJitMode(jitMode);
                for (long minRows : new long[]{Long.MAX_VALUE, 0}) {
                    setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, minRows);
                    final ObjList<CountingCompiledFilter> compiledFilters = new ObjList<>();
                    try (
                            RecordCursorFactory probe = select("SELECT * FROM t");
                            RecordCursorFactory build = select("SELECT * FROM b WHERE w > 5 AND w < 35 ORDER BY dts DESC");
                            AsyncHashJoinRecordCursorFactory factory = newLeftJoin(probe, build, compiledFilters)
                    ) {
                        final String plan = plan(factory, sqlExecutionContext);
                        TestUtils.assertContains(plan, "Frame backward scan on: b");
                        final boolean isCompiled = jitMode == SqlJitMode.JIT_MODE_ENABLED && JitUtil.isJitSupported();
                        TestUtils.assertContains(plan, isCompiled ? "buildJitFilter: " : "buildFilter: ");
                        Assert.assertEquals(isCompiled ? 1 : 0, compiledFilters.size());
                        Assert.assertEquals(expected, rows(result(factory, sqlExecutionContext)));
                        final HashJoinBuildStage stage = factory.getAtom().getStage(0);
                        if (isCompiled) {
                            compiledFilters.getQuick(0).calls.set(0);
                        }
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            Assert.assertTrue(cursor.hasNext());
                            Assert.assertEquals(minRows == 0, stage.isBuiltInRounds());
                            // The filter kept the 29 rows with w from 6 to 34.
                            Assert.assertEquals(29, stage.getFrozenBuild().getRowCount());
                            // Native frames without column tops run the compiled filter, each once.
                            if (isCompiled) {
                                Assert.assertTrue(stage.getBuildFrameCount() > 1);
                                Assert.assertEquals(stage.getBuildFrameCount(), compiledFilters.getQuick(0).calls.get());
                            }
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testBackwardProbeScanWithFilter() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            for (int jitMode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                sqlExecutionContext.setJitMode(jitMode);
                for (int frameRows : new int[]{1, 3, 1_000_000}) {
                    sqlExecutionContext.changePageFrameSizes(frameRows, frameRows);
                    // ORDER BY the designated timestamp descending runs the left input backward, and
                    // the join keeps that order: each frame's kept rows probe from the last one.
                    for (String join : JOINS) {
                        for (String filter : new String[]{"t.v > 20", "t.str ~ 'str[13]'", "t.v > 1_000"}) {
                            final String plan = assertParallelHashJoin(
                                    "SELECT t.k, t.v, t.ts, d.w, d.s FROM t" + join + "d ON t.k = d.k WHERE " + filter + " ORDER BY t.ts DESC",
                                    sqlExecutionContext
                            );
                            TestUtils.assertContains(plan, "Frame backward scan on: t");
                            TestUtils.assertContains(plan, "filter: ");
                            Assert.assertFalse(plan, plan.contains("Sort"));
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testBindVariablesInFilters() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // $1 and $3 filter the probe, $2 the build; $3 compares a SYMBOL column with a STRING
            // variable, which a compiled filter looks up in the column's symbol table.
            final String sql = """
                    SELECT t.k, t.v, t.sym, d2.w, d2.s
                    FROM t LEFT JOIN (SELECT * FROM d WHERE w > $2) d2 ON t.k = d2.k
                    WHERE t.v > $1 AND t.sym <> $3
                    """;
            for (int jitMode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                sqlExecutionContext.setJitMode(jitMode);
                bind(20, 10, "s1");
                try (RecordCursorFactory factory = select(sql)) {
                    final String plan = plan(factory, sqlExecutionContext);
                    final boolean isCompiled = jitMode == SqlJitMode.JIT_MODE_ENABLED && JitUtil.isJitSupported();
                    Assert.assertEquals(plan, isCompiled, plan.contains("Async JIT Hash Join"));
                    Assert.assertEquals(plan, isCompiled, plan.contains("buildJitFilter: "));
                    // Each execution reads the values bound at the time: the compiled filters take them
                    // into their bind variable memory, the interpreted ones read the variables.
                    final String first = result(factory, sqlExecutionContext);
                    Assert.assertEquals(ordinary(sql, sqlExecutionContext), first);
                    bind(40, 30, "s2");
                    final String second = result(factory, sqlExecutionContext);
                    Assert.assertEquals(ordinary(sql, sqlExecutionContext), second);
                    Assert.assertNotEquals(first, second);
                    // Values that every row passes, and values that no row passes.
                    bind(0, 0, "none");
                    Assert.assertEquals(ordinary(sql, sqlExecutionContext), result(factory, sqlExecutionContext));
                    bind(1_000, 1_000, "s1");
                    Assert.assertEquals(ordinary(sql, sqlExecutionContext), result(factory, sqlExecutionContext));
                }
            }
        });
    }

    @Test
    public void testBuildFilter() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_ROWS_PER_PARTITION, 4);
            for (int jitMode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                sqlExecutionContext.setJitMode(jitMode);
                for (long minRows : new long[]{Long.MAX_VALUE, 0}) {
                    setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, minRows);
                    for (int frameRows : new int[]{3, 1_000_000}) {
                        sqlExecutionContext.changePageFrameSizes(frameRows, frameRows);
                        for (String filter : BUILD_FILTERS) {
                            for (String join : JOINS) {
                                final String sql = "SELECT t.k, t.v, d2.w, d2.s, d2.vch FROM t" + join + "(SELECT * FROM d WHERE " + filter + ") d2 ON t.k = d2.k";
                                final String plan = assertParallelHashJoin(sql, sqlExecutionContext);
                                // The stolen filter runs as JIT code when the ordinary plan's filter does.
                                final boolean isCompiled = ordinaryPlan(sql).contains("Async JIT Filter");
                                Assert.assertEquals(plan, isCompiled, plan.contains("buildJitFilter: "));
                                Assert.assertEquals(plan, !isCompiled, plan.contains("buildFilter: "));
                                // The operator's name speaks for the probe filter only.
                                TestUtils.assertContains(plan, "Async Hash Join workers");
                                assertBuiltInRounds(sql, minRows == 0);
                            }
                        }
                    }
                }
                final boolean isJitExpected = jitMode == SqlJitMode.JIT_MODE_ENABLED && JitUtil.isJitSupported();
                Assert.assertEquals(isJitExpected, planText("SELECT t.k, d2.w FROM t LEFT JOIN (SELECT * FROM d WHERE w > 10) d2 ON t.k = d2.k").contains("buildJitFilter: 10<w"));
                // A LIMIT stops the cursor with frames in flight, and a negative LIMIT counts the rows
                // first, then reads them again. The optimiser hands a LIMIT to the left input's
                // filter, which the join cannot steal then, but not to the build's.
                for (String limit : new String[]{" LIMIT 4", " LIMIT -4", " LIMIT 3, 7"}) {
                    assertParallelHashJoin("SELECT t.k, t.v, d2.w FROM t LEFT JOIN (SELECT * FROM d WHERE w > 10) d2 ON t.k = d2.k" + limit, sqlExecutionContext);
                    assertParallelHashJoin("SELECT t.k, t.v, d2.w FROM t JOIN (SELECT * FROM d WHERE vch ~ 'v[12]') d2 ON t.k = d2.k" + limit, sqlExecutionContext);
                }
            }
        });
    }

    @Test
    public void testColumnTopsAndParquetFallBackToInterpretedFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (k INT, v LONG, note DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE d (k INT, w LONG, note DOUBLE, dts TIMESTAMP) TIMESTAMP(dts) PARTITION BY DAY");
            execute("INSERT INTO t SELECT (x % 7)::INT, x, CASE WHEN x % 3 = 0 THEN NULL ELSE x::DOUBLE END, timestamp_sequence('2024-01-01', 7_200_000_000) FROM long_sequence(40)");
            execute("INSERT INTO d SELECT (x % 5)::INT, x, CASE WHEN x % 4 = 0 THEN NULL ELSE x::DOUBLE END, timestamp_sequence('2024-01-01', 10_800_000_000) FROM long_sequence(30)");
            // Columns added after the first rows: every partition written before carries a column top,
            // where a compiled filter would read no column at all, so those frames take the
            // interpreted filter; the later ones run the compiled filter.
            execute("ALTER TABLE t ADD COLUMN extra INT");
            execute("ALTER TABLE d ADD COLUMN dextra INT");
            execute("INSERT INTO t SELECT (x % 7)::INT, x, x::DOUBLE, timestamp_sequence('2024-01-05', 7_200_000_000), (x % 5)::INT FROM long_sequence(40)");
            execute("INSERT INTO d SELECT (x % 5)::INT, x, x::DOUBLE, timestamp_sequence('2024-01-05', 10_800_000_000), (x % 4)::INT FROM long_sequence(30)");
            final String[] queries = {
                    "SELECT * FROM t LEFT JOIN d ON t.k = d.k WHERE t.extra IS NULL OR t.extra > 2",
                    "SELECT * FROM t LEFT JOIN (SELECT * FROM d WHERE dextra IS NULL OR dextra > 1) d2 ON t.k = d2.k",
                    "SELECT * FROM t JOIN (SELECT * FROM d WHERE dextra > 1) d2 ON t.k = d2.k WHERE t.extra > 2",
                    // The filters read the columns that the conversion below changes.
                    "SELECT * FROM t LEFT JOIN (SELECT * FROM d WHERE note IS NOT NULL) d2 ON t.k = d2.k WHERE t.note IS NOT NULL",
            };
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_ROWS_PER_PARTITION, 4);
            for (int pass = 0; pass < 2; pass++) {
                if (pass == 1) {
                    // A Parquet column converted from a fixed to a variable size resolves per row through
                    // the record, which a compiled filter cannot address either. The active partition
                    // never converts, so native frames stay in the mix.
                    execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-05'");
                    execute("ALTER TABLE d CONVERT PARTITION TO PARQUET WHERE dts < '2024-01-05'");
                    execute("ALTER TABLE t ALTER COLUMN note TYPE VARCHAR");
                    execute("ALTER TABLE d ALTER COLUMN note TYPE VARCHAR");
                }
                for (long minRows : new long[]{Long.MAX_VALUE, 0}) {
                    setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, minRows);
                    for (String sql : queries) {
                        final String plan = assertParallelHashJoin(sql, sqlExecutionContext);
                        // The filters stay compiled; the frames that cannot run them interpret them.
                        Assert.assertEquals(plan, JitUtil.isJitSupported(), plan.contains("buildJitFilter: ") || plan.contains("Async JIT Hash Join"));
                    }
                }
            }
            // The probe and the build read Parquet frames and native ones.
            assertQuery("SELECT count() c, sum(isParquet::INT) p FROM table_partitions('t')")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            c\tp
                            8\t4
                            """);
            assertQuery("SELECT count() c, sum(isParquet::INT) p FROM table_partitions('d')")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            c\tp
                            8\t4
                            """);
        });
    }

    @Test
    public void testCompileUnderMemoryLimitLeaksNothing() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // Both inputs give up a compiled filter, and the build's regex has worker copies. A
            // native memory limit just above what the process holds fails the compile at each of its
            // allocations in turn, the stolen filters' contexts and the factory's included; every
            // failure releases what the compile took, as assertMemoryLeak checks, and the first
            // limit that admits the compile plans the operator.
            final String sql = "SELECT t.k, t.v, d2.w FROM t LEFT JOIN (SELECT * FROM d WHERE w > 3 AND vch ~ 'v1') d2 ON t.k = d2.k WHERE t.v > 2";
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
                    Assert.assertNotNull(find(factory));
                    Assert.assertEquals(expected, result(factory, sqlExecutionContext));
                } finally {
                    factory.close();
                }
                break;
            }
            Assert.assertTrue(failures > 0);
            assertParallelHashJoin(sql, sqlExecutionContext);
        });
    }

    @Test
    public void testCompiledProbeFilterRunsOnFramesThatAllowIt() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // Five daily partitions written before the column was added, whose frames have a column
            // top, and five after.
            execute("CREATE TABLE tc (k INT, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO tc SELECT (x % 7)::INT, x, timestamp_sequence('2024-01-01', 14_400_000_000) FROM long_sequence(30)");
            execute("ALTER TABLE tc ADD COLUMN extra INT");
            execute("INSERT INTO tc SELECT (x % 7)::INT, x, timestamp_sequence('2024-01-06', 14_400_000_000), (x % 3)::INT FROM long_sequence(30)");
            // Five daily partitions, the first three converted to Parquet with a column that then
            // changes from a fixed to a variable size, which the record converts per row.
            execute("CREATE TABLE tq (k INT, v LONG, note DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO tq SELECT (x % 7)::INT, x, x::DOUBLE, timestamp_sequence('2024-01-01', 14_400_000_000) FROM long_sequence(30)");
            execute("ALTER TABLE tq CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-04'");
            execute("ALTER TABLE tq ALTER COLUMN note TYPE VARCHAR");
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            Assume.assumeTrue(JitUtil.isJitSupported());
            // The compiled filter runs on the frames that have neither, once per frame and execution,
            // and the interpreted one on the rest; the rows are those of the ordinary plan either way.
            for (String table : new String[]{"tc", "tq"}) {
                final ObjList<CountingCompiledFilter> compiledFilters = new ObjList<>();
                final String expected = rows(ordinary("SELECT * FROM " + table + " p LEFT JOIN d ON p.k = d.k WHERE p.v > 3", sqlExecutionContext));
                try (
                        RecordCursorFactory probe = select("SELECT * FROM " + table + " WHERE v > 3");
                        RecordCursorFactory build = select("SELECT * FROM d");
                        AsyncHashJoinRecordCursorFactory factory = newLeftJoin(probe, build, compiledFilters)
                ) {
                    TestUtils.assertContains(plan(factory, sqlExecutionContext), "Async JIT Hash Join");
                    Assert.assertEquals(1, compiledFilters.size());
                    Assert.assertEquals(expected, rows(result(factory, sqlExecutionContext)));
                    Assert.assertEquals(table.equals("tc") ? 5 : 2, compiledFilters.getQuick(0).calls.get());
                    compiledFilters.getQuick(0).calls.set(0);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        //noinspection StatementWithEmptyBody
                        while (cursor.hasNext()) {
                        }
                        Assert.assertEquals(table.equals("tc") ? 10 : 5, factory.getProbeFrameCount());
                    }
                    Assert.assertEquals(table.equals("tc") ? 5 : 2, compiledFilters.getQuick(0).calls.get());
                }
            }
        });
    }

    @Test
    public void testConstructionFailureReleasesStolenFilters() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // Both inputs give their filters up, compiled and interpreted, with worker copies of the
            // regex; the constructor then fails, and the compile releases them with the inputs.
            final String sql = "SELECT t.k, t.v, d2.w FROM t LEFT JOIN (SELECT * FROM d WHERE w > 3 AND vch ~ 'v1') d2 ON t.k = d2.k WHERE t.v > 2";
            final String regexSql = "SELECT t.k, t.v, d2.w FROM t LEFT JOIN (SELECT * FROM d WHERE w > 3) d2 ON t.k = d2.k WHERE t.str ~ 'str[12]'";
            for (String query : new String[]{sql, regexSql}) {
                AsyncHashJoinRecordCursorFactory.setConstructorFailureForTesting(CairoException.nonCritical().put("injected construction failure"));
                try {
                    assertExceptionNoLeakCheck(query, 0, "injected construction failure");
                } finally {
                    AsyncHashJoinRecordCursorFactory.setConstructorFailureForTesting(null);
                }
                assertParallelHashJoin(query, sqlExecutionContext);
            }
        });
    }

    @Test
    public void testFilterThatRejectsEveryRow() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            for (int jitMode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                sqlExecutionContext.setJitMode(jitMode);
                // A probe filter that keeps no row leaves every frame without a tuple.
                for (String join : JOINS) {
                    final String sql = "SELECT t.k, d.w FROM t" + join + "d ON t.k = d.k WHERE t.v < 0";
                    assertParallelHashJoin(sql, sqlExecutionContext);
                    try (RecordCursorFactory factory = select(sql)) {
                        final AsyncHashJoinRecordCursorFactory join0 = find(factory);
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            Assert.assertFalse(cursor.hasNext());
                            // The probe's frames were dispatched, and none yielded a row.
                            Assert.assertEquals(10, join0.getProbeFrameCount());
                        }
                    }
                }
                // A build filter that keeps no row: the INNER join returns no row and dispatches no
                // probe task, the LEFT join reads every probe row with NULL build columns.
                final String inner = "SELECT t.k, d2.w FROM t JOIN (SELECT * FROM d WHERE w < 0) d2 ON t.k = d2.k";
                assertParallelHashJoin(inner, sqlExecutionContext);
                try (RecordCursorFactory factory = select(inner)) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.assertFalse(cursor.hasNext());
                        Assert.assertEquals(0, join.getProbeFrameCount());
                        Assert.assertEquals(0, join.getAtom().getStage(0).getFrozenBuild().getRowCount());
                    }
                }
                final String left = "SELECT t.k, d2.w, d2.s, d2.vch FROM t LEFT JOIN (SELECT * FROM d WHERE w < 0) d2 ON t.k = d2.k";
                assertParallelHashJoin(left, sqlExecutionContext);
                assertQuery("SELECT count() c, count(w) w FROM (" + left + ")")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                c\tw
                                60\t0
                                """);
            }
        });
    }

    @Test
    public void testFiltersOnBothInputsUnderProjections() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String[] queries = {
                    // Projections above both filters name the scans' columns in another order.
                    "SELECT t2.*, d2.* FROM (SELECT v, k, ts, sym FROM t WHERE v > 5) t2 LEFT JOIN (SELECT w, s, k FROM d WHERE w > 3) d2 ON t2.k = d2.k",
                    "SELECT t2.*, d2.* FROM (SELECT v, k, ts, sym FROM t WHERE v > 5) t2 JOIN (SELECT w, s, k FROM d WHERE w > 3) d2 ON t2.k = d2.k",
                    // A build projection that names its key column twice, whose keys read it twice.
                    "SELECT t.v, d2.* FROM t LEFT JOIN (SELECT k k1, k k2, w, s FROM d WHERE w > 3) d2 ON t.k = d2.k1 AND t.k = d2.k2 WHERE t.v < 40",
                    // A SYMBOL key that the probe translates into the build's dictionary, and SYMBOL
                    // payloads that read their own dictionaries.
                    "SELECT t.sym, t.v, d2.s, d2.w FROM t LEFT JOIN (SELECT s, w FROM d WHERE w % 2 = 0) d2 ON t.sym = d2.s WHERE t.v % 3 <> 0",
                    "SELECT t.sym, t.v, d2.s, d2.w FROM t JOIN (SELECT s, w FROM d WHERE w % 2 = 0) d2 ON t.sym = d2.s WHERE t.v % 3 <> 0",
                    // Composite keys over filtered inputs.
                    "SELECT t.k, t.sym, d.w, d.vch FROM t LEFT JOIN d ON t.k = d.k AND t.sym = d.s WHERE t.v > 10 AND t.str IS NOT NULL",
            };
            for (int jitMode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                sqlExecutionContext.setJitMode(jitMode);
                for (int frameRows : new int[]{1, 4, 1_000_000}) {
                    sqlExecutionContext.changePageFrameSizes(frameRows, frameRows);
                    for (String sql : queries) {
                        assertParallelHashJoin(sql, sqlExecutionContext);
                    }
                }
            }
            TestUtils.assertContains(planText(queries[3]), "symbolKeyJoin: true");
        });
    }

    @Test
    public void testIndexedSymbolPatternFilter() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE TABLE ix (s SYMBOL INDEX, k INT, w LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO ix SELECT ('p' || (x % 30))::SYMBOL, (x % 5)::INT, x, timestamp_sequence('2023-12-31', 60_000_000) FROM long_sequence(300)");
            // A pattern over an indexed SYMBOL column reads the rows through the index or through a
            // scan, whichever each execution picks, in the order of the table's rows either way, so the
            // join takes its filter, which it runs over the scan's frames.
            for (String sql : new String[]{
                    "SELECT t.k, t.v, i.w, i.s FROM t LEFT JOIN (SELECT * FROM ix WHERE s LIKE 'p1%') i ON t.k = i.k",
                    "SELECT i.s, i.w, d.w FROM ix i LEFT JOIN d ON i.k = d.k WHERE i.s LIKE 'p2%'",
            }) {
                TestUtils.assertContains(ordinaryPlan(sql), "AdaptiveSymbolPattern");
                TestUtils.assertContains(assertParallelHashJoin(sql, sqlExecutionContext), "s like p");
            }
            // Under a count, whose order does not matter, the index route reads the rows key by key,
            // out of the scan's order, so the join keeps the ordinary plan.
            final String count = "SELECT count(*) c FROM (SELECT * FROM ix WHERE s LIKE 'p1%') i LEFT JOIN t ON t.k = i.k";
            sqlExecutionContext.setParallelHashJoinGroupByEnabled(false);
            TestUtils.assertContains(ordinaryPlan(count), "Cursor-order scan");
            assertOrdinaryPlan(count, sqlExecutionContext);
        });
    }

    @Test
    public void testInnerJoinWithFilteredInputTakesOperator() throws Exception {
        assertMemoryLeak(() -> {
            // The left input is the smaller one, so an unfiltered INNER join's ordinary plan swaps its
            // inputs and keeps the ordinary plan, but a filtered input's cursor reports no size, so the
            // ordinary plan streams the left input and the operator takes the join.
            execute("CREATE TABLE small AS (SELECT (x % 4)::INT k, x v, timestamp_sequence('2024-01-01', 600_000_000) ts FROM long_sequence(10)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("CREATE TABLE big AS (SELECT (x % 6)::INT k, x w, timestamp_sequence('2023-12-31', 600_000_000) bts FROM long_sequence(50)) TIMESTAMP(bts) PARTITION BY HOUR");
            assertOrdinaryPlan("SELECT small.v, big.w FROM small JOIN big ON small.k = big.k", sqlExecutionContext);
            for (String sql : new String[]{
                    "SELECT small.v, big.w FROM small JOIN big ON small.k = big.k WHERE small.v > 3",
                    "SELECT small.v, b.w FROM small JOIN (SELECT * FROM big WHERE w > 3) b ON small.k = b.k",
                    "SELECT small.v, b.w FROM small JOIN (SELECT * FROM big WHERE w > 3) b ON small.k = b.k WHERE small.v > 3",
                    // An interval scan reports no size either, filtered or not.
                    "SELECT small.v, b.w FROM small JOIN (SELECT * FROM big WHERE bts IN '2023-12-31T02') b ON small.k = b.k",
            }) {
                TestUtils.assertContains(assertParallelHashJoin(sql, sqlExecutionContext), "joinType: inner");
            }
        });
    }

    @Test
    public void testPlanInsideScalarSubQuery() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // The join's plan prints inside an aggregate of the outer GROUP BY, which prints its
            // functions over its base metadata: the join prints its filters against its own scans and
            // gives the plan sink back its metadata and mode, so the text that follows the sub-query's
            // plan reads as in the ordinary plan. The sub-query's own GROUP BY runs over the join.
            sqlExecutionContext.setParallelHashJoinGroupByEnabled(false);
            final String sql = "SELECT t.k, sum(CASE WHEN t.v > (SELECT max(d2.w) FROM t t2 LEFT JOIN (SELECT * FROM d WHERE w > 3) d2 ON t2.k = d2.k WHERE t2.v > 5) THEN t.v ELSE 0 END) s FROM t ORDER BY 1";
            final String plan = planText(sql);
            TestUtils.assertContains(plan, "Hash Join workers");
            TestUtils.assertContains(plan, "filter: 5<v");
            TestUtils.assertContains(plan, "Filter: 3<w");
            final String ordinaryPlan = ordinaryPlan(sql);
            final String tail = ordinaryPlan.substring(ordinaryPlan.lastIndexOf("Frame forward scan on: d") + "Frame forward scan on: d".length());
            Assert.assertTrue(ordinaryPlan, tail.startsWith(",v,0]))]"));
            Assert.assertTrue(plan, plan.endsWith(tail));
        });
    }

    @Test
    public void testPlans() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String jitJoin = JitUtil.isJitSupported() ? "Async JIT Hash Join" : "Async Hash Join";
            final String jitBuildFilter = JitUtil.isJitSupported() ? "buildJitFilter" : "buildFilter";
            final String sql = "SELECT t.k, d2.w FROM t LEFT JOIN (SELECT * FROM d WHERE w > 3) d2 ON t.k = d2.k WHERE t.v > 2";
            assertQuery(sql)
                    .noLeakCheck()
                    .assertsPlan("SelectedRecord\n" +
                            "    " + jitJoin + " workers: 1\n" +
                            "      stages: 1\n" +
                            "      filter: 2<v\n" +
                            "        Probe\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: t\n" +
                            "        Build\n" +
                            "          joinType: left outer\n" +
                            "          condition: d2.k=t.k\n" +
                            "          buildPayload: copied when the probe is large enough\n" +
                            "          " + jitBuildFilter + ": 3<w\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: d\n");
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            // An INNER join over filtered inputs, a filter that the JIT cannot compile, and a SYMBOL key.
            assertQuery("SELECT t.k, d2.w FROM t JOIN (SELECT * FROM d WHERE s = 's1') d2 ON t.sym = d2.s WHERE t.str ~ 'str1'")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Async Hash Join workers: 1
                                  stages: 1
                                  filter: str ~ str1
                                    Probe
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t
                                    Build
                                      joinType: inner
                                      condition: d2.s=t.sym
                                      symbolKeyJoin: true
                                      buildPayload: copied when the probe is large enough
                                      buildFilter: s='s1'
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: d
                            """);
        });
    }

    @Test
    public void testProbeFilter() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            for (int jitMode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                sqlExecutionContext.setJitMode(jitMode);
                for (int frameRows : new int[]{1, 3, 1_000_000}) {
                    sqlExecutionContext.changePageFrameSizes(frameRows, frameRows);
                    for (String filter : PROBE_FILTERS) {
                        for (String join : JOINS) {
                            final String sql = "SELECT t.k, t.v, t.sym, t.str, d.w, d.s FROM t" + join + "d ON t.k = d.k WHERE " + filter;
                            final String plan = assertParallelHashJoin(sql, sqlExecutionContext);
                            // The stolen filter runs as JIT code when the ordinary plan's filter does.
                            final boolean isCompiled = ordinaryPlan(sql).contains("Async JIT Filter");
                            Assert.assertEquals(plan, isCompiled, plan.contains("Async JIT Hash Join"));
                            TestUtils.assertContains(plan, "filter: ");
                        }
                    }
                }
                final boolean isJitExpected = jitMode == SqlJitMode.JIT_MODE_ENABLED && JitUtil.isJitSupported();
                Assert.assertEquals(isJitExpected, planText("SELECT t.k, d.w FROM t LEFT JOIN d ON t.k = d.k WHERE t.v > 20").contains("Async JIT Hash Join"));
            }
        });
    }

    @Test
    public void testProbeFilterWhoseCursorClosedThrows() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String expected = printRows("SELECT * FROM t LEFT JOIN d ON t.k = d.k WHERE t.v > 30", false);
            // The probe filter fails as the cursor closes. The cursor goes on releasing the rest, the
            // scans' readers and the builds included, reports the failure, and opens again.
            final RecordingFilter filter = new RecordingFilter(true);
            try (
                    RecordCursorFactory probeQuery = select("SELECT * FROM t");
                    RecordCursorFactory buildQuery = select("SELECT * FROM d");
                    AsyncHashJoinRecordCursorFactory factory = newLeftJoin(
                            newInput(probeQuery.getBaseFactory(), filter),
                            newInput(buildQuery, new ObjList<>())
                    )
            ) {
                TestUtils.assertContains(plan(factory, sqlExecutionContext), "filter: v>30");
                for (int execution = 1; execution < 3; execution++) {
                    final StringSink sink = new StringSink();
                    final RecordCursor cursor = factory.getCursor(sqlExecutionContext);
                    while (cursor.hasNext()) {
                        CursorPrinter.println(cursor.getRecord(), factory.getMetadata(), sink, true);
                    }
                    try {
                        cursor.close();
                        Assert.fail("expected the probe filter to fail as the cursor closes");
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "injected cursorClosed failure");
                    }
                    Assert.assertEquals(expected, sink.toString());
                    Assert.assertEquals(execution, filter.closedCursors);
                    Assert.assertNull(factory.getAtom().getStage(0).getFrozenBuild());
                    Assert.assertEquals(0, engine.getBusyReaderCount());
                }
            }
        });
    }

    @Test
    public void testRejectedFilters() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // A filter whose value may change within an execution: the join cannot run it per frame
            // as its own cursor does.
            assertOrdinaryPlan("SELECT t.k, d2.w FROM t LEFT JOIN (SELECT * FROM d WHERE w > 2 AND systimestamp() > dts) d2 ON t.k = d2.k", sqlExecutionContext);
            assertOrdinaryPlan("SELECT t2.k, d.w FROM (SELECT * FROM t WHERE v > 2 AND systimestamp() > ts) t2 LEFT JOIN d ON t2.k = d.k", sqlExecutionContext);
            // A serial filter, which runs where parallel filters are off and keeps no expression to
            // compile worker copies from.
            sqlExecutionContext.setParallelFilterEnabled(false);
            TestUtils.assertContains(ordinaryPlan("SELECT t.k, d.w FROM t LEFT JOIN d ON t.k = d.k WHERE t.v > 2"), "Filter filter: 2<v");
            assertOrdinaryPlan("SELECT t.k, d.w FROM t LEFT JOIN d ON t.k = d.k WHERE t.v > 2", sqlExecutionContext);
            assertOrdinaryPlan("SELECT t.k, d2.w FROM t LEFT JOIN (SELECT * FROM d WHERE w > 2) d2 ON t.k = d2.k", sqlExecutionContext);
            sqlExecutionContext.setParallelFilterEnabled(true);
            // A filter that applies a LIMIT: the build's own, and the probe's, which the optimiser
            // gives the query's LIMIT.
            assertOrdinaryPlan("SELECT t.k, d2.w FROM t LEFT JOIN (SELECT * FROM d WHERE w > 2 LIMIT 3) d2 ON t.k = d2.k", sqlExecutionContext);
            TestUtils.assertContains(ordinaryPlan("SELECT t.k, d.w FROM t LEFT JOIN d ON t.k = d.k WHERE t.v > 20 LIMIT 4"), "limit: 4");
            assertOrdinaryPlan("SELECT t.k, d.w FROM t LEFT JOIN d ON t.k = d.k WHERE t.v > 20 LIMIT 4", sqlExecutionContext);
        });
    }

    @Test
    public void testSharedStepperTemplateWithProbeFilter() throws Exception {
        assertMemoryLeak(() -> {
            // A JVM that cannot define hidden classes hands every stage the shared stepper template,
            // whose loop over a frame's kept rows runs forward and backward here.
            AsyncHashJoinRecordCursorFactory.setStepperCopiesEnabledForTesting(false);
            try {
                createTables();
                sqlExecutionContext.changePageFrameSizes(3, 3);
                for (String sql : new String[]{
                        "SELECT t.k, t.v, d.w FROM t LEFT JOIN d ON t.k = d.k WHERE t.v > 20",
                        "SELECT t.k, t.v, d.w FROM t JOIN d ON t.k = d.k WHERE t.v > 20 ORDER BY t.ts DESC",
                        "SELECT t.k, t.v, d.w FROM t LEFT JOIN d ON t.k = d.k WHERE t.str ~ 'str[13]' ORDER BY t.ts DESC",
                }) {
                    assertParallelHashJoin(sql, sqlExecutionContext);
                    try (RecordCursorFactory factory = select(sql)) {
                        Assert.assertFalse(find(factory).getStepperClass(0).isHidden());
                    }
                }
            } finally {
                AsyncHashJoinRecordCursorFactory.setStepperCopiesEnabledForTesting(true);
            }
        });
    }

    @Test
    public void testThrowingFilters() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // p * 1000 does not fit DECIMAL(4,0) for the one row with p = 10, so a filter that casts it
            // fails there, in a frame between frames that pass.
            execute("CREATE TABLE tp AS (SELECT (x % 7)::INT k, CASE WHEN x = 47 THEN 10 ELSE x % 10 END p, timestamp_sequence('2024-01-01', 600_000_000) ts FROM long_sequence(60)) TIMESTAMP(ts) PARTITION BY HOUR");
            final String poison = "(p * 1000)::DECIMAL(4,0) > 0::DECIMAL(4,0)";
            final String[] queries = {
                    "SELECT tp.k, tp.p, d.w FROM tp LEFT JOIN d ON tp.k = d.k WHERE " + poison,
                    "SELECT tp.k, tp.p, d.w FROM tp JOIN d ON tp.k = d.k WHERE " + poison,
                    "SELECT t.k, b.p FROM t LEFT JOIN (SELECT * FROM tp WHERE " + poison + ") b ON t.k = b.k",
                    "SELECT t.k, b.p FROM t JOIN (SELECT * FROM tp WHERE " + poison + ") b ON t.k = b.k",
            };
            sqlExecutionContext.changePageFrameSizes(3, 3);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_ROWS_PER_PARTITION, 4);
            for (long minRows : new long[]{Long.MAX_VALUE, 0}) {
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, minRows);
                for (String sql : queries) {
                    final String expected = error(sql, false);
                    TestUtils.assertContains(expected, "inconvertible value: 10000 [LONG -> DECIMAL(4,0)]");
                    Assert.assertEquals(sql, expected, error(sql, true));
                }
            }
            // The tables still work, and so does the join with a filter that passes the row.
            final String safe = "(p * 100)::DECIMAL(4,0) > 0::DECIMAL(4,0)";
            assertParallelHashJoin("SELECT tp.k, tp.p, d.w FROM tp LEFT JOIN d ON tp.k = d.k WHERE " + safe, sqlExecutionContext);
            assertParallelHashJoin("SELECT t.k, b.p FROM t LEFT JOIN (SELECT * FROM tp WHERE " + safe + ") b ON t.k = b.k", sqlExecutionContext);
        });
    }

    @Test
    public void testToTopMidStream() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            sqlExecutionContext.changePageFrameSizes(2, 3);
            for (String sql : new String[]{
                    "SELECT t.k, t.v, d2.w, d2.s FROM t LEFT JOIN (SELECT * FROM d WHERE w > 10) d2 ON t.k = d2.k WHERE t.v > 7",
                    "SELECT t.k, t.v, d2.w, d2.s FROM t JOIN (SELECT * FROM d WHERE w > 10) d2 ON t.k = d2.k WHERE t.str ~ 'str[13]'",
            }) {
                final String expected = printRows(sql, false);
                try (RecordCursorFactory factory = select(sql)) {
                    Assert.assertNotNull(find(factory));
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        // toTop() before the first read, after a few rows, inside a chain of matches
                        // and after the last row each read the whole result again.
                        for (int stop : new int[]{0, 1, 5, 9, Integer.MAX_VALUE}) {
                            final String rows = printRows(cursor, factory.getMetadata(), stop);
                            Assert.assertTrue(sql + '\n' + rows, expected.startsWith(rows));
                            cursor.toTop();
                        }
                        Assert.assertEquals(expected, printRows(cursor, factory.getMetadata(), Integer.MAX_VALUE));
                    }
                }
            }
            // The owner's probe filter goes back to the top with the cursor, as the filter the join
            // stole it from does, and closes with it.
            final String expected = printRows("SELECT * FROM t LEFT JOIN d ON t.k = d.k WHERE t.v > 30", false);
            final RecordingFilter filter = new RecordingFilter(false);
            try (
                    RecordCursorFactory probeQuery = select("SELECT * FROM t");
                    RecordCursorFactory buildQuery = select("SELECT * FROM d");
                    AsyncHashJoinRecordCursorFactory factory = newLeftJoin(
                            newInput(probeQuery.getBaseFactory(), filter),
                            newInput(buildQuery, new ObjList<>())
                    )
            ) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(expected.startsWith(printRows(cursor, factory.getMetadata(), 3)));
                    Assert.assertEquals(0, filter.toTops);
                    cursor.toTop();
                    Assert.assertEquals(1, filter.toTops);
                    Assert.assertEquals(expected, printRows(cursor, factory.getMetadata(), Integer.MAX_VALUE));
                    Assert.assertEquals(0, filter.closedCursors);
                }
                Assert.assertEquals(1, filter.closedCursors);
            }
        });
    }

    private static void assertBuiltInRounds(String sql, boolean isBuiltInRounds) throws SqlException {
        try (RecordCursorFactory factory = select(sql)) {
            final HashJoinBuildStage stage = find(factory).getAtom().getStage(0);
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                cursor.hasNext();
                Assert.assertEquals(sql, isBuiltInRounds, stage.isBuiltInRounds());
            }
        }
    }

    private static void bind(long probeMin, long buildMin, String symbol) throws SqlException {
        bindVariableService.clear();
        bindVariableService.setLong(0, probeMin);
        bindVariableService.setLong(1, buildMin);
        bindVariableService.setStr(2, symbol);
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
        // d inserts its symbols in another order than t, so equal texts have different keys.
        execute("""
                CREATE TABLE d AS (
                    SELECT
                        CASE WHEN x % 6 = 0 THEN NULL ELSE (x % 5)::INT END k,
                        x w,
                        ('s' || (5 - x % 5))::SYMBOL s,
                        CASE WHEN x % 4 = 0 THEN NULL ELSE 'v' || x END::VARCHAR vch,
                        timestamp_sequence('2023-12-31', 600_000_000) dts
                    FROM long_sequence(40)
                ) TIMESTAMP(dts) PARTITION BY HOUR
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

    private static IntList identity(int columnCount) {
        final IntList columns = new IntList();
        for (int i = 0; i < columnCount; i++) {
            columns.add(i);
        }
        return columns;
    }

    // A LEFT join of two compiled queries on their first columns, planned by hand as the planner plans
    // its inputs: a query that filters its scan gives the join its filter, its JIT code, which the
    // join counts the calls of, and its bind variables, and the join reads the filter's base, the
    // query's scan. The QueryProgress factories then close the factories they wrap again, which is a
    // no-op for the scans, closes the filter's JIT code and bind variable memory again, which release
    // nothing twice, and closes the filter again, which, over columns and constants, holds nothing.
    private static AsyncHashJoinRecordCursorFactory newLeftJoin(
            RecordCursorFactory probeQuery,
            RecordCursorFactory buildQuery,
            ObjList<CountingCompiledFilter> compiledFilters
    ) throws SqlException {
        return newLeftJoin(newInput(probeQuery, compiledFilters), newInput(buildQuery, compiledFilters));
    }

    private static AsyncHashJoinRecordCursorFactory newLeftJoin(HashJoinInput probe, HashJoinInput build) throws SqlException {
        final RecordMetadata probeMetadata = probe.getMetadata();
        final RecordMetadata buildMetadata = build.getMetadata();
        final HashJoinGroupByKeys keys = new HashJoinGroupByKeys();
        Assert.assertTrue(keys.add(0, probeMetadata.getColumnType(0), 0, buildMetadata.getColumnType(0)));
        final HashJoinKeyLayout keyLayout = new HashJoinKeyLayout(configuration, new BytecodeAssembler(), keys,
                "t", probeMetadata, probe.getColumnMap(), "b", buildMetadata, build.getColumnMap());
        final JoinRecordMetadata metadata = new JoinRecordMetadata(configuration, probeMetadata.getColumnCount() + buildMetadata.getColumnCount());
        metadata.copyColumnMetadataFrom("t", probeMetadata);
        metadata.copyColumnMetadataFrom("b", buildMetadata);
        metadata.setTimestampIndex(probeMetadata.getTimestampIndex());
        final AsyncHashJoinSpec spec = new AsyncHashJoinSpec(
                probe,
                new HashJoinStageSpec(build, keyLayout, true, "b.k=t.k", null, null, null),
                metadata,
                probe.getFactory().followedOrderByAdvice(),
                probe.getFactory().getScanDirection()
        );
        return new AsyncHashJoinRecordCursorFactory(
                engine,
                configuration,
                engine.getMessageBus(),
                spec,
                () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                1
        );
    }

    // An input over a scan that runs the given filter, which it owns from here on.
    private static HashJoinInput newInput(RecordCursorFactory scan, Function filter) {
        return new HashJoinInput(scan, identity(scan.getMetadata().getColumnCount()),
                new AsyncFilterContext(configuration, null, null, null, filter, null, null, 1, 0, 0, 0));
    }

    private static HashJoinInput newInput(RecordCursorFactory query, ObjList<CountingCompiledFilter> compiledFilters) {
        final RecordCursorFactory factory = query.getBaseFactory();
        if (factory.supportsPageFrameCursor()) {
            return new HashJoinInput(factory, identity(factory.getMetadata().getColumnCount()),
                    new AsyncFilterContext(configuration, null, null, null, null, null, null, 1, 0, 0, 0));
        }
        Assert.assertTrue(factory.supportsFilterStealing());
        final RecordCursorFactory base = factory.getBaseFactory();
        final Function filter = factory.getFilter();
        Assert.assertTrue(filter.isThreadSafe());
        CompiledFilter compiledFilter = factory.getCompiledFilter();
        if (compiledFilter != null) {
            final CountingCompiledFilter counting = new CountingCompiledFilter(compiledFilter);
            compiledFilters.add(counting);
            compiledFilter = counting;
        }
        final MemoryCARW bindVarMemory = factory.getBindVarMemory();
        final ObjList<Function> bindVarFunctions = factory.getBindVarFunctions();
        factory.halfClose();
        return new HashJoinInput(base, identity(base.getMetadata().getColumnCount()),
                new AsyncFilterContext(configuration, compiledFilter, bindVarMemory, bindVarFunctions, filter, null, null, 1, 0, 0, 0));
    }

    // The query's rows, as CursorPrinter prints them, under the operator or under the ordinary plan.
    private static String printRows(String sql, boolean isParallel) throws SqlException {
        sqlExecutionContext.setParallelHashJoinEnabled(isParallel);
        try (RecordCursorFactory factory = select(sql); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            final StringSink sink = new StringSink();
            while (cursor.hasNext()) {
                CursorPrinter.println(cursor.getRecord(), factory.getMetadata(), sink, true);
            }
            return sink.toString();
        } finally {
            sqlExecutionContext.setParallelHashJoinEnabled(true);
        }
    }

    // Up to the given number of the cursor's next rows, as CursorPrinter prints them.
    private static String printRows(RecordCursor cursor, RecordMetadata metadata, int maxRows) {
        final StringSink sink = new StringSink();
        for (int rows = 0; rows < maxRows && cursor.hasNext(); rows++) {
            CursorPrinter.println(cursor.getRecord(), metadata, sink, true);
        }
        return sink.toString();
    }

    private static String ordinaryPlan(String sql) throws SqlException {
        sqlExecutionContext.setParallelHashJoinEnabled(false);
        try (RecordCursorFactory factory = select(sql)) {
            return plan(factory, sqlExecutionContext);
        } finally {
            sqlExecutionContext.setParallelHashJoinEnabled(true);
        }
    }

    private static String planText(String sql) throws SqlException {
        try (RecordCursorFactory factory = select(sql)) {
            return ParallelHashJoinTestUtils.plan(factory, sqlExecutionContext);
        }
    }

    // A result's rows, without its metadata lines.
    private static String rows(String result) {
        return result.substring(result.indexOf('\n', result.indexOf('\n') + 1) + 1);
    }

    // t.v > 30, which counts the times a cursor that runs it closes and goes back to the top, and which
    // may fail whenever such a cursor closes.
    private static final class RecordingFilter extends BooleanFunction {
        private final boolean isThrowingOnClose;
        int closedCursors;
        int toTops;

        RecordingFilter(boolean isThrowingOnClose) {
            this.isThrowingOnClose = isThrowingOnClose;
        }

        @Override
        public void cursorClosed() {
            closedCursors++;
            if (isThrowingOnClose) {
                throw CairoException.nonCritical().put("injected cursorClosed failure");
            }
        }

        @Override
        public boolean getBool(Record rec) {
            return rec.getLong(1) > 30;
        }

        @Override
        public boolean isThreadSafe() {
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("v>30");
        }

        @Override
        public void toTop() {
            toTops++;
        }
    }

    // Counts the frames that the compiled filter runs on, and runs the filter it wraps.
    private static final class CountingCompiledFilter extends CompiledFilter {
        final AtomicInteger calls = new AtomicInteger();
        private final CompiledFilter delegate;

        CountingCompiledFilter(CompiledFilter delegate) {
            this.delegate = delegate;
        }

        @Override
        public long call(long dataAddress, long dataSize, long auxAddress, long varsAddress, long varsSize, long rowsAddress, long rowCount) {
            calls.incrementAndGet();
            return delegate.call(dataAddress, dataSize, auxAddress, varsAddress, varsSize, rowsAddress, rowCount);
        }

        @Override
        public void close() {
            delegate.close();
        }
    }
}
