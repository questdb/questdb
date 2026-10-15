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
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.table.AsyncHashJoinGroupByAtom;
import io.questdb.griffin.engine.table.AsyncHashJoinGroupByRecordCursorFactory;
import io.questdb.griffin.engine.table.HashJoinGroupByBuildChoiceRecordCursorFactory;
import io.questdb.jit.JitUtil;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.SqlJitMode.JIT_MODE_DISABLED;
import static io.questdb.cairo.SqlJitMode.JIT_MODE_ENABLED;
import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.fused;
import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.plan;
import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.result;

/**
 * One fused factory serves every execution of a statement, whatever its bind values. Each test here
 * compiles a query once and executes it with values that change its result, on real worker pools
 * and the owner alone, under both build modes, and compares every execution with the ordinary plan
 * run with the same values. The bind variables sit where each execution must re-read them: in the
 * GROUP BY keys, which have per-worker copies, in an outer join's ON condition on the build, in the
 * INNER build's JIT filter, in a runtime-constant ON conjunct, and in a filter that is the build's in
 * one orientation of a build choice and the probe's in the other.
 */
public class HashJoinGroupByBindVariableTest extends AbstractCairoTest {
    private static final WorkerPoolMode[] POOL_MODES = {WorkerPoolMode.LEGACY, WorkerPoolMode.FIBER_HOST, null};
    private static final int WORKER_COUNT = 4;
    private boolean isParallelBuild;

    @Test
    public void testBuildChoiceWithRoleSwitchingBindFilters() throws Exception {
        // p.kwp > $3 filters the build when p builds, and the probe when r does; r.d > $4 the other
        // way round. Each orientation compiles both filters with bind memory of its own, and each
        // execution must refill the memory of the orientation that it runs.
        final String sql = """
                SELECT p.country, count() n, sum(r.d) d, sum(p.kwp) w
                FROM r JOIN p ON r.id = p.id
                WHERE r.ts >= $1 AND r.ts < $2 AND p.kwp > $3 AND r.d > $4
                ORDER BY country
                """;
        forEachMode((db, context, label) -> {
            for (int jitMode : new int[]{JIT_MODE_ENABLED, JIT_MODE_DISABLED}) {
                context.setJitMode(jitMode);
                bindChoice("2020-01-01T00", "2020-01-01T05", 10, 30);
                try (RecordCursorFactory factory = db.select(sql, context)) {
                    final HashJoinGroupByBuildChoiceRecordCursorFactory choice = choice(factory);
                    final boolean isCompiled = JitUtil.isJitSupported() && jitMode == JIT_MODE_ENABLED;
                    Assert.assertEquals(label, isCompiled, choice.getPrimary().hasCompiledBuildFilter());
                    Assert.assertEquals(label, isCompiled, choice.getAlternate().hasCompiledBuildFilter());
                    Assert.assertEquals(label, isCompiled, choice.getPrimary().usesCompiledFilter());
                    Assert.assertEquals(label, isCompiled, choice.getAlternate().usesCompiledFilter());
                    // 300 interval rows of r against p's 600 build r; a whole day of r, 1_440 rows,
                    // builds p. The filter values change on every execution.
                    assertChoiceExecution(db, sql, factory, choice, context, label, true);
                    bindChoice("2020-01-01T00", "2020-01-02T00", 40, 50);
                    assertChoiceExecution(db, sql, factory, choice, context, label, false);
                    bindChoice("2020-01-01T10", "2020-01-01T12", 60, 5);
                    assertChoiceExecution(db, sql, factory, choice, context, label, true);
                    bindChoice("2020-01-01T12", "2020-01-02T12", 5, 90);
                    assertChoiceExecution(db, sql, factory, choice, context, label, false);
                }
            }
        });
    }

    @Test
    public void testBuildOnFilterBindVariables() throws Exception {
        // An outer join's ON condition on the build alone drops build rows as the build walks its
        // frames, on the owner's filter or a worker's copy. The regex is not thread safe.
        final String[] queries = {
                """
                        SELECT r.g, count() n, count(p.id) m, sum(p.kwp) w
                        FROM r LEFT JOIN p ON r.id = p.id AND p.kwp > $1 AND p.name::STRING ~ $2
                        ORDER BY r.g
                        """,
                """
                        SELECT count() n, count(p.id) m, sum(p.kwp) w, sum(r.d) d
                        FROM p RIGHT JOIN r ON r.id = p.id AND p.kwp > $1 AND p.name::STRING ~ $2
                        """,
                // A composite key stages into the build's map.
                """
                        SELECT r.g, count() n, count(p.id) m, sum(p.kwp) w
                        FROM r LEFT JOIN p ON r.id = p.id AND r.g = p.g AND p.kwp > $1 AND p.name::STRING ~ $2
                        ORDER BY r.g
                        """
        };
        forEachMode((db, context, label) -> {
            for (String sql : queries) {
                bindOn(10, "n[0-3]");
                try (RecordCursorFactory factory = db.select(sql, context)) {
                    final String text = label + '\n' + sql + '\n' + plan(factory, context);
                    Assert.assertTrue(text, text.contains("Hash Join Group By"));
                    Assert.assertTrue(text, text.contains("buildOnFilter: "));
                    final String first = assertExecution(db, sql, factory, context, text);
                    bindOn(50, "n[0-9]");
                    final String second = assertExecution(db, sql, factory, context, text);
                    Assert.assertNotEquals(text, first, second);
                    // A threshold above every kwp leaves every probe row unmatched.
                    bindOn(1_000, "n");
                    assertExecution(db, sql, factory, context, text);
                    bindOn(10, "n[0-3]");
                    Assert.assertEquals(text, first, assertExecution(db, sql, factory, context, text));
                }
            }
        });
    }

    @Test
    public void testGroupByKeyBindVariables() throws Exception {
        // concat() is not thread safe, so each worker gets a copy of the key functions, and every
        // copy must read the values of the execution it runs in.
        final String[] queries = {
                """
                        SELECT r.id % $1 k, concat(p.country, $2) c, count() n, sum(r.d) d, sum(p.kwp) w
                        FROM r JOIN p ON r.id = p.id
                        ORDER BY k, c
                        """,
                """
                        SELECT r.id % $1 k, concat(p.country, $2) c, count() n, sum(r.d) d, sum(p.kwp) w
                        FROM r LEFT JOIN p ON r.id = p.id
                        ORDER BY k, c
                        """
        };
        forEachMode((db, context, label) -> {
            for (int threshold : new int[]{Integer.MAX_VALUE, 1}) {
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, threshold);
                final boolean isSharded = threshold == 1;
                for (String sql : queries) {
                    bindKeys(3, "-a");
                    try (RecordCursorFactory factory = db.select(sql, context)) {
                        final String text = label + " sharded=" + isSharded + '\n' + sql + '\n' + plan(factory, context);
                        Assert.assertTrue(text, text.contains("Hash Join Group By"));
                        final AsyncHashJoinGroupByAtom atom = fused(factory).getAtom();
                        for (int slot = 0; slot < WORKER_COUNT; slot++) {
                            Assert.assertNotSame(text, atom.getFunctions().getKeyFunctions(-1), atom.getFunctions().getKeyFunctions(slot));
                        }
                        final String first = assertKeyedExecution(db, sql, factory, context, text, isSharded);
                        bindKeys(7, "-b");
                        final String second = assertKeyedExecution(db, sql, factory, context, text, isSharded);
                        Assert.assertNotEquals(text, first, second);
                        bindKeys(1, "");
                        assertKeyedExecution(db, sql, factory, context, text, isSharded);
                        bindKeys(3, "-a");
                        Assert.assertEquals(text, first, assertKeyedExecution(db, sql, factory, context, text, isSharded));
                    }
                }
            }
        });
    }

    @Test
    public void testInnerBuildJitFilterBindVariables() throws Exception {
        // p, the smaller table, builds, and its scan's filter compiles to JIT code that reads $1 and
        // $2 from bind memory, which every execution must refill.
        final String[] queries = {
                "SELECT r.g, count() n, sum(p.kwp) w, sum(r.d) d FROM r JOIN p ON r.id = p.id WHERE p.kwp > $1 AND p.id <> $2 ORDER BY r.g",
                "SELECT count() n, sum(p.kwp) w, sum(r.d) d FROM r JOIN p ON r.id = p.id WHERE p.kwp > $1 AND p.id <> $2",
                // The lone count takes each key's pairs from the key's row count in the filtered build.
                "SELECT count() FROM r JOIN p ON r.id = p.id WHERE p.kwp > $1 AND p.id <> $2"
        };
        forEachMode((db, context, label) -> {
            for (int jitMode : new int[]{JIT_MODE_ENABLED, JIT_MODE_DISABLED}) {
                context.setJitMode(jitMode);
                final boolean isCompiled = JitUtil.isJitSupported() && jitMode == JIT_MODE_ENABLED;
                for (String sql : queries) {
                    bindBuildFilter(10, 7);
                    try (RecordCursorFactory factory = db.select(sql, context)) {
                        final String text = label + " jit=" + jitMode + '\n' + sql + '\n' + plan(factory, context);
                        Assert.assertTrue(text, text.contains("Hash Join Group By"));
                        Assert.assertFalse(text, text.contains("Build Choice"));
                        Assert.assertEquals(text, isCompiled, text.contains("buildJitFilter: "));
                        Assert.assertEquals(text, isCompiled, fused(factory).hasCompiledBuildFilter());
                        final String first = assertExecution(db, sql, factory, context, text);
                        bindBuildFilter(80, 450);
                        final String second = assertExecution(db, sql, factory, context, text);
                        Assert.assertNotEquals(text, first, second);
                        // No row passes, so the build is empty.
                        bindBuildFilter(1_000, 0);
                        assertExecution(db, sql, factory, context, text);
                        bindBuildFilter(10, 7);
                        Assert.assertEquals(text, first, assertExecution(db, sql, factory, context, text));
                    }
                }
            }
        });
    }

    @Test
    public void testRuntimeConstantOnConjunct() throws Exception {
        // $1 = 1 reads no column, so the analysis takes an outer join's conjunct as a condition on
        // the build. False, it drops every build row, and the join keeps each probe row unmatched.
        // An INNER join's conjunct becomes a filter above the join, a gate that the ordinary plan
        // evaluates once per execution, so the analysis keeps the ordinary plan for it.
        final String[] fusedQueries = {
                "SELECT r.g, count() n, count(p.id) m, sum(p.kwp) w FROM r LEFT JOIN p ON r.id = p.id AND $1 = 1 ORDER BY r.g",
                "SELECT count() n, count(p.id) m, sum(p.kwp) w FROM p RIGHT JOIN r ON r.id = p.id AND $1 = 1",
                "SELECT count() n, count(p.id) m, sum(p.kwp) w FROM r LEFT JOIN p ON r.id = p.id AND p.kwp > $2 AND $1 = 1"
        };
        final String[] ordinaryQueries = {
                "SELECT count() n, sum(p.kwp) w FROM r JOIN p ON r.id = p.id AND $1 = 1",
                "SELECT count() n, sum(p.kwp) w FROM r JOIN p ON r.id = p.id AND p.kwp > $2 AND $1 = 1"
        };
        forEachMode((db, context, label) -> {
            for (String sql : fusedQueries) {
                bindGate(1, 20);
                try (RecordCursorFactory factory = db.select(sql, context)) {
                    final String text = label + '\n' + sql + '\n' + plan(factory, context);
                    Assert.assertTrue(text, text.contains("Hash Join Group By"));
                    Assert.assertTrue(text, text.contains("buildOnFilter: "));
                    final String open = assertExecution(db, sql, factory, context, text);
                    bindGate(2, 20);
                    final String closed = assertExecution(db, sql, factory, context, text);
                    Assert.assertNotEquals(text, open, closed);
                    bindGate(1, 60);
                    assertExecution(db, sql, factory, context, text);
                    bindGate(1, 20);
                    Assert.assertEquals(text, open, assertExecution(db, sql, factory, context, text));
                }
            }
            for (String sql : ordinaryQueries) {
                bindGate(1, 20);
                try (RecordCursorFactory factory = db.select(sql, context)) {
                    final String text = label + '\n' + sql + '\n' + plan(factory, context);
                    Assert.assertFalse(text, text.contains("Hash Join Group By"));
                }
            }
        });
    }

    private static HashJoinGroupByBuildChoiceRecordCursorFactory choice(RecordCursorFactory factory) {
        // QueryProgress and projections wrap the fused plan.
        for (RecordCursorFactory current = factory; current != null; current = current.getBaseFactory()) {
            if (current instanceof HashJoinGroupByBuildChoiceRecordCursorFactory choice) {
                return choice;
            }
        }
        Assert.fail("expected a build choice");
        return null;
    }

    // r holds 3_000 rows, one a minute from 2020-01-01, with 400 ids; p holds 600 rows with 500 ids,
    // so that some r ids find two p rows, some one and some none.
    private static void createTables(CairoEngine db, SqlExecutionContext context) throws Exception {
        db.execute("CREATE TABLE r (id INT, g INT, d DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR", context);
        db.execute("CREATE TABLE p (id INT, g INT, country SYMBOL, name SYMBOL, kwp DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY", context);
        db.execute("""
                INSERT INTO r
                SELECT (x % 400)::INT, (x % 5)::INT, (x % 200) * 0.5, timestamp_sequence('2020-01-01', 60_000_000L)
                FROM long_sequence(3_000)
                """, context);
        db.execute("""
                INSERT INTO p
                SELECT (x % 500)::INT, (x % 5)::INT, CASE WHEN x % 9 = 0 THEN NULL ELSE 'C' || (x % 4) END,
                    'n' || (x % 10), (x % 100)::DOUBLE, timestamp_sequence('2020-01-01', 300_000_000L)
                FROM long_sequence(600)
                """, context);
    }

    private static String ordinary(CairoEngine db, String sql, SqlExecutionContextImpl context) throws Exception {
        context.setParallelHashJoinGroupByEnabled(false);
        try (RecordCursorFactory baseline = db.select(sql, context)) {
            Assert.assertFalse(sql, plan(baseline, context).contains("Hash Join Group By"));
            return result(baseline, context);
        } finally {
            context.setParallelHashJoinGroupByEnabled(true);
        }
    }

    private static String rows(RecordCursor cursor, RecordCursorFactory factory) {
        final StringSink sink = new StringSink();
        CursorPrinter.println(cursor, factory.getMetadata(), sink, true, true);
        return sink.toString();
    }

    private void assertChoiceExecution(
            CairoEngine db,
            String sql,
            RecordCursorFactory factory,
            HashJoinGroupByBuildChoiceRecordCursorFactory choice,
            SqlExecutionContextImpl context,
            String label,
            boolean isAlternate
    ) throws Exception {
        final String expected = ordinary(db, sql, context);
        try (RecordCursor cursor = factory.getCursor(context)) {
            Assert.assertEquals(label, isAlternate, choice.isAlternateChosen());
            Assert.assertEquals(label, expected, rows(cursor, factory));
            final AsyncHashJoinGroupByRecordCursorFactory chosen = isAlternate ? choice.getAlternate() : choice.getPrimary();
            Assert.assertNotNull(label, chosen.getAtom().getFrozenBuild());
            Assert.assertEquals(label, isParallelBuild, chosen.getAtom().isBuiltInRounds());
        }
        Assert.assertNull(label, context.getMemoryTracker());
    }

    // Returns the rows, so that a test can tell executions with different values apart.
    private String assertExecution(
            CairoEngine db,
            String sql,
            RecordCursorFactory factory,
            SqlExecutionContextImpl context,
            String label
    ) throws Exception {
        final String expected = ordinary(db, sql, context);
        try (RecordCursor cursor = factory.getCursor(context)) {
            Assert.assertEquals(label, expected, rows(cursor, factory));
            Assert.assertEquals(label, isParallelBuild, fused(factory).getAtom().isBuiltInRounds());
        }
        Assert.assertNull(label, context.getMemoryTracker());
        return expected;
    }

    private String assertKeyedExecution(
            CairoEngine db,
            String sql,
            RecordCursorFactory factory,
            SqlExecutionContextImpl context,
            String label,
            boolean isSharded
    ) throws Exception {
        final String expected = ordinary(db, sql, context);
        try (RecordCursor cursor = factory.getCursor(context)) {
            Assert.assertEquals(label, expected, rows(cursor, factory));
            Assert.assertEquals(label, isSharded, fused(factory).getAtom().isSharded());
            Assert.assertEquals(label, isParallelBuild, fused(factory).getAtom().isBuiltInRounds());
            cursor.toTop();
            Assert.assertEquals(label, expected, rows(cursor, factory));
        }
        Assert.assertNull(label, context.getMemoryTracker());
        return expected;
    }

    private void bindBuildFilter(double kwp, int id) throws Exception {
        bindVariableService.clear();
        bindVariableService.setDouble(0, kwp);
        bindVariableService.setInt(1, id);
    }

    private void bindChoice(String lo, String hi, double kwp, double d) throws Exception {
        bindVariableService.clear();
        bindVariableService.setTimestamp(0, MicrosFormatUtils.parseTimestamp(lo + ":00:00.000000Z"));
        bindVariableService.setTimestamp(1, MicrosFormatUtils.parseTimestamp(hi + ":00:00.000000Z"));
        bindVariableService.setDouble(2, kwp);
        bindVariableService.setDouble(3, d);
    }

    private void bindGate(int gate, double kwp) throws Exception {
        bindVariableService.clear();
        bindVariableService.setInt(0, gate);
        bindVariableService.setDouble(1, kwp);
    }

    private void bindKeys(int modulo, String suffix) throws Exception {
        bindVariableService.clear();
        bindVariableService.setInt(0, modulo);
        bindVariableService.setStr(1, suffix);
    }

    private void bindOn(double kwp, String pattern) throws Exception {
        bindVariableService.clear();
        bindVariableService.setDouble(0, kwp);
        bindVariableService.setStr(1, pattern);
    }

    /**
     * Runs the check on an engine of its own per pool mode, with a pool of workers or without, where
     * the query's own thread runs every task, under both build modes. Frames of up to 50 rows give
     * every build and probe several tasks. Each build mode applies once per test: Overrides keeps one
     * flag for a change, so that applying a mode whose last property is already set would leave the
     * configuration as it was.
     */
    private void forEachMode(ModeCheck check) throws Exception {
        assertMemoryLeak(() -> {
            for (HashJoinBuildMode buildMode : HashJoinBuildMode.values()) {
                buildMode.applyProperties(node1.getConfigurationOverrides());
                isParallelBuild = buildMode == HashJoinBuildMode.PARALLEL;
                Assert.assertEquals(isParallelBuild ? 0 : Long.MAX_VALUE, configuration.getSqlParallelHashJoinGroupByBuildParallelMinRows());
                for (WorkerPoolMode mode : POOL_MODES) {
                    final TestWorkerPool pool = mode == null ? null : new TestWorkerPool(WORKER_COUNT, mode);
                    final String label = (mode == null ? "OWNER" : mode.name()) + '-' + buildMode;
                    TestUtils.execute(pool, (db, compiler, ignored) -> {
                        createTables(db, ignored);
                        try (SqlExecutionContextImpl context = new SqlExecutionContextImpl(db, WORKER_COUNT)) {
                            context.with(AllowAllSecurityContext.INSTANCE, bindVariableService, null, -1, null);
                            context.setParallelGroupByEnabled(true);
                            context.setParallelHashJoinGroupByEnabled(true);
                            context.changePageFrameSizes(20, 50);
                            check.run(db, context, label);
                        } finally {
                            db.execute("DROP TABLE r", ignored);
                            db.execute("DROP TABLE p", ignored);
                        }
                    }, configuration, LOG);
                }
            }
        });
    }

    @FunctionalInterface
    private interface ModeCheck {
        void run(CairoEngine db, SqlExecutionContextImpl context, String label) throws Exception;
    }
}
