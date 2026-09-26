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
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.TextPlanSink;
import io.questdb.griffin.engine.table.AsyncHashJoinGroupByRecordCursorFactory;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Covers the JIT-compiled probe filter of the fused hash join GROUP BY: which shapes reach
 * the compiled filter, the {@code Async JIT Hash Join Group By} plan name that reports it,
 * and the per-frame fallbacks to the interpreted filter that must keep the results identical.
 */
public class HashJoinGroupByJitFilterTest extends AbstractCairoTest {
    private static final String INTERPRETED = "Async Hash Join Group By";
    private static final String[] JOINS = {
            " FROM r JOIN p ON r.plant_id = p.plant_id",
            " FROM r LEFT JOIN p ON r.plant_id = p.plant_id",
            " FROM p RIGHT JOIN r ON r.plant_id = p.plant_id"
    };
    private static final String JIT = "Async JIT Hash Join Group By";
    private static final String KEYED_SELECT = "SELECT p.country, count(*) pairs, sum(r.energy_kwh) energy";
    private static final String SCALAR_SELECT = "SELECT count(*) pairs, sum(r.energy_kwh) energy, avg(r.energy_kwh) mean";

    @Test
    public void testBindVariablesInProbeFilter() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContextImpl context = enabledContext()) {
                context.with(AllowAllSecurityContext.INSTANCE, bindVariableService, null, -1, null);
                bindVariableService.setDouble(0, 50);
                bindVariableService.setInt(1, 3);
                final String sql = KEYED_SELECT + JOINS[0]
                        + " WHERE r.energy_kwh > $1 AND r.plant_id <> $2 ORDER BY country";
                try (RecordCursorFactory factory = engine.select(sql, context)) {
                    Assert.assertTrue(fused(factory).usesCompiledFilter());
                    assertMatchesOrdinaryPlan(sql, factory, context);
                    // Rebinding must re-run prepareBindVarMemory(), so the compiled filter sees
                    // the new values rather than the ones baked in at the first execution.
                    final String first = result(factory, context);
                    bindVariableService.setDouble(0, 10);
                    bindVariableService.setInt(1, 1);
                    final String second = result(factory, context);
                    Assert.assertNotEquals(first, second);
                    assertMatchesOrdinaryPlan(sql, factory, context);
                }
            }
        });
    }

    @Test
    public void testColumnTopsAndTypeCastsFallBackToInterpretedFilter() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // The column exists only from here on, so every partition written before it carries a
            // column top and the compiled filter, which reads raw column addresses, cannot run on
            // those frames. A wrong answer here means the fallback stopped happening.
            execute("ALTER TABLE r ADD COLUMN backup_kwh DOUBLE");
            execute("INSERT INTO r SELECT (x % 7)::int, timestamp_sequence('2021-01-01', 3_600_000_000L),"
                    + " (x % 97)::double, 'tag' || (x % 3), x::double, (x % 13)::double FROM long_sequence(2_000)");
            try (SqlExecutionContextImpl context = enabledContext()) {
                assertDifferential(KEYED_SELECT + JOINS[0]
                        + " WHERE r.energy_kwh > 10 AND (r.backup_kwh IS NULL OR r.backup_kwh > 3) ORDER BY country", context, JIT);
                // A Parquet column converted from a fixed to a variable size resolves per row
                // through the logical record, which the compiled filter cannot address either.
                // The active partition never converts, so the untouched frames stay in the mix.
                execute("ALTER TABLE r CONVERT PARTITION TO PARQUET WHERE reading_ts < '2021-01-01'");
                execute("ALTER TABLE r ALTER COLUMN note TYPE VARCHAR");
                assertDifferential(KEYED_SELECT + JOINS[0]
                        + " WHERE r.energy_kwh > 10 AND (r.backup_kwh IS NULL OR r.backup_kwh > 3) ORDER BY country", context, JIT);
            }
        });
    }

    @Test
    public void testCompiledProbeFilterAcrossJoinsAndShardingModes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContextImpl context = enabledContext()) {
                for (boolean sharded : new boolean[]{false, true}) {
                    setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, sharded ? 1 : Integer.MAX_VALUE);
                    for (String join : JOINS) {
                        assertDifferential(KEYED_SELECT + join + " WHERE r.energy_kwh > 50 ORDER BY country", context, JIT);
                        assertDifferential(SCALAR_SELECT + join + " WHERE r.energy_kwh > 50", context, JIT);
                    }
                }
            }
        });
    }

    @Test
    public void testCompiledProbeFilterSelectivityRange() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContextImpl context = enabledContext()) {
                // 100%, ~50%, ~2% and 0% of the probe rows survive. The empty case must still
                // produce the scalar aggregation's empty-input row.
                for (String predicate : new String[]{"r.energy_kwh >= 0", "r.energy_kwh > 48", "r.energy_kwh > 95", "r.energy_kwh > 1_000"}) {
                    assertDifferential(KEYED_SELECT + JOINS[0] + " WHERE " + predicate + " ORDER BY country", context, JIT);
                    assertDifferential(SCALAR_SELECT + JOINS[0] + " WHERE " + predicate, context, JIT);
                }
            }
        });
    }

    @Test
    public void testDuplicateBuildKeysWithCompiledProbeFilter() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // Several build rows per key take the duplicate-chain loop rather than the
            // unique-build one, so both filtered loops run against the same data.
            execute("INSERT INTO p VALUES (1, 'PT', 3), (1, 'PT', 9), (2, 'IT', 2), (3, 'DE', 4)");
            try (SqlExecutionContextImpl context = enabledContext()) {
                for (String join : JOINS) {
                    assertDifferential(KEYED_SELECT + join + " WHERE r.energy_kwh > 50 ORDER BY country", context, JIT);
                    assertDifferential(SCALAR_SELECT + join + " WHERE r.energy_kwh > 50", context, JIT);
                }
            }
        });
    }

    @Test
    public void testJitDisabledKeepsInterpretedFilter() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContextImpl context = enabledContext()) {
                final String sql = KEYED_SELECT + JOINS[0] + " WHERE r.energy_kwh > 50 ORDER BY country";
                context.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
                assertDifferential(sql, context, INTERPRETED);
                // The scalar backend still compiles a filter, so the plan keeps reporting JIT.
                context.setJitMode(SqlJitMode.JIT_MODE_FORCE_SCALAR);
                assertDifferential(sql, context, JIT);
            }
        });
    }

    @Test
    public void testJitRejectedProbeFilterKeepsInterpretedFilter() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContextImpl context = enabledContext()) {
                // Neither a function call nor LIKE has an IR opcode, so the JIT declines both
                // and the fused operator keeps the interpreted filter it has always run.
                for (String predicate : new String[]{"abs(r.energy_kwh) > 50", "r.tag LIKE 'tag1%'"}) {
                    assertDifferential(KEYED_SELECT + JOINS[0] + " WHERE " + predicate + " ORDER BY country", context, INTERPRETED);
                }
            }
        });
    }

    @Test
    public void testProbeFilterOverManyFramesAndWorkers() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContextImpl context = enabledContext()) {
                // Small frames put many frames in flight, so the filtered row list is reused
                // across frames on every worker slot and on the owner slot alike.
                context.changePageFrameSizes(128, 128);
                for (String join : JOINS) {
                    assertDifferential(KEYED_SELECT + join + " WHERE r.energy_kwh > 50 ORDER BY country", context, JIT);
                }
            }
        });
    }

    @Test
    public void testProbeFilterWithPostJoinFilterAndProjection() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContextImpl context = enabledContext()) {
                // A build-side predicate on an outer join becomes a post-join filter, which runs
                // after the probe filter has already narrowed the frame.
                assertDifferential(KEYED_SELECT + JOINS[1]
                        + " WHERE r.energy_kwh > 50 AND p.installed_kwp > 5 ORDER BY country", context, JIT);
                assertDifferential("SELECT p.country, sum(r.energy_kwh) / nullif(sum(p.installed_kwp), 0) yield"
                        + JOINS[1] + " WHERE r.energy_kwh > 50 ORDER BY country", context, JIT);
            }
        });
    }

    private static AsyncHashJoinGroupByRecordCursorFactory fused(RecordCursorFactory factory) {
        while (factory != null && !(factory instanceof AsyncHashJoinGroupByRecordCursorFactory)) {
            factory = factory.getBaseFactory();
        }
        Assert.assertNotNull(factory);
        return (AsyncHashJoinGroupByRecordCursorFactory) factory;
    }

    private static String plan(RecordCursorFactory factory, SqlExecutionContext context) {
        TextPlanSink sink = new TextPlanSink();
        sink.of(factory, context);
        return sink.getSink().toString();
    }

    private static String result(RecordCursorFactory factory, SqlExecutionContext context) throws Exception {
        try (RecordCursor cursor = factory.getCursor(context)) {
            StringSink sink = new StringSink();
            CursorPrinter.println(cursor, factory.getMetadata(), sink, true, true);
            return sink.toString();
        }
    }

    // Compares the fused results against the ordinary plan's, then pins the plan type name,
    // so a regression reports the wrong answer rather than only the wrong operator.
    private void assertDifferential(String sql, SqlExecutionContextImpl context, String expectedType) throws Exception {
        try (RecordCursorFactory factory = engine.select(sql, context)) {
            assertMatchesOrdinaryPlan(sql, factory, context);
            final String actualPlan = plan(factory, context);
            // The two type names are not substrings of one another, so containment pins the type.
            Assert.assertTrue(sql + "\n" + actualPlan, actualPlan.contains(expectedType));
            Assert.assertEquals(sql, JIT.equals(expectedType), fused(factory).usesCompiledFilter());
        }
    }

    private void assertMatchesOrdinaryPlan(String sql, RecordCursorFactory factory, SqlExecutionContextImpl context) throws Exception {
        final String expected;
        context.setParallelHashJoinGroupByEnabled(false);
        try (RecordCursorFactory baseline = engine.select(sql, context)) {
            Assert.assertFalse(plan(baseline, context).contains("Hash Join Group By"));
            expected = result(baseline, context);
        } finally {
            context.setParallelHashJoinGroupByEnabled(true);
        }
        Assert.assertEquals(sql, expected, result(factory, context));
        Assert.assertNull(context.getMemoryTracker());
    }

    private void createTables() throws Exception {
        // "note" takes no part in any query here, so a storage-level conversion of it changes
        // which filter the reducer runs without changing what the results must be.
        execute("CREATE TABLE r (plant_id INT, reading_ts TIMESTAMP, energy_kwh DOUBLE, tag SYMBOL, note DOUBLE)"
                + " TIMESTAMP(reading_ts) PARTITION BY MONTH");
        execute("CREATE TABLE p (plant_id INT, country SYMBOL, installed_kwp DOUBLE)");
        execute("INSERT INTO r SELECT (x % 7)::int, timestamp_sequence('2020-01-01', 3_600_000_000L),"
                + " (x % 97)::double, 'tag' || (x % 3), x::double FROM long_sequence(20_000)");
        execute("INSERT INTO p VALUES (1, 'ES', 5), (2, 'IT', 7), (3, 'DE', 11), (4, 'FR', null), (null, 'ES', 13)");
    }

    private SqlExecutionContextImpl enabledContext() {
        SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 4)
                .with(AllowAllSecurityContext.INSTANCE, null, null, -1, null);
        context.setParallelGroupByEnabled(true);
        context.setParallelHashJoinGroupByEnabled(true);
        return context;
    }
}
