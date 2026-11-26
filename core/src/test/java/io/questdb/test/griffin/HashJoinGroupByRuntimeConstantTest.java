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
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.griffin.engine.table.AsyncHashJoinGroupByAtom;
import io.questdb.griffin.engine.table.AsyncHashJoinGroupByRecordCursorFactory;
import io.questdb.griffin.engine.table.HashJoinGroupByBuildChoiceRecordCursorFactory;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.std.Numbers;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.assertDifferential;
import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.choice;
import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.fused;
import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.ordinary;
import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.plan;
import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.result;

/**
 * Runtime constants and bind variables that one compiled fused factory re-evaluates on every
 * execution: a runtime-constant gate over an input, NULL bind values, bind-variable and now()
 * intervals, SAMPLE BY time zones and offsets, and the payload copy rule. Each execution compares
 * its rows with the ordinary plan's, compiled and run under the same values.
 */
public class HashJoinGroupByRuntimeConstantTest extends AbstractCairoTest {
    private static final String KEYED = "SELECT p.country, count(*) n, count(p.plant_id) matched, sum(r.energy_kwh) energy, sum(p.installed_kwp) capacity";

    @Test
    public void testNowIntervalChoosesTheBuildOnEveryExecution() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContextImpl context = bindingContext()) {
                // r holds 96 hourly rows and p 8 rows. now() moves the end of r's interval, so the
                // input with fewer interval rows changes between executions of one factory.
                final String sql = KEYED + " FROM r JOIN p ON r.plant_id = p.plant_id"
                        + " WHERE r.reading_ts >= '2020-01-01' AND r.reading_ts < now() ORDER BY country";
                setNow(context, "2020-01-01T04:00:00.000000Z");
                try (RecordCursorFactory factory = engine.select(sql, context)) {
                    final HashJoinGroupByBuildChoiceRecordCursorFactory choice = choice(factory);
                    // 4 interval rows of r against 8 of p: r builds.
                    assertChoice(sql, factory, choice, context, true, 4);
                    // 24 rows: p builds.
                    setNow(context, "2020-01-02T00:00:00.000000Z");
                    assertChoice(sql, factory, choice, context, false, 8);
                    // now() before the data leaves r's interval empty, and r builds no rows.
                    setNow(context, "2019-12-31T00:00:00.000000Z");
                    assertChoice(sql, factory, choice, context, true, 0);
                    setNow(context, "2020-01-01T07:00:00.000000Z");
                    assertChoice(sql, factory, choice, context, true, 7);
                }
                // A window of ten hours that ends at now(): it covers 10 rows inside the data and 7 at
                // its end, which stops at 2020-01-04T23:00.
                final String window = KEYED + " FROM r JOIN p ON r.plant_id = p.plant_id"
                        + " WHERE r.reading_ts >= dateadd('h', -10, now()) AND r.reading_ts < now() ORDER BY country";
                setNow(context, "2020-01-02T10:00:00.000000Z");
                try (RecordCursorFactory factory = engine.select(window, context)) {
                    final HashJoinGroupByBuildChoiceRecordCursorFactory choice = choice(factory);
                    assertChoice(window, factory, choice, context, false, 8);
                    setNow(context, "2020-01-05T03:00:00.000000Z");
                    assertChoice(window, factory, choice, context, true, 7);
                    setNow(context, "2020-01-02T10:00:00.000000Z");
                    assertChoice(window, factory, choice, context, false, 8);
                }
            }
        });
    }

    @Test
    public void testNullBindValueOnNullExtendedRows() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // No row of p matches plant 7, so the outer joins NULL-extend r's 12 rows of plant 7.
            // p's plant 5 has a NULL country, and p's NULL key matches no row. A NULL bind value
            // compares equal to a NULL column value: above the join, p.country = $1 keeps the
            // NULL-extended rows and the rows of plant 5, and as the build's ON filter it keeps the
            // build rows of plant 5 alone, so the outer join NULL-extends every other probe row.
            final String[] queries = {
                    KEYED + " FROM r LEFT JOIN p ON r.plant_id = p.plant_id WHERE p.country = $1 ORDER BY country",
                    KEYED + " FROM r LEFT JOIN p ON r.plant_id = p.plant_id AND p.country = $1 ORDER BY country",
                    KEYED + " FROM r LEFT JOIN p ON r.plant_id = p.plant_id WHERE p.s = $1 ORDER BY country",
                    KEYED + " FROM r LEFT JOIN p ON r.plant_id = p.plant_id AND p.s = $1 ORDER BY country",
                    KEYED + " FROM p RIGHT JOIN r ON r.plant_id = p.plant_id WHERE p.country = $1 ORDER BY country",
                    KEYED + " FROM r JOIN p ON r.plant_id = p.plant_id WHERE p.country = $1 ORDER BY country",
                    "SELECT count(*) n, count(p.plant_id) matched FROM r LEFT JOIN p ON r.plant_id = p.plant_id WHERE p.country = $1"
            };
            try (SqlExecutionContextImpl context = bindingContext()) {
                for (String sql : queries) {
                    bindVariableService.clear();
                    bindVariableService.setStr(0, null);
                    try (RecordCursorFactory factory = engine.select(sql, context)) {
                        assertFusedPlan(sql, factory, context);
                        for (String value : new String[]{null, "ES", "P1", null, "missing"}) {
                            bindVariableService.setStr(0, value);
                            assertExecution(sql, factory, context);
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testNullIntervalBoundInBuildChoice() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContextImpl context = bindingContext()) {
                final String sql = KEYED + " FROM r JOIN p ON r.plant_id = p.plant_id"
                        + " WHERE r.reading_ts >= $1 AND r.reading_ts < $2 ORDER BY country";
                // Bind before compiling, as a client that sends typed parameters does.
                bindInterval("2020-01-01T00:00:00.000000Z", "2020-01-01T05:00:00.000000Z");
                try (RecordCursorFactory factory = engine.select(sql, context)) {
                    final HashJoinGroupByBuildChoiceRecordCursorFactory choice = choice(factory);
                    assertChoice(sql, factory, choice, context, true, 5);
                    // A NULL bound leaves r's interval empty, as in the ordinary plan, so the row count
                    // that picks the build reads no rows of r, and r builds none.
                    bindInterval(null, "2020-01-01T05:00:00.000000Z");
                    assertChoice(sql, factory, choice, context, true, 0);
                    bindInterval("2020-01-01T00:00:00.000000Z", null);
                    assertChoice(sql, factory, choice, context, true, 0);
                    bindInterval(null, null);
                    assertChoice(sql, factory, choice, context, true, 0);
                    bindInterval("2020-01-01T00:00:00.000000Z", "2020-01-02T06:00:00.000000Z");
                    assertChoice(sql, factory, choice, context, false, 8);
                    bindInterval("2020-01-01T00:00:00.000000Z", "2020-01-01T05:00:00.000000Z");
                    assertChoice(sql, factory, choice, context, true, 5);
                }
            }
        });
    }

    @Test
    public void testPayloadCopyDecisionChangesBetweenExecutions() throws Exception {
        assertMemoryLeak(() -> {
            createPayloadTables();
            try (SqlExecutionContextImpl context = bindingContext()) {
                // The LEFT join builds pb's 2_000 rows and probes the rows of pa's interval. The owner's
                // build copies the payload for a probe of at least 0.4 x 2_000 = 800 rows, so the
                // interval's end moves the decision across the ratio between executions.
                final String sql = "SELECT count(*) n, sum(pb.v) v FROM pa LEFT JOIN pb ON pa.k = pb.k WHERE pa.ts < $1";
                bindVariableService.setTimestamp(0, MicrosFormatUtils.parseTimestamp("2020-01-04T04:48:00.000000Z"));
                try (RecordCursorFactory factory = engine.select(sql, context)) {
                    assertFusedPlan(sql, factory, context);
                    // 800, 799, 800 and 0 probe rows.
                    for (String end : new String[]{"2020-01-04T04:48", "2020-01-04T04:42", "2020-01-04T04:48", "2019-12-31T00:00"}) {
                        bindVariableService.setTimestamp(0, MicrosFormatUtils.parseTimestamp(end + ":00.000000Z"));
                        assertExecution(sql, factory, context);
                        assertPayloadCopy(factory, context, end.equals("2020-01-04T04:48"), false);
                    }
                }
            }
        });
    }

    @Test
    public void testPayloadCopyDecisionChangesBetweenExecutionsInRounds() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 0);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_ROWS_PER_PARTITION, 1);
        // The owner's ratio would copy for no probe below 2_000 x 1_000 rows, so only the parallel
        // ratio, 0.125 by default, can copy.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_MIN_PROBE_RATIO, "1000");
        assertMemoryLeak(() -> {
            createPayloadTables();
            try (SqlExecutionContextImpl context = bindingContext()) {
                // A build in rounds copies on the workers for a probe of at least 0.125 x 2_000 = 250
                // rows: one day of pa holds 250 rows, and 249 of them stop short of it.
                final String sql = "SELECT count(*) n, sum(pb.v) v FROM pa LEFT JOIN pb ON pa.k = pb.k WHERE pa.ts >= $1 AND pa.ts < $2";
                bindInterval("2020-01-02T00:00:00.000000Z", "2020-01-03T00:00:00.000000Z");
                try (RecordCursorFactory factory = engine.select(sql, context)) {
                    assertFusedPlan(sql, factory, context);
                    for (String end : new String[]{"2020-01-03T00:00", "2020-01-02T23:54", "2020-01-03T00:00", "2020-01-02T23:54"}) {
                        bindInterval("2020-01-02T00:00:00.000000Z", end + ":00.000000Z");
                        assertExecution(sql, factory, context);
                        assertPayloadCopy(factory, context, end.equals("2020-01-03T00:00"), true);
                    }
                }
            }
        });
    }

    @Test
    public void testRuntimeConstantGateDecidesTheBuildChoice() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContextImpl context = bindingContext()) {
                bindVariableService.setInt(0, 1);
                // The row count that picks the build walks the gate's frames: all of p's 8 rows while
                // $1 = 1 holds, and none otherwise. r's interval holds 4 rows.
                final String gatedBuild = KEYED + " FROM r JOIN (SELECT * FROM p WHERE $1 = 1) p ON r.plant_id = p.plant_id"
                        + " WHERE r.reading_ts >= '2020-01-01' AND r.reading_ts < '2020-01-01T04:00' ORDER BY country";
                try (RecordCursorFactory factory = engine.select(gatedBuild, context)) {
                    final HashJoinGroupByBuildChoiceRecordCursorFactory choice = choice(factory);
                    Assert.assertTrue(plan(factory, context).contains("Filter filter: $0::int=1"));
                    assertChoice(gatedBuild, factory, choice, context, true, 4);
                    bindVariableService.setInt(0, 2);
                    assertChoice(gatedBuild, factory, choice, context, false, 0);
                    bindVariableService.setInt(0, Numbers.INT_NULL);
                    assertChoice(gatedBuild, factory, choice, context, false, 0);
                    bindVariableService.setInt(0, 1);
                    assertChoice(gatedBuild, factory, choice, context, true, 4);
                }
                // The gate sits over r's interval scan of 24 rows, so p's 8 rows build while it holds,
                // and r's empty frames otherwise.
                final String gatedProbe = KEYED + " FROM (SELECT * FROM r WHERE $1 = 1) r JOIN p ON r.plant_id = p.plant_id"
                        + " WHERE r.reading_ts < '2020-01-02' ORDER BY country";
                try (RecordCursorFactory factory = engine.select(gatedProbe, context)) {
                    final HashJoinGroupByBuildChoiceRecordCursorFactory choice = choice(factory);
                    assertChoice(gatedProbe, factory, choice, context, false, 8);
                    bindVariableService.setInt(0, 2);
                    assertChoice(gatedProbe, factory, choice, context, true, 0);
                    bindVariableService.setInt(0, 1);
                    assertChoice(gatedProbe, factory, choice, context, false, 8);
                }
            }
        });
    }

    @Test
    public void testRuntimeConstantGateOverEitherInput() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // Each row holds the FROM clause, whether the gate is over the input that builds, and
            // whether the query takes the fused plan. The joins take no interval, so table sizes fix
            // the build: p, the smaller table, for the INNER join, and the side that the outer join
            // NULL-extends otherwise. A RIGHT join keeps the ordinary plan for a gate over r, which
            // it preserves, since the gate is a WHERE clause of r's own, which reduces the rows that
            // the ordinary plan hashes.
            final Object[][] froms = {
                    {" FROM r JOIN (SELECT * FROM p WHERE $1 = 1) p ON r.plant_id = p.plant_id", true, true},
                    {" FROM (SELECT * FROM r WHERE $1 = 1) r JOIN p ON r.plant_id = p.plant_id", false, true},
                    {" FROM r LEFT JOIN (SELECT * FROM p WHERE $1 = 1) p ON r.plant_id = p.plant_id", true, true},
                    {" FROM (SELECT * FROM r WHERE $1 = 1) r LEFT JOIN p ON r.plant_id = p.plant_id", false, true},
                    {" FROM (SELECT * FROM p WHERE $1 = 1) p RIGHT JOIN r ON r.plant_id = p.plant_id", true, true},
                    {" FROM p RIGHT JOIN (SELECT * FROM r WHERE $1 = 1) r ON r.plant_id = p.plant_id", false, false},
                    {" FROM (SELECT * FROM r WHERE $1 = 1) r LEFT JOIN (SELECT * FROM p WHERE $1 = 1) p ON r.plant_id = p.plant_id", true, true}
            };
            try (SqlExecutionContextImpl context = bindingContext()) {
                for (Object[] from : froms) {
                    final boolean isBuildGated = (boolean) from[1];
                    final boolean isFused = (boolean) from[2];
                    for (String select : new String[]{KEYED, "SELECT count(*) n, sum(r.energy_kwh) energy, sum(p.installed_kwp) capacity"}) {
                        final String sql = select + from[0] + (select.equals(KEYED) ? " ORDER BY country" : "");
                        bindVariableService.clear();
                        bindVariableService.setInt(0, 1);
                        try (RecordCursorFactory factory = engine.select(sql, context)) {
                            final String plan = plan(factory, context);
                            Assert.assertEquals(sql + '\n' + plan, isFused, plan.contains("Hash Join Group By"));
                            Assert.assertTrue(sql + '\n' + plan, plan.contains("Filter filter: $0::int=1"));
                            for (int value : new int[]{1, 2, 1, Numbers.INT_NULL, 1}) {
                                bindVariableService.setInt(0, value);
                                assertExecution(sql, factory, context);
                                if (isBuildGated) {
                                    // A false gate yields no frames, so the build walks none.
                                    Assert.assertEquals(sql, value == 1 ? 8 : 0, buildRows(factory, context));
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    // A build in rounds dispatches a task per frame of the gate's base while the gate holds, and walks
    // the false gate's empty frames otherwise. A SYMBOL key translates the probe's keys through the
    // dictionary of the gate's base, which the false gate still opens.
    @Test
    public void testRuntimeConstantGateOverParallelBuild() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 0);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_ROWS_PER_PARTITION, 1);
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContextImpl context = bindingContext()) {
                // Frames of up to 2 rows split p's 8 rows into 4 build tasks.
                context.changePageFrameSizes(1, 2);
                for (String sql : new String[]{
                        KEYED + " FROM r JOIN (SELECT * FROM p WHERE $1 = 1) p ON r.plant_id = p.plant_id ORDER BY country",
                        KEYED + " FROM r LEFT JOIN (SELECT * FROM p WHERE $1 = 1) p ON r.plant_id = p.plant_id ORDER BY country",
                        "SELECT p.s, count(*) n, sum(r.energy_kwh) energy FROM r JOIN (SELECT * FROM p WHERE $1 = 1) p ON r.s = p.s ORDER BY s",
                        "SELECT p.s, count(*) n, sum(r.energy_kwh) energy FROM r LEFT JOIN (SELECT * FROM p WHERE $1 = 1) p ON r.s = p.s ORDER BY s",
                        "SELECT count(*) n FROM r LEFT JOIN (SELECT * FROM p WHERE $1 = 1) p ON r.plant_id = p.plant_id"
                }) {
                    bindVariableService.clear();
                    bindVariableService.setInt(0, 1);
                    try (RecordCursorFactory factory = engine.select(sql, context)) {
                        assertFusedPlan(sql, factory, context);
                        for (int value : new int[]{1, 2, 1, Numbers.INT_NULL, 1}) {
                            bindVariableService.setInt(0, value);
                            assertExecution(sql, factory, context);
                            final AsyncHashJoinGroupByAtom atom = fused(factory).getAtom();
                            try (RecordCursor cursor = factory.getCursor(context)) {
                                cursor.hasNext();
                                Assert.assertEquals(sql, value == 1 ? 8 : 0, atom.getFrozenBuild().getRowCount());
                                if (value == 1) {
                                    Assert.assertTrue(sql, atom.isBuiltInRounds());
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testSampleByBindVariableTimeZone() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContextImpl context = bindingContext()) {
                for (String join : new String[]{" JOIN ", " LEFT JOIN "}) {
                    for (String sample : new String[]{
                            " SAMPLE BY 1d ALIGN TO CALENDAR TIME ZONE $1",
                            " SAMPLE BY 5h ALIGN TO CALENDAR TIME ZONE $1 WITH OFFSET '00:30'"
                    }) {
                        final String sql = "SELECT r.reading_ts, count(*) n, sum(r.energy_kwh) energy, sum(p.installed_kwp) capacity"
                                + " FROM r" + join + "p ON r.plant_id = p.plant_id" + sample;
                        bindVariableService.clear();
                        bindVariableService.setStr(0, "Europe/Berlin");
                        try (RecordCursorFactory factory = engine.select(sql, context)) {
                            final String plan = assertFusedPlan(sql, factory, context);
                            Assert.assertTrue(sql + '\n' + plan, plan.contains("$0::string"));
                            String previous = null;
                            // A NULL time zone floors in UTC, as the ordinary plan does.
                            for (String timeZone : new String[]{"Europe/Berlin", "America/New_York", "Asia/Kolkata", "+03:00", null, "Europe/Berlin"}) {
                                bindVariableService.setStr(0, timeZone);
                                final String rows = assertExecution(sql, factory, context);
                                // Each time zone moves the buckets, so the rows differ from the previous ones.
                                Assert.assertNotEquals(sql + '\n' + timeZone, previous, rows);
                                previous = rows;
                            }
                        }
                    }
                }
            }
        });
    }

    // Each worker floors the timestamps of its frames with its own copy of the key function, so every
    // copy must take the time zone that each execution binds.
    @Test
    public void testSampleByBindVariableTimeZoneOnWorkers() throws Exception {
        for (WorkerPoolMode mode : new WorkerPoolMode[]{WorkerPoolMode.LEGACY, WorkerPoolMode.FIBER_HOST}) {
            assertMemoryLeak(() -> {
                final TestWorkerPool pool = new TestWorkerPool(4, mode);
                TestUtils.execute(pool, (db, compiler, ctx) -> {
                    final BindVariableService bindVariables = new BindVariableServiceImpl(db.getConfiguration());
                    try (SqlExecutionContextImpl context = new SqlExecutionContextImpl(db, 4)
                            .with(AllowAllSecurityContext.INSTANCE, bindVariables, null, -1, null)) {
                        context.setParallelGroupByEnabled(true);
                        context.setParallelHashJoinGroupByEnabled(true);
                        // Frames of up to 4 rows split r's four days into 24 probe tasks.
                        context.changePageFrameSizes(1, 4);
                        db.execute("""
                                CREATE TABLE r (plant_id INT, reading_ts TIMESTAMP, energy_kwh DOUBLE)
                                TIMESTAMP(reading_ts) PARTITION BY DAY
                                """, context);
                        db.execute("CREATE TABLE p (plant_id INT, country SYMBOL, installed_kwp DOUBLE)", context);
                        db.execute("""
                                INSERT INTO r
                                SELECT (x % 8)::INT, '2020-01-01'::TIMESTAMP + (x - 1) * 3_600_000_000L, x::DOUBLE
                                FROM long_sequence(96)
                                """, context);
                        db.execute("INSERT INTO p VALUES (0, 'ES', 1), (1, 'ES', 2), (2, 'IT', 3), (3, 'IT', 4), (4, 'DE', 5), (5, NULL, 6)", context);
                        final String sql = """
                                SELECT * FROM (
                                    SELECT r.reading_ts, p.country, count(*) n, sum(r.energy_kwh) energy
                                    FROM r JOIN p ON r.plant_id = p.plant_id
                                    SAMPLE BY 5h ALIGN TO CALENDAR TIME ZONE $1
                                ) ORDER BY reading_ts, country
                                """;
                        bindVariables.setStr(0, "Europe/Berlin");
                        try (RecordCursorFactory factory = db.select(sql, context)) {
                            assertFusedPlan(sql, factory, context);
                            for (String timeZone : new String[]{"Europe/Berlin", "Asia/Kolkata", "America/New_York", "Europe/Berlin"}) {
                                bindVariables.setStr(0, timeZone);
                                final String expected;
                                context.setParallelHashJoinGroupByEnabled(false);
                                try (RecordCursorFactory baseline = db.select(sql, context)) {
                                    expected = result(baseline, context);
                                } finally {
                                    context.setParallelHashJoinGroupByEnabled(true);
                                }
                                Assert.assertEquals(mode + " " + timeZone, expected, result(factory, context));
                                Assert.assertEquals(mode + " " + timeZone, expected, result(factory, context));
                            }
                        }
                        Assert.assertNull(context.getMemoryTracker());
                        // The next mode's engine opens the same root.
                        db.execute("DROP TABLE r", context);
                        db.execute("DROP TABLE p", context);
                    }
                }, configuration, LOG);
            });
        }
    }

    @Test
    public void testSampleByFromBindVariableKeepsOrdinaryPlan() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContextImpl context = bindingContext()) {
                // The optimizer rewrites SAMPLE BY to GROUP BY only for a FROM bound that is neither a
                // bind variable nor an expression, so these keep the SAMPLE BY cursor over the
                // ordinary join, which the fused path leaves alone.
                for (String join : new String[]{" JOIN ", " LEFT JOIN "}) {
                    for (String sample : new String[]{
                            " SAMPLE BY 1h FROM $1",
                            " SAMPLE BY 1h FROM $1 TO $2",
                            " SAMPLE BY 1h FROM dateadd('h', 1, $1) TO $2",
                            " SAMPLE BY 1d FROM $1 TO $2 ALIGN TO CALENDAR TIME ZONE 'Europe/Berlin'"
                    }) {
                        final String sql = "SELECT r.reading_ts, count(*) n, sum(r.energy_kwh) energy FROM r" + join
                                + "p ON r.plant_id = p.plant_id" + sample;
                        bindInterval("2020-01-01T05:00:00.000000Z", "2020-01-01T12:00:00.000000Z");
                        Assert.assertEquals(sql, outcome(sql, context, false), outcome(sql, context, true));
                        try (RecordCursorFactory factory = engine.select(sql, context)) {
                            final String plan = plan(factory, context);
                            Assert.assertFalse(sql + '\n' + plan, plan.contains("Hash Join Group By"));
                            Assert.assertTrue(sql + '\n' + plan, plan.startsWith("Sample By"));
                        }
                    }
                }
            }
        });
    }

    // A sub-day SAMPLE BY with a TIME ZONE converts its FROM and TO bounds to UTC at compile time, and
    // reads a bind variable bound before it is linked. Fails with or without the fused path, and on a
    // single table too.
    // PR #7681 rejects these statements with a SqlException at the FROM bind variable. Once it lands,
    // expect that error with the fused path on and off instead of rows.
    @Ignore("Known bug #7750: SAMPLE BY FROM $1 with a sub-day stride and TIME ZONE fails to compile")
    @Test
    public void testSampleByFromBindVariableWithSubDayTimeZone() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContextImpl context = bindingContext()) {
                for (String join : new String[]{" JOIN ", " LEFT JOIN "}) {
                    final String sql = "SELECT r.reading_ts, count(*) n, sum(r.energy_kwh) energy FROM r" + join
                            + "p ON r.plant_id = p.plant_id SAMPLE BY 1h FROM $1 TO $2 ALIGN TO CALENDAR TIME ZONE 'Europe/Berlin'";
                    bindInterval("2020-01-01T05:00:00.000000Z", "2020-01-01T12:00:00.000000Z");
                    final String expected = outcome(sql, context, false);
                    Assert.assertFalse(sql + '\n' + expected, expected.startsWith("error:"));
                    Assert.assertEquals(sql, expected, outcome(sql, context, true));
                }
            }
        });
    }

    @Test
    public void testSampleByFromToWithoutFillFuses() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContextImpl context = bindingContext()) {
                for (String join : new String[]{" JOIN ", " LEFT JOIN "}) {
                    for (String sample : new String[]{
                            " SAMPLE BY 1h FROM '2020-01-01T02:00' TO '2020-01-01T08:00'",
                            " SAMPLE BY 1h FROM '2020-01-01T02:30' TO '2020-01-02T08:00'",
                            " SAMPLE BY 1d FROM '2020-01-02'",
                            " SAMPLE BY 3h TO '2020-01-02T08:00'",
                            " SAMPLE BY 1h FROM '2020-01-01T02:00' TO '2020-01-01T08:00' FILL(NONE)"
                    }) {
                        for (String select : new String[]{
                                "SELECT r.reading_ts, count(*) n, sum(r.energy_kwh) energy, sum(p.installed_kwp) capacity FROM r",
                                "SELECT r.reading_ts, p.country, count(*) n, sum(r.energy_kwh) energy FROM r"
                        }) {
                            String sql = select + join + "p ON r.plant_id = p.plant_id" + sample;
                            if (select.contains("country")) {
                                // Keys within one bucket come in hash order, which differs between plans.
                                sql = "SELECT * FROM (" + sql + ") ORDER BY reading_ts, country";
                            }
                            assertDifferential(sql, context, true);
                            try (RecordCursorFactory factory = engine.select(sql, context)) {
                                // FROM and TO narrow r's scan to their interval, inside the fused plan.
                                final String plan = plan(factory, context);
                                Assert.assertTrue(sql + '\n' + plan, plan.contains("Interval forward scan on: r"));
                                // An INNER join with an interval may build either input.
                                Assert.assertEquals(sql + '\n' + plan, join.equals(" JOIN "), plan.contains("Build Choice"));
                            }
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testSampleByTimeZoneAndOffsetWithoutFillFuses() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContextImpl context = bindingContext()) {
                // The ordinary plan fails to compile TIME ZONE over aliased inputs, so use table names.
                for (String join : new String[]{" JOIN ", " LEFT JOIN "}) {
                    for (String sample : new String[]{
                            " SAMPLE BY 1d ALIGN TO CALENDAR TIME ZONE 'Europe/Berlin'",
                            " SAMPLE BY 5h ALIGN TO CALENDAR TIME ZONE 'Asia/Kolkata'",
                            " SAMPLE BY 1d ALIGN TO CALENDAR WITH OFFSET '02:30'",
                            " SAMPLE BY 1d ALIGN TO CALENDAR TIME ZONE 'America/New_York' WITH OFFSET '01:00'",
                            " SAMPLE BY 3h FILL(NONE) ALIGN TO CALENDAR TIME ZONE 'Europe/Berlin' WITH OFFSET '00:15'"
                    }) {
                        for (String select : new String[]{
                                "SELECT r.reading_ts, count(*) n, sum(r.energy_kwh) energy, sum(p.installed_kwp) capacity FROM r",
                                "SELECT r.reading_ts, p.country, count(*) n, avg(r.energy_kwh) energy FROM r"
                        }) {
                            String sql = select + join + "p ON r.plant_id = p.plant_id" + sample;
                            if (select.contains("country")) {
                                // Keys within one bucket come in hash order, which differs between plans.
                                sql = "SELECT * FROM (" + sql + ") ORDER BY reading_ts, country";
                            }
                            assertDifferential(sql, context, true);
                            try (RecordCursorFactory factory = engine.select(sql, context)) {
                                // The fused plan floors the timestamp in its key, with the zone and offset.
                                final String plan = plan(factory, context);
                                Assert.assertTrue(sql + '\n' + plan, plan.contains("keyFunctions: [timestamp_floor_utc("));
                            }
                        }
                    }
                }
            }
        });
    }

    private static String assertFusedPlan(String sql, RecordCursorFactory factory, SqlExecutionContextImpl context) {
        final String plan = plan(factory, context);
        Assert.assertTrue(sql + '\n' + plan, plan.contains("Hash Join Group By"));
        return plan;
    }

    // Checks whether an execution copies the build's payload, and where the build ran.
    private static void assertPayloadCopy(
            RecordCursorFactory factory,
            SqlExecutionContextImpl context,
            boolean isCopied,
            boolean isBuiltInRounds
    ) throws Exception {
        final AsyncHashJoinGroupByAtom atom = fused(factory).getAtom();
        try (RecordCursor cursor = factory.getCursor(context)) {
            // The copy decision runs once the probe's frames are known, on the first read.
            Assert.assertTrue(cursor.hasNext());
            Assert.assertEquals(isCopied, atom.isPayloadCopied());
            Assert.assertEquals(isBuiltInRounds, atom.isBuiltInRounds());
        }
        Assert.assertFalse(atom.isPayloadCopied());
    }

    private static void bindInterval(@Nullable String lo, @Nullable String hi) throws Exception {
        bindVariableService.setTimestamp(0, lo == null ? Numbers.LONG_NULL : MicrosFormatUtils.parseTimestamp(lo));
        bindVariableService.setTimestamp(1, hi == null ? Numbers.LONG_NULL : MicrosFormatUtils.parseTimestamp(hi));
    }

    // Runs the factory once and returns the rows that its execution built.
    private static long buildRows(RecordCursorFactory factory, SqlExecutionContextImpl context) throws Exception {
        try (RecordCursor cursor = factory.getCursor(context)) {
            // The first read builds, whether or not it returns a row.
            cursor.hasNext();
            return fused(factory).getAtom().getFrozenBuild().getRowCount();
        }
    }

    // Returns two reads of the query's rows, or its error, with the fused path off or on.
    private static String outcome(String sql, SqlExecutionContextImpl context, boolean isEnabled) throws Exception {
        context.setParallelHashJoinGroupByEnabled(isEnabled);
        try (RecordCursorFactory factory = context.getCairoEngine().select(sql, context)) {
            return result(factory, context) + result(factory, context);
        } catch (SqlException | CairoException e) {
            return "error: " + e.getFlyweightMessage();
        } finally {
            context.setParallelHashJoinGroupByEnabled(true);
        }
    }

    private static void setNow(SqlExecutionContextImpl context, String now) throws Exception {
        context.setNowAndFixClock(MicrosFormatUtils.parseTimestamp(now), ColumnType.TIMESTAMP_MICRO);
    }

    /**
     * Runs one execution of the build choice: compares the rows with the ordinary plan's, and checks
     * the orientation that ran and the rows it built, which stay published while the cursor is open.
     */
    private void assertChoice(
            String sql,
            RecordCursorFactory factory,
            HashJoinGroupByBuildChoiceRecordCursorFactory choice,
            SqlExecutionContextImpl context,
            boolean isAlternate,
            long buildRows
    ) throws Exception {
        final String expected = ordinary(engine, sql, context);
        try (RecordCursor cursor = factory.getCursor(context)) {
            Assert.assertEquals(sql, isAlternate, choice.isAlternateChosen());
            final StringSink sink = new StringSink();
            CursorPrinter.println(cursor, factory.getMetadata(), sink, true, true);
            Assert.assertEquals(sql, expected, sink.toString());
            final AsyncHashJoinGroupByRecordCursorFactory chosen = isAlternate ? choice.getAlternate() : choice.getPrimary();
            Assert.assertEquals(sql, buildRows, chosen.getAtom().getFrozenBuild().getRowCount());
            Assert.assertNull(sql, (isAlternate ? choice.getPrimary() : choice.getAlternate()).getAtom().getFrozenBuild());
        }
        // The second read of the same execution's values.
        Assert.assertEquals(sql, expected, result(factory, context));
        Assert.assertNull(context.getMemoryTracker());
    }

    // Compares two reads of one execution with the ordinary plan's rows under the same values.
    private String assertExecution(String sql, RecordCursorFactory factory, SqlExecutionContextImpl context) throws Exception {
        final String expected = ordinary(engine, sql, context);
        Assert.assertEquals(sql, expected, result(factory, context));
        Assert.assertEquals(sql, expected, result(factory, context));
        Assert.assertNull(context.getMemoryTracker());
        return expected;
    }

    private SqlExecutionContextImpl bindingContext() {
        final SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 4)
                .with(AllowAllSecurityContext.INSTANCE, bindVariableService, null, -1, null);
        context.setParallelGroupByEnabled(true);
        context.setParallelHashJoinGroupByEnabled(true);
        return context;
    }

    // pa spreads 1_000 rows over four days, 250 a day; pb holds 2_000 rows. Both key their rows by ten values.
    private void createPayloadTables() throws Exception {
        execute("CREATE TABLE pa (k INT, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE pb (k INT, v DOUBLE)");
        execute("INSERT INTO pa SELECT (x % 10)::INT, x, timestamp_sequence('2020-01-01', 345_600_000L) FROM long_sequence(1_000)");
        execute("INSERT INTO pb SELECT (x % 10)::INT, x FROM long_sequence(2_000)");
    }

    // r holds 96 hourly rows from 2020-01-01, 24 a day, over plants 0 to 7; p holds 8 rows, one of
    // them with a NULL key, and no row for plant 7.
    private void createTables() throws Exception {
        execute("""
                CREATE TABLE r (plant_id INT, reading_ts TIMESTAMP, energy_kwh DOUBLE, s SYMBOL)
                TIMESTAMP(reading_ts) PARTITION BY DAY
                """);
        execute("CREATE TABLE p (plant_id INT, country SYMBOL, installed_kwp DOUBLE, s SYMBOL)");
        execute("""
                INSERT INTO r
                SELECT (x % 8)::INT, '2020-01-01'::TIMESTAMP + (x - 1) * 3_600_000_000L, x::DOUBLE, 'P' || (x % 8)
                FROM long_sequence(96)
                """);
        execute("""
                INSERT INTO p VALUES
                    (0, 'ES', 1, 'P0'), (1, 'ES', 2, 'P1'), (2, 'IT', 3, 'P2'), (3, 'IT', 4, 'P3'),
                    (4, 'DE', 5, 'P4'), (5, NULL, 6, 'P5'), (6, 'ES', NULL, 'P6'), (NULL, 'FR', 8, NULL)
                """);
    }
}
