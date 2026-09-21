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
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.TextPlanSink;
import io.questdb.griffin.engine.table.AsyncHashJoinGroupByRecordCursorFactory;
import io.questdb.griffin.engine.table.HashJoinGroupByBuildChoiceRecordCursorFactory;
import io.questdb.jit.JitUtil;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

/**
 * An INNER join whose input scans a timestamp interval compiles both orientations under a
 * {@link HashJoinGroupByBuildChoiceRecordCursorFactory}, which builds the input with fewer
 * interval rows on every execution. Every case compares the rows with the ordinary plan's,
 * twice, and pins which orientation ran and how many rows it built.
 */
public class HashJoinGroupByBuildChoiceTest extends AbstractCairoTest {
    private static final String BUILD_CHOICE = "Hash Join Group By Build Choice";
    private static final String FIVE_HOURS = " WHERE r.reading_ts >= '2020-01-01T02:00' AND r.reading_ts < '2020-01-01T07:00'";
    private static final String ONE_DAY = " WHERE r.reading_ts >= '2020-01-02' AND r.reading_ts < '2020-01-03'";
    // r holds 96 hourly rows from 2020-01-01, 24 a day; p holds 8 rows, one of them with a NULL key.
    private static final String KEYED = "SELECT p.country, sum(r.energy_kwh) energy, count(*) n, sum(p.installed_kwp) capacity";
    private static final String SCALAR = "SELECT count(*) n, sum(r.energy_kwh) energy, sum(p.installed_kwp) capacity";

    @Test
    public void testBindVariablesChooseOnEveryExecution() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContextImpl context = enabledContext()) {
                String sql = KEYED + " FROM r JOIN p ON r.plant_id = p.plant_id"
                        + " WHERE r.reading_ts >= $1 AND r.reading_ts < $2 ORDER BY country";
                // The analysis rejects a bind variable without a type, so bind before compiling.
                bindInterval("2020-01-01T00", "2020-01-01T05");
                try (RecordCursorFactory factory = engine.select(sql, context)) {
                    HashJoinGroupByBuildChoiceRecordCursorFactory choice = choice(factory);
                    // One compiled factory, three executions: 5 rows, then 30, then 3 again.
                    bindInterval("2020-01-01T00", "2020-01-01T05");
                    assertExecution(sql, factory, choice, context, true, 5);
                    bindInterval("2020-01-01T00", "2020-01-02T06");
                    assertExecution(sql, factory, choice, context, false, 8);
                    bindInterval("2020-01-03T10", "2020-01-03T13");
                    assertExecution(sql, factory, choice, context, true, 3);
                    // An interval past the data selects no rows of r, so r is the build, and an empty one.
                    bindInterval("2021-01-01T00", "2021-01-02T00");
                    assertExecution(sql, factory, choice, context, true, 0);
                }
            }
        });
    }

    @Test
    public void testBuildsTheInputWithFewerIntervalRows() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContextImpl context = enabledContext()) {
                for (String join : new String[]{" FROM r JOIN p ON r.plant_id = p.plant_id", " FROM p JOIN r ON r.plant_id = p.plant_id"}) {
                    for (String select : new String[]{KEYED, SCALAR}) {
                        String order = select.equals(KEYED) ? " ORDER BY country" : "";
                        // 4 interval rows against p's 8: build r.
                        assertChoice(select + join + " WHERE r.reading_ts >= '2020-01-01' AND r.reading_ts < '2020-01-01T04:00'" + order,
                                context, true, 4);
                        // 7 against 8: still r.
                        assertChoice(select + join + " WHERE r.reading_ts >= '2020-01-02T01:00' AND r.reading_ts < '2020-01-02T08:00'" + order,
                                context, true, 7);
                        // 8 against 8: a tie keeps the primary, which builds p.
                        assertChoice(select + join + " WHERE r.reading_ts >= '2020-01-02T01:00' AND r.reading_ts < '2020-01-02T09:00'" + order,
                                context, false, 8);
                        // 24 against 8: p.
                        assertChoice(select + join + " WHERE r.reading_ts >= '2020-01-02' AND r.reading_ts < '2020-01-03'" + order,
                                context, false, 8);
                        // An interval that starts before the data and ends inside it.
                        assertChoice(select + join + " WHERE r.reading_ts < '2020-01-01T03:00'" + order, context, true, 3);
                    }
                }
            }
        });
    }

    @Test
    public void testExplain() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContextImpl context = enabledContext()) {
                assertQuery(SCALAR + " FROM r JOIN p ON r.plant_id = p.plant_id"
                        + " WHERE r.reading_ts >= '2020-01-01' AND r.reading_ts < '2020-01-01T04:00'")
                        .withContext(context)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .withPlan("""
                                Hash Join Group By Build Choice
                                  builds: input with fewer rows in its intervals
                                    Primary
                                        Async Hash Join Group By workers: 4
                                          logicalJoinType: inner
                                          physicalJoinType: inner
                                          inputSwapped: false
                                          condition: r.plant_id=p.plant_id
                                          buildStrategy: shared
                                          aggregation: scalar
                                          values: [count(*),sum(r.energy_kwh),sum(p.installed_kwp)]
                                            Probe
                                                PageFrame
                                                    Row forward scan
                                                    Interval forward scan on: r
                                                      intervals: [("2020-01-01T00:00:00.000000Z","2020-01-01T03:59:59.999999Z")]
                                            Build
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: p
                                    Alternate
                                        Async Hash Join Group By workers: 4
                                          logicalJoinType: inner
                                          physicalJoinType: inner
                                          inputSwapped: true
                                          condition: p.plant_id=r.plant_id
                                          buildStrategy: shared
                                          aggregation: scalar
                                          values: [count(*),sum(r.energy_kwh),sum(p.installed_kwp)]
                                            Probe
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: p
                                            Build
                                                PageFrame
                                                    Row forward scan
                                                    Interval forward scan on: r
                                                      intervals: [("2020-01-01T00:00:00.000000Z","2020-01-01T03:59:59.999999Z")]
                                """)
                        .returns("""
                                n\tenergy\tcapacity
                                4\t10.0\t14.0
                                """);
            }
        });
    }

    @Test
    public void testFilterOnTheDimensionBecomesTheProbeFilter() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            for (int jitMode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                try (SqlExecutionContextImpl context = enabledContext()) {
                    context.setJitMode(jitMode);
                    // Each filter runs over the build cursor when its table is the build and inside the
                    // probe when it is the probe, so the orientations swap which one the JIT compiles.
                    String sql = KEYED + " FROM r JOIN p ON r.plant_id = p.plant_id"
                            + " WHERE r.reading_ts >= '2020-01-01' AND r.reading_ts < '2020-01-01T06:00'"
                            + " AND p.installed_kwp > 1 AND r.energy_kwh > 1 ORDER BY country";
                    try (RecordCursorFactory factory = engine.select(sql, context)) {
                        HashJoinGroupByBuildChoiceRecordCursorFactory choice = choice(factory);
                        final boolean isCompiled = JitUtil.isJitSupported() && jitMode == SqlJitMode.JIT_MODE_ENABLED;
                        Assert.assertEquals(isCompiled, choice.getPrimary().usesCompiledFilter());
                        Assert.assertEquals(isCompiled, choice.getAlternate().usesCompiledFilter());
                        Assert.assertEquals(choice.getAlternate().usesCompiledFilter() || choice.getPrimary().usesCompiledFilter(),
                                choice.usesCompiledFilter());
                    }
                    // 6 interval rows against 8, and r's filter leaves 5 of them in the build.
                    assertChoice(sql, context, true, 5);
                }
            }
        });
    }

    @Test
    public void testIntervalsOnBothInputs() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // q carries p's 8 rows once a day for four days, so its plant ids repeat across days.
            execute("CREATE TABLE q (plant_id INT, country SYMBOL, installed_kwp DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO q SELECT p.plant_id, p.country, p.installed_kwp, d.ts FROM p CROSS JOIN"
                    + " (SELECT '2020-01-01'::TIMESTAMP + (x - 1) * 86_400_000_000L ts FROM long_sequence(4)) d");
            try (SqlExecutionContextImpl context = enabledContext()) {
                String select = "SELECT q.country, sum(r.energy_kwh) energy, count(*) n FROM r JOIN q ON r.plant_id = q.plant_id";
                // q has 32 rows, r 96, so the primary builds q. 16 q rows against 10 r rows: build r.
                String rHours = " WHERE r.reading_ts >= '2020-01-01' AND r.reading_ts < '2020-01-01T10:00'";
                assertChoice(select + rHours + " AND q.ts >= '2020-01-02' AND q.ts < '2020-01-04' ORDER BY country",
                        context, true, 10);
                // 8 q rows against 10 r rows: build q.
                assertChoice(select + rHours + " AND q.ts >= '2020-01-03' AND q.ts < '2020-01-04' ORDER BY country",
                        context, false, 8);
                // Only q has an interval, and 16 of its rows are still fewer than r's 96.
                assertChoice(select + " WHERE q.ts >= '2020-01-02' AND q.ts < '2020-01-04' ORDER BY country", context, false, 16);
            }
        });
    }

    @Test
    public void testMemoryLimitFailsEitherOrientationAndReuse() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContextImpl context = enabledContext()) {
                String sql = KEYED + " FROM r JOIN p ON r.plant_id = p.plant_id"
                        + " WHERE r.reading_ts >= $1 AND r.reading_ts < $2 ORDER BY country";
                // The analysis rejects a bind variable without a type, so bind before compiling.
                bindInterval("2020-01-01T00", "2020-01-01T05");
                try (RecordCursorFactory factory = engine.select(sql, context)) {
                    HashJoinGroupByBuildChoiceRecordCursorFactory choice = choice(factory);
                    for (boolean isAlternate : new boolean[]{true, false}) {
                        bindInterval("2020-01-01T00", isAlternate ? "2020-01-01T05" : "2020-01-02T00");
                        String expected = result(factory, context);
                        Assert.assertEquals(isAlternate, choice.isAlternateChosen());
                        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 64);
                        try {
                            result(factory, context);
                            Assert.fail("expected the query memory limit error");
                        } catch (CairoException ex) {
                            Assert.assertTrue(ex.getMessage(), ex.isOutOfMemory());
                        }
                        Assert.assertEquals(isAlternate, choice.isAlternateChosen());
                        for (AsyncHashJoinGroupByRecordCursorFactory fused : new AsyncHashJoinGroupByRecordCursorFactory[]{choice.getPrimary(), choice.getAlternate()}) {
                            Assert.assertEquals(0, fused.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                            Assert.assertNull(fused.getAtom().getFrozenBuild());
                        }
                        Assert.assertNull(context.getMemoryTracker());
                        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0);
                        Assert.assertEquals(expected, result(factory, context));
                    }
                }
            }
        });
    }

    @Test
    public void testOneFactoryWhenTheChoiceCannotChange() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContextImpl context = enabledContext()) {
                String interval = " WHERE r.reading_ts >= '2020-01-01' AND r.reading_ts < '2020-01-01T04:00'";
                // No interval on either input: the table sizes fix the build.
                assertChoice(SCALAR + " FROM r JOIN p ON r.plant_id = p.plant_id WHERE r.energy_kwh < 5", context, false, -1);
                // Outer joins fix the build whatever the intervals select.
                assertChoice(SCALAR + " FROM r LEFT JOIN p ON r.plant_id = p.plant_id" + interval, context, false, -1);
                assertChoice(SCALAR + " FROM p RIGHT JOIN r ON r.plant_id = p.plant_id" + interval, context, false, -1);
                // A VARCHAR column reaches the aggregate only from the probe, so r cannot be the build.
                assertChoice("SELECT count(r.label) labels, count(*) n FROM r JOIN p ON r.plant_id = p.plant_id" + interval,
                        context, false, -1);
            }
        });
    }

    @Test
    public void testParquetPartitions() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("ALTER TABLE r CONVERT PARTITION TO PARQUET WHERE reading_ts < '2020-01-03'");
            try (SqlExecutionContextImpl context = enabledContext()) {
                String join = KEYED + " FROM r JOIN p ON r.plant_id = p.plant_id";
                // Inside one Parquet partition, then across the Parquet and native ones.
                assertChoice(join + " WHERE r.reading_ts >= '2020-01-01T10:00' AND r.reading_ts < '2020-01-01T16:00' ORDER BY country",
                        context, true, 6);
                assertChoice(join + " WHERE r.reading_ts >= '2020-01-02T21:00' AND r.reading_ts < '2020-01-03T03:00' ORDER BY country",
                        context, true, 6);
                assertChoice(join + " WHERE r.reading_ts >= '2020-01-02T12:00' AND r.reading_ts < '2020-01-03T12:00' ORDER BY country",
                        context, false, 8);
            }
        });
    }

    @Test
    public void testSecondCursorClosesTheFirst() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContextImpl context = enabledContext()) {
                String sql = KEYED + " FROM r JOIN p ON r.plant_id = p.plant_id"
                        + " WHERE r.reading_ts >= $1 AND r.reading_ts < $2 ORDER BY country";
                // The analysis rejects a bind variable without a type, so bind before compiling.
                bindInterval("2020-01-01T00", "2020-01-01T05");
                try (RecordCursorFactory factory = engine.select(sql, context)) {
                    // QueryProgress hands back its open cursor without asking again, so drive the choice itself.
                    HashJoinGroupByBuildChoiceRecordCursorFactory choice = choice(factory);
                    bindInterval("2020-01-01T00", "2020-01-01T05");
                    RecordCursor first = choice.getCursor(context);
                    Assert.assertTrue(choice.isAlternateChosen());
                    Assert.assertTrue(first.hasNext());
                    Assert.assertNotNull(choice.getAlternate().getAtom().getFrozenBuild());
                    // The next execution runs the other orientation; the first one's build goes with its cursor.
                    bindInterval("2020-01-01T00", "2020-01-03T00");
                    try (RecordCursor second = choice.getCursor(context)) {
                        Assert.assertFalse(choice.isAlternateChosen());
                        Assert.assertNull(choice.getAlternate().getAtom().getFrozenBuild());
                        Assert.assertTrue(second.hasNext());
                    } finally {
                        first.close();
                    }
                }
            }
        });
    }

    @Test
    public void testSymbolKeys() throws Exception {
        assertMemoryLeak(() -> {
            // r's dictionary starts at P1 and p's at P0, so the two number their texts differently, and p lacks P7.
            createTables();
            try (SqlExecutionContextImpl context = enabledContext()) {
                String select = KEYED + " FROM r JOIN p ON r.s = p.s";
                assertChoice(select + FIVE_HOURS + " ORDER BY country", context, true, 5);
                assertChoice(select + ONE_DAY + " ORDER BY country", context, false, 8);
                // A composite key with a SYMBOL pair stages its key through the translating record.
                String composite = KEYED + " FROM r JOIN p ON r.s = p.s AND r.plant_id = p.plant_id";
                assertChoice(composite + FIVE_HOURS + " ORDER BY country", context, true, 5);
                assertChoice(composite + ONE_DAY + " ORDER BY country", context, false, 8);
            }
        });
    }

    // Hour-precision arguments, such as 2020-01-01T05.
    private static void bindInterval(String lo, String hi) throws Exception {
        bindVariableService.setTimestamp(0, MicrosFormatUtils.parseTimestamp(lo + ":00:00.000000Z"));
        bindVariableService.setTimestamp(1, MicrosFormatUtils.parseTimestamp(hi + ":00:00.000000Z"));
    }

    @Nullable
    private static HashJoinGroupByBuildChoiceRecordCursorFactory findChoice(RecordCursorFactory factory) {
        // QueryProgress and projections wrap the fused plan.
        for (RecordCursorFactory current = factory; current != null; current = current.getBaseFactory()) {
            if (current instanceof HashJoinGroupByBuildChoiceRecordCursorFactory choice) {
                return choice;
            }
        }
        return null;
    }

    private static HashJoinGroupByBuildChoiceRecordCursorFactory choice(RecordCursorFactory factory) {
        HashJoinGroupByBuildChoiceRecordCursorFactory choice = findChoice(factory);
        Assert.assertNotNull(choice);
        return choice;
    }

    private static String plan(RecordCursorFactory factory, SqlExecutionContext context) {
        TextPlanSink sink = new TextPlanSink();
        sink.of(factory, context);
        StringSink lines = new StringSink();
        for (int i = 1; i <= sink.getLineCount(); i++) {
            lines.put(sink.getLine(i)).put('\n');
        }
        return lines.toString();
    }

    private static String result(RecordCursorFactory factory, SqlExecutionContext context) throws Exception {
        try (RecordCursor cursor = factory.getCursor(context)) {
            StringSink sink = new StringSink();
            CursorPrinter.println(cursor, factory.getMetadata(), sink, true, true);
            return sink.toString();
        }
    }

    /**
     * Compiles the query with the fused plan on and off and compares the rows twice. With
     * {@code isChoice}, asserts that the fused plan pairs two orientations, that each execution
     * built the input it names and that the build held {@code buildRows} rows; without it,
     * asserts one fused factory and no choice. A negative {@code buildRows} skips that check.
     */
    private void assertChoice(String sql, SqlExecutionContextImpl context, boolean isAlternate, long buildRows) throws Exception {
        final boolean isChoice = buildRows > -1;
        final String expected = ordinary(sql, context);
        try (RecordCursorFactory factory = engine.select(sql, context)) {
            final String plan = plan(factory, context);
            Assert.assertTrue(sql + '\n' + plan, plan.contains("Hash Join Group By"));
            Assert.assertEquals(sql + '\n' + plan, isChoice, plan.contains(BUILD_CHOICE));
            HashJoinGroupByBuildChoiceRecordCursorFactory choice = findChoice(factory);
            Assert.assertEquals(sql, isChoice, choice != null);
            for (int i = 0; i < 2; i++) {
                if (choice == null) {
                    Assert.assertEquals(sql, expected, result(factory, context));
                } else {
                    assertExecution(sql, factory, choice, context, isAlternate, buildRows, expected);
                }
            }
        }
    }

    private void assertExecution(
            String sql,
            RecordCursorFactory factory,
            HashJoinGroupByBuildChoiceRecordCursorFactory choice,
            SqlExecutionContextImpl context,
            boolean isAlternate,
            long buildRows
    ) throws Exception {
        assertExecution(sql, factory, choice, context, isAlternate, buildRows, ordinary(sql, context));
    }

    private void assertExecution(
            String sql,
            RecordCursorFactory factory,
            HashJoinGroupByBuildChoiceRecordCursorFactory choice,
            SqlExecutionContextImpl context,
            boolean isAlternate,
            long buildRows,
            String expected
    ) throws Exception {
        try (RecordCursor cursor = factory.getCursor(context)) {
            Assert.assertEquals(sql, isAlternate, choice.isAlternateChosen());
            AsyncHashJoinGroupByRecordCursorFactory chosen = isAlternate ? choice.getAlternate() : choice.getPrimary();
            StringSink sink = new StringSink();
            CursorPrinter.println(cursor, factory.getMetadata(), sink, true, true);
            Assert.assertEquals(sql, expected, sink.toString());
            // The build stays published until the cursor closes.
            Assert.assertEquals(sql, buildRows, chosen.getAtom().getFrozenBuild().getRowCount());
            Assert.assertNull(sql, (isAlternate ? choice.getPrimary() : choice.getAlternate()).getAtom().getFrozenBuild());
        }
    }

    private void createTables() throws Exception {
        execute("""
                CREATE TABLE r (
                    plant_id INT, reading_ts TIMESTAMP, energy_kwh DOUBLE, irradiance_wm2 DOUBLE, s SYMBOL, label VARCHAR
                ) TIMESTAMP(reading_ts) PARTITION BY DAY
                """);
        execute("CREATE TABLE p (plant_id INT, country SYMBOL, installed_kwp DOUBLE, s SYMBOL)");
        execute("""
                INSERT INTO r
                SELECT (x % 8)::INT, '2020-01-01'::TIMESTAMP + (x - 1) * 3_600_000_000L, x::DOUBLE, (x * 10)::DOUBLE,
                    'P' || (x % 8), 'L' || (x % 8)
                FROM long_sequence(96)
                """);
        execute("""
                INSERT INTO p VALUES
                    (0, 'ES', 1, 'P0'), (1, 'ES', 2, 'P1'), (2, 'IT', 3, 'P2'), (3, 'IT', 4, 'P3'),
                    (4, 'DE', 5, 'P4'), (5, NULL, 6, 'P5'), (6, 'ES', NULL, 'P6'), (NULL, 'FR', 8, NULL)
                """);
    }

    private SqlExecutionContextImpl enabledContext() {
        SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 4)
                .with(AllowAllSecurityContext.INSTANCE, bindVariableService, null, -1, null);
        context.setParallelGroupByEnabled(true);
        context.setParallelHashJoinGroupByEnabled(true);
        return context;
    }

    private String ordinary(String sql, SqlExecutionContextImpl context) throws Exception {
        context.setParallelHashJoinGroupByEnabled(false);
        try (RecordCursorFactory baseline = engine.select(sql, context)) {
            Assert.assertFalse(sql, plan(baseline, context).contains("Hash Join Group By"));
            return result(baseline, context);
        } finally {
            context.setParallelHashJoinGroupByEnabled(true);
        }
    }
}
