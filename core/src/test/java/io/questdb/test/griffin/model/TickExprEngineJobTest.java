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

package io.questdb.test.griffin.model;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.NanosTimestampDriver;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.lv.LiveViewRefreshSqlExecutionContext;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.griffin.model.CompiledTickExpression;
import io.questdb.griffin.model.IntervalOperation;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.griffin.model.RuntimeIntervalModelBuilder;
import io.questdb.griffin.model.RuntimeIntrinsicIntervalModel;
import io.questdb.griffin.model.TimestampMonotonicInverter;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.LongList;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.test.griffin.GriffinParserTestUtils.intervalToString;

/**
 * The SQL that the engine runs on its own, such as WAL apply, has no caller to report a failure to,
 * see {@link io.questdb.griffin.SqlExecutionContext#isBackgroundJob()}. There a tick expression
 * evaluates without the checks that depend on "now": a range that starts after it ends adds no
 * interval and the interval limit does not apply. An expression that does not parse acts as a null
 * value. A query that a user runs keeps failing on all of these.
 */
public class TickExprEngineJobTest extends AbstractCairoTest {
    private static final String LIMIT_ERROR = "produces too many intervals (max ";
    private static final Log LOG = LogFactory.getLog(TickExprEngineJobTest.class);
    private static final String RANGE_ORDER_ERROR = "Invalid date range: start is after end";
    private static final TimestampDriver[] TIMESTAMP_DRIVERS = {MicrosTimestampDriver.INSTANCE, NanosTimestampDriver.INSTANCE};
    private final LongList intervals = new LongList();
    private final StringSink sink = new StringSink();
    // what the fuzz test met, so that it can tell that it reached the paths it is for
    private int fuzzNotParsedCount;
    private int fuzzRangeOrderCount;
    private int fuzzUnknownUnitNotParsedCount;

    /**
     * A context for SQL that the engine runs on its own, such as WAL apply: a tick expression
     * evaluates in it without the checks that depend on "now", see the class comment.
     */
    public static SqlExecutionContextImpl newEngineJobContext() {
        final SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1) {
            @Override
            public boolean isBackgroundJob() {
                return true;
            }
        };
        context.with(engine.getConfiguration().getFactoryProvider().getSecurityContextFactory().getRootContext(), new BindVariableServiceImpl(configuration));
        return context;
    }

    @Test
    public void testEngineContextsTreatRowValueThatDoesNotParseAsNull() throws Exception {
        // The live view refresh and the view compiler run SQL on the engine's behalf too, as WAL
        // apply and the materialized view refresh do, which WalTickExprTest and MatViewTickExprTest
        // cover
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, s STRING) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO base VALUES
                    ('2024-01-02T08:00:00.000000Z', '2024-01-02'),
                    ('2024-01-02T09:00:00.000000Z', 'garbage')
                    """);
            drainWalQueue();
            try (
                    SqlExecutionContextImpl liveViewRefresh = new LiveViewRefreshSqlExecutionContext(engine, 1);
                    SqlExecutionContextImpl viewCompiler = engine.createViewCompilerContext(1)
            ) {
                for (SqlExecutionContextImpl context : new SqlExecutionContextImpl[]{liveViewRefresh, viewCompiler}) {
                    // IN matches the row whose value parses, NOT IN the row whose value does not
                    for (String query : new String[]{"SELECT ts FROM base WHERE ts IN s", "SELECT ts FROM base WHERE ts NOT IN s"}) {
                        try (RecordCursorFactory factory = select(query, context)) {
                            assertFactory(factory)
                                    .withContext(context)
                                    .timestamp("ts")
                                    .returns(query.contains("NOT")
                                            ? "ts\n2024-01-02T09:00:00.000000Z\n"
                                            : "ts\n2024-01-02T08:00:00.000000Z\n");
                        }
                    }
                }
            }
            // a user's query fails on the value
            assertExceptionNoLeakCheck("SELECT ts FROM base WHERE ts IN s", 32, "Invalid date: garbage");
        });
    }

    @Test
    public void testEngineJobCompilesWithoutValidating() throws Exception {
        // The engine compiles SQL on its own that the statement of a user validated as it compiled,
        // against another "now" possibly, so it does not validate it again: a literal that does not
        // parse, or that has too many intervals, compiles there, and evaluates as the class comment
        // describes
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t AS (
                        SELECT timestamp_sequence('2024-01-01T00:00:00.000000Z', 3_600_000_000L) ts
                        FROM long_sequence(96)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T03:00:00.000000Z"));
            try (SqlExecutionContextImpl engineJob = newEngineJobContext()) {
                engineJob.setNowAndFixClock(MicrosFormatUtils.parseTimestamp("2024-01-02T03:00:00.000000Z"), ColumnType.TIMESTAMP_MICRO);
                final String malformed = "'[$today,garbage]T[09:00,10:00]'";
                final String overLimit = "'[$today..$today + 2000d]T09:00'";
                // a duration unit that the timestamp drivers do not add
                final String unknownUnit = "'$today;1x'";
                final String[][] queries = {
                        // a null value: IN matches nothing, NOT IN everything
                        {"SELECT count() FROM t WHERE ts IN " + malformed, "0"},
                        {"SELECT count() FROM t WHERE ts NOT IN " + malformed, "96"},
                        {"SELECT count() FROM t WHERE ts IN " + malformed + " OR ts = '2024-01-03T09:00:00.000000Z'", "1"},
                        // 09:00 on each day from 2024-01-02 on
                        {"SELECT count() FROM t WHERE ts IN " + overLimit, "3"},
                        // the same, pushed below the dateadd(): 10:00 on each day from 2024-01-02 on
                        {"SELECT count() FROM (SELECT dateadd('h', -1, ts) tt FROM t) WHERE tt IN " + overLimit, "3"},
                        // and the offset of the scan that the optimiser writes for a constant bound,
                        // written by hand around the date variable: the same rows
                        {"SELECT count() FROM t WHERE and_offset(ts IN " + overLimit + ", 'h', 1)", "3"},
                        // a null value, the same as the malformed expression
                        {"SELECT count() FROM t WHERE ts IN " + unknownUnit, "0"},
                        {"SELECT count() FROM t WHERE ts NOT IN " + unknownUnit, "96"},
                        {"SELECT count() FROM t WHERE ts IN " + unknownUnit + " OR ts = '2024-01-03T09:00:00.000000Z'", "1"}
                };
                for (String[] query : queries) {
                    try (RecordCursorFactory factory = select(query[0], engineJob)) {
                        assertFactory(factory)
                                .withContext(engineJob)
                                .noRandomAccess()
                                .expectSize()
                                .returns("count\n" + query[1] + "\n");
                    }
                }
                // a user's query does not compile
                for (String query : new String[]{queries[0][0], queries[3][0], queries[4][0], queries[5][0]}) {
                    try (RecordCursorFactory ignore = select(query)) {
                        Assert.fail(query);
                    } catch (SqlException e) {
                        Assert.assertEquals(query, query.indexOf("'["), e.getPosition());
                    }
                }
                for (String query : new String[]{queries[6][0], queries[7][0], queries[8][0]}) {
                    try (RecordCursorFactory ignore = select(query)) {
                        Assert.fail(query);
                    } catch (SqlException e) {
                        TestUtils.assertEquals(query, "Invalid duration unit: x", e.getFlyweightMessage());
                        Assert.assertEquals(query, query.indexOf(unknownUnit), e.getPosition());
                    }
                }
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testEngineJobFuzz() throws Exception {
        // Relaxing a check changes the result only where that check fires, and an engine job
        // evaluates the compiled form of a literal the way the parser does without the checks
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            final StringSink expression = new StringSink();
            try (SqlExecutionContextImpl engineJob = newEngineJobContext()) {
                for (TimestampDriver driver : TIMESTAMP_DRIVERS) {
                    for (int i = 0; i < 2_000; i++) {
                        expression.clear();
                        final boolean isMalformed = TickExprTest.appendRandomTickExpr(rnd, expression);
                        for (String now : TickExprTest.FUZZ_NOWS) {
                            assertChecksAgree(driver, engineJob, expression.toString(), isMalformed, driver.parseFloorLiteral(now));
                        }
                    }
                }
            }
            // the generator makes ranges that start after they end, malformed expressions that
            // only the form parsing on every evaluation lets through, and durations with a unit that
            // the timestamp drivers do not add
            Assert.assertTrue(fuzzRangeOrderCount > 0);
            Assert.assertTrue(fuzzNotParsedCount > 0);
            Assert.assertTrue(fuzzUnknownUnitNotParsedCount > 0);
        });
    }

    @Test
    public void testExpressionThatDoesNotParseActsAsNull() throws Exception {
        assertMemoryLeak(() -> {
            try (SqlExecutionContextImpl engineJob = newEngineJobContext()) {
                for (TimestampDriver driver : TIMESTAMP_DRIVERS) {
                    // forms that parse on every evaluation, which a WAL may hold from before SQL
                    // validated them as it compiled
                    for (String expression : new String[]{
                            "[$today,garbage]T[09:00,10:00]",
                            "$todayT[09,14]:[00,99]",
                            "[$today, 2024-01-03]T[09:00@Bogus/Zone]"
                    }) {
                        try (CompiledTickExpression compiled = compile(driver, expression)) {
                            engineJob.setNowAndFixClock(driver.parseFloorLiteral("2024-01-02T03:00:00.000000Z"), driver.getTimestampType());
                            compiled.init(null, engineJob);
                            intervals.clear();
                            // it appends no interval, and what the caller appended before stays
                            intervals.add(1L, 2L);
                            compiled.evaluate(intervals);
                            Assert.assertEquals(expression, 2, intervals.size());
                            Assert.assertEquals(1, intervals.getQuick(0));
                            Assert.assertEquals(2, intervals.getQuick(1));
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testIntervalLimitDoesNotApply() throws Exception {
        assertMemoryLeak(() -> {
            try (SqlExecutionContextImpl engineJob = newEngineJobContext()) {
                for (TimestampDriver driver : TIMESTAMP_DRIVERS) {
                    final long now = driver.parseFloorLiteral("2024-01-02T03:00:00.000000Z");
                    // the compiled form: 2,001 days
                    assertEngineJobIntervalCount(driver, engineJob, now, "[$today..$today + 2000d]T09:00", 2001);
                    // a time list on each of 601 days: 1,202 intervals
                    assertEngineJobIntervalCount(driver, engineJob, now, "$today..$today + 600dT[09:00,15:00]", 1202);
                    // the form that parses on every evaluation: 601 days and one more, of 2 slots
                    assertEngineJobIntervalCount(driver, engineJob, now, "[$today..$today + 600d, 2024-01-01]T[09:00,10:00]", 1204);
                }
            }
        });
    }

    @Test
    public void testIntervalScanOfExpressionThatDoesNotParse() throws Exception {
        // A WAL written before SQL validated literals as it compiled can hold one that does not
        // parse. An engine job evaluates it as a null value in the interval scan: an intersection
        // with it is empty, a subtraction of it leaves every timestamp, and a union with it keeps
        // the other side. The inverter of a monotonic function of the timestamp adds no interval.
        assertMemoryLeak(() -> {
            final String malformed = "[$today,garbage]T[09:00,10:00]";
            final TimestampDriver driver = MicrosTimestampDriver.INSTANCE;
            final RuntimeIntervalModelBuilder builder = new RuntimeIntervalModelBuilder();
            try (SqlExecutionContextImpl engineJob = newEngineJobContext()) {
                engineJob.setNowAndFixClock(driver.parseFloorLiteral("2024-01-02T03:00:00.000000Z"), driver.getTimestampType());

                builder.of(ColumnType.TIMESTAMP, PartitionBy.DAY, configuration, false);
                builder.intersectIntervals(malformed, 0, malformed.length(), 0);
                assertIntervalModel(builder, engineJob);

                builder.of(ColumnType.TIMESTAMP, PartitionBy.DAY, configuration, false);
                builder.intersectIntervals("$today", 0, 6, 0);
                builder.subtractIntervals(malformed, 0, malformed.length(), 0);
                assertIntervalModel(builder, engineJob, driver.parseFloorLiteral("2024-01-02T00:00:00.000000Z"), driver.parseFloorLiteral("2024-01-02T23:59:59.999999Z"));

                builder.of(ColumnType.TIMESTAMP, PartitionBy.DAY, configuration, false);
                builder.subtractIntervals(malformed, 0, malformed.length(), 0);
                assertIntervalModel(builder, engineJob, Long.MIN_VALUE, Long.MAX_VALUE);

                builder.of(ColumnType.TIMESTAMP, PartitionBy.DAY, configuration, false);
                builder.unionIntervals("$tomorrow", 0, 9, 0);
                builder.unionIntervals(malformed, 0, malformed.length(), 0);
                assertIntervalModel(builder, engineJob, driver.parseFloorLiteral("2024-01-03T00:00:00.000000Z"), driver.parseFloorLiteral("2024-01-03T23:59:59.999999Z"));

                // an OR intersects with its first branch, and unions the others
                builder.of(ColumnType.TIMESTAMP, PartitionBy.DAY, configuration, false);
                builder.intersectIntervals(malformed, 0, malformed.length(), 0);
                builder.unionIntervals("$tomorrow", 0, 9, 0);
                assertIntervalModel(builder, engineJob, driver.parseFloorLiteral("2024-01-03T00:00:00.000000Z"), driver.parseFloorLiteral("2024-01-03T23:59:59.999999Z"));

                final TimestampFunction head = new TimestampFunction(ColumnType.TIMESTAMP) {
                    @Override
                    public long getTimestamp(Record rec) {
                        return 0;
                    }
                };
                try (TimestampMonotonicInverter inverter = new TimestampMonotonicInverter(head, compile(driver, malformed), driver)) {
                    inverter.init(null, engineJob);
                    intervals.clear();
                    inverter.evaluate(intervals);
                    Assert.assertEquals(0, intervals.size());
                }
            } finally {
                builder.clear();
            }
        });
    }

    @Test
    public void testInvertedRangeAddsNoInterval() throws Exception {
        // At 03:00 a range from 09:30 to now starts after it ends. In an engine job it adds no
        // interval, and the other elements of the expression keep theirs.
        assertMemoryLeak(() -> {
            try (SqlExecutionContextImpl engineJob = newEngineJobContext()) {
                for (TimestampDriver driver : TIMESTAMP_DRIVERS) {
                    final long now = driver.parseFloorLiteral("2024-01-02T03:00:00.000000Z");
                    final long tomorrow = driver.parseFloorLiteral("2024-01-03T00:00:00.000000Z");
                    final long nineAm = driver.parseFloorLiteral("2024-01-03T09:00:00.000000Z");
                    final long tenAm = driver.parseFloorLiteral("2024-01-03T10:00:00.000000Z");
                    final long minute = driver.fromMinutes(1) - 1;
                    // the compiled form
                    assertEngineJobIntervals(driver, engineJob, now, "$today + 570m..$now");
                    assertEngineJobIntervals(driver, engineJob, now, "[$tomorrow..$today]");
                    assertEngineJobIntervals(driver, engineJob, now, "[$now..$now - 1h]");
                    assertEngineJobIntervals(driver, engineJob, now, "[$tomorrow..$today, $tomorrow]", tomorrow, driver.endOfDay(tomorrow));
                    assertEngineJobIntervals(driver, engineJob, now, "[$today + 570m..$now, $tomorrow]T09:00", nineAm, nineAm + minute);
                    // the intervals that the caller appended before stay, as when an interval scan
                    // evaluates the expression after another condition
                    try (CompiledTickExpression compiled = compile(driver, "$now..$now-1h")) {
                        engineJob.setNowAndFixClock(now, driver.getTimestampType());
                        compiled.init(null, engineJob);
                        intervals.clear();
                        intervals.add(1L, 2L);
                        compiled.evaluate(intervals);
                        Assert.assertEquals(2, intervals.size());
                        Assert.assertEquals(1, intervals.getQuick(0));
                        Assert.assertEquals(2, intervals.getQuick(1));
                    }
                    // the form that parses on every evaluation
                    assertEngineJobIntervals(
                            driver,
                            engineJob,
                            now,
                            "[$tomorrow..$today, 2024-01-03]T[09:00,10:00]",
                            nineAm, nineAm + minute, tenAm, tenAm + minute
                    );
                }
            }
        });
    }

    @Test
    public void testUnknownDurationUnitActsAsNull() throws Exception {
        // A WAL may hold a literal with a duration unit that the timestamp drivers do not add from
        // before SQL rejected it as it compiled. An engine job evaluates it as a null value, and a
        // user's query fails on it.
        assertMemoryLeak(() -> {
            try (
                    SqlExecutionContextImpl engineJob = newEngineJobContext();
                    SqlExecutionContextImpl user = new SqlExecutionContextImpl(engine, 1)
            ) {
                for (TimestampDriver driver : TIMESTAMP_DRIVERS) {
                    final long now = driver.parseFloorLiteral("2024-01-02T03:00:00.000000Z");
                    engineJob.setNowAndFixClock(now, driver.getTimestampType());
                    user.setNowAndFixClock(now, driver.getTimestampType());
                    for (String[] expression : new String[][]{
                            {"$today;1x", "x"},
                            {"$now;1H", "H"},
                            {"[$today]T09:00;2D", "D"},
                            {"[$today, $tomorrow]T[09:00,10:00]@Europe/London;1h1S", "S"}
                    }) {
                        try (CompiledTickExpression compiled = compile(driver, expression[0])) {
                            compiled.init(null, engineJob);
                            intervals.clear();
                            // it appends no interval, and what the caller appended before stays
                            intervals.add(1L, 2L);
                            compiled.evaluate(intervals);
                            Assert.assertEquals(expression[0], 2, intervals.size());
                            Assert.assertEquals(1, intervals.getQuick(0));
                            Assert.assertEquals(2, intervals.getQuick(1));
                            compiled.init(null, user);
                            intervals.clear();
                            try {
                                compiled.evaluate(intervals);
                                Assert.fail(expression[0]);
                            } catch (CairoException e) {
                                TestUtils.assertEquals(expression[0], "Invalid duration unit: " + expression[1], e.getFlyweightMessage());
                            }
                            Assert.assertEquals(expression[0], 0, intervals.size());
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testUserQueryChecksAfterEngineJob() throws Exception {
        // a compiled expression that an engine job evaluated fails again when a user's query
        // evaluates it, as the context of each evaluation decides the checks
        assertMemoryLeak(() -> {
            try (
                    SqlExecutionContextImpl engineJob = newEngineJobContext();
                    SqlExecutionContextImpl user = new SqlExecutionContextImpl(engine, 1)
            ) {
                for (TimestampDriver driver : TIMESTAMP_DRIVERS) {
                    final long now = driver.parseFloorLiteral("2024-01-02T03:00:00.000000Z");
                    engineJob.setNowAndFixClock(now, driver.getTimestampType());
                    user.setNowAndFixClock(now, driver.getTimestampType());
                    for (String expression : new String[]{"$today + 570m..$now", "[$tomorrow..$today, 2024-01-03]T[09:00,10:00]"}) {
                        try (CompiledTickExpression compiled = compile(driver, expression)) {
                            compiled.init(null, engineJob);
                            intervals.clear();
                            compiled.evaluate(intervals);
                            compiled.init(null, user);
                            intervals.clear();
                            try {
                                compiled.evaluate(intervals);
                                Assert.fail(expression);
                            } catch (CairoException e) {
                                TestUtils.assertContains(e.getFlyweightMessage(), RANGE_ORDER_ERROR);
                            }
                        }
                    }
                    try (CompiledTickExpression compiled = compile(driver, "[$today..$today + 2000d]T09:00")) {
                        compiled.init(null, engineJob);
                        intervals.clear();
                        compiled.evaluate(intervals);
                        Assert.assertEquals(2 * 2001, intervals.size());
                        compiled.init(null, user);
                        intervals.clear();
                        try {
                            compiled.evaluate(intervals);
                            Assert.fail();
                        } catch (CairoException e) {
                            TestUtils.assertContains(e.getFlyweightMessage(), "Date range expansion produces too many intervals (max 1024)");
                        }
                    }
                }
            }
        });
    }

    private static boolean isError(String result) {
        return result.startsWith("error: ");
    }

    /**
     * Checks one expression at one "now": the parser with fewer checks agrees with the parser with
     * all of them wherever those succeed or fail on a check that the others also run, and an engine
     * job evaluates the compiled form to what the parser returns without any check.
     */
    private void assertChecksAgree(
            TimestampDriver driver,
            SqlExecutionContextImpl engineJob,
            String expression,
            boolean isMalformed,
            long now
    ) {
        final String strict = parse(driver, expression, now, IntervalUtils.TICK_CHECK_ALL);
        final String withoutRangeOrder = parse(driver, expression, now, IntervalUtils.TICK_CHECK_INTERVAL_LIMIT);
        final String unchecked = parse(driver, expression, now, 0);
        final String context = expression + " at " + now;
        if (!isError(strict)) {
            Assert.assertEquals(context, strict, withoutRangeOrder);
            Assert.assertEquals(context, strict, unchecked);
        } else if (strict.contains(RANGE_ORDER_ERROR)) {
            fuzzRangeOrderCount++;
            Assert.assertFalse(context, withoutRangeOrder.contains(RANGE_ORDER_ERROR));
            Assert.assertFalse(context, unchecked.contains(RANGE_ORDER_ERROR));
        } else if (strict.contains(LIMIT_ERROR)) {
            Assert.assertEquals(context, strict, withoutRangeOrder);
            Assert.assertFalse(context, unchecked.contains(LIMIT_ERROR));
        } else {
            // an expression that does not parse fails whatever the checks
            Assert.assertEquals(context, strict, withoutRangeOrder);
            Assert.assertEquals(context, strict, unchecked);
        }

        final CompiledTickExpression compiled;
        try {
            compiled = IntervalUtils.compileTickExpr(driver, configuration, expression, 0, expression.length(), 0);
        } catch (SqlException | CairoException e) {
            // SQL rejects it as it compiles
            return;
        }
        try (compiled) {
            engineJob.setNowAndFixClock(now, driver.getTimestampType());
            compiled.init(null, engineJob);
            intervals.clear();
            // an engine job never fails on a tick expression
            compiled.evaluate(intervals);
            if (isError(unchecked)) {
                fuzzNotParsedCount++;
                if (!isMalformed) {
                    // a well-formed expression that does not parse, such as one with a duration unit
                    // that the timestamp drivers do not add, acts as a null value
                    Assert.assertEquals(context, 0, intervals.size());
                    if (TickExprTest.hasUnknownDurationUnit(expression)) {
                        fuzzUnknownUnitNotParsedCount++;
                    }
                }
            } else if (!isMalformed) {
                Assert.assertEquals(context, unchecked, intervalToString(driver, intervals).toString());
            }
        }
    }

    private void assertEngineJobIntervalCount(TimestampDriver driver, SqlExecutionContextImpl engineJob, long now, String expression, int expectedCount) throws SqlException {
        try (CompiledTickExpression compiled = compile(driver, expression)) {
            engineJob.setNowAndFixClock(now, driver.getTimestampType());
            compiled.init(null, engineJob);
            intervals.clear();
            compiled.evaluate(intervals);
            Assert.assertEquals(expression, expectedCount, intervals.size() / 2);
        }
    }

    private void assertEngineJobIntervals(TimestampDriver driver, SqlExecutionContextImpl engineJob, long now, String expression, long... expected) throws SqlException {
        try (CompiledTickExpression compiled = compile(driver, expression)) {
            engineJob.setNowAndFixClock(now, driver.getTimestampType());
            compiled.init(null, engineJob);
            intervals.clear();
            compiled.evaluate(intervals);
            final LongList expectedIntervals = new LongList();
            for (long value : expected) {
                expectedIntervals.add(value);
            }
            TestUtils.assertEquals(expression, intervalToString(driver, expectedIntervals), intervalToString(driver, intervals));
        }
    }

    private void assertIntervalModel(RuntimeIntervalModelBuilder builder, SqlExecutionContextImpl engineJob, long... expected) throws SqlException {
        try (RuntimeIntrinsicIntervalModel model = builder.build()) {
            final LongList actual = model.calculateIntervals(engineJob);
            Assert.assertEquals(expected.length, actual.size());
            for (int i = 0; i < expected.length; i++) {
                Assert.assertEquals(expected[i], actual.getQuick(i));
            }
        } finally {
            builder.clear();
        }
    }

    private CompiledTickExpression compile(TimestampDriver driver, String expression) throws SqlException {
        return IntervalUtils.compileTickExpr(driver, configuration, expression, 0, expression.length(), 0);
    }

    private String parse(TimestampDriver driver, String expression, long now, int checks) {
        final LongList out = new LongList();
        try {
            IntervalUtils.parseTickExpr(driver, configuration, expression, 0, expression.length(), 0, out, IntervalOperation.INTERSECT, sink, true, now, checks);
            return intervalToString(driver, out).toString();
        } catch (SqlException e) {
            return "error: " + e.getFlyweightMessage();
        }
    }
}
