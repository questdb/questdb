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

import io.questdb.DefaultFactoryProvider;
import io.questdb.FactoryProvider;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.NanosTimestampDriver;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TickCalendarServiceFactory;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.lv.LiveViewRefreshSqlExecutionContext;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.griffin.model.CompiledTickExpression;
import io.questdb.griffin.model.DateVariableExpr;
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
import io.questdb.test.cairo.TestTickCalendarService;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
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
    private final LongList compileProbeIntervals = new LongList();
    private final LongList intervals = new LongList();
    private final StringSink sink = new StringSink();
    // what the fuzz test met, so that it can tell that it reached the paths it is for
    private int fuzzMalformedElementZoneRangeOrderCount;
    private int fuzzMalformedElementZoneWithoutDayCount;
    private int fuzzNotParsedCount;
    private int fuzzRangeOrderCount;
    private int fuzzRejectedSuffixCount;
    private int fuzzUnknownDayFilterOnReversedRangeCount;
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
    public void testCompileProbeIsLenientAndMetadataProbeIsNot() throws Exception {
        // A cursor that the compiler holds to inspect a query evaluates a tick expression as an
        // engine job does: at 03:00 the range from 09:30 to "now" adds no interval. A metadata
        // probe, which view auditing reads, does not change the checks of a user's context.
        assertMemoryLeak(() -> {
            try (SqlExecutionContextImpl user = new SqlExecutionContextImpl(engine, 1)) {
                user.with(securityContext, bindVariableService, null);
                for (TimestampDriver driver : TIMESTAMP_DRIVERS) {
                    user.setNowAndFixClock(driver.parseFloorLiteral("2024-01-02T03:00:00.000000Z"), driver.getTimestampType());
                    try (CompiledTickExpression compiled = compile(driver, "$today + 570m..$now")) {
                        user.setCompileProbe(true);
                        compiled.init(null, user);
                        intervals.clear();
                        // it appends no interval, and what the caller appended before stays
                        intervals.add(1L, 2L);
                        compiled.evaluate(intervals);
                        Assert.assertEquals(2, intervals.size());
                        Assert.assertEquals(1, intervals.getQuick(0));
                        Assert.assertEquals(2, intervals.getQuick(1));
                        user.setCompileProbe(false);

                        for (boolean isMetadataProbe : new boolean[]{true, false}) {
                            user.setMetadataProbe(isMetadataProbe);
                            compiled.init(null, user);
                            intervals.clear();
                            try {
                                compiled.evaluate(intervals);
                                Assert.fail("metadata probe: " + isMetadataProbe);
                            } catch (CairoException e) {
                                TestUtils.assertEquals(RANGE_ORDER_ERROR, e.getFlyweightMessage());
                                Assert.assertEquals(0, e.getPosition());
                            }
                        }
                    }
                }
            }
        });
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
        // Relaxing a check changes the result only where that check fires, an engine job
        // evaluates the compiled form of a literal the way the parser does without the checks,
        // and a cursor that the compiler holds to inspect a query evaluates it as the engine job
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            final StringSink expression = new StringSink();
            try (
                    SqlExecutionContextImpl engineJob = newEngineJobContext();
                    SqlExecutionContextImpl compileProbe = new SqlExecutionContextImpl(engine, 1)
            ) {
                compileProbe.with(securityContext, bindVariableService, null);
                compileProbe.setCompileProbe(true);
                for (TimestampDriver driver : TIMESTAMP_DRIVERS) {
                    // enough expressions for each kind that the assertions below count to come up
                    // in every run, the rarest, a range of business days without a day with a
                    // timezone that does not parse, some 40 times
                    for (int i = 0; i < 6_000; i++) {
                        expression.clear();
                        final boolean isMalformed = TickExprTest.appendRandomTickExpr(rnd, expression);
                        for (String now : TickExprTest.FUZZ_NOWS) {
                            assertChecksAgree(driver, engineJob, compileProbe, expression.toString(), isMalformed, driver.parseFloorLiteral(now));
                        }
                    }
                }
            }
            // the generator makes ranges that start after they end, malformed expressions that
            // only the form parsing on every evaluation lets through, durations with a unit that
            // the timestamp drivers do not add, texts after the first ';' that do not parse, and
            // timezones of an element that do not parse on ranges that start after they end and
            // on ranges of business days that hold no business day, and day filters of an element
            // that do not parse on ranges between two points in time that start after they end
            LOG.info().$("engine job fuzz done [rangeOrder=").$(fuzzRangeOrderCount)
                    .$(", notParsed=").$(fuzzNotParsedCount)
                    .$(", unknownUnitNotParsed=").$(fuzzUnknownUnitNotParsedCount)
                    .$(", rejectedSuffix=").$(fuzzRejectedSuffixCount)
                    .$(", malformedElementZoneRangeOrder=").$(fuzzMalformedElementZoneRangeOrderCount)
                    .$(", malformedElementZoneWithoutDay=").$(fuzzMalformedElementZoneWithoutDayCount)
                    .$(", unknownDayFilterOnReversedRange=").$(fuzzUnknownDayFilterOnReversedRangeCount)
                    .I$();
            Assert.assertTrue(fuzzRangeOrderCount > 0);
            Assert.assertTrue(fuzzNotParsedCount > 0);
            Assert.assertTrue(fuzzUnknownUnitNotParsedCount > 0);
            Assert.assertTrue(fuzzRejectedSuffixCount > 0);
            Assert.assertTrue(fuzzMalformedElementZoneRangeOrderCount > 0);
            Assert.assertTrue(fuzzMalformedElementZoneWithoutDayCount > 0);
            Assert.assertTrue(fuzzUnknownDayFilterOnReversedRangeCount > 0);
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
                            "[$today, 2024-01-03]T[09:00@Bogus/Zone]",
                            // a range between two points in time, with a duration that does not parse
                            "[$now - 1h..$now@+00:00];1h#Tue",
                            "[$now - 1h..$now@+00:00];1h;1d",
                            // a static element with a time part of its own that starts with a
                            // bracket, under a time of the list
                            "[2024-01T[09:00,10:00], $today]T10:00",
                            "[2024-01-05T[09:00], $today]T10:00",
                            // a range that starts after it ends at 03:00, with a timezone or day
                            // filter of its element that does not parse, whatever the other
                            // elements
                            "[$tomorrow..$today@Bogus/Zone]",
                            "[$tomorrow..$today@Bogus/Zone, $tomorrow]",
                            "[$today + 570m..$now@Bogus/Zone]",
                            "[$today..$now - 4h#Xyz]",
                            "[$today + 570m..$now#Xyz]",
                            // a range between two points in time that adds an interval, with a day
                            // filter of its element that does not parse, and with a timezone that
                            // does not parse before a day filter that does
                            "[$now - 2h..$now#Xyz]",
                            "[$now - 2h..$now@Bogus/Zone#Mon]"
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
                    // with a timezone of the element that parses, which SQL parses on every
                    // evaluation
                    assertEngineJobIntervals(driver, engineJob, now, "[$tomorrow..$today@+02:00, $tomorrow]", tomorrow, driver.endOfDay(tomorrow));
                    assertEngineJobIntervals(driver, engineJob, now, "[$today + 570m..$now@+00:00, $tomorrow]T09:00", nineAm, nineAm + minute);
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
    public void testStaticDateTickCalendarLenientOverflowMergesLocally() throws Exception {
        // A list whose fixed date alone has more own extents than the interval limit parses as its bind
        // variable on each evaluation, with the limit where the evaluation is lenient. The parse merges the
        // intervals of each date into those merged so far once there are as many as the merge threshold,
        // rather than all of them again after each new one. At the default threshold of 256 and limit of
        // 1,024, on Monday 2025-01-27, nothing holds the 1,440 seconds at the minutes of 2025-01-24, which
        // lie before today and apart. The first merge, after 255 of them, unites all 256 intervals. Then
        // the j-th second, for j from 256 on, merges into j intervals: 1 + floor(log2(j)) comparisons find
        // that it lands before today, after the second before it, which it reads, and today moves after it,
        // 4 + floor(log2(j)) in all, up to the 1,024th, whose merge leaves 1,025 intervals, past the limit:
        // 256 + 769 x 4 + 256 x 8 + 512 x 9 + 10 = 9,998. Before, each merge sorted and merged every
        // interval so far, 493,185 in all. The list then evaluates as it always has, see
        // testStaticDateTickCalendarOverflowInBackgroundJob.
        final String expression = "[$today, 2025-01-24T[00..23]:[00..59]:00]#XNYS;1m";
        final CairoConfiguration defaultLimits = tickCalendarConfiguration(256, 1024);
        assertMemoryLeak(() -> {
            try (SqlExecutionContextImpl engineJob = newEngineJobContext()) {
                for (TimestampDriver driver : TIMESTAMP_DRIVERS) {
                    final String context = expression + " [" + ColumnType.nameOf(driver.getTimestampType()) + ']';
                    try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(driver, defaultLimits, expression, 0, expression.length(), 0)) {
                        Assert.assertTrue(context, compiled.isParsedOnEvaluation());
                        engineJob.setNowAndFixClock(driver.parseFloorLiteral("2025-01-27T12:00:00.000000Z"), driver.getTimestampType());
                        compiled.init(null, engineJob);
                        // intervalToString() prints to a sink that it reuses
                        final String expected = intervalToString(driver, expectedIntervals(
                                driver,
                                "2025-01-24T14:30:00.000000Z", "2025-01-24T21:01:00.000000Z",
                                "2025-01-27T14:30:00.000000Z", "2025-01-27T21:01:00.000000Z"
                        )).toString();
                        for (int i = 0; i < 2; i++) {
                            intervals.clear();
                            final long before = IntervalUtils.getStaticOwnExtentMergeIntervalCountForTesting();
                            compiled.evaluate(intervals);
                            Assert.assertEquals(context, 9_998, IntervalUtils.getStaticOwnExtentMergeIntervalCountForTesting() - before);
                            TestUtils.assertEquals(context, expected, intervalToString(driver, intervals));
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testStaticDateTickCalendarLenientOverflowStopsAtLimit() throws Exception {
        // A list whose fixed date alone has more own extents than the interval limit parses as its bind
        // variable on each evaluation. SQL that the engine runs on its own parses it with the limit
        // too, so the parse stops once its intervals exceed the limit, and then evaluates the list as it
        // always has. At a merge threshold of 8 and a limit of 16, on Monday 2025-01-27, nothing holds
        // the 1,440 seconds at the minutes of 2025-01-24; their windows of a minute join into the day, and
        // the sessions of the Friday and of the Monday keep 14:30 to 21:00Z of them, a minute longer. With
        // the limit, the parse holds at most 17 intervals, 34 longs, and a sort of them takes 128 more
        // longs of the list as its stack, so the list, which doubles as it grows, never exceeds 324 longs;
        // a parse without the limit would hold 1,441 intervals, 2,882 longs.
        final String expression = "[$today, 2025-01-24T[00..23]:[00..59]:00]#XNYS;1m";
        final CairoConfiguration smallLimits = tickCalendarConfiguration(8, 16);
        assertMemoryLeak(() -> {
            try (SqlExecutionContextImpl engineJob = newEngineJobContext()) {
                for (TimestampDriver driver : TIMESTAMP_DRIVERS) {
                    try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(driver, smallLimits, expression, 0, expression.length(), 0)) {
                        Assert.assertTrue(expression, compiled.isParsedOnEvaluation());
                        engineJob.setNowAndFixClock(driver.parseFloorLiteral("2025-01-27T12:00:00.000000Z"), driver.getTimestampType());
                        compiled.init(null, engineJob);
                        final LongList out = new LongList();
                        compiled.evaluate(out);
                        // intervalToString() prints to a sink that it reuses
                        final String expected = intervalToString(driver, expectedIntervals(
                                driver,
                                "2025-01-24T14:30:00.000000Z", "2025-01-24T21:01:00.000000Z",
                                "2025-01-27T14:30:00.000000Z", "2025-01-27T21:01:00.000000Z"
                        )).toString();
                        TestUtils.assertEquals(expression, expected, intervalToString(driver, out));
                        Assert.assertTrue(expression + ": capacity " + out.capacity(), out.capacity() <= 2 * (2 * 17 + 128));
                    }
                }
            }
        });
    }

    @Test
    public void testStaticDateTickCalendarOverflowFuzzInBackgroundJob() throws Exception {
        // Random lists whose fixed date alone has more own extents than the interval limit, which SQL
        // that the engine runs on its own and a compile probe evaluate without an error. Where the
        // parse with the limit fits, as the bind variable with the limit shows, they evaluate to the
        // documented semantics: the own extents of the elements, united, cut by the sessions, each piece
        // extended by the duration, and merged. Where it does not, they evaluate as the list always
        // has: the same with the fixed date as windows as long as the duration from each of its
        // slots. No element holds a window there, as an element that held the slots would let the parse
        // fit. The windows are at least as long as the time between the slots, so that they join, as
        // windows that stay apart fail the list as it compiles. questdb.fuzz.tickExpressions sets the
        // number of lists, a tenth of it.
        final String[] variables = {"$today", "$yesterday", "$today-3d..$today", "$yesterday..$tomorrow"};
        final int[][] variableDays = {{0, 0}, {-1, -1}, {-3, 0}, {-1, 1}};
        final String[] days = {"2025-01-23", "2025-01-24", "2025-01-27", "2025-01-28"};
        // the durations at the default limit, of which those from an hour on at a limit of 16
        final String[] durations = {"1m", "10m", "30m", "1h", "4h", "1d"};
        final long[] durationSeconds = {60, 600, 1_800, 3_600, 14_400, 86_400};
        final String[] zones = {"", "@+08:00", "@-05:00"};
        final int[] zoneMinutes = {0, 480, -300};
        final String[] nows = {"2025-01-27T12:00:00.000000Z", "2025-01-27T15:30:00.000000Z", "2025-01-24T03:00:00.000000Z", "2025-01-28T12:00:00.000000Z"};
        final int listCount = Math.max(100, Integer.getInteger("questdb.fuzz.tickExpressions", 1_000) / 10);
        final Rnd rnd = TestUtils.generateRandom(LOG);
        final CairoConfiguration smallLimits = tickCalendarConfiguration(8, 16);
        final CairoConfiguration defaultLimits = tickCalendarConfiguration(256, 1024);
        assertMemoryLeak(() -> {
            int fittingCount = 0;
            int fallbackCount = 0;
            final LongList ownExtents = new LongList();
            final LongList windows = new LongList();
            final StringBuilder sb = new StringBuilder();
            try (
                    SqlExecutionContextImpl engineJob = newEngineJobContext();
                    SqlExecutionContextImpl compileProbe = new SqlExecutionContextImpl(engine, 1)
            ) {
                compileProbe.with(securityContext, bindVariableService, null);
                compileProbe.setCompileProbe(true);
                for (int i = 0; i < listCount; i++) {
                    final TimestampDriver driver = TIMESTAMP_DRIVERS[rnd.nextInt(TIMESTAMP_DRIVERS.length)];
                    final String nowText = nows[rnd.nextInt(nows.length)];
                    final long now = driver.parseFloorLiteral(nowText);
                    final long today = driver.startOfDay(now, 0);
                    final String calendar = rnd.nextBoolean() ? "XNYS" : "XHKG";
                    // the seconds at the hours of a day at a limit of 16, or at the minutes at the default limit
                    final boolean isSmallLimit = rnd.nextInt(10) != 0;
                    final int zoneIndex = rnd.nextInt(zones.length);
                    final long zoneOffset = driver.fromMinutes(zoneMinutes[zoneIndex]);
                    final int durationIndex = isSmallLimit ? 3 + rnd.nextInt(3) : rnd.nextInt(durations.length);
                    final long duration = driver.fromSeconds(durationSeconds[durationIndex]);
                    ownExtents.clear();
                    windows.clear();
                    sb.setLength(0);
                    sb.append('[');
                    final int variableCount = rnd.nextInt(3);
                    final int fixedDatePlace = rnd.nextInt(variableCount + 1);
                    for (int e = 0; e <= variableCount; e++) {
                        if (e > 0) {
                            sb.append(", ");
                        }
                        if (e == fixedDatePlace) {
                            final String dayText = days[rnd.nextInt(days.length)];
                            final long day = driver.parseFloorLiteral(dayText + "T00:00:00.000000Z");
                            sb.append(dayText).append(isSmallLimit ? "T[00..23]:00:00" : "T[00..23]:[00..59]:00");
                            for (int hour = 0; hour < 24; hour++) {
                                for (int minute = 0, n = isSmallLimit ? 1 : 60; minute < n; minute++) {
                                    final long lo = day + driver.fromHours(hour) + driver.fromMinutes(minute) - zoneOffset;
                                    ownExtents.add(lo, lo + driver.fromSeconds(1) - 1);
                                    windows.add(lo, lo + duration - 1);
                                }
                            }
                        } else {
                            final int v = rnd.nextInt(variables.length);
                            sb.append(variables[v]);
                            for (int d = variableDays[v][0]; d <= variableDays[v][1]; d++) {
                                final long lo = today + driver.fromDays(d) - zoneOffset;
                                ownExtents.add(lo, lo + driver.fromDays(1) - 1);
                                windows.add(lo, lo + driver.fromDays(1) - 1);
                            }
                        }
                    }
                    sb.append(']').append(zones[zoneIndex]).append('#').append(calendar).append(';').append(durations[durationIndex]);
                    final String text = sb.toString();
                    final CairoConfiguration limits = isSmallLimit ? smallLimits : defaultLimits;
                    final String message = text + " at " + nowText + " [" + ColumnType.nameOf(driver.getTimestampType()) + (isSmallLimit ? ", limit 16]" : "]");
                    boolean isFit = true;
                    try {
                        IntervalUtils.parseTickExpr(driver, limits, text, 0, text.length(), 0, new LongList(), IntervalOperation.INTERSECT, sink, true, now, IntervalUtils.TICK_CHECK_INTERVAL_LIMIT);
                    } catch (SqlException e) {
                        TestUtils.assertContains(message, e.getFlyweightMessage(), LIMIT_ERROR);
                        isFit = false;
                    }
                    final LongList kept = TickCalendarTest.union(TickCalendarTest.intersect(TickCalendarTest.union(isFit ? ownExtents : windows), TickCalendarTest.sessions(driver, calendar)));
                    for (int k = 1, n = kept.size(); k < n; k += 2) {
                        kept.setQuick(k, kept.getQuick(k) + duration);
                    }
                    // intervalToString() prints to a sink that it reuses
                    final String expected = intervalToString(driver, TickCalendarTest.union(kept)).toString();
                    if (isFit) {
                        fittingCount++;
                    } else {
                        fallbackCount++;
                    }
                    try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(driver, limits, text, 0, text.length(), 0)) {
                        Assert.assertTrue(message, compiled.isParsedOnEvaluation());
                        for (SqlExecutionContextImpl context : new SqlExecutionContextImpl[]{engineJob, compileProbe}) {
                            context.setNowAndFixClock(now, driver.getTimestampType());
                            compiled.init(null, context);
                            intervals.clear();
                            compiled.evaluate(intervals);
                            TestUtils.assertEquals(message, expected, intervalToString(driver, intervals));
                        }
                    }
                }
            }
            LOG.info().$("static date overflow fuzz in background jobs [fitting=").$(fittingCount).$(", fallback=").$(fallbackCount).I$();
            Assert.assertTrue("a parse that fits", fittingCount > 0);
            Assert.assertTrue("a parse that does not fit", fallbackCount > 0);
        });
    }

    @Test
    public void testStaticDateTickCalendarOverflowInBackgroundJob() throws Exception {
        // A list whose fixed date alone has more own extents than the interval limit parses as its bind
        // variable on each evaluation. SQL that the engine runs on its own, and a cursor that the compiler
        // holds to inspect a query, parse it with the limit, and where the parse fails, they evaluate the
        // list as it always has, without an error: the windows of the fixed date as long as the duration,
        // which join into its day, cut by the sessions, and extended by the duration again. On Monday
        // 2025-01-27, nothing holds the 1,440 seconds at the minutes of 2025-01-24, nor at a limit of 16
        // its 24 seconds at the hours. At 03:30Z, the range from today to now holds the seconds every ten
        // minutes from 01:00 up to now, and the parse keeps those at 03:40 and 03:50, the last of which
        // ends the session of XHKG at 04:00Z, ten minutes later; the windows of ten minutes used to end it
        // at 04:10.
        final CairoConfiguration defaultLimits = tickCalendarConfiguration(256, 1024);
        final CairoConfiguration smallLimits = tickCalendarConfiguration(8, 16);
        assertMemoryLeak(() -> {
            try (
                    SqlExecutionContextImpl engineJob = newEngineJobContext();
                    SqlExecutionContextImpl compileProbe = new SqlExecutionContextImpl(engine, 1)
            ) {
                compileProbe.with(securityContext, bindVariableService, null);
                compileProbe.setCompileProbe(true);
                for (TimestampDriver driver : TIMESTAMP_DRIVERS) {
                    final long monday = driver.parseFloorLiteral("2025-01-27T12:00:00.000000Z");
                    final long mondayMorning = driver.parseFloorLiteral("2025-01-27T03:30:00.000000Z");
                    for (SqlExecutionContextImpl context : new SqlExecutionContextImpl[]{engineJob, compileProbe}) {
                        assertLenientIntervals(
                                driver, defaultLimits, context, monday,
                                "[$today, 2025-01-24T[00..23]:[00..59]:00]#XNYS;1m",
                                "2025-01-24T14:30:00.000000Z", "2025-01-24T21:01:00.000000Z",
                                "2025-01-27T14:30:00.000000Z", "2025-01-27T21:01:00.000000Z"
                        );
                        assertLenientIntervals(
                                driver, smallLimits, context, monday,
                                "[$today, 2025-01-24T[00..23]:00:00]#XNYS;1h",
                                "2025-01-24T14:30:00.000000Z", "2025-01-24T22:00:00.000000Z",
                                "2025-01-27T14:30:00.000000Z", "2025-01-27T22:00:00.000000Z"
                        );
                        assertLenientIntervals(
                                driver, smallLimits, context, mondayMorning,
                                "[$today..$now, 2025-01-27T[01..03]:[00,10,20,30,40,50]:00]#XHKG;10m",
                                "2025-01-27T01:30:00.000000Z", "2025-01-27T04:00:01.000000Z"
                        );
                    }
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
                            {"[$today, $tomorrow]T[09:00,10:00]@Europe/London;1h1S", "S"},
                            // ranges between two points in time, which ignore the duration
                            {"$now - 1h..$now;1x", "x"},
                            {"[$now - 1h..$now]@Europe/London;1H", "H"},
                            {"[$today + 1h..$now];2D", "D"}
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
                    // a timezone of the element that does not parse: the engine job appends no
                    // interval, and the user's query fails on the order of the range first
                    try (CompiledTickExpression compiled = compile(driver, "[$tomorrow..$today@Bogus/Zone]")) {
                        compiled.init(null, engineJob);
                        intervals.clear();
                        intervals.add(1L, 2L);
                        compiled.evaluate(intervals);
                        Assert.assertEquals(2, intervals.size());
                        Assert.assertEquals(1, intervals.getQuick(0));
                        Assert.assertEquals(2, intervals.getQuick(1));
                        compiled.init(null, user);
                        intervals.clear();
                        try {
                            compiled.evaluate(intervals);
                            Assert.fail();
                        } catch (CairoException e) {
                            TestUtils.assertEquals(RANGE_ORDER_ERROR, e.getFlyweightMessage());
                        }
                        Assert.assertEquals(0, intervals.size());
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

    /**
     * Returns the intervals from each start to the instant before each end, given as pairs of
     * timestamps, at the precision of the driver.
     */
    private static LongList expectedIntervals(TimestampDriver driver, String... startsAndEnds) {
        final LongList intervals = new LongList();
        for (int i = 0; i < startsAndEnds.length; i += 2) {
            intervals.add(driver.parseFloorLiteral(startsAndEnds[i]), driver.parseFloorLiteral(startsAndEnds[i + 1]) - 1);
        }
        return intervals;
    }

    private static boolean isError(String result) {
        return result.startsWith("error: ");
    }

    /**
     * Returns a configuration with the test tick calendars, XNYS and XHKG, the given number of
     * intervals after which an expansion merges them, and the given limit on the merged intervals.
     */
    private static CairoConfiguration tickCalendarConfiguration(int mergeThreshold, int maxIntervals) {
        final FactoryProvider factoryProvider = new DefaultFactoryProvider() {
            @Override
            public @NotNull TickCalendarServiceFactory getTickCalendarServiceFactory() {
                return () -> TestTickCalendarService.INSTANCE;
            }
        };
        return new DefaultCairoConfiguration(root) {
            @Override
            public @NotNull FactoryProvider getFactoryProvider() {
                return factoryProvider;
            }

            @Override
            public int getSqlIntervalIncrementalMergeThreshold() {
                return mergeThreshold;
            }

            @Override
            public int getSqlIntervalMaxIntervalsAfterMerge() {
                return maxIntervals;
            }
        };
    }

    /**
     * Checks one expression at one "now": the parser with fewer checks agrees with the parser with
     * all of them wherever those succeed or fail on a check that the others also run, an engine
     * job evaluates the compiled form to what the parser returns without any check, and a user's
     * context with {@link io.questdb.griffin.SqlExecutionContext#isCompileProbe()} up evaluates it
     * to what the engine job does.
     */
    private void assertChecksAgree(
            TimestampDriver driver,
            SqlExecutionContextImpl engineJob,
            SqlExecutionContextImpl compileProbe,
            String expression,
            boolean isMalformed,
            long now
    ) throws SqlException {
        final String strict = parse(driver, expression, now, IntervalUtils.TICK_CHECK_ALL);
        final String withoutRangeOrder = parse(driver, expression, now, IntervalUtils.TICK_CHECK_INTERVAL_LIMIT);
        final String unchecked = parse(driver, expression, now, 0);
        final String context = expression + " at " + now;
        if (!isMalformed && (TickExprTest.hasRejectedSuffix(expression) || TickExprTest.hasMisplacedDayFilter(expression))) {
            // a text after the first ';' that does not parse fails the expression after every
            // element, a range between two points in time included, unless a range that starts
            // after it ends fails first; a malformed time part that takes that text in fails it
            // too. A range of business days that holds no day reads no such text, see
            // TickExprTest.hasBusinessDayRange().
            fuzzRejectedSuffixCount++;
            if (!("[]".equals(strict) && TickExprTest.hasBusinessDayRange(expression))) {
                Assert.assertTrue(context, isError(strict));
            }
        }
        if (!isMalformed && TickExprTest.hasMalformedElementZone(expression) && !expression.contains("]T[10:30@+02:00]")) {
            // a timezone of an element that does not parse fails the expression whatever the
            // checks, on a range that adds no interval as well; a slot of a time list with a
            // timezone of its own takes that timezone rather than the one of the element
            Assert.assertTrue(context, isError(strict));
            Assert.assertTrue(context, isError(withoutRangeOrder));
            Assert.assertTrue(context, isError(unchecked));
            // count the expressions where only ranges that add no interval hold such a timezone,
            // and it fails them: ranges that start after they end, without the range order check,
            // and ranges of business days that hold no day, with all the checks
            if (strict.contains(RANGE_ORDER_ERROR)
                    && withoutRangeOrder.contains("invalid timezone: ")
                    && hasMalformedZoneOnlyOnReversedRanges(driver, expression, now)) {
                fuzzMalformedElementZoneRangeOrderCount++;
            } else if (strict.contains("invalid timezone: ") && hasMalformedZoneOnlyOnRangesWithoutDay(driver, expression, now)) {
                fuzzMalformedElementZoneWithoutDayCount++;
            }
        }
        if (!isMalformed && hasUnknownDayFilterOnReversedPointInTimeRange(driver, expression, now)) {
            // a day filter of its element that does not parse fails a range between two points in
            // time that starts after it ends, whatever the checks, as it fails one that adds an
            // interval
            Assert.assertTrue(context, isError(strict));
            Assert.assertTrue(context, isError(withoutRangeOrder));
            Assert.assertTrue(context, isError(unchecked));
            fuzzUnknownDayFilterOnReversedRangeCount++;
        }
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
            // a cursor that the compiler holds to inspect a query never fails on a tick expression
            // either, and gets the intervals of the engine job
            compileProbe.setNowAndFixClock(now, driver.getTimestampType());
            compiled.init(null, compileProbe);
            compileProbeIntervals.clear();
            compiled.evaluate(compileProbeIntervals);
            Assert.assertEquals(context, intervals, compileProbeIntervals);
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

    /**
     * Asserts that the compiled form of the expression, which parses it on evaluation, evaluates it at
     * "now" in the lenient context to the intervals from each start to the instant before each end.
     */
    private void assertLenientIntervals(
            TimestampDriver driver,
            CairoConfiguration limitConfiguration,
            SqlExecutionContextImpl context,
            long now,
            String expression,
            String... startsAndEnds
    ) throws SqlException {
        final String message = expression + " [" + ColumnType.nameOf(driver.getTimestampType()) + (context.isCompileProbe() ? ", compile probe]" : ", engine job]");
        try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(driver, limitConfiguration, expression, 0, expression.length(), 0)) {
            Assert.assertTrue(message, compiled.isParsedOnEvaluation());
            context.setNowAndFixClock(now, driver.getTimestampType());
            compiled.init(null, context);
            intervals.clear();
            compiled.evaluate(intervals);
            // intervalToString() prints to a sink that it reuses
            final String expected = intervalToString(driver, expectedIntervals(driver, startsAndEnds)).toString();
            TestUtils.assertEquals(message, expected, intervalToString(driver, intervals));
        }
    }

    private CompiledTickExpression compile(TimestampDriver driver, String expression) throws SqlException {
        return IntervalUtils.compileTickExpr(driver, configuration, expression, 0, expression.length(), 0);
    }

    /**
     * Returns whether every element of a generated expression with a timezone of
     * TickExprTest.FUZZ_MALFORMED_ELEMENT_ZONES is a range of business days that holds no day at
     * "now": the same range without the timezone parses to no interval, where one that starts
     * after it ends fails on its order.
     */
    private boolean hasMalformedZoneOnlyOnRangesWithoutDay(TimestampDriver driver, String expression, long now) {
        final String rangeEnd = ".." + TickExprTest.FUZZ_BUSINESS_DAY_RANGE_END;
        for (String zone : TickExprTest.FUZZ_MALFORMED_ELEMENT_ZONES) {
            for (int i = expression.indexOf(zone); i >= 0; i = expression.indexOf(zone, i + 1)) {
                if (!expression.startsWith(rangeEnd, i - rangeEnd.length())) {
                    return false;
                }
                // a generated range holds no ',' or '[', so the last one before the timezone
                // starts its element
                final int elementStart = Math.max(expression.lastIndexOf(',', i), expression.lastIndexOf('[', i)) + 1;
                if (!"[]".equals(parse(driver, "[" + expression.substring(elementStart, i) + "]", now, IntervalUtils.TICK_CHECK_ALL))) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Returns whether every element of a generated expression with a timezone of
     * TickExprTest.FUZZ_MALFORMED_ELEMENT_ZONES is a range that starts after it ends at "now": the
     * same range without the timezone fails on its order with all the checks.
     */
    private boolean hasMalformedZoneOnlyOnReversedRanges(TimestampDriver driver, String expression, long now) {
        for (String zone : TickExprTest.FUZZ_MALFORMED_ELEMENT_ZONES) {
            for (int i = expression.indexOf(zone); i >= 0; i = expression.indexOf(zone, i + 1)) {
                // a generated range holds no ',' or '[', so the last one before the timezone
                // starts its element when that is a range, and only a range fails on its order
                final int elementStart = Math.max(expression.lastIndexOf(',', i), expression.lastIndexOf('[', i)) + 1;
                final String range = expression.substring(elementStart, i);
                if (!("error: " + RANGE_ORDER_ERROR).equals(parse(driver, "[" + range + "]", now, IntervalUtils.TICK_CHECK_ALL))) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Returns whether a generated expression has a range between two points in time with the day
     * filter TickExprTest.FUZZ_UNKNOWN_DAY_FILTER of its element that starts after it ends at
     * "now": the same range without its suffix fails on its order with all the checks.
     */
    private boolean hasUnknownDayFilterOnReversedPointInTimeRange(TimestampDriver driver, String expression, long now) throws SqlException {
        for (int i = expression.indexOf(TickExprTest.FUZZ_UNKNOWN_DAY_FILTER); i >= 0; i = expression.indexOf(TickExprTest.FUZZ_UNKNOWN_DAY_FILTER, i + 1)) {
            // a generated element of a date variable holds no ',' or '[', so the last one before the
            // day filter starts it, and its expression ends at its timezone, if it has one
            final int elementStart = Math.max(expression.lastIndexOf(',', i), expression.lastIndexOf('[', i)) + 1;
            final int zone = expression.indexOf('@', elementStart);
            final String range = expression.substring(elementStart, zone >= 0 && zone < i ? zone : i).trim();
            final int dots = range.indexOf("..");
            if (!range.startsWith("$") || dots < 0) {
                continue;
            }
            final String start = range.substring(0, dots).trim();
            final String end = range.substring(dots + 2).trim();
            if (end.startsWith("$")
                    && !DateVariableExpr.isDayLevel(DateVariableExpr.parseEncoded(start, 0, start.length(), 0))
                    && !DateVariableExpr.isDayLevel(DateVariableExpr.parseEncoded(end, 0, end.length(), 0))
                    && ("error: " + RANGE_ORDER_ERROR).equals(parse(driver, "[" + range + "]", now, IntervalUtils.TICK_CHECK_ALL))) {
                return true;
            }
        }
        return false;
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
