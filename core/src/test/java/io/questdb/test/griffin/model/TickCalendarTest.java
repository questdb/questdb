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
import io.questdb.cairo.TickCalendarService;
import io.questdb.cairo.TickCalendarServiceFactory;
import io.questdb.cairo.TimestampDriver;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.model.CompiledTickExpression;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.StringSink;
import io.questdb.test.cairo.TestTickCalendarService;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Predicate;

/**
 * Tests for TICK expression parsing with tick calendars.
 * Uses TestTickCalendarService with hardcoded XNYS and XHKG schedules for 2025,
 * covering regular trading weeks, bank holidays, and early closes for both exchanges.
 */
public class TickCalendarTest {

    // the test calendars XNYS and XHKG, see TestTickCalendarService, and X24H
    private static final TickCalendarService CALENDARS = exchange -> Chars.equalsIgnoreCase("X24H", exchange)
            ? TickCalendarTest.X24H
            : TestTickCalendarService.INSTANCE.getSchedule(exchange);
    // a Friday that XNYS trades on from 14:30 to 21:00Z
    private static final String FRIDAY_NOON = "2025-01-24T12:00:00.000000Z";
    private static final Log LOG = LogFactory.getLog(TickCalendarTest.class);
    // the Monday after FRIDAY_NOON, which XNYS trades on from 14:30 to 21:00Z too
    private static final String MONDAY_NOON = "2025-01-27T12:00:00.000000Z";
    private static final String OVER_CAP_ERROR = "Bracket expansion produces too many intervals (max 1024)";
    // a test calendar with one session a UTC day, on 1969-12-31, 1970-01-01, every day of 2025,
    // 2099-01-02 and 2099-01-03, so that a session holds every hour of those days
    private static final LongList X24H = buildX24h();
    private static final StringSink sink = new StringSink();
    private static CairoConfiguration configuration;

    @BeforeClass
    public static void setUp() {
        configuration = new DefaultCairoConfiguration(System.getProperty("java.io.tmpdir")) {
            private final FactoryProvider factoryProvider = new DefaultFactoryProvider() {
                @Override
                public @NotNull TickCalendarServiceFactory getTickCalendarServiceFactory() {
                    return () -> CALENDARS;
                }
            };

            @Override
            public @NotNull FactoryProvider getFactoryProvider() {
                return factoryProvider;
            }
        };
    }

    @Test
    public void testBareDateVariableWithTickCalendarAppliesDurationOnce() {
        // A bare date variable expression with a tick calendar and a duration takes each of its days
        // and time slots with its own extent, a day, the minute of 15:00 or the point in time of
        // $now, the tick calendar keeps what its sessions hold of them, and the duration extends
        // what they keep, once, as for the date list spelling of the expression and for its
        // compiled form. XNYS trades from 14:30 to 21:00Z on Thursday 2025-01-23, Friday 2025-01-24,
        // Monday 2025-01-27 and Tuesday 2025-01-28, until 17:00Z on 2025-07-03, and not on Saturday
        // 2025-01-25, on the holiday 2025-01-20, nor outside 2025, which the test calendar has no
        // sessions for. XHKG trades from 01:30 to 04:00Z and from 05:00 to 08:00Z.
        final String[][] cases = {
                // now, expression, intervals
                {FRIDAY_NOON, "$todayT[15:00,16:00]#XNYS;1h", "[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T17:00:59.999999Z}]"},
                {FRIDAY_NOON, "$todayT[15:00@UTC]#XNYS;1h", "[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T16:00:59.999999Z}]"},
                {FRIDAY_NOON, "$todayT[15..16]:00#XNYS;1h", "[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T17:00:59.999999Z}]"},
                {FRIDAY_NOON, "$todayT15:00#XNYS;1h", "[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T16:00:59.999999Z}]"},
                {FRIDAY_NOON, "$TODAYT15:00#xnys;1h", "[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T16:00:59.999999Z}]"},
                {FRIDAY_NOON, "$today#XNYS;1h", "[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T21:59:59.999999Z}]"},
                {FRIDAY_NOON, " $today #XNYS;1h", "[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T21:59:59.999999Z}]"},
                // an empty and a zero duration leave the session as it is, a negative one shortens it
                {FRIDAY_NOON, "$today#XNYS;", "[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T20:59:59.999999Z}]"},
                {FRIDAY_NOON, "$today#XNYS;0h", "[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T20:59:59.999999Z}]"},
                {FRIDAY_NOON, "$today#XNYS;-1h", "[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T19:59:59.999999Z}]"},
                // the minute of 15:00 ends an hour before it ends, an interval that holds no timestamp
                {FRIDAY_NOON, "$todayT15:00#XNYS;-1h", "[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T14:00:59.999999Z}]"},
                {FRIDAY_NOON, "$today#XNYS;1M", "[{lo=2025-01-24T14:30:00.000000Z, hi=2025-02-24T20:59:59.999999Z}]"},
                {FRIDAY_NOON, "$todayT15:00#XNYS;1y", "[{lo=2025-01-24T15:00:00.000000Z, hi=2026-01-24T15:00:59.999999Z}]"},
                {"2025-01-23T12:00:00.000000Z", "$today#XNYS;2d", "[{lo=2025-01-23T14:30:00.000000Z, hi=2025-01-25T20:59:59.999999Z}]"},
                // the two sessions, each four hours longer, overlap
                {FRIDAY_NOON, "$today#XHKG;4h", "[{lo=2025-01-24T01:30:00.000000Z, hi=2025-01-24T11:59:59.999999Z}]"},
                {"2025-07-03T12:00:00.000000Z", "$todayT16:30#XNYS;1h", "[{lo=2025-07-03T16:30:00.000000Z, hi=2025-07-03T17:30:59.999999Z}]"},
                {FRIDAY_NOON, "$todayT20:30#XNYS;4h", "[{lo=2025-01-24T20:30:00.000000Z, hi=2025-01-25T00:30:59.999999Z}]"},
                {"2025-01-24T15:30:00.000000Z", "$now#XNYS;1h", "[{lo=2025-01-24T15:30:00.000000Z, hi=2025-01-24T16:30:00.000000Z}]"},
                {FRIDAY_NOON, "$now#XNYS;1h", "[]"},
                {"2025-01-24T15:30:00.000000Z", "$today..$now#XNYS;1h", "[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T16:30:00.000000Z}]"},
                {FRIDAY_NOON, "$today+1bd#XNYS;1h", "[{lo=2025-01-27T14:30:00.000000Z, hi=2025-01-27T21:59:59.999999Z}]"},
                {
                        FRIDAY_NOON,
                        "$today..$today+2bd#XNYS;1h",
                        "[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T21:59:59.999999Z},"
                                + "{lo=2025-01-27T14:30:00.000000Z, hi=2025-01-27T21:59:59.999999Z},"
                                + "{lo=2025-01-28T14:30:00.000000Z, hi=2025-01-28T21:59:59.999999Z}]"
                },
                // the minutes from 14:00 to 20:59 join, and the session keeps 14:30 to 21:00 of them
                {FRIDAY_NOON, "$todayT[14..20]:[00..59]#XNYS;-30s", "[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T20:59:29.999999Z}]"},
                // the days from Friday to Sunday at +08:00 start at 16:00Z on Thursday
                {
                        FRIDAY_NOON,
                        "$today..$today+2d@+08:00#XNYS;-30m",
                        "[{lo=2025-01-23T16:00:00.000000Z, hi=2025-01-23T20:29:59.999999Z},{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T20:29:59.999999Z}]"
                },
                // a range between two points in time, and slots an hour long, as long as the duration
                {"2025-01-24T15:30:00.000000Z", "$now - 1h..$now#XNYS;1h", "[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T16:30:00.000000Z}]"},
                {FRIDAY_NOON, "$todayT[15,16]#XNYS;1h", "[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T17:59:59.999999Z}]"},
                // days without a session
                {"2025-01-25T12:00:00.000000Z", "$todayT[15:00,16:00]#XNYS;1h", "[]"},
                {"2025-01-20T12:00:00.000000Z", "$today#XNYS;1h", "[]"},
                {"2024-01-24T12:00:00.000000Z", "$today#XNYS;1h", "[]"},
                {FRIDAY_NOON, "$today - 20000d#XNYS;1h", "[]"},
                {FRIDAY_NOON, "$today + 2000000d#XNYS;1h", "[]"}
        };
        for (int timestampType : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
            final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
            for (String[] c : cases) {
                assertBareDateVariableMatchesDateList(driver, c[1], driver.parseFloorLiteral(c[0]), forTimestampType(c[2], timestampType));
            }
        }
    }

    @Test
    public void testBareDateVariableWithTickCalendarFuzz() throws SqlException {
        // Random bare date variable expressions with a tick calendar and a duration, each checked
        // against its date list spelling, its compiled form, the same text without the tick
        // calendar, and, where modelBareDateVariable() models the shape, the documented semantics.
        // questdb.fuzz.tickExpressions sets the number of expressions.
        final int expressionCount = Integer.getInteger("questdb.fuzz.tickExpressions", 1_000);
        final Rnd rnd = TestUtils.generateRandom(LOG);
        final BareDateVariableFuzzer fuzzer = new BareDateVariableFuzzer();
        for (int i = 0; i < expressionCount; i++) {
            fuzzer.check(rnd);
        }
        fuzzer.log();
        if (expressionCount >= 1_000) {
            fuzzer.assertEveryShape();
        }
    }

    @Test
    public void testBareDateVariableWithTickCalendarIntervalLimit() throws SqlException {
        // The interval limit counts what the date list spelling of a bare expression counts: each
        // day and time slot with its own extent before the tick calendar runs, and what the
        // calendar keeps, extended by the duration, at the end. Its date list spelling fails where
        // the expression fails. At 2025-12-31, the days back to 200 and 365 days before hold 1,407
        // and 2,562 one-minute slots that do not join, more than 1,024, and so do the 1,560
        // one-second slots of 00:00 to 12:59:30; whole days join each other.
        final CairoConfiguration smallLimits = calendarConfiguration(8, 16);
        for (int timestampType : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
            final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
            final String context = " [" + ColumnType.nameOf(timestampType) + ']';
            final long yearEnd = driver.parseFloorLiteral("2025-12-31T12:00:00.000000Z");
            final long friday = driver.parseFloorLiteral(FRIDAY_NOON);
            final String dateRangeError = "error: Date range expansion produces too many intervals (max 1024)";
            final String bracketError = "error: Bracket expansion produces too many intervals (max 1024)";
            for (String[] c : new String[][]{
                    {"$today-200d..$todayT[09:00,09:30,10:00,10:30,11:00,11:30,12:00]#XNYS;30m", dateRangeError},
                    {"$todayT[00..12]:[00..59]:[00,30]#XNYS;30s", bracketError},
                    {"$today-365d..$todayT[14:30,15:30,16:30,17:30,18:30,19:30,20:30]#XNYS;1h", dateRangeError}
            }) {
                Assert.assertEquals(c[0] + context, c[1], parseTickExpr(driver, c[0], yearEnd));
                Assert.assertEquals(c[0] + context, c[1], parseTickExpr(driver, toDateList(c[0]), yearEnd));
            }
            // the time slots of the second example have no IR, so its literal fails as its SQL compiles
            final String seconds = "$todayT[00..12]:[00..59]:[00,30]#XNYS;30s";
            try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(driver, configuration, seconds, 0, seconds.length(), 0)) {
                Assert.assertTrue(compiled.isParsedOnEvaluation());
                try {
                    compiled.validate(yearEnd);
                    Assert.fail("validate should fail" + context);
                } catch (SqlException e) {
                    Assert.assertEquals(context, bracketError, "error: " + e.getFlyweightMessage());
                }
            }
            // whole days join, so only the sessions count: the 250 sessions of 2025, and from Friday
            // 2025-01-24 on, 236, each an hour longer, where the days of the range walk for 8,000 years
            final long hour = driver.fromHours(1);
            Assert.assertEquals(
                    context,
                    sessionsToString(
                            driver,
                            "XNYS",
                            driver.parseFloorLiteral("2022-09-18T00:00:00.000000Z"),
                            driver.parseFloorLiteral("2025-12-31T23:59:59.999999Z"),
                            hour,
                            250
                    ),
                    parseTickExpr(driver, "$today-1200d..$today#XNYS;1h", yearEnd)
            );
            if (timestampType == ColumnType.TIMESTAMP_MICRO) {
                // nanoseconds do not reach 8,000 years ahead
                Assert.assertEquals(
                        context,
                        sessionsToString(driver, "XNYS", driver.parseFloorLiteral("2025-01-24T00:00:00.000000Z"), Long.MAX_VALUE, hour, 236),
                        parseTickExpr(driver, "$today..$today+2900000d#XNYS;1h", friday)
                );
            }

            // At a merge threshold of 8 and a limit of 16, from Thursday 2025-01-02, XNYS has 13 sessions in
            // the 21 days to 2025-01-22, 15 in the 23 days to 2025-01-24, and 20 in the 31 days to
            // 2025-02-01, and XHKG two a day on 10 days in the 14 days to 2025-01-15
            final long januarySecond = driver.parseFloorLiteral("2025-01-02T12:00:00.000000Z");
            final long january = driver.parseFloorLiteral("2025-01-02T00:00:00.000000Z");
            final String smallError = "error: Bracket expansion produces too many intervals (max 16)";
            Assert.assertEquals(
                    context,
                    sessionsToString(driver, "XNYS", january, driver.parseFloorLiteral("2025-01-22T23:59:59.999999Z"), hour, 13),
                    parseTickExpr(smallLimits, driver, "$today..$today+20d#XNYS;1h", januarySecond, IntervalUtils.TICK_CHECK_ALL)
            );
            Assert.assertEquals(context, smallError, parseTickExpr(smallLimits, driver, "$today..$today+13d#XHKG;30m", januarySecond, IntervalUtils.TICK_CHECK_ALL));
            Assert.assertEquals(context, smallError, parseTickExpr(smallLimits, driver, toDateList("$today..$today+13d#XHKG;30m"), januarySecond, IntervalUtils.TICK_CHECK_ALL));
            Assert.assertEquals(context, smallError, parseTickExpr(smallLimits, driver, "$today..$today+30d#XNYS;1h", januarySecond, IntervalUtils.TICK_CHECK_ALL));
            // the minutes of the session join
            Assert.assertEquals(
                    context,
                    forTimestampType("[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T20:59:29.999999Z}]", timestampType),
                    parseTickExpr(smallLimits, driver, "$todayT[14..20]:[00..59]#XNYS;-30s", friday, IntervalUtils.TICK_CHECK_ALL)
            );
            // the hours of each day, out of order, merge before the calendar runs
            Assert.assertEquals(
                    context,
                    sessionsToString(driver, "XNYS", january, driver.parseFloorLiteral("2025-01-24T23:59:59.999999Z"), hour, 15),
                    parseTickExpr(smallLimits, driver, "$today..$today+22dT[20,17,00..16,18,19,21..23]#XNYS;1h", januarySecond, IntervalUtils.TICK_CHECK_ALL)
            );
        }
    }

    @Test
    public void testBareDateVariableWithTickCalendarKeepsDurationErrors() {
        // Each day and time slot of a bare date variable expression with a tick calendar still
        // parses with the duration, which fails it as it fails the same text without the tick
        // calendar, whether or not a session holds the day: on Friday 2025-01-24 at 12:00 and at
        // 03:00, on Saturday 2025-01-25, and on the holiday 2025-01-20.
        final String[] expressions = {
                "$today", "$todayT15:00", "$todayT[15:00,16:00]", "$todayT[15:00@UTC]", "$todayT[15..16]:00",
                "$now", "$today..$today+2d", "$today..$now", "$now - 1h..$now"
        };
        final String[] durations = {"1x", "abc", "1", "1h@UTC", "99999999999h", "1H", "1h ", "-1x"};
        final String[] nows = {FRIDAY_NOON, "2025-01-24T03:00:00.000000Z", "2025-01-25T12:00:00.000000Z", "2025-01-20T12:00:00.000000Z"};
        for (int timestampType : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
            final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
            for (String nowText : nows) {
                final long now = driver.parseFloorLiteral(nowText);
                for (String expression : expressions) {
                    for (String duration : durations) {
                        final String text = expression + "#XNYS;" + duration;
                        final String context = text + " at " + nowText + " [" + ColumnType.nameOf(timestampType) + ']';
                        String expected = null;
                        for (int checks : new int[]{IntervalUtils.TICK_CHECK_ALL, IntervalUtils.TICK_CHECK_INTERVAL_LIMIT, 0}) {
                            expected = parseTickExpr(driver, expression + ';' + duration, now, checks);
                            Assert.assertTrue(context, expected.startsWith("error: "));
                            Assert.assertEquals(context, expected, parseTickExpr(driver, text, now, checks));
                        }
                        // the IR rejects such a duration as it compiles, and a compiled form that parses
                        // the expression on evaluation fails as the parser does
                        final String compiled = evaluateCompiledTickExpr(driver, text, now);
                        Assert.assertTrue(context + ": " + compiled, compiled.startsWith("compile error: ") || compiled.equals(expected));
                    }
                }
            }

            final long friday = driver.parseFloorLiteral(FRIDAY_NOON);
            final long saturday = driver.parseFloorLiteral("2025-01-25T12:00:00.000000Z");
            // the duration fails the first slot before the time of the second, as without the tick
            // calendar, while the date list spelling fails on the time first
            Assert.assertEquals("error: Invalid duration unit: x", parseTickExpr(driver, "$todayT[15:00,25:00]#XNYS;1x", friday));
            Assert.assertEquals(parseTickExpr(driver, "$todayT[15:00,25:00];1x", friday), parseTickExpr(driver, "$todayT[15:00,25:00]#XNYS;1x", friday));
            Assert.assertEquals("error: Invalid date: 2025-01-24T25:00", parseTickExpr(driver, "$todayT[25:00,15:00]#XNYS;1x", friday));
            // the day parses with the duration before its timezone converts it
            Assert.assertEquals("error: Invalid duration unit: x", parseTickExpr(driver, "$today@Bogus/Zone#XNYS;1x", friday));
            // a range of business days within a weekend has no day to parse with the duration
            Assert.assertEquals("[]", parseTickExpr(driver, "$today..$today+0bd#XNYS;1x", saturday));
            // a range that starts after it ends fails at its '..', or, without the range order check,
            // adds no day
            final String inverted = "$tomorrow..$today#XNYS;1x";
            try {
                IntervalUtils.parseTickExpr(driver, configuration, inverted, 0, inverted.length(), 0, new LongList(), sink, friday, IntervalUtils.TICK_CHECK_ALL);
                Assert.fail("the range should fail: " + inverted);
            } catch (SqlException e) {
                Assert.assertEquals("Invalid date range: start is after end", e.getFlyweightMessage().toString());
                Assert.assertEquals(inverted.indexOf(".."), e.getPosition());
            }
            Assert.assertEquals("[]", parseTickExpr(driver, inverted, friday, IntervalUtils.TICK_CHECK_INTERVAL_LIMIT));
            Assert.assertEquals("[]", parseTickExpr(driver, inverted, friday, 0));
            Assert.assertEquals("error: Unknown date variable: $tody", parseTickExpr(driver, "$tody#XNYS;1h", friday));
            // a ';' in a bracket leaves the bracket unclosed before the time part
            Assert.assertEquals("error: Unclosed '[' in interval", parseTickExpr(driver, "$todayT[15:00;1h,16:00]#XNYS", friday));
            Assert.assertEquals("error: Unclosed '[' in interval", parseTickExpr(driver, "$todayT[15;16]:00#XNYS", friday));
            // $now keeps its own time, and the time part of the list still has to parse
            Assert.assertEquals("error: Invalid date: 2025-01-24T15:00:00UTC", parseTickExpr(driver, "$nowT15:00:00UTC#XNYS;1h", friday));
            if (timestampType == ColumnType.TIMESTAMP_MICRO) {
                // nine digits of a second are more than microseconds hold
                Assert.assertEquals(
                        "error: Invalid date: 2025-01-24T15:00:00.123456789Z",
                        parseTickExpr(driver, "$todayT15:00:00.123456789Z#XNYS;1h", friday)
                );
            }
        }
    }

    @Test
    public void testBareDateVariableWithTickCalendarKeepsTimeSpellings() {
        // A bare date variable expression with a tick calendar and a duration accepts each spelling
        // of the time that the same text without the tick calendar accepts, with the same error
        // otherwise. A time with a zone is a point in time, which the duration extends by an hour
        // where the session from 14:30 to 21:00Z on Friday 2025-01-24 holds it. Not here: the
        // spelling 15:00:00.1234Z, which the parser reads as 15:00:00.234 with a duration, see
        // bug-duration-date-parse-any-format-misreads.
        final String[][] spellings = {
                // time, the point in time, or null where the time does not parse
                {"15:00:00Z", "2025-01-24T15:00:00.000000Z"},
                {"09:00:00Z", "2025-01-24T09:00:00.000000Z"},
                {"15:00:00UTC", "2025-01-24T15:00:00.000000Z"},
                {"09:00:00UTC", "2025-01-24T09:00:00.000000Z"},
                {"15:00:00GMT", "2025-01-24T15:00:00.000000Z"},
                {"15:00:00z", "2025-01-24T15:00:00.000000Z"},
                {"15:00:00EST", "2025-01-24T20:00:00.000000Z"},
                {"15:00:00+01:00", "2025-01-24T14:00:00.000000Z"},
                {"15:00:00-05", "2025-01-24T20:00:00.000000Z"},
                {"15:00:00.5Z", "2025-01-24T15:00:00.500000Z"},
                {"15:00:00.123456789Z", null},
                {"15:00:00.0000000001", null},
                {"15:00:00 ", null},
                {"15:0", null},
                {"1", null}
        };
        for (int timestampType : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
            final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
            final long friday = driver.parseFloorLiteral(FRIDAY_NOON);
            final long sessionStart = driver.parseFloorLiteral("2025-01-24T14:30:00.000000Z");
            final long sessionEnd = driver.parseFloorLiteral("2025-01-24T21:00:00.000000Z");
            for (String[] spelling : spellings) {
                final String text = "$todayT" + spelling[0] + "#XNYS;1h";
                final String context = text + " [" + ColumnType.nameOf(timestampType) + ']';
                final String withoutCalendar = "$todayT" + spelling[0] + ";1h";
                final LongList window = new LongList();
                String expected;
                try {
                    IntervalUtils.parseTickExpr(driver, configuration, withoutCalendar, 0, withoutCalendar.length(), 0, window, sink, friday, IntervalUtils.TICK_CHECK_ALL);
                    // without the tick calendar, the duration makes the time an hour-wide window
                    // that starts at it
                    Assert.assertEquals(context, 2, window.size());
                    final long point = window.getQuick(0);
                    expected = point >= sessionStart && point < sessionEnd
                            ? intervalToString(driver, new LongList(new long[]{point, point + driver.fromHours(1)}))
                            : "[]";
                    if (spelling[1] != null) {
                        Assert.assertEquals(context, driver.parseFloorLiteral(spelling[1]), point);
                    }
                } catch (SqlException e) {
                    expected = "error: " + e.getFlyweightMessage();
                    // nanoseconds hold nine digits of a second, which microseconds do not, and
                    // 15:00:00.0000000001 reads as a point in time
                    final boolean isNanoFraction = spelling[0].equals("15:00:00.123456789Z") && timestampType == ColumnType.TIMESTAMP_NANO;
                    Assert.assertTrue(context, spelling[1] == null && !isNanoFraction && !spelling[0].equals("15:00:00.0000000001"));
                    Assert.assertTrue(context, expected.startsWith("error: Invalid date: "));
                }
                Assert.assertEquals(context, expected, parseTickExpr(driver, text, friday));
            }
            // the points from 15:00 and 20:00Z on, and none at 09:00 or 14:00Z, which the session
            // does not hold
            Assert.assertEquals(
                    forTimestampType("[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T16:00:00.000000Z}]", timestampType),
                    parseTickExpr(driver, "$todayT15:00:00UTC#XNYS;1h", friday)
            );
            Assert.assertEquals(
                    forTimestampType("[{lo=2025-01-24T20:00:00.000000Z, hi=2025-01-24T21:00:00.000000Z}]", timestampType),
                    parseTickExpr(driver, "$todayT15:00:00EST#XNYS;1h", friday)
            );
            Assert.assertEquals("[]", parseTickExpr(driver, "$todayT09:00:00UTC#XNYS;1h", friday));
            Assert.assertEquals("[]", parseTickExpr(driver, "$todayT15:00:00+01:00#XNYS;1h", friday));
        }
        final TimestampDriver nanos = ColumnType.getTimestampDriver(ColumnType.TIMESTAMP_NANO);
        Assert.assertEquals(
                "[{lo=2025-01-24T15:00:00.123456789Z, hi=2025-01-24T16:00:00.123456789Z}]",
                parseTickExpr(nanos, "$todayT15:00:00.123456789Z#XNYS;1h", nanos.parseFloorLiteral(FRIDAY_NOON))
        );
    }

    @Test
    public void testBareDateVariableWithTickCalendarLeavesLaterParsesAlone() throws SqlException {
        // A bare date variable expression with a tick calendar and a duration marks the steps that
        // its dates take from the pool of the thread to keep their own extent, see
        // PendingSteps.isDurationAfterCalendar. The expressions that the thread parses next take
        // the same steps from the pool, and a duration still makes each of their dates an hour
        // long from 15:00: a bracket expansion without a date list, and the date of a date list
        // beside a date variable, which compileTickExpr() expands as it compiles. Today is Friday
        // 2025-01-24, which XNYS trades on from 14:30 to 21:00Z.
        final String bare = "$todayT[15:00,16:00]#XNYS;1h";
        final String expansion = "2025-01-[23..24]T15:00;1h";
        final String dateList = "[2025-01-[22..23], $today]T15:00;1h";
        for (int timestampType : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
            final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
            final long friday = driver.parseFloorLiteral(FRIDAY_NOON);
            final String bareExpected = forTimestampType("[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T17:00:59.999999Z}]", timestampType);

            Assert.assertEquals(bare, bareExpected, parseTickExpr(driver, bare, friday));
            Assert.assertEquals(
                    expansion,
                    forTimestampType(
                            "[{lo=2025-01-23T15:00:00.000000Z, hi=2025-01-23T15:59:59.999999Z},{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T15:59:59.999999Z}]",
                            timestampType
                    ),
                    parseTickExpr(driver, expansion, friday)
            );

            Assert.assertEquals(bare, bareExpected, parseTickExpr(driver, bare, friday));
            try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(driver, configuration, dateList, 0, dateList.length(), 0)) {
                Assert.assertFalse(dateList, compiled.isParsedOnEvaluation());
                final LongList out = new LongList();
                compiled.evaluate(out, friday);
                Assert.assertEquals(
                        dateList,
                        forTimestampType(
                                "[{lo=2025-01-22T15:00:00.000000Z, hi=2025-01-22T15:59:59.999999Z},{lo=2025-01-23T15:00:00.000000Z, hi=2025-01-23T15:59:59.999999Z},"
                                        + "{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T15:59:59.999999Z}]",
                                timestampType
                        ),
                        intervalToString(driver, out)
                );
            }
        }
    }

    @Test
    public void testBareDateVariableWithTickCalendarMatchesDateList() throws SqlException {
        // Every combination of a bare date variable expression, a time part, a timezone, a tick
        // calendar, a day filter or neither, and a duration, at seven "now"s: the expression parses
        // as its date list spelling does, and with a tick calendar as its IR evaluates it, unless it
        // has a time that only a duration lets the parser read. Time slots that hold one another
        // unite before the tick calendar cuts them in all three. With a tick calendar and a duration of a fixed length, it parses to what
        // modelBareDateVariable() gives where that models it.
        final String[] expressions = {
                "$today", "$TODAY", "$yesterday", "$tomorrow", "$today + 3d", "$today+1bd", "$now", "$now - 2h",
                "$today..$today+2d", "$today..$today+2bd", "$yesterday..$tomorrow", "$today..$now", "$now - 1h..$now",
                "$today + 870m..$now", "$today-10d..$today"
        };
        // the days of each of the expressions above that are days, relative to today, or null
        final int[][] expressionDays = {
                {0, 0}, {0, 0}, {-1, -1}, {1, 1}, {3, 3}, null, null, null,
                {0, 2}, null, {-1, 1}, null, null,
                null, {-10, 0}
        };
        final String[] timeParts = {
                "", "T15:00", "T[15:00,16:00]", "T[15:00@UTC]", "T[15..16]:00", "T20:30", "T[09:00,14:30,15:00]",
                "T[23:30,00:30]", "T15", "T[15:00,15:00:30]", "T15:00:00Z", "T15:00:00UTC", "T09:00:00UTC",
                "T15:00:00+01:00", "T[15:00@America/New_York,16:00]", "T[14..20]:[00,30]"
        };
        // the slots of each of the time parts above that modelBareDateVariable() models, or null
        final String[][] timePartSlots = {
                {}, {"15:00"}, {"15:00", "16:00"}, {"15:00@UTC"}, null, {"20:30"}, {"09:00", "14:30", "15:00"},
                {"23:30", "00:30"}, {"15"}, {"15:00", "15:00:30"}, {"15:00:00Z"}, {"15:00:00UTC"}, {"09:00:00UTC"},
                {"15:00:00+01:00"}, null, null
        };
        final String[] zones = {"", "@America/New_York", "@+08:00"};
        final String[] suffixes = {"#XNYS", "#XHKG", "", "#Fri"};
        final String[] durations = {"1h", "30m", "1h30m", "1d", "4h", "", "0h", "2h-1h", "-30m", "-1h", "1M"};
        // the length of each of the durations above in seconds, or null for a calendar unit
        final Long[] durationSeconds = {3_600L, 1_800L, 5_400L, 86_400L, 14_400L, 0L, 0L, 3_600L, -1_800L, -3_600L, null};
        final String[] nows = {
                "2025-01-24T03:00:00.000000Z", "2025-01-24T15:30:00.000000Z", "2025-01-24T22:00:00.000000Z",
                "2025-01-25T12:00:00.000000Z", "2025-01-20T12:00:00.000000Z", "2025-07-03T12:00:00.000000Z",
                "2025-03-10T12:00:00.000000Z"
        };
        int modelCount = 0;
        int irCount = 0;
        final LongList days = new LongList();
        for (int timestampType : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
            final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
            for (int e = 0; e < expressions.length; e++) {
                for (int t = 0; t < timeParts.length; t++) {
                    // a time that only a duration lets the parser read
                    final boolean isReadOnlyWithDuration = timeParts[t].contains("UTC") && !timeParts[t].contains("@UTC");
                    final boolean hasPoint = timeParts[t].contains(":00:00");
                    for (String zone : zones) {
                        for (String suffix : suffixes) {
                            final boolean isCalendar = suffix.startsWith("#X");
                            for (int d = 0; d < durations.length; d++) {
                                final String text = expressions[e] + timeParts[t] + zone + suffix + ';' + durations[d];
                                final String dateList = toDateList(text);
                                CompiledTickExpression compiled = null;
                                try {
                                    if (isCalendar) {
                                        compiled = IntervalUtils.compileTickExpr(driver, configuration, text, 0, text.length(), 0);
                                    }
                                } catch (SqlException ignore) {
                                    // the IR rejects the time, and in() fails such a literal
                                }
                                try {
                                    for (String nowText : nows) {
                                        final long now = driver.parseFloorLiteral(nowText);
                                        final String context = text + " at " + nowText + " [" + ColumnType.nameOf(timestampType) + ']';
                                        final String parsed = parseTickExpr(driver, text, now);
                                        if (!isReadOnlyWithDuration) {
                                            Assert.assertEquals(context, parseTickExpr(driver, dateList, now), parsed);
                                        }
                                        if (isCalendar && compiled != null && !compiled.isParsedOnEvaluation()) {
                                            irCount++;
                                            final LongList out = new LongList();
                                            String evaluated;
                                            try {
                                                compiled.evaluate(out, now);
                                                evaluated = intervalToString(driver, out);
                                            } catch (CairoException ex) {
                                                evaluated = "error: " + ex.getFlyweightMessage();
                                            }
                                            Assert.assertEquals(context, evaluated, parsed);
                                        }
                                        if (isCalendar && expressionDays[e] != null && timePartSlots[t] != null && durationSeconds[d] != null
                                                && (zone.isEmpty() || (zone.equals("@+08:00") && !hasPoint))) {
                                            modelCount++;
                                            final long today = driver.startOfDay(now, 0);
                                            days.clear();
                                            for (int day = expressionDays[e][0]; day <= expressionDays[e][1]; day++) {
                                                days.add(today + driver.fromDays(day));
                                            }
                                            Assert.assertEquals(
                                                    context,
                                                    modelBareDateVariable(
                                                            driver,
                                                            days,
                                                            timePartSlots[t],
                                                            zone.isEmpty() ? 0 : driver.fromHours(8),
                                                            suffix.substring(1),
                                                            driver.fromSeconds(durationSeconds[d]),
                                                            false
                                                    ),
                                                    parsed
                                            );
                                        }
                                    }
                                } finally {
                                    Misc.free(compiled);
                                }
                            }
                        }
                    }
                }
            }
        }
        LOG.info().$("bare tick calendar grid [modelCount=").$(modelCount).$(", irCount=").$(irCount).I$();
        Assert.assertTrue(modelCount > 0);
        Assert.assertTrue(irCount > 0);

        // A duration that does not parse fails the expression as it fails the same text without the
        // tick calendar. So does a repeating interval, which each date parses as without the tick
        // calendar, where that fails; the tick calendar then reads it as a duration, which fails
        // the expression where a session keeps something (bug-repeating-suffix-tick-calendar).
        for (int timestampType : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
            final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
            for (String nowText : nows) {
                final long now = driver.parseFloorLiteral(nowText);
                for (String expression : expressions) {
                    for (String timePart : timeParts) {
                        for (String duration : new String[]{"1x", "1h;1d;3", "1h;1d"}) {
                            final String text = expression + timePart + "#XNYS;" + duration;
                            final String parsed = parseTickExpr(driver, text, now);
                            final String withoutCalendar = parseTickExpr(driver, expression + timePart + ';' + duration, now);
                            if (withoutCalendar.startsWith("error: ") || (parsed.startsWith("error: ") && duration.indexOf(';') < 0)) {
                                Assert.assertEquals(text + " at " + nowText, withoutCalendar, parsed);
                            }
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testBareDateVariableWithTickCalendarParsesEveryLeafKind() throws SqlException {
        // A day of a range of days, with and without a time part, a time slot below a time list
        // slot with a timezone of its own, and a date that does not parse, under a tick calendar
        // and a duration. On Friday 2025-01-24, XNYS trades from 14:30 to 21:00Z, and not on the
        // weekend after it.
        for (int timestampType : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
            final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
            final long friday = driver.parseFloorLiteral(FRIDAY_NOON);
            assertBareDateVariableMatchesDateList(
                    driver,
                    "$today..$today+2dT15:00#XNYS;1h",
                    friday,
                    forTimestampType("[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T16:00:59.999999Z}]", timestampType)
            );
            assertBareDateVariableMatchesDateList(
                    driver,
                    "$today..$today+2d#XNYS;1h",
                    friday,
                    forTimestampType("[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T21:59:59.999999Z}]", timestampType)
            );
            // the seconds 00 and 30 of 15:00 and 16:00, an hour longer, join
            assertBareDateVariableMatchesDateList(
                    driver,
                    "$todayT[15:00@UTC,16:00]:[00,30]#XNYS;1h",
                    friday,
                    forTimestampType("[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T17:00:30.999999Z}]", timestampType)
            );
            Assert.assertEquals("error: Invalid date: 2025-01-24T25:00", parseTickExpr(driver, "$todayT25:00#XNYS;1h", friday));
            Assert.assertEquals(parseTickExpr(driver, "$todayT25:00;1h", friday), parseTickExpr(driver, "$todayT25:00#XNYS;1h", friday));
            // a second ';' keeps each date parsing the whole suffix, as without the tick calendar
            Assert.assertEquals("error: Invalid interval format: 2025-01-24;1h;1d", parseTickExpr(driver, "$today#XNYS;1h;1d", friday));
            Assert.assertEquals(parseTickExpr(driver, "$today;1h;1d", friday), parseTickExpr(driver, "$today#XNYS;1h;1d", friday));
            // without a duration, the tick calendar keeps the minutes, and without a tick calendar, the
            // duration makes each minute an hour long
            assertBareDateVariableMatchesDateList(
                    driver,
                    "$todayT[15:00,16:00]#XNYS",
                    friday,
                    forTimestampType("[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T15:00:59.999999Z},{lo=2025-01-24T16:00:00.000000Z, hi=2025-01-24T16:00:59.999999Z}]", timestampType)
            );
            assertBareDateVariableMatchesDateList(
                    driver,
                    "$today#XNYS",
                    friday,
                    forTimestampType("[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T20:59:59.999999Z}]", timestampType)
            );
            for (String expression : new String[]{"$todayT[15:00,16:00]#Fri;1h", "$todayT[15:00,16:00];1h"}) {
                assertBareDateVariableMatchesDateList(
                        driver,
                        expression,
                        friday,
                        forTimestampType("[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T16:59:59.999999Z}]", timestampType)
                );
            }
        }
    }

    @Test
    public void testBareDateVariableWithTickCalendarSteadyStateAllocatesNoJavaHeap() throws SqlException {
        // Filters parse a bind value per execution and a string column per row. Once the thread has
        // parsed it, a bare date variable expression with a tick calendar and a duration, whose time
        // slots hold one another and unite before the tick calendar runs, parses without allocating.
        // The minute of 15:00 on Friday 2025-01-24 holds the second 15:00:30, the session from 14:30
        // to 21:00Z holds the minute, and the duration extends it by a minute.
        final String expression = "$todayT[15:00,15:00:30]#XNYS;1m";
        try (TestUtils.ThreadMetricsScope<com.sun.management.ThreadMXBean> scope = TestUtils.threadAllocationScope()) {
            final com.sun.management.ThreadMXBean threadMXBean = scope.getBean();
            for (int timestampType : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
                final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
                final String context = expression + " [" + ColumnType.nameOf(timestampType) + ']';
                final long friday = driver.parseFloorLiteral(FRIDAY_NOON);
                final long expectedLo = driver.parseFloorLiteral("2025-01-24T15:00:00.000000Z");
                final long expectedHi = driver.parseFloorLiteral("2025-01-24T15:02:00.000000Z") - 1;
                final LongList out = new LongList();
                final StringSink scratch = new StringSink();
                for (int i = 0; i < 20_000; i++) {
                    out.clear();
                    IntervalUtils.parseTickExpr(driver, configuration, expression, 0, expression.length(), 0, out, scratch, friday, IntervalUtils.TICK_CHECK_ALL);
                }
                long minAllocatedBytes = Long.MAX_VALUE;
                int expectedParseCount = 0;
                for (int round = 0; round < 5; round++) {
                    final long allocatedBefore = threadMXBean.getCurrentThreadAllocatedBytes();
                    for (int i = 0; i < 10_000; i++) {
                        out.clear();
                        IntervalUtils.parseTickExpr(driver, configuration, expression, 0, expression.length(), 0, out, scratch, friday, IntervalUtils.TICK_CHECK_ALL);
                        if (out.size() == 2 && out.getQuick(0) == expectedLo && out.getQuick(1) == expectedHi) {
                            expectedParseCount++;
                        }
                    }
                    minAllocatedBytes = Math.min(minAllocatedBytes, threadMXBean.getCurrentThreadAllocatedBytes() - allocatedBefore);
                }
                Assert.assertEquals(context, forTimestampType("[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T15:01:59.999999Z}]", timestampType), intervalToString(driver, out));
                Assert.assertEquals(context, 50_000, expectedParseCount);
                Assert.assertEquals(context, 0, minAllocatedBytes);
            }
        }
    }

    @Test
    public void testBareDateVariableWithTickCalendarUnitesNestedSlots() {
        // Time slots of a bare date variable expression with a tick calendar and a duration that
        // hold one another, a minute and a second in it, an hour and a minute in it, unite before
        // the tick calendar runs, and the duration extends what it keeps once. The tick calendar
        // unites the intervals of the date list spelling and of the IR the same way before it cuts
        // them. On Friday 2025-01-24, XNYS trades from 14:30 to 21:00Z;
        // on Friday 2025-04-04, from 13:30 to 20:00Z, and the point 13:30:00Z lies in the hour 13.
        final String[][] cases = {
                {FRIDAY_NOON, "$todayT[15:00,15:00:30]#XNYS;30s", "[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T15:01:29.999999Z}]"},
                {FRIDAY_NOON, "$todayT[15:00:30,15:00]#XNYS;30s", "[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T15:01:29.999999Z}]"},
                {FRIDAY_NOON, "$todayT[15,15:30]#XNYS;30m", "[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T16:29:59.999999Z}]"},
                {FRIDAY_NOON, "$todayT[15@UTC,10:30@America/New_York]#XNYS;30m", "[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T16:29:59.999999Z}]"},
                {FRIDAY_NOON, "$todayT[15:00,15:00:30]#XNYS;1m", "[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T15:01:59.999999Z}]"},
                {"2025-04-04T12:00:00.000000Z", "$todayT[13,13:30:00Z]#XNYS;10m", "[{lo=2025-04-04T13:30:00.000000Z, hi=2025-04-04T14:09:59.999999Z}]"},
                {FRIDAY_NOON, "$today..$today+1dT[15:00,15:00:30]#XNYS;1m", "[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T15:01:59.999999Z}]"}
        };
        for (int timestampType : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
            final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
            for (String[] c : cases) {
                Assert.assertEquals(
                        c[1] + " [" + ColumnType.nameOf(timestampType) + ']',
                        forTimestampType(c[2], timestampType),
                        parseTickExpr(driver, c[1], driver.parseFloorLiteral(c[0]))
                );
            }
        }
    }

    @Test
    public void testBareDollarWithExchangeFilter() throws SqlException {
        // $today#XNYS takes the bare $ path (IntervalUtils lines 702-766)
        // With today = 2025-01-24 (Friday, trading day), this should produce
        // XNYS trading hours, not a full-day interval.
        long nowTimestamp = Micros.toMicros(2025, 1, 24, 12, 0);
        assertTickIntervalWithNow(
                "[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T20:59:59.999999Z}]",
                "$today#XNYS",
                nowTimestamp
        );
        // Compare: the bracketed equivalent works correctly
        assertTickIntervalWithNow(
                "[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T20:59:59.999999Z}]",
                "[$today]#XNYS",
                nowTimestamp
        );
    }

    @Test
    public void testBareDurationPerDateKeepsStaticInversionWindows() {
        // WhereClauseParser's static inversion of a literal that compileTickExpr() rejects parses a
        // bare date variable expression with a tick calendar and a duration with
        // isBareDurationPerDate: each date and time slot is a window as wide as the duration from
        // where it starts, the tick calendar cuts the windows, and the duration extends what it keeps
        // again. Without it, as every other caller parses, each keeps its own extent, and the
        // duration extends what the tick calendar keeps once. Without a ';', with a day filter, or
        // with another ';', both parse alike. XNYS trades from 14:30 to 21:00Z on Friday 2025-01-24
        // and the days before it, from 13:30 to 20:00Z on Friday 2025-04-04, and not on Saturday
        // 2025-01-25.
        final String[][] cases = {
                // now, expression, with isBareDurationPerDate, without it
                {
                        FRIDAY_NOON,
                        "$todayT[15:00,16:00]#XNYS;1h",
                        "[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T17:59:59.999999Z}]",
                        "[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T17:00:59.999999Z}]"
                },
                {
                        FRIDAY_NOON,
                        "$todayT15:00:00Z#XNYS;1h",
                        "[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T16:59:59.999999Z}]",
                        "[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T16:00:00.000000Z}]"
                },
                // a window of no length ends before it starts
                {
                        FRIDAY_NOON,
                        "$todayT15:00:00Z#XNYS;0h",
                        "[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T14:59:59.999999Z}]",
                        "[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T15:00:00.000000Z}]"
                },
                {
                        FRIDAY_NOON,
                        "$todayT15:00:00Z#XNYS;",
                        "[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T14:59:59.999999Z}]",
                        "[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T15:00:00.000000Z}]"
                },
                // a negative duration makes the window the hour before the point
                {
                        FRIDAY_NOON,
                        "$todayT15:00:00Z#XNYS;-1h",
                        "[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T13:59:59.999999Z}]",
                        "[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T14:00:00.000000Z}]"
                },
                // 14:00Z lies before the session, its window from 14:00 into it
                {
                        FRIDAY_NOON,
                        "$todayT15:00:00+01:00#XNYS;1h",
                        "[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T15:59:59.999999Z}]",
                        "[]"
                },
                {
                        FRIDAY_NOON,
                        "$todayT[15:00:00Z,17:00:00Z]#XNYS;1h",
                        "[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T18:59:59.999999Z}]",
                        "[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T16:00:00.000000Z},{lo=2025-01-24T17:00:00.000000Z, hi=2025-01-24T18:00:00.000000Z}]"
                },
                {
                        FRIDAY_NOON,
                        "$todayT[15:00:00Z,18:00:00Z]#XNYS;1h",
                        "[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T16:59:59.999999Z},{lo=2025-01-24T18:00:00.000000Z, hi=2025-01-24T19:59:59.999999Z}]",
                        "[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T16:00:00.000000Z},{lo=2025-01-24T18:00:00.000000Z, hi=2025-01-24T19:00:00.000000Z}]"
                },
                {
                        FRIDAY_NOON,
                        "$todayT20:30:00Z#XNYS;4h",
                        "[{lo=2025-01-24T20:30:00.000000Z, hi=2025-01-25T00:59:59.999999Z}]",
                        "[{lo=2025-01-24T20:30:00.000000Z, hi=2025-01-25T00:30:00.000000Z}]"
                },
                {
                        FRIDAY_NOON,
                        "$todayT[15:00@UTC,16:30@UTC]@Bogus#XNYS;1h",
                        "[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T18:29:59.999999Z}]",
                        "[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T16:00:59.999999Z},{lo=2025-01-24T16:30:00.000000Z, hi=2025-01-24T17:30:59.999999Z}]"
                },
                {
                        FRIDAY_NOON,
                        "$todayT[15:00@UTC,16:00@UTC]:[00,30]@Bogus#XNYS;1h",
                        "[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T18:00:29.999999Z}]",
                        "[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T17:00:30.999999Z}]"
                },
                {
                        FRIDAY_NOON,
                        "$todayT[15:00,]#XNYS;1h",
                        "[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T16:59:59.999999Z}]",
                        "[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T16:00:59.999999Z}]"
                },
                {
                        FRIDAY_NOON,
                        "$today-3d..$todayT15:00:00Z#XNYS;1h",
                        "[{lo=2025-01-21T15:00:00.000000Z, hi=2025-01-21T16:59:59.999999Z},{lo=2025-01-22T15:00:00.000000Z, hi=2025-01-22T16:59:59.999999Z},"
                                + "{lo=2025-01-23T15:00:00.000000Z, hi=2025-01-23T16:59:59.999999Z},{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T16:59:59.999999Z}]",
                        "[{lo=2025-01-21T15:00:00.000000Z, hi=2025-01-21T16:00:00.000000Z},{lo=2025-01-22T15:00:00.000000Z, hi=2025-01-22T16:00:00.000000Z},"
                                + "{lo=2025-01-23T15:00:00.000000Z, hi=2025-01-23T16:00:00.000000Z},{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T16:00:00.000000Z}]"
                },
                {
                        FRIDAY_NOON,
                        "$today..$today+2dT15:00:00Z#XNYS;1h",
                        "[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T16:59:59.999999Z}]",
                        "[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T16:00:00.000000Z}]"
                },
                // the hour-wide window of the day ends before the session starts
                {FRIDAY_NOON, "$today#XNYS;1h", "[]", "[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T21:59:59.999999Z}]"},
                // slots that the windows tile, and that they do not
                {
                        FRIDAY_NOON,
                        "$todayT[15:00,15:00:30]#XNYS;30s",
                        "[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T15:01:29.999999Z}]",
                        "[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T15:01:29.999999Z}]"
                },
                {
                        FRIDAY_NOON,
                        "$todayT[15:00,15:00:30]#XNYS;1m",
                        "[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T15:02:29.999999Z}]",
                        "[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T15:01:59.999999Z}]"
                },
                {
                        "2025-04-04T12:00:00.000000Z",
                        "$todayT[13,13:30:00Z]#XNYS;10m",
                        "[{lo=2025-04-04T13:30:00.000000Z, hi=2025-04-04T13:49:59.999999Z}]",
                        "[{lo=2025-04-04T13:30:00.000000Z, hi=2025-04-04T14:09:59.999999Z}]"
                },
                {FRIDAY_NOON, "$todayT09:00:00Z#XNYS;1h", "[]", "[]"},
                {"2025-01-25T12:00:00.000000Z", "$todayT15:00:00Z#XNYS;1h", "[]", "[]"},
                // without a ';', with a day filter, and with another ';'
                {
                        FRIDAY_NOON,
                        "$todayT[15:00,16:00]#XNYS",
                        "[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T15:00:59.999999Z},{lo=2025-01-24T16:00:00.000000Z, hi=2025-01-24T16:00:59.999999Z}]",
                        "[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T15:00:59.999999Z},{lo=2025-01-24T16:00:00.000000Z, hi=2025-01-24T16:00:59.999999Z}]"
                },
                {
                        FRIDAY_NOON,
                        "$todayT[15:00,16:00]#Fri;1h",
                        "[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T16:59:59.999999Z}]",
                        "[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T16:59:59.999999Z}]"
                },
                {FRIDAY_NOON, "$todayT[15:00,16:00]#XNYS;1h;1d;2", "error: Expected number before unit ';'", "error: Expected number before unit ';'"},
                // errors
                {FRIDAY_NOON, "$todayT[15:00,25:00]#XNYS;1x", "error: Invalid duration unit: x", "error: Invalid duration unit: x"},
                {FRIDAY_NOON, "$tody#XNYS;1h", "error: Unknown date variable: $tody", "error: Unknown date variable: $tody"}
        };
        for (int timestampType : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
            final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
            for (String[] c : cases) {
                final long now = driver.parseFloorLiteral(c[0]);
                final String context = c[1] + " at " + c[0] + " [" + ColumnType.nameOf(timestampType) + ']';
                Assert.assertEquals(context, forTimestampType(c[2], timestampType), parseTickExpr(driver, c[1], now, IntervalUtils.TICK_CHECK_ALL, true));
                Assert.assertEquals(context, forTimestampType(c[3], timestampType), parseTickExpr(driver, c[1], now, IntervalUtils.TICK_CHECK_ALL, false));
                // the overload without the parameter parses without it
                Assert.assertEquals(context, forTimestampType(c[3], timestampType), parseTickExpr(driver, c[1], now));
            }
        }
    }

    @Test
    public void testBareDurationPerDateLeavesOtherForms() throws SqlException {
        // isBareDurationPerDate changes only how the parser takes a bare date variable expression
        // with a tick calendar and a duration. An interval without a date variable, with or without
        // a timezone, a tick calendar and a duration, a date list, and dates of month precision
        // parse alike with it and without it, and as their IR evaluates them. Without a tick
        // calendar, a duration makes each date as long as the duration; with one, the duration
        // extends what the session of Friday 2025-01-24, from 14:30 to 21:00Z, keeps of the minute.
        for (int timestampType : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
            final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
            final long friday = driver.parseFloorLiteral(FRIDAY_NOON);
            final String[][] cases = {
                    {"2025-01-24T15:00@UTC#XNYS;1h", "[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T16:00:59.999999Z}]"},
                    {"2025-01-24T15:00#XNYS;1h", "[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T16:00:59.999999Z}]"},
                    {"2025-01-24T15:00@UTC;1h", "[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T15:59:59.999999Z}]"},
                    {"2025-01-24T15:00@UTC", "[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T15:00:59.999999Z}]"},
                    {"2025-01-24T15:00;1h", "[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T15:59:59.999999Z}]"},
                    {"[$today]T15:00#XNYS;1h", "[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T16:00:59.999999Z}]"},
                    // the IR takes a date of its own beside a date variable
                    {"[2025-01-24, $today]T15:00;1h", "[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T15:59:59.999999Z}]"}
            };
            for (String[] c : cases) {
                assertParsedAlikeWithAndWithoutDurationPerDate(driver, c[0], friday, forTimestampType(c[1], timestampType));
            }
            // every day of January and February 2025 at 09:30, an hour long, and every day of
            // January at 09:30, a minute long, which today, Friday 2025-01-24, is one of
            final LongList twoMonths = new LongList();
            final LongList january = new LongList();
            for (LocalDate day = LocalDate.of(2025, 1, 1); day.getMonthValue() < 3; day = day.plusDays(1)) {
                final long dayStart = driver.fromDays((int) day.toEpochDay());
                twoMonths.add(dayStart + driver.fromMinutes(9 * 60 + 30), dayStart + driver.fromMinutes(10 * 60 + 30) - 1);
                if (day.getMonthValue() == 1) {
                    january.add(dayStart + driver.fromMinutes(9 * 60 + 30), dayStart + driver.fromMinutes(9 * 60 + 31) - 1);
                }
            }
            Assert.assertEquals(2 * 59, twoMonths.size());
            assertParsedAlikeWithAndWithoutDurationPerDate(driver, "2025-[01,02]T09:30;1h", friday, intervalToString(driver, twoMonths));
            assertParsedAlikeWithAndWithoutDurationPerDate(driver, "[2025-01, $today]T09:30", friday, intervalToString(driver, january));
        }
    }

    @Test
    public void testBracketExpansionAllFilteredOut() throws SqlException {
        // Bracket expansion where all dates are non-trading days (weekend)
        // 2025-01-[25..26]#XNYS - both are weekend days (Sat, Sun)
        assertTickInterval(
                "[]",
                "2025-01-[25..26]#XNYS"
        );
    }

    @Test
    public void testBracketExpansionSingleElementWithExchangeFilter() throws SqlException {
        // Single bracket expansion element in list with exchange filter
        // [2025-01-[24..28]]#XNYS - single element that expands to multiple dates
        // Jan 24 (Fri), 25 (Sat), 26 (Sun), 27 (Mon), 28 (Tue)
        // Trading days: Jan 24, 27, 28
        assertTickInterval(
                "[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T20:59:59.999999Z}," +
                        "{lo=2025-01-27T14:30:00.000000Z, hi=2025-01-27T20:59:59.999999Z}," +
                        "{lo=2025-01-28T14:30:00.000000Z, hi=2025-01-28T20:59:59.999999Z}]",
                "[2025-01-[24..28]]#XNYS"
        );
    }

    @Test
    public void testBracketExpansionWithExchangeFilterAndTimeSuffix() throws SqlException {
        // Bracket expansion with exchange filter and time suffix
        // 2025-02-[03..05]T03:00#XHKG - Mon, Tue, Wed
        // 03:00 UTC is within XHKG morning session (01:30-04:00)
        assertTickInterval(
                "[{lo=2025-02-03T03:00:00.000000Z, hi=2025-02-03T03:00:59.999999Z}," +
                        "{lo=2025-02-04T03:00:00.000000Z, hi=2025-02-04T03:00:59.999999Z}," +
                        "{lo=2025-02-05T03:00:00.000000Z, hi=2025-02-05T03:00:59.999999Z}]",
                "2025-02-[03..05]T03:00#XHKG"
        );
    }

    @Test
    public void testBracketExpansionWithExchangeFilterTimeSuffixAfternoon() throws SqlException {
        // Bracket expansion with exchange filter and time suffix in afternoon session
        // 2025-02-[03..05]T06:00#XHKG - Mon, Tue, Wed
        // 06:00 UTC is within XHKG afternoon session (05:00-08:00)
        assertTickInterval(
                "[{lo=2025-02-03T06:00:00.000000Z, hi=2025-02-03T06:00:59.999999Z}," +
                        "{lo=2025-02-04T06:00:00.000000Z, hi=2025-02-04T06:00:59.999999Z}," +
                        "{lo=2025-02-05T06:00:00.000000Z, hi=2025-02-05T06:00:59.999999Z}]",
                "2025-02-[03..05]T06:00#XHKG"
        );
    }

    @Test
    public void testBracketExpansionWithTimezoneAndExchangeFilter() throws SqlException {
        // Bracket expansion with timezone and exchange filter
        // 2025-01-[24..28]@-05:00#XNYS
        // Each date in -05:00 timezone, then intersected with XNYS trading hours
        // Jan 24 (Fri), 27 (Mon), 28 (Tue) are trading days; 25 (Sat), 26 (Sun) are not
        // Each day in -05:00 = [05:00 UTC, next day 04:59:59.999999 UTC]
        // XNYS hours: 14:30-21:00 UTC -> intersection: [14:30, 20:59:59.999999]
        assertTickInterval(
                "[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T20:59:59.999999Z}," +
                        "{lo=2025-01-27T14:30:00.000000Z, hi=2025-01-27T20:59:59.999999Z}," +
                        "{lo=2025-01-28T14:30:00.000000Z, hi=2025-01-28T20:59:59.999999Z}]",
                "2025-01-[24..28]@-05:00#XNYS"
        );
    }

    @Test
    public void testBracketTimezoneWithExchangeFilter() throws SqlException {
        // Bracket notation with NYSE timezone (EST = -05:00) and exchange filter
        // [2025-01-24@-05:00#XNYS]
        // 2025-01-24 (Fri) in -05:00 = [2025-01-24T05:00:00Z, 2025-01-25T04:59:59.999999Z]
        // XNYS Jan 24 (Fri): 14:30-21:00 UTC -> intersection: [14:30, 20:59:59.999999]
        assertTickInterval(
                "[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T20:59:59.999999Z}]",
                "[2025-01-24@-05:00#XNYS]"
        );
    }

    @Test
    public void testCompiledStaticDateConvertsSlotZonesOnce() throws SqlException {
        // A time slot of a fixed date in a compiled list with a tick calendar and a duration converts
        // to UTC once, with its own timezone, or else with the timezone of the list, as the parser
        // converts it for a bind variable. The compiled form used to convert a slot with its own
        // timezone a second time, with the timezone of the list. XNYS trades from 14:30 to 21:00Z on
        // Thursday 2025-01-23, Friday 2025-01-24 and Monday 2025-01-27; 09:30 in New York is 14:30Z,
        // and "today" in Paris, Tokyo or at an offset is the day of that timezone. X24H trades all day,
        // so the hour from 01:00 that New York repeats on 2025-11-02 keeps both of its instants.
        final String nov4 = "2025-11-04T12:00:00.000000Z";
        final String[][] cases = {
                // now, expression, "bind" where the parser reads the text alike, then the intervals
                {MONDAY_NOON, "[$today, 2025-01-24T[09:30@America/New_York]]@Europe/Paris#XNYS;1h", "bind", "2025-01-24T14:30", "2025-01-24T15:30:59.999999", "2025-01-27T14:30", "2025-01-27T21:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-24T[09:30@America/New_York]]@Europe/Paris#XNYS;1h1m", "bind", "2025-01-24T14:30", "2025-01-24T15:31:59.999999", "2025-01-27T14:30", "2025-01-27T22:00:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-[23..24]T[09:30@America/New_York]]@Europe/Paris#XNYS;1h1m", "bind", "2025-01-23T14:30", "2025-01-23T15:31:59.999999", "2025-01-24T14:30", "2025-01-24T15:31:59.999999", "2025-01-27T14:30", "2025-01-27T22:00:59.999999"},
                // today in Tokyo ends at 15:00Z
                {MONDAY_NOON, "[$today, 2025-01-24T[15:00@UTC]]@Asia/Tokyo#XNYS;1h", "bind", "2025-01-24T15:00", "2025-01-24T16:00:59.999999", "2025-01-27T14:30", "2025-01-27T15:59:59.999999"},
                // 16:00 in Tokyo is 07:00Z
                {MONDAY_NOON, "[$today, 2025-01-24T[15:00@UTC,16:00]]@Asia/Tokyo#XNYS;1h", "bind", "2025-01-24T15:00", "2025-01-24T16:00:59.999999", "2025-01-27T14:30", "2025-01-27T15:59:59.999999"},
                // both slots are 15:00Z
                {MONDAY_NOON, "[$today, 2025-01-24T[15:00@UTC,10:00]]@-05:00#XNYS;1h", "bind", "2025-01-24T15:00", "2025-01-24T16:00:59.999999", "2025-01-27T14:30", "2025-01-27T21:59:59.999999"},
                // 14:00Z lies before the session
                {MONDAY_NOON, "[$today, 2025-01-24T[14:00@UTC]]@-01:00#XNYS;2h", "bind", "2025-01-27T14:30", "2025-01-27T22:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-24T[15:00@-01:00]]@+01:00#XNYS;3h", "bind", "2025-01-24T16:00", "2025-01-24T19:00:59.999999", "2025-01-27T14:30", "2025-01-27T23:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-24T[15:00@+01:00]]@+01:00#XNYS;1h", "bind", "2025-01-27T14:30", "2025-01-27T21:59:59.999999"},
                // a date with an offset of its own reads as an instant, which the timezone of the list
                // shifts again, to 14:00Z, see bug-explicit-offset-shifted-by-zone
                {MONDAY_NOON, "[$today, 2025-01-24T15:00:00Z]@Europe/Paris#XNYS;1h", "bind", "2025-01-27T14:30", "2025-01-27T21:59:59.999999"},
                // a slot at 06:30Z on the day New York repeats 01:00, a minute longer, and the day of
                // 2025-11-04 in New York, from 05:00Z, a minute longer; a slot taken to the local time of
                // the list and back would add 05:30Z, which repeats 01:30 too
                {nov4, "[$today, 2025-11-02T[06:30@UTC]]@America/New_York#X24H;1m", "bind", "2025-11-02T06:30", "2025-11-02T06:31:59.999999", "2025-11-04T05:00", "2025-11-05T05:00:59.999999"},
                {nov4, "[$today, 2025-11-02T01:30]@America/New_York#X24H;1m", "bind", "2025-11-02T05:30", "2025-11-02T05:31:59.999999", "2025-11-02T06:30", "2025-11-02T06:31:59.999999", "2025-11-04T05:00", "2025-11-05T05:00:59.999999"}
        };
        assertCompiledStaticDates(configuration, cases);
        // 15:00 at -05:00 is 20:00Z on each trading day of January, which the session holds, an hour
        // longer; today's session, an hour longer, holds it on 2025-01-27
        for (int timestampType : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
            final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
            final long now = driver.parseFloorLiteral(MONDAY_NOON);
            final String expected = slotOnSessionDays(driver, "XNYS", "2025-01-01", "2025-02-01", driver.fromHours(20), driver.fromMinutes(1), driver.fromHours(1), "2025-01-27T14:30", "2025-01-27T22:00");
            assertCompiledStaticDate(configuration, driver, "[$today, 2025-01T[15:00@-05:00]]@Europe/Paris#XNYS;1h", now, expected, true);
        }
    }

    @Test
    public void testCompiledStaticDateIntervalLimit() throws SqlException {
        // The compiled form of a list with a fixed date, a tick calendar and a duration counts the own
        // extent of each fixed date against the interval limit, as the parser counts it for a bind
        // variable, where it counted windows as long as the duration, which merged: it fails where the
        // bind variable fails, at the same "now", as its SQL compiles and as it runs. At a merge
        // threshold of 8 and a limit of 16, on Monday 2025-01-27, the minute at 15:00 of each day of a
        // range of days stays apart, and so do the 24 sessions of XHKG on the 12 trading days from
        // 2025-01-02 to 2025-01-17 once each ends 30 minutes later.
        final CairoConfiguration smallLimits = calendarConfiguration(8, 16);
        final String dateRangeError = "error: Date range expansion produces too many intervals (max 16)";
        final String bracketError = "error: Bracket expansion produces too many intervals (max 16)";
        for (int timestampType : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
            final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
            final long now = driver.parseFloorLiteral(MONDAY_NOON);
            // the check after each day of a range, and the check at the end, of the IR
            for (String[] c : new String[][]{
                    {"[2025-01-02, $today-20d..$today]T15:00#XNYS;30d", dateRangeError},
                    {"[2025-01-[02..09], $today-12d..$today]T15:00#XNYS;1d", dateRangeError},
                    {"[$today, 2025-01-[02..17]]#XHKG;30m", bracketError}
            }) {
                assertCompiledStaticDateFails(smallLimits, driver, c[0], now, false, c[1]);
                Assert.assertEquals(c[0], c[1], parseTickExpr(smallLimits, driver, c[0], now, IntervalUtils.TICK_CHECK_ALL));
            }
            // a fixed date whose own extents alone exceed the limit: the list parses as its bind
            // variable does, which fails where no earlier element holds them
            for (String[] c : new String[][]{
                    {"[$today, 2025-01-24T[00..23]:00:00]#XNYS;1h", bracketError},
                    // the minutes at 15:00Z and at 03:00Z of 20 days, whose windows used to be empty
                    {"[$today, 2025-01-[01..20]T[15:00@UTC]]#XNYS;0h", bracketError},
                    {"[$today, 2025-01-[01..20]T[03:00@UTC]]#XNYS;0h", bracketError}
            }) {
                assertCompiledStaticDateFails(smallLimits, driver, c[0], now, true, c[1]);
                Assert.assertEquals(c[0], c[1], parseTickExpr(smallLimits, driver, c[0], now, IntervalUtils.TICK_CHECK_ALL));
            }
            for (String expression : new String[]{
                    "[$today, 2025-01-24T[00..23]:[00..59]:00]#XNYS;1m",
                    "[$today, 2025-[01..02]-[01..28]T[00..13]:[00,30]]#XNYS;30m"
            }) {
                assertCompiledStaticDateFails(configuration, driver, expression, now, true, "error: Bracket expansion produces too many intervals (max 1024)");
                Assert.assertEquals(expression, "error: Bracket expansion produces too many intervals (max 1024)", parseTickExpr(driver, expression, now));
            }
            // The IR counts the own extents of a fixed date apart from the elements before it as it
            // compiles, so the list keeps its result where the parser counts the 8 minutes of the fixed
            // date with the 13 of the range before them and fails: the minute at 15:00 of each trading
            // day, a day longer
            final String rangeFirst = "[$today-12d..$today, 2025-01-[02..09]]T15:00#XNYS;1d";
            assertCompiledStaticDate(
                    smallLimits,
                    driver,
                    rangeFirst,
                    now,
                    expectedIntervals(
                            timestampType,
                            "2025-01-02T15:00", "2025-01-04T15:00:59.999999",
                            "2025-01-06T15:00", "2025-01-09T15:00:59.999999",
                            "2025-01-15T15:00", "2025-01-18T15:00:59.999999",
                            "2025-01-21T15:00", "2025-01-25T15:00:59.999999",
                            "2025-01-27T15:00", "2025-01-28T15:00:59.999999"
                    ),
                    false
            );
            Assert.assertEquals(rangeFirst, bracketError, parseTickExpr(smallLimits, driver, rangeFirst, now, IntervalUtils.TICK_CHECK_ALL));
        }
    }

    @Test
    public void testCompiledStaticDateKeepsCompileErrors() {
        // A compiled list with a fixed date, a tick calendar and a duration fails to compile with the
        // error it has always failed with, at the position of the literal: the error of the same list
        // with the day filter #Fri for the tick calendar, which compiles each fixed date with the
        // duration as it always has. A ';' of a fixed date before the ';' of the list makes the parse of
        // the date read a repeating interval, or too many parts; a ';' after a zone of a time slot is
        // part of the zone, see testCompiledStaticDateKeepsZoneSwallowedSemicolon.
        final String[][] cases = {
                {"[$today, 2025-01-24;1h;1d]#XNYS;1h", "Count not a number"},
                {"[$today, 2025-01-24;1h;1d]#XHKG;1h", "Count not a number"},
                {"[$today, 2025-01-24;1h;1d]@America/New_York#XNYS;1h", "Count not a number"},
                {"[$today, 2025-01-24;1h;1d]#XNYS;", "Count not a number"},
                {"[2025-01-24;1h;1d, $today]#XNYS;1h", "Count not a number"},
                {"[$today, 2025-01-24;1h;1d, 2025-01-23]#XNYS;1h", "Count not a number"},
                {"[$today, 2025-01-24;1h;1d]T15:00#XNYS;1h", "Period not a number"},
                {"[$today, 2025-01-24;1h]#XNYS;1h", "Invalid interval format: 2025-01-24;1h;1h"},
                {"[$today, 2025-01-24;]#XNYS;1h", "Invalid interval format: 2025-01-24;;1h"},
                {"[$today, 2025-01-24;;]#XNYS;1h", "Period not a number"},
                {"[$today, 2025-01-24;1h;1d;2]#XNYS;1h", "Invalid interval format (too many semicolons): 2025-01-24;1h;1d;2;1h"},
                {"[$today, 2025-01-24;30m;2d;2]#XNYS;1h", "Invalid interval format (too many semicolons): 2025-01-24;30m;2d;2;1h"},
                {"[$today, 2025-01-[24,25];1h;1d]#XNYS;1h", "Count not a number"},
                {"[$today, 2025-01-24T[15:00;1h;1d]]#XNYS;1h", "Count not a number"},
                {"[$today, 2025-01-24T[15:00,16:00;1h;1d]]#XNYS;1h", "Count not a number"},
                {"[$today, 2025-01-24T15:00;1h;1d]#XNYS;1h", "Count not a number"},
                {"[$today, 2025-[01..02]T15:00:00-05;1h;1d]#XNYS;1h", "Count not a number"},
                {"[$today, 2025-01-[24,25;1h;1d]]#XNYS;1h", "Expected ',' or end of bracket"},
                {"[$today, 2025-01-24]#XNYS;1h1;1d1;", "Period not a number"},
                {"[$today, 2025-01-24]#XNYS;1;", "Invalid interval format: 2025-01-24;1;"},
                {"[$today, 2025-01-24]#XNYS;1;23;", "Count not a number"},
                {"[$today;1h;1d, 2025-01-24]#XNYS;1h", "Unknown date variable: $today;1h;1d"},
                // the date, the time or the duration does not parse
                {"[$today, 2025-01-32]#XNYS;1h", "Invalid date: 2025-01-32"},
                {"[$today, 2025-01-[24,32]]#XNYS;1h", "Invalid date: 2025-01-32"},
                {"[$today, 2025-01-24T25:00]#XNYS;1h", "Invalid date: 2025-01-24T25:00"},
                {"[$today, 2025-01-24T15Z]#XNYS;1h", "Invalid date: 2025-01-24T15Z"},
                {"[$today, 2025-01-24 15:00:00]#XNYS;1h", "Invalid date: 2025-01-24 15:00:00"},
                {"[$today, 2025-01-24]#XNYS;1x", "Invalid duration unit: x"},
                {"[$today, 2025-01-24T[15:00,25:00]]#XNYS;1x", "Invalid duration unit: x"},
                {"[$today, 2025-01-24]#XNYS;h", "Expected number before unit 'h'"},
                {"[$today, 2025-01-24]#XNYS;1h;1d;2", "Expected number before unit ';'"}
        };
        for (int timestampType : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
            final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
            for (String[] c : cases) {
                assertCompileError(configuration, driver, c[0], c[1]);
                assertCompileError(configuration, driver, c[0].replace("#XNYS", "#Fri").replace("#XHKG", "#Fri"), c[1]);
            }
        }
        // nine digits of a second are more than microseconds hold
        final TimestampDriver micros = ColumnType.getTimestampDriver(ColumnType.TIMESTAMP_MICRO);
        assertCompileError(configuration, micros, "[$today, 2025-01-24T15:00:00.123456789Z]#XNYS;1h", "Invalid date: 2025-01-24T15:00:00.123456789Z");
        // the windows of the days of these fixed dates stay apart, so their list fails the limit as it
        // compiles, which the own extents, whole days that join, would not: at a limit of 16, the 14 trading
        // days of 2025-01-01 to 2025-01-20 an hour longer, and at the default limit, the trading days
        // of four years
        assertCompileError(calendarConfiguration(8, 16), micros, "[$today, 2025-01-[01..20]]#XNYS;1h", "Bracket expansion produces too many intervals (max 16)");
        assertCompileError(configuration, micros, "[$today, [2022..2025]-[01..12]-[01..28]]#XNYS;1h", "Bracket expansion produces too many intervals (max 1024)");
    }

    @Test
    public void testCompiledStaticDateKeepsItsExtentUnderTickCalendar() throws SqlException {
        // A fixed date in a compiled list with a tick calendar and a duration keeps its own extent,
        // a day, an hour, a minute, a second or a point in time, as the parser keeps it for a bind
        // variable: the tick calendar keeps what its sessions hold of it, and the duration extends
        // what they keep, once. The compiled form used to make each fixed date a window as long as the
        // duration from its start, which the calendar cut and the duration extended again. XNYS trades
        // from 14:30 to 21:00Z on Thursday 2025-01-23, Friday 2025-01-24, Monday 2025-01-27 and Tuesday
        // 2025-01-28, from 13:30 to 20:00Z after New York moves to summer time on 2025-03-09, from 13:30
        // to 17:00Z on 2025-07-03, and not on weekends nor on the holiday 2025-01-20. XHKG trades from
        // 01:30 to 04:00Z and from 05:00 to 08:00Z. X24H trades all day on 1969-12-31, 1970-01-01,
        // in 2025 and on 2099-01-02 and 2099-01-03.
        final String jul3 = "2025-07-03T12:00:00.000000Z";
        final String[][] cases = {
                // now, expression, "bind" where the parser reads the text alike, "S2" otherwise, then
                // the intervals
                {MONDAY_NOON, "[$today, 2025-01-24]#XNYS;1h", "bind", "2025-01-24T14:30", "2025-01-24T21:59:59.999999", "2025-01-27T14:30", "2025-01-27T21:59:59.999999"},
                {MONDAY_NOON, "[2025-01-24, $today]#XNYS;1h", "bind", "2025-01-24T14:30", "2025-01-24T21:59:59.999999", "2025-01-27T14:30", "2025-01-27T21:59:59.999999"},
                {MONDAY_NOON, "[ $TODAY , 2025-01-24 ]#xnys;1h", "bind", "2025-01-24T14:30", "2025-01-24T21:59:59.999999", "2025-01-27T14:30", "2025-01-27T21:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-[23..24]]#XNYS;1h", "bind", "2025-01-23T14:30", "2025-01-23T21:59:59.999999", "2025-01-24T14:30", "2025-01-24T21:59:59.999999", "2025-01-27T14:30", "2025-01-27T21:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-W04-5]#XNYS;1h", "bind", "2025-01-24T14:30", "2025-01-24T21:59:59.999999", "2025-01-27T14:30", "2025-01-27T21:59:59.999999"},
                // a Saturday and a holiday without a session
                {MONDAY_NOON, "[$today, 2025-01-24, 2025-01-25]#XNYS;1h", "bind", "2025-01-24T14:30", "2025-01-24T21:59:59.999999", "2025-01-27T14:30", "2025-01-27T21:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-20]#XNYS;1h", "bind", "2025-01-27T14:30", "2025-01-27T21:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-24T15:00]#XNYS;1h", "bind", "2025-01-24T15:00", "2025-01-24T16:00:59.999999", "2025-01-27T14:30", "2025-01-27T21:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-24T15:00]#XNYS;30s", "bind", "2025-01-24T15:00", "2025-01-24T15:01:29.999999", "2025-01-27T14:30", "2025-01-27T21:00:29.999999"},
                {MONDAY_NOON, "[$today, 2025-01-24T15]#XNYS;2h", "bind", "2025-01-24T15:00", "2025-01-24T17:59:59.999999", "2025-01-27T14:30", "2025-01-27T22:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-24T15:00:00Z]#XNYS;1h", "bind", "2025-01-24T15:00", "2025-01-24T16:00", "2025-01-27T14:30", "2025-01-27T21:59:59.999999"},
                // 14:00Z lies before the session, 20:00Z in it
                {MONDAY_NOON, "[$today, 2025-01-24T15:00:00+01:00]#XNYS;1h", "bind", "2025-01-27T14:30", "2025-01-27T21:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-24T15:00:00-05]#XNYS;1h", "bind", "2025-01-24T20:00", "2025-01-24T21:00", "2025-01-27T14:30", "2025-01-27T21:59:59.999999"},
                // times that the literal reads as it always has, and the parser does not: 15:00:00UTC, which
                // the parser reads only with a duration, and 15:00:00.1234Z, which it reads as 15:00:00.234
                // with a duration, see bug-duration-date-parse-any-format-misreads
                {MONDAY_NOON, "[$today, 2025-01-24T09:00:00UTC]#XNYS;1h", "S2", "2025-01-27T14:30", "2025-01-27T21:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-24T15:00:00UTC]#XNYS;1h", "S2", "2025-01-24T15:00", "2025-01-24T16:00", "2025-01-27T14:30", "2025-01-27T21:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-24T15:00:00.1234Z]#XNYS;1h", "S2", "2025-01-24T15:00:00.234000", "2025-01-24T16:00:00.234000", "2025-01-27T14:30", "2025-01-27T21:59:59.999999"},
                // 02:30 in Jerusalem does not exist on 2025-03-28, so no interval is left
                {"2025-03-28T12:00:00.000000Z", "[$today, 2025-03-28]T02:30@Asia/Jerusalem#XHKG;1h", "bind"},
                // a negative, a zero and an empty duration
                {MONDAY_NOON, "[$today, 2025-01-24]#XNYS;-1h", "bind", "2025-01-24T14:30", "2025-01-24T19:59:59.999999", "2025-01-27T14:30", "2025-01-27T19:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-24]#XNYS;0h", "bind", "2025-01-24T14:30", "2025-01-24T20:59:59.999999", "2025-01-27T14:30", "2025-01-27T20:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-24]#XNYS;", "bind", "2025-01-24T14:30", "2025-01-24T20:59:59.999999", "2025-01-27T14:30", "2025-01-27T20:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-24T15:00]#XNYS;", "bind", "2025-01-24T15:00", "2025-01-24T15:00:59.999999", "2025-01-27T14:30", "2025-01-27T20:59:59.999999"},
                // days, months, years and compound durations
                {MONDAY_NOON, "[$today, 2025-01-23]#XNYS;2d", "bind", "2025-01-23T14:30", "2025-01-25T20:59:59.999999", "2025-01-27T14:30", "2025-01-29T20:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-24]#XNYS;1M", "bind", "2025-01-24T14:30", "2025-02-27T20:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-24]#XNYS;1y", "bind", "2025-01-24T14:30", "2026-01-27T20:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-24]#XNYS;1h30m", "bind", "2025-01-24T14:30", "2025-01-24T22:29:59.999999", "2025-01-27T14:30", "2025-01-27T22:29:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-24]#XNYS;2h-1h", "bind", "2025-01-24T14:30", "2025-01-24T21:59:59.999999", "2025-01-27T14:30", "2025-01-27T21:59:59.999999"},
                // two sessions a day, which an hour longer join, and half an hour shorter do not
                {MONDAY_NOON, "[$today, 2025-01-24]#XHKG;1h", "bind", "2025-01-24T01:30", "2025-01-24T08:59:59.999999", "2025-01-27T01:30", "2025-01-27T08:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-24]#XHKG;-30m", "bind", "2025-01-24T01:30", "2025-01-24T03:29:59.999999", "2025-01-24T05:00", "2025-01-24T07:29:59.999999", "2025-01-27T01:30", "2025-01-27T03:29:59.999999", "2025-01-27T05:00", "2025-01-27T07:29:59.999999"},
                // the timezone of the list: the day of 2025-01-24 at +08:00 starts at 16:00Z the day before
                {MONDAY_NOON, "[$today, 2025-01-24]@America/New_York#XNYS;1h", "bind", "2025-01-24T14:30", "2025-01-24T21:59:59.999999", "2025-01-27T14:30", "2025-01-27T21:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-24]@+08:00#XNYS;1h", "bind", "2025-01-23T16:00", "2025-01-23T21:59:59.999999", "2025-01-24T14:30", "2025-01-24T16:59:59.999999", "2025-01-27T14:30", "2025-01-27T16:59:59.999999"},
                {"2025-03-10T12:00:00.000000Z", "[$today, 2025-03-07, 2025-03-10T13:00]@America/New_York#XNYS;1h", "bind", "2025-03-07T14:30", "2025-03-07T21:59:59.999999", "2025-03-10T13:30", "2025-03-10T20:59:59.999999"},
                // the early close
                {jul3, "[$today, 2025-07-02]#XNYS;1h", "bind", "2025-07-02T13:30", "2025-07-02T20:59:59.999999", "2025-07-03T13:30", "2025-07-03T17:59:59.999999"},
                {jul3, "[$today, 2025-07-03T16:30]#XNYS;1h", "bind", "2025-07-03T13:30", "2025-07-03T17:59:59.999999"},
                // $now before and in the session
                {MONDAY_NOON, "[$now, 2025-01-27]#XNYS;1h", "bind", "2025-01-27T14:30", "2025-01-27T21:59:59.999999"},
                {"2025-01-27T15:30:00.000000Z", "[$now, 2025-01-27]#XNYS;1h", "bind", "2025-01-27T14:30", "2025-01-27T21:59:59.999999"},
                // times in brackets and time lists of the fixed date, and the time part of the list
                {MONDAY_NOON, "[$today, 2025-01-24T[15,16]:00]#XNYS;1h", "bind", "2025-01-24T15:00", "2025-01-24T17:00:59.999999", "2025-01-27T14:30", "2025-01-27T21:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-24T[15:00,16:00]]#XNYS;1h", "bind", "2025-01-24T15:00", "2025-01-24T17:00:59.999999", "2025-01-27T14:30", "2025-01-27T21:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-24T[15:00@UTC,16:00]]#XNYS;1h", "bind", "2025-01-24T15:00", "2025-01-24T17:00:59.999999", "2025-01-27T14:30", "2025-01-27T21:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-24, 2025-01-24T16:00]#XNYS;1h", "bind", "2025-01-24T14:30", "2025-01-24T21:59:59.999999", "2025-01-27T14:30", "2025-01-27T21:59:59.999999"},
                {MONDAY_NOON, "[$today - 1d..$today, 2025-01-24]#XNYS;1h", "bind", "2025-01-24T14:30", "2025-01-24T21:59:59.999999", "2025-01-27T14:30", "2025-01-27T21:59:59.999999"},
                {MONDAY_NOON, "[$today-3d..$today, 2025-01-24]T15:00#XNYS;1h", "bind", "2025-01-24T15:00", "2025-01-24T16:00:59.999999", "2025-01-27T15:00", "2025-01-27T16:00:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-24]T15:00#XNYS;1h", "bind", "2025-01-24T15:00", "2025-01-24T16:00:59.999999", "2025-01-27T15:00", "2025-01-27T16:00:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-24]T15:00@America/New_York#XNYS;1h", "bind", "2025-01-24T20:00", "2025-01-24T21:00:59.999999", "2025-01-27T20:00", "2025-01-27T21:00:59.999999"},
                // Fixed dates that hold one another, a minute and a second in it, and a day of today
                // in Hong Kong and its minute at 23:00 or 23:30, unite before the tick calendar cuts
                // them, so the session keeps all of the outer one, as the parser keeps it
                {MONDAY_NOON, "[$today, 2025-01-24T[15:00,15:00:30]]#XNYS;1m", "bind", "2025-01-24T15:00", "2025-01-24T15:01:59.999999", "2025-01-27T14:30", "2025-01-27T21:00:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-27T23:00]@Asia/Hong_Kong#XNYS;1h", "bind", "2025-01-27T14:30", "2025-01-27T16:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-27T23:00]@Asia/Hong_Kong#XNYS;", "bind", "2025-01-27T14:30", "2025-01-27T15:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-27T23:30]@Asia/Hong_Kong#XNYS;1h", "bind", "2025-01-27T14:30", "2025-01-27T16:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-27T23:30]@-02:00#XHKG;1h", "bind", "2025-01-27T02:00", "2025-01-27T08:59:59.999999", "2025-01-28T01:30", "2025-01-28T02:59:59.999999"},
                // the results that do not depend on the extent of the fixed dates
                {MONDAY_NOON, "[$today, 2025-01-24]#XNYS;1d", "bind", "2025-01-24T14:30", "2025-01-25T20:59:59.999999", "2025-01-27T14:30", "2025-01-28T20:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-24]#XNYS;25h", "bind", "2025-01-24T14:30", "2025-01-25T21:59:59.999999", "2025-01-27T14:30", "2025-01-28T21:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-24]#XNYS", "bind", "2025-01-24T14:30", "2025-01-24T20:59:59.999999", "2025-01-27T14:30", "2025-01-27T20:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-24]#Fri;1h", "bind", "2025-01-24T00:00", "2025-01-24T00:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-24];1h", "bind", "2025-01-24T00:00", "2025-01-24T00:59:59.999999", "2025-01-27T00:00", "2025-01-27T00:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-27T15:00]#XNYS;1h", "bind", "2025-01-27T14:30", "2025-01-27T21:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-24T15:00:00+01:00]@Europe/Paris#XNYS;1h", "bind", "2025-01-27T14:30", "2025-01-27T21:59:59.999999"},
                // dates before 1970 and far in the future, which X24H has sessions on
                {MONDAY_NOON, "[$today, 1969-12-31]#X24H;1h", "bind", "1969-12-31T00:00", "1970-01-01T00:59:59.999999", "2025-01-27T00:00", "2025-01-28T00:59:59.999999"},
                {MONDAY_NOON, "[1969-12-31, $today]#X24H;1h", "bind", "1969-12-31T00:00", "1970-01-01T00:59:59.999999", "2025-01-27T00:00", "2025-01-28T00:59:59.999999"},
                {MONDAY_NOON, "[$today, 2099-01-02]#X24H;1h", "bind", "2025-01-27T00:00", "2025-01-28T00:59:59.999999", "2099-01-02T00:00", "2099-01-03T00:59:59.999999"},
                {MONDAY_NOON, "[$today, 1969-12-31T23:30]#X24H;1h", "bind", "1969-12-31T23:30", "1970-01-01T00:30:59.999999", "2025-01-27T00:00", "2025-01-28T00:59:59.999999"},
                {MONDAY_NOON, "[$today, 2099-01-02T23:30]#X24H;1h", "bind", "2025-01-27T00:00", "2025-01-28T00:59:59.999999", "2099-01-02T23:30", "2099-01-03T00:30:59.999999"},
                {MONDAY_NOON, "[$today, 1969-12-31]@America/New_York#X24H;1h", "bind", "1969-12-31T05:00", "1970-01-01T05:59:59.999999", "2025-01-27T05:00", "2025-01-28T05:59:59.999999"},
                {MONDAY_NOON, "[$today, 1969-12-31]#X24H;", "bind", "1969-12-31T00:00", "1969-12-31T23:59:59.999999", "2025-01-27T00:00", "2025-01-27T23:59:59.999999"},
                {MONDAY_NOON, "[$today, 1969-12-31T[23:30@UTC]]#X24H;0h", "bind", "1969-12-31T23:30", "1969-12-31T23:30:59.999999", "2025-01-27T00:00", "2025-01-27T23:59:59.999999"},
                {MONDAY_NOON, "[$today, 2099-01-02T[23:30@UTC]]#X24H;0h", "bind", "2025-01-27T00:00", "2025-01-27T23:59:59.999999", "2099-01-02T23:30", "2099-01-02T23:30:59.999999"},
                {MONDAY_NOON, "[$today, 1969-12-31]#X24H", "bind", "1969-12-31T00:00", "1969-12-31T23:59:59.999999", "2025-01-27T00:00", "2025-01-27T23:59:59.999999"}
        };
        assertCompiledStaticDates(configuration, cases);
        for (int timestampType : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
            final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
            final long now = driver.parseFloorLiteral(MONDAY_NOON);
            // 15:00 at -05:00 is 20:00Z on each trading day of the month, or of the two months, a minute
            // or a point in time that the session holds, an hour longer
            assertCompiledStaticDate(
                    configuration,
                    driver,
                    "[$today, 2025-01T[15:00@-05:00]]#XNYS;1h",
                    now,
                    slotOnSessionDays(driver, "XNYS", "2025-01-01", "2025-02-01", driver.fromHours(20), driver.fromMinutes(1), driver.fromHours(1), "2025-01-27T14:30", "2025-01-27T22:00"),
                    true
            );
            assertCompiledStaticDate(
                    configuration,
                    driver,
                    "[$today, 2025-[01..02]T15:00:00-05]#XNYS;1h",
                    now,
                    slotOnSessionDays(driver, "XNYS", "2025-01-01", "2025-03-01", driver.fromHours(20), 1, driver.fromHours(1), "2025-01-27T14:30", "2025-01-27T22:00"),
                    true
            );
        }
        // a nanosecond, which only nanosecond timestamps add
        final TimestampDriver nanos = ColumnType.getTimestampDriver(ColumnType.TIMESTAMP_NANO);
        assertCompiledStaticDate(
                configuration,
                nanos,
                "[$today, 2025-01-24]#XNYS;1n",
                nanos.parseFloorLiteral(MONDAY_NOON),
                "[{lo=2025-01-24T14:30:00.000000000Z, hi=2025-01-24T21:00:00.000000000Z},{lo=2025-01-27T14:30:00.000000000Z, hi=2025-01-27T21:00:00.000000000Z}]",
                true
        );
    }

    @Test
    public void testCalendarKeepsSessionsOfNestedIntervals() throws SqlException {
        // The calendar keeps the sessions of the union of the list. The minute 15:30 of 2025-01-02 lies
        // inside the hour 15:00 and ends inside the XNYS session, 14:30 to 21:00 UTC, and it adds no
        // instant to the hour, so the whole hour stays.
        assertTickInterval("[{lo=2025-01-02T15:00:00.000000Z, hi=2025-01-02T15:59:59.999999Z}]", "[2025-01-02T15,2025-01-02T15:30]#XNYS");
        // the hour alone
        assertTickInterval("[{lo=2025-01-02T15:00:00.000000Z, hi=2025-01-02T15:59:59.999999Z}]", "2025-01-02T15#XNYS");
        // The same next to a date variable, $today, or the Monday 2025-01-06 that stands for it.
        final long mondayNow = Micros.toMicros(2025, 1, 6, 1, 0);
        final String hourAndMonday = "[{lo=2025-01-02T15:00:00.000000Z, hi=2025-01-02T15:59:59.999999Z},"
                + "{lo=2025-01-06T14:30:00.000000Z, hi=2025-01-06T20:59:59.999999Z}]";
        assertTickIntervalWithNow(hourAndMonday, "[2025-01-06,2025-01-02T15,2025-01-02T15:30]#XNYS", mondayNow);
        assertCompiledTickIntervalWithNow(hourAndMonday, "[$today,2025-01-02T15,2025-01-02T15:30]#XNYS", mondayNow);
        // An empty interval, which a zero duration yields, adds no instant either. parseTickExpr reads
        // the duration of a list with a calendar from its first ';', so only the compiled expression
        // takes this list.
        assertCompiledTickIntervalWithNow(hourAndMonday, "[$today,2025-01-02T15,2025-01-02T15:30;0s]#XNYS", mondayNow);

        // The compiler unions the static intervals of a list once they exceed the cap and the merge
        // threshold (1,024 + 256) together: six copies of the days 1-20 of February to December hold
        // 1,320 days. The union joins the hours 14:00 and 15:00 of 2025-01-02, across which the date
        // variable range from 14:45 to 15:15 lies, and the session from 14:30 keeps both hours.
        final TimestampDriver driver = ColumnType.getTimestampDriver(ColumnType.TIMESTAMP);
        final LongList expected = new LongList();
        expected.add(Micros.toMicros(2025, 1, 2, 14, 30), Micros.toMicros(2025, 1, 2, 16, 0) - 1);
        addSessions(expected, "XNYS", day -> day.getMonthValue() > 1 && day.getDayOfMonth() <= 20);
        final String expectedIntervals = intervalToString(driver, expected);
        final String straddle = "[$now - 30m..$now,2025-01-02T[14..15]," + repeatedValues("2025-[02..12]-[01..20]", 6) + "]#XNYS";
        final long now = Micros.toMicros(2025, 1, 2, 15, 15);
        assertCompiledTickIntervalWithNow(expectedIntervals, straddle, now);
        assertTickIntervalWithNow(expectedIntervals, straddle, now);
    }

    @Test
    public void testCalendarKeepsSessionsOfNestedIntervalsNanos() throws SqlException {
        // The minute 15:30 of the Friday 2025-01-24 lies inside the hour 15:00 and ends inside the XNYS
        // session, 14:30 to 21:00 UTC, so the whole hour stays; $today is the Monday 2025-01-27.
        assertTickIntervalNanos(
                "[{lo=2025-01-24T15:00:00.000000000Z, hi=2025-01-24T15:59:59.999999999Z}]",
                "[2025-01-24T15,2025-01-24T15:30]#XNYS"
        );
        final TimestampDriver driver = ColumnType.getTimestampDriver(ColumnType.TIMESTAMP_NANO);
        final LongList out = new LongList();
        final String expression = "[$today,2025-01-24T15,2025-01-24T15:30]#XNYS";
        try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(driver, configuration, expression, 0, expression.length(), 0)) {
            compiled.evaluate(out, driver.fromMicros(Micros.toMicros(2025, 1, 27, 1, 0)));
        }
        Assert.assertEquals(
                "[{lo=2025-01-24T15:00:00.000000000Z, hi=2025-01-24T15:59:59.999999999Z},"
                        + "{lo=2025-01-27T14:30:00.000000000Z, hi=2025-01-27T20:59:59.999999999Z}]",
                intervalToString(driver, out)
        );
    }

    @Test
    public void testCompiledStaticDateKeepsZoneSwallowedSemicolon() throws SqlException {
        // The timezone of a time slot runs to the end of the slot, so a ';' after it is part of the zone,
        // which reads as -05:00 up to it, see bug-zone-name-prefix-match. Each day of January keeps the
        // window that the list has always made of it: the minute at 20:00Z an hour long, which the
        // session cuts at 21:00Z and the duration extends by an hour.
        for (int timestampType : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
            final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
            final long now = driver.parseFloorLiteral(MONDAY_NOON);
            assertCompiledStaticDate(
                    configuration,
                    driver,
                    "[$today, 2025-01T[15:00@-05:00;1h;1d]]#XNYS;1h",
                    now,
                    slotOnSessionDays(driver, "XNYS", "2025-01-01", "2025-02-01", driver.fromHours(20), driver.fromHours(1), driver.fromHours(1), "2025-01-27T14:30", "2025-01-27T22:00"),
                    false
            );
        }
    }

    @Test
    public void testCompiledStaticDateOverflowParseFuzz() throws SqlException {
        // Random lists whose fixed date alone has more own extents than the interval limit, which the
        // compiled form parses as their bind variable on each evaluation, see OverflowParseFuzzer: the
        // parse merges the intervals of each date into those merged so far, which leaves the list as
        // merging all of them again does, so it fails where the parser fails before the tick calendar,
        // with the same error, and otherwise evaluates to the documented semantics.
        // questdb.fuzz.tickExpressions sets the number of lists.
        final int listCount = Integer.getInteger("questdb.fuzz.tickExpressions", 1_000);
        final Rnd rnd = TestUtils.generateRandom(LOG);
        final OverflowParseFuzzer fuzzer = new OverflowParseFuzzer();
        for (int i = 0; i < listCount; i++) {
            fuzzer.check(rnd);
        }
        fuzzer.log();
        if (listCount >= 1_000) {
            fuzzer.assertEveryShape();
        }
    }

    @Test
    public void testCompiledStaticDateOverflowParseMergesLocally() throws SqlException {
        // A list that parses as its bind variable on each evaluation merges the intervals of each of its
        // dates into those merged so far once the merge range grew by the merge threshold, as the
        // compiled form merges the own extents of a fixed date, see
        // testCompiledStaticDateOwnExtentsMergeLocally, rather than all of them again. At the default
        // threshold of 256 and limit of 1,024, on Monday 2025-01-27:
        // - today holds the 1,440 seconds at the minutes of 2025-01-27, which merge into it at the 255th
        //   second, all 256 intervals, and then every 256 seconds, at the 511th, 767th, 1,023rd and
        //   1,279th: the 256 new ones and today, which they land after, found with one comparison, 258,
        //   four times: 256 + 1,032 = 1,288;
        // - the 161 seconds from 21:19 wait in today for the 840 seconds at the minutes from 00:00 to
        //   13:59 of Friday 2025-01-24, which lie before today and apart. The first merge of the date, at
        //   its 95th second, sorts the 256 new ones, finds with one comparison that they land before
        //   today, and merges today, which holds the 161: 256 + 1 + 1 = 258. The merges at its 351st and
        //   607th seconds sort the 256 new ones, find with 7 and 9 comparisons that they land right
        //   before today, after the second before them, which they read, and move today after them:
        //   256 + 1 + 1 + 7 = 265 and 256 + 1 + 1 + 9 = 267. The merge after the last second does so
        //   with the 233 left among 608 groups: 233 + 1 + 1 + 10 = 245.
        // That makes 1,288 + 258 + 265 + 267 + 245 = 2,323 for each evaluation and each validate(). A
        // merge of every interval so far each time would sort and merge 3,342. The parser of a bind
        // variable merges as it always has, which the count leaves out. The session keeps today, a
        // minute longer.
        final String expression = "[$today, 2025-01-27T[00..23]:[00..59]:00, 2025-01-24T[00..13]:[00..59]:00]#XNYS;1m";
        for (int timestampType : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
            final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
            final String context = expression + " [" + ColumnType.nameOf(timestampType) + ']';
            final long now = driver.parseFloorLiteral(MONDAY_NOON);
            final String expected = expectedIntervals(timestampType, "2025-01-27T14:30", "2025-01-27T21:00:59.999999");
            try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(driver, configuration, expression, 0, expression.length(), 0)) {
                Assert.assertTrue(context, compiled.isParsedOnEvaluation());
                long before = IntervalUtils.getStaticOwnExtentMergeIntervalCountForTesting();
                compiled.validate(now);
                Assert.assertEquals(context + " validate()", 2_323, IntervalUtils.getStaticOwnExtentMergeIntervalCountForTesting() - before);
                final LongList out = new LongList();
                for (int i = 0; i < 2; i++) {
                    out.clear();
                    before = IntervalUtils.getStaticOwnExtentMergeIntervalCountForTesting();
                    compiled.evaluate(out, now);
                    Assert.assertEquals(context, 2_323, IntervalUtils.getStaticOwnExtentMergeIntervalCountForTesting() - before);
                    Assert.assertEquals(context, expected, intervalToString(driver, out));
                }
            }
            final long before = IntervalUtils.getStaticOwnExtentMergeIntervalCountForTesting();
            Assert.assertEquals(context + " as a bind variable", expected, parseTickExpr(driver, expression, now));
            Assert.assertEquals(context + " as a bind variable", before, IntervalUtils.getStaticOwnExtentMergeIntervalCountForTesting());
        }
    }

    @Test
    public void testCompiledStaticDateOverflowParseMergesLocallyAfterEarlierIntervals() throws SqlException {
        // RuntimeIntervalModel evaluates a literal into a list that already holds the intervals of the
        // conditions that it applies first, as for ts IN '<literal>' AND ts >= '2025-01-24', see
        // InTimestampTimestampTest#testStaticDateTickCalendarOverflowAfterOtherCondition. A list that
        // parses as its bind variable on each evaluation merges only from where its own intervals start:
        // it leaves the intervals before them, here from 1 to 2 and from 5 to 6, as they are, appends the
        // session of Monday 2025-01-27, from 14:30 to 21:00Z, a minute longer, and its merges count as
        // many intervals as into an empty list, 2,323, see testCompiledStaticDateOverflowParseMergesLocally.
        final String expression = "[$today, 2025-01-27T[00..23]:[00..59]:00, 2025-01-24T[00..13]:[00..59]:00]#XNYS;1m";
        for (int timestampType : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
            final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
            final String context = expression + " [" + ColumnType.nameOf(timestampType) + ']';
            final long now = driver.parseFloorLiteral(MONDAY_NOON);
            final LongList expected = new LongList();
            expected.add(1L, 2L);
            expected.add(5L, 6L);
            expected.add(driver.parseFloorLiteral("2025-01-27T14:30:00.000000Z"), driver.parseFloorLiteral("2025-01-27T21:01:00.000000Z") - 1);
            try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(driver, configuration, expression, 0, expression.length(), 0)) {
                Assert.assertTrue(context, compiled.isParsedOnEvaluation());
                final LongList out = new LongList();
                for (int i = 0; i < 2; i++) {
                    out.clear();
                    out.add(1L, 2L);
                    out.add(5L, 6L);
                    final long before = IntervalUtils.getStaticOwnExtentMergeIntervalCountForTesting();
                    compiled.evaluate(out, now);
                    Assert.assertEquals(context, 2_323, IntervalUtils.getStaticOwnExtentMergeIntervalCountForTesting() - before);
                    Assert.assertEquals(context, intervalToString(driver, expected), intervalToString(driver, out));
                }
            }
        }
    }

    @Test
    public void testCompiledStaticDateOverflowParsesAsBindVariable() throws SqlException {
        // A fixed date whose own extents alone exceed the interval limit makes the compiled form parse
        // the list as its bind variable on each evaluation, where earlier elements may hold them, and
        // fail with the error of the bind variable, at its position, where they do not. At a merge
        // threshold of 8 and a limit of 16, on Monday 2025-01-27: the 24 seconds at the hours of the day
        // lie in today; at 03:30Z, the seconds every ten minutes from 01:00 to 03:00 of the range from
        // today to now that lie after now, at 03:40 and 03:50, end the session of XHKG, which ends at
        // 04:00Z, ten minutes later. The text 00:00:00UTC reads as a point in time only with a duration.
        final CairoConfiguration smallLimits = calendarConfiguration(8, 16);
        final String bracketError = "error: Bracket expansion produces too many intervals (max 16)";
        for (int timestampType : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
            final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
            final long now = driver.parseFloorLiteral(MONDAY_NOON);
            final String hours = "[$today, 2025-01-27T[00..23]:00:00]#XNYS;1h";
            final String session = expectedIntervals(timestampType, "2025-01-27T14:30", "2025-01-27T21:59:59.999999");
            assertCompiledStaticDateParsesFirst(smallLimits, driver, hours, now, session);
            Assert.assertEquals(hours, session, parseTickExpr(smallLimits, driver, hours, now, IntervalUtils.TICK_CHECK_ALL));
            // the 21 seconds to 20:00:00 merge into today as the last of them expands, after each 7th,
            // which leaves the parse a single interval for the tick calendar
            final String toEight = "[$today, 2025-01-27T[00..20]:00:00]#XNYS;1h";
            assertCompiledStaticDateParsesFirst(smallLimits, driver, toEight, now, session);
            Assert.assertEquals(toEight, session, parseTickExpr(smallLimits, driver, toEight, now, IntervalUtils.TICK_CHECK_ALL));

            final String xhkg = "[$today..$now, 2025-01-27T[01..03]:[00,10,20,30,40,50]:00]#XHKG;10m";
            final long earlyMorning = driver.parseFloorLiteral("2025-01-27T03:30:00.000000Z");
            final String xhkgExpected = expectedIntervals(timestampType, "2025-01-27T01:30", "2025-01-27T04:00:00.999999");
            assertCompiledStaticDateParsesFirst(smallLimits, driver, xhkg, earlyMorning, xhkgExpected);
            Assert.assertEquals(xhkg, xhkgExpected, parseTickExpr(smallLimits, driver, xhkg, earlyMorning, IntervalUtils.TICK_CHECK_ALL));

            // the fixed date first: nothing holds its seconds
            final String dateFirst = "[2025-01-27T[00..23]:00:00, $today]#XNYS;1h";
            assertCompiledStaticDateParsesFirst(smallLimits, driver, dateFirst, now, bracketError);
            Assert.assertEquals(dateFirst, bracketError, parseTickExpr(smallLimits, driver, dateFirst, now, IntervalUtils.TICK_CHECK_ALL));

            // the bind variable fails on the time, which the literal reads with the duration
            final String utc = "[$today, 2025-01-27T[00..23]:00:00UTC]#XNYS;1h";
            assertCompiledStaticDateParsesFirst(smallLimits, driver, utc, now, session);
            Assert.assertEquals(utc, "error: Invalid date: 2025-01-27T00:00:00UTC", parseTickExpr(smallLimits, driver, utc, now, IntervalUtils.TICK_CHECK_ALL));

            // at the default limits, the 1,440 seconds at the minutes of the day lie in today on
            // Monday, and nothing holds them on Tuesday
            final String minutes = "[$today, 2025-01-27T[00..23]:[00..59]:00]#XNYS;1m";
            final String minutesExpected = expectedIntervals(timestampType, "2025-01-27T14:30", "2025-01-27T21:00:59.999999");
            assertCompiledStaticDateParsesFirst(configuration, driver, minutes, now, minutesExpected);
            Assert.assertEquals(minutes, minutesExpected, parseTickExpr(driver, minutes, now));
            final long tuesday = driver.parseFloorLiteral("2025-01-28T12:00:00.000000Z");
            assertCompiledStaticDateParsesFirst(configuration, driver, minutes, tuesday, "error: Bracket expansion produces too many intervals (max 1024)");
            Assert.assertEquals(minutes, "error: Bracket expansion produces too many intervals (max 1024)", parseTickExpr(driver, minutes, tuesday));
        }
    }

    @Test
    public void testCompiledStaticDateOverflowRangeOrderErrorIsBindVariableError() throws SqlException {
        // The list parses as its bind variable on each evaluation, so a range that starts after it ends
        // fails with the error of the bind variable at the position of the bind variable: the literal at
        // position 100, and the '..' 6 characters into the first element. At 14:00Z on Monday 2025-01-27
        // the range from today to 13 hours ago ends at 01:00Z, and validates, today holding the 1,440
        // seconds of the fixed date; at 12:00Z it ends on the Sunday before.
        final String expression = "[$today..$now-13h, $today, 2025-01-27T[00..23]:[00..59]:00]#XNYS;1m";
        for (int timestampType : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
            final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
            final String context = expression + " [" + ColumnType.nameOf(timestampType) + ']';
            final long noon = driver.parseFloorLiteral(MONDAY_NOON);
            try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(driver, configuration, expression, 0, expression.length(), 100)) {
                Assert.assertTrue(context, compiled.isParsedOnEvaluation());
                compiled.validate(driver.parseFloorLiteral("2025-01-27T14:00:00.000000Z"));
                try {
                    compiled.evaluate(new LongList(), noon);
                    Assert.fail(context);
                } catch (CairoException e) {
                    Assert.assertEquals(context, "Invalid date range: start is after end", e.getFlyweightMessage().toString());
                    Assert.assertEquals(context, 106, e.getPosition());
                }
            }
            try {
                IntervalUtils.parseTickExpr(driver, configuration, expression, 0, expression.length(), 100, new LongList(), sink, noon, IntervalUtils.TICK_CHECK_ALL);
                Assert.fail(context);
            } catch (SqlException e) {
                Assert.assertEquals(context, "Invalid date range: start is after end", e.getFlyweightMessage().toString());
                Assert.assertEquals(context, 106, e.getPosition());
            }
        }
    }

    @Test
    public void testCompiledStaticDateOverflowUnitesBeforeTickCalendar() throws SqlException {
        // A list that parses as its bind variable on each evaluation unites its intervals before the
        // tick calendar cuts them. At a merge threshold of 8 and a limit of 16, the 24 seconds at the
        // hours of 2025-01-27 exceed the limit alone, so the compiled form parses the list. Today,
        // Monday 2025-01-27, holds the seconds, which merge into it, and the session of XNYS, from
        // 14:30 to 21:00Z, keeps today from 14:30, which the duration ends an hour later.
        // With a timezone, each element merges its intervals in a range of its own, so the 24 seconds
        // count against the limit before today holds them, and the list fails as its bind variable
        // does.
        final CairoConfiguration smallLimits = calendarConfiguration(8, 16);
        final String overLimit = "error: Bracket expansion produces too many intervals (max 16)";
        final String zoned = "[$today, 2025-01-27T[00..23]:00:00]@+08:00#XNYS;1h";
        for (int timestampType : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
            final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
            final long now = driver.parseFloorLiteral(MONDAY_NOON);
            assertCompiledStaticDateParsesFirst(
                    smallLimits,
                    driver,
                    "[$today, 2025-01-27T[00..23]:00:00]#XNYS;1h",
                    now,
                    expectedIntervals(timestampType, "2025-01-27T14:30", "2025-01-27T21:59:59.999999")
            );
            assertCompiledStaticDateParsesFirst(smallLimits, driver, zoned, now, overLimit);
            Assert.assertEquals(zoned, overLimit, parseTickExpr(smallLimits, driver, zoned, now, IntervalUtils.TICK_CHECK_ALL));
        }
    }

    @Test
    public void testCompiledStaticDateOwnExtentsAtTheLimit() throws SqlException {
        // The compiled form counts the own extents of a fixed date alone, as it always has, so at a
        // limit of 16 a fixed date with 16 seconds keeps its IR, and one with 17 parses as its bind
        // variable, which fails. The session of Friday 2025-01-24 holds the second at 15:00:00, an hour
        // longer. The parser counts the seconds with today before them, so the list fails as a bind
        // variable, and as a bind variable with the fixed date first it returns the same intervals.
        final CairoConfiguration smallLimits = calendarConfiguration(8, 16);
        for (int timestampType : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
            final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
            final long now = driver.parseFloorLiteral(MONDAY_NOON);
            final String sixteen = "[$today, 2025-01-24T[00..15]:00:00]#XNYS;1h";
            final String expected = expectedIntervals(timestampType, "2025-01-24T15:00", "2025-01-24T16:00:00.999999", "2025-01-27T14:30", "2025-01-27T21:59:59.999999");
            assertCompiledStaticDate(smallLimits, driver, sixteen, now, expected, false);
            Assert.assertEquals(sixteen, expected, parseTickExpr(smallLimits, driver, "[2025-01-24T[00..15]:00:00, $today]#XNYS;1h", now, IntervalUtils.TICK_CHECK_ALL));
            Assert.assertEquals(sixteen, "error: Bracket expansion produces too many intervals (max 16)", parseTickExpr(smallLimits, driver, sixteen, now, IntervalUtils.TICK_CHECK_ALL));

            assertCompiledStaticDateFails(smallLimits, driver, "[$today, 2025-01-24T[00..16]:00:00]#XNYS;1h", now, true, "error: Bracket expansion produces too many intervals (max 16)");

            // the same counts without a duration, where the own extents always counted
            assertCompiledStaticDate(
                    smallLimits,
                    driver,
                    "[$today, 2025-01-24T[00..15]:00:00]#XNYS",
                    now,
                    expectedIntervals(timestampType, "2025-01-24T15:00", "2025-01-24T15:00:00.999999", "2025-01-27T14:30", "2025-01-27T20:59:59.999999"),
                    false
            );
            assertCompileError(smallLimits, driver, "[$today, 2025-01-24T[00..16]:00:00]#XNYS", "Bracket expansion produces too many intervals (max 16)");
        }
    }

    @Test
    public void testCompiledStaticDateOwnExtentsMergeAcrossRepeatedHour() throws SqlException {
        // The 360 seconds every ten seconds of the hour from 01:00 that New York repeats on 2025-11-02
        // turn into 720 instants, each at 05:MM:SS and at 06:MM:SS UTC, past the merge threshold of 256,
        // so each second after the 256th lands two seconds amid those merged so far. X24H trades all
        // day: each second, a second longer, and the day of 2025-11-04 in New York, from 05:00Z, a
        // second longer.
        for (int timestampType : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
            final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
            final long now = driver.parseFloorLiteral("2025-11-04T12:00:00.000000Z");
            final LongList expected = new LongList();
            for (int hour = 5; hour <= 6; hour++) {
                for (int minute = 0; minute < 60; minute++) {
                    for (int second = 0; second < 60; second += 10) {
                        final long lo = driver.parseFloorLiteral("2025-11-02T00:00:00.000000Z") + driver.fromHours(hour) + driver.fromMinutes(minute) + driver.fromSeconds(second);
                        expected.add(lo, lo + driver.fromSeconds(2) - 1);
                    }
                }
            }
            expected.add(driver.parseFloorLiteral("2025-11-04T05:00:00.000000Z"), driver.parseFloorLiteral("2025-11-05T05:00:01.000000Z") - 1);
            assertCompiledStaticDate(
                    configuration,
                    driver,
                    "[$today, 2025-11-02T01:[00..59]:[00,10,20,30,40,50]]@America/New_York#X24H;1s",
                    now,
                    intervalToString(driver, expected),
                    true
            );
        }
    }

    @Test
    public void testCompiledStaticDateOwnExtentsMergeLocally() throws SqlException {
        // The own extents of a fixed date that stay apart merge into the ones merged so far once as
        // many again as the merge threshold have come, rather than all of them again. The 1,020 seconds
        // at the minutes from 00:00 to 16:59 of 2025-01-24 stay apart at the default threshold of 256:
        // the first merge sorts and merges 256, the merges at the 512th and 768th seconds sort the 256
        // new ones, which land last, as one comparison finds, and merge them with the last merged
        // second, 258 each, and the merge after the last second does so with the 252 left, 254: 1,026
        // in all. A merge of all seconds so far each time would sort and merge 2,556. The session of
        // Friday 2025-01-24 holds the seconds from 14:30 on, which a minute longer join. Neither the IR
        // without a duration nor the parser, which returns the same intervals, merges this way.
        final String ascending = "[$today, 2025-01-24T[00..16]:[00..59]:00]#XNYS;1m";
        final String descending = "[$today, 2025-01-24T[16,15,14,13,12,11,10,09,08,07,06,05,04,03,02,01,00]:[00..59]:00]#XNYS;1m";
        final CairoConfiguration smallLimits = calendarConfiguration(8, 16);
        for (int timestampType : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
            final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
            final long now = driver.parseFloorLiteral(MONDAY_NOON);
            final String expected = expectedIntervals(timestampType, "2025-01-24T14:30", "2025-01-24T17:00:00.999999", "2025-01-27T14:30", "2025-01-27T21:00:59.999999");
            final long before = IntervalUtils.getStaticOwnExtentMergeIntervalCountForTesting();
            try (CompiledTickExpression ignore = IntervalUtils.compileTickExpr(driver, configuration, ascending, 0, ascending.length(), 0)) {
                Assert.assertEquals(ascending, 1_026, IntervalUtils.getStaticOwnExtentMergeIntervalCountForTesting() - before);
            }
            assertCompiledStaticDate(configuration, driver, ascending, now, expected, false);
            assertCompiledStaticDate(configuration, driver, descending, now, expected, false);

            final long afterCompiles = IntervalUtils.getStaticOwnExtentMergeIntervalCountForTesting();
            Assert.assertEquals(ascending, expected, parseTickExpr(driver, ascending, now));
            final String withoutDuration = "[$today, 2025-01-24T[00..16]:[00..59]:00]#XNYS";
            try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(driver, configuration, withoutDuration, 0, withoutDuration.length(), 0)) {
                Assert.assertFalse(withoutDuration, compiled.isParsedOnEvaluation());
            }
            // At a limit of 16, the compiled form expands the months of a year to their days, with a
            // time part of the list and no duration, as it always has, and their 365 minutes at 15:00
            // exceed the limit as the list compiles, before the tick calendar keeps the 250 that XNYS
            // trades on in 2025, which exceed it too
            assertCompileError(smallLimits, driver, "[$today, 2025-[01..12]]T15:00#XNYS", "Bracket expansion produces too many intervals (max 16)");
            Assert.assertEquals(afterCompiles, IntervalUtils.getStaticOwnExtentMergeIntervalCountForTesting());

            // At a limit of 16, the seconds from 15:00:00 and 16:00:00Z to 6 seconds after each merge as
            // the second time slot expands them, which joins, and each lasts a second longer
            final String slots = "[$today, 2025-01-24T[15:00@UTC,16:00@UTC]:[00..05]]#XNYS;1s";
            assertCompiledStaticDate(
                    smallLimits,
                    driver,
                    slots,
                    now,
                    expectedIntervals(
                            timestampType,
                            "2025-01-24T15:00", "2025-01-24T15:00:06.999999",
                            "2025-01-24T16:00", "2025-01-24T16:00:06.999999",
                            "2025-01-27T14:30", "2025-01-27T21:00:00.999999"
                    ),
                    true
            );
            Assert.assertTrue(slots, IntervalUtils.getStaticOwnExtentMergeIntervalCountForTesting() > afterCompiles);
        }
    }

    @Test
    public void testCompiledStaticDateOwnSemicolonKeepsHeadReading() throws SqlException {
        // A compiled list with a tick calendar and no duration of its own reads the ';' of a fixed date
        // as the duration or the repetition of that date, as it always has, and extends nothing after
        // the tick calendar: the interval of the date, cut by the sessions. Where the parser reads the
        // text, that is what the parser makes of the list without its tick calendar, cut by the
        // sessions; it does not read a ';' in a bracket of a fixed date, nor in a month under the time of
        // the list, see bug-fixed-date-own-semicolon-parser-rejects. With a duration of the list too,
        // the fixed date has two ';' and fails to compile, as it always has.
        final CairoConfiguration smallLimits = calendarConfiguration(8, 16);
        final String[][] cases = {
                // now, expression, "S9" where the parser reads it, "S2" otherwise, then the intervals
                {MONDAY_NOON, "[$today, 2025-01-24T15:00;1h]#XNYS", "S9", "2025-01-24T15:00", "2025-01-24T15:59:59.999999", "2025-01-27T14:30", "2025-01-27T20:59:59.999999"},
                {MONDAY_NOON, "[2025-01-24T15:00;1h, $today]#XNYS", "S9", "2025-01-24T15:00", "2025-01-24T15:59:59.999999", "2025-01-27T14:30", "2025-01-27T20:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-24T15:00;1h]@America/New_York#XNYS", "S9", "2025-01-24T20:00", "2025-01-24T20:59:59.999999", "2025-01-27T14:30", "2025-01-27T20:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-24T15:00;1h]@-05:00#XNYS", "S9", "2025-01-24T20:00", "2025-01-24T20:59:59.999999", "2025-01-27T14:30", "2025-01-27T20:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-[23..24]T15:00;1h]#XNYS", "S9", "2025-01-23T15:00", "2025-01-23T15:59:59.999999", "2025-01-24T15:00", "2025-01-24T15:59:59.999999", "2025-01-27T14:30", "2025-01-27T20:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-24T15:00;1h, 2025-01-23T15:00;1h]#XNYS", "S9", "2025-01-23T15:00", "2025-01-23T15:59:59.999999", "2025-01-24T15:00", "2025-01-24T15:59:59.999999", "2025-01-27T14:30", "2025-01-27T20:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-24T14:00;2h]#XNYS", "S9", "2025-01-24T14:30", "2025-01-24T15:59:59.999999", "2025-01-27T14:30", "2025-01-27T20:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-24T15:00;-1h]#XNYS", "S9", "2025-01-24T14:30", "2025-01-24T14:59:59.999999", "2025-01-27T14:30", "2025-01-27T20:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-24;1h]#XNYS", "S9", "2025-01-27T14:30", "2025-01-27T20:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-24;1d]#XNYS", "S9", "2025-01-24T14:30", "2025-01-24T20:59:59.999999", "2025-01-27T14:30", "2025-01-27T20:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-24T02:00;1h]#XHKG", "S9", "2025-01-24T02:00", "2025-01-24T02:59:59.999999", "2025-01-27T01:30", "2025-01-27T03:59:59.999999", "2025-01-27T05:00", "2025-01-27T07:59:59.999999"},
                {MONDAY_NOON, "[$today..$today+1d, 2025-01-24T15:00;1h]#XNYS", "S9", "2025-01-24T15:00", "2025-01-24T15:59:59.999999", "2025-01-27T14:30", "2025-01-27T20:59:59.999999", "2025-01-28T14:30", "2025-01-28T20:59:59.999999"},
                // a repeating interval: 30 minutes from 15:00 on 2025-01-24 and on the Sunday after
                {MONDAY_NOON, "[$today, 2025-01-24T15:00;30m;2d;2]#XNYS", "S9", "2025-01-24T15:00", "2025-01-24T15:29:59.999999", "2025-01-27T14:30", "2025-01-27T20:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-24;30m;2d;2]#XNYS", "S9", "2025-01-27T14:30", "2025-01-27T20:59:59.999999"},
                // the windows of a minute or an hour of the fixed date cover its day
                {MONDAY_NOON, "[$today, 2025-01-27T[00..23]:[00..59]:00;1m]#XNYS", "S9", "2025-01-27T14:30", "2025-01-27T20:59:59.999999"},
                // a ';' in a time slot, after the time or in its zone
                {MONDAY_NOON, "[$today, 2025-01-24T[15:00;1h]]#XNYS", "S2", "2025-01-24T15:00", "2025-01-24T15:59:59.999999", "2025-01-27T14:30", "2025-01-27T20:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-24T[15:00,16:00;1h]]#XNYS", "S2", "2025-01-24T15:00", "2025-01-24T15:00:59.999999", "2025-01-24T16:00", "2025-01-24T16:59:59.999999", "2025-01-27T14:30", "2025-01-27T20:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-24T[15:00@-05:00;1h]]#XNYS", "S2", "2025-01-24T20:00", "2025-01-24T20:00:59.999999", "2025-01-27T14:30", "2025-01-27T20:59:59.999999"},
                // the slot converts with its own timezone and again with that of the list, which drops
                // it, see questdb-enterprise#1362
                {MONDAY_NOON, "[$today, 2025-01-24T[09:30@America/New_York;1h]]@Europe/Paris#XNYS", "S2", "2025-01-27T14:30", "2025-01-27T20:59:59.999999"},
                // a month with a duration of its own under the time of the list: the first hour of
                // the month lies on the holiday 2025-01-01
                {MONDAY_NOON, "[$today, 2025-01;1h]T15:00@America/New_York#XNYS", "S2", "2025-01-27T20:00", "2025-01-27T20:00:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01;1h]T15:00#XNYS", "S2", "2025-01-27T15:00", "2025-01-27T15:00:59.999999"}
        };
        for (int timestampType : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
            final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
            for (String[] c : cases) {
                final long now = driver.parseFloorLiteral(c[0]);
                final String expected = expectedIntervals(timestampType, c, 3);
                assertCompiledStaticDate(configuration, driver, c[1], now, expected, false);
                if (c[2].equals("S9")) {
                    final String calendar = c[1].contains("#XHKG") ? "XHKG" : "XNYS";
                    Assert.assertEquals(c[1] + " without its calendar", expected, parsedWithoutCalendarCutBySessions(driver, c[1], calendar, now));
                }
            }
            final long now = driver.parseFloorLiteral(MONDAY_NOON);
            // 15:00 in New York on each day of the first 24 days of January, which the sessions hold
            // on the trading days, and on today
            assertCompiledStaticDate(
                    configuration,
                    driver,
                    "[$today, 2025-01;24d]T15:00@America/New_York#XNYS",
                    now,
                    slotOnSessionDays(driver, "XNYS", "2025-01-01", "2025-01-25", driver.fromHours(20), driver.fromMinutes(1), 0, "2025-01-27T20:00", "2025-01-27T20:01"),
                    false
            );
            // the hourly windows of the fixed dates cover their days
            for (String[] c : new String[][]{
                    {"[$today, 2025-01-27T[00..23]:00:00;1h]#XNYS", "2025-01-27T14:30", "2025-01-27T20:59:59.999999"},
                    {"[$today, 2025-01-24T[00..23]:00:00;1h]#XNYS", "2025-01-24T14:30", "2025-01-24T20:59:59.999999", "2025-01-27T14:30", "2025-01-27T20:59:59.999999"}
            }) {
                assertCompiledStaticDate(smallLimits, driver, c[0], now, expectedIntervals(timestampType, c, 1), false);
            }
            // the errors
            assertCompileError(configuration, driver, "[$today, 2025-01-24;1h]T15:00#XNYS", "Expected number before unit 'T'");
            assertCompileError(configuration, driver, "[$today, 2025-01-24;1h]T15:00#Fri", "Expected number before unit 'T'");
            assertCompileError(configuration, driver, "[$today, 2025-01-24;1h]T15:00@America/New_York#XNYS", "Expected number before unit 'T'");
            assertCompileError(configuration, driver, "[$today, 2025-01-24;1h]T15:00@America/New_York#Fri", "Expected number before unit 'T'");
            assertCompileError(smallLimits, driver, "[$today, 2025-01-[01..31]T15:00;1h]#XNYS", "Bracket expansion produces too many intervals (max 16)");
            assertCompileError(configuration, driver, "[$today, 2025-01-24T15:00;1h]#XNYS;1h", "Invalid interval format: 2025-01-24T15:00;1h;1h");
            assertCompileError(configuration, driver, "[$today, 2025-01-24T15:00;1h]#XNYS;", "Invalid interval format: 2025-01-24T15:00;1h;");
            // a fixed date with a time of its own under the time of the list has no IR, and the parser
            // takes the list to its first ';'
            final String listTime = "[$today, 2025-01-24T15:00;1h]T16:00#XNYS";
            assertCompiledParsesOnEvaluation(driver, listTime, now, "error: Unclosed '[' in date list");
            try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(driver, configuration, listTime, 0, listTime.length(), 0)) {
                compiled.validate(now);
                Assert.fail(listTime);
            } catch (SqlException e) {
                Assert.assertEquals(listTime, "Unclosed '[' in date list", e.getFlyweightMessage().toString());
            }
        }
    }

    @Test
    public void testCompiledStaticDateRunOneDropsEveryWindow() throws SqlException {
        // A fixed date keeps its own extent where the window that the compiled form used to make of it
        // was empty, so that nothing of it remained: a zero or an empty duration made each time slot an
        // empty window, which a slot with a named timezone of its own dropped, and so did a negative
        // duration whose window lay in the hour that New York skips on 2025-03-09 from 02:00. An offset
        // timezone kept the empty window instead. A slot in the skipped hour has no instant either way.
        // A list without a fixed date keeps its intervals as they were, the ones of the range from an
        // hour before now to an hour after included, which hold now. The calendar unites them before it
        // cuts them, so the session keeps the range to an hour after now.
        final String mar10 = "2025-03-10T12:00:00.000000Z";
        final String mon1500 = "2025-01-27T15:00:00.000000Z";
        final String[][] cases = {
                // now, expression, "bind" where the parser reads the text alike and nothing nests, then the intervals
                {MONDAY_NOON, "[$today, 2025-01-24T[15:00@UTC]]#XNYS;0h", "bind", "2025-01-24T15:00", "2025-01-24T15:00:59.999999", "2025-01-27T14:30", "2025-01-27T20:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-24T[15:00@UTC]]#XNYS;", "bind", "2025-01-24T15:00", "2025-01-24T15:00:59.999999", "2025-01-27T14:30", "2025-01-27T20:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-24T[15:00@UTC]]#XNYS;0m", "bind", "2025-01-24T15:00", "2025-01-24T15:00:59.999999", "2025-01-27T14:30", "2025-01-27T20:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-24T[15:00@UTC]]#XNYS;0s", "bind", "2025-01-24T15:00", "2025-01-24T15:00:59.999999", "2025-01-27T14:30", "2025-01-27T20:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-24T[15:00@UTC]]#XNYS;1h-1h", "bind", "2025-01-24T15:00", "2025-01-24T15:00:59.999999", "2025-01-27T14:30", "2025-01-27T20:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-24T[15:00@GMT]]#XNYS;0h", "bind", "2025-01-24T15:00", "2025-01-24T15:00:59.999999", "2025-01-27T14:30", "2025-01-27T20:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-24T[15:00@UTC]]@America/New_York#XNYS;0h", "bind", "2025-01-24T15:00", "2025-01-24T15:00:59.999999", "2025-01-27T14:30", "2025-01-27T20:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-24T[15:00@UTC]]@-05:00#XNYS;0h", "bind", "2025-01-24T15:00", "2025-01-24T15:00:59.999999", "2025-01-27T14:30", "2025-01-27T20:59:59.999999"},
                {MONDAY_NOON, "[2025-01-24T[15:00@UTC], $today]#XNYS;0h", "bind", "2025-01-24T15:00", "2025-01-24T15:00:59.999999", "2025-01-27T14:30", "2025-01-27T20:59:59.999999"},
                {MONDAY_NOON, "[ $today , 2025-01-24T[15:00@UTC] ]#XNYS;0h", "bind", "2025-01-24T15:00", "2025-01-24T15:00:59.999999", "2025-01-27T14:30", "2025-01-27T20:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-24T[15:00@UTC], 2025-01-23T[15:00@UTC]]#XNYS;0h", "bind", "2025-01-23T15:00", "2025-01-23T15:00:59.999999", "2025-01-24T15:00", "2025-01-24T15:00:59.999999", "2025-01-27T14:30", "2025-01-27T20:59:59.999999"},
                {MONDAY_NOON, "[2025-01-24T[15:00@UTC], $today, 2025-01-23T[15:00@UTC]]#XNYS;0h", "bind", "2025-01-23T15:00", "2025-01-23T15:00:59.999999", "2025-01-24T15:00", "2025-01-24T15:00:59.999999", "2025-01-27T14:30", "2025-01-27T20:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-[23..24]T[15:00@UTC]]#XNYS;0h", "bind", "2025-01-23T15:00", "2025-01-23T15:00:59.999999", "2025-01-24T15:00", "2025-01-24T15:00:59.999999", "2025-01-27T14:30", "2025-01-27T20:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-[23,24]T[15:00@UTC]]#XNYS;0h", "bind", "2025-01-23T15:00", "2025-01-23T15:00:59.999999", "2025-01-24T15:00", "2025-01-24T15:00:59.999999", "2025-01-27T14:30", "2025-01-27T20:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-24T[15:00@UTC,16:00@UTC]]#XNYS;0h", "bind", "2025-01-24T15:00", "2025-01-24T15:00:59.999999", "2025-01-24T16:00", "2025-01-24T16:00:59.999999", "2025-01-27T14:30", "2025-01-27T20:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-24T[09:30@America/New_York]]#XNYS;0h", "bind", "2025-01-24T14:30", "2025-01-24T14:30:59.999999", "2025-01-27T14:30", "2025-01-27T20:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-24T[09:30@America/New_York]]#XNYS;", "bind", "2025-01-24T14:30", "2025-01-24T14:30:59.999999", "2025-01-27T14:30", "2025-01-27T20:59:59.999999"},
                // offset timezones kept the empty window, which held nothing
                {MONDAY_NOON, "[$today, 2025-01-24T[15:00@Z]]#XNYS;0h", "bind", "2025-01-24T15:00", "2025-01-24T15:00:59.999999", "2025-01-27T14:30", "2025-01-27T20:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-24T[15:00@+00:00]]#XNYS;0h", "bind", "2025-01-24T15:00", "2025-01-24T15:00:59.999999", "2025-01-27T14:30", "2025-01-27T20:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-24T[10:00@-05:00]]#XNYS;0h", "bind", "2025-01-24T15:00", "2025-01-24T15:00:59.999999", "2025-01-27T14:30", "2025-01-27T20:59:59.999999"},
                {MONDAY_NOON, "[$today, 2025-01-24T[15:00@UTC+00:00]]#XNYS;0h", "bind", "2025-01-24T15:00", "2025-01-24T15:00:59.999999", "2025-01-27T14:30", "2025-01-27T20:59:59.999999"},
                // 03:00 in New York on 2025-03-09 is 07:00Z, right after the skipped hour, 30 seconds
                // shorter; 03:30 is 07:30Z
                {mar10, "[$today, 2025-03-09T[03:00@America/New_York]]#X24H;-30s", "bind", "2025-03-09T07:00", "2025-03-09T07:00:29.999999", "2025-03-10T00:00", "2025-03-10T23:59:29.999999"},
                {mar10, "[$today, 2025-03-09T[03:30@America/New_York]]#X24H;-30s", "bind", "2025-03-09T07:30", "2025-03-09T07:30:29.999999", "2025-03-10T00:00", "2025-03-10T23:59:29.999999"},
                // 02:30 in New York on 2025-03-09 does not exist
                {mar10, "[$today, 2025-03-09T[02:30@America/New_York]]#X24H;1m", "bind", "2025-03-10T00:00", "2025-03-11T00:00:59.999999"},
                {mar10, "[$today, 2025-03-09T[02:30@America/New_York]]#X24H;0h", "bind", "2025-03-10T00:00", "2025-03-10T23:59:59.999999"},
                // lists without a fixed date, and a date variable without a list
                {MONDAY_NOON, "[$today, $yesterday]#XNYS;0h", "bind", "2025-01-27T14:30", "2025-01-27T20:59:59.999999"},
                {mon1500, "[$now-1h..$now+1h, $now]#XNYS;0h", "bind", "2025-01-27T14:30", "2025-01-27T16:00"},
                {MONDAY_NOON, "$todayT15:00#XNYS;1h", "bind", "2025-01-27T15:00", "2025-01-27T16:00:59.999999"},
                // the fixed date makes the list unite its intervals before the calendar cuts them
                {mon1500, "[$now-1h..$now+1h, $now, 2025-01-24T[15:00@UTC]]#XNYS;0h", "S2", "2025-01-24T15:00", "2025-01-24T15:00:59.999999", "2025-01-27T14:30", "2025-01-27T16:00"},
                {mon1500, "[$now-1h..$now+1h, $now, 2025-01-24T[15:00@UTC]]#XNYS;1h", "S2", "2025-01-24T15:00", "2025-01-24T16:00:59.999999", "2025-01-27T14:30", "2025-01-27T17:00"}
        };
        assertCompiledStaticDates(configuration, cases);
    }

    @Test
    public void testCompiledStaticDateSteadyStateAllocatesNoJavaHeap() throws SqlException {
        // Filters evaluate a compiled tick expression per execution, and per worker. Once warm, a list
        // with fixed dates, a tick calendar and a duration evaluates without allocating, with an IR
        // whose fixed dates keep their own extent, and parsed as its bind variable where a fixed date
        // alone has more own extents than the limit.
        final String[][] cases = {
                // expression, intervals, evaluations to warm up with, evaluations in each of 5 rounds;
                // the parse of the 1,440 seconds of the second takes far longer than the IR of the first
                {"[$today, 2025-01-24, 2025-01-27T15:00]#XNYS;1h", "[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T21:59:59.999999Z},{lo=2025-01-27T14:30:00.000000Z, hi=2025-01-27T21:59:59.999999Z}]", "20000", "10000"},
                {"[$today, 2025-01-27T[00..23]:[00..59]:00]#XNYS;1m", "[{lo=2025-01-27T14:30:00.000000Z, hi=2025-01-27T21:00:59.999999Z}]", "2000", "1000"}
        };
        try (TestUtils.ThreadMetricsScope<com.sun.management.ThreadMXBean> scope = TestUtils.threadAllocationScope()) {
            final com.sun.management.ThreadMXBean threadMXBean = scope.getBean();
            for (int timestampType : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
                final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
                final long now = driver.parseFloorLiteral(MONDAY_NOON);
                for (String[] c : cases) {
                    final String context = c[0] + " [" + ColumnType.nameOf(timestampType) + ']';
                    final int warmUpCount = Integer.parseInt(c[2]);
                    final int roundCount = Integer.parseInt(c[3]);
                    try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(driver, configuration, c[0], 0, c[0].length(), 0)) {
                        final LongList out = new LongList();
                        for (int i = 0; i < warmUpCount; i++) {
                            out.clear();
                            compiled.evaluate(out, now);
                        }
                        final String expected = forTimestampType(c[1], timestampType);
                        Assert.assertEquals(context, expected, intervalToString(driver, out));
                        final long first = out.getQuick(0);
                        final long last = out.getQuick(out.size() - 1);
                        final int size = out.size();
                        long minAllocatedBytes = Long.MAX_VALUE;
                        int expectedEvaluationCount = 0;
                        for (int round = 0; round < 5; round++) {
                            final long allocatedBefore = threadMXBean.getCurrentThreadAllocatedBytes();
                            for (int i = 0; i < roundCount; i++) {
                                out.clear();
                                compiled.evaluate(out, now);
                                if (out.size() == size && out.getQuick(0) == first && out.getQuick(size - 1) == last) {
                                    expectedEvaluationCount++;
                                }
                            }
                            minAllocatedBytes = Math.min(minAllocatedBytes, threadMXBean.getCurrentThreadAllocatedBytes() - allocatedBefore);
                        }
                        Assert.assertEquals(context, expected, intervalToString(driver, out));
                        Assert.assertEquals(context, 5 * roundCount, expectedEvaluationCount);
                        Assert.assertEquals(context, 0, minAllocatedBytes);
                    }
                }
            }
        }
    }

    @Test
    public void testCompiledStaticDateWithTickCalendarFuzz() throws SqlException {
        // Random compiled lists of fixed dates and date variables with a tick calendar, checked against
        // the documented semantics where StaticDateListFuzzer models the list, the parser, which a bind
        // variable takes, where it reads the list alike, the same list with the day filter #Fri for a
        // compile error, and the parser without the tick calendar for a list without a duration of its
        // own. questdb.fuzz.tickExpressions sets the number of lists.
        final int listCount = Integer.getInteger("questdb.fuzz.tickExpressions", 1_000);
        final Rnd rnd = TestUtils.generateRandom(LOG);
        final StaticDateListFuzzer fuzzer = new StaticDateListFuzzer();
        for (int i = 0; i < listCount; i++) {
            fuzzer.check(rnd);
        }
        fuzzer.log();
        if (listCount >= 1_000) {
            fuzzer.assertEveryShape();
        }
    }

    @Test
    public void testCompiledCalendarListFuzz() throws SqlException {
        // Builds random date lists of 2025 days, hours, pairs of hours, minutes and day ranges with a
        // date variable, written one to three times in random spellings, under the XNYS or XHKG
        // calendar and maybe a zone, and expects evaluate() at random nows of 2025 to return what
        // parseTickExpr returns at the same now, and the parts of the sessions that the union of the
        // list covers, from java.time and the test calendar. The compiled list keeps one IR element
        // for the copies of the date variable. Half of the lists add six copies of the days 1-20 of
        // each month, which take the static intervals past the cap and the merge threshold
        // (1,024 + 256), so that the compiler unions them before the calendar runs. The other
        // elements fall on a trading day after the 20th, within one of its sessions. The lists must
        // reach each shape that the assertions at the end name, so the test builds more lists, up to
        // ten times as many, until they do.
        final int listCount = Integer.getInteger("questdb.fuzz.tick.lists", 200);
        final Rnd rnd = TestUtils.generateRandom(LOG);
        final TimestampDriver driver = ColumnType.getTimestampDriver(ColumnType.TIMESTAMP);
        final String copies = repeatedValues("2025-[01..12]-[01..20]", 6);
        final LongList copyDays = new LongList();
        for (int month = 1; month <= 12; month++) {
            copyDays.add(Micros.toMicros(2025, month, 1, 0, 0), Micros.toMicros(2025, month, 21, 0, 0) - 1);
        }
        final ObjList<String> elements = new ObjList<>();
        final LongList staticIntervals = new LongList();
        final LongList listIntervals = new LongList();
        final LongList compiledIntervals = new LongList();
        final LongList parsedIntervals = new LongList();
        final LongList modelIntervals = new LongList();
        final StringSink expression = new StringSink();
        final StringSink staticElementList = new StringSink();
        final StringSink spelling = new StringSink();
        boolean hasNestedPairInSession = false;
        boolean hasDateVariableAcrossJoinedParts = false;
        for (int i = 0; i < listCount || (i < 10 * listCount && !(hasNestedPairInSession && hasDateVariableAcrossJoinedParts)); i++) {
            final String exchange = rnd.nextBoolean() ? "XNYS" : "XHKG";
            final LongList schedule = TestTickCalendarService.INSTANCE.getSchedule(exchange);
            final boolean isZoned = rnd.nextBoolean();
            // the local time of the zone -05:00 lies 5 hours behind UTC
            final long localOffset = isZoned ? -5 * Micros.HOUR_MICROS : 0;
            final boolean hasCopies = rnd.nextBoolean();
            // a session after the 20th day of its month, and an instant within it
            int session;
            do {
                session = rnd.nextInt(schedule.size() / 2);
            } while (toUtcDateTime(schedule.getQuick(2 * session)).getDayOfMonth() <= 20);
            final long sessionLo = schedule.getQuick(2 * session);
            final long sessionHi = schedule.getQuick(2 * session + 1);

            elements.clear();
            staticIntervals.clear();
            final int elementCount = 2 + rnd.nextInt(4);
            int lastHourIndex = -1;
            long pairNow = Long.MIN_VALUE;
            for (int e = 0; e < elementCount; e++) {
                final LocalDateTime local = toUtcDateTime(sessionLo + (long) (rnd.nextDouble() * (sessionHi - sessionLo)) + localOffset);
                final long localDayStart = Micros.toMicros(local.getYear(), local.getMonthValue(), local.getDayOfMonth(), 0, 0);
                final long localHourStart = localDayStart + local.getHour() * Micros.HOUR_MICROS;
                final String date = String.format("%04d-%02d-%02d", local.getYear(), local.getMonthValue(), local.getDayOfMonth());
                switch (rnd.nextInt(6)) {
                    case 0 -> {
                        elements.add(date);
                        staticIntervals.add(localDayStart, localDayStart + Micros.DAY_MICROS - 1);
                    }
                    case 1 -> {
                        elements.add(date + String.format("T%02d", local.getHour()));
                        staticIntervals.add(localHourStart, localHourStart + Micros.HOUR_MICROS - 1);
                        lastHourIndex = staticIntervals.size() - 2;
                    }
                    case 2, 3 -> {
                        // two hours that touch, which a union joins, the second the hour of the instant
                        // in the session, and an instant in it off the full hour
                        final int hour = Math.max(local.getHour(), 1) - 1;
                        final long hourStart = localDayStart + hour * Micros.HOUR_MICROS;
                        elements.add(date + String.format("T[%02d..%02d]", hour, hour + 1));
                        staticIntervals.add(hourStart, hourStart + Micros.HOUR_MICROS - 1);
                        staticIntervals.add(hourStart + Micros.HOUR_MICROS, hourStart + 2 * Micros.HOUR_MICROS - 1);
                        pairNow = hourStart + Micros.HOUR_MICROS + Math.max(1, local.getMinute()) * Micros.MINUTE_MICROS;
                    }
                    case 4 -> {
                        // a minute of an earlier hour element, which lies inside that hour
                        final long minuteStart = (lastHourIndex >= 0 ? staticIntervals.getQuick(lastHourIndex) : localHourStart)
                                + rnd.nextInt(60) * Micros.MINUTE_MICROS;
                        final LocalDateTime minute = toUtcDateTime(minuteStart);
                        elements.add(String.format(
                                "%04d-%02d-%02dT%02d:%02d",
                                minute.getYear(), minute.getMonthValue(), minute.getDayOfMonth(), minute.getHour(), minute.getMinute()
                        ));
                        staticIntervals.add(minuteStart, minuteStart + Micros.MINUTE_MICROS - 1);
                    }
                    default -> {
                        final int firstDay = Math.max(1, local.getDayOfMonth() - rnd.nextInt(3));
                        final int lastDay = Math.min(28, local.getDayOfMonth() + rnd.nextInt(3));
                        elements.add(String.format("%04d-%02d-[%02d..%02d]", local.getYear(), local.getMonthValue(), firstDay, Math.max(firstDay, lastDay)));
                        final long firstDayStart = Micros.toMicros(local.getYear(), local.getMonthValue(), firstDay, 0, 0);
                        staticIntervals.add(firstDayStart, firstDayStart + (Math.max(firstDay, lastDay) - firstDay + 1) * Micros.DAY_MICROS - 1);
                    }
                }
            }
            if (hasCopies) {
                staticIntervals.add(copyDays);
            }
            // the static intervals in UTC
            for (int k = 0, n = staticIntervals.size(); k < n; k++) {
                staticIntervals.setQuick(k, staticIntervals.getQuick(k) - localOffset);
            }
            final boolean isRange = rnd.nextInt(3) > 0;
            final int rangeHours = rnd.nextBoolean() ? 1 : 2 + rnd.nextInt(2);
            if (hasCopies) {
                elements.add(copies);
            }
            // the date variable one to three times, each at a random place in a random spelling
            for (int c = 0, copyCount = 1 + rnd.nextInt(3); c < copyCount; c++) {
                spelling.clear();
                appendDateVariableSpelling(rnd, spelling, isRange, rangeHours);
                elements.insert(rnd.nextInt(elements.size() + 1), 1, spelling.toString());
            }
            // the list, and the list without the date variable
            expression.clear();
            staticElementList.clear();
            for (int e = 0, n = elements.size(); e < n; e++) {
                final String element = elements.getQuick(e);
                expression.put(e == 0 ? '[' : ',').put(element);
                if (element.charAt(0) != '$') {
                    staticElementList.put(staticElementList.length() == 0 ? '[' : ',').put(element);
                }
            }
            final String listSuffix = ']' + (isZoned ? "@-05:00" : "") + '#' + exchange;
            final String tick = expression + listSuffix;
            final String staticElements = staticElementList + listSuffix;

            try (
                    CompiledTickExpression compiled = IntervalUtils.compileTickExpr(driver, configuration, tick, 0, tick.length(), 0);
                    CompiledTickExpression staticOnly = IntervalUtils.compileTickExpr(driver, configuration, staticElements, 0, staticElements.length(), 0)
            ) {
                // one IR element for the copies of the date variable, next to the STATIC entries
                Assert.assertEquals(tick, staticOnly.getElementCount() + 1, compiled.getElementCount());
                for (int trial = 0; trial < 3; trial++) {
                    // The first now falls in the second hour of a pair, so that a range of an hour or more
                    // crosses the start of that hour. The others fall anywhere in 2025. A now off the full
                    // hour gives both ends of the range a time of day.
                    final long now;
                    if (trial == 0 && pairNow != Long.MIN_VALUE) {
                        now = pairNow;
                    } else {
                        now = Micros.toMicros(2025, 1, 1, 0, 0) + rnd.nextInt(365) * Micros.DAY_MICROS
                                + rnd.nextInt(24) * Micros.HOUR_MICROS + (1 + rnd.nextInt(59)) * Micros.MINUTE_MICROS;
                    }
                    // the date variable takes now as a local time
                    final long variableLo = isRange ? now - rangeHours * Micros.HOUR_MICROS : now - now % Micros.DAY_MICROS;
                    final long variableHi = isRange ? now : variableLo + Micros.DAY_MICROS - 1;
                    listIntervals.clear();
                    listIntervals.add(staticIntervals);
                    listIntervals.add(variableLo - localOffset, variableHi - localOffset);
                    modelIntervals.clear();
                    addSessionParts(modelIntervals, unionOf(listIntervals), schedule);
                    final String expected = intervalToString(driver, modelIntervals);

                    compiledIntervals.clear();
                    compiled.evaluate(compiledIntervals, now);
                    Assert.assertEquals(tick + " at " + Micros.toString(now), expected, intervalToString(driver, compiledIntervals));
                    parsedIntervals.clear();
                    IntervalUtils.parseTickExpr(driver, configuration, tick, 0, tick.length(), 0, parsedIntervals, sink, now);
                    Assert.assertEquals(tick + " at " + Micros.toString(now), expected, intervalToString(driver, parsedIntervals));

                    hasNestedPairInSession |= hasNestedPairInSession(listIntervals, schedule);
                    hasDateVariableAcrossJoinedParts |= hasCopies && isRange
                            && hasNestedPairInSession(unionOf(staticIntervals), variableLo - localOffset, variableHi - localOffset, schedule)
                            && !hasNestedPairInSession(staticIntervals, variableLo - localOffset, variableHi - localOffset, schedule);
                }
            }
        }
        Assert.assertTrue("an interval inside another one that ends inside a session", hasNestedPairInSession);
        Assert.assertTrue("a date variable range that lies across two static intervals the compiler joins", hasDateVariableAcrossJoinedParts);
    }

    @Test
    public void testCompiledListStaticIntervalsWithCalendar() throws SqlException {
        // The compiler keeps at most the cap (1,024) of static intervals of a list before the calendar
        // runs, as parseTickExpr does: six copies of the days 1-20 of each month of 2025 hold 1,440
        // days, which the list unions into 12 intervals. Their XNYS sessions are the 163 of those days.
        // $today is the Wednesday 2025-01-08, one of them.
        final TimestampDriver driver = ColumnType.getTimestampDriver(ColumnType.TIMESTAMP);
        final long now = Micros.toMicros(2025, 1, 8, 1, 0);
        final String days = repeatedValues("2025-[01..12]-[01..20]", 6);
        final LongList sessions = new LongList();
        addSessions(sessions, "XNYS", day -> day.getDayOfMonth() <= 20);
        Assert.assertEquals(2 * 163, sessions.size());
        final String expected = intervalToString(driver, sessions);
        final String expression = "[$today," + days + "]#XNYS";
        final LongList out = new LongList();
        try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(driver, configuration, expression, 0, expression.length(), 0)) {
            Assert.assertEquals(13, compiled.getElementCount());
            compiled.evaluate(out, now);
        }
        Assert.assertEquals(expected, intervalToString(driver, out));
        assertTickIntervalWithNow(expected, "[2025-01-08," + days + "]#XNYS", now);

        // With a time, a year turns into one interval per day before the calendar runs: the 1,461 days
        // of 2025-2028 at 09:30 never touch.
        assertCompiledOverMaxIntervals("[$today,[2025..2028]]T09:30#XNYS");
        assertTickIntervalErrorWithNow("[2025-01-08,[2025..2028]-[01..12]]T09:30#XNYS", OVER_CAP_ERROR, now);
    }

    @Test
    public void testCompiledListStaticIntervalsWithCalendarNanos() throws SqlException {
        // The list of testCompiledListStaticIntervalsWithCalendar with TIMESTAMP_NS: the sessions end
        // on their last nanosecond.
        final TimestampDriver driver = ColumnType.getTimestampDriver(ColumnType.TIMESTAMP_NANO);
        final LongList sessions = new LongList();
        addSessions(sessions, "XNYS", day -> day.getDayOfMonth() <= 20);
        for (int k = 0, n = sessions.size(); k < n; k += 2) {
            sessions.setQuick(k, driver.fromMicros(sessions.getQuick(k)));
            sessions.setQuick(k + 1, driver.fromMicros(sessions.getQuick(k + 1)) + 999);
        }
        final String expected = intervalToString(driver, sessions);
        final String days = repeatedValues("2025-[01..12]-[01..20]", 6);
        final String expression = "[$today," + days + "]#XNYS";
        final LongList out = new LongList();
        try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(driver, configuration, expression, 0, expression.length(), 0)) {
            Assert.assertEquals(13, compiled.getElementCount());
            compiled.evaluate(out, driver.fromMicros(Micros.toMicros(2025, 1, 8, 1, 0)));
        }
        Assert.assertEquals(expected, intervalToString(driver, out));
        assertTickIntervalNanos(expected, "[2025-01-08," + days + "]#XNYS");
    }

    @Test
    public void testCompiledRepeatedDateVariableWithCalendar() throws SqlException {
        // The list stores the copies of $today, in any case, as one element. The calendar runs on the
        // union of the list, so they yield what one copy yields: the hour 15:00 of 2025-01-02 within
        // its XNYS session, and the session of $today, the Monday 2025-01-06.
        final TimestampDriver driver = ColumnType.getTimestampDriver(ColumnType.TIMESTAMP);
        final String expression = "[$today,2025-01-02T15,$TODAY,$today]#XNYS";
        final LongList out = new LongList();
        try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(driver, configuration, expression, 0, expression.length(), 0)) {
            Assert.assertEquals(2, compiled.getElementCount());
            compiled.evaluate(out, Micros.toMicros(2025, 1, 6, 1, 0));
        }
        Assert.assertEquals(
                "[{lo=2025-01-02T15:00:00.000000Z, hi=2025-01-02T15:59:59.999999Z},"
                        + "{lo=2025-01-06T14:30:00.000000Z, hi=2025-01-06T20:59:59.999999Z}]",
                intervalToString(driver, out)
        );
    }

    @Test
    public void testCompiledTickExprWithExchangeFilterHasNoIntervalBound() throws SqlException {
        // The sessions of a day and its holidays set the interval count, so the JIT filter cannot
        // compile a fixed number of range checks for the expression and leaves it to the Java filter.
        for (String expression : new String[]{"$today#XNYS", "[$today, $tomorrow]#XNYS", "$today#XNYS;1h"}) {
            try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(
                    ColumnType.getTimestampDriver(ColumnType.TIMESTAMP), configuration, expression, 0, expression.length(), 0)) {
                Assert.assertEquals(expression, -1, compiled.getMaxIntervalCount(Integer.MAX_VALUE));
            }
        }
    }

    @Test
    public void testDateListBracketExpansionWithPerElementExchangeFilter() throws SqlException {
        // Bracket expansion inside element WITH per-element exchange filter
        // [2025-01-[24..28]#XNYS,2025-02-03] - the #XNYS applies to all expanded dates from 24..28
        // 24 (Fri), 27 (Mon), 28 (Tue) are trading days; 25 (Sat), 26 (Sun) are not
        // 2025-02-03 has no filter so returns full day
        assertTickInterval(
                "[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T20:59:59.999999Z}," +
                        "{lo=2025-01-27T14:30:00.000000Z, hi=2025-01-27T20:59:59.999999Z}," +
                        "{lo=2025-01-28T14:30:00.000000Z, hi=2025-01-28T20:59:59.999999Z}," +
                        "{lo=2025-02-03T00:00:00.000000Z, hi=2025-02-03T23:59:59.999999Z}]",
                "[2025-01-[24..28]#XNYS,2025-02-03]"
        );
    }

    @Test
    public void testDateListCoveredElementOverMaxIntervalsWithCalendar() throws SqlException {
        // The 1,200 slots of 10 ms of the second element never touch. With no calendar of its own,
        // the element adds them to the merge range of the list, whose first element covers them,
        // and the global calendar keeps the session of 2025-01-02, 14:30 to 21:00 UTC.
        assertTickInterval(
                "[{lo=2025-01-02T14:30:00.000000Z, hi=2025-01-02T20:59:59.999999Z}]",
                "[2025-01-02,2025-01-02T[14..15]:00:[00..59].[0..9]0]#XNYS"
        );
        // Its own calendar runs on the element's intervals only, so the element keeps a merge range
        // of its own and counts its 1,200 slots against the cap (1,024).
        assertTickIntervalErrorWithNow(
                "[2025-01-02,2025-01-02T[14..15]:00:[00..59].[0..9]0#XNYS]",
                "Bracket expansion produces too many intervals (max 1024)",
                Micros.toMicros(2025, 1, 2, 12, 0)
        );
        // The same for a day of a date variable range, $today is 2025-01-02: its calendar cuts the
        // merged minutes 14:00 to 20:59 at the open. A merge from the start of the list would join
        // the first element's minute of 2025-01-01 and move the day's first block below the day's
        // start, where the calendar never reaches it.
        assertTickIntervalWithNow(
                "[{lo=2025-01-01T00:00:00.000000Z, hi=2025-01-01T00:00:59.999999Z},{lo=2025-01-02T14:30:00.000000Z, hi=2025-01-02T20:59:59.999999Z}]",
                "[2025-01-01T00:00:[00..59],$today..$today#XNYS]T[14..20]:[00..59]",
                Micros.toMicros(2025, 1, 2, 12, 0)
        );
    }

    @Test
    public void testDateListElementOverMaxIntervalsAtLastLeafWithCalendar() throws SqlException {
        // The second element's own calendar gives it a merge range of its own, whose unions wait
        // until it grew by the merge threshold (256) since the last one. The outermost bracket of
        // the element unions the range once more after its last leaf, so the element counts its
        // intervals against the cap (1,024) before its calendar runs: 25 days x 41 minutes are
        // 1,025 seconds that never touch, one leaf after the last union by the threshold, although
        // the first element covers them and the calendar keeps only those of the trading days.
        assertTickIntervalErrorWithNow(
                "[2025-01,2025-01-[01..25]T15:[00..40]:00#XNYS]",
                "Bracket expansion produces too many intervals (max 1024)",
                Micros.toMicros(2025, 1, 2, 12, 0)
        );
        // control: 16 days x 8 hours x 8 minutes are 1,024 seconds, at the cap, and the month covers
        // those that the calendar keeps
        assertTickInterval(
                "[{lo=2025-01-01T00:00:00.000000Z, hi=2025-01-31T23:59:59.999999Z}]",
                "[2025-01,2025-01-[01..16]T[15..22]:[00..07]:00#XNYS]"
        );
    }

    @Test
    public void testDateListGlobalFilterWithDuration() throws SqlException {
        // Date list with global exchange filter and duration
        // [2025-01-24,2025-01-27]#XNYS;1h - Fri and Mon
        assertTickInterval(
                "[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T21:59:59.999999Z}," +
                        "{lo=2025-01-27T14:30:00.000000Z, hi=2025-01-27T21:59:59.999999Z}]",
                "[2025-01-24,2025-01-27]#XNYS;1h"
        );
    }

    @Test
    public void testDateListMixedPerElementAndGlobalExchangeFilter() throws SqlException {
        // Mix of per-element and global exchange filter
        // [2025-01-24#XNYS,2025-02-03]#XHKG
        // Per-element filter is applied first, then global filter intersects with the result:
        // - 2025-01-24#XNYS produces 14:30-21:00 UTC, intersected with XHKG (01:30-04:00, 05:00-08:00 UTC) -> empty
        // - 2025-02-03 (full day), intersected with XHKG -> XHKG hours
        assertTickInterval(
                "[{lo=2025-02-03T01:30:00.000000Z, hi=2025-02-03T03:59:59.999999Z}," +
                        "{lo=2025-02-03T05:00:00.000000Z, hi=2025-02-03T07:59:59.999999Z}]",
                "[2025-01-24#XNYS,2025-02-03]#XHKG"
        );
    }

    @Test
    public void testDateListNestedElementsWithGlobalExchangeFilter() throws SqlException {
        // Nested date list elements with a global exchange filter
        // [2025-01, 2025-01-10]#XNYS: the month element [Jan 1, Jan 31] fully contains
        // the day element [Jan 10, Jan 10]. After the lo-sort inside
        // applyTickCalendarFilter, the LAST interval (Jan 10) has a smaller hi than the
        // month element, so the schedule scan window must use the maximum hi (Jan 31),
        // not the last interval's hi - otherwise all sessions after Jan 10 are dropped.
        // XNYS January 2025 trading days: 2, 3, 6, 7, 8, 10, 13, 14, 15, 16, 17,
        // 21, 22, 23, 24, 27, 28, 29, 30, 31 (closed Jan 9 and Jan 20).
        assertTickInterval(
                "[{lo=2025-01-02T14:30:00.000000Z, hi=2025-01-02T20:59:59.999999Z}," +
                        "{lo=2025-01-03T14:30:00.000000Z, hi=2025-01-03T20:59:59.999999Z}," +
                        "{lo=2025-01-06T14:30:00.000000Z, hi=2025-01-06T20:59:59.999999Z}," +
                        "{lo=2025-01-07T14:30:00.000000Z, hi=2025-01-07T20:59:59.999999Z}," +
                        "{lo=2025-01-08T14:30:00.000000Z, hi=2025-01-08T20:59:59.999999Z}," +
                        "{lo=2025-01-10T14:30:00.000000Z, hi=2025-01-10T20:59:59.999999Z}," +
                        "{lo=2025-01-13T14:30:00.000000Z, hi=2025-01-13T20:59:59.999999Z}," +
                        "{lo=2025-01-14T14:30:00.000000Z, hi=2025-01-14T20:59:59.999999Z}," +
                        "{lo=2025-01-15T14:30:00.000000Z, hi=2025-01-15T20:59:59.999999Z}," +
                        "{lo=2025-01-16T14:30:00.000000Z, hi=2025-01-16T20:59:59.999999Z}," +
                        "{lo=2025-01-17T14:30:00.000000Z, hi=2025-01-17T20:59:59.999999Z}," +
                        "{lo=2025-01-21T14:30:00.000000Z, hi=2025-01-21T20:59:59.999999Z}," +
                        "{lo=2025-01-22T14:30:00.000000Z, hi=2025-01-22T20:59:59.999999Z}," +
                        "{lo=2025-01-23T14:30:00.000000Z, hi=2025-01-23T20:59:59.999999Z}," +
                        "{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T20:59:59.999999Z}," +
                        "{lo=2025-01-27T14:30:00.000000Z, hi=2025-01-27T20:59:59.999999Z}," +
                        "{lo=2025-01-28T14:30:00.000000Z, hi=2025-01-28T20:59:59.999999Z}," +
                        "{lo=2025-01-29T14:30:00.000000Z, hi=2025-01-29T20:59:59.999999Z}," +
                        "{lo=2025-01-30T14:30:00.000000Z, hi=2025-01-30T20:59:59.999999Z}," +
                        "{lo=2025-01-31T14:30:00.000000Z, hi=2025-01-31T20:59:59.999999Z}]",
                "[2025-01, 2025-01-10]#XNYS"
        );
    }

    @Test
    public void testDateListNestedElementsWithGlobalExchangeFilterNanos() throws SqlException {
        // Nanos variant of the nested-element case: the month element [Feb 1, Feb 28]
        // contains the day element [Feb 3, Feb 3]. The max-hi query bound (Feb 28 end)
        // must survive the nanos-to-micros ceiling conversion so that all February
        // sessions after Feb 3 stay in the schedule scan window.
        // XNYS February 2025 trading days: 3-7, 10-14, 18-21, 24-28 (closed Feb 17).
        assertTickIntervalNanos(
                "[{lo=2025-02-03T14:30:00.000000000Z, hi=2025-02-03T20:59:59.999999999Z}," +
                        "{lo=2025-02-04T14:30:00.000000000Z, hi=2025-02-04T20:59:59.999999999Z}," +
                        "{lo=2025-02-05T14:30:00.000000000Z, hi=2025-02-05T20:59:59.999999999Z}," +
                        "{lo=2025-02-06T14:30:00.000000000Z, hi=2025-02-06T20:59:59.999999999Z}," +
                        "{lo=2025-02-07T14:30:00.000000000Z, hi=2025-02-07T20:59:59.999999999Z}," +
                        "{lo=2025-02-10T14:30:00.000000000Z, hi=2025-02-10T20:59:59.999999999Z}," +
                        "{lo=2025-02-11T14:30:00.000000000Z, hi=2025-02-11T20:59:59.999999999Z}," +
                        "{lo=2025-02-12T14:30:00.000000000Z, hi=2025-02-12T20:59:59.999999999Z}," +
                        "{lo=2025-02-13T14:30:00.000000000Z, hi=2025-02-13T20:59:59.999999999Z}," +
                        "{lo=2025-02-14T14:30:00.000000000Z, hi=2025-02-14T20:59:59.999999999Z}," +
                        "{lo=2025-02-18T14:30:00.000000000Z, hi=2025-02-18T20:59:59.999999999Z}," +
                        "{lo=2025-02-19T14:30:00.000000000Z, hi=2025-02-19T20:59:59.999999999Z}," +
                        "{lo=2025-02-20T14:30:00.000000000Z, hi=2025-02-20T20:59:59.999999999Z}," +
                        "{lo=2025-02-21T14:30:00.000000000Z, hi=2025-02-21T20:59:59.999999999Z}," +
                        "{lo=2025-02-24T14:30:00.000000000Z, hi=2025-02-24T20:59:59.999999999Z}," +
                        "{lo=2025-02-25T14:30:00.000000000Z, hi=2025-02-25T20:59:59.999999999Z}," +
                        "{lo=2025-02-26T14:30:00.000000000Z, hi=2025-02-26T20:59:59.999999999Z}," +
                        "{lo=2025-02-27T14:30:00.000000000Z, hi=2025-02-27T20:59:59.999999999Z}," +
                        "{lo=2025-02-28T14:30:00.000000000Z, hi=2025-02-28T20:59:59.999999999Z}]",
                "[2025-02, 2025-02-03]#XNYS"
        );
    }

    @Test
    public void testDateListPerElementExchangeFilter() throws SqlException {
        // Per-element exchange filter inside date list
        // [2025-01-24#XNYS,2025-02-03#XHKG] - each element has its own exchange
        // 2025-01-24 (Fri) uses XNYS (single interval), 2025-02-03 (Mon) uses XHKG (two intervals with lunch break)
        assertTickInterval(
                "[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T20:59:59.999999Z}," +
                        "{lo=2025-02-03T01:30:00.000000Z, hi=2025-02-03T03:59:59.999999Z}," +
                        "{lo=2025-02-03T05:00:00.000000Z, hi=2025-02-03T07:59:59.999999Z}]",
                "[2025-01-24#XNYS,2025-02-03#XHKG]"
        );
    }

    @Test
    public void testDateListPerElementExchangeFilterFiltersOut() throws SqlException {
        // Per-element exchange filter that filters out the element
        // 2025-01-25 is Saturday (not a trading day for XNYS), 2025-02-03 is Monday (trading day for XHKG)
        assertTickInterval(
                "[{lo=2025-02-03T01:30:00.000000Z, hi=2025-02-03T03:59:59.999999Z}," +
                        "{lo=2025-02-03T05:00:00.000000Z, hi=2025-02-03T07:59:59.999999Z}]",
                "[2025-01-25#XNYS,2025-02-03#XHKG]"
        );
    }

    @Test
    public void testDateListPerElementExchangeFilterWithTimeSuffix() throws SqlException {
        // Per-element exchange filter with time suffix
        // [2025-01-24#XNYS,2025-02-03#XHKG]T03:00
        // 03:00 UTC is within XHKG morning session (02:00-04:00) but outside XNYS hours (14:30-21:00)
        assertTickInterval(
                "[{lo=2025-02-03T03:00:00.000000Z, hi=2025-02-03T03:00:59.999999Z}]",
                "[2025-01-24#XNYS,2025-02-03#XHKG]T03:00"
        );
    }

    @Test
    public void testDateListPerElementTimeWithGlobalTimeSuffix() throws SqlException {
        // Date list with per-element time and global time suffix
        // [2025-01-24T15:00,2025-01-27]T16:00#XNYS
        // Element with time (15:00) keeps its time, element without (Jan 27) uses global (16:00)
        assertTickInterval(
                "[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T15:00:59.999999Z}," +
                        "{lo=2025-01-27T16:00:00.000000Z, hi=2025-01-27T16:00:59.999999Z}]",
                "[2025-01-24T15:00,2025-01-27]T16:00#XNYS"
        );
    }

    @Test
    public void testDateListWithBracketExpansionAndGlobalExchangeFilter() throws SqlException {
        // Date list with bracket expansion AND global exchange filter
        // [2025-01-[24..28],2025-01-31]#XNYS should expand Jan 24-28 and Jan 31, then apply XNYS
        // Trading days: Jan 24 (Fri), Jan 27 (Mon), Jan 28 (Tue), Jan 31 (Fri)
        assertTickInterval(
                "[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T20:59:59.999999Z}," +
                        "{lo=2025-01-27T14:30:00.000000Z, hi=2025-01-27T20:59:59.999999Z}," +
                        "{lo=2025-01-28T14:30:00.000000Z, hi=2025-01-28T20:59:59.999999Z}," +
                        "{lo=2025-01-31T14:30:00.000000Z, hi=2025-01-31T20:59:59.999999Z}]",
                "[2025-01-[24..28],2025-01-31]#XNYS"
        );
    }

    @Test
    public void testDateListWithGlobalTimezoneAndExchangeFilter() throws SqlException {
        // Date list with NYSE timezone (EST = -05:00) and exchange filter
        // [2025-01-24,2025-01-27]@-05:00#XNYS
        // Jan 24 in -05:00 = [2025-01-24T05:00:00Z, 2025-01-25T04:59:59.999999Z]
        // Jan 27 in -05:00 = [2025-01-27T05:00:00Z, 2025-01-28T04:59:59.999999Z]
        // XNYS Jan 24 (Fri): 14:30-21:00 UTC -> [14:30, 20:59:59.999999]
        // Jan 25 (Sat) - no trading
        // XNYS Jan 27 (Mon): 14:30-21:00 UTC -> [14:30, 20:59:59.999999]
        // Jan 28 (Tue) - overlaps but query ends at 04:59:59.999999, so no intersection
        assertTickInterval(
                "[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T20:59:59.999999Z}," +
                        "{lo=2025-01-27T14:30:00.000000Z, hi=2025-01-27T20:59:59.999999Z}]",
                "[2025-01-24,2025-01-27]@-05:00#XNYS"
        );
    }

    @Test
    public void testDateListWithSpacesAndExchangeFilter() throws SqlException {
        // Date list with spaces around comma
        // [2025-01-24, 2025-01-27]#XNYS - note the space after comma
        assertTickInterval(
                "[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T20:59:59.999999Z}," +
                        "{lo=2025-01-27T14:30:00.000000Z, hi=2025-01-27T20:59:59.999999Z}]",
                "[2025-01-24, 2025-01-27]#XNYS"
        );
    }

    @Test
    public void testDateListWithTimeSuffixAndGlobalExchangeFilter() throws SqlException {
        // Date list with time suffix + global exchange filter
        // The time suffix restricts the interval, then exchange filter applies
        // [2025-01-24,2025-01-27]T15:00#XNYS
        // 15:00 UTC is within XNYS trading hours (14:30-21:00 UTC)
        assertTickInterval(
                "[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T15:00:59.999999Z}," +
                        "{lo=2025-01-27T15:00:00.000000Z, hi=2025-01-27T15:00:59.999999Z}]",
                "[2025-01-24,2025-01-27]T15:00#XNYS"
        );
    }

    @Test
    public void testDateVariableMixedWithExchangeFilters() throws SqlException {
        // Mix date variable with fixed date, each with different exchange filter
        // [$today#XNYS,2025-02-03#XHKG] with now = 2025-01-24 (Friday, trading day)
        // $today resolves to 2025-01-24, XNYS hours: 14:30-20:59:59.999999
        // 2025-02-03 XHKG: two sessions (01:30-03:59:59.999999, 05:00-07:59:59.999999)
        long nowTimestamp = Micros.toMicros(2025, 1, 24, 12, 0);
        assertTickIntervalWithNow(
                "[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T20:59:59.999999Z}," +
                        "{lo=2025-02-03T01:30:00.000000Z, hi=2025-02-03T03:59:59.999999Z}," +
                        "{lo=2025-02-03T05:00:00.000000Z, hi=2025-02-03T07:59:59.999999Z}]",
                "[$today#XNYS,2025-02-03#XHKG]",
                nowTimestamp
        );
    }

    @Test
    public void testDateVariableRangeAddingNoIntervalWithTickCalendar() throws SqlException {
        // A date variable range that adds no interval, one that starts after it ends, without the
        // range order check, or a range of business days that holds no business day, takes the
        // tick calendar of its element for one, as a day of the range does. A timezone before the
        // calendar ends at its '#'. 2025-01-28 is a Tuesday, and 2025-01-25 a Saturday.
        for (int timestampType : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
            final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
            final String context = " [" + ColumnType.nameOf(timestampType) + ']';
            final long tuesday = driver.parseFloorLiteral("2025-01-28T10:00:00.000000Z");
            final long saturday = driver.parseFloorLiteral("2025-01-25T10:00:00.000000Z");
            final String zoneError = "error: invalid timezone: Bogus/Zone";
            Assert.assertEquals(context, "error: Invalid date range: start is after end", parseTickExpr(driver, "[$tomorrow..$today@Bogus/Zone#XNYS]", tuesday));
            for (int checks : new int[]{IntervalUtils.TICK_CHECK_INTERVAL_LIMIT, 0}) {
                Assert.assertEquals(context, "[]", parseTickExpr(driver, "[$tomorrow..$today#XNYS]", tuesday, checks));
                Assert.assertEquals(context, "[]", parseTickExpr(driver, "[$tomorrow..$today@UTC#XNYS]", tuesday, checks));
                Assert.assertEquals(context, zoneError, parseTickExpr(driver, "[$tomorrow..$today@Bogus/Zone#XNYS]", tuesday, checks));
            }
            for (int checks : new int[]{IntervalUtils.TICK_CHECK_ALL, IntervalUtils.TICK_CHECK_INTERVAL_LIMIT, 0}) {
                Assert.assertEquals(context, "[]", parseTickExpr(driver, "[$today..$today + 0bd#XNYS]", saturday, checks));
                Assert.assertEquals(context, "[]", parseTickExpr(driver, "[$today..$today + 0bd@UTC#XNYS]", saturday, checks));
                Assert.assertEquals(context, zoneError, parseTickExpr(driver, "[$today..$today + 0bd@Bogus/Zone#XNYS]", saturday, checks));
            }
            // the compiled form parses an element with a tick calendar of its own on every
            // evaluation, and fails as the SQL that holds it is validated
            for (String expression : new String[]{"[$tomorrow..$today@Bogus/Zone#XNYS]", "[$today..$today + 0bd@Bogus/Zone#XNYS]"}) {
                try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(driver, configuration, expression, 0, expression.length(), 0)) {
                    Assert.assertTrue(expression + context, compiled.isParsedOnEvaluation());
                    try {
                        compiled.validate(expression.contains("bd") ? saturday : tuesday);
                        Assert.fail("validate should fail: " + expression + context);
                    } catch (SqlException e) {
                        Assert.assertEquals(expression + context, zoneError, "error: " + e.getFlyweightMessage());
                    }
                }
            }
            // the same ranges where they add intervals: the trading sessions of the Tuesday and of
            // the Wednesday from 14:30 to 21:00Z
            final String sessions = "[{lo=2025-01-28T14:30:00.000000Z, hi=2025-01-28T20:59:59.999999Z},{lo=2025-01-29T14:30:00.000000Z, hi=2025-01-29T20:59:59.999999Z}]";
            Assert.assertEquals(
                    context,
                    ColumnType.isTimestampNano(timestampType) ? sessions.replace("00Z", "00000Z").replace("99Z", "99999Z") : sessions,
                    parseTickExpr(driver, "[$today..$tomorrow#XNYS]", tuesday)
            );
            Assert.assertEquals(context, zoneError, parseTickExpr(driver, "[$today..$tomorrow@Bogus/Zone#XNYS]", tuesday));
            Assert.assertEquals(context, zoneError, parseTickExpr(driver, "[$today..$today + 0bd@Bogus/Zone#XNYS]", tuesday));
        }
    }

    @Test
    public void testDateVariableRangeElementCalendarMatchesListCalendarFuzz() throws SqlException {
        // A date variable range of either kind with a timezone and a tick calendar of its element
        // selects, among other elements, what the range selects alone with the same suffix on the
        // list, see TickExprTest.ElementSuffixChecker. A range of days runs the tick calendar of
        // its element on each of its days, and takes no negative duration here.
        // questdb.fuzz.tickExpressions sets the number of expressions.
        final int expressionCount = Integer.getInteger("questdb.fuzz.tickExpressions", 1_000);
        final String[] nows = {
                "2025-01-24T15:30:00.000000Z", // a Friday, in the XNYS session
                "2025-01-24T03:00:00.000000Z", // a Friday, in the morning XHKG session
                "2025-01-27T04:30:00.000000Z", // a Monday, in the XHKG lunch break
                "2025-01-20T15:00:00.000000Z", // a Monday that XNYS closes for
                "2025-01-25T12:00:00.000000Z", // a Saturday
                "2025-01-26T23:30:00.000000Z", // half an hour before a Monday
                "2025-03-10T14:00:00.000000Z" // the Monday after New York moves to summer time
        };
        final String[] calendars = {"", "#XNYS", "#XHKG", "#xnys"};
        final String[] dayRangeDurations = {"", ";1h", ";30m", ";0s", ";1d"};
        final String[] pointRangeDurations = {"", ";1h", ";30m", ";0s", ";-30m", ";1d"};
        final Rnd rnd = TestUtils.generateRandom(LOG);
        for (int timestampType : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
            final TickExprTest.ElementSuffixChecker checker = new TickExprTest.ElementSuffixChecker(
                    ColumnType.getTimestampDriver(timestampType),
                    configuration,
                    calendars,
                    dayRangeDurations,
                    pointRangeDurations,
                    nows
            );
            for (int i = 0; i < expressionCount; i++) {
                checker.check(rnd);
            }
            checker.log("tick calendar");
            if (expressionCount >= 1_000) {
                checker.assertEveryShape();
                // a tick calendar after a timezone that reads as another zone, or fails, where it
                // runs on into the tick calendar, such as UTC+05 or Z
                Assert.assertTrue(checker.zoneBeforeSuffixCount > 0);
            }
        }
    }

    @Test
    public void testDateVariableRangeElementZoneBeforeCalendar() throws SqlException {
        // The timezone of a date variable range element ends at the tick calendar of the element,
        // as the one of a single element and the one of the list do, so UTC-05 is an offset rather
        // than UTC, and one that does not parse fails without the calendar in its text. On Friday
        // 2025-01-24 XNYS trades from 14:30 to 21:00Z.
        for (int timestampType : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
            final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
            final long friday = driver.parseFloorLiteral("2025-01-24T15:30:00.000000Z");
            // 13:30 to 15:30 at -05:00 is 18:30 to 20:30Z
            final String evening = forTimestampType("[{lo=2025-01-24T18:30:00.000000Z, hi=2025-01-24T20:30:00.000000Z}]", timestampType);
            Assert.assertEquals(evening, parseTickExpr(driver, "[$now - 2h..$now]@UTC-05#XNYS", friday));
            assertParsedAtEveryCheckLevel(driver, "[$now - 2h..$now@UTC-05#XNYS]", friday, evening);
            assertCompiledParsesOnEvaluation(driver, "[$now - 2h..$now@UTC-05#XNYS]", friday, evening);
            // 10:00 at -05:00 is 15:00Z, and the duration ends the minute an hour later
            final String ten = forTimestampType("[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T16:00:59.999999Z}]", timestampType);
            Assert.assertEquals(ten, parseTickExpr(driver, "[$today@UTC-05#XNYS]T10:00;1h", friday));
            Assert.assertEquals(ten, parseTickExpr(driver, "[$today..$today]T10:00@UTC-05#XNYS;1h", friday));
            assertParsedAtEveryCheckLevel(driver, "[$today..$today@UTC-05#XNYS]T10:00;1h", friday, ten);
            assertCompiledParsesOnEvaluation(driver, "[$today..$today@UTC-05#XNYS]T10:00;1h", friday, ten);
            // a range that starts after it ends
            Assert.assertEquals("error: Invalid date range: start is after end", parseTickExpr(driver, "[$now..$now - 1h@Bogus/Zone#XNYS]", friday));
            for (int checks : new int[]{IntervalUtils.TICK_CHECK_INTERVAL_LIMIT, 0}) {
                Assert.assertEquals("error: invalid timezone: Bogus/Zone", parseTickExpr(driver, "[$now..$now - 1h@Bogus/Zone#XNYS]", friday, checks));
                Assert.assertEquals("[]", parseTickExpr(driver, "[$now..$now - 1h#XNYS]", friday, checks));
                Assert.assertEquals("[]", parseTickExpr(driver, "[$now..$now - 1h@UTC-05#XNYS]", friday, checks));
            }
        }
    }

    @Test
    public void testDateVariableRangeWithExchangeFilter() throws SqlException {
        // $today..$today+10d with exchange filter, now = 2025-01-24 (Friday)
        // Range: Jan 24 to Feb 3 (11 days)
        // Trading days: Jan 24(Fri), 27(Mon), 28(Tue), 29(Wed), 30(Thu), 31(Fri), Feb 3(Mon) = 7
        long nowTimestamp = Micros.toMicros(2025, 1, 24, 12, 0);
        assertTickIntervalWithNow(
                "[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T20:59:59.999999Z}," +
                        "{lo=2025-01-27T14:30:00.000000Z, hi=2025-01-27T20:59:59.999999Z}," +
                        "{lo=2025-01-28T14:30:00.000000Z, hi=2025-01-28T20:59:59.999999Z}," +
                        "{lo=2025-01-29T14:30:00.000000Z, hi=2025-01-29T20:59:59.999999Z}," +
                        "{lo=2025-01-30T14:30:00.000000Z, hi=2025-01-30T20:59:59.999999Z}," +
                        "{lo=2025-01-31T14:30:00.000000Z, hi=2025-01-31T20:59:59.999999Z}," +
                        "{lo=2025-02-03T14:30:00.000000Z, hi=2025-02-03T20:59:59.999999Z}]",
                "[$today..$today+10d]#XNYS",
                nowTimestamp
        );
    }

    @Test
    public void testDateVariableRangeWithPerElementExchangeFilter() throws SqlException {
        // Per-element exchange filter on a date variable range (inside brackets)
        // [$today..$today+10d#XNYS] with now = 2025-01-24 (Friday)
        long nowTimestamp = Micros.toMicros(2025, 1, 24, 12, 0);
        assertTickIntervalWithNow(
                "[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T20:59:59.999999Z}," +
                        "{lo=2025-01-27T14:30:00.000000Z, hi=2025-01-27T20:59:59.999999Z}," +
                        "{lo=2025-01-28T14:30:00.000000Z, hi=2025-01-28T20:59:59.999999Z}," +
                        "{lo=2025-01-29T14:30:00.000000Z, hi=2025-01-29T20:59:59.999999Z}," +
                        "{lo=2025-01-30T14:30:00.000000Z, hi=2025-01-30T20:59:59.999999Z}," +
                        "{lo=2025-01-31T14:30:00.000000Z, hi=2025-01-31T20:59:59.999999Z}," +
                        "{lo=2025-02-03T14:30:00.000000Z, hi=2025-02-03T20:59:59.999999Z}]",
                "[$today..$today+10d#XNYS]",
                nowTimestamp
        );
    }

    @Test
    public void testDateVariableRangesInAnyOrderWithExchangeFilterAndDuration() throws SqlException {
        // 200 later days first, then the 201 days before them, two 1-minute slots a day: 802 intervals,
        // which merge after each day once there are 256 of them, in front of the later days, before
        // the tick calendar runs. It keeps the slots of the XNYS sessions, then the duration ends each
        // an hour later, and the final union joins the two: 14:30 to 16:30:59Z on each of the 250
        // sessions of 2025 in the 401 days around 2025-07-01, the same as with the days in order and
        // as with a bind variable.
        final LocalDate[] holidays = {
                LocalDate.of(2025, 1, 1), LocalDate.of(2025, 1, 9), LocalDate.of(2025, 1, 20), LocalDate.of(2025, 2, 17),
                LocalDate.of(2025, 4, 18), LocalDate.of(2025, 5, 26), LocalDate.of(2025, 6, 19), LocalDate.of(2025, 7, 4),
                LocalDate.of(2025, 9, 1), LocalDate.of(2025, 11, 27), LocalDate.of(2025, 12, 25)
        };
        final String laterDaysFirst = "[$today+1d..$today+200d, $today-200d..$today]T[14:30,15:30]#XNYS;1h";
        final String inOrder = "[$today-200d..$today+200d]T[14:30,15:30]#XNYS;1h";
        for (int timestampType : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
            final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
            final String context = " [" + ColumnType.nameOf(timestampType) + ']';
            final LongList expected = new LongList();
            for (LocalDate day = LocalDate.of(2025, 1, 1); day.getYear() == 2025; day = day.plusDays(1)) {
                if (day.getDayOfWeek() != DayOfWeek.SATURDAY && day.getDayOfWeek() != DayOfWeek.SUNDAY && !Arrays.asList(holidays).contains(day)) {
                    final long dayStart = driver.fromDays((int) day.toEpochDay());
                    expected.add(dayStart + driver.fromMinutes(14 * 60 + 30), dayStart + driver.fromMinutes(16 * 60 + 31) - 1);
                }
            }
            Assert.assertEquals(context, 2 * 250, expected.size());
            final String expectedText = intervalToString(driver, expected);
            final long now = driver.parseFloorLiteral("2025-07-01T12:00:00.000000Z");
            for (String expression : new String[]{laterDaysFirst, inOrder}) {
                try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(driver, configuration, expression, 0, expression.length(), 0)) {
                    Assert.assertFalse(expression + context, compiled.isParsedOnEvaluation());
                }
                Assert.assertEquals(expression + context, expectedText, evaluateCompiledTickExpr(driver, expression, now));
                Assert.assertEquals(expression + context, expectedText, parseTickExpr(driver, expression, now));
            }
        }
    }

    @Test
    public void testDateVariableRangeWithPerElementExchangeFilterAndDayFilter() throws SqlException {
        // An element's own exchange filter replaces the list's day filter for that element, as in the
        // date list below: #Mon keeps Tuesday 2025-01-28 too. The 420 minutes of each day reach the
        // incremental merge threshold (256), and that merge must not apply the day filter either.
        // XNYS trades 14:30-21:00 UTC on both days, and the time list covers 14:00-20:59.
        // now = 2025-01-27 (Monday)
        final long nowTimestamp = Micros.toMicros(2025, 1, 27, 12, 0);
        final String expected = "[{lo=2025-01-27T14:30:00.000000Z, hi=2025-01-27T20:59:59.999999Z}," +
                "{lo=2025-01-28T14:30:00.000000Z, hi=2025-01-28T20:59:59.999999Z}]";
        assertTickIntervalWithNow(expected, "[$today..$today+1d#XNYS]T[14..20]:[00..59]#Mon", nowTimestamp);
        assertTickInterval(expected, "[2025-01-27#XNYS,2025-01-28#XNYS]T[14..20]:[00..59]#Mon");
    }

    @Test
    public void testDateVariableWithExchangeFilter() throws SqlException {
        // $today with exchange filter, now = 2025-01-24 (Friday, trading day)
        // Should return XNYS trading hours for Jan 24
        long nowTimestamp = Micros.toMicros(2025, 1, 24, 12, 0);
        assertTickIntervalWithNow(
                "[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T20:59:59.999999Z}]",
                "[$today#XNYS]",
                nowTimestamp
        );
    }

    @Test
    public void testDatesOutOfOrderWithExchangeFilter() throws SqlException {
        // Dates out of order should be sorted in output
        // [2025-01-28,2025-01-24,2025-01-27]#XNYS - Tue, Fri, Mon out of order
        // Should output in chronological order: Fri, Mon, Tue
        assertTickInterval(
                "[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T20:59:59.999999Z}," +
                        "{lo=2025-01-27T14:30:00.000000Z, hi=2025-01-27T20:59:59.999999Z}," +
                        "{lo=2025-01-28T14:30:00.000000Z, hi=2025-01-28T20:59:59.999999Z}]",
                "[2025-01-28,2025-01-24,2025-01-27]#XNYS"
        );
    }

    @Test
    public void testExchangeFilterBinarySearchBoundaries() throws SqlException {
        // Tests microsecond-precision boundaries of the binary search in applyTickCalendarFilter.
        // XNYS Jan 24 2025 (Fri) schedule: open=14:30:00.000000, close=21:00:00.000000 (exclusive after -1).

        // (1) Query at exact open microsecond -> single-microsecond overlap
        assertTickInterval(
                "[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T14:30:00.000000Z}]",
                "2025-01-24T14:30:00.000000#XNYS"
        );

        // (2) Query one microsecond before open -> no overlap (binary search upper bound)
        assertTickInterval(
                "[]",
                "2025-01-24T14:29:59.999999#XNYS"
        );

        // (3) Query at last trading microsecond (close - 1) -> single-microsecond overlap
        assertTickInterval(
                "[{lo=2025-01-24T20:59:59.999999Z, hi=2025-01-24T20:59:59.999999Z}]",
                "2025-01-24T20:59:59.999999#XNYS"
        );

        // (4) Query at exact close timestamp -> no overlap (close is exclusive)
        assertTickInterval(
                "[]",
                "2025-01-24T21:00:00.000000#XNYS"
        );

        // (5) Query in weekend gap between Friday close and Monday open -> empty
        assertTickInterval(
                "[]",
                "2025-01-25T14:30:00.000000#XNYS"
        );

        // (6) Query one microsecond after close -> no overlap (binary search lower bound)
        assertTickInterval(
                "[]",
                "2025-01-24T21:00:00.000001#XNYS"
        );
    }

    @Test
    public void testExchangeFilterWithDuration() throws SqlException {
        // Exchange filter with duration suffix
        // 2025-01-24#XNYS;1h extends each interval by 1 hour
        // XNYS trading hours: 14:30-21:00 UTC -> with ;1h becomes 14:30-22:00
        assertTickInterval(
                "[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T21:59:59.999999Z}]",
                "2025-01-24#XNYS;1h"
        );
    }

    @Test
    public void testExchangeFilterWithNegativeDuration() throws SqlException {
        // XNYS Jan 24: 14:30-21:00 UTC (6.5h session)
        // ;-1h shrinks session by 1h from the end: 14:30-20:00
        assertTickInterval(
                "[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T19:59:59.999999Z}]",
                "2025-01-24#XNYS;-1h"
        );
    }

    @Test
    public void testExchangeFilterWithNegativeDurationMultipleDays() throws SqlException {
        // XNYS ;-1h over Fri-Tue (Sat/Sun filtered out)
        assertTickInterval(
                "[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T19:59:59.999999Z}," +
                        "{lo=2025-01-27T14:30:00.000000Z, hi=2025-01-27T19:59:59.999999Z}," +
                        "{lo=2025-01-28T14:30:00.000000Z, hi=2025-01-28T19:59:59.999999Z}]",
                "2025-01-[24..28]#XNYS;-1h"
        );
    }

    @Test
    public void testExchangeFilterWithDurationMultipleDays() throws SqlException {
        // Exchange filter with duration over multiple days
        // 2025-01-[24..28]#XNYS;1h
        assertTickInterval(
                "[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T21:59:59.999999Z}," +
                        "{lo=2025-01-27T14:30:00.000000Z, hi=2025-01-27T21:59:59.999999Z}," +
                        "{lo=2025-01-28T14:30:00.000000Z, hi=2025-01-28T21:59:59.999999Z}]",
                "2025-01-[24..28]#XNYS;1h"
        );
    }

    @Test
    public void testFullMonthWithExchangeFilter() throws SqlException {
        // Full month with exchange filter
        // 2025-03#XNYS - March 2025, XNYS trading days only
        LongList out = new LongList();
        TimestampDriver driver = ColumnType.getTimestampDriver(ColumnType.TIMESTAMP);
        IntervalUtils.parseTickExpr(driver, configuration, "2025-03#XNYS", 0, 12, 0, out, sink);

        // Should have an even number of elements (pairs)
        Assert.assertEquals("Should have interval pairs", 0, out.size() % 2);

        // Should have at least 15 trading days (20-21 expected)
        Assert.assertTrue("Should have at least 15 trading day intervals", out.size() >= 30);

        // First trading day should be Mar 3 (Mon) - Mar 1 is Sat, Mar 2 is Sun
        Assert.assertEquals("First day should be Mar 3 open",
                Micros.toMicros(2025, 3, 3, 14, 30), out.getQuick(0));
        Assert.assertEquals("First day should be Mar 3 close",
                Micros.toMicros(2025, 3, 3, 21, 0) - 1, out.getQuick(1));

        // Last trading day should be Mar 31 (Mon) - DST is in effect (started Mar 9)
        // NYSE hours during DST: 13:30-20:00 UTC
        int lastIdx = out.size() - 2;
        Assert.assertEquals("Last day should be Mar 31 open",
                Micros.toMicros(2025, 3, 31, 13, 30), out.getQuick(lastIdx));
        Assert.assertEquals("Last day should be Mar 31 close",
                Micros.toMicros(2025, 3, 31, 20, 0) - 1, out.getQuick(lastIdx + 1));
    }

    @Test
    public void testFullMonthWithExchangeFilterHongKong() throws SqlException {
        // Full month with XHKG (has lunch breaks, so 2 intervals per day)
        // 2025-03#XHKG
        LongList out = new LongList();
        TimestampDriver driver = ColumnType.getTimestampDriver(ColumnType.TIMESTAMP);
        IntervalUtils.parseTickExpr(driver, configuration, "2025-03#XHKG", 0, 12, 0, out, sink);

        // Each trading day produces 2 intervals (morning + afternoon due to lunch break)
        Assert.assertEquals("Should have interval pairs", 0, out.size() % 2);

        // Should have at least 30 intervals (15+ trading days * 2 sessions)
        Assert.assertTrue("Should have at least 30 intervals for XHKG", out.size() >= 60);

        // First session should be Mar 3 morning (01:30-04:00 UTC) - Mar 1 is Sat, Mar 2 is Sun
        Assert.assertEquals("First session should be Mar 3 morning open",
                Micros.toMicros(2025, 3, 3, 1, 30), out.getQuick(0));
        Assert.assertEquals("First session should be Mar 3 morning close",
                Micros.toMicros(2025, 3, 3, 4, 0) - 1, out.getQuick(1));

        // Second interval should be Mar 3 afternoon (05:00-08:00 UTC)
        Assert.assertEquals("Second session should be Mar 3 afternoon open",
                Micros.toMicros(2025, 3, 3, 5, 0), out.getQuick(2));
        Assert.assertEquals("Second session should be Mar 3 afternoon close",
                Micros.toMicros(2025, 3, 3, 8, 0) - 1, out.getQuick(3));
    }

    @Test
    public void testFullYearWithExchangeFilter() throws SqlException {
        // Full year with exchange filter
        // 2025#XNYS - all trading days in 2025
        LongList out = new LongList();
        TimestampDriver driver = ColumnType.getTimestampDriver(ColumnType.TIMESTAMP);
        IntervalUtils.parseTickExpr(driver, configuration, "2025#XNYS", 0, 9, 0, out, sink);

        // Should have interval pairs
        Assert.assertEquals("Should have interval pairs", 0, out.size() % 2);

        // NYSE has ~252 trading days per year
        int tradingDays = out.size() / 2;
        Assert.assertTrue("Should have at least 200 trading days", tradingDays >= 200);
        Assert.assertTrue("Should have at most 260 trading days", tradingDays <= 260);

        // First trading day should be Jan 2 (Thu) - Jan 1 is holiday
        long firstLo = out.getQuick(0);
        Assert.assertTrue("First trading day should be in early January 2025",
                firstLo >= Micros.toMicros(2025, 1, 1, 0, 0) &&
                        firstLo <= Micros.toMicros(2025, 1, 5, 0, 0));

        // Last trading day should be Dec 31 (Wed) or Dec 26 depending on holidays
        int lastIdx = out.size() - 2;
        long lastLo = out.getQuick(lastIdx);
        Assert.assertTrue("Last trading day should be in late December 2025",
                lastLo >= Micros.toMicros(2025, 12, 25, 0, 0) &&
                        lastLo <= Micros.toMicros(2025, 12, 31, 23, 59));
    }

    @Test
    public void testHongKongCaseInsensitive() throws SqlException {
        assertTickInterval(
                "[{lo=2025-01-24T01:30:00.000000Z, hi=2025-01-24T03:59:59.999999Z}," +
                        "{lo=2025-01-24T05:00:00.000000Z, hi=2025-01-24T07:59:59.999999Z}]",
                "2025-01-24#xhkg"
        );
    }

    @Test
    public void testHongKongChristmas() throws SqlException {
        // XHKG around Christmas 2025
        // Dec 23 (Tue) full trading, Dec 24 (Wed) morning only (Christmas Eve early close),
        // Dec 25 (Thu) Christmas, Dec 26 (Fri) Boxing Day, Dec 27-28 (weekend), Dec 29 (Mon) full
        assertTickInterval(
                "[{lo=2025-12-23T01:30:00.000000Z, hi=2025-12-23T03:59:59.999999Z}," +
                        "{lo=2025-12-23T05:00:00.000000Z, hi=2025-12-23T07:59:59.999999Z}," +
                        "{lo=2025-12-24T01:30:00.000000Z, hi=2025-12-24T03:59:59.999999Z}," +
                        "{lo=2025-12-29T01:30:00.000000Z, hi=2025-12-29T03:59:59.999999Z}," +
                        "{lo=2025-12-29T05:00:00.000000Z, hi=2025-12-29T07:59:59.999999Z}]",
                "2025-12-[23..29]#XHKG"
        );
    }

    @Test
    public void testHongKongDateRange() throws SqlException {
        // Date range with XHKG spanning Ching Ming Festival (Apr 4) and weekend
        // 2025-04-[03..07] - Thu to Mon; Apr 4 (Ching Ming) and Apr 5-6 (weekend) excluded
        assertTickInterval(
                "[{lo=2025-04-03T01:30:00.000000Z, hi=2025-04-03T03:59:59.999999Z}," +
                        "{lo=2025-04-03T05:00:00.000000Z, hi=2025-04-03T07:59:59.999999Z}," +
                        "{lo=2025-04-07T01:30:00.000000Z, hi=2025-04-07T03:59:59.999999Z}," +
                        "{lo=2025-04-07T05:00:00.000000Z, hi=2025-04-07T07:59:59.999999Z}]",
                "2025-04-[03..07]#XHKG"
        );
    }

    @Test
    public void testHongKongIsoWeek() throws SqlException {
        // ISO week with XHKG (has lunch break)
        // 2025-W06: Mon Feb 3 to Sun Feb 9
        // XHKG trading days with two sessions each (Mon-Fri)
        assertTickInterval(
                "[{lo=2025-02-03T01:30:00.000000Z, hi=2025-02-03T03:59:59.999999Z}," +
                        "{lo=2025-02-03T05:00:00.000000Z, hi=2025-02-03T07:59:59.999999Z}," +
                        "{lo=2025-02-04T01:30:00.000000Z, hi=2025-02-04T03:59:59.999999Z}," +
                        "{lo=2025-02-04T05:00:00.000000Z, hi=2025-02-04T07:59:59.999999Z}," +
                        "{lo=2025-02-05T01:30:00.000000Z, hi=2025-02-05T03:59:59.999999Z}," +
                        "{lo=2025-02-05T05:00:00.000000Z, hi=2025-02-05T07:59:59.999999Z}," +
                        "{lo=2025-02-06T01:30:00.000000Z, hi=2025-02-06T03:59:59.999999Z}," +
                        "{lo=2025-02-06T05:00:00.000000Z, hi=2025-02-06T07:59:59.999999Z}," +
                        "{lo=2025-02-07T01:30:00.000000Z, hi=2025-02-07T03:59:59.999999Z}," +
                        "{lo=2025-02-07T05:00:00.000000Z, hi=2025-02-07T07:59:59.999999Z}]",
                "2025-W06#XHKG"
        );
    }

    @Test
    public void testHongKongLunarNewYear() throws SqlException {
        // XHKG around Lunar New Year 2025
        // Jan 27 (Mon) full trading, Jan 28 (Tue) morning only (LNY Eve early close),
        // Jan 29-31 (Wed-Fri) Lunar New Year holidays
        assertTickInterval(
                "[{lo=2025-01-27T01:30:00.000000Z, hi=2025-01-27T03:59:59.999999Z}," +
                        "{lo=2025-01-27T05:00:00.000000Z, hi=2025-01-27T07:59:59.999999Z}," +
                        "{lo=2025-01-28T01:30:00.000000Z, hi=2025-01-28T03:59:59.999999Z}]",
                "2025-01-[27..31]#XHKG"
        );
    }

    @Test
    public void testHongKongMultipleDays() throws SqlException {
        // Two trading days with lunch breaks - Fri and Mon
        assertTickInterval(
                "[{lo=2025-02-07T01:30:00.000000Z, hi=2025-02-07T03:59:59.999999Z}," +
                        "{lo=2025-02-07T05:00:00.000000Z, hi=2025-02-07T07:59:59.999999Z}," +
                        "{lo=2025-02-10T01:30:00.000000Z, hi=2025-02-10T03:59:59.999999Z}," +
                        "{lo=2025-02-10T05:00:00.000000Z, hi=2025-02-10T07:59:59.999999Z}]",
                "[2025-02-07,2025-02-10]#XHKG"
        );
    }

    @Test
    public void testHongKongSingleDay() throws SqlException {
        // XHKG has lunch break: 01:30-04:00 UTC and 05:00-08:00 UTC
        // 2025-10-06 (Mon) is a trading day (the day before Chung Yeung Festival)
        assertTickInterval(
                "[{lo=2025-10-06T01:30:00.000000Z, hi=2025-10-06T03:59:59.999999Z}," +
                        "{lo=2025-10-06T05:00:00.000000Z, hi=2025-10-06T07:59:59.999999Z}]",
                "2025-10-06#XHKG"
        );
    }

    @Test
    public void testHongKongTimeSuffixAtAfternoonOpen() throws SqlException {
        // Time suffix at afternoon session open
        // 2025-01-24T05:00#XHKG - XHKG afternoon opens at 05:00
        assertTickInterval(
                "[{lo=2025-01-24T05:00:00.000000Z, hi=2025-01-24T05:00:59.999999Z}]",
                "2025-01-24T05:00#XHKG"
        );
    }

    @Test
    public void testHongKongTimeSuffixAtLunchBreakStart() throws SqlException {
        // Time suffix at lunch break start (exclusive)
        // 2025-01-24T04:00#XHKG - XHKG morning ends at 04:00 (exclusive)
        assertTickInterval(
                "[]",
                "2025-01-24T04:00#XHKG"
        );
    }

    @Test
    public void testHongKongTimeSuffixBeforeLunchBreak() throws SqlException {
        // Time suffix just before lunch break
        // 2025-01-24T03:59#XHKG - just before XHKG morning session ends
        assertTickInterval(
                "[{lo=2025-01-24T03:59:00.000000Z, hi=2025-01-24T03:59:59.999999Z}]",
                "2025-01-24T03:59#XHKG"
        );
    }

    @Test
    public void testHongKongWeekend() throws SqlException {
        // 2025-01-25 (Sat) and 2025-01-26 (Sun) are weekends - no trading
        assertTickInterval(
                "[]",
                "[2025-01-25,2025-01-26]#XHKG"
        );
    }

    @Test
    public void testHongKongWithDuration() throws SqlException {
        // XHKG (with lunch break) with duration
        // 2025-01-24#XHKG;1h - extends each session by 1 hour
        assertTickInterval(
                "[{lo=2025-01-24T01:30:00.000000Z, hi=2025-01-24T04:59:59.999999Z}," +
                        "{lo=2025-01-24T05:00:00.000000Z, hi=2025-01-24T08:59:59.999999Z}]",
                "2025-01-24#XHKG;1h"
        );
    }

    @Test
    public void testHongKongWithDurationLongerThanLunchBreak() throws SqlException {
        // 2025-01-24#XHKG;2h - the duration stretches the morning session (01:30-04:00 UTC)
        // past the afternoon open (05:00 UTC), so both sessions merge into one interval
        assertTickInterval(
                "[{lo=2025-01-24T01:30:00.000000Z, hi=2025-01-24T09:59:59.999999Z}]",
                "2025-01-24#XHKG;2h"
        );
    }

    @Test
    public void testLargeTimezoneOffsetWithExchangeFilter() throws SqlException {
        // Large positive timezone offset (+14:00)
        // [2025-01-24@+14:00]#XNYS
        // 2025-01-24 in +14:00 = [2025-01-23T10:00:00Z, 2025-01-24T09:59:59.999999Z]
        // XNYS Jan 23 (Thu): 14:30-21:00 UTC -> intersection: [14:30, 20:59:59.999999]
        // XNYS Jan 24 (Fri): 14:30-21:00 UTC -> no intersection (query ends at 09:59:59.999999)
        assertTickInterval(
                "[{lo=2025-01-23T14:30:00.000000Z, hi=2025-01-23T20:59:59.999999Z}]",
                "[2025-01-24@+14:00]#XNYS"
        );
    }

    @Test
    public void testNanosecondTimestampBoundaries() throws SqlException {
        // Tests nanosecond-precision boundaries exercising the nanos-to-micros rounding
        // in applyTickCalendarFilter's binary search.
        // XNYS Jan 24 2025 (Fri) schedule in micros: open=14:30:00.000000, close=21:00:00.000000
        // After nanos conversion: open=14:30:00.000000000, last_trading_ns=20:59:59.999999999

        // (1) Full trading day with nanos driver - schedule hi is 20:59:59.999999999 (not .999999)
        assertTickIntervalNanos(
                "[{lo=2025-01-24T14:30:00.000000000Z, hi=2025-01-24T20:59:59.999999999Z}]",
                "2025-01-24#XNYS"
        );

        // (2) Point at exact open (nanos-aligned) -> single-nanosecond overlap
        assertTickIntervalNanos(
                "[{lo=2025-01-24T14:30:00.000000000Z, hi=2025-01-24T14:30:00.000000000Z}]",
                "2025-01-24T14:30:00.000000000#XNYS"
        );

        // (3) Point 1ns before open -> no overlap
        // queryHi = 14:29:59.999999999ns, ceil to micros = 14:30:00.000000 (included in binary search)
        // but intersectInPlace correctly finds no overlap since query < schedule open
        assertTickIntervalNanos(
                "[]",
                "2025-01-24T14:29:59.999999999#XNYS"
        );

        // (4) Point 1ns after open -> overlap
        assertTickIntervalNanos(
                "[{lo=2025-01-24T14:30:00.000000001Z, hi=2025-01-24T14:30:00.000000001Z}]",
                "2025-01-24T14:30:00.000000001#XNYS"
        );

        // (5) Point at last trading nanosecond -> single-nanosecond overlap
        // queryLo = 20:59:59.999999999ns, floor to micros = 20:59:59.999999 (within schedule)
        assertTickIntervalNanos(
                "[{lo=2025-01-24T20:59:59.999999999Z, hi=2025-01-24T20:59:59.999999999Z}]",
                "2025-01-24T20:59:59.999999999#XNYS"
        );

        // (6) Point at exact close (nanos-aligned) -> no overlap (close is exclusive)
        // queryLo = 21:00:00.000000000ns > schedule hi 20:59:59.999999999ns
        assertTickIntervalNanos(
                "[]",
                "2025-01-24T21:00:00.000000000#XNYS"
        );

        // (7) Point 1ns after close -> no overlap
        // queryLo floors to 21:00:00.000000 micros; binary search still includes schedule interval
        // (since schedule hi 21:00:00.000000 >= queryLoMicros 21:00:00.000000)
        // but intersectInPlace finds no overlap
        assertTickIntervalNanos(
                "[]",
                "2025-01-24T21:00:00.000000001#XNYS"
        );

        // (8) 6-digit fractional second with nanos driver: .999999 expands to [.999999000, .999999999]
        // Just before open: entire 1us range is before 14:30:00.000000000 -> no overlap
        assertTickIntervalNanos(
                "[]",
                "2025-01-24T14:29:59.999999#XNYS"
        );

        // (9) Sub-microsecond within last trading microsecond -> overlap with 999ns range
        assertTickIntervalNanos(
                "[{lo=2025-01-24T20:59:59.999999000Z, hi=2025-01-24T20:59:59.999999999Z}]",
                "2025-01-24T20:59:59.999999#XNYS"
        );

        // (10) 6-digit fractional at close: .000000 expands to [.000000000, .000000999]
        // All 1000 nanos are at or after close -> no overlap
        assertTickIntervalNanos(
                "[]",
                "2025-01-24T21:00:00.000000#XNYS"
        );
    }

    @Test
    public void testNestedTimeListsCountBeforeCalendar() throws SqlException {
        // The outer time list values merge whenever they have added 256 intervals since the last
        // merge, and they count the merged intervals against the cap (1,024) before the calendar
        // runs after the whole expansion, as the merges of numeric brackets do. Each outer value
        // adds 50 one-second intervals that never touch, so 24 hours hold 1,200 at the last merge
        // and fail, although the session of 2025-01-02, 14:30 to 21:00 UTC, keeps 300 of them.
        final long nowTimestamp = Micros.toMicros(2025, 1, 2, 12, 0);
        final String overCapError = "Bracket expansion produces too many intervals (max 1024)";
        final StringBuilder hours20 = new StringBuilder();
        final StringBuilder hours24 = new StringBuilder();
        for (int hour = 0; hour < 24; hour++) {
            if (hour < 20) {
                hours20.append(hour == 0 ? "" : ",").append(String.format("%02d:", hour));
            }
            hours24.append(hour == 0 ? "" : ",").append(String.format("%02d:", hour));
        }
        // the even seconds from 00:00 to 01:38, and second 00 of the minutes 00 to 49
        final StringBuilder evenSeconds = new StringBuilder();
        final StringBuilder minutes = new StringBuilder();
        for (int k = 0; k < 50; k++) {
            evenSeconds.append(k == 0 ? "" : ",").append(String.format("%02d:%02d", 2 * k / 60, 2 * k % 60));
            minutes.append(k == 0 ? "" : ",").append(String.format("%02d:00", k));
        }
        assertTickIntervalErrorWithNow("2025-01-02T[" + hours24 + "][" + evenSeconds + "]#XNYS", overCapError, nowTimestamp);
        // 20 hours hold 1,000 intervals, and the session keeps the 250 of hours 15 to 19: hour 14's
        // seconds end at 14:01:38, before the open
        final LongList evenSecondsInSession = new LongList();
        for (int hour = 15; hour < 20; hour++) {
            for (int k = 0; k < 50; k++) {
                final long lo = Micros.toMicros(2025, 1, 2, hour, 0) + 2 * k * Micros.SECOND_MICROS;
                evenSecondsInSession.add(lo, lo + Micros.SECOND_MICROS - 1);
            }
        }
        final TimestampDriver driver = ColumnType.getTimestampDriver(ColumnType.TIMESTAMP);
        assertTickIntervalWithNow(intervalToString(driver, evenSecondsInSession), "2025-01-02T[" + hours20 + "][" + evenSeconds + "]#XNYS", nowTimestamp);
        // The same intervals as nested time lists and as numeric brackets: second 00 of the minutes
        // 00 to 49 of each hour. With 20 hours the session keeps 14:30 to 14:49 and hours 15 to 19.
        assertTickIntervalErrorWithNow("2025-01-02T[" + hours24 + "][" + minutes + "]#XNYS", overCapError, nowTimestamp);
        assertTickIntervalErrorWithNow("2025-01-02T[00..23]:[00..49]:00#XNYS", overCapError, nowTimestamp);
        final LongList minutesInSession = new LongList();
        for (int hour = 14; hour < 20; hour++) {
            for (int minute = hour == 14 ? 30 : 0; minute < 50; minute++) {
                final long lo = Micros.toMicros(2025, 1, 2, hour, minute);
                minutesInSession.add(lo, lo + Micros.SECOND_MICROS - 1);
            }
        }
        Assert.assertEquals(2 * 270, minutesInSession.size());
        final String expected = intervalToString(driver, minutesInSession);
        assertTickIntervalWithNow(expected, "2025-01-02T[" + hours20 + "][" + minutes + "]#XNYS", nowTimestamp);
        assertTickIntervalWithNow(expected, "2025-01-02T[00..19]:[00..49]:00#XNYS", nowTimestamp);
    }

    @Test
    public void testNonTradingDayWithDuration() throws SqlException {
        // Non-trading day with duration - still empty
        // 2025-01-25#XNYS;1h - Saturday is not a trading day
        assertTickInterval(
                "[]",
                "2025-01-25#XNYS;1h"
        );
    }

    @Test
    public void testNyseDateList() throws SqlException {
        // Explicit date list with tick calendar - Mon and Wed
        assertTickInterval(
                "[{lo=2025-01-27T14:30:00.000000Z, hi=2025-01-27T20:59:59.999999Z}," +
                        "{lo=2025-01-29T14:30:00.000000Z, hi=2025-01-29T20:59:59.999999Z}]",
                "[2025-01-27,2025-01-29]#XNYS"
        );
    }

    @Test
    public void testNyseDateListWithNonTradingDay() throws SqlException {
        // Date list where one date is a non-trading day (Saturday)
        // Only the trading day should appear in output
        assertTickInterval(
                "[{lo=2025-01-27T14:30:00.000000Z, hi=2025-01-27T20:59:59.999999Z}]",
                "[2025-01-25,2025-01-27]#XNYS"
        );
    }

    @Test
    public void testNyseGoodFriday() throws SqlException {
        // NYSE week ending on Good Friday (Apr 18), DST active: 13:30-20:00 UTC
        // Apr 14 (Mon) through Apr 18 (Fri); Apr 18 is Good Friday, closed
        assertTickInterval(
                "[{lo=2025-04-14T13:30:00.000000Z, hi=2025-04-14T19:59:59.999999Z}," +
                        "{lo=2025-04-15T13:30:00.000000Z, hi=2025-04-15T19:59:59.999999Z}," +
                        "{lo=2025-04-16T13:30:00.000000Z, hi=2025-04-16T19:59:59.999999Z}," +
                        "{lo=2025-04-17T13:30:00.000000Z, hi=2025-04-17T19:59:59.999999Z}]",
                "2025-04-[14..18]#XNYS"
        );
    }

    @Test
    public void testNyseIndependenceDayEarlyClose() throws SqlException {
        // NYSE around Independence Day 2025, DST active: normal hours 13:30-20:00 UTC
        // Jul 2 (Wed) normal, Jul 3 (Thu) early close at 17:00 UTC (1:00 PM ET),
        // Jul 4 (Fri) Independence Day, Jul 5-6 (weekend), Jul 7 (Mon) normal
        assertTickInterval(
                "[{lo=2025-07-02T13:30:00.000000Z, hi=2025-07-02T19:59:59.999999Z}," +
                        "{lo=2025-07-03T13:30:00.000000Z, hi=2025-07-03T16:59:59.999999Z}," +
                        "{lo=2025-07-07T13:30:00.000000Z, hi=2025-07-07T19:59:59.999999Z}]",
                "2025-07-[02..07]#XNYS"
        );
    }

    @Test
    public void testNyseIsoWeek() throws SqlException {
        // ISO week 2025-W05 is Mon Jan 27 to Sun Feb 2
        // NYSE trading days in that week: Mon, Tue, Wed, Thu, Fri (Jan 27-31)
        assertTickInterval(
                "[{lo=2025-01-27T14:30:00.000000Z, hi=2025-01-27T20:59:59.999999Z}," +
                        "{lo=2025-01-28T14:30:00.000000Z, hi=2025-01-28T20:59:59.999999Z}," +
                        "{lo=2025-01-29T14:30:00.000000Z, hi=2025-01-29T20:59:59.999999Z}," +
                        "{lo=2025-01-30T14:30:00.000000Z, hi=2025-01-30T20:59:59.999999Z}," +
                        "{lo=2025-01-31T14:30:00.000000Z, hi=2025-01-31T20:59:59.999999Z}]",
                "2025-W05#XNYS"
        );
    }

    @Test
    public void testNyseMonthPartial() throws SqlException {
        // Test a few days within a month - verifies weekends are excluded
        // 2025-01-29 (Wed) through 2025-02-02 (Sun)
        // Trading days: Wed, Thu, Fri (Jan 29-31)
        assertTickInterval(
                "[{lo=2025-01-29T14:30:00.000000Z, hi=2025-01-29T20:59:59.999999Z}," +
                        "{lo=2025-01-30T14:30:00.000000Z, hi=2025-01-30T20:59:59.999999Z}," +
                        "{lo=2025-01-31T14:30:00.000000Z, hi=2025-01-31T20:59:59.999999Z}]",
                "2025-01-[29..31]#XNYS"
        );
    }

    @Test
    public void testNyseSingleDay() throws SqlException {
        // XNYS (NYSE) has no lunch break: 13:30-20:00 UTC (DST active in Sep)
        // 2025-09-15 is Monday
        assertTickInterval(
                "[{lo=2025-09-15T13:30:00.000000Z, hi=2025-09-15T19:59:59.999999Z}]",
                "2025-09-15#XNYS"
        );
    }

    @Test
    public void testNyseThanksgivingEarlyClose() throws SqlException {
        // NYSE around Thanksgiving 2025, post-DST: normal hours 14:30-21:00 UTC
        // Nov 26 (Wed) normal, Nov 27 (Thu) Thanksgiving (closed),
        // Nov 28 (Fri) Black Friday early close at 18:00 UTC (1:00 PM ET)
        assertTickInterval(
                "[{lo=2025-11-26T14:30:00.000000Z, hi=2025-11-26T20:59:59.999999Z}," +
                        "{lo=2025-11-28T14:30:00.000000Z, hi=2025-11-28T17:59:59.999999Z}]",
                "2025-11-[26..28]#XNYS"
        );
    }

    @Test
    public void testNyseWithDateRange() throws SqlException {
        // Date range with tick calendar
        // 2025-01-[24..28] spans Fri, Sat, Sun, Mon, Tue - trading days are Fri, Mon, Tue
        assertTickInterval(
                "[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T20:59:59.999999Z}," +
                        "{lo=2025-01-27T14:30:00.000000Z, hi=2025-01-27T20:59:59.999999Z}," +
                        "{lo=2025-01-28T14:30:00.000000Z, hi=2025-01-28T20:59:59.999999Z}]",
                "2025-01-[24..28]#XNYS"
        );
    }

    @Test
    public void testPerElementExchangeFilterWithDuration() throws SqlException {
        // Per-element exchange filter with global duration
        // [2025-01-24#XNYS,2025-02-03#XHKG];1h
        assertTickInterval(
                "[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T21:59:59.999999Z}," +
                        "{lo=2025-02-03T01:30:00.000000Z, hi=2025-02-03T08:59:59.999999Z}]",
                "[2025-01-24#XNYS,2025-02-03#XHKG];1h"
        );
    }

    @Test
    public void testPerElementTimezoneWithGlobalTimezoneAndExchangeFilter() throws SqlException {
        // Per-element timezone takes precedence over global timezone
        // [2025-01-24@-05:00,2025-01-27]@+08:00#XNYS
        // Jan 24 uses -05:00: [05:00 UTC, next day 04:59:59.999999 UTC]
        // Jan 27 uses global +08:00: [2025-01-26T16:00 UTC, 2025-01-27T15:59:59.999999 UTC]
        // XNYS hours: 14:30-21:00 UTC
        // Jan 24: intersection [14:30, 20:59:59.999999]
        // Jan 26 (Sun) no trading, Jan 27: intersection [14:30, 15:59:59.999999]
        assertTickInterval(
                "[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T20:59:59.999999Z}," +
                        "{lo=2025-01-27T14:30:00.000000Z, hi=2025-01-27T15:59:59.999999Z}]",
                "[2025-01-24@-05:00,2025-01-27]@+08:00#XNYS"
        );
    }

    @Test
    public void testPointInTimeRangeElementCalendar() throws SqlException {
        // A range between two points in time keeps the parts of its interval in the sessions of the
        // tick calendar of its element, and ends each later by the duration of the list, as the tick
        // calendar of the list does, at every check level. It ignores the time part of the list. On
        // Friday 2025-01-24 the XNYS session runs from 14:30 to 21:00Z, and the range from 13:30 to
        // 15:30Z.
        final String[][] durations = {
                // the duration, and the interval that it makes of the part of the range in the
                // session, by hand, or null where the list form gives it
                {"", "[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T15:30:00.000000Z}]"},
                {";1h", "[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T16:30:00.000000Z}]"},
                {";30m", "[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T16:00:00.000000Z}]"},
                {";0s", "[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T15:30:00.000000Z}]"},
                {";-30m", "[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T15:00:00.000000Z}]"},
                {";1d", "[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-25T15:30:00.000000Z}]"},
                {";1w", "[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-31T15:30:00.000000Z}]"},
                {";1M", "[{lo=2025-01-24T14:30:00.000000Z, hi=2025-02-24T15:30:00.000000Z}]"},
                {";1y", "[{lo=2025-01-24T14:30:00.000000Z, hi=2026-01-24T15:30:00.000000Z}]"},
                // an end before the start
                {";-2h", null}
        };
        for (int timestampType : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
            final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
            final String context = " [" + ColumnType.nameOf(timestampType) + ']';
            final long friday = driver.parseFloorLiteral("2025-01-24T15:30:00.000000Z");
            for (String[] duration : durations) {
                final String list = "[$now - 2h..$now]#XNYS" + duration[0];
                final String expected = parseTickExpr(driver, list, friday);
                if (duration[1] != null) {
                    Assert.assertEquals(list + context, forTimestampType(duration[1], timestampType), expected);
                }
                Assert.assertEquals(list + context, expected, evaluateCompiledTickExpr(driver, list, friday));
                assertParsedAtEveryCheckLevel(driver, "[$now - 2h..$now#XNYS]" + duration[0], friday, expected);
                assertCompiledParsesOnEvaluation(driver, "[$now - 2h..$now#XNYS]" + duration[0], friday, expected);
            }
            // a Saturday, and the Friday before the session opens
            assertParsedAtEveryCheckLevel(driver, "[$now - 2h..$now#XNYS]", driver.parseFloorLiteral("2025-01-25T12:00:00.000000Z"), "[]");
            assertParsedAtEveryCheckLevel(driver, "[$now - 2h..$now#XNYS]", driver.parseFloorLiteral("2025-01-24T03:00:00.000000Z"), "[]");
            // 11:30 to 15:30 at -01:00 is 12:30 to 16:30Z
            final String atMinusOne = forTimestampType("[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T16:30:00.000000Z}]", timestampType);
            Assert.assertEquals(context, atMinusOne, parseTickExpr(driver, "[$now - 4h..$now]@UTC-01#XNYS", friday));
            assertParsedAtEveryCheckLevel(driver, "[$now - 4h..$now@UTC-01#XNYS]", friday, atMinusOne);
            // XHKG trades from 01:30 to 04:00Z and from 05:00 to 08:00Z on Monday 2025-01-27, and the
            // range runs from 02:30 to 04:30Z
            final long monday = driver.parseFloorLiteral("2025-01-27T04:30:00.000000Z");
            final String morning = forTimestampType("[{lo=2025-01-27T02:30:00.000000Z, hi=2025-01-27T03:59:59.999999Z}]", timestampType);
            Assert.assertEquals(context, morning, parseTickExpr(driver, "[$now - 2h..$now]#XHKG", monday));
            assertParsedAtEveryCheckLevel(driver, "[$now - 2h..$now#XHKG]", monday, morning);
            final String morningAndHour = forTimestampType("[{lo=2025-01-27T02:30:00.000000Z, hi=2025-01-27T04:59:59.999999Z}]", timestampType);
            Assert.assertEquals(context, morningAndHour, parseTickExpr(driver, "[$now - 2h..$now]#XHKG;1h", monday));
            assertParsedAtEveryCheckLevel(driver, "[$now - 2h..$now#XHKG];1h", monday, morningAndHour);
            // the time part of the list
            final String withHour = forTimestampType("[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T16:30:00.000000Z}]", timestampType);
            Assert.assertEquals(context, withHour, parseTickExpr(driver, "[$now - 2h..$now]T09:00#XNYS;1h", friday));
            assertParsedAtEveryCheckLevel(driver, "[$now - 2h..$now#XNYS]T09:00;1h", friday, withHour);
            assertCompiledParsesOnEvaluation(driver, "[$now - 2h..$now#XNYS]T09:00;1h", friday, withHour);
        }
    }

    @Test
    public void testPointInTimeRangeElementCalendarOverIntervalLimit() throws SqlException {
        // A range between two points in time under the tick calendar of its element becomes an
        // interval for each session, and the interval limit counts them, as it counts those of the
        // tick calendar of the list. From 2025-01-01T12:00 to 2025-01-31T12:00Z, XNYS has the 19
        // sessions from 01-02 to 01-30: the weekdays but the closures on 01-09 and 01-20, each from
        // 14:30 to 21:00Z.
        final CairoConfiguration smallLimits = calendarConfiguration(8, 16);
        final String limitError = "error: Bracket expansion produces too many intervals (max 16)";
        final String element = "[$now - 30d..$now#XNYS]";
        for (int timestampType : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
            final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
            final String context = " [" + ColumnType.nameOf(timestampType) + ']';
            final LongList expected = new LongList();
            for (LocalDate day = LocalDate.of(2025, 1, 2); !day.isAfter(LocalDate.of(2025, 1, 30)); day = day.plusDays(1)) {
                if (day.getDayOfWeek() != DayOfWeek.SATURDAY && day.getDayOfWeek() != DayOfWeek.SUNDAY && day.getDayOfMonth() != 9 && day.getDayOfMonth() != 20) {
                    final long dayStart = driver.fromDays((int) day.toEpochDay());
                    expected.add(dayStart + driver.fromMinutes(14 * 60 + 30), dayStart + driver.fromHours(21) - 1);
                }
            }
            Assert.assertEquals(context, 2 * 19, expected.size());
            final String sessions = intervalToString(driver, expected);
            final long now = driver.parseFloorLiteral("2025-01-31T12:00:00.000000Z");
            for (int checks : new int[]{IntervalUtils.TICK_CHECK_ALL, IntervalUtils.TICK_CHECK_INTERVAL_LIMIT}) {
                Assert.assertEquals(context, limitError, parseTickExpr(smallLimits, driver, "[$now - 30d..$now]#XNYS", now, checks));
                Assert.assertEquals(context, limitError, parseTickExpr(smallLimits, driver, element, now, checks));
            }
            Assert.assertEquals(context, sessions, parseTickExpr(smallLimits, driver, "[$now - 30d..$now]#XNYS", now, 0));
            Assert.assertEquals(context, sessions, parseTickExpr(smallLimits, driver, element, now, 0));
            try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(driver, smallLimits, element, 0, element.length(), 7)) {
                Assert.assertTrue(context, compiled.isParsedOnEvaluation());
                try {
                    compiled.validate(now);
                    Assert.fail("validate should fail: " + element + context);
                } catch (SqlException e) {
                    Assert.assertEquals(context, limitError, "error: " + e.getFlyweightMessage());
                    Assert.assertEquals(context, 7, e.getPosition());
                }
            }
            // the default limits
            Assert.assertEquals(context, sessions, parseTickExpr(driver, element, now));
            // a range of days after the range: the sessions do not merge into one interval of the
            // range, so the check of each day counts them, as for a range of days under the tick
            // calendar of its element
            final String rangeOfDays = "[$today - 30d..$today#XNYS, $today - 30d..$today]";
            Assert.assertEquals(context, "error: Date range expansion produces too many intervals (max 16)", parseTickExpr(smallLimits, driver, rangeOfDays, now, IntervalUtils.TICK_CHECK_ALL));
            Assert.assertEquals(
                    context,
                    parseTickExpr(smallLimits, driver, rangeOfDays, now, IntervalUtils.TICK_CHECK_ALL),
                    parseTickExpr(smallLimits, driver, "[$now - 30d..$now#XNYS, $today - 30d..$today]", now, IntervalUtils.TICK_CHECK_ALL)
            );
        }
    }

    @Test
    public void testPointInTimeRangeElementCalendarWithListFilters() throws SqlException {
        // The tick calendar of an element replaces the day filter of the list, as it does on a day
        // of a range of days; a day filter of the element runs before the tick calendar of the list;
        // and the tick calendar of the list runs after the one of the element, on what that keeps.
        // On Friday 2025-01-24, XNYS trades from 14:30 to 21:00Z, XHKG from 01:30 to 04:00Z and from
        // 05:00 to 08:00Z, and the range runs from 13:30 to 15:30Z.
        for (int timestampType : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
            final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
            final long friday = driver.parseFloorLiteral("2025-01-24T15:30:00.000000Z");
            final String kept = forTimestampType("[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T15:30:00.000000Z}]", timestampType);
            // a range of days does the same
            Assert.assertEquals(kept, parseTickExpr(driver, "[$today..$now#XNYS]#Sun", friday));
            final String[][] cases = {
                    {"[$now - 2h..$now#XNYS]#Sun", kept},
                    {"[$now - 2h..$now#Fri]#XNYS", kept},
                    {"[$now - 2h..$now#Sun]#XNYS", "[]"},
                    {"[$now - 2h..$now#XNYS]#XHKG", "[]"}
            };
            for (String[] c : cases) {
                assertParsedAtEveryCheckLevel(driver, c[0], friday, c[1]);
                assertCompiledParsesOnEvaluation(driver, c[0], friday, c[1]);
            }
        }
    }

    @Test
    public void testPointInTimeRangeElementCalendarWithRepeatingDuration() throws SqlException {
        // A duration with a period and a count, which a tick calendar does not add, fails a range
        // between two points in time under the tick calendar of its element where the session keeps
        // part of the range, and adds no interval where it keeps none, as it does under the tick
        // calendar of the list, and as a range of days with the tick calendar of its element does.
        // The parser reads the duration only to add it to what the session keeps. The XNYS session
        // of Friday 2025-01-24 runs from 14:30 to 21:00Z.
        final String element = "[$now - 1h..$now#XNYS];1h;1d;3";
        final String error = "error: Expected number before unit ';'";
        final String[][] nows = {
                {"2025-01-24T15:00:00.000000Z", error},
                {"2025-01-24T03:00:00.000000Z", "[]"},
                {"2025-01-25T15:00:00.000000Z", "[]"}
        };
        for (int timestampType : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
            final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
            for (String[] now : nows) {
                final long timestamp = driver.parseFloorLiteral(now[0]);
                final String context = " at " + now[0] + " [" + ColumnType.nameOf(timestampType) + ']';
                Assert.assertEquals(element + context, now[1], parseTickExpr(driver, "[$now - 1h..$now]#XNYS;1h;1d;3", timestamp));
                Assert.assertEquals(element + context, now[1], parseTickExpr(driver, "[$today..$now#XNYS];1h;1d;3", timestamp));
                assertParsedAtEveryCheckLevel(driver, element, timestamp, now[1]);
            }
            // SQL validates the literal against the "now" it compiles at, and runs it at another
            try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(driver, configuration, element, 0, element.length(), 7)) {
                Assert.assertTrue(compiled.isParsedOnEvaluation());
                compiled.validate(driver.parseFloorLiteral("2025-01-24T03:00:00.000000Z"));
                try {
                    compiled.evaluate(new LongList(), driver.parseFloorLiteral("2025-01-24T15:00:00.000000Z"));
                    Assert.fail("evaluation should fail: " + element);
                } catch (CairoException e) {
                    Assert.assertEquals(error, "error: " + e.getFlyweightMessage());
                    Assert.assertEquals(7, e.getPosition());
                }
            }
        }
    }

    @Test
    public void testPointInTimeRangeSuffixUnderTickCalendar() {
        // A range between two points in time under a tick calendar: the parser checks the text after
        // the first ';' whether or not the session holds the range, and adds the duration to the end
        // of what the session keeps of it, as the compiled form does. The session of the trading
        // Friday 2025-01-24 runs from 14:30 to 21:00Z: at 15:00Z it holds the end of the range, at
        // 03:00Z nothing of it, and the Saturday has none.
        final String[] nows = {"2025-01-24T15:00:00.000000Z", "2025-01-24T03:00:00.000000Z", "2025-01-25T15:00:00.000000Z"};
        for (int timestampType : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
            final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
            final boolean isNano = ColumnType.isTimestampNano(timestampType);
            for (String nowText : nows) {
                final long now = driver.parseFloorLiteral(nowText);
                final String context = " at " + nowText + " [" + ColumnType.nameOf(timestampType) + ']';
                // the compiled form parses an expression with a unit that the drivers do not add on
                // every evaluation
                final String unknownUnit = "$now - 1h..$now#XNYS;1x";
                Assert.assertEquals(unknownUnit + context, "error: Invalid duration unit: x", parseTickExpr(driver, unknownUnit, now));
                Assert.assertEquals(unknownUnit + context, "error: Invalid duration unit: x", evaluateCompiledTickExpr(driver, unknownUnit, now));
                // one ';' more, which the parser shows from the first ';' on, and which the IR
                // rejects with an error of its own
                final String oneMore = "$now - 1h..$now#XNYS;1h;1d";
                Assert.assertEquals(oneMore + context, "error: Invalid interval format: ;1h;1d", parseTickExpr(driver, oneMore, now));
                Assert.assertEquals(oneMore + context, "compile error: Expected number before unit ';'", evaluateCompiledTickExpr(driver, oneMore, now));
                // the session keeps 14:30 to 15:00 of the range at 15:00Z, and the duration takes its
                // end to 16:00
                final String wellFormed = "$now - 1h..$now#XNYS;1h";
                final String expected = nowText.equals(nows[0])
                        ? "[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T16:00:00.000000Z}]"
                        : "[]";
                final String expectedForType = isNano ? expected.replace("00Z", "00000Z") : expected;
                Assert.assertEquals(wellFormed + context, expectedForType, parseTickExpr(driver, wellFormed, now));
                Assert.assertEquals(wellFormed + context, expectedForType, evaluateCompiledTickExpr(driver, wellFormed, now));
            }
        }
        // Not asserted: the parser reads the duration of a date list under a tick calendar only
        // where it applies it to what the session keeps, so [$now - 1h..$now]#XNYS;1x, and the
        // repeating interval $now - 1h..$now#XNYS;1h;1d;3, return no interval when the session
        // keeps nothing of the range.
    }

    @Test
    public void testSameDateDifferentTimezonesWithExchangeFilter() throws SqlException {
        // Same date with different timezones
        // [2025-01-24@+12:00,2025-01-24@-12:00]#XNYS
        // 2025-01-24 in +12:00 = [2025-01-23T12:00:00Z, 2025-01-24T11:59:59.999999Z]
        //   -> XNYS Jan 23 (Thu): 14:30-21:00 UTC intersects -> [14:30, 20:59:59.999999]
        // 2025-01-24 in -12:00 = [2025-01-24T12:00:00Z, 2025-01-25T11:59:59.999999Z]
        //   -> XNYS Jan 24 (Fri): 14:30-21:00 UTC intersects -> [14:30, 20:59:59.999999]
        assertTickInterval(
                "[{lo=2025-01-23T14:30:00.000000Z, hi=2025-01-23T20:59:59.999999Z}," +
                        "{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T20:59:59.999999Z}]",
                "[2025-01-24@+12:00,2025-01-24@-12:00]#XNYS"
        );
    }

    @Test
    public void testSingleElementWithExchangeFilterAndMultiDayDuration() throws SqlException {
        // Single element in brackets with exchange filter and multi-day duration
        // [2025-01-24#XNYS];1d - trading day extends by 1 day
        assertTickInterval(
                "[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-25T20:59:59.999999Z}]",
                "[2025-01-24#XNYS];1d"
        );
    }

    @Test
    public void testSingleElementWithExchangeFilterAndMultiDayDurationFiltersOut() throws SqlException {
        // Single element with exchange filter and duration where base day is non-trading
        // [2025-01-25#XNYS];1d - Saturday, so empty even with duration
        assertTickInterval(
                "[]",
                "[2025-01-25#XNYS];1d"
        );
    }

    @Test
    public void testSingleNonTradingDay() throws SqlException {
        // Single non-trading day (Saturday)
        // 2025-01-25#XNYS - Saturday is not a trading day
        assertTickInterval(
                "[]",
                "2025-01-25#XNYS"
        );
    }

    @Test
    public void testThreeDatesWithGlobalFilter() throws SqlException {
        // Three dates with global exchange filter - Fri, Mon, Tue
        // [2025-01-24,2025-01-27,2025-01-28]#XNYS
        assertTickInterval(
                "[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T20:59:59.999999Z}," +
                        "{lo=2025-01-27T14:30:00.000000Z, hi=2025-01-27T20:59:59.999999Z}," +
                        "{lo=2025-01-28T14:30:00.000000Z, hi=2025-01-28T20:59:59.999999Z}]",
                "[2025-01-24,2025-01-27,2025-01-28]#XNYS"
        );
    }

    @Test
    public void testThreeDatesWithGlobalFilterSomeFiltered() throws SqlException {
        // Three dates with global exchange filter where some are non-trading
        // [2025-01-24,2025-01-25,2025-01-27]#XNYS - Jan 25 is Saturday
        assertTickInterval(
                "[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T20:59:59.999999Z}," +
                        "{lo=2025-01-27T14:30:00.000000Z, hi=2025-01-27T20:59:59.999999Z}]",
                "[2025-01-24,2025-01-25,2025-01-27]#XNYS"
        );
    }

    @Test
    public void testTimeSuffixAndExchangeFilterWithDuration() throws SqlException {
        // Time suffix + exchange filter + duration
        // 2025-01-[24..28]T15:00#XNYS;1h
        // 15:00 UTC is within XNYS hours, so:
        // 1. take the minute at 15:00: [15:00, 15:01)
        // 2. extend it by an hour, getting [15:00, 16:01)
        assertTickInterval(
                "[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T16:00:59.999999Z}," +
                        "{lo=2025-01-27T15:00:00.000000Z, hi=2025-01-27T16:00:59.999999Z}," +
                        "{lo=2025-01-28T15:00:00.000000Z, hi=2025-01-28T16:00:59.999999Z}]",
                "2025-01-[24..28]T15:00#XNYS;1h"
        );
    }

    @Test
    public void testTimeSuffixAtMarketCloseBoundary() throws SqlException {
        // Time suffix at market close boundary
        // 2025-01-24T20:59#XNYS - just before XNYS closes at 21:00 UTC
        assertTickInterval(
                "[{lo=2025-01-24T20:59:00.000000Z, hi=2025-01-24T20:59:59.999999Z}]",
                "2025-01-24T20:59#XNYS"
        );
    }

    @Test
    public void testTimeSuffixAtMarketCloseExact() throws SqlException {
        // Time suffix exactly at market close (exclusive)
        // 2025-01-24T21:00#XNYS - XNYS closes at 21:00, so this minute is outside
        assertTickInterval(
                "[]",
                "2025-01-24T21:00#XNYS"
        );
    }

    @Test
    public void testTimeSuffixAtMarketOpen() throws SqlException {
        // Time suffix exactly at market open
        // 2025-01-24T14:30#XNYS - XNYS opens at 14:30 UTC
        assertTickInterval(
                "[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T14:30:59.999999Z}]",
                "2025-01-24T14:30#XNYS"
        );
    }

    @Test
    public void testTimeSuffixInsidePerElementBrackets() throws SqlException {
        // Time suffix inside per-element with exchange filter
        // [2025-01-24T15:00#XNYS] - 15:00 is within XNYS trading hours
        assertTickInterval(
                "[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T15:00:59.999999Z}]",
                "[2025-01-24T15:00#XNYS]"
        );
    }

    @Test
    public void testTimeSuffixInsidePerElementBracketsOutsideHours() throws SqlException {
        // Time suffix inside per-element, outside trading hours
        // [2025-01-24T10:00#XNYS] - 10:00 UTC is before XNYS opens (14:30)
        assertTickInterval(
                "[]",
                "[2025-01-24T10:00#XNYS]"
        );
    }

    @Test
    public void testTimeSuffixOutsideTradingHours() throws SqlException {
        // Time suffix that falls outside all trading sessions
        // 2025-02-03T04:30#XHKG - 04:30 is in lunch break (04:00-05:00)
        assertTickInterval(
                "[]",
                "2025-02-03T04:30#XHKG"
        );
    }

    @Test
    public void testTimeSuffixTimezoneExchangeFilterDuration() throws SqlException {
        // Time suffix + timezone + exchange filter + duration
        // 2025-01-[24..28]T15:00@-05:00#XNYS;1h
        // 15:00 in -05:00 = 20:00 UTC (within XNYS 14:30-21:00)
        // After exchange filter intersection: [20:00, 20:00:59.999999]
        // After ;1h duration: [20:00, 21:00:59.999999]
        assertTickInterval(
                "[{lo=2025-01-24T20:00:00.000000Z, hi=2025-01-24T21:00:59.999999Z}," +
                        "{lo=2025-01-27T20:00:00.000000Z, hi=2025-01-27T21:00:59.999999Z}," +
                        "{lo=2025-01-28T20:00:00.000000Z, hi=2025-01-28T21:00:59.999999Z}]",
                "2025-01-[24..28]T15:00@-05:00#XNYS;1h"
        );
    }

    @Test
    public void testTimezoneWithExchangeFilter() throws SqlException {
        // NYSE timezone (EST = -05:00) applied first, then exchange filter intersects
        // 2025-01-24@-05:00#XNYS
        // 2025-01-24 (Fri) in -05:00 (EST) = [2025-01-24T05:00:00Z, 2025-01-25T04:59:59.999999Z]
        // XNYS Jan 24 (Fri): 14:30-21:00 UTC -> intersection: [14:30, 20:59:59.999999]
        // Jan 25 is Saturday - no trading
        assertTickInterval(
                "[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T20:59:59.999999Z}]",
                "2025-01-24@-05:00#XNYS"
        );
    }

    @Test
    public void testTimezoneWithExchangeFilterAndDuration() throws SqlException {
        // NYSE timezone + exchange filter + duration
        // 2025-01-24@-05:00#XNYS;1h
        // 2025-01-24 in -05:00 = [2025-01-24T05:00:00Z, 2025-01-25T04:59:59.999999Z]
        // XNYS Jan 24: 14:30-21:00 UTC -> intersection: [14:30, 20:59:59.999999]
        // With ;1h duration: [14:30, 21:59:59.999999]
        assertTickInterval(
                "[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T21:59:59.999999Z}]",
                "2025-01-24@-05:00#XNYS;1h"
        );
    }

    @Test
    public void testTimezoneWithExchangeFilterWeekend() throws SqlException {
        // Weekend day in NYSE timezone
        // 2025-01-25@-05:00#XNYS - Saturday in EST
        // 2025-01-25 in -05:00 = [2025-01-25T05:00:00Z, 2025-01-26T04:59:59.999999Z]
        // Jan 25 (Sat) - no trading, Jan 26 (Sun) - no trading
        assertTickInterval(
                "[]",
                "2025-01-25@-05:00#XNYS"
        );
    }

    @Test
    public void testTimezoneWithHongKongExchange() throws SqlException {
        // Timezone with XHKG (which has lunch break)
        // 2025-02-03@+08:00#XHKG
        // 2025-02-03 (Mon) in +08:00 = [2025-02-02T16:00:00Z, 2025-02-03T15:59:59.999999Z]
        // XHKG Feb 3: 01:30-04:00, 05:00-08:00 UTC
        // Feb 2 (Sun) - no trading
        // Feb 3: intersection with [..., 15:59:59.999999] gives both sessions: [01:30-03:59:59.999999], [05:00-07:59:59.999999]
        assertTickInterval(
                "[{lo=2025-02-03T01:30:00.000000Z, hi=2025-02-03T03:59:59.999999Z}," +
                        "{lo=2025-02-03T05:00:00.000000Z, hi=2025-02-03T07:59:59.999999Z}]",
                "2025-02-03@+08:00#XHKG"
        );
    }

    @Test
    public void testUnknownDurationUnitWithTickCalendarAgreesWithBindVariable() throws SqlException {
        // A literal with a duration unit that the timestamp drivers do not add evaluates to what a
        // bind variable with the same expression parses to. On the trading days of the week, from
        // 2025-01-21 to 24, both fail with the error of the unit. On a day without a session, the
        // holiday on 2025-01-20 and the weekend, the parser skips the duration of the forms that
        // apply it to the sessions, where both return no interval: this asserts only that the two
        // agree on those days.
        for (int timestampType : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
            final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
            for (String expression : new String[]{"[$today]#XNYS;1x", "$today#XNYS;1x", "[$today]T15:00#XNYS;1x", "[$today#XNYS];1x"}) {
                try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(driver, configuration, expression, 0, expression.length(), 0)) {
                    for (int day = 20; day <= 26; day++) {
                        final String context = expression + " at 2025-01-" + day + "T12:00Z";
                        final long now = driver.parseFloorLiteral("2025-01-" + day + "T12:00:00.000000Z");
                        final LongList out = new LongList();
                        String parsed;
                        try {
                            IntervalUtils.parseTickExpr(driver, configuration, expression, 0, expression.length(), 0, out, sink, now);
                            parsed = intervalToString(driver, out);
                        } catch (SqlException e) {
                            parsed = "error: " + e.getFlyweightMessage();
                        }
                        out.clear();
                        String evaluated;
                        try {
                            compiled.evaluate(out, now);
                            evaluated = intervalToString(driver, out);
                        } catch (CairoException e) {
                            evaluated = "error: " + e.getFlyweightMessage();
                        }
                        Assert.assertEquals(context, parsed, evaluated);
                        if (day >= 21 && day <= 24) {
                            Assert.assertEquals(context, "error: Invalid duration unit: x", evaluated);
                        }
                    }
                }
            }
        }
        // A unit that the drivers add keeps the IR, which adds the duration to the end of the session
        // of the day, from 14:30 to 21:00, as the parser does
        final TimestampDriver driver = ColumnType.getTimestampDriver(ColumnType.TIMESTAMP_MICRO);
        final long now = driver.parseFloorLiteral("2025-01-24T12:00:00.000000Z");
        final String expected = "[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T21:59:59.999999Z}]";
        final String expression = "[$today]#XNYS;1h";
        assertTickIntervalWithNow(expected, expression, now);
        try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(driver, configuration, expression, 0, expression.length(), 0)) {
            final LongList out = new LongList();
            compiled.evaluate(out, now);
            Assert.assertEquals(expected, intervalToString(driver, out));
            Assert.assertFalse(compiled.isParsedOnEvaluation());
        }
    }

    @Test
    public void testWorkdayExpressionMatchesXnys() throws SqlException {
        // 09:30 America/New_York in January (EST, UTC-5) = 14:30 UTC
        // 6h30m duration from 09:30 = 16:00 ET = 21:00 UTC
        // This should produce the same intervals as the XNYS exchange calendar
        LongList out1 = new LongList();
        LongList out2 = new LongList();
        TimestampDriver driver = ColumnType.getTimestampDriver(ColumnType.TIMESTAMP);

        String expr1 = "2025-01-[02..8,10..19,21..31]T09:30@America/New_York#workday;6h30m";
        String expr2 = "2025-01-[01..31]#XNYS";

        IntervalUtils.parseTickExpr(driver, configuration, expr1, 0, expr1.length(), 0, out1, sink);
        IntervalUtils.parseTickExpr(driver, configuration, expr2, 0, expr2.length(), 0, out2, sink);

        Assert.assertEquals(intervalToString(driver, out1), intervalToString(driver, out2));
    }

    /**
     * Adds the extent of the time slot on the day: its hour, minute or second, or the point in time
     * of a time with a zone, such as 15:00:00Z, 15:00:00UTC or 15:00:00+01:00. A slot with a
     * timezone of its own, such as 15:00@UTC or 16:30@+01:00, moves by its offset, and a slot
     * without one by the offset of the expression, which callers leave at zero for a point in time.
     */
    static void addSlotExtent(TimestampDriver driver, long day, String slot, long expressionOffset, LongList out) {
        String time = slot;
        long offset = expressionOffset;
        final int zone = slot.indexOf('@');
        if (zone >= 0) {
            offset = parseOffset(driver, slot.substring(zone + 1));
            time = slot.substring(0, zone);
        }
        boolean isPoint = true;
        if (time.endsWith("UTC")) {
            assert expressionOffset == 0;
            time = time.substring(0, time.length() - 3);
            offset = 0;
        } else if (time.endsWith("Z")) {
            assert expressionOffset == 0;
            time = time.substring(0, time.length() - 1);
            offset = 0;
        } else if (time.length() > 8) {
            assert expressionOffset == 0;
            // HH:MM:SS+hh:mm
            offset = parseOffset(driver, time.substring(8));
            time = time.substring(0, 8);
        } else {
            isPoint = false;
        }
        final String[] fields = time.split(":");
        long lo = day + driver.fromHours(Integer.parseInt(fields[0]));
        long width = driver.fromHours(1);
        if (fields.length > 1) {
            lo += driver.fromMinutes(Integer.parseInt(fields[1]));
            width = driver.fromMinutes(1);
        }
        if (fields.length > 2) {
            lo += driver.fromSeconds(Integer.parseInt(fields[2]));
            width = driver.fromSeconds(1);
        }
        lo -= offset;
        out.add(lo, isPoint ? lo : lo + width - 1);
    }

    /**
     * Returns the parts of the sorted, disjoint intervals a that the sorted, disjoint intervals b
     * hold.
     */
    static LongList intersect(LongList a, LongList b) {
        final LongList out = new LongList();
        int i = 0;
        int j = 0;
        while (i < a.size() && j < b.size()) {
            final long lo = Math.max(a.getQuick(i), b.getQuick(j));
            final long hi = Math.min(a.getQuick(i + 1), b.getQuick(j + 1));
            if (lo <= hi) {
                out.add(lo, hi);
            }
            if (a.getQuick(i + 1) < b.getQuick(j + 1)) {
                i += 2;
            } else {
                j += 2;
            }
        }
        return out;
    }

    /**
     * Returns the sessions of the test calendar at the precision of the driver.
     */
    static LongList sessions(TimestampDriver driver, String calendar) {
        final LongList schedule = CALENDARS.getSchedule(calendar);
        Assert.assertNotNull(calendar, schedule);
        final LongList out = new LongList(schedule.size());
        for (int i = 0, n = schedule.size(); i < n; i += 2) {
            out.add(driver.fromMicros(schedule.getQuick(i)), driver.fromMicros(schedule.getQuick(i + 1) + 1) - 1);
        }
        return out;
    }

    /**
     * Returns the intervals sorted by their starts, with those that overlap or touch merged, as
     * the parser merges its result.
     */
    static LongList union(LongList intervals) {
        final long[][] pairs = new long[intervals.size() / 2][];
        for (int i = 0; i < pairs.length; i++) {
            pairs[i] = new long[]{intervals.getQuick(2 * i), intervals.getQuick(2 * i + 1)};
        }
        Arrays.sort(pairs, (a, b) -> Long.compare(a[0], b[0]));
        final LongList out = new LongList();
        for (long[] pair : pairs) {
            final int last = out.size() - 1;
            if (last > 0 && pair[0] <= out.getQuick(last) + 1) {
                out.setQuick(last, Math.max(out.getQuick(last), pair[1]));
            } else {
                out.add(pair[0], pair[1]);
            }
        }
        return out;
    }

    /**
     * Asserts that the parser parses the bare date variable expression and its date list spelling
     * to the expected intervals, and that the compiled form of the expression evaluates it to them.
     */
    private static void assertBareDateVariableMatchesDateList(TimestampDriver driver, String expression, long now, String expected) {
        final String context = expression + " [" + ColumnType.nameOf(driver.getTimestampType()) + ']';
        Assert.assertEquals(context, expected, parseTickExpr(driver, expression, now));
        Assert.assertEquals(context + " as a date list", expected, parseTickExpr(driver, toDateList(expression), now));
        Assert.assertEquals(context + " compiled", expected, evaluateCompiledTickExpr(driver, expression, now));
    }

    /**
     * Asserts that compiling the expression fails with the error, at the position of the literal, 100.
     */
    private static void assertCompileError(CairoConfiguration configuration, TimestampDriver driver, String expression, String expected) {
        final String context = expression + " [" + ColumnType.nameOf(driver.getTimestampType()) + ']';
        try (CompiledTickExpression ignore = IntervalUtils.compileTickExpr(driver, configuration, expression, 0, expression.length(), 100)) {
            Assert.fail(context + " should fail to compile");
        } catch (SqlException e) {
            Assert.assertEquals(context, expected, e.getFlyweightMessage().toString());
            Assert.assertEquals(context, 100, e.getPosition());
        }
    }

    /**
     * Asserts that the compiled form of the expression, which parses it on every evaluation,
     * validates it and evaluates it to the expected intervals or error.
     */
    private static void assertCompiledParsesOnEvaluation(TimestampDriver driver, String expression, long now, String expected) throws SqlException {
        try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(driver, configuration, expression, 0, expression.length(), 0)) {
            Assert.assertTrue(expression, compiled.isParsedOnEvaluation());
            if (!expected.startsWith("error: ")) {
                compiled.validate(now);
            }
        }
        Assert.assertEquals(expression, expected, evaluateCompiledTickExpr(driver, expression, now));
    }

    /**
     * Asserts that the compiled form of a tick expression has an IR, which validates the expression at
     * "now" and evaluates it to the expected intervals, and, with isBindAlike, that the parser, which
     * a bind variable takes, parses the same text to them too.
     */
    private static void assertCompiledStaticDate(
            CairoConfiguration configuration,
            TimestampDriver driver,
            String expression,
            long now,
            String expected,
            boolean isBindAlike
    ) throws SqlException {
        final String context = expression + " [" + ColumnType.nameOf(driver.getTimestampType()) + ']';
        try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(driver, configuration, expression, 0, expression.length(), 0)) {
            Assert.assertFalse(context, compiled.isParsedOnEvaluation());
            compiled.validate(now);
            final LongList out = new LongList();
            compiled.evaluate(out, now);
            Assert.assertEquals(context, expected, intervalToString(driver, out));
        }
        if (isBindAlike) {
            Assert.assertEquals(context + " as a bind variable", expected, parseTickExpr(configuration, driver, expression, now, IntervalUtils.TICK_CHECK_ALL));
        }
    }

    /**
     * Asserts that the expression compiles, with an IR or to parse it on evaluation as
     * isParsedOnEvaluation tells, and that validate() and an evaluation at "now" both fail with the
     * error, at the position of the literal, 100.
     */
    private static void assertCompiledStaticDateFails(
            CairoConfiguration configuration,
            TimestampDriver driver,
            String expression,
            long now,
            boolean isParsedOnEvaluation,
            String expected
    ) throws SqlException {
        final String context = expression + " [" + ColumnType.nameOf(driver.getTimestampType()) + ']';
        try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(driver, configuration, expression, 0, expression.length(), 100)) {
            Assert.assertEquals(context, isParsedOnEvaluation, compiled.isParsedOnEvaluation());
            try {
                compiled.validate(now);
                Assert.fail(context + " should fail to validate");
            } catch (SqlException e) {
                Assert.assertEquals(context, expected, "error: " + e.getFlyweightMessage());
                Assert.assertEquals(context, 100, e.getPosition());
            }
            try {
                compiled.evaluate(new LongList(), now);
                Assert.fail(context + " should fail to evaluate");
            } catch (CairoException e) {
                Assert.assertEquals(context, expected, "error: " + e.getFlyweightMessage());
                Assert.assertEquals(context, 100, e.getPosition());
            }
        }
    }

    // Adds the parts of the sessions of the schedule that the intervals, sorted and apart, cover
    private static void addSessionParts(LongList out, LongList intervals, LongList schedule) {
        for (int i = 0, n = intervals.size(); i < n; i += 2) {
            for (int s = 0, m = schedule.size(); s < m; s += 2) {
                final long lo = Math.max(intervals.getQuick(i), schedule.getQuick(s));
                final long hi = Math.min(intervals.getQuick(i + 1), schedule.getQuick(s + 1));
                if (lo <= hi) {
                    out.add(lo, hi);
                }
            }
        }
    }

    // Adds the sessions of the exchange on the days that isKept accepts
    private static void addSessions(LongList out, String exchange, Predicate<LocalDate> isKept) {
        final LongList schedule = TestTickCalendarService.INSTANCE.getSchedule(exchange);
        for (int i = 0, n = schedule.size(); i < n; i += 2) {
            if (isKept.test(toUtcDateTime(schedule.getQuick(i)).toLocalDate())) {
                out.add(schedule.getQuick(i), schedule.getQuick(i + 1));
            }
        }
    }

    /**
     * Asserts that the compiled form of the expression parses it on evaluation, as a bind variable,
     * and that validate() and an evaluation at "now" both give the expected intervals, or both fail
     * with the expected error, at the position of the literal, 100.
     */
    private static void assertCompiledStaticDateParsesFirst(CairoConfiguration configuration, TimestampDriver driver, String expression, long now, String expected) throws SqlException {
        if (expected.startsWith("error: ")) {
            assertCompiledStaticDateFails(configuration, driver, expression, now, true, expected);
            return;
        }
        final String context = expression + " [" + ColumnType.nameOf(driver.getTimestampType()) + ']';
        try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(driver, configuration, expression, 0, expression.length(), 100)) {
            Assert.assertTrue(context, compiled.isParsedOnEvaluation());
            compiled.validate(now);
            final LongList out = new LongList();
            compiled.evaluate(out, now);
            Assert.assertEquals(context, expected, intervalToString(driver, out));
        }
    }

    /**
     * Runs {@link #assertCompiledStaticDate} on each case with both timestamp types: the "now", the
     * expression, "bind" where the parser reads the text alike, then the bounds of the expected
     * intervals in pairs, see {@link #expectedIntervals(int, String[], int)}.
     */
    private static void assertCompiledStaticDates(CairoConfiguration configuration, String[][] cases) throws SqlException {
        for (int timestampType : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
            final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
            for (String[] c : cases) {
                assertCompiledStaticDate(configuration, driver, c[1], driver.parseFloorLiteral(c[0]), expectedIntervals(timestampType, c, 3), c[2].equals("bind"));
            }
        }
    }

    /**
     * Asserts that the parser parses the expression to the expected intervals with and without
     * isBareDurationPerDate, and that the IR of an expression with a date variable evaluates it to
     * them. SQL parses an expression without one as it compiles.
     */
    private static void assertParsedAlikeWithAndWithoutDurationPerDate(TimestampDriver driver, String expression, long now, String expected) throws SqlException {
        final String context = expression + " [" + ColumnType.nameOf(driver.getTimestampType()) + ']';
        Assert.assertEquals(context, expected, parseTickExpr(driver, expression, now, IntervalUtils.TICK_CHECK_ALL, true));
        Assert.assertEquals(context, expected, parseTickExpr(driver, expression, now, IntervalUtils.TICK_CHECK_ALL, false));
        if (expression.indexOf('$') < 0) {
            return;
        }
        try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(driver, configuration, expression, 0, expression.length(), 0)) {
            Assert.assertFalse(context, compiled.isParsedOnEvaluation());
            final LongList out = new LongList();
            compiled.evaluate(out, now);
            Assert.assertEquals(context + " compiled", expected, intervalToString(driver, out));
        }
    }

    /**
     * Asserts that the parser parses the expression to the expected intervals or error with every
     * combination of the checks that depend on "now".
     */
    private static void assertParsedAtEveryCheckLevel(TimestampDriver driver, String expression, long now, String expected) {
        for (int checks : new int[]{IntervalUtils.TICK_CHECK_ALL, IntervalUtils.TICK_CHECK_INTERVAL_LIMIT, 0}) {
            Assert.assertEquals(expression + " [checks=" + checks + ']', expected, parseTickExpr(driver, expression, now, checks));
        }
    }

    private static LongList buildX24h() {
        final long day = Micros.DAY_MICROS;
        final LongList sessions = new LongList();
        sessions.add(-day, -1L);
        sessions.add(0L, day - 1);
        final long start = LocalDate.of(2025, 1, 1).toEpochDay() * day;
        for (int d = 0; d < 365; d++) {
            sessions.add(start + d * day, start + (d + 1) * day - 1);
        }
        final long year2099 = LocalDate.of(2099, 1, 2).toEpochDay() * day;
        sessions.add(year2099, year2099 + day - 1);
        sessions.add(year2099 + day, year2099 + 2 * day - 1);
        return sessions;
    }

    /**
     * Returns a configuration with the test tick calendars, the given number of intervals after
     * which a date range merges the intervals of each day into the ones so far, and the given
     * limit on the merged intervals.
     */
    private static CairoConfiguration calendarConfiguration(int mergeThreshold, int maxIntervals) {
        return new DefaultCairoConfiguration(System.getProperty("java.io.tmpdir")) {
            @Override
            public @NotNull FactoryProvider getFactoryProvider() {
                return configuration.getFactoryProvider();
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
     * Returns the intervals that the compiled form evaluates the expression to, or its error.
     */
    private static String evaluateCompiledTickExpr(TimestampDriver driver, String expression, long now) {
        try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(driver, configuration, expression, 0, expression.length(), 0)) {
            final LongList out = new LongList();
            try {
                compiled.evaluate(out, now);
                return intervalToString(driver, out);
            } catch (CairoException e) {
                return "error: " + e.getFlyweightMessage();
            }
        } catch (SqlException e) {
            return "compile error: " + e.getFlyweightMessage();
        }
    }

    /**
     * Returns the intervals of the bounds from index lo on, in pairs, written as {@link #ts} takes them,
     * as intervalToString() prints them at the precision of the timestamp type.
     */
    private static String expectedIntervals(int timestampType, String[] bounds, int lo) {
        final StringBuilder sb = new StringBuilder("[");
        for (int i = lo; i < bounds.length; i += 2) {
            if (i > lo) {
                sb.append(',');
            }
            sb.append("{lo=").append(ts(bounds[i])).append(", hi=").append(ts(bounds[i + 1])).append('}');
        }
        return forTimestampType(sb.append(']').toString(), timestampType);
    }

    private static String expectedIntervals(int timestampType, String... bounds) {
        return expectedIntervals(timestampType, bounds, 0);
    }

    /**
     * Returns intervals given at microsecond precision at the precision of the timestamp type.
     */
    private static String forTimestampType(String expected, int timestampType) {
        return ColumnType.isTimestampNano(timestampType) ? expected.replace("00Z", "00000Z").replace("99Z", "99999Z") : expected;
    }

    // Appends $today, or $now - rangeHours h..$now when isRange is set, in a random spelling, which
    // the IR encodes as the date variable: names in a random case, spaces around the operators and
    // before the unit, and a leading zero
    private static void appendDateVariableSpelling(Rnd rnd, StringSink sink, boolean isRange, int rangeHours) {
        if (!isRange) {
            appendRandomCase(rnd, sink, "$today");
            return;
        }
        appendRandomCase(rnd, sink, "$now");
        appendRandomSpaces(rnd, sink);
        sink.put('-');
        appendRandomSpaces(rnd, sink);
        if (rnd.nextBoolean()) {
            sink.put('0');
        }
        sink.put(rangeHours);
        appendRandomSpaces(rnd, sink);
        sink.put('h');
        appendRandomSpaces(rnd, sink);
        sink.put("..");
        appendRandomSpaces(rnd, sink);
        appendRandomCase(rnd, sink, "$now");
    }

    // Appends the name with each letter in a random case
    private static void appendRandomCase(Rnd rnd, StringSink sink, String name) {
        for (int k = 0, n = name.length(); k < n; k++) {
            final char c = name.charAt(k);
            sink.put(rnd.nextBoolean() ? Character.toUpperCase(c) : c);
        }
    }

    // Appends 0 to 2 spaces
    private static void appendRandomSpaces(Rnd rnd, StringSink sink) {
        for (int k = rnd.nextInt(3); k > 0; k--) {
            sink.put(' ');
        }
    }

    // Whether an interval lies inside another one, which starts before it and ends after it, and
    // ends inside a session
    private static boolean hasNestedPairInSession(LongList intervals, LongList schedule) {
        for (int i = 0, n = intervals.size(); i < n; i += 2) {
            if (hasNestedPairInSession(intervals, intervals.getQuick(i), intervals.getQuick(i + 1), schedule)) {
                return true;
            }
        }
        return false;
    }

    // Whether [lo, hi] lies inside one of the intervals, which starts before it and ends after it, and
    // ends inside a session
    private static boolean hasNestedPairInSession(LongList intervals, long lo, long hi, LongList schedule) {
        for (int i = 0, n = intervals.size(); i < n; i += 2) {
            if (intervals.getQuick(i) < lo && hi < intervals.getQuick(i + 1)) {
                for (int s = 0, m = schedule.size(); s < m; s += 2) {
                    if (schedule.getQuick(s) <= hi && hi < schedule.getQuick(s + 1)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private static String intervalToString(TimestampDriver driver, LongList intervals) {
        sink.clear();
        sink.put('[');
        for (int i = 0, n = intervals.size(); i < n; i += 2) {
            if (i > 0) {
                sink.put(',');
            }
            sink.put('{');
            sink.put("lo=");
            driver.append(sink, intervals.getQuick(i));
            sink.put(", ");
            sink.put("hi=");
            driver.append(sink, intervals.getQuick(i + 1));
            sink.put('}');
        }
        sink.put(']');
        return sink.toString();
    }

    /**
     * Returns whether the result of parseTickExpr() is the error of the interval limit.
     */
    private static boolean isIntervalLimitError(String parsed) {
        return parsed.startsWith("error: ") && parsed.contains(" produces too many intervals (max ");
    }

    /**
     * Returns the intervals of a bare date variable expression with a tick calendar and a duration
     * of a fixed length by its documented semantics: the extent of each time slot on each of the
     * days, or the whole day without time slots, see addSlotExtent(), united, cut by the sessions
     * of the calendar, each end extended by the duration, and merged. The expression offset moves
     * the days and the slots without a timezone or a zone of their own, as a timezone of the
     * expression such as @+08:00 does.
     * <p>
     * With isDurationPerDate, each day and slot is instead a window as wide as the duration from
     * where it starts, which the duration extends again after the tick calendar cuts it, as
     * WhereClauseParser's static inversion parses such an expression. That takes a duration longer
     * than zero.
     */
    private static String modelBareDateVariable(
            TimestampDriver driver,
            LongList days,
            String[] slots,
            long expressionOffset,
            String calendar,
            long duration,
            boolean isDurationPerDate
    ) {
        assert !isDurationPerDate || duration > 0;
        final LongList extents = new LongList();
        for (int i = 0, n = days.size(); i < n; i++) {
            final long day = days.getQuick(i);
            if (slots.length == 0) {
                extents.add(day - expressionOffset, day - expressionOffset + driver.fromDays(1) - 1);
            } else {
                for (String slot : slots) {
                    addSlotExtent(driver, day, slot, expressionOffset, extents);
                }
            }
        }
        if (isDurationPerDate) {
            for (int i = 0, n = extents.size(); i < n; i += 2) {
                extents.setQuick(i + 1, extents.getQuick(i) + duration - 1);
            }
        }
        final LongList kept = intersect(union(extents), sessions(driver, calendar));
        for (int i = 1, n = kept.size(); i < n; i += 2) {
            kept.setQuick(i, kept.getQuick(i) + duration);
        }
        return intervalToString(driver, union(kept));
    }

    /**
     * Returns the offset from UTC of a timezone written as UTC, +hh:mm or -hh:mm.
     */
    private static long parseOffset(TimestampDriver driver, String zone) {
        if (zone.equals("UTC")) {
            return 0;
        }
        final long offset = driver.fromHours(Integer.parseInt(zone.substring(1, 3))) + driver.fromMinutes(Integer.parseInt(zone.substring(4, 6)));
        return zone.charAt(0) == '-' ? -offset : offset;
    }

    /**
     * Returns the intervals that the parser makes of the expression without its tick calendar, which
     * reads the duration of each fixed date, cut by the sessions of the calendar.
     */
    private static String parsedWithoutCalendarCutBySessions(TimestampDriver driver, String expression, String calendar, long now) throws SqlException {
        final String withoutCalendar = expression.replace('#' + calendar, "");
        final LongList out = new LongList();
        IntervalUtils.parseTickExpr(driver, configuration, withoutCalendar, 0, withoutCalendar.length(), 0, out, sink, now, IntervalUtils.TICK_CHECK_ALL);
        return intervalToString(driver, union(intersect(out, sessions(driver, calendar))));
    }

    /**
     * Returns the intervals that the parser parses the expression to, or its error.
     */
    private static String parseTickExpr(TimestampDriver driver, String expression, long now) {
        return parseTickExpr(driver, expression, now, IntervalUtils.TICK_CHECK_ALL);
    }

    /**
     * Returns the intervals that the parser parses the expression to with the checks, or its
     * error.
     */
    private static String parseTickExpr(TimestampDriver driver, String expression, long now, int checks) {
        return parseTickExpr(configuration, driver, expression, now, checks);
    }

    /**
     * Returns the intervals that the parser parses the expression to with the checks, with or
     * without isBareDurationPerDate, or its error.
     */
    private static String parseTickExpr(TimestampDriver driver, String expression, long now, int checks, boolean isBareDurationPerDate) {
        final LongList out = new LongList();
        try {
            IntervalUtils.parseTickExpr(driver, configuration, expression, 0, expression.length(), 0, out, sink, now, checks, isBareDurationPerDate);
            return intervalToString(driver, out);
        } catch (SqlException e) {
            return "error: " + e.getFlyweightMessage();
        }
    }

    /**
     * Returns the intervals that the parser parses the expression to with the configuration and
     * the checks, or its error.
     */
    private static String parseTickExpr(CairoConfiguration configuration, TimestampDriver driver, String expression, long now, int checks) {
        final LongList out = new LongList();
        try {
            IntervalUtils.parseTickExpr(driver, configuration, expression, 0, expression.length(), 0, out, sink, now, checks);
            return intervalToString(driver, out);
        } catch (SqlException e) {
            return "error: " + e.getFlyweightMessage();
        }
    }

    /**
     * Returns the sessions of the test calendar that start from {@code from} to {@code to}, each
     * extended by the duration, and asserts that there are as many as expected.
     */
    private static String sessionsToString(TimestampDriver driver, String calendar, long from, long to, long duration, int expectedCount) {
        final LongList all = sessions(driver, calendar);
        final LongList out = new LongList();
        for (int i = 0, n = all.size(); i < n; i += 2) {
            if (all.getQuick(i) >= from && all.getQuick(i) <= to) {
                out.add(all.getQuick(i), all.getQuick(i + 1) + duration);
            }
        }
        final LongList merged = union(out);
        Assert.assertEquals(calendar, expectedCount, merged.size() / 2);
        return intervalToString(driver, merged);
    }

    /**
     * Returns the part of a slot that the session of each day holds, for the sessions of the calendar
     * that start from fromDay to before toDay, each extended by the duration, united with the extra
     * intervals, given as pairs of a start and an end that is not part of them, as the parser merges its
     * result. The slot starts slotOffset after the start of its day and lasts slotWidth.
     */
    private static String slotOnSessionDays(
            TimestampDriver driver,
            String calendar,
            String fromDay,
            String toDay,
            long slotOffset,
            long slotWidth,
            long duration,
            String... extra
    ) {
        final long from = driver.parseFloorLiteral(fromDay + "T00:00:00.000000Z");
        final long to = driver.parseFloorLiteral(toDay + "T00:00:00.000000Z");
        final LongList sessions = sessions(driver, calendar);
        final LongList out = new LongList();
        for (int i = 0, n = sessions.size(); i < n; i += 2) {
            final long sessionLo = sessions.getQuick(i);
            final long sessionHi = sessions.getQuick(i + 1);
            if (sessionLo >= from && sessionLo < to) {
                final long slotLo = driver.startOfDay(sessionLo, 0) + slotOffset;
                final long lo = Math.max(slotLo, sessionLo);
                final long hi = Math.min(slotLo + slotWidth - 1, sessionHi);
                if (lo <= hi) {
                    out.add(lo, hi + duration);
                }
            }
        }
        for (int i = 0; i < extra.length; i += 2) {
            out.add(driver.parseFloorLiteral(ts(extra[i])), driver.parseFloorLiteral(ts(extra[i + 1])) - 1);
        }
        return intervalToString(driver, union(out));
    }

    /**
     * Returns the date list spelling of a bare date variable expression: $exprT10:00#XNYS;1h as
     * [$expr]T10:00#XNYS;1h. The expression ends where the parser ends it, at '@', '#', ';', or a
     * 'T' before a digit or a '['.
     */
    private static String toDateList(String bare) {
        int end = bare.indexOf('$') + 1;
        while (end < bare.length()) {
            final char c = bare.charAt(end);
            if (c == '@' || c == '#' || c == ';' || (c == 'T' && end + 1 < bare.length() && (Character.isDigit(bare.charAt(end + 1)) || bare.charAt(end + 1) == '['))) {
                break;
            }
            end++;
        }
        return '[' + bare.substring(0, end) + ']' + bare.substring(end);
    }

    /**
     * Returns a timestamp written to the minute, the second or the microsecond as intervalToString()
     * prints it at microsecond precision.
     */
    private static String ts(String text) {
        return switch (text.length()) {
            case 16 -> text + ":00.000000Z";
            case 19 -> text + ".000000Z";
            case 26 -> text + 'Z';
            default -> throw new IllegalArgumentException(text);
        };
    }

    // n copies of the value, separated by commas
    private static String repeatedValues(String value, int n) {
        final StringBuilder values = new StringBuilder();
        for (int k = 0; k < n; k++) {
            values.append(k == 0 ? "" : ",").append(value);
        }
        return values.toString();
    }

    private static LocalDateTime toUtcDateTime(long micros) {
        return LocalDateTime.ofInstant(Instant.ofEpochSecond(0, micros * 1_000), ZoneOffset.UTC);
    }

    // The union of the intervals: sorted by lo, and joined where they overlap or touch
    private static LongList unionOf(LongList intervals) {
        final TreeMap<Long, Long> hiByLo = new TreeMap<>();
        for (int i = 0, n = intervals.size(); i < n; i += 2) {
            hiByLo.merge(intervals.getQuick(i), intervals.getQuick(i + 1), Math::max);
        }
        final LongList union = new LongList();
        for (Map.Entry<Long, Long> interval : hiByLo.entrySet()) {
            final int size = union.size();
            if (size > 0 && interval.getKey() <= union.getQuick(size - 1) + 1) {
                union.setQuick(size - 1, Math.max(union.getQuick(size - 1), interval.getValue()));
            } else {
                union.add(interval.getKey(), interval.getValue());
            }
        }
        return union;
    }

    // Expects compileTickExpr() to fail with the cap error at position 0
    private void assertCompiledOverMaxIntervals(String expression) {
        try {
            IntervalUtils.compileTickExpr(ColumnType.getTimestampDriver(ColumnType.TIMESTAMP), configuration, expression, 0, expression.length(), 0).close();
            Assert.fail("Expected the cap error for " + expression);
        } catch (SqlException e) {
            Assert.assertEquals(expression, 0, e.getPosition());
            TestUtils.assertEquals(OVER_CAP_ERROR, e.getFlyweightMessage());
        }
    }

    private void assertCompiledTickIntervalWithNow(String expected, String expression, long nowTimestamp) throws SqlException {
        final LongList out = new LongList();
        final TimestampDriver driver = ColumnType.getTimestampDriver(ColumnType.TIMESTAMP);
        try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(driver, configuration, expression, 0, expression.length(), 0)) {
            compiled.evaluate(out, nowTimestamp);
        }
        Assert.assertEquals(expected, intervalToString(driver, out));
    }

    private void assertTickInterval(String expected, String interval) throws SqlException {
        LongList out = new LongList();
        TimestampDriver driver = ColumnType.getTimestampDriver(ColumnType.TIMESTAMP);
        IntervalUtils.parseTickExpr(driver, configuration, interval, 0, interval.length(), 0, out, sink);
        Assert.assertEquals(expected, intervalToString(driver, out));
    }

    private void assertTickIntervalNanos(String expected, String interval) throws SqlException {
        LongList out = new LongList();
        TimestampDriver driver = ColumnType.getTimestampDriver(ColumnType.TIMESTAMP_NANO);
        IntervalUtils.parseTickExpr(driver, configuration, interval, 0, interval.length(), 0, out, sink);
        Assert.assertEquals(expected, intervalToString(driver, out));
    }

    private void assertTickIntervalErrorWithNow(String interval, String expectedError, long nowTimestamp) {
        final LongList out = new LongList();
        final TimestampDriver driver = ColumnType.getTimestampDriver(ColumnType.TIMESTAMP);
        try {
            IntervalUtils.parseTickExpr(driver, configuration, interval, 0, interval.length(), 0, out, sink, nowTimestamp);
            Assert.fail("Expected SqlException with message containing: " + expectedError + ", got " + intervalToString(driver, out));
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), expectedError);
        }
        // parseTickExpr unwinds the list
        Assert.assertEquals(0, out.size());
    }

    private void assertTickIntervalWithNow(String expected, String interval, long nowTimestamp) throws SqlException {
        LongList out = new LongList();
        TimestampDriver driver = ColumnType.getTimestampDriver(ColumnType.TIMESTAMP);
        IntervalUtils.parseTickExpr(driver, configuration, interval, 0, interval.length(), 0, out, sink, nowTimestamp);
        Assert.assertEquals(expected, intervalToString(driver, out));
    }

    /**
     * Generates bare date variable expressions with a tick calendar and a duration, and checks what
     * the parser makes of each: what it makes of the date list spelling and what the IR evaluates,
     * the error of the same text without the tick calendar for a duration that does not parse or
     * a repeating interval, and, for days and time slots that modelBareDateVariable() models, the
     * intervals of the documented semantics. The date list differs only where only a duration lets
     * the parser read a time, such as 15:00:00UTC, which the date list fails on; time slots that
     * hold one another unite before the tick calendar cuts them in all three. It counts the shapes
     * it checks.
     */
    private static final class BareDateVariableFuzzer {
        private static final String[] CALENDARS = {"XNYS", "XHKG", "xnys"};
        private static final int[] CHECKS = {IntervalUtils.TICK_CHECK_ALL, IntervalUtils.TICK_CHECK_INTERVAL_LIMIT, 0};
        private static final String[] DURATIONS = {"1h", "30m", "1h30m", "1d", "4h", "30s", "", "0h", "2h-1h", "-30m", "-1h", "1M", "1y"};
        private static final String[] EXPRESSIONS = {
                "$today", "$TODAY", "$yesterday", "$tomorrow", "$today + 3d", "$today+1bd", "$now", "$now - 2h",
                "$today..$today+2d", "$today..$today+2bd", "$yesterday..$tomorrow", "$today..$now", "$now - 1h..$now",
                "$today + 870m..$now", "$today-10d..$today"
        };
        private static final String[] MALFORMED_DURATIONS = {"1x", "abc", "1", "1h@UTC", "-1x", "1h;1d;3", "1h;1d"};
        // the durations of the modelled shapes, and their lengths in seconds
        private static final String[] MODEL_DURATIONS = {"30s", "1m", "10m", "30m", "1h", "90m", "4h", "1d", "1h30m", "0h", "", "-30m", "-1h"};
        private static final long[] MODEL_DURATION_SECONDS = {30, 60, 600, 1_800, 3_600, 5_400, 14_400, 86_400, 5_400, 0, 0, -1_800, -3_600};
        private static final String[] MODEL_SLOTS = {
                "15", "16", "15:00", "15:30", "16:30", "20:30", "14:15", "09:00", "15:00:30", "15:00:00Z", "17:00:00Z",
                "15:00:00UTC", "15:00:00+01:00", "15:00:00-05:00", "15:00@UTC", "16:30@+01:00"
        };
        private static final String[] NOWS = {
                "2025-01-24T03:00:00.000000Z", // a Friday, before the XNYS session and in the morning XHKG one
                "2025-01-24T15:30:00.000000Z", // a Friday, in the XNYS session
                "2025-01-24T22:00:00.000000Z", // a Friday, after the XNYS session
                "2025-01-25T12:00:00.000000Z", // a Saturday
                "2025-01-20T12:00:00.000000Z", // a Monday that XNYS closes for
                "2025-07-03T12:00:00.000000Z", // a Thursday that XNYS closes early on
                "2025-03-10T12:00:00.000000Z" // the Monday after New York moves to summer time
        };
        // the hours of the day, out of order
        private static final String OUT_OF_ORDER_HOURS = "T[20,17,00..16,18,19,21..23]";
        private static final String[] TIME_PARTS = {
                "", "T15:00", "T15", "T15:00:30", "T20:30", "T[15:00,16:00]", "T[09:00,14:30,15:00]", "T[23:30,00:30]",
                "T[15:00@UTC]", "T[15:00@UTC,16:30@+01:00]", "T[15:00@America/New_York,16:00]", "T[15..16]:00",
                "T[14..20]:[00,30]", OUT_OF_ORDER_HOURS, "T[15:00@UTC,16:00]:[00,30]", "T[15:00,15:00:30]", "T[15,15:30]",
                "T15:00:00Z", "T15:00:00UTC", "T09:00:00UTC", "T15:00:00+01:00", "T15:00:00-05"
        };
        private static final String[] ZONES = {"", "@America/New_York", "@+08:00"};
        private final LongList days = new LongList();
        private final CairoConfiguration smallLimits = calendarConfiguration(8, 16);
        // the shapes that the fuzzer checked
        private int calendarUnitDurationCount;
        private int dayRangeCount;
        private int irCount;
        private int malformedDurationCount;
        private int modelCount;
        private int negativeDurationCount;
        private int nestedCount;
        private int numericBracketsCount;
        private int outOfOrderAtSmallLimitCount;
        private int pointInTimeCount;
        private int pointInTimeRangeCount;
        private int repeatingSuffixCount;
        private int slotZoneCount;
        private int timeListCount;
        // the modelled shapes with a duration longer than zero, which the windows of the static
        // inversion model too
        private int windowModelCount;

        void assertEveryShape() {
            Assert.assertTrue("a time list", timeListCount > 0);
            Assert.assertTrue("a time slot with a timezone of its own", slotZoneCount > 0);
            Assert.assertTrue("numeric brackets", numericBracketsCount > 0);
            Assert.assertTrue("time slots that hold one another", nestedCount > 0);
            Assert.assertTrue("hours out of order at a limit of 16 with a result", outOfOrderAtSmallLimitCount > 0);
            Assert.assertTrue("a range of days", dayRangeCount > 0);
            Assert.assertTrue("a range between two points in time", pointInTimeRangeCount > 0);
            Assert.assertTrue("a negative duration", negativeDurationCount > 0);
            Assert.assertTrue("a duration in months or years", calendarUnitDurationCount > 0);
            Assert.assertTrue("a duration that does not parse", malformedDurationCount > 0);
            Assert.assertTrue("a repeating interval", repeatingSuffixCount > 0);
            Assert.assertTrue("a time with a zone", pointInTimeCount > 0);
            Assert.assertTrue("an IR", irCount > 0);
            Assert.assertTrue("a modelled shape", modelCount > 0);
            Assert.assertTrue("a modelled shape with a duration longer than zero", windowModelCount > 0);
        }

        void check(Rnd rnd) throws SqlException {
            final TimestampDriver driver = ColumnType.getTimestampDriver(rnd.nextBoolean() ? ColumnType.TIMESTAMP_MICRO : ColumnType.TIMESTAMP_NANO);
            final String nowText = NOWS[rnd.nextInt(NOWS.length)];
            final long now = driver.parseFloorLiteral(nowText);
            final int checks = CHECKS[rnd.nextInt(CHECKS.length)];
            final String calendar = CALENDARS[rnd.nextInt(CALENDARS.length)];
            if (rnd.nextBoolean()) {
                checkModelled(rnd, driver, nowText, now, checks, calendar);
            } else {
                checkGeneral(rnd, driver, nowText, now, checks, calendar);
            }
        }

        void log() {
            LOG.info().$("bare tick calendar fuzz [timeLists=").$(timeListCount)
                    .$(", slotZones=").$(slotZoneCount)
                    .$(", numericBrackets=").$(numericBracketsCount)
                    .$(", nested=").$(nestedCount)
                    .$(", outOfOrderAtSmallLimit=").$(outOfOrderAtSmallLimitCount)
                    .$(", dayRanges=").$(dayRangeCount)
                    .$(", pointInTimeRanges=").$(pointInTimeRangeCount)
                    .$(", negativeDurations=").$(negativeDurationCount)
                    .$(", calendarUnitDurations=").$(calendarUnitDurationCount)
                    .$(", malformedDurations=").$(malformedDurationCount)
                    .$(", repeatingSuffixes=").$(repeatingSuffixCount)
                    .$(", pointsInTime=").$(pointInTimeCount)
                    .$(", irs=").$(irCount)
                    .$(", modelled=").$(modelCount)
                    .$(", windowModelled=").$(windowModelCount)
                    .I$();
        }

        private static String dayExpression(int day) {
            return switch (day) {
                case -1 -> "$yesterday";
                case 0 -> "$today";
                case 1 -> "$tomorrow";
                default -> day > 0 ? "$today + " + day + 'd' : "$today - " + -day + 'd';
            };
        }

        /**
         * Returns whether the extents of any two of the slots on the days overlap, as a minute and a
         * second in it do.
         */
        private static boolean hasOverlappingExtents(TimestampDriver driver, LongList days, String[] slots) {
            final LongList extents = new LongList();
            for (int i = 0, n = days.size(); i < n; i++) {
                for (String slot : slots) {
                    addSlotExtent(driver, days.getQuick(i), slot, 0, extents);
                }
            }
            final long[][] pairs = new long[extents.size() / 2][];
            for (int i = 0; i < pairs.length; i++) {
                pairs[i] = new long[]{extents.getQuick(2 * i), extents.getQuick(2 * i + 1)};
            }
            Arrays.sort(pairs, (a, b) -> Long.compare(a[0], b[0]));
            for (int i = 1; i < pairs.length; i++) {
                if (pairs[i][0] <= pairs[i - 1][1]) {
                    return true;
                }
            }
            return false;
        }

        private void checkDateListAndIr(
                TimestampDriver driver,
                CairoConfiguration limitConfiguration,
                String text,
                long now,
                int checks,
                boolean isSmallLimit,
                String context,
                String parsed,
                String withoutCalendar,
                boolean isReadOnlyWithDuration
        ) throws SqlException {
            if (isReadOnlyWithDuration) {
                // the date list fails on the time, which the same text without the tick calendar reads
                if (parsed.startsWith("error: ") || withoutCalendar.startsWith("error: ")) {
                    Assert.assertEquals(context, withoutCalendar, parsed);
                }
            } else {
                Assert.assertEquals(context + " as a date list", parseTickExpr(limitConfiguration, driver, toDateList(text), now, checks), parsed);
            }
            if (!isSmallLimit && checks == IntervalUtils.TICK_CHECK_ALL) {
                CompiledTickExpression compiled = null;
                try {
                    compiled = IntervalUtils.compileTickExpr(driver, configuration, text, 0, text.length(), 0);
                } catch (SqlException ignore) {
                    // the IR rejects the time, which the parser reads
                }
                try {
                    if (compiled != null && !compiled.isParsedOnEvaluation()) {
                        irCount++;
                        final LongList out = new LongList();
                        String evaluated;
                        try {
                            compiled.evaluate(out, now);
                            evaluated = intervalToString(driver, out);
                        } catch (CairoException e) {
                            evaluated = "error: " + e.getFlyweightMessage();
                        }
                        Assert.assertEquals(context + " as its IR evaluates it", evaluated, parsed);
                    }
                } finally {
                    Misc.free(compiled);
                }
            }
        }

        private void checkGeneral(Rnd rnd, TimestampDriver driver, String nowText, long now, int checks, String calendar) throws SqlException {
            final String expression = EXPRESSIONS[rnd.nextInt(EXPRESSIONS.length)];
            final String timePart = TIME_PARTS[rnd.nextInt(TIME_PARTS.length)];
            final String zone = ZONES[rnd.nextInt(ZONES.length)];
            final boolean isMalformed = rnd.nextInt(4) == 0;
            final String duration = isMalformed ? MALFORMED_DURATIONS[rnd.nextInt(MALFORMED_DURATIONS.length)] : DURATIONS[rnd.nextInt(DURATIONS.length)];
            // the hours out of order run at the small limits more often, see assertEveryShape()
            final boolean isSmallLimit = timePart.equals(OUT_OF_ORDER_HOURS) ? rnd.nextInt(4) != 0 : rnd.nextInt(4) == 0;
            final CairoConfiguration limitConfiguration = isSmallLimit ? smallLimits : configuration;
            final String text = expression + timePart + zone + '#' + calendar + ';' + duration;
            final String context = text + " at " + nowText + " [" + ColumnType.nameOf(driver.getTimestampType())
                    + ", checks=" + checks + (isSmallLimit ? ", limit 16]" : "]");
            final String parsed = parseTickExpr(limitConfiguration, driver, text, now, checks);
            final String withoutCalendar = parseTickExpr(limitConfiguration, driver, expression + timePart + zone + ';' + duration, now, checks);
            if (isMalformed) {
                if (duration.indexOf(';') >= 0) {
                    repeatingSuffixCount++;
                    // the parser reads a repeating interval with each date, as without the tick
                    // calendar, so it fails where that does
                    if (withoutCalendar.startsWith("error: ")) {
                        Assert.assertEquals(context, withoutCalendar, parsed);
                    }
                } else {
                    malformedDurationCount++;
                    // each date parses with the duration, which fails it as without the tick calendar
                    if (parsed.startsWith("error: ") || withoutCalendar.startsWith("error: ")) {
                        Assert.assertEquals(context, withoutCalendar, parsed);
                    }
                }
                return;
            }
            final boolean isNested = timePart.equals("T[15:00,15:00:30]") || timePart.equals("T[15,15:30]");
            final boolean isReadOnlyWithDuration = timePart.endsWith("UTC");
            checkDateListAndIr(driver, limitConfiguration, text, now, checks, isSmallLimit, context, parsed, withoutCalendar, isReadOnlyWithDuration);

            timeListCount += timePart.startsWith("T[") ? 1 : 0;
            slotZoneCount += timePart.indexOf('@') >= 0 ? 1 : 0;
            numericBracketsCount += timePart.contains("..") || timePart.contains(":[") ? 1 : 0;
            nestedCount += isNested ? 1 : 0;
            outOfOrderAtSmallLimitCount += timePart.equals(OUT_OF_ORDER_HOURS) && isSmallLimit && !parsed.startsWith("error: ") ? 1 : 0;
            final boolean isPointInTimeRange = expression.endsWith("..$now") && expression.startsWith("$now") || expression.startsWith("$today + 870m");
            pointInTimeRangeCount += isPointInTimeRange ? 1 : 0;
            dayRangeCount += expression.contains("..") && !isPointInTimeRange ? 1 : 0;
            negativeDurationCount += duration.startsWith("-") ? 1 : 0;
            calendarUnitDurationCount += duration.equals("1M") || duration.equals("1y") ? 1 : 0;
            pointInTimeCount += timePart.contains(":00:00") ? 1 : 0;
        }

        private void checkModelled(Rnd rnd, TimestampDriver driver, String nowText, long now, int checks, String calendar) throws SqlException {
            // a day, or a range of days, relative to today
            final int startDay = rnd.nextInt(5) - 2;
            final boolean isRange = rnd.nextBoolean();
            final int endDay = isRange ? startDay + rnd.nextInt(4) : startDay;
            final String expression = isRange ? dayExpression(startDay) + ".." + dayExpression(endDay) : dayExpression(startDay);
            // no time part, a single time, or a time list
            final String[] slots = new String[rnd.nextInt(4)];
            boolean hasSlotZone = false;
            boolean isReadOnlyWithDuration = false;
            boolean hasPointInTime = false;
            for (int i = 0; i < slots.length; i++) {
                slots[i] = MODEL_SLOTS[rnd.nextInt(MODEL_SLOTS.length)];
                final boolean isSlotZone = slots[i].indexOf('@') >= 0;
                hasSlotZone |= isSlotZone;
                isReadOnlyWithDuration |= !isSlotZone && slots[i].endsWith("UTC");
                hasPointInTime |= !isSlotZone && slots[i].length() > 8;
            }
            final String timePart;
            if (slots.length == 0) {
                timePart = "";
            } else if (slots.length == 1 && !hasSlotZone && rnd.nextBoolean()) {
                timePart = "T" + slots[0];
            } else {
                timePart = "T[" + String.join(",", slots) + ']';
            }
            final int durationIndex = rnd.nextInt(MODEL_DURATIONS.length);
            final long duration = driver.fromSeconds(MODEL_DURATION_SECONDS[durationIndex]);
            final boolean isSmallLimit = rnd.nextInt(4) == 0;
            final CairoConfiguration limitConfiguration = isSmallLimit ? smallLimits : configuration;
            final String text = expression + timePart + '#' + calendar + ';' + MODEL_DURATIONS[durationIndex];
            final String context = text + " at " + nowText + " [" + ColumnType.nameOf(driver.getTimestampType())
                    + ", checks=" + checks + (isSmallLimit ? ", limit 16]" : "]");

            final long today = driver.startOfDay(now, 0);
            days.clear();
            for (int day = startDay; day <= endDay; day++) {
                days.add(today + driver.fromDays(day));
            }
            final String parsed = parseTickExpr(limitConfiguration, driver, text, now, checks);
            if (isSmallLimit && parsed.startsWith("error: ")) {
                // the date list counts the same intervals, see checkDateListAndIr()
                Assert.assertTrue(context + ": " + parsed, isIntervalLimitError(parsed));
            } else {
                Assert.assertEquals(context, modelBareDateVariable(driver, days, slots, 0, calendar, duration, false), parsed);
            }
            modelCount++;
            if (!isSmallLimit && MODEL_DURATION_SECONDS[durationIndex] > 0) {
                windowModelCount++;
                Assert.assertEquals(
                        context + " with isBareDurationPerDate",
                        modelBareDateVariable(driver, days, slots, 0, calendar, duration, true),
                        parseTickExpr(driver, text, now, checks, true)
                );
            }
            final boolean isNested = hasOverlappingExtents(driver, days, slots);
            final String withoutCalendar = parseTickExpr(limitConfiguration, driver, expression + timePart + ';' + MODEL_DURATIONS[durationIndex], now, checks);
            checkDateListAndIr(driver, limitConfiguration, text, now, checks, isSmallLimit, context, parsed, withoutCalendar, isReadOnlyWithDuration);

            timeListCount += timePart.startsWith("T[") ? 1 : 0;
            slotZoneCount += hasSlotZone ? 1 : 0;
            nestedCount += isNested ? 1 : 0;
            dayRangeCount += isRange ? 1 : 0;
            negativeDurationCount += MODEL_DURATION_SECONDS[durationIndex] < 0 ? 1 : 0;
            pointInTimeCount += hasPointInTime ? 1 : 0;
        }
    }

    /**
     * Generates lists with a tick calendar and a duration whose fixed date alone has more own extents
     * than the interval limit, which the compiled form parses as their bind variable on each evaluation:
     * fixed dates whose slots overflow the limit or stay apart, on days that date variables hold or not,
     * ranges of days, elements in any order, numeric timezones and New York, which moves its clocks back
     * on 2025-11-02 and forward on 2025-03-09, at a limit of 16 or at the default limit. It checks what a
     * user's evaluation and validate() make of each against a model:
     * <ul>
     * <li>where a merge before the tick calendar leaves more intervals than the limit, see
     * {@link #runningLimitError}, the list fails with that error, at the position of the literal;</li>
     * <li>otherwise it evaluates to the documented semantics of the own extents of its elements, see
     * {@link #model}, or fails on the limit where those leave more intervals than it;</li>
     * <li>validate() runs the same parse with the limit, as a lenient evaluation does: it fails where
     * the evaluation fails, with the same error, and merges as much.</li>
     * </ul>
     * The parser, which a bind variable takes and whose merges unite the merge ranges of the list, checks
     * the model: it parses the list without its tick calendar and duration to the union of the own
     * extents, or fails where the model fails before the tick calendar, or where that union holds more
     * intervals than the limit, and fails the whole list where the model fails before the tick calendar.
     * It counts the shapes it checks.
     */
    private static final class OverflowParseFuzzer {
        private static final String[] CALENDARS = {"XNYS", "XHKG", "X24H"};
        // at the default limit: the slots of fixed dates that overflow it alone, and fewer, which stay apart
        private static final Slots[] DEFAULT_APART = {
                new Slots("T[00..13]:[00..59]:00", 0, 13, 1),
                new Slots("T[14..20]:[00..59]:00", 14, 20, 1),
                new Slots("T[01..03]:[00..59]:00", 1, 3, 1)
        };
        private static final Slots[] DEFAULT_OVERFLOW = {new Slots("T[00..23]:[00..59]:00", 0, 23, 1)};
        // durations, and their lengths in seconds; a list takes one at least as long as the time between
        // the slots of its fixed dates that overflow, so that their windows join in its IR
        private static final String[] DURATIONS = {"1m", "10m", "30m", "1h", "90m", "4h", "1d"};
        private static final int[] DURATION_SECONDS = {60, 600, 1_800, 3_600, 5_400, 14_400, 86_400};
        // New York moves its clocks back two days before 2025-11-04, and forward the day before 2025-03-10
        private static final String[] NOWS = {
                "2025-01-27T12:00:00.000000Z",
                "2025-01-24T03:00:00.000000Z",
                "2025-01-29T15:30:00.000000Z",
                "2025-11-04T12:00:00.000000Z",
                "2025-03-10T12:00:00.000000Z"
        };
        // at a limit of 16
        private static final Slots[] SMALL_APART = {
                new Slots("T[00..09]:00:00", 0, 9, 60),
                new Slots("T[14..20]:00:00", 14, 20, 60),
                new Slots("T[10..12]:[00,20,40]:00", 10, 12, 20)
        };
        private static final Slots[] SMALL_OVERFLOW = {
                new Slots("T[00..23]:00:00", 0, 23, 60),
                new Slots("T[06..21]:[00,30]:00", 6, 21, 30)
        };
        // date variables of whole days, and their first and last day after today
        private static final String[] VARIABLES = {"$today", "$yesterday", "$tomorrow", "$today-3d..$today", "$yesterday..$tomorrow"};
        private static final int[][] VARIABLE_DAYS = {{0, 0}, {-1, -1}, {1, 1}, {-3, 0}, {-1, 1}};
        // the timezones of the list, and their rules
        private static final String[] ZONES = {"", "@+08:00", "@-05:00", "@America/New_York"};
        private static final ZoneId[] ZONE_IDS = {ZoneOffset.UTC, ZoneOffset.ofHours(8), ZoneOffset.ofHours(-5), ZoneId.of("America/New_York")};
        private final ObjList<Element> elements = new ObjList<>();
        private final LongList extents = new LongList();
        // the minutes of the day of the slots of a fixed date that runningLimitError() has not converted yet
        private final IntList pendingSlots = new IntList();
        private final StringBuilder sb = new StringBuilder();
        private final CairoConfiguration smallLimits = calendarConfiguration(8, 16);
        // the shapes that the fuzzer checked
        private int defaultLimitFailureCount;
        private int defaultLimitFitCount;
        private int earlierAfterLaterCount;
        private int finalLimitCount;
        private int fitApartCount;
        private int fitCount;
        private int fixedDateAfterRangeCount;
        private int microCount;
        private int namedZoneCount;
        private int nanoCount;
        private int rangeLimitCount;
        private int repeatedHourCount;
        private int runningLimitCount;
        private int skippedHourCount;
        private int twoOverflowCount;
        private TimestampDriver driver;
        private boolean hasRepeatedHour;
        private boolean hasSkippedHour;

        void assertEveryShape() {
            Assert.assertTrue("a list that fits the limit", fitCount > 0);
            Assert.assertTrue("a list that fits with as many intervals apart as the merge threshold", fitApartCount > 0);
            Assert.assertTrue("a list that fits the default limit", defaultLimitFitCount > 0);
            Assert.assertTrue("a list that fails before the tick calendar", runningLimitCount > 0);
            Assert.assertTrue("a list that fails the default limit before the tick calendar", defaultLimitFailureCount > 0);
            Assert.assertTrue("a fixed date after a range", fixedDateAfterRangeCount > 0);
            Assert.assertTrue("two fixed dates that overflow the limit", twoOverflowCount > 0);
            Assert.assertTrue("an element whose days lie before those of an element before it", earlierAfterLaterCount > 0);
            Assert.assertTrue("a timezone of the list with clock changes", namedZoneCount > 0);
            Assert.assertTrue("slots in an hour that the clocks repeat", repeatedHourCount > 0);
            Assert.assertTrue("slots in an hour that the clocks skip", skippedHourCount > 0);
            Assert.assertTrue("a TIMESTAMP list", microCount > 0);
            Assert.assertTrue("a TIMESTAMP_NS list", nanoCount > 0);
        }

        void check(Rnd rnd) throws SqlException {
            driver = ColumnType.getTimestampDriver(rnd.nextBoolean() ? ColumnType.TIMESTAMP_MICRO : ColumnType.TIMESTAMP_NANO);
            // a tenth of the lists in New York, two days after it moves its clocks back or the day after it
            // moves them forward, with a fixed date on the day of the change
            final boolean isClockChange = rnd.nextInt(10) == 0;
            final String nowText = isClockChange ? NOWS[3 + rnd.nextInt(2)] : NOWS[rnd.nextInt(NOWS.length)];
            final long now = driver.parseFloorLiteral(nowText);
            // a date variable takes the date of "now" in UTC, which the timezone of the list reads as a
            // local date
            final LocalDate today = LocalDate.parse(nowText.substring(0, 10));
            final String calendar = CALENDARS[rnd.nextInt(CALENDARS.length)];
            final boolean isDefaultLimit = rnd.nextInt(5) == 0;
            final int threshold = isDefaultLimit ? 256 : 8;
            final int maxIntervals = isDefaultLimit ? 1024 : 16;
            final int zoneIndex = isClockChange ? ZONES.length - 1 : rnd.nextInt(ZONES.length);
            final ZoneId zone = ZONE_IDS[zoneIndex];

            // one or two fixed dates whose slots alone overflow the limit, on days that a date variable can
            // hold, the longest time between their slots, fixed dates whose slots stay apart, and date
            // variables, in any order
            elements.clear();
            final int overflowCount = rnd.nextInt(4) == 0 ? 2 : 1;
            int overflowSpacingSeconds = 0;
            for (int i = 0; i < overflowCount; i++) {
                final Slots[] choices = isDefaultLimit ? DEFAULT_OVERFLOW : SMALL_OVERFLOW;
                // the slots of each hour on the day of a clock change
                final Slots slots = isClockChange && i == 0 ? choices[0] : choices[rnd.nextInt(choices.length)];
                final LocalDate day = isClockChange && i == 0 ? today.minusDays(today.getMonthValue() == 11 ? 2 : 1) : today.plusDays(rnd.nextInt(5) - 3);
                elements.add(new Element(day + slots.text(), day, day, slots, true));
                overflowSpacingSeconds = Math.max(overflowSpacingSeconds, slots.minuteStep() * 60);
            }
            for (int i = 0, n = rnd.nextInt(3); i < n; i++) {
                final Slots[] choices = isDefaultLimit ? DEFAULT_APART : SMALL_APART;
                final Slots slots = choices[rnd.nextInt(choices.length)];
                final LocalDate day = today.plusDays(rnd.nextInt(7) - 4);
                elements.add(new Element(day + slots.text(), day, day, slots, false));
            }
            for (int i = 0, n = rnd.nextInt(3); i < n; i++) {
                final int v = rnd.nextInt(VARIABLES.length);
                elements.add(new Element(VARIABLES[v], today.plusDays(VARIABLE_DAYS[v][0]), today.plusDays(VARIABLE_DAYS[v][1]), null, false));
            }
            for (int i = elements.size() - 1; i > 0; i--) {
                final int j = rnd.nextInt(i + 1);
                final Element element = elements.getQuick(i);
                elements.setQuick(i, elements.getQuick(j));
                elements.setQuick(j, element);
            }
            // half the time, a date variable that holds a fixed date that overflows comes before it, which
            // lets the parse fit the limit
            for (int i = 0; i < elements.size(); i++) {
                final Element element = elements.getQuick(i);
                if (element.isOverflow() && rnd.nextBoolean()) {
                    final long dayAfterToday = element.first().toEpochDay() - today.toEpochDay();
                    int v;
                    do {
                        v = rnd.nextInt(VARIABLES.length);
                    } while (dayAfterToday < VARIABLE_DAYS[v][0] || dayAfterToday > VARIABLE_DAYS[v][1]);
                    elements.insert(rnd.nextInt(i + 1), 1, new Element(VARIABLES[v], today.plusDays(VARIABLE_DAYS[v][0]), today.plusDays(VARIABLE_DAYS[v][1]), null, false));
                    i++;
                }
            }
            int durationIndex = 0;
            while (DURATION_SECONDS[durationIndex] < overflowSpacingSeconds) {
                durationIndex++;
            }
            durationIndex += rnd.nextInt(DURATIONS.length - durationIndex);

            sb.setLength(0);
            sb.append('[');
            extents.clear();
            hasRepeatedHour = false;
            hasSkippedHour = false;
            boolean hasRange = false;
            boolean isEarlierAfterLater = false;
            boolean isFixedDateAfterRange = false;
            for (int i = 0, n = elements.size(); i < n; i++) {
                final Element element = elements.getQuick(i);
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append(element.text());
                if (element.slots() == null) {
                    for (LocalDate day = element.first(); !day.isAfter(element.last()); day = day.plusDays(1)) {
                        addDay(day, zone, extents);
                    }
                } else {
                    for (int hour = element.slots().firstHour(); hour <= element.slots().lastHour(); hour++) {
                        for (int minute = 0; minute < 60; minute += element.slots().minuteStep()) {
                            addSlot(element.first(), hour * 60 + minute, zone, extents);
                        }
                    }
                    isFixedDateAfterRange |= hasRange;
                }
                hasRange |= element.isRange();
                for (int j = 0; j < i; j++) {
                    isEarlierAfterLater |= element.first().isBefore(elements.getQuick(j).last());
                }
            }
            sb.append(']').append(ZONES[zoneIndex]);
            final String withoutCalendar = sb.toString();
            final String text = sb.append('#').append(calendar).append(';').append(DURATIONS[durationIndex]).toString();
            final String context = text + " at " + nowText + " [" + ColumnType.nameOf(driver.getTimestampType()) + (isDefaultLimit ? "]" : ", limit 16]");

            final String limitError = "error: Bracket expansion produces too many intervals (max " + maxIntervals + ") @100";
            final LongList united = union(extents);
            final String runningError = runningLimitError(zone, zoneIndex > 0, threshold, maxIntervals);
            final String expected = runningError != null ? runningError : model(united, calendar, driver.fromSeconds(DURATION_SECONDS[durationIndex]), maxIntervals, limitError);
            final CairoConfiguration limits = isDefaultLimit ? configuration : smallLimits;
            // the parser checks the model
            final String unitedOrError = united.size() / 2 > maxIntervals ? limitError : intervalToString(driver, united);
            Assert.assertEquals(context + " without its tick calendar and duration", runningError != null ? runningError : unitedOrError, parse(limits, withoutCalendar, now));
            if (runningError != null) {
                Assert.assertEquals(context + " as a bind variable", runningError, parse(limits, text, now));
            }

            try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(driver, limits, text, 0, text.length(), 100)) {
                Assert.assertTrue(context, compiled.isParsedOnEvaluation());
                final long before = IntervalUtils.getStaticOwnExtentMergeIntervalCountForTesting();
                final LongList out = new LongList();
                String evaluated;
                try {
                    compiled.evaluate(out, now);
                    evaluated = intervalToString(driver, out);
                } catch (CairoException e) {
                    evaluated = "error: " + e.getFlyweightMessage() + " @" + e.getPosition();
                }
                final long evaluateMergeCount = IntervalUtils.getStaticOwnExtentMergeIntervalCountForTesting() - before;
                String validated = null;
                try {
                    compiled.validate(now);
                } catch (SqlException e) {
                    validated = "error: " + e.getFlyweightMessage() + " @" + e.getPosition();
                }
                Assert.assertEquals(context + " merges of validate()", evaluateMergeCount, IntervalUtils.getStaticOwnExtentMergeIntervalCountForTesting() - before - evaluateMergeCount);
                Assert.assertEquals(context, expected, evaluated);
                Assert.assertEquals(context + " validate()", isError(evaluated) ? evaluated : null, validated);
            }

            if (runningError != null) {
                runningLimitCount++;
                rangeLimitCount += runningError.startsWith("error: Date range expansion") ? 1 : 0;
                defaultLimitFailureCount += isDefaultLimit ? 1 : 0;
            } else if (isError(expected)) {
                finalLimitCount++;
            } else {
                fitCount++;
                fitApartCount += united.size() / 2 >= threshold ? 1 : 0;
                defaultLimitFitCount += isDefaultLimit ? 1 : 0;
            }
            fixedDateAfterRangeCount += isFixedDateAfterRange ? 1 : 0;
            twoOverflowCount += overflowCount == 2 ? 1 : 0;
            earlierAfterLaterCount += isEarlierAfterLater ? 1 : 0;
            namedZoneCount += zone instanceof ZoneOffset ? 0 : 1;
            repeatedHourCount += hasRepeatedHour ? 1 : 0;
            skippedHourCount += hasSkippedHour ? 1 : 0;
            microCount += ColumnType.isTimestampNano(driver.getTimestampType()) ? 0 : 1;
            nanoCount += ColumnType.isTimestampNano(driver.getTimestampType()) ? 1 : 0;
        }

        void log() {
            LOG.info().$("compiled static date overflow parse fuzz [fit=").$(fitCount)
                    .$(", fitApart=").$(fitApartCount)
                    .$(", defaultLimitFit=").$(defaultLimitFitCount)
                    .$(", runningLimit=").$(runningLimitCount)
                    .$(", defaultLimitFailure=").$(defaultLimitFailureCount)
                    .$(", rangeLimit=").$(rangeLimitCount)
                    .$(", finalLimit=").$(finalLimitCount)
                    .$(", fixedDateAfterRange=").$(fixedDateAfterRangeCount)
                    .$(", twoOverflow=").$(twoOverflowCount)
                    .$(", earlierAfterLater=").$(earlierAfterLaterCount)
                    .$(", namedZone=").$(namedZoneCount)
                    .$(", repeatedHour=").$(repeatedHourCount)
                    .$(", skippedHour=").$(skippedHourCount)
                    .$(", micro=").$(microCount)
                    .$(", nano=").$(nanoCount)
                    .I$();
        }

        private static boolean isError(String result) {
            return result.startsWith("error: ");
        }

        /**
         * Appends the own extent of a day of a date variable, which the timezone of the list reads as a
         * local date.
         */
        private void addDay(LocalDate day, ZoneId zone, LongList out) {
            out.add(
                    driver.fromSeconds(day.atStartOfDay(zone).toEpochSecond()),
                    driver.fromSeconds(day.plusDays(1).atStartOfDay(zone).toEpochSecond()) - 1
            );
        }

        /**
         * Appends the own extents of the slot at the minute of the day, at second 00: the second from each
         * instant whose local time it is, none where the clocks skip it, two where they repeat it, and
         * returns how many.
         */
        private int addSlot(LocalDate day, int minuteOfDay, ZoneId zone, LongList out) {
            final LocalDateTime local = day.atTime(minuteOfDay / 60, minuteOfDay % 60);
            final List<ZoneOffset> offsets = zone.getRules().getValidOffsets(local);
            hasRepeatedHour |= offsets.size() == 2;
            hasSkippedHour |= offsets.isEmpty();
            for (int i = 0, n = offsets.size(); i < n; i++) {
                final long lo = driver.fromSeconds(local.toEpochSecond(offsets.get(i)));
                out.add(lo, lo + driver.fromSeconds(1) - 1);
            }
            return offsets.size();
        }

        /**
         * Returns the documented semantics of a list whose own extents unite to the intervals: cut by the
         * sessions of the calendar into pieces as long as the sessions hold them, each piece's end extended
         * by the duration, and merged, or the error of the limit where that leaves more intervals than it.
         */
        private String model(LongList united, String calendar, long duration, int maxIntervals, String limitError) {
            final LongList kept = union(intersect(united, sessions(driver, calendar)));
            for (int i = 1, n = kept.size(); i < n; i += 2) {
                kept.setQuick(i, kept.getQuick(i) + duration);
            }
            final LongList merged = union(kept);
            return merged.size() / 2 > maxIntervals ? limitError : intervalToString(driver, merged);
        }

        /**
         * Returns the intervals that the parser parses the expression to, as it parses a bind variable at
         * position 100, or its error with its position.
         */
        private String parse(CairoConfiguration limits, String expression, long now) {
            final LongList out = new LongList();
            try {
                IntervalUtils.parseTickExpr(driver, limits, expression, 0, expression.length(), 100, out, sink, now, IntervalUtils.TICK_CHECK_ALL);
                return intervalToString(driver, out);
            } catch (SqlException e) {
                return "error: " + e.getFlyweightMessage() + " @" + e.getPosition();
            }
        }

        /**
         * Returns the error of the interval limit that the parse of the elements raises before the tick
         * calendar, with its position, or null. The parse unions a merge range once it grew by the merge
         * threshold since its last union, and fails where the range then holds more intervals than the
         * limit: the merge range of the list, or, with a timezone, one of each fixed date's own. A fixed
         * date counts a slot as one until a merge or the end of the date converts it to UTC, and its
         * outermost bracket merges once more after its last slot when its range holds the threshold or
         * more intervals. After each element other than a range of days, the list merges once it grew
         * by the threshold since its last union. A range of days merges the list after each day once the
         * list holds the threshold or more intervals, and a day converts at once.
         */
        private String runningLimitError(ZoneId zone, boolean isZoned, int threshold, int maxIntervals) {
            final String rangeError = "error: Date range expansion produces too many intervals (max " + maxIntervals + ") @100";
            final String bracketError = "error: Bracket expansion produces too many intervals (max " + maxIntervals + ") @100";
            final LongList list = new LongList();
            // the intervals of the list after its last union
            int listMergedLim = 0;
            for (int i = 0, n = elements.size(); i < n; i++) {
                final Element element = elements.getQuick(i);
                if (element.slots() == null) {
                    for (LocalDate day = element.first(); !day.isAfter(element.last()); day = day.plusDays(1)) {
                        addDay(day, zone, list);
                        if (element.isRange() && list.size() / 2 >= threshold) {
                            unionFrom(list, 0);
                            listMergedLim = list.size() / 2;
                            if (listMergedLim > maxIntervals) {
                                return rangeError;
                            }
                        }
                    }
                    if (element.isRange()) {
                        continue;
                    }
                } else {
                    // the merge range of the fixed date: its own with a timezone, else the list's
                    final int rangeBase = isZoned ? list.size() / 2 : 0;
                    int rangeMergedLim = isZoned ? rangeBase : listMergedLim;
                    pendingSlots.clear();
                    for (int hour = element.slots().firstHour(); hour <= element.slots().lastHour(); hour++) {
                        for (int minute = 0; minute < 60; minute += element.slots().minuteStep()) {
                            pendingSlots.add(hour * 60 + minute);
                            if (list.size() / 2 + pendingSlots.size() - rangeMergedLim >= threshold) {
                                convertPendingSlots(element, zone, list);
                                unionFrom(list, rangeBase);
                                rangeMergedLim = list.size() / 2;
                                if (rangeMergedLim - rangeBase > maxIntervals) {
                                    return bracketError;
                                }
                            }
                        }
                    }
                    if (pendingSlots.size() > 0 && list.size() / 2 + pendingSlots.size() - rangeBase >= threshold) {
                        convertPendingSlots(element, zone, list);
                        unionFrom(list, rangeBase);
                        rangeMergedLim = list.size() / 2;
                        if (rangeMergedLim - rangeBase > maxIntervals) {
                            return bracketError;
                        }
                    }
                    // the end of the date converts the rest
                    convertPendingSlots(element, zone, list);
                    if (!isZoned) {
                        listMergedLim = rangeMergedLim;
                    }
                }
                if (list.size() / 2 - listMergedLim >= threshold) {
                    unionFrom(list, 0);
                    listMergedLim = list.size() / 2;
                    if (listMergedLim > maxIntervals) {
                        return bracketError;
                    }
                }
            }
            return null;
        }

        // Appends the own extents of the slots that wait for their conversion to UTC, see addSlot().
        private void convertPendingSlots(Element element, ZoneId zone, LongList list) {
            for (int k = 0, m = pendingSlots.size(); k < m; k++) {
                addSlot(element.first(), pendingSlots.getQuick(k), zone, list);
            }
            pendingSlots.clear();
        }

        // Unions the intervals of the list from interval index lo on, as the parser unions a merge range.
        private void unionFrom(LongList list, int lo) {
            final LongList range = new LongList();
            range.add(list, 2 * lo, list.size());
            final LongList united = union(range);
            list.setPos(2 * lo);
            list.add(united);
        }

        // an element of a list: a fixed date with its slots, which may overflow the limit alone, or a date
        // variable of whole days without them
        private record Element(String text, LocalDate first, LocalDate last, Slots slots, boolean isOverflow) {
            boolean isRange() {
                return slots == null && text.contains("..");
            }
        }

        // the slots of a fixed date, as its text writes them: second 00 of every minuteStep-th minute of
        // each hour from the first to the last
        private record Slots(String text, int firstHour, int lastHour, int minuteStep) {
        }
    }

    /**
     * Generates compiled lists of fixed dates and date variables with a tick calendar, mostly with a
     * duration, and checks what the compiled form makes of each as a user's query evaluates and
     * validates it:
     * <ul>
     * <li>a list that fails to compile fails as the same list with the day filter #Fri does, which
     * compiles its fixed dates the same way, unless the interval limit fails it;</li>
     * <li>a list with a fixed date whose only ';' starts its duration evaluates, where the model takes
     * its elements, to the documented semantics of their own extents, see {@link #model}, and, where the
     * parser reads it alike, to what the parser makes of it for a bind variable, which fails where it
     * fails, with the same error; a fixed date whose own extents alone exceed the limit makes it parse
     * as the bind variable;</li>
     * <li>a list without a duration of its own evaluates to what the parser makes of it without the
     * tick calendar, cut by the sessions, and a list of date variables to what the parser makes of
     * it;</li>
     * <li>validate() fails only where the evaluation fails, on the interval limit.</li>
     * </ul>
     * The parser differs where it fails on a time that it reads only with a duration, such as
     * 15:00:00UTC, and where it counts the own extents of a bracketed fixed date with the intervals of
     * the elements before it, which the compiled form counts apart. Dates that hold one another unite
     * before the tick calendar cuts them in both. It counts the shapes it checks.
     */
    private static final class StaticDateListFuzzer {
        private static final String[] CALENDARS = {"XNYS", "XHKG", "X24H"};
        // the date variable ranges of whole days, and their days relative to today
        private static final String[] DAY_RANGES = {"$today-3d..$today", "$yesterday..$tomorrow", "$today..$today+2d", "$today-20d..$today"};
        private static final int[][] DAY_RANGE_DAYS = {{-3, 0}, {-1, 1}, {0, 2}, {-20, 0}};
        // the date variables of a whole day, and their days relative to today
        private static final String[] DAY_VARIABLES = {"$today", "$yesterday", "$tomorrow", "$today - 3d", "$today + 1d"};
        private static final int[] DAY_VARIABLE_DAYS = {0, -1, 1, -3, 1};
        // the days of fixed dates; X24H trades on the last day of 1969, in 2099, and across the DST changes
        private static final String[] DAYS = {"2025-01-20", "2025-01-23", "2025-01-24", "2025-01-25", "2025-01-27", "2025-01-28"};
        // durations of a fixed length, and their lengths in seconds
        private static final String[] DURATIONS = {"30s", "1m", "10m", "30m", "1h", "90m", "4h", "1d", "25h", "1h30m", "2h-1h", "0h", "", "-30m", "-1h"};
        private static final long[] DURATION_SECONDS = {30, 60, 600, 1_800, 3_600, 5_400, 14_400, 86_400, 90_000, 5_400, 3_600, 0, 0, -1_800, -3_600};
        private static final String[] CALENDAR_UNIT_DURATIONS = {"1M", "1y"};
        private static final String[] MALFORMED_DURATIONS = {"1x", "h", "1h;1d;2", "1"};
        private static final String[] NOWS = {
                "2025-01-27T12:00:00.000000Z", // a Monday, before the XNYS session
                "2025-01-27T15:30:00.000000Z", // in the XNYS session
                "2025-01-24T03:00:00.000000Z", // a Friday, in the morning XHKG session
                "2025-01-25T12:00:00.000000Z", // a Saturday
                "2025-01-20T12:00:00.000000Z", // a Monday that XNYS closes for
                "2025-03-10T12:00:00.000000Z", // the Monday after New York moves to summer time
                "2025-11-04T12:00:00.000000Z" // two days after New York moves back
        };
        // fixed dates that the model does not take, which the parser reads alike only as whole days
        private static final String[] OTHER_FIXED_DATES = {
                "2025-W04-5", "2025-01-24T[09:30@America/New_York]", "2025-01T[15:00@-05:00]", "2025-[01..02]T15:00:00-05",
                "2025-01-24T[15:00@GMT]", "2025-01-24T15:00:00.1234Z", "2025-03-09T[02:30@America/New_York]", "2025-11-02T01:30"
        };
        private static final String[] OTHER_VARIABLES = {"$now", "$now - 1h..$now + 1h", "$today..$today+2bd", "$today + 870m..$now"};
        // fixed dates with a ';' of their own: the duration of the date in its text, a repeating interval,
        // a ';' in a bracket, which the parser does not read, and one in the timezone of a time slot
        private static final String[] OWN_SEMICOLON_DATES = {
                "2025-01-24T15:00;1h", "2025-01-23T14:00;2h", "2025-01-24;1d", "2025-01-24;30m;2d;2", "2025-01-24T[15:00;1h]",
                "2025-01-24T[15:00@-05:00;1h]"
        };
        // the windows of those dates in a list without a duration: the day, the start and the length in
        // seconds, and the days to a repetition of the first window, where the time slot with the zone
        // -05:00 starts at 20:00Z
        private static final String[] OWN_SEMICOLON_DAYS = {"2025-01-24", "2025-01-23", "2025-01-24", "2025-01-24", "2025-01-24", "2025-01-24"};
        private static final long[][] OWN_SEMICOLON_WINDOWS = {
                {15 * 3_600, 3_600, 0}, {14 * 3_600, 7_200, 0}, {0, 86_400, 0}, {0, 1_800, 2}, {15 * 3_600, 3_600, 0}, {20 * 3_600, 60, 0}
        };
        private static final String RANGE_ORDER_ERROR = "error: Invalid date range: start is after end";
        // points in time with a zone of their own, which the timezone of the list would move again
        private static final String[] POINTS = {"15:00:00Z", "17:00:00Z", "02:00:00Z", "15:00:00+01:00", "15:00:00-05:00", "15:00:00UTC"};
        private static final String[] SLOTS = {"15:00", "16:00", "15:30", "15:00:30", "20:30", "02:00", "15", "15:00@UTC", "16:30@+01:00", "10:00@-05:00", "15:00@+00:00"};
        private static final String[] TIMES = {"15", "15:00", "15:30", "15:00:30", "20:30", "14:15", "02:00", "23:30"};
        private static final String[] X24H_DAYS = {"1969-12-31", "2099-01-02", "2025-03-09", "2025-11-02"};
        // the timezones of the list, and their offsets in minutes; the model takes no named one
        private static final String[] ZONES = {"", "", "@+08:00", "@-05:00", "@America/New_York"};
        private static final int[] ZONE_MINUTES = {0, 0, 480, -300, 0};
        private final LongList extents = new LongList();
        private final StringBuilder sb = new StringBuilder();
        private final CairoConfiguration smallLimits = calendarConfiguration(8, 16);
        // the shapes that the fuzzer checked
        private int bindAlikeCount;
        private int calendarUnitDurationCount;
        private int compileErrorCount;
        private int dateRangeLimitCount;
        private int dateVariableOnlyCount;
        private int emptyDurationCount;
        private int finalLimitCount;
        private int irCount;
        private int malformedDurationCount;
        private int modelCount;
        private int negativeDurationCount;
        private int nestedCount;
        private int ownSemicolonCount;
        private int otherSemicolonCount;
        private int overflowBridgedCount;
        private int overflowErrorCount;
        private int pastOrFutureCount;
        private int rangeOrderErrorCount;
        private int separateCountCount;
        private int slotZoneCount;
        private int slotZoneUnderListZoneCount;
        private int withoutDurationCount;
        private int zeroDurationCount;
        private String calendar;
        private TimestampDriver driver;
        private long durationUnits;
        // the list has a bracketed fixed date after another element, which the parser counts with it
        private boolean hasBracketedFixedDateAfterElement;
        private boolean hasFixedDate;
        private boolean hasListDuration;
        private boolean hasOwnSemicolon;
        // a ';' of a fixed date in a bracket, which the parser does not read without a tick calendar either
        private boolean hasOwnSemicolonInBracket;
        private boolean hasSlotZone;
        private boolean isBindAlike;
        private boolean isDurationModelled;
        private boolean isModelled;
        private boolean isSmallLimit;
        private long now;
        private String nowText;
        private String text;
        private String zone;
        private long zoneOffset;

        void assertEveryShape() {
            Assert.assertTrue("a list whose fixed dates keep their own extent in its IR", irCount > 0);
            Assert.assertTrue("a modelled list", modelCount > 0);
            Assert.assertTrue("a list that the parser reads alike", bindAlikeCount > 0);
            Assert.assertTrue("fixed dates that hold one another", nestedCount > 0);
            Assert.assertTrue("a fixed date that parses as the bind variable where an earlier element holds it", overflowBridgedCount > 0);
            Assert.assertTrue("a fixed date that parses as the bind variable and fails", overflowErrorCount > 0);
            Assert.assertTrue("a failure of the check after a day of a range", dateRangeLimitCount > 0);
            Assert.assertTrue("a failure of the check at the end", finalLimitCount > 0);
            // The parser counts a bracketed fixed date with the elements before it only where a merge
            // after the range grew by the merge threshold finds more intervals than the limit, which
            // random lists seldom reach. testCompiledStaticDateIntervalLimit pins that case.
            Assert.assertTrue("a range that starts after it ends", rangeOrderErrorCount > 0);
            Assert.assertTrue("a time slot with a timezone of its own", slotZoneCount > 0);
            Assert.assertTrue("a time slot with a timezone of its own under a timezone of the list", slotZoneUnderListZoneCount > 0);
            Assert.assertTrue("an empty duration", emptyDurationCount > 0);
            Assert.assertTrue("a zero duration", zeroDurationCount > 0);
            Assert.assertTrue("a negative duration", negativeDurationCount > 0);
            Assert.assertTrue("a duration in months or years", calendarUnitDurationCount > 0);
            Assert.assertTrue("a duration that does not parse", malformedDurationCount > 0);
            Assert.assertTrue("a list without a duration of its own", withoutDurationCount > 0);
            Assert.assertTrue("a fixed date with a ';' of its own in a list without a duration", ownSemicolonCount > 0);
            Assert.assertTrue("a fixed date with a ';' of its own beside the duration of the list", otherSemicolonCount > 0);
            Assert.assertTrue("a list of date variables only", dateVariableOnlyCount > 0);
            Assert.assertTrue("a fixed date before 1970 or in 2099", pastOrFutureCount > 0);
            Assert.assertTrue("a compile error", compileErrorCount > 0);
        }

        void check(Rnd rnd) throws SqlException {
            generate(rnd);
            final CairoConfiguration limitConfiguration = isSmallLimit ? smallLimits : configuration;
            final String context = text + " at " + nowText + " [" + ColumnType.nameOf(driver.getTimestampType()) + (isSmallLimit ? ", limit 16]" : "]");
            final CompiledTickExpression compiled;
            try {
                compiled = IntervalUtils.compileTickExpr(driver, limitConfiguration, text, 0, text.length(), 0);
            } catch (SqlException e) {
                compileErrorCount++;
                final String error = e.getFlyweightMessage().toString();
                // the static intervals of the list exceed the limit at the end of the list, which the
                // compiler checks
                finalLimitCount += error.contains(" produces too many intervals ") ? 1 : 0;
                if (!error.contains(" produces too many intervals ")) {
                    // the compiled form compiles the fixed dates of the same list with a day filter the same way
                    final String dayFilter = text.replace('#' + calendar, "#Fri");
                    try (CompiledTickExpression ignore = IntervalUtils.compileTickExpr(driver, limitConfiguration, dayFilter, 0, dayFilter.length(), 0)) {
                        Assert.fail(context + " compiles as " + dayFilter + " where it fails with " + error);
                    } catch (SqlException dayFilterError) {
                        Assert.assertEquals(context + " as " + dayFilter, error, dayFilterError.getFlyweightMessage().toString());
                    }
                }
                otherSemicolonCount += hasListDuration && hasOwnSemicolon ? 1 : 0;
                return;
            }
            try {
                final LongList out = new LongList();
                String evaluated;
                try {
                    compiled.evaluate(out, now);
                    evaluated = intervalToString(driver, out);
                } catch (CairoException e) {
                    evaluated = "error: " + e.getFlyweightMessage();
                }
                String validated = null;
                try {
                    compiled.validate(now);
                } catch (SqlException e) {
                    validated = "error: " + e.getFlyweightMessage();
                }
                if (validated == null) {
                    // validate() runs the interval limit as the evaluation does, but not the range order check
                    Assert.assertFalse(context + ": " + evaluated, isIntervalLimitError(evaluated));
                } else {
                    Assert.assertTrue(context + ": " + evaluated + " where validate() fails with " + validated, evaluated.equals(validated) || evaluated.equals(RANGE_ORDER_ERROR));
                }
                rangeOrderErrorCount += evaluated.equals(RANGE_ORDER_ERROR) ? 1 : 0;
                final String bind = parseTickExpr(limitConfiguration, driver, text, now, IntervalUtils.TICK_CHECK_ALL);
                if (!hasListDuration) {
                    withoutDurationCount++;
                    checkWithoutDuration(context, evaluated);
                } else if (!hasFixedDate) {
                    dateVariableOnlyCount++;
                    // the compiled form keeps its code for a list without a fixed date
                    Assert.assertEquals(context + " as a bind variable", bind, evaluated);
                } else if (hasOwnSemicolon) {
                    // a ';' in the timezone of a time slot, which reads as the offset up to it
                    otherSemicolonCount++;
                } else {
                    checkOwnExtents(context, compiled.isParsedOnEvaluation(), evaluated, bind);
                }
            } finally {
                Misc.free(compiled);
            }
        }

        void log() {
            LOG.info().$("compiled static date fuzz [irs=").$(irCount)
                    .$(", modelled=").$(modelCount)
                    .$(", bindAlike=").$(bindAlikeCount)
                    .$(", nested=").$(nestedCount)
                    .$(", overflowBridged=").$(overflowBridgedCount)
                    .$(", overflowErrors=").$(overflowErrorCount)
                    .$(", dateRangeLimits=").$(dateRangeLimitCount)
                    .$(", finalLimits=").$(finalLimitCount)
                    .$(", separateCounts=").$(separateCountCount)
                    .$(", rangeOrderErrors=").$(rangeOrderErrorCount)
                    .$(", slotZones=").$(slotZoneCount)
                    .$(", slotZonesUnderListZone=").$(slotZoneUnderListZoneCount)
                    .$(", emptyDurations=").$(emptyDurationCount)
                    .$(", zeroDurations=").$(zeroDurationCount)
                    .$(", negativeDurations=").$(negativeDurationCount)
                    .$(", calendarUnitDurations=").$(calendarUnitDurationCount)
                    .$(", malformedDurations=").$(malformedDurationCount)
                    .$(", withoutDuration=").$(withoutDurationCount)
                    .$(", ownSemicolons=").$(ownSemicolonCount)
                    .$(", otherSemicolons=").$(otherSemicolonCount)
                    .$(", dateVariablesOnly=").$(dateVariableOnlyCount)
                    .$(", pastOrFuture=").$(pastOrFutureCount)
                    .$(", compileErrors=").$(compileErrorCount)
                    .I$();
        }

        /**
         * Returns whether any two of the intervals overlap so that, sorted by their starts and ends, one
         * ends before one before it ends, which the tick calendar must unite before it cuts them.
         */
        private static boolean isNested(LongList intervals) {
            final long[][] pairs = new long[intervals.size() / 2][];
            for (int i = 0; i < pairs.length; i++) {
                pairs[i] = new long[]{intervals.getQuick(2 * i), intervals.getQuick(2 * i + 1)};
            }
            Arrays.sort(pairs, (a, b) -> a[0] != b[0] ? Long.compare(a[0], b[0]) : Long.compare(a[1], b[1]));
            long maxHi = Long.MIN_VALUE;
            for (long[] pair : pairs) {
                if (pair[1] < maxHi) {
                    return true;
                }
                maxHi = Math.max(maxHi, pair[1]);
            }
            return false;
        }

        private void addDays(long firstDay, int dayCount, String time) {
            for (int day = 0; day < dayCount; day++) {
                final long localDay = firstDay + driver.fromDays(day);
                if (time == null) {
                    extents.add(localDay - zoneOffset, localDay - zoneOffset + driver.fromDays(1) - 1);
                } else {
                    addSlotExtent(driver, localDay, time, zoneOffset, extents);
                }
            }
        }

        /**
         * Appends an element of the kind, or a random one where it is -1, to the list, and its own
         * extents to the model.
         */
        private void appendElement(Rnd rnd, String listTime, boolean isFirst, int forcedKind) {
            // a date variable takes the date of "now" in UTC, which the timezone of the list then reads
            // as a local date, in both forms
            final long localToday = driver.startOfDay(now, 0);
            final String[] days = calendar.equals("X24H") && rnd.nextBoolean() ? X24H_DAYS : DAYS;
            final String dayText = days[rnd.nextInt(days.length)];
            pastOrFutureCount += dayText.startsWith("1969") || dayText.startsWith("2099") ? 1 : 0;
            final long day = driver.parseFloorLiteral(dayText + "T00:00:00.000000Z");
            // with a time part of the list, only whole days, which take it
            final int kind = forcedKind >= 0 ? forcedKind : listTime.isEmpty() ? rnd.nextInt(12) : (rnd.nextInt(3) == 0 ? 1 : rnd.nextBoolean() ? 0 : 3);
            switch (kind) {
                case 0 -> {
                    final int i = rnd.nextInt(DAY_VARIABLES.length);
                    sb.append(DAY_VARIABLES[i]);
                    addDays(localToday + driver.fromDays(DAY_VARIABLE_DAYS[i]), 1, listTime.isEmpty() ? null : "15:00");
                }
                case 1 -> {
                    final int i = rnd.nextInt(DAY_RANGES.length);
                    sb.append(DAY_RANGES[i]);
                    addDays(localToday + driver.fromDays(DAY_RANGE_DAYS[i][0]), DAY_RANGE_DAYS[i][1] - DAY_RANGE_DAYS[i][0] + 1, listTime.isEmpty() ? null : "15:00");
                }
                case 2 -> {
                    sb.append(OTHER_VARIABLES[rnd.nextInt(OTHER_VARIABLES.length)]);
                    isModelled = false;
                }
                case 3 -> {
                    hasFixedDate = true;
                    sb.append(dayText);
                    addDays(day, 1, listTime.isEmpty() ? null : "15:00");
                }
                case 4 -> {
                    hasFixedDate = true;
                    final String time = TIMES[rnd.nextInt(TIMES.length)];
                    sb.append(dayText).append('T').append(time);
                    addSlotExtent(driver, day, time, zoneOffset, extents);
                }
                case 5 -> {
                    hasFixedDate = true;
                    if (zoneOffset == 0 && isModelled) {
                        final String point = POINTS[rnd.nextInt(POINTS.length)];
                        sb.append(dayText).append('T').append(point);
                        addSlotExtent(driver, day, point, 0, extents);
                        isBindAlike &= !point.endsWith("UTC");
                    } else {
                        sb.append(dayText).append("T15:00:00Z");
                        isModelled = false;
                    }
                }
                case 6 -> {
                    hasFixedDate = true;
                    hasBracketedFixedDateAfterElement |= !isFirst;
                    sb.append(dayText).append("T[");
                    for (int i = 0, n = 1 + rnd.nextInt(3); i < n; i++) {
                        final String slot = SLOTS[rnd.nextInt(SLOTS.length)];
                        if (i > 0) {
                            sb.append(',');
                        }
                        sb.append(slot);
                        addSlotExtent(driver, day, slot, zoneOffset, extents);
                        if (slot.indexOf('@') >= 0) {
                            hasSlotZone = true;
                        }
                    }
                    sb.append(']');
                }
                case 7 -> {
                    // two whole days, or their minute at 15:00
                    hasFixedDate = true;
                    hasBracketedFixedDateAfterElement |= !isFirst;
                    final boolean isTimed = rnd.nextBoolean();
                    sb.append("2025-01-[23..24]").append(isTimed ? "T15:00" : "");
                    addDays(driver.parseFloorLiteral("2025-01-23T00:00:00.000000Z"), 2, isTimed ? "15:00" : null);
                }
                case 8, 9 -> {
                    // the seconds at the hours of a day, or the minutes from 14:00 to 20:30 every half hour
                    hasFixedDate = true;
                    hasBracketedFixedDateAfterElement |= !isFirst;
                    if (kind == 8) {
                        sb.append(dayText).append("T[00..23]:00:00");
                        for (int hour = 0; hour < 24; hour++) {
                            addSlotExtent(driver, day, (hour < 10 ? "0" : "") + hour + ":00:00", zoneOffset, extents);
                        }
                    } else {
                        sb.append(dayText).append("T[14..20]:[00,30]");
                        for (int hour = 14; hour <= 20; hour++) {
                            addSlotExtent(driver, day, hour + ":00", zoneOffset, extents);
                            addSlotExtent(driver, day, hour + ":30", zoneOffset, extents);
                        }
                    }
                }
                case 10 -> {
                    hasFixedDate = true;
                    final String date = OTHER_FIXED_DATES[rnd.nextInt(OTHER_FIXED_DATES.length)];
                    sb.append(date);
                    isModelled = false;
                    // the parser reads 15:00:00.1234Z as 15:00:00.234 with a duration only, see
                    // bug-duration-date-parse-any-format-misreads
                    isBindAlike &= !date.endsWith(".1234Z");
                    hasBracketedFixedDateAfterElement |= !isFirst && date.indexOf('[') >= 0;
                    hasSlotZone |= date.indexOf('@') >= 0;
                }
                default -> {
                    // the extents are the windows of the date, which tell whether the date holds another
                    hasFixedDate = true;
                    hasOwnSemicolon = true;
                    final int i = rnd.nextInt(OWN_SEMICOLON_DATES.length);
                    sb.append(OWN_SEMICOLON_DATES[i]);
                    hasOwnSemicolonInBracket |= OWN_SEMICOLON_DATES[i].indexOf('[') >= 0;
                    hasSlotZone |= OWN_SEMICOLON_DATES[i].indexOf('@') >= 0;
                    final long windowDay = driver.parseFloorLiteral(OWN_SEMICOLON_DAYS[i] + "T00:00:00.000000Z");
                    final long[] window = OWN_SEMICOLON_WINDOWS[i];
                    for (long repeat = 0; repeat <= window[2]; repeat += Math.max(window[2], 1)) {
                        final long lo = windowDay + driver.fromDays((int) repeat) + driver.fromSeconds(window[0]) - (OWN_SEMICOLON_DATES[i].contains("@-05:00") ? 0 : zoneOffset);
                        extents.add(lo, lo + driver.fromSeconds(window[1]) - 1);
                    }
                }
            }
        }

        /**
         * Checks a list with a fixed date whose only ';' starts its duration, whose fixed dates keep
         * their own extent: the model, the parser where it reads the list alike, and the errors.
         */
        private void checkOwnExtents(String context, boolean isParsedOnEvaluation, String evaluated, String bind) {
            irCount += isParsedOnEvaluation ? 0 : 1;
            final boolean isNested = isModelled && isNested(extents);
            nestedCount += isNested ? 1 : 0;
            final boolean isAlike = isBindAlike;
            if (evaluated.startsWith("error: ")) {
                if (isParsedOnEvaluation) {
                    overflowErrorCount++;
                }
                dateRangeLimitCount += evaluated.startsWith("error: Date range expansion") ? 1 : 0;
                finalLimitCount += !isParsedOnEvaluation && evaluated.startsWith("error: Bracket expansion") ? 1 : 0;
                // the compiled form fails where the parser fails, with the same error, unless the parser
                // counted a bracketed fixed date with the elements before it, and failed first
                if (isAlike && (isParsedOnEvaluation || !hasBracketedFixedDateAfterElement)) {
                    Assert.assertEquals(context + " as a bind variable", evaluated, bind);
                }
                return;
            }
            if (isParsedOnEvaluation) {
                overflowBridgedCount++;
            }
            if (isModelled && isDurationModelled) {
                modelCount++;
                Assert.assertEquals(context + " by the model", model(), evaluated);
            }
            if (isAlike) {
                if (isIntervalLimitError(bind)) {
                    // the parser counts a bracketed fixed date with the elements before it
                    Assert.assertTrue(context + ": " + bind, hasBracketedFixedDateAfterElement && !isParsedOnEvaluation);
                    separateCountCount++;
                } else {
                    bindAlikeCount++;
                    Assert.assertEquals(context + " as a bind variable", bind, evaluated);
                }
            }
        }

        /**
         * Checks a list without a duration of its own, which compiles as it always has: where the fixed
         * dates have no ';' in a bracket, and no time slot with a timezone of its own has one of the
         * list too, it evaluates to what the parser makes of it without the tick calendar, cut by the
         * sessions.
         */
        private void checkWithoutDuration(String context, String evaluated) {
            ownSemicolonCount += hasOwnSemicolon ? 1 : 0;
            final boolean isS9 = isBindAlike
                    && !hasOwnSemicolonInBracket
                    && !(hasSlotZone && !zone.isEmpty());
            if (isS9 && !evaluated.startsWith("error: ")) {
                final String withoutCalendar = text.replace('#' + calendar, "");
                final LongList out = new LongList();
                try {
                    IntervalUtils.parseTickExpr(driver, configuration, withoutCalendar, 0, withoutCalendar.length(), 0, out, sink, now, IntervalUtils.TICK_CHECK_ALL);
                } catch (SqlException e) {
                    return;
                }
                Assert.assertEquals(context + " without its tick calendar, cut by the sessions", intervalToString(driver, union(intersect(out, sessions(driver, calendar)))), evaluated);
            }
        }

        private void generate(Rnd rnd) {
            driver = ColumnType.getTimestampDriver(rnd.nextBoolean() ? ColumnType.TIMESTAMP_MICRO : ColumnType.TIMESTAMP_NANO);
            nowText = NOWS[rnd.nextInt(NOWS.length)];
            now = driver.parseFloorLiteral(nowText);
            calendar = CALENDARS[rnd.nextInt(CALENDARS.length)];
            isSmallLimit = rnd.nextInt(3) == 0;
            final int zoneIndex = rnd.nextInt(ZONES.length);
            zone = ZONES[zoneIndex];
            zoneOffset = driver.fromMinutes(ZONE_MINUTES[zoneIndex]);
            isModelled = !zone.equals("@America/New_York");
            isBindAlike = true;
            hasFixedDate = false;
            hasOwnSemicolon = false;
            hasOwnSemicolonInBracket = false;
            hasSlotZone = false;
            hasBracketedFixedDateAfterElement = false;
            extents.clear();
            sb.setLength(0);
            sb.append('[');
            // a duration of DURATIONS that a scenario takes, or -1
            int forcedDuration = -1;
            final String listTime;
            final int scenario = rnd.nextInt(20);
            if (scenario == 0) {
                // At a limit of 16, three minutes and the 14 of a bracketed fixed date after them: the
                // parser counts the 17 together, and the compiled form the 14 apart
                isSmallLimit = true;
                listTime = "";
                sb.append("2025-01-23T[15:00,16:00,20:30], ");
                addSlotExtent(driver, driver.parseFloorLiteral("2025-01-23T00:00:00.000000Z"), "15:00", zoneOffset, extents);
                addSlotExtent(driver, driver.parseFloorLiteral("2025-01-23T00:00:00.000000Z"), "16:00", zoneOffset, extents);
                addSlotExtent(driver, driver.parseFloorLiteral("2025-01-23T00:00:00.000000Z"), "20:30", zoneOffset, extents);
                appendElement(rnd, listTime, false, 9);
            } else if (scenario == 1) {
                // At a limit of 16, the 24 seconds at the hours of today, which today holds
                isSmallLimit = true;
                listTime = "";
                hasFixedDate = true;
                final String todayText = nowText.substring(0, 10);
                sb.append("$today, ").append(todayText).append("T[00..23]:00:00");
                final long today = driver.startOfDay(now, 0);
                addDays(today, 1, null);
                for (int hour = 0; hour < 24; hour++) {
                    addSlotExtent(driver, today, (hour < 10 ? "0" : "") + hour + ":00:00", zoneOffset, extents);
                }
                hasBracketedFixedDateAfterElement = true;
            } else if (scenario == 2) {
                // At a limit of 16, the minute at 15:00 of a fixed date and of the 21 days of a range,
                // which stay apart, past the limit after a day of the range
                isSmallLimit = true;
                listTime = "T15:00";
                hasFixedDate = true;
                sb.append("2025-01-24, $today-20d..$today");
                addDays(driver.parseFloorLiteral("2025-01-24T00:00:00.000000Z"), 1, "15:00");
                addDays(driver.startOfDay(now, 0) - driver.fromDays(20), 21, "15:00");
            } else if (scenario == 3) {
                // At a limit of 16, the minutes of three fixed dates, 18 that XNYS trades on, each 30
                // seconds longer, past the limit at the end
                isSmallLimit = true;
                listTime = "";
                calendar = "XNYS";
                zone = "";
                zoneOffset = 0;
                isModelled = true;
                hasFixedDate = true;
                forcedDuration = 0;
                sb.append("2025-01-24T[14..20]:[00,30], 2025-01-23T[15:00,16:00,20:30], 2025-01-27T[15:00,16:00]");
                final long january23 = driver.parseFloorLiteral("2025-01-23T00:00:00.000000Z");
                for (int hour = 14; hour <= 20; hour++) {
                    addSlotExtent(driver, january23 + driver.fromDays(1), hour + ":00", 0, extents);
                    addSlotExtent(driver, january23 + driver.fromDays(1), hour + ":30", 0, extents);
                }
                for (String slot : new String[]{"15:00", "16:00", "20:30"}) {
                    addSlotExtent(driver, january23, slot, 0, extents);
                }
                for (String slot : new String[]{"15:00", "16:00"}) {
                    addSlotExtent(driver, january23 + driver.fromDays(4), slot, 0, extents);
                }
                hasBracketedFixedDateAfterElement = true;
            } else {
                // the time part of the list, for whole days only
                listTime = rnd.nextInt(6) == 0 ? "T15:00" : "";
                for (int i = 0, n = 1 + rnd.nextInt(4); i < n; i++) {
                    if (i > 0) {
                        sb.append(", ");
                    }
                    appendElement(rnd, listTime, i == 0, -1);
                }
            }
            sb.append(']').append(listTime).append(zone).append('#').append(calendar);
            slotZoneCount += hasSlotZone ? 1 : 0;
            slotZoneUnderListZoneCount += hasSlotZone && !zone.isEmpty() ? 1 : 0;
            // the duration of the list, which a list with a fixed date with a ';' of its own mostly goes without
            final int durationKind = rnd.nextInt(12);
            isDurationModelled = false;
            durationUnits = 0;
            if (forcedDuration >= 0 || durationKind < 11 && !(hasOwnSemicolon && rnd.nextInt(4) != 0)) {
                final String duration;
                if (forcedDuration >= 0 || durationKind < 8) {
                    final int i = forcedDuration >= 0 ? forcedDuration : rnd.nextInt(DURATIONS.length);
                    duration = DURATIONS[i];
                    durationUnits = driver.fromSeconds(DURATION_SECONDS[i]);
                    isDurationModelled = true;
                    emptyDurationCount += duration.isEmpty() ? 1 : 0;
                    zeroDurationCount += !duration.isEmpty() && DURATION_SECONDS[i] == 0 ? 1 : 0;
                    negativeDurationCount += DURATION_SECONDS[i] < 0 ? 1 : 0;
                } else if (durationKind < 10) {
                    duration = CALENDAR_UNIT_DURATIONS[rnd.nextInt(CALENDAR_UNIT_DURATIONS.length)];
                    calendarUnitDurationCount++;
                } else {
                    duration = MALFORMED_DURATIONS[rnd.nextInt(MALFORMED_DURATIONS.length)];
                    malformedDurationCount++;
                }
                sb.append(';').append(duration);
                hasListDuration = true;
            } else {
                hasListDuration = false;
            }
            text = sb.toString();
        }

        /**
         * Returns the documented semantics of the list: the own extents of its elements, united, cut by
         * the sessions of the calendar into pieces as long as the sessions hold them, each piece's end
         * extended by the duration, and merged.
         */
        private String model() {
            final LongList kept = union(intersect(union(extents), sessions(driver, calendar)));
            for (int i = 1, n = kept.size(); i < n; i += 2) {
                kept.setQuick(i, kept.getQuick(i) + durationUnits);
            }
            return intervalToString(driver, union(kept));
        }
    }
}
