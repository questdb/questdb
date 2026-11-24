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
import io.questdb.cairo.TickCalendarServiceFactory;
import io.questdb.cairo.TimestampDriver;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.model.CompiledTickExpression;
import io.questdb.griffin.model.IntervalOperation;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
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
import java.time.LocalDate;
import java.util.Arrays;

/**
 * Tests for TICK expression parsing with tick calendars.
 * Uses TestTickCalendarService with hardcoded XNYS and XHKG schedules for 2025,
 * covering regular trading weeks, bank holidays, and early closes for both exchanges.
 */
public class TickCalendarTest {

    // a Friday that XNYS trades on from 14:30 to 21:00Z
    private static final String FRIDAY_NOON = "2025-01-24T12:00:00.000000Z";
    private static final Log LOG = LogFactory.getLog(TickCalendarTest.class);
    private static final StringSink sink = new StringSink();
    private static CairoConfiguration configuration;

    @BeforeClass
    public static void setUp() {
        configuration = new DefaultCairoConfiguration(System.getProperty("java.io.tmpdir")) {
            private final FactoryProvider factoryProvider = new DefaultFactoryProvider() {
                @Override
                public @NotNull TickCalendarServiceFactory getTickCalendarServiceFactory() {
                    return () -> TestTickCalendarService.INSTANCE;
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
                IntervalUtils.parseTickExpr(driver, configuration, inverted, 0, inverted.length(), 0, new LongList(), IntervalOperation.INTERSECT, sink, true, friday, IntervalUtils.TICK_CHECK_ALL);
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
                    IntervalUtils.parseTickExpr(driver, configuration, withoutCalendar, 0, withoutCalendar.length(), 0, window, IntervalOperation.INTERSECT, sink, true, friday, IntervalUtils.TICK_CHECK_ALL);
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
        // as its date list spelling does, and with a tick calendar as its IR evaluates it, unless its
        // time slots hold one another, which the date list and the IR lose the end of
        // (bug-nested-slots-tick-calendar), or it has a time that only a duration lets the parser
        // read. With a tick calendar and a duration of a fixed length, it parses to what
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
                    // slots that hold one another, and a time that only a duration lets the parser read
                    final boolean isNested = timeParts[t].equals("T[15:00,15:00:30]");
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
                                        if (!isNested && !isReadOnlyWithDuration) {
                                            Assert.assertEquals(context, parseTickExpr(driver, dateList, now), parsed);
                                        }
                                        if (isCalendar && compiled != null && !compiled.isParsedOnEvaluation() && !isNested) {
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
            // the dynamic format, which no SQL takes with a tick calendar, leaves the intervals that
            // the tick calendar keeps as pairs, unmerged, as for the date list spelling
            final String expected = forTimestampType(
                    "[{lo=2025-01-24T15:00:00.000000Z, hi=2025-01-24T16:00:59.999999Z},{lo=2025-01-24T16:00:00.000000Z, hi=2025-01-24T17:00:59.999999Z}]",
                    timestampType
            );
            for (String expression : new String[]{"$todayT[15:00,16:00]#XNYS;1h", "[$today]T[15:00,16:00]#XNYS;1h"}) {
                final LongList out = new LongList();
                IntervalUtils.parseTickExpr(driver, configuration, expression, 0, expression.length(), 0, out, IntervalOperation.INTERSECT, sink, false, friday, IntervalUtils.TICK_CHECK_ALL);
                Assert.assertEquals(expression, expected, intervalToString(driver, out));
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
                    IntervalUtils.parseTickExpr(driver, configuration, expression, 0, expression.length(), 0, out, IntervalOperation.INTERSECT, scratch, true, friday, IntervalUtils.TICK_CHECK_ALL);
                }
                long minAllocatedBytes = Long.MAX_VALUE;
                int expectedParseCount = 0;
                for (int round = 0; round < 5; round++) {
                    final long allocatedBefore = threadMXBean.getCurrentThreadAllocatedBytes();
                    for (int i = 0; i < 10_000; i++) {
                        out.clear();
                        IntervalUtils.parseTickExpr(driver, configuration, expression, 0, expression.length(), 0, out, IntervalOperation.INTERSECT, scratch, true, friday, IntervalUtils.TICK_CHECK_ALL);
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
        // the tick calendar runs, and the duration extends what it keeps once. The date list
        // spelling and the IR keep the end of the inner slot instead, see
        // bug-nested-slots-tick-calendar. On Friday 2025-01-24, XNYS trades from 14:30 to 21:00Z;
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
        IntervalUtils.parseTickExpr(driver, configuration, "2025-03#XNYS", 0, 12, 0, out, IntervalOperation.INTERSECT, sink, true);

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
        IntervalUtils.parseTickExpr(driver, configuration, "2025-03#XHKG", 0, 12, 0, out, IntervalOperation.INTERSECT, sink, true);

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
        IntervalUtils.parseTickExpr(driver, configuration, "2025#XNYS", 0, 9, 0, out, IntervalOperation.INTERSECT, sink, true);

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
                            IntervalUtils.parseTickExpr(driver, configuration, expression, 0, expression.length(), 0, out, IntervalOperation.INTERSECT, sink, true, now);
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

        IntervalUtils.parseTickExpr(driver, configuration, expr1, 0, expr1.length(), 0, out1, IntervalOperation.INTERSECT, sink, true);
        IntervalUtils.parseTickExpr(driver, configuration, expr2, 0, expr2.length(), 0, out2, IntervalOperation.INTERSECT, sink, true);

        Assert.assertEquals(intervalToString(driver, out1), intervalToString(driver, out2));
    }

    /**
     * Adds the extent of the time slot on the day: its hour, minute or second, or the point in time
     * of a time with a zone, such as 15:00:00Z, 15:00:00UTC or 15:00:00+01:00. A slot with a
     * timezone of its own, such as 15:00@UTC or 16:30@+01:00, moves by its offset, and a slot
     * without one by the offset of the expression, which callers leave at zero for a point in time.
     */
    private static void addSlotExtent(TimestampDriver driver, long day, String slot, long expressionOffset, LongList out) {
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
     * Returns intervals given at microsecond precision at the precision of the timestamp type.
     */
    private static String forTimestampType(String expected, int timestampType) {
        return ColumnType.isTimestampNano(timestampType) ? expected.replace("00Z", "00000Z").replace("99Z", "99999Z") : expected;
    }

    /**
     * Returns the parts of the sorted, disjoint intervals a that the sorted, disjoint intervals b
     * hold.
     */
    private static LongList intersect(LongList a, LongList b) {
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
            IntervalUtils.parseTickExpr(driver, configuration, expression, 0, expression.length(), 0, out, IntervalOperation.INTERSECT, sink, true, now, checks, isBareDurationPerDate);
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
            IntervalUtils.parseTickExpr(driver, configuration, expression, 0, expression.length(), 0, out, IntervalOperation.INTERSECT, sink, true, now, checks);
            return intervalToString(driver, out);
        } catch (SqlException e) {
            return "error: " + e.getFlyweightMessage();
        }
    }

    /**
     * Returns the sessions of the test calendar at the precision of the driver.
     */
    private static LongList sessions(TimestampDriver driver, String calendar) {
        final LongList schedule = TestTickCalendarService.INSTANCE.getSchedule(calendar);
        Assert.assertNotNull(calendar, schedule);
        final LongList out = new LongList(schedule.size());
        for (int i = 0, n = schedule.size(); i < n; i += 2) {
            out.add(driver.fromMicros(schedule.getQuick(i)), driver.fromMicros(schedule.getQuick(i + 1) + 1) - 1);
        }
        return out;
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
     * Returns the intervals sorted by their starts, with those that overlap or touch merged, as
     * the parser merges its result.
     */
    private static LongList union(LongList intervals) {
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

    private void assertTickInterval(String expected, String interval) throws SqlException {
        LongList out = new LongList();
        TimestampDriver driver = ColumnType.getTimestampDriver(ColumnType.TIMESTAMP);
        IntervalUtils.parseTickExpr(driver, configuration, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT, sink, true);
        Assert.assertEquals(expected, intervalToString(driver, out));
    }

    private void assertTickIntervalNanos(String expected, String interval) throws SqlException {
        LongList out = new LongList();
        TimestampDriver driver = ColumnType.getTimestampDriver(ColumnType.TIMESTAMP_NANO);
        IntervalUtils.parseTickExpr(driver, configuration, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT, sink, true);
        Assert.assertEquals(expected, intervalToString(driver, out));
    }

    private void assertTickIntervalWithNow(String expected, String interval, long nowTimestamp) throws SqlException {
        LongList out = new LongList();
        TimestampDriver driver = ColumnType.getTimestampDriver(ColumnType.TIMESTAMP);
        IntervalUtils.parseTickExpr(driver, configuration, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT, sink, true, nowTimestamp);
        Assert.assertEquals(expected, intervalToString(driver, out));
    }

    /**
     * Generates bare date variable expressions with a tick calendar and a duration, and checks what
     * the parser makes of each: what it makes of the date list spelling and what the IR evaluates,
     * the error of the same text without the tick calendar for a duration that does not parse or
     * a repeating interval, and, for days and time slots that modelBareDateVariable() models, the
     * intervals of the documented semantics. The date list differs where time slots hold one
     * another, as it loses the end of the outer slot (bug-nested-slots-tick-calendar), and where
     * only a duration lets the parser read a time, such as 15:00:00UTC, which the date list fails
     * on. It counts the shapes it checks.
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
                boolean isNested,
                boolean isReadOnlyWithDuration
        ) throws SqlException {
            if (isReadOnlyWithDuration) {
                // the date list fails on the time, which the same text without the tick calendar reads
                if (parsed.startsWith("error: ") || withoutCalendar.startsWith("error: ")) {
                    Assert.assertEquals(context, withoutCalendar, parsed);
                }
            } else if (isNested) {
                // the date list splits what it keeps of the outer slot, so it fails the interval limit
                // where the expression does, and can fail it where the expression does not
                if (isIntervalLimitError(parsed)) {
                    Assert.assertTrue(context, isIntervalLimitError(parseTickExpr(limitConfiguration, driver, toDateList(text), now, checks)));
                }
            } else {
                Assert.assertEquals(context + " as a date list", parseTickExpr(limitConfiguration, driver, toDateList(text), now, checks), parsed);
            }
            if (!isSmallLimit && checks == IntervalUtils.TICK_CHECK_ALL && !isNested) {
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
            checkDateListAndIr(driver, limitConfiguration, text, now, checks, isSmallLimit, context, parsed, withoutCalendar, isNested, isReadOnlyWithDuration);

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
            checkDateListAndIr(driver, limitConfiguration, text, now, checks, isSmallLimit, context, parsed, withoutCalendar, isNested, isReadOnlyWithDuration);

            timeListCount += timePart.startsWith("T[") ? 1 : 0;
            slotZoneCount += hasSlotZone ? 1 : 0;
            nestedCount += isNested ? 1 : 0;
            dayRangeCount += isRange ? 1 : 0;
            negativeDurationCount += MODEL_DURATION_SECONDS[durationIndex] < 0 ? 1 : 0;
            pointInTimeCount += hasPointInTime ? 1 : 0;
        }
    }
}
