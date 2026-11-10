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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.TickCalendarServiceFactory;
import io.questdb.cairo.TimestampDriver;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.model.CompiledTickExpression;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.LongList;
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

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Predicate;

/**
 * Tests for TICK expression parsing with tick calendars.
 * Uses TestTickCalendarService with hardcoded XNYS and XHKG schedules for 2025,
 * covering regular trading weeks, bank holidays, and early closes for both exchanges.
 */
public class TickCalendarTest {

    private static final Log LOG = LogFactory.getLog(TickCalendarTest.class);
    private static final String OVER_CAP_ERROR = "Bracket expansion produces too many intervals (max 1024)";
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
}
