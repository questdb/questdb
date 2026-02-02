/*******************************************************************************
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
import io.questdb.cairo.ExchangeCalendarServiceFactory;
import io.questdb.cairo.TimestampDriver;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.model.IntervalOperation;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.LongList;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.StringSink;
import io.questdb.test.cairo.TestExchangeCalendarService;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for TICK expression parsing with exchange calendars.
 * Uses TestExchangeCalendarService with hardcoded XNYS and XHKG schedules for 2025.
 * <p>
 * 2025 Calendar Reference (used throughout):
 * - Jan 20 (Mon), 21 (Tue), 22 (Wed), 23 (Thu), 24 (Fri), 25 (Sat), 26 (Sun)
 * - Jan 27 (Mon), 28 (Tue), 29 (Wed), 30 (Thu), 31 (Fri), Feb 1 (Sat), Feb 2 (Sun)
 * - Feb 3 (Mon), 4 (Tue), 5 (Wed), 6 (Thu), 7 (Fri), 8 (Sat), 9 (Sun)
 */
public class TickExprTest {

    private static final StringSink sink = new StringSink();
    private static CairoConfiguration configuration;

    @BeforeClass
    public static void setUp() {
        configuration = new DefaultCairoConfiguration(System.getProperty("java.io.tmpdir")) {
            private final FactoryProvider factoryProvider = new DefaultFactoryProvider() {
                @Override
                public @NotNull ExchangeCalendarServiceFactory getExchangeCalendarServiceFactory() {
                    return () -> TestExchangeCalendarService.INSTANCE;
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
        // Tests microsecond-precision boundaries of the binary search in applyExchangeCalendarFilter.
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
    public void testHongKongDateRange() throws SqlException {
        // Date range with XHKG - each trading day produces two intervals
        // 2025-02-[03..04] - Mon, Tue
        assertTickInterval(
                "[{lo=2025-02-03T01:30:00.000000Z, hi=2025-02-03T03:59:59.999999Z}," +
                        "{lo=2025-02-03T05:00:00.000000Z, hi=2025-02-03T07:59:59.999999Z}," +
                        "{lo=2025-02-04T01:30:00.000000Z, hi=2025-02-04T03:59:59.999999Z}," +
                        "{lo=2025-02-04T05:00:00.000000Z, hi=2025-02-04T07:59:59.999999Z}]",
                "2025-02-[03..04]#XHKG"
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
        // 2025-01-24 (Fri) is a trading day
        assertTickInterval(
                "[{lo=2025-01-24T01:30:00.000000Z, hi=2025-01-24T03:59:59.999999Z}," +
                        "{lo=2025-01-24T05:00:00.000000Z, hi=2025-01-24T07:59:59.999999Z}]",
                "2025-01-24#XHKG"
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
        // in applyExchangeCalendarFilter's binary search.
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
        // Explicit date list with exchange calendar - Mon and Wed
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
        // XNYS (NYSE) has no lunch break: 14:30-21:00 UTC
        // 2025-01-24 is Friday
        assertTickInterval(
                "[{lo=2025-01-24T14:30:00.000000Z, hi=2025-01-24T20:59:59.999999Z}]",
                "2025-01-24#XNYS"
        );
    }

    @Test
    public void testNyseWithDateRange() throws SqlException {
        // Date range with exchange calendar
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
}
