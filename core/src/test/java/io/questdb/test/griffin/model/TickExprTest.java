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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.TimestampDriver;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.model.CompiledTickExpression;
import io.questdb.griffin.model.DateExpressionEvaluator;
import io.questdb.griffin.model.DateVariableExpr;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.griffin.model.RuntimeIntervalModelBuilder;
import io.questdb.griffin.model.RuntimeIntrinsicIntervalModel;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.LongList;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.StringSink;
import io.questdb.test.TestTimestampType;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.math.BigInteger;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAdjusters;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Predicate;

import static io.questdb.test.griffin.GriffinParserTestUtils.intervalToString;

@RunWith(Parameterized.class)
public class TickExprTest {
    // the end of a range of business days, which holds no day when the range starts on a weekend
    static final String FUZZ_BUSINESS_DAY_RANGE_END = "$today + 0bd";
    // timezones of an element that do not parse, which the parser and the compiled form reject
    // alike, see DateVariableDetectionChecker
    static final String[] FUZZ_MALFORMED_ELEMENT_ZONES = {"@Bogus/Zone", "@+25:00"};
    // "now" on different days, weekdays and times of day, and on days when the clocks change
    static final String[] FUZZ_NOWS = {
            "2024-01-02T10:00:00.000000Z", // a Tuesday
            "2024-01-02T15:00:00.000000Z",
            "2024-01-03T10:00:00.000000Z", // a Wednesday
            "2024-01-06T10:00:00.000000Z", // a Saturday
            "2024-02-29T23:59:00.000000Z", // the last minute of a leap day
            "2024-03-31T00:30:00.000000Z", // London springs forward at 01:00Z
            "2024-11-03T05:30:00.000000Z" // New York falls back at 06:00Z
    };
    // a day filter of an element that does not parse, which the parser and the compiled form reject
    // alike, see DateVariableDetectionChecker
    static final String FUZZ_UNKNOWN_DAY_FILTER = "#Xyz";
    // durations with a unit that the timestamp drivers do not add, which the parser and the compiled
    // form reject where the parser applies the duration
    static final String[] FUZZ_UNKNOWN_UNIT_DURATIONS = {";1x", ";1H", ";2D", ";1h1x", ";1-"};
    // day filters in every form, with the days that each holds
    private static final DayFilter[] DAY_FILTERS = {
            new DayFilter("Mon", EnumSet.of(DayOfWeek.MONDAY)),
            new DayFilter("Tue", EnumSet.of(DayOfWeek.TUESDAY)),
            new DayFilter("Wed", EnumSet.of(DayOfWeek.WEDNESDAY)),
            new DayFilter("Thu", EnumSet.of(DayOfWeek.THURSDAY)),
            new DayFilter("Fri", EnumSet.of(DayOfWeek.FRIDAY)),
            new DayFilter("Sat", EnumSet.of(DayOfWeek.SATURDAY)),
            new DayFilter("Sun", EnumSet.of(DayOfWeek.SUNDAY)),
            new DayFilter("Monday", EnumSet.of(DayOfWeek.MONDAY)),
            new DayFilter("Tuesday", EnumSet.of(DayOfWeek.TUESDAY)),
            new DayFilter("Wednesday", EnumSet.of(DayOfWeek.WEDNESDAY)),
            new DayFilter("Thursday", EnumSet.of(DayOfWeek.THURSDAY)),
            new DayFilter("Friday", EnumSet.of(DayOfWeek.FRIDAY)),
            new DayFilter("Saturday", EnumSet.of(DayOfWeek.SATURDAY)),
            new DayFilter("Sunday", EnumSet.of(DayOfWeek.SUNDAY)),
            new DayFilter("MON", EnumSet.of(DayOfWeek.MONDAY)),
            new DayFilter("mon", EnumSet.of(DayOfWeek.MONDAY)),
            new DayFilter("wd", EnumSet.range(DayOfWeek.MONDAY, DayOfWeek.FRIDAY)),
            new DayFilter("workday", EnumSet.range(DayOfWeek.MONDAY, DayOfWeek.FRIDAY)),
            new DayFilter("weekend", EnumSet.of(DayOfWeek.SATURDAY, DayOfWeek.SUNDAY)),
            new DayFilter("WEEKEND", EnumSet.of(DayOfWeek.SATURDAY, DayOfWeek.SUNDAY))
    };
    private static final String[] FUZZ_DAY_FILTERS = {"", "#Tue", "#workday", "#weekend"};
    private static final String[] FUZZ_DURATIONS = {"", ";1h", ";30m"};
    // day filters of an element after a timezone of the element
    private static final String[] FUZZ_ELEMENT_DAY_FILTERS = {"#Tue", "#workday", "#weekend", FUZZ_UNKNOWN_DAY_FILTER};
    // timezones of an element before a day filter of the element: those of the list, and offsets
    // that a timezone running on into the day filter took for UTC, or failed on
    private static final String[] FUZZ_ELEMENT_ZONES_BEFORE_DAY_FILTER = {"", "@+02:00", "@Europe/London", "@America/New_York", "@UTC+05", "@Z", "@+05"};
    // the suffixes of the long date ranges of testCompiledDateRangeMergeFuzz(): durations of none,
    // zero, which makes each interval end before it starts, less than a day back, and more than a
    // day, time slots in any order, and a timezone with days when the clocks change
    private static final String[] FUZZ_LONG_RANGE_DAY_FILTERS = {"", "", "#Sat", "#workday"};
    private static final String[] FUZZ_LONG_RANGE_DURATIONS = {"", ";1h", ";0h", ";-23h", ";25h", ";30m"};
    private static final String[] FUZZ_LONG_RANGE_TIMES = {"", "T09:00", "T01:30", "T[15:00,09:00]", "T[09:00,14:30,01:30]"};
    private static final String[] FUZZ_LONG_RANGE_ZONES = {"", "@Europe/London", "@+02:00"};
    // time parts that the grammar rejects
    private static final String[] FUZZ_MALFORMED_TIMES = {"T[25:00]", "T25:00", "t09:00", "t[09:00]", "T[", "T"};
    private static final String[] FUZZ_RANGE_ENDS = {"$tomorrow", "$today + 3d", "$now", "$NOW + 1h", "$yesterday", "2024-01-09", FUZZ_BUSINESS_DAY_RANGE_END};
    // texts after the first ';' that the parser rejects after every element, and the compiled form
    // as well: one more ';', too many, a period or count that is not a number, a count below 1, and
    // a period with a unit that does not repeat
    private static final String[] FUZZ_REJECTED_SUFFIXES = {";1h;1d", ";1h;1d;3;4", ";1h;xd;3", ";1h;1d;0", ";1h;1x;3"};
    // the repeating interval of the parser, a duration, a period and a count, which the compiled form
    // rejects, see DateVariableDetectionChecker
    private static final String FUZZ_REPEATING_SUFFIX = ";1h;1d;3";
    private static final String[] FUZZ_SHIFTS = {" + 1d", "+2h", " - 1bd", "-30m", "+1w", " - 1M", "+90s"};
    private static final String[] FUZZ_STATIC_ELEMENTS = {"2024-01-05", "2024-01-05T10:00", "2024-01", "2024-01-0[1..3]"};
    private static final String[] FUZZ_TIMES = {
            "", "T09:00", "T09", "T09:00:30", "T[09:00]", "T[09:00,14:30]", "T[09..10]:30", "T[01,02]:30",
            "T[10:30@+02:00]", "T[09:00,10:00]:[15,45]"
    };
    private static final String[] FUZZ_VARIABLES = {"$now", "$today", "$yesterday", "$tomorrow", "$NOW", "$TODAY", "$Tomorrow", "$YESTERDAY"};
    private static final String[] FUZZ_ZONES = {"", "@+02:00", "@Europe/London", "@America/New_York"};
    private static final Log LOG = LogFactory.getLog(TickExprTest.class);
    private static final String NINE_TO_TEN = "[{lo=2024-01-02T09:00:00.000000Z, hi=2024-01-02T10:00:00.000000Z}]";
    // date variable ranges that add no interval at their "now": ranges that start after they end,
    // and ranges of business days that hold no business day, with the first day of a range of
    // days, and a range of the same kind that adds intervals at 2024-01-02T10:00Z, a Tuesday
    private static final NoIntervalRange[] NO_INTERVAL_RANGES = {
            new NoIntervalRange("$tomorrow..$today", "2024-01-02T10:00:00.000000Z", "2024-01-03", true, "$today..$tomorrow"),
            // before 1970, after a leap day, on the day London springs forward, and at the end of
            // a century
            new NoIntervalRange("$tomorrow..$today", "1969-12-31T23:30:00.000000Z", "1970-01-01", true, "$today..$tomorrow"),
            new NoIntervalRange("$tomorrow..$today", "2024-02-29T23:59:00.000000Z", "2024-03-01", true, "$today..$tomorrow"),
            new NoIntervalRange("$tomorrow..$today", "2024-03-31T00:30:00.000000Z", "2024-04-01", true, "$today..$tomorrow"),
            new NoIntervalRange("$tomorrow..$today", "2099-12-31T23:59:00.000000Z", "2100-01-01", true, "$today..$tomorrow"),
            // from a day to a point in time, which starts after it ends before 04:00
            new NoIntervalRange("$today..$now - 4h", "2024-01-02T03:00:00.000000Z", "2024-01-02", true, "$today..$now"),
            new NoIntervalRange("$today..$now - 4h", "1969-12-31T03:00:00.000000Z", "1969-12-31", true, "$today..$now"),
            // from a point in time to a day, which starts after it ends from 22:00 on
            new NoIntervalRange("$now + 2h..$today", "2024-01-02T23:00:00.000000Z", "2024-01-03", true, "$now..$tomorrow"),
            // between two points in time
            new NoIntervalRange("$now..$now - 1h", "2024-01-02T10:00:00.000000Z", null, true, "$now - 1h..$now"),
            new NoIntervalRange("$today + 570m..$now", "2024-01-02T03:00:00.000000Z", null, true, "$now - 1h..$now"),
            // business days on a Saturday, on a Sunday, over a weekend, and on the Saturday after
            // a Friday
            new NoIntervalRange("$today..$today + 0bd", "2024-01-06T10:00:00.000000Z", "2024-01-06", false, "$today..$today + 0bd"),
            new NoIntervalRange("$today..$today + 0bd", "2024-01-07T10:00:00.000000Z", "2024-01-07", false, "$today..$today + 0bd"),
            new NoIntervalRange("$today..$tomorrow + 0bd", "2024-01-06T10:00:00.000000Z", "2024-01-06", false, "$today..$today + 0bd"),
            new NoIntervalRange("$tomorrow..$tomorrow + 0bd", "2024-01-05T10:00:00.000000Z", "2024-01-06", false, "$today..$today + 0bd")
    };
    // ranges between two points in time, each a single interval, with the intervals that they select
    // at 2024-01-02T10:00Z, a Tuesday; ? stands for the text after the first ';' of the expression
    private static final PointInTimeRange[] POINT_IN_TIME_RANGES = {
            new PointInTimeRange("$now - 1h..$now;?", NINE_TO_TEN, true),
            new PointInTimeRange("[$now - 1h..$now];?", NINE_TO_TEN, true),
            new PointInTimeRange("$today + 2h..$now;?", "[{lo=2024-01-02T02:00:00.000000Z, hi=2024-01-02T10:00:00.000000Z}]", true),
            new PointInTimeRange("$now..$now;?", "[{lo=2024-01-02T10:00:00.000000Z, hi=2024-01-02T10:00:00.000000Z}]", true),
            new PointInTimeRange("[$NOW - 1h..$Now + 1h];?", "[{lo=2024-01-02T09:00:00.000000Z, hi=2024-01-02T11:00:00.000000Z}]", true),
            new PointInTimeRange("[$yesterday + 30m..$now];?", "[{lo=2024-01-01T00:30:00.000000Z, hi=2024-01-02T10:00:00.000000Z}]", true),
            new PointInTimeRange(
                    "[$now - 1h..$now, $now - 3h..$now - 2h];?",
                    "[{lo=2024-01-02T07:00:00.000000Z, hi=2024-01-02T08:00:00.000000Z},{lo=2024-01-02T09:00:00.000000Z, hi=2024-01-02T10:00:00.000000Z}]",
                    true
            ),
            // 09:00 to 10:00 at +02:00
            new PointInTimeRange("[$now - 1h..$now]@+02:00;?", "[{lo=2024-01-02T07:00:00.000000Z, hi=2024-01-02T08:00:00.000000Z}]", true),
            // London keeps UTC in January
            new PointInTimeRange("[$now - 1h..$now]@Europe/London;?", NINE_TO_TEN, true),
            new PointInTimeRange("$now - 1h..$now#Tue;?", NINE_TO_TEN, true),
            // the day filter drops the range
            new PointInTimeRange("$now - 1h..$now#Mon;?", "[]", true),
            // the range ignores the time part; the IR represents neither an element with its own
            // timezone nor numeric brackets in the time, so those parse on every evaluation
            new PointInTimeRange("[$now - 1h..$now@+00:00];?", NINE_TO_TEN, false),
            new PointInTimeRange("[$now - 1h..$now]T09:00;?", NINE_TO_TEN, true),
            new PointInTimeRange("[$now - 1h..$now]T[09:00,10:00];?", NINE_TO_TEN, true),
            new PointInTimeRange("[$now - 1h..$now]T[09..10]:30;?", NINE_TO_TEN, false),
            new PointInTimeRange("  $now - 1h ..  $now ;?", NINE_TO_TEN, true)
    };
    private static final String RANGE_ORDER_ERROR = "Invalid date range: start is after end";
    private static final int[] TICK_CHECK_LEVELS = {IntervalUtils.TICK_CHECK_ALL, IntervalUtils.TICK_CHECK_INTERVAL_LIMIT, 0};
    // 2024-01-08T01:00, a Monday
    private static final long MONDAY_NOW_MICROS = Micros.toMicros(2024, 1, 8, 1, 0);
    private static final CairoConfiguration configuration = new DefaultCairoConfiguration(".");
    private static final CairoConfiguration mergeThreshold2Configuration = new DefaultCairoConfiguration(".") {
        @Override
        public int getSqlIntervalIncrementalMergeThreshold() {
            return 2;
        }
    };
    private static final CairoConfiguration mergeThreshold4Configuration = new DefaultCairoConfiguration(".") {
        @Override
        public int getSqlIntervalIncrementalMergeThreshold() {
            return 4;
        }
    };
    private static final StringSink sink = new StringSink();
    // A merge threshold and cap above the interval count of every tick string that a test parses
    // with it, so that only the final union runs
    private static final CairoConfiguration unmergedConfiguration = new DefaultCairoConfiguration(".") {
        @Override
        public int getSqlIntervalIncrementalMergeThreshold() {
            return 5_000_000;
        }

        @Override
        public int getSqlIntervalMaxIntervalsAfterMerge() {
            return 10_000_000;
        }
    };
    private final LongList a = new LongList();
    private final LongList b = new LongList();
    private final LongList out = new LongList();
    private final TestTimestampType timestampType;

    public TickExprTest(TestTimestampType timestampType) {
        this.timestampType = timestampType;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {TestTimestampType.MICRO}, {TestTimestampType.NANO}
        });
    }

    /**
     * Convenience overload that allocates a temporary StringSink.
     */
    public static void parseTickExpr(
            TimestampDriver timestampDriver,
            CharSequence seq,
            int lo,
            int lim,
            int position,
            LongList out
    ) throws SqlException {
        IntervalUtils.parseTickExpr(timestampDriver, configuration, seq, lo, lim, position, out, new StringSink());
    }

    /**
     * Overload that allows specifying nowTimestamp for testing date variables.
     */
    public static void parseTickExprWithNow(
            TimestampDriver timestampDriver,
            CharSequence seq,
            int lo,
            int lim,
            int position,
            LongList out,
            long nowTimestamp
    ) throws SqlException {
        IntervalUtils.parseTickExpr(timestampDriver, configuration, seq, lo, lim, position, out, new StringSink(), nowTimestamp);
    }

    @Before
    public void setUp() {
        a.clear();
        b.clear();
        out.clear();
    }

    @Test
    public void testBracketExpansionCartesianProduct() throws SqlException {
        // 2018-[01,06]-[10,15] should produce 4 intervals: Jan 10, Jan 15, Jun 10, Jun 15
        assertBracketInterval(
                "[{lo=2018-01-10T00:00:00.000000Z, hi=2018-01-10T23:59:59.999999Z},{lo=2018-01-15T00:00:00.000000Z, hi=2018-01-15T23:59:59.999999Z},{lo=2018-06-10T00:00:00.000000Z, hi=2018-06-10T23:59:59.999999Z},{lo=2018-06-15T00:00:00.000000Z, hi=2018-06-15T23:59:59.999999Z}]",
                "2018-[01,06]-[10,15]"
        );
    }

    /**
     * Tests cartesian product of bracket groups with incremental merging.
     * <p>
     * Multiple bracket groups create a cartesian product. For example:
     * 2018-[01,06]-[10,15] produces 2 * 2 = 4 combinations.
     * <p>
     * With incremental merging (PR #6674), large cartesian products that produce
     * adjacent intervals are merged during expansion to bound memory usage.
     *
     * @see <a href="https://github.com/questdb/questdb/pull/6674">PR #6674</a>
     */
    @Test
    public void testBracketExpansionCartesianProductWithMerge() throws SqlException {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        out.clear();

        // Test 1: Small cartesian product with non-adjacent values (no merging)
        String interval = "2018-[01,06]-[10,15]"; // 2 * 2 = 4 combinations
        parseTickExpr(timestampDriver, interval, 0, interval.length(), 0, out);
        Assert.assertEquals("Expected 4 intervals for 2x2 cartesian product", 4, out.size() / 2);

        // Test 2: Larger cartesian product with non-consecutive months
        // 2018-[01,03,05,07,09,11]-[05,10,15,20,25] = 6 months * 5 days = 30 combinations
        out.clear();
        interval = "2018-[01,03,05,07,09,11]-[05,10,15,20,25]";
        parseTickExpr(timestampDriver, interval, 0, interval.length(), 0, out);
        Assert.assertEquals("Expected 30 intervals for 6x5 cartesian product", 30, out.size() / 2);

        // Test 3: Cartesian product with adjacent days within months
        // 2020-[01,02]-[01..28] = 2 months * 28 days = 56 combinations before merging
        // Days 1-28 within each month are adjacent and merge.
        // Jan 28 -> Feb 1 has gap (Jan 29,30,31), so months don't merge.
        out.clear();
        interval = "2020-[01,02]-[01..28]";
        parseTickExpr(timestampDriver, interval, 0, interval.length(), 0, out);
        // Each month's days merge to 1 interval, total 2 (gap between months)
        Assert.assertEquals("2 months * 28 days should merge to 2 intervals", 2, out.size() / 2);
    }

    @Test
    public void testBracketExpansionErrorCountNotANumber() {
        // Invalid count in repeating interval format (exercises line 1242)
        assertBracketIntervalError("2018-01-10T10:30;30m;2d;X", "Count not a number");
    }

    @Test
    public void testBracketExpansionErrorCountNegative() {
        assertBracketIntervalError("2026-01-27T15:00;-1s;-1h;-2", "Count must be positive");
    }

    @Test
    public void testBracketExpansionErrorCountZero() {
        assertBracketIntervalError("2026-01-27T15:00;1s;1h;0", "Count must be positive");
    }

    @Test
    public void testBracketExpansionErrorDescendingRange() {
        assertBracketIntervalError("2018-01-[15..10]", "Range must be ascending");
    }

    @Test
    public void testBracketExpansionErrorDurationInvalidUnit() {
        // Invalid duration unit character
        assertBracketIntervalError("2024-01-15T10:00;1x", "Invalid duration unit");
    }

    @Test
    public void testBracketExpansionErrorDurationMissingNumber() {
        // Missing number before unit
        assertBracketIntervalError("2024-01-15T10:00;h", "Expected number before unit");
    }

    @Test
    public void testBracketExpansionErrorDurationMissingUnit() {
        // Multi-unit duration with missing unit at end
        assertBracketIntervalError("2024-01-15T10:00;1h30", "Missing unit at end of duration");
    }

    @Test
    public void testBracketExpansionErrorDurationPeriodOverflow() {
        // Duration period number overflow in addDuration
        assertBracketIntervalError("2018-01-[10,15];999999999999h", "Duration not a number");
    }

    @Test
    public void testBracketExpansionErrorEmptyBracket() {
        assertBracketIntervalError("2018-01-[]", "Empty bracket expansion");
    }

    @Test
    public void testBracketExpansionErrorInvalidDateWithDuration() {
        // Valid bracket expansion but invalid date that fails both parseInterval and parseAnyFormat
        assertBracketIntervalError("xyz-[10,15];1h", "Invalid date");
    }

    @Test
    public void testBracketExpansionErrorMissingRangeEnd() {
        assertBracketIntervalError("2018-01-[10..]", "Expected number after '..'");
    }

    @Test
    public void testBracketExpansionNegativeDuration() throws SqlException {
        // Negative duration should create a range that ends at the anchor time
        assertBracketInterval(
                "[{lo=2018-01-09T23:00:00.000000Z, hi=2018-01-09T23:59:59.999999Z}," +
                        "{lo=2018-01-14T23:00:00.000000Z, hi=2018-01-14T23:59:59.999999Z}]",
                "2018-01-[10,15]T00:00;-1h"
        );
    }

    @Test
    public void testBracketExpansionNegativeDurationAnyFormat() throws SqlException {
        assertBracketInterval(
                "[{lo=2018-01-09T23:00:00.000000Z, hi=2018-01-09T23:59:59.999999Z}," +
                        "{lo=2018-01-14T23:00:00.000000Z, hi=2018-01-14T23:59:59.999999Z}]",
                "2018-01-[10,15]T00:00:00.000000Z;-1h"
        );
    }

    @Test
    public void testBracketExpansionNegativeDurationSimpleMonth() throws SqlException {
        // Bare imprecise month expands per-day; negative duration shifts each day backwards
        // resulting in a merged window covering the previous 3 days of the first element
        // through the tail of the month (Dec 29 2025 .. Jan 30 2026)
        assertShortInterval(
                "[{lo=2025-12-29T00:00:00.000000Z, hi=2026-01-30T23:59:59.999999Z}]",
                "2026-01;-3d"
        );
    }

    @Test
    public void testBracketExpansionErrorNestedBrackets() {
        // Nested brackets - exercises depth tracking in findMatchingBracket
        assertBracketIntervalError("2018-01-[[10]]", "Expected number in bracket expansion");
    }

    @Test
    public void testBracketExpansionErrorNonNumeric() {
        assertBracketIntervalError("2018-01-[abc]", "Expected number in bracket expansion");
    }

    @Test
    public void testBracketExpansionErrorNumberOverflow() {
        // Number too large for int (Integer.MAX_VALUE is 2147483647)
        assertBracketIntervalError("2018-01-[99999999999]", "Expected number in bracket expansion");
    }

    @Test
    public void testBracketExpansionErrorPeriodNotANumber() {
        // Invalid period in repeating interval format (exercises line 1236)
        assertBracketIntervalError("2018-01-10T10:30;30m;Xd;2", "Period not a number");
    }

    @Test
    public void testBracketExpansionErrorRangeEndOverflow() {
        // Range end number too large for int
        assertBracketIntervalError("2018-01-[10..99999999999]", "Expected number after '..'");
    }

    @Test
    public void testBracketExpansionErrorSemicolonInsideBracket() {
        // Semicolon inside brackets - exercises depth check for semicolon search
        assertBracketIntervalError("2018-01-[10;15];1h", "Expected ',' or end of bracket");
    }

    @Test
    public void testBracketExpansionErrorSingleDot() {
        // Single dot (not range ..) - exercises second dot check being false
        assertBracketIntervalError("2018-01-[10.5]", "Expected ',' or end of bracket");
    }

    @Test
    public void testBracketExpansionErrorTooManyGroups() {
        // MAX_BRACKET_DEPTH is 8, so 9 bracket groups should fail
        assertBracketIntervalError(
                "[1]-[1]-[1]T[1]:[1]:[1].[1][1][1]",
                "Too many bracket groups"
        );
    }

    @Test
    public void testBracketExpansionErrorTooManySemicolons() {
        // 4 semicolons triggers "Invalid interval format" (exercises line 1192)
        assertBracketIntervalError("2018-01-10;1h;1d;2;extra", "Invalid interval format");
    }

    @Test
    public void testBracketExpansionErrorTwoSemicolons() {
        // Exactly 2 semicolons is invalid format (exercises line 1264)
        assertBracketIntervalError("2018-01-10T10:30;30m;2d", "Invalid interval format");
    }

    @Test
    public void testBracketExpansionErrorUnclosedBracket() {
        assertBracketIntervalError("2018-01-[10,15", "Unclosed '[' in interval");
    }

    @Test
    public void testBracketExpansionErrorUnknownPeriodType() {
        // Unknown period type 'w' (exercises line 1260)
        assertBracketIntervalError("2018-01-10T10:30;30m;2w;3", "Unknown period");
    }

    @Test
    public void testBracketExpansionHourMinute() throws SqlException {
        // Test bracket expansion in time part
        assertBracketInterval(
                "[{lo=2018-01-10T10:30:00.000000Z, hi=2018-01-10T10:30:59.999999Z},{lo=2018-01-10T14:30:00.000000Z, hi=2018-01-10T14:30:59.999999Z}]",
                "2018-01-10T[10,14]:30"
        );
    }

    @Test
    public void testBracketExpansionHourWithSpaceSeparator() throws SqlException {
        // Tests bracket in hour position with space separator.
        // Space separator produces point timestamps (lo=hi)
        assertBracketInterval(
                "[{lo=2018-01-10T08:30:00.000000Z, hi=2018-01-10T08:30:00.000000Z},{lo=2018-01-10T14:30:00.000000Z, hi=2018-01-10T14:30:00.000000Z}]",
                "2018-01-10 [08,14]:30"
        );
    }

    /**
     * Tests that large bracket range expansions with adjacent intervals are
     * handled efficiently through incremental merging.
     * <p>
     * The fix (PR #6674): incremental merging during expansion bounds memory usage.
     * When interval count exceeds a threshold (256), intervals are merged mid-expansion.
     * This prevents OOM from large ranges like [1..1000000] with adjacent values.
     * <p>
     * Adjacent intervals (like consecutive days) merge into one, so a range of
     * 1000 consecutive days results in 1 merged interval, not 1000.
     *
     * @see <a href="https://github.com/questdb/questdb/pull/6674">PR #6674</a>
     */
    @Test(timeout = 30000)
    public void testBracketExpansionLargeRangeWithIncrementalMerge() throws SqlException {
        // Test 1: 31 consecutive days merge to 1 interval
        assertBracketInterval(
                "[{lo=2020-01-01T00:00:00.000000Z, hi=2020-01-31T23:59:59.999999Z}]",
                "2020-01-[01..31]"
        );

        // Test 2: 201 non-adjacent year intervals (Jan 1 each year) don't merge
        final TimestampDriver timestampDriver = timestampType.getDriver();
        out.clear();
        String interval = "[1970..2170]-01-01";
        parseTickExpr(timestampDriver, interval, 0, interval.length(), 0, out);
        Assert.assertEquals("201 non-adjacent year intervals", 201, out.size() / 2);

        // Test 3: Max interval limit (1024) enforced
        assertBracketIntervalError("[1000..3000]-01-01", "too many intervals");

        // Test 4: 12 months × 28 days merge to 12 intervals (one per month)
        out.clear();
        interval = "2020-[01,02,03,04,05,06,07,08,09,10,11,12]-[01..28]";
        parseTickExpr(timestampDriver, interval, 0, interval.length(), 0, out);
        Assert.assertEquals("12 month intervals after merge", 12, out.size() / 2);
    }

    @Test
    public void testBracketExpansionMergeCountWithoutDayFilter() throws SqlException {
        // Without a day filter the incremental merge unions the range once it grew by the merge
        // threshold (256) since its last union, and the outermost bracket unions it once more after
        // its last leaf when the range holds at least 256 intervals then. It reads the cap once per
        // union, once in the -ea assert of parseTickExpr and once in the final check. The tests
        // always run with -ea.
        Assert.assertEquals(256, configuration.getSqlIntervalIncrementalMergeThreshold());
        // 21 years x 12 months x 28 days = 7,056 leaves: 27 unions by the threshold, and one after
        // the last leaf, where the 144 leaves since the last union and the 232 blocks of days that
        // it left make 376 intervals
        Assert.assertEquals(30, countMaxIntervalsGetterCalls("[2004..2024]-[01..12]-[01..28]"));
        // 21 x 12 x 4 = 1,008 one-hour leaves that never touch: 3 unions by the threshold and one
        // after the last leaf, over all 1,008
        Assert.assertEquals(6, countMaxIntervalsGetterCalls("[2004..2024]-[01..12]-[01,08,15,22]T09:00;1h"));
        // 16 days x 8 hours = 128 leaves, below the threshold: no incremental union
        Assert.assertEquals(2, countMaxIntervalsGetterCalls("2024-01-[01..16]T[00..07]:00"));
    }

    @Test
    public void testBracketExpansionMicroseconds() throws SqlException {
        // Tests bracket in microseconds part (exercises c == '.' branch)
        // No padding for microseconds field
        // Note: hi includes 999ns/999999ns to cover the full microsecond interval
        assertBracketInterval(
                "[{lo=2018-01-10T10:30:00.100000Z, hi=2018-01-10T10:30:00.100999Z},{lo=2018-01-10T10:30:00.200000Z, hi=2018-01-10T10:30:00.200999Z}]",
                "2018-01-10T10:30:00.[100,200]"
        );
    }

    @Test
    public void testBracketExpansionMixed() throws SqlException {
        // Adjacent day intervals are merged: 5, 10-12 merged, 20
        assertBracketInterval(
                "[{lo=2018-01-05T00:00:00.000000Z, hi=2018-01-05T23:59:59.999999Z},{lo=2018-01-10T00:00:00.000000Z, hi=2018-01-12T23:59:59.999999Z},{lo=2018-01-20T00:00:00.000000Z, hi=2018-01-20T23:59:59.999999Z}]",
                "2018-01-[5,10..12,20]"
        );
    }

    @Test
    public void testBracketExpansionMonth() throws SqlException {
        assertBracketInterval(
                "[{lo=2018-01-15T00:00:00.000000Z, hi=2018-01-15T23:59:59.999999Z},{lo=2018-06-15T00:00:00.000000Z, hi=2018-06-15T23:59:59.999999Z}]",
                "2018-[01,06]-15"
        );
    }

    // ================= ISO Week Date Tests =================

    @Test
    public void testBracketExpansionNoBrackets() throws SqlException {
        // Should delegate to parseInterval when no brackets
        assertBracketInterval(
                "[{lo=2018-01-10T00:00:00.000000Z, hi=2018-01-10T23:59:59.999999Z}]",
                "2018-01-10"
        );
    }

    @Test
    public void testBracketExpansionOutOfOrderValuesWithinMaxIntervals() throws SqlException {
        // The cap counts the intervals of an expansion after merging. At a merge threshold of 2 and
        // a cap of 4, the even minutes 00 to 08 stay 5 intervals apart, above the cap, and the odd
        // minutes after them join all nine minutes into one interval. The unions wait until the
        // range grew by 2 leaves since the last one: after the minutes 02, 06, 01 and 05, which
        // leave at most 4 intervals, and once more after the last leaf.
        final CairoConfiguration mergeThreshold2Cap4Configuration = new DefaultCairoConfiguration(".") {
            @Override
            public int getSqlIntervalIncrementalMergeThreshold() {
                return 2;
            }

            @Override
            public int getSqlIntervalMaxIntervalsAfterMerge() {
                return 4;
            }
        };
        final LongList expected = new LongList();
        addLocalBlock(expected, LocalDateTime.of(2024, 1, 1, 9, 0), ZoneOffset.UTC, 9 * Micros.MINUTE_MICROS);
        assertTickExprIntervals(mergeThreshold2Cap4Configuration, expected, "2024-01-01T09:[00,02,04,06,08,01,03,05,07]");
        // control: the even minutes alone exceed the cap
        assertTickExprErrorAtMonday(
                mergeThreshold2Cap4Configuration,
                "2024-01-01T09:[00,02,04,06,08]",
                "Bracket expansion produces too many intervals (max 4)"
        );
    }

    @Test
    public void testBracketExpansionRange() throws SqlException {
        // Range expands to intervals that are merged when adjacent
        assertBracketInterval(
                "[{lo=2018-01-10T00:00:00.000000Z, hi=2018-01-12T23:59:59.999999Z}]",
                "2018-01-[10..12]"
        );
    }

    @Test
    public void testBracketExpansionRangeWithWhitespace() throws SqlException {
        // Range with whitespace after .. and after range end (exercises L899 and L933)
        // Adjacent day intervals are merged into one continuous interval
        assertBracketInterval(
                "[{lo=2018-01-10T00:00:00.000000Z, hi=2018-01-12T23:59:59.999999Z}]",
                "2018-01-[10 .. 12 ]"
        );
    }

    @Test
    public void testBracketExpansionRawTimestamp() throws SqlException {
        // Raw long timestamp (exercises line 1219 - Numbers.parseLong fallback)
        // 1234567890000000 microseconds = 2009-02-13T23:31:30.000000Z
        final TimestampDriver timestampDriver = timestampType.getDriver();
        out.clear();
        String interval = "1234567890000000";
        parseTickExpr(timestampDriver, interval, 0, interval.length(), 0, out);
        Assert.assertEquals(2, out.size());
        Assert.assertEquals(1234567890000000L, out.getQuick(0));
        Assert.assertEquals(1234567890000000L, out.getQuick(1));
    }

    @Test
    public void testBracketExpansionSingleValue() throws SqlException {
        assertBracketInterval(
                "[{lo=2018-01-10T00:00:00.000000Z, hi=2018-01-10T23:59:59.999999Z}]",
                "2018-01-[10]"
        );
    }

    @Test
    public void testBracketExpansionTrailingCommaWhitespace() throws SqlException {
        // Tests whitespace loop exiting at bracket end (trailing comma with spaces)
        assertBracketInterval(
                "[{lo=2018-01-10T00:00:00.000000Z, hi=2018-01-10T23:59:59.999999Z}]",
                "2018-01-[10,   ]"
        );
    }

    @Test
    public void testBracketExpansionTwoValues() throws SqlException {
        assertBracketInterval(
                "[{lo=2018-01-10T00:00:00.000000Z, hi=2018-01-10T23:59:59.999999Z},{lo=2018-01-15T00:00:00.000000Z, hi=2018-01-15T23:59:59.999999Z}]",
                "2018-01-[10,15]"
        );
    }

    @Test
    public void testBracketExpansionWithDenseTimeSuffixAndDayFilter() throws SqlException {
        // T[00..23]:[00..59] expands the two days to 2880 minutes, beyond the incremental merge
        // threshold (256 intervals). The day filter must still judge each minute by its own day,
        // not by the day of a merged interval: 2024-01-05 is a Friday, 2024-01-06 a Saturday.
        assertBracketInterval(
                "[{lo=2024-01-05T00:00:00.000000Z, hi=2024-01-05T23:59:59.999999Z}]",
                "2024-01-[05..06]T[00..23]:[00..59]#workday"
        );
        assertBracketInterval(
                "[{lo=2024-01-05T00:00:00.000000Z, hi=2024-01-05T23:59:59.999999Z}]",
                "[2024-01-[05..06]]T[00..23]:[00..59]#workday"
        );
    }

    @Test
    public void testBracketExpansionWithDuration() throws SqlException {
        // 10:30 is minute-level, so hi = 10:30:59.999999, then +1h = 11:30:59.999999
        assertBracketInterval(
                "[{lo=2018-01-10T10:30:00.000000Z, hi=2018-01-10T11:29:59.999999Z},{lo=2018-01-15T10:30:00.000000Z, hi=2018-01-15T11:29:59.999999Z}]",
                "2018-01-[10,15]T10:30;1h"
        );
    }

    @Test
    public void testBracketExpansionWithMultiUnitDuration() throws SqlException {
        // Multi-unit duration: 1h30m = 1 hour 30 minutes
        // 10:00 + 1h30m = 11:30:59.999999
        assertBracketInterval(
                "[{lo=2024-01-15T10:00:00.000000Z, hi=2024-01-15T11:29:59.999999Z}]",
                "2024-01-15T10:00;1h30m"
        );
    }

    @Test
    public void testBracketExpansionWithMultiUnitDurationComplex() throws SqlException {
        // Complex multi-unit duration: 2h15m30s
        // 10:00:00 + 2h15m30s = 12:15:30.999999
        assertBracketInterval(
                "[{lo=2024-01-15T10:00:00.000000Z, hi=2024-01-15T12:15:29.999999Z}]",
                "2024-01-15T10:00:00;2h15m30s"
        );
    }

    @Test
    public void testBracketExpansionWithMultiUnitDurationDaysHours() throws SqlException {
        // Days and hours: 1d12h = 1 day 12 hours = 36 hours
        assertBracketInterval(
                "[{lo=2024-01-15T00:00:00.000000Z, hi=2024-01-16T11:59:59.999999Z}]",
                "2024-01-15T00:00;1d12h"
        );
    }

    @Test
    public void testBracketExpansionWithMultiUnitDurationSubSecond() throws SqlException {
        // Sub-second units: 1s500T (1 second 500 millis)
        // For exact results, use full precision for each timestamp type:
        // - MICRO: 6 digits (.000000)
        // - NANO: 9 digits (.000000000)
        // With partial precision (e.g., 6 digits for NANO), the interval includes
        // the sub-precision range (e.g., all 999 nanoseconds within that microsecond).
        String input = "2024-01-15T10:00:00.000000;1s500T";
        if (ColumnType.isTimestampNano(timestampType.getTimestampType())) {
            final TimestampDriver timestampDriver = timestampType.getDriver();
            parseTickExpr(timestampDriver, input, 0, input.length(), 0, out);
            TestUtils.assertEquals(
                    "[{lo=2024-01-15T10:00:00.000000000Z, hi=2024-01-15T10:00:01.499999999Z}]",
                    intervalToString(timestampDriver, out)
            );
        } else {
            assertBracketInterval(
                    "[{lo=2024-01-15T10:00:00.000000Z, hi=2024-01-15T10:00:01.499999Z}]",
                    input
            );
        }
    }

    @Test
    public void testBracketExpansionWithMultiUnitDurationUnderscoreNumber() throws SqlException {
        // Duration with underscore number separator: 1_500T = 1500 millis = 1.5 seconds
        assertBracketInterval(
                "[{lo=2024-01-15T10:00:00.000000Z, hi=2024-01-15T10:00:01.499999Z}]",
                "2024-01-15T10:00:00.000000;1_500T"
        );
    }

    @Test
    public void testBracketExpansionWithNonZeroOffset() throws SqlException {
        // Tests that bracket parsing works correctly when lo > 0
        // The prefix "XXX" should be ignored, and padding should be calculated from the interval start
        String input = "XXX2018-01-[5,10]";
        int lo = 3; // skip "XXX"
        out.clear();
        final TimestampDriver timestampDriver = timestampType.getDriver();
        parseTickExpr(timestampDriver, input, lo, input.length(), 0, out);
        // Day field should be zero-padded to 2 digits
        TestUtils.assertEquals(
                ColumnType.isTimestampNano(timestampType.getTimestampType())
                        ? "[{lo=2018-01-05T00:00:00.000000000Z, hi=2018-01-05T23:59:59.999999999Z},{lo=2018-01-10T00:00:00.000000000Z, hi=2018-01-10T23:59:59.999999999Z}]"
                        : "[{lo=2018-01-05T00:00:00.000000Z, hi=2018-01-05T23:59:59.999999Z},{lo=2018-01-10T00:00:00.000000Z, hi=2018-01-10T23:59:59.999999Z}]",
                intervalToString(timestampDriver, out)
        );
    }

    @Test
    public void testBracketExpansionWithRepeatingInterval() throws SqlException {
        // 2018-01-[10,15]T10:30;30m;2d;2 means 10:30-11:00:59 every second day for each expanded date
        // 10:30 is minute-level, so hi = 10:30:59.999999, then +30m = 11:00:59.999999
        assertBracketInterval(
                "[{lo=2018-01-10T10:30:00.000000Z, hi=2018-01-10T10:59:59.999999Z},{lo=2018-01-12T10:30:00.000000Z, hi=2018-01-12T10:59:59.999999Z},{lo=2018-01-15T10:30:00.000000Z, hi=2018-01-15T10:59:59.999999Z},{lo=2018-01-17T10:30:00.000000Z, hi=2018-01-17T10:59:59.999999Z}]",
                "2018-01-[10,15]T10:30;30m;2d;2"
        );
    }

    @Test
    public void testBracketExpansionWithSpaceSeparator() throws SqlException {
        // Tests space as date/time separator instead of 'T' (exercises c == ' ' branch)
        // Space separator produces point timestamps (lo=hi)
        assertBracketInterval(
                "[{lo=2018-01-10T10:30:00.000000Z, hi=2018-01-10T10:30:00.000000Z},{lo=2018-01-15T10:30:00.000000Z, hi=2018-01-15T10:30:00.000000Z}]",
                "2018-01-[10,15] 10:30"
        );
    }

    @Test
    public void testBracketExpansionWithTime() throws SqlException {
        assertBracketInterval(
                "[{lo=2018-01-10T10:30:00.000000Z, hi=2018-01-10T10:30:59.999999Z},{lo=2018-01-15T10:30:00.000000Z, hi=2018-01-15T10:30:59.999999Z}]",
                "2018-01-[10,15]T10:30"
        );
    }

    @Test
    public void testBracketExpansionWithWhitespace() throws SqlException {
        assertBracketInterval(
                "[{lo=2018-01-10T00:00:00.000000Z, hi=2018-01-10T23:59:59.999999Z},{lo=2018-01-15T00:00:00.000000Z, hi=2018-01-15T23:59:59.999999Z}]",
                "2018-01-[ 10 , 15 ]"
        );
    }

    @Test
    public void testBracketExpansionYear() throws SqlException {
        // Tests bracket in year position (exercises dashes == 0, return 0 for no padding)
        assertBracketInterval(
                "[{lo=2018-01-10T00:00:00.000000Z, hi=2018-01-10T23:59:59.999999Z},{lo=2019-01-10T00:00:00.000000Z, hi=2019-01-10T23:59:59.999999Z}]",
                "[2018,2019]-01-10"
        );
    }

    @Test
    public void testBracketExpansionZeroPaddingDay() throws SqlException {
        // Single digit day should be zero-padded
        assertBracketInterval(
                "[{lo=2018-01-05T00:00:00.000000Z, hi=2018-01-05T23:59:59.999999Z}]",
                "2018-01-[5]"
        );
    }

    @Test
    public void testBracketExpansionZeroPaddingMonth() throws SqlException {
        // Single digit month should be zero-padded
        assertBracketInterval(
                "[{lo=2018-05-01T00:00:00.000000Z, hi=2018-05-31T23:59:59.999999Z}]",
                "2018-[5]"
        );
    }

    @Test
    public void testCompiledDateRangeMergeCount() throws SqlException {
        // Once a compiled date range holds as many intervals as the merge threshold, it merges each
        // day's intervals into the merged ones so far. The days of a range come in order, so a day
        // lands after every merged interval: the merge sorts the day's intervals, compares them with
        // the last merged one, and merges them with it, and never sorts the merged ones again. The
        // count is of the intervals that the merges sort, merge with, move and compare with; the
        // first merge of an evaluation is a union of all intervals. 2025-01-24 is a Friday.
        final TimestampDriver driver = timestampType.getDriver();
        final long now = driver.parseFloorLiteral("2025-01-24T12:00:00.000000Z");
        final LocalDate today = LocalDate.of(2025, 1, 24);
        final long nine = driver.fromHours(9);
        final long hour = driver.fromHours(1);
        final long minute = driver.fromMinutes(1);
        final LongList expected = new LongList();

        // 1,001 days from 2022-04-30, with 2024-02-29: a union of the first 256, then each of 745 days
        // adds one interval and merges it with the last merged one, after one comparison
        expected.clear();
        addDayIntervals(expected, today.minusDays(1000), today, null, nine, nine + hour - 1);
        assertDateRangeMergeCount(configuration, "[$today-1000d..$today]T09:00;1h", now, expected, 256 + 745 * 3);

        // 1,000 Saturdays in 7,001 days: the other days add no interval and merge nothing
        expected.clear();
        addDayIntervals(expected, today.minusDays(7000), today, day -> day.getDayOfWeek() == DayOfWeek.SATURDAY, nine, nine + hour - 1);
        assertDateRangeMergeCount(configuration, "[$today-7000d..$today]T09:00#Sat;1h", now, expected, 256 + 744 * 3);

        // two intervals a day, in reverse order: a union after 128 days, then 373 days each sort two
        expected.clear();
        addDayIntervals(expected, today.minusDays(500), today, null, nine, nine + hour - 1, nine + 6 * hour, nine + 7 * hour - 1);
        assertDateRangeMergeCount(configuration, "[$today-500d..$today]T[15:00,09:00];1h", now, expected, 256 + 373 * 4);

        // 401 business days from Friday 2023-07-14: a range whose end is in business days skips the
        // weekends before the merge
        expected.clear();
        addDayIntervals(
                expected,
                today.minusDays(560),
                today,
                day -> day.getDayOfWeek() != DayOfWeek.SATURDAY && day.getDayOfWeek() != DayOfWeek.SUNDAY,
                nine,
                nine + minute - 1
        );
        assertDateRangeMergeCount(configuration, "[$today-560d..$today+0bd]T09:00", now, expected, 256 + 145 * 3);

        // days of 3 days that overlap: the 256 intervals of the first merge become one, then the
        // next 255 merge after the 511th day and the 766th, and the last 235 only in the final union
        expected.clear();
        expected.add(driver.fromDays((int) today.minusDays(1000).toEpochDay()) + nine, driver.fromDays((int) today.plusDays(3).toEpochDay()) + nine - 1);
        assertDateRangeMergeCount(configuration, "[$today-1000d..$today]T09:00;3d", now, expected, 256 + 2 * (255 + 1 + 1));

        // fewer intervals than the threshold merge only in the final union
        expected.clear();
        addDayIntervals(expected, today.minusDays(100), today, null, nine, nine + hour - 1);
        assertDateRangeMergeCount(configuration, "[$today-100d..$today]T09:00;1h", now, expected, 0);

        // 255, 256 and 257 days: the first merge comes with the 256th
        for (int days = 255; days <= 257; days++) {
            expected.clear();
            addDayIntervals(expected, today.minusDays(days - 1), today, null, nine, nine + hour - 1);
            assertDateRangeMergeCount(
                    configuration,
                    "[$today-" + (days - 1) + "d..$today]T09:00;1h",
                    now,
                    expected,
                    days < 256 ? 0 : 256 + (days - 256) * 3
            );
        }

        // a threshold of 8 and a limit of 16
        expected.clear();
        addDayIntervals(expected, today.minusDays(15), today, null, nine, nine + minute - 1);
        assertDateRangeMergeCount(intervalLimitConfiguration(8, 16), "[$today-15d..$today]T09:00", now, expected, 8 + 8 * 3);

        // a threshold that no range reaches: one union at the end, of the same intervals
        final CairoConfiguration noMerge = intervalLimitConfiguration(1_000_000, 2_000_000);
        expected.clear();
        addDayIntervals(expected, today.minusDays(1000), today, null, nine, nine + hour - 1);
        assertDateRangeMergeCount(noMerge, "[$today-1000d..$today]T09:00;1h", now, expected, 0);
        expected.clear();
        addDayIntervals(expected, today.minusDays(500), today.plusDays(500), null, nine, nine + hour - 1);
        assertDateRangeMergeCount(noMerge, "[$today+1d..$today+500d, $today-500d..$today]T09:00;1h", now, expected, 0);

        // before 1970, across 1968-02-29, and in 2200
        for (String day : new String[]{"1969-01-15", "2200-01-01"}) {
            final LocalDate otherToday = LocalDate.parse(day);
            expected.clear();
            addDayIntervals(expected, otherToday.minusDays(400), otherToday, null, nine, nine + hour - 1);
            assertDateRangeMergeCount(
                    configuration,
                    "[$today-400d..$today]T09:00;1h",
                    driver.parseFloorLiteral(day + "T12:00:00.000000Z"),
                    expected,
                    256 + 145 * 3
            );
        }
    }

    @Test
    public void testCompiledDateRangeMergeCountAcrossDstChange() throws SqlException {
        // 01:30 to 02:30 in London, on the 401 days to 2024-12-01: on the days the clocks go back,
        // 2023-10-29 and 2024-10-27, 01:30 to 02:00 comes twice and the day has two intervals, the
        // second time after the first merge; on 2024-03-31 the day starts at the change, at 01:00Z
        final TimestampDriver driver = timestampType.getDriver();
        final long now = driver.parseFloorLiteral("2024-12-01T12:00:00.000000Z");
        final LocalDate today = LocalDate.of(2024, 12, 1);
        final long hour = driver.fromHours(1);
        final long halfHour = driver.fromMinutes(30);
        final LocalDate springForward = LocalDate.of(2024, 3, 31);
        final LocalDate[] fallBacks = {LocalDate.of(2023, 10, 29), LocalDate.of(2024, 10, 27)};
        final LongList expected = new LongList();
        for (LocalDate day = today.minusDays(400); !day.isAfter(today); day = day.plusDays(1)) {
            final long dayStart = driver.fromDays((int) day.toEpochDay());
            if (day.equals(fallBacks[0]) || day.equals(fallBacks[1])) {
                expected.add(dayStart + halfHour, dayStart + hour - 1);
                expected.add(dayStart + hour + halfHour, dayStart + 2 * hour + halfHour - 1);
            } else if (day.equals(springForward)) {
                expected.add(dayStart + hour, dayStart + hour + halfHour - 1);
            } else if (day.isBefore(fallBacks[0]) || (day.isAfter(springForward) && day.isBefore(fallBacks[1]))) {
                // British Summer Time, UTC+1
                expected.add(dayStart + halfHour, dayStart + hour + halfHour - 1);
            } else {
                expected.add(dayStart + hour + halfHour, dayStart + 2 * hour + halfHour - 1);
            }
        }
        Assert.assertEquals(2 * 403, expected.size());
        // a union of the first 256 intervals after 255 days, to 2024-07-08, then 145 days of one
        // interval, and 2024-10-27, whose two intervals sort and merge with the last merged one
        assertDateRangeMergeCount(configuration, "[$today-400d..$today]T01:30@Europe/London;1h", now, expected, 256 + 145 * 3 + (2 + 1 + 1));
    }

    @Test
    public void testCompiledDateRangeMergeCountKeepsMergedIntervalsAcrossRanges() throws SqlException {
        // The merged intervals of a range stay merged for the next range of the list, also across a
        // range that starts after it ends, which adds no interval without the range order check, and
        // across a range between two points in time, which adds its interval without a merge. Each
        // day of the next range then merges with the last merged interval, as in one range.
        final TimestampDriver driver = timestampType.getDriver();
        final long now = driver.parseFloorLiteral("2025-01-24T12:00:00.000000Z");
        final LocalDate today = LocalDate.of(2025, 1, 24);
        final long nine = driver.fromHours(9);
        final long hour = driver.fromHours(1);
        final LongList expected = new LongList();
        addDayIntervals(expected, today.minusDays(1000), today, null, nine, nine + hour - 1);
        // 501 days, then 500 days that come after them
        assertDateRangeMergeCount(configuration, "[$today-1000d..$today-500d, $today-499d..$today]T09:00;1h", now, expected, 256 + 245 * 3 + 500 * 3);

        final String reversed = "[$today-1000d..$today-500d, $today..$yesterday, $today-499d..$today]T09:00;1h";
        final String pointInTime = "[$today-1000d..$today-500d, $now-1h..$now, $today-499d..$today]T09:00;1h";
        try (
                CompiledTickExpression reversedRange = IntervalUtils.compileTickExpr(driver, configuration, reversed, 0, reversed.length(), 7);
                CompiledTickExpression pointInTimeRange = IntervalUtils.compileTickExpr(driver, configuration, pointInTime, 0, pointInTime.length(), 7)
        ) {
            // validate() checks only the interval limit, so the range that starts after it ends adds no
            // interval there
            long countBefore = reversedRange.getRangeMergeIntervalCountForTesting();
            reversedRange.validate(now);
            Assert.assertEquals(256 + 245 * 3 + 500 * 3, reversedRange.getRangeMergeIntervalCountForTesting() - countBefore);
            out.clear();
            try {
                reversedRange.evaluate(out, now);
                Assert.fail(reversed);
            } catch (CairoException e) {
                TestUtils.assertEquals(RANGE_ORDER_ERROR, e.getFlyweightMessage());
                Assert.assertEquals(7, e.getPosition());
            }

            // the interval from 11:00 to 12:00 goes after the merged ones: the first day of the next
            // range sorts the two and merges them with the last merged one, and each day after that
            // lands before 11:00, so a binary search over the merged ones finds its place, and 11:00
            // to 12:00 moves after it; the binary searches compare 4,482 merged intervals
            final long expectedCount = 256 + 245 * 3 + (2 + 1 + 1) + 499 * 4 + 4_482;
            countBefore = pointInTimeRange.getRangeMergeIntervalCountForTesting();
            pointInTimeRange.validate(now);
            Assert.assertEquals(expectedCount, pointInTimeRange.getRangeMergeIntervalCountForTesting() - countBefore);
            final long todayStart = driver.fromDays((int) today.toEpochDay());
            expected.add(todayStart + 11 * hour, todayStart + 12 * hour);
            assertDateRangeMergeCount(pointInTimeRange, pointInTime, now, expected, expectedCount);
        }
    }

    @Test
    public void testCompiledDateRangeMergeCountWithElementsLaterInTimeFirst() throws SqlException {
        // Merged intervals of the elements before a range can lie later in time than the days of the
        // range, which then land in front of them. The merge finds the place of a day by a binary
        // search, merges it with the merged interval before that place, and moves the merged
        // intervals after it once their number before them changes. No merged interval sorts again.
        final TimestampDriver driver = timestampType.getDriver();
        final long now = driver.parseFloorLiteral("2025-01-24T12:00:00.000000Z");
        final LocalDate today = LocalDate.of(2025, 1, 24);
        final long nine = driver.fromHours(9);
        final long hour = driver.fromHours(1);
        final long minute = driver.fromMinutes(1);
        final LongList expected = new LongList();

        // 500 later days, then 501 days before them: each of these lands after its own days and before
        // the 500 later ones, which move by one interval, after a binary search of up to 10 steps:
        // the binary searches compare 4,793 merged intervals, besides 9 for the first day
        final long reachBackCount = 256 + 244 * 3 + (1 + 500 + 1 + 9) + 500 * (1 + 1 + 500 + 1) + 4_793;
        expected.clear();
        addDayIntervals(expected, today.minusDays(500), today.plusDays(500), null, nine, nine + hour - 1);
        assertDateRangeMergeCount(configuration, "[$today+1d..$today+500d, $today-500d..$today]T09:00;1h", now, expected, reachBackCount);
        // a duration of 0 makes each interval end right before it starts, and changes no merge
        expected.clear();
        addDayIntervals(expected, today.minusDays(500), today.plusDays(500), null, nine, nine - 1);
        assertDateRangeMergeCount(configuration, "[$today+1d..$today+500d, $today-500d..$today]T09:00;0h", now, expected, reachBackCount);

        // 1,000 static dates from 2200, then 10,001 days from today that merge into one interval: a
        // union of all 1,001 intervals after the first day, then each day merges with the first merged
        // one, after a comparison with the last one and a binary search of 10 steps over 1,001
        final StringBuilder hostile = new StringBuilder("[");
        final LongList staticDays = new LongList();
        for (LocalDate day = LocalDate.of(2200, 1, 1), last = day.plusDays(1998); !day.isAfter(last); day = day.plusDays(2)) {
            hostile.append(day).append(", ");
            final long dayStart = driver.fromDays((int) day.toEpochDay());
            staticDays.add(dayStart, dayStart + driver.fromDays(1) - 1);
        }
        hostile.append("$today..$today+10000d]");
        Assert.assertEquals(2000, staticDays.size());
        expected.clear();
        expected.add(driver.fromDays((int) today.toEpochDay()), driver.fromDays((int) today.plusDays(10_001).toEpochDay()) - 1);
        expected.addAll(staticDays);
        assertDateRangeMergeCount(configuration, hostile.toString(), now, expected, 1001 + 10_000 * (1 + 1 + 1 + 10));

        // a later day first: each day of the range lands before it, and moves it
        expected.clear();
        addDayIntervals(expected, today.minusDays(1000), today, null, nine, nine + minute - 1);
        addDayIntervals(expected, today.plusDays(5), today.plusDays(5), null, nine, nine + minute - 1);
        assertDateRangeMergeCount(configuration, "[$today+5d, $today-1000d..$today]T09:00", now, expected, 256 + 746 * (1 + 1 + 1 + 1) + 6_458);

        // 11 later days first: each day of the range moves them
        expected.clear();
        addDayIntervals(expected, today.minusDays(1000), today, null, nine, nine + minute - 1);
        assertDateRangeMergeCount(configuration, "[$today-10d..$today, $today-1000d..$today-11d]T09:00", now, expected, 256 + 745 * (1 + 1 + 11 + 1) + 6_906);

        // an earlier day after a merged range: the first day of the next range, which comes after the
        // merged ones, and the earlier day sort and merge with all 501 merged intervals, after a
        // binary search of 9 steps
        expected.clear();
        addDayIntervals(expected, today.minusDays(1000), today.minusDays(1000), null, nine, nine + hour - 1);
        addDayIntervals(expected, today.minusDays(500), today.plusDays(5), null, nine, nine + hour - 1);
        assertDateRangeMergeCount(
                configuration,
                "[$today-500d..$today, $today-1000d, $today+1d..$today+5d]T09:00;1h",
                now,
                expected,
                256 + 245 * 3 + (2 + 501 + 1 + 9) + 4 * 3
        );

        // a limit of 10,000: 2,000 later days, then 2,001 days that move them
        expected.clear();
        addDayIntervals(expected, today.minusDays(2000), today.plusDays(2000), null, nine, nine + hour - 1);
        assertDateRangeMergeCount(
                intervalLimitConfiguration(256, 10_000),
                "[$today+1d..$today+2000d, $today-2000d..$today]T09:00;1h",
                now,
                expected,
                256 + 1744 * 3 + (1 + 2000 + 1 + 11) + 2000 * (1 + 1 + 2000 + 1) + 23_175
        );
    }

    @Test
    public void testCompiledDateRangeMergeFuzz() throws SqlException {
        // Long date ranges next to ranges, days, static dates and points in time that lie earlier or
        // later, in any order, with time slots in any order, durations, day filters and timezones. The
        // compiled form must evaluate to what the parser parses, a union of all intervals after each
        // day, or fail with the parser's error. questdb.fuzz.tickExpressions sets the number of
        // expressions for each configuration; the parser merges all intervals after each day, which
        // makes a run of 10,000 take about a minute.
        final int expressionCount = Integer.getInteger("questdb.fuzz.tickExpressions", 1_000);
        final TimestampDriver driver = timestampType.getDriver();
        final LocalDate today = LocalDate.of(2025, 1, 24);
        final long[] nows = {driver.parseFloorLiteral("2025-01-24T12:00:00.000000Z"), driver.parseFloorLiteral("2024-10-27T00:30:00.000000Z")};
        final Rnd rnd = TestUtils.generateRandom(LOG);
        final StringSink expression = new StringSink();
        final StringSink parseSink = new StringSink();
        final LongList parsed = new LongList();
        final LongList compiled = new LongList();
        for (int[] limits : new int[][]{{256, 1024}, {8, 64}}) {
            final CairoConfiguration limitConfiguration = intervalLimitConfiguration(limits[0], limits[1]);
            final LongRangeShapes shapes = new LongRangeShapes();
            int irCount = 0;
            int limitErrorCount = 0;
            for (int i = 0; i < expressionCount; i++) {
                expression.clear();
                appendRandomLongRangeTickExpr(rnd, expression, today, limits[0], shapes);
                final String text = expression.toString();
                try (CompiledTickExpression compiledExpr = IntervalUtils.compileTickExpr(driver, limitConfiguration, text, 0, text.length(), 0)) {
                    irCount += compiledExpr.isParsedOnEvaluation() ? 0 : 1;
                    for (long now : nows) {
                        parsed.clear();
                        String parsedText;
                        try {
                            IntervalUtils.parseTickExpr(driver, limitConfiguration, text, 0, text.length(), 0, parsed, parseSink, now);
                            parsedText = intervalToString(driver, parsed).toString();
                        } catch (SqlException e) {
                            parsedText = "error: " + e.getFlyweightMessage();
                        }
                        compiled.clear();
                        String compiledText;
                        try {
                            compiledExpr.evaluate(compiled, now);
                            compiledText = intervalToString(driver, compiled).toString();
                        } catch (CairoException e) {
                            compiledText = "error: " + e.getFlyweightMessage();
                            if (Chars.startsWith(e.getFlyweightMessage(), "Date range expansion produces too many intervals")) {
                                limitErrorCount++;
                            }
                        }
                        if (!parsedText.equals(compiledText)) {
                            sink.clear();
                            driver.append(sink, now);
                            Assert.fail(text + " at " + sink + " [mergeThreshold=" + limits[0] + ", maxIntervals=" + limits[1]
                                    + "]\nparsed:   " + parsedText + "\ncompiled: " + compiledText);
                        }
                    }
                } catch (SqlException e) {
                    Assert.fail(text + " does not compile: " + e.getFlyweightMessage());
                }
            }
            LOG.info().$("long date range fuzz [mergeThreshold=").$(limits[0])
                    .$(", maxIntervals=").$(limits[1])
                    .$(", expressions=").$(expressionCount)
                    .$(", ir=").$(irCount)
                    .$(", limitErrors=").$(limitErrorCount)
                    .$(", laterElementBeforeRange=").$(shapes.laterElementBeforeRangeCount)
                    .$(", earlierElementAfterMergedRange=").$(shapes.earlierElementAfterMergedRangeCount)
                    .$(", emptyDaysAfterFirstMerge=").$(shapes.emptyDaysAfterFirstMergeCount)
                    .$(", dstSplitAfterFirstMerge=").$(shapes.dstSplitAfterFirstMergeCount)
                    .I$();
            if (expressionCount >= 1_000) {
                Assert.assertTrue(irCount > 0);
                Assert.assertTrue(limitErrorCount > 0);
                Assert.assertTrue(shapes.laterElementBeforeRangeCount > 0);
                Assert.assertTrue(shapes.earlierElementAfterMergedRangeCount > 0);
                Assert.assertTrue(shapes.emptyDaysAfterFirstMergeCount > 0);
                Assert.assertTrue(shapes.dstSplitAfterFirstMergeCount > 0);
            }
        }
    }

    @Test
    public void testCompiledDateRangeMergeKeepsLimitError() throws SqlException {
        // A merge after a day of a date range fails on more merged intervals than the limit, after
        // the same day as a union of all intervals, and leaves the intervals that the list held
        // before the expression. Nothing of the failed evaluation stays for the next one.
        final TimestampDriver driver = timestampType.getDriver();
        final long now = driver.parseFloorLiteral("2025-01-24T12:00:00.000000Z");
        final LocalDate today = LocalDate.of(2025, 1, 24);
        final long nine = driver.fromHours(9);
        final long hour = driver.fromHours(1);
        final CairoConfiguration limit16 = intervalLimitConfiguration(8, 16);
        // 10 later days, then 6 days before them: 16 intervals, the limit; one more day fails
        assertDateRangeLimit(
                limit16,
                "[$today+1d..$today+10d, $today-5d..$today]T09:00;1h",
                "[$today+1d..$today+10d, $today-6d..$today]T09:00;1h",
                now,
                today.minusDays(5),
                today.plusDays(10),
                nine,
                nine + hour - 1,
                "Date range expansion produces too many intervals (max 16)"
        );
        // a merge after 2 intervals and a limit of 3: the first merge is of all intervals
        assertDateRangeLimit(
                intervalLimitConfiguration(2, 3),
                "[$today+1d..$today+1d, $today-1d..$today]T09:00;1h",
                "[$today+1d..$today+1d, $today-2d..$today]T09:00;1h",
                now,
                today.minusDays(1),
                today.plusDays(1),
                nine,
                nine + hour - 1,
                "Date range expansion produces too many intervals (max 3)"
        );
        // a merge after every day and a limit of 2
        assertDateRangeLimit(
                intervalLimitConfiguration(1, 2),
                "[$today+1d..$today+1d, $today..$today]T09:00;1h",
                "[$today+1d..$today+1d, $today-1d..$today]T09:00;1h",
                now,
                today,
                today.plusDays(1),
                nine,
                nine + hour - 1,
                "Date range expansion produces too many intervals (max 2)"
        );
        // the default limit of 1,024
        assertDateRangeLimit(
                configuration,
                "[$today-1023d..$today]T09:00",
                "[$today-1024d..$today]T09:00",
                now,
                today.minusDays(1023),
                today,
                nine,
                nine + driver.fromMinutes(1) - 1,
                "Date range expansion produces too many intervals (max 1024)"
        );
    }

    @Test
    public void testCompiledDateVariableListAtConfiguredCap() throws SqlException {
        // The list reads the cap from the configuration: at a cap of 4, two date variables take two
        // time list values, and five copies of one take four. $today is the Monday 2024-01-08.
        final CairoConfiguration cap4Configuration = new DefaultCairoConfiguration(".") {
            @Override
            public int getSqlIntervalIncrementalMergeThreshold() {
                return 2;
            }

            @Override
            public int getSqlIntervalMaxIntervalsAfterMerge() {
                return 4;
            }
        };
        final LongList twoDays = new LongList();
        for (LocalDate day = LocalDate.of(2024, 1, 7); day.isBefore(LocalDate.of(2024, 1, 9)); day = day.plusDays(1)) {
            addLocalBlock(twoDays, day.atTime(9, 0), ZoneOffset.UTC, Micros.MINUTE_MICROS);
            addLocalBlock(twoDays, day.atTime(10, 0), ZoneOffset.UTC, Micros.MINUTE_MICROS);
        }
        assertCompiledTickExprIntervals(cap4Configuration, "[$today,$yesterday]T[09:00,10:00]", 2, 4, twoDays);
        assertCompiledOverMaxIntervals(cap4Configuration, "[$today,$yesterday]T[09:00,10:00,11:00]");
        assertCompiledOverMaxIntervals(cap4Configuration, "[$today,$yesterday,$tomorrow,$today-2d,$today-3d]");
        final LongList fourHours = new LongList();
        for (int hour = 9; hour < 13; hour++) {
            addLocalBlock(fourHours, LocalDateTime.of(2024, 1, 8, hour, 0), ZoneOffset.UTC, Micros.MINUTE_MICROS);
        }
        assertCompiledTickExprIntervals(cap4Configuration, "[$today,$today,$today,$today,$today]T[09:00,10:00,11:00,12:00]", 1, 4, fourHours);

        // At the largest cap, 32,768 date variables x 65,536 time list values make 2^31 intervals, one
        // more than the cap. The list counts them in a long: an int would wrap to a negative count
        // there and let the list through.
        final CairoConfiguration maxCapConfiguration = new DefaultCairoConfiguration(".") {
            @Override
            public int getSqlIntervalMaxIntervalsAfterMerge() {
                return Integer.MAX_VALUE;
            }
        };
        assertCompiledOverMaxIntervals(maxCapConfiguration, '[' + todayMinusDays(32_769, 1) + "]T[" + repeatedValues("00:00", 65_536) + ']');
    }

    @Test
    public void testCompiledDateVariableListCapFuzz() throws SqlException {
        // Builds random lists of date variables, with offsets in days, hours and business days or
        // none, and of ranges of them, whose elements repeat earlier ones in other spellings: the
        // name of a variable in any case, spaces around the operator and before the unit, a leading
        // zero, an underscore, bd in any case, and -0 for +0. A list takes a time list of T values two
        // seconds apart or none, maybe a day filter or a zone, and the cap 8 or 1,024. It takes no day
        // filter and zone together: parseTickExpr filters the values of a zoned time list by their
        // day in UTC, where the compiled list filters them by their local day. The IR encodes
        // an element by its variables, offset units and offsets, so the model counts the distinct
        // elements by these, where no offset differs from an offset of 0. A list compiles when its
        // distinct elements x max(1, T) stay within the cap, and keeps one IR element for each;
        // evaluate() stays within getMaxIntervalCount() every 6 hours through 2024, and at three nows
        // it returns what parseTickExpr returns for the list whenever that parses. A list over the cap
        // fails with the cap error at position 0. The lists must reach each shape that the assertions
        // at the end name, so the test builds more lists, up to ten times as many, until they do.
        final int listCount = Integer.getInteger("questdb.fuzz.tick.lists", 300);
        final CairoConfiguration cap8Configuration = new DefaultCairoConfiguration(".") {
            @Override
            public int getSqlIntervalIncrementalMergeThreshold() {
                return 4;
            }

            @Override
            public int getSqlIntervalMaxIntervalsAfterMerge() {
                return 8;
            }
        };
        final String[] variables = {"$today", "$yesterday", "$tomorrow", "$now"};
        // the variables of ranges, in the order of their days
        final String[] rangeVariables = {"$yesterday", "$today", "$tomorrow"};
        final String[] suffixes = {"", "#workday", "@+05:00", "@Europe/London"};
        final Rnd rnd = TestUtils.generateRandom(LOG);
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final long start = timestampDriver.parseFloorLiteral("2024-01-01T00:00:00.000000Z");
        final long end = timestampDriver.parseFloorLiteral("2025-01-01T00:00:00.000000Z");
        final long step = timestampDriver.fromHours(6);
        final long saturdayNoon = timestampDriver.parseFloorLiteral("2024-01-13T12:00:00.000000Z");
        final ObjList<DateVariableListElement> elements = new ObjList<>();
        final HashMap<DateVariableListElement, String> spellingByElement = new HashMap<>();
        final StringSink spelling = new StringSink();
        final StringSink expression = new StringSink();
        final LongList intervals = new LongList();
        final LongList parsed = new LongList();
        boolean hasAcceptedByRepeats = false;
        boolean hasRejectedWithRepeats = false;
        boolean hasAtCap = false;
        boolean hasRepeatSpelledApart = false;
        boolean hasVariableAndRangeOfOneStart = false;
        for (
                int i = 0;
                i < listCount || (i < 10 * listCount && !(hasAcceptedByRepeats && hasRejectedWithRepeats && hasAtCap && hasRepeatSpelledApart && hasVariableAndRangeOfOneStart));
                i++
        ) {
            final CairoConfiguration cfg = rnd.nextBoolean() ? cap8Configuration : configuration;
            final int cap = cfg.getSqlIntervalMaxIntervalsAfterMerge();
            elements.clear();
            spellingByElement.clear();
            expression.clear();
            expression.put('[');
            boolean isRepeatSpelledApart = false;
            for (int e = 0, n = 1 + rnd.nextInt(12); e < n; e++) {
                final DateVariableListElement element;
                final DateVariableListElement earlier = e > 0 ? elements.getQuick(rnd.nextInt(e)) : null;
                if (earlier != null && rnd.nextBoolean()) {
                    element = earlier;
                } else if (rnd.nextInt(4) == 0) {
                    // A range starts at or before the day of its variable and ends at or after the
                    // day of its own, which never comes first, so the parse path takes every range.
                    final int startVariable = rnd.nextInt(rangeVariables.length);
                    final int endVariable = startVariable + rnd.nextInt(rangeVariables.length - startVariable);
                    element = new DateVariableListElement(
                            rnd.nextBoolean()
                                    ? new OffsetDateVariable(rangeVariables[startVariable], "", 0)
                                    : new OffsetDateVariable(rangeVariables[startVariable], "d", -rnd.nextInt(4)),
                            new OffsetDateVariable(rangeVariables[endVariable], rnd.nextBoolean() ? "d" : "bd", rnd.nextInt(4))
                    );
                } else if (earlier != null && earlier.end() != null && rnd.nextBoolean()) {
                    // the start of an earlier range on its own
                    element = new DateVariableListElement(earlier.start(), null);
                } else {
                    final String name = variables[rnd.nextInt(variables.length)];
                    final OffsetDateVariable variable = switch (rnd.nextInt("$now".equals(name) ? 4 : 3)) {
                        case 0 -> new OffsetDateVariable(name, "", 0);
                        case 1 -> new OffsetDateVariable(name, "d", rnd.nextInt(61) - 30);
                        case 2 -> new OffsetDateVariable(name, "bd", rnd.nextInt(21) - 10);
                        default -> new OffsetDateVariable(name, "h", rnd.nextInt(95) - 47);
                    };
                    element = new DateVariableListElement(variable, null);
                }
                elements.add(element);
                spelling.clear();
                appendDateVariableSpelling(rnd, spelling, element.start());
                if (element.end() != null) {
                    appendRandomSpaces(rnd, spelling);
                    spelling.put("..");
                    appendRandomSpaces(rnd, spelling);
                    appendDateVariableSpelling(rnd, spelling, element.end());
                }
                final String firstSpelling = spellingByElement.putIfAbsent(element, spelling.toString());
                isRepeatSpelledApart |= firstSpelling != null && !firstSpelling.contentEquals(spelling);
                if (e > 0) {
                    expression.put(',');
                }
                expression.put(spelling);
            }
            expression.put(']');
            final int distinctCount = spellingByElement.size();
            final int timeCount = switch (rnd.nextInt(5)) {
                case 0 -> 0;
                case 1 -> 1;
                case 2 -> cap / distinctCount;
                case 3 -> cap / distinctCount + 1;
                default -> 1 + rnd.nextInt(2 * cap / distinctCount + 1);
            };
            if (timeCount > 0) {
                expression.put("T[").put(evenSecondsOfDay(timeCount)).put(']');
            }
            expression.put(suffixes[rnd.nextInt(suffixes.length)]);
            final String tick = expression.toString();
            final long intervalCount = (long) distinctCount * Math.max(1, timeCount);

            final CompiledTickExpression compiled;
            try {
                compiled = IntervalUtils.compileTickExpr(timestampDriver, cfg, tick, 0, tick.length(), 0);
            } catch (SqlException ex) {
                Assert.assertTrue(tick + ": " + ex.getFlyweightMessage(), intervalCount > cap);
                Assert.assertEquals(tick, 0, ex.getPosition());
                TestUtils.assertEquals("Bracket expansion produces too many intervals (max " + cap + ')', ex.getFlyweightMessage());
                hasRejectedWithRepeats |= elements.size() > distinctCount;
                continue;
            }
            try (compiled) {
                Assert.assertTrue(tick + " compiles with " + intervalCount + " intervals", intervalCount <= cap);
                Assert.assertEquals(tick, distinctCount, compiled.getElementCount());
                final int bound = compiled.getMaxIntervalCount(Integer.MAX_VALUE);
                if (bound > 0) {
                    for (long now = start; now < end; now += step) {
                        intervals.clear();
                        try {
                            compiled.evaluate(intervals, now);
                        } catch (CairoException ex) {
                            // the evaluation stops where the parser stops, on the interval cap, see below
                            TestUtils.assertContains(ex.getFlyweightMessage(), "too many intervals");
                            continue;
                        }
                        if (intervals.size() / 2 > bound) {
                            Assert.fail(tick + " yields " + intervals.size() / 2 + " intervals, above its bound of " + bound
                                    + ", at " + timestampDriver.toMSecString(now));
                        }
                    }
                }
                final long seededNow = start + timestampDriver.fromSeconds(rnd.nextInt(366 * 24 * 3_600));
                for (long now : new long[]{mondayNow(), saturdayNoon, seededNow}) {
                    intervals.clear();
                    String evaluationError = null;
                    try {
                        compiled.evaluate(intervals, now);
                    } catch (CairoException ex) {
                        evaluationError = ex.getFlyweightMessage().toString();
                        TestUtils.assertContains(evaluationError, "too many intervals");
                    }
                    parsed.clear();
                    try {
                        IntervalUtils.parseTickExpr(timestampDriver, cfg, tick, 0, tick.length(), 0, parsed, new StringSink(), now);
                    } catch (SqlException ex) {
                        TestUtils.assertContains(ex.getFlyweightMessage(), "too many intervals");
                        continue;
                    }
                    // the evaluation fails on the interval cap only where the parser fails
                    Assert.assertNull(tick + " at " + timestampDriver.toMSecString(now), evaluationError);
                    Assert.assertEquals(
                            tick + " at " + timestampDriver.toMSecString(now),
                            intervalToString(timestampDriver, parsed).toString(),
                            intervalToString(timestampDriver, intervals).toString()
                    );
                }
            }
            hasAcceptedByRepeats |= (long) elements.size() * Math.max(1, timeCount) > cap;
            hasAtCap |= intervalCount == cap;
            hasRepeatSpelledApart |= isRepeatSpelledApart;
            for (int e = 0, n = elements.size(); e < n; e++) {
                final DateVariableListElement range = elements.getQuick(e);
                if (range.end() != null) {
                    hasVariableAndRangeOfOneStart |= spellingByElement.containsKey(new DateVariableListElement(range.start(), null));
                }
            }
        }
        Assert.assertTrue("a list within the cap only because its repeats count once", hasAcceptedByRepeats);
        Assert.assertTrue("a list over the cap by its distinct elements that holds repeats", hasRejectedWithRepeats);
        Assert.assertTrue("a list at the cap", hasAtCap);
        Assert.assertTrue("a list that repeats an element in another spelling", hasRepeatSpelledApart);
        Assert.assertTrue("a list of a date variable and a range that starts with it", hasVariableAndRangeOfOneStart);
    }

    @Test
    public void testCompiledDateVariableListOverMaxIntervals() throws SqlException {
        // evaluate() emits max(1, T) intervals for each date variable element of a list with T time
        // list values on every execution, so the list keeps at most the cap (1,024) of them: its
        // distinct date variables x max(1, T). parseTickExpr keeps a list within the cap too, so the
        // static equivalents of these lists, whose intervals never touch, fail with the same error.
        // $today is the Monday 2024-01-08.
        Assert.assertEquals(1_024, configuration.getSqlIntervalMaxIntervalsAfterMerge());
        final String overCapError = "Bracket expansion produces too many intervals (max 1024)";
        // 4 days x 256 minutes two minutes apart, from 00:00 to 08:30, stay within the cap, and 5 days
        // x 205 minutes do not.
        final LongList fourDays = new LongList();
        for (LocalDate day = LocalDate.of(2024, 1, 5); day.isBefore(LocalDate.of(2024, 1, 9)); day = day.plusDays(1)) {
            addMinuteBlocks(fourDays, day, 256, 2);
        }
        final String fourDayTimes = "]T[" + minutesOfDay(256, 2) + ']';
        assertCompiledTickExprIntervals("[$today,$yesterday,$today-2d,$today-3d" + fourDayTimes, 4, 1_024, fourDays);
        assertTickExprIntervals(configuration, fourDays, "[2024-01-08,2024-01-07,2024-01-06,2024-01-05" + fourDayTimes);
        final String fiveDayTimes = "]T[" + minutesOfDay(205, 2) + ']';
        assertCompiledOverMaxIntervals(configuration, "[$today,$yesterday,$today-2d,$today-3d,$today-4d" + fiveDayTimes);
        assertTickExprErrorAtMonday("[2024-01-08,2024-01-07,2024-01-06,2024-01-05,2024-01-04" + fiveDayTimes, overCapError);

        // one date variable x 1,024 seconds two seconds apart, and x 1,025
        final LongList seconds = new LongList();
        for (int k = 0; k < 1_024; k++) {
            addLocalBlock(seconds, LocalDateTime.of(2024, 1, 8, 0, 0).plusSeconds(2L * k), ZoneOffset.UTC, Micros.SECOND_MICROS);
        }
        assertCompiledTickExprIntervals("[$today]T[" + evenSecondsOfDay(1_024) + ']', 1, 1_024, seconds);
        assertCompiledOverMaxIntervals(configuration, "[$today]T[" + evenSecondsOfDay(1_025) + ']');
        assertTickExprErrorAtMonday("[2024-01-08]T[" + evenSecondsOfDay(1_025) + ']', overCapError);

        // 1,024 date variables without a time list, every other day back from $today, and 1,025
        final LongList everyOtherDay = new LongList();
        for (int k = 1_023; k >= 0; k--) {
            final LocalDate day = LocalDate.of(2024, 1, 8).minusDays(2L * k);
            addLocalDays(everyOtherDay, day, day.plusDays(1), ZoneOffset.UTC);
        }
        assertCompiledTickExprIntervals('[' + todayMinusDays(1_024, 2) + ']', 1_024, 1_024, everyOtherDay);
        assertTickExprIntervals(configuration, everyOtherDay, '[' + mondayMinusDays(1_024, 2) + ']');
        assertCompiledOverMaxIntervals(configuration, '[' + todayMinusDays(1_025, 2) + ']');
        assertTickExprErrorAtMonday('[' + mondayMinusDays(1_025, 2) + ']', overCapError);

        // A range counts as one date variable: 2 ranges of a day x 512 minutes stay within the cap,
        // and x 513 do not.
        final LongList twoDays = new LongList();
        addMinuteBlocks(twoDays, LocalDate.of(2024, 1, 7), 512, 2);
        addMinuteBlocks(twoDays, LocalDate.of(2024, 1, 8), 512, 2);
        final String twoDayRanges = "[$today..$today,$yesterday..$yesterday]T[" + minutesOfDay(512, 2) + ']';
        assertCompiledTickExprIntervals(twoDayRanges, 2, -1, twoDays);
        assertTickExprIntervals(configuration, twoDays, twoDayRanges);
        assertCompiledOverMaxIntervals(configuration, "[$today..$today,$yesterday..$yesterday]T[" + minutesOfDay(513, 2) + ']');

        // The list counts its date variables, not the intervals their union leaves: 1,025 adjacent
        // days fail, while parseTickExpr joins the static equivalent into one interval.
        assertCompiledOverMaxIntervals(configuration, '[' + todayMinusDays(1_025, 1) + ']');
        final LongList adjacentDays = new LongList();
        addLocalDays(adjacentDays, LocalDate.of(2024, 1, 8).minusDays(1_024), LocalDate.of(2024, 1, 9), ZoneOffset.UTC);
        assertTickExprIntervals(configuration, adjacentDays, '[' + mondayMinusDays(1_025, 1) + ']');

        // The list fails at the first date variable over the cap, before an invalid element after
        // it, and after an invalid element before it.
        assertCompiledOverMaxIntervals(configuration, '[' + todayMinusDays(1_025, 2) + ",$bogus]");
        assertCompileTickExprError("[$bogus," + todayMinusDays(1_025, 2) + ']', "Unknown date variable: $bogus");
    }

    @Test
    public void testCompiledDateVariableListRepeatedElementsStoredOnce() throws SqlException {
        // A date variable element that repeats an earlier one, as the IR encodes them, yields the same
        // intervals for every "now", so the list stores and counts it once. 6,000 copies of $today
        // with 300 minutes two minutes apart keep one element, which yields the 300 minutes of the
        // Monday 2024-01-08 from 00:00 to 09:58.
        final LongList mondayMinutes = new LongList();
        addMinuteBlocks(mondayMinutes, LocalDate.of(2024, 1, 8), 300, 2);
        assertCompiledTickExprIntervals('[' + repeatedValues("$today", 6_000) + "]T[" + minutesOfDay(300, 2) + ']', 1, 300, mondayMinutes);

        // The name of a variable takes any case, the operator and the unit any spaces, and the number
        // leading zeros; -0d is +0d, and bd takes any case. No offset differs from an offset of 0d.
        final LongList monday = new LongList();
        addLocalDays(monday, LocalDate.of(2024, 1, 8), LocalDate.of(2024, 1, 9), ZoneOffset.UTC);
        assertCompiledTickExprIntervals("[$today,$TODAY, $Today ,$today - 0d,$today+0d]", 2, 2, monday);
        assertCompiledTickExpr("[$today,$TODAY, $Today ,$today - 0d,$today+0d]");
        final LongList sunday = new LongList();
        addLocalDays(sunday, LocalDate.of(2024, 1, 7), LocalDate.of(2024, 1, 8), ZoneOffset.UTC);
        assertCompiledTickExprIntervals("[$today - 1d,$today-1d,$today -  1d,$today-01d]", 1, 1, sunday);
        final LongList tuesday = new LongList();
        addLocalDays(tuesday, LocalDate.of(2024, 1, 9), LocalDate.of(2024, 1, 10), ZoneOffset.UTC);
        assertCompiledTickExprIntervals("[$today+1bd,$today + 1BD]", 1, 1, tuesday);

        // The list counts an element once it drops the repeats: two copies of $today x 1,024 minutes
        // one minute apart stay within the cap and yield one interval, from 00:00 to 17:03:59.999999.
        final LongList touchingMinutes = new LongList();
        addLocalBlock(touchingMinutes, LocalDateTime.of(2024, 1, 8, 0, 0), ZoneOffset.UTC, 1_024 * Micros.MINUTE_MICROS);
        assertCompiledTickExprIntervals("[$today,$today]T[" + minutesOfDay(1_024, 1) + ']', 1, 1_024, touchingMinutes);
        // $today + 0d encodes apart from $today, though it yields the same intervals, so 2 x 513
        // minutes fail, while parseTickExpr unions the static equivalent into the 513.
        assertCompiledOverMaxIntervals(configuration, "[$today,$today + 0d]T[" + minutesOfDay(513, 2) + ']');
        final LongList minutes513 = new LongList();
        addMinuteBlocks(minutes513, LocalDate.of(2024, 1, 8), 513, 2);
        assertTickExprIntervals(configuration, minutes513, "[2024-01-08,2024-01-08]T[" + minutesOfDay(513, 2) + ']');

        // A range repeats a range with the same start and the same end, not one with the same start.
        final LongList fourMinutes = new LongList();
        for (LocalDate day = LocalDate.of(2024, 1, 8); day.isBefore(LocalDate.of(2024, 1, 10)); day = day.plusDays(1)) {
            addLocalBlock(fourMinutes, day.atTime(9, 0), ZoneOffset.UTC, Micros.MINUTE_MICROS);
            addLocalBlock(fourMinutes, day.atTime(10, 0), ZoneOffset.UTC, Micros.MINUTE_MICROS);
        }
        assertCompiledTickExprIntervals("[$today..$today+1d, $today .. $today + 1d]T[09:00,10:00]", 1, -1, fourMinutes);
        final LongList threeDays = new LongList();
        addLocalDays(threeDays, LocalDate.of(2024, 1, 8), LocalDate.of(2024, 1, 11), ZoneOffset.UTC);
        assertCompiledTickExprIntervals("[$today..$today+1d,$today..$today+2d]", 2, 2, threeDays);
        // $now encodes as 0 and $now..$now as a range of it: each stays one element. At 01:00, both
        // yield the instant.
        final LongList instant = new LongList();
        instant.add(mondayNow(), mondayNow());
        assertCompiledTickExprIntervals("[$now,$now..$now,$now,$now..$now]", 2, 2, instant);
        assertCompiledTickExpr("[$now,$now..$now,$now,$now..$now]");

        // A list starts with none of the elements of an earlier list on the thread, also after one
        // that failed.
        final LongList weekend = new LongList();
        addLocalDays(weekend, LocalDate.of(2024, 1, 7), LocalDate.of(2024, 1, 9), ZoneOffset.UTC);
        assertCompiledTickExprIntervals("[$yesterday,$today]", 2, 2, weekend);
        final LongList twoMinutes = new LongList();
        addLocalBlock(twoMinutes, LocalDateTime.of(2024, 1, 8, 9, 0), ZoneOffset.UTC, Micros.MINUTE_MICROS);
        addLocalBlock(twoMinutes, LocalDateTime.of(2024, 1, 8, 10, 0), ZoneOffset.UTC, Micros.MINUTE_MICROS);
        assertCompiledTickExprIntervals("[$today]T[09:00,10:00]", 1, 2, twoMinutes);
        assertCompiledOverMaxIntervals(configuration, "[$today,$yesterday,$today-2d,$today-3d,$today-4d]T[" + minutesOfDay(205, 2) + ']');
        assertCompiledTickExprIntervals("[$today,$yesterday]", 2, 2, weekend);
    }

    @Test
    public void testCompiledDayFilterBracketExpansionAtMaxIntervals() throws SqlException {
        // Days 1-28 of a month hold exactly 4 Mondays, so 2004-2024 hold 21 x 12 x 4 = 1,008 Monday
        // intervals, within the cap of 1,024. $today, the Monday 2024-01-08, is one of them.
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final String interval = "[$today,20[04..24]-[01..12]-[01..28]]#mon";
        try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(timestampDriver, configuration, interval, 0, interval.length(), 0)) {
            out.clear();
            compiled.evaluate(out, mondayNow());
        }
        assertFullDayMondays(out, 1_008);
    }

    @Test
    public void testCompiledDayFilterBracketExpansionOverMaxIntervals() {
        // 22 x 12 x 4 = 1,056 Monday intervals exceed the cap of 1,024. The incremental merge checks
        // the count only once per 256 leaves, so the checks of the list count them after the last
        // leaf.
        assertCompileTickExprError(
                "[$today,20[03..24]-[01..12]-[01..28]]#mon",
                "Bracket expansion produces too many intervals (max 1024)"
        );
    }

    @Test
    public void testCompiledDayFilterImpreciseBracketExpansion() throws SqlException {
        // A bracketed element without date precision (year, month or ISO week) leaves the day filter
        // holder of compileStaticElement unarmed, so the element takes the expanding filter, which
        // turns each year, month or week into its matching days. The compiler applies the run-time
        // filter of CompiledTickExpression too, but that one judges only the first day of each
        // interval: alone it would keep January 2024 whole, since it starts on a Monday, and drop
        // February and March. $today, the Monday 2024-01-08, lies in every range.
        assertCompiledFullDayMondays(configuration, "[$today,2024-[01..03]]#mon", LocalDate.of(2024, 1, 1), LocalDate.of(2024, 3, 31));
        // ISO week 1 of 2024 starts on Monday 2024-01-01, and week 13 ends on Sunday 2024-03-31
        assertCompiledFullDayMondays(configuration, "[$today,2024-W[01..13]]#mon", LocalDate.of(2024, 1, 1), LocalDate.of(2024, 3, 31));
        assertCompiledFullDayMondays(configuration, "[$today,[2023..2024]]#mon", LocalDate.of(2023, 1, 1), LocalDate.of(2024, 12, 31));
        // at merge threshold 4 the expansion joins the months into blocks before the filter expands
        // the blocks
        assertCompiledFullDayMondays(mergeThreshold4Configuration, "[$today,2024-[01..12]]#mon", LocalDate.of(2024, 1, 1), LocalDate.of(2024, 12, 31));

        assertCompiledTickExpr("[$today,2024-[01..03]]#mon");
        assertCompiledTickExpr("[$today,2024-[01..03]]#workday");
        // none of the three months starts on a weekend day, so the run-time filter alone would drop
        // them all
        assertCompiledTickExpr("[$today,2024-[01..03]]#weekend");
        // the filter judges local days; New York moves its clocks on 2024-03-10
        assertCompiledTickExpr("[$today,2024-[01..03]]@America/New_York#workday");
        // after the holder of a date-precise element, and after an element without brackets
        assertCompiledTickExpr("[$today,2024-05-[01..31],2024-[01..03]]#mon");
        assertCompiledTickExpr("[$today,2024-01,2024-[02..03]]#workday");
    }

    @Test
    public void testCompiledImpreciseDateWithTimeDayFilterControl() throws SqlException {
        // compileImpreciseDateWithTime expands the months with no pending day filter and then filters
        // every day of every month: one 09:00 interval per Monday. The list keeps at most the cap
        // (1,024) of static intervals, so the 1,305 Mondays of 2000-2024 fail, as in parseTickExpr, and
        // the 992 of 2000-2018 stay, next to $today, the Monday 2024-01-08.
        assertCompiledAndParsedOverMaxIntervals("[$today,20[00..24]-[01..12]]T09:00#mon");
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final String interval = "[$today,20[00..18]-[01..12]]T09:00#mon";
        try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(timestampDriver, configuration, interval, 0, interval.length(), 0)) {
            out.clear();
            compiled.evaluate(out, mondayNow());
        }
        Assert.assertEquals(countMondays(LocalDate.of(2000, 1, 1), LocalDate.of(2018, 12, 31)) + 1, out.size() / 2);
        for (int i = 0, n = out.size(); i < n; i += 2) {
            final long lo = out.getQuick(i);
            Assert.assertEquals(1, timestampDriver.getDayOfWeek(lo));
            Assert.assertEquals(9, timestampDriver.getHourOfDay(lo));
            Assert.assertEquals(0, timestampDriver.getMinuteOfHour(lo));
        }
    }

    @Test
    public void testCompiledImpreciseDateWithTimeOverMaxIntervals() throws SqlException {
        // compileImpreciseDateWithTime turns a month or a year with a time into one interval per day,
        // and the list keeps at most the cap (1,024) of static intervals. The 31,280 days of the odd
        // months of 1900-2069 at 09:30 never touch.
        Assert.assertEquals(1_024, configuration.getSqlIntervalMaxIntervalsAfterMerge());
        assertCompiledAndParsedOverMaxIntervals("[$today,[1900..2069]-[01,03,05,07,09,11]]T09:30");

        // A duration of 25 hours reaches into the next day, so the days of a month join into one
        // interval, from 09:30 on its first day to 10:29:59.999999 on the first day of the next month.
        // The list unions its days whenever it holds more than the cap and the merge threshold (256)
        // together, and once more at the end, which leaves the 1,020 months. $today, the Monday
        // 2024-01-08, lies in January 2024.
        final LongList oddMonths = new LongList();
        for (int year = 1900; year < 2070; year++) {
            for (int month = 1; month < 12; month += 2) {
                final LocalDateTime lo = LocalDate.of(year, month, 1).atTime(9, 30);
                final LocalDateTime end = LocalDate.of(year, month + 1, 1).atTime(10, 30);
                addLocalBlock(oddMonths, lo, ZoneOffset.UTC, (end.toEpochSecond(ZoneOffset.UTC) - lo.toEpochSecond(ZoneOffset.UTC)) * Micros.SECOND_MICROS);
            }
        }
        final String oddMonthDays = "[$today,[1900..2069]-[01,03,05,07,09,11]]T09:30;25h";
        assertCompiledTickExprIntervals(oddMonthDays, 1_021, 1_021, oddMonths);
        // The expansion of the 1,020 month leaves reads the cap at its unions at the leaves 256, 512
        // and 768 and after the last leaf, and the list reads it once.
        final MaxIntervalsCountingConfiguration countingConfiguration = new MaxIntervalsCountingConfiguration();
        IntervalUtils.compileTickExpr(timestampType.getDriver(), countingConfiguration, oddMonthDays, 0, oddMonthDays.length(), 0).close();
        Assert.assertEquals(5, countingConfiguration.getterCallCount);

        // The 1,461 days of 2000-2003, an odd count, join into one interval, also after a static
        // element.
        final LongList fourYears = new LongList();
        addLocalBlock(fourYears, LocalDateTime.of(2000, 1, 1, 9, 30), ZoneOffset.UTC, (1_461 * 24 + 1) * Micros.HOUR_MICROS);
        addLocalBlock(fourYears, LocalDateTime.of(2024, 1, 8, 9, 30), ZoneOffset.UTC, 25 * Micros.HOUR_MICROS);
        assertCompiledTickExprIntervals("[$today,20[00..03]-[01..12]]T09:30;25h", 182, 2, fourYears);
        assertCompiledTickExpr("[$today,20[00..03]-[01..12]]T09:30;25h");
        addLocalBlock(fourYears, LocalDateTime.of(2024, 1, 15, 9, 30), ZoneOffset.UTC, 25 * Micros.HOUR_MICROS);
        assertCompiledTickExprIntervals("[$today,2024-01-15,20[00..03]-[01..12]]T09:30;25h", 184, 3, fourYears);
        assertCompiledTickExpr("[$today,2024-01-15,20[00..03]-[01..12]]T09:30;25h");

        // Two copies of the 992 Mondays of 2000-2018 at 09:00 hold 1,984 intervals, whose union is the
        // 992. parseTickExpr counts the days of a month with a time before the day filter, so it
        // rejects the static equivalent.
        final LongList mondayMinutes = new LongList();
        for (LocalDate day = LocalDate.of(2000, 1, 3); day.getYear() < 2019; day = day.plusWeeks(1)) {
            addLocalBlock(mondayMinutes, day.atTime(9, 0), ZoneOffset.UTC, Micros.MINUTE_MICROS);
        }
        Assert.assertEquals(2 * 992, mondayMinutes.size());
        addLocalBlock(mondayMinutes, LocalDateTime.of(2024, 1, 8, 9, 0), ZoneOffset.UTC, Micros.MINUTE_MICROS);
        assertCompiledTickExprIntervals("[$today,20[00..18]-[01..12],20[00..18]-[01..12]]T09:00#mon", 993, 993, mondayMinutes);

        // A negative duration starts the interval of a weekend day D at D - 6d, and evaluate() keeps
        // each interval by the weekday of its start, so only those of the Saturdays stay, and they
        // never touch: the 783 of 2000-2014 stay within the cap, the 1,305 of 2000-2024 do not. The
        // interval of $today starts on a Tuesday and goes. parseTickExpr judges these intervals by
        // another day, so it gives no reference here.
        final LongList saturdayWeeks = new LongList();
        for (LocalDate day = LocalDate.of(2000, 1, 1); day.getYear() < 2015; day = day.plusWeeks(1)) {
            addLocalDays(saturdayWeeks, day.minusDays(6), day, ZoneOffset.UTC);
        }
        Assert.assertEquals(2 * 783, saturdayWeeks.size());
        assertCompiledTickExprIntervals("[$today,[2000..2014]]T00:00#weekend;-6d", 784, 784, saturdayWeeks);
        assertCompiledOverMaxIntervals(configuration, "[$today,[2000..2024]]T00:00#weekend;-6d");
    }

    @Test
    public void testCompiledListStaticIntervalsAcrossElements() throws SqlException {
        // The cap (1,024) bounds the static intervals of the whole list, not those of each element:
        // elements within the cap can exceed it together, and the union of repeated elements stays
        // within it. The list unions its static intervals whenever they exceed the cap and the merge
        // threshold (256) together, and once more at the end.
        final LongList mondays = new LongList();
        addDayRuns(mondays, LocalDate.of(1981, 1, 1), LocalDate.of(1999, 12, 31), EnumSet.of(DayOfWeek.MONDAY), ZoneOffset.UTC);
        Assert.assertEquals(2 * 991, mondays.size());
        addLocalDays(mondays, LocalDate.of(2024, 1, 8), LocalDate.of(2024, 1, 9), ZoneOffset.UTC);
        // the 991 Mondays of 1981-1999, twice and 50 times, and $today, the Monday 2024-01-08
        assertCompiledTickExprIntervals("[$today,[1981..1999],[1981..1999]]#mon", 992, 992, mondays);
        assertCompiledTickExpr("[$today,[1981..1999],[1981..1999]]#mon");
        assertCompiledTickExprIntervals("[$today," + repeatedValues("[1981..1999]", 50) + "]#mon", 992, 992, mondays);
        // the 991 Mondays of 1981-1999 and the 992 of 1961-1979
        assertCompiledAndParsedOverMaxIntervals("[$today,[1981..1999],[1961..1979]]#mon");

        // 25 days x 8 hours x 5 minutes are 1,000 seconds that never touch, and the same seconds again
        // stay within the cap, while those of March next to them do not.
        final String february = "2024-02-[01..25]T[00..07]:[00..04]:00";
        final LongList seconds = new LongList();
        addLocalDays(seconds, LocalDate.of(2024, 1, 8), LocalDate.of(2024, 1, 9), ZoneOffset.UTC);
        for (int day = 1; day <= 25; day++) {
            for (int hour = 0; hour < 8; hour++) {
                for (int minute = 0; minute < 5; minute++) {
                    addLocalBlock(seconds, LocalDateTime.of(2024, 2, day, hour, minute), ZoneOffset.UTC, Micros.SECOND_MICROS);
                }
            }
        }
        assertCompiledTickExprIntervals("[$today," + february + ',' + february + ']', 1_001, 1_001, seconds);
        assertCompiledTickExpr("[$today," + february + ',' + february + ']');
        assertCompiledAndParsedOverMaxIntervals("[$today," + february + ",2024-03-[01..25]T[00..07]:[00..04]:00]");

        // An element that covers the 1,043 Mondays of 1980-1999 joins them at the final union, before
        // or after them, while they stay within the cap and the threshold. parseTickExpr unions a list
        // whenever it grew by the threshold, so it rejects the list with the cover last.
        final LongList twentyYears = new LongList();
        addLocalDays(twentyYears, LocalDate.of(1980, 1, 7), LocalDate.of(2000, 1, 7), ZoneOffset.UTC);
        addLocalDays(twentyYears, LocalDate.of(2024, 1, 8), LocalDate.of(2024, 1, 9), ZoneOffset.UTC);
        assertCompiledTickExprIntervals("[$today,1980-01-07;20y,[1980..1999]]#mon", 2, 2, twentyYears);
        assertCompiledTickExpr("[$today,1980-01-07;20y,[1980..1999]]#mon");
        assertCompiledTickExprIntervals("[$today,[1980..1999],1980-01-07;20y]#mon", 2, 2, twentyYears);
        // the 1,304 Mondays of 1975-1999 exceed the cap and the threshold before the cover
        assertCompiledAndParsedOverMaxIntervals("[$today,[1975..1999],1975-01-06;25y]#mon");
    }

    @Test
    public void testCompiledListStaticIntervalsAfterCapError() throws SqlException {
        // A list that fails leaves its static intervals in the scratch list of the thread. The next
        // list on the thread starts with an empty one, and converts its own intervals once.
        assertCompiledOverMaxIntervals(configuration, "[$today,[1981..1999],[1961..1979]]#mon");
        final ZoneId santiago = ZoneId.of("America/Santiago");
        final LongList days = new LongList();
        addZonedLocalDay(days, LocalDate.of(2024, 1, 8), santiago);
        addZonedLocalDay(days, LocalDate.of(2024, 1, 15), santiago);
        assertCompiledTickExprIntervals("[$today,2024-01-15]@America/Santiago", 2, 2, days);
    }

    @Test
    public void testCompiledListStaticIntervalsAtConfiguredCap() throws SqlException {
        // The list reads the cap and the merge threshold from the configuration. 2024 holds 53
        // Mondays, the first on 2024-01-01, and 2023 holds 52. $today, the Monday 2024-01-08, is one of
        // those of 2024.
        final CairoConfiguration cap53Configuration = new DefaultCairoConfiguration(".") {
            @Override
            public int getSqlIntervalIncrementalMergeThreshold() {
                return 4;
            }

            @Override
            public int getSqlIntervalMaxIntervalsAfterMerge() {
                return 53;
            }
        };
        final LongList mondays = new LongList();
        addDayRuns(mondays, LocalDate.of(2024, 1, 1), LocalDate.of(2024, 12, 31), EnumSet.of(DayOfWeek.MONDAY), ZoneOffset.UTC);
        Assert.assertEquals(2 * 53, mondays.size());
        assertCompiledTickExprIntervals(cap53Configuration, "[$today,2024]#mon", 54, 54, mondays);
        // two copies of the Mondays exceed the cap and the threshold, and their union does not
        assertCompiledTickExprIntervals(cap53Configuration, "[$today,2024,2024]#mon", 54, 54, mondays);
        assertCompiledOverMaxIntervals(cap53Configuration, "[$today,2024,2023]#mon");
        final CairoConfiguration cap52Configuration = new DefaultCairoConfiguration(".") {
            @Override
            public int getSqlIntervalIncrementalMergeThreshold() {
                return 4;
            }

            @Override
            public int getSqlIntervalMaxIntervalsAfterMerge() {
                return 52;
            }
        };
        assertCompiledOverMaxIntervals(cap52Configuration, "[$today,2024]#mon");

        // At the largest merge threshold below the default cap (1,024), only the final check of the
        // list unions the 1,305 Mondays of 2000-2024 at 09:00, and they fail.
        final CairoConfiguration mergeThreshold1023Configuration = new DefaultCairoConfiguration(".") {
            @Override
            public int getSqlIntervalIncrementalMergeThreshold() {
                return 1_023;
            }
        };
        assertCompiledOverMaxIntervals(mergeThreshold1023Configuration, "[$today,20[00..24]-[01..12]]T09:00#mon");
        final LongList mondayMinutes = new LongList();
        for (LocalDate day = LocalDate.of(2000, 1, 3); day.getYear() < 2019; day = day.plusWeeks(1)) {
            addLocalBlock(mondayMinutes, day.atTime(9, 0), ZoneOffset.UTC, Micros.MINUTE_MICROS);
        }
        addLocalBlock(mondayMinutes, LocalDateTime.of(2024, 1, 8, 9, 0), ZoneOffset.UTC, Micros.MINUTE_MICROS);
        assertCompiledTickExprIntervals(mergeThreshold1023Configuration, "[$today,20[00..18]-[01..12]]T09:00#mon", 993, 993, mondayMinutes);
    }

    @Test
    public void testCompiledListStaticIntervalsCoveredElementOverMaxIntervals() throws SqlException {
        // Days 1-28 of a month hold exactly 4 Mondays, so 2003-2024 hold 22 x 12 x 4 = 1,056 Monday
        // intervals, above the cap (1,024) on their own, as
        // testCompiledDayFilterBracketExpansionOverMaxIntervals expects. The date-precise filter stays
        // pending while the element expands, and the expansion counts its intervals only once per 256
        // leaves, the last time at 1,024 Mondays, so the element ends above the cap, and the checks of
        // the list count it together with the earlier element. The 22 years from the Monday
        // 2003-01-06, which the filter keeps, hold every one of the Mondays and $today, the Monday
        // 2024-01-08, so the final union of the list leaves one interval. parseTickExpr unions the
        // element with the earlier elements of the list, so it accepts the static equivalent too.
        final LongList listIntervals = new LongList();
        for (LocalDate day = LocalDate.of(2003, 1, 1); day.getYear() <= 2024; day = day.plusDays(1)) {
            if (day.getDayOfMonth() <= 28 && day.getDayOfWeek() == DayOfWeek.MONDAY) {
                addLocalDays(listIntervals, day, day.plusDays(1), ZoneOffset.UTC);
            }
        }
        Assert.assertEquals(2 * 1_056, listIntervals.size());
        final LocalDate coverStart = LocalDate.of(2003, 1, 6);
        addLocalDays(listIntervals, coverStart, coverStart.plusYears(22), ZoneOffset.UTC);
        addLocalDays(listIntervals, LocalDate.of(2024, 1, 8), LocalDate.of(2024, 1, 9), ZoneOffset.UTC);
        final LongList twentyTwoYears = unionOf(listIntervals);
        Assert.assertEquals(2, twentyTwoYears.size());
        final String list = "[$today,2003-01-06;22y,20[03..24]-[01..12]-[01..28]]#mon";
        assertCompiledTickExprIntervals(list, 2, 2, twentyTwoYears);
        assertTickExprIntervals(configuration, twentyTwoYears, list.replace("$today", "2024-01-08"));
        assertCompiledTickExpr(list);
    }

    @Test
    public void testCompiledListStaticIntervalsOverMaxIntervals() throws SqlException {
        // The IR of a compiled list keeps its static intervals, and every evaluate() emits them all,
        // so the list keeps at most the cap (1,024) of them, as parseTickExpr keeps a list within the
        // cap. An expanding day filter turns each year into its matching days before the list counts
        // them: the 1,043 Mondays of 1980-1999 never touch, and their workdays form 1,044 runs from
        // Monday to Friday.
        Assert.assertEquals(1_024, configuration.getSqlIntervalMaxIntervalsAfterMerge());
        assertCompiledAndParsedOverMaxIntervals("[$today,[1980..1999]]#mon");
        assertCompiledAndParsedOverMaxIntervals("[$today,[1980..1999]]#workday");
        // the 991 Mondays of 1981-1999, and $today, the Monday 2024-01-08
        final LongList mondays = new LongList();
        addDayRuns(mondays, LocalDate.of(1981, 1, 1), LocalDate.of(1999, 12, 31), EnumSet.of(DayOfWeek.MONDAY), ZoneOffset.UTC);
        addLocalDays(mondays, LocalDate.of(2024, 1, 8), LocalDate.of(2024, 1, 9), ZoneOffset.UTC);
        assertCompiledTickExprIntervals("[$today,[1981..1999]]#mon", 992, 992, mondays);
        // The 4,957 workdays of 1981-1999 exceed the cap, and their union, 992 runs, does not.
        final LongList workdayRuns = new LongList();
        addDayRuns(workdayRuns, LocalDate.of(1981, 1, 1), LocalDate.of(1999, 12, 31), EnumSet.range(DayOfWeek.MONDAY, DayOfWeek.FRIDAY), ZoneOffset.UTC);
        Assert.assertEquals(2 * 992, workdayRuns.size());
        addLocalDays(workdayRuns, LocalDate.of(2024, 1, 8), LocalDate.of(2024, 1, 9), ZoneOffset.UTC);
        assertCompiledTickExprIntervals("[$today,[1981..1999]]#workday", 993, 993, workdayRuns);
        assertCompiledTickExpr("[$today,[1981..1999]]#workday");
        // the 36,524 days of 1800-1899 join into one interval
        final LongList century = new LongList();
        addLocalDays(century, LocalDate.of(1800, 1, 1), LocalDate.of(1900, 1, 1), ZoneOffset.UTC);
        addLocalDays(century, LocalDate.of(2024, 1, 8), LocalDate.of(2024, 1, 9), ZoneOffset.UTC);
        assertCompiledTickExprIntervals("[$today,[1800..1899]]#mon,tue,wed,thu,fri,sat,sun", 2, 2, century);
        assertCompiledTickExpr("[$today,[1800..1899]]#mon,tue,wed,thu,fri,sat,sun");
    }

    @Test
    public void testCompiledListStaticIntervalsOverMaxIntervalsOutsideNanoRange() throws SqlException {
        // TIMESTAMP_NS covers the years 1677-2262 only
        Assume.assumeTrue(timestampType == TestTimestampType.MICRO);
        // The 3,287,182 days of 1000-9999 join into one interval, which holds $today, the Monday
        // 2024-01-08, and their runs from Monday to Friday exceed the cap (1,024).
        final LongList millennia = new LongList();
        addLocalDays(millennia, LocalDate.of(1000, 1, 1), LocalDate.of(10_000, 1, 1), ZoneOffset.UTC);
        assertCompiledTickExprIntervals("[$today,[1000..9999]]#mon,tue,wed,thu,fri,sat,sun", 2, 2, millennia);
        assertCompiledAndParsedOverMaxIntervals("[$today,[1000..9999]]#workday");
    }

    @Test
    public void testCompiledListStaticIntervalsPeriodic() throws SqlException {
        // A periodic element expands into its repetitions before the list counts them: 1,024 hours
        // from 09:00, one a day, stay within the cap (1,024), and 1,025 do not. $today, the Monday
        // 2024-01-08, widens the hour of that day to the whole day.
        final LocalDate today = LocalDate.of(2024, 1, 8);
        final LongList hours = new LongList();
        for (LocalDate day = LocalDate.of(2024, 1, 1); !day.isAfter(LocalDate.of(2026, 10, 20)); day = day.plusDays(1)) {
            if (day.equals(today)) {
                addLocalDays(hours, day, day.plusDays(1), ZoneOffset.UTC);
            } else {
                addLocalBlock(hours, day.atTime(9, 0), ZoneOffset.UTC, Micros.HOUR_MICROS);
            }
        }
        Assert.assertEquals(2 * 1_024, hours.size());
        assertCompiledTickExprIntervals("[$today,2024-01-01T09:00;1h;1d;1024]", 1_025, 1_025, hours);
        assertCompiledAndParsedOverMaxIntervals("[$today,2024-01-01T09:00;1h;1d;1025]");
        // the 1,024 Mondays among 7,168 days, the last on 2043-08-10, and the 1,025 among 7,169
        final LongList mondayHours = new LongList();
        for (LocalDate day = LocalDate.of(2024, 1, 1); !day.isAfter(LocalDate.of(2043, 8, 10)); day = day.plusWeeks(1)) {
            if (day.equals(today)) {
                addLocalDays(mondayHours, day, day.plusDays(1), ZoneOffset.UTC);
            } else {
                addLocalBlock(mondayHours, day.atTime(9, 0), ZoneOffset.UTC, Micros.HOUR_MICROS);
            }
        }
        Assert.assertEquals(2 * 1_024, mondayHours.size());
        assertCompiledTickExprIntervals("[$today,2024-01-01T09:00;1h;1d;7168]#mon", 1_025, 1_025, mondayHours);
        assertCompiledAndParsedOverMaxIntervals("[$today,2024-01-01T09:00;1h;1d;7169]#mon");

        // The list unions its static intervals once they exceed the cap and the merge threshold (256)
        // together: 1,280 hours wait for the four years that cover them, while 1,281 fail before.
        // parseTickExpr unions a list whenever it grew by the threshold, so it rejects both.
        final LongList fourYears = new LongList();
        addLocalDays(fourYears, LocalDate.of(2024, 1, 1), LocalDate.of(2028, 1, 1), ZoneOffset.UTC);
        assertCompiledTickExprIntervals("[$today,2024-01-01T09:00;1h;1d;1280,2024-01-01;4y]", 2, 2, fourYears);
        assertCompiledAndParsedOverMaxIntervals("[$today,2024-01-01T09:00;1h;1d;1281,2024-01-01;4y]");
        // the filter drops every repetition, each on a Saturday, which leaves $today
        final LongList todayOnly = new LongList();
        addLocalDays(todayOnly, today, today.plusDays(1), ZoneOffset.UTC);
        assertCompiledTickExprIntervals("[$today,2024-01-06T09:00;1h;7d;2000]#mon", 1, 1, todayOnly);
        // and so does the filter of the 16 x 16 = 256 seconds of the Sunday 2024-01-07, which the
        // expansion runs on its leaves at its union by the merge threshold (256), before it ends
        assertCompiledTickExprIntervals("[$today,2024-01-07T[00..15]:[00..15]:00]#mon", 1, 1, todayOnly);
    }

    @Test
    public void testCompiledListStaticIntervalsZoned() throws SqlException {
        // The list converts each static interval to UTC on its own, as evaluate() converts the date
        // variables, before it unions them. America/Santiago and Asia/Beirut go back from 00:00 to
        // 23:00 at the end of a Saturday, and that Saturday covers both occurrences of its repeated
        // hour, so it ends where the Sunday starts in UTC: in America/Santiago,
        // [2024-04-06T03:00Z, 2024-04-07T03:59:59.999999Z] and [2024-04-07T04:00Z, ...]. The 1,774
        // weekend days of 2008-2024 exceed the cap (1,024), and their union, a run for each of the
        // 887 weekends, does not. $today, the Monday 2024-01-08, fails the filter.
        final ZoneId santiago = ZoneId.of("America/Santiago");
        final LongList santiagoWeekends = unionOfZonedWeekendDays(santiago, 2008, 2024);
        Assert.assertEquals(2 * 887, santiagoWeekends.size());
        assertCompiledTickExprIntervals("[$today,[2008..2024]]@America/Santiago#weekend", 888, 888, santiagoWeekends);
        assertTickExprIntervals(configuration, santiagoWeekends, "[2024-01-08,[2008..2024]]@America/Santiago#weekend");
        assertCompiledTickExpr("[$today,[2008..2024]]@America/Santiago#weekend");
        final LongList beirutWeekends = unionOfZonedWeekendDays(ZoneId.of("Asia/Beirut"), 2008, 2024);
        Assert.assertEquals(2 * 887, beirutWeekends.size());
        assertCompiledTickExprIntervals("[$today,[2008..2024]]@Asia/Beirut#weekend", 888, 888, beirutWeekends);
        assertTickExprIntervals(configuration, beirutWeekends, "[2024-01-08,[2008..2024]]@Asia/Beirut#weekend");
        assertCompiledTickExpr("[$today,[2008..2024]]@Asia/Beirut#weekend");

        // Across elements: 12 copies of the days 1-28 of May to August 2023, which keep one offset in
        // America/Santiago, take the list past the cap and the threshold (256), so the list unions
        // its intervals in UTC, where the days 2024-04-01 to 04-10 form one run across the clock change.
        final String summerDays = repeatedValues("2023-[05..08]-[01..28]", 12);
        final LongList summerAndToday = new LongList();
        addZonedLocalDay(summerAndToday, LocalDate.of(2024, 1, 8), santiago);
        for (int month = 5; month <= 8; month++) {
            for (int day = 1; day <= 28; day++) {
                addZonedLocalDay(summerAndToday, LocalDate.of(2023, month, day), santiago);
            }
        }
        final LongList aprilDays = new LongList();
        aprilDays.add(summerAndToday);
        for (int day = 1; day <= 10; day++) {
            addZonedLocalDay(aprilDays, LocalDate.of(2024, 4, day), santiago);
        }
        final LongList april = unionOf(aprilDays);
        Assert.assertEquals(2 * 6, april.size());
        final String aprilList = "[$today,2024-04-[01..10]," + summerDays + "]@America/Santiago";
        assertCompiledTickExprIntervals(aprilList, 6, 6, april);
        assertCompiledTickExpr(aprilList);

        // The minutes 00:30 to 00:59 of 2024-09-08 lie in the gap of America/Santiago, which jumps from
        // 00:00 to 01:00, so no instant has their local times, and the list drops them: it keeps
        // $today and the four runs of the summer days.
        final LongList gap = unionOf(summerAndToday);
        Assert.assertEquals(2 * 5, gap.size());
        final String gapList = "[$today,2024-09-08T00:[30..59]," + summerDays + "]@America/Santiago";
        assertCompiledTickExprIntervals(gapList, 5, 5, gap);
        assertCompiledTickExpr(gapList);

        // America/New_York jumps from 02:00 to 03:00 on 2024-03-10, so the minute 01:59 with a duration
        // of 2 minutes ends in the gap, which moves the end alone. The compiler converts the static
        // interval as parseTickExpr does, on the Monday 2024-03-11.
        assertCompiledTickExprWithNow("[$today,2024-03-10T01:59;2m]@America/New_York", "2024-03-11T12:00:00.000000Z");

        // A numeric offset shifts every interval alike: the 992 runs from Monday to Friday of
        // 1981-1999 at +05:00.
        final LongList workdayRuns = new LongList();
        addDayRuns(workdayRuns, LocalDate.of(1981, 1, 1), LocalDate.of(1999, 12, 31), EnumSet.range(DayOfWeek.MONDAY, DayOfWeek.FRIDAY), ZoneOffset.ofHours(5));
        addLocalDays(workdayRuns, LocalDate.of(2024, 1, 8), LocalDate.of(2024, 1, 9), ZoneOffset.ofHours(5));
        assertCompiledTickExprIntervals("[$today,[1981..1999]]@+05:00#workday", 993, 993, workdayRuns);
        assertCompiledTickExpr("[$today,[1981..1999]]@Asia/Beirut#workday");
    }

    @Test
    public void testCompiledRangeEqual() throws SqlException {
        final TimestampDriver driver = timestampType.getDriver();
        final String expression = "$now..$now";
        final long now = driver.parseFloorLiteral("2026-03-15T12:00:00.000000Z");
        try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(
                driver, configuration, expression, 0, expression.length(), 0)) {
            compiled.evaluate(out, now);
            Assert.assertEquals(2, out.size());
            Assert.assertEquals(now, out.getQuick(0));
            Assert.assertEquals(now, out.getQuick(1));
        }
    }

    @Test
    public void testCompiledRangeReversed() throws SqlException {
        // A range between two points in time that starts after it ends fails in a user's query, in
        // the compiled form as in the parser. An engine job adds no interval for it, and keeps the
        // intervals already in the output, see TickExprEngineJobTest#testInvertedRangeAddsNoInterval.
        assertParsedAndCompiledErrorWithNow("$now..$now-1h", "2026-03-15T12:00:00.000000Z", "Invalid date range: start is after end");
    }

    @Test
    public void testCompiledStaticListCapFuzz() throws SqlException {
        // Builds random date lists of year ranges, years of months, periodic hours and days around the
        // clock changes of 2024 next to one to three copies of $today in random spellings, under a
        // day filter or a time, and maybe a zone. A list that compiles keeps one IR element for the
        // copies of $today and at most the cap (1,024) of static IR entries, evaluate() stays within
        // getMaxIntervalCount() every 6 hours through 2024, and at the Monday 2024-01-08 it returns
        // what parseTickExpr returns for the static equivalent whenever that parses. A list that
        // fails, fails with the cap error at position 0. Under #mon, alone or after a time, each
        // interval lies within one Monday, and two Mondays never touch, so the union of a list only
        // grows with its elements, and the static equivalent fails too. The lists must reach each
        // shape that the assertions at the end name, so the test builds more lists, up to ten times
        // as many, until they do.
        final int listCount = Integer.getInteger("questdb.fuzz.tick.lists", 300);
        final String[] suffixes = {"#mon", "#workday", "#weekend", "T09:30#mon", "T09:30;25h"};
        final String[] zones = {"", "@+05:00", "@Europe/London", "@America/Santiago", "@Asia/Beirut"};
        final String overCapError = "Bracket expansion produces too many intervals (max 1024)";
        final Rnd rnd = TestUtils.generateRandom(LOG);
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final long start = timestampDriver.parseFloorLiteral("2024-01-01T00:00:00.000000Z");
        final long end = timestampDriver.parseFloorLiteral("2025-01-01T00:00:00.000000Z");
        final long step = timestampDriver.fromHours(6);
        final ObjList<String> elements = new ObjList<>();
        final ObjList<int[]> elementCounts = new ObjList<>();
        final ObjList<String> listElements = new ObjList<>();
        final TreeSet<LocalDate> keptDays = new TreeSet<>();
        final StringSink expression = new StringSink();
        final StringSink staticEquivalentList = new StringSink();
        final StringSink staticElementList = new StringSink();
        final StringSink spelling = new StringSink();
        final LongList intervals = new LongList();
        final LongList parsed = new LongList();
        boolean hasUnionWithinCap = false;
        boolean hasZonedUnionAcrossFallBack = false;
        boolean hasElementOverCap = false;
        boolean hasElementsWithinCapOverCap = false;
        for (
                int i = 0;
                i < listCount || (i < 10 * listCount && !(hasUnionWithinCap && hasZonedUnionAcrossFallBack && hasElementOverCap && hasElementsWithinCapOverCap));
                i++
        ) {
            // America/Santiago and Asia/Beirut go back at local midnight. A list in either zone keeps
            // the days around the clock change, under #weekend or every day of T09:30;25h, half the
            // time, and draws the days around its clock changes twice as often, mostly those of the
            // fall-back.
            final String zone = zones[rnd.nextInt(zones.length)];
            final boolean isMidnightFallBackZone = zone.equals("@America/Santiago") || zone.equals("@Asia/Beirut");
            final String suffix = isMidnightFallBackZone && rnd.nextBoolean()
                    ? (rnd.nextBoolean() ? "#weekend" : "T09:30;25h")
                    : suffixes[rnd.nextInt(suffixes.length)];
            final boolean hasTime = suffix.charAt(0) == 'T';
            final Set<DayOfWeek> keptWeekdays = switch (suffix) {
                case "#workday" -> EnumSet.range(DayOfWeek.MONDAY, DayOfWeek.FRIDAY);
                case "#weekend" -> EnumSet.of(DayOfWeek.SATURDAY, DayOfWeek.SUNDAY);
                case "T09:30;25h" -> EnumSet.allOf(DayOfWeek.class);
                default -> EnumSet.of(DayOfWeek.MONDAY);
            };
            elements.clear();
            elementCounts.clear();
            keptDays.clear();
            int rawCount = 0;
            int maxElementUnionCount = 0;
            for (int e = 0, n = 2 + rnd.nextInt(5); e < n; e++) {
                int kind = rnd.nextInt(isMidnightFallBackZone ? 6 : 5);
                if (kind == 5) {
                    kind = 3;
                }
                if (kind == 4 && e > 0) {
                    // a copy of an earlier element
                    final int copied = rnd.nextInt(e);
                    elements.add(elements.getQuick(copied));
                    elementCounts.add(elementCounts.getQuick(copied));
                    rawCount += elementCounts.getQuick(copied)[0];
                    continue;
                }
                if (hasTime && (kind == 0 || kind == 2)) {
                    // a bracketed year or a repetition takes no time of the list
                    kind = kind == 0 ? 1 : 3;
                }
                LocalDate first;
                LocalDate last;
                boolean isWholeDay = true;
                switch (kind) {
                    case 0 -> {
                        final int firstYear = 1981 + rnd.nextInt(30);
                        final int lastYear = firstYear + rnd.nextInt(25);
                        elements.add("[" + firstYear + ".." + lastYear + ']');
                        first = LocalDate.of(firstYear, 1, 1);
                        last = LocalDate.of(lastYear, 12, 31);
                    }
                    case 2 -> {
                        final int count = 900 + rnd.nextInt(401);
                        elements.add("2024-01-01T09:00;1h;1d;" + count);
                        first = LocalDate.of(2024, 1, 1);
                        last = first.plusDays(count - 1);
                        isWholeDay = false;
                    }
                    case 3 -> {
                        // Days around a clock change at local midnight in America/Santiago (back in
                        // April, forward in September) and Asia/Beirut (forward in March, back in
                        // October), and at 01:00 UTC in Europe/London (March and October); a named zone
                        // takes its own months.
                        final String days = switch (zone) {
                            case "@America/Santiago" -> rnd.nextInt(4) > 0 ? "2024-04-[01..10]" : "2024-09-[06..09]";
                            case "@Asia/Beirut" -> rnd.nextInt(4) > 0 ? "2024-10-[25..28]" : "2024-03-[28..31]";
                            case "@Europe/London" -> rnd.nextBoolean() ? "2024-10-[25..28]" : "2024-03-[28..31]";
                            default -> switch (rnd.nextInt(4)) {
                                case 0 -> "2024-04-[01..10]";
                                case 1 -> "2024-09-[06..09]";
                                case 2 -> "2024-03-[28..31]";
                                default -> "2024-10-[25..28]";
                            };
                        };
                        elements.add(days);
                        final int month = Integer.parseInt(days.substring(5, 7));
                        first = LocalDate.of(2024, month, Integer.parseInt(days.substring(9, 11)));
                        last = LocalDate.of(2024, month, Integer.parseInt(days.substring(13, 15)));
                    }
                    default -> {
                        final int year = 1981 + rnd.nextInt(44);
                        elements.add(year + "-[01..12]");
                        first = LocalDate.of(year, 1, 1);
                        last = LocalDate.of(year, 12, 31);
                    }
                }
                // An element yields one interval per day that the filter keeps. Whole days, and days
                // of 25 hours, join into runs of consecutive days; hours and minutes stay apart.
                int keptCount = 0;
                int runCount = 0;
                LocalDate previousKeptDay = null;
                for (LocalDate day = first; !day.isAfter(last); day = day.plusDays(1)) {
                    if (keptWeekdays.contains(day.getDayOfWeek())) {
                        keptCount++;
                        if (previousKeptDay == null || !previousKeptDay.plusDays(1).equals(day)) {
                            runCount++;
                        }
                        previousKeptDay = day;
                        if (isWholeDay) {
                            keptDays.add(day);
                        }
                    }
                }
                final int unionCount = isWholeDay && (!hasTime || suffix.endsWith(";25h")) ? runCount : keptCount;
                elementCounts.add(new int[]{keptCount, unionCount});
                rawCount += keptCount;
            }
            for (int e = 0, n = elementCounts.size(); e < n; e++) {
                maxElementUnionCount = Math.max(maxElementUnionCount, elementCounts.getQuick(e)[1]);
            }
            // the Saturday that ends at a clock change from 00:00 to 23:00 and the Sunday after it
            final boolean hasFallBackPair = (zone.equals("@America/Santiago") && keptDays.contains(LocalDate.of(2024, 4, 6)) && keptDays.contains(LocalDate.of(2024, 4, 7)))
                    || (zone.equals("@Asia/Beirut") && keptDays.contains(LocalDate.of(2024, 10, 26)) && keptDays.contains(LocalDate.of(2024, 10, 27)));

            // $today one to three times, each at a random place in a random spelling
            listElements.clear();
            listElements.addAll(elements);
            for (int c = 0, copyCount = 1 + rnd.nextInt(3); c < copyCount; c++) {
                spelling.clear();
                appendDateVariableSpelling(rnd, spelling, new OffsetDateVariable("$today", "", 0));
                listElements.insert(rnd.nextInt(listElements.size() + 1), 1, spelling.toString());
            }
            // the list, the list with the Monday 2024-01-08 for $today, and the list without $today
            expression.clear();
            staticEquivalentList.clear();
            staticElementList.clear();
            for (int e = 0, n = listElements.size(); e < n; e++) {
                final String element = listElements.getQuick(e);
                final boolean isToday = element.charAt(0) == '$';
                expression.put(e == 0 ? '[' : ',').put(element);
                staticEquivalentList.put(e == 0 ? '[' : ',').put(isToday ? "2024-01-08" : element);
                if (!isToday) {
                    staticElementList.put(staticElementList.length() == 0 ? '[' : ',').put(element);
                }
            }
            final int filterIndex = suffix.indexOf('#');
            final String listSuffix = ']' + (hasTime ? "T09:30" : "") + zone + (filterIndex >= 0 ? suffix.substring(filterIndex) : ";25h");
            final String tick = expression + listSuffix;
            final String staticEquivalent = staticEquivalentList + listSuffix;
            final String staticElements = staticElementList + listSuffix;

            final CompiledTickExpression compiled;
            try {
                compiled = IntervalUtils.compileTickExpr(timestampDriver, configuration, tick, 0, tick.length(), 0);
            } catch (SqlException e) {
                Assert.assertEquals(tick, 0, e.getPosition());
                Assert.assertEquals(tick, overCapError, e.getFlyweightMessage().toString());
                if (suffix.endsWith("#mon")) {
                    assertTickExprErrorAtMonday(staticEquivalent, overCapError);
                }
                if (maxElementUnionCount > 1_024) {
                    hasElementOverCap = true;
                } else {
                    hasElementsWithinCapOverCap = true;
                }
                continue;
            }
            try (compiled) {
                // $today is the only date variable, and the list stores its copies as one element next
                // to the STATIC entries of the list without them
                try (CompiledTickExpression staticOnly = IntervalUtils.compileTickExpr(timestampDriver, configuration, staticElements, 0, staticElements.length(), 0)) {
                    Assert.assertEquals(tick, staticOnly.getElementCount() + 1, compiled.getElementCount());
                }
                Assert.assertTrue(tick + ": " + compiled.getElementCount(), compiled.getElementCount() - 1 <= 1_024);
                final int bound = compiled.getMaxIntervalCount(Integer.MAX_VALUE);
                Assert.assertTrue(tick, bound > 0);
                for (long now = start; now < end; now += step) {
                    intervals.clear();
                    try {
                        compiled.evaluate(intervals, now);
                    } catch (CairoException e) {
                        // the evaluation stops where the parser stops, on the interval cap, see below
                        TestUtils.assertContains(e.getFlyweightMessage(), overCapError);
                        continue;
                    }
                    if (intervals.size() / 2 > bound) {
                        Assert.fail(tick + " yields " + intervals.size() / 2 + " intervals, above its bound of " + bound
                                + ", at " + timestampDriver.toMSecString(now));
                    }
                }
                intervals.clear();
                try {
                    compiled.evaluate(intervals, mondayNow());
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), overCapError);
                    // the static equivalent fails as the evaluation does
                    assertTickExprErrorAtMonday(staticEquivalent, overCapError);
                    continue;
                }
            }
            parsed.clear();
            boolean isParsed = true;
            try {
                parseTickExprAtMonday(configuration, parsed, staticEquivalent);
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), overCapError);
                isParsed = false;
            }
            if (isParsed) {
                Assert.assertEquals(tick, intervalToString(timestampDriver, parsed).toString(), intervalToString(timestampDriver, intervals).toString());
            }
            hasUnionWithinCap |= rawCount > 1_024;
            hasZonedUnionAcrossFallBack |= rawCount > 1_024 && hasFallBackPair;
        }
        Assert.assertTrue("a list above the cap that its union takes within", hasUnionWithinCap);
        Assert.assertTrue("such a list whose union in a zone crosses a clock change at local midnight", hasZonedUnionAcrossFallBack);
        Assert.assertTrue("a list that fails on an element above the cap", hasElementOverCap);
        Assert.assertTrue("a list that fails on elements within the cap", hasElementsWithinCapOverCap);
    }

    @Test
    public void testCompiledTickExprAllSuffixesCombined() throws SqlException {
        assertCompiledTickExpr("$today..$today + 5dT09:30@+02:00;1h");
    }

    @Test
    public void testCompiledTickExprAllWhitespace() {
        assertCompileTickExprError("   ", "Empty tick expression");
    }

    @Test
    public void testCompiledTickExprBareT() {
        assertCompileTickExprError("[$today]T", "T with no value");
    }

    @Test
    public void testCompiledTickExprBracketExpansionInList() throws SqlException {
        assertCompiledTickExpr("[2025-01-[13..15], $today]");
    }

    @Test
    public void testCompiledTickExprBusinessDayRange() throws SqlException {
        assertCompiledTickExpr("$today..$today + 5bd");
    }

    @Test
    public void testCompiledTickExprCacheReuseProducesDifferentResults() throws SqlException {
        // Same CompiledTickExpression evaluated with different "now" must produce different results
        final TimestampDriver timestampDriver = timestampType.getDriver();
        try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(
                timestampDriver, configuration, "$today", 0, 6, 0)) {
            long now1 = timestampDriver.parseFloorLiteral("2026-01-15T10:00:00.000000Z");
            LongList result1 = new LongList();
            compiled.evaluate(result1, now1);

            long now2 = timestampDriver.parseFloorLiteral("2026-06-20T10:00:00.000000Z");
            LongList result2 = new LongList();
            compiled.evaluate(result2, now2);

            Assert.assertNotEquals("Cached query should produce different results for different days",
                    result1.getQuick(0), result2.getQuick(0));
        }
    }

    @Test
    public void testCompiledTickExprDayFilterMon() throws SqlException {
        assertCompiledTickExpr("$today..$today + 7d#Mon");
    }

    // ================= End ISO Week Date Tests =================

    @Test
    public void testCompiledTickExprDayFilterMultipleDays() throws SqlException {
        assertCompiledTickExpr("$today..$today + 14d#Mon,Wed,Fri");
    }

    @Test
    public void testCompiledTickExprDayFilterWeekend() throws SqlException {
        assertCompiledTickExpr("$today..$today + 7d#weekend");
    }

    @Test
    public void testCompiledTickExprDayFilterWithDuration() throws SqlException {
        assertCompiledTickExpr("$today..$today + 7d#Mon;4h");
    }

    @Test
    public void testCompiledTickExprDayFilterWithTimezone() throws SqlException {
        assertCompiledTickExpr("$today..$today + 7d@+01:00#workday");
    }

    @Test
    public void testCompiledTickExprDayFilterWorkday() throws SqlException {
        assertCompiledTickExpr("$today..$today + 7d#workday");
    }

    @Test
    public void testCompiledTickExprDstFallBack() throws SqlException {
        // Nov 3, 2024 in America/New_York: clocks fall back at 2:00 AM EDT, so 01:30 happens twice,
        // at 05:30Z and at 06:30Z, and the expression covers both
        assertParsedAndCompiledWithNow(
                "[{lo=2024-11-03T05:30:00.000000Z, hi=2024-11-03T05:30:59.999999Z},{lo=2024-11-03T06:30:00.000000Z, hi=2024-11-03T06:30:59.999999Z}]",
                "$todayT01:30@America/New_York",
                "2024-11-03T12:00:00.000000Z"
        );
    }

    @Test
    public void testCompiledTickExprDstGapHiOnly() throws SqlException {
        // March 10, 2024: spring forward at 2:00 AM in America/New_York
        // T01:59 + 2m duration → hi lands at 02:01 (in the DST gap), lo is before the gap
        assertCompiledTickExprWithNow("$todayT01:59@America/New_York;2m", "2024-03-10T12:00:00.000000Z");
    }

    @Test
    public void testCompiledTickExprDstPerElementSummer() throws SqlException {
        // Per-element named tz during summer (BST, UTC+1)
        assertCompiledTickExprWithNow("[$today]T[09:00@Europe/London,14:00]", "2024-07-15T12:00:00.000000Z");
    }

    @Test
    public void testCompiledTickExprDstRangeSpanningTransition() throws SqlException {
        // Range spanning the spring forward DST transition in America/New_York
        assertCompiledTickExprWithNow("$today..$today+7dT09:00@America/New_York", "2024-03-07T12:00:00.000000Z");
    }

    @Test
    public void testCompiledTickExprDstSpringForward() throws SqlException {
        // March 10, 2024 2:00 AM in America/New_York: clocks spring forward to 3:00 AM
        // T02:30 falls in the DST gap
        assertCompiledTickExprWithNow("$todayT02:30@America/New_York", "2024-03-10T12:00:00.000000Z");
    }

    @Test
    public void testCompiledTickExprDstSummerVsWinter() throws SqlException {
        // Europe/London: winter (UTC+0) vs summer (BST, UTC+1)
        // Same expression, different UTC offsets depending on time of year
        assertCompiledTickExprWithNow("$todayT08:00@Europe/London", "2024-01-15T12:00:00.000000Z");
        assertCompiledTickExprWithNow("$todayT08:00@Europe/London", "2024-07-15T12:00:00.000000Z");
    }

    @Test
    public void testCompiledTickExprDurationDigitsOnly() {
        assertCompileTickExprError("$today;30", "Missing unit at end of duration");
    }

    @Test
    public void testCompiledTickExprDurationManyParts() throws SqlException {
        // 65 duration parts to trigger ir[] growth beyond initial capacity of 64
        assertCompiledTickExpr("$today;" + "1s".repeat(65));
    }

    @Test
    public void testCompiledTickExprDurationMissingUnit() {
        assertCompileTickExprError("$today;1h30", "Missing unit at end of duration");
    }

    @Test
    public void testCompiledTickExprDurationMoreThan255Parts() throws SqlException {
        // The duration part count used to take 8 bits, so 300 parts read as 44.
        assertCompiledTickExpr("$todayT09:30;" + "1s".repeat(300));
    }

    @Test
    public void testCompiledTickExprDurationMultiPart() throws SqlException {
        assertCompiledTickExpr("$today;1h30m15s");
    }

    @Test
    public void testCompiledTickExprDurationOnly() throws SqlException {
        assertCompiledTickExpr("$today;6h30m");
    }

    @Test
    public void testCompiledTickExprNegativeDuration() throws SqlException {
        assertCompiledTickExpr("$today;-3d");
    }

    @Test
    public void testCompiledTickExprNegativeDurationMixedSigns() throws SqlException {
        assertCompiledTickExpr("$today;-3d2h");
    }

    @Test
    public void testCompiledTickExprNegativeDurationMultiNegative() throws SqlException {
        assertCompiledTickExpr("$today;-3d-2h");
    }

    @Test
    public void testCompiledTickExprNegativeDurationWithTimeOverride() throws SqlException {
        assertCompiledTickExpr("[$today, $tomorrow]T[09:30,14:00];-5m");
    }

    @Test
    public void testCompiledTickExprOver255Elements() throws SqlException {
        // 300 date variables, more elements than the compiled expression could once count
        final StringBuilder expression = new StringBuilder("[$today");
        for (int day = 1; day < 300; day++) {
            expression.append(",$today+").append(day).append('d');
        }
        expression.append(']');
        assertParsedAndCompiledWithNow(
                "[{lo=2024-03-01T00:00:00.000000Z, hi=2024-12-25T23:59:59.999999Z}]",
                expression.toString(),
                "2024-03-01T12:00:00.000000Z"
        );
    }

    @Test
    public void testCompiledTickExprOver255StaticIntervals() throws SqlException {
        // The static element expands to 480 half-hourly intervals that do not merge, more
        // elements than the compiled expression could once count
        final String expression = "[$today,2024-01-[01..10]T[00..23]:[00,30]]";
        final String now = "2024-03-01T12:00:00.000000Z";
        final TimestampDriver timestampDriver = timestampType.getDriver();
        out.clear();
        parseTickExprWithNow(timestampDriver, expression, 0, expression.length(), 0, out, timestampDriver.parseFloorLiteral(now));
        Assert.assertEquals(481, out.size() / 2);
        assertCompiledTickExprWithNow(expression, now);
    }

    @Test
    public void testCompiledTickExprPositiveDurationExplicitSign() throws SqlException {
        assertCompiledTickExpr("$today;+3d");
    }

    @Test
    public void testCompiledTickExprZeroDuration() throws SqlException {
        assertCompiledTickExpr("$today;0d");
    }

    @Test
    public void testCompiledTickExprDurationOver255Parts() throws SqlException {
        // 260 duration parts of 1s each, more than the compiled expression could once count
        assertParsedAndCompiledWithNow(
                "[{lo=2024-03-01T09:00:00.000000Z, hi=2024-03-01T09:04:19.999999Z}]",
                "[$today]T09:00;" + "1s".repeat(260),
                "2024-03-01T12:00:00.000000Z"
        );
    }

    @Test
    public void testCompiledTickExprDurationOverflow() {
        assertCompileTickExprError("$today;99999999999h", "Duration not a number");
    }

    @Test
    public void testCompiledTickExprDurationSpaceBeforeUnit() {
        assertCompileTickExprError("$today; h", "Expected number before unit");
    }

    @Test
    public void testCompiledTickExprDurationWithUnderscore() throws SqlException {
        assertCompiledTickExpr("$today;1_000s");
    }

    @Test
    public void testCompiledTickExprDynamicBracketList() throws SqlException {
        assertCompiledTickExpr("[$today, $tomorrow]");
    }

    @Test
    public void testCompiledTickExprDynamicMixedList() throws SqlException {
        assertCompiledTickExpr("[2025-01-01, $today, 2025-06-15]");
    }

    @Test
    public void testCompiledTickExprDynamicNow() throws SqlException {
        assertCompiledTickExpr("$now");
    }

    @Test
    public void testCompiledTickExprDynamicNowDuration() throws SqlException {
        assertCompiledTickExpr("$now;1h");
    }

    @Test
    public void testCompiledTickExprDynamicNowNegativeDuration() throws SqlException {
        assertCompiledTickExpr("$now;-1h");
    }

    @Test
    public void testCompiledTickExprDynamicNowMinusHours() throws SqlException {
        assertCompiledTickExpr("$now - 2h");
    }

    @Test
    public void testCompiledTickExprDynamicOffsetUppercaseD() throws SqlException {
        assertCompiledTickExpr("$today + 3D");
    }

    @Test
    public void testCompiledTickExprDynamicRange() throws SqlException {
        assertCompiledTickExpr("$today..$today + 2d");
    }

    @Test
    public void testCompiledTickExprDynamicRangeTimeLevelWithDuration() throws SqlException {
        assertCompiledTickExpr("$now - 2h..$now;1h");
    }

    @Test
    public void testCompiledTickExprDynamicToday() throws SqlException {
        assertCompiledTickExpr("$today");
    }

    @Test
    public void testCompiledTickExprDynamicTodayMinusBusinessDays() throws SqlException {
        assertCompiledTickExpr("$today - 3bd");
    }

    @Test
    public void testCompiledTickExprDynamicTodayPlusBusinessDays() throws SqlException {
        assertCompiledTickExpr("$today + 3bd");
    }

    @Test
    public void testCompiledTickExprDynamicTodayPlusZeroBusinessDays() throws SqlException {
        assertCompiledTickExpr("$today + 0bd");
    }

    @Test
    public void testCompiledTickExprDynamicTomorrow() throws SqlException {
        assertCompiledTickExpr("$tomorrow");
    }

    @Test
    public void testCompiledTickExprDynamicWithTimeSuffix() throws SqlException {
        assertCompiledTickExpr("$todayT09:30;1h");
    }

    @Test
    public void testCompiledTickExprDynamicYesterday() throws SqlException {
        assertCompiledTickExpr("$yesterday");
    }

    @Test
    public void testCompiledTickExprEmptyElementInDateList() {
        assertCompileTickExprError("[$today,, $tomorrow]", "Empty element in date list");
    }

    @Test
    public void testCompiledTickExprIntervalLimit() throws SqlException {
        // A literal with a date variable evaluates through its IR, which must stop at the
        // interval limit where the parser stops, with the same error. The parser checks the
        // merged intervals after each day of a date variable range, and the final result.
        final String now = "2024-01-02T10:30:00.000000Z";
        assertParsedAndCompiledErrorWithNow("[$today..$today + 2000d]T09:00", now, "Date range expansion produces too many intervals (max 1024)");
        assertParsedAndCompiledErrorWithNow("[$today - 2000d..$now]T09:30;1h", now, "Date range expansion produces too many intervals (max 1024)");
        assertParsedAndCompiledErrorWithNow("$today..$today + 600dT[09:00,15:00]", now, "Date range expansion produces too many intervals (max 1024)");
        // a single day with 1,100 separate seconds reaches the limit only in the final result
        assertParsedAndCompiledErrorWithNow("[$today]T[00:00:00,00:00:02,00:00:04,00:00:06,00:00:08,00:00:10,00:00:12,00:00:14,00:00:16,00:00:18,00:00:20,00:00:22,00:00:24,00:00:26,00:00:28,00:00:30,00:00:32,00:00:34,00:00:36,00:00:38,00:00:40,00:00:42,00:00:44,00:00:46,00:00:48,00:00:50,00:00:52,00:00:54,00:00:56,00:00:58,00:01:00,00:01:02,00:01:04,00:01:06,00:01:08,00:01:10,00:01:12,00:01:14,00:01:16,00:01:18,00:01:20,00:01:22,00:01:24,00:01:26,00:01:28,00:01:30,00:01:32,00:01:34,00:01:36,00:01:38,00:01:40,00:01:42,00:01:44,00:01:46,00:01:48,00:01:50,00:01:52,00:01:54,00:01:56,00:01:58,00:02:00,00:02:02,00:02:04,00:02:06,00:02:08,00:02:10,00:02:12,00:02:14,00:02:16,00:02:18,00:02:20,00:02:22,00:02:24,00:02:26,00:02:28,00:02:30,00:02:32,00:02:34,00:02:36,00:02:38,00:02:40,00:02:42,00:02:44,00:02:46,00:02:48,00:02:50,00:02:52,00:02:54,00:02:56,00:02:58,00:03:00,00:03:02,00:03:04,00:03:06,00:03:08,00:03:10,00:03:12,00:03:14,00:03:16,00:03:18,00:03:20,00:03:22,00:03:24,00:03:26,00:03:28,00:03:30,00:03:32,00:03:34,00:03:36,00:03:38,00:03:40,00:03:42,00:03:44,00:03:46,00:03:48,00:03:50,00:03:52,00:03:54,00:03:56,00:03:58,00:04:00,00:04:02,00:04:04,00:04:06,00:04:08,00:04:10,00:04:12,00:04:14,00:04:16,00:04:18,00:04:20,00:04:22,00:04:24,00:04:26,00:04:28,00:04:30,00:04:32,00:04:34,00:04:36,00:04:38,00:04:40,00:04:42,00:04:44,00:04:46,00:04:48,00:04:50,00:04:52,00:04:54,00:04:56,00:04:58,00:05:00,00:05:02,00:05:04,00:05:06,00:05:08,00:05:10,00:05:12,00:05:14,00:05:16,00:05:18,00:05:20,00:05:22,00:05:24,00:05:26,00:05:28,00:05:30,00:05:32,00:05:34,00:05:36,00:05:38,00:05:40,00:05:42,00:05:44,00:05:46,00:05:48,00:05:50,00:05:52,00:05:54,00:05:56,00:05:58,00:06:00,00:06:02,00:06:04,00:06:06,00:06:08,00:06:10,00:06:12,00:06:14,00:06:16,00:06:18,00:06:20,00:06:22,00:06:24,00:06:26,00:06:28,00:06:30,00:06:32,00:06:34,00:06:36,00:06:38,00:06:40,00:06:42,00:06:44,00:06:46,00:06:48,00:06:50,00:06:52,00:06:54,00:06:56,00:06:58,00:07:00,00:07:02,00:07:04,00:07:06,00:07:08,00:07:10,00:07:12,00:07:14,00:07:16,00:07:18,00:07:20,00:07:22,00:07:24,00:07:26,00:07:28,00:07:30,00:07:32,00:07:34,00:07:36,00:07:38,00:07:40,00:07:42,00:07:44,00:07:46,00:07:48,00:07:50,00:07:52,00:07:54,00:07:56,00:07:58,00:08:00,00:08:02,00:08:04,00:08:06,00:08:08,00:08:10,00:08:12,00:08:14,00:08:16,00:08:18,00:08:20,00:08:22,00:08:24,00:08:26,00:08:28,00:08:30,00:08:32,00:08:34,00:08:36,00:08:38,00:08:40,00:08:42,00:08:44,00:08:46,00:08:48,00:08:50,00:08:52,00:08:54,00:08:56,00:08:58,00:09:00,00:09:02,00:09:04,00:09:06,00:09:08,00:09:10,00:09:12,00:09:14,00:09:16,00:09:18,00:09:20,00:09:22,00:09:24,00:09:26,00:09:28,00:09:30,00:09:32,00:09:34,00:09:36,00:09:38,00:09:40,00:09:42,00:09:44,00:09:46,00:09:48,00:09:50,00:09:52,00:09:54,00:09:56,00:09:58,00:10:00,00:10:02,00:10:04,00:10:06,00:10:08,00:10:10,00:10:12,00:10:14,00:10:16,00:10:18,00:10:20,00:10:22,00:10:24,00:10:26,00:10:28,00:10:30,00:10:32,00:10:34,00:10:36,00:10:38,00:10:40,00:10:42,00:10:44,00:10:46,00:10:48,00:10:50,00:10:52,00:10:54,00:10:56,00:10:58,00:11:00,00:11:02,00:11:04,00:11:06,00:11:08,00:11:10,00:11:12,00:11:14,00:11:16,00:11:18,00:11:20,00:11:22,00:11:24,00:11:26,00:11:28,00:11:30,00:11:32,00:11:34,00:11:36,00:11:38,00:11:40,00:11:42,00:11:44,00:11:46,00:11:48,00:11:50,00:11:52,00:11:54,00:11:56,00:11:58,00:12:00,00:12:02,00:12:04,00:12:06,00:12:08,00:12:10,00:12:12,00:12:14,00:12:16,00:12:18,00:12:20,00:12:22,00:12:24,00:12:26,00:12:28,00:12:30,00:12:32,00:12:34,00:12:36,00:12:38,00:12:40,00:12:42,00:12:44,00:12:46,00:12:48,00:12:50,00:12:52,00:12:54,00:12:56,00:12:58,00:13:00,00:13:02,00:13:04,00:13:06,00:13:08,00:13:10,00:13:12,00:13:14,00:13:16,00:13:18,00:13:20,00:13:22,00:13:24,00:13:26,00:13:28,00:13:30,00:13:32,00:13:34,00:13:36,00:13:38,00:13:40,00:13:42,00:13:44,00:13:46,00:13:48,00:13:50,00:13:52,00:13:54,00:13:56,00:13:58,00:14:00,00:14:02,00:14:04,00:14:06,00:14:08,00:14:10,00:14:12,00:14:14,00:14:16,00:14:18,00:14:20,00:14:22,00:14:24,00:14:26,00:14:28,00:14:30,00:14:32,00:14:34,00:14:36,00:14:38,00:14:40,00:14:42,00:14:44,00:14:46,00:14:48,00:14:50,00:14:52,00:14:54,00:14:56,00:14:58,00:15:00,00:15:02,00:15:04,00:15:06,00:15:08,00:15:10,00:15:12,00:15:14,00:15:16,00:15:18,00:15:20,00:15:22,00:15:24,00:15:26,00:15:28,00:15:30,00:15:32,00:15:34,00:15:36,00:15:38,00:15:40,00:15:42,00:15:44,00:15:46,00:15:48,00:15:50,00:15:52,00:15:54,00:15:56,00:15:58,00:16:00,00:16:02,00:16:04,00:16:06,00:16:08,00:16:10,00:16:12,00:16:14,00:16:16,00:16:18,00:16:20,00:16:22,00:16:24,00:16:26,00:16:28,00:16:30,00:16:32,00:16:34,00:16:36,00:16:38,00:16:40,00:16:42,00:16:44,00:16:46,00:16:48,00:16:50,00:16:52,00:16:54,00:16:56,00:16:58,00:17:00,00:17:02,00:17:04,00:17:06,00:17:08,00:17:10,00:17:12,00:17:14,00:17:16,00:17:18,00:17:20,00:17:22,00:17:24,00:17:26,00:17:28,00:17:30,00:17:32,00:17:34,00:17:36,00:17:38,00:17:40,00:17:42,00:17:44,00:17:46,00:17:48,00:17:50,00:17:52,00:17:54,00:17:56,00:17:58,00:18:00,00:18:02,00:18:04,00:18:06,00:18:08,00:18:10,00:18:12,00:18:14,00:18:16,00:18:18,00:18:20,00:18:22,00:18:24,00:18:26,00:18:28,00:18:30,00:18:32,00:18:34,00:18:36,00:18:38,00:18:40,00:18:42,00:18:44,00:18:46,00:18:48,00:18:50,00:18:52,00:18:54,00:18:56,00:18:58,00:19:00,00:19:02,00:19:04,00:19:06,00:19:08,00:19:10,00:19:12,00:19:14,00:19:16,00:19:18,00:19:20,00:19:22,00:19:24,00:19:26,00:19:28,00:19:30,00:19:32,00:19:34,00:19:36,00:19:38,00:19:40,00:19:42,00:19:44,00:19:46,00:19:48,00:19:50,00:19:52,00:19:54,00:19:56,00:19:58,00:20:00,00:20:02,00:20:04,00:20:06,00:20:08,00:20:10,00:20:12,00:20:14,00:20:16,00:20:18,00:20:20,00:20:22,00:20:24,00:20:26,00:20:28,00:20:30,00:20:32,00:20:34,00:20:36,00:20:38,00:20:40,00:20:42,00:20:44,00:20:46,00:20:48,00:20:50,00:20:52,00:20:54,00:20:56,00:20:58,00:21:00,00:21:02,00:21:04,00:21:06,00:21:08,00:21:10,00:21:12,00:21:14,00:21:16,00:21:18,00:21:20,00:21:22,00:21:24,00:21:26,00:21:28,00:21:30,00:21:32,00:21:34,00:21:36,00:21:38,00:21:40,00:21:42,00:21:44,00:21:46,00:21:48,00:21:50,00:21:52,00:21:54,00:21:56,00:21:58,00:22:00,00:22:02,00:22:04,00:22:06,00:22:08,00:22:10,00:22:12,00:22:14,00:22:16,00:22:18,00:22:20,00:22:22,00:22:24,00:22:26,00:22:28,00:22:30,00:22:32,00:22:34,00:22:36,00:22:38,00:22:40,00:22:42,00:22:44,00:22:46,00:22:48,00:22:50,00:22:52,00:22:54,00:22:56,00:22:58,00:23:00,00:23:02,00:23:04,00:23:06,00:23:08,00:23:10,00:23:12,00:23:14,00:23:16,00:23:18,00:23:20,00:23:22,00:23:24,00:23:26,00:23:28,00:23:30,00:23:32,00:23:34,00:23:36,00:23:38,00:23:40,00:23:42,00:23:44,00:23:46,00:23:48,00:23:50,00:23:52,00:23:54,00:23:56,00:23:58,00:24:00,00:24:02,00:24:04,00:24:06,00:24:08,00:24:10,00:24:12,00:24:14,00:24:16,00:24:18,00:24:20,00:24:22,00:24:24,00:24:26,00:24:28,00:24:30,00:24:32,00:24:34,00:24:36,00:24:38,00:24:40,00:24:42,00:24:44,00:24:46,00:24:48,00:24:50,00:24:52,00:24:54,00:24:56,00:24:58,00:25:00,00:25:02,00:25:04,00:25:06,00:25:08,00:25:10,00:25:12,00:25:14,00:25:16,00:25:18,00:25:20,00:25:22,00:25:24,00:25:26,00:25:28,00:25:30,00:25:32,00:25:34,00:25:36,00:25:38,00:25:40,00:25:42,00:25:44,00:25:46,00:25:48,00:25:50,00:25:52,00:25:54,00:25:56,00:25:58,00:26:00,00:26:02,00:26:04,00:26:06,00:26:08,00:26:10,00:26:12,00:26:14,00:26:16,00:26:18,00:26:20,00:26:22,00:26:24,00:26:26,00:26:28,00:26:30,00:26:32,00:26:34,00:26:36,00:26:38,00:26:40,00:26:42,00:26:44,00:26:46,00:26:48,00:26:50,00:26:52,00:26:54,00:26:56,00:26:58,00:27:00,00:27:02,00:27:04,00:27:06,00:27:08,00:27:10,00:27:12,00:27:14,00:27:16,00:27:18,00:27:20,00:27:22,00:27:24,00:27:26,00:27:28,00:27:30,00:27:32,00:27:34,00:27:36,00:27:38,00:27:40,00:27:42,00:27:44,00:27:46,00:27:48,00:27:50,00:27:52,00:27:54,00:27:56,00:27:58,00:28:00,00:28:02,00:28:04,00:28:06,00:28:08,00:28:10,00:28:12,00:28:14,00:28:16,00:28:18,00:28:20,00:28:22,00:28:24,00:28:26,00:28:28,00:28:30,00:28:32,00:28:34,00:28:36,00:28:38,00:28:40,00:28:42,00:28:44,00:28:46,00:28:48,00:28:50,00:28:52,00:28:54,00:28:56,00:28:58,00:29:00,00:29:02,00:29:04,00:29:06,00:29:08,00:29:10,00:29:12,00:29:14,00:29:16,00:29:18,00:29:20,00:29:22,00:29:24,00:29:26,00:29:28,00:29:30,00:29:32,00:29:34,00:29:36,00:29:38,00:29:40,00:29:42,00:29:44,00:29:46,00:29:48,00:29:50,00:29:52,00:29:54,00:29:56,00:29:58,00:30:00,00:30:02,00:30:04,00:30:06,00:30:08,00:30:10,00:30:12,00:30:14,00:30:16,00:30:18,00:30:20,00:30:22,00:30:24,00:30:26,00:30:28,00:30:30,00:30:32,00:30:34,00:30:36,00:30:38,00:30:40,00:30:42,00:30:44,00:30:46,00:30:48,00:30:50,00:30:52,00:30:54,00:30:56,00:30:58,00:31:00,00:31:02,00:31:04,00:31:06,00:31:08,00:31:10,00:31:12,00:31:14,00:31:16,00:31:18,00:31:20,00:31:22,00:31:24,00:31:26,00:31:28,00:31:30,00:31:32,00:31:34,00:31:36,00:31:38,00:31:40,00:31:42,00:31:44,00:31:46,00:31:48,00:31:50,00:31:52,00:31:54,00:31:56,00:31:58,00:32:00,00:32:02,00:32:04,00:32:06,00:32:08,00:32:10,00:32:12,00:32:14,00:32:16,00:32:18,00:32:20,00:32:22,00:32:24,00:32:26,00:32:28,00:32:30,00:32:32,00:32:34,00:32:36,00:32:38,00:32:40,00:32:42,00:32:44,00:32:46,00:32:48,00:32:50,00:32:52,00:32:54,00:32:56,00:32:58,00:33:00,00:33:02,00:33:04,00:33:06,00:33:08,00:33:10,00:33:12,00:33:14,00:33:16,00:33:18,00:33:20,00:33:22,00:33:24,00:33:26,00:33:28,00:33:30,00:33:32,00:33:34,00:33:36,00:33:38,00:33:40,00:33:42,00:33:44,00:33:46,00:33:48,00:33:50,00:33:52,00:33:54,00:33:56,00:33:58,00:34:00,00:34:02,00:34:04,00:34:06,00:34:08,00:34:10,00:34:12,00:34:14,00:34:16,00:34:18,00:34:20,00:34:22,00:34:24,00:34:26,00:34:28,00:34:30,00:34:32,00:34:34,00:34:36,00:34:38,00:34:40,00:34:42,00:34:44,00:34:46,00:34:48,00:34:50,00:34:52,00:34:54,00:34:56,00:34:58,00:35:00,00:35:02,00:35:04,00:35:06,00:35:08,00:35:10,00:35:12,00:35:14,00:35:16,00:35:18,00:35:20,00:35:22,00:35:24,00:35:26,00:35:28,00:35:30,00:35:32,00:35:34,00:35:36,00:35:38,00:35:40,00:35:42,00:35:44,00:35:46,00:35:48,00:35:50,00:35:52,00:35:54,00:35:56,00:35:58,00:36:00,00:36:02,00:36:04,00:36:06,00:36:08,00:36:10,00:36:12,00:36:14,00:36:16,00:36:18,00:36:20,00:36:22,00:36:24,00:36:26,00:36:28,00:36:30,00:36:32,00:36:34,00:36:36,00:36:38]", now, "Bracket expansion produces too many intervals (max 1024)");
        // a range that merges below the limit is fine
        assertCompiledTickExprWithNow("[$today..$today + 2000d]", now);
        assertCompiledTickExprWithNow("[$today..$today + 500d]T[09:00,15:00]", now);
    }

    @Test
    public void testCompiledTickExprInvalidDateRange() throws SqlException {
        // A literal fails on a range that starts after it ends, the way a bind variable does
        final String now = "2024-01-02T10:30:00.000000Z";
        final String error = "Invalid date range: start is after end";
        assertParsedAndCompiledErrorWithNow("[$tomorrow..$today]", now, error);
        assertParsedAndCompiledErrorWithNow("$tomorrow..$today", now, error);
        assertParsedAndCompiledErrorWithNow("[$now..$now - 1h]", now, error);
        assertParsedAndCompiledErrorWithNow("[$today..$now - 2d]T09:00", now, error);
        assertParsedAndCompiledErrorWithNow("[$today + 3bd..$today, $today]", now, error);
        // a range within one day is fine
        assertCompiledTickExprWithNow("[$today..$now]", now);
        assertCompiledTickExprWithNow("[$now..$today]", now);
    }

    @Test
    public void testCompiledTickExprInvalidDurationUnit() {
        // The parser rejects a duration with an invalid unit. The compiled form, which SQL uses for a
        // literal with a date variable, must reject it too, rather than evaluate it to an interval
        // that starts at the beginning of time, such as everything before now for $now;1x.
        final String now = "2024-01-02T10:00:00.000000Z";
        assertParsedAndCompiledRejectedWithNow("$now;1x", now, "Invalid duration unit: x");
        assertParsedAndCompiledRejectedWithNow("$today;1x", now, "Invalid duration unit: x");
        assertParsedAndCompiledRejectedWithNow("[$now];1x", now, "Invalid duration unit: x");
        assertParsedAndCompiledRejectedWithNow("$now;1h1x", now, "Invalid duration unit: x");
        assertParsedAndCompiledRejectedWithNow("$today..$tomorrow;1x", now, "Invalid duration unit: x");
        assertParsedAndCompiledRejectedWithNow("[$today, $tomorrow]T09:00;1h1x", now, "Invalid duration unit: x");
    }

    @Test
    public void testCompiledTickExprInvalidSingleTimeOverride() {
        assertCompileTickExprError("[$today]Tabc", "Invalid time override");
    }

    @Test
    public void testCompiledTickExprInvalidSuffixAfterBracket() {
        assertCompileTickExprError("[$today]09:30", "Expected 'T' time override");
    }

    @Test
    public void testCompiledTickExprInvalidTimezone() {
        assertCompileTickExprError("$today@Bogus/Zone", "invalid timezone");
    }

    @Test
    public void testCompiledTickExprInvalidVariableFailsAtCompileTime() {
        // Invalid $-expressions like $garbage must fail at compile time, not be
        // deferred to runtime. containsDateVariable() only matches known variable
        // names ($now, $today, $yesterday, $tomorrow), so $garbage falls through
        // to parseTickExpr which rejects it immediately.
        RuntimeIntervalModelBuilder builder = new RuntimeIntervalModelBuilder();
        builder.of(timestampType.getTimestampType(), 0, configuration);
        try {
            builder.intersectIntervals("$garbage", 0, 8, 42);
            Assert.fail("Should throw SqlException at compile time");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "Unknown date variable");
            Assert.assertEquals(42, e.getPosition());
        }
    }

    @Test
    public void testCompiledTickExprIsoWeekElementBracketedTimeAgreesWithBindVariable() throws SqlException {
        // The parser takes an ISO week with a time part of its own that starts with a bracket, under
        // a time of the list. The literal parses the expression on every evaluation, so it evaluates
        // to what the bind variable parses to.
        final String expression = "[2024-W01T[09:00], $today]T10:00";
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final long now = timestampDriver.parseFloorLiteral("2024-01-02T10:00:00.000000Z");
        out.clear();
        parseTickExprWithNow(timestampDriver, expression, 0, expression.length(), 7, out, now);
        final String parsed = intervalToString(timestampDriver, out).toString();
        try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(
                timestampDriver, configuration, expression, 0, expression.length(), 7)) {
            Assert.assertTrue(expression, compiled.isParsedOnEvaluation());
            compiled.validate(now);
            out.clear();
            compiled.evaluate(out, now);
            TestUtils.assertEquals(expression, parsed, intervalToString(timestampDriver, out));
        }
    }

    @Test
    public void testCompiledTickExprKnownDurationUnitsMatchBindVariable() throws SqlException {
        // The timestamp drivers add the duration units y M w d h m s T u n. The compiled form keeps
        // its IR for a duration of them, and evaluates it to the intervals of the parser, which SQL
        // uses for a bind variable. $today is 2024-01-02, and $now is 10:00 on that day. A duration
        // covers the time from its instant up to the instant it ends at, a negative one the time
        // before its instant. Each case gives the first and the last microsecond of the interval,
        // which ends 999 nanoseconds into its last microsecond on a nanosecond timestamp.
        final String[][] cases = {
                {"$today;1u", "2024-01-02T00:00:00.000000Z", "2024-01-02T00:00:00.000000Z"},
                {"$today;-1u", "2024-01-01T23:59:59.999999Z", "2024-01-01T23:59:59.999999Z"},
                {"$today;1T", "2024-01-02T00:00:00.000000Z", "2024-01-02T00:00:00.000999Z"},
                {"$today;-1T", "2024-01-01T23:59:59.999000Z", "2024-01-01T23:59:59.999999Z"},
                {"$today;1s", "2024-01-02T00:00:00.000000Z", "2024-01-02T00:00:00.999999Z"},
                {"$today;-1s", "2024-01-01T23:59:59.000000Z", "2024-01-01T23:59:59.999999Z"},
                {"$today;1m", "2024-01-02T00:00:00.000000Z", "2024-01-02T00:00:59.999999Z"},
                {"$today;-1m", "2024-01-01T23:59:00.000000Z", "2024-01-01T23:59:59.999999Z"},
                {"$today;1h", "2024-01-02T00:00:00.000000Z", "2024-01-02T00:59:59.999999Z"},
                {"$today;-1h", "2024-01-01T23:00:00.000000Z", "2024-01-01T23:59:59.999999Z"},
                {"$today;1d", "2024-01-02T00:00:00.000000Z", "2024-01-02T23:59:59.999999Z"},
                {"$today;-1d", "2024-01-01T00:00:00.000000Z", "2024-01-01T23:59:59.999999Z"},
                {"$today;1w", "2024-01-02T00:00:00.000000Z", "2024-01-08T23:59:59.999999Z"},
                {"$today;-1w", "2023-12-26T00:00:00.000000Z", "2024-01-01T23:59:59.999999Z"},
                // M is months, and m minutes
                {"$today;1M", "2024-01-02T00:00:00.000000Z", "2024-02-01T23:59:59.999999Z"},
                {"$today;-1M", "2023-12-02T00:00:00.000000Z", "2024-01-01T23:59:59.999999Z"},
                {"$today;1y", "2024-01-02T00:00:00.000000Z", "2025-01-01T23:59:59.999999Z"},
                {"$today;-1y", "2023-01-02T00:00:00.000000Z", "2024-01-01T23:59:59.999999Z"},
                {"$now;1u", "2024-01-02T10:00:00.000000Z", "2024-01-02T10:00:00.000000Z"},
                {"$now;-1u", "2024-01-02T09:59:59.999999Z", "2024-01-02T09:59:59.999999Z"},
                {"$now;1T", "2024-01-02T10:00:00.000000Z", "2024-01-02T10:00:00.000999Z"},
                {"$now;-1T", "2024-01-02T09:59:59.999000Z", "2024-01-02T09:59:59.999999Z"},
                {"$now;1s", "2024-01-02T10:00:00.000000Z", "2024-01-02T10:00:00.999999Z"},
                {"$now;-1s", "2024-01-02T09:59:59.000000Z", "2024-01-02T09:59:59.999999Z"},
                {"$now;1m", "2024-01-02T10:00:00.000000Z", "2024-01-02T10:00:59.999999Z"},
                {"$now;-1m", "2024-01-02T09:59:00.000000Z", "2024-01-02T09:59:59.999999Z"},
                {"$now;1h", "2024-01-02T10:00:00.000000Z", "2024-01-02T10:59:59.999999Z"},
                {"$now;-1h", "2024-01-02T09:00:00.000000Z", "2024-01-02T09:59:59.999999Z"},
                {"$now;1d", "2024-01-02T10:00:00.000000Z", "2024-01-03T09:59:59.999999Z"},
                {"$now;-1d", "2024-01-01T10:00:00.000000Z", "2024-01-02T09:59:59.999999Z"},
                {"$now;1w", "2024-01-02T10:00:00.000000Z", "2024-01-09T09:59:59.999999Z"},
                {"$now;-1w", "2023-12-26T10:00:00.000000Z", "2024-01-02T09:59:59.999999Z"},
                {"$now;1M", "2024-01-02T10:00:00.000000Z", "2024-02-02T09:59:59.999999Z"},
                {"$now;-1M", "2023-12-02T10:00:00.000000Z", "2024-01-02T09:59:59.999999Z"},
                {"$now;1y", "2024-01-02T10:00:00.000000Z", "2025-01-02T09:59:59.999999Z"},
                {"$now;-1y", "2023-01-02T10:00:00.000000Z", "2024-01-02T09:59:59.999999Z"},
                // several units add up
                {"$today;1h30m", "2024-01-02T00:00:00.000000Z", "2024-01-02T01:29:59.999999Z"},
                {"$today;1d1h1m1s1T1u", "2024-01-02T00:00:00.000000Z", "2024-01-03T01:01:01.001000Z"},
                // inside the nanosecond range, which starts on 1677-09-21
                {"$today;-300y", "1724-01-02T00:00:00.000000Z", "2024-01-01T23:59:59.999999Z"}
        };
        final boolean isNano = ColumnType.isTimestampNano(timestampType.getTimestampType());
        for (String[] c : cases) {
            assertCompiledIrWithNow(
                    c[0],
                    "2024-01-02T10:00:00.000000Z",
                    isNano ? c[1].replace("Z", "000Z") : c[1],
                    isNano ? c[2].replace("Z", "999Z") : c[2]
            );
        }
        // nanoseconds, which a microsecond timestamp adds in whole microseconds
        if (isNano) {
            assertCompiledIrWithNow("$today;1n", "2024-01-02T10:00:00.000000Z", "2024-01-02T00:00:00.000000000Z", "2024-01-02T00:00:00.000000000Z");
            assertCompiledIrWithNow("$today;-1n", "2024-01-02T10:00:00.000000Z", "2024-01-01T23:59:59.999999999Z", "2024-01-01T23:59:59.999999999Z");
            assertCompiledIrWithNow("$now;1n", "2024-01-02T10:00:00.000000Z", "2024-01-02T10:00:00.000000000Z", "2024-01-02T10:00:00.000000000Z");
            assertCompiledIrWithNow("$now;-1n", "2024-01-02T10:00:00.000000Z", "2024-01-02T09:59:59.999999999Z", "2024-01-02T09:59:59.999999999Z");
        } else {
            assertCompiledIrWithNow("$today;1000n", "2024-01-02T10:00:00.000000Z", "2024-01-02T00:00:00.000000Z", "2024-01-02T00:00:00.000000Z");
            assertCompiledIrWithNow("$today;-1000n", "2024-01-02T10:00:00.000000Z", "2024-01-01T23:59:59.999999Z", "2024-01-01T23:59:59.999999Z");
            assertCompiledIrWithNow("$now;1000n", "2024-01-02T10:00:00.000000Z", "2024-01-02T10:00:00.000000Z", "2024-01-02T10:00:00.000000Z");
            assertCompiledIrWithNow("$now;-1000n", "2024-01-02T10:00:00.000000Z", "2024-01-02T09:59:59.999999Z", "2024-01-02T09:59:59.999999Z");
        }
    }

    @Test
    public void testCompiledTickExprLeadingWhitespace() throws SqlException {
        assertCompiledTickExpr("  $today");
    }

    @Test
    public void testCompiledTickExprLeadingWhitespaceList() throws SqlException {
        assertCompiledTickExpr("  [ $today , $tomorrow]");
    }

    @Test
    public void testCompiledTickExprElementSuffixes() throws SqlException {
        // An element with its own day filter or timezone compiles, and evaluates like a bind variable.
        // 2024-01-01 is a Monday and 2024-01-02 a Tuesday.
        assertParsedAndCompiledWithNow(
                "[{lo=2024-01-01T00:00:00.000000Z, hi=2024-01-01T23:59:59.999999Z}]",
                "[$today#Mon]",
                "2024-01-01T10:30:00.000000Z"
        );
        assertParsedAndCompiledWithNow("[]", "[$today#Mon]", "2024-01-02T10:30:00.000000Z");
        // London is at +01:00 in July
        assertParsedAndCompiledWithNow(
                "[{lo=2024-07-14T23:00:00.000000Z, hi=2024-07-16T22:59:59.999999Z}]",
                "[$today..$tomorrow@Europe/London]",
                "2024-07-15T10:00:00.000000Z"
        );
        // a static element with its own timezone
        assertParsedAndCompiledWithNow(
                "[{lo=2024-01-02T00:00:00.000000Z, hi=2024-01-02T23:59:59.999999Z},{lo=2024-01-04T22:00:00.000000Z, hi=2024-01-05T21:59:59.999999Z}]",
                "[$today, 2024-01-05@+02:00]",
                "2024-01-02T10:30:00.000000Z"
        );
    }

    @Test
    public void testCompiledTickExprMaxIntervalCount() throws SqlException {
        // Date variable elements, time overrides and contiguous ranges take one interval each, and
        // the static elements one per run of adjacent intervals.
        assertMaxIntervalCount("$today", 10, 1);
        assertMaxIntervalCount("$now - 2h", 10, 1);
        assertMaxIntervalCount("$today - 3bd", 10, 1);
        assertMaxIntervalCount("[$yesterday, $today]", 10, 2);
        assertMaxIntervalCount("[$yesterday, 2024-01-02]", 10, 2);
        assertMaxIntervalCount("[$today, 2024-06-[01..05]]", 10, 2);
        assertMaxIntervalCount("[$today, $yesterday]T[09:00,12:00,15:00]", 10, 6);
        // The list stores a repeated date variable once.
        assertMaxIntervalCount("[$today, $today, $yesterday]T[09:00,12:00,15:00]", 10, 6);
        assertMaxIntervalCount('[' + repeatedValues("$today", 11) + ']', 10, 1);
        assertMaxIntervalCount("$today#workday", 10, 1);
        assertMaxIntervalCount("$today@Europe/London", 10, 1);
        assertMaxIntervalCount("$today..$today+5d", 10, 1);
        assertMaxIntervalCount("$now - 1h..$now@+05:30", 10, 1);
        assertMaxIntervalCount("[$today..$today+2d, $yesterday, 2024-01-01]", 10, 3);
        // The end precedes the start, so the range never yields an interval, but a check stays.
        assertMaxIntervalCount("$today..$today - 3d", 10, 1);
        // The bound comes from the IR alone, so an offset that would walk billions of days costs
        // nothing here.
        assertMaxIntervalCount("$today..$today + 2147483647d", 10, 1);
        assertMaxIntervalCount("$today - 2147483647y", 10, 1);

        // The cap.
        final String elevenTimes = "[$today]T[01:00,02:00,03:00,04:00,05:00,06:00,07:00,08:00,09:00,10:00,11:00]";
        assertMaxIntervalCount(elevenTimes, 11, 11);
        assertMaxIntervalCount(elevenTimes, 10, -1);
        // Ten adjacent days form one run.
        assertMaxIntervalCount("[$today, 2024-01-[01..10]]", 10, 2);
        assertMaxIntervalCount("[$today, 2024-01-[01..10]]", 2, 2);
        assertMaxIntervalCount("[$today, 2024-01-[01..10]]", 1, -1);
        // Ten minutes on ten days form ten runs.
        assertMaxIntervalCount("[$today, 2024-01-[01..10]T10:00]", 11, 11);
        assertMaxIntervalCount("[$today, 2024-01-[01..10]T10:00]", 10, -1);
        assertMaxIntervalCount("[$yesterday, $today]", 1, -1);

        // No bound: these yield an interval per day or per session, and a range of days varies in
        // its weekend days, its month or year lengths and the clock changes of its zone.
        assertMaxIntervalCount("$today..$today+3bd", Integer.MAX_VALUE, -1);
        assertMaxIntervalCount("$today..$today+5d@Europe/London", Integer.MAX_VALUE, -1);
        assertMaxIntervalCount("$now - 1h..$now@America/Santiago", Integer.MAX_VALUE, -1);
        assertMaxIntervalCount("$today..$today+1M#workday", Integer.MAX_VALUE, -1);
        assertMaxIntervalCount("[$today..$today+1y]T09:00", Integer.MAX_VALUE, -1);
        assertMaxIntervalCount("$today..$today+2d;1h", Integer.MAX_VALUE, -1);
        assertMaxIntervalCount("[$today, $yesterday..$today+3bd]", Integer.MAX_VALUE, -1);
        // Micros.addPeriod() and Nanos.addPeriod() return NULL for an unknown unit.
        assertMaxIntervalCount("$today + 1x", Integer.MAX_VALUE, -1);
        assertMaxIntervalCount("[$yesterday, $today + 1x]", Integer.MAX_VALUE, -1);
        assertMaxIntervalCount("$today..$today + 1x", Integer.MAX_VALUE, -1);
        assertMaxIntervalCount("$today + 1x..$today", Integer.MAX_VALUE, -1);
        assertMaxIntervalCount("$today;1h1x", Integer.MAX_VALUE, -1);
        // evaluateExpr() maps an offset in 'D' to 'd', while applyDuration() does not.
        assertMaxIntervalCount("$today + 1D", Integer.MAX_VALUE, 1);
        assertMaxIntervalCount("$todayT09:00;1D", Integer.MAX_VALUE, -1);
    }

    @Test
    public void testCompiledTickExprMaxIntervalCountHoldsForAnyNow() throws SqlException {
        // A count above the bound at any "now" would lose rows of the JIT filter, so
        // maxIntervalCountForAnyNow() evaluates each expression through 2024 and 2025.
        final String[] expressions = {
                "$today", "$yesterday", "$tomorrow", "$now", "$now - 2h", "$now + 90m", "$now - 7s",
                "$now + 1T", "$now - 3u", "$now + 5n", "$today + 1M", "$today - 1y", "$today + 3bd",
                "$today - 1w", "$today + 5D",
                "[$yesterday, $today]", "[$today, $tomorrow, $today + 2d]", "[$yesterday, 2024-01-02]",
                "[$today, 2024-06-[01..05]]", "[$today - 1bd, $today + 1bd]",
                "$todayT09:30;1h", "[$today]T[09:30,14:30];1h", "[$today, $yesterday]T[09:00,12:00,15:00]",
                "[$today]T[00:00,23:30];1h",
                "$today;6h", "$now;1h30m",
                "$today#workday", "[$yesterday, $today, $tomorrow]#weekend", "[$now, $today]#workday",
                "$today@Europe/London", "$today@+02:00", "$now@-03:30",
                "[$yesterday, $today, $tomorrow]@America/Santiago", "[$yesterday, $today]@Asia/Beirut",
                "[$today]T[00:00,23:00]@Atlantic/Azores;1h", "[$today, $tomorrow]@America/Santiago#workday",
                "$now - 1h..$now", "$now - 1d..$now", "$today..$today+5d", "$yesterday..$tomorrow",
                "$now..$now + 1M", "$today - 1y..$today", "$now - 3h..$today", "$today..$now",
                "$now - 1h..$now@+05:30", "$today..$today+5d@-03:00",
                "[$today..$today+2d, $yesterday]", "[$now - 1h..$now, 2024-03-[01..03]]",
                "[$today..$tomorrow, $today + 1w..$today + 8d]",
                // repeated date variables, which the list stores once
                "[$today, $today, $yesterday]T[09:00,12:00,15:00]", '[' + repeatedValues("$today", 11) + ']',
                "[$now, $now..$now, $now]",
                // Static runs next to a range, and lists whose bound exceeds every count: it counts
                // the empty intervals of a zero duration apart from the days that cover them, and
                // two date variables that touch apart from each other.
                "[$today, 2024-01-[15..17]T09:00;0h, 2024-01-[15..17]]",
                "[$yesterday, $today, 2024-01-[01..20]]#weekend",
                "[$today..$today+2d, 2024-01-[01..20], 2024-02-[01..03]]"
        };
        for (String expression : expressions) {
            // Each expression yields an interval at some point, so the walk did evaluate it.
            Assert.assertTrue(expression, maxIntervalCountForAnyNow(expression) > 0);
        }
        // A time slot with a timezone of its own parses on every evaluation, as the IR cannot
        // represent it, so the JIT filter gets no bound for it.
        assertMaxIntervalCount("[$today]T[09:00@Europe/London,10:00@America/New_York]", Integer.MAX_VALUE, -1);
        // The bound counts a run of static intervals as one, and some "now" away from the runs
        // yields as many intervals as the bound. 2024-01-01 is a Monday.
        assertMaxIntervalCountReached("[$today, 2024-01-[01..20]]", 2);
        assertMaxIntervalCountReached("[2024-01-[01..20], $today, 2024-01-[21..31]]", 2);
        // The workdays from 2024-01-01 to 01-20 form three runs, and those of January 2024 five.
        assertMaxIntervalCountReached("[$today, 2024-01-[01..20]]#workday", 4);
        assertMaxIntervalCountReached("[$today, 2024-01]#workday", 6);
        // The clock goes back at local midnight on 2024-04-07 in America/Santiago and on 2024-10-27
        // in Asia/Beirut, and the day before it covers both occurrences of the repeated hour, so the
        // days before and after it touch in UTC and form one run.
        assertMaxIntervalCountReached("[$today, 2024-04-[01..10]]@America/Santiago", 2);
        assertMaxIntervalCountReached("[$today, 2024-10-[20..31]]@Asia/Beirut", 2);
        // evaluate() keeps the interval [D - 6d, D) of each weekend day D of January 2024 by the
        // weekday of its lo, which leaves four runs, and the one of $today when $today is a Friday or
        // a Saturday.
        assertMaxIntervalCountReached("[$today, 2024-01]T00:00#weekend;-6d", 5);
    }

    @Test
    public void testCompiledTickExprMaxIntervalCountMergesOneUnitStaticIntervals() throws SqlException {
        // A microsecond is one unit of TIMESTAMP, so each of the two static leaves is an interval
        // with lo == hi, which is not empty, and the second starts one unit after the first. The
        // compiler keeps an IR entry for each, and the bound counts them as one run, next to the
        // date variable. In TIMESTAMP_NS, each leaf spans 1,000 units. On the Monday 2024-01-08,
        // evaluate() yields that day and the two leaves as one interval.
        final LongList expected = new LongList();
        addLocalDays(expected, LocalDate.of(2024, 1, 8), LocalDate.of(2024, 1, 9), ZoneOffset.UTC);
        addLocalBlock(expected, LocalDateTime.of(2024, 1, 15, 10, 0), ZoneOffset.UTC, 2);
        assertCompiledTickExprIntervals("[$today, 2024-01-15T10:00:00.000000, 2024-01-15T10:00:00.000001]", 3, 2, expected);
    }

    @Test
    public void testCompiledTickExprMaxIntervalCountMergesStaticIntervals() throws SqlException {
        // The compiler writes one STATIC entry per bracket leaf, day or list value, and evaluate()
        // merges the intervals that overlap or touch. The bound counts the runs that the static
        // intervals form, plus one per empty interval, and one interval for the date variable.
        // One run: two seconds that touch, adjacent days of two elements, a day within its month,
        // unsorted bracket values, runs on both sides of a date variable, and days around 1970 and
        // near the end of the TIMESTAMP_NS range.
        assertMaxIntervalCount("[$today, 2024-01-15T10:00:00, 2024-01-15T10:00:01]", Integer.MAX_VALUE, 2);
        assertMaxIntervalCount("[$today, 2024-01-01, 2024-01-02]", Integer.MAX_VALUE, 2);
        assertMaxIntervalCount("[$today, 2024-01, 2024-01-15]", Integer.MAX_VALUE, 2);
        assertMaxIntervalCount("[$today, 2024-01-[03,01,02]]", Integer.MAX_VALUE, 2);
        assertMaxIntervalCount("[$today, 2024-01-[01..20]]", Integer.MAX_VALUE, 2);
        assertMaxIntervalCount("[2024-01-[01..20], $today, 2024-01-[21..31]]", Integer.MAX_VALUE, 2);
        assertMaxIntervalCount("[$today, 1969-12-[30..31], 1970-01-01]", Integer.MAX_VALUE, 2);
        assertMaxIntervalCount("[$today, 2261-12-[30..31]]", Integer.MAX_VALUE, 2);
        // A single static interval, and the hour before 09:00 that a negative duration yields.
        assertMaxIntervalCount("[$yesterday, 2024-01-02]", Integer.MAX_VALUE, 2);
        assertMaxIntervalCount("[$today, 2024-01-15T09:00;-1h]", Integer.MAX_VALUE, 2);
        // Apart: two seconds with a second between them, three days, and a minute on each of 336
        // days.
        assertMaxIntervalCount("[$today, 2024-01-15T10:00:00, 2024-01-15T10:00:02]", Integer.MAX_VALUE, 3);
        assertMaxIntervalCount("[$today, 2024-01-[20,05,10]]", Integer.MAX_VALUE, 4);
        assertMaxIntervalCount("[$today, 2024-[01..12]-[01..28]T10:00]", Integer.MAX_VALUE, 337);
        // A zero duration yields the empty interval [09:00, 09:00 - 1 unit], which counts apart even
        // within a day.
        assertMaxIntervalCount("[$today, 2024-01-15T09:00;0h]", Integer.MAX_VALUE, 2);
        assertMaxIntervalCount("[$today, 2024-01-[15..17]T09:00;0h]", Integer.MAX_VALUE, 4);
        assertMaxIntervalCount("[$today, 2024-01-15T09:00;0h, 2024-01-15]", Integer.MAX_VALUE, 3);
        // A zone without a clock change keeps the run.
        assertMaxIntervalCount("[$today, 2024-01-[01..20]]@+05:00", Integer.MAX_VALUE, 2);
        assertMaxIntervalCount("[$today, 2024-01-[01..20]]@Europe/London", Integer.MAX_VALUE, 2);
        // America/Santiago goes back from 00:00 to 23:00 at the end of 2024-04-06 and Asia/Beirut at
        // the end of 2024-10-26. A day covers both occurrences of its repeated hour, so it ends where
        // the next day starts in UTC, and the days stay one run. The jump of America/Santiago from
        // 00:00 to 01:00 on 2024-09-08 keeps the days adjacent too.
        assertMaxIntervalCount("[$today, 2024-04-[01..10]]@America/Santiago", Integer.MAX_VALUE, 2);
        assertMaxIntervalCount("[$today, 2024-10-[20..31]]@Asia/Beirut", Integer.MAX_VALUE, 2);
        assertMaxIntervalCount("[$today, 2024-09-[06..09]]@America/Santiago", Integer.MAX_VALUE, 2);
        // The 1,774 weekend days of 2008-2024 in America/Santiago are 887 weekends, each a run of its
        // Saturday and Sunday in UTC, also where the clocks go back between them.
        assertMaxIntervalCount("[$today,[2008..2024]]@America/Santiago#weekend", Integer.MAX_VALUE, 888);
        // The compiler stores the static intervals filtered and in UTC, where the Mondays 2024-01-01,
        // 01-08 and 01-15 at +12:00 start on a Sunday, so the bound counts them as stored, and a
        // Monday away from them yields them and $today.
        assertMaxIntervalCount("[$today, 2024-01-[01..20]]@+12:00#mon", Integer.MAX_VALUE, 4);
        assertMaxIntervalCountReached("[$today, 2024-01-[01..20]]@+12:00#mon", 4);
        // The day filter splits a run at a weekend. 2024-01-01 is a Monday, and the compiler drops
        // the weekend 2024-01-06 and 01-07 whole.
        assertMaxIntervalCount("[$today, 2024-01-[01..20]]#workday", Integer.MAX_VALUE, 4);
        assertMaxIntervalCount("[$today, 2024-01]#workday", Integer.MAX_VALUE, 6);
        assertMaxIntervalCount("[$today, 2024-01-[06..07]]#workday", Integer.MAX_VALUE, 1);
        // The compiler keeps the interval [D - 6d, D) of each weekend day D of January 2024, and
        // evaluate() keeps or drops each by the weekday of its lo. Those of the Sundays start on a
        // Monday and go, and those of the Saturdays leave the runs from 2023-12-31 to 2024-01-05,
        // 01-07 to 01-12, 01-14 to 01-19 and 01-21 to 01-26, while all eight would form one run.
        assertMaxIntervalCount("[$today, 2024-01]T00:00#weekend;-6d", Integer.MAX_VALUE, 5);
    }

    @Test
    public void testCompiledTickExprMaxIntervalCountRandomLists() throws SqlException {
        // Builds random lists of date variables, which may repeat in other spellings, and static days,
        // bracket ranges, unsorted brackets, months and zero duration days of 2024, under a day
        // filter, a zone with clock changes at local midnight, a time override or a duration, and
        // expects evaluate() to stay within the bound every 6 hours through 2024, and one unit either
        // side of each of those instants.
        final String[] suffixes = {
                "", "#workday", "#weekend", "@Europe/London", "@America/Santiago", "@Asia/Beirut", "@+05:00",
                "T10:00", ";1d", "T00:00;1d", "T00:00#weekend;-6d", "#workday;-1d"
        };
        final Rnd rnd = TestUtils.generateRandom(LOG);
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final long start = timestampDriver.parseFloorLiteral("2024-01-01T00:00:00.000000Z");
        final long end = timestampDriver.parseFloorLiteral("2025-01-01T00:00:00.000000Z");
        final long step = timestampDriver.fromHours(6);
        final LongList intervals = new LongList();
        final StringSink expression = new StringSink();
        for (int i = 0; i < 200; i++) {
            final String suffix = suffixes[rnd.nextInt(suffixes.length)];
            // The time override or duration of the list would follow the one of a zero duration day.
            final boolean isZeroDurationDayAllowed = suffix.indexOf('T') < 0 && suffix.indexOf(';') < 0;
            final int elementCount = 2 + rnd.nextInt(4);
            final int dateVariableIndex = rnd.nextInt(elementCount);
            expression.clear();
            expression.put('[');
            for (int e = 0; e < elementCount; e++) {
                if (e > 0) {
                    expression.put(", ");
                }
                appendRandomListElement(rnd, expression, e == dateVariableIndex, isZeroDurationDayAllowed);
            }
            expression.put(']').put(suffix);
            try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(
                    timestampDriver, configuration, expression, 0, expression.length(), 0)) {
                final int bound = compiled.getMaxIntervalCount(Integer.MAX_VALUE);
                if (compiled.isParsedOnEvaluation()) {
                    // A month with a duration and no time override parses on every evaluation, as the
                    // IR cannot represent it, so the JIT filter gets no bound for it.
                    Assert.assertEquals(expression.toString(), -1, bound);
                    continue;
                }
                Assert.assertTrue(expression.toString(), bound > 0);
                for (long now = start; now < end; now += step) {
                    for (long delta = -1; delta <= 1; delta++) {
                        intervals.clear();
                        compiled.evaluate(intervals, now + delta);
                        final int count = intervals.size() / 2;
                        if (count > bound) {
                            Assert.fail(expression + " yields " + count + " intervals, above its bound of " + bound
                                    + ", at " + timestampDriver.toMSecString(now + delta));
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testCompiledTickExprMixedListWithMoreThan255Elements() throws SqlException {
        // The element count used to take 8 bits, so the expression kept only count % 256 elements.
        // 2024-[01..09]-[01..28]T10:00 holds 252 elements and 2024-10-[01..0N]T10:00 holds N.
        final String days252 = "2024-[01..09]-[01..28]T10:00";
        assertCompiledTickExprElementCount("[$today, " + days252 + "]", 253);
        assertCompiledTickExprElementCount("[$today, " + days252 + ", 2024-10-[01..02]T10:00]", 255);
        assertCompiledTickExprElementCount("[$today, " + days252 + ", 2024-10-[01..03]T10:00]", 256);
        assertCompiledTickExprElementCount("[$today, " + days252 + ", 2024-10-[01..04]T10:00]", 257);
        assertCompiledTickExprElementCount("[$today, 2024-[01..12]-[01..28]T10:00]", 337);
        assertCompiledTickExprElementCount("[2024-[01..12]-[01..28]T10:00, $today, 2025-[01..12]-[01..28]T10:00]", 673);
        assertCompiledTickExpr("[2024-[01..12]-[01..28]T10:00, $today..$tomorrow, 2025-01-[01..28]T10:00]");
    }

    @Test
    public void testCompiledTickExprMixedListWithRange() throws SqlException {
        assertCompiledTickExpr("[2025-03-01, $today .. $today + 2d , $yesterday]");
    }

    @Test
    public void testCompiledTickExprMixedListWithTimeOverride() throws SqlException {
        assertCompiledTickExpr("[2025-06-15, $today]T09:30");
    }

    @Test
    public void testCompiledTickExprMixedListWithTimeOverrideAndDuration() throws SqlException {
        assertCompiledTickExpr("[2025-06-15, $today]T09:30;1h");
    }

    @Test
    public void testCompiledTickExprMixedListWithTimeOverrideAndTz() throws SqlException {
        assertCompiledTickExpr("[2025-06-15, $today]T09:30@+05:00");
    }

    @Test
    public void testCompiledTickExprMixedListWithTimeOverrideTzAndDuration() throws SqlException {
        assertCompiledTickExpr("[2025-06-15, $today]T09:30@+05:00;1h");
    }

    @Test
    public void testCompiledTickExprMixedPrecisionRange() throws SqlException {
        // Range where start is a whole day ($today) and end is a point in time ($now):
        // the range covers today up to $now.
        assertCompiledTickExpr("$today..$now");
    }

    @Test
    public void testCompiledTickExprMixedPrecisionRangeWithBusinessDays() throws SqlException {
        // Business days from $now, each with a 09:30 session; $now clips today's session
        assertCompiledTickExpr("[$now..$today + 3bd]T09:30;1h");
    }

    @Test
    public void testCompiledTickExprMixedPrecisionRangeWithDayFilter() throws SqlException {
        assertCompiledTickExpr("[$yesterday + 12h..$tomorrow]#workday");
    }

    @Test
    public void testCompiledTickExprMixedPrecisionRangeWithTimeListAcrossDstChanges() throws SqlException {
        // The range clips its time list in local time, so the parse path and the compiled path
        // must agree at every moment around a clock change, including while the point-in-time
        // endpoint falls in the hour that the spring-forward change skips
        assertCompiledTickExprEvery5Minutes(
                "2024-03-31T00:00:00.000000Z",
                "[$now - 2h..$tomorrow]T[00:30,01:30,02:30]@Europe/London",
                "[$yesterday..$now]T[00:30,01:30,02:30]@Europe/London",
                "[$now - 2h..$tomorrow]T[01:00,01:15,01:30,01:45,02:00,02:15]@Europe/London",
                "[$now - 2h..$tomorrow]T[01:30,02:30]@Europe/London;30m",
                "[$now - 2h..$tomorrow]T[00:30,01:30,02:30]@Europe/London#workday",
                "[$now - 2h..$tomorrow]T[10:30@Asia/Tokyo,02:30]@Europe/London",
                "[$now - 2h..$tomorrow]T[02:30@+01:00]@Europe/London"
        );
        assertCompiledTickExprEvery5Minutes(
                "2024-03-10T00:00:00.000000Z",
                "[$now - 2h..$tomorrow]T[01:30,02:30,03:30]@America/New_York"
        );
        assertCompiledTickExprEvery5Minutes(
                "2024-10-27T00:00:00.000000Z",
                "[$now - 2h..$tomorrow]T[00:30,01:30,02:30]@Europe/London",
                "[$yesterday..$now]T[00:30,01:30,02:30]@Europe/London",
                "[$now - 2h..$tomorrow]T[01:00,01:15,01:30,01:45,02:00,02:15]@Europe/London"
        );
    }

    @Test
    public void testCompiledTickExprMixedPrecisionRangeWithTimeListAndTimezone() throws SqlException {
        // Both paths clip the time list to $now - 2h in local time, then apply the timezone
        assertCompiledTickExpr("[$now - 2h..$tomorrow]T[09:00,14:00]@+02:00");
    }

    @Test
    public void testCompiledTickExprMonthPrecisionStaticWithDayFilter() throws SqlException {
        // Month-precision static element requires multi-day expansion at compile time.
        // The compile-time applyDayFilter (with hasDatePrecision=false) expands
        // "2025-01" into individual Mondays; runtime Phase 2 re-checks idempotently.
        assertCompiledTickExpr("[2025-01, $today]#Mon");
    }

    @Test
    public void testCompiledTickExprNamedTimezone() throws SqlException {
        assertCompiledTickExpr("$today@Europe/London");
    }

    @Test
    public void testCompiledTickExprNamedTimezoneWithDuration() throws SqlException {
        assertCompiledTickExpr("$today@Europe/Berlin;8h");
    }

    @Test
    public void testCompiledTickExprNonVariableStaysStatic() throws SqlException {
        RuntimeIntervalModelBuilder builder = new RuntimeIntervalModelBuilder();
        builder.of(timestampType.getTimestampType(), 0, configuration);
        builder.intersectIntervals("2024-01", 0, 7, 0);
        try (RuntimeIntrinsicIntervalModel m = builder.build()) {
            Assert.assertTrue("Non-variable expression should produce static model", m.isStatic());
        }
    }

    @Test
    public void testCompiledTickExprNowMixedListWithTimeOverride() throws SqlException {
        // $now keeps its sub-day time (time override skipped), $today gets T09:30.
        assertCompiledTickExpr("[$now, $today]T09:30");
    }

    @Test
    public void testCompiledTickExprNowTimezone() throws SqlException {
        // Bare $now with numeric timezone — point interval shifted by offset.
        assertCompiledTickExpr("$now@+05:00");
    }

    @Test
    public void testCompiledTickExprNumericTimezone() throws SqlException {
        assertCompiledTickExpr("$today@+05:30");
    }

    @Test
    public void testCompiledTickExprRangeInDateList() throws SqlException {
        assertCompiledTickExpr("[$today..$today + 2d, $tomorrow]");
    }

    @Test
    public void testCompiledTickExprRangeInListIrGrowth() throws SqlException {
        // 2 duration parts → irPos=4 after shift, ir.length=64.
        // 20 bracket-expanded static dates → 60 longs → irPos=64.
        // Range needs 2 more → triggers IR growth at L529.
        assertCompiledTickExpr("[2025-01-[01..20], $today..$tomorrow];1s1s");
    }

    @Test
    public void testCompiledTickExprRangeWithSpaceBeforeDots() throws SqlException {
        assertCompiledTickExpr("$today + 1d .. $today + 5d ;1h");
    }

    @Test
    public void testCompiledTickExprRangeWithTimeSuffix() throws SqlException {
        assertCompiledTickExpr("$today..$today + 3dT09:30;1h");
    }

    @Test
    public void testCompiledTickExprRangeWithTimezone() throws SqlException {
        assertCompiledTickExpr("$today..$today + 3d@+05:30");
    }

    @Test
    public void testCompiledTickExprNumericTimeBrackets() throws SqlException {
        // Numeric brackets in the time part, and brackets after a time list, expand the same way in
        // a literal as in a bind variable
        assertParsedAndCompiledWithNow(
                "[{lo=2024-01-02T01:30:00.000000Z, hi=2024-01-02T01:30:59.999999Z},{lo=2024-01-02T02:30:00.000000Z, hi=2024-01-02T02:30:59.999999Z}]",
                "[$today]T[01,02]:30",
                "2024-01-02T10:30:00.000000Z"
        );
        assertParsedAndCompiledWithNow(
                "[{lo=2024-01-02T09:00:00.000000Z, hi=2024-01-02T09:00:59.999999Z},{lo=2024-01-02T09:30:00.000000Z, hi=2024-01-02T09:30:59.999999Z},{lo=2024-01-02T10:00:00.000000Z, hi=2024-01-02T10:00:59.999999Z},{lo=2024-01-02T10:30:00.000000Z, hi=2024-01-02T10:30:59.999999Z}]",
                "[$today]T[09..10]:[00,30]",
                "2024-01-02T10:30:00.000000Z"
        );
        assertParsedAndCompiledWithNow(
                "[{lo=2024-01-02T09:00:00.000000Z, hi=2024-01-02T09:00:59.999999Z},{lo=2024-01-02T09:30:00.000000Z, hi=2024-01-02T09:30:59.999999Z}]",
                "[$today]T09:[00,30]",
                "2024-01-02T10:30:00.000000Z"
        );
        assertParsedAndCompiledWithNow(
                "[{lo=2024-01-02T09:00:15.000000Z, hi=2024-01-02T09:00:15.999999Z},{lo=2024-01-02T09:00:45.000000Z, hi=2024-01-02T09:00:45.999999Z},{lo=2024-01-02T10:00:15.000000Z, hi=2024-01-02T10:00:15.999999Z},{lo=2024-01-02T10:00:45.000000Z, hi=2024-01-02T10:00:45.999999Z}]",
                "[$today]T[09:00,10:00]:[15,45]",
                "2024-01-02T10:30:00.000000Z"
        );
    }

    @Test
    public void testCompiledTickExprSingleTimeWithManyDurationParts() throws SqlException {
        assertCompiledTickExpr("$todayT09:30;" + "1s".repeat(62));
    }

    @Test
    public void testCompiledTickExprSingleVarInListIrGrowth() throws SqlException {
        // 2 duration parts → irPos=4 after shift, ir.length=64.
        // 20 bracket-expanded static dates → 60 longs → irPos=64.
        // Single var needs 1 more → triggers IR growth at L537.
        assertCompiledTickExpr("[2025-01-[01..20], $today];1s1s");
    }

    @Test
    public void testCompiledTickExprStaticElementBracketedTimeFailsLikeBindVariable() throws SqlException {
        // A static element with a time part of its own that starts with a bracket, under a time of
        // the list: the parser, which SQL uses for a bind variable, joins the element and the time of
        // the list, without its timezone and day filter, and rejects the first date that this gives.
        // The literal fails with the same error, where the IR gave the days of a month element the
        // time of the list, or rejected some elements with an error of its own.
        final long now = timestampType.getDriver().parseFloorLiteral("2024-01-02T10:00:00.000000Z");
        final String[][] cases = {
                // the time list of a month element, under a time of the list of each precision, with a
                // timezone, and with a day filter
                {"[2024-01T[09:00,10:00], $today]T10:00", "Invalid date: 2024-01-01T09:00T10:00"},
                {"[2024-01T[09:00,10:00], $today]T10", "Invalid date: 2024-01-01T09:00T10"},
                {"[2024-01T[09:00,10:00], $today]T10:00:30", "Invalid date: 2024-01-01T09:00T10:00:30"},
                {"[2024-01T[09:00,10:00], $today]T10:00@+02:00", "Invalid date: 2024-01-01T09:00T10:00"},
                {"[2024-01T[09:00,10:00], $today]T10:00@Europe/London", "Invalid date: 2024-01-01T09:00T10:00"},
                {"[2024-01T[09:00,10:00], $today]T10:00#Tue", "Invalid date: 2024-01-01T09:00T10:00"},
                {"[2024-01T[09:00,10:00], $today]T10:00@+02:00#Tue", "Invalid date: 2024-01-01T09:00T10:00"},
                // a single slot, one with spaces, a text after the bracket, brackets after it, a slot
                // with a timezone of its own, and numeric brackets
                {"[2024-01T[09:00], $today]T10:00", "Invalid date: 2024-01-01T09:00T10:00"},
                {"[2024-01T[ 09:00 ], $today]T10:00", "Invalid date: 2024-01-01T09:00T10:00"},
                {"[2024-01T[09:00]:30, $today]T10:00", "Invalid date: 2024-01-01T09:00:30T10:00"},
                {"[2024-01T[09:00,10:00]:[15,45], $today]T10:00", "Invalid date: 2024-01-01T09:00:15T10:00"},
                {"[2024-01T[10:30@+02:00], $today]T10:00", "Invalid date: 2024-01-01T10:30T10:00"},
                {"[2024-01T[09..10]:30, $today]T10:00", "Invalid date: 2024-01T09:30T10:00"},
                // a duration of the element, which runs into the time of the list
                {"[2024-01T[09:00];1h, $today]T10:00", "Expected number before unit 'T'"},
                // a year, a bracketed month, a day and a bracketed day
                {"[2024T[09:00], $today]T10:00", "Invalid date: 2024T09:00T10:00"},
                {"[2024-[01,02]T[09:00], $today]T10:00", "Invalid date: 2024-01T09:00T10:00"},
                {"[2024-01-05T[09:00], $today]T10:00", "Invalid date: 2024-01-05T09:00T10:00"},
                {"[2024-01-[05,06]T[09:00], $today]T10:00", "Invalid date: 2024-01-05T09:00T10:00"},
                // after a date variable, next to other date variables and ranges, among three elements,
                // and with spaces around the list and its elements
                {"[$today, 2024-01T[09:00,10:00]]T10:00", "Invalid date: 2024-01-01T09:00T10:00"},
                {"[2024-01T[09:00], $now]T10", "Invalid date: 2024-01-01T09:00T10"},
                {"[2024-01T[09:00], $NOW + 1w]T09:00:30", "Invalid date: 2024-01-01T09:00T09:00:30"},
                {"[2024-01T[09:00], $today..$tomorrow]T10:00:30", "Invalid date: 2024-01-01T09:00T10:00:30"},
                {"[$today..$tomorrow, 2024-01T[09:00]]T10:00", "Invalid date: 2024-01-01T09:00T10:00"},
                {"[2024-01T[09:00], $today, 2024-02T[10:00]]T10:00", "Invalid date: 2024-01-01T09:00T10:00"},
                {"  [2024-01T[09:00], $today]T10:00", "Invalid date: 2024-01-01T09:00T10:00"},
                {"[ 2024-01T[09:00] , $today ]T10:00", "Invalid date: 2024-01-01T09:00T10:00"},
                {"[2024-01T[09:00],$today]T10:00", "Invalid date: 2024-01-01T09:00T10:00"}
        };
        for (String[] c : cases) {
            assertFailsLikeBindVariable(c[0], now, c[1]);
        }
    }

    @Test
    public void testCompiledTickExprStaticElementBracketedTimeWithoutListTimeKeepsIr() throws SqlException {
        // The IR represents a static element with a time part of its own that starts with a bracket
        // when the list has no time part, with or without a timezone of the list, and a month element
        // without a time part of its own under a time of the list: the literal evaluates them without
        // parsing, to the intervals of the bind variable. "Now" is 10:00 on 2024-01-02, which $today
        // covers whole.
        final String now = "2024-01-02T10:00:00.000000Z";
        final StringBuilder slots = new StringBuilder();
        final StringBuilder zonedSlots = new StringBuilder();
        final StringBuilder tenAm = new StringBuilder();
        for (int day = 1; day <= 31; day++) {
            final String date = String.format("2024-01-%02d", day);
            if (day == 2) {
                // $today holds the slots of its day; at +02:00 it starts at 22:00Z the day before
                slots.append(",{lo=2024-01-02T00:00:00.000000Z, hi=2024-01-02T23:59:59.999999Z}");
                zonedSlots.append(",{lo=2024-01-01T22:00:00.000000Z, hi=2024-01-02T21:59:59.999999Z}");
            } else {
                slots.append(minute(date, "09:00")).append(minute(date, "10:00"));
                // 09:00 and 10:00 at +02:00
                zonedSlots.append(minute(date, "07:00")).append(minute(date, "08:00"));
            }
            // $today at 10:00 is the minute of its day
            tenAm.append(minute(date, "10:00"));
        }
        final String[][] cases = {
                {"[2024-01T[09:00,10:00], $today]", "[" + slots.substring(1) + "]"},
                {"[2024-01T[09:00,10:00], $today]@+02:00", "[" + zonedSlots.substring(1) + "]"},
                {"[2024-01, $today]T10:00", "[" + tenAm.substring(1) + "]"}
        };
        for (String[] c : cases) {
            assertParsedAndCompiledWithNow(c[1], c[0], now);
            assertCompiledEvaluatesTo(forTimestampType(c[1]), c[0], timestampType.getDriver().parseFloorLiteral(now), true);
        }
        // a 'T' that starts no time part leaves the element to the IR, which rejects it as it
        // compiles, as the parser does
        assertParseError("[2024-01Tx, $today]T10:00", "Invalid date: 2024-01Tx");
        assertCompileError("[2024-01Tx, $today]T10:00", "Invalid date: 2024-01Tx");
    }

    @Test
    public void testCompiledTickExprStaticElementUnderTimeList() throws SqlException {
        // A static element takes the time list like a date variable does
        assertParsedAndCompiledWithNow(
                "[{lo=2023-12-30T09:00:00.000000Z, hi=2023-12-30T09:00:59.999999Z},{lo=2023-12-30T14:30:00.000000Z, hi=2023-12-30T14:30:59.999999Z},{lo=2024-03-31T09:00:00.000000Z, hi=2024-03-31T09:00:59.999999Z},{lo=2024-03-31T14:30:00.000000Z, hi=2024-03-31T14:30:59.999999Z}]",
                "[$today - 3d, 2024-03-31]T[09:00,14:30]",
                "2024-01-02T10:30:00.000000Z"
        );
        // an element with its own time of day keeps it and skips the time override
        assertParsedAndCompiledWithNow(
                "[{lo=2024-01-02T10:30:00.000000Z, hi=2024-01-02T10:30:59.999999Z},{lo=2024-01-02T11:00:00.000000Z, hi=2024-01-02T11:00:59.999999Z},{lo=2024-01-05T09:00:00.000000Z, hi=2024-01-05T09:00:59.999999Z}]",
                "[2024-01-05T09:00, $today]T[10:30,11:00]",
                "2024-01-02T10:30:00.000000Z"
        );
        // a month element with a duration applies the duration to each of its days
        assertCompiledTickExprWithNow("[$today, 2024-03]@America/New_York;55m", "2024-01-02T10:30:00.000000Z");
    }

    @Test
    public void testCompiledTickExprSingleVarWithMoreThan255TimeOverrides() throws SqlException {
        // The time override count used to take 8 bits, so 300 time overrides read as 44. The times
        // are 2 minutes apart, so their intervals do not merge.
        final StringBuilder times = new StringBuilder("[$today]T[");
        for (int i = 0; i < 300; i++) {
            if (i > 0) {
                times.append(',');
            }
            final int minutes = 2 * i;
            times.append(String.format("%02d:%02d", minutes / 60, minutes % 60));
        }
        times.append(']');
        assertCompiledTickExpr(times.toString());
        final TimestampDriver timestampDriver = timestampType.getDriver();
        try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(
                timestampDriver, configuration, times, 0, times.length(), 0)) {
            final LongList intervals = new LongList();
            compiled.evaluate(intervals, timestampDriver.parseFloorLiteral("2025-06-01T01:00:00.000000Z"));
            Assert.assertEquals(600, intervals.size());
            Assert.assertEquals(timestampDriver.parseFloorLiteral("2025-06-01T09:58:00.000000Z"), intervals.getQuick(598));
        }
    }

    @Test
    public void testCompiledTickExprStaticElementIrGrowth() throws SqlException {
        // 22 bracket-expanded dates start at irPos=2, ir.length=64.
        // 21st date has irPos=62 → 62+3=65>64 → triggers IR growth at L600.
        assertCompiledTickExpr("[2025-01-[01..22], $today]");
    }

    @Test
    public void testCompiledTickExprStaticWithDayFilterBeyondMergeThreshold() throws SqlException {
        // The static element expands to 336 days, beyond the incremental merge threshold
        // (256 intervals). Both paths must apply the day filter to each day, and agree.
        final String expression = "[$today, 2024-[01..12]-[01..28]]#workday";
        assertCompiledTickExpr(expression);

        final TimestampDriver timestampDriver = timestampType.getDriver();
        out.clear();
        parseTickExprWithNow(timestampDriver, expression, 0, expression.length(), 0, out,
                timestampDriver.parseFloorLiteral("2024-01-02T10:30:00.000000Z"));
        int days = 0;
        for (int i = 0; i < out.size(); i += 2) {
            for (long day = timestampDriver.startOfDay(out.getQuick(i), 0); day <= out.getQuick(i + 1); day = timestampDriver.addDays(day, 1)) {
                final int dayOfWeek = timestampDriver.getDayOfWeek(day);
                Assert.assertTrue("weekend day in " + intervalToString(timestampDriver, out), dayOfWeek != 6 && dayOfWeek != 7);
                days++;
            }
        }
        // the first 28 days of a month are four whole weeks, 20 of them weekdays, and $today
        // (Tuesday 2024-01-02) is one of them
        Assert.assertEquals(12 * 20, days);
    }

    @Test
    public void testCompiledTickExprStaticElementTimeListGrowth() throws SqlException {
        // The compiler expands a static element next to a date variable on its own and keeps one IR
        // entry per interval that the expansion leaves. Its time list values merge between them as
        // in parseTickExpr, so the IR holds the 600 slots of the minute and the date variable,
        // whatever the number of duplicate values.
        final LongList expected = new LongList();
        addMinuteSlots(expected, LocalDateTime.of(2024, 1, 1, 9, 0), ZoneOffset.UTC, 10);
        addLocalDays(expected, LocalDate.of(2024, 1, 8), LocalDate.of(2024, 1, 9), ZoneOffset.UTC);
        final LongList expectedAtPlus5 = new LongList();
        addMinuteSlots(expectedAtPlus5, LocalDateTime.of(2024, 1, 1, 9, 0), ZoneOffset.ofHours(5), 10);
        addLocalDays(expectedAtPlus5, LocalDate.of(2024, 1, 8), LocalDate.of(2024, 1, 9), ZoneOffset.UTC);
        for (int n : new int[]{1, 10, 30}) {
            assertCompiledTickExprIntervals("[$today,2024-01-01T[" + repeatedValues("09:00", n) + "]:[00..59].[0..9]0]", 601, 601, expected);
            assertCompiledTickExprIntervals("[$today,2024-01-01T[" + repeatedValues("09:00@+05:00", n) + "]:[00..59].[0..9]0]", 601, 601, expectedAtPlus5);
        }
        // Each of the 100 outer values adds the same second. The merges after the outer values 3, 6,
        // ..., 99 leave one interval, and the last value adds 100 more. The 101 entries hold one
        // run, so the expression yields 2 intervals at most.
        final LongList second = new LongList();
        addLocalBlock(second, LocalDateTime.of(2024, 1, 1, 9, 0), ZoneOffset.UTC, Micros.SECOND_MICROS);
        addLocalDays(second, LocalDate.of(2024, 1, 8), LocalDate.of(2024, 1, 9), ZoneOffset.UTC);
        assertCompiledTickExprIntervals(
                "[$today,2024-01-01T[" + repeatedValues("09:", 100) + "][" + repeatedValues("00:00", 100) + "]]",
                102,
                2,
                second
        );
    }

    @Test
    public void testCompiledTickExprStaticWithDayFilterInList() throws SqlException {
        assertCompiledTickExpr("[2025-01-15, $today..$today + 7d]#workday");
    }

    @Test
    public void testCompiledTickExprStaticWithDurationInList() throws SqlException {
        assertCompiledTickExpr("[2025-01-15, $today];4h");
    }

    @Test
    public void testCompiledTickExprStaticWithTimezoneInList() throws SqlException {
        assertCompiledTickExpr("[2025-01-15, $today]@+03:00");
    }

    @Test
    public void testCompiledTickExprSubDayRangeSpanningDays() throws SqlException {
        // Both endpoints are sub-day → hasBothTimeComponents = true → single interval.
        // The interval spans across midnight into the next day.
        assertCompiledTickExpr("$now..$now + 1d");
    }

    @Test
    public void testCompiledTickExprTimeListBracket() throws SqlException {
        assertCompiledTickExpr("[$today]T[09:00,14:00]");
    }

    @Test
    public void testCompiledTickExprTimeListBracketWithDuration() throws SqlException {
        assertCompiledTickExpr("[$today]T[09:30,14:30];1h");
    }

    @Test
    public void testCompiledTickExprTimeListEmptyEntry() {
        assertCompileTickExprError("[$today]T[ ,14:00]", "Empty element in time list");
    }

    @Test
    public void testCompiledTickExprTimeListInvalidTime() {
        assertCompileTickExprError("[$today]T[abc,14:00]", "Invalid time in time list");
    }

    @Test
    public void testCompiledTickExprTimeListInvalidTimezone() throws SqlException {
        // a time list slot with its own timezone parses when it evaluates, so the error is the one a
        // bind variable gets
        assertParsedAndCompiledErrorWithNow("[$today]T[09:00@Bogus,14:00]", "2024-01-02T10:30:00.000000Z", "invalid timezone: Bogus");
    }

    @Test
    public void testCompiledTickExprTimeListManyEntries() throws SqlException {
        // 33 time entries × 2 longs each = 66 > 64 initial ir[] capacity
        StringBuilder sb = new StringBuilder("[$today]T[");
        for (int i = 0; i < 33; i++) {
            if (i > 0) sb.append(',');
            sb.append(String.format("%02d:00", i % 24));
        }
        sb.append(']');
        assertCompiledTickExpr(sb.toString());
    }

    @Test
    public void testCompiledTickExprTimeListOver255Slots() throws SqlException {
        // 300 time list slots, a minute each from 00:00, more than the compiled expression could once count
        final StringBuilder slots = new StringBuilder();
        for (int minute = 0; minute < 300; minute++) {
            if (minute > 0) {
                slots.append(',');
            }
            slots.append(String.format("%02d:%02d", minute / 60, minute % 60));
        }
        assertParsedAndCompiledWithNow(
                "[{lo=2024-03-01T00:00:00.000000Z, hi=2024-03-01T04:59:59.999999Z}]",
                "[$today]T[" + slots + "]",
                "2024-03-01T12:00:00.000000Z"
        );
    }

    @Test
    public void testCompiledTickExprTimeListSlotTimezones() throws SqlException {
        // 10:30 in Tokyo is 01:30Z on both days, also on the day London falls back
        assertParsedAndCompiledWithNow(
                "[{lo=2024-10-27T01:30:00.000000Z, hi=2024-10-27T01:30:59.999999Z},{lo=2024-10-28T01:30:00.000000Z, hi=2024-10-28T01:30:59.999999Z}]",
                "[$today..$tomorrow]T[10:30@Asia/Tokyo]@Europe/London",
                "2024-10-27T05:00:00.000000Z"
        );
        // London springs forward at 01:00 GMT on 2024-03-31, so the minute 01:30 does not exist
        // and matches nothing; 02:30 BST is 01:30Z
        assertParsedAndCompiledWithNow(
                "[{lo=2024-03-31T01:30:00.000000Z, hi=2024-03-31T01:30:59.999999Z}]",
                "[$today]T[01:30@Europe/London,02:30@Europe/London]",
                "2024-03-31T05:00:00.000000Z"
        );
    }

    @Test
    public void testCompiledTickExprTimeListWithElemAndGlobalNamedTimezone() throws SqlException {
        assertCompiledTickExpr("[$today]T[09:00@Europe/London,14:00]@America/New_York");
    }

    @Test
    public void testCompiledTickExprTimeListWithElemAndGlobalTimezone() throws SqlException {
        assertCompiledTickExpr("[$today]T[09:00@Europe/London,14:00]@+03:00");
    }

    @Test
    public void testCompiledTickExprTimeListWithElemTimezone() throws SqlException {
        assertCompiledTickExpr("[$today]T[09:00@+05:00,14:00]");
    }

    @Test
    public void testCompiledTickExprTimeListWithNamedTimezone() throws SqlException {
        assertCompiledTickExpr("[$today]T[09:00@Europe/London,14:00]");
    }

    @Test
    public void testCompiledTickExprTimeListWithSpaces() throws SqlException {
        assertCompiledTickExpr("[$today]T[ 09:00 , 14:00 ]");
    }

    @Test
    public void testCompiledTickExprTimeOverrideWithNamedTimezone() throws SqlException {
        assertCompiledTickExpr("$todayT10:00@America/New_York");
    }

    @Test
    public void testCompiledTickExprTimezoneWithDuration() throws SqlException {
        assertCompiledTickExpr("$today@+03:00;2h");
    }

    @Test
    public void testCompiledTickExprTimezoneWithTimeOverride() throws SqlException {
        assertCompiledTickExpr("$todayT09:30@-05:00");
    }

    @Test
    public void testCompiledTickExprTomorrowWithOffset() throws SqlException {
        assertCompiledTickExpr("$tomorrow - 1d");
    }

    @Test
    public void testCompiledTickExprTrailingT() {
        assertCompileTickExprError("$nowT", "Unknown date variable");
    }

    @Test
    public void testCompiledTickExprUnclosedBracket() {
        assertCompileTickExprError("[$today", "Unclosed '['");
    }

    @Test
    public void testCompiledTickExprUnclosedTimeListBracket() {
        assertCompileTickExprError("[$today]T[09:00", "Unclosed '[' in time list");
    }

    @Test
    public void testCompiledTickExprUnknownDurationUnitAfterManyParts() throws SqlException {
        // the unit that the timestamp drivers do not add follows 100,000 parts that they do
        final String expression = "$today;" + "1h".repeat(100_000) + "1x";
        assertFailsLikeBindVariable(expression, timestampType.getDriver().parseFloorLiteral("2024-01-02T10:00:00.000000Z"), "Invalid duration unit: x");
    }

    @Test
    public void testCompiledTickExprUnknownDurationUnitAgreesWithBindVariableEveryHour() throws SqlException {
        // A literal with a duration unit that the timestamp drivers do not add evaluates to what a
        // bind variable with the same expression parses to, at every hour of a week with a weekend:
        // the same intervals, or the same error. The parser reports the unit where it applies the
        // duration, and checks it after a range between two points in time, which ignores the
        // duration. It skips the duration of a business day range that holds no weekday, where both
        // return no interval: this asserts that the two agree there, not those intervals. A range
        // that starts after it ends fails on both before its duration.
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final long monday = timestampDriver.parseFloorLiteral("2024-01-01T00:00:00.000000Z");
        for (String expression : new String[]{
                "$today;1x",
                "[$today..$today+0bd];1x",
                "$today..$today+1bd;1x",
                "[$now - 1h..$now];1x",
                "[$today + 570m..$now];1x",
                "$tomorrow..$today;1x",
                "[$today]T[09:00,10:00]@Europe/London;1x"
        }) {
            try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(
                    timestampDriver, configuration, expression, 0, expression.length(), 0)) {
                for (int hour = 0; hour < 8 * 24; hour++) {
                    final long now = monday + timestampDriver.fromHours(hour);
                    final String parsed = parseWithChecks(timestampDriver, expression, now, IntervalUtils.TICK_CHECK_ALL);
                    out.clear();
                    String evaluated;
                    try {
                        compiled.evaluate(out, now);
                        evaluated = intervalToString(timestampDriver, out).toString();
                    } catch (CairoException e) {
                        evaluated = "error: " + e.getFlyweightMessage();
                    }
                    sink.clear();
                    sink.put(expression).put(" at ");
                    timestampDriver.append(sink, now);
                    TestUtils.assertEquals(sink.toString(), parsed, evaluated);
                    if ("$today;1x".equals(expression) || "[$now - 1h..$now];1x".equals(expression)) {
                        TestUtils.assertEquals(sink.toString(), "error: Invalid duration unit: x", evaluated);
                    } else if ("[$today + 570m..$now];1x".equals(expression)) {
                        // the range starts at 09:30 and ends at "now"
                        TestUtils.assertEquals(
                                sink.toString(),
                                hour % 24 < 10 ? "error: Invalid date range: start is after end" : "error: Invalid duration unit: x",
                                evaluated
                        );
                    }
                }
            }
        }
    }

    @Test
    public void testCompiledTickExprUnknownDurationUnitFailsLikeBindVariable() throws SqlException {
        // The timestamp drivers add the duration units y M w d h m s T u n, and the parser, which SQL
        // uses for a bind variable, fails on any other character in the place of a unit where it
        // applies the duration. The compiled form, which SQL uses for a literal with a date variable,
        // took any such character for a unit, and evaluated the expression to an interval from the
        // beginning of time. It fails with the error of the parser instead, as the SQL compiles and
        // when the query runs.
        final TimestampDriver timestampDriver = timestampType.getDriver();
        // typos of units, signs, separators, a space and characters of two UTF-8 bytes
        final String[] units = {"x", "q", "b", "H", "D", "S", "Y", "W", "U", "N", "t", "-", "+", "!", "%", ".", " ", "\u00b5", "\u00e9"};
        // ? stands for the unit, which a digit or the end of the duration follows, so that the
        // duration has no other error
        final String[] expressions = {
                "$now;1?", "$NOW;1?", "$today;1?", "[$now];1?", "$now;1h1?", "$today;1?1h", "$today;0?", "$today;-1?",
                "$today;+1?", "$today;2_147_483_647?", "$today..$tomorrow;1?", "$yesterday..$today + 3d;1?",
                "[$today, $tomorrow]T09:00;1h1?", "[$today]T[09:00,10:00];1?", "$todayT09:00;1?", "$today@Europe/London;1?",
                "[$today]T09:00@+02:00;1?", "$today#Tue;1?", "[$yesterday..$today, $now];1?",
                // ranges between two points in time, which ignore the duration
                "$now - 1h..$now;1?", "[$now - 1h..$now]@+02:00;1?", "$yesterday + 30m..$now;1h1?",
                "[$now - 1h..$now, $now - 3h..$now - 2h];1?"
        };
        final String[] nows = {
                "2024-01-02T10:00:00.000000Z",
                // before the epoch
                "1969-12-31T23:30:00.000000Z",
                // the last minute of a leap day
                "2024-02-29T23:59:00.000000Z",
                // London springs forward at 01:00Z
                "2024-03-31T00:30:00.000000Z",
                // far in the future, before 2262: the parser writes $now out as text and reads it
                // back with the duration, and the nanosecond date format ends with the year 2261
                ColumnType.isTimestampNano(timestampType.getTimestampType()) ? "2261-12-27T10:00:00.000000Z" : "9999-12-30T10:00:00.000000Z"
        };
        for (String now : nows) {
            for (String unit : units) {
                for (String expression : expressions) {
                    assertFailsLikeBindVariable(
                            expression.replace("?", unit),
                            timestampDriver.parseFloorLiteral(now),
                            "Invalid duration unit: " + unit
                    );
                }
            }
        }
        // the parser takes a separator in the place of the unit for a repeating interval
        assertFailsLikeBindVariable(
                "$today;1;",
                timestampDriver.parseFloorLiteral("2024-01-02T10:00:00.000000Z"),
                "Invalid interval format: 2024-01-02;1;"
        );
        // and shows the text from the first ';' on after a range between two points in time
        assertFailsLikeBindVariable(
                "$now - 1h..$now;1;",
                timestampDriver.parseFloorLiteral("2024-01-02T10:00:00.000000Z"),
                "Invalid interval format: ;1;"
        );
    }

    @Test
    public void testCompiledTickExprUnknownDurationUnitKeepsCompileErrors() {
        // A duration with a unit that the timestamp drivers do not add makes the compiled form parse
        // the expression when it evaluates, once it built the IR in full: SQL compiling a literal
        // keeps rejecting what the IR rejects, as it does with the unit h, including parts of the
        // expression that the parser checks after the duration. Each case gives the error of the
        // compiled form, and the error of the parser.
        final String[][] cases = {
                // a range between two points in time ignores the time part, which the parser checks
                // before the duration, as after a day
                {"[$now - 1h..$now]T25:00;1x", "Invalid time override: 25:00", "Invalid date: 2024-01-02T25:00"},
                {"[$today]T[25:00];1x", "Invalid time in time list: 25:00", "Invalid date: 2024-01-02T25:00"},
                {"$today@Bogus/Zone;1x", "invalid timezone: Bogus/Zone", "Invalid duration unit: x"},
                {"$today;h1x", "Expected number before unit 'h'", "Expected number before unit 'h'"},
                {"[$today]T09:00;1x;1d;3", "Expected number before unit ';'", "Invalid duration unit: x"},
                // the compiled form reports a later error of the duration than the parser
                {"$today;1x-", "Missing unit at end of duration", "Invalid duration unit: x"},
                {"$today;1x99999999999h", "Duration not a number: 99999999999", "Invalid duration unit: x"}
        };
        for (String[] c : cases) {
            assertCompileError(c[0], c[1]);
            assertCompileError(c[0].replace('x', 'h'), c[1]);
            assertParseError(c[0], c[2]);
        }
        // a space, and a character of 4 UTF-8 bytes, which is two Java chars: the second char takes
        // the place of a number
        assertCompileError("$today;1 h", "Expected number before unit 'h'");
        assertParseError("$today;1 h", "Invalid duration unit:  ");
        assertCompileError("$today;1\uD83D\uDE00", "Expected number before unit '\uDE00'");
        assertParseError("$today;1\uD83D\uDE00", "Invalid duration unit: \uD83D");
        // the IR parses a static element with the duration as it compiles
        assertCompileError("[$today, 2024-01-05];1x", "Invalid duration unit: x");
        assertParseError("[$today, 2024-01-05];1x", "Invalid duration unit: x");
    }

    @Test
    public void testCompiledTickExprUppercaseTomorrow() throws SqlException {
        assertCompiledTickExpr("$TOMORROW");
    }

    @Test
    public void testCompiledTickExprUppercaseVariable() throws SqlException {
        assertCompiledTickExpr("$TODAY");
    }

    @Test
    public void testCompiledTickExprValidate() throws SqlException {
        // validate() runs as user SQL compiles, against the time it compiles at: it fails on an
        // expression that does not parse, and on one over the interval limit, with the error and at
        // the position that evaluating it would give
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final long now = timestampDriver.parseFloorLiteral("2024-01-02T03:00:00.000000Z");
        // the compiled form
        assertValidateFails("[$today..$today + 2000d]T09:00", now, "Date range expansion produces too many intervals (max 1024)");
        assertValidateFails("$today..$today + 600dT[09:00,15:00]", now, "Date range expansion produces too many intervals (max 1024)");
        // the form that parses on every evaluation
        assertValidateFails("[$today,garbage]T[09:00,10:00]", now, "Invalid date: garbage");
        assertValidateFails("$todayT[09,14]:[00,99]", now, "Invalid date: 2024-01-02T09:99");
        assertValidateFails("[$today..$today + 600d, 2024-01-01]T[09:00,10:00]", now, "produces too many intervals (max 1024)");
        // An expression within the limit passes, and so does a range that starts after it ends:
        // whether it does depends on the time the query runs. Such an expression still fails when
        // it evaluates, so validate() leaves the checks of evaluate() as they were.
        for (String expression : new String[]{
                "[$today..$today + 500d]T[09:00,15:00]",
                "[$tomorrow..$today]",
                "$today + 570m..$now",
                "[$tomorrow..$today, 2024-01-03]T[09:00,10:00]"
        }) {
            try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(
                    timestampDriver, configuration, expression, 0, expression.length(), 7)) {
                compiled.validate(now);
                out.clear();
                try {
                    compiled.evaluate(out, now);
                    Assert.assertTrue(expression, expression.startsWith("[$today.."));
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "Invalid date range: start is after end");
                }
            }
        }
    }

    @Test
    public void testCompiledTickExprYesterdayWithOffset() throws SqlException {
        assertCompiledTickExpr("$yesterday + 3h");
    }

    @Test
    public void testCompiledZonedStaticElementMergeCadence() throws SqlException {
        // compileStaticElement expands the static element on its own: 8 hours x 60 minutes x 10
        // seconds x 100 = 480,000 leaves of 10 ms, which form 480 ten-second blocks, more than the
        // merge threshold (256). Its unions wait until the range grew by the threshold since the
        // last one: 1,875 of them, the last at the last leaf, each of which reads the cap once, and
        // the list reads it once more for its own checks. A union after every leaf, once the range
        // holds 256 blocks, would read the cap about 256,000 times.
        Assert.assertEquals(256, configuration.getSqlIntervalIncrementalMergeThreshold());
        final String expression = "[$today,2024-01-01T[09..16]:[00..59]:[00..09].[0..9][0..9]]@America/New_York";
        final MaxIntervalsCountingConfiguration countingConfiguration = new MaxIntervalsCountingConfiguration();
        IntervalUtils.compileTickExpr(timestampType.getDriver(), countingConfiguration, expression, 0, expression.length(), 0).close();
        Assert.assertEquals(1_876, countingConfiguration.getterCallCount);
        // The blocks from 09:00 to 16:59 at New York, which keeps -05:00 in January, and the New York
        // day of $today, the Monday 2024-01-08. The compiled expression keeps an IR entry per block
        // and one for $today, and counts as many intervals.
        final LongList expected = new LongList();
        for (int hour = 9; hour < 17; hour++) {
            for (int minute = 0; minute < 60; minute++) {
                addLocalBlock(expected, LocalDateTime.of(2024, 1, 1, hour, minute), ZoneOffset.ofHours(-5), 10 * Micros.SECOND_MICROS);
            }
        }
        addLocalDays(expected, LocalDate.of(2024, 1, 8), LocalDate.of(2024, 1, 9), ZoneId.of("America/New_York"));
        assertCompiledTickExprIntervals(expression, 481, 481, expected);
    }

    // ==================== DateVariableExpr unit tests ====================

    @Test
    public void testDateCeilMicroWithDiffFraction() throws NumericException {
        assertDateFloor("2015-02-28T08:22:44.556012Z", "2015-02-28T08:22:44.556012");
        assertDateFloor("2015-02-28T08:22:44.556010Z", "2015-02-28T08:22:44.55601");
        assertDateFloor("2015-02-28T08:22:44.556000Z", "2015-02-28T08:22:44.5560");
        assertDateFloor("2015-02-28T08:22:44.556000Z", "2015-02-28T08:22:44.556");
        assertDateFloor("2015-02-28T08:22:44.550000Z", "2015-02-28T08:22:44.55");
        assertDateFloor("2015-02-28T08:22:44.500000Z", "2015-02-28T08:22:44.5");
    }

    @Test(expected = NumericException.class)
    public void testDateFloorFails() throws NumericException {
        assertDateFloor("", "2015-01-01T00:00:00.000000-1");
    }

    @Test(expected = NumericException.class)
    public void testDateFloorFailsOnTzSign() throws NumericException {
        assertDateFloor("", "2015-01-01T00:00:00.000000≠10");
    }

    @Test
    public void testDateFloorMicroWithTzHrs() throws NumericException {
        assertDateFloor("2015-02-28T09:22:44.556011Z", "2015-02-28T08:22:44.556011-01");
        assertDateFloor("2015-02-28T09:22:44.556011Z", "2015-02-28 08:22:44.556011-01");
    }

    @Test
    public void testDateFloorMicroWithTzHrsMins() throws NumericException {
        assertDateFloor("2015-02-28T04:38:44.556011Z", "2015-02-28T06:00:44.556011+01:22");
        assertDateFloor("2015-02-28T04:38:44.556011Z", "2015-02-28T06:00:44.556011+0122");
        assertDateFloor("2015-02-28T04:38:44.556011Z", "2015-02-28 06:00:44.556011+0122");
    }

    @Test
    public void testDateFloorMillsWithTzHrsMins() throws NumericException {
        assertDateFloor("2015-02-28T06:30:44.555000Z", "2015-02-28T06:00:44.555-00:30");
        assertDateFloor("2015-02-28T06:30:44.555000Z", "2015-02-28T06:00:44.555-0030");
    }

    @Test
    public void testDateFloorSecsWithTzHrsMins() throws NumericException {
        assertDateFloor("2015-02-28T05:00:44.000000Z", "2015-02-28T06:00:44+01:00");
        assertDateFloor("2015-02-28T05:00:44.000000Z", "2015-02-28T06:00:44+0100");
        assertDateFloor("2015-02-28T05:00:44.000000Z", "2015-02-28 06:00:44+0100");
    }

    @Test
    public void testDateFloorYYYY() throws NumericException {
        assertDateFloor("2015-01-01T00:00:00.000000Z", "2015");
    }

    @Test
    public void testDateFloorYYYYMM() throws NumericException {
        assertDateFloor("2015-02-01T00:00:00.000000Z", "2015-02");
    }

    @Test
    public void testDateFloorYYYYMMDD() throws NumericException {
        assertDateFloor("2015-02-28T00:00:00.000000Z", "2015-02-28");
    }

    @Test
    public void testDateFloorYYYYMMDDH() throws NumericException {
        assertDateFloor("2015-02-28T07:00:00.000000Z", "2015-02-28T07");
        assertDateFloor("2015-02-28T07:00:00.000000Z", "2015-02-28 07");
    }

    @Test
    public void testDateFloorYYYYMMDDHm() throws NumericException {
        assertDateFloor("2015-02-28T07:21:00.000000Z", "2015-02-28T07:21");
        assertDateFloor("2015-02-28T07:21:00.000000Z", "2015-02-28 07:21");
    }

    @Test
    public void testDateFloorYYYYMMDDHms() throws NumericException {
        assertDateFloor("2015-02-28T07:21:44.000000Z", "2015-02-28T07:21:44");
        assertDateFloor("2015-02-28T07:21:44.000000Z", "2015-02-28 07:21:44");
    }

    @Test
    public void testDateFloorYYYYMMDDHmsS() throws NumericException {
        assertDateFloor("2015-02-28T07:21:44.556000Z", "2015-02-28T07:21:44.556");
        assertDateFloor("2015-02-28T07:21:44.556000Z", "2015-02-28 07:21:44.556");
    }

    @Test
    public void testDateFloorYYYYMMDDHmsSU() throws NumericException {
        assertDateFloor("2015-02-28T07:21:44.556011Z", "2015-02-28T07:21:44.556011");
        assertDateFloor("2015-02-28T07:21:44.556011Z", "2015-02-28 07:21:44.556011");
    }

    @Test
    public void testDateListAllElementsHaveTime() throws SqlException {
        // All elements have time - global time suffix is completely ignored
        assertBracketInterval(
                "[{lo=2026-01-01T09:30:00.000000Z, hi=2026-01-01T09:30:59.999999Z},{lo=2026-01-02T14:00:00.000000Z, hi=2026-01-02T14:00:59.999999Z}]",
                "[2026-01-01T09:30, 2026-01-02T14:00]T10:00"
        );
    }

    @Test
    public void testDateListBracketExpansionWithPerElementDayFilter() throws SqlException {
        // Bracket expansion inside element WITH per-element day filter
        // [2024-01-[01..07]#Mon,2024-01-15] - the #Mon applies to all expanded dates from 01..07
        // 2024-01-01 is Monday (passes), 02-07 are Tue-Sun (fail), 2024-01-15 has no filter (passes)
        assertBracketInterval(
                "[{lo=2024-01-01T00:00:00.000000Z, hi=2024-01-01T23:59:59.999999Z},{lo=2024-01-15T00:00:00.000000Z, hi=2024-01-15T23:59:59.999999Z}]",
                "[2024-01-[01..07]#Mon,2024-01-15]"
        );
    }

    @Test
    public void testDateListCoveredElementAfterZonedDateVariableDay() throws SqlException {
        // The time zone of the date variable range gives its day, the Monday 2024-01-08, a merge
        // range of its own, and the day restores the merge range of the list when it closes its
        // own. The third element adds its 1,200 slots of 10 ms, which never touch, to the merge
        // range of the list, so its merges union them into the first element's day before they
        // count them against the cap (1,024). Merges from the start of the zoned day would count
        // every slot, since no interval there covers them.
        Assert.assertEquals(1_024, configuration.getSqlIntervalMaxIntervalsAfterMerge());
        final ZoneOffset plus5 = ZoneOffset.ofHours(5);
        final LongList expected = new LongList();
        addLocalDays(expected, LocalDate.of(2024, 1, 1), LocalDate.of(2024, 1, 2), ZoneOffset.UTC);
        // the minutes 09:00 and 10:00 of the zoned day
        addLocalBlock(expected, LocalDateTime.of(2024, 1, 8, 9, 0), plus5, Micros.MINUTE_MICROS);
        addLocalBlock(expected, LocalDateTime.of(2024, 1, 8, 10, 0), plus5, Micros.MINUTE_MICROS);
        for (CairoConfiguration cfg : new CairoConfiguration[]{configuration, mergeThreshold4Configuration}) {
            assertTickExprIntervals(cfg, expected, "[2024-01-01T00:00;1d,$today..$today@+05:00,2024-01-01T09:[00..01]:[00..59].[0..9]0]T[09..10]:00");
        }
        // control: without the covering day, the slots exceed the cap
        assertTickExprErrorAtMonday(
                "[$today..$today@+05:00,2024-01-01T09:[00..01]:[00..59].[0..9]0]T[09..10]:00",
                "Bracket expansion produces too many intervals (max 1024)"
        );
    }

    @Test
    public void testDateListCoveredElementAfterZonedElement() throws SqlException {
        // The second element's time zone gives it a merge range of its own, and the element
        // restores the merge range of the list when it closes its own. The third element adds its
        // 1,200 slots of 10 ms, which never touch, to the merge range of the list, so its merges
        // union them into the first element's day before they count them against the cap
        // (1,024). Merges from the start of the zoned element would count every slot, since no
        // interval there covers them.
        Assert.assertEquals(1_024, configuration.getSqlIntervalMaxIntervalsAfterMerge());
        final LongList expected = new LongList();
        addLocalDays(expected, LocalDate.of(2024, 1, 1), LocalDate.of(2024, 1, 2), ZoneOffset.UTC);
        // the zoned element's days 2024-01-05 and 2024-01-06 at +05:00
        addLocalDays(expected, LocalDate.of(2024, 1, 5), LocalDate.of(2024, 1, 7), ZoneOffset.ofHours(5));
        for (CairoConfiguration cfg : new CairoConfiguration[]{configuration, mergeThreshold4Configuration}) {
            assertTickExprIntervals(cfg, expected, "[2024-01-01,2024-01-[05..06]@+05:00,2024-01-01T[09..10]:00:[00..59].[0..9]0]");
        }
        // control: without the covering day, the slots exceed the cap
        assertTickExprErrorAtMonday(
                "[2024-01-[05..06]@+05:00,2024-01-01T[09..10]:00:[00..59].[0..9]0]",
                "Bracket expansion produces too many intervals (max 1024)"
        );
    }

    @Test
    public void testDateListCoveredElementOverMaxIntervals() throws SqlException {
        // A date list element with no time zone, expanding day filter or tick calendar adds its
        // intervals to the merge range of the list, and so do the time list values with no zone.
        // Their merges union them with the earlier elements before they count them, so a later
        // element whose own intervals exceed the cap (1,024) parses when an earlier element covers
        // them.
        Assert.assertEquals(1_024, configuration.getSqlIntervalMaxIntervalsAfterMerge());
        final LongList day = new LongList();
        addLocalDays(day, LocalDate.of(2024, 1, 1), LocalDate.of(2024, 1, 2), ZoneOffset.UTC);
        final LongList monday = new LongList();
        addLocalDays(monday, LocalDate.of(2024, 1, 8), LocalDate.of(2024, 1, 9), ZoneOffset.UTC);
        final String[] coveredLists = {
                // 1,200 slots of 10 ms that never touch
                "[2024-01-01,2024-01-01T[09..10]:00:[00..59].[0..9]0]",
                "[2024-01-01,2024-01-01T[09:00,10:00]:[00..59].[0..9]0]",
                // 6,000 slots of 1 ms in one time list value, and in two
                "[2024-01-01,2024-01-01T[09:00]:[00..59].[0..9][0..9]0]",
                "[2024-01-01,2024-01-01T[09:00,10:00]:[00..59].[0..9][0..9]0]",
                // nested time lists: 1,200 slots, and 1,200 seconds that the outer values add 600 at a
                // time, which a merge from the start of the time list would count
                "[2024-01-01,2024-01-01T[09:,10:][00:,01:][00..59].[0..9]0]",
                "[2024-01-01,2024-01-01T[09:,10:][" + evenSeconds(600) + "]]",
                // a zoned value of 600 slots and a zone-less one
                "[2024-01-01,2024-01-01T[09:00@+00:00,10:00]:[00..59].[0..9]0]",
                // date-precise day filters; 2024-01-01 is a Monday
                "[2024-01-01,2024-01-01T[09..10]:00:[00..59].[0..9]0#mon]",
                "[2024-01-01,2024-01-01T[09..10]:00:[00..59].[0..9]0]#mon",
                "[2024-01-01,2024-01-01T[09:00,10:00]:[00..59].[0..9]0]#mon",
        };
        for (CairoConfiguration cfg : new CairoConfiguration[]{configuration, mergeThreshold4Configuration}) {
            for (String interval : coveredLists) {
                assertTickExprIntervals(cfg, day, interval);
            }
            // a day of a date variable range, $today is 2024-01-08
            assertTickExprIntervals(cfg, monday, "[2024-01-08T00:00;1d,$today..$today]T[09..10]:00:[00..59].[0..9]0");
        }
        // An element over the cap fails when no earlier element covers it, as it does alone
        assertTickExprErrorAtMonday("[2024-01-01T[09..10]:00:[00..59].[0..9]0,2024-01-01]", "Bracket expansion produces too many intervals (max 1024)");
        assertTickExprErrorAtMonday("2024-01-01T[09..10]:00:[00..59].[0..9]0", "Bracket expansion produces too many intervals (max 1024)");
    }

    @Test
    public void testDateListDayFilterBracketElementsOverMaxIntervalsFailsEarly() {
        // control: the third element alone fails with its own parse error
        assertTickExprErrorAtMonday("[2024-13-45]#mon", "Invalid date: 2024-13-45");
        // Each 100-year element filters to about 5,218 Mondays. The incremental merge checks the
        // count inside the second element, so too-many-intervals wins over the unparsable element.
        assertTickExprErrorAtMonday(
                "[20[00..99],20[00..99],2024-13-45]#mon",
                "Bracket expansion produces too many intervals (max 1024)"
        );
    }

    @Test
    public void testDateListDayFilterElementGrowthBetweenMerges() throws SqlException {
        // N copies of a 31-day element: each adds the Mondays of January 2024 again, so the merged
        // filtered result stays at those 5 Mondays for any N. Each element filters its own leaves
        // before any union, and expandDateList merges the growth of the filtered elements.
        for (int n : new int[]{6, 50, 400}) {
            final StringBuilder interval = new StringBuilder("[");
            for (int k = 0; k < n; k++) {
                interval.append(k == 0 ? "" : ",").append("2024-01-[01..31]");
            }
            interval.append("]#mon");
            final PeakLongList peakOut = new PeakLongList();
            parseTickExprAtMonday(configuration, peakOut, interval.toString());
            assertFullDayMondays(peakOut, countMondays(LocalDate.of(2024, 1, 1), LocalDate.of(2024, 1, 31)));
            // At most C + 2(T - 1 + B) intervals with C = 1,024, T = 256 and B = 1: the list at a
            // merge, plus the unmerged tail and the output of applyDayFilter past the list end.
            Assert.assertTrue("peak longs " + peakOut.peak, peakOut.peak <= 2 * (1_024 + 2 * 256));
        }
    }

    @Test
    public void testDateListDayFilterElementsAtMaxIntervals() throws SqlException {
        // The first element keeps the 1,008 Mondays among days 1-28 of 2004 to 2024 (21 years x 12
        // months x 4), below the cap of 1,024, and ends with 20 filtered Mondays that no union of the
        // list joined yet. The second element repeats the 48 Mondays of 2024 and adds its leaves to
        // the merge range of the list, which has grown by the merge threshold (256) at its 236th
        // leaf 2024-09-12. That merge unions its 34 Mondays into the first element's before it
        // counts them: counted without that union, the list would hold 1,042 intervals.
        final LongList expected = new LongList();
        for (LocalDate day = LocalDate.of(2004, 1, 1); day.getYear() <= 2024; day = day.plusDays(1)) {
            if (day.getDayOfMonth() <= 28 && day.getDayOfWeek() == DayOfWeek.MONDAY) {
                addLocalDays(expected, day, day.plusDays(1), ZoneOffset.UTC);
            }
        }
        Assert.assertEquals(2 * 1_008, expected.size());
        final TimestampDriver timestampDriver = timestampType.getDriver();
        out.clear();
        parseTickExprAtMonday(configuration, out, "[[2004..2024]-[01..12]-[01..28]#mon,2024-[01..12]-[01..28]#mon]");
        TestUtils.assertEquals(intervalToString(timestampDriver, expected).toString(), intervalToString(timestampDriver, out));
    }

    @Test
    public void testDateListDayFilterMatchesUnmergedOracle() throws SqlException {
        // The oracle configuration never merges incrementally, so it filters every leaf before the
        // single final union. The default configuration merges along the way and must agree.
        final CairoConfiguration oracleConfiguration = new DefaultCairoConfiguration(".") {
            @Override
            public int getSqlIntervalIncrementalMergeThreshold() {
                return 100_000;
            }

            @Override
            public int getSqlIntervalMaxIntervalsAfterMerge() {
                return 1_000_000;
            }
        };
        for (String interval : new String[]{
                "[20[00..05],2024-02-[01..07]]@+05:00#mon",
                "[20[00..05]@+05:00,2024-02-[01..07]@+05:00]#mon",
                "[2024-[01..12],2024-[01..12],2024-[01..12],2024-[01..12],2024-[01..12],2024-02-[01..07]]@+05:00#mon",
                "[2024-[01..12],2024-[01..12],2024-[01..12],2024-[01..12],2024-[01..12],2024-02-[01..07]]#mon",
                "[20[00..05]-[01..12]-[01..28],2024-02-[01..07]]@+05:00",
        }) {
            final LongList expected = new LongList();
            final LongList actual = new LongList();
            parseTickExprAtMonday(oracleConfiguration, expected, interval);
            parseTickExprAtMonday(configuration, actual, interval);
            Assert.assertEquals(interval, expected, actual);
        }
    }

    @Test
    public void testDateListElementOverMaxIntervalsFailsEarly() throws SqlException {
        // control: month 13 fails at the first value of the second element
        assertTickExprErrorAtMonday("[2024-01-01,2024-13-[01..28]T[00..23]:00]", "Invalid date: 2024-13-01T00:00");
        // The one-minute values of the second element lie an hour apart and never touch. Its merges
        // union them with the first element's day, which covers the 24 values of January 1, and the
        // list's 1,025th interval is the 16th value of February 16, so too-many-intervals wins over
        // month 13.
        assertTickExprErrorAtMonday(
                "[2024-01-01,2024-[01..13]-[01..28]T[00..23]:00]",
                "Bracket expansion produces too many intervals (max 1024)"
        );
        // parseTickExpr unwinds the list, and the next parse on this thread succeeds
        Assert.assertEquals(0, out.size());
        assertFirstMinuteOfEachHour(configuration, "[2024-[01..02]-[01..20]T[00..23]:00,2024-01-[01..11]T[00..23]:00]", 2, 20, 24);
    }

    @Test
    public void testDateListElementWithHighPrecisionTime() throws SqlException {
        // Element with microseconds keeps precise time, second element gets T10:00 suffix
        // Note: nano mode fills remaining nanos with 9s for hi (.123456999Z)
        out.clear();
        final TimestampDriver timestampDriver = timestampType.getDriver();
        String interval = "[2026-01-01T09:30:45.123456, 2026-01-02]T10:00";
        parseTickExpr(timestampDriver, interval, 0, interval.length(), 0, out);
        TestUtils.assertEquals(
                ColumnType.isTimestampNano(timestampType.getTimestampType())
                        ? "[{lo=2026-01-01T09:30:45.123456000Z, hi=2026-01-01T09:30:45.123456999Z},{lo=2026-01-02T10:00:00.000000000Z, hi=2026-01-02T10:00:59.999999999Z}]"
                        : "[{lo=2026-01-01T09:30:45.123456Z, hi=2026-01-01T09:30:45.123456Z},{lo=2026-01-02T10:00:00.000000Z, hi=2026-01-02T10:00:59.999999Z}]",
                intervalToString(timestampDriver, out)
        );
    }

    @Test
    public void testDateListElementWithTimeAndGlobalDayFilter() throws SqlException {
        // Day filter from suffix applies to both elements
        // 2026-01-05 is Monday (passes), 2026-01-06 is Tuesday (filtered out)
        assertBracketInterval(
                "[{lo=2026-01-05T09:30:00.000000Z, hi=2026-01-05T09:30:59.999999Z}]",
                "[2026-01-05T09:30, 2026-01-06]T10:00#Mon"
        );
    }

    @Test
    public void testDateListElementWithTimeAndGlobalTimeSuffix() throws SqlException {
        // Element with time keeps its time, element without time gets global suffix time
        // 2026-01-01T09:30 keeps 09:30, 2026-01-02 gets T10:00
        assertBracketInterval(
                "[{lo=2026-01-01T09:30:00.000000Z, hi=2026-01-01T09:30:59.999999Z},{lo=2026-01-02T10:00:00.000000Z, hi=2026-01-02T10:00:59.999999Z}]",
                "[2026-01-01T09:30, 2026-01-02]T10:00"
        );
    }

    @Test
    public void testDateListElementWithTimeAndGlobalTimeSuffixWithDuration() throws SqlException {
        // Duration from suffix applies to both elements
        // 2026-01-01T09:30 keeps 09:30 + 1h duration, 2026-01-02 gets T10:00 + 1h duration
        assertBracketInterval(
                "[{lo=2026-01-01T09:30:00.000000Z, hi=2026-01-01T10:29:59.999999Z},{lo=2026-01-02T10:00:00.000000Z, hi=2026-01-02T10:59:59.999999Z}]",
                "[2026-01-01T09:30, 2026-01-02]T10:00;1h"
        );
    }

    @Test
    public void testDateListElementWithTimeAndGlobalTimeSuffixWithTimezone() throws SqlException {
        // Timezone from suffix applies to both elements
        // 2026-01-01T09:30 in UTC+2 = 07:30 UTC, 2026-01-02T10:00 in UTC+2 = 08:00 UTC
        assertBracketInterval(
                "[{lo=2026-01-01T07:30:00.000000Z, hi=2026-01-01T07:30:59.999999Z},{lo=2026-01-02T08:00:00.000000Z, hi=2026-01-02T08:00:59.999999Z}]",
                "[2026-01-01T09:30, 2026-01-02]T10:00@+02:00"
        );
    }

    @Test
    public void testDateListElementWithTimeAndMalformedListTimeFails() throws SqlException {
        // An element with a time of its own keeps it and ignores the time part of the list, which
        // still fails the list where it does not parse, as it fails a day: the parser reads it after
        // the date of the element. The parser selected the element there.
        final long now = timestampType.getDriver().parseFloorLiteral("2024-01-02T10:00:00.000000Z");
        // the reference: the same time part after a day
        assertParseFailsAtEveryCheckLevel("[2024-01-02]T[25:00]", now, "Invalid date: 2024-01-02T25:00");
        for (String element : new String[]{
                "2024-01-02T10:00", "2024-01-02T09", "2024-01-02T09:00:00.000000Z", "2024-01-02T09:00@+02:00",
                "2024-01-02T09:00#Tue", "2024-01-02T09:00;1h"
        }) {
            for (String timePart : new String[]{"T[25:00]", "T25:00"}) {
                assertParseFailsAtEveryCheckLevel("[" + element + "]" + timePart, now, "Invalid date: 2024-01-02T25:00");
            }
            assertParseFailsAtEveryCheckLevel("[" + element + "]T", now, "Invalid date: 2024-01-02T");
        }
        // the date of an element with brackets expands as a day would, and fails on its first day
        assertParseFailsAtEveryCheckLevel("[2024-01-[01..03]T09:00]T25:00", now, "Invalid date: 2024-01-01T25:00");
        assertParseFailsAtEveryCheckLevel("[2024-W03-1T09:00]T25:00", now, "Invalid date: 2024-W03-1T25:00");
        // an element without a day fails on the time part of the list after its date, which no date
        // takes, before it fails on its own time
        assertParseFailsAtEveryCheckLevel("[2024-01T09:00]T10:00", now, "Invalid date: 2024-01T10:00");
        assertParseFailsAtEveryCheckLevel("[2024-01T09:00]", now, "Invalid date: 2024-01T09:00");
        // with a time part that parses, the element keeps its own time
        assertBracketInterval(
                "[{lo=2024-01-01T09:00:00.000000Z, hi=2024-01-01T09:00:59.999999Z},{lo=2024-01-02T09:00:00.000000Z, hi=2024-01-02T09:00:59.999999Z},{lo=2024-01-03T09:00:00.000000Z, hi=2024-01-03T09:00:59.999999Z}]",
                "[2024-01-[01..03]T09:00]T[10:00,14:30]"
        );
        assertBracketInterval(
                "[{lo=2024-01-15T09:00:00.000000Z, hi=2024-01-15T09:00:59.999999Z}]",
                "[2024-W03-1T09:00]T10:00"
        );
    }

    @Test
    public void testDateListElementWithTimeAndPerElementTzAndGlobalTz() throws SqlException {
        // Per-element timezone takes precedence over global
        // 2026-01-01T09:30@UTC stays UTC, 2026-01-02 gets T10:00@+03:00 = 07:00 UTC
        assertBracketInterval(
                "[{lo=2026-01-01T09:30:00.000000Z, hi=2026-01-01T09:30:59.999999Z},{lo=2026-01-02T07:00:00.000000Z, hi=2026-01-02T07:00:59.999999Z}]",
                "[2026-01-01T09:30@UTC, 2026-01-02]T10:00@+03:00"
        );
    }

    @Test
    public void testDateListElementWithTimeAndTimeListSuffix() throws SqlException {
        // Element with time gets one interval, element without time gets expanded from time list
        // 2026-01-01T09:30 keeps its time (one interval)
        // 2026-01-02 gets both T[10:00,14:00] (two intervals)
        assertBracketInterval(
                "[{lo=2026-01-01T09:30:00.000000Z, hi=2026-01-01T09:30:59.999999Z},{lo=2026-01-02T10:00:00.000000Z, hi=2026-01-02T10:00:59.999999Z},{lo=2026-01-02T14:00:00.000000Z, hi=2026-01-02T14:00:59.999999Z}]",
                "[2026-01-01T09:30, 2026-01-02]T[10:00,14:00]"
        );
    }

    @Test
    public void testDateListElementWithTimeAndZonedTimeListSuffix() throws SqlException {
        // An element with its own time of day keeps it and skips the time part of the suffix,
        // also when the slots of that time list carry their own timezone
        assertBracketIntervalWithNow(
                "[{lo=2024-01-02T18:00:00.000000Z, hi=2024-01-02T18:00:00.000000Z}]",
                "[$today + 18h]T[10:30@Europe/London]",
                "2024-01-02T10:30:00.000000Z"
        );
        // the global timezone still applies to the element
        assertBracketIntervalWithNow(
                "[{lo=2024-01-02T16:00:00.000000Z, hi=2024-01-02T16:00:00.000000Z}]",
                "[$today + 18h]T[10:30@Europe/London]@+02:00",
                "2024-01-02T10:30:00.000000Z"
        );
        // an element without a time of day takes the slots: 10:30@+02:00 is 08:30Z
        assertBracketIntervalWithNow(
                "[{lo=2024-01-02T08:30:00.000000Z, hi=2024-01-02T08:30:59.999999Z},{lo=2024-01-02T11:00:00.000000Z, hi=2024-01-02T11:00:59.999999Z},{lo=2024-01-02T18:00:00.000000Z, hi=2024-01-02T18:00:00.000000Z}]",
                "[$today + 18h, $today]T[10:30@+02:00,11:00]",
                "2024-01-02T10:30:00.000000Z"
        );
        // a static element with its own time, with and without a duration
        assertBracketInterval(
                "[{lo=2024-01-05T09:00:00.000000Z, hi=2024-01-05T09:00:59.999999Z}]",
                "[2024-01-05T09:00]T[10:30@+02:00]"
        );
        assertBracketInterval(
                "[{lo=2024-01-05T09:00:00.000000Z, hi=2024-01-05T09:59:59.999999Z}]",
                "[2024-01-05T09:00]T[10:30@+02:00];1h"
        );
    }

    @Test
    public void testDateListElementWithTimeListAndListTimeFails() {
        // The parser rejects a date list element without a day that has a time part of its own when
        // the list has a time part too, and an element whose own time part is a time list. The
        // compiled form, which SQL uses for a literal with a date variable, must reject them too,
        // rather than ignore the time list of the element and expand its month with the time of the
        // list.
        final String now = "2024-01-02T10:00:00.000000Z";
        assertParsedAndCompiledRejectedWithNow("[2024-01T[09:00,10:00], $today]T10:00", now, "Invalid date");
        assertParsedAndCompiledRejectedWithNow("[2024-01T[09:00], $NOW + 1w]T09:00:30", now, "Invalid date");
        // the element fails on both paths with a time of day of its own, or with a day
        assertParsedAndCompiledRejectedWithNow("[2024-01T09:00, $today]T10:00", now, "Invalid date");
        assertParsedAndCompiledRejectedWithNow("[2024-01-[01..03]T[09:00], $today]T10:00", now, "Invalid date");
    }

    @Test
    public void testDateListElementsAtMaxIntervals() throws SqlException {
        // The one-minute values of each list lie an hour apart and never touch, so each stays an
        // interval of its own. A later element with no time zone, expanding day filter or tick
        // calendar adds its intervals to the merge range of the list, so its merges union them
        // with the earlier elements' before they count them against the cap. The first two cases
        // rely on the default merge threshold and cap.
        Assert.assertEquals(256, configuration.getSqlIntervalIncrementalMergeThreshold());
        Assert.assertEquals(1_024, configuration.getSqlIntervalMaxIntervalsAfterMerge());
        // The second element repeats days 1-11 of January. Its merges union its values into the
        // first element's 960 before they count: counted without that union, its 256th value would
        // make 1,216 intervals.
        assertFirstMinuteOfEachHour(configuration, "[2024-[01..02]-[01..20]T[00..23]:00,2024-01-[01..11]T[00..23]:00]", 2, 20, 24);
        // The second element ends at exactly the cap of 1,024 intervals. Its first value repeats
        // the first element's minute, which its merges absorb before they count, so the count never
        // reaches 1,025.
        assertFirstMinuteOfEachHour(configuration, "[2024-01-01T00:00,2024-[01..04]-[01..16]T[00..15]:00]", 4, 16, 16);
        // A merge threshold of 1 and a cap of 2, the smallest cap above it. The second element
        // repeats the first element's values, and each of its merges unions the value into them
        // before it counts 2 intervals; counted without that union, its first value would make 3.
        final CairoConfiguration mergeThreshold1Configuration = new DefaultCairoConfiguration(".") {
            @Override
            public int getSqlIntervalIncrementalMergeThreshold() {
                return 1;
            }

            @Override
            public int getSqlIntervalMaxIntervalsAfterMerge() {
                return 2;
            }
        };
        assertFirstMinuteOfEachHour(mergeThreshold1Configuration, "[2024-01-01T[00,01]:00,2024-01-01T[00,01]:00]", 1, 1, 2);
    }

    @Test
    public void testDateListErrorDescendingRangeInNested() {
        // '[2025-01-[10..05]]' produces range error
        assertBracketIntervalError("[2025-01-[10..05]]", "Range must be ascending");
    }

    @Test
    public void testDateListErrorDoubleBrackets() {
        // '[[2025-01-01]]' - inner bracket is not valid field expansion (parsed as element "[2025-01-01]")
        assertBracketIntervalError("[[2025-01-01]]", "Expected ',' or end of bracket");
    }

    @Test
    public void testDateListErrorDoubleComma() {
        // '[2025-01-01,,2025-01-02]' produces error
        assertBracketIntervalError("[2025-01-01,,2025-01-02]", "Empty element in date list");
    }

    @Test
    public void testDateListErrorEmpty() {
        // '[]' produces error
        assertBracketIntervalError("[]", "Empty date list");
    }

    @Test
    public void testDateListErrorInvalidDayInNestedExpansion() {
        // '[2025-01-[32..35]]' produces invalid day error
        assertBracketIntervalError("[2025-01-[32..35]]", "Invalid date");
    }

    @Test
    public void testDateListErrorLeadingComma() {
        // '[,2025-01-01]' produces error
        assertBracketIntervalError("[,2025-01-01]", "Empty element in date list");
    }

    @Test
    public void testDateListErrorLowercaseT() {
        // Lowercase 't' is not supported as time separator
        assertBracketIntervalError("[2026-01-01t09:30]", "Invalid date");
    }

    @Test
    public void testDateListNegativeDuration() throws SqlException {
        // '[2025-01-01]T09:30;-5m' - goes 5 minutes backwards from 09:30
        assertBracketInterval(
                "[{lo=2025-01-01T09:25:00.000000Z, hi=2025-01-01T09:29:59.999999Z}]",
                "[2025-01-01]T09:30;-5m"
        );
    }

    @Test
    public void testDateListErrorTrailingComma() {
        // '[2025-01-01,]' produces error
        assertBracketIntervalError("[2025-01-01,]", "Empty element in date list");
    }

    @Test
    public void testDateListErrorUnclosed() {
        // '[2025-01-01' produces error
        assertBracketIntervalError("[2025-01-01", "Unclosed '[' in date list");
    }

    @Test
    public void testDateListErrorWhitespaceOnlyElements() {
        // '[   ,   ]' - whitespace-only elements should error
        assertBracketIntervalError("[   ,   ]", "Empty element in date list");
    }

    @Test
    public void testDateListExpandingFilterElementKeepsElementBase() throws SqlException {
        // The second element repeats January 300 times under its own month-precise day filter,
        // which splits the merged month into its Mondays from the element's start on. Its merges
        // stay above that start: a merge from the start of the list would join the first element's
        // minute and the first 196 months into one January below it, which the filter never
        // splits. The minute lies in the Monday 2024-01-01, so the result is the five Mondays of
        // January as whole days.
        final LongList mondays = new LongList();
        for (LocalDate day = LocalDate.of(2024, 1, 1); day.getMonthValue() == 1; day = day.plusDays(1)) {
            if (day.getDayOfWeek() == DayOfWeek.MONDAY) {
                addLocalDays(mondays, day, day.plusDays(1), ZoneOffset.UTC);
            }
        }
        Assert.assertEquals(2 * 5, mondays.size());
        assertTickExprIntervals(configuration, mondays, "[2024-01-01T00:00:[00..59],2024-[" + repeatedValues("01", 300) + "]#mon]");
    }

    @Test
    public void testDateListImpreciseBareExpression() throws SqlException {
        // Bare imprecise date + time (no brackets): 2024-02T09:30
        // parseTickExpr wraps it as [2024-02]T09:30 and routes to expandDateList
        final TimestampDriver timestampDriver = timestampType.getDriver();
        String bare = "2024-02T09:30";
        String bracketed = "[2024-02]T09:30";
        out.clear();
        parseTickExpr(timestampDriver, bare, 0, bare.length(), 0, out);
        String bareResult = intervalToString(timestampDriver, out).toString();
        out.clear();
        parseTickExpr(timestampDriver, bracketed, 0, bracketed.length(), 0, out);
        String bracketedResult = intervalToString(timestampDriver, out).toString();
        TestUtils.assertEquals(bracketedResult, bareResult);
    }

    @Test
    public void testDateListImpreciseBareInvalidMonth() {
        // Bare expression with invalid month
        assertBracketIntervalError("2024-13T09:30", "Invalid date");
    }

    @Test
    public void testDateListImpreciseBracketExpansion() throws SqlException {
        // Bracket expansion at month level with time: 2024-[01..02]T09:30
        // This goes through expandBracketsRecursive path
        final TimestampDriver timestampDriver = timestampType.getDriver();
        out.clear();
        String interval = "2024-[01..02]T09:30";
        parseTickExpr(timestampDriver, interval, 0, interval.length(), 0, out);
        // January 31 + February 29 (leap year) = 60 days
        Assert.assertEquals(60, out.size() / 2);
    }

    @Test
    public void testDateListImpreciseBracketExpansionTimestamps() throws SqlException {
        // Verify actual timestamp values for bracket expansion, not just count
        final TimestampDriver timestampDriver = timestampType.getDriver();
        out.clear();
        String interval = "2024-[01..02]T09:30";
        parseTickExpr(timestampDriver, interval, 0, interval.length(), 0, out);
        // 31 (Jan) + 29 (Feb, leap year) = 60 days
        Assert.assertEquals(60, out.size() / 2);
        // Verify first interval is Jan 1 09:30
        long firstLo = out.getQuick(0);
        Assert.assertEquals(2024, timestampDriver.getYear(firstLo));
        Assert.assertEquals(1, timestampDriver.getMonthOfYear(firstLo));
        Assert.assertEquals(1, timestampDriver.getDayOfMonth(firstLo));
        // Verify last interval is Feb 29 09:30
        long lastLo = out.getQuick((60 - 1) * 2);
        Assert.assertEquals(2024, timestampDriver.getYear(lastLo));
        Assert.assertEquals(2, timestampDriver.getMonthOfYear(lastLo));
        Assert.assertEquals(29, timestampDriver.getDayOfMonth(lastLo));
    }

    @Test
    public void testDateListImpreciseCenturyNonLeapFebruary() throws SqlException {
        // Century non-leap year: 2100 is not a leap year (divisible by 100 but not 400)
        final TimestampDriver timestampDriver = timestampType.getDriver();
        out.clear();
        String imprecise = "[2100-02]T09:30";
        parseTickExpr(timestampDriver, imprecise, 0, imprecise.length(), 0, out);
        Assert.assertEquals(28, out.size() / 2);
    }

    // ── Month-level equivalence matrix ──────────────────────────────────
    // Each test verifies [2024-02]<suffix> == [2024-02-[01..29]]<suffix>

    @Test
    public void testDateListImpreciseDateCompiledPath() throws SqlException {
        // Imprecise date with time in compiled tick expression (has $today variable)
        assertCompiledTickExpr("[$today, 2024-01]T09:30");
    }

    @Test
    public void testDateListImpreciseDateEquivalence() throws SqlException {
        // Core correctness property: imprecise month produces same output
        // as explicit day range
        final TimestampDriver timestampDriver = timestampType.getDriver();
        String imprecise = "[2024-02]T09:30";
        String explicit = "[2024-02-[01..29]]T09:30";
        out.clear();
        parseTickExpr(timestampDriver, imprecise, 0, imprecise.length(), 0, out);
        String impreciseResult = intervalToString(timestampDriver, out).toString();
        out.clear();
        parseTickExpr(timestampDriver, explicit, 0, explicit.length(), 0, out);
        String explicitResult = intervalToString(timestampDriver, out).toString();
        TestUtils.assertEquals(explicitResult, impreciseResult);
    }

    @Test
    public void testDateListImpreciseDateNamedTimezone() throws SqlException {
        // DST transition test: March 2024 contains spring-forward on March 10
        // in America/New_York. Verify imprecise month handles it correctly
        // by comparing against explicit day range.
        final TimestampDriver timestampDriver = timestampType.getDriver();
        String imprecise = "[2024-03]T09:30@America/New_York#workday";
        String explicit = "[2024-03-[01..31]]T09:30@America/New_York#workday";
        out.clear();
        parseTickExpr(timestampDriver, imprecise, 0, imprecise.length(), 0, out);
        String impreciseResult = intervalToString(timestampDriver, out).toString();
        out.clear();
        parseTickExpr(timestampDriver, explicit, 0, explicit.length(), 0, out);
        String explicitResult = intervalToString(timestampDriver, out).toString();
        TestUtils.assertEquals(explicitResult, impreciseResult);
    }

    @Test
    public void testDateListImpreciseDateWithDurationAndDayFilter() throws SqlException {
        // Duration + day filter on imprecise date without time override.
        // [2024-01]#workday;6h30m should expand each workday from midnight for 6h30m.
        // 2024-01-01 is Monday. First week workdays: Mon 1, Tue 2, Wed 3, Thu 4, Fri 5.
        // Verify equivalence with explicit day range.
        final TimestampDriver timestampDriver = timestampType.getDriver();
        String imprecise = "[2024-01]#workday;6h30m";
        String explicit = "[2024-01-[01..31]]#workday;6h30m";
        out.clear();
        parseTickExpr(timestampDriver, imprecise, 0, imprecise.length(), 0, out);
        String impreciseResult = intervalToString(timestampDriver, out).toString();
        out.clear();
        parseTickExpr(timestampDriver, explicit, 0, explicit.length(), 0, out);
        String explicitResult = intervalToString(timestampDriver, out).toString();
        TestUtils.assertEquals(explicitResult, impreciseResult);
        // 23 workdays in Jan 2024
        Assert.assertEquals(23, out.size() / 2);
    }

    @Test
    public void testDateListImpreciseDateWithMixedPrecision() throws SqlException {
        // Mixed precise and imprecise elements: one day-level, one month-level
        // 2024-01-15 is precise (1 interval), 2024-02 is imprecise (29 intervals)
        final TimestampDriver timestampDriver = timestampType.getDriver();
        out.clear();
        String interval = "[2024-01-15, 2024-02]T09:30";
        parseTickExpr(timestampDriver, interval, 0, interval.length(), 0, out);
        Assert.assertEquals(30, out.size() / 2); // 1 + 29
    }

    @Test
    public void testDateListImpreciseDateWithTimeAndDayFilter() throws SqlException {
        // Month-level date + time + workday filter + duration
        // 2024-01-01 is Monday. Workdays in first week: Mon 1, Tue 2, Wed 3, Thu 4, Fri 5
        assertBracketInterval(
                "[{lo=2024-01-01T09:30:00.000000Z, hi=2024-01-01T15:59:59.999999Z}" +
                        ",{lo=2024-01-02T09:30:00.000000Z, hi=2024-01-02T15:59:59.999999Z}" +
                        ",{lo=2024-01-03T09:30:00.000000Z, hi=2024-01-03T15:59:59.999999Z}" +
                        ",{lo=2024-01-04T09:30:00.000000Z, hi=2024-01-04T15:59:59.999999Z}" +
                        ",{lo=2024-01-05T09:30:00.000000Z, hi=2024-01-05T15:59:59.999999Z}" +
                        ",{lo=2024-01-08T09:30:00.000000Z, hi=2024-01-08T15:59:59.999999Z}" +
                        ",{lo=2024-01-09T09:30:00.000000Z, hi=2024-01-09T15:59:59.999999Z}" +
                        ",{lo=2024-01-10T09:30:00.000000Z, hi=2024-01-10T15:59:59.999999Z}" +
                        ",{lo=2024-01-11T09:30:00.000000Z, hi=2024-01-11T15:59:59.999999Z}" +
                        ",{lo=2024-01-12T09:30:00.000000Z, hi=2024-01-12T15:59:59.999999Z}" +
                        ",{lo=2024-01-15T09:30:00.000000Z, hi=2024-01-15T15:59:59.999999Z}" +
                        ",{lo=2024-01-16T09:30:00.000000Z, hi=2024-01-16T15:59:59.999999Z}" +
                        ",{lo=2024-01-17T09:30:00.000000Z, hi=2024-01-17T15:59:59.999999Z}" +
                        ",{lo=2024-01-18T09:30:00.000000Z, hi=2024-01-18T15:59:59.999999Z}" +
                        ",{lo=2024-01-19T09:30:00.000000Z, hi=2024-01-19T15:59:59.999999Z}" +
                        ",{lo=2024-01-22T09:30:00.000000Z, hi=2024-01-22T15:59:59.999999Z}" +
                        ",{lo=2024-01-23T09:30:00.000000Z, hi=2024-01-23T15:59:59.999999Z}" +
                        ",{lo=2024-01-24T09:30:00.000000Z, hi=2024-01-24T15:59:59.999999Z}" +
                        ",{lo=2024-01-25T09:30:00.000000Z, hi=2024-01-25T15:59:59.999999Z}" +
                        ",{lo=2024-01-26T09:30:00.000000Z, hi=2024-01-26T15:59:59.999999Z}" +
                        ",{lo=2024-01-29T09:30:00.000000Z, hi=2024-01-29T15:59:59.999999Z}" +
                        ",{lo=2024-01-30T09:30:00.000000Z, hi=2024-01-30T15:59:59.999999Z}" +
                        ",{lo=2024-01-31T09:30:00.000000Z, hi=2024-01-31T15:59:59.999999Z}]",
                "[2024-01]T09:30#workday;6h30m"
        );
    }

    @Test
    public void testDateListImpreciseDateWithTimeList() throws SqlException {
        // Time list with imprecise date: [2024-02]T[09:30,14:00]
        // Each day in Feb 2024 at both 09:30 and 14:00 = 29 * 2 = 58 intervals
        final TimestampDriver timestampDriver = timestampType.getDriver();
        String imprecise = "[2024-02]T[09:30,14:00]";
        String explicit = "[2024-02-[01..29]]T[09:30,14:00]";
        out.clear();
        parseTickExpr(timestampDriver, imprecise, 0, imprecise.length(), 0, out);
        String impreciseResult = intervalToString(timestampDriver, out).toString();
        out.clear();
        parseTickExpr(timestampDriver, explicit, 0, explicit.length(), 0, out);
        String explicitResult = intervalToString(timestampDriver, out).toString();
        TestUtils.assertEquals(explicitResult, impreciseResult);
        Assert.assertEquals(58, out.size() / 2);
    }

    @Test
    public void testDateListImpreciseDateWithTimeOverride() throws SqlException {
        // Month-level dates with time override: [2024-02]T09:30
        // February 2024 has 29 days (leap year), all at 09:30.
        assertBracketInterval(
                "[{lo=2024-02-01T09:30:00.000000Z, hi=2024-02-01T09:30:59.999999Z}" +
                        ",{lo=2024-02-02T09:30:00.000000Z, hi=2024-02-02T09:30:59.999999Z}" +
                        ",{lo=2024-02-03T09:30:00.000000Z, hi=2024-02-03T09:30:59.999999Z}" +
                        ",{lo=2024-02-04T09:30:00.000000Z, hi=2024-02-04T09:30:59.999999Z}" +
                        ",{lo=2024-02-05T09:30:00.000000Z, hi=2024-02-05T09:30:59.999999Z}" +
                        ",{lo=2024-02-06T09:30:00.000000Z, hi=2024-02-06T09:30:59.999999Z}" +
                        ",{lo=2024-02-07T09:30:00.000000Z, hi=2024-02-07T09:30:59.999999Z}" +
                        ",{lo=2024-02-08T09:30:00.000000Z, hi=2024-02-08T09:30:59.999999Z}" +
                        ",{lo=2024-02-09T09:30:00.000000Z, hi=2024-02-09T09:30:59.999999Z}" +
                        ",{lo=2024-02-10T09:30:00.000000Z, hi=2024-02-10T09:30:59.999999Z}" +
                        ",{lo=2024-02-11T09:30:00.000000Z, hi=2024-02-11T09:30:59.999999Z}" +
                        ",{lo=2024-02-12T09:30:00.000000Z, hi=2024-02-12T09:30:59.999999Z}" +
                        ",{lo=2024-02-13T09:30:00.000000Z, hi=2024-02-13T09:30:59.999999Z}" +
                        ",{lo=2024-02-14T09:30:00.000000Z, hi=2024-02-14T09:30:59.999999Z}" +
                        ",{lo=2024-02-15T09:30:00.000000Z, hi=2024-02-15T09:30:59.999999Z}" +
                        ",{lo=2024-02-16T09:30:00.000000Z, hi=2024-02-16T09:30:59.999999Z}" +
                        ",{lo=2024-02-17T09:30:00.000000Z, hi=2024-02-17T09:30:59.999999Z}" +
                        ",{lo=2024-02-18T09:30:00.000000Z, hi=2024-02-18T09:30:59.999999Z}" +
                        ",{lo=2024-02-19T09:30:00.000000Z, hi=2024-02-19T09:30:59.999999Z}" +
                        ",{lo=2024-02-20T09:30:00.000000Z, hi=2024-02-20T09:30:59.999999Z}" +
                        ",{lo=2024-02-21T09:30:00.000000Z, hi=2024-02-21T09:30:59.999999Z}" +
                        ",{lo=2024-02-22T09:30:00.000000Z, hi=2024-02-22T09:30:59.999999Z}" +
                        ",{lo=2024-02-23T09:30:00.000000Z, hi=2024-02-23T09:30:59.999999Z}" +
                        ",{lo=2024-02-24T09:30:00.000000Z, hi=2024-02-24T09:30:59.999999Z}" +
                        ",{lo=2024-02-25T09:30:00.000000Z, hi=2024-02-25T09:30:59.999999Z}" +
                        ",{lo=2024-02-26T09:30:00.000000Z, hi=2024-02-26T09:30:59.999999Z}" +
                        ",{lo=2024-02-27T09:30:00.000000Z, hi=2024-02-27T09:30:59.999999Z}" +
                        ",{lo=2024-02-28T09:30:00.000000Z, hi=2024-02-28T09:30:59.999999Z}" +
                        ",{lo=2024-02-29T09:30:00.000000Z, hi=2024-02-29T09:30:59.999999Z}]",
                "[2024-02]T09:30"
        );
    }

    @Test
    public void testDateListImpreciseDateWithTimezone() throws SqlException {
        // Imprecise month with time + timezone: verify against explicit day range
        // 09:30 in UTC+5 = 04:30 UTC
        final TimestampDriver timestampDriver = timestampType.getDriver();
        String precise = "[2024-01-[01..03]]T09:30@+05:00";
        out.clear();
        parseTickExpr(timestampDriver, precise, 0, precise.length(), 0, out);
        String expected = intervalToString(timestampDriver, out).toString();

        // Imprecise month with day filter should produce same first 3 days
        // 2024-01-01 is Monday, 02 Tue, 03 Wed
        String imprecise = "[2024-01]T09:30@+05:00#Mon,Tue,Wed";
        out.clear();
        parseTickExpr(timestampDriver, imprecise, 0, imprecise.length(), 0, out);
        // Take only the first 3 intervals (6 longs) — both expressions produce them
        LongList first3 = new LongList();
        for (int i = 0; i < Math.min(6, out.size()); i++) {
            first3.add(out.getQuick(i));
        }
        TestUtils.assertEquals(expected, intervalToString(timestampDriver, first3));
    }

    @Test
    public void testDateListImpreciseDecember() throws SqlException {
        // December (month 12) boundary: [2024-12]T09:30 should produce 31 intervals
        final TimestampDriver timestampDriver = timestampType.getDriver();
        String imprecise = "[2024-12]T09:30";
        String explicit = "[2024-12-[01..31]]T09:30";
        out.clear();
        parseTickExpr(timestampDriver, imprecise, 0, imprecise.length(), 0, out);
        String impreciseResult = intervalToString(timestampDriver, out).toString();
        out.clear();
        parseTickExpr(timestampDriver, explicit, 0, explicit.length(), 0, out);
        String explicitResult = intervalToString(timestampDriver, out).toString();
        TestUtils.assertEquals(explicitResult, impreciseResult);
        Assert.assertEquals(31, out.size() / 2);
    }

    @Test
    public void testDateListImpreciseInvalidMonth13() {
        // Month 13 should produce an error, not ArrayIndexOutOfBoundsException
        assertBracketIntervalError("[2024-13]T09:30", "Invalid date");
    }

    @Test
    public void testDateListImpreciseInvalidMonthZero() {
        // Month 0 should produce an error, not ArrayIndexOutOfBoundsException
        assertBracketIntervalError("[2024-00]T09:30", "Invalid date");
    }

    @Test
    public void testDateListImpreciseMixedPrecisionTimestamps() throws SqlException {
        // Verify the precise element appears alongside imprecise expansion
        final TimestampDriver timestampDriver = timestampType.getDriver();
        out.clear();
        String interval = "[2024-01-15, 2024-02]T09:30";
        parseTickExpr(timestampDriver, interval, 0, interval.length(), 0, out);
        Assert.assertEquals(30, out.size() / 2); // 1 + 29
        // First interval: Jan 15
        long firstLo = out.getQuick(0);
        Assert.assertEquals(1, timestampDriver.getMonthOfYear(firstLo));
        Assert.assertEquals(15, timestampDriver.getDayOfMonth(firstLo));
        // Second interval: Feb 1
        long secondLo = out.getQuick(2);
        Assert.assertEquals(2, timestampDriver.getMonthOfYear(secondLo));
        Assert.assertEquals(1, timestampDriver.getDayOfMonth(secondLo));
    }

    @Test
    public void testDateListImpreciseMonthWithDurationOnly() throws SqlException {
        // Month-level date + duration (no time, no day filter): [2024-02];6h30m
        // Should expand to 29 days, each from midnight for 6h30m.
        final TimestampDriver timestampDriver = timestampType.getDriver();
        String imprecise = "[2024-02];6h30m";
        String explicit = "[2024-02-[01..29]];6h30m";
        out.clear();
        parseTickExpr(timestampDriver, imprecise, 0, imprecise.length(), 0, out);
        String impreciseResult = intervalToString(timestampDriver, out).toString();
        out.clear();
        parseTickExpr(timestampDriver, explicit, 0, explicit.length(), 0, out);
        String explicitResult = intervalToString(timestampDriver, out).toString();
        TestUtils.assertEquals(explicitResult, impreciseResult);
        Assert.assertEquals(29, out.size() / 2);
    }

    // ── Year-level equivalence matrix ────────────────────────────────────
    // Each test verifies [2024]<suffix> == [2024-[01..12]]<suffix>

    @Test
    public void testDateListImpreciseMultipleMonths() throws SqlException {
        // Two month-level elements with time override
        final TimestampDriver timestampDriver = timestampType.getDriver();
        out.clear();
        String interval = "[2024-01, 2024-02]T09:30";
        parseTickExpr(timestampDriver, interval, 0, interval.length(), 0, out);
        // January has 31 days, February 2024 has 29 days (leap year) = 60 total
        Assert.assertEquals(60, out.size() / 2);
    }

    @Test
    public void testDateListImpreciseNonLeapFebruary() throws SqlException {
        // Non-leap year February: [2023-02]T09:30 should produce 28 intervals
        final TimestampDriver timestampDriver = timestampType.getDriver();
        String imprecise = "[2023-02]T09:30";
        String explicit = "[2023-02-[01..28]]T09:30";
        out.clear();
        parseTickExpr(timestampDriver, imprecise, 0, imprecise.length(), 0, out);
        String impreciseResult = intervalToString(timestampDriver, out).toString();
        out.clear();
        parseTickExpr(timestampDriver, explicit, 0, explicit.length(), 0, out);
        String explicitResult = intervalToString(timestampDriver, out).toString();
        TestUtils.assertEquals(explicitResult, impreciseResult);
        Assert.assertEquals(28, out.size() / 2);
    }

    @Test
    public void testDateListImpreciseYearLevel() throws SqlException {
        // Year-level date with time: [2024]T09:30
        // Expands to all 366 days of 2024 (leap year) at 09:30
        final TimestampDriver timestampDriver = timestampType.getDriver();
        String imprecise = "[2024]T09:30";
        out.clear();
        parseTickExpr(timestampDriver, imprecise, 0, imprecise.length(), 0, out);
        Assert.assertEquals(366, out.size() / 2);
    }

    @Test
    public void testDateListImpreciseYearLevelNonLeap() throws SqlException {
        // Non-leap year: [2023]T09:30 should expand to 365 days
        final TimestampDriver timestampDriver = timestampType.getDriver();
        out.clear();
        String imprecise = "[2023]T09:30";
        parseTickExpr(timestampDriver, imprecise, 0, imprecise.length(), 0, out);
        Assert.assertEquals(365, out.size() / 2);
    }

    @Test
    public void testDateListImpreciseYearLevelWithDayFilter() throws SqlException {
        // Year-level date + day filter + duration: [2024]#workday;6h30m
        // Should expand to all workdays in 2024, each from midnight for 6h30m.
        // Verify equivalence with month-by-month explicit expansion.
        final TimestampDriver timestampDriver = timestampType.getDriver();
        String imprecise = "[2024]#workday;6h30m";
        String explicit = "[2024-[01..12]]#workday;6h30m";
        out.clear();
        parseTickExpr(timestampDriver, imprecise, 0, imprecise.length(), 0, out);
        String impreciseResult = intervalToString(timestampDriver, out).toString();
        out.clear();
        parseTickExpr(timestampDriver, explicit, 0, explicit.length(), 0, out);
        String explicitResult = intervalToString(timestampDriver, out).toString();
        TestUtils.assertEquals(explicitResult, impreciseResult);
        // 2024 has 262 workdays
        Assert.assertEquals(262, out.size() / 2);
    }

    @Test
    public void testDateListMatchesUnmergedOracle() throws SqlException {
        // An element with a time zone, an expanding day filter or a tick calendar merges its own
        // intervals only, before its day filter and time zone, so the intervals of the earlier
        // elements keep their place and their conversion. A time list value with a zone merges its
        // own intervals before its zone. Other elements and values add their intervals to the
        // enclosing merge range.
        final String[] intervals = {
                "[2024-01-[01..31],2024-[02..12]-[01..28]]@+05:00",
                "[2024-01-[01..31]@+05:00,2024-[02..12]-[01..28]@+05:00]",
                "[2024-01-[01..31]@Europe/London,2024-[02..12]-[01..28]@America/New_York]",
                "[2024-01-[01..31],2024-[02..12]-[01..28]]T09:00@+05:00;1h",
                "[2024-01-[01..31],2024-[02..12]-[01..28]]@+05:00#mon",
                // Only the later element is filtered. Its merges must leave out January's converted
                // days, which a union would join into one block, moving the Mondays below the
                // element's start, where its time zone never reaches them.
                "[2024-01-[01..31]@+05:00,2024-[02..12]-[01..28]@+05:00#mon]",
                "[2024-01-[01..31],2024-[02..12]-[01..28]#mon]@+05:00",
                "[2024-01-[01..31]@Europe/London,2024-[02..12]-[01..28]@America/New_York#mon]",
                // the zone moves the first element's last hours to 2024-01-04T00:45Z-02:45Z, among the
                // leaves of the second element, which a union would take below the element's start
                "[2024-01-[01..03]T[00..23]:15,2024-01-[04..20]T[00..23]:40]@-03:30#workday",
                "[2023-[01..12]-[01..28]T09:00,2023-[03..04]#mon]",
                "[[2019..2024]-[01..12]-[01..28]#mon,2024-[09..10]#workday]",
                "[[2019..2024]-[01..12]-[01..28],2024-[09..10]]#mon",
                // the day filter drops every leaf of the last element: 2024-01-01 is a Monday
                "[2024-01-05,2024-01-07,2024-01-01T[00..23]:[00..59]#sun]",
                // each time list value converts its own 600 intervals
                "2024-01-01T[20:00@-05:00,10:00@+05:00]:[00..59].[0..9]",
                "2024-01-01T[20:00,10:00]:[00..59].[0..9]@-05:00",
                // the same under a day filter; both values stay on Monday 2024-01-01 in UTC (#8113)
                "2024-01-01T[10:00@-05:00,12:00@+05:00]:[00..59].[0..9]#mon",
                "[$today,2024-[01..12]-[01..28]@+05:00]",
                "[2024-[01..12]-[01..28]@+05:00,$today..$today+300d]",
                // each day of a date variable range merges its own 1,440 minutes; $today is the
                // Monday 2024-01-08
                "[$today..$today+1d]T[00..23]:[00..59]@+05:00#workday",
                // The raw 10:00 of the zone-less value sorts before the converted 2024-01-02T01:00Z
                // of the zoned values: their merges run after their zones.
                "2024-01-01T[20:00@-05:00,10:00,20:00@-05:00]:[00..59].[0..9]0",
                "2024-01-01T[09:00@+05:00,04:00]:[00..59].[0..9]0#mon",
                "[2024-01-01,2024-01-01T[09:00@-05:00,10:00]:[00..59].[0..9]0]",
                // nested time lists with zones on one level
                "2024-01-01T[09:@+05:00,10:@-03:30][00:,01:][00..59].[0..9]0",
                "2024-01-01T[09:,10:][00:@+05:00,01:][00..59].[0..9]0",
                "2024-01-01T[09:,10:][" + evenSeconds(120) + "]",
                "2024-01-01T[09:@+05:00,10:@-03:30][" + evenSeconds(120) + "]",
                "2024-01-01T[09@+05:00,10@+05:00][" + numberedValues(":", 16, "") + "][" + numberedValues(":", 16, "") + "]#mon",
                // a time list call per day of an imprecise month element
                "[2024-01T[09:00@+05:00,10:00]:[00..59]]",
                // a later element over the cap that the first element covers
                "[2024-01-01,2024-01-01T[09..10]:00:[00..59].[0..9]0]",
                "[$today..$today+1d]T[09:00@+05:00,10:00]:[00..59].[0..9]0",
                // the time zone of a later element or day keeps its intervals out of the merges of the
                // list, which hold the first element's converted minute
                "[2024-01-08T00:00:[00..59],$today..$today]T09:[00..05]:[00..59]@+05:00",
                "[2024-01-01T00:00:[00..59],2024-01-01T09:[00..05]:[00..59]]@+05:00",
                // One zoned time list value per day under a day filter. Each value finishes the
                // filter of the earlier value's leaves before it starts, so the dropped weekend
                // leaves never move a Monday's leaves below its start, where its zone would miss
                // them.
                "2024-[01..12]-[01..28]T[09:00@+05:00]:[00..50].[0..9]#workday",
                "20[20..23]-[01..12]-[01..28]T[09:00@+05:00]:[00..50].[0..9]#workday",
                "2024-01-[07..08]T[09@+05:00,09@+05:00]:[00..23]:[00..15]#mon"
        };
        // At the server default and at low thresholds, at which the earlier elements and time list
        // values merge as well, and a union reorders the intervals it takes in.
        for (int mergeThreshold : new int[]{256, 4, 2}) {
            assertMatchesUnmergedOracle(mergeThreshold, intervals);
        }

        // The oracle runs the same parser, so independent results pin the time list, the date
        // variable range and the named zones. 10:00 at -05:00 is 15:00Z and 12:00 at +05:00 is 07:00Z.
        assertTickExprAtMonday(
                configuration,
                "[{lo=2024-01-01T07:00:00.000000Z, hi=2024-01-01T07:00:59.999999Z},{lo=2024-01-01T15:00:00.000000Z, hi=2024-01-01T15:00:59.999999Z}]",
                "2024-01-01T[10:00@-05:00,12:00@+05:00]:[00..59].[0..9]#mon"
        );
        // the Monday 2024-01-08 and the Tuesday 2024-01-09 at +05:00
        assertTickExprAtMonday(
                configuration,
                "[{lo=2024-01-07T19:00:00.000000Z, hi=2024-01-09T18:59:59.999999Z}]",
                "[$today..$today+1d]T[00..23]:[00..59]@+05:00#workday"
        );
        // January at Europe/London as one block, then the 44 Mondays among days 1-28 of February to
        // December at America/New_York, whose offset is -04:00 from 2024-03-10 to 2024-11-03 and
        // -05:00 otherwise: 2024-02-05 is [05:00Z, 2024-02-06T04:59:59.999999Z]
        final ZoneId london = ZoneId.of("Europe/London");
        final ZoneId newYork = ZoneId.of("America/New_York");
        final LongList expected = new LongList();
        addLocalDays(expected, LocalDate.of(2024, 1, 1), LocalDate.of(2024, 2, 1), london);
        for (LocalDate day = LocalDate.of(2024, 2, 1); day.getYear() == 2024; day = day.plusDays(1)) {
            if (day.getDayOfMonth() <= 28 && day.getDayOfWeek() == DayOfWeek.MONDAY) {
                addLocalDays(expected, day, day.plusDays(1), newYork);
            }
        }
        Assert.assertEquals(2 * 45, expected.size());
        final TimestampDriver timestampDriver = timestampType.getDriver();
        out.clear();
        parseTickExprAtMonday(configuration, out, "[2024-01-[01..31]@Europe/London,2024-[02..12]-[01..28]@America/New_York#mon]");
        TestUtils.assertEquals(intervalToString(timestampDriver, expected).toString(), intervalToString(timestampDriver, out));
        // The 600 slots of 20:00 at -05:00 are 2024-01-02T01:00Z, after the 600 raw slots of 10:00.
        // Together they exceed the default cap (1,024), which this check lifts.
        final CairoConfiguration uncappedConfiguration = new DefaultCairoConfiguration(".") {
            @Override
            public int getSqlIntervalMaxIntervalsAfterMerge() {
                return 10_000_000;
            }
        };
        final LongList slots = new LongList();
        addMinuteSlots(slots, LocalDateTime.of(2024, 1, 1, 10, 0), ZoneOffset.UTC, 10);
        addMinuteSlots(slots, LocalDateTime.of(2024, 1, 1, 20, 0), ZoneOffset.ofHours(-5), 10);
        assertTickExprIntervals(uncappedConfiguration, slots, "2024-01-01T[20:00@-05:00,10:00,20:00@-05:00]:[00..59].[0..9]0");
    }

    @Test
    public void testDateListMixedElementFiltersGrowthBetweenMerges() throws SqlException {
        // Filtered 31-day elements around an unfiltered element, whose 336 leaves the merges of the
        // list union with the Mondays of the elements before it
        final StringBuilder interval = new StringBuilder("[");
        for (int k = 0; k < 200; k++) {
            interval.append("2024-01-[01..31]#mon,");
        }
        interval.append("2023-[01..12]-[01..28]");
        for (int k = 0; k < 200; k++) {
            interval.append(",2024-01-[01..31]#mon");
        }
        interval.append(']');
        final PeakLongList peakOut = new PeakLongList();
        parseTickExprAtMonday(configuration, peakOut, interval.toString());
        // the days 2023-MM-01..28 and the Mondays of January 2024, one interval per run of
        // consecutive days: 2023-02-28 touches 2023-03-01, so 2023 holds 11 runs
        final TreeSet<LocalDate> days = new TreeSet<>();
        for (int month = 1; month <= 12; month++) {
            for (int day = 1; day <= 28; day++) {
                days.add(LocalDate.of(2023, month, day));
            }
        }
        for (LocalDate day = LocalDate.of(2024, 1, 1); day.getMonthValue() == 1; day = day.plusDays(1)) {
            if (day.getDayOfWeek() == DayOfWeek.MONDAY) {
                days.add(day);
            }
        }
        int runCount = 0;
        LocalDate prev = null;
        for (LocalDate day : days) {
            if (prev == null || !day.equals(prev.plusDays(1))) {
                runCount++;
            }
            prev = day;
        }
        Assert.assertEquals(16, runCount);
        Assert.assertEquals(runCount, peakOut.size() / 2);
        final TimestampDriver timestampDriver = timestampType.getDriver();
        for (int i = 0, n = peakOut.size(); i < n; i += 2) {
            final long lo = peakOut.getQuick(i);
            Assert.assertEquals(timestampDriver.startOfDay(lo, 0), lo);
            Assert.assertTrue(days.contains(toLocalDate(timestampDriver, lo)));
        }
        Assert.assertTrue("peak longs " + peakOut.peak, peakOut.peak <= 2 * (1_024 + 2 * 256));
    }

    @Test
    public void testDateListMixedPerElementAndGlobalDayFilter() throws SqlException {
        // Mix of per-element and global day filter
        // [2024-01-01#Mon,2024-01-06]#Sat
        // 2024-01-01 uses #Mon (is Monday, passes), 2024-01-06 uses global #Sat (is Saturday, passes)
        assertBracketInterval(
                "[{lo=2024-01-01T00:00:00.000000Z, hi=2024-01-01T23:59:59.999999Z},{lo=2024-01-06T00:00:00.000000Z, hi=2024-01-06T23:59:59.999999Z}]",
                "[2024-01-01#Mon,2024-01-06]#Sat"
        );
    }

    @Test
    public void testDateListOverlappingIntervals() throws SqlException {
        // Duplicate dates should be merged into one interval (exercises line 1350)
        assertBracketInterval(
                "[{lo=2025-01-05T00:00:00.000000Z, hi=2025-01-05T23:59:59.999999Z}]",
                "[2025-01-05,2025-01-05,2025-01-05]"
        );
    }

    @Test
    public void testDateListPerElementDayFilter() throws SqlException {
        // Per-element day filter inside date list IS supported
        // [2024-01-01#Mon,2024-01-06#Sat] - each element has its own day filter
        // 2024-01-01 is Monday (passes #Mon), 2024-01-06 is Saturday (passes #Sat)
        assertBracketInterval(
                "[{lo=2024-01-01T00:00:00.000000Z, hi=2024-01-01T23:59:59.999999Z},{lo=2024-01-06T00:00:00.000000Z, hi=2024-01-06T23:59:59.999999Z}]",
                "[2024-01-01#Mon,2024-01-06#Sat]"
        );
    }

    // ── Bare expression equivalence ──────────────────────────────────────
    // Bare (no brackets) imprecise dates vs bracketed equivalents

    @Test
    public void testDateListPerElementDayFilterFiltersOut() throws SqlException {
        // Per-element day filter that filters out the element
        // 2024-01-01 is Monday (fails #Tue), 2024-01-06 is Saturday (passes #Sat)
        assertBracketInterval(
                "[{lo=2024-01-06T00:00:00.000000Z, hi=2024-01-06T23:59:59.999999Z}]",
                "[2024-01-01#Tue,2024-01-06#Sat]"
        );
    }

    @Test
    public void testDateListPerElementDayFilterWithDuration() throws SqlException {
        // Per-element day filter with duration suffix
        // [2024-01-01#Mon,2024-01-06#Sat];1h
        assertBracketInterval(
                "[{lo=2024-01-01T00:00:00.000000Z, hi=2024-01-01T00:59:59.999999Z},{lo=2024-01-06T00:00:00.000000Z, hi=2024-01-06T00:59:59.999999Z}]",
                "[2024-01-01#Mon,2024-01-06#Sat];1h"
        );
    }

    @Test
    public void testDateListPerElementDayFilterWithGlobalTimezone() throws SqlException {
        // Per-element day filter with global timezone suffix
        // [2024-01-01#Mon,2024-01-06]@+05:00 - global timezone applies to all
        // 2024-01-01 is Monday (passes #Mon), 2024-01-06 has no per-element filter (passes)
        // 2024-01-01 00:00 +05:00 = 2023-12-31 19:00 UTC
        // 2024-01-06 00:00 +05:00 = 2024-01-05 19:00 UTC
        assertBracketInterval(
                "[{lo=2023-12-31T19:00:00.000000Z, hi=2024-01-01T18:59:59.999999Z},{lo=2024-01-05T19:00:00.000000Z, hi=2024-01-06T18:59:59.999999Z}]",
                "[2024-01-01#Mon,2024-01-06]@+05:00"
        );
    }

    // ── Bracket range equivalence ────────────────────────────────────────
    // Bracket ranges at month level vs explicit day ranges

    @Test
    public void testDateListPerElementDayFilterWithTimeSuffix() throws SqlException {
        // Per-element day filter with time suffix
        // [2024-01-01#Mon,2024-01-06#Sat]T09:00
        assertBracketInterval(
                "[{lo=2024-01-01T09:00:00.000000Z, hi=2024-01-01T09:00:59.999999Z},{lo=2024-01-06T09:00:00.000000Z, hi=2024-01-06T09:00:59.999999Z}]",
                "[2024-01-01#Mon,2024-01-06#Sat]T09:00"
        );
    }

    @Test
    public void testDateListPerElementDayFilterWithTimezone() throws SqlException {
        // Per-element day filter with timezone
        // 2024-01-01@+05:00#Mon - Monday in +05:00 timezone
        // 2024-01-01 00:00 +05:00 = 2023-12-31 19:00 UTC
        assertBracketInterval(
                "[{lo=2023-12-31T19:00:00.000000Z, hi=2024-01-01T18:59:59.999999Z}]",
                "[2024-01-01@+05:00#Mon]"
        );
    }

    @Test
    public void testDateListSingleDate() throws SqlException {
        // '[2025-12-31]' produces 1 full-day interval
        assertBracketInterval(
                "[{lo=2025-12-31T00:00:00.000000Z, hi=2025-12-31T23:59:59.999999Z}]",
                "[2025-12-31]"
        );
    }

    @Test
    public void testDateListThreeDates() throws SqlException {
        // '[2025-01-01,2025-01-05,2025-01-13]' produces 3 full-day intervals
        assertBracketInterval(
                "[{lo=2025-01-01T00:00:00.000000Z, hi=2025-01-01T23:59:59.999999Z},{lo=2025-01-05T00:00:00.000000Z, hi=2025-01-05T23:59:59.999999Z},{lo=2025-01-13T00:00:00.000000Z, hi=2025-01-13T23:59:59.999999Z}]",
                "[2025-01-01,2025-01-05,2025-01-13]"
        );
    }

    @Test
    public void testDateListTimeListOnlyElementsOverMaxIntervalsFailsEarly() {
        // 700 Mondays from 2020-01-06 with two one-minute times each: 1,400 intervals that never
        // touch. These elements have only a time list bracket, so only the check after each
        // filtered element counts them, and too-many-intervals wins over a later unparsable element.
        final StringBuilder elements = new StringBuilder("[");
        LocalDate day = LocalDate.of(2020, 1, 6);
        for (int k = 0; k < 700; k++, day = day.plusDays(7)) {
            elements.append(k == 0 ? "" : ",").append(day).append("T[09:00,10:00]");
        }
        assertTickExprErrorAtMonday(elements + ",2024-13-45]#mon", "Bracket expansion produces too many intervals (max 1024)");
        assertTickExprErrorAtMonday(elements + "]#mon", "Bracket expansion produces too many intervals (max 1024)");
    }

    @Test
    public void testDateListTwoDates() throws SqlException {
        // '[2025-01-01,2025-01-05]' produces 2 full-day intervals
        assertBracketInterval(
                "[{lo=2025-01-01T00:00:00.000000Z, hi=2025-01-01T23:59:59.999999Z},{lo=2025-01-05T00:00:00.000000Z, hi=2025-01-05T23:59:59.999999Z}]",
                "[2025-01-01,2025-01-05]"
        );
    }

    @Test
    public void testDateListUnsortedDates() throws SqlException {
        // Dates out of chronological order - tests insertion sort (exercises line 1329)
        // '[2025-01-20,2025-01-05,2025-01-15]' should be sorted to 05, 15, 20
        assertBracketInterval(
                "[{lo=2025-01-05T00:00:00.000000Z, hi=2025-01-05T23:59:59.999999Z},{lo=2025-01-15T00:00:00.000000Z, hi=2025-01-15T23:59:59.999999Z},{lo=2025-01-20T00:00:00.000000Z, hi=2025-01-20T23:59:59.999999Z}]",
                "[2025-01-20,2025-01-05,2025-01-15]"
        );
    }

    @Test
    public void testDateListUnsortedDatesWithDayFilter() throws SqlException {
        // Dates out of chronological order WITH day filter
        // [2024-01-15,2024-01-01,2024-01-08]#Mon - all three are Mondays, out of order
        // Should be sorted to: Jan 1, Jan 8, Jan 15
        assertBracketInterval(
                "[{lo=2024-01-01T00:00:00.000000Z, hi=2024-01-01T23:59:59.999999Z}," +
                        "{lo=2024-01-08T00:00:00.000000Z, hi=2024-01-08T23:59:59.999999Z}," +
                        "{lo=2024-01-15T00:00:00.000000Z, hi=2024-01-15T23:59:59.999999Z}]",
                "[2024-01-15,2024-01-01,2024-01-08]#Mon"
        );
    }

    @Test
    public void testDateListWithBracketExpansionAndDayFilter() throws SqlException {
        // Date list with bracket expansion AND day filter
        // [2024-01-[01..07],2024-01-15]#Mon should expand Jan 1-7 and Jan 15, then filter to Mondays only
        // 2024-01-01 is Monday, 2024-01-08 is Monday, 2024-01-15 is Monday
        // From 01..07, only 01 is Monday. 15 is also Monday.
        assertBracketInterval(
                "[{lo=2024-01-01T00:00:00.000000Z, hi=2024-01-01T23:59:59.999999Z},{lo=2024-01-15T00:00:00.000000Z, hi=2024-01-15T23:59:59.999999Z}]",
                "[2024-01-[01..07],2024-01-15]#Mon"
        );
    }

    @Test
    public void testDateListWithBracketExpansionAndWorkdayFilter() throws SqlException {
        // Date list with bracket expansion AND workday filter
        // [2024-01-[01..07]]#workday - Jan 1-7, 2024: Mon(1), Tue(2), Wed(3), Thu(4), Fri(5), Sat(6), Sun(7)
        // Workdays are Mon-Fri, so 1-5 should pass
        assertBracketInterval(
                "[{lo=2024-01-01T00:00:00.000000Z, hi=2024-01-05T23:59:59.999999Z}]",
                "[2024-01-[01..07]]#workday"
        );
    }

    @Test
    public void testDateListWithBracketExpansionList() throws SqlException {
        // Date list with bracket expansion using list syntax inside an element
        // [2024-01-[10,15,20],2024-02-01] should expand to Jan 10, 15, 20 and Feb 1
        assertBracketInterval(
                "[{lo=2024-01-10T00:00:00.000000Z, hi=2024-01-10T23:59:59.999999Z},{lo=2024-01-15T00:00:00.000000Z, hi=2024-01-15T23:59:59.999999Z},{lo=2024-01-20T00:00:00.000000Z, hi=2024-01-20T23:59:59.999999Z},{lo=2024-02-01T00:00:00.000000Z, hi=2024-02-01T23:59:59.999999Z}]",
                "[2024-01-[10,15,20],2024-02-01]"
        );
    }

    @Test
    public void testDateListWithBracketExpansionRange() throws SqlException {
        // Date list with bracket expansion using range syntax inside an element
        // [2024-01-[01..03],2024-01-08] should expand to Jan 1, 2, 3, and Jan 8
        assertBracketInterval(
                "[{lo=2024-01-01T00:00:00.000000Z, hi=2024-01-03T23:59:59.999999Z},{lo=2024-01-08T00:00:00.000000Z, hi=2024-01-08T23:59:59.999999Z}]",
                "[2024-01-[01..03],2024-01-08]"
        );
    }

    @Test
    public void testDateListWithDenseTimeExpansionAndDayFilter() throws SqlException {
        // T[hh..hh]:[00..59] emits one interval per minute. Each time the date list holds 256
        // intervals, the default incremental merge threshold, the bracket expansion merges them,
        // which can happen part way through a date.
        // 2024-01-12 is a Friday and 2024-01-13 a Saturday, so #workday keeps 2024-01-12 only.
        // 2 dates x 120 minutes stay below the threshold
        assertBracketInterval(
                "[{lo=2024-01-12T00:00:00.000000Z, hi=2024-01-12T01:59:59.999999Z}]",
                "[2024-01-12,2024-01-13]T[00..01]:[00..59]#workday"
        );
        // 2 dates x 180 minutes cross the threshold while the second date expands
        assertBracketInterval(
                "[{lo=2024-01-12T00:00:00.000000Z, hi=2024-01-12T02:59:59.999999Z}]",
                "[2024-01-12,2024-01-13]T[00..02]:[00..59]#workday"
        );
        // every minute of both dates, with the global and with a per-element day filter
        assertBracketInterval(
                "[{lo=2024-01-12T00:00:00.000000Z, hi=2024-01-12T23:59:59.999999Z}]",
                "[2024-01-12,2024-01-13]T[00..23]:[00..59]#workday"
        );
        assertBracketInterval(
                "[{lo=2024-01-12T00:00:00.000000Z, hi=2024-01-12T23:59:59.999999Z}]",
                "[2024-01-12#workday,2024-01-13#workday]T[00..23]:[00..59]"
        );
    }

    @Test
    public void testDateListWithDenseTimeExpansionAndTimezone() throws SqlException {
        // T[hh..hh]:[00..59] emits one interval per minute. Each time the date list holds 256
        // intervals, the default incremental merge threshold, the bracket expansion merges them,
        // which can happen part way through a date.
        // +02:00 shifts every minute of every date two hours back.
        // 2 dates x 120 minutes stay below the threshold
        assertBracketInterval(
                "[{lo=2024-01-10T22:00:00.000000Z, hi=2024-01-10T23:59:59.999999Z},{lo=2024-01-11T22:00:00.000000Z, hi=2024-01-11T23:59:59.999999Z}]",
                "[2024-01-11,2024-01-12]T[00..01]:[00..59]@+02:00"
        );
        // 2 dates x 180 minutes cross the threshold while the second date expands
        assertBracketInterval(
                "[{lo=2024-01-10T22:00:00.000000Z, hi=2024-01-11T00:59:59.999999Z},{lo=2024-01-11T22:00:00.000000Z, hi=2024-01-12T00:59:59.999999Z}]",
                "[2024-01-11,2024-01-12]T[00..02]:[00..59]@+02:00"
        );
        // every minute of both dates, with the global and with a per-element timezone
        assertBracketInterval(
                "[{lo=2024-01-10T22:00:00.000000Z, hi=2024-01-12T21:59:59.999999Z}]",
                "[2024-01-11,2024-01-12]T[00..23]:[00..59]@+02:00"
        );
        assertBracketInterval(
                "[{lo=2024-01-10T22:00:00.000000Z, hi=2024-01-12T21:59:59.999999Z}]",
                "[2024-01-11@+02:00,2024-01-12@+02:00]T[00..23]:[00..59]"
        );
    }

    @Test
    public void testDateListWithDenseTimeSuffixAndDayFilter() throws SqlException {
        // T[00..23]:[00..59] expands each element to 1440 minutes, so the second element
        // reaches the incremental merge threshold (256 intervals). The day filter must still
        // see only the intervals of its own element: 2024-01-06 is a Saturday.
        assertBracketInterval(
                "[{lo=2024-01-05T00:00:00.000000Z, hi=2024-01-05T23:59:59.999999Z}]",
                "[2024-01-05, 2024-01-06]T[00..23]:[00..59]#workday"
        );
    }

    @Test
    public void testDateListWithDenseTimeSuffixAndDstOverlap() throws SqlException {
        // London falls back from BST to GMT at 02:00 on 2024-10-27, so each local minute from
        // 01:00 to 01:59 happens twice, and covers both of its occurrences. The 1440 minutes
        // cover the whole local day, 25 hours, the same as the day itself. They reach the
        // incremental merge threshold (256 intervals), and the result must not depend on where
        // the merges happen: listing the hours in another order gives the same intervals.
        final String expected = "[{lo=2024-10-26T23:00:00.000000Z, hi=2024-10-27T23:59:59.999999Z}]";
        assertBracketInterval(expected, "2024-10-27@Europe/London");
        assertBracketInterval(expected, "[2024-10-27]T[00..23]:[00..59]@Europe/London");
        assertBracketInterval(expected, "[2024-10-27]T[02..23,00,01]:[00..59]@Europe/London");
        assertBracketInterval(expected, "2024-10-27T[00..23]:[00..59]@Europe/London");
        assertBracketInterval(expected, "2024-10-27T[02..23,00,01]:[00..59]@Europe/London");
    }

    @Test
    public void testDateListWithDenseTimeSuffixAndTimezone() throws SqlException {
        // The second element reaches the incremental merge threshold (256 intervals). Its
        // timezone must still convert exactly its own intervals.
        assertBracketInterval(
                "[{lo=2023-12-31T22:00:00.000000Z, hi=2024-01-01T21:59:59.999999Z},{lo=2024-01-05T00:00:00.000000Z, hi=2024-01-05T23:59:59.999999Z}]",
                "[2024-01-05, 2024-01-01@+02:00]T[00..23]:[00..59]"
        );
        // a global timezone converts each element separately
        assertBracketInterval(
                "[{lo=2023-12-31T22:00:00.000000Z, hi=2024-01-01T21:59:59.999999Z},{lo=2024-01-04T22:00:00.000000Z, hi=2024-01-05T21:59:59.999999Z}]",
                "[2024-01-05, 2024-01-01]T[00..23]:[00..59]@+02:00"
        );
    }

    @Test
    public void testDateListWithDenseTimeSuffixBoundsMemory() throws SqlException {
        // 100 elements of 240 adjacent minutes each: 24,000 intervals before merging, while no
        // single element reaches the incremental merge threshold (256 intervals). The incremental
        // merge must still bound the list across elements.
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final long firstDay = timestampDriver.parseFloorLiteral("2024-01-01T00:00:00.000000Z");
        final StringSink expression = new StringSink();
        expression.put('[');
        for (int i = 0; i < 100; i++) {
            if (i > 0) {
                expression.put(',');
            }
            IntervalUtils.appendDate(timestampDriver, timestampDriver.addDays(firstDay, i), expression);
        }
        expression.put("]T[09..12]:[00..59]");

        out.clear();
        parseTickExpr(timestampDriver, expression, 0, expression.length(), 0, out);
        Assert.assertEquals(100, out.size() / 2);
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2024-01-01T09:00:00.000000Z"), out.getQuick(0));
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2024-04-09T13:00:00.000000Z") - 1, out.getQuick(out.size() - 1));
        // without the incremental merge, the list would hold 48,000 longs
        Assert.assertTrue("capacity: " + out.capacity(), out.capacity() <= 4_096);
    }

    @Test
    public void testDateListWithDenseTimeSuffixCoveredByAnotherElement() throws SqlException {
        // The second element alone has 1440 separate seconds, beyond the limit of 1024 intervals,
        // but the first element covers them all, so the result is a single interval
        assertBracketInterval(
                "[{lo=2024-01-01T00:00:00.000000Z, hi=2024-01-03T23:59:59.999999Z}]",
                "[2024-01-01T00:00;3d, 2024-01-03]T[00..23]:[00..59]:00"
        );
        // 2024-01-03 is a Wednesday, so the day filter keeps all of its seconds
        assertBracketInterval(
                "[{lo=2024-01-01T00:00:00.000000Z, hi=2024-01-03T23:59:59.999999Z}]",
                "[2024-01-01T00:00;3d, 2024-01-03#workday]T[00..23]:[00..59]:00"
        );
    }

    @Test
    public void testDateListWithDenseTimeSuffixCoveredByAnotherElementAndTimezone() throws SqlException {
        // The timezone converts the intervals of the second element only, so the element merges its
        // 1440 seconds in a merge range of its own, which the first element cannot cover before the
        // timezone ran, and the range holds more of them than the limit of 1024 intervals. Without
        // the timezone, see testDateListWithDenseTimeSuffixCoveredByAnotherElement(), the first
        // element covers them.
        assertBracketIntervalError(
                "[2024-01-01T00:00;3d, 2024-01-03@-05:00]T[00..23]:[00..59]:00",
                "Bracket expansion produces too many intervals (max 1024)"
        );
        // The seconds of the minutes 00 to 40 of each hour, 984, fit the limit: 2024-01-03 at -05:00 runs
        // from 2024-01-03T05:00Z to 2024-01-04T04:59:59.999999Z, and the first element covers its
        // seconds up to the end of 2024-01-03 in UTC, so the 205 seconds from 19:00 local remain on
        // 2024-01-04. The first of them touches the end of the first element and joins it.
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final String expression = "[2024-01-01T00:00;3d, 2024-01-03@-05:00]T[00..23]:[00..40]:00";
        out.clear();
        parseTickExpr(timestampDriver, expression, 0, expression.length(), 0, out);
        final LongList expected = new LongList();
        final long nextDay = timestampDriver.parseFloorLiteral("2024-01-04T00:00:00.000000Z");
        expected.add(timestampDriver.parseFloorLiteral("2024-01-01T00:00:00.000000Z"), nextDay + timestampDriver.fromSeconds(1) - 1);
        for (int minute = 1; minute < 5 * 60; minute++) {
            if (minute % 60 <= 40) {
                final long lo = nextDay + timestampDriver.fromMinutes(minute);
                expected.add(lo, lo + timestampDriver.fromSeconds(1) - 1);
            }
        }
        // intervalToString() reuses one sink, so take a copy of the expected text first
        final String expectedText = intervalToString(timestampDriver, expected).toString();
        TestUtils.assertEquals(expectedText, intervalToString(timestampDriver, out));
    }

    @Test
    public void testDateListWithDenseTimeSuffixTooManyIntervals() {
        // five elements of 255 separate seconds each: 1,275 intervals that do not merge
        assertBracketIntervalError(
                "[2024-01-01,2024-01-02,2024-01-03,2024-01-04,2024-01-05]T[00..14]:[00..16]:00",
                "too many intervals"
        );
    }

    @Test
    public void testDateListWithDurationOnly() throws SqlException {
        // Date list with simple duration suffix (no repeating)
        // For a date without time, hi starts at end-of-day (23:59:59.999999), then +1h is added
        assertBracketInterval(
                "[{lo=2025-01-01T00:00:00.000000Z, hi=2025-01-01T00:59:59.999999Z},{lo=2025-01-05T00:00:00.000000Z, hi=2025-01-05T00:59:59.999999Z}]",
                "[2025-01-01,2025-01-05];1h"
        );
    }

    @Test
    public void testDateListWithDurationSuffix() throws SqlException {
        // '[2025-01-15,2025-01-20]T09:30;390m' produces 2 trading-hours intervals
        // 09:30 + 390 minutes = 09:30 + 6h30m = 16:00
        assertBracketInterval(
                "[{lo=2025-01-15T09:30:00.000000Z, hi=2025-01-15T15:59:59.999999Z},{lo=2025-01-20T09:30:00.000000Z, hi=2025-01-20T15:59:59.999999Z}]",
                "[2025-01-15,2025-01-20]T09:30;390m"
        );
    }

    @Test
    public void testDateListWithLeadingWhitespace() throws SqlException {
        // Ensure leading whitespace before '[' is handled
        assertBracketInterval(
                "[{lo=2025-01-01T00:00:00.000000Z, hi=2025-01-01T23:59:59.999999Z}]",
                "  [2025-01-01]"
        );
    }

    @Test
    public void testDateListWithMultipleNestedExpansions() throws SqlException {
        // '[2025-12-[30,31],2026-01-[02,03]]' - adjacent days merged: Dec 30-31, Jan 2-3
        assertBracketInterval(
                "[{lo=2025-12-30T00:00:00.000000Z, hi=2025-12-31T23:59:59.999999Z},{lo=2026-01-02T00:00:00.000000Z, hi=2026-01-03T23:59:59.999999Z}]",
                "[2025-12-[30,31],2026-01-[02,03]]"
        );
    }

    @Test
    public void testDateListWithNestedExpansion() throws SqlException {
        // '[2025-12-31,2026-01-[03..05]]' - Dec 31 separate, Jan 3-5 merged
        assertBracketInterval(
                "[{lo=2025-12-31T00:00:00.000000Z, hi=2025-12-31T23:59:59.999999Z},{lo=2026-01-03T00:00:00.000000Z, hi=2026-01-05T23:59:59.999999Z}]",
                "[2025-12-31,2026-01-[03..05]]"
        );
    }

    @Test
    public void testDateListWithNestedTimeExpansion() throws SqlException {
        assertBracketInterval(
                "[{lo=2024-01-15T09:00:00.000000Z, hi=2024-01-15T09:00:00.999999Z},{lo=2024-01-15T09:00:30.000000Z, hi=2024-01-15T09:00:30.999999Z},{lo=2024-01-15T14:30:00.000000Z, hi=2024-01-15T14:30:00.999999Z},{lo=2024-01-15T14:30:30.000000Z, hi=2024-01-15T14:30:30.999999Z},{lo=2024-01-16T09:00:00.000000Z, hi=2024-01-16T09:00:00.999999Z},{lo=2024-01-16T09:00:30.000000Z, hi=2024-01-16T09:00:30.999999Z},{lo=2024-01-16T14:30:00.000000Z, hi=2024-01-16T14:30:00.999999Z},{lo=2024-01-16T14:30:30.000000Z, hi=2024-01-16T14:30:30.999999Z}]",
                "[2024-01-15,2024-01-16]T[09:00,14:30]:[00,30]"
        );
    }

    @Test
    public void testDateListWithNestedTimeExpansionAfterError() throws SqlException {
        for (int i = 0; i < 3; i++) {
            assertBracketIntervalError("[2024-01-15,2024-01-16]T[09:00,14:30]:[00,99]", "Invalid date");
            testDateListWithNestedTimeExpansion();
        }
    }

    @Test
    public void testDateListWithNestedTimeExpansionAndDayFilterAndTimezone() throws SqlException {
        assertBracketInterval(
                "[{lo=2024-01-15T07:00:00.000000Z, hi=2024-01-15T07:00:00.999999Z},{lo=2024-01-15T07:00:30.000000Z, hi=2024-01-15T07:00:30.999999Z},{lo=2024-01-15T12:30:00.000000Z, hi=2024-01-15T12:30:00.999999Z},{lo=2024-01-15T12:30:30.000000Z, hi=2024-01-15T12:30:30.999999Z}]",
                "[2024-01-15,2024-01-16]T[09:00,14:30]:[00,30]@+02:00#Mon"
        );
    }

    @Test
    public void testDateListWithTimeExpansion() throws SqlException {
        // '[2025-01-15,2025-01-20]T[09,14]:30;1h' produces 4 intervals (date × time expansion)
        assertBracketInterval(
                "[{lo=2025-01-15T09:30:00.000000Z, hi=2025-01-15T10:29:59.999999Z},{lo=2025-01-15T14:30:00.000000Z, hi=2025-01-15T15:29:59.999999Z},{lo=2025-01-20T09:30:00.000000Z, hi=2025-01-20T10:29:59.999999Z},{lo=2025-01-20T14:30:00.000000Z, hi=2025-01-20T15:29:59.999999Z}]",
                "[2025-01-15,2025-01-20]T[09,14]:30;1h"
        );
    }

    @Test
    public void testDateListWithTimeSuffix() throws SqlException {
        // '[2025-01-15,2025-01-20]T09:30' produces 2 intervals with specific time
        assertBracketInterval(
                "[{lo=2025-01-15T09:30:00.000000Z, hi=2025-01-15T09:30:59.999999Z},{lo=2025-01-20T09:30:00.000000Z, hi=2025-01-20T09:30:59.999999Z}]",
                "[2025-01-15,2025-01-20]T09:30"
        );
    }

    @Test
    public void testDateListWithWhitespace() throws SqlException {
        // '[ 2025-01-01 , 2025-01-02 ]' - whitespace inside brackets, adjacent days merged
        assertBracketInterval(
                "[{lo=2025-01-01T00:00:00.000000Z, hi=2025-01-02T23:59:59.999999Z}]",
                "[ 2025-01-01 , 2025-01-02 ]"
        );
    }

    @Test
    public void testDateListZonedElementOverMaxIntervalsFails() throws SqlException {
        // A date list element or a time list value with a time zone keeps a merge range of its
        // own, because the zone converts only its intervals. Its merges count its own intervals
        // against the cap (1,024), so it fails even when an earlier element covers them.
        final LongList day = new LongList();
        addLocalDays(day, LocalDate.of(2024, 1, 1), LocalDate.of(2024, 1, 2), ZoneOffset.UTC);
        for (String interval : new String[]{
                "[2024-01-01,2024-01-01T[09..10]:00:[00..59].[0..9]0]@+05:00",
                "[2024-01-01,2024-01-01T[09:00,10:00]:[00..59].[0..9]0]@+05:00",
                "[2024-01-01,2024-01-01T[09:00]:[00..59].[0..9][0..9]0]@+05:00",
                "[2024-01-01@+05:00,2024-01-01T[09..10]:00:[00..59].[0..9]0@+05:00]",
                "[2024-01-01,2024-01-01T[09:00@+05:00]:[00..59].[0..9][0..9]0]",
        }) {
            assertTickExprErrorAtMonday(interval, "Bracket expansion produces too many intervals (max 1024)");
            // parseTickExpr unwinds the list, and the zone-less list parses next
            Assert.assertEquals(interval, 0, out.size());
            assertTickExprIntervals(configuration, day, "[2024-01-01,2024-01-01T[09..10]:00:[00..59].[0..9]0]");
        }
    }

    @Test
    public void testDateVariableArithmeticBusinessDays() throws SqlException {
        // 2026-01-22 is Thursday
        final TimestampDriver timestampDriver = timestampType.getDriver();
        long now = timestampDriver.parseFloorLiteral("2026-01-22T10:30:00.000000Z");
        out.clear();

        // [$today + 1bd] should be Friday 2026-01-23
        String interval = "[$today + 1bd]";
        parseTickExprWithNow(timestampDriver, interval, 0, interval.length(), 0, out, now);
        TestUtils.assertEquals(
                ColumnType.isTimestampNano(timestampType.getTimestampType())
                        ? "[{lo=2026-01-23T00:00:00.000000000Z, hi=2026-01-23T23:59:59.999999999Z}]"
                        : "[{lo=2026-01-23T00:00:00.000000Z, hi=2026-01-23T23:59:59.999999Z}]",
                intervalToString(timestampDriver, out)
        );

        out.clear();
        // [$today + 2bd] should be Monday 2026-01-26 (skip weekend)
        interval = "[$today + 2bd]";
        parseTickExprWithNow(timestampDriver, interval, 0, interval.length(), 0, out, now);
        TestUtils.assertEquals(
                ColumnType.isTimestampNano(timestampType.getTimestampType())
                        ? "[{lo=2026-01-26T00:00:00.000000000Z, hi=2026-01-26T23:59:59.999999999Z}]"
                        : "[{lo=2026-01-26T00:00:00.000000Z, hi=2026-01-26T23:59:59.999999Z}]",
                intervalToString(timestampDriver, out)
        );

        out.clear();
        // [$today - 1bd] should be Wednesday 2026-01-21
        interval = "[$today - 1bd]";
        parseTickExprWithNow(timestampDriver, interval, 0, interval.length(), 0, out, now);
        TestUtils.assertEquals(
                ColumnType.isTimestampNano(timestampType.getTimestampType())
                        ? "[{lo=2026-01-21T00:00:00.000000000Z, hi=2026-01-21T23:59:59.999999999Z}]"
                        : "[{lo=2026-01-21T00:00:00.000000Z, hi=2026-01-21T23:59:59.999999Z}]",
                intervalToString(timestampDriver, out)
        );

        out.clear();
        // [$today + 0bd] should be today 2026-01-22
        interval = "[$today + 0bd]";
        parseTickExprWithNow(timestampDriver, interval, 0, interval.length(), 0, out, now);
        TestUtils.assertEquals(
                ColumnType.isTimestampNano(timestampType.getTimestampType())
                        ? "[{lo=2026-01-22T00:00:00.000000000Z, hi=2026-01-22T23:59:59.999999999Z}]"
                        : "[{lo=2026-01-22T00:00:00.000000Z, hi=2026-01-22T23:59:59.999999Z}]",
                intervalToString(timestampDriver, out)
        );
    }

    @Test
    public void testDateVariableArithmeticCalendarDays() throws SqlException {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        long now = timestampDriver.parseFloorLiteral("2026-01-22T10:30:00.000000Z");
        out.clear();

        // [$today + 5d] should be 2026-01-27
        String interval = "[$today + 5d]";
        parseTickExprWithNow(timestampDriver, interval, 0, interval.length(), 0, out, now);
        TestUtils.assertEquals(
                ColumnType.isTimestampNano(timestampType.getTimestampType())
                        ? "[{lo=2026-01-27T00:00:00.000000000Z, hi=2026-01-27T23:59:59.999999999Z}]"
                        : "[{lo=2026-01-27T00:00:00.000000Z, hi=2026-01-27T23:59:59.999999Z}]",
                intervalToString(timestampDriver, out)
        );

        out.clear();
        // [$today - 3d] should be 2026-01-19
        interval = "[$today - 3d]";
        parseTickExprWithNow(timestampDriver, interval, 0, interval.length(), 0, out, now);
        TestUtils.assertEquals(
                ColumnType.isTimestampNano(timestampType.getTimestampType())
                        ? "[{lo=2026-01-19T00:00:00.000000000Z, hi=2026-01-19T23:59:59.999999999Z}]"
                        : "[{lo=2026-01-19T00:00:00.000000Z, hi=2026-01-19T23:59:59.999999Z}]",
                intervalToString(timestampDriver, out)
        );
    }

    @Test
    public void testDateVariableArithmeticHours() throws SqlException {
        // $now - 2h should be 2 hours earlier (point-in-time with microsecond precision)
        assertBracketIntervalWithNow(
                "[{lo=2026-01-22T08:30:00.000000Z, hi=2026-01-22T08:30:00.000000Z}]",
                "[$now - 2h]",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableArithmeticHoursWithTimeSuffix() throws SqlException {
        // $now - 1h already has time, so T09:30 suffix is skipped (element time takes precedence)
        // Result is point-in-time with microsecond precision
        assertBracketIntervalWithNow(
                "[{lo=2026-01-22T09:30:00.000000Z, hi=2026-01-22T09:30:00.000000Z}]",
                "[$now - 1h]T09:30",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableArithmeticMicroseconds() throws SqlException {
        // $now + 100u should be 100 microseconds later (point-in-time with microsecond precision)
        // Using 100u so the pattern replacement for nano works (ends in 00)
        assertBracketIntervalWithNow(
                "[{lo=2026-01-22T10:30:00.000100Z, hi=2026-01-22T10:30:00.000100Z}]",
                "[$now + 100u]",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableArithmeticMilliseconds() throws SqlException {
        // $now + 500T should be 500 milliseconds later (point-in-time with microsecond precision)
        assertBracketIntervalWithNow(
                "[{lo=2026-01-22T10:30:00.500000Z, hi=2026-01-22T10:30:00.500000Z}]",
                "[$now + 500T]",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableArithmeticMinutes() throws SqlException {
        // $now + 30m should be 30 minutes later (point-in-time with microsecond precision)
        assertBracketIntervalWithNow(
                "[{lo=2026-01-22T11:00:00.000000Z, hi=2026-01-22T11:00:00.000000Z}]",
                "[$now + 30m]",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableArithmeticMonths() throws SqlException {
        // $today + 1M should be 1 month later (Feb 22)
        assertBracketIntervalWithNow(
                "[{lo=2026-02-22T00:00:00.000000Z, hi=2026-02-22T23:59:59.999999Z}]",
                "[$today + 1M]",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableArithmeticMonthsEndOfMonth() throws SqlException {
        // $today + 1M from Jan 31 should clamp to Feb 28 (non-leap year)
        assertBracketIntervalWithNow(
                "[{lo=2026-02-28T00:00:00.000000Z, hi=2026-02-28T23:59:59.999999Z}]",
                "[$today + 1M]",
                "2026-01-31T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableArithmeticSeconds() throws SqlException {
        // $now + 90s should be 90 seconds later (point-in-time with microsecond precision)
        assertBracketIntervalWithNow(
                "[{lo=2026-01-22T10:31:30.000000Z, hi=2026-01-22T10:31:30.000000Z}]",
                "[$now + 90s]",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableArithmeticSeconds3600() throws SqlException {
        // $now + 3600s should be exactly 1 hour later (point-in-time with microsecond precision)
        assertBracketIntervalWithNow(
                "[{lo=2026-01-22T11:30:00.000000Z, hi=2026-01-22T11:30:00.000000Z}]",
                "[$now + 3600s]",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableArithmeticWeeks() throws SqlException {
        // $today + 2w should be 2 weeks (14 days) later
        assertBracketIntervalWithNow(
                "[{lo=2026-02-05T00:00:00.000000Z, hi=2026-02-05T23:59:59.999999Z}]",
                "[$today + 2w]",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableArithmeticWithTimeSuffix() throws SqlException {
        // Date variable arithmetic + time suffix
        // $today + 3d at 09:30
        assertDateVariableInterval(
                (now, driver, tsType) -> {
                    // $today + 3d at 09:30
                    long todayStart = driver.startOfDay(now, 0);
                    long targetDay = driver.addDays(todayStart, 3);
                    String date = formatDate(targetDay, driver);
                    return "[{lo=" + date + "T09:30:00.000000Z, hi=" + date + "T09:30:59.999999Z}]";
                },
                "[$today + 3d]T09:30"
        );
    }

    @Test
    public void testDateVariableArithmeticYears() throws SqlException {
        // $today + 1y should be 1 year later
        assertBracketIntervalWithNow(
                "[{lo=2027-01-22T00:00:00.000000Z, hi=2027-01-22T23:59:59.999999Z}]",
                "[$today + 1y]",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableArithmeticYearsLeapYear() throws SqlException {
        // $today + 1y from Feb 29 leap year clamps to Feb 28 (non-leap year)
        assertBracketIntervalWithNow(
                "[{lo=2025-02-28T00:00:00.000000Z, hi=2025-02-28T23:59:59.999999Z}]",
                "[$today + 1y]",
                "2024-02-29T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableBareCommaListRejected() {
        // Bare comma lists without brackets should be rejected
        // $now,$tomorrow should fail - needs brackets: [$now,$tomorrow]
        assertBracketIntervalError("$now,$tomorrow", "comma-separated date lists require brackets");
    }

    @Test
    public void testDateVariableBareInvalidVariable() {
        // $invalid - unknown bare date variable triggers error and resets output
        assertBracketIntervalError("$invalid", "Unknown date variable");
    }

    @Test
    public void testDateVariableBareRange() throws SqlException {
        // $today..$today+5d without brackets - range of 6 days
        assertBracketIntervalWithNow(
                "[{lo=2026-01-22T00:00:00.000000Z, hi=2026-01-27T23:59:59.999999Z}]",
                "$today..$today+5d",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableBareWithArithmetic() throws SqlException {
        // $now - 2h without brackets
        assertBracketIntervalWithNow(
                "[{lo=2026-01-22T08:30:00.000000Z, hi=2026-01-22T08:30:00.000000Z}]",
                "$now - 2h",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableBareWithBracketedTime() throws SqlException {
        // A bare date variable takes a bracketed time part the same way as the bracketed form
        final String now = "2024-01-02T10:30:00.000000Z";
        assertParsedAndCompiledWithNow(
                "[{lo=2024-01-02T09:00:00.000000Z, hi=2024-01-02T09:00:59.999999Z},{lo=2024-01-02T14:30:00.000000Z, hi=2024-01-02T14:30:59.999999Z}]",
                "$todayT[09:00,14:30]",
                now
        );
        assertParsedAndCompiledWithNow(
                "[{lo=2024-01-01T09:00:00.000000Z, hi=2024-01-01T09:00:59.999999Z},{lo=2024-01-01T14:30:00.000000Z, hi=2024-01-01T14:30:59.999999Z},{lo=2024-01-02T09:00:00.000000Z, hi=2024-01-02T09:00:59.999999Z},{lo=2024-01-02T14:30:00.000000Z, hi=2024-01-02T14:30:59.999999Z}]",
                "$yesterday..$todayT[09:00,14:30]",
                now
        );
        assertParsedAndCompiledWithNow(
                "[{lo=2024-01-02T01:30:00.000000Z, hi=2024-01-02T01:30:59.999999Z},{lo=2024-01-02T02:30:00.000000Z, hi=2024-01-02T02:30:59.999999Z}]",
                "$todayT[01,02]:30",
                now
        );
        // 10:30 at +02:00 is 08:30Z
        assertParsedAndCompiledWithNow(
                "[{lo=2024-01-02T08:30:00.000000Z, hi=2024-01-02T08:30:59.999999Z}]",
                "$todayT[10:30@+02:00]",
                now
        );
    }

    @Test
    public void testDateVariableBareWithBracketedTimeIsDynamic() throws SqlException {
        // A bare date variable with a bracketed time part holds a date variable, the same as its
        // bracketed form, so the interval model evaluates it on every execution rather than once,
        // when the SQL compiles
        assertCompiledTickExpr("$todayT[09:00,14:30]");
        assertCompiledTickExpr("$tomorrowT[10:30]");
        assertCompiledTickExpr("$yesterdayT[09:00];30m");
        assertCompiledTickExpr("$TODAYT[09:00]@Europe/London#workday");
        assertCompiledTickExpr("$nowT[09:00]");
        // the compiled form parses these at every evaluation
        assertCompiledTickExpr("$todayT[01,02]:30");
        assertCompiledTickExpr("$todayT[09..10]:30");
        assertCompiledTickExpr("$todayT[10:30@+02:00]");
    }

    @Test
    public void testDateVariableBareWithDayFilter() throws SqlException {
        // $today#Thu - bare date variable with day filter (2026-01-22 is Thursday)
        assertBracketIntervalWithNow(
                "[{lo=2026-01-22T00:00:00.000000Z, hi=2026-01-22T23:59:59.999999Z}]",
                "$today#Thu",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableBareWithDayFilterNoMatch() throws SqlException {
        // $today#Mon - bare date variable with day filter that doesn't match (2026-01-22 is Thursday)
        assertBracketIntervalWithNow(
                "[]",
                "$today#Mon",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableBareWithDuration() throws SqlException {
        // $now - 1h with duration suffix ;30m without brackets
        assertBracketIntervalWithNow(
                "[{lo=2026-01-22T09:30:00.000000Z, hi=2026-01-22T09:59:59.999999Z}]",
                "$now - 1h;30m",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableBareWithMalformedTimeFailsAtCompileTime() {
        // A malformed time part after a bare date variable fails when the SQL compiles
        assertIntervalModelError("$todayT[", "Unclosed '['");
        assertIntervalModelError("$todayT[25:00]", "25:00");
        assertIntervalModelError("[$todayT[09:00]]", "Unknown date variable");
        assertIntervalModelError("$todayt[09:00]", "Unknown date variable");
        assertIntervalModelError("$todayT", "Unknown date variable");
    }

    @Test
    public void testDateVariableBareWithTimeSuffix() throws SqlException {
        // $today with time suffix T09:30 without brackets
        assertBracketIntervalWithNow(
                "[{lo=2026-01-22T09:30:00.000000Z, hi=2026-01-22T09:30:59.999999Z}]",
                "$todayT09:30",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableBareWithTimezone() throws SqlException {
        // $today@America/New_York - bare date variable with timezone
        // 2026-01-22 in NY = 05:00 UTC to 04:59:59 UTC next day (EST is UTC-5)
        assertBracketIntervalWithNow(
                "[{lo=2026-01-22T05:00:00.000000Z, hi=2026-01-23T04:59:59.999999Z}]",
                "$today@America/New_York",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableBareWithUppercaseToday() throws SqlException {
        // $TODAY in uppercase (should not stop at 'T')
        assertBracketIntervalWithNow(
                "[{lo=2026-01-22T00:00:00.000000Z, hi=2026-01-22T23:59:59.999999Z}]",
                "$TODAY",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableBareWithUppercaseTomorrow() throws SqlException {
        // $TOMORROW in uppercase (should not stop at 'T')
        assertBracketIntervalWithNow(
                "[{lo=2026-01-23T00:00:00.000000Z, hi=2026-01-23T23:59:59.999999Z}]",
                "$TOMORROW",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableBusinessDaysFromWeekend() throws SqlException {
        // 2026-01-24 is Saturday
        final TimestampDriver timestampDriver = timestampType.getDriver();
        long now = timestampDriver.parseFloorLiteral("2026-01-24T10:30:00.000000Z");
        out.clear();

        // From Saturday, +1bd should be Monday 2026-01-26
        String interval = "[$today + 1bd]";
        parseTickExprWithNow(timestampDriver, interval, 0, interval.length(), 0, out, now);
        TestUtils.assertEquals(
                ColumnType.isTimestampNano(timestampType.getTimestampType())
                        ? "[{lo=2026-01-26T00:00:00.000000000Z, hi=2026-01-26T23:59:59.999999999Z}]"
                        : "[{lo=2026-01-26T00:00:00.000000Z, hi=2026-01-26T23:59:59.999999Z}]",
                intervalToString(timestampDriver, out)
        );

        out.clear();
        // From Saturday, -1bd should be Friday 2026-01-23
        interval = "[$today - 1bd]";
        parseTickExprWithNow(timestampDriver, interval, 0, interval.length(), 0, out, now);
        TestUtils.assertEquals(
                ColumnType.isTimestampNano(timestampType.getTimestampType())
                        ? "[{lo=2026-01-23T00:00:00.000000000Z, hi=2026-01-23T23:59:59.999999999Z}]"
                        : "[{lo=2026-01-23T00:00:00.000000Z, hi=2026-01-23T23:59:59.999999Z}]",
                intervalToString(timestampDriver, out)
        );
    }

    @Test
    public void testDateVariableBusinessDaysWithTimeSuffix() throws SqlException {
        // Date variable business day arithmetic + time suffix
        // $today (Wednesday 2025-04-09) + 2bd = Friday 2025-04-11
        assertBracketIntervalWithNow(
                "[{lo=2025-04-11T09:30:00.000000Z, hi=2025-04-11T10:29:59.999999Z}]",
                "[$today + 2bd]T09:30;1h",
                "2025-04-09T14:45:00.000000Z"
        );
    }

    @Test
    public void testDateVariableCaseInsensitive() throws SqlException {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        long now = timestampDriver.parseFloorLiteral("2026-01-22T10:30:00.000000Z");
        out.clear();

        // Test various case combinations
        String interval = "[$TODAY]";
        parseTickExprWithNow(timestampDriver, interval, 0, interval.length(), 0, out, now);
        TestUtils.assertEquals(
                ColumnType.isTimestampNano(timestampType.getTimestampType())
                        ? "[{lo=2026-01-22T00:00:00.000000000Z, hi=2026-01-22T23:59:59.999999999Z}]"
                        : "[{lo=2026-01-22T00:00:00.000000Z, hi=2026-01-22T23:59:59.999999Z}]",
                intervalToString(timestampDriver, out)
        );

        out.clear();
        interval = "[$ToDay + 1BD]";
        parseTickExprWithNow(timestampDriver, interval, 0, interval.length(), 0, out, now);
        TestUtils.assertEquals(
                ColumnType.isTimestampNano(timestampType.getTimestampType())
                        ? "[{lo=2026-01-23T00:00:00.000000000Z, hi=2026-01-23T23:59:59.999999999Z}]"
                        : "[{lo=2026-01-23T00:00:00.000000Z, hi=2026-01-23T23:59:59.999999Z}]",
                intervalToString(timestampDriver, out)
        );
    }

    @Test
    public void testDateVariableDetectionFuzz() {
        // SQL folds a tick expression into constants when the query compiles, unless
        // DateExpressionEvaluator.containsDateVariable() finds a date variable in it. Then it evaluates
        // the expression on every execution, through CompiledTickExpression. An expression that depends
        // on "now" but that the detector misses keeps, in a cached plan, the intervals of the time the
        // query compiled at. The checker parses each expression at several "now" values and requires a
        // detected date variable when the results differ.
        final DateVariableDetectionChecker checker = new DateVariableDetectionChecker(timestampType.getDriver(), configuration);
        // every bare date variable followed by every suffix, where the end of the variable name meets
        // the start of the suffix
        for (String variable : FUZZ_VARIABLES) {
            for (String time : FUZZ_TIMES) {
                for (String zone : FUZZ_ZONES) {
                    for (String dayFilter : FUZZ_DAY_FILTERS) {
                        for (String duration : FUZZ_DURATIONS) {
                            checker.check(variable + time + zone + dayFilter + duration, false);
                        }
                        for (String duration : FUZZ_UNKNOWN_UNIT_DURATIONS) {
                            checker.check(variable + time + zone + dayFilter + duration, false);
                        }
                        for (String duration : FUZZ_REJECTED_SUFFIXES) {
                            checker.check(variable + time + zone + dayFilter + duration, false);
                        }
                    }
                }
            }
            for (String time : FUZZ_MALFORMED_TIMES) {
                for (String zone : FUZZ_ZONES) {
                    checker.check(variable + time + zone, false);
                }
            }
        }
        // every range between two points in time followed by every suffix, with the day filter
        // before and after the duration; [$today + 570m..$now] starts after it ends at the "now"
        // values 00:30 and 05:30
        for (String range : new String[]{"$now - 1h..$now", "$today + 2h..$NOW", "[$now - 1h..$now]", "[$today + 570m..$now]"}) {
            for (String time : FUZZ_TIMES) {
                for (String zone : FUZZ_ZONES) {
                    for (String dayFilter : FUZZ_DAY_FILTERS) {
                        for (String[] durations : new String[][]{FUZZ_DURATIONS, FUZZ_UNKNOWN_UNIT_DURATIONS, FUZZ_REJECTED_SUFFIXES}) {
                            for (String duration : durations) {
                                checker.check(range + time + zone + dayFilter + duration, false);
                                if (!dayFilter.isEmpty() && !duration.isEmpty()) {
                                    checker.check(range + time + zone + duration + dayFilter, false);
                                }
                            }
                        }
                    }
                }
            }
        }
        // every static element with every time part of its own, next to a date variable, under every
        // time part and timezone of the list; a time part with a slot that has a timezone of its own
        // counts as malformed, see DateVariableDetectionChecker
        for (String element : FUZZ_STATIC_ELEMENTS) {
            for (String elementTime : FUZZ_TIMES) {
                for (String time : FUZZ_TIMES) {
                    for (String zone : FUZZ_ZONES) {
                        checker.check("[" + element + elementTime + ", $today]" + time + zone, elementTime.indexOf('@') >= 0);
                    }
                }
            }
        }
        // and random expressions of every shape; they hold static elements with a bracketed time part
        // under a time of the list too, which the checker then compares next to other elements,
        // ranges, day filters and durations
        final int sweptStaticElementBracketedTimeParsedCount = checker.staticElementBracketedTimeParsedCount;
        final Rnd rnd = TestUtils.generateRandom(LOG);
        final StringSink expression = new StringSink();
        for (int i = 0; i < 10_000; i++) {
            expression.clear();
            final boolean isMalformed = appendRandomTickExpr(rnd, expression);
            checker.check(expression.toString(), isMalformed);
        }
        checker.assertNoFailures();
        Assert.assertTrue(
                "no random expression with a static element with a bracketed time part under a time of the list parsed on evaluation",
                checker.staticElementBracketedTimeParsedCount > sweptStaticElementBracketedTimeParsedCount
        );
    }

    @Test
    public void testDateVariableDetectionFuzzAtSmallMergeThresholds() {
        // The random expressions of testDateVariableDetectionFuzz(), whose date ranges merge their
        // days into the intervals so far after 2 or 4 intervals, with a limit of 1,024 or 16. Their
        // elements come in any order, so a range often follows elements that lie later in time.
        for (int[] limits : new int[][]{{2, 1024}, {4, 16}}) {
            final DateVariableDetectionChecker checker = new DateVariableDetectionChecker(
                    timestampType.getDriver(),
                    intervalLimitConfiguration(limits[0], limits[1])
            );
            final Rnd rnd = TestUtils.generateRandom(LOG);
            final StringSink expression = new StringSink();
            for (int i = 0; i < 10_000; i++) {
                expression.clear();
                final boolean isMalformed = appendRandomTickExpr(rnd, expression);
                checker.check(expression.toString(), isMalformed);
            }
            checker.assertNoFailures();
        }
    }

    @Test
    public void testDateVariableExprEvaluateBusinessDays() throws SqlException {
        // 2026-03-16 is a Monday
        final TimestampDriver driver = timestampType.getDriver();
        long now = driver.parseFloorLiteral("2026-03-16T10:00:00.000000Z");
        DateVariableExpr expr = parseDateVar("$today + 3bd", 12);
        long todayStart = driver.startOfDay(now, 0);
        // Mon + 3bd = Thu (skip no weekends)
        long expected = driver.addDays(todayStart, 3);
        Assert.assertEquals(expected, expr.evaluate(driver, now));
    }

    @Test
    public void testDateVariableExprEvaluateBusinessDaysAcrossWeekend() throws SqlException {
        // 2026-03-20 is a Friday
        final TimestampDriver driver = timestampType.getDriver();
        long now = driver.parseFloorLiteral("2026-03-20T10:00:00.000000Z");
        DateVariableExpr expr = parseDateVar("$today + 3bd", 12);
        long todayStart = driver.startOfDay(now, 0);
        // Fri + 3bd = Wed (skip Sat, Sun)
        long expected = driver.addDays(todayStart, 5); // Fri + 5 calendar days = Wed
        Assert.assertEquals(expected, expr.evaluate(driver, now));
    }

    @Test
    public void testDateVariableExprEvaluateNow() throws SqlException {
        final TimestampDriver driver = timestampType.getDriver();
        long now = driver.parseFloorLiteral("2026-03-15T14:30:00.000000Z");
        DateVariableExpr expr = parseDateVar("$now", 4);
        Assert.assertEquals(now, expr.evaluate(driver, now));
    }

    @Test
    public void testDateVariableExprEvaluateToday() throws SqlException {
        final TimestampDriver driver = timestampType.getDriver();
        long now = driver.parseFloorLiteral("2026-03-15T14:30:00.000000Z");
        DateVariableExpr expr = parseDateVar("$today", 6);
        long expected = driver.startOfDay(now, 0);
        Assert.assertEquals(expected, expr.evaluate(driver, now));
    }

    @Test
    public void testDateVariableExprEvaluateTomorrow() throws SqlException {
        final TimestampDriver driver = timestampType.getDriver();
        long now = driver.parseFloorLiteral("2026-03-15T14:30:00.000000Z");
        DateVariableExpr expr = parseDateVar("$tomorrow", 9);
        long expected = driver.startOfDay(now, 1);
        Assert.assertEquals(expected, expr.evaluate(driver, now));
    }

    @Test
    public void testDateVariableExprEvaluateWithOffset() throws SqlException {
        final TimestampDriver driver = timestampType.getDriver();
        long now = driver.parseFloorLiteral("2026-03-15T14:30:00.000000Z");
        DateVariableExpr expr = parseDateVar("$now - 2h", 9);
        long expected = driver.add(now, 'h', -2);
        Assert.assertEquals(expected, expr.evaluate(driver, now));
    }

    @Test
    public void testDateVariableExprEvaluateYesterday() throws SqlException {
        final TimestampDriver driver = timestampType.getDriver();
        long now = driver.parseFloorLiteral("2026-03-15T14:30:00.000000Z");
        DateVariableExpr expr = parseDateVar("$yesterday", 10);
        long expected = driver.startOfDay(now, -1);
        Assert.assertEquals(expected, expr.evaluate(driver, now));
    }

    @Test
    public void testDateVariableExprParseCaseInsensitive() throws SqlException {
        DateVariableExpr expr1 = parseDateVar("$TODAY", 6);
        Assert.assertEquals(DateVariableExpr.VAR_TODAY, expr1.getVarType());

        DateVariableExpr expr2 = parseDateVar("$Now", 4);
        Assert.assertEquals(DateVariableExpr.VAR_NOW, expr2.getVarType());

        DateVariableExpr expr3 = parseDateVar("$YESTERDAY", 10);
        Assert.assertEquals(DateVariableExpr.VAR_YESTERDAY, expr3.getVarType());

        DateVariableExpr expr4 = parseDateVar("$TOMORROW", 9);
        Assert.assertEquals(DateVariableExpr.VAR_TOMORROW, expr4.getVarType());
    }

    @Test
    public void testDateVariableExprParseCompact() throws SqlException {
        // Compact form without spaces: $today+3d
        final TimestampDriver driver = timestampType.getDriver();
        long now = driver.parseFloorLiteral("2026-03-15T10:00:00.000000Z");
        DateVariableExpr expr = parseDateVar("$today+3d", 9);
        long expected = driver.add(driver.startOfDay(now, 0), 'd', 3);
        Assert.assertEquals(expected, expr.evaluate(driver, now));
    }

    @Test
    public void testDateVariableExprParseInvalidVariable() {
        try {
            parseDateVar("$garbage", 8);
            Assert.fail("Should throw SqlException");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "Unknown date variable");
        }
    }

    @Test
    public void testDateVariableExprProducesDifferentResultsForDifferentNow() throws SqlException {
        final TimestampDriver driver = timestampType.getDriver();
        DateVariableExpr expr = parseDateVar("$today", 6);

        long now1 = driver.parseFloorLiteral("2026-01-15T10:00:00.000000Z");
        long now2 = driver.parseFloorLiteral("2026-06-20T10:00:00.000000Z");

        long result1 = expr.evaluate(driver, now1);
        long result2 = expr.evaluate(driver, now2);

        Assert.assertNotEquals("Same expr should produce different results for different now", result1, result2);
    }

    @Test
    public void testDateVariableInvalidAddition() {
        // Adding two date variables is not supported - expects number after operator
        assertBracketIntervalError("[$today + $today]", "Expected number after operator");
    }

    @Test
    public void testDateVariableInvalidName() {
        assertBracketIntervalError("[$invalid]", "Unknown date variable");
    }

    @Test
    public void testDateVariableInvalidOperator() {
        // "$today * 5d" - multiplication is not a valid operator
        assertBracketIntervalError("[$today * 5d]", "Expected '+' or '-' operator");
    }

    @Test
    public void testDateVariableInvalidUnit() {
        assertBracketIntervalError("[$today + 5x]", "Invalid time unit");
    }

    @Test
    public void testDateVariableInvalidUnitStartingWithB() {
        // 'b' followed by non-'d' character - detected as unexpected trailing chars
        assertBracketIntervalError("[$today + 5bx]", "Unexpected characters after unit");
    }

    @Test
    public void testDateVariableMissingOffsetAfterOperator() {
        // Operator at end with nothing after
        assertBracketIntervalError("[$today +]", "Expected number after operator");
    }

    @Test
    public void testDateVariableMissingUnit() {
        // Number without unit at end
        assertBracketIntervalError("[$today + 5]", "Expected time unit after number");
    }

    @Test
    public void testDateVariableMixedList() throws SqlException {
        // [$today, $yesterday, 2025-06-10]T18:30 - mixed list with date variables and static date
        // Intervals should be sorted: Jun 10, Jun 17 (yesterday), Jun 18 (today)
        assertBracketIntervalWithNow(
                "[{lo=2025-06-10T18:30:00.000000Z, hi=2025-06-10T18:30:59.999999Z},{lo=2025-06-17T18:30:00.000000Z, hi=2025-06-17T18:30:59.999999Z},{lo=2025-06-18T18:30:00.000000Z, hi=2025-06-18T18:30:59.999999Z}]",
                "[$today, $yesterday, 2025-06-10]T18:30",
                "2025-06-18T08:15:00.000000Z"
        );
    }

    @Test
    public void testDateVariableMixedListWithDifferentTimes() throws SqlException {
        // Mixed list: some date vars, some static dates with times, some without
        // $yesterday gets T14:00, 2025-03-05T09:30 keeps 09:30, 2025-03-10 gets T14:00
        assertBracketIntervalWithNow(
                "[{lo=2025-03-05T09:30:00.000000Z, hi=2025-03-05T09:30:59.999999Z},{lo=2025-03-10T14:00:00.000000Z, hi=2025-03-10T14:00:59.999999Z},{lo=2025-03-19T14:00:00.000000Z, hi=2025-03-19T14:00:59.999999Z}]",
                "[$yesterday, 2025-03-05T09:30, 2025-03-10]T14:00",
                "2025-03-20T16:45:00.000000Z"
        );
    }

    @Test
    public void testDateVariableMixedWithStaticDateWithTime() throws SqlException {
        // Date variable gets time suffix, static date with time keeps its time
        // $today (2025-08-12) gets T10:00, 2025-08-01T09:30 keeps 09:30
        assertBracketIntervalWithNow(
                "[{lo=2025-08-01T09:30:00.000000Z, hi=2025-08-01T09:30:59.999999Z},{lo=2025-08-12T10:00:00.000000Z, hi=2025-08-12T10:00:59.999999Z}]",
                "[$today, 2025-08-01T09:30]T10:00",
                "2025-08-12T11:20:00.000000Z"
        );
    }

    @Test
    public void testDateVariableNow() throws SqlException {
        // [$now] preserves time component from now (10:30) at microsecond precision (point-in-time)
        assertBracketIntervalWithNow(
                "[{lo=2026-01-22T10:30:00.000000Z, hi=2026-01-22T10:30:00.000000Z}]",
                "[$now]",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableNowAtMidnight() throws SqlException {
        // https://github.com/questdb/questdb/issues/8083
        // $now is a point in time even when the clock reads exactly midnight
        assertParsedAndCompiledWithNow(
                "[{lo=2024-01-02T00:00:00.000000Z, hi=2024-01-02T00:00:00.000000Z}]",
                "$now",
                "2024-01-02T00:00:00.000000Z"
        );
    }

    @Test
    public void testDateVariableNowBare() throws SqlException {
        // $now without brackets - same result as [$now]
        assertBracketIntervalWithNow(
                "[{lo=2026-01-22T10:30:00.000000Z, hi=2026-01-22T10:30:00.000000Z}]",
                "$now",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableNowMixedWithGlobalTimezone() throws SqlException {
        // [$now, 2026-01-15]@America/New_York - both get timezone applied
        // $now 10:30 in NY = 15:30 UTC (point-in-time); 2026-01-15 full day in NY = 05:00 UTC to 04:59:59 UTC next day
        assertBracketIntervalWithNow(
                "[{lo=2026-01-15T05:00:00.000000Z, hi=2026-01-16T04:59:59.999999Z},{lo=2026-01-22T15:30:00.000000Z, hi=2026-01-22T15:30:00.000000Z}]",
                "[$now, 2026-01-15]@America/New_York",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableNowMixedWithStaticDate() throws SqlException {
        // [$now, 2026-01-15]T09:00 - $now keeps its time (10:30, point-in-time), static date gets T09:00
        assertBracketIntervalWithNow(
                "[{lo=2026-01-15T09:00:00.000000Z, hi=2026-01-15T09:00:59.999999Z},{lo=2026-01-22T10:30:00.000000Z, hi=2026-01-22T10:30:00.000000Z}]",
                "[$now, 2026-01-15]T09:00",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableNowWithGlobalTimezone() throws SqlException {
        // [$now]@America/New_York - 10:30 in NY = 15:30 UTC (point-in-time)
        assertBracketIntervalWithNow(
                "[{lo=2026-01-22T15:30:00.000000Z, hi=2026-01-22T15:30:00.000000Z}]",
                "[$now]@America/New_York",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableNowWithPerElementTimezone() throws SqlException {
        // [$now@Europe/London, 2026-01-15@America/New_York] - per-element timezones
        // $now 10:30 in London = 10:30 UTC (no DST in Jan, point-in-time); 2026-01-15 in NY
        assertBracketIntervalWithNow(
                "[{lo=2026-01-15T05:00:00.000000Z, hi=2026-01-16T04:59:59.999999Z},{lo=2026-01-22T10:30:00.000000Z, hi=2026-01-22T10:30:00.000000Z}]",
                "[$now@Europe/London, 2026-01-15@America/New_York]",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableNumberOverflow() {
        // Number too large for int
        assertBracketIntervalError("[$today + 99999999999d]", "Invalid number in date expression");
    }

    @Test
    public void testDateVariablePointInTimeRangeSuffixErrorOrder() throws SqlException {
        // The parser checks the text after the first ';' of a range between two points in time
        // before the timezone, as it does on a day, and after the order of the range: a range that
        // starts after it ends fails on its order under the range order check, at its '..', and on
        // the text without that check, whatever "now" makes it start after it ends.
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final long ten = timestampDriver.parseFloorLiteral("2024-01-02T10:00:00.000000Z");
        final long three = timestampDriver.parseFloorLiteral("2024-01-02T03:00:00.000000Z");
        final String rangeOrderError = "Invalid date range: start is after end";

        // the text before the timezone, as on a day, while the IR parses the timezone first
        assertParseFailsAtEveryCheckLevel("[$today]@Bogus/Zone;1x", ten, "Invalid duration unit: x");
        assertParseFailsAtEveryCheckLevel("[$now - 1h..$now]@Bogus/Zone;1x", ten, "Invalid duration unit: x");
        assertCompileError("[$now - 1h..$now]@Bogus/Zone;1x", "invalid timezone: Bogus/Zone");
        // an element with its own timezone, which the IR does not represent
        assertParseFailsAtEveryCheckLevel("[$now - 1h..$now@Bogus/Zone];1x", ten, "Invalid duration unit: x");
        assertFailsLikeBindVariable("[$now - 1h..$now@Bogus/Zone];1x", ten, "Invalid duration unit: x");

        // a range that starts after it ends at every "now"
        assertParseFails("[$now..$now - 1h];1x", ten, IntervalUtils.TICK_CHECK_ALL, 11, rangeOrderError);
        assertParseFails("[$now..$now - 1h];1x", ten, IntervalUtils.TICK_CHECK_INTERVAL_LIMIT, 7, "Invalid duration unit: x");
        assertParseFails("[$now..$now - 1h];1x", ten, 0, 7, "Invalid duration unit: x");
        assertValidateFailsWith("[$now..$now - 1h];1x", ten, "Invalid duration unit: x");

        // one that starts after it ends at 03:00, and not at 10:00: SQL validates a literal against
        // the time it compiles at, and runs it at another
        assertParseFails("[$today + 570m..$now];1x", three, IntervalUtils.TICK_CHECK_ALL, 20, rangeOrderError);
        assertParseFails("[$today + 570m..$now];1x", three, IntervalUtils.TICK_CHECK_INTERVAL_LIMIT, 7, "Invalid duration unit: x");
        assertParseFails("[$today + 570m..$now];1x", three, 0, 7, "Invalid duration unit: x");
        assertValidateFailsWith("[$today + 570m..$now];1x", three, "Invalid duration unit: x");
        assertParseFailsAtEveryCheckLevel("[$today + 570m..$now];1x", ten, "Invalid duration unit: x");
        assertValidateFailsWith("[$today + 570m..$now];1x", ten, "Invalid duration unit: x");

        // without the range order check, a range of days that starts after it ends adds no
        // interval, and the next element checks the text that the list shares
        assertParseFails("[$tomorrow..$today, $now - 1h..$now];1x", ten, IntervalUtils.TICK_CHECK_ALL, 16, rangeOrderError);
        assertParseFails("[$tomorrow..$today, $now - 1h..$now];1x", ten, IntervalUtils.TICK_CHECK_INTERVAL_LIMIT, 7, "Invalid duration unit: x");
        assertParseFails("[$tomorrow..$today, $now - 1h..$now];1x", ten, 0, 7, "Invalid duration unit: x");

        // the case of the variables and of the unit
        assertParseFailsAtEveryCheckLevel("$NOW - 1h..$NOW;1H", ten, "Invalid duration unit: H");
        assertParseFailsAtEveryCheckLevel("[$NOW - 1h..$Now + 1h];1h#Tue", ten, "Expected number before unit '#'");
    }

    @Test
    public void testDateVariablePointInTimeRangeSuffixFailsAsAfterADay() throws SqlException {
        // A range between two points in time is a single interval, which ignores the text after
        // the first ';' of the list. The parser, which SQL uses for a bind variable and for a
        // literal that the IR does not represent, read that text only where a day takes it, so it
        // selected the range where the text does not parse. It fails there now with the error that
        // a day gives, at every check level, and keeps what the caller appended before. The
        // compiled form rejects it too: building the IR fails, or the form that parses on every
        // evaluation fails as the SQL that holds it is validated.
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final long now = timestampDriver.parseFloorLiteral("2024-01-02T10:00:00.000000Z");
        // the text, the error of the parser, and the error of building the IR, which reads all of
        // the text as a duration, or null when the IR takes each of its units for one and leaves
        // the expression to the parser on every evaluation
        final String[][] suffixes = {
                // a unit, or another character, where a number goes
                {"h", "Expected number before unit 'h'", "Expected number before unit 'h'"},
                {"1h#Tue", "Expected number before unit '#'", "Expected number before unit '#'"},
                {"1h@UTC", "Expected number before unit '@'", "Expected number before unit '@'"},
                {"1h ", "Expected number before unit ' '", "Expected number before unit ' '"},
                {" 1h", "Expected number before unit ' '", "Expected number before unit ' '"},
                {"1h]", "Expected number before unit ']'", "Expected number before unit ']'"},
                {"1h[", "Expected number before unit '['", "Expected number before unit '['"},
                {"1h\u00e9", "Expected number before unit '\u00e9'", "Expected number before unit '\u00e9'"},
                // a number, or a sign, without a unit
                {"1h-", "Missing unit at end of duration", "Missing unit at end of duration"},
                {"1", "Missing unit at end of duration", "Missing unit at end of duration"},
                {"-", "Missing unit at end of duration", "Missing unit at end of duration"},
                {"+", "Missing unit at end of duration", "Missing unit at end of duration"},
                // a number that does not fit an int, or is not one
                {"99999999999h", "Duration not a number: 99999999999", "Duration not a number: 99999999999"},
                {"2147483648h", "Duration not a number: 2147483648", "Duration not a number: 2147483648"},
                {"_h", "Duration not a number: _", "Duration not a number: _"},
                {"1__h", "Duration not a number: 1__", "Duration not a number: 1__"},
                {"--1h", "Duration not a number: -", "Duration not a number: -"},
                // a unit that the timestamp drivers do not add, the first such unit; the IR reports
                // a later error that it finds
                {"1x", "Invalid duration unit: x", null},
                {"1H", "Invalid duration unit: H", null},
                {"2D", "Invalid duration unit: D", null},
                {"1h1x", "Invalid duration unit: x", null},
                {"1x1", "Invalid duration unit: x", "Missing unit at end of duration"},
                {"1x-", "Invalid duration unit: x", "Missing unit at end of duration"},
                {"1 h", "Invalid duration unit:  ", "Expected number before unit 'h'"},
                {"1\u00e9", "Invalid duration unit: \u00e9", null},
                // a character of 4 UTF-8 bytes is two Java chars, the first one takes the place of
                // the unit
                {"1\uD83D\uDE00", "Invalid duration unit: \uD83D", "Expected number before unit '\uDE00'"},
                // one ';' more, which the parser shows from the first ';' on
                {"1h;1d", "Invalid interval format: ;1h;1d", "Expected number before unit ';'"},
                {"1h;", "Invalid interval format: ;1h;", "Expected number before unit ';'"},
                {";", "Invalid interval format: ;;", "Expected number before unit ';'"},
                {"1;", "Invalid interval format: ;1;", null},
                // three or more
                {"1h;1d;3;4", "Invalid interval format (too many semicolons): ;1h;1d;3;4", "Expected number before unit ';'"},
                {"1h;1d;3;", "Invalid interval format (too many semicolons): ;1h;1d;3;", "Expected number before unit ';'"},
                {";;;", "Invalid interval format (too many semicolons): ;;;;", "Expected number before unit ';'"},
                // two: a duration, a period and a count, which the parser checks in this order:
                // the number of the period
                {"1h;d;3", "Period not a number", "Expected number before unit ';'"},
                {"1h;xd;3", "Period not a number", "Expected number before unit ';'"},
                {"1h;;3", "Period not a number", "Expected number before unit ';'"},
                {"1h;;", "Period not a number", "Expected number before unit ';'"},
                {";;", "Period not a number", "Expected number before unit ';'"},
                {"1h;99999999999d;3", "Period not a number", "Expected number before unit ';'"},
                {"1h;2147483648d;3", "Period not a number", "Expected number before unit ';'"},
                {"1x;xd;0", "Period not a number", "Expected number before unit ';'"},
                // the count
                {"1h;1d;x", "Count not a number", "Expected number before unit ';'"},
                {"1h;1d;", "Count not a number", "Expected number before unit ';'"},
                {"1h;1d;3x", "Count not a number", "Expected number before unit ';'"},
                {"1h;1d;99999999999", "Count not a number", "Expected number before unit ';'"},
                {"1h;1d;0", "Count must be positive", "Expected number before unit ';'"},
                {"1h;1d;-1", "Count must be positive", "Expected number before unit ';'"},
                {"x;1d;0", "Count must be positive", "Expected number before unit 'x'"},
                {"1x;1d;0", "Count must be positive", "Expected number before unit ';'"},
                // the duration
                {"1x;1d;3", "Invalid duration unit: x", "Expected number before unit ';'"},
                {"1x;1q;3", "Invalid duration unit: x", "Expected number before unit ';'"},
                {"1h#Tue;1d;3", "Expected number before unit '#'", "Expected number before unit '#'"},
                {"1h-;1d;3", "Missing unit at end of duration", "Duration not a number: -"},
                {"1;1d;3", "Missing unit at end of duration", "Expected number before unit ';'"},
                // the unit of the period
                {"1h;1x;3", "Unknown period: x at 1", "Expected number before unit ';'"},
                {"1h;1w;3", "Unknown period: w at 1", "Expected number before unit ';'"},
                {"1h;1n;3", "Unknown period: n at 1", "Expected number before unit ';'"},
                {"1h;1D;3", "Unknown period: D at 1", "Expected number before unit ';'"}
        };
        for (String[] suffix : suffixes) {
            // the reference: the same text after a point in time and after a day, where the parser
            // applies it, with the date that it shows aside
            assertReferenceSuffixFails("$now;" + suffix[0], now, forTimestampType("2024-01-02T10:00:00.000000Z"), suffix[1]);
            assertReferenceSuffixFails("[$today];" + suffix[0], now, "2024-01-02", suffix[1]);
            for (PointInTimeRange range : POINT_IN_TIME_RANGES) {
                final String expression = range.expression().replace("?", suffix[0]);
                assertParseFailsAtEveryCheckLevel(expression, now, suffix[1]);
                if (range.hasIr() && suffix[2] != null) {
                    assertCompileError(expression, suffix[2]);
                } else {
                    assertFailsLikeBindVariable(expression, now, suffix[1]);
                }
            }
        }
    }

    @Test
    public void testDateVariablePointInTimeRangeWellFormedSuffixKeepsRange() throws SqlException {
        // The parser checks the text after the first ';' of a range between two points in time
        // with the grammar that it applies after a day, and applies nothing: every text that a day
        // takes leaves the range as it was, at every check level, and the compiled form keeps it as
        // well. The check adds no duration to a timestamp, so a duration that takes the start of
        // the range, its end, or zero to the null timestamp passes, where adding it fails.
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final long now = timestampDriver.parseFloorLiteral("2024-01-02T10:00:00.000000Z");
        final long start = timestampDriver.parseFloorLiteral("2024-01-02T09:00:00.000000Z");
        final String nullUnit = ColumnType.isTimestampNano(timestampType.getTimestampType()) ? "n" : "u";
        assertParseFails("[$now - 1h];" + durationToNull(start), now, IntervalUtils.TICK_CHECK_ALL, 7, "Invalid duration unit: " + nullUnit);
        assertParseFails("$now;" + durationToNull(now), now, IntervalUtils.TICK_CHECK_ALL, 7, "Invalid duration unit: " + nullUnit);
        assertParseFails("[1970-01-01];" + durationToNull(0), now, IntervalUtils.TICK_CHECK_ALL, 7, "Invalid duration unit: " + nullUnit);
        final String[] durations = {
                "", "1h", "1h30m", "+1h", "-1h", "1_000h", "0h", "2147483647y", "-2147483648h", "1u", "1T", "1s", "1m",
                "1d", "1w", "1M", "1y", "1n", "1000n", "1h".repeat(100_000), durationToNull(start), durationToNull(now),
                durationToNull(0)
        };
        for (String duration : durations) {
            for (PointInTimeRange range : POINT_IN_TIME_RANGES) {
                final String expression = range.expression().replace("?", duration);
                final String expected = forTimestampType(range.intervals());
                assertParsedAtEveryCheckLevel(expected, expression, now);
                assertCompiledEvaluatesTo(expected, expression, now, range.hasIr());
            }
        }
        // The repeating interval of the parser, a duration, a period and a count, repeats nothing
        // after such a range, with every count that a day takes, from 1 to the largest int. The IR
        // rejects it, and the form that parses on every evaluation takes it, as for every other
        // element.
        final String[] repeatingSuffixes = {
                "1h;1d;3", ";1d;3", "1h;1_0d;3", "1h;+1d;3", "1h;-1d;3", "1h;1d;+3", "1h;1d;3_0", "1h;1M;2", "1h;1y;2",
                "1h;1s;2", "1h;1m;2", "1h;1h;2", "1h;0d;3", "1h;1d;1", "1h;1s;2147483647"
        };
        for (String suffix : repeatingSuffixes) {
            for (PointInTimeRange range : POINT_IN_TIME_RANGES) {
                final String expression = range.expression().replace("?", suffix);
                final String expected = forTimestampType(range.intervals());
                assertParsedAtEveryCheckLevel(expected, expression, now);
                if (range.hasIr()) {
                    assertCompileError(expression, "Expected number before unit ';'");
                } else {
                    assertCompiledEvaluatesTo(expected, expression, now, false);
                }
            }
        }
        // a range that starts after it ends adds no interval without the range order check
        final long three = timestampDriver.parseFloorLiteral("2024-01-02T03:00:00.000000Z");
        final String rangeOrderError = "Invalid date range: start is after end";
        for (String[] suffixes : new String[][]{durations, repeatingSuffixes}) {
            for (String suffix : suffixes) {
                assertParseFails("[$now..$now - 1h];" + suffix, now, IntervalUtils.TICK_CHECK_ALL, 11, rangeOrderError);
                assertParsed("[]", "[$now..$now - 1h];" + suffix, now, IntervalUtils.TICK_CHECK_INTERVAL_LIMIT);
                assertParsed("[]", "[$now..$now - 1h];" + suffix, now, 0);
                assertParseFails("[$today + 570m..$now];" + suffix, three, IntervalUtils.TICK_CHECK_ALL, 20, rangeOrderError);
                assertParsed("[]", "[$today + 570m..$now];" + suffix, three, IntervalUtils.TICK_CHECK_INTERVAL_LIMIT);
                assertParsed("[]", "[$today + 570m..$now];" + suffix, three, 0);
            }
        }
    }

    @Test
    public void testDateVariablePointInTimeRangeWithMalformedDurationFails() {
        // A range between two points in time stays one interval, which ignores the duration of the
        // expression, but a malformed duration still fails the expression, the same as for a range of
        // days. The parser, which SQL uses for a bind variable, must not let it through where the
        // compiled form, which SQL uses for a literal, rejects it. The day filter goes before the
        // duration, so ';1h#Tue' is a malformed duration.
        final String now = "2024-01-02T10:00:00.000000Z";
        assertParsedAndCompiledRejectedWithNow("$now - 1h..$now;1h#Tue", now, "Expected number before unit '#'");
        assertParsedAndCompiledRejectedWithNow("$today+2h..$now;1h#Tue", now, "Expected number before unit '#'");
        assertParsedAndCompiledRejectedWithNow("[$now - 1h..$now];1h#Tue", now, "Expected number before unit '#'");
        // both paths let these through
        assertParsedAndCompiledRejectedWithNow("$now - 1h..$now;1x", now, "Invalid duration unit: x");
        assertParsedAndCompiledRejectedWithNow("[$now - 1h..$now];1x", now, "Invalid duration unit: x");
        // a range of days applies the duration, which fails on both paths
        assertParsedAndCompiledRejectedWithNow("$today..$tomorrow;1h#Tue", now, "Expected number before unit '#'");
    }

    @Test
    public void testDateVariablePointInTimeTimePartErrorOrder() throws SqlException {
        // An element that keeps its own time checks the time part of the list where a day reads it:
        // after the day filter of the element, before the timezone of the element and the one of
        // the list, and before the text after the first ';'. A range between two points in time
        // checks it after its order, under the range order check, and before its duration.
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final long now = timestampDriver.parseFloorLiteral("2024-01-02T10:00:00.000000Z");
        final String timeError = "Invalid date: 2024-01-02T25:00";
        // the expression, the same text after a day, the reference, and the error of both
        final String[][] cases = {
                {"[$now#Bogus]T25:00", "[$today#Bogus]T25:00", "Invalid day name: Bogus"},
                {"[$now@Bogus/Zone]T25:00", "[$today@Bogus/Zone]T25:00", timeError},
                {"[$now]T25:00@Bogus/Zone", "[$today]T25:00@Bogus/Zone", timeError},
                {"[$now]T25:00;1x", "[$today]T25:00;1x", timeError},
                {"[$now]T[25:00]@+02:00;1h#Tue", "[$today]T[25:00]@+02:00;1h#Tue", timeError},
                {"[$now - 1h..$now@Bogus/Zone]T25:00", "[$today..$tomorrow@Bogus/Zone]T25:00", timeError},
                {"[$now - 1h..$now]T25:00@Bogus/Zone", "[$today..$tomorrow]T25:00@Bogus/Zone", timeError},
                {"[$now - 1h..$now]T25:00;1x", "[$today..$tomorrow]T25:00;1x", timeError},
                {"$now - 1h..$nowT[25:00];1h#Tue", "$today..$tomorrowT[25:00];1h#Tue", timeError}
        };
        for (String[] c : cases) {
            assertParseFailsAtEveryCheckLevel(c[1], now, c[2]);
            assertParseFailsAtEveryCheckLevel(c[0], now, c[2]);
        }
        // a range that starts after it ends fails on its order first under the range order check,
        // at its '..', and on the time part without that check, whatever "now" makes it start after
        // it ends: SQL validates a literal against the time it compiles at, and runs it at another
        assertParseFails("[$now..$now - 1h]T25:00", now, IntervalUtils.TICK_CHECK_ALL, 11, RANGE_ORDER_ERROR);
        assertParseFails("[$now..$now - 1h]T25:00", now, IntervalUtils.TICK_CHECK_INTERVAL_LIMIT, 7, timeError);
        assertParseFails("[$now..$now - 1h]T25:00", now, 0, 7, timeError);
        assertValidateFailsWith("[$now..$now - 1h@+00:00]T[25:00]", now, timeError);
        final long three = timestampDriver.parseFloorLiteral("2024-01-02T03:00:00.000000Z");
        assertParseFails("[$today + 570m..$now]T25:00", three, IntervalUtils.TICK_CHECK_ALL, 20, RANGE_ORDER_ERROR);
        assertParseFails("[$today + 570m..$now]T25:00", three, IntervalUtils.TICK_CHECK_INTERVAL_LIMIT, 7, timeError);
        assertParseFails("[$today + 570m..$now]T25:00", three, 0, 7, timeError);
        assertValidateFailsWith("[$today + 570m..$now@+00:00]T[25:00]", three, timeError);
        // where the text after the first ';' is a repeating interval with a count that does not
        // parse, a day reports the count, which it reads before its time, and the element that
        // keeps its own time reports the time part, which it checks first
        assertParseFailsAtEveryCheckLevel("[$today]T25:00;1h;1d;x", now, "Count not a number");
        assertParseFailsAtEveryCheckLevel("[$now]T25:00;1h;1d;x", now, timeError);
        assertParseFailsAtEveryCheckLevel("[$now - 1h..$now]T25:00;1h;1d;x", now, timeError);
    }

    @Test
    public void testDateVariablePointInTimeTimePartFailsAsAfterADay() throws SqlException {
        // An element that keeps its own time, a point in time or a range between two points in
        // time, ignores the time part of the list. The parser, which SQL uses for a bind variable
        // and for a literal that the IR does not represent, skipped that time part without reading
        // it, so it selected the element where the time part does not parse, while the literal with
        // IR failed. The parser now fails there with the error that a day gives, on the date of the
        // element or of the start of the range, at every check level, and keeps what the caller
        // appended before. The compiled form rejects the expression too: building the IR fails, or
        // the form that parses on every evaluation fails as the SQL that holds it is validated, and
        // when it evaluates.
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final long now = timestampDriver.parseFloorLiteral("2024-01-02T10:00:00.000000Z");
        // the time part, the error of the parser after the day 2024-01-02, and the error of building
        // the IR, or null where the compiled form parses the expression on every evaluation
        final String[][] timeParts = {
                // a time that does not exist, or that is not one
                {"T25:00", "Invalid date: 2024-01-02T25:00", "Invalid time override: 25:00"},
                {"T09:99", "Invalid date: 2024-01-02T09:99", "Invalid time override: 09:99"},
                {"T10:00:61", "Invalid date: 2024-01-02T10:00:61", "Invalid time override: 10:00:61"},
                {"T1000", "Invalid date: 2024-01-02T1000", "Invalid time override: 1000"},
                {"T09:00T10:00", "Invalid date: 2024-01-02T09:00T10:00", "Invalid time override: 09:00T10:00"},
                {"T", "Invalid date: 2024-01-02T", "Invalid time override: T with no value"},
                {"Tabc", "Invalid date: 2024-01-02Tabc", "Invalid time override: abc"},
                {"T 10:00", "Invalid date: 2024-01-02T 10:00", "Invalid time override:  10:00"},
                {"T]", "Invalid date: 2024-01-02T]", "Invalid time override: ]"},
                {"TT09:00", "Invalid date: 2024-01-02TT09:00", "Invalid time override: T09:00"},
                // a time list with a slot that does not parse, or text after it
                {"T[25:00]", "Invalid date: 2024-01-02T25:00", "Invalid time in time list: 25:00"},
                {"T[09:00,25:00]", "Invalid date: 2024-01-02T25:00", "Invalid time in time list: 25:00"},
                {"T[10:00@Bogus/Zone]", "invalid timezone: Bogus/Zone", null},
                {"T[10:00]x", "Invalid date: 2024-01-02T10:00x", null},
                {"T[0900,1430]", "Military time format not supported in bracket expansion. Use colons: [09:00,14:30] instead of [0900,1430]", null},
                {"T[09:[00,30]]", "Nested brackets not supported in time list. Use separate expansions: T[09:00,09:30] instead of T[09:[00,30]]", null},
                // brackets that expand to a time that does not parse, to nothing, or do not close
                {"T[09..10]:99", "Invalid date: 2024-01-02T09:99", null},
                {"T[]", "Empty bracket expansion", null},
                {"T[10:00", "Unclosed '[' in interval", "Unclosed '[' in time list"},
                // the first ';' ends the time part, in brackets as well, so the bracket does not close
                {"T[09:00;1h]", "Unclosed '[' in interval", "Expected number before unit ']'"}
        };
        // elements that keep their own time, on 2024-01-02, and ranges between two points in time
        // that start on that day; ? stands for the time part
        final String[] shapes = {
                "[$now]?", "[$NOW - 1h]?", "[$today + 2h]?", "[$now - 1h..$now]?", "[$today + 2h..$NOW]?",
                // with a timezone or a day filter of the element, which the IR does not represent
                "[$now@+00:00]?", "[$now#Tue]?", "[$now - 1h..$now@+00:00]?",
                // a static element with a time of its own next to a date variable
                "[2024-01-02T09:00, $now]?",
                // under a timezone and a duration of the list
                "[$now]?@Europe/London;1h", "[$now - 1h..$now]?@+02:00;30m"
        };
        // bare date variables, which end where a 'T' starts a time with a digit or a bracket
        final String[] bareShapes = {"$now?", "$NOW - 1h?", "$now - 1h..$now?", "$today + 2h..$NOW?;1h"};
        for (String[] timePart : timeParts) {
            // the reference: the same time part after a day
            assertParseFailsAtEveryCheckLevel("[$today]" + timePart[0], now, timePart[1]);
            for (String shape : shapes) {
                assertParsedAndCompiledFail(shape.replace("?", timePart[0]), now, timePart[1], timePart[2]);
            }
            final char afterT = timePart[0].length() > 1 ? timePart[0].charAt(1) : ' ';
            if (Character.isDigit(afterT) || afterT == '[') {
                for (String shape : bareShapes) {
                    assertParsedAndCompiledFail(shape.replace("?", timePart[0]), now, timePart[1], timePart[2]);
                }
            }
        }
        // the time part goes after the date of the element, or of the start of the range
        assertParsedAndCompiledFail("[$now - 1d]T25:00", now, "Invalid date: 2024-01-01T25:00", "Invalid time override: 25:00");
        assertParsedAndCompiledFail("[$now + 14h]T[25:00]", now, "Invalid date: 2024-01-03T25:00", "Invalid time in time list: 25:00");
        assertParsedAndCompiledFail("[$yesterday + 30m..$now]T[25:00]", now, "Invalid date: 2024-01-01T25:00", "Invalid time in time list: 25:00");
        assertParsedAndCompiledFail("[$now..$now + 1d]T[25:00]", now, "Invalid date: 2024-01-02T25:00", "Invalid time in time list: 25:00");
        // a date before 1970
        final long beforeEpoch = timestampDriver.parseFloorLiteral("1969-12-31T23:30:00.000000Z");
        assertParsedAndCompiledFail("[$now]T25:00", beforeEpoch, "Invalid date: 1969-12-31T25:00", "Invalid time override: 25:00");
        assertParsedAndCompiledFail("[$now..$now + 1h]T[25:00]", beforeEpoch, "Invalid date: 1969-12-31T25:00", "Invalid time in time list: 25:00");
    }

    @Test
    public void testDateVariablePointInTimeWellFormedTimePartKeepsInterval() throws SqlException {
        // An element that keeps its own time ignores a time part of the list that parses: with every
        // time part that a day takes, the expression selects what it selects without one, at every
        // check level, and the compiled form evaluates to that as well, from its IR where it has
        // one. The parser checks the time part as it reads it after a day, but without the interval
        // limit, as the time part adds no interval: 1,440 separate seconds take a day past the
        // limit of 1,024 intervals, and not a point in time.
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final long now = timestampDriver.parseFloorLiteral("2024-01-02T10:00:00.000000Z");
        final String[] irTimeParts = {
                "T09:00", "T09", "T09:00:30", "T09:00:00.123456", "T[09:00]", "T[09:00,14:30]", "T[ 09:00 , 14:30 ]"
        };
        // time parts that the compiled form parses on every evaluation
        final String[] parsedTimeParts = {
                "T[09..10]:30", "T[01,02]:30", "T[10:30@+02:00]", "T[09:00@Europe/London,10:00]", "T[09:00,10:00]:[15,45]",
                "T[00..23]:[00..59]", "T[00..23]:[00..59]:00"
        };
        // time parts that the parser takes after a day, and the IR rejects, with its error
        final String[][] irRejectedTimeParts = {
                {"T09:00:00Z", "Invalid time override: 09:00:00Z"},
                {"T[09:00,]", "Empty element in time list"}
        };
        final OwnTimeElement[] elements = {
                new OwnTimeElement("[$now]?", "[{lo=2024-01-02T10:00:00.000000Z, hi=2024-01-02T10:00:00.000000Z}]", true),
                new OwnTimeElement("$now?", "[{lo=2024-01-02T10:00:00.000000Z, hi=2024-01-02T10:00:00.000000Z}]", true),
                new OwnTimeElement("[$NOW - 1h]?", "[{lo=2024-01-02T09:00:00.000000Z, hi=2024-01-02T09:00:00.000000Z}]", true),
                new OwnTimeElement("[$now - 1h..$now]?", NINE_TO_TEN, true),
                new OwnTimeElement("$now - 1h..$now?", NINE_TO_TEN, true),
                new OwnTimeElement("[$today + 2h..$NOW]?", "[{lo=2024-01-02T02:00:00.000000Z, hi=2024-01-02T10:00:00.000000Z}]", true),
                new OwnTimeElement("[$now@+00:00]?", "[{lo=2024-01-02T10:00:00.000000Z, hi=2024-01-02T10:00:00.000000Z}]", false),
                new OwnTimeElement("[$now#Tue]?", "[{lo=2024-01-02T10:00:00.000000Z, hi=2024-01-02T10:00:00.000000Z}]", false),
                // a static element with a time of its own, the minute of 10:00, which holds $now
                new OwnTimeElement("[2024-01-02T10:00, $now]?", "[{lo=2024-01-02T10:00:00.000000Z, hi=2024-01-02T10:00:59.999999Z}]", false)
        };
        for (OwnTimeElement element : elements) {
            final String expected = forTimestampType(element.intervals());
            assertParsedAtEveryCheckLevel(expected, element.expression().replace("?", ""), now);
            for (String timePart : irTimeParts) {
                final String expression = element.expression().replace("?", timePart);
                assertParsedAtEveryCheckLevel(expected, expression, now);
                assertCompiledEvaluatesTo(expected, expression, now, element.hasIr());
            }
            for (String timePart : parsedTimeParts) {
                final String expression = element.expression().replace("?", timePart);
                assertParsedAtEveryCheckLevel(expected, expression, now);
                assertCompiledEvaluatesTo(expected, expression, now, false);
            }
            for (String[] timePart : irRejectedTimeParts) {
                final String expression = element.expression().replace("?", timePart[0]);
                assertParsedAtEveryCheckLevel(expected, expression, now);
                if (element.hasIr()) {
                    assertCompileError(expression, timePart[1]);
                } else {
                    assertCompiledEvaluatesTo(expected, expression, now, false);
                }
            }
        }
        // the reference: a day takes the time parts that the IR rejects, and 1,440 separate seconds
        // take it past the interval limit
        assertParsedAtEveryCheckLevel(forTimestampType("[{lo=2024-01-02T09:00:00.000000Z, hi=2024-01-02T09:00:00.000000Z}]"), "[$today]T09:00:00Z", now);
        assertParsedAtEveryCheckLevel(forTimestampType("[{lo=2024-01-02T09:00:00.000000Z, hi=2024-01-02T09:00:59.999999Z}]"), "[$today]T[09:00,]", now);
        assertParseFails("[$today]T[00..23]:[00..59]:00", now, IntervalUtils.TICK_CHECK_ALL, 7, "Bracket expansion produces too many intervals (max 1024)");
    }

    @Test
    public void testDateVariablePointInTimeWithMalformedTimeFails() {
        // A point in time keeps its own time and ignores the time part of the expression, but a
        // malformed time part still fails the expression, the same as when a day takes it. The parser,
        // which SQL uses for a bind variable, must not let it through where the compiled form, which
        // SQL uses for a literal, rejects it.
        final String now = "2024-01-02T10:00:00.000000Z";
        assertParsedAndCompiledRejectedWithNow("[$now]T[25:00]", now, "25:00");
        assertParsedAndCompiledRejectedWithNow("[$now]T25:00", now, "25:00");
        assertParsedAndCompiledRejectedWithNow("$nowT25:00", now, "25:00");
        assertParsedAndCompiledRejectedWithNow("$nowT[25:00]", now, "25:00");
        assertParsedAndCompiledRejectedWithNow("$today+2hT[25:00]", now, "25:00");
        assertParsedAndCompiledRejectedWithNow("[$now - 1h..$now]T[25:00]", now, "25:00");
        assertParsedAndCompiledRejectedWithNow("[$now]T09:99", now, "09:99");
        // the compiled form parses this one at every evaluation, as the parser does a bind variable
        assertParsedAndCompiledRejectedWithNow("[$now]T[09..10]:99", now, "09:99");
        // a day takes the time part, which fails on both paths
        assertParsedAndCompiledRejectedWithNow("[$now, $today]T[25:00]", now, "25:00");
        assertParsedAndCompiledRejectedWithNow("[$today]T09:99", now, "09:99");
    }

    @Test
    public void testDateVariableRangeAddingNoIntervalElementSuffixErrorOrder() throws SqlException {
        // A date variable range that adds no interval fails on the order of its ends first under
        // the range order check, at its '..', and a range between two points in time checks the
        // text after the first ';' of the list before the suffix of its element, as when it adds
        // an interval. In a list, the first element that fails decides the error.
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final long ten = timestampDriver.parseFloorLiteral("2024-01-02T10:00:00.000000Z");
        final long three = timestampDriver.parseFloorLiteral("2024-01-02T03:00:00.000000Z");
        final int[] withoutRangeOrderCheck = {IntervalUtils.TICK_CHECK_INTERVAL_LIMIT, 0};

        assertParseFails("[$now..$now - 1h@Invalid/Zone];1x", ten, IntervalUtils.TICK_CHECK_ALL, 11, RANGE_ORDER_ERROR);
        for (int checks : withoutRangeOrderCheck) {
            assertParseFails("[$now..$now - 1h@Invalid/Zone];1x", ten, checks, 7, "Invalid duration unit: x");
        }
        assertParseFailsAtEveryCheckLevel("[$now - 1h..$now@Invalid/Zone];1x", ten, "Invalid duration unit: x");
        assertValidateFailsWith("[$now..$now - 1h@Invalid/Zone];1x", ten, "Invalid duration unit: x");
        assertParseFails("[$today + 570m..$now@Invalid/Zone];1x", three, IntervalUtils.TICK_CHECK_ALL, 20, RANGE_ORDER_ERROR);
        for (int checks : withoutRangeOrderCheck) {
            assertParseFails("[$today + 570m..$now@Invalid/Zone];1x", three, checks, 7, "Invalid duration unit: x");
            assertParseFails("[$today + 570m..$now@Invalid/Zone]", three, checks, 7, "invalid timezone: Invalid/Zone");
        }

        // a range of days that adds no interval fails on the suffix of its element, where a range
        // of days that adds intervals fails on the time part and the text after the first ';' of
        // the list first
        assertParseFails("[$tomorrow..$today@Invalid/Zone]", ten, IntervalUtils.TICK_CHECK_ALL, 16, RANGE_ORDER_ERROR);
        for (int checks : withoutRangeOrderCheck) {
            assertParseFails("[$tomorrow..$today@Invalid/Zone]", ten, checks, 7, "invalid timezone: Invalid/Zone");
            assertParseFails("[$tomorrow..$today@Invalid/Zone];1x", ten, checks, 7, "invalid timezone: Invalid/Zone");
            assertParseFails("[$tomorrow..$today@Invalid/Zone]T25:00", ten, checks, 7, "invalid timezone: Invalid/Zone");
        }
        assertParseFailsAtEveryCheckLevel("[$today..$tomorrow@Invalid/Zone];1x", ten, "Invalid duration unit: x");
        assertParseFailsAtEveryCheckLevel("[$today..$tomorrow@Invalid/Zone]T25:00", ten, "Invalid date: 2024-01-02T25:00");
        assertValidateFailsWith("[$tomorrow..$today@Invalid/Zone];1x", ten, "invalid timezone: Invalid/Zone");

        // the first element that fails decides
        assertParseFails("[$tomorrow..$today@Invalid/Zone, $today@Bogus/Zone]", ten, IntervalUtils.TICK_CHECK_ALL, 16, RANGE_ORDER_ERROR);
        for (int checks : withoutRangeOrderCheck) {
            assertParseFails("[$tomorrow..$today@Invalid/Zone, $today@Bogus/Zone]", ten, checks, 7, "invalid timezone: Invalid/Zone");
        }
        assertParseFailsAtEveryCheckLevel("[$today@Bogus/Zone, $tomorrow..$today@Invalid/Zone]", ten, "invalid timezone: Bogus/Zone");
        assertParseFails("[$today, $tomorrow..$today#Xyz, $today#Qqq]", ten, IntervalUtils.TICK_CHECK_ALL, 16, RANGE_ORDER_ERROR);
        for (int checks : withoutRangeOrderCheck) {
            assertParseFails("[$today, $tomorrow..$today#Xyz, $today#Qqq]", ten, checks, 7, "Invalid day name: Xyz");
        }
        assertValidateFailsWith("[$tomorrow..$today@Invalid/Zone, $today@Bogus/Zone]", ten, "invalid timezone: Invalid/Zone");

        // a time list whose every slot has a timezone of its own
        assertParseFails("[$tomorrow..$today@Invalid/Zone]T[09:00@UTC]", ten, IntervalUtils.TICK_CHECK_ALL, 16, RANGE_ORDER_ERROR);
        for (int checks : withoutRangeOrderCheck) {
            assertParseFails("[$tomorrow..$today@Invalid/Zone]T[09:00@UTC]", ten, checks, 7, "invalid timezone: Invalid/Zone");
        }

        // the letter case of the variables, the timezone and the day filter, and whitespace
        // around the parts of the element
        for (int checks : withoutRangeOrderCheck) {
            assertParseFails("[$TOMORROW..$Today@INVALID/ZONE]", ten, checks, 7, "invalid timezone: INVALID/ZONE");
            assertParseFails("[$TOMORROW..$Today#XYZ]", ten, checks, 7, "Invalid day name: XYZ");
            assertParseFails("[ $tomorrow .. $today @Invalid/Zone ]", ten, checks, 7, "invalid timezone: Invalid/Zone");
        }
        assertParseFails("[ $tomorrow .. $today @Invalid/Zone ]", ten, IntervalUtils.TICK_CHECK_ALL, 17, RANGE_ORDER_ERROR);
    }

    @Test
    public void testDateVariableRangeAddingNoIntervalFailsOnMalformedElementSuffix() throws SqlException {
        // A date variable range adds no interval when it starts after it ends, which the parser
        // lets through without the range order check, and when it is a range of business days
        // that holds no business day. A timezone or day filter of its element still has to parse:
        // the range fails on it with the error of a day of the range, at every check level that
        // lets the range through, whatever the time part, timezone and day filter of the list. The
        // compiled form parses such an expression on every evaluation, so it fails the same way as
        // the SQL that holds it is validated. The same suffix fails a range of the same kind that
        // adds intervals with the same error, apart from the date that a day shows: that of the
        // first day of the range, followed by the time part of the list where there is one.
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final long tuesday = timestampDriver.parseFloorLiteral("2024-01-02T10:00:00.000000Z");
        final String manyX = "x".repeat(100_000);
        // the suffix of the element, the error of a range of days, where ? stands for its first
        // day, and the error of a range between two points in time, where null leaves it out
        final String[][] suffixes = {
                // a timezone that does not parse
                {"@Invalid/Zone", "invalid timezone: Invalid/Zone", "invalid timezone: Invalid/Zone"},
                {"@", "invalid timezone: ", "invalid timezone: "},
                {"@ UTC", "invalid timezone:  UTC", "invalid timezone:  UTC"},
                {"@[UTC]", "invalid timezone: [UTC]", "invalid timezone: [UTC]"},
                {"@+25:00", "invalid timezone: +25:00", "invalid timezone: +25:00"},
                {"@\u00dcnknown", "invalid timezone: \u00dcnknown", "invalid timezone: \u00dcnknown"},
                {"@\uD83D\uDE00", "invalid timezone: \uD83D\uDE00", "invalid timezone: \uD83D\uDE00"},
                {"@Bogus/" + manyX, "invalid timezone: Bogus/" + manyX, "invalid timezone: Bogus/" + manyX},
                // a timezone ends at the day filter of its element, which a range parses first
                {"@Invalid/Zone#Mon", "invalid timezone: Invalid/Zone", "invalid timezone: Invalid/Zone"},
                {"@Invalid/Zone#Xyz", "Invalid day name: Xyz", "Invalid day name: Xyz"},
                {"#Mon@Invalid/Zone", "Invalid day name: Mon@Invalid/Zone", "Invalid day name: Mon@Invalid/Zone"},
                // a range of days takes the text before the last '@' for a part of its date
                {"@UTC@Invalid/Zone", "Invalid date: ?@UTC", "invalid timezone: Invalid/Zone"},
                {"@+02:00@UTC", "Invalid date: ?@+02:00", null},
                {"@Invalid/Zone@UTC", "Invalid date: ?@Invalid/Zone", null},
                // a day filter that does not parse
                {"#Xyz", "Invalid day name: Xyz", "Invalid day name: Xyz"},
                {"#", "Empty day filter after '#'", "Empty day filter after '#'"},
                {"# Mon", "Invalid day name:  Mon", "Invalid day name:  Mon"},
                {"#[Mon]", "Invalid day name: [Mon]", "Invalid day name: [Mon]"},
                {"#Mon#Tue", "Invalid day name: Mon#Tue", "Invalid day name: Mon#Tue"},
                {"#L\u00fcn", "Invalid day name: L\u00fcn", "Invalid day name: L\u00fcn"},
                // no tick calendar service knows it
                {"#XNYS", "Invalid day name: XNYS", "Invalid day name: XNYS"},
                {"#" + manyX, "Invalid day name: " + manyX, "Invalid day name: " + manyX},
                {"@+02:00#Xyz", "Invalid day name: Xyz", "Invalid day name: Xyz"},
                {"@UTC@UTC#Xyz", "Invalid day name: Xyz", "Invalid day name: Xyz"}
        };
        final String[] listSuffixes = {"", "T09:00", "T[09..10]:30", "T[09:00,10:00]", "@+00:00", "#Tue"};
        for (NoIntervalRange range : NO_INTERVAL_RANGES) {
            for (String[] suffix : suffixes) {
                final String error = range.isPointInTime() ? suffix[2] : suffix[1];
                if (error == null) {
                    continue;
                }
                for (String listSuffix : listSuffixes) {
                    final String expression = "[" + range.range() + suffix[0] + "]" + listSuffix;
                    assertNoIntervalRangeFails(range, expression, range.isPointInTime() ? error : error.replace("?", range.firstDay()));
                    if (listSuffix.indexOf('T') < 0 || error.indexOf('?') < 0) {
                        final String forward = "[" + range.forwardRange() + suffix[0] + "]" + listSuffix;
                        TestUtils.assertEquals(
                                forward,
                                "error: " + error.replace("?", "2024-01-02"),
                                parseWithChecks(timestampDriver, forward, tuesday, IntervalUtils.TICK_CHECK_ALL)
                        );
                    }
                }
            }
        }
    }

    @Test
    public void testDateVariableRangeAddingNoIntervalKeepsIntervalsOfEarlierElements() throws SqlException {
        // A date variable range that adds no interval parses the timezone of its element without
        // converting an interval with it: the element before it in the list keeps its whole day in
        // UTC, after a range of days and a range between two points in time that start after they
        // end, at the check levels that let them through, and after a range of business days that
        // holds no business day, at every check level
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final long tuesday = timestampDriver.parseFloorLiteral("2024-01-02T10:00:00.000000Z");
        final String tuesdayWholeDay = forTimestampType("[{lo=2024-01-02T00:00:00.000000Z, hi=2024-01-02T23:59:59.999999Z}]");
        for (int checks : new int[]{IntervalUtils.TICK_CHECK_INTERVAL_LIMIT, 0}) {
            assertParsed(tuesdayWholeDay, "[$today, $tomorrow..$today@+02:00]", tuesday, checks);
            assertParsed(tuesdayWholeDay, "[$today, $now..$now - 1h@+02:00]", tuesday, checks);
        }
        assertParsedAtEveryCheckLevel(
                forTimestampType("[{lo=2024-01-06T00:00:00.000000Z, hi=2024-01-06T23:59:59.999999Z}]"),
                "[$today, $today..$today + 0bd@+02:00]",
                timestampDriver.parseFloorLiteral("2024-01-06T10:00:00.000000Z")
        );
    }

    @Test
    public void testDateVariableRangeAddingNoIntervalKeepsWellFormedElementSuffix() throws SqlException {
        // A date variable range that adds no interval, with a timezone or day filter of its
        // element that parses, adds no interval at every check level that lets the range through,
        // whatever the time part, timezone and day filter of the list, and the other elements of
        // the list keep their intervals. The compiled form validates it as the SQL that holds it
        // compiles. A timezone name matches by its prefix, in any letter case.
        final String[] zones = {
                "", "@+02:00", "@-05:30", "@+14:00", "@-12:00", "@UTC", "@utc", "@Z", "@GMT+2", "@UTCx", "@Europe/London",
                "@europe/london", "@America/New_York", " @UTC"
        };
        final String[] dayFilters = {
                "#Mon", "#MON", "#mon", "#Monday", "#Tue", "#workday", "#WORKDAY", "#wd", "#weekend", "#Mon ", "@UTC#Mon", "@+02:00#Sat",
                "@Z#Mon", "@+05#Mon", "@UTC+05#Mon"
        };
        // the time part, timezone and day filter of the list, and the intervals that $today
        // selects with them at 2024-01-02T10:00Z, a Tuesday
        final String wholeDay = "[{lo=2024-01-02T00:00:00.000000Z, hi=2024-01-02T23:59:59.999999Z}]";
        final String[][] listSuffixes = {
                {"", wholeDay},
                {"T09:00", "[{lo=2024-01-02T09:00:00.000000Z, hi=2024-01-02T09:00:59.999999Z}]"},
                {"T[09..10]:30", "[{lo=2024-01-02T09:30:00.000000Z, hi=2024-01-02T09:30:59.999999Z},{lo=2024-01-02T10:30:00.000000Z, hi=2024-01-02T10:30:59.999999Z}]"},
                {"T[09:00,10:00]", "[{lo=2024-01-02T09:00:00.000000Z, hi=2024-01-02T09:00:59.999999Z},{lo=2024-01-02T10:00:00.000000Z, hi=2024-01-02T10:00:59.999999Z}]"},
                {"@+00:00", wholeDay},
                {"#Tue", wholeDay}
        };
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final long tuesday = timestampDriver.parseFloorLiteral("2024-01-02T10:00:00.000000Z");
        for (NoIntervalRange range : NO_INTERVAL_RANGES) {
            for (String[] suffixes : new String[][]{zones, dayFilters}) {
                for (String suffix : suffixes) {
                    for (String[] listSuffix : listSuffixes) {
                        assertNoIntervalRangeAddsNoInterval(range, "[" + range.range() + suffix + "]" + listSuffix[0]);
                    }
                }
            }
        }
        // next to another element
        for (String[] suffixes : new String[][]{zones, dayFilters}) {
            for (String suffix : suffixes) {
                for (String[] listSuffix : listSuffixes) {
                    for (String range : new String[]{"$tomorrow..$today", "$now..$now - 1h"}) {
                        final String expression = "[" + range + suffix + ", $today]" + listSuffix[0];
                        assertParseFails(expression, tuesday, IntervalUtils.TICK_CHECK_ALL, 7 + expression.indexOf("..") - 1, RANGE_ORDER_ERROR);
                        for (int checks : new int[]{IntervalUtils.TICK_CHECK_INTERVAL_LIMIT, 0}) {
                            assertParsed(forTimestampType(listSuffix[1]), expression, tuesday, checks);
                        }
                        try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(
                                timestampDriver, configuration, expression, 0, expression.length(), 7)) {
                            compiled.validate(tuesday);
                        }
                    }
                }
            }
        }
        // a range of business days that skips a weekend before its first day
        final long saturday = timestampDriver.parseFloorLiteral("2024-01-06T10:00:00.000000Z");
        assertParsedAtEveryCheckLevel(
                forTimestampType("[{lo=2024-01-08T00:00:00.000000Z, hi=2024-01-08T23:59:59.999999Z}]"),
                "[$today..$today + 1bd@+00:00]",
                saturday
        );
        assertParseFailsAtEveryCheckLevel("[$today..$today + 1bd@Bogus/Zone]", saturday, "invalid timezone: Bogus/Zone");
    }

    @Test
    public void testDateVariableRangeArithmeticBothSides() throws SqlException {
        // [$today+1bd..$today+5bd] produces 2 merged intervals (business days)
        // 2026-01-22 (Thu) + 1bd = Fri 23, + 5bd = Thu 29
        // Days: Fri 23, (skip Sat 24, Sun 25), Mon 26, Tue 27, Wed 28, Thu 29
        // Merged: [Jan 23] and [Jan 26-29]
        assertBracketIntervalWithNow(
                "[{lo=2026-01-23T00:00:00.000000Z, hi=2026-01-23T23:59:59.999999Z}," +
                        "{lo=2026-01-26T00:00:00.000000Z, hi=2026-01-29T23:59:59.999999Z}]",
                "[$today+1bd..$today+5bd]",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableRangeArithmeticBothSidesCalendar() throws SqlException {
        // [$today+1d..$today+5d] produces a merged interval (all calendar days)
        // 2026-01-22 + 1d = Jan 23, + 5d = Jan 27
        assertBracketIntervalWithNow(
                "[{lo=2026-01-23T00:00:00.000000Z, hi=2026-01-27T23:59:59.999999Z}]",
                "[$today+1d..$today+5d]",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableRangeBothNegativeArithmetic() throws SqlException {
        // Both expressions with negative arithmetic - range in the past
        assertBracketIntervalWithNow(
                "[{lo=2026-01-17T00:00:00.000000Z, hi=2026-01-20T23:59:59.999999Z}]",
                "[$today-5d..$today-2d]",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableRangeBusinessDays() throws SqlException {
        // [$today..$today+5bd] produces 2 merged intervals (weekdays only)
        // 2026-01-22 (Thu) + 5bd = 2026-01-29 (Thu), skipping Sat/Sun
        // Days: Thu 22, Fri 23, (skip Sat 24, Sun 25), Mon 26, Tue 27, Wed 28, Thu 29
        // Adjacent weekdays are merged: [Jan 22-23] and [Jan 26-29]
        assertBracketIntervalWithNow(
                "[{lo=2026-01-22T00:00:00.000000Z, hi=2026-01-23T23:59:59.999999Z}," +
                        "{lo=2026-01-26T00:00:00.000000Z, hi=2026-01-29T23:59:59.999999Z}]",
                "[$today..$today+5bd]",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableRangeBusinessDaysFromNow() throws SqlException {
        // https://github.com/questdb/questdb/issues/8083
        // $now starts the business-day range at the current moment, not at midnight.
        // 2024-01-02 is a Tuesday and $today + 3bd is Friday 2024-01-05, so no weekend
        // falls inside the range.
        assertParsedAndCompiledWithNow(
                "[{lo=2024-01-02T10:00:00.123400Z, hi=2024-01-05T23:59:59.999999Z}]",
                "$now..$today + 3bd",
                "2024-01-02T10:00:00.123400Z"
        );
    }

    @Test
    public void testDateVariableRangeBusinessDaysFromWeekend() throws SqlException {
        // Start from Saturday - business day range should still work
        // 2026-01-24 is Saturday, +3bd should skip Sat/Sun and give Mon 26, Tue 27, Wed 28
        assertBracketIntervalWithNow(
                "[{lo=2026-01-26T00:00:00.000000Z, hi=2026-01-28T23:59:59.999999Z}]",
                "[$today..$today+3bd]",
                "2026-01-24T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableRangeBusinessDaysOnlyWeekends() throws SqlException {
        // Range that spans only weekend days with business day mode
        // Sat to Sun with bd range = empty (no weekdays)
        // 2026-01-24 is Saturday, 2026-01-25 is Sunday
        // $today..$today+1bd from Saturday = Monday only (skips Sat, Sun)
        assertBracketIntervalWithNow(
                "[{lo=2026-01-26T00:00:00.000000Z, hi=2026-01-26T23:59:59.999999Z}]",
                "[$today..$today+1bd]",
                "2026-01-24T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableRangeBusinessDaysWithTrailingWhitespaceBeforeTimezone() throws SqlException {
        // Bug test: trailing whitespace before @ should not break business day detection
        // [$today..$today+5bd @+05:00] - the space before @ should be trimmed
        // 2026-01-22 (Thu) + 5bd = Thu 29, skipping Sat/Sun
        // Should produce 2 merged intervals: [Thu 22 - Fri 23] and [Mon 26 - Thu 29]
        // In UTC+5: 19:00 UTC previous day to 18:59:59 UTC
        assertBracketIntervalWithNow(
                "[{lo=2026-01-21T19:00:00.000000Z, hi=2026-01-23T18:59:59.999999Z}," +
                        "{lo=2026-01-25T19:00:00.000000Z, hi=2026-01-29T18:59:59.999999Z}]",
                "[$today..$today+5bd @+05:00]",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableRangeCalendarDays() throws SqlException {
        // [$today..$today+5d] produces a merged interval for all calendar days
        // 2026-01-22 (Thu) to 2026-01-27 (Tue) - adjacent full-day intervals are merged
        assertBracketIntervalWithNow(
                "[{lo=2026-01-22T00:00:00.000000Z, hi=2026-01-27T23:59:59.999999Z}]",
                "[$today..$today+5d]",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableRangeCaseInsensitive() throws SqlException {
        // Variable names and units should be case-insensitive
        assertBracketIntervalWithNow(
                "[{lo=2026-01-22T00:00:00.000000Z, hi=2026-01-24T23:59:59.999999Z}]",
                "[$TODAY..$TODAY+2D]",
                "2026-01-22T10:30:00.000000Z"
        );
        assertBracketIntervalWithNow(
                "[{lo=2026-01-22T00:00:00.000000Z, hi=2026-01-23T23:59:59.999999Z}," +
                        "{lo=2026-01-26T00:00:00.000000Z, hi=2026-01-26T23:59:59.999999Z}]",
                "[$Today..$Today+2BD]",
                "2026-01-22T10:30:00.000000Z"
        );
        // Mixed case for "bd" unit - covers all branches: bD and Bd
        assertBracketIntervalWithNow(
                "[{lo=2026-01-22T00:00:00.000000Z, hi=2026-01-23T23:59:59.999999Z}," +
                        "{lo=2026-01-26T00:00:00.000000Z, hi=2026-01-26T23:59:59.999999Z}]",
                "[$today..$today+2bD]",
                "2026-01-22T10:30:00.000000Z"
        );
        assertBracketIntervalWithNow(
                "[{lo=2026-01-22T00:00:00.000000Z, hi=2026-01-23T23:59:59.999999Z}," +
                        "{lo=2026-01-26T00:00:00.000000Z, hi=2026-01-26T23:59:59.999999Z}]",
                "[$today..$today+2Bd]",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableRangeCompactVsSpaced() throws SqlException {
        // Both compact ($today+5d) and spaced ($today + 5d) formats should work
        assertBracketIntervalWithNow(
                "[{lo=2026-01-22T00:00:00.000000Z, hi=2026-01-27T23:59:59.999999Z}]",
                "[$today..$today+5d]",
                "2026-01-22T10:30:00.000000Z"
        );
        assertBracketIntervalWithNow(
                "[{lo=2026-01-22T00:00:00.000000Z, hi=2026-01-27T23:59:59.999999Z}]",
                "[$today .. $today + 5d]",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableRangeDayFilterAppliesBeforeIncrementalMerge() throws SqlException {
        // The Mondays 2024-01-08 and 2024-01-15 at 00:00 and 18:00 for 12 hours. Each 18:00 interval
        // runs into Tuesday, and the Tuesday intervals must not survive a merge with it.
        final String mondays0018 = "[{lo=2024-01-08T00:00:00.000000Z, hi=2024-01-08T11:59:59.999999Z},"
                + "{lo=2024-01-08T18:00:00.000000Z, hi=2024-01-09T05:59:59.999999Z},"
                + "{lo=2024-01-15T00:00:00.000000Z, hi=2024-01-15T11:59:59.999999Z},"
                + "{lo=2024-01-15T18:00:00.000000Z, hi=2024-01-16T05:59:59.999999Z}]";
        // the same Mondays at 00:00, 06:00, 12:00 and 18:00 for 12 hours: one block per Monday
        final String mondays00061218 = "[{lo=2024-01-08T00:00:00.000000Z, hi=2024-01-09T05:59:59.999999Z},"
                + "{lo=2024-01-15T00:00:00.000000Z, hi=2024-01-16T05:59:59.999999Z}]";
        for (CairoConfiguration cfg : new CairoConfiguration[]{mergeThreshold2Configuration, mergeThreshold4Configuration, configuration}) {
            assertTickExprAtMonday(cfg, mondays0018, "[$today..$today+8d]T[00,18]:00#mon;12h");
            assertTickExprAtMonday(cfg, mondays0018, "[$today..$today+8d#mon]T[00,18]:00;12h");
            assertTickExprAtMonday(cfg, mondays00061218, "[$today..$today+8d]T[00,06,12,18]:00#mon;12h");
            assertTickExprAtMonday(cfg, mondays00061218, "[$today..$today+8d#mon]T[00,06,12,18]:00;12h");
        }
    }

    @Test
    public void testDateVariableRangeDayFilterInStartExpression() {
        // '#' is parsed as day filter marker, so "workday..$tomorrow" becomes the day filter value
        assertBracketIntervalError("[$today#workday..$tomorrow]", "Invalid day name: workday..$tomorrow");
    }

    @Test
    public void testDateVariableRangeDayStartTimeEnd() throws SqlException {
        // https://github.com/questdb/questdb/issues/8083
        // $today starts the range at midnight and $now ends it at the current moment,
        // so the range stops at $now instead of covering the rest of the day
        assertParsedAndCompiledWithNow(
                "[{lo=2024-01-02T00:00:00.000000Z, hi=2024-01-02T10:00:00.123400Z}]",
                "$today..$now",
                "2024-01-02T10:00:00.123400Z"
        );
    }

    @Test
    public void testDateVariableRangeDroppedByElementFilterActsAsAbsent() throws SqlException {
        // A range between two points in time that the day filter of its element drops adds nothing
        // to the list, which parses as it does without that range, at every check level: the
        // intervals of the other elements no longer merge into the range, so they reach the
        // interval limit where they exceed it. 2025-01-19 and 2025-03-02 are Sundays.
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final String slots = "]T[00..23]:[00..59]:00";
        assertParsedAsWithoutElement(
                configuration,
                "[$now - 1d..$now#Mon, $today, $now - 1d..$now" + slots,
                "[$today, $now - 1d..$now" + slots,
                timestampDriver.parseFloorLiteral("2025-01-19T12:00:00.000000Z"),
                "Bracket expansion produces too many intervals (max 1024)"
        );
        assertParsedAsWithoutElement(
                intervalLimitConfiguration(8, 16),
                "[$now - 140d..$now#Mon, $today - 140d..$today#Tue, $today - 140d..$today]",
                "[$today - 140d..$today#Tue, $today - 140d..$today]",
                timestampDriver.parseFloorLiteral("2025-03-02T12:00:00.000000Z"),
                "Date range expansion produces too many intervals (max 16)"
        );
    }

    @Test
    public void testDateVariableRangeDayFilterOverMaxIntervalsMessage() throws SqlException {
        // About 715 Mondays, two 1-hour intervals each. The check after each day of the range runs
        // before the bracket expansion of a later day reaches its own check.
        assertTickExprErrorAtMonday("[$today..$today+5000d]T[00,12]:00#mon;1h", "Date range expansion produces too many intervals (max 1024)");
        assertTickExprErrorAtMonday("[$today..$today+5000d#mon]T[00,12]:00;1h", "Date range expansion produces too many intervals (max 1024)");
        // control: one interval per Monday fits the cap
        out.clear();
        parseTickExprAtMonday(configuration, out, "[$today..$today+5000d]#mon");
        assertFullDayMondays(out, countMondays(LocalDate.of(2024, 1, 8), LocalDate.of(2024, 1, 8).plusDays(5_000)));
    }

    @Test
    public void testDateVariableRangeElementDayFilter() throws SqlException {
        // the Monday 2024-01-08 of the six days from $today
        final String monday = "[{lo=2024-01-08T00:00:00.000000Z, hi=2024-01-08T23:59:59.999999Z}]";
        assertTickExprAtMonday(configuration, monday, "[$today..$today+5d#mon]");
        assertTickExprAtMonday(configuration, monday, "[$today..$today+5d]#mon");
    }

    @Test
    public void testDateVariableRangeElementDayFilterOverridesGlobal() throws SqlException {
        // the element filter overrides the global one, as in testDateListMixedPerElementAndGlobalDayFilter
        assertTickExprAtMonday(
                configuration,
                "[{lo=2024-01-08T00:00:00.000000Z, hi=2024-01-08T23:59:59.999999Z}]",
                "[$today..$today+5d#mon]#tue"
        );
    }

    @Test
    public void testDateVariableRangeElementSuffixMatchesListSuffixFuzz() throws SqlException {
        // A date variable range of either kind with a timezone and a day filter of its element
        // selects, among other elements, what the range selects alone with the same suffix on the
        // list, see ElementSuffixChecker. questdb.fuzz.tickExpressions sets the number of
        // expressions.
        final int expressionCount = Integer.getInteger("questdb.fuzz.tickExpressions", 1_000);
        final String[] dayFilters = new String[DAY_FILTERS.length + 1];
        dayFilters[0] = "";
        for (int i = 0; i < DAY_FILTERS.length; i++) {
            dayFilters[i + 1] = "#" + DAY_FILTERS[i].text();
        }
        final String[] durations = {"", ";1h", ";30m", ";0s", ";-30m", ";1d"};
        // FUZZ_NOWS, and "now" just after midnight, before 1970 too, and an hour before it
        final String[] nows = Arrays.copyOf(FUZZ_NOWS, FUZZ_NOWS.length + 3);
        nows[FUZZ_NOWS.length] = "2025-01-20T01:00:00.000000Z";
        nows[FUZZ_NOWS.length + 1] = "1969-12-29T01:00:00.000000Z";
        nows[FUZZ_NOWS.length + 2] = "2025-01-19T23:30:00.000000Z";
        final ElementSuffixChecker checker = new ElementSuffixChecker(timestampType.getDriver(), configuration, dayFilters, durations, durations, nows);
        final Rnd rnd = TestUtils.generateRandom(LOG);
        for (int i = 0; i < expressionCount; i++) {
            checker.check(rnd);
        }
        checker.log("day filter");
        if (expressionCount >= 1_000) {
            checker.assertEveryShape();
            // a day filter after a timezone that reads as another zone, or fails, where it runs
            // on into the day filter, such as UTC+05 or Z
            Assert.assertTrue(checker.zoneBeforeSuffixCount > 0);
            Assert.assertTrue(checker.startDayIsNotEndDayCount > 0);
        }
    }

    @Test
    public void testDateVariableRangeElementZoneEndsAtDayFilter() throws SqlException {
        // The timezone of a date variable range element ends at the day filter of the element, as
        // the timezone of a single element and the one of the list do: [R@Z#F] selects what
        // [R@Z]#F and [R]@Z#F select, for both kinds of range, with and without a time part of the
        // list, at every check level, and the compiled forms agree. Dates.parseOffset() and
        // DateLocale.matchZone() take a prefix of their text, so a timezone that ran on into the
        // day filter took UTC+05 and GMT+05 for UTC and GMT, and failed on Z and +05. 2025-01-19 is
        // a Sunday, and London springs forward at 01:00Z on 2024-03-31.
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final String[] ranges = {"$now - 2h..$now", "$today..$tomorrow", "$today..$now", "$now - 2h..$tomorrow", "$today + 570m..$today + 16h"};
        final String[] zones = {
                "UTC+05", "utc+05", "GMT+05", "gmt-05", "UTC-05", "+05", "-05", "05", "+0500", "+05:00", "UTC+05:00", "Z", "z",
                "UTC", "UTCx", "Europe/London", "America/New_York"
        };
        final String[] dayFilters = {"#Sun", "#Mon", "#weekend"};
        // the last two time lists have a slot with a timezone of its own
        final String[] times = {"", "T09:00", "T[09:00,10:00]", "T[09..10]:00", "T[09:00@+03:00,10:00]", "T[11:00@UTC,10:00]"};
        for (String nowText : new String[]{"2025-01-19T12:00:00.000000Z", "2024-03-31T00:30:00.000000Z"}) {
            final long now = timestampDriver.parseFloorLiteral(nowText);
            for (String range : ranges) {
                for (String zone : zones) {
                    for (String dayFilter : dayFilters) {
                        for (String time : times) {
                            final String list = "[" + range + "]" + time + "@" + zone + dayFilter;
                            final String expected = parseWithChecks(timestampDriver, list, now, IntervalUtils.TICK_CHECK_ALL);
                            Assert.assertFalse(list + " at " + nowText + ": " + expected, expected.startsWith("error: "));
                            final String element = "[" + range + "@" + zone + dayFilter + "]" + time;
                            assertParsedAtEveryCheckLevel(expected, element, now);
                            assertParsed(expected, "[" + range + "@" + zone + "]" + time + dayFilter, now, IntervalUtils.TICK_CHECK_ALL);
                            assertCompiledEvaluatesTo(expected, element, now, false);
                            try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(timestampDriver, configuration, list, 0, list.length(), 7)) {
                                out.clear();
                                compiled.evaluate(out, now);
                                TestUtils.assertEquals(list + " at " + nowText, expected, intervalToString(timestampDriver, out));
                            }
                        }
                    }
                }
            }
        }
        // by hand, where an offset subtracts from the local time
        final long sunday = timestampDriver.parseFloorLiteral("2025-01-19T12:00:00.000000Z");
        final String fiveToSeven = forTimestampType("[{lo=2025-01-19T05:00:00.000000Z, hi=2025-01-19T07:00:00.000000Z}]");
        assertParsedAtEveryCheckLevel(fiveToSeven, "[$now - 2h..$now@UTC+05#Sun]", sunday);
        assertParsedAtEveryCheckLevel("[]", "[$now - 2h..$now@UTC+05#Mon]", sunday);
        assertParsedAtEveryCheckLevel(fiveToSeven, "[$now - 2h..$now@GMT+05#Sun]", sunday);
        assertParsedAtEveryCheckLevel(fiveToSeven, "[$now - 2h..$now@+05#Sun]", sunday);
        assertParsedAtEveryCheckLevel(fiveToSeven, "[$now - 2h..$now@05#Sun]", sunday);
        assertParsedAtEveryCheckLevel(forTimestampType("[{lo=2025-01-19T15:00:00.000000Z, hi=2025-01-19T17:00:00.000000Z}]"), "[$now - 2h..$now@UTC-05#Sun]", sunday);
        assertParsedAtEveryCheckLevel(forTimestampType("[{lo=2025-01-19T10:00:00.000000Z, hi=2025-01-19T12:00:00.000000Z}]"), "[$now - 2h..$now@Z#Sun]", sunday);
        assertParsedAtEveryCheckLevel(
                forTimestampType("[{lo=2025-01-18T19:00:00.000000Z, hi=2025-01-19T18:59:59.999999Z}]"),
                "[$today..$tomorrow@UTC+05#Sun]",
                sunday
        );
        assertParsedAtEveryCheckLevel(
                forTimestampType("[{lo=2025-01-19T00:00:00.000000Z, hi=2025-01-19T23:59:59.999999Z}]"),
                "[$today..$tomorrow@Z#Sun]",
                sunday
        );
        assertParsedAtEveryCheckLevel(
                forTimestampType("[{lo=2025-01-18T19:00:00.000000Z, hi=2025-01-19T07:00:00.000000Z}]"),
                "[$today..$now@UTC+05#Sun]",
                sunday
        );
        assertParsedAtEveryCheckLevel(
                forTimestampType("[{lo=2025-01-19T05:00:00.000000Z, hi=2025-01-19T18:59:59.999999Z}]"),
                "[$now - 2h..$tomorrow@UTC+05#Sun]",
                sunday
        );
        final String nineAndTen = forTimestampType("[{lo=2025-01-19T04:00:00.000000Z, hi=2025-01-19T04:00:59.999999Z},"
                + "{lo=2025-01-19T05:00:00.000000Z, hi=2025-01-19T05:00:59.999999Z}]");
        assertParsedAtEveryCheckLevel(
                forTimestampType("[{lo=2025-01-19T04:00:00.000000Z, hi=2025-01-19T04:00:59.999999Z}]"),
                "[$today..$tomorrow@UTC+05#Sun]T09:00",
                sunday
        );
        assertParsedAtEveryCheckLevel(nineAndTen, "[$today..$tomorrow@UTC+05#Sun]T[09:00,10:00]", sunday);
        assertParsedAtEveryCheckLevel(nineAndTen, "[$today..$tomorrow@UTC+05#Sun]T[09..10]:00", sunday);
        // a slot with a timezone of its own keeps it
        assertParsedAtEveryCheckLevel(
                forTimestampType("[{lo=2025-01-19T05:00:00.000000Z, hi=2025-01-19T05:00:59.999999Z},"
                        + "{lo=2025-01-19T06:00:00.000000Z, hi=2025-01-19T06:00:59.999999Z}]"),
                "[$today..$tomorrow@UTC+05#Sun]T[09:00@+03:00,10:00]",
                sunday
        );
        // the range ends at 12:00 at +05:00, 07:00Z, before the slot at 11:00Z, and the slot at 10:00
        // takes +05:00
        final String tenAtPlusFive = forTimestampType("[{lo=2025-01-19T05:00:00.000000Z, hi=2025-01-19T05:00:59.999999Z}]");
        assertParsedAtEveryCheckLevel(tenAtPlusFive, "[$today..$now@UTC+05#Sun]T[11:00@UTC,10:00]", sunday);
        assertParsedAtEveryCheckLevel(tenAtPlusFive, "[$today..$now]T[11:00@UTC,10:00]@UTC+05#Sun", sunday);
        // London keeps GMT until 01:00Z
        final long londonSpring = timestampDriver.parseFloorLiteral("2024-03-31T00:30:00.000000Z");
        assertParsedAtEveryCheckLevel(
                forTimestampType("[{lo=2024-03-30T22:30:00.000000Z, hi=2024-03-31T00:30:00.000000Z}]"),
                "[$now - 2h..$now@Europe/London#Sat]",
                londonSpring
        );
        assertParsedAtEveryCheckLevel("[]", "[$now - 2h..$now@Europe/London#Sun]", londonSpring);
    }

    @Test
    public void testDateVariableRangeElementZoneErrorEndsAtDayFilter() throws SqlException {
        // A timezone of a date variable range element that does not parse fails the range with its
        // text up to the day filter of the element, as the timezone of a single element and the one
        // of the list do, whether the range adds intervals or not, at every check level that lets
        // the range through. A timezone that parses there lets a range that adds no interval add
        // none. 2025-01-19 is a Sunday, and 2025-01-25 a Saturday.
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final long sunday = timestampDriver.parseFloorLiteral("2025-01-19T12:00:00.000000Z");
        final String[][] zones = {
                {"@Bogus/Zone#Sun", "invalid timezone: Bogus/Zone"},
                {"@#Sun", "invalid timezone: "},
                {"@ #Sun", "invalid timezone:  "},
                {"@[UTC]#Sun", "invalid timezone: [UTC]"},
                {"@+05x#Sun", "invalid timezone: +05x"}
        };
        final NoIntervalRange[] noIntervalRanges = {
                new NoIntervalRange("$now..$now - 1h", "2025-01-19T12:00:00.000000Z", null, true, "$now - 1h..$now"),
                new NoIntervalRange("$tomorrow..$today", "2025-01-19T12:00:00.000000Z", "2025-01-20", true, "$today..$tomorrow"),
                new NoIntervalRange("$today..$today + 0bd", "2025-01-25T12:00:00.000000Z", "2025-01-25", false, "$today..$today + 0bd")
        };
        for (String[] zone : zones) {
            // the single element and the list
            assertParseFailsAtEveryCheckLevel("[$today" + zone[0] + "]", sunday, zone[1]);
            assertParseFailsAtEveryCheckLevel("[$today]" + zone[0], sunday, zone[1]);
            for (String range : new String[]{"$now - 2h..$now", "$today..$tomorrow"}) {
                final String expression = "[" + range + zone[0] + "]";
                assertParseFailsAtEveryCheckLevel(expression, sunday, zone[1]);
                assertFailsLikeBindVariable(expression, sunday, zone[1]);
            }
            for (NoIntervalRange range : noIntervalRanges) {
                assertNoIntervalRangeFails(range, "[" + range.range() + zone[0] + "]", zone[1]);
            }
        }
        for (String zone : new String[]{"@+05#Sun", "@Z#Sun", "@UTC+05#Sun"}) {
            for (NoIntervalRange range : noIntervalRanges) {
                assertNoIntervalRangeAddsNoInterval(range, "[" + range.range() + zone + "]");
            }
        }
    }

    @Test
    public void testDateVariableRangeEmptyEndExpression() {
        // Empty end expression
        assertBracketIntervalError("[$today..]", "Empty end expression in date range");
    }

    @Test
    public void testDateVariableRangeEmptyStartExpression() {
        // Empty start expression - fails at date parsing
        assertBracketIntervalError("[..$today]", "Invalid date");
    }

    @Test
    public void testDateVariableRangeInMixedList() throws SqlException {
        // Range as part of a larger date list
        // [$yesterday..$tomorrow, 2026-02-01] - range + static date
        assertBracketIntervalWithNow(
                "[{lo=2026-01-21T00:00:00.000000Z, hi=2026-01-23T23:59:59.999999Z}," +
                        "{lo=2026-02-01T00:00:00.000000Z, hi=2026-02-01T23:59:59.999999Z}]",
                "[$yesterday..$tomorrow, 2026-02-01]",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableRangeInvalidOrder() {
        // End before start should produce error
        assertBracketIntervalError("[$today+5d..$today]", "Invalid date range: start is after end");
    }

    @Test
    public void testDateVariableRangeInvalidOrderMixed() {
        // $now is after the end of $yesterday
        assertBracketIntervalError("[$now..$yesterday]", "Invalid date range: start is after end");
    }

    @Test
    public void testDateVariableRangeLargeRangeTriggersIncrementalMerge() throws SqlException {
        // Test large day-based range that triggers incremental merge (exercises L2338)
        // [$today..$today+300d] - 301 days exceeds threshold of 256
        // Intervals are merged incrementally, result is a single merged interval
        assertBracketIntervalWithNow(
                "[{lo=2026-01-22T00:00:00.000000Z, hi=2026-11-18T23:59:59.999999Z}]",
                "[$today..$today+300d]",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableRangeMissingNumberAfterOperator() {
        // Missing number after + operator
        assertBracketIntervalError("[$today+..$today+5d]", "Expected number after operator");
    }

    // Bracket Expansion Tests

    @Test
    public void testDateVariableRangeMissingUnitAfterNumber() {
        // Missing unit after number
        assertBracketIntervalError("[$today+5..$today+10d]", "Expected time unit after number");
    }

    @Test
    public void testDateVariableRangeMixedExpressions() throws SqlException {
        // [$yesterday..$tomorrow] produces a merged interval
        // 2026-01-21, 2026-01-22, 2026-01-23 - adjacent full days are merged
        assertBracketIntervalWithNow(
                "[{lo=2026-01-21T00:00:00.000000Z, hi=2026-01-23T23:59:59.999999Z}]",
                "[$yesterday..$tomorrow]",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableRangeMultipleRangesInList() throws SqlException {
        // Multiple ranges in a date list (separated by gap)
        // This tests that we can have two range expressions in one list
        assertBracketIntervalWithNow(
                "[{lo=2026-01-20T00:00:00.000000Z, hi=2026-01-22T23:59:59.999999Z}," +
                        "{lo=2026-02-01T00:00:00.000000Z, hi=2026-02-03T23:59:59.999999Z}]",
                "[$today-2d..$today, $today+10d..$today+12d]",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableRangeNegativeOffset() throws SqlException {
        // [$today-2d..$today] produces a merged interval
        // Jan 20, 21, 22 - adjacent full days are merged
        assertBracketIntervalWithNow(
                "[{lo=2026-01-20T00:00:00.000000Z, hi=2026-01-22T23:59:59.999999Z}]",
                "[$today-2d..$today]",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableRangeNowEndAtMidnight() throws SqlException {
        // https://github.com/questdb/questdb/issues/8083
        // At exactly midnight, "the last 2 hours" still covers 2 hours rather than
        // the whole of yesterday and today
        assertParsedAndCompiledWithNow(
                "[{lo=2024-01-01T22:00:00.000000Z, hi=2024-01-02T00:00:00.000000Z}]",
                "$now - 2h..$now",
                "2024-01-02T00:00:00.000000Z"
        );
    }

    @Test
    public void testDateVariableRangeNowStartAtMidnight() throws SqlException {
        // https://github.com/questdb/questdb/issues/8083
        // $now - 1h lands exactly on midnight; the range still ends at $now
        // rather than covering the whole day
        assertParsedAndCompiledWithNow(
                "[{lo=2024-01-02T00:00:00.000000Z, hi=2024-01-02T01:00:00.000000Z}]",
                "$now - 1h..$now",
                "2024-01-02T01:00:00.000000Z"
        );
    }

    @Test
    public void testDateVariableRangeNowStartAtMidnightWithTimeSuffix() throws SqlException {
        // https://github.com/questdb/questdb/issues/8083
        // Both endpoints have a time of day, so the range skips the T09:00 override as it
        // does at any other moment (see testDateVariableArithmeticHoursWithTimeSuffix).
        // A start that lands exactly on midnight must not turn the range into whole days
        // that the override then narrows to 09:00, a window outside the range.
        assertParsedAndCompiledWithNow(
                "[{lo=2024-01-02T00:00:00.000000Z, hi=2024-01-02T02:00:00.000000Z}]",
                "[$now - 2h..$now]T09:00",
                "2024-01-02T02:00:00.000000Z"
        );
    }

    @Test
    public void testDateVariableRangeNowToToday() throws SqlException {
        // https://github.com/questdb/questdb/issues/8083
        // $today as the end stands for the end of today, so the range runs from $now to midnight
        assertParsedAndCompiledWithNow(
                "[{lo=2024-01-02T10:00:00.123400Z, hi=2024-01-02T23:59:59.999999Z}]",
                "$now..$today",
                "2024-01-02T10:00:00.123400Z"
        );
    }

    @Test
    public void testDateVariableRangeOverMaxIntervalsMessage() {
        // Each day of the range adds 24 one-minute intervals an hour apart, which never touch. A day
        // with no time zone or tick calendar adds them to the merge range of the list, so the merge
        // after each bracket value counts the intervals of every day. It passes the cap (1,024) at
        // the 17th value of the 43rd day, before the check after that day, and reports it as every
        // other bracket expansion does. testDateVariableRangeDayFilterOverMaxIntervalsMessage
        // covers the check after each day.
        assertTickExprErrorAtMonday("[$today..$today+100d]T[00..23]:00", "Bracket expansion produces too many intervals (max 1024)");
    }

    @Test
    public void testDateVariableRangeSameDaySingleBusinessDay() throws SqlException {
        // Single business day range: $today+1bd..$today+1bd (Friday 2026-01-23)
        assertBracketIntervalWithNow(
                "[{lo=2026-01-23T00:00:00.000000Z, hi=2026-01-23T23:59:59.999999Z}]",
                "[$today+1bd..$today+1bd]",
                "2026-01-22T10:30:00.000000Z"  // Thursday
        );
    }

    @Test
    public void testDateVariableRangeSingleDay() throws SqlException {
        // [$today..$today] should produce 1 interval
        assertBracketIntervalWithNow(
                "[{lo=2026-01-22T00:00:00.000000Z, hi=2026-01-22T23:59:59.999999Z}]",
                "[$today..$today]",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableRangeSingleDot() {
        // Single dot - findRangeOperator returns position of '.', end becomes "tomorrow" (no $)
        assertBracketIntervalError("[$today.$tomorrow]", "Unknown date variable: tomorrow");
    }

    // Date Variable Tests

    @Test
    public void testDateVariableRangeSpanningMonthBoundary() throws SqlException {
        // Range spanning Jan 29 to Feb 2 (month boundary)
        assertBracketIntervalWithNow(
                "[{lo=2026-01-29T00:00:00.000000Z, hi=2026-02-02T23:59:59.999999Z}]",
                "[$today..$today+4d]",
                "2026-01-29T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableRangeSpanningYearBoundary() throws SqlException {
        // Range spanning Dec 30 to Jan 2 (year boundary)
        // 2025-12-30 to 2026-01-02 = 4 days merged
        assertBracketIntervalWithNow(
                "[{lo=2025-12-30T00:00:00.000000Z, hi=2026-01-02T23:59:59.999999Z}]",
                "[$today-3d..$today]",
                "2026-01-02T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableRangeThreeDots() {
        // Three dots is invalid - parsed as ".." followed by ".$tomorrow" which fails
        assertBracketIntervalError("[$today...$tomorrow]", "Unknown date variable");
    }

    @Test
    public void testDateVariableRangeTimeEndAtMidnight() throws SqlException {
        // https://github.com/questdb/questdb/issues/8083
        // $today + 24h is the point at midnight of the next day, not the whole next day
        assertParsedAndCompiledWithNow(
                "[{lo=2024-01-02T06:00:00.000000Z, hi=2024-01-03T00:00:00.000000Z}]",
                "$today + 6h..$today + 24h",
                "2024-01-02T10:00:00.123400Z"
        );
    }

    @Test
    public void testDateVariableRangeTimeStartDayEnd() throws SqlException {
        // https://github.com/questdb/questdb/issues/8083
        // $today + 18h starts the range at 18:00 and $tomorrow ends it at the end of
        // tomorrow, so the range keeps the 18h offset instead of starting at midnight
        assertParsedAndCompiledWithNow(
                "[{lo=2024-01-02T18:00:00.000000Z, hi=2024-01-03T23:59:59.999999Z}]",
                "$today + 18h..$tomorrow",
                "2024-01-02T10:00:00.123400Z"
        );
    }

    @Test
    public void testDateVariableRangeTimeStartDayEndWithTimezone() throws SqlException {
        // https://github.com/questdb/questdb/issues/8083
        // Local range: 2024-01-02T08:00:00.1234 (+02:00) to the end of 2024-01-03 (+02:00)
        assertParsedAndCompiledWithNow(
                "[{lo=2024-01-02T06:00:00.123400Z, hi=2024-01-03T21:59:59.999999Z}]",
                "[$now - 2h..$tomorrow]@+02:00",
                "2024-01-02T10:00:00.123400Z"
        );
    }

    @Test
    public void testDateVariableRangeUnknownEndVariable() {
        // Unknown variable in end expression
        assertBracketIntervalError("[$today..$invalid]", "Unknown date variable");
    }

    @Test
    public void testDateVariableRangeUnknownStartVariable() {
        // Unknown variable in start expression
        assertBracketIntervalError("[$invalid..$today]", "Unknown date variable");
    }

    @Test
    public void testDateVariableRangeWithDayFilter() throws SqlException {
        // [$today..$today+10d]#workday - range then filter for workdays
        // From 2026-01-22 (Thu) to 2026-02-01 (Sun) = 11 days
        // Filtered to workdays: Thu 22, Fri 23, Mon 26, Tue 27, Wed 28, Thu 29, Fri 30
        // Adjacent weekdays are merged: [Jan 22-23] and [Jan 26-30]
        assertBracketIntervalWithNow(
                "[{lo=2026-01-22T00:00:00.000000Z, hi=2026-01-23T23:59:59.999999Z}," +
                        "{lo=2026-01-26T00:00:00.000000Z, hi=2026-01-30T23:59:59.999999Z}]",
                "[$today..$today+10d]#workday",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableRangeWithDenseTimeExpansionAndDayFilter() throws SqlException {
        // T[hh..hh]:[00..59] emits one interval per minute. Each time the range holds 256
        // intervals, the default incremental merge threshold, the bracket expansion merges them,
        // which can happen part way through a day.
        // $today is Saturday 2024-01-13, so #workday keeps $yesterday only.
        // 2 days x 24 hours stay below the threshold
        assertBracketIntervalWithNow(
                "[{lo=2024-01-12T00:00:00.000000Z, hi=2024-01-12T23:59:59.999999Z}]",
                "[$yesterday..$today]T[00..23]#workday",
                "2024-01-13T10:00:00.000000Z"
        );
        // 2 days x 1440 minutes cross the threshold while the second day expands
        assertBracketIntervalWithNow(
                "[{lo=2024-01-12T00:00:00.000000Z, hi=2024-01-12T23:59:59.999999Z}]",
                "[$yesterday..$today]T[00..23]:[00..59]#workday",
                "2024-01-13T10:00:00.000000Z"
        );
        // $yesterday and $today are both workdays
        assertBracketIntervalWithNow(
                "[{lo=2024-01-11T00:00:00.000000Z, hi=2024-01-12T23:59:59.999999Z}]",
                "[$yesterday..$today]T[00..23]:[00..59]#workday",
                "2024-01-12T10:00:00.000000Z"
        );
    }

    @Test
    public void testDateVariableRangeWithDenseTimeExpansionAndTimezone() throws SqlException {
        // T[hh..hh]:[00..59] emits one interval per minute. Each time the range holds 256
        // intervals, the default incremental merge threshold, the bracket expansion merges them,
        // which can happen part way through a day.
        // +02:00 shifts every minute of both days two hours back.
        // 2 days x 24 hours stay below the threshold
        assertBracketIntervalWithNow(
                "[{lo=2024-01-10T22:00:00.000000Z, hi=2024-01-12T21:59:59.999999Z}]",
                "[$yesterday..$today]T[00..23]@+02:00",
                "2024-01-12T10:00:00.000000Z"
        );
        // 2 days x 1440 minutes cross the threshold while the second day expands
        assertBracketIntervalWithNow(
                "[{lo=2024-01-10T22:00:00.000000Z, hi=2024-01-12T21:59:59.999999Z}]",
                "[$yesterday..$today]T[00..23]:[00..59]@+02:00",
                "2024-01-12T10:00:00.000000Z"
        );
    }

    @Test
    public void testDateVariableRangeWithDenseTimeSuffixAndDayFilter() throws SqlException {
        // T[00..23]:[00..59] expands each day to 1440 minutes, beyond the incremental merge
        // threshold (256 intervals). The day filter must still see only the intervals of its
        // own day: 2024-01-05 is a Friday, 2024-01-06 a Saturday.
        assertBracketIntervalWithNow(
                "[{lo=2024-01-05T00:00:00.000000Z, hi=2024-01-05T23:59:59.999999Z}]",
                "[$today + 3d..$today + 4d]T[00..23]:[00..59]#workday",
                "2024-01-02T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableRangeWithElementDayFilter() throws SqlException {
        // The range carries its own day filter. 2024-01-01 is a Monday.
        final String mondays = "[{lo=2024-01-01T00:00:00.000000Z, hi=2024-01-01T23:59:59.999999Z}," +
                "{lo=2024-01-08T00:00:00.000000Z, hi=2024-01-08T23:59:59.999999Z}]";
        final String now = "2024-01-01T12:00:00.000000Z";
        assertBracketIntervalWithNow(mondays, "[$today..$today+7d#Mon]", now);
        // the range's own day filter takes precedence over the global one
        assertBracketIntervalWithNow(mondays, "[$today..$today+7d#Mon]#Tue", now);
    }

    @Test
    public void testDateVariableRangeWithHours() throws SqlException {
        // Range with hour arithmetic: [$now-2h..$now] produces a 2-hour interval
        assertBracketIntervalWithNow(
                "[{lo=2026-01-22T08:30:00.000000Z, hi=2026-01-22T10:30:00.000000Z}]",
                "[$now - 2h..$now]",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableRangeWithHoursAndGlobalTimezone() throws SqlException {
        // PR review test: Time-precision range with GLOBAL timezone (outside brackets)
        // [$now - 2h..$now]@America/New_York - should apply timezone to time-precision range
        // If global timezone is honored: 08:30-10:30 in NY (EST, UTC-5) = 13:30-15:30 UTC
        // If global timezone is ignored: 08:30-10:30 UTC unchanged
        assertBracketIntervalWithNow(
                "[{lo=2026-01-22T13:30:00.000000Z, hi=2026-01-22T15:30:00.000000Z}]",
                "[$now - 2h..$now]@America/New_York",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableRangeWithHoursAndTimezone() throws SqlException {
        // Time-precision range with timezone INSIDE element (exercises L2176 tzMarker >= 0)
        // [$now - 2h..$now@America/New_York] - timestamps are in NY time, converted to UTC
        // 08:30-10:30 in NY (EST, UTC-5) = 13:30-15:30 UTC
        assertBracketIntervalWithNow(
                "[{lo=2026-01-22T13:30:00.000000Z, hi=2026-01-22T15:30:00.000000Z}]",
                "[$now - 2h..$now@America/New_York]",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableRangeWithHoursBareWithGlobalTimezone() throws SqlException {
        // Edge case: bare time-precision range (no brackets) with global timezone
        // $now - 2h..$now@America/New_York - should apply NY timezone
        // 08:30-10:30 in NY (EST, UTC-5) = 13:30-15:30 UTC
        assertBracketIntervalWithNow(
                "[{lo=2026-01-22T13:30:00.000000Z, hi=2026-01-22T15:30:00.000000Z}]",
                "$now - 2h..$now@America/New_York",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableRangeWithHoursElementTzPrecedesGlobal() throws SqlException {
        // Edge case: element-level timezone should take precedence over global
        // [$now - 2h..$now@+05:00]@America/New_York - should use +05:00, not NY
        // 08:30-10:30 at +05:00 = 03:30-05:30 UTC
        assertBracketIntervalWithNow(
                "[{lo=2026-01-22T03:30:00.000000Z, hi=2026-01-22T05:30:00.000000Z}]",
                "[$now - 2h..$now@+05:00]@America/New_York",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableRangeWithHoursGlobalTimezoneAndDayFilter() throws SqlException {
        // Edge case: time-precision range with global timezone AND day filter
        // [$now - 2h..$now]@America/New_York#Thu - 2026-01-22 is Thursday, so it passes filter
        // 08:30-10:30 in NY (EST, UTC-5) = 13:30-15:30 UTC
        assertBracketIntervalWithNow(
                "[{lo=2026-01-22T13:30:00.000000Z, hi=2026-01-22T15:30:00.000000Z}]",
                "[$now - 2h..$now]@America/New_York#Thu",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableRangeWithHoursGlobalTimezoneAndDayFilterNoMatch() throws SqlException {
        // Edge case: time-precision range with timezone and day filter that doesn't match
        // [$now - 2h..$now]@America/New_York#Mon - 2026-01-22 is Thursday, should produce empty
        assertBracketIntervalWithNow(
                "[]",
                "[$now - 2h..$now]@America/New_York#Mon",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableRangeWithNestedTimeExpansion() throws SqlException {
        assertBracketIntervalWithNow(
                "[{lo=2024-01-15T09:00:00.000000Z, hi=2024-01-15T09:00:00.999999Z},{lo=2024-01-15T09:00:30.000000Z, hi=2024-01-15T09:00:30.999999Z},{lo=2024-01-15T14:30:00.000000Z, hi=2024-01-15T14:30:00.999999Z},{lo=2024-01-15T14:30:30.000000Z, hi=2024-01-15T14:30:30.999999Z},{lo=2024-01-16T09:00:00.000000Z, hi=2024-01-16T09:00:00.999999Z},{lo=2024-01-16T09:00:30.000000Z, hi=2024-01-16T09:00:30.999999Z},{lo=2024-01-16T14:30:00.000000Z, hi=2024-01-16T14:30:00.999999Z},{lo=2024-01-16T14:30:30.000000Z, hi=2024-01-16T14:30:30.999999Z}]",
                "[$today..$today+1d]T[09:00,14:30]:[00,30]",
                "2024-01-15T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableRangeWithNowAndDenseTimeExpansion() throws SqlException {
        // T[hh..hh]:[00..59] emits one interval per minute. Each time the range holds 256
        // intervals, the default incremental merge threshold, the bracket expansion merges them,
        // which can happen part way through a day.
        // $now ends the range, so the intervals of today stop at 10:00.
        // 2 days x 24 hours stay below the threshold
        assertBracketIntervalWithNow(
                "[{lo=2024-01-11T00:00:00.000000Z, hi=2024-01-12T10:00:00.000000Z}]",
                "[$yesterday..$now]T[00..23]",
                "2024-01-12T10:00:00.000000Z"
        );
        // 2 days x 1440 minutes cross the threshold while the second day expands
        assertBracketIntervalWithNow(
                "[{lo=2024-01-11T00:00:00.000000Z, hi=2024-01-12T10:00:00.000000Z}]",
                "[$yesterday..$now]T[00..23]:[00..59]",
                "2024-01-12T10:00:00.000000Z"
        );
    }

    @Test
    public void testDateVariableRangeWithNowAfterDenseElement() throws SqlException {
        // $tomorrow adds 240 intervals, so the first day of the range reaches the incremental
        // merge threshold (256 intervals). The merge sorts the intervals of $tomorrow after the
        // ones of the range, and the clip to $now must not drop them.
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final long now = timestampDriver.parseFloorLiteral("2024-01-02T10:30:00.000000Z");
        final String expression = "[$tomorrow, $yesterday..$now]T[09..12]:[00..59]:00";
        out.clear();
        parseTickExprWithNow(timestampDriver, expression, 0, expression.length(), 0, out, now);

        // the first second of every minute from 09:00 to 12:59, on 2024-01-01 and 2024-01-03,
        // and up to $now on 2024-01-02
        final LongList expected = new LongList();
        final long firstDay = timestampDriver.parseFloorLiteral("2024-01-01T00:00:00.000000Z");
        for (int day = 0; day < 3; day++) {
            final long dayStart = timestampDriver.addDays(firstDay, day);
            for (int minute = 9 * 60; minute < 13 * 60; minute++) {
                final long lo = dayStart + timestampDriver.fromMinutes(minute);
                final long hi = lo + timestampDriver.fromSeconds(1) - 1;
                if (day != 1) {
                    expected.add(lo, hi);
                } else if (lo <= now) {
                    expected.add(lo, Math.min(hi, now));
                }
            }
        }
        Assert.assertEquals(571, expected.size() / 2);
        // intervalToString() reuses one sink, so take a copy of the expected text first
        final String expectedText = intervalToString(timestampDriver, expected).toString();
        TestUtils.assertEquals(expectedText, intervalToString(timestampDriver, out));
    }

    @Test
    public void testDateVariableRangeWithNowAndDenseTimeSuffix() throws SqlException {
        // T[00..23]:[00..59] expands each day to 1440 minutes, beyond the incremental merge
        // threshold (256 intervals). The merge must not change which intervals the clip to
        // $now applies to.
        assertBracketIntervalWithNow(
                "[{lo=2024-01-02T10:30:00.000000Z, hi=2024-01-03T23:59:59.999999Z}]",
                "[$now..$tomorrow]T[00..23]:[00..59]",
                "2024-01-02T10:30:00.000000Z"
        );
        assertBracketIntervalWithNow(
                "[{lo=2024-01-01T00:00:00.000000Z, hi=2024-01-02T10:30:00.000000Z}]",
                "[$yesterday..$now]T[00..23]:[00..59]",
                "2024-01-02T10:30:00.000000Z"
        );
        // the intervals of $tomorrow come first in the list, and sort after the range
        assertBracketIntervalWithNow(
                "[{lo=2024-01-01T00:00:00.000000Z, hi=2024-01-02T10:30:00.000000Z},{lo=2024-01-03T00:00:00.000000Z, hi=2024-01-03T23:59:59.999999Z}]",
                "[$tomorrow, $yesterday..$now]T[00..23]:[00..59]",
                "2024-01-02T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableRangeWithNowAndDenseTimeSuffixWithinLimit() throws SqlException {
        // The day has 1440 separate seconds, beyond the limit of 1024 intervals, but the range
        // starts at 18:00 and keeps 360 of them
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final String expression = "[$today + 18h..$today]T[00..23]:[00..59]:00";
        out.clear();
        parseTickExprWithNow(timestampDriver, expression, 0, expression.length(), 0, out,
                timestampDriver.parseFloorLiteral("2024-01-02T10:30:00.000000Z"));
        Assert.assertEquals(360, out.size() / 2);
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2024-01-02T18:00:00.000000Z"), out.getQuick(0));
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2024-01-02T23:59:01.000000Z") - 1, out.getQuick(out.size() - 1));
    }

    @Test
    public void testDateVariableRangeWithNowAndTimeListAndTimezone() throws SqlException {
        // https://github.com/questdb/questdb/issues/8083
        // The range starts at $now - 2h = 10:00 local (+02:00), which drops the 09:00 of today
        // but keeps its 10:30. The time list clips its slots in local time before converting
        // them to UTC: 10:30 local, after 10:00 local, converts to 08:30Z.
        assertParsedAndCompiledWithNow(
                "[{lo=2024-01-02T08:30:00.000000Z, hi=2024-01-02T08:30:59.999999Z}," +
                        "{lo=2024-01-03T07:00:00.000000Z, hi=2024-01-03T07:00:59.999999Z}," +
                        "{lo=2024-01-03T08:30:00.000000Z, hi=2024-01-03T08:30:59.999999Z}]",
                "[$now - 2h..$tomorrow]T[09:00,10:30]@+02:00",
                "2024-01-02T12:00:00.000000Z"
        );
    }

    @Test
    public void testDateVariableRangeWithNowAndTimeListEndInDstGap() throws SqlException {
        // On 2024-03-31 London clocks skip from 01:00 to 02:00 local time. $now = 01:10 local
        // ends the range inside that hour, so today's 01:30 and 02:30 fall after the range and
        // only today's 00:30 remains. The range compares the slots with $now in local time.
        assertParsedAndCompiledWithNow(
                "[{lo=2024-03-30T00:30:00.000000Z, hi=2024-03-30T00:30:59.999999Z}," +
                        "{lo=2024-03-30T01:30:00.000000Z, hi=2024-03-30T01:30:59.999999Z}," +
                        "{lo=2024-03-30T02:30:00.000000Z, hi=2024-03-30T02:30:59.999999Z}," +
                        "{lo=2024-03-31T00:30:00.000000Z, hi=2024-03-31T00:30:59.999999Z}]",
                "[$yesterday..$now]T[00:30,01:30,02:30]@Europe/London",
                "2024-03-31T01:10:00.000000Z"
        );
    }

    @Test
    public void testDateVariableRangeWithNowAndTimeListInDstGap() throws SqlException {
        // On 2024-03-31 London clocks skip from 01:00 to 02:00 local time. The range compares
        // the slots with $now - 2h in local time, the way it compares its other intervals, then
        // converts the slots it keeps to UTC. Today's 01:30 does not exist and matches nothing,
        // 02:30 BST is 01:30Z, and tomorrow's 00:30, 01:30 and 02:30 BST are 23:30Z, 00:30Z and
        // 01:30Z.
        final String expression = "[$now - 2h..$tomorrow]T[00:30,01:30,02:30]@Europe/London";
        // $now - 2h = 01:20 local, inside the skipped hour: 01:30 and 02:30 are after it
        assertParsedAndCompiledWithNow(
                "[{lo=2024-03-31T01:30:00.000000Z, hi=2024-03-31T01:30:59.999999Z}," +
                        "{lo=2024-03-31T23:30:00.000000Z, hi=2024-03-31T23:30:59.999999Z}," +
                        "{lo=2024-04-01T00:30:00.000000Z, hi=2024-04-01T00:30:59.999999Z}," +
                        "{lo=2024-04-01T01:30:00.000000Z, hi=2024-04-01T01:30:59.999999Z}]",
                expression,
                "2024-03-31T03:20:00.000000Z"
        );
        final String afterTodays0130 = "[{lo=2024-03-31T01:30:00.000000Z, hi=2024-03-31T01:30:59.999999Z}," +
                "{lo=2024-03-31T23:30:00.000000Z, hi=2024-03-31T23:30:59.999999Z}," +
                "{lo=2024-04-01T00:30:00.000000Z, hi=2024-04-01T00:30:59.999999Z}," +
                "{lo=2024-04-01T01:30:00.000000Z, hi=2024-04-01T01:30:59.999999Z}]";
        // $now - 2h = 01:40 local, still inside the skipped hour: 01:30 is before it
        assertParsedAndCompiledWithNow(afterTodays0130, expression, "2024-03-31T03:40:00.000000Z");
        // $now - 2h = 02:00 local, the first moment after the skipped hour: 01:30 is before it
        assertParsedAndCompiledWithNow(afterTodays0130, expression, "2024-03-31T04:00:00.000000Z");
    }

    @Test
    public void testDateVariableRangeWithNowAndTimeListMergedSlots() throws SqlException {
        // Each slot expands to 60 one-second intervals, 300 a day, so the expansion merges the
        // intervals while it expands them. The merge must not move a slot's intervals before the
        // range clips the slot to $now = 12:00. Each slot's seconds merge into its minute.
        final String expression = "T[09:00,10:00,11:00,12:00,13:00]:[00..59]";
        final String now = "2024-01-02T12:00:00.000000Z";
        assertParsedAndCompiledWithNow(
                "[{lo=2024-01-02T12:00:00.000000Z, hi=2024-01-02T12:00:59.999999Z}," +
                        "{lo=2024-01-02T13:00:00.000000Z, hi=2024-01-02T13:00:59.999999Z}," +
                        "{lo=2024-01-03T09:00:00.000000Z, hi=2024-01-03T09:00:59.999999Z}," +
                        "{lo=2024-01-03T10:00:00.000000Z, hi=2024-01-03T10:00:59.999999Z}," +
                        "{lo=2024-01-03T11:00:00.000000Z, hi=2024-01-03T11:00:59.999999Z}," +
                        "{lo=2024-01-03T12:00:00.000000Z, hi=2024-01-03T12:00:59.999999Z}," +
                        "{lo=2024-01-03T13:00:00.000000Z, hi=2024-01-03T13:00:59.999999Z}]",
                "[$now..$tomorrow]" + expression,
                now
        );
        assertParsedAndCompiledWithNow(
                "[{lo=2024-01-01T09:00:00.000000Z, hi=2024-01-01T09:00:59.999999Z}," +
                        "{lo=2024-01-01T10:00:00.000000Z, hi=2024-01-01T10:00:59.999999Z}," +
                        "{lo=2024-01-01T11:00:00.000000Z, hi=2024-01-01T11:00:59.999999Z}," +
                        "{lo=2024-01-01T12:00:00.000000Z, hi=2024-01-01T12:00:59.999999Z}," +
                        "{lo=2024-01-01T13:00:00.000000Z, hi=2024-01-01T13:00:59.999999Z}," +
                        "{lo=2024-01-02T09:00:00.000000Z, hi=2024-01-02T09:00:59.999999Z}," +
                        "{lo=2024-01-02T10:00:00.000000Z, hi=2024-01-02T10:00:59.999999Z}," +
                        "{lo=2024-01-02T11:00:00.000000Z, hi=2024-01-02T11:00:59.999999Z}," +
                        "{lo=2024-01-02T12:00:00.000000Z, hi=2024-01-02T12:00:00.000000Z}]",
                "[$yesterday..$now]" + expression,
                now
        );
        // with a timezone, the slots convert after the clip, in the merges as well
        assertBracketIntervalWithNow(
                "[{lo=2024-01-02T10:00:00.000000Z, hi=2024-01-02T10:00:59.999999Z}," +
                        "{lo=2024-01-02T11:00:00.000000Z, hi=2024-01-02T11:00:59.999999Z}," +
                        "{lo=2024-01-03T07:00:00.000000Z, hi=2024-01-03T07:00:59.999999Z}," +
                        "{lo=2024-01-03T08:00:00.000000Z, hi=2024-01-03T08:00:59.999999Z}," +
                        "{lo=2024-01-03T09:00:00.000000Z, hi=2024-01-03T09:00:59.999999Z}," +
                        "{lo=2024-01-03T10:00:00.000000Z, hi=2024-01-03T10:00:59.999999Z}," +
                        "{lo=2024-01-03T11:00:00.000000Z, hi=2024-01-03T11:00:59.999999Z}]",
                "[$now..$tomorrow]" + expression + "@+02:00",
                now
        );
    }

    @Test
    public void testDateVariableRangeWithNowAndTimeListPerElementTimezoneInDstGap() throws SqlException {
        // $now - 2h = 01:20 London time falls in the hour that London clocks skip on 2024-03-31,
        // so the range starts at the transition, 01:00Z. A slot with its own timezone is a point
        // in time, which the range compares with that start: 10:30 in Tokyo is 01:30Z, after it.
        final String expected = "[{lo=2024-03-31T01:30:00.000000Z, hi=2024-03-31T01:30:59.999999Z}," +
                "{lo=2024-04-01T01:30:00.000000Z, hi=2024-04-01T01:30:59.999999Z}]";
        final String now = "2024-03-31T03:20:00.000000Z";
        assertParsedAndCompiledWithNow(expected, "[$now - 2h..$tomorrow]T[10:30@Asia/Tokyo]@Europe/London", now);
        assertParsedAndCompiledWithNow(expected, "[$now - 2h..$tomorrow]T[02:30@+01:00]@Europe/London", now);
        // naming the range's own timezone on the slot does not change the result
        assertParsedAndCompiledWithNow(expected, "[$now - 2h..$tomorrow]T[02:30@Europe/London]@Europe/London", now);
        assertParsedAndCompiledWithNow(expected, "[$now - 2h..$tomorrow]T[02:30]@Europe/London", now);
        // The range cuts a slot with its own timezone at the point in time where it starts:
        // 09:30 to 10:30 in Tokyo is 00:30Z to 01:30Z, and the range starts at 01:00Z
        assertParsedAndCompiledWithNow(
                "[{lo=2024-03-31T01:00:00.000000Z, hi=2024-03-31T01:29:59.999999Z}," +
                        "{lo=2024-04-01T00:30:00.000000Z, hi=2024-04-01T01:29:59.999999Z}]",
                "[$now - 2h..$tomorrow]T[09:30@Asia/Tokyo]@Europe/London;1h",
                now
        );
    }

    @Test
    public void testDateVariableRangeWithNowAndTimeSuffix() throws SqlException {
        // https://github.com/questdb/questdb/issues/8083
        // The range starts at $now (10:30), so today's 09:00 falls outside it
        assertParsedAndCompiledWithNow(
                "[{lo=2026-01-23T09:00:00.000000Z, hi=2026-01-23T09:00:59.999999Z}]",
                "[$now..$tomorrow]T09:00",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableRangeWithNowKeepsTime() throws SqlException {
        // https://github.com/questdb/questdb/issues/8083
        // The range runs from $now (10:30 on Jan 22) to the end of $tomorrow (Jan 23),
        // keeping the time of day of $now rather than starting at midnight
        assertParsedAndCompiledWithNow(
                "[{lo=2026-01-22T10:30:00.000000Z, hi=2026-01-23T23:59:59.999999Z}]",
                "[$now..$tomorrow]",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableRangeWithNumericExpansionBrackets() throws SqlException {
        // [$today..$today+1d]T09:[00,30] - numeric expansion brackets, not time list (exercises L2312 false)
        // 2 days * 2 minute values = 4 intervals at 09:00 and 09:30
        assertBracketIntervalWithNow(
                "[{lo=2026-01-22T09:00:00.000000Z, hi=2026-01-22T09:00:59.999999Z}," +
                        "{lo=2026-01-22T09:30:00.000000Z, hi=2026-01-22T09:30:59.999999Z}," +
                        "{lo=2026-01-23T09:00:00.000000Z, hi=2026-01-23T09:00:59.999999Z}," +
                        "{lo=2026-01-23T09:30:00.000000Z, hi=2026-01-23T09:30:59.999999Z}]",
                "[$today..$today+1d]T09:[00,30]",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableRangeWithPerElementTimezone() throws SqlException {
        // Per-element timezone on range expression
        // [$today..$today+2d@+05:00] - each date at +05:00
        // Full day at +05:00 = 19:00 UTC previous day to 18:59:59 UTC
        assertBracketIntervalWithNow(
                "[{lo=2026-01-21T19:00:00.000000Z, hi=2026-01-24T18:59:59.999999Z}]",
                "[$today..$today+2d]@+05:00",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableRangeWithPerElementTimezoneInside() throws SqlException {
        // Per-element timezone INSIDE the range element (exercises ec == '@' at L2113)
        // [$today..$today+2d@+05:00] - timezone is part of the range element
        // Full day at +05:00 = 19:00 UTC previous day to 18:59:59 UTC
        assertBracketIntervalWithNow(
                "[{lo=2026-01-21T19:00:00.000000Z, hi=2026-01-24T18:59:59.999999Z}]",
                "[$today..$today+2d@+05:00]",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableRangeWithTimeList() throws SqlException {
        // [$today..$today+2d]T[09:00,14:00] should produce 6 intervals (3 days * 2 times)
        assertBracketIntervalWithNow(
                "[{lo=2026-01-22T09:00:00.000000Z, hi=2026-01-22T09:00:59.999999Z}," +
                        "{lo=2026-01-22T14:00:00.000000Z, hi=2026-01-22T14:00:59.999999Z}," +
                        "{lo=2026-01-23T09:00:00.000000Z, hi=2026-01-23T09:00:59.999999Z}," +
                        "{lo=2026-01-23T14:00:00.000000Z, hi=2026-01-23T14:00:59.999999Z}," +
                        "{lo=2026-01-24T09:00:00.000000Z, hi=2026-01-24T09:00:59.999999Z}," +
                        "{lo=2026-01-24T14:00:00.000000Z, hi=2026-01-24T14:00:59.999999Z}]",
                "[$today..$today+2d]T[09:00,14:00]",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableRangeWithTimeListAndDuration() throws SqlException {
        // [$today..$today+1d]T[09:00,14:00];30m - time list with duration (exercises L2287)
        // 2 days * 2 times = 4 intervals, each with 30m duration
        assertBracketIntervalWithNow(
                "[{lo=2026-01-22T09:00:00.000000Z, hi=2026-01-22T09:29:59.999999Z}," +
                        "{lo=2026-01-22T14:00:00.000000Z, hi=2026-01-22T14:29:59.999999Z}," +
                        "{lo=2026-01-23T09:00:00.000000Z, hi=2026-01-23T09:29:59.999999Z}," +
                        "{lo=2026-01-23T14:00:00.000000Z, hi=2026-01-23T14:29:59.999999Z}]",
                "[$today..$today+1d]T[09:00,14:00];30m",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableRangeWithTimeSuffix() throws SqlException {
        // [$today..$today+3d]T09:00;1h should produce 4 intervals with time
        assertBracketIntervalWithNow(
                "[{lo=2026-01-22T09:00:00.000000Z, hi=2026-01-22T09:59:59.999999Z}," +
                        "{lo=2026-01-23T09:00:00.000000Z, hi=2026-01-23T09:59:59.999999Z}," +
                        "{lo=2026-01-24T09:00:00.000000Z, hi=2026-01-24T09:59:59.999999Z}," +
                        "{lo=2026-01-25T09:00:00.000000Z, hi=2026-01-25T09:59:59.999999Z}]",
                "[$today..$today+3d]T09:00;1h",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableRangeWithTimezone() throws SqlException {
        // [$today..$today+2d]T09:00@America/New_York
        // 09:00 EST = 14:00 UTC (winter time, UTC-5)
        assertBracketIntervalWithNow(
                "[{lo=2026-01-22T14:00:00.000000Z, hi=2026-01-22T14:00:59.999999Z}," +
                        "{lo=2026-01-23T14:00:00.000000Z, hi=2026-01-23T14:00:59.999999Z}," +
                        "{lo=2026-01-24T14:00:00.000000Z, hi=2026-01-24T14:00:59.999999Z}]",
                "[$today..$today+2d]T09:00@America/New_York",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableRangeWithTimezoneAndDuration() throws SqlException {
        // Day-based range with global timezone AND duration suffix (exercises L2265)
        // [$today..$today+2d]@America/New_York;1h - range from today to today+2d, 1h duration applied
        // Full range: Jan 22 00:00 to Jan 24 23:59:59 NY, with 1h duration = Jan 22 00:00 to Jan 25 00:59:59 NY
        // Converted to UTC (EST = UTC-5): Jan 22 05:00 to Jan 25 05:59:59 UTC
        assertBracketIntervalWithNow(
                "[{lo=2026-01-22T05:00:00.000000Z, hi=2026-01-22T05:59:59.999999Z},{lo=2026-01-23T05:00:00.000000Z, hi=2026-01-23T05:59:59.999999Z},{lo=2026-01-24T05:00:00.000000Z, hi=2026-01-24T05:59:59.999999Z}]",
                "[$today..$today+2d]@America/New_York;1h",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    // Date Variable Range Tests

    @Test
    public void testDateVariableRangeWithTrailingWhitespaceInStartExpr() throws SqlException {
        // Trailing whitespace in start expression before .. (exercises L2126 whitespace trimming)
        // [$today   ..$tomorrow] - spaces after $today are trimmed
        assertBracketIntervalWithNow(
                "[{lo=2026-01-22T00:00:00.000000Z, hi=2026-01-23T23:59:59.999999Z}]",
                "[$today   ..$tomorrow]",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableRangeWithWhitespace() throws SqlException {
        // Whitespace around range operator and in expressions
        assertBracketIntervalWithNow(
                "[{lo=2026-01-22T00:00:00.000000Z, hi=2026-01-24T23:59:59.999999Z}]",
                "[ $today .. $today + 2d ]",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableRangeZeroBusinessDays() throws SqlException {
        // $today..$today+0bd should be just today (if weekday)
        // 2026-01-22 is Thursday
        assertBracketIntervalWithNow(
                "[{lo=2026-01-22T00:00:00.000000Z, hi=2026-01-22T23:59:59.999999Z}]",
                "[$today..$today+0bd]",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableRangeZeroBusinessDaysFromWeekend() throws SqlException {
        // $today..$today+0bd from Saturday = empty (Saturday is not a business day)
        // The start and end are both Saturday, but bd range skips weekends
        // So no days are included
        assertBracketIntervalWithNow(
                "[]",
                "[$today..$today+0bd]",
                "2026-01-24T10:30:00.000000Z"  // Saturday
        );
    }

    @Test
    public void testDateVariableToday() throws SqlException {
        // [$today] should resolve to full day
        assertBracketIntervalWithNow(
                "[{lo=2026-01-22T00:00:00.000000Z, hi=2026-01-22T23:59:59.999999Z}]",
                "[$today]",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableTodayPlusHoursAtMidnight() throws SqlException {
        // https://github.com/questdb/questdb/issues/8083
        // $today + 24h is a point in time, like $today + 23h, even though it lands exactly
        // on midnight; only day-unit arithmetic such as $today + 1d yields a whole day
        assertParsedAndCompiledWithNow(
                "[{lo=2024-01-03T00:00:00.000000Z, hi=2024-01-03T00:00:00.000000Z}]",
                "$today + 24h",
                "2024-01-02T10:00:00.123400Z"
        );
    }

    @Test
    public void testDateVariableTodayWithDuration() throws SqlException {
        // [$today]T09:30;1h should resolve to 2026-01-22T09:30 to 10:30
        // Duration is added to end of the minute (09:30:59), so end is 10:30:59
        assertBracketIntervalWithNow(
                "[{lo=2026-01-22T09:30:00.000000Z, hi=2026-01-22T10:29:59.999999Z}]",
                "[$today]T09:30;1h",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableTodayWithTimeSuffix() throws SqlException {
        // [$today]T09:30 should resolve to 2026-01-22T09:30
        assertBracketIntervalWithNow(
                "[{lo=2026-01-22T09:30:00.000000Z, hi=2026-01-22T09:30:59.999999Z}]",
                "[$today]T09:30",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableTomorrow() throws SqlException {
        // [$tomorrow] should resolve to 2026-01-23
        assertBracketIntervalWithNow(
                "[{lo=2026-01-23T00:00:00.000000Z, hi=2026-01-23T23:59:59.999999Z}]",
                "[$tomorrow]",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableTrailingCharactersAfterBusinessDayUnit() {
        // "$today + 2bdxyz" should fail - trailing characters after 'bd' unit
        assertBracketIntervalError("[$today + 2bdxyz]", "Unexpected characters after unit");
    }

    @Test
    public void testDateVariableTrailingCharactersAfterUnit() {
        // "$today + 3dabc" should fail - trailing characters after 'd' unit
        assertBracketIntervalError("[$today + 3dabc]", "Unexpected characters after unit");
    }

    @Test
    public void testDateVariableUnderscoreConsecutiveInvalid() {
        // Consecutive underscores should fail
        assertBracketIntervalError("[$now - 1__000T]", "Invalid number in date expression");
    }

    @Test
    public void testDateVariableUnderscoreInNumber() throws SqlException {
        // $now - 10_000T should work the same as $now - 10000T
        assertBracketIntervalWithNow(
                "[{lo=2026-01-22T10:29:50.000000Z, hi=2026-01-22T10:29:50.000000Z}]",
                "[$now - 10_000T]",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableUnderscoreInNumberBusinessDays() throws SqlException {
        // $today + 1_0bd should work as 10 business days
        assertBracketIntervalWithNow(
                "[{lo=2026-02-05T00:00:00.000000Z, hi=2026-02-05T23:59:59.999999Z}]",
                "[$today + 1_0bd]",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableUnderscoreInNumberCompact() throws SqlException {
        // $now-10_000T compact form (no spaces)
        assertBracketIntervalWithNow(
                "[{lo=2026-01-22T10:29:50.000000Z, hi=2026-01-22T10:29:50.000000Z}]",
                "[$now-10_000T]",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableUnderscoreInNumberRange() throws SqlException {
        // $now-10_000T..$now-9_900T range with underscores
        assertBracketIntervalWithNow(
                "[{lo=2026-01-22T10:29:50.000000Z, hi=2026-01-22T10:29:50.100000Z}]",
                "$now-10_000T..$now-9_900T",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableUnderscoreLeadingInvalid() {
        // Leading underscore should fail
        assertBracketIntervalError("[$now - _100T]", "[7] Invalid number in date expression");
    }

    @Test
    public void testDateVariableUnderscoreTrailingInvalid() {
        // Trailing underscore: number is "100_", parseInt will reject it
        assertBracketIntervalError("[$now - 100_T]", "Invalid number in date expression");
    }

    @Test
    public void testDateVariableWithPerElementDayFilter() throws SqlException {
        // Date variable with per-element day filter: $today#Thu
        // 2025-07-17 is Thursday, so it passes the filter
        assertBracketIntervalWithNow(
                "[{lo=2025-07-17T00:00:00.000000Z, hi=2025-07-17T23:59:59.999999Z}]",
                "[$today#Thu]",
                "2025-07-17T09:15:00.000000Z"
        );
    }

    @Test
    public void testDateVariableWithPerElementDayFilterNoMatch() throws SqlException {
        // Date variable with per-element day filter that doesn't match
        // 2025-09-10 is Wednesday, but we filter for Monday - empty result
        assertBracketIntervalWithNow(
                "[]",
                "[$today#Mon]",
                "2025-09-10T17:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableWithPerElementTimezone() throws SqlException {
        // Date variable with per-element timezone: $today@+02:00
        // Start of day in UTC+2 = previous day at 22:00 UTC
        assertDateVariableInterval(
                (now, driver, tsType) -> {
                    long todayStart = driver.startOfDay(now, 0);
                    String prevDate = formatDate(driver.addDays(todayStart, -1), driver);
                    String todayDate = formatDate(todayStart, driver);
                    // Full day in +02:00 = 22:00 UTC previous day to 21:59:59 UTC today
                    return "[{lo=" + prevDate + "T22:00:00.000000Z, hi=" + todayDate + "T21:59:59.999999Z}]";
                },
                "[$today@+02:00]"
        );
    }

    @Test
    public void testDateVariableWithPerElementTimezoneAndGlobalTimeSuffix() throws SqlException {
        // Date variable with per-element timezone + global time suffix
        // $today@+02:00 at T09:00 = today at 09:00 in UTC+2 = 07:00 UTC
        assertDateVariableInterval(
                (now, driver, tsType) -> {
                    String date = formatDate(driver.startOfDay(now, 0), driver);
                    return "[{lo=" + date + "T07:00:00.000000Z, hi=" + date + "T07:00:59.999999Z}]";
                },
                "[$today@+02:00]T09:00"
        );
    }

    @Test
    public void testDateVariableWithTimeListSuffix() throws SqlException {
        // Date variable gets expanded with time list
        assertDateVariableInterval(
                (now, driver, tsType) -> {
                    String date = formatDate(driver.startOfDay(now, 0), driver);
                    return "[{lo=" + date + "T09:00:00.000000Z, hi=" + date + "T09:00:59.999999Z},{lo=" + date + "T14:30:00.000000Z, hi=" + date + "T14:30:59.999999Z}]";
                },
                "[$today]T[09:00,14:30]"
        );
    }

    @Test
    public void testDateVariableWithTimeSuffixAndDuration() throws SqlException {
        // Date variable gets time and duration from suffix
        assertDateVariableInterval(
                (now, driver, tsType) -> {
                    String date = formatDate(driver.startOfDay(now, 0), driver);
                    return "[{lo=" + date + "T09:30:00.000000Z, hi=" + date + "T10:29:59.999999Z}]";
                },
                "[$today]T09:30;1h"
        );
    }

    @Test
    public void testDateVariableWithTimeSuffixAndTimezone() throws SqlException {
        // Date variable gets time and timezone from suffix
        // $today at 09:30 in UTC+2 = 07:30 UTC
        assertDateVariableInterval(
                (now, driver, tsType) -> {
                    String date = formatDate(driver.startOfDay(now, 0), driver);
                    return "[{lo=" + date + "T07:30:00.000000Z, hi=" + date + "T07:30:59.999999Z}]";
                },
                "[$today]T09:30@+02:00"
        );
    }

    @Test
    public void testDateVariableWithTimezone() throws SqlException {
        // [$today]T09:00@America/New_York
        // 2026-01-22 - New York is in EST (UTC-5) during winter
        // 09:00 EST = 14:00 UTC
        assertBracketIntervalWithNow(
                "[{lo=2026-01-22T14:00:00.000000Z, hi=2026-01-22T14:00:59.999999Z}]",
                "[$today]T09:00@America/New_York",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableWithWhitespace() throws SqlException {
        // Test whitespace handling in expressions
        // Whitespace around operators and between number and unit
        assertBracketIntervalWithNow(
                "[{lo=2026-01-25T00:00:00.000000Z, hi=2026-01-25T23:59:59.999999Z}]",
                "[ $today   +   3 d ]",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDateVariableYesterday() throws SqlException {
        // [$yesterday] should resolve to 2026-01-21
        assertBracketIntervalWithNow(
                "[{lo=2026-01-21T00:00:00.000000Z, hi=2026-01-21T23:59:59.999999Z}]",
                "[$yesterday]",
                "2026-01-22T10:30:00.000000Z"
        );
    }

    @Test
    public void testDayFilterAllDaysOfWeek() throws SqlException {
        // Test each day abbreviation
        // 2024-01-01=Mon, 02=Tue, 03=Wed, 04=Thu, 05=Fri, 06=Sat, 07=Sun
        assertBracketInterval(
                "[{lo=2024-01-02T00:00:00.000000Z, hi=2024-01-02T23:59:59.999999Z}]",
                "2024-01-[01..07]#Tue"
        );
        assertBracketInterval(
                "[{lo=2024-01-04T00:00:00.000000Z, hi=2024-01-04T23:59:59.999999Z}]",
                "2024-01-[01..07]#Thu"
        );
        assertBracketInterval(
                "[{lo=2024-01-06T00:00:00.000000Z, hi=2024-01-06T23:59:59.999999Z}]",
                "2024-01-[01..07]#Sat"
        );
        assertBracketInterval(
                "[{lo=2024-01-07T00:00:00.000000Z, hi=2024-01-07T23:59:59.999999Z}]",
                "2024-01-[01..07]#Sun"
        );
    }

    @Test
    public void testDayFilterAppliesBeforeIncrementalMerge() throws SqlException {
        // 2024-01-01 and 2024-01-08 are Mondays. A merge of the adjacent days would leave one block
        // that starts on a Monday.
        assertTickExprAtMonday(
                mergeThreshold4Configuration,
                "[{lo=2024-01-01T00:00:00.000000Z, hi=2024-01-01T23:59:59.999999Z},{lo=2024-01-08T00:00:00.000000Z, hi=2024-01-08T23:59:59.999999Z}]",
                "2024-01-[01..09]#mon"
        );
    }

    @Test
    public void testDayFilterAppliesBeforeIncrementalMergeDateList() throws SqlException {
        assertTickExprAtMonday(
                mergeThreshold4Configuration,
                "[{lo=2024-01-01T00:00:00.000000Z, hi=2024-01-01T23:59:59.999999Z},{lo=2024-01-05T00:00:00.000000Z, hi=2024-01-05T23:59:59.999999Z},{lo=2024-01-08T00:00:00.000000Z, hi=2024-01-08T23:59:59.999999Z}]",
                "[2024-01-05,2024-01-[01..09]#mon]"
        );
    }

    @Test
    public void testDayFilterAppliesBeforeIncrementalMergeTimeList() throws SqlException {
        // Monday 18:00 for 6 hours runs into Tuesday, which the filter keeps as well
        assertTickExprAtMonday(
                mergeThreshold4Configuration,
                "[{lo=2024-01-01T00:00:00.000000Z, hi=2024-01-01T05:59:59.999999Z},"
                        + "{lo=2024-01-01T18:00:00.000000Z, hi=2024-01-02T05:59:59.999999Z},"
                        + "{lo=2024-01-02T18:00:00.000000Z, hi=2024-01-02T23:59:59.999999Z},"
                        + "{lo=2024-01-08T00:00:00.000000Z, hi=2024-01-08T05:59:59.999999Z},"
                        + "{lo=2024-01-08T18:00:00.000000Z, hi=2024-01-09T05:59:59.999999Z},"
                        + "{lo=2024-01-09T18:00:00.000000Z, hi=2024-01-09T23:59:59.999999Z}]",
                "2024-01-[01..09]T[00:00,18:00]#mon,tue;6h"
        );
    }

    @Test
    public void testDayFilterBelowIncrementalMergeControl() throws SqlException {
        // the default threshold (256) never merges these few values: the same expected lists
        assertTickExprAtMonday(
                configuration,
                "[{lo=2024-01-01T00:00:00.000000Z, hi=2024-01-01T23:59:59.999999Z},{lo=2024-01-08T00:00:00.000000Z, hi=2024-01-08T23:59:59.999999Z}]",
                "2024-01-[01..09]#mon"
        );
        assertTickExprAtMonday(
                configuration,
                "[{lo=2024-01-01T00:00:00.000000Z, hi=2024-01-01T05:59:59.999999Z},"
                        + "{lo=2024-01-01T18:00:00.000000Z, hi=2024-01-02T05:59:59.999999Z},"
                        + "{lo=2024-01-02T18:00:00.000000Z, hi=2024-01-02T23:59:59.999999Z},"
                        + "{lo=2024-01-08T00:00:00.000000Z, hi=2024-01-08T05:59:59.999999Z},"
                        + "{lo=2024-01-08T18:00:00.000000Z, hi=2024-01-09T05:59:59.999999Z},"
                        + "{lo=2024-01-09T18:00:00.000000Z, hi=2024-01-09T23:59:59.999999Z}]",
                "2024-01-[01..09]T[00:00,18:00]#mon,tue;6h"
        );
        assertTickExprAtMonday(
                configuration,
                "[{lo=2024-01-01T00:00:00.000000Z, hi=2024-01-01T23:59:59.999999Z},{lo=2024-01-05T00:00:00.000000Z, hi=2024-01-05T23:59:59.999999Z},{lo=2024-01-08T00:00:00.000000Z, hi=2024-01-08T23:59:59.999999Z}]",
                "[2024-01-05,2024-01-[01..09]#mon]"
        );
    }

    @Test
    public void testDayFilterBracketExpansionAtMaxIntervals() throws SqlException {
        // 21 years x 12 months x 4 Mondays among days 1-28 = 1,008 single-day intervals
        out.clear();
        parseTickExprAtMonday(configuration, out, "[2004..2024]-[01..12]-[01..28]#mon");
        assertFullDayMondays(out, 1_008);
    }

    @Test
    public void testDayFilterBracketExpansionMergeCount() throws SqlException {
        // 21 years x 12 months x 28 days = 7,056 leaves. The pending day filter merges once per 256
        // unfiltered leaves, and the cap is read once per merge, once in the -ea assert of
        // parseTickExpr and once in the final check. The count depends on the merge block reading
        // the cap only when it merges.
        final int getterCallCount = countMaxIntervalsGetterCalls("[2004..2024]-[01..12]-[01..28]#mon");
        Assert.assertTrue("getter calls: " + getterCallCount, getterCallCount <= 7_056 / 256 + 2);
    }

    @Test
    public void testDayFilterBracketExpansionOverMaxIntervals() {
        // 22 years x 12 months x 4 Mondays among days 1-28 = 1,056 intervals > 1,024
        assertTickExprErrorAtMonday("[2003..2024]-[01..12]-[01..28]#mon", "Bracket expansion produces too many intervals (max 1024)");
    }

    @Test
    public void testDayFilterBracketExpansionWithTimezoneConsistent() throws SqlException {
        // Bracket expansion path with timezone and day filter
        //
        // This uses bracket expansion with global timezone.
        // 2024-01-01 IS a Monday in local time (+12:00)
        // 2024-01-02 IS a Tuesday in local time (+12:00)
        //
        // Day filter is applied BEFORE timezone conversion,
        // so it correctly identifies 2024-01-01 as Monday and keeps it,
        // while filtering out 2024-01-02 (Tuesday).
        //
        // Conversion to UTC:
        // - 2024-01-01 00:00 +12:00 = 2023-12-31 12:00 UTC
        // - 2024-01-01 23:59 +12:00 = 2024-01-01 11:59 UTC

        final TimestampDriver timestampDriver = timestampType.getDriver();
        out.clear();
        String interval = "2024-01-[01..02]@+12:00#Mon";
        parseTickExpr(timestampDriver, interval, 0, interval.length(), 0, out);

        // Non-date-list path: day filter applied before TZ conversion
        // 2024-01-01 is Monday (local) → kept, 2024-01-02 is Tuesday → filtered out
        // Then converted to UTC
        Assert.assertEquals("Bracket expansion correctly filters by local day", 2, out.size());

        // Verify the UTC timestamp corresponds to 2024-01-01 (Monday) in +12:00
        String expected = ColumnType.isTimestampNano(timestampType.getTimestampType())
                ? "[{lo=2023-12-31T12:00:00.000000000Z, hi=2024-01-01T11:59:59.999999999Z}]"
                : "[{lo=2023-12-31T12:00:00.000000Z, hi=2024-01-01T11:59:59.999999Z}]";
        TestUtils.assertEquals(expected, intervalToString(timestampDriver, out));
    }

    @Test
    public void testDayFilterCaseInsensitive() throws SqlException {
        // Test case insensitivity
        assertBracketInterval(
                "[{lo=2024-01-06T00:00:00.000000Z, hi=2024-01-07T23:59:59.999999Z}]",
                "2024-01-[01..07]#WEEKEND"
        );
    }

    @Test
    public void testDayFilterDateListAllMatch() throws SqlException {
        // Date list where all elements match day filter
        // 2024-01-01, 2024-01-08, 2024-01-15 are all Mondays
        assertBracketInterval(
                "[{lo=2024-01-01T00:00:00.000000Z, hi=2024-01-01T23:59:59.999999Z},{lo=2024-01-08T00:00:00.000000Z, hi=2024-01-08T23:59:59.999999Z},{lo=2024-01-15T00:00:00.000000Z, hi=2024-01-15T23:59:59.999999Z}]",
                "[2024-01-01,2024-01-08,2024-01-15]#Mon"
        );
    }

    @Test
    public void testDayFilterDateListMixedMatching() throws SqlException {
        // Date list with mixed matching/non-matching elements
        // 2024-01-01 is Monday, 2024-01-02 is Tuesday, 2024-01-03 is Wednesday
        // Only Monday should survive
        assertBracketInterval(
                "[{lo=2024-01-01T00:00:00.000000Z, hi=2024-01-01T23:59:59.999999Z}]",
                "[2024-01-01,2024-01-02,2024-01-03]#Mon"
        );
    }

    @Test
    public void testDayFilterDateListMixedPerElementAndGlobalTimezone() throws SqlException {
        // Mixed per-element and global timezone with day filter
        // First element uses +05:00, second uses global +12:00
        // 2024-01-01 is Monday in both timezones
        // 2024-01-02 is Tuesday - filtered out
        // 2024-01-01 00:00 +05:00 = 2023-12-31 19:00 UTC
        // 2024-01-01 23:59 +05:00 = 2024-01-01 18:59 UTC
        assertBracketInterval(
                "[{lo=2023-12-31T19:00:00.000000Z, hi=2024-01-01T18:59:59.999999Z}]",
                "[2024-01-01@+05:00,2024-01-02]@+12:00#Mon"
        );
    }

    @Test
    public void testDayFilterDateListMultipleDays() throws SqlException {
        // Comma-separated day filter with date list
        // 2024-01-01 is Mon, 02 is Tue, 03 is Wed
        // #Mon,Wed keeps Mon and Wed
        assertBracketInterval(
                "[{lo=2024-01-01T00:00:00.000000Z, hi=2024-01-01T23:59:59.999999Z},{lo=2024-01-03T00:00:00.000000Z, hi=2024-01-03T23:59:59.999999Z}]",
                "[2024-01-01,2024-01-02,2024-01-03]#Mon,Wed"
        );
    }

    @Test
    public void testDayFilterDateListMultipleMondays() throws SqlException {
        // Date list with multiple Mondays and a non-Monday
        // 2024-01-01 and 2024-01-08 are Mondays, 2024-01-02 is Tuesday
        // Result: only Mondays (01 and 08)
        assertBracketInterval(
                "[{lo=2024-01-01T00:00:00.000000Z, hi=2024-01-01T23:59:59.999999Z},{lo=2024-01-08T00:00:00.000000Z, hi=2024-01-08T23:59:59.999999Z}]",
                "[2024-01-01,2024-01-08,2024-01-02]#Mon"
        );
    }

    @Test
    public void testDayFilterDateListNegativeTimezone() throws SqlException {
        // Negative timezone offset edge case
        // 2024-01-01 00:00 in -12:00 = 2024-01-01 12:00 UTC (still Monday)
        assertBracketInterval(
                "[{lo=2024-01-01T12:00:00.000000Z, hi=2024-01-02T11:59:59.999999Z}]",
                "[2024-01-01@-12:00]#Mon"
        );
    }

    @Test
    public void testDayFilterDateListNoneMatch() throws SqlException {
        // Date list where no elements match day filter - should produce empty result
        // 2024-01-02 is Tuesday, 2024-01-03 is Wednesday
        final TimestampDriver timestampDriver = timestampType.getDriver();
        out.clear();
        String interval = "[2024-01-02,2024-01-03]#Mon";
        parseTickExpr(timestampDriver, interval, 0, interval.length(), 0, out);
        Assert.assertEquals(0, out.size());
    }

    @Test
    public void testDayFilterDateListOppositeTimezones() throws SqlException {
        // Same date with opposite timezones - both are Monday locally
        // 2024-01-01 00:00 +12:00 = 2023-12-31 12:00 UTC, ends at 2024-01-01 12:00 UTC
        // 2024-01-01 00:00 -12:00 = 2024-01-01 12:00 UTC, ends at 2024-01-02 12:00 UTC
        // Both days are Monday in their respective local times
        // The two intervals are adjacent and get merged
        assertBracketInterval(
                "[{lo=2023-12-31T12:00:00.000000Z, hi=2024-01-02T11:59:59.999999Z}]",
                "[2024-01-01@+12:00,2024-01-01@-12:00]#Mon"
        );
    }

    @Test
    public void testDayFilterDateListTimezoneChangesDayOfWeek() throws SqlException {
        // Edge case: UTC timestamp is Sunday, but local time is Monday
        // 2024-01-01 (Monday) at 00:00 in +14:00 = 2023-12-31 (Sunday) 10:00 UTC
        // Day filter should keep this because it's Monday locally
        assertBracketInterval(
                "[{lo=2023-12-31T10:00:00.000000Z, hi=2024-01-01T09:59:59.999999Z}]",
                "[2024-01-01@+14:00]#Mon"
        );
    }

    @Test
    public void testDayFilterDateListWeekend() throws SqlException {
        // Weekend filter with date list
        // 2024-01-05 is Friday, 06 is Saturday, 07 is Sunday
        assertBracketInterval(
                "[{lo=2024-01-06T00:00:00.000000Z, hi=2024-01-07T23:59:59.999999Z}]",
                "[2024-01-05,2024-01-06,2024-01-07]#weekend"
        );
    }

    @Test
    public void testDayFilterDateListWithDuration() throws SqlException {
        // Date list with duration + day filter
        // 2024-01-01 is Monday, 2024-01-02 is Tuesday
        assertBracketInterval(
                "[{lo=2024-01-01T09:00:00.000000Z, hi=2024-01-01T09:59:59.999999Z}]",
                "[2024-01-01,2024-01-02]T09:00#Mon;1h"
        );
    }

    @Test
    public void testDayFilterDateListWithDurationAndTimezone() throws SqlException {
        // Date list with duration + timezone + day filter
        // 2024-01-01 is Monday in +05:30
        // 2024-01-01 09:00 +05:30 = 2024-01-01 03:30 UTC
        assertBracketInterval(
                "[{lo=2024-01-01T03:30:00.000000Z, hi=2024-01-01T04:29:59.999999Z}]",
                "[2024-01-01,2024-01-02]T09:00@+05:30#Mon;1h"
        );
    }

    // CompiledTickExpression (dynamic date variable) tests

    @Test
    public void testDayFilterDateListWithGlobalTimezone() throws SqlException {
        // Date list with global timezone + day filter
        // 2024-01-01 is Monday, 2024-01-02 is Tuesday (in +05:30 local time)
        // Day filter applied based on local time, then converted to UTC
        // 2024-01-01 00:00 +05:30 = 2023-12-31 18:30 UTC
        // 2024-01-01 23:59:59 +05:30 = 2024-01-01 18:29:59 UTC
        assertBracketInterval(
                "[{lo=2023-12-31T18:30:00.000000Z, hi=2024-01-01T18:29:59.999999Z}]",
                "[2024-01-01,2024-01-02]@+05:30#Mon"
        );
    }

    @Test
    public void testDayFilterDateListWithPerElementTimezone() throws SqlException {
        // Day filter with per-element timezone in date list
        //
        // 2024-01-01 IS a Monday in local time (+12:00)
        // Day filter should be based on LOCAL time, so the interval should be kept.
        //
        // After timezone conversion:
        // 2024-01-01 00:00 in +12:00 = 2023-12-31 12:00 UTC
        // 2024-01-01 23:59:59.999999 in +12:00 = 2024-01-01 11:59:59.999999 UTC

        final TimestampDriver timestampDriver = timestampType.getDriver();
        out.clear();
        String interval = "[2024-01-01@+12:00]#Mon";
        parseTickExpr(timestampDriver, interval, 0, interval.length(), 0, out);

        // Day filter is applied based on local time (Monday), interval is kept
        Assert.assertEquals(2, out.size());
        String expected = ColumnType.isTimestampNano(timestampType.getTimestampType())
                ? "[{lo=2023-12-31T12:00:00.000000000Z, hi=2024-01-01T11:59:59.999999999Z}]"
                : "[{lo=2023-12-31T12:00:00.000000Z, hi=2024-01-01T11:59:59.999999Z}]";
        TestUtils.assertEquals(expected, intervalToString(timestampDriver, out));
    }

    @Test
    public void testDayFilterDateListWithTimeSuffix() throws SqlException {
        // Date list with time suffix + day filter
        // 2024-01-01 is Monday, 2024-01-02 is Tuesday
        assertBracketInterval(
                "[{lo=2024-01-01T09:00:00.000000Z, hi=2024-01-01T09:00:59.999999Z}]",
                "[2024-01-01,2024-01-02]T09:00#Mon"
        );
    }

    @Test
    public void testDayFilterEmptyResult() throws SqlException {
        // Filter that removes all intervals (e.g., looking for Monday in a Tue-Thu range)
        // 2024-01-02 is Tuesday, 2024-01-04 is Thursday
        final TimestampDriver timestampDriver = timestampType.getDriver();
        out.clear();
        String interval = "2024-01-[02..04]#Mon";
        parseTickExpr(timestampDriver, interval, 0, interval.length(), 0, out);
        Assert.assertEquals(0, out.size());
    }

    @Test
    public void testDayFilterErrorEmpty() {
        assertBracketIntervalError("2024-01-[01..07]#", "Empty day filter after '#'");
    }

    @Test
    public void testDayFilterErrorInvalidDay() {
        assertBracketIntervalError("2024-01-[01..07]#invalid", "Invalid day name");
    }

    @Test
    public void testDayFilterErrorInvalidDayInList() {
        assertBracketIntervalError("2024-01-[01..07]#Mon,invalid,Fri", "Invalid day name");
    }

    @Test
    public void testDayFilterErrorOnlyCommas() {
        // Test that #,,, results in "Invalid day filter" error (mask == 0)
        assertBracketIntervalError("2024-01-[01..07]#,,,", "Invalid day filter");
    }

    @Test
    public void testDayFilterFullDayNames() throws SqlException {
        // Test full day names - Monday and Wednesday
        assertBracketInterval(
                "[{lo=2024-01-01T00:00:00.000000Z, hi=2024-01-01T23:59:59.999999Z},{lo=2024-01-03T00:00:00.000000Z, hi=2024-01-03T23:59:59.999999Z}]",
                "2024-01-[01..07]#Monday,Wednesday"
        );
    }

    @Test
    public void testDayFilterFullDayNamesAllDays() throws SqlException {
        // Test all full day names individually
        // 2024-01-01=Mon, 02=Tue, 03=Wed, 04=Thu, 05=Fri, 06=Sat, 07=Sun

        // Tuesday
        assertBracketInterval(
                "[{lo=2024-01-02T00:00:00.000000Z, hi=2024-01-02T23:59:59.999999Z}]",
                "2024-01-[01..07]#Tuesday"
        );

        // Thursday
        assertBracketInterval(
                "[{lo=2024-01-04T00:00:00.000000Z, hi=2024-01-04T23:59:59.999999Z}]",
                "2024-01-[01..07]#Thursday"
        );

        // Friday
        assertBracketInterval(
                "[{lo=2024-01-05T00:00:00.000000Z, hi=2024-01-05T23:59:59.999999Z}]",
                "2024-01-[01..07]#Friday"
        );

        // Saturday
        assertBracketInterval(
                "[{lo=2024-01-06T00:00:00.000000Z, hi=2024-01-06T23:59:59.999999Z}]",
                "2024-01-[01..07]#Saturday"
        );

        // Sunday
        assertBracketInterval(
                "[{lo=2024-01-07T00:00:00.000000Z, hi=2024-01-07T23:59:59.999999Z}]",
                "2024-01-[01..07]#Sunday"
        );

        // Mixed full names
        assertBracketInterval(
                "[{lo=2024-01-02T00:00:00.000000Z, hi=2024-01-02T23:59:59.999999Z},{lo=2024-01-04T00:00:00.000000Z, hi=2024-01-04T23:59:59.999999Z},{lo=2024-01-06T00:00:00.000000Z, hi=2024-01-06T23:59:59.999999Z}]",
                "2024-01-[01..07]#Tuesday,Thursday,Saturday"
        );
    }

    @Test
    public void testDayFilterIsoWeek() throws SqlException {
        // ISO week is exactly 7 days, so totalDays % 7 == 0 (no remainder)
        // 2024-W01 is Mon Jan 1 to Sun Jan 7
        // #Tue should return just Tuesday Jan 2
        assertBracketInterval(
                "[{lo=2024-01-02T00:00:00.000000Z, hi=2024-01-02T23:59:59.999999Z}]",
                "2024-W01#Tue"
        );
    }

    @Test
    public void testDayFilterIsoWeekMultipleDays() throws SqlException {
        // ISO week with workday filter (Mon-Fri) = 5 days
        // 2024-W01 is Mon Jan 1 to Sun Jan 7
        // #workday should return Mon-Fri (Jan 1-5)
        assertBracketInterval(
                "[{lo=2024-01-01T00:00:00.000000Z, hi=2024-01-01T23:59:59.999999Z}," +
                        "{lo=2024-01-02T00:00:00.000000Z, hi=2024-01-02T23:59:59.999999Z}," +
                        "{lo=2024-01-03T00:00:00.000000Z, hi=2024-01-03T23:59:59.999999Z}," +
                        "{lo=2024-01-04T00:00:00.000000Z, hi=2024-01-04T23:59:59.999999Z}," +
                        "{lo=2024-01-05T00:00:00.000000Z, hi=2024-01-05T23:59:59.999999Z}]",
                "2024-W01#workday"
        );
    }

    @Test
    public void testDayFilterLeavesFewIntervalsAtIncrementalMerge() throws SqlException {
        // the filter at a merge point may leave no interval or a single one: 2024-01-02..07 holds
        // no Monday, and 2024-01-01..07 holds one
        assertTickExprAtMonday(mergeThreshold4Configuration, "[]", "2024-01-[02..07]#mon");
        assertTickExprAtMonday(
                mergeThreshold4Configuration,
                "[{lo=2024-01-01T00:00:00.000000Z, hi=2024-01-01T23:59:59.999999Z}]",
                "2024-01-[01..07]#mon"
        );
    }

    @Test
    public void testDayFilterLocalTimeSemanticsMonday() throws SqlException {
        // Bug investigation: Day filter should use local-time semantics, not UTC
        // The bug occurs when timezone conversion causes the date to cross midnight.
        //
        // nowTimestamp = 2026-01-26T23:30:00Z (treated as "23:30 local" by timezone logic)
        // Expression: [$now - 1h..$now]@America/New_York#Mon
        // Local time range: 22:30-23:30 on 2026-01-26 (Monday)
        // After timezone conversion (+5h for EST): 03:30-04:30 UTC on 2026-01-27 (Tuesday)
        //
        // If day filter uses LOCAL semantics (correct): checks 2026-01-26 = Monday → matches #Mon
        // If day filter uses UTC semantics (bug): checks 2026-01-27 = Tuesday → no match → empty
        // Note: assertBracketIntervalWithNow transforms 00Z→00000Z for nano, so use micro format
        assertBracketIntervalWithNow(
                "[{lo=2026-01-27T03:30:00.000000Z, hi=2026-01-27T04:30:00.000000Z}]",
                "[$now - 1h..$now]@America/New_York#Mon",
                "2026-01-26T23:30:00.000000Z"
        );
    }

    @Test
    public void testDayFilterLocalTimeSemanticsNotTuesday() throws SqlException {
        // Complementary test: Same scenario but with #Tue filter
        // nowTimestamp = 2026-01-26T23:30:00Z (treated as "23:30 local Monday")
        // Expression: [$now - 1h..$now]@America/New_York#Tue
        // Local time: Monday evening → does NOT match #Tue → should be empty
        //
        // If day filter uses LOCAL semantics (correct): Monday doesn't match #Tue → empty
        // If day filter uses UTC semantics (bug): checks UTC date Tuesday → matches #Tue → returns interval
        assertBracketIntervalWithNow(
                "[]",
                "[$now - 1h..$now]@America/New_York#Tue",
                "2026-01-26T23:30:00.000000Z"
        );
    }

    @Test
    public void testDayFilterMonthStartingSunday() throws SqlException {
        // December 2024 starts on Sunday (dow=6), has 31 days
        // remainderDays = 31 % 7 = 3, startDow + remainderDays = 6 + 3 = 9 > 7 → wrap-around case
        // #Mon should return all 5 Mondays: Dec 2, 9, 16, 23, 30
        final TimestampDriver timestampDriver = timestampType.getDriver();
        out.clear();
        String interval = "2024-12#Mon";
        parseTickExpr(timestampDriver, interval, 0, interval.length(), 0, out);

        // Should have 5 Mondays
        Assert.assertEquals(10, out.size());

        String result = intervalToString(timestampDriver, out).toString();
        Assert.assertTrue("Should have Dec 2", result.contains("2024-12-02T00:00:00"));
        Assert.assertTrue("Should have Dec 30", result.contains("2024-12-30T"));
    }

    @Test
    public void testDayFilterMultipleDays() throws SqlException {
        // 2024-01-01 is Monday, 2024-01-03 is Wednesday, 2024-01-05 is Friday
        assertBracketInterval(
                "[{lo=2024-01-01T00:00:00.000000Z, hi=2024-01-01T23:59:59.999999Z},{lo=2024-01-03T00:00:00.000000Z, hi=2024-01-03T23:59:59.999999Z},{lo=2024-01-05T00:00:00.000000Z, hi=2024-01-05T23:59:59.999999Z}]",
                "2024-01-[01..07]#Mon,Wed,Fri"
        );
    }

    @Test
    public void testDayFilterPreciseTimeAtEndOfDay() throws SqlException {
        // Precise time that happens to be at end-of-day
        // 2024-01-01T23:59:59 with minute precision ends at 23:59:59.999999
        // This should NOT be treated as a "natural" interval and expanded
        // 2024-01-01 is Monday, so #Mon should keep it
        assertBracketInterval(
                "[{lo=2024-01-01T23:59:00.000000Z, hi=2024-01-01T23:59:59.999999Z}]",
                "[2024-01-01T23:59#Mon]"
        );
    }

    @Test
    public void testDayFilterSingleDate() throws SqlException {
        // Single date with day filter - should keep if matches
        // 2024-01-01 is Monday
        assertBracketInterval(
                "[{lo=2024-01-01T00:00:00.000000Z, hi=2024-01-01T23:59:59.999999Z}]",
                "2024-01-01#Mon"
        );
    }

    @Test
    public void testDayFilterSingleDateNoMatch() throws SqlException {
        // Single date with day filter - should filter out if doesn't match
        // 2024-01-01 is Monday, asking for Tuesday
        final TimestampDriver timestampDriver = timestampType.getDriver();
        out.clear();
        String interval = "2024-01-01#Tue";
        parseTickExpr(timestampDriver, interval, 0, interval.length(), 0, out);
        Assert.assertEquals(0, out.size());
    }

    @Test
    public void testDayFilterSpecificDay() throws SqlException {
        // 2024-01 has Mondays on: 1, 8, 15, 22, 29
        assertBracketInterval(
                "[{lo=2024-01-01T00:00:00.000000Z, hi=2024-01-01T23:59:59.999999Z},{lo=2024-01-08T00:00:00.000000Z, hi=2024-01-08T23:59:59.999999Z},{lo=2024-01-15T00:00:00.000000Z, hi=2024-01-15T23:59:59.999999Z},{lo=2024-01-22T00:00:00.000000Z, hi=2024-01-22T23:59:59.999999Z},{lo=2024-01-29T00:00:00.000000Z, hi=2024-01-29T23:59:59.999999Z}]",
                "2024-01-[01..31]#Mon"
        );
    }

    @Test
    public void testDayFilterWeekend() throws SqlException {
        // 2024-01-06 is Saturday, 2024-01-07 is Sunday
        assertBracketInterval(
                "[{lo=2024-01-06T00:00:00.000000Z, hi=2024-01-07T23:59:59.999999Z}]",
                "2024-01-[01..07]#weekend"
        );
    }

    @Test
    public void testDayFilterWithDateList() throws SqlException {
        // Date list with day filter
        // 2024-01-01 is Monday, 2024-01-06 is Saturday
        assertBracketInterval(
                "[{lo=2024-01-01T00:00:00.000000Z, hi=2024-01-01T23:59:59.999999Z}]",
                "[2024-01-01,2024-01-06]#Mon"
        );
    }

    @Test
    public void testDayFilterWithDuration() throws SqlException {
        // 2024-01-01 is Monday, 2024-01-07 is Sunday
        // #workday should keep Mon-Fri (01..05) with 1h duration
        assertBracketInterval(
                "[{lo=2024-01-01T09:00:00.000000Z, hi=2024-01-01T09:59:59.999999Z},{lo=2024-01-02T09:00:00.000000Z, hi=2024-01-02T09:59:59.999999Z},{lo=2024-01-03T09:00:00.000000Z, hi=2024-01-03T09:59:59.999999Z},{lo=2024-01-04T09:00:00.000000Z, hi=2024-01-04T09:59:59.999999Z},{lo=2024-01-05T09:00:00.000000Z, hi=2024-01-05T09:59:59.999999Z}]",
                "2024-01-[01..07]T09:00#workday;1h"
        );
    }

    // End Date Variable Tests

    @Test
    public void testDayFilterWithMultiDayDuration() throws SqlException {
        // Single date with 3d duration and day filter
        // 2024-01-01 is Monday. With ;3d duration, interval spans Jan 1-4
        // Day filter #Mon should keep the ENTIRE interval, not expand
        assertBracketInterval(
                "[{lo=2024-01-01T00:00:00.000000Z, hi=2024-01-03T23:59:59.999999Z}]",
                "[2024-01-01#Mon];3d"
        );
    }

    @Test
    public void testDayFilterWithOneDayDuration() throws SqlException {
        // Single date with 1d duration and day filter
        // 2024-01-01 is Monday. With ;1d duration, interval spans Jan 1-2 (ends at end of Jan 2)
        // Day filter #Mon should check if Jan 1 is Monday (yes) and keep the ENTIRE 2-day interval
        // Should NOT expand into individual days
        assertBracketInterval(
                "[{lo=2024-01-01T00:00:00.000000Z, hi=2024-01-01T23:59:59.999999Z}]",
                "[2024-01-01#Mon];1d"
        );
    }

    @Test
    public void testDayFilterWithOneDayDurationNoMatch() throws SqlException {
        // Single date with 1d duration and day filter that doesn't match
        // 2024-01-01 is Monday. With ;1d duration and #Tue filter, should be empty
        assertBracketInterval(
                "[]",
                "[2024-01-01#Tue];1d"
        );
    }

    @Test
    public void testDayFilterWithTimezone() throws SqlException {
        // Day filter with timezone: date@timezone#dayFilter;duration
        // 2024-01-01 is Monday in local time (+02:00)
        // Filter workdays, then convert to UTC
        // 09:00 local (+02:00) = 07:00 UTC
        assertBracketInterval(
                "[{lo=2024-01-01T07:00:00.000000Z, hi=2024-01-01T07:59:59.999999Z},{lo=2024-01-02T07:00:00.000000Z, hi=2024-01-02T07:59:59.999999Z},{lo=2024-01-03T07:00:00.000000Z, hi=2024-01-03T07:59:59.999999Z},{lo=2024-01-04T07:00:00.000000Z, hi=2024-01-04T07:59:59.999999Z},{lo=2024-01-05T07:00:00.000000Z, hi=2024-01-05T07:59:59.999999Z}]",
                "2024-01-[01..07]T09:00@+02:00#workday;1h"
        );
    }

    @Test
    public void testDayFilterWithTimezoneAndTimeList() throws SqlException {
        // The day filter judges the slots of a time list by their local day, before the time list
        // converts them to UTC, the same way as a single time of day. 2024-01-15 is a Monday, and
        // 08:00 in Tokyo on that day is 2024-01-14T23:00Z, a Sunday.
        final String mondayInTokyo = "[{lo=2024-01-14T23:00:00.000000Z, hi=2024-01-14T23:00:59.999999Z}]";
        assertBracketInterval(mondayInTokyo, "2024-01-[15..17]T08:00@Asia/Tokyo#Mon");
        assertBracketInterval(mondayInTokyo, "2024-01-[15..17]T[08:00]@Asia/Tokyo#Mon");
        assertBracketInterval(mondayInTokyo, "[2024-01-15,2024-01-16,2024-01-17]T[08:00]@Asia/Tokyo#Mon");
        assertBracketInterval(mondayInTokyo, "[2024-01-15#Mon,2024-01-16#Mon,2024-01-17#Mon]T[08:00]@Asia/Tokyo");
        // a slot with its own timezone falls on the day of its date
        assertBracketInterval(mondayInTokyo, "2024-01-[15..17]T[08:00@Asia/Tokyo]#Mon");
        assertBracketInterval(mondayInTokyo, "2024-01-[15..17]T[08:00@Asia/Tokyo]@America/New_York#Mon");
        // with suffix brackets, the slot expands to several intervals before the filter
        assertBracketInterval(
                "[{lo=2024-01-14T23:00:00.000000Z, hi=2024-01-14T23:00:00.999999Z},{lo=2024-01-14T23:00:30.000000Z, hi=2024-01-14T23:00:30.999999Z}]",
                "2024-01-[15..17]T[08:00]:[00,30]@Asia/Tokyo#Mon"
        );
        // a slot that runs past midnight stays whole, on the day it starts
        assertBracketInterval(
                "[{lo=2024-01-15T13:00:00.000000Z, hi=2024-01-15T16:59:59.999999Z}]",
                "2024-01-[15..17]T[22:00]@Asia/Tokyo#Mon;4h"
        );
        // 20:00 in New York on Monday 2024-01-15 is 2024-01-16T01:00Z, a Tuesday
        assertBracketInterval(
                "[{lo=2024-01-16T01:00:00.000000Z, hi=2024-01-16T01:00:59.999999Z}]",
                "2024-01-[14..16]T[20:00]@America/New_York#Mon"
        );
        // a date variable range filters its days the same way, as does the compiled path
        assertParsedAndCompiledWithNow(mondayInTokyo, "[$today..$today+2d]T[08:00]@Asia/Tokyo#Mon", "2024-01-15T12:00:00.000000Z");
        assertParsedAndCompiledWithNow(mondayInTokyo, "[$today..$today+2d#Mon]T[08:00]@Asia/Tokyo", "2024-01-15T12:00:00.000000Z");
    }

    @Test
    public void testDayFilterWorkday() throws SqlException {
        // 2024-01-01 is Monday, 2024-01-07 is Sunday
        // Range [01..07] with #workday should return Mon-Fri (01..05)
        assertBracketInterval(
                "[{lo=2024-01-01T00:00:00.000000Z, hi=2024-01-05T23:59:59.999999Z}]",
                "2024-01-[01..07]#workday"
        );
    }

    @Test
    public void testDayFilterWorkdayShort() throws SqlException {
        // Same as above but using #wd shorthand
        assertBracketInterval(
                "[{lo=2024-01-01T00:00:00.000000Z, hi=2024-01-05T23:59:59.999999Z}]",
                "2024-01-[01..07]#wd"
        );
    }

    @Test
    public void testDayFilterYearOnly() throws SqlException {
        // Year-only date with day filter expands to all matching days
        // 2022 has 52 Tuesdays, first is Jan 4, last is Dec 27
        final TimestampDriver timestampDriver = timestampType.getDriver();
        out.clear();
        String interval = "2022#Tue";
        parseTickExpr(timestampDriver, interval, 0, interval.length(), 0, out);

        // Should have 52 Tuesdays (52 intervals = 104 longs)
        Assert.assertEquals(104, out.size());

        // First Tuesday is 2022-01-04
        String result = intervalToString(timestampDriver, out).toString();
        Assert.assertTrue("Should start with Jan 4", result.contains("2022-01-04T00:00:00"));

        // Last Tuesday is 2022-12-27
        Assert.assertTrue("Should end with Dec 27", result.contains("2022-12-27T"));
    }

    @Test
    public void testDayFilterYearOnlyMatches() throws SqlException {
        // Year-only date with day filter - all Saturdays in 2022
        // 2022 has 53 Saturdays: Jan 1 is Saturday, and since 365 = 52*7 + 1, Dec 31 is also Saturday
        final TimestampDriver timestampDriver = timestampType.getDriver();
        out.clear();
        String interval = "2022#Sat";
        parseTickExpr(timestampDriver, interval, 0, interval.length(), 0, out);

        // Should have 53 Saturdays (53 intervals = 106 longs)
        Assert.assertEquals(106, out.size());

        // First Saturday is 2022-01-01
        String result = intervalToString(timestampDriver, out).toString();
        Assert.assertTrue("Should start with Jan 1", result.contains("2022-01-01T00:00:00"));

        // Last Saturday is 2022-12-31
        Assert.assertTrue("Should end with Dec 31", result.contains("2022-12-31T"));
    }

    @Test
    public void testElementOverMaxIntervalsAtLastLeafFails() throws SqlException {
        // The unions of a merge range wait until it grew by the merge threshold (256) since the last
        // one, and the outermost bracket of an expansion unions its range once more after its last
        // leaf, so the cap (1,024) counts all intervals of the range before a later element, a time
        // zone or a tick calendar shrinks them. 16 days x 8 hours x 8 minutes are 1,024 seconds that
        // never touch, at the cap, and 25 days x 41 minutes are 1,025, one leaf after the last union
        // by the threshold.
        Assert.assertEquals(256, configuration.getSqlIntervalIncrementalMergeThreshold());
        Assert.assertEquals(1_024, configuration.getSqlIntervalMaxIntervalsAfterMerge());
        final String overCapError = "Bracket expansion produces too many intervals (max 1024)";
        final LongList january = new LongList();
        addLocalDays(january, LocalDate.of(2024, 1, 1), LocalDate.of(2024, 2, 1), ZoneOffset.UTC);
        final LongList januaryAtPlus5 = new LongList();
        addLocalDays(januaryAtPlus5, LocalDate.of(2024, 1, 1), LocalDate.of(2024, 2, 1), ZoneOffset.ofHours(5));
        // A zone-less element adds its intervals to the merge range of the list, which the later
        // month covers only after the element ended.
        assertTickExprIntervals(configuration, january, "[2024-01-[01..16]T[00..07]:[00..07]:00,2024-01]");
        assertTickExprErrorAtMonday("[2024-01-[01..25]T09:[00..40]:00,2024-01]", overCapError);
        // parseTickExpr unwinds the list, and the next parse on this thread succeeds
        Assert.assertEquals(0, out.size());
        assertTickExprIntervals(configuration, january, "[2024-01-[01..16]T[00..07]:[00..07]:00,2024-01]");
        // A zoned element keeps a merge range of its own, which the earlier month cannot cover
        // before the zone ran.
        assertTickExprIntervals(configuration, januaryAtPlus5, "[2024-01@+05:00,2024-01-[01..16]T[00..07]:[00..07]:00@+05:00]");
        assertTickExprErrorAtMonday("[2024-01@+05:00,2024-01-[01..25]T09:[00..40]:00@+05:00]", overCapError);
        Assert.assertEquals(0, out.size());
        assertTickExprIntervals(configuration, januaryAtPlus5, "[2024-01@+05:00,2024-01-[01..16]T[00..07]:[00..07]:00@+05:00]");

        // At the largest merge threshold below the cap, the union by the threshold at the 1,023rd
        // leaf leaves 1 or 2 leaves for the union after the last leaf.
        final CairoConfiguration mergeThreshold1023Configuration = new DefaultCairoConfiguration(".") {
            @Override
            public int getSqlIntervalIncrementalMergeThreshold() {
                return 1_023;
            }
        };
        assertTickExprErrorAtMonday(mergeThreshold1023Configuration, "2024-01-[01..25]T09:[00..40]:00", overCapError);
        assertTickExprErrorAtMonday(mergeThreshold1023Configuration, "[2024-01-[01..25]T09:[00..40]:00,2024-01]", overCapError);
        assertTickExprIntervals(mergeThreshold1023Configuration, january, "[2024-01-[01..16]T[00..07]:[00..07]:00,2024-01]");

        // compileStaticElement expands a static element on its own, and the union after the last
        // leaf counts it, as the final check of the list does. $today is the Monday 2024-01-08. The
        // 1,024 seconds fit the cap, so the list compiles, and with $today it holds 1,025 intervals,
        // over the cap, which its evaluation rejects as the parser of the same text does.
        final String overCapList = "[$today,2024-02-[01..16]T[00..07]:[00..07]:00]";
        try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(timestampType.getDriver(), configuration, overCapList, 0, overCapList.length(), 0)) {
            Assert.assertEquals(1_025, compiled.getElementCount());
            Assert.assertEquals(1_025, compiled.getMaxIntervalCount(Integer.MAX_VALUE));
        }
        assertParsedAndCompiledErrorWithNow(overCapList, "2024-01-08T01:00:00.000000Z", overCapError);
        assertCompileTickExprError("[$today,2024-02-[01..25]T09:[00..40]:00]", overCapError);
        // compileImpreciseDateWithTime expands the months before it adds the time to each day: 180
        // years x 6 months are 1,080 months that never touch
        assertCompileTickExprError("[$today,[1900..2079]-[01,03,05,07,09,11]]T09:30", overCapError);
    }

    @Test
    public void testExpandingDayFilterMatchesUnmergedOracle() throws SqlException {
        // For these inputs an expanding day filter (year, month or week precision) gives the same
        // days whether it runs on merged blocks or on each leaf.
        assertMatchesUnmergedOracle(
                "2024-[01..12]#mon;3650d",
                "20[00..24]-[01..12]#mon,wed",
                "20[00..99]-[01,03,05,07,09,11]#sat,sun",
                "20[00..24]-[01..12]@+05:00#mon",
                "[20[00..24]-[01..12]]T22:00#mon;3h",
                "[20[00..24]-[01..12]]T22:00#mon;26h",
                "20[00..24]-W[01..52]#mon",
                "[20[00..24]-[01..12]]#mon"
        );
    }

    @Test
    public void testExpandingDayFilterMatchesUnmergedOracleOutsideNanoRange() throws SqlException {
        // TIMESTAMP_NS covers the years 1677-2262 only
        Assume.assumeTrue(timestampType == TestTimestampType.MICRO);
        assertMatchesUnmergedOracle("1[000..999]#mon", "2[100..355]#mon;100000d");
    }

    @Test
    public void testExpandingDayFilterMergesBeforeFilterPeak() {
        // TIMESTAMP_NS covers the years 1677-2262 only
        Assume.assumeTrue(timestampType == TestTimestampType.MICRO);
        // 256 year leaves of 100,000 days each overlap. The expansion merges them into one block
        // before the filter expands the block into its Mondays. Filtering each leaf first would
        // expand every leaf into about 14,000 Mondays and need about 7.3 million longs.
        final MaxIntervalsCountingConfiguration countingConfiguration = new MaxIntervalsCountingConfiguration();
        final PeakLongList peakOut = new PeakLongList();
        try {
            parseTickExprAtMonday(countingConfiguration, peakOut, "2[100..355]#mon;100000d");
            Assert.fail("too many intervals expected, got " + peakOut.size() / 2 + " intervals");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "Bracket expansion produces too many intervals (max 1024)");
        }
        Assert.assertTrue("peak longs " + peakOut.peak, peakOut.peak < 200_000);
        // one merge, the -ea assert of parseTickExpr and the final check
        Assert.assertEquals(3, countingConfiguration.getterCallCount);
    }

    @Test
    public void testImpreciseBareDuration() throws SqlException {
        assertImpreciseEquivalence("2024-02;6h30m", "[2024-02];6h30m");
    }

    @Test
    public void testImpreciseBareTime() throws SqlException {
        assertImpreciseEquivalence("2024-02T09:30", "[2024-02]T09:30");
    }

    @Test
    public void testImpreciseBareTimeDuration() throws SqlException {
        assertImpreciseEquivalence("2024-02T09:30;6h30m", "[2024-02]T09:30;6h30m");
    }

    @Test
    public void testImpreciseBracketRangeDuration() throws SqlException {
        assertImpreciseEquivalence(
                "2024-[01..02];6h30m",
                "[2024-01-[01..31], 2024-02-[01..29]];6h30m"
        );
    }

    @Test
    public void testImpreciseBracketRangeTime() throws SqlException {
        assertImpreciseEquivalence(
                "2024-[01..02]T09:30",
                "[2024-01-[01..31], 2024-02-[01..29]]T09:30"
        );
    }

    @Test
    public void testImpreciseBracketRangeTimeDayFilter() throws SqlException {
        assertImpreciseEquivalence(
                "2024-[01..02]T09:30#workday",
                "[2024-01-[01..31], 2024-02-[01..29]]T09:30#workday"
        );
    }

    @Test
    public void testImpreciseLeafDayFilterOverMaxIntervalsPeak() {
        // TIMESTAMP_NS covers the years 1677-2262 only
        Assume.assumeTrue(timestampType == TestTimestampType.MICRO);
        // 1,000 year leaves hold about 52,000 Mondays. The expanding filter keeps the
        // merge-then-filter order, so the list holds at most C + (T + B)(D + 1) intervals with
        // C = 1,024, T = 256, B = 1 and D = 366 days per leaf.
        final PeakLongList peakOut = new PeakLongList();
        try {
            parseTickExprAtMonday(configuration, peakOut, "1[000..999]#mon");
            Assert.fail("too many intervals expected, got " + peakOut.size() / 2 + " intervals");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "Bracket expansion produces too many intervals (max 1024)");
        }
        Assert.assertTrue("peak longs " + peakOut.peak, peakOut.peak <= 2 * (1_024 + 257 * 367));
    }

    @Test
    public void testImpreciseMonthAllSuffixes() throws SqlException {
        assertImpreciseEquivalence(
                "[2024-02]T09:30@+05:00#workday;6h30m",
                "[2024-02-[01..29]]T09:30@+05:00#workday;6h30m"
        );
    }

    @Test
    public void testImpreciseMonthDayFilter() throws SqlException {
        assertImpreciseEquivalence("[2024-02]#workday", "[2024-02-[01..29]]#workday");
    }

    @Test
    public void testImpreciseMonthDuration() throws SqlException {
        assertImpreciseEquivalence("[2024-02];6h30m", "[2024-02-[01..29]];6h30m");
    }

    @Test
    public void testImpreciseMonthDurationDayFilter() throws SqlException {
        assertImpreciseEquivalence("[2024-02]#workday;6h30m", "[2024-02-[01..29]]#workday;6h30m");
    }

    @Test
    public void testImpreciseMonthDurationDayFilterTimezone() throws SqlException {
        assertImpreciseEquivalence(
                "[2024-02]@+05:00#workday;6h30m",
                "[2024-02-[01..29]]@+05:00#workday;6h30m"
        );
    }

    @Test
    public void testImpreciseMonthNamedTimezone() throws SqlException {
        assertImpreciseEquivalence(
                "[2024-03]T09:30@America/New_York",
                "[2024-03-[01..31]]T09:30@America/New_York"
        );
    }

    @Test
    public void testImpreciseMonthTime() throws SqlException {
        assertImpreciseEquivalence("[2024-02]T09:30", "[2024-02-[01..29]]T09:30");
    }

    @Test
    public void testImpreciseMonthTimeDayFilter() throws SqlException {
        assertImpreciseEquivalence("[2024-02]T09:30#workday", "[2024-02-[01..29]]T09:30#workday");
    }

    @Test
    public void testImpreciseMonthTimeDuration() throws SqlException {
        assertImpreciseEquivalence("[2024-02]T09:30;6h30m", "[2024-02-[01..29]]T09:30;6h30m");
    }

    @Test
    public void testImpreciseMonthTimeDurationDayFilter() throws SqlException {
        assertImpreciseEquivalence("[2024-02]T09:30#workday;6h30m", "[2024-02-[01..29]]T09:30#workday;6h30m");
    }

    @Test
    public void testImpreciseMonthTimeList() throws SqlException {
        assertImpreciseEquivalence(
                "[2024-02]T[09:30,14:00]",
                "[2024-02-[01..29]]T[09:30,14:00]"
        );
    }

    @Test
    public void testImpreciseMonthTimeTimezone() throws SqlException {
        assertImpreciseEquivalence("[2024-02]T09:30@+05:00", "[2024-02-[01..29]]T09:30@+05:00");
    }

    // TIME LIST BRACKET TESTS

    @Test
    public void testImpreciseMonthTimeTimezoneDayFilter() throws SqlException {
        assertImpreciseEquivalence(
                "[2024-02]T09:30@+05:00#workday",
                "[2024-02-[01..29]]T09:30@+05:00#workday"
        );
    }

    @Test
    public void testImpreciseMonthTimeTimezoneDuration() throws SqlException {
        assertImpreciseEquivalence(
                "[2024-02]T09:30@+05:00;6h30m",
                "[2024-02-[01..29]]T09:30@+05:00;6h30m"
        );
    }

    @Test
    public void testImpreciseYearAllSuffixes() throws SqlException {
        assertImpreciseEquivalence(
                "[2024]T09:30@+05:00#workday;6h30m",
                "[2024-[01..12]]T09:30@+05:00#workday;6h30m"
        );
    }

    @Test
    public void testImpreciseYearDayFilter() throws SqlException {
        assertImpreciseEquivalence("[2024]#workday", "[2024-[01..12]]#workday");
    }

    @Test
    public void testImpreciseYearDuration() throws SqlException {
        assertImpreciseEquivalence("[2024];6h30m", "[2024-[01..12]];6h30m");
    }

    @Test
    public void testImpreciseYearDurationDayFilter() throws SqlException {
        assertImpreciseEquivalence("[2024]#workday;6h30m", "[2024-[01..12]]#workday;6h30m");
    }

    @Test
    public void testImpreciseYearTime() throws SqlException {
        assertImpreciseEquivalence("[2024]T09:30", "[2024-[01..12]]T09:30");
    }

    @Test
    public void testImpreciseYearTimeDayFilter() throws SqlException {
        assertImpreciseEquivalence("[2024]T09:30#workday", "[2024-[01..12]]T09:30#workday");
    }

    @Test
    public void testImpreciseYearTimeDuration() throws SqlException {
        assertImpreciseEquivalence("[2024]T09:30;6h30m", "[2024-[01..12]]T09:30;6h30m");
    }

    @Test
    public void testImpreciseYearTimeDurationDayFilter() throws SqlException {
        assertImpreciseEquivalence(
                "[2024]T09:30#workday;6h30m",
                "[2024-[01..12]]T09:30#workday;6h30m"
        );
    }

    @Test
    public void testIntersectContain2() {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        a.add(timestampDriver.parseFloorLiteral("2016-03-10T10:00:00.000Z"));
        a.add(timestampDriver.parseFloorLiteral("2016-03-10T12:00:00.000Z"));

        b.add(timestampDriver.parseFloorLiteral("2016-03-10T09:00:00.000Z"));
        b.add(timestampDriver.parseFloorLiteral("2016-03-10T13:30:00.000Z"));

        assertIntersect("[{lo=2016-03-10T10:00:00.000000Z, hi=2016-03-10T12:00:00.000000Z}]");
    }

    @Test
    public void testIntersectMergeOverlap() {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        a.add(timestampDriver.parseFloorLiteral("2016-03-10T10:00:00.000Z"));
        a.add(timestampDriver.parseFloorLiteral("2016-03-10T12:00:00.000Z"));

        b.add(timestampDriver.parseFloorLiteral("2016-03-10T11:00:00.000Z"));
        b.add(timestampDriver.parseFloorLiteral("2016-03-10T14:00:00.000Z"));

        assertIntersect("[{lo=2016-03-10T11:00:00.000000Z, hi=2016-03-10T12:00:00.000000Z}]");
    }

    @Test
    public void testIntersectMergeOverlap2() {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        a.add(timestampDriver.parseFloorLiteral("2016-03-10T10:00:00.000Z"));
        a.add(timestampDriver.parseFloorLiteral("2016-03-10T12:00:00.000Z"));

        b.add(timestampDriver.parseFloorLiteral("2016-03-10T11:00:00.000Z"));
        b.add(timestampDriver.parseFloorLiteral("2016-03-10T14:00:00.000Z"));

        assertIntersect("[{lo=2016-03-10T11:00:00.000000Z, hi=2016-03-10T12:00:00.000000Z}]");
    }

    @Test
    public void testIntersectNoOverlap() {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        a.add(timestampDriver.parseFloorLiteral("2016-03-10T10:00:00.000Z"));
        a.add(timestampDriver.parseFloorLiteral("2016-03-10T12:00:00.000Z"));
        a.add(timestampDriver.parseFloorLiteral("2016-03-10T14:00:00.000Z"));
        a.add(timestampDriver.parseFloorLiteral("2016-03-10T16:00:00.000Z"));

        b.add(timestampDriver.parseFloorLiteral("2016-03-10T13:00:00.000Z"));
        b.add(timestampDriver.parseFloorLiteral("2016-03-10T13:30:00.000Z"));

        assertIntersect("[]");
    }

    @Test
    public void testIntersectSame() {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        a.add(timestampDriver.parseFloorLiteral("2016-03-10T10:00:00.000Z"));
        a.add(timestampDriver.parseFloorLiteral("2016-03-10T12:00:00.000Z"));

        b.add(timestampDriver.parseFloorLiteral("2016-03-10T10:00:00.000Z"));
        b.add(timestampDriver.parseFloorLiteral("2016-03-10T12:00:00.000Z"));

        assertIntersect("[{lo=2016-03-10T10:00:00.000000Z, hi=2016-03-10T12:00:00.000000Z}]");
    }

    @Test
    public void testIntersectTwoOverlapOne2() {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        a.add(timestampDriver.parseFloorLiteral("2016-03-10T10:00:00.000Z"));
        a.add(timestampDriver.parseFloorLiteral("2016-03-10T12:00:00.000Z"));

        a.add(timestampDriver.parseFloorLiteral("2016-03-10T14:00:00.000Z"));
        a.add(timestampDriver.parseFloorLiteral("2016-03-10T16:00:00.000Z"));

        b.add(timestampDriver.parseFloorLiteral("2016-03-10T11:00:00.000Z"));
        b.add(timestampDriver.parseFloorLiteral("2016-03-10T15:00:00.000Z"));

        assertIntersect("[{lo=2016-03-10T11:00:00.000000Z, hi=2016-03-10T12:00:00.000000Z},{lo=2016-03-10T14:00:00.000000Z, hi=2016-03-10T15:00:00.000000Z}]");
    }

    @Test
    public void testInvert() throws SqlException {
        final String intervalStr = "2018-01-10T10:30:00.000Z;30m;2d;2";
        out.clear();
        TimestampDriver timestampDriver = ColumnType.getTimestampDriver(ColumnType.TIMESTAMP);
        parseTickExpr(timestampDriver, intervalStr, 0, intervalStr.length(), 0, out);
        IntervalUtils.invert(out);
        TestUtils.assertEquals(
                "[{lo=, hi=2018-01-10T10:29:59.999999Z},{lo=2018-01-10T11:00:00.000000Z, hi=2018-01-12T10:29:59.999999Z},{lo=2018-01-12T11:00:00.000000Z, hi=294247-01-10T04:00:54.775807Z}]",
                intervalToString(timestampDriver, out)
        );
    }

    @Test
    public void testIsoWeek2015Has53Weeks() throws SqlException {
        // 2015 also has 53 weeks (Dec 31, 2015 was Thursday)
        // Week 53 of 2015 starts Mon 2015-12-28
        assertBracketInterval(
                "[{lo=2015-12-28T00:00:00.000000Z, hi=2016-01-03T23:59:59.999999Z}]",
                "2015-W53"
        );
    }

    @Test
    public void testIsoWeekBasic() throws SqlException {
        // 2024-W01 is the first week of 2024, starting Mon 2024-01-01
        // Week 1 spans Mon 2024-01-01 to Sun 2024-01-07
        assertBracketInterval(
                "[{lo=2024-01-01T00:00:00.000000Z, hi=2024-01-07T23:59:59.999999Z}]",
                "2024-W01"
        );
    }

    @Test
    public void testIsoWeekBracketExpansionDays() throws SqlException {
        // Bracket expansion for day of week - weekdays (Mon-Fri) of week 1
        // These are adjacent days so they should merge
        assertBracketInterval(
                "[{lo=2024-01-01T00:00:00.000000Z, hi=2024-01-05T23:59:59.999999Z}]",
                "2024-W01-[1..5]"
        );
    }

    @Test
    public void testIsoWeekBracketExpansionInvalidWeekInRange() {
        // Range that includes invalid week 53 for 2024 (only has 52 weeks)
        assertBracketIntervalError("2024-W[52..53]", "Invalid date");
    }

    @Test
    public void testIsoWeekBracketExpansionWeek() throws SqlException {
        // Bracket expansion for weeks - first 4 weeks of 2024
        // These are adjacent weeks so they should merge
        assertBracketInterval(
                "[{lo=2024-01-01T00:00:00.000000Z, hi=2024-01-28T23:59:59.999999Z}]",
                "2024-W[01..04]"
        );
    }

    @Test
    public void testIsoWeekBracketExpansionWeekAndDay() throws SqlException {
        // Cartesian product: 2 weeks × 2 days = 4 intervals
        // 2024-W01-1 (Mon Jan 1), 2024-W01-5 (Fri Jan 5), 2024-W02-1 (Mon Jan 8), 2024-W02-5 (Fri Jan 12)
        assertBracketInterval(
                "[{lo=2024-01-01T00:00:00.000000Z, hi=2024-01-01T23:59:59.999999Z},{lo=2024-01-05T00:00:00.000000Z, hi=2024-01-05T23:59:59.999999Z},{lo=2024-01-08T00:00:00.000000Z, hi=2024-01-08T23:59:59.999999Z},{lo=2024-01-12T00:00:00.000000Z, hi=2024-01-12T23:59:59.999999Z}]",
                "2024-W[01,02]-[1,5]"
        );
    }

    @Test
    public void testIsoWeekFloorParsing() throws NumericException {
        // Test parseFloor for ISO week format
        assertDateFloor("2024-01-01T00:00:00.000000Z", "2024-W01");
        assertDateFloor("2024-01-01T00:00:00.000000Z", "2024-W01-1");
        assertDateFloor("2024-01-05T00:00:00.000000Z", "2024-W01-5");
        assertDateFloor("2024-01-01T09:00:00.000000Z", "2024-W01-1T09:00");
        assertDateFloor("2024-01-01T09:30:15.000000Z", "2024-W01-1T09:30:15");
        assertDateFloor("2024-01-01T09:30:15.123456Z", "2024-W01-1T09:30:15.123456");
    }

    @Test
    public void testIsoWeekHourOnly() throws SqlException {
        // Hour-only time (no minutes)
        assertBracketInterval(
                "[{lo=2024-01-01T09:00:00.000000Z, hi=2024-01-01T09:59:59.999999Z}]",
                "2024-W01-1T09"
        );
    }

    @Test
    public void testIsoWeekInvalidDay() {
        // Day of week must be 1-7
        assertBracketIntervalError("2024-W01-0", "Invalid date");
        assertBracketIntervalError("2024-W01-8", "Invalid date");
    }

    @Test
    public void testIsoWeekInvalidWeek() {
        // Week must be 01-53
        assertBracketIntervalError("2024-W00", "Invalid date");
        assertBracketIntervalError("2024-W54", "Invalid date");
    }

    @Test
    public void testIsoWeekInvalidWeek53For52WeekYear() {
        // 2024 has only 52 weeks, so W53 is invalid
        assertBracketIntervalError("2024-W53", "Invalid date");
    }

    @Test
    public void testIsoWeekMalformedSingleDigitWeek() {
        // Single digit week number should fail (must be 2 digits)
        assertBracketIntervalError("2024-W1", "Invalid date");
    }

    @Test
    public void testIsoWeekMalformedTrailingDash() {
        // Trailing dash with no day should fail
        assertBracketIntervalError("2024-W01-", "Invalid date");
    }

    @Test
    public void testIsoWeekMalformedTrailingT() {
        // Trailing T with no time should fail
        assertBracketIntervalError("2024-W01-1T", "Invalid date");
    }

    @Test
    public void testIsoWeekPartialMicroseconds() throws SqlException {
        // Partial microseconds (1 digit = 100000 micros)
        assertBracketInterval(
                "[{lo=2024-01-01T09:30:15.100000Z, hi=2024-01-01T09:30:15.199999Z}]",
                "2024-W01-1T09:30:15.1"
        );
    }

    @Test
    public void testIsoWeekStartsOnJan1() throws SqlException {
        // 2018: Jan 1 was Monday, so week 1 = Jan 1-7 (no year boundary crossing)
        assertBracketInterval(
                "[{lo=2018-01-01T00:00:00.000000Z, hi=2018-01-07T23:59:59.999999Z}]",
                "2018-W01"
        );
    }

    @Test
    public void testIsoWeekWeek52() throws SqlException {
        // Week 52 of 2024 (Dec 23-29, 2024)
        assertBracketInterval(
                "[{lo=2024-12-23T00:00:00.000000Z, hi=2024-12-29T23:59:59.999999Z}]",
                "2024-W52"
        );
    }

    @Test
    public void testIsoWeekWithDay() throws SqlException {
        // 2024-W01-1 is Monday of week 1 (2024-01-01)
        assertBracketInterval(
                "[{lo=2024-01-01T00:00:00.000000Z, hi=2024-01-01T23:59:59.999999Z}]",
                "2024-W01-1"
        );
        // 2024-W01-5 is Friday of week 1 (2024-01-05)
        assertBracketInterval(
                "[{lo=2024-01-05T00:00:00.000000Z, hi=2024-01-05T23:59:59.999999Z}]",
                "2024-W01-5"
        );
        // 2024-W01-7 is Sunday of week 1 (2024-01-07)
        assertBracketInterval(
                "[{lo=2024-01-07T00:00:00.000000Z, hi=2024-01-07T23:59:59.999999Z}]",
                "2024-W01-7"
        );
    }

    @Test
    public void testIsoWeekWithDurationSuffix() throws SqlException {
        // 2024-W01-1T09:00;1h - Monday 9am + 1h duration
        // Duration is added to the end: 09:00:59.999999 + 1h = 10:00:59.999999
        assertBracketInterval(
                "[{lo=2024-01-01T09:00:00.000000Z, hi=2024-01-01T09:59:59.999999Z}]",
                "2024-W01-1T09:00;1h"
        );
    }

    @Test
    public void testIsoWeekWithSpaceSeparator() throws SqlException {
        // Space separator instead of T should work
        assertBracketInterval(
                "[{lo=2024-01-01T09:00:00.000000Z, hi=2024-01-01T09:00:59.999999Z}]",
                "2024-W01-1 09:00"
        );
    }

    @Test
    public void testIsoWeekWithTime() throws SqlException {
        // 2024-W01-1T09:00 is Monday 9am
        assertBracketInterval(
                "[{lo=2024-01-01T09:00:00.000000Z, hi=2024-01-01T09:00:59.999999Z}]",
                "2024-W01-1T09:00"
        );
        // 2024-W01-1T09:30:15 is Monday 9:30:15
        assertBracketInterval(
                "[{lo=2024-01-01T09:30:15.000000Z, hi=2024-01-01T09:30:15.999999Z}]",
                "2024-W01-1T09:30:15"
        );
    }

    // ==================== Date List Tests ====================

    @Test
    public void testIsoWeekWithTimezone() throws SqlException {
        // 2024-W01-1T09:00@+02:00 - Monday 9am in UTC+2 = 07:00 UTC
        assertBracketInterval(
                "[{lo=2024-01-01T07:00:00.000000Z, hi=2024-01-01T07:00:59.999999Z}]",
                "2024-W01-1T09:00@+02:00"
        );
    }

    @Test
    public void testIsoWeekYear53Weeks() throws SqlException {
        // 2020 has 53 weeks (Dec 31 is a Thursday, and previous year ends on Wednesday)
        // Week 53 of 2020 starts Mon 2020-12-28
        assertBracketInterval(
                "[{lo=2020-12-28T00:00:00.000000Z, hi=2021-01-03T23:59:59.999999Z}]",
                "2020-W53"
        );
    }

    @Test
    public void testIsoWeekYearBoundary() throws SqlException {
        // Week 1 of 2020 starts on Mon 2019-12-30 (ISO week year boundary)
        // The year in ISO week format refers to the ISO week year, not calendar year
        assertBracketInterval(
                "[{lo=2019-12-30T00:00:00.000000Z, hi=2020-01-05T23:59:59.999999Z}]",
                "2020-W01"
        );
    }

    @Test
    public void testMonthDayFilterAfterIncrementalMergeControl() throws SqlException {
        // month leaves expand to their Mondays after the merge, which gives the same days
        assertTickExprAtMonday(
                mergeThreshold4Configuration,
                "[{lo=2024-01-01T00:00:00.000000Z, hi=2024-01-01T23:59:59.999999Z},{lo=2024-01-08T00:00:00.000000Z, hi=2024-01-08T23:59:59.999999Z},"
                        + "{lo=2024-01-15T00:00:00.000000Z, hi=2024-01-15T23:59:59.999999Z},{lo=2024-01-22T00:00:00.000000Z, hi=2024-01-22T23:59:59.999999Z},"
                        + "{lo=2024-01-29T00:00:00.000000Z, hi=2024-01-29T23:59:59.999999Z},{lo=2024-02-05T00:00:00.000000Z, hi=2024-02-05T23:59:59.999999Z},"
                        + "{lo=2024-02-12T00:00:00.000000Z, hi=2024-02-12T23:59:59.999999Z},{lo=2024-02-19T00:00:00.000000Z, hi=2024-02-19T23:59:59.999999Z},"
                        + "{lo=2024-02-26T00:00:00.000000Z, hi=2024-02-26T23:59:59.999999Z},{lo=2024-03-04T00:00:00.000000Z, hi=2024-03-04T23:59:59.999999Z},"
                        + "{lo=2024-03-11T00:00:00.000000Z, hi=2024-03-11T23:59:59.999999Z},{lo=2024-03-18T00:00:00.000000Z, hi=2024-03-18T23:59:59.999999Z},"
                        + "{lo=2024-03-25T00:00:00.000000Z, hi=2024-03-25T23:59:59.999999Z},{lo=2024-04-01T00:00:00.000000Z, hi=2024-04-01T23:59:59.999999Z},"
                        + "{lo=2024-04-08T00:00:00.000000Z, hi=2024-04-08T23:59:59.999999Z},{lo=2024-04-15T00:00:00.000000Z, hi=2024-04-15T23:59:59.999999Z},"
                        + "{lo=2024-04-22T00:00:00.000000Z, hi=2024-04-22T23:59:59.999999Z},{lo=2024-04-29T00:00:00.000000Z, hi=2024-04-29T23:59:59.999999Z}]",
                "2024-[01..04]#mon"
        );
    }

    @Test
    public void testParseLongInterval22() throws Exception {
        assertShortInterval(
                "[{lo=2015-03-12T10:00:00.000000Z, hi=2015-03-12T10:04:59.999999Z},{lo=2015-03-12T10:30:00.000000Z, hi=2015-03-12T10:34:59.999999Z},{lo=2015-03-12T11:00:00.000000Z, hi=2015-03-12T11:04:59.999999Z},{lo=2015-03-12T11:30:00.000000Z, hi=2015-03-12T11:34:59.999999Z},{lo=2015-03-12T12:00:00.000000Z, hi=2015-03-12T12:04:59.999999Z},{lo=2015-03-12T12:30:00.000000Z, hi=2015-03-12T12:34:59.999999Z},{lo=2015-03-12T13:00:00.000000Z, hi=2015-03-12T13:04:59.999999Z},{lo=2015-03-12T13:30:00.000000Z, hi=2015-03-12T13:34:59.999999Z},{lo=2015-03-12T14:00:00.000000Z, hi=2015-03-12T14:04:59.999999Z},{lo=2015-03-12T14:30:00.000000Z, hi=2015-03-12T14:34:59.999999Z}]",
                "2015-03-12T10:00:00;5m;30m;10"
        );
    }

    @Test
    public void testParseLongInterval32() throws Exception {
        assertShortInterval("[{lo=2016-03-21T00:00:00.000000Z, hi=2021-03-20T23:59:59.999999Z}]", "2016-03-21;3y;6M;5");
    }

    @Test
    public void testParseLongIntervalPositiveYearPeriod() throws Exception {
        // Test positive year period (exercises period >= 0 branch in addYearIntervals, line 675)
        // 2015-03-12T11:00:00;5m;1y;3 means: start at 2015, duration 5m, repeat every 1 year, 3 times
        assertShortInterval(
                "[{lo=2015-03-12T11:00:00.000000Z, hi=2015-03-12T11:04:59.999999Z},{lo=2016-03-12T11:00:00.000000Z, hi=2016-03-12T11:04:59.999999Z},{lo=2017-03-12T11:00:00.000000Z, hi=2017-03-12T11:04:59.999999Z}]",
                "2015-03-12T11:00:00;5m;1y;3"
        );
    }

    @Test
    public void testParseLongMinusInterval() throws Exception {
        assertShortInterval(
                "[{lo=2015-03-12T10:00:00.000000Z, hi=2015-03-12T10:04:59.999999Z},{lo=2015-03-12T10:30:00.000000Z, hi=2015-03-12T10:34:59.999999Z},{lo=2015-03-12T11:00:00.000000Z, hi=2015-03-12T11:04:59.999999Z}]",
                "2015-03-12T11:00:00;5m;-30m;3"
        );
        assertShortInterval(
                "[{lo=2014-11-12T11:00:00.000000Z, hi=2014-11-12T11:04:59.999999Z},{lo=2015-01-12T11:00:00.000000Z, hi=2015-01-12T11:04:59.999999Z},{lo=2015-03-12T11:00:00.000000Z, hi=2015-03-12T11:04:59.999999Z}]",
                "2015-03-12T11:00:00;5m;-2M;3"
        );
        assertShortInterval(
                "[{lo=2013-03-12T11:00:00.000000Z, hi=2013-03-12T11:04:59.999999Z},{lo=2014-03-12T11:00:00.000000Z, hi=2014-03-12T11:04:59.999999Z},{lo=2015-03-12T11:00:00.000000Z, hi=2015-03-12T11:04:59.999999Z}]",
                "2015-03-12T11:00:00;5m;-1y;3"
        );
    }

    @Test
    public void testParseNegativeDurationCalendarMonth() throws Exception {
        // -1M from Jan 27 15:00 → [Dec 27 15:00, Jan 27 14:59:59.999999]
        assertShortInterval(
                "[{lo=2025-12-27T15:00:00.000000Z, hi=2026-01-27T14:59:59.999999Z}]",
                "2026-01-27T15:00;-1M"
        );
    }

    @Test
    public void testParseNegativeDurationCalendarMonthDayClamping() throws Exception {
        // -1M from Mar 31 → Feb 28 (2026 is not leap), interval [Feb 28, Mar 30]
        // Mar 31 itself is excluded (hi = anchor - 1)
        assertShortInterval(
                "[{lo=2026-02-28T00:00:00.000000Z, hi=2026-03-30T23:59:59.999999Z}]",
                "2026-03-31;-1M"
        );
    }

    @Test
    public void testParseNegativeDurationCalendarYear() throws Exception {
        // -1y from Feb 29 (leap 2024) → Feb 28 2023 (clamped), interval [Feb 28 2023, Feb 28 2024]
        assertShortInterval(
                "[{lo=2023-02-28T00:00:00.000000Z, hi=2024-02-28T23:59:59.999999Z}]",
                "2024-02-29;-1y"
        );
    }

    @Test
    public void testParseNegativeDurationRepeatingCalendarMonth() throws Exception {
        // -1s window from 12:00, repeated 3x with -1M period
        // Each interval computed from base (Mar 31): -2M → Jan 31, -1M → Feb 28, 0 → Mar 31
        assertShortInterval(
                "[{lo=2026-01-31T11:59:59.000000Z, hi=2026-01-31T11:59:59.999999Z}," +
                        "{lo=2026-02-28T11:59:59.000000Z, hi=2026-02-28T11:59:59.999999Z}," +
                        "{lo=2026-03-31T11:59:59.000000Z, hi=2026-03-31T11:59:59.999999Z}]",
                "2026-03-31T12:00;-1s;-1M;3"
        );
    }

    @Test
    public void testParseNegativeDurationRepeating() throws Exception {
        // Negative duration with negative period: -1s window repeated 2x going back 1h
        assertShortInterval(
                "[{lo=2026-01-27T13:59:59.000000Z, hi=2026-01-27T13:59:59.999999Z}," +
                        "{lo=2026-01-27T14:59:59.000000Z, hi=2026-01-27T14:59:59.999999Z}]",
                "2026-01-27T15:00;-1s;-1h;2"
        );
    }

    @Test
    public void testParseNegativeDuration() throws Exception {
        // Plain timestamp with negative duration — exercises parseRange static path directly
        assertShortInterval(
                "[{lo=2025-01-15T08:00:00.000000Z, hi=2025-01-15T09:59:59.999999Z}]",
                "2025-01-15T10:00:00;-2h"
        );
    }

    @Test
    public void testParseNegativeDurationMixedSigns() throws Exception {
        // -3d2h: go back 3 days then forward 2 hours
        assertShortInterval(
                "[{lo=2025-01-12T02:00:00.000000Z, hi=2025-01-14T23:59:59.999999Z}]",
                "2025-01-15;-3d2h"
        );
    }

    @Test
    public void testParseNegativeDurationMultiNegative() throws Exception {
        // -3d-2h: go back 3 days and 2 hours
        assertShortInterval(
                "[{lo=2025-01-11T22:00:00.000000Z, hi=2025-01-14T23:59:59.999999Z}]",
                "2025-01-15;-3d-2h"
        );
    }

    @Test
    public void testParsePositiveDurationExplicitSign() throws Exception {
        // +3d should behave identically to 3d
        assertShortInterval(
                "[{lo=2025-01-15T00:00:00.000000Z, hi=2025-01-17T23:59:59.999999Z}]",
                "2025-01-15;+3d"
        );
    }

    @Test
    public void testParseTickExprChecksLeaveOtherExpressionsAlone() {
        // The checks change nothing for an expression without a range that starts after it ends,
        // and within the interval limit, whichever path of the parser it takes
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final long now = timestampDriver.parseFloorLiteral("2024-01-02T03:00:00.000000Z");
        for (String expression : new String[]{
                // a year, a month that does not exist, and a month before a time list, also out of
                // a bracket expansion or a date list
                "2024T[09:00,10:00]",
                "2024-13T[09:00]",
                "2024-01T[09:00,10:00]",
                "202[3,4]T[09:00,10:00]",
                "2024-1[2,3]T[09:00]",
                "[2024]T[09:00,10:00]",
                "[2024-13]T[09:00]",
                // neither a month nor a day before a time list
                "garbageT[09:00,10:00]",
                // date variables, a time zone, a duration, a day filter and a bad duration unit
                "[2024-01, $today]T[09:00,10:00]",
                "2024-01-0[1..3]T09:00",
                "[$today, $tomorrow]T[09:00@Europe/London]",
                "$now - 2h..$now;1h",
                "[$today..$tomorrow]T09:00#Tue",
                "[2024-01-05, $today]T09:30",
                "$today;1x"
        }) {
            final String withAllChecks = parseWithChecks(timestampDriver, expression, now, IntervalUtils.TICK_CHECK_ALL);
            for (int checks : new int[]{0, IntervalUtils.TICK_CHECK_RANGE_ORDER, IntervalUtils.TICK_CHECK_INTERVAL_LIMIT}) {
                Assert.assertEquals(expression, withAllChecks, parseWithChecks(timestampDriver, expression, now, checks));
            }
        }
    }

    @Test
    public void testParseTickExprWithoutIntervalLimitCheck() throws SqlException {
        // Without the interval limit check, nothing bounds the number of intervals. WAL apply
        // evaluates a literal this way, as the limit held when its SQL compiled.
        final String now = "2024-01-02T03:00:00.000000Z";
        // a date variable range: 2,001 days
        assertParsedIntervalCount(2001, "[$today..$today + 2000d]T09:00", now, IntervalUtils.TICK_CHECK_RANGE_ORDER);
        assertParsedWithChecksFails("[$today..$today + 2000d]T09:00", now, IntervalUtils.TICK_CHECK_ALL, "Date range expansion produces too many intervals (max 1024)");
        // a bracket expansion, which merges as it goes and at the end: 31 days of 24 hours of 2 minutes
        assertParsedIntervalCount(1488, "2024-01-[01..31]T[00..23]:[00,30]", now, 0);
        assertParsedWithChecksFails("2024-01-[01..31]T[00..23]:[00,30]", now, IntervalUtils.TICK_CHECK_INTERVAL_LIMIT, "Bracket expansion produces too many intervals (max 1024)");
        // a date list with a time list: 601 days and one more of 2 slots
        assertParsedIntervalCount(1204, "[$today..$today + 600d, 2024-01-01]T[09:00,10:00]", now, 0);
        assertParsedWithChecksFails("[$today..$today + 600d, 2024-01-01]T[09:00,10:00]", now, IntervalUtils.TICK_CHECK_INTERVAL_LIMIT, "produces too many intervals (max 1024)");
        // a time list of 1,100 separate seconds, which reaches the limit only in the final result
        final StringBuilder timeList = new StringBuilder("2024-01-01T[");
        for (int i = 0; i < 1100; i++) {
            final int second = 2 * i;
            if (i > 0) {
                timeList.append(',');
            }
            timeList.append(String.format("%02d:%02d:%02d", second / 3600, second / 60 % 60, second % 60));
        }
        final String seconds = timeList.append(']').toString();
        assertParsedIntervalCount(1100, seconds, now, IntervalUtils.TICK_CHECK_RANGE_ORDER);
        assertParsedWithChecksFails(seconds, now, IntervalUtils.TICK_CHECK_ALL, "Bracket expansion produces too many intervals (max 1024)");
    }

    @Test
    public void testParseTickExprWithoutRangeOrderCheck() throws SqlException {
        // Without the range order check, a date variable range that starts after it ends adds no
        // interval, and the other elements of the expression keep theirs. At 03:00 a range from
        // 09:30 to now starts after it ends.
        final String now = "2024-01-02T03:00:00.000000Z";
        final int checks = IntervalUtils.TICK_CHECK_INTERVAL_LIMIT;
        assertParsedWithChecks("[]", "[$tomorrow..$today]", now, checks);
        assertParsedWithChecks("[]", "$tomorrow..$today", now, checks);
        assertParsedWithChecks("[]", "$today + 570m..$now", now, checks);
        assertParsedWithChecks("[]", "[$now..$now - 1h]", now, checks);
        assertParsedWithChecks("[{lo=2024-01-01T00:00:00.000000Z, hi=2024-01-01T23:59:59.999999Z}]", "[$tomorrow..$today, $yesterday]", now, checks);
        assertParsedWithChecks("[{lo=2024-01-03T09:00:00.000000Z, hi=2024-01-03T09:00:59.999999Z}]", "[$today + 570m..$now, $tomorrow]T09:00", now, checks);
        assertParsedWithChecks(
                "[{lo=2024-01-03T09:00:00.000000Z, hi=2024-01-03T09:00:59.999999Z},{lo=2024-01-03T10:00:00.000000Z, hi=2024-01-03T10:00:59.999999Z}]",
                "[$tomorrow..$today, 2024-01-03]T[09:00,10:00]",
                now,
                checks
        );
        // a range that does not start after it ends is unchanged
        assertParsedWithChecks("[{lo=2024-01-02T00:00:00.000000Z, hi=2024-01-03T23:59:59.999999Z}]", "[$today..$tomorrow]", now, checks);
        assertParsedWithChecks("[{lo=2024-01-02T02:00:00.000000Z, hi=2024-01-02T03:00:00.000000Z}]", "$now - 1h..$now", now, checks);
        // the check fails on such a range
        assertParsedWithChecksFails("[$tomorrow..$today, $yesterday]", now, IntervalUtils.TICK_CHECK_RANGE_ORDER, "Invalid date range: start is after end");
        assertParsedWithChecksFails("$today + 570m..$now", now, IntervalUtils.TICK_CHECK_ALL, "Invalid date range: start is after end");
        // and an expression that does not parse fails without any check
        assertParsedWithChecksFails("[$tomorrow..$today, garbage]", now, 0, "Invalid date");
    }

    @Test
    public void testParseZeroDuration() throws Exception {
        // 0d produces an inverted interval (hi < lo) — effectively empty
        assertShortInterval(
                "[{lo=2025-01-15T10:00:00.000000Z, hi=2025-01-15T09:59:59.999999Z}]",
                "2025-01-15T10:00:00;0d"
        );
    }

    @Test
    public void testParseShortDayErr() {
        assertIntervalError("2016-02-30");
    }

    @Test
    public void testParseShortDayErr2() {
        assertIntervalError("2016-02-3");
    }

    @Test
    public void testParseShortHourErr1() {
        assertIntervalError("2016-02-15T1");
    }

    @Test
    public void testParseShortHourErr2() {
        assertIntervalError("2016-02-15T31");
    }

    @Test
    public void testParseShortHourErr3() {
        assertIntervalError("2016-02-15X1");
    }

    @Test
    public void testParseShortInterval1() throws Exception {
        assertShortInterval("[{lo=2016-01-01T00:00:00.000000Z, hi=2016-12-31T23:59:59.999999Z}]", "2016");
    }

    @Test
    public void testParseShortInterval10() throws Exception {
        Assume.assumeTrue(timestampType == TestTimestampType.NANO);
        assertShortInterval("[{lo=2016-03-21T10:30:40.123456780Z, hi=2016-03-21T10:30:40.123456789Z}]", "2016-03-21T10:30:40.12345678");
    }

    @Test
    public void testParseShortInterval2() throws Exception {
        assertShortInterval("[{lo=2016-03-01T00:00:00.000000Z, hi=2016-03-31T23:59:59.999999Z}]", "2016-03");
    }

    @Test
    public void testParseShortInterval3() throws Exception {
        assertShortInterval("[{lo=2016-03-21T00:00:00.000000Z, hi=2016-03-21T23:59:59.999999Z}]", "2016-03-21");
    }

    @Test
    public void testParseShortInterval32() throws Exception {
        assertShortInterval("[{lo=2016-03-21T00:00:00.000000Z, hi=2016-03-21T23:59:59.999999Z}]", "2016-03-21");
    }

    @Test
    public void testParseShortInterval4() throws Exception {
        assertShortInterval("[{lo=2016-03-21T10:00:00.000000Z, hi=2016-03-21T10:59:59.999999Z}]", "2016-03-21T10");
    }

    @Test
    public void testParseShortInterval5() throws Exception {
        assertShortInterval("[{lo=2016-03-21T10:30:00.000000Z, hi=2016-03-21T10:30:59.999999Z}]", "2016-03-21T10:30");
    }

    @Test
    public void testParseShortInterval6() throws Exception {
        assertShortInterval("[{lo=2016-03-21T10:30:40.000000Z, hi=2016-03-21T10:30:40.999999Z}]", "2016-03-21T10:30:40");
    }

    @Test
    public void testParseShortInterval7() throws Exception {
        assertShortInterval("[{lo=2016-03-21T10:30:40.100000Z, hi=2016-03-21T10:30:40.100000Z}]", "2016-03-21T10:30:40.100Z");
    }

    @Test
    public void testParseShortInterval8() throws Exception {
        assertShortInterval("[{lo=2016-03-21T10:30:40.100000Z, hi=2016-03-21T10:30:40.100999Z}]", "2016-03-21T10:30:40.100");
        assertShortInterval("[{lo=2016-03-21T10:30:40.280000Z, hi=2016-03-21T10:30:40.289999Z}]", "2016-03-21T10:30:40.28");
    }

    @Test
    public void testParseShortInterval9() throws Exception {
        Assume.assumeTrue(timestampType == TestTimestampType.MICRO);
        assertShortInterval("[{lo=2016-03-21T10:30:40.123456Z, hi=2016-03-21T10:30:40.123456Z}]", "2016-03-21T10:30:40.12345678");
    }

    @Test
    public void testParseShortMilliErr() {
        assertIntervalError("2016-03-21T10:31:61.23");
    }

    @Test
    public void testParseShortMinErr() {
        assertIntervalError("2016-03-21T10:3");
    }

    // =====================================================
    // Timezone tests
    // =====================================================

    @Test
    public void testParseShortMinErr2() {
        assertIntervalError("2016-03-21T10:69");
    }

    @Test
    public void testParseShortMonthErr() {
        assertIntervalError("2016-1");
    }

    @Test
    public void testParseShortMonthErr2() {
        assertIntervalError("2016x11");
    }

    @Test
    public void testParseShortMonthRange() {
        assertIntervalError("2016-66");
    }

    @Test
    public void testParseShortSecErr() {
        assertIntervalError("2016-03-21T10:31:61");
    }

    @Test
    public void testParseShortSecErr1() {
        assertIntervalError("2016-03-21T10:31:1");
    }

    @Test
    public void testParseShortYearErr() {
        assertIntervalError("201-");
    }

    @Test
    public void testParseShortYearErr1() {
        assertIntervalError("20-");
    }

    @Test
    public void testPointInTimeRangeElementDayFilter() throws SqlException {
        // A range between two points in time keeps its interval where the day filter of its element
        // holds the day that the interval starts on, by java.time, and drops it otherwise, at every
        // check level, as the day filter of the list does. The range without a day filter gives
        // the interval. The compiled form parses an element with a day filter of its own on every
        // evaluation, and keeps the IR of the list form.
        // https://github.com/questdb/questdb/issues/8225
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final String[] ranges = {
                "$now - 2h..$now", "$today + 570m..$today + 16h", "$today + 9h..$today + 24h", "$now - 3d..$now",
                "$NOW - 1h..$Now + 1h", "$yesterday + 30m..$now", "$now..$now"
        };
        final String[] nows = {
                "2025-01-19T12:00:00.000000Z", // a Sunday
                "2025-01-20T12:00:00.000000Z", // a Monday
                "2025-01-20T01:00:00.000000Z", // a Monday, two hours after a Sunday
                "1969-12-29T01:00:00.000000Z", // a Monday before 1970, two hours after a Sunday
                "2099-12-31T23:30:00.000000Z", // a Thursday, an hour before 2100
                "2024-02-29T23:59:00.000000Z" // the last minute of a leap day
        };
        for (String nowText : nows) {
            final long now = timestampDriver.parseFloorLiteral(nowText);
            for (String range : ranges) {
                final String unfiltered = "[" + range + "]";
                out.clear();
                IntervalUtils.parseTickExpr(timestampDriver, configuration, unfiltered, 0, unfiltered.length(), 7, out, sink, now);
                Assert.assertEquals(unfiltered + " at " + nowText, 2, out.size());
                final DayOfWeek startDay = LocalDate.ofEpochDay(Math.floorDiv(out.getQuick(0), timestampDriver.fromDays(1))).getDayOfWeek();
                final String interval = intervalToString(timestampDriver, out).toString();
                for (DayFilter dayFilter : DAY_FILTERS) {
                    final String expected = dayFilter.days().contains(startDay) ? interval : "[]";
                    final String element = "[" + range + "#" + dayFilter.text() + "]";
                    final String list = unfiltered + "#" + dayFilter.text();
                    assertParsedAtEveryCheckLevel(expected, element, now);
                    assertParsed(expected, list, now, IntervalUtils.TICK_CHECK_ALL);
                    assertCompiledEvaluatesTo(expected, element, now, false);
                    assertCompiledEvaluatesTo(expected, list, now, true);
                }
            }
        }
        // by hand
        final long sunday = timestampDriver.parseFloorLiteral("2025-01-19T12:00:00.000000Z");
        final long monday = timestampDriver.parseFloorLiteral("2025-01-20T12:00:00.000000Z");
        final long mondayOne = timestampDriver.parseFloorLiteral("2025-01-20T01:00:00.000000Z");
        assertParsedAtEveryCheckLevel(forTimestampType("[{lo=2025-01-19T10:00:00.000000Z, hi=2025-01-19T12:00:00.000000Z}]"), "[$now - 2h..$now#Sun]", sunday);
        assertParsedAtEveryCheckLevel("[]", "[$now - 2h..$now#Mon]", sunday);
        assertParsedAtEveryCheckLevel(forTimestampType("[{lo=2025-01-19T23:00:00.000000Z, hi=2025-01-20T01:00:00.000000Z}]"), "[$now - 2h..$now#Sun]", mondayOne);
        assertParsedAtEveryCheckLevel("[]", "[$now - 2h..$now#Mon]", mondayOne);
        assertParsedAtEveryCheckLevel("[]", "[$today + 570m..$today + 16h#workday]", sunday);
        assertParsedAtEveryCheckLevel(
                forTimestampType("[{lo=2025-01-20T09:30:00.000000Z, hi=2025-01-20T16:00:00.000000Z}]"),
                "[$today + 570m..$today + 16h#workday]",
                monday
        );
    }

    @Test
    public void testPointInTimeRangeElementDayFilterIgnoresListTimeAndDuration() throws SqlException {
        // A range between two points in time with a day filter of its element ignores the time part
        // and the duration of the list, as one without a day filter does. 2025-01-19 is a Sunday.
        final long sunday = timestampType.getDriver().parseFloorLiteral("2025-01-19T12:00:00.000000Z");
        final String tenToNoon = forTimestampType("[{lo=2025-01-19T10:00:00.000000Z, hi=2025-01-19T12:00:00.000000Z}]");
        for (String listSuffix : new String[]{"T09:00;1h", "T[09:00,10:00];30m"}) {
            assertParsedAtEveryCheckLevel(tenToNoon, "[$now - 2h..$now#Sun]" + listSuffix, sunday);
            assertParsedAtEveryCheckLevel("[]", "[$now - 2h..$now#Mon]" + listSuffix, sunday);
            assertCompiledEvaluatesTo(tenToNoon, "[$now - 2h..$now#Sun]" + listSuffix, sunday, false);
            assertCompiledEvaluatesTo("[]", "[$now - 2h..$now#Mon]" + listSuffix, sunday, false);
        }
    }

    @Test
    public void testPointInTimeRangeElementDayFilterReplacesListDayFilter() throws SqlException {
        // The day filter of an element replaces the day filter of the list for that element, as for
        // a range of days, see testDateVariableRangeWithElementDayFilter, and the other elements keep
        // the one of the list. 2025-01-19 is a Sunday.
        final long sunday = timestampType.getDriver().parseFloorLiteral("2025-01-19T12:00:00.000000Z");
        final String[][] cases = {
                {"[$now - 2h..$now#Sun]#Mon", "[{lo=2025-01-19T10:00:00.000000Z, hi=2025-01-19T12:00:00.000000Z}]"},
                {"[$now - 2h..$now#Mon]#Sun", "[]"},
                {"[$now - 2h..$now#Mon, $now - 26h..$now - 24h]#Sat", "[{lo=2025-01-18T10:00:00.000000Z, hi=2025-01-18T12:00:00.000000Z}]"}
        };
        for (String[] c : cases) {
            final String expected = forTimestampType(c[1]);
            assertParsedAtEveryCheckLevel(expected, c[0], sunday);
            assertCompiledEvaluatesTo(expected, c[0], sunday, false);
        }
    }

    @Test
    public void testPointInTimeRangeElementSuffixErrorOrder() throws SqlException {
        // A range between two points in time fails on the time part and on the text after the
        // first ';' of the list before the day filter of its element, under the range order check
        // on its order first, at its '..', and on the day filter of its element before its
        // timezone, as a range of days does. A timezone after the day filter is part of the day
        // filter, as for a single element.
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final long sunday = timestampDriver.parseFloorLiteral("2025-01-19T12:00:00.000000Z");
        final String[][] cases = {
                {"[$now - 2h..$now#Xyz]T25:00", "Invalid date: 2025-01-19T25:00"},
                {"[$now - 2h..$now#Xyz];1x", "Invalid duration unit: x"},
                {"[$now - 2h..$now@Bogus/Zone#Xyz]", "Invalid day name: Xyz"},
                {"[$today..$tomorrow@Bogus/Zone#Xyz]", "Invalid day name: Xyz"},
                {"[$now - 2h..$now#Mon@+05:00]", "Invalid day name: Mon@+05:00"},
                {"[$now#Mon@+05:00]", "Invalid day name: Mon@+05:00"}
        };
        for (String[] c : cases) {
            assertParseFailsAtEveryCheckLevel(c[0], sunday, c[1]);
            assertFailsLikeBindVariable(c[0], sunday, c[1]);
        }
        assertParseFails("[$now..$now - 1h#Xyz]", sunday, IntervalUtils.TICK_CHECK_ALL, 11, RANGE_ORDER_ERROR);
    }

    @Test
    public void testPointInTimeRangeElementSuffixFailsWhenRangeAddsNoInterval() throws SqlException {
        // A range between two points in time that starts after it ends adds no interval below the
        // range order check, and fails on a day filter of its element that does not parse all the
        // same, as it does where it adds an interval; one that parses lets it add none. The compiled
        // form fails as the SQL that holds it is validated. 2025-01-19 is a Sunday.
        final NoIntervalRange[] ranges = {
                new NoIntervalRange("$now..$now - 1h", "2025-01-19T12:00:00.000000Z", null, true, "$now - 1h..$now"),
                new NoIntervalRange("$today + 570m..$now", "2025-01-19T03:00:00.000000Z", null, true, "$now - 1h..$now")
        };
        for (NoIntervalRange range : ranges) {
            assertNoIntervalRangeFails(range, "[" + range.range() + "#Xyz]", "Invalid day name: Xyz");
            assertNoIntervalRangeAddsNoInterval(range, "[" + range.range() + "#Mon]");
            assertNoIntervalRangeAddsNoInterval(range, "[" + range.range() + "#Sun]");
        }
    }

    @Test
    public void testRepeatingIntervalCountOne() throws SqlException {
        // Count=1 with period - the period is parsed but only one interval is generated
        // This tests the `if (count > 1)` branch being false
        assertBracketInterval(
                "[{lo=2018-01-10T10:30:00.000000Z, hi=2018-01-10T10:59:59.999999Z}]",
                "2018-01-10T10:30;30m;2h;1"
        );
    }

    @Test
    public void testRepeatingIntervalWithHourPeriod() throws SqlException {
        // 2018-01-10T10:30;30m;2h;3 means 30min window starting at 10:30, then 12:30, then 14:30
        assertBracketInterval(
                "[{lo=2018-01-10T10:30:00.000000Z, hi=2018-01-10T10:59:59.999999Z},{lo=2018-01-10T12:30:00.000000Z, hi=2018-01-10T12:59:59.999999Z},{lo=2018-01-10T14:30:00.000000Z, hi=2018-01-10T14:59:59.999999Z}]",
                "2018-01-10T10:30;30m;2h;3"
        );
    }

    @Test
    public void testRepeatingIntervalWithSecondPeriod() throws SqlException {
        // 2018-01-10T10:30:00;5s;10s;3 means 5sec window starting at 10:30:00, then 10:30:10, then 10:30:20
        assertBracketInterval(
                "[{lo=2018-01-10T10:30:00.000000Z, hi=2018-01-10T10:30:04.999999Z},{lo=2018-01-10T10:30:10.000000Z, hi=2018-01-10T10:30:14.999999Z},{lo=2018-01-10T10:30:20.000000Z, hi=2018-01-10T10:30:24.999999Z}]",
                "2018-01-10T10:30:00;5s;10s;3"
        );
    }

    @Test
    public void testSteadyStateNestedExpansionAllocatesNoJavaHeap() throws Exception {
        try (TestUtils.ThreadMetricsScope<com.sun.management.ThreadMXBean> scope = TestUtils.threadAllocationScope()) {
            final com.sun.management.ThreadMXBean threadMXBean = scope.getBean();
            final TimestampDriver timestampDriver = timestampType.getDriver();
            final String interval = "[2024-01-15,2024-01-16]T[09:00,14:30]:[00,30]";
            final StringSink scratch = new StringSink();
            for (int i = 0; i < 20_000; i++) {
                out.clear();
                IntervalUtils.parseTickExpr(timestampDriver, configuration, interval, 0, interval.length(),
                        0, out, scratch);
            }
            long minAllocatedBytes = Long.MAX_VALUE;
            long intervalCount = 0;
            for (int round = 0; round < 5; round++) {
                final long allocatedBefore = threadMXBean.getCurrentThreadAllocatedBytes();
                for (int i = 0; i < 10_000; i++) {
                    out.clear();
                    IntervalUtils.parseTickExpr(timestampDriver, configuration, interval, 0, interval.length(),
                            0, out, scratch);
                    intervalCount += out.size() / 2;
                }
                minAllocatedBytes = Math.min(minAllocatedBytes,
                        threadMXBean.getCurrentThreadAllocatedBytes() - allocatedBefore);
            }
            Assert.assertEquals(400_000, intervalCount);
            Assert.assertEquals(0, minAllocatedBytes);
        }
    }

    @Test
    public void testTimeListBracketAdjacentIntervals() throws SqlException {
        // Test adjacent intervals: 09:00 (covers 09:00:00.000000-09:00:59.999999)
        // and 09:01 (covers 09:01:00.000000-09:01:59.999999) are exactly adjacent.
        // These should be merged into a single interval.
        assertBracketInterval(
                "[{lo=2024-01-15T09:00:00.000000Z, hi=2024-01-15T09:01:59.999999Z}]",
                "2024-01-15T[09:00,09:01]"
        );
    }

    @Test
    public void testTimeListBracketEmptyBracketIsNumericExpansion() {
        // Empty bracket T[] has no ':' inside, so it's treated as numeric expansion
        // This documents current behavior: error message is "Empty bracket expansion" not "Empty time list bracket"
        assertBracketIntervalError("2024-01-15T[];1h", "Empty bracket expansion");
    }

    @Test
    public void testTimeListBracketErrorEmptyElement() {
        // Empty element in time list
        assertBracketIntervalError("2024-01-15T[09:00,,14:30];6h", "Empty element in time list");
    }

    @Test
    public void testTimeListBracketMilitaryTimeFormatNotSupported() {
        // Military time without colons [0900,1430] is NOT supported
        // Provides actionable error message guiding users to use colons
        assertBracketIntervalError("2024-01-15T[0900,1430]", "Military time format not supported");
    }

    @Test
    public void testTimeListBracketMixedFormats() throws SqlException {
        // Mixed time formats work: "09:00" (minute precision) and "14" (hour only)
        // The parser accepts partial times, so "14" becomes "14:00:00" to "14:59:59"
        assertBracketInterval(
                "[{lo=2024-01-15T09:00:00.000000Z, hi=2024-01-15T09:00:59.999999Z},{lo=2024-01-15T14:00:00.000000Z, hi=2024-01-15T14:59:59.999999Z}]",
                "2024-01-15T[09:00,14]"
        );
    }

    @Test
    public void testTimeListBracketMixedFormats2() throws SqlException {
        // Mixed time formats: "09" (hour only) and "14:29" (hour:minute)
        // "09" becomes hour interval 09:00:00 to 09:59:59
        // "14:29" becomes minute interval 14:29:00 to 14:29:59
        assertBracketInterval(
                "[{lo=2024-01-15T09:00:00.000000Z, hi=2024-01-15T09:59:59.999999Z},{lo=2024-01-15T14:29:00.000000Z, hi=2024-01-15T14:29:59.999999Z}]",
                "2024-01-15T[09,14:29]"
        );
    }

    @Test
    public void testTimeListBracketMixedTimezone() throws SqlException {
        // Time list with mixed: some per-element, some using global
        // T[09:00@UTC,14:30]@+02:00;1h
        // 09:00@UTC = 09:00 UTC, 14:30@+02:00 = 12:30 UTC
        assertBracketInterval(
                "[{lo=2024-01-15T09:00:00.000000Z, hi=2024-01-15T09:59:59.999999Z},{lo=2024-01-15T12:30:00.000000Z, hi=2024-01-15T13:29:59.999999Z}]",
                "2024-01-15T[09:00@UTC,14:30]@+02:00;1h"
        );
    }

    @Test
    public void testTimeListBracketNestedBracketsInElementFails() {
        // Nested brackets inside time list elements are not supported
        // Provides actionable error message guiding users to use separate expansions
        assertBracketIntervalError("2024-01-15T[09:[00,30],14:30]", "Nested brackets not supported in time list");
    }

    @Test
    public void testTimeListBracketOverlappingMerged() throws SqlException {
        // When time list intervals overlap, they get merged (correct behavior)
        // T[09:00,10:30];2h creates intervals 09:00-11:00 and 10:30-12:30 which overlap
        // These get merged into a single interval 09:00-12:30
        assertBracketInterval(
                "[{lo=2024-01-15T09:00:00.000000Z, hi=2024-01-15T12:29:59.999999Z}]",
                "2024-01-15T[09:00,10:30];2h"
        );
    }

    @Test
    public void testTimeListBracketPerElementTimezone() throws SqlException {
        // Time list with per-element timezones
        // T[09:00@+05:00,08:00@+02:00];1h
        // 09:00 in +05:00 = 04:00 UTC, 08:00 in +02:00 = 06:00 UTC
        assertBracketInterval(
                "[{lo=2024-01-15T04:00:00.000000Z, hi=2024-01-15T04:59:59.999999Z},{lo=2024-01-15T06:00:00.000000Z, hi=2024-01-15T06:59:59.999999Z}]",
                "2024-01-15T[09:00@+05:00,08:00@+02:00];1h"
        );
    }

    @Test
    public void testTimeListBracketSimple() throws SqlException {
        // Simple time list: T[09:00,18:00];1h (non-overlapping intervals)
        // Creates intervals at 09:00-10:00 and 18:00-19:00
        assertBracketInterval(
                "[{lo=2024-01-15T09:00:00.000000Z, hi=2024-01-15T09:59:59.999999Z},{lo=2024-01-15T18:00:00.000000Z, hi=2024-01-15T18:59:59.999999Z}]",
                "2024-01-15T[09:00,18:00];1h"
        );
    }

    @Test
    public void testTimeListBracketSingleTime() throws SqlException {
        // Single time in time list bracket (edge case)
        assertBracketInterval(
                "[{lo=2024-01-15T09:00:00.000000Z, hi=2024-01-15T09:59:59.999999Z}]",
                "2024-01-15T[09:00];1h"
        );
    }

    @Test
    public void testTimeListBracketThreeTimes() throws SqlException {
        // Three time values (non-overlapping)
        assertBracketInterval(
                "[{lo=2024-01-15T08:00:00.000000Z, hi=2024-01-15T08:59:59.999999Z},{lo=2024-01-15T12:00:00.000000Z, hi=2024-01-15T12:59:59.999999Z},{lo=2024-01-15T18:00:00.000000Z, hi=2024-01-15T18:59:59.999999Z}]",
                "2024-01-15T[08:00,12:00,18:00];1h"
        );
    }

    @Test
    public void testTimeListBracketWithDateExpansionAndTimezone() throws SqlException {
        // Date expansion + time list + global timezone
        // 2024-01-[15,16]T[09:00,18:00]@+02:00;1h = 4 intervals
        // Times in +02:00: 09:00 = 07:00 UTC, 18:00 = 16:00 UTC
        assertBracketInterval(
                "[{lo=2024-01-15T07:00:00.000000Z, hi=2024-01-15T07:59:59.999999Z},{lo=2024-01-15T16:00:00.000000Z, hi=2024-01-15T16:59:59.999999Z},{lo=2024-01-16T07:00:00.000000Z, hi=2024-01-16T07:59:59.999999Z},{lo=2024-01-16T16:00:00.000000Z, hi=2024-01-16T16:59:59.999999Z}]",
                "2024-01-[15,16]T[09:00,18:00]@+02:00;1h"
        );
    }

    @Test
    public void testTimeListBracketWithDayExpansion() throws SqlException {
        // Combination: day expansion + time list
        // 2024-01-[15,16]T[09:00,18:00];1h = 4 intervals (2 days × 2 times, non-overlapping)
        assertBracketInterval(
                "[{lo=2024-01-15T09:00:00.000000Z, hi=2024-01-15T09:59:59.999999Z},{lo=2024-01-15T18:00:00.000000Z, hi=2024-01-15T18:59:59.999999Z},{lo=2024-01-16T09:00:00.000000Z, hi=2024-01-16T09:59:59.999999Z},{lo=2024-01-16T18:00:00.000000Z, hi=2024-01-16T18:59:59.999999Z}]",
                "2024-01-[15,16]T[09:00,18:00];1h"
        );
    }

    @Test
    public void testTimeListBracketWithDenseSuffixAndTimezone() throws SqlException {
        // Each element expands its :[00..59] suffix to 60 seconds, so the fifth element reaches
        // the incremental merge threshold (256 intervals). Its timezone must still convert
        // exactly its own intervals. 09:00@+02:00 and 08:00@+01:00 are both 07:00Z,
        // 11:00@-01:00 is 12:00Z and 12:00@+03:00 is 09:00Z.
        assertBracketInterval(
                "[{lo=2024-01-01T07:00:00.000000Z, hi=2024-01-01T07:00:59.999999Z},{lo=2024-01-01T09:00:00.000000Z, hi=2024-01-01T09:00:59.999999Z},{lo=2024-01-01T10:00:00.000000Z, hi=2024-01-01T10:00:59.999999Z},{lo=2024-01-01T12:00:00.000000Z, hi=2024-01-01T12:00:59.999999Z}]",
                "2024-01-01T[10:00,09:00@+02:00,08:00@+01:00,11:00@-01:00,12:00@+03:00]:[00..59]"
        );
        // a global timezone converts each element separately
        assertBracketInterval(
                "[{lo=2024-01-01T06:00:00.000000Z, hi=2024-01-01T06:00:59.999999Z},{lo=2024-01-01T07:00:00.000000Z, hi=2024-01-01T07:00:59.999999Z},{lo=2024-01-01T08:00:00.000000Z, hi=2024-01-01T08:00:59.999999Z},{lo=2024-01-01T09:00:00.000000Z, hi=2024-01-01T09:00:59.999999Z},{lo=2024-01-01T10:00:00.000000Z, hi=2024-01-01T10:00:59.999999Z}]",
                "2024-01-01T[10:00,09:00,08:00,11:00,12:00]:[00..59]@+02:00"
        );
    }

    @Test
    public void testTimeListBracketWithDenseSuffixBoundsMemory() throws SqlException {
        // 480 elements, each expanding its :[00..59] suffix to 60 seconds: 28,800 intervals
        // before merging, while no single element reaches the incremental merge threshold
        // (256 intervals). The incremental merge must still bound the list across elements.
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final StringSink expression = new StringSink();
        expression.put("2024-01-01T[");
        for (int minute = 0; minute < 480; minute++) {
            if (minute > 0) {
                expression.put(',');
            }
            expression.put('0').put(minute / 60).put(':');
            if (minute % 60 < 10) {
                expression.put('0');
            }
            expression.put(minute % 60);
        }
        expression.put("]:[00..59]");

        out.clear();
        parseTickExpr(timestampDriver, expression, 0, expression.length(), 0, out);
        Assert.assertEquals(2, out.size());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2024-01-01T00:00:00.000000Z"), out.getQuick(0));
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2024-01-01T08:00:00.000000Z") - 1, out.getQuick(1));
        // without the incremental merge, the list would hold 57,600 longs
        Assert.assertTrue("capacity: " + out.capacity(), out.capacity() <= 4_096);
    }

    @Test
    public void testTimeListBracketWithGlobalTimezone() throws SqlException {
        // Time list with global timezone (applied to all elements)
        // T[09:00,18:00]@+02:00;1h (non-overlapping)
        // Both times in +02:00 offset: 09:00 = 07:00 UTC, 18:00 = 16:00 UTC
        assertBracketInterval(
                "[{lo=2024-01-15T07:00:00.000000Z, hi=2024-01-15T07:59:59.999999Z},{lo=2024-01-15T16:00:00.000000Z, hi=2024-01-15T16:59:59.999999Z}]",
                "2024-01-15T[09:00,18:00]@+02:00;1h"
        );
    }

    @Test
    public void testTimeListBracketWithPerElementTimezoneAndSuffixBracket() throws SqlException {
        // Time list with per-element timezone AND suffix bracket expansion (exercises L1484 tzLo >= 0)
        // T[09:00@+02:00,14:30]:[00,30] - first element has timezone, suffix has bracket
        // 09:00@+02:00 = 07:00 UTC, 14:30 stays as-is (no timezone)
        assertBracketInterval(
                "[{lo=2024-01-15T07:00:00.000000Z, hi=2024-01-15T07:00:00.999999Z},{lo=2024-01-15T07:00:30.000000Z, hi=2024-01-15T07:00:30.999999Z},{lo=2024-01-15T14:30:00.000000Z, hi=2024-01-15T14:30:00.999999Z},{lo=2024-01-15T14:30:30.000000Z, hi=2024-01-15T14:30:30.999999Z}]",
                "2024-01-15T[09:00@+02:00,14:30]:[00,30]"
        );
    }

    @Test
    public void testTimeListBracketWithSeconds() throws SqlException {
        // Time list with full time values including seconds
        assertBracketInterval(
                "[{lo=2024-01-15T09:00:30.000000Z, hi=2024-01-15T09:00:30.999999Z},{lo=2024-01-15T14:30:45.000000Z, hi=2024-01-15T14:30:45.999999Z}]",
                "2024-01-15T[09:00:30,14:30:45]"
        );
    }

    @Test
    public void testTimeListBracketWithSuffixBracketAndDuration() throws SqlException {
        // Time list bracket with suffix bracket expansion AND duration (exercises L1463 semicolon finding)
        // T[09:00,14:30]:[00,30];1h - time list + seconds expansion + duration
        // Creates: 09:00:00+1h, 09:00:30+1h, 14:30:00+1h, 14:30:30+1h - overlapping intervals merge
        assertBracketInterval(
                "[{lo=2024-01-15T09:00:00.000000Z, hi=2024-01-15T10:00:29.999999Z},{lo=2024-01-15T14:30:00.000000Z, hi=2024-01-15T15:30:29.999999Z}]",
                "2024-01-15T[09:00,14:30]:[00,30];1h"
        );
    }

    @Test
    public void testTimeListBracketWithSuffixExpansion() throws SqlException {
        // Time list bracket with suffix expansion: [09:00,14:30]:[00,30]
        // Should create 4 intervals: 09:00:00, 09:00:30, 14:30:00, 14:30:30
        assertBracketInterval(
                "[{lo=2024-01-15T09:00:00.000000Z, hi=2024-01-15T09:00:00.999999Z},{lo=2024-01-15T09:00:30.000000Z, hi=2024-01-15T09:00:30.999999Z},{lo=2024-01-15T14:30:00.000000Z, hi=2024-01-15T14:30:00.999999Z},{lo=2024-01-15T14:30:30.000000Z, hi=2024-01-15T14:30:30.999999Z}]",
                "2024-01-15T[09:00,14:30]:[00,30]"
        );
    }

    @Test
    public void testTimeListBracketWithTrailingCommaAndWhitespace() throws SqlException {
        // Time list with trailing comma and whitespace (exercises L1406/L1409 whitespace to bracket end)
        assertBracketInterval(
                "[{lo=2024-01-15T09:00:00.000000Z, hi=2024-01-15T09:00:59.999999Z}]",
                "2024-01-15T[09:00,   ]"
        );
    }

    // ==================== Day-of-Week Filter Tests ====================

    @Test
    public void testTimeListBracketWithTrailingWhitespace() throws SqlException {
        // Time list with trailing whitespace after last element (exercises L1406 i >= bracketEnd)
        assertBracketInterval(
                "[{lo=2024-01-15T09:00:00.000000Z, hi=2024-01-15T09:00:59.999999Z}]",
                "2024-01-15T[09:00   ]"
        );
    }

    @Test
    public void testTimeListBracketWithWhitespace() throws SqlException {
        // Time list with whitespace around values
        assertBracketInterval(
                "[{lo=2024-01-15T09:00:00.000000Z, hi=2024-01-15T09:59:59.999999Z},{lo=2024-01-15T18:00:00.000000Z, hi=2024-01-15T18:59:59.999999Z}]",
                "2024-01-15T[ 09:00 , 18:00 ];1h"
        );
    }

    @Test
    public void testTimeListBracketWithoutDuration() throws SqlException {
        // Time list without duration (minute-level precision)
        assertBracketInterval(
                "[{lo=2024-01-15T09:00:00.000000Z, hi=2024-01-15T09:00:59.999999Z},{lo=2024-01-15T18:00:00.000000Z, hi=2024-01-15T18:00:59.999999Z}]",
                "2024-01-15T[09:00,18:00]"
        );
    }

    @Test
    public void testTimeListInheritedZoneFilterAfterDroppedTail() throws SqlException {
        // The values of testTimeListNestedZoneFilterAfterDroppedTail, which inherit the time zone
        // of the string or of the date list element instead of carrying their own: Sunday
        // 2024-01-07 and Monday 2024-01-08 at 09:00 at +05:00, which is 04:00Z on the same day, so
        // the day filter drops Sunday by its local and its UTC day alike. A value that inherits a
        // zone also finishes the filter of the leaves below it before it starts, so the dropped
        // Sunday leaves never move Monday's first leaves below the start of their value, where its
        // zone would miss them and leave them in local time.
        final ZoneOffset plus5 = ZoneOffset.ofHours(5);
        // the 16 touching seconds 00-15 of each minute from 09:00 local on Monday
        final LongList minutes24 = new LongList();
        for (int minute = 0; minute < 24; minute++) {
            addLocalBlock(minutes24, LocalDateTime.of(2024, 1, 8, 9, minute), plus5, 16 * Micros.SECOND_MICROS);
        }
        for (CairoConfiguration cfg : new CairoConfiguration[]{configuration, mergeThreshold4Configuration, unmergedConfiguration}) {
            // two values per day, each of 384 leaves
            assertTickExprIntervals(cfg, minutes24, "2024-01-[07..08]T[09:,09:][00..23]:[00..15]@+05:00#mon");
            assertTickExprIntervals(cfg, minutes24, "[2024-01-[07..08]@+05:00]T[09:,09:][00..23]:[00..15]#mon");
        }
    }

    @Test
    public void testTimeListNestedZoneFilterAfterDroppedTail() throws SqlException {
        // Sunday 2024-01-07 and Monday 2024-01-08 at 09:00 at +05:00, which is 04:00Z on the same
        // day, so the day filter drops Sunday by its local and its UTC day alike. The last leaves of
        // a zoned value stay unfiltered until a later merge. A zoned value finishes the filter of the
        // leaves below it before it starts, so the dropped leaves never move its own leaves below
        // its start, where its zone would miss them.
        final ZoneOffset plus5 = ZoneOffset.ofHours(5);
        // the 16 touching seconds 00-15 of each minute from 09:00 local on Monday
        final LongList minutes17 = new LongList();
        final LongList minutes24 = new LongList();
        for (int minute = 0; minute < 24; minute++) {
            if (minute < 17) {
                addLocalBlock(minutes17, LocalDateTime.of(2024, 1, 8, 9, minute), plus5, 16 * Micros.SECOND_MICROS);
            }
            addLocalBlock(minutes24, LocalDateTime.of(2024, 1, 8, 9, minute), plus5, 16 * Micros.SECOND_MICROS);
        }
        for (CairoConfiguration cfg : new CairoConfiguration[]{configuration, mergeThreshold4Configuration, unmergedConfiguration}) {
            // nested time lists: the inner values merge in the outer value's range
            assertTickExprIntervals(
                    cfg,
                    minutes17,
                    "2024-01-[07..08]T[09@+05:00][" + numberedValues(":", 17, "") + "][" + numberedValues(":", 16, "") + "]#mon"
            );
            // two zoned values per day, each of 384 leaves
            assertTickExprIntervals(cfg, minutes24, "2024-01-[07..08]T[09@+05:00,09@+05:00]:[00..23]:[00..15]#mon");
            assertTickExprIntervals(cfg, minutes24, "2024-01-[07..08]T[09:@+05:00,09:@+05:00][00..23]:[00..15]#mon");
            assertTickExprIntervals(cfg, minutes24, "[2024-01-[07..08]]T[09@+05:00,09@+05:00]:[00..23]:[00..15]#mon");
        }
    }

    @Test
    public void testTimeListSuffixBracketsDayFilter() throws SqlException {
        // 48 Mondays x 2 one-minute blocks. The 336 days x 120 minutes = 40,320 leaves merge once
        // per 256 unfiltered leaves.
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final String interval = "2024-[01..12]-[01..28]T[09:00,14:30]:[00..59]#mon";
        out.clear();
        parseTickExprAtMonday(configuration, out, interval);
        Assert.assertEquals(96, out.size() / 2);
        for (int i = 0, n = out.size(); i < n; i += 2) {
            final long lo = out.getQuick(i);
            Assert.assertEquals(1, timestampDriver.getDayOfWeek(lo));
            Assert.assertEquals(lo + timestampDriver.fromMinutes(1) - 1, out.getQuick(i + 1));
        }
        final int getterCallCount = countMaxIntervalsGetterCalls(interval);
        Assert.assertTrue("getter calls: " + getterCallCount, getterCallCount <= 40_320 / 256 + 2);
    }

    @Test
    public void testTimeListSuffixBracketsGrowthBetweenMerges() throws SqlException {
        // Every loop that adds to a merge range unions it once the range grew by the merge
        // threshold since its last union: the values of a time list with bracketed suffixes, also
        // when the suffix holds only another time list, and the numeric brackets around them under
        // a pending day filter. The list holds the same intervals for any number of duplicate
        // values, and its peak size stays within the bound that assertTimeListGrowthBounded
        // checks.
        Assert.assertEquals(256, configuration.getSqlIntervalIncrementalMergeThreshold());
        Assert.assertEquals(1_024, configuration.getSqlIntervalMaxIntervalsAfterMerge());
        final ZoneOffset plus5 = ZoneOffset.ofHours(5);
        final LocalDateTime nine = LocalDateTime.of(2024, 1, 1, 9, 0);
        // the 600 slots of 10 ms of the minute 09:00, local and at +05:00 (04:00Z), and the 120
        // slots of the suffix :[00..59].[0..1]0 at +05:00
        final LongList slotsAt09 = new LongList();
        addMinuteSlots(slotsAt09, nine, ZoneOffset.UTC, 10);
        final LongList slotsAt04 = new LongList();
        addMinuteSlots(slotsAt04, nine, plus5, 10);
        final LongList twoTenthSlotsAt04 = new LongList();
        addMinuteSlots(twoTenthSlotsAt04, nine, plus5, 2);
        final LongList slotsOnMonday = new LongList();
        addMinuteSlots(slotsOnMonday, LocalDateTime.of(2024, 1, 8, 9, 0), ZoneOffset.UTC, 10);
        for (int n : new int[]{1, 10, 30}) {
            final String values = repeatedValues("09:00", n);
            assertTimeListGrowthBounded(slotsAt09, 0, "2024-01-01T[" + values + "]:[00..59].[0..9]0");
            assertTimeListGrowthBounded(slotsAt04, 0, "2024-01-01T[" + values + "]:[00..59].[0..9]0@+05:00");
            assertTimeListGrowthBounded(slotsAt04, 0, "2024-01-01T[" + repeatedValues("09:00@+05:00", n) + "]:[00..59].[0..9]0");
            // 09:00 at +05:00 and 04:00 local are the same slots
            assertTimeListGrowthBounded(slotsAt04, 0, "2024-01-01T[" + repeatedValues("09:00@+05:00,04:00", n) + "]:[00..59].[0..9]0");
            // a pending day filter: 2024-01-01 is a Monday and 2024-01-02 a Tuesday
            assertTimeListGrowthBounded(slotsAt09, 0, "2024-01-01T[" + values + "]:[00..59].[0..9]0#mon");
            assertTimeListGrowthBounded(slotsAt04, 0, "2024-01-01T[" + values + "]:[00..59].[0..9]0@+05:00#mon");
            assertTimeListGrowthBounded(new LongList(), 0, "2024-01-02T[" + values + "]:[00..59].[0..9]0@+05:00#mon");
            // a date list element without and with a time zone, and a day of a date variable range
            assertTimeListGrowthBounded(slotsAt09, 0, "[2024-01-01]T[" + values + "]:[00..59].[0..9]0");
            assertTimeListGrowthBounded(slotsAt04, 0, "[2024-01-01@+05:00]T[" + values + "]:[00..59].[0..9]0");
            assertTimeListGrowthBounded(slotsOnMonday, 0, "[$today..$today]T[" + values + "]:[00..59].[0..9]0");
            // 120 slots per value, below the merge threshold: only the merges between values run
            assertTimeListGrowthBounded(twoTenthSlotsAt04, 0, "2024-01-01T[" + values + "]:[00..59].[0..1]0@+05:00");
            // date list elements with a time zone merge their own range, then the list's
            assertTimeListGrowthBounded(slotsAt04, 0, "[" + repeatedValues("2024-01-01T09:00:[00..59].[0..9]0@+05:00", n) + "]");
        }
        for (int n : new int[]{1, 4}) {
            final String innerValues = repeatedValues("00:", n);
            assertTimeListGrowthBounded(slotsAt09, 0, "2024-01-01T[" + repeatedValues("09:", n) + "][" + innerValues + "][00..59].[0..9]0");
            assertTimeListGrowthBounded(slotsAt04, 0, "2024-01-01T[" + repeatedValues("09:@+05:00", n) + "][" + innerValues + "][00..59].[0..9]0");
        }
        // Nested time lists whose suffix holds only another time list: n x n leaves of one second
        // that no numeric bracket merges
        final LongList secondAt09 = new LongList();
        addLocalBlock(secondAt09, nine, ZoneOffset.UTC, Micros.SECOND_MICROS);
        final LongList secondAt04 = new LongList();
        addLocalBlock(secondAt04, nine, plus5, Micros.SECOND_MICROS);
        final LongList secondOnMonday = new LongList();
        addLocalBlock(secondOnMonday, LocalDateTime.of(2024, 1, 8, 9, 0), ZoneOffset.UTC, Micros.SECOND_MICROS);
        for (int n : new int[]{1, 10, 100}) {
            final String lists = "[" + repeatedValues("09:", n) + "][" + repeatedValues("00:00", n) + "]";
            assertTimeListGrowthBounded(secondAt09, n, "2024-01-01T" + lists);
            assertTimeListGrowthBounded(secondAt09, n, "[2024-01-01]T" + lists);
            assertTimeListGrowthBounded(secondAt09, n, "[2024-01-01T" + lists + "]");
            assertTimeListGrowthBounded(secondOnMonday, n, "[$today..$today]T" + lists);
            final String zonedLists = "[" + repeatedValues("09:@+05:00", n) + "][" + repeatedValues("00:00", n) + "]";
            assertTimeListGrowthBounded(secondAt04, n, "2024-01-01T" + zonedLists);
            assertTimeListGrowthBounded(secondAt04, n, "2024-01-01T" + zonedLists + "#mon");
        }
        // three levels: the middle values merge in the range of the zoned outer value
        for (int n : new int[]{1, 10, 30}) {
            final String innerValues = repeatedValues(":00", n);
            assertTimeListGrowthBounded(secondAt04, n, "2024-01-01T[" + repeatedValues("09@+05:00", n) + "][" + innerValues + "][" + innerValues + "]#mon");
        }
        // 16 levels of two values that are only a zone: 65,536 leaves of the minute 09:00
        final StringBuilder levels = new StringBuilder("2024-01-01T");
        for (int level = 0; level < 16; level++) {
            levels.append("[@+00:00,@+00:00]");
        }
        final LongList minuteAt09 = new LongList();
        addLocalBlock(minuteAt09, nine, ZoneOffset.UTC, Micros.MINUTE_MICROS);
        assertTimeListGrowthBounded(minuteAt09, 1, levels.append("[09:00]").toString());
        // One time list call per day: 510 touching leaves of 100 ms per value and a day filter.
        // The range of the days merges values of many calls, and zoned values merge their own
        // ranges in between.
        final LongList workdaysAt04 = new LongList();
        addWorkdayBlocks(workdaysAt04, 2024, 2024, LocalTime.of(9, 0), plus5, 51 * Micros.SECOND_MICROS);
        Assert.assertEquals(2 * 240, workdaysAt04.size());
        assertTimeListGrowthBounded(workdaysAt04, 0, "2024-[01..12]-[01..28]T[09:00@+05:00]:[00..50].[0..9]#workday");
        final LongList workdaysAt09 = new LongList();
        addWorkdayBlocks(workdaysAt09, 2024, 2024, LocalTime.of(9, 0), ZoneOffset.UTC, 51 * Micros.SECOND_MICROS);
        assertTimeListGrowthBounded(workdaysAt09, 0, "2024-[01..12]-[01..28]T[09:00]:[00..50].[0..9]#workday");
        final LongList fourYearsAt04 = new LongList();
        addWorkdayBlocks(fourYearsAt04, 2020, 2023, LocalTime.of(9, 0), plus5, 51 * Micros.SECOND_MICROS);
        Assert.assertEquals(2 * 960, fourYearsAt04.size());
        assertTimeListGrowthBounded(fourYearsAt04, 0, "20[20..23]-[01..12]-[01..28]T[09:00@+05:00]:[00..50].[0..9]#workday");
        final LongList hoursAt04 = new LongList();
        addWorkdayBlocks(hoursAt04, 2024, 2024, LocalTime.of(9, 0), plus5, Micros.HOUR_MICROS);
        assertTimeListGrowthBounded(hoursAt04, 60, "2024-[01..12]-[01..28]T[09@+05:00][" + numberedValues(":", 60, "") + "]#workday");
    }

    @Test
    public void testTimeListSuffixBracketsMergeCadence() throws SqlException {
        // A merge range unions once it grew by the merge threshold (256) since its last union. The
        // cap reads: the assertion on entry, one per union and the final check.
        Assert.assertEquals(256, configuration.getSqlIntervalIncrementalMergeThreshold());
        // 120 slots per value, below the threshold: the merges after the values 3, 6, ..., 30
        Assert.assertEquals(12, countMaxIntervalsGetterCalls("2024-01-01T[" + repeatedValues("00:00", 30) + "]:[00..59].[0..1]0@+05:00"));
        // each outer value adds the same 120 seconds, and no numeric bracket merges
        Assert.assertEquals(12, countMaxIntervalsGetterCalls("2024-01-01T[" + repeatedValues("09:", 30) + "][" + evenSeconds(120) + "]"));
        // The first element keeps its 4 Mondays, below the threshold. The second element adds its
        // leaves to the merge range of the list, which has grown by the threshold at its 252nd
        // and last leaf. Then the final check runs.
        final String interval = "[2024-01-[01..28]#mon,2024-[01..09]-[01..28]#mon]";
        Assert.assertEquals(3, countMaxIntervalsGetterCalls(interval));
        // the 4 Mondays among days 1-28 of each month from January to September
        final LongList mondays = new LongList();
        for (LocalDate day = LocalDate.of(2024, 1, 1); day.getMonthValue() <= 9; day = day.plusDays(1)) {
            if (day.getDayOfMonth() <= 28 && day.getDayOfWeek() == DayOfWeek.MONDAY) {
                addLocalDays(mondays, day, day.plusDays(1), ZoneOffset.UTC);
            }
        }
        Assert.assertEquals(2 * 36, mondays.size());
        assertTickExprIntervals(configuration, mondays, interval);
    }

    @Test
    public void testTimeListSuffixBracketsOverMaxIntervalsFailsEarly() throws SqlException {
        // controls: the third value fails alone with its own parse error
        assertTickExprErrorAtMonday("2024-01-01T[25:99]:[00..59].[0..9]0", "Invalid date: 2024-01-01T25:99:00.00");
        assertTickExprErrorAtMonday("2024-01-01T[25:][" + evenSeconds(600) + "]", "Invalid date: 2024-01-01T25:00:00");
        // The first two values hold 1,200 intervals that never touch, and the values merge
        // between them, so too-many-intervals wins over the third value: zone-less values add
        // their intervals to the merge range of the expansion, zoned values merge their own range
        // and then the expansion's, and so do the outer values of nested time lists.
        final LongList slots = new LongList();
        addMinuteSlots(slots, LocalDateTime.of(2024, 1, 1, 0, 0), ZoneOffset.UTC, 10);
        for (String interval : new String[]{
                "2024-01-01T[00:00,00:01,25:99]:[00..59].[0..9]0",
                "2024-01-01T[00:00,00:01,25:99]:[00..59].[0..9]0@+05:00",
                "2024-01-01T[00:00@+05:00,00:01@+05:00,25:99@+05:00]:[00..59].[0..9]0",
                "2024-01-01T[09:,10:,25:][" + evenSeconds(600) + "]",
        }) {
            assertTickExprErrorAtMonday(interval, "Bracket expansion produces too many intervals (max 1024)");
            // parseTickExpr unwinds the list, and the next parse on this thread succeeds
            Assert.assertEquals(interval, 0, out.size());
            assertTickExprIntervals(configuration, slots, "2024-01-01T[00:00]:[00..59].[0..9]0");
        }
        // a value with an invalid time zone after the first value merged
        assertTickExprErrorAtMonday("2024-01-01T[00:00@+05:00,00:01@Mars/Base]:[00..59].[0..9]0", "invalid timezone: Mars/Base");
        // A merge threshold of 1 and a cap of 2, the smallest cap above it. Each value adds two
        // one-second intervals and merges them with the expansion's: duplicate values stay at the
        // cap, distinct ones exceed it.
        final CairoConfiguration mergeThreshold1Configuration = new DefaultCairoConfiguration(".") {
            @Override
            public int getSqlIntervalIncrementalMergeThreshold() {
                return 1;
            }

            @Override
            public int getSqlIntervalMaxIntervalsAfterMerge() {
                return 2;
            }
        };
        final LongList secondsAt04 = new LongList();
        addLocalBlock(secondsAt04, LocalDateTime.of(2024, 1, 1, 9, 0, 0), ZoneOffset.ofHours(5), Micros.SECOND_MICROS);
        addLocalBlock(secondsAt04, LocalDateTime.of(2024, 1, 1, 9, 0, 2), ZoneOffset.ofHours(5), Micros.SECOND_MICROS);
        assertTickExprIntervals(mergeThreshold1Configuration, secondsAt04, "2024-01-01T[09:00@+05:00,09:00@+05:00]:[00,02]");
        assertTickExprErrorAtMonday(mergeThreshold1Configuration, "2024-01-01T[09:00@+05:00,10:00@+05:00]:[00,02]", "Bracket expansion produces too many intervals (max 2)");
        final LongList secondsAt09 = new LongList();
        addLocalBlock(secondsAt09, LocalDateTime.of(2024, 1, 1, 9, 0, 0), ZoneOffset.UTC, Micros.SECOND_MICROS);
        addLocalBlock(secondsAt09, LocalDateTime.of(2024, 1, 1, 9, 0, 2), ZoneOffset.UTC, Micros.SECOND_MICROS);
        assertTickExprIntervals(mergeThreshold1Configuration, secondsAt09, "2024-01-01T[09:,09:][00:00,00:02]");
        assertTickExprErrorAtMonday(mergeThreshold1Configuration, "2024-01-01T[09:,10:][00:00,00:02]", "Bracket expansion produces too many intervals (max 2)");
    }

    @Test
    public void testTimezoneAtSignInsideBracketsIgnored() {
        // Test that @ inside brackets is NOT treated as timezone marker - exercises L1543 depth check
        // The string "2024-01-[15@00,16]" has @ inside brackets - should fail as invalid bracket content
        // not as invalid timezone
        final TimestampDriver timestampDriver = timestampType.getDriver();
        out.clear();
        String interval = "2024-01-[15@00,16]";
        try {
            parseTickExpr(timestampDriver, interval, 0, interval.length(), 0, out);
            Assert.fail("Expected SqlException for invalid bracket content");
        } catch (SqlException e) {
            // Should fail parsing the bracket content, not as "invalid timezone"
            Assert.assertFalse("Error should not mention timezone", e.getMessage().contains("timezone"));
        }
    }

    @Test
    public void testTimezoneBracketExpansionWithTimezoneAndDuration() throws SqlException {
        // Test combination: bracket expansion + timezone + duration
        // 2024-01-[15,16]T08:00@+02:00;1h
        // For each date: 08:00 in +02:00 = 06:00 UTC, duration extends hi by 1h
        assertBracketInterval(
                "[{lo=2024-01-15T06:00:00.000000Z, hi=2024-01-15T06:59:59.999999Z},{lo=2024-01-16T06:00:00.000000Z, hi=2024-01-16T06:59:59.999999Z}]",
                "2024-01-[15,16]T08:00@+02:00;1h"
        );
    }

    @Test
    public void testTimezoneDateListGlobalTimezone() throws SqlException {
        // Date list with global timezone applied to all elements
        // [2024-01-15,2024-01-16]T08:00@+02:00
        assertBracketInterval(
                "[{lo=2024-01-15T06:00:00.000000Z, hi=2024-01-15T06:00:59.999999Z},{lo=2024-01-16T06:00:00.000000Z, hi=2024-01-16T06:00:59.999999Z}]",
                "[2024-01-15,2024-01-16]T08:00@+02:00"
        );
    }

    @Test
    public void testTimezoneDateListGlobalTimezoneWithDuration() throws SqlException {
        // Date list with global timezone AND duration suffix - tests L1077 branch
        // [2024-01-15,2024-01-16]T08:00@+02:00;1h
        // 08:00 in +02:00 = 06:00 UTC, ;1h extends hi by 1 hour to 07:00:59 UTC
        assertBracketInterval(
                "[{lo=2024-01-15T06:00:00.000000Z, hi=2024-01-15T06:59:59.999999Z},{lo=2024-01-16T06:00:00.000000Z, hi=2024-01-16T06:59:59.999999Z}]",
                "[2024-01-15,2024-01-16]T08:00@+02:00;1h"
        );
    }

    @Test
    public void testTimezoneDateListMixedTimezones() throws SqlException {
        // Date list with mixed: some per-element, some using global
        // [2024-01-15@UTC,2024-01-16]T08:00@+03:00
        // First: has own timezone UTC -> 08:00 UTC
        // Second: uses global +03:00 -> 05:00 UTC
        assertBracketInterval(
                "[{lo=2024-01-15T08:00:00.000000Z, hi=2024-01-15T08:00:59.999999Z},{lo=2024-01-16T05:00:00.000000Z, hi=2024-01-16T05:00:59.999999Z}]",
                "[2024-01-15@UTC,2024-01-16]T08:00@+03:00"
        );
    }

    @Test
    public void testTimezoneDateListPerElementNamedTimezone() throws SqlException {
        // Date list with per-element named timezones
        // [2024-01-15@Europe/London,2024-07-15@Europe/London]T08:00
        // Winter: 08:00 in UTC+0 = 08:00 UTC
        // Summer: 08:00 in UTC+1 = 07:00 UTC
        assertBracketInterval(
                "[{lo=2024-01-15T08:00:00.000000Z, hi=2024-01-15T08:00:59.999999Z},{lo=2024-07-15T07:00:00.000000Z, hi=2024-07-15T07:00:59.999999Z}]",
                "[2024-01-15@Europe/London,2024-07-15@Europe/London]T08:00"
        );
    }

    @Test
    public void testTimezoneDateListPerElementTimezone() throws SqlException {
        // Date list with per-element timezones
        // [2024-01-15@+02:00,2024-01-16@+05:00]T08:00
        // First: 08:00 in +02:00 = 06:00 UTC
        // Second: 08:00 in +05:00 = 03:00 UTC
        assertBracketInterval(
                "[{lo=2024-01-15T06:00:00.000000Z, hi=2024-01-15T06:00:59.999999Z},{lo=2024-01-16T03:00:00.000000Z, hi=2024-01-16T03:00:59.999999Z}]",
                "[2024-01-15@+02:00,2024-01-16@+05:00]T08:00"
        );
    }

    @Test
    public void testTimezoneDateListWithBracketExpansion() throws SqlException {
        // Date list element with bracket expansion and timezone
        // [2024-01-[15,16]@+02:00]T08:00
        assertBracketInterval(
                "[{lo=2024-01-15T06:00:00.000000Z, hi=2024-01-15T06:00:59.999999Z},{lo=2024-01-16T06:00:00.000000Z, hi=2024-01-16T06:00:59.999999Z}]",
                "[2024-01-[15,16]@+02:00]T08:00"
        );
    }

    @Test
    public void testTimezoneDateListWithTimeListBracketAndGlobalTimezone() throws SqlException {
        // Date list element with time list brackets + global timezone
        // This tests L1329: time list handles TZ internally, so we skip applying it again
        // [2024-01-15T[09:00,14:30]]@+02:00
        // 09:00 in +02:00 = 07:00 UTC, 14:30 in +02:00 = 12:30 UTC
        assertBracketInterval(
                "[{lo=2024-01-15T07:00:00.000000Z, hi=2024-01-15T07:00:59.999999Z},{lo=2024-01-15T12:30:00.000000Z, hi=2024-01-15T12:30:59.999999Z}]",
                "[2024-01-15T[09:00,14:30]]@+02:00"
        );
    }

    @Test
    public void testTimezoneDstGapHiOnlyInGap() throws SqlException {
        // On March 10, 2024 at 2:00 AM in America/New_York, clocks spring forward to 3:00 AM.
        // The ;2m duration extends the interval from 01:59 EST into the gap. Local times from
        // 02:00 on do not exist, so the interval ends right before the transition, 07:00 UTC.
        assertBracketInterval(
                "[{lo=2024-03-10T06:59:00.000000Z, hi=2024-03-10T06:59:59.999999Z}]",
                "2024-03-10T01:59@America/New_York;2m"
        );
    }

    @Test
    public void testTimezoneDstGapIntervalStartsInGap() throws SqlException {
        // On March 31, 2024, London clocks spring forward from 01:00 GMT to 02:00 BST, so local
        // times from 01:00 to 01:59:59.999999 do not exist. An interval that starts in the gap
        // starts at the transition, 01:00 UTC. 01:10 to 02:04:59.999999 local time keeps the
        // five minutes from 02:00 BST that exist.
        assertBracketInterval(
                "[{lo=2024-03-31T01:00:00.000000Z, hi=2024-03-31T01:04:59.999999Z}]",
                "2024-03-31T01:10@Europe/London;55m"
        );
        // New York springs forward from 02:00 EST to 03:00 EDT, 07:00 UTC
        assertBracketInterval(
                "[{lo=2024-03-10T07:00:00.000000Z, hi=2024-03-10T07:04:59.999999Z}]",
                "2024-03-10T02:10@America/New_York;55m"
        );
        // both paths share the conversion
        assertCompiledTickExprWithNow("$todayT01:10@Europe/London;55m", "2024-03-31T12:00:00.000000Z");
        assertCompiledTickExprWithNow("$todayT01:00@Europe/London;30m", "2024-03-31T12:00:00.000000Z");
    }

    @Test
    public void testTimezoneDstGapMinuteMatchesNothing() throws SqlException {
        // On March 10, 2024 at 2:00 AM in America/New_York, clocks spring forward to 3:00 AM, so no
        // instant has a local time from 02:00 to 02:59:59.999999, and an interval in that time
        // matches nothing
        assertBracketInterval("[]", "2024-03-10T02:30@America/New_York");
        assertBracketInterval("[]", "2024-03-10T02:00@America/New_York;30m");
        assertParsedAndCompiledWithNow("[]", "$todayT02:30@America/New_York", "2024-03-10T12:00:00.000000Z");
        // so a day covers the same instants as its minutes, also when the day ends in the gap, as
        // in Nuuk, where clocks skip from 23:00 to 00:00 on 2024-03-30
        assertBracketInterval("[{lo=2024-03-10T05:00:00.000000Z, hi=2024-03-11T03:59:59.999999Z}]", "2024-03-10T[00..23]:[00..59]@America/New_York");
        assertBracketInterval("[{lo=2024-03-10T05:00:00.000000Z, hi=2024-03-11T03:59:59.999999Z}]", "2024-03-10@America/New_York");
        assertBracketInterval("[{lo=2024-03-30T02:00:00.000000Z, hi=2024-03-31T00:59:59.999999Z}]", "2024-03-30T[00..23]:[00..59]@America/Nuuk");
        assertBracketInterval("[{lo=2024-03-30T02:00:00.000000Z, hi=2024-03-31T00:59:59.999999Z}]", "2024-03-30@America/Nuuk");
        // a time list drops the minute in the gap and keeps the next one
        assertBracketInterval("[{lo=2024-03-10T07:30:00.000000Z, hi=2024-03-10T07:30:59.999999Z}]", "2024-03-10T[02:30,03:30]@America/New_York");
    }

    @Test
    public void testTimezoneDstGapOverlappingIntervals() throws SqlException {
        // On March 31, 2024, London clocks spring forward from 01:00 GMT to 02:00 BST, at 01:00 UTC.
        // Shifting the local times in the gap forward used to make the first three of these 50-minute
        // intervals overlap in UTC. Local times from 01:00 to 01:59:59.999999 do not exist, so the
        // first interval, 00:30 to 01:19:59.999999 local time, ends right before the transition, and
        // the second, 01:30 to 02:19:59.999999, starts at it. The intervals stay sorted and disjoint.
        assertBracketInterval(
                "[{lo=2024-03-31T00:30:00.000000Z, hi=2024-03-31T00:59:59.999999Z}," +
                        "{lo=2024-03-31T01:00:00.000000Z, hi=2024-03-31T01:19:59.999999Z}," +
                        "{lo=2024-03-31T01:30:00.000000Z, hi=2024-03-31T02:19:59.999999Z}," +
                        "{lo=2024-03-31T02:30:00.000000Z, hi=2024-03-31T03:19:59.999999Z}]",
                "2024-03-31T00:30@Europe/London;50m;1h;4"
        );
    }

    @Test
    public void testTimezoneDstGapRangeStartsInGap() throws SqlException {
        // On 2024-03-31 London clocks skip from 01:00 to 02:00 local time, and $now - 2h = 01:20
        // local falls in that hour. The range starts at the transition, 01:00Z, and ends with the
        // day, at 23:59:59.999999 BST.
        assertParsedAndCompiledWithNow(
                "[{lo=2024-03-31T01:00:00.000000Z, hi=2024-03-31T22:59:59.999999Z}]",
                "[$now - 2h..$today]@Europe/London",
                "2024-03-31T03:20:00.000000Z"
        );
    }

    @Test
    public void testTimezoneDstGapWholeDayAtMidnight() throws SqlException {
        // Some timezones spring forward at midnight, so the day starts at 01:00 local time, and
        // has 23 hours
        assertBracketInterval("[{lo=2024-09-08T04:00:00.000000Z, hi=2024-09-09T02:59:59.999999Z}]", "2024-09-08@America/Santiago");
        assertBracketInterval("[{lo=2024-03-10T05:00:00.000000Z, hi=2024-03-11T03:59:59.999999Z}]", "2024-03-10@America/Havana");
        assertBracketInterval("[{lo=2024-03-30T22:00:00.000000Z, hi=2024-03-31T20:59:59.999999Z}]", "2024-03-31@Asia/Beirut");
        assertBracketInterval("[{lo=2024-04-25T22:00:00.000000Z, hi=2024-04-26T20:59:59.999999Z}]", "2024-04-26@Africa/Cairo");
        assertParsedAndCompiledWithNow(
                "[{lo=2024-09-08T04:00:00.000000Z, hi=2024-09-09T02:59:59.999999Z}]",
                "$today@America/Santiago",
                "2024-09-08T12:00:00.000000Z"
        );
    }

    @Test
    public void testTimezoneDstOverlapFallBack() throws SqlException {
        // On Nov 3, 2024 in America/New_York, at 2:00 AM EDT clocks fall back to 1:00 AM EST, so
        // 1:30 AM happens twice: at 05:30 UTC in EDT, and at 06:30 UTC in EST. The minute covers
        // both.
        assertBracketInterval(
                "[{lo=2024-11-03T05:30:00.000000Z, hi=2024-11-03T05:30:59.999999Z},{lo=2024-11-03T06:30:00.000000Z, hi=2024-11-03T06:30:59.999999Z}]",
                "2024-11-03T01:30@America/New_York"
        );
    }

    @Test
    public void testTimezoneDstOverlapIntervalShapes() throws SqlException {
        // On 2024-10-27 Berlin clocks fall back from 03:00 CEST to 02:00 CET at 01:00Z, so local
        // times from 02:00 to 02:59:59.999999 happen twice, at 00:00Z to 00:59:59.999999Z and at
        // 01:00Z to 01:59:59.999999Z. An interval covers the instants whose local time falls in it.
        // 02:30 to 03:29:59.999999 local time: 02:30 to 02:59 in CEST, then 02:30 to 03:29 in CET
        assertBracketInterval(
                "[{lo=2024-10-27T00:30:00.000000Z, hi=2024-10-27T00:59:59.999999Z},{lo=2024-10-27T01:30:00.000000Z, hi=2024-10-27T02:29:59.999999Z}]",
                "2024-10-27T02:30@Europe/Berlin;1h"
        );
        // 01:30 to 02:29:59.999999 local time: 01:30 to 02:29 in CEST, then 02:00 to 02:29 in CET
        assertBracketInterval(
                "[{lo=2024-10-26T23:30:00.000000Z, hi=2024-10-27T00:29:59.999999Z},{lo=2024-10-27T01:00:00.000000Z, hi=2024-10-27T01:29:59.999999Z}]",
                "2024-10-27T01:30@Europe/Berlin;1h"
        );
        // the whole repeated hour
        assertBracketInterval(
                "[{lo=2024-10-27T00:00:00.000000Z, hi=2024-10-27T01:59:59.999999Z}]",
                "2024-10-27T02:00@Europe/Berlin;1h"
        );
        // Both ends repeat, a year apart: Berlin falls back at 01:00Z on 2025-10-26 too
        assertBracketInterval(
                "[{lo=2024-10-27T00:30:00.000000Z, hi=2024-10-27T00:59:59.999999Z}," +
                        "{lo=2024-10-27T01:30:00.000000Z, hi=2025-10-26T00:44:59.999999Z}," +
                        "{lo=2025-10-26T01:00:00.000000Z, hi=2025-10-26T01:44:59.999999Z}]",
                "2024-10-27T02:30@Europe/Berlin;364d15m"
        );
        assertParsedAndCompiledWithNow(
                "[{lo=2024-10-27T00:30:00.000000Z, hi=2024-10-27T00:59:59.999999Z},{lo=2024-10-27T01:30:00.000000Z, hi=2024-10-27T02:29:59.999999Z}]",
                "$todayT02:30@Europe/Berlin;1h",
                "2024-10-27T12:00:00.000000Z"
        );
    }

    @Test
    public void testTimezoneDstOverlapTimeListSlotClip() throws SqlException {
        // On 2024-10-27 Berlin clocks fall back at 01:00Z, and $now = 02:30 local time happens
        // twice, at 00:30Z and at 01:30Z. A slot with its own timezone is a point in time, which
        // the range compares with the instants whose Berlin time falls in it. In Tokyo, 09:45 is
        // 00:45Z, 10:00 is 01:00Z and 10:45 is 01:45Z.
        final String now = "2024-10-27T02:30:00.000000Z";
        // from 02:30 Berlin time on: 00:30Z to 00:59:59.999999Z, and from 01:30Z on
        assertParsedAndCompiledWithNow(
                "[{lo=2024-10-27T00:45:00.000000Z, hi=2024-10-27T00:45:59.999999Z},{lo=2024-10-27T01:45:00.000000Z, hi=2024-10-27T01:45:59.999999Z}]",
                "[$now..$today]T[09:45@Asia/Tokyo,10:00@Asia/Tokyo,10:45@Asia/Tokyo]@Europe/Berlin",
                now
        );
        // up to 02:30 Berlin time: up to 00:30Z, and 01:00Z to 01:30Z
        assertParsedAndCompiledWithNow(
                "[{lo=2024-10-27T01:00:00.000000Z, hi=2024-10-27T01:00:59.999999Z}]",
                "[$today..$now]T[09:45@Asia/Tokyo,10:00@Asia/Tokyo,10:45@Asia/Tokyo]@Europe/Berlin",
                now
        );
    }

    @Test
    public void testTimezoneEmpty() {
        // Test empty timezone after @ - should throw error
        assertBracketIntervalError("2024-01-15T08:00@", "invalid timezone");
    }

    @Test
    public void testTimezoneExtremeNegativeOffset() throws SqlException {
        // Test extreme negative offset -12:00
        // 08:00 in -12:00 = 08:00 + 12:00 = 20:00 UTC
        assertBracketInterval(
                "[{lo=2024-01-15T20:00:00.000000Z, hi=2024-01-15T20:00:59.999999Z}]",
                "2024-01-15T08:00@-12:00"
        );
    }

    @Test
    public void testTimezoneExtremePositiveOffset() throws SqlException {
        // Test extreme positive offset +14:00 (e.g., Pacific/Kiritimati)
        // 08:00 in +14:00 = 08:00 - 14:00 = -06:00 = previous day 18:00 UTC
        assertBracketInterval(
                "[{lo=2024-01-14T18:00:00.000000Z, hi=2024-01-14T18:00:59.999999Z}]",
                "2024-01-15T08:00@+14:00"
        );
    }

    @Test
    public void testTimezoneGMT() throws SqlException {
        // GMT timezone
        assertBracketInterval(
                "[{lo=2024-01-15T08:00:00.000000Z, hi=2024-01-15T08:00:59.999999Z}]",
                "2024-01-15T08:00@GMT"
        );
    }

    @Test
    public void testTimezoneIncompleteOffset() {
        // Test incomplete offsets - potential NPE cases
        final TimestampDriver timestampDriver = timestampType.getDriver();
        String[] testCases = {"2024-01-15T08:00@+", "2024-01-15T08:00@-", "2024-01-15T08:00@+:", "2024-01-15T08:00@+00"};
        for (String interval : testCases) {
            out.clear();
            try {
                parseTickExpr(timestampDriver, interval, 0, interval.length(), 0, out);
                // If it succeeds without error, that's also fine
            } catch (SqlException e) {
                // Expected - invalid timezone
            } catch (NullPointerException e) {
                Assert.fail("NPE for input '" + interval + "' - need null check");
            }
        }
    }

    @Test
    public void testTimezoneInvalid() {
        // Invalid timezone should throw error
        assertBracketIntervalError("2024-01-15T08:00@InvalidZone", "invalid timezone");
    }

    @Test
    public void testTimezoneMalformedOffset() {
        // Malformed offset should throw error
        assertBracketIntervalError("2024-01-15T08:00@+02:x0", "invalid timezone");
    }

    @Test
    public void testTimezoneNamedTimezone() throws SqlException {
        // 2024-01-15T08:00 in Europe/London (winter, UTC+0) = 2024-01-15T08:00:00Z in UTC
        assertBracketInterval(
                "[{lo=2024-01-15T08:00:00.000000Z, hi=2024-01-15T08:00:59.999999Z}]",
                "2024-01-15T08:00@Europe/London"
        );
    }

    @Test
    public void testTimezoneNamedTimezoneSummer() throws SqlException {
        // 2024-07-15T08:00 in Europe/London (summer, UTC+1) = 2024-07-15T07:00:00Z in UTC
        assertBracketInterval(
                "[{lo=2024-07-15T07:00:00.000000Z, hi=2024-07-15T07:00:59.999999Z}]",
                "2024-07-15T08:00@Europe/London"
        );
    }

    @Test
    public void testTimezoneNegativeOffset() throws SqlException {
        // 08:00 in -05:00 = 13:00 UTC
        assertBracketInterval(
                "[{lo=2024-01-15T13:00:00.000000Z, hi=2024-01-15T13:00:59.999999Z}]",
                "2024-01-15T08:00@-05:00"
        );
    }

    @Test
    public void testTimezoneRangeExpansion() throws SqlException {
        // 08:00 in +02:00 = 06:00 UTC for each day
        assertBracketInterval(
                "[{lo=2024-01-15T06:00:00.000000Z, hi=2024-01-15T06:00:59.999999Z},{lo=2024-01-16T06:00:00.000000Z, hi=2024-01-16T06:00:59.999999Z},{lo=2024-01-17T06:00:00.000000Z, hi=2024-01-17T06:00:59.999999Z}]",
                "2024-01-[15..17]T08:00@+02:00"
        );
    }

    @Test
    public void testTimezoneSimpleNumericOffset() throws SqlException {
        // 08:00 in +03:00 = 05:00 UTC
        assertBracketInterval(
                "[{lo=2024-01-15T05:00:00.000000Z, hi=2024-01-15T05:00:59.999999Z}]",
                "2024-01-15T08:00@+03:00"
        );
    }

    @Test
    public void testTimezoneUTC() throws SqlException {
        // UTC timezone (no-op)
        assertBracketInterval(
                "[{lo=2024-01-15T08:00:00.000000Z, hi=2024-01-15T08:00:59.999999Z}]",
                "2024-01-15T08:00@UTC"
        );
    }

    @Test
    public void testTimezoneWithBracketExpansion() throws SqlException {
        // 08:00 in +02:00 = 06:00 UTC for each day
        assertBracketInterval(
                "[{lo=2024-01-15T06:00:00.000000Z, hi=2024-01-15T06:00:59.999999Z},{lo=2024-01-16T06:00:00.000000Z, hi=2024-01-16T06:00:59.999999Z}]",
                "2024-01-[15,16]T08:00@+02:00"
        );
    }

    @Test
    public void testTimezoneWithBracketExpansionAndDuration() throws SqlException {
        // 08:00+1h in +02:00 = 06:00-07:00:59 UTC for each day
        assertBracketInterval(
                "[{lo=2024-01-15T06:00:00.000000Z, hi=2024-01-15T06:59:59.999999Z},{lo=2024-01-16T06:00:00.000000Z, hi=2024-01-16T06:59:59.999999Z}]",
                "2024-01-[15,16]T08:00@+02:00;1h"
        );
    }

    @Test
    public void testTimezoneWithDurationSuffix() throws SqlException {
        // Timezone before duration suffix
        // 2024-01-15T08:00@+03:00;1h
        // 08:00 is minute-level, so hi = 08:00:59, then +1h = 09:00:59
        // Convert to UTC: lo = 08:00 - 3h = 05:00, hi = 09:00:59 - 3h = 06:00:59
        assertBracketInterval(
                "[{lo=2024-01-15T05:00:00.000000Z, hi=2024-01-15T05:59:59.999999Z}]",
                "2024-01-15T08:00@+03:00;1h"
        );
    }

    @Test
    public void testTimezoneZ() {
        // Test @Z - potential NPE if getZoneRules returns null
        final TimestampDriver timestampDriver = timestampType.getDriver();
        out.clear();
        String interval = "2024-01-15T08:00@Z";
        try {
            parseTickExpr(timestampDriver, interval, 0, interval.length(), 0, out);
            // If it succeeds, Z should be treated as UTC
            Assert.assertEquals(2, out.size());
        } catch (SqlException e) {
            // Also acceptable if it throws invalid timezone
            Assert.assertTrue("Error should mention invalid timezone", e.getMessage().contains("invalid timezone"));
        } catch (NullPointerException e) {
            Assert.fail("NPE indicates getZoneRules returned null - need null check");
        }
    }

    @Test
    public void testTimezoneZeroOffset() throws SqlException {
        // Test +00:00 offset - should be same as UTC
        assertBracketInterval(
                "[{lo=2024-01-15T08:00:00.000000Z, hi=2024-01-15T08:00:59.999999Z}]",
                "2024-01-15T08:00@+00:00"
        );
    }

    @Test
    public void testWhitespaceOnlyInput() {
        // All whitespace input (exercises line 426 - firstNonSpace reaches lim)
        assertBracketIntervalError("   ", "Invalid date");
    }

    /**
     * Appends $today shifted by a number of days.
     */
    private static void appendDayOffset(StringSink sink, int days) {
        sink.put("$today");
        if (days != 0) {
            sink.put(days > 0 ? " + " : " - ").put(Math.abs(days)).put('d');
        }
    }

    private static void appendRandomDateVariableExpr(Rnd rnd, StringSink sink) {
        sink.put(pick(rnd, FUZZ_VARIABLES));
        if (rnd.nextBoolean()) {
            sink.put(pick(rnd, FUZZ_SHIFTS));
        }
        if (rnd.nextInt(3) == 0) {
            sink.put("..").put(pick(rnd, FUZZ_RANGE_ENDS));
        }
    }

    /**
     * Appends a random tick expression with date variables, either bare or in a date list, and
     * returns whether the expression holds a part that the grammar rejects.
     */
    static boolean appendRandomTickExpr(Rnd rnd, StringSink sink) {
        boolean isMalformed = false;
        if (rnd.nextInt(8) == 0) {
            sink.put("  ");
        }
        if (rnd.nextInt(3) == 0) {
            appendRandomDateVariableExpr(rnd, sink);
        } else {
            sink.put('[');
            for (int i = 0, n = 1 + rnd.nextInt(3); i < n; i++) {
                if (i > 0) {
                    sink.put(rnd.nextBoolean() ? "," : ", ");
                }
                if (rnd.nextInt(4) == 0) {
                    sink.put(pick(rnd, FUZZ_STATIC_ELEMENTS));
                } else {
                    appendRandomDateVariableExpr(rnd, sink);
                }
                // an element with a suffix of its own
                switch (rnd.nextInt(10)) {
                    case 0 -> sink.put(pick(rnd, FUZZ_ZONES));
                    case 1 -> sink.put(pick(rnd, FUZZ_DAY_FILTERS));
                    case 2 -> {
                        // a time part on an element rather than after the list, which counts as
                        // well-formed unless a slot of it has a timezone of its own, see
                        // DateVariableDetectionChecker
                        final String time = pick(rnd, FUZZ_TIMES);
                        sink.put(time);
                        isMalformed |= time.indexOf('@') >= 0;
                    }
                    // a timezone that does not parse, which counts as well-formed, see
                    // DateVariableDetectionChecker
                    case 3 -> sink.put(pick(rnd, FUZZ_MALFORMED_ELEMENT_ZONES));
                    // a timezone that ends at a day filter of the element, which can be one that
                    // does not parse, which counts as well-formed, see DateVariableDetectionChecker
                    case 4 -> sink.put(pick(rnd, FUZZ_ELEMENT_ZONES_BEFORE_DAY_FILTER)).put(pick(rnd, FUZZ_ELEMENT_DAY_FILTERS));
                    default -> {
                    }
                }
            }
            sink.put(']');
        }
        if (rnd.nextInt(10) == 0) {
            // a time part of the list that the grammar rejects, which counts as well-formed, see
            // DateVariableDetectionChecker
            sink.put(pick(rnd, FUZZ_MALFORMED_TIMES));
        } else {
            sink.put(pick(rnd, FUZZ_TIMES));
        }
        sink.put(pick(rnd, FUZZ_ZONES));
        final String dayFilter = pick(rnd, FUZZ_DAY_FILTERS);
        final String duration;
        switch (rnd.nextInt(20)) {
            case 0, 1 -> duration = pick(rnd, FUZZ_UNKNOWN_UNIT_DURATIONS);
            case 2 -> duration = pick(rnd, FUZZ_REJECTED_SUFFIXES);
            case 3 -> {
                duration = FUZZ_REPEATING_SUFFIX;
                isMalformed = true;
            }
            default -> duration = pick(rnd, FUZZ_DURATIONS);
        }
        if (rnd.nextInt(10) == 0) {
            // the day filter after the duration, which makes the duration malformed
            sink.put(duration).put(dayFilter);
        } else {
            sink.put(dayFilter).put(duration);
        }
        return isMalformed;
    }

    /**
     * Appends a list of one to three elements, one of them a date range of 250 to 1,100 days, the
     * others ranges, days, static dates and points in time that lie earlier or later, in any order,
     * then a time part, timezone, day filter and duration. Counts the shapes that its choices make at
     * a "now" on {@code today}, with the merge threshold, in {@code shapes}.
     */
    private static void appendRandomLongRangeTickExpr(Rnd rnd, StringSink sink, LocalDate today, int mergeThreshold, LongRangeShapes shapes) {
        final int elementCount = 1 + rnd.nextInt(3);
        final int longRangeIndex = rnd.nextInt(elementCount);
        // now and then a long range, then a day or a static date before it, then a range
        final boolean isEarlierAfterRange = elementCount == 3 && rnd.nextInt(4) == 0;
        // the first and the last day of each element, as days after today
        final int[] firstDays = new int[elementCount];
        final int[] lastDays = new int[elementCount];
        final boolean[] isRange = new boolean[elementCount];
        sink.put('[');
        for (int i = 0; i < elementCount; i++) {
            if (i > 0) {
                sink.put(", ");
            }
            final int kind = isEarlierAfterRange ? (i == 1 ? 2 + rnd.nextInt(2) : rnd.nextInt(2)) : (i == longRangeIndex ? 0 : rnd.nextInt(5));
            switch (kind) {
                case 0, 1 -> {
                    // a long range, or a short one
                    final int length = kind == 0 || (isEarlierAfterRange && i == 0) ? 250 + rnd.nextInt(851) : 1 + rnd.nextInt(30);
                    firstDays[i] = length >= 250 ? -1500 + rnd.nextInt(1900) : -1600 + rnd.nextInt(3200);
                    lastDays[i] = firstDays[i] + length - 1;
                    isRange[i] = true;
                    appendDayOffset(sink, firstDays[i]);
                    sink.put("..");
                    appendDayOffset(sink, lastDays[i]);
                }
                case 2, 3 -> {
                    // a day, or a static date, before the range when it follows one
                    firstDays[i] = lastDays[i] = isEarlierAfterRange ? firstDays[0] - 1 - rnd.nextInt(100) : -1600 + rnd.nextInt(3200);
                    if (kind == 2) {
                        appendDayOffset(sink, firstDays[i]);
                    } else {
                        sink.put(today.plusDays(firstDays[i]).toString());
                    }
                }
                default -> {
                    // a range between two points in time, which ends now
                    firstDays[i] = lastDays[i] = 0;
                    sink.put("$now - ").put(1 + rnd.nextInt(48)).put("h..$now");
                }
            }
        }
        sink.put(']');
        final String time = pick(rnd, FUZZ_LONG_RANGE_TIMES);
        final String zone = pick(rnd, FUZZ_LONG_RANGE_ZONES);
        final String dayFilter = pick(rnd, FUZZ_LONG_RANGE_DAY_FILTERS);
        sink.put(time).put(zone).put(dayFilter).put(pick(rnd, FUZZ_LONG_RANGE_DURATIONS));

        final int slotCount = time.length() - time.replace(",", "").length() + 1;
        final Predicate<LocalDate> isIncluded = switch (dayFilter) {
            case "#Sat" -> day -> day.getDayOfWeek() == DayOfWeek.SATURDAY;
            case "#workday" -> day -> day.getDayOfWeek() != DayOfWeek.SATURDAY && day.getDayOfWeek() != DayOfWeek.SUNDAY;
            default -> day -> true;
        };
        boolean hasLaterElementBeforeRange = false;
        boolean hasEarlierElementAfterMergedRange = false;
        boolean hasEmptyDaysAfterFirstMerge = false;
        boolean hasDstSplitAfterFirstMerge = false;
        for (int i = 0; i < elementCount; i++) {
            if (!isRange[i]) {
                continue;
            }
            for (int j = 0; j < i; j++) {
                hasLaterElementBeforeRange |= firstDays[j] > lastDays[i];
            }
            // the days of the range up to the one that reaches the threshold on its own, and the days
            // after that one which the day filter leaves without an interval, or on which London
            // turns the clocks back at 01:00 local time for the second time
            int intervalCount = 0;
            for (LocalDate day = today.plusDays(firstDays[i]), last = today.plusDays(lastDays[i]); !day.isAfter(last); day = day.plusDays(1)) {
                final boolean isMerged = intervalCount >= mergeThreshold;
                if (isIncluded.test(day)) {
                    intervalCount += slotCount;
                } else {
                    hasEmptyDaysAfterFirstMerge |= isMerged;
                }
                hasDstSplitAfterFirstMerge |= isMerged
                        && dayFilter.isEmpty()
                        && "@Europe/London".equals(zone)
                        && time.contains("01:30")
                        && day.getMonth() == Month.OCTOBER
                        && day.equals(day.with(TemporalAdjusters.lastInMonth(DayOfWeek.SUNDAY)));
            }
            // a merged range, then an element that lies earlier, then another range: the first day of
            // that range merges with intervals before and after all merged ones
            if (intervalCount >= mergeThreshold) {
                for (int j = i + 1; j < elementCount; j++) {
                    if (!isRange[j] && lastDays[j] < firstDays[i]) {
                        for (int k = j + 1; k < elementCount; k++) {
                            hasEarlierElementAfterMergedRange |= isRange[k];
                        }
                    }
                }
            }
        }
        shapes.laterElementBeforeRangeCount += hasLaterElementBeforeRange ? 1 : 0;
        shapes.earlierElementAfterMergedRangeCount += hasEarlierElementAfterMergedRange ? 1 : 0;
        shapes.emptyDaysAfterFirstMergeCount += hasEmptyDaysAfterFirstMerge ? 1 : 0;
        shapes.dstSplitAfterFirstMergeCount += hasDstSplitAfterFirstMerge ? 1 : 0;
    }

    @Test
    public void testZonedElementBehindCoverMatchesUnmergedOracle() throws SqlException {
        // The zoned elements and the zoned day below hold 5 hours x 60 minutes x 10 seconds x 10 =
        // 30,000 slots of 10 ms that never touch, in a merge range of their own, behind an element
        // that covers them. Their unions by the merge threshold and after the last leaf must give
        // the intervals of a single union at the end of the parse.
        final String suffix = ":[00..59]:[00..09].[0..9]0";
        final String[] intervals = {
                "[2024-01-01,2024-01-01T[09..13]" + suffix + "]@America/New_York",
                "[2024-01-01,2024-01-01T[13,12,11,10,09]" + suffix + "]@America/New_York",
                "[2024-01-01,2024-01-01T[09..13]" + suffix + "@America/New_York]",
                "[2024-01-08T09,2024-01-08T10,2024-01-08T11,2024-01-08T12,2024-01-08T13,$today..$today]T[09..13]" + suffix + "@America/New_York",
        };
        for (int mergeThreshold : new int[]{256, 4}) {
            assertMatchesUnmergedOracle(mergeThreshold, intervals);
        }
        // The oracle runs the same parser, so java.time pins the results. The slots of one element
        // exceed the cap (1,024), which this check lifts. New York keeps -05:00 in January, and
        // $today is the Monday 2024-01-08.
        final CairoConfiguration uncappedConfiguration = new DefaultCairoConfiguration(".") {
            @Override
            public int getSqlIntervalMaxIntervalsAfterMerge() {
                return 10_000_000;
            }
        };
        final LongList newYorkDay = new LongList();
        addLocalDays(newYorkDay, LocalDate.of(2024, 1, 1), LocalDate.of(2024, 1, 2), ZoneId.of("America/New_York"));
        final LongList utcDay = new LongList();
        addLocalDays(utcDay, LocalDate.of(2024, 1, 1), LocalDate.of(2024, 1, 2), ZoneOffset.UTC);
        final LongList mondayHours = new LongList();
        addLocalBlock(mondayHours, LocalDateTime.of(2024, 1, 8, 9, 0), ZoneOffset.ofHours(-5), 5 * Micros.HOUR_MICROS);
        assertTickExprIntervals(uncappedConfiguration, newYorkDay, intervals[0]);
        assertTickExprIntervals(uncappedConfiguration, newYorkDay, intervals[1]);
        assertTickExprIntervals(uncappedConfiguration, utcDay, intervals[2]);
        assertTickExprIntervals(uncappedConfiguration, mondayHours, intervals[3]);
    }

    @Test
    public void testZonedElementBehindCoverMergeCadence() throws SqlException {
        // A date list element or a day of a date variable range with a time zone, and a time list
        // value with bracketed suffixes and a time zone, merge their leaves in a merge range of their
        // own, since the zone converts only their intervals, so an earlier element that covers them
        // cannot shrink that range. The second element below holds 8 hours x 60 minutes x 10 seconds
        // x 100 = 480,000 leaves of 10 ms, which form 480 ten-second blocks, more than the merge
        // threshold (256). Its unions wait until the range grew by the threshold since the last
        // one: 1,875 of them, the last at the last leaf. Then the list, or the loop over the days of
        // the range, unions its intervals into the covering element, and the -ea assert of
        // parseTickExpr reads the cap. A union after every leaf, once the range holds 256 blocks,
        // would read the cap about 256,000 times.
        Assert.assertEquals(256, configuration.getSqlIntervalIncrementalMergeThreshold());
        final String suffix = ":[00..59]:[00..09].[0..9][0..9]";
        final LongList newYorkDay = new LongList();
        addLocalDays(newYorkDay, LocalDate.of(2024, 1, 1), LocalDate.of(2024, 1, 2), ZoneId.of("America/New_York"));
        final LongList utcDay = new LongList();
        addLocalDays(utcDay, LocalDate.of(2024, 1, 1), LocalDate.of(2024, 1, 2), ZoneOffset.UTC);
        // 09:00 to 16:59 at New York, which keeps -05:00 in January, on the Monday 2024-01-08
        final LongList mondayHours = new LongList();
        addLocalBlock(mondayHours, LocalDateTime.of(2024, 1, 8, 9, 0), ZoneOffset.ofHours(-5), 8 * Micros.HOUR_MICROS);
        assertCapReadCount(newYorkDay, 1_877, "[2024-01-01,2024-01-01T[09..16]" + suffix + "]@America/New_York");
        // the leaves of each hour sort before the blocks of the later hours
        assertCapReadCount(newYorkDay, 1_877, "[2024-01-01,2024-01-01T[16,15,14,13,12,11,10,09]" + suffix + "]@America/New_York");
        // the zone of the second element only, whose New York hours lie in the UTC day
        assertCapReadCount(utcDay, 1_877, "[2024-01-01,2024-01-01T[09..16]" + suffix + "@America/New_York]");
        // a day of a date variable range, $today is 2024-01-08, behind the eight hours that cover it
        assertCapReadCount(
                mondayHours,
                1_877,
                "[2024-01-08T09,2024-01-08T10,2024-01-08T11,2024-01-08T12,2024-01-08T13,2024-01-08T14,2024-01-08T15,2024-01-08T16,$today..$today]T[09..16]"
                        + suffix + "@America/New_York"
        );
        // Under a date-precise day filter, which keeps the Monday 2024-01-01, the 7 hours of 420,000
        // leaves take 1,640 unions by the threshold, and no union follows the 160 leaves after the
        // last one: the element filters them after the expansion.
        assertCapReadCount(newYorkDay, 1_642, "[2024-01-01,2024-01-01T[09..15]" + suffix + "#mon]@America/New_York");
        // A zoned time list value of 60 seconds x 10 = 600 slots of 10 ms that never touch: unions
        // at the leaves 256 and 512 and after the last leaf, then the time list unions the value
        // into the covering day, and the -ea assert reads the cap.
        assertCapReadCount(utcDay, 5, "[2024-01-01,2024-01-01T[09:00@+05:00]:[00..59].[0..9]0]");
    }

    // Appends the date variable in a random spelling, which the IR encodes as the variable: the name
    // in a random case, spaces around the operator and before the unit, a leading zero, an
    // underscore in an offset of 10 or more, bd in any case, and - for an offset of 0 half the time
    private static void appendDateVariableSpelling(Rnd rnd, StringSink sink, OffsetDateVariable variable) {
        final String name = variable.name();
        for (int k = 0, n = name.length(); k < n; k++) {
            final char c = name.charAt(k);
            sink.put(rnd.nextBoolean() ? Character.toUpperCase(c) : c);
        }
        final String unit = variable.unit();
        if (unit.isEmpty()) {
            return;
        }
        final int offset = variable.offset();
        appendRandomSpaces(rnd, sink);
        sink.put(offset < 0 || (offset == 0 && rnd.nextBoolean()) ? '-' : '+');
        appendRandomSpaces(rnd, sink);
        if (rnd.nextInt(4) == 0) {
            sink.put('0');
        }
        final int magnitude = Math.abs(offset);
        if (magnitude >= 10 && rnd.nextBoolean()) {
            sink.put(magnitude / 10).put('_').put(magnitude % 10);
        } else {
            sink.put(magnitude);
        }
        appendRandomSpaces(rnd, sink);
        if ("bd".equals(unit)) {
            sink.put(rnd.nextBoolean() ? 'b' : 'B').put(rnd.nextBoolean() ? 'd' : 'D');
        } else {
            sink.put(unit);
        }
    }

    // Appends a random date variable when isDateVariable is set, and otherwise a random date
    // variable, or a 2024 day, bracket range, unsorted bracket, month or, when
    // isZeroDurationDayAllowed is set, a day with a zero duration. A date variable takes a random
    // spelling, so that a list may repeat one in several. The months include those in which
    // America/Santiago (April, September) and Asia/Beirut (March, October) change their clocks at
    // local midnight.
    private static void appendRandomListElement(Rnd rnd, StringSink sink, boolean isDateVariable, boolean isZeroDurationDayAllowed) {
        final int kind = isDateVariable ? rnd.nextInt(4) : rnd.nextInt(isZeroDurationDayAllowed ? 9 : 8);
        switch (kind) {
            case 0 -> appendDateVariableSpelling(rnd, sink, new OffsetDateVariable("$today", "", 0));
            case 1 -> appendDateVariableSpelling(rnd, sink, new OffsetDateVariable("$yesterday", "", 0));
            case 2 -> appendDateVariableSpelling(rnd, sink, new OffsetDateVariable("$today", "d", -1 - rnd.nextInt(9)));
            case 3 -> appendDateVariableSpelling(rnd, sink, new OffsetDateVariable("$now", "h", -1 - rnd.nextInt(47)));
            default -> {
                final int[] months = {1, 3, 4, 9, 10, 12};
                sink.put("2024-").put(String.format("%02d", months[rnd.nextInt(months.length)]));
                switch (kind) {
                    case 4 -> sink.put(String.format("-%02d", 1 + rnd.nextInt(28)));
                    case 5 -> {
                        final int lo = 1 + rnd.nextInt(20);
                        sink.put(String.format("-[%02d..%02d]", lo, lo + rnd.nextInt(9)));
                    }
                    case 6 -> sink.put(String.format(
                            "-[%02d,%02d,%02d]",
                            1 + rnd.nextInt(28),
                            1 + rnd.nextInt(28),
                            1 + rnd.nextInt(28)
                    ));
                    case 7 -> {
                        // the whole month
                    }
                    default -> sink.put(String.format("-%02dT09:00;0h", 1 + rnd.nextInt(28)));
                }
            }
        }
    }

    // Appends 0 to 2 spaces
    private static void appendRandomSpaces(Rnd rnd, StringSink sink) {
        for (int k = rnd.nextInt(3); k > 0; k--) {
            sink.put(' ');
        }
    }

    private static int countMondays(LocalDate from, LocalDate to) {
        int mondayCount = 0;
        for (LocalDate day = from; !day.isAfter(to); day = day.plusDays(1)) {
            if (day.getDayOfWeek() == DayOfWeek.MONDAY) {
                mondayCount++;
            }
        }
        return mondayCount;
    }

    // The time list values MM:SS of the first count even seconds from 00:00: 00:00,00:02,...
    private static String evenSeconds(int count) {
        final StringBuilder values = new StringBuilder();
        for (int k = 0; k < count; k++) {
            final int second = 2 * k;
            values.append(k == 0 ? "" : ",").append(String.format("%02d:%02d", second / 60, second % 60));
        }
        return values.toString();
    }

    // The time list values HH:MM:SS of the first count even seconds of a day: 00:00:00,00:00:02,...
    private static String evenSecondsOfDay(int count) {
        final StringBuilder values = new StringBuilder();
        for (int k = 0; k < count; k++) {
            final int second = 2 * k;
            values.append(k == 0 ? "" : ",").append(String.format("%02d:%02d:%02d", second / 3_600, second / 60 % 60, second % 60));
        }
        return values.toString();
    }

    /**
     * Formats a timestamp as YYYY-MM-DD for use in expected results.
     */
    private static String formatDate(long timestamp, TimestampDriver driver) {
        int year = driver.getYear(timestamp);
        int month = driver.getMonthOfYear(timestamp);
        int day = driver.getDayOfMonth(timestamp);
        return String.format("%04d-%02d-%02d", year, month, day);
    }

    /**
     * Returns whether a generated expression has a range of business days, which ends at
     * {@link #FUZZ_BUSINESS_DAY_RANGE_END}. The parser reads the time part and the text after the
     * first ';' of the list for each day of a range, and such a range holds no day when it starts
     * on a weekend.
     */
    static boolean hasBusinessDayRange(String expression) {
        return expression.contains(".." + FUZZ_BUSINESS_DAY_RANGE_END);
    }

    /**
     * Returns whether a generated expression has an element with a timezone of
     * {@link #FUZZ_MALFORMED_ELEMENT_ZONES}, which no other part of a generated expression holds.
     */
    static boolean hasMalformedElementZone(String expression) {
        for (String zone : FUZZ_MALFORMED_ELEMENT_ZONES) {
            if (expression.contains(zone)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns whether a generated expression has a day filter after the first ';', which no
     * generated duration holds, so the day filter follows the duration.
     */
    static boolean hasMisplacedDayFilter(String expression) {
        final int semicolon = expression.indexOf(';');
        return semicolon >= 0 && expression.indexOf('#', semicolon) >= 0;
    }

    /**
     * Returns whether the text after the first ';' of a generated expression, up to a day filter
     * after it, is one of {@link #FUZZ_REJECTED_SUFFIXES}: no other part of a generated expression
     * holds a ';'.
     */
    static boolean hasRejectedSuffix(String expression) {
        final int semicolon = expression.indexOf(';');
        if (semicolon < 0) {
            return false;
        }
        final int dayFilter = expression.indexOf('#', semicolon);
        final String suffix = expression.substring(semicolon, dayFilter < 0 ? expression.length() : dayFilter);
        for (String rejected : FUZZ_REJECTED_SUFFIXES) {
            if (rejected.equals(suffix)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns whether a generated date list has a static element with a time part of its own that
     * starts with a bracket, such as {@code 2024-01T[09:00]}, and a single time after the list, such
     * as {@code T09:00}.
     */
    private static boolean hasStaticElementBracketedTimeUnderListTime(String expression) {
        final String list = expression.trim();
        if (list.isEmpty() || list.charAt(0) != '[') {
            return false;
        }
        boolean hasBracketedTime = false;
        int depth = 0;
        int elementStart = 1;
        for (int i = 1, n = list.length(); i < n; i++) {
            final char c = list.charAt(i);
            if (c == '[') {
                depth++;
            } else if (c == ']' && depth > 0) {
                depth--;
            } else if (depth == 0 && (c == ',' || c == ']')) {
                final String element = list.substring(elementStart, i).trim();
                hasBracketedTime |= !element.startsWith("$") && element.contains("T[");
                if (c == ']') {
                    return hasBracketedTime && i + 2 < n && list.charAt(i + 1) == 'T' && Character.isDigit(list.charAt(i + 2));
                }
                elementStart = i + 1;
            }
        }
        return false;
    }

    /**
     * Returns whether a generated expression ends with a duration of
     * {@link #FUZZ_UNKNOWN_UNIT_DURATIONS}, as each one does that holds such a duration and is not
     * malformed.
     */
    static boolean hasUnknownDurationUnit(String expression) {
        for (String duration : FUZZ_UNKNOWN_UNIT_DURATIONS) {
            if (expression.endsWith(duration)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the minute at the time of the day, an interval at microsecond precision, after a
     * comma.
     */
    /**
     * Returns a configuration with the given number of intervals after which a date range merges each
     * day's intervals into the ones so far, and the given limit on the merged intervals.
     */
    private static CairoConfiguration intervalLimitConfiguration(int mergeThreshold, int maxIntervals) {
        return new DefaultCairoConfiguration(".") {
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

    private static String minute(String date, String time) {
        return ",{lo=" + date + 'T' + time + ":00.000000Z, hi=" + date + 'T' + time + ":59.999999Z}";
    }

    // The time list values HH:MM of count minutes of a day stepMinutes apart from 00:00
    private static String minutesOfDay(int count, int stepMinutes) {
        final StringBuilder values = new StringBuilder();
        for (int k = 0; k < count; k++) {
            final int minute = k * stepMinutes;
            values.append(k == 0 ? "" : ",").append(String.format("%02d:%02d", minute / 60, minute % 60));
        }
        return values.toString();
    }

    // The static equivalent of todayMinusDays(count, stepDays) at the Monday 2024-01-08
    private static String mondayMinusDays(int count, int stepDays) {
        final StringBuilder values = new StringBuilder();
        for (int k = 0; k < count; k++) {
            values.append(k == 0 ? "" : ",").append(LocalDate.of(2024, 1, 8).minusDays((long) k * stepDays));
        }
        return values.toString();
    }

    // The values prefix + NN + suffix for NN from 00 to count - 1, separated by commas
    private static String numberedValues(String prefix, int count, String suffix) {
        final StringBuilder values = new StringBuilder();
        for (int k = 0; k < count; k++) {
            values.append(k == 0 ? "" : ",").append(prefix).append(String.format("%02d", k)).append(suffix);
        }
        return values.toString();
    }

    private static DateVariableExpr parseDateVar(CharSequence seq, int hi) throws SqlException {
        long encoded = DateVariableExpr.parseEncoded(seq, 0, hi, 0);
        return new DateVariableExpr(
                (byte) ((encoded >>> 58) & 0x3),
                (char) ((encoded >>> 40) & 0xFFFF),
                (int) encoded,
                (encoded & (1L << 57)) != 0
        );
    }

    private static String parseWithChecks(TimestampDriver timestampDriver, String expression, long now, int checks) {
        return parseWithChecks(timestampDriver, configuration, expression, now, checks);
    }

    private static String parseWithChecks(TimestampDriver timestampDriver, CairoConfiguration configuration, String expression, long now, int checks) {
        final LongList intervals = new LongList();
        try {
            IntervalUtils.parseTickExpr(timestampDriver, configuration, expression, 0, expression.length(), 0, intervals, new StringSink(), now, checks);
            return intervalToString(timestampDriver, intervals).toString();
        } catch (SqlException e) {
            return "error: " + e.getFlyweightMessage();
        }
    }

    private static String pick(Rnd rnd, String[] values) {
        return values[rnd.nextInt(values.length)];
    }

    /**
     * Adds, for each day from first to last that isIncluded accepts, or for every day when it is
     * null, the intervals [day + bounds[0], day + bounds[1]], [day + bounds[2], day + bounds[3]] and
     * so on, in the units of the timestamp type.
     */
    private void addDayIntervals(LongList out, LocalDate first, LocalDate last, Predicate<LocalDate> isIncluded, long... bounds) {
        final TimestampDriver driver = timestampType.getDriver();
        for (LocalDate day = first; !day.isAfter(last); day = day.plusDays(1)) {
            if (isIncluded == null || isIncluded.test(day)) {
                final long dayStart = driver.fromDays((int) day.toEpochDay());
                for (int i = 0; i < bounds.length; i += 2) {
                    out.add(dayStart + bounds[i], dayStart + bounds[i + 1]);
                }
            }
        }
    }

    // n copies of the value, separated by commas
    private static String repeatedValues(String value, int n) {
        final StringBuilder values = new StringBuilder();
        for (int k = 0; k < n; k++) {
            values.append(k == 0 ? "" : ",").append(value);
        }
        return values.toString();
    }

    // The count date variables $today-0d,$today-{stepDays}d,..., stepDays days apart
    private static String todayMinusDays(int count, int stepDays) {
        final StringBuilder values = new StringBuilder();
        for (int k = 0; k < count; k++) {
            values.append(k == 0 ? "" : ",").append("$today-").append((long) k * stepDays).append('d');
        }
        return values.toString();
    }

    private static LocalDate toLocalDate(TimestampDriver timestampDriver, long timestamp) {
        return LocalDate.ofEpochDay(Math.floorDiv(timestampDriver.toMicros(timestamp), Micros.DAY_MICROS));
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

    // Adds one whole-day interval at the offset for each run of consecutive days from first to last
    // whose weekday is one of the days: one day each for Mondays, Monday to Friday for workdays
    private void addDayRuns(LongList intervals, LocalDate first, LocalDate last, Set<DayOfWeek> days, ZoneOffset offset) {
        LocalDate runStart = null;
        for (LocalDate day = first; !day.isAfter(last); day = day.plusDays(1)) {
            if (days.contains(day.getDayOfWeek())) {
                if (runStart == null) {
                    runStart = day;
                }
            } else if (runStart != null) {
                addLocalDays(intervals, runStart, day, offset);
                runStart = null;
            }
        }
        if (runStart != null) {
            addLocalDays(intervals, runStart, last.plusDays(1), offset);
        }
    }

    // Adds one interval of lengthMicros from the local time at the offset
    private void addLocalBlock(LongList intervals, LocalDateTime lo, ZoneOffset offset, long lengthMicros) {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final long loMicros = lo.toEpochSecond(offset) * Micros.SECOND_MICROS;
        intervals.add(timestampDriver.fromMicros(loMicros));
        intervals.add(timestampDriver.fromMicros(loMicros + lengthMicros) - 1);
    }

    // Adds the local days [from, to) in the zone as one interval
    private void addLocalDays(LongList intervals, LocalDate from, LocalDate to, ZoneId zone) {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        intervals.add(timestampDriver.fromMicros(from.atStartOfDay(zone).toEpochSecond() * Micros.SECOND_MICROS));
        intervals.add(timestampDriver.fromMicros(to.atStartOfDay(zone).toEpochSecond() * Micros.SECOND_MICROS) - 1);
    }

    // Adds one interval of a minute at UTC for each of count minutes of the day stepMinutes apart
    // from 00:00
    private void addMinuteBlocks(LongList intervals, LocalDate day, int count, int stepMinutes) {
        for (int k = 0; k < count; k++) {
            addLocalBlock(intervals, day.atStartOfDay().plusMinutes((long) k * stepMinutes), ZoneOffset.UTC, Micros.MINUTE_MICROS);
        }
    }

    // Adds the 60 x tenthCount slots that the suffix :[00..59].[0..9]0, with tenths up to
    // tenthCount - 1, gives a local minute at the offset: for each second s and tenth d, the 10 ms
    // from s.d00.
    private void addMinuteSlots(LongList intervals, LocalDateTime minute, ZoneOffset offset, int tenthCount) {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final long minuteMicros = minute.toEpochSecond(offset) * Micros.SECOND_MICROS;
        for (int second = 0; second < 60; second++) {
            for (int tenth = 0; tenth < tenthCount; tenth++) {
                final long loMicros = minuteMicros + second * Micros.SECOND_MICROS + tenth * 100 * Micros.MILLI_MICROS;
                intervals.add(timestampDriver.fromMicros(loMicros));
                intervals.add(timestampDriver.fromMicros(loMicros + 10 * Micros.MILLI_MICROS) - 1);
            }
        }
    }

    // Adds one interval of lengthMicros from the local time of day at the offset on each Monday to
    // Friday among days 1-28 of each month of the years firstYear to lastYear
    private void addWorkdayBlocks(LongList intervals, int firstYear, int lastYear, LocalTime time, ZoneOffset offset, long lengthMicros) {
        for (LocalDate day = LocalDate.of(firstYear, 1, 1); day.getYear() <= lastYear; day = day.plusDays(1)) {
            if (day.getDayOfMonth() <= 28 && day.getDayOfWeek().getValue() <= DayOfWeek.FRIDAY.getValue()) {
                addLocalBlock(intervals, day.atTime(time), offset, lengthMicros);
            }
        }
    }

    // Adds the local day in the zone as one interval: the instants whose local time falls on the day,
    // as the time zone conversion of tick expressions takes it, from the start of the day to the start
    // of the next day, so a day covers both occurrences of a repeated hour of a clock change that
    // follows it.
    private void addZonedLocalDay(LongList intervals, LocalDate day, ZoneId zone) {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        intervals.add(timestampDriver.fromMicros(day.atStartOfDay(zone).toEpochSecond() * Micros.SECOND_MICROS));
        intervals.add(timestampDriver.fromMicros(day.plusDays(1).atStartOfDay(zone).toEpochSecond() * Micros.SECOND_MICROS) - 1);
    }

    private void assertBracketInterval(String expected, String interval) throws SqlException {
        out.clear();
        final TimestampDriver timestampDriver = timestampType.getDriver();
        parseTickExpr(timestampDriver, interval, 0, interval.length(), 0, out);
        TestUtils.assertEquals(
                ColumnType.isTimestampNano(timestampType.getTimestampType())
                        ? expected.replace("00Z", "00000Z").replace("99Z", "99999Z")
                        : expected,
                intervalToString(timestampDriver, out)
        );
    }

    private void assertBracketIntervalError(String interval, String expectedError) {
        try {
            final TimestampDriver timestampDriver = timestampType.getDriver();
            out.clear();
            parseTickExpr(timestampDriver, interval, 0, interval.length(), 0, out);
            Assert.fail("Expected SqlException with message containing: " + expectedError);
        } catch (SqlException e) {
            Assert.assertTrue("Expected error message to contain '" + expectedError + "' but got: " + e.getMessage(),
                    e.getMessage().contains(expectedError));
        }
    }

    private void assertBracketIntervalWithNow(String expected, String interval, String nowTimestamp) throws SqlException {
        out.clear();
        final TimestampDriver timestampDriver = timestampType.getDriver();
        long now = timestampDriver.parseFloorLiteral(nowTimestamp);
        parseTickExprWithNow(timestampDriver, interval, 0, interval.length(), 0, out, now);
        TestUtils.assertEquals(
                ColumnType.isTimestampNano(timestampType.getTimestampType())
                        ? expected.replace("00Z", "00000Z").replace("99Z", "99999Z")
                        : expected,
                intervalToString(timestampDriver, out)
        );
    }

    /**
     * Asserts that the compiled form rejects the expression as it compiles, with exactly the error,
     * at the position of the expression.
     */
    private void assertCompileError(String expression, String expectedError) {
        try {
            IntervalUtils.compileTickExpr(timestampType.getDriver(), configuration, expression, 0, expression.length(), 7).close();
            Assert.fail("Should throw SqlException for: " + expression);
        } catch (SqlException e) {
            TestUtils.assertEquals(expression, expectedError, e.getFlyweightMessage());
            Assert.assertEquals(expression, 7, e.getPosition());
        }
    }

    // Expects the number of reads of the cap, one per incremental union, one per check of the
    // final interval count and one in the -ea assert of parseTickExpr, and then the intervals and
    // the peak list size of assertTimeListGrowthBounded
    private void assertCapReadCount(LongList expected, int expectedCapReadCount, String interval) throws SqlException {
        Assert.assertEquals(interval, expectedCapReadCount, countMaxIntervalsGetterCalls(interval));
        assertTimeListGrowthBounded(expected, 0, interval);
    }

    private void assertCompileTickExprError(String expression, String expectedError) {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        try {
            IntervalUtils.compileTickExpr(timestampDriver, configuration, expression, 0, expression.length(), 0).close();
            Assert.fail("Should throw SqlException for: " + expression);
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), expectedError);
        }
    }

    // Expects compileTickExpr() to fail with the cap error at position 0, and parseTickExpr() to fail
    // with it on the static equivalent, in which the Monday 2024-01-08 stands for $today
    private void assertCompiledAndParsedOverMaxIntervals(String expression) {
        assertCompiledOverMaxIntervals(configuration, expression);
        assertTickExprErrorAtMonday(expression.replace("$today", "2024-01-08"), "Bracket expansion produces too many intervals (max 1024)");
    }

    // Compiles the expression, evaluates it at mondayNow() and expects one full-day interval for
    // every Monday from first to last, as java.time lists them.
    private void assertCompiledFullDayMondays(CairoConfiguration cfg, String expression, LocalDate first, LocalDate last) throws SqlException {
        final LongList expected = new LongList();
        for (LocalDate day = first; !day.isAfter(last); day = day.plusDays(1)) {
            if (day.getDayOfWeek() == DayOfWeek.MONDAY) {
                addLocalDays(expected, day, day.plusDays(1), ZoneOffset.UTC);
            }
        }
        final TimestampDriver timestampDriver = timestampType.getDriver();
        out.clear();
        try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(timestampDriver, cfg, expression, 0, expression.length(), 0)) {
            compiled.evaluate(out, mondayNow());
        }
        TestUtils.assertEquals(intervalToString(timestampDriver, expected).toString(), intervalToString(timestampDriver, out));
    }

    // Expects compileTickExpr() to fail with the cap error of the configuration at position 0
    private void assertCompiledOverMaxIntervals(CairoConfiguration cfg, String expression) {
        final int maxIntervals = cfg.getSqlIntervalMaxIntervalsAfterMerge();
        try {
            IntervalUtils.compileTickExpr(timestampType.getDriver(), cfg, expression, 0, expression.length(), 0).close();
            Assert.fail("Expected the cap error for " + expression);
        } catch (SqlException e) {
            Assert.assertEquals(expression, 0, e.getPosition());
            TestUtils.assertEquals("Bracket expansion produces too many intervals (max " + maxIntervals + ')', e.getFlyweightMessage());
        }
    }

    /**
     * Asserts that the compiled form validates the expression and evaluates it to the intervals,
     * from its IR when it has one, or by parsing it on every evaluation.
     */
    private void assertCompiledEvaluatesTo(String expected, String expression, long now, boolean hasIr) throws SqlException {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(
                timestampDriver, configuration, expression, 0, expression.length(), 7)) {
            Assert.assertEquals(expression, !hasIr, compiled.isParsedOnEvaluation());
            compiled.validate(now);
            out.clear();
            compiled.evaluate(out, now);
            TestUtils.assertEquals(expression, expected, intervalToString(timestampDriver, out));
        }
    }

    /**
     * Asserts that the parser, which SQL uses for a bind variable, and the compiled form, which
     * keeps its IR, evaluate the expression to the single interval [lo, hi], which is given at the
     * precision of the timestamp type.
     */
    private void assertCompiledIrWithNow(String expression, String nowTimestamp, String lo, String hi) throws SqlException {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final long now = timestampDriver.parseFloorLiteral(nowTimestamp);
        final String expected = "[{lo=" + lo + ", hi=" + hi + "}]";
        out.clear();
        parseTickExprWithNow(timestampDriver, expression, 0, expression.length(), 0, out, now);
        final String parsed = intervalToString(timestampDriver, out).toString();
        out.clear();
        try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(
                timestampDriver, configuration, expression, 0, expression.length(), 0)) {
            compiled.evaluate(out, now);
            Assert.assertFalse(expression, compiled.isParsedOnEvaluation());
        }
        // one assertion over both paths, so a failure shows the result of each
        TestUtils.assertEquals(
                expression,
                "parsed: " + expected + "\ncompiled: " + expected,
                "parsed: " + parsed + "\ncompiled: " + intervalToString(timestampDriver, out)
        );
    }

    /**
     * Asserts that an expression containing date variables:
     * 1. Produces a dynamic model when going through RuntimeIntervalModelBuilder
     * 2. Produces the same intervals via CompiledTickExpression as the static parseTickExpr path
     */
    private void assertCompiledTickExpr(String expression) throws SqlException {
        // Verify the builder produces a dynamic model
        RuntimeIntervalModelBuilder builder = new RuntimeIntervalModelBuilder();
        builder.of(timestampType.getTimestampType(), 0, configuration);
        builder.intersectIntervals(expression, 0, expression.length(), 0);
        try (RuntimeIntrinsicIntervalModel m = builder.build()) {
            Assert.assertFalse("Expression should produce dynamic model: " + expression, m.isStatic());
        }

        // Verify dynamic path produces same results as static path
        // across multiple random "now" values to catch alignment-dependent bugs
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final Rnd rnd = TestUtils.generateRandom(LOG);

        long minTimestamp = timestampDriver.parseFloorLiteral("2020-01-01T00:00:00.000000Z");
        long maxTimestamp = timestampDriver.parseFloorLiteral("2030-12-31T23:59:59.999999Z");
        long range = maxTimestamp - minTimestamp;

        try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(
                timestampDriver, configuration, expression, 0, expression.length(), 0)) {
            LongList dynamicResult = new LongList();
            LongList staticResult = new LongList();

            for (int trial = 0; trial < 10; trial++) {
                long randomNow = minTimestamp + Math.abs(rnd.nextLong() % range);
                randomNow = timestampDriver.startOfDay(randomNow, 0);
                randomNow += timestampDriver.fromHours(rnd.nextInt(24));
                randomNow += timestampDriver.fromMinutes(rnd.nextInt(60));

                dynamicResult.clear();
                compiled.evaluate(dynamicResult, randomNow);

                staticResult.clear();
                parseTickExprWithNow(timestampDriver, expression, 0, expression.length(), 0,
                        staticResult, randomNow);

                TestUtils.assertEquals(
                        intervalToString(timestampDriver, staticResult).toString(),
                        intervalToString(timestampDriver, dynamicResult)
                );
            }
        }
    }

    /**
     * Asserts that the parse path and the compiled path agree for each expression at every
     * 5 minutes of the 24 hours from {@code from}.
     */
    private void assertCompiledTickExprEvery5Minutes(String from, String... expressions) throws SqlException {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final long start = timestampDriver.parseFloorLiteral(from);
        final LongList parsed = new LongList();
        final LongList compiled = new LongList();
        for (String expression : expressions) {
            try (CompiledTickExpression compiledExpr = IntervalUtils.compileTickExpr(
                    timestampDriver, configuration, expression, 0, expression.length(), 0)) {
                for (int i = 0; i <= 288; i++) {
                    final long now = start + timestampDriver.fromMinutes(5 * i);
                    parsed.clear();
                    parseTickExprWithNow(timestampDriver, expression, 0, expression.length(), 0, parsed, now);
                    compiled.clear();
                    compiledExpr.evaluate(compiled, now);
                    sink.clear();
                    sink.put(expression).put(" at now=");
                    timestampDriver.append(sink, now);
                    final String message = sink.toString();
                    // intervalToString() reuses one sink, so take a copy of the parsed text first
                    final String parsedText = intervalToString(timestampDriver, parsed).toString();
                    TestUtils.assertEquals(message, parsedText, intervalToString(timestampDriver, compiled));
                }
            }
        }
    }

    // Evaluates the expression in 2026, outside the static elements of the tests, where each element
    // yields one interval, and then runs assertCompiledTickExpr().
    private void assertCompiledTickExprElementCount(String expression, int expectedElementCount) throws SqlException {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(
                timestampDriver, configuration, expression, 0, expression.length(), 0)) {
            final LongList intervals = new LongList();
            compiled.evaluate(intervals, timestampDriver.parseFloorLiteral("2026-06-01T01:00:00.000000Z"));
            Assert.assertEquals(expression, 2 * expectedElementCount, intervals.size());
        }
        assertCompiledTickExpr(expression);
    }

    // Compiles the expression, expects the number of its IR elements and the number of intervals
    // that it yields at most, and evaluates it at mondayNow()
    private void assertCompiledTickExprIntervals(
            String expression,
            int expectedElementCount,
            int expectedMaxIntervalCount,
            LongList expected
    ) throws SqlException {
        assertCompiledTickExprIntervals(configuration, expression, expectedElementCount, expectedMaxIntervalCount, expected);
    }

    private void assertCompiledTickExprIntervals(
            CairoConfiguration cfg,
            String expression,
            int expectedElementCount,
            int expectedMaxIntervalCount,
            LongList expected
    ) throws SqlException {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        out.clear();
        try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(timestampDriver, cfg, expression, 0, expression.length(), 0)) {
            Assert.assertEquals(expression, expectedElementCount, compiled.getElementCount());
            Assert.assertEquals(expression, expectedMaxIntervalCount, compiled.getMaxIntervalCount(Integer.MAX_VALUE));
            compiled.evaluate(out, mondayNow());
        }
        Assert.assertEquals(expression, intervalToString(timestampDriver, expected).toString(), intervalToString(timestampDriver, out).toString());
    }

    private void assertCompiledTickExprWithNow(String expression, String nowStr) throws SqlException {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        long now = timestampDriver.parseFloorLiteral(nowStr);

        try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(
                timestampDriver, configuration, expression, 0, expression.length(), 0)) {
            LongList dynamicResult = new LongList();
            LongList staticResult = new LongList();

            compiled.evaluate(dynamicResult, now);
            parseTickExprWithNow(timestampDriver, expression, 0, expression.length(), 0,
                    staticResult, now);

            TestUtils.assertEquals(
                    intervalToString(timestampDriver, staticResult).toString(),
                    intervalToString(timestampDriver, dynamicResult)
            );
        }
    }

    private void assertDateFloor(String expected, String value) throws NumericException {
        sink.clear();
        final TimestampDriver timestampDriver = timestampType.getDriver();
        long t = timestampDriver.parseFloorLiteral(value);
        timestampDriver.append(sink, t);
        TestUtils.assertEquals(
                ColumnType.isTimestampNano(timestampType.getTimestampType())
                        ? expected.replace("Z", "000Z").replace("999999Z", "999999999Z")
                        : expected,
                sink
        );
    }

    /**
     * Asserts that the compiled form of withinLimit evaluates to [day + lo, day + hi] for each day
     * from firstDay to lastDay, after the intervals that the list holds before it, and that
     * overLimit fails with the error at the position of the expression, as the parser does, and
     * leaves the list as it found it, every time it evaluates, and as it validates.
     */
    private void assertDateRangeLimit(
            CairoConfiguration configuration,
            String withinLimit,
            String overLimit,
            long now,
            LocalDate firstDay,
            LocalDate lastDay,
            long lo,
            long hi,
            String expectedError
    ) throws SqlException {
        final TimestampDriver driver = timestampType.getDriver();
        // the intervals of another expression, which the list holds before these
        final LongList head = new LongList();
        head.add(1L, 2L);
        head.add(5L, 6L);
        final String headText = intervalToString(driver, head).toString();
        final LongList expected = new LongList();
        expected.addAll(head);
        addDayIntervals(expected, firstDay, lastDay, null, lo, hi);
        final String expectedText = intervalToString(driver, expected).toString();
        try (
                CompiledTickExpression within = IntervalUtils.compileTickExpr(driver, configuration, withinLimit, 0, withinLimit.length(), 7);
                CompiledTickExpression over = IntervalUtils.compileTickExpr(driver, configuration, overLimit, 0, overLimit.length(), 7)
        ) {
            Assert.assertFalse(withinLimit, within.isParsedOnEvaluation());
            Assert.assertFalse(overLimit, over.isParsedOnEvaluation());
            for (int i = 0; i < 2; i++) {
                out.clear();
                out.addAll(head);
                within.evaluate(out, now);
                TestUtils.assertEquals(withinLimit, expectedText, intervalToString(driver, out));
                out.clear();
                out.addAll(head);
                try {
                    over.evaluate(out, now);
                    Assert.fail(overLimit);
                } catch (CairoException e) {
                    TestUtils.assertEquals(overLimit, expectedError, e.getFlyweightMessage());
                    Assert.assertEquals(overLimit, 7, e.getPosition());
                }
                TestUtils.assertEquals(overLimit, headText, intervalToString(driver, out));
            }
            within.validate(now);
            try {
                over.validate(now);
                Assert.fail(overLimit);
            } catch (SqlException e) {
                TestUtils.assertEquals(overLimit, expectedError, e.getFlyweightMessage());
                Assert.assertEquals(overLimit, 7, e.getPosition());
            }
        }
        // the parser, which a bind variable takes
        out.clear();
        try {
            IntervalUtils.parseTickExpr(driver, configuration, overLimit, 0, overLimit.length(), 7, out, new StringSink(), now);
            Assert.fail(overLimit);
        } catch (SqlException e) {
            TestUtils.assertEquals(overLimit, expectedError, e.getFlyweightMessage());
            Assert.assertEquals(overLimit, 7, e.getPosition());
        }
    }

    /**
     * Asserts that the compiled form of the expression evaluates through its IR to the expected
     * intervals, twice, and that the merges after the days of its date ranges count the expected
     * number of intervals each time, see CompiledTickExpression#getRangeMergeIntervalCountForTesting.
     */
    private void assertDateRangeMergeCount(
            CairoConfiguration configuration,
            String expression,
            long now,
            LongList expected,
            long expectedMergeCount
    ) throws SqlException {
        try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(timestampType.getDriver(), configuration, expression, 0, expression.length(), 0)) {
            assertDateRangeMergeCount(compiled, expression, now, expected, expectedMergeCount);
        }
    }

    private void assertDateRangeMergeCount(CompiledTickExpression compiled, String expression, long now, LongList expected, long expectedMergeCount) {
        final TimestampDriver driver = timestampType.getDriver();
        Assert.assertFalse(expression, compiled.isParsedOnEvaluation());
        final String expectedText = intervalToString(driver, expected).toString();
        for (int i = 0; i < 2; i++) {
            final long countBefore = compiled.getRangeMergeIntervalCountForTesting();
            out.clear();
            compiled.evaluate(out, now);
            final long mergeCount = compiled.getRangeMergeIntervalCountForTesting() - countBefore;
            TestUtils.assertEquals(expression, expectedText, intervalToString(driver, out));
            Assert.assertEquals(expression, expectedMergeCount, mergeCount);
        }
    }

    /**
     * Asserts date variable interval with a randomized "now" timestamp.
     * The expectedBuilder function receives the random "now" and TimestampDriver to build the expected result.
     */
    private void assertDateVariableInterval(
            DateVariableExpectedBuilder expectedBuilder,
            String interval
    ) throws SqlException {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final Rnd rnd = TestUtils.generateRandom(LOG);

        // Generate random "now" between 2020-01-01 and 2030-12-31
        // Use a weekday (Mon-Fri) to make business day tests predictable
        long minTimestamp = timestampDriver.parseFloorLiteral("2020-01-01T00:00:00.000000Z");
        long maxTimestamp = timestampDriver.parseFloorLiteral("2030-12-31T23:59:59.999999Z");
        long range = maxTimestamp - minTimestamp;
        long randomNow = minTimestamp + Math.abs(rnd.nextLong() % range);

        // Add random time component (0-23 hours, 0-59 minutes)
        randomNow = timestampDriver.startOfDay(randomNow, 0);
        randomNow += timestampDriver.fromHours(rnd.nextInt(24));
        randomNow += timestampDriver.fromMinutes(rnd.nextInt(60));

        out.clear();
        parseTickExprWithNow(timestampDriver, interval, 0, interval.length(), 0, out, randomNow);

        String expected = expectedBuilder.build(randomNow, timestampDriver, timestampType);
        TestUtils.assertEquals(
                ColumnType.isTimestampNano(timestampType.getTimestampType())
                        ? expected.replace("00Z", "00000Z").replace("99Z", "99999Z")
                        : expected,
                intervalToString(timestampDriver, out)
        );
    }

    // Parses the interval and expects one interval for the first minute of each hour from 0 to
    // hourCount - 1 on days 1 to dayCount of months 1 to monthCount of 2024, as java.time
    // computes them.
    private void assertFirstMinuteOfEachHour(
            CairoConfiguration cfg,
            String interval,
            int monthCount,
            int dayCount,
            int hourCount
    ) throws SqlException {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final LongList expected = new LongList();
        for (int month = 1; month <= monthCount; month++) {
            for (int day = 1; day <= dayCount; day++) {
                for (int hour = 0; hour < hourCount; hour++) {
                    final long loMicros = LocalDate.of(2024, month, day).atTime(hour, 0).toEpochSecond(ZoneOffset.UTC) * Micros.SECOND_MICROS;
                    expected.add(timestampDriver.fromMicros(loMicros));
                    expected.add(timestampDriver.fromMicros(loMicros + Micros.MINUTE_MICROS) - 1);
                }
            }
        }
        out.clear();
        parseTickExprAtMonday(cfg, out, interval);
        TestUtils.assertEquals(intervalToString(timestampDriver, expected).toString(), intervalToString(timestampDriver, out));
    }

    private void assertFullDayMondays(LongList intervals, int expectedCount) {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        Assert.assertEquals(expectedCount, intervals.size() / 2);
        for (int i = 0, n = intervals.size(); i < n; i += 2) {
            final long lo = intervals.getQuick(i);
            Assert.assertEquals(timestampDriver.startOfDay(lo, 0), lo);
            Assert.assertEquals(timestampDriver.endOfDay(lo), intervals.getQuick(i + 1));
            Assert.assertEquals(1, timestampDriver.getDayOfWeek(lo));
        }
    }

    /**
     * Asserts that the parser, which SQL uses for a bind variable, and the compiled form, which SQL
     * uses for a literal, fail on the expression at the given "now" with exactly the error, at the
     * position of the expression. The compiled form compiles, fails as the SQL that holds it is
     * validated, and fails when it evaluates, where it keeps what the caller appended before. It
     * parses the expression when it evaluates.
     */
    private void assertFailsLikeBindVariable(String expression, long now, String expectedError) throws SqlException {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        sink.clear();
        sink.put(expression).put(" at ");
        timestampDriver.append(sink, now);
        final String context = sink.toString();
        out.clear();
        try {
            parseTickExprWithNow(timestampDriver, expression, 0, expression.length(), 7, out, now);
            Assert.fail("parse should fail: " + context);
        } catch (SqlException e) {
            TestUtils.assertEquals(context, expectedError, e.getFlyweightMessage());
            Assert.assertEquals(context, 7, e.getPosition());
        }
        try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(
                timestampDriver, configuration, expression, 0, expression.length(), 7)) {
            try {
                compiled.validate(now);
                Assert.fail("validate should fail: " + context);
            } catch (SqlException e) {
                TestUtils.assertEquals(context, expectedError, e.getFlyweightMessage());
                Assert.assertEquals(context, 7, e.getPosition());
            }
            out.clear();
            out.add(1L, 2L);
            try {
                compiled.evaluate(out, now);
                Assert.fail("evaluation should fail: " + context);
            } catch (CairoException e) {
                TestUtils.assertEquals(context, expectedError, e.getFlyweightMessage());
                Assert.assertEquals(context, 7, e.getPosition());
            }
            Assert.assertEquals(context, 2, out.size());
            Assert.assertEquals(context, 1, out.getQuick(0));
            Assert.assertEquals(context, 2, out.getQuick(1));
            Assert.assertTrue(context, compiled.isParsedOnEvaluation());
        }
    }

    /**
     * Asserts that an imprecise tick expression produces the same intervals
     * as its explicit day-range equivalent.
     */
    private void assertImpreciseEquivalence(String imprecise, String explicit) throws SqlException {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        out.clear();
        parseTickExpr(timestampDriver, imprecise, 0, imprecise.length(), 0, out);
        String impreciseResult = intervalToString(timestampDriver, out).toString();
        int impreciseCount = out.size() / 2;
        out.clear();
        parseTickExpr(timestampDriver, explicit, 0, explicit.length(), 0, out);
        String explicitResult = intervalToString(timestampDriver, out).toString();
        int explicitCount = out.size() / 2;
        Assert.assertTrue("explicit expansion should produce intervals", explicitCount > 0);
        Assert.assertEquals("interval count mismatch", explicitCount, impreciseCount);
        TestUtils.assertEquals(explicitResult, impreciseResult);
    }

    private void assertIntersect(String expected) {
        out.add(a);
        out.add(b);
        IntervalUtils.intersectInPlace(out, a.size());
        TestUtils.assertEquals(
                ColumnType.isTimestampNano(timestampType.getTimestampType()) ?
                        expected.replace("000000Z", "000000000Z").replace("999999Z", "999999999Z")
                        : expected,
                intervalToString(timestampType.getDriver(), out)
        );
    }

    private void assertIntervalError(String interval) {
        try {
            final TimestampDriver timestampDriver = timestampType.getDriver();
            parseTickExpr(timestampDriver, interval, 0, interval.length(), 0, out);
            Assert.fail();
        } catch (SqlException ignore) {
        }
    }

    /**
     * Asserts that the interval model rejects the expression when the SQL compiles, with the
     * position of the expression.
     */
    private void assertIntervalModelError(String expression, String expectedError) {
        final RuntimeIntervalModelBuilder builder = new RuntimeIntervalModelBuilder();
        builder.of(timestampType.getTimestampType(), 0, configuration);
        try {
            builder.intersectIntervals(expression, 0, expression.length(), 42);
            Assert.fail("Should throw SqlException at compile time: " + expression);
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), expectedError);
            Assert.assertEquals(42, e.getPosition());
        }
    }

    /**
     * Asserts that the expression, a date list with the range as its first element, adds no
     * interval at the "now" of the range, at every check level that lets the range through: one
     * that starts after it ends fails on its order under the range order check, at its '..'. The
     * compiled form validates it, and evaluates it to the same.
     */
    private void assertNoIntervalRangeAddsNoInterval(NoIntervalRange range, String expression) throws SqlException {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final long now = timestampDriver.parseFloorLiteral(range.now());
        final int rangeOrderPosition = 7 + expression.indexOf("..") - 1;
        for (int checks : TICK_CHECK_LEVELS) {
            if (range.isReversed() && (checks & IntervalUtils.TICK_CHECK_RANGE_ORDER) != 0) {
                assertParseFails(expression, now, checks, rangeOrderPosition, RANGE_ORDER_ERROR);
            } else {
                assertParsed("[]", expression, now, checks);
            }
        }
        final String context = expression + " at " + range.now();
        try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(
                timestampDriver, configuration, expression, 0, expression.length(), 7)) {
            compiled.validate(now);
            out.clear();
            try {
                compiled.evaluate(out, now);
                Assert.assertFalse(context, range.isReversed());
                Assert.assertEquals(context, 0, out.size());
            } catch (CairoException e) {
                Assert.assertTrue(context, range.isReversed());
                TestUtils.assertEquals(context, RANGE_ORDER_ERROR, e.getFlyweightMessage());
            }
        }
    }

    /**
     * Asserts that the parser and the compiled form fail on the expression, a date list with the
     * range as its first element, at the "now" of the range, with exactly the error, at the
     * position of the expression, at every check level that lets the range through: one that
     * starts after it ends fails on its order under the range order check, at its '..'. The
     * compiled form parses the expression on every evaluation, fails as the SQL that holds it is
     * validated, and when it evaluates, where it keeps what the caller appended before.
     */
    private void assertNoIntervalRangeFails(NoIntervalRange range, String expression, String expectedError) throws SqlException {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final long now = timestampDriver.parseFloorLiteral(range.now());
        final int rangeOrderPosition = 7 + expression.indexOf("..") - 1;
        for (int checks : TICK_CHECK_LEVELS) {
            if (range.isReversed() && (checks & IntervalUtils.TICK_CHECK_RANGE_ORDER) != 0) {
                assertParseFails(expression, now, checks, rangeOrderPosition, RANGE_ORDER_ERROR);
            } else {
                assertParseFails(expression, now, checks, 7, expectedError);
            }
        }
        final String context = expression + " at " + range.now();
        try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(
                timestampDriver, configuration, expression, 0, expression.length(), 7)) {
            Assert.assertTrue(context, compiled.isParsedOnEvaluation());
            try {
                compiled.validate(now);
                Assert.fail("validate should fail: " + context);
            } catch (SqlException e) {
                TestUtils.assertEquals(context, expectedError, e.getFlyweightMessage());
                Assert.assertEquals(context, 7, e.getPosition());
            }
            out.clear();
            out.add(1L, 2L);
            try {
                compiled.evaluate(out, now);
                Assert.fail("evaluation should fail: " + context);
            } catch (CairoException e) {
                TestUtils.assertEquals(context, range.isReversed() ? RANGE_ORDER_ERROR : expectedError, e.getFlyweightMessage());
                Assert.assertEquals(context, range.isReversed() ? rangeOrderPosition : 7, e.getPosition());
            }
            Assert.assertEquals(context, 2, out.size());
            Assert.assertEquals(context, 1, out.getQuick(0));
            Assert.assertEquals(context, 2, out.getQuick(1));
        }
    }

    /**
     * Asserts that the parser rejects the expression at 2024-01-02T10:00Z with exactly the error, at
     * the position of the expression.
     */
    private void assertParseError(String expression, String expectedError) {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        out.clear();
        try {
            parseTickExprWithNow(
                    timestampDriver,
                    expression,
                    0,
                    expression.length(),
                    7,
                    out,
                    timestampDriver.parseFloorLiteral("2024-01-02T10:00:00.000000Z")
            );
            Assert.fail("parse should fail: " + expression);
        } catch (SqlException e) {
            TestUtils.assertEquals(expression, expectedError, e.getFlyweightMessage());
            Assert.assertEquals(expression, 7, e.getPosition());
        }
    }

    /**
     * Asserts that the parser fails on the expression with the checks, with exactly the error at the
     * position, and keeps what the caller appended before. The expression has the position 7.
     */
    private void assertParseFails(String expression, long now, int checks, int position, String expectedError) {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final String context = expression + " [checks=" + checks + ']';
        out.clear();
        out.add(1L, 2L);
        try {
            IntervalUtils.parseTickExpr(timestampDriver, configuration, expression, 0, expression.length(), 7, out, sink, now, checks);
            Assert.fail("parse should fail: " + context);
        } catch (SqlException e) {
            TestUtils.assertEquals(context, expectedError, e.getFlyweightMessage());
            Assert.assertEquals(context, position, e.getPosition());
        }
        Assert.assertEquals(context, 2, out.size());
        Assert.assertEquals(context, 1, out.getQuick(0));
        Assert.assertEquals(context, 2, out.getQuick(1));
    }

    /**
     * Asserts that the parser fails on the expression with exactly the error, at the position of
     * the expression, with every combination of the checks that depend on "now".
     */
    private void assertParseFailsAtEveryCheckLevel(String expression, long now, String expectedError) {
        for (int checks : TICK_CHECK_LEVELS) {
            assertParseFails(expression, now, checks, 7, expectedError);
        }
    }

    private void assertParsed(String expected, String expression, long now, int checks) throws SqlException {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        out.clear();
        IntervalUtils.parseTickExpr(timestampDriver, configuration, expression, 0, expression.length(), 7, out, sink, now, checks);
        TestUtils.assertEquals(expression, expected, intervalToString(timestampDriver, out));
    }

    /**
     * Asserts that both the parse path ({@code parseTickExpr}) and the compiled path
     * ({@link CompiledTickExpression#evaluate}) produce the expected intervals for the given "now".
     */
    private void assertParsedAndCompiledErrorWithNow(String expression, String nowTimestamp, String expectedError) throws SqlException {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final long now = timestampDriver.parseFloorLiteral(nowTimestamp);
        out.clear();
        try {
            parseTickExprWithNow(timestampDriver, expression, 0, expression.length(), 0, out, now);
            Assert.fail("parse should fail: " + expression);
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), expectedError);
        }
        out.clear();
        final CompiledTickExpression compiled;
        try {
            compiled = IntervalUtils.compileTickExpr(timestampDriver, configuration, expression, 0, expression.length(), 0);
        } catch (SqlException e) {
            // the compiler rejects a list whose date variable elements or fixed dates alone exceed the
            // interval cap, with the error of the parser
            TestUtils.assertContains(e.getFlyweightMessage(), expectedError);
            return;
        }
        try (compiled) {
            compiled.evaluate(out, now);
            Assert.fail("evaluation should fail: " + expression);
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), expectedError);
        }
    }

    /**
     * Asserts that the parser fails on the expression with exactly the error, at the position of
     * the expression, at every check level, and that the compiled form rejects it: building the IR
     * fails with exactly the error of the IR, or, where the IR does not represent the expression and
     * irError is null, the form that parses it on every evaluation fails like the bind variable.
     */
    private void assertParsedAndCompiledFail(String expression, long now, String parserError, String irError) throws SqlException {
        assertParseFailsAtEveryCheckLevel(expression, now, parserError);
        final boolean isParsedOnEvaluation;
        try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(
                timestampType.getDriver(), configuration, expression, 0, expression.length(), 7)) {
            isParsedOnEvaluation = compiled.isParsedOnEvaluation();
        } catch (SqlException e) {
            TestUtils.assertEquals(expression, irError, e.getFlyweightMessage());
            Assert.assertEquals(expression, 7, e.getPosition());
            return;
        }
        Assert.assertTrue(expression, isParsedOnEvaluation);
        assertFailsLikeBindVariable(expression, now, parserError);
    }

    /**
     * Asserts that both the parser and the compiled form reject the expression, the compiled form
     * when it compiles or when it evaluates. SQL parses a bind variable and compiles a literal with a
     * date variable, so the two must fail on the same expressions.
     */
    private void assertParsedAndCompiledRejectedWithNow(String expression, String nowTimestamp, String expectedError) {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final long now = timestampDriver.parseFloorLiteral(nowTimestamp);
        String parsed;
        out.clear();
        try {
            parseTickExprWithNow(timestampDriver, expression, 0, expression.length(), 0, out, now);
            parsed = intervalToString(timestampDriver, out).toString();
        } catch (SqlException e) {
            parsed = "error: " + e.getFlyweightMessage();
        }
        String compiled;
        try (CompiledTickExpression compiledExpr = IntervalUtils.compileTickExpr(
                timestampDriver, configuration, expression, 0, expression.length(), 0)) {
            out.clear();
            try {
                compiledExpr.evaluate(out, now);
                compiled = intervalToString(timestampDriver, out).toString();
            } catch (CairoException e) {
                compiled = "error: " + e.getFlyweightMessage();
            }
        } catch (SqlException e) {
            compiled = "error: " + e.getFlyweightMessage();
        }
        // one assertion over both paths, so a failure shows the result of each
        final String message = expression + " at " + nowTimestamp + ", expected both to fail with '" + expectedError
                + "'\nparsed: " + parsed + "\ncompiled: " + compiled;
        Assert.assertTrue(message, parsed.startsWith("error: ") && parsed.contains(expectedError)
                && compiled.startsWith("error: ") && compiled.contains(expectedError));
    }

    private void assertParsedAndCompiledWithNow(String expected, String expression, String nowTimestamp) throws SqlException {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final long now = timestampDriver.parseFloorLiteral(nowTimestamp);
        final String expectedForType = ColumnType.isTimestampNano(timestampType.getTimestampType())
                ? expected.replace("00Z", "00000Z").replace("99Z", "99999Z")
                : expected;

        out.clear();
        parseTickExprWithNow(timestampDriver, expression, 0, expression.length(), 0, out, now);
        final String parsed = intervalToString(timestampDriver, out).toString();

        out.clear();
        try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(
                timestampDriver, configuration, expression, 0, expression.length(), 0)) {
            compiled.evaluate(out, now);
        }
        final String compiled = intervalToString(timestampDriver, out).toString();

        // one assertion over both paths, so a failure shows the result of each
        TestUtils.assertEquals(
                expression,
                "parsed: " + expectedForType + "\ncompiled: " + expectedForType,
                "parsed: " + parsed + "\ncompiled: " + compiled
        );
    }

    /**
     * Asserts that the parser parses the expression with the configuration as it parses the list
     * without one of its elements, at every check level, where the levels with the interval limit
     * check fail with the error, and that the compiled form, which parses the expression on every
     * evaluation, fails with it as the SQL that holds it is validated, at the position of the
     * expression.
     */
    private void assertParsedAsWithoutElement(CairoConfiguration configuration, String expression, String withoutElement, long now, String limitError) throws SqlException {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        for (int checks : TICK_CHECK_LEVELS) {
            final String expected = parseWithChecks(timestampDriver, configuration, withoutElement, now, checks);
            if ((checks & IntervalUtils.TICK_CHECK_INTERVAL_LIMIT) != 0) {
                TestUtils.assertEquals(withoutElement, "error: " + limitError, expected);
            }
            TestUtils.assertEquals(expression + " [checks=" + checks + ']', expected, parseWithChecks(timestampDriver, configuration, expression, now, checks));
        }
        try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(timestampDriver, configuration, expression, 0, expression.length(), 7)) {
            Assert.assertTrue(expression, compiled.isParsedOnEvaluation());
            try {
                compiled.validate(now);
                Assert.fail("validate should fail: " + expression);
            } catch (SqlException e) {
                TestUtils.assertEquals(expression, limitError, e.getFlyweightMessage());
                Assert.assertEquals(expression, 7, e.getPosition());
            }
        }
    }

    private void assertParsedAtEveryCheckLevel(String expected, String expression, long now) throws SqlException {
        for (int checks : TICK_CHECK_LEVELS) {
            assertParsed(expected, expression, now, checks);
        }
    }

    private void assertParsedIntervalCount(int expectedCount, String expression, String nowTimestamp, int checks) throws SqlException {
        out.clear();
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final long now = timestampDriver.parseFloorLiteral(nowTimestamp);
        IntervalUtils.parseTickExpr(timestampDriver, configuration, expression, 0, expression.length(), 0, out, sink, now, checks);
        Assert.assertEquals(expression, expectedCount, out.size() / 2);
    }

    private void assertParsedWithChecks(String expected, String expression, String nowTimestamp, int checks) throws SqlException {
        out.clear();
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final long now = timestampDriver.parseFloorLiteral(nowTimestamp);
        IntervalUtils.parseTickExpr(timestampDriver, configuration, expression, 0, expression.length(), 0, out, sink, now, checks);
        TestUtils.assertEquals(
                expression,
                ColumnType.isTimestampNano(timestampType.getTimestampType())
                        ? expected.replace("00Z", "00000Z").replace("99Z", "99999Z")
                        : expected,
                intervalToString(timestampDriver, out)
        );
    }

    private void assertParsedWithChecksFails(String expression, String nowTimestamp, int checks, String expectedError) {
        out.clear();
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final long now = timestampDriver.parseFloorLiteral(nowTimestamp);
        try {
            IntervalUtils.parseTickExpr(timestampDriver, configuration, expression, 0, expression.length(), 0, out, sink, now, checks);
            Assert.fail("parse should fail: " + expression);
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), expectedError);
        }
        // the parser unwinds what it appended
        Assert.assertEquals(0, out.size());
    }

    /**
     * Asserts that the parser fails on the expression, a date and a text after it, with the error
     * that it shows from the first ';' on: the date is the one that it shows before that ';'.
     */
    private void assertReferenceSuffixFails(String expression, long now, String date, String expectedError) {
        assertParseFails(expression, now, IntervalUtils.TICK_CHECK_ALL, 7, expectedError.replace(": ;", ": " + date + ";"));
    }

    private void assertMatchesUnmergedOracle(String... intervals) throws SqlException {
        assertMatchesUnmergedOracle(configuration.getSqlIntervalIncrementalMergeThreshold(), intervals);
    }

    private void assertMatchesUnmergedOracle(int mergeThreshold, String... intervals) throws SqlException {
        // both configurations lift the cap, so only the merge points differ
        final CairoConfiguration mergingConfiguration = new DefaultCairoConfiguration(".") {
            @Override
            public int getSqlIntervalIncrementalMergeThreshold() {
                return mergeThreshold;
            }

            @Override
            public int getSqlIntervalMaxIntervalsAfterMerge() {
                return 10_000_000;
            }
        };
        final CairoConfiguration oracleConfiguration = new DefaultCairoConfiguration(".") {
            @Override
            public int getSqlIntervalIncrementalMergeThreshold() {
                return 5_000_000;
            }

            @Override
            public int getSqlIntervalMaxIntervalsAfterMerge() {
                return 10_000_000;
            }
        };
        for (String interval : intervals) {
            final LongList expected = new LongList();
            final LongList actual = new LongList();
            parseTickExprAtMonday(oracleConfiguration, expected, interval);
            parseTickExprAtMonday(mergingConfiguration, actual, interval);
            Assert.assertTrue(interval, expected.size() > 0);
            Assert.assertEquals(interval + " at merge threshold " + mergeThreshold, expected, actual);
        }
    }

    private void assertMaxIntervalCount(String expression, int cap, int expectedCount) throws SqlException {
        try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(
                timestampType.getDriver(), configuration, expression, 0, expression.length(), 0)) {
            Assert.assertEquals(expression, expectedCount, compiled.getMaxIntervalCount(cap));
        }
    }

    // Expects the bound of the expression to be expectedCount, and evaluate() to yield that many
    // intervals at some "now" of maxIntervalCountForAnyNow()
    private void assertMaxIntervalCountReached(String expression, int expectedCount) throws SqlException {
        assertMaxIntervalCount(expression, Integer.MAX_VALUE, expectedCount);
        Assert.assertEquals(expression, expectedCount, maxIntervalCountForAnyNow(expression));
    }

    private void assertShortInterval(String expected, String interval) throws SqlException {
        out.clear();
        final TimestampDriver timestampDriver = timestampType.getDriver();
        parseTickExpr(timestampDriver, interval, 0, interval.length(), 0, out);
        TestUtils.assertEquals(
                ColumnType.isTimestampNano(timestampType.getTimestampType())
                        ? expected.replace("00Z", "00000Z").replace("99Z", "99999Z")
                        : expected,
                intervalToString(timestampDriver, out)
        );
    }

    private void assertValidateFails(String expression, long now, String expectedError) throws SqlException {
        try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(
                timestampType.getDriver(), configuration, expression, 0, expression.length(), 7)) {
            compiled.validate(now);
            Assert.fail("validate should fail: " + expression);
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), expectedError);
            Assert.assertEquals(expression, 7, e.getPosition());
        }
    }

    /**
     * Asserts that the compiled form, which parses the expression on every evaluation, fails as the
     * SQL that holds it is validated against the "now", with exactly the error, at the position of
     * the expression.
     */
    private void assertValidateFailsWith(String expression, long now, String expectedError) throws SqlException {
        try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(
                timestampType.getDriver(), configuration, expression, 0, expression.length(), 7)) {
            Assert.assertTrue(expression, compiled.isParsedOnEvaluation());
            try {
                compiled.validate(now);
                Assert.fail("validate should fail: " + expression);
            } catch (SqlException e) {
                TestUtils.assertEquals(expression, expectedError, e.getFlyweightMessage());
                Assert.assertEquals(expression, 7, e.getPosition());
            }
        }
    }

    /**
     * Returns a duration that takes the timestamp to the null timestamp, Long.MIN_VALUE, which
     * TimestampDriver.add() also returns for a unit that it does not add: in days, seconds and the
     * rest, each of which fits an int, and whose running sum stays above the null timestamp until
     * the last part.
     */
    private String durationToNull(long timestamp) {
        final boolean isNano = ColumnType.isTimestampNano(timestampType.getTimestampType());
        final BigInteger second = BigInteger.valueOf(isNano ? 1_000_000_000L : 1_000_000L);
        final BigInteger day = second.multiply(BigInteger.valueOf(86_400L));
        final BigInteger[] days = BigInteger.valueOf(timestamp).subtract(BigInteger.valueOf(Long.MIN_VALUE)).divideAndRemainder(day);
        final BigInteger[] seconds = days[1].divideAndRemainder(second);
        return "-" + days[0] + "d-" + seconds[0] + "s-" + seconds[1] + (isNano ? "n" : "u");
    }

    /**
     * Returns intervals given at microsecond precision at the precision of the timestamp type.
     */
    private String forTimestampType(String expected) {
        return ColumnType.isTimestampNano(timestampType.getTimestampType())
                ? expected.replace("00Z", "00000Z").replace("99Z", "99999Z")
                : expected;
    }

    private void assertTickExprAtMonday(CairoConfiguration cfg, String expected, String interval) throws SqlException {
        out.clear();
        parseTickExprAtMonday(cfg, out, interval);
        TestUtils.assertEquals(
                ColumnType.isTimestampNano(timestampType.getTimestampType())
                        ? expected.replace("00Z", "00000Z").replace("99Z", "99999Z")
                        : expected,
                intervalToString(timestampType.getDriver(), out)
        );
    }

    private void assertTickExprErrorAtMonday(String interval, String expectedError) {
        assertTickExprErrorAtMonday(configuration, interval, expectedError);
    }

    private void assertTickExprErrorAtMonday(CairoConfiguration cfg, String interval, String expectedError) {
        out.clear();
        try {
            parseTickExprAtMonday(cfg, out, interval);
            Assert.fail("Expected SqlException with message containing: " + expectedError + ", got " + out.size() / 2 + " intervals");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), expectedError);
        }
    }

    // Parses the interval at mondayNow() and expects the intervals
    private void assertTickExprIntervals(CairoConfiguration cfg, LongList expected, String interval) throws SqlException {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        out.clear();
        parseTickExprAtMonday(cfg, out, interval);
        Assert.assertEquals(interval, intervalToString(timestampDriver, expected).toString(), intervalToString(timestampDriver, out).toString());
    }

    // Parses the interval at mondayNow() with the default configuration, expects the intervals, and
    // expects the largest list size it requests within the memory bound of the incremental merge:
    // two open merge ranges, each below C + T intervals at the end of an iteration of its loops,
    // the T filtered intervals that applyDayFilter writes past the end of the list, and the
    // plainValueCount values of the innermost time list without bracketed suffixes (C = 1,024 and
    // T = 256), at 2 longs per interval.
    private void assertTimeListGrowthBounded(LongList expected, int plainValueCount, String interval) throws SqlException {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final PeakLongList peakOut = new PeakLongList();
        parseTickExprAtMonday(configuration, peakOut, interval);
        Assert.assertEquals(interval, intervalToString(timestampDriver, expected).toString(), intervalToString(timestampDriver, peakOut).toString());
        Assert.assertTrue(interval + ": peak longs " + peakOut.peak, peakOut.peak <= 2 * (2 * 1_024 + 3 * 256 + plainValueCount));
    }

    private int countMaxIntervalsGetterCalls(String interval) throws SqlException {
        final MaxIntervalsCountingConfiguration countingConfiguration = new MaxIntervalsCountingConfiguration();
        parseTickExprAtMonday(countingConfiguration, new LongList(), interval);
        return countingConfiguration.getterCallCount;
    }

    // The JIT filter compiles getMaxIntervalCount() range checks for an expression and refills
    // them on every run, so a count above the bound at any "now" would lose rows. Evaluates the
    // expression every 30 minutes through 2024 and 2025, and one unit either side of each of those
    // instants, fails at a count above the bound, and returns the largest count. The half hours
    // cover the midnights of every zone of the tests, the clock changes at local midnight in
    // America/Santiago and Asia/Beirut, and the one before it in Atlantic/Azores.
    private int maxIntervalCountForAnyNow(String expression) throws SqlException {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final long start = timestampDriver.parseFloorLiteral("2024-01-01T00:00:00.000000Z");
        final long end = timestampDriver.parseFloorLiteral("2026-01-01T00:00:00.000000Z");
        final long step = timestampDriver.fromMinutes(30);
        final LongList intervals = new LongList();
        try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(
                timestampDriver, configuration, expression, 0, expression.length(), 0)) {
            final int bound = compiled.getMaxIntervalCount(Integer.MAX_VALUE);
            Assert.assertTrue(expression, bound > 0);
            int maxCount = 0;
            for (long now = start; now < end; now += step) {
                for (long delta = -1; delta <= 1; delta++) {
                    intervals.clear();
                    compiled.evaluate(intervals, now + delta);
                    final int count = intervals.size() / 2;
                    if (count > bound) {
                        Assert.fail(expression + " yields " + count + " intervals, above its bound of " + bound
                                + ", at " + timestampDriver.toMSecString(now + delta));
                    }
                    maxCount = Math.max(maxCount, count);
                }
            }
            return maxCount;
        }
    }

    // 2024-01-08T01:00, a Monday
    private long mondayNow() {
        return timestampType.getDriver().fromMicros(MONDAY_NOW_MICROS);
    }

    private void parseTickExprAtMonday(CairoConfiguration cfg, LongList intervals, String interval) throws SqlException {
        IntervalUtils.parseTickExpr(timestampType.getDriver(), cfg, interval, 0, interval.length(), 0, intervals, new StringSink(), mondayNow());
    }

    // The union of the weekend days of the years in the zone, each day converted on its own
    private LongList unionOfZonedWeekendDays(ZoneId zone, int firstYear, int lastYear) {
        final LongList days = new LongList();
        for (LocalDate day = LocalDate.of(firstYear, 1, 1); day.getYear() <= lastYear; day = day.plusDays(1)) {
            if (day.getDayOfWeek() == DayOfWeek.SATURDAY || day.getDayOfWeek() == DayOfWeek.SUNDAY) {
                addZonedLocalDay(days, day, zone);
            }
        }
        return unionOf(days);
    }

    @FunctionalInterface
    interface DateVariableExpectedBuilder {
        String build(long now, TimestampDriver driver, TestTimestampType tsType);
    }

    /**
     * Checks tick expressions against the contract of
     * {@link DateExpressionEvaluator#containsDateVariable}: an expression whose intervals depend on
     * "now" holds a date variable. SQL evaluates a detected expression through
     * {@link CompiledTickExpression}, which must agree with the parser on a well-formed expression:
     * one that it compiles evaluates to the intervals of the parser at every "now", and one that it
     * rejects fails to parse at every "now".
     * <p>
     * The two validate some expressions in different ways, which the checker counts as malformed,
     * and of which it only requires a detected date variable.
     * <p>
     * A time part of the list that the grammar rejects counts as well-formed: the two reject it, the
     * parser also after an element that keeps its own time and ignores it. A duration with a unit
     * that the timestamp drivers do not add counts as well-formed: the two agree on it, as the
     * compiled form parses such an expression when it evaluates. So do a day filter after the
     * duration, a text after the first ';' that the parser rejects after every element, such as one
     * with a ';' more, which both reject, and a time part of an element: the compiled form parses an
     * expression with a static element with a time part of its own under a time of the list when it
     * evaluates. A time part of an element with a slot that has a timezone
     * of its own counts as malformed: under a timezone of the list, the compiled form converts the
     * slot by both timezones, and the parser by its own. A timezone of an element that does not
     * parse counts as well-formed: the compiled form parses an expression with an element with a
     * timezone of its own on every evaluation, and so does a day filter of an element that does not
     * parse. The repeating interval of the parser, a duration, a
     * period and a count, counts as malformed: the parser takes it, and the IR of the compiled form
     * rejects it. A range of business days that holds no day at a "now" reads neither the time part
     * nor the text after the first ';' of the list there, see {@link #hasBusinessDayRange}, so the
     * parser may return no interval at that "now" for an expression that the compiled form rejects
     * as it compiles.
     */
    private static class DateVariableDetectionChecker {
        private static final String ERROR = "error";
        private static final int MAX_REPORTED_FAILURES = 20;
        // the interval merge threshold and limit of both forms
        private final CairoConfiguration configuration;
        private final StringSink failures = new StringSink();
        private final LongList intervals = new LongList();
        private final long[] nows = new long[FUZZ_NOWS.length];
        private final StringSink parseSink = new StringSink();
        private final String[] parsed = new String[FUZZ_NOWS.length];
        private final TimestampDriver timestampDriver;
        private int checkCount;
        private int compiledCount;
        private int failureCount;
        // expressions with a day filter after the duration, which failed on both forms at every "now"
        private int misplacedDayFilterErrorCount;
        private int nowDependentCount;
        // compiled expressions with a static element whose time part starts with a bracket, under a
        // single time of the list, which parse on every evaluation
        private int staticElementBracketedTimeParsedCount;
        // compiled expressions with a duration unit that the drivers do not add, which failed at some "now"
        private int unknownUnitErrorCount;

        DateVariableDetectionChecker(TimestampDriver timestampDriver, CairoConfiguration configuration) {
            this.timestampDriver = timestampDriver;
            this.configuration = configuration;
            for (int i = 0; i < FUZZ_NOWS.length; i++) {
                nows[i] = timestampDriver.parseFloorLiteral(FUZZ_NOWS[i]);
            }
        }

        void assertNoFailures() {
            LOG.info().$("date variable detection fuzz done [expressions=").$(checkCount)
                    .$(", nowDependent=").$(nowDependentCount)
                    .$(", compiled=").$(compiledCount)
                    .$(", unknownUnitErrors=").$(unknownUnitErrorCount)
                    .$(", misplacedDayFilterErrors=").$(misplacedDayFilterErrorCount)
                    .$(", staticElementBracketedTimeParsed=").$(staticElementBracketedTimeParsedCount)
                    .$(", failures=").$(failureCount)
                    .I$();
            if (failureCount > 0) {
                Assert.fail(failureCount + " of " + checkCount + " tick expressions failed, the first "
                        + Math.min(failureCount, MAX_REPORTED_FAILURES) + ":\n" + failures);
            }
            Assert.assertTrue("no expression depended on now", nowDependentCount > 0);
            Assert.assertTrue("no expression compiled", compiledCount > 0);
            Assert.assertTrue("no expression with an unknown duration unit failed", unknownUnitErrorCount > 0);
            Assert.assertTrue("no expression with a day filter after the duration failed", misplacedDayFilterErrorCount > 0);
            Assert.assertTrue(
                    "no expression with a static element with a bracketed time part under a time of the list parsed on evaluation",
                    staticElementBracketedTimeParsedCount > 0
            );
        }

        /**
         * @param isMalformed whether the expression holds a part that the grammar rejects
         */
        void check(String expression, boolean isMalformed) {
            checkCount++;
            try {
                check0(expression, isMalformed);
            } catch (AssertionError | RuntimeException e) {
                throw new AssertionError("tick expression: " + expression, e);
            }
        }

        private void check0(String expression, boolean isMalformed) {
            boolean isNowDependent = false;
            for (int i = 0; i < nows.length; i++) {
                parsed[i] = parse(expression, nows[i]);
                isNowDependent |= !parsed[i].equals(parsed[0]);
            }
            if (isNowDependent) {
                nowDependentCount++;
            }
            if (!DateExpressionEvaluator.containsDateVariable(expression, 0, expression.length())) {
                if (isNowDependent) {
                    for (int i = 1; i < nows.length; i++) {
                        if (!parsed[i].equals(parsed[0])) {
                            fail(expression, "holds no date variable, but parses to " + parsed[0] + " at " + FUZZ_NOWS[0]
                                    + " and to " + parsed[i] + " at " + FUZZ_NOWS[i]);
                            return;
                        }
                    }
                }
                return;
            }
            if (isMalformed) {
                return;
            }
            final CompiledTickExpression compiled;
            try {
                compiled = IntervalUtils.compileTickExpr(timestampDriver, configuration, expression, 0, expression.length(), 0);
            } catch (SqlException | CairoException e) {
                for (int i = 0; i < nows.length; i++) {
                    if (!ERROR.equals(parsed[i]) && !("[]".equals(parsed[i]) && hasBusinessDayRange(expression))) {
                        fail(expression, "does not compile [error=" + e.getFlyweightMessage() + "], but parses to "
                                + parsed[i] + " at " + FUZZ_NOWS[i]);
                        return;
                    }
                }
                if (hasMisplacedDayFilter(expression)) {
                    misplacedDayFilterErrorCount++;
                }
                return;
            }
            compiledCount++;
            try (compiled) {
                if (compiled.isParsedOnEvaluation() && hasStaticElementBracketedTimeUnderListTime(expression)) {
                    staticElementBracketedTimeParsedCount++;
                }
                boolean hasError = false;
                boolean isAlwaysError = true;
                for (int i = 0; i < nows.length; i++) {
                    intervals.clear();
                    String evaluated;
                    try {
                        compiled.evaluate(intervals, nows[i]);
                        evaluated = intervalToString(timestampDriver, intervals).toString();
                    } catch (CairoException e) {
                        evaluated = ERROR;
                    }
                    if (!evaluated.equals(parsed[i])) {
                        fail(expression, "evaluates to " + evaluated + ", but parses to " + parsed[i] + " at " + FUZZ_NOWS[i]);
                        return;
                    }
                    hasError |= ERROR.equals(evaluated);
                    isAlwaysError &= ERROR.equals(evaluated);
                }
                if (hasError && hasUnknownDurationUnit(expression)) {
                    unknownUnitErrorCount++;
                }
                if (isAlwaysError && hasMisplacedDayFilter(expression)) {
                    misplacedDayFilterErrorCount++;
                }
            }
        }

        private void fail(String expression, String reason) {
            if (failureCount++ < MAX_REPORTED_FAILURES) {
                failures.put(expression).put(": ").put(reason).put('\n');
            }
        }

        private String parse(String expression, long now) {
            intervals.clear();
            try {
                IntervalUtils.parseTickExpr(timestampDriver, configuration, expression, 0, expression.length(), 0,
                        intervals, parseSink, now);
                return intervalToString(timestampDriver, intervals).toString();
            } catch (SqlException | CairoException e) {
                return ERROR;
            }
        }
    }

    /**
     * Checks date lists with a date variable range of either kind whose element has a timezone and
     * a day filter or tick calendar of its own, among other elements, against the same suffix on a
     * list of the range alone. At every "now" of the checker, with all the checks, the date list
     * selects what the list of the range alone selects, with the timezone of the element, or else
     * the one of the list, then the suffix of the element after the time part of the list and
     * before its duration, together with what the list of the other elements selects with the
     * time part, timezone and duration of the list. The compiled form of the date list selects the
     * same, and the IR of the list of the range alone, where it has one, agrees with the parser.
     * Every start of a range lies before every end at any "now", and a range spans at most five
     * days, so no list fails. A negative duration turns a short interval into one that ends before
     * it starts, and the union of such intervals depends on their order, so the checker joins two
     * lists, and compares an IR with the parser, only where every interval ends after it starts.
     */
    static class ElementSuffixChecker {
        private static final String[] DAY_ENDS = {"$today", "$tomorrow", "$today + 2d"};
        private static final String[] DAY_STARTS = {"$today", "$yesterday", "$today - 2d"};
        // the timezones of an element: those of the list, then offsets that a timezone which ran on
        // into the day filter of its element took for UTC or GMT, or failed on
        private static final String[] ELEMENT_ZONES = {"", "@+02:00", "@Europe/London", "@America/New_York", "@UTC+05", "@utc-03", "@GMT+05", "@Z", "@+05", "@-05", "@05"};
        private static final int FIRST_ZONE_READ_INTO_SUFFIX = 4;
        private static final String[] OTHER_ELEMENTS = {"$today", "$now", "$yesterday + 3h", "2024-01-05", "2025-01-24", "$tomorrow..$today + 2d", "$now - 5h..$now - 4h"};
        private static final String[] POINT_ENDS = {"$now", "$NOW + 1h", "$Now + 45m", "$now + 1d"};
        private static final String[] POINT_STARTS = {"$now - 2h", "$NOW - 30m", "$now - 1d", "$now - 90s", "$yesterday + 30m", "$yesterday + 23h"};
        private final CairoConfiguration configuration;
        private final String[] dayRangeDurations;
        private final LongList intervals = new LongList();
        private final String[] nowTexts;
        private final long[] nows;
        private final StringSink parseSink = new StringSink();
        private final String[] pointRangeDurations;
        // the number of expressions that the checker compared, for each suffix of the element
        private final int[] suffixCounts;
        private final String[] suffixes;
        private final TimestampDriver timestampDriver;
        // the shapes that the checker compared
        int clippedDayRangeWithSlotZoneCount;
        int dayRangeWithTimeListCount;
        int droppedCount;
        int irCount;
        int keptCount;
        int listZoneWithSuffixCount;
        int otherElementsCount;
        int pointInTimeRangeCount;
        int startDayIsNotEndDayCount;
        int zoneBeforeSuffixCount;
        private int checkCount;

        /**
         * @param suffixes            the day filters or tick calendars of the element, with their '#'
         * @param dayRangeDurations   the durations of the list of a range with a whole day at one end
         * @param pointRangeDurations the durations of the list of a range between two points in time
         */
        ElementSuffixChecker(
                TimestampDriver timestampDriver,
                CairoConfiguration configuration,
                String[] suffixes,
                String[] dayRangeDurations,
                String[] pointRangeDurations,
                String[] nows
        ) {
            this.timestampDriver = timestampDriver;
            this.configuration = configuration;
            this.suffixes = suffixes;
            this.dayRangeDurations = dayRangeDurations;
            this.pointRangeDurations = pointRangeDurations;
            this.nowTexts = nows;
            this.nows = new long[nows.length];
            for (int i = 0; i < nows.length; i++) {
                this.nows[i] = timestampDriver.parseFloorLiteral(nows[i]);
            }
            this.suffixCounts = new int[suffixes.length];
        }

        /**
         * Asserts that the checker compared each suffix, and each shape that both the day filters
         * and the tick calendars make.
         */
        void assertEveryShape() {
            for (int i = 0; i < suffixes.length; i++) {
                Assert.assertTrue("suffix " + suffixes[i], suffixCounts[i] > 0);
            }
            Assert.assertTrue("a range between two points in time with a suffix", pointInTimeRangeCount > 0);
            Assert.assertTrue("a range that its suffix drops", droppedCount > 0);
            Assert.assertTrue("a range that its suffix keeps", keptCount > 0);
            Assert.assertTrue("a suffix after the timezone of the list", listZoneWithSuffixCount > 0);
            Assert.assertTrue("other elements", otherElementsCount > 0);
            Assert.assertTrue("a range of days with a time list", dayRangeWithTimeListCount > 0);
            Assert.assertTrue("a range of days that a point in time clips, with a time list slot that has a timezone", clippedDayRangeWithSlotZoneCount > 0);
            Assert.assertTrue("an IR", irCount > 0);
        }

        void check(Rnd rnd) throws SqlException {
            checkCount++;
            // two points in time, a day and a point in time, or two days
            final int kind = rnd.nextInt(4);
            final boolean isPointInTime = kind < 2;
            final boolean isClipped = kind == 2;
            final boolean isDayStart = kind == 3 || (isClipped && rnd.nextBoolean());
            final String start = pick(rnd, isDayStart ? DAY_STARTS : POINT_STARTS);
            final String end = pick(rnd, kind == 3 || (isClipped && !isDayStart) ? DAY_ENDS : POINT_ENDS);
            final String range = start + ".." + end;
            final int zoneIndex = rnd.nextInt(ELEMENT_ZONES.length);
            final String zone = ELEMENT_ZONES[zoneIndex];
            final int suffixIndex = rnd.nextInt(suffixes.length);
            final String suffix = suffixes[suffixIndex];
            final String time = pick(rnd, FUZZ_TIMES);
            final String listZone = pick(rnd, FUZZ_ZONES);
            final String duration = pick(rnd, isPointInTime ? pointRangeDurations : dayRangeDurations);
            // up to two other elements, before or after the range
            final int otherCount = rnd.nextInt(3);
            final int rangeIndex = rnd.nextInt(otherCount + 1);
            final StringBuilder list = new StringBuilder("[");
            final StringBuilder others = new StringBuilder("[");
            for (int i = 0; i <= otherCount; i++) {
                if (i > 0) {
                    list.append(", ");
                }
                if (i == rangeIndex) {
                    list.append(range).append(zone).append(suffix);
                } else {
                    final String other = pick(rnd, OTHER_ELEMENTS);
                    list.append(other);
                    others.append(others.length() > 1 ? ", " : "").append(other);
                }
            }
            final String expression = list + "]" + time + listZone + duration;
            final String rangeZone = zone.isEmpty() ? listZone : zone;
            final String rangeList = "[" + range + "]" + time + rangeZone + suffix + duration;
            final String otherList = otherCount > 0 ? others + "]" + time + listZone + duration : null;

            final boolean hasSuffix = !suffix.isEmpty();
            suffixCounts[suffixIndex]++;
            pointInTimeRangeCount += isPointInTime && hasSuffix ? 1 : 0;
            zoneBeforeSuffixCount += zoneIndex >= FIRST_ZONE_READ_INTO_SUFFIX && hasSuffix ? 1 : 0;
            listZoneWithSuffixCount += zone.isEmpty() && !listZone.isEmpty() && hasSuffix ? 1 : 0;
            dayRangeWithTimeListCount += !isPointInTime && time.indexOf('[') >= 0 && (hasSuffix || !zone.isEmpty()) ? 1 : 0;
            clippedDayRangeWithSlotZoneCount += isClipped && !zone.isEmpty() && hasSuffix && time.indexOf('@') >= 0 ? 1 : 0;

            CompiledTickExpression compiledRangeList = null;
            try {
                compiledRangeList = IntervalUtils.compileTickExpr(timestampDriver, configuration, rangeList, 0, rangeList.length(), 0);
            } catch (SqlException e) {
                // SQL rejects this list as it compiles, so it has no IR to compare
            }
            try (
                    CompiledTickExpression ignore = compiledRangeList;
                    CompiledTickExpression compiledExpression = IntervalUtils.compileTickExpr(timestampDriver, configuration, expression, 0, expression.length(), 0)
            ) {
                for (int i = 0; i < nows.length; i++) {
                    final long now = nows[i];
                    final String context = expression + " at " + nowTexts[i];
                    final LongList parsed = parse(expression, now, context);
                    final LongList rangeParsed = parse(rangeList, now, context);
                    LongList expected = rangeParsed;
                    if (otherList != null) {
                        final LongList otherParsed = parse(otherList, now, context);
                        if (isWellFormed(rangeParsed) && isWellFormed(otherParsed)) {
                            expected = union(rangeParsed, otherParsed);
                            otherElementsCount++;
                        } else {
                            expected = null;
                        }
                    }
                    if (expected != null) {
                        final String expectedText = intervalToString(timestampDriver, expected).toString();
                        TestUtils.assertEquals(
                                context + "\nthe range alone: " + rangeList + (otherList != null ? "\nthe other elements: " + otherList : ""),
                                expectedText,
                                intervalToString(timestampDriver, parsed)
                        );
                    }
                    final String parsedText = intervalToString(timestampDriver, parsed).toString();
                    if (compiledExpression.isParsedOnEvaluation() || isWellFormed(parsed)) {
                        intervals.clear();
                        compiledExpression.evaluate(intervals, now);
                        TestUtils.assertEquals(context + ", compiled", parsedText, intervalToString(timestampDriver, intervals));
                    }
                    if (compiledRangeList != null && !compiledRangeList.isParsedOnEvaluation() && isWellFormed(rangeParsed)) {
                        final String rangeParsedText = intervalToString(timestampDriver, rangeParsed).toString();
                        intervals.clear();
                        compiledRangeList.evaluate(intervals, now);
                        TestUtils.assertEquals(rangeList + " at " + nowTexts[i] + ", through its IR", rangeParsedText, intervalToString(timestampDriver, intervals));
                        irCount++;
                    }
                    if (hasSuffix) {
                        if (rangeParsed.size() > 0) {
                            keptCount++;
                        } else if (parse("[" + range + "]" + time + rangeZone + duration, now, context).size() > 0) {
                            droppedCount++;
                        }
                        if (isPointInTime) {
                            final LongList unfiltered = parse("[" + range + "]", now, context);
                            if (timestampDriver.startOfDay(unfiltered.getQuick(0), 0) != timestampDriver.startOfDay(unfiltered.getQuick(1), 0)) {
                                startDayIsNotEndDayCount++;
                            }
                        }
                    }
                }
            }
        }

        void log(String suffixKind) {
            LOG.info().$("element suffix fuzz done [suffixes=").$(suffixKind)
                    .$(", expressions=").$(checkCount)
                    .$(", pointInTimeRanges=").$(pointInTimeRangeCount)
                    .$(", dropped=").$(droppedCount)
                    .$(", kept=").$(keptCount)
                    .$(", startDayIsNotEndDay=").$(startDayIsNotEndDayCount)
                    .$(", zoneBeforeSuffix=").$(zoneBeforeSuffixCount)
                    .$(", listZoneWithSuffix=").$(listZoneWithSuffixCount)
                    .$(", otherElements=").$(otherElementsCount)
                    .$(", dayRangeWithTimeList=").$(dayRangeWithTimeListCount)
                    .$(", clippedDayRangeWithSlotZone=").$(clippedDayRangeWithSlotZoneCount)
                    .$(", ir=").$(irCount)
                    .I$();
        }

        private static boolean isWellFormed(LongList intervals) {
            for (int i = 0, n = intervals.size(); i < n; i += 2) {
                if (intervals.getQuick(i) > intervals.getQuick(i + 1)) {
                    return false;
                }
            }
            return true;
        }

        /**
         * Returns the union of two lists of intervals: the intervals of both, sorted by start, each
         * joined to the one before it where the two overlap or touch.
         */
        private static LongList union(LongList a, LongList b) {
            final ArrayList<long[]> all = new ArrayList<>();
            for (LongList list : new LongList[]{a, b}) {
                for (int i = 0, n = list.size(); i < n; i += 2) {
                    all.add(new long[]{list.getQuick(i), list.getQuick(i + 1)});
                }
            }
            all.sort(Comparator.comparingLong(interval -> interval[0]));
            final LongList union = new LongList();
            for (long[] interval : all) {
                final int n = union.size();
                if (n > 0 && interval[0] <= union.getQuick(n - 1) + 1) {
                    union.setQuick(n - 1, Math.max(union.getQuick(n - 1), interval[1]));
                } else {
                    union.add(interval[0], interval[1]);
                }
            }
            return union;
        }

        private LongList parse(String expression, long now, String context) {
            final LongList result = new LongList();
            try {
                IntervalUtils.parseTickExpr(timestampDriver, configuration, expression, 0, expression.length(), 0, result, parseSink, now);
            } catch (SqlException e) {
                throw new AssertionError(expression + " fails, checking " + context + ": " + e.getFlyweightMessage());
            }
            return result;
        }
    }

    /**
     * The shapes of the expressions of testCompiledDateRangeMergeFuzz(), which the generator counts
     * from its own choices.
     */
    private static class LongRangeShapes {
        // a range in London whose 01:30 comes twice on a day the clocks go back, after its first merge
        int dstSplitAfterFirstMergeCount;
        // a range that merged, then an element that lies earlier, then a range
        int earlierElementAfterMergedRangeCount;
        // a range whose day filter leaves days without an interval after its first merge
        int emptyDaysAfterFirstMergeCount;
        // an element that lies later than a range, before it in the list
        int laterElementBeforeRangeCount;
    }

    /**
     * A day filter, the text after its '#', and the days that it holds.
     */
    private record DayFilter(String text, Set<DayOfWeek> days) {
    }

    /**
     * A date variable range that adds no interval at {@code now}, whether it starts after it ends
     * there, the first day of a range of days, null for a range between two points in time, and a
     * range of the same kind that adds intervals at 2024-01-02T10:00Z.
     */
    private record NoIntervalRange(String range, String now, String firstDay, boolean isReversed, String forwardRange) {
        boolean isPointInTime() {
            return firstDay == null;
        }
    }

    /**
     * An expression with an element that keeps its own time, where ? stands for the time part of
     * the list, the intervals that it selects at 2024-01-02T10:00Z at microsecond precision, and
     * whether the IR of the compiled form represents it with a time part that the IR represents.
     */
    private record OwnTimeElement(String expression, String intervals, boolean hasIr) {
    }

    /**
     * A range between two points in time, the intervals that it selects at microsecond precision,
     * and whether the IR of the compiled form represents it, rather than parsing it on every
     * evaluation.
     */
    private record PointInTimeRange(String expression, String intervals, boolean hasIr) {
    }
    // An element of a date list: a date variable, or the range from start to end when end is not null
    private record DateVariableListElement(OffsetDateVariable start, OffsetDateVariable end) {
    }

    // A date variable with an offset of offset units: d, h or bd, or no offset when unit is empty
    private record OffsetDateVariable(String name, String unit, int offset) {
    }

    // IntervalUtils reads the cap through this getter only when it merges or checks the interval
    // count, so the read count measures the merge cadence.
    private static class MaxIntervalsCountingConfiguration extends DefaultCairoConfiguration {
        int getterCallCount;

        MaxIntervalsCountingConfiguration() {
            super(".");
        }

        @Override
        public int getSqlIntervalMaxIntervalsAfterMerge() {
            getterCallCount++;
            return super.getSqlIntervalMaxIntervalsAfterMerge();
        }
    }

    // Records the largest capacity the list requests. Every growth path of LongList, such as add,
    // setPos and extendAndSet, calls checkCapacity.
    private static class PeakLongList extends LongList {
        int peak;

        @Override
        public void checkCapacity(int capacity) {
            peak = Math.max(peak, capacity);
            super.checkCapacity(capacity);
        }
    }
}
