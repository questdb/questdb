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
import io.questdb.griffin.model.IntervalOperation;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.griffin.model.RuntimeIntervalModelBuilder;
import io.questdb.griffin.model.RuntimeIntrinsicIntervalModel;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.TestTimestampType;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static io.questdb.test.griffin.GriffinParserTestUtils.intervalToString;

@RunWith(Parameterized.class)
public class TickExprTest {
    private static final String[] FUZZ_DAY_FILTERS = {"", "#Tue", "#workday", "#weekend"};
    private static final String[] FUZZ_DURATIONS = {"", ";1h", ";30m"};
    // time parts that the grammar rejects
    private static final String[] FUZZ_MALFORMED_TIMES = {"T[25:00]", "T25:00", "t09:00", "t[09:00]", "T[", "T"};
    // "now" on different days, weekdays and times of day, and on days when the clocks change
    private static final String[] FUZZ_NOWS = {
            "2024-01-02T10:00:00.000000Z", // a Tuesday
            "2024-01-02T15:00:00.000000Z",
            "2024-01-03T10:00:00.000000Z", // a Wednesday
            "2024-01-06T10:00:00.000000Z", // a Saturday
            "2024-02-29T23:59:00.000000Z", // the last minute of a leap day
            "2024-03-31T00:30:00.000000Z", // London springs forward at 01:00Z
            "2024-11-03T05:30:00.000000Z" // New York falls back at 06:00Z
    };
    private static final String[] FUZZ_RANGE_ENDS = {"$tomorrow", "$today + 3d", "$now", "$NOW + 1h", "$yesterday", "2024-01-09"};
    private static final String[] FUZZ_SHIFTS = {" + 1d", "+2h", " - 1bd", "-30m", "+1w", " - 1M", "+90s"};
    private static final String[] FUZZ_STATIC_ELEMENTS = {"2024-01-05", "2024-01-05T10:00", "2024-01", "2024-01-0[1..3]"};
    private static final String[] FUZZ_TIMES = {
            "", "T09:00", "T09", "T09:00:30", "T[09:00]", "T[09:00,14:30]", "T[09..10]:30", "T[01,02]:30",
            "T[10:30@+02:00]", "T[09:00,10:00]:[15,45]"
    };
    private static final String[] FUZZ_VARIABLES = {"$now", "$today", "$yesterday", "$tomorrow", "$NOW", "$TODAY", "$Tomorrow", "$YESTERDAY"};
    private static final String[] FUZZ_ZONES = {"", "@+02:00", "@Europe/London", "@America/New_York"};
    private static final Log LOG = LogFactory.getLog(TickExprTest.class);
    private static final CairoConfiguration configuration = new DefaultCairoConfiguration(".");
    private static final StringSink sink = new StringSink();
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
     * Convenience overload that allocates a temporary StringSink and always returns simple format.
     */
    public static void parseTickExpr(
            TimestampDriver timestampDriver,
            CharSequence seq,
            int lo,
            int lim,
            int position,
            LongList out,
            short operation
    ) throws SqlException {
        IntervalUtils.parseTickExpr(timestampDriver, configuration, seq, lo, lim, position, out, operation, new StringSink(), true);
    }

    /**
     * Overload that allows specifying applyEncoded parameter for testing encoded format paths.
     */
    public static void parseTickExpr(
            TimestampDriver timestampDriver,
            CharSequence seq,
            int lo,
            int lim,
            int position,
            LongList out,
            short operation,
            boolean applyEncoded
    ) throws SqlException {
        IntervalUtils.parseTickExpr(timestampDriver, configuration, seq, lo, lim, position, out, operation, new StringSink(), applyEncoded);
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
            short operation,
            long nowTimestamp
    ) throws SqlException {
        IntervalUtils.parseTickExpr(timestampDriver, configuration, seq, lo, lim, position, out, operation, new StringSink(), true, nowTimestamp);
    }

    /**
     * Overload that allows specifying both applyEncoded and nowTimestamp for testing date variables.
     */
    public static void parseTickExprWithNow(
            TimestampDriver timestampDriver,
            CharSequence seq,
            int lo,
            int lim,
            int position,
            LongList out,
            short operation,
            boolean applyEncoded,
            long nowTimestamp
    ) throws SqlException {
        IntervalUtils.parseTickExpr(timestampDriver, configuration, seq, lo, lim, position, out, operation, new StringSink(), applyEncoded, nowTimestamp);
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
        parseTickExpr(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
        Assert.assertEquals("Expected 4 intervals for 2x2 cartesian product", 4, out.size() / 2);

        // Test 2: Larger cartesian product with non-consecutive months
        // 2018-[01,03,05,07,09,11]-[05,10,15,20,25] = 6 months * 5 days = 30 combinations
        out.clear();
        interval = "2018-[01,03,05,07,09,11]-[05,10,15,20,25]";
        parseTickExpr(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
        Assert.assertEquals("Expected 30 intervals for 6x5 cartesian product", 30, out.size() / 2);

        // Test 3: Cartesian product with adjacent days within months
        // 2020-[01,02]-[01..28] = 2 months * 28 days = 56 combinations before merging
        // Days 1-28 within each month are adjacent and merge.
        // Jan 28 -> Feb 1 has gap (Jan 29,30,31), so months don't merge.
        out.clear();
        interval = "2020-[01,02]-[01..28]";
        parseTickExpr(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
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
        parseTickExpr(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
        Assert.assertEquals("201 non-adjacent year intervals", 201, out.size() / 2);

        // Test 3: Max interval limit (1024) enforced
        assertBracketIntervalError("[1000..3000]-01-01", "too many intervals");

        // Test 4: 12 months × 28 days merge to 12 intervals (one per month)
        out.clear();
        interval = "2020-[01,02,03,04,05,06,07,08,09,10,11,12]-[01..28]";
        parseTickExpr(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
        Assert.assertEquals("12 month intervals after merge", 12, out.size() / 2);
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
        parseTickExpr(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
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
    public void testBracketExpansionWithApplyEncodedFalse() throws SqlException {
        // Field expansion with applyEncoded=false - two separate day intervals
        assertEncodedInterval(
                "[{lo=2018-01-10T00:00:00.000000Z, hi=2018-01-10T23:59:59.999999Z}," +
                        "{lo=2018-01-15T00:00:00.000000Z, hi=2018-01-15T23:59:59.999999Z}]",
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
            parseTickExpr(timestampDriver, input, 0, input.length(), 0, out, IntervalOperation.INTERSECT);
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
        parseTickExpr(timestampDriver, input, lo, input.length(), 0, out, IntervalOperation.INTERSECT);
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
    public void testBracketExpansionWithSubtractApplyEncodedFalse() throws SqlException {
        // Test SUBTRACT operation with applyEncoded=false (exercises line 1167)
        final TimestampDriver timestampDriver = timestampType.getDriver();
        out.clear();
        String interval = "2018-01-[10,15]";
        parseTickExpr(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.SUBTRACT, false);
        // With applyEncoded=false, we get 4 longs per interval (encoded format)
        // 2 intervals * 4 longs = 8
        Assert.assertEquals(8, out.size());
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
        parseTickExprWithNow(timestampDriver, expression, 0, expression.length(), 0, out, IntervalOperation.INTERSECT, timestampDriver.parseFloorLiteral(now));
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
        parseTickExprWithNow(timestampDriver, expression, 0, expression.length(), 0, out, IntervalOperation.INTERSECT,
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
    public void testCompiledTickExprUppercaseTomorrow() throws SqlException {
        assertCompiledTickExpr("$TOMORROW");
    }

    @Test
    public void testCompiledTickExprUppercaseVariable() throws SqlException {
        assertCompiledTickExpr("$TODAY");
    }

    @Test
    public void testCompiledTickExprYesterdayWithOffset() throws SqlException {
        assertCompiledTickExpr("$yesterday + 3h");
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
    public void testDateListElementWithHighPrecisionTime() throws SqlException {
        // Element with microseconds keeps precise time, second element gets T10:00 suffix
        // Note: nano mode fills remaining nanos with 9s for hi (.123456999Z)
        out.clear();
        final TimestampDriver timestampDriver = timestampType.getDriver();
        String interval = "[2026-01-01T09:30:45.123456, 2026-01-02]T10:00";
        parseTickExpr(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
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
    public void testDateListImpreciseBareExpression() throws SqlException {
        // Bare imprecise date + time (no brackets): 2024-02T09:30
        // parseTickExpr wraps it as [2024-02]T09:30 and routes to expandDateList
        final TimestampDriver timestampDriver = timestampType.getDriver();
        String bare = "2024-02T09:30";
        String bracketed = "[2024-02]T09:30";
        out.clear();
        parseTickExpr(timestampDriver, bare, 0, bare.length(), 0, out, IntervalOperation.INTERSECT);
        String bareResult = intervalToString(timestampDriver, out).toString();
        out.clear();
        parseTickExpr(timestampDriver, bracketed, 0, bracketed.length(), 0, out, IntervalOperation.INTERSECT);
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
        parseTickExpr(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
        // January 31 + February 29 (leap year) = 60 days
        Assert.assertEquals(60, out.size() / 2);
    }

    @Test
    public void testDateListImpreciseBracketExpansionTimestamps() throws SqlException {
        // Verify actual timestamp values for bracket expansion, not just count
        final TimestampDriver timestampDriver = timestampType.getDriver();
        out.clear();
        String interval = "2024-[01..02]T09:30";
        parseTickExpr(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
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
        parseTickExpr(timestampDriver, imprecise, 0, imprecise.length(), 0, out, IntervalOperation.INTERSECT);
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
        parseTickExpr(timestampDriver, imprecise, 0, imprecise.length(), 0, out, IntervalOperation.INTERSECT);
        String impreciseResult = intervalToString(timestampDriver, out).toString();
        out.clear();
        parseTickExpr(timestampDriver, explicit, 0, explicit.length(), 0, out, IntervalOperation.INTERSECT);
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
        parseTickExpr(timestampDriver, imprecise, 0, imprecise.length(), 0, out, IntervalOperation.INTERSECT);
        String impreciseResult = intervalToString(timestampDriver, out).toString();
        out.clear();
        parseTickExpr(timestampDriver, explicit, 0, explicit.length(), 0, out, IntervalOperation.INTERSECT);
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
        parseTickExpr(timestampDriver, imprecise, 0, imprecise.length(), 0, out, IntervalOperation.INTERSECT);
        String impreciseResult = intervalToString(timestampDriver, out).toString();
        out.clear();
        parseTickExpr(timestampDriver, explicit, 0, explicit.length(), 0, out, IntervalOperation.INTERSECT);
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
        parseTickExpr(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
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
        parseTickExpr(timestampDriver, imprecise, 0, imprecise.length(), 0, out, IntervalOperation.INTERSECT);
        String impreciseResult = intervalToString(timestampDriver, out).toString();
        out.clear();
        parseTickExpr(timestampDriver, explicit, 0, explicit.length(), 0, out, IntervalOperation.INTERSECT);
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
        parseTickExpr(timestampDriver, precise, 0, precise.length(), 0, out, IntervalOperation.INTERSECT);
        String expected = intervalToString(timestampDriver, out).toString();

        // Imprecise month with day filter should produce same first 3 days
        // 2024-01-01 is Monday, 02 Tue, 03 Wed
        String imprecise = "[2024-01]T09:30@+05:00#Mon,Tue,Wed";
        out.clear();
        parseTickExpr(timestampDriver, imprecise, 0, imprecise.length(), 0, out, IntervalOperation.INTERSECT);
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
        parseTickExpr(timestampDriver, imprecise, 0, imprecise.length(), 0, out, IntervalOperation.INTERSECT);
        String impreciseResult = intervalToString(timestampDriver, out).toString();
        out.clear();
        parseTickExpr(timestampDriver, explicit, 0, explicit.length(), 0, out, IntervalOperation.INTERSECT);
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
        parseTickExpr(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
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
        parseTickExpr(timestampDriver, imprecise, 0, imprecise.length(), 0, out, IntervalOperation.INTERSECT);
        String impreciseResult = intervalToString(timestampDriver, out).toString();
        out.clear();
        parseTickExpr(timestampDriver, explicit, 0, explicit.length(), 0, out, IntervalOperation.INTERSECT);
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
        parseTickExpr(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
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
        parseTickExpr(timestampDriver, imprecise, 0, imprecise.length(), 0, out, IntervalOperation.INTERSECT);
        String impreciseResult = intervalToString(timestampDriver, out).toString();
        out.clear();
        parseTickExpr(timestampDriver, explicit, 0, explicit.length(), 0, out, IntervalOperation.INTERSECT);
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
        parseTickExpr(timestampDriver, imprecise, 0, imprecise.length(), 0, out, IntervalOperation.INTERSECT);
        Assert.assertEquals(366, out.size() / 2);
    }

    @Test
    public void testDateListImpreciseYearLevelNonLeap() throws SqlException {
        // Non-leap year: [2023]T09:30 should expand to 365 days
        final TimestampDriver timestampDriver = timestampType.getDriver();
        out.clear();
        String imprecise = "[2023]T09:30";
        parseTickExpr(timestampDriver, imprecise, 0, imprecise.length(), 0, out, IntervalOperation.INTERSECT);
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
        parseTickExpr(timestampDriver, imprecise, 0, imprecise.length(), 0, out, IntervalOperation.INTERSECT);
        String impreciseResult = intervalToString(timestampDriver, out).toString();
        out.clear();
        parseTickExpr(timestampDriver, explicit, 0, explicit.length(), 0, out, IntervalOperation.INTERSECT);
        String explicitResult = intervalToString(timestampDriver, out).toString();
        TestUtils.assertEquals(explicitResult, impreciseResult);
        // 2024 has 262 workdays
        Assert.assertEquals(262, out.size() / 2);
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
    public void testDateListWithApplyEncodedFalse() throws SqlException {
        // Test date list path with applyEncoded=false (exercises line 455 branch)
        // When applyEncoded=false, intervals stay in 4-long encoded format and union is skipped
        // 2 intervals * 4 longs = 8
        assertEncodedInterval(
                "[{lo=2025-01-01T00:00:00.000000Z, hi=2025-01-01T23:59:59.999999Z},{lo=2025-01-05T00:00:00.000000Z, hi=2025-01-05T23:59:59.999999Z}]",
                "[2025-01-01,2025-01-05]"
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
    public void testDateListWithDayFilterApplyEncodedFalse() throws SqlException {
        // Test date list path with day filter and applyEncoded=false (exercises line 1617)
        // 2024-01-01 is Monday, 2024-01-02 is Tuesday, 2024-01-03 is Wednesday
        // Day filter mask (Monday = bit 0 = 1) is encoded on all intervals
        assertEncodedInterval(
                "[{lo=2024-01-01T00:00:00.000000Z, hi=2024-01-01T23:59:59.999999Z, dayFilter=Mon},{lo=2024-01-02T00:00:00.000000Z, hi=2024-01-02T23:59:59.999999Z, dayFilter=Mon},{lo=2024-01-03T00:00:00.000000Z, hi=2024-01-03T23:59:59.999999Z, dayFilter=Mon}]",
                "[2024-01-01,2024-01-02,2024-01-03]#Mon"
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
        parseTickExpr(timestampDriver, expression, 0, expression.length(), 0, out, IntervalOperation.INTERSECT);
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
        // 2024-01-03 at -05:00 runs from 2024-01-03T05:00Z to 2024-01-04T04:59:59.999999Z. The
        // first element covers its seconds up to the end of 2024-01-03 in UTC, not in local time,
        // so the 300 seconds from 19:00 local remain on 2024-01-04. The first of them touches
        // the end of the first element and joins it.
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final String expression = "[2024-01-01T00:00;3d, 2024-01-03@-05:00]T[00..23]:[00..59]:00";
        out.clear();
        parseTickExpr(timestampDriver, expression, 0, expression.length(), 0, out, IntervalOperation.INTERSECT);

        final LongList expected = new LongList();
        final long nextDay = timestampDriver.parseFloorLiteral("2024-01-04T00:00:00.000000Z");
        expected.add(timestampDriver.parseFloorLiteral("2024-01-01T00:00:00.000000Z"), nextDay + timestampDriver.fromSeconds(1) - 1);
        for (int minute = 1; minute < 5 * 60; minute++) {
            final long lo = nextDay + timestampDriver.fromMinutes(minute);
            expected.add(lo, lo + timestampDriver.fromSeconds(1) - 1);
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
    public void testDateListWithNestedTimeExpansionEncoded() throws SqlException {
        assertEncodedInterval(
                "[{lo=2024-01-15T09:00:00.000000Z, hi=2024-01-15T09:00:00.999999Z},{lo=2024-01-15T09:00:30.000000Z, hi=2024-01-15T09:00:30.999999Z},{lo=2024-01-15T14:30:00.000000Z, hi=2024-01-15T14:30:00.999999Z},{lo=2024-01-15T14:30:30.000000Z, hi=2024-01-15T14:30:30.999999Z},{lo=2024-01-16T09:00:00.000000Z, hi=2024-01-16T09:00:00.999999Z},{lo=2024-01-16T09:00:30.000000Z, hi=2024-01-16T09:00:30.999999Z},{lo=2024-01-16T14:30:00.000000Z, hi=2024-01-16T14:30:00.999999Z},{lo=2024-01-16T14:30:30.000000Z, hi=2024-01-16T14:30:30.999999Z}]",
                "[2024-01-15,2024-01-16]T[09:00,14:30]:[00,30]"
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
    public void testDateVariableArithmeticBusinessDays() throws SqlException {
        // 2026-01-22 is Thursday
        final TimestampDriver timestampDriver = timestampType.getDriver();
        long now = timestampDriver.parseFloorLiteral("2026-01-22T10:30:00.000000Z");
        out.clear();

        // [$today + 1bd] should be Friday 2026-01-23
        String interval = "[$today + 1bd]";
        parseTickExprWithNow(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT, now);
        TestUtils.assertEquals(
                ColumnType.isTimestampNano(timestampType.getTimestampType())
                        ? "[{lo=2026-01-23T00:00:00.000000000Z, hi=2026-01-23T23:59:59.999999999Z}]"
                        : "[{lo=2026-01-23T00:00:00.000000Z, hi=2026-01-23T23:59:59.999999Z}]",
                intervalToString(timestampDriver, out)
        );

        out.clear();
        // [$today + 2bd] should be Monday 2026-01-26 (skip weekend)
        interval = "[$today + 2bd]";
        parseTickExprWithNow(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT, now);
        TestUtils.assertEquals(
                ColumnType.isTimestampNano(timestampType.getTimestampType())
                        ? "[{lo=2026-01-26T00:00:00.000000000Z, hi=2026-01-26T23:59:59.999999999Z}]"
                        : "[{lo=2026-01-26T00:00:00.000000Z, hi=2026-01-26T23:59:59.999999Z}]",
                intervalToString(timestampDriver, out)
        );

        out.clear();
        // [$today - 1bd] should be Wednesday 2026-01-21
        interval = "[$today - 1bd]";
        parseTickExprWithNow(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT, now);
        TestUtils.assertEquals(
                ColumnType.isTimestampNano(timestampType.getTimestampType())
                        ? "[{lo=2026-01-21T00:00:00.000000000Z, hi=2026-01-21T23:59:59.999999999Z}]"
                        : "[{lo=2026-01-21T00:00:00.000000Z, hi=2026-01-21T23:59:59.999999Z}]",
                intervalToString(timestampDriver, out)
        );

        out.clear();
        // [$today + 0bd] should be today 2026-01-22
        interval = "[$today + 0bd]";
        parseTickExprWithNow(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT, now);
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
        parseTickExprWithNow(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT, now);
        TestUtils.assertEquals(
                ColumnType.isTimestampNano(timestampType.getTimestampType())
                        ? "[{lo=2026-01-27T00:00:00.000000000Z, hi=2026-01-27T23:59:59.999999999Z}]"
                        : "[{lo=2026-01-27T00:00:00.000000Z, hi=2026-01-27T23:59:59.999999Z}]",
                intervalToString(timestampDriver, out)
        );

        out.clear();
        // [$today - 3d] should be 2026-01-19
        interval = "[$today - 3d]";
        parseTickExprWithNow(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT, now);
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
    public void testDateVariableBareWithApplyEncodedFalse() throws SqlException {
        // Bare date variable with applyEncoded=false - full day interval
        assertEncodedIntervalWithNow(
                "[{lo=2026-03-15T00:00:00.000000Z, hi=2026-03-15T23:59:59.999999Z}]",
                "$today",
                "2026-03-15T14:30:00.000000Z"
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
        parseTickExprWithNow(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT, now);
        TestUtils.assertEquals(
                ColumnType.isTimestampNano(timestampType.getTimestampType())
                        ? "[{lo=2026-01-26T00:00:00.000000000Z, hi=2026-01-26T23:59:59.999999999Z}]"
                        : "[{lo=2026-01-26T00:00:00.000000Z, hi=2026-01-26T23:59:59.999999Z}]",
                intervalToString(timestampDriver, out)
        );

        out.clear();
        // From Saturday, -1bd should be Friday 2026-01-23
        interval = "[$today - 1bd]";
        parseTickExprWithNow(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT, now);
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
        parseTickExprWithNow(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT, now);
        TestUtils.assertEquals(
                ColumnType.isTimestampNano(timestampType.getTimestampType())
                        ? "[{lo=2026-01-22T00:00:00.000000000Z, hi=2026-01-22T23:59:59.999999999Z}]"
                        : "[{lo=2026-01-22T00:00:00.000000Z, hi=2026-01-22T23:59:59.999999Z}]",
                intervalToString(timestampDriver, out)
        );

        out.clear();
        interval = "[$ToDay + 1BD]";
        parseTickExprWithNow(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT, now);
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
        final DateVariableDetectionChecker checker = new DateVariableDetectionChecker(timestampType.getDriver());
        // every bare date variable followed by every suffix, where the end of the variable name meets
        // the start of the suffix
        for (String variable : FUZZ_VARIABLES) {
            for (String time : FUZZ_TIMES) {
                for (String zone : FUZZ_ZONES) {
                    for (String dayFilter : FUZZ_DAY_FILTERS) {
                        for (String duration : FUZZ_DURATIONS) {
                            checker.check(variable + time + zone + dayFilter + duration, false);
                        }
                    }
                }
            }
            for (String time : FUZZ_MALFORMED_TIMES) {
                for (String zone : FUZZ_ZONES) {
                    checker.check(variable + time + zone, true);
                }
            }
        }
        // and random expressions of every shape
        final Rnd rnd = TestUtils.generateRandom(LOG);
        final StringSink expression = new StringSink();
        for (int i = 0; i < 10_000; i++) {
            expression.clear();
            final boolean isMalformed = appendRandomTickExpr(rnd, expression);
            checker.check(expression.toString(), isMalformed);
        }
        checker.assertNoFailures();
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
    public void testDateVariableRangeWithDayFilterAndApplyEncodedFalse() throws SqlException {
        // Day range with #Mon filter, applyEncoded=false - all 7 days stored with Mon mask
        // 2026-02-09 is Monday, range covers Mon-Sun with #Fri filter stored on each
        assertEncodedIntervalWithNow(
                "[{lo=2026-02-09T00:00:00.000000Z, hi=2026-02-09T23:59:59.999999Z, dayFilter=Fri}," +
                        "{lo=2026-02-10T00:00:00.000000Z, hi=2026-02-10T23:59:59.999999Z, dayFilter=Fri}," +
                        "{lo=2026-02-11T00:00:00.000000Z, hi=2026-02-11T23:59:59.999999Z, dayFilter=Fri}," +
                        "{lo=2026-02-12T00:00:00.000000Z, hi=2026-02-12T23:59:59.999999Z, dayFilter=Fri}," +
                        "{lo=2026-02-13T00:00:00.000000Z, hi=2026-02-13T23:59:59.999999Z, dayFilter=Fri}," +
                        "{lo=2026-02-14T00:00:00.000000Z, hi=2026-02-14T23:59:59.999999Z, dayFilter=Fri}," +
                        "{lo=2026-02-15T00:00:00.000000Z, hi=2026-02-15T23:59:59.999999Z, dayFilter=Fri}]",
                "[$today..$today+6d]#Fri",
                "2026-02-09T10:30:00.000000Z"
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
        // dynamic mode keeps the range's day filter on each day for runtime evaluation
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final String interval = "[$today..$today+7d#Mon]";
        out.clear();
        parseTickExprWithNow(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT, false, timestampDriver.parseFloorLiteral(now));
        Assert.assertEquals(8 * IntervalUtils.STATIC_LONGS_PER_DYNAMIC_INTERVAL, out.size());
        for (int i = 0; i < out.size(); i += IntervalUtils.STATIC_LONGS_PER_DYNAMIC_INTERVAL) {
            Assert.assertEquals(1, IntervalUtils.decodeDayFilterMask(out, i)); // Monday
        }
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
    public void testDateVariableRangeWithHoursTimezoneAndApplyEncodedFalse() throws SqlException {
        // Time-precision with timezone and applyEncoded=false
        // 16:45-18:45 local Tokyo (UTC+9) → 07:45-09:45 UTC
        assertEncodedIntervalWithNow(
                "[{lo=2026-04-10T07:45:00.000000Z, hi=2026-04-10T09:45:00.000000Z}]",
                "[$now - 2h..$now]@Asia/Tokyo",
                "2026-04-10T18:45:00.000000Z"
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
        parseTickExprWithNow(timestampDriver, expression, 0, expression.length(), 0, out, IntervalOperation.INTERSECT, now);

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
        parseTickExprWithNow(timestampDriver, expression, 0, expression.length(), 0, out, IntervalOperation.INTERSECT,
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
    public void testDateVariableRangeWithNowAndTimeListApplyEncodedFalse() throws SqlException {
        // https://github.com/questdb/questdb/issues/8083
        // $now (10:30) drops today's 09:00. In dynamic mode the first interval carries the
        // operation and the rest carry UNION, so today's 11:00 takes over INTERSECT.
        assertEncodedIntervalWithNow(
                "[{lo=2026-01-22T11:00:00.000000Z, hi=2026-01-22T11:00:59.999999Z}," +
                        "{lo=2026-01-23T09:00:00.000000Z, hi=2026-01-23T09:00:59.999999Z}," +
                        "{lo=2026-01-23T11:00:00.000000Z, hi=2026-01-23T11:00:59.999999Z}]",
                "[$now..$tomorrow]T[09:00,11:00]",
                "2026-01-22T10:30:00.000000Z"
        );
        Assert.assertEquals(IntervalOperation.INTERSECT, IntervalUtils.getEncodedOperation(out, 0));
        Assert.assertEquals(IntervalOperation.UNION, IntervalUtils.getEncodedOperation(out, IntervalUtils.STATIC_LONGS_PER_DYNAMIC_INTERVAL));
        Assert.assertEquals(IntervalOperation.UNION, IntervalUtils.getEncodedOperation(out, 2 * IntervalUtils.STATIC_LONGS_PER_DYNAMIC_INTERVAL));
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
    public void testDateVariableRangeWithNowAndTimeListInDstGapApplyEncodedFalse() throws SqlException {
        // Dynamic mode clips the slots before converting them too. $now - 2h = 02:00 local is the
        // first moment after the hour that London clocks skip on 2024-03-31. It drops today's 00:30
        // and 01:30, so today's 02:30 BST (01:30Z) takes over INTERSECT.
        assertEncodedIntervalWithNow(
                "[{lo=2024-03-31T01:30:00.000000Z, hi=2024-03-31T01:30:59.999999Z}," +
                        "{lo=2024-03-31T23:30:00.000000Z, hi=2024-03-31T23:30:59.999999Z}," +
                        "{lo=2024-04-01T00:30:00.000000Z, hi=2024-04-01T00:30:59.999999Z}," +
                        "{lo=2024-04-01T01:30:00.000000Z, hi=2024-04-01T01:30:59.999999Z}]",
                "[$now - 2h..$tomorrow]T[00:30,01:30,02:30]@Europe/London",
                "2024-03-31T04:00:00.000000Z"
        );
        assertEncodedOperations(4);

        // $now - 2h = 01:20 local does not exist, so the range starts at the transition, 01:00Z.
        // 09:00 in Tokyo today is 00:00Z, before it, so the slot with its own timezone drops out
        // and today's 02:30 BST (01:30Z) takes over INTERSECT.
        assertEncodedIntervalWithNow(
                "[{lo=2024-03-31T01:30:00.000000Z, hi=2024-03-31T01:30:59.999999Z}," +
                        "{lo=2024-04-01T00:00:00.000000Z, hi=2024-04-01T00:00:59.999999Z}," +
                        "{lo=2024-04-01T01:30:00.000000Z, hi=2024-04-01T01:30:59.999999Z}]",
                "[$now - 2h..$tomorrow]T[09:00@Asia/Tokyo,02:30]@Europe/London",
                "2024-03-31T03:20:00.000000Z"
        );
        assertEncodedOperations(3);
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
    public void testDateVariableRangeWithNowAndTimeListMultipleIntervalsPerSlotApplyEncodedFalse() throws SqlException {
        // Each slot expands to two intervals, at :00 and :30 seconds. $now = 09:00:15 drops the
        // first interval of today's slot, so the second one takes over INTERSECT.
        assertEncodedIntervalWithNow(
                "[{lo=2026-01-22T09:00:30.000000Z, hi=2026-01-22T09:00:30.999999Z}," +
                        "{lo=2026-01-23T09:00:00.000000Z, hi=2026-01-23T09:00:00.999999Z}," +
                        "{lo=2026-01-23T09:00:30.000000Z, hi=2026-01-23T09:00:30.999999Z}]",
                "[$now..$tomorrow]T[09:00]:[00,30]",
                "2026-01-22T09:00:15.000000Z"
        );
        assertEncodedOperations(3);
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
    public void testDateVariableRangeWithNowRepeatingIntervalApplyEncodedFalse() throws SqlException {
        // Dynamic mode keeps a repeating interval as one encoded interval, and clipping it
        // to $now would need its repetitions expanded
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final long now = timestampDriver.parseFloorLiteral("2026-01-22T10:30:00.000000Z");
        // a time list clips its slots itself, and rejects a repeating slot the same way, with
        // and without a timezone of its own
        for (String interval : new String[]{
                "[$now..$tomorrow];1h;1d;2",
                "[$now..$tomorrow]T[09:00];1h;1d;2",
                "[$now..$tomorrow]T[09:00@Asia/Tokyo];1h;1d;2"
        }) {
            try {
                parseTickExprWithNow(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT, false, now);
                Assert.fail("Expected SqlException for " + interval);
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "Repeating interval is not supported in a date variable range with a time of day");
            }
        }
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
    public void testDateVariableRangeWithTimePrecisionAndApplyEncodedFalse() throws SqlException {
        // Time-precision range with applyEncoded=false, no timezone
        assertEncodedIntervalWithNow(
                "[{lo=2026-07-04T19:00:00.000000Z, hi=2026-07-04T21:00:00.000000Z}]",
                "[$now - 2h..$now]",
                "2026-07-04T21:00:00.000000Z"
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
        parseTickExpr(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);

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
        parseTickExpr(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
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
        parseTickExpr(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);

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
    public void testDayFilterDynamicModeEncoding() throws SqlException {
        // Test that day filter mask is properly encoded in dynamic (4-long) format
        // This verifies the encoding, not the runtime filtering
        final TimestampDriver timestampDriver = timestampType.getDriver();
        out.clear();
        String interval = "2024-01-[01..03]#Mon";
        parseTickExpr(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT, false);

        // With applyEncoded=false, we get 4 longs per interval (encoded format)
        // 3 intervals (01, 02, 03) * 4 longs = 12
        Assert.assertEquals(12, out.size());

        // Verify day filter mask is encoded (Monday = bit 0 = 1)
        int dayFilterMask0 = IntervalUtils.decodeDayFilterMask(out, 0);
        int dayFilterMask1 = IntervalUtils.decodeDayFilterMask(out, 4);
        int dayFilterMask2 = IntervalUtils.decodeDayFilterMask(out, 8);
        Assert.assertEquals("Day filter mask should be 1 (Monday)", 1, dayFilterMask0);
        Assert.assertEquals("Day filter mask should be 1 (Monday)", 1, dayFilterMask1);
        Assert.assertEquals("Day filter mask should be 1 (Monday)", 1, dayFilterMask2);
    }

    @Test
    public void testDayFilterDynamicModeEncodingWeekend() throws SqlException {
        // Weekend mask = Sat|Sun encoded in interval
        assertEncodedInterval(
                "[{lo=2024-01-01T00:00:00.000000Z, hi=2024-01-01T23:59:59.999999Z, dayFilter=Sat,Sun}]",
                "2024-01-01#weekend"
        );
    }

    @Test
    public void testDayFilterDynamicModeEncodingWorkday() throws SqlException {
        // Workday mask = Mon-Fri encoded in interval
        assertEncodedInterval(
                "[{lo=2024-01-01T00:00:00.000000Z, hi=2024-01-01T23:59:59.999999Z, dayFilter=Mon,Tue,Wed,Thu,Fri}]",
                "2024-01-01#workday"
        );
    }

    @Test
    public void testDayFilterEmptyResult() throws SqlException {
        // Filter that removes all intervals (e.g., looking for Monday in a Tue-Thu range)
        // 2024-01-02 is Tuesday, 2024-01-04 is Thursday
        final TimestampDriver timestampDriver = timestampType.getDriver();
        out.clear();
        String interval = "2024-01-[02..04]#Mon";
        parseTickExpr(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
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
        parseTickExpr(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);

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
        parseTickExpr(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
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
    public void testDayFilterWithTimezoneApplyEncodedFalse() throws SqlException {
        // Dynamic mode: day filter mask stored for runtime evaluation
        // Friday 2026-01-30 14:00 local NY → 19:00 UTC, #Fri matches
        assertEncodedIntervalWithNow(
                "[{lo=2026-01-30T18:00:00.000000Z, hi=2026-01-30T19:00:00.000000Z, dayFilter=Fri}]",
                "[$now - 1h..$now]@America/New_York#Fri",
                "2026-01-30T14:00:00.000000Z"
        );
    }

    @Test
    public void testDayFilterWithTimezoneApplyEncodedFalseMidnightCrossing() throws SqlException {
        // Dynamic mode: late Saturday night local crosses to Sunday UTC
        // Saturday 2026-01-31 23:30 local NY → Sunday 04:30 UTC
        // Day filter checks LOCAL day (Saturday) → #Sat matches
        assertEncodedIntervalWithNow(
                "[{lo=2026-02-01T03:30:00.000000Z, hi=2026-02-01T04:30:00.000000Z, dayFilter=Sat}]",
                "[$now - 1h..$now]@America/New_York#Sat",
                "2026-01-31T23:30:00.000000Z"
        );
    }

    @Test
    public void testDayFilterWithTimezoneApplyEncodedFalseNoMatch() throws SqlException {
        // Dynamic mode: Wednesday 2026-01-28 09:15 local, #Thu doesn't match but mask stored
        assertEncodedIntervalWithNow(
                "[{lo=2026-01-28T12:15:00.000000Z, hi=2026-01-28T14:15:00.000000Z, dayFilter=Thu}]",
                "[$now - 2h..$now]@America/New_York#Thu",
                "2026-01-28T09:15:00.000000Z"
        );
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
        parseTickExpr(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);

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
        parseTickExpr(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);

        // Should have 53 Saturdays (53 intervals = 106 longs)
        Assert.assertEquals(106, out.size());

        // First Saturday is 2022-01-01
        String result = intervalToString(timestampDriver, out).toString();
        Assert.assertTrue("Should start with Jan 1", result.contains("2022-01-01T00:00:00"));

        // Last Saturday is 2022-12-31
        Assert.assertTrue("Should end with Dec 31", result.contains("2022-12-31T"));
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
        parseTickExpr(timestampDriver, intervalStr, 0, intervalStr.length(), 0, out, IntervalOperation.INTERSECT);
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
    public void testNoBracketsWithApplyEncodedFalse() throws SqlException {
        // No-brackets path with applyEncoded=false
        assertEncodedInterval(
                "[{lo=2025-01-15T00:00:00.000000Z, hi=2025-01-15T23:59:59.999999Z}]",
                "2025-01-15"
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
                        0, out, IntervalOperation.INTERSECT, scratch, true);
            }
            long minAllocatedBytes = Long.MAX_VALUE;
            long intervalCount = 0;
            for (int round = 0; round < 5; round++) {
                final long allocatedBefore = threadMXBean.getCurrentThreadAllocatedBytes();
                for (int i = 0; i < 10_000; i++) {
                    out.clear();
                    IntervalUtils.parseTickExpr(timestampDriver, configuration, interval, 0, interval.length(),
                            0, out, IntervalOperation.INTERSECT, scratch, true);
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
        parseTickExpr(timestampDriver, expression, 0, expression.length(), 0, out, IntervalOperation.INTERSECT);
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
    public void testTimezoneAtSignInsideBracketsIgnored() {
        // Test that @ inside brackets is NOT treated as timezone marker - exercises L1543 depth check
        // The string "2024-01-[15@00,16]" has @ inside brackets - should fail as invalid bracket content
        // not as invalid timezone
        final TimestampDriver timestampDriver = timestampType.getDriver();
        out.clear();
        String interval = "2024-01-[15@00,16]";
        try {
            parseTickExpr(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
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
        // in dynamic mode, the minute after the dropped one carries the operation
        assertEncodedInterval("[{lo=2024-03-10T07:30:00.000000Z, hi=2024-03-10T07:30:59.999999Z}]", "2024-03-10T[02:30,03:30]@America/New_York");
        assertEncodedOperations(1);
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
    public void testTimezoneDstOverlapApplyEncodedFalse() throws SqlException {
        // Dynamic mode keeps both occurrences of a repeated local time: the first interval
        // carries the operation, the second UNION
        assertEncodedInterval(
                "[{lo=2024-11-03T05:30:00.000000Z, hi=2024-11-03T05:30:59.999999Z},{lo=2024-11-03T06:30:00.000000Z, hi=2024-11-03T06:30:59.999999Z}]",
                "2024-11-03T01:30@America/New_York"
        );
        assertEncodedOperations(2);
        // a subtraction subtracts both
        final TimestampDriver timestampDriver = timestampType.getDriver();
        out.clear();
        parseTickExpr(timestampDriver, "2024-11-03T01:30@America/New_York", 0, 33, 0, out, IntervalOperation.SUBTRACT, false);
        Assert.assertEquals(2 * IntervalUtils.STATIC_LONGS_PER_DYNAMIC_INTERVAL, out.size());
        Assert.assertEquals(IntervalOperation.SUBTRACT, IntervalUtils.getEncodedOperation(out, 0));
        Assert.assertEquals(IntervalOperation.SUBTRACT, IntervalUtils.getEncodedOperation(out, IntervalUtils.STATIC_LONGS_PER_DYNAMIC_INTERVAL));
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
    public void testTimezoneEncodedFormat() throws SqlException {
        // Test timezone conversion with encoded format (applyEncoded=false, stride=4)
        // This exercises the isStaticMode=false branch in applyTimezoneToIntervals
        // 08:00 in +03:00 = 05:00 UTC
        final TimestampDriver timestampDriver = timestampType.getDriver();
        out.clear();
        parseTickExpr(timestampDriver, "2024-01-15T08:00@+03:00", 0, 23, 0, out, IntervalOperation.INTERSECT, false);
        IntervalUtils.applyLastEncodedInterval(timestampDriver, out);
        String expected = ColumnType.isTimestampNano(timestampType.getTimestampType())
                ? "[{lo=2024-01-15T05:00:00.000000000Z, hi=2024-01-15T05:00:59.999999999Z}]"
                : "[{lo=2024-01-15T05:00:00.000000Z, hi=2024-01-15T05:00:59.999999Z}]";
        TestUtils.assertEquals(expected, intervalToString(timestampDriver, out));
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
                parseTickExpr(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
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
            parseTickExpr(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
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
    private static boolean appendRandomTickExpr(Rnd rnd, StringSink sink) {
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
                        // a time part on an element rather than after the list, which the grammar
                        // rejects on a date variable and on a date without a day
                        sink.put(pick(rnd, FUZZ_TIMES));
                        isMalformed = true;
                    }
                    default -> {
                    }
                }
            }
            sink.put(']');
        }
        if (rnd.nextInt(10) == 0) {
            sink.put(pick(rnd, FUZZ_MALFORMED_TIMES));
            isMalformed = true;
        } else {
            sink.put(pick(rnd, FUZZ_TIMES));
        }
        sink.put(pick(rnd, FUZZ_ZONES));
        final String dayFilter = pick(rnd, FUZZ_DAY_FILTERS);
        final String duration = pick(rnd, FUZZ_DURATIONS);
        if (rnd.nextInt(10) == 0) {
            // the day filter goes before the duration
            sink.put(duration).put(dayFilter);
            isMalformed |= !dayFilter.isEmpty() && !duration.isEmpty();
        } else {
            sink.put(dayFilter).put(duration);
        }
        return isMalformed;
    }

    /**
     * Converts encoded intervals (4-long format) to a readable string for assertions.
     * Format: [{lo=..., hi=..., dayFilter=Mon,Tue,...}]
     */
    private static CharSequence encodedIntervalToSink(TimestampDriver driver, LongList intervals) {
        sink.clear();
        sink.put('[');
        for (int i = 0, n = intervals.size(); i < n; i += 4) {
            if (i > 0) {
                sink.put(',');
            }
            sink.put('{');
            sink.put("lo=");
            driver.append(sink, intervals.getQuick(i));
            sink.put(", hi=");
            driver.append(sink, intervals.getQuick(i + 1));

            // Extract dayFilterMask from periodCount (high byte of high int)
            long periodCountLong = intervals.getQuick(i + 3);
            int periodCount = Numbers.decodeHighInt(periodCountLong);
            int dayFilterMask = (periodCount >> 24) & 0xFF;
            if (dayFilterMask != 0) {
                sink.put(", dayFilter=");
                boolean first = true;
                String[] days = {"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"};
                for (int d = 0; d < 7; d++) {
                    if ((dayFilterMask & (1 << d)) != 0) {
                        if (!first) sink.put(',');
                        sink.put(days[d]);
                        first = false;
                    }
                }
            }
            sink.put('}');
        }
        sink.put(']');
        return sink;
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

    private static DateVariableExpr parseDateVar(CharSequence seq, int hi) throws SqlException {
        long encoded = DateVariableExpr.parseEncoded(seq, 0, hi, 0);
        return new DateVariableExpr(
                (byte) ((encoded >>> 58) & 0x3),
                (char) ((encoded >>> 40) & 0xFFFF),
                (int) encoded,
                (encoded & (1L << 57)) != 0
        );
    }

    private static String pick(Rnd rnd, String[] values) {
        return values[rnd.nextInt(values.length)];
    }

    private void assertBracketInterval(String expected, String interval) throws SqlException {
        out.clear();
        final TimestampDriver timestampDriver = timestampType.getDriver();
        parseTickExpr(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
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
            parseTickExpr(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
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
        parseTickExprWithNow(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT, now);
        TestUtils.assertEquals(
                ColumnType.isTimestampNano(timestampType.getTimestampType())
                        ? expected.replace("00Z", "00000Z").replace("99Z", "99999Z")
                        : expected,
                intervalToString(timestampDriver, out)
        );
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
                        staticResult, IntervalOperation.INTERSECT, randomNow);

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
                    parseTickExprWithNow(timestampDriver, expression, 0, expression.length(), 0, parsed, IntervalOperation.INTERSECT, now);
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

    private void assertCompiledTickExprWithNow(String expression, String nowStr) throws SqlException {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        long now = timestampDriver.parseFloorLiteral(nowStr);

        try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(
                timestampDriver, configuration, expression, 0, expression.length(), 0)) {
            LongList dynamicResult = new LongList();
            LongList staticResult = new LongList();

            compiled.evaluate(dynamicResult, now);
            parseTickExprWithNow(timestampDriver, expression, 0, expression.length(), 0,
                    staticResult, IntervalOperation.INTERSECT, now);

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
        parseTickExprWithNow(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT, randomNow);

        String expected = expectedBuilder.build(randomNow, timestampDriver, timestampType);
        TestUtils.assertEquals(
                ColumnType.isTimestampNano(timestampType.getTimestampType())
                        ? expected.replace("00Z", "00000Z").replace("99Z", "99999Z")
                        : expected,
                intervalToString(timestampDriver, out)
        );
    }

    /**
     * Asserts encoded interval (4-long format, applyEncoded=false) for static intervals.
     * Use this for testing dynamic mode where day filter is stored for runtime evaluation.
     */
    private void assertEncodedInterval(String expected, String interval) throws SqlException {
        out.clear();
        final TimestampDriver timestampDriver = timestampType.getDriver();
        parseTickExpr(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT, false);
        TestUtils.assertEquals(
                ColumnType.isTimestampNano(timestampType.getTimestampType())
                        ? expected.replace("00Z", "00000Z").replace("99Z", "99999Z")
                        : expected,
                encodedIntervalToSink(timestampDriver, out)
        );
    }

    /**
     * Asserts encoded interval (4-long format, applyEncoded=false) with a specific "now" timestamp.
     * Use this for testing dynamic mode where day filter is stored for runtime evaluation.
     */
    private void assertEncodedIntervalWithNow(String expected, String interval, String nowTimestamp) throws SqlException {
        out.clear();
        final TimestampDriver timestampDriver = timestampType.getDriver();
        long now = timestampDriver.parseFloorLiteral(nowTimestamp);
        parseTickExprWithNow(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT, false, now);
        TestUtils.assertEquals(
                ColumnType.isTimestampNano(timestampType.getTimestampType())
                        ? expected.replace("00Z", "00000Z").replace("99Z", "99999Z")
                        : expected,
                encodedIntervalToSink(timestampDriver, out)
        );
    }

    /**
     * Asserts that {@link #out} holds {@code intervalCount} encoded intervals, the first of which
     * carries INTERSECT and the rest UNION.
     */
    private void assertEncodedOperations(int intervalCount) {
        Assert.assertEquals(intervalCount * IntervalUtils.STATIC_LONGS_PER_DYNAMIC_INTERVAL, out.size());
        Assert.assertEquals(IntervalOperation.INTERSECT, IntervalUtils.getEncodedOperation(out, 0));
        for (int i = 1; i < intervalCount; i++) {
            Assert.assertEquals(IntervalOperation.UNION, IntervalUtils.getEncodedOperation(out, i * IntervalUtils.STATIC_LONGS_PER_DYNAMIC_INTERVAL));
        }
    }

    /**
     * Asserts that an imprecise tick expression produces the same intervals
     * as its explicit day-range equivalent.
     */
    private void assertImpreciseEquivalence(String imprecise, String explicit) throws SqlException {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        out.clear();
        parseTickExpr(timestampDriver, imprecise, 0, imprecise.length(), 0, out, IntervalOperation.INTERSECT);
        String impreciseResult = intervalToString(timestampDriver, out).toString();
        int impreciseCount = out.size() / 2;
        out.clear();
        parseTickExpr(timestampDriver, explicit, 0, explicit.length(), 0, out, IntervalOperation.INTERSECT);
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
            parseTickExpr(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
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
     * Asserts that both the parse path ({@code parseTickExpr}) and the compiled path
     * ({@link CompiledTickExpression#evaluate}) produce the expected intervals for the given "now".
     */
    private void assertParsedAndCompiledErrorWithNow(String expression, String nowTimestamp, String expectedError) throws SqlException {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final long now = timestampDriver.parseFloorLiteral(nowTimestamp);
        out.clear();
        try {
            parseTickExprWithNow(timestampDriver, expression, 0, expression.length(), 0, out, IntervalOperation.INTERSECT, now);
            Assert.fail("parse should fail: " + expression);
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), expectedError);
        }
        out.clear();
        try (CompiledTickExpression compiled = IntervalUtils.compileTickExpr(
                timestampDriver, configuration, expression, 0, expression.length(), 0)) {
            compiled.evaluate(out, now);
            Assert.fail("evaluation should fail: " + expression);
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), expectedError);
        }
    }

    private void assertParsedAndCompiledWithNow(String expected, String expression, String nowTimestamp) throws SqlException {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final long now = timestampDriver.parseFloorLiteral(nowTimestamp);
        final String expectedForType = ColumnType.isTimestampNano(timestampType.getTimestampType())
                ? expected.replace("00Z", "00000Z").replace("99Z", "99999Z")
                : expected;

        out.clear();
        parseTickExprWithNow(timestampDriver, expression, 0, expression.length(), 0, out, IntervalOperation.INTERSECT, now);
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

    private void assertShortInterval(String expected, String interval) throws SqlException {
        out.clear();
        final TimestampDriver timestampDriver = timestampType.getDriver();
        parseTickExpr(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
        TestUtils.assertEquals(
                ColumnType.isTimestampNano(timestampType.getTimestampType())
                        ? expected.replace("00Z", "00000Z").replace("99Z", "99999Z")
                        : expected,
                intervalToString(timestampDriver, out)
        );
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
     * The two validate a malformed expression in different ways. The parser ignores the time part
     * of a point in time, so it lets {@code $nowT[25:00]} through where the compiled form rejects it,
     * and the compiled form ignores the time part of a static element in a date list with a time of
     * its own, which the parser rejects. The checker therefore only requires a detected date variable
     * of a malformed expression.
     */
    private static class DateVariableDetectionChecker {
        private static final String ERROR = "error";
        private static final int MAX_REPORTED_FAILURES = 20;
        private final StringSink failures = new StringSink();
        private final LongList intervals = new LongList();
        private final long[] nows = new long[FUZZ_NOWS.length];
        private final StringSink parseSink = new StringSink();
        private final String[] parsed = new String[FUZZ_NOWS.length];
        private final TimestampDriver timestampDriver;
        private int checkCount;
        private int compiledCount;
        private int failureCount;
        private int nowDependentCount;

        DateVariableDetectionChecker(TimestampDriver timestampDriver) {
            this.timestampDriver = timestampDriver;
            for (int i = 0; i < FUZZ_NOWS.length; i++) {
                nows[i] = timestampDriver.parseFloorLiteral(FUZZ_NOWS[i]);
            }
        }

        void assertNoFailures() {
            LOG.info().$("date variable detection fuzz done [expressions=").$(checkCount)
                    .$(", nowDependent=").$(nowDependentCount)
                    .$(", compiled=").$(compiledCount)
                    .$(", failures=").$(failureCount)
                    .I$();
            if (failureCount > 0) {
                Assert.fail(failureCount + " of " + checkCount + " tick expressions failed, the first "
                        + Math.min(failureCount, MAX_REPORTED_FAILURES) + ":\n" + failures);
            }
            Assert.assertTrue("no expression depended on now", nowDependentCount > 0);
            Assert.assertTrue("no expression compiled", compiledCount > 0);
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
                    if (!ERROR.equals(parsed[i])) {
                        fail(expression, "does not compile [error=" + e.getFlyweightMessage() + "], but parses to "
                                + parsed[i] + " at " + FUZZ_NOWS[i]);
                        return;
                    }
                }
                return;
            }
            compiledCount++;
            try (compiled) {
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
                        intervals, IntervalOperation.INTERSECT, parseSink, true, now);
                return intervalToString(timestampDriver, intervals).toString();
            } catch (SqlException | CairoException e) {
                return ERROR;
            }
        }
    }
}
