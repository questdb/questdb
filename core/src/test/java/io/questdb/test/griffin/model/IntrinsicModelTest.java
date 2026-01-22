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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.TimestampDriver;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.model.IntervalOperation;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.LongList;
import io.questdb.std.NumericException;
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
public class IntrinsicModelTest {
    private static final CairoConfiguration configuration = new DefaultCairoConfiguration(".");
    private static final StringSink sink = new StringSink();
    private final LongList a = new LongList();
    private final LongList b = new LongList();
    private final LongList out = new LongList();
    private final TestTimestampType timestampType;

    public IntrinsicModelTest(TestTimestampType timestampType) {
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
    public static void parseBracketInterval(
            TimestampDriver timestampDriver,
            CharSequence seq,
            int lo,
            int lim,
            int position,
            LongList out,
            short operation
    ) throws SqlException {
        IntervalUtils.parseBracketInterval(timestampDriver, configuration, seq, lo, lim, position, out, operation, new StringSink(), true);
    }

    /**
     * Overload that allows specifying applyEncoded parameter for testing encoded format paths.
     */
    public static void parseBracketInterval(
            TimestampDriver timestampDriver,
            CharSequence seq,
            int lo,
            int lim,
            int position,
            LongList out,
            short operation,
            boolean applyEncoded
    ) throws SqlException {
        IntervalUtils.parseBracketInterval(timestampDriver, configuration, seq, lo, lim, position, out, operation, new StringSink(), applyEncoded);
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
    @Test(timeout = 60000)
    public void testBracketExpansionCartesianProductWithMerge() throws SqlException {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        LongList out = new LongList();

        // Test 1: Small cartesian product with non-adjacent values (no merging)
        String interval = "2018-[01,06]-[10,15]"; // 2 * 2 = 4 combinations
        parseBracketInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
        Assert.assertEquals("Expected 4 intervals for 2x2 cartesian product", 4, out.size() / 2);

        // Test 2: Larger cartesian product with non-consecutive months
        // 2018-[01,03,05,07,09,11]-[05,10,15,20,25] = 6 months * 5 days = 30 combinations
        out.clear();
        interval = "2018-[01,03,05,07,09,11]-[05,10,15,20,25]";
        parseBracketInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
        Assert.assertEquals("Expected 30 intervals for 6x5 cartesian product", 30, out.size() / 2);

        // Test 3: Cartesian product with adjacent days within months
        // 2020-[01,02]-[01..28] = 2 months * 28 days = 56 combinations before merging
        // Days 1-28 within each month are adjacent and merge.
        // Jan 28 -> Feb 1 has gap (Jan 29,30,31), so months don't merge.
        out.clear();
        interval = "2020-[01,02]-[01..28]";
        parseBracketInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
        // Each month's days merge to 1 interval, total 2 (gap between months)
        Assert.assertEquals("2 months * 28 days should merge to 2 intervals", 2, out.size() / 2);
    }

    @Test
    public void testBracketExpansionErrorCountNotANumber() {
        // Invalid count in repeating interval format (exercises line 1242)
        assertBracketIntervalError("2018-01-10T10:30;30m;2d;X", "Count not a number");
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
    public void testBracketExpansionErrorNegativeDuration() {
        // Negative duration - '-' is not a valid start for a duration segment
        assertBracketIntervalError("2018-01-[10,15]T00:00;-1h", "Expected number before unit");
    }

    @Test
    public void testBracketExpansionErrorNegativeDurationAnyFormat() {
        // Negative duration via parseAnyFormat path - same error
        assertBracketIntervalError("2018-01-[10,15]T00:00:00.000000Z;-1h", "Expected number before unit");
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
        // Tests bracket in hour position with space separator
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
        final TimestampDriver timestampDriver = timestampType.getDriver();
        LongList out = new LongList();

        // Test 1: Large range of consecutive days (adjacent intervals merge to 1)
        // 2020-01-[1..500] would create 500 day intervals, but they're adjacent so merge to 1
        // Note: Days > 31 are invalid, so use month expansion instead:
        // 2020-[01..12]-[01..28] = 12 months * 28 days = 336 consecutive-ish intervals
        // But days within each month are adjacent and merge, and months don't merge (gap between months)
        // So let's use a simpler test with consecutive days in January
        String interval = "2020-01-[01..31]"; // 31 consecutive days -> should merge to 1
        parseBracketInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
        Assert.assertEquals("31 consecutive days should merge to 1 interval", 1, out.size() / 2);

        // Test 2: Large year range (non-adjacent intervals don't merge)
        // [1970..2170]-01-01 = 201 separate day intervals (Jan 1 each year, ~365 day gaps)
        out.clear();
        interval = "[1970..2170]-01-01";
        parseBracketInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
        // These don't merge because there's a ~365 day gap between Jan 1 of each year
        Assert.assertEquals("201 non-adjacent year intervals should remain separate", 201, out.size() / 2);

        // Test 4: Verify max interval limit (1024) is enforced
        // [1000..3000]-01-01 = 2001 non-adjacent intervals, exceeds limit of 1024
        out.clear();
        interval = "[1000..3000]-01-01";
        try {
            parseBracketInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
            Assert.fail("Should fail with 'too many intervals' error");
        } catch (SqlException e) {
            Assert.assertTrue("Expected 'too many intervals' error, got: " + e.getMessage(),
                    e.getMessage().contains("too many intervals"));
        }

        // Test 3: Verify incremental merge kicks in for large adjacent ranges
        // Using format that matches existing tests: year-[months]-[days]
        // 2020-[01,02,03,04,05,06,07,08,09,10,11,12]-[01..28] = 12 * 28 = 336 combinations
        out.clear();
        interval = "2020-[01,02,03,04,05,06,07,08,09,10,11,12]-[01..28]";
        parseBracketInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
        // Days 1-28 within each month are adjacent and merge to 1.
        // Dec 28 -> Jan 1 has gaps (Dec 29,30,31), so 12 separate month intervals.
        // But wait - all months are in 2020, so Jan 28 -> Feb 1 only has Jan 29,30,31 gap
        // So we expect 12 intervals (one per month, each spanning days 1-28)
        Assert.assertEquals("12 months * 28 days should merge to 12 intervals", 12, out.size() / 2);
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
        LongList out = new LongList();
        String interval = "1234567890000000";
        parseBracketInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
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
        // Test field expansion path with applyEncoded=false (exercises line 510 branch)
        // When applyEncoded=false, intervals stay in 4-long encoded format and union is skipped
        final TimestampDriver timestampDriver = timestampType.getDriver();
        LongList out = new LongList();
        String interval = "2018-01-[10,15]";
        parseBracketInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT, false);
        // With applyEncoded=false, we get 4 longs per interval (encoded format)
        // 2 intervals * 4 longs = 8
        Assert.assertEquals(8, out.size());
    }

    @Test
    public void testBracketExpansionWithDuration() throws SqlException {
        // 10:30 is minute-level, so hi = 10:30:59.999999, then +1h = 11:30:59.999999
        assertBracketInterval(
                "[{lo=2018-01-10T10:30:00.000000Z, hi=2018-01-10T11:30:59.999999Z},{lo=2018-01-15T10:30:00.000000Z, hi=2018-01-15T11:30:59.999999Z}]",
                "2018-01-[10,15]T10:30;1h"
        );
    }

    @Test
    public void testBracketExpansionWithMultiUnitDuration() throws SqlException {
        // Multi-unit duration: 1h30m = 1 hour 30 minutes
        // 10:00 + 1h30m = 11:30:59.999999
        assertBracketInterval(
                "[{lo=2024-01-15T10:00:00.000000Z, hi=2024-01-15T11:30:59.999999Z}]",
                "2024-01-15T10:00;1h30m"
        );
    }

    @Test
    public void testBracketExpansionWithMultiUnitDurationComplex() throws SqlException {
        // Complex multi-unit duration: 2h15m30s
        // 10:00:00 + 2h15m30s = 12:15:30.999999
        assertBracketInterval(
                "[{lo=2024-01-15T10:00:00.000000Z, hi=2024-01-15T12:15:30.999999Z}]",
                "2024-01-15T10:00:00;2h15m30s"
        );
    }

    @Test
    public void testBracketExpansionWithMultiUnitDurationDaysHours() throws SqlException {
        // Days and hours: 1d12h = 1 day 12 hours = 36 hours
        assertBracketInterval(
                "[{lo=2024-01-15T00:00:00.000000Z, hi=2024-01-16T12:00:59.999999Z}]",
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
            LongList out1 = new LongList();
            final TimestampDriver timestampDriver = timestampType.getDriver();
            parseBracketInterval(timestampDriver, input, 0, input.length(), 0, out1, IntervalOperation.INTERSECT);
            TestUtils.assertEquals(
                    "[{lo=2024-01-15T10:00:00.000000000Z, hi=2024-01-15T10:00:01.500000999Z}]",
                    intervalToString(timestampDriver, out1)
            );
        } else {
            assertBracketInterval(
                    "[{lo=2024-01-15T10:00:00.000000Z, hi=2024-01-15T10:00:01.500000Z}]",
                    input
            );
        }
    }

    @Test
    public void testBracketExpansionWithNonZeroOffset() throws SqlException {
        // Tests that bracket parsing works correctly when lo > 0
        // The prefix "XXX" should be ignored, and padding should be calculated from the interval start
        String input = "XXX2018-01-[5,10]";
        int lo = 3; // skip "XXX"
        LongList out = new LongList();
        final TimestampDriver timestampDriver = timestampType.getDriver();
        parseBracketInterval(timestampDriver, input, lo, input.length(), 0, out, IntervalOperation.INTERSECT);
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
                "[{lo=2018-01-10T10:30:00.000000Z, hi=2018-01-10T11:00:59.999999Z},{lo=2018-01-12T10:30:00.000000Z, hi=2018-01-12T11:00:59.999999Z},{lo=2018-01-15T10:30:00.000000Z, hi=2018-01-15T11:00:59.999999Z},{lo=2018-01-17T10:30:00.000000Z, hi=2018-01-17T11:00:59.999999Z}]",
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
        LongList out = new LongList();
        String interval = "2018-01-[10,15]";
        parseBracketInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.SUBTRACT, false);
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

    // ==================== Bracket Expansion Tests ====================

    @Test
    public void testDateListErrorLeadingComma() {
        // '[,2025-01-01]' produces error
        assertBracketIntervalError("[,2025-01-01]", "Empty element in date list");
    }

    @Test
    public void testDateListErrorNegativeDuration() {
        // '[2025-01-01]T09:30;-5m' - negative duration not supported
        assertBracketIntervalError("[2025-01-01]T09:30;-5m", "Expected number before unit");
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
                "[{lo=2024-01-01T00:00:00.000000Z, hi=2024-01-02T00:59:59.999999Z},{lo=2024-01-06T00:00:00.000000Z, hi=2024-01-07T00:59:59.999999Z}]",
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
    public void testDateListWithApplyEncodedFalse() throws SqlException {
        // Test date list path with applyEncoded=false (exercises line 455 branch)
        // When applyEncoded=false, intervals stay in 4-long encoded format and union is skipped
        final TimestampDriver timestampDriver = timestampType.getDriver();
        LongList out = new LongList();
        String interval = "[2025-01-01,2025-01-05]";
        parseBracketInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT, false);
        // With applyEncoded=false, we get 4 longs per interval (encoded format)
        // 2 intervals * 4 longs = 8
        Assert.assertEquals(8, out.size());
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
        final TimestampDriver timestampDriver = timestampType.getDriver();
        LongList out = new LongList();
        String interval = "[2024-01-01,2024-01-02,2024-01-03]#Mon";
        parseBracketInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT, false);
        // With applyEncoded=false, we get 4 longs per interval (encoded format)
        // 3 intervals * 4 longs = 12
        Assert.assertEquals(12, out.size());

        // Verify day filter mask is encoded on all intervals (Monday = bit 0 = 1)
        Assert.assertEquals(1, IntervalUtils.decodeDayFilterMask(out, 0));
        Assert.assertEquals(1, IntervalUtils.decodeDayFilterMask(out, 4));
        Assert.assertEquals(1, IntervalUtils.decodeDayFilterMask(out, 8));
    }

    @Test
    public void testDateListWithDurationOnly() throws SqlException {
        // Date list with simple duration suffix (no repeating)
        // For a date without time, hi starts at end-of-day (23:59:59.999999), then +1h is added
        assertBracketInterval(
                "[{lo=2025-01-01T00:00:00.000000Z, hi=2025-01-02T00:59:59.999999Z},{lo=2025-01-05T00:00:00.000000Z, hi=2025-01-06T00:59:59.999999Z}]",
                "[2025-01-01,2025-01-05];1h"
        );
    }

    @Test
    public void testDateListWithDurationSuffix() throws SqlException {
        // '[2025-01-15,2025-01-20]T09:30;389m' produces 2 trading-hours intervals
        // 09:30 + 389 minutes = 09:30 + 6h29m = 15:59
        assertBracketInterval(
                "[{lo=2025-01-15T09:30:00.000000Z, hi=2025-01-15T15:59:59.999999Z},{lo=2025-01-20T09:30:00.000000Z, hi=2025-01-20T15:59:59.999999Z}]",
                "[2025-01-15,2025-01-20]T09:30;389m"
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
    public void testDateListWithTimeExpansion() throws SqlException {
        // '[2025-01-15,2025-01-20]T[09,14]:30;1h' produces 4 intervals (date × time expansion)
        assertBracketInterval(
                "[{lo=2025-01-15T09:30:00.000000Z, hi=2025-01-15T10:30:59.999999Z},{lo=2025-01-15T14:30:00.000000Z, hi=2025-01-15T15:30:59.999999Z},{lo=2025-01-20T09:30:00.000000Z, hi=2025-01-20T10:30:59.999999Z},{lo=2025-01-20T14:30:00.000000Z, hi=2025-01-20T15:30:59.999999Z}]",
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

    // ==================== TIME LIST BRACKET TESTS ====================

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
        LongList out = new LongList();
        String interval = "2024-01-[01..02]@+12:00#Mon";
        parseBracketInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);

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
        LongList out = new LongList();
        String interval = "[2024-01-02,2024-01-03]#Mon";
        parseBracketInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
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
                "[{lo=2024-01-01T09:00:00.000000Z, hi=2024-01-01T10:00:59.999999Z}]",
                "[2024-01-01,2024-01-02]T09:00#Mon;1h"
        );
    }

    @Test
    public void testDayFilterDateListWithDurationAndTimezone() throws SqlException {
        // Date list with duration + timezone + day filter
        // 2024-01-01 is Monday in +05:30
        // 2024-01-01 09:00 +05:30 = 2024-01-01 03:30 UTC
        assertBracketInterval(
                "[{lo=2024-01-01T03:30:00.000000Z, hi=2024-01-01T04:30:59.999999Z}]",
                "[2024-01-01,2024-01-02]T09:00@+05:30#Mon;1h"
        );
    }

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
        LongList out = new LongList();
        String interval = "[2024-01-01@+12:00]#Mon";
        parseBracketInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);

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
        LongList out = new LongList();
        String interval = "2024-01-[01..03]#Mon";
        parseBracketInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT, false);

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
        // Test weekend filter encoding in dynamic mode
        final TimestampDriver timestampDriver = timestampType.getDriver();
        LongList out = new LongList();
        String interval = "2024-01-01#weekend";
        parseBracketInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT, false);

        // 1 interval * 4 longs = 4
        Assert.assertEquals(4, out.size());

        // Weekend mask = Sat|Sun = bits 5-6 = 0x60 = 96
        int dayFilterMask = IntervalUtils.decodeDayFilterMask(out, 0);
        Assert.assertEquals("Weekend mask should be 96 (Sat-Sun)", 96, dayFilterMask);
    }

    @Test
    public void testDayFilterDynamicModeEncodingWorkday() throws SqlException {
        // Test workday filter encoding in dynamic mode
        final TimestampDriver timestampDriver = timestampType.getDriver();
        LongList out = new LongList();
        String interval = "2024-01-01#workday";
        parseBracketInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT, false);

        // 1 interval * 4 longs = 4
        Assert.assertEquals(4, out.size());

        // Workday mask = Mon|Tue|Wed|Thu|Fri = bits 0-4 = 0x1F = 31
        int dayFilterMask = IntervalUtils.decodeDayFilterMask(out, 0);
        Assert.assertEquals("Workday mask should be 31 (Mon-Fri)", 31, dayFilterMask);
    }

    @Test
    public void testDayFilterEmptyResult() throws SqlException {
        // Filter that removes all intervals (e.g., looking for Monday in a Tue-Thu range)
        // 2024-01-02 is Tuesday, 2024-01-04 is Thursday
        final TimestampDriver timestampDriver = timestampType.getDriver();
        LongList out = new LongList();
        String interval = "2024-01-[02..04]#Mon";
        parseBracketInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
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
    public void testDayFilterMultipleDays() throws SqlException {
        // 2024-01-01 is Monday, 2024-01-03 is Wednesday, 2024-01-05 is Friday
        assertBracketInterval(
                "[{lo=2024-01-01T00:00:00.000000Z, hi=2024-01-01T23:59:59.999999Z},{lo=2024-01-03T00:00:00.000000Z, hi=2024-01-03T23:59:59.999999Z},{lo=2024-01-05T00:00:00.000000Z, hi=2024-01-05T23:59:59.999999Z}]",
                "2024-01-[01..07]#Mon,Wed,Fri"
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
        LongList out = new LongList();
        String interval = "2024-01-01#Tue";
        parseBracketInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
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
                "[{lo=2024-01-01T09:00:00.000000Z, hi=2024-01-01T10:00:59.999999Z},{lo=2024-01-02T09:00:00.000000Z, hi=2024-01-02T10:00:59.999999Z},{lo=2024-01-03T09:00:00.000000Z, hi=2024-01-03T10:00:59.999999Z},{lo=2024-01-04T09:00:00.000000Z, hi=2024-01-04T10:00:59.999999Z},{lo=2024-01-05T09:00:00.000000Z, hi=2024-01-05T10:00:59.999999Z}]",
                "2024-01-[01..07]T09:00#workday;1h"
        );
    }

    @Test
    public void testDayFilterWithTimezone() throws SqlException {
        // Day filter with timezone: date@timezone#dayFilter;duration
        // 2024-01-01 is Monday in local time (+02:00)
        // Filter workdays, then convert to UTC
        // 09:00 local (+02:00) = 07:00 UTC
        assertBracketInterval(
                "[{lo=2024-01-01T07:00:00.000000Z, hi=2024-01-01T08:00:59.999999Z},{lo=2024-01-02T07:00:00.000000Z, hi=2024-01-02T08:00:59.999999Z},{lo=2024-01-03T07:00:00.000000Z, hi=2024-01-03T08:00:59.999999Z},{lo=2024-01-04T07:00:00.000000Z, hi=2024-01-04T08:00:59.999999Z},{lo=2024-01-05T07:00:00.000000Z, hi=2024-01-05T08:00:59.999999Z}]",
                "2024-01-[01..07]T09:00@+02:00#workday;1h"
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

    // ==================== Date List Tests ====================

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
        LongList out = new LongList();
        TimestampDriver timestampDriver = ColumnType.getTimestampDriver(ColumnType.TIMESTAMP);
        parseBracketInterval(timestampDriver, intervalStr, 0, intervalStr.length(), 0, out, IntervalOperation.INTERSECT);
        IntervalUtils.invert(out);
        TestUtils.assertEquals(
                "[{lo=, hi=2018-01-10T10:29:59.999999Z},{lo=2018-01-10T11:00:00.000001Z, hi=2018-01-12T10:29:59.999999Z},{lo=2018-01-12T11:00:00.000001Z, hi=294247-01-10T04:00:54.775807Z}]",
                intervalToString(timestampDriver, out)
        );
    }

    @Test
    public void testNoBracketsWithApplyEncodedFalse() throws SqlException {
        // Test no-brackets path with applyEncoded=false (exercises line 484 branch)
        final TimestampDriver timestampDriver = timestampType.getDriver();
        LongList out = new LongList();
        String interval = "2025-01-15";
        parseBracketInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT, false);
        // With applyEncoded=false, we get 4 longs per interval (encoded format)
        Assert.assertEquals(4, out.size());
    }

    @Test
    public void testParseLongInterval22() throws Exception {
        assertShortInterval(
                "[{lo=2015-03-12T10:00:00.000000Z, hi=2015-03-12T10:05:00.999999Z},{lo=2015-03-12T10:30:00.000000Z, hi=2015-03-12T10:35:00.999999Z},{lo=2015-03-12T11:00:00.000000Z, hi=2015-03-12T11:05:00.999999Z},{lo=2015-03-12T11:30:00.000000Z, hi=2015-03-12T11:35:00.999999Z},{lo=2015-03-12T12:00:00.000000Z, hi=2015-03-12T12:05:00.999999Z},{lo=2015-03-12T12:30:00.000000Z, hi=2015-03-12T12:35:00.999999Z},{lo=2015-03-12T13:00:00.000000Z, hi=2015-03-12T13:05:00.999999Z},{lo=2015-03-12T13:30:00.000000Z, hi=2015-03-12T13:35:00.999999Z},{lo=2015-03-12T14:00:00.000000Z, hi=2015-03-12T14:05:00.999999Z},{lo=2015-03-12T14:30:00.000000Z, hi=2015-03-12T14:35:00.999999Z}]",
                "2015-03-12T10:00:00;5m;30m;10"
        );
    }

    @Test
    public void testParseLongInterval32() throws Exception {
        assertShortInterval("[{lo=2016-03-21T00:00:00.000000Z, hi=2021-03-21T23:59:59.999999Z}]", "2016-03-21;3y;6M;5");
    }

    @Test
    public void testParseLongIntervalPositiveYearPeriod() throws Exception {
        // Test positive year period (exercises period >= 0 branch in addYearIntervals, line 675)
        // 2015-03-12T11:00:00;5m;1y;3 means: start at 2015, duration 5m, repeat every 1 year, 3 times
        assertShortInterval(
                "[{lo=2015-03-12T11:00:00.000000Z, hi=2015-03-12T11:05:00.999999Z},{lo=2016-03-12T11:00:00.000000Z, hi=2016-03-12T11:05:00.999999Z},{lo=2017-03-12T11:00:00.000000Z, hi=2017-03-12T11:05:00.999999Z}]",
                "2015-03-12T11:00:00;5m;1y;3"
        );
    }

    @Test
    public void testParseLongMinusInterval() throws Exception {
        assertShortInterval(
                "[{lo=2015-03-12T10:00:00.000000Z, hi=2015-03-12T10:05:00.999999Z},{lo=2015-03-12T10:30:00.000000Z, hi=2015-03-12T10:35:00.999999Z},{lo=2015-03-12T11:00:00.000000Z, hi=2015-03-12T11:05:00.999999Z}]",
                "2015-03-12T11:00:00;5m;-30m;3"
        );
        assertShortInterval(
                "[{lo=2014-11-12T11:00:00.000000Z, hi=2014-11-12T11:05:00.999999Z},{lo=2015-01-12T11:00:00.000000Z, hi=2015-01-12T11:05:00.999999Z},{lo=2015-03-12T11:00:00.000000Z, hi=2015-03-12T11:05:00.999999Z}]",
                "2015-03-12T11:00:00;5m;-2M;3"
        );
        assertShortInterval(
                "[{lo=2013-03-12T11:00:00.000000Z, hi=2013-03-12T11:05:00.999999Z},{lo=2014-03-12T11:00:00.000000Z, hi=2014-03-12T11:05:00.999999Z},{lo=2015-03-12T11:00:00.000000Z, hi=2015-03-12T11:05:00.999999Z}]",
                "2015-03-12T11:00:00;5m;-1y;3"
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

    // =====================================================
    // Timezone tests
    // =====================================================

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
                "[{lo=2024-01-15T09:00:00.000000Z, hi=2024-01-15T10:00:59.999999Z},{lo=2024-01-15T12:30:00.000000Z, hi=2024-01-15T13:30:59.999999Z}]",
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
                "[{lo=2024-01-15T09:00:00.000000Z, hi=2024-01-15T12:30:59.999999Z}]",
                "2024-01-15T[09:00,10:30];2h"
        );
    }

    @Test
    public void testTimeListBracketPerElementTimezone() throws SqlException {
        // Time list with per-element timezones
        // T[09:00@+05:00,08:00@+02:00];1h
        // 09:00 in +05:00 = 04:00 UTC, 08:00 in +02:00 = 06:00 UTC
        assertBracketInterval(
                "[{lo=2024-01-15T04:00:00.000000Z, hi=2024-01-15T05:00:59.999999Z},{lo=2024-01-15T06:00:00.000000Z, hi=2024-01-15T07:00:59.999999Z}]",
                "2024-01-15T[09:00@+05:00,08:00@+02:00];1h"
        );
    }

    @Test
    public void testTimeListBracketSimple() throws SqlException {
        // Simple time list: T[09:00,18:00];1h (non-overlapping intervals)
        // Creates intervals at 09:00-10:00 and 18:00-19:00
        assertBracketInterval(
                "[{lo=2024-01-15T09:00:00.000000Z, hi=2024-01-15T10:00:59.999999Z},{lo=2024-01-15T18:00:00.000000Z, hi=2024-01-15T19:00:59.999999Z}]",
                "2024-01-15T[09:00,18:00];1h"
        );
    }

    @Test
    public void testTimeListBracketSingleTime() throws SqlException {
        // Single time in time list bracket (edge case)
        assertBracketInterval(
                "[{lo=2024-01-15T09:00:00.000000Z, hi=2024-01-15T10:00:59.999999Z}]",
                "2024-01-15T[09:00];1h"
        );
    }

    @Test
    public void testTimeListBracketThreeTimes() throws SqlException {
        // Three time values (non-overlapping)
        assertBracketInterval(
                "[{lo=2024-01-15T08:00:00.000000Z, hi=2024-01-15T09:00:59.999999Z},{lo=2024-01-15T12:00:00.000000Z, hi=2024-01-15T13:00:59.999999Z},{lo=2024-01-15T18:00:00.000000Z, hi=2024-01-15T19:00:59.999999Z}]",
                "2024-01-15T[08:00,12:00,18:00];1h"
        );
    }

    @Test
    public void testTimeListBracketWithDateExpansionAndTimezone() throws SqlException {
        // Date expansion + time list + global timezone
        // 2024-01-[15,16]T[09:00,18:00]@+02:00;1h = 4 intervals
        // Times in +02:00: 09:00 = 07:00 UTC, 18:00 = 16:00 UTC
        assertBracketInterval(
                "[{lo=2024-01-15T07:00:00.000000Z, hi=2024-01-15T08:00:59.999999Z},{lo=2024-01-15T16:00:00.000000Z, hi=2024-01-15T17:00:59.999999Z},{lo=2024-01-16T07:00:00.000000Z, hi=2024-01-16T08:00:59.999999Z},{lo=2024-01-16T16:00:00.000000Z, hi=2024-01-16T17:00:59.999999Z}]",
                "2024-01-[15,16]T[09:00,18:00]@+02:00;1h"
        );
    }

    @Test
    public void testTimeListBracketWithDayExpansion() throws SqlException {
        // Combination: day expansion + time list
        // 2024-01-[15,16]T[09:00,18:00];1h = 4 intervals (2 days × 2 times, non-overlapping)
        assertBracketInterval(
                "[{lo=2024-01-15T09:00:00.000000Z, hi=2024-01-15T10:00:59.999999Z},{lo=2024-01-15T18:00:00.000000Z, hi=2024-01-15T19:00:59.999999Z},{lo=2024-01-16T09:00:00.000000Z, hi=2024-01-16T10:00:59.999999Z},{lo=2024-01-16T18:00:00.000000Z, hi=2024-01-16T19:00:59.999999Z}]",
                "2024-01-[15,16]T[09:00,18:00];1h"
        );
    }

    @Test
    public void testTimeListBracketWithGlobalTimezone() throws SqlException {
        // Time list with global timezone (applied to all elements)
        // T[09:00,18:00]@+02:00;1h (non-overlapping)
        // Both times in +02:00 offset: 09:00 = 07:00 UTC, 18:00 = 16:00 UTC
        assertBracketInterval(
                "[{lo=2024-01-15T07:00:00.000000Z, hi=2024-01-15T08:00:59.999999Z},{lo=2024-01-15T16:00:00.000000Z, hi=2024-01-15T17:00:59.999999Z}]",
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
                "[{lo=2024-01-15T09:00:00.000000Z, hi=2024-01-15T10:00:30.999999Z},{lo=2024-01-15T14:30:00.000000Z, hi=2024-01-15T15:30:30.999999Z}]",
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

    // ==================== Day-of-Week Filter Tests ====================

    @Test
    public void testTimeListBracketWithTrailingCommaAndWhitespace() throws SqlException {
        // Time list with trailing comma and whitespace (exercises L1406/L1409 whitespace to bracket end)
        assertBracketInterval(
                "[{lo=2024-01-15T09:00:00.000000Z, hi=2024-01-15T09:00:59.999999Z}]",
                "2024-01-15T[09:00,   ]"
        );
    }

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
                "[{lo=2024-01-15T09:00:00.000000Z, hi=2024-01-15T10:00:59.999999Z},{lo=2024-01-15T18:00:00.000000Z, hi=2024-01-15T19:00:59.999999Z}]",
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
        LongList out = new LongList();
        String interval = "2024-01-[15@00,16]";
        try {
            parseBracketInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
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
        final TimestampDriver timestampDriver = timestampType.getDriver();
        LongList out = new LongList();
        String interval = "2024-01-[15,16]T08:00@+02:00;1h";
        parseBracketInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
        Assert.assertEquals(4, out.size());
        String expected = ColumnType.isTimestampNano(timestampType.getTimestampType())
                ? "[{lo=2024-01-15T06:00:00.000000000Z, hi=2024-01-15T07:00:59.999999999Z},{lo=2024-01-16T06:00:00.000000000Z, hi=2024-01-16T07:00:59.999999999Z}]"
                : "[{lo=2024-01-15T06:00:00.000000Z, hi=2024-01-15T07:00:59.999999Z},{lo=2024-01-16T06:00:00.000000Z, hi=2024-01-16T07:00:59.999999Z}]";
        TestUtils.assertEquals(expected, intervalToString(timestampDriver, out));
    }

    @Test
    public void testTimezoneDateListGlobalTimezone() throws SqlException {
        // Date list with global timezone applied to all elements
        // [2024-01-15,2024-01-16]T08:00@+02:00
        final TimestampDriver timestampDriver = timestampType.getDriver();
        LongList out = new LongList();
        String interval = "[2024-01-15,2024-01-16]T08:00@+02:00";
        parseBracketInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
        Assert.assertEquals(4, out.size());
        String expected = ColumnType.isTimestampNano(timestampType.getTimestampType())
                ? "[{lo=2024-01-15T06:00:00.000000000Z, hi=2024-01-15T06:00:59.999999999Z},{lo=2024-01-16T06:00:00.000000000Z, hi=2024-01-16T06:00:59.999999999Z}]"
                : "[{lo=2024-01-15T06:00:00.000000Z, hi=2024-01-15T06:00:59.999999Z},{lo=2024-01-16T06:00:00.000000Z, hi=2024-01-16T06:00:59.999999Z}]";
        TestUtils.assertEquals(expected, intervalToString(timestampDriver, out));
    }

    @Test
    public void testTimezoneDateListGlobalTimezoneWithDuration() throws SqlException {
        // Date list with global timezone AND duration suffix - tests L1077 branch
        // [2024-01-15,2024-01-16]T08:00@+02:00;1h
        // 08:00 in +02:00 = 06:00 UTC, ;1h extends hi by 1 hour to 07:00:59 UTC
        final TimestampDriver timestampDriver = timestampType.getDriver();
        LongList out = new LongList();
        String interval = "[2024-01-15,2024-01-16]T08:00@+02:00;1h";
        parseBracketInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
        Assert.assertEquals(4, out.size()); // 2 dates, each with 1 interval (duration extends hi, doesn't add intervals)
        String expected = ColumnType.isTimestampNano(timestampType.getTimestampType())
                ? "[{lo=2024-01-15T06:00:00.000000000Z, hi=2024-01-15T07:00:59.999999999Z},{lo=2024-01-16T06:00:00.000000000Z, hi=2024-01-16T07:00:59.999999999Z}]"
                : "[{lo=2024-01-15T06:00:00.000000Z, hi=2024-01-15T07:00:59.999999Z},{lo=2024-01-16T06:00:00.000000Z, hi=2024-01-16T07:00:59.999999Z}]";
        TestUtils.assertEquals(expected, intervalToString(timestampDriver, out));
    }

    @Test
    public void testTimezoneDateListMixedTimezones() throws SqlException {
        // Date list with mixed: some per-element, some using global
        // [2024-01-15@UTC,2024-01-16]T08:00@+03:00
        // First: has own timezone UTC -> 08:00 UTC
        // Second: uses global +03:00 -> 05:00 UTC
        final TimestampDriver timestampDriver = timestampType.getDriver();
        LongList out = new LongList();
        String interval = "[2024-01-15@UTC,2024-01-16]T08:00@+03:00";
        parseBracketInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
        Assert.assertEquals(4, out.size());
        String expected = ColumnType.isTimestampNano(timestampType.getTimestampType())
                ? "[{lo=2024-01-15T08:00:00.000000000Z, hi=2024-01-15T08:00:59.999999999Z},{lo=2024-01-16T05:00:00.000000000Z, hi=2024-01-16T05:00:59.999999999Z}]"
                : "[{lo=2024-01-15T08:00:00.000000Z, hi=2024-01-15T08:00:59.999999Z},{lo=2024-01-16T05:00:00.000000Z, hi=2024-01-16T05:00:59.999999Z}]";
        TestUtils.assertEquals(expected, intervalToString(timestampDriver, out));
    }

    @Test
    public void testTimezoneDateListPerElementNamedTimezone() throws SqlException {
        // Date list with per-element named timezones
        // [2024-01-15@Europe/London,2024-07-15@Europe/London]T08:00
        // Winter: 08:00 in UTC+0 = 08:00 UTC
        // Summer: 08:00 in UTC+1 = 07:00 UTC
        final TimestampDriver timestampDriver = timestampType.getDriver();
        LongList out = new LongList();
        String interval = "[2024-01-15@Europe/London,2024-07-15@Europe/London]T08:00";
        parseBracketInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
        Assert.assertEquals(4, out.size());
        String expected = ColumnType.isTimestampNano(timestampType.getTimestampType())
                ? "[{lo=2024-01-15T08:00:00.000000000Z, hi=2024-01-15T08:00:59.999999999Z},{lo=2024-07-15T07:00:00.000000000Z, hi=2024-07-15T07:00:59.999999999Z}]"
                : "[{lo=2024-01-15T08:00:00.000000Z, hi=2024-01-15T08:00:59.999999Z},{lo=2024-07-15T07:00:00.000000Z, hi=2024-07-15T07:00:59.999999Z}]";
        TestUtils.assertEquals(expected, intervalToString(timestampDriver, out));
    }

    @Test
    public void testTimezoneDateListPerElementTimezone() throws SqlException {
        // Date list with per-element timezones
        // [2024-01-15@+02:00,2024-01-16@+05:00]T08:00
        // First: 08:00 in +02:00 = 06:00 UTC
        // Second: 08:00 in +05:00 = 03:00 UTC
        final TimestampDriver timestampDriver = timestampType.getDriver();
        LongList out = new LongList();
        String interval = "[2024-01-15@+02:00,2024-01-16@+05:00]T08:00";
        parseBracketInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
        Assert.assertEquals(4, out.size());
        String expected = ColumnType.isTimestampNano(timestampType.getTimestampType())
                ? "[{lo=2024-01-15T06:00:00.000000000Z, hi=2024-01-15T06:00:59.999999999Z},{lo=2024-01-16T03:00:00.000000000Z, hi=2024-01-16T03:00:59.999999999Z}]"
                : "[{lo=2024-01-15T06:00:00.000000Z, hi=2024-01-15T06:00:59.999999Z},{lo=2024-01-16T03:00:00.000000Z, hi=2024-01-16T03:00:59.999999Z}]";
        TestUtils.assertEquals(expected, intervalToString(timestampDriver, out));
    }

    @Test
    public void testTimezoneDateListWithBracketExpansion() throws SqlException {
        // Date list element with bracket expansion and timezone
        // [2024-01-[15,16]@+02:00]T08:00
        final TimestampDriver timestampDriver = timestampType.getDriver();
        LongList out = new LongList();
        String interval = "[2024-01-[15,16]@+02:00]T08:00";
        parseBracketInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
        Assert.assertEquals(4, out.size());
        String expected = ColumnType.isTimestampNano(timestampType.getTimestampType())
                ? "[{lo=2024-01-15T06:00:00.000000000Z, hi=2024-01-15T06:00:59.999999999Z},{lo=2024-01-16T06:00:00.000000000Z, hi=2024-01-16T06:00:59.999999999Z}]"
                : "[{lo=2024-01-15T06:00:00.000000Z, hi=2024-01-15T06:00:59.999999Z},{lo=2024-01-16T06:00:00.000000Z, hi=2024-01-16T06:00:59.999999Z}]";
        TestUtils.assertEquals(expected, intervalToString(timestampDriver, out));
    }

    @Test
    public void testTimezoneDateListWithTimeListBracketAndGlobalTimezone() throws SqlException {
        // Date list element with time list brackets + global timezone
        // This tests L1329: time list handles TZ internally, so we skip applying it again
        // [2024-01-15T[09:00,14:30]]@+02:00
        // 09:00 in +02:00 = 07:00 UTC, 14:30 in +02:00 = 12:30 UTC
        final TimestampDriver timestampDriver = timestampType.getDriver();
        LongList out = new LongList();
        String interval = "[2024-01-15T[09:00,14:30]]@+02:00";
        parseBracketInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
        Assert.assertEquals(4, out.size());
        String expected = ColumnType.isTimestampNano(timestampType.getTimestampType())
                ? "[{lo=2024-01-15T07:00:00.000000000Z, hi=2024-01-15T07:00:59.999999999Z},{lo=2024-01-15T12:30:00.000000000Z, hi=2024-01-15T12:30:59.999999999Z}]"
                : "[{lo=2024-01-15T07:00:00.000000Z, hi=2024-01-15T07:00:59.999999Z},{lo=2024-01-15T12:30:00.000000Z, hi=2024-01-15T12:30:59.999999Z}]";
        TestUtils.assertEquals(expected, intervalToString(timestampDriver, out));
    }

    @Test
    public void testTimezoneDstGapAdjustment() throws SqlException {
        // Test DST gap handling - exercises L1602-1604 (lo and hi adjustment)
        // On March 10, 2024 at 2:00 AM in America/New_York, clocks spring forward to 3:00 AM
        // So 2:30 AM doesn't exist - both lo and hi are adjusted forward by the same gap offset
        // to preserve interval width
        // lo (2:30:00) -> 3:00:00 EDT = 07:00:00 UTC
        // hi (2:30:59.999999) -> 3:00:59.999999 EDT = 07:00:59.999999 UTC
        final TimestampDriver timestampDriver = timestampType.getDriver();
        LongList out = new LongList();
        String interval = "2024-03-10T02:30@America/New_York";
        parseBracketInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
        Assert.assertEquals(2, out.size());
        String expected = ColumnType.isTimestampNano(timestampType.getTimestampType())
                ? "[{lo=2024-03-10T07:00:00.000000000Z, hi=2024-03-10T07:00:59.999999999Z}]"
                : "[{lo=2024-03-10T07:00:00.000000Z, hi=2024-03-10T07:00:59.999999Z}]";
        TestUtils.assertEquals(expected, intervalToString(timestampDriver, out));
    }

    @Test
    public void testTimezoneDstGapHiOnlyInGap() throws SqlException {
        // Test edge case: lo is before DST gap, hi is in the gap - exercises L1610
        // On March 10, 2024 at 2:00 AM in America/New_York, clocks spring forward to 3:00 AM
        // Using ;2m duration to extend interval from 01:59 into the gap
        // lo = 01:59:00 EST = 06:59:00 UTC (not in gap, unchanged)
        // hi falls in gap and gets adjusted forward
        final TimestampDriver timestampDriver = timestampType.getDriver();
        LongList out = new LongList();
        String interval = "2024-03-10T01:59@America/New_York;2m";
        parseBracketInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
        Assert.assertEquals(2, out.size());
        String expected = ColumnType.isTimestampNano(timestampType.getTimestampType())
                ? "[{lo=2024-03-10T06:59:00.000000000Z, hi=2024-03-10T07:03:59.999999998Z}]"
                : "[{lo=2024-03-10T06:59:00.000000Z, hi=2024-03-10T07:03:59.999998Z}]";
        TestUtils.assertEquals(expected, intervalToString(timestampDriver, out));
    }

    @Test
    public void testTimezoneDstOverlapFallBack() throws SqlException {
        // Test DST overlap (fall back) - Nov 3, 2024 in America/New_York
        // At 2:00 AM EDT, clocks fall back to 1:00 AM EST
        // So 1:30 AM occurs twice - the code uses daylight time (EDT) offset
        // 1:30 AM EDT = 05:30 UTC
        final TimestampDriver timestampDriver = timestampType.getDriver();
        LongList out = new LongList();
        String interval = "2024-11-03T01:30@America/New_York";
        parseBracketInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
        Assert.assertEquals(2, out.size());
        // During overlap, daylight time (EDT = UTC-4) is used
        String expected = ColumnType.isTimestampNano(timestampType.getTimestampType())
                ? "[{lo=2024-11-03T05:30:00.000000000Z, hi=2024-11-03T05:30:59.999999999Z}]"
                : "[{lo=2024-11-03T05:30:00.000000Z, hi=2024-11-03T05:30:59.999999Z}]";
        TestUtils.assertEquals(expected, intervalToString(timestampDriver, out));
    }

    @Test
    public void testTimezoneEmpty() {
        // Test empty timezone after @ - should throw error
        final TimestampDriver timestampDriver = timestampType.getDriver();
        LongList out = new LongList();
        String interval = "2024-01-15T08:00@";
        try {
            parseBracketInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
            Assert.fail("Expected SqlException for empty timezone");
        } catch (SqlException e) {
            Assert.assertTrue("Error should mention invalid timezone", e.getMessage().contains("invalid timezone"));
        }
    }

    @Test
    public void testTimezoneEncodedFormat() throws SqlException {
        // Test timezone conversion with encoded format (applyEncoded=false, stride=4)
        // This exercises the isStaticMode=false branch in applyTimezoneToIntervals
        final TimestampDriver timestampDriver = timestampType.getDriver();
        LongList out = new LongList();
        String interval = "2024-01-15T08:00@+03:00";
        // applyEncoded=false produces 4-long encoded intervals
        parseBracketInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT, false);
        Assert.assertEquals(4, out.size()); // 4-long encoded format
        // Convert to 2-long format for comparison
        IntervalUtils.applyLastEncodedInterval(timestampDriver, out);
        Assert.assertEquals(2, out.size());
        // 08:00 in +03:00 = 05:00 UTC
        String expected = ColumnType.isTimestampNano(timestampType.getTimestampType())
                ? "[{lo=2024-01-15T05:00:00.000000000Z, hi=2024-01-15T05:00:59.999999999Z}]"
                : "[{lo=2024-01-15T05:00:00.000000Z, hi=2024-01-15T05:00:59.999999Z}]";
        TestUtils.assertEquals(expected, intervalToString(timestampDriver, out));
    }

    @Test
    public void testTimezoneExtremeNegativeOffset() throws SqlException {
        // Test extreme negative offset -12:00
        // 08:00 in -12:00 = 08:00 + 12:00 = 20:00 UTC
        final TimestampDriver timestampDriver = timestampType.getDriver();
        LongList out = new LongList();
        String interval = "2024-01-15T08:00@-12:00";
        parseBracketInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
        Assert.assertEquals(2, out.size());
        String expected = ColumnType.isTimestampNano(timestampType.getTimestampType())
                ? "[{lo=2024-01-15T20:00:00.000000000Z, hi=2024-01-15T20:00:59.999999999Z}]"
                : "[{lo=2024-01-15T20:00:00.000000Z, hi=2024-01-15T20:00:59.999999Z}]";
        TestUtils.assertEquals(expected, intervalToString(timestampDriver, out));
    }

    @Test
    public void testTimezoneExtremePositiveOffset() throws SqlException {
        // Test extreme positive offset +14:00 (e.g., Pacific/Kiritimati)
        // 08:00 in +14:00 = 08:00 - 14:00 = -06:00 = previous day 18:00 UTC
        final TimestampDriver timestampDriver = timestampType.getDriver();
        LongList out = new LongList();
        String interval = "2024-01-15T08:00@+14:00";
        parseBracketInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
        Assert.assertEquals(2, out.size());
        String expected = ColumnType.isTimestampNano(timestampType.getTimestampType())
                ? "[{lo=2024-01-14T18:00:00.000000000Z, hi=2024-01-14T18:00:59.999999999Z}]"
                : "[{lo=2024-01-14T18:00:00.000000Z, hi=2024-01-14T18:00:59.999999Z}]";
        TestUtils.assertEquals(expected, intervalToString(timestampDriver, out));
    }

    @Test
    public void testTimezoneGMT() throws SqlException {
        // GMT timezone
        final TimestampDriver timestampDriver = timestampType.getDriver();
        LongList out = new LongList();
        String interval = "2024-01-15T08:00@GMT";
        parseBracketInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
        Assert.assertEquals(2, out.size());
        String expected = ColumnType.isTimestampNano(timestampType.getTimestampType())
                ? "[{lo=2024-01-15T08:00:00.000000000Z, hi=2024-01-15T08:00:59.999999999Z}]"
                : "[{lo=2024-01-15T08:00:00.000000Z, hi=2024-01-15T08:00:59.999999Z}]";
        TestUtils.assertEquals(expected, intervalToString(timestampDriver, out));
    }

    @Test
    public void testTimezoneIncompleteOffset() {
        // Test incomplete offsets - potential NPE cases
        final TimestampDriver timestampDriver = timestampType.getDriver();
        String[] testCases = {"2024-01-15T08:00@+", "2024-01-15T08:00@-", "2024-01-15T08:00@+:", "2024-01-15T08:00@+00"};
        for (String interval : testCases) {
            LongList out = new LongList();
            try {
                parseBracketInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
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
        // Test invalid timezone name - should throw error
        final TimestampDriver timestampDriver = timestampType.getDriver();
        LongList out = new LongList();
        String interval = "2024-01-15T08:00@InvalidZone";
        try {
            parseBracketInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
            Assert.fail("Expected SqlException for invalid timezone");
        } catch (SqlException e) {
            Assert.assertTrue("Error should mention invalid timezone", e.getMessage().contains("invalid timezone"));
        }
    }

    @Test
    public void testTimezoneInvalidTimezone() {
        // Invalid timezone should throw error
        assertBracketIntervalError("2024-01-15T08:00@InvalidTz", "invalid timezone");
    }

    @Test
    public void testTimezoneMalformedOffset() {
        // Test malformed offset @+03:x0 - might bypass numeric parsing and cause NPE
        final TimestampDriver timestampDriver = timestampType.getDriver();
        LongList out = new LongList();
        String interval = "2024-01-15T08:00@+02:x0";
        try {
            parseBracketInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
            Assert.fail("Expected SqlException for malformed timezone");
        } catch (SqlException e) {
            // Expected - invalid timezone
            Assert.assertTrue("Error should mention invalid timezone", e.getMessage().contains("invalid timezone"));
        } catch (NullPointerException e) {
            Assert.fail("NPE for malformed offset - need null check");
        }
    }

    @Test
    public void testTimezoneNamedTimezone() throws SqlException {
        // Simple timestamp with named timezone Europe/London
        // 2024-01-15T08:00 in Europe/London (winter, UTC+0) = 2024-01-15T08:00:00Z in UTC
        final TimestampDriver timestampDriver = timestampType.getDriver();
        LongList out = new LongList();
        String interval = "2024-01-15T08:00@Europe/London";
        parseBracketInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
        Assert.assertEquals(2, out.size());
        String expected = ColumnType.isTimestampNano(timestampType.getTimestampType())
                ? "[{lo=2024-01-15T08:00:00.000000000Z, hi=2024-01-15T08:00:59.999999999Z}]"
                : "[{lo=2024-01-15T08:00:00.000000Z, hi=2024-01-15T08:00:59.999999Z}]";
        TestUtils.assertEquals(expected, intervalToString(timestampDriver, out));
    }

    @Test
    public void testTimezoneNamedTimezoneSummer() throws SqlException {
        // Named timezone in summer (Europe/London is UTC+1 in July)
        // 2024-07-15T08:00 in Europe/London (summer, UTC+1) = 2024-07-15T07:00:00Z in UTC
        final TimestampDriver timestampDriver = timestampType.getDriver();
        LongList out = new LongList();
        String interval = "2024-07-15T08:00@Europe/London";
        parseBracketInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
        Assert.assertEquals(2, out.size());
        String expected = ColumnType.isTimestampNano(timestampType.getTimestampType())
                ? "[{lo=2024-07-15T07:00:00.000000000Z, hi=2024-07-15T07:00:59.999999999Z}]"
                : "[{lo=2024-07-15T07:00:00.000000Z, hi=2024-07-15T07:00:59.999999Z}]";
        TestUtils.assertEquals(expected, intervalToString(timestampDriver, out));
    }

    @Test
    public void testTimezoneNegativeOffset() throws SqlException {
        // Test negative UTC offset - 08:00 in -05:00 = 13:00 UTC
        final TimestampDriver timestampDriver = timestampType.getDriver();
        LongList out = new LongList();
        String interval = "2024-01-15T08:00@-05:00";
        parseBracketInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
        Assert.assertEquals(2, out.size());
        String expected = ColumnType.isTimestampNano(timestampType.getTimestampType())
                ? "[{lo=2024-01-15T13:00:00.000000000Z, hi=2024-01-15T13:00:59.999999999Z}]"
                : "[{lo=2024-01-15T13:00:00.000000Z, hi=2024-01-15T13:00:59.999999Z}]";
        TestUtils.assertEquals(expected, intervalToString(timestampDriver, out));
    }

    @Test
    public void testTimezoneRangeExpansion() throws SqlException {
        // Range expansion with timezone
        // 2024-01-[15..17]T08:00@+02:00
        final TimestampDriver timestampDriver = timestampType.getDriver();
        LongList out = new LongList();
        String interval = "2024-01-[15..17]T08:00@+02:00";
        parseBracketInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
        Assert.assertEquals(6, out.size()); // Three intervals
        String expected = ColumnType.isTimestampNano(timestampType.getTimestampType())
                ? "[{lo=2024-01-15T06:00:00.000000000Z, hi=2024-01-15T06:00:59.999999999Z},{lo=2024-01-16T06:00:00.000000000Z, hi=2024-01-16T06:00:59.999999999Z},{lo=2024-01-17T06:00:00.000000000Z, hi=2024-01-17T06:00:59.999999999Z}]"
                : "[{lo=2024-01-15T06:00:00.000000Z, hi=2024-01-15T06:00:59.999999Z},{lo=2024-01-16T06:00:00.000000Z, hi=2024-01-16T06:00:59.999999Z},{lo=2024-01-17T06:00:00.000000Z, hi=2024-01-17T06:00:59.999999Z}]";
        TestUtils.assertEquals(expected, intervalToString(timestampDriver, out));
    }

    @Test
    public void testTimezoneSimpleNegativeOffset() throws SqlException {
        // Simple timestamp with negative offset -05:00
        // 2024-01-15T08:00 in -05:00 = 2024-01-15T13:00:00Z in UTC
        final TimestampDriver timestampDriver = timestampType.getDriver();
        LongList out = new LongList();
        String interval = "2024-01-15T08:00@-05:00";
        parseBracketInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
        Assert.assertEquals(2, out.size());
        String expected = ColumnType.isTimestampNano(timestampType.getTimestampType())
                ? "[{lo=2024-01-15T13:00:00.000000000Z, hi=2024-01-15T13:00:59.999999999Z}]"
                : "[{lo=2024-01-15T13:00:00.000000Z, hi=2024-01-15T13:00:59.999999Z}]";
        TestUtils.assertEquals(expected, intervalToString(timestampDriver, out));
    }

    @Test
    public void testTimezoneSimpleNumericOffset() throws SqlException {
        // Simple timestamp with numeric offset +03:00
        // 2024-01-15T08:00 in +03:00 = 2024-01-15T05:00:00Z in UTC
        final TimestampDriver timestampDriver = timestampType.getDriver();
        LongList out = new LongList();
        String interval = "2024-01-15T08:00@+03:00";
        parseBracketInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
        Assert.assertEquals(2, out.size());
        String expected = ColumnType.isTimestampNano(timestampType.getTimestampType())
                ? "[{lo=2024-01-15T05:00:00.000000000Z, hi=2024-01-15T05:00:59.999999999Z}]"
                : "[{lo=2024-01-15T05:00:00.000000Z, hi=2024-01-15T05:00:59.999999Z}]";
        TestUtils.assertEquals(expected, intervalToString(timestampDriver, out));
    }

    @Test
    public void testTimezoneUTC() throws SqlException {
        // UTC timezone (should be no-op essentially)
        final TimestampDriver timestampDriver = timestampType.getDriver();
        LongList out = new LongList();
        String interval = "2024-01-15T08:00@UTC";
        parseBracketInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
        Assert.assertEquals(2, out.size());
        String expected = ColumnType.isTimestampNano(timestampType.getTimestampType())
                ? "[{lo=2024-01-15T08:00:00.000000000Z, hi=2024-01-15T08:00:59.999999999Z}]"
                : "[{lo=2024-01-15T08:00:00.000000Z, hi=2024-01-15T08:00:59.999999Z}]";
        TestUtils.assertEquals(expected, intervalToString(timestampDriver, out));
    }

    @Test
    public void testTimezoneWithBracketExpansion() throws SqlException {
        // Bracket expansion with timezone
        // 2024-01-[15,16]T08:00@+02:00 -> two intervals in UTC
        final TimestampDriver timestampDriver = timestampType.getDriver();
        LongList out = new LongList();
        String interval = "2024-01-[15,16]T08:00@+02:00";
        parseBracketInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
        Assert.assertEquals(4, out.size()); // Two intervals, 2 longs each
        String expected = ColumnType.isTimestampNano(timestampType.getTimestampType())
                ? "[{lo=2024-01-15T06:00:00.000000000Z, hi=2024-01-15T06:00:59.999999999Z},{lo=2024-01-16T06:00:00.000000000Z, hi=2024-01-16T06:00:59.999999999Z}]"
                : "[{lo=2024-01-15T06:00:00.000000Z, hi=2024-01-15T06:00:59.999999Z},{lo=2024-01-16T06:00:00.000000Z, hi=2024-01-16T06:00:59.999999Z}]";
        TestUtils.assertEquals(expected, intervalToString(timestampDriver, out));
    }

    @Test
    public void testTimezoneWithBracketExpansionAndDuration() throws SqlException {
        // Bracket expansion with timezone and duration
        // 2024-01-[15,16]T08:00@+02:00;1h
        // 08:00 is minute-level, so hi = 08:00:59, then +1h = 09:00:59
        // Convert to UTC: lo = 08:00 - 2h = 06:00, hi = 09:00:59 - 2h = 07:00:59
        final TimestampDriver timestampDriver = timestampType.getDriver();
        LongList out = new LongList();
        String interval = "2024-01-[15,16]T08:00@+02:00;1h";
        parseBracketInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
        Assert.assertEquals(4, out.size());
        String expected = ColumnType.isTimestampNano(timestampType.getTimestampType())
                ? "[{lo=2024-01-15T06:00:00.000000000Z, hi=2024-01-15T07:00:59.999999999Z},{lo=2024-01-16T06:00:00.000000000Z, hi=2024-01-16T07:00:59.999999999Z}]"
                : "[{lo=2024-01-15T06:00:00.000000Z, hi=2024-01-15T07:00:59.999999Z},{lo=2024-01-16T06:00:00.000000Z, hi=2024-01-16T07:00:59.999999Z}]";
        TestUtils.assertEquals(expected, intervalToString(timestampDriver, out));
    }

    @Test
    public void testTimezoneWithDurationSuffix() throws SqlException {
        // Timezone before duration suffix
        // 2024-01-15T08:00@+03:00;1h
        // 08:00 is minute-level, so hi = 08:00:59, then +1h = 09:00:59
        // Convert to UTC: lo = 08:00 - 3h = 05:00, hi = 09:00:59 - 3h = 06:00:59
        final TimestampDriver timestampDriver = timestampType.getDriver();
        LongList out = new LongList();
        String interval = "2024-01-15T08:00@+03:00;1h";
        parseBracketInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
        Assert.assertEquals(2, out.size());
        String expected = ColumnType.isTimestampNano(timestampType.getTimestampType())
                ? "[{lo=2024-01-15T05:00:00.000000000Z, hi=2024-01-15T06:00:59.999999999Z}]"
                : "[{lo=2024-01-15T05:00:00.000000Z, hi=2024-01-15T06:00:59.999999Z}]";
        TestUtils.assertEquals(expected, intervalToString(timestampDriver, out));
    }

    @Test
    public void testTimezoneZ() {
        // Test @Z - potential NPE if getZoneRules returns null
        final TimestampDriver timestampDriver = timestampType.getDriver();
        LongList out = new LongList();
        String interval = "2024-01-15T08:00@Z";
        try {
            parseBracketInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
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
        final TimestampDriver timestampDriver = timestampType.getDriver();
        LongList out = new LongList();
        String interval = "2024-01-15T08:00@+00:00";
        parseBracketInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
        Assert.assertEquals(2, out.size());
        String expected = ColumnType.isTimestampNano(timestampType.getTimestampType())
                ? "[{lo=2024-01-15T08:00:00.000000000Z, hi=2024-01-15T08:00:59.999999999Z}]"
                : "[{lo=2024-01-15T08:00:00.000000Z, hi=2024-01-15T08:00:59.999999Z}]";
        TestUtils.assertEquals(expected, intervalToString(timestampDriver, out));
    }

    @Test
    public void testWhitespaceOnlyInput() {
        // All whitespace input (exercises line 426 - firstNonSpace reaches lim)
        assertBracketIntervalError("   ", "Invalid date");
    }

    private void assertBracketInterval(String expected, String interval) throws SqlException {
        LongList out = new LongList();
        final TimestampDriver timestampDriver = timestampType.getDriver();
        parseBracketInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
        TestUtils.assertEquals(
                ColumnType.isTimestampNano(timestampType.getTimestampType())
                        ? expected.replaceAll("00Z", "00000Z").replaceAll("99Z", "99999Z")
                        : expected,
                intervalToString(timestampDriver, out)
        );
    }

    private void assertBracketIntervalError(String interval, String expectedError) {
        try {
            final TimestampDriver timestampDriver = timestampType.getDriver();
            LongList out = new LongList();
            parseBracketInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
            Assert.fail("Expected SqlException with message containing: " + expectedError);
        } catch (SqlException e) {
            Assert.assertTrue("Expected error message to contain '" + expectedError + "' but got: " + e.getMessage(),
                    e.getMessage().contains(expectedError));
        }
    }

    private void assertDateFloor(String expected, String value) throws NumericException {
        sink.clear();
        final TimestampDriver timestampDriver = timestampType.getDriver();
        long t = timestampDriver.parseFloorLiteral(value);
        timestampDriver.append(sink, t);
        TestUtils.assertEquals(
                ColumnType.isTimestampNano(timestampType.getTimestampType())
                        ? expected.replaceAll("Z", "000Z").replaceAll("999999Z", "999999999Z")
                        : expected,
                sink
        );
    }

    private void assertIntersect(String expected) {
        out.add(a);
        out.add(b);
        IntervalUtils.intersectInPlace(out, a.size());
        TestUtils.assertEquals(
                ColumnType.isTimestampNano(timestampType.getTimestampType()) ?
                        expected.replaceAll("000000Z", "000000000Z").replaceAll("999999Z", "999999999Z")
                        : expected,
                intervalToString(timestampType.getDriver(), out)
        );
    }

    private void assertIntervalError(String interval) {
        try {
            final TimestampDriver timestampDriver = timestampType.getDriver();
            parseBracketInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
            Assert.fail();
        } catch (SqlException ignore) {
        }
    }

    private void assertShortInterval(String expected, String interval) throws SqlException {
        LongList out = new LongList();
        final TimestampDriver timestampDriver = timestampType.getDriver();
        parseBracketInterval(timestampDriver, interval, 0, interval.length(), 0, out, IntervalOperation.INTERSECT);
        TestUtils.assertEquals(
                ColumnType.isTimestampNano(timestampType.getTimestampType())
                        ? expected.replaceAll("00Z", "00000Z").replaceAll("99Z", "99999Z")
                        : expected,
                intervalToString(timestampDriver, out)
        );
    }
}
