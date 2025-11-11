/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.cairo;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TimestampDriver;
import io.questdb.std.Chars;
import io.questdb.std.NumericException;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.CommonUtils;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.DateLocaleFactory;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8String;
import io.questdb.test.TestTimestampType;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static io.questdb.test.tools.TestUtils.maxDayOfMonth;
import static io.questdb.test.tools.TestUtils.putWithLeadingZeroIfNeeded;

@RunWith(Parameterized.class)
public class PartitionByTest {
    private static final StringSink sink = new StringSink();
    private final TestTimestampType timestampType;

    public PartitionByTest(TestTimestampType timestampType) {
        this.timestampType = timestampType;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {TestTimestampType.MICRO}, {TestTimestampType.NANO}
        });
    }

    @Before
    public void setUp() {
        sink.clear();
    }

    @Test
    public void testAddCeilFloorDay() throws NumericException {
        testAddCeilFloor(
                "2023-03-02T00:00:00.000000Z",
                PartitionBy.DAY,
                "2023-03-01T00:00:00.000000Z",
                "2023-03-01T11:22:00.000000Z"
        );
    }

    @Test
    public void testAddCeilFloorDayEdge() throws NumericException {
        testAddCeilFloor(
                "2023-03-02T00:00:00.000000Z",
                PartitionBy.DAY,
                "2023-03-01T00:00:00.000000Z",
                "2023-03-01T00:00:00.000000Z"
        );
    }

    @Test
    public void testAddCeilFloorHour() throws NumericException {
        testAddCeilFloor(
                "2021-02-12T17:00:00.000000Z",
                PartitionBy.HOUR,
                "2021-02-12T16:00:00.000000Z",
                "2021-02-12T16:38:00.000000Z"
        );
    }

    @Test
    public void testAddCeilFloorHourEdge() throws NumericException {
        testAddCeilFloor(
                "2021-02-12T17:00:00.000000Z",
                PartitionBy.HOUR,
                "2021-02-12T16:00:00.000000Z",
                "2021-02-12T16:00:00.000000Z"
        );
    }

    @Test
    public void testAddCeilFloorMonth() throws NumericException {
        testAddCeilFloor(
                "2023-04-01T00:00:00.000000Z",
                PartitionBy.MONTH,
                "2023-03-01T00:00:00.000000Z",
                "2023-03-16T11:22:00.000000Z"
        );
    }

    @Test
    public void testAddCeilFloorMonthEdge() throws NumericException {
        testAddCeilFloor(
                "2023-04-01T00:00:00.000000Z",
                PartitionBy.MONTH,
                "2023-03-01T00:00:00.000000Z",
                "2023-03-01T00:00:00.000000Z"
        );
    }

    @Test
    public void testAddCeilFloorNone() {
        TimestampDriver.PartitionAddMethod addMethod = PartitionBy.getPartitionAddMethod(timestampType.getTimestampType(), PartitionBy.NONE);
        Assert.assertNull(addMethod);
        TimestampDriver.TimestampFloorMethod floorMethod = PartitionBy.getPartitionFloorMethod(timestampType.getTimestampType(), PartitionBy.NONE);
        Assert.assertNull(floorMethod);
        TimestampDriver.TimestampCeilMethod ceilMethod = PartitionBy.getPartitionCeilMethod(timestampType.getTimestampType(), PartitionBy.NONE);
        Assert.assertNull(ceilMethod);
    }

    @Test
    public void testAddCeilFloorWeek() throws NumericException {
        testAddCeilFloor(
                "2022-01-03T00:00:00.000000Z",
                PartitionBy.WEEK,
                "2021-12-27T00:00:00.000000Z",
                "2022-01-01T11:22:00.000000Z"
        );
    }

    @Test
    public void testAddCeilFloorWeekEdge() throws NumericException {
        testAddCeilFloor(
                "2022-01-03T00:00:00.000000Z",
                PartitionBy.WEEK,
                "2021-12-27T00:00:00.000000Z",
                "2021-12-27T00:00:00.000000Z"
        );
    }

    @Test
    public void testAddCeilFloorYear() throws NumericException {
        testAddCeilFloor(
                "2024-01-01T00:00:00.000000Z",
                PartitionBy.YEAR,
                "2023-01-01T00:00:00.000000Z",
                "2023-03-17T11:22:00.000000Z"
        );
    }

    @Test
    public void testAddCeilFloorYearEdge() throws NumericException {
        testAddCeilFloor(
                "2024-01-01T00:00:00.000000Z",
                PartitionBy.YEAR,
                "2023-01-01T00:00:00.000000Z",
                "2023-01-01T00:00:00.000000Z"
        );
    }

    @Test
    public void testCeilWeek() {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        long start = timestampDriver.fromDays(0);
        for (int i = 0; i < 3 * 366; i++) {
            long timestamp = start + timestampDriver.fromDays(i);
            String date = timestampDriver.toMSecString(timestamp);

            long ceil = PartitionBy.getPartitionCeilMethod(timestampType.getTimestampType(), PartitionBy.WEEK).ceil(timestamp);
            String ceilDate = timestampDriver.toMSecString(ceil);
            String message = "ceil(" + date + ")=" + ceilDate;

            Assert.assertEquals(message, 1, timestampDriver.getDayOfWeek(ceil));
            Assert.assertTrue(message, ceil > timestamp);
            Assert.assertTrue(message, ceil <= timestamp + timestampDriver.fromWeeks(1));
        }
    }

    @Test
    public void testCeilWeekEpoch() {
        TimestampDriver.TimestampCeilMethod ceilMethod = PartitionBy.getPartitionCeilMethod(timestampType.getTimestampType(), PartitionBy.WEEK);
        long floor1 = ceilMethod.ceil(0);
        // negative timestamps are treated as zero
        long floor2 = ceilMethod.ceil(-1000);
        Assert.assertEquals(floor1, floor2);
    }

    @Test
    public void testDaySplit() throws NumericException {
        assertFormatAndParse("2013-03-31T175500", "2013-03-31T17:55:00.000000Z", PartitionBy.DAY);
        assertFormatAndParse(ColumnType.isTimestampNano(timestampType.getTimestampType()) ? "2013-03-31T175501-123000100" : "2013-03-31T175501-123000", "2013-03-31T17:55:01.123000100Z", PartitionBy.DAY);
        assertFormatAndParse("2013-03-01T17", "2013-03-01T17:00:00.000000Z", PartitionBy.DAY);
    }

    @Test
    public void testDaySplitFuzz() {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        Rnd rnd = TestUtils.generateRandom(null);
        testDaySplitFuzz(PartitionBy.DAY, 1, rnd);
        testDaySplitFuzz(PartitionBy.DAY, 1000, rnd);
        testDaySplitFuzz(PartitionBy.DAY, timestampDriver.fromSeconds(1), rnd);
        testDaySplitFuzz(PartitionBy.DAY, timestampDriver.fromHours(1), rnd);
        testDaySplitFuzz(PartitionBy.DAY, timestampDriver.fromDays(1), rnd);
    }

    @Test
    public void testDirectoryFormattingDay() throws NumericException {
        assertFormatAndParse("2013-03-31", "2013-03-31T00:00:00.000000Z", PartitionBy.DAY);
        assertFormatAndParse("2013-03-31", "2013-03-31T00:00:00Z", PartitionBy.DAY);
        assertFormatAndParse("2013-03-31", "2013-03-31T00", PartitionBy.DAY);
        assertFormatAndParse("2013-03-31", "2013-03-31", PartitionBy.DAY);
    }

    @Test
    public void testDirectoryFormattingHour() throws NumericException {
        assertFormatAndParse("2014-09-03T21", "2014-09-03T21:00:00.000000Z", PartitionBy.HOUR);
        assertFormatAndParse("2014-09-03T21", "2014-09-03T21:00:00Z", PartitionBy.HOUR);
        assertFormatAndParse("2014-09-03T21", "2014-09-03T21", PartitionBy.HOUR);
    }

    @Test
    public void testDirectoryFormattingMonth() throws NumericException {
        assertFormatAndParse("2013-03", "2013-03-01T00:00:00.000000Z", PartitionBy.MONTH);
        assertFormatAndParse("2013-03", "2013-03-01T00:00:00Z", PartitionBy.MONTH);
        assertFormatAndParse("2013-03", "2013-03-01T00", PartitionBy.MONTH);
        assertFormatAndParse("2013-03", "2013-03-01", PartitionBy.MONTH);
        assertFormatAndParse("2013-03", "2013-03", PartitionBy.MONTH);
    }

    @Test
    public void testDirectoryFormattingNone() throws NumericException {
        assertFormatAndParse("default", "1970-01-01T00:00:00.000000Z", PartitionBy.NONE);
        assertFormatAndParse("default", "1970-01-01T00:00:00Z", PartitionBy.NONE);
        assertFormatAndParse("default", "1970-01-01T00", PartitionBy.NONE);
        assertFormatAndParse("default", "1970-01-01", PartitionBy.NONE);
        assertFormatAndParse("default", "1970-01", PartitionBy.NONE);
        assertFormatAndParse("default", "1970", PartitionBy.NONE);
    }

    @Test
    public void testDirectoryFormattingWeek() throws NumericException {
        assertFormatAndParse("2020-W53", "2020-12-28T00:00:00.000000Z", PartitionBy.WEEK);
        assertFormatAndParse("2020-W01", "2019-12-30T00:00:00.000000Z", PartitionBy.WEEK);
        assertFormatAndParse("2021-W33", "2021-08-16T00:00:00.000000Z", PartitionBy.WEEK);
        assertFormatAndParse("2013-W09-5", "2013-03-01T00:00:00.000000Z", PartitionBy.WEEK);
        assertFormatAndParse("2013-W09-5", "2013-03-01T00:00:00Z", PartitionBy.WEEK);
        assertFormatAndParse("2013-W09-5", "2013-03-01T00", PartitionBy.WEEK);
        assertFormatAndParse("2013-W09-5", "2013-03-01", PartitionBy.WEEK);
    }

    @Test
    public void testDirectoryFormattingYear() throws NumericException {
        assertFormatAndParse("2014", "2014-01-01T00:00:00.000000Z", PartitionBy.YEAR);
        assertFormatAndParse("2014", "2014-01-01T00:00:00Z", PartitionBy.YEAR);
        assertFormatAndParse("2014", "2014-01-01T00", PartitionBy.YEAR);
        assertFormatAndParse("2014", "2014-01-01", PartitionBy.YEAR);
        assertFormatAndParse("2014", "2014-01", PartitionBy.YEAR);
        assertFormatAndParse("2014", "2014", PartitionBy.YEAR);
    }

    @Test
    public void testDirectoryParseFailureByDay() {
        assertParseFailure("'yyyy-MM-dd' expected, found [ts=2013-03]", "2013-03", PartitionBy.DAY);
    }

    @Test
    public void testDirectoryParseFailureByHour() {
        assertParseFailure("'yyyy-MM-ddTHH' expected, found [ts=2013-03-12]", "2013-03-12", PartitionBy.HOUR);
    }

    @Test
    public void testDirectoryParseFailureByMonth() {
        assertParseFailure("'yyyy-MM' expected, found [ts=2013-0-12]", "2013-0-12", PartitionBy.MONTH);
    }

    @Test
    public void testDirectoryParseFailureByWeek() {
        assertParseFailure("'YYYY-Www' or 'yyyy-MM-dd' expected, found [ts=2013-0-12]", "2013-0-12", PartitionBy.WEEK);
    }

    @Test
    public void testDirectoryParseFailureByYear() {
        assertParseFailure("'yyyy' expected, found [ts=201-03-12]", "201-03-12", PartitionBy.YEAR);
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testFloorWeek() {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        long start = timestampDriver.fromDays(0);
        for (int i = 0; i < 3 * 366; i++) {
            long timestamp = start + timestampDriver.fromDays(i);
            String date = timestampDriver.toMSecString(timestamp);

            long floor = PartitionBy.getPartitionFloorMethod(timestampType.getTimestampType(), PartitionBy.WEEK).floor(timestamp);
            String floorDate = timestampDriver.toMSecString(floor);
            String message = "floor(" + date + ")=" + floorDate;

            Assert.assertEquals(message, 1, timestampDriver.getDayOfWeek(floor));
            Assert.assertTrue(message, floor <= timestamp);
            Assert.assertTrue(message, floor + timestampDriver.fromWeeks(1) > timestamp);
        }
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testFloorWeekEpoch() {
        TimestampDriver.TimestampFloorMethod floorMethod = PartitionBy.getPartitionFloorMethod(timestampType.getTimestampType(), PartitionBy.WEEK);
        long floor1 = floorMethod.floor(0);
        long floor2 = floorMethod.floor(floor1);
        Assert.assertEquals(floor1, floor2);
        // negative timestamps are treated as zero
        floor2 = floorMethod.floor(-1000);
        Assert.assertEquals(floor1, floor2);
    }

    @Test
    public void testHourSplit() throws NumericException {
        assertFormatAndParse("2013-03-31T175500", "2013-03-31T17:55:00.000000Z", PartitionBy.HOUR);
        assertFormatAndParse(ColumnType.isTimestampNano(timestampType.getTimestampType()) ? "2013-03-31T175501-123000101" : "2013-03-31T175501-123000", "2013-03-31T17:55:01.123000101Z", PartitionBy.HOUR);
        assertFormatAndParse("2013-03-01T17", "2013-03-01T17:00:00.000000Z", PartitionBy.HOUR);
        assertFormatAndParse(ColumnType.isTimestampNano(timestampType.getTimestampType()) ? "2013-03-31T175501-123020101" : "2013-03-31T175501-123020", "2013-03-31T17:55:01.123020101Z", PartitionBy.HOUR);
        assertFormatAndParse("2013-03-31T00", "2013-03-31T00:00:00.000000Z", PartitionBy.HOUR);
    }

    @Test
    public void testHourSplitFuzz() {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        Rnd rnd = TestUtils.generateRandom(null);
        testDaySplitFuzz(PartitionBy.HOUR, 1, rnd);
        testDaySplitFuzz(PartitionBy.HOUR, 1000, rnd);
        testDaySplitFuzz(PartitionBy.HOUR, timestampDriver.fromSeconds(1), rnd);
        testDaySplitFuzz(PartitionBy.HOUR, timestampDriver.fromHours(1), rnd);
        testDaySplitFuzz(PartitionBy.HOUR, timestampDriver.fromDays(1), rnd);
    }

    @Test
    public void testIsPartitioned() {
        Assert.assertTrue(PartitionBy.isPartitioned(PartitionBy.DAY));
        Assert.assertTrue(PartitionBy.isPartitioned(PartitionBy.MONTH));
        Assert.assertTrue(PartitionBy.isPartitioned(PartitionBy.YEAR));
        Assert.assertTrue(PartitionBy.isPartitioned(PartitionBy.HOUR));
        Assert.assertTrue(PartitionBy.isPartitioned(PartitionBy.WEEK));
        Assert.assertFalse(PartitionBy.isPartitioned(PartitionBy.NONE));
    }

    @Test
    public void testMonthSplit() throws NumericException {
        assertFormatAndParse("2013-03-31T175500", "2013-03-31T17:55:00.000000Z", PartitionBy.MONTH);
        assertFormatAndParse(ColumnType.isTimestampNano(timestampType.getTimestampType()) ? "2013-03-31T175501-123000101" : "2013-03-31T175501-123000", "2013-03-31T17:55:01.123000101Z", PartitionBy.MONTH);
        assertFormatAndParse("2013-03-01T17", "2013-03-01T17:00:00.000000Z", PartitionBy.MONTH);
        assertFormatAndParse("2013-03", "2013-03-01T00:00:00.000000Z", PartitionBy.MONTH);
    }

    @Test
    public void testMonthSplitFuzz() {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        Rnd rnd = TestUtils.generateRandom(null);
        testDaySplitFuzz(PartitionBy.MONTH, 1, rnd);
        testDaySplitFuzz(PartitionBy.MONTH, 1000, rnd);
        testDaySplitFuzz(PartitionBy.MONTH, timestampDriver.fromSeconds(1), rnd);
        testDaySplitFuzz(PartitionBy.MONTH, timestampDriver.fromHours(1), rnd);
        testDaySplitFuzz(PartitionBy.MONTH, timestampDriver.fromDays(1), rnd);
    }

    @Test
    public void testParseFloor() throws NumericException {
        checkPartitionPartialParseHour(PartitionBy.HOUR);
        checkPartitionPartialParseDay(PartitionBy.DAY);
        checkPartitionPartialParseMonth(PartitionBy.MONTH);
        checkPartitionPartialParseMonth(PartitionBy.YEAR);
        Assert.assertEquals(
                timestampType.getDriver().parseFloorLiteral("2013"),
                PartitionBy.parsePartitionDirName("2013", timestampType.getTimestampType(), PartitionBy.YEAR)
        );
    }

    @Test
    public void testParseFloorFailsMonthTooShort() {
        assertParseFails("2013-1", PartitionBy.MONTH);
    }

    @Test
    public void testParseFloorFailsTooManyDaysDigits() {
        assertParseFails("2019-02-003", PartitionBy.DAY);
    }

    @Test
    public void testParseFloorFailsTooManyMonthsDigits() {
        assertParseFails("2019-003", PartitionBy.DAY);
    }

    @Test
    public void testParseFloorFailsTooManyTimeDigits() {
        assertParseFails("2019-02-01T12234509", PartitionBy.DAY);
    }

    @Test
    public void testParseFloorFailsTooManyUsecs() {
        assertParseFails("2013-03-31T175501-12302123112", PartitionBy.DAY);
    }

    @Test
    public void testParseFloorFailsTooShort() {
        assertParseFails("201", PartitionBy.DAY);
    }

    @Test
    public void testParseFloorFailsWithTimezone() {
        assertParseFails("2019-02-01T122345-23450b", PartitionBy.DAY);
    }

    @Test
    public void testPartitionByNameDay() {
        testPartitionByName("DAY", PartitionBy.DAY);
    }

    @Test
    public void testPartitionByNameHour() {
        testPartitionByName("HOUR", PartitionBy.HOUR);
    }

    @Test
    public void testPartitionByNameMonth() {
        testPartitionByName("MONTH", PartitionBy.MONTH);
    }

    @Test
    public void testPartitionByNameNone() {
        testPartitionByName("NONE", PartitionBy.NONE);
    }

    @Test
    public void testPartitionByNameWeek() {
        testPartitionByName("WEEK", PartitionBy.WEEK);
    }

    @Test
    public void testPartitionByNameYear() {
        testPartitionByName("YEAR", PartitionBy.YEAR);
    }

    @Test
    public void testPartitionDayToWeekForWholeYear() throws NumericException {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final DateFormat weekFormat = PartitionBy.getPartitionDirFormatMethod(timestampType.getTimestampType(), PartitionBy.WEEK);
        final DateFormat dayFormat = PartitionBy.getPartitionDirFormatMethod(timestampType.getTimestampType(), PartitionBy.DAY);
        StringSink weekSink = new StringSink();
        StringSink dateSink = new StringSink();
        dateSink.put("2023-");
        final int yearLen = dateSink.length();
        for (int month = 1; month < 13; month++) {
            putWithLeadingZeroIfNeeded(dateSink, yearLen, month).put('-');
            final int monthLen = dateSink.length(); // yyyy-MM-
            for (int day = 1, maxDay = maxDayOfMonth(month); day < maxDay + 1; day++) {
                putWithLeadingZeroIfNeeded(dateSink, monthLen, day);
                final String expectedDayFormatted = dateSink.toString(); // yyyy-MM-dd
                final String expectedWeekFormatted;
                final long timestamp = timestampDriver.parseFloorLiteral(expectedDayFormatted);
                int year = timestampDriver.getYear(timestamp);
                int week = timestampDriver.getWeek(timestamp);
                if (week == 52 && month == 1) {
                    year--;
                }
                weekSink.clear();
                weekSink.put(year).put("-W");
                putWithLeadingZeroIfNeeded(weekSink, weekSink.length(), week);
                expectedWeekFormatted = weekSink.toString();

                // check formatting for day formatter
                sink.clear();
                dayFormat.format(timestamp, DateLocaleFactory.EN_LOCALE, null, sink);
                String dayFormatted = sink.toString();
                Assert.assertEquals(expectedDayFormatted, dayFormatted);

                // check formatting for week formatter
                sink.clear();
                weekFormat.format(timestamp, DateLocaleFactory.EN_LOCALE, null, sink);
                String weekFormatted = sink.toString();
                Assert.assertEquals(expectedWeekFormatted, weekFormatted.substring(0, 8));

                // assert that regardless of the format, when partitioned by week the timestamp
                // is the same, i.e. the first day of the week
                long weekTs = PartitionBy.parsePartitionDirName(weekFormatted, timestampType.getTimestampType(), PartitionBy.WEEK);
                long dayTs = PartitionBy.parsePartitionDirName(dayFormatted, timestampType.getTimestampType(), PartitionBy.DAY);
                Assert.assertEquals(weekTs, dayTs);
            }
        }
    }

    @Test
    public void testSetPathByDay() throws NumericException {
        setSetPath(
                "a/b/2018-10-12",
                "2018-10-12T00:00:00.000000Z",
                PartitionBy.DAY
        );
    }

    @Test
    public void testSetPathByHour() throws NumericException {
        setSetPath(
                "a/b/2021-04-01T18",
                "2021-04-01T18:00:00.000000Z",
                PartitionBy.HOUR
        );
    }

    @Test
    public void testSetPathByMonth() throws NumericException {
        setSetPath(
                "a/b/2021-04",
                "2021-04-01T00:00:00.000000Z",
                PartitionBy.MONTH
        );
    }

    @Test
    public void testSetPathByNone() throws NumericException {
        sink.put("a/b/");
        PartitionBy.setSinkForPartition(
                sink,
                timestampType.getTimestampType(),
                PartitionBy.NONE,
                timestampType.getDriver().parseFloorLiteral("2021-01-01T00:00:00.000000Z")
        );
        TestUtils.assertEquals("a/b/default", sink);
    }

    @Test
    public void testSetPathByWeek() throws NumericException {
        setSetPath(
                "a/b/2020-W53-5",
                "2021-01-01T00:00:00.000000Z",
                PartitionBy.WEEK
        );
    }

    @Test
    public void testSetPathByYear() throws NumericException {
        setSetPath(
                "a/b/2021",
                "2021-01-01T00:00:00.000000Z",
                PartitionBy.YEAR
        );
    }

    @Test
    public void testSetPathNoCalcByDay() throws NumericException {
        setSetPathNoCalc(
                "a/b/2018-10-12",
                "2018-10-12T00:00:00.000000Z",
                PartitionBy.DAY
        );
    }

    @Test
    public void testSetPathNoCalcByHour() throws NumericException {
        setSetPathNoCalc(
                "a/b/2021-04-01T18",
                "2021-04-01T18:00:00.000000Z",
                PartitionBy.HOUR
        );
    }

    @Test
    public void testSetPathNoCalcByMonth() throws NumericException {
        setSetPathNoCalc(
                "a/b/2021-04",
                "2021-04-01T00:00:00.000000Z",
                PartitionBy.MONTH
        );
    }

    @Test
    public void testSetPathNoCalcByWeek() throws NumericException {
        setSetPathNoCalc(
                "a/b/2020-W53-5",
                "2021-01-01T00:00:00.000000Z",
                PartitionBy.WEEK
        );
    }

    @Test
    public void testSetPathNoCalcByYear() throws NumericException {
        setSetPathNoCalc(
                "a/b/2021",
                "2021-01-01T00:00:00.000000Z",
                PartitionBy.YEAR
        );
    }

    @Test
    public void testUnknowns() {
        try {
            PartitionBy.getPartitionDirFormatMethod(timestampType.getTimestampType(), -1);
            Assert.fail();
        } catch (Exception ignored) {
            TestUtils.assertEquals("UNKNOWN", PartitionBy.toString(-1));
        }
    }

    @Test
    public void testWeekSplit() throws NumericException {
        assertFormatAndParse("2023-W13", "2023-03-27T00:00:00.000000Z", PartitionBy.WEEK);
        assertFormatAndParse(ColumnType.isTimestampNano(timestampType.getTimestampType()) ? "2023-W13-1T175501-123000101" : "2023-W13-1T175501-123000", "2023-03-27T17:55:01.123000101Z", PartitionBy.WEEK);
        assertFormatAndParse("2013-W09-5T17", "2013-03-01T17:00:00.000000Z", PartitionBy.WEEK);
        assertFormatAndParse("2013-W09-5", "2013-03-01T00:00:00.000000Z", PartitionBy.WEEK);
    }

    @Test
    public void testYearSplit() throws NumericException {
        assertFormatAndParse("2013-03-31T175500", "2013-03-31T17:55:00.000000Z", PartitionBy.YEAR);
        assertFormatAndParse(ColumnType.isTimestampNano(timestampType.getTimestampType()) ? "2013-03-31T175501-123000101" : "2013-03-31T175501-123000", "2013-03-31T17:55:01.123000101Z", PartitionBy.YEAR);
        assertFormatAndParse("2013-03-01T17", "2013-03-01T17:00:00.000000Z", PartitionBy.YEAR);
        assertFormatAndParse("2013", "2013-01-01T00:00:00.000000Z", PartitionBy.YEAR);
    }

    @Test
    public void testYearSplitFuzz() {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        Rnd rnd = TestUtils.generateRandom(null);
        testDaySplitFuzz(PartitionBy.YEAR, 1, rnd);
        testDaySplitFuzz(PartitionBy.YEAR, 1000, rnd);
        testDaySplitFuzz(PartitionBy.YEAR, timestampDriver.fromSeconds(1), rnd);
        testDaySplitFuzz(PartitionBy.YEAR, timestampDriver.fromHours(1), rnd);
        testDaySplitFuzz(PartitionBy.YEAR, timestampDriver.fromDays(1), rnd);
    }

    private void assertFormatAndParse(CharSequence expectedDirName, CharSequence timestampString, int partitionBy) throws NumericException {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        long expected = timestampDriver.parseFloorLiteral(timestampString);
        DateFormat dirFormatMethod = PartitionBy.getPartitionDirFormatMethod(timestampType.getTimestampType(), partitionBy);
        sink.clear();
        dirFormatMethod.format(expected, DateLocaleFactory.EN_LOCALE, null, sink);
        TestUtils.assertEquals(expectedDirName, sink);
        if (partitionBy == PartitionBy.WEEK) {
            int year = timestampDriver.getYear(expected);
            int week = timestampDriver.getWeek(expected);
            if (week == 52 && timestampDriver.getMonthOfYear(expected) == 1) {
                year--;
            }
            sink.clear();
            sink.put(year).put("-W");
            putWithLeadingZeroIfNeeded(sink, sink.length(), week);
            expected = timestampDriver.parseAnyFormat(sink, 0, CommonUtils.WEEK_PATTERN.length());
        }
        Assert.assertEquals(expected, PartitionBy.parsePartitionDirName(sink, timestampType.getTimestampType(), partitionBy));
    }

    private void assertParseFails(String partitionName, int partitionBy) {
        try {
            PartitionBy.parsePartitionDirName(partitionName, timestampType.getTimestampType(), partitionBy);
            Assert.fail("exception expected");
        } catch (Exception ignored) {
        }
    }

    private void assertParseFailure(CharSequence expected, CharSequence dirName, int partitionBy) {
        try {
            PartitionBy.parsePartitionDirName(dirName, timestampType.getTimestampType(), partitionBy);
            Assert.fail();
        } catch (CairoException e) {
            TestUtils.assertEquals(expected, e.getFlyweightMessage());
        }
    }

    private void checkPartitionPartialParseDay(int day) throws NumericException {
        checkPartitionPartialParseHour(day);
        final TimestampDriver timestampDriver = timestampType.getDriver();
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2013-03-31"), PartitionBy.parsePartitionDirName("2013-03-31", timestampType.getTimestampType(), day));
    }

    private void checkPartitionPartialParseHour(int partBy) throws NumericException {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2013-03-31T17:55:01.123021"),
                PartitionBy.parsePartitionDirName("2013-03-31T175501-123021", timestampType.getTimestampType(), partBy)
        );
        Assert.assertEquals(
                timestampDriver.parseFloorLiteral("2013-03-31T17:55:01.12302"),
                PartitionBy.parsePartitionDirName("2013-03-31T175501-12302", timestampType.getTimestampType(), partBy)
        );
        Assert.assertEquals(
                timestampDriver.parseFloorLiteral("2013-03-31T17:55:01.1230"),
                PartitionBy.parsePartitionDirName("2013-03-31T175501-1230", timestampType.getTimestampType(), partBy)
        );
        Assert.assertEquals(
                timestampDriver.parseFloorLiteral("2013-03-31T17:55:01.123"),
                PartitionBy.parsePartitionDirName("2013-03-31T175501-123", timestampType.getTimestampType(), partBy)
        );
        Assert.assertEquals(
                timestampDriver.parseFloorLiteral("2013-03-31T17:55:01.12"),
                PartitionBy.parsePartitionDirName("2013-03-31T175501-12", timestampType.getTimestampType(), partBy)
        );
        Assert.assertEquals(
                timestampDriver.parseFloorLiteral("2013-03-31T17:55:01.1"),
                PartitionBy.parsePartitionDirName("2013-03-31T175501-1", timestampType.getTimestampType(), partBy)
        );
        Assert.assertEquals(
                timestampDriver.parseFloorLiteral("2013-03-31T17:55:01"),
                PartitionBy.parsePartitionDirName("2013-03-31T175501", timestampType.getTimestampType(), partBy)
        );
        Assert.assertEquals(
                timestampDriver.parseFloorLiteral("2013-03-31T17:55"),
                PartitionBy.parsePartitionDirName("2013-03-31T1755", timestampType.getTimestampType(), partBy)
        );
        Assert.assertEquals(
                timestampDriver.parseFloorLiteral("2013-03-31T17"),
                PartitionBy.parsePartitionDirName("2013-03-31T17", timestampType.getTimestampType(), partBy)
        );
    }

    private void checkPartitionPartialParseMonth(int partitionBy) throws NumericException {
        checkPartitionPartialParseDay(partitionBy);
        final TimestampDriver timestampDriver = timestampType.getDriver();
        Assert.assertEquals(
                timestampDriver.parseFloorLiteral("2013-03"),
                PartitionBy.parsePartitionDirName("2013-03", timestampType.getTimestampType(), partitionBy)
        );
    }

    private void setSetPath(
            CharSequence expectedDirName,
            CharSequence timestamp,
            int partitionBy
    ) throws NumericException {
        sink.put("a/b/");
        final TimestampDriver timestampDriver = timestampType.getDriver();
        PartitionBy.setSinkForPartition(
                sink,
                timestampType.getTimestampType(),
                partitionBy,
                timestampDriver.parseFloorLiteral(timestamp)
        );
        TestUtils.assertEquals(expectedDirName, sink);
    }

    private void setSetPathNoCalc(
            CharSequence expectedDirName,
            CharSequence timestamp,
            int partitionBy
    ) throws NumericException {
        sink.put("a/b/");
        final TimestampDriver timestampDriver = timestampType.getDriver();
        PartitionBy.setSinkForPartition(
                sink,
                timestampType.getTimestampType(),
                partitionBy,
                timestampDriver.parseFloorLiteral(timestamp)
        );
        TestUtils.assertEquals(expectedDirName, sink);
    }

    private void testAddCeilFloor(
            CharSequence expectedNext,
            int partitionBy,
            CharSequence partitionTimestampStr,
            CharSequence midPartitionTimestampStr
    ) throws NumericException {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final long expectedNextPartitionTimestamp = timestampDriver.parseFloorLiteral(expectedNext);
        final long partitionTimestamp = timestampDriver.parseFloorLiteral(partitionTimestampStr);
        final long midPartitionTimestamp = timestampDriver.parseFloorLiteral(midPartitionTimestampStr);

        TimestampDriver.PartitionAddMethod addMethod = PartitionBy.getPartitionAddMethod(timestampType.getTimestampType(), partitionBy);
        Assert.assertNotNull(addMethod);

        TimestampDriver.TimestampFloorMethod floorMethod = PartitionBy.getPartitionFloorMethod(timestampType.getTimestampType(), partitionBy);
        Assert.assertNotNull(floorMethod);

        TimestampDriver.TimestampCeilMethod ceilMethod = PartitionBy.getPartitionCeilMethod(timestampType.getTimestampType(), partitionBy);
        Assert.assertNotNull(ceilMethod);

        Assert.assertEquals(expectedNextPartitionTimestamp, addMethod.calculate(partitionTimestamp, 1));
        Assert.assertEquals(partitionTimestamp, floorMethod.floor(midPartitionTimestamp));
        Assert.assertEquals(expectedNextPartitionTimestamp, ceilMethod.ceil(midPartitionTimestamp));
    }

    private void testDaySplitFuzz(int partitionBy, long multiplier, Rnd rnd) {
        StringSink tsSink = new StringSink();
        DateFormat formatter = PartitionBy.getPartitionDirFormatMethod(timestampType.getTimestampType(), partitionBy);

        final TimestampDriver timestampDriver = timestampType.getDriver();
        for (int i = 0; i < 10; i++) {
            long timestamp = rnd.nextLong(timestampDriver.fromDays(3000 * 365) / multiplier);
            tsSink.clear();
            formatter.format(timestamp, DateLocaleFactory.EN_LOCALE, null, tsSink);
            long actual = PartitionBy.parsePartitionDirName(tsSink, timestampType.getTimestampType(), partitionBy);

            Assert.assertEquals(tsSink.toString(), timestamp, actual);
        }
    }

    private void testPartitionByName(CharSequence expectedPartitionName, int partitionBy) {
        CharSequence partitionName = PartitionBy.toString(partitionBy);
        TestUtils.assertEquals(expectedPartitionName, partitionName);
        Assert.assertEquals(partitionBy, PartitionBy.fromString(partitionName));
        Assert.assertEquals(partitionBy, PartitionBy.fromString(Chars.toString(partitionName).toUpperCase()));
        Assert.assertEquals(partitionBy, PartitionBy.fromString(Chars.toString(partitionName).toLowerCase()));

        Assert.assertEquals(partitionBy, PartitionBy.fromUtf8String(new Utf8String(partitionName)));
        Assert.assertEquals(partitionBy, PartitionBy.fromUtf8String(new Utf8String(Chars.toString(partitionName).toUpperCase())));
        Assert.assertEquals(partitionBy, PartitionBy.fromUtf8String(new Utf8String(Chars.toString(partitionName).toLowerCase())));
    }
}
