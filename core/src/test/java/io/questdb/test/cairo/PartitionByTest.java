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
import io.questdb.cairo.PartitionBy;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.Chars;
import io.questdb.std.NumericException;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8String;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.test.tools.TestUtils.maxDayOfMonth;
import static io.questdb.test.tools.TestUtils.putWithLeadingZeroIfNeeded;

public class PartitionByTest {
    private static final StringSink sink = new StringSink();

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
        PartitionBy.PartitionAddMethod addMethod = PartitionBy.getPartitionAddMethod(PartitionBy.NONE);
        Assert.assertNull(addMethod);
        PartitionBy.PartitionFloorMethod floorMethod = PartitionBy.getPartitionFloorMethod(PartitionBy.NONE);
        Assert.assertNull(floorMethod);
        PartitionBy.PartitionCeilMethod ceilMethod = PartitionBy.getPartitionCeilMethod(PartitionBy.NONE);
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
    public void testCeilWeekBeforeAndAfterEpoch() {
        long start = -366 * Timestamps.DAY_MICROS;
        for (int i = 0; i < 2 * 366; i++) {
            long timestamp = start + i * Timestamps.DAY_MICROS;
            String date = Timestamps.toString(timestamp);

            long ceil = PartitionBy.getPartitionCeilMethod(PartitionBy.WEEK).ceil(timestamp);
            String ceilDate = Timestamps.toString(ceil);
            String message = "ceil(" + date + ")=" + ceilDate;

            Assert.assertEquals(message, 1, Timestamps.getDayOfWeek(ceil));
            Assert.assertTrue(message, ceil > timestamp);
            Assert.assertTrue(message, ceil <= timestamp + Timestamps.WEEK_MICROS);
        }
    }

    @Test
    public void testDaySplit() throws NumericException {
        assertFormatAndParse("2013-03-31T175500", "2013-03-31T17:55:00.000000Z", PartitionBy.DAY);
        assertFormatAndParse("2013-03-31T175501-123000", "2013-03-31T17:55:01.123000Z", PartitionBy.DAY);
        assertFormatAndParse("2013-03-01T17", "2013-03-01T17:00:00.000000Z", PartitionBy.DAY);
    }

    @Test
    public void testDaySplitFuzz() {
        Rnd rnd = TestUtils.generateRandom(null);
        testDaySplitFuzz(PartitionBy.DAY, 1, rnd);
        testDaySplitFuzz(PartitionBy.DAY, 1000, rnd);
        testDaySplitFuzz(PartitionBy.DAY, Timestamps.SECOND_MICROS, rnd);
        testDaySplitFuzz(PartitionBy.DAY, Timestamps.HOUR_MICROS, rnd);
        testDaySplitFuzz(PartitionBy.DAY, Timestamps.DAY_MICROS, rnd);
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
        long floor1 = PartitionBy.getPartitionFloorMethod(PartitionBy.WEEK).floor(0);
        long floor2 = PartitionBy.getPartitionFloorMethod(PartitionBy.WEEK).floor(floor1);
        Assert.assertEquals(floor1, floor2);
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testFloorWeekBeforeAndAfterEpoch() {
        long start = -366 * Timestamps.DAY_MICROS;
        for (int i = 0; i < 2 * 366; i++) {
            long timestamp = start + i * Timestamps.DAY_MICROS;
            String date = Timestamps.toString(timestamp);

            long floor = PartitionBy.getPartitionFloorMethod(PartitionBy.WEEK).floor(timestamp);
            String floorDate = Timestamps.toString(floor);
            String message = "floor(" + date + ")=" + floorDate;

            Assert.assertEquals(message, 1, Timestamps.getDayOfWeek(floor));
            Assert.assertTrue(message, floor <= timestamp);
            Assert.assertTrue(message, floor + Timestamps.WEEK_MICROS > timestamp);
        }
    }

    @Test
    public void testHourSplit() throws NumericException {
        assertFormatAndParse("2013-03-31T175500", "2013-03-31T17:55:00.000000Z", PartitionBy.HOUR);
        assertFormatAndParse("2013-03-31T175501-123000", "2013-03-31T17:55:01.123000Z", PartitionBy.HOUR);
        assertFormatAndParse("2013-03-01T17", "2013-03-01T17:00:00.000000Z", PartitionBy.HOUR);
        assertFormatAndParse("2013-03-31T175501-123020", "2013-03-31T17:55:01.123020Z", PartitionBy.HOUR);
        assertFormatAndParse("2013-03-31T00", "2013-03-31T00:00:00.000000Z", PartitionBy.HOUR);
    }

    @Test
    public void testHourSplitFuzz() {
        Rnd rnd = TestUtils.generateRandom(null);
        testDaySplitFuzz(PartitionBy.HOUR, 1, rnd);
        testDaySplitFuzz(PartitionBy.HOUR, 1000, rnd);
        testDaySplitFuzz(PartitionBy.HOUR, Timestamps.SECOND_MICROS, rnd);
        testDaySplitFuzz(PartitionBy.HOUR, Timestamps.HOUR_MICROS, rnd);
        testDaySplitFuzz(PartitionBy.HOUR, Timestamps.DAY_MICROS, rnd);
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
        assertFormatAndParse("2013-03-31T175501-123000", "2013-03-31T17:55:01.123000Z", PartitionBy.MONTH);
        assertFormatAndParse("2013-03-01T17", "2013-03-01T17:00:00.000000Z", PartitionBy.MONTH);
        assertFormatAndParse("2013-03", "2013-03-01T00:00:00.000000Z", PartitionBy.MONTH);
    }

    @Test
    public void testMonthSplitFuzz() {
        Rnd rnd = TestUtils.generateRandom(null);
        testDaySplitFuzz(PartitionBy.MONTH, 1, rnd);
        testDaySplitFuzz(PartitionBy.MONTH, 1000, rnd);
        testDaySplitFuzz(PartitionBy.MONTH, Timestamps.SECOND_MICROS, rnd);
        testDaySplitFuzz(PartitionBy.MONTH, Timestamps.HOUR_MICROS, rnd);
        testDaySplitFuzz(PartitionBy.MONTH, Timestamps.DAY_MICROS, rnd);
    }

    @Test
    public void testParseFloor() throws NumericException {
        checkPartitionPartialParseHour(PartitionBy.HOUR);
        checkPartitionPartialParseDay(PartitionBy.DAY);
        checkPartitionPartialParseMonth(PartitionBy.MONTH);
        checkPartitionPartialParseMonth(PartitionBy.YEAR);
        Assert.assertEquals(IntervalUtils.parseFloorPartialTimestamp("2013"), PartitionBy.parsePartitionDirName("2013", PartitionBy.YEAR));
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
        assertParseFails("2013-03-31T175501-12302123", PartitionBy.DAY);
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
        final DateFormat weekFormat = PartitionBy.getPartitionDirFormatMethod(PartitionBy.WEEK);
        final DateFormat dayFormat = PartitionBy.getPartitionDirFormatMethod(PartitionBy.DAY);
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
                final long timestamp = TimestampFormatUtils.parseTimestamp(expectedDayFormatted);
                int year = Timestamps.getYear(timestamp);
                int week = Timestamps.getWeek(timestamp);
                if (week == 52 && month == 1) {
                    year--;
                }
                weekSink.clear();
                weekSink.put(year).put("-W");
                putWithLeadingZeroIfNeeded(weekSink, weekSink.length(), week);
                expectedWeekFormatted = weekSink.toString();

                // check formatting for day formatter
                sink.clear();
                dayFormat.format(timestamp, TimestampFormatUtils.EN_LOCALE, null, sink);
                String dayFormatted = sink.toString();
                Assert.assertEquals(expectedDayFormatted, dayFormatted);

                // check formatting for week formatter
                sink.clear();
                weekFormat.format(timestamp, TimestampFormatUtils.EN_LOCALE, null, sink);
                String weekFormatted = sink.toString();
                Assert.assertEquals(expectedWeekFormatted, weekFormatted.substring(0, 8));

                // assert that regardless of the format, when partitioned by week the timestamp
                // is the same, ie. the first day of the week
                long weekTs = PartitionBy.parsePartitionDirName(weekFormatted, PartitionBy.WEEK);
                long dayTs = PartitionBy.parsePartitionDirName(dayFormatted, PartitionBy.DAY);
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
                PartitionBy.NONE,
                TimestampFormatUtils.parseTimestamp("2021-01-01T00:00:00.000000Z")
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
            //noinspection ResultOfMethodCallIgnored
            PartitionBy.getPartitionDirFormatMethod(-1);
            Assert.fail();
        } catch (Exception ignored) {
            TestUtils.assertEquals("UNKNOWN", PartitionBy.toString(-1));
        }
    }

    @Test
    public void testWeekSplit() throws NumericException {
        assertFormatAndParse("2023-W13", "2023-03-27T00:00:00.000000Z", PartitionBy.WEEK);
        assertFormatAndParse("2023-W13-1T175501-123000", "2023-03-27T17:55:01.123000Z", PartitionBy.WEEK);
        assertFormatAndParse("2013-W09-5T17", "2013-03-01T17:00:00.000000Z", PartitionBy.WEEK);
        assertFormatAndParse("2013-W09-5", "2013-03-01T00:00:00.000000Z", PartitionBy.WEEK);
    }

    @Test
    public void testYearSplit() throws NumericException {
        assertFormatAndParse("2013-03-31T175500", "2013-03-31T17:55:00.000000Z", PartitionBy.YEAR);
        assertFormatAndParse("2013-03-31T175501-123000", "2013-03-31T17:55:01.123000Z", PartitionBy.YEAR);
        assertFormatAndParse("2013-03-01T17", "2013-03-01T17:00:00.000000Z", PartitionBy.YEAR);
        assertFormatAndParse("2013", "2013-01-01T00:00:00.000000Z", PartitionBy.YEAR);
    }

    @Test
    public void testYearSplitFuzz() {
        Rnd rnd = TestUtils.generateRandom(null);
        testDaySplitFuzz(PartitionBy.YEAR, 1, rnd);
        testDaySplitFuzz(PartitionBy.YEAR, 1000, rnd);
        testDaySplitFuzz(PartitionBy.YEAR, Timestamps.SECOND_MICROS, rnd);
        testDaySplitFuzz(PartitionBy.YEAR, Timestamps.HOUR_MICROS, rnd);
        testDaySplitFuzz(PartitionBy.YEAR, Timestamps.DAY_MICROS, rnd);
    }

    private static void assertFormatAndParse(CharSequence expectedDirName, CharSequence timestampString, int partitionBy) throws NumericException {
        long expected = TimestampFormatUtils.parseTimestamp(timestampString);
        DateFormat dirFormatMethod = PartitionBy.getPartitionDirFormatMethod(partitionBy);
        sink.clear();
        dirFormatMethod.format(expected, TimestampFormatUtils.EN_LOCALE, null, sink);
        TestUtils.assertEquals(expectedDirName, sink);
        if (partitionBy == PartitionBy.WEEK) {
            int year = Timestamps.getYear(expected);
            int week = Timestamps.getWeek(expected);
            if (week == 52 && Timestamps.getMonthOfYear(expected) == 1) {
                year--;
            }
            sink.clear();
            sink.put(year).put("-W");
            putWithLeadingZeroIfNeeded(sink, sink.length(), week);
            expected = TimestampFormatUtils.parseTimestamp(sink, 0, TimestampFormatUtils.WEEK_PATTERN.length());
        }
        Assert.assertEquals(expected, PartitionBy.parsePartitionDirName(sink, partitionBy));
    }

    private static void assertParseFails(String partitionName, int partitionBy) {
        try {
            PartitionBy.parsePartitionDirName(partitionName, partitionBy);
            Assert.fail("exception expected");
        } catch (Exception ignored) {
        }
    }

    private static void assertParseFailure(CharSequence expected, CharSequence dirName, int partitionBy) {
        try {
            PartitionBy.parsePartitionDirName(dirName, partitionBy);
            Assert.fail();
        } catch (CairoException e) {
            TestUtils.assertEquals(expected, e.getFlyweightMessage());
        }
    }

    private static void checkPartitionPartialParseDay(int day) throws NumericException {
        checkPartitionPartialParseHour(day);
        Assert.assertEquals(IntervalUtils.parseFloorPartialTimestamp("2013-03-31"), PartitionBy.parsePartitionDirName("2013-03-31", day));
    }

    private static void checkPartitionPartialParseHour(int partBy) throws NumericException {
        Assert.assertEquals(IntervalUtils.parseFloorPartialTimestamp("2013-03-31T17:55:01.123021"), PartitionBy.parsePartitionDirName("2013-03-31T175501-123021", partBy));
        Assert.assertEquals(IntervalUtils.parseFloorPartialTimestamp("2013-03-31T17:55:01.12302"), PartitionBy.parsePartitionDirName("2013-03-31T175501-12302", partBy));
        Assert.assertEquals(IntervalUtils.parseFloorPartialTimestamp("2013-03-31T17:55:01.1230"), PartitionBy.parsePartitionDirName("2013-03-31T175501-1230", partBy));
        Assert.assertEquals(IntervalUtils.parseFloorPartialTimestamp("2013-03-31T17:55:01.123"), PartitionBy.parsePartitionDirName("2013-03-31T175501-123", partBy));
        Assert.assertEquals(IntervalUtils.parseFloorPartialTimestamp("2013-03-31T17:55:01.12"), PartitionBy.parsePartitionDirName("2013-03-31T175501-12", partBy));
        Assert.assertEquals(IntervalUtils.parseFloorPartialTimestamp("2013-03-31T17:55:01.1"), PartitionBy.parsePartitionDirName("2013-03-31T175501-1", partBy));
        Assert.assertEquals(IntervalUtils.parseFloorPartialTimestamp("2013-03-31T17:55:01"), PartitionBy.parsePartitionDirName("2013-03-31T175501", partBy));
        Assert.assertEquals(IntervalUtils.parseFloorPartialTimestamp("2013-03-31T17:55"), PartitionBy.parsePartitionDirName("2013-03-31T1755", partBy));
        Assert.assertEquals(IntervalUtils.parseFloorPartialTimestamp("2013-03-31T17"), PartitionBy.parsePartitionDirName("2013-03-31T17", partBy));
    }

    private static void checkPartitionPartialParseMonth(int partitionBy) throws NumericException {
        checkPartitionPartialParseDay(partitionBy);
        Assert.assertEquals(IntervalUtils.parseFloorPartialTimestamp("2013-03"), PartitionBy.parsePartitionDirName("2013-03", partitionBy));
    }

    private static void setSetPath(
            CharSequence expectedDirName,
            CharSequence timestamp,
            int partitionBy
    ) throws NumericException {
        sink.put("a/b/");
        PartitionBy.setSinkForPartition(
                sink,
                partitionBy,
                TimestampFormatUtils.parseTimestamp(timestamp)
        );
        TestUtils.assertEquals(expectedDirName, sink);
    }

    private static void setSetPathNoCalc(
            CharSequence expectedDirName,
            CharSequence timestamp,
            int partitionBy
    ) throws NumericException {
        sink.put("a/b/");
        PartitionBy.setSinkForPartition(
                sink,
                partitionBy,
                TimestampFormatUtils.parseTimestamp(timestamp)
        );
        TestUtils.assertEquals(expectedDirName, sink);
    }

    private static void testAddCeilFloor(
            CharSequence expectedNext,
            int partitionBy,
            CharSequence partitionTimestampStr,
            CharSequence midPartitionTimestampStr
    ) throws NumericException {
        final long expectedNextPartitionTimestamp = TimestampFormatUtils.parseTimestamp(expectedNext);
        final long partitionTimestamp = TimestampFormatUtils.parseTimestamp(partitionTimestampStr);
        final long midPartitionTimestamp = TimestampFormatUtils.parseTimestamp(midPartitionTimestampStr);

        PartitionBy.PartitionAddMethod addMethod = PartitionBy.getPartitionAddMethod(partitionBy);
        Assert.assertNotNull(addMethod);

        PartitionBy.PartitionFloorMethod floorMethod = PartitionBy.getPartitionFloorMethod(partitionBy);
        Assert.assertNotNull(floorMethod);

        PartitionBy.PartitionCeilMethod ceilMethod = PartitionBy.getPartitionCeilMethod(partitionBy);
        Assert.assertNotNull(ceilMethod);

        Assert.assertEquals(expectedNextPartitionTimestamp, addMethod.calculate(partitionTimestamp, 1));
        Assert.assertEquals(partitionTimestamp, floorMethod.floor(midPartitionTimestamp));
        Assert.assertEquals(expectedNextPartitionTimestamp, ceilMethod.ceil(midPartitionTimestamp));
    }

    private static void testPartitionByName(CharSequence expectedPartitionName, int partitionBy) {
        CharSequence partitionName = PartitionBy.toString(partitionBy);
        TestUtils.assertEquals(expectedPartitionName, partitionName);
        Assert.assertEquals(partitionBy, PartitionBy.fromString(partitionName));
        Assert.assertEquals(partitionBy, PartitionBy.fromString(Chars.toString(partitionName).toUpperCase()));
        Assert.assertEquals(partitionBy, PartitionBy.fromString(Chars.toString(partitionName).toLowerCase()));

        Assert.assertEquals(partitionBy, PartitionBy.fromUtf8String(new Utf8String(partitionName)));
        Assert.assertEquals(partitionBy, PartitionBy.fromUtf8String(new Utf8String(Chars.toString(partitionName).toUpperCase())));
        Assert.assertEquals(partitionBy, PartitionBy.fromUtf8String(new Utf8String(Chars.toString(partitionName).toLowerCase())));
    }

    private void testDaySplitFuzz(int partitionBy, long multiplier, Rnd rnd) {
        StringSink tsSink = new StringSink();
        DateFormat formatter = PartitionBy.getPartitionDirFormatMethod(partitionBy);

        for (int i = 0; i < 10; i++) {
            long timestamp = rnd.nextLong(3000 * Timestamps.DAY_MICROS * 365L / multiplier);
            tsSink.clear();
            formatter.format(timestamp, TimestampFormatUtils.EN_LOCALE, null, tsSink);
            long actual = PartitionBy.parsePartitionDirName(tsSink, partitionBy);

            Assert.assertEquals(tsSink.toString(), timestamp, actual);
        }
    }
}
