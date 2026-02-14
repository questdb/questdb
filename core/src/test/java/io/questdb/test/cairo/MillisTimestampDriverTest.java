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

package io.questdb.test.cairo;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.MillisTimestampDriver;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cutlass.line.tcp.LineTcpParser;
import io.questdb.griffin.SqlException;
import io.questdb.std.Interval;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.CommonUtils;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static io.questdb.std.datetime.TimeZoneRuleFactory.RESOLUTION_MILLIS;

public class MillisTimestampDriverTest extends AbstractCairoTest {
    private static final TimestampDriver driver = MillisTimestampDriver.INSTANCE;

    @Test
    public void testAdd() {
        long baseTimestamp = 1577836800000L;

        Assert.assertEquals(baseTimestamp + 1, driver.add(baseTimestamp, 'T', 1));
        Assert.assertEquals(baseTimestamp + 1000, driver.add(baseTimestamp, 's', 1));
        Assert.assertEquals(baseTimestamp + 60000, driver.add(baseTimestamp, 'm', 1));
        Assert.assertEquals(baseTimestamp + 3600000, driver.add(baseTimestamp, 'h', 1));
        Assert.assertEquals(baseTimestamp + 3600000, driver.add(baseTimestamp, 'H', 1));
        Assert.assertEquals(baseTimestamp + 86400000, driver.add(baseTimestamp, 'd', 1));
        Assert.assertEquals(baseTimestamp + 604800000, driver.add(baseTimestamp, 'w', 1));

        Assert.assertEquals(baseTimestamp, driver.add(baseTimestamp, 'u', 1));
        Assert.assertEquals(baseTimestamp + 1, driver.add(baseTimestamp, 'u', 1000));
        Assert.assertEquals(baseTimestamp, driver.add(baseTimestamp, 'n', 1));
        Assert.assertEquals(baseTimestamp + 1, driver.add(baseTimestamp, 'n', 1000000));
        try {
            driver.add(baseTimestamp, 'x', 1);
            Assert.fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException expected) {
        }
    }

    @Test
    public void testAdditionalGetMethods() {
        long timestamp = 1640995200123L;

        Assert.assertEquals(0, driver.getMicrosOfMilli(timestamp));
        Assert.assertEquals(123000, driver.getMicrosOfSecond(timestamp));
        Assert.assertEquals(0, driver.getNanosOfMicros(timestamp));
        Assert.assertEquals(123000000, driver.getNanosOfSecond(timestamp));
        Assert.assertEquals(31, driver.getDaysPerMonth(timestamp));

        Assert.assertEquals(Numbers.INT_NULL, driver.getMicrosOfMilli(Numbers.LONG_NULL));
        Assert.assertEquals(Numbers.INT_NULL, driver.getMicrosOfSecond(Numbers.LONG_NULL));
        Assert.assertEquals(Numbers.INT_NULL, driver.getNanosOfMicros(Numbers.LONG_NULL));
        Assert.assertEquals(Numbers.INT_NULL, driver.getNanosOfSecond(Numbers.LONG_NULL));
        Assert.assertEquals(Numbers.INT_NULL, driver.getDaysPerMonth(Numbers.LONG_NULL));
    }

    @Test
    public void testAppend() {
        CharSink<?> sink = new StringSink();
        long timestamp = 1577836800000L;
        driver.append(sink, timestamp);
        String result = sink.toString();
        Assert.assertNotNull(result);
        Assert.assertTrue(result.contains("2020"));
        StringSink newSink = new StringSink();
        driver.append(newSink, Numbers.LONG_NULL);
    }

    @Test
    public void testAppendToPGWireText() {
        StringSink sink = new StringSink();
        long timestamp = 1577836800000L;

        driver.appendToPGWireText(sink, timestamp);
        String result = sink.toString();
        Assert.assertNotNull(result);
        Assert.assertTrue(result.contains("2020"));
    }

    @Test
    public void testConstantValues() {
        Assert.assertNotNull(driver.getTimestampConstantNull());
        Assert.assertNotNull(driver.getTimestampDateFormatFactory());
    }

    @Test
    public void testConversions() {
        long millis = 1577836800000L;

        Assert.assertEquals(millis, driver.fromMillis(millis));
        Assert.assertEquals(millis, driver.fromSeconds(millis / 1000));

        int minutes = (int) (millis / 60000);
        Assert.assertEquals(minutes * 60000L, driver.fromMinutes(minutes));

        int hours = (int) (millis / 3600000);
        Assert.assertEquals(hours * 3600000L, driver.fromHours(hours));

        int days = (int) (millis / 86400000);
        Assert.assertEquals(days * 86400000L, driver.fromDays(days));

        int weeks = (int) (millis / 604800000);
        Assert.assertEquals(weeks * 604800000L, driver.fromWeeks(weeks));

        Assert.assertEquals(millis, driver.toDate(millis));
        Assert.assertEquals(millis / 1000, driver.toSeconds(millis));
        Assert.assertEquals(millis / 3600000, driver.toHours(millis));

        Assert.assertEquals(Numbers.LONG_NULL, driver.toSeconds(Numbers.LONG_NULL));
        Assert.assertEquals(Numbers.LONG_NULL, driver.toHours(Numbers.LONG_NULL));
    }

    @Test
    public void testDateArithmetic() {
        long baseTimestamp = 1577836800000L;

        Assert.assertEquals(baseTimestamp + 86400000, driver.addDays(baseTimestamp, 1));
        Assert.assertEquals(baseTimestamp - 86400000, driver.addDays(baseTimestamp, -1));

        Assert.assertEquals(baseTimestamp + 604800000, driver.addWeeks(baseTimestamp, 1));

        long oneMonthLater = driver.addMonths(baseTimestamp, 1);
        Assert.assertTrue(oneMonthLater > baseTimestamp);

        long oneYearLater = driver.addYears(baseTimestamp, 1);
        Assert.assertTrue(oneYearLater > baseTimestamp);
    }

    @Test
    public void testDateOfWeek() {
        long timestamp = 1640995200000L;

        int dayOfWeek = driver.getDayOfWeek(timestamp);
        Assert.assertTrue(dayOfWeek >= 1 && dayOfWeek <= 7);

        int dayOfWeekSundayFirst = driver.getDayOfWeekSundayFirst(timestamp);
        Assert.assertTrue(dayOfWeekSundayFirst >= 1 && dayOfWeekSundayFirst <= 7);

        Assert.assertEquals(Numbers.INT_NULL, driver.getDayOfWeek(Numbers.LONG_NULL));
        Assert.assertEquals(Numbers.INT_NULL, driver.getDayOfWeekSundayFirst(Numbers.LONG_NULL));
    }

    @Test
    public void testEndOfDay() {
        long timestamp = 1577836800000L;
        long endOfDay = driver.endOfDay(timestamp);
        Assert.assertEquals(timestamp + 86400000L - 1, endOfDay);
    }

    @Test
    public void testExtendedDateFields() {
        long timestamp = 1577836800000L;

        Assert.assertEquals(21, driver.getCentury(timestamp));
        Assert.assertEquals(202, driver.getDecade(timestamp));
        Assert.assertEquals(3, driver.getDow(timestamp));
        Assert.assertEquals(1, driver.getDoy(timestamp));
        Assert.assertEquals(2020, driver.getIsoYear(timestamp));
        Assert.assertEquals(3, driver.getMillennium(timestamp));

        Assert.assertEquals(Numbers.INT_NULL, driver.getCentury(Numbers.LONG_NULL));
        Assert.assertEquals(Numbers.INT_NULL, driver.getDecade(Numbers.LONG_NULL));
        Assert.assertEquals(Numbers.INT_NULL, driver.getDow(Numbers.LONG_NULL));
        Assert.assertEquals(Numbers.INT_NULL, driver.getDoy(Numbers.LONG_NULL));
        Assert.assertEquals(Numbers.INT_NULL, driver.getIsoYear(Numbers.LONG_NULL));
        Assert.assertEquals(Numbers.INT_NULL, driver.getMillennium(Numbers.LONG_NULL));
    }

    @Test
    public void testFixInterval() {
        Interval interval = new Interval();

        interval.of(1000000L, 2000000L);
        Interval result = driver.fixInterval(interval, ColumnType.INTERVAL_TIMESTAMP_NANO);
        Assert.assertEquals(1L, result.getLo());
        Assert.assertEquals(2L, result.getHi());

        interval.of(2000L, 4000L);
        result = driver.fixInterval(interval, ColumnType.INTERVAL_TIMESTAMP_MICRO);
        Assert.assertEquals(2L, result.getLo());
        Assert.assertEquals(4L, result.getHi());

        interval.of(1000L, 2000L);
        result = driver.fixInterval(interval, ColumnType.DATE);
        Assert.assertEquals(1000L, result.getLo());
        Assert.assertEquals(2000L, result.getHi());
    }

    @Test
    public void testFloorCeil() {
        long timestamp = 1577836800000L;

        long floorYear = driver.floorYYYY(timestamp);
        long ceilYear = driver.ceilYYYY(timestamp);

        Assert.assertTrue(floorYear <= timestamp);
        Assert.assertTrue(ceilYear >= timestamp);
    }

    @Test
    public void testFromChronosUnit() {
        Assert.assertEquals(1, driver.from(1000000, ChronoUnit.NANOS));
        Assert.assertEquals(1, driver.from(1000, ChronoUnit.MICROS));
        Assert.assertEquals(1, driver.from(1, ChronoUnit.MILLIS));
        Assert.assertEquals(1000, driver.from(1, ChronoUnit.SECONDS));

        Assert.assertEquals(60 * 1000, driver.from(1, ChronoUnit.MINUTES));
        Assert.assertEquals(60 * 60 * 1000, driver.from(1, ChronoUnit.HOURS));

        Assert.assertEquals(0, driver.from(0, ChronoUnit.NANOS));
        Assert.assertEquals(0, driver.from(0, ChronoUnit.MICROS));
        Assert.assertEquals(0, driver.from(0, ChronoUnit.MILLIS));
        Assert.assertEquals(0, driver.from(0, ChronoUnit.SECONDS));
        Assert.assertEquals(0, driver.from(0, ChronoUnit.MINUTES));

        Assert.assertEquals(123456789L, driver.from(123456789L, ChronoUnit.MILLIS));
        Assert.assertEquals(123456789L, driver.from(123456789000000L, ChronoUnit.NANOS));
    }

    @Test
    public void testFromInstant() {
        Instant instant = Instant.ofEpochSecond(1577836800L, 123456789L);
        long result = driver.from(instant);
        long expected = 1577836800000L + 123L;
        Assert.assertEquals(expected, result);

        instant = Instant.ofEpochSecond(1577836800L, 0);
        result = driver.from(instant);
        Assert.assertEquals(1577836800000L, result);
    }

    @Test
    public void testFromTimestampColumnType() {
        long nanoTimestamp = 1577836800123456789L;
        long microTimestamp = 1577836800123456L;
        long milliTimestamp = 1577836800123L;

        Assert.assertEquals(1577836800123L,
                driver.from(nanoTimestamp, ColumnType.TIMESTAMP_NANO));

        Assert.assertEquals(1577836800123L,
                driver.from(microTimestamp, ColumnType.TIMESTAMP_MICRO));

        Assert.assertEquals(milliTimestamp,
                driver.from(milliTimestamp, ColumnType.DATE));
    }

    @Test
    public void testFromTimestampUnit() {
        Assert.assertEquals(Numbers.LONG_NULL, driver.from(LineTcpParser.NULL_TIMESTAMP, CommonUtils.TIMESTAMP_UNIT_MILLIS));
        Assert.assertEquals(56L, driver.from(56799001, CommonUtils.TIMESTAMP_UNIT_NANOS));
        Assert.assertEquals(56L, driver.from(56799, CommonUtils.TIMESTAMP_UNIT_MICROS));
        Assert.assertEquals(56799L, driver.from(56799, CommonUtils.TIMESTAMP_UNIT_MILLIS));
        Assert.assertEquals(60_000L, driver.from(60, CommonUtils.TIMESTAMP_UNIT_SECONDS));
        Assert.assertEquals(3600_000L, driver.from(60, CommonUtils.TIMESTAMP_UNIT_MINUTES));
        Assert.assertEquals(86400_000L, driver.from(24, CommonUtils.TIMESTAMP_UNIT_HOURS));
        try {
            driver.from(123456, (byte) 100);
            Assert.fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException expected) {
        }
        try {
            driver.from(123456789000000L, CommonUtils.TIMESTAMP_UNIT_HOURS);
            Assert.fail("Expected ArithmeticException");
        } catch (ArithmeticException e) {
            TestUtils.assertContains(e.getMessage(), "long overflow");
        }
    }

    @Test
    public void testGetAddMethod() {
        Assert.assertNotNull(driver.getAddMethod('s'));
        Assert.assertNotNull(driver.getAddMethod('m'));
        Assert.assertNotNull(driver.getAddMethod('h'));
        Assert.assertNotNull(driver.getAddMethod('H'));
        Assert.assertNotNull(driver.getAddMethod('d'));
        Assert.assertNotNull(driver.getAddMethod('w'));
        Assert.assertNotNull(driver.getAddMethod('M'));
        Assert.assertNotNull(driver.getAddMethod('y'));
        Assert.assertNotNull(driver.getAddMethod('T'));
        Assert.assertNotNull(driver.getAddMethod('u'));
        Assert.assertNotNull(driver.getAddMethod('U'));
        Assert.assertNotNull(driver.getAddMethod('n'));

        Assert.assertNull(driver.getAddMethod('x'));
    }

    @Test
    public void testGetPartitionMethods() {
        Assert.assertNotNull(driver.getPartitionAddMethod(PartitionBy.DAY));
        Assert.assertNotNull(driver.getPartitionAddMethod(PartitionBy.MONTH));
        Assert.assertNotNull(driver.getPartitionAddMethod(PartitionBy.YEAR));
        Assert.assertNotNull(driver.getPartitionAddMethod(PartitionBy.HOUR));
        Assert.assertNotNull(driver.getPartitionAddMethod(PartitionBy.WEEK));
        Assert.assertNull(driver.getPartitionAddMethod(99));

        Assert.assertNotNull(driver.getPartitionCeilMethod(PartitionBy.DAY));
        Assert.assertNotNull(driver.getPartitionCeilMethod(PartitionBy.MONTH));
        Assert.assertNotNull(driver.getPartitionCeilMethod(PartitionBy.YEAR));
        Assert.assertNotNull(driver.getPartitionCeilMethod(PartitionBy.HOUR));
        Assert.assertNotNull(driver.getPartitionCeilMethod(PartitionBy.WEEK));
        Assert.assertNull(driver.getPartitionCeilMethod(99));
    }

    @Test
    public void testGetTZRuleResolution() {
        Assert.assertEquals(RESOLUTION_MILLIS, driver.getTZRuleResolution());
    }

    @Test
    public void testImplicitCast() {
        Assert.assertEquals(Numbers.LONG_NULL, driver.implicitCast(null));

        long expected2020_01_01 = 1577836800000L;
        Assert.assertEquals(expected2020_01_01, driver.implicitCast("2020-01-01T00:00:00Z"));
        Assert.assertEquals(expected2020_01_01, driver.implicitCast("2020-01-01T00:00:00Z", ColumnType.STRING));
        Assert.assertEquals(expected2020_01_01, driver.implicitCast("2020-01-01T00:00:00.000Z", ColumnType.STRING));
        Assert.assertEquals(1577836800123L, driver.implicitCast("2020-01-01T00:00:00.123Z", ColumnType.STRING));
        Assert.assertEquals(expected2020_01_01, driver.implicitCast("2020-01-01", ColumnType.STRING));

        try {
            driver.implicitCast("invalid-date", ColumnType.STRING);
            Assert.fail("Expected ImplicitCastException");
        } catch (ImplicitCastException expected) {
        }

        try {
            driver.implicitCast("not-a-timestamp", ColumnType.STRING);
            Assert.fail("Expected ImplicitCastException");
        } catch (ImplicitCastException expected) {
        }

        try {
            driver.implicitCast("abc123def", ColumnType.STRING);
            Assert.fail("Expected ImplicitCastException");
        } catch (ImplicitCastException expected) {
        }

        try {
            driver.implicitCast("123abc", ColumnType.STRING);
            Assert.fail("Expected ImplicitCastException");
        } catch (ImplicitCastException expected) {
        }

        try {
            driver.implicitCast("12.34.56", ColumnType.STRING);
            Assert.fail("Expected ImplicitCastException");
        } catch (ImplicitCastException expected) {
        }

        Assert.assertEquals(Numbers.LONG_NULL, driver.implicitCast(null, ColumnType.STRING));
        Assert.assertEquals(Numbers.LONG_NULL, driver.implicitCast(null, ColumnType.SYMBOL));

        try {
            driver.implicitCast("", ColumnType.STRING);
            Assert.fail("Expected ImplicitCastException");
        } catch (ImplicitCastException expected) {
        }

        try {
            driver.implicitCast("   ", ColumnType.STRING);
            Assert.fail("Expected ImplicitCastException");
        } catch (ImplicitCastException expected) {
        }

        Assert.assertEquals(1234567890L, driver.implicitCast("1234567890", ColumnType.STRING));
        Assert.assertEquals(1234567890L, driver.implicitCast("1234567890", ColumnType.SYMBOL));
        Assert.assertEquals(0L, driver.implicitCast("0", ColumnType.STRING));
        Assert.assertEquals(-1234567890L, driver.implicitCast("-1234567890", ColumnType.STRING));

        long expected20231225 = driver.implicitCast("2023-12-25", ColumnType.STRING);
        Assert.assertEquals(expected20231225, driver.implicitCast("2023-12-25 z", ColumnType.STRING));
        Assert.assertEquals(expected20231225, driver.implicitCast("2023-12-25 00:00:00.0z", ColumnType.STRING));
        Assert.assertEquals(expected20231225, driver.implicitCast("2023-12-25 00:00:00z", ColumnType.STRING));
        Assert.assertEquals(1577836800100L, driver.implicitCast("2020-01-01T00:00:00.1Z", ColumnType.STRING));
        Assert.assertEquals(1577836800120L, driver.implicitCast("2020-01-01T00:00:00.12Z", ColumnType.STRING));
        Assert.assertEquals(1577836800123L, driver.implicitCast("2020-01-01T00:00:00.123Z", ColumnType.STRING));
    }

    @Test
    public void testMonthsBetween() {
        long jan2020 = driver.implicitCast("2020-01-01", ColumnType.STRING);
        long feb2020 = driver.implicitCast("2020-02-01", ColumnType.STRING);
        long jan2021 = driver.implicitCast("2021-01-01", ColumnType.STRING);

        Assert.assertEquals(1L, driver.monthsBetween(feb2020, jan2020));
        Assert.assertEquals(12L, driver.monthsBetween(jan2021, jan2020));
        Assert.assertEquals(1L, driver.monthsBetween(jan2020, feb2020));
    }

    @Test
    public void testParseAnyFormat() {
        try {
            Assert.assertEquals(1577836800000L,
                    driver.parseAnyFormat("2020-01-01", 0, 10));
        } catch (NumericException e) {
            Assert.fail("Unexpected exception: " + e.getMessage());
        }

        try {
            driver.parseAnyFormat("invalid", 0, 7);
            Assert.fail("Expected NumericException");
        } catch (NumericException expected) {
        }
    }

    @Test
    public void testParseInterval() {
        LongList out = new LongList();

        try {
            driver.parseInterval("2020-01-01T10:30:45.123", 0, 23, (short) 1, out);
            Assert.assertTrue(out.size() > 0);

            out.clear();
            driver.parseInterval("2020-01-01", 0, 10, (short) 1, out);
            Assert.assertTrue(out.size() > 0);

            out.clear();
            driver.parseInterval("2020-01", 0, 7, (short) 1, out);
            Assert.assertTrue(out.size() > 0);

            out.clear();
            driver.parseInterval("2020", 0, 4, (short) 1, out);
            Assert.assertTrue(out.size() > 0);
        } catch (NumericException e) {
            Assert.fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    public void testStartOfDay() {
        long timestamp = 1577836800123L;
        long startOfDay = driver.startOfDay(timestamp, 0);
        Assert.assertTrue(startOfDay <= timestamp);

        long nextDayStart = driver.startOfDay(timestamp, 1);
        Assert.assertEquals(86400000L, nextDayStart - startOfDay);
    }

    @Test
    public void testStaticFloorMethod() {
        long expected2020_01_01 = 1577836800000L;
        Assert.assertEquals(expected2020_01_01, MillisTimestampDriver.floor("2020-01-01"));
        Assert.assertEquals(expected2020_01_01, MillisTimestampDriver.floor("2020-01-01T00:00:00Z"));
    }

    @Test
    public void testStringConversions() {
        long timestamp = 1577836800123L;

        String mSecString = driver.toMSecString(timestamp);
        Assert.assertNotNull(mSecString);
        Assert.assertTrue(mSecString.contains("2020"));

        String uSecString = driver.toUSecString(timestamp);
        Assert.assertNotNull(uSecString);
        Assert.assertTrue(uSecString.contains("2020"));
    }

    @Test
    public void testTimeExtraction() {
        long timestamp = 1640995200123L;

        Assert.assertEquals(2022, driver.getYear(timestamp));
        Assert.assertEquals(1, driver.getMonthOfYear(timestamp));
        Assert.assertEquals(1, driver.getDayOfMonth(timestamp));
        Assert.assertEquals(0, driver.getHourOfDay(timestamp));
        Assert.assertEquals(0, driver.getMinuteOfHour(timestamp));
        Assert.assertEquals(0, driver.getSecondOfMinute(timestamp));
        Assert.assertEquals(123, driver.getMillisOfSecond(timestamp));

        Assert.assertEquals(Numbers.INT_NULL, driver.getYear(Numbers.LONG_NULL));
        Assert.assertEquals(Numbers.INT_NULL, driver.getMonthOfYear(Numbers.LONG_NULL));
        Assert.assertEquals(Numbers.INT_NULL, driver.getDayOfMonth(Numbers.LONG_NULL));
    }

    @Test
    public void testTimestampType() {
        Assert.assertEquals(ColumnType.DATE, driver.getTimestampType());
    }

    @Test
    public void testToMicrosAndNanos() {
        long timestamp = 1577836800123L;

        Assert.assertEquals(timestamp * 1000L, driver.toMicros(timestamp));
        Assert.assertEquals(timestamp * 1000000L, driver.toNanos(timestamp));

        Assert.assertEquals(Numbers.LONG_NULL, driver.toMicros(Numbers.LONG_NULL));
        Assert.assertEquals(Numbers.LONG_NULL, driver.toNanos(Numbers.LONG_NULL));
    }

    @Test
    public void testToNanosScale() {
        Assert.assertEquals(1000000L, driver.toNanosScale());
    }

    @Test
    public void testUnsupportedOperations() {
        try {
            driver.approxPartitionDuration(1);
            Assert.fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException expected) {
        }

        try {
            driver.getGKKHourInt();
            Assert.fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException expected) {
        }

        try {
            driver.getIntervalConstantNull();
            Assert.fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException expected) {
        }

        try {
            driver.getPartitionDirFormatMethod(1);
            Assert.fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException expected) {
        }

        try {
            driver.getPartitionFloorMethod(1);
            Assert.fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException expected) {
        }

        try {
            driver.getPeriodBetween('d', 0, 1000, ColumnType.DATE, ColumnType.DATE);
            Assert.fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException expected) {
        }

        try {
            driver.getQuarter(1577836800000L);
            Assert.fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException expected) {
        }

        try {
            driver.getTimestampCeilMethod('d');
            Assert.fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException expected) {
        }

        try {
            driver.getTimestampDiffMethod('d');
            Assert.fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException expected) {
        }

        try {
            driver.getTimestampFloorMethod("day");
            Assert.fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException expected) {
        }

        try {
            driver.getTimestampFloorWithOffsetMethod('d');
            Assert.fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException expected) {
        }

        try {
            driver.getTimestampFloorWithStrideMethod("day");
            Assert.fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException expected) {
        }

        try {
            driver.getTimestampSampler(1000, 'd', 0);
            Assert.fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException expected) {
        } catch (SqlException e) {
            Assert.fail("Expected UnsupportedOperationException, got SqlException");
        }

        try {
            driver.getTimestampUnitConverter(ColumnType.TIMESTAMP_NANO);
            Assert.fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException expected) {
        }

        try {
            DateLocale locale = configuration.getDefaultDateLocale();
            driver.getTimezoneRules(locale, "UTC");
            Assert.fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException expected) {
        }

        try {
            driver.getWeek(1577836800000L);
            Assert.fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException expected) {
        }

        try {
            driver.inInterval(1577836800000L, ColumnType.INTERVAL_TIMESTAMP_NANO, null);
            Assert.fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException expected) {
        }

        try {
            driver.parsePartitionDirName("2020-01-01", 1, 0, 10);
            Assert.fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException expected) {
        }

        try {
            driver.validateBounds(1577836800000L);
            Assert.fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException expected) {
        }
    }
}