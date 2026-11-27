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

package io.questdb.test.cairo;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cutlass.line.tcp.LineTcpParser;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.CommonUtils;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.time.temporal.ChronoUnit;

public class NanoTimestampDriverTest extends AbstractCairoTest {
    TimestampDriver driver = ColumnType.getTimestampDriver(ColumnType.TIMESTAMP_NANO);

    @Test
    public void testFromChronosUnit() {
        Assert.assertEquals(1_000, driver.from(1000, ChronoUnit.NANOS));
        Assert.assertEquals(1_000, driver.from(1, ChronoUnit.MICROS));
        Assert.assertEquals(1000_000, driver.from(1, ChronoUnit.MILLIS));
        Assert.assertEquals(1_000_000_000, driver.from(1, ChronoUnit.SECONDS));

        Assert.assertEquals(60 * 1000 * 1000 * 1000L, driver.from(1, ChronoUnit.MINUTES));
        Assert.assertEquals(Long.MAX_VALUE, driver.from(Long.MAX_VALUE, ChronoUnit.NANOS));

        Assert.assertEquals(driver.from(1, ChronoUnit.HOURS), driver.from(60, ChronoUnit.MINUTES));
        Assert.assertEquals(0, driver.from(0, ChronoUnit.NANOS));
        Assert.assertEquals(0, driver.from(0, ChronoUnit.MICROS));
        Assert.assertEquals(0, driver.from(0, ChronoUnit.MILLIS));
        Assert.assertEquals(0, driver.from(0, ChronoUnit.SECONDS));
        Assert.assertEquals(0, driver.from(0, ChronoUnit.MINUTES));

        // nanos values remain unchanged
        Assert.assertEquals(123456789L, driver.from(123456789L, ChronoUnit.NANOS));
        Assert.assertEquals(123456789001L, driver.from(123456789001L, ChronoUnit.NANOS));
        try {
            driver.from(Long.MAX_VALUE, ChronoUnit.SECONDS);
            Assert.fail();
        } catch (ArithmeticException e) {
            Assert.assertTrue(e.getMessage().contains("long overflow"));
        }
    }

    @Test
    public void testFromDate() {
        Assert.assertEquals(Numbers.LONG_NULL, driver.fromDate(Numbers.LONG_NULL));
        Assert.assertEquals(1_000_000L, driver.fromDate(1));
        Assert.assertEquals(9_223_372_036_854_000_000L, driver.fromDate(9_223_372_036_854L));
        Assert.assertEquals(-9_223_372_036_854_000_000L, driver.fromDate(-9_223_372_036_854L));
        try {
            driver.fromDate(9_223_372_036_855L);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: 9223372036855 [DATE -> TIMESTAMP_NS]", e.getFlyweightMessage());
        }
        try {
            driver.fromDate(-9_223_372_036_855L);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: -9223372036855 [DATE -> TIMESTAMP_NS]", e.getFlyweightMessage());
        }
        try {
            driver.fromDate(18_446_744_073_709_552L);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: 18446744073709552 [DATE -> TIMESTAMP_NS]", e.getFlyweightMessage());
        }
        try {
            driver.fromDate(Long.MIN_VALUE + 1);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: -9223372036854775807 [DATE -> TIMESTAMP_NS]", e.getFlyweightMessage());
        }
    }

    @Test
    public void testFromTimestampUnit() {
        Assert.assertEquals(Numbers.LONG_NULL, driver.from(LineTcpParser.NULL_TIMESTAMP, CommonUtils.TIMESTAMP_UNIT_NANOS));
        Assert.assertEquals(56799L, driver.from(56799, CommonUtils.TIMESTAMP_UNIT_NANOS));
        Assert.assertEquals(56799_000L, driver.from(56799, CommonUtils.TIMESTAMP_UNIT_MICROS));
        Assert.assertEquals(56799_000_000L, driver.from(56799, CommonUtils.TIMESTAMP_UNIT_MILLIS));
        Assert.assertEquals(60_000_000_000L, driver.from(60, CommonUtils.TIMESTAMP_UNIT_SECONDS));
        Assert.assertEquals(3600_000_000_000L, driver.from(60, CommonUtils.TIMESTAMP_UNIT_MINUTES));
        Assert.assertEquals(86400_000_000_000L, driver.from(24, CommonUtils.TIMESTAMP_UNIT_HOURS));
        try {
            driver.from(123456, (byte) 100);
            Assert.fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException expected) {
        }
        try {
            driver.from(123456789000L, CommonUtils.TIMESTAMP_UNIT_MINUTES);
            Assert.fail("Expected ArithmeticException");
        } catch (ArithmeticException e) {
            TestUtils.assertContains(e.getMessage(), "long overflow");
        }
    }

    @Test
    public void testGetTimestampUnitConverter() {
        final CommonUtils.TimestampUnitConverter converter = driver.getTimestampUnitConverter(ColumnType.TIMESTAMP_MICRO);
        Assert.assertEquals(Numbers.LONG_NULL, converter.convert(Numbers.LONG_NULL));
        Assert.assertEquals(0, converter.convert(0));
        Assert.assertEquals(1_000, converter.convert(1));
        Assert.assertEquals(9_214_646_399_999_999_000L, converter.convert(9_214_646_399_999_999L));
        for (long micros : new long[]{-1, Long.MIN_VALUE + 1, 9_214_646_400_000_000L, Long.MAX_VALUE}) {
            final CairoException exception = Assert.assertThrows(CairoException.class,
                    () -> converter.convert(micros));
            TestUtils.assertContains(exception.getFlyweightMessage(),
                    "designated timestamp_ns before 1970-01-01 and beyond 2261-12-31 23:59:59.999999999 is not allowed");
        }
    }

    @Test
    public void testImplicitCast() {
        Assert.assertEquals(Numbers.LONG_NULL, driver.implicitCast(null));
        long expected2020_01_01 = 1577836800000000000L; // 2020-01-01T00:00:00Z in nanos
        Assert.assertEquals(expected2020_01_01, driver.implicitCast("2020-01-01T00:00:00Z"));
        Assert.assertEquals(expected2020_01_01, driver.implicitCast("2020-01-01T00:00:00Z", ColumnType.STRING));
        Assert.assertEquals(expected2020_01_01, driver.implicitCast("2020-01-01T00:00:00.000Z", ColumnType.STRING));
        Assert.assertEquals(1577836800123456000L, driver.implicitCast("2020-01-01T00:00:00.123456Z", ColumnType.STRING));
        Assert.assertEquals(1577836800123456789L, driver.implicitCast("2020-01-01T00:00:00.123456789Z", ColumnType.STRING));
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

        // Test partial numbers that are not valid timestamps
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

        // Test null values
        Assert.assertEquals(Numbers.LONG_NULL, driver.implicitCast(null, ColumnType.STRING));
        Assert.assertEquals(Numbers.LONG_NULL, driver.implicitCast(null, ColumnType.SYMBOL));

        try {
            driver.implicitCast("", ColumnType.STRING);
            Assert.fail("Expected ImplicitCastException");
        } catch (ImplicitCastException expected) {
            // Expected
        }

        try {
            driver.implicitCast("   ", ColumnType.STRING);
            Assert.fail("Expected ImplicitCastException");
        } catch (ImplicitCastException expected) {
            // Expected
        }

        Assert.assertEquals(Numbers.LONG_NULL, driver.implicitCast(null, ColumnType.STRING));
        Assert.assertEquals(Numbers.LONG_NULL, driver.implicitCast(null, ColumnType.SYMBOL));

        try {
            driver.implicitCast("", ColumnType.STRING);
            Assert.fail("Expected ImplicitCastException");
        } catch (ImplicitCastException expected) {
            // Expected
        }

        // Test numeric timestamp values as strings
        Assert.assertEquals(1234567890L, driver.implicitCast("1234567890", ColumnType.STRING));
        Assert.assertEquals(1234567890L, driver.implicitCast("1234567890", ColumnType.SYMBOL));
        Assert.assertEquals(0L, driver.implicitCast("0", ColumnType.STRING));
        Assert.assertEquals(-1234567890L, driver.implicitCast("-1234567890", ColumnType.STRING));

        Assert.assertEquals(1703462400000000000L, driver.implicitCast("2023-12-25 z", ColumnType.STRING));
        Assert.assertEquals(1703462400000000000L, driver.implicitCast("2023-12-25 00:00:00.0z", ColumnType.STRING));
        Assert.assertEquals(1703462400000000000L, driver.implicitCast("2023-12-25 00:00:00z", ColumnType.STRING));
        Assert.assertEquals(1577836800100000000L, driver.implicitCast("2020-01-01T00:00:00.1Z", ColumnType.STRING));
        Assert.assertEquals(1577836800120000000L, driver.implicitCast("2020-01-01T00:00:00.12Z", ColumnType.STRING));
        Assert.assertEquals(1577836800123000000L, driver.implicitCast("2020-01-01T00:00:00.123Z", ColumnType.STRING));
    }

    @Test
    public void testParseFloorNanosecondRange() throws Exception {
        assertParseFloor(Long.MIN_VALUE + 1, "1677-09-21T00:12:43.145224193Z");
        assertParseFloor(Long.MIN_VALUE + 2, "1677-09-21T00:12:43.145224194Z");
        assertParseFloor(Long.MIN_VALUE + 808, "1677-09-21T00:12:43.145225000Z");
        assertParseFloor(Long.MIN_VALUE + 1, "1677-09-21T01:12:43.145224193+01:00");
        assertParseFloor(Long.MIN_VALUE + 1, "1677-W38-2T00:12:43.145224193");
        assertParseFloor(Long.MAX_VALUE - 1, "2262-04-11T23:47:16.854775806Z");
        assertParseFloor(Long.MAX_VALUE, "2262-04-11T23:47:16.854775807Z");
        assertParseFloor(Long.MAX_VALUE, "2262-04-12T00:47:16.854775807+01:00");
        assertParseFloor(Long.MAX_VALUE, "2262-W15-5T23:47:16.854775807");
        assertParseFloor(-1, "1969-12-31T23:59:59.999999999Z");
        assertParseFloor(0, "1970-01-01T00:00:00.000000000Z");
        assertParseFloor(1, "1970-01-01T00:00:00.000000001Z");
        Assert.assertEquals(Numbers.LONG_NULL, driver.parseFloorLiteral((CharSequence) null));
        Assert.assertEquals(Numbers.LONG_NULL, driver.parseFloorLiteral((Utf8Sequence) null));
    }

    @Test
    public void testParseFloorRejectsNanosecondOverflow() {
        for (String text : new String[]{
                "1500-01-01", "1677", "1677-09-21T00:12:43.145224191Z",
                "1677-09-21T00:12:43.145224192Z", "1677-09-21T00:12:43.145224Z",
                "1677-09-21T00:12:43.145224193+00:01", "1677-W38-2T00:12:43.145224192",
                "2262-04-11T23:47:16.854775808Z", "2262-04-11T23:47:16.854775807-00:01",
                "2262-W15-5T23:47:16.854775808", "2262-12", "2263-01-01"
        }) {
            Assert.assertThrows(text, NumericException.class, () -> driver.parseFloorLiteral(text));
            final Utf8String utf8 = new Utf8String(text);
            Assert.assertThrows(text, NumericException.class, () -> driver.parseFloorLiteral(utf8));
        }
    }

    private void assertParseFloor(long expected, String text) throws NumericException {
        Assert.assertEquals(text, expected, driver.parseFloorLiteral(text));
        Assert.assertEquals(text, expected, driver.parseFloorLiteral(new Utf8String(text)));
    }
}
