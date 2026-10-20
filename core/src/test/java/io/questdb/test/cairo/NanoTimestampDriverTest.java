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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cutlass.line.tcp.LineTcpParser;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.CommonUtils;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
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
    public void testImplicitCastOutsideRange() {
        // the parser used to wrap these around the long range, and INSERT stored the wrapped value
        for (String text : new String[]{"2262-12-31T00:00:00Z", "9999-12-31T00:00:00Z", "0001-01-01T00:00:00Z"}) {
            try {
                driver.implicitCast(text, ColumnType.STRING);
                Assert.fail("Expected ImplicitCastException for " + text);
            } catch (ImplicitCastException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value");
            }
            try {
                driver.implicitCastVarchar(new Utf8String(text));
                Assert.fail("Expected ImplicitCastException for " + text);
            } catch (ImplicitCastException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value");
            }
        }
    }

    @Test
    public void testImplicitCastPGDateRange() {
        // the ISO parser rejects these texts, so implicitCast() falls back to the PG date formats,
        // which parse millis and used to wrap around the long range when scaled to nanos
        assertImplicitCastPGDate(epochNanos("2262-04-11T00:00:00Z"), "2262-04-11 UTC");
        assertImplicitCastPGDate(epochNanos("1677-09-22T00:00:00Z"), "1677-09-22 UTC");
        assertImplicitCastPGDate(epochNanos("2262-04-11T23:47:16Z"), "2262-04-11 23:47:16z");
        assertImplicitCastPGDate(epochNanos("1677-09-21T00:12:44Z"), "1677-09-21 00:12:44z");
        assertImplicitCastPGDate(epochNanos("2262-04-11T23:47:16.854Z"), "2262-04-11 23:47:16.854z");
        assertImplicitCastPGDate(epochNanos("1677-09-21T00:12:43.146Z"), "1677-09-21 00:12:43.146z");

        assertImplicitCastPGDateFails("2262-04-12 UTC");
        assertImplicitCastPGDateFails("1677-09-21 UTC");
        assertImplicitCastPGDateFails("2262-04-11 23:47:17z");
        assertImplicitCastPGDateFails("1677-09-21 00:12:43z");
        assertImplicitCastPGDateFails("2262-04-11 23:47:16.855z");
        assertImplicitCastPGDateFails("1677-09-21 00:12:43.145z");
        assertImplicitCastPGDateFails("9999-12-31 UTC");
        assertImplicitCastPGDateFails("9999-12-31 00:00:00.0Z");
        assertImplicitCastPGDateFails("0001-01-01 00:00:00z");
    }

    @Test
    public void testParseFloorRange() throws NumericException {
        // TIMESTAMP_NS holds Long.MIN_VALUE + 1 (1677-09-21T00:12:43.145224193Z) to
        // Long.MAX_VALUE (2262-04-11T23:47:16.854775807Z); Long.MIN_VALUE is NULL
        assertParseFloor(Long.MAX_VALUE, "2262-04-11T23:47:16.854775807Z");
        assertParseFloor(Long.MAX_VALUE - 1, "2262-04-11T23:47:16.854775806Z");
        assertParseFloor(Long.MAX_VALUE, "2262-04-12T00:47:16.854775807+01:00");
        assertParseFloor(Long.MIN_VALUE + 1, "1677-09-21T00:12:43.145224193Z");
        assertParseFloor(Long.MIN_VALUE + 2, "1677-09-21T00:12:43.145224194Z");
        assertParseFloor(Long.MIN_VALUE + 1, "1677-09-20T23:12:43.145224193-01:00");
        assertParseFloor(epochNanos("2262-01-01T00:00:00Z"), "2262");
        assertParseFloor(epochNanos("2262-01-01T00:00:00Z"), "2262-01-01");
        assertParseFloor(epochNanos("2262-04-11T00:00:00Z"), "2262-04-11");
        assertParseFloor(epochNanos("2262-01-01T00:00:00.000000123Z"), "2262-01-01T00:00:00.000000123Z");
        assertParseFloor(epochNanos("1677-09-22T00:00:00Z"), "1677-09-22");
        assertParseFloor(epochNanos("1677-12-31T00:00:00Z"), "1677-12-31");
        assertParseFloor(epochNanos("1677-12-31T23:59:59.999999999Z"), "1677-12-31T23:59:59.999999999Z");
        assertParseFloor(epochNanos("1677-12-31T22:00:00Z"), "1678-01-01T00:00:00+02:00");
        assertParseFloor(epochNanos("2262-01-01T01:00:00Z"), "2261-12-31T23:00:00-02:00");
        // ISO week: 2262-W15 starts on Monday 2262-04-07, 1677-W38 on Monday 1677-09-20
        assertParseFloor(epochNanos("2262-04-07T00:00:00Z"), "2262-W15-1");
        assertParseFloor(Long.MAX_VALUE, "2262-W15-5T23:47:16.854775807");
        assertParseFloor(Long.MIN_VALUE + 1, "1677-W38-2T00:12:43.145224193");
        assertParseFloor(epochNanos("1677-09-27T00:00:00Z"), "1677-W39-1");

        assertParseFloorFails("2262-04-11T23:47:16.854775808Z");
        assertParseFloorFails("2262-04-11T23:47:17Z");
        assertParseFloorFails("2262-04-12");
        assertParseFloorFails("2262-12-31");
        assertParseFloorFails("2262-12-31T23:59:59.999999999Z");
        assertParseFloorFails("2262-04-11T23:47:16.854775807-00:01");
        assertParseFloorFails("2263");
        assertParseFloorFails("2263-01-01T00:00:00Z");
        assertParseFloorFails("9999-12-31T23:59:59.999999999Z");
        // would be Long.MIN_VALUE, the NULL sentinel
        assertParseFloorFails("1677-09-21T00:12:43.145224192Z");
        assertParseFloorFails("1677-09-21T00:12:43.145224191Z");
        assertParseFloorFails("1677-09-21T00:12:43.145224193+00:01");
        assertParseFloorFails("1677-09-21");
        assertParseFloorFails("1677-01-01");
        assertParseFloorFails("1677");
        assertParseFloorFails("1676-12-31T23:59:59.999999999Z");
        assertParseFloorFails("1000-01-01");
        assertParseFloorFails("0001-01-01T00:00:00Z");
        assertParseFloorFails("2262-W15-5T23:47:16.854775808");
        assertParseFloorFails("2262-W16-1");
        assertParseFloorFails("2262-W52");
        assertParseFloorFails("1677-W38-2T00:12:43.145224192");
        assertParseFloorFails("1677-W38-1");
        assertParseFloorFails("1677-W01");
    }

    private static long epochNanos(String isoInstant) {
        final Instant instant = Instant.parse(isoInstant);
        long seconds = instant.getEpochSecond();
        long nanos = instant.getNano();
        if (seconds < 0 && nanos > 0) {
            // keeps seconds * 1e9 inside the long range at the lower end
            seconds++;
            nanos -= 1_000_000_000L;
        }
        return Math.addExact(Math.multiplyExact(seconds, 1_000_000_000L), nanos);
    }

    private void assertImplicitCastPGDate(long expected, String text) {
        assertParseFloorFails(text);
        Assert.assertEquals(text, expected, driver.implicitCast(text, ColumnType.STRING));
        Assert.assertEquals(text, expected, driver.implicitCastVarchar(new Utf8String(text)));
    }

    private void assertImplicitCastPGDateFails(String text) {
        assertParseFloorFails(text);
        try {
            driver.implicitCast(text, ColumnType.STRING);
            Assert.fail("Expected ImplicitCastException for " + text);
        } catch (ImplicitCastException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value");
        }
        try {
            driver.implicitCastVarchar(new Utf8String(text));
            Assert.fail("Expected ImplicitCastException for " + text);
        } catch (ImplicitCastException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value");
        }
    }

    private void assertParseFloor(long expected, String text) throws NumericException {
        Assert.assertEquals(text, expected, driver.parseFloorLiteral(text));
        Assert.assertEquals(text, expected, driver.parseFloorLiteral(new Utf8String(text)));
        Assert.assertEquals(text, expected, driver.parseQuotedLiteral("'" + text + "'"));
    }

    private void assertParseFloorFails(String text) {
        try {
            driver.parseFloorLiteral(text);
            Assert.fail("Expected NumericException for " + text);
        } catch (NumericException expected) {
        }
        try {
            driver.parseFloorLiteral(new Utf8String(text));
            Assert.fail("Expected NumericException for " + text);
        } catch (NumericException expected) {
        }
    }
}
