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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.TimestampDriver;
import io.questdb.std.Numbers;
import io.questdb.std.datetime.CommonUtils;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.time.temporal.ChronoUnit;

public class MicroTimestampDriverTest extends AbstractCairoTest {
    TimestampDriver driver = ColumnType.getTimestampDriver(ColumnType.TIMESTAMP_MICRO);

    @Test
    public void testFromChronosUnit() {
        Assert.assertEquals(1, driver.from(1000, ChronoUnit.NANOS));
        Assert.assertEquals(1, driver.from(1, ChronoUnit.MICROS));
        Assert.assertEquals(1000, driver.from(1, ChronoUnit.MILLIS));
        Assert.assertEquals(1_000_000, driver.from(1, ChronoUnit.SECONDS));

        Assert.assertEquals(60 * 1000 * 1000, driver.from(1, ChronoUnit.MINUTES));
        Assert.assertEquals(Long.MAX_VALUE, driver.from(Long.MAX_VALUE, ChronoUnit.MICROS));

        Assert.assertEquals(driver.from(1, ChronoUnit.HOURS), driver.from(60, ChronoUnit.MINUTES));
        Assert.assertEquals(0, driver.from(0, ChronoUnit.NANOS));
        Assert.assertEquals(0, driver.from(0, ChronoUnit.MICROS));
        Assert.assertEquals(0, driver.from(0, ChronoUnit.MILLIS));
        Assert.assertEquals(0, driver.from(0, ChronoUnit.SECONDS));
        Assert.assertEquals(0, driver.from(0, ChronoUnit.MINUTES));

        // micros values remain unchanged
        Assert.assertEquals(123456789L, driver.from(123456789L, ChronoUnit.MICROS));
        Assert.assertEquals(123456789L, driver.from(123456789000L, ChronoUnit.NANOS));
    }

    @Test
    public void testFromTimestampUnit() {
        Assert.assertEquals(56799L, driver.from(56799001, CommonUtils.TIMESTAMP_UNIT_NANOS));
        Assert.assertEquals(56799L, driver.from(56799, CommonUtils.TIMESTAMP_UNIT_MICROS));
        Assert.assertEquals(56799_000L, driver.from(56799, CommonUtils.TIMESTAMP_UNIT_MILLIS));
        Assert.assertEquals(60_000_000L, driver.from(60, CommonUtils.TIMESTAMP_UNIT_SECONDS));
        Assert.assertEquals(3600_000_000L, driver.from(60, CommonUtils.TIMESTAMP_UNIT_MINUTES));
        Assert.assertEquals(86400_000_000L, driver.from(24, CommonUtils.TIMESTAMP_UNIT_HOURS));
    }

    @Test
    public void testImplicitCast() {
        Assert.assertEquals(Numbers.LONG_NULL, driver.implicitCast(null));
        long expected2020_01_01 = 1577836800000000L; // 2020-01-01T00:00:00Z in micros
        Assert.assertEquals(expected2020_01_01, driver.implicitCast("2020-01-01T00:00:00Z"));
        Assert.assertEquals(expected2020_01_01, driver.implicitCast("2020-01-01T00:00:00Z", ColumnType.STRING));
        Assert.assertEquals(expected2020_01_01, driver.implicitCast("2020-01-01T00:00:00.000Z", ColumnType.STRING));
        Assert.assertEquals(1577836800123456L, driver.implicitCast("2020-01-01T00:00:00.123456Z", ColumnType.STRING));
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

        Assert.assertEquals(1703462400000000L, driver.implicitCast("2023-12-25 z", ColumnType.STRING));
        Assert.assertEquals(1703462400000000L, driver.implicitCast("2023-12-25 00:00:00.0z", ColumnType.STRING));
        Assert.assertEquals(1703462400000000L, driver.implicitCast("2023-12-25 00:00:00z", ColumnType.STRING));
        Assert.assertEquals(1577836800100000L, driver.implicitCast("2020-01-01T00:00:00.1Z", ColumnType.STRING));
        Assert.assertEquals(1577836800120000L, driver.implicitCast("2020-01-01T00:00:00.12Z", ColumnType.STRING));
        Assert.assertEquals(1577836800123000L, driver.implicitCast("2020-01-01T00:00:00.123Z", ColumnType.STRING));
    }
}
