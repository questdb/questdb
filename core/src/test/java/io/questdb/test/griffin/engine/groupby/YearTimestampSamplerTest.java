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

package io.questdb.test.griffin.engine.groupby;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.NanosTimestampDriver;
import io.questdb.cairo.TimestampDriver;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.groupby.TimestampSampler;
import io.questdb.griffin.engine.groupby.YearTimestampMicrosSampler;
import io.questdb.griffin.engine.groupby.YearTimestampNanosSampler;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.datetime.nanotime.Nanos;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class YearTimestampSamplerTest {
    private static final String FIXED_PART = "-11-16T15:00:00.000000Z\n";

    @Test
    public void testNegativeOffset() throws Exception {
        // GitHub issue #7760. A negative offset places each bucket start before its calendar year, on the last
        // day of the previous year. nextTimestamp() returned the bucket start itself, and round() floored a value
        // from the last minutes of a year to the bucket a year early.
        testNegativeOffset(MicrosTimestampDriver.INSTANCE);
        testNegativeOffset(NanosTimestampDriver.INSTANCE);
    }

    @Test
    public void testNegativeOffsetMatchesShiftedCalendarFloor() throws Exception {
        // With a negative offset, round() returns the calendar floor of the value shifted by the offset, shifted
        // back, and nextTimestamp() and previousTimestamp() step by whole calendar years on the shifted grid.
        testNegativeOffsetMatchesShiftedCalendarFloor(MicrosTimestampDriver.INSTANCE);
        testNegativeOffsetMatchesShiftedCalendarFloor(NanosTimestampDriver.INSTANCE);
    }

    @Test
    public void testNegativeOffsetResetsStart() throws Exception {
        // setOffset() anchors the grid at the calendar year start, also when setStart() anchored the same sampler
        // at another day of the year before
        testNegativeOffsetResetsStart(MicrosTimestampDriver.INSTANCE);
        testNegativeOffsetResetsStart(NanosTimestampDriver.INSTANCE);
    }

    @Test
    public void testRound() throws NumericException {
        testRound(1, "2023-01-01T00:00:00.000000Z", "2023-01-01T00:00:00.000000Z");
        testRound(1, "2023-01-01T00:00:00.000001Z", "2023-01-01T00:00:00.000000Z");
        testRound(1, "2024-08-08T12:57:07.388314Z", "2024-01-01T00:00:00.000000Z");

        testRound(2, "2024-01-01T00:00:00.000000Z", "2024-01-01T00:00:00.000000Z");
        testRound(2, "2025-08-08T12:57:07.388314Z", "2024-01-01T00:00:00.000000Z");
        testRound(2, "2025-12-31T23:59:59.999999Z", "2024-01-01T00:00:00.000000Z");

        testRound(10, "2020-01-01T00:00:00.000000Z", "2020-01-01T00:00:00.000000Z");
        testRound(10, "2024-01-01T00:00:00.000000Z", "2020-01-01T00:00:00.000000Z");
        testRound(10, "2025-12-31T23:59:59.999999Z", "2020-01-01T00:00:00.000000Z");

        testRound(50, "1970-01-01T00:00:00.000000Z", "1970-01-01T00:00:00.000000Z");
        testRound(50, "2051-01-01T00:00:00.000000Z", "2020-01-01T00:00:00.000000Z");
        testRound(100, "2024-01-01T00:00:00.000000Z", "1970-01-01T00:00:00.000000Z");
        testRound(1000, "2025-12-31T23:59:59.999999Z", "1970-01-01T00:00:00.000000Z");
    }

    @Test
    public void testRoundMatchesFloorMicros() throws Exception {
        testRoundMatchesFloor(MicrosTimestampDriver.INSTANCE);
    }

    @Test
    public void testRoundMatchesFloorNanos() throws Exception {
        testRoundMatchesFloor(NanosTimestampDriver.INSTANCE);
    }

    @Test
    public void testSingleStep() throws NumericException {
        testSampler(
                1,
                "2022" + FIXED_PART +
                        "2026" + FIXED_PART +
                        "2030" + FIXED_PART +
                        "2034" + FIXED_PART +
                        "2038" + FIXED_PART +
                        "2042" + FIXED_PART +
                        "2046" + FIXED_PART +
                        "2050" + FIXED_PART +
                        "2054" + FIXED_PART +
                        "2058" + FIXED_PART +
                        "2062" + FIXED_PART +
                        "2066" + FIXED_PART +
                        "2070" + FIXED_PART +
                        "2074" + FIXED_PART +
                        "2078" + FIXED_PART +
                        "2082" + FIXED_PART +
                        "2086" + FIXED_PART +
                        "2090" + FIXED_PART +
                        "2094" + FIXED_PART +
                        "2098" + FIXED_PART
        );
    }

    @Test
    public void testTripleStep() throws NumericException {
        testSampler(
                3,
                "2030" + FIXED_PART +
                        "2042" + FIXED_PART +
                        "2054" + FIXED_PART +
                        "2066" + FIXED_PART +
                        "2078" + FIXED_PART +
                        "2090" + FIXED_PART +
                        "2102" + FIXED_PART +
                        "2114" + FIXED_PART +
                        "2126" + FIXED_PART +
                        "2138" + FIXED_PART +
                        "2150" + FIXED_PART +
                        "2162" + FIXED_PART +
                        "2174" + FIXED_PART +
                        "2186" + FIXED_PART +
                        "2198" + FIXED_PART +
                        "2210" + FIXED_PART +
                        "2222" + FIXED_PART +
                        "2234" + FIXED_PART +
                        "2246" + FIXED_PART +
                        "2258" + FIXED_PART
        );
    }

    // Walks the grid from a bucket start: nextTimestamp() returns each next bucket start, round() returns each
    // bucket start for itself and the previous one for the instant before it, and previousTimestamp() and
    // nextTimestamp(ts, n) agree with the walk.
    private static void assertGrid(TimestampDriver driver, TimestampSampler sampler, String first, String expected) throws NumericException {
        final String expectedGrid = AbstractCairoTest.replaceTimestampSuffix(expected, ColumnType.nameOf(driver.getTimestampType()));
        final StringSink sink = new StringSink();
        final long firstTs = driver.parseFloorLiteral(first);
        long ts = firstTs;
        int steps = 0;
        while (sink.length() < expectedGrid.length()) {
            final long next = sampler.nextTimestamp(ts);
            Assert.assertTrue(next > ts);
            Assert.assertEquals(ts, sampler.previousTimestamp(next));
            Assert.assertEquals(next, sampler.round(next));
            Assert.assertEquals(ts, sampler.round(next - 1));
            Assert.assertEquals(next, sampler.nextTimestamp(firstTs, ++steps));
            sink.putISODate(driver, next).put('\n');
            ts = next;
        }
        TestUtils.assertEquals(expectedGrid, sink);
    }

    private static void assertRound(TimestampDriver driver, TimestampSampler sampler, String value, String expected) throws NumericException {
        Assert.assertEquals(driver.parseFloorLiteral(expected), sampler.round(driver.parseFloorLiteral(value)));
    }

    private static void testNegativeOffset(TimestampDriver driver) throws NumericException, SqlException {
        final TimestampSampler sampler = driver.getTimestampSampler(1, 'y', 0);
        sampler.setOffset(driver.fromMinutes(-10));
        assertRound(driver, sampler, "2024-06-01T00:00:00.000000Z", "2023-12-31T23:50:00.000000Z");
        assertRound(driver, sampler, "2023-12-31T23:50:00.000000Z", "2023-12-31T23:50:00.000000Z");
        assertRound(driver, sampler, "2023-12-31T23:49:59.999999Z", "2022-12-31T23:50:00.000000Z");
        assertRound(driver, sampler, "2024-12-31T23:49:59.999999Z", "2023-12-31T23:50:00.000000Z");
        assertRound(driver, sampler, "2024-12-31T23:55:00.000000Z", "2024-12-31T23:50:00.000000Z");
        assertGrid(
                driver,
                sampler,
                "2023-12-31T23:50:00.000000Z",
                """
                        2024-12-31T23:50:00.000000Z
                        2025-12-31T23:50:00.000000Z
                        2026-12-31T23:50:00.000000Z
                        """
        );

        final TimestampSampler twoYearSampler = driver.getTimestampSampler(2, 'y', 0);
        twoYearSampler.setOffset(driver.fromMinutes(-10));
        assertRound(driver, twoYearSampler, "2025-12-31T23:49:59.999999Z", "2023-12-31T23:50:00.000000Z");
        assertRound(driver, twoYearSampler, "2025-12-31T23:55:00.000000Z", "2025-12-31T23:50:00.000000Z");
        assertGrid(
                driver,
                twoYearSampler,
                "2023-12-31T23:50:00.000000Z",
                """
                        2025-12-31T23:50:00.000000Z
                        2027-12-31T23:50:00.000000Z
                        2029-12-31T23:50:00.000000Z
                        """
        );
    }

    private static void testNegativeOffsetMatchesShiftedCalendarFloor(TimestampDriver driver) throws NumericException, SqlException {
        final TimestampDriver.TimestampFloorWithStrideMethod floorMethod = driver.getTimestampFloorWithStrideMethod("year");
        final long[] offsets = {
                -1,
                driver.fromMinutes(-10),
                driver.fromMinutes(-(12 * 60 + 5)),
                driver.fromMinutes(-(23 * 60 + 59)),
        };
        final int[] strides = {1, 2, 3, 5, 10};
        final long lo = driver.parseFloorLiteral("1971-01-01T00:00:00.000000Z");
        final long hi = driver.parseFloorLiteral("2100-01-01T00:00:00.000000Z");
        final long step = driver.fromMinutes(3 * 24 * 60 + 7 * 60 + 13) + 17;
        for (int stride : strides) {
            final TimestampSampler sampler = driver.getTimestampSampler(stride, 'y', 0);
            for (long offset : offsets) {
                sampler.setOffset(offset);
                for (long value = lo; value < hi; value += step) {
                    final long expected = floorMethod.floor(value - offset, stride) + offset;
                    final long rounded = sampler.round(value);
                    Assert.assertEquals(expected, rounded);
                    final long next = sampler.nextTimestamp(rounded);
                    Assert.assertEquals(driver.addYears(expected - offset, stride) + offset, next);
                    Assert.assertTrue(value < next);
                    Assert.assertEquals(driver.addYears(expected - offset, 3 * stride) + offset, sampler.nextTimestamp(rounded, 3));
                    Assert.assertEquals(rounded, sampler.previousTimestamp(next));
                    Assert.assertEquals(rounded, sampler.round(next - 1));
                    Assert.assertEquals(next, sampler.round(next));
                }
            }
        }
    }

    private static void testNegativeOffsetResetsStart(TimestampDriver driver) throws NumericException, SqlException {
        final TimestampSampler sampler = driver.getTimestampSampler(1, 'y', 0);
        sampler.setStart(driver.parseFloorLiteral("2018-11-16T15:00:00.000000Z"));
        sampler.setOffset(driver.fromMinutes(-10));
        assertRound(driver, sampler, "2024-06-01T00:00:00.000000Z", "2023-12-31T23:50:00.000000Z");
        assertGrid(
                driver,
                sampler,
                "2023-12-31T23:50:00.000000Z",
                """
                        2024-12-31T23:50:00.000000Z
                        2025-12-31T23:50:00.000000Z
                        """
        );

        sampler.setStart(driver.parseFloorLiteral("2018-11-16T15:00:00.000000Z"));
        sampler.setOffset(driver.fromMinutes(10));
        assertRound(driver, sampler, "2024-06-01T00:00:00.000000Z", "2024-01-01T00:10:00.000000Z");
        Assert.assertEquals(
                driver.parseFloorLiteral("2025-01-01T00:10:00.000000Z"),
                sampler.nextTimestamp(driver.parseFloorLiteral("2024-01-01T00:10:00.000000Z"))
        );
    }

    private void testRound(int stepYears, String timestamp, String expectedRounded) throws NumericException {
        final YearTimestampMicrosSampler samplerUs = new YearTimestampMicrosSampler(stepYears);
        samplerUs.setStart(0);
        final long tsUs = MicrosFormatUtils.parseUTCTimestamp(timestamp);
        final long expectedUs = MicrosFormatUtils.parseUTCTimestamp(expectedRounded);
        Assert.assertEquals(expectedUs, samplerUs.round(tsUs));

        final YearTimestampNanosSampler samplerNs = new YearTimestampNanosSampler(stepYears);
        samplerNs.setStart(0);
        final long tsNs = tsUs * Nanos.MICRO_NANOS;
        Assert.assertEquals(expectedUs * Nanos.MICRO_NANOS, samplerNs.round(tsNs));
    }

    private void testRoundMatchesFloor(TimestampDriver timestampDriver) throws NumericException, SqlException {
        final String[] src = new String[]{
                "1967-12-31T01:11:42.123456789Z",
                "1970-01-01T00:00:00.000000000Z",
                "1970-01-05T00:00:00.000000000Z",
                "2013-12-31T00:00:00.000000000Z",
                "2014-01-01T01:12:12.000000001Z",
                "2014-02-12T12:12:12.123456789Z",
                "2025-09-04T10:45:01.987654321Z",
        };

        final TimestampSampler sampler = timestampDriver.getTimestampSampler(1, 'y', 0);
        sampler.setStart(0);

        final TimestampDriver.TimestampFloorMethod floorMethod = timestampDriver.getTimestampFloorMethod("year");
        final TimestampDriver.TimestampFloorWithStrideMethod floorWithStrideMethod = timestampDriver.getTimestampFloorWithStrideMethod("year");
        final TimestampDriver.TimestampFloorWithOffsetMethod floorWithOffsetMethod = timestampDriver.getTimestampFloorWithOffsetMethod('y');
        for (int i = 0; i < src.length; i++) {
            final long ts = timestampDriver.parseFloorLiteral(src[i]);
            final long roundedTs = sampler.round(ts);
            Assert.assertEquals(floorMethod.floor(ts), roundedTs);
            Assert.assertEquals(floorWithStrideMethod.floor(ts, 1), roundedTs);
            Assert.assertEquals(floorWithOffsetMethod.floor(ts, 1, 0), roundedTs);
        }
    }

    private void testSampler(int stepYears, String expected) throws NumericException {
        testSampler(stepYears, expected, ColumnType.TIMESTAMP_MICRO);
        testSampler(stepYears, expected, ColumnType.TIMESTAMP_NANO);
    }

    private void testSampler(int stepYears, String expected, int timestampType) throws NumericException {
        StringSink sink = new StringSink();
        TimestampSampler sampler = ColumnType.isTimestampMicro(timestampType)
                ? new YearTimestampMicrosSampler(4)
                : new YearTimestampNanosSampler(4);
        TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
        long timestamp = driver.parseFloorLiteral("2018-11-16T15:00:00.000000Z");
        sampler.setStart(timestamp);
        for (int i = 0; i < 20; i++) {
            long ts = sampler.nextTimestamp(timestamp, stepYears);
            sink.putISODate(driver, ts).put('\n');
            if (stepYears == 1) {
                Assert.assertEquals(timestamp, sampler.previousTimestamp(ts));
            }
            timestamp = ts;
        }
        TestUtils.assertEquals(AbstractCairoTest.replaceTimestampSuffix(expected, ColumnType.nameOf(timestampType)), sink);
    }
}
