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

import io.questdb.cairo.TimestampDriver;
import io.questdb.griffin.engine.groupby.TimestampSampler;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class MonthTimestampSamplerTest {
    private final TestTimestampType timestampType;

    public MonthTimestampSamplerTest(TestTimestampType timestampType) {
        this.timestampType = timestampType;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> testParams() {
        return Arrays.asList(new Object[][]{
                {TestTimestampType.MICRO}, {TestTimestampType.NANO}
        });
    }

    @Test
    public void testNegativeOffset() throws Exception {
        // GitHub issue #7760. A negative offset places each bucket start before its calendar month, on the last
        // day of the previous month. nextTimestamp() returned the bucket start itself, and round() floored a value
        // from the last minutes of a month to the bucket a month early.
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final TimestampSampler sampler = timestampDriver.getTimestampSampler(1, 'M', 0);
        sampler.setOffset(timestampDriver.fromMinutes(-10));
        assertRound(sampler, "2024-01-15T10:00:00.000000Z", "2023-12-31T23:50:00.000000Z");
        assertRound(sampler, "2023-12-31T23:50:00.000000Z", "2023-12-31T23:50:00.000000Z");
        assertRound(sampler, "2023-12-31T23:49:59.999999Z", "2023-11-30T23:50:00.000000Z");
        assertRound(sampler, "2024-01-31T23:55:00.000000Z", "2024-01-31T23:50:00.000000Z");
        assertRound(sampler, "2024-02-29T23:49:59.999999Z", "2024-01-31T23:50:00.000000Z");
        assertRound(sampler, "2024-02-29T23:50:00.000000Z", "2024-02-29T23:50:00.000000Z");
        assertRound(sampler, "1969-12-31T23:55:00.000000Z", "1969-12-31T23:50:00.000000Z");
        assertGrid(
                sampler,
                "2023-11-30T23:50:00.000000Z",
                """
                        2023-12-31T23:50:00.000000Z
                        2024-01-31T23:50:00.000000Z
                        2024-02-29T23:50:00.000000Z
                        2024-03-31T23:50:00.000000Z
                        2024-04-30T23:50:00.000000Z
                        """
        );

        final TimestampSampler quarterSampler = timestampDriver.getTimestampSampler(3, 'M', 0);
        quarterSampler.setOffset(timestampDriver.fromMinutes(-10));
        assertRound(quarterSampler, "2024-02-29T23:55:00.000000Z", "2023-12-31T23:50:00.000000Z");
        assertRound(quarterSampler, "2024-03-31T23:49:59.999999Z", "2023-12-31T23:50:00.000000Z");
        assertRound(quarterSampler, "2024-03-31T23:55:00.000000Z", "2024-03-31T23:50:00.000000Z");
        assertGrid(
                quarterSampler,
                "2023-12-31T23:50:00.000000Z",
                """
                        2024-03-31T23:50:00.000000Z
                        2024-06-30T23:50:00.000000Z
                        2024-09-30T23:50:00.000000Z
                        2024-12-31T23:50:00.000000Z
                        2025-03-31T23:50:00.000000Z
                        """
        );
    }

    @Test
    public void testNegativeOffsetMatchesShiftedCalendarFloor() throws Exception {
        // With a negative offset, round() returns the calendar floor of the value shifted by the offset, shifted
        // back, and nextTimestamp() and previousTimestamp() step by whole calendar months on the shifted grid.
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final TimestampDriver.TimestampFloorWithStrideMethod floorMethod = timestampDriver.getTimestampFloorWithStrideMethod("month");
        final long[] offsets = {
                -1,
                timestampDriver.fromMinutes(-10),
                timestampDriver.fromMinutes(-(12 * 60 + 5)),
                timestampDriver.fromMinutes(-(23 * 60 + 59)),
        };
        final int[] strides = {1, 2, 3, 4, 6, 12};
        final long lo = timestampDriver.parseFloorLiteral("1968-01-01T00:00:00.000000Z");
        final long hi = timestampDriver.parseFloorLiteral("2030-01-01T00:00:00.000000Z");
        final long step = timestampDriver.fromMinutes(7 * 60 + 13) + 17;
        for (int stride : strides) {
            final TimestampSampler sampler = timestampDriver.getTimestampSampler(stride, 'M', 0);
            for (long offset : offsets) {
                sampler.setOffset(offset);
                for (long value = lo; value < hi; value += step) {
                    final long expected = floorMethod.floor(value - offset, stride) + offset;
                    final long rounded = sampler.round(value);
                    Assert.assertEquals(expected, rounded);
                    final long next = sampler.nextTimestamp(rounded);
                    Assert.assertEquals(timestampDriver.addMonths(expected - offset, stride) + offset, next);
                    Assert.assertTrue(value < next);
                    Assert.assertEquals(timestampDriver.addMonths(expected - offset, 3 * stride) + offset, sampler.nextTimestamp(rounded, 3));
                    Assert.assertEquals(rounded, sampler.previousTimestamp(next));
                    Assert.assertEquals(rounded, sampler.round(next - 1));
                    Assert.assertEquals(next, sampler.round(next));
                }
            }
        }
    }

    @Test
    public void testNegativeOffsetResetsStart() throws Exception {
        // setOffset() anchors the grid at the calendar month start, also when setStart() anchored the same sampler
        // at another day of the month before
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final TimestampSampler sampler = timestampDriver.getTimestampSampler(1, 'M', 0);
        sampler.setStart(timestampDriver.parseFloorLiteral("2018-11-16T15:00:00.000000Z"));
        sampler.setOffset(timestampDriver.fromMinutes(-10));
        assertRound(sampler, "2024-01-15T10:00:00.000000Z", "2023-12-31T23:50:00.000000Z");
        assertGrid(
                sampler,
                "2023-12-31T23:50:00.000000Z",
                """
                        2024-01-31T23:50:00.000000Z
                        2024-02-29T23:50:00.000000Z
                        """
        );

        sampler.setStart(timestampDriver.parseFloorLiteral("2018-11-16T15:00:00.000000Z"));
        sampler.setOffset(timestampDriver.fromMinutes(10));
        assertRound(sampler, "2024-01-15T10:00:00.000000Z", "2024-01-01T00:10:00.000000Z");
        Assert.assertEquals(
                timestampDriver.parseFloorLiteral("2024-02-01T00:10:00.000000Z"),
                sampler.nextTimestamp(timestampDriver.parseFloorLiteral("2024-01-01T00:10:00.000000Z"))
        );
    }

    @Test
    public void testNextTimestamp() throws Exception {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final TimestampSampler sampler = timestampDriver.getTimestampSampler(1, 'M', 0);

        final String[] src = new String[]{
                "2013-12-31T00:00:00.000000Z",
                "2014-01-01T00:00:00.000000Z",
                "2020-01-01T12:12:12.123456Z",
        };
        final String[] next = new String[]{
                "2014-01-01T00:00:00.000000Z",
                "2014-02-01T00:00:00.000000Z",
                "2020-02-01T00:00:00.000000Z",
        };
        Assert.assertEquals(src.length, next.length);

        for (int i = 0; i < src.length; i++) {
            long ts = timestampDriver.parseFloorLiteral(src[i]);
            long nextTs = sampler.nextTimestamp(ts);
            Assert.assertEquals(timestampDriver.parseFloorLiteral(next[i]), nextTs);
        }
    }

    @Test
    public void testNextTimestampWithStep() throws Exception {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final TimestampSampler sampler = timestampDriver.getTimestampSampler(1, 'M', 0);

        final String[] src = new String[]{
                "2013-12-31T00:00:00.000000Z",
                "2014-01-01T00:00:00.000000Z",
                "2020-01-01T12:12:12.123456Z",
        };
        final String[] next = new String[]{
                "2014-03-01T00:00:00.000000Z",
                "2014-04-01T00:00:00.000000Z",
                "2020-04-01T00:00:00.000000Z",
        };
        Assert.assertEquals(src.length, next.length);

        for (int i = 0; i < src.length; i++) {
            long ts = timestampDriver.parseFloorLiteral(src[i]);
            long nextTs = sampler.nextTimestamp(ts, 3);
            Assert.assertEquals(timestampDriver.parseFloorLiteral(next[i]), nextTs);
        }
    }

    @Test
    public void testPreviousTimestamp() throws Exception {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final TimestampSampler sampler = timestampDriver.getTimestampSampler(1, 'M', 0);

        final String[] src = new String[]{
                "2013-12-31T00:00:00.000000Z",
                "2014-01-01T00:00:00.000000Z",
                "2020-02-01T12:12:12.123456Z",
        };
        final String[] prev = new String[]{
                "2013-11-01T00:00:00.000000Z",
                "2013-12-01T00:00:00.000000Z",
                "2020-01-01T00:00:00.000000Z",
        };
        Assert.assertEquals(src.length, prev.length);

        for (int i = 0; i < src.length; i++) {
            long ts = timestampDriver.parseFloorLiteral(src[i]);
            long prevTs = sampler.previousTimestamp(ts);
            Assert.assertEquals(timestampDriver.parseFloorLiteral(prev[i]), prevTs);
        }
    }

    @Test
    public void testRound() throws Exception {
        final String[] src = new String[]{
                "2013-12-31T00:00:00.000000Z",
                "2014-01-01T00:00:00.000000Z",
                "2014-02-12T12:12:12.123456Z",
                "2022-02-23T17:08:58.000000Z",
                "2014-02-12T12:12:12.123456Z",
                "2024-11-12T12:12:12.123456Z",
        };
        final String[] rounded = new String[]{
                "2013-12-01T00:00:00.000000Z",
                "2014-01-01T00:00:00.000000Z",
                "2014-02-01T00:00:00.000000Z",
                "2022-02-01T00:00:00.000000Z",
                "2014-01-01T00:00:00.000000Z",
                "2024-11-01T00:00:00.000000Z",
        };
        final int[] strides = new int[]{
                1,
                1,
                1,
                1,
                3,
                10,
        };
        Assert.assertEquals(src.length, rounded.length);
        Assert.assertEquals(src.length, strides.length);

        final TimestampDriver timestampDriver = timestampType.getDriver();
        for (int i = 0; i < src.length; i++) {
            final TimestampSampler sampler = timestampDriver.getTimestampSampler(strides[i], 'M', 0);
            final long ts = timestampDriver.parseFloorLiteral(src[i]);
            final long roundedTs = sampler.round(ts);
            Assert.assertEquals(timestampDriver.parseFloorLiteral(rounded[i]), roundedTs);
        }
    }

    @Test
    public void testRoundMatchesFloor() throws Exception {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final String[] src = new String[]{
                "1967-12-31T01:11:42.123456789Z",
                "1970-01-01T00:00:00.000000000Z",
                "1970-01-05T00:00:00.000000000Z",
                "2013-12-31T00:00:00.000000000Z",
                "2014-01-01T01:12:12.000000001Z",
                "2014-02-12T12:12:12.123456789Z",
                "2025-09-04T10:45:01.987654321Z",
        };

        final TimestampSampler sampler = timestampDriver.getTimestampSampler(1, 'M', 0);
        sampler.setStart(0);

        final TimestampDriver.TimestampFloorMethod floorMethod = timestampDriver.getTimestampFloorMethod("month");
        final TimestampDriver.TimestampFloorWithStrideMethod floorWithStrideMethod = timestampDriver.getTimestampFloorWithStrideMethod("month");
        final TimestampDriver.TimestampFloorWithOffsetMethod floorWithOffsetMethod = timestampDriver.getTimestampFloorWithOffsetMethod('M');
        for (int i = 0; i < src.length; i++) {
            final long ts = timestampDriver.parseFloorLiteral(src[i]);
            final long roundedTs = sampler.round(ts);
            Assert.assertEquals(floorMethod.floor(ts), roundedTs);
            Assert.assertEquals(floorWithStrideMethod.floor(ts, 1), roundedTs);
            Assert.assertEquals(floorWithOffsetMethod.floor(ts, 1, 0), roundedTs);
        }
    }

    @Test
    public void testSimple() throws Exception {
        final StringSink sink = new StringSink();
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final TimestampSampler sampler = timestampDriver.getTimestampSampler(6, 'M', 0);

        long timestamp = timestampDriver.parseFloorLiteral("2018-11-16T15:00:00.000000Z");
        sampler.setStart(timestamp);

        for (int i = 0; i < 20; i++) {
            long ts = sampler.nextTimestamp(timestamp);
            sink.putISODate(timestampDriver, ts).put('\n');
            Assert.assertEquals(timestamp, sampler.previousTimestamp(ts));
            timestamp = ts;
        }

        TestUtils.assertEquals(
                AbstractCairoTest.replaceTimestampSuffix(
                        """
                                2019-05-16T15:00:00.000000Z
                                2019-11-16T15:00:00.000000Z
                                2020-05-16T15:00:00.000000Z
                                2020-11-16T15:00:00.000000Z
                                2021-05-16T15:00:00.000000Z
                                2021-11-16T15:00:00.000000Z
                                2022-05-16T15:00:00.000000Z
                                2022-11-16T15:00:00.000000Z
                                2023-05-16T15:00:00.000000Z
                                2023-11-16T15:00:00.000000Z
                                2024-05-16T15:00:00.000000Z
                                2024-11-16T15:00:00.000000Z
                                2025-05-16T15:00:00.000000Z
                                2025-11-16T15:00:00.000000Z
                                2026-05-16T15:00:00.000000Z
                                2026-11-16T15:00:00.000000Z
                                2027-05-16T15:00:00.000000Z
                                2027-11-16T15:00:00.000000Z
                                2028-05-16T15:00:00.000000Z
                                2028-11-16T15:00:00.000000Z
                                """,
                        timestampType.getTypeName()
                ),
                sink
        );
    }

    @Test
    public void testSimpleNegative() throws Exception {
        final StringSink sink = new StringSink();
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final TimestampSampler sampler = timestampDriver.getTimestampSampler(6, 'M', 0);

        long timestamp = timestampDriver.parseFloorLiteral("1960-11-16T15:00:00.000000Z");
        sampler.setStart(timestamp);

        for (int i = 0; i < 20; i++) {
            long ts = sampler.nextTimestamp(timestamp);
            sink.putISODate(timestampDriver, ts).put('\n');
            Assert.assertEquals(timestamp, sampler.previousTimestamp(ts));
            timestamp = ts;
        }

        TestUtils.assertEquals(
                AbstractCairoTest.replaceTimestampSuffix(
                        """
                                1961-05-16T15:00:00.000000Z
                                1961-11-16T15:00:00.000000Z
                                1962-05-16T15:00:00.000000Z
                                1962-11-16T15:00:00.000000Z
                                1963-05-16T15:00:00.000000Z
                                1963-11-16T15:00:00.000000Z
                                1964-05-16T15:00:00.000000Z
                                1964-11-16T15:00:00.000000Z
                                1965-05-16T15:00:00.000000Z
                                1965-11-16T15:00:00.000000Z
                                1966-05-16T15:00:00.000000Z
                                1966-11-16T15:00:00.000000Z
                                1967-05-16T15:00:00.000000Z
                                1967-11-16T15:00:00.000000Z
                                1968-05-16T15:00:00.000000Z
                                1968-11-16T15:00:00.000000Z
                                1969-05-16T15:00:00.000000Z
                                1969-11-16T15:00:00.000000Z
                                1970-05-16T15:00:00.000000Z
                                1970-11-16T15:00:00.000000Z
                                """,
                        timestampType.getTypeName()
                ),
                sink
        );
    }

    // Walks the grid from a bucket start: nextTimestamp() returns each next bucket start, round() returns each
    // bucket start for itself and the previous one for the instant before it, and previousTimestamp() and
    // nextTimestamp(ts, n) agree with the walk.
    private void assertGrid(TimestampSampler sampler, String first, String expected) throws Exception {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final String expectedGrid = AbstractCairoTest.replaceTimestampSuffix(expected, timestampType.getTypeName());
        final StringSink sink = new StringSink();
        final long firstTs = timestampDriver.parseFloorLiteral(first);
        long ts = firstTs;
        int steps = 0;
        while (sink.length() < expectedGrid.length()) {
            final long next = sampler.nextTimestamp(ts);
            Assert.assertTrue(next > ts);
            Assert.assertEquals(ts, sampler.previousTimestamp(next));
            Assert.assertEquals(next, sampler.round(next));
            Assert.assertEquals(ts, sampler.round(next - 1));
            Assert.assertEquals(next, sampler.nextTimestamp(firstTs, ++steps));
            sink.putISODate(timestampDriver, next).put('\n');
            ts = next;
        }
        TestUtils.assertEquals(expectedGrid, sink);
    }

    private void assertRound(TimestampSampler sampler, String value, String expected) throws Exception {
        final TimestampDriver timestampDriver = timestampType.getDriver();
        Assert.assertEquals(
                timestampDriver.parseFloorLiteral(expected),
                sampler.round(timestampDriver.parseFloorLiteral(value))
        );
    }
}
