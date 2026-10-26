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
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
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
    public void testOffsetMatchesFloorWithOffset() throws Exception {
        // Regression test for #7769 and #7798. round() after setOffset() must return the same bucket as
        // timestamp_floor_utc() on the GROUP BY path, including negative offsets, where the GROUP BY path anchors
        // at the last day of December 1969 and used to put the end of a month shorter than 31 days into the
        // previous bucket.
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final TimestampDriver.TimestampFloorWithOffsetMethod floorMethod = timestampDriver.getTimestampFloorWithOffsetMethod('M');
        final long[] offsets = {
                timestampDriver.fromMinutes(-(23 * 60 + 59)),
                timestampDriver.fromMinutes(-60),
                -1,
                1,
                timestampDriver.fromMinutes(10 * 60),
                timestampDriver.fromMinutes(23 * 60 + 59),
        };
        final int[] strides = {1, 2, 3, 5, 7, 12};
        final long lo = timestampDriver.parseFloorLiteral("1970-03-01T00:00:00.000000Z");
        final long hi = timestampDriver.parseFloorLiteral("2030-01-01T00:00:00.000000Z");
        final long step = timestampDriver.fromMinutes(7 * 60 + 13) + 17;
        for (int stride : strides) {
            final TimestampSampler sampler = timestampDriver.getTimestampSampler(stride, 'M', 0);
            for (long offset : offsets) {
                sampler.setOffset(offset);
                for (long value = lo; value < hi; value += step) {
                    final long rounded = sampler.round(value);
                    Assert.assertEquals(floorMethod.floor(value, stride, offset), rounded);
                    Assert.assertTrue(rounded <= value);
                    Assert.assertTrue(value < sampler.nextTimestamp(rounded));
                }
            }
        }
    }

    @Test
    public void testPositiveOffset() throws Exception {
        // Regression test for #7769. A positive offset moves bucket starts past the start of their calendar month,
        // and round() used to return that bucket start even for values before it.
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final TimestampSampler sampler = timestampDriver.getTimestampSampler(1, 'M', 0);
        sampler.setOffset(timestampDriver.fromMinutes(10 * 60));
        assertRound(sampler, "2024-02-01T06:00:00.000000Z", "2024-01-01T10:00:00.000000Z");
        assertRound(sampler, "2024-02-01T09:59:59.999999Z", "2024-01-01T10:00:00.000000Z");
        assertRound(sampler, "2024-02-01T10:00:00.000000Z", "2024-02-01T10:00:00.000000Z");
        assertRound(sampler, "2024-02-29T23:59:59.999999Z", "2024-02-01T10:00:00.000000Z");
        assertRound(sampler, "2024-01-01T00:00:00.000000Z", "2023-12-01T10:00:00.000000Z");
        assertGrid(
                sampler,
                "2023-12-01T10:00:00.000000Z",
                """
                        2024-01-01T10:00:00.000000Z
                        2024-02-01T10:00:00.000000Z
                        2024-03-01T10:00:00.000000Z
                        """
        );
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
                // a stride that doesn't divide 12 counts months from January 1970, as Micros.floorMM() does
                "2024-03-01T00:00:00.000000Z",
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
    public void testRoundMatchesGridWalk() throws Exception {
        // round() must return the last bucket start, as reached by nextTimestamp(), at or before the value, for every
        // state that setStart() and setOffset() can leave: anchors on the first, middle and last days of a month,
        // before and after the values, with a time of day, and offsets of either sign up to 23:59 (the WITH OFFSET
        // limit) and 47:58 (what a mat view refresh passes for WITH OFFSET in a fixed-offset time zone)
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final String[] anchors = {
                "1965-03-01T00:00:00.000000Z",
                "1999-01-15T10:30:15.123456Z",
                "2018-01-28T23:59:59.999999Z",
                "2018-01-29T00:00:00.000000Z",
                "2018-03-30T12:00:00.000000Z",
                "2018-01-31T00:00:00.000001Z",
                "2024-02-29T08:00:00.000000Z",
                "2029-12-31T10:00:00.000000Z",
        };
        final long[] offsets = {
                timestampDriver.fromMinutes(-(47 * 60 + 58)),
                timestampDriver.fromMinutes(-(23 * 60 + 59)),
                timestampDriver.fromMinutes(-60),
                -1,
                0,
                1,
                timestampDriver.fromMinutes(10 * 60),
                timestampDriver.fromMinutes(23 * 60 + 59),
                timestampDriver.fromMinutes(47 * 60 + 58),
        };
        final int[] strides = {1, 2, 3, 4, 5, 6, 7, 12, 13, 24};
        final long lo = timestampDriver.parseFloorLiteral("1960-01-01T00:00:00.000000Z");
        final long hi = timestampDriver.parseFloorLiteral("2030-01-01T00:00:00.000000Z");
        final long step = timestampDriver.fromMinutes(3 * 24 * 60 + 7 * 60 + 13) + 17;
        for (int stride : strides) {
            // an unconfigured sampler (no setStart() or setOffset() call) anchors its grid at January 1970
            assertRoundMatchesGridWalk(timestampDriver.getTimestampSampler(stride, 'M', 0), 0, lo, hi, step);
            final TimestampSampler sampler = timestampDriver.getTimestampSampler(stride, 'M', 0);
            for (String anchor : anchors) {
                final long anchorTs = timestampDriver.parseFloorLiteral(anchor);
                sampler.setStart(anchorTs);
                assertRoundMatchesGridWalk(sampler, anchorTs, lo, hi, step);
            }
            for (long offset : offsets) {
                sampler.setOffset(offset);
                // the offset grid starts at January 1970 plus the offset
                assertRoundMatchesGridWalk(sampler, offset, lo, hi, step);
            }
            // setStart() after setOffset() re-anchors the grid at a month that is off the January 1970 grid for every
            // multi-month stride
            final long anchorTs = timestampDriver.parseFloorLiteral("2029-12-31T10:00:00.000000Z");
            sampler.setStart(anchorTs);
            assertRoundMatchesGridWalk(sampler, anchorTs, lo, hi, step);
        }
    }

    @Test
    public void testRoundMatchesGridWalkAroundYearZero() throws Exception {
        // TIMESTAMP reaches years before year 0, where the month index (12 * year + month - 1) of the value and of
        // the anchor is negative. round() must count strides from the anchor month across year 0 as well.
        Assume.assumeTrue(timestampType == TestTimestampType.MICRO);
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final long[] anchors = {
                Micros.toMicros(-2, 11, 30) + 10 * Micros.HOUR_MICROS,
                Micros.toMicros(-1, 1, 1),
                Micros.toMicros(0, 2, 29) + 8 * Micros.HOUR_MICROS,
                Micros.toMicros(1, 12, 31) + 23 * Micros.HOUR_MICROS,
        };
        final long[] offsets = {-Micros.HOUR_MICROS, Micros.HOUR_MICROS};
        final int[] strides = {1, 2, 3, 4, 5, 6, 7, 12, 13, 24};
        final long lo = Micros.toMicros(-4, 1, 1);
        final long hi = Micros.toMicros(4, 1, 1);
        final long step = timestampDriver.fromMinutes(3 * 24 * 60 + 7 * 60 + 13) + 17;
        for (int stride : strides) {
            final TimestampSampler sampler = timestampDriver.getTimestampSampler(stride, 'M', 0);
            assertRoundMatchesGridWalk(sampler, 0, lo, hi, step);
            for (long anchor : anchors) {
                sampler.setStart(anchor);
                assertRoundMatchesGridWalk(sampler, anchor, lo, hi, step);
            }
            for (long offset : offsets) {
                sampler.setOffset(offset);
                assertRoundMatchesGridWalk(sampler, offset, lo, hi, step);
            }
        }
    }

    @Test
    public void testRoundNearEndOfTimestampRange() throws Exception {
        // The candidate bucket in the value's calendar month may start after the largest timestamp, overflowing a
        // long. round() must return the previous bucket.
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final TimestampSampler sampler = timestampDriver.getTimestampSampler(1, 'M', 0);
        sampler.setStart(timestampDriver.parseFloorLiteral("2024-01-15T08:00:00.000000Z"));
        final long expected = timestampType == TestTimestampType.MICRO
                ? Micros.toMicros(294_246, 12, 15) + 8 * Micros.HOUR_MICROS
                : timestampDriver.parseFloorLiteral("2262-03-15T08:00:00.000000Z");
        Assert.assertEquals(expected, sampler.round(Long.MAX_VALUE));
        Assert.assertEquals(expected, sampler.round(expected + 1));
    }

    @Test
    public void testRoundNearEndOfTimestampRangeWithNegativeOffset() throws Exception {
        // round() shifts the value by a negative offset to find the calendar month of its bucket. Near the largest
        // timestamp, the shift overflows a long, so round() uses the value as is, which belongs to the bucket of the
        // last calendar month: 294247-01 for TIMESTAMP and 2262-04 for TIMESTAMP_NS.
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final long oneHour = timestampDriver.fromMinutes(60);
        final long almostTwoDays = timestampDriver.fromMinutes(47 * 60 + 58);
        final int stride;
        final long expectedOneHour;
        final long expectedAlmostTwoDays;
        final long expectedStride;
        if (timestampType == TestTimestampType.MICRO) {
            // 294246-12-31T23:00, 294246-12-30T00:02 and, for 5 months counted from January 1970, 294246-08-31T23:00
            stride = 5;
            expectedOneHour = 9_223_371_241_200_000_000L;
            expectedAlmostTwoDays = 9_223_371_072_120_000_000L;
            expectedStride = 9_223_360_700_400_000_000L;
            Assert.assertEquals(expectedOneHour, Micros.toMicros(294_247, 1, 1) - oneHour);
            Assert.assertEquals(expectedAlmostTwoDays, Micros.toMicros(294_247, 1, 1) - almostTwoDays);
            Assert.assertEquals(expectedStride, Micros.toMicros(294_246, 9, 1) - oneHour);
        } else {
            // 2 months counted from January 1970 start a bucket in 2262-03
            stride = 2;
            expectedOneHour = timestampDriver.parseFloorLiteral("2262-03-31T23:00:00.000000Z");
            expectedAlmostTwoDays = timestampDriver.parseFloorLiteral("2262-03-30T00:02:00.000000Z");
            expectedStride = timestampDriver.parseFloorLiteral("2262-02-28T23:00:00.000000Z");
        }

        final TimestampSampler sampler = timestampDriver.getTimestampSampler(1, 'M', 0);
        sampler.setOffset(-oneHour);
        assertRoundOfLargestTimestamps(sampler, expectedOneHour);
        sampler.setOffset(-almostTwoDays);
        assertRoundOfLargestTimestamps(sampler, expectedAlmostTwoDays);

        final TimestampSampler strideSampler = timestampDriver.getTimestampSampler(stride, 'M', 0);
        strideSampler.setOffset(-oneHour);
        assertRoundOfLargestTimestamps(strideSampler, expectedStride);
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

    @Test
    public void testStartMatchesFloorWithOffset() throws Exception {
        // Regression test for #7769. round() after setStart() must return the last bucket start at or before the
        // value. It used to return the bucket start in the value's calendar month, which lies after values that come
        // earlier in their month than the anchor, and it counted multi-month strides from January.
        // timestamp_floor_utc() on the GROUP BY path counts months from the anchor that FROM sets.
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final TimestampDriver.TimestampFloorWithOffsetMethod floorMethod = timestampDriver.getTimestampFloorWithOffsetMethod('M');
        final String[] anchors = {
                "2018-01-15T08:00:00.000000Z",
                "2018-02-15T08:00:00.000000Z",
                "2018-01-28T23:59:59.999999Z",
                "2018-01-29T12:00:00.000000Z",
                "2018-03-30T00:00:00.000000Z",
                "2018-01-31T00:00:00.000000Z",
                "2018-08-31T22:30:00.000000Z",
        };
        final int[] strides = {1, 2, 3, 5, 7, 12};
        final long hi = timestampDriver.parseFloorLiteral("2030-01-01T00:00:00.000000Z");
        final long step = timestampDriver.fromMinutes(7 * 60 + 13) + 17;
        for (int stride : strides) {
            final TimestampSampler sampler = timestampDriver.getTimestampSampler(stride, 'M', 0);
            for (String anchor : anchors) {
                final long anchorTs = timestampDriver.parseFloorLiteral(anchor);
                sampler.setStart(anchorTs);
                Assert.assertEquals(anchorTs, sampler.round(anchorTs));
                for (long value = anchorTs; value < hi; value += step) {
                    final long rounded = sampler.round(value);
                    Assert.assertEquals(floorMethod.floor(value, stride, anchorTs), rounded);
                    Assert.assertTrue(rounded <= value);
                    Assert.assertTrue(value < sampler.nextTimestamp(rounded));
                }
            }
        }
    }

    @Test
    public void testStartMidMonth() throws Exception {
        // GitHub issue #7769
        final TimestampDriver timestampDriver = timestampType.getDriver();
        final TimestampSampler sampler = timestampDriver.getTimestampSampler(1, 'M', 0);
        sampler.setStart(timestampDriver.parseFloorLiteral("2024-01-15T08:00:00.000000Z"));
        assertRound(sampler, "2024-02-10T12:00:00.000000Z", "2024-01-15T08:00:00.000000Z");
        assertRound(sampler, "2024-02-15T07:59:59.999999Z", "2024-01-15T08:00:00.000000Z");
        assertRound(sampler, "2024-02-15T08:00:00.000000Z", "2024-02-15T08:00:00.000000Z");
        assertRound(sampler, "2024-01-01T00:00:00.000000Z", "2023-12-15T08:00:00.000000Z");

        // the grid of a multi-month stride starts in the month of the anchor, not in January
        final TimestampSampler quarterSampler = timestampDriver.getTimestampSampler(3, 'M', 0);
        quarterSampler.setStart(timestampDriver.parseFloorLiteral("2024-02-15T08:00:00.000000Z"));
        assertRound(quarterSampler, "2024-02-15T08:00:00.000000Z", "2024-02-15T08:00:00.000000Z");
        assertRound(quarterSampler, "2024-05-15T07:59:59.999999Z", "2024-02-15T08:00:00.000000Z");
        assertRound(quarterSampler, "2024-06-01T00:00:00.000000Z", "2024-05-15T08:00:00.000000Z");
        assertRound(quarterSampler, "2024-02-01T00:00:00.000000Z", "2023-11-15T08:00:00.000000Z");

        // an anchor on the 31st clamps to the last day of shorter months
        sampler.setStart(timestampDriver.parseFloorLiteral("2024-01-31T00:00:00.000000Z"));
        assertRound(sampler, "2024-02-29T12:00:00.000000Z", "2024-02-29T00:00:00.000000Z");
        assertRound(sampler, "2024-03-30T23:59:59.999999Z", "2024-02-29T00:00:00.000000Z");
        assertRound(sampler, "2024-03-31T00:00:00.000000Z", "2024-03-31T00:00:00.000000Z");
        assertRound(sampler, "2024-04-30T05:00:00.000000Z", "2024-04-30T00:00:00.000000Z");
        assertGrid(
                sampler,
                "2024-01-31T00:00:00.000000Z",
                """
                        2024-02-29T00:00:00.000000Z
                        2024-03-31T00:00:00.000000Z
                        2024-04-30T00:00:00.000000Z
                        2024-05-31T00:00:00.000000Z
                        """
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

    // Walks the grid from gridPoint, a bucket start, back to lo and then forward to hi, and checks that round()
    // returns the bucket start for values every step apart and for the instants around each bucket start.
    private void assertRoundMatchesGridWalk(TimestampSampler sampler, long gridPoint, long lo, long hi, long step) {
        long bucketStart = gridPoint;
        while (bucketStart > lo) {
            final long previousBucketStart = sampler.previousTimestamp(bucketStart);
            // the walk fails instead of looping forever when previousTimestamp() doesn't move back
            Assert.assertTrue(previousBucketStart < bucketStart);
            bucketStart = previousBucketStart;
        }
        long value = lo;
        while (bucketStart < hi) {
            final long nextBucketStart = sampler.nextTimestamp(bucketStart);
            // the walk fails instead of looping forever when nextTimestamp() doesn't move forward
            Assert.assertTrue(nextBucketStart > bucketStart);
            for (; value < nextBucketStart; value += step) {
                Assert.assertEquals(bucketStart, sampler.round(value));
            }
            Assert.assertEquals(bucketStart, sampler.round(nextBucketStart - 1));
            Assert.assertEquals(nextBucketStart, sampler.round(nextBucketStart));
            Assert.assertEquals(nextBucketStart, sampler.round(nextBucketStart + 1));
            bucketStart = nextBucketStart;
        }
    }

    private void assertRoundOfLargestTimestamps(TimestampSampler sampler, long expected) {
        Assert.assertEquals(expected, sampler.round(Long.MAX_VALUE));
        Assert.assertEquals(expected, sampler.round(Long.MAX_VALUE - 1));
    }
}
