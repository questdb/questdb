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

package io.questdb.test.cairo.mv;

import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.NanosTimestampDriver;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.mv.SampleByIntervalIterator;
import io.questdb.cairo.mv.TimeZoneIntervalIterator;
import io.questdb.griffin.engine.groupby.TimestampSampler;
import io.questdb.griffin.engine.groupby.TimestampSamplerFactory;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.LongList;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.DateLocaleFactory;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class TimeZoneIntervalIteratorTest extends AbstractIntervalIteratorTest {
    private static final Log LOG = LogFactory.getLog(TimeZoneIntervalIteratorTest.class);

    public TimeZoneIntervalIteratorTest(TimestampDriver timestampDriver) {
        super(timestampDriver);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {MicrosTimestampDriver.INSTANCE}, {NanosTimestampDriver.INSTANCE}
        });
    }

    @Test
    public void testBigStep() throws Exception {
        final TimeZoneIntervalIterator iterator = new TimeZoneIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(timestampDriver, 1, 'd', 0);
        iterator.of(
                timestampDriver,
                sampler,
                timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, "UTC"),
                0,
                null,
                timestampDriver.parseFloorLiteral("2024-03-03T01:01:00.000000Z"),
                timestampDriver.parseFloorLiteral("2024-03-04T01:01:00.000000Z"),
                14
        );

        Assert.assertEquals(timestampDriver.parseFloorLiteral("2024-03-03T00:00:00.000000Z"), iterator.getMinTimestamp());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2024-03-05T00:00:00.000000Z"), iterator.getMaxTimestamp());

        Assert.assertTrue(iterator.next());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2024-03-03T00:00:00.000000Z"), iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2024-03-05T00:00:00.000000Z"), iterator.getTimestampHi());
        Assert.assertFalse(iterator.next());
    }

    @Test
    public void testFixedOffset() throws Exception {
        final TimeZoneIntervalIterator iterator = new TimeZoneIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(timestampDriver, 1, 'd', 0);
        final long offset = timestampDriver.fromHours(1);
        iterator.of(
                timestampDriver,
                sampler,
                timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, "UTC"),
                offset,
                null,
                0,
                timestampDriver.fromDays(7) - 1,
                1
        );

        Assert.assertEquals(offset, iterator.getMinTimestamp());
        Assert.assertEquals(offset + timestampDriver.fromDays(7), iterator.getMaxTimestamp());

        for (int i = 0; i < 7; i++) {
            Assert.assertTrue(iterator.next());
            Assert.assertEquals(offset + timestampDriver.fromDays(i), iterator.getTimestampLo());
            Assert.assertEquals(offset + timestampDriver.fromDays(i + 1), iterator.getTimestampHi());
        }
        Assert.assertFalse(iterator.next());
    }

    @Test
    public void testFuzzTimeZoneWithDst() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        testFuzz(
                rnd,
                timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, "Europe/Berlin"),
                Long.MIN_VALUE,
                timestampDriver.parseFloorLiteral("2021-03-26T10:03:00.000000Z"),
                timestampDriver.parseFloorLiteral("2021-03-29T12:01:00.000000Z")
        );
    }

    @Test
    public void testFuzzTimeZoneWithDstLargeInterval() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        testFuzz(
                rnd,
                timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, "Europe/Berlin"),
                Long.MIN_VALUE,
                timestampDriver.parseFloorLiteral("2020-01-01T00:00:00.000000Z"),
                timestampDriver.parseFloorLiteral("2030-01-01T00:00:00.000000Z")
        );
    }

    @Test
    public void testFuzzTimeZoneWithFixedOffset() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        testFuzz(
                rnd,
                timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, "GMT+2"),
                Long.MIN_VALUE,
                timestampDriver.parseFloorLiteral("2000-01-01T23:10:00.000000Z"),
                timestampDriver.parseFloorLiteral("2000-01-02T20:59:59.000000Z")
        );
    }

    @Test
    public void testFuzzUTC() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        testFuzz(
                rnd,
                timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, "UTC"),
                Long.MIN_VALUE,
                timestampDriver.parseFloorLiteral("2024-03-03T12:01:01.000000Z"),
                timestampDriver.parseFloorLiteral("2024-03-07T12:01:01.000000Z")
        );
    }

    @Test
    public void testIntervalsEmpty() throws Exception {
        final TimeZoneIntervalIterator iterator = new TimeZoneIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(timestampDriver, 1, 'd', 0);
        iterator.of(
                timestampDriver,
                sampler,
                timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, "UTC"),
                0,
                new LongList(),
                0,
                timestampDriver.fromDays(7) - 1,
                1
        );

        Assert.assertEquals(0, iterator.getMinTimestamp());
        Assert.assertEquals(timestampDriver.fromDays(7), iterator.getMaxTimestamp());

        Assert.assertFalse(iterator.next());
    }

    @Test
    public void testIntervalsSingle() throws Exception {
        final TimeZoneIntervalIterator iterator = new TimeZoneIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(timestampDriver, 1, 'd', 0);
        final TimeZoneRules tzRules = timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, "UTC");
        final LongList intervals = new LongList();

        final long minTs = 0;
        final long maxTs = timestampDriver.fromDays(3) - 1;

        // match
        intervals.add(minTs, maxTs);
        iterator.of(timestampDriver, sampler, tzRules, 0, intervals, minTs, maxTs, 1);
        for (int i = 0; i < 3; i++) {
            Assert.assertTrue(iterator.next());
            Assert.assertEquals(timestampDriver.fromDays(i), iterator.getTimestampLo());
            Assert.assertEquals(timestampDriver.fromDays(i + 1), iterator.getTimestampHi());
        }
        Assert.assertFalse(iterator.next());

        // to the left
        intervals.clear();
        intervals.add(-2L, -1L);
        iterator.of(timestampDriver, sampler, tzRules, 0, intervals, minTs, maxTs, 1);
        Assert.assertFalse(iterator.next());

        // to the left with intersection
        intervals.clear();
        intervals.add(-2L, 0L);
        iterator.of(timestampDriver, sampler, tzRules, 0, intervals, minTs, maxTs, 1);
        Assert.assertTrue(iterator.next());
        Assert.assertEquals(0, iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.fromDays(1), iterator.getTimestampHi());
        Assert.assertFalse(iterator.next());

        // to the right
        intervals.clear();
        intervals.add(maxTs + 1, maxTs + 2);
        iterator.of(timestampDriver, sampler, tzRules, 0, intervals, minTs, maxTs, 1);
        Assert.assertFalse(iterator.next());

        // to the right with intersection
        intervals.clear();
        intervals.add(maxTs, maxTs + 2);
        iterator.of(timestampDriver, sampler, tzRules, 0, intervals, minTs, maxTs, 1);
        Assert.assertTrue(iterator.next());
        Assert.assertEquals(timestampDriver.fromDays(2), iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.fromDays(3), iterator.getTimestampHi());
        Assert.assertFalse(iterator.next());

        // middle
        intervals.clear();
        intervals.add(timestampDriver.fromDays(1) + 1, timestampDriver.fromDays(2) - 1);
        iterator.of(timestampDriver, sampler, tzRules, 0, intervals, minTs, maxTs, 1);
        Assert.assertTrue(iterator.next());
        Assert.assertEquals(timestampDriver.fromDays(1), iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.fromDays(2), iterator.getTimestampHi());
        Assert.assertFalse(iterator.next());
    }

    @Test
    public void testIntervalsToTop() throws Exception {
        final TimeZoneIntervalIterator iterator = new TimeZoneIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(timestampDriver, 1, 'd', 0);
        final TimeZoneRules tzRules = timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, "UTC");
        final LongList intervals = new LongList();

        intervals.add(0, timestampDriver.fromDays(1) - 1);
        iterator.of(timestampDriver, sampler, tzRules, 0, intervals, 0, timestampDriver.fromDays(3) - 1, 1);

        Assert.assertTrue(iterator.next());
        Assert.assertEquals(0, iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.fromDays(1), iterator.getTimestampHi());
        Assert.assertFalse(iterator.next());

        iterator.toTop(1);

        Assert.assertTrue(iterator.next());
        Assert.assertEquals(0, iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.fromDays(1), iterator.getTimestampHi());
        Assert.assertFalse(iterator.next());
    }

    @Test
    public void testSmoke() throws Exception {
        final TimeZoneIntervalIterator iterator = new TimeZoneIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(timestampDriver, 1, 'd', 0);
        iterator.of(
                timestampDriver,
                sampler,
                timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, "UTC"),
                0,
                null,
                0,
                timestampDriver.fromDays(7) - 1,
                1
        );

        Assert.assertEquals(0, iterator.getMinTimestamp());
        Assert.assertEquals(timestampDriver.fromDays(7), iterator.getMaxTimestamp());

        for (int i = 0; i < 7; i++) {
            Assert.assertTrue(iterator.next());
            Assert.assertEquals(timestampDriver.fromDays(i), iterator.getTimestampLo());
            Assert.assertEquals(timestampDriver.fromDays(i + 1), iterator.getTimestampHi());
        }
        Assert.assertFalse(iterator.next());
    }

    @Test
    public void testTimeZoneWithDst() throws Exception {
        final TimeZoneIntervalIterator iterator = new TimeZoneIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(timestampDriver, 2, 'h', 0);
        iterator.of(
                timestampDriver,
                sampler,
                timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, "Europe/Berlin"),
                0,
                null,
                timestampDriver.parseFloorLiteral("2021-03-28T00:59:00.000000Z"),
                timestampDriver.parseFloorLiteral("2021-03-28T08:01:00.000000Z"),
                2
        );

        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-27T23:00:00.000000Z"), iterator.getMinTimestamp());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-28T10:00:00.000000Z"), iterator.getMaxTimestamp());

        // DST edge is here
        Assert.assertTrue(iterator.next());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-27T23:00:00.000000Z"), iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-28T02:00:00.000000Z"), iterator.getTimestampHi());

        Assert.assertTrue(iterator.next());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-28T02:00:00.000000Z"), iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-28T06:00:00.000000Z"), iterator.getTimestampHi());

        Assert.assertTrue(iterator.next());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-28T06:00:00.000000Z"), iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-28T10:00:00.000000Z"), iterator.getTimestampHi());

        Assert.assertFalse(iterator.next());
    }

    @Test
    public void testTimeZoneWithDstForwardShiftWithIntervalLargerThanShift() throws Exception {
        final TimeZoneIntervalIterator iterator = new TimeZoneIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(timestampDriver, 75, 'm', 0);
        iterator.of(
                timestampDriver,
                sampler,
                timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, "Europe/Berlin"),
                0,
                null,
                timestampDriver.parseFloorLiteral("2021-03-28T00:01:00.000000Z"), // 01:01 local time (GMT+1)
                timestampDriver.parseFloorLiteral("2021-03-28T02:52:00.000000Z"), // 04:52 local time (GMT+2)
                1
        );

        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-27T23:15:00.000000Z"), iterator.getMinTimestamp());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-28T03:15:00.000000Z"), iterator.getMaxTimestamp());

        Assert.assertTrue(iterator.next());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-27T23:15:00.000000Z"), iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-28T00:30:00.000000Z"), iterator.getTimestampHi());

        // DST edge is here (02:00 -> 03:00 local time)
        Assert.assertTrue(iterator.next());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-28T00:30:00.000000Z"), iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-28T02:00:00.000000Z"), iterator.getTimestampHi());

        Assert.assertTrue(iterator.next());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-28T02:00:00.000000Z"), iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-28T03:15:00.000000Z"), iterator.getTimestampHi());

        Assert.assertFalse(iterator.next());
    }

    @Test
    public void testTimeZoneWithDstShiftBackwardWithSmallInterval1() throws Exception {
        final TimeZoneIntervalIterator iterator = new TimeZoneIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(timestampDriver, 15, 'm', 0);
        iterator.of(
                timestampDriver,
                sampler,
                timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, "Europe/Berlin"),
                0,
                null,
                timestampDriver.parseFloorLiteral("2021-10-31T00:51:00.000000Z"),
                timestampDriver.parseFloorLiteral("2021-10-31T01:41:00.000000Z"),
                1
        );

        // both min and max timestamps must be aligned at the backward shift, not at 00:45-01:45
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-10-31T00:00:00.000000Z"), iterator.getMinTimestamp());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-10-31T02:00:00.000000Z"), iterator.getMaxTimestamp());

        Assert.assertTrue(iterator.next());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-10-31T00:00:00.000000Z"), iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-10-31T02:00:00.000000Z"), iterator.getTimestampHi());

        Assert.assertFalse(iterator.next());
    }

    @Test
    public void testTimeZoneWithDstShiftBackwardWithSmallInterval2() throws Exception {
        final TimeZoneIntervalIterator iterator = new TimeZoneIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(timestampDriver, 15, 'm', 0);
        iterator.of(
                timestampDriver,
                sampler,
                timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, "Europe/Berlin"),
                0,
                null,
                timestampDriver.parseFloorLiteral("2021-10-30T23:30:00.000000Z"),
                timestampDriver.parseFloorLiteral("2021-10-31T02:40:00.000000Z"),
                1
        );

        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-10-30T23:30:00.000000Z"), iterator.getMinTimestamp());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-10-31T02:45:00.000000Z"), iterator.getMaxTimestamp());

        Assert.assertTrue(iterator.next());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-10-30T23:30:00.000000Z"), iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-10-30T23:45:00.000000Z"), iterator.getTimestampHi());

        Assert.assertTrue(iterator.next());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-10-30T23:45:00.000000Z"), iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-10-31T00:00:00.000000Z"), iterator.getTimestampHi());

        Assert.assertTrue(iterator.next());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-10-31T00:00:00.000000Z"), iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-10-31T02:00:00.000000Z"), iterator.getTimestampHi());

        Assert.assertTrue(iterator.next());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-10-31T02:00:00.000000Z"), iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-10-31T02:15:00.000000Z"), iterator.getTimestampHi());

        Assert.assertTrue(iterator.next());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-10-31T02:15:00.000000Z"), iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-10-31T02:30:00.000000Z"), iterator.getTimestampHi());

        Assert.assertTrue(iterator.next());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-10-31T02:30:00.000000Z"), iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-10-31T02:45:00.000000Z"), iterator.getTimestampHi());

        Assert.assertFalse(iterator.next());
    }

    @Test
    public void testTimeZoneWithDstShiftForwardWithSmallInterval() throws Exception {
        final TimeZoneIntervalIterator iterator = new TimeZoneIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(timestampDriver, 30, 'm', 0);
        iterator.of(
                timestampDriver,
                sampler,
                timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, "Europe/Berlin"),
                0,
                null,
                timestampDriver.parseFloorLiteral("2021-03-28T00:01:00.000000Z"),
                timestampDriver.parseFloorLiteral("2021-03-28T02:52:00.000000Z"),
                1
        );

        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-28T00:00:00.000000Z"), iterator.getMinTimestamp());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-28T03:00:00.000000Z"), iterator.getMaxTimestamp());

        Assert.assertTrue(iterator.next());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-28T00:00:00.000000Z"), iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-28T00:30:00.000000Z"), iterator.getTimestampHi());

        Assert.assertTrue(iterator.next());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-28T00:30:00.000000Z"), iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-28T01:00:00.000000Z"), iterator.getTimestampHi());

        // DST edge is here
        Assert.assertTrue(iterator.next());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-28T01:00:00.000000Z"), iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-28T01:30:00.000000Z"), iterator.getTimestampHi());

        Assert.assertTrue(iterator.next());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-28T01:30:00.000000Z"), iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-28T02:00:00.000000Z"), iterator.getTimestampHi());

        Assert.assertTrue(iterator.next());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-28T02:00:00.000000Z"), iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-28T02:30:00.000000Z"), iterator.getTimestampHi());

        Assert.assertTrue(iterator.next());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-28T02:30:00.000000Z"), iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2021-03-28T03:00:00.000000Z"), iterator.getTimestampHi());

        Assert.assertFalse(iterator.next());
    }

    @Test
    public void testTimeZoneWithDstSingleRow1() throws Exception {
        final TimeZoneIntervalIterator iterator = new TimeZoneIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(timestampDriver, 30, 'm', 0);
        iterator.of(
                timestampDriver,
                sampler,
                timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, "Europe/London"),
                0,
                null,
                timestampDriver.parseFloorLiteral("2024-10-27T01:00:00.600000Z"),
                timestampDriver.parseFloorLiteral("2024-10-27T01:00:00.600000Z"),
                2
        );

        Assert.assertEquals(timestampDriver.parseFloorLiteral("2024-10-27T00:00:00.000000Z"), iterator.getMinTimestamp());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2024-10-27T02:00:00.000000Z"), iterator.getMaxTimestamp());

        Assert.assertTrue(iterator.next());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2024-10-27T00:00:00.000000Z"), iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2024-10-27T02:00:00.000000Z"), iterator.getTimestampHi());

        Assert.assertFalse(iterator.next());
    }

    @Test
    public void testTimeZoneWithDstSingleRow2() throws Exception {
        final TimeZoneIntervalIterator iterator = new TimeZoneIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(timestampDriver, 30, 'm', 0);
        iterator.of(
                timestampDriver,
                sampler,
                timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, "Europe/London"),
                0,
                null,
                timestampDriver.parseFloorLiteral("2024-03-31T00:30:06.000000Z"),
                timestampDriver.parseFloorLiteral("2024-03-31T00:30:06.000000Z"),
                2
        );

        Assert.assertEquals(timestampDriver.parseFloorLiteral("2024-03-31T00:30:00.000000Z"), iterator.getMinTimestamp());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2024-03-31T01:00:00.000000Z"), iterator.getMaxTimestamp());

        Assert.assertTrue(iterator.next());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2024-03-31T00:30:00.000000Z"), iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.parseFloorLiteral("2024-03-31T01:00:00.000000Z"), iterator.getTimestampHi());

        Assert.assertFalse(iterator.next());
    }

    @Test
    public void testTimeZoneWithFixedOffset() throws Exception {
        final TimeZoneIntervalIterator iterator = new TimeZoneIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(timestampDriver, 1, 'd', 0);
        iterator.of(
                timestampDriver,
                sampler,
                timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, "GMT+00:30"),
                0,
                null,
                timestampDriver.parseFloorLiteral("2024-01-01T01:01:00.000000Z"),
                timestampDriver.parseFloorLiteral("2024-01-03T07:00:01.000000Z"),
                1
        );

        final long minExpectedTs = timestampDriver.parseFloorLiteral("2024-01-01T00:00:00.000000Z");
        final long maxExpectedTs = timestampDriver.parseFloorLiteral("2024-01-04T00:00:00.000000Z");

        final long tzOffset = timestampDriver.fromMinutes(30);

        Assert.assertEquals(minExpectedTs - tzOffset, iterator.getMinTimestamp());
        Assert.assertEquals(maxExpectedTs - tzOffset, iterator.getMaxTimestamp());

        for (int i = 0; i < 3; i++) {
            Assert.assertTrue(iterator.next());
            Assert.assertEquals(minExpectedTs - tzOffset + timestampDriver.fromDays(i), iterator.getTimestampLo());
            Assert.assertEquals(minExpectedTs - tzOffset + timestampDriver.fromDays(i + 1), iterator.getTimestampHi());
        }
        Assert.assertFalse(iterator.next());
    }

    @Override
    protected SampleByIntervalIterator createIterator(
            TimestampSampler sampler,
            @Nullable TimeZoneRules tzRules,
            long offset,
            @Nullable LongList intervals,
            long minTs,
            long maxTs,
            int step
    ) {
        Assert.assertNotNull(tzRules);
        return new TimeZoneIntervalIterator().of(
                timestampDriver,
                sampler,
                tzRules,
                offset,
                intervals,
                minTs,
                maxTs,
                step
        );
    }
}
