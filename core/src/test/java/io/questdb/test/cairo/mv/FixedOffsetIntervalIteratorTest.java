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
import io.questdb.cairo.mv.FixedOffsetIntervalIterator;
import io.questdb.cairo.mv.SampleByIntervalIterator;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.groupby.TimestampSampler;
import io.questdb.griffin.engine.groupby.TimestampSamplerFactory;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.LongList;
import io.questdb.std.Rnd;
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
public class FixedOffsetIntervalIteratorTest extends AbstractIntervalIteratorTest {
    private static final Log LOG = LogFactory.getLog(FixedOffsetIntervalIteratorTest.class);

    public FixedOffsetIntervalIteratorTest(TimestampDriver timestampDriver) {
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
        final FixedOffsetIntervalIterator iterator = new FixedOffsetIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(timestampDriver, 1, 'd', 0);
        iterator.of(
                sampler,
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
    public void testFixedOffset() throws SqlException {
        final FixedOffsetIntervalIterator iterator = new FixedOffsetIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(timestampDriver, 1, 'd', 0);
        final long offset = timestampDriver.fromHours(1);
        iterator.of(
                sampler,
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
    public void testFuzzFixedOffsetHour() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        testFuzz(
                rnd,
                null,
                timestampDriver.fromDays((int) (rnd.nextLong() % 24)),
                timestampDriver.parseFloorLiteral("2000-01-01T23:10:00.000000Z"),
                timestampDriver.parseFloorLiteral("2000-01-02T20:59:59.000000Z")
        );
    }

    @Test
    public void testFuzzFixedOffsetMinute() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        testFuzz(
                rnd,
                null,
                timestampDriver.fromMinutes((int) (rnd.nextLong() % 120)),
                timestampDriver.parseFloorLiteral("2000-01-01T23:10:00.000000Z"),
                timestampDriver.parseFloorLiteral("2000-01-02T20:59:59.000000Z")
        );
    }

    @Test
    public void testFuzzZeroOffset() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        testFuzz(
                rnd,
                null,
                0,
                timestampDriver.parseFloorLiteral("2024-03-03T12:01:01.000000Z"),
                timestampDriver.parseFloorLiteral("2024-03-07T12:01:01.000000Z")
        );
    }

    @Test
    public void testIntervalSingle() throws Exception {
        final FixedOffsetIntervalIterator iterator = new FixedOffsetIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(timestampDriver, 1, 'd', 0);
        final LongList intervals = new LongList();

        final long minTs = 0;
        final long maxTs = timestampDriver.fromDays(3) - 1;

        // match
        intervals.add(minTs, maxTs);
        iterator.of(sampler, 0, intervals, minTs, maxTs, 1);
        for (int i = 0; i < 3; i++) {
            Assert.assertTrue(iterator.next());
            Assert.assertEquals(timestampDriver.fromDays(i), iterator.getTimestampLo());
            Assert.assertEquals(timestampDriver.fromDays(i + 1), iterator.getTimestampHi());
        }
        Assert.assertFalse(iterator.next());

        // to the left
        intervals.clear();
        intervals.add(-2L, -1L);
        iterator.of(sampler, 0, intervals, minTs, maxTs, 1);
        Assert.assertFalse(iterator.next());

        // to the left with intersection
        intervals.clear();
        intervals.add(-2L, 0L);
        iterator.of(sampler, 0, intervals, minTs, maxTs, 1);
        Assert.assertTrue(iterator.next());
        Assert.assertEquals(0, iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.fromDays(1), iterator.getTimestampHi());
        Assert.assertFalse(iterator.next());

        // to the right
        intervals.clear();
        intervals.add(maxTs + 1, maxTs + 2);
        iterator.of(sampler, 0, intervals, minTs, maxTs, 1);
        Assert.assertFalse(iterator.next());

        // to the right with intersection
        intervals.clear();
        intervals.add(maxTs, maxTs + 2);
        iterator.of(sampler, 0, intervals, minTs, maxTs, 1);
        Assert.assertTrue(iterator.next());
        Assert.assertEquals(timestampDriver.fromDays(2), iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.fromDays(3), iterator.getTimestampHi());
        Assert.assertFalse(iterator.next());

        // middle
        intervals.clear();
        intervals.add(timestampDriver.fromDays(1) + 1, timestampDriver.fromDays(2) - 1);
        iterator.of(sampler, 0, intervals, minTs, maxTs, 1);
        Assert.assertTrue(iterator.next());
        Assert.assertEquals(timestampDriver.fromDays(1), iterator.getTimestampLo());
        Assert.assertEquals(timestampDriver.fromDays(2), iterator.getTimestampHi());
        Assert.assertFalse(iterator.next());
    }

    @Test
    public void testIntervalsEmpty() throws Exception {
        final FixedOffsetIntervalIterator iterator = new FixedOffsetIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(timestampDriver, 1, 'd', 0);
        iterator.of(
                sampler,
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
    public void testIntervalsToTop() throws Exception {
        final FixedOffsetIntervalIterator iterator = new FixedOffsetIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(timestampDriver, 1, 'd', 0);
        final LongList intervals = new LongList();

        intervals.add(0, timestampDriver.fromDays(1) - 1);
        iterator.of(sampler, 0, intervals, 0, timestampDriver.fromDays(3) - 1, 1);

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
    public void testSmoke() throws SqlException {
        final FixedOffsetIntervalIterator iterator = new FixedOffsetIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(timestampDriver, 1, 'd', 0);
        iterator.of(
                sampler,
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

    @Override
    protected SampleByIntervalIterator createIterator(
            TimestampSampler sampler,
            @Nullable TimeZoneRules ignored,
            long offset,
            @Nullable LongList intervals,
            long minTs,
            long maxTs,
            int step
    ) {
        return new FixedOffsetIntervalIterator().of(
                sampler,
                offset,
                intervals,
                minTs,
                maxTs,
                step
        );
    }
}
