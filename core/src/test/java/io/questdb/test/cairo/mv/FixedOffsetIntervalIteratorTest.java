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

import io.questdb.cairo.mv.FixedOffsetIntervalIterator;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.groupby.TimestampSampler;
import io.questdb.griffin.engine.groupby.TimestampSamplerFactory;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class FixedOffsetIntervalIteratorTest {
    private static final Log LOG = LogFactory.getLog(FixedOffsetIntervalIteratorTest.class);

    @Test
    public void testBigStep() throws Exception {
        final FixedOffsetIntervalIterator iterator = new FixedOffsetIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(1, 'd', 0);
        iterator.of(
                sampler,
                0,
                TimestampFormatUtils.parseTimestamp("2024-03-03T01:01:00.000000Z"),
                TimestampFormatUtils.parseTimestamp("2024-03-04T01:01:00.000000Z"),
                14
        );

        Assert.assertEquals(TimestampFormatUtils.parseTimestamp("2024-03-03T00:00:00.000000Z"), iterator.getMinTimestamp());
        Assert.assertEquals(TimestampFormatUtils.parseTimestamp("2024-03-05T00:00:00.000000Z"), iterator.getMaxTimestamp());

        Assert.assertTrue(iterator.next());
        Assert.assertEquals(TimestampFormatUtils.parseTimestamp("2024-03-03T00:00:00.000000Z"), iterator.getTimestampLo());
        Assert.assertEquals(TimestampFormatUtils.parseTimestamp("2024-03-05T00:00:00.000000Z"), iterator.getTimestampHi());
        Assert.assertFalse(iterator.next());
    }

    @Test
    public void testFixedOffset() throws SqlException {
        final FixedOffsetIntervalIterator iterator = new FixedOffsetIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(1, 'd', 0);
        final long offset = Timestamps.HOUR_MICROS;
        iterator.of(
                sampler,
                offset,
                0,
                7 * Timestamps.DAY_MICROS - 1,
                1
        );

        Assert.assertEquals(offset, iterator.getMinTimestamp());
        Assert.assertEquals(offset + 7 * Timestamps.DAY_MICROS, iterator.getMaxTimestamp());

        for (int i = 0; i < 7; i++) {
            Assert.assertTrue(iterator.next());
            Assert.assertEquals(offset + i * Timestamps.DAY_MICROS, iterator.getTimestampLo());
            Assert.assertEquals(offset + (i + 1) * Timestamps.DAY_MICROS, iterator.getTimestampHi());
        }
        Assert.assertFalse(iterator.next());
    }

    @Test
    public void testFuzzFixedOffsetHour() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        testFuzz(
                rnd,
                (rnd.nextLong() % 24) * Timestamps.HOUR_MICROS,
                TimestampFormatUtils.parseTimestamp("2000-01-01T23:10:00.000000Z"),
                TimestampFormatUtils.parseTimestamp("2000-01-02T20:59:59.000000Z")
        );
    }

    @Test
    public void testFuzzFixedOffsetMinute() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        testFuzz(
                rnd,
                (rnd.nextLong() % 120) * Timestamps.MINUTE_MICROS,
                TimestampFormatUtils.parseTimestamp("2000-01-01T23:10:00.000000Z"),
                TimestampFormatUtils.parseTimestamp("2000-01-02T20:59:59.000000Z")
        );
    }

    @Test
    public void testFuzzZeroOffset() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        testFuzz(
                rnd,
                0,
                TimestampFormatUtils.parseTimestamp("2024-03-03T12:01:01.000000Z"),
                TimestampFormatUtils.parseTimestamp("2024-03-07T12:01:01.000000Z")
        );
    }

    @Test
    public void testSmoke() throws SqlException {
        final FixedOffsetIntervalIterator iterator = new FixedOffsetIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(1, 'd', 0);
        iterator.of(
                sampler,
                0,
                0,
                7 * Timestamps.DAY_MICROS - 1,
                1
        );

        Assert.assertEquals(0, iterator.getMinTimestamp());
        Assert.assertEquals(7 * Timestamps.DAY_MICROS, iterator.getMaxTimestamp());

        for (int i = 0; i < 7; i++) {
            Assert.assertTrue(iterator.next());
            Assert.assertEquals(i * Timestamps.DAY_MICROS, iterator.getTimestampLo());
            Assert.assertEquals((i + 1) * Timestamps.DAY_MICROS, iterator.getTimestampHi());
        }
        Assert.assertFalse(iterator.next());
    }

    private void testFuzz(
            Rnd rnd,
            long offset,
            long start,
            long end
    ) throws Exception {
        final int step = Math.max(1, rnd.nextInt(1000));
        final int interval = Math.max(1, rnd.nextInt(300));

        final char[] timeUnits = new char[]{'m', 'h', 'd'};
        final char timeUnit = timeUnits[rnd.nextInt(timeUnits.length)];

        final FixedOffsetIntervalIterator iterator = new FixedOffsetIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(interval, timeUnit, 0);
        iterator.of(
                sampler,
                offset,
                start,
                end,
                step
        );

        final long minTs = iterator.getMinTimestamp();
        final long maxTs = iterator.getMaxTimestamp();
        Assert.assertTrue(minTs < maxTs);

        long minObservedTs = Long.MAX_VALUE;
        long maxObservedTs = Long.MIN_VALUE;
        long prevTsHi = Long.MIN_VALUE;
        while (iterator.next()) {
            final long tsLo = iterator.getTimestampLo();
            final long tsHi = iterator.getTimestampHi();
            Assert.assertTrue(tsLo < tsHi);
            if (prevTsHi != Long.MIN_VALUE) {
                Assert.assertEquals(prevTsHi, tsLo);
            }
            prevTsHi = tsHi;
            minObservedTs = Math.min(minObservedTs, tsLo);
            maxObservedTs = Math.max(maxObservedTs, tsHi);
        }

        Assert.assertEquals(minTs, minObservedTs);
        Assert.assertEquals(maxTs, maxObservedTs);
    }
}
