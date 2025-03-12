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

import io.questdb.cairo.mv.MatViewQueryIntervalIterator;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.groupby.TimestampSampler;
import io.questdb.griffin.engine.groupby.TimestampSamplerFactory;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.microtime.Timestamps;
import org.junit.Assert;
import org.junit.Test;

public class MatViewQueryIntervalIteratorTest {

    @Test
    public void testBigStep() throws Exception {
        final MatViewQueryIntervalIterator iterator = new MatViewQueryIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(1, 'd', 0);
        iterator.of(
                sampler,
                null,
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
        final MatViewQueryIntervalIterator iterator = new MatViewQueryIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(1, 'd', 0);
        final long offset = Timestamps.HOUR_MICROS;
        iterator.of(
                sampler,
                null,
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
    public void testSmoke() throws SqlException {
        final MatViewQueryIntervalIterator iterator = new MatViewQueryIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(1, 'd', 0);
        iterator.of(
                sampler,
                null,
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

    @Test
    public void testTimeZoneWithDst() throws Exception {
        final MatViewQueryIntervalIterator iterator = new MatViewQueryIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(2, 'h', 0);
        iterator.of(
                sampler,
                Timestamps.getTimezoneRules(TimestampFormatUtils.EN_LOCALE, "Europe/Berlin"),
                0,
                TimestampFormatUtils.parseTimestamp("2021-03-28T00:59:00.000000Z"),
                TimestampFormatUtils.parseTimestamp("2021-03-28T08:01:00.000000Z"),
                2
        );

        Assert.assertEquals(TimestampFormatUtils.parseTimestamp("2021-03-27T23:00:00.000000Z"), iterator.getMinTimestamp());
        Assert.assertEquals(TimestampFormatUtils.parseTimestamp("2021-03-28T10:00:00.000000Z"), iterator.getMaxTimestamp());

        // DST edge is here
        Assert.assertTrue(iterator.next());
        Assert.assertEquals(TimestampFormatUtils.parseTimestamp("2021-03-27T23:00:00.000000Z"), iterator.getTimestampLo());
        Assert.assertEquals(TimestampFormatUtils.parseTimestamp("2021-03-28T02:00:00.000000Z"), iterator.getTimestampHi());

        Assert.assertTrue(iterator.next());
        Assert.assertEquals(TimestampFormatUtils.parseTimestamp("2021-03-28T02:00:00.000000Z"), iterator.getTimestampLo());
        Assert.assertEquals(TimestampFormatUtils.parseTimestamp("2021-03-28T06:00:00.000000Z"), iterator.getTimestampHi());

        Assert.assertTrue(iterator.next());
        Assert.assertEquals(TimestampFormatUtils.parseTimestamp("2021-03-28T06:00:00.000000Z"), iterator.getTimestampLo());
        Assert.assertEquals(TimestampFormatUtils.parseTimestamp("2021-03-28T10:00:00.000000Z"), iterator.getTimestampHi());

        Assert.assertFalse(iterator.next());
    }

    @Test
    public void testTimeZoneWithFixedOffset() throws Exception {
        final MatViewQueryIntervalIterator iterator = new MatViewQueryIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(1, 'd', 0);
        iterator.of(
                sampler,
                Timestamps.getTimezoneRules(TimestampFormatUtils.EN_LOCALE, "GMT+00:30"),
                0,
                TimestampFormatUtils.parseTimestamp("2024-01-01T01:01:00.000000Z"),
                TimestampFormatUtils.parseTimestamp("2024-01-03T07:00:01.000000Z"),
                1
        );

        final long minExpectedTs = TimestampFormatUtils.parseTimestamp("2024-01-01T00:00:00.000000Z");
        final long maxExpectedTs = TimestampFormatUtils.parseTimestamp("2024-01-04T00:00:00.000000Z");

        final long tzOffset = 30 * Timestamps.MINUTE_MICROS;

        Assert.assertEquals(minExpectedTs - tzOffset, iterator.getMinTimestamp());
        Assert.assertEquals(maxExpectedTs - tzOffset, iterator.getMaxTimestamp());

        for (int i = 0; i < 3; i++) {
            Assert.assertTrue(iterator.next());
            Assert.assertEquals(minExpectedTs - tzOffset + i * Timestamps.DAY_MICROS, iterator.getTimestampLo());
            Assert.assertEquals(minExpectedTs - tzOffset + (i + 1) * Timestamps.DAY_MICROS, iterator.getTimestampHi());
        }
        Assert.assertFalse(iterator.next());
    }
}
