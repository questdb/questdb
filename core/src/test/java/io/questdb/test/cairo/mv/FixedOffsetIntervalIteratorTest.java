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
import io.questdb.cairo.mv.SampleByIntervalIterator;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.groupby.TimestampSampler;
import io.questdb.griffin.engine.groupby.TimestampSamplerFactory;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.LongList;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

public class FixedOffsetIntervalIteratorTest extends AbstractIntervalIteratorTest {
    private static final Log LOG = LogFactory.getLog(FixedOffsetIntervalIteratorTest.class);

    @Test
    public void testBigStep() throws Exception {
        final FixedOffsetIntervalIterator iterator = new FixedOffsetIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(1, 'd', 0);
        iterator.of(
                sampler,
                0,
                null,
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
                null,
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
                null,
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
                null,
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
                null,
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
                null,
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
    public void testTxnIntervalSingle() throws Exception {
        final FixedOffsetIntervalIterator iterator = new FixedOffsetIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(1, 'd', 0);
        final LongList txnIntervals = new LongList();

        final long minTs = 0;
        final long maxTs = 3 * Timestamps.DAY_MICROS - 1;

        // match
        txnIntervals.add(minTs, maxTs);
        iterator.of(sampler, 0, txnIntervals, minTs, maxTs, 1);
        for (int i = 0; i < 3; i++) {
            Assert.assertTrue(iterator.next());
            Assert.assertEquals(i * Timestamps.DAY_MICROS, iterator.getTimestampLo());
            Assert.assertEquals((i + 1) * Timestamps.DAY_MICROS, iterator.getTimestampHi());
        }
        Assert.assertFalse(iterator.next());

        // to the left
        txnIntervals.clear();
        txnIntervals.add(-2L, -1L);
        iterator.of(sampler, 0, txnIntervals, minTs, maxTs, 1);
        Assert.assertFalse(iterator.next());

        // to the left with intersection
        txnIntervals.clear();
        txnIntervals.add(-2L, 0L);
        iterator.of(sampler, 0, txnIntervals, minTs, maxTs, 1);
        Assert.assertTrue(iterator.next());
        Assert.assertEquals(0, iterator.getTimestampLo());
        Assert.assertEquals(Timestamps.DAY_MICROS, iterator.getTimestampHi());
        Assert.assertFalse(iterator.next());

        // to the right
        txnIntervals.clear();
        txnIntervals.add(maxTs + 1, maxTs + 2);
        iterator.of(sampler, 0, txnIntervals, minTs, maxTs, 1);
        Assert.assertFalse(iterator.next());

        // to the right with intersection
        txnIntervals.clear();
        txnIntervals.add(maxTs, maxTs + 2);
        iterator.of(sampler, 0, txnIntervals, minTs, maxTs, 1);
        Assert.assertTrue(iterator.next());
        Assert.assertEquals(2 * Timestamps.DAY_MICROS, iterator.getTimestampLo());
        Assert.assertEquals(3 * Timestamps.DAY_MICROS, iterator.getTimestampHi());
        Assert.assertFalse(iterator.next());

        // middle
        txnIntervals.clear();
        txnIntervals.add(Timestamps.DAY_MICROS + 1, 2 * Timestamps.DAY_MICROS - 1);
        iterator.of(sampler, 0, txnIntervals, minTs, maxTs, 1);
        Assert.assertTrue(iterator.next());
        Assert.assertEquals(Timestamps.DAY_MICROS, iterator.getTimestampLo());
        Assert.assertEquals(2 * Timestamps.DAY_MICROS, iterator.getTimestampHi());
        Assert.assertFalse(iterator.next());
    }

    @Test
    public void testTxnIntervalToTop() throws Exception {
        final FixedOffsetIntervalIterator iterator = new FixedOffsetIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(1, 'd', 0);
        final LongList txnIntervals = new LongList();

        txnIntervals.add(0, Timestamps.DAY_MICROS - 1);
        iterator.of(sampler, 0, txnIntervals, 0, 3 * Timestamps.DAY_MICROS - 1, 1);

        Assert.assertTrue(iterator.next());
        Assert.assertEquals(0, iterator.getTimestampLo());
        Assert.assertEquals(Timestamps.DAY_MICROS, iterator.getTimestampHi());
        Assert.assertFalse(iterator.next());

        iterator.toTop(1);

        Assert.assertTrue(iterator.next());
        Assert.assertEquals(0, iterator.getTimestampLo());
        Assert.assertEquals(Timestamps.DAY_MICROS, iterator.getTimestampHi());
        Assert.assertFalse(iterator.next());
    }

    @Test
    public void testTxnIntervalsEmpty() throws Exception {
        final FixedOffsetIntervalIterator iterator = new FixedOffsetIntervalIterator();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(1, 'd', 0);
        iterator.of(
                sampler,
                0,
                new LongList(),
                0,
                7 * Timestamps.DAY_MICROS - 1,
                1
        );

        Assert.assertEquals(0, iterator.getMinTimestamp());
        Assert.assertEquals(7 * Timestamps.DAY_MICROS, iterator.getMaxTimestamp());

        Assert.assertFalse(iterator.next());
    }

    @Override
    protected SampleByIntervalIterator createIterator(
            TimestampSampler sampler,
            @Nullable TimeZoneRules ignored,
            long offset,
            @Nullable LongList txnIntervals,
            long minTs,
            long maxTs,
            int step
    ) {
        return new FixedOffsetIntervalIterator().of(
                sampler,
                offset,
                txnIntervals,
                minTs,
                maxTs,
                step
        );
    }
}
