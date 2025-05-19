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

import io.questdb.cairo.mv.SampleByIntervalIterator;
import io.questdb.griffin.engine.groupby.TimestampSampler;
import io.questdb.griffin.engine.groupby.TimestampSamplerFactory;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.LongList;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.std.datetime.microtime.Timestamps;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;

public abstract class AbstractIntervalIteratorTest {

    private static void intersectInPlace(LongList dest, long lo, long hi) {
        dest.add(lo, hi);
        IntervalUtils.intersectInPlace(dest, dest.size() - 2);
    }

    private static void subtractInPlace(LongList dest, long lo, long hi) {
        dest.add(lo, hi);
        IntervalUtils.subtract(dest, dest.size() - 2);
    }

    private static void unionInPlace(LongList dest, long lo, long hi) {
        dest.add(lo, hi);
        IntervalUtils.unionInPlace(dest, dest.size() - 2);
    }

    protected abstract SampleByIntervalIterator createIterator(
            TimestampSampler sampler,
            @Nullable TimeZoneRules tzRules,
            long offset,
            @Nullable LongList txnIntervals,
            long minTs,
            long maxTs,
            int step
    );

    protected void testFuzz(
            Rnd rnd,
            @Nullable TimeZoneRules tzRules,
            long offset,
            long start,
            long end
    ) throws Exception {
        final int step = Math.max(1, rnd.nextInt(1000));
        final int interval = Math.max(1, rnd.nextInt(300));

        final char[] timeUnits = new char[]{'m', 'h', 'd'};
        final char timeUnit = timeUnits[rnd.nextInt(timeUnits.length)];

        if (offset == Long.MIN_VALUE) {
            offset = (rnd.nextBoolean() ? 1 : -1) * rnd.nextLong(Timestamps.HOUR_MICROS);
        }

        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(interval, timeUnit, 0);

        LongList txnIntervals = null;
        if (rnd.nextBoolean()) {
            // generate a few txn min-max timestamp intervals
            final int count = 1 + rnd.nextInt(10);
            final long maxSize = Math.max((end - start) / count, 1);
            txnIntervals = new LongList(2 * count);
            long lo;
            long hi = start;
            for (int i = 0; i < count; i++) {
                lo = Math.min(end, hi + rnd.nextLong(maxSize));
                hi = Math.min(end, lo + rnd.nextLong(maxSize));
                unionInPlace(txnIntervals, lo, hi);
            }
            // txn interval must have start and end as the outer boundaries
            txnIntervals.setQuick(0, start);
            txnIntervals.setQuick(txnIntervals.size() - 1, end);
        }

        final SampleByIntervalIterator iterator = createIterator(
                sampler,
                tzRules,
                offset,
                txnIntervals,
                start,
                end,
                step
        );

        final long minTs = iterator.getMinTimestamp();
        final long maxTs = iterator.getMaxTimestamp();
        Assert.assertTrue(minTs < maxTs);

        LongList intervals = null;
        LongList remainingTxnIntervals = null;
        if (txnIntervals != null) {
            // used for intersection calculation, etc.
            intervals = new LongList(txnIntervals.capacity() + 2);
            // holds remaining txn intervals intersected with [min_ts,max_ts)
            remainingTxnIntervals = new LongList(txnIntervals);
            intersectInPlace(remainingTxnIntervals, minTs, maxTs - 1);
        }

        long minObservedTs = Long.MAX_VALUE;
        long maxObservedTs = Long.MIN_VALUE;
        long prevTsHi = Long.MIN_VALUE;
        while (iterator.next()) {
            final long lo = iterator.getTimestampLo();
            final long hi = iterator.getTimestampHi();
            Assert.assertTrue(lo < hi);
            if (txnIntervals != null) {
                // assert that the iterated bucket has an intersection with txn min-max timestamp intervals
                intervals.clear();
                intervals.addAll(txnIntervals);
                intersectInPlace(intervals, lo, hi - 1);
                Assert.assertTrue(intervals.size() > 0);
                // at this point, at least one interval should remain
                Assert.assertTrue(remainingTxnIntervals.size() > 0);
                // subtract the bucket from the remaining intervals
                subtractInPlace(remainingTxnIntervals, lo, hi - 1);
            } else {
                if (prevTsHi != Long.MIN_VALUE) {
                    Assert.assertEquals(prevTsHi, lo);
                }
                prevTsHi = hi;
            }
            minObservedTs = Math.min(minObservedTs, lo);
            maxObservedTs = Math.max(maxObservedTs, hi);
        }

        if (txnIntervals != null) {
            // we should have seen all buckets that intersect with txn intervals
            Assert.assertEquals(0, remainingTxnIntervals.size());
            Assert.assertTrue(minObservedTs >= minTs);
            Assert.assertTrue(maxObservedTs <= maxTs);
        } else {
            Assert.assertEquals(minTs, minObservedTs);
            Assert.assertEquals(maxTs, maxObservedTs);
        }
    }
}
