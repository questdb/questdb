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

package io.questdb.test.cairo.mv;

import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.mv.SampleByIntervalIterator;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.groupby.TimestampSampler;
import io.questdb.griffin.engine.groupby.TimestampSamplerFactory;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.LongList;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;

public abstract class AbstractIntervalIteratorTest {
    protected final TimestampDriver timestampDriver;

    public AbstractIntervalIteratorTest(TimestampDriver timestampDriver) {
        this.timestampDriver = timestampDriver;
    }

    // Asserts the iterator's step-groups as [lo0, hi0, lo1, hi1, ...]. Unlike assertSteps, this allows gaps between
    // consecutive step-groups, so it fits iterations whose clipped intervals leave gaps.
    protected static void assertStepGroups(SampleByIntervalIterator iterator, LongList expectedLoHiPairs) {
        final LongList actual = new LongList();
        while (iterator.next()) {
            final long lo = iterator.getTimestampLo();
            final long hi = iterator.getTimestampHi();
            Assert.assertTrue("step-group lo must be below its hi", lo < hi);
            actual.add(lo, hi);
        }
        TestUtils.assertEquals(expectedLoHiPairs, actual);
    }

    // Asserts the steps of an iterator walk: expectedBounds holds the lower bound of each step, followed by the upper
    // bound of the last step.
    protected static void assertSteps(SampleByIntervalIterator iterator, LongList expectedBounds) {
        final int expectedStepCount = expectedBounds.size() - 1;
        Assert.assertEquals(expectedBounds.getQuick(0), iterator.getMinTimestamp());
        Assert.assertEquals(expectedBounds.getQuick(expectedStepCount), iterator.getMaxTimestamp());
        int stepCount = 0;
        while (iterator.next()) {
            // fail on the first surplus step, so that an iterator that stops advancing fails instead of hanging
            Assert.assertTrue("unexpected step " + stepCount, stepCount < expectedStepCount);
            Assert.assertEquals(expectedBounds.getQuick(stepCount), iterator.getTimestampLo());
            Assert.assertEquals(expectedBounds.getQuick(stepCount + 1), iterator.getTimestampHi());
            stepCount++;
        }
        Assert.assertEquals(expectedStepCount, stepCount);
    }

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
            @Nullable LongList intervals,
            long minTs,
            long maxTs,
            int step
    );

    protected abstract SampleByIntervalIterator createIterator(
            TimestampSampler sampler,
            @Nullable TimeZoneRules tzRules,
            long offset,
            LongList intervals,
            long minTs,
            long maxTs,
            LongList stepPerInterval
    );

    // Fuzzes the per-cluster mode the refresh uses: disjoint intervals with random gaps between them, one random step
    // per interval, minTs and maxTs taken from the intervals as the refresh takes them. The bucket hull of an interval,
    // from the floor of its first bucket to the ceiling of its last one, comes from an iterator over that interval
    // alone. Every step-group must lie within the hulls of the intervals it intersects, with no gap between those
    // hulls, so that its replace range never covers a whole bucket outside the intervals. Step-groups must not
    // overlap, so that no bucket is recomputed twice, and together they must cover every interval.
    protected void testFuzzPerCluster(Rnd rnd, @Nullable TimeZoneRules tzRules, long start, long end) throws SqlException {
        final int samplingInterval = 1 + rnd.nextInt(5);
        final char timeUnit = "mhd".charAt(rnd.nextInt(3));
        final long offset = (rnd.nextBoolean() ? 1 : -1) * rnd.nextLong(timestampDriver.fromHours(1));
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(timestampDriver, samplingInterval, timeUnit, 0);
        final TimestampSampler hullSampler = TimestampSamplerFactory.getInstance(timestampDriver, samplingInterval, timeUnit, 0);

        final int count = 1 + rnd.nextInt(10);
        final long maxSize = Math.max((end - start) / (2L * count), 1);
        final LongList intervals = new LongList(2 * count);
        long hi = start;
        for (int i = 0; i < count; i++) {
            final long lo = Math.min(end, hi + 1 + rnd.nextLong(maxSize));
            hi = Math.min(end, lo + rnd.nextLong(maxSize));
            unionInPlace(intervals, lo, hi);
        }
        final int intervalCount = intervals.size() / 2;
        final LongList steps = new LongList(intervalCount);
        final LongList hulls = new LongList(intervals.size());
        for (int i = 0; i < intervalCount; i++) {
            steps.add(1 + rnd.nextInt(20));
            final SampleByIntervalIterator hull = createIterator(
                    hullSampler,
                    tzRules,
                    offset,
                    null,
                    intervals.getQuick(2 * i),
                    intervals.getQuick(2 * i + 1),
                    1
            );
            hulls.add(hull.getMinTimestamp(), hull.getMaxTimestamp());
        }

        final SampleByIntervalIterator iterator = createIterator(
                sampler,
                tzRules,
                offset,
                intervals,
                intervals.getQuick(0),
                intervals.getQuick(intervals.size() - 1),
                steps
        );
        final LongList remainingIntervals = new LongList(intervals);
        long prevHi = Long.MIN_VALUE;
        int stepGroups = 0;
        while (iterator.next()) {
            Assert.assertTrue("iterator does not advance", ++stepGroups < 1_000_000);
            final long lo = iterator.getTimestampLo();
            final long groupHi = iterator.getTimestampHi();
            Assert.assertTrue(lo < groupHi);
            Assert.assertTrue("step-groups overlap", lo >= prevHi);
            prevHi = groupHi;

            int first = -1;
            int last = -1;
            for (int i = 0; i < intervalCount; i++) {
                if (intervals.getQuick(2 * i) < groupHi && intervals.getQuick(2 * i + 1) >= lo) {
                    if (first == -1) {
                        first = i;
                    }
                    last = i;
                }
            }
            Assert.assertTrue("step-group intersects no interval", first != -1);
            Assert.assertTrue("step-group starts before its first interval's bucket", lo >= hulls.getQuick(2 * first));
            Assert.assertTrue("step-group ends after its last interval's bucket", groupHi <= hulls.getQuick(2 * last + 1));
            for (int i = first; i < last; i++) {
                Assert.assertTrue("step-group spans a gap", hulls.getQuick(2 * i + 1) >= hulls.getQuick(2 * i + 2));
            }
            subtractInPlace(remainingIntervals, lo, groupHi - 1);
        }
        Assert.assertEquals(0, remainingIntervals.size());
    }

    protected void testFuzz(
            Rnd rnd,
            @Nullable TimeZoneRules tzRules,
            long offset,
            long start,
            long end
    ) throws Exception {
        final int step = Math.max(1, rnd.nextInt(100));
        final int interval = Math.max(1, rnd.nextInt(300));

        final char[] timeUnits = new char[]{'m', 'h', 'd'};
        final char timeUnit = timeUnits[rnd.nextInt(timeUnits.length)];

        if (offset == Long.MIN_VALUE) {
            offset = (rnd.nextBoolean() ? 1 : -1) * rnd.nextLong(timestampDriver.fromHours(1));
        }

        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(timestampDriver, interval, timeUnit, 0);

        LongList intervals = null;
        if (rnd.nextBoolean()) {
            // generate a few txn min-max timestamp intervals
            final int count = 1 + rnd.nextInt(10);
            final long maxSize = Math.max((end - start) / count, 1);
            intervals = new LongList(2 * count);
            long lo;
            long hi = start;
            for (int i = 0; i < count; i++) {
                lo = Math.min(end, hi + rnd.nextLong(maxSize));
                hi = Math.min(end, lo + rnd.nextLong(maxSize));
                unionInPlace(intervals, lo, hi);
            }
            // txn interval must have start and end as the outer boundaries
            intervals.setQuick(0, start);
            intervals.setQuick(intervals.size() - 1, end);
        }

        final SampleByIntervalIterator iterator = createIterator(
                sampler,
                tzRules,
                offset,
                intervals,
                start,
                end,
                step
        );

        final long minTs = iterator.getMinTimestamp();
        final long maxTs = iterator.getMaxTimestamp();
        Assert.assertTrue(minTs < maxTs);

        LongList intervalsB = null;
        LongList remainingIntervals = null;
        if (intervals != null) {
            // used for intersection calculation, etc.
            intervalsB = new LongList(intervals.capacity() + 2);
            // holds remaining intervals intersected with [min_ts,max_ts)
            remainingIntervals = new LongList(intervals);
            intersectInPlace(remainingIntervals, minTs, maxTs - 1);
        }

        long minObservedTs = Long.MAX_VALUE;
        long maxObservedTs = Long.MIN_VALUE;
        long prevTsHi = Long.MIN_VALUE;
        while (iterator.next()) {
            final long lo = iterator.getTimestampLo();
            final long hi = iterator.getTimestampHi();
            Assert.assertTrue(lo < hi);
            if (intervals != null) {
                // assert that the iterated bucket has an intersection with txn min-max timestamp intervals
                intervalsB.clear();
                intervalsB.addAll(intervals);
                intersectInPlace(intervalsB, lo, hi - 1);
                Assert.assertTrue(intervalsB.size() > 0);
                // at this point, at least one interval should remain
                Assert.assertTrue(remainingIntervals.size() > 0);
                // subtract the bucket from the remaining intervals
                subtractInPlace(remainingIntervals, lo, hi - 1);
            } else {
                if (prevTsHi != Long.MIN_VALUE) {
                    Assert.assertEquals(prevTsHi, lo);
                }
                prevTsHi = hi;
            }
            minObservedTs = Math.min(minObservedTs, lo);
            maxObservedTs = Math.max(maxObservedTs, hi);
        }

        if (intervals != null) {
            // we should have seen all buckets that intersect with txn intervals
            Assert.assertEquals(0, remainingIntervals.size());
            Assert.assertTrue(minObservedTs >= minTs);
            Assert.assertTrue(maxObservedTs <= maxTs);
        } else {
            Assert.assertEquals(minTs, minObservedTs);
            Assert.assertEquals(maxTs, maxObservedTs);
        }
    }
}
