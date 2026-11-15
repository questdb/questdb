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

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.zone.ZoneOffsetTransition;

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

    // Draws a case of the per-cluster fuzz in its day-bucket mode: SAMPLE BY 1d or 1M with an offset anywhere in the
    // day, so that bucket floors land inside DST gaps and folds at any time of day, where the offsets of
    // testFuzzPerCluster(), within an hour of midnight, reach only shifts near midnight. Half of the offsets put the
    // floor of the shift's day inside the first DST shift after start, a gap or a fold.
    protected PerClusterFuzzCase drawDayBucketFuzzCase(Rnd rnd, TimeZoneRules tzRules, long start, long end) {
        final PerClusterFuzzCase fuzzCase = new PerClusterFuzzCase();
        fuzzCase.samplingInterval = 1;
        fuzzCase.timeUnit = rnd.nextInt(4) == 0 ? 'M' : 'd';
        final long day = timestampDriver.fromDays(1);
        if (rnd.nextBoolean()) {
            fuzzCase.offset = rnd.nextLong(day);
        } else {
            final long shiftTs = tzRules.getNextDST(start);
            final long offsetBefore = tzRules.getOffset(shiftTs - 1);
            final long offsetAfter = tzRules.getOffset(shiftTs);
            // a gap spans local [shift + before, shift + after), a fold local [shift + after, shift + before)
            final long shiftLocalLo = shiftTs + Math.min(offsetBefore, offsetAfter);
            fuzzCase.offset = Math.floorMod(shiftLocalLo + rnd.nextLong(Math.abs(offsetAfter - offsetBefore)), day);
        }
        drawIntervalsAndSteps(rnd, start, end, fuzzCase);
        return fuzzCase;
    }

    // The local floor, on the bucket grid of SAMPLE BY 1d or 1M ALIGN TO CALENDAR TIME ZONE zone WITH OFFSET offset, of
    // the local time of the given timestamp, from java.time.
    protected LocalDateTime localBucketFloor(ZoneId zone, char timeUnit, long offset, long ts) {
        final LocalDateTime shifted = toInstant(ts).atZone(zone).toLocalDateTime().minusNanos(toNanos(offset));
        final LocalDate floorDate = timeUnit == 'M' ? shifted.toLocalDate().withDayOfMonth(1) : shifted.toLocalDate();
        return floorDate.atStartOfDay().plusNanos(toNanos(offset));
    }

    // Fuzzes the per-cluster mode the refresh uses: disjoint intervals with random gaps between them, one random step
    // per interval, minTs and maxTs taken from the intervals as the refresh takes them. See assertPerClusterStepGroups().
    protected void testFuzzPerCluster(Rnd rnd, @Nullable TimeZoneRules tzRules, long start, long end) throws SqlException {
        final PerClusterFuzzCase fuzzCase = new PerClusterFuzzCase();
        fuzzCase.samplingInterval = 1 + rnd.nextInt(5);
        fuzzCase.timeUnit = "mhd".charAt(rnd.nextInt(3));
        fuzzCase.offset = (rnd.nextBoolean() ? 1 : -1) * rnd.nextLong(timestampDriver.fromHours(1));
        drawIntervalsAndSteps(rnd, start, end, fuzzCase);
        assertPerClusterStepGroups(tzRules, null, fuzzCase);
    }

    // The day-bucket mode of testFuzzPerCluster(), see drawDayBucketFuzzCase(). Its step-groups must also hold
    // whole buckets of the SQL, see assertStepGroupHoldsWholeBuckets().
    protected void testFuzzPerClusterDayBuckets(Rnd rnd, TimeZoneRules tzRules, ZoneId zone, long start, long end) throws SqlException {
        assertPerClusterStepGroups(tzRules, zone, drawDayBucketFuzzCase(rnd, tzRules, start, end));
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

    protected Instant toInstant(long ts) {
        final long unitsPerSecond = timestampDriver.fromSeconds(1);
        return Instant.ofEpochSecond(Math.floorDiv(ts, unitsPerSecond), Math.floorMod(ts, unitsPerSecond) * (1_000_000_000L / unitsPerSecond));
    }

    // Every step-group must lie within the hulls of the intervals it intersects, with no gap between those hulls, so
    // that its replace range never covers a whole bucket outside the intervals. The bucket hull of an interval, from
    // the floor of its first bucket to the ceiling of its last one, comes from an iterator over that interval alone.
    // Step-groups must not overlap, so that no bucket is recomputed twice, and together they must cover every
    // interval. Given the zone, every step-group must also hold whole buckets of the SQL.
    private void assertPerClusterStepGroups(@Nullable TimeZoneRules tzRules, @Nullable ZoneId zone, PerClusterFuzzCase fuzzCase) throws SqlException {
        final int samplingInterval = fuzzCase.samplingInterval;
        final char timeUnit = fuzzCase.timeUnit;
        final long offset = fuzzCase.offset;
        final LongList intervals = fuzzCase.intervals;
        final LongList steps = fuzzCase.steps;
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(timestampDriver, samplingInterval, timeUnit, 0);
        final TimestampSampler hullSampler = TimestampSamplerFactory.getInstance(timestampDriver, samplingInterval, timeUnit, 0);

        final int intervalCount = intervals.size() / 2;
        final LongList hulls = new LongList(intervals.size());
        for (int i = 0; i < intervalCount; i++) {
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
            if (zone != null) {
                assertStepGroupHoldsWholeBuckets(zone, fuzzCase, lo, groupHi);
            }
            subtractInPlace(remainingIntervals, lo, groupHi - 1);
        }
        Assert.assertEquals(0, remainingIntervals.size());
    }

    private void assertRowInStepGroupIffKeyIs(ZoneId zone, PerClusterFuzzCase fuzzCase, long lo, long hi, long rowTs) {
        final long key = bucketKey(zone, fuzzCase.timeUnit, fuzzCase.offset, rowTs);
        final boolean isRowInside = rowTs >= lo && rowTs < hi;
        final boolean isKeyInside = key >= lo && key < hi;
        if (isRowInside != isKeyInside) {
            Assert.fail("step-group splits a bucket [zone=" + zone + ", unit=1" + fuzzCase.timeUnit
                    + ", offset=" + fuzzCase.offset + ", stepGroup=[" + toInstant(lo) + ", " + toInstant(hi)
                    + "), row=" + toInstant(rowTs) + ", key=" + toInstant(key) + ']');
        }
    }

    // Every step-group [lo, hi) holds whole buckets of the SQL: a row lies in it if and only if the row's bucket key
    // does. Otherwise the refresh either fails with "timestamp out of replace range" or rewrites a bucket from part of
    // its rows. The rows: every 15 minutes, hourly for months, from a day before lo to a day after hi, the edges of
    // the step-group, and each DST transition in that range, with its neighbours.
    private void assertStepGroupHoldsWholeBuckets(ZoneId zone, PerClusterFuzzCase fuzzCase, long lo, long hi) {
        final long from = lo - timestampDriver.fromDays(1);
        final long to = hi + timestampDriver.fromDays(1);
        final long rowStep = timestampDriver.fromMinutes(fuzzCase.timeUnit == 'M' ? 60 : 15);
        for (long rowTs = from; rowTs <= to; rowTs += rowStep) {
            assertRowInStepGroupIffKeyIs(zone, fuzzCase, lo, hi, rowTs);
        }
        assertRowInStepGroupIffKeyIs(zone, fuzzCase, lo, hi, lo - 1);
        assertRowInStepGroupIffKeyIs(zone, fuzzCase, lo, hi, lo);
        assertRowInStepGroupIffKeyIs(zone, fuzzCase, lo, hi, hi - 1);
        assertRowInStepGroupIffKeyIs(zone, fuzzCase, lo, hi, hi);
        final long unitsPerSecond = timestampDriver.fromSeconds(1);
        ZoneOffsetTransition transition = zone.getRules().nextTransition(toInstant(from));
        while (transition != null && transition.getInstant().isBefore(toInstant(to))) {
            final Instant instant = transition.getInstant();
            final long transitionTs = instant.getEpochSecond() * unitsPerSecond;
            assertRowInStepGroupIffKeyIs(zone, fuzzCase, lo, hi, transitionTs - 1);
            assertRowInStepGroupIffKeyIs(zone, fuzzCase, lo, hi, transitionTs);
            assertRowInStepGroupIffKeyIs(zone, fuzzCase, lo, hi, transitionTs + 1);
            transition = zone.getRules().nextTransition(instant);
        }
    }

    // The key of the SQL bucket of a row, from java.time: the local floor of the row's local time, as a local time of
    // the row's own offset, which java.time moves forward by the gap length when the floor lies in a DST gap. That is
    // how timestamp_floor_utc keys a bucket whose floor lies in a gap or a fold.
    private long bucketKey(ZoneId zone, char timeUnit, long offset, long rowTs) {
        final ZonedDateTime row = toInstant(rowTs).atZone(zone);
        final LocalDateTime floor = localBucketFloor(zone, timeUnit, offset, rowTs);
        final Instant key = ZonedDateTime.ofLocal(floor, zone, row.getOffset()).toInstant();
        final long unitsPerSecond = timestampDriver.fromSeconds(1);
        return key.getEpochSecond() * unitsPerSecond + key.getNano() / (1_000_000_000L / unitsPerSecond);
    }

    // Disjoint intervals with random gaps between them in [start, end], and one random step per interval.
    private void drawIntervalsAndSteps(Rnd rnd, long start, long end, PerClusterFuzzCase fuzzCase) {
        final int count = 1 + rnd.nextInt(10);
        final long maxSize = Math.max((end - start) / (2L * count), 1);
        final LongList intervals = fuzzCase.intervals;
        long hi = start;
        for (int i = 0; i < count; i++) {
            final long lo = Math.min(end, hi + 1 + rnd.nextLong(maxSize));
            hi = Math.min(end, lo + rnd.nextLong(maxSize));
            unionInPlace(intervals, lo, hi);
        }
        for (int i = 0, n = intervals.size() / 2; i < n; i++) {
            fuzzCase.steps.add(1 + rnd.nextInt(20));
        }
    }

    private long toNanos(long ts) {
        return ts * (1_000_000_000L / timestampDriver.fromSeconds(1));
    }

    // A case of the per-cluster fuzz: the sampler, its offset, the intervals and one step per interval.
    protected static final class PerClusterFuzzCase {
        final LongList intervals = new LongList();
        final LongList steps = new LongList();
        long offset;
        int samplingInterval;
        char timeUnit;
    }
}
