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

package io.questdb.griffin.engine.functions.date;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TimestampDriver;
import io.questdb.griffin.engine.functions.MonotonicTimestampFunction;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.Numbers;
import io.questdb.std.datetime.CommonUtils;
import io.questdb.std.datetime.TimeZoneRules;

/**
 * Base for timestamp_floor_utc() with a constant named time zone, the function that calendar-aligned
 * SAMPLE BY with a TIME ZONE clause groups by. It remembers the bucket of the last floored
 * timestamp along with the range of timestamps known to floor to it. SAMPLE BY feeds timestamps
 * in order, so most rows land in the bucket of the previous row. A subclass checks the range
 * first and returns {@link #cachedResult} on a hit:
 * <pre>
 *   if (timestamp >= cachedLo &amp;&amp; timestamp &lt; cachedHi) {
 *       return cachedResult;
 *   }
 * </pre>
 * The range check then replaces the floor arithmetic and the time zone lookups.
 * <p>
 * The cache assumes ordered input. The SAMPLE BY rewrite and a live view's ANCHOR DAILY are
 * the only places where the engine emits this function, and both pass an ordered timestamp.
 * A direct call on an unordered column still returns correct results, but most rows then
 * take the miss path, which can cost more than the uncached arithmetic. Measure before
 * reusing this class for a producer that does not guarantee order.
 * <p>
 * Only buckets of a fixed width are cached, i.e. the units from nanoseconds to days. The other
 * units leave {@link #bucketWidth} at zero and the subclass floors them the uncached way. So do
 * buckets narrower than 8 units of the timestamp resolution, e.g. 1U on a microsecond column,
 * which are too narrow to be worth caching.
 * <p>
 * The cache is mutable state, so a caching function is not thread-safe and parallel execution
 * clones it per worker.
 */
abstract class TimestampFloorUtcBucketCachingFunction extends TimestampFunction implements UnaryFunction, MonotonicTimestampFunction {
    // zero when the function does not cache buckets
    protected final long bucketWidth;
    // Two consecutive misses closer than this to each other tell that several rows are likely
    // to share a bucket. Storing a bucket makes the next range check wait for this row's
    // division, which slows down sparse and unordered timestamps, so only then it pays off.
    private final long nearMissDistance;
    // timestamps in [cachedLo, cachedHi) floor to cachedResult; the range is initially empty
    protected long cachedHi = Long.MIN_VALUE;
    protected long cachedLo = Long.MAX_VALUE;
    protected long cachedResult;
    private long lastMissTimestamp = Long.MIN_VALUE;
    // timestamps in [tzSegmentLo, tzSegmentHi) share tzSegmentOffset; the range is initially
    // empty and only day buckets use it
    private long tzSegmentHi = Long.MIN_VALUE;
    private long tzSegmentLo = Long.MAX_VALUE;
    private long tzSegmentOffset;

    TimestampFloorUtcBucketCachingFunction(int timestampType, char unit, int stride, long effectiveOffset, boolean isBucketCached) {
        super(timestampType);
        long width = 0;
        if (isBucketCached) {
            final TimestampDriver.TimestampFloorWithOffsetMethod floor = timestampDriver.getTimestampFloorWithOffsetMethod(unit);
            width = computeFixedBucketWidth(timestampDriver, floor, unit, stride, effectiveOffset);
        }
        this.nearMissDistance = width / 8;
        // The near-miss distance of a bucket narrower than 8 units is zero, so no two misses
        // count as near each other and the function has no bucket to store. Such a function
        // stays uncached and thread-safe: a positive bucketWidth implies a positive
        // nearMissDistance.
        this.bucketWidth = nearMissDistance > 0 ? width : 0;
    }

    @Override
    public boolean isThreadSafe() {
        // the bucket cache is mutable state
        return bucketWidth == 0 && UnaryFunction.super.isThreadSafe();
    }

    /**
     * Floors the timestamp the way timestamp_floor_utc() does for a named time zone and caches
     * the bucket. The caller must check that {@link #bucketWidth} is positive.
     */
    protected final long floorUtcAndCache(long timestamp, TimeZoneRules tzRules, long effectiveOffset, char unit) {
        final boolean isNearLastMiss = isNearLastMiss(timestamp);
        if (CommonUtils.isSubDayUnit(unit)) {
            // Sub-day buckets convert with the standard offset, which never changes, so the
            // bucket is the local bucket [floored, floored + bucketWidth) shifted back to UTC.
            final long tzOff = CommonUtils.getFloorUtcTzOffset(tzRules, timestamp, unit);
            final long lo = floorFixedWidth(timestamp + tzOff, effectiveOffset) - tzOff;
            cacheBucket(isNearLastMiss, timestamp, lo, lo + bucketWidth, lo);
            return lo;
        }

        // Day buckets convert with the offset in effect at the timestamp. That offset stays the
        // same until the next transition, so the lookup runs once per such tz segment.
        if (timestamp < tzSegmentLo || timestamp >= tzSegmentHi) {
            if (!isNearLastMiss) {
                // A segment looked up for sparse or unordered timestamps is unlikely to serve
                // the next row, so floor the timestamp without touching the caches.
                clearBucket();
                final long tzOff = CommonUtils.getFloorUtcTzOffset(tzRules, timestamp, unit);
                final long floored = floorFixedWidth(timestamp + tzOff, effectiveOffset);
                return CommonUtils.offsetFlooredUtcResult(floored, tzOff, 0, tzRules, unit);
            }
            tzSegmentOffset = CommonUtils.getFloorUtcTzOffset(tzRules, timestamp, unit);
            tzSegmentLo = timestamp;
            tzSegmentHi = tzRules.getNextDST(timestamp);
        }
        final long tzOff = tzSegmentOffset;
        final long floored = floorFixedWidth(timestamp + tzOff, effectiveOffset);
        final long lo = floored - tzOff;
        final long result;
        if (lo >= tzSegmentLo && lo < tzSegmentHi) {
            // the bucket starts in the segment, so the same tz offset converts its start to UTC
            result = lo;
        } else {
            result = CommonUtils.offsetFlooredUtcResult(floored, tzOff, 0, tzRules, unit);
            if (isNearLastMiss && lo < tzSegmentLo && tzRules.getNextDST(lo) == tzSegmentHi) {
                // no transition between the bucket start and the segment: extend the segment
                tzSegmentLo = lo;
            }
        }
        // Timestamps of the segment that land in the local bucket [floored, floored + bucketWidth)
        // floor to the same result. A transition within the bucket splits it into two ranges.
        cacheBucket(isNearLastMiss, timestamp, Math.max(lo, tzSegmentLo), Math.min(lo + bucketWidth, tzSegmentHi), result);
        return result;
    }

    /**
     * Returns the width of the floor's buckets when all of them have the same width and
     * {@code add()} steps from one bucket boundary to the next one, zero otherwise. Calendar
     * units and sub-resolution strides (e.g. nanoseconds on a micro column) yield zero.
     */
    private static long computeFixedBucketWidth(
            TimestampDriver timestampDriver,
            TimestampDriver.TimestampFloorWithOffsetMethod floor,
            char unit,
            int stride,
            long offset
    ) {
        // the micro driver floors nanosecond strides in a nanosecond domain, so its buckets
        // are not guaranteed to repeat at a fixed micro width
        if (unit == 'n' && !ColumnType.isTimestampNano(timestampDriver.getTimestampType())) {
            return 0;
        }
        return AbstractTimestampFloorFromOffsetFunctionFactory.computeFloorBucketWidth(timestampDriver, floor, unit, stride, offset);
    }

    private void cacheBucket(boolean isNearLastMiss, long timestamp, long lo, long hi, long result) {
        // The range check is a safety net: the range does not contain the timestamp when it is
        // below the floor origin (the floor then returns the origin) and when a bound
        // overflows. A range that starts at LONG_NULL must not serve NULL timestamps.
        if (isNearLastMiss && timestamp >= lo && timestamp < hi && lo != Numbers.LONG_NULL) {
            cachedLo = lo;
            cachedHi = hi;
            cachedResult = result;
        } else {
            clearBucket();
        }
    }

    // An empty range keeps the range check predictable for sparse and unordered timestamps.
    private void clearBucket() {
        cachedLo = Long.MAX_VALUE;
        cachedHi = Long.MIN_VALUE;
    }

    // Floors the timestamp to a bucket of the fixed bucketWidth, the way the driver's floor
    // methods do for the fixed-width units. Unlike them, it involves neither a per-unit call
    // nor a multiplication.
    private long floorFixedWidth(long timestamp, long effectiveOffset) {
        if (effectiveOffset != 0 && timestamp < effectiveOffset) {
            // the floor clamps timestamps below a non-epoch origin to the origin
            return effectiveOffset;
        }
        final long remainder = (timestamp - effectiveOffset) % bucketWidth;
        return timestamp - (remainder < 0 ? remainder + bucketWidth : remainder);
    }

    private boolean isNearLastMiss(long timestamp) {
        final long distance = timestamp - lastMissTimestamp;
        lastMissTimestamp = timestamp;
        // a single comparison: the sign of the distance is a coin toss for unordered timestamps
        return Math.abs(distance) < nearMissDistance;
    }
}
