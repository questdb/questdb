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

package io.questdb.cairo.mv;

import io.questdb.cairo.TimestampDriver;
import io.questdb.griffin.engine.groupby.TimestampSampler;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.datetime.TimeZoneRules;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class TimeZoneIntervalIterator extends SampleByIntervalIterator {
    // Contains intervals with clock shifts, in UTC. Both low and high
    // boundaries are inclusive.
    //
    // Example:
    // In 'Europe/Berlin' a backward DST switch happened at '2021-10-31T01:00', UTC.
    // Namely, at 03:00, local time, the clocks are shifted one hour back.
    // This means that 02:00-03:00 local time interval happens twice:
    // at 00:00-01:00 and at 01:00-02:00, UTC time. Then, if the sampling interval
    // is '1m', 02:00-02:01 local time interval corresponds to two UTC intervals:
    // 00:00-00:01 and 01:00-01:01. Thus, we want to include the whole 00:00-02:00
    // UTC time interval into a single iteration. As a result, the 02:00-03:00 local
    // time interval will be stored in the list.
    //
    // Similar to local shifts, this list also contains local time "gaps" that happen
    // due to forward clock shifts.
    //
    // Example:
    // In 'Europe/Berlin' a forward DST switch happened at '2021-03-28T01:00', UTC.
    // Namely, at 02:00, local time, the clocks are shifted one hour ahead.
    // This means that 02:00-03:00 local time interval never happens. We need to
    // avoid iterating in local time over this interval as such nonexistent timestamps
    // are converted to the next UTC hour. As a result, the 02:00-03:00 local time
    // interval will be stored in the list.
    private final LongList localShifts = new LongList();
    private long localMaxTimestamp;
    private long localMinTimestamp;
    private long localTimestampHi;
    // index of next shift interval lo to check against
    private int shiftLoIndex;
    // time zone offset active up to the shiftLoIndex interval
    private long shiftOffset;
    private TimeZoneRules tzRules;
    private long utcMaxTimestamp; // computed from localMaxTimestamp
    private long utcMinTimestamp; // computed from localMinTimestamp
    private long utcTimestampHi; // computed from localTimestampHi
    private long utcTimestampLo;

    @Override
    public long getMaxTimestamp() {
        return utcMaxTimestamp;
    }

    @Override
    public long getMinTimestamp() {
        return utcMinTimestamp;
    }

    @Override
    public long getStep() {
        return step;
    }

    @Override
    public long getTimestampHi() {
        return utcTimestampHi;
    }

    @Override
    public long getTimestampLo() {
        return utcTimestampLo;
    }

    public TimeZoneIntervalIterator of(
            TimestampDriver driver,
            @NotNull TimestampSampler sampler,
            @NotNull TimeZoneRules tzRules,
            long fixedOffset,
            @Nullable LongList intervals,
            long minTs,
            long maxTs,
            long step
    ) {
        ofCommon(driver, sampler, tzRules, fixedOffset, intervals, minTs, maxTs);
        toTop(step);
        return this;
    }

    public TimeZoneIntervalIterator of(
            TimestampDriver driver,
            @NotNull TimestampSampler sampler,
            @NotNull TimeZoneRules tzRules,
            long fixedOffset,
            @NotNull LongList intervals,
            long minTs,
            long maxTs,
            @NotNull LongList stepPerInterval
    ) {
        ofCommon(driver, sampler, tzRules, fixedOffset, intervals, minTs, maxTs);
        toTop(stepPerInterval);
        return this;
    }

    private long adjustHiBoundary(long localTs) {
        final int idx = IntervalUtils.findInterval(localShifts, localTs);
        if (idx != -1) {
            final long shiftLo = IntervalUtils.decodeIntervalLo(localShifts, idx << 1);
            if (localTs == shiftLo) {
                // We're precisely at the left of the shift interval. No need to adjust.
                return localTs;
            }
            final long shiftHi = IntervalUtils.decodeIntervalHi(localShifts, idx << 1);
            return sampler.nextTimestamp(sampler.round(shiftHi - 1));
        }
        return localTs;
    }

    private long adjustLoBoundary(long localTs) {
        final int idx = IntervalUtils.findInterval(localShifts, localTs);
        if (idx != -1) {
            final long shiftLo = IntervalUtils.decodeIntervalLo(localShifts, idx << 1);
            return sampler.round(shiftLo);
        }
        return localTs;
    }

    // Sets the high boundary of the current step-group to the given local timestamp, and the shift tracking
    // to the shifts that lie before it, so that the next next0() resumes its DST scan with the right offset.
    private void anchorHi(long localTs) {
        shiftLoIndex = 0;
        shiftOffset = tzRules.getLocalOffset(localTs);
        for (int n = localShifts.size(); shiftLoIndex < n; shiftLoIndex += 2) {
            if (localTs <= localShifts.getQuick(shiftLoIndex)) {
                break;
            }
            shiftOffset = tzRules.getLocalOffset(localShifts.getQuick(shiftLoIndex + 1));
        }
        localTimestampHi = localTs;
        utcTimestampHi = localToUtc(localTs);
    }

    // Local bucket floor of the given UTC timestamp, where snapToCluster() anchors the iterator.
    private long localBucketFloor(long utcTs) {
        final long localTs = adjustLoBoundary(utcTs + tzRules.getOffset(utcTs));
        return Math.min(sampler.round(localTs), localMaxTimestamp);
    }

    // UTC timestamp where the bucket that starts at the given local timestamp starts, as anchorHi() computes it,
    // without changing the iterator. A local timestamp inside a forward shift's gap never occurs: the bucket's first
    // timestamp is the shift itself, the gap's start less the offset before the shift.
    private long localToUtc(long localTs) {
        final long gapOffset = tzRules.getDstGapOffset(localTs);
        if (gapOffset != 0) {
            final long gapStart = localTs - gapOffset;
            return gapStart - tzRules.getLocalOffset(gapStart);
        }
        long offset = tzRules.getLocalOffset(localTs);
        for (int i = 0, n = localShifts.size(); i < n; i += 2) {
            if (localTs <= localShifts.getQuick(i)) {
                break;
            }
            offset = tzRules.getLocalOffset(localShifts.getQuick(i + 1));
        }
        return localTs - offset;
    }

    @Override
    protected boolean clampToInterval(long intervalLoUtc, long intervalHiUtc) {
        // The iterator walks local time, while the txn intervals are in UTC, so both interval
        // boundaries are rounded to their buckets in local time.
        if (utcTimestampLo < localToUtc(localBucketFloor(intervalLoUtc))) {
            // The step-group starts in the gap before the interval's first bucket. Snap to that
            // bucket, so that the next step-group starts there.
            snapToCluster(intervalLoUtc);
            return false;
        }
        // The step-group starts inside the interval's buckets. End it no later than the ceiling
        // of the interval's last bucket, so that it does not run into the gap after the interval.
        final long localHi = intervalHiUtc + tzRules.getOffset(intervalHiUtc);
        final long ceilLocal = Math.min(adjustHiBoundary(sampler.nextTimestamp(sampler.round(localHi))), localMaxTimestamp);
        if (localTimestampHi > ceilLocal) {
            anchorHi(ceilLocal);
        }
        return true;
    }

    @Override
    protected boolean next0() {
        if (localTimestampHi == localMaxTimestamp) {
            return false;
        }

        utcTimestampLo = utcTimestampHi;
        localTimestampHi = Math.min(sampler.nextTimestamp(localTimestampHi, step), localMaxTimestamp);
        if (localTimestampHi == localMaxTimestamp) {
            utcTimestampHi = utcMaxTimestamp;
            return true;
        }
        // Make sure to adjust the right boundary in case if we ended up
        // in a gap or a backward shift interval.
        for (int n = localShifts.size(); shiftLoIndex < n; shiftLoIndex += 2) {
            final long shiftLo = localShifts.getQuick(shiftLoIndex);
            if (localTimestampHi <= shiftLo) {
                break;
            }
            final long shiftHi = localShifts.getQuick(shiftLoIndex + 1);
            if (localTimestampHi <= shiftHi) { // localTimestampHi > shiftLo is already true
                localTimestampHi = sampler.nextTimestamp(sampler.round(shiftHi - 1));
                shiftOffset = tzRules.getLocalOffset(shiftHi);
                shiftLoIndex += 2;
                break;
            }
            shiftOffset = tzRules.getLocalOffset(shiftHi);
        }
        utcTimestampHi = localTimestampHi - shiftOffset;
        return true;
    }

    @Override
    protected void snapToCluster(long clusterLoUtc) {
        // Re-anchor the iterator at the bucket floor of clusterLoUtc, in local
        // time. The next next0() call sets utcTimestampLo = utcTimestampHi and
        // advances localTimestampHi by `step` buckets from the snapped point.
        // We also reset shiftLoIndex / shiftOffset so subsequent shift-tracking
        // resumes from the correct position.
        anchorHi(localBucketFloor(clusterLoUtc));
    }

    @Override
    protected void toTop0() {
        this.utcTimestampLo = Numbers.LONG_NULL;
        this.localTimestampHi = localMinTimestamp;
        this.utcTimestampHi = utcMinTimestamp;
        this.shiftLoIndex = 0;
        this.shiftOffset = tzRules.getLocalOffset(localMinTimestamp);
    }

    private void ofCommon(
            TimestampDriver driver,
            @NotNull TimestampSampler sampler,
            @NotNull TimeZoneRules tzRules,
            long fixedOffset,
            @Nullable LongList intervals,
            long minTs,
            long maxTs
    ) {
        super.of(sampler, intervals);
        this.tzRules = tzRules;

        sampler.setOffset(fixedOffset);
        final long localMinTs = minTs + tzRules.getOffset(minTs);
        localMinTimestamp = sampler.round(localMinTs);
        final long localMaxTs = maxTs + tzRules.getOffset(maxTs);
        localMaxTimestamp = sampler.nextTimestamp(sampler.round(localMaxTs));

        // Collect shift intervals.
        localShifts.clear();
        final long limitTs = driver.ceilYYYY(localMaxTimestamp);
        long ts = tzRules.getNextDST(driver.floorYYYY(localMinTimestamp));
        while (ts < limitTs) {
            long offsetBefore = tzRules.getOffset(ts - 1);
            long offsetAfter = tzRules.getOffset(ts);
            long duration = offsetAfter - offsetBefore;
            if (duration < 0) { // backward shift
                final long localStart = ts + offsetAfter;
                final long localEnd = localStart - duration;
                localShifts.add(localStart, localEnd);
            } else { // forward shift (gap)
                final long localStart = ts + offsetBefore;
                final long localEnd = localStart + duration;
                // We don't want the gap to be used as the iterated interval, so increment
                // the right boundary to force the next sample by bucket to be included.
                localShifts.add(localStart, localEnd + 1);
            }
            ts = tzRules.getNextDST(ts);
        }

        // Adjust min/max boundaries in case if they're in a backward shift.
        localMinTimestamp = adjustLoBoundary(localMinTimestamp);
        localMaxTimestamp = adjustHiBoundary(localMaxTimestamp);

        utcMinTimestamp = driver.toUTC(localMinTimestamp, tzRules);
        utcMaxTimestamp = driver.toUTC(localMaxTimestamp, tzRules);
    }
}
