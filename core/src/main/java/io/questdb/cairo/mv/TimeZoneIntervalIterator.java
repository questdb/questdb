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
    public int getStep() {
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
            int step
    ) {
        super.of(sampler, intervals);
        this.tzRules = tzRules;

        sampler.setStart(fixedOffset);
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

        toTop(step);
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
    protected void toTop0() {
        this.utcTimestampLo = Numbers.LONG_NULL;
        this.localTimestampHi = localMinTimestamp;
        this.utcTimestampHi = utcMinTimestamp;
        this.shiftLoIndex = 0;
        this.shiftOffset = tzRules.getLocalOffset(localMinTimestamp);
    }
}
