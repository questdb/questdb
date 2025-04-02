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

import io.questdb.griffin.engine.groupby.TimestampSampler;
import io.questdb.std.Numbers;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.std.datetime.microtime.Timestamps;
import org.jetbrains.annotations.NotNull;

public class TimeZoneIntervalIterator implements SampleByIntervalIterator {
    private long localMaxTimestamp;
    private long localMinTimestamp;
    private long localTimestampHi;
    private long localTimestampLo;
    private TimestampSampler sampler;
    private int step;
    private TimeZoneRules tzRules;

    @Override
    public long getMaxTimestamp() {
        return Timestamps.toUTC(localMaxTimestamp, tzRules);
    }

    @Override
    public long getMinTimestamp() {
        return Timestamps.toUTC(localMinTimestamp, tzRules);
    }

    @Override
    public int getStep() {
        return step;
    }

    @Override
    public long getTimestampHi() {
        return Timestamps.toUTC(localTimestampHi, tzRules);
    }

    @Override
    public long getTimestampLo() {
        return Timestamps.toUTC(localTimestampLo, tzRules);
    }

    @Override
    public boolean next() {
        if (localTimestampHi != localMaxTimestamp) {
            localTimestampLo = localTimestampHi;
            long timestampHi = Timestamps.toUTC(localTimestampHi, tzRules);
            for (int i = 0; i < step; ) {
                localTimestampHi = sampler.nextTimestamp(localTimestampHi);
                if (localTimestampHi == localMaxTimestamp) {
                    break;
                }
                long newTimestampHi = Timestamps.toUTC(localTimestampHi, tzRules);
                if (newTimestampHi <= timestampHi) {
                    // We're in a DST gap, so keep skipping the interval until we get out of it.
                    // A DST gap means nonexistent local time, e.g. if the clocks move forward +1 hour
                    // at 02:00, the local hour between 02:00 and 03:00 is nonexistent.
                    continue;
                }
                timestampHi = newTimestampHi;
                i++;
            }
            return true;
        }
        return false;
    }

    public TimeZoneIntervalIterator of(
            @NotNull TimestampSampler sampler,
            @NotNull TimeZoneRules tzRules,
            long fixedOffset,
            long minTs,
            long maxTs,
            int step
    ) {
        this.sampler = sampler;
        this.tzRules = tzRules;

        sampler.setStart(fixedOffset);
        final long localMinTs = minTs + tzRules.getOffset(minTs);
        localMinTimestamp = sampler.round(localMinTs);
        final long localMaxTs = maxTs + tzRules.getOffset(maxTs);
        localMaxTimestamp = sampler.nextTimestamp(sampler.round(localMaxTs));

        toTop(step);
        return this;
    }

    @Override
    public void toTop(int step) {
        this.localTimestampLo = Numbers.LONG_NULL;
        this.localTimestampHi = localMinTimestamp;
        this.step = step;
    }
}
