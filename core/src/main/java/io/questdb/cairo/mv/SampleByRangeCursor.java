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
import org.jetbrains.annotations.Nullable;

/**
 * Allows iterating through SAMPLE BY buckets with the given step.
 * The goal is to split a potentially large time interval to be scanned by
 * the materialized view query into smaller intervals, thus minimizing
 */
public class SampleByRangeCursor {
    private long maxTimestamp;
    private long minTimestamp;
    private TimestampSampler sampler;
    private int step;
    private long timestampHi;
    private long timestampLo;
    private TimeZoneRules tzRules;

    public long getMaxTimestamp() {
        return maxTimestamp;
    }

    public long getMinTimestamp() {
        return minTimestamp;
    }

    public long getTimestampHi() {
        return timestampHi;
    }

    public long getTimestampLo() {
        return timestampLo;
    }

    public boolean next() {
        if (timestampHi != maxTimestamp) {
            timestampLo = timestampHi;
            for (int i = 0; i < step; i++) {
                timestampHi = nextTimestamp(timestampHi);
                if (timestampHi == maxTimestamp) {
                    break;
                }
            }
            return true;
        }
        return false;
    }

    public void of(
            @NotNull TimestampSampler sampler,
            @Nullable TimeZoneRules tzRules,
            long fixedTzOffset,
            long minTs,
            long maxTs,
            int step
    ) {
        this.sampler = sampler;
        this.tzRules = tzRules;
        this.step = step;

        sampler.setStart(fixedTzOffset);
        final long tzMinOffset = tzRules != null ? tzRules.getOffset(minTs) : 0;
        final long tzMinTs = sampler.round(minTs + tzMinOffset);
        minTimestamp = tzRules != null ? Timestamps.toUTC(tzMinTs, tzRules) : tzMinTs - tzMinOffset;
        maxTimestamp = nextTimestamp(maxTs);

        toTop();
    }

    public void toTop() {
        this.timestampLo = Numbers.LONG_NULL;
        this.timestampHi = minTimestamp;
    }

    private long nextTimestamp(long ts) {
        final long tzOffset = tzRules != null ? tzRules.getOffset(ts) : 0;
        final long tzTs = sampler.nextTimestamp(sampler.round(ts + tzOffset));
        return tzRules != null ? Timestamps.toUTC(tzTs, tzRules) : tzTs - tzOffset;
    }
}
