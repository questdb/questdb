/*******************************************************************************
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

import io.questdb.griffin.engine.groupby.TimestampSampler;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class FixedOffsetIntervalIterator extends SampleByIntervalIterator {
    private long maxTimestamp;
    private long minTimestamp;
    private long timestampHi;
    private long timestampLo;

    @Override
    public long getMaxTimestamp() {
        return maxTimestamp;
    }

    @Override
    public long getMinTimestamp() {
        return minTimestamp;
    }

    @Override
    public long getStep() {
        return step;
    }

    @Override
    public long getTimestampHi() {
        return timestampHi;
    }

    @Override
    public long getTimestampLo() {
        return timestampLo;
    }

    public FixedOffsetIntervalIterator of(
            @NotNull TimestampSampler sampler,
            long offset,
            @Nullable LongList intervals,
            long minTs,
            long maxTs,
            long step
    ) {
        super.of(sampler, intervals);

        sampler.setOffset(offset);
        minTimestamp = sampler.round(minTs);
        maxTimestamp = sampler.nextTimestamp(sampler.round(maxTs));

        toTop(step);
        return this;
    }

    @Override
    protected boolean next0() {
        if (timestampHi == maxTimestamp) {
            return false;
        }
        timestampLo = timestampHi;
        timestampHi = Math.min(sampler.nextTimestamp(timestampHi, step), maxTimestamp);
        return true;
    }

    @Override
    protected void toTop0() {
        this.timestampLo = Numbers.LONG_NULL;
        this.timestampHi = minTimestamp;
    }
}
