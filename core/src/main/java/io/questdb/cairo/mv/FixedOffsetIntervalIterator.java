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
        ofCommon(sampler, offset, intervals, minTs, maxTs);
        toTop(step);
        return this;
    }

    public FixedOffsetIntervalIterator of(
            @NotNull TimestampSampler sampler,
            long offset,
            @NotNull LongList intervals,
            long minTs,
            long maxTs,
            @NotNull LongList stepPerInterval
    ) {
        ofCommon(sampler, offset, intervals, minTs, maxTs);
        toTop(stepPerInterval);
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
    protected void snapToCluster(long clusterLo) {
        // Pull timestampHi back to the bucket-aligned floor of clusterLo. The
        // next next0() call sets timestampLo = timestampHi, then advances
        // timestampHi by `step` buckets, so the emitted step-group starts at
        // the cluster boundary.
        final long snapped = sampler.round(clusterLo);
        // Defensive guard: never advance past maxTimestamp via the snap.
        timestampHi = Math.min(snapped, maxTimestamp);
    }

    @Override
    protected void toTop0() {
        this.timestampLo = Numbers.LONG_NULL;
        this.timestampHi = minTimestamp;
    }

    private void ofCommon(
            @NotNull TimestampSampler sampler,
            long offset,
            @Nullable LongList intervals,
            long minTs,
            long maxTs
    ) {
        super.of(sampler, intervals);
        sampler.setOffset(offset);
        minTimestamp = sampler.round(minTs);
        maxTimestamp = sampler.nextTimestamp(sampler.round(maxTs));
    }
}
