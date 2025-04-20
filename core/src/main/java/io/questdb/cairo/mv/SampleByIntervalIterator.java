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
import io.questdb.std.LongList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Allows iterating through SAMPLE BY buckets with the given step.
 * The goal is to split a potentially large time interval to be scanned by
 * the materialized view query into smaller intervals, thus minimizing
 * chances of out-of-memory kills.
 */
public abstract class SampleByIntervalIterator {
    protected TimestampSampler sampler;
    protected int step;
    // index of next txn timestamp interval to check against
    private int txnIntervalLoIndex;
    private LongList txnIntervals;

    public abstract long getMaxTimestamp();

    public abstract long getMinTimestamp();

    public abstract int getStep();

    public abstract long getTimestampHi();

    public abstract long getTimestampLo();

    public boolean next() {
        OUT:
        while (next0()) {
            if (txnIntervals != null) {
                for (int n = txnIntervals.size(); txnIntervalLoIndex < n; txnIntervalLoIndex += 2) {
                    final long intervalLo = txnIntervals.getQuick(txnIntervalLoIndex);
                    final long intervalHi = txnIntervals.getQuick(txnIntervalLoIndex + 1);
                    final long iteratorLo = getTimestampLo();
                    final long iteratorHi = getTimestampHi() - 1; // hi is exclusive, hence -1

                    if (iteratorHi < intervalLo) {
                        // iterator timestamps are before the txn interval
                        // skip the iteration
                        continue OUT;
                    } else if (iteratorLo <= intervalHi) {
                        // iterator timestamps have an intersection with the txn interval
                        return true;
                    }
                    // otherwise, iterator timestamps are after the txn interval
                    // continue to the next txn interval
                }
                // all txn intervals are before the txn interval
                return false;
            }
            return true;
        }
        return false;
    }

    public void toTop(int step) {
        this.step = step;
        toTop0();
    }

    protected abstract boolean next0();

    protected void of(
            @NotNull TimestampSampler sampler,
            @Nullable LongList txnIntervals
    ) {
        this.sampler = sampler;
        this.txnIntervals = txnIntervals;
        this.txnIntervalLoIndex = 0;
    }

    protected abstract void toTop0();
}
