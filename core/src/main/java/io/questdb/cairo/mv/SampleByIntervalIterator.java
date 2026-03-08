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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Iterates through SAMPLE BY buckets with the given step. The goal is to split
 * a potentially large time interval to be scanned by the materialized view query
 * into smaller intervals, thus minimizing chances of out-of-memory kills.
 * <p>
 * When the list of txn min-max timestamp intervals is given, the iterated
 * SAMPLE BY buckets that have no intersection with the intervals,
 * i.e. remain unchanged, will be skipped.
 */
public abstract class SampleByIntervalIterator {
    protected TimestampSampler sampler;
    protected long step;
    private LongList intervals;
    // index of next txn timestamp interval to check against
    private int txnIntervalLoIndex;

    /**
     * Returns maximum timestamp that belong to the iterated intervals.
     */
    public abstract long getMaxTimestamp();

    /**
     * Returns minimum timestamp that belong to the iterated intervals.
     */
    public abstract long getMinTimestamp();

    /**
     * Returns minimal number of SAMPLE BY buckets for in a single iteration.
     */
    public abstract long getStep();

    /**
     * High boundary for the current iteration's interval.
     * Meant to be used as an exclusive boundary when querying.
     */
    public abstract long getTimestampHi();

    /**
     * Low boundary for the current iteration's interval.
     * Meant to be used as an inclusive boundary when querying.
     */
    public abstract long getTimestampLo();

    /**
     * Returns true if the current interval is the last interval.
     */
    public boolean isLast() {
        return getTimestampHi() >= getMaxTimestamp();
    }

    /**
     * Iterates to the next interval.
     *
     * @return true if the iterator moved to the next interval; false if the iteration has ended
     */
    public boolean next() {
        final int intervalsSize = intervals != null ? intervals.size() : -1;
        OUT:
        while (next0()) {
            if (intervalsSize != -1) {
                final long iteratorLo = getTimestampLo();
                final long iteratorHi = getTimestampHi() - 1; // hi is exclusive, hence -1
                while (txnIntervalLoIndex < intervalsSize) {
                    final long intervalLo = intervals.getQuick(txnIntervalLoIndex);
                    final long intervalHi = intervals.getQuick(txnIntervalLoIndex + 1);

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
                    txnIntervalLoIndex += 2;
                }
                // all txn intervals are before the txn interval
                return false;
            }
            return true;
        }
        return false;
    }

    /**
     * Reset the iterator for the given number of steps per iteration.
     *
     * @see #getStep()
     */
    public void toTop(long step) {
        this.step = step;
        this.txnIntervalLoIndex = 0;
        toTop0();
    }

    protected abstract boolean next0();

    protected void of(
            @NotNull TimestampSampler sampler,
            @Nullable LongList intervals
    ) {
        this.sampler = sampler;
        this.intervals = intervals;
    }

    protected abstract void toTop0();
}
