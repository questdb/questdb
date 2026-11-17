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
 * <p>
 * The iterator can run in two modes:
 * <ul>
 *   <li>Single-step (legacy) -- {@link #toTop(long)} sets one step for the
 *       whole iteration. All step-groups use the same step.</li>
 *   <li>Per-cluster -- {@link #toTop(LongList)} accepts one step per cached
 *       interval (cluster). On every cluster transition the iterator swaps
 *       the active step and {@link #clampToInterval(long, long)} aligns the
 *       iterator state to the cluster boundaries: it snaps a step-group that
 *       starts before a cluster forward to the cluster's lower boundary (via
 *       {@link #snapToCluster(long)}), and caps a step-group that would run
 *       past a cluster to the cluster's upper bucket ceiling, so a step-group
 *       never straddles a gap between two clusters.</li>
 * </ul>
 */
public abstract class SampleByIntervalIterator {
    protected TimestampSampler sampler;
    protected long step;
    private LongList intervals;
    private @Nullable LongList stepPerInterval;
    // index of next txn timestamp interval to check against
    private int txnIntervalLoIndex;

    /**
     * Returns the start of the SAMPLE BY bucket that holds the given timestamp, where the iterator starts the
     * step-group of an interval whose low boundary is that timestamp. Valid once the iterator is set up, for a
     * timestamp no later than the maximum timestamp it was set up with.
     */
    public abstract long bucketFloor(long timestamp);

    /**
     * Returns maximum timestamp that belong to the iterated intervals.
     */
    public abstract long getMaxTimestamp();

    /**
     * Returns minimum timestamp that belong to the iterated intervals.
     */
    public abstract long getMinTimestamp();

    /**
     * Returns number of SAMPLE BY buckets for the current iteration. In
     * per-cluster mode this is the step of whichever cluster the iterator is
     * currently inside.
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
                while (txnIntervalLoIndex < intervalsSize) {
                    final long intervalLo = intervals.getQuick(txnIntervalLoIndex);
                    final long intervalHi = intervals.getQuick(txnIntervalLoIndex + 1);

                    if (getTimestampLo() > intervalHi) {
                        // The step-group is entirely after this txn interval; try the next one.
                        txnIntervalLoIndex += 2;
                        // Cluster just advanced -- swap the step to the new
                        // cluster's step so the next next0() advances at the right
                        // granularity. The new cluster's snap/cap is handled below
                        // via clampToInterval().
                        if (stepPerInterval != null && txnIntervalLoIndex < intervalsSize) {
                            this.step = stepPerInterval.getQuick(txnIntervalLoIndex >> 1);
                        }
                        continue;
                    }
                    // The step-group precedes, straddles, or sits inside this interval.
                    // clampToInterval() either snaps the iterator forward to the interval's
                    // first bucket (returns false; re-run next0() from there) or caps the
                    // step-group's high boundary to the interval's last bucket ceiling
                    // (returns true) so that no step-group, and no replace range, spans a
                    // gap between intervals.
                    if (clampToInterval(intervalLo, intervalHi)) {
                        return true;
                    }
                    continue OUT;
                }
                // all txn intervals are before the iterator
                return false;
            }
            return true;
        }
        return false;
    }

    /**
     * Reset the iterator for per-cluster stepping. The given list must have
     * one entry per cached interval (i.e. {@code stepPerInterval.size() ==
     * intervals.size() / 2}). The iterator stores the reference and uses it
     * across subsequent {@link #next()} calls, so callers must not mutate it
     * while the iteration is in progress.
     */
    public void toTop(@NotNull LongList stepPerInterval) {
        this.stepPerInterval = stepPerInterval;
        this.step = stepPerInterval.size() > 0 ? stepPerInterval.getQuick(0) : 1L;
        this.txnIntervalLoIndex = 0;
        toTop0();
    }

    /**
     * Reset the iterator for the given number of steps per iteration.
     *
     * @see #getStep()
     */
    public void toTop(long step) {
        this.step = step;
        this.stepPerInterval = null;
        this.txnIntervalLoIndex = 0;
        toTop0();
    }

    /**
     * Reconciles the current step-group (produced by the last {@link #next0()})
     * with the txn interval {@code [intervalLo, intervalHi]} it precedes,
     * straddles or sits inside. The base {@link #next()} only calls this when
     * {@code getTimestampLo() <= intervalHi}.
     * <ul>
     *   <li>When the step-group starts before the interval's first bucket, it
     *       snaps the iterator forward to that bucket (via
     *       {@link #snapToCluster(long)}) and returns {@code false}, so the
     *       caller re-runs {@link #next0()} from the interval boundary and no
     *       step-group spans the gap before the interval.</li>
     *   <li>Otherwise the step-group starts inside the interval's buckets; it
     *       caps the step-group's high boundary to the bucket ceiling of
     *       {@code intervalHi} and returns {@code true}, so the step-group,
     *       and the replace range it drives, never runs past the interval into
     *       the gap after it.</li>
     * </ul>
     */
    protected abstract boolean clampToInterval(long intervalLo, long intervalHi);

    protected abstract boolean next0();

    protected void of(
            @NotNull TimestampSampler sampler,
            @Nullable LongList intervals
    ) {
        this.sampler = sampler;
        this.intervals = intervals;
    }

    /**
     * Snap the iterator's high-water mark to the bucket-aligned floor of the
     * given timestamp so that the next {@link #next0()} emits a fresh
     * step-group starting at the cluster boundary. Called when the iterator's
     * current step-group would land before a cluster, to avoid stepping
     * through the gap one tiny step at a time.
     */
    protected abstract void snapToCluster(long clusterLo);

    protected abstract void toTop0();
}
