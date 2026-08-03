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

package io.questdb.griffin.engine.functions.window;

/**
 * Pure Swinging Door Trending (SDT) state machine. No engine dependencies.
 * <p>
 * Feed points in non-decreasing timestamp order via {@link #accept}. The first
 * point of a series and the last point still pending when the series ends are
 * always kept. The sink is invoked for the current index and, when a tentative
 * point turns out to be interior (or the doors cross), for a previously-seen
 * index (back-patch). Callers must therefore use a random-access output slot.
 */
public class SwingingDoor {

    public interface Sink {
        void mark(long index, boolean keep);
    }

    private double compdev;

    // anchor (last archived point)
    private boolean hasAnchor;
    private long anchorIndex;
    private long anchorTs;
    private double anchorValue;

    // feasible slope interval [slopeLo, slopeHi]; valid only when hasInterval
    private boolean hasInterval;
    private double slopeHi; // running min of upper-tolerance slopes
    private double slopeLo; // running max of lower-tolerance slopes

    // pending (last-seen, tentatively-kept) point
    private boolean hasPending;
    private long pendingIndex;
    private long pendingTs;
    private double pendingValue;

    public void accept(long index, long ts, double value, boolean isNull, boolean ignoreNulls, Sink sink) {
        if (isNull) {
            if (ignoreNulls) {
                sink.mark(index, false); // skip entirely, no state change
                return;
            }
            // RESPECT NULLS: hard boundary, always kept, resets the series.
            // The pending point before the gap is flushed (kept) like end-of-series,
            // so the last real sample before the gap terminates its segment.
            sink.mark(index, true);
            reset();
            return;
        }

        if (!hasAnchor) {
            anchor(index, ts, value);
            sink.mark(index, true);
            return;
        }

        long dt = ts - anchorTs;
        if (dt <= 0) {
            // equal / non-increasing timestamp: treat as a hard boundary
            anchor(index, ts, value);
            sink.mark(index, true);
            return;
        }

        double sU = (value + compdev - anchorValue) / dt;
        double sL = (value - compdev - anchorValue) / dt;
        double nHi = sU < slopeHi ? sU : slopeHi;
        double nLo = sL > slopeLo ? sL : slopeLo;

        if (hasInterval && nLo > nHi) {
            // doors crossed: the pending point is archived (kept) and becomes the new anchor
            long dt2 = ts - pendingTs;
            anchorIndex = pendingIndex;
            anchorTs = pendingTs;
            anchorValue = pendingValue;
            if (dt2 <= 0) {
                // degenerate; restart the series at the current point
                anchor(index, ts, value);
                sink.mark(index, true);
                return;
            }
            slopeHi = (value + compdev - anchorValue) / dt2;
            slopeLo = (value - compdev - anchorValue) / dt2;
            hasInterval = true;
            // current point becomes the new pending, tentatively kept
            pending(index, ts, value);
            sink.mark(index, true);
            return;
        }

        // no cross: the previous pending was interior -> unmark it (unless it is the anchor)
        if (hasPending && pendingIndex != anchorIndex) {
            sink.mark(pendingIndex, false);
        }
        slopeHi = nHi;
        slopeLo = nLo;
        hasInterval = true;
        pending(index, ts, value);
        sink.mark(index, true);
    }

    public void configure(double compdev) {
        this.compdev = compdev;
    }

    public void reset() {
        hasAnchor = false;
        hasInterval = false;
        hasPending = false;
        slopeHi = Double.POSITIVE_INFINITY;
        slopeLo = Double.NEGATIVE_INFINITY;
    }

    private void anchor(long index, long ts, double value) {
        hasAnchor = true;
        anchorIndex = index;
        anchorTs = ts;
        anchorValue = value;
        hasInterval = false;
        slopeHi = Double.POSITIVE_INFINITY;
        slopeLo = Double.NEGATIVE_INFINITY;
        pending(index, ts, value);
    }

    private void pending(long index, long ts, double value) {
        hasPending = true;
        pendingIndex = index;
        pendingTs = ts;
        pendingValue = value;
    }

    // --- state serialization for map-backed per-partition storage ---

    public long packFlags() {
        return (hasAnchor ? 1 : 0) | (hasInterval ? 2 : 0) | (hasPending ? 4 : 0);
    }

    public void load(long flags, long anchorIndex, long anchorTs, double anchorValue,
                      double slopeHi, double slopeLo, long pendingIndex, long pendingTs, double pendingValue) {
        this.hasAnchor = (flags & 1) != 0;
        this.hasInterval = (flags & 2) != 0;
        this.hasPending = (flags & 4) != 0;
        this.anchorIndex = anchorIndex;
        this.anchorTs = anchorTs;
        this.anchorValue = anchorValue;
        this.slopeHi = slopeHi;
        this.slopeLo = slopeLo;
        this.pendingIndex = pendingIndex;
        this.pendingTs = pendingTs;
        this.pendingValue = pendingValue;
    }

    public long anchorIndex() {
        return anchorIndex;
    }

    public long anchorTs() {
        return anchorTs;
    }

    public double anchorValue() {
        return anchorValue;
    }

    public double slopeHi() {
        return slopeHi;
    }

    public double slopeLo() {
        return slopeLo;
    }

    public long pendingIndex() {
        return pendingIndex;
    }

    public long pendingTs() {
        return pendingTs;
    }

    public double pendingValue() {
        return pendingValue;
    }
}
