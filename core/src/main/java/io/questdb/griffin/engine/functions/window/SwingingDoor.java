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
 * Feed points in traversal order via {@link #accept}; timestamps need not be
 * monotonic. Corridor math only ever runs on a strictly forward step from the
 * last-seen (pending) point: any other step - backward, equal, or wrapping a
 * long in either direction - is a series boundary that keeps the pending point
 * (like end-of-series) and re-anchors at the offending point. The first point
 * of a series and the last point still pending when the series ends are
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
        // The corridor is only meaningful for a strictly forward step from the LAST-SEEN point,
        // so ts <= pendingTs is a series boundary: the pending point stays flushed (kept) like
        // end-of-series and the current point re-anchors. Comparing against anchorTs alone is
        // not enough - a backward step that stays above the anchor (0, 5, 3) reads as forward
        // and discards the pending endpoint at 5 as interior. The comparison cannot wrap, so
        // it also catches backward spans wider than Long.MAX, which wrap ts - anchorTs positive
        // and read as forward to dt alone: a NULL timestamp arrives as Long.MIN_VALUE, and a
        // nanosecond column spans only 292 years. ts <= anchorTs is subsumed while
        // pendingTs >= anchorTs holds (anchor() always re-seats pending) and stays as armor for
        // a loaded state that violates it. dt <= 0 catches the opposite wrap - a forward span
        // over Long.MAX always wraps negative - and dt is the slope divisor below.
        if (ts <= pendingTs || ts <= anchorTs || dt <= 0) {
            anchor(index, ts, value);
            sink.mark(index, true);
            return;
        }

        double nU = value + compdev - anchorValue;
        double nL = value - compdev - anchorValue;
        double sU = nU / dt;
        double sL = nL / dt;
        // A non-finite slope term (IEEE-754 overflow of value +/- compdev - anchorValue, or a
        // non-finite stored value) collapses distinct slopes into the same +/-Inf, making the
        // doors-crossed test unable to see a cross. Keeping the point and restarting the series
        // here is the only decision that provably honors the 2 * compdev reconstruction bound.
        if (!(Double.isFinite(sU) && Double.isFinite(sL))) {
            anchor(index, ts, value);
            sink.mark(index, true);
            return;
        }
        // The mirror hazard at the small end: the doors-crossed test cannot see a cross once
        // the two tolerance slopes round to the same double. Either stage of the slope
        // computation can cause the collapse: the DIVISION (subnormal flush, e.g. a 1e-320
        // peak over a 1e6-tick span, where the slope-domain ULP dwarfs 2 * compdev / dt), or
        // the SUBTRACTION producing the numerators (cancellation against a large anchor gap,
        // e.g. anchor -1e20 vs value 1000 at compdev 1: (value +/- compdev) - anchorValue
        // absorbs the deviation and the tolerance alike, so nU == nL even though the stored
        // doubles are exactly representable and NOT collinear - equal numerators only prove
        // 2 * compdev fell below the ULP of the anchor-gap-dominated difference, not below
        // the data's own resolution). In both cases the arithmetic cannot certify the stated
        // 2 * compdev reconstruction bound, and a surviving zero-width corridor drops points
        // at unbounded multiples of it; keeping the point and restarting is the only decision
        // that provably honors the bound. compdev == 0 is exempt: a zero-width corridor is
        // then the requested semantics - sdt(ts, v, 0) drops points collinear in double
        // arithmetic (pinned by the compdev == 0 tests).
        if (compdev > 0 && sU == sL) {
            anchor(index, ts, value);
            sink.mark(index, true);
            return;
        }
        double nHi = sU < slopeHi ? sU : slopeHi;
        double nLo = sL > slopeLo ? sL : slopeLo;

        if (hasInterval && nLo > nHi) {
            // doors crossed: the pending point is archived (kept) and becomes the new anchor.
            // dt2 cannot wrap or go non-positive: the guard above admits only ts > pendingTs,
            // and dt2 is bounded by dt, which is positive and representable, since
            // anchorTs <= pendingTs. The degenerate check below stays as armor for a loaded
            // state that breaks that invariant.
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
            double nU2 = value + compdev - anchorValue;
            double nL2 = value - compdev - anchorValue;
            slopeHi = nU2 / dt2;
            slopeLo = nL2 / dt2;
            if (!(Double.isFinite(slopeHi) && Double.isFinite(slopeLo))) {
                // same non-finite hazard against the just-promoted anchor; restart, keeping the point
                anchor(index, ts, value);
                sink.mark(index, true);
                return;
            }
            if (compdev > 0 && slopeHi == slopeLo) {
                // same collapse hazard against the just-promoted anchor: the numerators are
                // re-derived from its value and divided by dt2, so they can collapse even
                // though the pre-cross pair over dt stayed distinct - by division (e.g. a
                // flat step whose +/-compdev numerators flush to +/-0.0) or by cancellation
                // (the promoted anchor's gap absorbs 2 * compdev in the subtraction).
                // Restart rather than keep a zero-width corridor alive.
                anchor(index, ts, value);
                sink.mark(index, true);
                return;
            }
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
