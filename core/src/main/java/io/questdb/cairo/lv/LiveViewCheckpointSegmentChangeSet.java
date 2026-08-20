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

package io.questdb.cairo.lv;

import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.NotNull;

/**
 * One out-of-order change set decomposed into the anchor segments it actually touches.
 * <p>
 * A repair's cost is paid per <b>replacement range</b>, not per correction: the apply
 * merges rather than appends every live-view partition the range covers and writes each
 * of those partitions whole. Today's repair takes one union range running from the anchor
 * below the lowest correction to the frontier, so a commit carrying rows at the head and
 * rows a month back rewrites a month of output for the sake of a few thousand rows. The
 * union is an artefact of how the change set is measured - a scalar minimum and a scalar
 * maximum - rather than of what it holds: a deep commit reaches the head and one old
 * segment, and nothing in between.
 * <p>
 * Under a pure fixed-anchor plan the anchor resets every stateful function at the segment
 * boundary, so a row in one segment cannot influence the output of any other. That is what
 * makes the decomposition usable: each closed segment below the runtime's own segment can
 * be repaired and published on its own, over its own range, and the segments between them
 * are left untouched. {@link LiveViewBackfillEnvelope#deferralGate} is the predicate that
 * proves it; a view outside that gate keeps the union range.
 * <p>
 * The residual - everything at or above the active segment's start - is not decomposed.
 * It is the correction the runtime is still standing in, and it takes the ordinary resume
 * from the anchor below it, which Fix 2 already bounds to one checkpoint cadence.
 * <p>
 * Rows below the view's {@code START FROM} boundary belong to no segment here. They
 * produce no output at all, so the caller drops them before they reach {@link #addRow}
 * rather than letting them drag the change floor down to a boundary that denies the
 * repair.
 * <p>
 * Worker-owned scratch: {@link #of} clears every field, so one instance serves every
 * repair a refresh worker plans.
 */
public final class LiveViewCheckpointSegmentChangeSet {
    /**
     * How many distinct closed segments one change set may decompose into before the
     * decomposition stops being worth taking. Each segment costs its own replay, its own
     * replacement commit and its own timeline splice, so a change reaching more of them
     * than this is one the union range serves better. The measured workload's mean is
     * 1.68 and its maximum 35.
     */
    public static final int MAX_CLOSED_SEGMENTS = 64;
    // segmentStart, minTs, maxTs per entry, ordered by segmentStart ascending.
    private static final int STRIDE = 3;
    private final LongList segments = new LongList();
    private long activeSegmentStart;
    // Containment cache for the row loop: consecutive rows of one commit almost always
    // share a segment, and a hit skips both the floor arithmetic and the lookup.
    private long cachedSegmentEndExclusive;
    private long cachedSegmentStart;
    private boolean overflowed;
    private long residualMaxTs;
    private long residualMinTs;

    /**
     * Folds one qualifying base row into the decomposition. A row at or above
     * {@code activeSegmentStart} joins the residual; anything below it lands in - or opens -
     * the closed segment that holds it.
     *
     * @return false once the change set has opened more than {@link #MAX_CLOSED_SEGMENTS}
     * closed segments, after which the decomposition is abandoned and the caller
     * falls back to the union range
     */
    public boolean addRow(long ts, @NotNull LiveViewCheckpointAnchorPlan anchorPlan) {
        if (ts >= activeSegmentStart) {
            addResidual(ts, ts);
            return true;
        }
        if (overflowed) {
            return false;
        }
        if (ts < cachedSegmentStart || ts >= cachedSegmentEndExclusive) {
            final long start = anchorPlan.getSegmentStart(ts);
            final long end = anchorPlan.getSegmentEndExclusive(ts);
            if (end == Numbers.LONG_NULL || end > activeSegmentStart) {
                // No representable segment end is H = EOF, which no localized repair can
                // stand on; an end above the active segment's start means the arithmetic
                // does not agree with the runtime's own segmentation. Either way this row
                // has no closed segment of its own, so the decomposition cannot describe
                // it.
                overflowed = true;
                return false;
            }
            cachedSegmentStart = start;
            cachedSegmentEndExclusive = end;
        }
        final int index = indexOf(cachedSegmentStart);
        if (index >= 0) {
            final int base = index * STRIDE;
            segments.setQuick(base + 1, Math.min(segments.getQuick(base + 1), ts));
            segments.setQuick(base + 2, Math.max(segments.getQuick(base + 2), ts));
            return true;
        }
        if (segments.size() / STRIDE >= MAX_CLOSED_SEGMENTS) {
            overflowed = true;
            return false;
        }
        insertAt(-index - 1, cachedSegmentStart, ts);
        return true;
    }

    /**
     * Folds a whole commit's span into the residual without visiting its rows. The caller
     * takes this shortcut for a commit whose own minimum already sits at or above the
     * active segment's start, which is every in-order commit and every shallow correction.
     */
    public void addResidual(long minTs, long maxTs) {
        residualMinTs = residualMinTs == Numbers.LONG_NULL ? minTs : Math.min(residualMinTs, minTs);
        residualMaxTs = residualMaxTs == Numbers.LONG_NULL ? maxTs : Math.max(residualMaxTs, maxTs);
    }

    /**
     * @return the number of distinct closed anchor segments the change set touches
     */
    public int getClosedSegmentCount() {
        return segments.size() / STRIDE;
    }

    /**
     * @return the highest timestamp the change set touched at or above the active
     * segment's start, or {@link Numbers#LONG_NULL} when it touched nothing there
     */
    public long getResidualMaxTs() {
        return residualMaxTs;
    }

    /**
     * @return the lowest timestamp the change set touched at or above the active
     * segment's start, or {@link Numbers#LONG_NULL} when it touched nothing there.
     * This is the correction floor the residual repair plans from.
     */
    public long getResidualMinTs() {
        return residualMinTs;
    }

    /**
     * @return the highest in-view timestamp the change set touched inside closed segment
     * {@code index}
     */
    public long getSegmentMaxTs(int index) {
        return segments.getQuick(index * STRIDE + 2);
    }

    /**
     * @return the lowest in-view timestamp the change set touched inside closed segment
     * {@code index}
     */
    public long getSegmentMinTs(int index) {
        return segments.getQuick(index * STRIDE + 1);
    }

    /**
     * @return the inclusive start of closed segment {@code index}. Segments come back
     * oldest first, which is the order a repair must take them in: a later segment's
     * cumulative row positions depend on how many rows the earlier ones added.
     */
    public long getSegmentStart(int index) {
        return segments.getQuick(index * STRIDE);
    }

    /**
     * @return true once the decomposition gave up - too many distinct closed segments, or
     * a row whose segment has no representable end. The caller must then repair the whole
     * change set as one union range.
     */
    public boolean isOverflowed() {
        return overflowed;
    }

    /**
     * Rebinds this scratch to one repair. {@code activeSegmentStart} is the inclusive start
     * of the anchor segment the runtime frontier sits in: everything below it can be
     * repaired independently, everything at or above it is the runtime's own segment and
     * stays with the residual.
     */
    public void of(long activeSegmentStart) {
        this.activeSegmentStart = activeSegmentStart;
        segments.clear();
        overflowed = false;
        residualMinTs = Numbers.LONG_NULL;
        residualMaxTs = Numbers.LONG_NULL;
        // An empty cache must miss on the first row whatever it is.
        cachedSegmentStart = Long.MAX_VALUE;
        cachedSegmentEndExclusive = Long.MIN_VALUE;
    }

    /**
     * Binary search over the segment starts. Returns the entry index when found, and
     * {@code -(insertionPoint) - 1} when not, in the {@code Arrays.binarySearch} shape.
     */
    private int indexOf(long segmentStart) {
        int low = 0;
        int high = segments.size() / STRIDE - 1;
        while (low <= high) {
            final int mid = (low + high) >>> 1;
            final long midStart = segments.getQuick(mid * STRIDE);
            if (midStart < segmentStart) {
                low = mid + 1;
            } else if (midStart > segmentStart) {
                high = mid - 1;
            } else {
                return mid;
            }
        }
        return -(low + 1);
    }

    private void insertAt(int index, long segmentStart, long ts) {
        // Inserted in reverse so each add() lands ahead of the ones before it, leaving
        // (segmentStart, minTs, maxTs) in order at the entry's own base offset.
        final int base = index * STRIDE;
        segments.add(base, ts);
        segments.add(base, ts);
        segments.add(base, segmentStart);
    }
}
