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

import io.questdb.std.CharSequenceHashSet;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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
 * <h2>The affected keys</h2>
 * A segment also collects the partition keys its corrections carried, when the caller asks
 * for them. Inside one closed segment only those keys' output has changed - every other key
 * is already correct - so they are what a keyed replay would follow through the base's
 * posting index instead of reading every row of the segment. The values are the resolved
 * logical ones rather than the WAL's own symbol integers, because those index one
 * transaction's symbol space and a repair replays against a table reader's.
 * <p>
 * The collection is bounded per segment and its overflow is not a denial: a segment past
 * its budget reports {@link #isSegmentKeyDomainComplete(int)} false and reads whole, which
 * costs the same write and only a larger read.
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
    // segmentStart, segmentEndExclusive, minTs, maxTs, rowCount, keySetIndex,
    // isKeyDomainOverflowed, hasNullKey per entry, ordered by segmentStart ascending.
    private static final int STRIDE = 8;
    // The affected keys of each segment, in the order the segments were opened rather than
    // in segment order: an entry names its set by index, so a segment inserted ahead of
    // another does not have to move anyone's keys. Retained across repairs and cleared
    // rather than dropped, so a worker pays for the growth once.
    private final ObjList<CharSequenceHashSet> keySets = new ObjList<>();
    private final LongList segments = new LongList();
    private long activeSegmentStart;
    // How many distinct keys one segment may collect before the collection stops being
    // worth its memory. Zero collects none, which is what a view with no keyed repair
    // available - or a caller that cannot read the key column - asks for.
    private int maxKeysPerSegment;
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
     * @param key the row's resolved partition key, or null for the null symbol. Ignored
     *            when the caller asked for no keys, and read only for a row that lands in a
     *            closed segment - a residual row is repaired by the ordinary resume, which
     *            follows no key
     *
     * @return false once the change set has opened more than {@link #MAX_CLOSED_SEGMENTS}
     * closed segments, after which the decomposition is abandoned and the caller
     * falls back to the union range
     */
    public boolean addRow(long ts, @Nullable CharSequence key, @NotNull LiveViewCheckpointAnchorPlan anchorPlan) {
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
            segments.setQuick(base + 2, Math.min(segments.getQuick(base + 2), ts));
            segments.setQuick(base + 3, Math.max(segments.getQuick(base + 3), ts));
            segments.setQuick(base + 4, segments.getQuick(base + 4) + 1);
            addKey(base, key);
            return true;
        }
        if (segments.size() / STRIDE >= MAX_CLOSED_SEGMENTS) {
            overflowed = true;
            return false;
        }
        final int base = insertAt(-index - 1, cachedSegmentStart, cachedSegmentEndExclusive, ts);
        addKey(base, key);
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
     * @return the exclusive end of closed segment {@code index}. Carried alongside the
     * start because a deferred repair is planned from the entry rather than from this
     * scratch, and re-deriving the end there would need the anchor plan the segment was
     * placed against.
     */
    public long getSegmentEndExclusive(int index) {
        return segments.getQuick(index * STRIDE + 1);
    }

    /**
     * @return the highest in-view timestamp the change set touched inside closed segment
     * {@code index}
     */
    public long getSegmentMaxTs(int index) {
        return segments.getQuick(index * STRIDE + 3);
    }

    /**
     * @return the lowest in-view timestamp the change set touched inside closed segment
     * {@code index}
     */
    public long getSegmentMinTs(int index) {
        return segments.getQuick(index * STRIDE + 2);
    }

    /**
     * @return how many qualifying base rows the change set placed inside closed segment
     * {@code index}. Diagnostic: what a repair over the segment reads and rewrites is the
     * whole segment, not these rows.
     */
    public long getSegmentRowCount(int index) {
        return segments.getQuick(index * STRIDE + 4);
    }

    /**
     * @return the affected keys of closed segment {@code index} - the resolved logical
     * values the corrections carried, which a keyed replay would follow through the base's
     * posting index. Empty when the caller collected none, and meaningless unless
     * {@link #isSegmentKeyDomainComplete(int)} holds.
     * <p>
     * The set carries no null, which {@link #hasSegmentNullKey(int)} reports separately -
     * it is a partition key like any other, and holding it beside the set rather than in
     * it is what lets a caller walk the set by index without testing for one.
     */
    public @NotNull CharSequenceHashSet getSegmentKeys(int index) {
        return keySets.getQuick((int) segments.getQuick(index * STRIDE + 5));
    }

    /**
     * @return whether closed segment {@code index} was touched by a correction carrying a
     * null partition key
     */
    public boolean hasSegmentNullKey(int index) {
        return segments.getQuick(index * STRIDE + 7) != 0;
    }

    /**
     * @return whether the keys collected for closed segment {@code index} are all of them.
     * False once the segment reached its key budget, or when the caller collected no keys
     * at all - in both cases a repair of that segment has to read every row of it, which
     * costs the same write and only a larger read.
     */
    public boolean isSegmentKeyDomainComplete(int index) {
        return maxKeysPerSegment > 0 && segments.getQuick(index * STRIDE + 6) == 0;
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
        of(activeSegmentStart, 0);
    }

    /**
     * Rebinds this scratch to one repair that also collects the keys its corrections carry.
     * {@code maxKeysPerSegment} bounds one segment's key domain; a segment that reaches it
     * keeps the keys it has and reports the domain incomplete, which demotes that segment
     * to a whole-segment replay rather than denying it.
     */
    public void of(long activeSegmentStart, int maxKeysPerSegment) {
        this.activeSegmentStart = activeSegmentStart;
        this.maxKeysPerSegment = maxKeysPerSegment;
        for (int i = 0, n = keySets.size(); i < n; i++) {
            keySets.getQuick(i).clear();
        }
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

    /**
     * Joins one row's key to the segment at {@code base}, and records the budget overflow
     * that leaves the segment's key domain incomplete.
     */
    private void addKey(int base, CharSequence key) {
        if (maxKeysPerSegment < 1) {
            return;
        }
        if (key == null) {
            // Recorded beside the segment rather than in the set, so it costs no budget
            // and no slot - and, more to the point, so a caller walking the set by index
            // never has to test its entries for null. A duplicate null in a keyed scan's
            // key list yields the null key's rows twice.
            segments.setQuick(base + 7, 1);
            return;
        }
        final CharSequenceHashSet keys = keySets.getQuick((int) segments.getQuick(base + 5));
        final int keyIndex = keys.keyIndex(key);
        if (keyIndex < 0) {
            return;
        }
        if (keys.size() >= maxKeysPerSegment) {
            segments.setQuick(base + 6, 1);
            return;
        }
        // addAt copies the sequence, which the WAL's own symbol table hands out as a
        // flyweight over its mapped pages.
        keys.addAt(keyIndex, key);
    }

    /**
     * @return the entry's base offset in {@link #segments}
     */
    private int insertAt(int index, long segmentStart, long segmentEndExclusive, long ts) {
        // The key set is taken off the pool in discovery order and named by index, so an
        // entry inserted ahead of another leaves every other segment's keys where they are.
        final int keySetIndex = segments.size() / STRIDE;
        if (keySets.size() <= keySetIndex) {
            keySets.extendAndSet(keySetIndex, new CharSequenceHashSet());
        }
        // Inserted in reverse so each add() lands ahead of the ones before it, leaving
        // (segmentStart, segmentEndExclusive, minTs, maxTs, rowCount, keySetIndex,
        // isKeyDomainOverflowed, hasNullKey) in order at the entry's own base offset.
        final int base = index * STRIDE;
        segments.add(base, 0);
        segments.add(base, 0);
        segments.add(base, keySetIndex);
        segments.add(base, 1);
        segments.add(base, ts);
        segments.add(base, ts);
        segments.add(base, segmentEndExclusive);
        segments.add(base, segmentStart);
        return base;
    }
}
