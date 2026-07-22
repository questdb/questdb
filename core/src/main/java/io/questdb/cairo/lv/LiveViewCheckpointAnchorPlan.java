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

package io.questdb.cairo.lv;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TimestampDriver;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.Nullable;

/**
 * Immutable compiler-owned description of the fixed segment an anchored live view
 * resets on. The anchor counterpart of {@link LiveViewCheckpointRangePlan} and
 * {@link LiveViewCheckpointRowsPlan}, and the third way a live view can carry a
 * finite dependency contract.
 * <p>
 * The two frame plans describe what a window function reads around one row; this one
 * describes where the runtime throws that state away. An anchored window resets every
 * function on it the moment the anchor expression's value changes, so a segment - one
 * maximal run of rows sharing an anchor value - is a hard wall in both directions: no
 * row above the wall reads state from below it, and no row below it is influenced by
 * anything above. That is the whole dependency contract, and it needs neither a frame
 * width nor a row count to state.
 * <p>
 * What makes the segment <b>fixed</b> is that its boundaries are a function of the
 * designated timestamp alone, so this plan computes them by arithmetic and reads no
 * base row to do it. The recognized anchor is a calendar-period floor of the
 * designated timestamp - {@code timestamp_floor('<stride><unit>', ts)}, which is also
 * what {@code ANCHOR DAILY} desugars to - whose bucket start is
 * {@link #getSegmentStart(long)} and whose next bucket start is
 * {@link #getSegmentEndExclusive(long)}. A localized repair of a change at {@code C}
 * therefore has {@code L} at the start of {@code C}'s segment and {@code H} at its end,
 * both clamped by the caller against {@code S}.
 * <p>
 * <b>The arithmetic checks itself.</b> The segment end is the floor's own next boundary,
 * which this derives by adding one period to the segment start - a step that is exact
 * for a calendar-aligned unit and is not for a sub-resolution one (nanoseconds on a
 * microsecond column advance nothing). Rather than allowlist units per timestamp
 * precision, {@link #getSegmentEndExclusive(long)} verifies the boundary it computed
 * against the same floor the runtime anchor uses and reports no finite end when the two
 * disagree. A caller reads that as a high bound of
 * {@link LiveViewCheckpointContracts.HighBoundTag#EOF} and falls back to the unbounded
 * rebuild, so an anchor whose period this cannot reproduce costs the view only the
 * localized path.
 * <p>
 * Anchors outside the recognized shape - a time-zone-aware daily anchor whose buckets
 * change width at a DST transition, an anchor over a non-designated column, an
 * arbitrary expression - produce no plan at all. Declining is the conservative
 * direction: without a proven segment boundary there is nothing to bound a repair with,
 * and the view keeps the from-boundary rebuild it has today.
 */
public final class LiveViewCheckpointAnchorPlan {
    private final char addUnit;
    private final TimestampDriver driver;
    private final TimestampDriver.TimestampFloorWithOffsetMethod floor;
    private final long segmentOffset;
    private final int stride;
    private final int timestampType;
    private final char unit;

    private LiveViewCheckpointAnchorPlan(
            char unit,
            char addUnit,
            int stride,
            long segmentOffset,
            int timestampType,
            TimestampDriver driver,
            TimestampDriver.TimestampFloorWithOffsetMethod floor
    ) {
        this.unit = unit;
        this.addUnit = addUnit;
        this.stride = stride;
        this.segmentOffset = segmentOffset;
        this.timestampType = timestampType;
        this.driver = driver;
        this.floor = floor;
    }

    /**
     * Builds the plan for a calendar-period anchor, or returns null when the unit,
     * stride, or timestamp type gives no usable segment arithmetic. Null is an
     * ordinary answer rather than an error: the caller is asking whether this anchor
     * has a fixed segment boundary, and most of the time the honest answer for an
     * unrecognized shape is that it does not.
     *
     * @param unit          the floor unit as {@code timestamp_floor} spells it
     *                      ({@code 'd'}, {@code 'h'}, ..., with {@code 'U'} for
     *                      microseconds)
     * @param stride        how many units one segment spans; at least one
     * @param segmentOffset the origin the buckets are aligned to, in the designated
     *                      timestamp's own units. Zero aligns to the epoch, which is
     *                      what a two-argument {@code timestamp_floor} does
     * @param timestampType the base table's designated timestamp type
     */
    public static @Nullable LiveViewCheckpointAnchorPlan of(
            char unit,
            int stride,
            long segmentOffset,
            int timestampType
    ) {
        if (stride < 1 || !ColumnType.isTimestamp(timestampType)) {
            return null;
        }
        final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
        final TimestampDriver.TimestampFloorWithOffsetMethod floor = driver.getTimestampFloorWithOffsetMethod(unit);
        if (floor == null) {
            return null;
        }
        // add() and floor() spell the microsecond unit differently; every other unit is
        // shared. An unknown unit makes add() return LONG_NULL, which no boundary can be.
        final char addUnit = unit == 'U' ? 'u' : unit;
        if (driver.add(0, addUnit, stride) == Numbers.LONG_NULL) {
            return null;
        }
        return new LiveViewCheckpointAnchorPlan(unit, addUnit, stride, segmentOffset, timestampType, driver, floor);
    }

    /**
     * Returns the exclusive end of the segment that contains {@code timestamp}, or
     * {@link Numbers#LONG_NULL} when the period cannot be advanced exactly - a
     * sub-resolution unit, or an addition that leaves the timestamp domain. The
     * absent answer is the high bound {@code H = EOF}: the caller may not treat it as
     * a timestamp.
     */
    public long getSegmentEndExclusive(long timestamp) {
        final long start = floor.floor(timestamp, stride, segmentOffset);
        if (start > timestamp) {
            // Every timestamp below the origin floors to the origin, so they share one
            // segment that is open below and ends where the first aligned bucket starts.
            return start;
        }
        final long end = driver.add(start, addUnit, stride);
        // The end has to be the floor's own next boundary: one past it must floor back
        // to this segment, and it must floor to itself. A unit finer than the column's
        // resolution fails the first test by not advancing at all, and an addition that
        // overflows fails it by going backwards.
        if (end <= timestamp
                || floor.floor(end, stride, segmentOffset) != end
                || floor.floor(end - 1, stride, segmentOffset) != start) {
            return Numbers.LONG_NULL;
        }
        return end;
    }

    /**
     * Returns the origin the segments are aligned to, in the designated timestamp's own
     * units. Zero means epoch-aligned.
     */
    public long getSegmentOffset() {
        return segmentOffset;
    }

    /**
     * Returns the inclusive start of the segment that contains {@code timestamp}, or
     * {@link Long#MIN_VALUE} when the segment is open below - which happens only for a
     * timestamp under a non-zero alignment origin, since every such row carries the
     * origin as its anchor value. A caller clamps the floor to {@code S} either way, so
     * the open-below case needs no separate branch.
     */
    public long getSegmentStart(long timestamp) {
        final long start = floor.floor(timestamp, stride, segmentOffset);
        return start > timestamp ? Long.MIN_VALUE : start;
    }

    /**
     * Returns how many {@link #getUnit() units} one segment spans.
     */
    public int getStride() {
        return stride;
    }

    public int getTimestampType() {
        return timestampType;
    }

    /**
     * Returns the floor unit as {@code timestamp_floor} spells it.
     */
    public char getUnit() {
        return unit;
    }
}
