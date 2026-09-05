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
import io.questdb.std.NumericException;
import io.questdb.std.datetime.CommonUtils;
import io.questdb.std.datetime.DateLocaleFactory;
import io.questdb.std.datetime.TimeZoneRules;
import org.jetbrains.annotations.NotNull;
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
 * designated timestamp alone, so this plan computes them without reading a base row.
 * The recognized anchor is a calendar-period floor of the designated timestamp -
 * {@code timestamp_floor('<stride><unit>', ts)}, which is also what {@code ANCHOR DAILY}
 * desugars to - whose bucket start is {@link #getSegmentStart(long)} and whose next
 * bucket start is {@link #getSegmentEndExclusive(long)}. A localized repair of a change
 * at {@code C} therefore has {@code L} at the start of {@code C}'s segment and {@code H}
 * at its end, both clamped by the caller against {@code S}.
 * <p>
 * A daily anchor carrying an IANA time zone desugars to {@code timestamp_floor_utc}
 * instead, and {@link #ofTimeZone} builds the same two bounds for it. A civil day in a
 * DST-observing zone is 23 or 25 hours wide across a transition, so its boundaries are
 * not fixed-stride arithmetic - but they are still a function of the designated
 * timestamp alone, computed from the zone's transition table rather than from a modulus.
 * The variant floors and advances through the very primitives the runtime
 * {@code timestamp_floor_utc} calls ({@link CommonUtils#getFloorUtcTzOffset} and
 * {@link CommonUtils#offsetFlooredUtcResult}), so the grid it describes is the grid the
 * runtime resets on rather than one that merely resembles it.
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
 * The time-zone variant runs a self-check of its own on each bound and needs both more. A
 * zone floor is not monotone through a transition: an anchor whose local wall time falls in
 * the hour a spring-forward skips, or in the hour a fall-back repeats, reports boundaries
 * that overlap or reverse for the rows around it. The end check catches that from above and
 * answers EOF; the start check catches it from below and answers an open-below start.
 * <p>
 * A fall-back needs one check more than either, because it can split a segment in two. It
 * winds local time back, so an instant well above the segment's end can read below the
 * segment's own local boundary again and floor straight back to its start - the anchor value
 * is then carried by two disjoint intervals with a different value between them. The bounds
 * describe the lower interval and say nothing about the upper one, which a repair bounded by
 * them would leave standing with stale output, so {@link #getSegmentEndExclusive(long)} walks
 * the transitions above its end and reports no finite end when one of them winds local time
 * back below the boundary. A probe in the UPPER interval keeps both bounds, and the pair it
 * gets spans from the lower interval's start: wider than that probe's own run, but still a
 * wall at each end, so it is a union of whole runs and a repair over it recomputes each of
 * them from a reset.
 * <p>
 * <b>The two checks are independent, and refuse independently.</b> They are separate
 * expressions over separate instants, so a probe on one of the two days a year this applies
 * usually gets one finite bound and one refusal rather than two refusals: a start the
 * runtime agrees with under an EOF end on the day the gap ends a segment, and an open-below
 * start under a finite end on the day the gap starts one. Each refusal widens the repair on
 * its own side and each surviving bound stands on its own proof, so a caller that reads one
 * bound may take it. A caller that needs a <b>closed</b> segment must test both:
 * {@link LiveViewCheckpointSegmentChangeSet#addRow} does, because an open-below start is a
 * refusal rather than a floor a segment can be repaired from.
 * <p>
 * Anchors outside the recognized shape - an anchor over a non-designated column, an
 * arbitrary expression, a zone name this cannot resolve - produce no plan at all.
 * Declining is the conservative direction: without a proven segment boundary there is
 * nothing to bound a repair with, and the view keeps the from-boundary rebuild it has
 * today.
 */
public final class LiveViewCheckpointAnchorPlan {
    private final char addUnit;
    private final TimestampDriver driver;
    private final TimestampDriver.TimestampFloorWithOffsetMethod floor;
    private final long segmentOffset;
    private final int stride;
    private final int timestampType;
    // How far from either end of the timestamp domain the zone conversions stop being
    // representable. They shift a timestamp by one zone offset and read the table at the
    // shifted value, and 48 hours bounds any offset difference a zone has ever carried -
    // Samoa's 2011 date-line shift included. Zero for the plain anchor, which shifts nothing.
    private final long tzGuard;
    // The zone the buckets follow, or null when the anchor is fixed-stride arithmetic.
    private final TimeZoneRules tzRules;
    private final char unit;

    private LiveViewCheckpointAnchorPlan(
            char unit,
            char addUnit,
            int stride,
            long segmentOffset,
            int timestampType,
            TimestampDriver driver,
            TimestampDriver.TimestampFloorWithOffsetMethod floor,
            @Nullable TimeZoneRules tzRules
    ) {
        this.unit = unit;
        this.addUnit = addUnit;
        this.stride = stride;
        this.segmentOffset = segmentOffset;
        this.timestampType = timestampType;
        this.driver = driver;
        this.floor = floor;
        this.tzRules = tzRules;
        this.tzGuard = tzRules == null ? 0 : driver.fromDays(2);
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
        return build(unit, stride, segmentOffset, timestampType, null);
    }

    /**
     * Builds the plan for the same calendar-period anchor read in a named time zone -
     * what {@code ANCHOR DAILY 'HH:MM' '<zone>'} desugars to - or returns null when the
     * zone cannot be resolved or the period gives no usable segment arithmetic. Null is
     * an ordinary answer here for the same reason it is in {@link #of}.
     * <p>
     * The zone shifts where the buckets sit and how wide they are; it does not change
     * what a segment means, so the two bounds the caller reads are the same two. Both are
     * computed in local time and returned as UTC instants, which is what the runtime
     * {@code timestamp_floor_utc} anchor emits and therefore what the view's own output
     * timestamps are comparable to.
     *
     * @param segmentOffset the origin the buckets are aligned to, on the <i>local</i>
     *                      grid - {@code timestamp_floor_utc} treats its {@code from}
     *                      argument as a modulus seed for local time rather than as a
     *                      UTC instant
     * @param timeZone      the zone name as the anchor expression spells it
     */
    public static @Nullable LiveViewCheckpointAnchorPlan ofTimeZone(
            char unit,
            int stride,
            long segmentOffset,
            int timestampType,
            @NotNull CharSequence timeZone
    ) {
        if (!ColumnType.isTimestamp(timestampType)) {
            return null;
        }
        final TimeZoneRules tzRules;
        try {
            tzRules = DateLocaleFactory.EN_LOCALE.getRules(
                    timeZone,
                    ColumnType.getTimestampDriver(timestampType).getTZRuleResolution()
            );
        } catch (NumericException e) {
            // A persisted view was valid when it was created, so this should not happen -
            // but declining is the answer this method already has for a shape it cannot
            // describe, and it is the safe one for a zone that has left the tzdata since.
            return null;
        }
        return build(unit, stride, segmentOffset, timestampType, tzRules);
    }

    /**
     * Returns the exclusive end of the segment that contains {@code timestamp}, or
     * {@link Numbers#LONG_NULL} when the period cannot be advanced exactly - a
     * sub-resolution unit, an addition that leaves the timestamp domain, or a zone
     * transition the anchor's own wall time straddles - and also when a fall-back above
     * the end gives the segment a second part the end does not cover. The absent answer
     * is the high bound {@code H = EOF}: the caller may not treat it as a timestamp.
     */
    public long getSegmentEndExclusive(long timestamp) {
        if (tzRules != null) {
            return tzSegmentEndExclusive(timestamp);
        }
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
     * units. Zero means epoch-aligned. For a time-zone anchor the origin lies on the
     * zone's local grid rather than on the UTC one, which is how
     * {@code timestamp_floor_utc} reads its own {@code from} argument.
     */
    public long getSegmentOffset() {
        return segmentOffset;
    }

    /**
     * Returns the inclusive start of the segment that contains {@code timestamp}, or
     * {@link Long#MIN_VALUE} when the segment is open below - which happens for a
     * timestamp under a non-zero alignment origin, since every such row carries the
     * origin as its anchor value, and for a zone floor a transition makes non-monotone.
     * A caller that clamps the floor to {@code S} needs no separate branch for it. A caller
     * that instead needs a closed segment to repair on its own must reject it: it is not a
     * floor, and {@link #getSegmentEndExclusive(long)} reports a finite end beside it often
     * enough that reading the end alone proves nothing about the start.
     */
    public long getSegmentStart(long timestamp) {
        if (tzRules != null) {
            return tzSegmentStart(timestamp);
        }
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

    private static @Nullable LiveViewCheckpointAnchorPlan build(
            char unit,
            int stride,
            long segmentOffset,
            int timestampType,
            @Nullable TimeZoneRules tzRules
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
        return new LiveViewCheckpointAnchorPlan(unit, addUnit, stride, segmentOffset, timestampType, driver, floor, tzRules);
    }

    /**
     * Whether a zone conversion of {@code timestamp} stays inside the timestamp domain.
     * Every zone bound this plan computes shifts a value by one zone offset and reads the
     * table at the shifted value, so a timestamp within a zone offset of either end has
     * no bound to report rather than a wrapped one.
     */
    private boolean isTzRepresentable(long timestamp) {
        return timestamp > Long.MIN_VALUE + tzGuard && timestamp < Long.MAX_VALUE - tzGuard;
    }

    /**
     * The runtime {@code timestamp_floor_utc} floor, primitive for primitive: convert to
     * local at the offset in force at {@code timestamp}, floor on the local grid, convert
     * the floored value back to UTC at the offset in force there. Sharing the primitives
     * is what makes this the anchor value the runtime computes rather than an
     * approximation of it.
     */
    private long tzFloor(long timestamp) {
        final long tzOff = CommonUtils.getFloorUtcTzOffset(tzRules, timestamp, unit);
        final long flooredLocal = floor.floor(timestamp + tzOff, stride, segmentOffset);
        return CommonUtils.offsetFlooredUtcResult(flooredLocal, tzOff, 0, tzRules, unit);
    }

    private long tzSegmentEndExclusive(long timestamp) {
        if (!isTzRepresentable(timestamp)) {
            return Numbers.LONG_NULL;
        }
        final long tzOff = CommonUtils.getFloorUtcTzOffset(tzRules, timestamp, unit);
        final long local = timestamp + tzOff;
        final long localStart = floor.floor(local, stride, segmentOffset);
        // Every local time below the origin floors to it, so they share one segment that
        // ends where the first aligned bucket starts.
        final long localEnd = localStart > local ? localStart : driver.add(localStart, addUnit, stride);
        if (!isTzRepresentable(localEnd)) {
            return Numbers.LONG_NULL;
        }
        final long start = CommonUtils.offsetFlooredUtcResult(localStart, tzOff, 0, tzRules, unit);
        final long end = CommonUtils.offsetFlooredUtcResult(localEnd, tzOff, 0, tzRules, unit);
        // The same self-check the fixed-stride branch runs, against the same floor the
        // runtime anchor uses. It carries more weight here: the conversion back to UTC
        // reads the zone table at an approximation of the boundary's own instant, which a
        // transition sitting on that boundary can resolve to the wrong side.
        if (end <= timestamp
                || !isTzRepresentable(end)
                || tzFloor(end) != end
                || tzFloor(end - 1) != start) {
            return Numbers.LONG_NULL;
        }
        // Those checks prove the segment is closed just BELOW its end. They prove nothing
        // about the instants further above it, and a fall-back transition puts rows there
        // that carry this segment's own anchor value: it winds local time back, and while
        // local time reads below localEnd again the floor lands back on localStart. Under
        // ANCHOR DAILY '02:30' 'Europe/Berlin' the segment that ends at 2026-10-25T00:30Z
        // has exactly that second part, [2026-10-25T01:00Z, 2026-10-25T01:30Z) - those rows
        // read 02:00..02:29 CET, below the day's own 02:30. A repair bounded at the end
        // would stop below them and leave their output standing, so this reports no finite
        // end for such a segment and the caller widens to the unbounded rebuild instead.
        //
        // A row floors to localStart only while its local time reads below localEnd, and
        // local time increases strictly with the instant between two transitions. So over
        // the tail above the end the minimum local time is taken at the end itself or at one
        // of the transitions above it, and probing those instants covers the whole tail. The
        // end's own local time already reads at or above localEnd - a lower one would floor
        // back into this segment, which the tzFloor(end) == end check above rejects - so the
        // transitions are all that is left to probe. Every instant that reads below localEnd
        // sits below end + tzGuard: it is below localEnd less its own zone offset, end is
        // localEnd less another, and tzGuard bounds the difference between any two zone
        // offsets (see the field's own note). So the walk is finite.
        for (long transition = tzRules.getNextDST(end - 1);
             transition != Long.MAX_VALUE && transition < end + tzGuard;
             transition = tzRules.getNextDST(transition)) {
            if (!isTzRepresentable(transition)
                    || transition + CommonUtils.getFloorUtcTzOffset(tzRules, transition, unit) < localEnd) {
                return Numbers.LONG_NULL;
            }
        }
        return end;
    }

    private long tzSegmentStart(long timestamp) {
        if (!isTzRepresentable(timestamp)) {
            return Long.MIN_VALUE;
        }
        final long start = tzFloor(timestamp);
        if (start > timestamp || !isTzRepresentable(start)) {
            return Long.MIN_VALUE;
        }
        // A zone floor is not monotone through a transition, so a row below this start can
        // still carry it as its anchor value - and a repair floored at it would leave that
        // row out. The second check is what catches that: the instant one below a genuine
        // segment start belongs to the previous segment, and in the non-monotone case it
        // floors back to this one instead.
        if (tzFloor(start) != start || tzFloor(start - 1) == start) {
            return Long.MIN_VALUE;
        }
        return start;
    }
}
