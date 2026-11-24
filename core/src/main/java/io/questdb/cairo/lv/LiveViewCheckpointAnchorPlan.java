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
import io.questdb.std.datetime.FixedTimeZoneRule;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.std.datetime.millitime.Dates;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Compiler-owned bounds for anchors derived solely from the designated timestamp.
 * A segment is a maximal run of one anchor value; resetting at either end isolates
 * its state from adjacent runs. For a recognized calendar floor, repair uses the
 * segment start as L and its exclusive end as H, clamped against START FROM.
 * <p>
 * Time-zone bounds use the runtime's own floor and offset primitives, so civil days
 * may span 23 or 25 hours. Both bounds self-check against the runtime floor; an
 * unusable start is open below and an unusable end is EOF. The two checks are
 * independent, and a closed-segment caller must accept neither bound without the other.
 * <p>
 * A DST fallback can repeat one anchor value in disjoint runs. Transition checks
 * refuse a finite end that excludes a later run, and refuse a start crossed by a
 * run from below. A probe in the upper repeated run may retain bounds spanning a
 * union of complete runs. A skipped civil day can collapse both bounds to one
 * instant, which cannot be a finite segment.
 * <p>
 * Unrecognized expressions, unresolved zones and unprovable arithmetic widen repair
 * instead of supplying an unsafe bound.
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
     * Builds the runtime's calendar-floor grid in local time and returns UTC bounds, or
     * null for an unresolved zone or unusable period. Resolve fixed UTC offsets before
     * zone names, matching timestamp_floor_utc: prefix-based zone lookup alone can
     * interpret UTC+05:00 as UTC, and self-checking that wrong grid cannot detect it.
     *
     * @param segmentOffset modulus origin on the local grid, not a UTC instant
     * @param timeZone      fixed UTC offset or zone name as spelled by the anchor
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
        final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
        // The same call, over the same range, that the runtime makes on its zone argument,
        // and the same reading of its answer: only the minutes, whatever length it parsed.
        final long fixedOffset = Dates.parseOffset(timeZone, 0, timeZone.length());
        final TimeZoneRules tzRules;
        if (fixedOffset != Long.MIN_VALUE) {
            tzRules = new FixedTimeZoneRule(driver.fromMinutes(Numbers.decodeLowInt(fixedOffset)));
        } else {
            try {
                tzRules = DateLocaleFactory.EN_LOCALE.getRules(timeZone, driver.getTZRuleResolution());
            } catch (NumericException e) {
                // A persisted view was valid when it was created, so this should not happen -
                // but declining is the answer this method already has for a shape it cannot
                // describe, and it is the safe one for a zone that has left the tzdata since.
                return null;
            }
        }
        return build(unit, stride, segmentOffset, timestampType, tzRules);
    }

    /**
     * Returns the exclusive end of the segment that contains {@code timestamp}, or
     * {@link Numbers#LONG_NULL} when the period cannot be advanced exactly - a
     * sub-resolution unit, an addition that leaves the timestamp domain, or a zone
     * transition the anchor's own wall time straddles - and also when a fall-back above
     * the end gives the segment a second part the end does not cover, or a zone that skips
     * a whole civil day converts the segment's start and end back to one instant. The
     * absent answer is the high bound {@code H = EOF}: the caller may not treat it as a
     * timestamp.
     */
    public long getSegmentEndExclusive(long timestamp) {
        if (tzRules != null) {
            return tzSegmentEndExclusive(timestamp);
        }
        // Every timestamp below a non-zero origin floors to the origin, and so does the whole
        // first aligned bucket above it. Those timestamps therefore share the first bucket's
        // segment and end where it ends: the floor names the origin as their start, and the
        // end is computed from it the same way as from any other start.
        final long start = floor.floor(timestamp, stride, segmentOffset);
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
     * timestamp under a non-zero alignment origin and for one in the first aligned bucket
     * above it, since every such row carries the origin as its anchor value, and for a zone
     * floor a transition makes non-monotone - including a fall-back above the start that
     * hands the rows it repeats an anchor value from below it. A caller that clamps the floor
     * to {@code S} needs no separate branch for it. A caller that instead needs a closed
     * segment to repair on its own must reject it: it is not a floor, and
     * {@link #getSegmentEndExclusive(long)} reports a finite end beside it often enough that
     * reading the end alone proves nothing about the start.
     */
    public long getSegmentStart(long timestamp) {
        if (tzRules != null) {
            return tzSegmentStart(timestamp);
        }
        final long start = floor.floor(timestamp, stride, segmentOffset);
        if (start > timestamp || start == Long.MIN_VALUE) {
            return Long.MIN_VALUE;
        }
        // The instant one below a genuine segment start belongs to the previous segment. Under
        // a non-zero origin the floor clamps every instant below the origin to the origin
        // itself, so the first aligned bucket's start floors back to it instead: the rows below
        // the origin carry the same anchor value, and a replay floored at the origin would
        // restart their run part way through. The zone branch runs the same check.
        return floor.floor(start - 1, stride, segmentOffset) == start ? Long.MIN_VALUE : start;
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
        // Every local time below the origin floors to it. So does the whole first aligned
        // bucket above it, so the pair this names for such a probe - an end where that bucket
        // starts - reads one instant twice, and the end <= start check below refuses it.
        final long localEnd = localStart > local ? localStart : driver.add(localStart, addUnit, stride);
        if (!isTzRepresentable(localEnd)) {
            return Numbers.LONG_NULL;
        }
        final long start = CommonUtils.offsetFlooredUtcResult(localStart, tzOff, 0, tzRules, unit);
        final long end = CommonUtils.offsetFlooredUtcResult(localEnd, tzOff, 0, tzRules, unit);
        // The same self-check the fixed-stride branch runs, against the same floor the
        // runtime anchor uses. It carries more weight here: the conversion back to UTC
        // reads the zone table at an approximation of the boundary's own instant, which a
        // transition sitting on that boundary can resolve to the wrong side. And the end must
        // lie above the start: a zone that skips a whole civil day converts that day's start
        // and its end back to ONE instant. Pacific/Apia jumped from UTC-10 to UTC+14 at
        // 2011-12-30T10:00Z, and under ANCHOR DAILY '00:30' the missing day's start and end
        // both read 2011-12-30T10:30Z - a value the runtime hands to every row from the jump
        // up to 2011-12-31T10:30Z, so an end there cuts that run in two while both instant
        // checks below still pass.
        if (end <= timestamp
                || end <= start
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
        // Those checks prove the segment is closed just BELOW its start. They prove nothing
        // about the instants above it, and a fall-back puts rows there that floor below the
        // start: it winds local time back, and while local time reads below the start's own
        // wall time again the floor lands on the previous day's grid point. Under ANCHOR DAILY
        // '02:30' 'Europe/Berlin' the segment that starts at 2026-10-25T00:30Z has exactly
        // those rows above it, [2026-10-25T01:00Z, 2026-10-25T01:30Z) - they read 02:00..02:29
        // CET and carry 2026-10-24T00:30Z. A key whose run of that earlier value continues
        // there holds its state straight across the start, and a replay floored at it starts
        // every key from a reset, so this reports the segment open below instead.
        //
        // The offset is fixed between two transitions, so there local time increases with the
        // instant, its floor never decreases, and converting a later grid point back never
        // lands below an earlier one - two offsets in force within one period of each other
        // never differ by more than a whole period, and where they differ by exactly one, as
        // Pacific/Apia's did in 2011, the later grid point converts back to the same instant
        // as the earlier one. So
        // the lowest anchor value over the instants at or above the start is taken at the start
        // itself or at one of the transitions above it, and probing those covers the whole
        // tail. The walk is finite: a floor sits less than one period below the local time it
        // floors, and converting it back moves it by less than tzGuard against the instant's
        // own offset, so an instant that floors below the start sits below start + period +
        // tzGuard.
        final long localStart = floor.floor(
                timestamp + CommonUtils.getFloorUtcTzOffset(tzRules, timestamp, unit),
                stride,
                segmentOffset
        );
        final long localEnd = driver.add(localStart, addUnit, stride);
        if (localEnd <= localStart || !isTzRepresentable(localEnd)) {
            return Long.MIN_VALUE;
        }
        final long reach = localEnd - localStart + tzGuard;
        final long horizon = start > Long.MAX_VALUE - reach ? Long.MAX_VALUE : start + reach;
        for (long transition = tzRules.getNextDST(start);
             transition != Long.MAX_VALUE && transition < horizon;
             transition = tzRules.getNextDST(transition)) {
            if (!isTzRepresentable(transition) || tzFloor(transition) < start) {
                return Long.MIN_VALUE;
            }
        }
        return start;
    }
}
