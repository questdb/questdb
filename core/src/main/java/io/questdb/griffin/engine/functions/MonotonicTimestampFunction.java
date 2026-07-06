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

package io.questdb.griffin.engine.functions;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.std.Interval;
import io.questdb.std.Numbers;
import io.questdb.std.datetime.TimeZoneRules;

/**
 * Implemented by functions that are monotonic in their single designated-timestamp
 * argument, so that a predicate {@code g(ts) OP const} can be turned into a bound
 * on {@code ts} for interval-based partition pruning.
 * <p>
 * The bound is derived by walking the chain of these functions from the outermost
 * wrapper inward to the designated-timestamp column, inverting the queried value
 * range one layer at a time via {@link #invertTimestampInterval(Interval)}.
 */
public interface MonotonicTimestampFunction {

    /**
     * Cannot bound the argument; the predicate must stay a row filter.
     */
    int NONE = 0;
    /**
     * The inverted interval soundly contains the preimage but may include extra
     * timestamps; the predicate must be kept as a residual filter.
     */
    int SUPERSET = 1;
    /**
     * The inverted interval equals the preimage exactly; the predicate may be
     * dropped from the row filter.
     */
    int EXACT = 2;

    /**
     * Inverts {@code [lo, hi]} for a function whose result is {@code arg + shift}, a constant
     * offset applied to every timestamp: the inverse subtracts {@code shift} back. Returns
     * {@link #EXACT}, or {@link #NONE} when subtracting {@code shift} would overflow the long
     * boundary or when the forward shift wraps part of the preimage out of a single interval.
     * {@code maxTimestamp} is the ceiling on this shift's input (see {@link #shiftInputCeiling}):
     * it decides whether the forward shift can actually reach a storable overflow value.
     */
    static int invertConstantShift(Interval io, long shift, long maxTimestamp) {
        final long inLo = io.getLo();
        final long inHi = io.getHi();
        if (shiftWrapsIntoRange(shift, inLo, inHi, maxTimestamp)) {
            return NONE;
        }
        long lo = inLo;
        long hi = inHi;
        if (lo != Numbers.LONG_NULL) {
            if ((shift > 0 && lo < Long.MIN_VALUE + shift) || (shift < 0 && lo > Long.MAX_VALUE + shift)) {
                return NONE;
            }
            lo -= shift;
        } else if (shift < 0) {
            lo = Long.MIN_VALUE - shift;
        }
        if (hi != Long.MAX_VALUE) {
            if ((shift > 0 && hi < Long.MIN_VALUE + shift) || (shift < 0 && hi > Long.MAX_VALUE + shift)) {
                return NONE;
            }
            hi -= shift;
        } else if (shift > 0) {
            hi = Long.MAX_VALUE - shift;
        }
        io.of(lo, hi);
        return EXACT;
    }

    /**
     * Inverts {@code [lo, hi]} for a named-zone function whose result is
     * {@code arg + shiftSign * offset(arg)} ({@code to_timezone} uses {@code -1},
     * {@code to_utc} uses {@code +1}). Grades {@link #EXACT} when the zone offset is
     * provably constant across the preimage, otherwise {@link #SUPERSET} (a symmetric
     * 24h widening that bounds any real offset), or {@link #NONE} when widening overflows.
     */
    static int invertZoneOffsetShift(Interval io, TimeZoneRules tzRules, TimestampDriver timestampDriver, int shiftSign) {
        final long margin = timestampDriver.fromHours(24);
        final long lo = io.getLo();
        final long hi = io.getHi();
        final boolean isLoFinite = lo != Numbers.LONG_NULL;
        final boolean isHiFinite = hi != Long.MAX_VALUE;

        if ((isLoFinite || isHiFinite) && tryZoneOffsetExact(io, tzRules, margin, shiftSign, lo, hi, isLoFinite, isHiFinite)) {
            return EXACT;
        }

        long widenedLo = lo;
        long widenedHi = hi;
        if (isLoFinite) {
            if (lo < Long.MIN_VALUE + margin) {
                return NONE;
            }
            widenedLo = lo - margin;
        }
        if (isHiFinite) {
            if (hi > Long.MAX_VALUE - margin) {
                return NONE;
            }
            widenedHi = hi + margin;
        }
        io.of(widenedLo, widenedHi);
        return SUPERSET;
    }

    /**
     * Returns true when the forward function {@code arg + shift} can overflow the {@code long}
     * boundary into {@code [lo, hi]} in a way that splits the preimage. The designated timestamp is
     * non-negative, so only a positive shift can overflow (a negative one stays well above
     * {@code Long.MIN_VALUE}); the overflow wraps to a low value. The wrapped value only splits the
     * preimage when the upper bound is finite -- with an open upper bound the wrap merges
     * contiguously with the non-wrapping range. A split cannot be captured by a single interval, so
     * the inverse must decline ({@link #NONE}) and leave the predicate a row filter.
     * <p>
     * The split can only carry real rows if a <em>storable</em> timestamp overflows. The write path
     * ({@code TableWriter}/{@code WalWriter.validateBounds}) caps every designated timestamp at
     * {@code maxTimestamp}, so overflow is reachable only when {@code maxTimestamp + shift} itself
     * wraps, i.e. {@code shift > Long.MAX_VALUE - maxTimestamp}. For micros the ceiling is
     * {@code 9999-12-31}, leaving a gap of ~284000 years, so no realistic stride overflows and
     * {@code dateadd('<fixed>', +stride, ts) < / <= bound} still prunes exactly. Nanos are not
     * capped ({@code maxTimestamp == Long.MAX_VALUE}), so any positive shift is treated as wrapping
     * and the predicate stays a row filter. Do not drop this ceiling check -- without it a
     * {@code ts} whose {@code ts + stride} overflows negative satisfies the predicate but would be
     * wrongly pruned away.
     */
    static boolean shiftWrapsIntoRange(long shift, long lo, long hi, long maxTimestamp) {
        return shift > 0
                && hi != Long.MAX_VALUE
                && lo <= Long.MIN_VALUE + (shift - 1)
                && shift > Long.MAX_VALUE - maxTimestamp;
    }

    private static boolean tryZoneOffsetExact(
            Interval io,
            TimeZoneRules tzRules,
            long margin,
            int shiftSign,
            long lo,
            long hi,
            boolean isLoFinite,
            boolean isHiFinite
    ) {
        final long windowLo;
        final long windowHi;
        final long ref;
        if (isLoFinite && isHiFinite) {
            if (lo < Long.MIN_VALUE + margin || hi > Long.MAX_VALUE - margin) {
                return false;
            }
            windowLo = lo - margin;
            windowHi = hi + margin;
            ref = lo;
        } else if (isLoFinite) {
            if (lo < Long.MIN_VALUE + margin || lo > Long.MAX_VALUE - margin) {
                return false;
            }
            windowLo = lo - margin;
            windowHi = lo + margin;
            ref = lo;
        } else {
            if (hi < Long.MIN_VALUE + margin || hi > Long.MAX_VALUE - margin) {
                return false;
            }
            windowLo = hi - margin;
            windowHi = hi + margin;
            ref = hi;
        }
        // A transition inside the +/-24h window means the offset is not constant across the preimage.
        if (tzRules.getNextDST(windowLo) <= windowHi) {
            return false;
        }
        final long offset = shiftSign * tzRules.getOffset(ref);
        io.of(
                isLoFinite ? lo + offset : (offset > 0 ? Long.MIN_VALUE + offset : Numbers.LONG_NULL),
                isHiFinite ? hi + offset : (offset < 0 ? Long.MAX_VALUE + offset : Long.MAX_VALUE)
        );
        return true;
    }

    /**
     * Returns the argument that carries the timestamp. Every other argument must
     * be constant or runtime-constant.
     */
    Function getTimestampArg();

    /**
     * Returns the ceiling on this shift function's input for the overflow check in
     * {@link #invertConstantShift}. Only the innermost shift sits directly on the designated
     * timestamp, whose stored values {@code TableWriter}/{@code WalWriter.validateBounds} caps at
     * the domain ceiling; there the driver ceiling ({@link TimestampDriver#getMaxDesignatedTimestamp})
     * bounds the wrap exactly. An outer shift's input is an inner sub-expression whose value can
     * exceed that ceiling by the accumulated inner shifts, so the driver ceiling would understate the
     * reachable range; there this returns {@code Long.MAX_VALUE}, which makes any positive shift
     * decline (the conservative, always-sound choice). Without this distinction a chain of shifts
     * whose sum overflows -- but whose per-layer shifts each stay under the gap -- would wrongly
     * prune and drop matching rows.
     */
    default long shiftInputCeiling(int timestampType) {
        return getTimestampArg() instanceof MonotonicTimestampFunction
                ? Long.MAX_VALUE
                : ColumnType.getTimestampDriver(timestampType).getMaxDesignatedTimestamp();
    }

    /**
     * Inverts a value range of this function's result into a range on its timestamp
     * argument.
     * <p>
     * On entry {@code io} holds the closed interval {@code [lo, hi]} that this
     * function's result must fall in, expressed in this function's output domain.
     * On exit {@code io} holds the interval the timestamp argument must fall in.
     * {@link io.questdb.std.Numbers#LONG_NULL} is the open lower bound and
     * {@link Long#MAX_VALUE} the open upper bound; an empty result is encoded as
     * {@code lo > hi}.
     * <p>
     * The returned grade ({@link #EXACT}, {@link #SUPERSET} or {@link #NONE}) may
     * depend on the bound values (a named-zone function is EXACT only for bounds that
     * fall within a single DST segment). Probing with an unbounded interval must yield
     * a sound, conservative grade -- never more optimistic than any bounded interval --
     * which is what the runtime-deferred path relies on.
     */
    int invertTimestampInterval(Interval io);
}
