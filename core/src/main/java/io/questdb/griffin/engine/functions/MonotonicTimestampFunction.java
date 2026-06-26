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
