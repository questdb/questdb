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

package io.questdb.griffin.engine;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.Nullable;

/**
 * The bounds of a LIMIT, {@code LIMIT lo, hi}, or {@code LIMIT lo} where the hi function is null, as execution applies
 * them, which every LIMIT operator and the planner resolve through {@link #of}, and the rows a sort under the LIMIT
 * keeps for them. Owns the functions.
 */
public final class LimitBounds implements QuietCloseable {
    private final int position;
    private long hi;
    private @Nullable Function hiFunction;
    private long lo;
    private Function loFunction;

    /**
     * {@code position}: the position of the LIMIT in the query text, which {@link #init} reports its error at.
     */
    public LimitBounds(Function loFunction, @Nullable Function hiFunction, int position) {
        this.loFunction = loFunction;
        this.hiFunction = hiFunction;
        this.position = position;
    }

    /**
     * Bounds without functions, which the caller resolves from values through {@link #of}.
     */
    public LimitBounds() {
        this(null, null, 0);
    }

    /**
     * Whether {@link #of} applies the bounds of {@code LIMIT lo, hi} as {@code LIMIT hi, lo}: the {@link #rowCount} is
     * a constant and {@code lo} exceeds {@code hi}.
     */
    public static boolean isDescending(long lo, long hi) {
        return lo > hi && rowCount(lo, hi) > -1;
    }

    /**
     * The most rows {@code LIMIT lo, hi} returns, as {@link #of} resolves the bounds, which reads {@code LIMIT n} as
     * {@code LIMIT n, 0}: the distance between the bounds where both are non-negative or both negative, or where
     * {@code lo} is negative and {@code hi} is zero. -1 where the count is not a constant: a non-negative {@code lo}
     * with a negative {@code hi} ends the rows that many rows before the last row of the input, {@link #of} rejects a
     * negative {@code lo} with a positive {@code hi}, and the distance from {@link Long#MIN_VALUE} to zero exceeds a
     * long.
     */
    public static long rowCount(long lo, long hi) {
        final long count = Math.abs(hi - lo);
        return count > -1 && (Numbers.sameSign(lo, hi) || lo < 0 && hi == 0) ? count : -1;
    }

    @Override
    public void close() {
        CairoException.rethrowCleanupFailure(close(null));
    }

    /**
     * Frees the functions, folding each failure into {@code cleanupFailure}, which it returns.
     */
    public Throwable close(@Nullable Throwable cleanupFailure) {
        final Function loFunction = this.loFunction;
        this.loFunction = null;
        final Function hiFunction = this.hiFunction;
        this.hiFunction = null;
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, loFunction);
        return hiFunction != loFunction ? Misc.freeBestEffort(cleanupFailure, hiFunction) : cleanupFailure;
    }

    public long getHi() {
        return hi;
    }

    public @Nullable Function getHiFunction() {
        return hiFunction;
    }

    /**
     * The rows of its order a sort keeps: the first ones where {@link #isFirstN} holds, the last ones otherwise, none
     * where 0, and every row where negative.
     */
    public long getLimit() {
        if (lo < 0) {
            return lo == hi ? 0 : -lo;
        }
        if (hi < 0) {
            return hi == Numbers.LONG_NULL ? 0 : -1;
        }
        return hi;
    }

    public long getLo() {
        return lo;
    }

    public Function getLoFunction() {
        return loFunction;
    }

    /**
     * The rows a sort skips at the start of the rows it keeps.
     */
    public long getSkipFirst() {
        return lo >= 0 && hi != Numbers.LONG_NULL ? lo : 0;
    }

    /**
     * The rows a sort skips at the end of the rows it keeps.
     */
    public long getSkipLast() {
        if (lo < 0) {
            return lo == hi ? 0 : Math.max(-hi, 0);
        }
        return hi < 0 && hi != Numbers.LONG_NULL ? -hi : 0;
    }

    /**
     * Initializes the functions over {@code symbolTableSource} and resolves the bounds from their values, see
     * {@link #of}.
     */
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        loFunction.init(symbolTableSource, executionContext);
        if (hiFunction != null) {
            hiFunction.init(symbolTableSource, executionContext);
        }
        of(loFunction.getLong(null), hiFunction != null ? hiFunction.getLong(null) : 0, hiFunction != null, position);
    }

    /**
     * Whether a sort keeps the first rows of its order: both bounds are non-negative.
     */
    public boolean isFirstN() {
        return lo >= 0 && hi >= 0;
    }

    public boolean isNonDeterministic() {
        return loFunction.isNonDeterministic() || hiFunction != null && hiFunction.isNonDeterministic();
    }

    /**
     * Reads {@code LIMIT lo, hi}, or {@code LIMIT lo} as {@code LIMIT lo, 0} without {@code hasHi}. A NULL {@code lo}
     * of a pair reads 0 where {@code hi} is not NULL, and the bounds apply as {@code LIMIT hi, lo} where
     * {@link #isDescending} holds. Rejects a negative {@code lo} with a positive {@code hi} at {@code position}, and
     * keeps the bounds it held then.
     */
    public void of(long lo, long hi, boolean hasHi, int position) throws SqlException {
        if (!tryOf(lo, hi, hasHi)) {
            throw SqlException.$(position, "LIMIT <negative>, <positive> is not allowed");
        }
    }

    /**
     * Resolves the bounds from the current values of the functions as {@link #of} does, and returns false where
     * {@link #of} rejects them.
     */
    public boolean resolve() {
        return tryOf(loFunction.getLong(null), hiFunction != null ? hiFunction.getLong(null) : 0, hiFunction != null);
    }

    /**
     * Adds the functions to the plan of a sort under the LIMIT.
     */
    public void toPlan(PlanSink sink) {
        sink.meta("lo").val(loFunction);
        if (hiFunction != null) {
            sink.meta("hi").val(hiFunction);
        }
    }

    private boolean tryOf(long lo, long hi, boolean hasHi) {
        if (!hasHi) {
            hi = 0;
        } else if (lo == Numbers.LONG_NULL && hi != Numbers.LONG_NULL) {
            lo = 0;
        }
        final boolean isDescending = isDescending(lo, hi);
        final long first = isDescending ? hi : lo;
        final long second = isDescending ? lo : hi;
        if (first < 0 && second > 0) {
            return false;
        }
        this.lo = first;
        this.hi = second;
        return true;
    }
}
