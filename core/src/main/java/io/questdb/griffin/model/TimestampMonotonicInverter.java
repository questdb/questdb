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

package io.questdb.griffin.model;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.MonotonicTimestampFunction;
import io.questdb.griffin.engine.functions.UntypedFunction;
import io.questdb.std.Interval;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;

/**
 * Runtime helper that produces a designated-timestamp interval from a predicate
 * {@code g(ts) OP runtimeConst} where {@code g} is a chain of
 * {@link MonotonicTimestampFunction}s. The queried value range is built from the
 * resolved bound(s) and inverted layer by layer onto the timestamp axis.
 * <p>
 * Stored in {@link RuntimeIntervalModel}'s dynamic range list and evaluated under
 * {@link IntervalOperation#INTERSECT_INTERVALS}, mirroring
 * {@link CompiledTickExpression}.
 */
public class TimestampMonotonicInverter extends UntypedFunction {
    private final Function head;
    private final Function hiBound;
    private final short hiBoundAdjustment;
    private final long hiConst;
    private final Interval io = new Interval();
    private final boolean isBetween;
    private final Function loBound;
    private final short loBoundAdjustment;
    private final long loConst;
    private final TimestampDriver timestampDriver;

    public TimestampMonotonicInverter(
            Function head,
            Function loBound,
            short loBoundAdjustment,
            long loConst,
            Function hiBound,
            short hiBoundAdjustment,
            long hiConst,
            boolean isBetween,
            TimestampDriver timestampDriver
    ) {
        this.head = head;
        this.loBound = loBound;
        this.loBoundAdjustment = loBoundAdjustment;
        this.loConst = loConst;
        this.hiBound = hiBound;
        this.hiBoundAdjustment = hiBoundAdjustment;
        this.hiConst = hiConst;
        this.isBetween = isBetween;
        this.timestampDriver = timestampDriver;
    }

    @Override
    public void close() {
        // head, loBound and hiBound are distinct owners hidden inside the inverter; the outer
        // best-effort dynamic-range list cleanup only reaches this close(), not the functions it
        // holds. A close() failure on an earlier owner must not abandon the later ones, so free
        // every distinct owner best-effort (hiBound may alias loBound - free it once) and rethrow
        // the accumulated failure, mirroring the freeBorrowedModels convention.
        Throwable failure = Misc.freeBestEffort(null, head);
        failure = Misc.freeBestEffort(failure, loBound);
        if (hiBound != loBound) {
            failure = Misc.freeBestEffort(failure, hiBound);
        }
        CairoException.rethrowCleanupFailure(failure);
    }

    /**
     * Appends a single inverted {@code [lo, hi]} pair to {@code out}, or nothing
     * when the predicate matches no rows (a NULL bound, or an empty preimage). The
     * empty case relies on the enclosing {@code INTERSECT_INTERVALS} to reduce the
     * result to the empty set.
     */
    public void evaluate(LongList out) {
        long lo;
        if (loBound != null) {
            lo = resolveBound(loBound);
            if (lo == Numbers.LONG_NULL) {
                return;
            }
            if (loBoundAdjustment > 0 && lo == Long.MAX_VALUE) {
                // a strict '>' bound at the output-domain ceiling matches nothing (no value exceeds
                // Long.MAX_VALUE), so the predicate is unsatisfiable regardless of the chain. Without
                // this guard the +1 that turns '>' into a closed '>= lo+1' wraps to Numbers.LONG_NULL
                // (the open-lower sentinel), opening the interval to the whole storable domain and
                // scanning every row before the residual rejects them all. Mirrors the guard in
                // RuntimeIntervalModel. The symmetric strict '<' bound at Long.MIN_VALUE+1 is NOT
                // guarded here: '< lo' can still be satisfied by a forward-shift that overflows the
                // long boundary (reachable for uncapped nanos timestamps), so it must stay with the
                // chain inversion, which declines (NONE) and leaves it to the residual filter.
                return;
            }
            lo += loBoundAdjustment;
        } else {
            lo = loConst;
        }

        long hi;
        if (hiBound != null) {
            hi = resolveBound(hiBound);
            if (hi == Numbers.LONG_NULL) {
                return;
            }
            hi += hiBoundAdjustment;
        } else {
            hi = hiConst;
        }

        // BETWEEN tolerates reversed bounds; every bound here is a point, so normalizing is exact
        if (isBetween && lo > hi) {
            final long t = lo;
            lo = hi;
            hi = t;
        }

        io.of(lo, hi);
        // Traverse the owned head's linked monotonic chain (head -> getTimestampArg() -> ...) rather
        // than a copied ObjList. The links hold the same functions in the same outermost-first order
        // the compile path built, so inversion is identical, and the traversal allocates nothing.
        Function f = head;
        while (f instanceof MonotonicTimestampFunction m) {
            if (m.invertTimestampInterval(io) == MonotonicTimestampFunction.NONE) {
                // bound outside the invertible range: impose no interval, leaving it to the filter
                out.add(Numbers.LONG_NULL, Long.MAX_VALUE);
                return;
            }
            if (io.getLo() > io.getHi()) {
                return;
            }
            f = m.getTimestampArg();
        }
        if (io.getLo() <= io.getHi()) {
            out.add(io.getLo(), io.getHi());
        }
    }

    @Override
    public int getType() {
        return ColumnType.UNDEFINED;
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        if (loBound != null) {
            loBound.init(symbolTableSource, executionContext);
        }
        if (hiBound != null && hiBound != loBound) {
            hiBound.init(symbolTableSource, executionContext);
        }
    }

    @Override
    public boolean isNonDeterministic() {
        return true;
    }

    @Override
    public boolean isRuntimeConstant() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val("monotonic_ts_interval");
    }

    private long resolveBound(Function f) {
        final int type = f.getType();
        if (ColumnType.isTimestamp(type)) {
            final long v = f.getTimestamp(null);
            return v == Numbers.LONG_NULL ? Numbers.LONG_NULL : timestampDriver.from(v, ColumnType.getTimestampType(type));
        }
        // int and long bounds both read as long: IntFunction.getLong() widens and
        // maps INT_NULL to LONG_NULL, the no-rows sentinel checked by the caller.
        return f.getLong(null);
    }
}
