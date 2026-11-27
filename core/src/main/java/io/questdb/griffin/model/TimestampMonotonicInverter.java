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
 * The range comes either from point bounds, or from a {@link CompiledTickExpression} for
 * {@code g(ts) IN 'tick'}, when the TICK expression holds a date variable and so resolves
 * to different intervals on every execution. Those intervals are inverted as a list, and where
 * every function of the chain inverts them exactly, the query keeps no residual filter, see
 * {@link #invertTickIntervals}.
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
    // whether the query keeps no residual filter, so the intervals of the TICK expression must invert exactly
    private final boolean isExact;
    private final Function loBound;
    private final short loBoundAdjustment;
    private final long loConst;
    // the position of the TICK expression, for the error of an inversion that is not exact
    private final int position;
    private final CompiledTickExpression tickExpr;
    private final LongList tickIntervals;
    private final TimestampDriver timestampDriver;
    private boolean isLenient;

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
        this.tickExpr = null;
        this.tickIntervals = null;
        this.isExact = false;
        this.position = 0;
    }

    /**
     * Inverts {@code g(ts) IN 'tick'}, where {@code tickExpr} is the compiled TICK expression,
     * resolved in the output domain of the chain.
     *
     * @param isExact  whether the query keeps no residual filter for the predicate, which the
     *                 caller decides with {@link #invertTickIntervals}: an inversion that is not
     *                 exact when the scan opens then fails the query
     * @param position the position of the TICK expression in the SQL text
     */
    public TimestampMonotonicInverter(
            Function head,
            CompiledTickExpression tickExpr,
            TimestampDriver timestampDriver,
            boolean isExact,
            int position
    ) {
        this.head = head;
        this.tickExpr = tickExpr;
        this.tickIntervals = new LongList();
        this.timestampDriver = timestampDriver;
        this.isExact = isExact;
        this.position = position;
        this.loBound = null;
        this.loBoundAdjustment = 0;
        this.loConst = Long.MIN_VALUE;
        this.hiBound = null;
        this.hiBoundAdjustment = 0;
        this.hiConst = Long.MAX_VALUE;
        this.isBetween = false;
    }

    /**
     * Inverts the intervals of a TICK expression, in the output domain of the chain of
     * {@link MonotonicTimestampFunction}s that {@code head} starts, onto its timestamp, in place,
     * one function at a time from the outermost: on exit {@code intervals} holds the intervals
     * of the timestamp whose results fall in any of them, in ascending order and apart. A
     * duration of zero or less leaves an interval that ends before it starts, which selects no
     * timestamp. Returns the least exact grade of the functions, see
     * {@link MonotonicTimestampFunction#invertTimestampIntervals}; after
     * {@link MonotonicTimestampFunction#NONE} the intervals are undefined.
     *
     * @param scratch an interval that the method may overwrite
     */
    public static int invertTickIntervals(Function head, LongList intervals, Interval scratch) {
        int writeIndex = 0;
        for (int i = 0, n = intervals.size(); i < n; i += 2) {
            final long lo = intervals.getQuick(i);
            final long hi = intervals.getQuick(i + 1);
            if (lo <= hi) {
                intervals.setQuick(writeIndex++, lo);
                intervals.setQuick(writeIndex++, hi);
            }
        }
        intervals.setPos(writeIndex);
        int grade = MonotonicTimestampFunction.EXACT;
        Function f = head;
        while (intervals.size() > 0 && f instanceof MonotonicTimestampFunction m) {
            final int functionGrade = m.invertTimestampIntervals(intervals, 0, scratch);
            if (functionGrade == MonotonicTimestampFunction.NONE) {
                return MonotonicTimestampFunction.NONE;
            }
            if (functionGrade == MonotonicTimestampFunction.SUPERSET) {
                grade = MonotonicTimestampFunction.SUPERSET;
            }
            f = m.getTimestampArg();
        }
        return grade;
    }

    @Override
    public void close() {
        // head, loBound, hiBound and tickExpr are distinct owners hidden inside the inverter; the
        // outer best-effort dynamic-range list cleanup only reaches this close(), not the functions
        // it holds. A close() failure on an earlier owner must not abandon the later ones, so free
        // every distinct owner best-effort (hiBound may alias loBound - free it once) and rethrow
        // the accumulated failure, mirroring the freeBorrowedModels convention.
        Throwable failure = Misc.freeBestEffort(null, head);
        failure = Misc.freeBestEffort(failure, loBound);
        if (hiBound != loBound) {
            failure = Misc.freeBestEffort(failure, hiBound);
        }
        failure = Misc.freeBestEffort(failure, tickExpr);
        CairoException.rethrowCleanupFailure(failure);
    }

    /**
     * Appends the inverted intervals to {@code out}, in ascending order and apart: a single
     * {@code [lo, hi]} pair for point bounds, the inversion of each interval for a TICK
     * expression, or nothing when the predicate matches no rows (a NULL bound, an empty
     * preimage, or a TICK expression that resolves to no interval). The empty case relies on
     * the enclosing {@code INTERSECT_INTERVALS} to reduce the result to the empty set.
     */
    public void evaluate(LongList out) {
        if (tickExpr != null) {
            evaluateTickExpr(out);
            return;
        }

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
        invert(out);
    }

    @Override
    public int getType() {
        return ColumnType.UNDEFINED;
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        if (tickExpr != null) {
            tickExpr.init(symbolTableSource, executionContext);
            isLenient = IntervalUtils.isTickEvaluationLenient(executionContext);
        }
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

    private void evaluateTickExpr(LongList out) {
        tickIntervals.clear();
        tickExpr.evaluate(tickIntervals);
        // the expression may resolve to no interval at all, and then the predicate matches no rows
        final int grade = invertTickIntervals(head, tickIntervals, io);
        if (grade == MonotonicTimestampFunction.EXACT) {
            out.add(tickIntervals);
            return;
        }
        if (!isExact) {
            if (grade == MonotonicTimestampFunction.SUPERSET) {
                // the residual filter selects the rows of the wider intervals
                out.add(tickIntervals);
            } else {
                // bounds outside the invertible range: impose no interval, leaving the rows to the
                // residual filter
                out.add(Numbers.LONG_NULL, Long.MAX_VALUE);
            }
            return;
        }
        // The query has no residual filter to select the rows of a wider interval from, as the
        // intervals of the time it compiled at inverted exactly, and only an overflow at the end
        // of the range makes those of another time not. SQL that the engine runs on its own has no
        // caller to report it to, and selects nothing, as for a value that does not parse.
        if (isLenient) {
            return;
        }
        throw CairoException.nonCritical().position(position).put("TICK interval is out of range of the timestamp function");
    }

    private void invert(LongList out) {
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
