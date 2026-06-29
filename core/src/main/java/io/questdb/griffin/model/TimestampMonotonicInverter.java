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
import io.questdb.std.ObjList;

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
    private final ObjList<MonotonicTimestampFunction> chain;
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
            ObjList<MonotonicTimestampFunction> chain,
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
        this.chain = chain;
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
        Misc.free(head);
        Misc.free(loBound);
        if (hiBound != loBound) {
            Misc.free(hiBound);
        }
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
        for (int i = 0, n = chain.size(); i < n; i++) {
            if (chain.getQuick(i).invertTimestampInterval(io) == MonotonicTimestampFunction.NONE) {
                // bound outside the invertible range: impose no interval, leaving it to the filter
                out.add(Numbers.LONG_NULL, Long.MAX_VALUE);
                return;
            }
            if (io.getLo() > io.getHi()) {
                return;
            }
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
