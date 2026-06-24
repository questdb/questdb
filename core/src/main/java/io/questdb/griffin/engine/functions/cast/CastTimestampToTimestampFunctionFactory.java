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

package io.questdb.griffin.engine.functions.cast;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.MonotonicTimestampFunction;
import io.questdb.std.IntList;
import io.questdb.std.Interval;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

public class CastTimestampToTimestampFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "cast(Nn)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        final Function arg = args.getQuick(0);
        final int leftTimestampType = arg.getType();
        final int rightTimestampType = args.getQuick(1).getType();
        if (ColumnType.isTimestamp(leftTimestampType)) {
            if (leftTimestampType == rightTimestampType) {
                // Identity cast: arg is already a timestamp of the target type.
                // Return it unwrapped so that callers inspecting the argument
                // (e.g. ColumnFunction.unwrap for twap(price, ts::timestamp))
                // can still recognize a plain designated-timestamp column.
                return arg;
            }
            return new Func(arg, leftTimestampType, rightTimestampType);
        }
        // Non-timestamp source (e.g. long): reinterpret its raw value as the
        // target timestamp type.
        return new CastLongToTimestampFunctionFactory.Func(arg, rightTimestampType);
    }

    public static class Func extends AbstractCastToTimestampFunction implements MonotonicTimestampFunction {
        private static final long NANOS_PER_MICRO = 1000;
        private final int leftTimestampType;

        public Func(Function arg, int leftTimestampType, int rightTimestampType) {
            super(arg, rightTimestampType);
            this.leftTimestampType = leftTimestampType;
        }

        @Override
        public long getTimestamp(Record rec) {
            return timestampDriver.from(arg.getLong(rec), leftTimestampType);
        }

        @Override
        public Function getTimestampArg() {
            return getArg();
        }

        @Override
        public int invertTimestampInterval(Interval io) {
            long lo = io.getLo();
            long hi = io.getHi();
            final int outType = getType();
            if (ColumnType.isTimestampNano(outType) && ColumnType.isTimestampMicro(leftTimestampType)) {
                if (lo != Numbers.LONG_NULL) {
                    lo = ceilDiv(lo, NANOS_PER_MICRO);
                }
                if (hi != Long.MAX_VALUE) {
                    hi = Math.floorDiv(hi, NANOS_PER_MICRO);
                }
            } else if (ColumnType.isTimestampMicro(outType) && ColumnType.isTimestampNano(leftTimestampType)) {
                // this inverse runs only when the cast wraps the designated timestamp, which
                // TableWriter rejects before 1970-01-01, so the nano source is always >= 0 and
                // nanos/1000 truncates toward zero exactly as floor -- the inverse is exact
                if (lo != Numbers.LONG_NULL) {
                    if (lo > Long.MAX_VALUE / NANOS_PER_MICRO || lo < Long.MIN_VALUE / NANOS_PER_MICRO) {
                        return NONE;
                    }
                    lo = lo * NANOS_PER_MICRO;
                }
                if (hi != Long.MAX_VALUE) {
                    if (hi > (Long.MAX_VALUE - (NANOS_PER_MICRO - 1)) / NANOS_PER_MICRO || hi < Long.MIN_VALUE / NANOS_PER_MICRO) {
                        return NONE;
                    }
                    hi = hi * NANOS_PER_MICRO + (NANOS_PER_MICRO - 1);
                }
            } else {
                return NONE;
            }
            io.of(lo, hi);
            return EXACT;
        }

        private static long ceilDiv(long a, long b) {
            return -Math.floorDiv(-a, b);
        }
    }
}
