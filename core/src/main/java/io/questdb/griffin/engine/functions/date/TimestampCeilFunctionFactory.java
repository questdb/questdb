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

package io.questdb.griffin.engine.functions.date;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.MonotonicTimestampFunction;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.Interval;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.CommonUtils;

public class TimestampCeilFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "timestamp_ceil(sN)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) throws SqlException {
        final Function kind = args.getQuick(0);

        final char c = kind.getChar(null);
        switch (c) {
            case 'd':
            case 'M':
            case 'y':
            case 'w':
            case 'h':
            case 'm':
            case 's':
            case 'T':
            case 'U':
            case 'n':
                return new TimestampCeilFunction(args.getQuick(1), c, ColumnType.getHigherPrecisionTimestampType(ColumnType.getTimestampType(args.getQuick(1).getType()), ColumnType.TIMESTAMP_MICRO));
            case 0:
                throw SqlException.position(argPositions.getQuick(0)).put("invalid unit 'null'");
            default:
                throw SqlException.position(argPositions.getQuick(0)).put("invalid unit '").put(c).put('\'');
        }
    }

    static class TimestampCeilFunction extends TimestampFunction implements UnaryFunction, MonotonicTimestampFunction {
        private final Function arg;
        private final TimestampDriver.TimestampCeilMethod ceil;
        private final long fixedSize;
        private final char symbol;

        public TimestampCeilFunction(Function arg, char symbol, int timestampType) {
            super(timestampType);
            this.ceil = timestampDriver.getTimestampCeilMethod(symbol);
            this.arg = arg;
            this.symbol = symbol;
            // Only fixed-size, epoch-aligned units have boundaries at integer
            // multiples of the bucket size, which the arithmetic inverse needs.
            this.fixedSize = CommonUtils.isFixedAlignedUnit(symbol) ? ceil.ceil(0) : 0;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public final long getTimestamp(Record rec) {
            long ts = arg.getTimestamp(rec);
            return ts == Numbers.LONG_NULL ? Numbers.LONG_NULL : ceil.ceil(ts);
        }

        @Override
        public Function getTimestampArg() {
            return arg;
        }

        @Override
        public int invertTimestampInterval(Interval io) {
            if (fixedSize <= 0) {
                return NONE;
            }
            long lo = io.getLo();
            long hi = io.getHi();
            if (lo != Numbers.LONG_NULL) {
                final long ql = Numbers.ceilDiv(lo, fixedSize) - 1;
                if (mulOverflows(ql, fixedSize)) {
                    return NONE;
                }
                lo = ql * fixedSize;
            } else if (hi != Long.MAX_VALUE) {
                // ceil rounds the top partial bucket past the domain max, wrapping to a low value;
                // with an open lower but finite upper bound that wrapped value matches and splits
                // the preimage into two disjoint ranges
                return NONE;
            }
            // an open upper end is capped at the largest timestamp whose ceil stays in-domain,
            // excluding the wrapping top bucket
            final long qh = Math.floorDiv(hi, fixedSize);
            if (mulOverflows(qh, fixedSize)) {
                return NONE;
            }
            final long prod = qh * fixedSize;
            if (prod == Long.MIN_VALUE) {
                return NONE;
            }
            hi = prod - 1;
            io.of(lo, hi);
            return EXACT;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("timestamp_ceil('").val(symbol).val("',").val(arg).val(')');
        }

        private static boolean mulOverflows(long a, long b) {
            if (a == 0) {
                return false;
            }
            final long r = a * b;
            return r / b != a;
        }
    }
}
