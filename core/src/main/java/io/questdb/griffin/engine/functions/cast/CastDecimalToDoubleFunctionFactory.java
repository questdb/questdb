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
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.decimal.Decimal64LoaderFunctionFactory;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimal64;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;

public class CastDecimalToDoubleFunctionFactory implements FunctionFactory {
    // 10^22 is the largest power of ten a double holds exactly.
    private static final double[] POW10 = {
            1E0, 1E1, 1E2, 1E3, 1E4, 1E5, 1E6, 1E7, 1E8, 1E9, 1E10, 1E11,
            1E12, 1E13, 1E14, 1E15, 1E16, 1E17, 1E18, 1E19, 1E20, 1E21, 1E22
    };
    private static final long MAX_EXACT_UNSCALED = 1L << 53;
    private static final int MAX_EXACT_SCALE = POW10.length - 1;

    @Override
    public String getSignature() {
        return "cast(Ξd)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final Function arg = args.getQuick(0);
        return switch (ColumnType.tagOf(arg.getType())) {
            case ColumnType.DECIMAL8, ColumnType.DECIMAL16, ColumnType.DECIMAL32, ColumnType.DECIMAL64 ->
                    new Func64(Decimal64LoaderFunctionFactory.getInstance(arg));
            case ColumnType.DECIMAL128 -> new Func128(arg);
            default -> new Func(arg);
        };
    }

    static double toDouble(StringSink sink, Decimal128 value, int scale, int precision) {
        final long high = value.getHigh();
        final long low = value.getLow();
        if (isExact(low, scale) && high == (low >> 63)) {
            return (double) low / POW10[scale];
        }
        sink.clear();
        Decimal128.toSink(sink, high, low, scale, precision);
        return Numbers.parseDouble(sink);
    }

    static double toDouble(StringSink sink, Decimal256 value, int scale, int precision) {
        final long hh = value.getHh();
        final long hl = value.getHl();
        final long lh = value.getLh();
        final long ll = value.getLl();
        final long signExtension = ll >> 63;
        if (isExact(ll, scale) && hh == signExtension && hl == signExtension && lh == signExtension) {
            return (double) ll / POW10[scale];
        }
        sink.clear();
        Decimal256.toSink(sink, hh, hl, lh, ll, scale, precision);
        return Numbers.parseDouble(sink);
    }

    static double toDouble(StringSink sink, long value, int scale, int precision) {
        if (isExact(value, scale)) {
            return (double) value / POW10[scale];
        }
        sink.clear();
        Decimal64.toSink(sink, value, scale, precision);
        return Numbers.parseDouble(sink);
    }

    /**
     * When the unscaled value and 10^scale are both exact doubles the quotient is rounded once,
     * so dividing them gives the same double as parsing the decimal text.
     */
    private static boolean isExact(long unscaled, int scale) {
        return scale <= MAX_EXACT_SCALE && unscaled >= -MAX_EXACT_UNSCALED && unscaled <= MAX_EXACT_UNSCALED;
    }

    private static class Func extends AbstractCastToDoubleFunction {
        private final Decimal256 decimal256 = new Decimal256();
        private final int fromPrecision;
        private final int fromScale;
        private final StringSink sink = new StringSink();

        public Func(Function value) {
            super(value);
            int type = value.getType();
            fromScale = ColumnType.getDecimalScale(type);
            fromPrecision = ColumnType.getDecimalPrecision(type);
        }

        @Override
        public double getDouble(Record rec) {
            arg.getDecimal256(rec, decimal256);
            if (decimal256.isNull()) {
                return Double.NaN;
            }
            return toDouble(sink, decimal256, fromScale, fromPrecision);
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }
    }

    private static class Func128 extends AbstractCastToDoubleFunction {
        private final Decimal128 decimal128 = new Decimal128();
        private final int fromPrecision;
        private final int fromScale;
        private final StringSink sink = new StringSink();

        public Func128(Function value) {
            super(value);
            int type = arg.getType();
            this.fromScale = ColumnType.getDecimalScale(type);
            this.fromPrecision = ColumnType.getDecimalPrecision(type);
        }

        @Override
        public double getDouble(Record rec) {
            arg.getDecimal128(rec, decimal128);
            if (decimal128.isNull()) {
                return Double.NaN;
            }
            return toDouble(sink, decimal128, fromScale, fromPrecision);
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }
    }

    private static class Func64 extends AbstractCastToDoubleFunction {
        private final int fromPrecision;
        private final int fromScale;
        private final StringSink sink = new StringSink();

        public Func64(Function value) {
            super(value);
            int type = arg.getType();
            this.fromPrecision = ColumnType.getDecimalPrecision(type);
            this.fromScale = ColumnType.getDecimalScale(type);
        }

        @Override
        public double getDouble(Record rec) {
            long v = arg.getDecimal64(rec);
            if (Decimal64.isNull(v)) {
                return Double.NaN;
            }
            return toDouble(sink, v, fromScale, fromPrecision);
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }
    }
}
