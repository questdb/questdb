/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.DecimalUtil;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.constants.IntConstant;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

import java.math.RoundingMode;

public class CastDecimalToIntFunctionFactory implements FunctionFactory {

    public static Function newInstance(
            int position,
            Function arg,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        if (arg.isConstant()) {
            return newConstantInstance(sqlExecutionContext, position, arg);
        }

        final int fromType = arg.getType();
        final int fromScale = ColumnType.getDecimalScale(fromType);
        if (fromScale == 0) {
            return newUnscaledInstance(position, fromType, arg);
        }

        return new ScaledDecimalFunction(arg, position);
    }

    @Override
    public String getSignature() {
        return "cast(Ξi)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        return newInstance(position, args.getQuick(0), sqlExecutionContext);
    }

    private static Function newConstantInstance(
            SqlExecutionContext sqlExecutionContext,
            int valuePosition,
            Function value
    ) {
        final int fromType = value.getType();
        final int fromScale = ColumnType.getDecimalScale(fromType);

        Decimal256 decimal = sqlExecutionContext.getDecimal256();
        DecimalUtil.load(decimal, value, null, fromType);

        if (decimal.isNull()) {
            return IntConstant.NULL;
        }

        long hh = decimal.getHh();
        long hl = decimal.getHl();
        long lh = decimal.getLh();
        long ll = decimal.getLl();
        if (fromScale != 0) {
            decimal.round(0, RoundingMode.DOWN);
        }

        // Ensures that the value fits in an int
        if (overflowsInt(decimal)) {
            decimal.of(hh, hl, lh, ll, fromScale);
            throw ImplicitCastException.inconvertibleValue(decimal, fromType, ColumnType.INT).position(valuePosition);
        }

        return IntConstant.newInstance((int) decimal.getLl());
    }

    private static Function newUnscaledInstance(int valuePosition, int fromType, Function value) {
        return switch (ColumnType.tagOf(fromType)) {
            case ColumnType.DECIMAL8 -> new UnscaledDecimal8Func(value);
            case ColumnType.DECIMAL16 -> new UnscaledDecimal16Func(value);
            case ColumnType.DECIMAL32 -> new UnscaledDecimal32Func(value);
            case ColumnType.DECIMAL64 -> new UnscaledDecimal64Func(value, valuePosition);
            case ColumnType.DECIMAL128 -> new UnscaledDecimal128Func(value, valuePosition);
            default -> new UnscaledDecimal256Func(value, valuePosition);
        };
    }

    private static boolean overflowsInt(Decimal256 decimal) {
        return overflowsInt(decimal.getHh(), decimal.getHl(), decimal.getLh(), decimal.getLl());
    }

    private static boolean overflowsInt(long hh, long hl, long lh, long ll) {
        return hh >= 0 ? Decimal256.compareTo(hh, hl, lh, ll, 0, 0, 0, 0, Integer.MAX_VALUE, 0) > 0 :
                Decimal256.compareTo(hh, hl, lh, ll, 0, -1, -1, -1, Integer.MIN_VALUE, 0) < 0;
    }

    private static boolean overflowsInt(long high, long low) {
        return high >= 0 ? Decimal128.compareTo(high, low, 0, 0, Integer.MAX_VALUE, 0) > 0 :
                Decimal128.compareTo(high, low, 0, -1, Integer.MIN_VALUE, 0) < 0;
    }

    private static class ScaledDecimalFunction extends AbstractCastToIntFunction {
        private final Decimal256 decimal256 = new Decimal256();
        private final int fromScale;
        private final int fromType;
        private final int position;

        public ScaledDecimalFunction(Function arg, int position) {
            super(arg);
            this.fromType = arg.getType();
            this.position = position;
            this.fromScale = ColumnType.getDecimalScale(fromType);
        }

        public int getInt(Record rec) {
            DecimalUtil.load(decimal256, arg, rec, fromType);
            if (decimal256.isNull()) {
                return Numbers.INT_NULL;
            }
            long hh = decimal256.getHh();
            long hl = decimal256.getHl();
            long lh = decimal256.getLh();
            long ll = decimal256.getLl();
            decimal256.round(0, RoundingMode.DOWN);
            if (overflowsInt(decimal256)) {
                decimal256.of(hh, hl, lh, ll, fromScale);
                throw ImplicitCastException.inconvertibleValue(decimal256, arg.getType(), ColumnType.INT).position(position);
            }
            return (int) decimal256.getLl();
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }
    }

    private static class UnscaledDecimal128Func extends AbstractCastToIntFunction {
        private final int position;

        public UnscaledDecimal128Func(Function value, int position) {
            super(value);
            this.position = position;
        }

        public int getInt(Record rec) {
            long hi = this.arg.getDecimal128Hi(rec);
            long lo = this.arg.getDecimal128Lo(rec);
            if (Decimal128.isNull(hi, lo)) {
                return Numbers.INT_NULL;
            }
            if (overflowsInt(hi, lo)) {
                var decimal128 = Misc.getThreadLocalDecimal128();
                decimal128.of(hi, lo, 0);
                throw ImplicitCastException.inconvertibleValue(decimal128, arg.getType(), ColumnType.INT).position(position);
            }
            return (int) lo;
        }
    }

    private static class UnscaledDecimal16Func extends AbstractCastToIntFunction {
        public UnscaledDecimal16Func(Function value) {
            super(value);
        }

        public int getInt(Record rec) {
            short value = this.arg.getDecimal16(rec);
            if (value == Decimals.DECIMAL16_NULL) {
                return Numbers.INT_NULL;
            }
            return value;
        }
    }

    private static class UnscaledDecimal256Func extends AbstractCastToIntFunction {
        private final int position;

        public UnscaledDecimal256Func(Function value, int position) {
            super(value);
            this.position = position;
        }

        public int getInt(Record rec) {
            long hh = this.arg.getDecimal256HH(rec);
            long hl = this.arg.getDecimal256HL(rec);
            long lh = this.arg.getDecimal256LH(rec);
            long ll = this.arg.getDecimal256LL(rec);
            if (Decimal256.isNull(hh, hl, lh, ll)) {
                return Numbers.INT_NULL;
            }
            if (overflowsInt(hh, hl, lh, ll)) {
                Decimal256 decimal256 = Misc.getThreadLocalDecimal256();
                decimal256.of(hh, hl, lh, ll, 0);
                throw ImplicitCastException.inconvertibleValue(decimal256, arg.getType(), ColumnType.INT).position(position);
            }
            return (int) ll;
        }
    }

    private static class UnscaledDecimal32Func extends AbstractCastToIntFunction {
        public UnscaledDecimal32Func(Function value) {
            super(value);
        }

        public int getInt(Record rec) {
            int value = this.arg.getDecimal32(rec);
            if (value == Decimals.DECIMAL32_NULL) {
                return Numbers.INT_NULL;
            }
            return value;
        }
    }

    private static class UnscaledDecimal64Func extends AbstractCastToIntFunction {
        private final int position;

        public UnscaledDecimal64Func(Function value, int position) {
            super(value);
            this.position = position;
        }

        public int getInt(Record rec) {
            long value = this.arg.getDecimal64(rec);
            if (value == Decimals.DECIMAL64_NULL) {
                return Numbers.INT_NULL;
            }
            if (value > Integer.MAX_VALUE || value < Integer.MIN_VALUE) {
                throw ImplicitCastException.inconvertibleValue(value, arg.getType(), ColumnType.INT).position(position);
            }
            return (int) value;
        }
    }

    private static class UnscaledDecimal8Func extends AbstractCastToIntFunction {
        public UnscaledDecimal8Func(Function value) {
            super(value);
        }

        public int getInt(Record rec) {
            byte value = this.arg.getDecimal8(rec);
            if (value == Decimals.DECIMAL8_NULL) {
                return Numbers.INT_NULL;
            }
            return value;
        }
    }
}
