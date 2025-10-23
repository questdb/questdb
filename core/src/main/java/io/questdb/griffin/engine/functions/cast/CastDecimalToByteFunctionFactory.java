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
import io.questdb.griffin.engine.functions.constants.ByteConstant;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

import java.math.RoundingMode;

public class CastDecimalToByteFunctionFactory implements FunctionFactory {

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
        return "cast(Ξb)";
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
            return ByteConstant.ZERO;
        }

        long hh = decimal.getHh();
        long hl = decimal.getHl();
        long lh = decimal.getLh();
        long ll = decimal.getLl();
        if (fromScale != 0) {
            decimal.round(0, RoundingMode.DOWN);
        }

        // Ensures that the value fits in a byte
        if (overflowsByte(decimal)) {
            decimal.of(hh, hl, lh, ll, fromScale);
            throw ImplicitCastException.inconvertibleValue(decimal, fromType, ColumnType.BYTE).position(valuePosition);
        }

        return ByteConstant.newInstance((byte) decimal.getLl());
    }

    private static Function newUnscaledInstance(int valuePosition, int fromType, Function value) {
        return switch (ColumnType.tagOf(fromType)) {
            case ColumnType.DECIMAL8 -> new UnscaledDecimal8Func(value);
            case ColumnType.DECIMAL16 -> new UnscaledDecimal16Func(value, valuePosition);
            case ColumnType.DECIMAL32 -> new UnscaledDecimal32Func(value, valuePosition);
            case ColumnType.DECIMAL64 -> new UnscaledDecimal64Func(value, valuePosition);
            case ColumnType.DECIMAL128 -> new UnscaledDecimal128Func(value, valuePosition);
            default -> new UnscaledDecimal256Func(value, valuePosition);
        };
    }

    private static boolean overflowsByte(Decimal256 decimal) {
        return overflowsByte(decimal.getHh(), decimal.getHl(), decimal.getLh(), decimal.getLl());
    }

    private static boolean overflowsByte(long hh, long hl, long lh, long ll) {
        return hh >= 0 ? Decimal256.compareTo(hh, hl, lh, ll, 0, 0, 0, 0, Byte.MAX_VALUE, 0) > 0 :
                Decimal256.compareTo(hh, hl, lh, ll, 0, -1, -1, -1, Byte.MIN_VALUE, 0) < 0;
    }

    private static boolean overflowsByte(long high, long low) {
        return high >= 0 ? Decimal128.compareTo(high, low, 0, 0, Byte.MAX_VALUE, 0) > 0 :
                Decimal128.compareTo(high, low, 0, -1, Byte.MIN_VALUE, 0) < 0;
    }

    private static class ScaledDecimalFunction extends AbstractCastToByteFunction {
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

        public byte getByte(Record rec) {
            DecimalUtil.load(decimal256, arg, rec, fromType);
            if (decimal256.isNull()) {
                return 0;
            }
            long hh = decimal256.getHh();
            long hl = decimal256.getHl();
            long lh = decimal256.getLh();
            long ll = decimal256.getLl();
            decimal256.round(0, RoundingMode.DOWN);
            if (overflowsByte(decimal256)) {
                decimal256.of(hh, hl, lh, ll, fromScale);
                throw ImplicitCastException.inconvertibleValue(decimal256, arg.getType(), ColumnType.BYTE).position(position);
            }
            return (byte) decimal256.getLl();
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }
    }

    private static class UnscaledDecimal128Func extends AbstractCastToByteFunction {
        private final int position;

        public UnscaledDecimal128Func(Function value, int position) {
            super(value);
            this.position = position;
        }

        public byte getByte(Record rec) {
            long hi = this.arg.getDecimal128Hi(rec);
            long lo = this.arg.getDecimal128Lo(rec);
            if (Decimal128.isNull(hi, lo)) {
                return 0;
            }
            if (overflowsByte(hi, lo)) {
                var decimal128 = Misc.getThreadLocalDecimal128();
                decimal128.of(hi, lo, 0);
                throw ImplicitCastException.inconvertibleValue(decimal128, arg.getType(), ColumnType.BYTE).position(position);
            }
            return (byte) lo;
        }
    }

    private static class UnscaledDecimal16Func extends AbstractCastToByteFunction {
        private final int position;

        public UnscaledDecimal16Func(Function value, int position) {
            super(value);
            this.position = position;
        }

        public byte getByte(Record rec) {
            short value = this.arg.getDecimal16(rec);
            if (value == Decimals.DECIMAL16_NULL) {
                return 0;
            }
            if (value > Byte.MAX_VALUE || value < Byte.MIN_VALUE) {
                throw ImplicitCastException.inconvertibleValue(value, arg.getType(), ColumnType.BYTE).position(position);
            }
            return (byte) value;
        }
    }

    private static class UnscaledDecimal256Func extends AbstractCastToByteFunction {
        private final int position;

        public UnscaledDecimal256Func(Function value, int position) {
            super(value);
            this.position = position;
        }

        public byte getByte(Record rec) {
            long hh = this.arg.getDecimal256HH(rec);
            long hl = this.arg.getDecimal256HL(rec);
            long lh = this.arg.getDecimal256LH(rec);
            long ll = this.arg.getDecimal256LL(rec);
            if (Decimal256.isNull(hh, hl, lh, ll)) {
                return 0;
            }
            if (overflowsByte(hh, hl, lh, ll)) {
                Decimal256 decimal256 = Misc.getThreadLocalDecimal256();
                decimal256.of(hh, hl, lh, ll, 0);
                throw ImplicitCastException.inconvertibleValue(decimal256, arg.getType(), ColumnType.BYTE).position(position);
            }
            return (byte) ll;
        }
    }

    private static class UnscaledDecimal32Func extends AbstractCastToByteFunction {
        private final int position;

        public UnscaledDecimal32Func(Function value, int position) {
            super(value);
            this.position = position;
        }

        public byte getByte(Record rec) {
            int value = this.arg.getDecimal32(rec);
            if (value == Decimals.DECIMAL32_NULL) {
                return 0;
            }
            if (value > Byte.MAX_VALUE || value < Byte.MIN_VALUE) {
                throw ImplicitCastException.inconvertibleValue(value, arg.getType(), ColumnType.BYTE).position(position);
            }
            return (byte) value;
        }
    }

    private static class UnscaledDecimal64Func extends AbstractCastToByteFunction {
        private final int position;

        public UnscaledDecimal64Func(Function value, int position) {
            super(value);
            this.position = position;
        }

        public byte getByte(Record rec) {
            long value = this.arg.getDecimal64(rec);
            if (value == Decimals.DECIMAL64_NULL) {
                return 0;
            }
            if (value > Byte.MAX_VALUE || value < Byte.MIN_VALUE) {
                throw ImplicitCastException.inconvertibleValue(value, arg.getType(), ColumnType.BYTE).position(position);
            }
            return (byte) value;
        }
    }

    private static class UnscaledDecimal8Func extends AbstractCastToByteFunction {
        public UnscaledDecimal8Func(Function value) {
            super(value);
        }

        public byte getByte(Record rec) {
            byte value = this.arg.getDecimal8(rec);
            if (value == Decimals.DECIMAL8_NULL) {
                return 0;
            }
            return value;
        }
    }
}
