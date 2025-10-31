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

        return switch (ColumnType.tagOf(fromType)) {
            case ColumnType.DECIMAL8 -> new ScaledDecimal8Function(arg, position);
            case ColumnType.DECIMAL16 -> new ScaledDecimal16Function(arg, position);
            case ColumnType.DECIMAL32 -> new ScaledDecimal32Function(arg, position);
            case ColumnType.DECIMAL64 -> new ScaledDecimal64Function(arg, position);
            case ColumnType.DECIMAL128 -> new ScaledDecimal128Function(arg, position);
            default -> new ScaledDecimal256Function(arg, position);
        };
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
        DecimalUtil.load(decimal, sqlExecutionContext.getDecimal128(), value, null, fromType);

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

    private static boolean overflowsByte(Decimal256 decimal256) {
        return decimal256.getHh() >= 0 ? Decimal256.compare(decimal256, 0, 0, 0, Byte.MAX_VALUE) > 0 :
                Decimal256.compare(decimal256, -1, -1, -1, Byte.MIN_VALUE) < 0;
    }

    private static boolean overflowsByte(Decimal128 decimal128) {
        return decimal128.getHigh() >= 0 ? Decimal128.compare(decimal128, 0, Byte.MAX_VALUE) > 0 :
                Decimal128.compare(decimal128, -1, Byte.MIN_VALUE) < 0;
    }

    private static class ScaledDecimal128Function extends AbstractCastToByteFunction {
        private final Decimal128 decimal128 = new Decimal128();
        private final Decimal256 decimal256 = new Decimal256();
        private final int fromScale;
        private final int position;

        public ScaledDecimal128Function(Function arg, int position) {
            super(arg);
            this.position = position;
            this.fromScale = ColumnType.getDecimalScale(arg.getType());
        }

        public byte getByte(Record rec) {
            arg.getDecimal128(rec, decimal128);
            if (decimal128.isNull()) {
                return 0;
            }
            decimal256.of(decimal128, fromScale);
            decimal256.round(0, RoundingMode.DOWN);
            if (overflowsByte(decimal256)) {
                decimal256.of(decimal128, fromScale);
                throw ImplicitCastException.inconvertibleValue(decimal256, arg.getType(), ColumnType.BYTE).position(position);
            }
            return (byte) decimal256.getLl();
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }
    }

    private static class ScaledDecimal16Function extends AbstractCastToByteFunction {
        private final Decimal256 decimal256 = new Decimal256();
        private final int fromScale;
        private final int position;

        public ScaledDecimal16Function(Function arg, int position) {
            super(arg);
            this.position = position;
            this.fromScale = ColumnType.getDecimalScale(arg.getType());
        }

        public byte getByte(Record rec) {
            short decimal16 = arg.getDecimal16(rec);
            if (decimal16 == Decimals.DECIMAL16_NULL) {
                return 0;
            }
            decimal256.ofLong(decimal16, fromScale);
            decimal256.round(0, RoundingMode.DOWN);
            if (overflowsByte(decimal256)) {
                decimal256.ofLong(decimal16, fromScale);
                throw ImplicitCastException.inconvertibleValue(decimal256, arg.getType(), ColumnType.BYTE).position(position);
            }
            return (byte) decimal256.getLl();
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }
    }

    private static class ScaledDecimal256Function extends AbstractCastToByteFunction {
        private final Decimal256 decimal256 = new Decimal256();
        private final int fromScale;
        private final int position;

        public ScaledDecimal256Function(Function arg, int position) {
            super(arg);
            this.position = position;
            this.fromScale = ColumnType.getDecimalScale(arg.getType());
        }

        public byte getByte(Record rec) {
            arg.getDecimal256(rec, decimal256);
            if (decimal256.isNull()) {
                return 0;
            }
            long hh = decimal256.getHh();
            long hl = decimal256.getHl();
            long lh = decimal256.getLh();
            long ll = decimal256.getLl();
            decimal256.setScale(fromScale);
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

    private static class ScaledDecimal32Function extends AbstractCastToByteFunction {
        private final Decimal256 decimal256 = new Decimal256();
        private final int fromScale;
        private final int position;

        public ScaledDecimal32Function(Function arg, int position) {
            super(arg);
            this.position = position;
            this.fromScale = ColumnType.getDecimalScale(arg.getType());
        }

        public byte getByte(Record rec) {
            int decimal32 = arg.getDecimal32(rec);
            if (decimal32 == Decimals.DECIMAL32_NULL) {
                return 0;
            }
            decimal256.ofLong(decimal32, fromScale);
            decimal256.round(0, RoundingMode.DOWN);
            if (overflowsByte(decimal256)) {
                decimal256.ofLong(decimal32, fromScale);
                throw ImplicitCastException.inconvertibleValue(decimal256, arg.getType(), ColumnType.BYTE).position(position);
            }
            return (byte) decimal256.getLl();
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }
    }

    private static class ScaledDecimal64Function extends AbstractCastToByteFunction {
        private final Decimal256 decimal256 = new Decimal256();
        private final int fromScale;
        private final int position;

        public ScaledDecimal64Function(Function arg, int position) {
            super(arg);
            this.position = position;
            this.fromScale = ColumnType.getDecimalScale(arg.getType());
        }

        public byte getByte(Record rec) {
            long decimal64 = arg.getDecimal64(rec);
            if (decimal64 == Decimals.DECIMAL64_NULL) {
                return 0;
            }
            decimal256.ofLong(decimal64, fromScale);
            decimal256.round(0, RoundingMode.DOWN);
            if (overflowsByte(decimal256)) {
                decimal256.ofLong(decimal64, fromScale);
                throw ImplicitCastException.inconvertibleValue(decimal256, arg.getType(), ColumnType.BYTE).position(position);
            }
            return (byte) decimal256.getLl();
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }
    }

    private static class ScaledDecimal8Function extends AbstractCastToByteFunction {
        private final Decimal256 decimal256 = new Decimal256();
        private final int fromScale;
        private final int position;

        public ScaledDecimal8Function(Function arg, int position) {
            super(arg);
            this.position = position;
            this.fromScale = ColumnType.getDecimalScale(arg.getType());
        }

        public byte getByte(Record rec) {
            byte decimal8 = arg.getDecimal8(rec);
            if (decimal8 == Decimals.DECIMAL8_NULL) {
                return 0;
            }
            decimal256.ofLong(decimal8, fromScale);
            decimal256.round(0, RoundingMode.DOWN);
            if (overflowsByte(decimal256)) {
                decimal256.ofLong(decimal8, fromScale);
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
        private final Decimal128 decimal128 = new Decimal128();
        private final int position;

        public UnscaledDecimal128Func(Function value, int position) {
            super(value);
            this.position = position;
        }

        public byte getByte(Record rec) {
            this.arg.getDecimal128(rec, decimal128);
            if (decimal128.isNull()) {
                return 0;
            }
            if (overflowsByte(decimal128)) {
                throw ImplicitCastException.inconvertibleValue(decimal128, arg.getType(), ColumnType.BYTE).position(position);
            }
            return (byte) decimal128.getLow();
        }

        @Override
        public boolean isThreadSafe() {
            return false;
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
        private final Decimal256 decimal256 = new Decimal256();
        private final int position;

        public UnscaledDecimal256Func(Function value, int position) {
            super(value);
            this.position = position;
        }

        public byte getByte(Record rec) {
            this.arg.getDecimal256(rec, decimal256);
            if (decimal256.isNull()) {
                return 0;
            }
            if (overflowsByte(decimal256)) {
                throw ImplicitCastException.inconvertibleValue(decimal256, arg.getType(), ColumnType.BYTE).position(position);
            }
            return (byte) decimal256.getLl();
        }

        @Override
        public boolean isThreadSafe() {
            return false;
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
