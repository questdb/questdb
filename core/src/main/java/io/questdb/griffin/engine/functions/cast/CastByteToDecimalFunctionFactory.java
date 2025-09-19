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
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.DecimalFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

public class CastByteToDecimalFunctionFactory implements FunctionFactory {
    public static Function newInstance(
            int position,
            Function arg,
            int targetType,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        if (arg.isConstant()) {
            byte value = arg.getByte(null);
            return newConstantInstance(sqlExecutionContext, position, targetType, value);
        }

        final int targetScale = ColumnType.getDecimalScale(targetType);
        if (targetScale == 0) {
            return newUnscaledInstance(position, targetType, arg);
        }

        final int targetPrecision = ColumnType.getDecimalPrecision(targetType);
        long maxUnscaledValue = targetScale >= targetPrecision ? 0 : Numbers.getMaxValue(targetPrecision - targetScale);
        return new CastDecimalScaledFunc(position, targetType, (byte) Math.min(maxUnscaledValue, Byte.MAX_VALUE), arg);
    }

    @Override
    public String getSignature() {
        return "cast(Bξ)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        return newInstance(argPositions.getQuick(0), args.getQuick(0), args.getQuick(1).getType(), sqlExecutionContext);
    }

    private static Function newConstantInstance(
            SqlExecutionContext sqlExecutionContext,
            int valuePosition,
            int targetType,
            byte value
    ) {
        final int targetPrecision = ColumnType.getDecimalPrecision(targetType);
        final int targetScale = ColumnType.getDecimalScale(targetType);

        if (value == Numbers.BYTE_NULL) {
            return DecimalUtil.createNullDecimalConstant(targetPrecision, targetScale);
        }

        // Ensures that the value fits in the decimal target
        if (Numbers.getPrecision(value) + targetScale > ColumnType.getDecimalPrecision(targetType)) {
            throw ImplicitCastException.inconvertibleValue(
                    value,
                    ColumnType.BYTE,
                    targetType
            ).position(valuePosition);
        }

        Decimal256 d = sqlExecutionContext.getDecimal256();
        d.ofLong(value, 0);
        if (targetScale != 0) {
            // No overflow possible as we already checked the precision before
            d.rescale(targetScale);
        }

        // We don't try to narrow the type on its actual precision so that the user may pre-widen the constant decimal
        // to avoid doing it at query execution.
        return DecimalUtil.createDecimalConstant(d, targetPrecision, targetScale);
    }

    private static Function newUnscaledInstance(int valuePosition, int targetType, Function value) {
        int targetPrecision = ColumnType.getDecimalPrecision(targetType);
        switch (ColumnType.tagOf(targetType)) {
            case ColumnType.DECIMAL8:
            case ColumnType.DECIMAL16:
            case ColumnType.DECIMAL32:
            case ColumnType.DECIMAL64:
                return new CastDecimal64UnscaledFunc(valuePosition, targetType, (byte) Math.min(Numbers.getMaxValue(targetPrecision), Byte.MAX_VALUE), value);
            case ColumnType.DECIMAL128:
                return new CastDecimal128UnscaledFunc(targetType, value);
            default:
                return new CastDecimal256UnscaledFunc(targetType, value);
        }
    }

    private static class CastDecimal128UnscaledFunc extends DecimalFunction implements UnaryFunction {
        private final Function value;
        private long lo;

        public CastDecimal128UnscaledFunc(int targetType, Function value) {
            super(targetType);
            this.lo = 0;
            this.value = value;
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public long getDecimal128Hi(Record rec) {
            final byte value = this.value.getByte(rec);
            if (value == Numbers.BYTE_NULL) {
                lo = Decimals.DECIMAL128_LO_NULL;
                return Decimals.DECIMAL128_HI_NULL;
            }
            // No need for overflow check, if the precision was lower than
            // 19 it wouldn't be a Decimal128, otherwise, any long can fit.
            lo = value;
            return value < 0 ? -1 : 0;
        }

        @Override
        public long getDecimal128Lo(Record rec) {
            return lo;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(value).val("::").val(ColumnType.nameOf(type));
        }
    }

    private static class CastDecimal256UnscaledFunc extends DecimalFunction implements UnaryFunction {
        private final Function value;
        private long hl;
        private long lh;
        private long ll;

        public CastDecimal256UnscaledFunc(int targetType, Function value) {
            super(targetType);
            this.ll = 0;
            this.lh = 0;
            this.hl = 0;
            this.value = value;
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public long getDecimal256HH(Record rec) {
            final byte value = this.value.getByte(rec);
            if (value == Numbers.BYTE_NULL) {
                ll = Decimals.DECIMAL256_LL_NULL;
                lh = Decimals.DECIMAL256_LH_NULL;
                hl = Decimals.DECIMAL256_HL_NULL;
                return Decimals.DECIMAL256_HH_NULL;
            }
            // No need for overflow check, if the precision was lower than
            // 19 it wouldn't be a Decimal256, otherwise, any int can fit.
            ll = value;
            hl = lh = value < 0 ? -1 : 0;
            return lh;
        }

        @Override
        public long getDecimal256HL(Record rec) {
            return hl;
        }

        @Override
        public long getDecimal256LH(Record rec) {
            return lh;
        }

        @Override
        public long getDecimal256LL(Record rec) {
            return ll;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(value).val("::").val(ColumnType.nameOf(type));
        }
    }

    private static class CastDecimal64UnscaledFunc extends DecimalFunction implements UnaryFunction {
        private final short maxValue;
        private final short minValue;
        private final int position;
        private final Function value;

        public CastDecimal64UnscaledFunc(int position, int targetType, short maxValue, Function value) {
            super(targetType);
            this.position = position;
            this.maxValue = maxValue;
            this.minValue = (byte) (maxValue == Byte.MAX_VALUE ? Byte.MIN_VALUE : -maxValue);
            this.value = value;
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public short getDecimal16(Record rec) {
            final byte value = this.value.getByte(rec);
            if (value == Numbers.BYTE_NULL) {
                return Decimals.DECIMAL16_NULL;
            }
            // No overflow check needed here, if we're using this type we know that
            // we have enough space to store Byte.MAX_VALUE.
            return value;
        }

        @Override
        public int getDecimal32(Record rec) {
            final byte value = this.value.getByte(rec);
            if (value == Numbers.BYTE_NULL) {
                return Decimals.DECIMAL32_NULL;
            }
            // No overflow check needed here, if we're using this type we know that
            // we have enough space to store Byte.MAX_VALUE.
            return value;
        }

        @Override
        public long getDecimal64(Record rec) {
            final byte value = this.value.getByte(rec);
            if (value == Numbers.BYTE_NULL) {
                return Decimals.DECIMAL64_NULL;
            }
            // No overflow check needed here, if we're using this type we know that
            // we have enough space to store Byte.MAX_VALUE.
            return value;
        }

        @Override
        public byte getDecimal8(Record rec) {
            final byte value = this.value.getByte(rec);
            if (value == Numbers.BYTE_NULL) {
                return Decimals.DECIMAL8_NULL;
            }
            overflowCheck(value);
            return value;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(value).val("::").val(ColumnType.nameOf(type));
        }

        private void overflowCheck(int value) {
            if (value < minValue || value > maxValue) {
                throw ImplicitCastException.inconvertibleValue(
                        value,
                        ColumnType.BYTE,
                        type
                ).position(position);
            }
        }
    }

    private static class CastDecimalScaledFunc extends CastToDecimalFunction {
        private final byte maxUnscaledValue;
        private final byte minUnscaledValue;

        public CastDecimalScaledFunc(int position, int targetType, byte maxUnscaledValue, Function value) {
            super(value, targetType, position);
            this.maxUnscaledValue = maxUnscaledValue;
            this.minUnscaledValue = (byte) -maxUnscaledValue;
        }

        protected boolean cast(Record rec) {
            byte value = this.arg.getByte(rec);
            if (value == Numbers.BYTE_NULL) {
                return false;
            }
            if (value < minUnscaledValue || value > maxUnscaledValue) {
                throw ImplicitCastException.inconvertibleValue(
                        value,
                        ColumnType.BYTE,
                        type
                ).position(position);
            }
            decimal.ofLong(value, 0);
            decimal.rescale(scale);
            return true;
        }
    }
}
