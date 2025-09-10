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
import io.questdb.griffin.engine.functions.AbstractDecimalFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

public class CastLongToDecimalFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "cast(Lξ)";
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
        final int targetType = args.getQuick(1).getType();
        if (arg.isConstant()) {
            long value = arg.getLong(null);
            return newConstantInstance(sqlExecutionContext, argPositions.get(0), targetType, value);
        }

        final int targetScale = ColumnType.getDecimalScale(targetType);
        if (targetScale == 0) {
            return newUnscaledInstance(argPositions.get(0), targetType, arg);
        }

        final int targetPrecision = ColumnType.getDecimalPrecision(targetType);
        long maxUnscaledValue = targetScale >= targetPrecision ? 0L : Decimals.getMaxLong(targetPrecision - targetScale);
        return new CastDecimalScaledFunc(argPositions.get(0), targetType, maxUnscaledValue, arg);
    }

    private static Function newUnscaledInstance(int valuePosition, int targetType, Function value) {
        int targetPrecision = ColumnType.getDecimalPrecision(targetType);
        switch (ColumnType.tagOf(targetType)) {
            case ColumnType.DECIMAL8:
            case ColumnType.DECIMAL16:
            case ColumnType.DECIMAL32:
            case ColumnType.DECIMAL64:
                return new CastDecimal64UnscaledFunc(valuePosition, targetType, Decimals.getMaxLong(targetPrecision), value);
            case ColumnType.DECIMAL128:
                return new CastDecimal128UnscaledFunc(targetType, value);
            default:
                return new CastDecimal256UnscaledFunc(targetType, value);
        }
    }

    private Function newConstantInstance(
            SqlExecutionContext sqlExecutionContext,
            int valuePosition,
            int targetType,
            long value
    ) {
        final int targetPrecision = ColumnType.getDecimalPrecision(targetType);
        final int targetScale = ColumnType.getDecimalScale(targetType);

        // Ensures that the value fits in the decimal target
        if (value != 0 && Decimals.getLongPrecision(value) + targetScale > ColumnType.getDecimalPrecision(targetType)) {
            throw ImplicitCastException.inconvertibleValue(
                    value,
                    ColumnType.LONG,
                    targetType
            ).position(valuePosition);
        }

        if (value == Numbers.LONG_NULL) {
            return DecimalUtil.createNullDecimalConstant(targetPrecision, targetScale);
        }

        Decimal256 d = sqlExecutionContext.getDecimal256();
        d.ofLong(value, 0);
        if (targetScale != 0 && value != 0) {
            // No overflow possible as we already checked the precision before
            d.rescale(targetScale);
        }

        // We don't try to narrow the type on its actual precision so that the user may pre-widen the constant decimal
        // to avoid doing it at query execution.
        return DecimalUtil.createDecimalConstant(d, targetPrecision, targetScale);
    }

    private static class CastDecimal128UnscaledFunc extends AbstractDecimalFunction implements UnaryFunction {
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
            final long value = this.value.getLong(rec);
            if (value == Numbers.LONG_NULL) {
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

    private static class CastDecimal256UnscaledFunc extends AbstractDecimalFunction implements UnaryFunction {
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
            final long value = this.value.getLong(rec);
            if (value == Numbers.LONG_NULL) {
                ll = Decimals.DECIMAL256_LL_NULL;
                lh = Decimals.DECIMAL256_LH_NULL;
                hl = Decimals.DECIMAL256_HL_NULL;
                return Decimals.DECIMAL256_HH_NULL;
            }
            // No need for overflow check, if the precision was lower than
            // 19 it wouldn't be a Decimal256, otherwise, any long can fit.
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

    private static class CastDecimal64UnscaledFunc extends AbstractDecimalFunction implements UnaryFunction {
        private final long maxValue;
        private final long minValue;
        private final int position;
        private final Function value;

        public CastDecimal64UnscaledFunc(int position, int targetType, long maxValue, Function value) {
            super(targetType);
            this.position = position;
            this.maxValue = maxValue;
            this.minValue = -maxValue;
            this.value = value;
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public short getDecimal16(Record rec) {
            final long value = this.value.getLong(rec);
            if (value == Numbers.LONG_NULL) {
                return Decimals.DECIMAL16_NULL;
            }
            overflowCheck(value);
            return (short) value;
        }

        @Override
        public int getDecimal32(Record rec) {
            final long value = this.value.getLong(rec);
            if (value == Numbers.LONG_NULL) {
                return Decimals.DECIMAL32_NULL;
            }
            overflowCheck(value);
            return (int) value;
        }

        @Override
        public long getDecimal64(Record rec) {
            final long value = this.value.getLong(rec);
            if (value == Numbers.LONG_NULL) {
                return Decimals.DECIMAL64_NULL;
            }
            overflowCheck(value);
            return value;
        }

        @Override
        public byte getDecimal8(Record rec) {
            final long value = this.value.getLong(rec);
            if (value == Numbers.LONG_NULL) {
                return Decimals.DECIMAL8_NULL;
            }
            overflowCheck(value);
            return (byte) value;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(value).val("::").val(ColumnType.nameOf(type));
        }

        private void overflowCheck(long value) {
            if (value < minValue || value > maxValue) {
                throw ImplicitCastException.inconvertibleValue(
                        value,
                        ColumnType.LONG,
                        type
                ).position(position);
            }
        }
    }

    private static class CastDecimalScaledFunc extends AbstractCastToDecimalFunction {
        private final long maxUnscaledValue;
        private final long minUnscaledValue;

        public CastDecimalScaledFunc(int position, int targetType, long maxUnscaledValue, Function value) {
            super(value, targetType, position);
            this.maxUnscaledValue = maxUnscaledValue;
            this.minUnscaledValue = -maxUnscaledValue;
        }

        protected boolean cast(Record rec) {
            long value = this.arg.getLong(rec);
            if (value == Numbers.LONG_NULL) {
                return false;
            }
            if (value < minUnscaledValue || value > maxUnscaledValue) {
                throw ImplicitCastException.inconvertibleValue(
                        value,
                        ColumnType.LONG,
                        type
                ).position(position);
            }
            decimal.ofLong(value, 0);
            decimal.rescale(scale);
            return true;
        }
    }
}
