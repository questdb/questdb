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
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.decimal.Decimal128Function;
import io.questdb.griffin.engine.functions.decimal.Decimal256Function;
import io.questdb.griffin.engine.functions.decimal.Decimal64Function;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

public class CastShortToDecimalFunctionFactory implements FunctionFactory {
    public static Function newInstance(
            int position,
            Function arg,
            int targetType,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        if (arg.isConstant()) {
            short value = arg.getShort(null);
            return newConstantInstance(sqlExecutionContext, position, targetType, value);
        }

        final int targetScale = ColumnType.getDecimalScale(targetType);
        if (targetScale == 0) {
            return newUnscaledInstance(position, targetType, arg);
        }

        final int targetPrecision = ColumnType.getDecimalPrecision(targetType);
        long maxUnscaledValue = targetScale >= targetPrecision ? 0 : Numbers.getMaxValue(targetPrecision - targetScale);
        return new AbstractCastDecimalScaledFunc(position, targetType, maxUnscaledValue, arg);
    }

    public static Function newInstance(
            int position,
            Function arg,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        int targetPrecision;
        if (arg.isConstant()) {
            short value = arg.getShort(null);
            targetPrecision = Math.max(Numbers.getPrecision(value), 1);
        } else {
            targetPrecision = Numbers.getPrecision(Short.MAX_VALUE);
        }
        int targetType = ColumnType.getDecimalType(targetPrecision, 0);
        return newInstance(position, arg, targetType, sqlExecutionContext);
    }

    @Override
    public String getSignature() {
        return "cast(Eξ)";
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
            short value
    ) {
        final int targetPrecision = ColumnType.getDecimalPrecision(targetType);
        final int targetScale = ColumnType.getDecimalScale(targetType);

        // Ensures that the value fits in the decimal target
        if (value != 0 && Numbers.getPrecision(value) + targetScale > ColumnType.getDecimalPrecision(targetType)) {
            throw ImplicitCastException.inconvertibleValue(
                    value,
                    ColumnType.SHORT,
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
        return switch (ColumnType.tagOf(targetType)) {
            case ColumnType.DECIMAL8, ColumnType.DECIMAL16, ColumnType.DECIMAL32, ColumnType.DECIMAL64 ->
                    new CastDecimal64UnscaledFunc(valuePosition, targetType, Numbers.getMaxValue(targetPrecision), value);
            case ColumnType.DECIMAL128 -> new CastDecimal128UnscaledFunc(targetType, value);
            default -> new CastDecimal256UnscaledFunc(targetType, value);
        };
    }

    private static class AbstractCastDecimalScaledFunc extends AbstractCastToDecimalFunction {
        private final short maxUnscaledValue;
        private final short minUnscaledValue;

        public AbstractCastDecimalScaledFunc(int position, int targetType, long maxUnscaledValue, Function value) {
            super(value, targetType, position);
            this.maxUnscaledValue = (short) Math.min(maxUnscaledValue, Short.MAX_VALUE);
            this.minUnscaledValue = (short) Math.max(-maxUnscaledValue, Short.MIN_VALUE);
        }

        protected boolean cast(Record rec) {
            short value = this.arg.getShort(rec);
            if (value < minUnscaledValue || value > maxUnscaledValue) {
                throw ImplicitCastException.inconvertibleValue(
                        value,
                        ColumnType.SHORT,
                        type
                ).position(position);
            }
            decimal.ofLong(value, 0);
            decimal.rescale(scale);
            return true;
        }
    }

    private static class CastDecimal128UnscaledFunc extends Decimal128Function implements UnaryFunction {
        private final Function value;

        public CastDecimal128UnscaledFunc(int targetType, Function value) {
            super(targetType);
            this.value = value;
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            final short value = this.value.getShort(rec);
            // No need for overflow check, if the precision was lower than
            // 19 it wouldn't be a Decimal128, otherwise, any long can fit.
            sink.ofRaw(value);
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(value).val("::").val(ColumnType.nameOf(type));
        }
    }

    private static class CastDecimal256UnscaledFunc extends Decimal256Function implements UnaryFunction {
        private final Function value;

        public CastDecimal256UnscaledFunc(int targetType, Function value) {
            super(targetType);
            this.value = value;
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            final short value = this.value.getShort(rec);
            // No need for overflow check, if the precision was lower than
            // 19 it wouldn't be a Decimal256, otherwise, any int can fit.
            sink.ofRaw(value);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(value).val("::").val(ColumnType.nameOf(type));
        }
    }

    private static class CastDecimal64UnscaledFunc extends Decimal64Function implements UnaryFunction {
        private final short maxValue;
        private final short minValue;
        private final int position;
        private final Function value;

        public CastDecimal64UnscaledFunc(int position, int targetType, long maxValue, Function value) {
            super(targetType);
            this.position = position;
            this.maxValue = (short) Math.min(maxValue, Short.MAX_VALUE);
            this.minValue = (short) Math.max(-maxValue, Short.MIN_VALUE);
            this.value = value;
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public short getDecimal16(Record rec) {
            final short value = this.value.getShort(rec);
            overflowCheck(value);
            return value;
        }

        @Override
        public int getDecimal32(Record rec) {
            // No overflow check needed here, if we're using this type we know that
            // we have enough space to store Short.MAX_VALUE.
            return this.value.getShort(rec);
        }

        @Override
        public long getDecimal64(Record rec) {
            // No overflow check needed here, if we're using this type we know that
            // we have enough space to store Short.MAX_VALUE.
            return this.value.getShort(rec);
        }

        @Override
        public byte getDecimal8(Record rec) {
            final short value = this.value.getShort(rec);
            overflowCheck(value);
            return (byte) value;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(value).val("::").val(ColumnType.nameOf(type));
        }

        private void overflowCheck(short value) {
            if (value < minValue || value > maxValue) {
                throw ImplicitCastException.inconvertibleValue(
                        value,
                        ColumnType.SHORT,
                        type
                ).position(position);
            }
        }
    }
}
